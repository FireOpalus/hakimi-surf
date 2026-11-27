"""Tests for built-in EventLedger SystemModule, cross-day planner memory, and privacy."""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr

from hakimi.adapters import Sender
from hakimi.app import create_app
from hakimi.dual_memory import Extraction
from hakimi.event_ledger import EventLedger
from hakimi.life_engine import GROUP_SCRUBBED_INTENT_NOTE, LifeEngine
from hakimi.modules.actions import Action, ActionCall, NoArgs, Outcome
from hakimi.modules.base import BaseModule, sanitize_sensor_text
from hakimi.service import Service
from tests.fakes import FakeBili
from tests.test_admin import TOKEN
from tests.test_proactive_dm import Wire
from tests.test_v20_life_engine import LifeGateway

TZ = ZoneInfo("Asia/Shanghai")


def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


class _FarmModule(BaseModule):
    module_id = "farm"
    display_name = "模拟农场"
    description = "测试用跨日播种与收菜模块"

    def __init__(self, settings=None) -> None:
        super().__init__(settings)
        self.fp = "idle"
        self.harvested: list[str] = []

    def planning_fingerprint(self) -> str:
        return self.fp

    def actions(self) -> tuple[Action, ...]:
        async def harvest(call: ActionCall) -> Outcome:
            self.harvested.append(call.ticket["id"])
            return Outcome("done", "harvested")

        return (
            Action(
                name="farm.harvest",
                description="去农场收获已经成熟的作物",
                arguments=NoArgs,
                run=harvest,
            ),
        )

    async def _raw_sensor_summary(self) -> str:
        return "农场状态:" + self.fp


async def test_event_ledger_encryption_and_privacy_isolation(settings, store):
    now = [_ts("2026-11-19T18:00:00")]
    ledger = EventLedger(settings, store, clock=lambda: now[0])
    await ledger.initialize()

    ev = await ledger.post(
        "qq_owner",
        "owner_commitment",
        "SECRET_MINE_PROMISE_TOMORROW",
        payload={"note": "ONLY_OWNER_SECRET"},
        dedupe_key="mine-1",
        visibility="owner_private",
        now=now[0],
    )
    row = await store.one("SELECT body_blob, visibility FROM event_ledger WHERE id=?", (ev.id,))
    assert row["visibility"] == "owner_private"
    assert b"SECRET_MINE_PROMISE_TOMORROW" not in row["body_blob"]
    assert b"ONLY_OWNER_SECRET" not in row["body_blob"]

    assert await ledger.read(include_owner_private=False, now=now[0]) == []
    private_items = await ledger.read(include_owner_private=True, now=now[0])
    assert len(private_items) == 1
    assert private_items[0].summary == "SECRET_MINE_PROMISE_TOMORROW"
    assert private_items[0].payload == {"note": "ONLY_OWNER_SECRET"}

    assert not await ledger.consume(
        "mine-1", consumer="farm", allow_owner_private=False, now=now[0]
    )
    assert await ledger.consume(
        "mine-1",
        consumer="planner",
        allow_owner_private=True,
        allowed_sources={"qq_owner"},
        now=now[0] + 5,
    )
    assert await ledger.read(include_owner_private=True, now=now[0] + 10) == []


async def test_farm_minigame_sdk_post_read_consume_and_ttl(settings, store):
    settings.owner_qq_id = "20000"
    day1_1800 = _ts("2026-11-19T18:00:00")
    day1_2100 = _ts("2026-11-19T21:00:00")
    day2_1000 = _ts("2026-11-20T10:00:00")
    day2_1005 = _ts("2026-11-20T10:05:00")

    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, LifeGateway(), FakeBili(), Sender(settings, http, Wire()))
        farm = _FarmModule(settings)
        svc.modules.register(farm)
        await svc.event_ledger.initialize()

        posted = await farm.post_ledger_event(
            "crop_ready",
            "西边3块草莓成熟待收",
            payload={"plot": "west", "count": 3},
            target_module="farm",
            dedupe_key="plot:west",
            due_at=day2_1000,
            ttl_seconds=24 * 3600.0,
            now=day1_1800,
        )
        assert posted.dedupe_key == "farm:plot:west"

        assert await farm.read_ledger_events(only_due=True, now=day1_2100) == []
        upcoming = await farm.read_ledger_events(only_due=False, now=day1_2100)
        assert len(upcoming) == 1 and upcoming[0].due_at == day2_1000

        due_now = await farm.read_ledger_events(only_due=True, now=day2_1005)
        assert len(due_now) == 1 and due_now[0].summary == "西边3块草莓成熟待收"

        assert await farm.consume_ledger_event("plot:west", now=day2_1005) is True
        assert await farm.consume_ledger_event("plot:west", now=day2_1005) is False
        assert await farm.read_ledger_events(only_due=False, now=day2_1005) == []

        # Replanting with the same dedupe_key revives cleanly, then expires after TTL
        replanted = await farm.post_ledger_event(
            "crop_ready",
            "西边草莓第二茬待收",
            dedupe_key="plot:west",
            due_at=day2_1005 + 3600,
            ttl_seconds=3600.0,
            now=day2_1005 + 60,
        )
        assert replanted.status == "pending"
        expired_check = await farm.read_ledger_events(only_due=False, now=day2_1005 + 7201)
        assert expired_check == []
        # read() is strictly read-only; explicit expire_due() transitions DB status to expired.
        assert await svc.event_ledger.expire_due(day2_1005 + 7201) == 1
        row = await store.one("SELECT status FROM event_ledger WHERE id=?", (replanted.id,))
        assert row["status"] == "expired"


async def test_ledger_due_wakeup_signature_and_source_guard(settings, store):
    settings.owner_qq_id = "20000"
    now = [_ts("2026-11-19T09:00:00")]

    async with httpx.AsyncClient() as http:
        gw = LifeGateway(
            plan_json=json.dumps(
                {
                    "current_module": "idle",
                    "focus_level": "LOW",
                    "mood": "calm",
                    "mood_longnick": "",
                    "rest_minutes": 0,
                    "intents": [],
                    "agenda_changes": [],
                    "ledger_posts": [],
                    "ledger_consumes": ["farm:plot:west"],
                },
                ensure_ascii=False,
            )
        )
        svc = Service(settings, store, gw, FakeBili(), Sender(settings, http, Wire()))
        farm = _FarmModule(settings)
        svc.modules.register(farm)
        svc.world_state.clock = lambda: now[0]
        engine = LifeEngine(svc, clock=lambda: now[0])
        await engine.initialize()

        # First plan establishes signature and 2-4h next_plan_at
        res1 = await engine.trigger_plan_once(now=now[0])
        assert res1["status"] == "planned" and len(gw.calls) == 1

        # Normal automatic tick hits unchanged_state
        now[0] = res1["next_plan_at"]
        res_skip = await engine.trigger_plan_once(now=now[0], automatic=True)
        assert res_skip["reason"] == "unchanged_state" and len(gw.calls) == 1

        # Farm module posts commitment due in 30 minutes -> clamps next_plan_at
        post_at = now[0] + 60
        due_at = post_at + 1800
        now[0] = post_at
        await farm.post_ledger_event(
            "crop_ready",
            "西边草莓即将成熟待收",
            target_module="farm",
            dedupe_key="plot:west",
            due_at=due_at,
            now=post_at,
        )
        assert engine.planner.next_plan_at == due_at

        # At due_at, automatic planner wakes immediately (signature flipped via ledger_due)
        now[0] = due_at
        res_due = await engine.trigger_plan_once(now=due_at, automatic=True)
        assert res_due["status"] == "planned" and len(gw.calls) == 2
        payload = gw.calls[1][1]
        assert any(
            item["key"] == "farm:plot:west" and item["is_due"] is True
            for item in payload["pending_ledger_events"]
        )
        # Planner cannot prematurely consume FarmModule's objective event
        assert len(await farm.read_ledger_events(only_due=True, now=due_at)) == 1

        # Next automatic tick with same due state (while not restless_bored) hits unchanged_state
        now[0] = res_due["next_plan_at"]
        await svc.world_state.update_state(boredom=20)
        res_quiet = await engine.trigger_plan_once(now=now[0], automatic=True)
        assert res_quiet["reason"] == "unchanged_state" and len(gw.calls) == 2

        # Module planning_fingerprint change breaks idle suppression at next review tick
        now[0] = res_quiet["next_plan_at"]
        await svc.world_state.update_state(boredom=20)
        farm.fp = "water_low"
        res_fp = await engine.trigger_plan_once(now=now[0], automatic=True)
        assert res_fp["status"] == "planned" and len(gw.calls) == 3


async def test_owner_commitments_into_planner_and_group_intent_scrubbing(settings, store, message):
    settings.owner_qq_id = "20000"
    settings.onebot_enabled = True
    settings.onebot_self_id = "10000"
    settings.onebot_mode = "ws_reverse"
    now = [_ts("2026-11-19T14:00:00")]

    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, LifeGateway(), FakeBili(), Sender(settings, http, Wire()))
        farm = _FarmModule(settings)
        svc.modules.register(farm)
        svc.world_state.clock = lambda: now[0]
        await svc.proactive.initialize()
        await svc.proactive.on_connection(True)
        engine = LifeEngine(svc, clock=lambda: now[0])
        await engine.initialize()

        owner_msg = message.model_copy(
            update={
                "platform": "onebot",
                "bot_id": "10000",
                "kind": "private",
                "conversation_id": "20000",
                "user_id": "20000",
                "text": "明天下午两点你先上MC挖铁矿，我等一小时来",
                "addressed": True,
                "event_id": "owner-dm-1",
                "created_at": now[0],
            }
        )
        # Seed recent Owner turn and DualMemory chronicle commitment
        await svc.sessions.remember(
            owner_msg,
            "明天下午两点你先上MC挖铁矿，我等一小时来",
            "好嘟BOISER，我记在日程里啦喵~",
        )
        await svc.sessions.dual.snapshot_cold(owner_msg, force=True)
        job = await svc.sessions.dual.claim(owner_msg)
        owner_ext = Extraction.model_validate(
            {
                "profiles": [],
                "chronicles": [
                    {
                        "topic": "MC挖矿约定",
                        "category": "open_question",
                        "text": "和BOISER约好明天14:00先上MC挖铁矿并等他一小时后汇合",
                        "tags": ["MC", "约定"],
                        "evidence_seqs": [1],
                    }
                ],
            }
        )
        assert await svc.sessions.dual.commit(owner_msg, job, owner_ext)

        # Group message from Owner or member MUST NEVER post to event_ledger
        owner_in_group = owner_msg.model_copy(
            update={"kind": "group", "conversation_id": "40000", "event_id": "grp-owner-1"}
        )
        await svc.sessions.remember(owner_in_group, "群里开黑", "收到")
        await svc.sessions.dual.snapshot_cold(owner_in_group, force=True)
        grp_job = await svc.sessions.dual.claim(owner_in_group)
        grp_ext = Extraction.model_validate(
            {
                "profiles": [],
                "chronicles": [
                    {
                        "topic": "群聊约定",
                        "category": "open_question",
                        "text": "群里随便说的打游戏约定不应该进私密账本",
                        "tags": ["群聊"],
                        "evidence_seqs": [1],
                    }
                ],
            }
        )
        assert await svc.sessions.dual.commit(owner_in_group, grp_job, grp_ext)

        ledger_all = await svc.event_ledger.read(include_owner_private=True, now=now[0])
        assert len(ledger_all) == 1
        chronicle_key = ledger_all[0].dedupe_key
        assert ledger_all[0].visibility == "owner_private"
        assert ledger_all[0].source_module == "owner_chat"

        # Fake LLM tries to emit an owner ticket, an internal ticket, a leaked group ticket,
        # AND a planner self-memo marked visibility="public" while tainted by Owner context
        svc.ai.plan_json = json.dumps(
            {
                "current_module": "farm",
                "focus_level": "LOW",
                "mood": "excited",
                "mood_longnick": "",
                "rest_minutes": 0,
                "intents": [
                    {
                        "action_name": "farm.harvest",
                        "arguments_json": "{}",
                        "target_scope": "",
                        "delay_minutes": 10,
                        "ttl_minutes": 60,
                        "intent_note": "先去农场把成熟的草莓收掉再去挖矿",
                        "allow_desktop_care": False,
                    },
                    {
                        "action_name": "qq.speak",
                        "arguments_json": "{}",
                        "target_scope": "owner",
                        "delay_minutes": 20,
                        "ttl_minutes": 60,
                        "intent_note": "准备按照约定提前上MC挖铁矿前跟BOISER吱一声",
                        "allow_desktop_care": False,
                    },
                    {
                        "action_name": "qq.speak",
                        "arguments_json": "{}",
                        "target_scope": "onebot:40000",
                        "delay_minutes": 30,
                        "ttl_minutes": 60,
                        "intent_note": "刚刚私聊和BOISER约好两点挖铁矿，顺嘴去群里聊聊",
                        "allow_desktop_care": False,
                    },
                ],
                "agenda_changes": [],
                "ledger_posts": [
                    {
                        "key": "mc-prep",
                        "target_module": "farm",
                        "due_at_local": "2026-11-20 14:00",
                        "ttl_hours": 48,
                        "visibility": "public",
                        "summary": "明天14:00先上MC挖铁矿等BOISER一小时",
                    }
                ],
                "ledger_consumes": [chronicle_key],
            },
            ensure_ascii=False,
        )

        plan_res = await engine.trigger_plan_once(now=now[0])
        assert plan_res["intents_enqueued"] == 3
        prompt_payload = svc.ai.calls[0][1]
        assert "owner_recent_context" in prompt_payload
        assert any(
            "明天下午两点你先上MC挖铁矿" in turn
            for turn in prompt_payload["owner_recent_context"]["recent_turns"]
        )
        assert any(item["key"] == chronicle_key for item in prompt_payload["pending_ledger_events"])

        rows = {
            r["target_scope"]: json.loads(r["intent_note_json"])["note"]
            for r in await store.all("SELECT target_scope, intent_note_json FROM scheduled_intents")
        }
        assert rows["onebot:40000"] == sanitize_sensor_text(GROUP_SCRUBBED_INTENT_NOTE, settings)
        assert "BOISER" not in rows["onebot:40000"] and "挖铁矿" not in rows["onebot:40000"]
        assert "MC挖铁矿" in rows["owner:20000"]
        assert "草莓收掉" in rows[""]

        # Chronicle event was consumed, and planner memo was forced to owner_private
        post_items = await svc.event_ledger.read(include_owner_private=True, now=now[0])
        assert len(post_items) == 1
        assert post_items[0].dedupe_key == "planner:mc-prep"
        assert post_items[0].visibility == "owner_private"


async def test_ledger_system_module_and_admin_endpoints(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    app = create_app(settings, gateway_factory=lambda *_: LifeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert "event_ledger" in sys_mods
            assert sys_mods["event_ledger"]["panel"] == "event-ledger"
            assert sys_mods["event_ledger"]["status"]["state"] == "ready"

            initial = await client.get("/admin/api/life/ledger")
            assert initial.status_code == 200
            initial_body = initial.json()
            assert initial_body["counts"] == {
                "pending": 0,
                "due": 0,
                "consumed": 0,
                "expired": 0,
            }
            assert initial_body["ledger_events"] == []
            assert isinstance(initial_body["modules"], list)

            bad = await client.post("/admin/api/life/ledger", json={"summary": "   "})
            assert bad.status_code == 400

            created = await client.post(
                "/admin/api/life/ledger",
                json={
                    "summary": "明晚去MC盖二楼屋顶",
                    "target_module": "minecraft",
                    "due_in_minutes": 15,
                    "visibility": "owner_private",
                },
            )
            assert created.status_code == 200
            items = created.json()["ledger_events"]
            assert len(items) == 1
            item_id = items[0]["id"]
            assert items[0]["summary"] == "明晚去MC盖二楼屋顶"
            assert items[0]["status"] == "pending"

            assert (await client.delete("/admin/api/life/ledger/bad id!")).status_code == 400
            consumed = await client.delete(f"/admin/api/life/ledger/{item_id}")
            assert consumed.status_code == 200
            assert consumed.json()["ledger_events"][0]["status"] == "consumed"
            assert (await client.delete(f"/admin/api/life/ledger/{item_id}")).status_code == 404


async def test_planner_consumes_legacy_qq_owner_and_new_owner_chat_ledger_entries(settings, store):
    settings.owner_qq_id = "20000"
    settings.onebot_enabled = True
    settings.onebot_self_id = "10000"
    settings.onebot_mode = "ws_reverse"
    now = _ts("2026-11-19T14:00:00")

    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, LifeGateway(), FakeBili(), Sender(settings, http, Wire()))
        farm = _FarmModule(settings)
        svc.modules.register(farm)
        svc.world_state.clock = lambda: now
        await svc.proactive.initialize()
        await svc.proactive.on_connection(True)
        engine = LifeEngine(svc, clock=lambda: now)
        await engine.initialize()

        legacy_item = await svc.event_ledger.post(
            "qq_owner",
            "owner_commitment",
            "历史QQ私聊约定：明天一起看B站新番",
            dedupe_key="chronicle:legacy1",
            ttl_seconds=5 * 86400.0,
            visibility="owner_private",
            now=now - 600,
        )
        new_item = await svc.event_ledger.post(
            "owner_chat",
            "owner_commitment",
            "跨平台私聊约定：晚点收草莓",
            dedupe_key="chronicle:new1",
            ttl_seconds=5 * 86400.0,
            visibility="owner_private",
            now=now - 300,
        )
        assert legacy_item.dedupe_key == "qq_owner:chronicle:legacy1"
        assert new_item.dedupe_key == "owner_chat:chronicle:new1"

        svc.ai.plan_json = json.dumps(
            {
                "current_module": "farm",
                "focus_level": "LOW",
                "mood": "calm",
                "mood_longnick": "",
                "rest_minutes": 0,
                "intents": [],
                "agenda_changes": [],
                "ledger_posts": [],
                "ledger_consumes": [legacy_item.dedupe_key, "chronicle:new1"],
            },
            ensure_ascii=False,
        )
        await engine.trigger_plan_once(now=now)
        prompt_payload = svc.ai.calls[0][1]
        keys_seen = {item["key"] for item in prompt_payload["pending_ledger_events"]}
        assert legacy_item.dedupe_key in keys_seen
        assert new_item.dedupe_key in keys_seen

        remaining = await svc.event_ledger.read(include_owner_private=True, now=now)
        assert remaining == []
        all_rows = await store.all("SELECT dedupe_key, status FROM event_ledger")
        assert {row["dedupe_key"]: row["status"] for row in all_rows} == {
            "qq_owner:chronicle:legacy1": "consumed",
            "owner_chat:chronicle:new1": "consumed",
        }
