"""Cross-platform Phase 1 tests for MemoryHub, DomainBridge, WorldState, LifeEngine, Store, DM."""

from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.models import Message
from hakimi.modules import BaseModule
from hakimi.service import Service
from hakimi.social import OwnerBinding, owner_target
from hakimi.store import _classify_usage_category
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_plugin_bus import _PluginSocket

TZ = ZoneInfo("Asia/Shanghai")
TG_BOT_ID = "tg_bot_999"
TG_OWNER_ID = "tg_owner_777"


def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


class _FakeTelegramModule(BaseModule):
    module_id = "telegram_chat"
    display_name = "Telegram 社交"
    chat_platforms = ("telegram",)

    def __init__(self, settings, *, display_name: str = "Telegram 社交") -> None:
        super().__init__(settings)
        self.display_name = display_name
        self.online = True
        self.sent: list[tuple[Message, str]] = []

    async def _raw_sensor_summary(self) -> str:
        return ""

    def authorize_chat(self, message, settings, *, bot_exempt: bool = False) -> bool:
        return message.platform == "telegram" and message.bot_id == TG_BOT_ID

    def chat_connected(self, platform: str) -> bool:
        return platform == "telegram" and self.online

    async def send_chat(self, message: Message, text: str, *, before_send):
        if not await before_send():
            raise RuntimeError("revoked")
        self.sent.append((message, text))
        return f"tg-msg-{len(self.sent)}"


async def _seed_chat_session(
    svc: Service,
    target_msg: Message,
    *,
    now: float,
    summary: str,
    user_text: str,
    assistant_text: str,
    enabled: bool = True,
) -> None:
    async with svc.sessions.database(target_msg) as (db, key):
        await db.execute("UPDATE session_meta SET enabled=?", (1 if enabled else 0,))
        await db.execute(
            "UPDATE rolling_state SET summary=?",
            (svc.sessions.vault.seal(key, summary),),
        )
        for item in (
            {"role": "user", "text": user_text, "created_at": now - 600.0},
            {"role": "assistant", "text": assistant_text, "created_at": now - 580.0},
        ):
            await db.execute(
                "INSERT INTO recent_messages(payload, expires) VALUES (?, ?)",
                (svc.sessions.vault.seal(key, item), now + 86400.0),
            )


async def test_memory_hub_pure_social_owner_bindings_writes_and_planner_recall(settings):
    """MemoryHub works with pure social_owner_bindings and routes planner recall to Telegram."""
    now = _ts("2026-11-25T15:00:00")
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_qq_id": "",
            "owner_delivery_platform": "telegram",
            "proactive_dm_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=TG_BOT_ID,
                    user_id=TG_OWNER_ID,
                    notebook=True,
                    mirror=True,
                )
            },
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        hub = svc.memory_hub
        hub.clock = lambda: now
        svc.identity.clock = lambda: now

        assert hub._has_configured_owner() is True

        # 1. record_quick_recap succeeds without owner_qq_id when social_owner_bindings is set
        assert (
            hub.record_quick_recap("voice_companion", "不该写入", now=now, private_owner=False)
            is False
        )
        assert (
            hub.record_quick_recap(
                "voice_companion",
                "刚才和BOISER在Telegram语音复盘新功能",
                now=now,
                private_owner=True,
            )
            is True
        )
        recaps = svc.identity.owner_other_surface_recent("telegram", now=now)
        assert any(
            r["platform"] == "voice_companion" and "复盘新功能" in r["summary"] for r in recaps
        )

        # 2. record_experience routes owner_private recap + chronicle to Telegram owner DualMemory
        pub_res = await hub.record_experience(
            "demo_pub",
            "公开巡逻见闻",
            visibility="public",
            write_recap=True,
            chronicle_topic="公开不应入私密",
            now=now,
        )
        assert pub_res.recap_written is False
        assert pub_res.chronicle_written is False

        priv_res = await hub.record_experience(
            "steam_watch",
            "在星露谷一起收了草莓",
            visibility="owner_private",
            event_type="session_recap",
            dedupe_key="tg-exp-1",
            write_recap=True,
            recap_platform="steam_watch",
            write_ledger=True,
            chronicle_topic="跨端游玩:星露谷",
            chronicle_tags=["星露谷"],
            now=now,
        )
        assert priv_res.ledger_event_id is not None
        assert priv_res.recap_written is True
        assert priv_res.chronicle_written is True

        tg_owner = owner_target(svc.s, "telegram", now, "tg-check")
        assert tg_owner is not None
        dual_view = await svc.sessions.dual.view(tg_owner)
        assert len(dual_view["entries"]) == 1
        assert dual_view["entries"][0]["topic"] == "跨端游玩:星露谷"
        assert "收了草莓" in dual_view["entries"][0]["text"]

        # 3. recall_context(mode="planner") reads Telegram Owner recent chat session
        await _seed_chat_session(
            svc,
            tg_owner,
            now=now,
            summary="Telegram 约好明晚看新番",
            user_text="明晚九点一起看新番",
            assistant_text="没问题，九点准时蹲守！",
        )
        planner_ctx = await hub.recall_context(None, "", mode="planner", now=now)
        assert "Telegram 约好明晚看新番" in planner_ctx["rolling_summary"]
        assert any("明晚九点一起看新番" in t for t in planner_ctx["recent_turns"])

        # Official qq OpenID audience must never gain owner planner recall
        fake_qq_msg = Message(
            platform="qq",
            bot_id="qq-app",
            event_id="qq-ev-1",
            user_id=TG_OWNER_ID,
            kind="private",
            conversation_id=TG_OWNER_ID,
            text="hi",
            addressed=True,
            created_at=now,
        )
        assert await hub.recall_context(fake_qq_msg, "", mode="planner", now=now) == {}

        # Without owner_qq_id AND without social_owner_bindings, owner writes are blocked
        svc.s.social_owner_bindings = {}
        assert hub._has_configured_owner() is False
        assert (
            hub.record_quick_recap(
                "voice_companion",
                "无Owner不应写入",
                now=now,
                private_owner=True,
            )
            is False
        )
        no_owner_res = await hub.record_experience(
            "steam_watch",
            "无Owner不应写入",
            visibility="owner_private",
            write_recap=True,
            chronicle_topic="不应写入",
            now=now,
        )
        assert no_owner_res.recap_written is False
        assert no_owner_res.chronicle_written is False


async def test_memory_hub_planner_recall_respects_onebot_default_and_explicit_audience(settings):
    """Planner recall keeps OneBot default and honors explicit audience overrides."""
    now = _ts("2026-11-25T16:00:00")
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "onebot_owner_ids": {"20000"},
            "owner_delivery_platform": "onebot",
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        hub = svc.memory_hub
        hub.clock = lambda: now

        ob_owner = owner_target(svc.s, "onebot", now, "ob-ctx")
        tg_owner = owner_target(svc.s, "telegram", now, "tg-ctx")
        assert ob_owner is not None and tg_owner is not None

        await _seed_chat_session(
            svc,
            ob_owner,
            now=now,
            summary="OneBot 私聊摘要",
            user_text="QQ 这边刚聊完作业",
            assistant_text="加油写完喵",
        )
        await _seed_chat_session(
            svc,
            tg_owner,
            now=now,
            summary="Telegram 私聊摘要",
            user_text="TG 这边刚聊完晚饭",
            assistant_text="晚饭记得吃热乎的",
        )

        default_ctx = await hub.recall_context(None, "", mode="planner", now=now)
        assert "OneBot 私聊摘要" in default_ctx["rolling_summary"]
        assert any("QQ 这边刚聊完作业" in t for t in default_ctx["recent_turns"])

        explicit_ctx = await hub.recall_context(tg_owner, "", mode="planner", now=now)
        assert "Telegram 私聊摘要" in explicit_ctx["rolling_summary"]
        assert any("TG 这边刚聊完晚饭" in t for t in explicit_ctx["recent_turns"])


async def test_domain_bridge_world_event_owner_recap_with_pure_social_owner_bindings(settings):
    """PluginBus world.event owner_recap works when only social_owner_bindings is configured."""
    now = _ts("2026-11-25T17:00:00")
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_qq_id": "",
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        svc.plugin_bus.clock = lambda: now
        svc.memory_hub.clock = lambda: now
        svc.identity.clock = lambda: now

        await svc.plugin_bus.create_plugin("steam_watch", "游戏观测", ["sensors"])
        sock = _PluginSocket()
        task = asyncio.create_task(
            svc.plugin_bus.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                plugin_id="steam_watch",
            )
        )
        try:
            await sock.push(
                {
                    "type": "plugin.hello",
                    "protocol_version": 1,
                    "display_name": "游戏观测",
                    "scopes": ["sensors"],
                }
            )
            await sock.recv_until(lambda m: m["type"] == "plugin.ready")
            await sock.push(
                {
                    "type": "world.event",
                    "id": "we-tg-1",
                    "event_type": "session_recap",
                    "business_key": "tg-recap-1",
                    "visibility": "public",
                    "owner_recap": True,
                    "chronicle_topic": "纯TG绑定的跨端回流",
                    "summary": "和BOISER打完一局星露谷联机",
                }
            )
            ack = (await sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
            assert ack["accepted"] is True

            # Without owner_qq_id AND without social_owner_bindings, owner_recap is skipped
            svc.s.social_owner_bindings = {}
            await sock.push(
                {
                    "type": "world.event",
                    "id": "we-tg-2",
                    "event_type": "session_recap",
                    "business_key": "tg-recap-2",
                    "visibility": "public",
                    "owner_recap": True,
                    "summary": "无绑定时不应写入owner_recap",
                }
            )
            ack2 = (await sock.recv_until(lambda m: m.get("id") == "we-tg-2"))[-1]
            assert ack2["accepted"] is True
        finally:
            await sock.close(1000)
            await asyncio.wait_for(task, 2.0)

        recaps = svc.identity.owner_other_surface_recent("telegram", now=now)
        assert any(r["platform"] == "steam_watch" and "星露谷联机" in r["summary"] for r in recaps)
        assert not any("无绑定时不应写入" in r["summary"] for r in recaps)

        # Restore binding to inspect Telegram owner's DualMemory view
        svc.s.social_owner_bindings = {
            "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
        }
        tg_owner = owner_target(svc.s, "telegram", now, "tg-bridge-check")
        assert tg_owner is not None
        entries = (await svc.sessions.dual.view(tg_owner))["entries"]
        assert any(e["topic"] == "纯TG绑定的跨端回流" for e in entries)


async def test_world_state_recover_actions_preserves_social_and_qq_owner_digest_and_handles_null(
    settings, store
):
    """recover_actions keeps awaiting social/qq digests executing and marks others uncertain."""
    now = _ts("2026-11-25T21:00:00")
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    ws = svc.world_state
    ws.clock = lambda: now
    await ws.initialize()
    try:
        async with store.transaction():
            # 1. social.owner_digest awaiting delivery with pending DM -> must stay executing
            await store.db.execute(
                "INSERT INTO pending_owner_dms"
                "(id,scope,day,kind,payload,created,expires_at,status) "
                "VALUES ('dig-social','scope-tg','2026-11-25','digest',NULL,?,?,'pending')",
                (now, now + 3600),
            )
            await store.db.execute(
                "INSERT INTO owner_digest_intents(intent_id,digest_id,scheduled_for) "
                "VALUES ('int-social-ok','dig-social',?)",
                (now,),
            )
            await store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,execute_after,expire_at,target_module,target_scope,action_type,"
                "action_name,status,result_reason,updated_at,claimed_at,handler_started"
                ") VALUES ("
                "'int-social-ok',?,?,?,'telegram_chat','boundowner:tg','social.owner_digest',"
                "'social.owner_digest','executing','owner_digest_awaiting_delivery',?,?,1)",
                (now, now, now + 3600, now, now),
            )

            # 2. qq.owner_digest awaiting delivery with pending DM -> still stays executing
            await store.db.execute(
                "INSERT INTO pending_owner_dms"
                "(id,scope,day,kind,payload,created,expires_at,status) "
                "VALUES ('dig-qq','scope-qq','2026-11-25','digest',NULL,?,?,'pending')",
                (now, now + 3600),
            )
            await store.db.execute(
                "INSERT INTO owner_digest_intents(intent_id,digest_id,scheduled_for) "
                "VALUES ('int-qq-ok','dig-qq',?)",
                (now,),
            )
            await store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,execute_after,expire_at,target_module,target_scope,action_type,"
                "action_name,status,result_reason,updated_at,claimed_at,handler_started"
                ") VALUES ("
                "'int-qq-ok',?,?,?,'qq_social','owner:20000','qq.owner_digest',"
                "'qq.owner_digest','executing','owner_digest_awaiting_delivery',?,?,1)",
                (now, now, now + 3600, now, now),
            )

            # 3. social.owner_digest WITHOUT pending DM -> must become uncertain
            await store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,execute_after,expire_at,target_module,target_scope,action_type,"
                "action_name,status,result_reason,updated_at,claimed_at,handler_started"
                ") VALUES ("
                "'int-social-orphan',?,?,?,'telegram_chat','boundowner:tg','social.owner_digest',"
                "'social.owner_digest','executing','owner_digest_awaiting_delivery',?,?,1)",
                (now, now, now + 3600, now, now),
            )

            # 4. Legacy executing intent with NULL action_name -> must become uncertain (NULL-safe)
            await store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,execute_after,expire_at,target_module,target_scope,action_type,"
                "action_name,status,result_reason,updated_at,claimed_at,handler_started"
                ") VALUES ("
                "'int-null-action',?,?,?,'qq_social','owner:20000','legacy_action',"
                "NULL,'executing','owner_digest_awaiting_delivery',?,?,1)",
                (now, now, now + 3600, now, now),
            )

            # 5. Executing telegram.speak without receipts -> must become uncertain
            await store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,execute_after,expire_at,target_module,target_scope,action_type,"
                "action_name,status,result_reason,updated_at,claimed_at,handler_started"
                ") VALUES ("
                "'int-tg-speak',?,?,?,'telegram_chat','boundowner:tg','speak',"
                "'telegram.speak','executing','',?,?,1)",
                (now, now, now + 3600, now, now),
            )

        await ws.recover_actions()

        rows = {
            r["id"]: (r["status"], r["result_reason"])
            for r in await store.all("SELECT id,status,result_reason FROM scheduled_intents")
        }
        assert rows["int-social-ok"] == ("executing", "owner_digest_awaiting_delivery")
        assert rows["int-qq-ok"] == ("executing", "owner_digest_awaiting_delivery")
        assert rows["int-social-orphan"] == ("uncertain", "interrupted_without_final_receipt")
        assert rows["int-null-action"] == ("uncertain", "interrupted_without_final_receipt")
        assert rows["int-tg-speak"] == ("uncertain", "interrupted_without_final_receipt")
    finally:
        await svc.stop()


async def test_life_engine_recent_completed_intents_includes_boundowner_and_excludes_groups(
    settings, store
):
    """_recent_completed_intents includes '', owner:%, boundowner:% and excludes groups."""
    now = _ts("2026-11-25T10:00:00")
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    svc.world_state.clock = lambda: now
    engine = LifeEngine(svc, clock=lambda: now)
    await engine.initialize()
    try:
        entries = [
            ("i-global", "", "bili.patrol", '{"note": "刷了一圈科技区"}', now - 100),
            ("i-owner", "owner:20000", "qq.speak", '{"note": "给QQ主人打招呼"}', now - 200),
            (
                "i-bound",
                "boundowner:deadbeef",
                "telegram.speak",
                '{"note": "给TG主人发早安问候"}',
                now - 50,
            ),
            (
                "i-tg-group",
                "telegram:-100123456",
                "telegram.speak",
                '{"note": "TG群里聊公开见闻"}',
                now - 40,
            ),
            (
                "i-qq-group",
                "onebot:40000",
                "qq.speak",
                '{"note": "QQ群里接话"}',
                now - 30,
            ),
        ]
        async with store.transaction():
            for row_id, scope, action, note_json, updated_at in entries:
                await store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,execute_after,expire_at,target_module,target_scope,"
                    "action_type,action_name,intent_note_json,status,updated_at"
                    ") VALUES (?,?,?,?,?,?,?,?,?,'done',?)",
                    (
                        row_id,
                        updated_at,
                        updated_at,
                        updated_at + 600,
                        "telegram_chat",
                        scope,
                        action,
                        action,
                        note_json,
                        updated_at,
                    ),
                )

        items = await engine.planner._recent_completed_intents(now)
        actions = [it["action"] for it in items]
        notes = [it["note"] for it in items]
        assert actions == ["telegram.speak", "bili.patrol", "qq.speak"]
        assert "给TG主人发早安问候" in notes
        assert "刷了一圈科技区" in notes
        assert "给QQ主人打招呼" in notes
        assert all("群里" not in n for n in notes)
    finally:
        await svc.stop()


@pytest.mark.parametrize(
    "lane,work_kind,subject,expected",
    [
        ("proactive", "qq.speak", "onebot:g1", "qq_proactive"),
        ("proactive", "telegram.speak", "telegram:g1", "qq_proactive"),
        ("proactive", "social.speak", "discord:g1", "qq_proactive"),
        ("proactive", "qq.owner_digest", "onebot:owner", "owner_digest"),
        ("proactive", "social.owner_digest", "telegram:owner", "owner_digest"),
        ("proactive", "unknown.work", "other:sub", "proactive_other"),
    ],
)
def test_store_classify_usage_category_cross_platform(lane, work_kind, subject, expected):
    assert _classify_usage_category(lane, work_kind, subject) == expected


async def test_store_usage_overview_aggregates_telegram_speak_and_social_owner_digest(store):
    today = store.local_now().date().isoformat()
    async with store.transaction():
        for row_id, subject, amount, kind in (
            ("u-qq-speak", "onebot:g1", 300, "qq.speak"),
            ("u-tg-speak", "telegram:g1", 450, "telegram.speak"),
            ("u-qq-dig", "onebot:owner", 200, "qq.owner_digest"),
            ("u-tg-dig", "telegram:owner", 550, "social.owner_digest"),
        ):
            await store.db.execute(
                "INSERT INTO ledger VALUES (?,?,?,'proactive',?,?,'settled')",
                (row_id, today, subject, amount, amount),
            )
            await store.db.execute(
                "INSERT INTO ledger_work VALUES (?,?,?,?,?)",
                (row_id, kind, f"w-{row_id}", kind, f"r-{row_id}"),
            )

    overview = await store.usage_overview()
    by_cat = {c["category"]: c for c in overview["categories"]}
    assert by_cat["qq_proactive"] == {
        "category": "qq_proactive",
        "lane": "proactive",
        "amount": 750,
        "calls": 2,
    }
    assert by_cat["owner_digest"] == {
        "category": "owner_digest",
        "lane": "proactive",
        "amount": 750,
        "calls": 2,
    }
    assert sum(c["amount"] for c in overview["categories"]) == sum(
        row["amount"] for row in overview["lanes"]
    )


async def test_proactive_dm_on_connection_telegram_isolation_and_platform_label(settings, store):
    """OwnerProactive tracks Telegram state, uses display_name in alerts, and ignores OneBot."""
    clock = [_ts("2026-11-25T12:00:00")]
    s = settings.model_copy(
        update={
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_qq_id": "",
            "owner_delivery_platform": "telegram",
            "proactive_dm_enabled": True,
            "proactive_ws_alert_after_seconds": 300,
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    tg_mod = _FakeTelegramModule(s, display_name="Telegram 社交")
    svc.modules.register(tg_mod)
    dm = svc.proactive
    dm.clock = lambda: clock[0]
    await dm.initialize()
    try:
        # 1. Telegram comes online -> owner_dm_state updated and wakeup set
        dm.wakeup.clear()
        await dm.on_connection(True, platform="telegram")
        assert dm.wakeup.is_set()
        row = await store.one(
            "SELECT ever_online,online,disconnected_at FROM owner_dm_state WHERE scope=?",
            (dm.scope,),
        )
        assert (row["ever_online"], row["online"], row["disconnected_at"]) == (1, 1, 0)

        # 2. Mismatched OneBot disconnect/reconnect is completely ignored
        dm.wakeup.clear()
        await dm.on_connection(False, platform="onebot")
        clock[0] += 600
        await dm.on_connection(True, platform="onebot")
        assert not dm.wakeup.is_set()
        row = await store.one(
            "SELECT ever_online,online,disconnected_at FROM owner_dm_state WHERE scope=?",
            (dm.scope,),
        )
        assert (row["ever_online"], row["online"], row["disconnected_at"]) == (1, 1, 0)
        assert not await store.all("SELECT * FROM pending_owner_dms")

        # 3. Telegram disconnect -> long outage -> reconnect queues platform-aware alert
        await dm.on_connection(False, platform="telegram")
        clock[0] += 360
        await dm.on_connection(True, platform="telegram")
        rows = await store.all("SELECT id,kind,payload FROM pending_owner_dms")
        assert len(rows) == 1
        assert rows[0]["kind"] == "alert"
        alert_text = store.vault.open(rows[0]["id"], rows[0]["payload"])["text"]
        assert "Telegram 社交" in alert_text
        assert "NapCat" not in alert_text

        # 4. Backwards-compatible platform=None and fallback label when display_name is blank
        tg_mod.display_name = ""
        clock[0] += s.proactive_alert_cooldown_seconds + 10
        await dm.on_connection(False)
        clock[0] += 360
        await dm.on_connection(True)
        rows = await store.all("SELECT id,kind,payload FROM pending_owner_dms ORDER BY created")
        assert len(rows) == 2
        fallback_text = store.vault.open(rows[1]["id"], rows[1]["payload"])["text"]
        assert "BOISER，telegram 连接已恢复喵" in fallback_text
    finally:
        await svc.stop()


async def test_proactive_dm_onebot_ignores_telegram_events_and_keeps_napcat_label(settings, store):
    """When owner_delivery_platform=='onebot', Telegram events are ignored and alerts say NapCat."""
    clock = [_ts("2026-11-25T13:00:00")]
    s = settings.model_copy(
        update={
            "owner_qq_id": "20000",
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "owner_delivery_platform": "onebot",
            "proactive_ws_alert_after_seconds": 300,
        }
    )
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    dm = svc.proactive
    dm.clock = lambda: clock[0]
    svc.life_engine = LifeEngine(svc, clock=dm.clock)
    await svc.life_engine.initialize()
    await dm.initialize()
    try:
        await dm.on_connection(True, platform="onebot")
        await dm.on_connection(False, platform="telegram")
        row = await store.one(
            "SELECT online,disconnected_at FROM owner_dm_state WHERE scope=?",
            (dm.scope,),
        )
        assert (row["online"], row["disconnected_at"]) == (1, 0)

        await dm.on_connection(False, platform="onebot")
        clock[0] += 360
        await dm.on_connection(True, platform="onebot")
        rows = await store.all("SELECT id,payload FROM pending_owner_dms")
        assert len(rows) == 1
        text = store.vault.open(rows[0]["id"], rows[0]["payload"])["text"]
        assert "NapCat 连接已恢复" in text
    finally:
        await svc.stop()


async def test_app_onebot_on_state_tags_platform_onebot_and_isolates_telegram(settings):
    """create_app wires onebot.on_state with platform='onebot' so OneBot flaps skip Telegram."""
    ws_token = SecretStr("a" * 40)
    tg_settings = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": ws_token,
            "owner_delivery_platform": "telegram",
            "proactive_dm_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    app = create_app(tg_settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        assert app.state.onebot.on_state is not None

        await svc.proactive.on_connection(True, platform="telegram")
        await app.state.onebot.on_state(False)
        await app.state.onebot.on_state(True)

        row = await svc.store.one(
            "SELECT ever_online,online,disconnected_at FROM owner_dm_state WHERE scope=?",
            (svc.proactive.scope,),
        )
        assert (row["ever_online"], row["online"], row["disconnected_at"]) == (1, 1, 0)
        assert not await svc.store.all("SELECT * FROM pending_owner_dms")

    # And when owner_delivery_platform is 'onebot', onebot.on_state updates OneBot owner_dm_state
    ob_settings = settings.model_copy(
        update={
            "data_dir": settings.data_dir / "ob_sub",
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": ws_token,
            "owner_qq_id": "20000",
            "owner_delivery_platform": "onebot",
        }
    )
    app2 = create_app(ob_settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app2.router.lifespan_context(app2):
        svc2 = app2.state.service
        await app2.state.onebot.on_state(True)
        row2 = await svc2.store.one(
            "SELECT ever_online,online,disconnected_at FROM owner_dm_state WHERE scope=?",
            (svc2.proactive.scope,),
        )
        assert (row2["ever_online"], row2["online"], row2["disconnected_at"]) == (1, 1, 0)
