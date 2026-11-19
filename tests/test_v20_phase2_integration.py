"""End-to-end Phase 2 integration tests in hakimi-surf-v2."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.gemini import LIFE_PLAN_RESPONSE_SCHEMA, GeminiGateway
from hakimi.life_engine import LifeEngine, LifeEngineConfig
from hakimi.persona import PUBLIC_STATIC_CHARTER
from hakimi.roaming_logger import RoamingLogger
from tests.test_admin import TOKEN
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_social_integration import Wire, group

TZ = ZoneInfo("Asia/Shanghai")


def _stamp(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


class FakeDrive:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def sync_day(self, day: str, text: str) -> str:
        self.calls.append((day, text))
        return "diary-file-p2"


async def test_phase2_end_to_end_milestone_gatekeeper_jit_receipts_and_admin(
    settings, message, sdk_pair, monkeypatch
):
    now = _stamp("2026-11-17T15:00:00")
    cur_now = _stamp("2026-11-17T08:59:00")

    def clock():
        return cur_now

    def engine_at_test_time(service):
        service.world_state.clock = clock
        return LifeEngine(service, clock=clock)

    monkeypatch.setattr("hakimi.service.LifeEngine", engine_at_test_time)

    settings.admin_token = SecretStr(TOKEN)
    settings.onebot_enabled = True
    settings.onebot_owner_ids = set()
    settings.owner_qq_id = ""
    settings.onebot_allowed_groups = {"40000"}
    settings.group_policies = {"onebot:40000": "smart_reply"}
    settings.affection_enabled = True
    settings.roaming_enabled = False
    settings.rest_hours_enabled = True
    settings.rest_hours = "01:00-09:00"
    settings.enable_daily_longnick = True
    settings.longnick_cooldown_hours = 20

    interaction_reqs: list[dict] = []
    chat_json = json.dumps(
        {
            "abstain": False,
            "bubbles": [
                "刚在方块世界把末影龙放倒了",
                "群里大家下午也在摸鱼吗",
            ],
            "anchor": {"reply_id": None, "mentions": []},
            "sticker": {"label": None, "timing": "after_text"},
            "image_digest": None,
        },
        ensure_ascii=False,
    )
    plan_json = json.dumps(
        {
            "current_module": "bilibili_surf",
            "focus_level": "MEDIUM",
            "mood": "curious",
            "energy": 76,
            "boredom": 18,
            "mood_longnick": "放倒末影龙后来刷会儿B站",
            "intents": [],
        },
        ensure_ascii=False,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        interaction_reqs.append(body)
        resp = interaction_response()
        fmt_schema = (body.get("response_format") or {}).get("schema")
        if fmt_schema == LIFE_PLAN_RESPONSE_SCHEMA:
            resp["steps"][0]["content"][0]["text"] = plan_json
        else:
            resp["steps"][0]["content"][0]["text"] = chat_json
        return httpx.Response(200, json=resp)

    gate_holder: list[GeminiGateway] = []

    def gateway_factory(s, st):
        gw = GeminiGateway(s, st, *sdk_pair(handler))
        gate_holder.append(gw)
        return gw

    wire = Wire()
    app = create_app(settings, gateway_factory=gateway_factory, start_workers=True)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.sender.onebot = wire
        svc.s.roaming_enabled = True
        diary = RoamingLogger(svc.s, svc.store, FakeDrive(), gateway=svc.ai)
        await diary.initialize()
        svc.diary = diary

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            # 0. /healthz stays 200 and Service wiring is active
            hz = await client.get("/healthz", headers={"Authorization": ""})
            assert hz.status_code == 200
            assert hz.json() == {"status": "ok"}
            assert any(t.get_name() == "life-engine" and not t.done() for t in svc.tasks)
            assert svc.store.runtime.filters.hot_ring is svc.hot

            cfg_res = await client.put(
                "/admin/api/life",
                json=LifeEngineConfig(
                    enabled=True,
                    group_cooldown_seconds=1500,
                    group_min_recent_messages=2,
                ).model_dump(mode="json"),
            )
            assert cfg_res.status_code == 200
            cur_now = _stamp("2026-11-17T09:00:00")
            morning = await svc.life_engine.trigger_plan_once(automatic=True)
            assert morning["planning_mode"] == "morning"
            cur_now = now

            # 1. MilestoneEvent(importance="high") -> fuzzy intent + deduped diary world_event
            ev1 = await svc.world_state.publish_event(
                "minecraft",
                "ender_dragon_defeated",
                {"boss": "EnderDragon"},
                importance="high",
                now=now,
            )
            # Re-delivering the exact same event must NOT create duplicate diary rows
            await svc.life_engine.planner.on_milestone_event(ev1)
            milestone_key = svc.store.vault.digest(
                "diary-event", "milestone:" + ev1.id, "world_event"
            )
            diary_rows = await svc.store.all(
                "SELECT id FROM roaming_events WHERE event_kind='world_life' AND id=?",
                (milestone_key,),
            )
            assert len(diary_rows) == 1

            pending_rows = await svc.store.all(
                "SELECT * FROM scheduled_intents WHERE status='pending'"
            )
            assert len(pending_rows) == 1
            cold_intent_id = pending_rows[0]["id"]
            assert pending_rows[0]["target_scope"] == "onebot:40000"

            # 2. Gatekeeper skips when group is cold (0 LLM calls)
            cur_now = now + 120.0
            gk_cold = await client.post("/admin/api/life/gatekeeper")
            assert gk_cold.status_code == 200
            assert (
                await svc.store.one(
                    "SELECT status FROM scheduled_intents WHERE id=?", (cold_intent_id,)
                )
            )["status"] == "gated_skip"
            assert not any(
                PUBLIC_STATIC_CHARTER in r["system_instruction"] for r in interaction_reqs
            )

            # 3. Group becomes active in HotRing -> second milestone passes -> JIT + receipt + reply
            cur_now = now + 700.0
            grp_msg = group(message, event_id="g-seed", user_id="30001", created_at=cur_now)
            svc.hot.observe(
                group(
                    message,
                    event_id="g-m1",
                    user_id="30001",
                    text="下午好呀大家",
                    created_at=cur_now - 90.0,
                )
            )
            svc.hot.observe(
                group(
                    message,
                    event_id="g-m2",
                    user_id="30002",
                    text="摸鱼喝奶茶中",
                    created_at=cur_now - 30.0,
                )
            )
            ev2 = await svc.world_state.publish_event(
                "minecraft",
                "nether_fortress_cleared",
                {"loot": "blaze_rod"},
                importance="high",
                now=cur_now,
            )
            pending2 = await svc.store.all("SELECT * FROM scheduled_intents WHERE status='pending'")
            assert len(pending2) == 1
            warm_intent_id = pending2[0]["id"]

            cur_now = now + 800.0
            base_reply = svc.store.runtime.filters.thresholds(
                group(message, event_id="base-check", user_id="30001", addressed=False),
                now=cur_now,
            )["reply"]

            gk_warm = await client.post("/admin/api/life/gatekeeper")
            assert gk_warm.status_code == 200
            assert (
                await svc.store.one(
                    "SELECT status FROM scheduled_intents WHERE id=?", (warm_intent_id,)
                )
            )["status"] == "done"

            # Verify GeminiGateway used frozen PUBLIC_STATIC_CHARTER (Scenario.FAST_CHAT, Tier-1)
            jit_req = next(
                r for r in interaction_reqs if PUBLIC_STATIC_CHARTER in r["system_instruction"]
            )
            assert jit_req["generation_config"]["thinking_level"] == "low"
            assert "jit_proactive_intent" in json.dumps(jit_req["input"], ensure_ascii=False)

            # Verify Sender delivered 2 bubbles, recorded receipts & recent_messages
            group_sends = [c for c in wire.calls if c[0] == "send_group_msg"]
            assert len(group_sends) == 2
            snap = await svc.sessions.snapshot(grp_msg)
            assert len(snap["recent_messages"]) == 2
            receipt_rows = await svc.store.all("SELECT * FROM sent_bot_receipts")
            assert len(receipt_rows) == 2

            # attention.py recognizes subsequent user quote-reply as quoting Hakimi
            quote_back = group(
                message,
                event_id="quote-back-1",
                user_id="30001",
                addressed=False,
                reply_ids=["1"],
            )
            resolved = await svc.store.runtime.attention.resolve(quote_back)
            assert resolved.addressed is True

            # Unified cooldown updated in BOTH world_state and gateway.py filters
            scope = svc.store.session_scope(grp_msg)
            assert not await svc.world_state.group_cooldown_ok(scope, 1500.0, now=cur_now + 30.0)
            last_pro = svc.store.runtime.filters.last_proactive_spoke[
                svc.store.runtime.filters._group_scope(grp_msg)
            ]
            cooled_reply = svc.store.runtime.filters.thresholds(
                group(message, event_id="cool-check", user_id="30001", addressed=False),
                now=last_pro + 30.0,
            )["reply"]
            assert cooled_reply == round(min(0.95, base_reply + 0.12), 4)

            # 4. Planner wakeup calls life_plan, longnick.apply_life_block, diary, consumes events
            svc.s.owner_qq_id = "20000"
            plan_res = await client.post("/admin/api/life/plan")
            assert plan_res.status_code == 200
            assert plan_res.json()["current_module"] == "bilibili_surf"
            longnick_calls = [c for c in wire.calls if c[0] == "set_self_longnick"]
            assert len(longnick_calls) == 1
            world_diary = await svc.store.all(
                "SELECT * FROM roaming_events WHERE event_kind='world_life'"
            )
            plans = await svc.store.all(
                "SELECT id FROM life_plan_runs WHERE outcome IN "
                "('accepted','all_filtered','model_chose_idle','local_idle','day_rest')"
            )
            # High milestones can now wake an automatic plan independently of the
            # manual one. Each actual plan and each milestone has exactly one diary row.
            expected_entries = ["milestone:" + e.id for e in (ev1, ev2)] + [
                "life-block:" + plan["id"] for plan in plans
            ]
            assert {r["id"] for r in world_diary} == {
                svc.store.vault.digest("diary-event", entry, "world_event")
                for entry in expected_entries
            }

            # 5. /admin/api/life reflects updated world_state, intents (done), and events (consumed)
            life_view = await client.get("/admin/api/life")
            assert life_view.status_code == 200
            data = life_view.json()
            state = data["world_state"]
            assert state["current_module"] == "idle"
            assert state["experience"]["plan"]["plan_module"] == "bilibili_surf"
            assert state["experience"]["actual"]["state"] == "idle"
            assert data["world_state"]["last_proactive_at"] > 0
            intents_by_id = {row["id"]: row["status"] for row in data["intents"]}
            assert intents_by_id[cold_intent_id] == "gated_skip"
            assert intents_by_id[warm_intent_id] == "done"
            events_by_id = {row["id"]: row["consumed"] for row in data["events"]}
            assert events_by_id[ev1.id] is True
            assert events_by_id[ev2.id] is True

            # 6. /admin/api/life/world Attention Lock & /intents/{id} cancel work live
            lock_res = await client.put(
                "/admin/api/life/world",
                json={"current_module": "minecraft", "focus_level": "HIGH"},
            )
            assert lock_res.status_code == 400
            assert lock_res.json()["detail"] == "mc_task_required"
            await svc.world_state.experience.mc_snapshot("mine", cur_now)
            assert svc.world_state.proactive_suspended() is True
            await svc.world_state.experience.mc_snapshot("", cur_now)
            unlock_res = await client.put(
                "/admin/api/life/world",
                json={"focus_level": "LOW"},
            )
            assert unlock_res.status_code == 200
            assert unlock_res.json()["world_state"]["proactive_suspended"] is False

            cancel_id = await svc.world_state.enqueue_intent(
                {
                    "target_module": "qq_social",
                    "target_scope": "onebot:40000",
                    "action_type": "group_share",
                    "execute_after": cur_now + 60.0,
                    "expire_at": cur_now + 600.0,
                    "intent_note": "管理台撤销测试意图",
                },
                now=cur_now,
            )
            del_res = await client.delete(f"/admin/api/life/intents/{cancel_id}")
            assert del_res.status_code == 200
            assert (
                await svc.store.one("SELECT status FROM scheduled_intents WHERE id=?", (cancel_id,))
            )["status"] == "gated_skip"

            # Let event loop yield briefly before lifespan teardown
            await asyncio.sleep(0)
