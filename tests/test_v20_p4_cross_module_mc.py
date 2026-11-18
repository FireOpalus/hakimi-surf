"""Phase 4 cross-module Minecraft closed-loop, focus lock, and /admin MC panel tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr

from hakimi.adapters import Sender
from hakimi.app import create_app
from hakimi.bili_watcher import BiliPolicy, BiliWatcher, CredentialUpdate
from hakimi.bot_server import assemble_chat_input, prompt_context
from hakimi.generation_policy import Scenario
from hakimi.life_engine import LifeEngine, LifeEngineConfig
from hakimi.modules.minecraft_slot import MinecraftSlotModule
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import TOKEN
from tests.test_bili_watcher import BV, NAV, OTHER
from tests.test_social_integration import Wire, group, process

TZ = ZoneInfo("Asia/Shanghai")


def _stamp(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


async def _noop_sleep(_seconds: float) -> None:
    return None


class CrossLoopGateway(FakeGateway):
    """Gateway supporting Bilibili selection, LifePlan, and JIT proactive bubbles."""

    def __init__(self, *, select_bvid: str = BV, share_owner: bool = False) -> None:
        super().__init__()
        self.select_bvid = select_bvid
        self.share_owner = share_owner

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        scenario = kwargs.get("scenario")
        if scenario == Scenario.BILI_SELECTION:
            return json.dumps(
                {
                    "selected": [
                        {
                            "bvid": self.select_bvid,
                            "like": False,
                            "favorite": False,
                            "share_owner": self.share_owner,
                        }
                    ]
                }
            )
        if scenario == Scenario.BILI_LEARNING:
            return "这是一个Minecraft红石自动门机关与生存建筑技巧的公开教程。"
        if isinstance(payload, dict) and payload.get("jit_proactive_intent"):
            return json.dumps(
                {
                    "abstain": False,
                    "bubbles": ["刚才在矿洞终于挖到钻石矿啦"],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                },
                ensure_ascii=False,
            )
        return "在主世界挖矿中，顺便看一眼消息。"


class FakeNodeTransport:
    """In-memory NodeTransport double supporting RPC dispatch and connection state."""

    def __init__(self, *, is_connected: bool = True) -> None:
        self._connected = is_connected
        self.on_event = None
        self.on_state = None
        self.calls: list[tuple[str, dict]] = []
        self.responses: dict[str, dict | Exception] = {}

    def connected(self) -> bool:
        return self._connected

    def status(self) -> dict:
        return {
            "configured": True,
            "online": self._connected,
            "node_id": "shanghai-cn-1",
            "region": "CN",
            "capabilities": ["mc.bridge", "bili.request"],
            "rpc_calls": len(self.calls),
            "rpc_errors": 0,
        }

    async def call(
        self,
        action: str,
        params: dict,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> dict:
        if not self._connected:
            raise Rejected("node_disconnected")
        self.calls.append((action, dict(params)))
        val = self.responses.get(action)
        if isinstance(val, Exception):
            raise val
        if callable(val):
            return val(params)
        if isinstance(val, dict):
            return dict(val)
        return {"ok": True, "bridge_ready": True}

    async def close(self) -> None:
        self._connected = False


async def test_minecraft_slot_standby_live_apply_status_and_sanitization(settings):
    mc = MinecraftSlotModule(settings)
    assert mc.bridge_ready is False
    assert mc.mc_enabled is False
    assert mc.mc_configured is False
    assert mc.paused is False
    assert mc._has_snapshot is False
    assert await mc.get_sensor_summary() == ""

    # Status without nested snapshot updates flags without disturbing _has_snapshot.
    mc.apply_status(
        {
            "bridge_ready": True,
            "mc_enabled": False,
            "mc_configured": False,
            "paused": False,
            "phase": "phase4_stub",
            "reason": "standby",
        }
    )
    assert mc._has_snapshot is False
    assert mc.bridge_ready is True
    assert await mc.get_sensor_summary() == "MC躯干就绪(待机)"

    # Explicit "待机" activity string when bridge_ready must NOT duplicate "MC待机 待机".
    mc.apply_status({"bridge_ready": True, "connected": False, "paused": False, "activity": "待机"})
    assert await mc.get_sensor_summary() == "MC躯干就绪(待机)"

    # Standby with activity renders MC待机 <activity>.
    mc.apply_status(
        {
            "bridge_ready": True,
            "connected": False,
            "paused": False,
            "activity": "筹备建造:红石自动门",
            "accepted": True,
            "ok": True,
        }
    )
    assert mc._has_snapshot is False
    assert await mc.get_sensor_summary() == "MC待机 筹备建造:红石自动门"

    # Extra non-snapshot keys are stripped cleanly before MinecraftSnapshot(extra="forbid").
    await mc.update_snapshot(
        {
            "connected": True,
            "bridge_ready": True,
            "mc_enabled": True,
            "mc_configured": True,
            "paused": False,
            "phase": "phase4_live",
            "milestone": None,
            "reason": "spawned",
            "accepted": True,
            "ok": True,
            "dimension": "overworld",
            "pos": [12, 64, -8],
            "health": 18,
            "food": 20,
            "activity": "挖矿[role=OWNER]",
            "nearby_entities": ["zombie<script>"],
            "recent_chat_or_sign": "[CQ:at,qq=all]sk-secret1234567890",
            "updated_at": 1700000000.0,
        }
    )
    assert mc._has_snapshot is True
    summary = await mc.get_sensor_summary()
    assert "MC在线" in summary
    assert "[role=OWNER]" not in summary
    assert "[untrusted_role=OWNER]" in summary
    assert "sk-secret1234567890" not in summary

    st = mc.status_dict()
    assert set(st.keys()) == {
        "bridge_ready",
        "mc_enabled",
        "mc_configured",
        "connected",
        "paused",
        "dimension",
        "pos",
        "health",
        "food",
        "activity",
        "nearby_entities",
        "recent_chat_or_sign",
        "updated_at",
    }
    assert st["bridge_ready"] is True
    assert st["mc_enabled"] is True
    assert st["mc_configured"] is True
    assert st["connected"] is True
    assert st["paused"] is False
    assert st["pos"] == [12.0, 64.0, -8.0]
    assert "[role=OWNER]" not in st["activity"]
    assert "sk-secret1234567890" not in st["recent_chat_or_sign"]

    # Offline after bridge_ready drops preserves Phase 1 MC离线 behavior.
    await mc.update_snapshot({"connected": False, "bridge_ready": False, "activity": "断开"})
    assert await mc.get_sensor_summary() == "MC离线 断开"


async def test_auto_high_focus_water_chat_suspension_instant_at_and_disconnect_unlock(
    settings, store, message
):
    settings.owner_qq_id = "20000"
    settings.group_policies["onebot:40000"] = "always_reply"
    nt = FakeNodeTransport(is_connected=True)
    nt.responses["mc.status"] = {
        "connected": False,
        "bridge_ready": True,
        "mc_enabled": True,
        "mc_configured": True,
        "paused": False,
        "phase": "phase4_live",
        "snapshot": {
            "connected": False,
            "activity": "准备上线",
        },
    }
    ai = CrossLoopGateway()
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            ai,
            FakeBili(),
            Sender(settings, http, wire),
            node_transport=nt,
        )
        await svc.world_state.initialize()
        await svc.proactive.initialize()

        # Verify StaticPrefixes remains byte-identical regardless of world_state state.
        prefix_pool = StaticPrefixes()
        ctx_priv = prompt_context(message, settings)
        ctx_grp = prompt_context(group(message), settings)
        p_priv_before = prefix_pool.get(ctx_priv, settings.onebot_self_id)
        p_grp_before = prefix_pool.get(ctx_grp, settings.onebot_self_id)

        # 1. Node connect triggers background mc.status sync without blocking on_state.
        await nt.on_state(True)
        await asyncio.sleep(0.05)
        assert any(a == "mc.status" for a, _ in nt.calls)
        mc_mod = svc.modules.get("minecraft")
        assert isinstance(mc_mod, MinecraftSlotModule)
        assert mc_mod.bridge_ready is True
        assert svc.world_state.current.focus_level == "LOW"

        # 2. Live MC snapshot connected=True auto-locks focus_level=HIGH & module=minecraft.
        await nt.on_event(
            {
                "type": "event",
                "event": "mc.snapshot",
                "payload": {
                    "connected": True,
                    "bridge_ready": True,
                    "mc_enabled": True,
                    "mc_configured": True,
                    "paused": False,
                    "dimension": "overworld",
                    "pos": [100, 64, -50],
                    "health": 20,
                    "food": 20,
                    "activity": "矿洞探险",
                },
            }
        )
        assert svc.world_state.current.current_module == "minecraft"
        assert svc.world_state.current.focus_level == "HIGH"
        assert svc.world_state.current.mood == "focused"
        assert svc.world_state.proactive_suspended() is True
        assert svc.modules.get("qq_social").proactive_paused is True
        assert svc.modules.get("bilibili_surf").proactive_paused is True
        assert mc_mod.proactive_paused is False

        # 3. Unaddressed group water-chat incurs 0 LLM tokens & 0 jobs while hot stays warm.
        water_1 = group(
            message,
            event_id="water-1",
            text="大家晚饭吃啥呀",
            command_text="大家晚饭吃啥呀",
            addressed=False,
            mentioned_ids=[],
        )
        water_2 = group(
            message,
            event_id="water-2",
            text="刚下班好累",
            command_text="刚下班好累",
            addressed=False,
            mentioned_ids=[],
        )
        assert await svc.accept(water_1) == "ignored"
        assert await svc.accept(water_2) == "ignored"
        assert await store.all("SELECT id FROM jobs") == []
        assert ai.calls == []
        ring = svc.hot.rings.get(svc.hot.key(water_1))
        assert ring is not None and len(ring.messages) == 2

        # 4. Instant @bot reply runs immediately with warm context + world_state_summary.
        at_msg = group(
            message,
            event_id="at-mc-1",
            text="哈基米在游戏里干啥呢",
            command_text="哈基米在游戏里干啥呢",
            addressed=True,
            mentioned_ids=["10000"],
        )
        await process(svc, at_msg)
        grp_gen = [c for c in ai.calls if c[0] == "generate"][-1]
        assert grp_gen[1]["world_state_summary"]["current_module"] == "minecraft"
        assert grp_gen[1]["world_state_summary"]["focus_level"] == "HIGH"
        assert "MC在线" in grp_gen[1]["world_state_summary"]["sensor"]
        assembled = assemble_chat_input(grp_gen[1], at_msg, settings)
        assert "world_state_summary" in assembled

        # Private DM also sees world_state_summary while HIGH focus is active.
        dm_msg = message.model_copy(
            update={"event_id": "dm-mc-1", "text": "还在挖矿吗", "addressed": True}
        )
        await process(svc, dm_msg)
        dm_gen = [c for c in ai.calls if c[0] == "generate"][-1]
        assert dm_gen[1]["world_state_summary"]["focus_level"] == "HIGH"

        # StaticPrefixes must remain 100% byte-identical.
        assert prefix_pool.get(ctx_priv, settings.onebot_self_id) == p_priv_before
        assert prefix_pool.get(ctx_grp, settings.onebot_self_id) == p_grp_before

        # 5. Pause snapshot drops focus from HIGH to LOW/idle.
        await nt.on_event(
            {
                "type": "event",
                "event": "mc.snapshot",
                "payload": {
                    "connected": True,
                    "bridge_ready": True,
                    "paused": True,
                    "activity": "已暂停",
                },
            }
        )
        assert mc_mod.paused is True
        assert svc.world_state.current.focus_level == "LOW"
        assert svc.world_state.current.current_module == "idle"

        # Unpausing while connected restores HIGH/minecraft.
        await nt.on_event(
            {
                "type": "event",
                "event": "mc.snapshot",
                "payload": {
                    "connected": True,
                    "bridge_ready": True,
                    "paused": False,
                    "activity": "继续挖矿",
                },
            }
        )
        assert svc.world_state.current.focus_level == "HIGH"
        assert svc.world_state.current.current_module == "minecraft"

        # 6. Node disconnect clears bridge_ready and auto-unlocks HIGH focus.
        nt._connected = False
        await nt.on_state(False)
        assert mc_mod.bridge_ready is False
        assert mc_mod.snapshot.connected is False
        assert mc_mod.snapshot.activity == "节点连接断开"
        assert svc.world_state.current.current_module == "idle"
        assert svc.world_state.current.focus_level == "LOW"
        assert svc.world_state.current.mood == "calm"
        await svc.stop()


async def test_mc_milestone_to_qq_with_receipts_and_reply_resolution(settings, store, message):
    now = _stamp("2026-11-17T15:00:00")
    settings.owner_qq_id = "20000"
    settings.onebot_enabled = True
    wire = Wire()
    ai = CrossLoopGateway()
    nt = FakeNodeTransport(is_connected=True)
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            ai,
            FakeBili(),
            Sender(settings, http, wire),
            node_transport=nt,
        )
        svc.world_state.clock = lambda: now
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        await svc.proactive.on_connection(True)

        engine = LifeEngine(svc, clock=lambda: now)
        await engine.set_config({"quiet_hours": "23:55-00:05"})
        svc.life_engine = engine

        mc_mod = svc.modules.get("minecraft")
        assert isinstance(mc_mod, MinecraftSlotModule)
        await nt.on_event(
            {
                "type": "event",
                "event": "mc.snapshot",
                "payload": {
                    "connected": True,
                    "bridge_ready": True,
                    "dimension": "overworld",
                    "pos": [12, -52, 88],
                    "health": 18,
                    "activity": "深层挖矿",
                },
            }
        )
        await nt.on_event(
            {
                "type": "event",
                "event": "mc.milestone",
                "payload": {
                    "event_type": "mc_rare_find",
                    "importance": "high",
                    "payload": {
                        "block": "diamond_ore",
                        "pos": [12, -52, 88],
                        "dimension": "overworld",
                    },
                },
            }
        )
        events = await svc.world_state.unconsumed_events()
        rare_ev = next(e for e in events if e.event_type == "mc_rare_find")
        assert rare_ev.source_module == "minecraft"

        # Planner milestone subscriber should have queued a minecraft_milestone_share ticket.
        rows = await store.all(
            "SELECT * FROM scheduled_intents WHERE target_module='minecraft' AND status='pending'"
        )
        assert len(rows) == 1
        assert rows[0]["action_type"] == "minecraft_milestone_share"
        assert rows[0]["target_scope"] == "owner:20000"

        # Gatekeeper executes at now + 100 while HIGH focus on minecraft matches target_module.
        exec_now = now + 100.0
        engine.clock = lambda: exec_now
        engine.gatekeeper.clock = lambda: exec_now
        engine.jit.clock = lambda: exec_now
        res = await engine.tick_gatekeeper_once(now=exec_now)
        assert res["executed"] == 1
        assert len(wire.calls) == 1
        gen_call = next(c for c in ai.calls if c[0] == "generate")
        mat = gen_call[1]["jit_proactive_intent"]["module_material"]
        assert "MC在线" in mat
        assert "mc_rare_find" in mat and "diamond_ore" in mat

        # Group milestone share records sent_bot_receipts and resolves quote-reply addressed=True.
        settings.owner_qq_id = ""
        svc.hot.observe(
            group(
                message,
                event_id="grp-warm-1",
                user_id="30000",
                text="群里最近在聊MC",
                addressed=False,
                created_at=exec_now - 30.0,
            )
        )
        svc.hot.observe(
            group(
                message,
                event_id="grp-warm-2",
                user_id="30000",
                text="不知道哈基米挖得咋样了",
                addressed=False,
                created_at=exec_now - 10.0,
            )
        )
        grp_ticket_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "minecraft",
                "target_scope": "onebot:40000",
                "action_type": "minecraft_milestone_share",
                "execute_after": exec_now - 5.0,
                "expire_at": exec_now + 600.0,
                "gate_condition": {"milestone_event_id": rare_ev.id, "importance": "high"},
                "intent_note": "在群里顺手分享刚挖到钻石矿的进度",
            },
            now=exec_now - 5.0,
        )
        assert grp_ticket_id
        res_grp = await engine.tick_gatekeeper_once(now=exec_now)
        assert res_grp["executed"] == 1
        receipts = await store.all("SELECT id FROM sent_bot_receipts")
        assert len(receipts) == 1
        sent_group_msg_id = str(len(wire.calls))
        reply_to_bot = message.model_copy(
            update={
                "event_id": "group-reply-1",
                "kind": "group",
                "conversation_id": "40000",
                "user_id": "30000",
                "text": "哇真欧啊",
                "addressed": False,
                "reply_ids": [sent_group_msg_id],
            }
        )
        resolved = await svc.store.runtime.attention.resolve(reply_to_bot)
        assert resolved.addressed is True
        await svc.stop()


async def test_bili_mc_inspiration_zero_token_closed_loop(settings, store, message):
    now = _stamp("2026-11-17T16:00:00")
    settings.owner_qq_id = "20000"
    settings.onebot_enabled = True

    mc_title = "Minecraft 1.21 红石自动门建筑教程"
    responses = {
        "/x/web-interface/nav": NAV,
        "/x/web-interface/wbi/index/top/feed/rcmd": {
            "item": [
                {"goto": "av", "bvid": BV, "title": mc_title, "owner": {"name": "红石UP"}},
                {
                    "goto": "av",
                    "bvid": OTHER,
                    "title": "今日猫猫vlog",
                    "owner": {"name": "萌宠UP"},
                },
            ]
        },
        "/x/polymer/web-dynamic/v1/feed/all": {"items": []},
        "/x/web-interface/view": {
            "bvid": BV,
            "aid": 100,
            "cid": 200,
            "title": mc_title,
            "desc": "MC红石生电基础教程",
        },
        "/x/player/wbi/v2": {"subtitle": {"subtitles": []}},
        "/x/v2/reply": {"replies": []},
    }

    def handler(req):
        val = responses.get(req.url.path, {})
        return httpx.Response(200, json={"code": 0, "data": val})

    ai = CrossLoopGateway(select_bvid=BV, share_owner=False)
    wire = Wire()
    nt = FakeNodeTransport(is_connected=False)
    nt.responses["mc.dispatch_goal"] = lambda params: {
        "ok": True,
        "accepted": True,
        "bridge_ready": True,
        "mc_enabled": False,
        "mc_configured": False,
        "paused": False,
        "summary": f"筹备建造:{params.get('topic', '')}",
        "snapshot": {
            "connected": False,
            "bridge_ready": True,
            "activity": f"筹备建造:{params.get('topic', '')}",
        },
    }

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as mock_http:
        svc = Service(
            settings,
            store,
            ai,
            FakeBili(),
            Sender(settings, mock_http, wire),
            node_transport=nt,
        )
        svc.bili_watcher = BiliWatcher(svc, mock_http, sleep=_noop_sleep)
        svc.bili_watcher.clock = lambda: now
        svc.world_state.clock = lambda: now
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        await svc.bili_watcher.initialize()
        await svc.bili_watcher.set_policy(BiliPolicy(enabled=True))
        await svc.bili_watcher.update_credentials(
            CredentialUpdate(
                sessdata="private-cookie-value",
                bili_jct="c" * 32,
                buvid3="device-cookie-value",
                buvid4="device-v4-cookie-value",
            )
        )

        engine = LifeEngine(svc, clock=lambda: now)
        await engine.set_config({"quiet_hours": "23:55-00:05"})
        await engine.set_config(LifeEngineConfig(max_proactive_per_day=0))
        svc.life_engine = engine

        # 1. The watcher publishes a watched fact; the independent rule queues the build plan.
        await svc.bili_watcher.run_once(slot="manual")
        events = await svc.world_state.unconsumed_events()
        insp_ev = next(
            e for e in events if e.event_type == "bili_video_watched" and e.payload["bvid"] == BV
        )
        assert insp_ev.source_module == "bilibili_surf"
        assert insp_ev.importance == "normal"
        assert insp_ev.payload["bvid"] == BV

        # Non-MC video publishes bili_video_watched with share_owner importance rule.
        responses["/x/web-interface/view"]["bvid"] = OTHER
        responses["/x/web-interface/view"]["title"] = "今日猫猫vlog"
        responses["/x/web-interface/view"]["desc"] = "小猫晒太阳"
        ai.select_bvid = OTHER
        ai.share_owner = True
        await svc.bili_watcher.run_once(slot="manual-2")
        events2 = await svc.world_state.unconsumed_events()
        watch_ev = next(
            e
            for e in events2
            if e.event_type == "bili_video_watched" and e.payload["bvid"] == OTHER
        )
        assert watch_ev.importance == "high"

        tickets = await store.all(
            "SELECT * FROM scheduled_intents "
            "WHERE target_module='minecraft' AND action_type='build_plan'"
        )
        assert len(tickets) == 1
        mc_ticket = tickets[0]
        assert mc_ticket["target_scope"] == ""

        # 2. Gatekeeper with disconnected node_transport defers by 120s with 0 LLM calls.
        ai_calls_before = len(ai.calls)
        exec_now = now + 70.0
        engine.clock = lambda: exec_now
        engine.gatekeeper.clock = lambda: exec_now
        engine.jit.clock = lambda: exec_now
        res_defer = await engine.tick_gatekeeper_once(now=exec_now)
        assert res_defer["outcomes"].get("deferred") == 1
        assert len(ai.calls) == ai_calls_before

        # 3. Reconnect node_transport & keep proactive disconnected + max_proactive_per_day=0.
        # Gatekeeper still dispatches build_plan with 0 LLM tokens and 0 QQ quota usage!
        nt._connected = True
        await svc.proactive.on_connection(False)
        svc.modules.get("minecraft").apply_status({"bridge_ready": True})
        exec_now2 = exec_now + 130.0
        engine.clock = lambda: exec_now2
        engine.gatekeeper.clock = lambda: exec_now2
        engine.jit.clock = lambda: exec_now2
        res_exec = await engine.tick_gatekeeper_once(now=exec_now2)
        assert res_exec["outcomes"].get("done") == 1
        assert len(ai.calls) == ai_calls_before

        dispatch_calls = [p for a, p in nt.calls if a == "mc.dispatch_goal"]
        assert len(dispatch_calls) == 1
        assert dispatch_calls[0]["action_type"] == "build_plan"
        assert dispatch_calls[0]["topic"] == mc_title
        assert await engine.gatekeeper._count_today_proactive(exec_now2) == 0

        mc_mod = svc.modules.get("minecraft")
        assert isinstance(mc_mod, MinecraftSlotModule)
        assert mc_mod.bridge_ready is True
        assert "筹备建造:" in await mc_mod.get_sensor_summary()

        # Verify plan_once() accepts internal minecraft goals with empty target_scope.
        class McPlanGateway(CrossLoopGateway):
            async def generate(self, payload, subject, **kwargs):
                if kwargs.get("life_plan"):
                    return json.dumps(
                        {
                            "current_module": "minecraft",
                            "focus_level": "MEDIUM",
                            "mood": "creative",
                            "energy": 85,
                            "boredom": 15,
                            "mood_longnick": "",
                            "intents": [
                                {
                                    "target_module": "minecraft",
                                    "target_scope": "",
                                    "action_type": "explore",
                                    "delay_minutes": 2,
                                    "ttl_minutes": 30,
                                    "intent_note": "想让MC躯干在附近草原逛一圈看看地形",
                                    "allow_desktop_care": False,
                                }
                            ],
                        },
                        ensure_ascii=False,
                    )
                return await super().generate(payload, subject, **kwargs)

        svc.modules.get("minecraft").apply_status(
            {"bridge_ready": True, "snapshot": {"connected": True}}
        )
        svc.ai = McPlanGateway()
        plan_res = await engine.trigger_plan_once(now=exec_now2)
        assert plan_res["intents_enqueued"] == 1
        planned_rows = await store.all(
            "SELECT target_module, target_scope, action_type FROM scheduled_intents WHERE id=?",
            (plan_res["intent_ids"][0],),
        )
        assert len(planned_rows) == 1
        assert planned_rows[0]["target_module"] == "minecraft"
        assert planned_rows[0]["target_scope"] == ""
        assert planned_rows[0]["action_type"] == "explore"

        # Disconnected internal MC ticket near expiry (<120s left) becomes gated_skip.
        nt._connected = False
        expiring_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "minecraft",
                "target_scope": "",
                "action_type": "idle",
                "execute_after": exec_now2 - 5,
                "expire_at": exec_now2 + 60,
                "intent_note": "即将过期时断开则跳过",
            },
            now=exec_now2 - 5,
        )
        await engine.tick_gatekeeper_once(now=exec_now2)
        exp_row = await store.one("SELECT status FROM scheduled_intents WHERE id=?", (expiring_id,))
        assert exp_row["status"] == "gated_skip"
        await svc.stop()


async def test_service_dispatch_mc_goal_ssrf_filter_and_admin_mc_panel(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    ai = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: ai, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        nt = FakeNodeTransport(is_connected=True)
        nt.responses["mc.status"] = {
            "connected": False,
            "bridge_ready": True,
            "mc_enabled": False,
            "mc_configured": False,
            "paused": False,
            "phase": "phase4_live",
            "snapshot": {"connected": False, "activity": "待机"},
        }
        nt.responses["mc.pause"] = {
            "ok": True,
            "connected": False,
            "paused": True,
            "bridge_ready": True,
            "snapshot": {"connected": False, "paused": True, "activity": "已暂停"},
        }
        nt.responses["mc.resume"] = {
            "ok": True,
            "connected": False,
            "paused": False,
            "bridge_ready": True,
            "snapshot": {"connected": False, "paused": False, "activity": "待机"},
        }
        nt.responses["mc.dispatch_goal"] = lambda params: {
            "ok": True,
            "accepted": True,
            "bridge_ready": True,
            "paused": False,
            "summary": f"筹备建造:{params.get('topic', '')}",
            "snapshot": {
                "connected": False,
                "bridge_ready": True,
                "activity": f"筹备建造:{params.get('topic', '')}",
            },
        }
        svc.node_transport = nt

        # Direct _dispatch_mc_goal strips forbidden host/port/url/server keys.
        mc_mod = svc.modules.get("minecraft")
        assert isinstance(mc_mod, MinecraftSlotModule)
        direct = await mc_mod.dispatch_goal(
            {
                "action_type": "goto",
                "x": 10,
                "y": 64,
                "z": -5,
                "host": "169.254.169.254",
                "port": 25565,
                "url": "http://127.0.0.1:8080",
                "server": "evil.local",
            }
        )
        assert direct.ok is True
        last_action, last_params = nt.calls[-1]
        assert last_action == "mc.dispatch_goal"
        assert set(last_params.keys()) == {"action_type", "x", "y", "z"}

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            # 1. Verify /admin CSP SHA-256 and MC panel elements.
            page = await client.get("/admin", headers={"Authorization": ""})
            assert page.status_code == 200
            html = page.text
            styles = re.findall(r"<style>(.*?)</style>", html, re.S)
            scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
            assert len(styles) == 1 and len(scripts) == 1
            style_hash = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
            script_hash = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
            csp = page.headers["content-security-policy"]
            assert f"'sha256-{style_hash}'" in csp
            assert f"'sha256-{script_hash}'" in csp
            assert not re.search(r"\sstyle\s*=", html)
            assert not re.search(r"\son[a-z]+\s*=", html, re.I)
            assert "innerHTML" not in html
            for marker in (
                'id="life-mc-status"',
                'id="life-mc-form"',
                'id="life-mc-topic"',
                'id="life-mc-build"',
                'id="life-mc-status-btn"',
                'id="life-mc-idle"',
                'id="life-mc-pause"',
                'id="life-mc-resume"',
            ):
                assert marker in html

            # 2. Auth and validation guards on /admin/api/life/mc/goal.
            unauth = await client.post(
                "/admin/api/life/mc/goal",
                headers={"Authorization": "Bearer wrong"},
                json={"action_type": "status"},
            )
            assert unauth.status_code == 401
            for bad in (
                {"action_type": "connect"},
                {"action_type": "goto", "host": "127.0.0.1"},
                {"action_type": "goto", "x": True},
                {"action_type": "build_plan", "topic": "x" * 81},
            ):
                assert (await client.post("/admin/api/life/mc/goal", json=bad)).status_code == 400

            # Lock held -> 409
            async with app.state.admin_action_lock:
                busy = await client.post("/admin/api/life/mc/goal", json={"action_type": "status"})
                assert busy.status_code == 409

            # 3. Valid admin actions: status, build_plan, pause, resume, disconnected.
            st_res = await client.post("/admin/api/life/mc/goal", json={"action_type": "status"})
            assert st_res.status_code == 200
            assert st_res.json()["status"] == "ok"
            assert st_res.json()["life"]["nodes"]["shanghai_mc_agent"]["bridge_ready"] is True

            bp_res = await client.post(
                "/admin/api/life/mc/goal",
                json={"action_type": "build_plan", "topic": "红石雪傀儡刷雪机"},
            )
            assert bp_res.status_code == 200
            assert bp_res.json()["status"] == "ok"
            assert "红石雪傀儡刷雪机" in bp_res.json()["result"]["summary"]

            pause_res = await client.post("/admin/api/life/mc/goal", json={"action_type": "pause"})
            assert pause_res.status_code == 200
            assert pause_res.json()["life"]["nodes"]["shanghai_mc_agent"]["paused"] is True

            resume_res = await client.post(
                "/admin/api/life/mc/goal", json={"action_type": "resume"}
            )
            assert resume_res.status_code == 200
            assert resume_res.json()["life"]["nodes"]["shanghai_mc_agent"]["paused"] is False

            # RPC Rejected error returns ok=False with sanitized reason code.
            nt.responses["mc.dispatch_goal"] = Rejected("node_rpc_timeout")
            timeout_goal = await mc_mod.dispatch_goal({"action_type": "idle"})
            assert timeout_goal.ok is False
            assert timeout_goal.reason == "node_rpc_timeout"

            # Reset costly action window before final disconnect check.
            app.state.admin_auth.actions.clear()

            # Idle action works and node disconnected -> status="unavailable".
            nt.responses["mc.dispatch_goal"] = {
                "ok": True,
                "accepted": True,
                "bridge_ready": True,
                "paused": False,
                "summary": "待机",
                "snapshot": {"connected": False, "bridge_ready": True, "activity": "待机"},
            }
            idle_res = await client.post("/admin/api/life/mc/goal", json={"action_type": "idle"})
            assert idle_res.status_code == 200
            assert idle_res.json()["status"] == "ok"

            nt._connected = False
            disc_res = await client.post("/admin/api/life/mc/goal", json={"action_type": "status"})
            assert disc_res.status_code == 200
            assert disc_res.json()["status"] == "unavailable"
            assert disc_res.json()["result"]["reason"] == "node_disconnected"
