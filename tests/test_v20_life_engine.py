"""Tests for Phase 2 LifeEngine, GatekeeperWorker, JITExecutor, and life_plan."""

from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import ValidationError

from hakimi.adapters import Sender
from hakimi.affection import AffectionConfig
from hakimi.bot_server import assemble_chat_input
from hakimi.config import Settings
from hakimi.gemini import LIFE_PLAN_INSTRUCTION, LIFE_PLAN_RESPONSE_SCHEMA, GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.life_engine import (
    DESKTOP_CONTEXT_HINT,
    JIT_TRIGGER_MESSAGE,
    LifeEngine,
    LifeEngineConfig,
)
from hakimi.modules.base import GoalResult
from hakimi.outgoing import deliver_proactive_jit
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_provider_routing_and_vertex import _client_pair, _close_clients
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_social_integration import Wire, group

TZ = ZoneInfo("Asia/Shanghai")


def _stamp(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


class LifeGateway(FakeGateway):
    def __init__(self, *, plan_json: str | None = None, chat_json: str | None = None):
        super().__init__()
        self.plan_json = plan_json or json.dumps(
            {
                "current_module": "bilibili_surf",
                "focus_level": "MEDIUM",
                "mood": "curious",
                "energy": 78,
                "boredom": 22,
                "mood_longnick": "摸鱼刷片中",
                "intents": [
                    {
                        "target_module": "bilibili_surf",
                        "target_scope": "onebot:40000",
                        "action_type": "group_chat_share",
                        "delay_minutes": 10,
                        "ttl_minutes": 90,
                        "intent_note": (
                            "刚刷到一个有意思的开源项目评测，想看看群里气氛合适就顺口聊一句"
                        ),
                        "allow_desktop_care": False,
                    }
                ],
            },
            ensure_ascii=False,
        )
        self.chat_json = chat_json or json.dumps(
            {
                "abstain": False,
                "bubbles": [
                    "刚刷到个有点意思的 Rust 异步调度评测",
                    "你们写 tokio 会手写 Future 吗",
                ],
                "anchor": {"reply_id": None, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
            },
            ensure_ascii=False,
        )
        self.raise_on_plan: Exception | None = None
        self.raise_on_chat: Exception | None = None

    async def generate(self, payload, subject, **kwargs):
        if kwargs.get("life_plan"):
            self.calls.append(("life_plan", payload, subject, dict(kwargs)))
            if self.raise_on_plan is not None:
                raise self.raise_on_plan
            return self.plan_json
        self.calls.append(("generate", payload, subject, dict(kwargs)))
        if self.raise_on_chat is not None:
            raise self.raise_on_chat
        return self.chat_json


class RecordingDiary:
    def __init__(self):
        self.replies = []
        self.world_events = []

    async def bot_reply(self, reply_id, message, text, *, now=None):
        self.replies.append((reply_id, message.kind, message.conversation_id, text))

    async def world_event(self, event_id, source_module, event_type, summary, *, now=None):
        self.world_events.append((event_id, source_module, event_type, summary))


class FixedTimeAssembleDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 17, 14, 0, tzinfo=tz or TZ)


def test_assemble_chat_input_jit_intent_and_byte_identity(settings, message, monkeypatch):
    monkeypatch.setattr("hakimi.bot_server.datetime", FixedTimeAssembleDateTime)
    base_payload = {
        "current_message": "接着聊",
        "recent_messages": [{"seq": 1, "role": "user", "text": "在干嘛", "user_id": "30000"}],
        "rolling_summary": "一段历史摘要",
        "nearby_group_context": [{"sender": "群友", "text": "刚才聊到 Rust"}],
        "recent_bili_surfed": [{"bvid": "BV1xx411c7mD", "notes": "视频笔记"}],
        "world_state_summary": {"current_module": "bilibili_surf", "focus_level": "MEDIUM"},
    }
    grp = group(message, event_id="101")

    for cont in (False, True):
        out_plain = assemble_chat_input(dict(base_payload), grp, settings, continuation=cont)
        out_empty_jit = assemble_chat_input(
            {**base_payload, "jit_proactive_intent": None}, grp, settings, continuation=cont
        )
        assert out_plain == out_empty_jit
        assert "jit_proactive_intent" not in out_plain

        with_jit = assemble_chat_input(
            {
                **base_payload,
                "jit_proactive_intent": {
                    "target_module": "bilibili_surf",
                    "intent_note": "顺口分享一下刚刚看的视频",
                },
            },
            grp,
            settings,
            continuation=cont,
        )
        assert "jit_proactive_intent" in with_jit
        assert "顺口分享一下刚刚看的视频" in with_jit

        sandbox_out = assemble_chat_input(
            {
                **base_payload,
                "jit_proactive_intent": {"intent_note": "不应进入沙盒"},
            },
            grp,
            settings,
            sandbox_mode=True,
            continuation=cont,
        )
        assert "jit_proactive_intent" not in sandbox_out
        assert "不应进入沙盒" not in sandbox_out

    # Last-resort trimming: drop world_state_summary & recent_bili_surfed BEFORE jit_intent,
    # and pop jit_proactive_intent only if still over budget.
    tight_settings = Settings(_env_file=None, **{**settings.model_dump(), "chat_tail_tokens": 700})
    mid_out = assemble_chat_input(
        {
            "current_message": "触发主动消息",
            "recent_messages": [],
            "world_state_summary": {"current_module": "mc", "sensor": "x" * 1200},
            "recent_bili_surfed": [{"bvid": "BV1xx411c7mD", "notes": "y" * 1200}],
            "jit_proactive_intent": {"intent_note": "保留核心意图"},
        },
        grp,
        tight_settings,
    )
    assert "保留核心意图" in mid_out
    assert "world_state_summary" not in mid_out
    assert "recent_bili_surfed" not in mid_out

    very_tight = Settings(_env_file=None, **{**settings.model_dump(), "chat_tail_tokens": 560})
    dropped_out = assemble_chat_input(
        {
            "current_message": "触发主动消息",
            "recent_messages": [],
            "jit_proactive_intent": {"intent_note": "非常长的附加意图" * 300},
        },
        grp,
        very_tight,
    )
    assert "jit_proactive_intent" not in dropped_out


@pytest.mark.parametrize("morning_thinking", [None, "medium", "high"])
async def test_gemini_life_plan_interactions_and_vertex_gc(
    settings, store, sdk_pair, morning_thinking
):
    calls: list[dict] = []
    plan_text = json.dumps(
        {
            "current_module": "idle",
            "focus_level": "LOW",
            "mood": "relaxed",
            "energy": 85,
            "boredom": 15,
            "mood_longnick": "",
            "intents": [],
        }
    )

    def handler(request):
        body = json.loads(request.content)
        calls.append({"path": request.url.path, "body": body})
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = plan_text
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        with pytest.raises(Rejected, match="invalid_life_plan"):
            await gate.generate({"k": "v"}, "subj", budget_lane="chat", life_plan=True)

        result = await gate.generate(
            {"local_time": "2026-11-17 14:00"},
            "subj",
            budget_lane="proactive",
            life_plan=True,
            morning_thinking=morning_thinking,
        )
        assert json.loads(result)["mood"] == "relaxed"
        gen_req = next(c for c in calls if c["path"].endswith("interactions"))
        assert gen_req["body"]["store"] is False
        assert gen_req["body"]["generation_config"]["thinking_level"] == (
            morning_thinking or "medium"
        )
        assert gen_req["body"]["generation_config"]["max_output_tokens"] == (
            4096 if morning_thinking else 2048
        )
        assert gen_req["body"]["response_format"]["schema"] == LIFE_PLAN_RESPONSE_SCHEMA
        assert LIFE_PLAN_INSTRUCTION in gen_req["body"]["system_instruction"]
        dec_req = next(c for c in calls if c["path"].endswith("decisions"))
        assert dec_req["path"].endswith("/decisions")
    finally:
        await gate.close()

    # Verify Vertex generate_content path passes response_mime_type & response_json_schema cleanly
    vertex_calls: list[dict] = []

    def vertex_handler(request):
        body = json.loads(request.content)
        vertex_calls.append({"path": request.url.path, "body": body})
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {"role": "model", "parts": [{"text": plan_text}]},
                        "finishReason": "STOP",
                    }
                ],
                "usageMetadata": {"promptTokenCount": 50, "candidatesTokenCount": 20},
            },
        )

    v_client, v_clients, v_transports = _client_pair(vertex_handler, vertex=True)
    d_clients, d_transports = _client_pair(vertex_handler, vertex=False)
    gate_v = GeminiGateway(
        settings,
        store,
        d_clients[0],
        d_clients[1],
        vertex_chat_client=v_client,
    )
    try:
        await gate_v.routing.set_config(
            {
                **gate_v.routing.config.model_dump(mode="json"),
                "vertex_enabled": True,
                "vertex_protocol": "generate_content",
            }
        )
        out_v = await gate_v.generate(
            {"local_time": "2026-11-17 15:00"},
            "subj-v",
            budget_lane="proactive",
            life_plan=True,
            morning_thinking=morning_thinking,
        )
        assert json.loads(out_v)["current_module"] == "idle"
        gc_req = next(c for c in vertex_calls if "generateContent" in c["path"])
        gen_cfg = gc_req["body"]["generationConfig"]
        assert gen_cfg["responseMimeType"] == "application/json"
        assert gen_cfg["responseJsonSchema"] == LIFE_PLAN_RESPONSE_SCHEMA
        assert gen_cfg["thinkingConfig"]["thinking_level"] == (morning_thinking or "medium").upper()
    finally:
        await gate_v.close()
        await _close_clients([*v_clients, *d_clients], [*v_transports, *d_transports])


async def test_deliver_proactive_jit_semantics_and_ambiguity(settings, store, message):
    wire = Wire()
    diary = RecordingDiary()
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, wire),
            diary=diary,
        )
        await svc.world_state.initialize()
        grp_msg = group(message, event_id="jit-test-1", user_id="0", addressed=True)

        # 1. Abstain -> zero transport calls
        abstain_json = json.dumps(
            {
                "abstain": True,
                "bubbles": [""],
                "anchor": {},
                "sticker": {},
                "image_digest": None,
            }
        )
        res_abs = await deliver_proactive_jit(svc, grp_msg, abstain_json, reply_id="jit:abstain-1")
        assert res_abs == {"status": "abstained", "bubbles_sent": 0}
        assert len(wire.calls) == 0

        # 2. Group multi-bubble + link filtering + <=220 truncation + receipt/public_memory/diary
        long_b1 = "先说一句" + "很长的话" * 80
        b2 = "顺便看看这个 https://evil.example.com/phish 和 https://www.bilibili.com/video/BV1xx411c7mD"
        action_json = json.dumps(
            {
                "abstain": False,
                "bubbles": [long_b1, b2, "第三条"],
                "anchor": {},
                "sticker": {},
                "image_digest": None,
            },
            ensure_ascii=False,
        )
        res_ok = await deliver_proactive_jit(svc, grp_msg, action_json, reply_id="jit:ok-1")
        assert res_ok["status"] == "sent"
        assert res_ok["bubbles_sent"] == 3
        assert len(wire.calls) == 3
        sent_texts = [c[1]["message"][0]["data"]["text"] for c in wire.calls]
        assert all(len(t) <= 220 for t in sent_texts)
        assert "evil.example.com" not in sent_texts[1]
        assert "bilibili.com" in sent_texts[1]

        # sent_bot_receipts registered -> replying to message_id=1 resolves addressed=True
        reply_back = group(
            message,
            event_id="reply-back",
            user_id="30000",
            addressed=False,
            reply_ids=["1"],
        )
        resolved = await svc.store.runtime.attention.resolve(reply_back)
        assert resolved.addressed is True

        # recent_messages, public_memory, diary, world_state updated
        snap = await svc.sessions.snapshot(grp_msg)
        assert len(snap["recent_messages"]) == 3
        pub_rows = await store.all("SELECT * FROM public_interactions")
        assert len(pub_rows) == 1
        assert len(diary.replies) == 1
        assert svc.world_state.current.last_proactive_at > 0
        assert not await svc.world_state.group_cooldown_ok(store.session_scope(grp_msg), 1500.0)

        # 2b. Plain-text fallback (>220 chars truncated cleanly and delivered as 1 bubble)
        wire.calls.clear()
        res_plain = await deliver_proactive_jit(
            svc, grp_msg, "纯文本测试" + "喵" * 260, reply_id="jit:plain-1"
        )
        assert res_plain["status"] == "sent" and res_plain["bubbles_sent"] == 1
        assert len(wire.calls[0][1]["message"][0]["data"]["text"]) <= 220

        # 3. onebot_not_dispatched before any bubble -> status="not_dispatched"
        class NotDispatchedWire(Wire):
            async def call(self, action, params):
                raise Rejected("onebot_not_dispatched")

        svc.sender.onebot = NotDispatchedWire()
        res_nd = await deliver_proactive_jit(svc, grp_msg, "纯文本降级测试", reply_id="jit:nd-1")
        assert res_nd == {"status": "not_dispatched", "bubbles_sent": 0}

        # 4. Partial failure after bubble 0 confirmed -> applies bubble 0 and returns uncertain
        class PartialFailWire(Wire):
            async def call(self, action, params):
                self.calls.append((action, params))
                if len(self.calls) == 1:
                    return {"message_id": 901}
                raise TimeoutError("lost ack")

        p_wire = PartialFailWire()
        svc.sender.onebot = p_wire
        before_count = len((await svc.sessions.snapshot(grp_msg))["recent_messages"])
        partial_json = json.dumps(
            {
                "abstain": False,
                "bubbles": ["第一条稳稳发出去", "第二条超时"],
                "anchor": {},
                "sticker": {},
                "image_digest": None,
            },
            ensure_ascii=False,
        )
        res_uncertain = await deliver_proactive_jit(
            svc, grp_msg, partial_json, reply_id="jit:partial-1"
        )
        assert res_uncertain["status"] == "uncertain"
        assert res_uncertain["bubbles_sent"] == 1
        after_msgs = (await svc.sessions.snapshot(grp_msg))["recent_messages"]
        assert len(after_msgs) == before_count + 1
        assert after_msgs[-1]["text"] == "第一条稳稳发出去"
        assert len(p_wire.calls) == 2  # Never retried!


async def test_life_engine_config_and_planner_circadian_and_fallback(settings, store):
    with pytest.raises(ValidationError):
        LifeEngineConfig(active_hours="bad")
    with pytest.raises(ValidationError):
        LifeEngineConfig(planner_min_minutes=200, planner_max_minutes=120)
    with pytest.raises(ValidationError):
        LifeEngineConfig(unknown_field=1)

    now = _stamp("2026-11-17T02:00:00")
    diary = RecordingDiary()
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, Wire()),
            diary=diary,
        )
        svc.world_state.clock = lambda: now
        await svc.world_state.initialize()
        engine = LifeEngine(svc, clock=lambda: now, rng=random.Random(42))
        await engine.initialize()

        # Config persistence round-trip
        saved = await engine.set_config(
            LifeEngineConfig(
                enabled=True,
                active_hours="09:00-01:00",
                max_proactive_per_day=4,
            )
        )
        assert saved["source"] == "admin"
        assert saved["config"]["max_proactive_per_day"] == 4

        svc2 = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, Wire()),
        )
        svc2.world_state.clock = lambda: now
        await svc2.world_state.initialize()
        reloaded = LifeEngine(svc2, clock=lambda: now)
        st2 = await reloaded.status()
        assert st2["source"] == "admin" and st2["config"]["max_proactive_per_day"] == 4
        reset = await reloaded.set_config(None)
        assert reset["source"] == "env" and reset["config"]["max_proactive_per_day"] == 6

        # Entering rest does not prepay recovery. Replanning in the same instant is free.
        from hakimi.rest_policy import RestHoursConfig

        await engine.set_rest_policy(RestHoursConfig(rest_hours="01:00-09:00"))
        sleep_res = await engine.trigger_plan_once(now=now)
        assert sleep_res["status"] == "sleeping"
        assert sleep_res["intents_enqueued"] == 0
        # Only elapsed sleep time contributes to recovery.
        assert svc.world_state.current.mood == "sleeping"
        assert svc.world_state.current.energy == 80
        assert svc.world_state.current.boredom == 20
        assert (
            _stamp("2026-11-17T09:00:00")
            <= sleep_res["next_plan_at"]
            <= _stamp("2026-11-17T09:30:00")
        )
        assert svc.ai.calls == []

        # Daytime 14:00 with 1 valid + 1 pre-written speech ticket -> skips malformed ticket
        day_now = _stamp("2026-11-17T14:00:00")
        engine.clock = lambda: day_now
        engine.planner.clock = lambda: day_now
        svc.ai.plan_json = json.dumps(
            {
                "current_module": "minecraft",
                "focus_level": "MEDIUM",
                "mood": "excited",
                "energy": 70,
                "boredom": 10,
                "mood_longnick": "矿洞探险中",
                "intents": [
                    {
                        "target_module": "minecraft",
                        "target_scope": "owner",
                        "action_type": "mc_share",
                        "delay_minutes": 10,
                        "ttl_minutes": 60,
                        "intent_note": "刚挖到第一批矿石，想顺口跟对方分享下进展",
                        "allow_desktop_care": False,
                    },
                    {
                        "target_module": "qq_social",
                        "target_scope": "owner",
                        "action_type": "bad_speech",
                        "delay_minutes": 10,
                        "ttl_minutes": 60,
                        "intent_note": "主人，我刚刚挖到钻石啦喵~",
                        "allow_desktop_care": False,
                    },
                ],
            },
            ensure_ascii=False,
        )
        await svc.world_state.publish_event(
            "minecraft", "ore_found", {"ore": "iron"}, importance="normal", now=day_now
        )
        plan_res = await engine.trigger_plan_once(now=day_now)
        assert plan_res["status"] == "planned" and plan_res["source"] == "llm"
        assert plan_res["intents_enqueued"] == 1
        assert svc.world_state.current.current_module == "idle"
        view = await svc.world_state.experience.view(day_now)
        assert view["plan"]["plan_module"] == "minecraft" and view["actual"]["state"] == "idle"
        assert await svc.world_state.unconsumed_events() == []
        assert 2 * 3600 <= (plan_res["next_plan_at"] - day_now) <= 4 * 3600
        assert len(diary.world_events) == 1

        # Milestone high-importance impulse subscriber (0 LLM calls + 10m per-source dedup)
        calls_before = len(svc.ai.calls)
        await svc.world_state.publish_event(
            "minecraft", "diamond_found", {"count": 3}, importance="high", now=day_now
        )
        await svc.world_state.publish_event(
            "minecraft", "ancient_debris", {"count": 1}, importance="high", now=day_now + 60
        )
        assert len(svc.ai.calls) == calls_before
        rows = await store.all("SELECT * FROM scheduled_intents WHERE status='pending'")
        assert len(rows) == 2  # 1 from plan_once + 1 from first high milestone (second deduped)

        # LLM failure -> deterministic zero-Token fallback without raising
        svc.ai.raise_on_plan = Rejected("gemini_unavailable")
        fb_res = await engine.trigger_plan_once(now=day_now + 100)
        assert fb_res["status"] == "planned" and fb_res["source"] == "local_fallback"


async def test_gatekeeper_hard_gates_and_desktop_synergy(settings, store, message, tmp_path):
    settings.affection_enabled = True
    settings.enable_antigravity_memory = True
    mem_dir = tmp_path / "antigravity_mem"
    mem_dir.mkdir(parents=True)
    settings.antigravity_memory_dir = mem_dir

    now = _stamp("2026-11-17T15:00:00")
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, wire),
        )
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        await svc.store.runtime.affection.initialize()
        await svc.store.runtime.affection.set_config(AffectionConfig(enabled=True))

        engine = LifeEngine(svc, clock=lambda: now)
        await engine.initialize()

        # 1. Expired intents become expired
        exp_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": now - 100,
                "expire_at": now - 10,
                "intent_note": "过期测试",
            },
            now=now - 100,
        )
        await engine.tick_gatekeeper_once(now=now)
        row = await store.one("SELECT status FROM scheduled_intents WHERE id=?", (exp_id,))
        assert row["status"] == "expired"

        # 2. Quiet hours -> gated_skip
        quiet_now = _stamp("2026-11-17T23:45:00")
        q_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": quiet_now - 5,
                "expire_at": quiet_now + 600,
                "intent_note": "深夜门控测试",
            },
            now=quiet_now - 5,
        )
        await engine.tick_gatekeeper_once(now=quiet_now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (q_id,)))[
            "status"
        ] == "gated_skip"

        # 3. Attention Lock -> defers non-matching module
        await svc.world_state.experience.mc_snapshot("mine", svc.world_state.clock())
        bili_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "bilibili_surf",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": now - 5,
                "expire_at": now + 3600,
                "intent_note": "高专注锁延后测试",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        deferred_row = await store.one(
            "SELECT status, execute_after FROM scheduled_intents WHERE id=?", (bili_id,)
        )
        assert deferred_row["status"] == "pending"
        assert deferred_row["execute_after"] > now
        await svc.world_state.mark_intent(bili_id, "gated_skip", now=now)
        await svc.world_state.update_state(current_module="idle", focus_level="LOW")

        # 4. Group activity & affection gates:
        # Hostile (Lv.1) speaker messages are excluded -> cold -> gated_skip
        aff = svc.store.runtime.affection
        await aff.set_user_score("31001", 10, platform="onebot")
        await aff.set_user_score("32001", 65, platform="onebot")  # Lv.4 familiar
        svc.hot.observe(group(message, event_id="h1", user_id="31001", created_at=now - 120))
        svc.hot.observe(group(message, event_id="h2", user_id="31001", created_at=now - 60))
        cold_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "冷群与低好感排除测试",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (cold_id,)))[
            "status"
        ] == "gated_skip"
        assert svc.ai.calls == []

        # Now a Lv.4 friend speaks ONCE -> effective_min_msgs=1 -> passes and fires JIT!
        svc.hot.observe(
            group(message, event_id="f1", user_id="32001", text="刚下班好累", created_at=now - 30)
        )
        warm_grp_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "群里有熟人冒头，顺势打个招呼聊两句",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (warm_grp_id,)))[
            "status"
        ] == "done"
        gen_call = next(c for c in svc.ai.calls if c[0] == "generate")
        assert gen_call[3]["scenario"] == Scenario.FAST_CHAT
        assert gen_call[3]["budget_lane"] == "proactive"
        assert gen_call[3]["route"].tier == 1
        assert gen_call[1]["current_message"] == JIT_TRIGGER_MESSAGE
        assert "32001" in gen_call[1]["batch_affection"]
        assert len(wire.calls) == 2

        # 5. Group cooldown gate blocks immediate follow-up group ticket
        g_cool_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "onebot:40000",
                "action_type": "group_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "冷却拦截测试",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (g_cool_id,)))[
            "status"
        ] == "gated_skip"

        # 6. Owner DM + Desktop Antigravity_Memory Synergy Gate
        mem_file = mem_dir / "short_term_memory.json"
        mem_file.write_text(
            json.dumps(
                [
                    {
                        "timestamp": "2026-11-17 14:30:00",
                        "tag": "code",
                        "dialogue": {"user": "debug", "assistant": "fixed"},
                    }
                ]
            ),
            encoding="utf-8",
        )
        # 6a) owner_silence_care without allow_desktop_care -> silently gated_skip!
        sil_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "owner:20000",
                "action_type": "owner_silence_care",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "好久没见主人冒头了，看看在忙啥",
            },
            now=now - 5,
        )
        calls_before = len(svc.ai.calls)
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (sil_id,)))[
            "status"
        ] == "gated_skip"
        assert len(svc.ai.calls) == calls_before

        # 6b) owner_desktop_warm_care -> passes and injects desktop_context_hint!
        warm_dm_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "owner:20000",
                "action_type": "owner_desktop_warm_care",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "gate_condition": {"allow_desktop_care": True},
                "intent_note": "刚在电脑端忙完，轻声关心下休息喝水",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (warm_dm_id,)))[
            "status"
        ] == "done"
        dm_gen = [c for c in svc.ai.calls if c[0] == "generate"][-1]
        assert dm_gen[3]["scenario"] == Scenario.OWNER_PRIVATE
        assert dm_gen[1]["jit_proactive_intent"]["desktop_context_hint"] == DESKTOP_CONTEXT_HINT

        # 7. Session concurrency lock defers intent instead of colliding
        scope_owner = store.session_scope(message)
        async with svc.dialogue_scheduler.hold(scope_owner):
            busy_id = await svc.world_state.enqueue_intent(
                {
                    "target_module": "qq_social",
                    "target_scope": "owner:20000",
                    "action_type": "minecraft_milestone_share",
                    "execute_after": now - 5,
                    "expire_at": now + 600,
                    "intent_note": "会话并发锁定延后测试",
                },
                now=now - 5,
            )
            await engine.tick_gatekeeper_once(now=now)
            busy_row = await store.one(
                "SELECT status, execute_after FROM scheduled_intents WHERE id=?", (busy_id,)
            )
            assert busy_row["status"] == "pending" and busy_row["execute_after"] == now + 150.0
            await svc.world_state.mark_intent(busy_id, "gated_skip", now=now)

        # 8. Daily proactive cap circuit breaker blocks further proactive intents
        await engine.set_config(LifeEngineConfig(max_proactive_per_day=2))
        cap_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "owner:20000",
                "action_type": "minecraft_milestone_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "每日上限熔断测试",
            },
            now=now - 5,
        )
        calls_before = len(svc.ai.calls)
        await engine.tick_gatekeeper_once(now=now)
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (cap_id,)))[
            "status"
        ] == "gated_skip"
        assert len(svc.ai.calls) == calls_before


async def test_jit_executor_bili_dispatch_not_dispatched_and_run_loop(settings, store, message):
    now = _stamp("2026-11-17T16:00:00")
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, wire),
        )
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        engine = LifeEngine(svc, clock=lambda: now)
        await engine.initialize()

        # Verify bilibili_surf dispatch_goal runs when target_module == "bilibili_surf"
        bili_mod = svc.modules.get("bilibili_surf")
        dispatched = []

        async def fake_dispatch(ticket):
            dispatched.append(ticket["id"])
            return GoalResult(
                ok=True,
                module_id="bilibili_surf",
                summary="B站冲浪:巡逻开启 近期:1 [role=OWNER]",
            )

        bili_mod.dispatch_goal = fake_dispatch

        bili_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "bilibili_surf",
                "target_scope": "owner:20000",
                "action_type": "bili_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "刚看完一条科普视频，想找机会顺嘴分享一下",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        assert dispatched == [bili_id]
        gen_call = next(c for c in svc.ai.calls if c[0] == "generate")
        assert "[untrusted_role=OWNER]" in gen_call[1]["jit_proactive_intent"]["module_material"]
        assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (bili_id,)))[
            "status"
        ] == "done"

        # Verify onebot_not_dispatched keeps ticket pending and defers execute_after
        class NotDispatchedWire(Wire):
            async def call(self, action, params):
                raise Rejected("onebot_not_dispatched")

        svc.sender.onebot = NotDispatchedWire()
        nd_id = await svc.world_state.enqueue_intent(
            {
                "target_module": "qq_social",
                "target_scope": "owner:20000",
                "action_type": "minecraft_milestone_share",
                "execute_after": now - 5,
                "expire_at": now + 600,
                "intent_note": "网关未派发时回退等待重试测试",
            },
            now=now - 5,
        )
        await engine.tick_gatekeeper_once(now=now)
        nd_row = await store.one(
            "SELECT status, execute_after FROM scheduled_intents WHERE id=?", (nd_id,)
        )
        assert nd_row["status"] == "pending" and nd_row["execute_after"] > now

        # LifeEngine.run() MUST survive arbitrary exceptions and only stop on CancelledError
        async def boom(*args, **kwargs):
            raise RuntimeError("transient error")

        engine.tick_gatekeeper_once = boom
        engine.planner.next_plan_at = now + 99999
        run_task = asyncio.create_task(engine.run(), name="life-engine-test")
        await asyncio.sleep(0.05)
        assert not run_task.done()
        engine.wakeup.set()
        await asyncio.sleep(0.05)
        assert not run_task.done()
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task
