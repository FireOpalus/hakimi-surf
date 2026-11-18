import json
import random
import time
from datetime import datetime

import httpx
import pytest

from hakimi.adapters import Sender, notice_message
from hakimi.bot_server import prompt_context
from hakimi.context_budget import estimate_tokens
from hakimi.dual_memory import Extraction
from hakimi.generation_policy import Scenario
from hakimi.onebot_transport import response_data, validate_action
from hakimi.outbound_actions import parse_actions
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.proactive_dm import OwnerProactive
from hakimi.reply_style import casual_reply, clean_chinese_punctuation
from hakimi.roaming_logger import RoamingLogger
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_social_integration import Wire, group, process
from tests.test_streaming_v0100 import action


class TypingRichWire(Wire):
    supports_typing = True

    async def call(self, action_name, params):
        if action_name in {
            "set_input_status",
            "set_msg_emoji_like",
            "group_poke",
            "friend_poke",
            "send_poke",
        }:
            self.calls.append((action_name, params))
            return {}
        return await super().call(action_name, params)


def test_onebot_transport_allows_emoji_and_poke_actions(settings):
    for act in ("set_input_status", "set_msg_emoji_like", "group_poke", "friend_poke", "send_poke"):
        assert response_data({"status": "ok", "retcode": 0, "data": None}, act) == {}

    validate_action(settings, "set_msg_emoji_like", {"message_id": 12345, "emoji_id": "128514"})
    validate_action(settings, "group_poke", {"group_id": 40000, "user_id": 30000})
    validate_action(settings, "friend_poke", {"user_id": 20000})
    validate_action(settings, "send_poke", {"user_id": 30000})

    for bad_act, params in (
        ("set_msg_emoji_like", {"message_id": 123, "emoji_id": "9999999"}),
        ("group_poke", {"group_id": 99999, "user_id": 30000}),
        ("group_poke", {"group_id": 40000, "user_id": 10000}),
        ("friend_poke", {"user_id": 99999}),
    ):
        with pytest.raises(Rejected, match="onebot_action_forbidden"):
            validate_action(settings, bad_act, params)


async def test_napcat_poke_notices_and_loop_prevention(settings, store):
    now = time.time()
    friend_poke = {
        "self_id": "10000",
        "post_type": "notice",
        "notice_type": "notify",
        "sub_type": "poke",
        "sender_id": 20000,
        "user_id": 10000,
        "target_id": 10000,
        "sender": {"nickname": "BOISER"},
        "time": now,
    }
    msg = notice_message(friend_poke, settings)
    assert msg is not None
    assert msg.user_id == "20000"
    assert msg.kind == "private"
    assert msg.addressed is True
    assert msg.attention_reason == "private"
    assert msg.sender_nickname == "BOISER"
    assert msg.text == "[协议事件：戳了戳你]"

    # Bot's own poke echo and private non-self/reaction notices must be ignored to avoid loops.
    assert notice_message({**friend_poke, "sender_id": 10000, "target_id": 20000}, settings) is None
    assert notice_message({**friend_poke, "target_id": 20000}, settings) is None
    assert (
        notice_message(
            {
                "self_id": "10000",
                "post_type": "notice",
                "notice_type": "msg_emoji_like",
                "user_id": 20000,
                "message_id": "88",
                "time": now,
            },
            settings,
        )
        is None
    )

    class PokeGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return action(bubbles=["突然戳我干嘛！"], poke_qq="20000")

    wire = TypingRichWire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, PokeGw(), FakeBili(), Sender(settings, http, wire))
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            assert await svc.accept_event(friend_poke) == "accepted"
            assert await svc.accept_event({**friend_poke, "target_id": 20000}) == "ignored"
            job = await store.claim()
            await svc.process(job)
            assert await svc.deliver_once()
            names = [name for name, _ in wire.calls]
            assert "send_private_msg" in names
            assert any(name in {"friend_poke", "send_poke"} for name in names)
        finally:
            await svc.stop()


async def test_proactive_companion_uses_memory_and_bili_finds_while_digest_stays_public(
    settings, store
):
    now = datetime.fromisoformat("2026-11-16T15:30:00+08:00").timestamp()
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"

    class CompanionGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            if kwargs.get("scenario") == Scenario.OWNER_COMPANION:
                return "周一摸鱼时间到，顺手给你推个 Rust 视频。"
            return "今天公开巡逻安稳。"

    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, CompanionGw(), FakeBili(), Sender(settings, http, wire))
        await svc.bili_watcher.initialize()
        dm = OwnerProactive(svc, clock=lambda: now, rng=random.Random(7))
        svc.proactive = dm
        await dm.initialize()
        try:
            target = dm.target()
            await svc.sessions.remember(target, "昨晚那个并发锁问题还在研究呢", "慢慢查~")
            await svc.sessions.admit_history(
                target,
                [
                    target.model_copy(
                        update={
                            "event_id": "seed-u1",
                            "text": "今晚打算把测试全部跑通",
                            "created_at": now - 3600,
                        }
                    )
                ],
                expected_epoch=0,
            )
            snap = await svc.sessions.snapshot(target)
            target_seq = max(
                item["seq"] for item in snap["recent_messages"] if item.get("role") == "user"
            )
            await svc.sessions.dual.snapshot_cold(target, force=True)
            dual_job = await svc.sessions.dual.claim(target)
            assert dual_job is not None
            assert await svc.sessions.dual.commit(
                target,
                dual_job,
                Extraction.model_validate(
                    {
                        "profiles": [
                            {
                                "member_id": "20000",
                                "slot": "state",
                                "text": "这周准备重构异步测试",
                                "evidence_seq": target_seq,
                                "quote": "今晚打算把测试全部跑通",
                                "basis": "self_report",
                                "valid_days": 5,
                            }
                        ],
                        "chronicles": [],
                    }
                ),
            )
            async with store.transaction():
                await store.db.execute(
                    "INSERT INTO bili_watched_history(bvid,run_id,status,created,summary) "
                    "VALUES ('BV1xx411c7mD','pat-1','done',?,?)",
                    (
                        now - 600,
                        store.vault.seal("bili-learn:BV1xx411c7mD", "Rust 异步运行时底层剖析"),
                    ),
                )

            p = await dm.companion_context(now)
            assert p["weekday"] == "周一" and p["day_phase"] == "下午摸鱼时段"
            assert any("重构异步测试" in h for h in p["owner_memory_hints"])
            assert any("测试全部跑通" in t for t in p["recent_private_topics"])
            assert p["recent_bili_finds"][0]["bvid"] == "BV1xx411c7mD"

            # Digest must stay strictly public and never touch notebook.
            svc.diary = RoamingLogger(settings, store)
            await svc.diary.initialize()
            settings.roaming_enabled = True
            await svc.diary.patrol("p1", "公开科学小记", now=now)

            class GuardedNotebook:
                def for_message(self, _):
                    raise AssertionError("Digest accessed private notebook")

            svc.notebook = GuardedNotebook()
            svc.ai.calls.clear()
            dig = await dm.manual_digest()
            assert dig["delivery"] == "sent"
            digest_call = next(
                c for c in svc.ai.calls if c[3].get("scenario") == Scenario.OWNER_DIGEST
            )
            assert "owner_memory_hints" not in digest_call[1]
            assert "重构异步测试" not in json.dumps(digest_call[1], ensure_ascii=False)
        finally:
            await svc.stop()


async def test_pre_generation_typing_and_casual_private_punctuation(settings, store, message):
    order = []

    class TypingGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            order.append("generate")
            await super().generate(payload, subject, **kwargs)
            return action(bubbles=["诶？", "你真写完啦。"])

    wire = TypingRichWire()
    orig_call = wire.call

    async def recording_call(name, params):
        if name == "set_input_status":
            order.append("typing")
        return await orig_call(name, params)

    wire.call = recording_call
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, TypingGw(), FakeBili(), Sender(settings, http, wire))
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            # Deterministic commands never emit set_input_status before replying.
            await process(
                svc,
                message.model_copy(
                    update={
                        "event_id": "cmd-1",
                        "text": "/new-session",
                        "command_text": "/new-session",
                    }
                ),
            )
            assert "typing" not in order
            assert await svc.deliver_once()
            assert wire.calls[0][0] == "send_private_msg"

            wire.calls.clear()
            await process(
                svc, message.model_copy(update={"event_id": "chat-1", "text": "我搞定啦"})
            )
            assert "typing" in order and order.index("typing") < order.index("generate")
            assert await svc.deliver_once()
            sent_texts = [
                p["message"][0]["data"]["text"]
                for act, p in wire.calls
                if act == "send_private_msg"
            ]
            assert sent_texts == ["诶?", "你真写完啦"]
        finally:
            await svc.stop()

    parsed, _ = parse_actions(action(bubbles=["这句短的。", "第一句。第二句。"]), public=False)
    assert parsed.bubbles == ["这句短的", "第一句。第二句。"]
    assert casual_reply("好耶~。") == "好耶~"
    assert clean_chinese_punctuation("BOISER，慢慢说。") == "BOISER，慢慢说。"


async def test_group_borderline_lurk_reaction_zero_token_and_cooldown(settings, store, message):
    settings.group_policies = {"onebot:40000": "smart_reply"}

    class LurkGw(FakeGateway):
        async def screen(self, payload, subject, **kwargs):
            await super().screen(payload, subject, **kwargs)
            return RouteDecision("ignore", reply_score=0.25, reply_threshold=0.42)

        async def generate(self, payload, subject, **kwargs):
            raise AssertionError("Lurk reaction must not invoke generate")

    wire = TypingRichWire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, LurkGw(), FakeBili(), Sender(settings, http, wire))
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            m1 = group(
                message,
                "9001",
                text="哈哈哈笑死我了",
                addressed=False,
                attention_reason="",
                mentioned_ids=[],
            )
            await process(svc, m1)
            assert await svc.deliver_once()
            likes = [p for act, p in wire.calls if act == "set_msg_emoji_like"]
            assert len(likes) == 1 and likes[0]["emoji_id"] == "128514"

            # Cooldown suppresses immediate repeat lurk reaction in same group.
            wire.calls.clear()
            m2 = group(
                message,
                "9002",
                text="2333笑死",
                addressed=False,
                attention_reason="",
                mentioned_ids=[],
            )
            await process(svc, m2)
            assert not await svc.deliver_once()
            assert not [act for act, _ in wire.calls if act == "set_msg_emoji_like"]
        finally:
            await svc.stop()


def test_static_prefixes_remain_within_token_bounds(settings, message):
    pool = StaticPrefixes()
    for msg in (message, group(message)):
        ctx = prompt_context(msg, settings)
        text = pool.get(ctx, settings.onebot_self_id)
        tokens = estimate_tokens(text)
        assert 4500 <= tokens <= 5200
        if ctx.private_owner:
            assert "本地表情协议" not in text
