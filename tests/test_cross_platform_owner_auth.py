"""Tests for cross-platform bound Owner authorization across commands, turns, patrol, and alerts."""

from __future__ import annotations

import asyncio
import io
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from PIL import Image

from hakimi.bot_server import is_persona_owner
from hakimi.generation_policy import Scenario
from hakimi.models import AudioRef, FileRef, ImageRef, Message, Video
from hakimi.modules.actions import Action, ActionCall, NoArgs, Outcome
from hakimi.modules.base import BaseModule
from hakimi.outbound_actions import ChatActionTrigger
from hakimi.security import Rejected, owner
from hakimi.service import Service
from hakimi.social import OwnerBinding
from tests.fakes import FakeBili, FakeGateway, FakeSender

TG_BOT_ID = "tg_bot_1"
TG_OWNER_UID = "888001"
TG_STRANGER_UID = "888002"
TG_GROUP_ID = "-1009001"


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (16, 16), (180, 120, 240)).save(buf, format="PNG")
    return buf.getvalue()


def _action_json(**overrides) -> str:
    base = {
        "abstain": False,
        "bubbles": ["好呀，收到啦~"],
        "anchor": {"reply_id": None, "mentions": []},
        "sticker": {"label": None, "timing": "after_text"},
        "image_digest": None,
        "emoji_like": None,
        "poke_qq": None,
        "save_meme_label": None,
        "set_longnick": None,
        "affection_delta": 0,
        "affection_deltas": None,
        "trigger_action": None,
        "voice_text": None,
        "sing_song": None,
    }
    base.update(overrides)
    return json.dumps(base, ensure_ascii=False)


class _TelegramModule(BaseModule):
    module_id = "telegram"
    display_name = "Telegram 社交"
    chat_platforms = ("telegram",)
    supports_voice = True
    preferred_voice_format = "ogg_opus"

    def __init__(self, settings):
        super().__init__(settings)
        self.sent: list[tuple[Message, str]] = []
        self.online = True

    async def _raw_sensor_summary(self) -> str:
        return ""

    def authorize_chat(self, message: Message, settings, *, bot_exempt: bool = False) -> bool:
        if message.bot_id != TG_BOT_ID:
            return False
        if message.kind == "private":
            return message.user_id in {TG_OWNER_UID, TG_STRANGER_UID}
        return message.conversation_id == TG_GROUP_ID and message.user_id in {
            TG_OWNER_UID,
            TG_STRANGER_UID,
        }

    def chat_connected(self, platform: str) -> bool:
        return self.online

    async def send_chat(self, message: Message, text: str, *, before_send):
        if not await before_send():
            raise Rejected("reply_invalidated")
        self.sent.append((message, text))
        return f"tg-msg-{len(self.sent)}"


class _DummyOwnerActionModule(BaseModule):
    module_id = "dummy_cross_plat"
    display_name = "跨平台测试动作"

    def __init__(self, settings):
        super().__init__(settings)
        self.calls: list[ActionCall] = []

    async def _raw_sensor_summary(self) -> str:
        return "待机"

    def actions(self) -> tuple[Action, ...]:
        async def _ping(call: ActionCall) -> Outcome:
            self.calls.append(call)
            return Outcome("done", "ok")

        return (
            Action(
                name="cross.ping",
                description="跨平台 Owner 直触测试动作",
                run=_ping,
                arguments=NoArgs,
                origins=frozenset({"owner_chat"}),
            ),
        )


def _tg_msg(
    *,
    user_id: str = TG_OWNER_UID,
    kind: str = "private",
    conversation_id: str | None = None,
    bot_id: str = TG_BOT_ID,
    text: str = "你好",
    event_id: str = "tg-1",
    sender_nickname: str = "BOISER",
    addressed: bool = True,
    images: list[ImageRef] | None = None,
    files: list[FileRef] | None = None,
    audios: list[AudioRef] | None = None,
    forward_ids: list[str] | None = None,
) -> Message:
    conv = (
        conversation_id
        if conversation_id is not None
        else (user_id if kind == "private" else TG_GROUP_ID)
    )
    return Message(
        platform="telegram",
        bot_id=bot_id,
        event_id=event_id,
        user_id=user_id,
        kind=kind,
        conversation_id=conv,
        text=text,
        sender_nickname=sender_nickname,
        addressed=addressed,
        created_at=time.time(),
        images=images or [],
        files=files or [],
        audios=audios or [],
        forward_ids=forward_ids or [],
    )


def _qq_msg(
    settings,
    *,
    user_id: str = "user-openid",
    kind: str = "private",
    conversation_id: str | None = None,
    text: str = "/哈基米 状态",
    event_id: str = "qq-1",
) -> Message:
    conv = (
        conversation_id
        if conversation_id is not None
        else (user_id if kind == "private" else "group-openid")
    )
    return Message(
        platform="qq",
        bot_id=settings.qq_app_id,
        event_id=event_id,
        user_id=user_id,
        kind=kind,
        conversation_id=conv,
        text=text,
        addressed=True,
        created_at=time.time(),
    )


def _configure_telegram_owner(settings, *, delivery_to_telegram: bool = True):
    settings.social_owner_bindings = {
        "telegram": OwnerBinding(
            bot_id=TG_BOT_ID,
            user_id=TG_OWNER_UID,
            notebook=True,
            mirror=True,
        )
    }
    if delivery_to_telegram:
        settings.owner_delivery_platform = "telegram"
        settings.proactive_dm_enabled = True


async def test_telegram_bound_owner_commands_and_strict_rejections(settings, store, monkeypatch):
    _configure_telegram_owner(settings)
    settings.bilibili_enabled = True
    settings.affection_enabled = True
    settings.qq_allowed_users.add("user-openid")
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    tg = _TelegramModule(settings)
    svc.modules.register(tg)
    try:
        await svc.memes.initialize()
        await svc.moderation.initialize()
        await svc.identity.initialize()
        await svc.affection.initialize()
        await svc.proactive.initialize()

        owner_dm = _tg_msg()
        owner_grp = _tg_msg(kind="group")
        stranger_dm = _tg_msg(user_id=TG_STRANGER_UID, sender_nickname="BOISER 主人")
        stranger_grp = _tg_msg(user_id=TG_STRANGER_UID, kind="group", sender_nickname="BOISER 主人")
        wrong_bot_dm = _tg_msg(bot_id="tg_bot_evil")
        qq_owner_spoof_dm = _qq_msg(settings, user_id="user-openid")
        qq_numeric_spoof_dm = _qq_msg(settings, user_id=settings.owner_qq_id)
        qq_grp = _qq_msg(settings, kind="group")

        # Bound TG owner is persona_owner (not OneBot owner); official QQ & strangers are neither.
        assert is_persona_owner(owner_dm, settings) is True
        assert owner(owner_dm, settings) is False
        assert is_persona_owner(stranger_dm, settings) is False
        assert is_persona_owner(wrong_bot_dm, settings) is False
        assert is_persona_owner(qq_owner_spoof_dm, settings) is False
        assert is_persona_owner(qq_numeric_spoof_dm, settings) is False

        # 1) Bound Telegram owner private commands succeed.
        status_reply = await svc.command(owner_dm.model_copy(update={"text": "/哈基米 状态"}))
        assert status_reply and "每日全局上限" in status_reply

        reports_reply = await svc.command(owner_dm.model_copy(update={"text": "/哈基米 报告"}))
        assert reports_reply and "巡逻摘要" in reports_reply

        sub_reply = await svc.command(owner_dm.model_copy(update={"text": "/哈基米 订阅 12345"}))
        assert sub_reply and "已订阅" in sub_reply and "私聊汇报" in sub_reply

        sub_list = await svc.command(owner_dm.model_copy(update={"text": "/哈基米 订阅列表"}))
        assert sub_list and "12345" in sub_list

        unsub_reply = await svc.command(owner_dm.model_copy(update={"text": "/哈基米 退订 12345"}))
        assert unsub_reply and "已取消" in unsub_reply

        # /affection: summary, unprefixed telegram target, explicit onebot & qq targets
        aff_summary = await svc.command(owner_dm.model_copy(update={"text": "/affection"}))
        assert aff_summary and "120 分" in aff_summary and "Lv.6" in aff_summary

        aff_set_tg = await svc.command(
            owner_dm.model_copy(update={"text": f"/affection {TG_STRANGER_UID} 77"})
        )
        assert aff_set_tg and "77 分" in aff_set_tg
        tg_prof = await svc.affection.profile("telegram", TG_STRANGER_UID)
        assert tg_prof["score"] == 77

        aff_query_tg = await svc.command(
            owner_dm.model_copy(update={"text": f"/affection telegram:{TG_STRANGER_UID}"})
        )
        assert aff_query_tg
        assert f"telegram:{TG_STRANGER_UID}" in aff_query_tg and "77 分" in aff_query_tg

        aff_set_qq = await svc.command(
            owner_dm.model_copy(update={"text": "/affection onebot:30000 66"})
        )
        assert aff_set_qq and "66 分" in aff_set_qq
        assert (await svc.affection.profile("onebot", "30000"))["score"] == 66

        aff_query_official = await svc.command(
            owner_dm.model_copy(update={"text": "/affection qq:user-openid"})
        )
        assert aff_query_official and "qq:user-openid" in aff_query_official

        # /学歌 & /换歌版本
        learn_mock = AsyncMock(return_value={"id": "job-tg-1"})
        switch_mock = AsyncMock(
            return_value={"title": "枝垂桜", "active_engine": "seed_vc", "active_take": "seed_vc"}
        )
        monkeypatch.setattr(svc.songs, "request_learn_song", learn_mock)
        monkeypatch.setattr(svc.songs, "switch_take", switch_mock)
        expand_mock = AsyncMock(side_effect=AssertionError("must not call OneBot expand_forwards"))
        monkeypatch.setattr(svc, "expand_forwards", expand_mock)

        learn_reply = await svc.command(
            owner_dm.model_copy(update={"text": "/学歌 晴天 pitch=-1", "forward_ids": ["fwd-1"]})
        )
        assert learn_reply and "job-tg-1" in learn_reply
        learn_mock.assert_awaited_once()
        assert expand_mock.await_count == 0

        switch_reply = await svc.command(
            owner_dm.model_copy(update={"text": "/换歌版本 枝垂桜 seed"})
        )
        assert switch_reply and "Seed-VC" in switch_reply
        switch_mock.assert_awaited_once()

        # /存表情 in private & group (using plugin_media ref, never OneBot expand_forwards)
        monkeypatch.setattr(svc.images, "fetch_raw", AsyncMock(return_value=_png_bytes()))
        meme_dm = await svc.command(
            owner_dm.model_copy(
                update={
                    "text": "/存表情 开心喵",
                    "images": [ImageRef(file="plugin_media:telegram:PH_1")],
                }
            )
        )
        assert meme_dm and "开心喵" in meme_dm
        meme_grp = await svc.command(
            owner_grp.model_copy(
                update={
                    "text": "/存表情 群开心",
                    "images": [ImageRef(file="plugin_media:telegram:PH_2")],
                    "forward_ids": ["fwd-2"],
                }
            )
        )
        assert meme_grp and "群开心" in meme_grp
        assert expand_mock.await_count == 0

        # Group-owner commands succeed for bound Telegram owner in group
        assert "已清空" in (
            await svc.command(owner_grp.model_copy(update={"text": "/new-session"})) or ""
        )
        assert "已启用" in (
            await svc.command(owner_grp.model_copy(update={"text": "/哈基米 记忆 开"})) or ""
        )
        assert "已关闭" in (
            await svc.command(owner_grp.model_copy(update={"text": "/哈基米 记忆 关"})) or ""
        )

        # OneBot-only /set-sign remains blocked on Telegram bound owner!
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.command(owner_dm.model_copy(update={"text": "/set-sign 今日摸鱼"}))

        # 2) Bound Telegram owner in group is rejected for owner_private commands.
        for cmd in (
            "/学歌 晴天",
            "/换歌版本 枝垂桜 seed",
            "/affection",
            "/哈基米 状态",
            "/哈基米 报告",
            "/哈基米 订阅 12345",
            "/哈基米 订阅列表",
        ):
            with pytest.raises(Rejected, match="owner_private_only"):
                await svc.command(owner_grp.model_copy(update={"text": cmd}))

        # 3) Unbound Telegram users, wrong bot_id, and official QQ OpenIDs are strictly rejected.
        for bad in (stranger_dm, wrong_bot_dm, qq_owner_spoof_dm, qq_numeric_spoof_dm):
            for cmd in (
                "/学歌 晴天",
                "/换歌版本 枝垂桜 seed",
                "/affection",
                "/哈基米 状态",
                "/哈基米 报告",
                "/哈基米 订阅 12345",
                "/哈基米 订阅列表",
                "/存表情 偷图",
            ):
                with pytest.raises(Rejected, match="owner_private_only"):
                    await svc.command(bad.model_copy(update={"text": cmd}))

        for bad_grp in (stranger_grp, qq_grp):
            for cmd in ("/new-session", "/哈基米 记忆 开", "/哈基米 记忆 关", "/存表情 偷图"):
                with pytest.raises(Rejected, match="group_owner_only"):
                    await svc.command(bad_grp.model_copy(update={"text": cmd}))
            with pytest.raises(Rejected, match="owner_private_only"):
                await svc.command(bad_grp.model_copy(update={"text": "/哈基米 状态"}))

        # 4) Disabling the telegram module immediately revokes bound owner command privileges.
        tg.enabled = False
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.command(owner_dm.model_copy(update={"text": "/哈基米 状态"}))
        with pytest.raises(Rejected, match="group_owner_only"):
            await svc.command(owner_grp.model_copy(update={"text": "/new-session"}))
    finally:
        await svc.stop()


async def test_telegram_owner_deep_touch_owner_and_verified_owner_turn(
    settings, store, monkeypatch
):
    _configure_telegram_owner(settings)
    settings.qq_allowed_users.add("user-openid")
    seen_payloads: list[dict] = []

    class _TurnGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.next_reply: str | None = None

        async def generate(self, payload, subject, **kwargs):
            seen_payloads.append(dict(payload))
            await super().generate(payload, subject, **kwargs)
            if self.next_reply is not None:
                return self.next_reply
            return "简单回复"

    gw = _TurnGateway()
    svc = Service(settings, store, gw, FakeBili(), FakeSender())
    tg = _TelegramModule(settings)
    dummy = _DummyOwnerActionModule(settings)
    svc.modules.register(tg)
    svc.modules.register(dummy)
    try:
        await svc.world_state.initialize()
        await svc.life_engine.initialize()
        await svc.memes.initialize()
        await svc.moderation.initialize()

        touch_mock = AsyncMock(wraps=svc.world_state.experience.touch_owner)
        monkeypatch.setattr(svc.world_state.experience, "touch_owner", touch_mock)
        longnick_mock = AsyncMock()
        monkeypatch.setattr(svc.longnick, "update", longnick_mock)
        invite_mock = AsyncMock(return_value=set())
        monkeypatch.setattr(svc.bot_guard, "invite", invite_mock)

        async def _run_turn(msg: Message) -> dict | None:
            assert await svc.accept_from(tg, msg) == "accepted"
            job = await store.claim()
            assert job is not None
            await svc.process(job)
            return job

        # 1) /deep succeeds for Telegram bound owner DM and calls touch_owner.
        await _run_turn(_tg_msg(event_id="deep-ok", text="/deep 解释一下协程原理"))
        gen_call = [c for c in gw.calls if c[0] == "generate"][-1]
        assert gen_call[3]["deep"] is True
        assert gen_call[3]["scenario"] == Scenario.OWNER_PRIVATE
        assert touch_mock.await_count == 1
        assert await svc.deliver_once()
        assert len(tg.sent) == 1 and not svc.sender.sent

        # 2) /deep rejected before any AI call for TG owner group, TG stranger, and official QQ.
        gw.calls.clear()
        for bad in (
            _tg_msg(event_id="deep-grp", kind="group", text="/deep 分析"),
            _tg_msg(event_id="deep-stranger", user_id=TG_STRANGER_UID, text="/deep 分析"),
        ):
            await _run_turn(bad)
        assert not gw.calls

        # Direct _handle_message check for official QQ OpenID /deep
        qq_deep = _qq_msg(settings, event_id="deep-qq", text="/deep 分析")
        assert await svc.accept(qq_deep) == "accepted"
        qq_job = await store.claim()
        assert qq_job is not None
        await svc.process(qq_job)
        assert not gw.calls

        # 3) Stranger normal chat does NOT call touch_owner and gets NO owner_chat_actions.
        touch_before = touch_mock.await_count
        seen_payloads.clear()
        await _run_turn(_tg_msg(event_id="stranger-hi", user_id=TG_STRANGER_UID, text="你好呀"))
        assert touch_mock.await_count == touch_before
        assert "owner_chat_actions" not in seen_payloads[-1]

        # 4) Telegram bound owner DM gets verified_owner_turn=True:
        #    - receives owner_chat_actions
        #    - triggers cross.ping via trigger_action
        #    - OneBot-only set_longnick and bot_guard.invite stay blocked!
        seen_payloads.clear()
        gw.next_reply = _action_json(
            bubbles=["好的这就帮你跑一下~"],
            set_longnick="不应在Telegram生效的QQ签名",
            trigger_action={"action_name": "cross.ping", "arguments_json": "{}"},
        )
        await _run_turn(_tg_msg(event_id="owner-act-1", text="帮我跑一下 cross.ping"))
        assert any(
            a["action_name"] == "cross.ping" for a in seen_payloads[-1]["owner_chat_actions"]
        )
        await asyncio.sleep(0.05)
        assert len(dummy.calls) == 1
        assert longnick_mock.await_count == 0
        assert invite_mock.await_count == 0

        # 5) Bound TG owner group chat gets verified_owner_turn=True and explicit voice bypass;
        #    mixed group batch (owner + stranger) gets verified_owner_turn=False!
        voice_explicit_flags: list[bool] = []

        def _fake_can_send_now(_scope, *, explicit=False, is_group=False):
            voice_explicit_flags.append(bool(explicit))
            return (True, "ok")

        monkeypatch.setattr(svc.voice_reply, "allowed_for_message", lambda *a, **kw: True)
        monkeypatch.setattr(svc.voice_reply, "can_send_now", _fake_can_send_now)
        seen_payloads.clear()
        gw.next_reply = _action_json(bubbles=[""], voice_text="哈基米在群里给你发语音啦")
        await _run_turn(_tg_msg(event_id="owner-grp-voice", kind="group", text="给我发个语音"))
        assert "owner_chat_actions" in seen_payloads[-1]
        assert voice_explicit_flags[-1] is True
        outbox_row = await store.one(
            "SELECT id, payload FROM outbox WHERE status='pending' ORDER BY created DESC LIMIT 1"
        )
        assert outbox_row is not None
        out_payload = store.vault.open(outbox_row["id"], outbox_row["payload"])
        assert out_payload["actions"]["explicit_voice"] is True
        while (claimed_outbox := await store.claim_reply()) is not None:
            await store.finish_reply(claimed_outbox["id"], "sent")

        # Stranger in group asking for voice does NOT get explicit_voice_bypass
        await _run_turn(
            _tg_msg(
                event_id="stranger-grp-voice",
                user_id=TG_STRANGER_UID,
                kind="group",
                text="给我发个语音",
            )
        )
        assert voice_explicit_flags[-1] is False

        # Mixed group batch: owner + stranger -> verified_owner_turn MUST be False
        seen_payloads.clear()
        gw.next_reply = _action_json(
            bubbles=["群里聊天"],
            trigger_action={"action_name": "cross.ping", "arguments_json": "{}"},
        )
        owner_grp_m = _tg_msg(event_id="mix-1", kind="group", text="一起聊")
        stranger_grp_m = _tg_msg(
            event_id="mix-2", user_id=TG_STRANGER_UID, kind="group", text="我也来"
        )
        fake_mixed_job = {
            "id": store.event_key(owner_grp_m),
            "kind": "message",
            "scope": store.session_scope(owner_grp_m),
            "subject": store.subject(owner_grp_m),
            "payload": owner_grp_m.model_dump(),
            "batch_messages": [stranger_grp_m.model_dump(), owner_grp_m.model_dump()],
            "expires": time.time() + 120,
        }
        await svc.handle_message(fake_mixed_job)
        await asyncio.sleep(0.05)
        assert "owner_chat_actions" not in seen_payloads[-1]
        assert len(dummy.calls) == 1  # Not triggered by mixed batch!
    finally:
        await svc.stop()


async def test_service_owner_actions_song_studio_cross_platform(settings, store, monkeypatch):
    _configure_telegram_owner(settings)
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    tg = _TelegramModule(settings)
    svc.modules.register(tg)
    try:
        await svc.life_engine.initialize()
        monkeypatch.setattr(svc.colab_compute, "configured", lambda: True)
        learn_mock = AsyncMock(return_value={"id": "job-action-1"})
        switch_mock = AsyncMock(
            return_value={"title": "晴天", "active_engine": "seed_vc", "active_take": "seed_vc"}
        )
        monkeypatch.setattr(svc.songs, "request_learn_song", learn_mock)
        monkeypatch.setattr(svc.songs, "switch_take", switch_mock)

        learn_trigger = ChatActionTrigger(
            action_name="song_studio.learn_song",
            arguments_json=json.dumps({"title": "晴天", "engine": "dual"}),
        )
        switch_trigger = ChatActionTrigger(
            action_name="song_studio.switch_version",
            arguments_json=json.dumps({"title": "晴天", "take": "seed_vc"}),
        )

        # Bound Telegram owner succeeds
        tg_owner = _tg_msg()
        await svc._run_owner_chat_action(learn_trigger, context_message=tg_owner)
        await svc._run_owner_chat_action(switch_trigger, context_message=tg_owner)
        assert learn_mock.await_count == 1
        assert switch_mock.await_count == 1

        # Unbound Telegram user and official QQ OpenID are rejected cleanly
        for bad in (
            _tg_msg(user_id=TG_STRANGER_UID),
            _qq_msg(settings, user_id="user-openid"),
            _qq_msg(settings, user_id=settings.owner_qq_id),
        ):
            await svc._run_owner_chat_action(learn_trigger, context_message=bad)
            await svc._run_owner_chat_action(switch_trigger, context_message=bad)
        assert learn_mock.await_count == 1
        assert switch_mock.await_count == 1
    finally:
        await svc.stop()


async def test_patrol_subscriptions_and_live_watches_for_telegram_owner(settings, store):
    _configure_telegram_owner(settings)
    settings.bilibili_enabled = True
    settings.patrol_interval_seconds = 0
    settings.qq_allowed_users.add("user-openid")

    v1 = Video(
        bvid="BV1xx411c7mD",
        mid="12345",
        title="旧视频",
        description="desc1",
        author="UP",
        published_at=100,
    )
    v2 = Video(
        bvid="BV1Q541167Qg",
        mid="12345",
        title="新视频",
        description="desc2",
        author="UP",
        published_at=200,
    )

    class _PollBili(FakeBili):
        def __init__(self):
            super().__init__([v1])
            self.live_payload = {
                "room_id": "778899",
                "live_status": 0,
                "title": "夜聊",
                "area": "科技",
            }
            self.live_calls: list[str] = []

        async def live_info(self, room_id: str):
            self.live_calls.append(room_id)
            return dict(self.live_payload, room_id=room_id)

    bili = _PollBili()
    svc = Service(settings, store, FakeGateway(), bili, FakeSender())
    tg = _TelegramModule(settings)
    svc.modules.register(tg)
    try:
        await svc.proactive.initialize()
        tg_owner = _tg_msg(text="/哈基米 订阅 12345")
        tg_stranger = _tg_msg(user_id=TG_STRANGER_UID, event_id="tg-stranger-sub")
        qq_user = _qq_msg(settings, user_id="user-openid", event_id="qq-sub")

        # Bound TG owner subscribes via command; seed unauthorized subscriptions directly in DB
        reply = await svc.command(tg_owner)
        assert reply and "已订阅" in reply
        await store.add_subscription(tg_stranger, "99991")
        await store.add_subscription(qq_user, "99992")

        # Seed live watches for TG owner + unauthorized TG stranger & QQ OpenID
        async with store.transaction():
            await store.db.execute(
                "INSERT INTO live_watches(room_id,target) VALUES (?,?)",
                ("778899", store.vault.seal("live:778899", tg_owner.model_dump())),
            )
            await store.db.execute(
                "INSERT INTO live_watches(room_id,target) VALUES (?,?)",
                ("778801", store.vault.seal("live:778801", tg_stranger.model_dump())),
            )
            await store.db.execute(
                "INSERT INTO live_watches(room_id,target) VALUES (?,?)",
                ("778802", store.vault.seal("live:778802", qq_user.model_dump())),
            )

        # Baseline poll: ONLY bound TG owner's mid & room are polled!
        await svc.poll_once(force=True)
        assert bili.calls == ["12345"]
        assert bili.live_calls == ["778899"]

        # Second poll: new video + live 0 -> 1
        bili.videos = [v2, v1]
        bili.live_payload["live_status"] = 1
        await svc.poll_once(force=True)
        assert bili.calls == ["12345", "12345"]
        assert bili.live_calls == ["778899", "778899"]

        # Claim and run the patrol job for the new video
        job = await store.claim()
        assert job is not None and job["kind"] == "patrol"
        await svc.process(job)

        # Deliver both queued patrol DMs (live open + UP new video) through Telegram!
        delivered = 0
        while await svc.proactive.deliver_once():
            delivered += 1
        assert delivered == 2
        assert len(tg.sent) == 2
        combined = "\n".join(text for _, text in tg.sent)
        assert "778899" in combined and "BV1Q541167Qg" in combined
        assert not svc.sender.sent

        # Reports are saved under the TG owner's subject and readable via /哈基米 报告
        report_text = await svc.command(tg_owner.model_copy(update={"text": "/哈基米 报告"}))
        assert report_text and "778899" in report_text and "BV1Q541167Qg" in report_text

        # Direct handle_patrol rejects unbound TG stranger & QQ OpenID jobs even if queued
        for bad_target, mid in ((tg_stranger, "99991"), (qq_user, "99992")):
            bad_subj = store.subject(bad_target)
            bad_sub = (await store.subscriptions(bad_subj))[0]
            gw_before = len(svc.ai.calls)
            await svc.handle_patrol(
                {
                    "id": f"patrol-{mid}",
                    "kind": "patrol",
                    "subject": bad_subj,
                    "payload": {"subscription": bad_sub["id"], "videos": [v2.model_dump()]},
                }
            )
            assert len(svc.ai.calls) == gw_before
    finally:
        await svc.stop()


async def test_affection_blacklisted_alert_and_moderation_cross_platform(settings, store, message):
    _configure_telegram_owner(settings)
    settings.affection_enabled = True
    settings.qq_allowed_users.add("user-openid")
    onebot_wire = SimpleNamespace(call=AsyncMock(return_value=[{"user_id": 30000}]))
    sender = FakeSender()
    sender.onebot = onebot_wire

    svc = Service(settings, store, FakeGateway(), FakeBili(), sender)
    tg = _TelegramModule(settings)
    svc.modules.register(tg)
    try:
        await svc.proactive.initialize()
        await svc.moderation.initialize()
        await svc.affection.initialize()

        # 1) Affection-zero blacklist alert delivers via Telegram when configured.
        offender_tg = _tg_msg(user_id=TG_STRANGER_UID, kind="group", sender_nickname="暴躁网友")
        fake_update = SimpleNamespace(nickname="暴躁网友", reason="severe_offense")
        await svc._alert_affection_blacklisted(offender_tg, fake_update)
        assert await svc.proactive.deliver_once()
        assert len(tg.sent) == 1
        assert "暴躁网友" in tg.sent[0][1] and f"telegram:{TG_STRANGER_UID}" in tg.sent[0][1]

        # Gracefully handles missing proactive attribute
        saved_proactive = svc.proactive
        svc.proactive = None
        await svc._alert_affection_blacklisted(offender_tg, fake_update)
        svc.proactive = saved_proactive

        # 2) Moderation 3-strike mute proposal alert delivers to Telegram too!
        qq_offender = message.model_copy(
            update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
        )
        proposal = None
        for idx in range(3):
            proposal = await svc.moderation.record_strike(
                qq_offender.model_copy(update={"event_id": f"strike-{idx}"}),
                "prompt_injection",
                "捣蛋鬼",
            )
        assert proposal is not None
        assert await svc.proactive.deliver_once()
        assert len(tg.sent) == 2
        assert proposal in tg.sent[1][1] and "30000" in tg.sent[1][1]
        assert "QQ 私聊中使用 --delete-friend" in tg.sent[1][1]

        # 3) Moderation commands from Telegram bound owner:
        #    - /mute-approve & /unmute & /mute work (application-level)
        #    - --delete-friend stays OneBot-only (rejected on Telegram!)
        tg_owner_dm = _tg_msg()
        tg_owner_grp = _tg_msg(kind="group")
        tg_stranger_dm = _tg_msg(user_id=TG_STRANGER_UID)
        qq_spoof_dm = _qq_msg(settings, user_id="user-openid")

        # --delete-friend on Telegram bound owner is strictly rejected (never calls OneBot RPC)
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.moderation.command(
                tg_owner_dm.model_copy(update={"text": f"/mute-approve {proposal} --delete-friend"})
            )
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.moderation.command(
                tg_owner_dm.model_copy(update={"text": "/mute 30000 --delete-friend"})
            )
        assert not onebot_wire.call.called
        assert "30000" not in settings.muted_user_ids

        # Application-level /mute-approve, /unmute, /mute succeed from Telegram bound owner DM
        approve_reply = await svc.moderation.command(
            tg_owner_dm.model_copy(update={"text": f"/mute-approve {proposal}"})
        )
        assert approve_reply and "已批准" in approve_reply
        assert "30000" in settings.muted_user_ids
        assert not onebot_wire.call.called

        unmute_reply = await svc.moderation.command(
            tg_owner_dm.model_copy(update={"text": "/unmute 30000"})
        )
        assert unmute_reply and "已解除" in unmute_reply
        assert "30000" not in settings.muted_user_ids

        mute_reply = await svc.moderation.command(
            tg_owner_dm.model_copy(update={"text": "/mute 30000"})
        )
        assert mute_reply and "已" in mute_reply
        assert "30000" in settings.muted_user_ids

        # Unbound users, Telegram group, and official QQ OpenIDs are rejected on all mute commands
        for bad in (tg_owner_grp, tg_stranger_dm, qq_spoof_dm):
            for cmd in ("/mute 30000", "/unmute 30000", f"/mute-reject {proposal}"):
                with pytest.raises(Rejected, match="owner_private_only"):
                    await svc.moderation.command(bad.model_copy(update={"text": cmd}))

        # OneBot owner DM still can use --delete-friend
        await svc.moderation.set_muted("30000", False)
        onebot_wire.call.side_effect = [[{"user_id": 30000}], {}]
        ob_reply = await svc.moderation.command(
            message.model_copy(update={"text": "/mute 30000 --delete-friend"})
        )
        assert ob_reply and "done" in ob_reply
        assert onebot_wire.call.await_count == 2
    finally:
        await svc.stop()
