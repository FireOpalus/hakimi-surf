"""Tests for Phase 3B R4: BaseModule.apply_presence hook and 4 kernel call sites."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi.adapters import Sender
from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.longnick import LongnickConfig
from hakimi.models import Message
from hakimi.modules.base import BaseModule, ModuleRegistry
from hakimi.modules.qq_social import QQSocialModule
from hakimi.plugin_bus import RemoteModuleProxy
from hakimi.proactive_dm import OwnerProactive
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.social import OwnerBinding
from hakimi.store import Store
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_proactive_dm import Wire

TZ = ZoneInfo("Asia/Shanghai")


def _stamp(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


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


class _PlainModule(BaseModule):
    module_id = "plain_mod"
    display_name = "Plain"
    chat_platforms = ("plain",)

    async def _raw_sensor_summary(self) -> str:
        return "ok"


class _PresenceModule(BaseModule):
    module_id = "custom_presence"
    display_name = "Custom Presence"
    chat_platforms = ("telegram",)
    supports_presence = True

    def __init__(
        self,
        settings=None,
        *,
        enabled_flag: bool = True,
        boom: BaseException | None = None,
    ):
        super().__init__(settings)
        self.enabled_flag = enabled_flag
        self.boom = boom
        self.calls: list[tuple[str, str]] = []
        self.sent: list[tuple[Message, str]] = []

    async def _raw_sensor_summary(self) -> str:
        return "presence"

    def authorize_chat(self, message: Message, settings, *, bot_exempt: bool = False) -> bool:
        return message.platform == "telegram"

    def chat_connected(self, platform: str) -> bool:
        return platform == "telegram" and self.inactive_reason is None

    async def send_chat(self, message: Message, text: str, *, before_send):
        if not await before_send():
            raise Rejected("reply_invalidated")
        self.sent.append((message, text))
        return f"msg-{len(self.sent)}"

    def presence_enabled(self) -> bool:
        return self.inactive_reason is None and self.enabled_flag

    async def apply_presence(
        self,
        mood_text: str,
        *,
        kind: str = "life_block",
    ) -> dict | None:
        if self.boom is not None:
            raise self.boom
        self.calls.append((kind, mood_text))
        return {"status": "updated", "kind": kind, "text": mood_text}


class _NoPresenceTelegramModule(BaseModule):
    module_id = "telegram"
    display_name = "Telegram"
    chat_platforms = ("telegram",)

    def __init__(self, settings=None):
        super().__init__(settings)
        self.sent: list[tuple[Message, str]] = []

    async def _raw_sensor_summary(self) -> str:
        return "tg"

    def authorize_chat(self, message: Message, settings, *, bot_exempt: bool = False) -> bool:
        return message.platform == "telegram"

    def chat_connected(self, platform: str) -> bool:
        return platform == "telegram" and self.inactive_reason is None

    async def send_chat(self, message: Message, text: str, *, before_send):
        if not await before_send():
            raise Rejected("reply_invalidated")
        self.sent.append((message, text))
        return f"tg-{len(self.sent)}"


def _configure_tg_owner(settings, *, bot_id: str = "tg-bot-1", user_id: str = "888001") -> None:
    settings.social_owner_bindings = {
        "telegram": OwnerBinding(bot_id=bot_id, user_id=user_id, notebook=True, mirror=True)
    }
    settings.owner_delivery_platform = "telegram"
    settings.proactive_dm_enabled = True


async def test_base_module_and_remote_proxy_presence_defaults(settings):
    plain = _PlainModule(settings)
    assert plain.supports_presence is False
    assert plain.presence_platforms == ()
    assert plain.presence_enabled() is False
    assert await plain.apply_presence("心情不错") is None

    proxy = RemoteModuleProxy(
        settings,
        SimpleNamespace(),  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        plugin_id="telegram",
        display_name="Telegram",
        description="Remote TG",
        node_region="KR",
        scopes=frozenset({"chat"}),
        chat_platforms=("telegram",),
        thinking_scenes=(),
        actions=(),
        capabilities=("voice",),
        enabled=True,
    )
    assert proxy.supports_presence is False
    assert proxy.presence_enabled() is False
    assert await proxy.apply_presence("心情不错", kind="digest") is None

    reg = ModuleRegistry()
    reg.register(plain)
    reg.register(proxy)
    assert reg.supports_presence_for("plain") is False
    assert reg.supports_presence_for("telegram") is False
    assert reg.presence_enabled_for("telegram") is False
    assert await reg.apply_presence("心情不错", platform="telegram") is None
    assert await reg.apply_presence("心情不错") is None


async def test_qq_social_module_presence_dynamic_lookup_platforms_and_kinds(settings):
    no_svc = QQSocialModule(settings)
    assert no_svc.supports_presence is True
    assert no_svc.presence_platforms == ("onebot",)
    assert no_svc.presence_enabled() is False
    assert await no_svc.apply_presence("无服务") is None

    fake_longnick = SimpleNamespace(
        config=SimpleNamespace(enabled=False),
        module_enabled=lambda: True,
        apply_digest=AsyncMock(return_value={"status": "digest-ok"}),
        apply_patrol=AsyncMock(return_value={"status": "patrol-ok"}),
        apply_life_block=AsyncMock(return_value={"status": "life-ok"}),
        update=AsyncMock(return_value={"status": "chat-ok"}),
    )
    svc = SimpleNamespace(longnick=fake_longnick, hot=None, sender=None, proactive=None)
    qq_mod = QQSocialModule(settings, service=svc, connected=lambda: True)

    reg = ModuleRegistry()
    reg.register(qq_mod)

    # presence_platforms=("onebot",) excludes official "qq" OpenID even with chat_platforms
    assert reg.supports_presence_for("onebot") is True
    assert reg.supports_presence_for("qq") is False
    assert reg.presence_enabled_for("onebot") is False
    assert reg.presence_enabled_for("qq") is False
    assert await reg.apply_presence("不应触发QQ", platform="qq") is None

    # Dynamic lookup reflects config changes & monkeypatched longnick after construction
    fake_longnick.config.enabled = True
    assert reg.presence_enabled_for("onebot") is True

    assert await reg.apply_presence("小报心情", kind="digest", platform="onebot") == {
        "status": "digest-ok"
    }
    fake_longnick.apply_digest.assert_awaited_once_with("小报心情")

    assert await reg.apply_presence("巡逻心情", kind="patrol", platform="onebot") == {
        "status": "patrol-ok"
    }
    fake_longnick.apply_patrol.assert_awaited_once_with("巡逻心情")

    assert await reg.apply_presence("时段心情", kind="life_block", platform="onebot") == {
        "status": "life-ok"
    }
    fake_longnick.apply_life_block.assert_awaited_once_with("时段心情")

    assert await reg.apply_presence("聊天签名", kind="chat", platform="onebot") == {
        "status": "chat-ok"
    }
    fake_longnick.update.assert_awaited_once_with("聊天签名", automatic=False)

    # Replacing svc.longnick dynamically (without apply_life_block) falls back to apply_patrol
    fallback_patrol = AsyncMock(return_value={"status": "fallback-patrol"})
    svc.longnick = SimpleNamespace(
        config=SimpleNamespace(enabled=True),
        apply_patrol=fallback_patrol,
    )
    assert await qq_mod.apply_presence("回退心情", kind="life_block") == {
        "status": "fallback-patrol"
    }
    fallback_patrol.assert_awaited_once_with("回退心情")

    # Fail-closed when module is disabled
    qq_mod.enabled = False
    assert qq_mod.presence_enabled() is False
    assert await qq_mod.apply_presence("禁用时无效", kind="chat") is None
    assert reg.supports_presence_for("onebot") is False
    assert reg.presence_enabled_for("onebot") is False


async def test_registry_apply_presence_isolation_and_cancelled_error(settings, caplog):
    bad_mod = _PresenceModule(settings, boom=RuntimeError("boom"))
    bad_mod.module_id = "bad_mod"
    bad_mod.chat_platforms = ("bad_plat",)

    good_mod = _PresenceModule(settings)
    good_mod.module_id = "good_mod"
    good_mod.chat_platforms = ("good_plat",)

    reg = ModuleRegistry()
    reg.register(bad_mod)
    reg.register(good_mod)

    with caplog.at_level(logging.WARNING):
        res = await reg.apply_presence("一起更新", kind="patrol")
    assert res == {"good_mod": {"status": "updated", "kind": "patrol", "text": "一起更新"}}
    assert good_mod.calls == [("patrol", "一起更新")]
    assert "event=presence_apply_failed" in caplog.text

    assert await reg.apply_presence("单平台失败", kind="digest", platform="bad_plat") is None

    # asyncio.CancelledError must ALWAYS propagate
    cancel_mod = _PresenceModule(settings, boom=asyncio.CancelledError())
    cancel_mod.module_id = "cancel_mod"
    cancel_mod.chat_platforms = ("cancel_plat",)
    reg.register(cancel_mod)

    with pytest.raises(asyncio.CancelledError):
        await reg.apply_presence("取消测试", kind="life_block")
    with pytest.raises(asyncio.CancelledError):
        await reg.apply_presence("取消测试", kind="chat", platform="cancel_plat")


async def test_site1_life_engine_apply_presence_qq_vs_custom_vs_noop(settings, store, monkeypatch):
    now = _stamp("2026-11-17T14:00:00")
    settings.onebot_enabled = True
    settings.owner_qq_id = "20000"
    settings.onebot_allowed_groups = {"40000"}
    settings.group_policies = {"onebot:40000": "smart_reply"}
    settings.enable_daily_longnick = True

    class _PlanGateway(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", payload, kwargs))
            return json.dumps(
                {
                    "current_module": "qq_social",
                    "focus_level": "LOW",
                    "mood": "curious",
                    "energy": 80,
                    "boredom": 20,
                    "mood_longnick": "下午茶与代码时间",
                    "intents": [],
                },
                ensure_ascii=False,
            )

    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, _PlanGateway(), FakeBili(), Sender(settings, http, wire))
        try:
            svc.world_state.clock = lambda: now
            await svc.world_state.initialize()
            engine = LifeEngine(svc, clock=lambda: now)
            await engine.initialize()

            # Register a custom presence module alongside qq_social
            custom_tg = _PresenceModule(settings)
            svc.modules.register(custom_tg)

            qq_mod = svc.modules.get("qq_social")
            assert qq_mod is not None
            qq_apply_mock = AsyncMock(return_value={"status": "updated"})
            monkeypatch.setattr(svc.longnick, "apply_life_block", qq_apply_mock)

            res = await engine.trigger_plan_once(now=now)
            assert res["status"] == "planned"
            assert res["source"] == "llm"
            qq_apply_mock.assert_awaited_once_with("下午茶与代码时间")
            assert custom_tg.calls == [("life_block", "下午茶与代码时间")]

            # Disabled qq_social + failing custom_tg still lets plan_once complete cleanly
            qq_mod.enabled = False
            custom_tg.boom = RuntimeError("presence_down")
            res2 = await engine.trigger_plan_once(now=now + 60)
            assert res2["status"] == "planned"
        finally:
            await svc.stop()


async def test_site2_proactive_dm_digest_presence_telegram_vs_onebot_vs_custom(
    settings, store, tmp_path
):
    now = _stamp("2026-11-17T22:00:00")
    _configure_tg_owner(settings)
    settings.onebot_enabled = False
    settings.owner_qq_id = ""

    class _DigestGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.with_longnick_flags: list[bool] = []

        async def generate(self, payload, subject, **kwargs):
            self.with_longnick_flags.append(bool(kwargs.get("with_longnick", False)))
            self.calls.append(("generate", payload, kwargs))
            if kwargs.get("with_longnick"):
                return json.dumps(
                    {"digest": "今晚去巡逻了一圈喵。", "daily_longnick": "晚风轻拂"},
                    ensure_ascii=False,
                )
            return "今晚去巡逻了一圈喵。"

    # 1) Non-presence Telegram module -> with_longnick NOT requested
    gw1 = _DigestGateway()
    svc1 = Service(settings, store, gw1, FakeBili(), FakeSender())
    tg_plain = _NoPresenceTelegramModule(settings)
    svc1.modules.register(tg_plain)
    try:
        svc1.proactive = OwnerProactive(svc1, clock=lambda: now)
        svc1.life_engine = LifeEngine(svc1, clock=lambda: now)
        svc1.world_state.clock = lambda: now
        await svc1.life_engine.initialize()
        await svc1.proactive.initialize()
        res1 = await svc1.proactive.prepare_digest(automatic=False)
        assert res1["status"] == "queued"
        assert gw1.with_longnick_flags == [False]
    finally:
        await svc1.stop()

    # 2) Custom presence-enabled Telegram module -> with_longnick=True & kind="digest" invoked
    s2 = settings.model_copy(update={"data_dir": tmp_path / "svc2"})
    store2 = Store(s2)
    await store2.open()
    gw2 = _DigestGateway()
    svc2 = Service(s2, store2, gw2, FakeBili(), FakeSender())
    tg_presence = _PresenceModule(s2, enabled_flag=True)
    svc2.modules.register(tg_presence)
    try:
        svc2.proactive = OwnerProactive(svc2, clock=lambda: now)
        svc2.life_engine = LifeEngine(svc2, clock=lambda: now)
        svc2.world_state.clock = lambda: now
        await svc2.life_engine.initialize()
        await svc2.proactive.initialize()
        res2 = await svc2.proactive.prepare_digest(automatic=False)
        assert res2["status"] == "queued"
        assert gw2.with_longnick_flags == [True]
        assert tg_presence.calls == [("digest", "晚风轻拂")]

        # 3) CancelledError from apply_presence propagates instead of being swallowed
        s3 = settings.model_copy(update={"data_dir": tmp_path / "svc3"})
        store3 = Store(s3)
        await store3.open()
        svc3 = Service(s3, store3, _DigestGateway(), FakeBili(), FakeSender())
        tg_cancel = _PresenceModule(s3, enabled_flag=True, boom=asyncio.CancelledError())
        svc3.modules.register(tg_cancel)
        try:
            svc3.proactive = OwnerProactive(svc3, clock=lambda: now)
            svc3.life_engine = LifeEngine(svc3, clock=lambda: now)
            svc3.world_state.clock = lambda: now
            await svc3.life_engine.initialize()
            await svc3.proactive.initialize()
            with pytest.raises(asyncio.CancelledError):
                await svc3.proactive.prepare_digest(automatic=False)
        finally:
            await svc3.stop()
            await store3.close()
    finally:
        await svc2.stop()
        await store2.close()

    # 4) OneBot with longnick enabled -> with_longnick=True & apply_digest called
    s_ob = settings.model_copy(
        update={
            "data_dir": tmp_path / "svc4",
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "owner_qq_id": "20000",
            "owner_delivery_platform": "onebot",
            "social_owner_bindings": {},
            "enable_daily_longnick": True,
        }
    )
    store4 = Store(s_ob)
    await store4.open()
    gw4 = _DigestGateway()
    wire4 = Wire()
    async with httpx.AsyncClient() as http:
        svc4 = Service(s_ob, store4, gw4, FakeBili(), Sender(s_ob, http, wire4))
        try:
            svc4.proactive = OwnerProactive(svc4, clock=lambda: now)
            svc4.life_engine = LifeEngine(svc4, clock=lambda: now)
            svc4.world_state.clock = lambda: now
            await svc4.life_engine.initialize()
            await svc4.proactive.initialize()
            await svc4.longnick.initialize()
            await svc4.longnick.set_config(LongnickConfig(enabled=True))
            res4 = await svc4.proactive.prepare_digest(automatic=False)
            assert res4["status"] == "queued"
            assert gw4.with_longnick_flags == [True]
            assert any(
                act == "set_self_longnick" and params["longNick"] == "晚风轻拂"
                for act, params in wire4.calls
            )
        finally:
            await svc4.stop()
            await store4.close()


async def test_site3_domain_bridge_patrol_presence_and_private_first_skip(settings, monkeypatch):
    from tests.test_plugin_bus_phase3 import (
        _bili_hello,
        _get_bus,
        _phase3_settings,
        _Phase3Socket,
    )

    s = _phase3_settings(settings)
    s.onebot_enabled = True
    s.owner_qq_id = "20000"
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        bus = _get_bus(svc)
        await svc.world_state.initialize()
        svc.modules.unregister("bilibili_surf")
        await bus.create_plugin(
            "bilibili_surf",
            "B 站巡逻",
            ["actions", "sensors", "thinking_scenes", "node_bridge", "owner_notify"],
        )

        custom_mod = _PresenceModule(s)
        svc.modules.register(custom_mod)

        # Monkeypatch svc.longnick.apply_patrol while config.enabled remains False (default)
        patrol_calls: list[str] = []

        async def _fake_patrol(text: str):
            patrol_calls.append(text)
            return {"status": "updated"}

        monkeypatch.setattr(svc.longnick, "apply_patrol", _fake_patrol)

        b_sock = _Phase3Socket()
        b_run = asyncio.create_task(
            bus.serve(
                b_sock.receive,
                b_sock.send,
                b_sock.close,
                source_ip="127.0.0.1",
                plugin_id="bilibili_surf",
            )
        )
        await b_sock.push(_bili_hello())
        await b_sock.recv_until(lambda m: m.get("type") == "plugin.ready")

        # Public patrol event dispatches to all active presence modules
        await b_sock.push(
            {
                "type": "world.event",
                "id": "we-r4-public",
                "event_type": "bili_video_watched",
                "payload": {"bvid": "BV1xx411c7mD", "title": "测试"},
                "mood_longnick": "B站刷到有意思的猫咪视频",
            }
        )
        assert (await b_sock.recv_until(lambda m: m.get("id") == "we-r4-public"))[-1]["accepted"]
        assert patrol_calls == ["B站刷到有意思的猫咪视频"]
        assert custom_mod.calls == [("patrol", "B站刷到有意思的猫咪视频")]

        # private_first=True must NOT dispatch apply_presence
        await b_sock.push(
            {
                "type": "world.event",
                "id": "we-r4-priv",
                "event_type": "bili_video_watched",
                "payload": {"bvid": "BV1Q541167Qg", "title": "私密", "private_first": True},
                "mood_longnick": "不应外显的私密内容",
            }
        )
        assert (await b_sock.recv_until(lambda m: m.get("id") == "we-r4-priv"))[-1]["accepted"]
        assert patrol_calls == ["B站刷到有意思的猫咪视频"]
        assert custom_mod.calls == [("patrol", "B站刷到有意思的猫咪视频")]

        await b_sock.close()
        await b_run


async def test_site4_service_turn_chat_presence_gates_and_isolation(settings, store, monkeypatch):
    settings.admin_token = SecretStr(TOKEN)
    settings.onebot_enabled = True
    settings.onebot_mode = "ws_reverse"
    settings.owner_qq_id = "20000"
    settings.enable_daily_longnick = False  # chat manual updates must still work!
    settings.social_owner_bindings = {"telegram": OwnerBinding(bot_id="tg-bot-1", user_id="888001")}

    class _TurnGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.next_reply = ""

        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", payload, kwargs))
            return self.next_reply

    gw = _TurnGateway()
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, gw, FakeBili(), Sender(settings, http, wire))
        try:
            await svc.world_state.initialize()
            await svc.life_engine.initialize()
            await svc.memes.initialize()
            await svc.moderation.initialize()

            # 1) OneBot Owner private set_longnick succeeds via qq_social.apply_presence
            gw.next_reply = _action_json(bubbles=["改好啦"], set_longnick="猫猫午睡中")
            ob_msg = Message(
                platform="onebot",
                bot_id="10000",
                event_id="ob-chat-1",
                user_id="20000",
                kind="private",
                conversation_id="20000",
                text="帮我换个签名",
                addressed=True,
                created_at=time.time(),
            )
            assert await svc.accept(ob_msg) == "accepted"
            job = await store.claim()
            assert job is not None
            await svc.process(job)
            await store.finish(job["id"], "done")
            assert await svc.deliver_once()
            assert any(
                act == "set_self_longnick" and params["longNick"] == "猫猫午睡中"
                for act, params in wire.calls
            )

            # 2) Disabled qq_social blocks set_longnick even when a OneBot Owner job is processed
            wire.calls.clear()
            qq_mod = svc.modules.get("qq_social")
            assert qq_mod is not None
            gw.next_reply = _action_json(bubbles=["收到"], set_longnick="禁用时不应调用")
            ob_msg2 = ob_msg.model_copy(update={"event_id": "ob-chat-2"})
            assert await svc.accept(ob_msg2) == "accepted"
            job2 = await store.claim()
            assert job2 is not None
            qq_mod.enabled = False
            await svc.process(job2)
            await store.finish(job2["id"], "done")
            assert not any(act == "set_self_longnick" for act, _ in wire.calls)

            # 2b) Non-presence Telegram module blocks set_longnick for bound Telegram Owner
            tg_plain = _NoPresenceTelegramModule(settings)
            svc.modules.register(tg_plain)
            tg_plain_owner = Message(
                platform="telegram",
                bot_id="tg-bot-1",
                event_id="tg-plain-chat-1",
                user_id="888001",
                kind="private",
                conversation_id="888001",
                text="帮我换个签名",
                addressed=True,
                created_at=time.time(),
            )
            gw.next_reply = _action_json(bubbles=["收到"], set_longnick="TG无presence不应调用")
            assert await svc.accept_from(tg_plain, tg_plain_owner) == "accepted"
            job_tg_plain = await store.claim()
            assert job_tg_plain is not None
            await svc.process(job_tg_plain)
            await store.finish(job_tg_plain["id"], "done")
            assert await svc.deliver_once()
            assert not any(act == "set_self_longnick" for act, _ in wire.calls)
            svc.modules.unregister("telegram")

            # 3) Custom presence-enabled module executes apply_presence(kind="chat")
            tg_presence = _PresenceModule(settings)
            svc.modules.register(tg_presence)
            longnick_spy = AsyncMock()
            monkeypatch.setattr(svc.longnick, "update", longnick_spy)

            gw.next_reply = _action_json(bubbles=["TG签名也改啦"], set_longnick="TG同步签名")
            tg_owner = Message(
                platform="telegram",
                bot_id="tg-bot-1",
                event_id="tg-chat-1",
                user_id="888001",
                kind="private",
                conversation_id="888001",
                text="改一下TG状态",
                addressed=True,
                created_at=time.time(),
            )
            assert await svc.accept_from(tg_presence, tg_owner) == "accepted"
            job3 = await store.claim()
            assert job3 is not None
            await svc.process(job3)
            await store.finish(job3["id"], "done")
            assert await svc.deliver_once()
            assert tg_presence.calls == [("chat", "TG同步签名")]
            assert longnick_spy.await_count == 0

            # 4) Mixed group batch (owner + stranger) blocks set_longnick
            stranger = tg_owner.model_copy(
                update={
                    "event_id": "tg-mix-stranger",
                    "user_id": "999999",
                    "kind": "group",
                    "conversation_id": "-1001",
                }
            )
            owner_grp = tg_owner.model_copy(
                update={
                    "event_id": "tg-mix-owner",
                    "kind": "group",
                    "conversation_id": "-1001",
                }
            )
            mixed_job = {
                "id": store.event_key(owner_grp),
                "kind": "message",
                "scope": store.session_scope(owner_grp),
                "subject": store.subject(owner_grp),
                "payload": owner_grp.model_dump(),
                "batch_messages": [stranger.model_dump(), owner_grp.model_dump()],
                "expires": 9999999999.0,
            }
            await svc.handle_message(mixed_job)
            assert tg_presence.calls == [("chat", "TG同步签名")]

            # 5) RuntimeError inside apply_presence is isolated and reply still enqueues
            tg_presence.boom = RuntimeError("presence_boom")
            tg_owner2 = tg_owner.model_copy(update={"event_id": "tg-chat-err"})
            gw.next_reply = _action_json(bubbles=["消息正常发"], set_longnick="会炸的签名")
            assert await svc.accept_from(tg_presence, tg_owner2) == "accepted"
            job4 = await store.claim()
            assert job4 is not None
            await svc.process(job4)
            await store.finish(job4["id"], "done")
            while await svc.deliver_once():
                pass
            assert any(text == "消息正常发" for _, text in tg_presence.sent)

            # 6) CancelledError inside apply_presence propagates out of service_turn
            tg_presence.boom = asyncio.CancelledError()
            tg_owner3 = tg_owner.model_copy(update={"event_id": "tg-chat-cancel"})
            assert await svc.accept_from(tg_presence, tg_owner3) == "accepted"
            job5 = await store.claim()
            assert job5 is not None
            with pytest.raises(asyncio.CancelledError):
                await svc.process(job5)
        finally:
            await svc.stop()
