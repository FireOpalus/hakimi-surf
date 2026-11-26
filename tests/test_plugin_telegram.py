"""Unit and E2E tests for the Telegram remote plugin and platform-agnostic PluginBus hooks."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
import logging
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.bot_server import is_persona_owner
from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.life_engine import LifeEngine
from hakimi.mentions import AT_INSTRUCTION
from hakimi.models import AudioRef, FileRef, ImageRef, Message
from hakimi.modules.actions import ActionCall
from hakimi.platform_hooks import get_platform_hooks
from hakimi.plugin_bus import PluginBus
from hakimi.plugin_kernel import PluginKernel
from hakimi.routing import RouteDecision
from hakimi.social import OwnerBinding
from plugins.telegram import TelegramApiError, TelegramBotApi, TelegramPlugin
from plugins.telegram.tg_api import split_telegram_text
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN

BOT_TOKEN = "99887766:AAFakeSecretTelegramTokenForUnitTestsOnly"
BOT_ID = "99887766"
OWNER_UID = "555000111"
FRIEND_UID = "555000222"
STRANGER_UID = "999000999"
GROUP_ID = "-1001234567890"
OTHER_GROUP_ID = "-1009999999999"


def _make_png_bytes(color: tuple[int, int, int] = (40, 120, 220)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (64, 64), color=color).save(buf, format="PNG")
    return buf.getvalue()


def _make_gif_bytes() -> bytes:
    buf = io.BytesIO()
    frames = [
        Image.new("RGB", (32, 32), color=(220, 60, 80)),
        Image.new("RGB", (32, 32), color=(60, 160, 220)),
    ]
    frames[0].save(buf, format="GIF", save_all=True, append_images=frames[1:], duration=100, loop=0)
    return buf.getvalue()


class _LoopbackBridge:
    """In-memory bidirectional WebSocket pair connecting HakimiPluginClient <-> PluginBus.serve."""

    def __init__(self, bus: PluginBus, plugin_id: str) -> None:
        self.bus = bus
        self.plugin_id = plugin_id
        self.c2s: asyncio.Queue[str | None] = asyncio.Queue()
        self.s2c: asyncio.Queue[str | None] = asyncio.Queue()
        self.server_task: asyncio.Task[None] | None = None
        self.closed = False

    def connect_factory(self, _url: str, **_kwargs: Any) -> Any:
        bridge = self

        class _ClientConn:
            async def __aenter__(self) -> _ClientConn:
                if bridge.server_task is None or bridge.server_task.done():
                    bridge.server_task = asyncio.create_task(
                        bridge.bus.serve(
                            bridge._server_recv,
                            bridge._server_send,
                            bridge._server_close,
                            source_ip="127.0.0.1",
                            plugin_id=bridge.plugin_id,
                        )
                    )
                return self

            async def __aexit__(self, *_exc: Any) -> None:
                await self.close()

            async def send(self, raw: str) -> None:
                await bridge.c2s.put(raw)

            async def recv(self) -> str:
                item = await bridge.s2c.get()
                if item is None:
                    raise RuntimeError("ws closed")
                return item

            async def close(self, code: int = 1000) -> None:
                if not bridge.closed:
                    bridge.closed = True
                    await bridge.c2s.put(None)
                    await bridge.s2c.put(None)

        return _ClientConn()

    async def _server_recv(self) -> str:
        item = await self.c2s.get()
        if item is None:
            raise RuntimeError("client eof")
        return item

    async def _server_send(self, raw: str) -> None:
        await self.s2c.put(raw)

    async def _server_close(self, _code: int = 1000) -> None:
        if not self.closed:
            self.closed = True
            await self.s2c.put(None)
            await self.c2s.put(None)

    async def shutdown(self) -> None:
        await self._server_close(1000)
        if self.server_task is not None and not self.server_task.done():
            self.server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.server_task


class _FakeTelegramApi:
    """In-memory stand-in for TelegramBotApi recording outbound calls and serving media files."""

    def __init__(self, files: dict[str, tuple[bytes, str]] | None = None) -> None:
        self.files: dict[str, tuple[bytes, str]] = dict(files or {})
        self.sent_messages: list[dict[str, Any]] = []
        self.sent_voices: list[dict[str, Any]] = []
        self.sent_audios: list[dict[str, Any]] = []
        self.sent_photos: list[dict[str, Any]] = []
        self.sent_animations: list[dict[str, Any]] = []
        self.sent_reactions: list[dict[str, Any]] = []
        self.sent_chat_actions: list[dict[str, Any]] = []
        self.reaction_result: bool = True
        self._msg_seq = 100

    async def get_me(self) -> dict[str, Any]:
        return {"id": int(BOT_ID), "is_bot": True, "username": "hakimi_test_bot"}

    async def get_updates(
        self,
        *,
        offset: int | None = None,
        timeout: int = 25,  # noqa: ASYNC109
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        return []

    async def get_file_bytes(self, file_id: str, *, max_bytes: int) -> tuple[bytes, str]:
        if file_id not in self.files:
            raise TelegramApiError("tg_file_download_failed", "not found")
        raw, fname = self.files[file_id]
        if len(raw) > max_bytes:
            raise TelegramApiError("media_too_large", "too large")
        return raw, fname

    async def send_message(
        self, chat_id: str, text: str, *, reply_to_message_id: int | None = None
    ) -> str:
        self._msg_seq += 1
        self.sent_messages.append(
            {"chat_id": chat_id, "text": text, "reply_to": reply_to_message_id}
        )
        return str(self._msg_seq)

    async def send_voice(
        self,
        chat_id: str,
        audio_bytes: bytes,
        *,
        filename: str = "voice.ogg",
        mime_type: str = "audio/ogg",
        duration: int | None = None,
        reply_to_message_id: int | None = None,
    ) -> str:
        self._msg_seq += 1
        self.sent_voices.append(
            {
                "chat_id": chat_id,
                "bytes": audio_bytes,
                "filename": filename,
                "mime_type": mime_type,
                "duration": duration,
                "reply_to": reply_to_message_id,
            }
        )
        return str(self._msg_seq)

    async def send_audio(
        self,
        chat_id: str,
        audio_bytes: bytes,
        *,
        filename: str = "song.ogg",
        mime_type: str = "audio/ogg",
        title: str | None = None,
        performer: str = "哈基米",
        duration: int | None = None,
        reply_to_message_id: int | None = None,
    ) -> str:
        self._msg_seq += 1
        self.sent_audios.append(
            {
                "chat_id": chat_id,
                "bytes": audio_bytes,
                "filename": filename,
                "mime_type": mime_type,
                "title": title,
                "performer": performer,
                "duration": duration,
                "reply_to": reply_to_message_id,
            }
        )
        return str(self._msg_seq)

    async def send_photo(
        self,
        chat_id: str,
        photo_bytes: bytes,
        *,
        filename: str = "image.png",
        mime_type: str = "image/png",
        reply_to_message_id: int | None = None,
    ) -> str:
        self._msg_seq += 1
        self.sent_photos.append(
            {
                "chat_id": chat_id,
                "bytes": photo_bytes,
                "filename": filename,
                "mime_type": mime_type,
                "reply_to": reply_to_message_id,
            }
        )
        return str(self._msg_seq)

    async def send_animation(
        self,
        chat_id: str,
        animation_bytes: bytes,
        *,
        filename: str = "meme.gif",
        mime_type: str = "image/gif",
        reply_to_message_id: int | None = None,
    ) -> str:
        self._msg_seq += 1
        self.sent_animations.append(
            {
                "chat_id": chat_id,
                "bytes": animation_bytes,
                "filename": filename,
                "mime_type": mime_type,
                "reply_to": reply_to_message_id,
            }
        )
        return str(self._msg_seq)

    async def set_message_reaction(self, chat_id: str, message_id: int, emoji: str) -> bool:
        self.sent_reactions.append({"chat_id": chat_id, "message_id": message_id, "emoji": emoji})
        return self.reaction_result

    async def send_chat_action(self, chat_id: str, action: str = "typing") -> None:
        self.sent_chat_actions.append({"chat_id": chat_id, "action": action})

    async def close(self) -> None:
        return None


async def _wait_until(predicate: Any, *, max_wait: float = 4.0) -> None:
    async with asyncio.timeout(max_wait):
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0.02)


async def _start_connected_plugin(
    svc: Any,
    bus: PluginBus,
    fake_api: _FakeTelegramApi,
    *,
    allowed_groups: set[str] | None = None,
    proactive_groups: set[str] | None = None,
) -> tuple[TelegramPlugin, _LoopbackBridge, asyncio.Task[None]]:
    token, _ = await bus.create_plugin(
        "telegram",
        "Telegram 社交",
        ["chat", "media", "sensors"],
        node_region="KR",
    )
    bridge = _LoopbackBridge(bus, "telegram")
    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        allowed_users={FRIEND_UID},
        allowed_groups=allowed_groups if allowed_groups is not None else {GROUP_ID},
        proactive_groups=proactive_groups,
        api=fake_api,
        connect_factory=bridge.connect_factory,
        enable_polling=False,
    )
    runner = asyncio.create_task(plugin.client.run_forever())
    await _wait_until(
        lambda: (
            bus.is_plugin_online("telegram")
            and plugin.bot_id == BOT_ID
            and svc.modules.get("telegram") is not None
            and svc.modules.get("telegram").status_dict().get("bot_id") == BOT_ID
        )
    )
    return plugin, bridge, runner


async def _stop_connected_plugin(
    plugin: TelegramPlugin, bridge: _LoopbackBridge, runner: asyncio.Task[None]
) -> None:
    await plugin.close()
    await bridge.shutdown()
    runner.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await runner


# ---------------------------------------------------------------------------
# 1. TelegramBotApi transport & token redaction tests
# ---------------------------------------------------------------------------


async def test_tg_api_transport_split_reply_fallback_and_token_redaction() -> None:
    calls: list[tuple[str, str, bytes]] = []
    send_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal send_attempts
        path = request.url.path
        calls.append((request.method, path, request.content))
        if path.endswith("/getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"id": 42, "username": "bot"}})
        if path.endswith("/sendMessage"):
            send_attempts += 1
            body = json.loads(request.content.decode("utf-8"))
            if send_attempts == 1 and "reply_parameters" in body:
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "error_code": 400,
                        "description": (
                            f"Bad Request: message to be replied not found ({BOT_TOKEN})"
                        ),
                    },
                )
            return httpx.Response(
                200, json={"ok": True, "result": {"message_id": 700 + send_attempts}}
            )
        if path.endswith("/sendVoice"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 801}})
        if path.endswith("/sendAudio"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 802}})
        if path.endswith("/getFile"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "result": {"file_id": "f1", "file_path": "photos/pic.png", "file_size": 5},
                },
            )
        if path.endswith("/photos/pic.png"):
            return httpx.Response(200, content=b"12345")
        if path.endswith("/getUpdates"):
            return httpx.Response(
                429,
                json={
                    "ok": False,
                    "error_code": 429,
                    "description": f"Too Many Requests: {BOT_TOKEN}",
                    "parameters": {"retry_after": 7},
                },
            )
        return httpx.Response(
            404, json={"ok": False, "error_code": 404, "description": "not found"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        api = TelegramBotApi(BOT_TOKEN, http=http)
        assert BOT_TOKEN not in repr(api)

        me = await api.get_me()
        assert me["id"] == 42

        # Long message split (>4000 chars) + reply fallback retry on first chunk
        long_text = ("测试消息一行\n" * 600).strip()
        assert len(split_telegram_text(long_text)) >= 2
        mid = await api.send_message(OWNER_UID, long_text, reply_to_message_id=12)
        assert int(mid) > 700

        vid = await api.send_voice(OWNER_UID, b"OggS1234", duration=3)
        assert vid == "801"
        aid = await api.send_audio(OWNER_UID, b"OggS5678", title="夜曲", duration=180)
        assert aid == "802"

        raw, fname = await api.get_file_bytes("f1", max_bytes=16)
        assert raw == b"12345" and fname == "pic.png"
        with pytest.raises(TelegramApiError) as too_big:
            await api.get_file_bytes("f1", max_bytes=4)
        assert too_big.value.code == "media_too_large"

        # 429 rate limit parses retry_after and scrubs token
        with pytest.raises(TelegramApiError) as rl:
            await api.get_updates(offset=1, timeout=5)
        assert rl.value.code == "tg_rate_limited"
        assert rl.value.retry_after == 7.0
        assert BOT_TOKEN not in rl.value.description


# ---------------------------------------------------------------------------
# 2. TelegramPlugin parsing & fail-closed allowlist tests
# ---------------------------------------------------------------------------


async def test_tg_plugin_parsing_allowlists_mentions_commands_and_media_routing() -> None:
    fake_api = _FakeTelegramApi()
    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        allowed_users={FRIEND_UID},
        allowed_groups={GROUP_ID},
        private_mode="owner_only",
        api=fake_api,
        enable_polling=False,
    )
    plugin.bot_id = BOT_ID
    plugin.bot_username = "HakimiBot"

    # Private owner allowed; friend rejected in owner_only mode; stranger rejected
    assert (
        plugin.parse_update(
            {
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "date": 1700000000,
                    "chat": {"id": int(OWNER_UID), "type": "private"},
                    "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                    "text": "早上好",
                },
            }
        )
        is not None
    )
    assert (
        plugin.parse_update(
            {
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": {"id": int(FRIEND_UID), "type": "private"},
                    "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "Friend"},
                    "text": "hi",
                },
            }
        )
        is None
    )
    assert (
        plugin.parse_update(
            {
                "update_id": 3,
                "message": {
                    "message_id": 12,
                    "chat": {"id": int(OTHER_GROUP_ID), "type": "supergroup", "title": "Unknown"},
                    "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                    "text": "@HakimiBot hi",
                },
            }
        )
        is None
    )

    # Group emoji + UTF-16 @mention offset -> addressed=True
    mention_text = "😺 @HakimiBot 来看看"
    parsed_mention = plugin.parse_update(
        {
            "update_id": 4,
            "message": {
                "message_id": 13,
                "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                "text": mention_text,
                "entities": [{"type": "mention", "offset": 3, "length": 10}],
            },
        }
    )
    assert parsed_mention is not None
    assert parsed_mention["addressed"] is True
    assert parsed_mention["attention_reason"] == "mention"
    assert parsed_mention["group_name"] == "猫窝群"

    # Command normalization `/学歌@HakimiBot 测试曲` + audio attachment routes to `files`
    parsed_learn = plugin.parse_update(
        {
            "update_id": 5,
            "message": {
                "message_id": 14,
                "chat": {"id": int(OWNER_UID), "type": "private"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "caption": "/学歌@HakimiBot 夜曲",
                "audio": {
                    "file_id": "AU_LEARN_1",
                    "file_name": "yequ.mp3",
                    "mime_type": "audio/mpeg",
                    "file_size": 1024,
                    "duration": 210,
                },
            },
        }
    )
    assert parsed_learn is not None
    assert parsed_learn["command_text"] == "/学歌 夜曲"
    assert len(parsed_learn["files"]) == 1
    assert parsed_learn["files"][0]["file_id"] == "AU_LEARN_1"
    assert parsed_learn["audios"] == []

    # Ordinary voice note + photo -> `audios` + `images`
    parsed_media = plugin.parse_update(
        {
            "update_id": 6,
            "message": {
                "message_id": 15,
                "chat": {"id": int(OWNER_UID), "type": "private"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "photo": [
                    {"file_id": "PH_SMALL", "file_size": 1000},
                    {"file_id": "PH_LARGE", "file_size": 8000},
                ],
                "voice": {"file_id": "VO_1", "duration": 6, "file_size": 12000},
            },
        }
    )
    assert parsed_media is not None
    assert parsed_media["images"][0]["file"] == "PH_LARGE"
    assert parsed_media["audios"][0]["file"] == "VO_1"
    assert parsed_media["audios"][0]["duration"] == 6.0

    # Group command targeting a different bot is NOT addressed to us
    parsed_other_bot = plugin.parse_update(
        {
            "update_id": 7,
            "message": {
                "message_id": 16,
                "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                "text": "/help@OtherBot",
                "entities": [{"type": "bot_command", "offset": 0, "length": 14}],
            },
        }
    )
    assert parsed_other_bot is not None
    assert parsed_other_bot["addressed"] is False
    assert parsed_other_bot["command_text"] is None

    # Outbound fail-closed checks reject stranger and unknown group
    with pytest.raises(Exception) as exc1:
        await plugin._handle_chat_delivery(
            {
                "platform": "telegram",
                "kind": "private",
                "conversation_id": STRANGER_UID,
                "user_id": STRANGER_UID,
                "text": "leak",
            }
        )
    assert "delivery_not_authorized" in str(exc1.value)

    with pytest.raises(Exception) as exc2:
        await plugin._handle_voice_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": OTHER_GROUP_ID,
                "user_id": "0",
                "audio_b64": "T2dnUw==",
            }
        )
    assert "delivery_not_authorized" in str(exc2.value)


# ---------------------------------------------------------------------------
# 3. E2E PluginBus <-> TelegramPlugin: chat, pull-on-demand media, TTS & Song
# ---------------------------------------------------------------------------


async def test_telegram_e2e_chat_media_voice_and_chunked_song(settings: Any) -> None:
    png_bytes = _make_png_bytes()
    voice_bytes = b"OggS" + b"\x11" * 2048
    txt_bytes = "这是一份通过 Telegram 发来的测试文档内容".encode()
    large_song_bytes = b"OggS" + (b"\x5a\xa5" * 160_000)  # ~320 KiB -> forces chunked blob.put

    fake_api = _FakeTelegramApi(
        files={
            "TG_PH_1": (png_bytes, "photo.png"),
            "TG_VO_1": (voice_bytes, "voice.ogg"),
            "TG_TXT_1": (txt_bytes, "notes.txt"),
        }
    )
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {f"telegram:{GROUP_ID}": "at_only"},
        }
    )
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = svc.plugin_bus
        plugin, bridge, runner = await _start_connected_plugin(svc, bus, fake_api)
        try:
            # PlatformPolicyHooks registered live on connect
            assert get_platform_hooks("telegram", allow_lazy_default=False) is not None
            proxy = svc.modules.get("telegram")
            assert proxy is not None and proxy.supports_voice is True

            # 3a. Inbound private Owner text message -> full Service pipeline -> TG sendMessage
            now_ts = int(time.time())
            ok = await plugin.process_update(
                {
                    "update_id": 100,
                    "message": {
                        "message_id": 501,
                        "date": now_ts,
                        "chat": {"id": int(OWNER_UID), "type": "private"},
                        "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                        "text": "哈基米在吗",
                    },
                }
            )
            assert ok is True
            job = None
            async with asyncio.timeout(3.0):
                while job is None:
                    job = await svc.store.claim()
                    if job is None:
                        await asyncio.sleep(0.02)
            assert job["payload"]["platform"] == "telegram"
            incoming_msg = Message.model_validate(job["payload"])
            assert is_persona_owner(incoming_msg, s) is True

            await svc.process(job)
            assert await svc.deliver_once()
            assert len(fake_api.sent_messages) == 1
            assert fake_api.sent_messages[0]["chat_id"] == OWNER_UID

            # 3b. Inbound photo + voice + txt file pulled via `media.fetch` BlobResult
            media_update = {
                "update_id": 101,
                "message": {
                    "message_id": 502,
                    "date": now_ts + 1,
                    "chat": {"id": int(OWNER_UID), "type": "private"},
                    "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                    "caption": "看看这些",
                    "photo": [{"file_id": "TG_PH_1", "file_size": len(png_bytes)}],
                    "voice": {"file_id": "TG_VO_1", "duration": 4, "file_size": len(voice_bytes)},
                    "document": {
                        "file_id": "TG_TXT_1",
                        "file_name": "notes.txt",
                        "mime_type": "text/plain",
                        "file_size": len(txt_bytes),
                    },
                },
            }
            assert await plugin.process_update(media_update) is True
            media_job = None
            async with asyncio.timeout(3.0):
                while media_job is None:
                    media_job = await svc.store.claim()
                    if media_job is None:
                        await asyncio.sleep(0.02)
            media_msg = Message.model_validate(media_job["payload"])
            assert media_msg.images[0].file == "plugin_media:telegram:TG_PH_1"
            assert media_msg.images[0].url == ""
            assert media_msg.audios[0].file == "plugin_media:telegram:TG_VO_1"
            assert media_msg.files[0].file_id == "plugin_media:telegram:TG_TXT_1"

            # Verify ImageLoader, AudioLoader._fetch_raw, and FileReader all pull from plugin_bus
            loaded_imgs = await svc.images.load_results(media_msg.images)
            assert len(loaded_imgs) == 1 and loaded_imgs[0] is not None
            fetched_audio = await svc.audios._fetch_raw(None, media_msg.audios[0])  # type: ignore[arg-type]
            assert fetched_audio == voice_bytes
            loaded_file = await svc.files.read_one(
                media_msg.files[0], media_msg, svc.store.subject(media_msg)
            )
            assert "测试文档内容" in loaded_file.content

            # Untrusted arbitrary URLs on non-OneBot messages are still stripped by accept_from
            forged = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="forged-1",
                user_id=OWNER_UID,
                conversation_id=GROUP_ID,
                kind="group",
                text="evil",
                addressed=True,
                created_at=float(now_ts + 2),
                images=[ImageRef(url="https://evil.example/ssrf.png", file="raw-id")],
                files=[FileRef(name="a.txt", url="https://evil.example/a.txt", file_id="raw-fid")],
                audios=[AudioRef(url="https://evil.example/a.ogg", file="raw-aid")],
            )
            assert await svc.accept_from(proxy, forged) == "accepted"
            forged_job = await svc.store.claim()
            assert forged_job is not None
            forged_stored = Message.model_validate(forged_job["payload"])
            assert forged_stored.images == []
            assert forged_stored.files == []
            assert forged_stored.audios == []

            # 3c. Outbound short TTS voice bubble (inline audio_b64 -> sendVoice)
            tts_receipt = await proxy.send_voice(
                incoming_msg,
                b"OggS-short-tts-bubble",
                fmt="ogg_opus",
                mime_type="audio/ogg",
                duration_ms=2400,
            )
            assert tts_receipt.isdigit()
            assert len(fake_api.sent_voices) == 1
            assert fake_api.sent_voices[0]["bytes"] == b"OggS-short-tts-bubble"
            assert fake_api.sent_voices[0]["duration"] == 2

            # 3d. Outbound full-length cover song (>256 KiB -> chunked blob.put -> sendAudio)
            song_receipt = await proxy.send_voice(
                incoming_msg,
                large_song_bytes,
                fmt="ogg_opus",
                mime_type="audio/ogg",
                duration_ms=185_000,
                title="夜曲",
            )
            assert song_receipt.isdigit()
            assert len(fake_api.sent_audios) == 1
            assert fake_api.sent_audios[0]["bytes"] == large_song_bytes
            assert fake_api.sent_audios[0]["title"] == "夜曲"
            assert fake_api.sent_audios[0]["performer"] == "哈基米"
            assert fake_api.sent_audios[0]["duration"] == 185
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)

        # PlatformPolicyHooks cleanly unregistered after disconnect
        await _wait_until(lambda: get_platform_hooks("telegram", allow_lazy_default=False) is None)


# ---------------------------------------------------------------------------
# 4. E2E Owner delivery (daily digest) + LifeEngine proactive telegram.speak
# ---------------------------------------------------------------------------


async def test_telegram_owner_digest_and_proactive_speak(settings: Any) -> None:
    fake_api = _FakeTelegramApi()
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_delivery_platform": "telegram",
            "proactive_dm_enabled": True,
            "daily_digest_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {f"telegram:{GROUP_ID}": "at_only"},
        }
    )
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = svc.plugin_bus
        now = datetime(2026, 11, 26, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
        svc.proactive.clock = lambda: now
        svc.life_engine = LifeEngine(svc, clock=lambda: now)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        plugin, bridge, runner = await _start_connected_plugin(
            svc, bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            # 4a. Daily owner digest routes to Telegram via boundowner scope
            assert svc.proactive.configured is True
            assert svc.proactive.connected() is True
            res = await svc.proactive.manual_digest()
            assert res["receipt"]["stage"] == "confirmed"
            assert any(m["chat_id"] == OWNER_UID for m in fake_api.sent_messages)

            # 4b. Unified `social.speak` action available & Telegram planning_context populated
            assert "social.speak" in svc.modules.catalog.actions
            assert "telegram.speak" not in svc.modules.catalog.actions
            proxy = svc.modules.get("telegram")
            assert proxy is not None
            ctx = await proxy.planning_context(svc, svc.life_engine.config, now)
            assert ctx["available_groups"] == [f"telegram:{GROUP_ID}"]
            assert ctx["owner_dm_available"] is True

            # 4c. LifeEngine resolves social.speak (and alias telegram.speak) for boundowner/group
            spec_owner, _ = svc.life_engine.resolve(
                {
                    "action_name": "telegram.speak",
                    "target_scope": svc.proactive.target_scope,
                    "origin": "planner",
                }
            )
            assert spec_owner.name == "social.speak"
            spec_group, _ = svc.life_engine.resolve(
                {
                    "action_name": "social.speak",
                    "target_scope": f"telegram:{GROUP_ID}",
                    "origin": "planner",
                }
            )
            assert spec_group.name == "social.speak"

            # Execute proactive Owner DM via `social.speak`
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,updated_at,execute_after,expire_at,target_module,"
                    "action_type,action_name,target_scope,status,mood,"
                    "intent_note_json,gate_condition_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "tg-dm-1",
                        now,
                        now,
                        now,
                        now + 3600,
                        "social_presence",
                        "speak",
                        "social.speak",
                        svc.proactive.target_scope,
                        "executing",
                        "happy",
                        "{}",
                        "{}",
                    ),
                )
            dm_outcome = await spec_owner.run(
                ActionCall(
                    ticket={
                        "id": "tg-dm-1",
                        "target_module": "social_presence",
                        "action_type": "speak",
                        "action_name": "social.speak",
                        "target_scope": svc.proactive.target_scope,
                        "mood": "happy",
                        "intent_note": {"note": "打个招呼"},
                        "gate_condition": {},
                    },
                    args=spec_owner.arguments(),
                    now=now,
                    still_allowed=lambda: True,
                )
            )
            assert dm_outcome.status == "done"

            # Execute proactive group speak via `social.speak`
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,updated_at,execute_after,expire_at,target_module,"
                    "action_type,action_name,target_scope,status,mood,"
                    "intent_note_json,gate_condition_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "tg-grp-1",
                        now,
                        now,
                        now,
                        now + 3600,
                        "social_presence",
                        "speak",
                        "social.speak",
                        f"telegram:{GROUP_ID}",
                        "executing",
                        "curious",
                        "{}",
                        "{}",
                    ),
                )
            grp_outcome = await spec_group.run(
                ActionCall(
                    ticket={
                        "id": "tg-grp-1",
                        "target_module": "social_presence",
                        "action_type": "speak",
                        "action_name": "social.speak",
                        "target_scope": f"telegram:{GROUP_ID}",
                        "mood": "curious",
                        "intent_note": {"note": "群里冒泡"},
                        "gate_condition": {},
                    },
                    args=spec_group.arguments(),
                    now=now,
                    still_allowed=lambda: True,
                )
            )
            assert grp_outcome.status == "done"
            assert any(m["chat_id"] == GROUP_ID for m in fake_api.sent_messages)

            # Muted group policy blocks proactive group recipient
            s.group_policies[f"telegram:{GROUP_ID}"] = "muted"
            assert proxy.normalize_recipient(f"telegram:{GROUP_ID}") is None
            with pytest.raises(ValueError):
                svc.life_engine.resolve(
                    {
                        "action_name": "social.speak",
                        "target_scope": f"telegram:{GROUP_ID}",
                        "origin": "planner",
                    }
                )
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 5. P0: Group long replies, reply_to anchor routing, quote expansion & b23.tv
# ---------------------------------------------------------------------------


async def test_tg_plugin_quote_expansion_security_and_bili_links() -> None:
    fake_api = _FakeTelegramApi()
    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        allowed_users={FRIEND_UID},
        allowed_groups={GROUP_ID},
        api=fake_api,
        enable_polling=False,
    )
    plugin.bot_id = BOT_ID
    plugin.bot_username = "HakimiBot"

    # 5a. Group reply to friend's photo + caption with `/存表情` -> addressed, quote + media
    parsed_save = plugin.parse_update(
        {
            "update_id": 201,
            "message": {
                "message_id": 901,
                "date": 1700000100,
                "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "text": "/存表情 震撼猫猫",
                "reply_to_message": {
                    "message_id": 890,
                    "chat": {"id": int(GROUP_ID), "type": "supergroup"},
                    "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "小明"},
                    "caption": "快看这张图",
                    "photo": [{"file_id": "PH_QUOTED_1", "file_size": 4096}],
                },
            },
        }
    )
    assert parsed_save is not None
    assert parsed_save["addressed"] is True
    assert parsed_save["attention_reason"] == "command"
    assert parsed_save["command_text"] == "/存表情 震撼猫猫"
    assert parsed_save["reply_ids"] == ["890"]
    assert '[引用 小明(telegram:555000222) 的消息: "快看这张图 [图片]"]' in parsed_save["text"]
    assert len(parsed_save["images"]) == 1
    assert parsed_save["images"][0]["file"] == "PH_QUOTED_1"
    assert parsed_save["images"][0]["origin"] == "引用消息"

    # 5b. Quoted slash command CANNOT spoof command_text; text_link Bilibili URL is surfaced
    parsed_spoof = plugin.parse_update(
        {
            "update_id": 202,
            "message": {
                "message_id": 902,
                "date": 1700000101,
                "chat": {"id": int(OWNER_UID), "type": "private"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "text": "帮我看看这个",
                "reply_to_message": {
                    "message_id": 891,
                    "chat": {"id": int(OWNER_UID), "type": "private"},
                    "from": {"id": int(BOT_ID), "is_bot": True, "first_name": "哈基米"},
                    "text": "/watch-video 点这里看视频",
                    "entities": [
                        {
                            "type": "text_link",
                            "offset": 13,
                            "length": 6,
                            "url": "https://b23.tv/TgShort9?share_source=copy",
                        }
                    ],
                },
            },
        }
    )
    assert parsed_spoof is not None
    assert parsed_spoof["command_text"] == "帮我看看这个"
    assert "我自己(吉米奈喵)" in parsed_spoof["text"]
    assert "https://b23.tv/TgShort9" in parsed_spoof["text"]

    # 5c. Owner replying `/学歌 夜曲` to an audio message routes quoted audio into `files`
    parsed_learn_quote = plugin.parse_update(
        {
            "update_id": 203,
            "message": {
                "message_id": 903,
                "date": 1700000102,
                "chat": {"id": int(OWNER_UID), "type": "private"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "text": "/学歌 夜曲",
                "reply_to_message": {
                    "message_id": 892,
                    "chat": {"id": int(OWNER_UID), "type": "private"},
                    "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                    "audio": {
                        "file_id": "AU_QUOTED_SONG",
                        "file_name": "yequ.mp3",
                        "mime_type": "audio/mpeg",
                        "file_size": 2048,
                        "duration": 200,
                    },
                },
            },
        }
    )
    assert parsed_learn_quote is not None
    assert parsed_learn_quote["command_text"] == "/学歌 夜曲"
    assert len(parsed_learn_quote["files"]) == 1
    assert parsed_learn_quote["files"][0]["file_id"] == "AU_QUOTED_SONG"
    assert parsed_learn_quote["files"][0]["origin"] == "引用消息"
    assert parsed_learn_quote["audios"] == []

    # 5d. Cross-chat reply_to_message is NOT expanded (fail-closed scope guard)
    parsed_cross = plugin.parse_update(
        {
            "update_id": 204,
            "message": {
                "message_id": 904,
                "date": 1700000103,
                "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "text": "@HakimiBot 看看",
                "entities": [{"type": "mention", "offset": 0, "length": 10}],
                "reply_to_message": {
                    "message_id": 777,
                    "chat": {"id": int(OTHER_GROUP_ID), "type": "supergroup"},
                    "from": {"id": int(STRANGER_UID), "is_bot": False, "first_name": "外人"},
                    "text": "跨群机密",
                    "photo": [{"file_id": "PH_CROSS_CHAT", "file_size": 1000}],
                },
            },
        }
    )
    assert parsed_cross is not None
    assert parsed_cross["reply_ids"] == []
    assert "跨群机密" not in parsed_cross["text"]
    assert parsed_cross["images"] == []


async def test_telegram_p0_group_long_reply_anchor_quote_meme_and_b23_e2e(settings: Any) -> None:
    png_bytes = _make_png_bytes((220, 80, 120))
    voice_bytes = b"OggS" + b"\x22" * 512
    fake_api = _FakeTelegramApi(
        files={
            "TG_QUOTE_PH": (png_bytes, "meme.png"),
            "TG_DIRECT_VO": (voice_bytes, "voice.ogg"),
            "TG_QUOTE_VO": (voice_bytes, "quoted.ogg"),
        }
    )
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "bilibili_enabled": True,
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {f"telegram:{GROUP_ID}": "at_only"},
        }
    )

    long_bubble_1 = "这是一段超过一百八十字的群聊长回复第一段内容，" * 7
    long_bubble_2 = "这是第二段补充说明，不该重复引用原消息。"
    assert len(long_bubble_1) + len(long_bubble_2) > 180

    class _P0Gateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.next_json_reply: dict[str, Any] | None = None

        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", payload, kwargs))
            if self.next_json_reply is not None:
                raw = json.dumps(self.next_json_reply, ensure_ascii=False)
                self.next_json_reply = None
                return raw
            return "视频真不错喵~"

    gw = _P0Gateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = svc.plugin_bus
        plugin, bridge, runner = await _start_connected_plugin(svc, bus, fake_api)

        async def _claim_next() -> dict[str, Any]:
            async with asyncio.timeout(3.0):
                while True:
                    claimed = await svc.store.claim()
                    if claimed is not None:
                        return claimed
                    await asyncio.sleep(0.02)

        try:
            now_ts = int(time.time())

            # 1. Private chat replying to bot msg #400 -> reply_to MUST be None (not 400!)
            gw.next_json_reply = {
                "abstain": False,
                "bubbles": ["收到啦，私聊不用一直引用旧消息~"],
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
                "audio_gist": None,
                "search_query": None,
            }
            assert await plugin.process_update(
                {
                    "update_id": 301,
                    "message": {
                        "message_id": 601,
                        "date": now_ts,
                        "chat": {"id": int(OWNER_UID), "type": "private"},
                        "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                        "text": "展开讲讲",
                        "reply_to_message": {
                            "message_id": 400,
                            "chat": {"id": int(OWNER_UID), "type": "private"},
                            "from": {"id": int(BOT_ID), "is_bot": True, "first_name": "哈基米"},
                            "text": "之前说的那件事",
                        },
                    },
                }
            )
            job1 = await _claim_next()
            await svc.process(job1)
            assert await svc.deliver_once()
            assert len(fake_api.sent_messages) == 1
            assert fake_api.sent_messages[0]["reply_to"] is None

            # 2. Group long reply (>180 chars, triggers `folded`) + intervening drift ->
            #    must not fail with forward_group_only; bubble 1 replies to 602, bubble 2 is None
            gw.next_json_reply = {
                "abstain": False,
                "bubbles": [long_bubble_1, long_bubble_2],
                "anchor": {"reply_id": "602", "mentions": []},
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
                "audio_gist": None,
                "search_query": None,
            }
            assert await plugin.process_update(
                {
                    "update_id": 302,
                    "message": {
                        "message_id": 602,
                        "date": now_ts + 1,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "@hakimi_test_bot 详细讲一下原理",
                        "entities": [{"type": "mention", "offset": 0, "length": 16}],
                        "reply_to_message": {
                            "message_id": 401,
                            "chat": {"id": int(GROUP_ID), "type": "supergroup"},
                            "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                            "text": "前置问题",
                        },
                    },
                }
            )
            job2 = await _claim_next()
            await svc.process(job2)
            # Simulate intervening speaker so drift == 1 and anchor.reply_id ("602") is preserved
            svc.hot.observe(
                Message(
                    platform="telegram",
                    bot_id=BOT_ID,
                    event_id="603",
                    user_id=OWNER_UID,
                    conversation_id=GROUP_ID,
                    kind="group",
                    text="插一句",
                    addressed=False,
                    created_at=float(now_ts + 2),
                )
            )
            assert await svc.deliver_once()
            grp_sent = [m for m in fake_api.sent_messages if m["chat_id"] == GROUP_ID]
            assert len(grp_sent) == 2
            assert "这是一段超过一百八十字的群聊长回复第一段内容" in grp_sent[0]["text"]
            assert len(grp_sent[0]["text"]) > 140
            assert grp_sent[0]["reply_to"] == 602
            assert "第二段补充说明" in grp_sent[1]["text"]
            assert grp_sent[1]["reply_to"] is None

            # 3. Group quote-to-save-meme (`/存表情`) pulls quoted photo via bus and saves to memes
            assert await plugin.process_update(
                {
                    "update_id": 303,
                    "message": {
                        "message_id": 604,
                        "date": now_ts + 3,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                        "text": "/存表情 贴贴猫",
                        "reply_to_message": {
                            "message_id": 599,
                            "chat": {"id": int(GROUP_ID), "type": "supergroup"},
                            "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                            "photo": [{"file_id": "TG_QUOTE_PH", "file_size": len(png_bytes)}],
                        },
                    },
                }
            )
            job3 = await _claim_next()
            await svc.process(job3)
            assert await svc.deliver_once()
            assert "贴贴猫" in svc.memes.labels
            assert fake_api.sent_messages[-1]["reply_to"] == 604
            assert "表情已存好啦" in fake_api.sent_messages[-1]["text"]

            # 4. Quoted b23.tv short link populates short_urls and calls svc.bili.resolve_short
            resolved_shorts: list[str] = []

            async def _fake_resolve_short(url: str) -> str:
                resolved_shorts.append(url)
                return "BV1xx411c7mD"

            async def _fake_video(bvid: str) -> Any:
                from hakimi.models import Video

                return Video(
                    bvid=bvid,
                    mid="12345",
                    title="测试视频标题",
                    description="测试简介",
                    author="测试UP",
                    published_at=now_ts,
                )

            async def _fake_material(video: Any, guard=None) -> dict[str, Any]:
                return {
                    "bvid": video.bvid,
                    "title": video.title,
                    "author": video.author,
                    "material_scope": "metadata_only",
                }

            svc.bili.resolve_short = _fake_resolve_short  # type: ignore[method-assign]
            svc.bili.video = _fake_video  # type: ignore[method-assign]
            svc.bili_watcher.public_material = _fake_material  # type: ignore[method-assign]

            assert await plugin.process_update(
                {
                    "update_id": 304,
                    "message": {
                        "message_id": 605,
                        "date": now_ts + 4,
                        "chat": {"id": int(OWNER_UID), "type": "private"},
                        "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                        "text": "帮我看看这个视频讲啥",
                        "reply_to_message": {
                            "message_id": 600,
                            "chat": {"id": int(OWNER_UID), "type": "private"},
                            "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                            "text": "【分享】 https://b23.tv/Demo888?share_source=copy",
                        },
                    },
                }
            )
            job4 = await _claim_next()
            msg4 = Message.model_validate(job4["payload"])
            assert msg4.short_urls == ["https://b23.tv/Demo888"]
            await svc.process(job4)
            assert resolved_shorts == ["https://b23.tv/Demo888"]
            assert await svc.deliver_once()
            assert fake_api.sent_messages[-1]["chat_id"] == OWNER_UID
            assert fake_api.sent_messages[-1]["reply_to"] is None

            # 5. Direct TG audio with slash caption NEVER triggers slash command,
            #    while replying with slash command to a quoted voice DOES work
            direct_audio_msg = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="606",
                user_id=OWNER_UID,
                conversation_id=OWNER_UID,
                kind="private",
                text="/哈基米 帮助",
                command_text="/哈基米 帮助",
                addressed=True,
                created_at=float(now_ts + 5),
                audios=[
                    AudioRef(
                        file="plugin_media:telegram:TG_DIRECT_VO",
                        origin="telegram_voice",
                    )
                ],
            )
            assert await svc.command(direct_audio_msg) is None

            quoted_audio_msg = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="607",
                user_id=OWNER_UID,
                conversation_id=OWNER_UID,
                kind="private",
                text='/哈基米 帮助\n[引用 友友(telegram:555000222) 的消息: "[语音]"]',
                command_text="/哈基米 帮助",
                addressed=True,
                created_at=float(now_ts + 6),
                audios=[AudioRef(file="plugin_media:telegram:TG_QUOTE_VO", origin="引用消息")],
            )
            help_reply = await svc.command(quoted_audio_msg)
            assert help_reply is not None and "哈基米在这里" in help_reply

            # 6. send_voice uses explicit reply_to anchor and never falls back to inbound reply_ids
            proxy = svc.modules.get("telegram")
            assert proxy is not None
            msg_with_inbound_reply = Message(
                platform="telegram",
                bot_id=BOT_ID,
                event_id="608",
                user_id=OWNER_UID,
                conversation_id=GROUP_ID,
                kind="group",
                text="唱一句",
                addressed=True,
                reply_ids=["400"],
                created_at=float(now_ts + 7),
            )
            await proxy.send_voice(
                msg_with_inbound_reply,
                b"OggS-no-anchor",
                fmt="ogg_opus",
                mime_type="audio/ogg",
                duration_ms=2000,
            )
            assert fake_api.sent_voices[-1]["reply_to"] is None
            await proxy.send_voice(
                msg_with_inbound_reply,
                b"OggS-with-anchor",
                fmt="ogg_opus",
                mime_type="audio/ogg",
                duration_ms=2000,
                reply_to="608",
            )
            assert fake_api.sent_voices[-1]["reply_to"] == 608
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 6. P1 Plan A: TelegramBotApi & TelegramPlugin media / reaction / typing units
# ---------------------------------------------------------------------------


async def test_tg_api_media_reaction_and_typing(caplog: pytest.LogCaptureFixture) -> None:
    photo_attempts = 0
    react_bodies: list[dict[str, Any]] = []
    chat_actions: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal photo_attempts
        path = request.url.path
        if path.endswith("/sendPhoto"):
            photo_attempts += 1
            if photo_attempts == 1 and b"reply_parameters" in request.content:
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "error_code": 400,
                        "description": f"Bad Request: message to reply not found ({BOT_TOKEN})",
                    },
                )
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 910}})
        if path.endswith("/sendAnimation"):
            assert b"reply_parameters" in request.content
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 911}})
        if path.endswith("/setMessageReaction"):
            body = json.loads(request.content.decode("utf-8"))
            react_bodies.append(body)
            mid = body["message_id"]
            if mid == 1001:
                return httpx.Response(200, json={"ok": True, "result": True})
            if mid == 1002:
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "error_code": 400,
                        "description": f"Bad Request: REACTION_INVALID ({BOT_TOKEN})",
                    },
                )
            return httpx.Response(
                403,
                json={
                    "ok": False,
                    "error_code": 403,
                    "description": f"Forbidden: bot was kicked ({BOT_TOKEN})",
                },
            )
        if path.endswith("/sendChatAction"):
            chat_actions.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(200, json={"ok": True, "result": True})
        return httpx.Response(404, json={"ok": False, "error_code": 404, "description": "missing"})

    png_bytes = _make_png_bytes()
    gif_bytes = _make_gif_bytes()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        api = TelegramBotApi(BOT_TOKEN, http=http)

        # send_photo retries without reply_parameters on 400 reply error
        pid = await api.send_photo(GROUP_ID, png_bytes, filename="card.png", reply_to_message_id=55)
        assert pid == "910"
        assert photo_attempts == 2

        # send_animation posts multipart animation with reply_parameters
        aid = await api.send_animation(
            GROUP_ID, gif_bytes, filename="cat.gif", reply_to_message_id=56
        )
        assert aid == "911"

        # set_message_reaction: 200 -> True, 400 -> False, 403 -> False + scrubbed warning log
        assert await api.set_message_reaction(GROUP_ID, 1001, "👍") is True
        assert react_bodies[0] == {
            "chat_id": GROUP_ID,
            "message_id": 1001,
            "reaction": [{"type": "emoji", "emoji": "👍"}],
        }
        assert await api.set_message_reaction(GROUP_ID, 1002, "🥰") is False

        with caplog.at_level(logging.WARNING, logger="hakimi_plugin_telegram"):
            assert await api.set_message_reaction(GROUP_ID, 1003, "🔥") is False
        assert BOT_TOKEN not in caplog.text
        assert "tg_set_reaction_failed" in caplog.text

        # send_chat_action
        await api.send_chat_action(GROUP_ID, "typing")
        assert chat_actions == [{"chat_id": GROUP_ID, "action": "typing"}]


async def test_tg_plugin_media_react_typing_handlers() -> None:
    fake_api = _FakeTelegramApi()
    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        allowed_users={FRIEND_UID},
        allowed_groups={GROUP_ID},
        api=fake_api,
        enable_polling=False,
    )
    plugin.bot_id = BOT_ID
    plugin.bot_username = "HakimiBot"

    # Capabilities declare all P1 Plan A transport features
    assert set(plugin.client.config.capabilities) >= {
        "voice",
        "voice_blob",
        "reply_anchor",
        "proactive_speak",
        "sticker",
        "rich_card",
        "emoji_like",
        "typing",
        "quick_ack",
        "media_blob",
    }

    # Pure protocol parsing: group alias text without @bot stays addressed=False in plugin layer
    parsed_alias = plugin.parse_update(
        {
            "update_id": 401,
            "message": {
                "message_id": 1100,
                "date": 1700000200,
                "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                "text": "哈基米在吗",
            },
        }
    )
    assert parsed_alias is not None
    assert parsed_alias["addressed"] is False
    assert parsed_alias["attention_reason"] == ""

    # _handle_media_delivery: PNG (media_b64), JPEG/WEBP (blob) -> send_photo; GIF -> send_animation
    png_bytes = _make_png_bytes()
    gif_bytes = _make_gif_bytes()
    jpg_bytes = b"\xff\xd8\xff\xe0" + b"\x00" * 32
    webp_bytes = b"RIFF\x24\x00\x00\x00WEBP" + b"\x00" * 24

    res_png = await plugin._handle_media_delivery(
        {
            "platform": "telegram",
            "bot_id": BOT_ID,
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "media_kind": "card",
            "mime_type": "image/png",
            "filename": "reply_card.png",
            "reply_to": "1100",
            "media_b64": base64.b64encode(png_bytes).decode("ascii"),
        }
    )
    assert res_png["message_id"].isdigit()
    assert len(fake_api.sent_photos) == 1
    assert fake_api.sent_photos[0]["mime_type"] == "image/png"
    assert fake_api.sent_photos[0]["filename"] == "reply_card.png"
    assert fake_api.sent_photos[0]["reply_to"] == 1100

    await plugin._handle_media_delivery(
        {
            "platform": "telegram",
            "kind": "private",
            "conversation_id": OWNER_UID,
            "user_id": OWNER_UID,
            "blob": jpg_bytes,
            "blob_filename": "../nested/meme.jpg",
        }
    )
    assert fake_api.sent_photos[-1]["mime_type"] == "image/jpeg"
    assert fake_api.sent_photos[-1]["filename"] == "meme.jpg"

    await plugin._handle_media_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "blob": webp_bytes,
        }
    )
    assert fake_api.sent_photos[-1]["mime_type"] == "image/webp"

    res_gif = await plugin._handle_media_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "media_kind": "sticker",
            "reply_to": "1101",
            "blob": gif_bytes,
        }
    )
    assert res_gif["message_id"].isdigit()
    assert len(fake_api.sent_animations) == 1
    assert fake_api.sent_animations[0]["mime_type"] == "image/gif"
    assert fake_api.sent_animations[0]["reply_to"] == 1101
    assert plugin._tx_count == 4

    # Invalid / oversized / unauthorized media payloads are rejected fail-closed
    with pytest.raises(Exception) as bad_magic:
        await plugin._handle_media_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": GROUP_ID,
                "user_id": FRIEND_UID,
                "blob": b"<html>not an image</html>",
            }
        )
    assert "unsupported_media_format" in str(bad_magic.value)

    with pytest.raises(Exception) as too_big:
        await plugin._handle_media_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": GROUP_ID,
                "user_id": FRIEND_UID,
                "blob": b"\x89PNG\r\n\x1a\n" + b"\x00" * (5 * 1024 * 1024),
            }
        )
    assert "media_payload_too_large" in str(too_big.value)

    with pytest.raises(Exception) as unauth_media:
        await plugin._handle_media_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": OTHER_GROUP_ID,
                "user_id": FRIEND_UID,
                "blob": png_bytes,
            }
        )
    assert "delivery_not_authorized" in str(unauth_media.value)

    # _handle_react_delivery: allowlist mapping, \ufe0f stripping, emoji_id fallback, safe failure
    expected_pairs = [
        ("👍", None, "👍"),
        ("❤️", None, "❤"),
        ("🔥", None, "🔥"),
        ("👏", None, "👏"),
        ("🎉", None, "🎉"),
        ("🤔", None, "🤔"),
        ("👀", None, "👀"),
        ("😭", None, "😭"),
        ("💯", None, "💯"),
        ("😂", None, "🤣"),
        ("🐱", None, "🥰"),
        ("✨", None, "🤩"),
        ("💤", None, "😴"),
        ("", "76", "👍"),
        (None, "128049", "🥰"),
        (None, "128514", "🤣"),
    ]
    for idx, (emoji_in, eid_in, expected_out) in enumerate(expected_pairs, start=1):
        out = await plugin._handle_react_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": GROUP_ID,
                "user_id": FRIEND_UID,
                "message_id": str(2000 + idx),
                "emoji": emoji_in,
                "emoji_id": eid_in,
            }
        )
        assert out == {"applied": True}
        assert fake_api.sent_reactions[-1] == {
            "chat_id": GROUP_ID,
            "message_id": 2000 + idx,
            "emoji": expected_out,
        }

    # Unmapped emoji or non-numeric message_id degrades cleanly to applied=False
    prev_reacts = len(fake_api.sent_reactions)
    assert await plugin._handle_react_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "message_id": "3001",
            "emoji": "🚀",
        }
    ) == {"applied": False}
    assert await plugin._handle_react_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "message_id": "not-a-number",
            "emoji": "👍",
        }
    ) == {"applied": False}
    assert len(fake_api.sent_reactions) == prev_reacts

    # Telegram API reaction refusal returns applied=False
    fake_api.reaction_result = False
    assert await plugin._handle_react_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
            "message_id": "3002",
            "emoji": "👍",
        }
    ) == {"applied": False}
    fake_api.reaction_result = True

    with pytest.raises(Exception) as unauth_react:
        await plugin._handle_react_delivery(
            {
                "platform": "telegram",
                "kind": "private",
                "conversation_id": STRANGER_UID,
                "user_id": STRANGER_UID,
                "message_id": "3003",
                "emoji": "👍",
            }
        )
    assert "delivery_not_authorized" in str(unauth_react.value)

    # _handle_typing_delivery: private + group succeed; unauthorized fails closed
    assert await plugin._handle_typing_delivery(
        {
            "platform": "telegram",
            "kind": "group",
            "conversation_id": GROUP_ID,
            "user_id": FRIEND_UID,
        }
    ) == {"ok": True}
    assert fake_api.sent_chat_actions[-1] == {"chat_id": GROUP_ID, "action": "typing"}

    with pytest.raises(Exception) as unauth_typing:
        await plugin._handle_typing_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": OTHER_GROUP_ID,
                "user_id": FRIEND_UID,
            }
        )
    assert "delivery_not_authorized" in str(unauth_typing.value)


# ---------------------------------------------------------------------------
# 7. P1 Plan A E2E: alias wake, stickers/cards/save_meme, reactions, typing/quick_ack, prompts
# ---------------------------------------------------------------------------


def _chat_actions_json(**overrides: Any) -> str:
    payload: dict[str, Any] = {
        "abstain": False,
        "bubbles": ["收到啦~"],
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
        "audio_gist": None,
        "search_query": None,
    }
    payload.update(overrides)
    return json.dumps(payload, ensure_ascii=False)


async def test_telegram_p1_alias_stickers_cards_reactions_typing_and_quick_ack_e2e(
    settings: Any,
) -> None:
    png_bytes = _make_png_bytes((70, 150, 230))
    gif_bytes = _make_gif_bytes()
    auto_save_png = _make_png_bytes((235, 120, 75))
    large_png_bytes = b"\x89PNG\r\n\x1a\n" + (b"\x5a\xa5" * 160_000)  # >256 KiB -> chunked blob

    fake_api = _FakeTelegramApi(files={"TG_AUTO_MEME": (auto_save_png, "cute.png")})
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {f"telegram:{GROUP_ID}": "at_only"},
        }
    )

    class _P1Gateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.next_reply: str | None = None
            self.on_generate_wait: Any = None

        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", payload, kwargs))
            if self.on_generate_wait is not None:
                waiter = self.on_generate_wait
                self.on_generate_wait = None
                await waiter()
            if self.next_reply is not None:
                out = self.next_reply
                self.next_reply = None
                return out
            return _chat_actions_json()

    gw = _P1Gateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = svc.plugin_bus
        plugin, bridge, runner = await _start_connected_plugin(svc, bus, fake_api)

        async def _claim_next() -> dict[str, Any]:
            async with asyncio.timeout(3.0):
                while True:
                    claimed = await svc.store.claim()
                    if claimed is not None:
                        return claimed
                    await asyncio.sleep(0.02)

        try:
            proxy = svc.modules.get("telegram")
            assert proxy is not None
            assert proxy.supports_sticker is True
            assert proxy.supports_rich_card is True
            assert proxy.supports_emoji_like is True
            assert proxy.supports_typing is True
            assert proxy.supports_quick_ack is True
            assert proxy.supports_media_blob is True

            await svc.memes.initialize()
            await svc.memes.upload_bytes("摸摸", png_bytes)
            await svc.memes.upload_bytes("抖动猫", gif_bytes)

            now_ts = int(time.time())

            # (a) Group alias & possible_alias via domain_bridge (without @bot) under at_only
            assert await plugin.process_update(
                {
                    "update_id": 501,
                    "message": {
                        "message_id": 1501,
                        "date": now_ts,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米在吗",
                    },
                }
            )
            alias_job = await _claim_next()
            alias_msg = Message.model_validate(alias_job["payload"])
            assert alias_msg.addressed is True
            assert alias_msg.attention_reason == "alias"
            gw.next_reply = _chat_actions_json(bubbles=["在呢在呢~"])
            await svc.process(alias_job)
            assert await svc.deliver_once()

            assert await plugin.process_update(
                {
                    "update_id": 502,
                    "message": {
                        "message_id": 1502,
                        "date": now_ts + 1,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "猫猫觉得呢",
                    },
                }
            )
            poss_job = await _claim_next()
            poss_msg = Message.model_validate(poss_job["payload"])
            assert poss_msg.addressed is False
            assert poss_msg.attention_reason == "possible_alias"
            assert poss_msg.has_possible_alias is True
            assert poss_msg.invites_group_review is True
            gw.next_reply = _chat_actions_json(bubbles=["我觉得挺好呀"])
            await svc.process(poss_job)
            assert await svc.deliver_once()

            # Other bot slash command containing alias text must NOT wake us via domain_bridge
            assert await plugin.process_update(
                {
                    "update_id": 503,
                    "message": {
                        "message_id": 1503,
                        "date": now_ts + 2,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "/help@OtherBot 哈基米",
                        "entities": [{"type": "bot_command", "offset": 0, "length": 14}],
                    },
                }
            )
            await asyncio.sleep(0.08)
            assert await svc.store.claim() is None

            # (b1) Group PNG/JPEG sticker -> sendPhoto; GIF sticker -> sendAnimation
            gw.next_reply = _chat_actions_json(
                bubbles=["摸摸头~"],
                sticker={"label": "摸摸", "timing": "after_text"},
            )
            assert await plugin.process_update(
                {
                    "update_id": 504,
                    "message": {
                        "message_id": 1504,
                        "date": now_ts + 3,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米发张摸摸表情包",
                    },
                }
            )
            job_png = await _claim_next()
            await svc.process(job_png)
            assert await svc.deliver_once()
            assert len(fake_api.sent_photos) == 1
            assert fake_api.sent_photos[0]["chat_id"] == GROUP_ID
            assert fake_api.sent_photos[0]["mime_type"] in {"image/png", "image/jpeg"}

            gw.next_reply = _chat_actions_json(
                bubbles=[""],
                sticker={"label": "抖动猫", "timing": "after_text"},
            )
            assert await plugin.process_update(
                {
                    "update_id": 505,
                    "message": {
                        "message_id": 1505,
                        "date": now_ts + 4,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米再来张抖动猫表情包",
                    },
                }
            )
            job_gif = await _claim_next()
            await svc.process(job_gif)
            assert await svc.deliver_once()
            assert len(fake_api.sent_animations) == 1
            assert fake_api.sent_animations[0]["chat_id"] == GROUP_ID
            assert fake_api.sent_animations[0]["mime_type"] == "image/gif"

            # (b2) AI auto-save meme (`save_meme_label`) on TG group photo
            gw.next_reply = _chat_actions_json(
                bubbles=["这张收进表情库啦~"],
                save_meme_label="震撼猫",
            )
            assert await plugin.process_update(
                {
                    "update_id": 506,
                    "message": {
                        "message_id": 1506,
                        "date": now_ts + 5,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "caption": "哈基米收下这张猫猫图",
                        "photo": [{"file_id": "TG_AUTO_MEME", "file_size": len(auto_save_png)}],
                    },
                }
            )
            job_save = await _claim_next()
            await svc.process(job_save)
            assert await svc.deliver_once()
            assert "震撼猫" in svc.memes.labels

            # (b3) Rich code-block reply -> render_card -> send_media(kind="card") -> sendPhoto
            photos_before_card = len(fake_api.sent_photos)
            code_bubble = (
                "给你写个最小示例：\n"
                "```python\n"
                "def greet(name: str) -> str:\n"
                "    cleaned = name.strip()\n"
                "    if not cleaned:\n"
                "        return 'hello'\n"
                "    return f'hello, {cleaned}'\n"
                "```"
            )
            gw.next_reply = _chat_actions_json(bubbles=[code_bubble])
            assert await plugin.process_update(
                {
                    "update_id": 507,
                    "message": {
                        "message_id": 1507,
                        "date": now_ts + 6,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米写个python函数示例",
                    },
                }
            )
            job_card = await _claim_next()
            await svc.process(job_card)
            assert await svc.deliver_once()
            assert len(fake_api.sent_photos) == photos_before_card + 1
            assert fake_api.sent_photos[-1]["mime_type"] in {"image/png", "image/jpeg"}
            assert fake_api.sent_photos[-1]["filename"] in {"card.png", "card.jpg"}

            # (b4) Chunked media_blob transfer (>256 KiB image) over PluginBus -> sendPhoto
            chunk_receipt = await proxy.send_media(
                alias_msg,
                large_png_bytes,
                kind="card",
                mime_type="image/png",
                filename="large_card.png",
                reply_to="1501",
            )
            assert chunk_receipt.isdigit()
            assert fake_api.sent_photos[-1]["bytes"] == large_png_bytes
            assert fake_api.sent_photos[-1]["reply_to"] == 1501

            # (c) abstain=True + emoji_like (🐱->🥰, 👍->👍) & lurk reaction (😂->🤣)
            s.group_policies[f"telegram:{GROUP_ID}"] = "smart_reply"

            msgs_before_react = len(fake_api.sent_messages)
            gw.next_reply = _chat_actions_json(abstain=True, bubbles=[""], emoji_like="猫猫")
            assert await plugin.process_update(
                {
                    "update_id": 508,
                    "message": {
                        "message_id": 1508,
                        "date": now_ts + 7,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米看一眼",
                    },
                }
            )
            job_r1 = await _claim_next()
            await svc.process(job_r1)
            assert await svc.deliver_once()
            assert fake_api.sent_reactions[-1] == {
                "chat_id": GROUP_ID,
                "message_id": 1508,
                "emoji": "🥰",
            }
            assert len(fake_api.sent_messages) == msgs_before_react

            gw.next_reply = _chat_actions_json(abstain=True, bubbles=[""], emoji_like="赞")
            assert await plugin.process_update(
                {
                    "update_id": 509,
                    "message": {
                        "message_id": 1509,
                        "date": now_ts + 8,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米真棒",
                    },
                }
            )
            job_r2 = await _claim_next()
            await svc.process(job_r2)
            assert await svc.deliver_once()
            assert fake_api.sent_reactions[-1] == {
                "chat_id": GROUP_ID,
                "message_id": 1509,
                "emoji": "👍",
            }
            assert len(fake_api.sent_messages) == msgs_before_react

            # Lurk emoji on borderline ignore score -> 😂 mapped to 🤣
            gw.intent = RouteDecision("ignore", reply_score=0.25, reply_threshold=0.42)
            assert await plugin.process_update(
                {
                    "update_id": 510,
                    "message": {
                        "message_id": 1510,
                        "date": now_ts + 9,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈哈哈笑死我了",
                    },
                }
            )
            job_lurk = await _claim_next()
            await svc.process(job_lurk)
            assert await svc.deliver_once()
            assert fake_api.sent_reactions[-1] == {
                "chat_id": GROUP_ID,
                "message_id": 1510,
                "emoji": "🤣",
            }
            assert len(fake_api.sent_messages) == msgs_before_react

            # (d) TG typing (sendChatAction) + tier>=3 quick_ack
            fake_api.sent_chat_actions.clear()
            msgs_before_ack = len(fake_api.sent_messages)
            gw.intent = RouteDecision("chat", tier=3)
            gw.next_reply = _chat_actions_json(bubbles=["深度分析完毕~"])
            gw.on_generate_wait = lambda: _wait_until(
                lambda: len(fake_api.sent_messages) > msgs_before_ack,
                max_wait=1.5,
            )
            assert await plugin.process_update(
                {
                    "update_id": 511,
                    "message": {
                        "message_id": 1511,
                        "date": now_ts + 10,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "text": "哈基米帮我深度分析一下这个架构方案",
                    },
                }
            )
            job_t3 = await _claim_next()
            await svc.process(job_t3)
            assert {"chat_id": GROUP_ID, "action": "typing"} in fake_api.sent_chat_actions
            assert len(fake_api.sent_messages) == msgs_before_ack + 1
            ack_msg = fake_api.sent_messages[-1]
            assert ack_msg["chat_id"] == GROUP_ID
            assert ack_msg["reply_to"] == 1511
            assert ack_msg["text"] in {
                "稍等我捋一下",
                "等下，我看眼细节",
                "让我想想怎么讲",
                "稍等，我看下上下文",
            }
            assert await svc.deliver_once()
            assert fake_api.sent_messages[-1]["text"] == "深度分析完毕~"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_telegram_p1_prompt_metadata_no_qq_prefix_and_meme_protocol_injected(
    settings: Any,
) -> None:
    from types import SimpleNamespace

    png_bytes = _make_png_bytes((100, 180, 120))
    voice_bytes = b"OggS" + b"\x33" * 256
    fake_api = _FakeTelegramApi(
        files={
            "TG_PROMPT_PH": (png_bytes, "pic.png"),
            "TG_PROMPT_VO": (voice_bytes, "voice.ogg"),
        }
    )
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {f"telegram:{GROUP_ID}": "at_only"},
        }
    )

    class _CaptureGateway(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return _chat_actions_json(bubbles=["收到图片和语音啦"])

    gw = _CaptureGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = svc.plugin_bus
        plugin, bridge, runner = await _start_connected_plugin(svc, bus, fake_api)
        try:
            await svc.memes.initialize()
            await svc.memes.upload_bytes("开心", png_bytes)

            now_ts = int(time.time())
            assert await plugin.process_update(
                {
                    "update_id": 601,
                    "message": {
                        "message_id": 1601,
                        "date": now_ts,
                        "chat": {"id": int(GROUP_ID), "type": "supergroup", "title": "猫窝群"},
                        "from": {"id": int(FRIEND_UID), "is_bot": False, "first_name": "友友"},
                        "caption": "哈基米看图听语音",
                        "photo": [{"file_id": "TG_PROMPT_PH", "file_size": len(png_bytes)}],
                        "voice": {
                            "file_id": "TG_PROMPT_VO",
                            "duration": 3,
                            "file_size": len(voice_bytes),
                        },
                    },
                }
            )
            job = None
            async with asyncio.timeout(3.0):
                while job is None:
                    job = await svc.store.claim()
                    if job is None:
                        await asyncio.sleep(0.02)
            await svc.process(job)

            gen_payload = next(c[1] for c in gw.calls if c[0] == "generate")
            assert gen_payload["current_sender"] == {
                "nickname": "友友",
                "platform": "telegram",
                "user_id": FRIEND_UID,
            }
            assert "qq" not in gen_payload["current_sender"]
            assert gen_payload["image_sources"]
            assert f"telegram:{FRIEND_UID}" in gen_payload["image_sources"][0]["sender"]
            assert "QQ:" not in gen_payload["image_sources"][0]["sender"]
            assert gen_payload["audio_sources"]
            assert f"telegram:{FRIEND_UID}" in gen_payload["audio_sources"][0]["sender"]
            assert "QQ:" not in gen_payload["audio_sources"][0]["sender"]
            assert "QQ:" not in json.dumps(gen_payload, ensure_ascii=False)

            # Verify real GeminiGateway injects memes.protocol_prompt & meme_labels without AT/QQ:
            captured: list[dict[str, Any]] = []

            async def fake_create(**kwargs: Any) -> Any:
                captured.append(kwargs)

                async def _stream() -> Any:
                    yield SimpleNamespace(
                        event_type="step.start",
                        index=0,
                        step=SimpleNamespace(type="model_output", content=[]),
                    )
                    yield SimpleNamespace(
                        event_type="step.delta",
                        index=0,
                        delta=SimpleNamespace(type="text", text=_chat_actions_json()),
                    )
                    yield SimpleNamespace(
                        event_type="interaction.completed",
                        interaction=SimpleNamespace(id="tg-int-1", status="completed", usage=None),
                    )

                return _stream()

            async def fake_screen(*_args: Any, **_kwargs: Any) -> str:
                return "chat"

            real_gw = GeminiGateway(svc.s, svc.store)
            real_gw.chat_client = SimpleNamespace(
                aio=SimpleNamespace(interactions=SimpleNamespace(create=fake_create))
            )
            real_gw.vertex_chat_client = None
            real_gw.screen = fake_screen  # type: ignore[method-assign]
            tg_msg = Message.model_validate(job["payload"])
            await real_gw.generate(
                gen_payload,
                job["subject"],
                scenario=Scenario.FAST_CHAT,
                message=tg_msg,
            )
            assert len(captured) == 1
            sys_prompt = captured[0]["system_instruction"]
            user_input = captured[0]["input"][0]["text"]
            assert "公开聊天的本地表情协议" in sys_prompt
            assert AT_INSTRUCTION not in sys_prompt
            assert '"meme_labels":["开心"]' in user_input
            assert "QQ:" not in user_input
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)
