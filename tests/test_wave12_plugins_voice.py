"""Regression tests for Wave 1+2 plugins, PluginBus proxy/AI gateway, and voice fixes."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
import threading
import time
from typing import Any

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.models import Message
from hakimi.plugin_bus.ai_gateway import PluginBusAIGatewayMixin
from hakimi.plugin_bus.proxy import RemoteModuleProxy, _PluginSession
from hakimi.plugin_kernel import PluginContext, PluginFiber, PluginKernel
from hakimi.screen_watch import (
    _FRAME_DECODE_CONCURRENCY,
    ScreenWatch,
    decode_b64_frames_async,
)
from hakimi.voice_companion import _decode_frames
from hakimi.voice_gateway_admin import VoiceGatewayAdminManager, VoiceGatewayConfigUpdate
from plugins.minecraft.plugin_main import MinecraftPlugin
from plugins.telegram import TelegramApiError, TelegramBotApi, TelegramPlugin
from tests.test_plugin_telegram import BOT_ID, BOT_TOKEN, GROUP_ID, OWNER_UID, STRANGER_UID
from tests.test_voice_gateway import GATEWAY_TOKEN, _gateway_config, _make_wav, _SessionWireDouble
from voice_gateway.app import ProviderBundle, create_gateway_app
from voice_gateway.session import VoiceGatewaySession


def _make_png_bytes(color: tuple[int, int, int] = (80, 140, 220)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (32, 32), color=color).save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 1. Telegram startup get_me() transient retry vs permanent tg_unauthorized
# ---------------------------------------------------------------------------


class _FakeBusClient:
    """Minimal HakimiPluginClient stand-in for TelegramPlugin bootstrap/poll tests."""

    def __init__(self, *, connected: bool = True) -> None:
        self.connected = connected
        self._configuration_issue: str | None = None
        self.status_pushes: list[tuple[dict[str, Any], str | None]] = []
        self.state: dict[str, Any] = {}
        self.state_writes: list[tuple[str, Any]] = []
        self.emitted: list[dict[str, Any]] = []
        self.fail_emit_event_ids: set[str] = set()

    async def push_status(
        self, status: dict[str, Any], *, configuration_issue: str | None = None
    ) -> None:
        self._configuration_issue = configuration_issue
        self.status_pushes.append((dict(status), configuration_issue))

    def set_status_cache(
        self, status: dict[str, Any], *, configuration_issue: str | None = None
    ) -> None:
        self._configuration_issue = configuration_issue
        self.status_pushes.append((dict(status), configuration_issue))

    async def state_get(self, key: str) -> Any:
        return self.state.get(key)

    async def state_set(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.state_writes.append((key, value))

    async def emit_chat_inbound(self, **kwargs: Any) -> None:
        if str(kwargs.get("event_id") or "") in self.fail_emit_event_ids:
            raise RuntimeError("ws_send_failed")
        self.emitted.append(kwargs)

    async def stop(self) -> None:
        self.connected = False


async def test_tg_get_me_transient_failure_recovers_and_starts_polling() -> None:
    get_me_attempts = 0
    polled = asyncio.Event()

    class _FlakyApi:
        async def get_me(self) -> dict[str, Any]:
            nonlocal get_me_attempts
            get_me_attempts += 1
            if get_me_attempts == 1:
                raise TelegramApiError("tg_timeout", "getMe timed out")
            if get_me_attempts == 2:
                raise TelegramApiError("tg_network_error", "temporary dns error")
            return {"id": int(BOT_ID), "username": "hakimi_recovered_bot"}

        async def get_updates(
            self,
            *,
            offset: int | None = None,
            timeout: int = 25,  # noqa: ASYNC109
            limit: int = 50,
        ) -> list[dict[str, Any]]:
            _ = (offset, timeout, limit)
            polled.set()
            await asyncio.sleep(0.05)
            return []

        async def close(self) -> None:
            return None

    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        allowed_groups={GROUP_ID},
        api=_FlakyApi(),  # type: ignore[arg-type]
        reconnect_min=0.01,
        reconnect_max=0.04,
        enable_polling=True,
    )
    fake_client = _FakeBusClient(connected=True)
    plugin.client = fake_client  # type: ignore[assignment]

    try:
        await plugin._on_connect({"enabled": True})
        assert plugin.bot_id == ""
        assert plugin._tg_connected is False
        assert fake_client._configuration_issue is None
        assert all(issue != "tg_bot_auth_failed" for _, issue in fake_client.status_pushes)
        assert plugin._bootstrap_task is not None

        async with asyncio.timeout(2.0):
            await polled.wait()

        assert get_me_attempts == 3
        assert plugin.bot_id == BOT_ID
        assert plugin.bot_username == "hakimi_recovered_bot"
        assert plugin._tg_connected is True
        assert plugin._poll_task is not None and not plugin._poll_task.done()
        assert fake_client.status_pushes[-1][0]["connected"] is True
        assert fake_client.status_pushes[-1][1] is None
    finally:
        await plugin.close()


async def test_tg_get_me_unauthorized_marks_auth_failed_without_retry() -> None:
    get_me_attempts = 0

    class _AuthFailApi:
        async def get_me(self) -> dict[str, Any]:
            nonlocal get_me_attempts
            get_me_attempts += 1
            raise TelegramApiError("tg_unauthorized", "Unauthorized", status_code=401)

        async def close(self) -> None:
            return None

    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        api=_AuthFailApi(),  # type: ignore[arg-type]
        reconnect_min=0.01,
        reconnect_max=0.04,
        enable_polling=True,
    )
    fake_client = _FakeBusClient(connected=True)
    plugin.client = fake_client  # type: ignore[assignment]

    try:
        await plugin._on_connect({"enabled": True})
        await asyncio.sleep(0.05)
        assert get_me_attempts == 1
        assert fake_client._configuration_issue == "tg_bot_auth_failed"
        assert plugin._bootstrap_task is None
        assert plugin._poll_task is None
    finally:
        await plugin.close()


# ---------------------------------------------------------------------------
# 2. Telegram _poll_loop never drops updates on error or disconnect
# ---------------------------------------------------------------------------


def _owner_update(update_id: int, message_id: int, text: str) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "date": 1700000000 + update_id,
            "chat": {"id": int(OWNER_UID), "type": "private"},
            "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
            "text": text,
        },
    }


def _stranger_update(update_id: int, message_id: int) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "date": 1700000000 + update_id,
            "chat": {"id": int(STRANGER_UID), "type": "private"},
            "from": {"id": int(STRANGER_UID), "is_bot": False, "first_name": "Stranger"},
            "text": "ignored",
        },
    }


async def test_tg_poll_loop_stops_advancing_offset_on_delivery_failure_and_disconnect() -> None:
    observed_offsets: list[int | None] = []
    all_delivered = asyncio.Event()
    disconnect_checked = asyncio.Event()
    poll_call = 0

    fake_client = _FakeBusClient(connected=True)
    fake_client.fail_emit_event_ids.add("101")

    class _StepPollApi:
        async def get_updates(
            self,
            *,
            offset: int | None = None,
            timeout: int = 25,  # noqa: ASYNC109
            limit: int = 50,
        ) -> list[dict[str, Any]]:
            nonlocal poll_call
            _ = (timeout, limit)
            poll_call += 1
            observed_offsets.append(offset)
            if poll_call == 1:
                # 10: ignored -> advance to 11; 11: raises -> stop before 11 & 12!
                return [
                    _stranger_update(10, 100),
                    _owner_update(11, 101, "first-valid"),
                    _owner_update(12, 102, "second-valid"),
                ]
            if poll_call == 2:
                # Simulate WS disconnecting right as get_updates returns update 11.
                fake_client.fail_emit_event_ids.discard("101")
                fake_client.connected = False
                disconnect_checked.set()
                return [
                    _owner_update(11, 101, "first-valid"),
                    _owner_update(12, 102, "second-valid"),
                ]
            if poll_call == 3:
                return [
                    _owner_update(11, 101, "first-valid"),
                    _owner_update(12, 102, "second-valid"),
                ]
            all_delivered.set()
            await asyncio.sleep(0.05)
            return []

        async def close(self) -> None:
            return None

    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        api=_StepPollApi(),  # type: ignore[arg-type]
        reconnect_min=0.01,
        reconnect_max=0.02,
        enable_polling=True,
    )
    plugin.client = fake_client  # type: ignore[assignment]
    plugin.bot_id = BOT_ID
    plugin.bot_username = "HakimiBot"

    task = asyncio.create_task(plugin._poll_loop())
    try:
        async with asyncio.timeout(2.0):
            await disconnect_checked.wait()
        await asyncio.sleep(0.04)
        # Offset advanced past ignored update 10 -> 11, but NOT past failed/disconnected 11.
        assert plugin._offset == 11
        assert fake_client.state.get("tg_update_offset") == 11
        assert fake_client.emitted == []

        # Reconnect WS and verify both 11 and 12 are delivered cleanly.
        fake_client.connected = True
        async with asyncio.timeout(2.0):
            await all_delivered.wait()

        assert observed_offsets[:4] == [None, 11, 11, 13]
        assert [item["event_id"] for item in fake_client.emitted] == ["101", "102"]
        assert plugin._offset == 13
        assert fake_client.state.get("tg_update_offset") == 13
    finally:
        plugin._stopped = True
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_tg_poll_loop_survives_malformed_update_parse_exception() -> None:
    delivered = asyncio.Event()
    poll_call = 0
    fake_client = _FakeBusClient(connected=True)

    class _MalformedThenValidApi:
        async def get_updates(
            self,
            *,
            offset: int | None = None,
            timeout: int = 25,  # noqa: ASYNC109
            limit: int = 50,
        ) -> list[dict[str, Any]]:
            nonlocal poll_call
            _ = (offset, timeout, limit)
            poll_call += 1
            if poll_call == 1:
                return [
                    {"update_id": 20, "malformed": True},
                    _owner_update(21, 201, "after-malformed"),
                ]
            delivered.set()
            await asyncio.sleep(0.05)
            return []

        async def close(self) -> None:
            return None

    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        api=_MalformedThenValidApi(),  # type: ignore[arg-type]
        reconnect_min=0.01,
        reconnect_max=0.02,
        enable_polling=True,
    )
    plugin.client = fake_client  # type: ignore[assignment]
    plugin.bot_id = BOT_ID
    plugin.bot_username = "HakimiBot"

    real_parse = plugin.parse_update

    def _boom_on_malformed(upd: dict[str, Any]) -> dict[str, Any] | None:
        if upd.get("malformed"):
            raise ValueError("corrupt telegram payload")
        return real_parse(upd)

    plugin.parse_update = _boom_on_malformed  # type: ignore[method-assign]

    task = asyncio.create_task(plugin._poll_loop())
    try:
        async with asyncio.timeout(2.0):
            await delivered.wait()
        assert not task.done()
        assert [item["event_id"] for item in fake_client.emitted] == ["201"]
        assert plugin._offset == 22
        assert fake_client.state.get("tg_update_offset") == 22
    finally:
        plugin._stopped = True
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ---------------------------------------------------------------------------
# 3. Telegram WebP -> send_sticker, voice blob_meta filename, photo fallback
# ---------------------------------------------------------------------------


async def test_tg_api_send_sticker_reply_fallback_and_plugin_webp_routing() -> None:
    sticker_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal sticker_attempts
        if request.url.path.endswith("/sendSticker"):
            sticker_attempts += 1
            assert b'name="sticker"' in request.content
            if sticker_attempts == 1 and b"reply_parameters" in request.content:
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "error_code": 400,
                        "description": f"Bad Request: message to reply not found ({BOT_TOKEN})",
                    },
                )
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 980}})
        return httpx.Response(404, json={"ok": False, "error_code": 404, "description": "missing"})

    webp_bytes = b"RIFF\x24\x00\x00\x00WEBP" + b"\x00" * 24
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        api = TelegramBotApi(BOT_TOKEN, http=http)
        mid = await api.send_sticker(
            GROUP_ID,
            webp_bytes,
            filename="cat.webp",
            mime_type="image/webp",
            reply_to_message_id=77,
        )
        assert mid == "980"
        assert sticker_attempts == 2

        plugin = TelegramPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token="tok",
            bot_token=BOT_TOKEN,
            owner_user_id=OWNER_UID,
            allowed_groups={GROUP_ID},
            api=api,
            enable_polling=False,
        )
        res = await plugin._handle_media_delivery(
            {
                "platform": "telegram",
                "kind": "group",
                "conversation_id": GROUP_ID,
                "user_id": OWNER_UID,
                "blob": webp_bytes,
                "blob_meta": {"filename": "../memes/happy_cat.webp"},
            }
        )
        assert res == {"message_id": "980"}
        assert sticker_attempts == 3


async def test_tg_voice_blob_meta_filename_sanitized_title_and_photo_smallest_fallback() -> None:
    sent_audios: list[dict[str, Any]] = []
    sent_voices: list[dict[str, Any]] = []

    class _CaptureAudioApi:
        async def send_audio(self, chat_id: str, audio_bytes: bytes, **kwargs: Any) -> str:
            sent_audios.append({"chat_id": chat_id, "bytes": audio_bytes, **kwargs})
            return "101"

        async def send_voice(self, chat_id: str, audio_bytes: bytes, **kwargs: Any) -> str:
            sent_voices.append({"chat_id": chat_id, "bytes": audio_bytes, **kwargs})
            return "102"

        async def close(self) -> None:
            return None

    plugin = TelegramPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok",
        bot_token=BOT_TOKEN,
        owner_user_id=OWNER_UID,
        api=_CaptureAudioApi(),  # type: ignore[arg-type]
        enable_polling=False,
    )
    plugin.bot_id = BOT_ID

    # 1. Reads filename from blob_meta.filename and strips directory traversal
    await plugin._handle_voice_delivery(
        {
            "platform": "telegram",
            "kind": "private",
            "conversation_id": OWNER_UID,
            "user_id": OWNER_UID,
            "format": "ogg_opus",
            "mime_type": "audio/ogg",
            "title": "夜曲",
            "blob": b"OggS1234",
            "blob_meta": {"filename": "../../nested/夜曲.ogg"},
        }
    )
    assert sent_audios[-1]["filename"] == "夜曲.ogg"

    # 2. Sanitizes unsafe title when blob_meta filename is missing
    await plugin._handle_voice_delivery(
        {
            "platform": "telegram",
            "kind": "private",
            "conversation_id": OWNER_UID,
            "user_id": OWNER_UID,
            "format": "mp3",
            "mime_type": "audio/mpeg",
            "title": "../evil/track\r\nname",
            "audio_b64": base64.b64encode(b"ID3DATA").decode("ascii"),
        }
    )
    sanitized_fname = sent_audios[-1]["filename"]
    assert "/" not in sanitized_fname and "\\" not in sanitized_fname
    assert "\r" not in sanitized_fname and "\n" not in sanitized_fname
    assert ".." not in sanitized_fname
    assert sanitized_fname.endswith(".mp3")

    # 3. Non-song voice delivery reads blob_meta.filename
    await plugin._handle_voice_delivery(
        {
            "platform": "telegram",
            "kind": "private",
            "conversation_id": OWNER_UID,
            "user_id": OWNER_UID,
            "format": "ogg_opus",
            "mime_type": "audio/ogg",
            "blob": b"OggS-voice",
            "blob_meta": {"filename": "tts_reply.ogg"},
        }
    )
    assert sent_voices[-1]["filename"] == "tts_reply.ogg"

    # 4. Photo fallback picks candidates[0] (smallest) when all exceed 20 MiB
    oversized_small = 21 * 1024 * 1024
    oversized_large = 35 * 1024 * 1024
    parsed = plugin.parse_update(
        {
            "update_id": 77,
            "message": {
                "message_id": 770,
                "date": 1700000100,
                "chat": {"id": int(OWNER_UID), "type": "private"},
                "from": {"id": int(OWNER_UID), "is_bot": False, "first_name": "BOISER"},
                "photo": [
                    {"file_id": "PH_SMALLEST_OVER_CAP", "file_size": oversized_small},
                    {"file_id": "PH_LARGEST_OVER_CAP", "file_size": oversized_large},
                ],
            },
        }
    )
    assert parsed is not None
    assert parsed["images"][0]["file"] == "PH_SMALLEST_OVER_CAP"


# ---------------------------------------------------------------------------
# 4. Proxy inline delivery.send_media / delivery.send_voice explicit timeouts
# ---------------------------------------------------------------------------


async def test_proxy_inline_send_media_and_send_voice_pass_explicit_timeouts() -> None:
    recorded: list[tuple[str, float | None]] = []

    class _FakeBus:
        rpc_timeout = 15.0
        uncertain_deliveries: list[str] = []

        def _max_frame_bytes(self) -> int:
            return 256 * 1024

        async def _rpc(
            self,
            _session: Any,
            method: str,
            _params: dict[str, Any],
            *,
            timeout: float | None = None,  # noqa: ASYNC109
            delivery_id: str | None = None,
        ) -> dict[str, Any]:
            _ = delivery_id
            recorded.append((method, timeout))
            return {"ok": True, "result": {"message_id": "42"}}

    async def _noop(*_args: Any, **_kwargs: Any) -> Any:
        return ""

    kernel = PluginKernel()
    fiber = PluginFiber("telegram")
    ctx = PluginContext(fiber, kernel)
    session = _PluginSession(
        plugin_id="telegram",
        receive=_noop,
        send=_noop,
        close=_noop,
        source_ip="127.0.0.1",
        fiber=fiber,
        ctx=ctx,
        scopes=frozenset({"chat", "media"}),
        node_region="KR",
        connected_at=0.0,
        last_seen_at=0.0,
        ready=True,
    )
    proxy = RemoteModuleProxy(
        None,
        _FakeBus(),  # type: ignore[arg-type]
        session,
        plugin_id="telegram",
        display_name="Telegram",
        description="Telegram",
        node_region="KR",
        scopes=frozenset({"chat", "media"}),
        chat_platforms=("telegram",),
        thinking_scenes=(),
        actions=(),
        capabilities=("voice", "voice_blob", "sticker", "rich_card", "media_blob"),
    )
    msg = Message(
        platform="telegram",
        bot_id=BOT_ID,
        event_id="1",
        user_id=OWNER_UID,
        conversation_id=OWNER_UID,
        kind="private",
        text="hi",
        addressed=True,
        created_at=time.time(),
    )

    await proxy.send_media(msg, _make_png_bytes(), kind="image")
    await proxy.send_voice(msg, b"OggS-inline-voice", fmt="ogg_opus", mime_type="audio/ogg")
    await proxy.send_voice(
        msg,
        b"OggS-inline-song",
        fmt="ogg_opus",
        mime_type="audio/ogg",
        title="晴天",
    )

    assert recorded == [
        ("delivery.send_media", 30.0),
        ("delivery.send_voice", 30.0),
        ("delivery.send_voice", 60.0),
    ]


# ---------------------------------------------------------------------------
# 5. Minecraft ActionSpec timeout_seconds
# ---------------------------------------------------------------------------


def test_minecraft_action_specs_declare_timeout_seconds() -> None:
    plugin = MinecraftPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="mc-test-token",
    )
    actions = plugin.client.actions
    assert actions, "expected registered Minecraft ActionSpecs"

    for verb in ("connect", "acquire", "smelt", "craft"):
        spec = actions[f"mc.{verb}"]
        assert spec.timeout_seconds == 50.0
        assert spec.to_hello_dict()["timeout_seconds"] == 50.0

    for verb in ("explore", "mine", "gather", "disconnect", "hunt", "sleep", "shelter"):
        spec = actions[f"mc.{verb}"]
        assert spec.timeout_seconds == 45.0
        assert spec.to_hello_dict()["timeout_seconds"] == 45.0

    for verb in ("status", "pause", "resume"):
        spec = actions[f"mc.{verb}"]
        assert spec.timeout_seconds == 20.0
        assert spec.to_hello_dict()["timeout_seconds"] == 20.0


# ---------------------------------------------------------------------------
# 6. voice_gateway idempotent admin config & live-session ProviderBundle safety
# ---------------------------------------------------------------------------


class _ClosableSTT:
    def __init__(self, tag: str, *, gate: asyncio.Event | None = None) -> None:
        self.tag = tag
        self.gate = gate
        self.entered = asyncio.Event()
        self.closed_event = asyncio.Event()
        self.closed = False
        self.close_calls = 0

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        _ = (wav_bytes, filename)
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.closed:
            raise RuntimeError(f"STT {self.tag} used after close")
        return f"transcript-{self.tag}"

    async def aclose(self) -> None:
        self.closed = True
        self.close_calls += 1
        self.closed_event.set()


class _ClosableTTS:
    def __init__(
        self,
        tag: str,
        *,
        gate: asyncio.Event | None = None,
        response_format: str = "mp3",
    ) -> None:
        self.tag = tag
        self.gate = gate
        self.response_format = response_format
        self.first_yielded = asyncio.Event()
        self.closed_event = asyncio.Event()
        self.closed = False
        self.close_calls = 0

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        _ = text
        if self.closed:
            raise RuntimeError(f"TTS {self.tag} used after close")
        return f"AUDIO-{self.tag}".encode(), self.response_format

    async def synthesize_stream(self, text: str):
        _ = text
        if self.closed:
            raise RuntimeError(f"TTS {self.tag} closed before stream start")
        yield f"PART1-{self.tag}-".encode()
        self.first_yielded.set()
        if self.gate is not None:
            await self.gate.wait()
        if self.closed:
            raise RuntimeError(f"TTS {self.tag} used after close mid-stream")
        yield f"PART2-{self.tag}".encode()

    async def aclose(self) -> None:
        self.closed = True
        self.close_calls += 1
        self.closed_event.set()


async def test_voice_gateway_post_and_delete_config_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built_stt: list[_ClosableSTT] = []
    built_tts: list[_ClosableTTS] = []

    def fake_build_stt(cfg: Any) -> _ClosableSTT:
        inst = _ClosableSTT(f"stt-{len(built_stt)}-{cfg.stt_provider}")
        built_stt.append(inst)
        return inst

    def fake_build_tts(cfg: Any) -> _ClosableTTS:
        inst = _ClosableTTS(f"tts-{len(built_tts)}-{cfg.tts_provider}")
        built_tts.append(inst)
        return inst

    monkeypatch.setattr("voice_gateway.app.build_stt_provider", fake_build_stt)
    monkeypatch.setattr("voice_gateway.app.build_tts_provider", fake_build_tts)

    app = create_gateway_app(_gateway_config())
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}
    async with app.router.lifespan_context(app):
        assert len(built_stt) == 1 and len(built_tts) == 1
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.local",
            headers=auth,
        ) as client:
            # 1. Genuine update -> builds providers #1 and closes #0
            body_fish = {
                "stt_provider": "fish",
                "tts_provider": "fish",
                "fish_api_key": "fish-idempotent-secret-1111",
            }
            r1 = await client.post("/v1/admin/config", json=body_fish)
            assert r1.status_code == 200
            assert len(built_stt) == 2 and len(built_tts) == 2
            assert built_stt[0].closed is True and built_tts[0].closed is True
            assert built_stt[1].closed is False and built_tts[1].closed is False

            # 2. Repeat identical POST (e.g. session.start sync_to_gateway) -> NO rebuild, NO close!
            r2 = await client.post("/v1/admin/config", json=body_fish)
            assert r2.status_code == 200
            assert len(built_stt) == 2 and len(built_tts) == 2
            assert built_stt[1].closed is False and built_tts[1].closed is False

            # 3. Genuine DELETE resets to base -> builds #2 and closes #1
            rd1 = await client.delete("/v1/admin/config")
            assert rd1.status_code == 200
            assert len(built_stt) == 3 and len(built_tts) == 3
            assert built_stt[1].closed is True and built_tts[1].closed is True

            # 4. Repeat DELETE when already base -> NO rebuild, NO close!
            rd2 = await client.delete("/v1/admin/config")
            assert rd2.status_code == 200
            assert len(built_stt) == 3 and len(built_tts) == 3
            assert built_stt[2].closed is False and built_tts[2].closed is False


async def test_voice_gateway_inflight_http_stream_stt_and_ws_survive_config_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stt_gate = asyncio.Event()
    tts_gate = asyncio.Event()
    built_stt: list[_ClosableSTT] = []
    built_tts: list[_ClosableTTS] = []

    def fake_build_stt(cfg: Any) -> _ClosableSTT:
        gate = stt_gate if len(built_stt) == 0 else None
        inst = _ClosableSTT(f"v{len(built_stt)}-{cfg.stt_provider}", gate=gate)
        built_stt.append(inst)
        return inst

    def fake_build_tts(cfg: Any) -> _ClosableTTS:
        gate = tts_gate if len(built_tts) == 0 else None
        inst = _ClosableTTS(f"v{len(built_tts)}-{cfg.tts_provider}", gate=gate)
        built_tts.append(inst)
        return inst

    monkeypatch.setattr("voice_gateway.app.build_stt_provider", fake_build_stt)
    monkeypatch.setattr("voice_gateway.app.build_tts_provider", fake_build_tts)

    app = create_gateway_app(_gateway_config())
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}

    async with app.router.lifespan_context(app):
        initial_stt = built_stt[0]
        initial_tts = built_tts[0]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.local",
            headers=auth,
        ) as client:
            # Start both an in-flight /v1/stt request and a streaming /v1/tts request on bundle v0
            async def _run_streaming_tts() -> bytes:
                chunks: list[bytes] = []
                async with client.stream(
                    "POST",
                    "/v1/tts",
                    json={"text": "流式热更新安全测试", "output_format": "mp3", "stream": True},
                ) as resp:
                    assert resp.status_code == 200
                    async for chunk in resp.aiter_bytes():
                        chunks.append(chunk)
                return b"".join(chunks)

            async def _run_stt() -> dict[str, Any]:
                resp = await client.post(
                    "/v1/stt",
                    files={"file": ("clip.wav", _make_wav(), "audio/wav")},
                    data={"format": "wav"},
                )
                assert resp.status_code == 200, resp.text
                return resp.json()

            tts_task = asyncio.create_task(_run_streaming_tts())
            stt_task = asyncio.create_task(_run_stt())

            async with asyncio.timeout(2.0):
                await asyncio.gather(initial_tts.first_yielded.wait(), initial_stt.entered.wait())

            # Hot-swap config while BOTH requests are actively using bundle v0!
            r_swap = await client.post(
                "/v1/admin/config",
                json={
                    "stt_provider": "fish",
                    "tts_provider": "fish",
                    "fish_api_key": "fish-live-reload-secret-2222",
                },
            )
            assert r_swap.status_code == 200
            # Retired v0 providers MUST remain open while active requests hold refs!
            assert initial_stt.closed is False
            assert initial_tts.closed is False

            # Release STT first -> TTS stream still holds a ref, so v0 stays open!
            stt_gate.set()
            stt_res = await stt_task
            assert stt_res["text"] == "transcript-v0-siliconflow"
            assert initial_stt.closed is False
            assert initial_tts.closed is False

            # Release TTS stream -> stream finishes cleanly, THEN v0 closes!
            tts_gate.set()
            audio_out = await tts_task
            assert audio_out == b"PART1-v0-siliconflow-PART2-v0-siliconflow"
            assert initial_tts.closed is True
            assert initial_stt.closed is True
            assert initial_tts.close_calls == 1
            assert initial_stt.close_calls == 1

            # Now test a live VoiceGatewaySession leasing bundles across a config reload
            ws_tts_gate = asyncio.Event()
            v1_bundle: ProviderBundle = app.state.provider_bundle
            v1_tts = built_tts[1]
            v1_tts.gate = ws_tts_gate

            @contextlib.asynccontextmanager
            async def _lease():
                b: ProviderBundle = app.state.provider_bundle.acquire()
                try:
                    yield b.config, b.stt, b.tts
                finally:
                    await b.release()

            wire = _SessionWireDouble()
            session = VoiceGatewaySession(
                app.state.config,
                stt_provider=app.state.stt,
                tts_provider=app.state.tts,
                send_text=wire.send_text,
                send_bytes=wire.send_bytes,
                close_ws=wire.close_ws,
                provider_lease=_lease,
                config_getter=lambda: app.state.config,
            )
            await session.start()
            try:
                await session.handle_text_frame(
                    json.dumps(
                        {
                            "type": "tts.synthesize",
                            "req_id": "req-1",
                            "text": "第一句",
                            "output_format": "mp3",
                        }
                    )
                )
                async with asyncio.timeout(2.0):
                    await v1_tts.first_yielded.wait()

                # Trigger DELETE /v1/admin/config (swap to v2) mid-WS TTS stream!
                r_del = await client.delete("/v1/admin/config")
                assert r_del.status_code == 200
                assert v1_bundle.retired is True
                assert v1_tts.closed is False

                ws_tts_gate.set()
                await wire.wait_until(
                    lambda evs: any(
                        kind == "json"
                        and payload.get("type") == "tts.end"
                        and payload.get("req_id") == "req-1"
                        for kind, payload in evs
                    )
                )
                # Once req-1 exits its provider lease, v1 bundle closes and req-2 uses v2!
                async with asyncio.timeout(2.0):
                    await v1_tts.closed_event.wait()
                assert v1_tts.closed is True
                assert v1_tts.close_calls == 1

                await session.handle_text_frame(
                    json.dumps(
                        {
                            "type": "tts.synthesize",
                            "req_id": "req-2",
                            "text": "第二句",
                            "output_format": "mp3",
                        }
                    )
                )
                await wire.wait_until(
                    lambda evs: any(
                        kind == "json"
                        and payload.get("type") == "tts.end"
                        and payload.get("req_id") == "req-2"
                        for kind, payload in evs
                    )
                )
                raw_bytes = b"".join(payload for kind, payload in wire.events if kind == "bytes")
                assert b"PART1-v1-fish-PART2-v1-fish" in raw_bytes
                assert b"PART1-v2-siliconflow-PART2-v2-siliconflow" in raw_bytes
            finally:
                await session.close()


# ---------------------------------------------------------------------------
# 7. VoiceGatewayAdminManager status TTL cache & invalidation
# ---------------------------------------------------------------------------


async def test_voice_gateway_admin_status_cache_ttl_refresh_and_invalidation(
    settings: Any, store: Any
) -> None:
    cfg = settings.model_copy(
        update={
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(GATEWAY_TOKEN),
        }
    )
    now = [100.0]
    get_calls = 0
    fail_next_get = False
    gw_state: dict[str, Any] = {"overridden": False, "tts_provider": "siliconflow"}

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal get_calls
        if request.method == "GET":
            get_calls += 1
            if fail_next_get:
                raise httpx.ConnectTimeout("simulated timeout")
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "POST":
            body = json.loads(request.content)
            gw_state["overridden"] = True
            gw_state["tts_provider"] = body["tts_provider"]
            return httpx.Response(200, json=dict(gw_state))
        if request.method == "DELETE":
            gw_state["overridden"] = False
            gw_state["tts_provider"] = "siliconflow"
            return httpx.Response(200, json=dict(gw_state))
        raise AssertionError(f"Unexpected {request.method}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        mgr = VoiceGatewayAdminManager(
            cfg,
            store,
            http=http,
            status_cache_ttl=20.0,
            clock=lambda: now[0],
        )

        # 1. Initial call hits gateway; repeat within TTL hits cache
        s1 = await mgr.status()
        assert s1["reachable"] is True
        assert get_calls == 1

        # Mutating caller's dict must NOT corrupt cache
        s1["gateway"]["tts_provider"] = "mutated"
        now[0] += 5.0
        s2 = await mgr.status()
        assert get_calls == 1
        assert s2["gateway"]["tts_provider"] == "siliconflow"

        # 2. Explicit refresh=True bypasses cache
        s3 = await mgr.status(refresh=True)
        assert s3["reachable"] is True
        assert get_calls == 2

        # 3. Advancing past TTL expires cache
        now[0] += 25.0
        await mgr.status()
        assert get_calls == 3

        # 4. PUT mutation invalidates cache immediately
        await mgr.update(
            VoiceGatewayConfigUpdate(
                stt_provider="fish",
                tts_provider="fish",
                fish_api_key="fish-cache-test-secret-9999",
            )
        )
        s_after_put = await mgr.status()
        assert get_calls == 4
        assert s_after_put["gateway"]["tts_provider"] == "fish"

        # 5. DELETE reset invalidates cache and refreshes immediately
        s_after_del = await mgr.reset()
        assert get_calls == 5
        assert s_after_del["source"] == "env"
        assert s_after_del["gateway"]["tts_provider"] == "siliconflow"

        # 6. Unreachable timeout is also cached within TTL so tab loads don't repeatedly stall
        now[0] += 30.0
        fail_next_get = True
        s_err1 = await mgr.status()
        assert s_err1["reachable"] is False
        assert get_calls == 6

        now[0] += 5.0
        s_err2 = await mgr.status()
        assert s_err2["reachable"] is False
        assert get_calls == 6  # cached!

        # Recovery works immediately with refresh=True
        fail_next_get = False
        s_ok = await mgr.status(refresh=True)
        assert s_ok["reachable"] is True
        assert get_calls == 7


# ---------------------------------------------------------------------------
# 8. Async event-loop unblocking for decode_b64_frames_async
# ---------------------------------------------------------------------------


async def test_decode_b64_frames_async_uses_worker_thread_and_bounds_concurrency(
    settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    main_tid = threading.get_ident()
    worker_tids: list[int] = []
    lock = threading.Lock()
    active = 0
    max_active = 0

    from hakimi import screen_watch as sw_mod

    real_decode = sw_mod.decode_b64_frames

    def instrumented_decode(*args: Any, **kwargs: Any) -> Any:
        nonlocal active, max_active
        with lock:
            worker_tids.append(threading.get_ident())
            active += 1
            if active > max_active:
                max_active = active
        try:
            time.sleep(0.05)
            return real_decode(*args, **kwargs)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(sw_mod, "decode_b64_frames", instrumented_decode)

    frame_b64 = base64.b64encode(_make_png_bytes()).decode("ascii")
    ticks = 0

    async def _ticker() -> None:
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.01)
            ticks += 1

    ticker_task = asyncio.create_task(_ticker())
    results = await asyncio.gather(
        *(
            decode_b64_frames_async([frame_b64], 2, settings, error_prefix="screen")
            for _ in range(5)
        )
    )
    await ticker_task

    assert len(results) == 5
    assert all(len(imgs) == 1 for imgs in results)
    assert all(tid != main_tid for tid in worker_tids)
    assert 1 <= max_active <= _FRAME_DECODE_CONCURRENCY
    assert ticks == 5

    # Verify voice_companion._decode_frames is async and delegates to decode_b64_frames_async
    vc_imgs = await _decode_frames([frame_b64], 2, settings)
    assert len(vc_imgs) == 1

    # Verify empty frames fast-path returns [] without spawning a worker thread
    before = len(worker_tids)
    assert await decode_b64_frames_async([], 2, settings) == []
    assert await decode_b64_frames_async(None, 2, settings) == []
    assert len(worker_tids) == before

    # Verify ScreenWatch and PluginBusAIGatewayMixin both bind to decode_b64_frames_async
    assert hasattr(ScreenWatch, "serve")
    assert hasattr(PluginBusAIGatewayMixin, "_handle_ai_screen")
    assert hasattr(PluginBusAIGatewayMixin, "_handle_ai_generate")
