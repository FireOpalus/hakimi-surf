"""Tests for Phase 1 cross-platform voice reply pipeline, gateway ogg_opus, and QQ record."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import sys
import time
import wave
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.bot_server import assemble_chat_input, prompt_context
from hakimi.config import Settings
from hakimi.gemini import GeminiGateway
from hakimi.models import Message
from hakimi.modules.adapters import Sender
from hakimi.modules.base import BaseModule
from hakimi.outbound_actions import (
    ACTION_INSTRUCTION,
    CHAT_RESPONSE_FORMAT,
    ChatActions,
    parse_actions,
)
from hakimi.outgoing import deliver_actions, deliver_proactive_jit
from hakimi.plugin_admin import router as plugin_router
from hakimi.plugin_bus import PluginBus
from hakimi.plugin_kernel import PluginKernel
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.security import Rejected, configured_secrets
from hakimi.service import Service
from hakimi.voice_reply import (
    VoiceReplyClient,
    explicit_voice_request,
    sanitize_voice_text,
)
from hakimi_plugin_sdk import HakimiPluginClient, PluginRuntimeConfig
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import TOKEN
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_v20_phase1_modules_and_world_state import V181_STATIC_PREFIX_DIGESTS
from voice_gateway.app import create_gateway_app
from voice_gateway.audio_codec import (
    OUTPUT_FORMATS,
    output_format_meta,
    probe_audio_duration_ms,
    transcode_audio_stream,
)
from voice_gateway.config import VoiceGatewayConfig

FAKE_MP3 = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00" + b"\x00" * 64
FAKE_OGG = b"OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00" + b"\x01" * 64
VG_TOKEN = "vg-test-token-12345678901234567890"


def _make_wav(duration_ms: int = 250, *, sample_rate: int = 16000) -> bytes:
    buf = io.BytesIO()
    frames = max(1, round(sample_rate * duration_ms / 1000.0))
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(b"\x01\x00" * frames)
    return buf.getvalue()


def _write_fake_ffmpeg_and_ffprobe(tmp_path: Path) -> tuple[str, str]:
    ffmpeg_path = tmp_path / "ffmpeg"
    ffprobe_path = tmp_path / "ffprobe"
    ffmpeg_path.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import io, sys, wave",
                "args = sys.argv[1:]",
                "data = sys.stdin.buffer.read()",
                "if not data:",
                "    sys.exit(1)",
                "out_fmt = ''",
                "for idx in range(len(args) - 1):",
                "    if args[idx] == '-f':",
                "        out_fmt = args[idx + 1]",
                "if out_fmt == 'wav':",
                "    buf = io.BytesIO()",
                "    with wave.open(buf, 'wb') as wf:",
                "        wf.setnchannels(1)",
                "        wf.setsampwidth(2)",
                "        wf.setframerate(32000)",
                "        wf.writeframes(bytes([1, 0]) * 3200)",
                "    sys.stdout.buffer.write(buf.getvalue())",
                "elif out_fmt == 'ogg':",
                "    sys.stdout.buffer.write(b'OggS' + data)",
                "else:",
                "    sys.stdout.buffer.write(b'TRANSCODED:' + data)",
            ]
        ),
        encoding="utf-8",
    )
    ffprobe_path.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import os, sys",
                "path = sys.argv[-1]",
                "if not os.path.exists(path):",
                "    sys.exit(1)",
                "print('1.850000')",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(ffmpeg_path, 0o755)
    os.chmod(ffprobe_path, 0o755)
    return str(ffmpeg_path), str(ffprobe_path)


class _OneBotWire:
    def __init__(self, *, fail: bool = False, reject_code: str | None = None) -> None:
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls: list[tuple[str, dict]] = []
        self.fail = fail
        self.reject_code = reject_code

    async def call(self, action: str, params: dict) -> dict:
        self.calls.append((action, params))
        if self.reject_code:
            raise Rejected(self.reject_code)
        if self.fail:
            raise TimeoutError("simulated timeout")
        return {"message_id": 100 + len(self.calls)}


def _voice_settings(settings: Settings, **overrides) -> Settings:
    data = {
        "voice_reply_enabled": True,
        "voice_reply_mode": "owner_only",
        "voice_gateway_url": "http://hakimi-voice-gateway:12145",
        "voice_gateway_token": SecretStr(VG_TOKEN),
        "voice_reply_timeout_seconds": 5.0,
        "voice_reply_max_chars": 120,
        "voice_reply_cooldown_seconds": 30.0,
        "voice_reply_daily_limit": 40,
    }
    data.update(overrides)
    return settings.model_copy(update=data)


def _action_json(**overrides) -> str:
    base = {
        "abstain": False,
        "bubbles": ["好呀~"],
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
    }
    base.update(overrides)
    return json.dumps(base, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 1. voice_gateway: ogg_opus & X-Audio-Duration-Ms
# ---------------------------------------------------------------------------


async def test_gateway_ogg_opus_and_duration_headers(tmp_path):
    assert "ogg_opus" in OUTPUT_FORMATS
    assert output_format_meta("ogg_opus") == ("audio/ogg", 48000, 1)

    fake_ffmpeg, _ = _write_fake_ffmpeg_and_ffprobe(tmp_path)
    opus_chunks = [
        chunk
        async for chunk in transcode_audio_stream(
            [b"MP3DATA"], src_fmt="mp3", dst_fmt="ogg_opus", ffmpeg_bin=fake_ffmpeg
        )
    ]
    assert b"".join(opus_chunks) == b"OggSMP3DATA"

    wav_250 = _make_wav(250, sample_rate=16000)
    assert await probe_audio_duration_ms(wav_250, "wav") == 250
    assert await probe_audio_duration_ms(b"\x00" * 32000, "pcm_16k") == 1000
    assert await probe_audio_duration_ms(b"\x00" * 192000, "pcm_48k_stereo") == 1000
    assert await probe_audio_duration_ms(FAKE_OGG, "ogg_opus", ffmpeg_bin=fake_ffmpeg) == 1850
    assert (
        await probe_audio_duration_ms(FAKE_OGG, "ogg_opus", ffmpeg_bin="/nonexistent-ffmpeg")
        is None
    )

    class _FakeSTT:
        async def transcribe(self, wav_bytes: bytes, *, filename: str = "x.wav") -> str:
            return ""

    class _FakeTTS:
        response_format = "mp3"

        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return FAKE_MP3, "mp3"

        async def synthesize_stream(self, text: str):
            yield FAKE_MP3

    cfg = VoiceGatewayConfig(
        gateway_token=VG_TOKEN,
        ws_url="wss://127.0.0.1:12141/ws/companion/0123456789abcdef0123456789abcdef",
        ws_token="companion-secret-token-1234567890",
        ffmpeg_bin=fake_ffmpeg,
        tts_streaming=False,
        default_output_format="ogg_opus",
    )
    app = create_gateway_app(cfg, stt_provider=_FakeSTT(), tts_provider=_FakeTTS())
    with TestClient(app) as client:
        r_ogg = client.post(
            "/v1/tts",
            headers={"Authorization": f"Bearer {VG_TOKEN}"},
            json={"text": "早安喵", "output_format": "ogg_opus", "stream": False},
        )
        assert r_ogg.status_code == 200
        assert r_ogg.headers["x-audio-format"] == "ogg_opus"
        assert r_ogg.headers["content-type"].startswith("audio/ogg")
        assert r_ogg.headers["x-audio-duration-ms"] == "1850"
        assert r_ogg.content.startswith(b"OggS")

        r_wav = client.post(
            "/v1/tts",
            headers={"Authorization": f"Bearer {VG_TOKEN}"},
            json={"text": "测试wav", "output_format": "wav", "stream": False},
        )
        assert r_wav.status_code == 200
        assert r_wav.headers["x-audio-format"] == "wav"
        assert r_wav.headers["x-audio-duration-ms"] == "100"


# ---------------------------------------------------------------------------
# 2. Schema, static prefix preservation, prompt hint, and abstain clearing
# ---------------------------------------------------------------------------


async def test_schema_static_prefix_integrity_and_prompt_hint(settings, store, message, sdk_pair):
    assert "voice_text" in CHAT_RESPONSE_FORMAT["properties"]
    assert "voice_text" in CHAT_RESPONSE_FORMAT["required"]
    assert set(CHAT_RESPONSE_FORMAT["required"]) == set(ChatActions.model_fields)
    assert "voice_text" not in ACTION_INSTRUCTION

    # Backward compatibility: legacy JSON omitting voice_text still validates
    legacy_payload = json.loads(_action_json())
    legacy_payload.pop("voice_text")
    parsed_legacy = ChatActions.model_validate_json(json.dumps(legacy_payload))
    assert parsed_legacy.voice_text is None

    # StaticPrefixes KV-cache digests must remain byte-identical
    pool = StaticPrefixes()
    for bucket, msg in {
        "owner_dm": message,
        "friend_dm": message.model_copy(update={"user_id": "30000", "conversation_id": "30000"}),
        "group": message.model_copy(
            update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
        ),
    }.items():
        text = pool.get(prompt_context(msg, settings), settings.onebot_self_id)
        assert (
            hashlib.sha256(text.encode("utf-8")).hexdigest() == V181_STATIC_PREFIX_DIGESTS[bucket]
        )

    # voice_reply_hint injected only when voice_reply_available=True and not sandbox_mode
    wire_no = assemble_chat_input({"current_message": "hi"}, message, settings)
    assert "voice_reply_hint" not in wire_no
    wire_yes = assemble_chat_input(
        {"current_message": "hi", "voice_reply_available": True}, message, settings
    )
    assert "voice_reply_hint" in wire_yes
    wire_sb = assemble_chat_input(
        {"current_message": "hi", "voice_reply_available": True},
        message,
        settings,
        sandbox_mode=True,
    )
    assert "voice_reply_hint" not in wire_sb

    # parse_actions gate
    raw_voice = _action_json(bubbles=[""], voice_text="晚安啦，**快去睡觉** [CQ:face,id=1]")
    denied, _ = parse_actions(raw_voice, public=False, settings=settings, allow_voice=False)
    assert denied.voice_text is None
    allowed, _ = parse_actions(raw_voice, public=False, settings=settings, allow_voice=True)
    assert allowed.voice_text == "晚安啦，快去睡觉"

    # GeminiGateway clears voice_text on abstain=True
    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _action_json(
            abstain=True, bubbles=["别发"], voice_text="不该念出来"
        )
        return httpx.Response(200, json=resp)

    gw = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        out = await gw.generate(
            {"current_message": "hi", "current_sender": {"nickname": "BOISER", "qq": "20000"}},
            store.subject(message),
            message=message,
        )
        assert ChatActions.model_validate_json(out).voice_text is None
    finally:
        await gw.close()


# ---------------------------------------------------------------------------
# 3. Settings URL validation, secret redaction, sanitize_voice_text, VoiceReplyClient
# ---------------------------------------------------------------------------


async def test_voice_reply_client_gating_sanitization_and_tts(settings, message):
    s = _voice_settings(settings)
    assert VG_TOKEN in configured_secrets(s)

    # URL validation: internal Docker / loopback http allowed, public http & query rejected
    Settings(
        **{
            **s.model_dump(),
            "voice_gateway_url": "https://voice.example.com/",
        }
    )
    Settings(
        **{
            **s.model_dump(),
            "voice_gateway_url": "http://127.0.0.1:12145",
        }
    )
    with pytest.raises(ValidationError):
        Settings(**{**s.model_dump(), "voice_gateway_url": "http://public.example.com:12145"})
    with pytest.raises(ValidationError):
        Settings(**{**s.model_dump(), "voice_gateway_url": "https://voice.example.com?a=1"})

    # Sanitization
    assert sanitize_voice_text(f"token={VG_TOKEN}", settings=s) is None
    assert sanitize_voice_text("看看 https://example.com", settings=s) is None
    assert sanitize_voice_text("```print(1)```", settings=s) is None
    assert sanitize_voice_text("x" * 121, settings=s, max_chars=120) is None
    assert (
        sanitize_voice_text("去[这里](ref)看下 *好的* [CQ:at,qq=123] 喵~", settings=s)
        == "去这里看下 好的 喵~"
    )

    assert explicit_voice_request("给我发条语音听听")
    assert explicit_voice_request("use voice note please")
    assert not explicit_voice_request("今天天气真好")

    # Modes: owner_only / allowlist / all
    owner_dm = message
    friend_dm = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    grp_40000 = message.model_copy(
        update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
    )
    grp_50000 = message.model_copy(
        update={"user_id": "30000", "kind": "group", "conversation_id": "50000"}
    )

    c_owner = VoiceReplyClient(s)
    assert c_owner.allowed_for_message(owner_dm) is True
    assert c_owner.allowed_for_message(owner_dm, sandbox=True) is False
    assert c_owner.allowed_for_message(friend_dm) is False
    assert c_owner.allowed_for_message(grp_40000) is False

    c_allow = VoiceReplyClient(
        _voice_settings(
            settings,
            voice_reply_mode="allowlist",
            voice_reply_allowed_groups={"onebot:40000"},
        )
    )
    assert c_allow.allowed_for_message(owner_dm) is True
    assert c_allow.allowed_for_message(friend_dm) is False
    assert c_allow.allowed_for_message(grp_40000) is True
    assert c_allow.allowed_for_message(grp_50000) is False

    c_all = VoiceReplyClient(_voice_settings(settings, voice_reply_mode="all"))
    assert c_all.allowed_for_message(friend_dm) is True
    assert c_all.allowed_for_message(grp_50000) is True

    # Cooldown + explicit bypass + daily cap
    now = [1_700_000_000.0]
    c_lim = VoiceReplyClient(
        _voice_settings(settings, voice_reply_cooldown_seconds=30.0, voice_reply_daily_limit=2),
        clock=lambda: now[0],
    )
    assert c_lim.can_send_now("scope:a", explicit=False) == (True, "ok")
    c_lim.note_sent("scope:a")
    assert c_lim.can_send_now("scope:a", explicit=False) == (False, "cooldown")
    assert c_lim.can_send_now("scope:a", explicit=True) == (True, "ok")
    now[0] += 31.0
    assert c_lim.can_send_now("scope:a", explicit=False) == (True, "ok")
    c_lim.note_sent("scope:a")
    assert c_lim.can_send_now("scope:a", explicit=True) == (False, "daily_limit")

    # HTTP TTS synthesize success & failure
    tts_requests: list[dict] = []
    fail_mode = {"status": 200}

    def tts_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Bearer {VG_TOKEN}"
        tts_requests.append(json.loads(request.content))
        if fail_mode["status"] != 200:
            return httpx.Response(fail_mode["status"], text="boom")
        return httpx.Response(
            200,
            content=FAKE_MP3,
            headers={
                "content-type": "audio/mpeg",
                "x-audio-format": "mp3",
                "x-audio-duration-ms": "1420",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(tts_handler)) as http:
        client = VoiceReplyClient(s, http=http)
        synth = await client.synthesize("早安喵~", output_format="mp3")
        assert synth.audio_bytes == FAKE_MP3
        assert synth.fmt == "mp3"
        assert synth.mime_type == "audio/mpeg"
        assert synth.duration_ms == 1420
        assert tts_requests[-1] == {"text": "早安喵~", "output_format": "mp3", "stream": False}

        fail_mode["status"] = 502
        with pytest.raises(Rejected, match="voice_tts_failed"):
            await client.synthesize("失败测试", output_format="mp3")


# ---------------------------------------------------------------------------
# 4. Sender.send_voice (OneBot11 record segment + validation)
# ---------------------------------------------------------------------------


async def test_sender_send_voice_onebot_validation(settings, message):
    wire = _OneBotWire()
    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, wire)
        r_priv = await sender.send_voice(message, FAKE_MP3, fmt="mp3", duration_ms=1200)
        assert r_priv == {"message_id": 101}
        action, params = wire.calls[-1]
        assert action == "send_private_msg"
        assert params["user_id"] == 20000
        seg = params["message"][0]
        assert seg["type"] == "record"
        assert seg["data"]["file"] == "base64://" + base64.b64encode(FAKE_MP3).decode("ascii")

        grp = message.model_copy(
            update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
        )
        await sender.send_voice(grp, FAKE_OGG, fmt="ogg_opus")
        assert wire.calls[-1][0] == "send_group_msg"

        # Valid magic variants (WAV, AMR, SILK, 0xFF MP3)
        for sample in (_make_wav(100), b"#!AMR\n1234", b"\x02#!SILK_V3abc", b"\xff\xfb\x90\x00"):
            await sender.send_voice(message, sample)

        # Invalid magic rejected
        with pytest.raises(Rejected, match="invalid_voice_audio"):
            await sender.send_voice(message, b"not-audio-bytes")

        # Non-onebot rejected
        qq_msg = message.model_copy(
            update={
                "platform": "qq",
                "bot_id": "99999",
                "user_id": "user-openid",
                "conversation_id": "user-openid",
            }
        )
        with pytest.raises(Rejected, match="voice_delivery_forbidden"):
            await sender.send_voice(qq_msg, FAKE_MP3)

        # Proactive to non-owner rejected
        stranger = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
        with pytest.raises(Rejected, match="proactive_not_authorized"):
            await sender.send_voice(stranger, FAKE_MP3, proactive=True)


# ---------------------------------------------------------------------------
# 5. outgoing.py deliver_actions & deliver_proactive_jit voice + fallback
# ---------------------------------------------------------------------------


async def test_deliver_actions_voice_pure_mixed_fallback_and_uncertain(settings, store, message):
    s = _voice_settings(settings)
    wire = _OneBotWire()
    tts_state = {"fail": False}

    def tts_handler(request: httpx.Request) -> httpx.Response:
        if tts_state["fail"]:
            return httpx.Response(503, text="unavailable")
        return httpx.Response(
            200,
            content=FAKE_MP3,
            headers={
                "content-type": "audio/mpeg",
                "x-audio-format": "mp3",
                "x-audio-duration-ms": "900",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(tts_handler)) as http:
        sender = Sender(s, http, wire)
        svc = Service(s, store, FakeGateway(), FakeBili(), sender, http=http)
        try:
            # 5a. Pure voice reply (bubbles=[""], voice_text="晚安喵~")
            await store.enqueue_reply(
                "job-voice-pure",
                message,
                "",
                session_epoch=0,
                actions={
                    "abstain": False,
                    "bubbles": [""],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "watermark": 0,
                    "public": False,
                    "voice_text": "晚安喵~",
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            assert wire.calls[-1][1]["message"][0]["type"] == "record"
            snap = await svc.sessions.snapshot(message)
            assert snap["recent_messages"][-1]["text"] == "[语音] 晚安喵~"

            # 5b. Cooldown active -> non-explicit voice_text falls back to text
            await store.enqueue_reply(
                "job-voice-cooldown",
                message,
                "先文字说一句",
                session_epoch=0,
                actions={
                    "abstain": False,
                    "bubbles": ["先文字说一句"],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "watermark": 0,
                    "public": False,
                    "voice_text": "这句降级为文字",
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            assert [c[1]["message"][0]["type"] for c in wire.calls[-2:]] == ["text", "text"]
            assert wire.calls[-1][1]["message"][0]["data"]["text"] == "这句降级为文字"

            # 5c. Explicit voice bypasses cooldown, but TTS error falls back to text
            tts_state["fail"] = True
            await store.enqueue_reply(
                "job-voice-tts-fail",
                message,
                "",
                session_epoch=0,
                actions={
                    "abstain": False,
                    "bubbles": [""],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "watermark": 0,
                    "public": False,
                    "voice_text": "TTS坏了降级",
                    "explicit_voice": True,
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            assert wire.calls[-1][1]["message"][0]["type"] == "text"
            assert wire.calls[-1][1]["message"][0]["data"]["text"] == "TTS坏了降级"

            # 5d. Post-dispatch transport timeout stays uncertain, NEVER falls back to text
            tts_state["fail"] = False
            wire.fail = True
            before_calls = len(wire.calls)
            await store.enqueue_reply(
                "job-voice-uncertain",
                message,
                "",
                session_epoch=0,
                actions={
                    "abstain": False,
                    "bubbles": [""],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "watermark": 0,
                    "public": False,
                    "voice_text": "不能重复发文字",
                    "explicit_voice": True,
                },
            )
            assert await deliver_actions(svc, await store.claim_reply())
            assert len(wire.calls) == before_calls + 1
            assert wire.calls[-1][1]["message"][0]["type"] == "record"
            row = await store.one("SELECT status FROM outbox WHERE id='job-voice-uncertain'")
            assert row[0] == "uncertain"
        finally:
            await svc.stop()


async def test_deliver_proactive_jit_pure_voice_and_fallback(settings, store, message):
    s = _voice_settings(settings)
    wire = _OneBotWire()
    tts_state = {"fail": False}

    def tts_handler(request: httpx.Request) -> httpx.Response:
        if tts_state["fail"]:
            return httpx.Response(500)
        return httpx.Response(
            200,
            content=FAKE_MP3,
            headers={
                "content-type": "audio/mpeg",
                "x-audio-format": "mp3",
                "x-audio-duration-ms": "1100",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(tts_handler)) as http:
        svc = Service(s, store, FakeGateway(), FakeBili(), Sender(s, http, wire), http=http)
        await svc.world_state.initialize()
        try:
            # Pure voice proactive message (bubbles=[""])
            res_voice = await deliver_proactive_jit(
                svc,
                message,
                _action_json(bubbles=[""], voice_text="早安喵，记得吃早饭~"),
                reply_id="jit:voice-1",
            )
            assert res_voice["status"] == "sent"
            assert res_voice["bubbles_sent"] == 1
            assert wire.calls[-1][1]["message"][0]["type"] == "record"
            snap = await svc.sessions.snapshot(message)
            assert snap["recent_messages"][-1]["text"] == "[语音] 早安喵，记得吃早饭~"

            # TTS failure -> fallback to text bubble
            svc.voice_reply._last_sent.clear()
            tts_state["fail"] = True
            res_fb = await deliver_proactive_jit(
                svc,
                message,
                _action_json(bubbles=[""], voice_text="晚点再叫你~"),
                reply_id="jit:voice-2",
            )
            assert res_fb["status"] == "sent"
            assert res_fb["bubbles_sent"] == 1
            assert wire.calls[-1][1]["message"][0]["type"] == "text"
            assert wire.calls[-1][1]["message"][0]["data"]["text"] == "晚点再叫你~"
        finally:
            await svc.stop()


# ---------------------------------------------------------------------------
# 6. Cross-platform PluginBus + SDK delivery.send_voice & oversize fallback
# ---------------------------------------------------------------------------


async def test_plugin_bus_cross_platform_voice_delivery_and_oversize_fallback(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "voice_reply_enabled": True,
            "voice_reply_mode": "all",
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr(VG_TOKEN),
            "plugin_bus_ws_max_frame_bytes": 16_384,
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    if not any(getattr(r, "path", "") == "/admin/api/plugins" for r in app.routes):
        app.include_router(plugin_router)

    tts_payload = {"audio": FAKE_OGG}

    def tts_handler(request: httpx.Request) -> httpx.Response:
        req = json.loads(request.content)
        assert req["output_format"] == "ogg_opus"
        return httpx.Response(
            200,
            content=tts_payload["audio"],
            headers={
                "content-type": "audio/ogg",
                "x-audio-format": "ogg_opus",
                "x-audio-duration-ms": "1350",
            },
        )

    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus: PluginBus = svc.plugin_bus
        token, _ = await bus.create_plugin(
            "tg_bridge",
            "Telegram Bridge",
            ["chat"],
            node_region="KR",
        )

        class _InMemSocketPair:
            def __init__(self) -> None:
                self.c2s: asyncio.Queue[str] = asyncio.Queue()
                self.s2c: asyncio.Queue[str] = asyncio.Queue()
                self.closed = False

            async def server_recv(self) -> str:
                item = await self.c2s.get()
                if item == "__EOF__":
                    raise RuntimeError("eof")
                return item

            async def server_send(self, raw: str) -> None:
                await self.s2c.put(raw)

            async def server_close(self, code: int = 1000) -> None:
                self.closed = True
                await self.c2s.put("__EOF__")
                await self.s2c.put("__EOF__")

        pair = _InMemSocketPair()

        class _ClientWs:
            async def send(self, raw: str) -> None:
                await pair.c2s.put(raw)

            async def recv(self) -> str:
                item = await pair.s2c.get()
                if item == "__EOF__":
                    raise ConnectionError("closed")
                return item

            async def close(self, code: int = 1000) -> None:
                await pair.server_close(code)

        delivered_voices: list[dict] = []
        delivered_texts: list[dict] = []

        client = HakimiPluginClient(
            PluginRuntimeConfig(
                ws_url="ws://127.0.0.1:12141/ws/plugin",
                plugin_id="tg_bridge",
                token=token,
                display_name="Telegram Bridge",
                scopes=("chat",),
                chat_platforms=("telegram",),
                capabilities=("voice",),
                voice_format="ogg_opus",
            ),
            connect_factory=lambda *a, **kw: _ClientWs(),
            max_frame_bytes=16_384,
        )

        connected_evt = asyncio.Event()

        @client.on_connect
        async def _on_connect() -> None:
            connected_evt.set()

        @client.on_voice_delivery
        async def _on_voice(params: dict) -> dict:
            delivered_voices.append(params)
            return {"message_id": f"tg-v-{len(delivered_voices)}"}

        @client.rpc("delivery.send")
        async def _on_text(params: dict) -> dict:
            delivered_texts.append(params)
            return {"message_id": f"tg-t-{len(delivered_texts)}"}

        server_task = asyncio.create_task(
            bus.serve(
                pair.server_recv,
                pair.server_send,
                pair.server_close,
                plugin_id="tg_bridge",
                source_ip="127.0.0.1",
            )
        )
        await client.start()
        try:
            async with asyncio.timeout(2.0):
                await connected_evt.wait()

            proxy = svc.modules.chat_platforms["telegram"]
            assert proxy.supports_voice is True
            assert proxy.preferred_voice_format == "ogg_opus"
            assert proxy.admin_status()["supports_voice"] is True
            assert svc.platform_voice_capability(
                Message(
                    platform="telegram",
                    bot_id="tg-bot",
                    event_id="e1",
                    user_id="u1",
                    kind="private",
                    conversation_id="u1",
                    text="hi",
                    addressed=True,
                    created_at=time.time(),
                )
            ) == (True, "ogg_opus")

            with pytest.raises(Rejected, match="plugin_invalid_method"):
                await proxy.call_custom_rpc("delivery.send_voice", {})

            async with httpx.AsyncClient(transport=httpx.MockTransport(tts_handler)) as tts_http:
                svc.voice_reply = VoiceReplyClient(s, http=tts_http)
                tg_msg = Message(
                    platform="telegram",
                    bot_id="tg-bot",
                    event_id="tg-e1",
                    user_id="tg-u1",
                    kind="private",
                    conversation_id="tg-u1",
                    text="用语音说一句",
                    addressed=True,
                    created_at=time.time(),
                )
                await svc.store.enqueue_reply(
                    "job-tg-voice",
                    tg_msg,
                    "",
                    session_epoch=0,
                    actions={
                        "abstain": False,
                        "bubbles": [""],
                        "anchor": {"reply_id": None, "mentions": []},
                        "sticker": {"label": None, "timing": "after_text"},
                        "image_digest": None,
                        "watermark": 0,
                        "public": False,
                        "voice_text": "哈喽Telegram喵~",
                        "explicit_voice": True,
                    },
                )
                assert await deliver_actions(svc, await svc.store.claim_reply())
                assert len(delivered_voices) == 1
                assert delivered_voices[0]["format"] == "ogg_opus"
                assert delivered_voices[0]["mime_type"] == "audio/ogg"
                assert delivered_voices[0]["duration_ms"] == 1350
                assert base64.b64decode(delivered_voices[0]["audio_b64"]) == FAKE_OGG

                # Oversized voice payload (>16KiB frame) falls back to delivery.send text cleanly
                tts_payload["audio"] = b"OggS" + b"\x00" * 18000
                await svc.store.enqueue_reply(
                    "job-tg-oversize",
                    tg_msg,
                    "",
                    session_epoch=0,
                    actions={
                        "abstain": False,
                        "bubbles": [""],
                        "anchor": {"reply_id": None, "mentions": []},
                        "sticker": {"label": None, "timing": "after_text"},
                        "image_digest": None,
                        "watermark": 0,
                        "public": False,
                        "voice_text": "太大了改发文字",
                        "explicit_voice": True,
                    },
                )
                assert await deliver_actions(svc, await svc.store.claim_reply())
                assert len(delivered_voices) == 1
                assert len(delivered_texts) == 1
                assert delivered_texts[0]["text"] == "太大了改发文字"
        finally:
            await client.stop()
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)


def test_base_module_defaults():
    assert BaseModule.supports_voice is False
    assert BaseModule.preferred_voice_format == "ogg_opus"


async def test_service_handle_message_to_deliver_once_voice_e2e(settings, store, message):
    s = _voice_settings(settings)
    wire = _OneBotWire()

    class _JsonVoiceGateway(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", dict(payload), subject, {}))
            return _action_json(bubbles=[""], voice_text="晚安喵~")

    def tts_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=FAKE_MP3,
            headers={
                "content-type": "audio/mpeg",
                "x-audio-format": "mp3",
                "x-audio-duration-ms": "950",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(tts_handler)) as http:
        svc = Service(s, store, _JsonVoiceGateway(), FakeBili(), Sender(s, http, wire), http=http)
        try:
            assert await svc.accept(message) == "accepted"
            job = await store.claim()
            assert job is not None
            await svc.process(job)
            assert await svc.deliver_once()
            gen_payload = next(c[1] for c in svc.ai.calls if c[0] == "generate")
            assert gen_payload.get("voice_reply_available") is True
            assert wire.calls[-1][1]["message"][0]["type"] == "record"
            snap = await svc.sessions.snapshot(message)
            assert snap["recent_messages"][-1]["text"] == "[语音] 晚安喵~"
        finally:
            await svc.stop()
