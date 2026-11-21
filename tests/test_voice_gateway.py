"""Offline unit, protocol, HTTP/WS auth, and E2E ASGI tests for hakimi-voice-gateway."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import io
import json
import os
import socket
import struct
import sys
import wave
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect

from hakimi.app import create_app
from tests.test_voice_companion import COMPANION_PATH, COMPANION_TOKEN, _StreamMockGateway
from voice_client.audio import pcm16_to_wav_bytes
from voice_client.config import VoiceClientConfig
from voice_client.gateway_ws import VoiceGatewayWsClient
from voice_gateway import (
    ProtocolError,
    VoiceGatewayConfig,
    VoiceGatewaySession,
    build_error_frame,
    create_gateway_app,
    decode_json_frame,
    load_gateway_config,
    normalize_input_to_wav,
    output_format_meta,
    redact_secrets,
    transcode_audio_stream,
    validate_adapter_id,
    validate_client_id,
    validate_id,
)
from voice_gateway.protocol import safe_error_detail

GATEWAY_TOKEN = "voice-gateway-test-token-32chars-min"
GATEWAY_PATH = "/ws/voice/0123456789abcdef0123456789abcdef"


def _make_wav(amplitude: int = 1200, *, samples: int = 160, sample_rate: int = 16000) -> bytes:
    pcm = struct.pack("<" + "h" * samples, *([amplitude] * samples))
    return pcm16_to_wav_bytes(pcm, sample_rate=sample_rate, channels=1)


def _gateway_config(**overrides: Any) -> VoiceGatewayConfig:
    defaults: dict[str, Any] = {
        "ws_path": GATEWAY_PATH,
        "gateway_token": GATEWAY_TOKEN,
        "default_output_format": "mp3",
        "ffmpeg_bin": "/nonexistent-ffmpeg",
        "ws_url": "wss://127.0.0.1:12141" + COMPANION_PATH,
        "ws_token": COMPANION_TOKEN,
        "adapter_id": "voice-gateway",
        "client_id": "default",
        "session_mode": "owner_private",
        "game_title": "Minecraft",
        "stt_provider": "siliconflow",
        "tts_provider": "siliconflow",
        "siliconflow_api_key": "sk-test-siliconflow-secret-999",
        "fish_api_key": "fish-test-secret-888",
        "openai_stt_api_key": "sk-test-openai-secret-777",
        "tts_buffer_mode": "turn",
        "tts_streaming": True,
        "ping_interval_seconds": 0.0,
    }
    defaults.update(overrides)
    return VoiceGatewayConfig(**defaults)


def _ws_headers(**extra: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {GATEWAY_TOKEN}",
        "X-Voice-Adapter": "mac-voice",
        "X-Voice-Client-Id": "boiser-m4",
        **extra,
    }


def _write_fake_ffmpeg_script(path: Path) -> str:
    """Create an executable Python shim emulating ffmpeg stdin->stdout transcode."""
    path.write_text(
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
                "        wf.setframerate(16000)",
                "        wf.writeframes(b'\\x01\\x00' * 160)",
                "    sys.stdout.buffer.write(buf.getvalue())",
                "elif out_fmt == 's16le':",
                "    sys.stdout.buffer.write(b'PCM:' + data)",
                "else:",
                "    sys.stdout.buffer.write(b'TRANSCODED:' + data)",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(path, 0o755)
    return str(path)


class _FakeSTT:
    def __init__(self, transcript: str = "你好哈基米") -> None:
        self.transcript = transcript
        self.calls: list[bytes] = []

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        _ = filename
        self.calls.append(wav_bytes)
        return self.transcript

    async def aclose(self) -> None:
        return None


class _FakeTTS:
    def __init__(
        self,
        *,
        chunks: list[bytes] | None = None,
        response_format: str = "mp3",
        delay_after_first: float = 0.0,
    ) -> None:
        self.chunks = chunks if chunks is not None else [b"AUDIO-PART-1-", b"AUDIO-PART-2"]
        self.response_format = response_format
        self.delay_after_first = delay_after_first
        self.calls: list[str] = []
        self.cancelled = False

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        self.calls.append(text)
        return b"".join(self.chunks), self.response_format

    async def synthesize_stream(self, text: str):
        self.calls.append(text)
        try:
            for idx, chunk in enumerate(self.chunks):
                yield chunk
                if idx == 0 and self.delay_after_first > 0:
                    await asyncio.sleep(self.delay_after_first)
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def aclose(self) -> None:
        return None


class _CompanionWsBridge:
    """In-memory bridge between CompanionWsClient (text) and VoiceCompanion.serve."""

    _EOF = object()

    def __init__(self, voice_companion: Any, *, track_calls: list[int] | None = None) -> None:
        self._voice_companion = voice_companion
        self._track_calls = track_calls
        self.serve_tasks: list[asyncio.Task[None]] = []

    def connect_factory(self) -> Any:
        if self._track_calls is not None:
            self._track_calls.append(1)

        eof = self._EOF
        c2s: asyncio.Queue[str | object] = asyncio.Queue()
        s2c: asyncio.Queue[str | object] = asyncio.Queue()
        closed = False

        async def server_receive() -> str:
            item = await c2s.get()
            if item is eof:
                raise RuntimeError("eof")
            return str(item)

        async def server_send(raw: str) -> None:
            await s2c.put(raw)

        async def server_close(code: int = 1000) -> None:
            nonlocal closed
            _ = code
            closed = True
            await s2c.put(eof)
            await c2s.put(eof)

        class _ClientEndpoint:
            async def send(self, raw: str) -> None:
                if closed:
                    raise RuntimeError("closed")
                await c2s.put(raw)

            async def recv(self) -> str:
                item = await s2c.get()
                if item is eof:
                    raise RuntimeError("eof")
                return str(item)

            async def close(self) -> None:
                nonlocal closed
                closed = True
                await c2s.put(eof)
                await s2c.put(eof)

        task = asyncio.create_task(
            self._voice_companion.serve(
                server_receive,
                server_send,
                server_close,
                source_ip="127.0.0.1",
                adapter_id="voice-gateway",
                client_id="gateway-e2e",
            )
        )
        self.serve_tasks.append(task)
        return _ClientEndpoint()


class _SessionWireDouble:
    """Capture text JSON and raw binary frames emitted by VoiceGatewaySession."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []
        self.closed_codes: list[int] = []
        self.updated = asyncio.Event()

    async def send_text(self, raw: str) -> None:
        self.events.append(("json", json.loads(raw)))
        self.updated.set()

    async def send_bytes(self, data: bytes) -> None:
        self.events.append(("bytes", bytes(data)))
        self.updated.set()

    async def close_ws(self, code: int = 1000) -> None:
        self.closed_codes.append(code)

    async def wait_until(self, predicate: Any, *, timeout: float = 3.0) -> None:  # noqa: ASYNC109
        async with asyncio.timeout(timeout):
            while not predicate(self.events):
                self.updated.clear()
                if predicate(self.events):
                    return
                await self.updated.wait()


def test_gateway_startup_validation_and_secret_redaction(tmp_path):
    env_file = tmp_path / "gw.env"
    env_file.write_text(
        "\n".join(
            [
                f"VOICE_GATEWAY_TOKEN={GATEWAY_TOKEN}",
                f"VOICE_GATEWAY_WS_PATH={GATEWAY_PATH}",
                "VOICE_GATEWAY_PORT=12145",
                "VOICE_GATEWAY_DEFAULT_OUTPUT_FORMAT=PCM_16K",
                "COMPANION_WS_URL=wss://example.invalid" + COMPANION_PATH,
                f"COMPANION_WS_TOKEN={COMPANION_TOKEN}",
                "SILICONFLOW_API_KEY=sk-sf-super-secret-12345",
                "FISH_API_KEY=fish-super-secret-67890",
                "OPENAI_STT_API_KEY=sk-oai-super-secret-abcde",
                "PING_INTERVAL_SECONDS=15.0",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_gateway_config(env={}, env_file=env_file)
    assert cfg.gateway_token == GATEWAY_TOKEN
    assert cfg.ws_path == GATEWAY_PATH
    assert cfg.default_output_format == "pcm_16k"
    assert cfg.ping_interval_seconds == 15.0
    cfg.require_gateway_auth()

    client_cfg = cfg.to_client_config(
        adapter_id="mac-voice",
        client_id="client-9",
        tts_format_override="pcm_48k_stereo",
    )
    assert client_cfg.adapter_id == "mac-voice"
    assert client_cfg.client_id == "client-9"
    assert client_cfg.tts_format == "wav"

    # Reject missing or <32-char VOICE_GATEWAY_TOKEN at startup and in validation
    for bad_token in ("", "short-token", "x" * 31):
        bad_cfg = _gateway_config(gateway_token=bad_token)
        with pytest.raises(ValueError, match="32 characters"):
            bad_cfg.require_gateway_auth()
        bad_app = create_gateway_app(
            bad_cfg,
            stt_provider=_FakeSTT(),
            tts_provider=_FakeTTS(),
        )
        with pytest.raises(ValueError, match="32 characters"):
            with TestClient(bad_app):
                pass

    with pytest.raises(ValueError, match="VOICE_GATEWAY_WS_PATH"):
        load_gateway_config(env={"VOICE_GATEWAY_WS_PATH": "no-slash"}, env_file=None)
    with pytest.raises(ValueError, match="VOICE_GATEWAY_WS_PATH"):
        _gateway_config(ws_path="/ws/voice?bad=1").require_gateway_auth()

    # Secret redaction scrubs tokens, keys, Bearer, and WS paths
    leaky = (
        f"auth Bearer {GATEWAY_TOKEN} path={GATEWAY_PATH} "
        f"upstream=wss://example.invalid{COMPANION_PATH} "
        f"upstream_tok={COMPANION_TOKEN} "
        "sf=sk-sf-super-secret-12345 fish=fish-super-secret-67890 "
        "oai=sk-oai-super-secret-abcde"
    )
    scrubbed = redact_secrets(leaky, cfg)
    for secret in (
        GATEWAY_TOKEN,
        GATEWAY_PATH,
        COMPANION_PATH,
        COMPANION_TOKEN,
        "sk-sf-super-secret-12345",
        "fish-super-secret-67890",
        "sk-oai-super-secret-abcde",
    ):
        assert secret not in scrubbed
    assert "[REDACTED]" in scrubbed
    assert "[REDACTED_PATH]" in scrubbed

    err_frame = build_error_frame("tts_failed", leaky, config=cfg, turn_id="t-1")
    dumped = json.dumps(err_frame)
    assert GATEWAY_TOKEN not in dumped
    assert GATEWAY_PATH not in dumped
    assert COMPANION_TOKEN not in dumped
    assert "fish-super-secret-67890" not in dumped
    assert GATEWAY_TOKEN not in safe_error_detail(leaky, cfg)

    # Protocol validators
    assert validate_adapter_id(None) == "voice-client"
    assert validate_client_id(None) == "default"
    assert validate_id("t-abc_1.2", field_name="turn_id") == "t-abc_1.2"
    with pytest.raises(ProtocolError):
        decode_json_frame('{"type": "ping", "x": NaN}')


def test_gateway_ws_handshake_auth_rejects_invalid_and_accepts_valid():
    cfg = _gateway_config()
    app = create_gateway_app(cfg, stt_provider=_FakeSTT(), tts_provider=_FakeTTS())
    with TestClient(app) as client:
        bad_cases = (
            (GATEWAY_PATH, {"X-Voice-Adapter": "mac-voice"}),  # missing Bearer
            (GATEWAY_PATH, _ws_headers(Authorization="Bearer wrong-gateway-token-value")),
            ("/ws/voice", _ws_headers()),  # wrong path
            (GATEWAY_PATH + "?tok=x", _ws_headers()),  # query params forbidden
            (GATEWAY_PATH, _ws_headers(**{"X-Voice-Adapter": "BAD ADAPTER!"})),
            (GATEWAY_PATH, _ws_headers(**{"X-Voice-Client-Id": "bad/client"})),
        )
        for path, headers in bad_cases:
            with pytest.raises(WebSocketDisconnect) as exc_info:
                with client.websocket_connect(path, headers=headers):
                    pytest.fail(f"Unauthenticated WS accepted: path={path}")
            assert exc_info.value.code == 1008

        # Valid Bearer without optional headers uses defaults
        with client.websocket_connect(
            GATEWAY_PATH, headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"}
        ) as ws:
            ws.send_json({"type": "ping", "req_id": "p-def"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"
            assert pong["req_id"] == "p-def"
            assert pong["upstream_ms"] is None

        # Valid Bearer + X-Voice-Adapter / X-Voice-Client-Id headers
        with client.websocket_connect(GATEWAY_PATH, headers=_ws_headers()) as ws:
            ws.send_json({"type": "ping", "req_id": "p-hdr"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"
            assert pong["req_id"] == "p-hdr"


def test_gateway_internal_http_endpoints(tmp_path):
    fake_ffmpeg = _write_fake_ffmpeg_script(tmp_path / "fake_ffmpeg.py")
    cfg = _gateway_config(max_audio_bytes=4096, ffmpeg_bin=fake_ffmpeg)
    stt = _FakeSTT("内部识别结果")
    tts = _FakeTTS(chunks=[b"MP3-A-", b"MP3-B"], response_format="mp3")
    app = create_gateway_app(cfg, stt_provider=stt, tts_provider=tts)

    wav = _make_wav()
    with TestClient(app) as client:
        # GET /healthz
        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.json() == {
            "status": "ok",
            "stt_provider": "siliconflow",
            "tts_provider": "siliconflow",
        }
        assert GATEWAY_TOKEN not in health.text

        # POST /v1/stt auth failures (401 missing, 403 wrong)
        r_no_auth = client.post("/v1/stt", files={"file": ("clip.wav", wav, "audio/wav")})
        assert r_no_auth.status_code == 401
        r_bad_auth = client.post(
            "/v1/stt",
            headers={"Authorization": "Bearer wrong-token"},
            files={"file": ("clip.wav", wav, "audio/wav")},
        )
        assert r_bad_auth.status_code == 403

        # POST /v1/stt missing file or invalid multipart -> 400
        r_missing_file = client.post(
            "/v1/stt",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            files={"other": ("clip.wav", wav, "audio/wav")},
        )
        assert r_missing_file.status_code == 400

        # POST /v1/stt valid WAV transcription
        r_ok = client.post(
            "/v1/stt",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            files={"file": ("clip.wav", wav, "audio/wav")},
            data={"format": "wav", "sample_rate": "16000", "channels": "1"},
        )
        assert r_ok.status_code == 200
        body = r_ok.json()
        assert body["text"] == "内部识别结果"
        assert body["provider"] == "siliconflow"
        assert body["stt_ms"] >= 0.0

        # POST /v1/stt size overflow -> 413
        r_big = client.post(
            "/v1/stt",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            files={"file": ("big.wav", b"RIFF" + b"\x00" * 5000, "audio/wav")},
        )
        assert r_big.status_code == 413

        # POST /v1/tts auth fail, invalid format 400, streaming synthesis
        r_tts_no = client.post("/v1/tts", json={"text": "你好"})
        assert r_tts_no.status_code == 401
        r_tts_wrong = client.post(
            "/v1/tts",
            headers={"Authorization": "Bearer wrong-token"},
            json={"text": "你好"},
        )
        assert r_tts_wrong.status_code == 403

        r_tts_bad_fmt = client.post(
            "/v1/tts",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            json={"text": "你好", "output_format": "flac", "stream": True},
        )
        assert r_tts_bad_fmt.status_code == 400

        with client.stream(
            "POST",
            "/v1/tts",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            json={"text": "你好流式", "output_format": "mp3", "stream": True},
        ) as r_tts_stream:
            assert r_tts_stream.status_code == 200
            assert r_tts_stream.headers["x-audio-format"] == "mp3"
            assert r_tts_stream.headers["x-sample-rate"] == "44100"
            assert r_tts_stream.headers["x-channels"] == "1"
            streamed_bytes = b"".join(r_tts_stream.iter_bytes())
        assert streamed_bytes == b"MP3-A-MP3-B"

        # Non-streaming transcode via fake ffmpeg shim
        r_tts_pcm = client.post(
            "/v1/tts",
            headers={"Authorization": f"Bearer {GATEWAY_TOKEN}"},
            json={"text": "转码PCM", "output_format": "pcm_16k", "stream": False},
        )
        assert r_tts_pcm.status_code == 200
        assert r_tts_pcm.headers["x-audio-format"] == "pcm_16k"
        assert r_tts_pcm.content == b"PCM:MP3-A-MP3-B"


async def test_gateway_audio_codec_normalization_and_transcode(tmp_path):
    fake_ffmpeg = _write_fake_ffmpeg_script(tmp_path / "fake_ffmpeg.py")
    wav = _make_wav()

    assert output_format_meta("pcm_48k_stereo") == ("audio/L16;rate=48000;channels=2", 48000, 2)
    assert await normalize_input_to_wav(wav, "wav") == wav
    with pytest.raises(ValueError, match="RIFF"):
        await normalize_input_to_wav(b"not-a-wav", "wav")

    pcm_16k_wav = await normalize_input_to_wav(b"\x01\x00" * 160, "pcm_16k")
    with wave.open(io.BytesIO(pcm_16k_wav), "rb") as wf:
        assert (wf.getnchannels(), wf.getframerate()) == (1, 16000)

    # Graceful fallback when ffmpeg binary is missing for pcm_48k_stereo
    fallback_wav = await normalize_input_to_wav(
        b"\x01\x00\x02\x00" * 120,
        "pcm_48k_stereo",
        ffmpeg_bin="/nonexistent-ffmpeg",
    )
    with wave.open(io.BytesIO(fallback_wav), "rb") as wf:
        assert (wf.getnchannels(), wf.getframerate()) == (2, 48000)

    # Opus/ogg decode & transcode_audio_stream using fake ffmpeg
    decoded_opus = await normalize_input_to_wav(b"OggS-fake-opus", "opus", ffmpeg_bin=fake_ffmpeg)
    assert decoded_opus.startswith(b"RIFF")
    transcoded = [
        chunk
        async for chunk in transcode_audio_stream(
            [b"A", b"B"],
            src_fmt="mp3",
            dst_fmt="pcm_16k",
            ffmpeg_bin=fake_ffmpeg,
        )
    ]
    assert b"".join(transcoded) == b"PCM:AB"


async def test_gateway_ws_voice_turn_pcm48k_empty_speech_and_chunk_mode(settings):
    gw = _StreamMockGateway(deltas=["第一句！", "第二句喵。"])
    app = create_app(
        settings.model_copy(
            update={
                "owner_qq_id": "20000",
                "companion_ws_enabled": True,
                "companion_ws_token": SecretStr(COMPANION_TOKEN),
                "companion_ws_path": COMPANION_PATH,
            }
        ),
        gateway_factory=lambda *_: gw,
        start_workers=False,
    )

    async with app.router.lifespan_context(app):
        svc = app.state.service
        bridge = _CompanionWsBridge(svc.voice_companion)
        wire = _SessionWireDouble()
        stt = _FakeSTT("快看这波操作")
        tts = _FakeTTS(chunks=[b"AA", b"BB"], response_format="mp3")
        cfg = _gateway_config(ffmpeg_bin="/nonexistent-ffmpeg")
        session = VoiceGatewaySession(
            cfg,
            stt_provider=stt,
            tts_provider=tts,
            send_text=wire.send_text,
            send_bytes=wire.send_bytes,
            close_ws=wire.close_ws,
            upstream_connect_factory=bridge.connect_factory,
        )
        await session.start()
        try:
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "session.start",
                        "mode": "owner_private",
                        "output_format": "mp3",
                        "tts_buffer_mode": "turn",
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(k == "json" and v.get("type") == "session.ready" for k, v in evs)
            )
            wire.events.clear()

            # 1. Full audio.turn (WAV) -> stt.final -> turn.start -> text.chunk -> tts -> turn.end
            wav_b64 = base64.b64encode(_make_wav()).decode("ascii")
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.turn",
                        "turn_id": "t-wav",
                        "format": "wav",
                        "sample_rate": 16000,
                        "channels": 1,
                        "audio_b64": wav_b64,
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-wav"
                    for k, v in evs
                )
            )

            types_seq = [v["type"] if k == "json" else "binary" for k, v in wire.events]
            assert types_seq == [
                "stt.final",
                "turn.start",
                "text.chunk",
                "text.chunk",
                "tts.start",
                "binary",
                "binary",
                "tts.end",
                "turn.end",
            ]
            assert types_seq.index("tts.end") < types_seq.index("turn.end")
            turn_end_frame = wire.events[-1][1]
            assert turn_end_frame["status"] == "spoken"
            assert {"stt_ms", "llm_ms", "tts_ttfb_ms"} <= set(turn_end_frame["metrics"])
            assert tts.calls == ["第一句!第二句喵。"]

            # 2. audio.turn with pcm_48k_stereo (fallback to WAV without ffmpeg)
            wire.events.clear()
            pcm48_b64 = base64.b64encode(b"\x01\x00\x02\x00" * 120).decode("ascii")
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.turn",
                        "turn_id": "t-pcm48",
                        "format": "pcm_48k_stereo",
                        "sample_rate": 48000,
                        "channels": 2,
                        "audio_b64": pcm48_b64,
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-pcm48"
                    for k, v in evs
                )
            )
            assert wire.events[-1][1]["status"] == "spoken"
            with wave.open(io.BytesIO(stt.calls[-1]), "rb") as wf:
                assert (wf.getnchannels(), wf.getframerate()) == (2, 48000)

            # 3. empty_speech emits stt.final + turn.end(status="skipped") without LLM call
            wire.events.clear()
            prev_llm_calls = len(gw.calls)
            stt.transcript = "   "
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.turn",
                        "turn_id": "t-empty",
                        "format": "wav",
                        "audio_b64": wav_b64,
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-empty"
                    for k, v in evs
                )
            )
            assert [v["type"] for k, v in wire.events if k == "json"] == ["stt.final", "turn.end"]
            assert wire.events[-1][1]["status"] == "skipped"
            assert wire.events[-1][1]["reason"] == "empty_speech"
            assert len(gw.calls) == prev_llm_calls

            # 4. chunk buffer mode synthesizes per chunk without interleaving, turn.end after last
            wire.events.clear()
            tts.calls.clear()
            stt.transcript = "分句模式测试"
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "session.start",
                        "mode": "owner_private",
                        "output_format": "mp3",
                        "tts_buffer_mode": "chunk",
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(k == "json" and v.get("type") == "session.ready" for k, v in evs)
            )
            wire.events.clear()
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.turn",
                        "turn_id": "t-chunk",
                        "format": "wav",
                        "audio_b64": wav_b64,
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-chunk"
                    for k, v in evs
                )
            )
            assert tts.calls == ["第一句!", "第二句喵。"]
            chunk_tts_ends = [
                idx
                for idx, (k, v) in enumerate(wire.events)
                if k == "json" and v.get("type") == "tts.end"
            ]
            chunk_turn_end = next(
                idx
                for idx, (k, v) in enumerate(wire.events)
                if k == "json" and v.get("type") == "turn.end"
            )
            assert len(chunk_tts_ends) == 2
            assert chunk_tts_ends[-1] < chunk_turn_end

            await session.handle_text_frame(json.dumps({"type": "session.stop"}))
        finally:
            await session.close()
            await asyncio.gather(*bridge.serve_tasks, return_exceptions=True)


async def test_gateway_ws_streaming_binary_upload_and_max_audio_overflow(settings):
    gw = _StreamMockGateway(deltas=["流式上传收到！"])
    app = create_app(
        settings.model_copy(
            update={
                "owner_qq_id": "20000",
                "companion_ws_enabled": True,
                "companion_ws_token": SecretStr(COMPANION_TOKEN),
                "companion_ws_path": COMPANION_PATH,
            }
        ),
        gateway_factory=lambda *_: gw,
        start_workers=False,
    )

    async with app.router.lifespan_context(app):
        bridge = _CompanionWsBridge(app.state.service.voice_companion)
        wire = _SessionWireDouble()
        stt = _FakeSTT("二进制语音流")
        tts = _FakeTTS(chunks=[b"OK-BIN"], response_format="mp3")
        cfg = _gateway_config(max_audio_bytes=1024)
        session = VoiceGatewaySession(
            cfg,
            stt_provider=stt,
            tts_provider=tts,
            send_text=wire.send_text,
            send_bytes=wire.send_bytes,
            close_ws=wire.close_ws,
            upstream_connect_factory=bridge.connect_factory,
        )
        await session.start()
        try:
            await session.handle_text_frame(json.dumps({"type": "session.start"}))
            await wire.wait_until(
                lambda evs: any(k == "json" and v.get("type") == "session.ready" for k, v in evs)
            )
            wire.events.clear()

            # Valid streaming binary upload (start -> binary frames -> end)
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.stream.start",
                        "stream_id": "s-ok",
                        "turn_id": "t-bin-ok",
                        "format": "pcm_16k",
                        "sample_rate": 16000,
                        "channels": 1,
                    }
                )
            )
            await session.handle_binary_frame(b"\x01\x00" * 100)
            await session.handle_binary_frame(b"\x02\x00" * 100)
            await session.handle_text_frame(
                json.dumps({"type": "audio.stream.end", "stream_id": "s-ok", "turn_id": "t-bin-ok"})
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-bin-ok"
                    for k, v in evs
                )
            )
            assert wire.events[-1][1]["status"] == "spoken"
            assert len(stt.calls) == 1

            # Overflow rejection when binary frames exceed max_audio_bytes=1024
            wire.events.clear()
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "audio.stream.start",
                        "stream_id": "s-big",
                        "turn_id": "t-bin-big",
                        "format": "pcm_16k",
                    }
                )
            )
            await session.handle_binary_frame(b"A" * 600)
            await session.handle_binary_frame(b"B" * 600)
            await session.handle_text_frame(
                json.dumps(
                    {"type": "audio.stream.end", "stream_id": "s-big", "turn_id": "t-bin-big"}
                )
            )
            err_events = [v for k, v in wire.events if k == "json" and v.get("type") == "error"]
            assert len(err_events) == 1
            assert err_events[0]["reason"] == "audio_too_large"
            assert err_events[0]["turn_id"] == "t-bin-big"
            assert err_events[0]["stream_id"] == "s-big"
            assert len(stt.calls) == 1
            assert len(gw.calls) == 1

            await session.handle_text_frame(json.dumps({"type": "session.stop"}))
        finally:
            await session.close()
            await asyncio.gather(*bridge.serve_tasks, return_exceptions=True)


async def test_gateway_ws_text_turn_cancel_and_room_public_speakers(settings):
    gw = _StreamMockGateway(deltas=["多人房回答！"])
    app = create_app(
        settings.model_copy(
            update={
                "owner_qq_id": "20000",
                "companion_ws_enabled": True,
                "companion_ws_token": SecretStr(COMPANION_TOKEN),
                "companion_ws_path": COMPANION_PATH,
            }
        ),
        gateway_factory=lambda *_: gw,
        start_workers=False,
    )

    async with app.router.lifespan_context(app):
        bridge = _CompanionWsBridge(app.state.service.voice_companion)
        wire = _SessionWireDouble()
        stt = _FakeSTT("unused")
        tts = _FakeTTS(chunks=[b"FIRST-", b"SECOND"], response_format="mp3")
        cfg = _gateway_config()
        session = VoiceGatewaySession(
            cfg,
            stt_provider=stt,
            tts_provider=tts,
            send_text=wire.send_text,
            send_bytes=wire.send_bytes,
            close_ws=wire.close_ws,
            upstream_connect_factory=bridge.connect_factory,
        )
        await session.start()
        try:
            await session.handle_text_frame(
                json.dumps({"type": "session.start", "mode": "room_public", "game_title": "Apex"})
            )
            await wire.wait_until(
                lambda evs: any(k == "json" and v.get("type") == "session.ready" for k, v in evs)
            )

            # Multi-speaker propagation in room_public via text.turn
            wire.events.clear()
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "text.turn",
                        "turn_id": "t-pub-1",
                        "text": "这把跳哪",
                        "speaker_id": "player-1",
                        "speaker_name": "路人甲",
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-pub-1"
                    for k, v in evs
                )
            )
            assert stt.calls == []
            assert gw.calls[0][0]["speaker_id"] == "player-1"
            assert gw.calls[0][0]["speaker_name"] == "路人甲"

            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "text.turn",
                        "turn_id": "t-pub-2",
                        "text": "我掩护你",
                        "speaker_id": "player-2",
                        "speaker_name": "队友乙",
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-pub-2"
                    for k, v in evs
                )
            )
            assert gw.calls[1][0]["speaker_id"] == "player-2"
            assert gw.calls[1][0]["speaker_name"] == "队友乙"

            # turn.cancel aborts active TTS and forwards upstream turn.cancel
            wire.events.clear()
            tts.delay_after_first = 5.0
            await session.handle_text_frame(
                json.dumps(
                    {
                        "type": "text.turn",
                        "turn_id": "t-cancel",
                        "text": "说一段长回复",
                        "speaker_id": "player-1",
                        "speaker_name": "路人甲",
                    }
                )
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "tts.start" and v.get("turn_id") == "t-cancel"
                    for k, v in evs
                )
            )
            await session.handle_text_frame(
                json.dumps({"type": "turn.cancel", "turn_id": "t-cancel"})
            )
            await wire.wait_until(
                lambda evs: any(
                    k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-cancel"
                    for k, v in evs
                )
            )
            assert tts.cancelled is True
            tts_end = next(
                v
                for k, v in wire.events
                if k == "json" and v.get("type") == "tts.end" and v.get("turn_id") == "t-cancel"
            )
            turn_end = next(
                v
                for k, v in wire.events
                if k == "json" and v.get("type") == "turn.end" and v.get("turn_id") == "t-cancel"
            )
            assert tts_end["status"] == "cancelled"
            assert turn_end["status"] == "cancelled"
            assert turn_end["reason"] == "barge_in"

            await session.handle_text_frame(json.dumps({"type": "session.stop"}))
        finally:
            await session.close()
            await asyncio.gather(*bridge.serve_tasks, return_exceptions=True)


async def test_gateway_standalone_rpcs_make_zero_upstream_or_llm_calls():
    upstream_connects: list[int] = []

    def fail_if_called() -> Any:
        upstream_connects.append(1)
        raise AssertionError("Standalone RPC must not open upstream /ws/companion")

    wire = _SessionWireDouble()
    stt = _FakeSTT("独立识别结果")
    tts = _FakeTTS(chunks=[b"SOLO-1-", b"SOLO-2"], response_format="mp3")
    cfg = _gateway_config()
    session = VoiceGatewaySession(
        cfg,
        stt_provider=stt,
        tts_provider=tts,
        send_text=wire.send_text,
        send_bytes=wire.send_bytes,
        close_ws=wire.close_ws,
        upstream_connect_factory=fail_if_called,
    )
    await session.start()
    try:
        # 1. Pre-session ping
        await session.handle_text_frame(json.dumps({"type": "ping", "req_id": "p-solo"}))
        await wire.wait_until(
            lambda evs: any(k == "json" and v.get("type") == "pong" for k, v in evs)
        )
        assert wire.events[-1][1]["upstream_ms"] is None

        # 2. Pre-session stt.transcribe
        wav_b64 = base64.b64encode(_make_wav()).decode("ascii")
        await session.handle_text_frame(
            json.dumps(
                {
                    "type": "stt.transcribe",
                    "req_id": "stt-solo",
                    "format": "wav",
                    "audio_b64": wav_b64,
                }
            )
        )
        await wire.wait_until(
            lambda evs: any(k == "json" and v.get("type") == "stt.result" for k, v in evs)
        )
        stt_res = next(v for k, v in wire.events if k == "json" and v.get("type") == "stt.result")
        assert stt_res["req_id"] == "stt-solo"
        assert stt_res["text"] == "独立识别结果"

        # 3. Pre-session tts.synthesize
        wire.events.clear()
        await session.handle_text_frame(
            json.dumps(
                {
                    "type": "tts.synthesize",
                    "req_id": "tts-solo",
                    "text": "单独合成测试",
                    "output_format": "mp3",
                }
            )
        )
        await wire.wait_until(
            lambda evs: any(k == "json" and v.get("type") == "tts.end" for k, v in evs)
        )
        assert [k if k == "bytes" else v["type"] for k, v in wire.events] == [
            "tts.start",
            "bytes",
            "bytes",
            "tts.end",
        ]
        assert wire.events[0][1]["req_id"] == "tts-solo"
        assert wire.events[-1][1]["req_id"] == "tts-solo"
        assert wire.events[-1][1]["status"] == "ok"
        assert session.session_active is False
        assert upstream_connects == []
    finally:
        await session.close()


def _bound_loopback_socket() -> socket.socket:
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    return sock


async def test_e2e_voice_gateway_ws_client_asgi_to_real_voice_companion(settings, monkeypatch):
    monkeypatch.setattr("urllib.request.getproxies", lambda: {})

    gw = _StreamMockGateway(deltas=["端到端测试！", "成功喵!"])
    hakimi_app = create_app(
        settings.model_copy(
            update={
                "owner_qq_id": "20000",
                "companion_ws_enabled": True,
                "companion_ws_token": SecretStr(COMPANION_TOKEN),
                "companion_ws_path": COMPANION_PATH,
            }
        ),
        gateway_factory=lambda *_: gw,
        start_workers=False,
    )

    async with hakimi_app.router.lifespan_context(hakimi_app):
        svc = hakimi_app.state.service
        upstream_connects: list[int] = []
        bridge = _CompanionWsBridge(svc.voice_companion, track_calls=upstream_connects)
        stt = _FakeSTT("端到端语音")
        tts = _FakeTTS(chunks=[b"E2E-MP3-1-", b"E2E-MP3-2"], response_format="mp3")

        sock = _bound_loopback_socket()
        port = sock.getsockname()[1]
        gw_cfg = _gateway_config(port=port)
        gw_app = create_gateway_app(
            gw_cfg,
            stt_provider=stt,
            tts_provider=tts,
            upstream_connect_factory=bridge.connect_factory,
        )
        server = uvicorn.Server(
            uvicorn.Config(
                gw_app,
                host="127.0.0.1",
                port=port,
                log_level="critical",
                access_log=False,
                proxy_headers=False,
                ws="websockets-sansio",
                ws_max_size=gw_cfg.max_ws_frame_bytes,
                ws_per_message_deflate=False,
            )
        )
        server_task = asyncio.create_task(server.serve(sockets=[sock]))

        client_events: list[tuple[str, Any]] = []
        turn_started_events: dict[str, asyncio.Event] = {}
        turn_done_events: dict[str, asyncio.Event] = {}
        turn_end_frames: dict[str, dict[str, Any]] = {}

        async def on_stt_final(frame: dict[str, Any]) -> None:
            client_events.append(("stt.final", frame))

        async def on_turn_start(frame: dict[str, Any]) -> None:
            client_events.append(("turn.start", frame))
            tid = str(frame.get("turn_id") or "")
            turn_started_events.setdefault(tid, asyncio.Event()).set()

        async def on_text_chunk(frame: dict[str, Any]) -> None:
            client_events.append(("text.chunk", frame))

        async def on_tts_start(frame: dict[str, Any]) -> None:
            client_events.append(("tts.start", frame))

        async def on_tts_chunk(turn_id: str, seq: int, fmt: str, chunk: bytes) -> None:
            client_events.append(("tts.chunk", (turn_id, seq, fmt, chunk)))

        async def on_tts_end(frame: dict[str, Any]) -> None:
            client_events.append(("tts.end", frame))

        async def on_turn_end(frame: dict[str, Any]) -> None:
            client_events.append(("turn.end", frame))
            tid = str(frame.get("turn_id") or "")
            turn_end_frames[tid] = frame
            turn_done_events.setdefault(tid, asyncio.Event()).set()

        client_cfg = VoiceClientConfig(
            gateway_url=f"ws://127.0.0.1:{port}{GATEWAY_PATH}",
            gateway_token=GATEWAY_TOKEN,
            gateway_output_format="mp3",
            adapter_id="mac-voice",
            client_id="boiser-m4",
            session_mode="owner_private",
            game_title="终末地",
            ping_interval_seconds=0.0,
        )
        client = VoiceGatewayWsClient(
            client_cfg,
            on_stt_final=on_stt_final,
            on_turn_start=on_turn_start,
            on_text_chunk=on_text_chunk,
            on_tts_start=on_tts_start,
            on_tts_chunk=on_tts_chunk,
            on_tts_end=on_tts_end,
            on_turn_end=on_turn_end,
        )

        try:
            async with asyncio.timeout(5.0):
                while not server.started:
                    if server_task.done():
                        await server_task
                    await asyncio.sleep(0.01)

            await client.connect()

            # Pre-session standalone RPCs make zero upstream / LLM calls
            pre_pong = await client.ping_info(timeout=3.0)
            assert pre_pong["upstream_ms"] is None
            assert await client.transcribe_once(_make_wav(), timeout=3.0) == "端到端语音"
            solo_audio, solo_fmt = await client.synthesize_once("独立合成", timeout=3.0)
            assert solo_audio == b"E2E-MP3-1-E2E-MP3-2"
            assert solo_fmt == "mp3"
            assert upstream_connects == []
            assert gw.calls == []

            # Start full gateway -> VoiceCompanion session
            ready = await client.start_session(timeout=3.0)
            assert ready["mode"] == "owner_private"
            assert len(upstream_connects) == 1
            assert svc.world_state.current.current_module == "voice_companion"

            pong = await client.ping_info(timeout=3.0)
            assert pong["upstream_ms"] is not None

            # Streaming binary upload full voice turn
            turn_done_events["e2e-gw-1"] = asyncio.Event()
            await client.send_audio_turn(
                _make_wav(),
                fmt="wav",
                turn_id="e2e-gw-1",
                stream=True,
                chunk_size=1024,
            )
            await asyncio.wait_for(turn_done_events["e2e-gw-1"].wait(), timeout=3.0)

            ev_names = [name for name, _ in client_events]
            assert ev_names == [
                "stt.final",
                "turn.start",
                "text.chunk",
                "text.chunk",
                "tts.start",
                "tts.chunk",
                "tts.chunk",
                "tts.end",
                "turn.end",
            ]
            assert ev_names.index("tts.end") < ev_names.index("turn.end")
            assert turn_end_frames["e2e-gw-1"]["status"] == "spoken"
            received_audio = b"".join(
                payload[3] for name, payload in client_events if name == "tts.chunk"
            )
            assert received_audio == b"E2E-MP3-1-E2E-MP3-2"

            # Cancel slow turn end-to-end across client -> gateway -> VoiceCompanion
            gw.deltas = ["慢慢说一句。", "第二句不会说完。"]
            gw.delay = 0.25
            turn_started_events["e2e-gw-cancel"] = asyncio.Event()
            turn_done_events["e2e-gw-cancel"] = asyncio.Event()
            await client.send_text_turn("帮我看看前面", turn_id="e2e-gw-cancel")
            await asyncio.wait_for(turn_started_events["e2e-gw-cancel"].wait(), timeout=3.0)
            await client.cancel_turn("e2e-gw-cancel")
            await asyncio.wait_for(turn_done_events["e2e-gw-cancel"].wait(), timeout=3.0)
            assert turn_end_frames["e2e-gw-cancel"]["status"] == "cancelled"

            # Stop session restores VoiceCompanion idle state cleanly
            ended = await client.stop_session(timeout=3.0)
            assert ended["type"] == "session.ended"
            assert svc.world_state.current.current_module == "idle"
        finally:
            await client.close()
            server.should_exit = True
            with contextlib.suppress(Exception):
                await asyncio.wait_for(server_task, timeout=5.0)
            await asyncio.gather(*bridge.serve_tasks, return_exceptions=True)
