"""Tests for voice_gateway ffmpeg concurrency limits, TTS text limits, and memory caps."""

from __future__ import annotations

import asyncio
import json
import os
import struct
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from voice_client.audio import pcm16_to_wav_bytes
from voice_gateway import audio_codec
from voice_gateway.app import create_gateway_app
from voice_gateway.audio_codec import (
    normalize_input_to_wav,
    probe_audio_duration_ms,
    transcode_audio_stream,
)
from voice_gateway.config import (
    DEFAULT_MAX_FFMPEG_CONCURRENCY,
    VoiceGatewayConfig,
    load_gateway_config,
)
from voice_gateway.session import VoiceGatewaySession

GATEWAY_TOKEN = "voice-gateway-limits-token-32chars"
GATEWAY_PATH = "/ws/voice/limits0123456789abcdef01234567"


def _make_wav(amplitude: int = 1000, *, samples: int = 160, sample_rate: int = 16000) -> bytes:
    pcm = struct.pack("<" + "h" * samples, *([amplitude] * samples))
    return pcm16_to_wav_bytes(pcm, sample_rate=sample_rate, channels=1)


def _gateway_config(**overrides: Any) -> VoiceGatewayConfig:
    defaults: dict[str, Any] = {
        "ws_path": GATEWAY_PATH,
        "gateway_token": GATEWAY_TOKEN,
        "default_output_format": "mp3",
        "ffmpeg_bin": "/nonexistent-ffmpeg",
        "stt_provider": "siliconflow",
        "tts_provider": "siliconflow",
        "siliconflow_api_key": "sk-test-limits-secret-123456",
        "tts_streaming": True,
        "ping_interval_seconds": 0.0,
    }
    defaults.update(overrides)
    return VoiceGatewayConfig(**defaults)


def _write_tracked_ffmpeg_and_ffprobe(
    tmp_path: Path,
    *,
    delay_sec: float = 0.06,
    stream_many: bool = False,
) -> tuple[str, Path]:
    """Create executable fake ffmpeg & ffprobe shims that record peak OS-level concurrency."""
    state_file = tmp_path / "concurrency_state.json"
    state_file.write_text(json.dumps({"active": 0, "peak": 0, "total": 0}), encoding="utf-8")

    common_header = [
        "#!" + sys.executable,
        "import fcntl, io, json, sys, time, wave",
        f"STATE = {str(state_file)!r}",
        "def _bump(delta):",
        "    with open(STATE, 'r+', encoding='utf-8') as fh:",
        "        fcntl.flock(fh, fcntl.LOCK_EX)",
        "        data = json.load(fh)",
        "        data['active'] += delta",
        "        if delta > 0:",
        "            data['total'] += 1",
        "            if data['active'] > data['peak']:",
        "                data['peak'] = data['active']",
        "        fh.seek(0)",
        "        json.dump(data, fh)",
        "        fh.truncate()",
        "        fcntl.flock(fh, fcntl.LOCK_UN)",
    ]

    ffmpeg_path = tmp_path / "ffmpeg_fake.py"
    ffmpeg_body = [
        *common_header,
        "_bump(1)",
        "try:",
        "    args = sys.argv[1:]",
        "    out_fmt = ''",
        "    for idx in range(len(args) - 1):",
        "        if args[idx] == '-f':",
        "            out_fmt = args[idx + 1]",
        f"    if {stream_many!r}:",
        "        sys.stdout.buffer.write(b'CHUNK0' * 16)",
        "        sys.stdout.buffer.flush()",
        f"        time.sleep({delay_sec!r})",
        "        sys.stdout.buffer.write(b'CHUNK1' * 16)",
        "        sys.stdout.buffer.flush()",
        "    else:",
        "        data = sys.stdin.buffer.read()",
        f"        time.sleep({delay_sec!r})",
        "        if not data:",
        "            sys.exit(1)",
        "        if out_fmt == 'wav':",
        "            buf = io.BytesIO()",
        "            with wave.open(buf, 'wb') as wf:",
        "                wf.setnchannels(1)",
        "                wf.setsampwidth(2)",
        "                wf.setframerate(16000)",
        "                wf.writeframes(b'\\x01\\x00' * 160)",
        "            sys.stdout.buffer.write(buf.getvalue())",
        "        elif out_fmt == 's16le':",
        "            sys.stdout.buffer.write(b'PCM:' + data)",
        "        else:",
        "            sys.stdout.buffer.write(b'OUT:' + data)",
        "finally:",
        "    _bump(-1)",
    ]
    ffmpeg_path.write_text("\n".join(ffmpeg_body), encoding="utf-8")
    os.chmod(ffmpeg_path, 0o755)

    ffprobe_path = tmp_path / "ffprobe_fake.py"
    ffprobe_body = [
        *common_header,
        "_bump(1)",
        "try:",
        f"    time.sleep({delay_sec!r})",
        "    sys.stdout.write('1.250\\n')",
        "    sys.stdout.flush()",
        "finally:",
        "    _bump(-1)",
    ]
    ffprobe_path.write_text("\n".join(ffprobe_body), encoding="utf-8")
    os.chmod(ffprobe_path, 0o755)

    return str(ffmpeg_path), state_file


class _FakeSTT:
    def __init__(self, text: str = "测试文本") -> None:
        self.text = text
        self.calls: list[bytes] = []

    async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
        _ = filename
        self.calls.append(wav_bytes)
        return self.text

    async def aclose(self) -> None:
        return None


class _FakeTTS:
    def __init__(
        self,
        *,
        chunks: list[bytes] | None = None,
        response_format: str = "mp3",
    ) -> None:
        self.chunks = chunks if chunks is not None else [b"MP3-AAA-", b"MP3-BBB"]
        self.response_format = response_format
        self.calls: list[str] = []
        self.yielded_count = 0
        self.stream_closed = False

    async def synthesize(self, text: str) -> tuple[bytes, str]:
        self.calls.append(text)
        return b"".join(self.chunks), self.response_format

    async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
        self.calls.append(text)
        try:
            for chunk in self.chunks:
                self.yielded_count += 1
                yield chunk
        finally:
            self.stream_closed = True

    async def aclose(self) -> None:
        return None


class _WireCapture:
    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []
        self.updated = asyncio.Event()

    async def send_text(self, raw: str) -> None:
        self.events.append(("json", json.loads(raw)))
        self.updated.set()

    async def send_bytes(self, data: bytes) -> None:
        self.events.append(("bytes", bytes(data)))
        self.updated.set()

    async def wait_until(self, predicate: Any, *, timeout: float = 3.0) -> None:  # noqa: ASYNC109
        async with asyncio.timeout(timeout):
            while not predicate(self.events):
                self.updated.clear()
                if predicate(self.events):
                    return
                await self.updated.wait()


def test_config_limits_env_parsing_and_clamping():
    # Defaults when omitted or blank
    cfg_default = load_gateway_config(
        env={
            "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY": "",
            "VOICE_GATEWAY_MAX_TTS_CHARS": "   ",
            "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES": "",
        }
    )
    assert cfg_default.max_ffmpeg_concurrency == DEFAULT_MAX_FFMPEG_CONCURRENCY == 3
    assert cfg_default.max_tts_chars == 2000
    assert cfg_default.max_tts_audio_bytes == 16_777_216

    # In-range custom values
    cfg_custom = load_gateway_config(
        env={
            "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY": "6",
            "VOICE_GATEWAY_MAX_TTS_CHARS": "750",
            "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES": "1048576",
        }
    )
    assert cfg_custom.max_ffmpeg_concurrency == 6
    assert cfg_custom.max_tts_chars == 750
    assert cfg_custom.max_tts_audio_bytes == 1_048_576

    # Lower-bound clamping
    cfg_low = load_gateway_config(
        env={
            "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY": "0",
            "VOICE_GATEWAY_MAX_TTS_CHARS": "12",
            "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES": "1024",
        }
    )
    assert cfg_low.max_ffmpeg_concurrency == 1
    assert cfg_low.max_tts_chars == 100
    assert cfg_low.max_tts_audio_bytes == 262_144

    # Negative lower-bound clamping
    cfg_neg = load_gateway_config(
        env={
            "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY": "-9",
            "VOICE_GATEWAY_MAX_TTS_CHARS": "-50",
            "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES": "-1",
        }
    )
    assert cfg_neg.max_ffmpeg_concurrency == 1
    assert cfg_neg.max_tts_chars == 100
    assert cfg_neg.max_tts_audio_bytes == 262_144

    # Upper-bound clamping
    cfg_high = load_gateway_config(
        env={
            "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY": "99",
            "VOICE_GATEWAY_MAX_TTS_CHARS": "50000",
            "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES": "200000000",
        }
    )
    assert cfg_high.max_ffmpeg_concurrency == 16
    assert cfg_high.max_tts_chars == 10000
    assert cfg_high.max_tts_audio_bytes == 67_108_864

    # Invalid non-integer values raise ValueError
    for key in (
        "VOICE_GATEWAY_MAX_FFMPEG_CONCURRENCY",
        "VOICE_GATEWAY_MAX_TTS_CHARS",
        "VOICE_GATEWAY_MAX_TTS_AUDIO_BYTES",
    ):
        with pytest.raises(ValueError, match=key):
            load_gateway_config(env={key: "not-an-int"})


async def test_ffmpeg_concurrency_limit_across_all_three_spawn_sites(tmp_path):
    fake_ffmpeg, state_file = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=0.08)
    limit = 2
    sem = asyncio.Semaphore(limit)

    async def run_normalize(idx: int) -> bytes:
        return await normalize_input_to_wav(
            f"OggS-audio-{idx}".encode(),
            "opus",
            ffmpeg_bin=fake_ffmpeg,
            ffmpeg_semaphore=sem,
        )

    async def run_transcode(idx: int) -> bytes:
        chunks = [
            chunk
            async for chunk in transcode_audio_stream(
                [f"mp3-{idx}".encode()],
                src_fmt="mp3",
                dst_fmt="pcm_16k",
                ffmpeg_bin=fake_ffmpeg,
                ffmpeg_semaphore=sem,
            )
        ]
        return b"".join(chunks)

    async def run_probe(idx: int) -> int | None:
        return await probe_audio_duration_ms(
            f"ID3-fake-{idx}".encode(),
            "mp3",
            ffmpeg_bin=fake_ffmpeg,
            ffmpeg_semaphore=sem,
        )

    results = await asyncio.gather(
        run_normalize(1),
        run_transcode(1),
        run_probe(1),
        run_normalize(2),
        run_transcode(2),
        run_probe(2),
    )
    assert results[0].startswith(b"RIFF")
    assert results[1] == b"PCM:mp3-1"
    assert results[2] == 1250
    assert results[3].startswith(b"RIFF")
    assert results[4] == b"PCM:mp3-2"
    assert results[5] == 1250

    stats = json.loads(state_file.read_text(encoding="utf-8"))
    assert stats["total"] == 6
    assert stats["active"] == 0
    assert 1 <= stats["peak"] <= limit
    assert not sem.locked()


async def test_zero_copy_passthrough_and_pure_python_probe_do_not_acquire_semaphore():
    sem = asyncio.Semaphore(1)
    wav = _make_wav(samples=160, sample_rate=16000)

    # Hold the sole semaphore slot; pure-Python / zero-copy operations must not block.
    async with sem:
        assert sem.locked()
        async with asyncio.timeout(1.0):
            passthrough = [
                chunk
                async for chunk in transcode_audio_stream(
                    [b"PART-1", b"PART-2"],
                    src_fmt="mp3",
                    dst_fmt="mp3",
                    ffmpeg_bin="/nonexistent-ffmpeg",
                    ffmpeg_semaphore=sem,
                )
            ]
            assert b"".join(passthrough) == b"PART-1PART-2"
            assert await normalize_input_to_wav(wav, "wav", ffmpeg_semaphore=sem) == wav
            assert await probe_audio_duration_ms(wav, "wav", ffmpeg_semaphore=sem) == 10
            assert (
                await probe_audio_duration_ms(b"\x00" * 32000, "pcm_16k", ffmpeg_semaphore=sem)
                == 1000
            )
            assert (
                await probe_audio_duration_ms(
                    b"\x00" * 192000, "pcm_48k_stereo", ffmpeg_semaphore=sem
                )
                == 1000
            )


async def test_transcode_does_not_hold_slot_while_waiting_for_first_chunk(tmp_path):
    fake_ffmpeg, _ = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=0.02)
    sem = asyncio.Semaphore(1)
    release_first = asyncio.Event()
    upstream_started = asyncio.Event()

    async def slow_upstream() -> AsyncIterator[bytes]:
        upstream_started.set()
        await release_first.wait()
        yield b"AUDIO-DATA"

    gen = transcode_audio_stream(
        slow_upstream(),
        src_fmt="mp3",
        dst_fmt="pcm_16k",
        ffmpeg_bin=fake_ffmpeg,
        ffmpeg_semaphore=sem,
    )
    first_task = asyncio.create_task(anext(gen))
    await upstream_started.wait()
    await asyncio.sleep(0.02)

    # Upstream TTS has not emitted first_chunk yet, so no ffmpeg slot may be held.
    assert not sem.locked()

    release_first.set()
    first_chunk = await first_task
    assert first_chunk.startswith(b"PCM:")
    rest = [chunk async for chunk in gen]
    assert first_chunk + b"".join(rest) == b"PCM:AUDIO-DATA"
    assert not sem.locked()


async def test_semaphore_released_on_aclose_timeout_cancel_and_file_not_found(tmp_path):
    sem = asyncio.Semaphore(1)

    # 1. FileNotFoundError across all 3 spawn sites releases the slot
    fallback = await normalize_input_to_wav(
        b"\x01\x00\x02\x00" * 64,
        "pcm_48k_stereo",
        ffmpeg_bin="/nonexistent-ffmpeg",
        ffmpeg_semaphore=sem,
    )
    assert fallback.startswith(b"RIFF")
    assert not sem.locked()

    with pytest.raises(RuntimeError, match="ffmpeg binary not found"):
        await normalize_input_to_wav(
            b"OggS-fake",
            "opus",
            ffmpeg_bin="/nonexistent-ffmpeg",
            ffmpeg_semaphore=sem,
        )
    assert not sem.locked()

    with pytest.raises(RuntimeError, match="ffmpeg binary not found"):
        async for _ in transcode_audio_stream(
            [b"MP3DATA"],
            src_fmt="mp3",
            dst_fmt="pcm_16k",
            ffmpeg_bin="/nonexistent-ffmpeg",
            ffmpeg_semaphore=sem,
        ):
            pass
    assert not sem.locked()

    assert (
        await probe_audio_duration_ms(
            b"ID3-data",
            "mp3",
            ffmpeg_bin="/nonexistent-ffmpeg",
            ffmpeg_semaphore=sem,
        )
        is None
    )
    assert not sem.locked()

    # 2. Early generator aclose() after first yielded chunk terminates ffmpeg & releases slot
    slow_ffmpeg, _ = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=1.5, stream_many=True)
    upstream_closed = False

    async def slow_src() -> AsyncIterator[bytes]:
        nonlocal upstream_closed
        try:
            yield b"FIRST"
            await asyncio.sleep(5.0)
            yield b"SECOND"
        finally:
            upstream_closed = True

    gen = transcode_audio_stream(
        slow_src(),
        src_fmt="mp3",
        dst_fmt="pcm_16k",
        ffmpeg_bin=slow_ffmpeg,
        chunk_size=32,
        ffmpeg_semaphore=sem,
    )
    first = await anext(gen)
    assert first.startswith(b"CHUNK0")
    assert sem.locked()
    await gen.aclose()
    assert not sem.locked()
    assert upstream_closed is True

    # 3. TimeoutError on normalize_input_to_wav & probe_audio_duration_ms releases slot
    timeout_dir = tmp_path / "timeout_bins"
    timeout_dir.mkdir()
    hang_ffmpeg, _ = _write_tracked_ffmpeg_and_ffprobe(timeout_dir, delay_sec=2.0)

    with pytest.raises(TimeoutError):
        await normalize_input_to_wav(
            b"OggS-slow",
            "opus",
            ffmpeg_bin=hang_ffmpeg,
            timeout=0.08,
            ffmpeg_semaphore=sem,
        )
    assert not sem.locked()

    dur = await probe_audio_duration_ms(
        b"ID3-slow",
        "mp3",
        ffmpeg_bin=hang_ffmpeg,
        timeout=0.08,
        ffmpeg_semaphore=sem,
    )
    assert dur is None
    assert not sem.locked()

    # 4. CancelledError on normalize_input_to_wav & probe_audio_duration_ms releases slot
    norm_task = asyncio.create_task(
        normalize_input_to_wav(
            b"OggS-cancel",
            "opus",
            ffmpeg_bin=hang_ffmpeg,
            timeout=5.0,
            ffmpeg_semaphore=sem,
        )
    )
    await asyncio.sleep(0.08)
    assert sem.locked()
    norm_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await norm_task
    assert not sem.locked()

    probe_task = asyncio.create_task(
        probe_audio_duration_ms(
            b"ID3-cancel",
            "mp3",
            ffmpeg_bin=hang_ffmpeg,
            timeout=5.0,
            ffmpeg_semaphore=sem,
        )
    )
    await asyncio.sleep(0.08)
    assert sem.locked()
    probe_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe_task
    assert not sem.locked()


def test_default_semaphore_is_scoped_per_event_loop():
    """Calling codec helpers without an explicit semaphore across distinct loops never fails."""

    async def _get_default() -> tuple[asyncio.Semaphore, int]:
        sem = audio_codec._resolve_ffmpeg_semaphore(None)
        sem_again = audio_codec._resolve_ffmpeg_semaphore(None)
        assert sem is sem_again
        return sem, sem._value  # noqa: SLF001

    sem1, val1 = asyncio.run(_get_default())
    sem2, val2 = asyncio.run(_get_default())
    assert val1 == DEFAULT_MAX_FFMPEG_CONCURRENCY
    assert val2 == DEFAULT_MAX_FFMPEG_CONCURRENCY
    assert sem1 is not sem2


def test_http_tts_max_chars_enforced_before_provider():
    tts = _FakeTTS(chunks=[b"OK-MP3"], response_format="mp3")
    cfg = _gateway_config(max_tts_chars=10)
    app = create_gateway_app(cfg, stt_provider=_FakeSTT(), tts_provider=tts)
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}

    with TestClient(app) as client:
        # Exceeds max_tts_chars -> 400 and TTS provider never called
        r_over = client.post(
            "/v1/tts",
            headers=auth,
            json={"text": "a" * 11, "output_format": "mp3", "stream": False},
        )
        assert r_over.status_code == 400
        assert r_over.json()["detail"] == "text exceeds max_tts_chars (10)"
        assert tts.calls == []

        # Trimmed length == max_tts_chars -> accepted (200)
        r_exact = client.post(
            "/v1/tts",
            headers=auth,
            json={"text": "   " + "b" * 10 + "  ", "output_format": "mp3", "stream": False},
        )
        assert r_exact.status_code == 200
        assert r_exact.content == b"OK-MP3"
        assert tts.calls == ["b" * 10]


def test_http_tts_non_streaming_audio_cap_and_streaming_passthrough(tmp_path, monkeypatch):
    fake_ffmpeg, _ = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=0.01)
    probe_calls: list[int] = []

    async def _spy_probe(*args: Any, **kwargs: Any) -> int | None:
        probe_calls.append(1)
        return await probe_audio_duration_ms(*args, **kwargs)

    monkeypatch.setattr("voice_gateway.app.probe_audio_duration_ms", _spy_probe)

    # 1. Multi-chunk overflow in non-streaming mode rejects 502 without probing & stops reading
    tts_multi = _FakeTTS(chunks=[b"A" * 10, b"B" * 10, b"C" * 10, b"D" * 10], response_format="mp3")
    cfg = _gateway_config(max_tts_audio_bytes=25, ffmpeg_bin=fake_ffmpeg)
    app = create_gateway_app(cfg, stt_provider=_FakeSTT(), tts_provider=tts_multi)
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}

    with TestClient(app) as client:
        r_over = client.post(
            "/v1/tts",
            headers=auth,
            json={"text": "超长音频", "output_format": "mp3", "stream": False},
        )
        assert r_over.status_code == 502
        assert r_over.json()["detail"] == "TTS audio exceeds max_tts_audio_bytes"
        assert probe_calls == []
        assert tts_multi.yielded_count == 3  # stopped on 3rd chunk (30 > 25), never read 4th
        assert tts_multi.stream_closed is True

        # Streaming mode continues streaming cleanly past max_tts_audio_bytes
        with client.stream(
            "POST",
            "/v1/tts",
            headers=auth,
            json={"text": "流式不截断", "output_format": "mp3", "stream": True},
        ) as r_stream:
            assert r_stream.status_code == 200
            streamed = b"".join(r_stream.iter_bytes())
        assert len(streamed) == 40
        assert streamed == b"A" * 10 + b"B" * 10 + b"C" * 10 + b"D" * 10

    # 2. First chunk alone exceeding max_tts_audio_bytes rejects 502 and releases ffmpeg semaphore
    tts_first = _FakeTTS(chunks=[b"X" * 64, b"Y" * 64], response_format="mp3")
    cfg_first = _gateway_config(
        max_tts_audio_bytes=32,
        max_ffmpeg_concurrency=1,
        ffmpeg_bin=fake_ffmpeg,
    )
    app_first = create_gateway_app(cfg_first, stt_provider=_FakeSTT(), tts_provider=tts_first)
    with TestClient(app_first) as client:
        r_first = client.post(
            "/v1/tts",
            headers=auth,
            json={"text": "首块超限", "output_format": "pcm_16k", "stream": False},
        )
        assert r_first.status_code == 502
        assert r_first.json()["detail"] == "TTS audio exceeds max_tts_audio_bytes"
        assert probe_calls == []
        assert not app_first.state.ffmpeg_semaphore.locked()

    # 3. Exact boundary (total == max_tts_audio_bytes) succeeds and probes duration
    tts_exact = _FakeTTS(chunks=[b"Z" * 16, b"W" * 16], response_format="mp3")
    cfg_exact = _gateway_config(max_tts_audio_bytes=32, ffmpeg_bin=fake_ffmpeg)
    app_exact = create_gateway_app(cfg_exact, stt_provider=_FakeSTT(), tts_provider=tts_exact)
    with TestClient(app_exact) as client:
        r_exact = client.post(
            "/v1/tts",
            headers=auth,
            json={"text": "刚好达标", "output_format": "mp3", "stream": False},
        )
        assert r_exact.status_code == 200
        assert len(r_exact.content) == 32
        assert r_exact.headers["x-audio-duration-ms"] == "1250"
        assert len(probe_calls) == 1


async def test_ws_tts_synthesize_enforces_max_tts_chars_and_wires_semaphore(tmp_path):
    fake_ffmpeg, state_file = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=0.05)
    wire = _WireCapture()
    tts = _FakeTTS(chunks=[b"WS-MP3"], response_format="mp3")
    cfg = _gateway_config(max_tts_chars=8, max_ffmpeg_concurrency=1, ffmpeg_bin=fake_ffmpeg)
    sem = asyncio.Semaphore(1)
    session = VoiceGatewaySession(
        cfg,
        stt_provider=_FakeSTT(),
        tts_provider=tts,
        send_text=wire.send_text,
        send_bytes=wire.send_bytes,
        ffmpeg_semaphore=sem,
    )
    await session.start()
    try:
        # Exceeds max_tts_chars -> error frame, no TTS provider call
        await session.handle_text_frame(
            json.dumps(
                {
                    "type": "tts.synthesize",
                    "req_id": "tts-too-long",
                    "text": "123456789",
                    "output_format": "mp3",
                }
            )
        )
        await wire.wait_until(
            lambda evs: any(k == "json" and v.get("type") == "error" for k, v in evs)
        )
        err = wire.events[-1][1]
        assert err["reason"] == "text_too_long"
        assert err["req_id"] == "tts-too-long"
        assert "max_tts_chars (8)" in err["detail"]
        assert tts.calls == []

        # Exact limit + transcode uses injected semaphore cleanly
        wire.events.clear()
        await session.handle_text_frame(
            json.dumps(
                {
                    "type": "tts.synthesize",
                    "req_id": "tts-ok",
                    "text": "12345678",
                    "output_format": "pcm_16k",
                }
            )
        )
        await wire.wait_until(
            lambda evs: any(k == "json" and v.get("type") == "tts.end" for k, v in evs)
        )
        assert tts.calls == ["12345678"]
        assert not sem.locked()
        stats = json.loads(state_file.read_text(encoding="utf-8"))
        assert stats["total"] == 1
        assert stats["active"] == 0
    finally:
        await session.close()


async def test_app_lifespan_ffmpeg_semaphore_caps_concurrent_http_requests(tmp_path):
    fake_ffmpeg, state_file = _write_tracked_ffmpeg_and_ffprobe(tmp_path, delay_sec=0.08)
    cfg = _gateway_config(max_ffmpeg_concurrency=2, ffmpeg_bin=fake_ffmpeg)
    app = create_gateway_app(
        cfg,
        stt_provider=_FakeSTT("并发测试"),
        tts_provider=_FakeTTS(chunks=[b"MP3-DATA"], response_format="mp3"),
    )
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}

    async with app.router.lifespan_context(app):
        assert isinstance(app.state.ffmpeg_semaphore, asyncio.Semaphore)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://voice-gateway.local",
            headers=auth,
        ) as client:

            async def _stt_req() -> httpx.Response:
                return await client.post(
                    "/v1/stt",
                    files={"file": ("clip.ogg", b"OggS-opus-data", "audio/ogg")},
                    data={"format": "opus"},
                )

            async def _tts_req() -> httpx.Response:
                return await client.post(
                    "/v1/tts",
                    json={"text": "并发转码", "output_format": "pcm_16k", "stream": False},
                )

            resps = await asyncio.gather(
                _stt_req(),
                _tts_req(),
                _stt_req(),
                _tts_req(),
            )
            assert all(r.status_code == 200 for r in resps)

        stats = json.loads(state_file.read_text(encoding="utf-8"))
        assert stats["total"] == 4
        assert stats["active"] == 0
        assert 1 <= stats["peak"] <= 2
        assert not app.state.ffmpeg_semaphore.locked()
