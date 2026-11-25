"""Offline tests for the Mac real-time voice companion client package."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import math
import struct
import subprocess
import sys
import wave
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from tests.test_voice_companion import COMPANION_PATH, COMPANION_TOKEN
from voice_client import (
    AfplayAudioPlayer,
    BreezeBlueTTS,
    EnergyVadDetector,
    FishAudioSTT,
    FishAudioTTS,
    OpenAICompatibleSTT,
    ProviderError,
    SiliconFlowSTT,
    SiliconFlowTTS,
    VoiceClientConfig,
    build_tts_provider,
    list_avfoundation_audio_devices,
    load_config,
    load_dotenv_file,
    pcm16_rms,
    pcm16_to_wav_bytes,
    strip_stt_markup,
)
from voice_client.audio import RawStreamAudioPlayer, _default_ffplay_stream_cmd
from voice_client.cli import check_ws, main, run_interactive_session
from voice_client.companion_ws import CompanionWsClient, CompanionWsError
from voice_client.gateway_ws import VoiceGatewayError, VoiceGatewayWsClient


def _pcm_frame(amplitude: int, *, samples: int = 480) -> bytes:
    """Create one mono 16-bit little-endian PCM frame (30ms @ 16kHz by default)."""
    return struct.pack("<" + "h" * samples, *([amplitude] * samples))


def _silence_frame(*, samples: int = 480) -> bytes:
    return b"\x00\x00" * samples


def test_load_dotenv_file_and_config_validation_clamping(tmp_path):
    missing = tmp_path / "does-not-exist.env"
    assert load_dotenv_file(missing) == {}

    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "# comment line",
                "",
                "export COMPANION_WS_URL='wss://example.invalid/ws/companion/secret-abc'",
                'COMPANION_WS_TOKEN="tok-secret-12345678901234567890123456"',
                "COMPANION_ADAPTER_ID=mac-voice # trailing comment",
                "COMPANION_CLIENT_ID=boiser-m4",
                "COMPANION_MODE=room_public",
                'COMPANION_GAME_TITLE="Minecraft\\nTest"',
                "TTS_SPEED=9.9",
                "TTS_GAIN=-50.0",
                "TTS_FORMAT=WAV",
                "VOICE_MODE=ptt",
                "SILICONFLOW_API_KEY=sk-from-dotenv-secret",
                "BARGE_IN_ENABLED=false",
            ]
        ),
        encoding="utf-8",
    )

    parsed = load_dotenv_file(env_path)
    assert parsed["COMPANION_WS_URL"] == "wss://example.invalid/ws/companion/secret-abc"
    assert parsed["COMPANION_ADAPTER_ID"] == "mac-voice"
    assert parsed["COMPANION_GAME_TITLE"] == "Minecraft\nTest"

    cfg = load_config(
        env={
            "COMPANION_GAME_TITLE": "OverriddenGame",
            "TTS_SPEED": "0.1",
            "TTS_GAIN": "25.0",
            "VAD_THRESHOLD": "0.0",
            "VAD_SILENCE_MS": "10",
            "VAD_MIN_SPEECH_MS": "5",
            "VAD_MAX_SPEECH_MS": "100",
            "VAD_BARGE_IN_MULTIPLIER": "0.5",
        },
        env_file=env_path,
    )
    assert cfg.ws_url == "wss://example.invalid/ws/companion/secret-abc"
    assert cfg.session_mode == "room_public"
    assert cfg.game_title == "OverriddenGame"
    assert cfg.tts_format == "wav"
    assert cfg.voice_mode == "ptt"
    assert cfg.barge_in_enabled is False
    assert cfg.tts_speed == 0.25
    assert cfg.tts_gain == 10.0
    assert cfg.vad_threshold == 0.0005
    assert cfg.vad_silence_ms == 60
    assert cfg.vad_min_speech_ms == 30
    assert cfg.vad_max_speech_ms == 500
    assert cfg.vad_barge_in_multiplier == 1.0

    # High-side clamp check on tts_speed & low-side on tts_gain
    cfg_high = load_config(env={"TTS_SPEED": "8.0", "TTS_GAIN": "-99.0"}, env_file=missing)
    assert cfg_high.tts_speed == 4.0
    assert cfg_high.tts_gain == -10.0

    # Empty keys load fine; require_* enforce per-command requirements
    empty_cfg = load_config(env={}, env_file=missing)
    with pytest.raises(ValueError, match="COMPANION_WS_URL"):
        empty_cfg.require_ws()
    with pytest.raises(ValueError, match="ws://"):
        VoiceClientConfig(ws_url="https://bad", ws_token="x" * 32).require_ws()
    with pytest.raises(ValueError, match="COMPANION_WS_TOKEN"):
        VoiceClientConfig(ws_url="wss://ok.example/ws/companion/1", ws_token="").require_ws()
    with pytest.raises(ValueError, match="SILICONFLOW_API_KEY"):
        empty_cfg.require_stt()
    with pytest.raises(ValueError, match="SILICONFLOW_API_KEY"):
        empty_cfg.require_tts()

    VoiceClientConfig(
        ws_url="wss://ok.example/ws/companion/1",
        ws_token="secret-ws-tok",
        siliconflow_api_key="sk-secret",
    ).require_ws()
    VoiceClientConfig(siliconflow_api_key="sk-secret").require_stt()
    VoiceClientConfig(siliconflow_api_key="sk-secret").require_tts()
    VoiceClientConfig(stt_provider="fish", fish_api_key="fish-secret").require_stt()
    with pytest.raises(ValueError, match="FISH_API_KEY"):
        VoiceClientConfig(stt_provider="fish").require_stt()
    VoiceClientConfig(stt_provider="openai", openai_stt_api_key="sk-oai").require_stt()
    assert VoiceClientConfig().vad_silence_ms == 480
    assert VoiceClientConfig().tts_streaming is True
    VoiceClientConfig(tts_provider="fish", fish_api_key="f").require_tts()
    VoiceClientConfig(tts_provider="fish", fish_api_key="f", fish_reference_id="r").require_tts()
    with pytest.raises(ValueError, match="FISH_API_KEY"):
        VoiceClientConfig(tts_provider="fish").require_tts()
    with pytest.raises(ValueError, match="TTS_BUFFER_MODE"):
        load_config(env={"TTS_BUFFER_MODE": "bad"}, env_file=missing)
    with pytest.raises(ValueError, match="FISH_LATENCY"):
        load_config(env={"FISH_LATENCY": "turbo"}, env_file=missing)
    VoiceClientConfig(tts_provider="edge", edge_tts_voice="zh-CN-XiaoyiNeural").require_tts()

    # Regex validation for adapter_id and client_id
    with pytest.raises(ValueError, match="COMPANION_ADAPTER_ID"):
        load_config(env={"COMPANION_ADAPTER_ID": "BAD_ADAPTER"}, env_file=missing)
    with pytest.raises(ValueError, match="COMPANION_CLIENT_ID"):
        load_config(env={"COMPANION_CLIENT_ID": "bad/client"}, env_file=missing)


def test_strip_stt_markup_and_list_avfoundation_devices(monkeypatch):
    raw = "<|zh|><|NEUTRAL|><|Speech|><|withitn|>  你好   哈基米！ <|end|> "
    assert strip_stt_markup(raw) == "你好 哈基米！"

    fake_stderr = "\n".join(
        [
            "[AVFoundation indev @ 0x1] AVFoundation video devices:",
            "[AVFoundation indev @ 0x1] [0] FaceTime HD Camera",
            "[AVFoundation indev @ 0x1] AVFoundation audio devices:",
            "[AVFoundation indev @ 0x1] [0] MacBook Air麦克风",
            "[AVFoundation indev @ 0x1] [1] BlackHole 2ch",
        ]
    )
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            args=a[0], returncode=1, stdout="", stderr=fake_stderr
        ),
    )
    assert list_avfoundation_audio_devices() == [
        ("0", "MacBook Air麦克风"),
        ("1", "BlackHole 2ch"),
    ]


async def test_providers_siliconflow_fish_openai_and_secret_redaction(tmp_path):
    sf_key = "sk-siliconflow-top-secret-999"
    fish_key = "fish-secret-token-777"
    oai_key = "sk-openai-secret-token-555"

    seen_requests: list[tuple[str, str, dict[str, str], bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append(
            (
                request.method,
                str(request.url),
                dict(request.headers),
                request.content,
            )
        )
        path = request.url.path
        if path.endswith("/audio/transcriptions"):
            auth = request.headers.get("authorization", "")
            if auth == f"Bearer {sf_key}":
                return httpx.Response(
                    200,
                    json={"text": "<|zh|><|HAPPY|><|Speech|>哈基米   早上好"},
                )
            if auth == f"Bearer {oai_key}":
                return httpx.Response(200, json={"text": "  hello whisper  "})
            return httpx.Response(
                401,
                text=f"unauthorized for Bearer {sf_key} key {oai_key}",
            )
        if path == "/v1/audio/speech":
            return httpx.Response(200, content=b"FAKE-MP3-AUDIO")
        if path == "/v1/uploads/audio/voice":
            return httpx.Response(200, json={"uri": "speech:hakimi-custom:abc123"})
        if path == "/v1/tts":
            return httpx.Response(200, content=b"FAKE-FISH-MP3")
        return httpx.Response(404, text="not found")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        sf_stt = SiliconFlowSTT(
            api_key=sf_key,
            base_url="https://api.siliconflow.cn/v1",
            model="FunAudioLLM/SenseVoiceSmall",
            http=http,
        )
        recognized = await sf_stt.transcribe(b"RIFF-FAKE-WAV", filename="clip.wav")
        assert recognized == "哈基米 早上好"
        method, url, hdrs, body = seen_requests[-1]
        assert method == "POST"
        assert url == "https://api.siliconflow.cn/v1/audio/transcriptions"
        assert hdrs["authorization"] == f"Bearer {sf_key}"
        assert b"FunAudioLLM/SenseVoiceSmall" in body
        assert b"clip.wav" in body

        # SiliconFlow TTS
        sf_tts = SiliconFlowTTS(
            api_key=sf_key,
            base_url="https://api.siliconflow.cn/v1",
            model="FunAudioLLM/CosyVoice2-0.5B",
            voice="FunAudioLLM/CosyVoice2-0.5B:diana",
            response_format="mp3",
            speed=1.1,
            gain=1.5,
            http=http,
        )
        audio_bytes, fmt = await sf_tts.synthesize("喵呜测试")
        assert audio_bytes == b"FAKE-MP3-AUDIO"
        assert fmt == "mp3"
        method, url, hdrs, body = seen_requests[-1]
        assert url == "https://api.siliconflow.cn/v1/audio/speech"
        assert hdrs["authorization"] == f"Bearer {sf_key}"
        payload = json.loads(body.decode("utf-8"))
        assert payload == {
            "model": "FunAudioLLM/CosyVoice2-0.5B",
            "input": "喵呜测试",
            "voice": "FunAudioLLM/CosyVoice2-0.5B:diana",
            "response_format": "mp3",
            "sample_rate": 44100,
            "speed": 1.1,
            "gain": 1.5,
        }

        # Custom voice upload
        ref_audio = tmp_path / "ref.wav"
        ref_audio.write_bytes(b"RIFF-REF-AUDIO")
        uri = await sf_tts.upload_custom_voice(ref_audio, "hakimi_v1", "主人好呀喵")
        assert uri == "speech:hakimi-custom:abc123"
        _, url, hdrs, body = seen_requests[-1]
        assert url == "https://api.siliconflow.cn/v1/uploads/audio/voice"
        assert hdrs["authorization"] == f"Bearer {sf_key}"
        assert b"hakimi_v1" in body
        assert "主人好呀喵".encode() in body
        assert b"ref.wav" in body

        # Fish Audio TTS (with model header, prosody, latency, and optional reference_id)
        fish_tts = FishAudioTTS(
            api_key=fish_key,
            base_url="https://api.fish.audio",
            reference_id="ref-id-001",
            model="s2.1-pro",
            response_format="mp3",
            latency="balanced",
            speed=1.1,
            volume=1.5,
            http=http,
        )
        fish_bytes, fish_fmt = await fish_tts.synthesize("小鱼干")
        assert fish_bytes == b"FAKE-FISH-MP3"
        assert fish_fmt == "mp3"
        _, url, hdrs, body = seen_requests[-1]
        assert url == "https://api.fish.audio/v1/tts"
        assert hdrs["authorization"] == f"Bearer {fish_key}"
        assert hdrs["model"] == "s2.1-pro"
        assert json.loads(body.decode("utf-8")) == {
            "text": "小鱼干",
            "reference_id": "ref-id-001",
            "format": "mp3",
            "mp3_bitrate": 128,
            "latency": "balanced",
            "normalize": True,
            "prosody": {"speed": 1.1, "volume": 1.5},
        }

        # Fish Audio TTS without model / reference_id uses defaults cleanly
        fish_min = FishAudioTTS(api_key=fish_key, base_url="https://api.fish.audio", http=http)
        await fish_min.synthesize("测试默认")
        _, _, hdrs2, body2 = seen_requests[-1]
        assert "model" not in hdrs2
        assert "reference_id" not in json.loads(body2.decode("utf-8"))

        # OpenAI-compatible STT
        oai_stt = OpenAICompatibleSTT(
            api_key=oai_key,
            base_url="https://api.groq.com/openai/v1",
            model="whisper-large-v3-turbo",
            http=http,
        )
        assert await oai_stt.transcribe(b"RIFF-WAV") == "hello whisper"
        _, url, hdrs, _ = seen_requests[-1]
        assert url == "https://api.groq.com/openai/v1/audio/transcriptions"
        assert hdrs["authorization"] == f"Bearer {oai_key}"

        # Safe error handling never leaks API keys
        bad_stt = SiliconFlowSTT(api_key="wrong-key-secret-xyz", http=http)
        with pytest.raises(ProviderError) as exc_info:
            await bad_stt.transcribe(b"RIFF-WAV")
        err_msg = str(exc_info.value)
        assert "HTTP 401" in err_msg
        assert sf_key not in err_msg
        assert oai_key not in err_msg
        assert "wrong-key-secret-xyz" not in err_msg


def test_pcm_helpers_and_energy_vad_detector():
    assert pcm16_rms(b"") == 0.0
    assert pcm16_rms(b"\x01") == 0.0
    assert pcm16_rms(_silence_frame()) == 0.0

    loud_amp = 16384
    rms = pcm16_rms(_pcm_frame(loud_amp))
    assert math.isclose(rms, 0.5, rel_tol=1e-4)

    pcm = _pcm_frame(1200, samples=160)
    wav_bytes = pcm16_to_wav_bytes(pcm, sample_rate=16000, channels=1)
    with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
        assert wf.getnchannels() == 1
        assert wf.getsampwidth() == 2
        assert wf.getframerate() == 16000
        assert wf.readframes(wf.getnframes()) == pcm

    # Pre-roll + speech + silence hangover -> emits utterance
    vad = EnergyVadDetector(
        sample_rate=16000,
        frame_ms=30,
        threshold=0.05,
        silence_ms=90,  # 3 frames
        min_speech_ms=120,  # 4 active frames
        max_speech_ms=300,  # 10 total frames
        barge_in_multiplier=2.0,
        pre_roll_ms=60,  # 2 frames
        speech_start_frames=2,
    )
    pre1 = _pcm_frame(100)
    pre2 = _pcm_frame(200)
    assert vad.feed(pre1) == (False, None)
    assert vad.feed(pre2) == (False, None)

    speech = _pcm_frame(4000)  # rms ~ 0.122 > 0.05
    assert vad.feed(speech) == (False, None)  # onset frame 1
    assert vad.feed(speech) == (False, None)  # onset frame 2
    assert vad.feed(speech) == (False, None)
    assert vad.feed(speech) == (False, None)  # 4 active frames = 120ms

    assert vad.feed(_silence_frame()) == (False, None)
    assert vad.feed(_silence_frame()) == (False, None)
    barge_in, emitted = vad.feed(_silence_frame())
    assert barge_in is False
    assert emitted is not None
    # Pre-roll (2 frames) + 4 speech + 3 silence = 9 frames * 960 bytes
    assert emitted.startswith(pre1 + pre2 + speech)
    assert len(emitted) == 9 * 960

    # Short noise below min_speech_ms is dropped
    vad.reset()
    assert vad.feed(speech) == (False, None)
    assert vad.feed(speech) == (False, None)  # 60ms < 120ms
    assert vad.feed(_silence_frame()) == (False, None)
    assert vad.feed(_silence_frame()) == (False, None)
    assert vad.feed(_silence_frame()) == (False, None)

    # max_speech_ms cuts utterance cleanly without waiting for silence
    vad.reset()
    cut_emitted = None
    for _ in range(10):
        _, out = vad.feed(speech)
        if out is not None:
            cut_emitted = out
            break
    assert cut_emitted is not None
    assert len(cut_emitted) == 10 * 960

    # Barge-in during playback requires threshold * barge_in_multiplier (0.05 * 2.0 = 0.10)
    vad.reset()
    medium = _pcm_frame(2300)  # rms ~ 0.070 (> 0.05, < 0.10)
    assert vad.feed(medium, playback_active=True) == (False, None)
    assert vad.feed(medium, playback_active=True) == (False, None)

    loud = _pcm_frame(5000)  # rms ~ 0.153 (> 0.10)
    assert vad.feed(loud, playback_active=True) == (False, None)  # onset 1
    barge, out = vad.feed(loud, playback_active=True)  # onset 2 -> barge-in!
    assert barge is True
    assert out is None
    assert vad.flush() is None  # only 60ms < 120ms


async def test_afplay_audio_player_ordered_prefetch_and_cancel_turn(tmp_path):
    play_log = tmp_path / "played.jsonl"
    synth_started: list[str] = []
    release_chunk2 = asyncio.Event()
    playing_started = asyncio.Event()

    class FakeTTS:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            synth_started.append(text)
            if text == "第二句":
                await release_chunk2.wait()
            if text == "slow-turn":
                await asyncio.sleep(0.01)
            return text.encode("utf-8"), "mp3"

        async def aclose(self) -> None:
            return None

    def play_cmd_factory(audio_path: Path) -> list[str]:
        script = "\n".join(
            [
                "import json, pathlib, sys, time",
                "p = pathlib.Path(sys.argv[1])",
                "data = p.read_text(encoding='utf-8')",
                "log_p = pathlib.Path(sys.argv[2])",
                "line = json.dumps({'name': p.name, 'data': data}, ensure_ascii=False)",
                "with log_p.open('a', encoding='utf-8') as fh:",
                "    fh.write(line + '\\n')",
                "if data == 'slow-turn':",
                "    time.sleep(5.0)",
            ]
        )
        return [sys.executable, "-c", script, str(audio_path), str(play_log)]

    player = AfplayAudioPlayer(FakeTTS(), play_cmd_factory=play_cmd_factory)
    await player.start()
    try:
        # Enqueue 3 chunks: chunk 2 blocks briefly while chunk 3 still prefetches in parallel
        player.enqueue_chunk("t1", 1, "第一句")
        player.enqueue_chunk("t1", 2, "第二句")
        player.enqueue_chunk("t1", 3, "第三句")
        await asyncio.sleep(0.05)
        assert synth_started == ["第一句", "第二句", "第三句"]
        release_chunk2.set()
        await player.wait_idle()

        played = [
            json.loads(line)["data"]
            for line in play_log.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert played == ["第一句", "第二句", "第三句"]

        # Cancel turn terminates active playback subprocess & drops queued chunks
        player.enqueue_chunk("t2", 1, "slow-turn")
        player.enqueue_chunk("t2", 2, "never-played")
        for _ in range(100):
            if player.is_playing:
                playing_started.set()
                break
            await asyncio.sleep(0.01)
        assert playing_started.is_set()

        await player.cancel_turn("t2")
        await player.wait_idle()
        assert player.is_playing is False

        played_after = [
            json.loads(line)["data"]
            for line in play_log.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert "never-played" not in played_after
    finally:
        await player.stop()


def test_cli_main_list_devices_and_check_ws(monkeypatch, capsys, tmp_path):
    # 1. --list-devices
    monkeypatch.setattr(
        "voice_client.cli.list_avfoundation_audio_devices",
        lambda: [("0", "MacBook Air麦克风"), ("1", "外接声卡")],
    )
    assert main(["--list-devices"]) == 0
    out = capsys.readouterr().out
    assert "[0] MacBook Air麦克风" in out
    assert "[1] 外接声卡" in out

    # 2. main(["--env-file", ..., "--check-ws"]) CLI dispatch
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "COMPANION_WS_URL=wss://example.invalid/ws/companion/secret-path-xyz",
                "COMPANION_WS_TOKEN=companion-secret-token-32-chars-min",
            ]
        ),
        encoding="utf-8",
    )

    class FakeClientForMain:
        def __init__(self, config: VoiceClientConfig, **kwargs: Any) -> None:
            self._cfg = config

        async def connect(self) -> None:
            return None

        async def start_session(self) -> dict[str, Any]:
            return {"mode": "owner_private", "session_id": "cli-sess-1"}

        async def ping(self, timeout: float = 10.0) -> float:  # noqa: ASYNC109
            return 12.3

        async def stop_session(self, timeout: float = 5.0) -> dict[str, Any]:  # noqa: ASYNC109
            return {"session_id": "cli-sess-1"}

        async def close(self) -> None:
            return None

    monkeypatch.setattr("voice_client.cli.CompanionWsClient", FakeClientForMain)
    assert main(["--env-file", str(env_file), "--check-ws"]) == 0
    main_out = capsys.readouterr().out
    assert "mode=owner_private" in main_out
    assert "12.3ms" in main_out
    assert "secret-path-xyz" not in main_out


async def test_ws_client_check_ws_keepalive_and_scripted_session(monkeypatch, capsys):
    sent_frames: list[dict[str, Any]] = []

    class FakeWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str] = asyncio.Queue()
            self.closed = False

        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            sent_frames.append(frame)
            typ = frame["type"]
            if typ == "session.start":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "session.ready",
                            "session_id": "sess-12345678",
                            "mode": "owner_private",
                            "config": {},
                        }
                    )
                )
            elif typ == "ping":
                await self.incoming.put(json.dumps({"type": "pong", "ts": 1.0}))
            elif typ == "session.stop":
                await self.incoming.put(
                    json.dumps({"type": "session.ended", "session_id": "sess-12345678"})
                )

        async def recv(self) -> str:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            self.closed = True
            await self.incoming.put("__EOF__")

    cfg = VoiceClientConfig(
        ws_url="wss://example.invalid/ws/companion/secret-path-xyz",
        ws_token="companion-secret-token-32-chars-min",
        ping_interval_seconds=0.0,
    )
    assert await check_ws(cfg, connect_factory=FakeWs) == 0
    ws_out = capsys.readouterr().out
    assert "owner_private" in ws_out
    assert "rtt=" in ws_out
    assert "secret-path-xyz" not in ws_out
    assert [f["type"] for f in sent_frames] == ["session.start", "ping", "session.stop"]

    # Secret redaction on connect failure
    bad_cfg = VoiceClientConfig(
        ws_url="wss://example.invalid/ws/companion/top-secret-suffix-999",
        ws_token="super-secret-ws-token-abcdefg",
    )

    async def failing_factory(url: str, additional_headers: dict[str, str] | None = None) -> Any:
        raise RuntimeError(f"rejected {url} with {additional_headers}")

    failing_client = CompanionWsClient(bad_cfg, connect_factory=failing_factory)
    with pytest.raises(CompanionWsError) as conn_exc:
        await failing_client.connect()
    assert "top-secret-suffix-999" not in str(conn_exc.value)
    assert "super-secret-ws-token-abcdefg" not in str(conn_exc.value)

    # Periodic keepalive pings + input validation
    ka_frames: list[dict[str, Any]] = []

    class KeepaliveWs(FakeWs):
        async def send(self, raw: str) -> None:
            ka_frames.append(json.loads(raw))
            await super().send(raw)

    ka_cfg = VoiceClientConfig(
        ws_url="wss://example.invalid/ws/companion/secret-path-xyz",
        ws_token="companion-secret-token-32-chars-min",
        ping_interval_seconds=0.03,
    )
    ka_client = CompanionWsClient(ka_cfg, connect_factory=KeepaliveWs)
    await ka_client.connect()
    try:
        with pytest.raises(ValueError, match="empty"):
            await ka_client.send_speech_turn("   ")
        with pytest.raises(ValueError, match="turn_id"):
            await ka_client.send_speech_turn("hi", turn_id="bad id!")
        await asyncio.sleep(0.08)
        assert any(f["type"] == "ping" for f in ka_frames)
    finally:
        await ka_client.close()

    # Scripted non-TTY interactive session (mode toggle + text turn + quit)
    monkeypatch.setattr(sys, "stdin", io.StringIO("m\nt\n你好哈基米\nq\n"))

    class IdleMic:
        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            await asyncio.sleep(0.05)
            return _silence_frame()

        async def stop(self) -> None:
            return None

    class DummySTT:
        async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
            return ""

        async def aclose(self) -> None:
            return None

    class DummyTTS:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return b"MP3", "mp3"

        async def aclose(self) -> None:
            return None

    class DummyPlayer:
        is_playing = False

        async def start(self) -> None:
            return None

        def enqueue_chunk(self, turn_id: str, seq: int, text: str) -> None:
            return None

        async def cancel_turn(self, turn_id: str | None = None) -> None:
            return None

        async def stop(self) -> None:
            return None

    scripted_frames: list[dict[str, Any]] = []

    class ScriptedWs(FakeWs):
        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            scripted_frames.append(frame)
            if frame["type"] == "speech.turn":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "turn.start",
                            "turn_id": frame["turn_id"],
                            "kind": "speech",
                        }
                    )
                )
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "speak.chunk",
                            "turn_id": frame["turn_id"],
                            "seq": 1,
                            "text": "收到啦喵!",
                            "final": False,
                        }
                    )
                )
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "turn.end",
                            "turn_id": frame["turn_id"],
                            "status": "spoken",
                        }
                    )
                )
            else:
                await super().send(raw)

    int_cfg = VoiceClientConfig(
        ws_url="wss://example.invalid/ws/companion/secret-path-xyz",
        ws_token="companion-secret-token-32-chars-min",
        siliconflow_api_key="sk-dummy",
        ping_interval_seconds=0.0,
    )
    assert (
        await run_interactive_session(
            int_cfg,
            connect_factory=ScriptedWs,
            stt_provider=DummySTT(),
            tts_provider=DummyTTS(),
            mic_capture=IdleMic(),
            audio_player=DummyPlayer(),
        )
        == 0
    )
    assert any(f["type"] == "speech.turn" and f["text"] == "你好哈基米" for f in scripted_frames)


class _InMemoryWsBridge:
    """In-memory bidirectional bridge between CompanionWsClient and VoiceCompanion.serve."""

    _EOF = object()

    def __init__(self) -> None:
        self._c2s: asyncio.Queue[str | object] = asyncio.Queue()
        self._s2c: asyncio.Queue[str | object] = asyncio.Queue()
        self._closed = False

    async def server_receive(self) -> str:
        item = await self._c2s.get()
        if item is self._EOF:
            raise RuntimeError("eof")
        return str(item)

    async def server_send(self, raw: str) -> None:
        await self._s2c.put(raw)

    async def server_close(self, code: int = 1000) -> None:
        _ = code
        self._closed = True
        await self._s2c.put(self._EOF)
        await self._c2s.put(self._EOF)

    def client_ws(self) -> Any:
        bridge = self

        class _ClientEndpoint:
            async def send(self, raw: str) -> None:
                if bridge._closed:
                    raise RuntimeError("closed")
                await bridge._c2s.put(raw)

            async def recv(self) -> str:
                item = await bridge._s2c.get()
                if item is bridge._EOF:
                    raise RuntimeError("eof")
                return str(item)

            async def close(self) -> None:
                bridge._closed = True
                await bridge._c2s.put(bridge._EOF)
                await bridge._s2c.put(bridge._EOF)

        return _ClientEndpoint()


class _StreamingFakeGateway:
    """Streaming FakeGateway compatible with VoiceCompanion._run_turn."""

    def __init__(self) -> None:
        self.deltas: list[str] = ["诶！", "这波操作太帅了！", "左边好像有宝箱。"]
        self.delay: float = 0.0
        self.calls: list[tuple[dict[str, Any], str, dict[str, Any]]] = []

    async def screen(self, payload: Any, subject: str, **kwargs: Any) -> str:
        _ = payload, subject, kwargs
        return "chat"

    async def generate(self, payload: dict[str, Any], subject: str, **kwargs: Any) -> str:
        self.calls.append((payload, subject, kwargs))
        on_text = kwargs.get("on_text")
        cancel_event = kwargs.get("cancel_event")
        out: list[str] = []
        for piece in self.deltas:
            if self.delay:
                await asyncio.sleep(self.delay)
            if cancel_event is not None and cancel_event.is_set():
                raise asyncio.CancelledError
            out.append(piece)
            if on_text is not None:
                res = on_text(piece)
                if hasattr(res, "__await__"):
                    await res
        return "".join(out)

    async def close(self) -> None:
        return None


async def test_e2e_companion_ws_client_with_real_voice_companion(settings, tmp_path):
    gw = _StreamingFakeGateway()
    s = settings.model_copy(
        update={
            "owner_qq_id": "20000",
            "companion_ws_enabled": True,
            "companion_ws_token": SecretStr(COMPANION_TOKEN),
            "companion_ws_path": COMPANION_PATH,
        }
    )
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    played_log = tmp_path / "e2e_played.jsonl"

    class MockSynthTTS:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return text.encode("utf-8"), "mp3"

        async def aclose(self) -> None:
            return None

    def play_cmd_factory(audio_path: Path) -> list[str]:
        script = "\n".join(
            [
                "import json, pathlib, sys",
                "p = pathlib.Path(sys.argv[1])",
                "log_p = pathlib.Path(sys.argv[2])",
                "line = json.dumps(p.read_text(encoding='utf-8'), ensure_ascii=False)",
                "with log_p.open('a', encoding='utf-8') as fh:",
                "    fh.write(line + '\\n')",
            ]
        )
        return [sys.executable, "-c", script, str(audio_path), str(played_log)]

    async with app.router.lifespan_context(app):
        svc = app.state.service
        bridge = _InMemoryWsBridge()
        serve_task = asyncio.create_task(
            svc.voice_companion.serve(
                bridge.server_receive,
                bridge.server_send,
                bridge.server_close,
                source_ip="127.0.0.1",
                adapter_id="mac-voice",
                client_id="boiser-m4",
            )
        )

        player = AfplayAudioPlayer(MockSynthTTS(), play_cmd_factory=play_cmd_factory)
        await player.start()

        turn_started: dict[str, asyncio.Event] = {}
        turn_ended: dict[str, dict[str, Any]] = {}
        turn_end_events: dict[str, asyncio.Event] = {}
        received_chunks: list[tuple[str, int, str]] = []
        errors: list[dict[str, Any]] = []

        async def on_turn_start(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            turn_started.setdefault(tid, asyncio.Event()).set()

        async def on_speak_chunk(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            seq = int(frame.get("seq") or 0)
            txt = str(frame.get("text") or "")
            received_chunks.append((tid, seq, txt))
            player.enqueue_chunk(tid, seq, txt)

        async def on_turn_end(frame: dict[str, Any]) -> None:
            tid = str(frame.get("turn_id") or "")
            turn_ended[tid] = frame
            turn_end_events.setdefault(tid, asyncio.Event()).set()

        async def on_error(frame: dict[str, Any]) -> None:
            errors.append(frame)

        cfg = VoiceClientConfig(
            ws_url="wss://example.invalid" + COMPANION_PATH,
            ws_token=COMPANION_TOKEN,
            adapter_id="mac-voice",
            client_id="boiser-m4",
            session_mode="owner_private",
            game_title="Minecraft",
            ping_interval_seconds=0.0,
        )
        client = CompanionWsClient(
            cfg,
            connect_factory=bridge.client_ws,
            on_turn_start=on_turn_start,
            on_speak_chunk=on_speak_chunk,
            on_turn_end=on_turn_end,
            on_error=on_error,
        )

        try:
            await client.connect()

            # 1. session.start -> session.ready
            ready = await client.start_session()
            assert ready["type"] == "session.ready"
            assert ready["mode"] == "owner_private"
            sid = ready["session_id"]
            assert svc.world_state.current.current_module == "voice_companion"

            # 2. ping -> pong
            rtt_ms = await client.ping(timeout=3.0)
            assert rtt_ms >= 0.0

            # 3. speech.turn -> streamed speak.chunks synthesized & played in order
            turn_end_events["e2e-t1"] = asyncio.Event()
            tid1 = await client.send_speech_turn("快看我这波闪避反击", turn_id="e2e-t1")
            assert tid1 == "e2e-t1"
            await asyncio.wait_for(turn_end_events["e2e-t1"].wait(), timeout=3.0)
            assert turn_ended["e2e-t1"]["status"] == "spoken"
            assert [c[2] for c in received_chunks if c[0] == "e2e-t1"] == [
                "诶!",
                "这波操作太帅了!",
                "左边好像有宝箱。",
            ]
            await player.wait_idle()
            played_texts = [
                json.loads(line)
                for line in played_log.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            assert played_texts == ["诶!", "这波操作太帅了!", "左边好像有宝箱。"]

            # 4. turn.cancel aborts slow speech turn cleanly
            gw.deltas = ["慢慢说一句。", "第二句不会说完。"]
            gw.delay = 0.25
            turn_started["e2e-t2"] = asyncio.Event()
            turn_end_events["e2e-t2"] = asyncio.Event()
            await client.send_speech_turn("帮我看看前面有什么", turn_id="e2e-t2")
            await asyncio.wait_for(turn_started["e2e-t2"].wait(), timeout=3.0)
            await client.cancel_turn("e2e-t2")
            await player.cancel_turn("e2e-t2")
            await asyncio.wait_for(turn_end_events["e2e-t2"].wait(), timeout=3.0)
            assert turn_ended["e2e-t2"]["status"] == "cancelled"
            assert turn_ended["e2e-t2"]["reason"] == "barge_in"

            # 5. session.stop -> session.ended + clean teardown
            ended = await client.stop_session(timeout=3.0)
            assert ended["type"] == "session.ended"
            assert ended["session_id"] == sid
            assert svc.world_state.current.current_module == "idle"
            assert errors == []

            row = await svc.store.one(
                "SELECT status, spoken_turns FROM voice_companion_sessions WHERE id=?",
                (sid,),
            )
            assert row is not None and row[0] == "ended" and row[1] == 1
        finally:
            await client.close()
            await player.stop()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(serve_task, timeout=2.0)


async def test_cli_test_tts_stt_upload_and_vad_barge_in(monkeypatch, capsys, tmp_path):
    from voice_client.cli import test_stt, test_tts, upload_voice

    played_once: list[tuple[bytes, str]] = []

    class FakeTTS:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return f"audio:{text}".encode(), "mp3"

        async def aclose(self) -> None:
            return None

    class FakePlayerOnce:
        def __init__(self, tts: Any) -> None:
            self._tts = tts

        async def play_once(self, audio_bytes: bytes, ext: str = "mp3") -> None:
            played_once.append((audio_bytes, ext))

        async def stop(self) -> None:
            return None

    monkeypatch.setattr("voice_client.cli.build_tts_provider", lambda cfg: FakeTTS())
    monkeypatch.setattr("voice_client.cli.AfplayAudioPlayer", FakePlayerOnce)
    cfg = VoiceClientConfig(
        ws_url="wss://example.invalid" + COMPANION_PATH,
        ws_token=COMPANION_TOKEN,
        siliconflow_api_key="sk-test-dummy",
        ping_interval_seconds=0.0,
        vad_threshold=0.05,
        vad_silence_ms=90,
        vad_min_speech_ms=90,
        vad_barge_in_multiplier=2.0,
    )
    assert await test_tts(cfg, "测试一下") == 0
    assert played_once == [("audio:测试一下".encode(), "mp3")]

    # --test-stt (short duration)
    class FakeStt:
        async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
            assert wav_bytes.startswith(b"RIFF")
            return "识别成功喵"

        async def aclose(self) -> None:
            return None

    class FakeMic4s:
        def __init__(self, **kwargs: Any) -> None:
            self._count = 0

        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            self._count += 1
            if self._count > 10:
                return b""
            return _pcm_frame(2000)

        async def stop(self) -> None:
            return None

    monkeypatch.setattr("voice_client.cli.build_stt_provider", lambda cfg: FakeStt())
    monkeypatch.setattr("voice_client.cli.FfmpegMicCapture", FakeMic4s)
    assert await test_stt(cfg, duration_seconds=0.1) == 0
    assert "识别成功喵" in capsys.readouterr().out

    # --upload-voice
    class FakeUploadTTS(SiliconFlowTTS):
        async def upload_custom_voice(
            self, audio_path: Path, custom_name: str, ref_text: str
        ) -> str:
            assert custom_name == "hakimi"
            assert ref_text == "喵呜"
            return "speech:uploaded:999"

    sample = tmp_path / "voice.wav"
    sample.write_bytes(b"RIFF-DATA")
    monkeypatch.setattr(
        "voice_client.cli.build_tts_provider",
        lambda c: FakeUploadTTS(api_key=c.siliconflow_api_key),
    )
    assert await upload_voice(cfg, sample, "hakimi", "喵呜") == 0
    assert "speech:uploaded:999" in capsys.readouterr().out

    # VAD speech emission + barge-in interrupt inside run_interactive_session
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    frames_seq = [
        *([_pcm_frame(4000)] * 4),  # speech (120ms >= 90ms)
        *([_silence_frame()] * 3),  # silence hangover (90ms) -> triggers STT -> speech.turn
        *([_pcm_frame(5000)] * 2),  # loud barge-in while player.is_playing=True
    ]

    class VadMic:
        def __init__(self) -> None:
            self._idx = 0

        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            await asyncio.sleep(0.01)
            if self._idx < len(frames_seq):
                f = frames_seq[self._idx]
                self._idx += 1
                return f
            await asyncio.sleep(0.05)
            return b""  # clean EOF ends session

        async def stop(self) -> None:
            return None

    class BargeInPlayer:
        def __init__(self) -> None:
            self.is_playing = True
            self.cancelled = 0

        async def start(self) -> None:
            return None

        def enqueue_chunk(self, turn_id: str, seq: int, text: str) -> None:
            return None

        async def cancel_turn(self, turn_id: str | None = None) -> None:
            self.cancelled += 1
            self.is_playing = False

        async def stop(self) -> None:
            return None

    ws_seen: list[dict[str, Any]] = []

    class VadFakeWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str] = asyncio.Queue()

        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            ws_seen.append(frame)
            if frame["type"] == "session.start":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "session.ready",
                            "session_id": "vad-s1",
                            "mode": "owner_private",
                            "config": {},
                        }
                    )
                )
            elif frame["type"] == "session.stop":
                await self.incoming.put(
                    json.dumps({"type": "session.ended", "session_id": "vad-s1"})
                )

        async def recv(self) -> str:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self.incoming.put("__EOF__")

    barge_player = BargeInPlayer()
    assert (
        await run_interactive_session(
            cfg,
            connect_factory=VadFakeWs,
            stt_provider=FakeStt(),
            tts_provider=FakeTTS(),
            mic_capture=VadMic(),
            audio_player=barge_player,
        )
        == 0
    )
    assert barge_player.cancelled >= 1
    assert any(f["type"] == "turn.cancel" for f in ws_seen)
    assert any(f["type"] == "speech.turn" and f["text"] == "识别成功喵" for f in ws_seen)


async def test_resolve_ws_proxy_and_connect_passes_proxy(monkeypatch):
    import importlib.util
    import urllib.request

    from voice_client.companion_ws import _resolve_ws_proxy

    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)

    # 1. macOS Clash/Surge proxy dict with http + https + socks -> picks https, skips socks
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {
            "http": "http://127.0.0.1:7897",
            "https": "http://127.0.0.1:7898",
            "socks": "http://127.0.0.1:7897",
        },
    )
    assert _resolve_ws_proxy("wss://qq.example.com/ws/companion/x") == "http://127.0.0.1:7898"

    # 2. socks-only or socks scheme URLs without python_socks -> None (direct)
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {"socks": "http://127.0.0.1:7897", "https": "socks5://127.0.0.1:7898"},
    )
    assert _resolve_ws_proxy("wss://qq.example.com/ws/companion/x") is None

    # 3. proxy_bypass -> None
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {"https": "http://127.0.0.1:7898"},
    )
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: True)
    assert _resolve_ws_proxy("wss://qq.example.com/ws/companion/x") is None

    # 4. python_socks installed -> True (let websockets use default auto-detection)
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name: object() if name == "python_socks" else None
    )
    assert _resolve_ws_proxy("wss://qq.example.com/ws/companion/x") is True

    # 5. Default CompanionWsClient.connect() passes proxy=_resolve_ws_proxy(...) to ws_connect
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {
            "http": "http://127.0.0.1:7897",
            "https": "http://127.0.0.1:7897",
            "socks": "http://127.0.0.1:7897",
        },
    )

    connect_calls: list[dict[str, Any]] = []

    class DummyWs:
        def __init__(self) -> None:
            self._q: asyncio.Queue[str] = asyncio.Queue()

        async def send(self, raw: str) -> None:
            return None

        async def recv(self) -> str:
            item = await self._q.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self._q.put("__EOF__")

    async def fake_ws_connect(url: str, **kwargs: Any) -> DummyWs:
        connect_calls.append({"url": url, **kwargs})
        return DummyWs()

    monkeypatch.setattr("websockets.asyncio.client.connect", fake_ws_connect)
    cfg = VoiceClientConfig(
        ws_url="wss://qq.example.com/ws/companion/secret-path",
        ws_token="companion-secret-token-32-chars-min",
        ping_interval_seconds=0.0,
    )
    client = CompanionWsClient(cfg)
    await client.connect()
    try:
        assert len(connect_calls) == 1
        assert connect_calls[0]["proxy"] == "http://127.0.0.1:7897"
        assert connect_calls[0]["ping_interval"] is None
        assert connect_calls[0]["max_size"] == 4_194_304
    finally:
        await client.close()


async def test_full_turn_tts_buffer_mode_and_chunk_mode(monkeypatch):
    import sys

    from voice_client.cli import _join_turn_chunks, run_interactive_session

    assert (
        _join_turn_chunks(["哇，刚才吓死我了", "，", "还好没事！"])
        == "哇，刚才吓死我了，还好没事！"
    )
    assert _join_turn_chunks(["Hello", "World", "，走吧"]) == "Hello World，走吧"
    assert _join_turn_chunks(["Go ", "MC"]) == "Go MC"

    async def _run_case(mode: str, status: str) -> list[tuple[str, int, str]]:
        monkeypatch.setattr(sys, "stdin", io.StringIO("t\n第一句话\nq\n"))
        enqueued: list[tuple[str, int, str]] = []

        class IdleMic:
            async def start(self) -> None:
                return None

            async def read_frame(self) -> bytes:
                await asyncio.sleep(0.04)
                return _silence_frame()

            async def stop(self) -> None:
                return None

        class DummySTT:
            async def transcribe(self, wav_bytes: bytes, *, filename: str = "utterance.wav") -> str:
                return ""

            async def aclose(self) -> None:
                return None

        class DummyTTS:
            async def synthesize(self, text: str) -> tuple[bytes, str]:
                return b"MP3", "mp3"

            async def aclose(self) -> None:
                return None

        class CapturePlayer:
            is_playing = False

            async def start(self) -> None:
                return None

            def enqueue_chunk(self, turn_id: str, seq: int, text: str) -> None:
                enqueued.append((turn_id, seq, text))

            async def cancel_turn(self, turn_id: str | None = None) -> None:
                return None

            async def stop(self) -> None:
                return None

        class MultiChunkWs:
            def __init__(self) -> None:
                self.incoming: asyncio.Queue[str] = asyncio.Queue()

            async def send(self, raw: str) -> None:
                frame = json.loads(raw)
                if frame["type"] == "session.start":
                    await self.incoming.put(
                        json.dumps(
                            {
                                "type": "session.ready",
                                "session_id": "buf-s1",
                                "mode": "owner_private",
                                "config": {},
                            }
                        )
                    )
                elif frame["type"] == "speech.turn":
                    tid = frame["turn_id"]
                    await self.incoming.put(
                        json.dumps({"type": "turn.start", "turn_id": tid, "kind": "speech"})
                    )
                    for idx, piece in enumerate(("哇，", "苦力怕来了！", "快跑喵~"), start=1):
                        await self.incoming.put(
                            json.dumps(
                                {
                                    "type": "speak.chunk",
                                    "turn_id": tid,
                                    "seq": idx,
                                    "text": piece,
                                    "final": False,
                                }
                            )
                        )
                    # Wait briefly mid-turn before turn.end
                    await asyncio.sleep(0.02)
                    if mode == "turn":
                        assert enqueued == []
                    await self.incoming.put(
                        json.dumps({"type": "turn.end", "turn_id": tid, "status": status})
                    )
                elif frame["type"] == "session.stop":
                    await self.incoming.put(
                        json.dumps({"type": "session.ended", "session_id": "buf-s1"})
                    )

            async def recv(self) -> str:
                item = await self.incoming.get()
                if item == "__EOF__":
                    raise RuntimeError("eof")
                return item

            async def close(self) -> None:
                await self.incoming.put("__EOF__")

        cfg = VoiceClientConfig(
            ws_url="wss://example.invalid/ws/companion/secret-path-xyz",
            ws_token="companion-secret-token-32-chars-min",
            tts_provider="fish",
            fish_api_key="fish-dummy-key",
            tts_buffer_mode=mode,  # type: ignore[arg-type]
            ping_interval_seconds=0.0,
        )
        assert (
            await run_interactive_session(
                cfg,
                connect_factory=MultiChunkWs,
                stt_provider=DummySTT(),
                tts_provider=DummyTTS(),
                mic_capture=IdleMic(),
                audio_player=CapturePlayer(),
            )
            == 0
        )
        return enqueued

    # Full-turn mode merges chunks on turn.end(status="spoken") or "ok"
    res_spoken = await _run_case("turn", "spoken")
    assert len(res_spoken) == 1
    assert res_spoken[0][1] == 1
    assert res_spoken[0][2] == "哇，苦力怕来了！快跑喵~"

    res_ok = await _run_case("turn", "ok")
    assert len(res_ok) == 1
    assert res_ok[0][2] == "哇，苦力怕来了！快跑喵~"

    # Cancelled turn discards buffered text with 0 TTS synthesis calls
    res_cancelled = await _run_case("turn", "cancelled")
    assert res_cancelled == []

    # Chunk mode still enqueues each slice immediately
    res_chunk = await _run_case("chunk", "spoken")
    assert [txt for _, _, txt in res_chunk] == ["哇，", "苦力怕来了！", "快跑喵~"]


async def test_fish_asr_and_tts_streaming_chunks(monkeypatch):
    from voice_client.providers import build_stt_provider

    seen: list[tuple[str, dict[str, str], bytes]] = []
    fish_key = "fish-secret-token-never-leak"
    sf_key = "sf-secret-token-never-leak"

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), dict(request.headers), await request.aread()))
        if str(request.url) == "https://api.fish.audio/v1/asr":
            if request.headers.get("authorization") != f"Bearer {fish_key}":
                return httpx.Response(401, text=f"bad auth {request.headers.get('authorization')}")
            return httpx.Response(200, json={"text": "<|zh|> 你好呀，哈基米！"})
        if str(request.url) == "https://api.fish.audio/v1/tts":
            return httpx.Response(200, content=b"ID3" + b"A" * 2048 + b"B" * 2048)
        if str(request.url) == "https://api.siliconflow.cn/v1/audio/speech":
            return httpx.Response(403, text="quota_exceeded")
        raise AssertionError(f"unexpected url: {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        cfg = VoiceClientConfig(
            stt_provider="fish",
            fish_api_key=fish_key,
            fish_asr_language="zh",
            fish_asr_model="transcribe-1-pro",
        )
        stt = build_stt_provider(cfg, http=http)
        assert isinstance(stt, FishAudioSTT)
        assert await stt.transcribe(b"RIFF-WAV") == "你好呀，哈基米！"
        url, hdrs, body = seen[-1]
        assert url == "https://api.fish.audio/v1/asr"
        assert hdrs["authorization"] == f"Bearer {fish_key}"
        assert hdrs["model"] == "transcribe-1-pro"
        assert b'name="audio"' in body
        assert b'name="language"' in body

        bad_stt = FishAudioSTT(api_key="bad-key-xyz", http=http)
        with pytest.raises(ProviderError) as exc_info:
            await bad_stt.transcribe(b"RIFF-WAV")
        assert "HTTP 401" in str(exc_info.value)
        assert "bad-key-xyz" not in str(exc_info.value)

        fish_tts = FishAudioTTS(api_key=fish_key, reference_id="ref-1", http=http)
        chunks = [c async for c in fish_tts.synthesize_stream("测试流式")]
        assert b"".join(chunks).startswith(b"ID3")

        sf_tts = SiliconFlowTTS(api_key=sf_key, http=http)
        with pytest.raises(ProviderError, match="HTTP 403"):
            async for _ in sf_tts.synthesize_stream("失败测试"):
                pass


async def test_streaming_player_pipes_chunks_and_cancels_immediately(tmp_path):
    chunk1_emitted = asyncio.Event()
    allow_chunk2 = asyncio.Event()
    allow_cancel = asyncio.Event()
    first_audio_times: list[tuple[str, float]] = []

    class StreamTTS:
        response_format = "mp3"

        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return text.encode("utf-8"), "mp3"

        async def synthesize_stream(self, text: str):
            if text == "cancel-me":
                yield b"HEAD-"
                allow_cancel.set()
                await asyncio.sleep(5.0)
                yield b"TAIL"
                return
            yield b"FIRST-"
            chunk1_emitted.set()
            await allow_chunk2.wait()
            yield b"SECOND"

        async def aclose(self) -> None:
            return None

    sink_file = tmp_path / "streamed.bin"

    def stream_cmd_factory(fmt: str) -> list[str]:
        assert fmt == "mp3"
        script = (
            "import pathlib, sys; "
            "data = sys.stdin.buffer.read(); "
            "pathlib.Path(sys.argv[1]).write_bytes(data)"
        )
        return [sys.executable, "-c", script, str(sink_file)]

    player = AfplayAudioPlayer(
        StreamTTS(),
        stream_cmd_factory=stream_cmd_factory,
        enable_streaming=True,
        on_first_audio=lambda tid, ms: first_audio_times.append((tid, ms)),
    )
    assert player.streaming_active is True
    await player.start()
    try:
        player.enqueue_chunk("turn-1", 1, "hello-stream")
        await asyncio.wait_for(chunk1_emitted.wait(), timeout=2.0)
        for _ in range(50):
            if first_audio_times:
                break
            await asyncio.sleep(0.01)
        assert len(first_audio_times) == 1
        assert first_audio_times[0][0] == "turn-1"
        allow_chunk2.set()
        await player.wait_idle()
        assert sink_file.read_bytes() == b"FIRST-SECOND"

        player.enqueue_chunk("turn-cancel", 1, "cancel-me")
        await asyncio.wait_for(allow_cancel.wait(), timeout=2.0)
        for _ in range(50):
            if player.is_playing:
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(player.cancel_turn("turn-cancel"), timeout=1.5)
        await asyncio.wait_for(player.wait_idle(), timeout=1.5)
    finally:
        await player.stop()


def test_voice_gateway_config_loading_and_validation(tmp_path):
    missing = tmp_path / "missing.env"
    env_file = tmp_path / "gw.env"
    env_file.write_text(
        "\n".join(
            [
                "VOICE_GATEWAY_URL=wss://voice.example.invalid/ws/voice/secret-gw-path-001",
                "VOICE_GATEWAY_TOKEN=gateway-secret-token-32-chars-minimum",
                "VOICE_GATEWAY_OUTPUT_FORMAT=PCM_48K_STEREO",
                "PING_INTERVAL_SECONDS=12.5",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config(env={}, env_file=env_file)
    assert cfg.gateway_url == "wss://voice.example.invalid/ws/voice/secret-gw-path-001"
    assert cfg.gateway_token == "gateway-secret-token-32-chars-minimum"
    assert cfg.gateway_output_format == "pcm_48k_stereo"
    assert cfg.ping_interval_seconds == 12.5
    assert cfg.use_gateway is True
    cfg.require_gateway()

    for valid_fmt in ("", "mp3", "wav", "pcm_16k", "pcm_48k_stereo"):
        loaded = load_config(
            env={"VOICE_GATEWAY_OUTPUT_FORMAT": valid_fmt.upper()},
            env_file=missing,
        )
        assert loaded.gateway_output_format == valid_fmt

    with pytest.raises(ValueError, match="VOICE_GATEWAY_OUTPUT_FORMAT"):
        load_config(env={"VOICE_GATEWAY_OUTPUT_FORMAT": "ogg"}, env_file=missing)

    empty_cfg = load_config(env={}, env_file=missing)
    assert empty_cfg.use_gateway is False
    with pytest.raises(ValueError, match="VOICE_GATEWAY_URL"):
        empty_cfg.require_gateway()
    with pytest.raises(ValueError, match="ws://"):
        VoiceClientConfig(
            gateway_url="https://voice.example.invalid/ws/voice",
            gateway_token="g" * 32,
        ).require_gateway()
    with pytest.raises(ValueError, match="VOICE_GATEWAY_TOKEN"):
        VoiceClientConfig(
            gateway_url="wss://voice.example.invalid/ws/voice/secret",
            gateway_token="   ",
        ).require_gateway()


async def test_companion_ws_send_speech_turn_speaker_override():
    sent: list[dict[str, Any]] = []

    class DummyWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str] = asyncio.Queue()

        async def send(self, raw: str) -> None:
            sent.append(json.loads(raw))

        async def recv(self) -> str:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self.incoming.put("__EOF__")

    cfg = VoiceClientConfig(
        ws_url="wss://example.invalid/ws/companion/secret-path",
        ws_token=COMPANION_TOKEN,
        speaker_id="default-id",
        speaker_name="默认名",
        ping_interval_seconds=0.0,
    )
    client = CompanionWsClient(cfg, connect_factory=DummyWs)
    await client.connect()
    try:
        tid1 = await client.send_speech_turn("你好", turn_id="t-def")
        tid2 = await client.send_speech_turn(
            "这把冲",
            turn_id="t-ovr",
            speaker_id="guest-42",
            speaker_name="队友乙",
        )
        assert tid1 == "t-def"
        assert tid2 == "t-ovr"
        assert sent[0]["speaker_id"] == "default-id"
        assert sent[0]["speaker_name"] == "默认名"
        assert sent[1]["speaker_id"] == "guest-42"
        assert sent[1]["speaker_name"] == "队友乙"
    finally:
        await client.close()


async def test_voice_gateway_ws_client_protocol_binary_and_redaction(monkeypatch):
    import importlib.util
    import urllib.request

    gw_secret = "gateway-token-must-never-leak-32chars"
    gw_path = "/ws/voice/0123456789abcdef0123456789abcdef"
    gw_url = f"wss://gw.example.invalid{gw_path}"

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {"https": "http://127.0.0.1:7897", "socks": "http://127.0.0.1:7897"},
    )

    connect_calls: list[dict[str, Any]] = []

    async def failing_ws_connect(url: str, **kwargs: Any) -> Any:
        connect_calls.append({"url": url, **kwargs})
        raise RuntimeError(f"boom url={url} headers={kwargs.get('additional_headers')}")

    monkeypatch.setattr("websockets.asyncio.client.connect", failing_ws_connect)
    cfg = VoiceClientConfig(
        gateway_url=gw_url,
        gateway_token=gw_secret,
        gateway_output_format="pcm_16k",
        adapter_id="mac-voice",
        client_id="boiser-m4",
        speaker_id="owner",
        speaker_name="BOISER",
        ping_interval_seconds=0.0,
    )
    bad_client = VoiceGatewayWsClient(cfg)
    with pytest.raises(VoiceGatewayError) as exc_info:
        await bad_client.connect()
    err_str = str(exc_info.value)
    assert gw_secret not in err_str
    assert gw_path not in err_str
    assert len(connect_calls) == 1
    assert connect_calls[0]["additional_headers"] == {
        "Authorization": f"Bearer {gw_secret}",
        "X-Voice-Adapter": "mac-voice",
        "X-Voice-Client-Id": "boiser-m4",
    }
    assert connect_calls[0]["max_size"] == 16_777_216
    assert connect_calls[0]["ping_interval"] is None
    assert connect_calls[0]["proxy"] == "http://127.0.0.1:7897"

    sent_items: list[dict[str, Any] | bytes] = []

    class ScriptedGatewayWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str | bytes] = asyncio.Queue()

        async def send(self, data: str | bytes) -> None:
            if isinstance(data, (bytes, bytearray)):
                sent_items.append(bytes(data))
                return
            frame = json.loads(data)
            sent_items.append(frame)
            typ = frame["type"]
            if typ == "ping":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "pong",
                            "req_id": frame.get("req_id", ""),
                            "ts": 1.0,
                            "upstream_ms": None,
                        }
                    )
                )
            elif typ == "stt.transcribe":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "stt.result",
                            "req_id": frame["req_id"],
                            "text": "单独识别测试",
                            "stt_ms": 42.0,
                        }
                    )
                )
            elif typ == "tts.synthesize":
                req_id = frame["req_id"]
                if frame["text"] == "fail-me":
                    await self.incoming.put(
                        json.dumps(
                            {
                                "type": "error",
                                "req_id": req_id,
                                "reason": "tts_failed",
                                "detail": f"leaked {gw_secret} at {gw_url}",
                            }
                        )
                    )
                else:
                    await self.incoming.put(
                        json.dumps(
                            {
                                "type": "tts.start",
                                "req_id": req_id,
                                "turn_id": "",
                                "seq": 1,
                                "format": frame.get("output_format", "pcm_16k"),
                            }
                        )
                    )
                    await self.incoming.put(b"PCM-PART-1-")
                    await self.incoming.put(b"PCM-PART-2")
                    await self.incoming.put(
                        json.dumps(
                            {
                                "type": "tts.end",
                                "req_id": req_id,
                                "turn_id": "",
                                "seq": 1,
                                "bytes": 20,
                                "status": "ok",
                            }
                        )
                    )
            elif typ == "session.start":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "session.ready",
                            "session_id": "gw-sess-1",
                            "mode": frame["mode"],
                            "output_format": "pcm_16k",
                            "stt_provider": "fish",
                            "tts_provider": "siliconflow",
                            "tts_model": "CosyVoice2",
                            "upstream_config": {"use_screen_watch": True},
                        }
                    )
                )
            elif typ == "audio.stream.end":
                tid = frame["turn_id"]
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "stt.final",
                            "turn_id": tid,
                            "text": "语音流",
                            "stt_ms": 18,
                        }
                    )
                )
                await self.incoming.put(
                    json.dumps({"type": "turn.start", "turn_id": tid, "kind": "speech"})
                )
                await self.incoming.put(
                    json.dumps({"type": "text.chunk", "turn_id": tid, "seq": 1, "text": "收到喵"})
                )
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "tts.start",
                            "turn_id": tid,
                            "seq": 1,
                            "format": "pcm_16k",
                        }
                    )
                )
                await self.incoming.put(b"STREAM-AUDIO-BYTES")
                await self.incoming.put(
                    json.dumps(
                        {"type": "tts.end", "turn_id": tid, "seq": 1, "bytes": 18, "status": "ok"}
                    )
                )
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "turn.end",
                            "turn_id": tid,
                            "status": "spoken",
                            "metrics": {"stt_ms": 18.0, "llm_ms": 50.0, "tts_ttfb_ms": 12.0},
                        }
                    )
                )
            elif typ == "session.stop":
                await self.incoming.put(
                    json.dumps({"type": "session.ended", "session_id": "gw-sess-1"})
                )

        async def recv(self) -> str | bytes:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self.incoming.put("__EOF__")

    events: list[tuple[str, Any]] = []
    turn_done = asyncio.Event()

    async def on_stt_final(frame: dict[str, Any]) -> None:
        events.append(("stt.final", frame))

    async def on_turn_start(frame: dict[str, Any]) -> None:
        events.append(("turn.start", frame))

    async def on_text_chunk(frame: dict[str, Any]) -> None:
        events.append(("text.chunk", frame))

    async def on_tts_start(frame: dict[str, Any]) -> None:
        events.append(("tts.start", frame))

    async def on_tts_chunk(turn_id: str, seq: int, fmt: str, chunk: bytes) -> None:
        events.append(("tts.chunk", (turn_id, seq, fmt, chunk)))

    async def on_tts_end(frame: dict[str, Any]) -> None:
        events.append(("tts.end", frame))

    async def on_turn_end(frame: dict[str, Any]) -> None:
        events.append(("turn.end", frame))
        turn_done.set()

    client = VoiceGatewayWsClient(
        cfg,
        connect_factory=ScriptedGatewayWs,
        on_stt_final=on_stt_final,
        on_turn_start=on_turn_start,
        on_text_chunk=on_text_chunk,
        on_tts_start=on_tts_start,
        on_tts_chunk=on_tts_chunk,
        on_tts_end=on_tts_end,
        on_turn_end=on_turn_end,
    )
    await client.connect()
    try:
        # Standalone RPCs work pre-session
        pong = await client.ping_info(timeout=2.0)
        assert pong["upstream_ms"] is None
        assert pong["rtt_ms"] >= 0.0

        stt_txt = await client.transcribe_once(b"RIFF-WAV-DATA", timeout=2.0)
        assert stt_txt == "单独识别测试"

        one_arg_chunks: list[bytes] = []
        audio_out, fmt_out = await client.synthesize_once(
            "合成测试",
            on_chunk=lambda c: one_arg_chunks.append(c),
            timeout=2.0,
        )
        assert audio_out == b"PCM-PART-1-PCM-PART-2"
        assert fmt_out == "pcm_16k"
        assert one_arg_chunks == [b"PCM-PART-1-", b"PCM-PART-2"]

        two_arg_chunks: list[tuple[bytes, str]] = []
        await client.synthesize_once(
            "双参回调",
            output_format="mp3",
            on_chunk=lambda c, f: two_arg_chunks.append((c, f)),
            timeout=2.0,
        )
        assert two_arg_chunks == [(b"PCM-PART-1-", "mp3"), (b"PCM-PART-2", "mp3")]

        with pytest.raises(VoiceGatewayError) as synth_exc:
            await client.synthesize_once("fail-me", timeout=2.0)
        assert gw_secret not in str(synth_exc.value)
        assert gw_path not in str(synth_exc.value)

        # Session lifecycle + binary upload + text turn + cancel
        ready = await client.start_session(timeout=2.0)
        assert ready["session_id"] == "gw-sess-1"
        assert client.tts_model == "CosyVoice2"

        await client.send_audio_turn(
            b"AB" * 1500,
            fmt="pcm_16k",
            turn_id="gw-stream-1",
            speaker_id="spk-9",
            speaker_name="玩家A",
            stream=True,
            chunk_size=1024,
        )
        await asyncio.wait_for(turn_done.wait(), timeout=2.0)
        assert [name for name, _ in events] == [
            "stt.final",
            "turn.start",
            "text.chunk",
            "tts.start",
            "tts.chunk",
            "tts.end",
            "turn.end",
        ]
        assert events[4][1] == ("gw-stream-1", 1, "pcm_16k", b"STREAM-AUDIO-BYTES")
        assert any(isinstance(item, bytes) for item in sent_items)

        await client.send_audio_turn(b"RIFF-1", turn_id="gw-b64-1")
        await client.send_text_turn("文字发送", turn_id="gw-txt-1")
        await client.cancel_turn("gw-txt-1")
        assert any(
            isinstance(item, dict)
            and item.get("type") == "audio.turn"
            and item.get("turn_id") == "gw-b64-1"
            for item in sent_items
        )
        assert any(
            isinstance(item, dict)
            and item.get("type") == "text.turn"
            and item.get("turn_id") == "gw-txt-1"
            for item in sent_items
        )
        assert any(
            isinstance(item, dict)
            and item.get("type") == "turn.cancel"
            and item.get("turn_id") == "gw-txt-1"
            for item in sent_items
        )

        ended = await client.stop_session(timeout=2.0)
        assert ended["session_id"] == "gw-sess-1"
    finally:
        await client.close()


async def test_raw_stream_audio_player_streaming_fallback_and_cancel(tmp_path):
    assert _default_ffplay_stream_cmd("pcm_16k")[-8:] == [
        "-f",
        "s16le",
        "-ar",
        "16000",
        "-ac",
        "1",
        "-i",
        "pipe:0",
    ]
    assert _default_ffplay_stream_cmd("pcm_48k_stereo")[-8:] == [
        "-f",
        "s16le",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-i",
        "pipe:0",
    ]

    # 1. Streaming mode (live stdin pipe + on_first_audio + immediate cancel)
    sink_file = tmp_path / "raw_stream.bin"
    hold_file = tmp_path / "hold.flag"
    first_audio: list[tuple[str, float]] = []

    def stream_cmd_factory(fmt: str) -> list[str]:
        if fmt == "wav":
            script = (
                "import pathlib, sys, time; "
                "pathlib.Path(sys.argv[1]).write_text('started', encoding='utf-8'); "
                "sys.stdin.buffer.read(4); "
                "time.sleep(5.0)"
            )
            return [sys.executable, "-c", script, str(hold_file)]
        script = (
            "import pathlib, sys; "
            "data = sys.stdin.buffer.read(); "
            "pathlib.Path(sys.argv[1]).write_bytes(data)"
        )
        return [sys.executable, "-c", script, str(sink_file)]

    stream_player = RawStreamAudioPlayer(
        stream_cmd_factory=stream_cmd_factory,
        enable_streaming=True,
        on_first_audio=lambda tid, ms: first_audio.append((tid, ms)),
    )
    assert stream_player.streaming_active is True
    await stream_player.start()
    try:
        stream_player.begin_stream("raw-1", 1, "mp3")
        stream_player.push_chunk("raw-1", b"CHUNK-A-")
        stream_player.push_chunk("raw-1", b"CHUNK-B")
        stream_player.end_stream("raw-1", 1)
        await asyncio.wait_for(stream_player.wait_idle(), timeout=2.0)
        assert sink_file.read_bytes() == b"CHUNK-A-CHUNK-B"
        assert len(first_audio) == 1 and first_audio[0][0] == "raw-1"

        # Cancel active stream kills process immediately and drops subsequent chunks
        stream_player.begin_stream("raw-cancel", 1, "wav")
        stream_player.push_chunk("raw-cancel", b"HOLD")
        for _ in range(100):
            if hold_file.is_file() and stream_player.is_playing:
                break
            await asyncio.sleep(0.01)
        assert stream_player.is_playing is True
        await asyncio.wait_for(stream_player.cancel_turn("raw-cancel"), timeout=1.5)
        stream_player.push_chunk("raw-cancel", b"IGNORED-AFTER-CANCEL")
        await asyncio.wait_for(stream_player.wait_idle(), timeout=1.5)
        assert stream_player.is_playing is False
    finally:
        await stream_player.stop()

    # 2. Buffered afplay fallback wraps pcm_16k into WAV container
    afplay_out = tmp_path / "fallback.wav"

    def play_cmd_factory(path: Path) -> list[str]:
        script = "import pathlib, shutil, sys; shutil.copyfile(sys.argv[1], sys.argv[2])"
        return [sys.executable, "-c", script, str(path), str(afplay_out)]

    fallback_player = RawStreamAudioPlayer(
        play_cmd_factory=play_cmd_factory,
        enable_streaming=False,
    )
    assert fallback_player.streaming_active is False
    await fallback_player.start()
    try:
        pcm_payload = _pcm_frame(1000, samples=160)
        fallback_player.begin_stream("fb-1", 1, "pcm_16k")
        fallback_player.push_chunk("fb-1", pcm_payload[:160])
        fallback_player.push_chunk("fb-1", pcm_payload[160:])
        fallback_player.end_stream("fb-1", 1)
        await asyncio.wait_for(fallback_player.wait_idle(), timeout=2.0)
        wav_bytes = afplay_out.read_bytes()
        assert wav_bytes.startswith(b"RIFF")
        with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
            assert wf.getnchannels() == 1
            assert wf.getframerate() == 16000
            assert wf.readframes(wf.getnframes()) == pcm_payload
    finally:
        await fallback_player.stop()


def test_cli_direct_flag_and_gateway_check_ws(monkeypatch, capsys, tmp_path):
    gw_token = "gateway-cli-secret-token-32-chars-min"
    gw_path = "/ws/voice/secret-cli-gw-path"
    env_file = tmp_path / "cli_gw.env"
    env_file.write_text(
        "\n".join(
            [
                f"VOICE_GATEWAY_URL=wss://gw.example.invalid{gw_path}",
                f"VOICE_GATEWAY_TOKEN={gw_token}",
                "VOICE_GATEWAY_OUTPUT_FORMAT=mp3",
                "COMPANION_WS_URL=wss://direct.example.invalid/ws/companion/direct-secret",
                f"COMPANION_WS_TOKEN={COMPANION_TOKEN}",
            ]
        ),
        encoding="utf-8",
    )

    seen_calls: list[str] = []

    class FakeDirectForMain:
        def __init__(self, config: VoiceClientConfig, **kwargs: Any) -> None:
            _ = kwargs
            assert config.use_gateway is False
            seen_calls.append("direct")

        async def connect(self) -> None:
            return None

        async def start_session(self) -> dict[str, Any]:
            return {"mode": "owner_private", "session_id": "dir-sess-1"}

        async def ping(self, timeout: float = 10.0) -> float:  # noqa: ASYNC109
            _ = timeout
            return 9.1

        async def stop_session(self, timeout: float = 5.0) -> dict[str, Any]:  # noqa: ASYNC109
            _ = timeout
            return {"session_id": "dir-sess-1"}

        async def close(self) -> None:
            return None

    class FakeGwForMain:
        def __init__(self, config: VoiceClientConfig, **kwargs: Any) -> None:
            _ = kwargs
            assert config.use_gateway is True
            seen_calls.append("gateway")

        async def connect(self) -> None:
            return None

        async def start_session(self) -> dict[str, Any]:
            return {
                "mode": "owner_private",
                "session_id": "gw-sess-99",
                "stt_provider": "fish",
                "tts_provider": "siliconflow",
                "tts_model": "CosyVoice2",
            }

        async def ping_info(self, timeout: float = 10.0) -> dict[str, Any]:  # noqa: ASYNC109
            _ = timeout
            return {"rtt_ms": 14.2, "upstream_ms": 3.5}

        async def stop_session(self, timeout: float = 5.0) -> dict[str, Any]:  # noqa: ASYNC109
            _ = timeout
            return {"session_id": "gw-sess-99"}

        async def close(self) -> None:
            return None

    monkeypatch.setattr("voice_client.cli.CompanionWsClient", FakeDirectForMain)
    monkeypatch.setattr("voice_client.cli.VoiceGatewayWsClient", FakeGwForMain)

    assert main(["--env-file", str(env_file), "--check-ws"]) == 0
    gw_out = capsys.readouterr().out
    assert "语音网关握手正常" in gw_out
    assert "upstream=3.5ms" in gw_out
    assert "stt=fish" in gw_out
    assert "secret-cli-gw-path" not in gw_out

    assert main(["--env-file", str(env_file), "--direct", "--check-ws"]) == 0
    dir_out = capsys.readouterr().out
    assert "WS 握手正常" in dir_out
    assert seen_calls == ["gateway", "direct"]


async def test_cli_gateway_tts_stt_and_interactive_session(monkeypatch, capsys, tmp_path):
    gw_token = "gateway-cli-secret-token-32-chars-min"
    gw_path = "/ws/voice/secret-cli-gw-path"

    # Gateway --test-tts & --test-stt require NO local API keys
    gw_only_cfg = VoiceClientConfig(
        gateway_url=f"wss://gw.example.invalid{gw_path}",
        gateway_token=gw_token,
        gateway_output_format="mp3",
        siliconflow_api_key="",
        fish_api_key="",
        openai_stt_api_key="",
        ping_interval_seconds=0.0,
        vad_threshold=0.05,
        vad_silence_ms=90,
        vad_min_speech_ms=90,
        vad_barge_in_multiplier=2.0,
    )

    from voice_client.cli import test_stt, test_tts

    tts_sink = tmp_path / "cli_tts.mp3"

    class LocalRawPlayer(RawStreamAudioPlayer):
        def __init__(self, *, enable_streaming: bool = True, **kwargs: Any) -> None:
            _ = enable_streaming
            script = (
                "import pathlib, sys; "
                "pathlib.Path(sys.argv[1]).write_bytes(sys.stdin.buffer.read())"
            )
            super().__init__(
                stream_cmd_factory=lambda fmt: [sys.executable, "-c", script, str(tts_sink)],
                enable_streaming=True,
                **kwargs,
            )

    class CliGatewayWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str | bytes] = asyncio.Queue()

        async def send(self, data: str | bytes) -> None:
            if isinstance(data, (bytes, bytearray)):
                return
            frame = json.loads(data)
            typ = frame["type"]
            if typ == "tts.synthesize":
                req_id = frame["req_id"]
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "tts.start",
                            "req_id": req_id,
                            "seq": 1,
                            "format": "mp3",
                        }
                    )
                )
                await self.incoming.put(b"GW-CLI-MP3")
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "tts.end",
                            "req_id": req_id,
                            "seq": 1,
                            "bytes": 10,
                            "status": "ok",
                        }
                    )
                )
            elif typ == "stt.transcribe":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "stt.result",
                            "req_id": frame["req_id"],
                            "text": "网关识别结果喵",
                            "stt_ms": 21.0,
                        }
                    )
                )

        async def recv(self) -> str | bytes:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self.incoming.put("__EOF__")

    monkeypatch.setattr("voice_client.cli.RawStreamAudioPlayer", LocalRawPlayer)
    assert await test_tts(gw_only_cfg, "网关测试", connect_factory=CliGatewayWs) == 0
    assert tts_sink.read_bytes() == b"GW-CLI-MP3"
    assert "TTS 播放完成" in capsys.readouterr().out

    class FakeMicShort:
        def __init__(self, **kwargs: Any) -> None:
            _ = kwargs
            self._n = 0

        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            self._n += 1
            return b"" if self._n > 5 else _pcm_frame(2000)

        async def stop(self) -> None:
            return None

    monkeypatch.setattr("voice_client.cli.FfmpegMicCapture", FakeMicShort)
    assert await test_stt(gw_only_cfg, duration_seconds=0.1, connect_factory=CliGatewayWs) == 0
    assert "网关识别结果喵" in capsys.readouterr().out

    # Interactive gateway session A: [t] text.turn + [x] turn.cancel (EOF awaits bg_tasks)
    monkeypatch.setattr(sys, "stdin", io.StringIO("t\n文字问好\nx\n"))
    sess_sent: list[dict[str, Any]] = []

    class IdleGwMic:
        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            await asyncio.sleep(0.05)
            return _silence_frame()

        async def stop(self) -> None:
            return None

    class SessPlayer:
        def __init__(self, *, keep_playing_until_cancel: bool = False) -> None:
            self.is_playing = False
            self.keep_playing_until_cancel = keep_playing_until_cancel
            self.begun: list[tuple[str, int, str]] = []
            self.chunks: list[tuple[str, bytes]] = []
            self.ended: list[tuple[str, int]] = []
            self.cancelled = 0

        async def start(self) -> None:
            return None

        def begin_stream(self, turn_id: str, seq: int, fmt: str) -> None:
            self.is_playing = True
            self.begun.append((turn_id, seq, fmt))

        def push_chunk(self, turn_id: str, chunk: bytes) -> None:
            self.chunks.append((turn_id, chunk))

        def end_stream(self, turn_id: str, seq: int) -> None:
            if not self.keep_playing_until_cancel:
                self.is_playing = False
            self.ended.append((turn_id, seq))

        async def cancel_turn(self, turn_id: str | None = None) -> None:
            _ = turn_id
            self.cancelled += 1
            self.is_playing = False

        async def stop(self) -> None:
            return None

    class InteractiveGwWs:
        def __init__(self) -> None:
            self.incoming: asyncio.Queue[str | bytes] = asyncio.Queue()

        async def send(self, data: str | bytes) -> None:
            if isinstance(data, (bytes, bytearray)):
                return
            frame = json.loads(data)
            sess_sent.append(frame)
            typ = frame["type"]
            if typ == "session.start":
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "session.ready",
                            "session_id": "gw-int-1",
                            "mode": "owner_private",
                            "output_format": "mp3",
                            "stt_provider": "fish",
                            "tts_provider": "siliconflow",
                            "tts_model": "CosyVoice2",
                        }
                    )
                )
            elif typ in ("audio.turn", "text.turn"):
                tid = frame["turn_id"]
                if typ == "audio.turn":
                    await self.incoming.put(
                        json.dumps(
                            {
                                "type": "stt.final",
                                "turn_id": tid,
                                "text": "语音提问",
                                "stt_ms": 19.0,
                            }
                        )
                    )
                await self.incoming.put(
                    json.dumps({"type": "turn.start", "turn_id": tid, "kind": "speech"})
                )
                await self.incoming.put(
                    json.dumps({"type": "text.chunk", "turn_id": tid, "seq": 1, "text": "收到喵!"})
                )
                await self.incoming.put(
                    json.dumps({"type": "tts.start", "turn_id": tid, "seq": 1, "format": "mp3"})
                )
                await self.incoming.put(b"MP3-FRAME")
                await self.incoming.put(
                    json.dumps(
                        {"type": "tts.end", "turn_id": tid, "seq": 1, "bytes": 9, "status": "ok"}
                    )
                )
                await self.incoming.put(
                    json.dumps(
                        {
                            "type": "turn.end",
                            "turn_id": tid,
                            "status": "spoken",
                            "metrics": {"stt_ms": 19.0, "llm_ms": 40.0, "tts_ttfb_ms": 11.0},
                        }
                    )
                )
            elif typ == "session.stop":
                await self.incoming.put(
                    json.dumps({"type": "session.ended", "session_id": "gw-int-1"})
                )

        async def recv(self) -> str | bytes:
            item = await self.incoming.get()
            if item == "__EOF__":
                raise RuntimeError("eof")
            return item

        async def close(self) -> None:
            await self.incoming.put("__EOF__")

    sess_player = SessPlayer()
    assert (
        await run_interactive_session(
            gw_only_cfg,
            connect_factory=InteractiveGwWs,
            mic_capture=IdleGwMic(),
            audio_player=sess_player,
        )
        == 0
    )
    sess_out = capsys.readouterr().out
    assert "🐱 哈基米: 收到喵!" in sess_out
    assert "⏱️ 耗时:" in sess_out
    assert any(f["type"] == "text.turn" and f["text"] == "文字问好" for f in sess_sent)
    assert any(f["type"] == "turn.cancel" for f in sess_sent)
    assert sess_player.cancelled >= 1
    assert any(c[1] == b"MP3-FRAME" for c in sess_player.chunks)

    # Interactive gateway session B: VAD audio.turn + barge-in turn.cancel
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    sess_sent.clear()
    vad_frames = [
        *([_pcm_frame(4000)] * 4),
        *([_silence_frame()] * 3),
        *([_pcm_frame(5000)] * 2),
    ]

    class VadGwMic:
        def __init__(self) -> None:
            self._idx = 0

        async def start(self) -> None:
            return None

        async def read_frame(self) -> bytes:
            if self._idx == 7:
                await asyncio.sleep(0.08)
            else:
                await asyncio.sleep(0.01)
            if self._idx < len(vad_frames):
                f = vad_frames[self._idx]
                self._idx += 1
                return f
            await asyncio.sleep(0.08)
            return b""

        async def stop(self) -> None:
            return None

    vad_player = SessPlayer(keep_playing_until_cancel=True)
    assert (
        await run_interactive_session(
            gw_only_cfg,
            connect_factory=InteractiveGwWs,
            mic_capture=VadGwMic(),
            audio_player=vad_player,
        )
        == 0
    )
    vad_out = capsys.readouterr().out
    assert "🎤 你: 语音提问" in vad_out
    assert "🐱 哈基米: 收到喵!" in vad_out
    assert any(f["type"] == "audio.turn" for f in sess_sent)
    assert any(f["type"] == "turn.cancel" for f in sess_sent)
    assert vad_player.cancelled >= 1


async def test_breezeblue_config_provider_streaming_and_redaction(tmp_path):
    missing = tmp_path / "none.env"
    with pytest.raises(ValueError, match="BREEZEBLUE_API_KEY"):
        VoiceClientConfig(tts_provider="breezeblue", breezeblue_voice_id="voc_123").require_tts()
    with pytest.raises(ValueError, match="BREEZEBLUE_VOICE_ID"):
        VoiceClientConfig(
            tts_provider="breeze", breezeblue_api_key="brz_secret_123456"
        ).require_tts()

    cfg = load_config(
        env={
            "TTS_PROVIDER": "breezeblue",
            "BREEZEBLUE_API_KEY": "brz_secret_key_abcdef12",
            "BREEZEBLUE_BASE_URL": "https://api.breeze.blue/v1/",
            "BREEZEBLUE_VOICE_ID": "voc_hakimi_001",
            "BREEZEBLUE_MODEL_ID": "breeze-tts-2-multilingual",
            "BREEZEBLUE_LANGUAGE_CODE": "",
            "BREEZEBLUE_INSTRUCTIONS": "温柔娇俏地说话",
            "BREEZEBLUE_GUIDANCE_SCALE": "15.0",
            "BREEZEBLUE_SPEAKER_BOOST": "true",
            "TTS_SPEED": "1.1",
            "TTS_FORMAT": "wav",
        },
        env_file=missing,
    )
    cfg.require_tts()
    assert cfg.breezeblue_guidance_scale == 10.0
    assert cfg.breezeblue_speaker_boost is True
    assert cfg.breezeblue_language_code == ""

    seen: list[tuple[str, str, dict[str, str], bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, str(request.url), dict(request.headers), request.content))
        if request.url.path == "/v1/voices":
            return httpx.Response(
                200, json={"voices": [{"voice_id": "voc_hakimi_001", "name": "哈基米"}]}
            )
        if request.url.path == "/v1/text-to-speech/voc_bad/stream":
            return httpx.Response(
                401,
                text="bad key brz_secret_key_abcdef12 and brz_another_leaked_key",
            )
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, content=b"BRZ-STREAM-1BRZ-STREAM-2")
        return httpx.Response(200, content=b"RIFF-BRZ-WAV")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        provider = build_tts_provider(cfg, http=http)
        assert isinstance(provider, BreezeBlueTTS)
        audio, fmt = await provider.synthesize("早安喵~")
        assert audio == b"RIFF-BRZ-WAV"
        assert fmt == "wav"
        method, url, hdrs, body = seen[-1]
        assert method == "POST"
        assert (
            url
            == "https://api.breeze.blue/v1/text-to-speech/voc_hakimi_001?output_format=wav_24000"
        )
        assert hdrs["authorization"] == "Bearer brz_secret_key_abcdef12"
        assert json.loads(body.decode("utf-8")) == {
            "text": "早安喵~",
            "model_id": "breeze-tts-2-multilingual",
            "instructions": "温柔娇俏地说话",
            "voice_settings": {
                "speed": 1.1,
                "guidance_scale": 10.0,
                "use_speaker_boost": True,
            },
        }

        mp3_provider = BreezeBlueTTS(
            api_key="brz_secret_key_abcdef12",
            voice_id="voc_hakimi_001",
            response_format="mp3",
            speed=1.0,
            http=http,
        )
        streamed = [c async for c in mp3_provider.synthesize_stream("测试流式")]
        assert b"".join(streamed) == b"BRZ-STREAM-1BRZ-STREAM-2"
        _, surl, _, sbody = seen[-1]
        assert (
            surl
            == "https://api.breeze.blue/v1/text-to-speech/voc_hakimi_001/stream?output_format=mp3"
        )
        assert "voice_settings" not in json.loads(sbody.decode("utf-8"))

        voices = await mp3_provider.list_voices()
        assert voices == [{"voice_id": "voc_hakimi_001", "name": "哈基米"}]

        bad_provider = BreezeBlueTTS(
            api_key="brz_secret_key_abcdef12",
            voice_id="voc_bad",
            http=http,
        )
        with pytest.raises(ProviderError) as exc_info:
            await anext(bad_provider.synthesize_stream("fail"))
        err_msg = str(exc_info.value)
        assert "brz_secret_key_abcdef12" not in err_msg
        assert "brz_another_leaked_key" not in err_msg
        assert "brz_[REDACTED]" in err_msg or "[REDACTED]" in err_msg


async def test_playback_cursor_tracking_and_cancel_turn_cursor_payload(tmp_path):
    from voice_client.audio import AfplayAudioPlayer, RawStreamAudioPlayer

    fake_player = tmp_path / "fake_play.py"
    fake_player.write_text(
        "import sys, time\npath = sys.argv[1]\nif '2.' in path:\n    time.sleep(0.5)\n",
        encoding="utf-8",
    )

    second_started = asyncio.Event()

    class SlowTts:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            if text == "second":
                await asyncio.sleep(0.02)
            return b"ID3-DATA", "mp3"

    def on_first(tid: str, _ms: float) -> None:
        if tid == "t-cur" and player.playback_cursor("t-cur")["played_seq"] >= 2:
            second_started.set()

    player = AfplayAudioPlayer(
        SlowTts(),
        play_cmd_factory=lambda p: [sys.executable, str(fake_player), str(p)],
        enable_streaming=False,
        on_first_audio=on_first,
    )
    await player.start()
    try:
        player.enqueue_chunk("t-cur", 1, "first")
        player.enqueue_chunk("t-cur", 2, "second")
        assert player.playback_cursor("t-cur")["played_seq"] == 0
        await asyncio.wait_for(second_started.wait(), timeout=2.0)
        cur = player.playback_cursor("t-cur")
        assert cur["played_seq"] == 2
        assert cur["completed_seq"] >= 1
        await player.cancel_turn("t-cur")
    finally:
        await player.stop()

    raw_player = RawStreamAudioPlayer(
        play_cmd_factory=lambda p: [sys.executable, str(fake_player), str(p)],
        enable_streaming=False,
    )
    await raw_player.start()
    try:
        raw_player.begin_stream("t-raw", 1, "mp3")
        raw_player.push_chunk("t-raw", b"ID3-1")
        raw_player.end_stream("t-raw", 1)
        await raw_player.wait_idle()
        cur_raw = raw_player.playback_cursor("t-raw")
        assert cur_raw["played_seq"] == 1
        assert cur_raw["completed_seq"] == 1
    finally:
        await raw_player.stop()

    sent_frames: list[dict[str, Any]] = []

    class DummyWs:
        async def send(self, raw: str) -> None:
            sent_frames.append(json.loads(raw))

        async def recv(self) -> str:
            await asyncio.sleep(10)
            return "{}"

        async def close(self) -> None:
            pass

    cfg = VoiceClientConfig(ws_url="wss://example.com/ws", ws_token="tok")
    cws = CompanionWsClient(cfg, connect_factory=lambda *a, **kw: DummyWs())
    await cws.connect()
    try:
        await cws.cancel_turn("t-1")
        assert sent_frames[-1] == {"type": "turn.cancel", "turn_id": "t-1"}
        await cws.cancel_turn("t-2", played_seq=2, played_ms=850)
        assert sent_frames[-1] == {
            "type": "turn.cancel",
            "turn_id": "t-2",
            "played_seq": 2,
            "played_ms": 850,
        }
    finally:
        await cws.close()
