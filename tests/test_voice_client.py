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
    EnergyVadDetector,
    FishAudioSTT,
    FishAudioTTS,
    OpenAICompatibleSTT,
    ProviderError,
    SiliconFlowSTT,
    SiliconFlowTTS,
    VoiceClientConfig,
    list_avfoundation_audio_devices,
    load_config,
    load_dotenv_file,
    pcm16_rms,
    pcm16_to_wav_bytes,
    strip_stt_markup,
)
from voice_client.cli import check_ws, main, run_interactive_session
from voice_client.companion_ws import CompanionWsClient, CompanionWsError


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
