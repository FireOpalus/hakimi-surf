"""Comprehensive tests for native QQ voice direct-to-Gemini pipeline."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import shutil
import socket
import subprocess
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from hakimi.adapters import onebot_message
from hakimi.audio_media import (
    MAX_AUDIO_BYTES,
    MAX_INLINE_AUDIO_BYTES,
    AudioLoader,
    ModelAudio,
    audio_tokens,
    format_audio_memory_tag,
    normalize_audio,
    valid_record_file_id,
    validate_audio_url,
)
from hakimi.bot_server import assemble_chat_input, prompt_context
from hakimi.gemini import GeminiGateway
from hakimi.media import PublicResolver
from hakimi.message_context import MessageContextResolver
from hakimi.models import AudioRef, Message
from hakimi.onebot_transport import ACTIONS, HTTPTransport, WebSocketTransport, validate_action
from hakimi.outbound_actions import (
    ACTION_INSTRUCTION,
    CHAT_RESPONSE_FORMAT,
    MAX_AUDIO_GIST_CHARS,
    ChatActions,
    parse_actions,
)
from hakimi.persona import SYSTEM_INSTRUCTION
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.rich_message_parser import parse_forward, parse_message
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.video_files import ModelVideo
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_media import Client, Response
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_v20_phase1_modules_and_world_state import V181_STATIC_PREFIX_DIGESTS

HAS_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def _make_audio_bytes(fmt: str, *, duration: float = 1.2, metadata: dict[str, str] | None = None):
    ffmpeg = shutil.which("ffmpeg")
    assert ffmpeg is not None
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / f"sample.{fmt}"
        cmd = [
            ffmpeg,
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=440:duration={duration}",
        ]
        if fmt == "wav":
            cmd.extend(["-ar", "44100", "-ac", "2"])
        elif fmt == "ogg":
            cmd.extend(["-c:a", "libopus", "-ar", "48000", "-ac", "1"])
        elif fmt == "mp3":
            cmd.extend(["-c:a", "libmp3lame", "-ar", "44100", "-ac", "2", "-b:a", "128k"])
        for k, v in (metadata or {}).items():
            cmd.extend(["-metadata", f"{k}={v}"])
        cmd.append(str(out))
        subprocess.run(cmd, check=True, capture_output=True)
        return out.read_bytes()


def _probe_bytes(raw: bytes) -> dict:
    ffprobe = shutil.which("ffprobe")
    assert ffprobe is not None
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "out.mp3"
        path.write_bytes(raw)
        proc = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-protocol_whitelist",
                "file",
                "-show_entries",
                "format=duration,format_name,bit_rate,tags:stream=codec_type,codec_name,sample_rate,channels,duration",
                "-of",
                "json",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(proc.stdout)


def _action_json(**overrides) -> str:
    payload = {
        "abstain": False,
        "bubbles": ["听到了喵"],
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
    }
    payload.update(overrides)
    return json.dumps(payload, ensure_ascii=False)


def _sample_audio(duration_sec: float = 12.1, raw: bytes = b"ID3fake-mp3-audio") -> ModelAudio:
    return ModelAudio(
        data=base64.b64encode(raw).decode("ascii"),
        mime_type="audio/mp3",
        duration_sec=duration_sec,
        byte_size=len(raw),
        digest=hashlib.sha256(raw).hexdigest(),
    )


class Wire:
    def __init__(self, responses):
        self.responses = list(responses) if isinstance(responses, list) else responses
        self.calls: list[tuple[str, dict]] = []

    async def call(self, action: str, params: dict):
        self.calls.append((action, dict(params)))
        if isinstance(self.responses, list):
            value = self.responses.pop(0)
        else:
            key = (action, params.get("out_format"))
            value = self.responses.get(key, self.responses.get(action))
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise Rejected("onebot_rpc_failed")
        return value


class FakeAudioLoader:
    def __init__(self, result_map: dict[str, ModelAudio | None] | None = None):
        self.result_map = result_map or {}
        self.calls: list[list[AudioRef]] = []

    def failure_status(self, ref: AudioRef) -> str:
        return "oversized" if ref.file_size and ref.file_size > MAX_AUDIO_BYTES else "unavailable"

    async def load_results(
        self, refs: list[AudioRef], *, scope: str = "legacy", max_total_bytes: int = MAX_AUDIO_BYTES
    ) -> list[ModelAudio | None]:
        self.calls.append(list(refs))
        out: list[ModelAudio | None] = []
        for ref in refs:
            if ref.identity in self.result_map:
                out.append(self.result_map[ref.identity])
            else:
                out.append(_sample_audio(duration_sec=12.0))
        return out


# ---------------------------------------------------------------------------
# 1. OneBot transport whitelist & strict file/out_format validation
# ---------------------------------------------------------------------------


def test_onebot_transport_get_record_whitelist_and_validation(settings):
    assert "get_record" in ACTIONS
    validate_action(settings, "get_record", {"file": "abc123.amr", "out_format": "mp3"})
    validate_action(
        settings,
        "get_record",
        {"file": "{E4A3F1B2-1234-5678-9ABC-DEF012345678}.silk", "out_format": "wav"},
    )
    validate_action(settings, "get_record", {"file": "record_01.ogg"})

    for bad_params in (
        {"file": "/etc/passwd", "out_format": "mp3"},
        {"file": "/app/napcat/temp/a.amr", "out_format": "mp3"},
        {"file": "../secret.amr", "out_format": "mp3"},
        {"file": "a..b.amr", "out_format": "mp3"},
        {"file": "file:///etc/passwd", "out_format": "mp3"},
        {"file": "https://evil.test/a.mp3", "out_format": "mp3"},
        {"file": "base64://YWJj", "out_format": "mp3"},
        {"file": "a;rm -rf /", "out_format": "mp3"},
        {"file": "", "out_format": "mp3"},
        {"file": "abc.amr", "out_format": "sh"},
        {"file": "abc.amr", "out_format": "../mp3"},
    ):
        with pytest.raises(Rejected, match="onebot_action_forbidden"):
            validate_action(settings, "get_record", bad_params)


async def test_http_and_ws_transport_allow_get_record(settings):
    # HTTPTransport allows base64 get_record responses larger than max_body_bytes
    big_b64 = base64.b64encode(b"x" * 160_000).decode("ascii")

    def http_handler(request: httpx.Request):
        assert request.url.path.endswith("/get_record")
        body = json.loads(request.content.decode())
        assert body == {"file": "voice1.amr", "out_format": "mp3"}
        return httpx.Response(200, json={"status": "ok", "retcode": 0, "data": {"base64": big_b64}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
        transport = HTTPTransport(settings, client)
        res = await transport.call("get_record", {"file": "voice1.amr", "out_format": "mp3"})
        assert res["base64"] == big_b64

    # WebSocketTransport dispatches get_record cleanly
    ws_transport = WebSocketTransport(settings)
    inbound: asyncio.Queue[dict] = asyncio.Queue()
    outbound: asyncio.Queue[dict] = asyncio.Queue()

    async def close(code):
        pass

    async def on_event(_):
        return "accepted"

    async def send(raw):
        await outbound.put(json.loads(raw))

    async def receive():
        return json.dumps(await inbound.get())

    task = asyncio.create_task(ws_transport.serve(receive, send, close, on_event))
    try:
        login = await asyncio.wait_for(outbound.get(), 2)
        await inbound.put(
            {"echo": login["echo"], "status": "ok", "retcode": 0, "data": {"user_id": 10000}}
        )
        await asyncio.wait_for(ws_transport.ready.wait(), 2)
        rpc_task = asyncio.create_task(
            ws_transport.call("get_record", {"file": "v1.amr", "out_format": "mp3"})
        )
        req = await asyncio.wait_for(outbound.get(), 2)
        assert req["action"] == "get_record"
        assert req["params"] == {"file": "v1.amr", "out_format": "mp3"}
        await inbound.put(
            {"echo": req["echo"], "status": "ok", "retcode": 0, "data": {"base64": "YWJj"}}
        )
        assert await asyncio.wait_for(rpc_task, 2) == {"base64": "YWJj"}
    finally:
        await ws_transport.close()
        await asyncio.gather(task, return_exceptions=True)


# ---------------------------------------------------------------------------
# 2. Real ffmpeg/ffprobe transcoding, metadata stripping, SSRF & local-path block
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
@pytest.mark.parametrize("src_fmt", ["wav", "ogg", "mp3"])
def test_real_ffmpeg_normalizes_to_16khz_mono_32kbps_mp3_and_strips_metadata(src_fmt):
    raw = _make_audio_bytes(
        src_fmt,
        duration=1.5,
        metadata={"title": "EVIL_PROMPT_INJECTION", "comment": "SECRET_TOKEN_99999"},
    )
    normalized = normalize_audio(raw)
    assert normalized.mime_type == "audio/mp3"
    assert 1.3 <= normalized.duration_sec <= 1.7
    assert normalized.tokens == math.ceil(normalized.duration_sec * 35)
    out_bytes = normalized.raw_bytes()
    assert b"EVIL_PROMPT_INJECTION" not in out_bytes
    assert b"SECRET_TOKEN_99999" not in out_bytes

    info = _probe_bytes(out_bytes)
    streams = [s for s in info["streams"] if s.get("codec_type") == "audio"]
    assert len(streams) == 1
    assert streams[0]["codec_name"] == "mp3"
    assert int(streams[0]["sample_rate"]) == 16000
    assert int(streams[0]["channels"]) == 1
    assert not info.get("format", {}).get("tags")


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
def test_normalize_audio_rejects_corrupt_playlist_and_timeout(monkeypatch):
    for bad in (
        b"",
        b"<html>not audio</html>",
        b"#EXTM3U\n#EXT-X-TARGETDURATION:10\nhttp://127.0.0.1/secret\n#EXT-X-ENDLIST\n",
        b"ffconcat version 1.0\nfile '/etc/passwd'\n",
    ):
        with pytest.raises(Rejected, match="invalid_audio"):
            normalize_audio(bad)

    with pytest.raises(Rejected, match="audio_too_large"):
        normalize_audio(b"a" * (MAX_AUDIO_BYTES + 1))

    # Subprocess timeout is caught and converted to Rejected("audio_transcode_timeout")
    wav = _make_audio_bytes("wav", duration=0.5)

    def _boom(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=1)

    monkeypatch.setattr(subprocess, "run", _boom)
    with pytest.raises(Rejected, match="audio_transcode_timeout"):
        normalize_audio(wav)


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://grouptalk.c2c.qq.com/voice.mp3",
        "https://127.0.0.1/voice.mp3",
        "https://169.254.169.254/latest/meta-data",
        "https://10.0.0.1/voice.mp3",
        "https://[::1]/voice.mp3",
        "https://[::ffff:127.0.0.1]/voice.mp3",
        "https://localhost/voice.mp3",
        "https://napcat/voice.mp3",
        "https://service.internal/voice.mp3",
        "https://cdn.example.com:8443/voice.mp3",
        "https://user:pass@cdn.example.com/voice.mp3",
        "https://cdn.example.com/voice.mp3#frag",
        "file:///etc/passwd",
        "base64://YWJj",
    ],
)
def test_audio_url_validation_rejects_ssrf_and_non_https(bad_url):
    with pytest.raises(Rejected, match="audio_origin_forbidden"):
        validate_audio_url(bad_url)


def test_audio_url_and_file_id_validation_accept_safe_inputs():
    assert validate_audio_url("https://grouptalk.c2c.qq.com/download?id=1").startswith("https://")
    assert valid_record_file_id("abc_123.amr") == "abc_123.amr"
    assert valid_record_file_id("{ABCD-1234}.silk") == "{ABCD-1234}.silk"
    assert valid_record_file_id("/etc/passwd") == ""
    assert valid_record_file_id("../secret.amr") == ""
    assert valid_record_file_id("file:///etc/passwd") == ""


@pytest.mark.skipif(not HAS_FFMPEG, reason="ffmpeg/ffprobe not installed")
async def test_audio_loader_never_reads_local_path_and_blocks_redirect_ssrf(
    settings, tmp_path, monkeypatch
):
    real_mp3 = _make_audio_bytes("mp3", duration=1.0)
    secret_local_file = tmp_path / "local_voice.mp3"
    secret_local_file.write_bytes(real_mp3)

    # 1. NapCat returns ONLY local filesystem paths -> MUST NEVER open secret_local_file!
    local_only_wire = Wire(
        {
            ("get_record", "mp3"): {"file": str(secret_local_file), "path": str(secret_local_file)},
            ("get_record", "wav"): {"file": str(secret_local_file)},
            ("get_record", "ogg"): {"path": str(secret_local_file)},
            ("get_record", "amr"): {"file_path": str(secret_local_file)},
            "get_file": {"file": str(secret_local_file)},
        }
    )
    loader = AudioLoader(settings, local_only_wire)
    monkeypatch.setattr(loader, "_client", lambda: Client([]))
    res = await loader.load_results([AudioRef(file="voice_id.amr")])
    assert res == [None]
    assert local_only_wire.calls[0] == ("get_record", {"file": "voice_id.amr", "out_format": "mp3"})

    # 2. NapCat returns base64 -> succeeds without touching HTTP
    b64_wire = Wire(
        {("get_record", "mp3"): {"base64": "base64://" + base64.b64encode(real_mp3).decode()}}
    )
    loader2 = AudioLoader(settings, b64_wire)
    client2 = Client([])
    monkeypatch.setattr(loader2, "_client", lambda: client2)
    res2 = await loader2.load_results([AudioRef(file="voice_ok.amr")])
    assert len(res2) == 1 and res2[0] is not None
    assert res2[0].mime_type == "audio/mp3"
    assert not client2.requests
    assert b64_wire.calls == [("get_record", {"file": "voice_ok.amr", "out_format": "mp3"})]

    # 3. HTTPS redirect to internal IP or HTTP downgrade is blocked!
    class RedirectResp(Response):
        def __init__(self, location):
            super().__init__(b"", status=302, content_type="audio/mpeg")
            self.headers = {"Location": location}

    loader3 = AudioLoader(settings, None)
    monkeypatch.setattr(
        loader3, "_client", lambda: Client([RedirectResp("https://127.0.0.1/internal.mp3")])
    )
    assert await loader3.load_results([AudioRef(url="https://grouptalk.c2c.qq.com/a.mp3")]) == [
        None
    ]

    monkeypatch.setattr(
        loader3, "_client", lambda: Client([RedirectResp("http://grouptalk.c2c.qq.com/a.mp3")])
    )
    assert await loader3.load_results([AudioRef(url="https://grouptalk.c2c.qq.com/a.mp3")]) == [
        None
    ]

    # 4. Fallback chain: mp3 RPC failure -> wav RPC base64 -> normalized to 16kHz mono mp3
    real_wav = _make_audio_bytes("wav", duration=1.1)
    fallback_wire = Wire(
        {
            ("get_record", "mp3"): Rejected("onebot_rpc_failed"),
            ("get_record", "wav"): {"base64": base64.b64encode(real_wav).decode()},
        }
    )
    loader_fb = AudioLoader(settings, fallback_wire)
    monkeypatch.setattr(loader_fb, "_client", lambda: Client([]))
    res_fb = await loader_fb.load_results([AudioRef(file="fb_wav.silk")])
    assert len(res_fb) == 1 and res_fb[0] is not None
    assert res_fb[0].mime_type == "audio/mp3"
    assert fallback_wire.calls == [
        ("get_record", {"file": "fb_wav.silk", "out_format": "mp3"}),
        ("get_record", {"file": "fb_wav.silk", "out_format": "wav"}),
    ]

    # 5. HTTPS URL returned by get_record -> fetched via pinned client & transcoded
    real_ogg = _make_audio_bytes("ogg", duration=1.0)
    url_wire = Wire({("get_record", "mp3"): {"url": "https://grouptalk.c2c.qq.com/voice_ogg.ogg"}})
    loader_url = AudioLoader(settings, url_wire)
    url_client = Client([Response(real_ogg, status=200, content_type="audio/ogg")])
    monkeypatch.setattr(loader_url, "_client", lambda: url_client)
    res_url = await loader_url.load_results([AudioRef(file="ogg_url.silk")])
    assert len(res_url) == 1 and res_url[0] is not None
    assert res_url[0].mime_type == "audio/mp3"
    assert len(url_client.requests) == 1
    assert url_client.requests[0][0] == "https://grouptalk.c2c.qq.com/voice_ogg.ogg"

    # 6. Oversized declared file_size skips immediately without fallback RPCs
    big_wire = Wire({("get_record", "mp3"): {"file_size": str(MAX_AUDIO_BYTES + 1)}})
    loader_big = AudioLoader(settings, big_wire)
    monkeypatch.setattr(loader_big, "_client", lambda: Client([]))
    assert await loader_big.load_results([AudioRef(file="huge.amr")]) == [None]
    assert len(big_wire.calls) == 1

    # 7. No business count cap: 5 voices in one turn all load when under 20 MiB total
    multi_wire = Wire([{"base64": base64.b64encode(real_mp3).decode()} for _ in range(5)])
    loader4 = AudioLoader(settings, multi_wire)
    monkeypatch.setattr(loader4, "_client", lambda: Client([]))
    refs = [AudioRef(file=f"multi_{i}.amr") for i in range(5)]
    res4 = await loader4.load_results(refs)
    assert len(res4) == 5 and all(item is not None for item in res4)


async def test_audio_resolver_pins_public_ip_and_rejects_internal_dns(monkeypatch):
    async def internal_lookup(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", internal_lookup)
    with pytest.raises(Rejected, match="image_address_forbidden"):
        await PublicResolver().resolve("grouptalk.c2c.qq.com", 443)


# ---------------------------------------------------------------------------
# 3. Parser, OneBot adapter, and MessageContextResolver for record segments
# ---------------------------------------------------------------------------


def test_parse_record_segments_and_reject_unsafe_file_paths(settings):
    rich = parse_message(
        [
            {"type": "record", "data": {"file": "voice_1.amr", "file_size": "24000"}},
            {"type": "record", "data": {"file": "/etc/passwd", "url": "http://127.0.0.1/a.mp3"}},
            {
                "type": "record",
                "data": {"file": "../escape.amr", "url": "https://grouptalk.c2c.qq.com/ok.mp3"},
            },
        ],
        "10000",
    )
    assert rich.text == "[语音]\n[语音]\n[语音]"
    assert rich.command_text == ""
    assert len(rich.audios) == 2
    assert rich.audios[0].file == "voice_1.amr" and rich.audios[0].file_size == 24000
    assert rich.audios[1].file == "" and rich.audios[1].url == "https://grouptalk.c2c.qq.com/ok.mp3"

    # CQ string form
    cq = parse_message("[CQ:record,file=cq_voice.silk]", "10000")
    assert cq.text == "[语音]" and cq.command_text == ""
    assert len(cq.audios) == 1 and cq.audios[0].file == "cq_voice.silk"

    # onebot_message preserves voice-only private message
    msg = onebot_message(
        {
            "post_type": "message",
            "message_type": "private",
            "self_id": 10000,
            "user_id": 20000,
            "message_id": 77,
            "time": time.time(),
            "message": [{"type": "record", "data": {"file": "v77.amr"}}],
        },
        settings,
    )
    assert msg is not None
    assert msg.text == "[语音]"
    assert msg.command_text == ""
    assert msg.addressed is True
    assert len(msg.audios) == 1 and msg.audios[0].file == "v77.amr"

    # Forward propagates audios with author attribution
    fwd = parse_forward(
        {
            "messages": [
                {
                    "sender": {"nickname": "小王", "user_id": 30000},
                    "content": [{"type": "record", "data": {"file": "fwd_v1.amr"}}],
                }
            ]
        },
        "10000",
        include_authors=True,
    )
    assert len(fwd.audios) == 1
    assert fwd.audios[0].sender_id == "30000"
    assert fwd.audios[0].origin == "转发记录（作者声明）"


async def test_context_resolver_propagates_quoted_audio(settings, message):
    quoted = message.model_copy(
        update={
            "event_id": "500",
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "sender_nickname": "小明",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="quoted_v1.amr")],
        }
    )
    current = message.model_copy(
        update={
            "event_id": "501",
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "20000",
            "text": "[引用消息]\n@哈基米 听听他说啥",
            "reply_ids": ["500"],
            "addressed": True,
        }
    )

    async def lookup(_msg, ref_id):
        return quoted if ref_id == "500" else None

    resolver = MessageContextResolver(settings, None, lookup=lookup)
    resolved = await resolver.resolve(current)
    assert len(resolved.audios) == 1
    assert resolved.audios[0].file == "quoted_v1.amr"
    assert resolved.audios[0].origin == "引用消息"
    assert resolved.audios[0].sender_id == "30000"


# ---------------------------------------------------------------------------
# 4. Gateway inline audio, budget ceil(duration*35), Vertex, video mix block
# ---------------------------------------------------------------------------


async def test_gateway_screen_and_generate_inline_audio_and_budget(
    settings, store, sdk_pair, message
):
    seen_bodies: list[tuple[str, dict]] = []
    reserved: list[tuple[str, str, Decimal]] = []
    orig_reserve = store.reserve

    async def spy_reserve(subject, lane, amount, *args, **kwargs):
        reserved.append((subject, lane, amount))
        return await orig_reserve(subject, lane, amount, *args, **kwargs)

    store.reserve = spy_reserve

    def handler(request: httpx.Request):
        body = json.loads(request.content.decode("utf-8"))
        seen_bodies.append((request.url.path, body))
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _action_json(audio_gist="BOISER问今晚吃啥")
        return httpx.Response(200, json=resp)

    gw = GeminiGateway(settings, store, *sdk_pair(handler))
    audio = _sample_audio(duration_sec=12.1)
    assert audio.tokens == math.ceil(12.1 * 35) == 424
    assert audio_tokens(12.1) == 424

    msg = message.model_copy(update={"text": "[语音]", "command_text": ""})
    route = await gw.screen(
        {"current_message": "[语音]"},
        "subj-audio",
        purpose="message",
        message=msg,
        audios=[audio],
    )
    assert route == "chat"
    assert len(seen_bodies) == 1
    dec_input = seen_bodies[0][1]["input"]
    assert isinstance(dec_input, list)
    assert any(
        item.get("type") == "audio"
        and item.get("mime_type") == "audio/mp3"
        and item.get("data") == audio.data
        for item in dec_input
    )

    # Exact ceil(duration * 35) reservation delta on decision lane
    reserved_with_audio = reserved[-1][2]
    await gw.screen(
        {"current_message": "[语音]"},
        "subj-audio",
        purpose="message",
        message=msg,
    )
    reserved_no_audio = reserved[-1][2]
    expected_delta = Decimal(424) * settings.decision_input_usd_per_m / Decimal(1_000_000)
    assert reserved_with_audio - reserved_no_audio == expected_delta

    # Generate with audio (interactions.create) + audio_gist
    out = await gw.generate(
        {
            "current_message": "[语音]",
            "audio_sources": [{"audio_number": 1, "status": "available", "duration_sec": 12.1}],
        },
        "subj-audio",
        message=msg,
        audios=[audio],
    )
    parsed = ChatActions.model_validate_json(out)
    assert parsed.audio_gist == "BOISER问今晚吃啥"
    gen_body = next(b for path, b in seen_bodies if path.endswith("interactions"))
    gen_parts = (
        gen_body["input"][0]["content"]
        if isinstance(gen_body["input"][0], dict) and "content" in gen_body["input"][0]
        else gen_body["input"]
    )
    assert any(
        item.get("type") == "audio"
        and item.get("mime_type") == "audio/mp3"
        and item.get("data") == audio.data
        for item in gen_parts
    )
    assert "audio_gist_hint" in gen_parts[0]["text"]
    assert "audio_gist" in CHAT_RESPONSE_FORMAT["required"]
    assert set(CHAT_RESPONSE_FORMAT["required"]) == set(ChatActions.model_fields)
    assert "audio_gist" not in ACTION_INSTRUCTION

    # Verify exact ceil(duration * 35) reserve delta on chat generation lane too
    reserved.clear()
    same_payload = {"current_message": "[语音]", "session_enabled": False}
    await gw.generate(same_payload, "subj-audio", message=msg, audios=[audio])
    gen_with_audio = reserved[0][2]
    reserved.clear()
    await gw.generate(same_payload, "subj-audio", message=msg)
    gen_no_audio = reserved[0][2]
    expected_gen_delta = Decimal(424) * settings.chat_input_usd_per_m / Decimal(1_000_000)
    assert gen_with_audio - gen_no_audio == expected_gen_delta


async def test_gateway_vertex_generate_content_inline_audio_and_abstain_clears_gist(
    settings, store, sdk_pair, message
):
    captured_contents = []

    async def fake_stream(*, model, contents, config):
        captured_contents.append(contents)

        async def _gen():
            yield SimpleNamespace(
                candidates=[
                    SimpleNamespace(
                        finish_reason="STOP",
                        content=SimpleNamespace(
                            parts=[
                                SimpleNamespace(
                                    text=_action_json(
                                        abstain=True,
                                        bubbles=["不发"],
                                        audio_gist="不应保留的未审摘要",
                                    ),
                                    thought=False,
                                )
                            ]
                        ),
                    )
                ],
                usage_metadata=SimpleNamespace(
                    prompt_token_count=100,
                    candidates_token_count=20,
                    thoughts_token_count=0,
                    cached_content_token_count=0,
                    total_token_count=120,
                ),
            )

        return _gen()

    vertex_client = SimpleNamespace(
        aio=SimpleNamespace(
            models=SimpleNamespace(generate_content_stream=fake_stream),
        )
    )
    gw = GeminiGateway(
        settings,
        store,
        *sdk_pair(lambda req: httpx.Response(200, json=decision_response())),
        vertex_chat_client=vertex_client,
    )
    await gw.routing.initialize()
    gw.routing.config = gw.routing.config.model_copy(
        update={"vertex_enabled": True, "vertex_protocol": "generate_content"}
    )

    audio = _sample_audio(duration_sec=5.0, raw=b"ID3vertex-audio")
    out = await gw.generate(
        {"current_message": "[语音]"},
        "subj-vertex",
        message=message,
        audios=[audio],
    )
    parsed = ChatActions.model_validate_json(out)
    assert parsed.abstain is True
    assert parsed.audio_gist is None
    assert len(captured_contents) == 1
    parts = captured_contents[0][0].parts
    assert any(
        getattr(p, "inline_data", None) is not None
        and p.inline_data.mime_type == "audio/mp3"
        and p.inline_data.data == b"ID3vertex-audio"
        for p in parts
    )


async def test_gateway_rejects_audio_video_mix_and_oversized_inline_audio(
    settings, store, sdk_pair, message
):
    gw = GeminiGateway(
        settings,
        store,
        *sdk_pair(lambda req: httpx.Response(200, json=decision_response())),
    )
    audio = _sample_audio(duration_sec=3.0)
    vid = ModelVideo(
        name="files/hv-" + "a" * 32,
        uri="https://generativelanguage.googleapis.com/v1beta/files/hv-" + "a" * 32,
        duration=10.0,
    )
    with pytest.raises(Rejected, match="audio_video_mixed_forbidden"):
        await gw.screen({"x": 1}, "subj", purpose="source", video=vid, audios=[audio])
    with pytest.raises(Rejected, match="audio_video_mixed_forbidden"):
        await gw.generate({"x": 1}, "subj", summary=True, video=vid, audios=[audio])

    chunk = _sample_audio(duration_sec=10.0, raw=b"a" * (MAX_AUDIO_BYTES // 2 + 1024))
    assert chunk.validate() <= MAX_INLINE_AUDIO_BYTES
    with pytest.raises(Rejected, match="audio_too_large"):
        await gw.screen(
            {"current_message": "[语音]"}, "subj", purpose="message", audios=[chunk, chunk]
        )
    with pytest.raises(Rejected, match="audio_too_large"):
        await gw.generate(
            {"current_message": "[语音]"}, "subj", message=message, audios=[chunk, chunk]
        )


def test_static_prefix_and_persona_hash_locks_untouched(settings, message):
    assert (
        hashlib.sha256(SYSTEM_INSTRUCTION.strip("\n").encode("utf-8")).hexdigest()
        == "f54a8a92970f5e7ca45e6e7aae05ebfa7841c4e30e4da6e788b88cda909b2943"
    )
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

    # assemble_chat_input adds audio_gist_hint ONLY when audio_sources has available audio
    wire_plain = assemble_chat_input({"current_message": "hi"}, message, settings)
    assert "audio_gist_hint" not in wire_plain and "audio_sources" not in wire_plain
    wire_audio = assemble_chat_input(
        {
            "current_message": "[语音]",
            "audio_sources": [{"audio_number": 1, "status": "available", "duration_sec": 12.0}],
        },
        message,
        settings,
    )
    assert "audio_gist_hint" in wire_audio and "audio_sources" in wire_audio
    wire_sandbox = assemble_chat_input(
        {
            "current_message": "[语音]",
            "audio_sources": [{"audio_number": 1, "status": "available", "duration_sec": 12.0}],
        },
        message,
        settings,
        sandbox_mode=True,
    )
    assert "audio_gist_hint" not in wire_sandbox


# ---------------------------------------------------------------------------
# 5. Service end-to-end: group_policies, slash isolation, audio_gist memory
# ---------------------------------------------------------------------------


class ActionFakeGateway(FakeGateway):
    def __init__(self, reply_json: str, *, intent: str = "chat"):
        super().__init__()
        self.reply_json = reply_json
        self.intent = intent

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        return self.reply_json


def _group_voice_msg(
    message: Message,
    *,
    event_id: str = "gv-1",
    addressed: bool = False,
    attention_reason: str = "",
    file: str = "gv1.amr",
) -> Message:
    return message.model_copy(
        update={
            "event_id": event_id,
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "text": "[语音]",
            "command_text": "",
            "addressed": addressed,
            "attention_reason": attention_reason,
            "audios": [AudioRef(file=file)],
        }
    )


async def test_group_policies_for_voice_match_images(settings, store, message):
    settings.group_debounce_seconds = 0
    settings.group_debounce_max_seconds = 0
    audio_loader = FakeAudioLoader({"gv1.amr": _sample_audio(duration_sec=8.0)})

    # 1) muted -> ignored before download or LLM
    settings.group_policies = {"onebot:40000": "muted"}
    gw_muted = ActionFakeGateway(_action_json(bubbles=["不应回复"]))
    svc_muted = Service(
        settings, store, gw_muted, FakeBili(), FakeSender(), audio_loader=audio_loader
    )
    res_muted = await svc_muted.accept(
        _group_voice_msg(message, event_id="gv-muted", addressed=True, attention_reason="mention")
    )
    assert res_muted == "ignored"
    assert not audio_loader.calls and not gw_muted.calls

    # 2) at_only -> unaddressed voice does not trigger LLM; addressed voice does!
    settings.group_policies = {"onebot:40000": "at_only"}
    gw_at = ActionFakeGateway(_action_json(bubbles=["收到语音"], audio_gist="群友问天气"))
    svc_at = Service(settings, store, gw_at, FakeBili(), FakeSender(), audio_loader=audio_loader)
    res_unaddr = await svc_at.accept(_group_voice_msg(message, event_id="gv-at-0", addressed=False))
    assert res_unaddr == "ignored"
    assert await store.claim() is None

    res_addr = await svc_at.accept(
        _group_voice_msg(message, event_id="gv-at-1", addressed=True, attention_reason="mention")
    )
    assert res_addr == "accepted"
    job = await store.claim()
    assert job is not None
    await svc_at.process(job)
    assert await svc_at.deliver_once() is True
    assert [c[0] for c in gw_at.calls] == ["screen", "generate"]
    assert len(gw_at.calls[0][3]["audios"]) == 1
    assert len(gw_at.calls[1][3]["audios"]) == 1

    # 2a) at_only reply-to-bot with pure voice message -> resolved as addressed & processed!
    bot_msg_group = _group_voice_msg(message, event_id="bot-sent-900", addressed=True)
    await store.runtime.attention.record_sent(bot_msg_group, {"message_id": "900"})
    gw_at.calls.clear()
    reply_voice = _group_voice_msg(message, event_id="gv-at-reply", addressed=False).model_copy(
        update={"reply_ids": ["900"], "text": "[引用消息]\n[语音]"}
    )
    assert await svc_at.accept(reply_voice) == "accepted"
    await svc_at.process(await store.claim())
    assert await svc_at.deliver_once() is True
    assert [c[0] for c in gw_at.calls] == ["screen", "generate"]
    assert len(gw_at.calls[0][3]["audios"]) == 1
    assert len(gw_at.calls[1][3]["audios"]) == 1

    # 2b) at_only follow-up: user sends voice (context-only), then '@bot 听一下' pulls nearby audio
    audio_loader.result_map["gv_nearby.amr"] = _sample_audio(duration_sec=9.0, raw=b"ID3nearby9s")
    gw_nearby = ActionFakeGateway(
        _action_json(bubbles=["刚听完你说的话啦"], audio_gist="刚才那段在说周末去露营")
    )
    svc_nearby = Service(
        settings, store, gw_nearby, FakeBili(), FakeSender(), audio_loader=audio_loader
    )
    assert (
        await svc_nearby.accept(
            _group_voice_msg(
                message, event_id="gv-nearby-voice", addressed=False, file="gv_nearby.amr"
            )
        )
        == "ignored"
    )
    followup = message.model_copy(
        update={
            "event_id": "gv-nearby-ask",
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "text": "[提及机器人]\n帮我听一下刚才那条语音",
            "command_text": "帮我听一下刚才那条语音",
            "addressed": True,
            "attention_reason": "mention",
            "created_at": time.time() + 1,
        }
    )
    assert await svc_nearby.accept(followup) == "accepted"
    await svc_nearby.process(await store.claim())
    assert await svc_nearby.deliver_once() is True
    # Calls: inbound message screen -> source screen for nearby audio -> generate
    assert [c[0] for c in gw_nearby.calls] == ["screen", "screen", "generate"]
    assert gw_nearby.calls[1][3]["purpose"] == "source"
    assert len(gw_nearby.calls[1][3]["audios"]) >= 1
    assert len(gw_nearby.calls[2][3]["audios"]) >= 1
    # Consumed nearby audio is NOT re-sent on next turn
    gw_nearby.calls.clear()
    followup2 = followup.model_copy(
        update={"event_id": "gv-nearby-ask-2", "created_at": time.time() + 2}
    )
    assert await svc_nearby.accept(followup2) == "accepted"
    await svc_nearby.process(await store.claim())
    assert await svc_nearby.deliver_once() is True
    assert [c[0] for c in gw_nearby.calls] == ["screen", "generate"]
    assert "audios" not in gw_nearby.calls[1][3]

    # 3) smart_reply -> unaddressed voice enters screen(); ignore skips generate()
    settings.group_policies = {"onebot:40000": "smart_reply"}
    gw_smart_ignore = ActionFakeGateway(_action_json(bubbles=["不应生成"]), intent="ignore")
    svc_smart = Service(
        settings, store, gw_smart_ignore, FakeBili(), FakeSender(), audio_loader=audio_loader
    )
    res_smart = await svc_smart.accept(
        _group_voice_msg(message, event_id="gv-smart-1", addressed=False)
    )
    assert res_smart == "accepted"
    await svc_smart.process(await store.claim())
    assert [c[0] for c in gw_smart_ignore.calls] == ["screen"]
    assert len(gw_smart_ignore.calls[0][3]["audios"]) == 1

    # 4) smart_reply when screen returns "chat" -> proceeds to generate()
    gw_smart_chat = ActionFakeGateway(
        _action_json(bubbles=["哈哈听到了"], audio_gist="分享今天抽卡经历"), intent="chat"
    )
    svc_smart2 = Service(
        settings, store, gw_smart_chat, FakeBili(), FakeSender(), audio_loader=audio_loader
    )
    assert (
        await svc_smart2.accept(_group_voice_msg(message, event_id="gv-smart-2", addressed=False))
        == "accepted"
    )
    await svc_smart2.process(await store.claim())
    assert await svc_smart2.deliver_once() is True
    assert [c[0] for c in gw_smart_chat.calls] == ["screen", "generate"]


async def test_voice_never_triggers_slash_commands_but_allows_owner_trigger_action(
    settings, store, message
):
    settings.group_debounce_seconds = 0
    settings.group_debounce_max_seconds = 0
    settings.owner_qq_id = "20000"
    audio_loader = FakeAudioLoader({"cmd_voice.amr": _sample_audio(duration_sec=6.0)})
    gw = ActionFakeGateway(
        _action_json(
            bubbles=["好嘞"],
            audio_gist="/new-session",
        )
    )
    svc = Service(settings, store, gw, FakeBili(), FakeSender(), audio_loader=audio_loader)
    await svc.sessions.remember(message, "之前的记忆", "好的记住啦")

    # Even if a synthetic voice message carries '/new-session' or '/deep' in text/command_text,
    # voice must NEVER execute any text slash command or clear the session.
    voice_cmd_msg = message.model_copy(
        update={
            "event_id": "voice-slash-1",
            "text": "/new-session",
            "command_text": "/new-session",
            "audios": [AudioRef(file="cmd_voice.amr")],
        }
    )
    assert await svc.command(voice_cmd_msg) is None
    assert await svc.accept(voice_cmd_msg) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once() is True
    # Went through model pipeline (screen + generate), NOT command short-circuit!
    assert [c[0] for c in gw.calls] == ["screen", "generate"]
    assert gw.calls[1][3]["deep"] is False
    # Prior session memory was NOT wiped by '/new-session'
    snap = await svc.sessions.snapshot(message)
    assert any("之前的记忆" in row["text"] for row in snap["recent_messages"])

    # Owner natural-language trigger_action on voice turn still spawns normally
    spawned = []
    svc._spawn_owner_chat_action = lambda trigger, **kw: spawned.append(trigger)
    svc._collect_owner_chat_actions = lambda: [
        {"action_name": "mc.explore", "description": "探索", "arguments": {}}
    ]
    gw_action = ActionFakeGateway(
        _action_json(
            bubbles=["出发探索啦"],
            audio_gist="让哈基米去MC里逛逛",
            trigger_action={"action_name": "mc.explore", "arguments_json": "{}"},
        )
    )
    svc.ai = gw_action
    owner_voice = message.model_copy(
        update={
            "event_id": "voice-owner-action-1",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="cmd_voice.amr")],
        }
    )
    assert await svc.accept(owner_voice) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once() is True
    assert len(spawned) == 1 and spawned[0].action_name == "mc.explore"


async def test_audio_gist_history_retention_dual_memory_and_no_antigravity_memory_touch(
    settings, store, message, tmp_path
):
    settings.group_debounce_seconds = 0
    settings.group_debounce_max_seconds = 0
    ag_dir = tmp_path / "Antigravity_Memory_Should_Not_Exist"
    settings.antigravity_memory_dir = ag_dir
    settings.enable_antigravity_memory = False

    audio_loader = FakeAudioLoader(
        {
            "v_gist.amr": _sample_audio(duration_sec=12.2),
            "v_empty.amr": _sample_audio(duration_sec=7.8),
            "v_secret.amr": _sample_audio(duration_sec=4.0),
        }
    )

    # 1) Turn with audio_gist -> stored as '[语音 12s: 问今晚吃火锅还是烧烤]'
    gw1 = ActionFakeGateway(
        _action_json(bubbles=["吃火锅！"], audio_gist="问今晚吃火锅还是烧烤\n换行会被压平")
    )
    svc = Service(settings, store, gw1, FakeBili(), FakeSender(), audio_loader=audio_loader)
    msg1 = message.model_copy(
        update={
            "event_id": "mem-v1",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="v_gist.amr")],
        }
    )
    assert await svc.accept(msg1) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once() is True
    snap1 = await svc.sessions.snapshot(message)
    user_rows = [r for r in snap1["recent_messages"] if r.get("event_id") == "mem-v1"]
    assert len(user_rows) == 1
    assert user_rows[0]["text"] == "[语音 12s: 问今晚吃火锅还是烧烤 换行会被压平]"

    # 2) Turn with empty/null audio_gist -> stored as '[语音 8s]'
    svc.ai = ActionFakeGateway(_action_json(bubbles=["嗯嗯"], audio_gist=None))
    msg2 = message.model_copy(
        update={
            "event_id": "mem-v2",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="v_empty.amr")],
        }
    )
    assert await svc.accept(msg2) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once() is True
    snap2 = await svc.sessions.snapshot(message)
    row2 = next(r for r in snap2["recent_messages"] if r.get("event_id") == "mem-v2")
    assert row2["text"] == "[语音 8s]"

    # 3) Turn with credential in audio_gist -> scrubbed to '[语音 4s]' (no secret stored!)
    svc.ai = ActionFakeGateway(
        _action_json(bubbles=["收到"], audio_gist="password: supersecretpassword123")
    )
    msg3 = message.model_copy(
        update={
            "event_id": "mem-v3",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="v_secret.amr")],
        }
    )
    assert await svc.accept(msg3) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once() is True
    snap3 = await svc.sessions.snapshot(message)
    row3 = next(r for r in snap3["recent_messages"] if r.get("event_id") == "mem-v3")
    assert row3["text"] == "[语音 4s]"
    assert "supersecretpassword123" not in row3["text"]

    # 4) Verify dual memory cold slice AND rolling summary compression snapshot capture audio_gist
    await svc.sessions.dual.snapshot_cold(message, force=True)
    peek = await svc.sessions.dual.peek(message, background=False)
    assert peek is not None
    joined = "\n".join(str(m.get("text", "")) for m in peek["messages"])
    assert "[语音 12s: 问今晚吃火锅还是烧烤 换行会被压平]" in joined
    assert "[语音 8s]" in joined

    # Legacy rolling summary path also sees the formatted '[语音 12s: ...]' entries
    settings.prefix_history_enabled = False
    store.runtime.compression = store.runtime.compression.model_copy(
        update={
            "private": store.runtime.compression.private.model_copy(update={"keep": 1}),
        }
    )
    comp_snap = await svc.sessions.compression_snapshot(message, force=True)
    assert comp_snap is not None
    comp_joined = "\n".join(str(m.get("text", "")) for m in comp_snap["messages"])
    assert "[语音 12s: 问今晚吃火锅还是烧烤 换行会被压平]" in comp_joined
    assert "[语音 8s]" in comp_joined

    # 5) Antigravity_Memory is NEVER created or written to
    assert not ag_dir.exists()


async def test_debounced_batch_voice_plus_text_followup_annotates_voice_row_only(
    settings, store, message
):
    """When a batch has [m1(voice, e1), m2(text, e2)] and representative is e2,
    audio_gist must annotate m1 ('[语音 12s: <gist>]') and leave m2 plain text."""
    settings.group_debounce_seconds = 0
    settings.group_debounce_max_seconds = 0
    audio_loader = FakeAudioLoader({"batch_v1.amr": _sample_audio(duration_sec=12.0)})
    gw = ActionFakeGateway(_action_json(bubbles=["明白啦"], audio_gist="讨论周末爬山路线"))
    svc = Service(settings, store, gw, FakeBili(), FakeSender(), audio_loader=audio_loader)

    m1 = message.model_copy(
        update={
            "event_id": "batch-e1",
            "text": "[语音]",
            "command_text": "",
            "audios": [AudioRef(file="batch_v1.amr")],
            "created_at": time.time(),
        }
    )
    m2 = message.model_copy(
        update={
            "event_id": "batch-e2",
            "text": "补充一句记得带水",
            "command_text": "补充一句记得带水",
            "audios": [],
            "created_at": time.time() + 0.5,
        }
    )
    # Simulate debounced batch job whose merged representative carries event_id='batch-e2'
    merged = m2.model_copy(
        update={
            "event_id": "batch-e2",
            "text": "[语音]\n补充一句记得带水",
            "command_text": "",
            "audios": [AudioRef(file="batch_v1.amr")],
        }
    )
    await svc.sessions.receive(m1)
    await svc.sessions.receive(m2)
    job = {
        "id": "job-batch-voice-text",
        "kind": "message",
        "scope": store.session_scope(merged),
        "subject": store.subject(merged),
        "expires": time.time() + 120,
        "payload": merged.model_dump(),
        "batch_messages": [m1.model_dump(), m2.model_dump()],
    }
    await svc.handle_message(job)
    snap = await svc.sessions.snapshot(message)
    row_e1 = next(r for r in snap["recent_messages"] if r.get("event_id") == "batch-e1")
    row_e2 = next(r for r in snap["recent_messages"] if r.get("event_id") == "batch-e2")
    assert row_e1["text"] == "[语音 12s: 讨论周末爬山路线]"
    assert row_e2["text"] == "补充一句记得带水"
    assert "[语音" not in row_e2["text"]

    # Fallback check: when primary audio event is NOT in recent_messages (e.g. nearby group voice),
    # annotate_audio_gist falls back cleanly to fallback_event_id='batch-e2'.
    ok = await svc.sessions.annotate_audio_gist(
        message,
        ["unadmitted-nearby-v99"],
        "紧邻语音摘要",
        fallback_event_id="batch-e2",
        durations={"unadmitted-nearby-v99": [9.0]},
        expected_epoch=snap["epoch"],
    )
    assert ok is True
    snap_after = await svc.sessions.snapshot(message)
    row_e2_after = next(r for r in snap_after["recent_messages"] if r.get("event_id") == "batch-e2")
    assert row_e2_after["text"] == "补充一句记得带水 [语音 9s: 紧邻语音摘要]"


def test_parse_actions_audio_gist_bounds_and_format_helper(settings):
    long_gist = "哈" * 120
    parsed, _ = parse_actions(
        _action_json(audio_gist=long_gist),
        public=False,
        settings=settings,
        allow_audio_gist=True,
    )
    assert parsed.audio_gist is not None
    assert len(parsed.audio_gist) == MAX_AUDIO_GIST_CHARS == 80

    # Denied when turn had no audio
    denied, _ = parse_actions(
        _action_json(audio_gist="不应出现"),
        public=False,
        settings=settings,
        allow_audio_gist=False,
    )
    assert denied.audio_gist is None

    # Configured secret stripped
    secret_val = settings.gemini_api_key.get_secret_value()
    scrubbed, _ = parse_actions(
        _action_json(audio_gist=f"说了密钥 {secret_val}"),
        public=False,
        settings=settings,
        allow_audio_gist=True,
    )
    assert scrubbed.audio_gist is None

    assert format_audio_memory_tag(12.2, "问吃饭") == "[语音 12s: 问吃饭]"
    assert format_audio_memory_tag(12.2, "") == "[语音 12s]"
    assert format_audio_memory_tag(0.2, None) == "[语音 1s]"
    assert format_audio_memory_tag(None, None) == "[语音]"
