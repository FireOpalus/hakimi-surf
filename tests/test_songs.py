"""Tests for SongLibrary cover songbook, admin song routes/UI, and group voice replies."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import math
import os
import re
import shutil
import struct
import sys
import time
import wave
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest
from pydantic import SecretStr

from hakimi.affection import AffectionConfig
from hakimi.app import create_app
from hakimi.bot_server import assemble_chat_input, prompt_context
from hakimi.config import Settings
from hakimi.gemini import GeminiGateway
from hakimi.models import Message
from hakimi.modules.adapters import Sender
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
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.songs import (
    MAX_SONG_UPLOAD_BYTES,
    SongLibrary,
    explicit_song_request,
    normalize_song_audio,
    transcode_mp3_to_ogg_opus,
)
from hakimi.voice_reply import VoiceReplyClient
from hakimi_plugin_sdk import HakimiPluginClient, PluginRuntimeConfig
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import TOKEN
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_v20_phase1_modules_and_world_state import V181_STATIC_PREFIX_DIGESTS

FAKE_WAV_DUR_MS = 3000


def _make_sine_wav(duration_ms: int = FAKE_WAV_DUR_MS, *, sample_rate: int = 16000) -> bytes:
    buf = io.BytesIO()
    n_frames = max(1, round(sample_rate * duration_ms / 1000.0))
    samples = bytearray()
    for i in range(n_frames):
        val = int(12000 * math.sin(2.0 * math.pi * 440.0 * (i / sample_rate)))
        samples.extend(struct.pack("<h", val))
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(bytes(samples))
    return buf.getvalue()


def _write_song_ffmpeg_and_ffprobe(tmp_path: Path) -> tuple[str, str]:
    """Create executable shims for ffmpeg & ffprobe when absent from host PATH."""
    bin_dir = tmp_path / "shim_bin"
    bin_dir.mkdir(exist_ok=True)
    ffmpeg_path = bin_dir / "ffmpeg"
    ffprobe_path = bin_dir / "ffprobe"
    ffmpeg_path.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import hashlib, json, sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "in_file = ''",
                "ss = '0'",
                "dur = '58'",
                "fmt = 'mp3'",
                "ac = '2'",
                "for idx, a in enumerate(args):",
                "    if a == '-i' and idx + 1 < len(args): in_file = args[idx + 1]",
                "    elif a == '-ss' and idx + 1 < len(args): ss = args[idx + 1]",
                "    elif a == '-t' and idx + 1 < len(args): dur = args[idx + 1]",
                "    elif a == '-f' and idx + 1 < len(args): fmt = args[idx + 1]",
                "    elif a == '-ac' and idx + 1 < len(args): ac = args[idx + 1]",
                "out_file = args[-1]",
                "raw = Path(in_file).read_bytes()",
                "if not raw or raw.startswith(b'BAD_AUDIO'):",
                "    sys.exit(1)",
                "digest = hashlib.sha256(raw).digest()",
                "if fmt == 'ogg':",
                "    Path(out_file).write_bytes(b'OggS\\x00\\x02' + digest + raw[:32])",
                "else:",
                "    header = b'ID3\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\xfb\\x90\\x00'",
                "    tag = json.dumps({'ss': ss, 't': dur, 'ac': ac}).encode()",
                "    Path(out_file).write_bytes(header + digest + tag)",
            ]
        ),
        encoding="utf-8",
    )
    ffprobe_path.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import json, sys",
                "from pathlib import Path",
                "data = Path(sys.argv[-1]).read_bytes()",
                "idx = data.rfind(b'{')",
                "if idx < 0:",
                "    print('3.000000')",
                "    sys.exit(0)",
                "meta = json.loads(data[idx:].decode())",
                "t = min(58.0, max(1.5, float(meta.get('t', '5.0'))))",
                "print(f'{t:.6f}')",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(ffmpeg_path, 0o755)
    os.chmod(ffprobe_path, 0o755)
    return str(ffmpeg_path), str(ffprobe_path)


@pytest.fixture
def fake_ffmpeg_path(tmp_path: Path, monkeypatch):
    ffm, ffp = _write_song_ffmpeg_and_ffprobe(tmp_path)
    bin_dir = str(Path(ffm).parent)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    return ffm, ffp


class _OneBotWire:
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls: list[tuple[str, dict]] = []

    async def call(self, action: str, params: dict) -> dict:
        self.calls.append((action, params))
        return {"message_id": 200 + len(self.calls)}


def _song_settings(settings: Settings, **overrides) -> Settings:
    data = {
        "voice_reply_enabled": True,
        "voice_reply_mode": "all",
        "voice_reply_cooldown_seconds": 30.0,
        "voice_reply_group_cooldown_seconds": 60.0,
        "voice_reply_daily_limit": 40,
        "voice_gateway_url": "",
        "voice_gateway_token": SecretStr(""),
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
        "sing_song": None,
    }
    base.update(overrides)
    return json.dumps(base, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 1. SongLibrary normalization, trimming, deduplication, and transcoding
# ---------------------------------------------------------------------------


async def test_song_library_crud_trim_and_transcode(store, fake_ffmpeg_path):
    lib = SongLibrary(store)
    await lib.initialize()
    wav_a = _make_sine_wav(3000)
    wav_b = _make_sine_wav(2500)

    assert explicit_song_request("给我唱一首小星星")
    assert explicit_song_request("点歌 《晴天》")
    assert explicit_song_request("please sing a song for me")
    assert not explicit_song_request("今天天气真好")

    entry = await lib.upload(
        "小星星",
        wav_a,
        artist="传统儿歌",
        tags="温柔 晚安",
        filename="star.wav",
        start_sec=0.5,
        duration_sec=12.0,
    )
    assert entry["title"] == "小星星"
    assert entry["artist"] == "传统儿歌"
    assert entry["tags"] == "温柔 晚安"
    assert entry["duration_ms"] == 12000
    assert "filename" not in entry
    assert lib.titles == ("小星星",)

    catalog = lib.catalog_entries()
    assert catalog == [
        {"title": "小星星", "duration_sec": 12.0, "artist": "传统儿歌", "tags": "温柔 晚安"}
    ]

    # Duplicate title rejected (case-insensitive)
    with pytest.raises(Rejected, match="song_title_exists"):
        await lib.upload("小星星", wav_b)

    # Duplicate normalized audio digest rejected even under different title
    with pytest.raises(Rejected, match="song_already_exists"):
        await lib.upload("小星星2", wav_a, start_sec=0.5, duration_sec=12.0)

    # Different trim offsets produce distinct digest and succeed
    entry2 = await lib.upload(
        "StarCut",
        wav_a,
        start_sec=1.0,
        duration_sec=8.0,
    )
    assert entry2["title"] == "StarCut"

    # Invalid audio rejected
    with pytest.raises(Rejected, match="invalid_song_audio"):
        await lib.upload("坏音频", b"BAD_AUDIO")

    # Delivery in MP3 and OGG Opus + case-insensitive title match
    mp3_bytes, fmt_m, mime_m, dur_m, canon_m = await lib.load_for_delivery(
        "starcut", preferred_fmt="mp3"
    )
    assert fmt_m == "mp3" and mime_m == "audio/mpeg" and canon_m == "StarCut"
    assert mp3_bytes.startswith(b"ID3") and dur_m == 8000

    ogg_bytes, fmt_o, mime_o, dur_o, canon_o = await lib.load_for_delivery(
        "小星星", preferred_fmt="ogg_opus"
    )
    assert fmt_o == "ogg_opus" and mime_o == "audio/ogg" and canon_o == "小星星"
    assert ogg_bytes.startswith(b"OggS") and dur_o == 12000

    # Delete & physical cleanup
    song_file = lib.root / f"{entry['id']}.mp3"
    assert song_file.is_file()
    await lib.delete(entry["id"])
    assert not song_file.exists()
    assert "小星星" not in lib.titles
    with pytest.raises(Rejected, match="song_unavailable"):
        await lib.audio_bytes(entry["id"])


@pytest.mark.skipif(
    not (shutil.which("ffmpeg") and shutil.which("ffprobe")),
    reason="real ffmpeg/ffprobe not installed on host",
)
async def test_real_ffmpeg_song_normalization_when_available():
    wav_6s = _make_sine_wav(6000)
    norm = await normalize_song_audio(wav_6s, ext_hint="wav", start_sec=0.5, duration_sec=5.0)
    assert norm.mime_type == "audio/mpeg"
    assert norm.data.startswith((b"ID3", b"\xff\xfb", b"\xff\xf3"))
    assert 4500 <= norm.duration_ms <= 5500
    ogg = await transcode_mp3_to_ogg_opus(norm.data)
    assert ogg.startswith(b"OggS")


# ---------------------------------------------------------------------------
# 2. Admin routes (/admin/api/songs) + CSP + UI checks
# ---------------------------------------------------------------------------


async def test_song_admin_routes_csp_and_html_invariants(
    admin_frontend, settings, fake_ffmpeg_path
):
    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    wav = _make_sine_wav(3000)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            # 401 without token
            unauth = await client.get("/admin/api/songs", headers={"Authorization": ""})
            assert unauth.status_code == 401

            # HTML page CSP & song UI invariants
            page = await client.get("/admin", headers={"Authorization": ""})
            assert page.status_code == 200
            html = page.text
            assert html.splitlines()[0].strip() == "<!doctype html>"
            styles = re.findall(r"<style>(.*?)</style>", html, re.S)
            scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
            assert len(styles) == 1 and len(scripts) == 1
            style_hash = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
            script_hash = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
            csp = page.headers["content-security-policy"]
            assert f"'sha256-{style_hash}'" in csp
            assert f"'sha256-{script_hash}'" in csp
            assert "media-src blob:" in csp
            assert not re.search(r"\sstyle\s*=", html)
            assert not re.search(r"\son[a-z]+\s*=", html, re.I)
            assert "innerHTML" not in html
            assert 'id="nav-songs"' in html
            assert 'id="songs"' in html

            # Query parameters are rejected by AdminAuth
            bad_q = await client.post(
                "/admin/api/songs/upload?title=bad",
                content=wav,
                headers={"Content-Type": "audio/wav"},
            )
            assert bad_q.status_code == 400

            # Upload song via X-Song-* headers
            up = await client.post(
                "/admin/api/songs/upload",
                content=wav,
                headers={
                    "Content-Type": "audio/wav",
                    "X-Song-Title": quote("月光曲"),
                    "X-Song-Artist": quote("测试歌手"),
                    "X-Song-Tags": quote("抒情"),
                    "X-Song-Filename": "moon.wav",
                    "X-Song-Start": "0",
                    "X-Song-Duration": "10",
                },
            )
            assert up.status_code == 200, up.text
            item = up.json()
            assert item["title"] == "月光曲"
            assert "filename" not in item

            # List
            listing = (await client.get("/admin/api/songs")).json()
            assert len(listing["items"]) == 1
            assert listing["loaded_prompt_count"] == 1
            assert "filename" not in listing["items"][0]

            # Audio preview + media-src blob: CSP
            audio_resp = await client.get(f"/admin/api/songs/{item['id']}/audio")
            assert audio_resp.status_code == 200
            assert audio_resp.headers["content-type"] == "audio/mpeg"
            assert audio_resp.headers["cache-control"] == "no-store"
            assert "media-src blob:" in audio_resp.headers["content-security-policy"]

            # 413 oversized & 415 bad mime
            assert (
                await client.post(
                    "/admin/api/songs/upload",
                    content=b"x",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "Content-Length": str(MAX_SONG_UPLOAD_BYTES + 1),
                        "X-Song-Title": "big",
                    },
                )
            ).status_code == 413
            assert (
                await client.post(
                    "/admin/api/songs/upload",
                    content=b"x",
                    headers={
                        "Content-Type": "image/png",
                        "X-Song-Title": "badmime",
                    },
                )
            ).status_code == 415

            # Delete -> 200 then 404 on subsequent audio & delete
            assert (await client.delete(f"/admin/api/songs/{item['id']}")).status_code == 200
            assert (await client.get(f"/admin/api/songs/{item['id']}/audio")).status_code == 404
            assert (await client.delete(f"/admin/api/songs/{item['id']}")).status_code == 404


# ---------------------------------------------------------------------------
# 3. ChatActions.sing_song schema, StaticPrefixes locks, parse_actions & abstain
# ---------------------------------------------------------------------------


async def test_sing_song_schema_prefix_lock_and_parse_actions(settings, store, message, sdk_pair):
    assert "sing_song" in CHAT_RESPONSE_FORMAT["properties"]
    assert "sing_song" in CHAT_RESPONSE_FORMAT["required"]
    assert set(CHAT_RESPONSE_FORMAT["required"]) == set(ChatActions.model_fields)
    assert (
        hashlib.sha256(ACTION_INSTRUCTION.encode("utf-8")).hexdigest()
        == "545814e140335920327aff073021c72cf9a3a1f4abed9d8ad5a638b054879f14"
    )
    assert "sing_song" not in ACTION_INSTRUCTION

    # Backward compatibility when sing_song is omitted
    legacy = json.loads(_action_json())
    legacy.pop("sing_song")
    assert ChatActions.model_validate_json(json.dumps(legacy)).sing_song is None

    # StaticPrefixes byte-identical digests preserved
    pool = StaticPrefixes()
    for bucket, msg in {
        "owner_dm": message,
        "friend_dm": message.model_copy(update={"user_id": "30000", "conversation_id": "30000"}),
        "group": message.model_copy(
            update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
        ),
    }.items():
        text = pool.get(prompt_context(msg, settings), settings.onebot_self_id)
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        assert digest == V181_STATIC_PREFIX_DIGESTS[bucket]

    # Mutual exclusion with voice_text & allowlist enforcement
    raw_song = _action_json(bubbles=["我唱给你听~"], voice_text="不应共存", sing_song="starlight")
    denied, _ = parse_actions(
        raw_song,
        public=False,
        settings=settings,
        allow_voice=True,
        allow_sing_song=False,
        song_titles=("Starlight",),
    )
    assert denied.sing_song is None
    assert denied.voice_text == "不应共存"

    unknown, _ = parse_actions(
        _action_json(bubbles=["换一首"], sing_song="不存在的歌"),
        public=False,
        settings=settings,
        allow_voice=False,
        allow_sing_song=True,
        song_titles=("Starlight",),
    )
    assert unknown.sing_song is None

    allowed, _ = parse_actions(
        raw_song,
        public=False,
        settings=settings,
        allow_voice=True,
        allow_sing_song=True,
        song_titles=("Starlight",),
    )
    assert allowed.sing_song == "Starlight"
    assert allowed.voice_text is None

    # GeminiGateway clears sing_song when abstain=True
    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _action_json(
            abstain=True, bubbles=[""], voice_text="不应出现", sing_song="Starlight"
        )
        return httpx.Response(200, json=resp)

    gw = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        generated = await gw.generate(
            {"current_message": "唱首歌", "current_sender": {"nickname": "BOISER", "qq": "20000"}},
            store.subject(message),
            message=message,
        )
        parsed = ChatActions.model_validate_json(generated)
        assert parsed.sing_song is None
        assert parsed.voice_text is None
    finally:
        await gw.close()


# ---------------------------------------------------------------------------
# 4. Group voice policy, cooldown asymmetry, prompt hints, & affection blocking
# ---------------------------------------------------------------------------


async def test_group_voice_policy_cooldown_prompts_and_affection(
    settings, store, message, fake_ffmpeg_path, monkeypatch
):
    s = _song_settings(
        settings,
        voice_gateway_url="http://hakimi-voice-gateway:12145",
        voice_gateway_token=SecretStr("vg-test-token-12345678901234567890"),
    )
    now = [1_700_000_000.0]
    vc = VoiceReplyClient(s, clock=lambda: now[0])

    # Cooldown uses 60s for groups vs 30s for DM
    assert vc.can_send_now("onebot:group:40000", explicit=False, is_group=True) == (True, "ok")
    vc.note_sent("onebot:group:40000")
    now[0] += 35.0
    assert vc.can_send_now("onebot:group:40000", explicit=False, is_group=True) == (
        False,
        "cooldown",
    )
    assert vc.can_send_now("onebot:group:40000", explicit=True, is_group=True) == (True, "ok")
    now[0] += 26.0
    assert vc.can_send_now("onebot:group:40000", explicit=False, is_group=True) == (True, "ok")

    # Different prompt hints for owner DM vs group
    grp_msg = message.model_copy(
        update={
            "user_id": "30000",
            "kind": "group",
            "conversation_id": "40000",
            "text": "用语音唱一首",
        }
    )
    wire_dm = assemble_chat_input(
        {"current_message": "hi", "voice_reply_available": True},
        message,
        s,
    )
    wire_grp = assemble_chat_input(
        {
            "current_message": "hi",
            "voice_reply_available": True,
            "song_catalog": [{"title": "小星星", "duration_sec": 12.0}],
        },
        grp_msg,
        s,
    )
    meta_dm = json.loads(
        re.search(r"<untrusted_user_input>(.*?)</untrusted_user_input>", wire_dm, re.S).group(1)
    )
    meta_grp = json.loads(
        re.search(r"<untrusted_user_input>(.*?)</untrusted_user_input>", wire_grp, re.S).group(1)
    )
    assert "当前对话支持发送语音条" in meta_dm["voice_reply_hint"]
    assert "当前群聊支持发送语音条" in meta_grp["voice_reply_hint"]
    assert meta_grp["voice_reply_hint"] != meta_dm["voice_reply_hint"]
    assert "sing_song_hint" in meta_grp and meta_grp["song_catalog"][0]["title"] == "小星星"

    # Service turn: group member cannot bypass 60s group cooldown, Owner CAN; cold tier blocked
    wire = _OneBotWire()
    gw = FakeGateway()
    svc = Service(s, store, gw, FakeBili(), Sender(s, httpx.AsyncClient(), wire))
    await svc.songs.upload("小星星", _make_sine_wav(3000))
    svc.voice_reply.clock = lambda: now[0]

    async def _run_turn(msg: Message) -> dict:
        gw.calls.clear()
        assert await svc.accept(msg) == "accepted"
        job = await store.claim()
        assert job is not None
        await svc.process(job)
        while await svc.deliver_once():
            pass
        return next(c[1] for c in gw.calls if c[0] == "generate")

    try:
        # Mark group 40000 as recently sent
        svc.voice_reply.note_sent(svc.store.session_scope(grp_msg), now=now[0])
        now[0] += 10.0

        # Non-owner explicit voice/song ask in group within 60s -> blocked by cooldown!
        p_member = await _run_turn(grp_msg)
        assert "voice_reply_available" not in p_member
        assert "song_catalog" not in p_member

        # Owner in same group within 60s asking for song -> bypasses cooldown!
        owner_grp = grp_msg.model_copy(
            update={"event_id": "ev-owner-grp", "user_id": "20000", "text": "唱一首小星星"}
        )
        p_owner = await _run_turn(owner_grp)
        assert p_owner.get("voice_reply_available") is True
        assert p_owner.get("song_catalog")

        # After 60s passes, normal non-owner group member gets voice & song_catalog
        now[0] += 65.0
        p_member_ok = await _run_turn(
            grp_msg.model_copy(update={"event_id": "ev-member-ok", "text": "群里气氛真好"})
        )
        assert p_member_ok.get("voice_reply_available") is True
        assert p_member_ok.get("song_catalog")

        # Affection cold tier (level 2, allow_sticker=False) blocks both voice & singing
        monkeypatch.setattr("hakimi.service.random.random", lambda: 0.0)
        aff = store.runtime.affection
        await aff.initialize()
        await aff.set_config(AffectionConfig(enabled=True))
        await aff.set_user_score("30000", 20, reason="test_cold")
        p_cold = await _run_turn(
            grp_msg.model_copy(update={"event_id": "ev-cold", "text": "用语音唱小星星"})
        )
        assert "voice_reply_available" not in p_cold
        assert "song_catalog" not in p_cold
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# 5. End-to-end sing_song delivery (OneBot private/group + PluginBus ogg_opus + fallback)
# ---------------------------------------------------------------------------


async def test_sing_song_e2e_onebot_no_tts_and_fallback(settings, store, message, fake_ffmpeg_path):
    # Note: voice_gateway_url is EMPTY — singing does not depend on TTS gateway!
    s = _song_settings(settings)
    wire = _OneBotWire()
    song_choice = {"title": "小星星"}

    class _SingGateway(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            self.calls.append(("generate", dict(payload), subject, {}))
            return _action_json(bubbles=["我来唱一段喵~"], sing_song=song_choice["title"])

    async with httpx.AsyncClient() as http:
        svc = Service(s, store, _SingGateway(), FakeBili(), Sender(s, http, wire), http=http)
        await svc.world_state.initialize()
        await svc.songs.upload("小星星", _make_sine_wav(3000), artist="儿歌", duration_sec=10.0)
        try:
            # 1) Owner private singing
            dm_msg = message.model_copy(update={"text": "唱一首小星星"})
            assert await svc.accept(dm_msg) == "accepted"
            job = await store.claim()
            await svc.process(job)
            assert await svc.deliver_once()
            assert [c[0] for c in wire.calls[-2:]] == ["send_private_msg", "send_private_msg"]
            assert wire.calls[-2][1]["message"][0]["data"]["text"] == "我来唱一段喵~"
            assert wire.calls[-1][1]["message"][0]["type"] == "record"
            snap = await svc.sessions.snapshot(message)
            assert snap["recent_messages"][-1]["text"] == "[唱歌：《小星星》]"

            # 2) Group singing
            grp = message.model_copy(
                update={
                    "event_id": "grp-sing-1",
                    "user_id": "20000",
                    "kind": "group",
                    "conversation_id": "40000",
                    "text": "在群里唱小星星",
                }
            )
            assert await svc.accept(grp) == "accepted"
            job_g = await store.claim()
            await svc.process(job_g)
            assert await svc.deliver_once()
            assert wire.calls[-1][0] == "send_group_msg"
            assert wire.calls[-1][1]["message"][0]["type"] == "record"

            # 3) Missing song file fallback when nothing was sent yet vs silent when text sent
            svc.voice_reply._last_sent.clear()
            for e in svc.songs._entries.values():
                (svc.songs.root / e["filename"]).unlink()
            res_fb = await deliver_proactive_jit(
                svc,
                message,
                _action_json(bubbles=[""], sing_song="小星星"),
                reply_id="jit:song-missing",
            )
            assert res_fb["status"] == "sent"
            assert wire.calls[-1][1]["message"][0]["type"] == "text"
            assert "没找到伴奏" in wire.calls[-1][1]["message"][0]["data"]["text"]

            # When a text bubble already went out, failed song does not emit extra fallback text
            calls_before = len(wire.calls)
            res_quiet = await deliver_proactive_jit(
                svc,
                message,
                _action_json(bubbles=["先抱抱~"], sing_song="小星星"),
                reply_id="jit:song-missing-2",
            )
            assert res_quiet["status"] == "sent"
            assert len(wire.calls) == calls_before + 1
            assert wire.calls[-1][1]["message"][0]["data"]["text"] == "先抱抱~"
        finally:
            await svc.stop()


async def test_sing_song_cross_platform_plugin_ogg_opus(settings, fake_ffmpeg_path):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "voice_reply_enabled": True,
            "voice_reply_mode": "all",
            "voice_gateway_url": "",
            "voice_gateway_token": SecretStr(""),
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    if not any(getattr(r, "path", "") == "/admin/api/plugins" for r in app.routes):
        app.include_router(plugin_router)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        await svc.songs.upload("小星星", _make_sine_wav(3000), duration_sec=9.0)
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus: PluginBus = svc.plugin_bus
        token, _ = await bus.create_plugin("tg_song", "Telegram Song", ["chat"], node_region="KR")

        class _InMemPair:
            def __init__(self) -> None:
                self.c2s: asyncio.Queue[str] = asyncio.Queue()
                self.s2c: asyncio.Queue[str] = asyncio.Queue()

            async def server_recv(self) -> str:
                v = await self.c2s.get()
                if v == "__EOF__":
                    raise RuntimeError("eof")
                return v

            async def server_send(self, raw: str) -> None:
                await self.s2c.put(raw)

            async def server_close(self, code: int = 1000) -> None:
                await self.c2s.put("__EOF__")
                await self.s2c.put("__EOF__")

        pair = _InMemPair()

        class _ClientWs:
            async def send(self, raw: str) -> None:
                await pair.c2s.put(raw)

            async def recv(self) -> str:
                v = await pair.s2c.get()
                if v == "__EOF__":
                    raise ConnectionError("closed")
                return v

            async def close(self, code: int = 1000) -> None:
                await pair.server_close(code)

        delivered_voices: list[dict] = []
        client = HakimiPluginClient(
            PluginRuntimeConfig(
                ws_url="ws://127.0.0.1:12141/ws/plugin",
                plugin_id="tg_song",
                token=token,
                display_name="Telegram Song",
                scopes=("chat",),
                chat_platforms=("telegram",),
                capabilities=("voice",),
                voice_format="ogg_opus",
            ),
            connect_factory=lambda *a, **kw: _ClientWs(),
        )
        connected = asyncio.Event()

        @client.on_connect
        async def _on_conn() -> None:
            connected.set()

        @client.on_voice_delivery
        async def _on_voice(params: dict) -> dict:
            delivered_voices.append(params)
            return {"message_id": f"tg-song-{len(delivered_voices)}"}

        server_task = asyncio.create_task(
            bus.serve(
                pair.server_recv,
                pair.server_send,
                pair.server_close,
                plugin_id="tg_song",
                source_ip="127.0.0.1",
            )
        )
        await client.start()
        try:
            async with asyncio.timeout(2.0):
                await connected.wait()
            tg_msg = Message(
                platform="telegram",
                bot_id="tg-bot",
                event_id="tg-sing-1",
                user_id="tg-u1",
                kind="private",
                conversation_id="tg-u1",
                text="唱一首小星星",
                addressed=True,
                created_at=time.time(),
            )
            await svc.store.enqueue_reply(
                "job-tg-song",
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
                    "sing_song": "小星星",
                    "explicit_voice": True,
                },
            )
            assert await deliver_actions(svc, await svc.store.claim_reply())
            assert len(delivered_voices) == 1
            assert delivered_voices[0]["format"] == "ogg_opus"
            assert delivered_voices[0]["mime_type"] == "audio/ogg"
            assert delivered_voices[0]["duration_ms"] == 9000
            assert base64.b64decode(delivered_voices[0]["audio_b64"]).startswith(b"OggS")
        finally:
            await client.stop()
            server_task.cancel()
            await asyncio.gather(server_task, return_exceptions=True)


async def test_multi_take_switch_delivers_switched_version_in_mp3_and_ogg_opus(
    store, fake_ffmpeg_path
):
    """Switching a song's active take immediately changes load_for_delivery across formats."""
    hdr = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00"
    mp3_a = hdr + b"TAKE_A_AUDIO_PAYLOAD" * 16
    mp3_b = hdr + b"TAKE_B_AUDIO_PAYLOAD" * 16
    dig_a = hashlib.sha256(mp3_a).hexdigest()
    dig_b = hashlib.sha256(mp3_b).hexdigest()

    class _StubDrive:
        def configured(self) -> bool:
            return True

        async def download_song(self, file_id: str) -> bytes:
            return {"drv-a": mp3_a, "drv-b": mp3_b}[file_id]

    lib = SongLibrary(store, drive=_StubDrive())
    await lib.initialize()
    entry = await lib.register_colab_cover(
        title="换版实测曲",
        digest=dig_a,
        byte_size=len(mp3_a),
        duration_ms=20000,
        drive_chorus_id="drv-a",
        chorus_bytes=mp3_a,
        active_engine="yingmusic_svc",
        active_take="yingmusic_svc",
        audition_status="picked",
        review_note="A版先入库喵~",
        takes=[
            {
                "key": "yingmusic_svc",
                "engine": "yingmusic_svc",
                "status": "succeeded",
                "digest": dig_a,
                "byte_size": len(mp3_a),
                "duration_ms": 20000,
                "drive_chorus_id": "drv-a",
            },
            {
                "key": "seed_vc",
                "engine": "seed_vc",
                "status": "succeeded",
                "digest": dig_b,
                "byte_size": len(mp3_b),
                "duration_ms": 21000,
                "drive_chorus_id": "drv-b",
            },
        ],
    )
    raw1, fmt1, _, dur1, _ = await lib.load_for_delivery("换版实测曲", preferred_fmt="mp3")
    assert raw1 == mp3_a and fmt1 == "mp3" and dur1 == 20000

    switched = await lib.switch_take(entry["id"], "seed_vc")
    assert switched["active_take"] == "seed_vc"
    assert switched["active_engine"] == "seed_vc"

    raw2, fmt2, _, dur2, _ = await lib.load_for_delivery("换版实测曲", preferred_fmt="mp3")
    assert raw2 == mp3_b and fmt2 == "mp3" and dur2 == 21000

    ogg2, fmt_o, mime_o, dur_o, _ = await lib.load_for_delivery(
        "换版实测曲", preferred_fmt="ogg_opus"
    )
    assert fmt_o == "ogg_opus" and mime_o == "audio/ogg" and dur_o == 21000
    assert ogg2.startswith(b"OggS")
