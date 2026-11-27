"""Wave 1+2 regression tests for billing, Colab, songs, memes, audio, video, and outgoing."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

import hakimi.audio_media as audio_media_mod
import hakimi.gcp_billing as gcp_billing_mod
import hakimi.media_turn as media_turn_mod
import hakimi.outgoing as outgoing_mod
import hakimi.text_card as text_card_mod
import hakimi.video_download_worker as video_worker_mod
from hakimi.adapters import Sender
from hakimi.affection import AffectionConfig, AffectionManager
from hakimi.app import create_app
from hakimi.audio_media import ModelAudio
from hakimi.colab_compute import ColabComputeHub
from hakimi.gcp_billing import (
    TOKEN_URI,
    GcpBillingConfig,
    GcpBillingCredentialsUpdate,
    GcpBillingManager,
)
from hakimi.memes import MemeLibrary
from hakimi.models import ImageRef
from hakimi.outgoing import deliver_proactive_jit, is_affection_muted_only, send_platform_card
from hakimi.security import Rejected
from hakimi.songs import (
    LAST_USED_PERSIST_INTERVAL_SECONDS,
    SongLibrary,
    validate_rvc_cover_payload,
)
from tests.fakes import FakeGateway
from tests.test_gcp_billing_and_monitoring import make_sa_info
from tests.test_song_drive import ADMIN_TOKEN, _FakeGoogleDriveServer, _write_oauth_json


def _png_bytes(color: str = "purple", size: tuple[int, int] = (32, 24)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, format="PNG")
    return buf.getvalue()


def _mp3_bytes(tag: bytes, *, total_size: int = 256) -> bytes:
    header = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00"
    raw = header + tag
    if len(raw) < total_size:
        raw += b"\x00" * (total_size - len(raw))
    return raw


# ---------------------------------------------------------------------------
# Wave 1: media_turn.auto_save_meme, outgoing.is_affection_muted_only, deliver_proactive_jit
# ---------------------------------------------------------------------------


async def test_auto_save_meme_handles_recalled_parent_without_raising(message):
    saved: list[tuple[str, bytes]] = []

    class _Images:
        async def fetch_raw(self, ref, *, max_bytes):
            del ref, max_bytes
            return b"raw-img"

    class _Memes:
        async def upload_bytes(self, label, raw):
            saved.append((label, raw))

    async def lookup_recalled(_msg, reply_id):
        if reply_id == "recalled-1":
            raise Rejected("recalled")
        return message.model_copy(
            update={"event_id": reply_id, "images": [ImageRef(url="https://cdn.example/ok.png")]}
        )

    svc = SimpleNamespace(
        images=_Images(),
        memes=_Memes(),
        lookup_received=lookup_recalled,
    )

    msg_only_recalled = message.model_copy(update={"reply_ids": ["recalled-1"], "images": []})
    assert await media_turn_mod.auto_save_meme(svc, msg_only_recalled, [], "猫猫") is False
    assert saved == []

    # Fallback to earlier non-recalled reply_id succeeds
    msg_multi = message.model_copy(update={"reply_ids": ["good-0", "recalled-1"], "images": []})
    assert await media_turn_mod.auto_save_meme(svc, msg_multi, [], "猫猫") is True
    assert saved == [("猫猫", b"raw-img")]

    # CancelledError still propagates
    async def lookup_cancel(_msg, _reply_id):
        raise asyncio.CancelledError

    svc.lookup_received = lookup_cancel
    with pytest.raises(asyncio.CancelledError):
        await media_turn_mod.auto_save_meme(svc, msg_only_recalled, [], "猫猫")


async def test_is_affection_muted_only_fails_closed_when_identity_missing(message):
    tg_msg = message.model_copy(
        update={"platform": "telegram", "user_id": "999001", "conversation_id": "999001"}
    )
    svc_no_identity = SimpleNamespace(identity=None)
    assert await is_affection_muted_only(svc_no_identity, tg_msg) is False


async def test_deliver_proactive_jit_treats_plugin_not_dispatched_as_not_dispatched(
    settings, store, message, monkeypatch
):
    class _FakeHooks:
        @staticmethod
        def allowed_proactive(_msg, _settings):
            return True

    class _FakeTgModule:
        supports_reply_anchor = False
        supports_voice = False
        supports_emoji_like = False

        async def send_chat(self, _msg, _text, **_kwargs):
            raise Rejected("plugin_not_dispatched")

    tg_msg = message.model_copy(
        update={
            "platform": "telegram",
            "kind": "private",
            "user_id": "123456",
            "conversation_id": "123456",
            "event_id": "tg-1",
        }
    )
    monkeypatch.setattr(outgoing_mod, "get_platform_hooks", lambda _p: _FakeHooks())
    monkeypatch.setattr(outgoing_mod, "authorize", lambda _m, _s: True)
    monkeypatch.setattr(outgoing_mod, "chat_module", lambda _s, _p: _FakeTgModule())

    class _FakeSessions:
        async def current_epoch(self, _msg):
            return 0

    svc = SimpleNamespace(
        s=settings,
        store=store,
        sessions=_FakeSessions(),
        identity=None,
        songs=None,
        voice_reply=None,
        memes=None,
        sender=None,
        secrets=(),
    )
    res = await deliver_proactive_jit(svc, tg_msg, "你好喵", reply_id="jit:test-plugin-nd")
    assert res == {"status": "not_dispatched", "bubbles_sent": 0}


# ---------------------------------------------------------------------------
# Wave 1: video_download_worker wildcard 206, 256 chunks, mux headroom & RLIMIT_FSIZE
# ---------------------------------------------------------------------------


def test_video_worker_wildcard_206_continues_until_416_or_empty_eof_and_supports_over_64_chunks(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    full_payload = b"".join(f"{i:04d}".encode() * 10 for i in range(75))  # 75 chunks of 40B = 3000B
    chunk_size = 40
    seen_ranges: list[str] = []

    def handler_416(req: httpx.Request) -> httpx.Response:
        rng = req.headers.get("range", "")
        seen_ranges.append(rng)
        start = int(rng.removeprefix("bytes=").rstrip("-"))
        if start >= len(full_payload):
            return httpx.Response(416, headers={"Content-Range": f"bytes */{len(full_payload)}"})
        end = min(len(full_payload), start + chunk_size)
        body = full_payload[start:end]
        return httpx.Response(
            206,
            content=body,
            headers={"Content-Range": f"bytes {start}-{end - 1}/*"},
        )

    dest = tmp_path / "wildcard_416.m4s"
    with httpx.Client(transport=httpx.MockTransport(handler_416)) as client:
        written = video_worker_mod.download_cdn_stream(
            client,
            ["https://upos-sz-mirrorcosov.bilivideo.com/v.m4s"],
            dest,
            max_bytes=video_worker_mod.MAX_BYTES,
            sleep_fn=lambda _: None,
        )
    assert written == len(full_payload)
    assert dest.read_bytes() == full_payload
    assert len(seen_ranges) == 76  # 75 data chunks + final 416 EOF probe

    # Also verify 0-byte 206 continuation terminates cleanly as EOF
    two_chunks = b"A" * 100 + b"B" * 50

    def handler_zero_eof(req: httpx.Request) -> httpx.Response:
        start = int(req.headers.get("range", "").removeprefix("bytes=").rstrip("-"))
        if start == 0:
            return httpx.Response(
                206, content=two_chunks[:100], headers={"Content-Range": "bytes 0-99/*"}
            )
        if start == 100:
            return httpx.Response(
                206, content=two_chunks[100:], headers={"Content-Range": "bytes 100-149/*"}
            )
        return httpx.Response(206, content=b"")

    dest2 = tmp_path / "wildcard_zero.m4s"
    with httpx.Client(transport=httpx.MockTransport(handler_zero_eof)) as client:
        written2 = video_worker_mod.download_cdn_stream(
            client,
            ["https://upos-sz-mirrorcosov.bilivideo.com/v2.m4s"],
            dest2,
            max_bytes=video_worker_mod.MAX_BYTES,
            sleep_fn=lambda _: None,
        )
    assert written2 == 150
    assert dest2.read_bytes() == two_chunks

    # 416 on initial bytes=0- still triggers failover to backup URL
    def handler_initial_416(req: httpx.Request) -> httpx.Response:
        if req.url.host == "upos-sz-mirrorcosov.bilivideo.com":
            return httpx.Response(416)
        return httpx.Response(206, content=b"backup-ok", headers={"Content-Range": "bytes 0-8/9"})

    dest3 = tmp_path / "failover_416.m4s"
    with httpx.Client(transport=httpx.MockTransport(handler_initial_416)) as client:
        written3 = video_worker_mod.download_cdn_stream(
            client,
            [
                "https://upos-sz-mirrorcosov.bilivideo.com/v.m4s",
                "https://upos-hz-mirrorakam.akamaized.net/v.m4s",
            ],
            dest3,
            max_bytes=video_worker_mod.MAX_BYTES,
            sleep_fn=lambda _: None,
        )
    assert written3 == 9
    assert dest3.read_bytes() == b"backup-ok"


def test_video_worker_reserves_mux_headroom_and_sets_fsize_limit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(video_worker_mod, "MAX_BYTES", 10 * 1024 * 1024)
    monkeypatch.setattr(video_worker_mod, "MUX_HEADROOM_BYTES", 2 * 1024 * 1024)

    rlimits: dict[int, tuple[int, int]] = {}

    def fake_setrlimit(res, limits):
        rlimits[res] = limits

    with monkeypatch.context() as mp:
        mp.setattr(video_worker_mod.resource, "setrlimit", fake_setrlimit)
        mp.setattr(video_worker_mod, "execute", lambda _cfg: {"status": "ok", "duration": 5.0})
        (tmp_path / "request.json").write_text(
            json.dumps({"bvid": "BV1xx411c7mD", "max_seconds": 60, "page": 1})
        )
        assert video_worker_mod.main() == 0
    assert rlimits[video_worker_mod.resource.RLIMIT_FSIZE] == (12 * 1024 * 1024, 12 * 1024 * 1024)

    # Verify execute() enforces dash_budget and validates final st_size <= MAX_BYTES
    seen_max_bytes: list[int] = []

    def fake_dl(_client, _urls, dest, *, max_bytes, sleep_fn=None):
        del sleep_fn
        seen_max_bytes.append(max_bytes)
        dest.write_bytes(b"0" * 1024)
        return 1024

    def fake_mux_oversized(_v, _a, out_path):
        out_path.write_bytes(b"X" * (10 * 1024 * 1024 + 1))

    def api_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/x/web-interface/nav":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "wbi_img": {
                            "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
                            "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
                        }
                    },
                },
            )
        if req.url.path == "/x/web-interface/view":
            return httpx.Response(
                200,
                json={"code": 0, "data": {"bvid": "BV1xx411c7mD", "cid": 111, "duration": 10}},
            )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "dash": {
                        "video": [
                            {
                                "id": 16,
                                "width": 640,
                                "height": 360,
                                "bandwidth": 100_000,
                                "codecs": "avc1.64001e",
                                "mimeType": "video/mp4",
                                "baseUrl": "https://upos-sz-mirrorcosov.bilivideo.com/v.m4s",
                            }
                        ],
                        "audio": [
                            {
                                "id": 30216,
                                "bandwidth": 64_000,
                                "codecs": "mp4a.40.2",
                                "baseUrl": "https://upos-sz-mirrorcosov.bilivideo.com/a.m4s",
                            }
                        ],
                    }
                },
            },
        )

    monkeypatch.setattr(video_worker_mod, "download_cdn_stream", fake_dl)
    monkeypatch.setattr(video_worker_mod, "_mux_dash_streams", fake_mux_oversized)
    with httpx.Client(transport=httpx.MockTransport(api_handler)) as client:
        with pytest.raises(video_worker_mod.WorkerError, match="video_download_size"):
            video_worker_mod.execute(
                {"bvid": "BV1xx411c7mD", "max_seconds": 60, "page": 1},
                http_client=client,
                sleep_fn=lambda _: None,
            )
    assert seen_max_bytes[0] == 8 * 1024 * 1024
    assert seen_max_bytes[1] == 8 * 1024 * 1024 - 1024


# ---------------------------------------------------------------------------
# Wave 2: GCP Billing credential cache, off-loop jwt.encode, backoff, 3600s, 401 retry
# ---------------------------------------------------------------------------


async def test_gcp_billing_caches_sa_validation_and_offloads_jwt_sign(store, monkeypatch):
    sa1 = make_sa_info(
        project_id="cache-proj-1", client_email="a@cache-proj-1.iam.gserviceaccount.com"
    )
    sa2 = make_sa_info(
        project_id="cache-proj-2", client_email="b@cache-proj-2.iam.gserviceaccount.com"
    )
    active_sa = {"info": sa1}

    class _FakeRouting:
        def service_account_info(self):
            return active_sa["info"], "admin"

    store.runtime.provider_routing = _FakeRouting()

    validate_calls = 0
    orig_validate = gcp_billing_mod.validate_service_account_json

    def spy_validate(raw):
        nonlocal validate_calls
        validate_calls += 1
        return orig_validate(raw)

    monkeypatch.setattr(gcp_billing_mod, "validate_service_account_json", spy_validate)

    mgr = GcpBillingManager(store)
    await mgr.initialize()
    assert validate_calls == 1

    # Repeated status() polls reuse cached validated credentials (0 additional RSA parses)
    for _ in range(5):
        st = await mgr.status()
        assert st["credentials"]["service_account_email"] == sa1["client_email"]
    assert validate_calls == 1

    # Changing SA fingerprint triggers a single re-validation
    active_sa["info"] = sa2
    st2 = await mgr.status()
    assert st2["credentials"]["service_account_email"] == sa2["client_email"]
    assert validate_calls == 2
    await mgr.status()
    assert validate_calls == 2

    # Verify jwt.encode runs on a worker thread (not the event-loop thread)
    main_tid = threading.get_ident()
    encode_tids: list[int] = []
    orig_encode = gcp_billing_mod.jwt.encode

    def spy_encode(signer, payload):
        encode_tids.append(threading.get_ident())
        return orig_encode(signer, payload)

    monkeypatch.setattr(gcp_billing_mod.jwt, "encode", spy_encode)

    def tok_handler(req: httpx.Request) -> httpx.Response:
        if str(req.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "tok-thread", "expires_in": 3600})
        return httpx.Response(200, json={"timeSeries": []})

    await mgr.set_config(
        GcpBillingConfig(monitoring_enabled=True, monitoring_project="cache-proj-2")
    )
    await mgr.sync_monitoring(transport=httpx.MockTransport(tok_handler))
    assert encode_tids and all(tid != main_tid for tid in encode_tids)


async def test_gcp_billing_tick_exponential_backoff_and_401_retry(store):
    now = {"t": 50_000.0}
    sa = make_sa_info(project_id="backoff-proj-1")
    counts = {
        "token": 0,
        "bq": 0,
        "mon": 0,
        "fail_all": True,
        "force_401_bq": 0,
        "force_401_mon": 0,
    }
    seen_alignment: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        if str(req.url) == TOKEN_URI:
            counts["token"] += 1
            return httpx.Response(
                200, json={"access_token": f"tok-{counts['token']}", "expires_in": 3600}
            )
        if "/bigquery/" in str(req.url):
            counts["bq"] += 1
            if counts["force_401_bq"] > 0:
                counts["force_401_bq"] -= 1
                return httpx.Response(401, json={"error": "unauthenticated"})
            if counts["fail_all"]:
                return httpx.Response(503, json={"error": "down"})
            return httpx.Response(200, json={"jobComplete": True, "rows": []})
        counts["mon"] += 1
        seen_alignment.append(req.url.params.get("aggregation.alignmentPeriod", ""))
        if counts["force_401_mon"] > 0:
            counts["force_401_mon"] -= 1
            return httpx.Response(401, json={"error": "unauthenticated"})
        if counts["fail_all"]:
            return httpx.Response(503, json={"error": "down"})
        return httpx.Response(200, json={"timeSeries": []})

    mgr = GcpBillingManager(store, transport=httpx.MockTransport(handler), clock=lambda: now["t"])
    await mgr.initialize()
    await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
    )
    await mgr.set_config(
        GcpBillingConfig(
            bigquery_enabled=True,
            billing_project="backoff-proj-1",
            billing_table="ds.tbl",
            monitoring_enabled=True,
            monitoring_project="backoff-proj-1",
        )
    )

    # Failure 1 -> backoff 60s
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (1, 1)
    assert seen_alignment[-1] == "3600s"

    # 30s later: still in 60s backoff -> 0 upstream calls
    now["t"] += 30
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (1, 1)

    # 60s elapsed -> Failure 2 -> backoff 300s
    now["t"] += 30
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (2, 2)
    now["t"] += 299
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (2, 2)

    # 300s elapsed -> Failure 3 -> backoff 900s
    now["t"] += 1
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (3, 3)
    now["t"] += 899
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (3, 3)

    # 900s elapsed -> Failure 4 -> backoff 3600s
    now["t"] += 1
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (4, 4)
    now["t"] += 3599
    await mgr.tick()
    assert (counts["bq"], counts["mon"]) == (4, 4)

    # Config update resets backoff immediately
    await mgr.set_config(
        GcpBillingConfig(
            bigquery_enabled=True,
            billing_project="backoff-proj-1",
            billing_table="ds.tbl",
            monitoring_enabled=True,
            monitoring_project="backoff-proj-1",
        )
    )
    assert mgr._bq_next_retry_at == 0.0 and mgr._mon_next_retry_at == 0.0

    # Test HTTP 401 token eviction + single transparent retry on both BigQuery and Monitoring
    counts["fail_all"] = False
    counts["force_401_bq"] = 1
    counts["force_401_mon"] = 1
    tok_before = counts["token"]
    await mgr.sync_bigquery()
    assert counts["token"] >= tok_before + 1  # evicted cached token and re-fetched
    tok_before_mon = counts["token"]
    await mgr.sync_monitoring()
    assert counts["token"] >= tok_before_mon + 1


# ---------------------------------------------------------------------------
# Wave 2: render_card_async direct calls (outgoing + QQ adapters)
# ---------------------------------------------------------------------------


async def test_outgoing_and_qq_adapters_use_render_card_async_directly(
    settings, message, monkeypatch
):
    async_calls: list[str] = []
    valid_png_uri = "base64://" + base64.b64encode(_png_bytes()).decode("ascii")

    async def spy_render_card_async(text: str, **_kwargs) -> str:
        async_calls.append(text)
        return valid_png_uri

    def forbid_sync_render_card(_text: str) -> str:
        raise AssertionError("sync render_card must not be called via to_thread")

    monkeypatch.setattr(outgoing_mod, "render_card_async", spy_render_card_async)
    monkeypatch.setattr(text_card_mod, "render_card_async", spy_render_card_async)
    monkeypatch.setattr(text_card_mod, "render_card", forbid_sync_render_card)

    # 1. Non-OneBot outgoing.send_platform_card
    sent_media: list[dict[str, Any]] = []

    class _CardModule:
        supports_rich_card = True
        supports_reply_anchor = False

        async def send_media(self, _msg, raw: bytes, **kwargs):
            sent_media.append({"len": len(raw), **kwargs})
            return "42"

    tg_group = message.model_copy(
        update={
            "platform": "telegram",
            "kind": "group",
            "conversation_id": "-1001",
            "user_id": "20001",
        }
    )
    monkeypatch.setattr(outgoing_mod, "chat_module", lambda _s, _p: _CardModule())
    monkeypatch.setattr(outgoing_mod, "authorize", lambda _m, _s: True)
    svc = SimpleNamespace(s=settings, secrets=())
    res = await send_platform_card(svc, tg_group, "# Card Title")
    assert res == {"message_id": "42"}
    assert async_calls == ["# Card Title"]
    assert sent_media and sent_media[0]["kind"] == "card"

    # 2. OneBot Sender.send_card & Sender.send_folded fallback
    settings.onebot_allowed_groups = {"40000"}
    qq_group = message.model_copy(
        update={"platform": "onebot", "kind": "group", "conversation_id": "40000"}
    )

    class _Wire:
        async def call(self, action, params):
            del params
            if action == "send_group_forward_msg":
                raise Rejected("onebot_action_unsupported")
            return {"message_id": 7001}

    sender = Sender(settings, httpx.AsyncClient(), _Wire())
    r_card = await sender.send_card(qq_group, "# QQ Card")
    assert r_card == {"message_id": 7001}
    r_fold = await sender.send_folded(
        qq_group, "# QQ Folded", before_send=lambda: asyncio.sleep(0, True)
    )
    assert r_fold == {"message_id": 7001}
    assert async_calls == ["# Card Title", "# QQ Card", "# QQ Folded"]


# ---------------------------------------------------------------------------
# Wave 2: MemeLibrary offloaded fsync, single hash, coalesced transaction
# ---------------------------------------------------------------------------


async def test_memes_offloads_fsync_single_hash_and_single_tx(store, monkeypatch):
    lib = MemeLibrary(store)
    await lib.initialize()

    main_tid = threading.get_ident()
    write_tids: list[int] = []
    unlink_tids: list[int] = []
    orig_write = lib._write
    orig_unlink = lib._unlink

    def spy_write(filename, raw):
        write_tids.append(threading.get_ident())
        return orig_write(filename, raw)

    def spy_unlink(filename):
        unlink_tids.append(threading.get_ident())
        return orig_unlink(filename)

    monkeypatch.setattr(lib, "_write", spy_write)
    monkeypatch.setattr(lib, "_unlink", spy_unlink)

    tx_count = 0
    orig_tx = store.transaction

    @asynccontextmanager
    async def spy_tx():
        nonlocal tx_count
        tx_count += 1
        async with orig_tx():
            yield

    monkeypatch.setattr(store, "transaction", spy_tx)

    raw = _png_bytes("blue")
    sha_inputs: list[int] = []
    orig_sha256 = hashlib.sha256

    def spy_sha256(data=b"", *args, **kwargs):
        sha_inputs.append(len(data))
        return orig_sha256(data, *args, **kwargs)

    monkeypatch.setattr("hakimi.memes.hashlib.sha256", spy_sha256)

    entry = await lib.upload("开心.png", raw)
    assert tx_count == 1  # Coalesced meme_library + meme_source_hashes into ONE tx
    assert write_tids and all(tid != main_tid for tid in write_tids)
    # One hash for normalized.data + one hash for raw source (no duplicate hash of normalized.data)
    assert len(sha_inputs) == 2

    await lib.delete(entry["id"])
    assert unlink_tids and all(tid != main_tid for tid in unlink_tids)


# ---------------------------------------------------------------------------
# Wave 2: SongLibrary offloaded fsync, init skip unlink, debounced last_used_at,
#         and Drive probe TTL cache + invalidation + manual refresh
# ---------------------------------------------------------------------------


async def test_songs_init_skip_unlink_and_debounced_unlocked_last_used_at(
    store, monkeypatch, tmp_path
):
    raw = _mp3_bytes(b"debounce-track-1", total_size=320)
    digest = hashlib.sha256(raw).hexdigest()

    class _DriveStub:
        def configured(self) -> bool:
            return True

        async def download_song(self, file_id: str) -> bytes:
            assert file_id == "drv-1"
            return raw

    lib = SongLibrary(store, drive=_DriveStub())
    await lib.initialize()

    entry = await lib.register_colab_cover(
        title="测试歌",
        digest=digest,
        byte_size=len(raw),
        duration_ms=10_000,
        drive_chorus_id="drv-1",
        chorus_bytes=None,
    )

    # Re-initializing with absent Drive-backed cache file must NOT call _unlink
    reloaded = SongLibrary(store, drive=_DriveStub())
    unlinks: list[str] = []
    orig_unlink = reloaded._unlink

    def spy_unlink(fname: str) -> bool:
        unlinks.append(fname)
        return orig_unlink(fname)

    monkeypatch.setattr(reloaded, "_unlink", spy_unlink)
    await reloaded.initialize()
    assert unlinks == []

    # Track SQLite write transactions and assert reloaded.lock is NOT held during last_used_at write
    tx_lock_states: list[bool] = []
    orig_tx = store.transaction

    @asynccontextmanager
    async def spy_tx():
        tx_lock_states.append(reloaded.lock.locked())
        async with orig_tx():
            yield

    monkeypatch.setattr(store, "transaction", spy_tx)

    fake_now = [1_800_000_000.0]
    monkeypatch.setattr("hakimi.songs.time.time", lambda: fake_now[0])

    # First playback hydrates from Drive -> persists last_used_at OUTSIDE reloaded.lock
    await reloaded.audio_bytes(entry["id"])
    assert tx_lock_states == [False]

    # Repeated playback within 300s updates in-memory last_used_at, skips SQLite write!
    fake_now[0] += 60.0
    await reloaded.audio_bytes(entry["id"])
    assert tx_lock_states == [False]
    assert reloaded._entries[entry["id"]]["last_used_at"] == fake_now[0]

    # Playback after >= 300s triggers another debounced SQLite write OUTSIDE reloaded.lock
    fake_now[0] += LAST_USED_PERSIST_INTERVAL_SECONDS
    await reloaded.audio_bytes(entry["id"])
    assert tx_lock_states == [False, False]

    # delete() offloads _unlink to worker thread
    main_tid = threading.get_ident()
    del_tids: list[int] = []

    def spy_del_unlink(fname: str) -> bool:
        del_tids.append(threading.get_ident())
        return orig_unlink(fname)

    monkeypatch.setattr(reloaded, "_unlink", spy_del_unlink)
    await reloaded.delete(entry["id"])
    assert del_tids and all(tid != main_tid for tid in del_tids)


async def test_song_admin_drive_probe_ttl_cache_invalidation_and_manual_refresh(
    settings, tmp_path: Path
):
    oauth = _write_oauth_json(tmp_path / "roaming-oauth.json")
    s = settings.model_copy(
        update={
            "data_dir": tmp_path / "song_cache_data",
            "admin_token": SecretStr(ADMIN_TOKEN),
            "song_drive_oauth_file": oauth,
        }
    )
    srv = _FakeGoogleDriveServer()
    srv.add_file("inbox_1", name="track1.m4a", parent_id=srv.inbox_id, data=b"M4A_1")
    srv.add_file("ref_1", name="hakimi_ref.wav", parent_id=srv.models_id, data=b"WAV_1")

    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with srv.client() as mock_http:
        async with app.router.lifespan_context(app):
            lib: SongLibrary = app.state.service.songs
            lib.drive._injected_http = mock_http
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            ) as client:
                # 1. Cold calls hit Drive once each, then repeated calls hit cache (0 new HTTPS)
                assert (await client.get("/admin/api/songs/storage")).status_code == 200
                assert (await client.get("/admin/api/songs/inbox")).status_code == 200
                assert (await client.get("/admin/api/songs/voices")).status_code == 200
                req_after_warm = len(srv.requests)

                for _ in range(3):
                    assert (await client.get("/admin/api/songs/storage")).status_code == 200
                    assert (await client.get("/admin/api/songs/inbox")).status_code == 200
                    assert (await client.get("/admin/api/songs/voices")).status_code == 200
                assert len(srv.requests) == req_after_warm

                # 2. Query params remain forbidden (400)
                assert (await client.get("/admin/api/songs/inbox?refresh=1")).status_code == 400

                # 3. Manual refresh via header and POST /refresh bypasses cache
                srv.add_file("inbox_ext", name="external.flac", parent_id=srv.inbox_id, data=b"F")
                # Cached GET still sees old list until refreshed
                assert len((await client.get("/admin/api/songs/inbox")).json()["items"]) == 1
                # Header refresh sees new file
                refreshed_hdr = await client.get(
                    "/admin/api/songs/inbox", headers={"X-Force-Refresh": "1"}
                )
                assert refreshed_hdr.status_code == 200
                assert len(refreshed_hdr.json()["items"]) == 2
                # POST /refresh endpoints work and return exact schemas
                assert (await client.post("/admin/api/songs/storage/refresh")).status_code == 200
                assert (await client.post("/admin/api/songs/inbox/refresh")).status_code == 200
                assert (await client.post("/admin/api/songs/voices/refresh")).status_code == 200

                # 4. Mutations immediately invalidate corresponding caches
                up_inbox = await client.post(
                    "/admin/api/songs/inbox",
                    content=b"ID3NEW_INBOX",
                    headers={"Content-Type": "audio/mpeg", "X-Drive-Filename": "new_song.mp3"},
                )
                assert up_inbox.status_code == 200
                inbox_after = (await client.get("/admin/api/songs/inbox")).json()["items"]
                assert any(item["name"] == "new_song.mp3" for item in inbox_after)

                up_voice = await client.post(
                    "/admin/api/songs/ref-voices",
                    content=b"RIFF_NEW_VOICE",
                    headers={"Content-Type": "audio/wav", "X-Drive-Filename": "new_ref.wav"},
                )
                assert up_voice.status_code == 200
                voices_after = (await client.get("/admin/api/songs/voices")).json()["ref_voices"]
                assert any(item["name"] == "new_ref.wav" for item in voices_after)


# ---------------------------------------------------------------------------
# Wave 2: ModelAudio.validate() base64 decode caching & tamper detection
# ---------------------------------------------------------------------------


def test_model_audio_validate_caches_decode_and_revalidates_on_tamper(monkeypatch):
    raw = b"ID3-audio-payload-123456"
    audio = ModelAudio(
        data=base64.b64encode(raw).decode("ascii"),
        mime_type="audio/mp3",
        duration_sec=5.0,
        byte_size=len(raw),
    )

    decode_calls = 0
    orig_b64decode = audio_media_mod.base64.b64decode

    def spy_b64decode(*args, **kwargs):
        nonlocal decode_calls
        decode_calls += 1
        return orig_b64decode(*args, **kwargs)

    monkeypatch.setattr(audio_media_mod.base64, "b64decode", spy_b64decode)

    assert audio.validate() == len(raw)
    assert audio.validate() == len(raw)
    assert audio.validate() == len(raw)
    assert decode_calls == 1

    # Tampered data string MUST bypass cache and raise Rejected
    object.__setattr__(audio, "data", "%%%NOT-VALID-BASE64%%%")
    with pytest.raises(Rejected, match="invalid_audio"):
        audio.validate()
    assert decode_calls == 2

    # Tampered mime_type or duration_sec still rejected
    audio2 = ModelAudio(
        data=base64.b64encode(raw).decode("ascii"),
        mime_type="audio/mp3",
        duration_sec=5.0,
    )
    assert audio2.validate() == len(raw)
    object.__setattr__(audio2, "mime_type", "text/plain")
    with pytest.raises(Rejected, match="invalid_audio"):
        audio2.validate()


# ---------------------------------------------------------------------------
# Wave 2: ColabComputeHub._compute_analytics() caching & mutation invalidation
# ---------------------------------------------------------------------------


async def test_colab_analytics_cache_and_invalidation(settings, store, monkeypatch):
    clock_now = [1_760_000_000.0]
    s = settings.model_copy(
        update={
            "colab_worker_url": "http://127.0.0.1:12146",
            "colab_worker_token": SecretStr("worker-secret-token-cache-test"),
        }
    )

    def mock_worker(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/v1/status":
            return httpx.Response(
                200,
                json={
                    "reachable": True,
                    "compute_units": 50.0,
                    "active_sessions": 0,
                    "running_job_id": None,
                    "supported_job_types": ["rvc_cover"],
                    "warm_sessions": [],
                    "error": None,
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(mock_worker), base_url="http://127.0.0.1:12146"
    ) as http:
        hub = ColabComputeHub(s, store, None, http=http, clock=lambda: clock_now[0])
        await hub.initialize()
        hub.register_job_type("rvc_cover", validator=validate_rvc_cover_payload)

        job = await hub.submit_job("rvc_cover", {"title": "缓存测试", "inbox_filename": "song.m4a"})
        await hub._finalize_job(
            job["id"],
            status="succeeded",
            assigned_gpu="L4",
            cu_used=0.5,
            result={"acceleration": {"drive_direct": True, "warm_reused": True}},
        )

        # Warm the analytics cache
        open_calls = 0
        orig_open = hub._open_blob

        def spy_open(jid, kind, blob):
            nonlocal open_calls
            if kind == "result" and blob:
                open_calls += 1
            return orig_open(jid, kind, blob)

        monkeypatch.setattr(hub, "_open_blob", spy_open)

        ov1 = await hub.admin_overview(force_refresh=False)
        assert ov1["summary"]["succeeded_jobs"] == 1
        assert ov1["acceleration_stats"]["measured_jobs"] == 1
        assert open_calls == 2  # 1 in list_jobs + 1 in _compute_analytics

        # Subsequent _compute_analytics calls hit cache with zero result decrypts
        open_calls = 0
        a1 = await hub._compute_analytics([], hub._last_warm_pool)
        a2 = await hub._compute_analytics([], hub._last_warm_pool)
        assert a1["summary"]["succeeded_jobs"] == 1
        assert a2["summary"]["succeeded_jobs"] == 1
        assert open_calls == 0

        # Mutating via submit_job / cancel_job invalidates cache immediately
        job2 = await hub.submit_job(
            "rvc_cover", {"title": "缓存测试2", "inbox_filename": "song2.m4a"}
        )
        open_calls = 0
        a3 = await hub._compute_analytics([], hub._last_warm_pool)
        assert a3["summary"]["queued_jobs"] == 1
        assert open_calls == 1

        await hub.cancel_job(job2["id"])
        open_calls = 0
        a4 = await hub._compute_analytics([], hub._last_warm_pool)
        assert a4["summary"]["cancelled_jobs"] == 1
        assert open_calls == 1

        # Midnight rollover recomputes cleanly
        open_calls = 0
        clock_now[0] += 86400.0
        a5 = await hub._compute_analytics([], hub._last_warm_pool)
        assert a5["summary"]["total_jobs"] == 2
        assert open_calls == 1

        await hub.close()


# ---------------------------------------------------------------------------
# Wave 2: Affection index + 2-phase run_cooldown_recovery (zero write tx on idle,
#         no decrypt inside write tx)
# ---------------------------------------------------------------------------


async def test_affection_history_index_and_two_phase_cooldown_recovery(store, monkeypatch):
    clock = [1_750_000_000.0]
    aff = AffectionManager(store, clock=lambda: clock[0])
    await aff.initialize()
    await aff.set_config(AffectionConfig(enabled=True, cooldown_recovery_days=7))

    # Verify affection_history_scope_created index exists
    idx_rows = await store.all("PRAGMA index_list(affection_history)")
    idx_names = {r["name"] for r in idx_rows}
    assert "affection_history_scope_created" in idx_names

    # Seed one user below target (score 35) and one healthy user (score 50)
    await aff.set_user_score("30001", 35, reason="drop")
    await aff.set_user_score("30002", 50, reason="ok")

    in_write_tx = False
    write_tx_count = 0
    decrypt_inside_tx = 0
    decrypt_inside_aff_lock = 0

    orig_tx = store.transaction
    orig_open = store.vault.open

    @asynccontextmanager
    async def spy_tx():
        nonlocal in_write_tx, write_tx_count
        write_tx_count += 1
        in_write_tx = True
        try:
            async with orig_tx():
                yield
        finally:
            in_write_tx = False

    def spy_open(ctx, blob):
        nonlocal decrypt_inside_tx, decrypt_inside_aff_lock
        if ctx.startswith("affection:"):
            if in_write_tx:
                decrypt_inside_tx += 1
            if aff._lock.locked():
                decrypt_inside_aff_lock += 1
        return orig_open(ctx, blob)

    monkeypatch.setattr(store, "transaction", spy_tx)
    monkeypatch.setattr(store.vault, "open", spy_open)

    # Before cooldown elapses: 0 qualifying users -> ZERO write tx, ZERO decrypts under lock/tx!
    clock[0] += 3 * 86400.0
    assert await aff.run_cooldown_recovery() == 0
    assert write_tx_count == 0
    assert decrypt_inside_tx == 0
    assert decrypt_inside_aff_lock == 0

    # After 7 days: 1 qualifying user -> 1 write tx, all decrypts in Phase 1 (0 inside tx)
    clock[0] += 5 * 86400.0
    assert await aff.run_cooldown_recovery() == 1
    assert write_tx_count == 1
    assert decrypt_inside_tx == 0
    assert decrypt_inside_aff_lock == 0
    assert (await aff.profile("onebot", "30001"))["score"] == 36
    assert (await aff.profile("onebot", "30002"))["score"] == 50
