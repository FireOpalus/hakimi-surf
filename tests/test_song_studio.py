"""Tests for song_studio system module, Drive primary storage, LRU cache, and song_admin."""

from __future__ import annotations

import asyncio
import hashlib
import io
import math
import os
import struct
import sys
import time
import uuid
import wave
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.modules import ModuleRegistry
from hakimi.security import Rejected
from hakimi.songs import MAX_LEARN_UPLOAD_BYTES, MAX_STORED_SONG_BYTES, SongLibrary
from hakimi.system_modules import SystemResources
from hakimi.system_modules.song_studio import Module as SongStudioModule
from tests.fakes import FakeGateway

ADMIN_TOKEN = "song-studio-test-admin-token-never-live"


def _make_mp3_bytes(payload: bytes, *, total_size: int | None = None) -> bytes:
    header = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00"
    raw = header + payload
    if total_size is not None and len(raw) < total_size:
        raw = raw + b"\x00" * (total_size - len(raw))
    return raw


def _make_sine_wav(duration_ms: int = 3000, *, sample_rate: int = 16000) -> bytes:
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


@pytest.fixture
def fake_ffmpeg_bins(tmp_path: Path) -> tuple[str, str]:
    bin_dir = tmp_path / "song_studio_shim"
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
                "dur = '12'",
                "fmt = 'mp3'",
                "for idx, a in enumerate(args):",
                "    if a == '-i' and idx + 1 < len(args): in_file = args[idx + 1]",
                "    elif a == '-t' and idx + 1 < len(args): dur = args[idx + 1]",
                "    elif a == '-f' and idx + 1 < len(args): fmt = args[idx + 1]",
                "out_file = args[-1]",
                "raw = Path(in_file).read_bytes()",
                "if not raw or raw.startswith(b'BAD_AUDIO'): sys.exit(1)",
                "digest = hashlib.sha256(raw).digest()",
                "if fmt == 'ogg':",
                "    Path(out_file).write_bytes(b'OggS\\x00\\x02' + digest + raw[:16])",
                "else:",
                "    hdr = b'ID3\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\xfb\\x90\\x00'",
                "    Path(out_file).write_bytes(hdr + digest + json.dumps({'t': dur}).encode())",
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
                "meta = json.loads(data[idx:].decode()) if idx >= 0 else {}",
                "print(f\"{min(58.0, max(1.5, float(meta.get('t', '10.0')))):.6f}\")",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(ffmpeg_path, 0o755)
    os.chmod(ffprobe_path, 0o755)
    return str(ffmpeg_path), str(ffprobe_path)


class _FakeColabCompute:
    def __init__(self, *, is_configured: bool = True) -> None:
        self.is_configured = is_configured
        self.drive_files: dict[str, bytes] = {}
        self.download_calls: list[str] = []
        self.inbox_uploads: list[tuple[str, bytes, str]] = []
        self.submitted_jobs: list[tuple[str, dict[str, Any], str]] = []
        self.hooks: dict[str, Any] = {}
        self.download_error: Exception | None = None
        self.submit_error: Exception | None = None

    def configured(self) -> bool:
        return self.is_configured

    async def download_drive_song(self, file_id: str) -> bytes:
        self.download_calls.append(file_id)
        if self.download_error is not None:
            raise self.download_error
        if file_id not in self.drive_files:
            raise Rejected("colab_drive_invalid_file")
        return self.drive_files[file_id]

    async def upload_drive_inbox(
        self, filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
    ) -> dict[str, Any]:
        self.inbox_uploads.append((filename, raw, mime_type))
        return {
            "file_id": f"inbox-id-{len(self.inbox_uploads)}",
            "name": filename,
            "size": len(raw),
        }

    async def submit_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        requester: str = "owner",
        gpu: str | None = None,
    ) -> dict[str, Any]:
        if self.submit_error is not None:
            raise self.submit_error
        self.submitted_jobs.append((job_type, dict(payload), requester))
        job_id = uuid.uuid4().hex
        return {
            "id": job_id,
            "job_type": job_type,
            "status": "queued",
            "requester": requester,
            "payload": dict(payload),
            "created_at": time.time(),
        }

    def register_completion_hook(self, job_type: str, callback: Any) -> None:
        self.hooks[job_type] = callback


class _FakeProactive:
    def __init__(self) -> None:
        self.alerts: list[tuple[str, str]] = []

    async def alert(self, code: str, text: str, *, daily: bool = False) -> str:
        self.alerts.append((code, text))
        return uuid.uuid4().hex


async def test_legacy_schema_migration_and_initialize_preserves_evicted_drive_entries(
    settings, store
):
    await store.db.executescript(
        """
        DROP TABLE IF EXISTS song_library;
        CREATE TABLE song_library (
          id TEXT PRIMARY KEY,
          title TEXT UNIQUE NOT NULL,
          artist TEXT NOT NULL DEFAULT '',
          tags TEXT NOT NULL DEFAULT '',
          filename TEXT UNIQUE NOT NULL,
          mime_type TEXT NOT NULL,
          digest TEXT UNIQUE NOT NULL,
          byte_size INTEGER NOT NULL,
          duration_ms INTEGER NOT NULL,
          created REAL NOT NULL
        );
        """
    )
    songs_dir = settings.data_dir / "songs"
    songs_dir.mkdir(parents=True, exist_ok=True)

    local_ok_bytes = _make_mp3_bytes(b"local-valid-mp3")
    local_ok_id = uuid.uuid4().hex
    (songs_dir / f"{local_ok_id}.mp3").write_bytes(local_ok_bytes)

    local_missing_id = uuid.uuid4().hex
    local_missing_bytes = _make_mp3_bytes(b"local-missing-mp3")

    async with store.transaction():
        for sid, title, raw in (
            (local_ok_id, "旧本地有效歌", local_ok_bytes),
            (local_missing_id, "旧本地缺失歌", local_missing_bytes),
        ):
            await store.db.execute(
                "INSERT INTO song_library("
                "id,title,artist,tags,filename,mime_type,digest,byte_size,duration_ms,created"
                ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    sid,
                    title,
                    "歌手",
                    "标签",
                    f"{sid}.mp3",
                    "audio/mpeg",
                    hashlib.sha256(raw).hexdigest(),
                    len(raw),
                    5000,
                    time.time(),
                ),
            )

    lib = SongLibrary(store)
    await lib.initialize()

    cols = {r["name"] for r in await store.all("PRAGMA table_info(song_library)")}
    assert {"drive_chorus_id", "drive_full_id", "source_origin", "last_used_at"} <= cols
    # Idempotent second migration check
    lib2 = SongLibrary(store)
    await lib2.initialize()
    assert lib2.titles == ("旧本地有效歌",)

    # Now add two Drive-backed entries: one evicted (no file), one corrupted local cache file.
    evicted_id = uuid.uuid4().hex
    evicted_bytes = _make_mp3_bytes(b"drive-evicted-mp3")
    corrupt_id = uuid.uuid4().hex
    corrupt_bytes = _make_mp3_bytes(b"drive-corrupt-mp3")
    corrupt_path = songs_dir / f"{corrupt_id}.mp3"
    corrupt_path.write_bytes(b"CORRUPTED_BYTES")

    async with store.transaction():
        for sid, title, raw, drv_id in (
            (evicted_id, "云盘已淘汰歌", evicted_bytes, "drv-evicted-1"),
            (corrupt_id, "云盘损坏缓存歌", corrupt_bytes, "drv-corrupt-2"),
        ):
            await store.db.execute(
                "INSERT INTO song_library("
                "id,title,artist,tags,filename,mime_type,digest,byte_size,duration_ms,created,"
                "drive_chorus_id,drive_full_id,source_origin,last_used_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sid,
                    title,
                    "",
                    "",
                    f"{sid}.mp3",
                    "audio/mpeg",
                    hashlib.sha256(raw).hexdigest(),
                    len(raw),
                    8000,
                    time.time(),
                    drv_id,
                    "",
                    "colab",
                    0.0,
                ),
            )

    reloaded = SongLibrary(store, compute=_FakeColabCompute())
    await reloaded.initialize()
    assert set(reloaded.titles) == {"旧本地有效歌", "云盘已淘汰歌", "云盘损坏缓存歌"}
    assert not corrupt_path.exists()
    listed = {item["title"]: item for item in await reloaded.list()}
    assert listed["旧本地有效歌"]["cached_local"] is True
    assert listed["旧本地有效歌"]["drive_backed"] is False
    assert listed["云盘已淘汰歌"]["cached_local"] is False
    assert listed["云盘已淘汰歌"]["drive_backed"] is True
    assert listed["云盘损坏缓存歌"]["cached_local"] is False


async def test_drive_rehydration_sha256_verification_and_concurrency(store, fake_ffmpeg_bins):
    ffm, ffp = fake_ffmpeg_bins
    compute = _FakeColabCompute()
    lib = SongLibrary(store, ffmpeg_bin=ffm, ffprobe_bin=ffp, compute=compute)
    await lib.initialize()

    mp3_ok = _make_mp3_bytes(b"good-drive-chorus-payload", total_size=512)
    digest_ok = hashlib.sha256(mp3_ok).hexdigest()
    compute.drive_files["drv-c1"] = mp3_ok

    registered = await lib.register_colab_cover(
        title="雨爱",
        artist="杨丞琳",
        tags="温柔",
        digest=digest_ok,
        byte_size=len(mp3_ok),
        duration_ms=12000,
        drive_chorus_id="drv-c1",
        drive_full_id="drv-f1",
    )
    assert registered["cached_local"] is False
    assert registered["drive_backed"] is True
    assert registered["drive_full_backed"] is True
    assert registered["source_origin"] == "colab"
    assert lib.titles == ("雨爱",)
    assert lib.cache_stats()["drive_backed_count"] == 1
    assert lib.cache_stats()["cached_count"] == 0

    # Concurrent reads hydrate from Drive exactly once
    res_a, res_b = await asyncio.gather(
        lib.audio_bytes(registered["id"]),
        lib.audio_bytes(registered["id"]),
    )
    assert res_a == (mp3_ok, "audio/mpeg")
    assert res_b == (mp3_ok, "audio/mpeg")
    assert compute.download_calls == ["drv-c1"]
    assert lib.cache_stats()["cached_count"] == 1

    # DB last_used_at updated
    row = await store.one("SELECT last_used_at FROM song_library WHERE id=?", (registered["id"],))
    assert row is not None and float(row["last_used_at"]) > 0.0

    # Subsequent load_for_delivery uses local hot cache without hitting Drive again
    ogg, fmt, mime, dur, canon = await lib.load_for_delivery("雨爱", preferred_fmt="ogg_opus")
    assert fmt == "ogg_opus" and mime == "audio/ogg" and dur == 12000 and canon == "雨爱"
    assert ogg.startswith(b"OggS")
    assert compute.download_calls == ["drv-c1"]

    # Bad Drive payloads must be rejected before touching local disk
    bad_cases = [
        (
            "drv-bad-hash",
            _make_mp3_bytes(b"wrong-drive-chorus-payload", total_size=len(mp3_ok)),
            len(mp3_ok),
            "0" * 64,
        ),
        ("drv-bad-hdr", b"NOT_MP3" + b"x" * 100, 107, None),
        ("drv-too-big", _make_mp3_bytes(b"big", total_size=MAX_STORED_SONG_BYTES + 10), 500, None),
    ]
    for idx, (drv_id, raw_bad, expected_len, custom_digest) in enumerate(bad_cases):
        bad_digest = custom_digest or hashlib.sha256(raw_bad[:expected_len]).hexdigest()
        compute.drive_files[drv_id] = raw_bad
        bad_entry = await lib.register_colab_cover(
            title=f"坏文件{idx}",
            digest=bad_digest,
            byte_size=expected_len,
            duration_ms=5000,
            drive_chorus_id=drv_id,
        )
        with pytest.raises(Rejected, match="song_unavailable"):
            await lib.audio_bytes(bad_entry["id"])
        assert not (lib.root / f"{bad_entry['id']}.mp3").exists()


async def test_lru_eviction_preserves_local_songs_and_sqlite_rows(store, fake_ffmpeg_bins):
    ffm, ffp = fake_ffmpeg_bins
    compute = _FakeColabCompute()
    custom_settings = SimpleNamespace(
        data_dir=store.s.data_dir,
        song_cache_max_bytes=900,
    )
    lib = SongLibrary(
        store,
        ffmpeg_bin=ffm,
        ffprobe_bin=ffp,
        compute=compute,
        settings=custom_settings,
    )
    await lib.initialize()

    # Pure local song (must NEVER be evicted!)
    local_entry = await lib.upload("纯本地曲目", _make_sine_wav(2000), filename="local.wav")
    local_path = lib.root / f"{local_entry['id']}.mp3"
    assert local_path.is_file()

    # Three Drive-backed covers of 400 bytes each (total 1200 > 900)
    entries: dict[str, dict[str, Any]] = {}
    for idx in (1, 2, 3):
        raw = _make_mp3_bytes(f"track-{idx}".encode(), total_size=400)
        compute.drive_files[f"drv-{idx}"] = raw
        entries[f"s{idx}"] = await lib.register_colab_cover(
            title=f"云盘曲目{idx}",
            digest=hashlib.sha256(raw).hexdigest(),
            byte_size=len(raw),
            duration_ms=8000,
            drive_chorus_id=f"drv-{idx}",
            chorus_bytes=raw,
        )
        await asyncio.sleep(0.01)

    # Registration of s3 already triggered eviction down to <= 900 -> s1 (oldest) evicted!
    assert not (lib.root / f"{entries['s1']['id']}.mp3").exists()
    assert (lib.root / f"{entries['s2']['id']}.mp3").is_file()
    assert (lib.root / f"{entries['s3']['id']}.mp3").is_file()
    assert local_path.is_file()

    # Touch s2 so s3 becomes the oldest cached Drive song, then rehydrate s1
    await asyncio.sleep(0.01)
    await lib.audio_bytes(entries["s2"]["id"])
    await asyncio.sleep(0.01)
    await lib.audio_bytes(entries["s1"]["id"])

    # Now s1 and s2 are hottest; s3 was evicted during s1 hydration!
    assert (lib.root / f"{entries['s1']['id']}.mp3").is_file()
    assert (lib.root / f"{entries['s2']['id']}.mp3").is_file()
    assert not (lib.root / f"{entries['s3']['id']}.mp3").exists()

    # Evict everything in hot cache (target_bytes=0): pure local song & all SQLite rows survive!
    evicted = await lib.evict_cache(target_bytes=0)
    assert evicted["evicted_count"] == 2
    assert evicted["freed_bytes"] == 800
    assert evicted["cache_bytes"] == 0
    assert evicted["cached_count"] == 0
    assert local_path.is_file()
    assert set(lib.titles) == {"纯本地曲目", "云盘曲目1", "云盘曲目2", "云盘曲目3"}
    row_count = await store.one("SELECT COUNT(*) FROM song_library")
    assert row_count is not None and row_count[0] == 4

    # Periodic worker evicts and handles CancelledError cleanly
    await lib.audio_bytes(entries["s1"]["id"])
    await lib.audio_bytes(entries["s2"]["id"])
    assert lib.cache_stats()["cache_bytes"] == 800
    custom_settings.song_cache_max_bytes = 450
    evict_done = asyncio.Event()
    orig_evict = lib.evict_cache

    async def _tracked_evict(*, target_bytes: int | None = None) -> dict[str, Any]:
        res = await orig_evict(target_bytes=target_bytes)
        evict_done.set()
        return res

    lib.evict_cache = _tracked_evict  # type: ignore[method-assign]
    task = asyncio.create_task(lib.run_cache_evictor(interval_seconds=0.05))
    try:
        async with asyncio.timeout(1.0):
            await evict_done.wait()
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        lib.evict_cache = orig_evict  # type: ignore[method-assign]
    assert lib.cache_stats()["cache_bytes"] == 400
    assert local_path.is_file()


async def test_request_learn_song_dual_track_priority_and_validation(store):
    unconfigured = SongLibrary(store, compute=_FakeColabCompute(is_configured=False))
    with pytest.raises(Rejected, match="colab_unconfigured"):
        await unconfigured.request_learn_song("晴天")

    compute = _FakeColabCompute(is_configured=True)
    lib = SongLibrary(store, compute=compute)
    await lib.initialize()

    with pytest.raises(Rejected, match="invalid_song_title"):
        await lib.request_learn_song("../bad")
    for bad_pitch in (-25, 25, True, 1.5):
        with pytest.raises(Rejected, match="invalid_song_pitch"):
            await lib.request_learn_song("晴天", pitch=bad_pitch)  # type: ignore[arg-type]
    with pytest.raises(Rejected, match="song_upload_too_large"):
        await lib.request_learn_song(
            "晴天",
            audio_bytes=b"x" * (MAX_LEARN_UPLOAD_BYTES + 1),
            audio_filename="big.mp3",
        )
    with pytest.raises(Rejected, match="song_source_required"):
        await lib.request_learn_song("晴天", auto_search=False)

    # Track 1: direct audio_bytes takes precedence over inbox_filename & auto_search
    job1 = await lib.request_learn_song(
        "晴天",
        artist="周杰伦",
        tags="经典",
        pitch=2,
        audio_bytes=b"RAW_AUDIO_BYTES",
        audio_filename="晴天_orig.m4a",
        inbox_filename="ignored.wav",
        auto_search=True,
        requester="owner",
    )
    assert job1["job_type"] == "rvc_cover"
    assert len(compute.inbox_uploads) == 1
    assert compute.inbox_uploads[0][0] == "晴天_orig.m4a"
    assert compute.inbox_uploads[0][2] == "audio/mp4"
    _, p1, req1 = compute.submitted_jobs[-1]
    assert req1 == "owner"
    assert p1["inbox_file_id"] == "inbox-id-1"
    assert p1["inbox_filename"] == "晴天_orig.m4a"
    assert p1["auto_search"] is False
    assert p1["pitch"] == 2

    # Track 2: Drive inbox file selection
    await lib.request_learn_song(
        "小幸运",
        inbox_filename="lucky.flac",
        auto_search=True,
    )
    _, p2, _ = compute.submitted_jobs[-1]
    assert p2["inbox_filename"] == "lucky.flac"
    assert p2["auto_search"] is False
    assert "inbox_file_id" not in p2

    # Track 3: Auto-search fallback
    await lib.request_learn_song("夜曲", artist="周杰伦", auto_search=True)
    _, p3, _ = compute.submitted_jobs[-1]
    assert p3["query"] == "夜曲"
    assert p3["auto_search"] is True


async def test_song_studio_module_hooks_and_owner_notifications(settings, store):
    compute = _FakeColabCompute(is_configured=True)
    proactive = _FakeProactive()

    class _FakeColabMod:
        module_id = "colab_compute"

        def __init__(self) -> None:
            self.compute = compute

    class _FakeDriveMod:
        module_id = "drive_sync"

    registry = ModuleRegistry()
    registry.systems["colab_compute"] = _FakeColabMod()  # type: ignore[assignment]
    registry.systems["drive_sync"] = _FakeDriveMod()  # type: ignore[assignment]

    resources = SystemResources(
        settings=settings,
        store=store,
        gateway=FakeGateway(),  # type: ignore[arg-type]
        http=None,
        service=SimpleNamespace(proactive=proactive),  # type: ignore[arg-type]
    )
    mod = SongStudioModule(resources, registry)
    assert mod.module_id == "song_studio"
    assert mod.requires == ("colab_compute", "drive_sync")
    assert mod.admin_panel == "songs"
    assert mod.drive_usage == "猫猫翻唱歌单与音频缓存"
    assert store.runtime.songs is mod.songs
    assert mod.workers() == (mod.songs.run_cache_evictor,)

    await mod.initialize()
    assert "rvc_cover" in compute.hooks
    status = await mod.status()
    assert status["state"] == "ready"
    assert status["colab_configured"] is True

    hook = compute.hooks["rvc_cover"]

    # 1) Succeeded hook registers song & notifies Owner
    mp3_v1 = _make_mp3_bytes(b"cover-v1", total_size=360)
    compute.drive_files["drv-v1"] = mp3_v1
    await hook(
        {
            "id": "job-ok-1",
            "job_type": "rvc_cover",
            "status": "succeeded",
            "payload": {"title": "告白气球"},
            "result": {
                "title": "告白气球",
                "artist": "周杰伦",
                "tags": "甜美",
                "digest": hashlib.sha256(mp3_v1).hexdigest(),
                "byte_size": len(mp3_v1),
                "duration_ms": 14000,
                "drive_chorus_id": "drv-v1",
                "drive_full_id": "drv-full-v1",
                "chorus_bytes": mp3_v1,
            },
        }
    )
    assert "告白气球" in mod.songs.titles
    old_id = (await mod.songs.list())[0]["id"]
    assert (mod.songs.root / f"{old_id}.mp3").is_file()
    assert proactive.alerts[-1][0] == "song_cover_done:job-ok-1"
    assert "告白气球" in proactive.alerts[-1][1]

    # 2) Replacement cover cleans old local cache file
    mp3_v2 = _make_mp3_bytes(b"cover-v2", total_size=380)
    compute.drive_files["drv-v2"] = mp3_v2
    await hook(
        {
            "id": "job-ok-2",
            "job_type": "rvc_cover",
            "status": "succeeded",
            "payload": {"title": "告白气球"},
            "result": {
                "title": "告白气球",
                "artist": "周杰伦",
                "tags": "新版",
                "digest": hashlib.sha256(mp3_v2).hexdigest(),
                "byte_size": len(mp3_v2),
                "duration_ms": 15000,
                "drive_chorus_id": "drv-v2",
                "drive_full_id": "drv-full-v2",
            },
        }
    )
    assert not (mod.songs.root / f"{old_id}.mp3").exists()
    assert (await mod.songs.list())[0]["cached_local"] is False

    # 3) source_not_found gives actionable inbox/audio guidance
    await hook(
        {
            "id": "job-miss-1",
            "job_type": "rvc_cover",
            "status": "failed",
            "error_code": "source_not_found",
            "payload": {"title": "冷门小曲"},
            "result": {"error_code": "source_not_found"},
        }
    )
    code_miss, msg_miss = proactive.alerts[-1]
    assert code_miss == "song_cover_failed:job-miss-1"
    assert "Hakimi_Songs/inbox/" in msg_miss
    assert "冷门小曲" in msg_miss

    # 4) Generic failure notifies & cancelled does not notify
    await hook(
        {
            "id": "job-err-1",
            "job_type": "rvc_cover",
            "status": "failed",
            "error_code": "colab_gpu_oom",
            "payload": {"title": "测试失败曲"},
            "result": {},
        }
    )
    assert proactive.alerts[-1][0] == "song_cover_failed:job-err-1"

    alert_count = len(proactive.alerts)
    await hook({"id": "job-cancel-1", "job_type": "rvc_cover", "status": "cancelled"})
    assert len(proactive.alerts) == alert_count


async def test_song_admin_routes_learn_evict_and_extended_listing(settings, fake_ffmpeg_bins):
    ffm, ffp = fake_ffmpeg_bins
    s = settings.model_copy(update={"admin_token": SecretStr(ADMIN_TOKEN)})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        lib = svc.songs
        lib.ffmpeg_bin = ffm
        lib.ffprobe_bin = ffp
        compute = _FakeColabCompute(is_configured=True)
        lib.compute = compute

        # Seed 1 local song + 1 Drive-backed song
        await lib.upload("本地测试歌", _make_sine_wav(2500), filename="local.wav")
        drive_raw = _make_mp3_bytes(b"admin-drive-song", total_size=450)
        compute.drive_files["drv-admin-1"] = drive_raw
        await lib.register_colab_cover(
            title="云端翻唱歌",
            artist="测试原唱",
            tags="轻快",
            digest=hashlib.sha256(drive_raw).hexdigest(),
            byte_size=len(drive_raw),
            duration_ms=11000,
            drive_chorus_id="drv-admin-1",
            drive_full_id="drv-admin-full-1",
            chorus_bytes=drive_raw,
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ) as client:
            # Unauthorized rejected
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={"title": "晴天"},
                    headers={"Authorization": ""},
                )
            ).status_code == 401

            # Query params rejected
            assert (await client.post("/admin/api/songs/cache/evict?force=1")).status_code == 400

            # Extended GET /admin/api/songs fields
            listing_resp = await client.get("/admin/api/songs")
            assert listing_resp.status_code == 200
            listing = listing_resp.json()
            for legacy_key in (
                "items",
                "max_upload_bytes",
                "max_duration_seconds",
                "max_songs",
                "total_bytes",
                "max_library_bytes",
                "loaded_prompt_count",
            ):
                assert legacy_key in listing
            assert listing["cache_bytes"] == 450
            assert listing["cached_count"] == 1
            assert listing["drive_backed_count"] == 1
            assert listing["colab_configured"] is True
            by_title = {item["title"]: item for item in listing["items"]}
            assert by_title["本地测试歌"]["cached_local"] is True
            assert by_title["本地测试歌"]["drive_backed"] is False
            assert by_title["本地测试歌"]["source_origin"] == "local"
            assert by_title["云端翻唱歌"]["cached_local"] is True
            assert by_title["云端翻唱歌"]["drive_backed"] is True
            assert by_title["云端翻唱歌"]["drive_full_backed"] is True
            assert by_title["云端翻唱歌"]["source_origin"] == "colab"
            assert "filename" not in by_title["云端翻唱歌"]

            # Strict Pydantic validation on POST /admin/api/songs/learn
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={"title": "晴天", "extra": 1},
                )
            ).status_code == 400
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={"title": "晴天", "pitch": "2"},
                )
            ).status_code == 400
            bad_src = await client.post(
                "/admin/api/songs/learn",
                json={"title": "晴天", "auto_search": False},
            )
            assert bad_src.status_code == 400
            assert bad_src.json()["detail"] == "song_source_required"

            # Valid learn request
            ok_learn = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "七里香",
                    "artist": "周杰伦",
                    "tags": "夏夜",
                    "pitch": -2,
                    "inbox_filename": "qilixiang.flac",
                    "auto_search": False,
                },
            )
            assert ok_learn.status_code == 200
            assert ok_learn.json()["job_type"] == "rvc_cover"
            assert ok_learn.json()["payload"]["inbox_filename"] == "qilixiang.flac"

            # Error mappings: 409 conflict & 503 unconfigured
            compute.submit_error = Rejected("colab_daily_job_limit")
            assert (
                await client.post("/admin/api/songs/learn", json={"title": "晴天"})
            ).status_code == 409
            compute.submit_error = None
            compute.is_configured = False
            assert (
                await client.post("/admin/api/songs/learn", json={"title": "晴天"})
            ).status_code == 503
            compute.is_configured = True

            # POST /admin/api/songs/cache/evict
            evict_resp = await client.post("/admin/api/songs/cache/evict")
            assert evict_resp.status_code == 200
            evict_data = evict_resp.json()
            expected_keys = {
                "evicted_count",
                "freed_bytes",
                "cache_bytes",
                "cache_max_bytes",
                "cached_count",
            }
            assert expected_keys <= set(evict_data)
            assert evict_data["evicted_count"] == 0  # 450 <= 30 MiB limit

            # Trigger real eviction via HTTP when cache exceeds configured limit
            lib.settings = SimpleNamespace(
                data_dir=lib.root.parent,
                song_cache_max_bytes=500,
            )
            drive_raw_2 = _make_mp3_bytes(b"admin-drive-song-2", total_size=400)
            compute.drive_files["drv-admin-2"] = drive_raw_2
            await lib.register_colab_cover(
                title="云端翻唱歌2",
                digest=hashlib.sha256(drive_raw_2).hexdigest(),
                byte_size=len(drive_raw_2),
                duration_ms=9000,
                drive_chorus_id="drv-admin-2",
                chorus_bytes=drive_raw_2,
            )
            # Registration evicted oldest; force cache cap below remaining 400 to test endpoint
            lib.settings = SimpleNamespace(
                data_dir=lib.root.parent,
                song_cache_max_bytes=200,
            )
            evict_resp_2 = await client.post("/admin/api/songs/cache/evict")
            assert evict_resp_2.status_code == 200
            evict_data_2 = evict_resp_2.json()
            assert evict_data_2["evicted_count"] == 1
            assert evict_data_2["freed_bytes"] == 400
            assert evict_data_2["cache_bytes"] == 0
            # Pure local song still playable via audio endpoint
            local_item = by_title["本地测试歌"]
            local_audio = await client.get(f"/admin/api/songs/{local_item['id']}/audio")
            assert local_audio.status_code == 200
