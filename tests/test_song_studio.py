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
from hakimi.songs import (
    MAX_LEARN_UPLOAD_BYTES,
    MAX_SONG_TITLE_LEN,
    MAX_STORED_SONG_BYTES,
    RVC_COVER_ALLOWED_GPUS,
    RVC_COVER_DEFAULT_GPU,
    RVC_COVER_FALLBACK_GPUS,
    RVC_COVER_TIMEOUT_SECONDS,
    RVC_TRAIN_ALLOWED_GPUS,
    RVC_TRAIN_DEFAULT_GPU,
    RVC_TRAIN_FALLBACK_GPUS,
    RVC_TRAIN_TIMEOUT_SECONDS,
    SongLibrary,
    _safe_learn_upload_name,
    is_valid_song_title,
    validate_song_title,
)
from hakimi.system_modules import SystemResources
from hakimi.system_modules.song_studio import Module as SongStudioModule
from tests.fakes import FakeGateway
from tests.test_sdk_contract import sdk_pair as sdk_pair

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
        self.submitted_gpus: list[str | None] = []
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
        self.submitted_gpus.append(gpu)
        job_id = uuid.uuid4().hex
        return {
            "id": job_id,
            "job_type": job_type,
            "status": "queued",
            "requester": requester,
            "preferred_gpu": gpu or RVC_COVER_DEFAULT_GPU,
            "payload": dict(payload),
            "created_at": time.time(),
        }

    def register_completion_hook(self, job_type: str, callback: Any) -> None:
        self.hooks[job_type] = callback

    async def close(self) -> None:
        return None


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


async def test_request_learn_song_dual_track_priority_and_validation(
    store, monkeypatch: pytest.MonkeyPatch
):
    assert MAX_LEARN_UPLOAD_BYTES == 100 * 1024 * 1024
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
    with monkeypatch.context() as mp:
        mp.setattr("hakimi.songs.MAX_LEARN_UPLOAD_BYTES", 64)
        with pytest.raises(Rejected, match="song_upload_too_large"):
            await lib.request_learn_song(
                "晴天",
                audio_bytes=b"x" * 65,
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

    # Track 3: Auto-search requires online song_finder (never leaks to Colab on offline/miss)
    prev_jobs = len(compute.submitted_jobs)
    with pytest.raises(Rejected, match="song_finder_remote_unavailable"):
        await lib.request_learn_song("夜曲", artist="周杰伦", auto_search=True)
    assert len(compute.submitted_jobs) == prev_jobs

    # When song_finder succeeds, source is uploaded to inbox before Colab submit (auto_search=False)
    from hakimi_plugin_sdk import ToolCallResult

    class _FakeFinderBus:
        def has_tool(
            self, plugin_id: str, action_name: str, *, require_enabled: bool = True
        ) -> bool:
            del require_enabled
            return plugin_id == "song_finder" and action_name == "song_finder.fetch_source"

        async def invoke_tool(self, *_args: Any, **_kwargs: Any) -> ToolCallResult:
            raw = b"FOUND_ORIGINAL_AUDIO"
            return ToolCallResult(
                data={
                    "provider": "youtube",
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "filename": "夜曲.m4a",
                },
                blob=raw,
                blob_mime_type="audio/mp4",
                blob_filename="夜曲.m4a",
            )

    lib.plugin_bus = _FakeFinderBus()
    await lib.request_learn_song("夜曲", artist="周杰伦", auto_search=True)
    _, p3, _ = compute.submitted_jobs[-1]
    assert p3["inbox_file_id"] == "inbox-id-2"
    assert p3["inbox_filename"] == "夜曲.m4a"
    assert p3["auto_search"] is False
    assert "query" not in p3


async def test_multilingual_punctuation_song_titles_and_safe_upload_names(
    settings, store, fake_ffmpeg_bins, tmp_path: Path
):
    assert MAX_SONG_TITLE_LEN == 60
    assert _safe_learn_upload_name("夜に駆ける.mp3", "track") == ("夜に駆ける.mp3", "audio/mpeg")

    multilingual_titles = (
        "夜に駆ける",
        "사랑을 했다 (Love Scenario)",
        "Été d'amour",
        "Алые паруса",
        "พรุ่งนี้",
    )
    punctuation_titles = (
        "Don't Stop Me Now!",
        "Wait...",
        "LOVE・DIVE",
        "君の名は。",
        "A & B [Remix]",
    )
    boundary_60 = "a" * 60
    for good_title in (*multilingual_titles, *punctuation_titles, boundary_60):
        assert is_valid_song_title(good_title), good_title
        assert validate_song_title(good_title) == good_title

    assert not is_valid_song_title("bad\ntitle")
    assert not is_valid_song_title(" leading")

    unsafe_titles = (
        "",
        "   ",
        "../bad",
        "..",
        ".",
        "a/b",
        "a\\b",
        "<script>",
        '"quoted"',
        "歌" * 61,
    )
    for bad_title in unsafe_titles:
        assert not is_valid_song_title(bad_title), repr(bad_title)
        with pytest.raises(Rejected, match="invalid_song_title"):
            validate_song_title(bad_title)

    ffm, ffp = fake_ffmpeg_bins
    compute = _FakeColabCompute(is_configured=True)
    lib = SongLibrary(store, ffmpeg_bin=ffm, ffprobe_bin=ffp, compute=compute)
    await lib.initialize()

    # Register & reload multilingual/punctuation songs through SQLite persistence
    for idx, title in enumerate((*multilingual_titles, *punctuation_titles)):
        mp3 = _make_mp3_bytes(f"i18n-song-{idx}".encode(), total_size=300)
        compute.drive_files[f"drv-i18n-{idx}"] = mp3
        entry = await lib.register_colab_cover(
            title=title,
            digest=hashlib.sha256(mp3).hexdigest(),
            byte_size=len(mp3),
            duration_ms=9000,
            drive_chorus_id=f"drv-i18n-{idx}",
        )
        assert entry["title"] == title
        assert lib.resolve_title(title.lower()) == title

    reloaded = SongLibrary(store, ffmpeg_bin=ffm, ffprobe_bin=ffp, compute=compute)
    await reloaded.initialize()
    assert set(reloaded.titles) == set(multilingual_titles) | set(punctuation_titles)

    # Learn requests preserve Unicode audio filenames and reject unsafe/61-char titles
    await lib.request_learn_song(
        "夜に駆ける (Cover)",
        audio_bytes=b"MP3_BYTES",
        audio_filename="夜に駆ける.mp3",
    )
    assert compute.inbox_uploads[-1][0] == "夜に駆ける.mp3"
    assert compute.inbox_uploads[-1][2] == "audio/mpeg"

    for bad_title in unsafe_titles:
        with pytest.raises(Rejected, match="invalid_song_title"):
            await lib.request_learn_song(bad_title, auto_search=True)

    # /学歌 chat command & Admin API respect 60-char limit and multilingual titles
    s = settings.model_copy(
        update={
            "data_dir": tmp_path / "app_data",
            "admin_token": SecretStr(ADMIN_TOKEN),
            "owner_qq_id": "20000",
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        from hakimi.models import Message
        from hakimi.service import _LearnSongArgs

        assert _LearnSongArgs(title="夜" * 60).title == "夜" * 60
        app.state.service.songs.compute = compute
        app.state.service.colab_compute = compute
        bad_cmd_reply = await app.state.service.command(
            Message(
                platform="onebot",
                bot_id="10000",
                event_id="ev-bad-title",
                user_id="20000",
                kind="private",
                conversation_id="20000",
                text="/学歌 <script>",
                addressed=True,
                created_at=time.time(),
            )
        )
        assert bad_cmd_reply is not None and "1～60 字" in bad_cmd_reply
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ) as client:
            ok_60 = await client.post(
                "/admin/api/songs/learn",
                json={"title": "夜" * 60, "inbox_filename": "y60.flac", "auto_search": False},
            )
            assert ok_60.status_code == 200
            bad_61 = await client.post(
                "/admin/api/songs/learn",
                json={"title": "夜" * 61, "inbox_filename": "y61.flac", "auto_search": False},
            )
            assert bad_61.status_code == 400
            assert bad_61.json()["detail"] == "invalid_song_title"


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
            assert listing["drive_configured"] is False
            assert listing["drive_songs_folder_id"] == ""
            assert listing["drive_config_source"] == "none"
            assert listing["drive_folders"] is None
            assert listing["drive_error"] is None
            assert listing["colab_configured"] is True
            assert listing["learn_gpu_policy"] == {
                "default_gpu": "L4",
                "fallback_gpus": ["T4"],
                "allowed_gpus": ["L4", "T4", "A100", "H100"],
                "timeout_seconds": 2400,
            }
            assert listing["default_engine"] == "dual"
            assert listing["available_engines"] == ["dual", "yingmusic_svc", "seed_vc", "rvc"]
            assert listing["rvc_defaults"]["volume_envelope"] == 0.25
            assert listing["rvc_defaults"]["skip_dereverb"] is False
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

            # Valid learn request (default GPU -> L4)
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
            assert ok_learn.json()["preferred_gpu"] == "L4"
            assert ok_learn.json()["payload"]["inbox_filename"] == "qilixiang.flac"
            assert compute.submitted_gpus[-1] is None

            # Optional per-request GPU override (normalized) & blank treated as default
            gpu_learn = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "花海",
                    "inbox_filename": "huahai.flac",
                    "auto_search": False,
                    "gpu": "A100",
                },
            )
            assert gpu_learn.status_code == 200
            assert gpu_learn.json()["preferred_gpu"] == "A100"
            assert compute.submitted_gpus[-1] == "A100"

            blank_gpu_learn = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "枫",
                    "inbox_filename": "feng.flac",
                    "auto_search": False,
                    "gpu": "   ",
                },
            )
            assert blank_gpu_learn.status_code == 200
            assert blank_gpu_learn.json()["preferred_gpu"] == "L4"
            assert compute.submitted_gpus[-1] is None

            # Auto-search when song_finder is offline fails fast with 503 and creates 0 Colab jobs
            prev_submits = len(compute.submitted_jobs)
            finder_off = await client.post(
                "/admin/api/songs/learn",
                json={"title": "反乌托邦Pt.2", "auto_search": True},
            )
            assert finder_off.status_code == 503
            assert finder_off.json()["detail"] == "song_finder_remote_unavailable"
            assert len(compute.submitted_jobs) == prev_submits

            # Disallowed GPU (e.g. G4 excluded for RVC sm_120) rejected with 400 before submit
            prev_submits = len(compute.submitted_jobs)
            bad_gpu = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "晴天",
                    "inbox_filename": "qt.flac",
                    "auto_search": False,
                    "gpu": "G4",
                },
            )
            assert bad_gpu.status_code == 400
            assert bad_gpu.json()["detail"] == "colab_gpu_not_allowed"
            assert len(compute.submitted_jobs) == prev_submits
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={
                        "title": "晴天",
                        "inbox_filename": "qt.flac",
                        "auto_search": False,
                        "gpu": 123,
                    },
                )
            ).status_code == 400
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={
                        "title": "晴天",
                        "inbox_filename": "qt.flac",
                        "auto_search": False,
                        "gpu": "A" * 17,
                    },
                )
            ).status_code == 400

            # Error mappings: 409 conflict & 503 unconfigured
            compute.submit_error = Rejected("colab_daily_job_limit")
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={"title": "晴天", "inbox_filename": "qt.flac", "auto_search": False},
                )
            ).status_code == 409
            compute.submit_error = None
            compute.is_configured = False
            assert (
                await client.post(
                    "/admin/api/songs/learn",
                    json={"title": "晴天", "inbox_filename": "qt.flac", "auto_search": False},
                )
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


async def test_song_library_uses_direct_drive_store_independently_of_colab(store):
    class _DirectDriveStub:
        def __init__(self) -> None:
            self.songs_map: dict[str, bytes] = {}
            self.inbox_uploads: list[tuple[str, bytes, str]] = []

        def configured(self) -> bool:
            return True

        async def download_song(self, file_id: str) -> bytes:
            return self.songs_map[file_id]

        async def upload_inbox_bytes(
            self, filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
        ) -> dict[str, Any]:
            self.inbox_uploads.append((filename, raw, mime_type))
            return {"file_id": "direct-inbox-1", "name": filename, "size": len(raw)}

    class _PureGpuComputeStub:
        def __init__(self) -> None:
            self.submitted: list[tuple[str, dict[str, Any], str]] = []

        def configured(self) -> bool:
            return True

        async def submit_job(
            self, job_type: str, payload: dict[str, Any], *, requester: str = "owner"
        ) -> dict[str, Any]:
            self.submitted.append((job_type, dict(payload), requester))
            return {"id": "gpu-job-1", "job_type": job_type, "status": "queued", "payload": payload}

    drive = _DirectDriveStub()
    mp3 = _make_mp3_bytes(b"direct-drive-no-colab", total_size=320)
    drive.songs_map["drv-direct-1"] = mp3

    # Playback works with compute=None!
    lib = SongLibrary(store, compute=None, drive=drive)
    await lib.initialize()
    assert lib.cache_stats()["drive_configured"] is True
    assert lib.cache_stats()["colab_configured"] is False

    entry = await lib.register_colab_cover(
        title="独立云盘曲",
        digest=hashlib.sha256(mp3).hexdigest(),
        byte_size=len(mp3),
        duration_ms=7000,
        drive_chorus_id="drv-direct-1",
    )
    raw, mime = await lib.audio_bytes(entry["id"])
    assert raw == mp3 and mime == "audio/mpeg"

    # Learn song uploads directly to drive and submits job to pure-GPU compute (no Drive proxy)
    gpu_compute = _PureGpuComputeStub()
    lib.compute = gpu_compute
    job = await lib.request_learn_song(
        "新曲",
        audio_bytes=b"AUDIO_RAW",
        audio_filename="新曲.flac",
        requester="owner",
    )
    assert job["id"] == "gpu-job-1"
    assert drive.inbox_uploads == [("新曲.flac", b"AUDIO_RAW", "audio/flac")]
    assert gpu_compute.submitted[-1][1]["inbox_file_id"] == "direct-inbox-1"


async def test_song_studio_gpu_policy_registration_early_reject_and_command_error(settings, store):
    # 1) Single-source module constants
    assert RVC_COVER_DEFAULT_GPU == "L4"
    assert RVC_COVER_FALLBACK_GPUS == ("T4",)
    assert RVC_COVER_ALLOWED_GPUS == ("L4", "T4", "A100", "H100")
    assert RVC_COVER_TIMEOUT_SECONDS == 2400
    assert "G4" not in RVC_COVER_ALLOWED_GPUS
    assert RVC_TRAIN_DEFAULT_GPU == "H100"
    assert RVC_TRAIN_FALLBACK_GPUS == ("A100",)
    assert RVC_TRAIN_ALLOWED_GPUS == ("H100", "A100")
    assert RVC_TRAIN_TIMEOUT_SECONDS == 3600
    assert all(gpu not in RVC_TRAIN_ALLOWED_GPUS for gpu in ("T4", "L4", "G4"))

    # 2) Policy-aware compute stub receives full GPU policies from SongStudioModule.initialize()
    class _PolicyComputeStub:
        def __init__(self) -> None:
            self.registered: dict[str, dict[str, Any]] = {}
            self.submitted: list[tuple[str, dict[str, Any], str, str | None]] = []

        def configured(self) -> bool:
            return True

        def register_job_type(
            self,
            job_type: str,
            *,
            validator: Any,
            pre_dispatch: Any = None,
            on_complete: Any = None,
            owner_module: str = "",
            default_gpu: str = "L4",
            fallback_gpus: tuple[str, ...] = (),
            allowed_gpus: tuple[str, ...] | None = None,
            timeout_seconds: int | None = None,
        ) -> None:
            self.registered[job_type] = {
                "validator": validator,
                "pre_dispatch": pre_dispatch,
                "on_complete": on_complete,
                "owner_module": owner_module,
                "default_gpu": default_gpu,
                "fallback_gpus": tuple(fallback_gpus),
                "allowed_gpus": tuple(allowed_gpus or ()),
                "timeout_seconds": timeout_seconds,
            }

        def get_job_type_spec(self, job_type: str) -> dict[str, Any] | None:
            raw = self.registered.get(job_type)
            if raw is None:
                return None
            return {
                "job_type": job_type,
                "owner_module": raw["owner_module"],
                "default_gpu": raw["default_gpu"],
                "fallback_gpus": list(raw["fallback_gpus"]),
                "allowed_gpus": list(raw["allowed_gpus"]),
                "timeout_seconds": raw["timeout_seconds"],
            }

        async def submit_job(
            self,
            job_type: str,
            payload: dict[str, Any],
            *,
            requester: str = "owner",
            gpu: str | None = None,
        ) -> dict[str, Any]:
            spec = self.registered[job_type]
            chosen = gpu or spec["default_gpu"]
            if chosen not in spec["allowed_gpus"]:
                raise Rejected("colab_gpu_not_allowed")
            self.submitted.append((job_type, dict(payload), requester, gpu))
            return {
                "id": uuid.uuid4().hex,
                "job_type": job_type,
                "status": "queued",
                "preferred_gpu": chosen,
                "payload": dict(payload),
            }

    class _TrackingDriveStub:
        def __init__(self) -> None:
            self.uploads: list[tuple[str, bytes, str]] = []

        def configured(self) -> bool:
            return True

        async def upload_inbox_bytes(
            self, filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
        ) -> dict[str, Any]:
            self.uploads.append((filename, raw, mime_type))
            return {"file_id": "drv-inbox-1", "name": filename, "size": len(raw)}

    class _TrackingPluginBus:
        def __init__(self) -> None:
            self.calls = 0

        def has_tool(self, plugin_id: str, action: str, *, require_enabled: bool = True) -> bool:
            return plugin_id == "song_finder" and action == "song_finder.fetch_source"

        async def invoke_tool(self, *args: Any, **kwargs: Any) -> Any:
            self.calls += 1
            raise AssertionError("song_finder must not be invoked when GPU is rejected")

    compute = _PolicyComputeStub()

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
        service=SimpleNamespace(),  # type: ignore[arg-type]
    )
    mod = SongStudioModule(resources, registry)
    await mod.initialize()

    cover_spec = compute.registered["rvc_cover"]
    assert cover_spec["owner_module"] == "song_studio"
    assert cover_spec["default_gpu"] == "L4"
    assert cover_spec["fallback_gpus"] == ("T4",)
    assert cover_spec["allowed_gpus"] == ("L4", "T4", "A100", "H100")
    assert cover_spec["timeout_seconds"] == 2400

    train_spec = compute.registered["rvc_train"]
    assert train_spec["owner_module"] == "song_studio"
    assert train_spec["default_gpu"] == "H100"
    assert train_spec["fallback_gpus"] == ("A100",)
    assert train_spec["allowed_gpus"] == ("H100", "A100")
    assert train_spec["timeout_seconds"] == 3600

    # Verify rvc_train rejects T4 / L4 / G4 under the registered spec
    for disallowed_train_gpu in ("T4", "L4", "G4"):
        with pytest.raises(Rejected, match="colab_gpu_not_allowed"):
            await compute.submit_job(
                "rvc_train", {"model_name": "hakimi"}, gpu=disallowed_train_gpu
            )

    # 3) learn_gpu_policy reads from compute spec and falls back cleanly when compute=None
    assert mod.songs.learn_gpu_policy() == {
        "default_gpu": "L4",
        "fallback_gpus": ["T4"],
        "allowed_gpus": ["L4", "T4", "A100", "H100"],
        "timeout_seconds": 2400,
    }
    fallback_lib = SongLibrary(store, compute=None)
    assert fallback_lib.learn_gpu_policy() == {
        "default_gpu": "L4",
        "fallback_gpus": ["T4"],
        "allowed_gpus": ["L4", "T4", "A100", "H100"],
        "timeout_seconds": 2400,
    }

    # 4) Early GPU validation blocks Drive upload and remote song_finder before any I/O
    drive_stub = _TrackingDriveStub()
    bus_stub = _TrackingPluginBus()
    mod.songs.drive = drive_stub
    mod.songs.plugin_bus = bus_stub

    for bad_gpu in ("G4", "g4", "  g4 ", "RTX4090", 42):
        with pytest.raises(Rejected, match="colab_gpu_not_allowed"):
            await mod.songs.request_learn_song(
                "晴天",
                audio_bytes=b"RAW_AUDIO_SHOULD_NOT_UPLOAD",
                audio_filename="晴天.mp3",
                gpu=bad_gpu,  # type: ignore[arg-type]
            )
    with pytest.raises(Rejected, match="colab_gpu_not_allowed"):
        await mod.songs.request_learn_song("晴天", auto_search=True, gpu="G4")
    assert drive_stub.uploads == []
    assert bus_stub.calls == 0
    assert compute.submitted == []

    # Allowed GPUs normalize case/whitespace and pass through cleanly
    for raw_gpu, expected_gpu in (
        (None, None),
        ("", None),
        ("   ", None),
        ("l4", "L4"),
        (" t4 ", "T4"),
        ("a100", "A100"),
        ("H100", "H100"),
    ):
        job = await mod.songs.request_learn_song(
            "晴天",
            inbox_filename="sun.mp3",
            auto_search=False,
            gpu=raw_gpu,
        )
        assert compute.submitted[-1][3] == expected_gpu
        assert job["preferred_gpu"] == (expected_gpu or "L4")

    # 5) /学歌 command maps colab_gpu_not_allowed to a friendly message
    from hakimi.commands import handle_learn_song_command
    from hakimi.models import Message

    class _RejectGpuSongs:
        async def request_learn_song(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            raise Rejected("colab_gpu_not_allowed")

    fake_svc = SimpleNamespace(
        s=settings.model_copy(update={"owner_qq_id": "20000"}),
        songs=_RejectGpuSongs(),
    )
    msg = Message(
        platform="onebot",
        bot_id="10000",
        event_id="ev-gpu-err",
        user_id="20000",
        kind="private",
        conversation_id="20000",
        text="/学歌 晴天",
        addressed=True,
        created_at=time.time(),
    )
    reply = await handle_learn_song_command(fake_svc, msg, "晴天")
    assert reply == "当前任务不支持所选 GPU 型号，请更换可用显卡喵。"


async def test_song_studio_real_colab_hub_registration_and_admin_learn_gpu_e2e(
    settings, tmp_path: Path
):
    s = settings.model_copy(
        update={
            "data_dir": tmp_path / "e2e_song_gpu",
            "admin_token": SecretStr(ADMIN_TOKEN),
            "colab_worker_url": "http://127.0.0.1:12146",
            "colab_worker_token": SecretStr("test-colab-worker-token-never-live"),
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        hub = app.state.service.colab_compute
        assert hub.get_job_type_spec("rvc_cover") == {
            "job_type": "rvc_cover",
            "owner_module": "song_studio",
            "default_gpu": "L4",
            "fallback_gpus": ["T4"],
            "allowed_gpus": ["L4", "T4", "A100", "H100"],
            "timeout_seconds": 2400,
        }
        assert hub.get_job_type_spec("rvc_train") == {
            "job_type": "rvc_train",
            "owner_module": "song_studio",
            "default_gpu": "H100",
            "fallback_gpus": ["A100"],
            "allowed_gpus": ["H100", "A100"],
            "timeout_seconds": 3600,
        }

        train_job = await hub.submit_job("rvc_train", {"model_name": "hakimi"})
        assert train_job["preferred_gpu"] == "H100"
        assert train_job["fallback_gpus"] == ["A100"]
        for bad_train_gpu in ("T4", "L4", "G4"):
            with pytest.raises(Rejected, match="colab_gpu_not_allowed"):
                await hub.submit_job("rvc_train", {"model_name": "hakimi"}, gpu=bad_train_gpu)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ) as client:
            songs_resp = await client.get("/admin/api/songs")
            assert songs_resp.status_code == 200
            assert songs_resp.json()["learn_gpu_policy"] == {
                "default_gpu": "L4",
                "fallback_gpus": ["T4"],
                "allowed_gpus": ["L4", "T4", "A100", "H100"],
                "timeout_seconds": 2400,
            }

            default_learn = await client.post(
                "/admin/api/songs/learn",
                json={"title": "晴天", "inbox_filename": "qingtian.flac", "auto_search": False},
            )
            assert default_learn.status_code == 200
            assert default_learn.json()["preferred_gpu"] == "L4"
            assert default_learn.json()["fallback_gpus"] == ["T4"]

            override_learn = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "夜曲",
                    "inbox_filename": "yequ.flac",
                    "auto_search": False,
                    "gpu": "A100",
                },
            )
            assert override_learn.status_code == 200
            assert override_learn.json()["preferred_gpu"] == "A100"
            assert override_learn.json()["fallback_gpus"] == ["L4", "T4"]

            rejected_learn = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "稻香",
                    "inbox_filename": "daoxiang.flac",
                    "auto_search": False,
                    "gpu": "G4",
                },
            )
            assert rejected_learn.status_code == 400
            assert rejected_learn.json()["detail"] == "colab_gpu_not_allowed"


async def test_song_studio_main_model_blind_ab_audition_real_gateway_and_anti_bias(
    settings, store, sdk_pair
):
    """Verify dual-engine audition uses main model (not decision-1), blind A/B, & proactive lane."""
    import json

    from hakimi.gemini import GeminiGateway
    from hakimi.security import Rejected
    from hakimi.system_modules.song_studio import (
        SONG_AUDITION_RESPONSE_SCHEMA,
        SONG_AUDITION_SCENE_KEY,
        _parse_audition_verdict,
    )
    from tests.test_sdk_contract import decision_response, interaction_response

    def _schema_keys(node: Any) -> set[str]:
        keys: set[str] = set()
        if isinstance(node, dict):
            keys.update(node.keys())
            for value in node.values():
                keys.update(_schema_keys(value))
        elif isinstance(node, list):
            for item in node:
                keys.update(_schema_keys(item))
        return keys

    # Verify serving-safe strict JSON Schema: all core fields in `required`, zero length/regex caps
    assert set(SONG_AUDITION_RESPONSE_SCHEMA["required"]) == {
        "winner",
        "pick_reason",
        "review_note",
        "a_impression",
        "b_impression",
    }
    assert SONG_AUDITION_RESPONSE_SCHEMA.get("additionalProperties") is False
    forbidden_schema_keys = {
        "pattern",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "minItems",
        "maxItems",
    }
    assert not (_schema_keys(SONG_AUDITION_RESPONSE_SCHEMA) & forbidden_schema_keys)

    # Verify Python-side post-parse cleaning & truncation + blank-field rejection
    parsed_long = _parse_audition_verdict(
        json.dumps(
            {
                "winner": "A",
                "pick_reason": "音准超稳" * 50,
                "review_note": "听起来超自然\x00\n" + "开心" * 150,
                "a_impression": "甲" * 120,
                "b_impression": "乙" * 130,
            },
            ensure_ascii=False,
        )
    )
    assert parsed_long.winner == "A"
    assert len(parsed_long.pick_reason) == 160
    assert len(parsed_long.review_note) == 240
    assert "\x00" not in parsed_long.review_note
    assert len(parsed_long.a_impression) == 100
    assert len(parsed_long.b_impression) == 100
    for blank_patch in ({"pick_reason": "   \x00\n "}, {"review_note": "\t  "}):
        bad_payload = {
            "winner": "A",
            "pick_reason": "有效理由",
            "review_note": "有效点评",
            "a_impression": "A 印象",
            "b_impression": "B 印象",
            **blank_patch,
        }
        with pytest.raises(Rejected, match="invalid_audition_output"):
            _parse_audition_verdict(json.dumps(bad_payload, ensure_ascii=False))

    seen_decisions: list[dict[str, Any]] = []
    seen_interactions: list[dict[str, Any]] = []
    verdict_to_return: dict[str, Any] = {
        "winner": "B",
        "pick_reason": "候选B气息更自然，弱副歌衔接平稳没有电音感。",
        "review_note": "这首高潮唱起来超顺滑，轻柔段落也不会掉下去，我很喜欢这版喵~",
        "a_impression": "音色贴脸但尾音略有一点紧。",
        "b_impression": "自然透亮、咬字清楚且和伴奏贴得很稳。",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        if request.url.path.endswith("decisions"):
            seen_decisions.append(body)
            return httpx.Response(200, json=decision_response())
        seen_interactions.append(body)
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = json.dumps(verdict_to_return, ensure_ascii=False)
        return httpx.Response(200, json=resp)

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

    gw = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        resources = SystemResources(
            settings=settings,
            store=store,
            gateway=gw,
            http=None,
            service=SimpleNamespace(proactive=proactive),  # type: ignore[arg-type]
        )
        mod = SongStudioModule(resources, registry)
        # Register thinking scene onto settings just as Service does at boot
        settings._thinking_scenes = {scene.key: scene for scene in mod.thinking_scenes}
        assert SONG_AUDITION_SCENE_KEY in settings._thinking_scenes
        await mod.initialize()

        mp3_ying = _make_mp3_bytes(b"yingmusic-svc-shidarezakura", total_size=720)
        mp3_seed = _make_mp3_bytes(b"seed-vc-shidarezakura", total_size=680)
        compute.drive_files["drv-ying-c"] = mp3_ying
        compute.drive_files["drv-seed-c"] = mp3_seed

        # Deterministic no-op shuffle -> A=yingmusic_svc, B=seed_vc; verdict picks B -> seed_vc!
        class _FixedNoSwapRng:
            def shuffle(self, seq: list[Any]) -> None:
                return None

        mod._audition_rng = _FixedNoSwapRng()  # type: ignore[assignment]
        hook = compute.hooks["rvc_cover"]
        await hook(
            {
                "id": "job-dual-1",
                "job_type": "rvc_cover",
                "status": "succeeded",
                "payload": {"title": "枝垂桜", "engine": "dual"},
                "result": {
                    "title": "枝垂桜",
                    "artist": "Anna",
                    "tags": "日系",
                    "engine_requested": "dual",
                    "engine_used": "yingmusic_svc",
                    "digest": hashlib.sha256(mp3_ying).hexdigest(),
                    "byte_size": len(mp3_ying),
                    "duration_ms": 48000,
                    "drive_chorus_id": "drv-ying-c",
                    "drive_full_id": "drv-ying-f",
                    "candidates": [
                        {
                            "key": "yingmusic_svc",
                            "engine": "yingmusic_svc",
                            "status": "succeeded",
                            "digest": hashlib.sha256(mp3_ying).hexdigest(),
                            "byte_size": len(mp3_ying),
                            "duration_ms": 48000,
                            "drive_chorus_id": "drv-ying-c",
                            "drive_full_id": "drv-ying-f",
                            "acoustic_metrics": {
                                "chorus_mean_db": -12.1,
                                "chorus_dynamic_range_db": 7.2,
                                "clipped_peak_flag": False,
                            },
                        },
                        {
                            "key": "seed_vc",
                            "engine": "seed_vc",
                            "status": "succeeded",
                            "digest": hashlib.sha256(mp3_seed).hexdigest(),
                            "byte_size": len(mp3_seed),
                            "duration_ms": 48000,
                            "drive_chorus_id": "drv-seed-c",
                            "drive_full_id": "drv-seed-f",
                            "acoustic_metrics": {
                                "chorus_mean_db": -11.4,
                                "chorus_dynamic_range_db": 5.3,
                                "clipped_peak_flag": False,
                            },
                        },
                    ],
                },
            }
        )

        # 1) Main model: store=False, strict schema, interleaved A/B labels, blind prompt
        assert len(seen_interactions) == 1
        req = seen_interactions[0]
        assert req["model"].startswith("gemini-4-")
        assert req["store"] is False
        assert req["response_format"]["mime_type"] == "application/json"
        assert set(req["response_format"]["schema"]["required"]) == set(
            SONG_AUDITION_RESPONSE_SCHEMA["required"]
        )
        raw_input = req["input"]
        assert isinstance(raw_input, list) and raw_input
        blocks = (
            raw_input[0]["content"]
            if isinstance(raw_input[0], dict) and "content" in raw_input[0]
            else raw_input
        )
        text_blobs = [b["text"] for b in blocks if b.get("type") == "text"]
        audio_blobs = [b for b in blocks if b.get("type") == "audio"]
        assert len(audio_blobs) == 2
        assert [b["mime_type"] for b in audio_blobs] == ["audio/mp3", "audio/mp3"]
        assert "【候选A】" in text_blobs and "【候选B】" in text_blobs
        assert blocks[blocks.index({"type": "text", "text": "【候选A】"}) + 1]["type"] == "audio"
        assert blocks[blocks.index({"type": "text", "text": "【候选B】"}) + 1]["type"] == "audio"
        dumped_text = "\n".join(text_blobs) + "\n" + str(req.get("system_instruction", ""))
        for leaked_engine in ("yingmusic_svc", "seed_vc"):
            assert leaked_engine not in dumped_text

        # 2) gemini-decision-1 was NEVER called with audio to pick a candidate (text screen only)
        assert len(seen_decisions) == 1
        assert not isinstance(seen_decisions[0]["input"], list)

        # 3) Proactive budget lane accounted the audition call
        usage = await store.usage()
        proactive_lane = next((x for x in usage["lanes"] if x["lane"] == "proactive"), None)
        assert proactive_lane is not None and proactive_lane["amount"] > 0

        # 4) Winner is seed_vc (slot B), both takes archived, review_note saved & notified
        listed = await mod.songs.list()
        assert len(listed) == 1
        song = listed[0]
        assert song["title"] == "枝垂桜"
        assert song["active_engine"] == "seed_vc"
        assert song["active_take"] == "seed_vc"
        assert song["audition_status"] == "picked"
        assert song["audition_selected_engine"] == "seed_vc"
        assert "超顺滑" in song["review_note"]
        assert song["cached_local"] is True
        assert len(song["takes"]) == 2
        takes_by_key = {t["key"]: t for t in song["takes"]}
        assert takes_by_key["seed_vc"]["active"] is True
        assert takes_by_key["yingmusic_svc"]["active"] is False
        assert takes_by_key["yingmusic_svc"]["playable"] is True
        assert "Seed-VC" in proactive.alerts[-1][1]
        assert "超顺滑" in proactive.alerts[-1][1]

        # 5) Reverse A/B shuffle + overlong verdict text -> sanitized & truncated without fallback
        class _ReverseRng:
            def shuffle(self, seq: list[Any]) -> None:
                seq.reverse()

        verdict_to_return = {
            "winner": "B",
            "pick_reason": "副歌衔接非常自然且真唱共鸣很足" * 14,
            "review_note": "这版高潮听着超顺滑超自然喵~" + "太好听了" * 70,
            "a_impression": "甲" * 120,
            "b_impression": "乙" * 125,
        }
        mod._audition_rng = _ReverseRng()  # type: ignore[assignment]
        await hook(
            {
                "id": "job-dual-2",
                "job_type": "rvc_cover",
                "status": "succeeded",
                "payload": {"title": "枝垂桜", "engine": "dual"},
                "result": {
                    "title": "枝垂桜",
                    "artist": "Anna",
                    "tags": "日系",
                    "engine_requested": "dual",
                    "candidates": [
                        {
                            "key": "yingmusic_svc",
                            "engine": "yingmusic_svc",
                            "status": "succeeded",
                            "digest": hashlib.sha256(mp3_ying).hexdigest(),
                            "byte_size": len(mp3_ying),
                            "duration_ms": 48000,
                            "drive_chorus_id": "drv-ying-c",
                            "drive_full_id": "drv-ying-f",
                        },
                        {
                            "key": "seed_vc",
                            "engine": "seed_vc",
                            "status": "succeeded",
                            "digest": hashlib.sha256(mp3_seed).hexdigest(),
                            "byte_size": len(mp3_seed),
                            "duration_ms": 48000,
                            "drive_chorus_id": "drv-seed-c",
                            "drive_full_id": "drv-seed-f",
                        },
                    ],
                },
            }
        )
        song_rev = (await mod.songs.list())[0]
        assert song_rev["active_engine"] == "yingmusic_svc"
        assert song_rev["active_take"] == "yingmusic_svc"
        assert song_rev["audition_status"] == "picked"
        assert len(song_rev["review_note"]) == 240
        assert "超顺滑" in song_rev["review_note"]
        stored_entry = mod.songs._entries[song_rev["id"]]
        assert len(stored_entry["audition_meta"]["pick_reason"]) == 160

        # 6) SQLite reload preserves takes, review_note, audition_status, and active_engine
        reloaded = SongLibrary(store, compute=compute)
        await reloaded.initialize()
        reloaded_song = (await reloaded.list())[0]
        assert reloaded_song["active_engine"] == "yingmusic_svc"
        assert reloaded_song["active_take"] == "yingmusic_svc"
        assert reloaded_song["audition_status"] == "picked"
        assert len(reloaded_song["review_note"]) == 240
        assert "超顺滑" in reloaded_song["review_note"]
        assert len(reloaded_song["takes"]) == 2
    finally:
        await gw.close()


async def test_song_studio_audition_graceful_degradation_and_failure_notices(
    settings, store, monkeypatch: pytest.MonkeyPatch
):
    """Single-candidate, gateway errors, oversized inline caps, and SVC failure notifications."""
    compute = _FakeColabCompute(is_configured=True)
    proactive = _FakeProactive()

    class _FailingGateway:
        def __init__(self) -> None:
            self.calls = 0

        async def generate(self, *args: Any, **kwargs: Any) -> str:
            self.calls += 1
            raise Rejected("budget_hard_stop")

    gw = _FailingGateway()

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
        gateway=gw,  # type: ignore[arg-type]
        http=None,
        service=SimpleNamespace(proactive=proactive),  # type: ignore[arg-type]
    )
    mod = SongStudioModule(resources, registry)
    await mod.initialize()
    hook = compute.hooks["rvc_cover"]

    mp3_a = _make_mp3_bytes(b"single-surviving-seed", total_size=500)
    compute.drive_files["drv-single-seed"] = mp3_a

    # 1) Dual job where only seed_vc succeeded -> 0 LLM calls, fallback_single
    await hook(
        {
            "id": "job-single-1",
            "job_type": "rvc_cover",
            "status": "succeeded",
            "payload": {"title": "半边成功曲", "engine": "dual"},
            "result": {
                "title": "半边成功曲",
                "engine_requested": "dual",
                "candidates": [
                    {
                        "key": "yingmusic_svc",
                        "engine": "yingmusic_svc",
                        "status": "failed",
                        "error_code": "svc_oom",
                    },
                    {
                        "key": "seed_vc",
                        "engine": "seed_vc",
                        "status": "succeeded",
                        "digest": hashlib.sha256(mp3_a).hexdigest(),
                        "byte_size": len(mp3_a),
                        "duration_ms": 30000,
                        "drive_chorus_id": "drv-single-seed",
                    },
                ],
            },
        }
    )
    assert gw.calls == 0
    s1 = await mod.songs.get_entry_by_title("半边成功曲")
    assert s1 is not None
    assert s1["active_engine"] == "seed_vc"
    assert s1["audition_status"] == "fallback_single"
    assert len(s1["takes"]) == 2
    assert "自动降级" in proactive.alerts[-1][1]

    # 2) Both succeeded but gateway raises budget_hard_stop -> deterministic fallback, both kept
    mp3_y = _make_mp3_bytes(b"fallback-ying-track", total_size=520)
    mp3_s = _make_mp3_bytes(b"fallback-seed-track", total_size=510)
    compute.drive_files["drv-fb-y"] = mp3_y
    compute.drive_files["drv-fb-s"] = mp3_s
    await hook(
        {
            "id": "job-fb-err",
            "job_type": "rvc_cover",
            "status": "succeeded",
            "payload": {"title": "降级保底曲", "engine": "dual"},
            "result": {
                "title": "降级保底曲",
                "engine_requested": "dual",
                "candidates": [
                    {
                        "key": "seed_vc",
                        "engine": "seed_vc",
                        "status": "succeeded",
                        "digest": hashlib.sha256(mp3_s).hexdigest(),
                        "byte_size": len(mp3_s),
                        "duration_ms": 32000,
                        "drive_chorus_id": "drv-fb-s",
                    },
                    {
                        "key": "yingmusic_svc",
                        "engine": "yingmusic_svc",
                        "status": "succeeded",
                        "digest": hashlib.sha256(mp3_y).hexdigest(),
                        "byte_size": len(mp3_y),
                        "duration_ms": 32000,
                        "drive_chorus_id": "drv-fb-y",
                    },
                ],
            },
        }
    )
    assert gw.calls == 1
    s2 = await mod.songs.get_entry_by_title("降级保底曲")
    assert s2 is not None
    assert s2["active_engine"] == "yingmusic_svc"
    assert s2["audition_status"] == "fallback_error"
    assert len(s2["takes"]) == 2
    assert all(t["playable"] for t in s2["takes"])

    # 3) Inline audio byte cap enforced -> degrades cleanly without losing takes
    mp3_y2 = _make_mp3_bytes(b"oversize-ying", total_size=530)
    mp3_s2 = _make_mp3_bytes(b"oversize-seed", total_size=540)
    compute.drive_files["drv-os-y"] = mp3_y2
    compute.drive_files["drv-os-s"] = mp3_s2
    monkeypatch.setattr("hakimi.audio_media.MAX_INLINE_AUDIO_BYTES", 200)
    await hook(
        {
            "id": "job-os-1",
            "job_type": "rvc_cover",
            "status": "succeeded",
            "payload": {"title": "超限降级曲", "engine": "dual"},
            "result": {
                "title": "超限降级曲",
                "engine_requested": "dual",
                "candidates": [
                    {
                        "key": "yingmusic_svc",
                        "engine": "yingmusic_svc",
                        "status": "succeeded",
                        "digest": hashlib.sha256(mp3_y2).hexdigest(),
                        "byte_size": len(mp3_y2),
                        "duration_ms": 20000,
                        "drive_chorus_id": "drv-os-y",
                    },
                    {
                        "key": "seed_vc",
                        "engine": "seed_vc",
                        "status": "succeeded",
                        "digest": hashlib.sha256(mp3_s2).hexdigest(),
                        "byte_size": len(mp3_s2),
                        "duration_ms": 20000,
                        "drive_chorus_id": "drv-os-s",
                    },
                ],
            },
        }
    )
    assert gw.calls == 1  # Exceeded inline cap -> gateway never invoked
    s3 = await mod.songs.get_entry_by_title("超限降级曲")
    assert s3 is not None and s3["audition_status"] == "fallback_error" and len(s3["takes"]) == 2

    # 4) New failure error code notifications
    await hook(
        {
            "id": "job-noref",
            "job_type": "rvc_cover",
            "status": "failed",
            "error_code": "ref_voice_not_found",
            "payload": {"title": "缺干声曲"},
            "result": {},
        }
    )
    assert "参考干声" in proactive.alerts[-1][1]
    await hook(
        {
            "id": "job-allfail",
            "job_type": "rvc_cover",
            "status": "failed",
            "error_code": "svc_all_engines_failed",
            "payload": {"title": "双挂曲"},
            "result": {},
        }
    )
    assert "双引擎 SVC" in proactive.alerts[-1][1]


async def test_song_multi_take_admin_api_commands_and_owner_chat_actions(
    settings, fake_ffmpeg_bins, tmp_path: Path
):
    """Test take preview/activate APIs, /学歌 engine options, /换歌版本, and owner chat actions."""
    from hakimi.models import Message
    from hakimi.outbound_actions import ChatActionTrigger

    ffm, ffp = fake_ffmpeg_bins
    s = settings.model_copy(
        update={
            "data_dir": tmp_path / "multitake_e2e",
            "admin_token": SecretStr(ADMIN_TOKEN),
            "owner_qq_id": "20000",
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        lib = svc.songs
        lib.ffmpeg_bin = ffm
        lib.ffprobe_bin = ffp
        compute = _FakeColabCompute(is_configured=True)
        lib.compute = compute
        svc.colab_compute = compute

        # No multi-take songs yet -> song_studio.switch_version is NOT exposed in owner chat actions
        initial_actions = [a["action_name"] for a in svc._collect_owner_chat_actions()]
        assert "song_studio.learn_song" in initial_actions
        assert "song_studio.switch_version" not in initial_actions

        mp3_ying = _make_mp3_bytes(b"take-ying-audio-data", total_size=420)
        mp3_seed = _make_mp3_bytes(b"take-seed-audio-data", total_size=430)
        compute.drive_files["drv-t-ying"] = mp3_ying
        compute.drive_files["drv-t-seed"] = mp3_seed

        entry = await lib.register_colab_cover(
            title="枝垂桜",
            artist="Anna",
            tags="日系",
            digest=hashlib.sha256(mp3_ying).hexdigest(),
            byte_size=len(mp3_ying),
            duration_ms=45000,
            drive_chorus_id="drv-t-ying",
            drive_full_id="drv-t-ying-full",
            chorus_bytes=mp3_ying,
            active_engine="yingmusic_svc",
            active_take="yingmusic_svc",
            audition_status="picked",
            review_note="高潮共鸣很饱满喵~",
            audition_meta={"selected_engine": "yingmusic_svc"},
            takes=[
                {
                    "key": "yingmusic_svc",
                    "engine": "yingmusic_svc",
                    "status": "succeeded",
                    "digest": hashlib.sha256(mp3_ying).hexdigest(),
                    "byte_size": len(mp3_ying),
                    "duration_ms": 45000,
                    "drive_chorus_id": "drv-t-ying",
                    "drive_full_id": "drv-t-ying-full",
                },
                {
                    "key": "seed_vc",
                    "engine": "seed_vc",
                    "status": "succeeded",
                    "digest": hashlib.sha256(mp3_seed).hexdigest(),
                    "byte_size": len(mp3_seed),
                    "duration_ms": 45000,
                    "drive_chorus_id": "drv-t-seed",
                    "drive_full_id": "drv-t-seed-full",
                },
                {
                    "key": "rvc",
                    "engine": "rvc",
                    "status": "failed",
                    "error_code": "model_not_found",
                },
            ],
        )
        song_id = entry["id"]

        # Multi-take song exists -> song_studio.switch_version IS now exposed in owner chat actions
        with_switch = [a["action_name"] for a in svc._collect_owner_chat_actions()]
        assert "song_studio.switch_version" in with_switch

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ) as client:
            # Preview active & non-active takes via path params (no query params)
            r_ying = await client.get(f"/admin/api/songs/{song_id}/takes/yingmusic_svc/audio")
            assert r_ying.status_code == 200 and r_ying.content == mp3_ying
            r_seed = await client.get(f"/admin/api/songs/{song_id}/takes/seed_vc/audio")
            assert r_seed.status_code == 200 and r_seed.content == mp3_seed

            # Failed / missing / invalid take error codes
            assert (
                await client.get(f"/admin/api/songs/{song_id}/takes/rvc/audio")
            ).status_code == 409
            assert (
                await client.get(f"/admin/api/songs/{song_id}/takes/missing/audio")
            ).status_code == 404
            assert (
                await client.get(f"/admin/api/songs/{song_id}/takes/BAD!/audio")
            ).status_code == 400
            assert (
                await client.get(f"/admin/api/songs/{song_id}/takes/seed_vc/audio?x=1")
            ).status_code == 400

            # Activate seed_vc -> clears old local file, updates SQLite, preserves review_note
            assert (lib.root / f"{song_id}.mp3").is_file()
            act = await client.post(f"/admin/api/songs/{song_id}/takes/seed_vc/activate")
            assert act.status_code == 200
            act_data = act.json()
            assert act_data["active_engine"] == "seed_vc"
            assert act_data["active_take"] == "seed_vc"
            assert act_data["audition_status"] == "manual"
            assert act_data["audition_selected_engine"] == "yingmusic_svc"
            assert act_data["review_note"] == "高潮共鸣很饱满喵~"
            assert not (lib.root / f"{song_id}.mp3").exists()

            # Delivery now plays the switched seed_vc version!
            delivered, fmt, mime, dur, canon = await lib.load_for_delivery("枝垂桜")
            assert delivered == mp3_seed and fmt == "mp3" and mime == "audio/mpeg"
            assert dur == 45000 and canon == "枝垂桜"

            # POST /admin/api/songs/learn with new engine/ref_voice/svc_steps & validation
            ok_dual = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "夜に駆ける",
                    "engine": "dual",
                    "ref_voice": "hakimi_ref.wav",
                    "svc_steps": 40,
                    "inbox_filename": "yoru.flac",
                    "auto_search": False,
                },
            )
            assert ok_dual.status_code == 200
            assert ok_dual.json()["engine"] == "dual"
            assert compute.submitted_jobs[-1][1]["engine"] == "dual"
            assert compute.submitted_jobs[-1][1]["ref_voice"] == "hakimi_ref.wav"
            assert compute.submitted_jobs[-1][1]["svc_steps"] == 40

            ok_rvc = await client.post(
                "/admin/api/songs/learn",
                json={
                    "title": "晴天",
                    "engine": "rvc",
                    "model_name": "hakimi",
                    "volume_envelope": 0.2,
                    "skip_dereverb": True,
                    "inbox_filename": "qingtian.flac",
                    "auto_search": False,
                },
            )
            assert ok_rvc.status_code == 200
            assert ok_rvc.json()["engine"] == "rvc"
            assert compute.submitted_jobs[-1][1]["volume_envelope"] == 0.2
            assert compute.submitted_jobs[-1][1]["skip_dereverb"] is True

            bad_eng = await client.post(
                "/admin/api/songs/learn",
                json={"title": "晴天", "engine": "unknown_engine"},
            )
            assert bad_eng.status_code == 400 and bad_eng.json()["detail"] == "invalid_song_engine"

            bad_ref = await client.post(
                "/admin/api/songs/learn",
                json={"title": "晴天", "ref_voice": "hakimi.pth"},
            )
            assert bad_ref.status_code == 400 and bad_ref.json()["detail"] == "invalid_ref_voice"

        # Chat commands: /学歌 with engine & ref (via online song_finder -> inbox -> Colab)
        from hakimi_plugin_sdk import ToolCallResult

        class _OkFinderBus:
            def has_tool(
                self, plugin_id: str, action_name: str, *, require_enabled: bool = True
            ) -> bool:
                del require_enabled
                return plugin_id == "song_finder" and action_name == "song_finder.fetch_source"

            async def invoke_tool(self, *_args: Any, **_kwargs: Any) -> ToolCallResult:
                raw = b"FOUND_SONG_BYTES"
                return ToolCallResult(
                    data={
                        "provider": "youtube",
                        "sha256": hashlib.sha256(raw).hexdigest(),
                        "filename": "qingtian.m4a",
                    },
                    blob=raw,
                    blob_mime_type="audio/mp4",
                    blob_filename="qingtian.m4a",
                )

        lib.plugin_bus = _OkFinderBus()
        owner_dm = Message(
            platform="onebot",
            bot_id="10000",
            event_id="ev-cmd-1",
            user_id="20000",
            kind="private",
            conversation_id="20000",
            text="/学歌 晴天 engine=seed ref=hakimi_ref.wav pitch=1 gpu=A100",
            addressed=True,
            created_at=time.time(),
        )
        learn_reply = await svc.command(owner_dm)
        assert learn_reply is not None and "Seed-VC" in learn_reply
        assert compute.submitted_jobs[-1][1]["engine"] == "seed_vc"
        assert compute.submitted_jobs[-1][1]["ref_voice"] == "hakimi_ref.wav"
        assert compute.submitted_jobs[-1][1]["inbox_file_id"] == "inbox-id-1"
        assert compute.submitted_jobs[-1][1]["auto_search"] is False
        assert compute.submitted_gpus[-1] == "A100"

        list_reply = await svc.command(owner_dm.model_copy(update={"text": "/换歌版本 枝垂桜"}))
        assert list_reply is not None
        assert "yingmusic_svc" in list_reply and "seed_vc" in list_reply
        assert "高潮共鸣很饱满喵~" in list_reply

        switch_reply = await svc.command(
            owner_dm.model_copy(update={"text": "/换歌版本 枝垂桜 ying"})
        )
        assert switch_reply is not None and "YingMusic-SVC" in switch_reply
        assert (await lib.get_entry_by_title("枝垂桜"))["active_engine"] == "yingmusic_svc"

        # Non-owner denied on /换歌版本
        stranger_dm = owner_dm.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "/换歌版本 枝垂桜 seed"}
        )
        with pytest.raises(Rejected, match="owner_private_only"):
            await svc.command(stranger_dm)

        # Owner chat action: song_studio.switch_version works for owner and is blocked for non-owner
        await svc._run_owner_chat_action(
            ChatActionTrigger(
                action_name="song_studio.switch_version",
                arguments_json='{"title": "枝垂桜", "take": "seed_vc"}',
            ),
            context_message=stranger_dm,
        )
        assert (await lib.get_entry_by_title("枝垂桜"))["active_engine"] == "yingmusic_svc"

        await svc._run_owner_chat_action(
            ChatActionTrigger(
                action_name="song_studio.switch_version",
                arguments_json='{"title": "枝垂桜", "take": "seed_vc"}',
            ),
            context_message=owner_dm,
        )
        assert (await lib.get_entry_by_title("枝垂桜"))["active_engine"] == "seed_vc"


async def test_gemini_gateway_audio_labels_validation_vertex_interleaving_and_long_audio_deadline(
    settings, store, sdk_pair
):
    """Verify audio_labels validation, Vertex interleaving, and >60s deadline on GeminiGateway."""
    import base64
    import json

    from hakimi.audio_media import ModelAudio
    from hakimi.gemini import GeminiGateway
    from hakimi.provider_routing import ProviderRoutingConfig
    from hakimi.system_modules.song_studio import (
        SONG_AUDITION_INSTRUCTION,
        SONG_AUDITION_RESPONSE_SCHEMA,
        SONG_AUDITION_SCENE_KEY,
    )
    from hakimi.thinking_policy import ThinkingScene
    from tests.test_sdk_contract import decision_response

    settings._thinking_scenes = {
        SONG_AUDITION_SCENE_KEY: ThinkingScene(
            SONG_AUDITION_SCENE_KEY,
            "翻唱原声亲听选片",
            default="medium",
            levels=("low", "medium", "high"),
            output_tokens=1536,
        )
    }

    mp3_a = _make_mp3_bytes(b"vertex-audio-a", total_size=300)
    mp3_b = _make_mp3_bytes(b"vertex-audio-b", total_size=320)
    a1 = ModelAudio(
        data=base64.b64encode(mp3_a).decode("ascii"),
        mime_type="audio/mp3",
        duration_sec=45.0,
        byte_size=len(mp3_a),
        digest=hashlib.sha256(mp3_a).hexdigest(),
    )
    a2 = ModelAudio(
        data=base64.b64encode(mp3_b).decode("ascii"),
        mime_type="audio/mp3",
        duration_sec=45.0,
        byte_size=len(mp3_b),
        digest=hashlib.sha256(mp3_b).hexdigest(),
    )

    captured_gc: list[dict[str, Any]] = []

    class _FakeVertexModels:
        async def generate_content(self, *, model: str, contents: Any, config: Any) -> Any:
            captured_gc.append({"model": model, "contents": contents, "config": config})
            verdict = json.dumps(
                {
                    "winner": "A",
                    "pick_reason": "A更自然",
                    "review_note": "A版很好听喵~",
                    "a_impression": "自然",
                    "b_impression": "略闷",
                },
                ensure_ascii=False,
            )
            return SimpleNamespace(
                candidates=[
                    SimpleNamespace(
                        finish_reason="STOP",
                        content=SimpleNamespace(
                            parts=[SimpleNamespace(text=verdict, thought=False)]
                        ),
                    )
                ],
                usage_metadata=SimpleNamespace(
                    prompt_token_count=3200,
                    candidates_token_count=80,
                    thoughts_token_count=0,
                    cached_content_token_count=0,
                    total_token_count=3280,
                ),
            )

    fake_vertex = SimpleNamespace(aio=SimpleNamespace(models=_FakeVertexModels()))

    def dec_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=decision_response())

    gw = GeminiGateway(settings, store, *sdk_pair(dec_handler), vertex_chat_client=fake_vertex)
    try:
        await gw.routing.initialize()
        await gw.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="generate_content",
                vertex_fallback_to_gemini=False,
            )
        )

        # Invalid audio_labels shapes rejected before dispatch
        for bad_labels in ("AB", ["【候选A】"], ["", "【候选B】"], [123, "【候选B】"]):
            with pytest.raises(Rejected, match="invalid_audio"):
                await gw.generate(
                    {"task": "audition"},
                    "system:song_studio",
                    scenario=SONG_AUDITION_SCENE_KEY,
                    budget_lane="proactive",
                    audios=[a1, a2],
                    audio_labels=bad_labels,  # type: ignore[arg-type]
                    plugin_instruction=SONG_AUDITION_INSTRUCTION,
                    response_schema=SONG_AUDITION_RESPONSE_SCHEMA,
                )
        assert captured_gc == []

        # Valid Vertex call interleaves labels + audio Parts and raises timeout >= 90s
        out = await gw.generate(
            {"task": "audition"},
            "system:song_studio",
            scenario=SONG_AUDITION_SCENE_KEY,
            budget_lane="proactive",
            audios=[a1, a2],
            audio_labels=["【候选A】", "【候选B】"],
            plugin_instruction=SONG_AUDITION_INSTRUCTION,
            response_schema=SONG_AUDITION_RESPONSE_SCHEMA,
        )
        assert json.loads(out)["winner"] == "A"
        assert len(captured_gc) == 1
        gc_call = captured_gc[0]
        assert gc_call["config"].http_options.timeout >= 90_000
        parts = gc_call["contents"][0].parts
        # [wire_data_text, label_A, audio_A, label_B, audio_B]
        assert len(parts) == 5
        assert parts[1].text == "【候选A】"
        assert parts[2].inline_data.mime_type == "audio/mp3"
        assert parts[2].inline_data.data == mp3_a
        assert parts[3].text == "【候选B】"
        assert parts[4].inline_data.mime_type == "audio/mp3"
        assert parts[4].inline_data.data == mp3_b
    finally:
        await gw.close()
