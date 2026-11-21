"""Bounded, locally administered cover songbook for voice singing delivery."""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import re
import stat
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .security import Rejected, clean_text

MAX_SONG_UPLOAD_BYTES = 12 * 1024 * 1024
MAX_STORED_SONG_BYTES = 1_800_000
MAX_SONG_DURATION_SECONDS = 58.0
MIN_SONG_DURATION_MS = 1000
MAX_SONGS = 100
MAX_SONG_LIBRARY_BYTES = 100 * 1024 * 1024
MAX_PROMPT_SONGS = 40

SONG_TITLE_RE = re.compile(r"^[A-Za-z0-9_\-\u3400-\u4dbf\u4e00-\u9fff（）()·\s]{1,40}\Z")
SONG_ID_RE = re.compile(r"^[a-f0-9]{32}\Z")
ALLOWED_SONG_EXTENSIONS = frozenset({"mp3", "wav", "ogg", "opus", "m4a", "flac"})
ALLOWED_SONG_MIMES = frozenset(
    {
        "audio/mpeg",
        "audio/mp3",
        "audio/wav",
        "audio/x-wav",
        "audio/wave",
        "audio/ogg",
        "audio/opus",
        "audio/mp4",
        "audio/x-m4a",
        "audio/flac",
        "application/octet-stream",
    }
)
EXPLICIT_SONG_REQUEST_RE = re.compile(
    r"(?:唱歌|唱一首|唱首|唱给|唱一下|来首歌|来一首|点歌|唱《|再唱|献唱|翻唱|"
    r"sing\s+(?:a\s+song|me|for))",
    re.IGNORECASE,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS song_library (
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


def explicit_song_request(*texts: str) -> bool:
    """Return True when message text explicitly asks Hakimi to sing a song."""
    return any(isinstance(t, str) and t and EXPLICIT_SONG_REQUEST_RE.search(t) for t in texts)


def validate_song_title(title: str) -> str:
    """Normalize whitespace and validate canonical song title."""
    if not isinstance(title, str):
        raise Rejected("invalid_song_title")
    normalized = " ".join(clean_text(title).split()).strip()
    if not normalized or not SONG_TITLE_RE.fullmatch(normalized):
        raise Rejected("invalid_song_title")
    return normalized


def validate_song_meta(raw: str | None, *, max_len: int) -> str:
    """Normalize optional song metadata (artist/tags) to safe single-line text."""
    if not isinstance(raw, str) or max_len <= 0:
        return ""
    cleaned = " ".join(clean_text(raw).split()).strip()
    return cleaned[:max_len].strip()


def validate_clip_window(start_sec: float, duration_sec: float) -> tuple[float, float]:
    """Validate optional clip trim offsets before invoking ffmpeg."""
    if isinstance(start_sec, bool) or isinstance(duration_sec, bool):
        raise Rejected("invalid_song_clip")
    try:
        start = float(start_sec)
        dur = float(duration_sec)
    except (TypeError, ValueError):
        raise Rejected("invalid_song_clip") from None
    if not (math.isfinite(start) and math.isfinite(dur)):
        raise Rejected("invalid_song_clip")
    if start < 0.0 or start > 600.0 or dur < 5.0 or dur > MAX_SONG_DURATION_SECONDS:
        raise Rejected("invalid_song_clip")
    return start, dur


def _is_valid_mp3_bytes(raw: bytes) -> bool:
    return raw.startswith(b"ID3") or (len(raw) >= 2 and raw[0] == 0xFF and (raw[1] & 0xE0) == 0xE0)


def _ext_from_filename(filename: str) -> str:
    if not isinstance(filename, str) or not filename.strip():
        return "mp3"
    _, dot, ext = Path(filename.strip()).name.rpartition(".")
    lowered = ext.lower()
    return lowered if (dot and lowered in ALLOWED_SONG_EXTENSIONS) else "mp3"


async def _run_subprocess(
    cmd: list[str],
    *,
    timeout_sec: float,
    error_code: str,
) -> tuple[bytes, bytes]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError:
        raise Rejected(error_code) from None

    try:
        async with asyncio.timeout(timeout_sec):
            stdout, stderr = await proc.communicate()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        raise
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise Rejected(error_code) from None

    if proc.returncode != 0:
        raise Rejected(error_code)
    return stdout, stderr


@dataclass(frozen=True)
class NormalizedSong:
    data: bytes
    mime_type: str
    duration_ms: int
    digest: str


async def normalize_song_audio(
    raw: bytes,
    *,
    ext_hint: str = "mp3",
    start_sec: float = 0.0,
    duration_sec: float = MAX_SONG_DURATION_SECONDS,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> NormalizedSong:
    """Trim and normalize uploaded audio into a bounded metadata-free MP3 file."""
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        raise Rejected("invalid_song_audio")
    raw_bytes = bytes(raw)
    if len(raw_bytes) > MAX_SONG_UPLOAD_BYTES:
        raise Rejected("song_upload_too_large")

    start, dur = validate_clip_window(start_sec, duration_sec)
    ext = ext_hint.strip().lower().lstrip(".")
    if ext not in ALLOWED_SONG_EXTENSIONS:
        ext = "mp3"

    with tempfile.TemporaryDirectory(prefix="hakimi-song-") as tmpdir:
        tmp = Path(tmpdir)
        in_path = tmp / f"input.{ext}"
        out_path = tmp / "out.mp3"
        await asyncio.to_thread(in_path.write_bytes, raw_bytes)

        base_cmd = [ffmpeg_bin, "-hide_banner", "-loglevel", "error", "-y"]
        if start > 0.0:
            base_cmd.extend(["-ss", f"{start:.3f}"])
        base_cmd.extend(
            [
                "-i",
                str(in_path),
                "-t",
                f"{dur:.3f}",
                "-vn",
                "-sn",
                "-dn",
                "-map_metadata",
                "-1",
                "-ar",
                "44100",
            ]
        )

        cmd_stereo = [*base_cmd, "-ac", "2", "-c:a", "libmp3lame", "-b:a", "128k", str(out_path)]
        await _run_subprocess(cmd_stereo, timeout_sec=45.0, error_code="invalid_song_audio")

        data = await asyncio.to_thread(out_path.read_bytes) if out_path.is_file() else b""
        if len(data) > MAX_STORED_SONG_BYTES:
            cmd_mono = [*base_cmd, "-ac", "1", "-c:a", "libmp3lame", "-b:a", "96k", str(out_path)]
            await _run_subprocess(cmd_mono, timeout_sec=45.0, error_code="invalid_song_audio")
            data = await asyncio.to_thread(out_path.read_bytes) if out_path.is_file() else b""
            if len(data) > MAX_STORED_SONG_BYTES:
                raise Rejected("song_payload_too_large")

        if not data or not _is_valid_mp3_bytes(data):
            raise Rejected("invalid_song_audio")

        probe_cmd = [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(out_path),
        ]
        probe_out, _ = await _run_subprocess(
            probe_cmd, timeout_sec=10.0, error_code="invalid_song_audio"
        )
        try:
            probed_sec = float(probe_out.decode("utf-8", errors="ignore").strip())
        except ValueError:
            raise Rejected("invalid_song_audio") from None
        if not math.isfinite(probed_sec):
            raise Rejected("invalid_song_audio")
        duration_ms = int(round(probed_sec * 1000.0))
        if duration_ms < MIN_SONG_DURATION_MS or duration_ms > 60_000:
            raise Rejected("invalid_song_duration")

        digest = hashlib.sha256(data).hexdigest()
        return NormalizedSong(
            data=data,
            mime_type="audio/mpeg",
            duration_ms=duration_ms,
            digest=digest,
        )


async def transcode_mp3_to_ogg_opus(
    mp3_bytes: bytes,
    *,
    ffmpeg_bin: str = "ffmpeg",
) -> bytes:
    """Transcode normalized MP3 bytes into OGG Opus for non-QQ platforms."""
    if not isinstance(mp3_bytes, (bytes, bytearray)) or not mp3_bytes:
        raise Rejected("song_transcode_failed")
    raw = bytes(mp3_bytes)
    if len(raw) > MAX_STORED_SONG_BYTES or not _is_valid_mp3_bytes(raw):
        raise Rejected("song_transcode_failed")

    with tempfile.TemporaryDirectory(prefix="hakimi-song-") as tmpdir:
        tmp = Path(tmpdir)
        in_path = tmp / "in.mp3"
        out_path = tmp / "out.ogg"
        await asyncio.to_thread(in_path.write_bytes, raw)
        cmd = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(in_path),
            "-vn",
            "-sn",
            "-dn",
            "-map_metadata",
            "-1",
            "-ar",
            "48000",
            "-ac",
            "1",
            "-c:a",
            "libopus",
            "-b:a",
            "64k",
            "-f",
            "ogg",
            str(out_path),
        ]
        await _run_subprocess(cmd, timeout_sec=30.0, error_code="song_transcode_failed")
        ogg = await asyncio.to_thread(out_path.read_bytes) if out_path.is_file() else b""
        if not ogg or len(ogg) > MAX_STORED_SONG_BYTES or not ogg.startswith(b"OggS"):
            raise Rejected("song_transcode_failed")
        return ogg


class SongLibrary:
    """Locally administered pre-recorded cover songbook stored under data_dir/songs."""

    def __init__(
        self,
        store,
        *,
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
    ) -> None:
        self.store = store
        self.root = store.s.data_dir / "songs"
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.lock = asyncio.Lock()
        self.init_lock = asyncio.Lock()
        self.decode_slots = asyncio.Semaphore(1)
        self.initialized = False
        self._entries: dict[str, dict[str, Any]] = {}
        self.titles: tuple[str, ...] = ()

    @contextmanager
    def _directory(self):
        self.root.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fchmod(fd, 0o700)
            yield fd
        finally:
            os.close(fd)

    @staticmethod
    def _valid_entry(row: dict[str, Any]) -> bool:
        return (
            isinstance(row.get("id"), str)
            and bool(SONG_ID_RE.fullmatch(row["id"]))
            and isinstance(row.get("title"), str)
            and bool(SONG_TITLE_RE.fullmatch(row["title"]))
            and row.get("filename") == f"{row['id']}.mp3"
            and row.get("mime_type") == "audio/mpeg"
            and isinstance(row.get("digest"), str)
            and bool(re.fullmatch(r"[a-f0-9]{64}", row["digest"]))
            and isinstance(row.get("byte_size"), int)
            and 0 < row["byte_size"] <= MAX_STORED_SONG_BYTES
            and isinstance(row.get("duration_ms"), int)
            and MIN_SONG_DURATION_MS <= row["duration_ms"] <= 60_000
        )

    def _read(self, entry: dict[str, Any]) -> bytes:
        with self._directory() as directory:
            fd = os.open(
                entry["filename"],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory,
            )
            with os.fdopen(fd, "rb") as source:
                info = os.fstat(source.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                    or info.st_size != entry["byte_size"]
                    or info.st_size > MAX_STORED_SONG_BYTES
                ):
                    raise Rejected("song_unavailable")
                raw = source.read(MAX_STORED_SONG_BYTES + 1)
        if len(raw) != entry["byte_size"] or hashlib.sha256(raw).hexdigest() != entry["digest"]:
            raise Rejected("song_unavailable")
        if not _is_valid_mp3_bytes(raw):
            raise Rejected("song_unavailable")
        return raw

    def _write(self, filename: str, raw: bytes) -> None:
        with self._directory() as directory:
            fd = os.open(
                filename,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(raw)
                    output.flush()
                    os.fsync(output.fileno())
                os.fsync(directory)
            except BaseException:
                os.unlink(filename, dir_fd=directory)
                raise

    def _unlink(self, filename: str) -> None:
        with self._directory() as directory:
            try:
                os.unlink(filename, dir_fd=directory)
                os.fsync(directory)
            except FileNotFoundError:
                pass

    def _refresh_titles(self) -> None:
        self.titles = tuple(sorted(e["title"] for e in self._entries.values()))

    @staticmethod
    def _public(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            key: entry[key]
            for key in ("id", "title", "artist", "tags", "byte_size", "duration_ms", "created")
        }

    async def initialize(self) -> None:
        async with self.init_lock:
            if self.initialized:
                return
            with self._directory():
                pass
            async with self.store.lock:
                await self.store.db.executescript(SCHEMA)
                rows = await self.store.all(
                    "SELECT * FROM song_library ORDER BY created ASC, title ASC LIMIT 105"
                )
            valid: dict[str, dict[str, Any]] = {}
            stale_ids: list[str] = []
            for row in rows:
                item = dict(row)
                if not self._valid_entry(item):
                    if isinstance(item.get("id"), str):
                        stale_ids.append(item["id"])
                    continue
                try:
                    await asyncio.to_thread(self._read, item)
                except (OSError, Rejected):
                    stale_ids.append(item["id"])
                    continue
                valid[item["id"]] = item
            if stale_ids:
                async with self.store.transaction():
                    for bad_id in stale_ids:
                        await self.store.db.execute(
                            "DELETE FROM song_library WHERE id=?", (bad_id,)
                        )
            self._entries = valid
            self._refresh_titles()
            self.initialized = True

    def catalog_entries(self) -> list[dict[str, Any]]:
        """Return bounded songbook entries for model turn context."""
        items: list[dict[str, Any]] = []
        for entry in sorted(self._entries.values(), key=lambda e: e["title"]):
            row: dict[str, Any] = {
                "title": entry["title"],
                "duration_sec": round(entry["duration_ms"] / 1000.0, 1),
            }
            if entry["artist"]:
                row["artist"] = entry["artist"]
            if entry["tags"]:
                row["tags"] = entry["tags"]
            items.append(row)
            if len(items) >= MAX_PROMPT_SONGS:
                break
        return items

    def resolve_title(self, raw: str | None) -> str | None:
        """Resolve an input title strictly against loaded song titles."""
        if not isinstance(raw, str):
            return None
        cleaned = " ".join(clean_text(raw).split()).strip()
        if not cleaned:
            return None
        folded = cleaned.casefold()
        for title in self.titles:
            if title == cleaned or title.casefold() == folded:
                return title
        return None

    async def list(self) -> list[dict[str, Any]]:
        await self.initialize()
        async with self.lock:
            return [
                self._public(entry)
                for entry in sorted(
                    self._entries.values(),
                    key=lambda e: (-float(e["created"]), str(e["title"])),
                )
            ]

    async def upload(
        self,
        title: str,
        raw: bytes,
        *,
        artist: str = "",
        tags: str = "",
        filename: str = "",
        start_sec: float = 0.0,
        duration_sec: float = MAX_SONG_DURATION_SECONDS,
    ) -> dict[str, Any]:
        clean_title = validate_song_title(title)
        clean_artist = validate_song_meta(artist, max_len=40)
        clean_tags = validate_song_meta(tags, max_len=80)
        validate_clip_window(start_sec, duration_sec)
        ext_hint = _ext_from_filename(filename)

        await self.initialize()
        async with self.lock:
            if any(e["title"].casefold() == clean_title.casefold() for e in self._entries.values()):
                raise Rejected("song_title_exists")
            if len(self._entries) >= MAX_SONGS:
                raise Rejected("song_library_full")

        async with self.decode_slots:
            normalized = await normalize_song_audio(
                raw,
                ext_hint=ext_hint,
                start_sec=start_sec,
                duration_sec=duration_sec,
                ffmpeg_bin=self.ffmpeg_bin,
                ffprobe_bin=self.ffprobe_bin,
            )

        async with self.lock:
            if any(e["title"].casefold() == clean_title.casefold() for e in self._entries.values()):
                raise Rejected("song_title_exists")
            if any(e["digest"] == normalized.digest for e in self._entries.values()):
                raise Rejected("song_already_exists")
            total_bytes = sum(int(e["byte_size"]) for e in self._entries.values())
            if (
                len(self._entries) >= MAX_SONGS
                or total_bytes + len(normalized.data) > MAX_SONG_LIBRARY_BYTES
            ):
                raise Rejected("song_library_full")

            song_id = uuid.uuid4().hex
            stored_filename = f"{song_id}.mp3"
            entry: dict[str, Any] = {
                "id": song_id,
                "title": clean_title,
                "artist": clean_artist,
                "tags": clean_tags,
                "filename": stored_filename,
                "mime_type": normalized.mime_type,
                "digest": normalized.digest,
                "byte_size": len(normalized.data),
                "duration_ms": normalized.duration_ms,
                "created": time.time(),
            }
            self._write(stored_filename, normalized.data)
            try:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "INSERT INTO song_library ("
                        "id, title, artist, tags, filename, mime_type, digest, byte_size, "
                        "duration_ms, created"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (
                            entry["id"],
                            entry["title"],
                            entry["artist"],
                            entry["tags"],
                            entry["filename"],
                            entry["mime_type"],
                            entry["digest"],
                            entry["byte_size"],
                            entry["duration_ms"],
                            entry["created"],
                        ),
                    )
            except BaseException:
                self._unlink(stored_filename)
                raise
            self._entries[song_id] = entry
            self._refresh_titles()
            return self._public(entry)

    async def audio_bytes(self, song_id: str) -> tuple[bytes, str]:
        await self.initialize()
        if not isinstance(song_id, str) or not SONG_ID_RE.fullmatch(song_id):
            raise Rejected("song_unavailable")
        async with self.lock:
            entry = self._entries.get(song_id)
            if entry is None:
                raise Rejected("song_unavailable")
            try:
                raw = await asyncio.to_thread(self._read, entry)
            except (OSError, Rejected):
                raise Rejected("song_unavailable") from None
            return raw, entry["mime_type"]

    async def load_for_delivery(
        self,
        title: str,
        *,
        preferred_fmt: str = "mp3",
    ) -> tuple[bytes, str, str, int, str]:
        """Load song audio bytes transcoded to the target platform format."""
        await self.initialize()
        async with self.lock:
            canonical = self.resolve_title(title)
            if canonical is None:
                raise Rejected("song_unavailable")
            entry = next(
                (e for e in self._entries.values() if e["title"] == canonical),
                None,
            )
            if entry is None:
                raise Rejected("song_unavailable")
            try:
                mp3_raw = await asyncio.to_thread(self._read, entry)
            except (OSError, Rejected):
                raise Rejected("song_unavailable") from None
            duration_ms = int(entry["duration_ms"])

        fmt = (preferred_fmt or "mp3").strip().lower()
        if fmt == "ogg_opus":
            async with self.decode_slots:
                ogg = await transcode_mp3_to_ogg_opus(mp3_raw, ffmpeg_bin=self.ffmpeg_bin)
            return ogg, "ogg_opus", "audio/ogg", duration_ms, canonical
        return mp3_raw, "mp3", "audio/mpeg", duration_ms, canonical

    async def delete(self, song_id: str) -> None:
        await self.initialize()
        if not isinstance(song_id, str) or not SONG_ID_RE.fullmatch(song_id):
            raise Rejected("song_not_found")
        async with self.lock:
            entry = self._entries.get(song_id)
            if entry is None:
                raise Rejected("song_not_found")
            async with self.store.transaction():
                await self.store.db.execute("DELETE FROM song_library WHERE id=?", (song_id,))
            del self._entries[song_id]
            self._refresh_titles()
            self._unlink(entry["filename"])
