"""Bounded, locally administered cover songbook for voice singing delivery."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import re
import stat
import tempfile
import time
import unicodedata
import uuid
from collections.abc import AsyncIterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .security import Rejected, clean_text

log = logging.getLogger("hakimi")

MAX_SONG_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_LEARN_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_STORED_SONG_BYTES = 1_800_000
MAX_SONG_DURATION_SECONDS = 58.0
MIN_SONG_DURATION_MS = 1000
MAX_SONGS = 300
MAX_SONG_LIBRARY_BYTES = 500 * 1024 * 1024
MAX_PROMPT_SONGS = 40
DEFAULT_SONG_CACHE_MAX_BYTES = 30 * 1024 * 1024
CACHE_EVICT_INTERVAL_SECONDS = 1800.0
MIN_SONG_PITCH = -24
MAX_SONG_PITCH = 24
DEFAULT_BACKING_GAIN_DB = -14.0
MIN_BACKING_GAIN_DB = -24.0
MAX_BACKING_GAIN_DB = -6.0
SONG_FINDER_PLUGIN_ID = "song_finder"
SONG_FINDER_FETCH_ACTION = "song_finder.fetch_source"
SONG_FINDER_MAX_AUDIO_BYTES = 40 * 1024 * 1024
SONG_FINDER_TIMEOUT_SECONDS = 240.0
_SONG_FINDER_UNAVAILABLE_ERRORS = frozenset(
    {
        "plugin_offline",
        "plugin_disabled",
        "plugin_disconnected",
        "plugin_rpc_timeout",
        "action_not_found",
        "plugin_scope_rejected",
        "plugin_blob_disabled",
        "plugin_blob_backpressure",
    }
)

MAX_SONG_TITLE_LEN = 60
ALLOWED_SONG_TITLE_PUNCT = frozenset(
    " _-'’‘&!?！？,，.。、~～+:：·・—–()（）[]【】「」『』@#%*=♡♥★☆♪♫"
)
SONG_ID_RE = re.compile(r"^[a-f0-9]{32}\Z")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}\Z")
_SAFE_INBOX_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}\Z")
ALLOWED_SONG_EXTENSIONS = frozenset({"mp3", "wav", "ogg", "opus", "m4a", "flac"})
MIME_BY_SONG_EXTENSION = {
    "mp3": "audio/mpeg",
    "wav": "audio/wav",
    "ogg": "audio/ogg",
    "opus": "audio/opus",
    "m4a": "audio/mp4",
    "flac": "audio/flac",
}
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
  created REAL NOT NULL,
  drive_chorus_id TEXT NOT NULL DEFAULT '',
  drive_full_id TEXT NOT NULL DEFAULT '',
  source_origin TEXT NOT NULL DEFAULT 'local',
  last_used_at REAL NOT NULL DEFAULT 0
);
"""

_MIGRATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("drive_chorus_id", "TEXT NOT NULL DEFAULT ''"),
    ("drive_full_id", "TEXT NOT NULL DEFAULT ''"),
    ("source_origin", "TEXT NOT NULL DEFAULT 'local'"),
    ("last_used_at", "REAL NOT NULL DEFAULT 0"),
)


def explicit_song_request(*texts: str) -> bool:
    """Return True when message text explicitly asks Hakimi to sing a song."""
    return any(isinstance(t, str) and t and EXPLICIT_SONG_REQUEST_RE.search(t) for t in texts)


def _is_allowed_song_title_char(ch: str) -> bool:
    return ch in ALLOWED_SONG_TITLE_PUNCT or unicodedata.category(ch).startswith(("L", "M", "N"))


def is_valid_song_title(title: str) -> bool:
    """Validate canonical song title across multilingual text, digits, and safe punctuation."""
    if not isinstance(title, str):
        return False
    if not (1 <= len(title) <= MAX_SONG_TITLE_LEN):
        return False
    if title != title.strip():
        return False
    if "\r" in title or "\n" in title or "\t" in title:
        return False
    if title in {".", ".."} or "/" in title or "\\" in title:
        return False
    return all(_is_allowed_song_title_char(ch) for ch in title)


def validate_song_title(title: str) -> str:
    """Normalize whitespace and validate canonical song title."""
    if not isinstance(title, str):
        raise Rejected("invalid_song_title")
    normalized = " ".join(clean_text(title).split()).strip()
    if not is_valid_song_title(normalized):
        raise Rejected("invalid_song_title")
    return normalized


def validate_song_meta(raw: str | None, *, max_len: int) -> str:
    """Normalize optional song metadata (artist/tags) to safe single-line text."""
    if not isinstance(raw, str) or max_len <= 0:
        return ""
    cleaned = " ".join(clean_text(raw).split()).strip()
    return cleaned[:max_len].strip()


def validate_song_pitch(pitch: Any) -> int:
    """Validate RVC semitone pitch shift within [-24, 24]."""
    if isinstance(pitch, bool) or not isinstance(pitch, int):
        raise Rejected("invalid_song_pitch")
    if pitch < MIN_SONG_PITCH or pitch > MAX_SONG_PITCH:
        raise Rejected("invalid_song_pitch")
    return pitch


def _sanitize_drive_id(raw: Any) -> str:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise Rejected("invalid_drive_file_id")
    cleaned = clean_text(raw).strip()
    if not cleaned:
        return ""
    if (
        len(cleaned) > 256
        or any(ch.isspace() for ch in cleaned)
        or "/" in cleaned
        or "\\" in cleaned
        or ".." in cleaned
    ):
        raise Rejected("invalid_drive_file_id")
    return cleaned


def _sanitize_inbox_filename(raw: Any) -> str:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise Rejected("invalid_song_audio")
    cleaned = clean_text(raw).strip()
    if not cleaned:
        return ""
    if len(cleaned) > 200 or "/" in cleaned or "\\" in cleaned or ".." in cleaned:
        raise Rejected("invalid_song_audio")
    return cleaned


def _safe_learn_upload_name(audio_filename: str, fallback_title: str) -> tuple[str, str]:
    raw_name = clean_text(audio_filename).strip() if isinstance(audio_filename, str) else ""
    base = Path(raw_name).name if raw_name else ""
    ext = "mp3"
    stem_raw = fallback_title
    if base:
        stem_part, dot, ext_part = base.rpartition(".")
        if dot:
            lowered = ext_part.lower()
            if lowered not in ALLOWED_SONG_EXTENSIONS:
                raise Rejected("invalid_song_audio")
            ext = lowered
            stem_raw = stem_part or fallback_title
        else:
            stem_raw = base
    safe_stem = re.sub(r"[^\w\-]+", "_", stem_raw).strip("_")
    safe_stem = (safe_stem or "track")[:60]
    return f"{safe_stem}.{ext}", MIME_BY_SONG_EXTENSION.get(ext, "application/octet-stream")


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


def _reject_bool_num(value: Any, *, name: str) -> Any:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric, not bool")
    return value


class RvcCoverJobPayload(BaseModel):
    """Strict payload schema for Colab `rvc_cover` jobs registered by `song_studio`."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=80)
    artist: str = Field(default="", max_length=60)
    tags: str = Field(default="", max_length=120)
    pitch: int = Field(default=0, ge=MIN_SONG_PITCH, le=MAX_SONG_PITCH)
    auto_search: bool = True
    query: str = Field(default="", max_length=120)
    inbox_file_id: str = Field(default="", max_length=128)
    inbox_filename: str = Field(default="", max_length=160)
    chorus_anchor: str = Field(default="", max_length=120)
    start_sec: str | float | int = "auto"
    duration_sec: float = Field(default=50.0, ge=5.0, le=56.0, allow_inf_nan=False)
    fallback_start_sec: float = Field(default=60.0, ge=0.0, le=600.0, allow_inf_nan=False)
    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    index_rate: float = Field(default=0.15, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float = Field(default=0.42, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float = Field(default=0.55, ge=0.0, le=1.0, allow_inf_nan=False)
    backing_gain_db: float = Field(
        default=DEFAULT_BACKING_GAIN_DB,
        ge=MIN_BACKING_GAIN_DB,
        le=MAX_BACKING_GAIN_DB,
        allow_inf_nan=False,
    )
    skip_backing: bool = True

    @field_validator("title")
    @classmethod
    def _clean_title(cls, value: str) -> str:
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            raise ValueError("title cannot be empty")
        return cleaned

    @field_validator("pitch", mode="before")
    @classmethod
    def _check_pitch(cls, value: Any) -> Any:
        return _reject_bool_num(value, name="pitch")

    @field_validator("backing_gain_db", mode="before")
    @classmethod
    def _check_backing_gain_db(cls, value: Any) -> Any:
        return _reject_bool_num(value, name="backing_gain_db")

    @field_validator("skip_backing", mode="before")
    @classmethod
    def _check_skip_backing(cls, value: Any) -> bool:
        if not isinstance(value, bool):
            raise ValueError("skip_backing must be a boolean")
        return value

    @field_validator("inbox_file_id")
    @classmethod
    def _check_inbox_id(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned and (cleaned.lower() == "root" or not _DRIVE_FILE_ID_RE.fullmatch(cleaned)):
            raise ValueError("invalid inbox_file_id")
        return cleaned

    @field_validator("inbox_filename")
    @classmethod
    def _check_inbox_filename(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return ""
        if (
            ".." in cleaned
            or "/" in cleaned
            or "\\" in cleaned
            or not _SAFE_INBOX_FILENAME_RE.fullmatch(cleaned)
        ):
            raise ValueError("invalid inbox_filename")
        return cleaned


class RvcTrainJobPayload(BaseModel):
    """Strict payload schema for Colab `rvc_train` jobs registered by `song_studio`."""

    model_config = ConfigDict(extra="forbid")

    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    epochs: int = Field(default=150, ge=10, le=1000)
    save_every: int = Field(default=30, ge=5, le=500)
    batch_size: int = Field(default=12, ge=1, le=32)
    sample_rate: Literal[32000, 40000, 48000] = 48000
    force_dataset: bool = False


def validate_rvc_cover_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize an `rvc_cover` job payload for ColabComputeHub."""
    if not isinstance(payload, dict):
        raise Rejected("colab_invalid_job")
    try:
        parsed = RvcCoverJobPayload.model_validate(payload)
    except ValidationError:
        raise Rejected("colab_invalid_job") from None
    if not parsed.inbox_file_id and not parsed.inbox_filename and not parsed.auto_search:
        raise Rejected("song_source_required")
    return parsed.model_dump(mode="json")


def validate_rvc_train_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize an `rvc_train` job payload for ColabComputeHub."""
    if not isinstance(payload, dict):
        raise Rejected("colab_invalid_job")
    try:
        parsed = RvcTrainJobPayload.model_validate(payload)
    except ValidationError:
        raise Rejected("colab_invalid_job") from None
    return parsed.model_dump(mode="json")


class SongLibrary:
    """Cover songbook backed by SQLite metadata, Google Drive storage, and a bounded local cache."""

    def __init__(
        self,
        store,
        *,
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
        compute: Any | None = None,
        drive: Any | None = None,
        settings: Any | None = None,
        plugin_bus: Any | None = None,
        song_finder_remote_only: bool | None = None,
    ) -> None:
        self.store = store
        self.settings = settings if settings is not None else getattr(store, "s", None)
        data_dir = Path(getattr(self.settings, "data_dir", None) or store.s.data_dir)
        self.root = data_dir / "songs"
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.compute = compute
        self.drive = drive
        self.plugin_bus = plugin_bus
        self._song_finder_remote_only_override = song_finder_remote_only
        self.lock = asyncio.Lock()
        self.init_lock = asyncio.Lock()
        self.decode_slots = asyncio.Semaphore(1)
        self.initialized = False
        self._entries: dict[str, dict[str, Any]] = {}
        self._download_locks: dict[str, asyncio.Lock] = {}
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

    def _compute_configured(self) -> bool:
        if self.compute is None:
            return False
        fn = getattr(self.compute, "configured", None)
        return bool(fn()) if callable(fn) else True

    def _drive_configured(self) -> bool:
        if self.drive is None:
            return False
        fn = getattr(self.drive, "configured", None)
        return bool(fn()) if callable(fn) else True

    def _use_direct_drive(self, legacy_compute_attr: str) -> bool:
        if self.drive is None:
            return False
        if self._drive_configured():
            return True
        return not callable(getattr(self.compute, legacy_compute_attr, None))

    def _can_download_drive_song(self) -> bool:
        if self.drive is not None:
            return True
        return self._compute_configured() and callable(
            getattr(self.compute, "download_drive_song", None)
        )

    async def _upload_inbox_bytes(
        self, filename: str, raw: bytes, *, mime_type: str
    ) -> dict[str, Any]:
        if self._use_direct_drive("upload_drive_inbox"):
            return await self.drive.upload_inbox_bytes(filename, raw, mime_type=mime_type)
        legacy_fn = getattr(self.compute, "upload_drive_inbox", None)
        if callable(legacy_fn):
            return await legacy_fn(filename, raw, mime_type=mime_type)
        raise Rejected("song_drive_not_configured")

    async def _download_drive_song(self, file_id: str) -> bytes:
        if self._use_direct_drive("download_drive_song"):
            return await self.drive.download_song(file_id)
        legacy_fn = getattr(self.compute, "download_drive_song", None)
        if self._compute_configured() and callable(legacy_fn):
            return await legacy_fn(file_id)
        raise Rejected("song_unavailable")

    async def list_inbox(self) -> dict[str, Any]:
        """List pending original tracks in `Hakimi_Songs/inbox/` directly via SongDriveStore."""
        if self.drive is None:
            raise Rejected("song_drive_not_configured")
        tree = await self.drive.ensure_folder_tree()
        items = await self.drive.list_inbox()
        return {"items": items, "folders": tree.as_dict()}

    async def upload_inbox_stream(
        self,
        filename: str,
        stream: AsyncIterator[bytes],
        *,
        mime_type: str = "application/octet-stream",
        content_length: int | None = None,
        max_bytes: int = MAX_LEARN_UPLOAD_BYTES,
    ) -> dict[str, Any]:
        """Stream an original track into `Hakimi_Songs/inbox/` directly via SongDriveStore."""
        if self.drive is None:
            raise Rejected("song_drive_not_configured")
        return await self.drive.upload_inbox_stream(
            filename,
            stream,
            mime_type=mime_type,
            content_length=content_length,
            max_bytes=max_bytes,
        )

    async def drive_storage_status(self, *, probe: bool = False) -> dict[str, Any]:
        """Return Drive storage configuration and resolved folder tree independent of Colab."""
        if self.drive is None:
            return {
                "configured": False,
                "source": "none",
                "drive_songs_folder_id": "",
                "folders": None,
                "error": None,
            }
        return await self.drive.status_overview(probe=probe)

    async def set_drive_folder_id(self, folder_id: str | None) -> dict[str, Any]:
        """Persist or reset the `Hakimi_Songs/` root folder ID override via SongDriveStore."""
        if self.drive is None:
            raise Rejected("song_drive_not_configured")
        return await self.drive.set_folder_id(folder_id)

    def _song_finder_remote_only(self) -> bool:
        if self._song_finder_remote_only_override is not None:
            return bool(self._song_finder_remote_only_override)
        from_settings = getattr(self.settings, "song_finder_remote_only", None)
        if isinstance(from_settings, bool):
            return from_settings
        return (os.getenv("SONG_FINDER_REMOTE_ONLY") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _song_finder_online(self) -> bool:
        bus = self.plugin_bus
        if bus is None:
            return False
        has_tool = getattr(bus, "has_tool", None)
        if callable(has_tool):
            try:
                return bool(
                    has_tool(
                        SONG_FINDER_PLUGIN_ID,
                        SONG_FINDER_FETCH_ACTION,
                        require_enabled=True,
                    )
                )
            except TypeError:
                return bool(has_tool(SONG_FINDER_PLUGIN_ID, SONG_FINDER_FETCH_ACTION))
        is_online = getattr(bus, "is_plugin_online", None)
        if callable(is_online):
            try:
                return bool(is_online(SONG_FINDER_PLUGIN_ID, require_enabled=True))
            except TypeError:
                return bool(is_online(SONG_FINDER_PLUGIN_ID))
        return False

    async def _fetch_remote_song_source(
        self,
        clean_title: str,
        clean_artist: str,
    ) -> tuple[bytes, str, str, str] | None:
        """Fetch clean original audio from the isolated remote `song_finder` plugin when online."""
        remote_only = self._song_finder_remote_only()
        bus = self.plugin_bus
        invoke_tool = getattr(bus, "invoke_tool", None) if bus is not None else None
        if not self._song_finder_online() or not callable(invoke_tool):
            if remote_only:
                raise Rejected("song_finder_remote_unavailable")
            return None

        args: dict[str, Any] = {
            "query": clean_title,
            "title": clean_title,
            "artist": clean_artist,
            "strategy": "auto",
            "preferred_format": "m4a",
            "max_bytes": SONG_FINDER_MAX_AUDIO_BYTES,
        }
        try:
            tool_res = await invoke_tool(
                SONG_FINDER_PLUGIN_ID,
                SONG_FINDER_FETCH_ACTION,
                args,
                max_request_blob_bytes=0,
                max_response_blob_bytes=SONG_FINDER_MAX_AUDIO_BYTES,
                timeout=SONG_FINDER_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Rejected as exc:
            reason = str(exc) or "source_not_found"
            if remote_only:
                if reason in _SONG_FINDER_UNAVAILABLE_ERRORS:
                    raise Rejected("song_finder_remote_unavailable") from None
                raise Rejected(reason) from None
            log.info("event=song_learn_source_fallback reason=%s", reason)
            return None
        except Exception as exc:
            if remote_only:
                raise Rejected("song_finder_remote_unavailable") from None
            log.info("event=song_learn_source_fallback reason=%s", type(exc).__name__)
            return None

        raw_blob = getattr(tool_res, "blob", None)
        meta = getattr(tool_res, "data", None)
        meta_dict = meta if isinstance(meta, dict) else {}
        if not isinstance(raw_blob, (bytes, bytearray)) or not raw_blob:
            if remote_only:
                raise Rejected("source_not_found")
            log.info("event=song_learn_source_fallback reason=empty_blob")
            return None

        raw_bytes = bytes(raw_blob)
        if len(raw_bytes) > SONG_FINDER_MAX_AUDIO_BYTES or len(raw_bytes) > MAX_LEARN_UPLOAD_BYTES:
            if remote_only:
                raise Rejected("download_too_large")
            log.info("event=song_learn_source_fallback reason=download_too_large")
            return None

        expected_sha = str(meta_dict.get("sha256") or "").strip().lower()
        if expected_sha and hashlib.sha256(raw_bytes).hexdigest() != expected_sha:
            if remote_only:
                raise Rejected("song_finder_remote_unavailable")
            log.info("event=song_learn_source_fallback reason=sha256_mismatch")
            return None

        fname_hint = str(
            getattr(tool_res, "blob_filename", "") or meta_dict.get("filename") or ""
        ).strip()
        if not fname_hint:
            fmt_hint = str(meta_dict.get("format") or "m4a").strip().lower().lstrip(".")
            if fmt_hint not in ALLOWED_SONG_EXTENSIONS:
                fmt_hint = "m4a"
            fname_hint = f"{clean_title}.{fmt_hint}"

        try:
            safe_name, mime_type = _safe_learn_upload_name(fname_hint, clean_title)
        except Rejected as exc:
            if remote_only:
                raise Rejected(str(exc)) from None
            log.info("event=song_learn_source_fallback reason=%s", str(exc))
            return None

        provider_name = str(meta_dict.get("provider") or "remote").strip()[:32] or "remote"
        return raw_bytes, safe_name, mime_type, provider_name

    def _cache_max_bytes(self) -> int:
        raw = getattr(self.settings, "song_cache_max_bytes", None)
        if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0:
            return raw
        return DEFAULT_SONG_CACHE_MAX_BYTES

    @staticmethod
    def _valid_entry(row: dict[str, Any]) -> bool:
        drive_chorus_id = row.get("drive_chorus_id", "")
        drive_full_id = row.get("drive_full_id", "")
        source_origin = row.get("source_origin", "local")
        last_used_at = row.get("last_used_at", 0.0)
        return (
            isinstance(row.get("id"), str)
            and bool(SONG_ID_RE.fullmatch(row["id"]))
            and isinstance(row.get("title"), str)
            and is_valid_song_title(row["title"])
            and row.get("filename") == f"{row['id']}.mp3"
            and row.get("mime_type") == "audio/mpeg"
            and isinstance(row.get("digest"), str)
            and bool(re.fullmatch(r"[a-f0-9]{64}", row["digest"]))
            and isinstance(row.get("byte_size"), int)
            and not isinstance(row.get("byte_size"), bool)
            and 0 < row["byte_size"] <= MAX_STORED_SONG_BYTES
            and isinstance(row.get("duration_ms"), int)
            and not isinstance(row.get("duration_ms"), bool)
            and MIN_SONG_DURATION_MS <= row["duration_ms"] <= 60_000
            and isinstance(drive_chorus_id, str)
            and isinstance(drive_full_id, str)
            and isinstance(source_origin, str)
            and isinstance(last_used_at, (int, float))
            and not isinstance(last_used_at, bool)
            and math.isfinite(float(last_used_at))
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

    def _write_atomic(self, filename: str, raw: bytes) -> None:
        tmp_name = f".tmp-{uuid.uuid4().hex}"
        with self._directory() as directory:
            fd = os.open(
                tmp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(raw)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(tmp_name, filename, src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
            except BaseException:
                try:
                    os.unlink(tmp_name, dir_fd=directory)
                except FileNotFoundError:
                    pass
                raise

    def _unlink(self, filename: str) -> bool:
        with self._directory() as directory:
            try:
                os.unlink(filename, dir_fd=directory)
                os.fsync(directory)
                return True
            except FileNotFoundError:
                return False

    def _refresh_titles(self) -> None:
        self.titles = tuple(sorted(e["title"] for e in self._entries.values()))

    @staticmethod
    def _public(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": entry["id"],
            "title": entry["title"],
            "artist": entry["artist"],
            "tags": entry["tags"],
            "byte_size": entry["byte_size"],
            "duration_ms": entry["duration_ms"],
            "created": entry["created"],
            "cached_local": bool(entry.get("cached_local", False)),
            "drive_backed": bool(entry.get("drive_chorus_id")),
            "drive_full_backed": bool(entry.get("drive_full_id")),
            "source_origin": str(entry.get("source_origin") or "local"),
        }

    def cache_stats(self) -> dict[str, Any]:
        """Return non-blocking in-memory statistics for Drive backup and local LRU cache."""
        drive_entries = [e for e in self._entries.values() if e.get("drive_chorus_id")]
        cached_drive = [e for e in drive_entries if e.get("cached_local")]
        return {
            "cache_bytes": sum(int(e["byte_size"]) for e in cached_drive),
            "cache_max_bytes": self._cache_max_bytes(),
            "cached_count": len(cached_drive),
            "drive_backed_count": len(drive_entries),
            "drive_configured": self._drive_configured(),
            "colab_configured": self._compute_configured(),
            "song_finder_online": self._song_finder_online(),
        }

    async def _evict_cache_locked(
        self,
        *,
        target_bytes: int | None = None,
        keep_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        limit = self._cache_max_bytes() if target_bytes is None else max(0, int(target_bytes))
        drive_cached = [
            e for e in self._entries.values() if e.get("drive_chorus_id") and e.get("cached_local")
        ]
        cache_bytes = sum(int(e["byte_size"]) for e in drive_cached)
        evicted_count = 0
        freed_bytes = 0

        if cache_bytes > limit:
            candidates = sorted(
                (e for e in drive_cached if not keep_ids or e["id"] not in keep_ids),
                key=lambda e: (
                    float(e.get("last_used_at") or 0.0),
                    float(e.get("created") or 0.0),
                    e["id"],
                ),
            )
            for entry in candidates:
                if cache_bytes <= limit:
                    break
                removed = await asyncio.to_thread(self._unlink, entry["filename"])
                entry["cached_local"] = False
                size = int(entry["byte_size"])
                cache_bytes -= size
                evicted_count += 1
                if removed:
                    freed_bytes += size

        cached_count = sum(
            1 for e in self._entries.values() if e.get("drive_chorus_id") and e.get("cached_local")
        )
        return {
            "evicted_count": evicted_count,
            "freed_bytes": freed_bytes,
            "cache_bytes": cache_bytes,
            "cache_max_bytes": self._cache_max_bytes(),
            "cached_count": cached_count,
        }

    async def evict_cache(self, *, target_bytes: int | None = None) -> dict[str, Any]:
        """Evict least-recently-used local cache files for Drive-backed songs only."""
        if target_bytes is not None and (
            isinstance(target_bytes, bool) or not isinstance(target_bytes, int) or target_bytes < 0
        ):
            raise Rejected("invalid_admin_input")
        await self.initialize()
        async with self.lock:
            return await self._evict_cache_locked(target_bytes=target_bytes)

    async def run_cache_evictor(
        self, *, interval_seconds: float = CACHE_EVICT_INTERVAL_SECONDS
    ) -> None:
        """Periodically enforce the bounded local LRU cache without removing Drive songs."""
        while True:
            try:
                await self.evict_cache()
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("event=song_cache_evict_failed")
                await asyncio.sleep(interval_seconds)

    async def initialize(self) -> None:
        async with self.init_lock:
            if self.initialized:
                return
            with self._directory():
                pass
            async with self.store.lock:
                await self.store.db.executescript(SCHEMA)
            async with self.store.transaction():
                cols = {r["name"] for r in await self.store.all("PRAGMA table_info(song_library)")}
                for col_name, col_def in _MIGRATION_COLUMNS:
                    if col_name not in cols:
                        await self.store.db.execute(
                            f"ALTER TABLE song_library ADD COLUMN {col_name} {col_def}"
                        )
                rows = await self.store.all(
                    "SELECT * FROM song_library ORDER BY created ASC, title ASC LIMIT ?",
                    (MAX_SONGS + 5,),
                )
            valid: dict[str, dict[str, Any]] = {}
            stale_ids: list[str] = []
            for row in rows:
                item = dict(row)
                item["drive_chorus_id"] = str(item.get("drive_chorus_id") or "").strip()
                item["drive_full_id"] = str(item.get("drive_full_id") or "").strip()
                item["source_origin"] = str(item.get("source_origin") or "local").strip() or "local"
                try:
                    item["last_used_at"] = float(item.get("last_used_at") or 0.0)
                except (TypeError, ValueError):
                    item["last_used_at"] = 0.0
                if not self._valid_entry(item):
                    if isinstance(item.get("id"), str):
                        stale_ids.append(item["id"])
                    continue
                try:
                    await asyncio.to_thread(self._read, item)
                except (OSError, Rejected):
                    if item["drive_chorus_id"]:
                        # Keep Drive-backed entries indexed even when local cache is absent/evicted.
                        await asyncio.to_thread(self._unlink, item["filename"])
                        item["cached_local"] = False
                        valid[item["id"]] = item
                    else:
                        stale_ids.append(item["id"])
                    continue
                item["cached_local"] = True
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

    async def _insert_entry_tx(self, entry: dict[str, Any]) -> None:
        await self.store.db.execute(
            "INSERT INTO song_library ("
            "id, title, artist, tags, filename, mime_type, digest, byte_size, "
            "duration_ms, created, drive_chorus_id, drive_full_id, "
            "source_origin, last_used_at"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                entry["drive_chorus_id"],
                entry["drive_full_id"],
                entry["source_origin"],
                entry["last_used_at"],
            ),
        )

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
            now = time.time()
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
                "created": now,
                "drive_chorus_id": "",
                "drive_full_id": "",
                "source_origin": "local",
                "last_used_at": now,
                "cached_local": True,
            }
            self._write(stored_filename, normalized.data)
            try:
                async with self.store.transaction():
                    await self._insert_entry_tx(entry)
            except BaseException:
                self._unlink(stored_filename)
                raise
            self._entries[song_id] = entry
            self._refresh_titles()
            return self._public(entry)

    async def register_colab_cover(
        self,
        *,
        title: str,
        artist: str = "",
        tags: str = "",
        digest: str,
        byte_size: int,
        duration_ms: int,
        drive_chorus_id: str,
        drive_full_id: str = "",
        chorus_bytes: bytes | None = None,
        replace: bool = True,
    ) -> dict[str, Any]:
        """Register or replace a Drive-backed Colab RVC cover in the songbook."""
        clean_title = validate_song_title(title)
        clean_artist = validate_song_meta(artist, max_len=40)
        clean_tags = validate_song_meta(tags, max_len=80)
        clean_digest = str(digest).strip().lower() if isinstance(digest, str) else ""
        if not re.fullmatch(r"[a-f0-9]{64}", clean_digest):
            raise Rejected("invalid_song_audio")
        if (
            isinstance(byte_size, bool)
            or not isinstance(byte_size, int)
            or byte_size <= 0
            or byte_size > MAX_STORED_SONG_BYTES
        ):
            raise Rejected("invalid_song_audio")
        if (
            isinstance(duration_ms, bool)
            or not isinstance(duration_ms, int)
            or duration_ms < MIN_SONG_DURATION_MS
            or duration_ms > 60_000
        ):
            raise Rejected("invalid_song_duration")
        clean_chorus_id = _sanitize_drive_id(drive_chorus_id)
        if not clean_chorus_id:
            raise Rejected("invalid_drive_file_id")
        clean_full_id = _sanitize_drive_id(drive_full_id)

        validated_bytes: bytes | None = None
        if chorus_bytes is not None:
            if not isinstance(chorus_bytes, (bytes, bytearray)) or not chorus_bytes:
                raise Rejected("invalid_song_audio")
            validated_bytes = bytes(chorus_bytes)
            if (
                len(validated_bytes) != byte_size
                or len(validated_bytes) > MAX_STORED_SONG_BYTES
                or not _is_valid_mp3_bytes(validated_bytes)
                or hashlib.sha256(validated_bytes).hexdigest() != clean_digest
            ):
                raise Rejected("invalid_song_audio")

        await self.initialize()
        async with self.lock:
            folded = clean_title.casefold()
            existing = next(
                (e for e in self._entries.values() if e["title"].casefold() == folded),
                None,
            )
            if existing is not None and not replace:
                raise Rejected("song_title_exists")
            if any(
                e["digest"] == clean_digest and e is not existing for e in self._entries.values()
            ):
                raise Rejected("song_already_exists")
            if existing is None and len(self._entries) >= MAX_SONGS:
                raise Rejected("song_library_full")

            song_id = uuid.uuid4().hex
            stored_filename = f"{song_id}.mp3"
            now = time.time()
            entry: dict[str, Any] = {
                "id": song_id,
                "title": clean_title,
                "artist": clean_artist,
                "tags": clean_tags,
                "filename": stored_filename,
                "mime_type": "audio/mpeg",
                "digest": clean_digest,
                "byte_size": byte_size,
                "duration_ms": duration_ms,
                "created": now,
                "drive_chorus_id": clean_chorus_id,
                "drive_full_id": clean_full_id,
                "source_origin": "colab",
                "last_used_at": now if validated_bytes is not None else 0.0,
                "cached_local": validated_bytes is not None,
            }

            if validated_bytes is not None:
                await asyncio.to_thread(self._write_atomic, stored_filename, validated_bytes)

            try:
                async with self.store.transaction():
                    if existing is not None:
                        await self.store.db.execute(
                            "DELETE FROM song_library WHERE id=?", (existing["id"],)
                        )
                    await self._insert_entry_tx(entry)
            except BaseException:
                if validated_bytes is not None:
                    await asyncio.to_thread(self._unlink, stored_filename)
                raise

            if existing is not None:
                self._entries.pop(existing["id"], None)
                self._download_locks.pop(existing["id"], None)
                await asyncio.to_thread(self._unlink, existing["filename"])

            self._entries[song_id] = entry
            self._refresh_titles()
            if validated_bytes is not None:
                await self._evict_cache_locked(keep_ids={song_id})
            return self._public(entry)

    async def request_learn_song(
        self,
        title: str,
        *,
        artist: str = "",
        tags: str = "",
        pitch: int = 0,
        audio_bytes: bytes | None = None,
        audio_filename: str = "",
        inbox_filename: str = "",
        inbox_file_id: str = "",
        auto_search: bool = True,
        backing_gain_db: float | None = None,
        skip_backing: bool | None = None,
        requester: str = "owner",
    ) -> dict[str, Any]:
        """Submit an asynchronous Colab RVC cover job via upload, Drive inbox, or auto-search."""
        clean_title = validate_song_title(title)
        clean_artist = validate_song_meta(artist, max_len=40)
        clean_tags = validate_song_meta(tags, max_len=80)
        clean_pitch = validate_song_pitch(pitch)
        if not self._compute_configured():
            raise Rejected("colab_unconfigured")
        await self.initialize()

        payload: dict[str, Any] = {
            "title": clean_title,
            "artist": clean_artist,
            "tags": clean_tags,
            "pitch": clean_pitch,
        }
        if backing_gain_db is not None:
            if isinstance(backing_gain_db, bool):
                raise Rejected("colab_invalid_job")
            try:
                bg_val = float(backing_gain_db)
            except (TypeError, ValueError):
                raise Rejected("colab_invalid_job") from None
            if not math.isfinite(bg_val) or not (
                MIN_BACKING_GAIN_DB <= bg_val <= MAX_BACKING_GAIN_DB
            ):
                raise Rejected("colab_invalid_job")
            payload["backing_gain_db"] = bg_val
        if skip_backing is not None:
            if not isinstance(skip_backing, bool):
                raise Rejected("colab_invalid_job")
            payload["skip_backing"] = skip_backing

        if audio_bytes is not None:
            if not isinstance(audio_bytes, (bytes, bytearray)) or not audio_bytes:
                raise Rejected("invalid_song_audio")
            raw = bytes(audio_bytes)
            if len(raw) > MAX_LEARN_UPLOAD_BYTES:
                raise Rejected("song_upload_too_large")
            safe_name, mime_type = _safe_learn_upload_name(audio_filename, clean_title)
            uploaded = await self._upload_inbox_bytes(safe_name, raw, mime_type=mime_type)
            raw_file_id = uploaded.get("file_id") if isinstance(uploaded, dict) else ""
            file_id = _sanitize_drive_id(raw_file_id)
            if not file_id:
                raise Rejected("song_drive_invalid_file")
            payload["inbox_file_id"] = file_id
            payload["inbox_filename"] = str(uploaded.get("name") or safe_name)
            payload["auto_search"] = False
        else:
            clean_inbox_name = _sanitize_inbox_filename(inbox_filename)
            clean_inbox_id = _sanitize_drive_id(inbox_file_id)
            if clean_inbox_name or clean_inbox_id:
                if clean_inbox_name:
                    payload["inbox_filename"] = clean_inbox_name
                if clean_inbox_id:
                    payload["inbox_file_id"] = clean_inbox_id
                payload["auto_search"] = False
            elif auto_search:
                fetched = await self._fetch_remote_song_source(clean_title, clean_artist)
                if fetched is not None:
                    raw_source, safe_name, mime_type, source_provider = fetched
                    uploaded = await self._upload_inbox_bytes(
                        safe_name, raw_source, mime_type=mime_type
                    )
                    raw_file_id = uploaded.get("file_id") if isinstance(uploaded, dict) else ""
                    file_id = _sanitize_drive_id(raw_file_id)
                    if not file_id:
                        raise Rejected("song_drive_invalid_file")
                    payload["inbox_file_id"] = file_id
                    payload["inbox_filename"] = str(uploaded.get("name") or safe_name)
                    payload["auto_search"] = False
                    log.info(
                        "event=song_learn_source_resolved source=song_finder provider=%s bytes=%d",
                        source_provider,
                        len(raw_source),
                    )
                else:
                    payload["query"] = clean_title
                    payload["auto_search"] = True
            else:
                raise Rejected("song_source_required")

        return await self.compute.submit_job("rvc_cover", payload, requester=requester)

    async def _touch_last_used(self, entry: dict[str, Any], now: float) -> None:
        entry["cached_local"] = True
        entry["last_used_at"] = now
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE song_library SET last_used_at=? WHERE id=?",
                (now, entry["id"]),
            )

    async def _try_read_cached_locked(self, entry_id: str) -> tuple[bytes | None, dict[str, Any]]:
        entry = self._entries.get(entry_id)
        if entry is None:
            raise Rejected("song_unavailable")
        try:
            raw = await asyncio.to_thread(self._read, entry)
        except (OSError, Rejected):
            entry["cached_local"] = False
            if not entry["drive_chorus_id"] or not self._can_download_drive_song():
                raise Rejected("song_unavailable") from None
            return None, dict(entry)
        await self._touch_last_used(entry, time.time())
        return raw, dict(entry)

    async def _load_entry_audio(self, entry_id: str) -> tuple[bytes, dict[str, Any]]:
        async with self.lock:
            raw, snapshot = await self._try_read_cached_locked(entry_id)
            if raw is not None:
                return raw, snapshot

        dl_lock = self._download_locks.setdefault(entry_id, asyncio.Lock())
        async with dl_lock:
            async with self.lock:
                raw, snapshot = await self._try_read_cached_locked(entry_id)
                if raw is not None:
                    return raw, snapshot
                drive_chorus_id = snapshot["drive_chorus_id"]
                expected_digest = snapshot["digest"]
                expected_size = int(snapshot["byte_size"])
                filename = snapshot["filename"]

            # Download from Google Drive outside self.lock.
            try:
                downloaded = await self._download_drive_song(drive_chorus_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise Rejected("song_unavailable") from None

            if not isinstance(downloaded, (bytes, bytearray)):
                raise Rejected("song_unavailable")
            raw = bytes(downloaded)
            if (
                not raw
                or len(raw) > MAX_STORED_SONG_BYTES
                or len(raw) != expected_size
                or not _is_valid_mp3_bytes(raw)
                or hashlib.sha256(raw).hexdigest() != expected_digest
            ):
                raise Rejected("song_unavailable")

            async with self.lock:
                entry = self._entries.get(entry_id)
                if entry is None or entry["digest"] != expected_digest:
                    raise Rejected("song_unavailable")
                await asyncio.to_thread(self._write_atomic, filename, raw)
                await self._touch_last_used(entry, time.time())
                await self._evict_cache_locked(keep_ids={entry_id})
                return raw, dict(entry)

    async def audio_bytes(self, song_id: str) -> tuple[bytes, str]:
        await self.initialize()
        if not isinstance(song_id, str) or not SONG_ID_RE.fullmatch(song_id):
            raise Rejected("song_unavailable")
        raw, loaded = await self._load_entry_audio(song_id)
        return raw, loaded["mime_type"]

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
            entry_id = entry["id"]

        mp3_raw, loaded = await self._load_entry_audio(entry_id)
        duration_ms = int(loaded["duration_ms"])
        canonical_title = str(loaded["title"])

        fmt = (preferred_fmt or "mp3").strip().lower()
        if fmt == "ogg_opus":
            async with self.decode_slots:
                ogg = await transcode_mp3_to_ogg_opus(mp3_raw, ffmpeg_bin=self.ffmpeg_bin)
            return ogg, "ogg_opus", "audio/ogg", duration_ms, canonical_title
        return mp3_raw, "mp3", "audio/mpeg", duration_ms, canonical_title

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
            self._download_locks.pop(song_id, None)
            self._refresh_titles()
            self._unlink(entry["filename"])
