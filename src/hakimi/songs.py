"""Bounded, locally administered cover songbook for voice singing delivery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
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
MAX_REF_VOICE_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_STORED_SONG_BYTES = 1_800_000
MAX_SONG_DURATION_SECONDS = 58.0
MIN_SONG_DURATION_MS = 1000
MAX_SONGS = 300
MAX_SONG_TAKES = 8
MAX_REVIEW_NOTE_LEN = 300
MAX_SONG_LIBRARY_BYTES = 500 * 1024 * 1024
MAX_PROMPT_SONGS = 40
DEFAULT_SONG_CACHE_MAX_BYTES = 30 * 1024 * 1024
CACHE_EVICT_INTERVAL_SECONDS = 1800.0
MIN_SONG_PITCH = -24
MAX_SONG_PITCH = 24
CoverEngine = Literal["dual", "yingmusic_svc", "seed_vc", "rvc"]
DEFAULT_COVER_ENGINE: CoverEngine = "dual"
AVAILABLE_COVER_ENGINES: tuple[str, ...] = ("dual", "yingmusic_svc", "seed_vc", "rvc")
SINGLE_COVER_ENGINES: tuple[str, ...] = ("yingmusic_svc", "seed_vc", "rvc")
AUDITION_STATUSES: frozenset[str] = frozenset(
    {"picked", "fallback_single", "fallback_error", "manual", "none"}
)
RVC_DEFAULT_INDEX_RATE = 0.15
RVC_DEFAULT_PROTECT = 0.42
RVC_DEFAULT_VOLUME_ENVELOPE = 0.25
DEFAULT_BACKING_GAIN_DB = -14.0
MIN_BACKING_GAIN_DB = -24.0
MAX_BACKING_GAIN_DB = -6.0
RVC_DEFAULT_SKIP_BACKING = True
RVC_DEFAULT_SKIP_DEREVERB = False
RVC_DEFAULT_PARAMS: dict[str, Any] = {
    "model_name": "hakimi",
    "index_rate": RVC_DEFAULT_INDEX_RATE,
    "protect": RVC_DEFAULT_PROTECT,
    "volume_envelope": RVC_DEFAULT_VOLUME_ENVELOPE,
    "backing_gain_db": DEFAULT_BACKING_GAIN_DB,
    "skip_backing": RVC_DEFAULT_SKIP_BACKING,
    "skip_dereverb": RVC_DEFAULT_SKIP_DEREVERB,
    "f0_method": "rmvpe",
}
RVC_COVER_DEFAULT_GPU = "L4"
RVC_COVER_FALLBACK_GPUS: tuple[str, ...] = ("T4",)
RVC_COVER_ALLOWED_GPUS: tuple[str, ...] = ("L4", "T4", "A100", "H100")
RVC_COVER_TIMEOUT_SECONDS = 2400
RVC_TRAIN_DEFAULT_GPU = "H100"
RVC_TRAIN_FALLBACK_GPUS: tuple[str, ...] = ("A100",)
RVC_TRAIN_ALLOWED_GPUS: tuple[str, ...] = ("H100", "A100")
RVC_TRAIN_TIMEOUT_SECONDS = 3600
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
TAKE_KEY_RE = re.compile(r"^[a-z0-9_-]{1,32}\Z")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}\Z")
_MODEL_NAME_RE = re.compile(r"^[a-z0-9_-]{1,32}\Z")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}\Z")
_SAFE_INBOX_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}\Z")
_ACOUSTIC_KEY_RE = re.compile(r"^[a-z0-9_]{1,40}\Z")
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
  last_used_at REAL NOT NULL DEFAULT 0,
  active_engine TEXT NOT NULL DEFAULT '',
  active_take TEXT NOT NULL DEFAULT '',
  takes_json TEXT NOT NULL DEFAULT '[]',
  audition_status TEXT NOT NULL DEFAULT 'none',
  review_note TEXT NOT NULL DEFAULT '',
  audition_meta_json TEXT NOT NULL DEFAULT '{}'
);
"""

_MIGRATION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("drive_chorus_id", "TEXT NOT NULL DEFAULT ''"),
    ("drive_full_id", "TEXT NOT NULL DEFAULT ''"),
    ("source_origin", "TEXT NOT NULL DEFAULT 'local'"),
    ("last_used_at", "REAL NOT NULL DEFAULT 0"),
    ("active_engine", "TEXT NOT NULL DEFAULT ''"),
    ("active_take", "TEXT NOT NULL DEFAULT ''"),
    ("takes_json", "TEXT NOT NULL DEFAULT '[]'"),
    ("audition_status", "TEXT NOT NULL DEFAULT 'none'"),
    ("review_note", "TEXT NOT NULL DEFAULT ''"),
    ("audition_meta_json", "TEXT NOT NULL DEFAULT '{}'"),
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


def normalize_cover_engine(raw: Any, *, allow_aliases: bool = False) -> str:
    """Validate and normalize cover synthesis engine identifier."""
    if raw is None:
        return DEFAULT_COVER_ENGINE
    if not isinstance(raw, str):
        raise Rejected("invalid_song_engine")
    cleaned = clean_text(raw).strip().lower()
    if not cleaned:
        return DEFAULT_COVER_ENGINE
    if allow_aliases:
        alias_map = {
            "ying": "yingmusic_svc",
            "yingmusic": "yingmusic_svc",
            "yingmusic-svc": "yingmusic_svc",
            "seed": "seed_vc",
            "seedvc": "seed_vc",
            "seed-vc": "seed_vc",
            "both": "dual",
            "双引擎": "dual",
        }
        cleaned = alias_map.get(cleaned, cleaned)
    if cleaned not in AVAILABLE_COVER_ENGINES:
        raise Rejected("invalid_song_engine")
    return cleaned


def normalize_take_key(raw: Any, *, allow_aliases: bool = False) -> str:
    """Validate and normalize a path-safe take key or engine alias."""
    if not isinstance(raw, str):
        raise Rejected("invalid_song_take")
    cleaned = clean_text(raw).strip().lower()
    if allow_aliases:
        alias_map = {
            "ying": "yingmusic_svc",
            "yingmusic": "yingmusic_svc",
            "yingmusic-svc": "yingmusic_svc",
            "seed": "seed_vc",
            "seedvc": "seed_vc",
            "seed-vc": "seed_vc",
        }
        cleaned = alias_map.get(cleaned, cleaned)
    if not cleaned or not TAKE_KEY_RE.fullmatch(cleaned):
        raise Rejected("invalid_song_take")
    return cleaned


def _sanitize_ref_voice(raw: Any) -> str:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise Rejected("invalid_ref_voice")
    cleaned = clean_text(raw).strip()
    if not cleaned:
        return ""
    if (
        len(cleaned) > 160
        or ".." in cleaned
        or "/" in cleaned
        or "\\" in cleaned
        or not _SAFE_INBOX_FILENAME_RE.fullmatch(cleaned)
    ):
        raise Rejected("invalid_ref_voice")
    _, dot, ext = cleaned.rpartition(".")
    if not dot or ext.lower() not in ALLOWED_SONG_EXTENSIONS:
        raise Rejected("invalid_ref_voice")
    return cleaned


def sanitize_acoustic_metrics(raw: Any) -> dict[str, float | bool]:
    """Extract a bounded dictionary of finite numeric/bool acoustic metrics."""
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float | bool] = {}
    for key, val in raw.items():
        if len(out) >= 20:
            break
        if not isinstance(key, str) or not _ACOUSTIC_KEY_RE.fullmatch(key):
            continue
        if isinstance(val, bool):
            out[key] = val
        elif isinstance(val, (int, float)) and math.isfinite(float(val)):
            out[key] = round(float(val), 4)
    return out


def sanitize_review_note(raw: Any) -> str:
    """Normalize a single-line bounded audition review note."""
    if not isinstance(raw, str):
        return ""
    cleaned = " ".join(clean_text(raw).split()).strip()
    return cleaned[:MAX_REVIEW_NOTE_LEN].strip()


def _take_is_playable(take: dict[str, Any]) -> bool:
    return (
        take.get("status") == "succeeded"
        and bool(take.get("drive_chorus_id"))
        and isinstance(take.get("digest"), str)
        and bool(_SHA256_RE.fullmatch(take["digest"]))
        and isinstance(take.get("byte_size"), int)
        and not isinstance(take.get("byte_size"), bool)
        and 0 < take["byte_size"] <= MAX_STORED_SONG_BYTES
        and isinstance(take.get("duration_ms"), int)
        and not isinstance(take.get("duration_ms"), bool)
        and MIN_SONG_DURATION_MS <= take["duration_ms"] <= 60_000
    )


def normalize_song_takes(raw_takes: Any) -> list[dict[str, Any]]:
    """Validate and normalize archived candidate takes for a song entry."""
    if not isinstance(raw_takes, list):
        return []
    out: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in raw_takes:
        if len(out) >= MAX_SONG_TAKES:
            break
        if not isinstance(item, dict):
            continue
        raw_engine = str(item.get("engine") or "").strip().lower()
        if raw_engine not in SINGLE_COVER_ENGINES:
            continue
        raw_key = str(item.get("key") or raw_engine).strip().lower()
        if not TAKE_KEY_RE.fullmatch(raw_key) or raw_key in seen_keys:
            continue
        status = str(item.get("status") or "").strip().lower()
        if status not in {"succeeded", "failed"}:
            continue
        digest = str(item.get("digest") or "").strip().lower()
        if digest and not _SHA256_RE.fullmatch(digest):
            digest = ""
        try:
            byte_size = (
                int(item["byte_size"])
                if not isinstance(item.get("byte_size"), bool) and item.get("byte_size") is not None
                else 0
            )
        except (TypeError, ValueError):
            byte_size = 0
        try:
            duration_ms = (
                int(item["duration_ms"])
                if not isinstance(item.get("duration_ms"), bool)
                and item.get("duration_ms") is not None
                else 0
            )
        except (TypeError, ValueError):
            duration_ms = 0
        try:
            drive_chorus_id = _sanitize_drive_id(item.get("drive_chorus_id") or "")
        except Rejected:
            drive_chorus_id = ""
        try:
            drive_full_id = _sanitize_drive_id(item.get("drive_full_id") or "")
        except Rejected:
            drive_full_id = ""
        raw_err = str(item.get("error_code") or "").strip()
        error_code = raw_err[:48] if re.fullmatch(r"[a-z0-9_.-]{1,48}", raw_err) else ""
        try:
            created = float(item.get("created") or 0.0)
            if not math.isfinite(created):
                created = 0.0
        except (TypeError, ValueError):
            created = 0.0
        take: dict[str, Any] = {
            "key": raw_key,
            "engine": raw_engine,
            "status": status,
            "digest": digest,
            "byte_size": max(0, byte_size),
            "duration_ms": max(0, duration_ms),
            "drive_chorus_id": drive_chorus_id,
            "drive_full_id": drive_full_id,
            "error_code": error_code,
            "acoustic_metrics": sanitize_acoustic_metrics(item.get("acoustic_metrics")),
            "created": created,
        }
        seen_keys.add(raw_key)
        out.append(take)
    return out


def sanitize_audition_meta(raw: Any) -> dict[str, Any]:
    """Keep only bounded non-sensitive audition metadata for SQLite storage."""
    if not isinstance(raw, dict):
        return {}
    out: dict[str, Any] = {}
    for key in (
        "selected_engine",
        "selected_take",
        "engine_requested",
        "model_lane",
        "pick_reason",
    ):
        val = raw.get(key)
        if isinstance(val, str) and val.strip():
            out[key] = " ".join(clean_text(val).split()).strip()[:200]
    ab_order = raw.get("ab_order")
    if isinstance(ab_order, list) and len(ab_order) <= 4:
        clean_order = [
            str(x).strip()[:32]
            for x in ab_order
            if isinstance(x, str) and TAKE_KEY_RE.fullmatch(x.strip())
        ]
        if clean_order:
            out["ab_order"] = clean_order
    for bool_key in ("manual_override",):
        if isinstance(raw.get(bool_key), bool):
            out[bool_key] = raw[bool_key]
    for ts_key in ("reviewed_at", "switched_at"):
        val = raw.get(ts_key)
        if isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val):
            out[ts_key] = round(float(val), 3)
    return out


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


def _accepts_kwarg(fn: Any, name: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if param.name == name and param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            return True
    return False


class RvcCoverJobPayload(BaseModel):
    """Strict payload schema for Colab `rvc_cover` jobs registered by `song_studio`."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=80)
    artist: str = Field(default="", max_length=60)
    tags: str = Field(default="", max_length=120)
    engine: CoverEngine = DEFAULT_COVER_ENGINE
    ref_voice: str = Field(default="", max_length=160)
    ref_audio_file_id: str = Field(default="", max_length=128)
    svc_steps: int | None = Field(default=None, ge=10, le=100)
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
    index_rate: float = Field(default=RVC_DEFAULT_INDEX_RATE, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float = Field(default=RVC_DEFAULT_PROTECT, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float = Field(
        default=RVC_DEFAULT_VOLUME_ENVELOPE, ge=0.0, le=1.0, allow_inf_nan=False
    )
    backing_gain_db: float = Field(
        default=DEFAULT_BACKING_GAIN_DB,
        ge=MIN_BACKING_GAIN_DB,
        le=MAX_BACKING_GAIN_DB,
        allow_inf_nan=False,
    )
    skip_backing: bool = RVC_DEFAULT_SKIP_BACKING
    skip_dereverb: bool = RVC_DEFAULT_SKIP_DEREVERB

    @field_validator("title")
    @classmethod
    def _clean_title(cls, value: str) -> str:
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            raise ValueError("title cannot be empty")
        return cleaned

    @field_validator(
        "pitch",
        "svc_steps",
        "duration_sec",
        "fallback_start_sec",
        "index_rate",
        "protect",
        "volume_envelope",
        "backing_gain_db",
        mode="before",
    )
    @classmethod
    def _check_numeric_not_bool(cls, value: Any, info: Any) -> Any:
        if value is None and info.field_name == "svc_steps":
            return None
        return _reject_bool_num(value, name=str(info.field_name))

    @field_validator("skip_backing", "skip_dereverb", mode="before")
    @classmethod
    def _check_bool_flags(cls, value: Any, info: Any) -> bool:
        if not isinstance(value, bool):
            raise ValueError(f"{info.field_name} must be a boolean")
        return value

    @field_validator("inbox_file_id", "ref_audio_file_id")
    @classmethod
    def _check_drive_file_id(cls, value: str, info: Any) -> str:
        cleaned = value.strip()
        if cleaned and (cleaned.lower() == "root" or not _DRIVE_FILE_ID_RE.fullmatch(cleaned)):
            raise ValueError(f"invalid {info.field_name}")
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

    @field_validator("ref_voice")
    @classmethod
    def _check_ref_voice(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return ""
        if (
            ".." in cleaned
            or "/" in cleaned
            or "\\" in cleaned
            or not _SAFE_INBOX_FILENAME_RE.fullmatch(cleaned)
        ):
            raise ValueError("invalid ref_voice")
        _, dot, ext = cleaned.rpartition(".")
        if not dot or ext.lower() not in ALLOWED_SONG_EXTENSIONS:
            raise ValueError("invalid ref_voice")
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

    async def list_voices(self) -> dict[str, Any]:
        """List Zero-shot reference voice audio files and RVC models in `Hakimi_Songs/models/`."""
        if self.drive is None:
            raise Rejected("song_drive_not_configured")
        return await self.drive.list_models_assets()

    async def upload_ref_voice_stream(
        self,
        filename: str,
        stream: AsyncIterator[bytes],
        *,
        mime_type: str = "application/octet-stream",
        content_length: int | None = None,
        max_bytes: int = MAX_REF_VOICE_UPLOAD_BYTES,
    ) -> dict[str, Any]:
        """Stream a Zero-shot reference voice audio file into `Hakimi_Songs/models/` (<= 20 MiB)."""
        if self.drive is None:
            raise Rejected("song_drive_not_configured")
        return await self.drive.upload_ref_voice_stream(
            filename,
            stream,
            mime_type=mime_type,
            content_length=content_length,
            max_bytes=max_bytes,
        )

    @staticmethod
    def rvc_defaults() -> dict[str, Any]:
        """Return single-source universal RVC cover defaults."""
        return dict(RVC_DEFAULT_PARAMS)

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
    def _public_take(take: dict[str, Any], *, active_take: str) -> dict[str, Any]:
        return {
            "key": take["key"],
            "engine": take["engine"],
            "status": take["status"],
            "active": bool(active_take and take["key"] == active_take),
            "playable": _take_is_playable(take),
            "duration_ms": int(take.get("duration_ms") or 0),
            "byte_size": int(take.get("byte_size") or 0),
            "drive_backed": bool(take.get("drive_chorus_id")),
            "drive_full_backed": bool(take.get("drive_full_id")),
            "error_code": str(take.get("error_code") or ""),
            "acoustic_metrics": dict(take.get("acoustic_metrics") or {}),
        }

    @classmethod
    def _public(cls, entry: dict[str, Any]) -> dict[str, Any]:
        source_origin = str(entry.get("source_origin") or "local")
        active_engine = str(entry.get("active_engine") or "").strip()
        if not active_engine:
            active_engine = "local" if source_origin == "local" else "rvc"
        active_take = str(entry.get("active_take") or "").strip()
        takes = entry.get("takes") if isinstance(entry.get("takes"), list) else []
        audition_meta = (
            entry.get("audition_meta") if isinstance(entry.get("audition_meta"), dict) else {}
        )
        audition_status = str(entry.get("audition_status") or "none").strip()
        if audition_status not in AUDITION_STATUSES:
            audition_status = "none"
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
            "source_origin": source_origin,
            "active_engine": active_engine,
            "active_take": active_take,
            "audition_status": audition_status,
            "audition_selected_engine": str(audition_meta.get("selected_engine") or ""),
            "review_note": str(entry.get("review_note") or ""),
            "takes": [cls._public_take(take, active_take=active_take) for take in takes],
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
                item["active_engine"] = str(item.get("active_engine") or "").strip()
                item["active_take"] = str(item.get("active_take") or "").strip()
                raw_audition_status = str(item.get("audition_status") or "none").strip()
                item["audition_status"] = (
                    raw_audition_status if raw_audition_status in AUDITION_STATUSES else "none"
                )
                item["review_note"] = sanitize_review_note(item.get("review_note") or "")
                raw_takes_json = item.get("takes_json") or "[]"
                try:
                    parsed_takes = (
                        json.loads(raw_takes_json)
                        if isinstance(raw_takes_json, str)
                        else raw_takes_json
                    )
                except (TypeError, ValueError):
                    parsed_takes = []
                item["takes"] = normalize_song_takes(parsed_takes)
                raw_meta_json = item.get("audition_meta_json") or "{}"
                try:
                    parsed_meta = (
                        json.loads(raw_meta_json)
                        if isinstance(raw_meta_json, str)
                        else raw_meta_json
                    )
                except (TypeError, ValueError):
                    parsed_meta = {}
                item["audition_meta"] = sanitize_audition_meta(parsed_meta)
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

    def has_switchable_takes(self) -> bool:
        """Return True if at least one loaded song has 2+ playable archived takes."""
        return any(
            sum(1 for t in (entry.get("takes") or []) if _take_is_playable(t)) >= 2
            for entry in self._entries.values()
        )

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

    async def get_entry_by_title(self, title: str) -> dict[str, Any] | None:
        """Return public metadata for a loaded song matching `title` (case-insensitive)."""
        await self.initialize()
        async with self.lock:
            canonical = self.resolve_title(title)
            if canonical is None:
                return None
            entry = next((e for e in self._entries.values() if e["title"] == canonical), None)
            return self._public(entry) if entry is not None else None

    async def _insert_entry_tx(self, entry: dict[str, Any]) -> None:
        takes_json = json.dumps(entry.get("takes") or [], ensure_ascii=False)
        meta_json = json.dumps(entry.get("audition_meta") or {}, ensure_ascii=False)
        await self.store.db.execute(
            "INSERT INTO song_library ("
            "id, title, artist, tags, filename, mime_type, digest, byte_size, "
            "duration_ms, created, drive_chorus_id, drive_full_id, "
            "source_origin, last_used_at, active_engine, active_take, takes_json, "
            "audition_status, review_note, audition_meta_json"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                str(entry.get("active_engine") or ""),
                str(entry.get("active_take") or ""),
                takes_json,
                str(entry.get("audition_status") or "none"),
                str(entry.get("review_note") or ""),
                meta_json,
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
                "active_engine": "local",
                "active_take": "",
                "takes": [],
                "audition_status": "none",
                "review_note": "",
                "audition_meta": {},
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
        active_engine: str = "",
        active_take: str = "",
        takes: list[dict[str, Any]] | None = None,
        audition_status: str = "none",
        review_note: str = "",
        audition_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Register or replace a Drive-backed Colab SVC/RVC cover in the songbook."""
        clean_title = validate_song_title(title)
        clean_artist = validate_song_meta(artist, max_len=40)
        clean_tags = validate_song_meta(tags, max_len=80)
        clean_digest = str(digest).strip().lower() if isinstance(digest, str) else ""
        if not _SHA256_RE.fullmatch(clean_digest):
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

        clean_takes = normalize_song_takes(takes) if takes is not None else []
        clean_active_take = ""
        if isinstance(active_take, str) and active_take.strip():
            cand_key = active_take.strip().lower()
            if TAKE_KEY_RE.fullmatch(cand_key):
                clean_active_take = cand_key
        if not clean_active_take and clean_takes:
            matched = next(
                (
                    t["key"]
                    for t in clean_takes
                    if t["status"] == "succeeded"
                    and (
                        t.get("digest") == clean_digest
                        or t.get("drive_chorus_id") == clean_chorus_id
                    )
                ),
                "",
            )
            clean_active_take = matched
        clean_active_engine = str(active_engine or "").strip().lower()
        if clean_active_engine not in SINGLE_COVER_ENGINES:
            matched_take = next((t for t in clean_takes if t["key"] == clean_active_take), None)
            clean_active_engine = matched_take["engine"] if matched_take else "rvc"
        clean_audition_status = str(audition_status or "none").strip()
        if clean_audition_status not in AUDITION_STATUSES:
            clean_audition_status = "none"
        clean_review_note = sanitize_review_note(review_note)
        clean_audition_meta = sanitize_audition_meta(audition_meta)

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
                "active_engine": clean_active_engine,
                "active_take": clean_active_take,
                "takes": clean_takes,
                "audition_status": clean_audition_status,
                "review_note": clean_review_note,
                "audition_meta": clean_audition_meta,
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

    def learn_gpu_policy(self) -> dict[str, Any]:
        """Return the active GPU policy for `rvc_cover` learning jobs."""
        spec: Any = None
        get_spec = getattr(self.compute, "get_job_type_spec", None)
        if callable(get_spec):
            spec = get_spec("rvc_cover")
        if isinstance(spec, dict):
            default_gpu = str(spec.get("default_gpu") or RVC_COVER_DEFAULT_GPU).strip().upper()
            raw_fallbacks = spec.get("fallback_gpus")
            fallback_gpus = (
                [str(g).strip().upper() for g in raw_fallbacks if str(g).strip()]
                if isinstance(raw_fallbacks, (list, tuple))
                else list(RVC_COVER_FALLBACK_GPUS)
            )
            raw_allowed = spec.get("allowed_gpus")
            allowed_gpus = (
                [str(g).strip().upper() for g in raw_allowed if str(g).strip()]
                if isinstance(raw_allowed, (list, tuple))
                else list(RVC_COVER_ALLOWED_GPUS)
            )
            raw_timeout = spec.get("timeout_seconds")
            timeout_seconds = (
                int(raw_timeout)
                if isinstance(raw_timeout, int)
                and not isinstance(raw_timeout, bool)
                and raw_timeout > 0
                else RVC_COVER_TIMEOUT_SECONDS
            )
            return {
                "default_gpu": default_gpu or RVC_COVER_DEFAULT_GPU,
                "fallback_gpus": fallback_gpus,
                "allowed_gpus": allowed_gpus or list(RVC_COVER_ALLOWED_GPUS),
                "timeout_seconds": timeout_seconds,
            }
        return {
            "default_gpu": RVC_COVER_DEFAULT_GPU,
            "fallback_gpus": list(RVC_COVER_FALLBACK_GPUS),
            "allowed_gpus": list(RVC_COVER_ALLOWED_GPUS),
            "timeout_seconds": RVC_COVER_TIMEOUT_SECONDS,
        }

    async def request_learn_song(
        self,
        title: str,
        *,
        artist: str = "",
        tags: str = "",
        pitch: int = 0,
        engine: str = DEFAULT_COVER_ENGINE,
        ref_voice: str = "",
        ref_audio_file_id: str = "",
        svc_steps: int | None = None,
        model_name: str = "hakimi",
        index_rate: float | None = None,
        protect: float | None = None,
        volume_envelope: float | None = None,
        audio_bytes: bytes | None = None,
        audio_filename: str = "",
        inbox_filename: str = "",
        inbox_file_id: str = "",
        auto_search: bool = True,
        backing_gain_db: float | None = None,
        skip_backing: bool | None = None,
        skip_dereverb: bool | None = None,
        gpu: str | None = None,
        requester: str = "owner",
    ) -> dict[str, Any]:
        """Submit an asynchronous Colab SVC/RVC cover job via upload, inbox, or auto-search."""
        clean_title = validate_song_title(title)
        clean_artist = validate_song_meta(artist, max_len=40)
        clean_tags = validate_song_meta(tags, max_len=80)
        clean_pitch = validate_song_pitch(pitch)
        clean_engine = normalize_cover_engine(engine)
        clean_ref_voice = _sanitize_ref_voice(ref_voice)
        clean_ref_audio_id = _sanitize_drive_id(ref_audio_file_id)
        if clean_ref_audio_id.lower() == "root":
            raise Rejected("invalid_drive_file_id")
        clean_model_name = (
            str(model_name or "hakimi").strip() if isinstance(model_name, str) else ""
        )
        if not clean_model_name:
            clean_model_name = "hakimi"
        if not _MODEL_NAME_RE.fullmatch(clean_model_name):
            raise Rejected("colab_invalid_job")

        clean_svc_steps: int | None = None
        if svc_steps is not None:
            if isinstance(svc_steps, bool) or not isinstance(svc_steps, int):
                raise Rejected("colab_invalid_job")
            if not (10 <= svc_steps <= 100):
                raise Rejected("colab_invalid_job")
            clean_svc_steps = svc_steps

        clean_gpu: str | None = None
        if gpu is not None:
            if not isinstance(gpu, str):
                raise Rejected("colab_gpu_not_allowed")
            normalized_gpu = gpu.strip().upper()
            if normalized_gpu:
                if normalized_gpu not in RVC_COVER_ALLOWED_GPUS:
                    raise Rejected("colab_gpu_not_allowed")
                clean_gpu = normalized_gpu

        optional_floats: dict[str, float] = {}
        for param_name, raw_val, lo, hi in (
            ("index_rate", index_rate, 0.0, 1.0),
            ("protect", protect, 0.0, 0.5),
            ("volume_envelope", volume_envelope, 0.0, 1.0),
            ("backing_gain_db", backing_gain_db, MIN_BACKING_GAIN_DB, MAX_BACKING_GAIN_DB),
        ):
            if raw_val is not None:
                if isinstance(raw_val, bool):
                    raise Rejected("colab_invalid_job")
                try:
                    fval = float(raw_val)
                except (TypeError, ValueError):
                    raise Rejected("colab_invalid_job") from None
                if not math.isfinite(fval) or not (lo <= fval <= hi):
                    raise Rejected("colab_invalid_job")
                optional_floats[param_name] = fval

        optional_bools: dict[str, bool] = {}
        for flag_name, flag_val in (
            ("skip_backing", skip_backing),
            ("skip_dereverb", skip_dereverb),
        ):
            if flag_val is not None:
                if not isinstance(flag_val, bool):
                    raise Rejected("colab_invalid_job")
                optional_bools[flag_name] = flag_val

        if not self._compute_configured():
            raise Rejected("colab_unconfigured")
        await self.initialize()

        payload: dict[str, Any] = {
            "title": clean_title,
            "artist": clean_artist,
            "tags": clean_tags,
            "engine": clean_engine,
            "pitch": clean_pitch,
        }
        if clean_ref_voice:
            payload["ref_voice"] = clean_ref_voice
        if clean_ref_audio_id:
            payload["ref_audio_file_id"] = clean_ref_audio_id
        if clean_svc_steps is not None:
            payload["svc_steps"] = clean_svc_steps
        if clean_model_name != "hakimi":
            payload["model_name"] = clean_model_name
        payload.update(optional_floats)
        payload.update(optional_bools)

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

        submit_kwargs: dict[str, Any] = {"requester": requester}
        if clean_gpu is not None or _accepts_kwarg(self.compute.submit_job, "gpu"):
            submit_kwargs["gpu"] = clean_gpu
        job = await self.compute.submit_job("rvc_cover", payload, **submit_kwargs)
        if isinstance(job, dict) and "engine" not in job:
            return {**job, "engine": clean_engine}
        return job

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

    async def take_audio_bytes(self, song_id: str, take_key: str) -> tuple[bytes, str]:
        """Load MP3 audio bytes for a specific archived candidate take without touching active."""
        await self.initialize()
        if not isinstance(song_id, str) or not SONG_ID_RE.fullmatch(song_id):
            raise Rejected("song_not_found")
        clean_key = normalize_take_key(take_key)
        async with self.lock:
            entry = self._entries.get(song_id)
            if entry is None:
                raise Rejected("song_not_found")
            takes = entry.get("takes") if isinstance(entry.get("takes"), list) else []
            target = next((t for t in takes if t["key"] == clean_key), None)
            if target is None:
                raise Rejected("song_take_not_found")
            is_active = bool(entry.get("active_take") and entry["active_take"] == target["key"])
            if not is_active:
                if not _take_is_playable(target) or not self._can_download_drive_song():
                    raise Rejected("song_take_unavailable")
                take_snapshot = dict(target)

        if is_active:
            try:
                return await self.audio_bytes(song_id)
            except Rejected:
                raise Rejected("song_take_unavailable") from None

        try:
            downloaded = await self._download_drive_song(take_snapshot["drive_chorus_id"])
        except asyncio.CancelledError:
            raise
        except Exception:
            raise Rejected("song_take_unavailable") from None

        if not isinstance(downloaded, (bytes, bytearray)):
            raise Rejected("song_take_unavailable")
        raw = bytes(downloaded)
        if (
            not raw
            or len(raw) > MAX_STORED_SONG_BYTES
            or len(raw) != int(take_snapshot["byte_size"])
            or not _is_valid_mp3_bytes(raw)
            or hashlib.sha256(raw).hexdigest() != take_snapshot["digest"]
        ):
            raise Rejected("song_take_unavailable")
        return raw, "audio/mpeg"

    async def switch_take(
        self,
        song_id_or_title: str,
        take_key: str,
        *,
        by_title: bool = False,
    ) -> dict[str, Any]:
        """Switch a song's active playable take to another archived candidate take."""
        await self.initialize()
        clean_key = normalize_take_key(take_key, allow_aliases=by_title)
        async with self.lock:
            if by_title:
                canonical = self.resolve_title(song_id_or_title)
                if canonical is None:
                    raise Rejected("song_not_found")
                entry = next((e for e in self._entries.values() if e["title"] == canonical), None)
            else:
                if not isinstance(song_id_or_title, str) or not SONG_ID_RE.fullmatch(
                    song_id_or_title
                ):
                    raise Rejected("song_not_found")
                entry = self._entries.get(song_id_or_title)
            if entry is None:
                raise Rejected("song_not_found")

            takes = entry.get("takes") if isinstance(entry.get("takes"), list) else []
            target = next((t for t in takes if t["key"] == clean_key), None)
            if target is None:
                by_engine = [t for t in takes if t["engine"] == clean_key]
                if len(by_engine) == 1:
                    target = by_engine[0]
            if target is None:
                raise Rejected("song_take_not_found")
            if not _take_is_playable(target):
                raise Rejected("song_take_unavailable")

            if entry.get("active_take") == target["key"] and entry["digest"] == target["digest"]:
                return self._public(entry)

            if any(
                e["digest"] == target["digest"] and e["id"] != entry["id"]
                for e in self._entries.values()
            ):
                raise Rejected("song_already_exists")

            new_meta = dict(entry.get("audition_meta") or {})
            new_meta["manual_override"] = True
            new_meta["switched_at"] = round(time.time(), 3)
            new_meta = sanitize_audition_meta(new_meta)
            meta_json = json.dumps(new_meta, ensure_ascii=False)
            new_audition_status = "manual"

            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE song_library SET "
                    "digest=?, byte_size=?, duration_ms=?, drive_chorus_id=?, drive_full_id=?, "
                    "last_used_at=?, active_engine=?, active_take=?, audition_status=?, "
                    "audition_meta_json=? "
                    "WHERE id=?",
                    (
                        target["digest"],
                        int(target["byte_size"]),
                        int(target["duration_ms"]),
                        target["drive_chorus_id"],
                        target.get("drive_full_id", ""),
                        0.0,
                        target["engine"],
                        target["key"],
                        new_audition_status,
                        meta_json,
                        entry["id"],
                    ),
                )

            entry["digest"] = target["digest"]
            entry["byte_size"] = int(target["byte_size"])
            entry["duration_ms"] = int(target["duration_ms"])
            entry["drive_chorus_id"] = target["drive_chorus_id"]
            entry["drive_full_id"] = target.get("drive_full_id", "")
            entry["last_used_at"] = 0.0
            entry["cached_local"] = False
            entry["active_engine"] = target["engine"]
            entry["active_take"] = target["key"]
            entry["audition_status"] = new_audition_status
            entry["audition_meta"] = new_meta
            await asyncio.to_thread(self._unlink, entry["filename"])
            return self._public(entry)

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
