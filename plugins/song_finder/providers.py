"""Isolated multi-provider clean music source search, download, and transcode engine.

All providers are self-contained and strictly anonymous (zero account cookies or shared state).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import html
import json
import logging
import math
import os
import re
import shutil
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import httpx

log = logging.getLogger("hakimi_plugin_song_finder")

MIN_SOURCE_DURATION_SEC = 60
MAX_SOURCE_DURATION_SEC = 600
HARD_MIN_DURATION_SEC = 15
HARD_MAX_DURATION_SEC = 900
DEFAULT_MAX_DOWNLOAD_BYTES = 40 * 1024 * 1024
DEFAULT_MAX_OUTPUT_BYTES = 40 * 1024 * 1024
MIN_OUTPUT_BYTES_LIMIT = 65_536
MIN_DOWNLOAD_BYTES = 8192
MAX_QUERY_LEN = 160
MAX_SEARCH_LIMIT = 10
DEFAULT_COOLDOWN_SECONDS = 600.0
MAX_COOLDOWN_SECONDS = 3600.0
DEFAULT_MIN_INTERVAL_SECONDS = 1.0
MAX_HTTP_JSON_BYTES = 512 * 1024
DEFAULT_DOWNLOAD_TIMEOUT_SEC = 120.0
_DOWNLOAD_WRITE_BUFFER_BYTES = 262_144

VALID_PROVIDERS: tuple[str, ...] = ("youtube", "bilibili_public")
DEFAULT_PROVIDER_ORDER: tuple[str, ...] = ("bilibili_public", "youtube")
SUPPORTED_OUTPUT_FORMATS: frozenset[str] = frozenset({"m4a", "mp3", "flac"})
MIME_BY_FORMAT: dict[str, str] = {
    "m4a": "audio/mp4",
    "mp3": "audio/mpeg",
    "flac": "audio/flac",
}

_PUBLIC_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_SAFE_SUBPROCESS_ENV_KEYS = (
    "PATH",
    "LANG",
    "LC_ALL",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "TMPDIR",
)

_CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")
_SCHEME_RE = re.compile(r"(?i)(?:://|\b(?:file|ftp|data|javascript|php|gopher|dict):)")
_LOCAL_PATH_RE = re.compile(r"(?:^|\s)(?:[/\\~]|\.{1,2}[/\\]|[A-Za-z]:[/\\])")
_TRAVERSAL_RE = re.compile(r"(?:^|[/\\])\.\.(?:[/\\]|$)")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BVID_RE = re.compile(r"^BV[1-9A-HJ-NP-Za-km-z]{10}\Z")
_YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}\Z")
_CANDIDATE_ID_RE = re.compile(r"^(youtube|bilibili_public):([A-Za-z0-9_-]{4,32})\Z")
_ANON_BUVID_VAL_RE = re.compile(r"^[A-Za-z0-9_.%=-]{8,160}\Z")
_YOUTUBE_HOSTS = frozenset(
    {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",
    }
)
_DNS_LABEL_HOST_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$"
)
_AKAMAI_UPOS_RE = re.compile(r"^upos-[a-z0-9-]+\.akamaized\.net\Z")

_ASCII_NOISY_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pat, re.IGNORECASE)
    for pat in (
        r"\bcover\b",
        r"\bremix\b",
        r"\bnightcore\b",
        r"\b8d\b",
        r"\bdj\b",
        r"\bkaraoke\b",
        r"\bktv\b",
        r"\binstrumental\b",
        r"\bacapella\b",
        r"\blive\b",
        r"\bconcert\b",
        r"\breaction\b",
        r"#?shorts\b",
        r"\bslowed\b",
        r"\breverb\b",
        r"\bspeed\s*up\b",
        r"\bsped\s*up\b",
        r"\btutorial\b",
        r"\bpiano\s*ver\b",
        r"\bmashup\b",
        r"\bmedley\b",
        r"\b1\s*hour\b",
        r"\bloop\b",
    )
)

_CJK_NOISY_KEYWORDS: tuple[str, ...] = (
    "翻唱",
    "伴奏",
    "消音",
    "纯音乐",
    "现场",
    "演唱会",
    "反应",
    "升调",
    "降调",
    "教学",
    "吉他弹唱",
    "钢琴版",
    "翻自",
    "合唱版",
    "串烧",
    "合集",
    "联唱",
    "混剪",
    "鬼畜",
    "循环",
    "1小时",
    "一小时",
    "人声消除",
)

_POSITIVE_TITLE_MARKERS: tuple[str, ...] = (
    "official audio",
    "official music video",
    "official mv",
    "官方",
    "原版",
    "无损",
    "高音质",
    "hi-res",
    "flac",
)

_RISK_STDERR_MARKERS: tuple[str, ...] = (
    "http error 429",
    "http error 403",
    "http error 412",
    "too many requests",
    "sign in to confirm",
    "confirm you're not a bot",
    "confirm you’re not a bot",
    "rate-limit",
    "rate limit",
    "blocked",
    "captcha",
)

_WBI_MIXIN_KEY_ENC_TAB: tuple[int, ...] = (
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    48,
    7,
    16,
    24,
    55,
    40,
    61,
    26,
    17,
    0,
    1,
    60,
    51,
    30,
    4,
    22,
    25,
    54,
    21,
    56,
    59,
    6,
    63,
    57,
    62,
    11,
    36,
    20,
    34,
    44,
    52,
)
_WBI_FILTER_CHARS = frozenset("!'()*")
_BILI_RISK_API_CODES = frozenset({-352, -412, -403, -429, -509})
_PROPAGATABLE_FETCH_ERRORS = frozenset(
    {
        "download_too_large",
        "duration_out_of_range",
        "transcode_failed",
        "ssrf_blocked",
    }
)


class SongFinderError(Exception):
    """Typed constant-code error raised by song_finder providers or validation."""

    def __init__(self, code: str, message: str = "", *, provider: str = "") -> None:
        self.code = str(code or "source_not_found").strip() or "source_not_found"
        self.message = str(message or self.code)
        self.provider = provider
        super().__init__(self.message)


class ProviderRiskControlError(SongFinderError):
    """Raised when one specific upstream provider triggers anti-bot / rate-limit risk control."""

    def __init__(self, provider: str, reason: str = "provider_risk_control") -> None:
        super().__init__(reason, f"{provider}:{reason}", provider=provider)


@dataclass(frozen=True)
class SongCandidate:
    """Normalized clean audio candidate produced by a provider."""

    provider: str
    source_id: str
    title: str
    uploader: str
    duration_sec: float
    url: str
    score: float = 0.0

    @property
    def candidate_id(self) -> str:
        return f"{self.provider}:{self.source_id}"

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "provider": self.provider,
            "title": self.title,
            "uploader": self.uploader,
            "duration_sec": round(self.duration_sec, 1),
            "url": self.url,
            "score": round(self.score, 2),
        }


@dataclass(frozen=True)
class FetchedSongSource:
    """Validated, transcoded clean original audio payload ready for inbox upload."""

    provider: str
    source_id: str
    title: str
    uploader: str
    duration_sec: float
    audio_format: str
    mime_type: str
    filename: str
    data: bytes
    sha256: str
    byte_size: int
    skipped_providers: dict[str, str] = field(default_factory=dict)


def _clean_subprocess_env() -> dict[str, str]:
    """Build a minimal allowlisted subprocess environment (no credentials or unrelated env vars)."""
    env: dict[str, str] = {
        "HOME": "/tmp",
        "XDG_CACHE_HOME": "/tmp/.cache",
        "XDG_CONFIG_HOME": "/tmp/.config",
        "TMPDIR": "/tmp",
    }
    for key in _SAFE_SUBPROCESS_ENV_KEYS:
        val = os.environ.get(key)
        if val:
            env[key] = val
    return env


def _validate_query_fragment(raw: Any, *, allow_empty: bool) -> str:
    if raw is None:
        if allow_empty:
            return ""
        raise SongFinderError("invalid_query", "query must be a string")
    if not isinstance(raw, str):
        raise SongFinderError("invalid_query", "query field must be a string")
    if _CONTROL_CHAR_RE.search(raw):
        raise SongFinderError("invalid_query", "control characters are forbidden")
    stripped = raw.strip()
    if not stripped:
        if allow_empty:
            return ""
        raise SongFinderError("invalid_query", "query cannot be empty")
    if stripped.startswith("-"):
        raise SongFinderError("invalid_query", "leading option flag is forbidden")
    if _SCHEME_RE.search(stripped):
        raise SongFinderError("invalid_query", "URL or URI scheme is forbidden in search query")
    if _LOCAL_PATH_RE.search(stripped) or _TRAVERSAL_RE.search(stripped):
        raise SongFinderError("invalid_query", "local file path is forbidden in search query")
    normalized = " ".join(stripped.split()).strip()
    if normalized.startswith("-"):
        raise SongFinderError("invalid_query", "leading option flag is forbidden")
    return normalized


def sanitize_search_query(query: str, *, title: str = "", artist: str = "") -> str:
    """Validate and normalize a song search query, rejecting paths, URLs, and option flags."""
    clean_q = _validate_query_fragment(query, allow_empty=bool(title and isinstance(title, str)))
    clean_t = _validate_query_fragment(title, allow_empty=True)
    clean_a = _validate_query_fragment(artist, allow_empty=True)

    primary = clean_q or clean_t
    if not primary:
        raise SongFinderError("invalid_query", "query cannot be empty")
    if clean_a and clean_a.lower() not in primary.lower():
        combined = f"{clean_a} {primary}".strip()
    else:
        combined = primary
    if len(combined) > MAX_QUERY_LEN:
        raise SongFinderError("invalid_query", "query exceeds max length")
    if combined.startswith("-"):
        raise SongFinderError("invalid_query", "leading option flag is forbidden")
    return combined


def validate_proxy_url(raw: str | None) -> str | None:
    """Validate optional outbound proxy URL for yt-dlp without leaking credentials."""
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    if (
        cleaned.startswith("-")
        or _CONTROL_CHAR_RE.search(cleaned)
        or any(c.isspace() for c in cleaned)
    ):
        raise SongFinderError("invalid_query", "invalid proxy URL characters")
    parsed = urlsplit(cleaned)
    if parsed.scheme.lower() not in {"http", "https", "socks5", "socks5h"} or not parsed.hostname:
        raise SongFinderError("invalid_query", "unsupported proxy scheme or missing host")
    return cleaned


def validate_youtube_url(url: str) -> str:
    """Allowlist YouTube HTTPS watch/video URLs before passing to yt-dlp."""
    if not isinstance(url, str):
        raise SongFinderError("ssrf_blocked", "source URL must be a string")
    cleaned = url.strip()
    if (
        not cleaned
        or cleaned.startswith("-")
        or _CONTROL_CHAR_RE.search(cleaned)
        or any(c.isspace() for c in cleaned)
    ):
        raise SongFinderError("ssrf_blocked", "invalid source URL")
    parsed = urlsplit(cleaned)
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in (None, 443)
        or host not in _YOUTUBE_HOSTS
    ):
        raise SongFinderError("ssrf_blocked", "disallowed YouTube host")
    if "/shorts/" in parsed.path.lower():
        raise SongFinderError("ssrf_blocked", "YouTube shorts disallowed")
    return cleaned


def validate_bili_cdn_url(url: str) -> str:
    """Strict independent SSRF domain allowlist for public audio CDN hosts."""
    if not isinstance(url, str):
        raise SongFinderError("ssrf_blocked", "CDN URL must be a string")
    cleaned = url.strip()
    if not cleaned or _CONTROL_CHAR_RE.search(cleaned) or any(c.isspace() for c in cleaned):
        raise SongFinderError("ssrf_blocked", "invalid CDN URL")
    parsed = urlsplit(cleaned)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (
        parsed.scheme.lower() != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in (None, 443)
        or not host
        or not _DNS_LABEL_HOST_RE.fullmatch(host)
    ):
        raise SongFinderError("ssrf_blocked", "disallowed CDN URL structure")

    allowed = (
        host.endswith(".bilivideo.com")
        or host.endswith(".bilivideo.cn")
        or bool(_AKAMAI_UPOS_RE.fullmatch(host))
    )
    if not allowed:
        raise SongFinderError("ssrf_blocked", "CDN host outside strict allowlist")
    return cleaned


def parse_candidate_id(raw: str | None) -> tuple[str, str]:
    """Strictly parse a candidate identifier (`<provider>:<source_id>`) for targeted download."""
    if not isinstance(raw, str):
        raise SongFinderError("invalid_query", "candidate_id must be a string")
    cleaned = raw.strip()
    match = _CANDIDATE_ID_RE.fullmatch(cleaned)
    if not match:
        raise SongFinderError("invalid_query", "invalid candidate_id format")
    provider, source_id = match.group(1), match.group(2)
    if provider == "bilibili_public":
        if not _BVID_RE.fullmatch(source_id):
            raise SongFinderError("invalid_query", "invalid bilibili bvid")
    elif provider == "youtube":
        if not _YOUTUBE_ID_RE.fullmatch(source_id):
            raise SongFinderError("invalid_query", "invalid youtube video id")
    else:
        raise SongFinderError("invalid_query", "unsupported candidate provider")
    return provider, source_id


def parse_provider_order(raw: str | Sequence[str] | None) -> tuple[str, ...]:
    """Parse configured provider priority chain into valid unique provider IDs."""
    if raw is None:
        return DEFAULT_PROVIDER_ORDER
    items = raw.split(",") if isinstance(raw, str) else list(raw)
    out: list[str] = []
    for item in items:
        clean = str(item or "").strip().lower()
        if clean in VALID_PROVIDERS and clean not in out:
            out.append(clean)
    return tuple(out) if out else DEFAULT_PROVIDER_ORDER


def validate_output_format(raw: str | None) -> str:
    fmt = str(raw or "m4a").strip().lower().lstrip(".")
    if fmt not in SUPPORTED_OUTPUT_FORMATS:
        raise SongFinderError("invalid_query", f"unsupported format: {fmt}")
    return fmt


def validate_duration_bounds(
    min_duration_sec: int | float | None,
    max_duration_sec: int | float | None,
    *,
    default_min: float = float(MIN_SOURCE_DURATION_SEC),
    default_max: float = float(MAX_SOURCE_DURATION_SEC),
) -> tuple[float, float]:
    lo = default_min if min_duration_sec is None else min_duration_sec
    hi = default_max if max_duration_sec is None else max_duration_sec
    if isinstance(lo, bool) or isinstance(hi, bool):
        raise SongFinderError("invalid_query", "duration bounds must be numeric")
    try:
        lo_f = float(lo)
        hi_f = float(hi)
    except (TypeError, ValueError):
        raise SongFinderError("invalid_query", "duration bounds must be numeric") from None
    if (
        not math.isfinite(lo_f)
        or not math.isfinite(hi_f)
        or lo_f < HARD_MIN_DURATION_SEC
        or hi_f > HARD_MAX_DURATION_SEC
        or lo_f >= hi_f
    ):
        raise SongFinderError("invalid_query", "duration bounds out of range")
    return lo_f, hi_f


def _strip_html(raw: Any) -> str:
    text = html.unescape(str(raw or ""))
    return " ".join(_HTML_TAG_RE.sub("", text).split()).strip()


def parse_duration_seconds(raw: Any) -> float | None:
    """Parse numeric seconds or 'mm:ss'/'hh:mm:ss' duration strings safely."""
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, int | float):
        val = float(raw)
        return val if math.isfinite(val) and val > 0 else None
    if isinstance(raw, str):
        cleaned = raw.strip()
        if not cleaned:
            return None
        if ":" in cleaned:
            parts = cleaned.split(":")
            if len(parts) not in (2, 3):
                return None
            try:
                nums = [int(p) for p in parts]
            except ValueError:
                return None
            if any(n < 0 for n in nums):
                return None
            if len(nums) == 2:
                return float(nums[0] * 60 + nums[1])
            return float(nums[0] * 3600 + nums[1] * 60 + nums[2])
        try:
            val = float(cleaned)
            return val if math.isfinite(val) and val > 0 else None
        except ValueError:
            return None
    return None


def is_clean_source_candidate(
    info: dict[str, Any],
    *,
    min_duration_sec: float = float(MIN_SOURCE_DURATION_SEC),
    max_duration_sec: float = float(MAX_SOURCE_DURATION_SEC),
) -> bool:
    """Filter candidate metadata to reject covers, live clips, DJ remixes, shorts, and medleys."""
    if not isinstance(info, dict):
        return False
    if info.get("is_live") or info.get("was_live"):
        return False
    age_limit = info.get("age_limit")
    if isinstance(age_limit, int | float) and not isinstance(age_limit, bool) and age_limit >= 18:
        return False
    dur = parse_duration_seconds(info.get("duration"))
    if dur is None or dur < min_duration_sec or dur > max_duration_sec:
        return False
    title = _strip_html(info.get("title") or "").lower()
    if not title:
        return False
    webpage_url = str(info.get("webpage_url") or info.get("url") or "").lower()
    if "/shorts/" in webpage_url:
        return False
    for bad in _CJK_NOISY_KEYWORDS:
        if bad in title:
            return False
    for pat in _ASCII_NOISY_PATTERNS:
        if pat.search(title):
            return False
    return True


def score_candidate(
    *,
    title: str,
    uploader: str,
    duration_sec: float,
    query: str,
    title_hint: str = "",
    artist: str = "",
) -> float:
    """Rank clean candidates preferring official/original studio audio matching query and artist."""
    score = 0.0
    title_low = title.lower()
    uploader_low = uploader.lower()
    query_low = query.strip().lower()
    hint_low = title_hint.strip().lower()
    artist_low = artist.strip().lower()

    if query_low and query_low in title_low:
        score += 3.0
    if hint_low and hint_low != query_low and hint_low in title_low:
        score += 2.0
    if artist_low:
        if artist_low in title_low:
            score += 2.0
        if artist_low in uploader_low:
            score += 2.5
    if uploader_low.endswith("- topic") or "vevo" in uploader_low:
        score += 3.0
    for marker in _POSITIVE_TITLE_MARKERS:
        if marker in title_low:
            score += 1.5
            break
    if 150.0 <= duration_sec <= 320.0:
        score += 0.5
    return score


def safe_audio_filename(title: str, audio_format: str) -> str:
    """Produce a safe inbox filename with validated extension."""
    fmt = validate_output_format(audio_format)
    stem = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", title).strip("_")
    stem = (stem or "source_track")[:60]
    return f"{stem}.{fmt}"


@dataclass
class _ProviderCircuitState:
    cooldown_until: float = 0.0
    consecutive_risk_hits: int = 0
    last_error: str = ""
    last_tripped_at: float = 0.0
    last_request_at: float = 0.0
    rate_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class ProviderCircuitBreaker:
    """Strictly isolated per-provider anti-bot cooldown and rate-limit tracker."""

    def __init__(
        self,
        *,
        base_cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        max_cooldown_seconds: float = MAX_COOLDOWN_SECONDS,
        min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.base_cooldown_seconds = max(1.0, float(base_cooldown_seconds))
        self.max_cooldown_seconds = max(self.base_cooldown_seconds, float(max_cooldown_seconds))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._clock = clock
        self._states: dict[str, _ProviderCircuitState] = {}

    def _state(self, provider: str) -> _ProviderCircuitState:
        state = self._states.get(provider)
        if state is None:
            state = _ProviderCircuitState()
            self._states[provider] = state
        return state

    def is_open(self, provider: str) -> bool:
        return self.remaining_cooldown(provider) > 0.0

    def remaining_cooldown(self, provider: str) -> float:
        state = self._states.get(provider)
        if state is None:
            return 0.0
        rem = state.cooldown_until - self._clock()
        return rem if rem > 0.0 else 0.0

    def trip(self, provider: str, reason: str) -> float:
        state = self._state(provider)
        now = self._clock()
        state.consecutive_risk_hits += 1
        duration = min(
            self.base_cooldown_seconds * (2 ** (state.consecutive_risk_hits - 1)),
            self.max_cooldown_seconds,
        )
        state.cooldown_until = now + duration
        state.last_tripped_at = now
        state.last_error = str(reason or "provider_risk_control")[:80]
        return duration

    def record_success(self, provider: str) -> None:
        state = self._state(provider)
        state.consecutive_risk_hits = 0
        state.last_error = ""
        state.cooldown_until = 0.0

    async def pace(self, provider: str) -> None:
        if self.min_interval_seconds <= 0.0:
            return
        state = self._state(provider)
        async with state.rate_lock:
            now = self._clock()
            wait_for = self.min_interval_seconds - (now - state.last_request_at)
            if wait_for > 0.0:
                await asyncio.sleep(wait_for)
            state.last_request_at = self._clock()

    def snapshot(self, providers: Sequence[str] = VALID_PROVIDERS) -> dict[str, dict[str, Any]]:
        now = self._clock()
        out: dict[str, dict[str, Any]] = {}
        for name in providers:
            state = self._states.get(name)
            if state is None:
                out[name] = {
                    "cooling_down": False,
                    "cooldown_remaining_sec": 0,
                    "consecutive_risk_hits": 0,
                    "last_error": "",
                }
            else:
                rem = max(0.0, state.cooldown_until - now)
                out[name] = {
                    "cooling_down": rem > 0.0,
                    "cooldown_remaining_sec": int(math.ceil(rem)),
                    "consecutive_risk_hits": state.consecutive_risk_hits,
                    "last_error": state.last_error,
                }
        return out


async def _run_exec(
    cmd: list[str],
    *,
    timeout_sec: float,
    error_code: str,
) -> tuple[int, bytes, bytes]:
    """Execute a subprocess using argv list only (never shell=True) with sanitized env."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_clean_subprocess_env(),
        )
    except OSError as exc:
        raise SongFinderError(error_code, f"exec failed: {type(exc).__name__}") from None

    try:
        async with asyncio.timeout(timeout_sec):
            stdout, stderr = await proc.communicate()
    except asyncio.CancelledError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        raise
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        raise SongFinderError(error_code, "subprocess timed out") from None

    return int(proc.returncode or 0), stdout, stderr


def _find_yt_download_match(work_dir: Path, vid: str) -> tuple[Path, int] | None:
    matches = [
        p
        for p in sorted(work_dir.glob(f"yt_{vid}.*"))
        if p.is_file() and not p.name.endswith((".part", ".ytdl"))
    ]
    if not matches:
        return None
    chosen = matches[0]
    return chosen, chosen.stat().st_size


def _read_file_if_bounded(path: Path, *, min_bytes: int, max_bytes: int) -> bytes | None:
    if not path.is_file():
        return None
    size = path.stat().st_size
    if size < min_bytes:
        raise SongFinderError("transcode_failed", "output too small")
    if size > max_bytes:
        raise SongFinderError("download_too_large", "transcoded output exceeds max_bytes")
    data = path.read_bytes()
    if len(data) > max_bytes:
        raise SongFinderError("download_too_large", "transcoded output exceeds max_bytes")
    return data


async def transcode_clean_audio(
    input_path: Path,
    work_dir: Path,
    *,
    output_format: str = "m4a",
    min_duration_sec: float = float(MIN_SOURCE_DURATION_SEC),
    max_duration_sec: float = float(MAX_SOURCE_DURATION_SEC),
    max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> tuple[bytes, float, str]:
    """Verify decoded duration and transcode into clean metadata-free audio within byte cap."""
    fmt = validate_output_format(output_format)
    if not await asyncio.to_thread(input_path.is_file):
        raise SongFinderError("transcode_failed", "downloaded input missing")

    probe_cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-protocol_whitelist",
        "file",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        "-i",
        str(input_path),
    ]
    rc_probe, out_probe, _ = await _run_exec(
        probe_cmd, timeout_sec=15.0, error_code="transcode_failed"
    )
    if rc_probe != 0:
        raise SongFinderError("transcode_failed", "ffprobe returned non-zero")
    try:
        probed_sec = float(out_probe.decode("utf-8", errors="ignore").strip())
    except ValueError:
        raise SongFinderError("transcode_failed", "unparseable ffprobe duration") from None
    if (
        not math.isfinite(probed_sec)
        or probed_sec < min_duration_sec
        or probed_sec > max_duration_sec
    ):
        raise SongFinderError("duration_out_of_range", f"duration={probed_sec}")

    out_path = work_dir / f"clean_output.{fmt}"
    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-protocol_whitelist",
        "file",
        "-i",
        str(input_path),
        "-vn",
        "-sn",
        "-dn",
        "-map_metadata",
        "-1",
    ]
    if fmt == "m4a":
        cmd.extend(["-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", str(out_path)])
    elif fmt == "mp3":
        cmd.extend(["-c:a", "libmp3lame", "-b:a", "192k", str(out_path)])
    else:
        cmd.extend(["-c:a", "flac", str(out_path)])

    rc_ff, _, _ = await _run_exec(cmd, timeout_sec=90.0, error_code="transcode_failed")
    if rc_ff != 0:
        raise SongFinderError("transcode_failed", "ffmpeg failed")

    # Release raw input from tmpfs immediately before buffering transcoded output into RAM.
    if input_path != out_path:
        await asyncio.to_thread(input_path.unlink, missing_ok=True)

    data = await asyncio.to_thread(
        _read_file_if_bounded,
        out_path,
        min_bytes=MIN_DOWNLOAD_BYTES,
        max_bytes=max_output_bytes,
    )
    if data is None:
        raise SongFinderError("transcode_failed", "ffmpeg failed")
    digest = hashlib.sha256(data).hexdigest()
    return data, probed_sec, digest


def _stderr_indicates_risk(stderr_text: str) -> bool:
    low = stderr_text.lower()
    return any(marker in low for marker in _RISK_STDERR_MARKERS)


class YouTubeProvider:
    """Standalone yt-dlp YouTube clean audio search & download provider."""

    provider_id = "youtube"

    def __init__(
        self,
        *,
        yt_dlp_bin: str = "",
        proxy_url: str | None = None,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        min_duration_sec: float = float(MIN_SOURCE_DURATION_SEC),
        max_duration_sec: float = float(MAX_SOURCE_DURATION_SEC),
        search_pool: int = 8,
    ) -> None:
        self.yt_dlp_bin = yt_dlp_bin.strip() or shutil.which("yt-dlp") or "yt-dlp"
        self.proxy_url = validate_proxy_url(proxy_url)
        self.max_download_bytes = max(MIN_OUTPUT_BYTES_LIMIT, int(max_download_bytes))
        self.min_duration_sec = float(min_duration_sec)
        self.max_duration_sec = float(max_duration_sec)
        self.search_pool = max(3, min(15, int(search_pool)))

    def _base_flags(self) -> list[str]:
        flags = [
            self.yt_dlp_bin,
            "--no-playlist",
            "--no-config",
            "--no-warnings",
            "--socket-timeout",
            "15",
        ]
        if self.proxy_url:
            flags.extend(["--proxy", self.proxy_url])
        return flags

    async def search(
        self,
        clean_query: str,
        *,
        title_hint: str = "",
        artist: str = "",
        limit: int = 5,
        min_duration_sec: float | None = None,
        max_duration_sec: float | None = None,
    ) -> list[SongCandidate]:
        min_d = self.min_duration_sec if min_duration_sec is None else float(min_duration_sec)
        max_d = self.max_duration_sec if max_duration_sec is None else float(max_duration_sec)
        search_expr = f"ytsearch{self.search_pool}:{clean_query}"
        cmd = [
            *self._base_flags(),
            "--flat-playlist",
            "--dump-json",
            "--skip-download",
            "--ignore-errors",
            "--",
            search_expr,
        ]
        rc, stdout, stderr = await _run_exec(cmd, timeout_sec=45.0, error_code="source_not_found")
        stderr_text = stderr.decode("utf-8", errors="ignore")
        candidates: list[SongCandidate] = []
        ssrf_blocked_hits = 0
        for raw_line in stdout.decode("utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line.startswith("{"):
                continue
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if not is_clean_source_candidate(
                item,
                min_duration_sec=min_d,
                max_duration_sec=max_d,
            ):
                continue
            raw_vid = str(item.get("id") or "").strip()
            vid_id = re.sub(r"[^A-Za-z0-9_-]", "", raw_vid)[:24] or "yt"
            raw_url = str(item.get("webpage_url") or item.get("url") or "").strip()
            if (not raw_url or "://" not in raw_url) and _YOUTUBE_ID_RE.fullmatch(raw_vid):
                raw_url = f"https://www.youtube.com/watch?v={raw_vid}"
            try:
                safe_url = validate_youtube_url(raw_url)
            except SongFinderError:
                ssrf_blocked_hits += 1
                continue
            title = _strip_html(item.get("title") or "")
            uploader = _strip_html(item.get("uploader") or item.get("channel") or "")
            dur = parse_duration_seconds(item.get("duration")) or 0.0
            candidates.append(
                SongCandidate(
                    provider=self.provider_id,
                    source_id=vid_id,
                    title=title,
                    uploader=uploader,
                    duration_sec=dur,
                    url=safe_url,
                    score=score_candidate(
                        title=title,
                        uploader=uploader,
                        duration_sec=dur,
                        query=clean_query,
                        title_hint=title_hint,
                        artist=artist,
                    ),
                )
            )

        if not candidates:
            if _stderr_indicates_risk(stderr_text):
                raise ProviderRiskControlError(self.provider_id, "youtube_risk_control")
            if ssrf_blocked_hits > 0:
                raise SongFinderError("ssrf_blocked", provider=self.provider_id)
            if rc != 0:
                return []
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates[: max(1, min(MAX_SEARCH_LIMIT, int(limit)))]

    async def download(
        self,
        candidate: SongCandidate,
        work_dir: Path,
        *,
        max_download_bytes: int | None = None,
    ) -> Path:
        safe_url = validate_youtube_url(candidate.url)
        cap = (
            min(self.max_download_bytes, int(max_download_bytes))
            if max_download_bytes is not None and int(max_download_bytes) > 0
            else self.max_download_bytes
        )
        vid = re.sub(r"[^A-Za-z0-9_-]", "", candidate.source_id)[:24] or "yt"
        out_template = work_dir / f"yt_{vid}.%(ext)s"
        cmd = [
            *self._base_flags(),
            "-f",
            "bestaudio[ext=m4a]/bestaudio",
            "--max-filesize",
            str(cap),
            "-o",
            str(out_template),
            "--",
            safe_url,
        ]
        rc, _, stderr = await _run_exec(cmd, timeout_sec=120.0, error_code="source_not_found")
        stderr_text = stderr.decode("utf-8", errors="ignore")
        if rc != 0:
            if _stderr_indicates_risk(stderr_text):
                raise ProviderRiskControlError(self.provider_id, "youtube_risk_control")
            if "file is larger than max-filesize" in stderr_text.lower():
                raise SongFinderError("download_too_large", provider=self.provider_id)
            raise SongFinderError("source_not_found", provider=self.provider_id)

        found_match = await asyncio.to_thread(_find_yt_download_match, work_dir, vid)
        if found_match is None:
            if "file is larger than max-filesize" in stderr_text.lower():
                raise SongFinderError("download_too_large", provider=self.provider_id)
            raise SongFinderError("source_not_found", provider=self.provider_id)
        chosen, size = found_match
        if size < MIN_DOWNLOAD_BYTES:
            raise SongFinderError("transcode_failed", provider=self.provider_id)
        if size > cap:
            raise SongFinderError("download_too_large", provider=self.provider_id)
        return chosen


class _AsyncBufferedFileWriter:
    """Thread-safe buffered file writer offloading disk I/O off the asyncio event loop."""

    def __init__(self, path: Path, *, flush_bytes: int = _DOWNLOAD_WRITE_BUFFER_BYTES) -> None:
        self._path = path
        self._flush_bytes = max(4096, int(flush_bytes))
        self._lock = threading.Lock()
        self._fh: Any = None
        self._buf = bytearray()
        self._closed = False

    def _flush_locked(self, data: bytes) -> None:
        with self._lock:
            if self._closed or not data:
                return
            if self._fh is None:
                self._fh = self._path.open("wb")
            self._fh.write(data)

    def _close_locked(self, *, unlink: bool) -> None:
        with self._lock:
            self._closed = True
            if self._fh is not None:
                try:
                    self._fh.close()
                finally:
                    self._fh = None
            if unlink:
                self._path.unlink(missing_ok=True)

    async def write(self, chunk: bytes) -> None:
        if not chunk:
            return
        self._buf.extend(chunk)
        if len(self._buf) >= self._flush_bytes:
            payload = bytes(self._buf)
            self._buf.clear()
            await asyncio.to_thread(self._flush_locked, payload)

    async def finish(self) -> None:
        if self._buf:
            payload = bytes(self._buf)
            self._buf.clear()
            await asyncio.to_thread(self._flush_locked, payload)
        await asyncio.to_thread(self._close_locked, unlink=False)

    async def abort(self) -> None:
        self._buf.clear()
        await asyncio.to_thread(self._close_locked, unlink=True)

    def abort_sync(self) -> None:
        self._buf.clear()
        self._close_locked(unlink=True)


class BilibiliPublicProvider:
    """Strictly anonymous, zero-cookie public audio search & DASH stream fetcher.

    Uses only public unauthenticated endpoints, an independent HTTP client, independent UA,
    and a strict CDN domain allowlist. Never touches any account credentials or patrol state.
    """

    provider_id = "bilibili_public"

    _SPI_URL = "https://api.bilibili.com/x/frontend/finger/spi"
    _NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
    _SEARCH_URL = "https://api.bilibili.com/x/web-interface/wbi/search/type"
    _VIEW_URL = "https://api.bilibili.com/x/web-interface/view"
    _PLAYURL_URL = "https://api.bilibili.com/x/player/wbi/playurl"
    _ANON_BUVID_TTL_SEC = 21600.0

    def __init__(
        self,
        *,
        http: httpx.AsyncClient | None = None,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        min_duration_sec: float = float(MIN_SOURCE_DURATION_SEC),
        max_duration_sec: float = float(MAX_SOURCE_DURATION_SEC),
        download_timeout_sec: float = DEFAULT_DOWNLOAD_TIMEOUT_SEC,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._owns_http = http is None
        self.http = (
            http
            if http is not None
            else httpx.AsyncClient(
                follow_redirects=False,
                trust_env=False,
                timeout=httpx.Timeout(20.0, connect=10.0),
                headers={"User-Agent": _PUBLIC_USER_AGENT},
            )
        )
        self.max_download_bytes = max(MIN_OUTPUT_BYTES_LIMIT, int(max_download_bytes))
        self.min_duration_sec = float(min_duration_sec)
        self.max_duration_sec = float(max_duration_sec)
        self.download_timeout_sec = max(0.05, float(download_timeout_sec))
        self._clock = clock
        self._wbi_key: str = ""
        self._wbi_expires_at: float = 0.0
        self._wbi_lock = asyncio.Lock()
        self._buvid_cookie: str = ""
        self._buvid_expires_at: float = 0.0
        self._buvid_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    def _public_headers(
        self,
        *,
        referer: str = "https://www.bilibili.com/",
        send_buvid: bool = True,
    ) -> dict[str, str]:
        headers = {
            "User-Agent": _PUBLIC_USER_AGENT,
            "Referer": referer,
            "Origin": "https://www.bilibili.com",
            "Accept": "application/json, text/plain, */*",
        }
        if send_buvid and self._buvid_cookie:
            headers["Cookie"] = self._buvid_cookie
        return headers

    async def _ensure_anon_buvid(self) -> str:
        """Fetch strictly anonymous public visitor `buvid3`/`buvid4` via `/x/frontend/finger/spi`.

        Zero account credentials or login cookies are ever read, stored, or forwarded.
        Fails gracefully if the endpoint is unreachable so tests/minimal mocks keep working.
        """
        now = self._clock()
        if self._buvid_cookie and now < self._buvid_expires_at:
            return self._buvid_cookie
        async with self._buvid_lock:
            now = self._clock()
            if self._buvid_cookie and now < self._buvid_expires_at:
                return self._buvid_cookie
            try:
                payload = await self._get_json(self._SPI_URL, send_buvid=False)
            except (SongFinderError, ProviderRiskControlError):
                return self._buvid_cookie
            data = payload.get("data") if isinstance(payload.get("data"), dict) else None
            if not isinstance(data, dict):
                return self._buvid_cookie
            b3 = str(data.get("b_3") or "").strip()
            b4 = str(data.get("b_4") or "").strip()
            if not b3 or not _ANON_BUVID_VAL_RE.fullmatch(b3):
                return self._buvid_cookie
            parts = [f"buvid3={b3}"]
            if b4 and _ANON_BUVID_VAL_RE.fullmatch(b4):
                parts.append(f"buvid4={b4}")
            self._buvid_cookie = "; ".join(parts)
            self._buvid_expires_at = now + self._ANON_BUVID_TTL_SEC
            return self._buvid_cookie

    async def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        referer: str = "https://www.bilibili.com/",
        send_buvid: bool = True,
    ) -> dict[str, Any]:
        if hasattr(self.http, "cookies"):
            self.http.cookies.clear()
        try:
            async with self.http.stream(
                "GET",
                url,
                params=params,
                headers=self._public_headers(referer=referer, send_buvid=send_buvid),
                follow_redirects=False,
            ) as resp:
                if resp.status_code in (403, 412, 429):
                    if resp.status_code == 412:
                        self._buvid_cookie = ""
                        self._buvid_expires_at = 0.0
                    raise ProviderRiskControlError(self.provider_id, f"http_{resp.status_code}")
                if 300 <= resp.status_code < 400:
                    raise SongFinderError("ssrf_blocked", provider=self.provider_id)
                if resp.status_code >= 400:
                    raise SongFinderError("source_not_found", provider=self.provider_id)
                buf = bytearray()
                async for chunk in resp.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > MAX_HTTP_JSON_BYTES:
                        raise SongFinderError("download_too_large", provider=self.provider_id)
        except SongFinderError:
            raise
        except httpx.HTTPError as exc:
            raise SongFinderError(
                "source_not_found",
                f"http error: {type(exc).__name__}",
                provider=self.provider_id,
            ) from None

        try:
            payload = json.loads(buf)
        except ValueError:
            raise SongFinderError("source_not_found", provider=self.provider_id) from None
        if not isinstance(payload, dict):
            raise SongFinderError("source_not_found", provider=self.provider_id)

        code = payload.get("code")
        if isinstance(code, int) and code in _BILI_RISK_API_CODES:
            raise ProviderRiskControlError(self.provider_id, f"api_code_{code}")
        return payload

    @staticmethod
    def _derive_mixin_key(img_url: str, sub_url: str) -> str:
        img_key = img_url.rsplit("/", 1)[-1].split(".", 1)[0]
        sub_key = sub_url.rsplit("/", 1)[-1].split(".", 1)[0]
        raw = img_key + sub_key
        if len(raw) < 64:
            return ""
        return "".join(raw[idx] for idx in _WBI_MIXIN_KEY_ENC_TAB)[:32]

    async def _ensure_wbi_key(self) -> str:
        now = self._clock()
        if self._wbi_key and now < self._wbi_expires_at:
            return self._wbi_key
        async with self._wbi_lock:
            now = self._clock()
            if self._wbi_key and now < self._wbi_expires_at:
                return self._wbi_key
            data = await self._get_json(self._NAV_URL)
            wbi_img = (
                (data.get("data") or {}).get("wbi_img")
                if isinstance(data.get("data"), dict)
                else None
            )
            if not isinstance(wbi_img, dict):
                raise SongFinderError("source_not_found", provider=self.provider_id)
            mixin = self._derive_mixin_key(
                str(wbi_img.get("img_url") or ""),
                str(wbi_img.get("sub_url") or ""),
            )
            if len(mixin) != 32:
                raise SongFinderError("source_not_found", provider=self.provider_id)
            self._wbi_key = mixin
            self._wbi_expires_at = now + 1800.0
            return mixin

    def _sign_wbi(self, params: dict[str, Any], mixin_key: str) -> dict[str, Any]:
        signed: dict[str, str] = {}
        for k, v in sorted({**params, "wts": int(self._clock())}.items()):
            cleaned_val = "".join(ch for ch in str(v) if ch not in _WBI_FILTER_CHARS)
            signed[str(k)] = cleaned_val
        query_str = urlencode(signed, quote_via=quote)
        w_rid = hashlib.md5(f"{query_str}{mixin_key}".encode()).hexdigest()
        signed["w_rid"] = w_rid
        return signed

    async def search(
        self,
        clean_query: str,
        *,
        title_hint: str = "",
        artist: str = "",
        limit: int = 5,
        min_duration_sec: float | None = None,
        max_duration_sec: float | None = None,
    ) -> list[SongCandidate]:
        min_d = self.min_duration_sec if min_duration_sec is None else float(min_duration_sec)
        max_d = self.max_duration_sec if max_duration_sec is None else float(max_duration_sec)
        await self._ensure_anon_buvid()
        mixin_key = await self._ensure_wbi_key()
        params = self._sign_wbi(
            {
                "search_type": "video",
                "keyword": clean_query,
                "order": "totalrank",
                "page": 1,
                "page_size": 12,
            },
            mixin_key,
        )
        payload = await self._get_json(
            self._SEARCH_URL,
            params=params,
            referer="https://search.bilibili.com/",
        )
        if payload.get("code") != 0:
            raise SongFinderError("source_not_found", provider=self.provider_id)

        rows = (
            (payload.get("data") or {}).get("result")
            if isinstance(payload.get("data"), dict)
            else None
        )
        if not isinstance(rows, list):
            return []

        candidates: list[SongCandidate] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            bvid = str(row.get("bvid") or "").strip()
            if not _BVID_RE.fullmatch(bvid):
                continue
            title = _strip_html(row.get("title") or "")
            uploader = _strip_html(row.get("author") or "")
            dur = parse_duration_seconds(row.get("duration"))
            if dur is None:
                continue
            candidate_info = {
                "title": title,
                "duration": dur,
                "webpage_url": f"https://www.bilibili.com/video/{bvid}",
            }
            if not is_clean_source_candidate(
                candidate_info,
                min_duration_sec=min_d,
                max_duration_sec=max_d,
            ):
                continue
            candidates.append(
                SongCandidate(
                    provider=self.provider_id,
                    source_id=bvid,
                    title=title,
                    uploader=uploader,
                    duration_sec=dur,
                    url=f"https://www.bilibili.com/video/{bvid}",
                    score=score_candidate(
                        title=title,
                        uploader=uploader,
                        duration_sec=dur,
                        query=clean_query,
                        title_hint=title_hint,
                        artist=artist,
                    ),
                )
            )
        candidates.sort(key=lambda c: c.score, reverse=True)
        return candidates[: max(1, min(MAX_SEARCH_LIMIT, int(limit)))]

    async def _resolve_audio_urls(
        self,
        bvid: str,
        *,
        min_duration_sec: float | None = None,
        max_duration_sec: float | None = None,
    ) -> list[str]:
        if not _BVID_RE.fullmatch(bvid):
            raise SongFinderError("invalid_query", provider=self.provider_id)
        min_d = self.min_duration_sec if min_duration_sec is None else float(min_duration_sec)
        max_d = self.max_duration_sec if max_duration_sec is None else float(max_duration_sec)
        await self._ensure_anon_buvid()
        referer = f"https://www.bilibili.com/video/{bvid}"
        view_data = await self._get_json(self._VIEW_URL, params={"bvid": bvid}, referer=referer)
        if view_data.get("code") != 0 or not isinstance(view_data.get("data"), dict):
            raise SongFinderError("source_not_found", provider=self.provider_id)
        vinfo = view_data["data"]
        cid = vinfo.get("cid")
        if not isinstance(cid, int) or isinstance(cid, bool) or cid <= 0:
            pages = vinfo.get("pages")
            if isinstance(pages, list) and pages and isinstance(pages[0], dict):
                cid = pages[0].get("cid")
        if not isinstance(cid, int) or isinstance(cid, bool) or cid <= 0:
            raise SongFinderError("source_not_found", provider=self.provider_id)

        dur = parse_duration_seconds(vinfo.get("duration"))
        if dur is not None and (dur < min_d or dur > max_d):
            raise SongFinderError("duration_out_of_range", provider=self.provider_id)

        mixin_key = await self._ensure_wbi_key()
        play_params = self._sign_wbi(
            {"bvid": bvid, "cid": cid, "fnval": 16, "fnver": 0, "fourk": 0},
            mixin_key,
        )
        play_data = await self._get_json(self._PLAYURL_URL, params=play_params, referer=referer)
        if play_data.get("code") != 0 or not isinstance(play_data.get("data"), dict):
            raise SongFinderError("source_not_found", provider=self.provider_id)

        dash = play_data["data"].get("dash")
        audio_list = dash.get("audio") if isinstance(dash, dict) else None
        if not isinstance(audio_list, list) or not audio_list:
            raise SongFinderError("source_not_found", provider=self.provider_id)

        ordered_streams = sorted(
            (s for s in audio_list if isinstance(s, dict)),
            key=lambda s: int(s.get("bandwidth") or 0),
            reverse=True,
        )
        urls: list[str] = []
        for stream in ordered_streams[:3]:
            for key in ("baseUrl", "base_url"):
                u = stream.get(key)
                if isinstance(u, str) and u.strip():
                    urls.append(u.strip())
            for key in ("backupUrl", "backup_url"):
                bu = stream.get(key)
                if isinstance(bu, list):
                    for u in bu:
                        if isinstance(u, str) and u.strip():
                            urls.append(u.strip())

        if not urls:
            raise SongFinderError("source_not_found", provider=self.provider_id)

        validated: list[str] = []
        seen: set[str] = set()
        for raw_url in urls:
            try:
                safe = validate_bili_cdn_url(raw_url)
            except SongFinderError:
                continue
            if safe not in seen:
                seen.add(safe)
                validated.append(safe)
        if not validated:
            raise SongFinderError("ssrf_blocked", provider=self.provider_id)
        return validated

    async def download(
        self,
        candidate: SongCandidate,
        work_dir: Path,
        *,
        max_download_bytes: int | None = None,
        min_duration_sec: float | None = None,
        max_duration_sec: float | None = None,
        download_timeout_sec: float | None = None,
    ) -> Path:
        cap = (
            min(self.max_download_bytes, int(max_download_bytes))
            if max_download_bytes is not None and int(max_download_bytes) > 0
            else self.max_download_bytes
        )
        timeout_sec = (
            self.download_timeout_sec
            if download_timeout_sec is None
            else max(0.05, float(download_timeout_sec))
        )
        bvid = candidate.source_id
        urls = await self._resolve_audio_urls(
            bvid,
            min_duration_sec=min_duration_sec,
            max_duration_sec=max_duration_sec,
        )
        referer = f"https://www.bilibili.com/video/{bvid}"
        out_path = work_dir / f"bili_{bvid}.m4s"
        risk_hits = 0
        redirect_hits = 0

        try:
            async with asyncio.timeout(timeout_sec):
                for cdn_url in urls:
                    await asyncio.to_thread(out_path.unlink, missing_ok=True)
                    if hasattr(self.http, "cookies"):
                        self.http.cookies.clear()
                    headers = {
                        "User-Agent": _PUBLIC_USER_AGENT,
                        "Referer": referer,
                        "Origin": "https://www.bilibili.com",
                        "Range": "bytes=0-",
                    }
                    writer = _AsyncBufferedFileWriter(out_path)
                    try:
                        async with self.http.stream(
                            "GET", cdn_url, headers=headers, follow_redirects=False
                        ) as resp:
                            if resp.status_code in (403, 412, 429):
                                risk_hits += 1
                                continue
                            if 300 <= resp.status_code < 400:
                                redirect_hits += 1
                                continue
                            if resp.status_code not in (200, 206):
                                continue
                            cl_raw = resp.headers.get("content-length")
                            if cl_raw:
                                try:
                                    if int(cl_raw) > cap:
                                        raise SongFinderError(
                                            "download_too_large", provider=self.provider_id
                                        )
                                except ValueError:
                                    pass
                            total = 0
                            async for chunk in resp.aiter_bytes(chunk_size=65536):
                                total += len(chunk)
                                if total > cap:
                                    raise SongFinderError(
                                        "download_too_large", provider=self.provider_id
                                    )
                                await writer.write(chunk)
                            await writer.finish()
                            if total >= MIN_DOWNLOAD_BYTES:
                                return out_path
                            await asyncio.to_thread(out_path.unlink, missing_ok=True)
                    except SongFinderError as exc:
                        await writer.abort()
                        if exc.code == "download_too_large":
                            raise
                        continue
                    except httpx.HTTPError:
                        await writer.abort()
                        continue
                    except asyncio.CancelledError:
                        writer.abort_sync()
                        raise
                    except BaseException:
                        writer.abort_sync()
                        raise
        except TimeoutError:
            await asyncio.to_thread(out_path.unlink, missing_ok=True)
            raise SongFinderError(
                "source_not_found", "download timed out", provider=self.provider_id
            ) from None

        await asyncio.to_thread(out_path.unlink, missing_ok=True)
        if risk_hits == len(urls) and risk_hits > 0:
            raise ProviderRiskControlError(self.provider_id, "cdn_risk_control")
        if redirect_hits == len(urls) and redirect_hits > 0:
            raise SongFinderError("ssrf_blocked", provider=self.provider_id)
        raise SongFinderError("source_not_found", provider=self.provider_id)


class SongFinderEngine:
    """Multi-provider music source resolver with isolated per-provider risk-control failover."""

    def __init__(
        self,
        *,
        youtube: YouTubeProvider | None = None,
        bilibili_public: BilibiliPublicProvider | None = None,
        provider_order: str | Sequence[str] | None = None,
        circuit_breaker: ProviderCircuitBreaker | None = None,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
        min_duration_sec: float = float(MIN_SOURCE_DURATION_SEC),
        max_duration_sec: float = float(MAX_SOURCE_DURATION_SEC),
        ffmpeg_bin: str = "ffmpeg",
        ffprobe_bin: str = "ffprobe",
    ) -> None:
        self.max_download_bytes = max(MIN_OUTPUT_BYTES_LIMIT, int(max_download_bytes))
        self.max_output_bytes = max(MIN_OUTPUT_BYTES_LIMIT, int(max_output_bytes))
        self.min_duration_sec = float(min_duration_sec)
        self.max_duration_sec = float(max_duration_sec)
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.provider_order = parse_provider_order(provider_order)
        self.circuit_breaker = circuit_breaker or ProviderCircuitBreaker()
        self.providers: dict[str, Any] = {
            "youtube": youtube
            if youtube is not None
            else YouTubeProvider(
                max_download_bytes=self.max_download_bytes,
                min_duration_sec=self.min_duration_sec,
                max_duration_sec=self.max_duration_sec,
            ),
            "bilibili_public": bilibili_public
            if bilibili_public is not None
            else BilibiliPublicProvider(
                max_download_bytes=self.max_download_bytes,
                min_duration_sec=self.min_duration_sec,
                max_duration_sec=self.max_duration_sec,
            ),
        }

    async def close(self) -> None:
        bili = self.providers.get("bilibili_public")
        if bili is not None and hasattr(bili, "close"):
            await bili.close()

    def _resolve_chain(self, strategy: str | None) -> tuple[str, ...]:
        mode = str(strategy or "auto").strip().lower() or "auto"
        if mode == "auto":
            return self.provider_order
        if mode in self.providers:
            return (mode,)
        raise SongFinderError("invalid_query", f"unknown strategy: {mode}")

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "provider_order": list(self.provider_order),
            "providers": self.circuit_breaker.snapshot(VALID_PROVIDERS),
            "max_download_bytes": self.max_download_bytes,
            "max_output_bytes": self.max_output_bytes,
        }

    async def search(
        self,
        query: str,
        *,
        title: str = "",
        artist: str = "",
        strategy: str = "auto",
        limit: int = 5,
    ) -> dict[str, Any]:
        clean_query = sanitize_search_query(query, title=title, artist=artist)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not (1 <= limit <= MAX_SEARCH_LIMIT)
        ):
            raise SongFinderError("invalid_query", "limit must be between 1 and 10")
        chain = self._resolve_chain(strategy)
        skipped: dict[str, str] = {}
        all_candidates: list[SongCandidate] = []
        provider_used = ""

        for prov_name in chain:
            if self.circuit_breaker.is_open(prov_name):
                rem = int(math.ceil(self.circuit_breaker.remaining_cooldown(prov_name)))
                skipped[prov_name] = f"cooldown:{rem}s"
                continue
            prov = self.providers[prov_name]
            await self.circuit_breaker.pace(prov_name)
            try:
                found = await prov.search(
                    clean_query,
                    title_hint=title,
                    artist=artist,
                    limit=limit,
                )
            except ProviderRiskControlError as exc:
                self.circuit_breaker.trip(prov_name, exc.code)
                skipped[prov_name] = f"risk_control:{exc.code}"
                continue
            except SongFinderError as exc:
                skipped[prov_name] = exc.code
                continue

            self.circuit_breaker.record_success(prov_name)
            if not found:
                skipped[prov_name] = "no_clean_candidates"
                continue
            if not provider_used:
                provider_used = prov_name
            all_candidates.extend(found)
            if strategy == "auto":
                break

        all_candidates.sort(key=lambda c: c.score, reverse=True)
        return {
            "candidates": [c.to_public_dict() for c in all_candidates[:limit]],
            "provider_used": provider_used,
            "skipped_providers": skipped,
        }

    async def fetch_source(
        self,
        query: str = "",
        *,
        title: str = "",
        artist: str = "",
        candidate_id: str = "",
        strategy: str = "auto",
        preferred_format: str = "m4a",
        max_bytes: int | None = None,
        min_duration_sec: int | float | None = None,
        max_duration_sec: int | float | None = None,
    ) -> FetchedSongSource:
        clean_candidate = str(candidate_id or "").strip()
        if clean_candidate:
            clean_query = _validate_query_fragment(query, allow_empty=True)
            clean_title = _validate_query_fragment(title, allow_empty=True)
        else:
            clean_query = sanitize_search_query(query, title=title, artist=artist)
            clean_title = _validate_query_fragment(title, allow_empty=True)
        fmt = validate_output_format(preferred_format)
        min_d, max_d = validate_duration_bounds(
            min_duration_sec,
            max_duration_sec,
            default_min=self.min_duration_sec,
            default_max=self.max_duration_sec,
        )
        if max_bytes is not None:
            if isinstance(max_bytes, bool) or not isinstance(max_bytes, int):
                raise SongFinderError("invalid_query", "max_bytes must be an integer")
            if max_bytes < MIN_OUTPUT_BYTES_LIMIT or max_bytes > self.max_output_bytes:
                raise SongFinderError("invalid_query", "max_bytes out of range")
            cap = max_bytes
        else:
            cap = self.max_output_bytes

        if clean_candidate:
            prov_name, source_id = parse_candidate_id(clean_candidate)
            chain = self._resolve_chain(strategy)
            if prov_name not in chain:
                raise SongFinderError("invalid_query", "candidate_id does not match strategy")
            if self.circuit_breaker.is_open(prov_name):
                raise SongFinderError("all_providers_in_cooldown", provider=prov_name)
            prov = self.providers[prov_name]
            await self.circuit_breaker.pace(prov_name)
            url = (
                f"https://www.bilibili.com/video/{source_id}"
                if prov_name == "bilibili_public"
                else f"https://www.youtube.com/watch?v={source_id}"
            )
            cand = SongCandidate(
                provider=prov_name,
                source_id=source_id,
                title=clean_title or clean_query or source_id,
                uploader="",
                duration_sec=0.0,
                url=url,
            )
            with tempfile.TemporaryDirectory(prefix="hakimi-sf-") as tmpdir:
                work_dir = Path(tmpdir)
                try:
                    dl_kwargs: dict[str, Any] = {"max_download_bytes": self.max_download_bytes}
                    if prov_name == "bilibili_public":
                        dl_kwargs["min_duration_sec"] = min_d
                        dl_kwargs["max_duration_sec"] = max_d
                    raw_path = await prov.download(cand, work_dir, **dl_kwargs)
                    audio_bytes, probed_dur, digest = await transcode_clean_audio(
                        raw_path,
                        work_dir,
                        output_format=fmt,
                        min_duration_sec=min_d,
                        max_duration_sec=max_d,
                        max_output_bytes=cap,
                        ffmpeg_bin=self.ffmpeg_bin,
                        ffprobe_bin=self.ffprobe_bin,
                    )
                except ProviderRiskControlError as exc:
                    self.circuit_breaker.trip(prov_name, exc.code)
                    raise SongFinderError("all_providers_in_cooldown", provider=prov_name) from None
            self.circuit_breaker.record_success(prov_name)
            return FetchedSongSource(
                provider=prov_name,
                source_id=source_id,
                title=cand.title,
                uploader=cand.uploader,
                duration_sec=probed_dur,
                audio_format=fmt,
                mime_type=MIME_BY_FORMAT[fmt],
                filename=safe_audio_filename(clean_title or clean_query or source_id, fmt),
                data=audio_bytes,
                sha256=digest,
                byte_size=len(audio_bytes),
                skipped_providers={},
            )

        chain = self._resolve_chain(strategy)
        skipped: dict[str, str] = {}
        cooldown_providers = 0
        specific_errors: list[str] = []

        for prov_name in chain:
            if self.circuit_breaker.is_open(prov_name):
                rem = int(math.ceil(self.circuit_breaker.remaining_cooldown(prov_name)))
                skipped[prov_name] = f"cooldown:{rem}s"
                cooldown_providers += 1
                continue
            prov = self.providers[prov_name]
            await self.circuit_breaker.pace(prov_name)
            try:
                candidates = await prov.search(
                    clean_query,
                    title_hint=title,
                    artist=artist,
                    limit=3,
                    min_duration_sec=min_d,
                    max_duration_sec=max_d,
                )
            except ProviderRiskControlError as exc:
                self.circuit_breaker.trip(prov_name, exc.code)
                skipped[prov_name] = f"risk_control:{exc.code}"
                cooldown_providers += 1
                continue
            except SongFinderError as exc:
                skipped[prov_name] = exc.code
                if exc.code in _PROPAGATABLE_FETCH_ERRORS:
                    specific_errors.append(exc.code)
                continue

            if not candidates:
                skipped[prov_name] = "no_clean_candidates"
                continue

            tripped = False
            last_dl_err = "source_not_found"
            for cand in candidates[:3]:
                with tempfile.TemporaryDirectory(prefix="hakimi-sf-") as tmpdir:
                    work_dir = Path(tmpdir)
                    try:
                        dl_kwargs: dict[str, Any] = {"max_download_bytes": self.max_download_bytes}
                        if prov_name == "bilibili_public":
                            dl_kwargs["min_duration_sec"] = min_d
                            dl_kwargs["max_duration_sec"] = max_d
                        raw_path = await prov.download(cand, work_dir, **dl_kwargs)
                        audio_bytes, probed_dur, digest = await transcode_clean_audio(
                            raw_path,
                            work_dir,
                            output_format=fmt,
                            min_duration_sec=min_d,
                            max_duration_sec=max_d,
                            max_output_bytes=cap,
                            ffmpeg_bin=self.ffmpeg_bin,
                            ffprobe_bin=self.ffprobe_bin,
                        )
                    except ProviderRiskControlError as exc:
                        self.circuit_breaker.trip(prov_name, exc.code)
                        skipped[prov_name] = f"risk_control:{exc.code}"
                        cooldown_providers += 1
                        tripped = True
                        break
                    except SongFinderError as exc:
                        last_dl_err = exc.code
                        continue

                self.circuit_breaker.record_success(prov_name)
                return FetchedSongSource(
                    provider=prov_name,
                    source_id=cand.source_id,
                    title=cand.title,
                    uploader=cand.uploader,
                    duration_sec=probed_dur,
                    audio_format=fmt,
                    mime_type=MIME_BY_FORMAT[fmt],
                    filename=safe_audio_filename(title or cand.title or query, fmt),
                    data=audio_bytes,
                    sha256=digest,
                    byte_size=len(audio_bytes),
                    skipped_providers=skipped,
                )

            if not tripped:
                skipped[prov_name] = last_dl_err
                if last_dl_err in _PROPAGATABLE_FETCH_ERRORS:
                    specific_errors.append(last_dl_err)

        if cooldown_providers == len(chain) and cooldown_providers > 0:
            raise SongFinderError("all_providers_in_cooldown")
        if specific_errors and all(
            reason in _PROPAGATABLE_FETCH_ERRORS
            or reason.startswith(("cooldown:", "risk_control:"))
            for reason in skipped.values()
        ):
            raise SongFinderError(specific_errors[-1])
        raise SongFinderError("source_not_found")
