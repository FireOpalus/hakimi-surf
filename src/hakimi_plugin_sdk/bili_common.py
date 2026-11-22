"""Shared Bilibili constants, URL/BV parsers, WBI helpers, and Pydantic models.

Single source of truth imported by:
- ``hakimi`` kernel modules (``bili_bridge``, ``video_downloader``,
  ``rich_message_parser``, ``gemini``, ``admin_session_routes``, ``bili_account_admin``)
- built-in ``hakimi.modules.platforms.bilibili`` submodule
- standalone ``plugins/bilibili`` remote plugin container

Must remain free of any ``hakimi.*`` imports (stdlib + ``pydantic`` only) so it
works cleanly inside the minimal ``plugins/Dockerfile`` image.
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Identifiers, URLs, API origins & allowlists
# ---------------------------------------------------------------------------

BVID = r"BV[1-9A-HJ-NP-Za-km-z]{10}"
BVID_RE = re.compile(rf"^{BVID}$")
BV_RE = re.compile(rf"(?<![A-Za-z0-9])({BVID})(?![A-Za-z0-9])")
MID_RE = re.compile(r"^[1-9][0-9]{0,19}$")
URL_RE = re.compile(r"https?://[^\s<>\"'，。！？）]+", re.IGNORECASE)
FESTIVAL_PATH_RE = re.compile(r"^/festival(?:/[A-Za-z0-9_-]{1,80})?/?$")
COVER_URL_RE = re.compile(
    r"^https://(?:i[0-2]\.hdslb\.com|archive\.biliimg\.com)/bfs/[A-Za-z0-9_./-]+$"
)
MAX_COVER_THUMB_BYTES = 98_304

CREDENTIAL_KEYS: tuple[str, ...] = ("sessdata", "bili_jct", "buvid3", "buvid4")
COOKIE_NAMES: dict[str, str] = {
    "sessdata": "SESSDATA",
    "bili_jct": "bili_jct",
    "buvid3": "buvid3",
    "buvid4": "buvid4",
}

CANONICAL_VIDEO_HOSTS: frozenset[str] = frozenset(
    {"www.bilibili.com", "bilibili.com", "m.bilibili.com"}
)
SHORT_LINK_HOSTS: frozenset[str] = frozenset({"b23.tv", "bili2233.cn"})
SUBTITLE_HOSTS: frozenset[str] = frozenset(
    {"aisubtitle.hdslb.com", "i0.hdslb.com", "i1.hdslb.com", "i2.hdslb.com"}
)

API_ORIGIN = "https://api.bilibili.com"
LIVE_API_ORIGIN = "https://api.live.bilibili.com"
SPI_PATH = "/x/frontend/finger/spi"

USER_AGENT = "HakimiSurf/0.4 text patrol"
PUBLIC_USER_AGENT = "HakimiSurf/0.1 (public metadata reader)"

GET_PATHS: frozenset[str] = frozenset(
    {
        "/x/web-interface/nav",
        "/x/web-interface/wbi/index/top/feed/rcmd",
        "/x/polymer/web-dynamic/v1/feed/all",
        "/x/web-interface/view",
        "/x/player/wbi/v2",
        "/x/v2/reply",
        "/x/web-interface/archive/relation",
        "/x/v3/fav/folder/created/list-all",
    }
)
POST_PATHS: frozenset[str] = frozenset({"/x/web-interface/archive/like", "/x/v3/fav/resource/deal"})
PUBLIC_READ_PATHS: frozenset[str] = frozenset(
    {"/x/web-interface/nav", "/x/web-interface/view", "/x/player/wbi/v2", "/x/v2/reply"}
)

PRIVATE_SHARE_PRIORITY_SECONDS = 24 * 3600
RISK_COOLDOWN_SECONDS = 43200.0
RISK_CODES: frozenset[str] = frozenset({"bili_account_risk", "bili_cookie_expired"})
PUBLIC_DEGRADE_ERRORS: frozenset[str] = frozenset(
    {
        "bili_account_unavailable",
        "bili_account_upstream_error",
        "bili_video_schema_changed",
        "bili_video_mismatch",
        "bili_wbi_unavailable",
        "bili_subtitle_host_forbidden",
        "redirect_not_allowed",
        "upstream_body_too_large",
    }
)

MIXIN_KEY_ENC_TAB: tuple[int, ...] = (
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


# ---------------------------------------------------------------------------
# Pure text, URL, cookie, WBI & subtitle helpers
# ---------------------------------------------------------------------------


def clean_bili_text(text: Any) -> str:
    """Normalize NFKC text while stripping control characters except newline/tab."""
    return "".join(
        c
        for c in unicodedata.normalize("NFKC", str(text or ""))
        if c in "\n\t" or not unicodedata.category(c).startswith("C")
    ).strip()


def extract_url_video_ref(raw_url: str) -> tuple[str, int] | None:
    """Extract ``(bvid, page)`` from one canonical Bilibili video or festival URL."""
    if not isinstance(raw_url, str) or not raw_url:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in raw_url):
        return None
    try:
        u = urlsplit(raw_url)
        port = u.port
    except ValueError:
        return None
    host = (u.hostname or "").lower()
    if (
        u.scheme not in {"https", "http"}
        or host not in CANONICAL_VIDEO_HOSTS
        or u.username is not None
        or u.password is not None
        or (u.scheme == "https" and port not in {None, 443})
        or (u.scheme == "http" and port not in {None, 80})
        or ".." in u.path
    ):
        return None

    qs = parse_qs(u.query, keep_blank_values=False)
    bvid: str | None = None
    match = re.fullmatch(rf"/video/({BVID})/?", u.path)
    if match:
        bvid = match[1]
    elif FESTIVAL_PATH_RE.fullmatch(u.path):
        raw_bvids = qs.get("bvid")
        if raw_bvids and BVID_RE.fullmatch(raw_bvids[0]):
            bvid = raw_bvids[0]
    if not bvid:
        return None

    page = 1
    raw_pages = qs.get("p")
    if raw_pages:
        raw_p = raw_pages[0].strip()
        if re.fullmatch(r"[1-9][0-9]{0,2}", raw_p):
            parsed_page = int(raw_p)
            if 1 <= parsed_page <= 999:
                page = parsed_page
    return bvid, page


def extract_url_bvid(raw_url: str) -> str | None:
    """Extract a canonical BV ID from one Bilibili video or festival URL."""
    ref = extract_url_video_ref(raw_url)
    return ref[0] if ref is not None else None


def extract_video_ref(text: str) -> tuple[str, int] | None:
    """Extract ``(bvid, page)`` from canonical Bilibili URLs or bare BV text."""
    if not isinstance(text, str) or not text:
        return None
    for raw in URL_RE.findall(text):
        ref = extract_url_video_ref(raw)
        if ref is not None:
            return ref
    bare = BV_RE.findall(URL_RE.sub(" ", text))
    if bare:
        return bare[0], 1
    return None


def extract_bvid(text: str) -> str | None:
    """Extract a canonical Bilibili BV ID from canonical Bilibili URLs or bare text."""
    ref = extract_video_ref(text)
    return ref[0] if ref is not None else None


def validate_short_link(url: str) -> str | None:
    """Validate strict HTTPS short links (``b23.tv``, ``bili2233.cn``) and return host."""
    if not isinstance(url, str) or not url:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in url):
        return None
    try:
        u = urlsplit(url)
        port = u.port
    except ValueError:
        return None
    host = (u.hostname or "").lower()
    if (
        u.scheme != "https"
        or host not in SHORT_LINK_HOSTS
        or u.username is not None
        or u.password is not None
        or port not in {None, 443}
        or u.query
        or u.fragment
        or not re.fullmatch(r"/[A-Za-z0-9]{1,32}", u.path)
    ):
        return None
    return host


def validate_subtitle_url(url: str, *, error_cls: type[Exception] = ValueError) -> str:
    """Validate an HTTPS Bilibili subtitle URL against the strict host/path allowlist."""
    if (
        not isinstance(url, str)
        or len(url) > 2048
        or any(ord(c) < 32 or ord(c) == 127 for c in url)
    ):
        raise error_cls("bili_subtitle_host_forbidden")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise error_cls("bili_subtitle_host_forbidden") from None
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or host not in SUBTITLE_HOSTS
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.path.startswith("/bfs/")
        or ".." in parsed.path.split("/")
    ):
        raise error_cls("bili_subtitle_host_forbidden")
    return url


def validate_cookie(value: Any) -> str:
    """Reject raw Cookie headers, whitespace, or control bytes."""
    if (
        not isinstance(value, str)
        or len(value) > 4096
        or any(ord(c) < 33 or ord(c) > 126 or c in ";\r\n" for c in value)
    ):
        raise ValueError("Paste only the cookie value, not the complete Cookie header")
    return value


def wbi_keys(
    data: Any,
    *,
    error_cls: type[Exception] = ValueError,
) -> tuple[str, str]:
    """Extract 32-hex WBI image/sub keys from ``/x/web-interface/nav`` response data."""
    wbi_img = data.get("wbi_img") if isinstance(data, dict) else None
    if not isinstance(wbi_img, dict):
        raise error_cls("bili_wbi_unavailable")
    keys: list[str] = []
    for name in ("img_url", "sub_url"):
        url = str(wbi_img.get(name, "") or "")
        try:
            path = urlsplit(url).path
        except ValueError:
            raise error_cls("bili_wbi_unavailable") from None
        key = path.rsplit("/", 1)[-1].split(".")[0]
        if not re.fullmatch(r"[a-fA-F0-9]{32}", key):
            raise error_cls("bili_wbi_unavailable")
        keys.append(key)
    return (keys[0], keys[1])


def wbi_sign(
    params: dict[str, Any],
    keys: tuple[str, str] | list[str] | None,
    now: float,
    *,
    error_cls: type[Exception] = ValueError,
) -> dict[str, str]:
    """Sign Bilibili WBI query parameters using current ``img_key`` + ``sub_key``."""
    if not keys or not all(isinstance(k, str) for k in keys):
        raise error_cls("bili_wbi_unavailable")
    raw = "".join(keys)
    if not re.fullmatch(r"[a-fA-F0-9]{64}", raw):
        raise error_cls("bili_wbi_unavailable")
    mixin = "".join(raw[index] for index in MIXIN_KEY_ENC_TAB)[:32]
    values: dict[str, Any] = {**params, "wts": str(int(now))}
    cleaned: dict[str, str] = {k: re.sub(r"[!'()*]", "", str(v)) for k, v in sorted(values.items())}
    cleaned["w_rid"] = hashlib.md5(
        (urlencode(cleaned, quote_via=quote) + mixin).encode()
    ).hexdigest()
    return cleaned


def has_voucher(value: Any) -> bool:
    """Recursively detect Bilibili ``v_voucher`` risk-challenge payloads."""
    if isinstance(value, dict):
        return bool(value.get("v_voucher")) or any(has_voucher(v) for v in value.values())
    if isinstance(value, list):
        return any(has_voucher(v) for v in value)
    return False


def video_duration(value: Any) -> float | None:
    """Return finite positive video duration in seconds, or None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return float(value) if math.isfinite(value) and value > 0 else None
    except OverflowError:
        return None


def subtitle_timestamp(value: Any) -> float | None:
    """Return finite non-negative subtitle timestamp in seconds, or None."""
    if value == 0 and not isinstance(value, bool):
        return 0.0
    return video_duration(value)


def subtitle_text(body: Any, duration: float | None) -> tuple[str, bool, str]:
    """Bound subtitle text without describing clipped or untimed transcripts as complete."""
    lines = body.get("body") if isinstance(body, dict) else None
    if not isinstance(lines, list) or not lines:
        return "", False, "unavailable"
    parts: list[str] = []
    starts: list[float] = []
    ends: list[float] = []
    valid = True
    for line in lines[:1000]:
        if not isinstance(line, dict) or not isinstance(line.get("content"), str):
            valid = False
            continue
        text = clean_bili_text(line["content"])
        if text:
            parts.append(text)
        start, end = subtitle_timestamp(line.get("from")), subtitle_timestamp(line.get("to"))
        if (
            start is None
            or end is None
            or end <= start
            or (duration is not None and end > duration + 5)
        ):
            valid = False
        else:
            starts.append(start)
            ends.append(end)
    joined = " ".join(parts)
    clipped = len(lines) > 1000 or len(joined) > 12000
    joined = joined[:12000]
    if not joined:
        return "", False, "unavailable"
    complete = bool(
        not clipped
        and valid
        and duration is not None
        and ends
        and min(starts) <= max(5, duration * 0.05)
        and max(ends) >= min(duration - 5, duration * 0.95)
    )
    status = "truncated" if clipped else "complete" if complete else "partial"
    return joined, complete, status


# ---------------------------------------------------------------------------
# Strict Pydantic policy, credential & selection models
# ---------------------------------------------------------------------------


class BiliPolicy(BaseModel):
    """Strict autonomous Bilibili patrol and interaction policy."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    allow_like: bool = False
    allow_favorite: bool = False
    favorite_folder_id: str = Field(default="", pattern=r"^[1-9][0-9]{0,19}$|^$")
    daily_action_limit: int = Field(default=1, ge=1, le=2)

    @model_validator(mode="after")
    def folder_required(self) -> BiliPolicy:
        if self.allow_favorite and not self.favorite_folder_id:
            raise ValueError("An explicit owned favorite folder is required")
        return self


class CredentialUpdate(BaseModel):
    """Strict credential update payload with cookie-header rejection."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    sessdata: str = ""
    bili_jct: str = ""
    buvid3: str = ""
    buvid4: str = ""
    clear: bool = False

    @field_validator(*CREDENTIAL_KEYS)
    @classmethod
    def valid(cls, value: str) -> str:
        return validate_cookie(value)


class Pick(BaseModel):
    """Single selected patrol candidate."""

    model_config = ConfigDict(extra="forbid", strict=True)

    bvid: str = Field(pattern="^" + BVID + "$")
    like: bool = False
    favorite: bool = False
    share_owner: bool = False
    private_topic_ref: str = Field(default="", max_length=48)


class Selection(BaseModel):
    """Model candidate selection output."""

    model_config = ConfigDict(extra="forbid", strict=True)

    selected: list[Pick] = Field(max_length=2)
    mood_longnick: str = Field(default="", max_length=80)


__all__ = [
    "API_ORIGIN",
    "BVID",
    "BVID_RE",
    "BV_RE",
    "CANONICAL_VIDEO_HOSTS",
    "COOKIE_NAMES",
    "COVER_URL_RE",
    "CREDENTIAL_KEYS",
    "FESTIVAL_PATH_RE",
    "GET_PATHS",
    "LIVE_API_ORIGIN",
    "MAX_COVER_THUMB_BYTES",
    "MID_RE",
    "MIXIN_KEY_ENC_TAB",
    "POST_PATHS",
    "PRIVATE_SHARE_PRIORITY_SECONDS",
    "PUBLIC_DEGRADE_ERRORS",
    "PUBLIC_READ_PATHS",
    "PUBLIC_USER_AGENT",
    "RISK_CODES",
    "RISK_COOLDOWN_SECONDS",
    "SHORT_LINK_HOSTS",
    "SPI_PATH",
    "SUBTITLE_HOSTS",
    "URL_RE",
    "USER_AGENT",
    "BiliPolicy",
    "CredentialUpdate",
    "Pick",
    "Selection",
    "clean_bili_text",
    "extract_bvid",
    "extract_url_bvid",
    "extract_url_video_ref",
    "extract_video_ref",
    "has_voucher",
    "subtitle_text",
    "subtitle_timestamp",
    "validate_cookie",
    "validate_short_link",
    "validate_subtitle_url",
    "video_duration",
    "wbi_keys",
    "wbi_sign",
]
