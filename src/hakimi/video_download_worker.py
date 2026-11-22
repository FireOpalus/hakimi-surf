"""Isolated direct Bilibili WBI playurl + httpx + ffmpeg worker.

Its stdout is one bounded, credential-free JSON result. Run only through VideoDownloader.
No shell commands, playlists, browser cookies, plugins, CAPTCHA solving, or arbitrary URLs
are accepted.
"""

from __future__ import annotations

import contextlib
import hashlib
import ipaddress
import json
import math
import os
import random
import re
import resource
import socket
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urljoin, urlsplit

import httpx

MAX_BYTES = 80 * 1024 * 1024
MAX_JOB_BYTES = 192 * 1024 * 1024
MAX_API_BYTES = 1 * 1024 * 1024
MAX_REDIRECTS = 3
CDN_TRANSIENT_RETRIES = 2
CDN_TRANSIENT_STATUSES = frozenset({500, 502, 503, 504, 567})
CDN_FAILOVER_STATUSES = frozenset({403, 404, 410, 416})
API_RISK_STATUSES = frozenset({412, 429})
USER_AGENT = "HakimiSurf/0.4 text patrol"
BVID = r"BV[1-9A-HJ-NP-Za-km-z]{10}"

BILIVIDEO_HOST_RE = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)*\.bilivideo\.(?:com|cn)$")
AKAMAI_CDN_HOST_RE = re.compile(r"^upos-[a-z0-9-]+\.akamaized\.net$")
ALLOWED_COOKIE_NAMES = frozenset({"SESSDATA", "bili_jct", "buvid3", "buvid4"})
ALLOWED_COOKIE_DOMAINS = frozenset({".bilibili.com", "bilibili.com", "api.bilibili.com"})

WBI_MIXIN = (
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


class WorkerError(Exception):
    pass


def classify_error(text):
    text = str(text).lower()
    if re.search(r"(?<![a-z0-9])-?101(?![a-z0-9])", text) or "login required" in text:
        return "video_download_cookie_expired"
    if re.search(r"http(?: error)?\s+567\b", text):
        return "video_download_cdn_http_567"
    if re.search(r"http(?: error)?\s+403\b", text):
        return "video_download_cdn_http_403"
    if (
        re.search(r"(?<![a-z0-9])-?(?:352|412)(?![a-z0-9])", text)
        or any(s in text for s in ("captcha", "v_voucher", "rate limit", "验证码", "风控"))
        or re.search(r"http(?: error)?\s+(?:412|429)\b", text)
    ):
        return "video_download_risk"
    if any(s in text for s in ("supporter-only", "premium-only", "drm", "preview", "试看")):
        return "video_download_restricted"
    if any(s in text for s in ("timed out", "timeout")):
        return "video_download_timeout"
    return "video_download_unavailable"


def check_risk_payload(value):
    pending = [value]
    while pending:
        obj = pending.pop()
        if isinstance(obj, dict):
            code = obj.get("code")
            if code == -101:
                raise WorkerError("video_download_cookie_expired")
            if code in (-352, -412) or obj.get("v_voucher"):
                raise WorkerError("video_download_risk")
            pending.extend(v for v in obj.values() if isinstance(v, (dict, list)))
        elif isinstance(obj, list):
            pending.extend(v for v in obj if isinstance(v, (dict, list)))


def anonymous_nav_keys(url, data, *, authenticated=False, method="GET"):
    """Only the documented anonymous nav response can carry keys with code -101."""
    if (
        authenticated
        or method != "GET"
        or url != "https://api.bilibili.com/x/web-interface/nav"
        or not isinstance(data, dict)
        or data.get("code") != -101
        or not isinstance(data.get("data"), dict)
        or data["data"].get("isLogin") is not False
    ):
        return False
    keys = data["data"].get("wbi_img")
    if not isinstance(keys, dict):
        return False
    try:
        for name in ("img_url", "sub_url"):
            part = urlsplit(keys[name])
            if part.scheme != "https" or not re.fullmatch(
                r"[a-fA-F0-9]{32}\.png", part.path.rsplit("/", 1)[-1]
            ):
                return False
        # Ignore only this root nav code; nested risks still immediately abort.
        check_risk_payload({**data, "code": 0})
    except (WorkerError, KeyError, TypeError, ValueError):
        return False
    return True


def is_allowed_cdn_host(host: str) -> bool:
    normalized = str(host or "").lower()
    if not normalized:
        return False
    return bool(BILIVIDEO_HOST_RE.fullmatch(normalized) or AKAMAI_CDN_HOST_RE.fullmatch(normalized))


def allowed_url(url):
    try:
        if not isinstance(url, str) or not url or len(url) > 4096:
            raise ValueError
        if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in url):
            raise ValueError
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        if (
            parsed.scheme != "https"
            or parsed.username
            or parsed.password
            or parsed.port not in {None, 443}
            or parsed.fragment
            or not (host in {"www.bilibili.com", "api.bilibili.com"} or is_allowed_cdn_host(host))
        ):
            raise ValueError
        if host == "www.bilibili.com" and not re.fullmatch(r"/video/" + BVID + r"/?", parsed.path):
            raise ValueError
        if host == "api.bilibili.com" and (
            not parsed.path.startswith("/x/") or ".." in parsed.path.split("/")
        ):
            raise ValueError
        return url
    except (ValueError, TypeError):
        raise WorkerError("video_download_network_forbidden") from None


def checked_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return value if value > 0 else None


def valid_video_dimensions(width, height) -> bool:
    w, h = checked_number(width), checked_number(height)
    if not w or not h:
        return False
    return 180 <= min(w, h) <= 540 and max(w, h) <= 1280


def codec_rank(codec: Any) -> int:
    name = str(codec or "").lower()
    if name.startswith(("avc1", "h264")):
        return 0
    if name.startswith(("hvc1", "hev1", "hevc")):
        return 1
    if name.startswith(("av01", "av1")):
        return 2
    return 3


def validate_info(info, bvid, limit, *, page: int = 1):
    if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= 999:
        raise WorkerError("video_download_restricted")
    allowed_ids = {bvid, f"{bvid}_p{page}"}
    if (
        not isinstance(info, dict)
        or info.get("_type", "video") != "video"
        or info.get("id") not in allowed_ids
        or info.get("entries") is not None
        or info.get("is_live")
        or info.get("live_status") in {"is_live", "is_upcoming", "post_live"}
        or info.get("has_drm")
        or info.get("availability") in {"premium_only", "subscriber_only", "needs_auth", "private"}
    ):
        raise WorkerError("video_download_restricted")
    duration = checked_number(info.get("duration"))
    if not duration or duration > limit:
        raise WorkerError("video_download_duration")
    return duration


def format_size(fmt, duration):
    known = checked_number(fmt.get("filesize")) or checked_number(fmt.get("filesize_approx"))
    if known:
        return known
    rate = checked_number(fmt.get("tbr"))
    if rate:
        return duration * rate * 1000 / 8
    bw = checked_number(fmt.get("bandwidth"))
    return duration * bw / 8 if bw else float("inf")


def _extract_stream_urls(stream: dict[str, Any]) -> tuple[list[str], bool]:
    raw_candidates: list[str] = []
    for key in ("baseUrl", "base_url", "url"):
        val = stream.get(key)
        if isinstance(val, str) and val.strip():
            raw_candidates.append(val.strip())
    for key in ("backupUrl", "backup_url"):
        backups = stream.get(key)
        if isinstance(backups, list):
            for item in backups:
                if isinstance(item, str) and item.strip():
                    raw_candidates.append(item.strip())
    deduped = list(dict.fromkeys(raw_candidates))
    if not deduped:
        return [], False
    allowed: list[str] = []
    for candidate in deduped:
        try:
            allowed.append(allowed_url(candidate))
        except WorkerError:
            continue
    return allowed, True


def select_format(info):
    """Choose one best low-resolution MP4 with audio (never silently dropping audio)."""
    duration = info["duration"]
    videos, audios = [], []
    blocked_audio = False
    blocked_video = False
    for fmt in info.get("formats", []):
        if (
            not isinstance(fmt, dict)
            or fmt.get("has_drm")
            or fmt.get("fragments")
            or fmt.get("protocol", "https") != "https"
            or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", str(fmt.get("format_id", "")))
        ):
            continue
        is_audio = fmt.get("vcodec") == "none" and fmt.get("ext") in {"m4a", "mp4"}
        is_video = (
            fmt.get("ext") == "mp4"
            and fmt.get("vcodec") not in {None, "none"}
            and valid_video_dimensions(fmt.get("width"), fmt.get("height"))
        )
        if not is_audio and not is_video:
            continue
        allowed_urls, had_raw = _extract_stream_urls(fmt)
        if not allowed_urls:
            if had_raw:
                if is_audio:
                    blocked_audio = True
                if is_video:
                    blocked_video = True
            continue
        normalized_fmt = {**fmt, "url": allowed_urls[0], "candidate_urls": allowed_urls}
        if is_audio:
            audios.append(normalized_fmt)
        else:
            videos.append(normalized_fmt)

    if not videos:
        if blocked_video:
            raise WorkerError("video_download_cdn_host_blocked")
        raise WorkerError("video_download_format")

    options = []
    audio = min(audios, key=lambda f: format_size(f, duration)) if audios else None
    saw_dash_video = False
    for video in videos:
        # Bili's DASH extractor sets acodec='none'; progressive MP4 may use None or a codec string.
        if video.get("acodec") != "none":
            candidate, cost = dict(video), format_size(video, duration)
        elif audio is not None:
            candidate = {
                "format_id": video["format_id"] + "+" + audio["format_id"],
                "ext": "mp4",
                "requested_formats": [video, audio],
                "protocol": "https+https",
                "height": video["height"],
                "width": video["width"],
                "vcodec": video.get("vcodec"),
            }
            cost = format_size(video, duration) + format_size(audio, duration)
        else:
            # Never silently downgrade a DASH video to a silent download when audio is missing
            # or filtered out by the CDN host allowlist.
            saw_dash_video = True
            continue
        if math.isinf(cost) or cost <= MAX_BYTES:
            short_side = min(video["width"], video["height"])
            long_side = max(video["width"], video["height"])
            res_tier = 0 if 360 <= short_side <= 480 else 1
            options.append(
                (codec_rank(video.get("vcodec")), res_tier, cost, short_side, long_side, candidate)
            )
    if not options:
        if saw_dash_video and audio is None:
            raise WorkerError(
                "video_download_cdn_host_blocked"
                if blocked_audio
                else "video_download_no_audio_track"
            )
        raise WorkerError("video_download_format")
    return min(options, key=lambda value: value[:5])[5]


def select_dash_streams(dash: dict[str, Any], duration: float) -> dict[str, Any]:
    """Select best 360p/480p H.264 (avc1 > hvc1 > av01) DASH video + audio stream pair."""
    if not isinstance(dash, dict):
        raise WorkerError("video_download_format")

    raw_audios = dash.get("audio")
    audios: list[tuple[float, dict[str, Any]]] = []
    had_raw_audio = False
    if isinstance(raw_audios, list):
        for stream in raw_audios:
            if not isinstance(stream, dict):
                continue
            allowed_urls, had_raw = _extract_stream_urls(stream)
            if had_raw:
                had_raw_audio = True
            if not allowed_urls:
                continue
            est_bytes = format_size(stream, duration)
            audios.append(
                (
                    est_bytes,
                    {
                        "id": stream.get("id"),
                        "codecs": str(stream.get("codecs") or ""),
                        "bandwidth": checked_number(stream.get("bandwidth")) or 0,
                        "est_bytes": est_bytes,
                        "urls": allowed_urls,
                    },
                )
            )
    if not audios:
        if had_raw_audio:
            raise WorkerError("video_download_cdn_host_blocked")
        raise WorkerError("video_download_no_audio_track")

    best_audio = min(audios, key=lambda item: (item[0], item[1]["bandwidth"]))[1]
    audio_cost = best_audio["est_bytes"]

    raw_videos = dash.get("video")
    video_options: list[tuple[int, int, float, float, float, dict[str, Any]]] = []
    had_valid_dim_video_with_urls = False
    had_allowed_dim_video = False
    had_oversized_dim_video = False
    if isinstance(raw_videos, list):
        for stream in raw_videos:
            if not isinstance(stream, dict):
                continue
            mime = str(stream.get("mimeType") or stream.get("mime_type") or "").lower()
            if mime and "mp4" not in mime:
                continue
            width, height = stream.get("width"), stream.get("height")
            if not valid_video_dimensions(width, height):
                continue
            allowed_urls, had_raw = _extract_stream_urls(stream)
            if had_raw:
                had_valid_dim_video_with_urls = True
            if not allowed_urls:
                continue
            had_allowed_dim_video = True
            v_cost = format_size(stream, duration)
            total_cost = (
                v_cost + audio_cost
                if not (math.isinf(v_cost) or math.isinf(audio_cost))
                else float("inf")
            )
            if not math.isinf(total_cost) and total_cost > MAX_BYTES:
                had_oversized_dim_video = True
                continue
            short_side = min(float(width), float(height))
            long_side = max(float(width), float(height))
            res_tier = 0 if 360 <= short_side <= 480 else 1
            codecs = str(stream.get("codecs") or "")
            video_options.append(
                (
                    codec_rank(codecs),
                    res_tier,
                    total_cost,
                    short_side,
                    long_side,
                    {
                        "id": stream.get("id"),
                        "width": int(width),
                        "height": int(height),
                        "codecs": codecs,
                        "bandwidth": checked_number(stream.get("bandwidth")) or 0,
                        "est_bytes": v_cost,
                        "urls": allowed_urls,
                    },
                )
            )
    if not video_options:
        if had_oversized_dim_video:
            raise WorkerError("video_download_size")
        if had_valid_dim_video_with_urls and not had_allowed_dim_video:
            raise WorkerError("video_download_cdn_host_blocked")
        raise WorkerError("video_download_format")

    best_video = min(video_options, key=lambda item: item[:5])[5]
    return {"video": best_video, "audio": best_audio}


def extract_wbi_keys(data: dict[str, Any]) -> tuple[str, str]:
    wbi_img = data.get("wbi_img") if isinstance(data, dict) else None
    if not isinstance(wbi_img, dict):
        raise WorkerError("video_download_unavailable")
    keys: list[str] = []
    for name in ("img_url", "sub_url"):
        raw_url = str(wbi_img.get(name) or "")
        part = urlsplit(raw_url)
        key = part.path.rsplit("/", 1)[-1].split(".")[0]
        if part.scheme != "https" or not re.fullmatch(r"[a-fA-F0-9]{32}", key):
            raise WorkerError("video_download_unavailable")
        keys.append(key)
    return (keys[0], keys[1])


def wbi_sign(params: dict[str, Any], keys: tuple[str, str], now: float) -> dict[str, str]:
    raw = "".join(keys)
    if not re.fullmatch(r"[a-fA-F0-9]{64}", raw):
        raise WorkerError("video_download_unavailable")
    mixin = "".join(raw[index] for index in WBI_MIXIN)[:32]
    values = {**params, "wts": str(int(now))}
    cleaned = {k: re.sub(r"[!'()*]", "", str(v)) for k, v in sorted(values.items())}
    cleaned["w_rid"] = hashlib.md5(
        (urlencode(cleaned, quote_via=quote) + mixin).encode("utf-8")
    ).hexdigest()
    return cleaned


def parse_netscape_cookies(path: Path) -> dict[str, str]:
    cookies: dict[str, str] = {}
    if not path.is_file():
        return cookies
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split("\t")
        if len(parts) < 7:
            continue
        domain, _, _, _, _, name, value = parts[:7]
        if domain.lower() in ALLOWED_COOKIE_DOMAINS and name in ALLOWED_COOKIE_NAMES and value:
            cookies[name] = value
    return cookies


def resolve_part_metadata(
    view_data: dict[str, Any], bvid: str, max_seconds: int, *, page: int = 1
) -> tuple[int, float]:
    if not isinstance(view_data, dict) or view_data.get("bvid") != bvid:
        raise WorkerError("video_download_restricted")
    rights = view_data.get("rights") if isinstance(view_data.get("rights"), dict) else {}
    if (
        view_data.get("is_upower_exclusive")
        or view_data.get("is_live")
        or rights.get("pay")
        or rights.get("ugc_pay")
    ):
        raise WorkerError("video_download_restricted")
    if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= 999:
        raise WorkerError("video_download_part_not_found")

    pages = view_data.get("pages")
    if isinstance(pages, list) and pages:
        if page > len(pages):
            raise WorkerError("video_download_part_not_found")
        selected = pages[page - 1]
        if not isinstance(selected, dict):
            raise WorkerError("video_download_restricted")
        cid = selected.get("cid")
        duration = checked_number(selected.get("duration")) or checked_number(
            view_data.get("duration")
        )
    else:
        if page != 1:
            raise WorkerError("video_download_part_not_found")
        cid = view_data.get("cid")
        duration = checked_number(view_data.get("duration"))

    if isinstance(cid, bool) or not isinstance(cid, int) or cid <= 0:
        raise WorkerError("video_download_restricted")
    if not duration or duration > max_seconds:
        raise WorkerError("video_download_duration")
    return cid, float(duration)


def check_disk(*_):
    files = list(Path.cwd().rglob("*"))
    if len(files) > 100 or sum(p.lstat().st_size for p in files if p.is_file()) > MAX_JOB_BYTES:
        raise WorkerError("video_download_size")


def inspect_media(path, limit):
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-threads",
                "1",
                "-max_streams",
                "4",
                "-protocol_whitelist",
                "file,pipe",
                "-show_entries",
                "format=duration,format_name:stream=codec_type,width,height",
                "-of",
                "json",
                str(path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=True,
        )
        if len(result.stdout) > 16384:
            raise ValueError
        data = json.loads(result.stdout)
        raw = data.get("format") or {}
        duration = float(raw["duration"])
        video = [s for s in data["streams"] if s.get("codec_type") == "video"]
        audio = [s for s in data["streams"] if s.get("codec_type") == "audio"]
        if (
            not math.isfinite(duration)
            or not 0 < duration <= limit
            or "mp4" not in raw.get("format_name", "").split(",")
            or len(video) != 1
            or len(audio) > 1
            or not valid_video_dimensions(video[0].get("width"), video[0].get("height"))
            or not 0 < path.stat().st_size <= MAX_BYTES
        ):
            raise ValueError
        return duration
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        raise WorkerError("video_download_invalid_media") from None


def _mux_dash_streams(video_path: Path, audio_path: Path, out_path: Path) -> None:
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-v",
                "error",
                "-y",
                "-protocol_whitelist",
                "file,pipe",
                "-threads",
                "1",
                "-max_streams",
                "4",
                "-i",
                str(video_path),
                "-i",
                str(audio_path),
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                "-threads",
                "1",
                str(out_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
            check=True,
        )
    except subprocess.TimeoutExpired:
        raise WorkerError("video_download_timeout") from None
    except (OSError, subprocess.SubprocessError):
        raise WorkerError("video_download_invalid_media") from None


def _api_get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    cookies: dict[str, str],
    last_api_at: list[float],
    sleep_fn=time.sleep,
) -> dict[str, Any]:
    allowed_url(url)
    if last_api_at[0] > 0:
        sleep_fn(random.uniform(2, 6))
    last_api_at[0] = time.monotonic()

    headers = {
        "Referer": "https://www.bilibili.com/",
        "User-Agent": USER_AGENT,
    }
    cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items() if v)
    if cookie_header:
        headers["Cookie"] = cookie_header

    try:
        with client.stream(
            "GET",
            url,
            params=params,
            headers=headers,
            follow_redirects=False,
        ) as resp:
            if 300 <= resp.status_code < 400:
                raise WorkerError("video_download_network_forbidden")
            if resp.status_code in API_RISK_STATUSES:
                raise WorkerError("video_download_risk")
            if resp.status_code == 401:
                raise WorkerError("video_download_cookie_expired")
            if resp.status_code >= 400:
                raise WorkerError("video_download_unavailable")
            buf = bytearray()
            for chunk in resp.iter_bytes(chunk_size=16384):
                buf.extend(chunk)
                if len(buf) > MAX_API_BYTES:
                    raise WorkerError("video_download_size")
    except WorkerError:
        raise
    except httpx.TimeoutException:
        raise WorkerError("video_download_timeout") from None
    except httpx.HTTPError:
        raise WorkerError("video_download_unavailable") from None

    try:
        payload = json.loads(buf)
    except (ValueError, UnicodeDecodeError):
        raise WorkerError("video_download_unavailable") from None
    if not isinstance(payload, dict):
        raise WorkerError("video_download_unavailable")
    return payload


def _parse_content_range_total(header_value: str | None) -> int | None:
    if not header_value:
        return None
    match = re.match(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", header_value.strip(), re.IGNORECASE)
    if not match or match.group(3) == "*":
        return None
    total = int(match.group(3))
    return total if total > 0 else None


def _download_single_url(
    client: httpx.Client,
    url: str,
    dest: Path,
    *,
    max_bytes: int,
) -> tuple[bool, int, int | None, str | None]:
    """Download one CDN URL with forced Range: bytes=0- and strict redirect allowlist.

    Returns (ok, written_bytes, http_status, error_code).
    """
    current_url = allowed_url(url)
    written = 0
    total_expected: int | None = None
    max_chunks = 64

    with contextlib.suppress(OSError):
        dest.unlink()

    with dest.open("wb") as out:
        for _chunk_idx in range(max_chunks):
            redirects = 0
            while True:
                headers = {
                    "Referer": "https://www.bilibili.com/",
                    "User-Agent": USER_AGENT,
                    "Range": f"bytes={written}-",
                }
                try:
                    with client.stream(
                        "GET",
                        current_url,
                        headers=headers,
                        follow_redirects=False,
                    ) as resp:
                        status = resp.status_code
                        if status in {301, 302, 303, 307, 308}:
                            if redirects >= MAX_REDIRECTS:
                                return False, written, status, "video_download_network_forbidden"
                            loc = (resp.headers.get("location") or "").strip()
                            if not loc:
                                return False, written, status, "video_download_network_forbidden"
                            next_url = urljoin(current_url, loc)
                            current_url = allowed_url(next_url)
                            redirects += 1
                            continue
                        if status not in {200, 206}:
                            return False, written, status, None

                        if status == 200 and written > 0:
                            # Server ignored Range on continuation; restart cleanly from 0 once.
                            out.seek(0)
                            out.truncate(0)
                            written = 0

                        raw_cl = resp.headers.get("content-length")
                        if raw_cl and raw_cl.isdigit():
                            cl = int(raw_cl)
                            if written + cl > max_bytes:
                                raise WorkerError("video_download_size")

                        if status == 206:
                            parsed_total = _parse_content_range_total(
                                resp.headers.get("content-range")
                            )
                            if parsed_total is not None:
                                if parsed_total > max_bytes:
                                    raise WorkerError("video_download_size")
                                total_expected = parsed_total

                        chunk_bytes = 0
                        next_disk_check = 1024 * 1024
                        for piece in resp.iter_bytes(chunk_size=65536):
                            if not piece:
                                continue
                            written += len(piece)
                            chunk_bytes += len(piece)
                            if written > max_bytes:
                                raise WorkerError("video_download_size")
                            out.write(piece)
                            if written >= next_disk_check:
                                out.flush()
                                check_disk()
                                next_disk_check = written + 1024 * 1024
                        out.flush()
                        check_disk()

                        if status == 200:
                            return written > 0, written, status, None
                        if total_expected is None or written >= total_expected:
                            return written > 0, written, status, None
                        if chunk_bytes == 0:
                            return False, written, status, "video_download_unavailable"
                        break
                except WorkerError:
                    raise
                except httpx.TimeoutException:
                    return False, written, None, "video_download_timeout"
                except httpx.HTTPError:
                    return False, written, None, "video_download_unavailable"

    return False, written, None, "video_download_unavailable"


def download_cdn_stream(
    client: httpx.Client,
    urls: list[str],
    dest: Path,
    *,
    max_bytes: int,
    sleep_fn=time.sleep,
) -> int:
    """Stream one media track with baseUrl -> backupUrl failover and transient 5xx retries."""
    if not urls:
        raise WorkerError("video_download_cdn_host_blocked")
    observed_statuses: list[int] = []
    observed_errors: list[str] = []

    for candidate in urls:
        allowed_url(candidate)
        for attempt in range(1 + CDN_TRANSIENT_RETRIES):
            ok, written, status, err_code = _download_single_url(
                client, candidate, dest, max_bytes=max_bytes
            )
            if ok:
                return written
            with contextlib.suppress(OSError):
                dest.unlink()
            if status is not None:
                observed_statuses.append(status)
            if err_code:
                observed_errors.append(err_code)
                if err_code == "video_download_network_forbidden":
                    raise WorkerError("video_download_network_forbidden")
            if status in CDN_FAILOVER_STATUSES:
                break
            if status in CDN_TRANSIENT_STATUSES or err_code in {
                "video_download_timeout",
                "video_download_unavailable",
            }:
                if attempt < CDN_TRANSIENT_RETRIES:
                    sleep_fn(0.5 * (attempt + 1))
                    continue
            break

    if 567 in observed_statuses:
        raise WorkerError("video_download_cdn_http_567")
    if 403 in observed_statuses:
        raise WorkerError("video_download_cdn_http_403")
    if any(500 <= st < 600 for st in observed_statuses):
        raise WorkerError("video_download_cdn_http_5xx")
    if "video_download_timeout" in observed_errors:
        raise WorkerError("video_download_timeout")
    raise WorkerError("video_download_unavailable")


class NullOutput:
    def write(self, text):
        return len(text)

    def flush(self):
        pass


def execute(config, *, http_client: httpx.Client | None = None, sleep_fn=time.sleep):
    bvid = config["bvid"]
    max_seconds = config["max_seconds"]
    page = config.get("page", 1)
    proxy = config.get("proxy") or ""
    proxy_url = urlsplit(proxy) if proxy else None
    proxy_port = (
        (proxy_url.port or (443 if proxy_url.scheme == "https" else 80)) if proxy_url else None
    )

    orig_getaddrinfo = socket.getaddrinfo

    def safe_addresses(host, port, *args, **kwargs):
        addresses = orig_getaddrinfo(host, port, *args, **kwargs)
        # A deployment-owned explicit proxy may itself be on the private LAN.
        if (
            proxy_url
            and proxy_url.hostname
            and str(host).lower() == proxy_url.hostname.lower()
            and int(port) == proxy_port
        ):
            return addresses
        if any(not ipaddress.ip_address(row[4][0]).is_global for row in addresses):
            raise WorkerError("video_download_network_forbidden")
        return addresses

    cookies = parse_netscape_cookies(Path.cwd() / "cookies.txt")
    authenticated = bool(cookies.get("SESSDATA"))
    last_api_at = [0.0]

    owns_client = http_client is None
    client = http_client or httpx.Client(
        timeout=httpx.Timeout(20.0, connect=10.0),
        trust_env=False,
        follow_redirects=False,
        proxy=proxy or None,
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
    )

    video_part = Path.cwd() / "video.m4s"
    audio_part = Path.cwd() / "audio.m4s"
    out_path = Path.cwd() / "video.mp4"

    if owns_client:
        socket.getaddrinfo = safe_addresses
    try:
        nav_url = "https://api.bilibili.com/x/web-interface/nav"
        nav_json = _api_get_json(
            client, nav_url, cookies=cookies, last_api_at=last_api_at, sleep_fn=sleep_fn
        )
        if not anonymous_nav_keys(nav_url, nav_json, authenticated=authenticated, method="GET"):
            check_risk_payload(nav_json)
            if nav_json.get("code") != 0:
                raise WorkerError("video_download_unavailable")
        keys = extract_wbi_keys(nav_json.get("data") or {})

        view_json = _api_get_json(
            client,
            "https://api.bilibili.com/x/web-interface/view",
            params={"bvid": bvid},
            cookies=cookies,
            last_api_at=last_api_at,
            sleep_fn=sleep_fn,
        )
        check_risk_payload(view_json)
        view_code = view_json.get("code")
        if view_code != 0:
            if view_code in {-404, -403, 62002, 62004, 62012}:
                raise WorkerError("video_download_restricted")
            raise WorkerError("video_download_unavailable")
        cid, duration = resolve_part_metadata(
            view_json.get("data") or {}, bvid, max_seconds, page=page
        )

        play_params = wbi_sign(
            {
                "bvid": bvid,
                "cid": cid,
                "qn": 32,
                "fnval": 16,
                "fnver": 0,
                "fourk": 0,
            },
            keys,
            time.time(),
        )
        play_json = _api_get_json(
            client,
            "https://api.bilibili.com/x/player/wbi/playurl",
            params=play_params,
            cookies=cookies,
            last_api_at=last_api_at,
            sleep_fn=sleep_fn,
        )
        check_risk_payload(play_json)
        play_code = play_json.get("code")
        if play_code != 0:
            if play_code in {-10403, -403, -404, 87007, 87008}:
                raise WorkerError("video_download_restricted")
            raise WorkerError("video_download_unavailable")
        play_data = play_json.get("data") or {}
        if not isinstance(play_data, dict):
            raise WorkerError("video_download_unavailable")
        if play_data.get("is_preview") == 1:
            raise WorkerError("video_download_restricted")

        dash = play_data.get("dash")
        durl = play_data.get("durl")
        if isinstance(dash, dict):
            selected = select_dash_streams(dash, duration)
            audio_written = download_cdn_stream(
                client,
                selected["audio"]["urls"],
                audio_part,
                max_bytes=MAX_BYTES,
                sleep_fn=sleep_fn,
            )
            remaining = MAX_BYTES - audio_written
            if remaining <= 0:
                raise WorkerError("video_download_size")
            download_cdn_stream(
                client,
                selected["video"]["urls"],
                video_part,
                max_bytes=remaining,
                sleep_fn=sleep_fn,
            )
            _mux_dash_streams(video_part, audio_part, out_path)
            with contextlib.suppress(OSError):
                video_part.unlink()
            with contextlib.suppress(OSError):
                audio_part.unlink()
        elif isinstance(durl, list) and len(durl) == 1 and isinstance(durl[0], dict):
            durl_urls, had_raw = _extract_stream_urls(durl[0])
            if not durl_urls:
                raise WorkerError(
                    "video_download_cdn_host_blocked" if had_raw else "video_download_format"
                )
            durl_size = checked_number(durl[0].get("size"))
            if durl_size and durl_size > MAX_BYTES:
                raise WorkerError("video_download_size")
            download_cdn_stream(
                client,
                durl_urls,
                out_path,
                max_bytes=MAX_BYTES,
                sleep_fn=sleep_fn,
            )
        else:
            raise WorkerError("video_download_format")

        return {"status": "ok", "duration": inspect_media(out_path, max_seconds)}
    finally:
        with contextlib.suppress(OSError):
            video_part.unlink()
        with contextlib.suppress(OSError):
            audio_part.unlink()
        if owns_client:
            socket.getaddrinfo = orig_getaddrinfo
            client.close()


def main():
    result = {"status": "error", "error": "video_download_unavailable"}
    try:
        os.umask(0o077)
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_BYTES, MAX_BYTES))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        config = json.loads(Path("request.json").read_text())
        page = config.get("page", 1)
        if (
            not isinstance(config.get("bvid"), str)
            or not re.fullmatch(BVID, config["bvid"])
            or type(config.get("max_seconds")) is not int
            or not 1 <= config["max_seconds"] <= 15 * 60
            or type(page) is not int
            or not 1 <= page <= 999
        ):
            raise WorkerError("video_download_restricted")
        with contextlib.redirect_stdout(NullOutput()), contextlib.redirect_stderr(NullOutput()):
            result = execute(config)
    except WorkerError as exc:
        result["error"] = str(exc)
    except Exception as exc:
        result["error"] = classify_error(exc)
    print(json.dumps(result), flush=True)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
