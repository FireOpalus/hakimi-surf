"""SSRF-proof Bilibili domestic collector and local media compressor."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import math
import os
import re
import shutil
import stat
import sys
import time
import uuid
import warnings
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from PIL import Image, ImageOps, UnidentifiedImageError

log = logging.getLogger("node_agent.bili")

PUBLIC_API_ORIGIN = "https://api.bilibili.com"
LIVE_API_ORIGIN = "https://api.live.bilibili.com"
PUBLIC_API_PATHS = frozenset({"/x/web-interface/view", "/x/polymer/web-dynamic/v1/feed/space"})
LIVE_API_PATHS = frozenset({"/room/v1/Room/get_info"})

SHORT_URL_RE = re.compile(r"^https://b23\.tv/[A-Za-z0-9]{1,32}$")
REQUEST_GET_PATHS = frozenset(
    {
        "/x/frontend/finger/spi",
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
REQUEST_POST_PATHS = frozenset({"/x/web-interface/archive/like", "/x/v3/fav/resource/deal"})
SUBTITLE_HOSTS = frozenset({"aisubtitle.hdslb.com", "i0.hdslb.com", "i1.hdslb.com", "i2.hdslb.com"})
COVER_URL_RE = re.compile(
    r"^https://(?:i[0-2]\.hdslb\.com|archive\.biliimg\.com)/bfs/[A-Za-z0-9_./-]+$"
)
VIDEOSHOT_HOST_RE = re.compile(r"^[a-z0-9-]+(?:\.[a-z0-9-]+)*\.hdslb\.com$")
VIDEOSHOT_PATH_RE = re.compile(
    r"^/(?:bfs|videoshot[a-z0-9_-]{0,32})/[A-Za-z0-9_./@-]+\.(?:jpe?g|png|webp)$",
    re.IGNORECASE,
)
BVID_RE = re.compile(r"^BV[1-9A-HJ-NP-Za-km-z]{10}$")

PUBLIC_GET_MAX_BYTES = 1_048_576
REQUEST_MAX_BYTES = 524_288
COVER_MAX_DOWNLOAD_BYTES = 8_388_608
COVER_MAX_PIXELS = 25_000_000
COVER_MAX_JPEG_BYTES = 98_304
STORYBOARD_MAX_VIDEO_BYTES = 40 * 1024 * 1024
STORYBOARD_MAX_JPEG_BYTES = 245_760
STORYBOARD_CELL_W = 320
STORYBOARD_CELL_H = 180
STORYBOARD_YTDLP_FORMAT = "worstvideo[ext=mp4][height<=480]/worstvideo[height<=480]"
STORYBOARD_YTDLP_MAX_SECONDS = 15 * 60
STORYBOARD_MAX_SECONDS_CAP = 24 * 60 * 60
VIDEOSHOT_MAX_SHEETS = 1000
MAX_PROCESS_OUTPUT_BYTES = 32_768

_COOKIE_KEY_MAP = {
    "sessdata": "SESSDATA",
    "SESSDATA": "SESSDATA",
    "bili_jct": "bili_jct",
    "buvid3": "buvid3",
    "buvid4": "buvid4",
}
_COOKIE_ORDER = ("SESSDATA", "bili_jct", "buvid3", "buvid4")
_ALLOWED_IMAGE_FORMATS = frozenset({"JPEG", "PNG", "WEBP", "GIF"})
_MINIMAL_HEADERS = {
    "User-Agent": "HakimiSurf/0.4 text patrol",
    "Referer": "https://www.bilibili.com/",
}


class CollectorError(Exception):
    """Constant error code with optional upstream HTTP/API metadata (never secrets)."""

    def __init__(
        self,
        code: str,
        *,
        upstream_status: int | None = None,
        upstream_code: int | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.upstream_status = upstream_status
        self.upstream_code = upstream_code


def _validate_scalar_dict(raw: Any, *, err_code: str) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict) or len(raw) > 32:
        raise CollectorError(err_code)
    cleaned: dict[str, str] = {}
    for key, val in raw.items():
        if (
            not isinstance(key, str)
            or not key
            or len(key) > 64
            or any(ord(ch) < 33 or ord(ch) > 126 for ch in key)
        ):
            raise CollectorError(err_code)
        if isinstance(val, bool):
            cleaned[key] = "true" if val else "false"
        elif isinstance(val, int):
            cleaned[key] = str(val)
        elif isinstance(val, float):
            if not math.isfinite(val):
                raise CollectorError(err_code)
            cleaned[key] = str(val)
        elif isinstance(val, str):
            if len(val) > 4096 or any(ord(ch) < 32 and ch not in "\t" for ch in val):
                raise CollectorError(err_code)
            cleaned[key] = val
        else:
            raise CollectorError(err_code)
    return cleaned


def _normalize_cookies(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise CollectorError("bili_credentials_invalid")
    normalized: dict[str, str] = {}
    for key, val in raw.items():
        if not isinstance(key, str) or key not in _COOKIE_KEY_MAP:
            raise CollectorError("bili_credentials_invalid")
        if (
            not isinstance(val, str)
            or len(val) > 4096
            or any(ord(ch) < 33 or ord(ch) > 126 or ch == ";" for ch in val)
        ):
            raise CollectorError("bili_credentials_invalid")
        canonical = _COOKIE_KEY_MAP[key]
        if (
            canonical in normalized
            and normalized[canonical]
            and val
            and normalized[canonical] != val
        ):
            raise CollectorError("bili_credentials_invalid")
        if val:
            normalized[canonical] = val
    return {name: normalized[name] for name in _COOKIE_ORDER if name in normalized}


def _build_cookie_header(cookies: dict[str, str]) -> str:
    return "; ".join(f"{name}={cookies[name]}" for name in _COOKIE_ORDER if cookies.get(name))


def _netscape_cookie_text(cookies: dict[str, str]) -> str:
    rows = ["# Netscape HTTP Cookie File"]
    for name in _COOKIE_ORDER:
        value = cookies.get(name, "")
        if value:
            rows.append(f".bilibili.com\tTRUE\t/\tTRUE\t0\t{name}\t{value}")
    return "\n".join(rows) + "\n"


def _encode_jpeg_bounded(
    img: Image.Image,
    *,
    max_bytes: int,
    initial_quality: int = 78,
) -> tuple[bytes, int, int]:
    work = img.copy()
    work.info.clear()
    attempts = (
        (initial_quality, 0),
        (70, 0),
        (62, 2),
        (52, 2),
        (42, 2),
        (32, 2),
        (22, 2),
    )
    for _scale_step in range(5):
        for quality, subsampling in attempts:
            buf = io.BytesIO()
            work.save(buf, format="JPEG", quality=quality, subsampling=subsampling, optimize=True)
            raw = buf.getvalue()
            if len(raw) <= max_bytes:
                return raw, work.width, work.height
        new_w = max(64, int(work.width * 0.8))
        new_h = max(64, int(work.height * 0.8))
        if new_w == work.width and new_h == work.height:
            break
        work = work.resize((new_w, new_h), Image.Resampling.LANCZOS)
        work.info.clear()
    raise CollectorError("image_encode_too_large")


def _process_cover_image(raw: bytes) -> tuple[bytes, int, int]:
    if not raw:
        raise CollectorError("image_decode_failed")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as source:
                if (
                    source.format not in _ALLOWED_IMAGE_FORMATS
                    or source.width < 1
                    or source.height < 1
                    or source.width * source.height > COVER_MAX_PIXELS
                ):
                    raise CollectorError("image_format_or_size")
                source.seek(0)
                source.load()
                rgb = ImageOps.exif_transpose(source).convert("RGB")
    except CollectorError:
        raise
    except (
        Image.DecompressionBombWarning,
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        ValueError,
    ):
        raise CollectorError("image_decode_failed") from None

    rgb.thumbnail((640, 640), Image.Resampling.LANCZOS)
    return _encode_jpeg_bounded(rgb, max_bytes=COVER_MAX_JPEG_BYTES, initial_quality=78)


def _compose_storyboard_sync(frame_paths: list[Path]) -> bytes:
    if len(frame_paths) != 9:
        raise CollectorError("video_download_invalid_media")
    tiles: list[Image.Image] = []
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            for path in frame_paths:
                with Image.open(path) as src:
                    if src.width < 1 or src.height < 1 or src.width * src.height > COVER_MAX_PIXELS:
                        raise CollectorError("video_download_invalid_media")
                    src.load()
                    tile = ImageOps.exif_transpose(src).convert("RGB")
                    tile.thumbnail((STORYBOARD_CELL_W, STORYBOARD_CELL_H), Image.Resampling.LANCZOS)
                    tiles.append(tile)
    except CollectorError:
        raise
    except (
        Image.DecompressionBombWarning,
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        ValueError,
    ):
        raise CollectorError("video_download_invalid_media") from None

    cell_w = max(tile.width for tile in tiles)
    cell_h = max(tile.height for tile in tiles)
    cell_w = min(cell_w, STORYBOARD_CELL_W)
    cell_h = min(cell_h, STORYBOARD_CELL_H)

    canvas = Image.new("RGB", (cell_w * 3, cell_h * 3), (16, 16, 16))
    for idx, tile in enumerate(tiles):
        row, col = divmod(idx, 3)
        offset_x = col * cell_w + (cell_w - tile.width) // 2
        offset_y = row * cell_h + (cell_h - tile.height) // 2
        canvas.paste(tile, (offset_x, offset_y))

    jpeg_bytes, _, _ = _encode_jpeg_bounded(
        canvas, max_bytes=STORYBOARD_MAX_JPEG_BYTES, initial_quality=78
    )
    return jpeg_bytes


def _validate_videoshot_url(raw_url: str) -> str:
    if (
        not isinstance(raw_url, str)
        or not raw_url
        or len(raw_url) > 2048
        or "\\" in raw_url
        or any(ord(ch) <= 32 or ord(ch) == 127 for ch in raw_url)
    ):
        raise CollectorError("videoshot_host_forbidden")
    url = f"https:{raw_url}" if raw_url.startswith("//") else raw_url
    if not url.startswith("https://"):
        raise CollectorError("videoshot_host_forbidden")
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise CollectorError("videoshot_host_forbidden") from None
    host = (parsed.hostname or "").lower()
    lowered_path = parsed.path.lower()
    segments = parsed.path.split("/")[1:] if parsed.path.startswith("/") else []
    if (
        parsed.scheme != "https"
        or not VIDEOSHOT_HOST_RE.fullmatch(host)
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not VIDEOSHOT_PATH_RE.fullmatch(parsed.path)
        or not segments
        or any(seg in {"", ".", ".."} for seg in segments)
        or segments[-1].startswith(".")
        or "%2e" in lowered_path
        or "%2f" in lowered_path
        or "%5c" in lowered_path
        or "%00" in lowered_path
    ):
        raise CollectorError("videoshot_host_forbidden")
    return url


def _compose_videoshot_sync(
    sheet_blobs: dict[int, bytes],
    sample_slots: list[tuple[int, int]],
    *,
    img_x_len: int,
    img_y_len: int,
    img_x_size: int,
    img_y_size: int,
) -> bytes:
    if len(sample_slots) != 9:
        raise CollectorError("videoshot_invalid_media")
    decoded_sheets: dict[int, Image.Image] = {}
    tiles: list[Image.Image] = []
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            for sheet_idx, raw in sheet_blobs.items():
                if not raw:
                    raise CollectorError("videoshot_invalid_media")
                with Image.open(io.BytesIO(raw)) as src:
                    if (
                        src.format not in _ALLOWED_IMAGE_FORMATS
                        or src.width < 1
                        or src.height < 1
                        or src.width * src.height > COVER_MAX_PIXELS
                    ):
                        raise CollectorError("videoshot_invalid_media")
                    src.seek(0)
                    src.load()
                    decoded_sheets[sheet_idx] = src.convert("RGB")
            for sheet_idx, local_idx in sample_slots:
                sheet = decoded_sheets.get(sheet_idx)
                if sheet is None:
                    raise CollectorError("videoshot_invalid_media")
                avail_cols = min(img_x_len, sheet.width // img_x_size)
                avail_rows = min(img_y_len, sheet.height // img_y_size)
                if avail_cols < 1 or avail_rows < 1:
                    raise CollectorError("videoshot_invalid_media")
                row, col = divmod(local_idx, img_x_len)
                if row >= avail_rows:
                    row = avail_rows - 1
                if col >= avail_cols:
                    col = avail_cols - 1
                left = col * img_x_size
                top = row * img_y_size
                tile = sheet.crop((left, top, left + img_x_size, top + img_y_size))
                tile.thumbnail((STORYBOARD_CELL_W, STORYBOARD_CELL_H), Image.Resampling.LANCZOS)
                tiles.append(tile)
    except CollectorError:
        raise
    except (
        Image.DecompressionBombWarning,
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        ValueError,
    ):
        raise CollectorError("videoshot_invalid_media") from None
    finally:
        for img in decoded_sheets.values():
            img.close()

    cell_w = min(max(tile.width for tile in tiles), STORYBOARD_CELL_W)
    cell_h = min(max(tile.height for tile in tiles), STORYBOARD_CELL_H)
    canvas = Image.new("RGB", (cell_w * 3, cell_h * 3), (16, 16, 16))
    for idx, tile in enumerate(tiles):
        row, col = divmod(idx, 3)
        offset_x = col * cell_w + (cell_w - tile.width) // 2
        offset_y = row * cell_h + (cell_h - tile.height) // 2
        canvas.paste(tile, (offset_x, offset_y))

    jpeg_bytes, _, _ = _encode_jpeg_bounded(
        canvas, max_bytes=STORYBOARD_MAX_JPEG_BYTES, initial_quality=78
    )
    return jpeg_bytes


def _format_videoshot_reason(exc: CollectorError) -> str:
    if exc.code == "bili_http_status" and exc.upstream_status is not None:
        return f"videoshot_http_{exc.upstream_status}"
    if exc.code == "videoshot_unavailable" and exc.upstream_code is not None:
        return f"videoshot_api_{exc.upstream_code}"
    return exc.code


def _classify_ytdlp_error(text: str) -> str:
    lowered = text.lower()
    if re.search(r"(?<![a-z0-9])-?101(?![a-z0-9])", lowered) or "login required" in lowered:
        return "video_download_cookie_expired"
    if (
        re.search(r"(?<![a-z0-9])-?(?:352|412)(?![a-z0-9])", lowered)
        or re.search(
            r"(?:http(?:\s+error)?|status(?:\s+code)?)\s*:?\s*(?:412|429)(?![0-9])", lowered
        )
        or re.search(r"(?<![a-z0-9.])429(?![a-z0-9])", lowered)
        or any(tok in lowered for tok in ("captcha", "v_voucher", "rate limit", "验证码", "风控"))
    ):
        return "video_download_risk"
    if re.search(r"(?:http(?:\s+error)?|status(?:\s+code)?)\s*:?\s*567(?![0-9])", lowered):
        return "video_download_cdn_http_567"
    if (
        re.search(r"(?:http(?:\s+error)?|status(?:\s+code)?)\s*:?\s*403(?![0-9])", lowered)
        or "403 forbidden" in lowered
    ):
        return "video_download_cdn_http_403"
    if "timed out" in lowered or "timeout" in lowered:
        return "video_download_timeout"
    if any(
        tok in lowered
        for tok in (
            "requested format is not available",
            "format is not available",
            "format not available",
            "no video formats found",
        )
    ):
        return "video_download_format"
    if any(tok in lowered for tok in ("supporter-only", "premium-only", "drm", "preview", "试看")):
        return "video_download_restricted"
    if "file is larger than max-filesize" in lowered or "max-filesize" in lowered:
        return "video_download_size"
    return "video_download_unavailable"


class BiliCollector:
    """SSRF-proof domestic Bilibili collector + local media compressor."""

    def __init__(
        self,
        *,
        min_interval: float = 1.2,
        temp_dir: Path = Path("/tmp/node_bili_jobs"),
        http_client: httpx.AsyncClient | None = None,
        yt_dlp_bin: str | None = None,
        ffprobe_bin: str = "ffprobe",
        ffmpeg_bin: str = "ffmpeg",
    ) -> None:
        self.min_interval = max(0.0, float(min_interval))
        self.temp_dir = Path(temp_dir)
        self._owns_client = http_client is None
        self._http = http_client or httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        self._yt_dlp_bin = yt_dlp_bin
        self._ffprobe_bin = ffprobe_bin
        self._ffmpeg_bin = ffmpeg_bin
        self._pace_lock = asyncio.Lock()
        self._last_request = 0.0
        self._storyboard_lock = asyncio.Lock()
        self._active_jobs: set[Path] = set()

    async def close(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    def _ensure_temp_root_sync(self) -> None:
        self.temp_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.temp_dir.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise CollectorError("node_temp_dir_invalid")
        self.temp_dir.chmod(0o700)

    def _cleanup_stale_sync(self) -> int:
        self._ensure_temp_root_sync()
        removed = 0
        for entry in self.temp_dir.iterdir():
            if re.fullmatch(r"job-[0-9a-f]{32}", entry.name) and entry not in self._active_jobs:
                if entry.is_symlink() or entry.is_file():
                    entry.unlink(missing_ok=True)
                elif entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                removed += 1
        return removed

    async def cleanup_stale(self) -> int:
        return await asyncio.to_thread(self._cleanup_stale_sync)

    def _create_job_dir_sync(self) -> Path:
        self._ensure_temp_root_sync()
        job = self.temp_dir / f"job-{uuid.uuid4().hex}"
        job.mkdir(mode=0o700)
        return job

    @staticmethod
    def _write_private_file_sync(path: Path, text: str) -> None:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)

    @staticmethod
    def _unlink_sync(path: Path) -> None:
        path.unlink(missing_ok=True)

    @staticmethod
    def _rmtree_sync(path: Path) -> None:
        shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def _is_nonempty_file_sync(path: Path) -> bool:
        try:
            info = path.lstat()
        except OSError:
            return False
        return stat.S_ISREG(info.st_mode) and info.st_size > 0

    @staticmethod
    def _find_downloaded_video_sync(job_dir: Path) -> Path:
        candidate = job_dir / "video.mp4"
        if candidate.exists():
            info = candidate.lstat()
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= STORYBOARD_MAX_VIDEO_BYTES:
                raise CollectorError("video_download_size")
            return candidate
        for item in job_dir.iterdir():
            if item.name.startswith("video.") and item.name != "cookies.txt":
                info = item.lstat()
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                    raise CollectorError("video_download_invalid_media")
                if not 0 < info.st_size <= STORYBOARD_MAX_VIDEO_BYTES:
                    raise CollectorError("video_download_size")
                return item
        raise CollectorError("video_download_unavailable")

    async def _pace(self) -> None:
        if self.min_interval <= 0:
            return
        async with self._pace_lock:
            now = time.monotonic()
            delay = self.min_interval - (now - self._last_request)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_request = time.monotonic()

    async def _bounded_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        max_bytes: int,
    ) -> dict[str, Any]:
        body = await self._bounded_bytes(
            method,
            url,
            params=params,
            data=data,
            headers=headers,
            max_bytes=max_bytes,
        )
        try:
            obj = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise CollectorError("upstream_invalid_shape") from None
        if not isinstance(obj, dict):
            raise CollectorError("upstream_invalid_shape")
        return obj

    async def _bounded_bytes(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        max_bytes: int,
    ) -> bytes:
        try:
            async with self._http.stream(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    raise CollectorError(
                        "bili_redirect_forbidden", upstream_status=response.status_code
                    )
                if response.status_code >= 400:
                    raise CollectorError("bili_http_status", upstream_status=response.status_code)
                buf = bytearray()
                async for chunk in response.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > max_bytes:
                        raise CollectorError("upstream_body_too_large")
                return bytes(buf)
        except CollectorError:
            raise
        except httpx.HTTPError:
            raise CollectorError("bili_upstream_unavailable") from None

    async def public_get(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise CollectorError("invalid_params")
        live = params.get("live", False)
        path = params.get("path", "")
        if not isinstance(live, bool) or not isinstance(path, str):
            raise CollectorError("bili_endpoint_forbidden")
        allowed_paths = LIVE_API_PATHS if live else PUBLIC_API_PATHS
        if path not in allowed_paths:
            raise CollectorError("bili_endpoint_forbidden")
        query = _validate_scalar_dict(params.get("params"), err_code="invalid_params")
        origin = LIVE_API_ORIGIN if live else PUBLIC_API_ORIGIN
        await self._pace()
        obj = await self._bounded_json(
            "GET",
            origin + path,
            params=query,
            headers=dict(_MINIMAL_HEADERS),
            max_bytes=PUBLIC_GET_MAX_BYTES,
        )
        return {"code": obj.get("code"), "data": obj.get("data")}

    async def resolve_short(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise CollectorError("invalid_params")
        url = params.get("url", "")
        if not isinstance(url, str) or not SHORT_URL_RE.fullmatch(url):
            raise CollectorError("invalid_bilibili_short_link")
        await self._pace()
        try:
            async with self._http.stream(
                "GET",
                url,
                headers={"User-Agent": _MINIMAL_HEADERS["User-Agent"]},
                follow_redirects=False,
            ) as response:
                if response.status_code >= 400:
                    raise CollectorError("bili_http_status", upstream_status=response.status_code)
                if response.status_code not in {301, 302, 303, 307, 308}:
                    raise CollectorError(
                        "bili_short_link_unavailable", upstream_status=response.status_code
                    )
                location = response.headers.get("location", "").strip()
        except CollectorError:
            raise
        except httpx.HTTPError:
            raise CollectorError("bili_upstream_unavailable") from None
        if not location or any(ord(ch) < 32 or ord(ch) == 127 for ch in location):
            raise CollectorError("bili_short_link_unavailable")
        return {"location": location[:2048]}

    async def request(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise CollectorError("invalid_params")
        subtitle = params.get("subtitle", False)
        method = params.get("method", "GET")
        path = params.get("path", "")
        raw_post = params.get("post")
        if (
            not isinstance(subtitle, bool)
            or not isinstance(method, str)
            or not isinstance(path, str)
        ):
            raise CollectorError("invalid_params")
        method = method.upper()
        if method not in {"GET", "POST"}:
            raise CollectorError("bili_endpoint_forbidden")

        if subtitle:
            if method != "GET" or raw_post is not None or len(path) > 2048:
                raise CollectorError("bili_subtitle_host_forbidden")
            if any(ord(ch) < 32 or ord(ch) == 127 for ch in path):
                raise CollectorError("bili_subtitle_host_forbidden")
            try:
                parsed = urlsplit(path)
                port = parsed.port
            except ValueError:
                raise CollectorError("bili_subtitle_host_forbidden") from None
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
                raise CollectorError("bili_subtitle_host_forbidden")
            target_url = path
            cookies: dict[str, str] = {}
        else:
            allowed = REQUEST_POST_PATHS if method == "POST" else REQUEST_GET_PATHS
            if path not in allowed:
                raise CollectorError("bili_endpoint_forbidden")
            if method == "GET" and raw_post is not None:
                raise CollectorError("bili_endpoint_forbidden")
            target_url = PUBLIC_API_ORIGIN + path
            cookies = _normalize_cookies(params.get("cookies"))

        query = _validate_scalar_dict(params.get("params"), err_code="invalid_params")
        post_data = (
            _validate_scalar_dict(raw_post, err_code="invalid_params")
            if raw_post is not None
            else None
        )

        headers = dict(_MINIMAL_HEADERS)
        if not subtitle:
            cookie_header = _build_cookie_header(cookies)
            if cookie_header:
                headers["Cookie"] = cookie_header

        await self._pace()
        obj = await self._bounded_json(
            method,
            target_url,
            params=query or None,
            data=post_data,
            headers=headers,
            max_bytes=REQUEST_MAX_BYTES,
        )
        return {"envelope": obj}

    async def cover_thumb(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise CollectorError("invalid_params")
        url = params.get("url", "")
        if not isinstance(url, str) or len(url) > 2048 or not COVER_URL_RE.fullmatch(url):
            raise CollectorError("bili_cover_host_forbidden")
        parsed = urlsplit(url)
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or ".." in parsed.path.split("/")
        ):
            raise CollectorError("bili_cover_host_forbidden")

        await self._pace()
        raw = await self._bounded_bytes(
            "GET",
            url,
            headers=dict(_MINIMAL_HEADERS),
            max_bytes=COVER_MAX_DOWNLOAD_BYTES,
        )
        jpeg_bytes, width, height = await asyncio.to_thread(_process_cover_image, raw)
        return {
            "mime_type": "image/jpeg",
            "data_b64": base64.b64encode(jpeg_bytes).decode("ascii"),
            "width": width,
            "height": height,
            "bytes": len(jpeg_bytes),
        }

    def _resolve_yt_dlp_cmd(self) -> list[str]:
        if self._yt_dlp_bin:
            return [self._yt_dlp_bin]
        sibling = Path(sys.executable).with_name("yt-dlp")
        if sibling.exists():
            return [str(sibling)]
        found = shutil.which("yt-dlp")
        if found:
            return [found]
        return [sys.executable, "-m", "yt_dlp"]

    @staticmethod
    def _subprocess_env(job_dir: Path) -> dict[str, str]:
        return {
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            "LANG": "C.UTF-8",
            "HOME": str(job_dir),
            "TMPDIR": str(job_dir),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        }

    async def _exec_bounded(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
        wait_limit: float,
    ) -> tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(wait_limit):
                stdout, stderr = await proc.communicate()
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise CollectorError("video_download_timeout") from None
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            raise
        return (
            proc.returncode or 0,
            (stdout or b"")[:MAX_PROCESS_OUTPUT_BYTES],
            (stderr or b"")[:MAX_PROCESS_OUTPUT_BYTES],
        )

    async def _probe_duration(self, video_path: Path, *, cwd: Path, env: dict[str, str]) -> float:
        rc, stdout, _ = await self._exec_bounded(
            [
                self._ffprobe_bin,
                "-v",
                "error",
                "-threads",
                "1",
                "-max_streams",
                "4",
                "-protocol_whitelist",
                "file,pipe",
                "-show_entries",
                "format=duration,format_name",
                "-of",
                "json",
                str(video_path),
            ],
            cwd=cwd,
            env=env,
            wait_limit=15.0,
        )
        if rc != 0:
            raise CollectorError("video_download_invalid_media")
        try:
            probe = json.loads(stdout)
            duration = float((probe.get("format") or {})["duration"])
        except (ValueError, KeyError, TypeError):
            raise CollectorError("video_download_invalid_media") from None
        if not math.isfinite(duration) or duration <= 0:
            raise CollectorError("video_download_invalid_media")
        return duration

    async def _extract_frames(
        self,
        video_path: Path,
        duration: float,
        *,
        cwd: Path,
        env: dict[str, str],
    ) -> list[Path]:
        frames: list[Path] = []
        last_good: Path | None = None
        for idx in range(9):
            ts = max(0.0, min(max(0.0, duration - 0.05), duration * (idx + 0.5) / 9.0))
            out_path = cwd / f"frame_{idx}.jpg"
            rc, _, _ = await self._exec_bounded(
                [
                    self._ffmpeg_bin,
                    "-v",
                    "error",
                    "-y",
                    "-ss",
                    f"{ts:.3f}",
                    "-i",
                    str(video_path),
                    "-frames:v",
                    "1",
                    "-vf",
                    f"scale={STORYBOARD_CELL_W}:{STORYBOARD_CELL_H}:force_original_aspect_ratio=decrease",
                    "-q:v",
                    "3",
                    str(out_path),
                ],
                cwd=cwd,
                env=env,
                wait_limit=15.0,
            )
            has_frame = rc == 0 and await asyncio.to_thread(self._is_nonempty_file_sync, out_path)
            if not has_frame and ts > 0.0:
                rc, _, _ = await self._exec_bounded(
                    [
                        self._ffmpeg_bin,
                        "-v",
                        "error",
                        "-y",
                        "-ss",
                        "0",
                        "-i",
                        str(video_path),
                        "-frames:v",
                        "1",
                        "-vf",
                        f"scale={STORYBOARD_CELL_W}:{STORYBOARD_CELL_H}:force_original_aspect_ratio=decrease",
                        "-q:v",
                        "3",
                        str(out_path),
                    ],
                    cwd=cwd,
                    env=env,
                    wait_limit=15.0,
                )
            if rc == 0 and await asyncio.to_thread(self._is_nonempty_file_sync, out_path):
                last_good = out_path
                frames.append(out_path)
            elif last_good is not None:
                frames.append(last_good)
            else:
                raise CollectorError("video_download_invalid_media")
        return frames

    async def _resolve_page_cid_and_duration(
        self, bvid: str, page: int, api_headers: dict[str, str]
    ) -> tuple[int, float | None]:
        await self._pace()
        view_obj = await self._bounded_json(
            "GET",
            f"{PUBLIC_API_ORIGIN}/x/web-interface/view",
            params={"bvid": bvid},
            headers=api_headers,
            max_bytes=PUBLIC_GET_MAX_BYTES,
        )
        code = view_obj.get("code")
        data = view_obj.get("data")
        if code != 0 or not isinstance(data, dict):
            upstream_code = code if isinstance(code, int) and not isinstance(code, bool) else None
            raise CollectorError("videoshot_unavailable", upstream_code=upstream_code)
        pages = data.get("pages")
        if not isinstance(pages, list) or page - 1 >= len(pages):
            raise CollectorError("videoshot_unavailable")
        entry = pages[page - 1]
        if not isinstance(entry, dict):
            raise CollectorError("videoshot_unavailable")
        raw_cid = entry.get("cid")
        if isinstance(raw_cid, bool) or not isinstance(raw_cid, int) or raw_cid <= 0:
            raise CollectorError("videoshot_unavailable")
        raw_dur = entry.get("duration")
        page_duration = (
            float(raw_dur)
            if isinstance(raw_dur, (int, float))
            and not isinstance(raw_dur, bool)
            and math.isfinite(raw_dur)
            and raw_dur > 0
            else None
        )
        return raw_cid, page_duration

    async def _video_storyboard_via_videoshot(
        self,
        *,
        bvid: str,
        page: int,
        cid: int | None,
        hint_duration: float | None,
        cookies: dict[str, str],
        resolved_view_duration: list[float | None],
    ) -> dict[str, Any]:
        api_headers = dict(_MINIMAL_HEADERS)
        cookie_header = _build_cookie_header(cookies)
        if cookie_header:
            api_headers["Cookie"] = cookie_header

        resolved_cid = cid
        view_page_duration: float | None = None
        if resolved_cid is None and page > 1:
            resolved_cid, view_page_duration = await self._resolve_page_cid_and_duration(
                bvid, page, api_headers
            )
            resolved_view_duration[0] = view_page_duration

        vs_params: dict[str, str] = {"bvid": bvid, "index": "1"}
        if resolved_cid is not None:
            vs_params["cid"] = str(resolved_cid)

        await self._pace()
        vs_obj = await self._bounded_json(
            "GET",
            f"{PUBLIC_API_ORIGIN}/x/player/videoshot",
            params=vs_params,
            headers=api_headers,
            max_bytes=PUBLIC_GET_MAX_BYTES,
        )
        code = vs_obj.get("code")
        data = vs_obj.get("data")
        if code != 0 or not isinstance(data, dict):
            upstream_code = code if isinstance(code, int) and not isinstance(code, bool) else None
            raise CollectorError("videoshot_unavailable", upstream_code=upstream_code)

        raw_images = data.get("image")
        if (
            not isinstance(raw_images, list)
            or not raw_images
            or len(raw_images) > VIDEOSHOT_MAX_SHEETS
        ):
            raise CollectorError("videoshot_unavailable")

        sheet_urls = [_validate_videoshot_url(item) for item in raw_images]

        img_x_len = data.get("img_x_len")
        img_y_len = data.get("img_y_len")
        img_x_size = data.get("img_x_size")
        img_y_size = data.get("img_y_size")
        if (
            any(
                isinstance(v, bool) or not isinstance(v, int)
                for v in (img_x_len, img_y_len, img_x_size, img_y_size)
            )
            or not 1 <= img_x_len <= 50
            or not 1 <= img_y_len <= 50
            or not 1 <= img_x_size <= 1920
            or not 1 <= img_y_size <= 1080
        ):
            raise CollectorError("videoshot_invalid_media")

        per_sheet = img_x_len * img_y_len
        total_capacity = len(sheet_urls) * per_sheet

        raw_index = data.get("index")
        valid_index_count = 0
        max_index_ts: float | None = None
        if isinstance(raw_index, list):
            for val in raw_index:
                if (
                    isinstance(val, (int, float))
                    and not isinstance(val, bool)
                    and math.isfinite(val)
                    and val >= 0
                ):
                    valid_index_count += 1
                    if val > 0 and (max_index_ts is None or float(val) > max_index_ts):
                        max_index_ts = float(val)

        total_frames = (
            min(valid_index_count, total_capacity) if valid_index_count > 0 else total_capacity
        )
        sample_indices = [
            min(total_frames - 1, (total_frames * (2 * i + 1)) // 18) for i in range(9)
        ]
        sample_slots = [divmod(frame_idx, per_sheet) for frame_idx in sample_indices]
        needed_sheets = sorted({sheet_idx for sheet_idx, _ in sample_slots})

        sheet_blobs: dict[int, bytes] = {}
        for sheet_idx in needed_sheets:
            sheet_blobs[sheet_idx] = await self._bounded_bytes(
                "GET",
                sheet_urls[sheet_idx],
                headers=dict(_MINIMAL_HEADERS),
                max_bytes=COVER_MAX_DOWNLOAD_BYTES,
            )

        jpeg_bytes = await asyncio.to_thread(
            _compose_videoshot_sync,
            sheet_blobs,
            sample_slots,
            img_x_len=img_x_len,
            img_y_len=img_y_len,
            img_x_size=img_x_size,
            img_y_size=img_y_size,
        )

        dur = (
            hint_duration
            if hint_duration is not None
            else (view_page_duration if view_page_duration is not None else max_index_ts)
        )
        return {
            "bvid": bvid,
            "duration": round(dur, 3) if dur else None,
            "mime_type": "image/jpeg",
            "data_b64": base64.b64encode(jpeg_bytes).decode("ascii"),
            "bytes": len(jpeg_bytes),
            "grid": "3x3",
            "source": "videoshot",
        }

    async def video_storyboard(self, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(params, dict):
            raise CollectorError("invalid_params")
        bvid = params.get("bvid", "")
        if not isinstance(bvid, str) or not BVID_RE.fullmatch(bvid):
            raise CollectorError("invalid_bvid")
        raw_max = params.get("max_seconds", STORYBOARD_YTDLP_MAX_SECONDS)
        max_seconds = STORYBOARD_YTDLP_MAX_SECONDS if raw_max is None else raw_max
        if (
            isinstance(max_seconds, bool)
            or not isinstance(max_seconds, int)
            or not 30 <= max_seconds <= STORYBOARD_MAX_SECONDS_CAP
        ):
            raise CollectorError("video_download_duration")

        raw_page = params.get("page", 1)
        page = 1 if raw_page is None else raw_page
        if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= 999:
            raise CollectorError("invalid_params")

        raw_cid = params.get("cid")
        if raw_cid is None:
            cid: int | None = None
        elif isinstance(raw_cid, bool) or not isinstance(raw_cid, int) or raw_cid <= 0:
            raise CollectorError("invalid_params")
        else:
            cid = raw_cid

        raw_duration = params.get("duration")
        if raw_duration is None:
            hint_duration: float | None = None
        elif (
            isinstance(raw_duration, bool)
            or not isinstance(raw_duration, (int, float))
            or not math.isfinite(raw_duration)
            or raw_duration <= 0
        ):
            raise CollectorError("invalid_params")
        else:
            hint_duration = float(raw_duration)

        videoshot_only = params.get("videoshot_only", False)
        if not isinstance(videoshot_only, bool):
            raise CollectorError("invalid_params")

        cookies = _normalize_cookies(params.get("cookies"))

        resolved_view_duration: list[float | None] = [None]
        try:
            return await self._video_storyboard_via_videoshot(
                bvid=bvid,
                page=page,
                cid=cid,
                hint_duration=hint_duration,
                cookies=cookies,
                resolved_view_duration=resolved_view_duration,
            )
        except CollectorError as exc:
            videoshot_reason = _format_videoshot_reason(exc)
            if videoshot_only:
                log.warning(
                    "event=bili_storyboard_failed bvid=%s videoshot_reason=%s fallback_reason=%s",
                    bvid,
                    videoshot_reason,
                    "videoshot_only",
                )
                raise
            log.info(
                "event=bili_videoshot_fallback bvid=%s reason=%s",
                bvid,
                videoshot_reason,
            )

        ytdlp_max_seconds = min(max_seconds, STORYBOARD_YTDLP_MAX_SECONDS)
        known_duration = hint_duration if hint_duration is not None else resolved_view_duration[0]
        if known_duration is not None and known_duration > ytdlp_max_seconds:
            log.warning(
                "event=bili_storyboard_failed bvid=%s videoshot_reason=%s fallback_reason=%s",
                bvid,
                videoshot_reason,
                "video_download_duration",
            )
            raise CollectorError("video_download_duration")

        async with self._storyboard_lock:
            await self.cleanup_stale()
            job_dir = await asyncio.to_thread(self._create_job_dir_sync)
            self._active_jobs.add(job_dir)
            env = self._subprocess_env(job_dir)
            cookie_path = job_dir / "cookies.txt"
            try:
                try:
                    cmd = [
                        *self._resolve_yt_dlp_cmd(),
                        "--no-playlist",
                        "--max-filesize",
                        "40M",
                        "--format",
                        STORYBOARD_YTDLP_FORMAT,
                        "--merge-output-format",
                        "mp4",
                        "--no-part",
                        "--no-mtime",
                        "--no-write-info-json",
                        "--no-write-thumbnail",
                        "--no-write-subs",
                        "--no-write-auto-subs",
                        "--no-cache-dir",
                        "--retries",
                        "0",
                        "--fragment-retries",
                        "0",
                        "--extractor-retries",
                        "0",
                        "--socket-timeout",
                        "15",
                        "--quiet",
                        "--no-warnings",
                        "-o",
                        str(job_dir / "video.%(ext)s"),
                    ]
                    if cookies:
                        cookie_text = _netscape_cookie_text(cookies)
                        await asyncio.to_thread(
                            self._write_private_file_sync, cookie_path, cookie_text
                        )
                        cmd.extend(["--cookies", str(cookie_path)])
                    video_url = (
                        f"https://www.bilibili.com/video/{bvid}?p={page}"
                        if page > 1
                        else f"https://www.bilibili.com/video/{bvid}"
                    )
                    cmd.append(video_url)

                    await self._pace()
                    rc, stdout, stderr = await self._exec_bounded(
                        cmd, cwd=job_dir, env=env, wait_limit=90.0
                    )
                    if rc != 0:
                        combined = (stderr + b"\n" + stdout).decode("utf-8", errors="ignore")
                        raise CollectorError(_classify_ytdlp_error(combined))
                finally:
                    wipe_cookie = asyncio.create_task(
                        asyncio.to_thread(self._unlink_sync, cookie_path)
                    )
                    try:
                        await asyncio.shield(wipe_cookie)
                    except asyncio.CancelledError:
                        await wipe_cookie
                        raise

                video_path = await asyncio.to_thread(self._find_downloaded_video_sync, job_dir)
                duration = await self._probe_duration(video_path, cwd=job_dir, env=env)
                if duration > ytdlp_max_seconds:
                    raise CollectorError("video_download_duration")
                frames = await self._extract_frames(video_path, duration, cwd=job_dir, env=env)
                jpeg_bytes = await asyncio.to_thread(_compose_storyboard_sync, frames)
                return {
                    "bvid": bvid,
                    "duration": round(duration, 3),
                    "mime_type": "image/jpeg",
                    "data_b64": base64.b64encode(jpeg_bytes).decode("ascii"),
                    "bytes": len(jpeg_bytes),
                    "grid": "3x3",
                    "source": "ytdlp_fallback",
                }
            except CollectorError as exc:
                log.warning(
                    "event=bili_storyboard_failed bvid=%s videoshot_reason=%s fallback_reason=%s",
                    bvid,
                    videoshot_reason,
                    exc.code,
                )
                raise
            finally:
                self._active_jobs.discard(job_dir)
                wipe_job = asyncio.create_task(asyncio.to_thread(self._rmtree_sync, job_dir))
                try:
                    await asyncio.shield(wipe_job)
                except asyncio.CancelledError:
                    await wipe_job
                    raise
