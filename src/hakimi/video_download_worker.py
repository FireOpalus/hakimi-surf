"""Isolated yt-dlp worker. Its stdout is one bounded, credential-free JSON result.

Run only through VideoDownloader. No shell commands, playlists, browser cookies,
plugins, CAPTCHA solving, retries or arbitrary extractor/URL options are accepted.
"""

import contextlib
import functools
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
from urllib.parse import urlsplit

MAX_BYTES = 80 * 1024 * 1024
MAX_JOB_BYTES = 192 * 1024 * 1024
BVID = r"BV[1-9A-HJ-NP-Za-km-z]{10}"


class WorkerError(Exception):
    pass


def classify_error(text):
    text = str(text).lower()
    if re.search(r"(?<![a-z0-9])-?101(?![a-z0-9])", text) or "login required" in text:
        return "video_download_cookie_expired"
    if (
        re.search(r"(?<![a-z0-9])-?(?:352|412)(?![a-z0-9])", text)
        or any(s in text for s in ("captcha", "v_voucher", "rate limit", "验证码", "风控"))
        or re.search(r"http(?: error)?\s+(?:403|429)", text)
    ):
        return "video_download_risk"
    if any(s in text for s in ("supporter-only", "premium-only", "drm", "preview", "试看")):
        return "video_download_restricted"
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


def allowed_url(url):
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or parsed.username
            or parsed.password
            or parsed.port not in {None, 443}
            or parsed.fragment
            or not (
                host in {"www.bilibili.com", "api.bilibili.com"}
                or any(host.endswith("." + base) for base in ("bilivideo.com", "bilivideo.cn"))
            )
        ):
            raise ValueError
        if host == "www.bilibili.com" and not re.fullmatch(r"/video/" + BVID + r"/?", parsed.path):
            raise ValueError
        if host == "api.bilibili.com" and not parsed.path.startswith("/x/"):
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


def validate_info(info, bvid, limit):
    if (
        not isinstance(info, dict)
        or info.get("_type", "video") != "video"
        or info.get("id") != bvid
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
    return duration * rate * 1000 / 8 if rate else float("inf")


def select_format(info):
    """Choose one smallest acceptable MP4, or one MP4 video plus one M4A audio."""
    duration = info["duration"]
    videos, audios = [], []
    for fmt in info.get("formats", []):
        if (
            not isinstance(fmt, dict)
            or fmt.get("has_drm")
            or fmt.get("fragments")
            or fmt.get("protocol", "https") != "https"
            or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", str(fmt.get("format_id", "")))
        ):
            continue
        try:
            allowed_url(fmt.get("url", ""))
        except WorkerError:
            continue
        if fmt.get("vcodec") == "none" and fmt.get("ext") in {"m4a", "mp4"}:
            audios.append(fmt)
        elif (
            fmt.get("ext") == "mp4"
            and fmt.get("vcodec") not in {None, "none"}
            and valid_video_dimensions(fmt.get("width"), fmt.get("height"))
        ):
            videos.append(fmt)
    options = []
    audio = min(audios, key=lambda f: format_size(f, duration)) if audios else None
    muxed_audio_available = any(video.get("acodec") != "none" for video in videos)
    for video in videos:
        # Bili's DASH extractor sets acodec='none'; progressive MP4 may use None.
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
            }
            cost = format_size(video, duration) + format_size(audio, duration)
        else:
            # A genuinely silent source is valid visual input. Never discard an
            # available separate or muxed audio track merely to reduce file size.
            if muxed_audio_available:
                continue
            candidate, cost = dict(video), format_size(video, duration)
        if math.isinf(cost) or cost <= MAX_BYTES:
            short_side = min(video["width"], video["height"])
            long_side = max(video["width"], video["height"])
            options.append((cost, short_side, long_side, candidate))
    if not options:
        raise WorkerError("video_download_format")
    return min(options, key=lambda value: (value[0], value[1], value[2]))[3]


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


class QuietLogger:
    def debug(self, *_args, **_kwargs):
        pass

    def warning(self, text, **_kwargs):
        code = classify_error(text)
        if code != "video_download_unavailable":
            raise WorkerError(code)

    def error(self, text, **_kwargs):
        raise WorkerError(classify_error(text))


class NullOutput:
    def write(self, text):
        return len(text)

    def flush(self):
        pass


def execute(config):
    from yt_dlp import YoutubeDL
    from yt_dlp.globals import all_plugins_loaded, plugin_dirs
    from yt_dlp.networking import _urllib

    # Do this before constructing YoutubeDL or importing the extractor: a plugin
    # cannot extend the available commands, sites or postprocessors in this worker.
    plugin_dirs.value = []
    all_plugins_loaded.value = True
    from yt_dlp.extractor.bilibili import BiliBiliIE

    proxy = config["proxy"]
    proxy_url = urlsplit(proxy) if proxy else None
    proxy_port = (
        (proxy_url.port or (443 if proxy_url.scheme == "https" else 80)) if proxy_url else None
    )
    getaddrinfo = socket.getaddrinfo

    def safe_addresses(host, port, *args, **kwargs):
        addresses = getaddrinfo(host, port, *args, **kwargs)
        # A deployment-owned explicit proxy may itself be on the private LAN.
        if (
            proxy_url
            and str(host).lower() == proxy_url.hostname.lower()
            and int(port) == proxy_port
        ):
            return addresses
        if any(not ipaddress.ip_address(row[4][0]).is_global for row in addresses):
            raise WorkerError("video_download_network_forbidden")
        return addresses

    socket.getaddrinfo = safe_addresses
    redirect = _urllib.RedirectHandler

    class RestrictedRedirect(redirect):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            allowed_url(newurl)
            return super().redirect_request(req, fp, code, msg, headers, newurl)

    _urllib.RedirectHandler = RestrictedRedirect

    class RestrictedYoutubeDL(YoutubeDL):
        last_request = 0.0

        @functools.cached_property
        def _request_director(self):
            return self.build_request_director([_urllib.UrllibRH])

        def urlopen(self, req):
            allowed_url(req if isinstance(req, str) else req.url)
            if not isinstance(req, str) and getattr(req, "method", "GET") not in {"GET", "HEAD"}:
                raise WorkerError("video_download_network_forbidden")
            # No concurrent format downloads or automatic retry bursts.
            if self.last_request:
                time.sleep(random.uniform(2, 6))
            self.last_request = time.monotonic()
            return super().urlopen(req)

    class RestrictedBili(BiliBiliIE):
        @classmethod
        def ie_key(cls):
            return "BiliBili"

        def _get_chapters(self, *_args, **_kwargs):
            return []

        def extract_subtitles(self, *_args, **_kwargs):
            return {}

        def extract_comments(self, *_args, **_kwargs):
            return None

        def _download_json(self, *args, **kwargs):
            result = super()._download_json(*args, **kwargs)
            url = args[0] if args else kwargs.get("url_or_request")
            session = self._get_cookies("https://www.bilibili.com/").get("SESSDATA")
            anonymous_keys = anonymous_nav_keys(
                url,
                result,
                authenticated=bool(session and session.value),
                method="POST" if kwargs.get("data") is not None else "GET",
            )
            if not anonymous_keys:
                check_risk_payload(result)
            return result

        def _search_json(self, start_pattern, string, name, *args, **kwargs):
            result = super()._search_json(start_pattern, string, name, *args, **kwargs)
            check_risk_payload(result)
            if isinstance(result, dict):
                video = result.get("videoData") or result.get("videoInfo") or {}
                if isinstance(video, dict) and video.get("is_upower_exclusive"):
                    raise WorkerError("video_download_restricted")
            return result

        def _download_playinfo(self, bvid, cid, headers=None, query=None, fatal=True):
            # Fatal even when upstream would warn and try further quality variants.
            return super()._download_playinfo(bvid, cid, headers=headers, query=query, fatal=True)

    def selector(ctx):
        yield select_format({**ctx, "duration": duration})

    opts = {
        "quiet": True,
        "no_warnings": False,
        "logger": QuietLogger(),
        "cachedir": False,
        "cookiefile": str(Path.cwd() / "cookies.txt"),
        "proxy": proxy,
        "noplaylist": True,
        "extract_flat": False,
        "retries": 0,
        "fragment_retries": 0,
        "extractor_retries": 0,
        "file_access_retries": 0,
        "socket_timeout": 15,
        "concurrent_fragment_downloads": 1,
        "continuedl": False,
        "max_filesize": MAX_BYTES,
        "ratelimit": 4 * 1024 * 1024,
        "buffersize": 65536,
        "http_chunk_size": 0,
        "format": selector,
        "merge_output_format": "mp4",
        "fixup": "never",
        "outtmpl": str(Path.cwd() / "video.%(ext)s"),
        "writeinfojson": False,
        "writethumbnail": False,
        "writesubtitles": False,
        "writeautomaticsub": False,
        "getcomments": False,
        "progress_hooks": [check_disk],
        "postprocessor_hooks": [check_disk],
        "postprocessor_args": {
            "merger+ffmpeg_i": [
                "-protocol_whitelist",
                "file,pipe",
                "-threads",
                "1",
                "-max_streams",
                "4",
            ],
            "merger+ffmpeg_o": ["-threads", "1"],
        },
        "http_headers": {
            "Referer": "https://www.bilibili.com/",
            "User-Agent": "HakimiSurf/0.4 text patrol",
        },
    }
    bvid = config["bvid"]
    with RestrictedYoutubeDL(opts, auto_init=False) as downloader:
        downloader.add_info_extractor(RestrictedBili(downloader))
        info = downloader.extract_info(
            "https://www.bilibili.com/video/" + bvid,
            download=False,
            process=False,
            ie_key="BiliBili",
        )
        duration = validate_info(info, bvid, config["max_seconds"])
        select_format(info)  # Validate duration/format/estimated size BEFORE downloading bytes.
        downloader.process_ie_result(info, download=True)
    path = Path.cwd() / "video.mp4"
    return {"status": "ok", "duration": inspect_media(path, config["max_seconds"])}


def main():
    result = {"status": "error", "error": "video_download_unavailable"}
    try:
        os.umask(0o077)
        resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_BYTES, MAX_BYTES))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        config = json.loads(Path("request.json").read_text())
        if (
            not re.fullmatch(BVID, config["bvid"])
            or type(config["max_seconds"]) is not int
            or not 1 <= config["max_seconds"] <= 15 * 60
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
