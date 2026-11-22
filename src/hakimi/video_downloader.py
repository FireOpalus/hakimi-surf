"""Bounded subprocess downloads; no remote content controls commands or local paths."""

import asyncio
import inspect
import json
import math
import os
import re
import shutil
import signal
import stat
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .modules.bilibili import BVID
from .security import Rejected
from .video_policy import MAX_VIDEO_SECONDS

MAX_VIDEO_BYTES = 80 * 1024 * 1024
MAX_JOB_BYTES = 192 * 1024 * 1024
MAX_OUTPUT_BYTES = 16 * 1024
FAILURES = frozenset(
    {
        "video_download_risk",
        "video_download_cookie_expired",
        "video_download_unavailable",
        "video_download_duration",
        "video_download_format",
        "video_download_size",
        "video_download_timeout",
        "video_download_network_forbidden",
        "video_download_restricted",
        "video_download_invalid_media",
        "video_download_cdn_http_567",
        "video_download_cdn_http_403",
        "video_download_cdn_http_5xx",
        "video_download_cdn_host_blocked",
        "video_download_no_audio_track",
        "video_download_part_not_found",
    }
)


@dataclass(frozen=True)
class DownloadedVideo:
    path: Path
    duration: float
    bytes: int


async def check_guard(guard):
    if guard is not None:
        result = guard()
        if inspect.isawaitable(result):
            result = await result
        if result is False:
            raise Rejected("video_download_revoked")


def private_file(path: Path, text: str):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(text)


def cookie_file(cookies: dict[str, str]) -> str:
    rows = ["# Netscape HTTP Cookie File"]
    for source, name in (
        ("sessdata", "SESSDATA"),
        ("bili_jct", "bili_jct"),
        ("buvid3", "buvid3"),
        ("buvid4", "buvid4"),
    ):
        value = cookies.get(source, "")
        if (
            not isinstance(value, str)
            or len(value) > 4096
            or any(ord(c) < 33 or ord(c) > 126 or c == ";" for c in value)
        ):
            raise Rejected("video_download_credentials_invalid")
        if value:
            # Never a global Cookie header: CDN requests do not receive account credentials.
            rows.append(f".bilibili.com\tTRUE\t/\tTRUE\t0\t{name}\t{value}")
    return "\n".join(rows) + "\n"


def valid_proxy(proxy: str):
    if not isinstance(proxy, str) or len(proxy) > 4096 or any(ord(c) < 33 for c in proxy):
        raise Rejected("video_download_proxy_invalid")
    if not proxy:
        return
    try:
        url = urlsplit(proxy)
        if url.scheme not in {"http", "https"} or not url.hostname:
            raise ValueError
        _ = url.port
        if url.path not in {"", "/"} or url.query or url.fragment:
            raise ValueError
    except ValueError:
        raise Rejected("video_download_proxy_invalid") from None


class VideoDownloader:
    def __init__(self, cache_root=Path("/tmp/bili_video_cache"), *, timeout=180, poll_interval=0.2):
        self.root = Path(cache_root)
        self.timeout, self.poll_interval = timeout, poll_interval
        self.active: set[Path] = set()
        self.lock = asyncio.Lock()

    def _root(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise Rejected("video_download_cache_invalid")
        self.root.chmod(0o700)

    async def cleanup_stale(self):
        self._root()
        for path in self.root.iterdir():
            if re.fullmatch(r"job-[0-9a-f]{32}", path.name) and path not in self.active:
                if path.is_symlink() or path.is_file():
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)

    @staticmethod
    def _disk_size(job):
        total = 0
        paths = list(job.rglob("*"))
        if len(paths) > 100:
            raise Rejected("video_download_size")
        for path in paths:
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise Rejected("video_download_invalid_media")
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
        if total > MAX_JOB_BYTES:
            raise Rejected("video_download_size")
        return total

    async def _kill(self, process):
        # A crashed Python worker may leave ffmpeg descendants in its process group.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            async with asyncio.timeout(5):
                await process.wait()
        except TimeoutError:
            pass

    async def _run(self, job, guard):
        worker = Path(__file__).with_name("video_download_worker.py")
        # Interpreter isolation excludes user site packages and PYTHONPATH; the child
        # receives no server API keys, proxy environment, app settings or browser state.
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "TMPDIR": str(job),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
        }
        spawning = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(worker),
                "request.json",
                cwd=job,
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
                limit=MAX_OUTPUT_BYTES,
            )
        )
        try:
            process = await asyncio.shield(spawning)
        except asyncio.CancelledError:
            process = await spawning
            await self._kill(process)
            raise

        async def read():
            result = bytearray()
            while chunk := await process.stdout.read(1024):
                result.extend(chunk)
                if len(result) > MAX_OUTPUT_BYTES:
                    raise Rejected("video_download_unavailable")
            return bytes(result)

        reader = asyncio.create_task(read())
        wait = asyncio.create_task(process.wait())
        deadline = time.monotonic() + self.timeout
        try:
            while not wait.done():
                await check_guard(guard)
                self._disk_size(job)
                if time.monotonic() >= deadline:
                    raise Rejected("video_download_timeout")
                if reader.done() and reader.exception():
                    raise reader.exception()
                await asyncio.wait({wait}, timeout=self.poll_interval)
            await self._kill(process)
            async with asyncio.timeout(2):
                raw = await reader
            await check_guard(guard)
            self._disk_size(job)
            try:
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise ValueError
            except (ValueError, UnicodeError):
                raise Rejected("video_download_unavailable") from None
            if process.returncode or result.get("status") != "ok":
                code = result.get("error")
                raise Rejected(code if code in FAILURES else "video_download_unavailable")
            return result
        finally:
            # Cleanup must finish even when the request or the whole service is cancelled.
            cleanup = asyncio.create_task(self._kill(process))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise
            finally:
                for task in (reader, wait):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(reader, wait, return_exceptions=True)

    @asynccontextmanager
    async def download(
        self,
        bvid,
        *,
        max_seconds=MAX_VIDEO_SECONDS,
        cookies=None,
        proxy="",
        guard=None,
        page: int = 1,
    ):
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            raise Rejected("invalid_bvid")
        if (
            isinstance(max_seconds, bool)
            or not isinstance(max_seconds, int)
            or not 1 <= max_seconds <= MAX_VIDEO_SECONDS
        ):
            raise Rejected("video_download_duration")
        if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= 999:
            raise Rejected("video_download_part_not_found")
        valid_proxy(proxy)
        jar = cookie_file(cookies or {})
        async with self.lock:
            await check_guard(guard)
            self._root()
            job = self.root / ("job-" + uuid.uuid4().hex)
            job.mkdir(mode=0o700)
            self.active.add(job)
            try:
                private_file(job / "cookies.txt", jar)
                private_file(
                    job / "request.json",
                    json.dumps(
                        {"bvid": bvid, "max_seconds": max_seconds, "proxy": proxy, "page": page}
                    ),
                )
                result = await self._run(job, guard)
                path = job / "video.mp4"
                info = path.lstat()
                duration = result.get("duration")
                if (
                    not stat.S_ISREG(info.st_mode)
                    or not 0 < info.st_size <= MAX_VIDEO_BYTES
                    or isinstance(duration, bool)
                    or not isinstance(duration, (int, float))
                    or not math.isfinite(duration)
                    or not 0 < duration <= max_seconds
                ):
                    raise Rejected("video_download_invalid_media")
                await check_guard(guard)
                yield DownloadedVideo(path, float(duration), info.st_size)
            except OSError:
                raise Rejected("video_download_unavailable") from None
            finally:
                shutil.rmtree(job, ignore_errors=True)
                self.active.discard(job)
