"""Bounded Gemini Files lifecycle; only names created by this service are deleted."""

import asyncio
import inspect
import logging
import math
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from .security import Rejected
from .video_policy import MAX_VIDEO_SECONDS

SCHEMA = """
CREATE TABLE IF NOT EXISTS gemini_video_files (
  id TEXT PRIMARY KEY, payload BLOB NOT NULL, created REAL NOT NULL,
  retry_after REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0
);
"""
FILE_NAME = re.compile(r"files/hv-[0-9a-f]{32}")
MAX_FILE_BYTES = 80 * 1024 * 1024


async def run_guard(callback):
    if callback is not None:
        result = callback()
        if inspect.isawaitable(result):
            result = await result
        if result is False:
            raise Rejected("video_watch_invalidated")


def video_tokens(duration: float) -> int:
    if (
        isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or not 0 < duration <= MAX_VIDEO_SECONDS
    ):
        raise Rejected("video_duration_invalid")
    # Static 1 FPS + low resolution is ~66 visual + 32 audio tokens/second.
    # Reserve substantial headroom for timestamps/decoder changes; settle real usage.
    return math.ceil(duration) * 1024 + 2048


@dataclass(frozen=True)
class ModelVideo:
    name: str
    uri: str
    duration: float
    mime_type: str = "video/mp4"

    def block(self):
        self.validate()
        return {
            "type": "video",
            "uri": self.uri,
            "mime_type": self.mime_type,
            "resolution": "low",
            "processing": {"type": "static", "fps": 1},
        }

    def validate(self):
        video_tokens(self.duration)
        try:
            if not isinstance(self.name, str) or not isinstance(self.uri, str):
                raise ValueError
            url = urlsplit(self.uri)
            port = url.port
        except (TypeError, ValueError):
            raise Rejected("video_file_reference_invalid") from None
        if (
            not FILE_NAME.fullmatch(self.name)
            or self.mime_type != "video/mp4"
            or url.scheme != "https"
            or url.hostname != "generativelanguage.googleapis.com"
            or port not in {None, 443}
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"/v1beta/" + self.name, "/v1/" + self.name}
        ):
            raise Rejected("video_file_reference_invalid")

    @property
    def tokens(self):
        return video_tokens(self.duration)


class VideoFiles:
    def __init__(self, store, files):
        self.store, self.files = store, files
        self.init_lock = asyncio.Lock()
        self.cleanup_lock = asyncio.Lock()
        self.initialized = False
        self.active = set()
        self.upload_timeout = 120.0
        self.processing_timeout = 120.0
        self.delete_timeout = 5.0
        self.sleep = asyncio.sleep

    async def initialize(self):
        async with self.init_lock:
            if not self.initialized:
                async with self.store.transaction():
                    await self.store.db.execute(SCHEMA)
                self.initialized = True

    async def _delete(self, key, name):
        if not FILE_NAME.fullmatch(name):
            raise Rejected("video_file_reference_invalid")
        deleted = False
        try:
            async with asyncio.timeout(self.delete_timeout):
                await self.files().delete(name=name, config={"http_options": {"timeout": 5000}})
            deleted = True
        except Exception as exc:
            # A missing file is already clean. Never log upstream error text or file URI.
            code = getattr(exc, "code", None)
            status = getattr(getattr(exc, "response", None), "status_code", None)
            deleted = code == 404 or status == 404
        async with self.store.transaction():
            if deleted:
                await self.store.db.execute("DELETE FROM gemini_video_files WHERE id=?", (key,))
            else:
                await self.store.db.execute(
                    "UPDATE gemini_video_files SET attempts=attempts+1,retry_after=? WHERE id=?",
                    (time.time() + 300, key),
                )
        if not deleted:
            logging.getLogger("hakimi").info("event=video_file_cleanup_pending")
        return deleted

    async def _finish(self, key, name):
        # Deletion survives caller cancellation, and remains bounded during shutdown.
        task = asyncio.create_task(self._delete(key, name))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await asyncio.shield(task)
            finally:
                raise

    async def cleanup(self):
        await self.initialize()
        if self.cleanup_lock.locked():
            return
        async with self.cleanup_lock:
            async with self.store.lock:
                rows = await self.store.all(
                    "SELECT id,payload FROM gemini_video_files WHERE retry_after<=? "
                    "ORDER BY created LIMIT 20",
                    (time.time(),),
                )
            count = 0
            for row in rows:
                if row["id"] in self.active:
                    continue
                value = self.store.vault.open("video-file:" + row["id"], row["payload"])
                await self._finish(row["id"], value["name"])
                count += 1
                if count == 3:
                    break

    @asynccontextmanager
    async def uploaded(self, path, duration, before_call=None):
        video_tokens(duration)
        path = Path(path)
        if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE_BYTES:
            raise Rejected("video_file_size_invalid")
        await self.initialize()
        await run_guard(before_call)
        # A client-assigned unique name is persisted BEFORE upload. An interrupted upload
        # can therefore be deleted after restart without listing any other project files.
        name = "files/hv-" + uuid.uuid4().hex
        key = self.store.vault.digest("video-file", name)
        self.active.add(key)
        try:
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO gemini_video_files(id,payload,created) VALUES (?,?,?)",
                    (
                        key,
                        self.store.vault.seal("video-file:" + key, {"name": name}),
                        time.time(),
                    ),
                )
            await run_guard(before_call)
            try:
                async with asyncio.timeout(self.upload_timeout):
                    result = await self.files().upload(
                        file=path,
                        config={
                            "name": name,
                            "mime_type": "video/mp4",
                            "display_name": "hakimi-public-video",
                            "http_options": {"timeout": int(self.upload_timeout * 1000)},
                        },
                    )
                async with asyncio.timeout(self.processing_timeout):
                    while True:
                        await run_guard(before_call)
                        if getattr(result, "name", None) != name:
                            raise Rejected("video_file_reference_invalid")
                        state = getattr(result, "state", None)
                        state = getattr(state, "name", state)
                        if state == "FAILED":
                            raise Rejected("video_file_processing_failed")
                        if state == "ACTIVE":
                            break
                        if state not in {None, "PROCESSING", "STATE_UNSPECIFIED"}:
                            raise Rejected("video_file_state_invalid")
                        await self.sleep(3)
                        await run_guard(before_call)
                        result = await self.files().get(
                            name=name, config={"http_options": {"timeout": 10000}}
                        )
            except TimeoutError:
                raise Rejected("video_file_timeout") from None
            except (Rejected, asyncio.CancelledError):
                raise
            except Exception:
                raise Rejected("video_file_unavailable") from None
            video = ModelVideo(name, getattr(result, "uri", ""), duration, result.mime_type)
            video.validate()
            await run_guard(before_call)
            yield video
        finally:
            try:
                await self._finish(key, name)
            finally:
                self.active.discard(key)
