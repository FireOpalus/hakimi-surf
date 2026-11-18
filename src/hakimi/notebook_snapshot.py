"""Opt-in single-file read-only Drive snapshot; never a general Drive browser."""

import asyncio
import os
import stat
import tempfile
import time

from .bot_server import is_persona_owner
from .drive_client import API, DriveAuth
from .http_json import bounded_json
from .memory_compressor import check_memory_text
from .security import Rejected
from .work_usage import model_work


class SnapshotDrive(DriveAuth):
    scope = "https://www.googleapis.com/auth/drive.readonly"

    async def read(self):
        file_id = self.s.gdrive_notebook_snapshot_file_id
        if not file_id or file_id == "root":
            raise Rejected("notebook_file_required")
        # Exactly this configured ID; no files.list, parents traversal, shortcut or write API.
        url = API + "/" + file_id
        headers = {"Authorization": "Bearer " + await self._access_token()}
        meta = await bounded_json(
            self.http,
            "GET",
            url,
            headers=headers,
            params={
                "fields": "id,mimeType,trashed,capabilities(canDownload)",
                "supportsAllDrives": "true",
            },
            max_bytes=8192,
        )
        if (
            meta.get("id") != file_id
            or meta.get("trashed")
            or not meta.get("capabilities", {}).get("canDownload")
        ):
            raise Rejected("notebook_file_unavailable")
        mime = meta.get("mimeType")
        if mime == "application/vnd.google-apps.document":
            url += "/export"
            params = {"mimeType": "text/plain"}
        elif mime in {"text/plain", "text/markdown", "text/x-markdown"}:
            params = {"alt": "media", "supportsAllDrives": "true"}
        else:
            raise Rejected("notebook_file_type")
        data = bytearray()
        async with self.http.stream(
            "GET", url, headers=headers, params=params, follow_redirects=False
        ) as response:
            if response.status_code != 200:
                raise Rejected("notebook_download_failed")
            async for chunk in response.aiter_bytes():
                data.extend(chunk[: 16384 - len(data)])
                if len(data) >= 16384:
                    break
        return data.decode("utf-8", errors="ignore")[:4000]


class NotebookSnapshot:
    def __init__(self, settings, http, gateway, store):
        self.s, self.ai, self.store = settings, gateway, store
        self.drive = SnapshotDrive(settings, http)
        self.lock = asyncio.Lock()
        self.text = ""
        self.loaded_at = 0.0
        self.last_error = None
        self.next_refresh = 0.0

    def for_message(self, message):
        if (
            self.s.enable_notebook_snapshot
            and message.kind == "private"
            and is_persona_owner(message, self.s)
        ):
            return self.text
        return ""

    def status(self):
        return {
            "enabled": self.s.enable_notebook_snapshot,
            "source": "drive" if self.s.gdrive_notebook_snapshot_file_id else "local",
            "loaded_at": self.loaded_at,
            "last_error": self.last_error,
            "text": self.text if self.s.enable_notebook_snapshot else "",
            "max_chars": 4000,
        }

    def _path(self):
        path = self.s.notebook_snapshot_path.absolute()
        if any(p.is_symlink() for p in [path, *path.parents]):
            raise Rejected("notebook_symlink_forbidden")
        return path

    def _read_local(self):
        path = self._path()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as file:
            if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                raise Rejected("notebook_not_regular")
            return file.read(16384).decode("utf-8", errors="ignore")[:4000]

    async def _screen(self, text):
        check_memory_text(text, self.s)
        if text:
            with model_work("memory.notebook_review", independent=True):
                await self.ai.screen(
                    {"readonly_owner_snapshot": text},
                    self.store.vault.digest("notebook-budget"),
                    purpose="source",
                    budget_lane="admin",
                    audit_source="memory",
                )

    async def refresh(self):
        if not self.s.enable_notebook_snapshot:
            raise Rejected("notebook_disabled")
        async with self.lock:
            self.next_refresh = time.time() + 21600
            try:
                text = (
                    await self.drive.read()
                    if self.s.gdrive_notebook_snapshot_file_id
                    else await asyncio.to_thread(self._read_local)
                )
                await self._screen(text)
                self.text, self.loaded_at, self.last_error = text, time.time(), None
            except asyncio.CancelledError:
                raise
            except Exception:
                # Retain only the previously validated in-memory snapshot, never switch sources.
                self.last_error = "notebook_refresh_failed"
                await self.store.runtime.event(self.last_error)
                raise Rejected(self.last_error) from None
            return self.status()

    def _write_local(self, text):
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, name = tempfile.mkstemp(prefix=".notebook-", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as file:
                file.write(text)
                file.flush()
                os.fsync(file.fileno())
            self._path()
            os.replace(name, path)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    async def update_local(self, text):
        if not self.s.enable_notebook_snapshot:
            raise Rejected("notebook_disabled")
        if self.s.gdrive_notebook_snapshot_file_id:
            raise Rejected("notebook_cloud_readonly")
        if len(text) > 4000:
            raise Rejected("notebook_too_large")
        async with self.lock:
            await self._screen(text)
            await asyncio.to_thread(self._write_local, text)
            self.text, self.loaded_at, self.last_error = text, time.time(), None
            self.next_refresh = time.time() + 21600
            return self.status()

    async def run(self):
        while True:
            if self.s.enable_notebook_snapshot and time.time() >= self.next_refresh:
                try:
                    await self.refresh()
                except Rejected:
                    pass
            await asyncio.sleep(60)
