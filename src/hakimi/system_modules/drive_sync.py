"""Folder-scoped synchronization for installed in-process modules."""

import asyncio
import re
import time
import unicodedata
from contextlib import asynccontextmanager

from ..drive_client import FolderDrive
from ..security import Rejected
from . import SystemModule

MIME_TYPES = {"md": "text/markdown", "txt": "text/plain", "json": "application/json"}
MAX_TEXT_BYTES = 256 * 1024
MAX_BINARY_BYTES = 4 * 1024 * 1024
ERROR_LABELS = frozenset(
    {
        "drive_credentials_invalid",
        "drive_shared_folder_required",
        "drive_request_failed",
        "drive_file_outside_scope",
        "drive_ambiguous_file",
        "drive_token_failed",
        "drive_folder_unavailable",
        "drive_oauth_credentials_invalid",
        "drive_oauth_folder_mismatch",
        "drive_oauth_scope_mismatch",
        "drive_oauth_reauthorization_required",
        "drive_oauth_client_invalid",
        "drive_etag_conflict",
        "drive_etag_required",
        "drive_invalid_response",
        "drive_response_too_large",
        "drive_unavailable",
        "memory_mirror_local_conflict",
        "memory_mirror_unbound",
        "memory_mirror_remote_missing",
        "db_backup_too_large",
        "db_backup_integrity_failed",
        "sessions_backup_too_large",
    }
)


class Module(SystemModule):
    module_id = "drive_sync"
    display_name = "Google Drive 同步"
    description = "为已接入的模块同步文件，按模块记录投递结果。"
    admin_panel = "drive-sync"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.registry = registry
        self.drive = resources.drive
        self.issue = None
        self._locks = {}
        s = resources.settings
        if self.drive is None:
            path = (
                s.gdrive_oauth_credentials_file
                if s.gdrive_auth_mode == "oauth"
                else s.gdrive_service_account_file
            )
            if not s.gdrive_roaming_folder_id or not path or resources.http is None:
                self.issue = "等待配置隔离文件夹和对应凭据"
            else:
                try:
                    self.drive = FolderDrive(
                        s, resources.http, resources.store.vault.digest("diary-writer", s.namespace)
                    )
                except Rejected:
                    self.issue = "隔离文件夹配置无效"

    async def initialize(self):
        async with self.resources.store.transaction():
            await self.resources.store.db.execute(
                "CREATE TABLE IF NOT EXISTS drive_module_sync (module_id TEXT PRIMARY KEY,"
                " last_attempt REAL, last_success REAL, last_error TEXT)"
            )

    async def import_receipt(self, caller, last_success, error):
        """Seed a resident consumer's pre-upgrade receipt without scheduling a transfer."""
        if self.registry.systems.get(caller.module_id) is not caller or not caller.drive_usage:
            raise Rejected("drive_caller_not_registered")
        if last_success is None and not error:
            return
        if error and error not in ERROR_LABELS:
            error = "drive_sync_failed"
        store = self.resources.store
        async with store.transaction():
            await store.db.execute(
                "INSERT OR IGNORE INTO drive_module_sync(module_id,last_success,last_error)"
                " VALUES (?,?,?)",
                (caller.module_id, last_success, error),
            )

    def _access(self, caller):
        if not getattr(caller, "drive_permitted", True):
            return "unauthorized"
        if getattr(caller, "inactive_reason", None):
            return "inactive"
        return "ready" if self.drive is not None else "unconfigured"

    def _check(self, caller):
        ident = getattr(caller, "module_id", None)
        if (
            not isinstance(ident, str)
            or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", ident)
            or not getattr(caller, "drive_usage", None)
        ):
            raise Rejected("drive_caller_not_registered")
        if (
            self.registry.systems.get(ident) is not caller
            and self.registry.get(ident) is not caller
        ):
            raise Rejected("drive_caller_not_registered")
        access = self._access(caller)
        if access != "ready":
            raise Rejected(
                "drive_unconfigured" if access == "unconfigured" else "drive_caller_inactive"
            )
        return ident

    @asynccontextmanager
    async def transfer(self, caller):
        """Serialize a consumer's transfers and keep a content-free receipt across restarts."""
        ident = self._check(caller)
        async with self._locks.setdefault(ident, asyncio.Lock()):
            self._check(caller)
            store = self.resources.store
            async with store.transaction():
                await store.db.execute(
                    "INSERT INTO drive_module_sync(module_id,last_attempt,last_error)"
                    " VALUES (?,?,'unconfirmed')"
                    " ON CONFLICT(module_id) DO UPDATE SET last_attempt=excluded.last_attempt,"
                    "last_error=excluded.last_error",
                    (ident, time.time()),
                )
            try:
                yield
            except Exception as exc:
                reason = (
                    str(exc)
                    if isinstance(exc, Rejected) and str(exc) in ERROR_LABELS
                    else "drive_sync_failed"
                )
                async with store.transaction():
                    await store.db.execute(
                        "UPDATE drive_module_sync SET last_error=? WHERE module_id=?",
                        (reason, ident),
                    )
                raise
            else:
                async with store.transaction():
                    await store.db.execute(
                        "UPDATE drive_module_sync SET last_success=?,last_error=NULL"
                        " WHERE module_id=?",
                        (time.time(), ident),
                    )

    async def sync_file(self, caller, key: str, content: str | bytes) -> str:
        """Upsert a module-owned, authorized payload. Paths and remote file IDs are never inputs."""
        ident = self._check(caller)
        # Versioned group edits and encrypted backups must use their authorized producer.
        if not getattr(caller, "drive_accepts_files", True):
            raise Rejected("drive_specialized_sync_required")
        if not isinstance(key, str) or len(key) > 84:
            raise Rejected("drive_invalid_key")
        key = unicodedata.normalize("NFC", key)
        if not re.fullmatch(r"[^\W_][\w.-]{0,70}\.[A-Za-z0-9]{1,12}", key) or ".." in key:
            raise Rejected("drive_invalid_key")
        mime = MIME_TYPES.get(key.rsplit(".", 1)[1].lower(), "application/octet-stream")
        if mime == "application/octet-stream":
            if not isinstance(content, bytes) or len(content) > MAX_BINARY_BYTES:
                raise Rejected("drive_invalid_content")
            data = content
        else:
            if not isinstance(content, str) or len(content) > MAX_TEXT_BYTES:
                raise Rejected("drive_invalid_content")
            try:
                data = content.encode("utf-8")
            except UnicodeEncodeError:
                raise Rejected("drive_invalid_content") from None
            if len(data) > MAX_TEXT_BYTES:
                raise Rejected("drive_invalid_content")
        async with self.transfer(caller):
            # Shipped consumers with pre-existing remote layouts retain their file identities.
            if self.registry.systems.get(ident) is caller and (
                write := getattr(caller, "write_drive_text", None)
            ):
                if not isinstance(content, str):
                    raise Rejected("drive_invalid_content")
                return await write(self.drive, key, content)
            marker = self.resources.store.vault.digest(
                "module-drive", self.resources.settings.namespace, ident
            )
            return await self.drive.upsert(
                f"module-{ident}--{key}",
                data,
                mime,
                "hakimi_module",
                marker,
            )

    async def status(self):
        if self.issue:
            return {"state": "unconfigured", "detail": self.issue}
        store = self.resources.store
        async with store.lock:
            last = await store.one("SELECT MAX(last_success) FROM drive_module_sync")
        return {"state": "ready", "detail": "已配置隔离文件夹", "last_success": last[0]}

    async def overview(self):
        store = self.resources.store
        async with store.lock:
            receipts = {
                row["module_id"]: row for row in await store.all("SELECT * FROM drive_module_sync")
            }
        consumers = []
        for module in (*self.registry.systems.values(), *self.registry.all()):
            usage = getattr(module, "drive_usage", None)
            if not usage:
                continue
            row = receipts.get(module.module_id)
            lock = self._locks.get(module.module_id)
            consumers.append(
                {
                    "id": module.module_id,
                    "title": module.display_name,
                    "usage": usage,
                    "access": self._access(module),
                    "in_progress": bool(lock and lock.locked()),
                    "last_attempt": row["last_attempt"] if row else None,
                    "last_success": row["last_success"] if row else None,
                    "last_error": row["last_error"] if row else None,
                }
            )
        return {"status": await self.status(), "consumers": consumers}
