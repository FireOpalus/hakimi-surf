"""Encrypted local and Google Drive SQLite backups using VACUUM INTO."""

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken

from .config import Settings
from .drive_client import API, FILE_FIELDS, UPLOAD, FolderDrive
from .security import Rejected
from .store import Store

MAX_BACKUP_UPLOAD_BYTES = 33_554_432
LOCAL_BACKUP_PREFIX = "hakimi-backup-"
LOCAL_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PREFIX = "db-backup-"
DRIVE_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PROP = "hakimi_db_backup"

_LOCAL_BACKUP_RE = re.compile(r"^hakimi-backup-(\d{4}-\d{2}-\d{2})\.sqlite3\.gz\.enc$")
_DRIVE_BACKUP_RE = re.compile(r"^db-backup-(\d{4}-\d{2}-\d{2})\.sqlite3\.gz\.enc$")
_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_MARKER_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def derive_backup_fernet(encryption_key: str) -> Fernet:
    """Derive a dedicated Fernet cipher for database backups via HMAC-SHA256."""
    if not isinstance(encryption_key, str) or not encryption_key:
        raise Rejected("backup_key_invalid")
    try:
        key_bytes = encryption_key.encode("ascii")
        Fernet(key_bytes)
        raw_key = base64.urlsafe_b64decode(key_bytes)
    except (ValueError, TypeError, UnicodeEncodeError) as exc:
        raise Rejected("backup_key_invalid") from exc
    derived = hmac.digest(raw_key, b"hakimi-db-backup-v1", "sha256")
    return Fernet(base64.urlsafe_b64encode(derived))


def encrypt_backup_bytes(data: bytes, encryption_key: str | Fernet) -> bytes:
    """Encrypt compressed SQLite backup bytes with the derived backup Fernet key."""
    fernet = (
        encryption_key
        if isinstance(encryption_key, Fernet)
        else derive_backup_fernet(encryption_key)
    )
    return fernet.encrypt(data)


def decrypt_backup_bytes(token: bytes, encryption_key: str | Fernet) -> bytes:
    """Decrypt backup bytes using the derived backup Fernet key."""
    fernet = (
        encryption_key
        if isinstance(encryption_key, Fernet)
        else derive_backup_fernet(encryption_key)
    )
    try:
        return fernet.decrypt(token)
    except InvalidToken as exc:
        raise Rejected("backup_decrypt_failed") from exc


def verify_sqlite_integrity(db_path: Path) -> None:
    """Verify SQLite database file integrity via PRAGMA integrity_check."""
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        conn.execute("PRAGMA busy_timeout=10000")
        rows = conn.execute("PRAGMA integrity_check").fetchall()
    finally:
        conn.close()
    if rows != [("ok",)]:
        raise Rejected("db_backup_integrity_failed")


def verify_encrypted_backup(
    enc_path: Path,
    encryption_key: str | Fernet,
    *,
    work_dir: Path | None = None,
) -> dict[str, Any]:
    """Decrypt, decompress, and check SQLite integrity of a backup file without using /tmp."""
    encrypted = enc_path.read_bytes()
    compressed = decrypt_backup_bytes(encrypted, encryption_key)
    raw_sqlite = gzip.decompress(compressed)
    stage_dir = work_dir or enc_path.parent
    stage_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp_db = stage_dir / f".verify-{uuid.uuid4().hex}.sqlite3"
    try:
        fd = os.open(tmp_db, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            view = memoryview(raw_sqlite)
            written = 0
            while written < len(view):
                written += os.write(fd, view[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
        verify_sqlite_integrity(tmp_db)
    finally:
        tmp_db.unlink(missing_ok=True)
    return {
        "ok": True,
        "path": str(enc_path),
        "encrypted_bytes": len(encrypted),
        "gzip_bytes": len(compressed),
        "sqlite_bytes": len(raw_sqlite),
    }


def _atomic_write_private(target: Path, data: bytes) -> None:
    """Atomically write bytes with 0600 permissions using a sibling staging file."""
    tmp_path = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
    try:
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            view = memoryview(data)
            written = 0
            while written < len(view):
                written += os.write(fd, view[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp_path, target)
        os.chmod(target, 0o600)
    finally:
        tmp_path.unlink(missing_ok=True)


class DatabaseBackup:
    """Daily encrypted SQLite backup worker with local and Drive retention."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        drive: FolderDrive | None = None,
        *,
        interval_seconds: int = 86400,
        keep_days: int = 7,
        startup_delay_seconds: float = 60.0,
        check_interval_seconds: float = 3600.0,
    ):
        self.s = settings
        self.store = store
        self.drive = drive
        self.interval_seconds = interval_seconds
        self.keep_days = max(1, int(keep_days))
        self.startup_delay_seconds = startup_delay_seconds
        self.check_interval_seconds = check_interval_seconds
        self.zone = ZoneInfo(settings.bot_timezone)
        self._lock = asyncio.Lock()

    @property
    def backup_dir(self) -> Path:
        return Path(self.s.data_dir) / "backups"

    def _local_date(self, ts: float) -> date:
        return datetime.fromtimestamp(ts, UTC).astimezone(self.zone).date()

    def _needs_backup(self, ts: float) -> bool:
        day_str = self._local_date(ts).isoformat()
        target = self.backup_dir / f"{LOCAL_BACKUP_PREFIX}{day_str}{LOCAL_BACKUP_SUFFIX}"
        if not target.exists():
            return True
        stat_result = target.stat()
        if stat_result.st_size == 0:
            return True
        if self.interval_seconds < 86400:
            return (ts - stat_result.st_mtime) >= self.interval_seconds
        return False

    def _create_local_backup(
        self, day_str: str, cutoff_date: date
    ) -> tuple[dict[str, Any], bytes]:
        db_path = Path(self.s.data_dir) / "hakimi.sqlite3"
        if not db_path.exists():
            raise FileNotFoundError(db_path)

        backups_dir = self.backup_dir
        backups_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backups_dir, 0o700)

        tmp_snapshot = backups_dir / f".snapshot-{day_str}-{uuid.uuid4().hex}.sqlite3"
        try:
            conn = sqlite3.connect(db_path, timeout=10.0)
            try:
                conn.execute("PRAGMA busy_timeout=10000")
                conn.execute("VACUUM INTO ?", (str(tmp_snapshot),))
            finally:
                conn.close()
            verify_sqlite_integrity(tmp_snapshot)
            raw_bytes = tmp_snapshot.read_bytes()
        finally:
            tmp_snapshot.unlink(missing_ok=True)

        gz_bytes = gzip.compress(raw_bytes, mtime=0)
        enc_bytes = encrypt_backup_bytes(gz_bytes, self.s.encryption_key.get_secret_value())
        final_path = backups_dir / f"{LOCAL_BACKUP_PREFIX}{day_str}{LOCAL_BACKUP_SUFFIX}"
        _atomic_write_private(final_path, enc_bytes)

        pruned_local: list[str] = []
        for entry in sorted(backups_dir.iterdir()):
            if not entry.is_file():
                continue
            match = _LOCAL_BACKUP_RE.fullmatch(entry.name)
            if not match:
                continue
            try:
                file_day = date.fromisoformat(match.group(1))
            except ValueError:
                continue
            if file_day < cutoff_date:
                entry.unlink(missing_ok=True)
                pruned_local.append(entry.name)

        summary: dict[str, Any] = {
            "day": day_str,
            "path": str(final_path),
            "filename": final_path.name,
            "raw_bytes": len(raw_bytes),
            "gzip_bytes": len(gz_bytes),
            "encrypted_bytes": len(enc_bytes),
            "sha256": hashlib.sha256(enc_bytes).hexdigest(),
            "pruned_local": pruned_local,
        }
        return summary, enc_bytes

    def _validate_drive_item(
        self,
        item: dict[str, Any],
        marker: str,
        expected_name: str | None = None,
    ) -> str:
        if not isinstance(item, dict) or self.drive is None:
            raise Rejected("drive_file_outside_scope")
        file_id = item.get("id", "")
        props = item.get("appProperties")
        if (
            not isinstance(file_id, str)
            or not _DRIVE_ID_RE.fullmatch(file_id)
            or item.get("parents") != [self.drive.folder]
            or item.get("trashed", False)
            or not isinstance(props, dict)
            or props.get(DRIVE_BACKUP_PROP) != marker
            or item.get("mimeType") != "application/octet-stream"
            or (expected_name is not None and item.get("name") != expected_name)
        ):
            raise Rejected("drive_file_outside_scope")
        return file_id

    async def _drive_request_json(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        if self.drive is None:
            raise Rejected("drive_unavailable")
        token = await self.drive._access_token()
        headers = {"Authorization": f"Bearer {token}", **kwargs.pop("headers", {})}
        async with self.drive.http.stream(
            method, url, headers=headers, follow_redirects=False, **kwargs
        ) as response:
            if response.status_code not in {200, 201}:
                raise Rejected("drive_request_failed")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 65536:
                    raise Rejected("drive_response_too_large")
            try:
                value = json.loads(body)
            except ValueError as exc:
                raise Rejected("drive_invalid_response") from exc
            if not isinstance(value, dict):
                raise Rejected("drive_invalid_response")
            return value

    async def _drive_delete_file(self, file_id: str) -> None:
        if self.drive is None:
            raise Rejected("drive_unavailable")
        token = await self.drive._access_token()
        async with self.drive.http.stream(
            "DELETE",
            f"{API}/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
            params={"supportsAllDrives": "true"},
            follow_redirects=False,
        ) as response:
            if response.status_code not in {200, 204, 404}:
                raise Rejected("drive_request_failed")

    async def _drive_list_backups(self, marker: str) -> list[dict[str, Any]]:
        if self.drive is None:
            raise Rejected("drive_unavailable")
        query = (
            f"'{self.drive.folder}' in parents and trashed = false "
            f"and appProperties has {{ key='{DRIVE_BACKUP_PROP}' and value='{marker}' }}"
        )
        items: list[dict[str, Any]] = []
        page_token: str | None = None
        for _ in range(20):
            params: dict[str, str] = {
                "q": query,
                "fields": f"nextPageToken,incompleteSearch,files({FILE_FIELDS})",
                "pageSize": "100",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                "corpora": "user",
            }
            if page_token:
                params["pageToken"] = page_token
            listing = await self._drive_request_json("GET", API, params=params)
            files = listing.get("files")
            if not isinstance(files, list) or listing.get("incompleteSearch"):
                raise Rejected("drive_ambiguous_file")
            for item in files:
                self._validate_drive_item(item, marker)
                items.append(item)
            next_token = listing.get("nextPageToken")
            if not next_token:
                return items
            if not isinstance(next_token, str):
                raise Rejected("drive_invalid_response")
            page_token = next_token
        raise Rejected("drive_ambiguous_file")

    async def _sync_drive_backup(
        self, day_str: str, cutoff_date: date, enc_bytes: bytes
    ) -> tuple[str, list[str]]:
        if len(enc_bytes) > MAX_BACKUP_UPLOAD_BYTES:
            raise Rejected("db_backup_too_large")
        if self.drive is None:
            raise Rejected("drive_unavailable")

        await self.drive.validate_folder()
        await self.drive._access_token()
        marker = self.store.vault.digest("db-backup", self.s.namespace)
        if not _MARKER_RE.fullmatch(marker):
            raise Rejected("drive_backup_marker_invalid")

        files = await self._drive_list_backups(marker)
        remote_name = f"{DRIVE_BACKUP_PREFIX}{day_str}{DRIVE_BACKUP_SUFFIX}"
        matches = [item for item in files if item.get("name") == remote_name]
        if len(matches) > 1:
            raise Rejected("drive_ambiguous_file")

        if matches:
            target_id = self._validate_drive_item(matches[0], marker, remote_name)
            result = await self._drive_request_json(
                "PATCH",
                f"{UPLOAD}/{target_id}",
                params={
                    "uploadType": "media",
                    "supportsAllDrives": "true",
                    "fields": FILE_FIELDS,
                },
                headers={"Content-Type": "application/octet-stream"},
                content=enc_bytes,
            )
        else:
            metadata = {
                "name": remote_name,
                "mimeType": "application/octet-stream",
                "parents": [self.drive.folder],
                "appProperties": {DRIVE_BACKUP_PROP: marker},
            }
            boundary = f"hakimi_{uuid.uuid4().hex}"
            body = (
                (
                    f"--{boundary}\r\n"
                    "Content-Type: application/json; charset=UTF-8\r\n\r\n"
                    + json.dumps(metadata, ensure_ascii=True)
                    + f"\r\n--{boundary}\r\n"
                    "Content-Type: application/octet-stream\r\n\r\n"
                ).encode()
                + enc_bytes
                + f"\r\n--{boundary}--\r\n".encode()
            )
            result = await self._drive_request_json(
                "POST",
                UPLOAD,
                params={
                    "uploadType": "multipart",
                    "supportsAllDrives": "true",
                    "fields": FILE_FIELDS,
                },
                headers={"Content-Type": f"multipart/related; boundary={boundary}"},
                content=body,
            )

        file_id = self._validate_drive_item(result, marker, remote_name)

        pruned_drive: list[str] = []
        for item in files:
            item_id = self._validate_drive_item(item, marker)
            item_name = str(item.get("name", ""))
            match = _DRIVE_BACKUP_RE.fullmatch(item_name)
            if not match:
                continue
            try:
                item_date = date.fromisoformat(match.group(1))
            except ValueError:
                continue
            if item_date < cutoff_date and item_id != file_id:
                await self._drive_delete_file(item_id)
                pruned_drive.append(item_name)

        return file_id, pruned_drive

    async def snapshot_once(self, now: float | None = None, upload_drive: bool = True) -> dict:
        """Create a verified encrypted SQLite snapshot and optionally sync it to Google Drive."""
        ts = time.time() if now is None else float(now)
        today = self._local_date(ts)
        day_str = today.isoformat()
        cutoff_date = today - timedelta(days=self.keep_days - 1)

        async with self._lock:
            summary, enc_bytes = await asyncio.to_thread(
                self._create_local_backup, day_str, cutoff_date
            )
            drive_uploaded = False
            drive_file_id: str | None = None
            pruned_drive: list[str] = []
            if upload_drive and self.drive is not None and self.s.roaming_enabled:
                drive_file_id, pruned_drive = await self._sync_drive_backup(
                    day_str, cutoff_date, enc_bytes
                )
                drive_uploaded = True

            summary["drive_uploaded"] = drive_uploaded
            summary["drive_file_id"] = drive_file_id
            summary["pruned_drive"] = pruned_drive
            return summary

    async def run(self) -> None:
        """Run periodic daily backups without ever returning early on transient errors."""
        await asyncio.sleep(self.startup_delay_seconds)
        while True:
            try:
                now = time.time()
                if await asyncio.to_thread(self._needs_backup, now):
                    await self.snapshot_once(now=now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("hakimi").warning("event=db_backup_failed")
            await asyncio.sleep(self.check_interval_seconds)
