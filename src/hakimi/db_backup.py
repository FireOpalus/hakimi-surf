"""Encrypted local and Google Drive SQLite backups using VACUUM INTO."""

import asyncio
import base64
import gzip
import hashlib
import hmac
import logging
import os
import re
import sqlite3
import time
import uuid
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken

from .config import Settings
from .drive_client import BackupCollection, FolderDrive
from .security import Rejected
from .store import Store

MAX_BACKUP_UPLOAD_BYTES = 33_554_432
LOCAL_BACKUP_PREFIX = "hakimi-backup-"
LOCAL_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PREFIX = "db-backup-"
DRIVE_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PROP = BackupCollection.property_name

_LOCAL_BACKUP_RE = re.compile(r"^hakimi-backup-(\d{4}-\d{2}-\d{2})\.sqlite3\.gz\.enc$")


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
        transfer=nullcontext,
        interval_seconds: int = 86400,
        keep_days: int = 7,
        startup_delay_seconds: float = 60.0,
        check_interval_seconds: float = 3600.0,
    ):
        self.s = settings
        self.store = store
        self.drive = (
            BackupCollection(drive, store.vault.digest("db-backup", settings.namespace))
            if drive
            else None
        )
        self.transfer = transfer
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

    def _create_local_backup(self, day_str: str, cutoff_date: date) -> tuple[dict[str, Any], bytes]:
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

    async def _sync_drive_backup(self, day_str, cutoff_date, enc_bytes):
        if len(enc_bytes) > MAX_BACKUP_UPLOAD_BYTES:
            raise Rejected("db_backup_too_large")
        if self.drive is None:
            raise Rejected("drive_unavailable")
        return await self.drive.sync_day(day_str, cutoff_date, enc_bytes)

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
                async with self.transfer():
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
