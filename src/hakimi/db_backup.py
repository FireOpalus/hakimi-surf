"""Encrypted local and Google Drive SQLite backups using VACUUM INTO."""

import asyncio
import base64
import gzip
import hashlib
import hmac
import io
import logging
import os
import re
import shutil
import sqlite3
import stat
import tarfile
import time
import uuid
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet, InvalidToken

from .config import Settings
from .drive_client import API, FILE_FIELDS, BackupCollection, FolderDrive
from .security import Rejected
from .store import Store

MAX_BACKUP_UPLOAD_BYTES = 33_554_432
MAX_SESSIONS_BACKUP_UPLOAD_BYTES = 33_554_432
LOCAL_BACKUP_PREFIX = "hakimi-backup-"
LOCAL_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PREFIX = "db-backup-"
DRIVE_BACKUP_SUFFIX = ".sqlite3.gz.enc"
DRIVE_BACKUP_PROP = BackupCollection.property_name
LOCAL_SESSIONS_PREFIX = "hakimi-sessions-"
LOCAL_SESSIONS_SUFFIX = ".tar.gz.enc"
DRIVE_SESSIONS_PREFIX = "hakimi-sessions-"
DRIVE_SESSIONS_SUFFIX = ".tar.gz.enc"
DRIVE_SESSIONS_PROP = "hakimi_sessions_backup"

_LOCAL_BACKUP_RE = re.compile(r"^hakimi-backup-(\d{4}-\d{2}-\d{2})\.sqlite3\.gz\.enc$")
_LOCAL_SESSIONS_RE = re.compile(r"^hakimi-sessions-(\d{4}-\d{2}-\d{2})\.tar\.gz\.enc$")

log = logging.getLogger("hakimi")


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


def verify_sessions_backup(
    enc_path: Path,
    encryption_key: str | Fernet,
    *,
    work_dir: Path | None = None,
) -> dict[str, Any]:
    """Decrypt, unpack, and verify SQLite integrity of every session DB in a sessions archive."""
    encrypted = enc_path.read_bytes()
    tar_gz_bytes = decrypt_backup_bytes(encrypted, encryption_key)
    stage_root = work_dir or enc_path.parent
    stage_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp_dir = stage_root / f".verify-sessions-{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    verified_files: list[str] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(tar_gz_bytes), mode="r:gz") as tf:
            for member in tf.getmembers():
                if not member.isfile() or member.issym() or member.islnk():
                    raise Rejected("sessions_backup_invalid_member")
                parts = Path(member.name).parts
                if (
                    len(parts) != 2
                    or not parts[0]
                    or parts[0] in {".", ".."}
                    or parts[1] in {".", ".."}
                    or not parts[1].endswith(".sqlite3")
                ):
                    raise Rejected("sessions_backup_invalid_member")
                extracted = tf.extractfile(member)
                if extracted is None:
                    raise Rejected("sessions_backup_invalid_member")
                raw_db = extracted.read()
                out_db = tmp_dir / f"{len(verified_files)}.sqlite3"
                fd = os.open(out_db, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    view = memoryview(raw_db)
                    written = 0
                    while written < len(view):
                        written += os.write(fd, view[written:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                try:
                    verify_sqlite_integrity(out_db)
                finally:
                    out_db.unlink(missing_ok=True)
                verified_files.append(member.name)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return {
        "ok": True,
        "path": str(enc_path),
        "encrypted_bytes": len(encrypted),
        "tar_gz_bytes": len(tar_gz_bytes),
        "session_count": len(verified_files),
        "sessions": verified_files,
    }


class SessionsBackupCollection:
    """Date-keyed, encrypted sessions tar.gz backups in one scoped Drive folder."""

    property_name = DRIVE_SESSIONS_PROP
    names = _LOCAL_SESSIONS_RE

    def __init__(self, drive: FolderDrive, marker: str):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", marker):
            raise Rejected("drive_backup_marker_invalid")
        self._drive, self.marker = drive, marker

    async def _delete(self, file_id: str, filename: str) -> None:
        if not self.names.fullmatch(filename) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", file_id):
            raise Rejected("drive_file_outside_scope")
        current, etag = await self._drive._json(
            "GET", f"{API}/{file_id}", params={"fields": FILE_FIELDS, "supportsAllDrives": "true"}
        )
        if (
            self._drive.validate_owned_file(
                current, filename, "application/octet-stream", self.property_name, self.marker
            )
            != file_id
        ):
            raise Rejected("drive_file_outside_scope")
        if not etag:
            raise Rejected("drive_etag_required")
        token = await self._drive._access_token()
        async with self._drive.http.stream(
            "DELETE",
            f"{API}/{file_id}",
            headers={"Authorization": f"Bearer {token}", "If-Match": etag},
            params={"supportsAllDrives": "true"},
            follow_redirects=False,
        ) as response:
            if response.status_code not in {200, 204, 404}:
                raise Rejected("drive_request_failed")

    async def _list(self) -> list[dict[str, Any]]:
        query = (
            f"'{self._drive.folder}' in parents and trashed = false "
            f"and appProperties has {{ key='{self.property_name}' and value='{self.marker}' }}"
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
            listing, _ = await self._drive._json("GET", API, params=params)
            files = listing.get("files")
            if not isinstance(files, list) or listing.get("incompleteSearch"):
                raise Rejected("drive_ambiguous_file")
            for item in files:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                    raise Rejected("drive_invalid_response")
                self._drive.validate_owned_file(
                    item,
                    item.get("name"),
                    "application/octet-stream",
                    self.property_name,
                    self.marker,
                )
                items.append(item)
            next_token = listing.get("nextPageToken")
            if not next_token:
                return items
            if not isinstance(next_token, str):
                raise Rejected("drive_invalid_response")
            page_token = next_token
        raise Rejected("drive_ambiguous_file")

    async def sync_day(
        self, day_str: str, cutoff_date: date, enc_bytes: bytes
    ) -> tuple[str, list[str]]:
        await self._drive.validate_folder()
        files = await self._list()
        remote_name = (
            f"{DRIVE_SESSIONS_PREFIX}{date.fromisoformat(day_str).isoformat()}"
            f"{DRIVE_SESSIONS_SUFFIX}"
        )
        matches = [item for item in files if item.get("name") == remote_name]
        file_id = await self._drive.write_listed(
            remote_name,
            enc_bytes,
            "application/octet-stream",
            self.property_name,
            self.marker,
            matches,
        )
        pruned_drive: list[str] = []
        for item in files:
            item_id, item_name = item["id"], item["name"]
            match = self.names.fullmatch(item_name)
            if not match:
                continue
            try:
                item_date = date.fromisoformat(match.group(1))
            except ValueError:
                continue
            if item_date < cutoff_date and item_id != file_id:
                await self._delete(item_id, item_name)
                pruned_drive.append(item_name)
        return file_id, pruned_drive


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
        self.sessions_drive = (
            SessionsBackupCollection(
                drive, store.vault.digest("sessions-backup", settings.namespace)
            )
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

    def _prune_local_pattern(
        self, backups_dir: Path, pattern: re.Pattern[str], cutoff_date: date
    ) -> list[str]:
        pruned: list[str] = []
        for entry in sorted(backups_dir.iterdir()):
            if entry.is_symlink() or not entry.is_file():
                continue
            match = pattern.fullmatch(entry.name)
            if not match:
                continue
            try:
                file_day = date.fromisoformat(match.group(1))
            except ValueError:
                continue
            if file_day < cutoff_date:
                entry.unlink(missing_ok=True)
                pruned.append(entry.name)
        return pruned

    def _collect_session_dbs(self) -> list[tuple[str, Path]]:
        sessions_root = Path(self.s.data_dir) / "sessions"
        if not sessions_root.exists() or sessions_root.is_symlink() or not sessions_root.is_dir():
            return []
        found: list[tuple[str, Path]] = []
        for kind_dir in sorted(sessions_root.iterdir()):
            if kind_dir.is_symlink() or not kind_dir.is_dir():
                continue
            for db_file in sorted(kind_dir.iterdir()):
                if db_file.is_symlink() or not db_file.name.endswith(".sqlite3"):
                    continue
                try:
                    st = db_file.stat(follow_symlinks=False)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                found.append((f"{kind_dir.name}/{db_file.name}", db_file))
        return found

    def _create_sessions_backup(
        self, day_str: str, cutoff_date: date
    ) -> tuple[dict[str, Any] | None, bytes | None]:
        backups_dir = self.backup_dir
        backups_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(backups_dir, 0o700)
        session_dbs = self._collect_session_dbs()
        pruned_local = self._prune_local_pattern(backups_dir, _LOCAL_SESSIONS_RE, cutoff_date)
        if not session_dbs:
            return None, None

        stage_dir = backups_dir / f".sessions-stage-{day_str}-{uuid.uuid4().hex}"
        stage_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        tar_buf = io.BytesIO()
        session_count = 0
        try:
            with gzip.GzipFile(fileobj=tar_buf, mode="wb", mtime=0) as gz:
                with tarfile.open(fileobj=gz, mode="w") as tf:
                    for rel_name, db_file in session_dbs:
                        tmp_db = stage_dir / f"sess-{session_count}.sqlite3"
                        try:
                            conn = sqlite3.connect(db_file, timeout=10.0)
                            try:
                                conn.execute("PRAGMA busy_timeout=10000")
                                conn.execute("VACUUM INTO ?", (str(tmp_db),))
                            finally:
                                conn.close()
                            verify_sqlite_integrity(tmp_db)
                            data = tmp_db.read_bytes()
                            info = tarfile.TarInfo(name=rel_name)
                            info.size = len(data)
                            info.mode = 0o600
                            info.mtime = 0
                            tf.addfile(info, io.BytesIO(data))
                            session_count += 1
                        finally:
                            tmp_db.unlink(missing_ok=True)
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)

        tar_gz_bytes = tar_buf.getvalue()
        enc_bytes = encrypt_backup_bytes(tar_gz_bytes, self.s.encryption_key.get_secret_value())
        final_path = backups_dir / f"{LOCAL_SESSIONS_PREFIX}{day_str}{LOCAL_SESSIONS_SUFFIX}"
        _atomic_write_private(final_path, enc_bytes)
        summary: dict[str, Any] = {
            "day": day_str,
            "path": str(final_path),
            "filename": final_path.name,
            "session_count": session_count,
            "tar_gz_bytes": len(tar_gz_bytes),
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

    async def _sync_sessions_drive_backup(self, day_str, cutoff_date, enc_bytes):
        if len(enc_bytes) > MAX_SESSIONS_BACKUP_UPLOAD_BYTES:
            raise Rejected("sessions_backup_too_large")
        if self.sessions_drive is None:
            raise Rejected("drive_unavailable")
        return await self.sessions_drive.sync_day(day_str, cutoff_date, enc_bytes)

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

            sessions_summary, sessions_enc = await asyncio.to_thread(
                self._create_sessions_backup, day_str, cutoff_date
            )
            if sessions_summary is not None and sessions_enc is not None:
                sess_uploaded = False
                sess_file_id: str | None = None
                sess_pruned_drive: list[str] = []
                sess_skip_reason: str | None = None
                if upload_drive and self.sessions_drive is not None and self.s.roaming_enabled:
                    if len(sessions_enc) > MAX_SESSIONS_BACKUP_UPLOAD_BYTES:
                        sess_skip_reason = "sessions_backup_too_large"
                        log.warning(
                            "event=sessions_backup_too_large bytes=%d limit=%d",
                            len(sessions_enc),
                            MAX_SESSIONS_BACKUP_UPLOAD_BYTES,
                        )
                    else:
                        async with self.transfer():
                            (
                                sess_file_id,
                                sess_pruned_drive,
                            ) = await self._sync_sessions_drive_backup(
                                day_str, cutoff_date, sessions_enc
                            )
                        sess_uploaded = True
                sessions_summary["drive_uploaded"] = sess_uploaded
                sessions_summary["drive_file_id"] = sess_file_id
                sessions_summary["pruned_drive"] = sess_pruned_drive
                sessions_summary["drive_skipped_reason"] = sess_skip_reason
            summary["sessions_backup"] = sessions_summary
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
