import asyncio
import gzip
import json
import logging
import os
import sqlite3
import stat
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from cryptography.fernet import Fernet, InvalidToken

from hakimi import db_backup
from hakimi.db_backup import (
    DRIVE_BACKUP_PROP,
    DRIVE_SESSIONS_PROP,
    DatabaseBackup,
    SessionsBackupCollection,
    decrypt_backup_bytes,
    derive_backup_fernet,
    encrypt_backup_bytes,
    verify_encrypted_backup,
    verify_sessions_backup,
)
from hakimi.drive_client import API, UPLOAD, FolderDrive
from hakimi.security import Rejected
from hakimi.session_manager import SessionManager

FOLDER = "backup-folder-123456"


def _ts(iso_local: str) -> float:
    return datetime.fromisoformat(iso_local).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


def _folder_info(**changes):
    return {
        "id": FOLDER,
        "mimeType": "application/vnd.google-apps.folder",
        "driveId": "shared-drive-id",
        "capabilities": {"canAddChildren": True},
        "trashed": False,
    } | changes


def _backup_item(file_id: str, name: str, marker: str, **changes):
    return {
        "id": file_id,
        "name": name,
        "parents": [FOLDER],
        "mimeType": "application/octet-stream",
        "appProperties": {DRIVE_BACKUP_PROP: marker},
        "trashed": False,
    } | changes


def _inspect_snapshot_artifacts(
    enc_path: Path, key: str, work_dir: Path
) -> tuple[int, int, list[str], list[tuple], bytes]:
    dir_mode = stat.S_IMODE(enc_path.parent.stat().st_mode)
    file_mode = stat.S_IMODE(enc_path.stat().st_mode)
    entries = [p.name for p in enc_path.parent.iterdir()]

    gz_bytes = decrypt_backup_bytes(enc_path.read_bytes(), key)
    sqlite_bytes = gzip.decompress(gz_bytes)
    restored = work_dir / "restored.sqlite3"
    restored.write_bytes(sqlite_bytes)
    conn = sqlite3.connect(restored)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchall()
        consent_row = conn.execute(
            "SELECT enabled FROM consent WHERE scope='onebot:40000'"
        ).fetchone()
        meta_row = conn.execute("SELECT value FROM meta WHERE key='namespace'").fetchone()
    finally:
        conn.close()
    assert consent_row is not None and consent_row[0] == 1
    assert meta_row is not None
    return dir_mode, file_mode, entries, integrity, meta_row[0]


def _extract_multipart_payload(raw_body: bytes, content_type: str) -> bytes:
    boundary = content_type.split("boundary=", 1)[1].strip()
    marker = f"--{boundary}".encode()
    parts = raw_body.split(marker)
    file_part = parts[2]
    _, payload_with_crlf = file_part.split(b"\r\n\r\n", 1)
    return payload_with_crlf.removesuffix(b"\r\n")


def test_backup_key_derivation_and_roundtrip(settings):
    key = settings.encryption_key.get_secret_value()
    raw_fernet = Fernet(key.encode("ascii"))
    derived = derive_backup_fernet(key)
    assert isinstance(derived, Fernet)

    sample = b"sqlite-snapshot-payload"
    token = encrypt_backup_bytes(sample, key)
    assert decrypt_backup_bytes(token, key) == sample
    assert decrypt_backup_bytes(token, derived) == sample

    # Key domain separation: primary Fernet key cannot decrypt backup token and vice versa.
    with pytest.raises(InvalidToken):
        raw_fernet.decrypt(token)
    with pytest.raises(Rejected, match="backup_decrypt_failed"):
        decrypt_backup_bytes(raw_fernet.encrypt(sample), key)

    with pytest.raises(Rejected, match="backup_decrypt_failed"):
        decrypt_backup_bytes(token, Fernet.generate_key().decode())
    with pytest.raises(Rejected, match="backup_key_invalid"):
        derive_backup_fernet("not-a-valid-fernet-key")


async def test_snapshot_live_wal_concurrent_writes_and_integrity(settings, store, tmp_path):
    async with store.transaction():
        await store.db.execute("INSERT OR REPLACE INTO consent VALUES ('onebot:40000', 1)")

    journal_row = await store.one("PRAGMA journal_mode")
    assert journal_row is not None and str(journal_row[0]).lower() == "wal"

    stop_writing = asyncio.Event()

    async def writer_loop():
        idx = 0
        while not stop_writing.is_set():
            idx += 1
            async with store.transaction():
                await store.db.execute(
                    "INSERT OR REPLACE INTO counters VALUES (?, ?, ?)",
                    (f"wal-test-{idx % 10}", idx, 9999999999.0),
                )
            await asyncio.sleep(0.005)

    writer = asyncio.create_task(writer_loop())
    try:
        await asyncio.sleep(0.02)
        backup = DatabaseBackup(settings, store)
        result = await backup.snapshot_once(now=_ts("2026-11-17T15:30:00"), upload_drive=False)
    finally:
        stop_writing.set()
        await writer

    assert result["drive_uploaded"] is False
    assert result["drive_file_id"] is None
    enc_path = Path(result["path"])
    assert enc_path.parent == Path(settings.data_dir) / "backups"
    assert enc_path.name == "hakimi-backup-2026-11-17.sqlite3.gz.enc"

    key = settings.encryption_key.get_secret_value()
    dir_mode, file_mode, entries, integrity, ns_cipher = await asyncio.to_thread(
        _inspect_snapshot_artifacts, enc_path, key, tmp_path
    )
    assert dir_mode == 0o700
    assert file_mode == 0o600
    assert entries == [enc_path.name]
    assert integrity == [("ok",)]
    assert store.vault.open("namespace", ns_cipher) == settings.namespace

    verified = await asyncio.to_thread(verify_encrypted_backup, enc_path, key)
    assert verified["ok"] is True
    remaining = await asyncio.to_thread(lambda: [p.name for p in enc_path.parent.iterdir()])
    assert remaining == [enc_path.name]


async def test_local_seven_day_rotation(settings, store):
    async with store.transaction():
        await store.db.execute("INSERT OR REPLACE INTO consent VALUES ('onebot:40000', 1)")

    backups_dir = Path(settings.data_dir) / "backups"

    def seed_files():
        backups_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        for day_num in range(8, 17):
            (backups_dir / f"hakimi-backup-2026-11-{day_num:02d}.sqlite3.gz.enc").write_bytes(
                b"old-backup"
            )
        (backups_dir / "keep-me.txt").write_text("unrelated")

    await asyncio.to_thread(seed_files)

    backup = DatabaseBackup(settings, store, keep_days=7)
    result = await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=False)

    assert sorted(result["pruned_local"]) == [
        "hakimi-backup-2026-11-08.sqlite3.gz.enc",
        "hakimi-backup-2026-11-09.sqlite3.gz.enc",
        "hakimi-backup-2026-11-10.sqlite3.gz.enc",
    ]
    remaining = await asyncio.to_thread(lambda: sorted(p.name for p in backups_dir.iterdir()))
    assert remaining == [
        "hakimi-backup-2026-11-11.sqlite3.gz.enc",
        "hakimi-backup-2026-11-12.sqlite3.gz.enc",
        "hakimi-backup-2026-11-13.sqlite3.gz.enc",
        "hakimi-backup-2026-11-14.sqlite3.gz.enc",
        "hakimi-backup-2026-11-15.sqlite3.gz.enc",
        "hakimi-backup-2026-11-16.sqlite3.gz.enc",
        "hakimi-backup-2026-11-17.sqlite3.gz.enc",
        "keep-me.txt",
    ]


async def test_drive_upload_patch_and_seven_day_pruning(settings, store, tmp_path):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    marker = store.vault.digest("db-backup", roaming_settings.namespace)
    seen_requests = []
    deleted_ids = []
    uploaded_payloads = []
    patched_payloads = []
    phase = {"step": "create"}

    def handler(request: httpx.Request):
        seen_requests.append((request.method, str(request.url)))
        assert request.headers["Authorization"] == "Bearer test-token"
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            q = request.url.params["q"]
            assert f"'{FOLDER}' in parents" in q
            assert f"key='{DRIVE_BACKUP_PROP}' and value='{marker}'" in q
            if phase["step"] == "create":
                files = [
                    _backup_item("id-08", "db-backup-2026-11-08.sqlite3.gz.enc", marker),
                    _backup_item("id-09", "db-backup-2026-11-09.sqlite3.gz.enc", marker),
                    _backup_item("id-16", "db-backup-2026-11-16.sqlite3.gz.enc", marker),
                ]
            else:
                files = [
                    _backup_item("id-16", "db-backup-2026-11-16.sqlite3.gz.enc", marker),
                    _backup_item("id-17", "db-backup-2026-11-17.sqlite3.gz.enc", marker),
                ]
            return httpx.Response(200, json={"files": files})
        if request.method == "GET" and request.url.path.startswith("/drive/v3/files/id-"):
            fid = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(
                200,
                json=_backup_item(fid, f"db-backup-2026-11-{fid[-2:]}.sqlite3.gz.enc", marker),
                headers={"ETag": '"v1"'},
            )
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            assert request.url.params["uploadType"] == "multipart"
            payload = _extract_multipart_payload(request.content, request.headers["Content-Type"])
            uploaded_payloads.append(payload)
            return httpx.Response(
                200,
                json=_backup_item("id-17", "db-backup-2026-11-17.sqlite3.gz.enc", marker),
            )
        if request.method == "PATCH" and request.url.path == "/upload/drive/v3/files/id-17":
            assert request.url.params["uploadType"] == "media"
            assert request.headers["Content-Type"] == "application/octet-stream"
            patched_payloads.append(request.content)
            return httpx.Response(
                200,
                json=_backup_item("id-17", "db-backup-2026-11-17.sqlite3.gz.enc", marker),
            )
        if request.method == "DELETE" and str(request.url).startswith(f"{API}/"):
            file_id = request.url.path.rsplit("/", 1)[-1]
            assert request.headers["If-Match"] == '"v1"'
            deleted_ids.append(file_id)
            return httpx.Response(204)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused-diary-marker")

        async def token():
            return "test-token"

        drive._access_token = token
        backup = DatabaseBackup(roaming_settings, store, drive, keep_days=7)

        res1 = await backup.snapshot_once(now=_ts("2026-11-17T09:00:00"), upload_drive=True)
        assert res1["drive_uploaded"] is True
        assert res1["drive_file_id"] == "id-17"
        assert sorted(deleted_ids) == ["id-08", "id-09"]
        assert sorted(res1["pruned_drive"]) == [
            "db-backup-2026-11-08.sqlite3.gz.enc",
            "db-backup-2026-11-09.sqlite3.gz.enc",
        ]
        assert len(uploaded_payloads) == 1

        def check_uploaded() -> list[tuple]:
            raw = gzip.decompress(
                decrypt_backup_bytes(
                    uploaded_payloads[0], roaming_settings.encryption_key.get_secret_value()
                )
            )
            p = tmp_path / "from-drive.sqlite3"
            p.write_bytes(raw)
            c = sqlite3.connect(p)
            try:
                return c.execute("PRAGMA integrity_check").fetchall()
            finally:
                c.close()

        assert await asyncio.to_thread(check_uploaded) == [("ok",)]

        # Second snapshot on same day must PATCH existing remote file instead of POSTing.
        phase["step"] = "patch"
        deleted_ids.clear()
        res2 = await backup.snapshot_once(now=_ts("2026-11-17T21:00:00"), upload_drive=True)
        assert res2["drive_uploaded"] is True
        assert res2["drive_file_id"] == "id-17"
        assert len(patched_payloads) == 1
        assert deleted_ids == []


async def test_drive_scope_and_size_guards(settings, store, monkeypatch):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    marker = store.vault.digest("db-backup", roaming_settings.namespace)
    mutations = []
    mode = {"case": "foreign_parent"}

    def handler(request: httpx.Request):
        if request.method in {"POST", "PATCH", "DELETE"}:
            mutations.append((request.method, str(request.url)))
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            if mode["case"] == "foreign_parent":
                item = _backup_item(
                    "foreign-1",
                    "db-backup-2026-11-01.sqlite3.gz.enc",
                    marker,
                    parents=["other-folder"],
                )
                return httpx.Response(200, json={"files": [item]})
            if mode["case"] == "wrong_marker":
                item = _backup_item(
                    "wrong-marker-1",
                    "db-backup-2026-11-01.sqlite3.gz.enc",
                    "different-namespace-marker",
                )
                return httpx.Response(200, json={"files": [item]})
            if mode["case"] == "duplicate_day":
                return httpx.Response(
                    200,
                    json={
                        "files": [
                            _backup_item("dup-1", "db-backup-2026-11-17.sqlite3.gz.enc", marker),
                            _backup_item("dup-2", "db-backup-2026-11-17.sqlite3.gz.enc", marker),
                        ]
                    },
                )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def token():
            return "test-token"

        drive._access_token = token
        backup = DatabaseBackup(roaming_settings, store, drive)

        with pytest.raises(Rejected, match="drive_file_outside_scope"):
            await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)

        mode["case"] = "wrong_marker"
        with pytest.raises(Rejected, match="drive_file_outside_scope"):
            await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)

        mode["case"] = "duplicate_day"
        with pytest.raises(Rejected, match="drive_ambiguous_file"):
            await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)

        assert mutations == []

        monkeypatch.setattr(db_backup, "MAX_BACKUP_UPLOAD_BYTES", 64)
        with pytest.raises(Rejected, match="db_backup_too_large"):
            await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)
        assert mutations == []


async def test_run_loop_exception_resilience_and_cancellation(settings, store, monkeypatch, caplog):
    backup = DatabaseBackup(
        settings,
        store,
        startup_delay_seconds=0.0,
        check_interval_seconds=0.01,
    )
    attempts = {"n": 0}
    succeeded = asyncio.Event()
    original_snapshot = backup.snapshot_once

    async def flaky_snapshot(*, now=None, upload_drive=True):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("transient backup failure")
        res = await original_snapshot(now=now, upload_drive=False)
        succeeded.set()
        return res

    monkeypatch.setattr(backup, "snapshot_once", flaky_snapshot)

    with caplog.at_level(logging.WARNING, logger="hakimi"):
        task = asyncio.create_task(backup.run())
        await asyncio.wait_for(succeeded.wait(), timeout=2.0)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert attempts["n"] >= 2
    assert any(rec.getMessage() == "event=db_backup_failed" for rec in caplog.records)
    key = settings.encryption_key.get_secret_value()
    assert key not in caplog.text


async def test_run_loop_skips_when_today_backup_exists(settings, store):
    backup = DatabaseBackup(
        settings,
        store,
        startup_delay_seconds=0.0,
        check_interval_seconds=0.02,
    )
    await backup.snapshot_once(upload_drive=False)
    calls = {"n": 0}

    async def fail_if_called(*args, **kwargs):
        calls["n"] += 1
        raise AssertionError("Should not recreate today's backup")

    backup.snapshot_once = fail_if_called  # type: ignore[method-assign]
    task = asyncio.create_task(backup.run())
    await asyncio.sleep(0.06)
    assert not task.done()
    assert calls["n"] == 0
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def _run_cli_subprocess(
    repo_root: Path, env_file: Path, env: dict[str, str], cli_args: list[str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "backup_db.py"), *cli_args],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.sqlite_durable
async def test_cli_backup_db_script(settings, store, tmp_path):
    env_file = tmp_path / "test.env"
    key = settings.encryption_key.get_secret_value()

    def write_env_and_paths() -> tuple[Path, dict[str, str]]:
        env_file.write_text(
            "\n".join(
                [
                    f"HAKIMI_GEMINI_API_KEY={settings.gemini_api_key.get_secret_value()}",
                    f"HAKIMI_ENCRYPTION_KEY={key}",
                    f"HAKIMI_DATA_DIR={settings.data_dir}",
                    f"BOT_TIMEZONE={settings.bot_timezone}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        root = Path(__file__).resolve().parents[1]
        proc_env = {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(root / "src"),
        }
        return root, proc_env

    repo_root, env = await asyncio.to_thread(write_env_and_paths)

    proc = await asyncio.to_thread(
        _run_cli_subprocess,
        repo_root,
        env_file,
        env,
        ["--env-file", str(env_file), "--verify-decrypt"],
    )
    assert proc.returncode == 0, proc.stderr
    assert key not in proc.stdout and key not in proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["status"] == "ok"
    assert payload["verified"]["ok"] is True
    backup_file = Path(payload["snapshot"]["path"])
    assert await asyncio.to_thread(backup_file.exists)

    proc2 = await asyncio.to_thread(
        _run_cli_subprocess,
        repo_root,
        env_file,
        env,
        ["--env-file", str(env_file), "--verify-decrypt", "--file", str(backup_file)],
    )
    assert proc2.returncode == 0, proc2.stderr
    payload2 = json.loads(proc2.stdout)
    assert payload2["verified"]["ok"] is True

    await asyncio.to_thread(backup_file.write_bytes, b"corrupted-token")
    proc3 = await asyncio.to_thread(
        _run_cli_subprocess,
        repo_root,
        env_file,
        env,
        ["--env-file", str(env_file), "--verify-decrypt", "--file", str(backup_file)],
    )
    assert proc3.returncode != 0
    assert key not in proc3.stderr


async def test_sessions_backup_collection_v3_etagless_delete_and_mismatch(settings, store):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    marker = store.vault.digest("sessions-backup", roaming_settings.namespace)
    old_name = "hakimi-sessions-2026-11-01.tar.gz.enc"
    new_name = "hakimi-sessions-2026-11-17.tar.gz.enc"
    old_item = {
        "id": "sess-old-1",
        "name": old_name,
        "parents": [FOLDER],
        "mimeType": "application/octet-stream",
        "appProperties": {DRIVE_SESSIONS_PROP: marker},
        "trashed": False,
        "md5Checksum": "abcdef0123456789abcdef0123456789",
        "version": "4",
        "headRevisionId": "rev_sess_4",
    }
    mode = {"case": "v3_ok"}
    deletes: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [old_item]})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            return httpx.Response(
                200,
                json={
                    "id": "sess-new-1",
                    "name": new_name,
                    "parents": [FOLDER],
                    "mimeType": "application/octet-stream",
                    "appProperties": {DRIVE_SESSIONS_PROP: marker},
                    "trashed": False,
                },
            )
        if request.method == "GET" and request.url.path == "/drive/v3/files/sess-old-1":
            if mode["case"] == "v3_ok":
                return httpx.Response(200, json=old_item)
            if mode["case"] == "missing_meta":
                return httpx.Response(
                    200,
                    json={
                        "id": "sess-old-1",
                        "name": old_name,
                        "parents": [FOLDER],
                        "mimeType": "application/octet-stream",
                        "appProperties": {DRIVE_SESSIONS_PROP: marker},
                        "trashed": False,
                    },
                )
            if mode["case"] == "etag_412":
                return httpx.Response(200, json=old_item, headers={"ETag": '"sess-v1"'})
        if request.method == "DELETE" and request.url.path == "/drive/v3/files/sess-old-1":
            deletes.append(dict(request.headers))
            return httpx.Response(412 if mode["case"] == "etag_412" else 204)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok
        col = SessionsBackupCollection(drive, marker)

        # Drive v3 without HTTP ETag deletes safely using validated md5Checksum + version/revision.
        fid, pruned = await col.sync_day("2026-11-17", date(2026, 11, 11), b"enc-sessions")
        assert fid == "sess-new-1"
        assert pruned == [old_name]
        assert len(deletes) == 1 and "if-match" not in deletes[0]

        # Missing both ETag and v3 metadata fails safely without sending DELETE.
        mode["case"] = "missing_meta"
        with pytest.raises(Rejected, match="drive_etag_required"):
            await col._delete("sess-old-1", old_name)
        assert len(deletes) == 1

        # HTTP ETag present sends If-Match and maps 412 to drive_etag_conflict.
        mode["case"] = "etag_412"
        with pytest.raises(Rejected, match="drive_etag_conflict"):
            await col._delete("sess-old-1", old_name)
        assert len(deletes) == 2 and deletes[1]["if-match"] == '"sess-v1"'


async def test_snapshot_decouples_main_and_sessions_under_single_transfer(
    settings, store, message, caplog
):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    sessions = SessionManager(roaming_settings, store)
    await sessions.remember(message, "私聊消息", "哈基米回复")

    db_marker = store.vault.digest("db-backup", roaming_settings.namespace)
    sess_marker = store.vault.digest("sessions-backup", roaming_settings.namespace)
    mode = {"fail": "main_prune"}
    uploaded: list[str] = []
    transfer_events: list[str] = []

    @asynccontextmanager
    async def tracked_transfer():
        transfer_events.append("enter")
        try:
            yield
        except Exception as exc:
            transfer_events.append(f"error:{exc}")
            raise
        else:
            transfer_events.append("ok")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            q = request.url.params["q"]
            if DRIVE_BACKUP_PROP in q and mode["fail"] == "main_prune":
                return httpx.Response(
                    200,
                    json={
                        "files": [
                            _backup_item(
                                "main-old", "db-backup-2026-11-01.sqlite3.gz.enc", db_marker
                            )
                        ]
                    },
                )
            if DRIVE_SESSIONS_PROP in q and mode["fail"] == "sessions_upload":
                return httpx.Response(500, text="sessions drive outage")
            return httpx.Response(200, json={"files": []})
        if request.method == "GET" and request.url.path == "/drive/v3/files/main-old":
            # Simulate missing ETag + missing v3 metadata -> drive_etag_required during main prune.
            return httpx.Response(
                200,
                json=_backup_item("main-old", "db-backup-2026-11-01.sqlite3.gz.enc", db_marker),
            )
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content.decode("latin1")
            if "hakimi-sessions-" in raw:
                uploaded.append("sessions")
                return httpx.Response(
                    200,
                    json={
                        "id": "sess-17",
                        "name": "hakimi-sessions-2026-11-17.tar.gz.enc",
                        "parents": [FOLDER],
                        "mimeType": "application/octet-stream",
                        "appProperties": {DRIVE_SESSIONS_PROP: sess_marker},
                        "trashed": False,
                    },
                )
            uploaded.append("main")
            return httpx.Response(
                200,
                json=_backup_item("main-17", "db-backup-2026-11-17.sqlite3.gz.enc", db_marker),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok
        backup = DatabaseBackup(
            roaming_settings, store, drive, transfer=tracked_transfer, keep_days=7
        )

        # Case 1: Main DB Drive prune fails, yet sessions backup STILL runs locally AND uploads!
        with caplog.at_level(logging.WARNING, logger="hakimi"):
            with pytest.raises(Rejected, match="drive_etag_required"):
                await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)

        assert uploaded == ["main", "sessions"]
        assert transfer_events == ["enter", "error:drive_etag_required"]
        assert backup.last_summary is not None
        assert backup.last_summary["drive_uploaded"] is False
        assert backup.last_summary["drive_error"] == "drive_etag_required"
        sb = backup.last_summary["sessions_backup"]
        assert sb is not None
        assert sb["drive_uploaded"] is True
        assert sb["drive_file_id"] == "sess-17"
        assert sb["drive_error"] is None
        sess_file = Path(sb["path"])
        assert await asyncio.to_thread(sess_file.exists)
        verified = await asyncio.to_thread(
            verify_sessions_backup,
            sess_file,
            roaming_settings.encryption_key.get_secret_value(),
        )
        assert verified["ok"] is True and verified["session_count"] == 1

        # Case 2: Main DB succeeds while sessions Drive fails -> error surfaced, main uploaded.
        uploaded.clear()
        transfer_events.clear()
        mode["fail"] = "sessions_upload"
        with pytest.raises(Rejected, match="drive_request_failed"):
            await backup.snapshot_once(now=_ts("2026-11-17T14:00:00"), upload_drive=True)

        assert uploaded == ["main"]
        assert transfer_events == ["enter", "error:drive_request_failed"]
        assert backup.last_summary["drive_uploaded"] is True
        assert backup.last_summary["drive_file_id"] == "main-17"
        assert backup.last_summary["drive_error"] is None
        assert backup.last_summary["sessions_backup"]["drive_uploaded"] is False
        assert backup.last_summary["sessions_backup"]["drive_error"] == "drive_request_failed"


async def test_same_day_drive_retry_backoff_and_run_recovery(settings, store, message):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    sessions = SessionManager(roaming_settings, store)
    await sessions.remember(message, "今天也要同步", "好的喵")

    db_marker = store.vault.digest("db-backup", roaming_settings.namespace)
    sess_marker = store.vault.digest("sessions-backup", roaming_settings.namespace)
    state = {"main_fail": True, "sess_fail": False, "main_uploads": 0, "sess_uploads": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            q = request.url.params["q"]
            if DRIVE_BACKUP_PROP in q and state["main_fail"]:
                return httpx.Response(503, text="main drive temporary error")
            if DRIVE_SESSIONS_PROP in q and state["sess_fail"]:
                return httpx.Response(503, text="sessions drive temporary error")
            return httpx.Response(200, json={"files": []})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content.decode("latin1")
            active_day = (
                datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
                if state.get("use_real_today")
                else "2026-11-17"
            )
            if "hakimi-sessions-" in raw:
                state["sess_uploads"] += 1
                return httpx.Response(
                    200,
                    json={
                        "id": "sess-today",
                        "name": f"hakimi-sessions-{active_day}.tar.gz.enc",
                        "parents": [FOLDER],
                        "mimeType": "application/octet-stream",
                        "appProperties": {DRIVE_SESSIONS_PROP: sess_marker},
                        "trashed": False,
                    },
                )
            state["main_uploads"] += 1
            return httpx.Response(
                200,
                json=_backup_item(
                    "main-today",
                    f"db-backup-{active_day}.sqlite3.gz.enc",
                    db_marker,
                ),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok
        backup = DatabaseBackup(
            roaming_settings,
            store,
            drive,
            retry_backoff_seconds=900.0,
            max_retry_backoff_seconds=3600.0,
        )

        t0 = _ts("2026-11-17T08:00:00")
        assert backup._needs_backup(t0) is True

        # Attempt 1: main Drive fails (local file now exists!), sessions Drive succeeds.
        with pytest.raises(Rejected, match="drive_request_failed"):
            await backup.snapshot_once(now=t0, upload_drive=True)
        local_main = (
            Path(roaming_settings.data_dir) / "backups" / "hakimi-backup-2026-11-17.sqlite3.gz.enc"
        )
        assert await asyncio.to_thread(local_main.exists)

        # Within 900s backoff -> does NOT tight-loop; after 900s -> allows same-day retry!
        assert backup._needs_backup(t0 + 60) is False
        assert backup._needs_backup(t0 + 899) is False
        assert backup._needs_backup(t0 + 900) is True

        # Attempt 2: main Drive still down -> exponential backoff doubles to 1800s.
        with pytest.raises(Rejected, match="drive_request_failed"):
            await backup.snapshot_once(now=t0 + 900, upload_drive=True)
        assert backup._needs_backup(t0 + 900 + 1799) is False
        assert backup._needs_backup(t0 + 900 + 1800) is True

        # Attempt 3: main succeeds, sessions now fails -> still needs same-day retry after backoff!
        state["main_fail"] = False
        state["sess_fail"] = True
        with pytest.raises(Rejected, match="drive_request_failed"):
            await backup.snapshot_once(now=t0 + 2700, upload_drive=True)
        assert backup._needs_backup(t0 + 2700 + 3599) is False
        assert backup._needs_backup(t0 + 2700 + 3600) is True  # capped at max_retry_backoff_seconds

        # Attempt 4: both succeed -> no more backups needed for the rest of the same day!
        state["sess_fail"] = False
        res = await backup.snapshot_once(now=t0 + 6300, upload_drive=True)
        assert res["drive_uploaded"] is True
        assert res["sessions_backup"]["drive_uploaded"] is True
        assert backup._needs_backup(t0 + 7000) is False
        assert backup._needs_backup(t0 + 30000) is False

        # Next calendar day triggers fresh backup immediately.
        assert backup._needs_backup(_ts("2026-11-18T00:05:00")) is True

        # End-to-end run() loop retries on the same day after Drive recovers!
        state["use_real_today"] = True
        state["main_fail"] = True
        state["main_uploads"] = 0
        state["sess_uploads"] = 0
        done = asyncio.Event()
        runner = DatabaseBackup(
            roaming_settings,
            store,
            drive,
            startup_delay_seconds=0.0,
            check_interval_seconds=0.01,
            retry_backoff_seconds=0.01,
        )
        orig_snapshot = runner.snapshot_once
        calls = {"n": 0}

        async def wrapped_snapshot(*, now=None, upload_drive=True):
            calls["n"] += 1
            if calls["n"] == 2:
                state["main_fail"] = False
            res_inner = await orig_snapshot(now=now, upload_drive=upload_drive)
            done.set()
            return res_inner

        runner.snapshot_once = wrapped_snapshot  # type: ignore[method-assign]
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(done.wait(), timeout=3.0)
            await asyncio.sleep(0.05)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert calls["n"] == 2
        assert state["main_uploads"] == 1
        assert state["sess_uploads"] == 2


async def test_run_restart_hydrates_drive_receipt_for_same_day_retry_vs_skip(settings, store):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    async with store.transaction():
        await store.db.execute(
            "CREATE TABLE IF NOT EXISTS drive_module_sync ("
            "module_id TEXT PRIMARY KEY, last_attempt REAL, last_success REAL, last_error TEXT)"
        )

    db_marker = store.vault.digest("db-backup", roaming_settings.namespace)
    today_str = datetime.now(ZoneInfo(roaming_settings.bot_timezone)).date().isoformat()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": []})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            return httpx.Response(
                200,
                json=_backup_item(
                    "main-restart", f"db-backup-{today_str}.sqlite3.gz.enc", db_marker
                ),
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok

        # Seed today's local main backup file, simulating a failed Drive run before restart.
        seeder = DatabaseBackup(roaming_settings, store, drive)
        await seeder.snapshot_once(upload_drive=False)

        now = datetime.now(ZoneInfo(roaming_settings.bot_timezone)).timestamp()
        async with store.transaction():
            await store.db.execute(
                "INSERT OR REPLACE INTO drive_module_sync"
                "(module_id, last_attempt, last_success, last_error)"
                " VALUES ('database_backup', ?, ?, 'drive_etag_required')",
                (now - 60, now - 86400),
            )

        # Fresh instance after restart sees last_error -> retries on the same day!
        restarted_fail = DatabaseBackup(roaming_settings, store, drive)
        await restarted_fail._hydrate_drive_sync_state(now)
        assert restarted_fail._needs_backup(now) is True

        # Once receipt shows today's Drive sync succeeded cleanly -> fresh instance skips!
        async with store.transaction():
            await store.db.execute(
                "UPDATE drive_module_sync SET last_success=?, last_error=NULL"
                " WHERE module_id='database_backup'",
                (now,),
            )
        restarted_ok = DatabaseBackup(roaming_settings, store, drive)
        await restarted_ok._hydrate_drive_sync_state(now)
        assert restarted_ok._needs_backup(now) is False
