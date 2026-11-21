import asyncio
import gzip
import json
import logging
import os
import sqlite3
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from cryptography.fernet import Fernet, InvalidToken

from hakimi import db_backup
from hakimi.db_backup import (
    DRIVE_BACKUP_PROP,
    DatabaseBackup,
    decrypt_backup_bytes,
    derive_backup_fernet,
    encrypt_backup_bytes,
    verify_encrypted_backup,
)
from hakimi.drive_client import API, UPLOAD, FolderDrive
from hakimi.security import Rejected

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
