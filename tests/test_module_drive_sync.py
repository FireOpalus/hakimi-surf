import asyncio
import gzip
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from hakimi.db_backup import decrypt_backup_bytes
from hakimi.modules import BaseModule
from hakimi.security import Rejected
from hakimi.system_modules import SystemModule
from hakimi.system_modules.drive_sync import MAX_BINARY_BYTES, MAX_TEXT_BYTES
from tests.test_admin import admin_api as admin_api
from tests.test_drive_client import FOLDER, drive_for, folder_info


class Archive(BaseModule):
    module_id = "weather_archive"
    display_name = "天气档案"
    drive_usage = "公开天气报告"

    async def _raw_sensor_summary(self):
        return ""


class Relay(SystemModule):
    module_id = "observations"
    display_name = "观测档案"
    drive_usage = "公开观测记录"


async def test_registered_modules_share_folder_without_sharing_files(admin_api):
    app, _, gateway = admin_api
    svc = app.state.service
    files = {}
    changes = []

    async def handle(request):
        if request.url.path.endswith("/" + FOLDER):
            return httpx.Response(200, json=folder_info())
        if request.method == "GET" and request.url.path.endswith("/files"):
            q = request.url.params["q"]
            assert f"'{FOLDER}' in parents and trashed = false" in q
            assert "key='hakimi_module'" in q
            result = [item for item in files.values() if f"name = '{item['name']}'" in q]
            await asyncio.sleep(0)  # expose competing creates
            return httpx.Response(200, json={"files": result})
        if request.method == "POST":
            meta = json.loads(request.content.split(b"\r\n\r\n")[1].split(b"\r\n")[0])
            assert meta["parents"] == [FOLDER]
            fid = str(len(files) + 1)
            files[fid] = meta | {"id": fid}
            changes.append(("create", fid))
            return httpx.Response(200, json=files[fid])
        fid = request.url.path.rsplit("/", 1)[-1]
        if request.method == "GET":
            return httpx.Response(200, json=files[fid], headers={"ETag": '"v1"'})
        assert request.method == "PATCH" and request.headers["If-Match"] == '"v1"'
        changes.append(("update", fid))
        return httpx.Response(200, json=files[fid])

    drive, http = await drive_for(svc.s, handle)
    provider = svc.drive_sync
    # Rebuild against an isolated test folder; use the same registry as the real app.
    provider = type(provider)(replace(provider.resources, drive=drive), svc.modules)
    archive = Archive(svc.s)
    svc.modules.register(archive)
    relay = Relay(provider.resources, svc.modules)
    svc.modules.register_system(relay)
    async with http:
        results = await asyncio.gather(
            provider.sync_file(archive, "daily.json", '{"day":1}'),
            provider.sync_file(archive, "daily.json", '{"day":2}'),
        )
        other = await provider.sync_file(relay, "daily.json", '{"day":3}')
        assert results == ["1", "1"] and other == "2"
        assert changes == [("create", "1"), ("update", "1"), ("create", "2")]
        assert files["1"]["name"] == "module-weather_archive--daily.json"
        assert files["2"]["appProperties"] != files["1"]["appProperties"]
        assert files["1"]["mimeType"] == "application/json"
        binary_id = await provider.sync_file(archive, "逐时预报.bin", b"\x00\xff")
        assert files[binary_id]["mimeType"] == "application/octet-stream"
        assert files[binary_id]["name"] == "module-weather_archive--逐时预报.bin"
        report = await provider.overview()
        assert {r["id"] for r in report["consumers"] if r["last_success"]} == {
            archive.module_id,
            relay.module_id,
        }
        assert "daily.json" not in json.dumps(report)
        assert not gateway.calls


async def test_sync_checks_identity_scope_and_permissions_before_io(admin_api):
    app, client, _ = admin_api
    svc = app.state.service
    provider = svc.drive_sync
    archive = Archive(svc.s)
    svc.modules.register(archive)
    with pytest.raises(Rejected, match="drive_unconfigured"):
        await provider.sync_file(archive, "note.md", "text")
    provider = type(provider)(replace(provider.resources, drive=object()), svc.modules)
    for caller in (
        Archive(svc.s),
        svc.modules.get("qq_social"),
        svc.modules.systems["owner_notebook"],
    ):
        with pytest.raises(Rejected, match="drive_caller_not_registered"):
            await provider.sync_file(caller, "note.md", "text")
    with pytest.raises(Rejected, match="drive_caller_inactive"):
        await provider.sync_file(
            svc.modules.systems["surf_mirror"], "surf-mirror-latest.md", "text"
        )
    archive.enabled = False
    with pytest.raises(Rejected, match="drive_caller_inactive"):
        await provider.sync_file(archive, "note.md", "text")
    archive.enabled = True
    for key in (
        "../private.md",
        "two/files.txt",
        "bad'query.json",
        "https://example.com/n.txt",
        "noextension",
    ):
        with pytest.raises(Rejected, match="drive_invalid_key"):
            await provider.sync_file(archive, key, "text")
    with pytest.raises(Rejected, match="drive_invalid_content"):
        await provider.sync_file(archive, "large.txt", "云" * (MAX_TEXT_BYTES // 3 + 1))
    with pytest.raises(Rejected, match="drive_invalid_content"):
        await provider.sync_file(archive, "huge.bin", b"x" * (MAX_BINARY_BYTES + 1))
    assert not await svc.store.all("SELECT * FROM drive_module_sync")
    for url in ("/admin/api/drive-sync", "/admin/api/diaries/status"):
        assert (await client.get(url, headers={"Authorization": ""})).status_code == 401
    assert (
        await client.post("/admin/api/drive-sync", json={"key": "arbitrary"})
    ).status_code == 405
    svc.notebook.text = "test-private-notebook-content"
    for url in ("/admin/api/drive-sync", "/admin/api/system-modules"):
        assert svc.notebook.text not in (await client.get(url)).text


async def test_failure_interruption_restart_and_encrypted_backup(admin_api):
    app, client, _ = admin_api
    svc = app.state.service
    archive = Archive(svc.s)
    svc.modules.register(archive)
    provider = svc.drive_sync

    class FakeDrive:
        async def upsert(self, *args):
            raise Rejected("sensitive-remote-body")

    live = type(provider)(replace(provider.resources, drive=FakeDrive()), svc.modules)
    with pytest.raises(Rejected):
        await live.sync_file(archive, "secret-name.txt", "private-file-content")
    row = next(
        r
        for r in (await client.get("/admin/api/drive-sync")).json()["consumers"]
        if r["id"] == archive.module_id
    )
    assert row["last_attempt"] and row["last_error"] == "drive_sync_failed"
    assert not row["last_success"]

    started = asyncio.Event()

    async def interrupted():
        async with live.transfer(archive):
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(interrupted())
    await started.wait()
    assert next(r for r in (await live.overview())["consumers"] if r["id"] == archive.module_id)[
        "in_progress"
    ]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    restarted = type(provider)(live.resources, svc.modules)
    await restarted.initialize()
    row = next(r for r in (await restarted.overview())["consumers"] if r["id"] == archive.module_id)
    assert row["last_error"] == "unconfirmed" and not row["in_progress"]
    backup = await svc.db_backup.snapshot_once(upload_drive=False)

    raw = gzip.decompress(
        decrypt_backup_bytes(
            await asyncio.to_thread(Path(backup["path"]).read_bytes),
            svc.s.encryption_key.get_secret_value(),
        )
    )
    restored = sqlite3.connect(":memory:")
    try:
        restored.deserialize(raw)
        result = restored.execute("SELECT module_id,last_error FROM drive_module_sync").fetchall()
        assert result == [(archive.module_id, "unconfirmed")]
    finally:
        restored.close()
    assert all(
        secret not in raw
        for secret in (b"sensitive-remote-body", b"secret-name.txt", b"private-file-content")
    )


async def test_legacy_diary_receipt_migrates_without_upload(admin_api):
    app, client, gateway = admin_api
    svc = app.state.service
    async with svc.store.transaction():
        await svc.store.db.execute(
            "INSERT INTO roaming_sync(day,file_id,last_success)"
            " VALUES ('2026-11-01','old-file',12345)"
        )
    await svc.modules.systems["patrol_diary"].initialize()
    await svc.modules.systems["patrol_diary"].initialize()
    report = (await client.get("/admin/api/drive-sync")).json()
    assert [r["id"] for r in report["consumers"] if r["last_success"] == 12345] == ["patrol_diary"]
    assert "owner_notebook" not in {r["id"] for r in report["consumers"]}
    assert not gateway.calls


async def test_native_consumers_use_legacy_files_and_independent_receipts(settings, store, message):
    from hakimi.modules.adapters import Sender
    from hakimi.service import Service
    from tests.fakes import FakeBili, FakeGateway
    from tests.test_drive_client import DAY, FILE, file_info
    from tests.test_dual_memory_v100 import group
    from tests.test_social_integration import Wire

    writes = []
    settings.roaming_enabled = True

    def handle(request):
        if request.url.path.endswith("/" + FOLDER):
            return httpx.Response(200, json=folder_info())
        if request.method == "GET" and request.url.path.endswith("/files"):
            return httpx.Response(200, json={"files": []})
        assert request.method == "POST"
        meta, body = request.content.split(b"\r\n\r\n", 2)[1:]
        item = json.loads(meta.split(b"\r\n", 1)[0])
        writes.append((item["name"], body, item["appProperties"]))
        return httpx.Response(200, json=file_info(**item))

    drive, http = await drive_for(settings, handle)
    async with http:
        svc = Service(
            settings, store, FakeGateway(), FakeBili(), Sender(settings, http, Wire()), drive=drive
        )
        for mod in svc.modules.systems.values():
            await mod.initialize()
        for name in ("group_memory_mirror", "database_backup"):
            with pytest.raises(Rejected, match="drive_specialized_sync_required"):
                await svc.drive_sync.sync_file(svc.modules.systems[name], "raw.txt", "private")
        diary = svc.modules.systems["patrol_diary"]
        assert await diary.write_day(DAY, "公开巡逻材料") == FILE
        await svc.surf_mirror.export_once()
        backup = await svc.db_backup.snapshot_once()
        backup_entry = next(v for v in writes if v[0].endswith(".sqlite3.gz.enc"))
        assert backup["drive_uploaded"] and backup_entry[1].startswith(b"gAAAA")
        assert writes[0][0] == DAY + ".md" and writes[0][2] == {"hakimi_roaming": "test-marker"}
        assert any(v[0] == "surf-mirror-latest.md" and "hakimi_surf_mirror" in v[2] for v in writes)

        msg = group(message)
        view = await svc.sessions.dual.view(msg)
        with pytest.raises(Rejected, match="memory_mirror_disabled"):
            await svc.memory_mirror.sync(msg, push=True)
        assert not await store.one(
            "SELECT 1 FROM drive_module_sync WHERE module_id='group_memory_mirror'"
        )
        await svc.memory_mirror.configure(msg, True, view["version"])
        result = await svc.memory_mirror.sync(msg, push=True)
        assert result["status"] == "pushed" and "hakimi_group_scope" in writes[-1][2]
        with pytest.raises(Rejected, match="memory_mirror_group_only"):
            await svc.memory_mirror.sync(message, push=True)
        rows = (await svc.drive_sync.overview())["consumers"]
        assert {r["id"] for r in rows if r["last_success"] and not r["last_error"]} == {
            "patrol_diary",
            "surf_mirror",
            "database_backup",
            "group_memory_mirror",
        }
        assert svc.ai.calls == []


async def test_group_revoking_consent_while_a_sync_waits_prevents_drive_access(
    settings, store, message
):
    from contextlib import asynccontextmanager

    from tests.test_dual_memory_v100 import mirror_fixture

    mirror, drive, sessions, msg, _ = await mirror_fixture(settings, store, message)
    waiting, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def delayed_transfer():
        waiting.set()
        await release.wait()
        yield

    async def no_read(scope):
        pytest.fail("Drive was contacted after group consent was revoked")

    mirror.transfer = delayed_transfer
    drive.memory_head = no_read
    task = asyncio.create_task(mirror.sync(msg, push=True))
    try:
        await asyncio.wait_for(waiting.wait(), 2)
        await mirror.configure(msg, False, (await sessions.dual.view(msg))["version"])
        release.set()
        with pytest.raises(Rejected, match="memory_mirror_disabled"):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert drive.writes == 0
