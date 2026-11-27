import json

import httpx
import pytest
from google.auth import jwt

from hakimi.drive_client import (
    API,
    SCOPE,
    TOKEN_URI,
    UPLOAD,
    BackupCollection,
    FolderDrive,
)
from hakimi.security import Rejected
from tests.conftest import cached_rsa_private_key_pem

FOLDER = "dedicated-folder-123456"
FILE = "diary-file-123456"
DAY = "2026-11-14"


def file_info(**changes):
    return {
        "id": FILE,
        "name": DAY + ".md",
        "parents": [FOLDER],
        "mimeType": "text/markdown",
        "appProperties": {"hakimi_roaming": "test-marker"},
        "trashed": False,
    } | changes


def folder_info(**changes):
    return {
        "id": FOLDER,
        "mimeType": "application/vnd.google-apps.folder",
        "driveId": "shared-drive-id",
        "capabilities": {"canAddChildren": True},
        "trashed": False,
    } | changes


async def drive_for(settings, handler):
    settings = settings.model_copy(update={"gdrive_roaming_folder_id": FOLDER})
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    drive = FolderDrive(settings, client, "test-marker")

    async def token():
        return "test-token"

    drive._access_token = token
    return drive, client


async def test_create_reads_only_folder_and_parent_scoped_listing(settings):
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            assert f"'{FOLDER}' in parents" in request.url.params["q"]
            assert request.url.params["corpora"] == "user"
            assert "driveId" not in request.url.params
            return httpx.Response(200, json={"files": []})
        assert str(request.url).startswith(UPLOAD)
        assert request.method == "POST" and request.url.params["uploadType"] == "multipart"
        body = request.content.decode()
        assert f'"parents": ["{FOLDER}"]' in body and "safe diary" in body
        return httpx.Response(200, json=file_info())

    drive, client = await drive_for(settings, handler)
    async with client:
        assert await drive.sync_day(DAY, "safe diary") == FILE
    assert len(requests) == 3
    assert all(r.url.host == "www.googleapis.com" for r in requests)


async def test_update_validates_parents_before_patch_and_uses_etag(settings):
    methods = []

    def handler(request):
        methods.append(request.method)
        if str(request.url).startswith(API + "/" + FOLDER):
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [file_info()]})
        if request.method == "GET":
            assert request.url.path == "/drive/v3/files/" + FILE
            return httpx.Response(200, json=file_info(), headers={"ETag": '"version-1"'})
        assert request.method == "PATCH"
        assert request.headers["If-Match"] == '"version-1"'
        assert request.url.params["uploadType"] == "media"
        return httpx.Response(200, json=file_info())

    drive, client = await drive_for(settings, handler)
    async with client:
        assert await drive.sync_day(DAY, "new diary") == FILE
    assert methods == ["GET", "GET", "GET", "PATCH"]


@pytest.mark.parametrize(
    "change",
    [
        {"parents": ["private-folder"]},
        {"mimeType": "application/vnd.google-apps.shortcut"},
        {"appProperties": {}},
        {"name": "private-notes.md"},
    ],
)
async def test_out_of_scope_or_foreign_file_never_fetched_or_written(settings, change):
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        assert request.url.path == "/drive/v3/files"
        return httpx.Response(200, json={"files": [file_info(**change)]})

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected, match="outside_scope"):
            await drive.sync_day(DAY, "diary")
    assert len(requests) == 2


async def test_file_moved_after_listing_is_not_updated(settings):
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [file_info()]})
        return httpx.Response(200, json=file_info(parents=["some-other-folder"]))

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected):
            await drive.sync_day(DAY, "diary")
    assert all(r.method == "GET" for r in requests)


async def test_my_drive_folder_rejected_without_oauth_fallback(settings):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=folder_info(driveId=None))

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected, match="shared_folder_required"):
            await drive.sync_day(DAY, "diary")
    assert len(requests) == 1


@pytest.mark.parametrize(
    "listing", [{"files": [], "incompleteSearch": True}, {"files": [file_info(), file_info()]}]
)
async def test_ambiguous_listing_never_creates_another_diary(settings, listing):
    methods = []

    def handler(request):
        methods.append(request.method)
        if request.url.path.endswith(FOLDER):
            return httpx.Response(200, json=folder_info())
        return httpx.Response(200, json=listing)

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected, match="ambiguous_file"):
            await drive.sync_day(DAY, "diary")
    assert methods == ["GET", "GET"]


async def test_service_account_jwt_uses_only_drive_file_scope_and_fixed_token_uri(
    settings, tmp_path
):
    pem = cached_rsa_private_key_pem()
    path = tmp_path / "test-service-account.json"
    info = {
        "type": "service_account",
        "private_key": pem,
        "private_key_id": "test-key",
        "client_email": "diary@test-project.iam.gserviceaccount.com",
        "token_uri": TOKEN_URI,
    }
    path.write_text(json.dumps(info))
    requests = []

    def handler(request):
        from urllib.parse import parse_qs

        requests.append(request)
        assert str(request.url) == TOKEN_URI
        assertion = parse_qs(request.content.decode())["assertion"][0]
        claims = jwt.decode(assertion, verify=False)
        assert claims["scope"] == SCOPE and claims["aud"] == TOKEN_URI
        assert "sub" not in claims
        return httpx.Response(200, json={"access_token": "test-access-token", "expires_in": 3600})

    s = settings.model_copy(
        update={"gdrive_roaming_folder_id": FOLDER, "gdrive_service_account_file": path}
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(s, client, "test-marker")
        assert await drive._access_token() == "test-access-token"
        assert await drive._access_token() == "test-access-token"
        assert len(requests) == 1
        info["token_uri"] = "https://evil.test/token"
        path.write_text(json.dumps(info))
        with pytest.raises(Rejected, match="credentials_invalid"):
            drive._load_credentials()


async def test_group_memory_scope_properties_etag_download_and_conditional_update(settings):
    scope = "a" * 64
    filename = "group-memory-" + scope[:24] + ".md"
    item = file_info(
        name=filename,
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            q = request.url.params["q"]
            assert f"'{FOLDER}' in parents" in q
            assert "key='hakimi_group_memory'" in q and "key='hakimi_group_scope'" in q
            return httpx.Response(200, json={"files": [item]})
        if request.url.params.get("alt") == "media":
            assert request.headers["If-Match"] == '"v1"'
            return httpx.Response(200, text="readable memory")
        if request.method == "PATCH":
            assert request.headers["If-Match"] == '"v1"'
            return httpx.Response(200, json=item)
        return httpx.Response(200, json={**item, "size": "15"}, headers={"ETag": '"v1"'})

    drive, client = await drive_for(settings, handler)
    async with client:
        head = await drive.memory_head(scope)
        assert await drive.read_memory(scope, head) == "readable memory"
        assert await drive.write_memory(scope, "updated", head) == FILE
    assert all(r.url.host == "www.googleapis.com" for r in seen)


@pytest.mark.parametrize("mode", ["missing_etag", "changed_parent", "changed_etag", "too_big"])
async def test_group_memory_no_unconditional_update_or_out_of_scope_download(settings, mode):
    scope = "b" * 64
    item = file_info(
        name="group-memory-" + scope[:24] + ".md",
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )
    touched = []

    def handler(request):
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [item]})
        assert request.method == "GET" and request.url.params.get("alt") != "media"
        touched.append(1)
        meta = {**item, "size": str(262145 if mode == "too_big" else 20)}
        if mode == "changed_parent":
            meta["parents"] = ["foreign-folder"]
        return httpx.Response(
            200, json=meta, headers={} if mode == "missing_etag" else {"ETag": '"v2"'}
        )

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected):
            await drive.read_memory(
                scope, {"id": FILE, "etag": '"v2"' if mode == "too_big" else '"v1"'}
            )


async def test_sync_day_allows_large_diary_up_to_4_5mb_and_rejects_excess(settings):
    from hakimi.drive_client import MAX_DIARY_UPLOAD_BYTES

    def handler(request):
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.method == "POST":
            return httpx.Response(200, json=file_info())
        return httpx.Response(200, json={"files": []})

    drive, client = await drive_for(settings, handler)
    async with client:
        assert await drive.sync_day(DAY, "x" * 400_000) == FILE
        with pytest.raises(Rejected, match="drive_invalid_diary"):
            await drive.sync_day(DAY, "x" * (MAX_DIARY_UPLOAD_BYTES + 1))


@pytest.mark.parametrize(
    "v3_meta",
    [
        {
            "md5Checksum": "0123456789abcdef0123456789ABCDEF",
            "version": "9",
            "headRevisionId": "rev_9",
        },
        {
            "md5Checksum": "0123456789abcdef0123456789abcdef",
            "version": "12",
        },
        {
            "md5Checksum": "0123456789abcdef0123456789abcdef",
            "headRevisionId": "rev-only_1",
        },
    ],
)
async def test_backup_collection_delete_v3_without_http_etag(settings, v3_meta):
    from datetime import date

    marker = "backup-marker"
    old_name = "db-backup-2026-11-01.sqlite3.gz.enc"
    new_name = "db-backup-2026-11-17.sqlite3.gz.enc"
    old_item = {
        "id": "old-backup-1",
        "name": old_name,
        "parents": [FOLDER],
        "mimeType": "application/octet-stream",
        "appProperties": {"hakimi_db_backup": marker},
        "trashed": False,
        **v3_meta,
    }
    new_item = {
        "id": "new-backup-1",
        "name": new_name,
        "parents": [FOLDER],
        "mimeType": "application/octet-stream",
        "appProperties": {"hakimi_db_backup": marker},
        "trashed": False,
    }
    deleted: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=folder_info())
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            fields = request.url.params["fields"]
            assert "md5Checksum" in fields and "version" in fields and "headRevisionId" in fields
            return httpx.Response(200, json={"files": [old_item]})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            return httpx.Response(200, json=new_item)
        if request.method == "GET" and request.url.path == "/drive/v3/files/old-backup-1":
            fields = request.url.params["fields"]
            assert "md5Checksum" in fields and "version" in fields and "headRevisionId" in fields
            return httpx.Response(200, json=old_item)
        if request.method == "DELETE" and request.url.path == "/drive/v3/files/old-backup-1":
            assert "If-Match" not in request.headers
            deleted.append(request)
            return httpx.Response(204)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    drive, client = await drive_for(settings, handler)
    async with client:
        col = BackupCollection(drive, marker)
        fid, pruned = await col.sync_day("2026-11-17", date(2026, 11, 11), b"encrypted")
    assert fid == "new-backup-1"
    assert pruned == [old_name]
    assert len(deleted) == 1


@pytest.mark.parametrize(
    "bad_meta",
    [
        {},
        {"md5Checksum": "0123456789abcdef0123456789abcdef"},
        {"version": "5", "headRevisionId": "rev1"},
        {"md5Checksum": "not-an-md5", "version": "5"},
        {"md5Checksum": "0123456789abcdef0123456789abcdef", "version": "abc"},
        {
            "md5Checksum": "0123456789abcdef0123456789abcdef",
            "version": "5",
            "headRevisionId": "bad:rev",
        },
    ],
)
async def test_backup_collection_delete_requires_etag_or_valid_v3_metadata(settings, bad_meta):
    marker = "backup-marker"
    old_name = "db-backup-2026-11-01.sqlite3.gz.enc"
    delete_called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal delete_called
        if request.method == "GET" and request.url.path == "/drive/v3/files/old-1":
            return httpx.Response(
                200,
                json={
                    "id": "old-1",
                    "name": old_name,
                    "parents": [FOLDER],
                    "mimeType": "application/octet-stream",
                    "appProperties": {"hakimi_db_backup": marker},
                    "trashed": False,
                    **bad_meta,
                },
            )
        if request.method == "DELETE":
            delete_called = True
            return httpx.Response(204)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    drive, client = await drive_for(settings, handler)
    async with client:
        col = BackupCollection(drive, marker)
        with pytest.raises(Rejected, match="drive_etag_required"):
            await col._delete("old-1", old_name)
    assert delete_called is False


async def test_backup_collection_delete_etag_present_mismatch_and_v3_conflict(settings):
    marker = "backup-marker"
    old_name = "db-backup-2026-11-01.sqlite3.gz.enc"
    base_item = {
        "id": "old-1",
        "name": old_name,
        "parents": [FOLDER],
        "mimeType": "application/octet-stream",
        "appProperties": {"hakimi_db_backup": marker},
        "trashed": False,
    }
    mode = {"case": "etag_412"}
    delete_headers: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/drive/v3/files/old-1":
            if mode["case"] == "etag_412":
                return httpx.Response(200, json=base_item, headers={"ETag": '"v1"'})
            if mode["case"] == "v3_conflict":
                return httpx.Response(
                    200,
                    json={
                        **base_item,
                        "md5Checksum": "0123456789abcdef0123456789abcdef",
                        "version": "2",
                    },
                )
            if mode["case"] == "trashed":
                return httpx.Response(
                    200,
                    json={
                        **base_item,
                        "trashed": True,
                        "md5Checksum": "0123456789abcdef0123456789abcdef",
                        "version": "1",
                    },
                )
        if request.method == "DELETE":
            delete_headers.append(dict(request.headers))
            return httpx.Response(412)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    drive, client = await drive_for(settings, handler)
    async with client:
        col = BackupCollection(drive, marker)
        # HTTP ETag returned -> If-Match sent, 412 surfaces as drive_etag_conflict.
        with pytest.raises(Rejected, match="drive_etag_conflict"):
            await col._delete("old-1", old_name)
        assert len(delete_headers) == 1 and delete_headers[0]["if-match"] == '"v1"'

        # Drive v3 metadata changed since listing -> conflict raised before DELETE.
        mode["case"] = "v3_conflict"
        with pytest.raises(Rejected, match="drive_etag_conflict"):
            await col._delete(
                "old-1",
                old_name,
                expected={
                    **base_item,
                    "md5Checksum": "0123456789abcdef0123456789abcdef",
                    "version": "1",
                },
            )
        assert len(delete_headers) == 1

        # Trashed file rejected before DELETE.
        mode["case"] = "trashed"
        with pytest.raises(Rejected, match="drive_file_outside_scope"):
            await col._delete("old-1", old_name)
        assert len(delete_headers) == 1
