import json

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt

from hakimi.drive_client import API, SCOPE, TOKEN_URI, UPLOAD, FolderDrive
from hakimi.security import Rejected

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
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
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
