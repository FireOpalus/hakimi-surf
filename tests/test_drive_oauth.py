import json
import os
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from multidict import MultiDict

from hakimi.config import Settings
from hakimi.drive_client import API, SCOPE, TOKEN_URI, FolderDrive
from hakimi.drive_oauth import (
    AUTH_URI,
    FOLDER_MARKER,
    OAuthDriveAuth,
    load_oauth_credentials,
    request_oauth_token,
)
from hakimi.notebook_snapshot import SnapshotDrive
from hakimi.security import Rejected
from scripts.authorize_drive import (
    allowed_loopback_host,
    authorization_url,
    callback_code,
    ensure_folder,
    load_desktop_client,
    run,
    save_credentials,
)

FOLDER = "oauth-folder-123456"
FILE = "oauth-diary-123456"
MARKER = "a" * 32


@pytest.mark.parametrize(
    "host,allowed",
    [
        ("127.0.0.1:12142", True),
        ("localhost:12142", True),
        ("localhost", True),
        ("127.0.0.1", True),
        ("[::1]:12142", True),
        ("127.0.0.1:9999", False),
        ("127.0.0.1.evil.example:12142", False),
        ("localhost.evil.example", False),
        ("server.example:12142", False),
        ("evil@127.0.0.1:12142", False),
    ],
)
def test_loopback_host_handles_local_forward_aliases_only(host, allowed):
    assert allowed_loopback_host(host, 12142) is allowed


def credentials(**changes):
    return {
        "type": "authorized_user",
        "client_id": "123-test.apps.googleusercontent.com",
        "client_secret": "test-secret",
        "token_uri": TOKEN_URI,
        "scopes": [SCOPE],
        "refresh_token": "test-refresh",
        "folder_id": FOLDER,
        "folder_marker": MARKER,
    } | changes


def token(**changes):
    return {
        "access_token": "test-access",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": SCOPE,
    } | changes


def folder(**changes):
    return {
        "id": FOLDER,
        "mimeType": "application/vnd.google-apps.folder",
        "appProperties": {FOLDER_MARKER: MARKER},
        "capabilities": {"canAddChildren": True},
        "trashed": False,
    } | changes


def oauth_settings(settings, tmp_path):
    path = tmp_path / "oauth.json"
    save_credentials(path, credentials())
    return settings.model_copy(
        update={
            "gdrive_auth_mode": "oauth",
            "gdrive_oauth_credentials_file": path,
            "gdrive_roaming_folder_id": FOLDER,
        }
    )


def test_oauth_is_explicit_and_requires_its_own_path(settings, tmp_path):
    values = settings.model_dump()
    assert values["gdrive_auth_mode"] == "service_account"
    values.update(
        roaming_enabled=True,
        gdrive_auth_mode="oauth",
        gdrive_service_account_file=tmp_path / "sa.json",
        gdrive_roaming_folder_id=FOLDER,
    )
    missing = Settings(_env_file=None, **values)
    assert missing.gdrive_oauth_credentials_file is None
    values["gdrive_oauth_credentials_file"] = tmp_path / "oauth.json"
    assert Settings(_env_file=None, **values).gdrive_auth_mode == "oauth"


@pytest.mark.parametrize(
    "changes",
    [
        {"scopes": ["https://www.googleapis.com/auth/drive"]},
        {"scopes": [SCOPE, "https://www.googleapis.com/auth/drive.readonly"]},
        {"token_uri": "https://untrusted.example/token"},
        {"type": "service_account"},
        {"refresh_token": ""},
        {"folder_marker": ""},
    ],
)
def test_unbounded_or_wrong_credentials_rejected(tmp_path, changes):
    path = tmp_path / "oauth.json"
    save_credentials(path, credentials(**changes))
    with pytest.raises(Rejected, match="credentials_invalid"):
        load_oauth_credentials(path, FOLDER)


def test_token_file_permissions_symlinks_and_folder_binding(tmp_path):
    path = tmp_path / "oauth.json"
    save_credentials(path, credentials())
    assert path.stat().st_mode & 0o777 == 0o600
    assert load_oauth_credentials(path, FOLDER)["folder_id"] == FOLDER
    with pytest.raises(Rejected, match="folder_mismatch"):
        load_oauth_credentials(path, "other-folder-123456")
    symlink = tmp_path / "link.json"
    symlink.symlink_to(path)
    with pytest.raises(Rejected, match="credentials_invalid"):
        load_oauth_credentials(symlink)
    os.chmod(path, 0o644)
    with pytest.raises(Rejected, match="credentials_invalid"):
        load_oauth_credentials(path)


async def test_refresh_only_fixed_endpoint_and_scope_and_cached(settings, tmp_path):
    s = oauth_settings(settings, tmp_path)
    calls = []

    def handler(request):
        calls.append(request)
        assert str(request.url) == TOKEN_URI
        data = parse_qs(request.content.decode())
        assert data == {
            "grant_type": ["refresh_token"],
            "client_id": [credentials()["client_id"]],
            "client_secret": ["test-secret"],
            "refresh_token": ["test-refresh"],
            "scope": [SCOPE],
        }
        return httpx.Response(200, json=token())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        auth = OAuthDriveAuth(s.gdrive_oauth_credentials_file, FOLDER, client)
        assert await auth.access_token() == "test-access"
        assert await auth.access_token() == "test-access"
    assert len(calls) == 1


@pytest.mark.parametrize("scope", [None, SCOPE + " https://www.googleapis.com/auth/drive"])
async def test_initial_consent_must_confirm_only_requested_scope(scope):
    def handler(request):
        return httpx.Response(200, json=token(scope=scope))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(Rejected, match="scope_mismatch"):
            await request_oauth_token(http, {}, require_scope=True)


async def test_revoked_oauth_is_sanitized_and_never_falls_back(settings, tmp_path):
    s = oauth_settings(settings, tmp_path)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            400,
            json={
                "error": "invalid_grant",
                "error_description": "private-account-and-token-detail",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(s, client, "writer")
        with pytest.raises(Rejected, match="^drive_oauth_reauthorization_required$"):
            await drive.sync_day("2026-11-14", "safe")
    assert len(calls) == 1 and str(calls[0].url) == TOKEN_URI


async def test_personal_drive_creates_then_updates_only_bound_folder(settings, tmp_path):
    s = oauth_settings(settings, tmp_path)
    created = False
    writes = []
    item = {
        "id": FILE,
        "name": "2026-11-14.md",
        "parents": [FOLDER],
        "mimeType": "text/markdown",
        "appProperties": {"hakimi_roaming": "writer"},
    }

    def handler(request):
        nonlocal created
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json=token())
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder())
        if request.url.path == "/drive/v3/files":
            assert request.url.params["q"] == (
                f"'{FOLDER}' in parents and trashed = false and name = '2026-11-14.md'"
            )
            assert request.url.params["corpora"] == "user"
            assert "driveId" not in request.url.params
            return httpx.Response(200, json={"files": [item] if created else []})
        if request.method == "GET":
            assert request.url.path.endswith(FILE)
            return httpx.Response(200, json=item, headers={"etag": '"test-version"'})
        writes.append(request)
        if not created:
            assert request.method == "POST"
            assert f'"parents": ["{FOLDER}"]' in request.content.decode()
            created = True
        else:
            assert request.method == "PATCH" and request.url.path.endswith(FILE)
            assert request.headers["if-match"] == '"test-version"'
        return httpx.Response(200, json=item)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(s, client, "writer")
        assert await drive.sync_day("2026-11-14", "first") == FILE
        assert await drive.sync_day("2026-11-14", "second") == FILE
    assert [r.method for r in writes] == ["POST", "PATCH"]


async def test_wrong_folder_marker_cannot_even_list_children(settings, tmp_path):
    s = oauth_settings(settings, tmp_path)
    calls = []

    def handler(request):
        calls.append(request)
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json=token())
        assert request.url.path.endswith(FOLDER)
        return httpx.Response(200, json=folder(appProperties={}))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(Rejected, match="folder_mismatch"):
            await FolderDrive(s, client, "writer").sync_day("2026-11-14", "diary")
    assert len(calls) == 2


def test_notebook_does_not_inherit_diary_oauth(settings, tmp_path):
    s = oauth_settings(settings, tmp_path)
    assert not s.enable_notebook_snapshot
    snapshot = SnapshotDrive(s, None)
    assert snapshot.s.gdrive_service_account_file is None
    assert not hasattr(snapshot, "oauth")


def test_consent_request_pkce_and_state_are_mandatory():
    import base64
    import hashlib

    verifier = "v" * 64
    url = authorization_url("client-id", "http://127.0.0.1:12142/oauth/callback", "nonce", verifier)
    assert url.startswith(AUTH_URI + "?")
    query = parse_qs(urlsplit(url).query)
    assert query["scope"] == [SCOPE]
    assert query["code_challenge_method"] == ["S256"]
    assert query["code_challenge"] == [
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    ]
    assert query["state"] == ["nonce"] and "client_secret" not in query
    assert callback_code(MultiDict(state="nonce", code="code"), "nonce") == "code"
    for values in [
        [("state", "wrong"), ("code", "code")],
        [("state", "nonce"), ("state", "nonce"), ("code", "code")],
        [("state", "nonce"), ("code", "a"), ("code", "b")],
        [("state", "nonce"), ("error", "access_denied")],
    ]:
        with pytest.raises(Rejected):
            callback_code(MultiDict(values), "nonce")


def test_only_desktop_client_with_google_endpoints_accepted(tmp_path):
    path = tmp_path / "client.json"
    info = credentials() | {"auth_uri": "https://accounts.google.com/o/oauth2/auth"}
    save_credentials(path, {"installed": info})
    assert load_desktop_client(path)["client_id"] == info["client_id"]
    save_credentials(path, {"web": info})
    with pytest.raises(Rejected, match="desktop_client_required"):
        load_desktop_client(path)


async def test_setup_can_resume_known_folder_without_global_listing(tmp_path, monkeypatch):
    from argparse import Namespace

    import scripts.authorize_drive as setup

    path = tmp_path / "oauth.json"
    save_credentials(path, credentials())
    requests = []

    def handler(request):
        requests.append(request)
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json=token())
        assert str(request.url).startswith(API + "/" + FOLDER)
        return httpx.Response(200, json=folder())

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        setup.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
    )
    await run(Namespace(output=path, reauthorize=False))
    assert len(requests) == 2


async def test_setup_uses_reserved_id_when_creating_and_rechecks_marker():
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            assert request.url.path.endswith(FOLDER)
            return httpx.Response(404)
        data = json.loads(request.content)
        assert data["id"] == FOLDER and data["appProperties"][FOLDER_MARKER] == MARKER
        return httpx.Response(200, json=folder())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await ensure_folder(http, "access", credentials(), allow_create=True)
    assert [r.method for r in requests] == ["GET", "POST"]


async def test_first_authorization_saves_only_scoped_tokens_and_one_folder(tmp_path, monkeypatch):
    from argparse import Namespace

    import scripts.authorize_drive as setup

    client_path, output = tmp_path / "client.json", tmp_path / "oauth.json"
    save_credentials(
        client_path,
        {"installed": credentials() | {"auth_uri": "https://accounts.google.com/o/oauth2/auth"}},
    )

    async def fake_consent(client_id, port):
        return "one-time-code", "pkce-verifier", "http://127.0.0.1:12142/oauth/callback"

    def handler(request):
        if str(request.url) == TOKEN_URI:
            data = parse_qs(request.content.decode())
            assert data["code"] == ["one-time-code"]
            assert data["code_verifier"] == ["pkce-verifier"]
            assert data["grant_type"] == ["authorization_code"]
            return httpx.Response(200, json=token(refresh_token="test-refresh"))
        if request.url.path.endswith("/generateIds"):
            return httpx.Response(200, json={"ids": [FOLDER]})
        assert output.is_file()  # Journal credentials + reserved ID before a Drive write.
        info = load_oauth_credentials(output)
        if request.method == "GET":
            assert request.url.path.endswith(FOLDER)
            return httpx.Response(404)
        assert request.method == "POST" and str(request.url).startswith(API)
        assert json.loads(request.content)["id"] == FOLDER
        return httpx.Response(
            200, json=folder(appProperties={FOLDER_MARKER: info["folder_marker"]})
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(setup, "receive_code", fake_consent)
    monkeypatch.setattr(
        setup.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
    )
    await run(Namespace(client=client_path, output=output, reauthorize=False, port=12142))
    saved = load_oauth_credentials(output)
    assert saved["folder_id"] == FOLDER and saved["scopes"] == [SCOPE]
    assert "access_token" not in saved and "code" not in saved


async def test_reauthorization_wrong_account_preserves_original_credentials(tmp_path, monkeypatch):
    from argparse import Namespace

    import scripts.authorize_drive as setup

    client_path, output = tmp_path / "client.json", tmp_path / "oauth.json"
    save_credentials(
        client_path,
        {"installed": credentials() | {"auth_uri": "https://accounts.google.com/o/oauth2/auth"}},
    )
    save_credentials(output, credentials())
    original = output.read_bytes()

    async def fake_consent(client_id, port):
        return "code", "verifier", "http://127.0.0.1:12142/oauth/callback"

    def handler(request):
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json=token(refresh_token="new-token-must-not-replace-old"))
        assert request.method == "GET" and request.url.path.endswith(FOLDER)
        return httpx.Response(404)

    real_client = httpx.AsyncClient
    monkeypatch.setattr(setup, "receive_code", fake_consent)
    monkeypatch.setattr(
        setup.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
    )
    with pytest.raises(Rejected, match="drive_setup_http_404"):
        await run(Namespace(client=client_path, output=output, reauthorize=True, port=12142))
    assert output.read_bytes() == original
