import json
import time

import httpx
import pytest
from google.auth import jwt

from hakimi.drive_client import TOKEN_URI
from hakimi.notebook_snapshot import NotebookSnapshot, SnapshotDrive
from hakimi.security import Rejected
from tests.conftest import cached_rsa_private_key_pem
from tests.fakes import FakeGateway

FILE = "single-sanitized-snapshot-id"


async def test_snapshot_defaults_disabled_and_no_io(settings, store, message, tmp_path):
    path = tmp_path / "snapshot.md"
    path.write_text("private snapshot")
    settings.notebook_snapshot_path = path
    async with httpx.AsyncClient() as http:
        snap = NotebookSnapshot(settings, http, FakeGateway(), store)
        with pytest.raises(Rejected, match="disabled"):
            await snap.refresh()
        assert snap.for_message(message) == ""
        assert snap.status()["text"] == ""


async def test_local_snapshot_4000_chars_private_owner_only(settings, store, message, tmp_path):
    settings.enable_notebook_snapshot = True
    settings.owner_qq_id = "20000"
    settings.notebook_snapshot_path = tmp_path / "snapshot.md"
    settings.notebook_snapshot_path.write_text("近期关注天文。" * 1000)
    ai = FakeGateway()
    async with httpx.AsyncClient() as http:
        snap = NotebookSnapshot(settings, http, ai, store)
        await snap.refresh()
        assert len(snap.for_message(message)) == 4000
        assert snap.next_refresh >= time.time() + 21599
        for updates in [
            {"kind": "group", "conversation_id": "40000"},
            {"user_id": "30000", "conversation_id": "30000"},
            {"platform": "qq", "bot_id": "99999"},
        ]:
            assert snap.for_message(message.model_copy(update=updates)) == ""
        assert ai.calls[0][3]["purpose"] == "source"
        assert ai.calls[0][3]["budget_lane"] == "admin"


async def test_local_update_is_screened_atomic_and_symlinks_rejected(settings, store, tmp_path):
    settings.enable_notebook_snapshot = True
    settings.notebook_snapshot_path = tmp_path / "snapshot.md"
    ai = FakeGateway()
    async with httpx.AsyncClient() as http:
        snap = NotebookSnapshot(settings, http, ai, store)
        await snap.update_local("原快照")
        assert settings.notebook_snapshot_path.stat().st_mode & 0o077 == 0
        ai.block_purpose = "source"
        with pytest.raises(Rejected):
            await snap.update_local("恶意替换")
        assert settings.notebook_snapshot_path.read_text() == "原快照"
        assert snap.text == "原快照"
        ai.block_purpose = None
        settings.notebook_snapshot_path.unlink()
        settings.notebook_snapshot_path.symlink_to(tmp_path / "elsewhere")
        with pytest.raises(Rejected, match="symlink"):
            await snap.update_local("改写")
        assert not (tmp_path / "elsewhere").exists()


@pytest.mark.parametrize("mime", ["text/markdown", "application/vnd.google-apps.document"])
async def test_drive_reads_exact_single_file_and_never_lists_or_writes(settings, mime):
    settings.gdrive_notebook_snapshot_file_id = FILE
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        assert request.url.path in {
            "/drive/v3/files/" + FILE,
            "/drive/v3/files/" + FILE + "/export",
        }
        if "fields" in request.url.params:
            return httpx.Response(
                200, json={"id": FILE, "mimeType": mime, "capabilities": {"canDownload": True}}
            )
        if mime == "text/markdown":
            assert request.url.params["alt"] == "media"
        else:
            assert request.url.params["mimeType"] == "text/plain"
        return httpx.Response(200, text="云端脱敏快照" * 1000)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        drive = SnapshotDrive(settings, http)

        async def token():
            return "test-token"

        drive._access_token = token
        assert len(await drive.read()) == 4000
    assert len(calls) == 2


@pytest.mark.parametrize(
    "change",
    [
        {"id": "other-file"},
        {"mimeType": "application/vnd.google-apps.shortcut"},
        {"trashed": True},
        {"capabilities": {"canDownload": False}},
    ],
)
async def test_drive_rejects_wrong_identity_shortcuts_and_permissions(settings, change):
    settings.gdrive_notebook_snapshot_file_id = FILE
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={"id": FILE, "mimeType": "text/plain", "capabilities": {"canDownload": True}}
            | change,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        drive = SnapshotDrive(settings, http)

        async def token():
            return "test-token"

        drive._access_token = token
        with pytest.raises(Rejected):
            await drive.read()
    assert len(calls) == 1


async def test_cloud_failure_does_not_fall_back_or_allow_local_write(settings, store, tmp_path):
    settings.enable_notebook_snapshot = True
    settings.gdrive_notebook_snapshot_file_id = FILE
    settings.notebook_snapshot_path = tmp_path / "private-fallback.md"
    settings.notebook_snapshot_path.write_text("must-not-read")
    async with httpx.AsyncClient() as http:
        snap = NotebookSnapshot(settings, http, FakeGateway(), store)

        async def fail():
            raise RuntimeError("upstream secret details")

        snap.drive.read = fail
        with pytest.raises(Rejected, match="notebook_refresh_failed"):
            await snap.refresh()
        assert snap.text == "" and snap.last_error == "notebook_refresh_failed"
        with pytest.raises(Rejected, match="cloud_readonly"):
            await snap.update_local("change")
        assert settings.notebook_snapshot_path.read_text() == "must-not-read"


async def test_snapshot_token_scope_is_readonly_without_delegation(settings, tmp_path):
    pem = cached_rsa_private_key_pem()
    credentials = {
        "type": "service_account",
        "project_id": "test",
        "private_key_id": "test-key",
        "private_key": pem,
        "client_email": "snapshot@test.iam.gserviceaccount.com",
        "token_uri": TOKEN_URI,
    }
    settings.gdrive_service_account_file = tmp_path / "service-account.json"
    settings.gdrive_service_account_file.write_text(json.dumps(credentials))

    def handler(request):
        from urllib.parse import parse_qs

        claims = jwt.decode(parse_qs(request.content.decode())["assertion"][0], verify=False)
        assert claims["scope"] == "https://www.googleapis.com/auth/drive.readonly"
        assert "sub" not in claims
        assert str(request.url) == TOKEN_URI
        return httpx.Response(200, json={"access_token": "fake", "expires_in": 3600})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        assert await SnapshotDrive(settings, http)._access_token() == "fake"
