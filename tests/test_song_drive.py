"""Tests for SongDriveStore, resumable streaming, song_admin storage/inbox, and pre_dispatch."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import re
import tracemalloc
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.modules import ModuleRegistry
from hakimi.security import Rejected
from hakimi.song_drive import (
    DRIVE_FILE_SCOPE,
    DRIVE_FULL_SCOPE,
    FOLDER_MIME,
    RESUMABLE_CHUNK_BYTES,
    RESUMABLE_CHUNK_GRANULARITY,
    TOKEN_URI,
    SongDriveStore,
    compute_weight_cache_key,
    read_song_oauth_credentials,
    scrub_drive_secrets,
    validate_drive_id,
)
from hakimi.songs import RvcCoverJobPayload, RvcTrainJobPayload, SongLibrary
from hakimi.system_modules import SystemResources
from hakimi.system_modules.song_studio import Module as SongStudioModule
from tests.fakes import FakeGateway

ADMIN_TOKEN = "song-drive-test-admin-token-never-live"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_oauth_json(
    path: Path,
    *,
    scopes: list[str] | None = None,
    mode: int = 0o600,
    extra: dict[str, Any] | None = None,
    omit_scopes: bool = False,
) -> Path:
    payload: dict[str, Any] = {
        "type": "authorized_user",
        "client_id": "1234567890-testclient.apps.googleusercontent.com",
        "client_secret": "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",
        "refresh_token": "1//0g-top-secret-refresh-token-888",
        "token_uri": TOKEN_URI,
    }
    if not omit_scopes:
        payload["scopes"] = scopes if scopes is not None else [DRIVE_FILE_SCOPE]
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(path, mode)
    return path


class _FakeGoogleDriveServer:
    """In-memory Google OAuth2 + Drive v3 REST + Resumable Upload simulator."""

    def __init__(self, *, token_scope: str = DRIVE_FILE_SCOPE, token_ttl: int = 3600) -> None:
        self.token_scope = token_scope
        self.token_ttl = token_ttl
        self.token_calls = 0
        self.token_error: tuple[int, dict[str, Any]] | None = None
        self.force_401_once = False
        self.bad_resumable_location = False

        self.root_id = "drv_root_hakimi_songs_01"
        self.models_id = "drv_models_folder_0001"
        self.songs_id = "drv_songs_folder_00001"
        self.inbox_id = "drv_inbox_folder_00001"
        self.cache_id = "drv_cache_folder_00001"

        self.folders: dict[str, dict[str, Any]] = {
            self.root_id: {
                "id": self.root_id,
                "name": "Hakimi_Songs",
                "parents": ["root"],
                "mimeType": FOLDER_MIME,
                "trashed": False,
                "capabilities": {"canAddChildren": True},
                "appProperties": {"hakimi_songs_role": "root"},
            },
            self.models_id: {
                "id": self.models_id,
                "name": "models",
                "parents": [self.root_id],
                "mimeType": FOLDER_MIME,
                "trashed": False,
                "capabilities": {"canAddChildren": True},
                "appProperties": {"hakimi_songs_role": "models"},
            },
            self.songs_id: {
                "id": self.songs_id,
                "name": "songs",
                "parents": [self.root_id],
                "mimeType": FOLDER_MIME,
                "trashed": False,
                "capabilities": {"canAddChildren": True},
                "appProperties": {"hakimi_songs_role": "songs"},
            },
            self.inbox_id: {
                "id": self.inbox_id,
                "name": "inbox",
                "parents": [self.root_id],
                "mimeType": FOLDER_MIME,
                "trashed": False,
                "capabilities": {"canAddChildren": True},
                "appProperties": {"hakimi_songs_role": "inbox"},
            },
            self.cache_id: {
                "id": self.cache_id,
                "name": "cache",
                "parents": [self.root_id],
                "mimeType": FOLDER_MIME,
                "trashed": False,
                "capabilities": {"canAddChildren": True},
                "appProperties": {"hakimi_songs_role": "cache"},
            },
        }
        self.files: dict[str, dict[str, Any]] = {}
        self.file_bytes: dict[str, bytes] = {}
        self.sessions: dict[str, dict[str, Any]] = {}
        self.aborted_sessions: list[str] = []
        self.resumable_put_ranges: list[tuple[str, int]] = []
        self.requests: list[tuple[str, str]] = []
        self._id_seq = 100

    def add_file(
        self,
        file_id: str,
        *,
        name: str,
        parent_id: str,
        data: bytes,
        mime_type: str = "audio/mpeg",
        trashed: bool = False,
        reported_size: int | None = None,
    ) -> None:
        self.files[file_id] = {
            "id": file_id,
            "name": name,
            "parents": [parent_id],
            "mimeType": mime_type,
            "size": str(len(data) if reported_size is None else reported_size),
            "modifiedTime": "2026-11-22T10:00:00Z",
            "trashed": trashed,
            "appProperties": {},
        }
        self.file_bytes[file_id] = data

    def _next_id(self, prefix: str) -> str:
        self._id_seq += 1
        return f"{prefix}_{self._id_seq:012d}"

    def _handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        parsed = urlsplit(url)
        qs = parse_qs(parsed.query)
        self.requests.append((request.method, url))

        # 1) OAuth token endpoint
        if parsed.netloc == "oauth2.googleapis.com" and parsed.path == "/token":
            self.token_calls += 1
            if self.token_error is not None:
                code, body = self.token_error
                return httpx.Response(code, json=body)
            return httpx.Response(
                200,
                json={
                    "access_token": f"ya29.fake-short-token-{self.token_calls}",
                    "token_type": "Bearer",
                    "expires_in": self.token_ttl,
                    "scope": self.token_scope,
                },
            )

        # 2) Resumable upload session PUT / DELETE
        if parsed.path.startswith("/upload/drive/v3/resumable/"):
            sid = parsed.path.rsplit("/", 1)[-1]
            sess = self.sessions.get(sid)
            if request.method == "DELETE":
                self.aborted_sessions.append(sid)
                self.sessions.pop(sid, None)
                return httpx.Response(204)
            if request.method == "PUT":
                if sess is None:
                    return httpx.Response(404, json={"error": "session_missing"})
                cr = request.headers.get("content-range", "")
                body = request.content
                self.resumable_put_ranges.append((cr, len(body)))
                m = re.fullmatch(r"bytes (\d+)-(\d+)/(\*|\d+)", cr)
                if not m:
                    return httpx.Response(400, json={"error": "bad_range"})
                start, end, total_str = int(m.group(1)), int(m.group(2)), m.group(3)
                if start != len(sess["buf"]) or end - start + 1 != len(body):
                    return httpx.Response(400, json={"error": "range_mismatch"})
                sess["buf"].extend(body)
                if total_str == "*":
                    return httpx.Response(308, headers={"Range": f"bytes=0-{end}"})
                total = int(total_str)
                if len(sess["buf"]) != total:
                    return httpx.Response(400, json={"error": "total_mismatch"})
                fid = sess["existing_id"] or self._next_id("drv_file")
                data = bytes(sess["buf"])
                self.add_file(
                    fid,
                    name=sess["name"],
                    parent_id=sess["parent_id"],
                    data=data,
                    mime_type=sess["mime_type"],
                )
                self.sessions.pop(sid, None)
                return httpx.Response(
                    200,
                    json={
                        "id": fid,
                        "name": sess["name"],
                        "mimeType": sess["mime_type"],
                        "size": str(len(data)),
                    },
                )

        if self.force_401_once:
            self.force_401_once = False
            return httpx.Response(401, json={"error": {"code": 401}})

        # 3) Drive upload endpoints (resumable init / multipart / media patch)
        if parsed.path.startswith("/upload/drive/v3/files"):
            upload_type = (qs.get("uploadType") or [""])[0]
            tail = parsed.path[len("/upload/drive/v3/files") :].lstrip("/")
            if upload_type == "resumable":
                meta = json.loads(request.content.decode("utf-8"))
                sid = f"sess_{len(self.sessions) + 1}_{self._id_seq}"
                self._id_seq += 1
                parent_id = (
                    self.files[tail]["parents"][0]
                    if tail and tail in self.files
                    else (meta.get("parents") or [self.inbox_id])[0]
                )
                self.sessions[sid] = {
                    "existing_id": tail or None,
                    "parent_id": parent_id,
                    "name": meta["name"],
                    "mime_type": request.headers.get(
                        "x-upload-content-type", "application/octet-stream"
                    ),
                    "buf": bytearray(),
                }
                loc = (
                    "https://evil.example.com/upload/hijack"
                    if self.bad_resumable_location
                    else f"https://www.googleapis.com/upload/drive/v3/resumable/{sid}"
                )
                return httpx.Response(200, headers={"Location": loc})
            if upload_type == "media" and tail in self.files:
                data = request.content
                self.file_bytes[tail] = data
                self.files[tail]["size"] = str(len(data))
                return httpx.Response(
                    200,
                    json={
                        "id": tail,
                        "name": self.files[tail]["name"],
                        "mimeType": self.files[tail]["mimeType"],
                        "size": str(len(data)),
                    },
                )
            if upload_type == "multipart":
                ct = request.headers.get("content-type", "")
                boundary = ct.split("boundary=")[-1].encode()
                parts = request.content.split(b"--" + boundary)
                meta_part = parts[1].split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n")
                file_part = parts[2].split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n")
                meta = json.loads(meta_part.decode("utf-8"))
                fid = self._next_id("drv_file")
                self.add_file(
                    fid,
                    name=meta["name"],
                    parent_id=(meta.get("parents") or [self.songs_id])[0],
                    data=file_part,
                    mime_type="audio/mpeg",
                )
                return httpx.Response(
                    200,
                    json={
                        "id": fid,
                        "name": meta["name"],
                        "mimeType": "audio/mpeg",
                        "size": str(len(file_part)),
                    },
                )

        # 4) Drive metadata / listing / download
        if parsed.path == "/drive/v3/files":
            if request.method == "GET":
                q = (qs.get("q") or [""])[0]
                m_parent = re.search(r"'([^']+)' in parents", q)
                parent_id = m_parent.group(1) if m_parent else ""
                m_name = re.search(r"name = '([^']+)'", q)
                want_name = m_name.group(1).replace("\\'", "'") if m_name else None
                want_folder = f"mimeType = '{FOLDER_MIME}'" in q
                want_non_folder = f"mimeType != '{FOLDER_MIME}'" in q
                pool = list(self.folders.values()) + list(self.files.values())
                matched: list[dict[str, Any]] = []
                for item in pool:
                    if item.get("trashed"):
                        continue
                    if parent_id and parent_id not in (item.get("parents") or []):
                        continue
                    if want_folder and item.get("mimeType") != FOLDER_MIME:
                        continue
                    if want_non_folder and item.get("mimeType") == FOLDER_MIME:
                        continue
                    if want_name is not None and item.get("name") != want_name:
                        continue
                    matched.append(item)
                return httpx.Response(200, json={"files": matched})
            if request.method == "POST":
                meta = json.loads(request.content.decode("utf-8"))
                fid = self._next_id("drv_folder")
                entry = {
                    "id": fid,
                    "name": meta["name"],
                    "parents": meta.get("parents") or ["root"],
                    "mimeType": meta.get("mimeType", FOLDER_MIME),
                    "trashed": False,
                    "capabilities": {"canAddChildren": True},
                    "appProperties": meta.get("appProperties") or {},
                }
                self.folders[fid] = entry
                return httpx.Response(200, json=entry)

        if parsed.path.startswith("/drive/v3/files/"):
            fid = parsed.path[len("/drive/v3/files/") :]
            if (qs.get("alt") or [""])[0] == "media":
                if fid not in self.file_bytes:
                    return httpx.Response(404)
                return httpx.Response(200, content=self.file_bytes[fid])
            if fid in self.folders:
                return httpx.Response(200, json=self.folders[fid])
            if fid in self.files:
                return httpx.Response(200, json=self.files[fid])
            return httpx.Response(404, json={"error": {"code": 404}})

        return httpx.Response(404, json={"error": "unhandled"})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self._handle),
            follow_redirects=False,
            trust_env=False,
        )


def test_oauth_credentials_strict_readonly_and_security_guards(tmp_path: Path):
    # 1) Valid roaming-oauth.json with extra folder_id & folder_marker (must NOT be inherited!)
    roaming = _write_oauth_json(
        tmp_path / "roaming-oauth.json",
        scopes=[DRIVE_FILE_SCOPE],
        extra={
            "folder_id": "roaming_diary_folder_should_never_leak",
            "folder_marker": "deadbeef" * 4,
        },
    )
    mtime_before = roaming.stat().st_mtime_ns
    info = read_song_oauth_credentials(roaming)
    assert info["client_id"].endswith(".apps.googleusercontent.com")
    assert info["scopes"] == (DRIVE_FILE_SCOPE,)
    assert "folder_id" not in info
    assert "folder_marker" not in info
    assert roaming.stat().st_mtime_ns == mtime_before

    # 2) Full drive scope also accepted
    full_scope = _write_oauth_json(tmp_path / "full.json", scopes=[DRIVE_FULL_SCOPE])
    assert read_song_oauth_credentials(full_scope)["scopes"] == (DRIVE_FULL_SCOPE,)

    # 3) Group/world-readable modes rejected (0o077 check)
    for bad_mode in (0o640, 0o604, 0o644, 0o666):
        bad_perm = _write_oauth_json(tmp_path / f"perm_{oct(bad_mode)}.json", mode=bad_mode)
        with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
            read_song_oauth_credentials(bad_perm)

    # 4) Symlink rejected (O_NOFOLLOW)
    sym = tmp_path / "link.json"
    sym.symlink_to(roaming)
    with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
        read_song_oauth_credentials(sym)

    # 5) Oversized (> 65536 bytes) rejected
    big = tmp_path / "big.json"
    big.write_bytes(b"{" + b" " * 66_000 + b"}")
    os.chmod(big, 0o600)
    with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
        read_song_oauth_credentials(big)

    # 6) ADC JSON (no scopes) & non-drive scopes & service_account & evil token_uri rejected
    adc = _write_oauth_json(
        tmp_path / "colab-adc.json",
        omit_scopes=True,
        extra={"quota_project_id": "my-gcp-proj"},
    )
    with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
        read_song_oauth_credentials(adc)

    bad_scope = _write_oauth_json(
        tmp_path / "cloud_only.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    with pytest.raises(Rejected, match="song_drive_insufficient_scope"):
        read_song_oauth_credentials(bad_scope)

    evil_uri = _write_oauth_json(
        tmp_path / "evil_uri.json",
        extra={"token_uri": "https://evil.example.com/token"},
    )
    with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
        read_song_oauth_credentials(evil_uri)

    sa = tmp_path / "sa.json"
    sa.write_text(json.dumps({"type": "service_account"}), encoding="utf-8")
    os.chmod(sa, 0o600)
    with pytest.raises(Rejected, match="song_drive_credentials_invalid"):
        read_song_oauth_credentials(sa)

    # 7) "root" (case-insensitive) and invalid Drive IDs strictly forbidden for files & folders
    for bad_id in ("", "root", "ROOT", "Root", "rOoT", "short", "../etc", "abc/def", "has space 1"):
        with pytest.raises(Rejected, match="invalid_drive_file_id"):
            validate_drive_id(bad_id, is_folder=True)
    for bad_file_id in ("", "root", "ROOT", "Root", "rOoT", "../etc", "abc/def", "has space"):
        with pytest.raises(Rejected, match="invalid_drive_file_id"):
            validate_drive_id(bad_file_id, is_folder=False)
    for bad_opt in ("root", "ROOT", "Root", "rOoT"):
        assert SongDriveStore._clean_optional_folder_id(bad_opt) == ""
        with pytest.raises(ValidationError):
            RvcCoverJobPayload(title="t", inbox_file_id=bad_opt)

    # 8) Secret scrubbing redacts ya29.*, 1//*, Bearer, and explicit secrets
    scrubbed = scrub_drive_secrets(
        "Authorization: Bearer ya29.abc123456789 token=1//0g_secret_refresh key=GOCSPX-mysecret",
        "GOCSPX-mysecret",
    )
    assert "ya29." not in scrubbed
    assert "1//" not in scrubbed
    assert "GOCSPX-mysecret" not in scrubbed


def test_weight_cache_key_matches_colab_remote_rvc_and_static_hygiene():
    spec = importlib.util.spec_from_file_location(
        "colab_remote_rvc_ref",
        REPO_ROOT / "scripts" / "colab_remote_rvc.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert compute_weight_cache_key("cover") == mod.compute_weight_cache_key("cover")
    assert compute_weight_cache_key("all") == mod.compute_weight_cache_key("all")

    # Static AST/text hygiene: song_drive.py must never import scripts/ or touch Antigravity_Memory
    src_path = REPO_ROOT / "src" / "hakimi" / "song_drive.py"
    source = src_path.read_text(encoding="utf-8")
    assert "Antigravity_Memory" not in source
    assert "colab-adc" not in source
    tree = ast.parse(source, filename=str(src_path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("scripts")
        elif isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("scripts")


async def test_folder_config_priority_legacy_migration_and_reset(settings, store, tmp_path: Path):
    oauth = _write_oauth_json(tmp_path / "roaming-oauth.json")

    # 1) Legacy SQLite colab_compute_config migration works transparently on first init
    async with store.lock:
        await store.db.execute(
            "CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, payload BLOB NOT NULL)"
        )
    legacy_sealed = store.vault.seal(
        "admin:colab_compute_config",
        {"enabled": True, "drive_songs_folder_id": "legacy_sql_folder_1234"},
    )
    async with store.transaction():
        await store.db.execute(
            "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
            ("colab_compute_config", legacy_sealed),
        )

    s_env = settings.model_copy(
        update={
            "gdrive_oauth_credentials_file": oauth,
            "colab_drive_songs_folder_id": "legacy_env_folder_5678",
        }
    )
    srv = _FakeGoogleDriveServer()
    srv.folders["legacy_sql_folder_1234"] = {
        "id": "legacy_sql_folder_1234",
        "name": "Hakimi_Songs",
        "parents": ["root"],
        "mimeType": FOLDER_MIME,
        "trashed": False,
        "capabilities": {"canAddChildren": True},
    }
    srv.folders["legacy_env_folder_5678"] = {
        "id": "legacy_env_folder_5678",
        "name": "Hakimi_Songs",
        "parents": ["root"],
        "mimeType": FOLDER_MIME,
        "trashed": False,
        "capabilities": {"canAddChildren": True},
    }
    srv.folders["override_folder_999999"] = {
        "id": "override_folder_999999",
        "name": "Hakimi_Songs",
        "parents": ["root"],
        "mimeType": FOLDER_MIME,
        "trashed": False,
        "capabilities": {"canAddChildren": True},
    }

    async with srv.client() as http:
        drive = SongDriveStore(s_env, store, http=http)
        await drive.initialize()
        assert drive.configured() is True
        assert drive.drive_songs_folder_id == "legacy_sql_folder_1234"
        assert drive.config_source == "legacy_sqlite"

        # 2) Explicit set_folder_id writes song_studio_drive_config and wins over legacy
        with pytest.raises(Rejected, match="invalid_drive_file_id"):
            await drive.set_folder_id("root")
        st = await drive.set_folder_id("override_folder_999999")
        assert st["configured"] is True
        assert st["source"] == "admin"
        assert st["drive_songs_folder_id"] == "override_folder_999999"
        assert st["folders"]["root"] == "override_folder_999999"

        # 3) Reset (set_folder_id(None)) restores .env fallback and stays reset across restarts!
        reset_st = await drive.set_folder_id(None)
        assert reset_st["source"] == "legacy_env"
        assert reset_st["drive_songs_folder_id"] == "legacy_env_folder_5678"

        reloaded = SongDriveStore(s_env, store, http=http)
        await reloaded.initialize()
        assert reloaded.drive_songs_folder_id == "legacy_env_folder_5678"
        assert reloaded.config_source == "legacy_env"

        # 4) SONG_DRIVE_SONGS_FOLDER_ID beats deprecated COLAB_DRIVE_SONGS_FOLDER_ID
        s_new_env = s_env.model_copy(update={"song_drive_songs_folder_id": "new_env_folder_abcdef"})
        reloaded2 = SongDriveStore(s_new_env, store, http=http)
        await reloaded2.initialize()
        assert reloaded2.drive_songs_folder_id == "new_env_folder_abcdef"
        assert reloaded2.config_source == "env"


async def test_drive_scope_checks_tokens_cache_and_download_isolation(
    settings, store, tmp_path: Path
):
    oauth = _write_oauth_json(
        tmp_path / "roaming-oauth.json",
        extra={"folder_id": "diary_folder_must_never_be_used"},
    )
    s = settings.model_copy(update={"song_drive_oauth_file": oauth})
    srv = _FakeGoogleDriveServer()

    song_data = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00song-bytes"
    srv.add_file("song_in_songs_1", name="rain.mp3", parent_id=srv.songs_id, data=song_data)
    srv.add_file(
        "song_trashed_1", name="del.mp3", parent_id=srv.songs_id, data=song_data, trashed=True
    )
    srv.add_file("file_in_inbox_1", name="orig.m4a", parent_id=srv.inbox_id, data=b"M4A_DATA")
    srv.add_file(
        "file_elsewhere_1", name="secret.md", parent_id="other_folder_99999", data=b"SECRET"
    )
    srv.add_file("mdl_pth_1", name="hakimi.pth", parent_id=srv.models_id, data=b"PTH")
    srv.add_file("mdl_idx_1", name="hakimi.index", parent_id=srv.models_id, data=b"IDX")

    w_key = compute_weight_cache_key("cover")
    archive_bytes = b"TAR_ARCHIVE_BYTES"
    archive_sha = hashlib.sha256(archive_bytes).hexdigest()
    srv.add_file("cache_arc_1", name=f"{w_key}.tar", parent_id=srv.cache_id, data=archive_bytes)
    manifest = {
        "version_key": w_key,
        "kind": "weights",
        "archive_file_id": "cache_arc_1",
        "sha256": archive_sha,
    }
    srv.add_file(
        "cache_man_1",
        name=f"{w_key}.manifest.json",
        parent_id=srv.cache_id,
        data=json.dumps(manifest).encode("utf-8"),
        mime_type="application/json",
    )

    async with srv.client() as http:
        drive = SongDriveStore(s, store, http=http)
        await drive.initialize()

        # Folder tree anchors at Hakimi_Songs, NEVER diary_folder_must_never_be_used
        tree = await drive.ensure_folder_tree()
        assert tree.root == srv.root_id
        assert tree.songs == srv.songs_id
        assert tree.inbox == srv.inbox_id
        assert tree.models == srv.models_id

        # Short-lived token never leaks refresh_token or client_secret
        tok = await drive.get_short_lived_token(min_ttl_seconds=1800)
        assert set(tok) == {"access_token", "token_type", "expires_in", "expires_at"}
        assert tok["access_token"].startswith("ya29.")
        assert tok["token_type"] == "Bearer"

        # 401 auto-refresh retries transparently
        srv.force_401_once = True
        inbox = await drive.list_inbox()
        assert len(inbox) == 1
        assert inbox[0]["id"] == "file_in_inbox_1"
        assert inbox[0]["mime_type"] == "audio/mpeg"
        assert srv.token_calls == 2

        # Resolve inbox by ID & filename, reject outside-scope
        assert (await drive.resolve_inbox_file(file_id="file_in_inbox_1"))["name"] == "orig.m4a"
        assert (await drive.resolve_inbox_file(filename="orig.m4a"))["id"] == "file_in_inbox_1"
        with pytest.raises(Rejected, match="song_drive_file_outside_scope"):
            await drive.resolve_inbox_file(file_id="song_in_songs_1")
        with pytest.raises(Rejected, match="source_not_found"):
            await drive.resolve_inbox_file(filename="missing.mp3")

        # Resolve models & cache manifest
        models = await drive.resolve_model_files("hakimi")
        assert models is not None
        assert models["pth"]["id"] == "mdl_pth_1"
        assert models["index"]["id"] == "mdl_idx_1"
        assert await drive.resolve_model_files("missing_model") is None

        resolved_cache = await drive.resolve_cache_manifest(w_key, expected_kind="weights")
        assert resolved_cache is not None
        assert resolved_cache["archive_file_id"] == "cache_arc_1"
        assert resolved_cache["sha256"] == archive_sha

        # download_song enforces parents == [tree.songs] AND trashed == False AND max_bytes
        assert await drive.download_song("song_in_songs_1") == song_data
        with pytest.raises(Rejected, match="song_drive_file_outside_scope"):
            await drive.download_song("file_in_inbox_1")
        with pytest.raises(Rejected, match="song_drive_file_outside_scope"):
            await drive.download_song("file_elsewhere_1")
        with pytest.raises(Rejected, match="song_drive_file_outside_scope"):
            await drive.download_song("song_trashed_1")
        with pytest.raises(Rejected, match="song_drive_file_too_large"):
            await drive.download_song("song_in_songs_1", max_bytes=10)

        # upload_song stores under songs/
        up_song = await drive.upload_song("cover_demo.mp3", song_data, title="雨爱")
        assert up_song["file_id"] in srv.files
        assert srv.files[up_song["file_id"]]["parents"] == [srv.songs_id]


async def test_resumable_stream_upload_chunks_exact_boundaries_memory_and_abort(
    settings, store, tmp_path: Path
):
    assert RESUMABLE_CHUNK_BYTES == 2 * 1024 * 1024
    assert RESUMABLE_CHUNK_BYTES % RESUMABLE_CHUNK_GRANULARITY == 0

    oauth = _write_oauth_json(tmp_path / "oauth.json")
    s = settings.model_copy(update={"song_drive_oauth_file": oauth})
    srv = _FakeGoogleDriveServer()

    async with srv.client() as http:
        chunk_sz = RESUMABLE_CHUNK_GRANULARITY  # 256 KiB for fast multi-chunk testing
        drive = SongDriveStore(s, store, http=http, chunk_bytes=chunk_sz)
        await drive.initialize()

        # 1) Non-multiple multi-chunk streamed in irregular pieces (17 KiB + 400 KiB + tail)
        payload = os.urandom(chunk_sz * 2 + 12345)

        async def _irregular_stream() -> AsyncIterator[bytes]:
            yield b""
            yield payload[:17_000]
            yield payload[17_000 : 17_000 + 400_000]
            yield payload[17_000 + 400_000 :]

        res = await drive.upload_inbox_stream(
            "multi_chunk.flac",
            _irregular_stream(),
            mime_type="audio/flac",
        )
        assert res["size"] == len(payload)
        assert srv.file_bytes[res["file_id"]] == payload
        assert srv.files[res["file_id"]]["parents"] == [srv.inbox_id]
        # Intermediate chunks used /* and exact 256 KiB multiple; final used /total
        assert srv.resumable_put_ranges == [
            (f"bytes 0-{chunk_sz - 1}/*", chunk_sz),
            (f"bytes {chunk_sz}-{chunk_sz * 2 - 1}/*", chunk_sz),
            (f"bytes {chunk_sz * 2}-{len(payload) - 1}/{len(payload)}", 12345),
        ]

        # 2) Exact multiple of chunk_size (2 * 256 KiB) handles boundary cleanly
        srv.resumable_put_ranges.clear()
        exact_payload = b"E" * (chunk_sz * 2)

        async def _exact_stream() -> AsyncIterator[bytes]:
            for i in range(0, len(exact_payload), 65_536):
                yield exact_payload[i : i + 65_536]

        res_exact = await drive.upload_inbox_stream(
            "exact.mp3",
            _exact_stream(),
            mime_type="audio/mpeg",
            content_length=len(exact_payload),
        )
        assert srv.file_bytes[res_exact["file_id"]] == exact_payload
        assert srv.resumable_put_ranges == [
            (f"bytes 0-{chunk_sz - 1}/*", chunk_sz),
            (f"bytes {chunk_sz}-{chunk_sz * 2 - 1}/{len(exact_payload)}", chunk_sz),
        ]

        # 3) Bounded peak memory while streaming 6 MiB through 256 KiB chunks (<= 4 MiB peak!)
        total_6m = 6 * 1024 * 1024
        piece = b"M" * 65_536
        # Avoid holding all bytes inside the test fake server during memory measurement
        srv_mem_count = 0
        srv.resumable_put_ranges.clear()

        def _sink_handler(request: httpx.Request) -> httpx.Response:
            nonlocal srv_mem_count
            parsed = urlsplit(str(request.url))
            if parsed.path.startswith("/upload/drive/v3/resumable/"):
                cr = request.headers.get("content-range", "")
                srv_mem_count += len(request.content)
                if cr.endswith("/*"):
                    end = int(cr.split("-")[1].split("/")[0])
                    return httpx.Response(308, headers={"Range": f"bytes=0-{end}"})
                return httpx.Response(
                    200,
                    json={
                        "id": "drv_mem_ok_0001",
                        "name": "big_stream.wav",
                        "mimeType": "audio/wav",
                        "size": str(srv_mem_count),
                    },
                )
            return srv._handle(request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_sink_handler),
            follow_redirects=False,
            trust_env=False,
        ) as mem_http:
            drive_mem = SongDriveStore(s, store, http=mem_http, chunk_bytes=chunk_sz)
            await drive_mem.initialize()

            async def _gen_6m() -> AsyncIterator[bytes]:
                for _ in range(total_6m // len(piece)):
                    yield piece

            tracemalloc.start()
            try:
                mem_res = await drive_mem.upload_inbox_stream(
                    "big_stream.wav",
                    _gen_6m(),
                    mime_type="audio/wav",
                )
                _, peak_bytes = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            assert mem_res["size"] == total_6m
            assert srv_mem_count == total_6m
            assert peak_bytes <= 4 * 1024 * 1024

        # 4) Oversized stream aborts mid-way and cleans up Drive resumable session
        async def _oversized_stream() -> AsyncIterator[bytes]:
            yield b"A" * chunk_sz
            yield b"B" * (chunk_sz + 10)

        with pytest.raises(Rejected, match="song_inbox_upload_too_large"):
            await drive.upload_inbox_stream(
                "too_big.mp3",
                _oversized_stream(),
                mime_type="audio/mpeg",
                max_bytes=chunk_sz + 5,
            )
        assert len(srv.aborted_sessions) == 1

        # 5) Empty stream rejected before starting any session
        req_count_before = len(srv.requests)

        async def _empty_stream() -> AsyncIterator[bytes]:
            yield b""

        with pytest.raises(Rejected, match="invalid_song_audio"):
            await drive.upload_inbox_stream("empty.mp3", _empty_stream(), mime_type="audio/mpeg")
        assert len(srv.requests) == req_count_before

        # 6) Disallowed Location header on resumable init blocked
        srv.bad_resumable_location = True

        async def _tiny_stream() -> AsyncIterator[bytes]:
            yield b"HELLO"

        with pytest.raises(Rejected, match="song_drive_disallowed_endpoint"):
            await drive.upload_inbox_stream("hijack.mp3", _tiny_stream(), mime_type="audio/mpeg")


async def test_song_playback_and_admin_drive_routes_work_without_colab_compute(
    settings, tmp_path: Path
):
    oauth = _write_oauth_json(tmp_path / "roaming-oauth.json")
    s = settings.model_copy(
        update={
            "data_dir": tmp_path / "app_data",
            "admin_token": SecretStr(ADMIN_TOKEN),
            "gdrive_oauth_credentials_file": oauth,
            "colab_worker_url": "",
            "colab_worker_token": SecretStr(""),
        }
    )
    srv = _FakeGoogleDriveServer()
    mp3_bytes = b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00" + b"X" * 400
    srv.add_file(
        "drv_song_direct_1", name="rain_chorus.mp3", parent_id=srv.songs_id, data=mp3_bytes
    )
    srv.add_file("drv_inbox_demo_1", name="晴天.m4a", parent_id=srv.inbox_id, data=b"ORIG_M4A")
    srv.folders["custom_root_folder_1234"] = {
        "id": "custom_root_folder_1234",
        "name": "Hakimi_Songs",
        "parents": ["root"],
        "mimeType": FOLDER_MIME,
        "trashed": False,
        "capabilities": {"canAddChildren": True},
    }

    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with srv.client() as mock_http:
        async with app.router.lifespan_context(app):
            svc = app.state.service
            lib: SongLibrary = svc.songs
            assert svc.colab_compute.configured() is False
            lib.drive._injected_http = mock_http

            # Register evicted Drive-backed song and play it with Colab completely unconfigured!
            entry = await lib.register_colab_cover(
                title="离线可播歌",
                artist="原唱",
                digest=hashlib.sha256(mp3_bytes).hexdigest(),
                byte_size=len(mp3_bytes),
                duration_ms=10_000,
                drive_chorus_id="drv_song_direct_1",
                chorus_bytes=None,
            )
            assert entry["cached_local"] is False
            raw, mime = await lib.audio_bytes(entry["id"])
            assert raw == mp3_bytes and mime == "audio/mpeg"

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            ) as client:
                # GET /admin/api/songs includes decoupled Drive fields
                songs_resp = await client.get("/admin/api/songs")
                assert songs_resp.status_code == 200
                songs_data = songs_resp.json()
                assert songs_data["drive_configured"] is True
                assert songs_data["colab_configured"] is False
                assert songs_data["drive_config_source"] == "default"
                assert songs_data["drive_folders"]["songs"] == srv.songs_id
                assert songs_data["drive_error"] is None

                # GET /admin/api/songs/storage
                st_resp = await client.get("/admin/api/songs/storage")
                assert st_resp.status_code == 200
                st_data = st_resp.json()
                assert st_data["configured"] is True
                assert st_data["folders"]["root"] == srv.root_id

                # PUT /admin/api/songs/storage validates ID and rejects "root" / unknown fields
                assert (
                    await client.put(
                        "/admin/api/songs/storage",
                        json={"drive_songs_folder_id": "root"},
                    )
                ).status_code == 400
                assert (
                    await client.put(
                        "/admin/api/songs/storage",
                        json={"drive_songs_folder_id": "custom_root_folder_1234", "extra": 1},
                    )
                ).status_code == 400
                put_ok = await client.put(
                    "/admin/api/songs/storage",
                    json={"drive_songs_folder_id": "custom_root_folder_1234"},
                )
                assert put_ok.status_code == 200
                assert put_ok.json()["drive_songs_folder_id"] == "custom_root_folder_1234"
                assert put_ok.json()["source"] == "admin"

                # DELETE /admin/api/songs/storage resets back to default
                del_ok = await client.delete("/admin/api/songs/storage")
                assert del_ok.status_code == 200
                assert del_ok.json()["drive_songs_folder_id"] == ""
                assert del_ok.json()["source"] == "default"
                assert del_ok.json()["folders"]["root"] == srv.root_id

                # GET /admin/api/songs/inbox
                inbox_resp = await client.get("/admin/api/songs/inbox")
                assert inbox_resp.status_code == 200
                inbox_data = inbox_resp.json()
                assert inbox_data["folders"]["inbox"] == srv.inbox_id
                assert inbox_data["items"][0]["name"] == "晴天.m4a"

                # POST /admin/api/songs/inbox streaming upload & security rejections
                audio_body = b"ID3\x03\x00\x00\x00\x00\x00\x00STREAM_INBOX_AUDIO"
                up_resp = await client.post(
                    "/admin/api/songs/inbox",
                    content=audio_body,
                    headers={
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": quote("夜に駆ける.mp3", safe=""),
                    },
                )
                assert up_resp.status_code == 200
                up_data = up_resp.json()
                assert up_data["name"] == "夜に駆ける.mp3"
                assert up_data["size"] == len(audio_body)
                assert srv.file_bytes[up_data["file_id"]] == audio_body

                # Reject compressed Content-Encoding (415)
                assert (
                    await client.post(
                        "/admin/api/songs/inbox",
                        content=audio_body,
                        headers={
                            "Content-Type": "audio/mpeg",
                            "Content-Encoding": "gzip",
                            "X-Drive-Filename": "ok.mp3",
                        },
                    )
                ).status_code == 415

                # Reject disallowed MIME (415)
                assert (
                    await client.post(
                        "/admin/api/songs/inbox",
                        content=audio_body,
                        headers={
                            "Content-Type": "text/plain",
                            "X-Drive-Filename": "ok.mp3",
                        },
                    )
                ).status_code == 415

                # Reject path traversal & disallowed extension (400)
                assert (
                    await client.post(
                        "/admin/api/songs/inbox",
                        content=audio_body,
                        headers={
                            "Content-Type": "audio/mpeg",
                            "X-Drive-Filename": quote("../evil.mp3", safe=""),
                        },
                    )
                ).status_code == 400
                assert (
                    await client.post(
                        "/admin/api/songs/inbox",
                        content=audio_body,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "X-Drive-Filename": "bad.exe",
                        },
                    )
                ).status_code == 400

                # Reject oversized Content-Length (> 100 MiB -> 413)
                assert (
                    await client.post(
                        "/admin/api/songs/inbox",
                        content=b"",
                        headers={
                            "Content-Type": "audio/mpeg",
                            "X-Drive-Filename": "huge.mp3",
                            "Content-Length": str(100 * 1024 * 1024 + 1),
                        },
                    )
                ).status_code == 413

                # Unauthenticated rejected (401)
                assert (
                    await client.get("/admin/api/songs/storage", headers={"Authorization": ""})
                ).status_code == 401


async def test_song_studio_dynamic_registration_and_pre_dispatch_contract(
    settings, store, tmp_path: Path
):
    oauth = _write_oauth_json(tmp_path / "roaming-oauth.json")
    s = settings.model_copy(update={"song_drive_oauth_file": oauth})
    srv = _FakeGoogleDriveServer()
    srv.add_file("inbox_101", name="rain.m4a", parent_id=srv.inbox_id, data=b"M4A")
    srv.add_file("mdl_pth_101", name="hakimi.pth", parent_id=srv.models_id, data=b"PTH")
    srv.add_file("mdl_idx_101", name="hakimi.index", parent_id=srv.models_id, data=b"IDX")

    cover_key = compute_weight_cache_key("cover")
    train_key = compute_weight_cache_key("all")
    cover_sha = hashlib.sha256(b"COVER_CACHE").hexdigest()
    train_sha = hashlib.sha256(b"TRAIN_CACHE").hexdigest()
    srv.add_file(
        "arc_cover_1", name=f"{cover_key}.tar", parent_id=srv.cache_id, data=b"COVER_CACHE"
    )
    srv.add_file(
        "man_cover_1",
        name=f"{cover_key}.manifest.json",
        parent_id=srv.cache_id,
        data=json.dumps(
            {
                "version_key": cover_key,
                "kind": "weights",
                "archive_file_id": "arc_cover_1",
                "sha256": cover_sha,
            }
        ).encode(),
    )
    srv.add_file(
        "arc_train_1", name=f"{train_key}.tar", parent_id=srv.cache_id, data=b"TRAIN_CACHE"
    )
    srv.add_file(
        "man_train_1",
        name=f"{train_key}.manifest.json",
        parent_id=srv.cache_id,
        data=json.dumps(
            {
                "version_key": train_key,
                "kind": "weights",
                "archive_file_id": "arc_train_1",
                "sha256": train_sha,
            }
        ).encode(),
    )

    class _RegisteringCompute:
        def __init__(self) -> None:
            self.registered: dict[str, dict[str, Any]] = {}

        def configured(self) -> bool:
            return True

        def register_job_type(
            self,
            job_type: str,
            *,
            validator: Any,
            pre_dispatch: Any = None,
            on_complete: Any = None,
        ) -> None:
            self.registered[job_type] = {
                "validator": validator,
                "pre_dispatch": pre_dispatch,
                "on_complete": on_complete,
            }

    compute = _RegisteringCompute()

    class _FakeColabMod:
        module_id = "colab_compute"

        def __init__(self) -> None:
            self.compute = compute

    class _FakeDriveMod:
        module_id = "drive_sync"

    registry = ModuleRegistry()
    registry.systems["colab_compute"] = _FakeColabMod()  # type: ignore[assignment]
    registry.systems["drive_sync"] = _FakeDriveMod()  # type: ignore[assignment]
    resources = SystemResources(
        settings=s,
        store=store,
        gateway=FakeGateway(),  # type: ignore[arg-type]
        http=None,
        service=SimpleNamespace(),  # type: ignore[arg-type]
    )

    async with srv.client() as http:
        mod = SongStudioModule(resources, registry)
        mod.drive._injected_http = http
        await mod.initialize()

        assert set(compute.registered) == {"rvc_cover", "rvc_train"}
        assert compute.registered["rvc_cover"]["on_complete"] == mod._on_rvc_cover_completed

        # Validators match strict Pydantic schemas & defaults
        cover_val = compute.registered["rvc_cover"]["validator"]
        train_val = compute.registered["rvc_train"]["validator"]
        norm_cover = cover_val({"title": "雨爱", "auto_search": True})
        assert (
            norm_cover["index_rate"],
            norm_cover["protect"],
            norm_cover["volume_envelope"],
        ) == (0.25, 0.28, 0.25)
        with pytest.raises(Rejected, match="song_source_required"):
            cover_val({"title": "雨爱", "auto_search": False})
        with pytest.raises(Rejected, match="colab_invalid_job"):
            cover_val({"title": "雨爱", "unknown_field": 1})

        norm_train = train_val({})
        assert (
            norm_train["epochs"],
            norm_train["save_every"],
            norm_train["batch_size"],
            norm_train["sample_rate"],
        ) == (150, 30, 12, 48000)
        assert RvcCoverJobPayload(title="t").protect == 0.28
        assert RvcTrainJobPayload().sample_rate == 48000

        # pre_dispatch for rvc_cover returns exact transient dispatch_context contract
        cover_ctx = await compute.registered["rvc_cover"]["pre_dispatch"](
            {
                "id": "job-cover-1",
                "job_type": "rvc_cover",
                "payload": {"title": "雨爱", "inbox_filename": "rain.m4a", "model_name": "hakimi"},
            }
        )
        assert set(cover_ctx) == {"drive"}
        drv = cover_ctx["drive"]
        assert set(drv) == {"access_token", "folders", "files"}
        assert drv["access_token"].startswith("ya29.")
        assert "refresh_token" not in json.dumps(cover_ctx)
        assert "GOCSPX-" not in json.dumps(cover_ctx)
        assert drv["folders"] == {
            "root": srv.root_id,
            "models": srv.models_id,
            "songs": srv.songs_id,
            "inbox": srv.inbox_id,
            "cache": srv.cache_id,
        }
        assert drv["files"] == {
            "inbox_file_id": "inbox_101",
            "inbox_name": "rain.m4a",
            "pth_file_id": "mdl_pth_101",
            "index_file_id": "mdl_idx_101",
            "cache_weights_file_id": "arc_cover_1",
            "cache_weights_sha256": cover_sha,
        }

        # Missing inbox file raises source_not_found during pre_dispatch
        with pytest.raises(Rejected, match="source_not_found"):
            await compute.registered["rvc_cover"]["pre_dispatch"](
                {
                    "id": "job-cover-miss",
                    "job_type": "rvc_cover",
                    "payload": {"title": "雨爱", "inbox_filename": "nonexistent.m4a"},
                }
            )

        # pre_dispatch for rvc_train resolves "all" weight cache manifest
        train_ctx = await compute.registered["rvc_train"]["pre_dispatch"](
            {"id": "job-train-1", "job_type": "rvc_train", "payload": {"model_name": "hakimi"}}
        )
        assert train_ctx["drive"]["access_token"].startswith("ya29.")
        assert train_ctx["drive"]["folders"]["cache"] == srv.cache_id
        assert train_ctx["drive"]["files"] == {
            "inbox_file_id": "",
            "inbox_name": "",
            "pth_file_id": "",
            "index_file_id": "",
            "cache_weights_file_id": "arc_train_1",
            "cache_weights_sha256": train_sha,
        }

        # Unconfigured Drive raises song_drive_not_configured in pre_dispatch
        mod.drive.s = s.model_copy(
            update={"song_drive_oauth_file": None, "gdrive_oauth_credentials_file": None}
        )
        with pytest.raises(Rejected, match="song_drive_not_configured"):
            await compute.registered["rvc_cover"]["pre_dispatch"](
                {"id": "job-unconf", "job_type": "rvc_cover", "payload": {"title": "雨爱"}}
            )
