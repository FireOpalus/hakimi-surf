"""Scoped Google Drive client for Hakimi_Songs/ (`models/`, `songs/`, `inbox/`).

Stdlib-only so it runs across host `.venv`, `.colab-venv`, and remote Colab runtimes.
Strictly confines all Drive file and folder operations to the `Hakimi_Songs/` tree,
never modifies or overwrites `secrets/roaming-oauth.json` (read-only fallback), and
never logs or leaks OAuth client secrets, refresh tokens, or bearer access tokens.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.server
import json
import os
import re
import secrets
import stat
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file"
DRIVE_FULL_SCOPE = "https://www.googleapis.com/auth/drive"
AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"
DRIVE_FILES_API = "https://www.googleapis.com/drive/v3/files"
DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
SUBFOLDER_NAMES: tuple[str, ...] = ("models", "songs", "inbox")
ROOT_FOLDER_NAME = "Hakimi_Songs"

FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
FOLDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,128}$")
SAFE_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}$")

MAX_CREDENTIALS_BYTES = 65_536
MAX_JSON_RESPONSE_BYTES = 1_048_576
MULTIPART_THRESHOLD_BYTES = 4 * 1024 * 1024
RESUMABLE_CHUNK_BYTES = 4 * 1024 * 1024
MAX_DRIVE_UPLOAD_BYTES = 250 * 1024 * 1024
MAX_INBOX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_SONG_DOWNLOAD_BYTES = 30 * 1024 * 1024

DriveRole = Literal["root", "models", "songs", "inbox"]
DriveSubRole = Literal["models", "songs", "inbox"]
TransportFn = Callable[
    [str, str, dict[str, str], bytes | None, float],
    tuple[int, dict[str, str], bytes],
]


class ColabDriveError(RuntimeError):
    """Error raised for Drive auth, validation, or scoping violations."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True)
class DriveFolderTree:
    """Resolved folder IDs for Hakimi_Songs/ and its three scoped subfolders."""

    root: str
    models: str
    songs: str
    inbox: str

    def as_dict(self) -> dict[str, str]:
        return {
            "root": self.root,
            "models": self.models,
            "songs": self.songs,
            "inbox": self.inbox,
        }

    def folder_for(self, role: DriveRole) -> str:
        mapping = self.as_dict()
        if role not in mapping:
            raise ColabDriveError("drive_invalid_role", f"Unsupported Drive folder role: {role}")
        return mapping[role]


def resolve_drive_credentials_path(
    explicit_path: Path | None = None,
    *,
    repo_root: Path | None = None,
) -> Path | None:
    """Resolve Drive OAuth credential path, decoupled from Colab ADC auth.

    Priority:
      1) Explicit `explicit_path`
      2) `COLAB_DRIVE_OAUTH_FILE` / `HAKIMI_COLAB_DRIVE_OAUTH_FILE`
      3) `secrets/colab-drive-oauth.json`
      4) `secrets/roaming-oauth.json` (read-only fallback)
      5) `secrets/colab-adc.json` (if it happens to carry Drive scope)
    """
    if explicit_path is not None:
        return explicit_path
    for env_key in ("COLAB_DRIVE_OAUTH_FILE", "HAKIMI_COLAB_DRIVE_OAUTH_FILE"):
        raw = os.environ.get(env_key, "").strip()
        if raw:
            return Path(raw)
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[1]
    candidates = (
        root / "secrets" / "colab-drive-oauth.json",
        root / "secrets" / "roaming-oauth.json",
        root / "secrets" / "colab-adc.json",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]


def validate_drive_id(value: str, *, is_folder: bool = False) -> str:
    """Validate a Google Drive file or folder ID strictly."""
    if not isinstance(value, str):
        raise ColabDriveError("drive_invalid_id", "Drive ID must be a string")
    cleaned = value.strip()
    pattern = FOLDER_ID_RE if is_folder else FILE_ID_RE
    if not cleaned or cleaned == "root" or not pattern.fullmatch(cleaned):
        raise ColabDriveError("drive_invalid_id", "Invalid Google Drive identifier")
    return cleaned


def sanitize_drive_filename(name: str) -> str:
    """Validate and normalize a flat filename without path traversal segments."""
    if not isinstance(name, str):
        raise ColabDriveError("drive_invalid_filename", "Filename must be a string")
    cleaned = Path(name.strip()).name.strip()
    if (
        not cleaned
        or cleaned in {".", ".."}
        or ".." in cleaned
        or "/" in name
        or "\\" in name
        or not SAFE_FILENAME_RE.fullmatch(cleaned)
    ):
        raise ColabDriveError("drive_invalid_filename", "Invalid Drive filename")
    return cleaned


def _escape_drive_query_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _read_private_json(path: Path) -> dict[str, Any]:
    """Read an OAuth JSON file safely without following symlinks."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_CREDENTIALS_BYTES:
                raise ColabDriveError("drive_credentials_invalid", "Invalid credential file")
            if info.st_mode & 0o002:
                raise ColabDriveError(
                    "drive_credentials_invalid", "Credential file must not be world-writable"
                )
            data = json.loads(stream.read(MAX_CREDENTIALS_BYTES + 1).decode("utf-8"))
    except ColabDriveError:
        raise
    except (OSError, ValueError, TypeError, UnicodeDecodeError):
        raise ColabDriveError("drive_credentials_invalid", "Cannot read Drive JSON file") from None
    if not isinstance(data, dict):
        raise ColabDriveError("drive_credentials_invalid", "Invalid credential JSON structure")
    return data


def _read_authorized_user(path: Path) -> dict[str, Any]:
    data = _read_private_json(path)
    if data.get("type") != "authorized_user":
        raise ColabDriveError("drive_credentials_invalid", "Expected authorized_user credentials")
    token_uri = str(data.get("token_uri") or TOKEN_URI).strip()
    if token_uri != TOKEN_URI:
        raise ColabDriveError("drive_credentials_invalid", "Unexpected OAuth token_uri")
    for field in ("client_id", "client_secret", "refresh_token"):
        val = data.get(field)
        if not isinstance(val, str) or not val.strip():
            raise ColabDriveError("drive_credentials_invalid", f"Missing credential field: {field}")
    return data


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, D102
        raise urllib.error.HTTPError(req.full_url, code, "Redirects are forbidden", headers, fp)


_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _default_transport(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout: float,
) -> tuple[int, dict[str, str], bytes]:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc not in {
        "oauth2.googleapis.com",
        "www.googleapis.com",
    }:
        raise ColabDriveError("drive_disallowed_endpoint", "Disallowed Drive API endpoint")
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            payload = resp.read(MAX_DRIVE_UPLOAD_BYTES + 1)
            return int(resp.status), resp_headers, payload
    except urllib.error.HTTPError as exc:
        resp_headers = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}
        err_body = exc.read(MAX_JSON_RESPONSE_BYTES) if exc.fp is not None else b""
        return int(exc.code), resp_headers, err_body
    except urllib.error.URLError as exc:
        raise ColabDriveError(
            "drive_network_error", f"Drive network error: {type(exc.reason).__name__}"
        ) from None
    except TimeoutError:
        raise ColabDriveError("drive_network_timeout", "Drive API request timed out") from None


def _has_drive_scope(scope_value: Any) -> bool:
    if isinstance(scope_value, str):
        scopes = set(scope_value.split())
    elif isinstance(scope_value, (list, tuple, set)):
        scopes = {str(s).strip() for s in scope_value}
    else:
        return False
    return bool(scopes & {DRIVE_FILE_SCOPE, DRIVE_FULL_SCOPE})


class ColabDriveClient:
    """Scoped Google Drive REST client limited strictly to Hakimi_Songs/.

    Reads OAuth credentials strictly read-only and caches short-lived access
    tokens in RAM only (never modifies `secrets/roaming-oauth.json`).
    """

    def __init__(
        self,
        credentials_path: Path | None = None,
        *,
        root_folder_id: str = "",
        transport: TransportFn | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.credentials_path = credentials_path
        self._configured_root_id = root_folder_id.strip()
        self._transport: TransportFn = transport or _default_transport
        self._timeout = float(timeout)
        self._cred_info: dict[str, Any] | None = None
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._tree: DriveFolderTree | None = None

    @property
    def folder_tree(self) -> DriveFolderTree | None:
        return self._tree

    def _load_cred_info(self) -> dict[str, Any]:
        if self._cred_info is not None:
            return self._cred_info
        if self.credentials_path is None:
            raise ColabDriveError("drive_credentials_missing", "Drive credentials path not set")
        self._cred_info = _read_authorized_user(self.credentials_path)
        return self._cred_info

    def _get_access_token(self, *, force_refresh: bool = False) -> str:
        now = time.monotonic()
        if not force_refresh and self._access_token and now < self._token_expires_at:
            return self._access_token

        info = self._load_cred_info()
        form = urllib.parse.urlencode(
            {
                "grant_type": "refresh_token",
                "client_id": info["client_id"].strip(),
                "client_secret": info["client_secret"].strip(),
                "refresh_token": info["refresh_token"].strip(),
            }
        ).encode("utf-8")
        status, _, raw = self._transport(
            "POST",
            TOKEN_URI,
            {"Content-Type": "application/x-www-form-urlencoded"},
            form,
            min(self._timeout, 20.0),
        )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise ColabDriveError(
                "drive_token_failed", f"Invalid token response (HTTP {status})"
            ) from None

        if status != 200 or not isinstance(payload, dict):
            err_code = str(payload.get("error") or "") if isinstance(payload, dict) else ""
            if err_code in {"invalid_scope", "unauthorized_client"}:
                raise ColabDriveError("drive_insufficient_scope", f"OAuth token error: {err_code}")
            if err_code == "invalid_grant":
                raise ColabDriveError(
                    "drive_reauth_required", "OAuth refresh token expired or revoked"
                )
            raise ColabDriveError(
                "drive_token_failed", f"OAuth token refresh failed (HTTP {status})"
            )

        returned_scope = payload.get("scope")
        if returned_scope is not None and not _has_drive_scope(returned_scope):
            raise ColabDriveError(
                "drive_insufficient_scope",
                "Credentials lack https://www.googleapis.com/auth/drive.file scope",
            )

        token = payload.get("access_token")
        expires_in = payload.get("expires_in", 3600)
        if not isinstance(token, str) or not token.strip():
            raise ColabDriveError("drive_token_failed", "Missing access_token in OAuth response")
        ttl = max(60, min(3600, int(expires_in))) if isinstance(expires_in, (int, float)) else 3600
        self._access_token = token.strip()
        self._token_expires_at = time.monotonic() + ttl - 30
        return self._access_token

    def get_short_lived_token(self) -> str:
        """Return a valid short-lived Bearer access token for optional direct Drive transfer."""
        return self._get_access_token(force_refresh=False)

    def _api_request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout: float | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        full_url = url
        if params:
            query = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
            full_url = f"{url}?{query}"

        for attempt in range(2):
            token = self._get_access_token(force_refresh=(attempt > 0))
            req_headers = {"Authorization": f"Bearer {token}"}
            if headers:
                req_headers.update(headers)
            status, resp_headers, resp_body = self._transport(
                method,
                full_url,
                req_headers,
                body,
                timeout if timeout is not None else self._timeout,
            )
            if status == 401 and attempt == 0:
                self._access_token = ""
                continue
            return status, resp_headers, resp_body
        raise ColabDriveError("drive_token_failed", "Drive authorization failed after refresh")

    def _api_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {}
        body: bytes | None = None
        if json_body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
            body = json.dumps(json_body, ensure_ascii=False).encode("utf-8")

        status, _, raw = self._api_request(method, url, params=params, headers=headers, body=body)
        if len(raw) > MAX_JSON_RESPONSE_BYTES:
            raise ColabDriveError("drive_response_too_large", "Drive JSON response exceeded limit")
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            raise ColabDriveError(
                "drive_invalid_response", f"Drive returned non-JSON response (HTTP {status})"
            ) from None

        if status == 403:
            reason = ""
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):
                    errors = err.get("errors") or []
                    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                        reason = str(errors[0].get("reason") or "")
            if reason in {"insufficientPermissions", "accessNotConfigured"}:
                raise ColabDriveError(
                    "drive_insufficient_scope", f"Google Drive permission denied: {reason}"
                )
            raise ColabDriveError("drive_forbidden", "Google Drive access denied (HTTP 403)")
        if status == 404:
            raise ColabDriveError("drive_not_found", "Drive object not found")
        if status not in {200, 201}:
            raise ColabDriveError("drive_request_failed", f"Drive API error (HTTP {status})")
        if not isinstance(data, dict):
            raise ColabDriveError("drive_invalid_response", "Drive JSON payload is not an object")
        return data

    def _find_or_create_folder(
        self,
        name: str,
        parent_id: str,
        *,
        role: DriveRole,
    ) -> str:
        safe_name = _escape_drive_query_literal(name)
        safe_parent = _escape_drive_query_literal(parent_id)
        q = (
            f"'{safe_parent}' in parents and mimeType = '{FOLDER_MIME}' "
            f"and name = '{safe_name}' and trashed = false"
        )
        listing = self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,mimeType,trashed,appProperties)",
                "pageSize": 10,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        files = listing.get("files") or []
        if files:
            for item in files:
                props = item.get("appProperties") or {}
                if props.get("hakimi_songs_role") == role:
                    return validate_drive_id(str(item["id"]), is_folder=True)
            return validate_drive_id(str(files[0]["id"]), is_folder=True)

        body: dict[str, Any] = {
            "name": name,
            "mimeType": FOLDER_MIME,
            "appProperties": {"hakimi_songs_role": role},
        }
        if parent_id != "root":
            body["parents"] = [parent_id]
        created = self._api_json(
            "POST",
            DRIVE_FILES_API,
            params={"fields": "id,name,mimeType,appProperties", "supportsAllDrives": "true"},
            json_body=body,
        )
        return validate_drive_id(str(created["id"]), is_folder=True)

    def ensure_folder_tree(
        self,
        root_folder_id: str = "",
        *,
        force_refresh: bool = False,
    ) -> DriveFolderTree:
        """Idempotently resolve or create Hakimi_Songs/ and models/, songs/, inbox/."""
        requested_root = root_folder_id.strip() or self._configured_root_id
        if (
            not force_refresh
            and self._tree is not None
            and (not requested_root or self._tree.root == requested_root)
        ):
            return self._tree

        if requested_root:
            root_id = validate_drive_id(requested_root, is_folder=True)
            meta = self._api_json(
                "GET",
                f"{DRIVE_FILES_API}/{root_id}",
                params={
                    "fields": "id,name,mimeType,trashed,capabilities(canAddChildren),appProperties",
                    "supportsAllDrives": "true",
                },
            )
            if meta.get("mimeType") != FOLDER_MIME or meta.get("trashed"):
                raise ColabDriveError(
                    "drive_folder_unavailable", "Configured root ID is not a folder"
                )
            if meta.get("capabilities", {}).get("canAddChildren") is False:
                raise ColabDriveError("drive_folder_unavailable", "Root folder is not writable")
        else:
            # Never inherit folder_id from roaming-oauth.json; always anchor in My Drive root.
            root_id = self._find_or_create_folder(ROOT_FOLDER_NAME, "root", role="root")

        sub_ids: dict[str, str] = {}
        for sub in SUBFOLDER_NAMES:
            role_name: DriveRole = sub  # type: ignore[assignment]
            sub_ids[sub] = self._find_or_create_folder(sub, root_id, role=role_name)

        self._tree = DriveFolderTree(
            root=root_id,
            models=sub_ids["models"],
            songs=sub_ids["songs"],
            inbox=sub_ids["inbox"],
        )
        return self._tree

    def list_folder(
        self,
        role: DriveSubRole,
        *,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """List non-trashed files directly inside models/, songs/, or inbox/."""
        tree = self.ensure_folder_tree()
        parent_id = tree.folder_for(role)
        bounded_size = max(1, min(int(page_size), 200))
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,mimeType,size,modifiedTime,appProperties)",
                "orderBy": "modifiedTime desc",
                "pageSize": bounded_size,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        items: list[dict[str, Any]] = []
        for raw in listing.get("files") or []:
            try:
                fid = validate_drive_id(str(raw.get("id") or ""))
            except ColabDriveError:
                continue
            raw_size = raw.get("size")
            size_val = int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0
            items.append(
                {
                    "id": fid,
                    "name": str(raw.get("name") or ""),
                    "size": size_val,
                    "modified_time": str(raw.get("modifiedTime") or ""),
                    "mime_type": str(raw.get("mimeType") or "application/octet-stream"),
                }
            )
        return items

    def list_inbox(self) -> list[dict[str, Any]]:
        """Return inbox items formatted for ColabComputeHub.list_drive_inbox()."""
        return [
            {
                "id": item["id"],
                "name": item["name"],
                "size": item["size"],
                "modified_time": item["modified_time"],
            }
            for item in self.list_folder("inbox")
        ]

    def verify_file_in_tree(
        self,
        file_id: str,
        *,
        allowed_roles: tuple[DriveRole, ...] = ("models", "songs", "inbox"),
    ) -> dict[str, Any]:
        """Verify that a Drive file ID exists, is not trashed, and lives under Hakimi_Songs/."""
        fid = validate_drive_id(file_id)
        tree = self.ensure_folder_tree()
        allowed_parents = {tree.folder_for(role) for role in allowed_roles}
        meta = self._api_json(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={
                "fields": "id,name,parents,mimeType,size,modifiedTime,trashed,appProperties",
                "supportsAllDrives": "true",
            },
        )
        if meta.get("trashed") or meta.get("mimeType") == FOLDER_MIME:
            raise ColabDriveError("drive_file_outside_scope", "Drive object is trashed or a folder")
        parents = {str(p) for p in (meta.get("parents") or [])}
        if not (parents & allowed_parents):
            raise ColabDriveError(
                "drive_file_outside_scope",
                "Requested Drive file is outside Hakimi_Songs/ scope",
            )
        return meta

    def resolve_inbox_file(
        self,
        *,
        file_id: str = "",
        filename: str = "",
    ) -> dict[str, Any]:
        """Resolve an inbox song by Drive file ID or exact filename inside Hakimi_Songs/inbox/."""
        if file_id.strip():
            meta = self.verify_file_in_tree(file_id.strip(), allowed_roles=("inbox",))
            return {
                "id": str(meta["id"]),
                "name": str(meta.get("name") or ""),
                "size": int(meta["size"]) if str(meta.get("size") or "").isdigit() else 0,
                "modified_time": str(meta.get("modifiedTime") or ""),
            }
        clean_name = sanitize_drive_filename(filename)
        matches = [item for item in self.list_inbox() if item["name"] == clean_name]
        if not matches:
            raise ColabDriveError("source_not_found", f"Inbox file not found: {clean_name}")
        return matches[0]

    def resolve_named_file(
        self,
        role: DriveSubRole,
        filename: str,
    ) -> dict[str, Any] | None:
        """Find a single non-trashed file by exact filename within a scoped subfolder."""
        clean_name = sanitize_drive_filename(filename)
        tree = self.ensure_folder_tree()
        parent_id = tree.folder_for(role)
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and name = '{_escape_drive_query_literal(clean_name)}' "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,size,modifiedTime,mimeType)",
                "pageSize": 5,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        files = listing.get("files") or []
        if not files:
            return None
        first = files[0]
        raw_size = first.get("size")
        return {
            "id": validate_drive_id(str(first["id"])),
            "name": str(first.get("name") or clean_name),
            "size": int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0,
            "modified_time": str(first.get("modifiedTime") or ""),
            "mime_type": str(first.get("mimeType") or "application/octet-stream"),
        }

    def download_bytes(
        self,
        file_id: str,
        *,
        allowed_roles: tuple[DriveRole, ...] = ("songs",),
        max_bytes: int = MAX_SONG_DOWNLOAD_BYTES,
    ) -> bytes:
        """Download a strictly scoped Drive file's bytes with size enforcement."""
        meta = self.verify_file_in_tree(file_id, allowed_roles=allowed_roles)
        raw_size = meta.get("size")
        if raw_size is not None and str(raw_size).isdigit() and int(raw_size) > max_bytes:
            raise ColabDriveError(
                "drive_file_too_large", "Drive file exceeds maximum download size"
            )

        fid = str(meta["id"])
        status, _, raw = self._api_request(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={"alt": "media", "supportsAllDrives": "true"},
            timeout=max(self._timeout, 60.0),
        )
        if status == 404:
            raise ColabDriveError("drive_not_found", "Drive file not found")
        if status != 200:
            raise ColabDriveError("drive_download_failed", f"Drive download failed (HTTP {status})")
        if len(raw) > max_bytes:
            raise ColabDriveError(
                "drive_file_too_large", "Drive download exceeded maximum byte limit"
            )
        return raw

    def download_file(
        self,
        file_id: str,
        dest_path: Path,
        *,
        allowed_roles: tuple[DriveRole, ...] = ("models", "songs", "inbox"),
        max_bytes: int = MAX_DRIVE_UPLOAD_BYTES,
    ) -> Path:
        """Download a scoped Drive file atomically to `dest_path`."""
        data = self.download_bytes(file_id, allowed_roles=allowed_roles, max_bytes=max_bytes)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".drive-", dir=dest_path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, dest_path)
        finally:
            Path(tmp_name).unlink(missing_ok=True)
        return dest_path

    def upload_bytes(
        self,
        role: DriveSubRole,
        filename: str,
        raw: bytes,
        *,
        mime_type: str = "application/octet-stream",
        replace_existing: bool = True,
        app_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Upload bounded bytes into one of models/, songs/, or inbox/."""
        clean_name = sanitize_drive_filename(filename)
        if not isinstance(raw, (bytes, bytearray)) or not raw:
            raise ColabDriveError("drive_invalid_payload", "Upload payload cannot be empty")
        data = bytes(raw)
        limit = MAX_INBOX_UPLOAD_BYTES if role == "inbox" else MAX_DRIVE_UPLOAD_BYTES
        if len(data) > limit:
            raise ColabDriveError("drive_file_too_large", "Upload payload exceeds allowed size")

        tree = self.ensure_folder_tree()
        parent_id = tree.folder_for(role)
        existing = self.resolve_named_file(role, clean_name) if replace_existing else None

        props = {"hakimi_songs_role": role}
        if app_properties:
            for k, v in app_properties.items():
                if isinstance(k, str) and isinstance(v, str) and k and len(v) <= 120:
                    props[k[:40]] = v

        if len(data) <= MULTIPART_THRESHOLD_BYTES:
            if existing is not None:
                resp = self._upload_media_patch(existing["id"], data, mime_type)
            else:
                resp = self._upload_multipart_create(
                    parent_id, clean_name, data, mime_type, app_properties=props
                )
        else:
            resp = self._upload_resumable(
                parent_id,
                clean_name,
                data,
                mime_type,
                existing_id=existing["id"] if existing else None,
                app_properties=props,
            )

        fid = validate_drive_id(str(resp["id"]))
        return {
            "file_id": fid,
            "name": str(resp.get("name") or clean_name),
            "size": len(data),
            "mime_type": str(resp.get("mimeType") or mime_type),
        }

    def upload_file(
        self,
        role: DriveSubRole,
        local_path: Path,
        *,
        drive_name: str = "",
        mime_type: str = "",
        replace_existing: bool = True,
        app_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Upload a local file into models/, songs/, or inbox/."""
        if not local_path.is_file():
            raise ColabDriveError("drive_local_file_missing", f"Local file missing: {local_path}")
        size = local_path.stat().st_size
        limit = MAX_INBOX_UPLOAD_BYTES if role == "inbox" else MAX_DRIVE_UPLOAD_BYTES
        if size <= 0 or size > limit:
            raise ColabDriveError("drive_file_too_large", "Local file size invalid for upload")
        name = sanitize_drive_filename(drive_name or local_path.name)
        resolved_mime = mime_type or _guess_mime(local_path.suffix.lower())
        raw = local_path.read_bytes()
        return self.upload_bytes(
            role,
            name,
            raw,
            mime_type=resolved_mime,
            replace_existing=replace_existing,
            app_properties=app_properties,
        )

    def ensure_model_files(self, model_name: str, local_dir: Path) -> tuple[Path, Path]:
        """Return local `(pth_path, index_path)`, pulling from Hakimi_Songs/models/ if absent."""
        clean_model = re.sub(r"[^A-Za-z0-9_-]", "", model_name.strip())
        if not clean_model:
            raise ColabDriveError("drive_invalid_filename", "Invalid RVC model name")
        pth_name = f"{clean_model}.pth"
        idx_name = f"{clean_model}.index"
        pth_local = local_dir / pth_name
        idx_local = local_dir / idx_name

        if not pth_local.is_file():
            remote_pth = self.resolve_named_file("models", pth_name)
            if remote_pth is None:
                raise ColabDriveError("model_not_found", f"Model weight missing: {pth_name}")
            self.download_file(remote_pth["id"], pth_local, allowed_roles=("models",))

        if not idx_local.is_file():
            remote_idx = self.resolve_named_file("models", idx_name)
            if remote_idx is None:
                raise ColabDriveError("model_not_found", f"Model index missing: {idx_name}")
            self.download_file(remote_idx["id"], idx_local, allowed_roles=("models",))

        return pth_local, idx_local

    def _upload_media_patch(self, file_id: str, data: bytes, mime_type: str) -> dict[str, Any]:
        status, _, raw = self._api_request(
            "PATCH",
            f"{DRIVE_UPLOAD_API}/{file_id}",
            params={
                "uploadType": "media",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers={"Content-Type": mime_type},
            body=data,
            timeout=max(self._timeout, 120.0),
        )
        if status not in {200, 201}:
            raise ColabDriveError("drive_upload_failed", f"Drive upload failed (HTTP {status})")
        return json.loads(raw.decode("utf-8"))

    def _upload_multipart_create(
        self,
        parent_id: str,
        filename: str,
        data: bytes,
        mime_type: str,
        *,
        app_properties: dict[str, str],
    ) -> dict[str, Any]:
        boundary = f"hakimi_{uuid.uuid4().hex}"
        metadata = json.dumps(
            {
                "name": filename,
                "parents": [parent_id],
                "appProperties": app_properties,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        body = b"".join(
            [
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode(),
                metadata,
                f"\r\n--{boundary}\r\nContent-Type: {mime_type}\r\n\r\n".encode(),
                data,
                f"\r\n--{boundary}--\r\n".encode(),
            ]
        )
        status, _, raw = self._api_request(
            "POST",
            DRIVE_UPLOAD_API,
            params={
                "uploadType": "multipart",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers={"Content-Type": f"multipart/related; boundary={boundary}"},
            body=body,
            timeout=max(self._timeout, 120.0),
        )
        if status not in {200, 201}:
            raise ColabDriveError(
                "drive_upload_failed", f"Drive multipart upload failed (HTTP {status})"
            )
        return json.loads(raw.decode("utf-8"))

    def _upload_resumable(
        self,
        parent_id: str,
        filename: str,
        data: bytes,
        mime_type: str,
        *,
        existing_id: str | None = None,
        app_properties: dict[str, str],
    ) -> dict[str, Any]:
        meta: dict[str, Any] = {"name": filename, "appProperties": app_properties}
        if existing_id is None:
            meta["parents"] = [parent_id]
        method = "PATCH" if existing_id else "POST"
        start_url = f"{DRIVE_UPLOAD_API}/{existing_id}" if existing_id else DRIVE_UPLOAD_API
        status, resp_headers, _ = self._api_request(
            method,
            start_url,
            params={
                "uploadType": "resumable",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": mime_type,
                "X-Upload-Content-Length": str(len(data)),
            },
            body=json.dumps(meta, ensure_ascii=False).encode("utf-8"),
        )
        upload_url = resp_headers.get("location", "")
        if status not in {200, 201} or not upload_url.startswith("https://www.googleapis.com/"):
            raise ColabDriveError(
                "drive_upload_failed", f"Drive resumable init failed (HTTP {status})"
            )

        total = len(data)
        offset = 0
        while offset < total:
            end = min(total, offset + RESUMABLE_CHUNK_BYTES)
            chunk = data[offset:end]
            req_headers = {
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {offset}-{end - 1}/{total}",
                "Content-Type": mime_type,
            }
            c_status, _, c_raw = self._transport(
                "PUT", upload_url, req_headers, chunk, max(self._timeout, 120.0)
            )
            if c_status == 308:
                offset = end
                continue
            if c_status in {200, 201}:
                return json.loads(c_raw.decode("utf-8"))
            raise ColabDriveError(
                "drive_upload_failed", f"Drive resumable chunk failed (HTTP {c_status})"
            )
        raise ColabDriveError(
            "drive_upload_failed", "Drive resumable upload ended without response"
        )


def _guess_mime(ext: str) -> str:
    return {
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".ogg": "audio/ogg",
        ".opus": "audio/opus",
        ".m4a": "audio/mp4",
        ".flac": "audio/flac",
        ".json": "application/json",
    }.get(ext, "application/octet-stream")


# ---------------------------------------------------------------------------
# Dedicated Drive OAuth PKCE authorization subcommand (never touches roaming-oauth.json)
# ---------------------------------------------------------------------------


def load_desktop_oauth_client(path: Path) -> dict[str, str]:
    """Read an installed desktop OAuth client JSON (`secrets/roaming-oauth-client.json`)."""
    raw = _read_private_json(path)
    installed = raw.get("installed")
    if not isinstance(installed, dict):
        raise ColabDriveError(
            "drive_oauth_client_invalid", "Expected desktop `installed` OAuth JSON"
        )
    client_id = str(installed.get("client_id") or "").strip()
    client_secret = str(installed.get("client_secret") or "").strip()
    auth_uri = str(installed.get("auth_uri") or AUTH_URI).strip()
    token_uri = str(installed.get("token_uri") or TOKEN_URI).strip()
    if (
        not client_id.endswith(".apps.googleusercontent.com")
        or not client_secret
        or auth_uri not in {AUTH_URI, "https://accounts.google.com/o/oauth2/auth"}
        or token_uri != TOKEN_URI
    ):
        raise ColabDriveError("drive_oauth_client_invalid", "Invalid desktop OAuth client fields")
    return {"client_id": client_id, "client_secret": client_secret}


def build_pkce_auth_url(
    client_id: str,
    redirect_uri: str,
    state: str,
    code_verifier: str,
    *,
    scope: str = DRIVE_FILE_SCOPE,
) -> str:
    """Build Google OAuth2 authorization URL with PKCE S256 challenge."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii")
    return (
        AUTH_URI
        + "?"
        + urllib.parse.urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "response_type": "code",
                "scope": scope,
                "state": state,
                "code_challenge": challenge.rstrip("="),
                "code_challenge_method": "S256",
                "access_type": "offline",
                "prompt": "consent select_account",
            }
        )
    )


def exchange_pkce_code(
    *,
    client_id: str,
    client_secret: str,
    code: str,
    code_verifier: str,
    redirect_uri: str,
    scope: str,
    transport: TransportFn = _default_transport,
) -> dict[str, Any]:
    """Exchange an authorization code for an authorized_user JSON credential dict."""
    form = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "code": code.strip(),
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    status, _, raw = transport(
        "POST",
        TOKEN_URI,
        {"Content-Type": "application/x-www-form-urlencoded"},
        form,
        20.0,
    )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise ColabDriveError(
            "drive_token_failed", f"Invalid token exchange response ({status})"
        ) from None
    if status != 200 or not isinstance(payload, dict):
        err = str(payload.get("error") or "") if isinstance(payload, dict) else ""
        raise ColabDriveError("drive_token_failed", f"OAuth exchange failed ({status}: {err})")
    refresh = payload.get("refresh_token")
    if not isinstance(refresh, str) or not refresh.strip():
        raise ColabDriveError("drive_reauth_required", "OAuth response missing refresh_token")
    granted_scopes = str(payload.get("scope") or scope).split()
    return {
        "type": "authorized_user",
        "client_id": client_id,
        "client_secret": client_secret,
        "token_uri": TOKEN_URI,
        "scopes": granted_scopes,
        "refresh_token": refresh.strip(),
    }


def _save_private_json(path: Path, payload: dict[str, Any]) -> None:
    resolved = path.absolute()
    if any(p.is_symlink() for p in (resolved, *resolved.parents)):
        raise ColabDriveError("drive_credentials_invalid", "Refusing to write through symlink")
    resolved.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".drive-oauth-", dir=resolved.parent)
    try:
        os.chmod(tmp_name, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, resolved)
        os.chmod(resolved, 0o600)
    finally:
        Path(tmp_name).unlink(missing_ok=True)


def run_drive_oauth(
    *,
    client_path: Path,
    out_path: Path,
    scope_choice: Literal["drive.file", "drive"] = "drive.file",
    port: int = 12144,
    timeout_seconds: float = 900.0,
) -> dict[str, Any]:
    """Run loopback PKCE OAuth authorization and store dedicated Drive credentials."""
    if not 1024 <= port <= 65535:
        raise ColabDriveError("drive_invalid_port", "Loopback OAuth port must be 1024..65535")
    client = load_desktop_oauth_client(client_path)
    scope = DRIVE_FULL_SCOPE if scope_choice == "drive" else DRIVE_FILE_SCOPE
    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    redirect_uri = f"http://127.0.0.1:{port}/oauth/callback"
    start_path = f"/start/{secrets.token_urlsafe(24)}"
    auth_url = build_pkce_auth_url(client["client_id"], redirect_uri, state, verifier, scope=scope)
    result_box: dict[str, str] = {}

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == start_path:
                self.send_response(303)
                self.send_header("Location", auth_url)
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return
            if parsed.path == "/oauth/callback":
                qs = urllib.parse.parse_qs(parsed.query)
                got_state = (qs.get("state") or [""])[0]
                if not hmac.compare_digest(got_state.encode(), state.encode()):
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Invalid state")
                    return
                if "error" in qs:
                    result_box["error"] = str((qs.get("error") or ["denied"])[0])
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Authorization denied")
                    return
                code = (qs.get("code") or [""])[0]
                if not code:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Missing code")
                    return
                result_box["code"] = code
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(b"Drive authorization received. You may close this page.")
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

    server = http.server.HTTPServer(("127.0.0.1", port), _Handler)
    server.timeout = 1.0
    deadline = time.monotonic() + timeout_seconds
    print(f"AUTH_URL=http://127.0.0.1:{port}{start_path}", flush=True)
    print(f"DIRECT_GOOGLE_AUTH_URL={auth_url}", flush=True)
    try:
        while time.monotonic() < deadline and not result_box:
            server.handle_request()
    finally:
        server.server_close()

    if "error" in result_box:
        raise ColabDriveError("oauth_consent_denied", f"OAuth denied: {result_box['error']}")
    code = result_box.get("code", "")
    if not code:
        raise ColabDriveError("oauth_timeout", "Timed out waiting for Drive OAuth callback")

    cred = exchange_pkce_code(
        client_id=client["client_id"],
        client_secret=client["client_secret"],
        code=code,
        code_verifier=verifier,
        redirect_uri=redirect_uri,
        scope=scope,
    )
    _save_private_json(out_path, cred)
    client_drive = ColabDriveClient(out_path)
    tree = client_drive.ensure_folder_tree()
    return {"out_path": str(out_path), "folders": tree.as_dict(), "scopes": cred["scopes"]}


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hakimi_Songs/ scoped Google Drive helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-folders", help="Idempotently initialize Hakimi_Songs/ tree.")
    p_init.add_argument("--cred-path", type=Path, default=None)
    p_init.add_argument("--root-folder-id", default="")

    p_auth = sub.add_parser("auth", help="Authorize dedicated Drive OAuth credentials via PKCE.")
    p_auth.add_argument(
        "--client",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "secrets" / "roaming-oauth-client.json",
    )
    p_auth.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "secrets" / "colab-drive-oauth.json",
    )
    p_auth.add_argument("--scope", choices=("drive.file", "drive"), default="drive.file")
    p_auth.add_argument("--port", type=int, default=12144)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_cli_parser().parse_args(argv)
    if args.command == "init-folders":
        cred = resolve_drive_credentials_path(args.cred_path)
        client = ColabDriveClient(cred, root_folder_id=args.root_folder_id)
        tree = client.ensure_folder_tree()
        print(json.dumps(tree.as_dict(), indent=2), flush=True)
        return 0
    if args.command == "auth":
        res = run_drive_oauth(
            client_path=args.client,
            out_path=args.out,
            scope_choice=args.scope,
            port=args.port,
        )
        print(json.dumps(res, indent=2), flush=True)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
