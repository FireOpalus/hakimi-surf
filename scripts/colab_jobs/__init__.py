"""Pluggable host-side Colab GPU job handler registry and ephemeral Drive primitives.

Strictly decoupled from Google Drive OAuth credential files and refresh tokens:
every Drive operation uses a per-job in-memory ephemeral access token supplied via
`DispatchContext` and enforces strict folder-parent scoping, non-folder / non-trashed
metadata verification, and secret scrubbing.
"""

from __future__ import annotations

import email.utils
import json
import os
import re
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

DRIVE_FILES_API = "https://www.googleapis.com/drive/v3/files"
DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
_MAX_JSON_RESPONSE_BYTES = 1_048_576
_MULTIPART_THRESHOLD_BYTES = 4 * 1024 * 1024
_RESUMABLE_CHUNK_BYTES = 4 * 1024 * 1024
_MAX_EPHEMERAL_DOWNLOAD_BYTES = 250 * 1024 * 1024
_MAX_EPHEMERAL_UPLOAD_BYTES = 250 * 1024 * 1024
_RETRYABLE_DRIVE_STATUSES = frozenset({429, 500, 502, 503, 504})
_MAX_DRIVE_RETRIES = 3
_RETRY_BASE_DELAY_SECONDS = 0.5
_RETRY_MAX_DELAY_SECONDS = 8.0


def _parse_retry_after(headers: dict[str, str] | None) -> float | None:
    if not headers:
        return None
    raw = str(headers.get("retry-after") or headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        val = float(raw)
        if val >= 0.0:
            return min(30.0, val)
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        delta = (dt - datetime.now(UTC)).total_seconds()
        return max(0.0, min(30.0, delta))
    except Exception:
        return None


VALID_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
VALID_JOB_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
VALID_ERROR_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{1,48}$")
VALID_SHA256_RE = re.compile(r"^[a-fA-F0-9]{64}$")
SAFE_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}$")
_OAUTH_TOKEN_RE = re.compile(r"(?:ya29\.[A-Za-z0-9._-]+|1//[A-Za-z0-9._-]+)")
_BEARER_TOKEN_RE = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}")

TransportFn = Callable[
    [str, str, dict[str, str], bytes | None, float],
    tuple[int, dict[str, str], bytes],
]


def sanitize_filename(name: str) -> str:
    """Validate a single-segment filename without path separators or control chars."""
    if not isinstance(name, str):
        raise ValueError("filename must be a string")
    cleaned = Path(name.strip()).name.strip()
    if (
        not cleaned
        or cleaned in {".", ".."}
        or ".." in cleaned
        or "/" in name
        or "\\" in name
        or not SAFE_FILENAME_RE.fullmatch(cleaned)
    ):
        raise ValueError("invalid filename")
    return cleaned


def validate_drive_id(value: str, *, allow_empty: bool = False) -> str:
    """Validate a Google Drive object ID, strictly forbidding 'root' variants."""
    if not isinstance(value, str):
        raise ValueError("Drive ID must be a string")
    cleaned = value.strip()
    if not cleaned:
        if allow_empty:
            return ""
        raise ValueError("Drive ID cannot be empty")
    if cleaned.lower() == "root" or not VALID_DRIVE_ID_RE.fullmatch(cleaned):
        raise ValueError("invalid Google Drive identifier")
    return cleaned


def reject_bool_num(value: Any, *, name: str) -> Any:
    """Reject Python booleans for numeric Pydantic fields."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric, not bool")
    return value


def _escape_drive_query_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def scrub_secret(text: Any, *secrets: str) -> str:
    """Redact OAuth access/refresh tokens, Bearer headers, and explicit secret values."""
    out = str(text or "")
    out = _OAUTH_TOKEN_RE.sub("[REDACTED]", out)
    out = _BEARER_TOKEN_RE.sub(r"\1[REDACTED]", out)
    for sec in sorted({s for s in secrets if s and len(s) >= 6}, key=len, reverse=True):
        out = out.replace(sec, "[REDACTED]")
    return " ".join(out.split())[:240]


class EphemeralDriveError(RuntimeError):
    """Error raised for ephemeral Drive token validation, scope, or I/O failures."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        super().__init__(message or code)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, D102
        raise urllib.error.HTTPError(req.full_url, code, "Redirects are forbidden", headers, fp)


_DRIVE_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _default_drive_transport(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout: float,
) -> tuple[int, dict[str, str], bytes]:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.netloc != "www.googleapis.com":
        raise EphemeralDriveError("drive_disallowed_endpoint", "Disallowed Drive API endpoint")
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with _DRIVE_OPENER.open(req, timeout=timeout) as resp:
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            payload = resp.read(_MAX_EPHEMERAL_DOWNLOAD_BYTES + 1)
            return int(resp.status), resp_headers, payload
    except urllib.error.HTTPError as exc:
        resp_headers = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}
        err_body = exc.read(_MAX_JSON_RESPONSE_BYTES) if exc.fp is not None else b""
        return int(exc.code), resp_headers, err_body
    except urllib.error.URLError as exc:
        raise EphemeralDriveError(
            "drive_network_error", f"Drive network error: {type(exc.reason).__name__}"
        ) from None
    except TimeoutError:
        raise EphemeralDriveError("drive_network_timeout", "Drive API request timed out") from None


def _build_app_properties(role: str, app_properties: dict[str, str] | None) -> dict[str, str]:
    props = {"hakimi_songs_role": role}
    if app_properties:
        for k, v in app_properties.items():
            if isinstance(k, str) and isinstance(v, str) and k and len(v) <= 120:
                props[k[:40]] = v
    return props


class EphemeralDriveClient:
    """Strictly in-memory Drive REST client bound to a per-job ephemeral access token.

    Never reads credential files from disk, never handles refresh tokens, and strictly
    validates `parents` membership against the explicitly authorized `folders` map
    before downloading or uploading any file.
    """

    _ALLOWED_ROLES = frozenset({"root", "models", "songs", "inbox", "cache"})
    _UPLOAD_ROLES = frozenset({"songs", "models"})

    def __init__(
        self,
        access_token: str,
        folders: dict[str, str],
        *,
        transport: TransportFn | None = None,
        timeout: float = 30.0,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        cleaned_tok = str(access_token or "").strip()
        if (
            not cleaned_tok
            or len(cleaned_tok) > 4096
            or cleaned_tok.startswith("1//")
            or cleaned_tok.lower().startswith("bearer ")
            or any(ch.isspace() or ord(ch) < 32 for ch in cleaned_tok)
        ):
            raise EphemeralDriveError("drive_token_failed", "Invalid ephemeral Drive access token")
        clean_folders: dict[str, str] = {}
        for role, raw_id in (folders or {}).items():
            if role not in self._ALLOWED_ROLES:
                raise EphemeralDriveError("drive_context_required", f"Unsupported role: {role}")
            try:
                clean_folders[role] = validate_drive_id(str(raw_id or ""), allow_empty=True)
            except ValueError as exc:
                raise EphemeralDriveError("drive_context_required", str(exc)) from None
        self._access_token = cleaned_tok
        self._folders = clean_folders
        self._transport: TransportFn = transport or _default_drive_transport
        self._timeout = float(timeout)
        self._sleep_fn = sleep_fn
        self._verified_meta: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}

    def folder_for(self, role: str) -> str:
        fid = self._folders.get(role, "")
        if not fid:
            raise EphemeralDriveError(
                "drive_context_required", f"Missing authorized Drive folder ID for {role}"
            )
        return fid

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
        timeout: float | None = None,
        include_auth: bool = True,
    ) -> tuple[int, dict[str, str], bytes]:
        full_url = url
        if params:
            query = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
            full_url = f"{url}?{query}"
        req_headers: dict[str, str] = {}
        if include_auth:
            req_headers["Authorization"] = f"Bearer {self._access_token}"
        if headers:
            req_headers.update(headers)
        eff_timeout = timeout if timeout is not None else self._timeout
        for attempt in range(_MAX_DRIVE_RETRIES + 1):
            try:
                status, resp_headers, raw = self._transport(
                    method,
                    full_url,
                    req_headers,
                    body,
                    eff_timeout,
                )
            except EphemeralDriveError as exc:
                if (
                    exc.code in {"drive_network_error", "drive_network_timeout"}
                    and attempt < _MAX_DRIVE_RETRIES
                ):
                    delay = min(_RETRY_MAX_DELAY_SECONDS, _RETRY_BASE_DELAY_SECONDS * (2**attempt))
                    self._sleep_fn(delay)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt < _MAX_DRIVE_RETRIES:
                    delay = min(_RETRY_MAX_DELAY_SECONDS, _RETRY_BASE_DELAY_SECONDS * (2**attempt))
                    self._sleep_fn(delay)
                    continue
                if isinstance(exc, TimeoutError):
                    raise EphemeralDriveError(
                        "drive_network_timeout", "Drive API request timed out"
                    ) from None
                raise EphemeralDriveError(
                    "drive_network_error", f"Drive network error: {type(exc).__name__}"
                ) from None

            if status in _RETRYABLE_DRIVE_STATUSES and attempt < _MAX_DRIVE_RETRIES:
                retry_after = _parse_retry_after(resp_headers)
                delay = (
                    retry_after
                    if retry_after is not None
                    else min(_RETRY_MAX_DELAY_SECONDS, _RETRY_BASE_DELAY_SECONDS * (2**attempt))
                )
                self._sleep_fn(delay)
                continue
            return status, resp_headers, raw
        raise EphemeralDriveError("drive_request_failed", "Drive API request retries exhausted")

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
        status, _, raw = self._request(method, url, params=params, headers=headers, body=body)
        if len(raw) > _MAX_JSON_RESPONSE_BYTES:
            raise EphemeralDriveError("drive_response_too_large", "Drive response exceeded limit")
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            raise EphemeralDriveError(
                "drive_invalid_response", f"Drive returned non-JSON response (HTTP {status})"
            ) from None
        if status == 401:
            raise EphemeralDriveError("drive_token_failed", "Ephemeral Drive access token rejected")
        if status == 403:
            raise EphemeralDriveError(
                "drive_insufficient_scope", "Google Drive access denied (HTTP 403)"
            )
        if status == 404:
            raise EphemeralDriveError("drive_not_found", "Drive object not found")
        if status not in {200, 201}:
            raise EphemeralDriveError("drive_request_failed", f"Drive API error (HTTP {status})")
        if not isinstance(data, dict):
            raise EphemeralDriveError("drive_invalid_response", "Drive JSON payload is not object")
        return data

    def verify_file_in_folders(
        self,
        file_id: str,
        *,
        allowed_roles: tuple[str, ...],
    ) -> dict[str, Any]:
        try:
            fid = validate_drive_id(file_id)
        except ValueError as exc:
            raise EphemeralDriveError("drive_file_outside_scope", str(exc)) from None
        allowed_parents = {self.folder_for(role) for role in allowed_roles}
        cache_key = (fid, tuple(sorted(allowed_parents)))
        cached = self._verified_meta.get(cache_key)
        if cached is not None:
            return dict(cached)
        meta = self._api_json(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={
                "fields": "id,name,parents,mimeType,size,modifiedTime,trashed",
                "supportsAllDrives": "true",
            },
        )
        if (
            meta.get("id") != fid
            or meta.get("trashed") is not False
            or meta.get("mimeType") == FOLDER_MIME
        ):
            raise EphemeralDriveError(
                "drive_file_outside_scope", "Drive object is invalid, trashed, or a folder"
            )
        parents = {str(p) for p in (meta.get("parents") or [])}
        if not (parents & allowed_parents):
            raise EphemeralDriveError(
                "drive_file_outside_scope",
                "Requested Drive file is outside authorized folder scope",
            )
        self._verified_meta[cache_key] = dict(meta)
        return meta

    def download_bytes(
        self,
        file_id: str,
        *,
        allowed_roles: tuple[str, ...],
        max_bytes: int = _MAX_EPHEMERAL_DOWNLOAD_BYTES,
    ) -> bytes:
        meta = self.verify_file_in_folders(file_id, allowed_roles=allowed_roles)
        raw_size = meta.get("size")
        if raw_size is not None and str(raw_size).isdigit() and int(raw_size) > max_bytes:
            raise EphemeralDriveError(
                "drive_file_too_large", "Drive file exceeds maximum download size"
            )
        fid = str(meta["id"])
        status, _, raw = self._request(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={"alt": "media", "supportsAllDrives": "true"},
            timeout=max(self._timeout, 60.0),
        )
        if status == 401:
            raise EphemeralDriveError("drive_token_failed", "Ephemeral Drive access token rejected")
        if status == 403:
            raise EphemeralDriveError(
                "drive_insufficient_scope", "Google Drive download denied (HTTP 403)"
            )
        if status == 404:
            raise EphemeralDriveError("drive_not_found", "Drive file not found")
        if status != 200:
            raise EphemeralDriveError(
                "drive_download_failed", f"Drive download failed (HTTP {status})"
            )
        if len(raw) > max_bytes:
            raise EphemeralDriveError(
                "drive_file_too_large", "Drive download exceeded maximum byte limit"
            )
        return raw

    def download_file(
        self,
        file_id: str,
        dest_path: Path,
        *,
        allowed_roles: tuple[str, ...],
        max_bytes: int = _MAX_EPHEMERAL_DOWNLOAD_BYTES,
    ) -> Path:
        data = self.download_bytes(file_id, allowed_roles=allowed_roles, max_bytes=max_bytes)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".ephem-drive-", dir=dest_path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, dest_path)
        finally:
            Path(tmp_name).unlink(missing_ok=True)
        return dest_path

    def _find_existing_file_id(self, parent_id: str, filename: str) -> str | None:
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and name = '{_escape_drive_query_literal(filename)}' "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name)",
                "pageSize": 5,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        files = listing.get("files") or []
        if not files:
            return None
        try:
            return validate_drive_id(str(files[0].get("id") or ""))
        except ValueError:
            return None

    def _parse_upload_response(self, status: int, raw: bytes) -> dict[str, Any]:
        if status == 401:
            raise EphemeralDriveError("drive_token_failed", "Ephemeral Drive access token rejected")
        if status == 403:
            raise EphemeralDriveError(
                "drive_insufficient_scope", "Google Drive upload denied (HTTP 403)"
            )
        if status not in {200, 201}:
            raise EphemeralDriveError("drive_upload_failed", f"Drive upload failed (HTTP {status})")
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise EphemeralDriveError(
                "drive_invalid_response", "Invalid Drive upload JSON"
            ) from None
        if not isinstance(data, dict) or not data.get("id"):
            raise EphemeralDriveError("drive_invalid_response", "Drive upload missing file ID")
        return data

    def _upload_multipart(
        self,
        parent_id: str,
        filename: str,
        data: bytes,
        mime_type: str,
        *,
        existing_id: str | None,
        app_properties: dict[str, str],
    ) -> dict[str, Any]:
        if existing_id is not None:
            status, _, resp_raw = self._request(
                "PATCH",
                f"{DRIVE_UPLOAD_API}/{existing_id}",
                params={
                    "uploadType": "media",
                    "fields": "id,name,size,modifiedTime,mimeType",
                    "supportsAllDrives": "true",
                },
                headers={"Content-Type": mime_type or "application/octet-stream"},
                body=data,
                timeout=max(self._timeout, 60.0),
            )
            return self._parse_upload_response(status, resp_raw)

        boundary = f"hakimi-{uuid.uuid4().hex}"
        metadata = {
            "name": filename,
            "parents": [parent_id],
            "appProperties": app_properties,
        }
        meta_bytes = json.dumps(metadata, ensure_ascii=False).encode("utf-8")
        body = (
            (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").encode()
            + meta_bytes
            + (
                f"\r\n--{boundary}\r\n"
                f"Content-Type: {mime_type or 'application/octet-stream'}\r\n\r\n"
            ).encode()
            + data
            + f"\r\n--{boundary}--\r\n".encode()
        )
        status, _, resp_raw = self._request(
            "POST",
            DRIVE_UPLOAD_API,
            params={
                "uploadType": "multipart",
                "fields": "id,name,size,modifiedTime,mimeType",
                "supportsAllDrives": "true",
            },
            headers={"Content-Type": f"multipart/related; boundary={boundary}"},
            body=body,
            timeout=max(self._timeout, 60.0),
        )
        return self._parse_upload_response(status, resp_raw)

    def _upload_resumable_file(
        self,
        parent_id: str,
        filename: str,
        local_path: Path,
        total_size: int,
        mime_type: str,
        *,
        existing_id: str | None,
        app_properties: dict[str, str],
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {"name": filename, "appProperties": app_properties}
        if existing_id is None:
            metadata["parents"] = [parent_id]
            init_method = "POST"
            init_url = DRIVE_UPLOAD_API
        else:
            init_method = "PATCH"
            init_url = f"{DRIVE_UPLOAD_API}/{existing_id}"
        init_status, init_headers, _ = self._request(
            init_method,
            init_url,
            params={
                "uploadType": "resumable",
                "fields": "id,name,size,modifiedTime,mimeType",
                "supportsAllDrives": "true",
            },
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": mime_type or "application/octet-stream",
                "X-Upload-Content-Length": str(total_size),
            },
            body=json.dumps(metadata, ensure_ascii=False).encode("utf-8"),
            timeout=max(self._timeout, 30.0),
        )
        if init_status == 401:
            raise EphemeralDriveError("drive_token_failed", "Ephemeral Drive access token rejected")
        if init_status == 403:
            raise EphemeralDriveError(
                "drive_insufficient_scope", "Google Drive resumable upload denied (HTTP 403)"
            )
        location = init_headers.get("location", "").strip()
        parsed_loc = urllib.parse.urlsplit(location)
        if (
            init_status not in {200, 201}
            or parsed_loc.scheme != "https"
            or parsed_loc.netloc != "www.googleapis.com"
        ):
            raise EphemeralDriveError(
                "drive_upload_failed", f"Invalid resumable session response (HTTP {init_status})"
            )

        offset = 0
        with local_path.open("rb") as handle:
            while offset < total_size:
                chunk = handle.read(_RESUMABLE_CHUNK_BYTES)
                if not chunk:
                    raise EphemeralDriveError(
                        "drive_upload_failed", "Unexpected EOF while streaming upload"
                    )
                end = offset + len(chunk) - 1
                status, _, resp_raw = self._request(
                    "PUT",
                    location,
                    headers={
                        "Content-Type": mime_type or "application/octet-stream",
                        "Content-Length": str(len(chunk)),
                        "Content-Range": f"bytes {offset}-{end}/{total_size}",
                    },
                    body=chunk,
                    timeout=max(self._timeout, 120.0),
                    include_auth=False,
                )
                offset = end + 1
                if offset < total_size:
                    if status != 308:
                        raise EphemeralDriveError(
                            "drive_upload_failed",
                            f"Unexpected resumable chunk status (HTTP {status})",
                        )
                else:
                    return self._parse_upload_response(status, resp_raw)
        raise EphemeralDriveError("drive_upload_failed", "Resumable upload did not complete")

    def upload_file(
        self,
        role: Literal["songs", "models"],
        local_path: Path,
        *,
        drive_name: str = "",
        mime_type: str = "application/octet-stream",
        replace_existing: bool = True,
        app_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if role not in self._UPLOAD_ROLES:
            raise EphemeralDriveError("drive_file_outside_scope", f"Disallowed upload role: {role}")
        if not local_path.is_file():
            raise EphemeralDriveError("drive_not_found", f"Local upload file missing: {local_path}")
        try:
            clean_name = sanitize_filename(drive_name or local_path.name)
        except ValueError as exc:
            raise EphemeralDriveError("drive_file_outside_scope", str(exc)) from None
        size = local_path.stat().st_size
        if size <= 0 or size > _MAX_EPHEMERAL_UPLOAD_BYTES:
            raise EphemeralDriveError("drive_file_too_large", "Upload file size out of range")

        parent_id = self.folder_for(role)
        existing_id = (
            self._find_existing_file_id(parent_id, clean_name) if replace_existing else None
        )
        props = _build_app_properties(role, app_properties)
        if size <= _MULTIPART_THRESHOLD_BYTES:
            res = self._upload_multipart(
                parent_id,
                clean_name,
                local_path.read_bytes(),
                mime_type,
                existing_id=existing_id,
                app_properties=props,
            )
        else:
            res = self._upload_resumable_file(
                parent_id,
                clean_name,
                local_path,
                size,
                mime_type,
                existing_id=existing_id,
                app_properties=props,
            )
        return {
            "file_id": str(res["id"]),
            "name": str(res.get("name") or clean_name),
            "size": int(res["size"]) if str(res.get("size") or "").isdigit() else size,
        }


class DriveFoldersContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root: str = Field(default="", max_length=128)
    models: str = Field(default="", max_length=128)
    songs: str = Field(default="", max_length=128)
    inbox: str = Field(default="", max_length=128)
    cache: str = Field(default="", max_length=128)

    @field_validator("root", "models", "songs", "inbox", "cache", mode="before")
    @classmethod
    def _check_folder_ids(cls, value: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("folder ID must be a string")
        return validate_drive_id(value, allow_empty=True)

    def as_dict(self) -> dict[str, str]:
        return {
            "root": self.root,
            "models": self.models,
            "songs": self.songs,
            "inbox": self.inbox,
            "cache": self.cache,
        }


class DriveFilesContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inbox_file_id: str = Field(default="", max_length=128)
    inbox_name: str = Field(default="", max_length=160)
    pth_file_id: str = Field(default="", max_length=128)
    index_file_id: str = Field(default="", max_length=128)
    ref_audio_file_id: str = Field(default="", max_length=128)
    ref_audio_name: str = Field(default="", max_length=160)
    cache_weights_file_id: str = Field(default="", max_length=128)
    cache_weights_sha256: str = Field(default="", max_length=64)

    @field_validator(
        "inbox_file_id",
        "pth_file_id",
        "index_file_id",
        "ref_audio_file_id",
        "cache_weights_file_id",
        mode="before",
    )
    @classmethod
    def _check_file_ids(cls, value: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("file ID must be a string")
        return validate_drive_id(value, allow_empty=True)

    @field_validator("inbox_name", "ref_audio_name", mode="before")
    @classmethod
    def _check_safe_names(cls, value: Any, info: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError(f"{info.field_name} must be a string")
        cleaned = value.strip()
        if not cleaned:
            return ""
        return sanitize_filename(cleaned)

    @field_validator("cache_weights_sha256", mode="before")
    @classmethod
    def _check_sha256(cls, value: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("cache_weights_sha256 must be a string")
        cleaned = value.strip().lower()
        if not cleaned:
            return ""
        if not VALID_SHA256_RE.fullmatch(cleaned):
            raise ValueError("invalid cache_weights_sha256")
        return cleaned


class DriveDispatchContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    access_token: str = Field(min_length=1, max_length=4096)
    folders: DriveFoldersContext = Field(default_factory=DriveFoldersContext)
    files: DriveFilesContext = Field(default_factory=DriveFilesContext)

    @field_validator("access_token")
    @classmethod
    def _check_access_token(cls, value: str) -> str:
        cleaned = value.strip()
        if (
            not cleaned
            or cleaned.startswith("1//")
            or cleaned.lower().startswith("bearer ")
            or any(ch.isspace() or ord(ch) < 32 for ch in cleaned)
        ):
            raise ValueError("invalid ephemeral access_token")
        return cleaned


class DispatchContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    drive: DriveDispatchContext | None = None


@dataclass(frozen=True)
class WarmSessionClaim:
    """Result of attempting to claim a reusable warm GPU session from the worker pool."""

    entry: Any | None = None
    reused: bool = False
    gpu: str = ""
    setup_ready: bool = False
    sep_ready: bool = False
    svc_ready: bool = False
    rvc_ready: bool = False


@dataclass
class WorkerJobContext:
    """Ephemeral per-execution context passed to a `JobHandler.run_sync` call.

    Never stored on `WorkerJobRecord` or `WorkerState`; `dispatch_context` is cleared
    immediately after execution finishes.
    """

    job_id: str
    job_type: str
    gpu_candidates: tuple[str, ...]
    session_name: str
    config: Any
    dispatch_context: DispatchContext | None = None
    allow_warm_keepalive: bool = False
    cu_balance_hint: float | None = None
    drive_transport: TransportFn | None = None
    stop_session_fn: Callable[[str], None] | None = None
    clock: Callable[[], float] = time.time
    overrides: dict[str, Any] = field(default_factory=dict)
    cancel_event: threading.Event | None = field(default=None, repr=False)
    _is_cancelled_fn: Callable[[], bool] | None = field(default=None, repr=False)
    _can_keep_warm_fn: Callable[[str], bool] | None = field(default=None, repr=False)
    _claim_warm_fn: Callable[[], WarmSessionClaim] | None = field(default=None, repr=False)
    _park_warm_fn: Callable[[str, Any | None], None] | None = field(default=None, repr=False)
    _drain_warm_fn: Callable[[], None] | None = field(default=None, repr=False)

    def is_cancelled(self) -> bool:
        if self.cancel_event is not None and self.cancel_event.is_set():
            return True
        return bool(self._is_cancelled_fn()) if self._is_cancelled_fn is not None else False

    def can_keep_warm(self, gpu: str) -> bool:
        if self._can_keep_warm_fn is None:
            return False
        return bool(self._can_keep_warm_fn(gpu))

    def claim_warm_session(self) -> WarmSessionClaim:
        if self._claim_warm_fn is None:
            return WarmSessionClaim()
        claim = self._claim_warm_fn()
        if claim.reused and claim.entry is not None:
            self.session_name = str(claim.entry.session_name)
        return claim

    def park_or_stop_warm_session(
        self, assigned_gpu: str, *, prior_warm: Any | None = None
    ) -> None:
        if self._park_warm_fn is not None:
            self._park_warm_fn(assigned_gpu, prior_warm)
        else:
            self.stop_session(self.session_name)

    def drain_warm_sessions(self) -> None:
        if self._drain_warm_fn is not None:
            self._drain_warm_fn()

    def stop_session(self, session_name: str) -> None:
        if self.stop_session_fn is None:
            return
        try:
            self.stop_session_fn(session_name)
        except Exception:
            pass

    def build_ephemeral_drive(
        self,
        *,
        required_roles: tuple[str, ...] = (),
    ) -> tuple[DriveDispatchContext, EphemeralDriveClient]:
        if self.dispatch_context is None or self.dispatch_context.drive is None:
            raise EphemeralDriveError(
                "drive_context_required",
                "Missing ephemeral Drive dispatch_context for job execution",
            )
        drive_ctx = self.dispatch_context.drive
        client = EphemeralDriveClient(
            drive_ctx.access_token,
            drive_ctx.folders.as_dict(),
            transport=self.drive_transport,
        )
        for role in required_roles:
            client.folder_for(role)
        return drive_ctx, client


class JobValidationError(ValueError):
    """Raised when a job payload fails semantic validation before execution."""

    def __init__(self, code: str = "colab_invalid_job", message: str = "") -> None:
        clean_code = (
            code
            if isinstance(code, str) and VALID_ERROR_CODE_RE.fullmatch(code)
            else "colab_invalid_job"
        )
        self.code = clean_code
        super().__init__(message or clean_code)


class JobHandler(ABC):
    """Abstract base class for pluggable host Colab GPU job handlers."""

    job_type: str
    payload_model: type[BaseModel]
    allow_warm_keepalive: bool = False
    allowed_error_codes: frozenset[str] = frozenset()

    def validate_payload(self, payload: dict[str, Any]) -> BaseModel:
        if not isinstance(payload, dict):
            raise JobValidationError("colab_invalid_job", "payload must be an object")
        return self.payload_model.model_validate(payload)

    @abstractmethod
    def run_sync(self, ctx: WorkerJobContext, parsed_payload: Any) -> tuple[dict[str, Any], str]:
        """Run the GPU job synchronously and return `(result_dict, assigned_gpu)`."""


_JOB_HANDLERS: dict[str, JobHandler] = {}


def register_job_handler(handler: JobHandler, *, replace: bool = False) -> JobHandler:
    """Register a validated `JobHandler` into the worker's plugin registry."""
    if not isinstance(handler, JobHandler):
        raise TypeError("handler must inherit from JobHandler")
    job_type = getattr(handler, "job_type", "")
    if not isinstance(job_type, str) or not VALID_JOB_TYPE_RE.fullmatch(job_type):
        raise ValueError(f"Invalid worker job_type: {job_type!r}")
    payload_model = getattr(handler, "payload_model", None)
    if not (isinstance(payload_model, type) and issubclass(payload_model, BaseModel)):
        raise TypeError("handler.payload_model must be a Pydantic BaseModel subclass")
    if not isinstance(handler.allow_warm_keepalive, bool):
        raise TypeError("handler.allow_warm_keepalive must be a bool")
    if job_type in _JOB_HANDLERS and not replace:
        raise ValueError(f"Job handler already registered: {job_type!r}")
    _JOB_HANDLERS[job_type] = handler
    return handler


def unregister_job_handler(job_type: str) -> JobHandler | None:
    """Remove a handler by job_type (primarily for test isolation)."""
    return _JOB_HANDLERS.pop(job_type, None)


def get_job_handler(job_type: str) -> JobHandler | None:
    """Look up a registered `JobHandler` by `job_type`."""
    return _JOB_HANDLERS.get(job_type)


def registered_job_types() -> tuple[str, ...]:
    """Return sorted registered job_type identifiers."""
    return tuple(sorted(_JOB_HANDLERS))


# Auto-register built-in GPU job handler plugins.
from . import rvc  # noqa: E402, F401

__all__ = [
    "DRIVE_FILES_API",
    "DRIVE_UPLOAD_API",
    "FOLDER_MIME",
    "VALID_DRIVE_ID_RE",
    "VALID_ERROR_CODE_RE",
    "VALID_JOB_TYPE_RE",
    "VALID_SHA256_RE",
    "SAFE_FILENAME_RE",
    "TransportFn",
    "EphemeralDriveError",
    "EphemeralDriveClient",
    "DriveFoldersContext",
    "DriveFilesContext",
    "DriveDispatchContext",
    "DispatchContext",
    "WarmSessionClaim",
    "WorkerJobContext",
    "JobValidationError",
    "JobHandler",
    "register_job_handler",
    "unregister_job_handler",
    "get_job_handler",
    "registered_job_types",
    "sanitize_filename",
    "validate_drive_id",
    "reject_bool_num",
    "scrub_secret",
    "rvc",
]
