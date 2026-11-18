"""Single-folder Drive writer: explicit SA or minimal-scope personal OAuth, never ADC."""

import asyncio
import hashlib
import json
import re
import time
import uuid
from datetime import date

import httpx
from google.auth import jwt
from google.oauth2 import service_account

from .config import Settings
from .http_json import bounded_json
from .security import Rejected

SCOPE = "https://www.googleapis.com/auth/drive.file"
TOKEN_URI = "https://oauth2.googleapis.com/token"
API = "https://www.googleapis.com/drive/v3/files"
UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"
FILE_FIELDS = "id,name,parents,mimeType,trashed,appProperties"


class DriveAuth:
    scope = SCOPE

    def __init__(self, settings: Settings, client: httpx.AsyncClient):
        self.s, self.http = settings, client
        self.credentials = None
        self.token = ""
        self.expires = 0.0
        self.token_lock = asyncio.Lock()

    def _load_credentials(self):
        path = self.s.gdrive_service_account_file
        if path is None or path.stat().st_size > 65536:
            raise Rejected("drive_credentials_invalid")
        info = json.loads(path.read_bytes())
        if (
            info.get("type") != "service_account"
            or info.get("token_uri") != TOKEN_URI
            or not str(info.get("client_email", "")).endswith(".iam.gserviceaccount.com")
        ):
            raise Rejected("drive_credentials_invalid")
        # No ADC, user OAuth, domain-wide delegation, impersonation or subject field.
        return service_account.Credentials.from_service_account_info(info, scopes=[self.scope])

    async def _access_token(self) -> str:
        async with self.token_lock:
            if self.token and time.monotonic() < self.expires:
                return self.token
            if self.credentials is None:
                self.credentials = await asyncio.to_thread(self._load_credentials)
            now = int(time.time())
            assertion = jwt.encode(
                self.credentials.signer,
                {
                    "iss": self.credentials.service_account_email,
                    "scope": self.scope,
                    "aud": TOKEN_URI,
                    "iat": now,
                    "exp": now + 3600,
                },
            ).decode("ascii")
            token = await bounded_json(
                self.http,
                "POST",
                TOKEN_URI,
                max_bytes=16384,
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
                follow_redirects=False,
            )
            if (
                not isinstance(token.get("access_token"), str)
                or not 60 <= int(token.get("expires_in", 0)) <= 3600
            ):
                raise Rejected("drive_token_failed")
            self.token = token["access_token"]
            self.expires = time.monotonic() + int(token["expires_in"]) - 30
            return self.token


class FolderDrive(DriveAuth):
    def __init__(self, settings: Settings, client: httpx.AsyncClient, marker: str):
        super().__init__(settings, client)
        self.folder = settings.gdrive_roaming_folder_id
        if not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", self.folder) or self.folder == "root":
            raise Rejected("drive_folder_required")
        self.marker = marker
        self.oauth = None
        if settings.gdrive_auth_mode == "oauth":
            from .drive_oauth import OAuthDriveAuth

            self.oauth = OAuthDriveAuth(settings.gdrive_oauth_credentials_file, self.folder, client)

    async def _access_token(self) -> str:
        if self.oauth is not None:
            return await self.oauth.access_token()
        return await super()._access_token()

    async def _json(self, method: str, url: str, **kwargs) -> tuple[dict, str | None]:
        headers = {
            "Authorization": "Bearer " + await self._access_token(),
            **kwargs.pop("headers", {}),
        }
        async with self.http.stream(
            method, url, headers=headers, follow_redirects=False, **kwargs
        ) as response:
            if response.status_code == 412:
                raise Rejected("drive_etag_conflict")
            if response.status_code not in {200, 201}:
                raise Rejected("drive_request_failed")
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 65536:
                    raise Rejected("drive_response_too_large")
            value = json.loads(body)
            if not isinstance(value, dict):
                raise Rejected("drive_invalid_response")
            return value, response.headers.get("etag")

    def _validate_file(self, item: dict, filename: str) -> str:
        file_id = item.get("id", "")
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", file_id)
            or item.get("parents") != [self.folder]
            or item.get("trashed", False)
            or item.get("name") != filename
            or item.get("mimeType") != "text/markdown"
            or item.get("appProperties", {}).get("hakimi_roaming") != self.marker
        ):
            raise Rejected("drive_file_outside_scope")
        return file_id

    async def validate_folder(self):
        # The only unconditionally fetched object is the configured folder itself.
        folder, _ = await self._json(
            "GET",
            API + "/" + self.folder,
            params={
                "fields": "id,mimeType,trashed,driveId,appProperties,capabilities(canAddChildren)",
                "supportsAllDrives": "true",
            },
        )
        if (
            folder.get("id") != self.folder
            or folder.get("mimeType") != "application/vnd.google-apps.folder"
            or folder.get("trashed", False)
            or not folder.get("capabilities", {}).get("canAddChildren")
        ):
            raise Rejected("drive_folder_unavailable")
        if self.oauth is None and not folder.get("driveId"):
            # Service Accounts cannot own new My Drive files: do not fall back to personal OAuth.
            raise Rejected("drive_shared_folder_required")
        if self.oauth is not None and (
            self.oauth.info is None
            or folder.get("appProperties", {}).get("hakimi_roaming_folder")
            != self.oauth.info["folder_marker"]
        ):
            raise Rejected("drive_oauth_folder_mismatch")

    async def sync_day(self, day: str, markdown: str) -> str:
        if date.fromisoformat(day).isoformat() != day or len(markdown.encode()) > 262144:
            raise Rejected("drive_invalid_diary")
        filename = day + ".md"
        await self.validate_folder()
        listing, _ = await self._json(
            "GET",
            API,
            params={
                "q": f"'{self.folder}' in parents and trashed = false and name = '{filename}'",
                "fields": f"nextPageToken,incompleteSearch,files({FILE_FIELDS})",
                "pageSize": "100",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                # Supports both My Drive and a shared subfolder granted to a non-member.
                # The parent + day predicate is mandatory; never enumerate either Drive.
                "corpora": "user",
            },
        )
        files = listing.get("files")
        if (
            not isinstance(files, list)
            or len(files) > 1
            or listing.get("nextPageToken")
            or listing.get("incompleteSearch")
        ):
            raise Rejected("drive_ambiguous_file")
        if files:
            file_id = self._validate_file(files[0], filename)
            # Only IDs returned by the folder-constrained listing may be read or updated.
            current, etag = await self._json(
                "GET",
                API + "/" + file_id,
                params={"fields": FILE_FIELDS, "supportsAllDrives": "true"},
            )
            self._validate_file(current, filename)
            result, _ = await self._json(
                "PATCH",
                UPLOAD + "/" + file_id,
                params={"uploadType": "media", "supportsAllDrives": "true", "fields": FILE_FIELDS},
                headers={
                    "Content-Type": "text/markdown; charset=UTF-8",
                    **({"If-Match": etag} if etag else {}),
                },
                content=markdown.encode(),
            )
        else:
            metadata = {
                "name": filename,
                "mimeType": "text/markdown",
                "parents": [self.folder],
                "appProperties": {"hakimi_roaming": self.marker},
            }
            boundary = "hakimi_" + uuid.uuid4().hex
            body = (
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                + json.dumps(metadata)
                + f"\r\n--{boundary}\r\nContent-Type: text/markdown; charset=UTF-8\r\n\r\n"
                + markdown
                + f"\r\n--{boundary}--\r\n"
            ).encode()
            result, _ = await self._json(
                "POST",
                UPLOAD,
                params={
                    "uploadType": "multipart",
                    "supportsAllDrives": "true",
                    "fields": FILE_FIELDS,
                },
                headers={"Content-Type": "multipart/related; boundary=" + boundary},
                content=body,
            )
        return self._validate_file(result, filename)

    def _memory_identity(self, scope):
        if not re.fullmatch(r"[a-f0-9]{64}", scope):
            raise Rejected("drive_memory_scope_invalid")
        return "group-memory-" + scope[:24] + ".md"

    def _validate_memory_file(self, item, scope):
        file_id = item.get("id", "")
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", file_id)
            or item.get("parents") != [self.folder]
            or item.get("trashed", False)
            or item.get("name") != self._memory_identity(scope)
            or item.get("mimeType") != "text/markdown"
            or item.get("appProperties", {}).get("hakimi_group_memory") != self.marker
            or item.get("appProperties", {}).get("hakimi_group_scope") != scope
        ):
            raise Rejected("drive_file_outside_scope")
        return file_id

    async def memory_head(self, scope):
        filename = self._memory_identity(scope)
        await self.validate_folder()
        # All discovery is explicitly parent AND dedicated-property constrained.
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.marker):
            raise Rejected("drive_memory_marker_invalid")
        listing, _ = await self._json(
            "GET",
            API,
            params={
                "q": f"'{self.folder}' in parents and trashed = false and name = '{filename}' "
                f"and appProperties has {{ key='hakimi_group_memory' and value='{self.marker}' }} "
                f"and appProperties has {{ key='hakimi_group_scope' and value='{scope}' }}",
                "fields": f"nextPageToken,incompleteSearch,files({FILE_FIELDS})",
                "pageSize": "2",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                "corpora": "user",
            },
        )
        files = listing.get("files")
        if (
            not isinstance(files, list)
            or len(files) > 1
            or listing.get("nextPageToken")
            or listing.get("incompleteSearch")
        ):
            raise Rejected("drive_ambiguous_file")
        if not files:
            return None
        ident = self._validate_memory_file(files[0], scope)
        meta, etag = await self._json(
            "GET",
            API + "/" + ident,
            params={
                "fields": FILE_FIELDS + ",md5Checksum,size,version,headRevisionId",
                "supportsAllDrives": "true",
            },
        )
        self._validate_memory_file(meta, scope)
        etag = etag.strip() if isinstance(etag, str) else ""
        if not etag:
            md5 = (
                ("" if meta.get("md5Checksum") is None else str(meta.get("md5Checksum")))
                .strip()
                .lower()
            )
            ver = ("" if meta.get("version") is None else str(meta.get("version"))).strip()
            rev = (
                "" if meta.get("headRevisionId") is None else str(meta.get("headRevisionId"))
            ).strip()
            md5_ok = bool(re.fullmatch(r"[a-f0-9]{32}", md5))
            ver_ok = bool(re.fullmatch(r"[0-9]{1,20}", ver))
            rev_ok = bool(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", rev))
            if not md5_ok or (ver and not ver_ok) or (rev and not rev_ok) or not (ver_ok or rev_ok):
                raise Rejected("drive_etag_required")
            etag = f"v3:{ver}:{rev}:{md5}"
        return {
            "id": ident,
            "etag": etag,
            "md5": meta.get("md5Checksum"),
            "size": meta.get("size"),
        }

    async def read_memory(self, scope, head):
        # Revalidate against a fresh constrained listing, never accept an arbitrary file ID.
        fresh = await self.memory_head(scope)
        if not fresh or fresh["id"] != head["id"] or fresh["etag"] != head["etag"]:
            raise Rejected("drive_etag_conflict")
        try:
            if int(fresh.get("size") or 0) > 262144:
                raise Rejected("drive_memory_too_large")
        except (TypeError, ValueError):
            raise Rejected("drive_invalid_response") from None
        is_v3_etag = str(head["etag"]).startswith("v3:")
        headers = {"Authorization": "Bearer " + await self._access_token()}
        if not is_v3_etag:
            headers["If-Match"] = head["etag"]
        async with self.http.stream(
            "GET",
            API + "/" + head["id"],
            headers=headers,
            params={"alt": "media", "supportsAllDrives": "true"},
            follow_redirects=False,
        ) as response:
            if response.status_code != 200:
                raise Rejected("drive_memory_download_failed")
            data = bytearray()
            async for chunk in response.aiter_bytes():
                data.extend(chunk)
                if len(data) > 262144:
                    raise Rejected("drive_memory_too_large")
            if is_v3_etag and fresh.get("md5"):
                actual_md5 = hashlib.md5(data, usedforsecurity=False).hexdigest().lower()
                if actual_md5 != str(fresh["md5"]).strip().lower():
                    raise Rejected("drive_etag_conflict")
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                raise Rejected("drive_memory_not_text") from None

    async def write_memory(self, scope, markdown, head):
        filename = self._memory_identity(scope)
        if len(markdown.encode()) > 262144:
            raise Rejected("drive_memory_too_large")
        fresh = await self.memory_head(scope)
        if (fresh is None) != (head is None) or (
            fresh and (fresh["id"], fresh["etag"]) != (head["id"], head["etag"])
        ):
            raise Rejected("drive_etag_conflict")
        if head:
            headers = {"Content-Type": "text/markdown; charset=UTF-8"}
            if not str(head["etag"]).startswith("v3:"):
                headers["If-Match"] = head["etag"]
            result, _ = await self._json(
                "PATCH",
                UPLOAD + "/" + head["id"],
                params={"uploadType": "media", "supportsAllDrives": "true", "fields": FILE_FIELDS},
                headers=headers,
                content=markdown.encode(),
            )
        else:
            meta = {
                "name": filename,
                "parents": [self.folder],
                "mimeType": "text/markdown",
                "appProperties": {"hakimi_group_memory": self.marker, "hakimi_group_scope": scope},
            }
            boundary = "hakimi_" + uuid.uuid4().hex
            data = (
                f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                + json.dumps(meta)
                + f"\r\n--{boundary}\r\nContent-Type: text/markdown; charset=UTF-8\r\n\r\n"
                + markdown
                + f"\r\n--{boundary}--\r\n"
            ).encode()
            result, _ = await self._json(
                "POST",
                UPLOAD,
                params={
                    "uploadType": "multipart",
                    "supportsAllDrives": "true",
                    "fields": FILE_FIELDS,
                },
                headers={"Content-Type": "multipart/related; boundary=" + boundary},
                content=data,
            )
        return self._validate_memory_file(result, scope)
