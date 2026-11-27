"""Scoped async Google Drive storage client exclusively for `Hakimi_Songs/`.

Owns `Hakimi_Songs/` (`models/`, `songs/`, `inbox/`, `cache/`), persists optional root folder
overrides in SQLite `admin_config` (Vault-encrypted), reads OAuth credentials strictly read-only
(never ADC, never inheriting `folder_id` from `roaming-oauth.json`, never touching unrelated
Drive folders), and supports bounded-memory Drive v3 Resumable Upload streaming.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx

from .security import Rejected, configured_secrets

DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file"
DRIVE_FULL_SCOPE = "https://www.googleapis.com/auth/drive"
ALLOWED_DRIVE_SCOPES = frozenset({DRIVE_FILE_SCOPE, DRIVE_FULL_SCOPE})
TOKEN_URI = "https://oauth2.googleapis.com/token"
DRIVE_FILES_API = "https://www.googleapis.com/drive/v3/files"
DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"
FOLDER_MIME = "application/vnd.google-apps.folder"
ROOT_FOLDER_NAME = "Hakimi_Songs"
SUBFOLDER_NAMES: tuple[str, ...] = ("models", "songs", "inbox")
CACHE_FOLDER_NAME = "cache"

# Deterministic weight cache constants matching scripts/colab_remote_rvc.py
WEIGHT_CACHE_SCHEMA_VERSION = "v1"
PRIMARY_SEP_MODELS: tuple[str, ...] = (
    "model_bs_roformer_ep_317_sdr_12.9755.ckpt",
    "UVR-MDX-NET-Inst_HQ_3.onnx",
)
KARAOKE_SEP_MODELS: tuple[str, ...] = ("mel_band_roformer_karaoke_aufr33_viperx_sdr_10.1956.ckpt",)
DEREVERB_MODELS: tuple[str, ...] = (
    "dereverb_mel_band_roformer_less_aggressive_anvuew_sdr_18.8050.ckpt",
    "dereverb_mel_band_roformer_anvuew_sdr_19.1729.ckpt",
    "UVR-DeEcho-DeReverb.pth",
)

FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}\Z")
FOLDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,128}\Z")
SAFE_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}\Z")
CACHE_KEY_RE = re.compile(r"^[A-Za-z0-9._-]{4,128}\Z")
SHA256_HEX_RE = re.compile(r"^[a-fA-F0-9]{64}\Z")
MODEL_NAME_RE = re.compile(r"^[a-z0-9_-]{1,32}\Z")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}\.apps\.googleusercontent\.com\Z")
_OAUTH_TOKEN_RE = re.compile(r"(?:ya29\.[A-Za-z0-9._-]+|1//[A-Za-z0-9._-]+)")
_BEARER_RE = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
_RANGE_HEADER_RE = re.compile(r"^bytes=0-(\d+)\Z")

MAX_CREDENTIALS_BYTES = 65_536
MAX_TOKEN_RESPONSE_BYTES = 32_768
MAX_JSON_RESPONSE_BYTES = 1_048_576
MAX_CACHE_MANIFEST_BYTES = 262_144
MULTIPART_THRESHOLD_BYTES = 4 * 1024 * 1024
RESUMABLE_CHUNK_GRANULARITY = 256 * 1024
RESUMABLE_CHUNK_BYTES = 2 * 1024 * 1024  # 2 MiB (8 * 256 KiB); keeps peak RAM <= 4 MiB
MAX_RESUMABLE_CHUNK_BYTES = 4 * 1024 * 1024
MAX_INBOX_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_REF_VOICE_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_SONG_DOWNLOAD_BYTES = 30 * 1024 * 1024
MAX_SONG_UPLOAD_BYTES = 30 * 1024 * 1024
REF_VOICE_EXTENSIONS: frozenset[str] = frozenset({"wav", "flac", "mp3", "m4a", "ogg", "opus"})

_CONFIG_KEY = "song_studio_drive_config"
_VAULT_CFG_CTX = "admin:song_studio_drive_config"
_LEGACY_COLAB_CONFIG_KEY = "colab_compute_config"
_LEGACY_COLAB_VAULT_CTX = "admin:colab_compute_config"

DriveRole = Literal["root", "models", "songs", "inbox", "cache"]
DriveSubRole = Literal["models", "songs", "inbox", "cache"]


@dataclass(frozen=True)
class SongDriveFolderTree:
    """Resolved folder IDs for `Hakimi_Songs/` and its scoped subfolders."""

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
            raise Rejected("song_drive_invalid_file")
        return mapping[role]


def compute_weight_cache_key(mode: str = "cover") -> str:
    """Deterministic version key for RVC + audio-separator weights matching colab_remote_rvc."""
    norm_mode = "cover" if str(mode or "").strip().lower() == "cover" else "all"
    spec = {
        "schema": WEIGHT_CACHE_SCHEMA_VERSION,
        "mode": norm_mode,
        "primary_sep": list(PRIMARY_SEP_MODELS),
        "karaoke_sep": list(KARAOKE_SEP_MODELS),
        "dereverb": list(DEREVERB_MODELS),
        "applio_f0": "rmvpe",
        "applio_embedder": "contentvec",
    }
    digest = hashlib.sha256(json.dumps(spec, sort_keys=True).encode("utf-8")).hexdigest()
    return f"weights_{norm_mode}_{digest[:12]}"


def validate_drive_id(value: Any, *, is_folder: bool = False) -> str:
    """Strictly validate a Google Drive file or folder ID, forbidding `root`."""
    if not isinstance(value, str):
        raise Rejected("invalid_drive_file_id")
    cleaned = value.strip()
    pattern = FOLDER_ID_RE if is_folder else FILE_ID_RE
    if not cleaned or cleaned.lower() == "root" or not pattern.fullmatch(cleaned):
        raise Rejected("invalid_drive_file_id")
    return cleaned


def sanitize_drive_filename(name: Any) -> str:
    """Validate a flat Drive filename without path traversal or control chars."""
    if not isinstance(name, str):
        raise Rejected("song_drive_invalid_file")
    stripped = name.strip()
    cleaned = Path(stripped).name.strip()
    if (
        not cleaned
        or cleaned in {".", ".."}
        or ".." in cleaned
        or "/" in stripped
        or "\\" in stripped
        or not SAFE_FILENAME_RE.fullmatch(cleaned)
    ):
        raise Rejected("song_drive_invalid_file")
    return cleaned


def scrub_drive_secrets(text: Any, *extra_secrets: str) -> str:
    """Redact OAuth access/refresh tokens, Bearer headers, and explicit secrets."""
    out = " ".join(str(text or "").split())
    out = _OAUTH_TOKEN_RE.sub("[REDACTED]", out)
    out = _BEARER_RE.sub(r"\1[REDACTED]", out)
    for sec in sorted({s for s in extra_secrets if s and len(s) >= 6}, key=len, reverse=True):
        out = out.replace(sec, "[REDACTED]")
    return out[:220]


def is_ref_voice_filename(name: Any) -> bool:
    """Return True when `name` has an allowed Zero-shot SVC reference voice audio extension."""
    if not isinstance(name, str) or not name.strip():
        return False
    _, dot, ext = name.strip().rpartition(".")
    return bool(dot and ext.lower() in REF_VOICE_EXTENSIONS)


def _ref_voice_priority(name: str) -> int:
    low = name.strip().lower()
    if low == "hakimi_ref.wav":
        return 0
    if low.startswith("hakimi_ref") and low.endswith(".wav"):
        return 1
    if low.startswith("hakimi_ref"):
        return 2
    return 3


def _escape_drive_query_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _normalize_scopes(scope_value: Any) -> tuple[str, ...]:
    if isinstance(scope_value, str):
        return tuple(part.strip() for part in scope_value.split() if part.strip())
    if isinstance(scope_value, (list, tuple, set, frozenset)):
        return tuple(str(part).strip() for part in scope_value if str(part).strip())
    return ()


def _has_drive_scope(scope_value: Any) -> bool:
    return bool(set(_normalize_scopes(scope_value)) & ALLOWED_DRIVE_SCOPES)


def _validate_google_url(url: str, *, allow_hosts: frozenset[str]) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.netloc not in allow_hosts
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise Rejected("song_drive_disallowed_endpoint")
    return url


def _build_app_properties(
    role: DriveSubRole, app_properties: dict[str, str] | None
) -> dict[str, str]:
    props = {"hakimi_songs_role": role}
    if app_properties:
        for k, v in app_properties.items():
            if isinstance(k, str) and isinstance(v, str) and k and len(v) <= 120:
                props[k[:40]] = v
    return props


def read_song_oauth_credentials(path: Path) -> dict[str, Any]:
    """Read personal OAuth JSON strictly read-only without symlinks or group/world access.

    Never reads ADC credentials, never modifies the file, and never inherits `folder_id`
    or `folder_marker` from `roaming-oauth.json`.
    """
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or bool(info.st_mode & 0o077)
                or info.st_size > MAX_CREDENTIALS_BYTES
            ):
                raise Rejected("song_drive_credentials_invalid")
            raw = stream.read(MAX_CREDENTIALS_BYTES + 1)
            if len(raw) > MAX_CREDENTIALS_BYTES:
                raise Rejected("song_drive_credentials_invalid")
            data = json.loads(raw.decode("utf-8"))
    except Rejected:
        raise
    except (OSError, ValueError, TypeError, UnicodeDecodeError):
        raise Rejected("song_drive_credentials_invalid") from None

    if not isinstance(data, dict) or data.get("type") != "authorized_user":
        raise Rejected("song_drive_credentials_invalid")

    token_uri = data.get("token_uri", TOKEN_URI)
    if not isinstance(token_uri, str) or token_uri.strip() != TOKEN_URI:
        raise Rejected("song_drive_credentials_invalid")

    client_id = data.get("client_id")
    client_secret = data.get("client_secret")
    refresh_token = data.get("refresh_token")
    if (
        not isinstance(client_id, str)
        or not CLIENT_ID_RE.fullmatch(client_id.strip())
        or not isinstance(client_secret, str)
        or not 1 <= len(client_secret.strip()) <= 4096
        or not isinstance(refresh_token, str)
        or not 1 <= len(refresh_token.strip()) <= 8192
    ):
        raise Rejected("song_drive_credentials_invalid")

    raw_scopes = data.get("scopes")
    if raw_scopes is None:
        # Reject ADC files (which carry no explicit Drive OAuth scopes list).
        raise Rejected("song_drive_credentials_invalid")
    if not _has_drive_scope(raw_scopes):
        raise Rejected("song_drive_insufficient_scope")

    # Explicitly discard any `folder_id` / `folder_marker` present in roaming-oauth.json.
    return {
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "refresh_token": refresh_token.strip(),
        "token_uri": TOKEN_URI,
        "scopes": _normalize_scopes(raw_scopes),
    }


class SongDriveStore:
    """Async Google Drive client scoped strictly to `Hakimi_Songs/` for `song_studio`."""

    def __init__(
        self,
        settings: Any,
        store: Any,
        *,
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
        timeout: float = 30.0,
        chunk_bytes: int = RESUMABLE_CHUNK_BYTES,
    ) -> None:
        if (
            isinstance(chunk_bytes, bool)
            or not isinstance(chunk_bytes, int)
            or chunk_bytes < RESUMABLE_CHUNK_GRANULARITY
            or chunk_bytes > MAX_RESUMABLE_CHUNK_BYTES
            or chunk_bytes % RESUMABLE_CHUNK_GRANULARITY != 0
        ):
            raise ValueError("chunk_bytes must be a 256 KiB multiple within [256 KiB, 4 MiB]")
        self.s = settings
        self.store = store
        self._injected_http = http
        self._owned_http: httpx.AsyncClient | None = None
        self._clock = clock
        self._timeout = float(timeout)
        self._chunk_bytes = chunk_bytes

        self._init_lock = asyncio.Lock()
        self._token_lock = asyncio.Lock()
        self._tree_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()

        self.initialized = False
        self._closed = False
        self._folder_id: str = ""
        self._config_source: str = "none"
        self._cred_info: dict[str, Any] | None = None
        self._cred_path_loaded: Path | None = None
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._token_wall_expires_at: float = 0.0
        self._tree: SongDriveFolderTree | None = None
        self._cache_folder_id: str = ""
        self._cache_folder_root_id: str = ""
        self._last_error: str | None = None

    def _oauth_path(self) -> Path | None:
        for attr in ("song_drive_oauth_file", "gdrive_oauth_credentials_file"):
            raw = getattr(self.s, attr, None)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return Path(text)
        return None

    def configured(self) -> bool:
        path = self._oauth_path()
        if path is None:
            return False
        try:
            os.lstat(path)
        except OSError:
            return False
        return True

    @property
    def config_source(self) -> str:
        return self._config_source

    @property
    def drive_songs_folder_id(self) -> str:
        return self._folder_id

    @property
    def folder_tree(self) -> SongDriveFolderTree | None:
        return self._tree

    def _scrub(self, text: Any) -> str:
        extras: list[str] = []
        if self.s is not None:
            try:
                extras.extend(configured_secrets(self.s))
            except Exception:
                pass
        if self._access_token:
            extras.append(self._access_token)
        if self._cred_info:
            for field in ("refresh_token", "client_secret"):
                val = self._cred_info.get(field)
                if isinstance(val, str) and val:
                    extras.append(val)
        return scrub_drive_secrets(text, *extras)

    @staticmethod
    def _clean_optional_folder_id(raw: Any) -> str:
        if not isinstance(raw, str):
            return ""
        cleaned = raw.strip()
        if not cleaned or cleaned.lower() == "root" or not FOLDER_ID_RE.fullmatch(cleaned):
            return ""
        return cleaned

    def _default_env_folder(self) -> tuple[str, str]:
        fid_new = self._clean_optional_folder_id(getattr(self.s, "song_drive_songs_folder_id", ""))
        if fid_new:
            return fid_new, "env"
        fid_legacy = self._clean_optional_folder_id(
            getattr(self.s, "colab_drive_songs_folder_id", "")
        )
        if fid_legacy:
            return fid_legacy, "legacy_env"
        return "", ("default" if self.configured() else "none")

    async def _legacy_colab_folder_id(self) -> str:
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM admin_config WHERE key=?",
                (_LEGACY_COLAB_CONFIG_KEY,),
            )
        if not row or not row[0]:
            return ""
        try:
            raw = self.store.vault.open(_LEGACY_COLAB_VAULT_CTX, row[0])
        except Exception:
            return ""
        if not isinstance(raw, dict):
            return ""
        return self._clean_optional_folder_id(raw.get("drive_songs_folder_id"))

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.lock:
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS admin_config "
                    "(key TEXT PRIMARY KEY, payload BLOB NOT NULL)"
                )
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key=?",
                    (_CONFIG_KEY,),
                )

            env_fid, env_source = self._default_env_folder()
            if row and row[0]:
                try:
                    raw = self.store.vault.open(_VAULT_CFG_CTX, row[0])
                except Exception:
                    raw = None
                if isinstance(raw, dict):
                    if raw.get("cleared") is True:
                        self._folder_id, self._config_source = env_fid, env_source
                    else:
                        fid = self._clean_optional_folder_id(raw.get("drive_songs_folder_id"))
                        if fid or raw.get("drive_songs_folder_id") == "":
                            self._folder_id = fid
                            self._config_source = "admin"
                        else:
                            self._folder_id, self._config_source = env_fid, env_source
                else:
                    self._folder_id, self._config_source = env_fid, env_source
            else:
                legacy_fid = await self._legacy_colab_folder_id()
                if legacy_fid:
                    self._folder_id = legacy_fid
                    self._config_source = "legacy_sqlite"
                else:
                    self._folder_id, self._config_source = env_fid, env_source

            self.initialized = True

    async def set_folder_id(self, folder_id: str | None) -> dict[str, Any]:
        """Persist or reset the Vault-encrypted `Hakimi_Songs/` root folder ID override."""
        await self.initialize()
        async with self._op_lock:
            if folder_id is None:
                legacy_fid = await self._legacy_colab_folder_id()
                if legacy_fid:
                    sealed = self.store.vault.seal(
                        _VAULT_CFG_CTX, {"drive_songs_folder_id": "", "cleared": True}
                    )
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
                            (_CONFIG_KEY, sealed),
                        )
                else:
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "DELETE FROM admin_config WHERE key=?", (_CONFIG_KEY,)
                        )
                self._folder_id, self._config_source = self._default_env_folder()
            else:
                if not isinstance(folder_id, str):
                    raise Rejected("invalid_drive_file_id")
                stripped = folder_id.strip()
                clean_id = validate_drive_id(stripped, is_folder=True) if stripped else ""
                sealed = self.store.vault.seal(_VAULT_CFG_CTX, {"drive_songs_folder_id": clean_id})
                async with self.store.transaction():
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
                        (_CONFIG_KEY, sealed),
                    )
                self._folder_id = clean_id
                self._config_source = "admin"

            async with self._tree_lock:
                self._tree = None
                self._cache_folder_id = ""
                self._cache_folder_root_id = ""
            self._last_error = None

        return await self.status_overview(probe=self.configured())

    async def _http(self) -> httpx.AsyncClient:
        if self._injected_http is not None:
            return self._injected_http
        if self._owned_http is None or self._owned_http.is_closed:
            self._owned_http = httpx.AsyncClient(
                timeout=httpx.Timeout(self._timeout, connect=10.0),
                trust_env=False,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            )
        return self._owned_http

    async def close(self) -> None:
        self._closed = True
        if self._owned_http is not None and not self._owned_http.is_closed:
            await self._owned_http.aclose()
            self._owned_http = None

    async def _load_credentials(self) -> dict[str, Any]:
        path = self._oauth_path()
        if path is None:
            raise Rejected("song_drive_not_configured")
        if self._cred_info is not None and self._cred_path_loaded == path:
            return self._cred_info
        info = await asyncio.to_thread(read_song_oauth_credentials, path)
        self._cred_info = info
        self._cred_path_loaded = path
        return info

    async def _read_bounded_response(
        self,
        response: httpx.Response,
        *,
        max_bytes: int,
        too_large_code: str,
    ) -> bytes:
        buf = bytearray()
        async for chunk in response.aiter_bytes():
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) > max_bytes:
                raise Rejected(too_large_code)
        return bytes(buf)

    async def _refresh_access_token_locked(self) -> str:
        info = await self._load_credentials()
        client = await self._http()
        url = _validate_google_url(TOKEN_URI, allow_hosts=frozenset({"oauth2.googleapis.com"}))
        form_data = {
            "grant_type": "refresh_token",
            "client_id": info["client_id"],
            "client_secret": info["client_secret"],
            "refresh_token": info["refresh_token"],
        }
        try:
            async with client.stream(
                "POST",
                url,
                data=form_data,
                follow_redirects=False,
                timeout=httpx.Timeout(min(self._timeout, 20.0), connect=10.0),
            ) as resp:
                status = resp.status_code
                raw = await self._read_bounded_response(
                    resp,
                    max_bytes=MAX_TOKEN_RESPONSE_BYTES,
                    too_large_code="song_drive_token_failed",
                )
        except Rejected:
            raise
        except httpx.TimeoutException:
            raise Rejected("song_drive_network_timeout") from None
        except (httpx.HTTPError, OSError):
            raise Rejected("song_drive_network_error") from None

        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise Rejected("song_drive_token_failed") from None

        if status != 200 or not isinstance(payload, dict):
            err_code = str(payload.get("error") or "") if isinstance(payload, dict) else ""
            if err_code in {"invalid_scope", "unauthorized_client"}:
                raise Rejected("song_drive_insufficient_scope")
            if err_code == "invalid_grant":
                raise Rejected("song_drive_reauth_required")
            if err_code == "invalid_client":
                raise Rejected("song_drive_credentials_invalid")
            raise Rejected("song_drive_token_failed")

        returned_scope = payload.get("scope")
        if returned_scope is not None and not _has_drive_scope(returned_scope):
            raise Rejected("song_drive_insufficient_scope")

        token = payload.get("access_token")
        token_type = str(payload.get("token_type", "Bearer")).lower()
        expires_in = payload.get("expires_in", 3600)
        if (
            not isinstance(token, str)
            or not 1 <= len(token.strip()) <= 16_384
            or token_type != "bearer"
            or isinstance(expires_in, bool)
            or not isinstance(expires_in, (int, float))
        ):
            raise Rejected("song_drive_token_failed")

        ttl = max(60, min(3600, int(expires_in)))
        self._access_token = token.strip()
        self._token_expires_at = time.monotonic() + ttl - 30
        self._token_wall_expires_at = self._clock() + ttl
        return self._access_token

    async def _get_access_token(self, *, force_refresh: bool = False) -> str:
        async with self._token_lock:
            now = time.monotonic()
            if not force_refresh and self._access_token and now < self._token_expires_at:
                return self._access_token
            return await self._refresh_access_token_locked()

    async def get_short_lived_token(
        self,
        min_ttl_seconds: int = 1800,
        *,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Return an in-memory short-lived Bearer access token only (never refresh_token)."""
        await self.initialize()
        if not self.configured():
            raise Rejected("song_drive_not_configured")
        min_ttl = max(30, min(3300, int(min_ttl_seconds)))
        async with self._token_lock:
            remaining_usable = self._token_expires_at - time.monotonic()
            need_refresh = force_refresh or not self._access_token or remaining_usable < min_ttl
            if need_refresh:
                token = await self._refresh_access_token_locked()
            else:
                token = self._access_token
            expires_in = max(0, int(round(self._token_wall_expires_at - self._clock())))
            return {
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": expires_in,
                "expires_at": round(self._token_wall_expires_at, 3),
            }

    async def _api_request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
        timeout_sec: float | None = None,
        max_response_bytes: int = MAX_JSON_RESPONSE_BYTES,
        too_large_code: str = "song_drive_response_too_large",
    ) -> tuple[int, dict[str, str], bytes]:
        _validate_google_url(url, allow_hosts=frozenset({"www.googleapis.com"}))
        client = await self._http()
        eff_timeout = timeout_sec if timeout_sec is not None else self._timeout

        for attempt in range(2):
            token = await self._get_access_token(force_refresh=(attempt > 0))
            req_headers = {"Authorization": f"Bearer {token}"}
            if headers:
                req_headers.update(headers)
            try:
                async with client.stream(
                    method,
                    url,
                    params=params,
                    headers=req_headers,
                    content=content,
                    follow_redirects=False,
                    timeout=httpx.Timeout(eff_timeout, connect=10.0),
                ) as resp:
                    status = resp.status_code
                    resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                    raw = await self._read_bounded_response(
                        resp,
                        max_bytes=max_response_bytes,
                        too_large_code=too_large_code,
                    )
            except Rejected:
                raise
            except httpx.TimeoutException:
                raise Rejected("song_drive_network_timeout") from None
            except (httpx.HTTPError, OSError):
                raise Rejected("song_drive_network_error") from None

            if status == 401 and attempt == 0:
                async with self._token_lock:
                    self._access_token = ""
                continue
            return status, resp_headers, raw

        raise Rejected("song_drive_token_failed")

    async def _api_json(
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

        status, _, raw = await self._api_request(
            method, url, params=params, headers=headers, content=body
        )
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (ValueError, UnicodeDecodeError):
            raise Rejected("song_drive_invalid_response") from None

        if status == 403:
            reason = ""
            if isinstance(data, dict):
                err = data.get("error")
                if isinstance(err, dict):
                    errors = err.get("errors") or []
                    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
                        reason = str(errors[0].get("reason") or "")
            if reason in {"insufficientPermissions", "accessNotConfigured"}:
                raise Rejected("song_drive_insufficient_scope")
            raise Rejected("song_drive_forbidden")
        if status == 404:
            raise Rejected("song_drive_not_found")
        if status not in {200, 201}:
            raise Rejected("song_drive_request_failed")
        if not isinstance(data, dict):
            raise Rejected("song_drive_invalid_response")
        return data

    async def _find_or_create_folder(
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
        listing = await self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,mimeType,trashed,appProperties)",
                "pageSize": "10",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        files = listing.get("files") or []
        if isinstance(files, list) and files:
            for item in files:
                if not isinstance(item, dict):
                    continue
                props = item.get("appProperties") or {}
                if isinstance(props, dict) and props.get("hakimi_songs_role") == role:
                    return validate_drive_id(str(item.get("id") or ""), is_folder=True)
            if isinstance(files[0], dict):
                return validate_drive_id(str(files[0].get("id") or ""), is_folder=True)

        body: dict[str, Any] = {
            "name": name,
            "mimeType": FOLDER_MIME,
            "appProperties": {"hakimi_songs_role": role},
        }
        if parent_id != "root":
            body["parents"] = [parent_id]
        created = await self._api_json(
            "POST",
            DRIVE_FILES_API,
            params={"fields": "id,name,mimeType,appProperties", "supportsAllDrives": "true"},
            json_body=body,
        )
        return validate_drive_id(str(created.get("id") or ""), is_folder=True)

    async def ensure_folder_tree(self, force_refresh: bool = False) -> SongDriveFolderTree:
        """Idempotently resolve or create `Hakimi_Songs/` and `models/`, `songs/`, `inbox/`."""
        await self.initialize()
        if not self.configured():
            raise Rejected("song_drive_not_configured")

        requested_root = self._folder_id
        async with self._tree_lock:
            if (
                not force_refresh
                and self._tree is not None
                and (not requested_root or self._tree.root == requested_root)
            ):
                return self._tree

            if requested_root:
                root_id = validate_drive_id(requested_root, is_folder=True)
                meta = await self._api_json(
                    "GET",
                    f"{DRIVE_FILES_API}/{root_id}",
                    params={
                        "fields": (
                            "id,name,mimeType,trashed,capabilities(canAddChildren),appProperties"
                        ),
                        "supportsAllDrives": "true",
                    },
                )
                if (
                    meta.get("id") != root_id
                    or meta.get("mimeType") != FOLDER_MIME
                    or meta.get("trashed") is not False
                    or meta.get("capabilities", {}).get("canAddChildren") is False
                ):
                    raise Rejected("song_drive_folder_unavailable")
            else:
                # Never inherit `folder_id` from roaming-oauth.json; anchor in My Drive root.
                root_id = await self._find_or_create_folder(ROOT_FOLDER_NAME, "root", role="root")

            sub_ids: dict[str, str] = {}
            for sub in SUBFOLDER_NAMES:
                role_name: DriveRole = sub  # type: ignore[assignment]
                sub_ids[sub] = await self._find_or_create_folder(sub, root_id, role=role_name)

            self._tree = SongDriveFolderTree(
                root=root_id,
                models=sub_ids["models"],
                songs=sub_ids["songs"],
                inbox=sub_ids["inbox"],
            )
            self._last_error = None
            return self._tree

    async def ensure_cache_folder(self, force_refresh: bool = False) -> str:
        """Lazily resolve or create `Hakimi_Songs/cache/` for versioned weight/venv caches."""
        tree = await self.ensure_folder_tree(force_refresh=force_refresh)
        async with self._tree_lock:
            if (
                not force_refresh
                and self._cache_folder_id
                and self._cache_folder_root_id == tree.root
            ):
                return self._cache_folder_id
            cache_id = await self._find_or_create_folder(CACHE_FOLDER_NAME, tree.root, role="cache")
            self._cache_folder_id = cache_id
            self._cache_folder_root_id = tree.root
            return cache_id

    async def _folder_id_for_role(self, role: DriveRole) -> str:
        if role == "cache":
            return await self.ensure_cache_folder()
        tree = await self.ensure_folder_tree()
        return tree.folder_for(role)

    async def _verify_file_in_role(self, file_id: str, role: DriveSubRole) -> dict[str, Any]:
        """Verify file exists, is not trashed, is not a folder, and parents == [folder_id]."""
        fid = validate_drive_id(file_id)
        expected_parent = await self._folder_id_for_role(role)
        meta = await self._api_json(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={
                "fields": "id,name,parents,mimeType,size,modifiedTime,trashed,appProperties",
                "supportsAllDrives": "true",
            },
        )
        if (
            meta.get("id") != fid
            or meta.get("trashed") is not False
            or meta.get("mimeType") == FOLDER_MIME
            or meta.get("parents") != [expected_parent]
        ):
            raise Rejected("song_drive_file_outside_scope")
        return meta

    async def _resolve_named_file(self, role: DriveSubRole, filename: str) -> dict[str, Any] | None:
        clean_name = sanitize_drive_filename(filename)
        parent_id = await self._folder_id_for_role(role)
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and name = '{_escape_drive_query_literal(clean_name)}' "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = await self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,parents,size,modifiedTime,mimeType,trashed,appProperties)",
                "pageSize": "5",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        files = listing.get("files") or []
        if not isinstance(files, list) or not files:
            return None
        for item in files:
            if not isinstance(item, dict):
                continue
            if item.get("trashed") is True or item.get("mimeType") == FOLDER_MIME:
                continue
            parents = item.get("parents")
            if parents is not None and parents != [parent_id]:
                continue
            fid = validate_drive_id(str(item.get("id") or ""))
            raw_size = item.get("size")
            size_val = int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0
            return {
                "id": fid,
                "name": str(item.get("name") or clean_name),
                "size": size_val,
                "modified_time": str(item.get("modifiedTime") or ""),
                "mime_type": str(item.get("mimeType") or "application/octet-stream"),
                "app_properties": dict(item.get("appProperties") or {}),
            }
        return None

    async def list_inbox(self) -> list[dict[str, Any]]:
        """List non-trashed files directly inside `Hakimi_Songs/inbox/`."""
        parent_id = await self._folder_id_for_role("inbox")
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = await self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,mimeType,size,modifiedTime,parents,trashed)",
                "orderBy": "modifiedTime desc",
                "pageSize": "100",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        items: list[dict[str, Any]] = []
        for raw in listing.get("files") or []:
            if not isinstance(raw, dict):
                continue
            if raw.get("trashed") is True or raw.get("mimeType") == FOLDER_MIME:
                continue
            parents = raw.get("parents")
            if parents is not None and parents != [parent_id]:
                continue
            try:
                fid = validate_drive_id(str(raw.get("id") or ""))
            except Rejected:
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

    async def resolve_inbox_file(
        self,
        file_id: str = "",
        filename: str = "",
    ) -> dict[str, Any]:
        """Resolve an inbox song by Drive file ID or exact filename inside `Hakimi_Songs/inbox/`."""
        if isinstance(file_id, str) and file_id.strip():
            meta = await self._verify_file_in_role(file_id.strip(), "inbox")
            raw_size = meta.get("size")
            return {
                "id": str(meta["id"]),
                "name": str(meta.get("name") or ""),
                "size": int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0,
                "modified_time": str(meta.get("modifiedTime") or ""),
                "mime_type": str(meta.get("mimeType") or "application/octet-stream"),
            }
        if isinstance(filename, str) and filename.strip():
            found = await self._resolve_named_file("inbox", filename)
            if found is None:
                raise Rejected("source_not_found")
            return {
                "id": found["id"],
                "name": found["name"],
                "size": found["size"],
                "modified_time": found["modified_time"],
                "mime_type": found["mime_type"],
            }
        raise Rejected("song_drive_invalid_file")

    async def resolve_model_files(
        self, model_name: str = "hakimi"
    ) -> dict[str, dict[str, Any]] | None:
        """Return Drive metadata for `<model>.pth` and `<model>.index` if both exist in models/."""
        if not isinstance(model_name, str) or not MODEL_NAME_RE.fullmatch(model_name.strip()):
            raise Rejected("song_drive_invalid_file")
        clean_model = model_name.strip()
        remote_pth = await self._resolve_named_file("models", f"{clean_model}.pth")
        if remote_pth is None:
            return None
        remote_idx = await self._resolve_named_file("models", f"{clean_model}.index")
        if remote_idx is None:
            return None
        return {"pth": remote_pth, "index": remote_idx}

    async def list_models_assets(self) -> dict[str, Any]:
        """List Zero-shot reference voice audio files and RVC weights in `Hakimi_Songs/models/`."""
        parent_id = await self._folder_id_for_role("models")
        q = (
            f"'{_escape_drive_query_literal(parent_id)}' in parents "
            f"and mimeType != '{FOLDER_MIME}' and trashed = false"
        )
        listing = await self._api_json(
            "GET",
            DRIVE_FILES_API,
            params={
                "q": q,
                "fields": "files(id,name,mimeType,size,modifiedTime,parents,trashed)",
                "orderBy": "modifiedTime desc",
                "pageSize": "100",
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
            },
        )
        raw_voices: list[dict[str, Any]] = []
        pth_stems: set[str] = set()
        index_stems: set[str] = set()

        for raw in listing.get("files") or []:
            if not isinstance(raw, dict):
                continue
            if raw.get("trashed") is True or raw.get("mimeType") == FOLDER_MIME:
                continue
            parents = raw.get("parents")
            if parents is not None and parents != [parent_id]:
                continue
            try:
                fid = validate_drive_id(str(raw.get("id") or ""))
            except Rejected:
                continue
            name = str(raw.get("name") or "").strip()
            if not name:
                continue
            raw_size = raw.get("size")
            size_val = int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0
            low = name.lower()
            if low.endswith(".pth"):
                stem = name[:-4].strip()
                if MODEL_NAME_RE.fullmatch(stem):
                    pth_stems.add(stem)
                continue
            if low.endswith(".index"):
                stem = name[:-6].strip()
                if MODEL_NAME_RE.fullmatch(stem):
                    index_stems.add(stem)
                continue
            if is_ref_voice_filename(name):
                raw_voices.append(
                    {
                        "id": fid,
                        "name": name,
                        "size": size_val,
                        "modified_time": str(raw.get("modifiedTime") or ""),
                        "mime_type": str(raw.get("mimeType") or "application/octet-stream"),
                    }
                )

        default_id = ""
        default_name = ""
        if raw_voices:
            best_idx = min(
                range(len(raw_voices)),
                key=lambda idx: (_ref_voice_priority(raw_voices[idx]["name"]), idx),
            )
            default_id = raw_voices[best_idx]["id"]
            default_name = raw_voices[best_idx]["name"]

        ref_voices = [
            {
                "id": item["id"],
                "name": item["name"],
                "size": item["size"],
                "modified_time": item["modified_time"],
                "default": item["id"] == default_id,
            }
            for item in raw_voices
        ]
        rvc_models = [
            {"name": stem, "has_index": stem in index_stems} for stem in sorted(pth_stems)
        ]
        return {
            "ref_voices": ref_voices,
            "rvc_models": rvc_models,
            "default_ref_voice": default_name,
        }

    async def resolve_ref_voice(
        self,
        file_id: str = "",
        filename: str = "",
    ) -> dict[str, Any]:
        """Resolve a Zero-shot reference voice audio file strictly inside `Hakimi_Songs/models/`."""
        if isinstance(file_id, str) and file_id.strip():
            try:
                meta = await self._verify_file_in_role(file_id.strip(), "models")
            except Rejected as exc:
                if str(exc) == "song_drive_not_found":
                    raise Rejected("ref_voice_not_found") from None
                raise
            name = str(meta.get("name") or "").strip()
            if not is_ref_voice_filename(name):
                raise Rejected("invalid_ref_voice")
            raw_size = meta.get("size")
            return {
                "id": str(meta["id"]),
                "name": name,
                "size": int(raw_size) if raw_size is not None and str(raw_size).isdigit() else 0,
                "modified_time": str(meta.get("modifiedTime") or ""),
                "mime_type": str(meta.get("mimeType") or "application/octet-stream"),
            }
        if isinstance(filename, str) and filename.strip():
            try:
                clean_name = sanitize_drive_filename(filename)
            except Rejected:
                raise Rejected("invalid_ref_voice") from None
            if not is_ref_voice_filename(clean_name):
                raise Rejected("invalid_ref_voice")
            found = await self._resolve_named_file("models", clean_name)
            if found is None:
                raise Rejected("ref_voice_not_found")
            return {
                "id": found["id"],
                "name": found["name"],
                "size": found["size"],
                "modified_time": found["modified_time"],
                "mime_type": found["mime_type"],
            }
        assets = await self.list_models_assets()
        for item in assets["ref_voices"]:
            if item.get("default"):
                return {
                    "id": item["id"],
                    "name": item["name"],
                    "size": item["size"],
                    "modified_time": item.get("modified_time", ""),
                    "mime_type": "application/octet-stream",
                }
        raise Rejected("ref_voice_not_found")

    async def _download_role_bytes(
        self,
        file_id: str,
        *,
        role: DriveSubRole,
        max_bytes: int,
    ) -> bytes:
        meta = await self._verify_file_in_role(file_id, role)
        raw_size = meta.get("size")
        if raw_size is not None and str(raw_size).isdigit() and int(raw_size) > max_bytes:
            raise Rejected("song_drive_file_too_large")

        fid = str(meta["id"])
        status, _, raw = await self._api_request(
            "GET",
            f"{DRIVE_FILES_API}/{fid}",
            params={"alt": "media", "supportsAllDrives": "true"},
            timeout_sec=max(self._timeout, 60.0),
            max_response_bytes=max_bytes,
            too_large_code="song_drive_file_too_large",
        )
        if status == 404:
            raise Rejected("song_drive_not_found")
        if status != 200:
            raise Rejected("song_drive_download_failed")
        if len(raw) > max_bytes:
            raise Rejected("song_drive_file_too_large")
        return raw

    async def resolve_cache_manifest(
        self,
        cache_key: str,
        expected_kind: str = "weights",
        *,
        expected_fingerprint: str = "",
    ) -> dict[str, Any] | None:
        """Resolve and validate a versioned cache manifest inside `Hakimi_Songs/cache/`."""
        if not isinstance(cache_key, str):
            return None
        clean_key = cache_key.strip()
        if not CACHE_KEY_RE.fullmatch(clean_key):
            return None
        try:
            entry = await self._resolve_named_file("cache", f"{clean_key}.manifest.json")
            if entry is None:
                return None
            raw = await self._download_role_bytes(
                entry["id"],
                role="cache",
                max_bytes=MAX_CACHE_MANIFEST_BYTES,
            )
            manifest = json.loads(raw.decode("utf-8"))
        except (Rejected, ValueError, UnicodeDecodeError):
            return None
        if not isinstance(manifest, dict):
            return None
        if str(manifest.get("version_key") or "") != clean_key:
            return None
        if expected_kind and str(manifest.get("kind") or "") != expected_kind:
            return None
        if expected_fingerprint and str(manifest.get("fingerprint") or "") != expected_fingerprint:
            return None
        archive_id = str(manifest.get("archive_file_id") or "").strip()
        sha256_hex = str(manifest.get("sha256") or "").strip().lower()
        if not archive_id or not SHA256_HEX_RE.fullmatch(sha256_hex):
            return None
        try:
            await self._verify_file_in_role(archive_id, "cache")
        except Rejected:
            return None
        validated = dict(manifest)
        validated["archive_file_id"] = archive_id
        validated["sha256"] = sha256_hex
        validated["manifest_file_id"] = entry["id"]
        return validated

    async def download_song(
        self,
        file_id: str,
        max_bytes: int = MAX_SONG_DOWNLOAD_BYTES,
    ) -> bytes:
        """Download a cover song strictly confined to `Hakimi_Songs/songs/` (`parents==[songs]`)."""
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
            raise Rejected("song_drive_invalid_file")
        bounded = min(max_bytes, MAX_SONG_DOWNLOAD_BYTES)
        return await self._download_role_bytes(file_id, role="songs", max_bytes=bounded)

    async def _upload_media_patch(
        self,
        file_id: str,
        data: bytes,
        mime_type: str,
    ) -> dict[str, Any]:
        status, _, raw = await self._api_request(
            "PATCH",
            f"{DRIVE_UPLOAD_API}/{file_id}",
            params={
                "uploadType": "media",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers={"Content-Type": mime_type},
            content=data,
            timeout_sec=max(self._timeout, 120.0),
        )
        if status not in {200, 201}:
            raise Rejected("song_drive_upload_failed")
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise Rejected("song_drive_invalid_response") from None
        if not isinstance(parsed, dict):
            raise Rejected("song_drive_invalid_response")
        return parsed

    async def _upload_multipart_create(
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
        status, _, raw = await self._api_request(
            "POST",
            DRIVE_UPLOAD_API,
            params={
                "uploadType": "multipart",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers={"Content-Type": f"multipart/related; boundary={boundary}"},
            content=body,
            timeout_sec=max(self._timeout, 120.0),
        )
        if status not in {200, 201}:
            raise Rejected("song_drive_upload_failed")
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise Rejected("song_drive_invalid_response") from None
        if not isinstance(parsed, dict):
            raise Rejected("song_drive_invalid_response")
        return parsed

    async def _start_resumable_session(
        self,
        parent_id: str,
        filename: str,
        mime_type: str,
        *,
        total_bytes: int | None = None,
        existing_id: str | None = None,
        app_properties: dict[str, str] | None = None,
    ) -> str:
        meta: dict[str, Any] = {"name": filename, "appProperties": app_properties or {}}
        if existing_id is None:
            meta["parents"] = [parent_id]
        method = "PATCH" if existing_id else "POST"
        start_url = f"{DRIVE_UPLOAD_API}/{existing_id}" if existing_id else DRIVE_UPLOAD_API
        headers: dict[str, str] = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": mime_type,
        }
        if total_bytes is not None and total_bytes > 0:
            headers["X-Upload-Content-Length"] = str(total_bytes)

        status, resp_headers, _ = await self._api_request(
            method,
            start_url,
            params={
                "uploadType": "resumable",
                "supportsAllDrives": "true",
                "fields": "id,name,mimeType,size",
            },
            headers=headers,
            content=json.dumps(meta, ensure_ascii=False).encode("utf-8"),
        )
        upload_url = resp_headers.get("location", "").strip()
        if status not in {200, 201} or not upload_url:
            raise Rejected("song_drive_upload_failed")
        _validate_google_url(upload_url, allow_hosts=frozenset({"www.googleapis.com"}))
        return upload_url

    async def _abort_resumable_session(self, upload_url: str) -> None:
        try:
            _validate_google_url(upload_url, allow_hosts=frozenset({"www.googleapis.com"}))
            client = await self._http()
            await client.delete(
                upload_url,
                follow_redirects=False,
                timeout=httpx.Timeout(5.0, connect=5.0),
            )
        except Exception:
            pass

    async def _put_resumable_chunk(
        self,
        upload_url: str,
        chunk: bytes,
        *,
        offset: int,
        final_total: int | None,
        mime_type: str,
    ) -> tuple[int, dict[str, str], bytes]:
        _validate_google_url(upload_url, allow_hosts=frozenset({"www.googleapis.com"}))
        end = offset + len(chunk) - 1
        total_spec = str(final_total) if final_total is not None else "*"
        req_headers = {
            "Content-Length": str(len(chunk)),
            "Content-Range": f"bytes {offset}-{end}/{total_spec}",
            "Content-Type": mime_type,
        }
        client = await self._http()
        req = client.build_request(
            "PUT",
            upload_url,
            headers=req_headers,
            content=chunk,
            timeout=httpx.Timeout(max(self._timeout, 120.0), connect=10.0),
        )
        resp: httpx.Response | None = None
        try:
            resp = await client.send(req, stream=True, follow_redirects=False)
            status = resp.status_code
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            raw = await self._read_bounded_response(
                resp,
                max_bytes=MAX_JSON_RESPONSE_BYTES,
                too_large_code="song_drive_response_too_large",
            )
        except Rejected:
            raise
        except httpx.TimeoutException:
            raise Rejected("song_drive_network_timeout") from None
        except (httpx.HTTPError, OSError):
            raise Rejected("song_drive_network_error") from None
        finally:
            try:
                if resp is not None:
                    await resp.aclose()
            except Exception:
                pass
            finally:
                if resp is not None:
                    resp._request = None
                # Explicitly detach the transmitted chunk from httpx.Request (_content and
                # ByteStream) so cyclic Request/Response references never retain multi-MiB
                # uploaded chunks until the next cyclic GC sweep.
                req._content = b""
                req.stream = httpx.ByteStream(b"")
        return status, resp_headers, raw

    async def _upload_role_stream(
        self,
        role: DriveSubRole,
        filename: str,
        stream: AsyncIterator[bytes],
        *,
        mime_type: str = "application/octet-stream",
        content_length: int | None = None,
        max_bytes: int,
        hard_limit_bytes: int,
        too_large_code: str,
        app_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Stream an audio file into a scoped Drive subfolder via Resumable Upload."""
        await self.initialize()
        if not self.configured():
            raise Rejected("song_drive_not_configured")
        clean_name = sanitize_drive_filename(filename)
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
            raise Rejected("song_drive_invalid_file")
        effective_max = min(max_bytes, hard_limit_bytes)

        if content_length is not None:
            if isinstance(content_length, bool) or not isinstance(content_length, int):
                raise Rejected("song_drive_invalid_file")
            if content_length < 0:
                raise Rejected("song_drive_invalid_file")
            if content_length == 0:
                raise Rejected("invalid_song_audio")
            if content_length > effective_max:
                raise Rejected(too_large_code)

        resolved_mime = (mime_type or "application/octet-stream").split(";")[0].strip()
        if not resolved_mime:
            resolved_mime = "application/octet-stream"

        it = aiter(stream)
        first_non_empty = b""
        while not first_non_empty:
            try:
                part = await anext(it)
            except StopAsyncIteration:
                raise Rejected("invalid_song_audio") from None
            if part:
                first_non_empty = bytes(part)

        if len(first_non_empty) > effective_max:
            raise Rejected(too_large_code)

        parent_id = await self._folder_id_for_role(role)
        existing = await self._resolve_named_file(role, clean_name)
        props = _build_app_properties(role, app_properties)
        upload_url = await self._start_resumable_session(
            parent_id,
            clean_name,
            resolved_mime,
            total_bytes=content_length,
            existing_id=existing["id"] if existing else None,
            app_properties=props,
        )

        chunk_size = self._chunk_bytes
        buf = bytearray()
        offset = 0
        total_seen = 0
        completed = False

        async def _feed_bytes(data: bytes) -> None:
            nonlocal offset, total_seen
            if total_seen + len(data) > effective_max:
                raise Rejected(too_large_code)
            total_seen += len(data)
            view = memoryview(data)
            pos = 0
            n = len(view)
            while pos < n:
                if len(buf) == chunk_size:
                    # More bytes remain (`pos < n`), so this full chunk is strictly non-final.
                    payload = bytes(buf)
                    buf.clear()
                    c_status, c_headers, _ = await self._put_resumable_chunk(
                        upload_url,
                        payload,
                        offset=offset,
                        final_total=None,
                        mime_type=resolved_mime,
                    )
                    del payload
                    if c_status != 308:
                        raise Rejected("song_drive_upload_failed")
                    range_hdr = c_headers.get("range", "").strip()
                    if range_hdr:
                        m = _RANGE_HEADER_RE.fullmatch(range_hdr)
                        if m is None or int(m.group(1)) != offset + chunk_size - 1:
                            raise Rejected("song_drive_upload_failed")
                    offset += chunk_size
                take = min(chunk_size - len(buf), n - pos)
                buf.extend(view[pos : pos + take])
                pos += take

        try:
            await _feed_bytes(first_non_empty)
            del first_non_empty
            async for raw_part in it:
                if not raw_part:
                    continue
                await _feed_bytes(bytes(raw_part))

            if content_length is not None and total_seen != content_length:
                raise Rejected("invalid_song_audio")
            if not buf or total_seen <= 0:
                raise Rejected("invalid_song_audio")

            final_payload = bytes(buf)
            buf.clear()
            f_status, _, f_raw = await self._put_resumable_chunk(
                upload_url,
                final_payload,
                offset=offset,
                final_total=total_seen,
                mime_type=resolved_mime,
            )
            del final_payload
            if f_status not in {200, 201}:
                raise Rejected("song_drive_upload_failed")
            try:
                resp = json.loads(f_raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                raise Rejected("song_drive_invalid_response") from None
            if not isinstance(resp, dict):
                raise Rejected("song_drive_invalid_response")
            fid = validate_drive_id(str(resp.get("id") or ""))
            completed = True
            return {
                "file_id": fid,
                "name": str(resp.get("name") or clean_name),
                "size": total_seen,
                "mime_type": str(resp.get("mimeType") or resolved_mime),
            }
        finally:
            buf.clear()
            if not completed:
                await self._abort_resumable_session(upload_url)

    async def upload_inbox_stream(
        self,
        filename: str,
        stream: AsyncIterator[bytes],
        *,
        mime_type: str = "application/octet-stream",
        content_length: int | None = None,
        max_bytes: int = MAX_INBOX_UPLOAD_BYTES,
    ) -> dict[str, Any]:
        """Stream an audio file into `Hakimi_Songs/inbox/` via Drive v3 Resumable Upload.

        Uses 256 KiB-aligned chunks (default 2 MiB) so uploading up to 100 MiB keeps peak
        in-process memory strictly bounded (<= 4 MiB).
        """
        return await self._upload_role_stream(
            "inbox",
            filename,
            stream,
            mime_type=mime_type,
            content_length=content_length,
            max_bytes=max_bytes,
            hard_limit_bytes=MAX_INBOX_UPLOAD_BYTES,
            too_large_code="song_inbox_upload_too_large",
        )

    async def upload_ref_voice_stream(
        self,
        filename: str,
        stream: AsyncIterator[bytes],
        *,
        mime_type: str = "application/octet-stream",
        content_length: int | None = None,
        max_bytes: int = MAX_REF_VOICE_UPLOAD_BYTES,
    ) -> dict[str, Any]:
        """Stream a Zero-shot reference voice audio file into `Hakimi_Songs/models/` (<= 20 MiB)."""
        try:
            clean_name = sanitize_drive_filename(filename)
        except Rejected:
            raise Rejected("invalid_ref_voice") from None
        if not is_ref_voice_filename(clean_name):
            raise Rejected("invalid_ref_voice")
        return await self._upload_role_stream(
            "models",
            clean_name,
            stream,
            mime_type=mime_type,
            content_length=content_length,
            max_bytes=max_bytes,
            hard_limit_bytes=MAX_REF_VOICE_UPLOAD_BYTES,
            too_large_code="song_ref_voice_too_large",
            app_properties={"asset_kind": "ref_voice"},
        )

    async def _upload_role_bytes(
        self,
        role: DriveSubRole,
        filename: str,
        raw: bytes,
        *,
        mime_type: str,
        max_bytes: int,
        app_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("song_drive_not_configured")
        clean_name = sanitize_drive_filename(filename)
        if not isinstance(raw, (bytes, bytearray)) or not raw:
            raise Rejected("invalid_song_audio")
        data = bytes(raw)
        if len(data) > max_bytes:
            raise Rejected(
                "song_inbox_upload_too_large" if role == "inbox" else "song_drive_file_too_large"
            )
        resolved_mime = (mime_type or "application/octet-stream").split(";")[0].strip()
        if not resolved_mime:
            resolved_mime = "application/octet-stream"

        if len(data) > MULTIPART_THRESHOLD_BYTES and role == "inbox":

            async def _slice_iter() -> AsyncIterator[bytes]:
                step = self._chunk_bytes
                for idx in range(0, len(data), step):
                    yield data[idx : idx + step]

            return await self.upload_inbox_stream(
                clean_name,
                _slice_iter(),
                mime_type=resolved_mime,
                content_length=len(data),
                max_bytes=max_bytes,
            )

        parent_id = await self._folder_id_for_role(role)
        existing = await self._resolve_named_file(role, clean_name)
        props = _build_app_properties(role, app_properties)
        if len(data) <= MULTIPART_THRESHOLD_BYTES:
            if existing is not None:
                resp = await self._upload_media_patch(existing["id"], data, resolved_mime)
            else:
                resp = await self._upload_multipart_create(
                    parent_id, clean_name, data, resolved_mime, app_properties=props
                )
        else:
            upload_url = await self._start_resumable_session(
                parent_id,
                clean_name,
                resolved_mime,
                total_bytes=len(data),
                existing_id=existing["id"] if existing else None,
                app_properties=props,
            )
            step = self._chunk_bytes
            total = len(data)
            resp = {}
            completed = False
            try:
                for off in range(0, total, step):
                    end = min(total, off + step)
                    is_final = end == total
                    st, _, r_raw = await self._put_resumable_chunk(
                        upload_url,
                        data[off:end],
                        offset=off,
                        final_total=total if is_final else None,
                        mime_type=resolved_mime,
                    )
                    if not is_final:
                        if st != 308:
                            raise Rejected("song_drive_upload_failed")
                        continue
                    if st not in {200, 201}:
                        raise Rejected("song_drive_upload_failed")
                    try:
                        parsed = json.loads(r_raw.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        raise Rejected("song_drive_invalid_response") from None
                    if not isinstance(parsed, dict):
                        raise Rejected("song_drive_invalid_response")
                    resp = parsed
                    completed = True
            finally:
                if not completed:
                    await self._abort_resumable_session(upload_url)

        fid = validate_drive_id(str(resp.get("id") or ""))
        return {
            "file_id": fid,
            "name": str(resp.get("name") or clean_name),
            "size": len(data),
            "mime_type": str(resp.get("mimeType") or resolved_mime),
        }

    async def upload_inbox_bytes(
        self,
        filename: str,
        raw: bytes,
        *,
        mime_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """Upload bounded audio bytes into `Hakimi_Songs/inbox/`."""
        return await self._upload_role_bytes(
            "inbox",
            filename,
            raw,
            mime_type=mime_type,
            max_bytes=MAX_INBOX_UPLOAD_BYTES,
        )

    async def upload_song(
        self,
        filename: str,
        raw: bytes,
        title: str = "",
        mime_type: str = "audio/mpeg",
    ) -> dict[str, Any]:
        """Upload a cover audio file into `Hakimi_Songs/songs/`."""
        clean_title = title.strip()[:80] if isinstance(title, str) else ""
        props = {"song_title": clean_title} if clean_title else None
        return await self._upload_role_bytes(
            "songs",
            filename,
            raw,
            mime_type=mime_type,
            max_bytes=MAX_SONG_UPLOAD_BYTES,
            app_properties=props,
        )

    async def status_overview(
        self, probe: bool = False, *, force_refresh: bool = False
    ) -> dict[str, Any]:
        """Return safe Drive configuration and folder status overview."""
        await self.initialize()
        is_cfg = self.configured()
        folders = self._tree.as_dict() if self._tree is not None else None
        error = self._last_error

        if probe:
            if not is_cfg:
                folders = None
                error = None
            else:
                try:
                    tree = await self.ensure_folder_tree(force_refresh=force_refresh)
                    folders = tree.as_dict()
                    error = None
                    self._last_error = None
                except Rejected as exc:
                    error = self._scrub(str(exc))
                    self._last_error = error
                except Exception as exc:
                    error = self._scrub(type(exc).__name__)
                    self._last_error = error

        return {
            "configured": is_cfg,
            "source": self._config_source,
            "drive_songs_folder_id": self._folder_id,
            "folders": folders,
            "error": error,
        }
