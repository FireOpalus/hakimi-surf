"""Explicit, folder-bound personal Drive OAuth; never used by the owner notebook."""

import asyncio
import json
import os
import re
import stat
import time
from pathlib import Path

import httpx

from .drive_client import SCOPE, TOKEN_URI
from .security import Rejected

AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
FOLDER_MARKER = "hakimi_roaming_folder"


def read_private_json(path: Path) -> dict:
    """Do not follow symlinks, read device files, or accept publicly readable tokens."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
                raise Rejected("drive_oauth_credentials_invalid")
            value = json.loads(stream.read(65537))
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (OSError, ValueError, TypeError):
        raise Rejected("drive_oauth_credentials_invalid") from None


def valid_client(info: dict) -> bool:
    return (
        isinstance(info.get("client_id"), str)
        and re.fullmatch(r"[A-Za-z0-9_-]{1,256}\.apps\.googleusercontent\.com", info["client_id"])
        is not None
        and isinstance(info.get("client_secret"), str)
        and 1 <= len(info["client_secret"]) <= 4096
        and info.get("token_uri") == TOKEN_URI
    )


def load_oauth_credentials(path: Path, folder: str | None = None) -> dict:
    info = read_private_json(path)
    if (
        info.get("type") != "authorized_user"
        or not valid_client(info)
        or info.get("scopes") != [SCOPE]
        or not isinstance(info.get("refresh_token"), str)
        or not 1 <= len(info["refresh_token"]) <= 8192
        or not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", str(info.get("folder_id", "")))
        or not re.fullmatch(r"[a-f0-9]{32}", str(info.get("folder_marker", "")))
    ):
        raise Rejected("drive_oauth_credentials_invalid")
    if folder is not None and info["folder_id"] != folder:
        raise Rejected("drive_oauth_folder_mismatch")
    return info


async def request_oauth_token(
    client: httpx.AsyncClient, data: dict, *, require_scope: bool = False
) -> dict:
    # Only the fixed Google endpoint receives secrets; no redirects or environment proxies.
    async with client.stream("POST", TOKEN_URI, data=data, follow_redirects=False) as response:
        raw = bytearray()
        async for chunk in response.aiter_bytes():
            raw.extend(chunk)
            if len(raw) > 32768:
                raise Rejected("drive_token_failed")
        try:
            result = json.loads(raw)
        except ValueError:
            raise Rejected("drive_token_failed") from None
        if not isinstance(result, dict):
            raise Rejected("drive_token_failed")
        if response.status_code != 200:
            error = result.get("error")
            reason = {
                "invalid_grant": "drive_oauth_reauthorization_required",
                "invalid_client": "drive_oauth_client_invalid",
            }.get(error if isinstance(error, str) else "", "drive_token_failed")
            raise Rejected(reason)
    scopes = result.get("scope")
    if (require_scope or scopes is not None) and (
        not isinstance(scopes, str) or set(scopes.split()) != {SCOPE}
    ):
        raise Rejected("drive_oauth_scope_mismatch")
    if (
        not isinstance(result.get("access_token"), str)
        or not 1 <= len(result["access_token"]) <= 16384
        or str(result.get("token_type", "")).lower() != "bearer"
        or not isinstance(result.get("expires_in"), int)
        or not 60 <= result["expires_in"] <= 3600
    ):
        raise Rejected("drive_token_failed")
    return result


class OAuthDriveAuth:
    def __init__(self, path: Path | None, folder: str, client: httpx.AsyncClient):
        self.path, self.folder, self.http = path, folder, client
        self.info: dict | None = None
        self.token = ""
        self.expires = 0.0
        self.lock = asyncio.Lock()

    async def access_token(self) -> str:
        async with self.lock:
            if self.token and time.monotonic() < self.expires:
                return self.token
            if self.info is None:
                if self.path is None:
                    raise Rejected("drive_oauth_credentials_invalid")
                self.info = await asyncio.to_thread(load_oauth_credentials, self.path, self.folder)
            result = await request_oauth_token(
                self.http,
                {
                    "grant_type": "refresh_token",
                    "client_id": self.info["client_id"],
                    "client_secret": self.info["client_secret"],
                    "refresh_token": self.info["refresh_token"],
                    "scope": SCOPE,
                },
            )
            self.token = result["access_token"]
            self.expires = time.monotonic() + result["expires_in"] - 30
            return self.token
