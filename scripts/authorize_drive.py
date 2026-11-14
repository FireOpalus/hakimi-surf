"""One-time personal Drive setup through a loopback OAuth callback + SSH tunnel.

Run from the project root with .venv/bin/python scripts/authorize_drive.py.
Never logs authorization codes, client secrets, refresh tokens or callback URLs.
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import tempfile
from pathlib import Path
from urllib.parse import urlencode

import httpx
from aiohttp import web

from hakimi.drive_client import API, SCOPE, TOKEN_URI
from hakimi.drive_oauth import (
    AUTH_URI,
    FOLDER_MARKER,
    OAuthDriveAuth,
    load_oauth_credentials,
    read_private_json,
    request_oauth_token,
    valid_client,
)
from hakimi.security import Rejected

FOLDER_FIELDS = "id,name,mimeType,trashed,appProperties,capabilities(canAddChildren)"
WEB_HEADERS = {
    "Cache-Control": "no-store",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
    "X-Content-Type-Options": "nosniff",
}


def load_desktop_client(path: Path) -> dict:
    value = read_private_json(path).get("installed")
    if (
        not isinstance(value, dict)
        or not valid_client(value)
        or value.get("auth_uri") not in {"https://accounts.google.com/o/oauth2/auth", AUTH_URI}
    ):
        raise Rejected("drive_oauth_desktop_client_required")
    return value


def authorization_url(client_id: str, redirect: str, state: str, verifier: str) -> str:
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode()
    return (
        AUTH_URI
        + "?"
        + urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect,
                "response_type": "code",
                "scope": SCOPE,
                "state": state,
                "code_challenge": challenge.rstrip("="),
                "code_challenge_method": "S256",
                "access_type": "offline",
                "prompt": "consent select_account",
            }
        )
    )


def callback_code(query, expected_state: str) -> str:
    states, codes = query.getall("state", []), query.getall("code", [])
    if len(states) != 1 or not hmac.compare_digest(states[0].encode(), expected_state.encode()):
        raise Rejected("oauth_invalid_state")
    if "error" in query:
        raise Rejected("oauth_consent_denied")
    if len(codes) != 1 or not 1 <= len(codes[0]) <= 4096:
        raise Rejected("oauth_invalid_code")
    return codes[0]


def allowed_loopback_host(host: str, port: int) -> bool:
    # SSH/IDE local forwards may use localhost or omit the port in Host.
    # Only literal loopback aliases are accepted; no arbitrary proxy/public hosts.
    match = re.fullmatch(r"(?:127\.0\.0\.1|localhost|\[::1\])(?::([0-9]{1,5}))?", host, re.I)
    return bool(match and (match[1] is None or int(match[1]) == port))


async def receive_code(client_id: str, port: int) -> tuple[str, str, str]:
    state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(64)
    authority = f"127.0.0.1:{port}"
    redirect = f"http://{authority}/oauth/callback"
    start_path = "/start/" + secrets.token_urlsafe(32)
    url = authorization_url(client_id, redirect, state, verifier)
    future = asyncio.get_running_loop().create_future()

    @web.middleware
    async def loopback_only(request, handler):
        if len(request.headers.getall("Host", [])) != 1 or not allowed_loopback_host(
            request.host, port
        ):
            return web.Response(status=403, text="Invalid host", headers=WEB_HEADERS)
        return await handler(request)

    async def start(request):
        return web.Response(status=303, headers={**WEB_HEADERS, "Location": url})

    async def callback(request):
        if future.done():
            return web.Response(
                status=409, text="Authorization already received", headers=WEB_HEADERS
            )
        try:
            code = callback_code(request.query, state)
        except Rejected as exc:
            if str(exc) == "oauth_consent_denied":
                future.set_exception(exc)
            return web.Response(status=400, text="Authorization rejected", headers=WEB_HEADERS)
        future.set_result(code)
        return web.Response(status=303, headers={**WEB_HEADERS, "Location": "/complete"})

    async def complete(request):
        return web.Response(
            text="授权已接收。请返回部署聊天查看凭证核验和日记同步结果，可以关闭此页面。",
            headers=WEB_HEADERS,
        )

    app = web.Application(middlewares=[loopback_only], client_max_size=8192)
    app.router.add_get(start_path, start)
    app.router.add_get("/oauth/callback", callback)
    app.router.add_get("/complete", complete)
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=2)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", port).start()
        print(f"AUTH_URL=http://{authority}{start_path}", flush=True)
        print("Waiting up to 15 minutes for browser consent through the SSH tunnel.", flush=True)
        code = await asyncio.wait_for(future, timeout=900)
        # Let the browser's redirect finish before shutting the temporary listener down.
        await asyncio.sleep(2)
        return code, verifier, redirect
    finally:
        await runner.cleanup()


def save_credentials(path: Path, value: dict):
    path = path.absolute()
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise Rejected("drive_oauth_credentials_invalid")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".oauth-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def drive_json(http, token, method, suffix="", *, missing_ok=False, **kwargs):
    async with http.stream(
        method,
        API + suffix,
        headers={"Authorization": "Bearer " + token},
        follow_redirects=False,
        **kwargs,
    ) as response:
        if missing_ok and response.status_code == 404:
            return None
        if response.status_code not in {200, 201}:
            raise Rejected(f"drive_setup_http_{response.status_code}")
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > 65536:
                raise Rejected("drive_setup_response_invalid")
        value = json.loads(body)
        if not isinstance(value, dict):
            raise Rejected("drive_setup_response_invalid")
        return value


def check_folder(item: dict, info: dict):
    if (
        item.get("id") != info["folder_id"]
        or item.get("mimeType") != "application/vnd.google-apps.folder"
        or item.get("trashed")
        or not item.get("capabilities", {}).get("canAddChildren")
        or item.get("appProperties", {}).get(FOLDER_MARKER) != info["folder_marker"]
    ):
        raise Rejected("drive_oauth_folder_mismatch")


async def ensure_folder(http, token: str, info: dict, *, allow_create: bool):
    # Preallocated ID is journaled before creation. Resuming never searches My Drive.
    item = await drive_json(
        http,
        token,
        "GET",
        "/" + info["folder_id"],
        missing_ok=allow_create,
        params={"fields": FOLDER_FIELDS, "supportsAllDrives": "true"},
    )
    if item is None:
        item = await drive_json(
            http,
            token,
            "POST",
            params={"fields": FOLDER_FIELDS},
            json={
                "id": info["folder_id"],
                "name": "哈基米巡逻日记",
                "mimeType": "application/vnd.google-apps.folder",
                "appProperties": {FOLDER_MARKER: info["folder_marker"]},
            },
        )
    check_folder(item, info)


async def run(args):
    old = load_oauth_credentials(args.output) if args.output.exists() else None
    async with httpx.AsyncClient(timeout=20, trust_env=False, follow_redirects=False) as http:
        if old is not None and not args.reauthorize:
            token = await OAuthDriveAuth(args.output, old["folder_id"], http).access_token()
            await ensure_folder(http, token, old, allow_create=True)
            info = old
        else:
            client = load_desktop_client(args.client)
            if old is not None and client["client_id"] != old["client_id"]:
                raise Rejected("drive_oauth_client_mismatch")
            code, verifier, redirect = await receive_code(client["client_id"], args.port)
            tokens = await request_oauth_token(
                http,
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "code_verifier": verifier,
                    "redirect_uri": redirect,
                    "client_id": client["client_id"],
                    "client_secret": client["client_secret"],
                },
                require_scope=True,
            )
            refresh = tokens.get("refresh_token")
            if not isinstance(refresh, str) or not 1 <= len(refresh) <= 8192:
                raise Rejected("drive_oauth_refresh_token_required")
            if old is None:
                generated = await drive_json(
                    http,
                    tokens["access_token"],
                    "GET",
                    "/generateIds",
                    params={"count": 1, "space": "drive", "type": "files", "fields": "ids"},
                )
                ids = generated.get("ids")
                if (
                    not isinstance(ids, list)
                    or len(ids) != 1
                    or not re.fullmatch(r"[A-Za-z0-9_-]{10,128}", str(ids[0]))
                ):
                    raise Rejected("drive_setup_response_invalid")
                folder_id, marker = ids[0], secrets.token_hex(16)
            else:
                folder_id, marker = old["folder_id"], old["folder_marker"]
            info = {
                "type": "authorized_user",
                "client_id": client["client_id"],
                "client_secret": client["client_secret"],
                "token_uri": TOKEN_URI,
                "scopes": [SCOPE],
                "refresh_token": refresh,
                "folder_id": folder_id,
                "folder_marker": marker,
            }
            if old is None:
                # Persist the reserved ID before an ambiguous network write can occur.
                save_credentials(args.output, info)
            await ensure_folder(http, tokens["access_token"], info, allow_create=old is None)
            if old is not None:
                # A wrong Google account must not replace working credentials.
                save_credentials(args.output, info)
        print("OAUTH_SETUP_OK", flush=True)
        print("GDRIVE_ROAMING_FOLDER_ID=" + info["folder_id"], flush=True)
        print("Credentials saved locally; no private Drive files were listed or read.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, default=Path("secrets/roaming-oauth-client.json"))
    parser.add_argument("--output", type=Path, default=Path("secrets/roaming-oauth.json"))
    parser.add_argument("--port", type=int, default=12142)
    parser.add_argument("--reauthorize", action="store_true")
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        parser.error("Use a loopback port between 1024 and 65535")
    try:
        asyncio.run(run(args))
    except Rejected as exc:
        parser.exit(1, f"Setup stopped: {exc}\n")
    except (TimeoutError, KeyboardInterrupt):
        parser.exit(1, "Setup stopped: authorization timed out or was cancelled.\n")
    except Exception:
        # Never expose callback codes, credentials or provider error bodies in a traceback.
        parser.exit(1, "Setup stopped: network or local file error. No credentials printed.\n")


if __name__ == "__main__":
    main()
