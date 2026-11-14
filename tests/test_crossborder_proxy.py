"""Real, isolated TLS Nginx -> ASGI smoke test; never touches the host's live Nginx."""

import asyncio
import json
import shutil
import socket
import ssl
import subprocess
from contextlib import suppress
from pathlib import Path

import httpx
import pytest
import uvicorn
from pydantic import SecretStr
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from hakimi.app import create_app
from tests.fakes import FakeGateway

TOKEN = "nginx-smoke-token-fake-32chars-never-live"
ROUTE = "/ws/onebot/0123456789abcdef0123456789abcdef"


def bound_socket():
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    return sock


@pytest.fixture
async def proxy(settings, tmp_path):
    if not shutil.which("nginx") or not shutil.which("openssl"):
        pytest.skip("Nginx and openssl are required for the isolated TLS smoke test")
    settings.onebot_mode = "ws_reverse"
    settings.onebot_ws_token = SecretStr(TOKEN)
    settings.onebot_ws_path = ROUTE
    settings.proactive_dm_enabled = False
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    backend_socket = bound_socket()
    backend_port = backend_socket.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=backend_port,
            log_level="critical",
            access_log=False,
            ws="websockets-sansio",
            ws_max_size=16777216,
            ws_max_queue=2,
            ws_ping_interval=25,
            ws_ping_timeout=30,
            proxy_headers=False,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[backend_socket]))
    nginx = None
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
        await asyncio.to_thread(
            subprocess.run,
            [
                "openssl",
                "req",
                "-x509",
                "-nodes",
                "-newkey",
                "rsa:2048",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        probe = bound_socket()
        port = probe.getsockname()[1]
        probe.close()
        snippet = (Path(__file__).parents[1] / "deploy/hakimi-ws.conf").read_text()
        snippet = (
            snippet.replace("listen 443 ssl;", f"listen 127.0.0.1:{port} ssl;")
            .replace("listen [::]:443 ssl;", "")
            .replace("/etc/letsencrypt/live/qq.example.com/fullchain.pem", str(cert))
            .replace("/etc/letsencrypt/live/qq.example.com/privkey.pem", str(key))
            .replace("/ws/onebot/REPLACE_WITH_48_RANDOM_HEX_CHARS", ROUTE)
            .replace("http://127.0.0.1:12141", f"http://127.0.0.1:{backend_port}")
        )
        config = tmp_path / "nginx.conf"
        config.write_text(
            f"pid {tmp_path / 'pid'};\nerror_log stderr;\nevents {{}}\nhttp {{\n" + snippet + "\n}"
        )
        await asyncio.to_thread(
            subprocess.run, ["nginx", "-t", "-c", str(config)], check=True, capture_output=True
        )
        nginx = await asyncio.create_subprocess_exec(
            "nginx",
            "-c",
            str(config),
            "-g",
            "daemon off;",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        context = ssl.create_default_context(cafile=str(cert))
        async with httpx.AsyncClient(verify=context, trust_env=False) as client:
            async with asyncio.timeout(5):
                while True:
                    try:
                        response = await client.get(f"https://localhost:{port}/")
                        if response.status_code == 404:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.02)
            yield app, gateway, client, port, context
    finally:
        if nginx is not None:
            with suppress(ProcessLookupError):
                nginx.terminate()
            async with asyncio.timeout(5):
                await nginx.communicate()
        server.should_exit = True
        async with asyncio.timeout(5):
            await server_task
        backend_socket.close()


async def test_exact_tls_route_auth_and_large_media_rpc(proxy):
    app, gateway, client, port, context = proxy
    for path in (
        "/",
        "/admin",
        "/admin/api/dashboard",
        "/docs",
        "/openapi.json",
        "/ws/onebot",
        ROUTE + "/extra",
        ROUTE,
    ):
        assert (await client.get(f"https://localhost:{port}" + path)).status_code == 404
    auth = {
        "Authorization": "Bearer " + TOKEN,
        "X-Self-ID": "10000",
        "X-Client-Role": "Universal",
        "X-Real-IP": "198.51.100.99",
    }
    for path, headers, status in [
        ("/ws/onebot", auth, 404),
        (ROUTE, {**auth, "Authorization": "Bearer invalid"}, 403),
    ]:
        with pytest.raises(InvalidStatus) as caught:
            async with connect(
                f"wss://localhost:{port}" + path,
                ssl=context,
                additional_headers=headers,
                proxy=None,
            ):
                pytest.fail("invalid handshake accepted")
        assert caught.value.response.status_code == status
    async with connect(
        f"wss://localhost:{port}" + ROUTE,
        ssl=context,
        additional_headers=auth,
        proxy=None,
        max_size=16777216,
    ) as websocket:
        login = json.loads(await websocket.recv())
        assert login["action"] == "get_login_info"
        await websocket.send(
            json.dumps(
                {"echo": login["echo"], "status": "ok", "retcode": 0, "data": {"user_id": 10000}}
            )
        )
        await asyncio.wait_for(app.state.onebot.ready.wait(), 2)
        assert app.state.onebot.status()["source_ip"] == "127.0.0.1"  # Proxy replaces spoof.
        operation = asyncio.create_task(app.state.onebot.call("get_image", {"file": "fixture"}))
        request = json.loads(await websocket.recv())
        assert request["action"] == "get_image"
        # An RPC payload much larger than both the old 64 KiB cap and old 1 MiB default.
        large_base64 = "A" * (10 * 1024 * 1024)
        await websocket.send(
            json.dumps(
                {
                    "echo": request["echo"],
                    "status": "ok",
                    "retcode": 0,
                    "data": {"base64": large_base64},
                }
            )
        )
        assert (await asyncio.wait_for(operation, 3))["base64"] == large_base64
        # The same expansion is never granted to unsolicited chat events.
        await websocket.send(
            json.dumps({"post_type": "message", "self_id": 10000, "message": large_base64})
        )
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(websocket.recv(), 3)
    assert not app.state.onebot.ready.is_set()
    assert not gateway.calls
    assert not await app.state.service.store.all("SELECT * FROM ledger")
