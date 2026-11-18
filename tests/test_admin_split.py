"""Exercise the released Nginx config against a live engine with a paused model reply."""

import asyncio
import json
import os
import shutil
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import uvicorn
from pydantic import SecretStr
from websockets.asyncio.client import connect

from admin.csp import content_security_policy
from hakimi import admin_routes
from hakimi.app import create_app
from tests.fakes import FakeGateway

ASSETS = Path(__file__).resolve().parents[1] / "admin"


@asynccontextmanager
async def frontend(directory, port, engine_port, html):
    # The production image uses Docker DNS. Both test processes bind to loopback.
    config = (ASSETS / "nginx.conf").read_text()
    config = (
        config.replace("listen 8080;", f"listen 127.0.0.1:{port};")
        .replace("hakimi-bot:12141 resolve", f"127.0.0.1:{engine_port}")
        .replace("/tmp/", str(directory) + "/")
        .replace("/srv/admin", str(directory))
        .replace("/etc/nginx/admin-csp.conf", str(directory / "csp.conf"))
    )
    if os.getuid() == 0:
        config = "user root;\n" + config  # pytest's private directory belongs to the test user.
    (directory / "nginx.conf").write_text(config)
    (directory / "admin_page.html").write_text(html)
    (directory / "csp.conf").write_text(
        f'add_header Content-Security-Policy "{content_security_policy(html)}" always;'
    )
    proc = await asyncio.create_subprocess_exec(
        "nginx",
        "-c",
        str(directory / "nginx.conf"),
        "-g",
        "daemon off;",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False) as ui:
            async with asyncio.timeout(8):
                while True:
                    if proc.returncode is not None:
                        pytest.fail((await proc.stderr.read()).decode())
                    try:
                        if (await ui.get("/healthz")).is_success:
                            break
                    except httpx.ConnectError:
                        pass
                    await asyncio.sleep(0.03)
            yield ui
    finally:
        if proc.returncode is None:
            proc.terminate()
        await proc.wait()


async def test_ui_release_preserves_inflight_onebot_reply(settings, tmp_path, monkeypatch):
    if not shutil.which("nginx"):
        pytest.skip("The split admin integration test uses nginx")
    waiting, release = asyncio.Event(), asyncio.Event()

    class PausedModel(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            waiting.set()
            await asyncio.wait_for(release.wait(), 15)
            await super().generate(payload, subject, **kwargs)
            return "这条回复已完成"

    settings.onebot_mode = "ws_reverse"
    settings.onebot_ws_token = SecretStr("offline-onebot-token-with-at-least-32-bytes")
    settings.admin_token = SecretStr("private-admin-integration-token")
    settings.proactive_dm_enabled = False
    app = create_app(settings, gateway_factory=PausedModel)
    with socket.socket() as backend, socket.socket() as port_probe:
        backend.bind(("127.0.0.1", 0))
        engine_port = backend.getsockname()[1]
        port_probe.bind(("127.0.0.1", 0))
        port = port_probe.getsockname()[1]
        port_probe.close()
        ui_url = f"http://127.0.0.1:{port}/admin"
        monkeypatch.setattr(admin_routes, "ADMIN_PAGE_URL", ui_url)
        server = uvicorn.Server(uvicorn.Config(app, log_level="critical", access_log=False))
        task = asyncio.create_task(server.serve(sockets=[backend]))
        try:
            async with asyncio.timeout(8):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.02)
            await app.state.service.life_engine.set_config({"enabled": False})
            ws_auth = {
                "Authorization": "Bearer " + settings.onebot_ws_token.get_secret_value(),
                "X-Self-ID": "10000",
                "X-Client-Role": "Universal",
            }
            original = (ASSETS / "admin_page.html").read_text()
            previous = original.replace("<style>", "<style>\n/* Previous UI */\n", 1)
            auth = {"Authorization": "Bearer " + settings.admin_token.get_secret_value()}
            async with connect(
                f"ws://127.0.0.1:{engine_port}/ws/onebot", additional_headers=ws_auth, proxy=None
            ) as ws:
                login = json.loads(await asyncio.wait_for(ws.recv(), 6))
                assert login["action"] == "get_login_info"
                await ws.send(
                    json.dumps(
                        {
                            "echo": login["echo"],
                            "status": "ok",
                            "retcode": 0,
                            "data": {"user_id": 10000},
                        }
                    )
                )
                async with frontend(tmp_path, port, engine_port, previous) as ui:
                    page = await ui.get("/admin")
                    assert page.status_code == 200 and page.text == previous
                    assert page.headers["content-security-policy"] == content_security_policy(
                        previous
                    )
                    assert (await ui.get("/admin/api/dashboard")).status_code == 401
                    assert (
                        await ui.get("/admin", headers={"Host": "public.invalid"})
                    ).status_code == 403
                    assert (
                        await ui.get("/admin", headers={"Origin": "https://foreign.invalid"})
                    ).status_code == 403
                    assert (await ui.get("/admin?token=invalid")).status_code == 400
                    assert (await ui.get("/_admin_access")).status_code == 404
                    assert (await ui.get("/ws/onebot")).status_code == 404
                    assert (await ui.get("/admin/api/dashboard", headers=auth)).is_success
                    settings.admin_token = SecretStr("")
                    assert (await ui.get("/admin/")).status_code == 403
                    assert (await ui.get("/admin/api/dashboard", headers=auth)).status_code == 403
                    settings.admin_token = SecretStr(auth["Authorization"][7:])
                    await ws.send(
                        json.dumps(
                            {
                                "post_type": "message",
                                "message_type": "private",
                                "self_id": 10000,
                                "user_id": 30000,
                                "message_id": 123,
                                "time": int(time.time()),
                                "message": [
                                    {"type": "text", "data": {"text": "哈基米，今天心情好吗"}}
                                ],
                            }
                        )
                    )
                    await asyncio.wait_for(waiting.wait(), 8)
                async with frontend(tmp_path, port, engine_port, original) as ui:
                    assert (await ui.get("/admin/")).text == original
                    async with httpx.AsyncClient(trust_env=False) as old_tunnel:
                        page = await old_tunnel.get(f"http://127.0.0.1:{engine_port}/admin")
                    assert page.text == original
                    assert page.headers["content-security-policy"] == content_security_policy(
                        original
                    )
                    release.set()
                    async with asyncio.timeout(8):
                        while True:
                            reply = json.loads(await ws.recv())
                            if reply["action"] == "send_private_msg":
                                break
                            assert reply["action"] in {"set_input_status", "get_status"}
                            await ws.send(
                                json.dumps(
                                    {
                                        "echo": reply["echo"],
                                        "retcode": 0,
                                        "status": "ok",
                                        "data": {"online": True, "good": True},
                                    }
                                )
                            )
                    assert reply["params"]["user_id"] == 30000
                    assert "这条回复已完成" in reply["params"]["message"][0]["data"]["text"]
                    await ws.send(
                        json.dumps(
                            {
                                "echo": reply["echo"],
                                "retcode": 0,
                                "status": "ok",
                                "data": {"message_id": 456},
                            }
                        )
                    )
        finally:
            release.set()
            server.should_exit = True
            await task
