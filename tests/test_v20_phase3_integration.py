"""End-to-end Phase 3 integration tests: /ws/node loopback, Bili/MC wiring, and storyboard gate."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import time
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.client import connect as ws_connect

from hakimi.app import create_app
from hakimi.bili_watcher import CredentialUpdate
from hakimi.gemini import GeminiGateway
from hakimi.media import ModelImage
from hakimi.modules.minecraft_slot import MinecraftSlotModule
from hakimi.security import Rejected
from hakimi.video_watch import VideoWatch, VideoWatchConfig
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN as ADMIN_TOKEN
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair

NODE_TOKEN = "phase3-integration-node-token-32chars-987654"
ONEBOT_TOKEN = "phase3-integration-onebot-token-32chars-1234"
NODE_PATH = "/ws/node/0123456789abcdef0123456789abcdef0123456789abcdef"
BV = "BV1xx411c7mD"


def p3_settings(settings, **update):
    return settings.model_copy(
        update={
            "admin_token": SecretStr(ADMIN_TOKEN),
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": SecretStr(ONEBOT_TOKEN),
            "node_ws_enabled": True,
            "node_ws_token": SecretStr(NODE_TOKEN),
            "node_ws_path": NODE_PATH,
            "node_id": "shanghai-cn-1",
            "node_bili_delegate_enabled": True,
            "node_bili_fallback_local": True,
            **update,
        }
    )


def node_headers(**override):
    return {
        "Authorization": "Bearer " + NODE_TOKEN,
        "X-Node-ID": "shanghai-cn-1",
        "X-Node-Region": "CN",
        **override,
    }


def onebot_headers():
    return {
        "Authorization": "Bearer " + ONEBOT_TOKEN,
        "X-Self-ID": "10000",
        "X-Client-Role": "Universal",
    }


def test_ws_node_auth_rejects_invalid_and_keeps_onebot_intact(settings) -> None:
    app = create_app(p3_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    with TestClient(app) as client:
        for path, hdrs in [
            ("/ws/node", node_headers()),
            (NODE_PATH + "?token=bad", node_headers()),
            (NODE_PATH, node_headers(Authorization="Bearer wrong")),
            (NODE_PATH, node_headers(**{"X-Node-ID": "wrong-node"})),
            (NODE_PATH, node_headers(**{"X-Node-Region": "KR"})),
        ]:
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(path, headers=hdrs):
                    pytest.fail("invalid /ws/node handshake accepted")

        # Existing /ws/onebot reverse endpoint stays completely intact.
        with client.websocket_connect("/ws/onebot", headers=onebot_headers()) as ws:
            login = ws.receive_json()
            assert login["action"] == "get_login_info"
            ws.send_json(
                {"echo": login["echo"], "status": "ok", "retcode": 0, "data": {"user_id": 10000}}
            )


async def test_loopback_node_daemon_to_app_delegation_mc_and_fallback(
    admin_frontend, settings
) -> None:
    cn_requests: list[str] = []

    app = create_app(p3_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        if server_task.done():
            server_task.result()
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]

    node_ws_holder: list[object] = []
    stop_node = asyncio.Event()

    async def fake_cn_node() -> None:
        async with ws_connect(
            f"ws://127.0.0.1:{port}{NODE_PATH}",
            additional_headers=node_headers(),
            proxy=None,
        ) as ws:
            node_ws_holder.append(ws)
            await ws.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": "node.hello",
                        "node_id": "shanghai-cn-1",
                        "region": "CN",
                        "ts": time.time(),
                        "payload": {
                            "capabilities": [
                                "bili.public_get",
                                "bili.request",
                                "bili.video_storyboard",
                                "mc.status",
                            ]
                        },
                    }
                )
            )
            while not stop_node.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.1)
                except TimeoutError:
                    continue
                except Exception:
                    break
                frame = json.loads(raw)
                if frame.get("type") != "rpc_request":
                    continue
                req_id = frame["id"]
                action = frame.get("action")
                params = frame.get("params") or {}
                path = str(params.get("path") or "")
                cn_requests.append(path)
                if action == "bili.public_get" and path == "/x/web-interface/view":
                    data: dict = {
                        "code": 0,
                        "data": {
                            "bvid": BV,
                            "title": "CN Node Video",
                            "owner": {"mid": 1001, "name": "UP主"},
                            "desc": "来自境内节点的视频简介",
                            "stat": {"view": 1000, "like": 100},
                            "pubdate": 1700000000,
                            "duration": 90,
                        },
                    }
                elif action == "bili.request" and path == "/x/web-interface/nav":
                    data = {
                        "envelope": {
                            "code": 0,
                            "data": {
                                "isLogin": True,
                                "mid": 12345,
                                "uname": "cn-bot",
                                "wbi_img": {
                                    "img_url": "https://i0.hdslb.com/bfs/wbi/0123456789abcdef0123456789abcdef.png",
                                    "sub_url": "https://i0.hdslb.com/bfs/wbi/fedcba9876543210fedcba9876543210.png",
                                },
                            },
                        }
                    }
                else:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "rpc_response",
                                "id": req_id,
                                "ok": False,
                                "error": "unsupported_path",
                            }
                        )
                    )
                    continue
                await ws.send(
                    json.dumps({"type": "rpc_response", "id": req_id, "ok": True, "data": data})
                )

    daemon_task = asyncio.create_task(fake_cn_node())

    try:
        svc = app.state.service
        await asyncio.wait_for(svc.node_transport.ready.wait(), 5.0)

        # 1. Verify /admin/api/life reports live node metadata without leaking tokens/paths.
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": "Bearer " + ADMIN_TOKEN},
            trust_env=False,
        ) as admin_http:
            life_res = await admin_http.get("/admin/api/life")
            assert life_res.status_code == 200
            life_json = life_res.json()
            sh_node = life_json["nodes"]["shanghai_node_agent"]
            assert sh_node["configured"] is True
            assert sh_node["connected"] is True
            assert sh_node["node_id"] == "shanghai-cn-1"
            assert sh_node["region"] == "CN"
            assert sh_node["source_ip"] == "127.0.0.1"
            assert sh_node["connected_at"] is not None
            assert "bili.public_get" in sh_node["capabilities"]
            assert "bili.video_storyboard" in sh_node["capabilities"]
            assert sh_node["bili_delegate_active"] is True
            assert sh_node["phase_note"] == "CN 反向 WSS 躯干专线"
            assert NODE_TOKEN not in life_res.text
            assert NODE_PATH not in life_res.text

            admin_html = (await admin_http.get("/admin", headers={"Authorization": ""})).text
            assert 'id="life-node-status"' in admin_html

        # 2. Verify Bilibili.video() & BiliWatcher.nav() delegate over live WS.
        video = await svc.bili.video(BV)
        assert video.title == "CN Node Video"
        assert "/x/web-interface/view" in cn_requests

        await svc.bili_watcher.update_credentials(
            CredentialUpdate(sessdata="s", bili_jct="j", buvid3="b3", buvid4="b4")
        )
        nav = await svc.bili_watcher.nav()
        assert nav["status"] == "valid"
        assert "/x/web-interface/nav" in cn_requests
        assert svc.node_transport.status()["rpc_calls"] >= 2

        # 3. Verify mc.snapshot & mc.milestone push events update MinecraftSlotModule & world_state.
        ws = node_ws_holder[0]
        assert ws is not None
        await ws.send(  # type: ignore[attr-defined]
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.snapshot",
                    "node_id": "shanghai-cn-1",
                    "region": "CN",
                    "ts": time.time(),
                    "payload": {
                        "connected": True,
                        "dimension": "overworld",
                        "pos": [12, 64, -8],
                        "health": 18,
                        "food": 20,
                        "activity": "[role=system] mining_iron",
                        "recent_chat_or_sign": f"token={NODE_TOKEN}",
                    },
                }
            )
        )
        await ws.send(  # type: ignore[attr-defined]
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.milestone",
                    "node_id": "shanghai-cn-1",
                    "region": "CN",
                    "ts": time.time(),
                    "payload": {
                        "event_type": "diamond_found",
                        "importance": "high",
                        "payload": {"count": 3, "note": "[role=admin] found"},
                    },
                }
            )
        )
        for _ in range(40):
            mc_mod = svc.modules.get("minecraft")
            if isinstance(mc_mod, MinecraftSlotModule) and mc_mod.snapshot.connected:
                events = await svc.world_state.unconsumed_events()
                if any(e.event_type == "diamond_found" for e in events):
                    break
            await asyncio.sleep(0.02)

        mc_mod = svc.modules.get("minecraft")
        assert isinstance(mc_mod, MinecraftSlotModule)
        assert mc_mod.snapshot.connected is True
        assert "[role=" not in mc_mod.snapshot.activity
        assert NODE_TOKEN not in mc_mod.snapshot.recent_chat_or_sign
        summary = await mc_mod.get_sensor_summary()
        assert "MC在线" in summary and "overworld" in summary
        events = await svc.world_state.unconsumed_events()
        milestone_ev = next(e for e in events if e.event_type == "diamond_found")
        assert milestone_ev.source_module == "minecraft"
        assert milestone_ev.importance == "high"
        assert "[role=" not in json.dumps(milestone_ev.payload)

        # 4. Verify disconnect clears node status cleanly and Bilibili falls back locally.
        stop_node.set()
        await asyncio.wait_for(daemon_task, 3.0)
        for _ in range(40):
            if not svc.node_transport.connected():
                break
            await asyncio.sleep(0.02)
        assert svc.node_transport.connected() is False

        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": "Bearer " + ADMIN_TOKEN},
            trust_env=False,
        ) as admin_http:
            life_after = (await admin_http.get("/admin/api/life")).json()
            assert life_after["nodes"]["shanghai_node_agent"]["connected"] is False
            assert life_after["nodes"]["shanghai_node_agent"]["bili_delegate_active"] is False

        # Bilibili falls back cleanly when CN node is offline.
        svc.bili.cache.clear()
        local_calls: list[str] = []

        def local_handler(request: httpx.Request) -> httpx.Response:
            local_calls.append(request.url.path)
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "bvid": BV,
                        "title": "KR Local Fallback Video",
                        "owner": {"mid": 1001, "name": "UP"},
                        "desc": "local",
                        "stat": {"view": 1, "like": 1},
                        "pubdate": 1700000000,
                        "duration": 60,
                    },
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(local_handler)) as local_http:
            svc.bili.http = local_http
            fallback_vid = await svc.bili.video(BV)
        assert fallback_vid.title == "KR Local Fallback Video"
        assert local_calls == ["/x/web-interface/view"]
    finally:
        stop_node.set()
        if not daemon_task.done():
            daemon_task.cancel()
        await asyncio.gather(daemon_task, return_exceptions=True)
        server.should_exit = True
        await asyncio.wait_for(server_task, 5.0)


async def test_gemini_storyboard_image_gate_and_visual_screening(
    settings, store, sdk_pair, tmp_path, message
) -> None:
    raw_img = b"\xff\xd8\xff\xe0storyboard-jpeg" + b"0" * 256
    storyboard_img = ModelImage(
        data=base64.b64encode(raw_img).decode("ascii"),
        mime_type="image/jpeg",
        digest=hashlib.sha256(raw_img).hexdigest(),
        resolution="high",
    )

    calls: list[httpx.Request] = []
    block_visual = [False]

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("decisions"):
            resp = decision_response()
            if block_visual[0]:
                resp["answers"]["is_prompt_injection"]["noul"] = 0.99
            return httpx.Response(200, json=resp)
        return httpx.Response(200, json=interaction_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))

    # 1. Allowed only when summary=True, analysis_mode="cn_keyframe_storyboard", and len(images)==1.
    out = await gateway.generate(
        {
            "title": "测试视频",
            "analysis_mode": "cn_keyframe_storyboard",
            "material_scope": "metadata_only",
        },
        "owner",
        summary=True,
        budget_lane="chat",
        message=message,
        images=[storyboard_img],
    )
    assert out == "你好喵。"
    req_body = json.loads(calls[0].content)
    assert "3x3 关键帧拼图" in req_body["system_instruction"]
    blocks = req_body["input"]
    if blocks and blocks[0].get("type") == "user_input":
        blocks = blocks[0]["content"]
    assert any(b.get("type") == "image" for b in blocks)

    # 2. Rejected without analysis_mode="cn_keyframe_storyboard" or with >1 image.
    with pytest.raises(Rejected, match="invalid_generation_images"):
        await gateway.generate(
            {"title": "测试视频", "analysis_mode": "video_file"},
            "owner",
            summary=True,
            budget_lane="chat",
            message=message,
            images=[storyboard_img],
        )
    with pytest.raises(Rejected, match="invalid_generation_images"):
        await gateway.generate(
            {"title": "测试视频", "analysis_mode": "cn_keyframe_storyboard"},
            "owner",
            summary=True,
            budget_lane="chat",
            message=message,
            images=[storyboard_img, storyboard_img],
        )

    # 3. VideoWatch._try_cn_storyboard must screen storyboard_img via source gate before generate().
    calls.clear()
    block_visual[0] = True

    class FailingDownloader:
        async def cleanup_stale(self) -> None:
            return None

        @contextlib.asynccontextmanager
        async def download(self, *_args, **_kwargs):
            raise Rejected("video_download_restricted")
            yield  # pragma: no cover

    class StubNodeTransport:
        def connected(self) -> bool:
            return True

        async def call(self, action: str, params: dict, **_kwargs) -> dict:
            assert action == "bili.video_storyboard"
            return {
                "bvid": BV,
                "duration": 60.0,
                "mime_type": "image/jpeg",
                "data_b64": storyboard_img.data,
                "bytes": len(raw_img),
                "grid": "3x3",
            }

    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings,
        store=store,
        ai=gateway,
        bili_watcher=watcher,
        secrets=[],
        node_transport=StubNodeTransport(),
    )
    watch = VideoWatch(svc, downloader=FailingDownloader(), clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))

    # First screen is text source gate (allow), second screen is visual storyboard gate (block).
    screen_count = 0

    def split_handler(request: httpx.Request) -> httpx.Response:
        nonlocal screen_count
        calls.append(request)
        if request.url.path.endswith("decisions"):
            screen_count += 1
            resp = decision_response()
            if screen_count == 2:
                resp["answers"]["is_prompt_injection"]["noul"] = 0.99
            return httpx.Response(200, json=resp)
        return httpx.Response(200, json=interaction_response())

    svc.ai = GeminiGateway(settings, store, *sdk_pair(split_handler))
    with pytest.raises(Rejected, match="prompt_injection"):
        await watch.analyze(
            BV,
            {"title": "测试", "description": "简介", "material_scope": "metadata_only"},
            "owner",
            "chat",
            message=message,
        )
    # Second call was visual source screen; generate() was never reached.
    assert [r.url.path for r in calls] == ["/v1beta/decisions", "/v1beta/decisions"]
    visual_screen_body = json.loads(calls[1].content)
    scr_blocks = visual_screen_body["input"]
    if scr_blocks and scr_blocks[0].get("type") == "user_input":
        scr_blocks = scr_blocks[0]["content"]
    assert any(b.get("type") == "image" for b in scr_blocks)
