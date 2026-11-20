"""End-to-end Phase 2 integration tests with external_plugins={"weather", "minecraft"}."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import re
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import uvicorn
import websockets
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.gemini import GeminiGateway
from hakimi.models import Message
from hakimi.runtime_state import CODES
from plugins.minecraft.plugin_main import MinecraftPlugin
from plugins.weather.plugin_main import WeatherPlugin
from tests.fakes import FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair

NODE_PATH = "/ws/node/0123456789abcdef0123456789abcdef0123456789abcdef"
NODE_TOKEN = "node-phase2-integration-secret-token-32chars"


async def _wait_until(predicate: Any, *, max_wait: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + max_wait
    while True:
        res = predicate()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not met within max_wait")
        await asyncio.sleep(0.02)


def _base_settings(settings: Settings, **update: Any) -> Settings:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "external_plugins": {"weather", "minecraft"},
            **update,
        }
    )


@contextlib.asynccontextmanager
async def _running_app(app: Any) -> AsyncIterator[tuple[int, httpx.AsyncClient]]:
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        if server_task.done():
            server_task.result()
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            trust_env=False,
        ) as client:
            yield port, client
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server_task, timeout=5.0)


async def test_external_plugins_boot_and_env_aliases(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("HAKIMI_GEMINI_API_KEY", "test-gemini-key-never-live")
    monkeypatch.setenv("HAKIMI_ENCRYPTION_KEY", settings.encryption_key.get_secret_value())
    monkeypatch.setenv("HAKIMI_DATA_DIR", str(tmp_path / "env_data"))
    monkeypatch.setenv("EXTERNAL_PLUGINS", '["weather", "minecraft"]')
    loaded = Settings(_env_file=None)
    assert loaded.external_plugins == {"weather", "minecraft"}

    monkeypatch.delenv("EXTERNAL_PLUGINS", raising=False)
    monkeypatch.setenv("HAKIMI_EXTERNAL_PLUGINS", '["weather"]')
    loaded_prefixed = Settings(_env_file=None)
    assert loaded_prefixed.external_plugins == {"weather"}

    monkeypatch.setenv("HAKIMI_EXTERNAL_PLUGINS", "weather,minecraft")
    loaded_csv = Settings(_env_file=None)
    assert loaded_csv.external_plugins == {"weather", "minecraft"}

    monkeypatch.setenv("HAKIMI_EXTERNAL_PLUGINS", "")
    loaded_empty = Settings(_env_file=None)
    assert loaded_empty.external_plugins == set()

    app = create_app(_base_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert svc.modules.get("weather") is None
        assert svc.modules.get("minecraft") is None
        assert svc.weather is None
        assert "weather.read" not in svc.modules.catalog.actions
        assert "mc.build_plan" not in svc.modules.catalog.actions
        assert svc.modules.get("qq_social") is not None
        assert svc.modules.get("bilibili_surf") is not None

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            assert (await client.get("/admin/api/life/modules/weather")).status_code == 404
            assert (await client.get("/admin/api/life/modules/minecraft")).status_code == 404

            for method, path, body in (
                ("GET", "/admin/api/life/modules/weather/current", None),
                ("POST", "/admin/api/life/modules/weather/location/cities", {"query": "首尔"}),
                ("POST", "/admin/api/life/modules/weather/location/detect", None),
                (
                    "PUT",
                    "/admin/api/life/modules/weather/location",
                    {"latitude": 37.56, "longitude": 126.98},
                ),
                ("DELETE", "/admin/api/life/modules/weather/location", None),
            ):
                res = await client.request(method, path, json=body)
                assert res.status_code == 503
                assert res.json()["detail"] == "weather_module_unavailable"

            mc_res = await client.post(
                "/admin/api/life/mc/goal",
                json={"action_type": "status", "view": "module"},
            )
            assert mc_res.status_code == 503
            assert mc_res.json()["detail"] == "minecraft_module_unavailable"


async def test_weather_hotplug_via_real_ws_plugin_admin_and_chat_query(
    settings: Settings, message: Message, sdk_pair: Any
) -> None:
    obs_time = 1_800_000_000
    place = {"latitude": 37.56, "longitude": 126.98}
    decisions: list[dict[str, Any]] = []
    generations: list[dict[str, Any]] = []
    weather_calls: list[httpx.Request] = []
    fail_city_lookup = False

    def gemini(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content)
        if req.url.path.endswith("decisions"):
            result = decision_response()
            if "module_query" in body["questions"]:
                decisions.append(body["questions"]["module_query"])
                result["answers"]["module_query"] = {
                    "type": "choice",
                    "choice": "weather.read",
                    "confidence": 0.99,
                    "probabilities": {"weather.read": 0.99},
                }
                result["answers"]["need_external_lookup"]["noul"] = 0.95
            return httpx.Response(200, json=result)
        generations.append(body)
        return httpx.Response(200, json=interaction_response())

    def meteo(req: httpx.Request) -> httpx.Response:
        weather_calls.append(req)
        if req.url.host == "geocoding-api.open-meteo.com":
            if fail_city_lookup:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "results": [
                        {**place, "name": "首尔", "admin1": "首尔", "country": "韩国"},
                    ]
                },
            )
        if req.url.host == "ipwho.is":
            return httpx.Response(
                200,
                json={
                    **place,
                    "city": "Seoul",
                    "country": "South Korea",
                    "success": True,
                },
            )
        assert req.url.host == "api.open-meteo.com"
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": obs_time,
                    "interval": 900,
                    "temperature_2m": 19.75,
                    "weather_code": 61,
                }
            },
        )

    app = create_app(
        _base_settings(settings),
        gateway_factory=lambda s, st: GeminiGateway(s, st, *sdk_pair(gemini)),
        start_workers=False,
    )
    async with _running_app(app) as (port, client):
        svc = app.state.service
        svc.sender = FakeSender()
        svc.life_engine.clock = lambda: obs_time

        created = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "weather",
                "display_name": "天气观测",
                "scopes": ["actions", "sensors"],
                "node_region": "KR",
            },
        )
        assert created.status_code == 200
        token = created.json()["token"]

        async with httpx.AsyncClient(transport=httpx.MockTransport(meteo)) as upstream:
            plugin = WeatherPlugin(
                ws_url=f"ws://127.0.0.1:{port}/ws/plugin",
                token=token,
                http=upstream,
                clock=lambda: obs_time,
                reconnect_min=0.05,
                reconnect_max=0.2,
            )
            runner = asyncio.create_task(plugin.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        svc.modules.get("weather") is not None
                        and svc.modules.get("weather").configuration_issue
                        == "weather_location_missing"
                    )
                )

                detail_res = await client.get("/admin/api/life/modules/weather")
                assert detail_res.status_code == 200
                detail = detail_res.json()
                assert detail["panel"] == "weather"
                assert detail["configuration_issue"] == "weather_location_missing"
                assert detail["status"]["online"] is True
                assert [a["name"] for a in detail["actions"]] == ["weather.read"]

                before_loc = await client.get("/admin/api/life/modules/weather/current")
                assert before_loc.status_code == 409
                assert before_loc.json()["detail"] == "weather_location_missing"

                cities_res = await client.post(
                    "/admin/api/life/modules/weather/location/cities",
                    json={"query": " 首尔 "},
                )
                assert cities_res.status_code == 200
                candidate = {**place, "label": "首尔 · 韩国", "source": "city"}
                assert cities_res.json() == {"places": [candidate]}

                fail_city_lookup = True
                failed_cities = await client.post(
                    "/admin/api/life/modules/weather/location/cities",
                    json={"query": "首尔"},
                )
                assert failed_cities.status_code == 503
                assert failed_cities.json()["detail"] == "weather_city_lookup_unavailable"
                fail_city_lookup = False

                detect_res = await client.post("/admin/api/life/modules/weather/location/detect")
                assert detect_res.status_code == 200
                assert detect_res.json() == {
                    **place,
                    "label": "Seoul · South Korea",
                    "source": "server_ip",
                }

                saved_res = await client.put(
                    "/admin/api/life/modules/weather/location",
                    json=candidate,
                )
                assert saved_res.status_code == 200
                saved = saved_res.json()
                assert saved["configuration_issue"] is None
                assert saved["status"]["location"] == candidate
                assert saved["actions"][0]["available"] is True

                cur_res = await client.get("/admin/api/life/modules/weather/current")
                assert cur_res.status_code == 200
                cur = cur_res.json()
                assert cur["observation"]["temperature_2m"] == 19.75
                assert "小雨" in cur["summary"]

                msg = message.model_copy(update={"text": "你那边现在要带伞吗？"})
                assert await svc.accept(msg) == "accepted"
                await svc.process(await svc.store.claim())
                assert await svc.deliver_once() and len(svc.sender.sent) == 1
                assert len(decisions) == 1
                assert set(decisions[0]["criteria"]) == {"none", "weather.read"}
                model_input = json.dumps(generations[0]["input"], ensure_ascii=False)
                assert "module_query_result" in model_input
                assert "19.75" in model_input and "小雨" in model_input
                assert "126.98" not in model_input

                del_res = await client.delete("/admin/api/life/modules/weather/location")
                assert del_res.status_code == 200
                deleted = del_res.json()
                assert deleted["configuration_issue"] == "weather_location_missing"
                assert deleted["status"]["location"] is None
            finally:
                await plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(runner, timeout=3.0)

        await _wait_until(lambda: svc.modules.get("weather") is None)
        assert svc.weather is None
        assert "weather.read" not in svc.modules.catalog.actions
        assert "remote:weather" not in svc.kernel.fibers()
        assert (await client.get("/admin/api/life/modules/weather")).status_code == 404
        assert (await client.get("/admin/api/life/modules/weather/current")).status_code == 503


async def test_minecraft_hotplug_via_real_ws_plugin_node_bridge_and_world_reaction(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="hakimi")
    s = _base_settings(
        settings,
        node_ws_enabled=True,
        node_ws_token=SecretStr(NODE_TOKEN),
        node_ws_path=NODE_PATH,
        node_id="shanghai-cn-1",
    )
    app = create_app(s, gateway_factory=FakeGateway, start_workers=False)

    node_rpcs: list[dict[str, Any]] = []
    node_ws_holder: list[Any] = []
    node_stop = asyncio.Event()

    async def _run_fake_cn_node(ws_url: str) -> None:
        headers = {
            "Authorization": f"Bearer {NODE_TOKEN}",
            "X-Node-Id": "shanghai-cn-1",
            "X-Node-Region": "CN",
        }
        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            node_ws_holder.append(ws)
            await ws.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": "node.hello",
                        "node_id": "shanghai-cn-1",
                        "region": "CN",
                        "payload": {"capabilities": ["mc.status", "mc.dispatch_goal"]},
                    }
                )
            )
            while not node_stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.1)
                except TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
                frame = json.loads(raw)
                if frame.get("type") != "rpc_request":
                    continue
                action = frame.get("action")
                req_id = frame.get("id")
                params = frame.get("params") or {}
                node_rpcs.append({"action": action, "params": params})
                if action == "node.ping":
                    data: dict[str, Any] = {"node_id": "shanghai-cn-1", "region": "CN"}
                elif action == "mc.status":
                    data = {
                        "bridge_ready": True,
                        "mc_enabled": True,
                        "mc_configured": True,
                        "paused": False,
                        "snapshot": {
                            "connected": True,
                            "dimension": "overworld",
                            "pos": [10.0, 64.0, -5.0],
                            "health": 20.0,
                            "food": 18.0,
                            "activity": "巡逻营地",
                            "updated_at": time.time(),
                        },
                    }
                elif action == "mc.dispatch_goal":
                    label = params.get("topic") or params.get("action_type")
                    data = {
                        "ok": True,
                        "accepted": True,
                        "phase": "queued",
                        "summary": f"已登记目标: {label}",
                    }
                else:
                    data = {"ok": True, "accepted": True}
                await ws.send(
                    json.dumps({"type": "rpc_response", "id": req_id, "ok": True, "data": data})
                )

    async with _running_app(app) as (port, client):
        svc = app.state.service
        created = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "minecraft",
                "display_name": "Minecraft",
                "scopes": ["actions", "sensors", "node_bridge"],
                "node_region": "CN",
            },
        )
        assert created.status_code == 200
        token = created.json()["token"]

        node_task = asyncio.create_task(_run_fake_cn_node(f"ws://127.0.0.1:{port}{NODE_PATH}"))
        await asyncio.wait_for(svc.node_transport.ready.wait(), timeout=5.0)

        plugin = MinecraftPlugin(
            ws_url=f"ws://127.0.0.1:{port}/ws/plugin",
            token=token,
            reconnect_min=0.05,
            reconnect_max=0.2,
        )
        plugin_task = asyncio.create_task(plugin.client.run_forever())
        try:
            await _wait_until(
                lambda: (
                    (mod := svc.modules.get("minecraft")) is not None
                    and mod.status_dict().get("bridge_ready") is True
                    and mod.status_dict().get("connected") is True
                )
            )

            detail_res = await client.get("/admin/api/life/modules/minecraft")
            assert detail_res.status_code == 200
            detail = detail_res.json()
            assert detail["panel"] == "minecraft"
            assert detail["status"]["bridge_ready"] is True
            assert detail["status"]["connected"] is True
            assert detail["status"]["dimension"] == "overworld"
            assert len(detail["actions"]) == 14
            assert "mc.chat" in {a["name"] for a in detail["actions"]}

            goal_res = await client.post(
                "/admin/api/life/mc/goal",
                json={"action_type": "build_plan", "topic": "红石自动门", "view": "module"},
            )
            assert goal_res.status_code == 200
            goal_body = goal_res.json()
            assert goal_body["status"] == "ok"
            assert goal_body["result"]["ok"] is True
            assert goal_body["mc_status"]["bridge_ready"] is True
            assert any(
                call["action"] == "mc.dispatch_goal" and call["params"].get("topic") == "红石自动门"
                for call in node_rpcs
            )

            # Node event relay: mc.snapshot -> node_transport.on_event -> plugin -> HIGH focus
            node_ws = node_ws_holder[0]
            snap_ts = time.time()
            await node_ws.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": "mc.snapshot",
                        "node_id": "shanghai-cn-1",
                        "region": "CN",
                        "payload": {
                            "connected": True,
                            "bridge_ready": True,
                            "mc_enabled": True,
                            "mc_configured": True,
                            "dimension": "the_nether",
                            "pos": [42.0, 70.0, -8.0],
                            "health": 19.0,
                            "food": 17.0,
                            "activity": "搭建下界传送门",
                            "active_task": "explore",
                            "updated_at": snap_ts,
                        },
                    }
                )
            )
            await _wait_until(
                lambda: (
                    (mod := svc.modules.get("minecraft")) is not None
                    and mod.status_dict().get("dimension") == "the_nether"
                    and svc.world_state.current.current_module == "minecraft"
                    and svc.world_state.current.focus_level == "HIGH"
                )
            )

            life_json = (await client.get("/admin/api/life")).json()
            assert life_json["nodes"]["shanghai_mc_agent"]["dimension"] == "the_nether"
            assert life_json["world_state"]["current_module"] == "minecraft"
            assert life_json["world_state"]["focus_level"] == "HIGH"

            # Node event relay: mc.milestone -> plugin emits world.event -> global_events
            await node_ws.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": "mc.milestone",
                        "node_id": "shanghai-cn-1",
                        "region": "CN",
                        "payload": {
                            "event_type": "mc_rare_find",
                            "importance": "high",
                            "event_id": "ms-nether-1",
                            "block": "ancient_debris",
                        },
                    }
                )
            )
            await _wait_until(lambda: svc.world_state.latest_event("minecraft", "mc_rare_find"))

            # World broadcast: private_first skipped; public bili_video_watched -> mc.build_plan
            await svc.world_state.publish_event(
                "bilibili_surf",
                "bili_video_watched",
                {"title": "我的世界秘密基地", "private_first": True},
            )
            await asyncio.sleep(0.1)
            priv_rows = await svc.store.all(
                "SELECT id FROM scheduled_intents WHERE action_name='mc.build_plan'"
            )
            assert priv_rows == []

            pub_event = await svc.world_state.publish_event(
                "bilibili_surf",
                "bili_video_watched",
                {"title": "我的世界红石自动门教程"},
            )
            await _wait_until(
                lambda: svc.store.one(
                    "SELECT action_name, origin, source_module, source_event_id, arguments_json "
                    "FROM scheduled_intents WHERE action_name='mc.build_plan' "
                    "AND source_event_id=?",
                    (pub_event.id,),
                )
            )
            intent_row = await svc.store.one(
                "SELECT action_name, origin, source_module, source_event_id, arguments_json "
                "FROM scheduled_intents WHERE action_name='mc.build_plan' "
                "AND source_event_id=?",
                (pub_event.id,),
            )
            assert intent_row is not None
            assert intent_row["origin"] == "rule"
            assert intent_row["source_module"] == "minecraft"
            assert "红石自动门" in json.loads(intent_row["arguments_json"])["topic"]
            assert {
                "plugin_node_bridge_rejected",
                "plugin_state_error",
                "plugin_reaction_enqueued",
                "plugin_reaction_rejected",
            } <= CODES
            assert "event=plugin_reaction_enqueued" in caplog.text

            # Node disconnect triggers on_state(False) -> plugin clears bridge_ready & HIGH focus
            node_stop.set()
            await node_ws.close()
            await _wait_until(
                lambda: (
                    (mod := svc.modules.get("minecraft")) is not None
                    and mod.status_dict().get("bridge_ready") is False
                    and svc.world_state.current.focus_level == "LOW"
                )
            )
        finally:
            node_stop.set()
            await plugin.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(plugin_task, timeout=3.0)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(node_task, timeout=3.0)

        await _wait_until(lambda: svc.modules.get("minecraft") is None)
        assert "mc.build_plan" not in svc.modules.catalog.actions
        assert "remote:minecraft" not in svc.kernel.fibers()
        assert (await client.get("/admin/api/life/modules/minecraft")).status_code == 404
        assert (
            await client.post(
                "/admin/api/life/mc/goal",
                json={"action_type": "status", "view": "module"},
            )
        ).status_code == 503
        assert (
            await svc.node_transport.on_event({"event": "mc.snapshot", "payload": {}}) == "ignored"
        )


async def test_admin_html_reorganization_via_admin_route(
    admin_frontend: Any, settings: Settings
) -> None:
    app = create_app(_base_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
        ) as client:
            res = await client.get("/admin")
            assert res.status_code == 200
            html = res.text

    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(styles) == 1
    assert len(scripts) == 1
    style_hash = hashlib.sha256(styles[0].encode()).digest()
    script_hash = hashlib.sha256(scripts[0].encode()).digest()
    csp = res.headers["content-security-policy"]
    assert f"'sha256-{base64.b64encode(style_hash).decode()}'" in csp
    assert f"'sha256-{base64.b64encode(script_hash).decode()}'" in csp
    assert "'unsafe-inline'" not in csp

    core_nav = re.search(r'<nav id="core-nav"[^>]*>(.*?)</nav>', html, re.S)
    modules_nav = re.search(r'<nav id="modules-nav"[^>]*>(.*?)</nav>', html, re.S)
    assert core_nav is not None and modules_nav is not None
    for marker in (
        'id="nav-identity-center"',
        'id="nav-community"',
        'id="nav-memes"',
        'id="nav-proactive"',
    ):
        assert marker in core_nav.group(1)
    assert 'id="nav-plugin-bus"' in modules_nav.group(1)

    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    assert 'id="sign-form"' in panels["qq"]
    assert 'id="sign-form"' not in panels["community"]
