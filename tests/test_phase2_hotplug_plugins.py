"""Phase 2 end-to-end tests for standalone hot-plug weather and minecraft plugins."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.modules import platform_entries
from hakimi.modules.actions import ActionCall
from hakimi.plugin_bus import PluginBus, RemoteModuleProxy
from hakimi.plugin_kernel import PluginKernel
from plugins.minecraft.plugin_main import MinecraftPlugin, build_minecraft_plugin_from_env
from plugins.weather.plugin_main import (
    WeatherLocation,
    WeatherPlugin,
    build_weather_plugin_from_env,
)
from tests.test_admin import TOKEN
from tests.test_v20_p4_cross_module_mc import FakeNodeTransport

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 11, 17, 14, 0, tzinfo=TZ).timestamp()


class _PipeClientSide:
    """Client-side endpoint of an in-memory bidirectional WebSocket pipe."""

    def __init__(self, pipe: _BiPipe) -> None:
        self._pipe = pipe
        self.close_code: int | None = None

    async def send(self, raw: str) -> None:
        await self._pipe.c2s.put(raw)

    async def recv(self) -> str:
        item = await self._pipe.s2c.get()
        if item.startswith("__EOF__:"):
            code = int(item.split(":", 1)[1])
            self.close_code = code
            raise ConnectionError(f"closed:{code}")
        return item

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        await self._pipe.close_from_client(code)


class _BiPipe:
    """In-memory bidirectional WebSocket pair wiring HakimiPluginClient to PluginBus.serve."""

    def __init__(self, bus: PluginBus, plugin_id: str, *, source_ip: str = "127.0.0.1") -> None:
        self.bus = bus
        self.plugin_id = plugin_id
        self.source_ip = source_ip
        self.c2s: asyncio.Queue[str] = asyncio.Queue()
        self.s2c: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.server_task: asyncio.Task[None] | None = None

    async def server_recv(self) -> str:
        item = await self.c2s.get()
        if item.startswith("__EOF__:"):
            raise ConnectionError(item)
        return item

    async def server_send(self, raw: str) -> None:
        if not self.closed:
            await self.s2c.put(raw)

    async def server_close(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.s2c.put(f"__EOF__:{code}")
        await self.c2s.put(f"__EOF__:{code}")

    async def close_from_client(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.c2s.put(f"__EOF__:{code}")
        await self.s2c.put(f"__EOF__:{code}")

    def connect_factory(self, _url: str, _headers: dict[str, str]) -> _PipeClientSide:
        self.server_task = asyncio.create_task(
            self.bus.serve(
                self.server_recv,
                self.server_send,
                self.server_close,
                source_ip=self.source_ip,
                plugin_id=self.plugin_id,
            )
        )
        return _PipeClientSide(self)

    async def shutdown(self) -> None:
        await self.close_from_client(1000)
        if self.server_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(self.server_task, timeout=2.0)


async def _wait_until(predicate: Any, *, max_wait: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + max_wait
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not met within max_wait")
        await asyncio.sleep(0.01)  # noqa: ASYNC110


@pytest.fixture
async def phase2_env(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
        }
    )
    app = create_app(s, start_workers=False)
    async with app.router.lifespan_context(app):
        now_box = [NOW]
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: now_box[0]
        svc.life_engine.clock = lambda: now_box[0]
        bus = getattr(svc, "plugin_bus", None) or svc.modules.systems["plugin_bus"].plugin_bus
        bus.clock = lambda: now_box[0]
        await svc.world_state.initialize()
        yield app, svc, bus, now_box


def test_plugins_zero_hakimi_imports_and_env_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path(__file__).resolve().parents[1]
    plugins_dir = root / "plugins"
    py_files = sorted(plugins_dir.rglob("*.py"))
    assert len(py_files) >= 5

    allowed_third_party = {"hakimi_plugin_sdk", "httpx", "pydantic"}
    stdlib_names = getattr(sys, "stdlib_module_names", frozenset())

    for path in py_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".", 1)[0]
                    assert top != "hakimi", f"{path} imports hakimi"
                    assert top in stdlib_names or top in allowed_third_party, (
                        f"{path} imports disallowed module {top}"
                    )
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    continue
                mod = node.module or ""
                top = mod.split(".", 1)[0]
                assert top != "hakimi", f"{path} imports from hakimi"
                assert top in stdlib_names or top in allowed_third_party, (
                    f"{path} imports disallowed module {top}"
                )

    excluded = [p.name for p, _ in platform_entries(exclude={"weather", "minecraft"})]
    assert "weather" not in excluded
    assert "minecraft" not in excluded

    monkeypatch.setenv("HAKIMI_PLUGIN_WS_URL", "ws://127.0.0.1:12141/ws/plugin")
    monkeypatch.setenv("HAKIMI_PLUGIN_WEATHER_TOKEN", "w-tok")
    monkeypatch.setenv("HAKIMI_WEATHER_LATITUDE", "37.56")
    monkeypatch.setenv("HAKIMI_WEATHER_LONGITUDE", "126.98")
    monkeypatch.setenv("TZ", "Asia/Seoul")
    wp = build_weather_plugin_from_env()
    assert wp.client.config.plugin_id == "weather"
    assert wp.location is not None and wp.location.point == (37.56, 126.98)
    assert wp.timezone == "Asia/Seoul"

    monkeypatch.setenv("HAKIMI_PLUGIN_MINECRAFT_TOKEN", "m-tok")
    monkeypatch.setenv("MC_BRIDGE_URL", "http://127.0.0.1:8791")
    mp = build_minecraft_plugin_from_env()
    assert mp.client.config.plugin_id == "minecraft"
    assert mp.mc_bridge_url == "http://127.0.0.1:8791"


async def test_weather_plugin_e2e_location_crud_geocoding_forecast_and_reconnect(
    phase2_env,
) -> None:
    _, svc, bus, now_box = phase2_env
    assert svc.modules.unregister("weather") is True
    assert svc.modules.get("weather") is None
    await svc.life_engine.initialize()

    token, _ = await bus.create_plugin(
        "weather", "天气观测", ["actions", "sensors"], node_region="KR"
    )
    assert len(token) >= 43
    upstream_calls: list[httpx.Request] = []
    fail_geocoding = False
    fail_ip = False

    def weather_handler(req: httpx.Request) -> httpx.Response:
        upstream_calls.append(req)
        if req.url.host == "geocoding-api.open-meteo.com":
            assert req.url.path == "/v1/search"
            assert req.url.params["name"] == "首尔"
            assert req.url.params["language"] == "zh"
            assert req.url.params["count"] == "6"
            if fail_geocoding:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "name": "首尔",
                            "admin1": "首尔",
                            "country": "韩国",
                            "latitude": 37.56,
                            "longitude": 126.98,
                        },
                        {
                            "name": "非法纬度",
                            "latitude": 200,
                            "longitude": 10,
                        },
                    ]
                },
            )
        if req.url.host == "ipwho.is":
            assert req.url.params["fields"] == "success,city,region,country,latitude,longitude"
            if fail_ip:
                return httpx.Response(429)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "city": "Seoul",
                    "region": "Seoul",
                    "country": "South Korea",
                    "latitude": 37.56,
                    "longitude": 126.98,
                },
            )
        if req.url.host == "api.open-meteo.com":
            assert req.url.path == "/v1/forecast"
            assert req.url.params["timeformat"] == "unixtime"
            assert req.url.params["forecast_days"] == "1"
            assert (
                req.url.params["current"]
                == "temperature_2m,weather_code,precipitation,shortwave_radiation,is_day"
            )
            return httpx.Response(
                200,
                json={
                    "current": {
                        "time": int(NOW),
                        "interval": 900,
                        "temperature_2m": 18.5,
                        "weather_code": 61,
                        "precipitation": 0.6,
                        "shortwave_radiation": 120.0,
                        "is_day": 1,
                    }
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(weather_handler)) as http:
        pipe = _BiPipe(bus, "weather")
        plugin = WeatherPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token=token,
            timezone="Asia/Shanghai",
            http=http,
            clock=lambda: now_box[0],
            connect_factory=pipe.connect_factory,
            reconnect_min=0.05,
            reconnect_max=0.1,
        )
        await plugin.client.start()
        try:
            assert await plugin.client.wait_connected(timeout=2.0)
            await _wait_until(lambda: isinstance(svc.modules.get("weather"), RemoteModuleProxy))
            proxy = svc.modules.get("weather")
            assert isinstance(proxy, RemoteModuleProxy)
            await _wait_until(lambda: proxy.configuration_issue == "weather_location_missing")

            assert await proxy.get_sensor_summary() == "尚未设置观测地点"
            gated = await svc.life_engine.query("weather.read")
            assert gated.status == "gated_skip"
            assert gated.reason == "weather_location_missing"

            # Geocoding lookup
            cities = await proxy.call_custom_rpc("weather.location.cities", {"query": "首尔"})
            assert cities == {
                "places": [
                    {
                        "source": "city",
                        "label": "首尔 · 韩国",
                        "latitude": 37.56,
                        "longitude": 126.98,
                    }
                ]
            }
            fail_geocoding = True
            with pytest.raises(Exception, match="weather_city_lookup_unavailable"):
                await proxy.call_custom_rpc("weather.location.cities", {"query": "首尔"})
            fail_geocoding = False

            # IP detection
            detected = await proxy.call_custom_rpc("weather.location.detect", {})
            assert detected == {
                "source": "server_ip",
                "label": "Seoul · South Korea",
                "latitude": 37.56,
                "longitude": 126.98,
            }
            fail_ip = True
            with pytest.raises(Exception, match="weather_ip_lookup_unavailable"):
                await proxy.call_custom_rpc("weather.location.detect", {})
            fail_ip = False

            # Set location -> persisted in KV, clears configuration_issue
            set_res = await proxy.call_custom_rpc("weather.location.set", detected)
            assert set_res == {"location": detected}
            await _wait_until(lambda: proxy.configuration_issue is None)
            assert proxy.admin_status(None)["location"] == detected
            assert await proxy.get_sensor_summary() == "等待首次天气查询"

            got_loc = await proxy.call_custom_rpc("weather.location.get", {})
            assert got_loc == {"location": detected}

            # Read forecast -> records observation, emits weather_observed, caches within interval
            seen_events: list[Any] = []
            svc.world_state.subscribe(seen_events.append)

            res1 = await svc.life_engine.query("weather.read")
            assert res1.status == "done"
            assert res1.reason == "observation_recorded"
            assert res1.details["source"] == "Open-Meteo"
            assert "小雨" in res1.details["summary"]
            assert "18.5" in res1.details["summary"]
            assert "0.6" in res1.details["summary"]

            await _wait_until(lambda: any(e.event_type == "weather_observed" for e in seen_events))
            obs_events = [e for e in seen_events if e.event_type == "weather_observed"]
            assert len(obs_events) == 1
            assert obs_events[0].source_module == "weather"
            assert "latitude" not in obs_events[0].payload
            assert "longitude" not in obs_events[0].payload

            # Cached hit within 900s interval does NOT hit Open-Meteo or re-emit weather_observed
            forecast_calls_before = sum(
                1 for r in upstream_calls if r.url.host == "api.open-meteo.com"
            )
            now_box[0] = NOW + 120
            res2 = await svc.life_engine.query("weather.read")
            assert res2.status == "done"
            assert res2.reason == "observation_cached"
            assert (
                sum(1 for r in upstream_calls if r.url.host == "api.open-meteo.com")
                == forecast_calls_before
            )
            assert len([e for e in seen_events if e.event_type == "weather_observed"]) == 1
            assert "最近查询:" in await proxy.get_sensor_summary()

            # Custom RPC weather.current returns details while fresh
            curr = await proxy.call_custom_rpc("weather.current", {})
            assert curr["observation"]["temperature_2m"] == 18.5

            # Expired observation -> sensor mentions 过期 & weather.current raises weather_stale
            now_box[0] = NOW + 900
            assert "过期" in await proxy.get_sensor_summary()
            with pytest.raises(Exception, match="weather_stale"):
                await proxy.call_custom_rpc("weather.current", {})

            # Reset without env coords -> configuration_issue returns to weather_location_missing
            reset_empty = await proxy.call_custom_rpc("weather.location.reset", {})
            assert reset_empty == {"location": None}
            await _wait_until(lambda: proxy.configuration_issue == "weather_location_missing")

            # Re-set before reconnect to verify persistence across reconnect
            await proxy.call_custom_rpc("weather.location.set", detected)
            await _wait_until(lambda: proxy.configuration_issue is None)
        finally:
            await plugin.client.stop()
            await pipe.shutdown()

        # Verify LIFO disconnect removed weather proxy & action cleanly
        await _wait_until(lambda: svc.modules.get("weather") is None)
        assert "weather.read" not in svc.modules.catalog.actions

        # Reconnect with env coordinates fallback enabled: saved KV location overrides env!
        pipe2 = _BiPipe(bus, "weather")
        reconnected = WeatherPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token=token,
            env_latitude="31.23",
            env_longitude="121.47",
            timezone="Asia/Shanghai",
            http=http,
            clock=lambda: now_box[0],
            connect_factory=pipe2.connect_factory,
        )
        await reconnected.client.start()
        try:
            assert await reconnected.client.wait_connected(timeout=2.0)
            await _wait_until(lambda: isinstance(svc.modules.get("weather"), RemoteModuleProxy))
            proxy2 = svc.modules.get("weather")
            assert isinstance(proxy2, RemoteModuleProxy)
            await _wait_until(
                lambda: proxy2.admin_status(None).get("location", {}).get("source") == "server_ip"
            )
            assert proxy2.configuration_issue is None

            # Reset location -> deletes KV, falls back to env coordinates
            reset_res = await proxy2.call_custom_rpc("weather.location.reset", {})
            assert reset_res == {
                "location": {
                    "latitude": 31.23,
                    "longitude": 121.47,
                    "source": "environment",
                    "label": "",
                }
            }
            await _wait_until(
                lambda: proxy2.admin_status(None).get("location", {}).get("source") == "environment"
            )
            assert proxy2.configuration_issue is None
        finally:
            await reconnected.close()
            await pipe2.shutdown()


@pytest.mark.parametrize(
    "dns_ip,expected_status", [("93.184.216.34", "done"), ("127.0.0.1", "gated_skip")]
)
async def test_weather_geodns_fallback_and_inflight_location_change(
    dns_ip: str, expected_status: str
) -> None:
    now_box = [NOW]
    hosts: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()
    attempt = 0

    async def handler(req: httpx.Request) -> httpx.Response:
        nonlocal attempt
        hosts.append(req.url.host)
        if req.url.host == "api.open-meteo.com":
            attempt += 1
            if attempt == 1:
                raise httpx.ConnectTimeout("simulated timeout", request=req)
            started.set()
            await release.wait()
            return httpx.Response(
                200,
                json={
                    "current": {
                        "time": int(NOW),
                        "interval": 900,
                        "temperature_2m": 15,
                        "weather_code": 0,
                    }
                },
            )
        if req.url.host == "dns.google":
            return httpx.Response(
                200,
                json={
                    "Status": 0,
                    "Answer": [{"name": "api.open-meteo.com.", "type": 1, "data": dns_ip}],
                },
            )
        assert req.url.host == "93.184.216.34"
        assert req.headers["Host"] == req.extensions["sni_hostname"] == "api.open-meteo.com"
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": int(NOW),
                    "interval": 900,
                    "temperature_2m": 17,
                    "weather_code": 0,
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        plugin = WeatherPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token="test-token",
            env_latitude="31.23",
            env_longitude="121.47",
            http=http,
            clock=lambda: now_box[0],
        )
        res = await plugin.read()
        assert res["status"] == expected_status
        assert hosts[:2] == ["api.open-meteo.com", "dns.google"]

        if expected_status == "done":
            # In-flight read returns weather_location_changed if override changes mid-request
            plugin._cached_observation = None
            task = asyncio.create_task(plugin.read())
            await asyncio.wait_for(started.wait(), timeout=2.0)
            async with plugin._location_lock:
                plugin._apply_override_locked(WeatherLocation(latitude=10.0, longitude=20.0))
            release.set()
            changed = await task
            assert changed == {
                "status": "gated_skip",
                "reason": "weather_location_changed",
                "details": {},
            }
        await plugin.close()


async def test_minecraft_plugin_e2e_actions_node_events_inspiration_and_reconnect(
    phase2_env,
) -> None:
    _, svc, bus, _now_box = phase2_env
    assert svc.modules.unregister("minecraft") is True
    assert svc.modules.get("minecraft") is None
    await svc.life_engine.initialize()

    nt = FakeNodeTransport(is_connected=True)
    nt.responses["mc.status"] = {
        "connected": False,
        "bridge_ready": True,
        "mc_enabled": True,
        "mc_configured": True,
        "paused": False,
        "snapshot": {"connected": False, "activity": "待机"},
    }
    nt.responses["mc.pause"] = {
        "ok": True,
        "bridge_ready": True,
        "paused": True,
    }
    nt.responses["mc.resume"] = {
        "ok": True,
        "bridge_ready": True,
        "paused": False,
    }
    nt.responses["mc.dispatch_goal"] = lambda params: {
        "ok": True,
        "accepted": True,
        "bridge_ready": True,
        "mc_enabled": True,
        "mc_configured": True,
        "paused": False,
        "summary": f"已受理:{params.get('action_type')}",
        "phase": "running",
    }
    svc.node_transport = nt

    token, _ = await bus.create_plugin(
        "minecraft", "Minecraft", ["actions", "sensors", "node_bridge"], node_region="CN"
    )
    pipe = _BiPipe(bus, "minecraft")
    plugin = MinecraftPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        clock=lambda: NOW,
        connect_factory=pipe.connect_factory,
        reconnect_min=0.05,
        reconnect_max=0.1,
    )
    await plugin.client.start()
    try:
        assert await plugin.client.wait_connected(timeout=2.0)
        await _wait_until(lambda: isinstance(svc.modules.get("minecraft"), RemoteModuleProxy))
        proxy = svc.modules.get("minecraft")
        assert isinstance(proxy, RemoteModuleProxy)
        assert proxy.attention_level == "HIGH"
        assert proxy.admin_panel == "minecraft"
        assert proxy.node_region == "CN"

        expected_actions = {
            f"mc.{verb}"
            for verb in (
                "build_plan",
                "explore",
                "mine",
                "gather",
                "goto",
                "follow",
                "chat",
                "observe",
                "look_signs",
                "stop",
                "idle",
                "status",
                "pause",
                "resume",
            )
        }
        assert expected_actions <= set(svc.modules.catalog.actions)
        for verb in (
            "build_plan",
            "explore",
            "mine",
            "gather",
            "goto",
            "follow",
            "chat",
            "observe",
            "look_signs",
            "stop",
            "idle",
        ):
            assert svc.modules.catalog.aliases.get(("minecraft", verb, False)) == f"mc.{verb}"

        # on_connect synced mc.status over node_bridge
        await _wait_until(lambda: proxy.status_dict().get("bridge_ready") is True)
        assert await proxy.get_sensor_summary() == "MC躯干就绪(待机)"

        # node.event mc.snapshot -> updates sensor, status, and HIGH world focus
        await bus.relay_node_event(
            {
                "type": "event",
                "event": "mc.snapshot",
                "payload": {
                    "connected": True,
                    "bridge_ready": True,
                    "mc_enabled": True,
                    "mc_configured": True,
                    "paused": False,
                    "dimension": "overworld",
                    "pos": [12, 64, -8],
                    "health": 18,
                    "food": 20,
                    "active_task": "explore",
                    "activity": "草原探险",
                    "nearby_entities": ["sheep", "cow"],
                    "recent_chat_or_sign": "欢迎来到村庄",
                    "updated_at": NOW,
                },
            }
        )
        await _wait_until(lambda: svc.world_state.current.focus_level == "HIGH")
        assert svc.world_state.current.current_module == "minecraft"
        summary = await proxy.get_sensor_summary()
        assert "MC在线" in summary
        assert "维度:overworld" in summary
        assert "坐标:(12, 64, -8)" in summary
        assert "生命:18/20" in summary
        assert "正在:草原探险" in summary
        assert "饱食:20/20" in summary
        assert "附近:sheep,cow" in summary
        assert "见闻:欢迎来到村庄" in summary

        # node.event mc.milestone -> publishes world event on kernel
        await bus.relay_node_event(
            {
                "type": "event",
                "event": "mc.milestone",
                "payload": {
                    "event_type": "mc_rare_find",
                    "importance": "high",
                    "event_id": "rare-diamond-1",
                    "payload": {"block": "diamond_ore", "pos": [12, -52, 88]},
                },
            }
        )
        events: list[Any] = []
        for _ in range(50):
            events = await svc.world_state.unconsumed_events()
            if any(e.event_type == "mc_rare_find" for e in events):
                break
            await asyncio.sleep(0.02)
        rare = next(e for e in events if e.event_type == "mc_rare_find")
        assert rare.source_module == "minecraft"
        assert rare.importance == "high"
        assert rare.payload["block"] == "diamond_ore"

        # Action dispatch via ActionCatalog + SSRF key stripping + mc.goal RPC
        goto_spec = svc.modules.catalog.actions["mc.goto"]
        goto_args = goto_spec.arguments.model_validate({"x": 10, "y": 64, "z": -5})
        goto_outcome = await goto_spec.run(
            ActionCall({"origin": "planner", "target_scope": ""}, goto_args, NOW)
        )
        assert goto_outcome.status == "done"
        assert goto_outcome.details["result"]["ok"] is True
        action_called, dispatched_params = nt.calls[-1]
        assert action_called == "mc.dispatch_goal"
        assert dispatched_params == {"action_type": "goto", "x": 10.0, "y": 64.0, "z": -5.0}

        goal_res = await proxy.call_custom_rpc(
            "mc.goal",
            {
                "action_type": "goto",
                "x": 1,
                "y": 70,
                "z": 2,
                "host": "169.254.169.254",
                "url": "http://127.0.0.1:8080",
            },
        )
        assert goal_res["ok"] is True
        assert set(nt.calls[-1][1]) == {"action_type", "x", "y", "z"}

        # mc.chat action dispatch, non-effortful origins, and slash/empty validation guards
        chat_spec = svc.modules.catalog.actions["mc.chat"]
        assert chat_spec.effortful is False
        assert "planner" in chat_spec.origins
        assert "admin" in chat_spec.origins
        chat_args = chat_spec.arguments.model_validate({"message": "大家好，我是哈基米！"})
        chat_outcome = await chat_spec.run(
            ActionCall({"origin": "planner", "target_scope": ""}, chat_args, NOW)
        )
        assert chat_outcome.status == "done"
        assert nt.calls[-1] == (
            "mc.dispatch_goal",
            {"action_type": "chat", "message": "大家好，我是哈基米！"},
        )

        calls_before_reject = len(nt.calls)
        slash_res = await plugin.dispatch_goal({"action_type": "chat", "message": "/op Geminy"})
        assert slash_res["ok"] is False
        assert slash_res["reason"] == "invalid_action_arguments"
        assert slash_res["artifacts"]["not_dispatched"] is True

        empty_res = await plugin.dispatch_goal({"action_type": "chat", "message": "   "})
        assert empty_res["ok"] is False
        assert empty_res["reason"] == "invalid_action_arguments"
        assert empty_res["artifacts"]["not_dispatched"] is True

        rpc_slash = await proxy.call_custom_rpc(
            "mc.goal", {"action_type": "chat", "message": " /gamemode creative"}
        )
        assert rpc_slash["ok"] is False
        assert rpc_slash["reason"] == "invalid_action_arguments"
        assert rpc_slash["artifacts"]["not_dispatched"] is True
        assert len(nt.calls) == calls_before_reject

        # Admin pause & resume keep active_task=explore intact when resumed
        pause_spec = svc.modules.catalog.actions["mc.pause"]
        pause_out = await pause_spec.run(
            ActionCall({"origin": "admin", "target_scope": ""}, pause_spec.arguments(), NOW)
        )
        assert pause_out.status == "done"
        assert nt.calls[-1][0] == "mc.pause"
        assert svc.world_state.current.focus_level == "LOW"

        resume_spec = svc.modules.catalog.actions["mc.resume"]
        resume_out = await resume_spec.run(
            ActionCall({"origin": "admin", "target_scope": ""}, resume_spec.arguments(), NOW)
        )
        assert resume_out.status == "done"
        assert nt.calls[-1][0] == "mc.resume"
        await _wait_until(lambda: svc.world_state.current.focus_level == "HIGH")

        # Public bili_video_watched triggers world.reaction -> mc.build_plan; private_first ignored
        pub_ev = await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {"title": "Minecraft 1.21 红石自动门教程", "summary": "生电教学"},
            now=NOW,
        )
        await bus.broadcast_world_event(pub_ev)
        priv_ev = await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_mc_inspiration",
            {"title": "私密MC视频", "private_first": True},
            now=NOW + 1,
        )
        await bus.broadcast_world_event(priv_ev)

        async def _get_build_rows() -> list[Any]:
            return await svc.store.all(
                "SELECT action_name, origin, arguments_json FROM scheduled_intents "
                "WHERE action_name='mc.build_plan'"
            )

        rows: list[Any] = []
        for _ in range(80):
            rows = await _get_build_rows()
            if rows:
                break
            await asyncio.sleep(0.02)
        assert len(rows) == 1
        assert rows[0]["origin"] == "rule"
        assert json.loads(rows[0]["arguments_json"]) == {"topic": "Minecraft 1.21 红石自动门教程"}

        # node.state(False) marks disconnected, pushes status, and releases HIGH focus
        await bus.relay_node_state(False)
        await _wait_until(
            lambda: (
                proxy.status_dict().get("connected") is False
                and proxy.status_dict().get("bridge_ready") is False
                and svc.world_state.current.focus_level == "LOW"
            )
        )

        # node.state(True) re-syncs mc.status when module enabled
        nt.calls.clear()
        await bus.relay_node_state(True)
        await _wait_until(lambda: any(a == "mc.status" for a, _ in nt.calls))

        # on_switch(False) sends stop goal & keeps activity cleared; on_switch(True) re-syncs
        nt.calls.clear()
        proxy.enabled = False
        await proxy.on_switch(svc)
        await _wait_until(
            lambda: any(
                a == "mc.dispatch_goal" and p.get("action_type") == "stop" for a, p in nt.calls
            )
        )
        proxy.enabled = True
        await proxy.on_switch(svc)
        await _wait_until(lambda: any(a == "mc.status" for a, _ in nt.calls))
    finally:
        await plugin.client.stop()
        await pipe.shutdown()

    # Hot disconnect cleanup removes proxy, all 14 actions, and aliases cleanly
    await _wait_until(lambda: svc.modules.get("minecraft") is None)
    assert not any(name.startswith("mc.") for name in svc.modules.catalog.actions)
    assert not any(
        alias[0] == "minecraft" and alias[2] is False for alias in svc.modules.catalog.aliases
    )
    assert not any(target.startswith("mc.") for target in svc.modules.catalog.aliases.values())

    # Reconnect recovery restores proxy & syncs mc.status
    pipe2 = _BiPipe(bus, "minecraft")
    reconnected = MinecraftPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        clock=lambda: NOW,
        connect_factory=pipe2.connect_factory,
    )
    await reconnected.client.start()
    try:
        assert await reconnected.client.wait_connected(timeout=2.0)
        await _wait_until(lambda: isinstance(svc.modules.get("minecraft"), RemoteModuleProxy))
        proxy2 = svc.modules.get("minecraft")
        assert isinstance(proxy2, RemoteModuleProxy)
        await _wait_until(lambda: proxy2.status_dict().get("bridge_ready") is True)
    finally:
        await reconnected.close()
        await pipe2.shutdown()


async def test_minecraft_http_bridge_mode_and_disconnected_not_dispatched() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    mode = "ok"

    def bridge_handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/rpc"
        body = json.loads(req.content)
        calls.append((body["action"], body["params"]))
        if mode == "timeout":
            raise httpx.ReadTimeout("bridge timeout", request=req)
        if mode == "down":
            raise httpx.ConnectError("bridge down", request=req)
        return httpx.Response(
            200,
            json={
                "ok": True,
                "accepted": True,
                "bridge_ready": True,
                "paused": body["action"] == "mc.pause",
                "summary": f"http:{body['action']}",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(bridge_handler)) as http:
        plugin = MinecraftPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token="mc-token",
            mc_bridge_url="http://127.0.0.1:8791",
            http=http,
            clock=lambda: NOW,
        )
        res = await plugin.dispatch_goal({"action_type": "build_plan", "topic": "樱花小屋"})
        assert res["ok"] is True
        assert calls[-1] == ("mc.dispatch_goal", {"action_type": "build_plan", "topic": "樱花小屋"})

        pause_res = await plugin.dispatch_goal({"action_type": "pause"})
        assert pause_res["ok"] is True
        assert calls[-1] == ("mc.pause", {})

        mode = "timeout"
        timeout_out = await plugin._handle_action("idle", {"arguments": {}})
        assert timeout_out["status"] == "uncertain"
        assert timeout_out["reason"] == "mc_bridge_timeout"

        mode = "down"
        down_out = await plugin._handle_action("idle", {"arguments": {}})
        assert down_out["status"] == "uncertain"
        assert down_out["reason"] == "mc_bridge_disconnected"

        await plugin.close()

    # Without MC_BRIDGE_URL and node_online=False, pre-dispatch failure maps to not_dispatched
    offline = MinecraftPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="mc-token",
        clock=lambda: NOW,
    )
    assert offline.node_online is False

    async def fail_node_call(_action: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
        from hakimi_plugin_sdk import PluginRpcError

        raise PluginRpcError("node_disconnected")

    offline.client.node_call = fail_node_call  # type: ignore[method-assign]
    out = await offline._handle_action("build_plan", {"arguments": {"topic": "铁轨"}})
    assert out["status"] == "not_dispatched"
    assert out["reason"] == "node_disconnected"
    assert out["details"]["result"]["artifacts"]["not_dispatched"] is True
    await offline.close()
