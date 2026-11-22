"""Tests for standalone HakimiPluginClient SDK (Contract F)."""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import random
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from websockets.asyncio.server import serve as ws_serve

from hakimi_plugin_sdk import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
    ThinkingSceneSpec,
)


class _FakeSocket:
    def __init__(self, *, close_on_eof_code: int = 1000) -> None:
        self.incoming: asyncio.Queue[str | bytes] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.sent_event = asyncio.Event()
        self.closed_codes: list[int] = []
        self.close_code: int | None = None
        self._close_on_eof_code = close_on_eof_code

    async def push(self, frame: dict[str, Any] | str) -> None:
        raw = frame if isinstance(frame, str) else json.dumps(frame, ensure_ascii=False)
        await self.incoming.put(raw)

    async def push_eof(self, code: int = 1000) -> None:
        self.close_code = code
        await self.incoming.put(f"__EOF__:{code}")

    async def recv(self) -> str | bytes:
        item = await self.incoming.get()
        if isinstance(item, str) and item.startswith("__EOF__:"):
            code = int(item.split(":", 1)[1])
            self.close_code = code
            raise ConnectionError(f"closed:{code}")
        return item

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))
        self.sent_event.set()

    async def close(self, code: int = 1000) -> None:
        self.closed_codes.append(code)
        self.close_code = code
        await self.incoming.put(f"__EOF__:{code}")

    async def wait_sent(self, predicate, *, max_wait: float = 2.0) -> dict[str, Any]:
        async with asyncio.timeout(max_wait):
            while True:
                for item in self.sent:
                    if predicate(item):
                        return item
                self.sent_event.clear()
                await self.sent_event.wait()


def _make_config(**overrides: Any) -> PluginRuntimeConfig:
    defaults: dict[str, Any] = {
        "ws_url": "ws://127.0.0.1:12141/ws/plugin",
        "plugin_id": "weather_pilot",
        "token": "secret-token-value-1234567890",
        "display_name": "Weather Pilot",
        "description": "Standalone weather observer",
        "scopes": ("actions", "sensors", "node_bridge"),
        "node_region": "KR",
        "admin_panel": "weather",
        "attention_level": "HIGH",
        "capabilities": ("weather.forecast",),
        "event_subscriptions": ("bili_video_watched",),
        "reconnect_min": 0.01,
        "reconnect_max": 0.04,
        "heartbeat_seconds": 10.0,
        "rpc_timeout": 2.0,
    }
    defaults.update(overrides)
    return PluginRuntimeConfig(**defaults)


def test_zero_hakimi_imports_and_config_validation():
    sdk_dir = Path(__file__).resolve().parents[1] / "src" / "hakimi_plugin_sdk"
    py_files = sorted(sdk_dir.glob("*.py"))
    assert py_files
    for path in py_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name != "hakimi" and not alias.name.startswith("hakimi.")
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module != "hakimi" and not node.module.startswith("hakimi.")

    cfg = _make_config()
    assert cfg.token not in repr(cfg)

    with pytest.raises(ValidationError):
        _make_config(ws_url="ws://127.0.0.1:12141/ws/plugin?token=leak")

    with pytest.raises(ValidationError):
        ThinkingSceneSpec(
            key="weather.scene",
            label="Weather",
            default="xhigh",
            levels=("low", "medium"),
        )


async def test_handshake_headers_and_on_connect_state_get():
    captured_connections: list[tuple[str, dict[str, str], _FakeSocket]] = []
    connected_ready = asyncio.Event()

    def connect_factory(url: str, headers: dict[str, str]) -> _FakeSocket:
        sock = _FakeSocket()
        captured_connections.append((url, dict(headers), sock))
        connected_ready.set()
        return sock

    cfg = _make_config(chat_platforms=("ext_chat",))
    scene = ThinkingSceneSpec(
        key="pilot.think",
        label="Pilot Think",
        default="medium",
        levels=("low", "medium", "high"),
        output_tokens=1024,
    )

    async def echo_handler(params: dict[str, Any]) -> dict[str, Any]:
        return {"status": "done", "reason": "ok", "details": {"echo": params}}

    action = ActionSpec(
        name="weather_pilot.read",
        description="Read pilot weather",
        arguments={"type": "object", "properties": {}, "additionalProperties": False},
        origins=["query", "admin"],
        effortful=False,
        interrupts_focus=False,
        window="any",
        social=False,
        owner_only=False,
        aliases=[["weather_pilot", "read", False]],
        handler=echo_handler,
    )

    client = HakimiPluginClient(
        cfg,
        actions=[action],
        thinking_scenes=[scene],
        initial_status={"ready": False},
        configuration_issue="weather_location_missing",
        connect_factory=connect_factory,
    )

    connect_results: list[tuple[dict[str, Any], Any]] = []
    connect_done = asyncio.Event()

    @client.on_connect
    async def handle_connect(welcome: dict[str, Any]) -> None:
        loc = await client.state_get("location")
        await client.push_status({"ready": True, "location": loc}, configuration_issue=None)
        connect_results.append((welcome, loc))
        connect_done.set()

    await client.start()
    try:
        await asyncio.wait_for(connected_ready.wait(), timeout=2.0)
        sock = captured_connections[0][2]
        url, headers, _ = captured_connections[0]
        assert url == "ws://127.0.0.1:12141/ws/plugin"
        assert "?" not in url
        assert headers == {
            "Authorization": "Bearer secret-token-value-1234567890",
            "X-Plugin-Id": "weather_pilot",
        }

        hello = await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        assert hello["protocol_version"] == 1
        assert hello["display_name"] == "Weather Pilot"
        assert hello["scopes"] == ["actions", "sensors", "node_bridge"]
        assert hello["admin_panel"] == "weather"
        assert hello["attention_level"] == "HIGH"
        assert hello["configuration_issue"] == "weather_location_missing"
        assert hello["initial_status"] == {"ready": False}
        assert hello["event_subscriptions"] == ["bili_video_watched"]
        assert hello["chat_platforms"] == ["ext_chat"]
        assert hello["actions"][0]["aliases"] == [["weather_pilot", "read", False]]
        assert hello["thinking_scenes"][0]["key"] == "pilot.think"

        await sock.push({"type": "plugin.welcome", "plugin_id": "weather_pilot", "enabled": True})
        assert await client.wait_connected(timeout=2.0)

        get_req = await sock.wait_sent(lambda m: m.get("type") == "state.get")
        assert get_req["key"] == "location"
        await sock.push(
            {
                "type": "kernel.result",
                "id": get_req["id"],
                "ok": True,
                "result": {"found": True, "value": {"label": "Seoul"}},
            }
        )
        status_push = await sock.wait_sent(lambda m: m.get("type") == "plugin.status")
        assert status_push == {
            "type": "plugin.status",
            "status": {"ready": True, "location": {"label": "Seoul"}},
            "configuration_issue": None,
        }
        await asyncio.wait_for(connect_done.wait(), timeout=2.0)
        assert connect_results[0][1] == {"label": "Seoul"}
    finally:
        await client.stop()


async def test_real_websockets_reconnect_backoff_and_1008_recovery():
    attempts = 0
    seen_headers: list[tuple[str | None, str | None, str]] = []
    accepted_token = "old-token"
    welcomed = asyncio.Event()

    async def ws_handler(websocket) -> None:
        nonlocal attempts
        attempts += 1
        auth = websocket.request.headers.get("Authorization")
        pid = websocket.request.headers.get("X-Plugin-Id")
        path = websocket.request.path
        seen_headers.append((auth, pid, path))
        if auth != f"Bearer {accepted_token}" or attempts < 3:
            await websocket.close(code=1008, reason="rotated")
            return
        raw = await websocket.recv()
        hello = json.loads(raw)
        assert hello["type"] == "plugin.hello"
        await websocket.send(
            json.dumps({"type": "plugin.welcome", "plugin_id": pid, "enabled": True})
        )
        welcomed.set()
        try:
            await websocket.wait_closed()
        except Exception:
            pass

    async with ws_serve(ws_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        cfg = _make_config(
            ws_url=f"ws://127.0.0.1:{port}/ws/plugin",
            token="new-token",
            reconnect_min=0.02,
            reconnect_max=0.06,
        )
        client = HakimiPluginClient(cfg, rng=random.Random(42))

        async def _rotate_soon() -> None:
            nonlocal accepted_token
            await asyncio.sleep(0.05)
            accepted_token = "new-token"

        rotator = asyncio.create_task(_rotate_soon())
        await client.start()
        try:
            await asyncio.wait_for(welcomed.wait(), timeout=3.0)
            assert await client.wait_connected(timeout=2.0)
            assert attempts >= 3
            assert len(client.backoff_history) >= 2
            for delay in client.backoff_history:
                assert 0.01 <= delay <= 0.06
            for auth, pid, path in seen_headers:
                assert auth == "Bearer new-token"
                assert pid == "weather_pilot"
                assert path == "/ws/plugin"
        finally:
            await rotator
            await client.stop()


async def test_ping_pong_heartbeat_and_frame_validation():
    sock = _FakeSocket()
    cfg = _make_config(heartbeat_seconds=0.04)
    client = HakimiPluginClient(cfg, connect_factory=lambda *_: sock)
    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        await sock.push({"type": "ping", "ts": 1234.5, "id": "hb-1"})
        pong = await sock.wait_sent(lambda m: m.get("type") == "pong")
        assert pong == {"type": "pong", "ts": 1234.5, "id": "hb-1"}

        client_ping = await sock.wait_sent(lambda m: m.get("type") == "ping")
        assert isinstance(client_ping["ts"], float)

        # Malformed NaN / duplicate-key frame triggers reconnect cleanly without hanging
        await sock.push('{"type": "ping", "ts": NaN}')
        await asyncio.sleep(0.05)
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        assert len([m for m in sock.sent if m.get("type") == "plugin.hello"]) >= 2
    finally:
        await client.stop()


async def test_rpc_dispatch_action_sensor_custom_and_errors():
    sock = _FakeSocket()
    client = HakimiPluginClient(_make_config(), connect_factory=lambda *_: sock)

    @client.action(
        "weather_pilot.forecast",
        "Get forecast",
        arguments={"city": {"type": "string"}},
        origins=["planner", "admin"],
    )
    async def handle_forecast(params: dict[str, Any]) -> dict[str, Any]:
        args = params["arguments"]
        if args.get("city") == "bad":
            raise PluginRpcError("weather_city_not_found", "city missing")
        if args.get("city") == "boom":
            raise RuntimeError("sensitive internal secret=abc")
        return {
            "status": "done",
            "reason": "ok",
            "details": {"city": args["city"], "origin": params.get("origin")},
        }

    @client.sensor
    async def read_sensor() -> str:
        return "21C 晴"

    @client.rpc("weather.location.set")
    async def set_location(params: dict[str, Any]) -> dict[str, Any]:
        await client.state_set("location", params)
        return {"location": params}

    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        # 1. Action success
        await sock.push(
            {
                "type": "rpc.call",
                "id": "r1",
                "method": "action.run",
                "params": {
                    "action": "weather_pilot.forecast",
                    "arguments": {"city": "Seoul"},
                    "origin": "planner",
                },
            }
        )
        r1 = await sock.wait_sent(lambda m: m.get("id") == "r1")
        assert r1 == {
            "type": "rpc.result",
            "id": "r1",
            "ok": True,
            "result": {
                "status": "done",
                "reason": "ok",
                "details": {"city": "Seoul", "origin": "planner"},
            },
        }

        # 2. Action PluginRpcError propagation
        await sock.push(
            {
                "type": "rpc.call",
                "id": "r2",
                "method": "action.run",
                "params": {"action": "weather_pilot.forecast", "arguments": {"city": "bad"}},
            }
        )
        r2 = await sock.wait_sent(lambda m: m.get("id") == "r2")
        assert r2 == {
            "type": "rpc.result",
            "id": "r2",
            "ok": False,
            "error": "weather_city_not_found",
        }

        # 3. Unexpected exception does not leak message
        await sock.push(
            {
                "type": "rpc.call",
                "id": "r3",
                "method": "action.run",
                "params": {"action": "weather_pilot.forecast", "arguments": {"city": "boom"}},
            }
        )
        r3 = await sock.wait_sent(lambda m: m.get("id") == "r3")
        assert r3 == {
            "type": "rpc.result",
            "id": "r3",
            "ok": False,
            "error": "plugin_internal_error",
        }

        # 4. Unknown action
        await sock.push(
            {
                "type": "rpc.call",
                "id": "r4",
                "method": "action.run",
                "params": {"action": "weather_pilot.missing"},
            }
        )
        r4 = await sock.wait_sent(lambda m: m.get("id") == "r4")
        assert r4 == {"type": "rpc.result", "id": "r4", "ok": False, "error": "action_not_found"}

        # 4b. PluginBus wire frame ("type": "rpc.request") & post-welcome "plugin.ready"
        await sock.push({"type": "plugin.ready", "plugin_id": "weather_pilot"})
        await sock.push(
            {
                "type": "rpc.request",
                "id": "r4b",
                "method": "action.run",
                "params": {
                    "action": "weather_pilot.forecast",
                    "arguments": {"city": "Tokyo"},
                    "origin": "admin",
                },
            }
        )
        r4b = await sock.wait_sent(lambda m: m.get("id") == "r4b")
        assert r4b == {
            "type": "rpc.result",
            "id": "r4b",
            "ok": True,
            "result": {
                "status": "done",
                "reason": "ok",
                "details": {"city": "Tokyo", "origin": "admin"},
            },
        }

        # 5. Sensor RPC
        await sock.push({"type": "rpc.call", "id": "r5", "method": "sensor.read", "params": {}})
        r5 = await sock.wait_sent(lambda m: m.get("id") == "r5")
        assert r5 == {
            "type": "rpc.result",
            "id": "r5",
            "ok": True,
            "result": {"summary": "21C 晴"},
        }

        # 6. Custom RPC calling outbound state_set concurrently without deadlock
        await sock.push(
            {
                "type": "rpc.call",
                "id": "r6",
                "method": "weather.location.set",
                "params": {"label": "Busan", "latitude": 35.1, "longitude": 129.0},
            }
        )
        set_req = await sock.wait_sent(lambda m: m.get("type") == "state.set")
        await sock.push(
            {"type": "kernel.result", "id": set_req["id"], "ok": True, "result": {"saved": True}}
        )
        r6 = await sock.wait_sent(lambda m: m.get("id") == "r6")
        assert r6 == {
            "type": "rpc.result",
            "id": "r6",
            "ok": True,
            "result": {"location": {"label": "Busan", "latitude": 35.1, "longitude": 129.0}},
        }

        # 7. Unknown custom RPC method
        await sock.push({"type": "rpc.call", "id": "r7", "method": "no.such.method", "params": {}})
        r7 = await sock.wait_sent(lambda m: m.get("id") == "r7")
        assert r7 == {
            "type": "rpc.result",
            "id": "r7",
            "ok": False,
            "error": "rpc_method_not_found",
        }
    finally:
        await client.stop()


async def test_kernel_state_node_call_and_one_way_frames():
    sock = _FakeSocket()
    client = HakimiPluginClient(_make_config(rpc_timeout=0.3), connect_factory=lambda *_: sock)
    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        # state_get missing -> None
        get_task = asyncio.create_task(client.state_get("missing"))
        get_req = await sock.wait_sent(lambda m: m.get("type") == "state.get")
        await sock.push(
            {
                "type": "kernel.result",
                "id": get_req["id"],
                "ok": True,
                "result": {"found": False, "value": None},
            }
        )
        assert await get_task is None

        # state_delete
        del_task = asyncio.create_task(client.state_delete("missing"))
        del_req = await sock.wait_sent(lambda m: m.get("type") == "state.delete")
        assert del_req["key"] == "missing"
        await sock.push(
            {
                "type": "kernel.result",
                "id": del_req["id"],
                "ok": True,
                "result": {"deleted": True},
            }
        )
        await del_task

        # node_call success
        node_task = asyncio.create_task(client.node_call("mc.status", {"brief": True}))
        node_req = await sock.wait_sent(lambda m: m.get("type") == "node.call")
        assert node_req["action"] == "mc.status"
        assert node_req["params"] == {"brief": True}
        await sock.push(
            {
                "type": "kernel.result",
                "id": node_req["id"],
                "ok": True,
                "result": {"connected": True, "bridge_ready": True},
            }
        )
        assert await node_task == {"connected": True, "bridge_ready": True}

        # node_call error propagation
        err_task = asyncio.create_task(client.node_call("bili.forbidden"))
        err_req = await sock.wait_sent(
            lambda m: m.get("type") == "node.call" and m.get("action") == "bili.forbidden"
        )
        await sock.push(
            {
                "type": "kernel.result",
                "id": err_req["id"],
                "ok": False,
                "error": "scope_forbidden",
            }
        )
        with pytest.raises(PluginRpcError) as exc_info:
            await err_task
        assert exc_info.value.code == "scope_forbidden"

        # node_call timeout
        with pytest.raises(PluginRpcError) as timeout_exc:
            await client.node_call("mc.pause", timeout=0.05)
        assert timeout_exc.value.code == "plugin_rpc_timeout"

        # Outbound one-way frames
        await client.push_status({"connected": True}, configuration_issue="none")
        st = await sock.wait_sent(lambda m: m.get("type") == "plugin.status")
        assert st == {
            "type": "plugin.status",
            "status": {"connected": True},
            "configuration_issue": "none",
        }

        await client.emit_world_event(
            "weather_observed",
            {"temperature_2m": 22.5},
            importance="high",
            business_key="obs:1",
        )
        ev = await sock.wait_sent(lambda m: m.get("type") == "world.event")
        assert ev == {
            "type": "world.event",
            "event_type": "weather_observed",
            "payload": {"temperature_2m": 22.5},
            "importance": "high",
            "business_key": "obs:1",
        }

        await client.report_activity("mining", disconnected=False, updated_at=999.0)
        act = await sock.wait_sent(lambda m: m.get("type") == "world.activity")
        assert act == {
            "type": "world.activity",
            "active_task": "mining",
            "disconnected": False,
            "updated_at": 999.0,
        }

        await client.enqueue_reaction(
            "mc.build_plan",
            "灵感笔记",
            ttl=3600,
            arguments={"topic": "红石门"},
            legacy_module="minecraft",
            legacy_verb="build_plan",
            source_event_id="ev-9",
        )
        react = await sock.wait_sent(lambda m: m.get("type") == "world.reaction")
        assert react == {
            "type": "world.reaction",
            "action_name": "mc.build_plan",
            "note": "灵感笔记",
            "ttl": 3600,
            "arguments": {"topic": "红石门"},
            "legacy_module": "minecraft",
            "legacy_verb": "build_plan",
            "source_event_id": "ev-9",
        }
    finally:
        await client.stop()


async def test_inbound_notifications_and_graceful_stop():
    sock = _FakeSocket()
    client = HakimiPluginClient(_make_config(), connect_factory=lambda *_: sock)

    seen: list[tuple[str, Any]] = []
    done = asyncio.Event()

    @client.on_world_event
    async def on_world(event: dict[str, Any]) -> None:
        seen.append(("world", event))

    @client.on_switch
    def on_switch(enabled: bool) -> None:
        seen.append(("switch", enabled))

    @client.on_node_event
    async def on_node_ev(frame: dict[str, Any]) -> None:
        seen.append(("node_event", frame))

    @client.on_node_state
    async def on_node_st(connected: bool) -> None:
        seen.append(("node_state", connected))
        if len(seen) >= 4:
            done.set()

    async with client:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        await sock.push(
            {
                "type": "world.event.broadcast",
                "event": {"event_type": "bili_video_watched", "payload": {"title": "MC"}},
            }
        )
        await sock.push({"type": "module.switch", "enabled": False})
        await sock.push(
            {"type": "node.event", "frame": {"event": "mc.snapshot", "payload": {"health": 20}}}
        )
        await sock.push({"type": "node.state", "connected": True})

        await asyncio.wait_for(done.wait(), timeout=2.0)
        assert seen == [
            ("world", {"event_type": "bili_video_watched", "payload": {"title": "MC"}}),
            ("switch", False),
            ("node_event", {"event": "mc.snapshot", "payload": {"health": 20}}),
            ("node_state", True),
        ]

    assert not client.connected
    assert 1000 in sock.closed_codes
    # Idempotent stop
    await client.stop()

    with pytest.raises(PluginRpcError) as disc_exc:
        await client.state_get("location")
    assert disc_exc.value.code == "plugin_disconnected"


async def test_pending_kernel_rpc_fails_on_disconnect_and_duplicate_keys_rejected():
    sock = _FakeSocket()
    client = HakimiPluginClient(_make_config(rpc_timeout=2.0), connect_factory=lambda *_: sock)
    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        # Duplicate-key JSON triggers reconnect cleanly
        await sock.push('{"type": "ping", "ts": 1, "ts": 2}')
        await sock.wait_sent(
            lambda m: (
                m.get("type") == "plugin.hello"
                and len([x for x in sock.sent if x.get("type") == "plugin.hello"]) >= 2
            )
        )
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        # In-flight state_get interrupted by disconnect raises plugin_disconnected.
        pending = asyncio.create_task(client.state_get("location"))
        await sock.wait_sent(lambda m: m.get("type") == "state.get")
        await sock.push_eof(1012)
        with pytest.raises(PluginRpcError) as exc_info:
            await pending
        assert exc_info.value.code == "plugin_disconnected"
    finally:
        await client.stop()


def test_packaging_and_deploy_script_conventions():
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "plugins" / "Dockerfile").read_text(encoding="utf-8")
    assert "python:3.12-slim" in dockerfile
    assert "pip install --no-cache-dir" in dockerfile
    for pkg in ("websockets", "httpx", "pydantic"):
        assert pkg in dockerfile
    assert "10001" in dockerfile
    assert "PYTHONPATH=/app/src:/app" in dockerfile

    compose_text = (root / "compose.plugins.yaml").read_text(encoding="utf-8")
    for required in (
        "hakimi-plugin-weather:",
        "hakimi-plugin-minecraft:",
        "hakimi-plugin-bilibili:",
        "hakimi-plugin-web-tools:",
        "hakimi-plugin-doc-render:",
        "hakimi-plugin-song-finder:",
        "plugins.weather.plugin_main",
        "plugins.minecraft.plugin_main",
        "plugins.bilibili.plugin_main",
        "plugins.web_tools.plugin_main",
        "plugins.doc_render.plugin_main",
        "plugins.song_finder.plugin_main",
        "dockerfile: plugins/doc_render/Dockerfile",
        "dockerfile: plugins/song_finder/Dockerfile",
        "secrets/plugins.env",
        "ws://hakimi-bot:12141/ws/plugin",
        "condition: service_healthy",
        "read_only: true",
        "no-new-privileges:true",
        "mem_limit: 128m",
        "pids_limit: 32",
    ):
        assert required in compose_text
    for forbidden in ("colab-adc.json", "BILI_COOKIE", "SESSDATA"):
        assert forbidden not in compose_text

    # Verify every service block in compose.plugins.yaml enforces hardened security defaults.
    blocks: dict[str, str] = {}
    cur_svc: str | None = None
    for line in compose_text.splitlines():
        if line.startswith("  ") and not line.startswith("   ") and line.rstrip().endswith(":"):
            cur_svc = line.strip()[:-1]
            blocks[cur_svc] = ""
        elif cur_svc is not None:
            blocks[cur_svc] += line + "\n"
    assert set(blocks) == {
        "hakimi-plugin-weather",
        "hakimi-plugin-minecraft",
        "hakimi-plugin-bilibili",
        "hakimi-plugin-web-tools",
        "hakimi-plugin-doc-render",
        "hakimi-plugin-song-finder",
    }
    for svc_name, block in blocks.items():
        assert "read_only: true" in block, svc_name
        assert "cap_drop: [ALL]" in block, svc_name
        assert "no-new-privileges:true" in block, svc_name
        assert "init: true" in block, svc_name
        assert "volumes:" not in block, svc_name
        assert "mem_limit:" in block, svc_name
        assert "pids_limit:" in block, svc_name
        assert "tmpfs:" in block, svc_name

    # Lightweight shared Dockerfile remains unbloated.
    for heavy in ("pillow", "pypdf", "yt-dlp", "ffmpeg", "fonts-wqy"):
        assert heavy not in dockerfile.lower()

    deploy_sh = root / "scripts" / "deploy_plugins.sh"
    assert deploy_sh.stat().st_mode & 0o111
    deploy_text = deploy_sh.read_text(encoding="utf-8")
    assert "compose.plugins.yaml" in deploy_text
    assert "--no-deps" in deploy_text
    assert "hakimi-plugin-bilibili" in deploy_text
    assert "bilibili|bilibili_surf|hakimi-plugin-bilibili" in deploy_text
    for wave3_pattern in (
        "web_tools|web-tools|hakimi-plugin-web-tools",
        "doc_render|doc-render|hakimi-plugin-doc-render",
        "song_finder|song-finder|hakimi-plugin-song-finder",
        "wave3)",
    ):
        assert wave3_pattern in deploy_text


def test_phase3_extended_specs_and_validation():
    default_scene = ThinkingSceneSpec(key="bili.default", label="Default Scene")
    assert "output_cap" not in default_scene.to_hello_dict()

    capped_scene = ThinkingSceneSpec(
        key="bilibili_summary",
        label="B站视频总结",
        default="medium",
        levels=("low", "medium", "high"),
        output_tokens=2048,
        output_cap="summary_output_tokens",
    )
    assert capped_scene.to_hello_dict()["output_cap"] == "summary_output_tokens"

    with pytest.raises(ValidationError):
        ThinkingSceneSpec(key="bad.cap", label="Bad", output_cap="unknown_cap")  # type: ignore[arg-type]

    async def noop(params: dict[str, Any]) -> dict[str, Any]:
        return {"status": "done"}

    plain_action = ActionSpec(name="bili.plain", description="Plain", handler=noop)
    plain_hello = plain_action.to_hello_dict()
    assert "budget_lane" not in plain_hello
    assert "quotas" not in plain_hello
    assert "timeout_seconds" not in plain_hello

    patrol_action = ActionSpec(
        name="bili.patrol",
        description="Run patrol",
        budget_lane="patrol",
        quotas=[{"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}],
        timeout_seconds=1500.0,
        handler=noop,
    )
    assert patrol_action.to_hello_dict()["budget_lane"] == "patrol"
    assert patrol_action.to_hello_dict()["quotas"] == [
        {"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}
    ]
    assert patrol_action.to_hello_dict()["timeout_seconds"] == 1500.0
    assert (
        ActionSpec(
            name="bili.max", description="Max", timeout_seconds=1800.0, handler=noop
        ).to_hello_dict()["timeout_seconds"]
        == 1800.0
    )

    for bad_timeout in (0.5, 1801.0, True, float("nan")):
        with pytest.raises(ValidationError):
            ActionSpec(
                name="bili.bad",
                description="Bad timeout",
                timeout_seconds=bad_timeout,  # type: ignore[arg-type]
                handler=noop,
            )

    assert (
        inspect.signature(HakimiPluginClient.ai_video_analyze).parameters["timeout"].default
        == 720.0
    )

    with pytest.raises(ValidationError):
        ActionSpec(
            name="bili.bad_lane",
            description="Bad lane",
            budget_lane="invalid",  # type: ignore[arg-type]
            handler=noop,
        )

    client = HakimiPluginClient(_make_config(), thinking_scenes=[capped_scene])

    @client.action(
        "bili.patrol",
        "Patrol Bilibili",
        budget_lane="patrol",
        quotas=[{"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}],
        timeout_seconds=180.0,
    )
    async def patrol_handler(params: dict[str, Any]) -> dict[str, Any]:
        return {"status": "done"}

    hello = client.hello_payload()
    assert hello["thinking_scenes"][0]["output_cap"] == "summary_output_tokens"
    assert hello["actions"][0]["budget_lane"] == "patrol"
    assert hello["actions"][0]["quotas"] == [
        {"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}
    ]
    assert hello["actions"][0]["timeout_seconds"] == 180.0


async def test_phase3_ai_helpers_notify_telemetry_and_acks():
    sock = _FakeSocket()
    client = HakimiPluginClient(
        _make_config(
            plugin_id="bilibili_surf",
            scopes=("actions", "sensors", "thinking_scenes", "owner_notify"),
        ),
        initial_status={"ready": False},
        connect_factory=lambda *_: sock,
    )
    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push({"type": "plugin.welcome"})
        assert await client.wait_connected(timeout=2.0)

        # 1. ai_budget_check
        budget_task = asyncio.create_task(client.ai_budget_check("patrol"))
        budget_req = await sock.wait_sent(lambda m: m.get("type") == "ai.budget_check")
        assert budget_req["budget_lane"] == "patrol"
        await sock.push(
            {
                "type": "kernel.result",
                "id": budget_req["id"],
                "ok": True,
                "result": {"available": True, "hard_stop": False},
            }
        )
        assert await budget_task == {"available": True, "hard_stop": False}

        # 2. ai_screen (without and with images)
        screen_task = asyncio.create_task(
            client.ai_screen({"title": "Hello"}, budget_lane="patrol", purpose="source")
        )
        screen_req = await sock.wait_sent(
            lambda m: m.get("type") == "ai.screen" and m.get("payload") == {"title": "Hello"}
        )
        assert screen_req["budget_lane"] == "patrol"
        assert screen_req["purpose"] == "source"
        assert "images" not in screen_req
        assert "decision_template" not in screen_req
        await sock.push(
            {
                "type": "kernel.result",
                "id": screen_req["id"],
                "ok": True,
                "result": {"passed": True},
            }
        )
        assert await screen_task == {"passed": True}

        tpl_screen_task = asyncio.create_task(
            client.ai_screen(
                {"trigger": "cruise"},
                budget_lane="proactive",
                purpose="source",
                decision_template="mc_cruise_gate",
            )
        )
        tpl_screen_req = await sock.wait_sent(
            lambda m: (
                m.get("type") == "ai.screen" and m.get("decision_template") == "mc_cruise_gate"
            )
        )
        assert tpl_screen_req["budget_lane"] == "proactive"
        await sock.push(
            {
                "type": "kernel.result",
                "id": tpl_screen_req["id"],
                "ok": True,
                "result": {
                    "passed": True,
                    "template": "mc_cruise_gate",
                    "decisions": {
                        "should_replan": {
                            "type": "noul",
                            "value": False,
                            "noul": 0.1,
                            "confidence": 0.9,
                            "confident": True,
                        }
                    },
                },
            }
        )
        tpl_res = await tpl_screen_task
        assert tpl_res["passed"] is True
        assert tpl_res["template"] == "mc_cruise_gate"
        assert tpl_res["decisions"]["should_replan"]["value"] is False

        img_screen_task = asyncio.create_task(
            client.ai_screen(
                {"title": "With Image"},
                images=[{"mime_type": "image/jpeg", "data": "Zm9v"}],
            )
        )
        img_screen_req = await sock.wait_sent(
            lambda m: m.get("type") == "ai.screen" and m.get("payload") == {"title": "With Image"}
        )
        assert img_screen_req["images"] == [{"mime_type": "image/jpeg", "data": "Zm9v"}]
        await sock.push(
            {
                "type": "kernel.result",
                "id": img_screen_req["id"],
                "ok": False,
                "error": "ai_screen_blocked",
                "message": "unsafe content",
            }
        )
        with pytest.raises(PluginRpcError) as screen_err:
            await img_screen_task
        assert screen_err.value.code == "ai_screen_blocked"

        # 3. ai_generate (ai.result ok & error)
        gen_task = asyncio.create_task(
            client.ai_generate(
                "bili_selection",
                {"candidates": [{"bvid": "BV1xx411c7mD"}]},
                prompt="select picks",
                budget_lane="patrol",
                images=[{"mime_type": "image/png", "data": "YmFy"}],
            )
        )
        gen_req = await sock.wait_sent(
            lambda m: m.get("type") == "ai.generate" and m.get("scenario") == "bili_selection"
        )
        assert gen_req["payload"] == {"candidates": [{"bvid": "BV1xx411c7mD"}]}
        assert gen_req["prompt"] == "select picks"
        assert gen_req["budget_lane"] == "patrol"
        assert gen_req["images"] == [{"mime_type": "image/png", "data": "YmFy"}]
        await sock.push(
            {"type": "ai.result", "id": gen_req["id"], "ok": True, "text": '{"picks": []}'}
        )
        assert await gen_task == '{"picks": []}'

        gen_err_task = asyncio.create_task(
            client.ai_generate("bili_learning", {"bvid": "BV1xx411c7mD"})
        )
        gen_err_req = await sock.wait_sent(
            lambda m: m.get("type") == "ai.generate" and m.get("scenario") == "bili_learning"
        )
        assert gen_err_req["budget_lane"] == "proactive"
        assert "prompt" not in gen_err_req
        assert "images" not in gen_err_req
        await sock.push(
            {
                "type": "ai.result",
                "id": gen_err_req["id"],
                "ok": False,
                "error": "budget_exhausted",
                "message": "no quota",
            }
        )
        with pytest.raises(PluginRpcError) as gen_err:
            await gen_err_task
        assert gen_err.value.code == "budget_exhausted"

        # 4. ai_video_analyze
        va_task = asyncio.create_task(
            client.ai_video_analyze("BV1xx411c7mD", {"title": "Test Video"})
        )
        va_req = await sock.wait_sent(lambda m: m.get("type") == "ai.video_analyze")
        assert va_req["bvid"] == "BV1xx411c7mD"
        assert va_req["material"] == {"title": "Test Video"}
        assert va_req["budget_lane"] == "patrol"
        await sock.push(
            {
                "type": "kernel.result",
                "id": va_req["id"],
                "ok": True,
                "result": {"text": "总结内容", "mode": "subtitle", "material_scope": "subtitles"},
            }
        )
        assert await va_task == {
            "text": "总结内容",
            "mode": "subtitle",
            "material_scope": "subtitles",
        }

        # 4b. bili_memory_preferences & bili_commit_private_topic
        mp_task = asyncio.create_task(client.bili_memory_preferences())
        mp_req = await sock.wait_sent(lambda m: m.get("type") == "bili.memory_preferences")
        await sock.push(
            {
                "type": "kernel.result",
                "id": mp_req["id"],
                "ok": True,
                "result": {
                    "preferences": {
                        "recent_chat_topics": ["帮我找个天文视频"],
                        "private_topic_refs": ["recent_chat_topics:0"],
                    },
                    "private_topic": {
                        "handle": "hdl-1",
                        "owner_private": True,
                        "audience": "a" * 64,
                        "session_scope": "s" * 64,
                    },
                },
            }
        )
        mp_res = await mp_task
        assert mp_res["private_topic"]["handle"] == "hdl-1"

        cm_task = asyncio.create_task(client.bili_commit_private_topic("hdl-1", "BV1xx411c7mD"))
        cm_req = await sock.wait_sent(lambda m: m.get("type") == "bili.private_topic_commit")
        assert cm_req["handle"] == "hdl-1"
        assert cm_req["bvid"] == "BV1xx411c7mD"
        await sock.push(
            {
                "type": "kernel.result",
                "id": cm_req["id"],
                "ok": True,
                "result": {
                    "committed": True,
                    "owner_private": True,
                    "audience": "a" * 64,
                    "session_scope": "s" * 64,
                },
            }
        )
        assert (await cm_task)["committed"] is True

        cm_err_task = asyncio.create_task(
            client.bili_commit_private_topic("hdl-revoked", "BV1xx411c7mD")
        )
        cm_err_req = await sock.wait_sent(
            lambda m: (
                m.get("type") == "bili.private_topic_commit" and m.get("handle") == "hdl-revoked"
            )
        )
        await sock.push(
            {
                "type": "kernel.result",
                "id": cm_err_req["id"],
                "ok": False,
                "error": "bili_private_request_revoked",
            }
        )
        with pytest.raises(PluginRpcError) as cm_err:
            await cm_err_task
        assert cm_err.value.code == "bili_private_request_revoked"

        # 5. notify_owner
        await client.notify_owner("风控触发", code="bili_risk")
        notify = await sock.wait_sent(lambda m: m.get("type") == "owner.notify")
        assert notify == {"type": "owner.notify", "text": "风控触发", "code": "bili_risk"}

        # 6. push_status with unavailable_actions & current_activity (including explicit None clear)
        await client.push_status(
            {"ready": True},
            unavailable_actions={"bili.patrol": "risk_cooldown"},
            current_activity="bili_patrol",
        )
        st1 = await sock.wait_sent(
            lambda m: (
                m.get("type") == "plugin.status" and m.get("current_activity") == "bili_patrol"
            )
        )
        assert st1 == {
            "type": "plugin.status",
            "status": {"ready": True},
            "configuration_issue": None,
            "unavailable_actions": {"bili.patrol": "risk_cooldown"},
            "current_activity": "bili_patrol",
        }
        assert "unavailable_actions" not in client.hello_payload()["initial_status"]

        await client.push_status(
            {"ready": True},
            unavailable_actions={"bili.patrol": None},
            current_activity=None,
        )
        st2 = await sock.wait_sent(
            lambda m: (
                m.get("type") == "plugin.status"
                and "current_activity" in m
                and m["current_activity"] is None
            )
        )
        assert st2 == {
            "type": "plugin.status",
            "status": {"ready": True},
            "configuration_issue": None,
            "unavailable_actions": {"bili.patrol": None},
            "current_activity": None,
        }

        # 7. report_activity(kind="bili_reading", active=True)
        await client.report_activity(updated_at=1234.0, kind="bili_reading", active=True)
        act = await sock.wait_sent(
            lambda m: m.get("type") == "world.activity" and m.get("kind") == "bili_reading"
        )
        assert act == {
            "type": "world.activity",
            "active_task": "",
            "disconnected": False,
            "updated_at": 1234.0,
            "kind": "bili_reading",
            "active": True,
        }

        # 8. emit_world_event(..., report_text=..., mood_longnick=...)
        await client.emit_world_event(
            "bili_video_watched",
            {"bvid": "BV1xx411c7mD"},
            report_text="今日看了红石教程",
            mood_longnick="沉迷红石电路",
        )
        ev = await sock.wait_sent(
            lambda m: m.get("type") == "world.event" and m.get("event_type") == "bili_video_watched"
        )
        assert ev == {
            "type": "world.event",
            "event_type": "bili_video_watched",
            "payload": {"bvid": "BV1xx411c7mD"},
            "importance": "normal",
            "report_text": "今日看了红石教程",
            "mood_longnick": "沉迷红石电路",
        }

        # 9. Silently ignored ack frames do not break session or pending RPCs
        for ack_type in (
            "plugin.status.ack",
            "world.event.ack",
            "world.activity.ack",
            "world.reaction.ack",
            "chat.inbound.ack",
            "owner.notify.ack",
        ):
            await sock.push({"type": ack_type, "id": "ack-1", "ok": True})
        ping_task = asyncio.create_task(client.ai_budget_check("patrol"))
        ping_req = await sock.wait_sent(
            lambda m: m.get("type") == "ai.budget_check" and m.get("id") != budget_req["id"]
        )
        await sock.push(
            {
                "type": "kernel.result",
                "id": ping_req["id"],
                "ok": True,
                "result": {"available": False, "hard_stop": True},
            }
        )
        assert await ping_task == {"available": False, "hard_stop": True}
    finally:
        await client.stop()


def test_session_exit_action_config_and_hello_payload():
    async def noop(params: dict[str, Any]) -> dict[str, Any]:
        return {"status": "done"}

    action = ActionSpec(name="weather_pilot.stop", description="Stop pilot", handler=noop)

    cfg_default = _make_config()
    assert cfg_default.session_exit_action is None
    hello_default = HakimiPluginClient(cfg_default, actions=[action]).hello_payload()
    assert "session_exit_action" not in hello_default

    cfg_empty = _make_config(session_exit_action="")
    assert cfg_empty.session_exit_action is None
    hello_empty = HakimiPluginClient(cfg_empty, actions=[action]).hello_payload()
    assert "session_exit_action" not in hello_empty

    cfg_set = _make_config(session_exit_action="weather_pilot.stop")
    assert cfg_set.session_exit_action == "weather_pilot.stop"
    hello_set = HakimiPluginClient(cfg_set, actions=[action]).hello_payload()
    assert hello_set["session_exit_action"] == "weather_pilot.stop"

    for bad_action in ("stop", "Bad.Action", "weather_pilot.stop!", "   ", "1bad.stop"):
        with pytest.raises(ValidationError):
            _make_config(session_exit_action=bad_action)


def test_blob_protocol_helpers_bounds_and_sanitization():
    from hakimi_plugin_sdk.protocol import (
        BLOB_CHUNK_RAW_BYTES,
        BLOB_HARD_MAX_BYTES,
        BLOB_INLINE_MAX_BYTES,
        compute_chunk_raw_bytes,
        compute_inline_max_bytes,
        decode_blob_chunk,
        encode_blob_chunk,
        sanitize_blob_meta_str,
        sha256_hex,
        validate_blob_descriptor,
    )

    assert compute_chunk_raw_bytes(262_144) == BLOB_CHUNK_RAW_BYTES
    assert 4096 <= compute_chunk_raw_bytes(16_384) <= 10_000
    assert compute_inline_max_bytes(262_144) == BLOB_INLINE_MAX_BYTES
    assert compute_inline_max_bytes(16_384) == 0

    assert sanitize_blob_meta_str("../../etc/passwd\n", max_len=64, is_filename=True) == "passwd"
    assert (
        sanitize_blob_meta_str("..", max_len=64, default="fallback", is_filename=True) == "fallback"
    )
    assert sanitize_blob_meta_str("audio/mpeg\x00\r\n", max_len=64) == "audio/mpeg"

    sample = b"hello-blob-chunk"
    b64 = encode_blob_chunk(sample)
    assert decode_blob_chunk(b64, max_raw_bytes=len(sample)) == sample
    with pytest.raises(ValueError, match="blob_too_large"):
        decode_blob_chunk(b64, max_raw_bytes=len(sample) - 1)
    with pytest.raises(ValueError, match="blob_invalid"):
        decode_blob_chunk("not!valid!base64", max_raw_bytes=64)

    desc = validate_blob_descriptor(
        {
            "encoding": "inline_b64",
            "data_b64": b64,
            "byte_size": len(sample),
            "sha256": sha256_hex(sample),
            "mime_type": "text/plain",
            "filename": "../notes/demo.txt",
        },
        max_bytes=1024,
    )
    assert desc["filename"] == "demo.txt"
    with pytest.raises(ValueError, match="blob_too_large"):
        validate_blob_descriptor(
            {
                "encoding": "chunked",
                "blob_id": "a" * 32,
                "byte_size": BLOB_HARD_MAX_BYTES + 1,
                "sha256": sha256_hex(sample),
                "total_chunks": 2,
            },
            max_bytes=BLOB_HARD_MAX_BYTES + 1024,
        )


async def test_sdk_bidirectional_chunked_blob_and_backpressure():
    from hakimi_plugin_sdk import BlobResult
    from hakimi_plugin_sdk.protocol import (
        BLOB_FIELD,
        BLOB_RPC_GET,
        BLOB_RPC_PUT,
        BLOB_RPC_RELEASE,
        encode_blob_chunk,
        sha256_hex,
    )

    sock = _FakeSocket()
    received_blobs: list[tuple[bytes, dict[str, Any]]] = []

    async def echo_blob(params: dict[str, Any]) -> BlobResult:
        raw = params["blob"]
        received_blobs.append((raw, dict(params["blob_meta"])))
        doubled = raw + raw[::-1]
        return BlobResult(
            data={"echoed": True, "in_size": len(raw)},
            blob=doubled,
            mime_type="application/octet-stream",
            filename="../out/echo.bin",
        )

    cfg = _make_config(plugin_id="doc_render", scopes=("actions",), admin_panel=None)
    client = HakimiPluginClient(
        cfg,
        actions=[ActionSpec(name="doc_render.echo", description="Echo blob", handler=echo_blob)],
        connect_factory=lambda _url, **_kw: sock,
        max_frame_bytes=32_768,
        max_blob_bytes=512 * 1024,
    )

    await client.start()
    try:
        await sock.wait_sent(lambda m: m.get("type") == "plugin.hello")
        await sock.push(
            {"type": "plugin.welcome", "plugin_id": "doc_render", "scopes": ["actions"]}
        )
        assert await client.wait_connected(timeout=2.0)

        # Upload 30 KiB in 3 x 10 KiB chunks (exceeds inline threshold for 32 KiB frame).
        payload = bytes(range(256)) * 120  # 30,720 bytes
        digest = sha256_hex(payload)
        tid = "1" * 32
        chunk_size = 10_240
        for seq in range(3):
            chunk = payload[seq * chunk_size : (seq + 1) * chunk_size]
            await sock.push(
                {
                    "type": "rpc.request",
                    "id": f"put-{seq}",
                    "method": BLOB_RPC_PUT,
                    "params": {
                        "transfer_id": tid,
                        "seq": seq,
                        "total_chunks": 3,
                        "total_bytes": len(payload),
                        "sha256": digest,
                        "data_b64": encode_blob_chunk(chunk),
                        "mime_type": "application/pdf",
                        "filename": "../../secret/sample.pdf",
                    },
                }
            )
            ack = await sock.wait_sent(lambda m, rid=f"put-{seq}": m.get("id") == rid)
            assert ack["ok"] is True
            assert ack["result"]["complete"] is (seq == 2)

        # Invoke action referencing the uploaded chunked blob
        await sock.push(
            {
                "type": "rpc.request",
                "id": "call-echo",
                "method": "action.run",
                "params": {
                    "action": "doc_render.echo",
                    "arguments": {},
                    BLOB_FIELD: {
                        "encoding": "chunked",
                        "transfer_id": tid,
                        "byte_size": len(payload),
                        "sha256": digest,
                        "total_chunks": 3,
                        "mime_type": "application/pdf",
                        "filename": "sample.pdf",
                    },
                },
            }
        )
        res = await sock.wait_sent(lambda m: m.get("id") == "call-echo")
        assert res["ok"] is True
        assert received_blobs[0][0] == payload
        assert received_blobs[0][1]["filename"] == "sample.pdf"
        assert not client._inbound_blobs

        out_desc = res["result"][BLOB_FIELD]
        assert out_desc["encoding"] == "chunked"
        assert out_desc["filename"] == "echo.bin"
        assert out_desc["byte_size"] == len(payload) * 2
        blob_id = out_desc["blob_id"]

        assembled = bytearray()
        for seq in range(out_desc["total_chunks"]):
            await sock.push(
                {
                    "type": "rpc.request",
                    "id": f"get-{seq}",
                    "method": BLOB_RPC_GET,
                    "params": {"blob_id": blob_id, "seq": seq},
                }
            )
            chunk_msg = await sock.wait_sent(lambda m, rid=f"get-{seq}": m.get("id") == rid)
            assert chunk_msg["ok"] is True
            import base64

            assembled.extend(base64.b64decode(chunk_msg["result"]["data_b64"]))
        assert bytes(assembled) == payload + payload[::-1]
        assert not client._outbound_blobs

        # Reject oversized blob.put immediately
        await sock.push(
            {
                "type": "rpc.request",
                "id": "put-huge",
                "method": BLOB_RPC_PUT,
                "params": {
                    "transfer_id": "2" * 32,
                    "seq": 0,
                    "total_chunks": 100,
                    "total_bytes": 1024 * 1024,
                    "sha256": digest,
                    "data_b64": encode_blob_chunk(b"x"),
                },
            }
        )
        huge_res = await sock.wait_sent(lambda m: m.get("id") == "put-huge")
        assert huge_res["ok"] is False and huge_res["error"] == "plugin_blob_too_large"

        # Explicit blob.release cleans up an incomplete inbound transfer
        await sock.push(
            {
                "type": "rpc.request",
                "id": "put-partial",
                "method": BLOB_RPC_PUT,
                "params": {
                    "transfer_id": "3" * 32,
                    "seq": 0,
                    "total_chunks": 2,
                    "total_bytes": 20,
                    "sha256": sha256_hex(b"a" * 20),
                    "data_b64": encode_blob_chunk(b"a" * 10),
                },
            }
        )
        await sock.wait_sent(lambda m: m.get("id") == "put-partial")
        assert "3" * 32 in client._inbound_blobs

        await sock.push(
            {
                "type": "rpc.request",
                "id": "rel-partial",
                "method": BLOB_RPC_RELEASE,
                "params": {"transfer_id": "3" * 32},
            }
        )
        rel_res = await sock.wait_sent(lambda m: m.get("id") == "rel-partial")
        assert rel_res["ok"] is True and rel_res["result"]["released"] is True
        assert "3" * 32 not in client._inbound_blobs
    finally:
        await client.stop()
