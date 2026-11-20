"""Tests for standalone HakimiPluginClient SDK (Contract F)."""

from __future__ import annotations

import ast
import asyncio
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
        "plugins.weather.plugin_main",
        "plugins.minecraft.plugin_main",
        "secrets/plugins.env",
        "ws://hakimi-bot:12141/ws/plugin",
        "condition: service_healthy",
        "read_only: true",
        "no-new-privileges:true",
        "mem_limit: 128m",
        "pids_limit: 32",
    ):
        assert required in compose_text

    deploy_sh = root / "scripts" / "deploy_plugins.sh"
    assert deploy_sh.stat().st_mode & 0o111
    deploy_text = deploy_sh.read_text(encoding="utf-8")
    assert "compose.plugins.yaml" in deploy_text
    assert "--no-deps" in deploy_text
