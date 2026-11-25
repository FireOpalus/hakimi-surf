"""Tests for plugin session_exit_action protocol, handshake validation, and MC exit flow."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.modules.actions import ActionCall
from hakimi.plugin_bus import PluginBus, RemoteModuleProxy
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import configured_secrets
from hakimi_plugin_sdk import PluginRpcError
from plugins.minecraft.play_brain import McBrainConfig
from plugins.minecraft.plugin_main import MinecraftPlugin
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_phase2_hotplug_plugins import _BiPipe, _wait_until
from tests.test_plugin_bus import _PluginSocket
from tests.test_v20_p4_cross_module_mc import FakeNodeTransport

TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 11, 21, 22, 30, tzinfo=TZ).timestamp()


@pytest.fixture
async def exit_bus_env(settings: Any):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
        }
    )
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        svc.modules.unregister("minecraft")
        await svc.life_engine.initialize()
        bus: PluginBus = (
            getattr(svc, "plugin_bus", None) or svc.modules.systems["plugin_bus"].plugin_bus
        )
        yield app, svc, bus, gw


def test_mc_brain_config_solo_cruise_interval_default() -> None:
    assert McBrainConfig().solo_cruise_interval_seconds == 60.0


async def test_plugin_bus_session_exit_action_handshake_acceptance_and_rejection(
    exit_bus_env: Any,
) -> None:
    _, svc, bus, _ = exit_bus_env
    await bus.create_plugin("demo_plug", "Demo Plug", ["actions", "chat"])

    async def _attempt_hello(hello_overrides: dict[str, Any]) -> tuple[_PluginSocket, list[dict]]:
        sock = _PluginSocket()
        task = asyncio.create_task(
            bus.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                plugin_id="demo_plug",
            )
        )
        frame: dict[str, Any] = {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Demo Plug",
            "scopes": ["actions", "chat"],
            "actions": [
                {
                    "name": "demo_plug.stop",
                    "description": "Exit active session",
                    "arguments": {
                        "type": "object",
                        "properties": {"graceful": {"type": "boolean", "default": True}},
                        "required": [],
                        "additionalProperties": False,
                    },
                },
                {
                    "name": "demo_plug.say",
                    "description": "Social chat",
                    "social": True,
                },
                {
                    "name": "demo_plug.need_arg",
                    "description": "Needs required arg",
                    "arguments": {
                        "type": "object",
                        "properties": {"target": {"type": "string"}},
                        "required": ["target"],
                        "additionalProperties": False,
                    },
                },
            ],
            "chat_platforms": ["demo_chat"],
        }
        frame.update(hello_overrides)
        await sock.push(frame)
        try:
            msgs = await sock.recv_until(lambda m: m.get("type") in {"plugin.ready", "error"})
            if msgs[-1].get("type") == "error":
                await asyncio.wait_for(task, 2.0)
            return sock, msgs
        except Exception:
            await asyncio.wait_for(task, 2.0)
            raise

    # 1. Invalid schema rejections -> plugin_invalid_schema + close 1008 + unmounted
    bad_cases: list[dict[str, Any]] = [
        {"session_exit_action": "invalid_no_dot"},
        {"session_exit_action": "Demo.Stop"},
        {"session_exit_action": 123},
        {"session_exit_action": "demo_plug.unknown"},
        {"session_exit_action": "other_plug.stop"},
        {"session_exit_action": "demo_plug.say"},
        {"session_exit_action": "demo_plug.need_arg"},
        {"scopes": ["chat"], "actions": [], "session_exit_action": "demo_plug.stop"},
    ]
    for bad in bad_cases:
        sock, msgs = await _attempt_hello(bad)
        assert msgs[-1] == {"type": "error", "reason": "plugin_invalid_schema"}
        assert sock.closed_codes == [1008]
        assert svc.modules.get("demo_plug") is None

    # 2. Empty string normalizes to None and mounts cleanly
    sock_empty = _PluginSocket()
    runner_empty = asyncio.create_task(
        bus.serve(
            sock_empty.receive,
            sock_empty.send,
            sock_empty.close,
            source_ip="127.0.0.1",
            plugin_id="demo_plug",
        )
    )
    await sock_empty.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Demo Plug",
            "scopes": ["actions"],
            "actions": [{"name": "demo_plug.stop", "description": "Exit active session"}],
            "session_exit_action": "",
        }
    )
    await sock_empty.recv_until(lambda m: m.get("type") == "plugin.ready")
    proxy_empty = svc.modules.get("demo_plug")
    assert isinstance(proxy_empty, RemoteModuleProxy)
    assert proxy_empty.session_exit_action is None
    await sock_empty.close(1000)
    await asyncio.wait_for(runner_empty, 2.0)

    # 3. Valid session_exit_action with defaultable args mounts and exposes attribute
    sock_ok = _PluginSocket()
    runner_ok = asyncio.create_task(
        bus.serve(
            sock_ok.receive,
            sock_ok.send,
            sock_ok.close,
            source_ip="127.0.0.1",
            plugin_id="demo_plug",
        )
    )
    await sock_ok.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Demo Plug",
            "scopes": ["actions"],
            "actions": [
                {
                    "name": "demo_plug.stop",
                    "description": "Exit active session",
                    "arguments": {
                        "type": "object",
                        "properties": {"graceful": {"type": "boolean", "default": True}},
                        "required": [],
                        "additionalProperties": False,
                    },
                }
            ],
            "session_exit_action": "demo_plug.stop",
        }
    )
    await sock_ok.recv_until(lambda m: m.get("type") == "plugin.ready")
    proxy_ok = svc.modules.get("demo_plug")
    assert isinstance(proxy_ok, RemoteModuleProxy)
    assert proxy_ok.session_exit_action == "demo_plug.stop"
    await sock_ok.close(1000)
    await asyncio.wait_for(runner_ok, 2.0)


async def test_plugin_bus_action_run_forwards_exit_reason_only_when_present(
    exit_bus_env: Any,
) -> None:
    _, svc, bus, _ = exit_bus_env
    await bus.create_plugin("demo_plug", "Demo Plug", ["actions"])

    sock = _PluginSocket()
    runner = asyncio.create_task(
        bus.serve(
            sock.receive,
            sock.send,
            sock.close,
            source_ip="127.0.0.1",
            plugin_id="demo_plug",
        )
    )
    await sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Demo Plug",
            "scopes": ["actions"],
            "actions": [{"name": "demo_plug.stop", "description": "Exit active session"}],
            "session_exit_action": "demo_plug.stop",
        }
    )
    await sock.recv_until(lambda m: m.get("type") == "plugin.ready")
    spec = svc.modules.catalog.actions["demo_plug.stop"]

    try:
        # Omitted or whitespace-only exit_reason must NOT appear in RPC params
        for ticket in (
            {"origin": "timer", "target_scope": ""},
            {"origin": "timer", "target_scope": "", "exit_reason": "   "},
        ):
            t = asyncio.create_task(spec.run(ActionCall(ticket, spec.arguments(), 100.0)))
            req = (await sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
            assert "exit_reason" not in req["params"]
            await sock.push(
                {"type": "rpc.result", "id": req["id"], "ok": True, "result": {"status": "done"}}
            )
            out = await t
            assert out.status == "done"

        # Non-empty exit_reason is sanitized, truncated to <=32 chars, and forwarded
        secret = configured_secrets(svc.s)[0]
        raw_reason = f"sleeping {secret} " + "x" * 50
        t2 = asyncio.create_task(
            spec.run(
                ActionCall(
                    {"origin": "timer", "target_scope": "", "exit_reason": raw_reason},
                    spec.arguments(),
                    100.0,
                )
            )
        )
        req2 = (await sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
        assert "exit_reason" in req2["params"]
        assert secret not in req2["params"]["exit_reason"]
        assert len(req2["params"]["exit_reason"]) <= 32
        await sock.push(
            {"type": "rpc.result", "id": req2["id"], "ok": True, "result": {"status": "done"}}
        )
        assert (await t2).status == "done"

        # Standard reason passes intact
        t3 = asyncio.create_task(
            spec.run(
                ActionCall(
                    {"origin": "timer", "target_scope": "", "exit_reason": "day_rest"},
                    spec.arguments(),
                    100.0,
                )
            )
        )
        req3 = (await sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
        assert req3["params"]["exit_reason"] == "day_rest"
        await sock.push(
            {"type": "rpc.result", "id": req3["id"], "ok": True, "result": {"status": "done"}}
        )
        assert (await t3).status == "done"
    finally:
        await sock.close(1000)
        await asyncio.wait_for(runner, 2.0)


async def test_minecraft_plugin_declares_exit_action_goodbye_and_recap(
    exit_bus_env: Any,
) -> None:
    _, svc, bus, _ = exit_bus_env
    nt = FakeNodeTransport(is_connected=True)
    nt.responses["mc.status"] = {
        "connected": False,
        "bridge_ready": True,
        "mc_enabled": True,
        "mc_configured": True,
        "paused": False,
    }
    nt.responses["mc.dispatch_goal"] = lambda params: {
        "ok": True,
        "accepted": True,
        "connected": params.get("action_type") != "disconnect",
        "activity": "待机" if params.get("action_type") == "disconnect" else "在线",
    }
    svc.node_transport = nt

    token, _ = await bus.create_plugin(
        "minecraft",
        "Minecraft",
        ["actions", "sensors", "node_bridge", "thinking_scenes"],
        node_region="CN",
    )
    pipe = _BiPipe(bus, "minecraft")
    now_box = [NOW]
    plugin = MinecraftPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        clock=lambda: now_box[0],
        connect_factory=pipe.connect_factory,
        enable_brain=True,
        brain_config=McBrainConfig(
            cruise_interval_seconds=300.0, solo_cruise_interval_seconds=300.0
        ),
    )

    assert plugin.client.config.session_exit_action == "mc.disconnect"
    assert plugin.client.hello_payload()["session_exit_action"] == "mc.disconnect"

    await plugin.client.start()
    try:
        assert await plugin.client.wait_connected(timeout=2.0)
        await _wait_until(lambda: isinstance(svc.modules.get("minecraft"), RemoteModuleProxy))
        proxy = svc.modules.get("minecraft")
        assert isinstance(proxy, RemoteModuleProxy)
        assert proxy.session_exit_action == "mc.disconnect"

        connect_spec = svc.modules.catalog.actions["mc.connect"]
        disconnect_spec = svc.modules.catalog.actions["mc.disconnect"]
        assert "白天精力充沛" in connect_spec.description
        assert "精力偏低或准备换其他活动时使用" in disconnect_spec.description

        # Capture emitted world events cleanly without breaking normal plugin operation
        emitted_recaps: list[tuple[dict[str, Any], dict[str, Any]]] = []
        orig_emit = plugin.client.emit_world_event

        async def _capture_emit(event_type: str, payload: dict[str, Any] | None = None, **kw: Any):
            if event_type == "mc_session_recap":
                emitted_recaps.append((dict(payload or {}), dict(kw)))
            return await orig_emit(event_type, payload, **kw)

        plugin.client.emit_world_event = _capture_emit  # type: ignore[method-assign]

        # 1. sleeping + nearby_players -> sleeping goodbye, disconnect, recap("sleeping")
        await plugin.update_snapshot(
            {
                "connected": True,
                "bridge_ready": True,
                "mc_enabled": True,
                "mc_configured": True,
                "nearby_players": ["M1haiier"],
                "updated_at": now_box[0],
            }
        )
        now_box[0] += 90.0
        nt.calls.clear()
        out_sleep = await disconnect_spec.run(
            ActionCall(
                {"origin": "timer", "target_scope": "", "exit_reason": "sleeping"},
                disconnect_spec.arguments(),
                now_box[0],
            )
        )
        assert out_sleep.status == "done"
        dispatch_calls = [p for a, p in nt.calls if a == "mc.dispatch_goal"]
        assert dispatch_calls == [
            {"action_type": "chat", "message": "时间不早啦，猫猫困了先下线睡觉去咯，晚安喵~"},
            {"action_type": "disconnect", "sleep_server": False},
        ]
        assert len(emitted_recaps) == 1
        assert emitted_recaps[-1][0]["reason"] == "sleeping"
        assert "晚安喵~" in emitted_recaps[-1][1]["summary"]

        # 2. day_rest + nearby_players -> day_rest goodbye, disconnect, recap("day_rest")
        emitted_recaps.clear()
        await plugin.update_snapshot(
            {
                "connected": True,
                "bridge_ready": True,
                "mc_enabled": True,
                "mc_configured": True,
                "nearby_players": ["M1haiier"],
                "updated_at": now_box[0],
            }
        )
        now_box[0] += 90.0
        nt.calls.clear()
        out_rest = await disconnect_spec.run(
            ActionCall(
                {"origin": "timer", "target_scope": "", "exit_reason": "day_rest"},
                disconnect_spec.arguments(),
                now_box[0],
            )
        )
        assert out_rest.status == "done"
        dispatch_calls = [p for a, p in nt.calls if a == "mc.dispatch_goal"]
        assert dispatch_calls == [
            {"action_type": "chat", "message": "猫猫有点累了，先下线小憩一会儿，待会儿见喵~"},
            {"action_type": "disconnect", "sleep_server": False},
        ]
        assert len(emitted_recaps) == 1
        assert emitted_recaps[-1][0]["reason"] == "day_rest"
        assert "小憩一会儿" in emitted_recaps[-1][1]["summary"]

        # 3. Solo sleeping (no nearby_players) -> no goodbye chat, disconnect + recap("sleeping")
        emitted_recaps.clear()
        await plugin.update_snapshot(
            {
                "connected": True,
                "bridge_ready": True,
                "mc_enabled": True,
                "mc_configured": True,
                "nearby_players": [],
                "updated_at": now_box[0],
            }
        )
        plugin.brain._append_milestone("挖到煤炭")
        now_box[0] += 90.0
        nt.calls.clear()
        out_solo = await disconnect_spec.run(
            ActionCall(
                {"origin": "timer", "target_scope": "", "exit_reason": "sleeping"},
                disconnect_spec.arguments(),
                now_box[0],
            )
        )
        assert out_solo.status == "done"
        assert [p for a, p in nt.calls if a == "mc.dispatch_goal"] == [
            {"action_type": "disconnect", "sleep_server": False}
        ]
        assert len(emitted_recaps) == 1
        assert emitted_recaps[-1][0]["reason"] == "sleeping"

        # 4. Normal disconnect (no exit_reason) -> no goodbye chat, recap("disconnect")
        emitted_recaps.clear()
        await plugin.update_snapshot(
            {
                "connected": True,
                "bridge_ready": True,
                "mc_enabled": True,
                "mc_configured": True,
                "nearby_players": ["M1haiier"],
                "updated_at": now_box[0],
            }
        )
        now_box[0] += 90.0
        nt.calls.clear()
        out_normal = await disconnect_spec.run(
            ActionCall(
                {"origin": "planner", "target_scope": ""},
                disconnect_spec.arguments(),
                now_box[0],
            )
        )
        assert out_normal.status == "done"
        assert [p for a, p in nt.calls if a == "mc.dispatch_goal"] == [
            {"action_type": "disconnect", "sleep_server": False}
        ]
        assert len(emitted_recaps) == 1
        assert emitted_recaps[-1][0]["reason"] == "disconnect"

        # 5. Goodbye chat RPC failure must NEVER block disconnect
        emitted_recaps.clear()
        await plugin.update_snapshot(
            {
                "connected": True,
                "bridge_ready": True,
                "mc_enabled": True,
                "mc_configured": True,
                "nearby_players": ["M1haiier"],
                "updated_at": now_box[0],
            }
        )
        now_box[0] += 90.0
        orig_call_bridge = plugin._call_bridge
        bridge_calls: list[dict[str, Any]] = []

        async def _flaky_chat_bridge(action: str, params: dict[str, Any]) -> dict[str, Any]:
            bridge_calls.append({"action": action, **params})
            if action == "mc.dispatch_goal" and params.get("action_type") == "chat":
                raise PluginRpcError("mc_bridge_timeout", "chat timed out")
            return await orig_call_bridge(action, params)

        plugin._call_bridge = _flaky_chat_bridge  # type: ignore[method-assign]
        try:
            out_fail = await disconnect_spec.run(
                ActionCall(
                    {"origin": "timer", "target_scope": "", "exit_reason": "sleeping"},
                    disconnect_spec.arguments(),
                    now_box[0],
                )
            )
            assert out_fail.status == "done"
            dispatched_types = [
                c["action_type"] for c in bridge_calls if c["action"] == "mc.dispatch_goal"
            ]
            assert dispatched_types == [
                "chat",
                "disconnect",
            ]
            assert len(emitted_recaps) == 1
            assert emitted_recaps[-1][0]["reason"] == "sleeping"
            assert "晚安喵~" not in emitted_recaps[-1][1]["summary"]
        finally:
            plugin._call_bridge = orig_call_bridge  # type: ignore[method-assign]
    finally:
        await plugin.close()
        await pipe.shutdown()
