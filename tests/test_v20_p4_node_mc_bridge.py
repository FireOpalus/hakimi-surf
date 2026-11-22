"""Unit and loopback integration tests for Phase 4 CN MC bridge slot."""

from __future__ import annotations

import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from websockets.asyncio.server import serve

from node_agent.bili_collector import BiliCollector
from node_agent.config import NodeAgentSettings
from node_agent.daemon import CAPABILITIES, NodeDaemon
from node_agent.mc_bridge_slot import McBridgeSlot

NODE_TOKEN = "cn-node-test-secret-token-0123456789abcdef"
MC_TOKEN = "mc-bridge-test-secret-token-0123456789abcdef"


def _make_settings(tmp_path: Path, **overrides: Any) -> NodeAgentSettings:
    defaults: dict[str, Any] = {
        "node_ws_url": "ws://127.0.0.1:18443/ws/node/0123456789abcdef01234567",
        "node_ws_token": NODE_TOKEN,
        "node_id": "shanghai-cn-1",
        "node_region": "CN",
        "reconnect_max": 0.25,
        "bili_min_interval": 0.0,
        "temp_dir": tmp_path / "node_bili_jobs",
        "mc_bridge_enabled": False,
        "mc_bridge_url": "ws://hakimi-mc-agent:8791/ws",
        "mc_bridge_token": MC_TOKEN,
        "mc_rpc_timeout": 15.0,
    }
    defaults.update(overrides)
    return NodeAgentSettings(_env_file=None, **defaults)


def test_mc_bridge_config_validation_ssrf_and_aliases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    disabled_cfg = _make_settings(tmp_path, mc_bridge_enabled=False, mc_bridge_token="")
    assert disabled_cfg.mc_bridge_enabled is False
    assert disabled_cfg.mc_bridge_url == "ws://hakimi-mc-agent:8791/ws"

    for good_url in (
        "ws://hakimi-mc-agent:8791/ws",
        "ws://127.0.0.1:8791/ws",
        "ws://localhost:8791/ws",
        "ws://[::1]:8791/ws",
        "wss://mc.example.com/ws",
    ):
        cfg = _make_settings(tmp_path, mc_bridge_enabled=True, mc_bridge_url=good_url)
        assert cfg.mc_bridge_url == good_url

    for bad_url in (
        "ws://169.254.169.254/latest/meta-data",
        "ws://10.0.0.5:8791/ws",
        "ws://evil.example:8791/ws",
        "ws://hakimi-mc-agent.evil.com:8791/ws",
        "http://hakimi-mc-agent:8791/ws",
        "wss://user:pass@hakimi-mc-agent:8791/ws",
        "wss://mc.example.com/ws?token=abc",
        "wss://mc.example.com/ws#frag",
        "ws://hakimi-mc-agent:8791/ws\nX-Inject: 1",
    ):
        with pytest.raises(ValidationError):
            _make_settings(tmp_path, mc_bridge_enabled=True, mc_bridge_url=bad_url)

    for bad_token in (
        "",
        "short",
        "GENERATE_32+_CHAR_RANDOM_MC_BRIDGE_TOKEN_BEFORE_ENABLE",
        "REPLACE_WITH_32+_CHAR_RANDOM_TOKEN_VALUE_HERE",
        "a" * 31 + " ",
        "a" * 31 + "\n",
    ):
        with pytest.raises(ValidationError):
            _make_settings(tmp_path, mc_bridge_enabled=True, mc_bridge_token=bad_token)

    for bad_timeout in (0.5, 61.0):
        with pytest.raises(ValidationError):
            _make_settings(tmp_path, mc_bridge_enabled=True, mc_rpc_timeout=bad_timeout)

    monkeypatch.setenv("NODE_WS_URL", "ws://127.0.0.1:18443/ws/node/0123456789abcdef01234567")
    monkeypatch.setenv("NODE_WS_TOKEN", NODE_TOKEN)
    monkeypatch.setenv("MC_BRIDGE_ENABLED", "true")
    monkeypatch.setenv("MC_BRIDGE_URL", "ws://127.0.0.1:8791/ws")
    monkeypatch.setenv("MC_BRIDGE_TOKEN", MC_TOKEN)
    monkeypatch.setenv("MC_RPC_TIMEOUT", "12.5")
    env_cfg = NodeAgentSettings(_env_file=None)
    assert env_cfg.mc_bridge_enabled is True
    assert env_cfg.mc_bridge_url == "ws://127.0.0.1:8791/ws"
    assert env_cfg.mc_bridge_token.get_secret_value() == MC_TOKEN
    assert env_cfg.mc_rpc_timeout == 12.5


def test_compose_and_env_example_mc_hardening() -> None:
    repo = Path(__file__).resolve().parents[1]
    compose_text = (repo / "node_agent/compose.cn.yaml").read_text(encoding="utf-8")
    for expected in (
        "hakimi_cn_net:",
        "hakimi-mc-agent:",
        "image: hakimi-mc-agent:v2.1.0",
        "env_file: .env.mc",
        "mem_limit: 768m",
        "cpus: 1.5",
        "pids_limit: 128",
        "expose:",
        '"8791"',
    ):
        assert expected in compose_text
    assert not re.search(r"(?m)^\s+ports:\s*$", compose_text)

    env_example = (repo / "node_agent/.env.node.example").read_text(encoding="utf-8")
    for key in (
        "MC_BRIDGE_ENABLED=false",
        "MC_BRIDGE_URL=ws://hakimi-mc-agent:8791/ws",
        "MC_BRIDGE_TOKEN=GENERATE_",
        "MC_RPC_TIMEOUT=15.0",
    ):
        assert key in env_example
    if live_tok := os.environ.get("MC_BRIDGE_TOKEN"):
        assert live_tok not in env_example


async def test_mc_bridge_stub_compat() -> None:
    stub = McBridgeSlot()
    stub.start()
    try:
        st = await stub.status()
        assert st["bridge_ready"] is False
        assert st["phase"] == "phase4_stub"
        goal = await stub.dispatch_goal({"action_type": "explore"})
        assert goal == {
            "ok": False,
            "accepted": False,
            "bridge_ready": False,
            "paused": False,
            "reason": "mc_bridge_not_configured",
        }
        p = await stub.pause()
        assert p["ok"] is True and p["paused"] is True and p["bridge_ready"] is False
        goal_paused = await stub.dispatch_goal({"action_type": "explore"})
        assert goal_paused["reason"] == "mc_paused"
        r = await stub.resume()
        assert r["ok"] is True and r["paused"] is False and r["bridge_ready"] is False
        snap = stub.current_snapshot()
        assert snap["connected"] is False
        assert snap["bridge_ready"] is False
    finally:
        await stub.close()


async def test_mc_bridge_hello_frame_validation_and_malformed_json() -> None:
    events: list[tuple[str, dict[str, Any]]] = []
    observed_auth: list[str] = []
    ready_evt = asyncio.Event()

    async def on_event(name: str, payload: dict[str, Any]) -> None:
        events.append((name, payload))

    async def sidecar_handler(ws: Any) -> None:
        observed_auth.append(ws.request.headers.get("authorization", ""))
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.hello",
                    "ts": 100.0,
                    "payload": {
                        "version": "2.0.0",
                        "bridge_ready": True,
                        "mc_enabled": True,
                        "mc_configured": True,
                        "paused": False,
                        "snapshot": {
                            "connected": True,
                            "dimension": "nether",
                            "pos": [1, 64, 2],
                            "health": 19,
                            "food": 18,
                            "activity": "mining_quartz",
                            "nearby_entities": ["piglin"],
                            "recent_chat_or_sign": "sign",
                            "updated_at": 100.0,
                        },
                    },
                }
            )
        )
        # Malformed frames must be rejected without crashing the bridge loop.
        await ws.send('{"type":"event","type":"event"}')
        await ws.send('{"type":"event","ts":NaN}')
        await ws.send("[1, 2, 3]")
        await ws.send(json.dumps({"type": "event", "event": "mc.snapshot", "payload": "bad"}))
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.milestone",
                    "ts": 101.0,
                    "payload": {
                        "event_type": "mc_rare_find",
                        "importance": "high",
                        "payload": {"block": "ancient_debris"},
                    },
                }
            )
        )
        ready_evt.set()
        await ws.wait_closed()

    async with serve(sidecar_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        slot = McBridgeSlot(
            enabled=True,
            bridge_url=f"ws://127.0.0.1:{port}/ws",
            token=MC_TOKEN,
            max_frame_bytes=262_144,
            reconnect_base=0.02,
            reconnect_max=0.05,
            on_event=on_event,
        )
        slot.start()
        try:
            await asyncio.wait_for(ready_evt.wait(), timeout=2.0)
            for _ in range(40):
                if any(name == "mc.milestone" for name, _ in events):
                    break
                await asyncio.sleep(0.01)

            assert observed_auth == [f"Bearer {MC_TOKEN}"]
            snap = slot.current_snapshot()
            assert snap["bridge_ready"] is True
            assert snap["connected"] is True
            assert snap["mc_enabled"] is True
            assert snap["dimension"] == "nether"
            assert snap["activity"] == "mining_quartz"
            # mc.hello updates cached snapshot without emitting "mc.hello" to on_event.
            assert [name for name, _ in events] == ["mc.milestone"]
            assert events[0][1]["event_type"] == "mc_rare_find"
        finally:
            await slot.close()


async def test_serve_session_emits_cached_hello_snapshot_immediately(tmp_path: Path) -> None:
    hello_processed = asyncio.Event()

    async def sidecar_handler(ws: Any) -> None:
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.hello",
                    "ts": 140.0,
                    "payload": {
                        "version": "2.0.0",
                        "bridge_ready": True,
                        "mc_enabled": False,
                        "mc_configured": False,
                        "paused": False,
                        "snapshot": {
                            "connected": False,
                            "dimension": "overworld",
                            "pos": None,
                            "health": None,
                            "food": None,
                            "activity": "待机",
                            "nearby_entities": [],
                            "recent_chat_or_sign": "",
                            "updated_at": 140.0,
                        },
                    },
                }
            )
        )
        await ws.wait_closed()

    class FakeUpstreamWS:
        def __init__(self) -> None:
            self.sent: list[dict[str, Any]] = []

        async def send(self, raw: str) -> None:
            self.sent.append(json.loads(raw))

        def __aiter__(self) -> FakeUpstreamWS:
            return self

        async def __anext__(self) -> str:
            raise StopAsyncIteration

    async with serve(sidecar_handler, "127.0.0.1", 0) as sidecar_srv:
        sidecar_port = sidecar_srv.sockets[0].getsockname()[1]
        cfg = _make_settings(
            tmp_path,
            mc_bridge_enabled=True,
            mc_bridge_url=f"ws://127.0.0.1:{sidecar_port}/ws",
        )
        daemon = NodeDaemon(cfg, collector=BiliCollector(min_interval=0.0, temp_dir=tmp_path))
        daemon.mc_slot.start()
        try:
            for _ in range(60):
                if daemon.mc_slot.current_snapshot()["bridge_ready"]:
                    hello_processed.set()
                    break
                await asyncio.sleep(0.01)
            assert hello_processed.is_set()

            fake_ws = FakeUpstreamWS()
            await daemon.serve_session(fake_ws)
            assert len(fake_ws.sent) == 2
            assert fake_ws.sent[0]["event"] == "node.hello"
            assert fake_ws.sent[0]["payload"]["capabilities"] == [*CAPABILITIES, "mc.bridge"]
            assert fake_ws.sent[1]["event"] == "mc.snapshot"
            assert fake_ws.sent[1]["payload"]["bridge_ready"] is True
            assert fake_ws.sent[1]["payload"]["activity"] == "待机"
        finally:
            await daemon.mc_slot.close()


async def test_daemon_auto_wires_mc_slot_from_settings_and_initial_snapshot(
    tmp_path: Path,
) -> None:
    sidecar_ready = asyncio.Event()
    upstream_events: list[dict[str, Any]] = []
    upstream_ready_snap = asyncio.Event()

    async def sidecar_handler(ws: Any) -> None:
        snap = {
            "connected": False,
            "bridge_ready": True,
            "mc_enabled": False,
            "mc_configured": False,
            "paused": False,
            "dimension": "overworld",
            "pos": None,
            "health": None,
            "food": None,
            "activity": "待机",
            "nearby_entities": [],
            "recent_chat_or_sign": "",
            "updated_at": 150.0,
        }
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.hello",
                    "ts": 150.0,
                    "payload": {
                        "version": "2.0.0",
                        "bridge_ready": True,
                        "mc_enabled": False,
                        "mc_configured": False,
                        "paused": False,
                        "snapshot": snap,
                    },
                }
            )
        )
        await ws.send(
            json.dumps({"type": "event", "event": "mc.snapshot", "ts": 150.1, "payload": snap})
        )
        sidecar_ready.set()
        await ws.wait_closed()

    async def upstream_handler(ws: Any) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            upstream_events.append(msg)
            if (
                msg.get("event") == "mc.snapshot"
                and isinstance(msg.get("payload"), dict)
                and msg["payload"].get("bridge_ready") is True
            ):
                upstream_ready_snap.set()

    async with (
        serve(sidecar_handler, "127.0.0.1", 0) as sidecar_srv,
        serve(upstream_handler, "127.0.0.1", 0) as upstream_srv,
    ):
        sidecar_port = sidecar_srv.sockets[0].getsockname()[1]
        upstream_port = upstream_srv.sockets[0].getsockname()[1]
        cfg = _make_settings(
            tmp_path,
            node_ws_url=f"ws://127.0.0.1:{upstream_port}/ws/node/0123456789abcdef01234567",
            mc_bridge_enabled=True,
            mc_bridge_url=f"ws://127.0.0.1:{sidecar_port}/ws",
            mc_rpc_timeout=2.0,
        )
        daemon = NodeDaemon(cfg, collector=BiliCollector(min_interval=0.0, temp_dir=tmp_path))
        assert daemon.mc_slot.enabled is True
        assert daemon.mc_slot.bridge_url == cfg.mc_bridge_url
        assert daemon.mc_slot.on_event == daemon._forward_mc_event

        run_task = asyncio.create_task(daemon.run())
        try:
            await asyncio.wait_for(sidecar_ready.wait(), timeout=2.0)
            await asyncio.wait_for(upstream_ready_snap.wait(), timeout=2.0)
            assert upstream_events[0]["event"] == "node.hello"
            assert upstream_events[0]["payload"]["capabilities"] == [*CAPABILITIES, "mc.bridge"]
            assert upstream_events[1]["event"] == "mc.snapshot"
            assert any(
                ev.get("event") == "mc.snapshot" and ev["payload"].get("bridge_ready") is True
                for ev in upstream_events[1:]
            )
        finally:
            await daemon.stop()
            await asyncio.wait_for(run_task, timeout=3.0)


async def test_daemon_live_bridge_forwarding_multiplex_and_reconnect(tmp_path: Path) -> None:
    sidecar_seen_ids: list[str] = []
    sidecar_connected = asyncio.Event()
    drop_sidecar = asyncio.Event()
    sidecar_mode = {"conn": 0, "hang_dispatch": False, "drop_mid_rpc": False}

    async def sidecar_handler(ws: Any) -> None:
        sidecar_mode["conn"] += 1
        conn_num = sidecar_mode["conn"]
        assert ws.request.headers.get("authorization") == f"Bearer {MC_TOKEN}"

        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.hello",
                    "ts": 200.0,
                    "payload": {
                        "version": "2.0.0",
                        "bridge_ready": True,
                        "mc_enabled": True,
                        "mc_configured": True,
                        "paused": False,
                        "snapshot": {
                            "connected": True,
                            "dimension": "overworld",
                            "pos": [10, 64, -5],
                            "health": 20,
                            "food": 20,
                            "activity": "exploring",
                            "nearby_entities": ["cow"],
                            "recent_chat_or_sign": "",
                            "updated_at": 200.0,
                        },
                    },
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.snapshot",
                    "ts": 200.1,
                    "payload": {
                        "connected": True,
                        "bridge_ready": True,
                        "mc_enabled": True,
                        "mc_configured": True,
                        "paused": False,
                        "dimension": "overworld",
                        "pos": [10, 64, -5],
                        "health": 20,
                        "food": 20,
                        "activity": "exploring",
                        "nearby_entities": ["cow"],
                        "recent_chat_or_sign": "",
                        "updated_at": 200.1,
                    },
                }
            )
        )
        if conn_num == 1:
            await ws.send(
                json.dumps(
                    {
                        "type": "event",
                        "event": "mc.milestone",
                        "ts": 200.2,
                        "payload": {
                            "event_type": "mc_rare_find",
                            "importance": "high",
                            "payload": {"block": "diamond_ore", "pos": [10, -40, -5]},
                        },
                    }
                )
            )
        sidecar_connected.set()

        delayed_status: dict[str, Any] | None = None

        async def handle_requests() -> None:
            nonlocal delayed_status
            async for raw in ws:
                req = json.loads(raw)
                assert req["type"] == "rpc_request"
                req_id = req["id"]
                assert len(req_id) == 32
                sidecar_seen_ids.append(req_id)
                action = req["action"]
                if action == "mc.status":
                    delayed_status = req
                    continue
                if action == "mc.pause":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "rpc_response",
                                "id": req_id,
                                "ok": True,
                                "data": {
                                    "ok": True,
                                    "connected": True,
                                    "paused": True,
                                    "bridge_ready": True,
                                },
                            }
                        )
                    )
                    if delayed_status is not None:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "rpc_response",
                                    "id": delayed_status["id"],
                                    "ok": True,
                                    "data": {
                                        "connected": True,
                                        "paused": True,
                                        "bridge_ready": True,
                                        "mc_enabled": True,
                                        "mc_configured": True,
                                        "activity": "exploring",
                                        "phase": "phase4_live",
                                        "updated_at": 201.0,
                                    },
                                }
                            )
                        )
                        delayed_status = None
                elif action == "mc.resume":
                    await ws.send(
                        json.dumps(
                            {
                                "type": "rpc_response",
                                "id": req_id,
                                "ok": True,
                                "data": {
                                    "ok": True,
                                    "connected": True,
                                    "paused": False,
                                    "bridge_ready": True,
                                },
                            }
                        )
                    )
                elif action == "mc.dispatch_goal":
                    if sidecar_mode["hang_dispatch"]:
                        await asyncio.sleep(5.0)
                    if sidecar_mode.get("drop_mid_rpc"):
                        await ws.close()
                        return
                    if isinstance(req.get("params"), dict) and "host" in req["params"]:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "rpc_response",
                                    "id": req_id,
                                    "ok": False,
                                    "data": {},
                                    "error": "mc_target_override_forbidden",
                                }
                            )
                        )
                        continue
                    await ws.send(
                        json.dumps(
                            {
                                "type": "rpc_response",
                                "id": req_id,
                                "ok": True,
                                "data": {
                                    "ok": True,
                                    "accepted": True,
                                    "bridge_ready": True,
                                    "paused": False,
                                    "summary": "筹备建造:红石门",
                                },
                            }
                        )
                    )

        req_task = asyncio.create_task(handle_requests())
        try:
            if conn_num == 1:
                await drop_sidecar.wait()
                await ws.close()
            else:
                await ws.wait_closed()
        finally:
            req_task.cancel()
            await asyncio.gather(req_task, return_exceptions=True)

    upstream_events: list[dict[str, Any]] = []
    upstream_rpcs: dict[str, dict[str, Any]] = {}
    upstream_rpc_events: dict[str, asyncio.Event] = {}
    upstream_connected = asyncio.Event()
    upstream_ws_holder: dict[str, Any] = {}

    async def upstream_handler(ws: Any) -> None:
        upstream_ws_holder["ws"] = ws
        upstream_connected.set()
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") == "event":
                upstream_events.append(msg)
            elif msg.get("type") == "rpc_response":
                rid = msg["id"]
                upstream_rpcs[rid] = msg
                if rid in upstream_rpc_events:
                    upstream_rpc_events[rid].set()

    async def call_upstream(action: str, params: dict[str, Any], rid: str) -> dict[str, Any]:
        evt = asyncio.Event()
        upstream_rpc_events[rid] = evt
        await upstream_ws_holder["ws"].send(
            json.dumps({"type": "rpc_request", "id": rid, "action": action, "params": params})
        )
        await asyncio.wait_for(evt.wait(), timeout=3.0)
        return upstream_rpcs[rid]

    sidecar_server = await serve(sidecar_handler, "127.0.0.1", 0)
    sidecar_port = sidecar_server.sockets[0].getsockname()[1]
    upstream_server = await serve(upstream_handler, "127.0.0.1", 0)
    upstream_port = upstream_server.sockets[0].getsockname()[1]

    cfg = _make_settings(
        tmp_path,
        node_ws_url=f"ws://127.0.0.1:{upstream_port}/ws/node/0123456789abcdef01234567",
        mc_bridge_enabled=True,
        mc_bridge_url=f"ws://127.0.0.1:{sidecar_port}/ws",
        mc_rpc_timeout=1.0,
    )
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "jobs")
    mc_slot = McBridgeSlot(
        enabled=True,
        bridge_url=cfg.mc_bridge_url,
        token=MC_TOKEN,
        rpc_timeout=0.25,
        reconnect_base=0.02,
        reconnect_max=0.06,
    )
    daemon = NodeDaemon(cfg, collector=collector, mc_slot=mc_slot, reconnect_base=0.05)
    run_task = asyncio.create_task(daemon.run())

    try:
        await asyncio.wait_for(upstream_connected.wait(), timeout=3.0)
        await asyncio.wait_for(sidecar_connected.wait(), timeout=3.0)

        for _ in range(50):
            if any(ev["event"] == "mc.milestone" for ev in upstream_events):
                break
            await asyncio.sleep(0.01)

        hello = upstream_events[0]
        assert hello["event"] == "node.hello"
        assert hello["payload"]["capabilities"] == [*CAPABILITIES, "mc.bridge"]
        # serve_session emits an initial mc.snapshot immediately after node.hello.
        assert upstream_events[1]["event"] == "mc.snapshot"
        assert any(
            ev["event"] == "mc.snapshot" and ev["payload"]["connected"] is True
            for ev in upstream_events
        )
        assert any(
            ev["event"] == "mc.milestone"
            and ev["payload"]["event_type"] == "mc_rare_find"
            and ev["node_id"] == "shanghai-cn-1"
            and ev["region"] == "CN"
            for ev in upstream_events
        )

        # 1. Concurrent out-of-order RPC multiplexing: mc.status (delayed) + mc.pause.
        res_status, res_pause = await asyncio.gather(
            call_upstream("mc.status", {}, "up-status-1"),
            call_upstream("mc.pause", {}, "up-pause-1"),
        )
        assert res_pause["ok"] is True and res_pause["data"]["paused"] is True
        assert res_status["ok"] is True and res_status["data"]["bridge_ready"] is True
        assert res_status["data"]["phase"] == "phase4_live"
        assert len(set(sidecar_seen_ids)) == len(sidecar_seen_ids)

        # 2. Resume, valid dispatch_goal, and sidecar error propagation.
        res_resume = await call_upstream("mc.resume", {}, "up-resume-1")
        assert res_resume["ok"] is True and res_resume["data"]["paused"] is False
        res_goal = await call_upstream(
            "mc.dispatch_goal", {"action_type": "build_plan", "topic": "红石门"}, "up-goal-1"
        )
        assert res_goal["ok"] is True and res_goal["data"]["accepted"] is True

        res_ssrf = await call_upstream(
            "mc.dispatch_goal",
            {"action_type": "connect", "host": "169.254.169.254"},
            "up-ssrf-1",
        )
        assert res_ssrf["ok"] is True
        assert res_ssrf["data"]["ok"] is False
        assert res_ssrf["data"]["reason"] == "mc_target_override_forbidden"

        # 3. Timeout resilience: sidecar hangs > rpc_timeout (0.25s) without crashing node_agent.
        sidecar_mode["hang_dispatch"] = True
        res_timeout = await call_upstream(
            "mc.dispatch_goal", {"action_type": "explore"}, "up-timeout-1"
        )
        assert res_timeout["ok"] is True
        assert res_timeout["data"]["ok"] is False
        assert res_timeout["data"]["reason"] == "mc_rpc_timeout"
        sidecar_mode["hang_dispatch"] = False

        # 4. Mid-flight disconnect fails pending RPC immediately and triggers synthetic snapshot.
        sidecar_connected.clear()
        sidecar_server.close()
        sidecar_mode["drop_mid_rpc"] = True
        res_mid_drop = await call_upstream(
            "mc.dispatch_goal", {"action_type": "explore"}, "up-mid-drop-1"
        )
        assert res_mid_drop["data"]["ok"] is False
        assert res_mid_drop["data"]["reason"] == "mc_bridge_disconnected"
        sidecar_mode["drop_mid_rpc"] = False
        drop_sidecar.set()
        await sidecar_server.wait_closed()

        for _ in range(60):
            if any(
                ev["event"] == "mc.snapshot" and ev["payload"].get("activity") == "MC容器重连中"
                for ev in upstream_events
            ):
                break
            await asyncio.sleep(0.01)

        synth = [
            ev
            for ev in upstream_events
            if ev["event"] == "mc.snapshot" and ev["payload"].get("activity") == "MC容器重连中"
        ][-1]
        assert synth["payload"]["connected"] is False
        assert synth["payload"]["bridge_ready"] is False
        assert synth["payload"]["paused"] is False
        assert synth["payload"]["pos"] is None

        res_disc_status = await call_upstream("mc.status", {}, "up-disc-status")
        assert res_disc_status["data"] == {
            "connected": False,
            "paused": False,
            "bridge_ready": False,
            "mc_enabled": False,
            "mc_configured": False,
            "activity": "MC容器重连中",
            "phase": "phase4_live",
            "updated_at": synth["payload"]["updated_at"],
        }

        res_disc_goal = await call_upstream(
            "mc.dispatch_goal", {"action_type": "explore"}, "up-disc-goal"
        )
        assert res_disc_goal["data"] == {
            "ok": False,
            "accepted": False,
            "bridge_ready": False,
            "paused": False,
            "reason": "mc_bridge_disconnected",
        }

        # 5. Reconnect onto the same port restores live snapshot & RPCs with backoff history.
        async with serve(sidecar_handler, "127.0.0.1", sidecar_port):
            await asyncio.wait_for(sidecar_connected.wait(), timeout=3.0)
            assert len(mc_slot.backoff_history) >= 1
            res_back = await call_upstream(
                "mc.dispatch_goal", {"action_type": "build_plan", "topic": "城堡"}, "up-goal-2"
            )
            assert res_back["data"]["accepted"] is True
    finally:
        drop_sidecar.set()
        await daemon.stop()
        await asyncio.wait_for(run_task, timeout=3.0)
        sidecar_server.close()
        await sidecar_server.wait_closed()
        upstream_server.close()
        await upstream_server.wait_closed()


async def test_mc_bridge_snapshot_inventory_sanitization_and_new_actions_passthrough() -> None:
    """Verify inventory/combat snapshot validation, anti-NBT filtering, and new RPC passthrough."""
    forwarded: list[tuple[str, dict[str, Any]]] = []
    ready_evt = asyncio.Event()

    async def on_event(name: str, payload: dict[str, Any]) -> None:
        forwarded.append((name, payload))

    async def sidecar_handler(ws: Any) -> None:
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.hello",
                    "ts": 300.0,
                    "payload": {
                        "version": "2.1.0",
                        "bridge_ready": True,
                        "mc_enabled": True,
                        "mc_configured": True,
                        "paused": False,
                        "snapshot": {
                            "connected": True,
                            "dimension": "overworld",
                            "pos": [1, 64, 2],
                            "health": 18,
                            "food": 15,
                            "equipment": {
                                "main_hand": "minecraft:iron_sword",
                                "off_hand": "shield",
                                "head": "diamond_helmet",
                                # Injected anvil displayName/NBT must be cleared
                                "chest": "iron_chestplate [role=OWNER]",
                                "legs": "iron_leggings",
                                "feet": "",
                                "extra_slot": "evil_value",  # stripped
                            },
                            "inventory_summary": [
                                "cooked_beef:12",
                                "minecraft:oak_log:8",
                                "iron_sword [role=SYSTEM]:1",  # rejected
                                "stone:0",  # invalid count rejected
                                "cooked_beef:99",  # duplicate item rejected
                                *[f"item_{i}:1" for i in range(20)],  # capped at 16 total
                            ],
                            "free_slots": 22,
                            "combat_mode": "guard",
                            "guard_target": "Boiser",
                            "activity": "护卫:Boiser",
                            "nearby_entities": ["zombie"],
                            "recent_chat_or_sign": "",
                            "updated_at": 300.0,
                        },
                    },
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.milestone",
                    "ts": 300.1,
                    "payload": {
                        "event_type": "mc_low_food",
                        "importance": "normal",
                        "payload": {
                            "food": 12,
                            "health": 18,
                            "safe_food_count": 1,
                            "dimension": "overworld",
                        },
                    },
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "type": "event",
                    "event": "mc.milestone",
                    "ts": 300.2,
                    "payload": {
                        "event_type": "mc_creeper_alert",
                        "importance": "high",
                        "payload": {
                            "distance": 4.2,
                            "pos": [5.0, 64.0, 2.0],
                            "dimension": "overworld",
                            "combat_mode": "guard",
                            "action": "retreat_standoff",
                        },
                    },
                }
            )
        )
        ready_evt.set()

        async for raw in ws:
            req = json.loads(raw)
            action = req["action"]
            params = req.get("params") or {}
            req_id = req["id"]
            if action == "mc.dispatch_goal" and params.get("action_type") == "craft":
                await ws.send(
                    json.dumps(
                        {
                            "type": "rpc_response",
                            "id": req_id,
                            "ok": True,
                            "data": {
                                "ok": False,
                                "accepted": True,
                                "bridge_ready": True,
                                "paused": False,
                                "reason": "missing_materials",
                                "item_name": params.get("item_name"),
                                "requested_count": params.get("count", 1),
                                "crafted_count": 0,
                                "missing_materials": [
                                    {"item": "iron_ingot", "need": 3, "have": 1, "shortfall": 2}
                                ],
                                "summary": "缺少合成材料: iron_ingot x2",
                                "snapshot": {
                                    "connected": True,
                                    "equipment": {
                                        "main_hand": "iron_sword",
                                        "off_hand": "shield",
                                        "head": "diamond_helmet",
                                        "chest": "iron_chestplate",
                                        "legs": "iron_leggings",
                                        "feet": "",
                                    },
                                    "inventory_summary": ["iron_ingot:1"],
                                    "free_slots": 35,
                                    "combat_mode": "guard",
                                    "guard_target": "Boiser",
                                },
                            },
                        }
                    )
                )
            elif action == "mc.dispatch_goal" and params.get("action_type") == "guard":
                # Response without nested snapshot: top-level combat_mode/guard_target must patch
                await ws.send(
                    json.dumps(
                        {
                            "type": "rpc_response",
                            "id": req_id,
                            "ok": True,
                            "data": {
                                "ok": True,
                                "accepted": True,
                                "bridge_ready": True,
                                "paused": False,
                                "combat_mode": "guard",
                                "guard_target": "Boiser",
                                "summary": "已切换护卫模式(守护Boiser)",
                            },
                        }
                    )
                )
            elif action == "mc.dispatch_goal" and params.get("action_type") == "evade":
                await ws.send(
                    json.dumps(
                        {
                            "type": "rpc_response",
                            "id": req_id,
                            "ok": True,
                            "data": {
                                "ok": True,
                                "accepted": True,
                                "bridge_ready": True,
                                "paused": False,
                                "combat_mode": "evade",
                                "guard_target": "",
                                "summary": "已切换躲避模式",
                                "snapshot": {
                                    "connected": True,
                                    "combat_mode": "evade",
                                    "guard_target": "ShouldBeCleared",
                                    "free_slots": 99,  # out-of-range -> None
                                },
                            },
                        }
                    )
                )

    async with serve(sidecar_handler, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        slot = McBridgeSlot(
            enabled=True,
            bridge_url=f"ws://127.0.0.1:{port}/ws",
            token=MC_TOKEN,
            reconnect_base=0.02,
            reconnect_max=0.05,
            on_event=on_event,
        )
        slot.start()
        try:
            await asyncio.wait_for(ready_evt.wait(), timeout=2.0)
            for _ in range(40):
                if len(forwarded) >= 2:
                    break
                await asyncio.sleep(0.01)

            snap = slot.current_snapshot()
            assert snap["equipment"] == {
                "main_hand": "iron_sword",
                "off_hand": "shield",
                "head": "diamond_helmet",
                "chest": "",
                "legs": "iron_leggings",
                "feet": "",
            }
            assert len(snap["inventory_summary"]) == 16
            assert snap["inventory_summary"][:2] == ["cooked_beef:12", "oak_log:8"]
            assert all("role=" not in entry for entry in snap["inventory_summary"])
            assert snap["free_slots"] == 22
            assert snap["combat_mode"] == "guard"
            assert snap["guard_target"] == "Boiser"

            # Verify milestones forwarded intact
            m_types = [p["event_type"] for n, p in forwarded if n == "mc.milestone"]
            assert m_types == ["mc_low_food", "mc_creeper_alert"]

            # craft RPC passthrough preserves missing_materials and updates sanitized snapshot
            craft_res = await slot.dispatch_goal(
                {"action_type": "craft", "item_name": "iron_pickaxe", "count": 1}
            )
            assert craft_res["ok"] is False
            assert craft_res["reason"] == "missing_materials"
            assert craft_res["missing_materials"] == [
                {"item": "iron_ingot", "need": 3, "have": 1, "shortfall": 2}
            ]
            assert craft_res["snapshot"]["equipment"]["chest"] == "iron_chestplate"
            assert slot.current_snapshot()["free_slots"] == 35

            # evade RPC clears guard_target and rejects out-of-range free_slots
            evade_res = await slot.dispatch_goal({"action_type": "evade"})
            assert evade_res["ok"] is True
            assert evade_res["snapshot"]["combat_mode"] == "evade"
            assert evade_res["snapshot"]["guard_target"] == ""
            assert evade_res["snapshot"]["free_slots"] is None

            # Top-level combat_mode/guard_target without nested snapshot updates current_snapshot
            guard_patch_res = await slot.dispatch_goal(
                {"action_type": "guard", "target_player": "Boiser"}
            )
            assert guard_patch_res["ok"] is True
            assert slot.current_snapshot()["combat_mode"] == "guard"
            assert slot.current_snapshot()["guard_target"] == "Boiser"
        finally:
            await slot.close()
