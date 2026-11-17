"""Unit tests for Phase 3 reverse WebSocket NodeTransport, config, and secret scrubbing."""

import asyncio
import json
import time

import pytest
from pydantic import SecretStr, ValidationError

from hakimi.config import Settings
from hakimi.node_transport import (
    ALLOWED_NODE_ACTIONS,
    ALLOWED_NODE_EVENTS,
    NodeTransport,
    decode_node_frame,
)
from hakimi.security import Rejected, configured_secrets, safe_output

NODE_TOKEN = "node-ws-test-token-32chars-min-9876543210"
NODE_PATH = "/ws/node/0123456789abcdef0123456789abcdef0123456789abcdef"


def node_settings(settings: Settings, **update) -> Settings:
    return settings.model_copy(
        update={
            "node_ws_enabled": True,
            "node_ws_token": SecretStr(NODE_TOKEN),
            "node_ws_path": NODE_PATH,
            "ws_ping_interval": 25.0,
            **update,
        }
    )


def hello_event(
    *,
    node_id: str = "shanghai-cn-1",
    region: str = "CN",
    capabilities: list[str] | None = None,
) -> dict:
    return {
        "type": "event",
        "event": "node.hello",
        "node_id": node_id,
        "region": region,
        "ts": time.time(),
        "payload": {
            "version": "2.0.0",
            "capabilities": (
                ["bili.public_get", "bili.request", "bili.video_storyboard"]
                if capabilities is None
                else capabilities
            ),
        },
    }


def rpc_ok(req: dict, data: dict) -> dict:
    return {
        "type": "rpc_response",
        "id": req["id"],
        "ok": True,
        "data": data,
    }


def rpc_err(
    req: dict,
    error: str,
    *,
    upstream_status: int | None = None,
    upstream_code: int | None = None,
) -> dict:
    return {
        "type": "rpc_response",
        "id": req["id"],
        "ok": False,
        "error": error,
        "upstream_status": upstream_status,
        "upstream_code": upstream_code,
    }


class FakeNodePeer:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str | bytes] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict] = asyncio.Queue()
        self.recv_started: asyncio.Event = asyncio.Event()
        self.close_gate: asyncio.Event | None = None
        self.close_codes: list[int] = []
        self.task: asyncio.Task | None = None

    async def push(self, frame: dict | str | bytes) -> None:
        if isinstance(frame, dict):
            await self.incoming.put(json.dumps(frame))
        else:
            await self.incoming.put(frame)

    async def receive(self) -> str | bytes:
        self.recv_started.set()
        return await self.incoming.get()

    async def send(self, raw: str) -> None:
        await self.outgoing.put(json.loads(raw))

    async def close(self, code: int) -> None:
        self.close_codes.append(code)
        if self.close_gate is not None:
            await self.close_gate.wait()

    async def connect(
        self,
        transport: NodeTransport,
        *,
        source_ip: str = "203.0.113.10",
        node_id: str = "shanghai-cn-1",
        region: str = "CN",
        send_hello: bool = True,
        capabilities: list[str] | None = None,
    ) -> None:
        self.task = asyncio.create_task(
            transport.serve(
                self.receive,
                self.send,
                self.close,
                source_ip=source_ip,
                node_id=node_id,
                region=region,
            )
        )
        if send_hello:
            await asyncio.wait_for(self.recv_started.wait(), 2)
            await self.push(hello_event(node_id=node_id, region=region, capabilities=capabilities))
            await asyncio.wait_for(transport.ready.wait(), 2)


def test_settings_node_ws_validation_and_aliases(settings: Settings) -> None:
    assert settings.node_ws_enabled is False
    assert settings.node_ws_path == "/ws/node"
    assert settings.node_id == "shanghai-cn-1"
    assert settings.node_ws_max_frame_bytes == 1_048_576
    assert settings.node_ws_rpc_timeout == 15.0
    assert settings.node_bili_delegate_enabled is True
    assert settings.node_bili_fallback_local is True

    base = settings.model_dump()
    for bad_token in ("", "short", "x" * 31, "GENERATE_" + "a" * 32, "REPLACE_" + "b" * 32):
        with pytest.raises(ValidationError):
            Settings(
                _env_file=None,
                **(base | {"node_ws_enabled": True, "node_ws_token": SecretStr(bad_token)}),
            )

    for bad_path in (
        "/ws/onebot",
        "/ws/node/short",
        NODE_PATH + "?token=leak",
        NODE_PATH + "/extra",
        "/admin",
    ):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **(base | {"node_ws_path": bad_path}))

    for bad_id in ("", "bad space", "../escape", "x" * 65):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **(base | {"node_id": bad_id}))

    for bad_frame in (1024, 2_097_153):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **(base | {"node_ws_max_frame_bytes": bad_frame}))

    for bad_timeout in (1.0, 121.0):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **(base | {"node_ws_rpc_timeout": bad_timeout}))

    env_loaded = Settings(
        _env_file=None,
        **{
            k: v
            for k, v in base.items()
            if k
            not in {
                "node_ws_enabled",
                "node_ws_token",
                "node_ws_path",
                "node_id",
                "node_ws_max_frame_bytes",
                "node_ws_rpc_timeout",
                "node_bili_delegate_enabled",
                "node_bili_fallback_local",
            }
        },
        NODE_WS_ENABLED=True,
        NODE_WS_TOKEN=NODE_TOKEN,
        NODE_WS_PATH=NODE_PATH,
        NODE_ID="shanghai-cn-2",
        NODE_WS_MAX_FRAME_BYTES=524_288,
        NODE_WS_RPC_TIMEOUT=20.0,
        NODE_BILI_DELEGATE_ENABLED=False,
        NODE_BILI_FALLBACK_LOCAL=False,
    )
    assert env_loaded.node_ws_enabled is True
    assert env_loaded.node_ws_token.get_secret_value() == NODE_TOKEN
    assert env_loaded.node_ws_path == NODE_PATH
    assert env_loaded.node_id == "shanghai-cn-2"
    assert env_loaded.node_ws_max_frame_bytes == 524_288
    assert env_loaded.node_ws_rpc_timeout == 20.0
    assert env_loaded.node_bili_delegate_enabled is False
    assert env_loaded.node_bili_fallback_local is False


def test_node_ws_token_included_in_configured_secrets_and_scrubbed(settings: Settings) -> None:
    cfg = node_settings(settings)
    secrets = configured_secrets(cfg)
    assert NODE_TOKEN in secrets
    with pytest.raises(Rejected, match="invalid_output"):
        safe_output(f"leaked token {NODE_TOKEN}", secrets)


@pytest.mark.parametrize(
    ("raw", "expected_code"),
    [
        ('{"id":"1","id":"2"}', "node_invalid_json"),
        ('{"x":NaN}', "node_invalid_json"),
        ('{"x":Infinity}', "node_invalid_json"),
        ("[1, 2, 3]", "node_invalid_json"),
        (b"\xff\xfe", "node_invalid_json"),
        ("x" * 101, "node_frame_too_large"),
        (json.dumps({"s": "你" * 40}), "node_frame_too_large"),
    ],
)
def test_decode_node_frame_rejects_malformed_and_oversized(
    raw: str | bytes, expected_code: str
) -> None:
    with pytest.raises(Rejected, match=expected_code):
        decode_node_frame(raw, 100)


def test_decode_node_frame_accepts_exact_boundary() -> None:
    payload = '{"ok":true}'
    assert decode_node_frame(payload, len(payload.encode("utf-8"))) == {"ok": True}
    assert decode_node_frame(payload.encode("utf-8"), len(payload.encode("utf-8"))) == {"ok": True}


async def test_hello_handshake_status_and_state_callbacks(settings: Settings) -> None:
    states: list[bool] = []
    events: list[dict] = []

    async def on_state(online: bool) -> None:
        states.append(online)

    async def on_event(ev: dict) -> None:
        events.append(ev)

    wire = NodeTransport(node_settings(settings), on_event=on_event, on_state=on_state)
    peer = FakeNodePeer()
    try:
        assert wire.connected() is False
        assert wire.status()["online"] is False
        await peer.connect(wire, source_ip="203.0.113.55")
        assert wire.connected() is True
        st = wire.status()
        assert st["configured"] is True
        assert st["online"] is True
        assert st["node_id"] == "shanghai-cn-1"
        assert st["region"] == "CN"
        assert st["capabilities"] == ["bili.public_get", "bili.request", "bili.video_storyboard"]
        assert st["source_ip"] == "203.0.113.55"
        assert st["connected_at"] is not None
        assert st["last_heartbeat_at"] is None
        assert st["rpc_calls"] == 0
        assert st["rpc_errors"] == 0
        await asyncio.sleep(0.01)
        assert states == [True]
        assert len(events) == 1 and events[0]["event"] == "node.hello"
    finally:
        await wire.close()
    assert wire.connected() is False
    assert states == [True, False]


async def test_handshake_rejects_wrong_identity_and_timeout(settings: Settings) -> None:
    wire = NodeTransport(node_settings(settings))
    wire._handshake_timeout = 0.08
    try:
        bad_header_peer = FakeNodePeer()
        await bad_header_peer.connect(wire, node_id="wrong-node", send_hello=False)
        await asyncio.wait_for(bad_header_peer.task, 1)
        assert bad_header_peer.close_codes == [1008]
        assert wire.connected() is False

        bad_hello_peer = FakeNodePeer()
        await bad_hello_peer.connect(wire, node_id="", region="CN", send_hello=False)
        await bad_hello_peer.push(hello_event(node_id="shanghai-cn-1", region="US"))
        await asyncio.wait_for(bad_hello_peer.task, 1)
        assert wire.connected() is False

        silent_peer = FakeNodePeer()
        await silent_peer.connect(wire, send_hello=False)
        await asyncio.wait_for(silent_peer.task, 1)
        assert wire.connected() is False
    finally:
        await wire.close()


async def test_handshake_ping_fallback_succeeds_without_hello(settings: Settings) -> None:
    wire = NodeTransport(node_settings(settings))
    wire._handshake_timeout = 0.4
    peer = FakeNodePeer()
    try:
        await peer.connect(wire, send_hello=False)
        req = await asyncio.wait_for(peer.outgoing.get(), 1)
        assert req["type"] == "rpc_request"
        assert req["action"] == "node.ping"
        await peer.push(
            rpc_ok(
                req,
                {"node_id": "shanghai-cn-1", "region": "CN", "capabilities": ["bili.public_get"]},
            )
        )
        await asyncio.wait_for(wire.ready.wait(), 1)
        assert wire.connected() is True
        assert wire.status()["capabilities"] == ["bili.public_get"]
    finally:
        await wire.close()


async def test_peer_handoff_replaces_half_open_peer_and_isolates_pending(
    settings: Settings,
) -> None:
    states: list[bool] = []

    async def on_state(online: bool) -> None:
        states.append(online)

    wire = NodeTransport(node_settings(settings), on_state=on_state)
    first, second = FakeNodePeer(), FakeNodePeer()
    first.close_gate = asyncio.Event()
    try:
        await first.connect(wire, source_ip="203.0.113.1")
        inflight = asyncio.create_task(
            wire.call("bili.public_get", {"path": "/x/web-interface/view"})
        )
        req1 = await asyncio.wait_for(first.outgoing.get(), 1)

        await second.connect(wire, source_ip="203.0.113.2")
        assert wire.connected() is True
        assert wire.status()["source_ip"] == "203.0.113.2"
        with pytest.raises(Rejected, match="node_disconnected"):
            await inflight

        first.close_gate.set()
        await asyncio.gather(first.task, return_exceptions=True)
        assert states == [True, True]
        assert wire.connected() is True

        # Old peer response cannot resolve new peer's RPC.
        call2 = asyncio.create_task(wire.call("bili.resolve_short", {"url": "https://b23.tv/abc"}))
        req2 = await asyncio.wait_for(second.outgoing.get(), 1)
        await first.push(rpc_ok(req2, {"location": "https://evil.example"}))
        await first.push(rpc_ok(req1, {"code": 0, "data": {}}))
        await asyncio.sleep(0.01)
        assert not call2.done()

        await second.push(rpc_ok(req2, {"location": "https://www.bilibili.com/video/BV1xx411c7mD"}))
        assert (await call2)["location"] == "https://www.bilibili.com/video/BV1xx411c7mD"
    finally:
        if first.close_gate is not None:
            first.close_gate.set()
        await wire.close()


async def test_concurrent_rpcs_allowlist_timeout_errors_and_limits(settings: Settings) -> None:
    wire = NodeTransport(node_settings(settings))
    peer = FakeNodePeer()
    try:
        with pytest.raises(Rejected, match="node_action_forbidden"):
            await wire.call("shell.exec", {})
        with pytest.raises(Rejected, match="node_disconnected"):
            await wire.call("bili.public_get", {})

        await peer.connect(wire)

        # Concurrent RPCs with out-of-order responses and forged id ignored.
        t1 = asyncio.create_task(wire.call("bili.public_get", {"n": 1}))
        t2 = asyncio.create_task(wire.call("bili.resolve_short", {"n": 2}))
        r1 = await asyncio.wait_for(peer.outgoing.get(), 1)
        r2 = await asyncio.wait_for(peer.outgoing.get(), 1)
        assert r1["type"] == "rpc_request" and r2["type"] == "rpc_request"
        assert r1["action"] == "bili.public_get" and r2["action"] == "bili.resolve_short"

        await peer.push({"type": "rpc_response", "id": "forged-id", "ok": True, "data": {"bad": 1}})
        await peer.push(rpc_ok(r2, {"location": "https://www.bilibili.com/video/BV1xx411c7mD"}))
        await peer.push(rpc_ok(r1, {"code": 0, "data": {"title": "ok"}}))
        assert (await t1)["data"]["title"] == "ok"
        assert (await t2)["location"].endswith("BV1xx411c7mD")

        # Timeout error propagation.
        with pytest.raises(Rejected, match="node_rpc_timeout"):
            await wire.call("bili.public_get", {"slow": True}, timeout=0.04)
        _ = await asyncio.wait_for(peer.outgoing.get(), 1)

        # Upstream error propagation (status + code).
        err_task = asyncio.create_task(wire.call("bili.request", {"path": "/x/web-interface/view"}))
        req_err = await asyncio.wait_for(peer.outgoing.get(), 1)
        await peer.push(rpc_err(req_err, "bili_http_status", upstream_status=412))
        with pytest.raises(Rejected, match="bili_http_status") as exc_info:
            await err_task
        assert exc_info.value.detail == "412"

        err_code_task = asyncio.create_task(
            wire.call("bili.request", {"path": "/x/web-interface/nav"})
        )
        req_code = await asyncio.wait_for(peer.outgoing.get(), 1)
        await peer.push(rpc_err(req_code, "bili_risk", upstream_status=200, upstream_code=-352))
        with pytest.raises(Rejected, match="bili_risk") as exc_code:
            await err_code_task
        assert exc_code.value.detail == "-352"

        # Invalid response shape.
        bad_shape_task = asyncio.create_task(wire.call("mc.status", {}))
        req_bad = await asyncio.wait_for(peer.outgoing.get(), 1)
        await peer.push(
            {"type": "rpc_response", "id": req_bad["id"], "ok": True, "data": ["not-dict"]}
        )
        with pytest.raises(Rejected, match="node_invalid_response"):
            await bad_shape_task

        # Pending RPC queue cap = 16.
        pending_tasks = [asyncio.create_task(wire.call("mc.status", {"i": i})) for i in range(16)]
        pending_reqs = [await asyncio.wait_for(peer.outgoing.get(), 1) for _ in range(16)]
        with pytest.raises(Rejected, match="node_unavailable"):
            await wire.call("mc.status", {"i": 16})
        for req in pending_reqs:
            await peer.push(rpc_ok(req, {"ok": True}))
        await asyncio.gather(*pending_tasks)

        st = wire.status()
        assert st["rpc_calls"] == 18
        assert st["rpc_errors"] == 5
    finally:
        await wire.close()


async def test_event_consumer_backpressure_and_forbidden_event(settings: Settings) -> None:
    seen: list[str] = []

    async def on_event(ev: dict) -> str:
        seen.append(ev["event"])
        if ev["event"] == "mc.milestone":
            return "full"
        return "ok"

    wire = NodeTransport(node_settings(settings), on_event=on_event)
    peer = FakeNodePeer()
    try:
        await peer.connect(wire)
        await peer.push(
            {
                "type": "event",
                "event": "node.heartbeat",
                "node_id": "shanghai-cn-1",
                "region": "CN",
                "ts": time.time(),
                "payload": {},
            }
        )
        await peer.push(
            {
                "type": "event",
                "event": "mc.snapshot",
                "node_id": "shanghai-cn-1",
                "region": "CN",
                "ts": time.time(),
                "payload": {"hp": 20},
            }
        )
        await asyncio.sleep(0.02)
        assert seen == ["node.hello", "node.heartbeat", "mc.snapshot"]
        assert wire.status()["last_heartbeat_at"] is not None

        # Backpressure closes peer.
        await peer.push(
            {
                "type": "event",
                "event": "mc.milestone",
                "node_id": "shanghai-cn-1",
                "region": "CN",
                "ts": time.time(),
                "payload": {},
            }
        )
        await asyncio.wait_for(peer.task, 1)
        assert wire.connected() is False

        # Forbidden event disconnects peer.
        peer2 = FakeNodePeer()
        wire.on_event = None
        await peer2.connect(wire)
        await peer2.push(
            {
                "type": "event",
                "event": "evil.event",
                "node_id": "shanghai-cn-1",
                "region": "CN",
                "ts": time.time(),
                "payload": {},
            }
        )
        await asyncio.wait_for(peer2.task, 1)
        assert wire.connected() is False
    finally:
        await wire.close()


async def test_heartbeat_updates_timestamp_and_disconnects_on_failure(settings: Settings) -> None:
    wire = NodeTransport(node_settings(settings, ws_ping_interval=1.0))
    # Speed up heartbeat loop for test without hitting Settings ge=1 constraint.
    peer = FakeNodePeer()
    try:
        await peer.connect(wire)
        assert wire.status()["last_heartbeat_at"] is None
        # Wait for first heartbeat (1.0s interval)
        req = await asyncio.wait_for(peer.outgoing.get(), 2.0)
        assert req["action"] == "node.ping"
        await peer.push(rpc_ok(req, {"pong": True}))
        await asyncio.sleep(0.02)
        hb_at = wire.status()["last_heartbeat_at"]
        assert hb_at is not None and hb_at <= time.time()
        assert wire.status()["rpc_calls"] == 0

        # Next heartbeat fails -> connection terminates.
        req2 = await asyncio.wait_for(peer.outgoing.get(), 2.0)
        await peer.push(rpc_err(req2, "node_unhealthy"))
        await asyncio.wait_for(peer.task, 1.0)
        assert wire.connected() is False
    finally:
        await wire.close()


async def test_close_fails_pending_rpcs_and_is_idempotent(settings: Settings) -> None:
    assert "bili.video_storyboard" in ALLOWED_NODE_ACTIONS
    assert "mc.snapshot" in ALLOWED_NODE_EVENTS

    wire = NodeTransport(node_settings(settings))
    peer = FakeNodePeer()
    await peer.connect(wire)
    pending = asyncio.create_task(wire.call("bili.cover_thumb", {"url": "https://i0.hdslb.com/bfs/a.jpg"}))
    await asyncio.wait_for(peer.outgoing.get(), 1)
    await wire.close()
    await wire.close()
    with pytest.raises(Rejected, match="node_disconnected"):
        await pending
    assert wire.connected() is False
    assert 1012 in peer.close_codes


async def test_default_1mib_frame_cap_enforced(settings: Settings) -> None:
    wire = NodeTransport(node_settings(settings))
    assert wire.s.node_ws_max_frame_bytes == 1_048_576
    oversized = (
        '{"type":"rpc_response","id":"1","ok":true,"data":{"x":"' + ("a" * 1_048_576) + '"}}'
    )
    with pytest.raises(Rejected, match="node_frame_too_large"):
        decode_node_frame(oversized, wire.s.node_ws_max_frame_bytes)

    peer = FakeNodePeer()
    try:
        await peer.connect(wire)
        with pytest.raises(Rejected, match="node_frame_too_large"):
            await wire.call("bili.request", {"blob": "x" * 1_048_576})
        assert wire.status()["rpc_errors"] == 1
    finally:
        await wire.close()
