"""Authenticated takeover, bounded media frames and no duplicate ambiguous sends."""

import asyncio
import json
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError
from starlette.websockets import WebSocketDisconnect

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.onebot_transport import WebSocketTransport, websocket_source_ip
from hakimi.security import Rejected
from tests.fakes import FakeGateway

TOKEN = "crossborder-test-token-32chars-minimum-not-live"
PATH = "/ws/onebot/0123456789abcdef0123456789abcdef"


def ws_settings(settings, **update):
    return settings.model_copy(
        update={
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": SecretStr(TOKEN),
            **update,
        }
    )


def headers(**extra):
    return {
        "Authorization": "Bearer " + TOKEN,
        "X-Self-ID": "10000",
        "X-Client-Role": "Universal",
        **extra,
    }


def response(request, data):
    return {"status": "ok", "retcode": 0, "echo": request["echo"], "data": data}


class Peer:
    def __init__(self):
        self.incoming, self.outgoing = asyncio.Queue(), asyncio.Queue()
        self.close_gate = None
        self.close_started = asyncio.Event()
        self.send_error = False
        self.task = None
        self.sent = []

    async def receive(self):
        return json.dumps(await self.incoming.get())

    async def send(self, raw):
        value = json.loads(raw)
        self.sent.append(value)
        await self.outgoing.put(value)
        if self.send_error and value["action"].startswith("send_"):
            raise ConnectionResetError("potentially delivered")

    async def close(self, code):
        self.close_started.set()
        if self.close_gate:
            await self.close_gate.wait()

    async def login(self, transport, source="203.0.113.20"):
        async def event(_):
            return "ignored"

        self.task = asyncio.create_task(
            transport.serve(
                self.receive,
                self.send,
                self.close,
                event,
                source_ip=source,
            )
        )
        request = await asyncio.wait_for(self.outgoing.get(), 1)
        assert request["action"] == "get_login_info"
        await self.incoming.put(response(request, {"user_id": 10000}))
        await asyncio.wait_for(transport.ready.wait(), 1)


@pytest.fixture
async def transport(settings):
    wire = WebSocketTransport(ws_settings(settings))
    yield wire
    await wire.close()


def test_custom_route_rejects_default_path_and_query_before_accept(settings):
    app = create_app(
        ws_settings(settings, onebot_ws_path=PATH), gateway_factory=FakeGateway, start_workers=False
    )
    with TestClient(app) as client:
        for path, hdr in [
            ("/ws/onebot", headers()),
            (PATH + "?token=bad", headers()),
            (PATH, headers(Authorization="Bearer bad")),
        ]:
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(path, headers=hdr):
                    pytest.fail("invalid handshake accepted")
        with client.websocket_connect(PATH, headers=headers()) as socket:
            login = socket.receive_json()
            socket.send_json(response(login, {"user_id": 10000}))


def test_startup_rejects_empty_or_short_ws_token(settings):
    for token in ("", "short", "x" * 31):
        app = create_app(
            ws_settings(settings, onebot_ws_token=SecretStr(token)),
            gateway_factory=FakeGateway,
            start_workers=False,
        )
        with pytest.raises(RuntimeError, match="32-character"):
            with TestClient(app):
                pass


def test_new_token_alias_path_and_legacy_compatibility(settings):
    values = settings.model_dump()
    values.update(onebot_mode="ws_reverse", onebot_ws_token=TOKEN)
    valid = Settings(_env_file=None, **values)
    assert valid.onebot_ws_token.get_secret_value() == TOKEN
    values.pop("onebot_ws_token")
    values.pop("onebot_ws_path")
    valid = Settings(_env_file=None, **values, ONEBOT_ACCESS_TOKEN=TOKEN, ONEBOT_WS_PATH=PATH)
    assert valid.onebot_ws_token.get_secret_value() == TOKEN
    assert valid.onebot_ws_path == PATH
    for path in ("/admin", "/ws/onebot/short", PATH + "?token=x", PATH + "/extra"):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **values, ONEBOT_ACCESS_TOKEN=TOKEN, ONEBOT_WS_PATH=path)


@pytest.mark.parametrize(
    "peer,claimed,expected",
    [
        ("127.0.0.1", "203.0.113.4", "203.0.113.4"),
        ("198.51.100.5", "203.0.113.4", "198.51.100.5"),
        ("127.0.0.1", "bad\nspoof", "127.0.0.1"),
        ("127.0.0.1", "203.0.113.4, 127.0.0.1", "127.0.0.1"),
    ],
)
def test_source_ip_trusts_only_explicit_proxy(settings, peer, claimed, expected):
    socket = SimpleNamespace(client=SimpleNamespace(host=peer), headers={"x-real-ip": claimed})
    assert websocket_source_ip(socket, settings) == expected


async def test_new_peer_takes_over_half_open_peer_without_waiting(transport):
    first, second = Peer(), Peer()
    first.close_gate = asyncio.Event()
    states = []

    async def state(value):
        states.append(value)

    transport.on_state = state
    await first.login(transport)
    pending = asyncio.create_task(transport.call("send_private_msg", {"user_id": 20000}))
    await asyncio.wait_for(first.outgoing.get(), 1)
    await second.login(transport, "203.0.113.21")
    assert transport.ready.is_set()
    assert transport.status()["source_ip"] == "203.0.113.21"
    first.close_gate.set()
    await asyncio.gather(first.task, return_exceptions=True)
    with pytest.raises(Rejected, match="delivery_uncertain"):
        await pending
    assert transport.ready.is_set() and states == [True, True]
    request_task = asyncio.create_task(transport.call("get_status", {}))
    request = await asyncio.wait_for(second.outgoing.get(), 1)
    await second.incoming.put(response(request, {"online": True, "good": True}))
    assert (await request_task)["online"] is True


async def test_large_rpc_allowed_but_large_message_event_disconnects(transport):
    peer = Peer()
    await peer.login(transport)
    operation = asyncio.create_task(transport.call("get_image", {"file": "image-id"}))
    request = await asyncio.wait_for(peer.outgoing.get(), 1)
    data = "A" * (1024 * 1024 + 100)
    await peer.incoming.put(response(request, {"base64": data}))
    assert (await operation)["base64"] == data
    await peer.incoming.put({"post_type": "message", "self_id": 10000, "message": data})
    await asyncio.wait_for(peer.task, 1)
    assert not transport.ready.is_set()


async def test_pending_echo_cannot_be_resolved_by_an_old_peer(transport):
    first, second = Peer(), Peer()
    first.close_gate = asyncio.Event()
    await first.login(transport)
    await second.login(transport)
    call = asyncio.create_task(transport.call("get_image", {"file": "id"}))
    request = await asyncio.wait_for(second.outgoing.get(), 1)
    await first.incoming.put(response(request, {"base64": "forged-old-peer"}))
    await asyncio.sleep(0.01)
    assert not call.done()
    await second.incoming.put(response(request, {"base64": "current-peer"}))
    assert (await call)["base64"] == "current-peer"
    first.close_gate.set()


async def test_waiting_sender_resumes_once_on_reconnect(transport):
    send = asyncio.create_task(
        transport.send_onebot_action_with_retry(
            "send_private_msg",
            {"user_id": 20000},
            max_wait=1,
        )
    )
    await asyncio.sleep(0)
    peer = Peer()
    await peer.login(transport)
    request = await asyncio.wait_for(peer.outgoing.get(), 1)
    await peer.incoming.put(response(request, {"message_id": 123}))
    assert await send == {"message_id": 123}
    assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 1


async def test_wait_timeout_is_not_dispatched_and_never_loops_forever(transport):
    start = time.monotonic()
    with pytest.raises(Rejected, match="onebot_not_dispatched"):
        await transport.send_onebot_action_with_retry("send_private_msg", {}, max_wait=0.03)
    assert time.monotonic() - start < 0.2


async def test_ambiguous_write_is_never_retried(transport):
    peer = Peer()
    await peer.login(transport)
    peer.send_error = True
    with pytest.raises(Rejected, match="delivery_uncertain"):
        await transport.send_onebot_action_with_retry("send_private_msg", {}, max_wait=0.1)
    assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 1


async def test_send_guard_rechecked_after_waiting_for_login(transport):
    async def guard():
        return False

    send = asyncio.create_task(
        transport.send_onebot_action_with_retry(
            "send_private_msg",
            {},
            before_send=guard,
            max_wait=1,
        )
    )
    peer = Peer()
    await peer.login(transport)
    with pytest.raises(Rejected, match="onebot_not_dispatched"):
        await send
    assert len(peer.sent) == 1


async def test_heartbeat_time_only_changes_after_successful_rpc(settings):
    wire = WebSocketTransport(ws_settings(settings, ws_ping_interval=0.02))
    peer = Peer()
    try:
        await peer.login(wire)
        assert wire.status()["last_heartbeat_at"] is None
        req = await asyncio.wait_for(peer.outgoing.get(), 1)
        assert req["action"] == "get_status"
        await peer.incoming.put(response(req, {"online": True, "good": True}))
        await asyncio.sleep(0.01)
        assert wire.status()["last_heartbeat_at"] <= time.time()
    finally:
        await wire.close()
