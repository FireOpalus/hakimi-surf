import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.server import serve

from hakimi.app import create_app
from hakimi.onebot_transport import WebSocketTransport, decode_frame
from hakimi.security import Rejected
from tests.fakes import FakeGateway


def reverse_settings(settings):
    return settings.model_copy(
        update={
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": SecretStr("test-ws-secret-123456-strong-9876543210"),
            "ws_ping_interval": 1,
        }
    )


def headers():
    return {
        "Authorization": "Bearer test-ws-secret-123456-strong-9876543210",
        "X-Self-ID": "10000",
        "X-Client-Role": "Universal",
    }


def reply(request, data):
    return {"echo": request["echo"], "status": "ok", "retcode": 0, "data": data}


def test_reverse_websocket_login_and_heartbeat(settings):
    app = create_app(reverse_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    with TestClient(app) as client, client.websocket_connect("/ws/onebot", headers=headers()) as ws:
        login = ws.receive_json()
        assert login["action"] == "get_login_info"
        ws.send_json(reply(login, {"user_id": 10000}))
        heartbeat = ws.receive_json()
        assert heartbeat["action"] == "get_status"
        ws.send_json(reply(heartbeat, {"online": True, "good": True}))


@pytest.mark.parametrize(
    "change",
    [
        {"Authorization": "Bearer wrong"},
        {"X-Self-ID": "999"},
        {"X-Client-Role": "Event"},
    ],
)
def test_reverse_websocket_rejects_wrong_credentials_or_nonuniversal_role(settings, change):
    app = create_app(reverse_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/onebot", headers=headers() | change):
                pass


def test_reverse_verifies_actual_login_identity(settings):
    app = create_app(reverse_settings(settings), gateway_factory=FakeGateway, start_workers=False)
    with TestClient(app) as client, client.websocket_connect("/ws/onebot", headers=headers()) as ws:
        ws.send_json(reply(ws.receive_json(), {"user_id": 999}))
        with pytest.raises(WebSocketDisconnect):
            ws.receive_json()


async def test_echo_routing_forward_fetch_does_not_deadlock_and_unknown_action_rejected(settings):
    transport = WebSocketTransport(settings)
    inbound, outbound = asyncio.Queue(), asyncio.Queue()
    received = asyncio.Event()

    async def close(code):
        pass

    async def event(value):
        result = await transport.call("get_forward_msg", {"id": "forward-1"})
        assert result == {"messages": []}
        received.set()
        return "accepted"

    async def send(raw):
        await outbound.put(json.loads(raw))

    async def receive():
        return json.dumps(await inbound.get())

    task = asyncio.create_task(transport.serve(receive, send, close, event))
    try:
        login = await asyncio.wait_for(outbound.get(), 2)
        await inbound.put(reply(login, {"user_id": 10000}))
        await asyncio.wait_for(transport.ready.wait(), 2)
        await inbound.put({"post_type": "message", "self_id": 10000})
        request = await asyncio.wait_for(outbound.get(), 2)
        assert request["action"] == "get_forward_msg"
        await inbound.put(reply({"echo": "forged-echo"}, {"messages": ["bad"]}))
        assert not received.is_set()
        await inbound.put(reply(request, {"messages": []}))
        await asyncio.wait_for(received.wait(), 2)
        with pytest.raises(Rejected, match="action_forbidden"):
            await transport.call("set_group_ban", {})
    finally:
        await transport.close()
        await asyncio.gather(task, return_exceptions=True)


async def test_forward_reconnects_after_remote_close(settings):
    connected = asyncio.Event()
    connections = 0

    async def server(ws):
        nonlocal connections
        assert (
            ws.request.headers["Authorization"] == "Bearer test-ws-secret-123456-strong-9876543210"
        )
        login = json.loads(await ws.recv())
        await ws.send(json.dumps(reply(login, {"user_id": 10000})))
        connections += 1
        if connections >= 2:
            connected.set()
            await ws.wait_closed()

    async with serve(server, "127.0.0.1", 0) as server_instance:
        port = server_instance.sockets[0].getsockname()[1]
        settings = reverse_settings(settings).model_copy(
            update={
                "onebot_mode": "ws_forward",
                "onebot_ws_url": f"ws://127.0.0.1:{port}/",
                "ws_reconnect_max": 1,
            }
        )
        transport = WebSocketTransport(settings)

        async def event(value):
            return "ignored"

        transport.start_forward(event)
        try:
            await asyncio.wait_for(connected.wait(), 5)
            assert connections >= 2
        finally:
            await transport.close()


@pytest.mark.parametrize("raw", ['{"self_id":1,"self_id":2}', '{"x":NaN}', "[1]", "x" * 101])
def test_websocket_rejects_malformed_or_large_frames(raw):
    with pytest.raises(Rejected):
        decode_frame(raw, 100)
