"""Real WS transport + durable owner queue under pre-write and ambiguous failures."""

import asyncio

from hakimi.onebot_transport import WebSocketTransport
from tests.test_crossborder_transport import Peer, response
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import queued, stamp


async def make_wire(dm):
    wire = WebSocketTransport(dm.s)
    dm.service.sender.onebot = wire
    peer = Peer()
    await peer.login(wire)
    return wire, peer


async def test_disconnect_after_first_guard_preserves_pending_then_sends_once(dm, monkeypatch):
    wire, first = await make_wire(dm)
    original = wire.send_onebot_action_with_retry

    async def lost_before_dispatch(action, params, **kwargs):
        # Sender already ran the first policy guard and marked the queue row sending.
        await wire.close()
        return await original(action, params, **kwargs, max_wait=0.03)

    monkeypatch.setattr(wire, "send_onebot_action_with_retry", lost_before_dispatch)
    try:
        item = await dm.test_dm()
        assert item["status"] == "pending"
        row = (await queued(dm))[0]
        assert row["payload"] is not None and row["session_epoch"] is None
        assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
        assert [r["action"] for r in first.sent] == ["get_login_info"]
        # Recovery/restart must not lose the never-submitted encrypted text.
        await dm.initialize()
        monkeypatch.setattr(wire, "send_onebot_action_with_retry", original)
        second = Peer()
        await second.login(wire)
        deliver = asyncio.create_task(dm.deliver_once())
        request = await asyncio.wait_for(second.outgoing.get(), 1)
        assert request["action"] == "send_private_msg"
        await second.incoming.put(response(request, {"message_id": 321}))
        assert await asyncio.wait_for(deliver, 1)
        assert await dm.delivery_status(item["id"]) == "sent"
        assert len((await dm.service.sessions.snapshot(dm.target()))["recent_messages"]) == 1
        assert not await dm.deliver_once()
        assert sum(r["action"] == "send_private_msg" for r in second.sent) == 1
        assert not dm.service.ai.calls
        assert not await dm.store.all("SELECT * FROM ledger")
    finally:
        await wire.close()


async def test_ambiguous_dispatch_is_not_retried_by_transport_or_durable_queue(dm):
    wire, peer = await make_wire(dm)
    peer.send_error = True
    try:
        item = await dm.test_dm()
        assert item["status"] == "uncertain"
        assert (await queued(dm))[0]["payload"] is None
        assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
        peer.send_error = False
        await dm.initialize()
        assert not await dm.deliver_once()
        assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 1
        assert not dm.service.ai.calls
    finally:
        await wire.close()


async def test_second_guard_expiry_preserves_terminal_skip_instead_of_uncertain(dm, monkeypatch):
    wire, peer = await make_wire(dm)
    original = wire.send_onebot_action_with_retry

    async def expire_before_second_guard(action, params, **kwargs):
        dm.time[0] += 13 * 3600  # Test/alert message has expired, despite first guard succeeding.
        return await original(action, params, **kwargs, max_wait=0.1)

    monkeypatch.setattr(wire, "send_onebot_action_with_retry", expire_before_second_guard)
    try:
        item = await dm.test_dm()
        row = (await queued(dm))[0]
        assert item["status"] == row["status"] == "skipped"
        assert row["reason"] == "expired" and row["payload"] is None
        assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 0
        assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
    finally:
        await wire.close()


async def test_second_guard_quiet_boundary_does_not_send_or_replay_digest(dm, monkeypatch):
    wire, peer = await make_wire(dm)
    original = wire.send_onebot_action_with_retry
    dm.time[0] = stamp("2026-11-14T23:29:00")
    result = await dm.prepare_digest(automatic=True, expires=dm.clock() + 60)
    assert result["status"] == "queued"

    async def quiet_before_second_guard(action, params, **kwargs):
        dm.time[0] = stamp("2026-11-14T23:30:00")
        return await original(action, params, **kwargs, max_wait=0.1)

    monkeypatch.setattr(wire, "send_onebot_action_with_retry", quiet_before_second_guard)
    try:
        await dm.deliver_once()
        row = (await queued(dm))[0]
        assert row["status"] == "skipped" and row["reason"] in {"outside_digest_window", "expired"}
        assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 0
        dm.time[0] = stamp("2026-11-15T09:00:00")
        assert not await dm.deliver_once()
        assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
    finally:
        await wire.close()


async def test_owner_change_while_waiting_does_not_send_to_previous_owner(dm, monkeypatch):
    wire, peer = await make_wire(dm)
    original = wire.send_onebot_action_with_retry

    async def changed_owner_before_recheck(action, params, **kwargs):
        dm.s.owner_qq_id = "30000"
        return await original(action, params, **kwargs, max_wait=0.1)

    monkeypatch.setattr(wire, "send_onebot_action_with_retry", changed_owner_before_recheck)
    try:
        await dm.test_dm()
        assert sum(r["action"] == "send_private_msg" for r in peer.sent) == 0
    finally:
        dm.s.owner_qq_id = "20000"
        await wire.close()
