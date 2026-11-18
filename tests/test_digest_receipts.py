"""A paid digest has one daily result and an independent, zero-token QQ delivery receipt."""

import asyncio
import json
from contextlib import asynccontextmanager
from decimal import Decimal

import httpx
import pytest

from hakimi.adapters import Sender
from hakimi.life_engine import LifeEngine
from hakimi.service import Service
from hakimi.store import Store
from tests.fakes import FakeBili, FakeGateway
from tests.test_crossborder_owner_queue import make_wire
from tests.test_proactive_dm import Wire, stamp
from tests.test_proactive_dm import dm as dm


async def ticket_state(dm, key):
    row = await dm.store.one(
        "SELECT status,result_reason FROM scheduled_intents WHERE id=?", (key,)
    )
    return tuple(row)


@asynccontextmanager
async def process(settings, clock, gateway, wire):
    store = Store(settings)
    await store.open()
    try:
        async with httpx.AsyncClient() as http:
            service = Service(settings, store, gateway, FakeBili(), Sender(settings, http, wire))
            service.life_engine = LifeEngine(service, clock=clock)
            service.world_state.clock = service.proactive.clock = clock
            await service.life_engine.initialize()
            await service.proactive.initialize()
            await service.world_state.recover_actions()
            try:
                yield service.proactive
            finally:
                await service.stop()
    finally:
        await store.close()


async def test_generated_digest_survives_crash_cutoff_and_two_real_restarts(settings, monkeypatch):
    settings.onebot_mode = "ws_reverse"
    settings.total_usd = Decimal("0.01")
    at = [stamp("2026-11-14T22:15:00")]
    gateway, wire = FakeGateway(), Wire()
    wire.ready.clear()
    body = "持久化日报正文只属于收件人"
    async with process(settings, lambda: at[0], gateway, wire) as dm:
        await dm.service.life_engine.set_config({"active_hours": "09:00-22:30"})
        await dm.service.life_engine.schedule_due(at[0])
        intent = (await dm.service.world_state.list_due_intents(at[0]))[0]
        generation_started, finish = asyncio.Event(), asyncio.Event()
        generate = gateway.generate

        async def billed(*args, **kwargs):
            await generate(*args, **kwargs)
            generation_started.set()
            await finish.wait()
            reservation = await dm.store.reserve("digest-test", "proactive", Decimal("0.01"))
            await dm.store.settle(reservation, Decimal("0.01"))
            assert (await dm.store.budget_status())["hard_stop"]
            return body

        async def stop_after_save(*_args):
            raise asyncio.CancelledError()

        monkeypatch.setattr(gateway, "generate", billed)
        monkeypatch.setattr(dm.digests, "enqueue_generated_tx", stop_after_save)
        generating = asyncio.create_task(dm.prepare_digest(automatic=True, intent_id=intent["id"]))
        await asyncio.wait_for(generation_started.wait(), 3)
        assert (await dm.manual_digest())["receipt"]["stage"] == "generating"
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await generating
        receipt = (await dm.digest_history())[0]
        key = receipt["id"]
        assert receipt["stage"] == "generated" and receipt["queued_at"] is None
        assert receipt["scheduled_for"] == receipt["generated_at"] == at[0]
        saved = (await dm.store.one("SELECT payload FROM owner_digest_receipts"))[0]
        assert body.encode() not in saved and body in dm.store.vault.open(key, saved)["text"]
        assert not await dm.store.all("SELECT * FROM pending_owner_dms")

    for _ in range(2):
        at[0] += 20
        async with process(settings, lambda: at[0], gateway, wire) as dm:
            receipt = (await dm.digest_history())[0]
            assert (
                receipt["stage"] == "awaiting_qq" and receipt["generated_at"] < receipt["queued_at"]
            )
            assert receipt["submitted_at"] is receipt["confirmed_at"] is None
            assert body not in json.dumps(receipt, ensure_ascii=False)
            assert not (await dm.store.one("SELECT payload FROM owner_digest_receipts"))[0]
            assert (
                body.encode()
                not in (await dm.store.one("SELECT payload FROM pending_owner_dms"))[0]
            )
            assert await ticket_state(dm, intent["id"]) == (
                "executing",
                "owner_digest_awaiting_delivery",
            )
            assert not await dm.deliver_once()
    wire.ready.set()
    at[0] += 20
    async with process(settings, lambda: at[0], gateway, wire) as dm:
        await asyncio.gather(dm.manual_digest(), dm.deliver_once(), dm.deliver_once())
        receipt = (await dm.digest_history())[0]
        assert (
            receipt["stage"] == "confirmed"
            and receipt["confirmed_at"] == receipt["submitted_at"] == at[0]
        )
        assert await ticket_state(dm, intent["id"]) == ("done", "owner_digest_qq_confirmed")
        assert (await dm.store.budget_status())["hard_stop"]
        assert [(a, p["user_id"]) for a, p in wire.calls] == [("send_private_msg", 20000)]
        assert len((await dm.service.sessions.snapshot(dm.target()))["recent_messages"]) == 1
        assert [call[0] for call in gateway.calls] == ["screen", "generate"]


async def test_uncertain_qq_submission_never_replays_or_reaches_private_memory(dm):
    engine = dm.service.life_engine
    await engine.set_config({"active_hours": "09:00-22:30"})
    dm.time[0] = stamp("2026-11-14T22:15:00")
    await engine.tick_gatekeeper_once()
    started = asyncio.Event()

    async def absent_ack():
        started.set()
        await asyncio.Future()

    dm.service.sender.onebot.hook = absent_ack
    sending = asyncio.create_task(dm.deliver_once())
    await asyncio.wait_for(started.wait(), 3)
    assert (await dm.digest_history())[0]["stage"] == "submitting"
    sending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sending
    await dm.initialize()
    await dm.service.world_state.recover_actions()
    receipt = (await dm.digest_history())[0]
    assert receipt["stage"] == "uncertain" and receipt["reason"] == "restart_after_submit"
    assert receipt["submitted_at"] and receipt["confirmed_at"] is None
    assert await ticket_state(dm, receipt["intent_id"]) == ("uncertain", "owner_digest_uncertain")
    await dm.manual_digest()
    assert not await dm.deliver_once()
    assert len(dm.service.sender.onebot.calls) == 1 and len(dm.service.ai.calls) == 2
    assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]


async def test_cancel_before_websocket_write_preserves_never_submitted_digest(dm, monkeypatch):
    result = await dm.prepare_digest()
    wire, _ = await make_wire(dm)
    entered = asyncio.Event()
    rpc = wire._rpc

    async def before_write(*args, **kwargs):
        entered.set()
        return await rpc(*args, **kwargs)

    monkeypatch.setattr(wire, "_rpc", before_write)
    try:
        async with wire._peer.send_lock:
            sending = asyncio.create_task(dm.deliver_once())
            await asyncio.wait_for(entered.wait(), 3)
            receipt = (await dm.digest_history())[0]
            assert receipt["stage"] == "queued" and receipt["submitted_at"] is None
            sending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await sending
        await dm.initialize()
        assert await dm.delivery_status(result["id"]) == "pending"
    finally:
        await wire.close()


@pytest.mark.parametrize("manual", [True, False])
async def test_digest_expiry_and_quiet_apply_to_paid_manual_and_timer_messages(dm, manual):
    engine = dm.service.life_engine
    await engine.set_config({"active_hours": "09:00-22:30"})
    dm.time[0] = stamp("2026-11-14T22:15:00")
    dm.service.sender.onebot.ready.clear()
    if manual:
        await dm.manual_digest()
    else:
        await engine.tick_gatekeeper_once()
    receipt = (await dm.digest_history())[0]
    assert receipt["stage"] == "awaiting_qq"
    dm.time[0] = stamp("2026-11-14T22:30:00")
    dm.service.sender.onebot.ready.set()
    assert not await dm.deliver_once()
    assert (await dm.digest_history())[0]["stage"] == "expired"
    if not manual:
        assert await ticket_state(dm, receipt["intent_id"]) == ("expired", "owner_digest_expired")
    await dm.manual_digest()
    assert len(dm.service.ai.calls) == 2 and not dm.service.sender.onebot.calls
    dm.time[0] = stamp("2026-11-15T08:00:00")
    assert (await dm.manual_digest())["reason"] == "outside_digest_window"
    assert len(dm.service.ai.calls) == 2 and not dm.service.sender.onebot.calls


async def test_legacy_queue_only_world_completion_backfills_real_delivery(dm):
    engine = dm.service.life_engine
    await engine.set_config({"active_hours": "09:00-22:30"})
    dm.time[0] = stamp("2026-11-14T22:15:00")
    dm.service.sender.onebot.ready.clear()
    await engine.tick_gatekeeper_once()
    key = (await dm.digest_history())[0]["intent_id"]
    async with dm.store.transaction():
        # On the previous release there was neither a generation timestamp nor an intent link.
        await dm.store.db.execute("DELETE FROM owner_digest_receipts")
        await dm.store.db.execute("DELETE FROM owner_digest_intents")
        await dm.store.db.execute(
            "UPDATE scheduled_intents SET status='done',result_reason='owner_digest_queued' "
            "WHERE id=?",
            (key,),
        )
    await dm.initialize()
    await dm.service.world_state.recover_actions()
    assert await ticket_state(dm, key) == ("executing", "owner_digest_awaiting_delivery")
    info = (await dm.digest_history())[0]
    assert info["queued_at"] and info["generated_at"] is None and info["confirmed_at"] is None
    dm.service.sender.onebot.ready.set()
    await dm.deliver_once()
    assert await ticket_state(dm, key) == ("done", "owner_digest_qq_confirmed")


async def test_cutoff_after_screen_cancels_unpurchased_generation(dm, monkeypatch):
    screen = dm.service.ai.screen

    async def screened(*args, **kwargs):
        result = await screen(*args, **kwargs)
        await dm.store.db.execute("INSERT INTO meta VALUES ('budget_overrun','1')")
        return result

    monkeypatch.setattr(dm.service.ai, "screen", screened)
    result = await dm.manual_digest()
    assert result["receipt"]["stage"] == "failed" and result["reason"] == "budget_hard_stop"
    assert [call[0] for call in dm.service.ai.calls] == ["screen"]
    assert not dm.service.sender.onebot.calls


async def test_replacement_owner_cannot_access_queued_report(settings):
    settings.onebot_mode = "ws_reverse"
    gateway, wire = FakeGateway(), Wire()
    wire.ready.clear()
    now = stamp("2026-11-14T15:00:00")
    async with process(settings, lambda: now, gateway, wire) as dm:
        previous = await dm.manual_digest()
        assert previous["receipt"]["stage"] == "awaiting_qq"
    settings.owner_qq_id = "30000"
    wire.ready.set()
    async with process(settings, lambda: now + 60, gateway, wire) as dm:
        assert not await dm.digest_history() and not await dm.deliver_once()
        discarded = await dm.store.one(
            "SELECT status,payload FROM pending_owner_dms WHERE id=?", (previous["id"],)
        )
        assert tuple(discarded) == ("cancelled", None)
        assert not wire.calls


async def test_retry_of_paid_manual_digest_needs_no_budget_lookup(dm, monkeypatch):
    dm.service.sender.onebot.ready.clear()
    first = await dm.manual_digest()
    assert first["receipt"]["stage"] == "awaiting_qq"

    async def ledger_unavailable():
        raise AssertionError("zero-token delivery read the paid-call ledger")

    monkeypatch.setattr(dm.store, "budget_status", ledger_unavailable)
    dm.service.sender.onebot.ready.set()
    result = await dm.manual_digest()
    assert result["id"] == first["id"] and result["receipt"]["stage"] == "confirmed"
    assert len(dm.service.ai.calls) == 2 and len(dm.service.sender.onebot.calls) == 1


async def test_digest_admits_only_public_diary_entries_and_public_reports(dm):
    from hakimi.roaming_logger import RoamingLogger

    now = dm.clock()
    dm.s.roaming_timezone = dm.s.bot_timezone
    diary = dm.service.diary = RoamingLogger(dm.s, dm.store)
    await diary.initialize()
    day = dm.local(now).date().isoformat()
    async with dm.store.transaction():
        for kind in ("patrol", "chat_group", "chat_private", "world_life"):
            await dm.store.db.execute(
                "INSERT INTO roaming_events VALUES (?,?,?,?)",
                (kind, day, dm.store.vault.seal(kind, "marker-" + kind), kind),
            )
    payload = str(await dm._public_digest(now))
    assert "marker-patrol" in payload and "marker-world_life" in payload
    assert "marker-chat_private" not in payload and "marker-chat_group" not in payload
    dm.service.diary = None
    subject = dm.store.subject(dm.target())
    for body, public in [("known-public", True), ("owner-only-marker", False)]:
        await dm.store.db.execute(
            "INSERT INTO reports(subject,payload,created,public) VALUES(?,?,?,?)",
            (subject, dm.store.vault.seal(subject, body), now, public),
        )
    assert (await dm._public_digest(now))["public_summaries"] == ["known-public"]
