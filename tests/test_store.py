import asyncio
import time
from decimal import Decimal

import pytest

from hakimi.security import Rejected
from hakimi.store import Store


async def test_duplicate_admission_is_atomic(store, message):
    results = await asyncio.gather(*(store.admit(message) for _ in range(20)))
    assert results.count("accepted") == 1
    assert results.count("duplicate") == 19


async def test_scope_claims_are_serial_and_queue_is_bounded(store, message):
    store.s.queue_capacity = 2
    assert await store.admit(message) == "accepted"
    assert await store.admit(message.model_copy(update={"event_id": "event-2"})) == "accepted"
    assert await store.admit(message.model_copy(update={"event_id": "event-3"})) == "full"
    first = await store.claim()
    assert first is not None
    assert await store.claim() is None
    await store.finish(first["id"])
    assert (await store.claim())["payload"]["event_id"] == "event-2"


async def test_rates_are_persistent_and_shared_across_conversations(store, message):
    store.s.user_requests_per_minute = 1
    assert await store.admit(message) == "accepted"
    group = message.model_copy(
        update={"event_id": "new", "kind": "group", "conversation_id": "40000"}
    )
    assert await store.admit(group) == "limited"
    assert await store.admit(message) == "duplicate"


async def test_memory_opt_in_boundaries_encryption_ttl_and_deletion(store, message):
    scope = store.scope(message)
    await store.remember(scope, "my-super-private-text", "reply")
    assert await store.history(scope) == []
    await store.set_memory(scope, True)
    await store.remember(scope, "my-super-private-text", "reply")
    assert (await store.history(scope))[0]["user"] == "my-super-private-text"
    for change in (
        {"user_id": "someone-else"},
        {"kind": "group", "conversation_id": "40000"},
        {"platform": "qq"},
        {"bot_id": "other-bot"},
        {"conversation_id": "another-conversation"},
    ):
        assert await store.history(store.scope(message.model_copy(update=change))) == []
    for path in store.s.data_dir.glob("hakimi.sqlite3*"):
        assert b"my-super-private-text" not in path.read_bytes()
    blob = store.vault.seal(scope, {"text": "private"})
    with pytest.raises(Rejected, match="ciphertext_context_mismatch"):
        store.vault.open("another-scope", blob)
    async with store.transaction():
        await store.db.execute("UPDATE memory SET expires=?", (time.time() - 1,))
    assert await store.history(scope) == []
    await store.remember(scope, "new", "reply")
    await store.set_memory(scope, False)
    assert await store.history(scope) == []
    assert not await store.memory_enabled(scope)


async def test_memory_turns_are_bounded(store, message):
    scope = store.scope(message)
    await store.set_memory(scope, True)
    for i in range(10):
        await store.remember(scope, str(i), "reply")
    assert [h["user"] for h in await store.history(scope)] == ["6", "7", "8", "9"]


async def test_budget_atomic_under_concurrency(store):
    store.s.daily_usd = Decimal("1")
    store.s.user_daily_usd = Decimal("100")
    results = await asyncio.gather(
        *(store.reserve(f"user-{i}", "chat", Decimal("0.2")) for i in range(20)),
        return_exceptions=True,
    )
    assert sum(isinstance(r, str) for r in results) == 5
    assert (await store.usage())["lanes"][0]["amount"] == 1_000_000


async def test_patrol_and_per_user_budget_caps(store):
    store.s.patrol_daily_usd = Decimal("0.05")
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("p", "patrol", Decimal("0.06"))
    store.s.user_daily_usd = Decimal("0.01")
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("p", "chat", Decimal("0.02"))


async def test_actual_usage_refunds_only_confirmed_unused_reservation(store):
    first = await store.reserve("s", "chat", Decimal("0.1"))
    await store.settle(first, Decimal("0.03"))
    second = await store.reserve("s", "chat", Decimal("0.1"))
    await store.settle(second, None)
    assert (await store.usage())["lanes"][0]["amount"] == 130_000


async def test_overrun_latches_circuit(store):
    reservation = await store.reserve("s", "chat", Decimal("0.01"))
    with pytest.raises(Rejected, match="budget_reservation_exceeded"):
        await store.settle(reservation, Decimal("0.02"))
    with pytest.raises(Rejected, match="budget_circuit_open"):
        await store.reserve("other", "chat", Decimal("0.01"))
    assert (await store.usage())["lanes"][0]["amount"] == 20_000


async def test_crash_recovery_does_not_repeat_paid_or_sent_work(settings, message):
    first = Store(settings)
    await first.open()
    await first.admit(message)
    claimed = await first.claim()
    reservation = await first.reserve("s", "chat", Decimal("0.1"))
    await first.enqueue_reply(claimed["id"], message, "sensitive-reply")
    assert await first.claim_reply()
    await first.close()
    second = Store(settings)
    await second.open()
    try:
        assert await second.claim() is None
        assert await second.claim_reply() is None
        assert await second.admit(message) == "duplicate"
        assert (await second.usage())["lanes"][0]["amount"] == 100_000
        await second.settle(reservation, None)
    finally:
        await second.close()


async def test_second_process_store_is_rejected_before_recovery(store, settings):
    second = Store(settings)
    with pytest.raises(RuntimeError, match="one service process"):
        await second.open()
    await second.close()
