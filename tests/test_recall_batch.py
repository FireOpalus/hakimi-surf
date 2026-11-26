import asyncio
import time

import pytest

from hakimi.hot_ring import HotRing
from hakimi.models import Message
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender


class _CountingLock:
    def __init__(self, inner: asyncio.Lock) -> None:
        self._inner = inner
        self.acquisitions = 0

    async def __aenter__(self):
        self.acquisitions += 1
        return await self._inner.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        return await self._inner.__aexit__(exc_type, exc, tb)


def _instrument_store(store, monkeypatch):
    lock = _CountingLock(store.lock)
    monkeypatch.setattr(store, "lock", lock)
    queries: list[tuple[str, tuple]] = []
    orig_one = store.one

    async def _spy_one(sql: str, args: tuple = ()):
        queries.append((sql, args))
        return await orig_one(sql, args)

    monkeypatch.setattr(store, "one", _spy_one)
    return lock, queries


def _group_msg(
    message: Message,
    event_id: str,
    *,
    reply_ids: list[str] | None = None,
    conversation_id: str = "40000",
    user_id: str = "30000",
) -> Message:
    return message.model_copy(
        update={
            "event_id": event_id,
            "kind": "group",
            "conversation_id": conversation_id,
            "user_id": user_id,
            "reply_ids": list(reply_ids or []),
        }
    )


async def _insert_recall(store, msg: Message, ref: str | None = None) -> str:
    target = msg if ref is None else msg.model_copy(update={"event_id": ref})
    event = store.event_key(target)
    async with store.transaction():
        await store.db.execute(
            "INSERT OR REPLACE INTO message_recalls VALUES (?,?)",
            (event, time.time()),
        )
    return event


@pytest.fixture
async def service(settings, store):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        yield svc
    finally:
        await svc.stop()


async def test_multi_member_batch_uses_single_lock_and_single_query(
    service, store, message, monkeypatch
):
    members = [
        _group_msg(message, "e1", reply_ids=["r1", "r2"]),
        _group_msg(message, "e2", reply_ids=["r3"]),
        _group_msg(message, "e3", reply_ids=["r4", "r5"]),
    ]
    lock, queries = _instrument_store(store, monkeypatch)

    assert await service.recalled(members) is False
    assert lock.acquisitions == 1
    assert len(queries) == 1
    sql, params = queries[0]
    assert "FROM message_recalls WHERE event IN (" in sql
    assert "LIMIT 1" in sql
    assert len(params) == 8


async def test_hot_cache_short_circuits_without_lock_or_sql(service, store, message, monkeypatch):
    m1 = _group_msg(message, "e1", reply_ids=["r1"])
    m2 = _group_msg(message, "e2", reply_ids=["r2", "r3"])
    # Mark a later member's nested reply_id in HotRing; earlier refs are NOT in HotRing.
    service.hot.recall(m2, "r3")
    lock, queries = _instrument_store(store, monkeypatch)

    assert await service.recalled([m1, m2]) is True
    assert lock.acquisitions == 0
    assert queries == []


async def test_hot_cache_short_circuits_even_when_earlier_member_recalled_in_db(
    service, store, message, monkeypatch
):
    m1 = _group_msg(message, "e1")
    m2 = _group_msg(message, "e2")
    await _insert_recall(store, m1)
    service.hot.recall(m2, "e2")
    lock, queries = _instrument_store(store, monkeypatch)

    assert await service.recalled([m1, m2]) is True
    assert lock.acquisitions == 0
    assert queries == []


async def test_empty_members_performs_no_lock_or_sql(service, store, monkeypatch):
    lock, queries = _instrument_store(store, monkeypatch)

    assert await service.recalled([]) is False
    assert await service.recalled(x for x in ()) is False
    assert lock.acquisitions == 0
    assert queries == []


async def test_recall_hits_on_event_id_and_nested_reply_ids_in_sqlite(
    service, store, message, monkeypatch
):
    m1 = _group_msg(message, "101", reply_ids=["90"])
    m2 = _group_msg(message, "102", reply_ids=["91", "92"])
    m3 = _group_msg(message, "103", reply_ids=["93"])

    # 1. Hit on a member's own event_id (persisted in SQLite, HotRing empty)
    await _insert_recall(store, m2)
    service.hot = HotRing()
    lock, queries = _instrument_store(store, monkeypatch)
    assert await service.recalled([m1, m2, m3]) is True
    assert lock.acquisitions == 1
    assert len(queries) == 1

    # Clear recalls table and verify nested reply_id hit across a different member
    async with store.transaction():
        await store.db.execute("DELETE FROM message_recalls")
    await _insert_recall(store, m3, ref="92")

    lock2, queries2 = _instrument_store(store, monkeypatch)
    # m1 & m3 don't reference "92", so batch [m1, m3] misses
    assert await service.recalled([m1, m3]) is False
    assert lock2.acquisitions == 1
    assert len(queries2) == 1

    # Including m2 (which has reply_id="92") hits in SQLite with 1 query
    assert await service.recalled([m1, m2, m3]) is True
    assert lock2.acquisitions == 2
    assert len(queries2) == 2


async def test_duplicate_refs_deduplicated_while_preserving_conversation_isolation(
    service, store, message, monkeypatch
):
    # Same conversation: overlapping event_ids and reply_ids collapse to 3 unique keys (e1, e2, e3)
    m1 = _group_msg(message, "e1", reply_ids=["e2", "e1"])
    m2 = _group_msg(message, "e2", reply_ids=["e1", "e3"])
    m3 = _group_msg(message, "e1", reply_ids=["e3", "e2"])
    # Different conversation with same event_id "e1": must keep a distinct event_key
    m_other = _group_msg(message, "e1", conversation_id="50000")

    lock, queries = _instrument_store(store, monkeypatch)
    assert await service.recalled([m1, m2, m3, m_other]) is False
    assert lock.acquisitions == 1
    assert len(queries) == 1
    _, params = queries[0]
    expected_keys = [
        store.event_key(m1.model_copy(update={"event_id": "e1"})),
        store.event_key(m1.model_copy(update={"event_id": "e2"})),
        store.event_key(m1.model_copy(update={"event_id": "e3"})),
        store.event_key(m_other.model_copy(update={"event_id": "e1"})),
    ]
    assert list(params) == expected_keys

    # Recalling "e1" in group 50000 must NOT flag group 40000, and MUST flag group 50000
    await _insert_recall(store, m_other)
    assert await service.recalled([m1, m2, m3]) is False
    assert await service.recalled([m1, m_other]) is True


async def test_large_batch_chunks_under_single_lock_and_short_circuits_on_hit(
    service, store, message, monkeypatch
):
    # 1050 unique refs -> chunks of 500, 500, 50
    members = [_group_msg(message, f"ev-{i}") for i in range(1050)]

    # 1. Miss across all chunks: 1 lock, 3 chunked queries (500, 500, 50)
    lock, queries = _instrument_store(store, monkeypatch)
    assert await service.recalled(members) is False
    assert lock.acquisitions == 1
    assert [len(params) for _, params in queries] == [500, 500, 50]

    # 2. Hit in the final chunk (index 1049): 1 lock, 3 queries, returns True
    await _insert_recall(store, members[1049])
    lock2, queries2 = _instrument_store(store, monkeypatch)
    assert await service.recalled(members) is True
    assert lock2.acquisitions == 1
    assert [len(params) for _, params in queries2] == [500, 500, 50]

    # 3. Hit in the second chunk (index 600): 1 lock, stops after 2 queries
    await _insert_recall(store, members[600])
    lock3, queries3 = _instrument_store(store, monkeypatch)
    assert await service.recalled(members) is True
    assert lock3.acquisitions == 1
    assert [len(params) for _, params in queries3] == [500, 500]

    # 4. Hit in the first chunk (index 5): 1 lock, stops after 1 query
    await _insert_recall(store, members[5])
    lock4, queries4 = _instrument_store(store, monkeypatch)
    assert await service.recalled(members) is True
    assert lock4.acquisitions == 1
    assert [len(params) for _, params in queries4] == [500]
