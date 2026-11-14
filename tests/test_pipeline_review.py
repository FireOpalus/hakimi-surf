"""Review regressions: untrusted receipts and clear-during-delivery isolation."""

import asyncio

import pytest

from hakimi.public_memory import PublicMemory
from hakimi.service import Service
from hakimi.store import Store
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.fixture
async def review_service(settings, store):
    service = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    yield service
    await service.stop()


def public_message(message):
    return message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "event_id": "review-group",
            "sender_nickname": "测试群友",
            "text": "旧群话题",
        }
    )


async def test_rejected_receipt_never_enters_hot_history(review_service, message):
    svc = review_service
    svc.ai.block_purpose = "message"
    await svc.accept(message)
    assert (await svc.sessions.lookup_received(message, message.event_id)).text == message.text
    assert (await svc.sessions.snapshot(message))["recent_messages"] == []
    await svc.process(await svc.store.claim())
    assert (await svc.sessions.snapshot(message))["recent_messages"] == []
    assert not any(call[0] == "generate" for call in svc.ai.calls)


async def test_private_bot_receipt_resolves_only_inside_original_peer(review_service, message):
    svc = review_service
    await svc.sessions.receive_outgoing(message, 987654, "仅此私聊可见", expected_epoch=0)
    reply = message.model_copy(update={"event_id": "reply", "reply_ids": ["987654"]})
    expanded = await svc.context_resolver.resolve(reply)
    assert "我自己(吉米奈喵)" in expanded.text and "仅此私聊可见" in expanded.text
    group = public_message(reply)
    assert await svc.sessions.lookup_received(group, "987654") is None
    expanded_group = await svc.context_resolver.resolve(group)
    assert "仅此私聊可见" not in expanded_group.text


async def test_clear_while_send_ack_pending_does_not_recreate_receipt_or_bridge(
    review_service, message
):
    svc = review_service
    group = public_message(message)
    sending, ack = asyncio.Event(), asyncio.Event()

    async def delayed_ack(*args, **kwargs):
        assert await kwargs["before_send"]()
        sending.set()
        await ack.wait()
        return {"message_id": 90001}

    svc.sender.send = delayed_ack
    await svc.accept(group)
    await svc.process(await svc.store.claim())
    task = asyncio.create_task(svc.deliver_once())
    try:
        await asyncio.wait_for(sending.wait(), 3)
        await svc.sessions.clear(group)
        ack.set()
        await task
        assert await svc.sessions.lookup_received(group, "90001") is None
        assert await svc.public_memory.recent_for(message) == []
        async with svc.store.lock:
            status = await svc.store.one(
                "SELECT status FROM outbox WHERE id=?", (svc.store.event_key(group),)
            )
        assert status[0] == "cancelled"
    finally:
        ack.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_clear_after_confirmed_send_prevents_stale_public_index_write(
    review_service, message, monkeypatch
):
    svc = review_service
    group = public_message(message)
    pending, resume = asyncio.Event(), asyncio.Event()
    original_record = svc.public_memory.record

    async def sent(*args, **kwargs):
        assert await kwargs["before_send"]()
        return {"message_id": 90002}

    async def delayed_record(*args, **kwargs):
        pending.set()
        await resume.wait()
        await original_record(*args, **kwargs)

    svc.sender.send = sent
    monkeypatch.setattr(svc.public_memory, "record", delayed_record)
    await svc.accept(group)
    await svc.process(await svc.store.claim())
    task = asyncio.create_task(svc.deliver_once())
    try:
        await asyncio.wait_for(pending.wait(), 3)
        await svc.sessions.clear(group)
        resume.set()
        await task
        assert await svc.public_memory.recent_for(message) == []
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_public_epoch_tombstone_survives_restart_and_allows_new_epoch(
    review_service, message
):
    svc = review_service
    group = public_message(message)
    await svc.sessions.clear(group)
    await svc.store.close()
    other = Store(svc.s)
    await other.open()
    try:
        memory = PublicMemory(other)
        assert not await memory.record("stale", group, [group], "旧回复", expected_epoch=0)
        assert await memory.record("fresh", group, [group], "新回复", expected_epoch=1)
        records = await memory.recent_for(message)
        assert [record["bot_reply"] for record in records] == ["新回复"]
    finally:
        await other.close()
        await svc.store.open()


async def test_public_read_defensively_ignores_stale_epoch_payload(review_service, message):
    svc = review_service
    group = public_message(message)
    await svc.public_memory.record("old", group, [group], "不应复活", expected_epoch=0)
    async with svc.store.lock:
        old = await svc.store.one("SELECT * FROM public_interactions WHERE id='old'")
    await svc.sessions.clear(group)
    # Even a leftover row from an interrupted migration must not be used as background.
    async with svc.store.transaction():
        await svc.store.db.execute("INSERT INTO public_interactions VALUES (?,?,?,?)", tuple(old))
    assert await svc.public_memory.recent_for(message) == []


async def test_single_group_input_exposes_protocol_sender_as_data(review_service, message):
    svc = review_service
    group = public_message(message)
    await svc.accept(group)
    await svc.process(await svc.store.claim())
    payload = next(call[1] for call in svc.ai.calls if call[0] == "generate")
    assert payload["current_sender"] == {"nickname": "测试群友", "qq": "30000"}
