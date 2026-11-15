import asyncio
import time

import pytest

from hakimi.service import Service
from hakimi.session_scheduler import SessionScheduler
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.fixture
async def pipeline(settings, store):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    yield svc
    await svc.stop()


async def test_private_receipt_precedes_debounce_and_llm(pipeline, message, monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr("hakimi.store.time.time", lambda: clock[0])
    pipeline.s.group_debounce_seconds = 1.8
    pipeline.s.group_debounce_max_seconds = 3.5
    first = message.model_copy(update={"event_id": "1", "created_at": clock[0]})
    assert await pipeline.accept(first) == "accepted"
    assert (await pipeline.sessions.lookup_received(first, "1")).text == first.text
    assert not pipeline.ai.calls
    assert await pipeline.store.claim() is None
    clock[0] += 0.3
    second = first.model_copy(
        update={"event_id": "2", "text": "再补充一句", "created_at": clock[0]}
    )
    assert await pipeline.accept(second) == "accepted"
    clock[0] += 0.5
    assert await pipeline.store.claim() is None
    clock[0] += 0.2
    job = await pipeline.store.claim()
    assert len(job["batch_messages"]) == 2
    await pipeline.process(job)
    await pipeline.deliver_once()
    assert sum(c[0] == "generate" for c in pipeline.ai.calls) == 1
    history = (await pipeline.sessions.snapshot(first))["recent_messages"]
    assert [r["role"] for r in history] == ["user", "user", "assistant"]
    assert [r["text"] for r in history[:2]] == [first.text, second.text]


async def test_same_sender_busy_followup_merges_before_any_reply(pipeline, message, monkeypatch):
    first = message.model_copy(update={"event_id": "first"})
    await pipeline.accept(first)
    job = await pipeline.store.claim()
    entered, resume = asyncio.Event(), asyncio.Event()
    original = pipeline.ai.generate

    async def blocked(*args, **kwargs):
        entered.set()
        await resume.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(pipeline.ai, "generate", blocked)
    task = asyncio.create_task(pipeline.process(job))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await pipeline.accept(
            first.model_copy(update={"event_id": "later", "text": "补充截图说明"})
        )
        await asyncio.wait_for(task, 3)
        assert not await pipeline.store.claim_reply()
        pending = await pipeline.store.claim()
        assert len(pending["batch_messages"]) == 2
        resume.set()
        await pipeline.process(pending)
        calls = [c for c in pipeline.ai.calls if c[0] == "generate"]
        assert len(calls) == 1
        assert "补充截图说明" in calls[0][1]["current_message"]
        await pipeline.deliver_once()
        assert len(pipeline.sender.sent) == 1
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_named_session_locks_serialize_only_the_same_session():
    scheduler = SessionScheduler()
    first, other, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    order = []

    async def work(key, label):
        async with scheduler.hold(key):
            order.append(label)
            if label == "first":
                first.set()
                await release.wait()
            if label == "other":
                other.set()

    tasks = [asyncio.create_task(work("group:1", "first"))]
    await first.wait()
    tasks.extend(
        [
            asyncio.create_task(work("group:1", "second")),
            asyncio.create_task(work("private:2", "other")),
        ]
    )
    await asyncio.wait_for(other.wait(), 2)
    assert order == ["first", "other"]
    release.set()
    await asyncio.gather(*tasks)
    assert order == ["first", "other", "second"] and scheduler.entries == {}


async def test_public_memory_bridge_is_owner_private_only(pipeline, message):
    original_send = pipeline.sender.send

    async def confirmed_send(*args, **kwargs):
        await original_send(*args, **kwargs)
        return {"message_id": 90001}

    pipeline.sender.send = confirmed_send
    group = message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "sender_nickname": "普通群友",
            "group_name": "测试群",
            "event_id": "g",
            "text": "今天聊猫猫",
        }
    )
    await pipeline.accept(group)
    await pipeline.process(await pipeline.store.claim())
    assert await pipeline.public_memory.recent_for(message) == []  # Not sent yet.
    assert await pipeline.deliver_once()
    rows = await pipeline.public_memory.recent_for(message)
    assert len(rows) == 1 and rows[0]["group_name"] == "测试群"
    assert await pipeline.public_memory.recent_for(group) == []
    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    assert await pipeline.public_memory.recent_for(friend) == []
    owner_question = message.model_copy(update={"event_id": "private-question"})
    await pipeline.accept(owner_question)
    await pipeline.process(await pipeline.store.claim())
    latest = [c for c in pipeline.ai.calls if c[0] == "generate"][-1][1]
    assert latest["近期群聊巡逻记忆（只读背景）"][0]["speakers"][0]["qq"] == "30000"
    await pipeline.sessions.clear(group)
    assert await pipeline.public_memory.recent_for(message) == []


async def test_local_reply_lookup_never_crosses_private_or_group_sessions(pipeline, message):
    await pipeline.sessions.receive(
        message.model_copy(update={"event_id": "100", "text": "private-secret"})
    )
    group = message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "event_id": "101",
            "reply_ids": ["100"],
        }
    )
    assert await pipeline.sessions.lookup_received(group, "100") is None
    assert await pipeline.accept(group) == "accepted"
    stored = await pipeline.sessions.lookup_received(group, "101")
    assert "private-secret" not in stored.text
