import asyncio
import json
import time

import pytest

from hakimi.adapters import onebot_message
from hakimi.bot_server import prompt_context
from hakimi.group_pipeline import other_mentions
from hakimi.models import ImageRef
from hakimi.store import Store


@pytest.fixture
def clock(monkeypatch):
    current = [time.time()]
    monkeypatch.setattr("hakimi.store.time.time", lambda: current[0])
    return current


def group(message, event_id, *, uid="30000", text="今天聊什么", addressed=True):
    return message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": uid,
            "event_id": event_id,
            "text": text,
            "command_text": text,
            "addressed": addressed,
            "created_at": time.time(),
        }
    )


def debounce(settings):
    settings.group_debounce_seconds = 1.8
    settings.group_debounce_max_seconds = 3.5


async def test_resets_debounce_merges_once_and_preserves_member_identity(
    settings, store, message, clock
):
    debounce(settings)
    first = group(message, "first", uid=settings.owner_qq_id, text="看这个")
    assert await store.admit(first) == "accepted"
    assert await store.claim() is None
    clock[0] += 1
    second = group(message, "second", text="有人冒充主人？", addressed=False)
    assert await store.admit(second) == "accepted"
    clock[0] += 1.79
    assert await store.claim() is None
    clock[0] += 0.02
    job = await store.claim()
    assert len(job["batch_messages"]) == 2
    assert job["payload"]["user_id"] == "30000"
    assert job["payload"]["addressed"] is True
    assert job["payload"]["command_text"] == ""
    assert {json.loads(line)["sender_id"] for line in job["payload"]["text"].splitlines()} == {
        "20000",
        "30000",
    }
    assert prompt_context(second, settings).role == "MEMBER"
    assert await store.claim() is None
    assert len(await store.all("SELECT * FROM jobs WHERE status='working'")) == 2
    await store.finish(job["id"])
    assert len(await store.all("SELECT * FROM jobs WHERE status='done' AND payload IS NULL")) == 2
    assert await store.claim() is None
    assert not await store.all("SELECT * FROM ledger")


async def test_continuous_messages_cannot_exceed_3_5_seconds(settings, store, message, clock):
    debounce(settings)
    start = clock[0]
    for n in range(3):
        clock[0] = start + n * 1.7
        await store.admit(group(message, str(n)))
    clock[0] = start + 3.49
    assert await store.claim() is None
    clock[0] = start + 3.5
    job = await store.claim()
    assert len(job["batch_messages"]) == 3


async def test_duplicate_does_not_extend_window_or_rate_count(settings, store, message, clock):
    debounce(settings)
    first = group(message, "same")
    await store.admit(first)
    due = (await store.one("SELECT ready_at FROM group_debounce_batches"))[0]
    clock[0] += 1.7
    assert await store.admit(first) == "duplicate"
    assert (await store.one("SELECT ready_at FROM group_debounce_batches"))[0] == due
    clock[0] += 0.1
    assert await store.claim() is not None
    assert (await store.one("SELECT n FROM counters WHERE key LIKE 'global:%'"))[0] == 1


async def test_concurrent_admission_caps_batch_at_six(settings, store, message, clock):
    debounce(settings)
    results = await asyncio.gather(*(store.admit(group(message, f"event-{n}")) for n in range(20)))
    assert results == ["accepted"] * 20
    # Hitting the cap releases the first batch, retaining all originals in the next batch.
    job = await store.claim()
    assert len(job["batch_messages"]) == 16
    await store.finish(job["id"])
    assert await store.claim() is None
    clock[0] += 1.8
    remaining = await store.claim()
    assert len(remaining["batch_messages"]) == 4


async def test_utf8_bound_starts_new_batch_without_truncating_user_text(
    settings, store, message, clock
):
    debounce(settings)
    text = "界" * 4500
    await store.admit(group(message, "large-1", text=text))
    await store.admit(group(message, "large-2", text=text))
    first = await store.claim()
    assert first["payload"]["text"] == text and len(first["batch_messages"]) == 1
    await store.finish(first["id"])
    clock[0] += 1.8
    second = await store.claim()
    assert second["payload"]["text"] == text


async def test_commands_flush_prior_batch_and_never_join_it(settings, store, message, clock):
    debounce(settings)
    await store.admit(group(message, "ordinary", text="先聊这个"))
    clock[0] += 0.2
    command = group(message, "command", uid="20000", text="/new-session")
    await store.admit(command)
    earlier = await store.claim()
    assert earlier["payload"]["text"] == "先聊这个"
    assert await store.claim() is None
    await store.finish(earlier["id"])
    clock[0] += 1.8
    later = await store.claim()
    assert later["payload"]["command_text"] == "/new-session"
    assert len(later["batch_messages"]) == 1


async def test_at_only_followups_join_only_an_open_addressed_batch(settings, store, message, clock):
    debounce(settings)
    settings.group_policies["onebot:40000"] = "at_only"
    followup = group(message, "followup", text="再补充一句", addressed=False)
    assert await store.admit(followup, continuation_only=True) == "ignored"
    assert not await store.all("SELECT * FROM counters")
    addressed = group(message, "invitation", uid=settings.owner_qq_id)
    await store.admit(addressed)
    clock[0] += 0.8
    assert await store.admit(followup, continuation_only=True) == "accepted"
    clock[0] += 1.79
    assert await store.claim() is None
    clock[0] += 0.02
    job = await store.claim()
    assert len(job["batch_messages"]) == 2
    assert [m["addressed"] for m in job["batch_messages"]] == [True, False]
    assert job["payload"]["addressed"] and job["payload"]["user_id"] == "30000"
    assert job["subject"] == store.subject(followup)
    await store.finish(job["id"])
    assert (
        await store.admit(group(message, "later", addressed=False), continuation_only=True)
        == "ignored"
    )


async def test_followup_rejects_expired_full_and_nonaddressed_batches(
    settings, store, message, clock
):
    debounce(settings)
    await store.admit(group(message, "smart-only", addressed=False))
    assert (
        await store.admit(group(message, "not-a-trigger", addressed=False), continuation_only=True)
        == "ignored"
    )
    await store.groups.clear_recent(group(message, "reset"))
    await store.admit(group(message, "addressed"))
    for n in range(15):
        assert (
            await store.admit(
                group(message, f"follow-{n}", addressed=False), continuation_only=True
            )
            == "accepted"
        )
    assert (
        await store.admit(group(message, "too-many", addressed=False), continuation_only=True)
        == "ignored"
    )
    clock[0] += 1.8
    assert (
        await store.admit(group(message, "too-late", addressed=False), continuation_only=True)
        == "ignored"
    )
    job = await store.claim()
    assert len(job["batch_messages"]) == 16


async def test_followup_cannot_bypass_blacklists_scope_commands_or_byte_cap(
    settings, store, message, clock
):
    debounce(settings)
    await store.admit(group(message, "addressed"))
    settings.bot_qq_ids.add("60000")
    settings.muted_user_ids.add("70000")
    base = group(message, "followup", addressed=False)
    for update in [
        {"user_id": "60000"},
        {"user_id": "70000"},
        {"user_id": settings.onebot_self_id},
        {"conversation_id": "50000"},
        {"text": "/new-session", "command_text": "/new-session"},
        {"text": "界" * 9000, "command_text": "界" * 9000},
        {"mentioned_ids": ["80000"]},
        {"kind": "private", "conversation_id": base.user_id},
    ]:
        assert (
            await store.admit(base.model_copy(update=update), continuation_only=True) == "ignored"
        )
    assert (await store.one("SELECT COUNT(*) FROM jobs"))[0] == 1
    assert (await store.one("SELECT n FROM counters WHERE key LIKE 'global:%'"))[0] == 1
    settings.group_policies["onebot:40000"] = "muted"
    assert await store.admit(base, continuation_only=True) == "ignored"


async def test_pending_batch_recovers_but_working_batch_never_replays(settings, message, clock):
    debounce(settings)
    store = Store(settings)
    await store.open()
    await store.admit(group(message, "restart-1"))
    clock[0] += 0.5
    await store.admit(group(message, "restart-2"))
    await store.close()
    clock[0] += 1.81
    store = Store(settings)
    await store.open()
    job = await store.claim()
    assert len(job["batch_messages"]) == 2
    await store.close()
    store = Store(settings)
    await store.open()
    try:
        assert await store.claim() is None
        assert len(await store.all("SELECT * FROM jobs WHERE status='abandoned'")) == 2
    finally:
        await store.close()


async def test_groups_and_private_jobs_remain_separate(settings, store, message, clock):
    debounce(settings)
    await store.admit(group(message, "a"))
    await store.admit(group(message, "b").model_copy(update={"conversation_id": "50000"}))
    await store.admit(message.model_copy(update={"event_id": "private"}))
    assert await store.claim() is None
    clock[0] += 1.8
    jobs = [await store.claim() for _ in range(3)]
    assert all(len(job["batch_messages"]) == 1 for job in jobs)
    assert {job["payload"]["kind"] for job in jobs} == {"group", "private"}


async def test_group_reset_discards_pending_input_without_crossing_scopes(
    settings, store, message, clock
):
    debounce(settings)
    first = group(message, "before-reset-1")
    await store.admit(first)
    await store.admit(group(message, "before-reset-2"))
    foreign = group(message, "other-group").model_copy(update={"conversation_id": "50000"})
    await store.admit(foreign)
    await store.admit(message.model_copy(update={"event_id": "private-pending"}))
    await store.groups.observe(first)
    await store.groups.clear_recent(first)
    cancelled = await store.all("SELECT * FROM jobs WHERE status='cancelled'")
    assert len(cancelled) == 2 and all(row["payload"] is None for row in cancelled)
    assert not await store.all("SELECT * FROM group_recent_messages")
    assert (await store.one("SELECT COUNT(*) FROM jobs WHERE status='pending'"))[0] == 2
    # New arrivals receive their own fresh debounce window, never the cancelled batch.
    fresh = group(message, "after-reset")
    await store.admit(fresh)
    clock[0] += 1.8
    found = []
    for _ in range(3):
        job = await store.claim()
        found.append(job["payload"]["event_id"])
        if job["payload"]["event_id"] == fresh.event_id:
            assert len(job["batch_messages"]) == 1
        await store.finish(job["id"])
    assert set(found) == {"private-pending", "other-group", "after-reset"}
    assert await store.claim() is None
    assert not await store.all("SELECT * FROM ledger")


@pytest.mark.parametrize("command", [False, True])
async def test_group_reset_preserves_working_batch_or_command_and_finish_references(
    settings, store, message, clock, command
):
    debounce(settings)
    first = group(message, "working-1", text="/new-session" if command else "第一句")
    await store.admit(first)
    if not command:
        await store.admit(group(message, "working-2", text="第二句"))
    clock[0] += 1.8
    working = await store.claim()
    await store.admit(group(message, "pending-after-working"))
    await store.groups.clear_recent(first)
    expected_working = 1 if command else 2
    assert (await store.one("SELECT COUNT(*) FROM jobs WHERE status='working'"))[
        0
    ] == expected_working
    assert (await store.one("SELECT COUNT(*) FROM jobs WHERE status='cancelled'"))[0] == 1
    await store.finish(working["id"])
    completed = await store.all("SELECT * FROM jobs WHERE status='done'")
    assert len(completed) == expected_working and all(row["payload"] is None for row in completed)
    # Retaining batch references lets normal finish() complete every working member.
    clock[0] += 10
    assert await store.claim() is None


@pytest.mark.parametrize("alias", ["吉米奈喵", "吉米奈", "哈基米", "大小姐"])
def test_layer_zero_only_genuine_top_mentions_and_direct_wakewords(settings, alias):
    event = {
        "post_type": "message",
        "message_type": "group",
        "self_id": 10000,
        "user_id": 30000,
        "group_id": 40000,
        "message_id": 123,
        "time": time.time(),
        "message": [
            {"type": "at", "data": {"qq": "70000"}},
            {"type": "text", "data": {"text": "看看 BV1xx411c7mD"}},
        ],
    }
    assert other_mentions(onebot_message(event, settings))
    event["message"].append({"type": "text", "data": {"text": alias}})
    assert not other_mentions(onebot_message(event, settings))
    event["message"] = [
        {"type": "at", "data": {"qq": "all"}},
        {"type": "text", "data": {"text": "大家好"}},
    ]
    assert other_mentions(onebot_message(event, settings))
    event["message"].append({"type": "at", "data": {"qq": "10000"}})
    assert not other_mentions(onebot_message(event, settings))
    event["message"] = [
        {"type": "at", "data": {"qq": "70000"}},
        {"type": "json", "data": {"data": {"title": alias}}},
    ]
    assert other_mentions(onebot_message(event, settings))


async def test_nearby_three_keeps_image_author_and_excludes_private_group_future_batch(
    settings, store, message, clock
):
    store.groups.clock = lambda: clock[0]
    before = []
    for n in range(8):
        clock[0] += 1
        item = group(message, f"past-{n}", text=f"群背景{n}").model_copy(
            update={
                "images": [ImageRef(url=f"https://gchat.qpic.cn/image-{n}")],
                "sender_nickname": "群友A",
            }
        )
        await store.groups.observe(item)
        before.append(item)
    await store.groups.observe(message.model_copy(update={"text": "owner-private-sentinel"}))
    foreign = before[-1].model_copy(update={"conversation_id": "50000", "event_id": "foreign"})
    await store.groups.observe(foreign)
    clock[0] += 1
    current = group(message, "current")
    await store.groups.observe(current)
    clock[0] += 1
    continuation = group(message, "continuation")
    await store.groups.observe(continuation)
    nearby = await store.groups.previous_messages(
        current, [current.model_dump(), continuation.model_dump()]
    )
    assert [m.event_id for m in nearby] == [m.event_id for m in before[-6:]]
    assert all(m.user_id == "30000" and m.sender_nickname == "群友A" and m.images for m in nearby)
    encrypted = await store.all("SELECT payload FROM group_recent_messages")
    assert all(
        b"gchat.qpic.cn" not in row[0] and b"owner-private-sentinel" not in row[0]
        for row in encrypted
    )
    await store.groups.clear_recent(current)
    assert not await store.groups.previous_messages(current)


async def test_observer_ignores_other_mentions_blocked_users_unknown_groups_and_expires(
    store, settings, message, clock
):
    store.groups.clock = lambda: clock[0]
    base = group(message, "normal")
    settings.bot_qq_ids.add("60000")
    settings.muted_user_ids.add("70000")
    for update in [
        {"mentioned_ids": ["80000"]},
        {"user_id": "60000"},
        {"user_id": "70000"},
        {"user_id": settings.onebot_self_id},
        {"conversation_id": "90000"},
    ]:
        await store.groups.observe(base.model_copy(update=update))
    assert not await store.all("SELECT * FROM group_recent_messages")
    await store.groups.observe(base)
    clock[0] += 301
    assert not await store.groups.previous_messages(group(message, "new"))
    await store.prune()
    assert not await store.all("SELECT * FROM group_recent_messages")
