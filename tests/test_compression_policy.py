import asyncio
from contextlib import asynccontextmanager

import pytest
from pydantic import ValidationError

from hakimi.compression_policy import CompressionConfig
from hakimi.config import Settings
from hakimi.memory_compressor import MemoryCompressor
from hakimi.session_manager import SessionManager
from tests.fakes import FakeGateway


async def fill_exact(sessions, message, count, chars):
    width, remainder = divmod(chars, count)
    texts = ["字" * (width + int(n < remainder)) for n in range(count)]
    for n in range(0, count, 2):
        await sessions.remember(message, texts[n], texts[n + 1] if n + 1 < count else "")
    if count % 2:
        async with sessions.database(message) as (db, _):
            await db.execute(
                "DELETE FROM recent_messages WHERE id=(SELECT MAX(id) FROM recent_messages)"
            )


@pytest.mark.parametrize(
    "kind,count,chars,expected",
    [
        ("group", 58, 5999, False),
        ("group", 58, 6000, True),
        ("group", 60, 1199, False),
        ("group", 60, 1200, True),
        ("group", 60, 6000, True),
        ("group", 89, 1199, False),
        ("group", 90, 1199, True),
        ("group", 90, 180, True),
        ("group", 25, 6000, False),
        ("group", 26, 6000, True),
        ("private", 38, 5999, False),
        ("private", 38, 6000, True),
        ("private", 40, 80, True),
        ("private", 16, 6000, False),
        ("private", 18, 6000, True),
    ],
)
async def test_exact_boundaries_keep_tail_and_do_not_pay_for_deferred_messages(
    settings, store, message, kind, count, chars, expected
):
    msg = (
        message.model_copy(update={"kind": kind, "conversation_id": "40000"})
        if kind == "group"
        else message
    )
    sessions = SessionManager(settings, store)
    ai = FakeGateway()
    compressor = MemoryCompressor(settings, sessions, ai, store)
    await fill_exact(sessions, msg, count, chars)
    before = await sessions.snapshot(msg)
    assert len(before["recent_messages"]) == count  # No legacy 40-message truncation.
    assert sum(len(m["text"]) for m in before["recent_messages"]) == chars
    result = await compressor.compress(msg)
    assert (result["status"] == "compressed") is expected
    assert [call[0] for call in ai.calls] == (["screen", "generate"] if expected else [])
    after = await sessions.snapshot(msg)
    keep = 25 if kind == "group" else 16
    assert after["recent_messages"] == (
        before["recent_messages"][-keep:] if expected else before["recent_messages"]
    )
    if not expected:
        async with sessions.database(msg) as (db, _):
            async with db.execute("SELECT attempt,retry_after FROM rolling_state") as cur:
                assert await cur.fetchone() == (None, 0)
    elif kind == "group":
        assert after["recent_messages"][0]["role"] == "assistant"
        # Legacy history consumers must not interpret the retained leading reply as a user.
        history = await sessions.history(msg)
        assert len(history) == 12
        assert [p["user"] for p in history] == [
            m["text"] for m in after["recent_messages"] if m["role"] == "user"
        ]


async def test_manual_compression_bypasses_deferral_but_preserves_keep(settings, store, message):
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    sessions = SessionManager(settings, store)
    ai = FakeGateway()
    compressor = MemoryCompressor(settings, sessions, ai, store)
    await fill_exact(sessions, group, 60, 120)
    assert (await compressor.compress(group))["status"] == "not_due_or_cooling_down"
    assert (await compressor.compress(group, force=True))["status"] == "compressed"
    assert len((await sessions.snapshot(group))["recent_messages"]) == 25


@pytest.mark.parametrize("change", ["keep", "enabled"])
async def test_live_policy_change_invalidates_inflight_compression(
    settings, store, message, change
):
    sessions, ai = SessionManager(settings, store), FakeGateway()
    compressor = MemoryCompressor(settings, sessions, ai, store)
    await fill_exact(sessions, message, 40, 2000)
    entered, release = asyncio.Event(), asyncio.Event()

    async def generate(*args, **kwargs):
        entered.set()
        await release.wait()
        return "old policy result"

    ai.generate = generate
    task = asyncio.create_task(compressor.compress(message))
    await asyncio.wait_for(entered.wait(), 3)
    data = store.runtime.compression.model_dump()
    if change == "keep":
        data["private"]["keep"] = 20
    else:
        data["enabled"] = False
    await store.runtime.set_compression(CompressionConfig.model_validate(data))
    release.set()
    assert (await task)["status"] == "stale"
    after = await sessions.snapshot(message)
    assert len(after["recent_messages"]) == 40 and after["rolling_summary"] == ""


def test_env_thresholds_and_validation_are_shared_with_admin(settings, tmp_path):
    path = tmp_path / ".env"
    path.write_text(
        "HAKIMI_GROUP_COMPRESSION_MESSAGES=70\nHAKIMI_GROUP_COMPRESSION_CHARS=7000\n"
        "HAKIMI_GROUP_COMPRESSION_KEEP=27\nHAKIMI_GROUP_COMPRESSION_MIN_CHARS=1400\n"
        "HAKIMI_GROUP_COMPRESSION_MAX_MESSAGES=95\nHAKIMI_PRIVATE_COMPRESSION_MESSAGES=42\n"
        "HAKIMI_PRIVATE_COMPRESSION_CHARS=6500\nHAKIMI_PRIVATE_COMPRESSION_KEEP=18\n"
    )
    fields = {k: v for k, v in settings.model_dump().items() if "compression" not in k}
    loaded = Settings(_env_file=path, **fields)
    policy = CompressionConfig.from_settings(loaded)
    assert policy.group.model_dump() == {
        "messages": 70,
        "chars": 7000,
        "keep": 27,
        "min_chars": 1400,
        "max_messages": 95,
    }
    assert policy.private.model_dump() == {"messages": 42, "chars": 6500, "keep": 18}
    path.write_text("HAKIMI_GROUP_COMPRESSION_KEEP=60\n")
    with pytest.raises(ValidationError):
        Settings(_env_file=path, **fields)


async def test_policy_write_and_compression_commit_have_one_consistent_order(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    await fill_exact(sessions, message, 40, 2000)
    snapshot = await sessions.compression_snapshot(message)
    entered, release = asyncio.Event(), asyncio.Event()
    database = sessions.database

    @asynccontextmanager
    async def delayed_database(msg):
        async with database(msg) as connection:
            entered.set()
            await release.wait()
            yield connection

    monkeypatch.setattr(sessions, "database", delayed_database)
    commit = asyncio.create_task(sessions.commit_compression(message, snapshot, "summary"))
    await asyncio.wait_for(entered.wait(), 3)
    changed = store.runtime.compression.model_dump()
    changed["private"]["keep"] = 20
    update = asyncio.create_task(
        store.runtime.set_compression(CompressionConfig.model_validate(changed))
    )
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(update), 0.1)
    finally:
        release.set()
        assert await commit
        await update
    assert store.runtime.compression.private.keep == 20
    # Commit won the race and finished under the old policy, then the update was acknowledged.
    assert len((await sessions.snapshot(message))["recent_messages"]) == 16
