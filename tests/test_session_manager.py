import aiosqlite
import pytest

from hakimi.security import Rejected
from hakimi.session_manager import SessionManager
from hakimi.store import Store


async def test_physical_group_private_isolation_and_restart(settings, store, message):
    sessions = SessionManager(settings, store)
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    other_member = group.model_copy(update={"user_id": "30000"})
    other_group = group.model_copy(update={"conversation_id": "50000"})
    private2 = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    await sessions.remember(message, "owner-private-secret", "private-response")
    await sessions.remember(group, "shared-group-text", "group-response")
    assert sessions.path(message).parent.name == "private"
    assert sessions.path(group).parent.name == "group"
    assert sessions.path(message) != sessions.path(group)
    assert sessions.path(other_member) == sessions.path(group)
    recovered = SessionManager(settings, store)
    assert (await recovered.history(other_member))[0]["user"] == "shared-group-text"
    assert (await recovered.history(message))[0]["user"] == "owner-private-secret"
    assert await recovered.history(other_group) == []
    assert await recovered.history(private2) == []
    for path in settings.data_dir.glob("sessions/*/*.sqlite3"):
        assert b"owner-private-secret" not in path.read_bytes()
        assert path.stat().st_mode & 0o077 == 0


async def test_restore_after_store_close_and_reopen(settings, message):
    first = Store(settings)
    await first.open()
    try:
        await SessionManager(settings, first).remember(message, "persisted", "reply")
    finally:
        await first.close()
    second = Store(settings)
    await second.open()
    try:
        assert (await SessionManager(settings, second).history(message))[0]["user"] == "persisted"
    finally:
        await second.close()


async def test_bounded_fallback_window_and_clear_preserves_switch(settings, store, message):
    sessions = SessionManager(settings, store)
    for n in range(44):
        await sessions.remember(message, str(n), "reply")
    assert [x["user"] for x in await sessions.history(message)] == list(map(str, range(4, 44)))
    await sessions.clear(message)
    assert await sessions.history(message) == []
    await sessions.remember(message, "new-session", "reply")
    assert len(await sessions.history(message)) == 1
    await sessions.clear(message, enabled=False)
    await sessions.remember(message, "disabled", "reply")
    assert await sessions.history(message) == []


async def test_legacy_private_migration_once_and_group_not_merged(settings, store, message):
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    for current in (message, group):
        scope = store.scope(current)
        await store.set_memory(scope, True)
        await store.remember(scope, "old-memory", "reply")
    sessions = SessionManager(settings, store)
    assert len(await sessions.history(message)) == 1
    assert len(await sessions.history(message)) == 1
    assert await sessions.history(group) == []
    await sessions.clear(message)
    assert await sessions.history(message) == []
    assert await store.history(store.scope(message)) == []


async def test_ciphertext_swapped_between_databases_fails(settings, store, message):
    sessions = SessionManager(settings, store)
    other = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    await sessions.remember(message, "secret", "reply")
    await sessions.remember(other, "other", "reply")
    async with aiosqlite.connect(sessions.path(message)) as db:
        async with db.execute("SELECT payload FROM recent_messages") as cur:
            blob = (await cur.fetchone())[0]
    async with aiosqlite.connect(sessions.path(other)) as db:
        await db.execute("UPDATE recent_messages SET payload=?", (blob,))
        await db.commit()
    with pytest.raises(Rejected, match="ciphertext_context_mismatch"):
        await sessions.history(other)


async def test_members_of_one_group_are_serialized_in_queue(store, message):
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    await store.admit(group)
    await store.admit(group.model_copy(update={"user_id": "30000", "event_id": "next"}))
    first = await store.claim()
    assert first
    assert await store.claim() is None
    await store.finish(first["id"])
    assert await store.claim()


async def test_pruning_removes_expired_idle_session(settings, store, message):
    sessions = SessionManager(settings, store)
    await sessions.remember(message, "old", "reply")
    async with aiosqlite.connect(sessions.path(message)) as db:
        await db.execute("UPDATE recent_messages SET expires=0")
        await db.commit()
    await sessions.prune()
    assert await sessions.history(message) == []
