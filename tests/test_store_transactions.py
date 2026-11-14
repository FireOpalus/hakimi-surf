import asyncio

import anyio
import pytest


@pytest.mark.parametrize("cancel_with", ["asyncio", "anyio"])
@pytest.mark.parametrize("stage", ["begin", "body"])
async def test_cancelled_transaction_finishes_rollback_before_reuse(
    store, monkeypatch, cancel_with, stage
):
    started = asyncio.Event()
    never = asyncio.Event()
    execute = store.db.execute
    rollback = store.db.rollback
    rolled_back = False

    async def delayed_begin(sql, parameters):
        # SQLite can open the transaction before cancellation reaches its caller.
        await execute(sql, parameters)
        started.set()
        await never.wait()

    def execute_with_delayed_begin(sql, parameters=()):
        if stage == "begin" and sql == "BEGIN IMMEDIATE" and not started.is_set():
            return delayed_begin(sql, parameters)
        return execute(sql, parameters)

    async def delayed_rollback():
        nonlocal rolled_back
        # Cleanup must survive AnyIO's repeated cancellation at await points.
        await anyio.lowlevel.checkpoint()
        assert store.lock.locked()
        await rollback()
        rolled_back = True

    monkeypatch.setattr(store.db, "execute", execute_with_delayed_begin)
    monkeypatch.setattr(store.db, "rollback", delayed_rollback)

    async def write():
        async with store.transaction():
            assert stage == "body"
            await store.db.execute("INSERT INTO meta VALUES ('cancelled', 'value')")
            started.set()
            await never.wait()
        pytest.fail("transaction swallowed cancellation")

    with anyio.fail_after(5):
        if cancel_with == "asyncio":
            task = asyncio.create_task(write())
            try:
                await started.wait()
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
        else:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(write)
                await started.wait()
                tasks.cancel_scope.cancel()

    assert rolled_back
    assert not store.db.in_transaction
    assert not store.lock.locked()
    assert await store.one("SELECT value FROM meta WHERE key='cancelled'") is None
    async with store.transaction():
        await store.db.execute("INSERT INTO meta VALUES ('committed', 'value')")
    assert (await store.one("SELECT value FROM meta WHERE key='committed'"))[0] == "value"
