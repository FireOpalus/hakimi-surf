import asyncio
import json
import sqlite3
import time

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.memory_compressor import MemoryCompressor
from hakimi.session_manager import SessionManager
from tests.fakes import FakeGateway
from tests.test_dual_memory_v100 import group, seed
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


def archive_response():
    return httpx.Response(
        200,
        json={
            **interaction_response(),
            "steps": [
                {
                    "type": "model_output",
                    "content": [{"type": "text", "text": '{"profiles":[],"chronicles":[]}'}],
                }
            ],
        },
    )


@pytest.mark.parametrize("recovers", [True, False])
async def test_capacity_schedule_survives_restarts_and_keeps_new_slices_separate(
    settings, store, message, sdk_pair, recovers
):
    now = 1_800_000_000
    calls = []
    available = False

    def wall_clock():
        return now

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        assert json.loads(request.content)["service_tier"] == "flex"
        if available:
            return archive_response()
        return httpx.Response(503, json={"error": {"code": 503, "message": "capacity"}})

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    sessions, msg = SessionManager(settings, store), group(message)
    await seed(sessions, msg)
    first_slice = await sessions.dual.snapshot_cold(msg, force=True)
    for number, delay in enumerate((60, 120, 900, 3600), start=1):
        sessions = SessionManager(settings, store)
        worker = MemoryCompressor(settings, sessions, gateway, store, wall_clock=wall_clock)
        result = await worker.extract(msg, background=True)
        due = now + delay
        assert result == {
            "status": "provider_backoff",
            "reason": "flex_capacity_busy",
            "retry_number": number,
            "next_retry_at": due,
        }
        if number == 1:
            await seed(sessions, msg, text="刚来的另一批")
            await sessions.dual.snapshot_cold(msg, force=True)
        assert (await sessions.dual.view(msg))["flex_retry"] == {
            "retry_number": number,
            "next_retry_at": due,
            "slices": 1,
        }
        now = due - 1
        after_restart = SessionManager(settings, store)
        restarted = MemoryCompressor(settings, after_restart, gateway, store, wall_clock=wall_clock)
        count = len(calls)
        assert (await restarted.extract(msg, force=True))["next_retry_at"] == due
        assert await after_restart.dual.claim(msg, now=now) is None
        assert len(calls) == count
        now = due

    available = recovers
    result = await worker.extract(msg, background=True)
    assert result["status"] == ("compressed" if recovers else "failed")
    view = await sessions.dual.view(msg)
    assert view["flex_retry"] is None
    assert view["jobs"] == {"pending": 1, ("done" if recovers else "failed"): 1}
    async with sessions.database(msg, preserve_history=True) as (db, _):
        async with db.execute(
            "SELECT error,flex_failures,retry_at FROM memory_slice_jobs WHERE id=?", (first_slice,)
        ) as cur:
            assert await cur.fetchone() == (
                (None, 0, 0) if recovers else ("flex_capacity_exhausted", 5, 0)
            )
    events = await store.all(
        "SELECT code,diagnostics FROM audit_events WHERE source='memory' ORDER BY id"
    )
    assert [row["code"] for row in events] == ["memory_flex_retry_scheduled"] * 4 + [
        "memory_flex_retry_succeeded" if recovers else "memory_flex_retry_exhausted"
    ]
    for number, row in enumerate(events[:4], start=1):
        [detail] = json.loads(row["diagnostics"])
        assert detail["http_status"] == 503 and detail["model_lane"] == "memory_flex"
        assert detail["retry_number"] == number
        assert detail["next_retry_at"] == 1_800_000_000 + sum((60, 120, 900, 3600)[:number])
    assert (
        await store.one(
            "SELECT COUNT(DISTINCT work_id) FROM ledger_work WHERE kind='memory.extract'"
        )
    )[0] == 1
    next_batch = await sessions.dual.peek(msg, now=now)
    assert first_slice not in next_batch["ids"] and next_batch["flex_failures"] == 0


async def test_capacity_retry_reset_and_unrelated_503(settings, store, message, sdk_pair):
    source_failure = False

    def handler(request):
        if request.url.path.endswith("decisions") and not source_failure:
            return httpx.Response(200, json=decision_response())
        return httpx.Response(503, json={"error": {"code": 503, "message": "unavailable"}})

    sessions, msg = SessionManager(settings, store), group(message)
    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    worker = MemoryCompressor(settings, sessions, gateway, store)
    await seed(sessions, msg)
    assert (await worker.extract(msg, force=True))["reason"] == "flex_capacity_busy"
    await sessions.clear(msg)
    assert (await sessions.dual.view(msg))["flex_retry"] is None
    assert await sessions.dual.claim(msg) is None
    await seed(sessions, msg, text="新的会话")
    source_failure = True
    assert (await worker.extract(msg, force=True))["status"] == "failed"
    view = await sessions.dual.view(msg)
    assert view["flex_retry"] is None and view["jobs"] == {"failed": 1}


async def test_existing_failed_and_uncertain_jobs_stay_terminal_on_upgrade(
    settings, store, message
):
    sessions, msg = SessionManager(settings, store), group(message)
    for status in ("failed", "attempted"):
        await seed(sessions, msg)
        await sessions.dual.snapshot_cold(msg, force=True)
        job = await sessions.dual.claim(msg)
        if status == "failed":
            await sessions.dual.finish(msg, job, "failed", error="gemini_unavailable")
    with sqlite3.connect(sessions.path(msg)) as db:
        db.execute("ALTER TABLE memory_slice_jobs DROP COLUMN flex_failures")
        db.execute("ALTER TABLE memory_slice_jobs DROP COLUMN retry_at")
        rows = db.execute("SELECT id,payload FROM cold_history_slices ORDER BY id").fetchall()

    upgraded = SessionManager(settings, store)
    for _ in range(2):
        view = await upgraded.dual.view(msg)
        assert view["jobs"] == {"attempted": 1, "failed": 1} and view["flex_retry"] is None
        assert await upgraded.dual.claim(msg) is None
    with sqlite3.connect(sessions.path(msg)) as db:
        assert (
            db.execute("SELECT id,payload FROM cold_history_slices ORDER BY id").fetchall() == rows
        )


async def test_interrupted_archive_is_visible_and_never_dispatched_again(settings, store, message):
    sessions, msg = SessionManager(settings, store), group(message)
    started = asyncio.Event()
    gateway = FakeGateway()

    async def wait_for_reply(*args, **kwargs):
        started.set()
        await asyncio.Future()

    gateway.generate = wait_for_reply
    worker = MemoryCompressor(settings, sessions, gateway, store)
    await seed(sessions, msg)
    task = asyncio.create_task(worker.extract(msg, force=True))
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await sessions.dual.view(msg))["jobs"] == {"unconfirmed": 1}
    assert await sessions.dual.claim(msg) is None
    assert (
        await store.one("SELECT count(*) FROM audit_events WHERE code='memory_archive_unconfirmed'")
    )[0] == 1

    # A hard stop may leave only the original durable claim. Render it truthfully on restart.
    await seed(sessions, msg, text="后来的一批")
    await sessions.dual.snapshot_cold(msg, force=True)
    await sessions.dual.claim(msg, now=time.time() - 86400)
    assert (await sessions.dual.view(msg))["jobs"] == {"unconfirmed": 2}
    assert await sessions.dual.claim(msg) is None
