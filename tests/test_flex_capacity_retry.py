import asyncio
import json
import sqlite3
import time

import aiosqlite
import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.dual_memory import EXTRACTION_INSTRUCTION, EXTRACTION_RESPONSE_FORMAT
from hakimi.gemini import GeminiGateway
from hakimi.memory_compressor import MemoryCompressor
from hakimi.security import Rejected
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
    decision_calls = []
    interaction_tiers = []
    available = False

    def wall_clock():
        return now

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("decisions"):
            decision_calls.append(request.url.path)
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        tier = body.get("service_tier")
        interaction_tiers.append(tier)
        if len(interaction_tiers) <= 5:
            assert tier == "flex"
        else:
            assert tier is None
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
        # Source screening runs once on attempt 1 and is skipped on subsequent retries.
        assert len(decision_calls) == 1
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
    if recovers:
        assert interaction_tiers == ["flex"] * 5
        # 1 source screen on attempt 1 + 1 output screen after attempt 5 succeeds.
        assert len(decision_calls) == 2
    else:
        # 5 Flex 503s + 1 standard-tier fallback (no service_tier) that also 503s.
        assert interaction_tiers == ["flex"] * 5 + [None]
        assert len(decision_calls) == 1
    view = await sessions.dual.view(msg)
    assert view["flex_retry"] is None
    assert view["jobs"] == {"pending": 1, ("done" if recovers else "failed"): 1}
    async with sessions.database(msg, preserve_history=True) as (db, _):
        async with db.execute(
            "SELECT error,flex_failures,retry_at,screened FROM memory_slice_jobs WHERE id=?",
            (first_slice,),
        ) as cur:
            assert await cur.fetchone() == (
                (None, 0, 0, 1) if recovers else ("flex_capacity_exhausted", 5, 0, 1)
            )
    events = await store.all(
        "SELECT code,diagnostics FROM audit_events WHERE source='memory' ORDER BY id"
    )
    if recovers:
        assert [row["code"] for row in events] == ["memory_flex_retry_scheduled"] * 4 + [
            "memory_flex_retry_succeeded"
        ]
    else:
        assert [row["code"] for row in events] == ["memory_flex_retry_scheduled"] * 4 + [
            "gemini_unavailable",
            "memory_flex_retry_exhausted",
        ]
        exhausted_diags = json.loads(events[-1]["diagnostics"])
        assert [d["model_lane"] for d in exhausted_diags] == ["memory_flex", "hydrogen"]
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


async def test_flex_exhaustion_falls_back_to_standard_tier_and_succeeds_end_to_end(
    settings, store, message, sdk_pair
):
    now = 1_800_000_000
    decision_calls = []
    interaction_bodies = []

    def wall_clock():
        return now

    def handler(request):
        if request.url.path.endswith("decisions"):
            decision_calls.append(json.loads(request.content))
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        interaction_bodies.append(body)
        if body.get("service_tier") == "flex":
            return httpx.Response(503, json={"error": {"code": 503, "message": "flex busy"}})
        # Standard-tier fallback keeps extraction instruction, schema, and store=False.
        assert body["store"] is False
        assert EXTRACTION_INSTRUCTION in body["system_instruction"]
        assert body["response_format"]["schema"] == EXTRACTION_RESPONSE_FORMAT
        return archive_response()

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    sessions, msg = SessionManager(settings, store), group(message)
    await seed(sessions, msg)
    slice_id = await sessions.dual.snapshot_cold(msg, force=True)
    worker = MemoryCompressor(settings, sessions, gateway, store, wall_clock=wall_clock)

    for delay in (60, 120, 900, 3600):
        res = await worker.extract(msg, background=True)
        assert res["status"] == "provider_backoff"
        now += delay

    # 5th attempt hits Flex 503 -> falls back once to standard tier -> commits done.
    final = await worker.extract(msg, background=True)
    assert final["status"] == "compressed"
    assert [b.get("service_tier") for b in interaction_bodies] == ["flex"] * 5 + [None]
    # Only 1 source screen (attempt 1) + 1 output screen (after standard fallback succeeded).
    assert len(decision_calls) == 2
    assert (await sessions.dual.view(msg))["jobs"] == {"done": 1}
    async with sessions.database(msg, preserve_history=True) as (db, _):
        async with db.execute(
            "SELECT status,error,flex_failures,retry_at,screened FROM memory_slice_jobs WHERE id=?",
            (slice_id,),
        ) as cur:
            assert await cur.fetchone() == ("done", None, 0, 0, 1)
    assert (
        await store.one(
            "SELECT COUNT(DISTINCT work_id) FROM ledger_work WHERE kind='memory.extract'"
        )
    )[0] == 1
    events = await store.all("SELECT code FROM audit_events WHERE source='memory' ORDER BY id")
    assert [row["code"] for row in events] == ["memory_flex_retry_scheduled"] * 4 + [
        "memory_flex_retry_succeeded"
    ]
    usage = await store.usage()
    assert any(
        m["model_lane"] == "hydrogen" and m["calls"] == 1
        for m in usage["token_usage"]["today"]["by_model"]
    )


@pytest.mark.parametrize("fallback_succeeds", [True, False])
async def test_flex_exhaustion_with_explicit_allow_flex_stub(
    settings, store, message, fallback_succeeds
):
    now = 1_800_000_000
    screen_calls = 0
    gen_calls = []

    class ExplicitAllowFlexStub:
        async def screen(self, payload, subject, *, purpose, budget_lane, audit_source):
            nonlocal screen_calls
            screen_calls += 1
            return "safe"

        async def generate(
            self,
            payload,
            subject,
            *,
            scenario=None,
            budget_lane="chat",
            audit_source="production",
            memory_archive=False,
            allow_flex=True,
            before_call=None,
        ):
            if before_call is not None:
                await before_call()
            gen_calls.append((memory_archive, allow_flex))
            if allow_flex:
                raise Rejected("gemini_unavailable", detail="flex_capacity_busy")
            if fallback_succeeds:
                return '{"profiles":[],"chronicles":[]}'
            raise Rejected("gemini_unavailable", detail="standard_outage")

    sessions, msg = SessionManager(settings, store), group(message)
    await seed(sessions, msg)
    await sessions.dual.snapshot_cold(msg, force=True)
    worker = MemoryCompressor(
        settings, sessions, ExplicitAllowFlexStub(), store, wall_clock=lambda: now
    )
    for delay in (60, 120, 900, 3600):
        assert (await worker.extract(msg, background=True))["status"] == "provider_backoff"
        now += delay

    final = await worker.extract(msg, background=True)
    assert screen_calls == 1
    assert gen_calls == [(True, True)] * 5 + [(True, False)]
    if fallback_succeeds:
        assert final["status"] == "compressed"
        assert (await sessions.dual.view(msg))["jobs"] == {"done": 1}
    else:
        assert final == {"status": "failed", "reason": "flex_capacity_exhausted"}
        assert (await sessions.dual.view(msg))["jobs"] == {"failed": 1}


async def test_flex_exhaustion_with_legacy_stub_without_allow_flex_no_typeerror(
    settings, store, message
):
    now = 1_800_000_000
    screen_calls = 0
    gen_calls = 0

    class LegacyStubWithoutAllowFlex:
        async def screen(self, payload, subject, *, purpose, budget_lane, audit_source):
            nonlocal screen_calls
            screen_calls += 1
            return "safe"

        async def generate(
            self,
            payload,
            subject,
            *,
            scenario=None,
            budget_lane="chat",
            audit_source="production",
            memory_archive=False,
            before_call=None,
        ):
            nonlocal gen_calls
            if before_call is not None:
                await before_call()
            gen_calls += 1
            raise Rejected("gemini_unavailable", detail="flex_capacity_busy")

    sessions, msg = SessionManager(settings, store), group(message)
    await seed(sessions, msg)
    await sessions.dual.snapshot_cold(msg, force=True)
    worker = MemoryCompressor(
        settings, sessions, LegacyStubWithoutAllowFlex(), store, wall_clock=lambda: now
    )
    for delay in (60, 120, 900, 3600):
        assert (await worker.extract(msg, background=True))["status"] == "provider_backoff"
        now += delay

    final = await worker.extract(msg, background=True)
    assert final == {"status": "failed", "reason": "flex_capacity_exhausted"}
    assert screen_calls == 1
    assert gen_calls == 5
    assert (await sessions.dual.view(msg))["jobs"] == {"failed": 1}


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
        db.execute("ALTER TABLE memory_slice_jobs DROP COLUMN screened")
        rows = db.execute("SELECT id,payload FROM cold_history_slices ORDER BY id").fetchall()

    upgraded = SessionManager(settings, store)
    for _ in range(2):
        view = await upgraded.dual.view(msg)
        assert view["jobs"] == {"attempted": 1, "failed": 1} and view["flex_retry"] is None
        assert await upgraded.dual.claim(msg) is None
    with sqlite3.connect(sessions.path(msg)) as db:
        cols = {row[1] for row in db.execute("PRAGMA table_info(memory_slice_jobs)").fetchall()}
        assert {"flex_failures", "retry_at", "screened"} <= cols
        assert all(
            row[0] == 0 for row in db.execute("SELECT screened FROM memory_slice_jobs").fetchall()
        )
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


async def test_stale_attempted_recovery_on_writable_paths_and_readonly_never_writes(
    settings, store, message, monkeypatch
):
    sessions, msg = SessionManager(settings, store), group(message)
    ids = []
    for label in ("old_ts", "null_ts", "empty_ts", "fresh_ts"):
        await seed(sessions, msg, text=f"批次-{label}")
        ident = await sessions.dual.snapshot_cold(msg, force=True)
        await sessions.dual.claim(msg)
        ids.append((label, ident))

    # Craft 3 stale attempted rows (< _PROCESS_STARTED_AT, NULL, '') and 1 current-process row.
    with sqlite3.connect(sessions.path(msg)) as db:
        db.execute(
            "UPDATE memory_slice_jobs SET attempted_at=? WHERE id=?",
            (time.time() - 86400, ids[0][1]),
        )
        db.execute("UPDATE memory_slice_jobs SET attempted_at=NULL WHERE id=?", (ids[1][1],))
        db.execute("UPDATE memory_slice_jobs SET attempted_at='' WHERE id=?", (ids[2][1],))
        db.execute(
            "UPDATE memory_slice_jobs SET attempted_at=? WHERE id=?",
            (time.time() + 60, ids[3][1]),
        )

    # Spy on SQL statements to verify readonly=True callers perform zero DB writes.
    executed_sql: list[str] = []
    orig_connect = aiosqlite.connect

    class _SpyConn:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, sql, *args, **kwargs):
            executed_sql.append(str(sql).strip())
            return self._conn.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    class _SpyCM:
        def __init__(self, cm):
            self._cm = cm

        async def __aenter__(self):
            return _SpyConn(await self._cm.__aenter__())

        async def __aexit__(self, *exc):
            return await self._cm.__aexit__(*exc)

    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        lambda *a, **kw: _SpyCM(orig_connect(*a, **kw)),
    )

    view = await sessions.dual.view(msg)
    stats = await sessions.dual.stats(msg)
    peeked = await sessions.dual.peek(msg)
    assert view["jobs"] == {"unconfirmed": 3, "attempted": 1}
    assert stats["archive_unconfirmed_slices"] == 3
    assert peeked is None
    assert not any(
        sql.upper().startswith(("BEGIN IMMEDIATE", "UPDATE", "INSERT", "DELETE"))
        for sql in executed_sql
    )
    # Raw SQLite rows must still be untouched because readonly=True never writes.
    with sqlite3.connect(sessions.path(msg)) as db:
        raw_statuses = dict(db.execute("SELECT id,status FROM memory_slice_jobs").fetchall())
    assert all(raw_statuses[ident] == "attempted" for _, ident in ids)

    # Writable claim() transitions stale attempted rows to unconfirmed (archive_interrupted).
    monkeypatch.setattr("hakimi.session_manager.aiosqlite.connect", orig_connect)
    assert await sessions.dual.claim(msg) is None
    with sqlite3.connect(sessions.path(msg)) as db:
        rows = {
            row[0]: row[1:]
            for row in db.execute(
                "SELECT id,status,error,retry_at FROM memory_slice_jobs"
            ).fetchall()
        }
    for label, ident in ids[:3]:
        assert rows[ident] == ("unconfirmed", "archive_interrupted", 0), label
    assert rows[ids[3][1]][0] == "attempted"

    # Fresh SessionManager after restart recovers stale attempted rows inside migrate().
    with sqlite3.connect(sessions.path(msg)) as db:
        db.execute(
            "UPDATE memory_slice_jobs SET status='attempted',error=NULL,attempted_at=? WHERE id=?",
            (time.time() - 3600, ids[0][1]),
        )
        db.execute(
            "UPDATE memory_slice_jobs SET status='attempted',error=NULL,attempted_at=NULL "
            "WHERE id=?",
            (ids[1][1],),
        )
        db.execute(
            "UPDATE memory_slice_jobs SET status='attempted',error=NULL,attempted_at='' WHERE id=?",
            (ids[2][1],),
        )
    restarted = SessionManager(settings, store)
    async with restarted.database(msg, preserve_history=True) as (db, _):
        async with db.execute("SELECT id,status,error,retry_at FROM memory_slice_jobs") as cur:
            migrated_rows = {row[0]: row[1:] for row in await cur.fetchall()}
    for label, ident in ids[:3]:
        assert migrated_rows[ident] == ("unconfirmed", "archive_interrupted", 0), label
    assert migrated_rows[ids[3][1]][0] == "attempted"


async def test_requeue_failed_and_admin_compress_retry_failed_preserve_unconfirmed(
    settings, message
):
    token = "admin-retry-failed-test-token-123456"
    settings.admin_token = SecretStr(token)
    settings.owner_qq_id = "20000"
    gateway = FakeGateway()
    screen_sources = 0

    async def fake_screen(payload, subject, **kwargs):
        nonlocal screen_sources
        if kwargs.get("purpose") == "source":
            screen_sources += 1
        return "safe"

    async def fake_generate(payload, subject, **kwargs):
        gateway.calls.append(("generate", payload, subject, kwargs))
        return '{"profiles":[],"chronicles":[]}'

    gateway.screen = fake_screen
    gateway.generate = fake_generate
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        msg = group(message)

        # Create 3 slices: flex_capacity_exhausted (screened=1), content_blocked, and unconfirmed.
        await seed(svc.sessions, msg, text="耗尽批次")
        s_flex = await svc.sessions.dual.snapshot_cold(msg, force=True)
        j_flex = await svc.sessions.dual.claim(msg)
        await svc.sessions.dual.mark_screened(msg, j_flex)
        await svc.sessions.dual.finish(msg, j_flex, "failed", error="flex_capacity_exhausted")
        async with svc.sessions.database(msg, preserve_history=True) as (db, _):
            await db.execute(
                "UPDATE memory_slice_jobs SET flex_failures=5,retry_at=12345 WHERE id=?",
                (s_flex,),
            )

        await seed(svc.sessions, msg, text="敏感批次")
        s_blocked = await svc.sessions.dual.snapshot_cold(msg, force=True)
        j_blocked = await svc.sessions.dual.claim(msg)
        await svc.sessions.dual.finish(msg, j_blocked, "failed", error="content_blocked")

        await seed(svc.sessions, msg, text="中断批次")
        s_unconfirmed = await svc.sessions.dual.snapshot_cold(msg, force=True)
        j_unconfirmed = await svc.sessions.dual.claim(msg)
        await svc.sessions.dual.finish(
            msg, j_unconfirmed, "unconfirmed", error="archive_interrupted"
        )

        # Direct requeue_failed never touches unconfirmed even when archive_interrupted is passed.
        assert await svc.sessions.dual.requeue_failed(msg, errors=("archive_interrupted",)) == 0
        async with svc.sessions.database(msg, preserve_history=True) as (db, _):
            async with db.execute(
                "SELECT status,error FROM memory_slice_jobs WHERE id=?", (s_unconfirmed,)
            ) as cur:
                assert await cur.fetchone() == ("unconfirmed", "archive_interrupted")

        session_key = svc.sessions.key(msg)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + token},
        ) as client:
            # Invalid payloads rejected before burning costly actions.
            assert (
                await client.post(
                    f"/admin/api/sessions/{session_key}/compress",
                    json={"retry_failed": "true"},
                )
            ).status_code == 400
            assert (
                await client.post(
                    f"/admin/api/sessions/{session_key}/compress",
                    json={"retry_failed": True, "unknown": 1},
                )
            ).status_code == 400
            assert (
                await client.post(
                    f"/admin/api/sessions/{session_key}/compress",
                    content="raw",
                    headers={"Content-Type": "text/plain"},
                )
            ).status_code == 415

            # Default POST (empty body, no Content-Type) does NOT requeue failed slices.
            r_default = await client.post(f"/admin/api/sessions/{session_key}/compress")
            assert r_default.status_code == 200
            assert r_default.json() == {"status": "not_due_or_cooling_down"}
            assert gateway.calls == []

            # Explicit retry_failed=False does NOT requeue failed slices.
            r_false = await client.post(
                f"/admin/api/sessions/{session_key}/compress",
                json={"retry_failed": False},
            )
            assert r_false.status_code == 200
            assert r_false.json() == {"status": "not_due_or_cooling_down"}
            assert gateway.calls == []

            # Explicit retry_failed=True requeues ONLY flex_capacity_exhausted and compresses it.
            r_retry = await client.post(
                f"/admin/api/sessions/{session_key}/compress",
                json={"retry_failed": True},
            )
            assert r_retry.status_code == 200
            body = r_retry.json()
            assert body["status"] == "compressed"
            assert body["requeued_failed"] == 1
            # Previously screened slice skips duplicate ai.screen(..., purpose="source").
            assert screen_sources == 0
            assert len(gateway.calls) == 1

        async with svc.sessions.database(msg, preserve_history=True) as (db, _):
            async with db.execute(
                "SELECT id,status,error,flex_failures,retry_at,screened FROM memory_slice_jobs"
            ) as cur:
                final_rows = {row[0]: row[1:] for row in await cur.fetchall()}
        assert final_rows[s_flex] == ("done", None, 0, 0, 1)
        assert final_rows[s_blocked][0:2] == ("failed", "content_blocked")
        assert final_rows[s_unconfirmed][0:2] == ("unconfirmed", "archive_interrupted")

        # Even requeue_failed(errors=None) resets remaining failed rows and preserves unconfirmed.
        assert await svc.sessions.dual.requeue_failed(msg, errors=None) == 1
        async with svc.sessions.database(msg, preserve_history=True) as (db, _):
            async with db.execute(
                "SELECT id,status,error,flex_failures,retry_at FROM memory_slice_jobs"
            ) as cur:
                after_all = {row[0]: row[1:] for row in await cur.fetchall()}
        assert after_all[s_blocked] == ("pending", None, 0, 0)
        assert after_all[s_unconfirmed] == ("unconfirmed", "archive_interrupted", 0, 0)
