"""Wave 1+2 storage, SQLite indexing, idle write-lock elimination, and perf regression tests."""

from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import aiosqlite
import httpx
import pytest

from hakimi.adapters import Sender
from hakimi.dual_memory import Extraction
from hakimi.event_ledger import EventLedger
from hakimi.life_engine import LifeEngine
from hakimi.memory_compressor import MemoryCompressor
from hakimi.moderation import Moderation
from hakimi.modules.base import BaseModule
from hakimi.roaming_logger import RoamingLogger
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.session_manager import SessionManager
from hakimi.social import OwnerBinding
from hakimi.world_state import WorldStateManager
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_proactive_dm import Wire, stamp
from tests.test_session_perf import _SQLSpyConnect

TZ = ZoneInfo("Asia/Shanghai")


def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


def _spy_store_sql(store, monkeypatch) -> list[str]:
    sqls: list[str] = []
    orig_exec = store.db.execute

    def _exec(sql, *args, **kwargs):
        sqls.append(str(sql))
        return orig_exec(sql, *args, **kwargs)

    monkeypatch.setattr(store.db, "execute", _exec)
    return sqls


class _CustomPlatformModule(BaseModule):
    module_id = "customchat"
    display_name = "CustomChat"
    description = "Non-OneBot test chat platform"
    chat_platforms = ("customchat",)

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self.connected = True
        self.sent: list[str] = []
        self.send_handler = None

    def chat_connected(self, platform: str) -> bool:
        return self.connected and platform == "customchat"

    def authorize_chat(self, message, settings) -> bool:
        return message.platform == "customchat"

    async def _raw_sensor_summary(self) -> str:
        return "customchat:ok"

    async def send_chat(self, message, text: str, *, before_send=None):
        if self.send_handler is not None:
            return await self.send_handler(message, text, before_send=before_send)
        assert before_send is None or await before_send()
        self.sent.append(text)
        return f"cc-{len(self.sent)}"


# ---------------------------------------------------------------------------
# 1. Wave 1 Bug: Non-OneBot plugin_not_dispatched + desktop activity thread
# ---------------------------------------------------------------------------


async def test_non_onebot_plugin_not_dispatched_preserves_pending_and_digest(settings, store):
    s = settings.model_copy(
        update={
            "onebot_enabled": False,
            "qq_enabled": False,
            "owner_delivery_platform": "customchat",
            "proactive_dm_enabled": True,
            "social_owner_bindings": {
                "customchat": OwnerBinding(
                    bot_id="cc-bot",
                    user_id="cc-owner",
                    notebook=True,
                    mirror=True,
                )
            },
        }
    )
    now = stamp("2026-11-14T20:00:00")
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    mod = _CustomPlatformModule(s)
    svc.modules.register(mod)
    svc.proactive.clock = lambda: now
    svc.life_engine = LifeEngine(svc, clock=svc.proactive.clock)
    svc.world_state.clock = svc.proactive.clock
    await svc.life_engine.initialize()
    await svc.proactive.initialize()

    async def fail_not_dispatched(_msg, _text, *, before_send=None):
        assert before_send is not None and await before_send()
        raise Rejected("plugin_not_dispatched")

    mod.send_handler = fail_not_dispatched
    try:
        # 1a. Regular alert/test DM reverts to pending with payload intact.
        key = await svc.proactive.enqueue("test", "非OneBot待重试消息", expires=now + 3600)
        assert await svc.proactive.deliver_once() is False
        row = await store.one(
            "SELECT status, payload, session_epoch FROM pending_owner_dms WHERE id=?", (key,)
        )
        assert row["status"] == "pending"
        assert row["payload"] is not None
        assert row["session_epoch"] is None

        # Succeeds on retry once transport recovers.
        mod.send_handler = None
        assert await svc.proactive.deliver_once() is True
        assert await svc.proactive.delivery_status(key) == "sent"
        assert mod.sent == ["非OneBot待重试消息"]

        # 1b. Digest also reverts submitted_at=NULL on plugin_not_dispatched.
        mod.send_handler = fail_not_dispatched
        res = await svc.proactive.manual_digest()
        assert res["delivery"] == "pending"
        assert res["receipt"]["submitted_at"] is None
        row_d = await store.one(
            "SELECT status, payload, session_epoch FROM pending_owner_dms WHERE id=?",
            (res["id"],),
        )
        assert row_d["status"] == "pending" and row_d["payload"] is not None

        mod.send_handler = None
        assert await svc.proactive.deliver_once() is True
        assert await svc.proactive.delivery_status(res["id"]) == "sent"
    finally:
        await svc.stop()


async def test_proactive_companion_context_runs_desktop_activity_off_event_loop(settings, store):
    settings.owner_qq_id = "20000"
    settings.enable_antigravity_memory = True
    now = stamp("2026-11-14T15:00:00")
    loop_thread_id = threading.get_ident()
    seen_threads: list[int] = []

    class _FakeCloudMemory:
        def latest_activity_info(self):
            seen_threads.append(threading.get_ident())
            return {
                "enabled": True,
                "latest_timestamp": now - 600,
                "latest_iso": "2026-11-14T14:50:00",
                "latest_tag": "coding",
            }

    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    svc.cloud_memory = _FakeCloudMemory()
    svc.proactive.clock = lambda: now
    await svc.proactive.initialize()
    try:
        payload = await svc.proactive.companion_context(now)
        assert "desktop_recent_activity" in payload
        assert seen_threads and seen_threads[0] != loop_thread_id
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# 2. Wave 2 Missing hot-path SQLite indexes
# ---------------------------------------------------------------------------


async def test_hot_path_sqlite_indexes_present_and_idempotent(settings, store):
    await store.runtime.filters.initialize()
    world = WorldStateManager(settings, store)
    await world.initialize()
    ledger = EventLedger(settings, store)
    await ledger.initialize()
    diary = RoamingLogger(settings, store)
    await diary.initialize()

    # Re-initialize all schemas to confirm idempotency on existing DBs.
    await store.runtime.filters.initialize()
    await world.initialize()
    await ledger.initialize()
    await diary.initialize()

    expected: dict[str, set[tuple[str, ...]]] = {
        "ledger_times": {("created",)},
        "outbox": {("status", "created"), ("status", "expires"), ("created",)},
        "jobs": {("scope", "status"), ("created",)},
        "gateway_group_context": {("scope", "at"), ("at",)},
        "group_recent_messages": {("at",)},
        "event_ledger": {("status", "expires_at"), ("status", "updated_at")},
        "global_events": {("created_at",)},
        "scheduled_intents": {("created_at",)},
        "world_news": {("expires",)},
        "roaming_receipts": {("day",)},
    }
    for table, required_cols in expected.items():
        idx_rows = await store.all(f"PRAGMA index_list({table})")
        indexed_cols: set[tuple[str, ...]] = set()
        for idx in idx_rows:
            cols = await store.all(f"PRAGMA index_info({idx['name']})")
            indexed_cols.add(tuple(c["name"] for c in sorted(cols, key=lambda r: r["seqno"])))
        for req in required_cols:
            assert req in indexed_cols, f"Missing index on {table}{req}; found {indexed_cols}"


# ---------------------------------------------------------------------------
# 3A. Event-driven worker/delivery wakeup & zero-write idle claim
# ---------------------------------------------------------------------------


async def test_store_idle_claim_zero_writes_and_event_driven_wakeups(
    settings, store, message, monkeypatch
):
    sqls = _spy_store_sql(store, monkeypatch)

    # Idle claim() and claim_reply() execute ZERO BEGIN IMMEDIATE transactions.
    assert await store.claim() is None
    assert await store.claim_reply() is None
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)

    # admit() signals job_wakeup and wakes wait_for_job() immediately.
    store.job_wakeup.clear()
    waiter = asyncio.create_task(store.wait_for_job(max_wait=5.0))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    assert await store.admit(message) == "accepted"
    await asyncio.wait_for(waiter, timeout=1.0)
    assert store.job_wakeup.is_set()

    job = await store.claim()
    assert job is not None

    # enqueue_reply() signals reply_wakeup and wakes wait_for_reply() immediately.
    store.reply_wakeup.clear()
    reply_waiter = asyncio.create_task(store.wait_for_reply(max_wait=5.0))
    await asyncio.sleep(0.01)
    assert not reply_waiter.done()
    await store.enqueue_reply(job["id"], message, "hello")
    await asyncio.wait_for(reply_waiter, timeout=1.0)
    assert store.reply_wakeup.is_set()

    # finish_reply() signals job_wakeup so blocked same-scope jobs wake up.
    reply = await store.claim_reply()
    assert reply is not None
    store.job_wakeup.clear()
    assert await store.finish_reply(reply["id"], "sent") is True
    assert store.job_wakeup.is_set()

    # finish() signals job_wakeup.
    store.job_wakeup.clear()
    await store.finish(job["id"])
    assert store.job_wakeup.is_set()

    # Cancellation during wait_for_job / wait_for_reply propagates cleanly.
    store.job_wakeup.clear()
    t = asyncio.create_task(store.wait_for_job(max_wait=5.0))
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


async def test_debounced_job_zero_write_while_waiting_and_wakes_on_deadline(
    settings, store, message, monkeypatch
):
    settings.group_debounce_seconds = 0.08
    settings.group_debounce_max_seconds = 0.12
    grp = message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "event_id": "deb-1",
            "addressed": True,
            "attention_reason": "mention",
            "created_at": time.time(),
        }
    )
    assert await store.admit(grp) == "accepted"

    sqls = _spy_store_sql(store, monkeypatch)
    # While still inside debounce window: claim() returns None with 0 BEGIN IMMEDIATE.
    assert await store.claim() is None
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)

    # wait_for_job() respects the debounce ready_at deadline (wakes in ~0.08s, not 5.0s).
    store.job_wakeup.clear()
    t0 = time.monotonic()
    await asyncio.wait_for(store.wait_for_job(max_wait=5.0), timeout=1.0)
    elapsed = time.monotonic() - t0
    assert elapsed < 0.8
    claimed = await store.claim()
    assert claimed is not None


# ---------------------------------------------------------------------------
# 3B. OwnerProactive idle zero-write & policy_lock released during network I/O
# ---------------------------------------------------------------------------


async def test_proactive_idle_zero_writes_and_policy_lock_free_during_network(
    settings, store, monkeypatch
):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    now = [stamp("2026-11-14T12:00:00")]
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, wire))
        dm = svc.proactive
        dm.clock = lambda: now[0]
        svc.life_engine = LifeEngine(svc, clock=dm.clock)
        svc.world_state.clock = dm.clock
        await svc.life_engine.initialize()
        await dm.initialize()
        try:
            # 1. Idle _expire_and_disable() & deliver_once() open 0 write txs and
            # do not take owner_dialogue_lock.
            sqls = _spy_store_sql(store, monkeypatch)
            async with svc.owner_dialogue_lock:
                # Even while owner_dialogue_lock is held, idle deliver_once returns immediately.
                assert await asyncio.wait_for(dm.deliver_once(), timeout=0.5) is False
            await dm._expire_and_disable()
            assert not any("BEGIN IMMEDIATE" in s for s in sqls)

            # 2. During active network send, policy_lock is released so enqueue/alert don't block.
            in_send = asyncio.Event()
            release_send = asyncio.Event()

            async def slow_network():
                in_send.set()
                await release_send.wait()

            wire.hook = slow_network
            k1 = await dm.enqueue("test", "first-dm")
            deliver_task = asyncio.create_task(dm.deliver_once())
            await asyncio.wait_for(in_send.wait(), timeout=2.0)

            assert not dm.policy_lock.locked()
            k2 = await asyncio.wait_for(dm.enqueue("test", "second-dm-while-sending"), timeout=0.5)
            assert k2 != k1

            release_send.set()
            assert await asyncio.wait_for(deliver_task, timeout=2.0) is True
            assert await dm.delivery_status(k1) == "sent"
            assert await dm.delivery_status(k2) == "pending"
        finally:
            await svc.stop()


# ---------------------------------------------------------------------------
# 3C. DiaryDriveSync & PatrolDiary zero idle writes / decryptions
# ---------------------------------------------------------------------------


async def test_diary_idle_closed_days_skip_decryption_and_writes(settings, store, monkeypatch):
    class _FakeDrive:
        def __init__(self):
            self.calls = []

        async def sync_day(self, day, text):
            self.calls.append((day, text))
            return f"file-{day}"

    s = settings.model_copy(update={"roaming_enabled": True})
    drive = _FakeDrive()
    diary = RoamingLogger(s, store, drive)
    await diary.initialize()

    day1 = _ts("2026-11-14T20:00:00")
    day2 = _ts("2026-11-15T10:00:00")
    await diary.patrol("p1", "昨天巡逻内容", now=day1)
    await diary.sync_due(now=day1)
    assert len(drive.calls) == 1

    # Day 2 first sync closes day 1 and syncs day 2.
    await diary.sync_due(now=day2)
    uploads_after_close = len(drive.calls)

    # Subsequent idle ticks on day 2 must perform ZERO BEGIN IMMEDIATE writes and ZERO renders!
    render_calls = 0
    orig_render = diary.render

    async def spy_render(day: str) -> str:
        nonlocal render_calls
        render_calls += 1
        return await orig_render(day)

    monkeypatch.setattr(diary, "render", spy_render)
    sqls = _spy_store_sql(store, monkeypatch)

    await diary.sync_due(now=day2 + 30)
    await diary.sync_due(now=day2 + 60)
    assert render_calls == 0
    assert not any("BEGIN IMMEDIATE" in q for q in sqls)
    assert len(drive.calls) == uploads_after_close

    # Force sync on unchanged day also skips redundant UPDATE roaming_sync.
    sqls.clear()
    await diary.sync_due(now=day2 + 90, force=True)
    assert not any("UPDATE roaming_sync" in q for q in sqls)

    # Late event on closed day 1 marks day 1 dirty -> triggers re-render & upload!
    await diary.injection("late-on-day1", now=day1 + 60)
    render_calls = 0
    await diary.sync_due(now=day2 + 120)
    assert render_calls == 1
    assert len(drive.calls) == uploads_after_close + 1


# ---------------------------------------------------------------------------
# 3D. SessionManager readonly=True, prune(), janitor interval, MemoryCompressor
# ---------------------------------------------------------------------------


async def test_session_readonly_callers_zero_write_locks_and_missing_files_safe(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    fresh_msg = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})

    # Missing session DB files must not crash read-only callers
    # (and peek/prefetch/view/stats do not create files).
    assert await sessions.dual.peek(fresh_msg) is None
    assert await sessions.dual.prefetch(fresh_msg, "hi", recall=True) == {
        "speaker_profile": {},
        "chronicle_recall": [],
    }
    assert (await sessions.dual.stats(fresh_msg))["dual_entries_count"] == 0
    assert (await sessions.dual.view(fresh_msg))["entries"] == []
    assert not sessions.path(fresh_msg).exists()
    epoch, window = await sessions.private_request_window(fresh_msg)
    assert epoch == 0 and window == []

    # Populate initialized session and spy on SQLite statements.
    await sessions.remember(message, "我喜欢咖啡", "收到啦")
    sql_log: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        _SQLSpyConnect(aiosqlite.connect, sql_log, []),
    )

    assert len((await sessions.snapshot(message))["recent_messages"]) == 2
    assert len((await sessions.private_request_window(message))[1]) == 2
    assert await sessions.dual.peek(message) is None
    assert (await sessions.dual.view(message))["epoch"] == 0
    assert (await sessions.dual.stats(message))["dual_entries_count"] == 0
    assert await sessions.dual.prefetch(message, "咖啡", recall=True) == {
        "speaker_profile": {},
        "chronicle_recall": [],
    }
    assert not any("BEGIN IMMEDIATE" in sql for _, sql in sql_log)
    assert any(sql == "BEGIN" for _, sql in sql_log)

    # prune() preserves _expiry_cache and does NOT open unmodified non-expiring sessions.
    sql_log.clear()
    assert sessions.key(message) in sessions._expiry_cache
    await sessions.prune()
    assert sessions.key(message) in sessions._expiry_cache
    assert sql_log == []


async def test_memory_compressor_single_budget_check_and_dirty_tracking(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    msgs = [
        message.model_copy(update={"kind": "group", "conversation_id": str(40000 + i)})
        for i in range(4)
    ]
    for m in msgs:
        await sessions.remember(m, "hi", "hello")

    budget_calls = 0
    orig_budget = store.budget_status

    async def spy_budget():
        nonlocal budget_calls
        budget_calls += 1
        return await orig_budget()

    monkeypatch.setattr(store, "budget_status", spy_budget)

    gw = FakeGateway()
    gw.reply = '{"profiles":[],"chronicles":[]}'
    comp = MemoryCompressor(settings, sessions, gw, store)

    # Pass 1 (startup seed): 4 sessions, 0 cold slices -> budget_status() called ONCE total!
    await comp.run_pass()
    assert budget_calls == 1
    assert not sessions.dual.dirty_sessions

    # Pass 2 (idle): 0 budget_status calls and 0 session DB opens!
    sql_log: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "hakimi.session_manager.aiosqlite.connect",
        _SQLSpyConnect(aiosqlite.connect, sql_log, []),
    )
    budget_calls = 0
    await comp.run_pass()
    assert budget_calls == 0
    assert sql_log == []

    # Appending a cold slice marks ONLY that session dirty; next pass extracts it.
    await sessions.dual.snapshot_cold(msgs[0], force=True)
    assert sessions.key(msgs[0]) in sessions.dual.dirty_sessions
    await comp.run_pass()
    assert budget_calls >= 1
    assert any(c[0] == "generate" for c in gw.calls)


# ---------------------------------------------------------------------------
# 3E. EventLedger read-only, Moderation.status throttle, Store.prune ledger skip
# ---------------------------------------------------------------------------


async def test_event_ledger_read_and_planner_state_zero_writes(settings, store, monkeypatch):
    now = [1_800_000_000.0]
    ledger = EventLedger(settings, store, clock=lambda: now[0])
    await ledger.initialize()
    ev = await ledger.post(
        "farm",
        "crop_ready",
        "草莓成熟",
        dedupe_key="plot:1",
        due_at=now[0] + 60,
        ttl_seconds=300.0,
        now=now[0],
    )

    sqls = _spy_store_sql(store, monkeypatch)
    # Active read + planner_state: ZERO write transactions.
    assert len(await ledger.read(now=now[0] + 100)) == 1
    items, _, _ = await ledger.planner_state(now[0] + 100)
    assert len(items) == 1
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)

    # Past expiry: read() hides expired item, admin_list reports expired, still ZERO write txs.
    past = now[0] + 500
    assert await ledger.read(now=past) == []
    assert (await ledger.planner_state(past))[0] == []
    admin_rows = await ledger.admin_list(now=past)
    assert admin_rows[0]["status"] == "expired"
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)

    # consume() also rejects expired row even before expire_due() runs.
    assert await ledger.consume(ev.id, consumer="farm", now=past) is False

    # Explicit expire_due() transitions DB row once, and second call is zero-write.
    assert await ledger.expire_due(past) == 1
    sqls.clear()
    assert await ledger.expire_due(past) == 0
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)


async def test_moderation_status_throttles_prune(settings, store, message, monkeypatch):
    settings.owner_qq_id = "20000"
    mono = [1000.0]
    wall = [1_800_000_000.0]
    monkeypatch.setattr("hakimi.moderation.time.monotonic", lambda: mono[0])

    svc = SimpleNamespace(
        s=settings,
        store=store,
        sender=SimpleNamespace(onebot=SimpleNamespace(call=AsyncMock(return_value=[]))),
        proactive=SimpleNamespace(alert=AsyncMock()),
    )
    mod = Moderation(svc, clock=lambda: wall[0])
    await mod.initialize()
    offender = message.model_copy(
        update={"user_id": "30000", "kind": "group", "conversation_id": "40000"}
    )
    for i in range(3):
        await mod.record_strike(
            offender.model_copy(update={"event_id": f"e-{i}"}), "prompt_injection"
        )

    # Advance past strike/proposal expiry so prune() would actually have work to write.
    wall[0] += 200_000.0
    await mod.status()  # First call runs prune and records mono=1000.0

    # Add another strike & expire it, then call status() within 300s -> throttled.
    await mod.record_strike(offender.model_copy(update={"event_id": "e-new"}), "prompt_injection")
    wall[0] += 90_000.0
    mono[0] += 60.0
    sqls = _spy_store_sql(store, monkeypatch)
    await mod.status()
    assert not any("BEGIN IMMEDIATE" in s for s in sqls)

    # After >= 300s (or force_prune=True), prune() runs again.
    mono[0] += 300.0
    await mod.status()
    assert any("BEGIN IMMEDIATE" in s for s in sqls)


async def test_store_prune_skips_ledger_subtable_scans_when_no_ledger_deleted(
    settings, store, monkeypatch
):
    # Insert a recent ledger row (not older than 32 days).
    res = await store.reserve("u1", "chat", Decimal("0.01"))
    await store.settle(res, Decimal("0.005"))

    sqls = _spy_store_sql(store, monkeypatch)
    await store.prune()
    assert not any("DELETE FROM ledger_attribution" in s for s in sqls)
    assert not any("DELETE FROM ledger_work" in s for s in sqls)
    assert not any("DELETE FROM ledger_usage" in s for s in sqls)
    assert not any("DELETE FROM ledger_thinking" in s for s in sqls)
    assert not any("DELETE FROM ledger_times" in s for s in sqls)
    assert not any("DELETE FROM ledger_provider" in s for s in sqls)

    # Age the ledger row past 32 days -> prune() deletes ledger AND cascades all 6 sub-tables.
    async with store.transaction():
        await store.db.execute("UPDATE ledger SET day='2000-01-01'")
    sqls.clear()
    await store.prune()
    for sub in (
        "ledger_attribution",
        "ledger_work",
        "ledger_usage",
        "ledger_thinking",
        "ledger_times",
        "ledger_provider",
    ):
        assert any(f"DELETE FROM {sub}" in s for s in sqls)
        assert not await store.all(f"SELECT 1 FROM {sub}")


# ---------------------------------------------------------------------------
# 4. DualMemory.commit() O(N*M) Fernet decryption fix
# ---------------------------------------------------------------------------


async def test_dual_memory_commit_skips_decrypting_unlocked_chronicles(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    msg = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "user_id": "30000"}
    )

    # Seed 30 unlocked chronicle entries.
    for i in range(30):
        assert await sessions.dual.record_chronicle(
            msg,
            topic=f"历史话题{i}",
            text=f"这是第 {i} 条未锁定的普通群聊编年史记录",
            category="event",
            tags=["测试"],
        )

    # Seed one locked+deleted tombstone to verify blocking still works.
    assert await sessions.dual.record_chronicle(
        msg,
        topic="禁提旧梗",
        text="大家约定再也不要提那个翻车名场面",
        category="slang",
        tags=["禁梗"],
    )
    view = await sessions.dual.view(msg)
    tomb = next(e for e in view["entries"] if e.get("topic") == "禁提旧梗")
    await sessions.dual.edit(msg, tomb["id"], version=view["version"], action="delete")

    # Seed a cold slice with 2 new chronicle facts (1 allowed, 1 paraphrasing the tombstone).
    async with sessions.database(msg) as (db, key):
        for idx, text in enumerate(("今晚去海边看流星雨", "再提那个翻车名场面"), start=1):
            await db.execute(
                "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                (
                    sessions.vault.seal(
                        key,
                        {
                            "role": "user",
                            "user_id": "30000",
                            "speaker": "群友",
                            "text": text,
                            "event_id": f"ev-{idx}",
                        },
                    ),
                    time.time() + 86400,
                ),
            )
    await sessions.dual.snapshot_cold(msg, force=True)
    snap = await sessions.dual.claim(msg)
    assert snap is not None
    seqs = [m["seq"] for m in snap["messages"]]

    opened_contexts: list[str] = []
    orig_open = sessions.vault.open

    def spy_open(context, token):
        opened_contexts.append(str(context))
        return orig_open(context, token)

    monkeypatch.setattr(sessions.vault, "open", spy_open)

    extraction = Extraction.model_validate(
        {
            "profiles": [],
            "chronicles": [
                {
                    "topic": "海边流星雨",
                    "category": "event",
                    "text": "群友相约今晚去海边看流星雨",
                    "tags": ["天文"],
                    "evidence_seqs": [seqs[0]],
                },
                {
                    "topic": "换个名字的旧梗",
                    "category": "slang",
                    "text": "大家约定再也不要提那个翻车名场面",
                    "tags": ["旧梗"],
                    "evidence_seqs": [seqs[1]],
                },
            ],
        }
    )
    assert await sessions.dual.commit(msg, snap, extraction) is True

    # Unlocked 30 chronicles must NEVER have been decrypted;
    # only the 1 locked+deleted tombstone (once each for :e: and :v:).
    chronicle_opens = [c for c in opened_contexts if ":e:" in c or ":v:" in c]
    assert len(chronicle_opens) == 2

    final_view = await sessions.dual.view(msg)
    active_topics = {e.get("topic") for e in final_view["entries"] if not e["deleted"]}
    assert "海边流星雨" in active_topics
    assert "换个名字的旧梗" not in active_topics


async def test_memory_hub_recent_session_readonly_and_janitor_prune_interval(
    settings, store, message, monkeypatch
):
    settings.owner_qq_id = "20000"
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        owner_msg = message.model_copy(
            update={"user_id": "20000", "conversation_id": "20000", "kind": "private"}
        )
        await svc.sessions.remember(owner_msg, "今天天气不错", "是呀喵")
        sql_log: list[tuple[str, str]] = []
        monkeypatch.setattr(
            "hakimi.session_manager.aiosqlite.connect",
            _SQLSpyConnect(aiosqlite.connect, sql_log, []),
        )
        res = await svc.memory_hub._read_owner_recent_session(owner_msg, limit=4)
        assert res["enabled"] is True and len(res["recent_messages"]) == 2
        assert not any("BEGIN IMMEDIATE" in sql for _, sql in sql_log)
        assert any(sql == "BEGIN" for _, sql in sql_log)

        # Verify janitor runs sessions.prune() once per 1800s, not every 60s tick.
        prune_calls = 0

        async def count_prune():
            nonlocal prune_calls
            prune_calls += 1

        now = [2_000_000.0]
        ticks = 0

        async def step_sleep(_sec: float):
            nonlocal ticks
            ticks += 1
            if ticks >= 3:
                raise asyncio.CancelledError
            now[0] += 60.0

        monkeypatch.setattr(svc.sessions, "prune", count_prune)
        monkeypatch.setattr("hakimi.service.time.time", lambda: now[0])
        monkeypatch.setattr("hakimi.service.asyncio.sleep", step_sleep)
        with pytest.raises(asyncio.CancelledError):
            await svc.janitor()
        assert prune_calls == 1
    finally:
        await svc.stop()
