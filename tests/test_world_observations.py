"""The operator sees observed windows and receipts, with no duplicate or assumed successes."""

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from hakimi.modules.actions import DailyAction, DailyReceipt
from hakimi.planning_journal import PlannerJournal
from hakimi.work_usage import model_work
from hakimi.world_observations import overview
from hakimi.world_state import IntentTicketCreate, WorldStateManager
from tests.test_module_scheduling import ReadingModule
from tests.test_v20_life_admin import life_api as life_api

ZONE = "Asia/Shanghai"
FIRST = datetime(2026, 11, 2, 22, 15, tzinfo=ZoneInfo(ZONE))


async def test_evening_baseline_dedupes_restarts_and_requires_actual_confirmation(store):
    world = WorldStateManager(store.s, store)
    await world.initialize()
    journal = PlannerJournal(store)
    slot = DailyAction("test.delivery", None, label="每日投递")
    for i in range(11):
        dt = FIRST + timedelta(days=i)
        day, due = dt.date().isoformat(), dt.timestamp()
        if i == 5:  # No scheduler observation even for a real historical receipt.
            await journal.record_receipts(slot.name, [DailyReceipt(day, "confirmed", due, due + 8)])
            continue
        assert await journal.observe_tick(day, due)
        assert not await journal.observe_tick(day, due + 1)
        assert await journal.observe_slot(day, slot, due, due + 900)
        assert not await journal.observe_slot(day, slot, due + 3, due + 900)
        if i == 6:
            rec = DailyReceipt(day, "confirmed", due - 3600, due - 3500)  # manual, early
        elif i == 7:
            rec = DailyReceipt(day, "confirmed", due + 91, due + 100)
        elif i == 8:
            rec = DailyReceipt(day, "uncertain", due + 50)
        elif i == 9:
            rec = DailyReceipt(day, "confirmed", due + 90)  # old receipt with no QQ timestamp
        else:
            rec = DailyReceipt(day, "confirmed", due + 60, due + 80)
        await journal.record_receipts(slot.name, [rec])
        if i == 6:  # Reopen the journal, duplicate read, stale snapshot and cleared source history.
            journal = PlannerJournal(store)
            assert not await journal.observe_tick(day, due + 10)
            await journal.record_receipts(slot.name, [DailyReceipt(day, "queued")])
            await journal.record_receipts(slot.name, [])
    now = (FIRST + timedelta(days=10, seconds=120)).timestamp()
    data = await overview(store, now, ZONE)
    assert len(data["baseline_days"]) == len(data["delivery_windows"]) == 7
    assert len(data["open_windows"]) == 1
    assert (
        data["open_windows"][0]["delivery_confirmed"] and not data["open_windows"][0]["successful"]
    )
    expected = data["delivery_progress"][slot.name]
    assert (
        expected["closed"],
        expected["timely"],
        expected["confirmed"],
        expected["successful"],
    ) == (
        7,
        6,
        5,
        4,
    )
    early = next(w for w in data["delivery_windows"] if w["day"] == "2026-11-08")
    assert early["start_delay_seconds"] == 0 and early["successful"]
    assert "2026-11-07" in data["unobserved_recent_days"]
    assert all(w["day"] != "2026-11-07" for w in data["delivery_windows"])


async def test_admin_projects_model_costs_skips_and_next_action(life_api):
    app, client, _ = life_api
    svc = app.state.service
    journal = svc.life_engine.planner.journal
    now = FIRST.timestamp()
    svc.life_engine.clock = lambda: now
    day = FIRST.date().isoformat()
    await journal.observe_tick(day, now)
    run = await journal.start(
        day,
        now,
        trigger="manual",
        next_at=now + 7200,
        signature="s",
        explore_at=now + 86400,
        events=[],
    )
    with model_work("world.plan", run):
        settled = await svc.store.reserve("test", "proactive", Decimal("0.005"))
        await svc.store.settle(settled, Decimal("0.001"))
        unknown = await svc.store.reserve("test", "proactive", Decimal("0.003"))
        await svc.store.settle(unknown, None)
        await svc.store.reserve("test", "proactive", Decimal("0.002"))
    with model_work("unrelated.action", run):
        outside = await svc.store.reserve("test", "chat", Decimal("0.006"))
        await svc.store.settle(outside, Decimal("0.006"))
    await journal.commit(
        svc.world_state,
        run,
        tickets=[],
        candidates=3,
        source="llm",
        outcome="all_filtered",
        reasons={"unknown_action": 3},
        avoided=False,
    )
    skipped = await journal.start(
        day,
        now + 1,
        trigger="unchanged",
        next_at=now + 10800,
        signature="s",
        explore_at=now + 86400,
        events=[],
    )
    await journal.commit(
        svc.world_state,
        skipped,
        tickets=[],
        candidates=0,
        source="local_idle",
        outcome="unchanged_state",
        reasons={"unchanged_state": 1},
        avoided=True,
    )
    ticket = IntentTicketCreate(
        action_name="my.work",
        action_type="work",
        target_module="sample",
        execute_after=now + 500,
        expire_at=now + 900,
        intent_note="DO_NOT_EXPOSE",
    )
    await svc.world_state.enqueue_intent(ticket, now=now)
    response = await client.get("/admin/api/life/overview")
    assert response.status_code == 200
    data = response.json()["observation"]
    assert data["today"]["avoided"] == 1 and data["today"]["candidates"] == 3
    cost = data["today"]["cost"]
    assert cost["calls"] == 3 and cost["estimated_microdollars"] == 1000
    assert cost["unknown_calls"] == cost["open_reservations"] == 1
    assert cost["reserved_microdollars"] == 2000 and cost["unknown_budget_microdollars"] == 3000
    assert data["recent_plans"][0]["cost"]["estimated_microdollars"] == 0
    assert data["recent_plans"][1]["reasons"] == {"unknown_action": 3}
    assert data["next_action"]["action"] == "my.work"
    assert data["today_actions"] == [{"action": "my.work", "status": "pending", "count": 1}]
    assert "DO_NOT_EXPOSE" not in json.dumps(data)


async def test_registered_slot_owns_receipt_source_and_polling(life_api):
    app, _, ai = life_api
    svc = app.state.service
    engine = svc.life_engine
    now = FIRST.replace(hour=14, minute=0).timestamp()
    day = FIRST.date().isoformat()
    reads = []
    rows = []

    async def receipts():
        reads.append(True)
        return rows

    class ObservedReading(ReadingModule):
        def schedules(self):
            return tuple(replace(s, receipts=receipts) for s in super().schedules())

    svc.modules.register(ObservedReading(svc.s))
    await engine.initialize()
    for stamp in (now - 60, now, now + 60):
        await engine.schedule_due(stamp)
    assert len(reads) == 2  # first periodic poll, then immediately on opening the slot
    rows.append(DailyReceipt(day, "confirmed", now + 10, now + 21))
    await engine.schedule_due(now + 900)
    await engine.schedule_due(now + 905)
    assert len(reads) == 3
    result = await overview(svc.store, now + 1900, ZONE)
    assert result["delivery_progress"]["reading.open"]["successful"] == 1
    assert not ai.calls


async def test_qq_receipt_adapter_projects_safe_timestamps_only():
    from hakimi.modules.qq_social import QQSocialModule

    async def history():
        return [
            {
                "day": "2026-11-01",
                "stage": "confirmed",
                "queued_at": 15,
                "confirmed_at": 30,
                "private_payload": "owner text",
            },
            {
                "day": "2026-11-02",
                "stage": "submitting",
                "generation_started_at": 10,
                "confirmed_at": 301,
            },
        ]

    module = QQSocialModule()
    module.proactive = SimpleNamespace(digest_history=history)
    assert await module.daily_receipts() == [
        DailyReceipt("2026-11-01", "confirmed", 15, 30),
        DailyReceipt("2026-11-02", "submitting", 10, None),
    ]


async def test_stalled_receipt_source_cannot_hold_the_schedule_clock(store, monkeypatch):
    from hakimi import planning_journal

    monkeypatch.setattr(planning_journal, "RECEIPT_READ_TIMEOUT", 0.01)
    journal = PlannerJournal(store)
    slot = DailyAction("test.delivery", None, label="每日投递")
    now = FIRST.timestamp()
    day = FIRST.date().isoformat()
    await journal.observe_tick(day, now)
    await journal.observe_slot(day, slot, now, now + 900)

    async def stalled():
        await asyncio.Event().wait()

    async def healthy():
        return [DailyReceipt(day, "confirmed", now, now + 3)]

    await asyncio.wait_for(
        asyncio.gather(
            journal.sample_receipts("test.unresponsive", stalled),
            journal.sample_receipts(slot.name, healthy),
        ),
        timeout=1,
    )
    row = await store.one("SELECT confirmed FROM life_observed_slots")
    assert row["confirmed"] == now + 3
