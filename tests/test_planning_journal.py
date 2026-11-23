import json

import pytest

from hakimi.life_engine import LifeEngine
from tests.test_proactive_dm import dm as dm
from tests.test_v20_life_engine import LifeGateway


def setup_model(dm, *, intents):
    fake = LifeGateway()
    plan = json.loads(fake.plan_json)
    plan.update(intents=intents, mood_longnick="")
    fake.plan_json = json.dumps(plan)
    dm.service.ai = fake
    return fake


async def test_unchanged_world_saves_plans_but_milestone_wakes_and_exploration_survives(dm):
    engine = dm.service.life_engine
    fake = setup_model(dm, intents=[])
    planner = engine.planner
    result = await planner.plan_once(now=dm.clock())
    assert result["source"] == "llm"
    dm.time[0] = result["next_plan_at"]
    assert (await planner.plan_once(now=dm.clock(), automatic=True))["reason"] == "unchanged_state"
    assert len(fake.calls) == 1
    event = await dm.service.world_state.publish_event(
        "minecraft", "discovery", {}, importance="high", now=dm.clock()
    )
    dm.time[0] += 65
    await planner.plan_once(now=dm.clock(), automatic=True)
    assert len(fake.calls) == 2
    assert (await dm.store.one("SELECT consumed FROM global_events WHERE id=?", (event.id,)))[0]
    assert (await dm.store.one("SELECT SUM(avoided) FROM life_plan_runs"))[0] == 1
    async with dm.store.transaction():
        await dm.store.db.execute(
            "UPDATE life_planner_clock SET explore_at=0,next_at=?", (dm.clock(),)
        )
    await planner.plan_once(now=dm.clock(), automatic=True)
    assert len(fake.calls) == 3


async def test_checkpoint_replays_without_second_model_charge_or_duplicate_tasks(dm, monkeypatch):
    engine, ws = dm.service.life_engine, dm.service.world_state
    fake = setup_model(
        dm,
        intents=[
            {
                "target_module": "bilibili_surf",
                "target_scope": "onebot:40000",
                "action_type": "group_chat_share",
                "intent_note": "分享公开视频",
            }
        ],
    )
    ev = await ws.publish_event("minecraft", "ambient", {}, now=dm.clock())
    original = ws.insert_intent_tx

    async def cut_power(ticket, *, now):
        await original(ticket, now=now)
        raise RuntimeError("simulated SQLite transaction interruption")

    monkeypatch.setattr(ws, "insert_intent_tx", cut_power)
    with pytest.raises(RuntimeError, match="interruption"):
        await engine.planner.plan_once(now=dm.clock())
    assert not (await dm.store.one("SELECT consumed FROM global_events WHERE id=?", (ev.id,)))[0]
    assert not await dm.store.one("SELECT id FROM scheduled_intents WHERE origin='planner'")
    monkeypatch.setattr(ws, "insert_intent_tx", original)
    for i in range(2):
        reboot = LifeEngine(dm.service, clock=dm.clock)
        await reboot.planner.initialize()
        result = await reboot.planner.plan_once(now=dm.clock(), automatic=True)
        assert result["status"] == ("planned" if i == 0 else "not_due")
    assert len(fake.calls) == 1
    assert (await dm.store.one("SELECT COUNT(*) FROM scheduled_intents WHERE origin='planner'"))[
        0
    ] == 1
    assert (await dm.store.one("SELECT consumed FROM global_events WHERE id=?", (ev.id,)))[0] == 1


async def test_uncheckpointed_provider_crash_preserves_input_and_reserved_wake(dm, monkeypatch):
    planner, ws = dm.service.life_engine.planner, dm.service.world_state
    fake = setup_model(dm, intents=[])
    ev = await ws.publish_event("minecraft", "ambient", {}, now=dm.clock())

    async def crash(*args, **kwargs):
        raise RuntimeError("lost provider response")

    monkeypatch.setattr(planner.journal, "checkpoint", crash)
    with pytest.raises(RuntimeError):
        await planner.plan_once(now=dm.clock())
    for _ in range(2):
        reboot = LifeEngine(dm.service, clock=dm.clock).planner
        await reboot.initialize()
        assert (await reboot.plan_once(now=dm.clock(), automatic=True))["status"] == "not_due"
    assert len(fake.calls) == 1
    assert not (await dm.store.one("SELECT consumed FROM global_events WHERE id=?", (ev.id,)))[0]
    assert (await dm.store.one("SELECT outcome FROM life_plan_runs"))[0] == "interrupted"


async def test_milestone_backlog_and_arrival_during_planning(dm, monkeypatch):
    planner, ws = dm.service.life_engine.planner, dm.service.world_state
    fake = setup_model(dm, intents=[])
    for i in range(21):
        await ws.publish_event("minecraft", "routine", {"sample": i}, now=dm.clock())
    first = await ws.publish_event(
        "minecraft", "milestone", {}, importance="high", now=dm.clock() + 1
    )
    before = planner.journal.start

    async def publish_during_start(*args, **kwargs):
        await ws.publish_event("minecraft", "second", {}, importance="high", now=dm.clock() + 2)
        return await before(*args, **kwargs)

    monkeypatch.setattr(planner.journal, "start", publish_during_start)
    result = await planner.plan_once(now=dm.clock())
    assert fake.calls[0][1]["unconsumed_events"][0]["id"] == first.id
    assert result["next_plan_at"] <= dm.clock() + 60
    monkeypatch.setattr(planner.journal, "start", before)
    dm.time[0] += 61
    await planner.plan_once(now=dm.clock(), automatic=True)
    assert fake.calls[1][1]["unconsumed_events"][0]["event_type"] == "second"


async def test_first_upgrade_honors_observed_recent_plan_without_counting_an_unseen_run(dm):
    from hakimi.planning_journal import PlannerJournal

    then = dm.clock() - 300
    async with dm.store.transaction():
        await dm.store.db.execute("DELETE FROM life_planner_clock")
        await dm.store.db.execute(
            "INSERT INTO ledger_work VALUES('call','world.plan','work','world.plan','work')"
        )
        await dm.store.db.execute("INSERT INTO ledger_times VALUES('call', ?)", (then,))
    journal = PlannerJournal(dm.store)
    assert (await journal.initialize(dm.clock(), initial_interval=7200))["next_at"] == then + 7200
    assert not await dm.store.one("SELECT 1 FROM life_plan_runs")


async def test_journal_commit_persists_optional_final_signature(dm):
    from hakimi.planning_journal import PlannerJournal

    journal = PlannerJournal(dm.store)
    now = dm.clock()
    await journal.initialize(now, initial_interval=3600)
    ws = dm.service.world_state

    run1 = await journal.start(
        "2026-11-14",
        now,
        trigger="revision",
        next_at=now + 3600,
        signature="pre-sig",
        explore_at=now + 7200,
        events=[],
    )
    assert run1 is not None
    await journal.commit(
        ws,
        run1,
        tickets=[],
        candidates=0,
        source="llm",
        outcome="model_chose_idle",
        reasons={},
        avoided=False,
        final_signature="post-sig",
    )
    clock_row = await dm.store.one("SELECT signature FROM life_planner_clock WHERE id=1")
    run_row = await dm.store.one("SELECT signature FROM life_plan_runs WHERE id=?", (run1,))
    assert clock_row[0] == "post-sig"
    assert run_row[0] == "pre-sig"

    run2 = await journal.start(
        "2026-11-14",
        now + 10,
        trigger="revision",
        next_at=now + 3610,
        signature="keep-sig",
        explore_at=now + 7210,
        events=[],
    )
    assert run2 is not None
    await journal.commit(
        ws,
        run2,
        tickets=[],
        candidates=0,
        source="llm",
        outcome="model_chose_idle",
        reasons={},
        avoided=False,
    )
    clock_row2 = await dm.store.one("SELECT signature FROM life_planner_clock WHERE id=1")
    assert clock_row2[0] == "keep-sig"


async def test_active_session_same_module_high_event_uses_15m_coalesce_cooldown(dm, monkeypatch):
    from hakimi.life_engine import ACTIVE_SESSION_EVENT_COOLDOWN_SECONDS

    engine = dm.service.life_engine
    planner = engine.planner
    ws = dm.service.world_state
    fake = setup_model(dm, intents=[])

    # Initial plan sets last_plan_at
    await planner.plan_once(now=dm.clock())
    assert len(fake.calls) == 1
    base_plan_at = planner.last_plan_at

    # Simulate active minecraft session -> same-module high event must NOT wake in 60s
    monkeypatch.setattr(engine, "active_session_exit", lambda: ("minecraft", object()))
    await ws.publish_event("minecraft", "mc_death", {}, importance="high", now=dm.clock() + 10)
    assert planner.next_plan_at >= base_plan_at + ACTIVE_SESSION_EVENT_COOLDOWN_SECONDS

    dm.time[0] = base_plan_at + 120
    skipped = await planner.plan_once(now=dm.clock(), automatic=True)
    assert skipped["status"] == "not_due"
    assert len(fake.calls) == 1

    # High event from a DIFFERENT module still wakes in 60s
    await ws.publish_event("bilibili_surf", "bili_hot", {}, importance="high", now=dm.clock())
    assert planner.next_plan_at <= dm.clock() + 10
