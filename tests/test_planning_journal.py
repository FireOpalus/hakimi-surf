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
