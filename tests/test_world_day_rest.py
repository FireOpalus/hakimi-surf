import asyncio
import json

from hakimi.rest_policy import RestHoursConfig
from hakimi.world_state import WorldStateManager
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import stamp
from tests.test_social_integration import group
from tests.test_v20_life_engine import LifeGateway
from tests.test_world_workers import enqueue, state


async def test_local_nap_recovers_whole_points_and_survives_restart(dm, message):
    engine, world = dm.service.life_engine, dm.service.world_state
    start = dm.clock()
    await engine.set_rest_policy(RestHoursConfig(allow_owner_private=True))
    await world.update_state(energy=27)
    await asyncio.gather(engine.rest_if_tired(start), engine.rest_if_tired(start))
    end = start + 90 * 60
    assert dm.store.runtime.day_rest_end(start) == end
    assert dm.store.runtime.is_resting_for(group(message), start)
    assert not dm.store.runtime.is_resting_for(message, start)
    assert (await engine.trigger_plan_once(now=start))["status"] == "day_rest"
    assert not dm.service.ai.calls
    dm.time[0] = start + 359
    await world.experience.advance(dm.clock())
    assert world.current.energy == 27
    dm.time[0] += 1
    restarted = WorldStateManager(dm.s, dm.store, clock=dm.clock)
    await restarted.initialize()
    assert restarted.current.energy == 28 and dm.store.runtime.day_rest_end(dm.clock()) == end
    dm.time[0] = end + 600
    await restarted.experience.advance(dm.clock())
    assert restarted.current.energy == 42
    assert dm.store.runtime.rest_kind(dm.clock()) == "awake"


async def test_planner_can_choose_daytime_rest_and_does_not_turn_idle_into_sleep(dm):
    engine, world = dm.service.life_engine, dm.service.world_state
    fake = dm.service.ai = LifeGateway()
    plan = json.loads(fake.plan_json)
    plan.update(current_module="idle", focus_level="LOW", mood="calm", intents=[], mood_longnick="")
    fake.plan_json = json.dumps(plan)
    await engine.trigger_plan_once()
    assert not dm.store.runtime.day_rest_end(dm.clock())
    plan["rest_minutes"] = 30
    fake.plan_json = json.dumps(plan)
    await engine.trigger_plan_once()
    start = dm.clock()
    assert dm.store.runtime.day_rest_end(start) == start + 1800
    assert len(fake.calls) == 2
    assert (await world.experience.view(start))["actual"]["day_rest_source"] == "planner"
    dm.time[0] += 720
    await engine.set_rest_policy(RestHoursConfig(day_rest_enabled=False))
    assert not dm.store.runtime.day_rest_end(dm.clock())
    assert world.current.energy == 82
    await engine.set_rest_policy(RestHoursConfig(day_rest_enabled=True))
    dm.time[0] += 600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 82 and not dm.store.runtime.day_rest_end(dm.clock())


async def test_nap_defers_discretionary_work_and_ends_before_daily_digest(dm):
    engine = dm.service.life_engine
    await engine.set_rest_policy(RestHoursConfig(rest_hours="22:30-09:00"))
    start = dm.time[0] = stamp("2026-11-14T22:04:00")
    await dm.service.world_state.update_state(energy=25)
    await engine.rest_if_tired(start)
    due = stamp("2026-11-14T22:15:00")
    assert dm.store.runtime.day_rest_end(start) == due
    key = await enqueue(dm, "bili.patrol")
    await engine.gatekeeper.tick(now=start)
    assert await state(dm, key) == ("pending", "day_rest")
    dm.time[0] = due
    await engine.rest_if_tired(due)
    assert not dm.store.runtime.day_rest_end(due)
    assert await dm._generation_reason(due, automatic=True) is None


async def test_sleep_vs_quiet_time_and_confirmed_energy_charges(dm, message):
    world, engine = dm.service.world_state, dm.service.life_engine
    await engine.set_config({"quiet_hours": "12:00-13:00"})
    now = dm.clock()
    await world.update_state(energy=50)
    dm.time[0] += 600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 50
    await engine.set_rest_policy(RestHoursConfig(rest_hours="12:10-13:00"))
    dm.time[0] += 239
    await world.experience.advance(dm.clock())
    assert world.current.energy == 50
    dm.time[0] += 1
    await world.experience.advance(dm.clock())
    assert world.current.energy == 51
    async with dm.store.transaction():
        for _ in range(2):
            await world.experience.confirmed_tx(message, "one-turn", dm.clock(), [])
    await world.refresh()
    assert world.current.energy == 50
    await world.update_state(energy=99)
    dm.time[0] = now + 3500
    await world.experience.advance(dm.clock())
    assert world.current.energy == 100


async def test_work_ends_nap_and_only_confirmed_mc_time_consumes_energy(dm):
    world, engine = dm.service.world_state, dm.service.life_engine
    start = dm.clock()
    await world.update_state(energy=29)
    await engine.rest_if_tired(start)
    dm.time[0] += 360
    await world.experience.mc_snapshot("mine", dm.clock())
    assert not dm.store.runtime.day_rest_end(dm.clock()) and world.current.energy == 30
    for _ in range(15):
        dm.time[0] += 30
        await world.experience.mc_snapshot("mine", dm.clock())
    assert world.current.energy == 29
    await world.experience.mc_snapshot("", dm.clock())
    dm.time[0] += 3600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 29
