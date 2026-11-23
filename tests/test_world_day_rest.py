import asyncio
import json

import pytest

from hakimi import world_experience
from hakimi.bili_watcher import BiliPolicy
from hakimi.modules.actions import Outcome
from hakimi.rest_policy import RestHoursConfig
from hakimi.world_experience import (
    DAY_REST_COOLDOWN_SECONDS,
    MAX_DAY_REST_PER_DAY,
    day_rest_energy_delta,
)
from hakimi.world_state import WorldStateManager
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import stamp
from tests.test_social_integration import group
from tests.test_v20_life_engine import LifeGateway
from tests.test_world_workers import enqueue, handler, state


def choose_rest(dm, minutes, *, intents=()):
    fake = dm.service.ai = LifeGateway()
    fake.plan_json = json.dumps(
        {
            "current_module": "idle",
            "focus_level": "LOW",
            "mood": "calm",
            "rest_minutes": minutes,
            "intents": list(intents),
        }
    )
    return fake


async def test_chosen_nap_recovers_fractional_energy_and_survives_restart(dm, message):
    engine, world = dm.service.life_engine, dm.service.world_state
    start = dm.clock()
    await engine.set_rest_policy(RestHoursConfig(allow_owner_private=True))
    await world.update_state(energy=27)
    fake = choose_rest(dm, 90)
    await asyncio.gather(engine.trigger_plan_once(), engine.trigger_plan_once())
    end = start + 90 * 60
    assert dm.store.runtime.day_rest_end(start) == end
    assert dm.store.runtime.is_resting_for(group(message), start)
    assert not dm.store.runtime.is_resting_for(message, start)
    assert (await engine.trigger_plan_once(now=start))["status"] == "day_rest"
    assert len(fake.calls) == 1
    dm.time[0] = start + 359
    await world.experience.advance(dm.clock())
    assert world.current.energy == 32.9
    dm.time[0] += 1
    restarted = WorldStateManager(dm.s, dm.store, clock=dm.clock)
    await restarted.initialize()
    assert restarted.current.energy == 33 and dm.store.runtime.day_rest_end(dm.clock()) == end
    dm.time[0] = end + 600
    await restarted.experience.advance(dm.clock())
    assert restarted.current.energy == 56.7
    assert dm.store.runtime.rest_kind(dm.clock()) == "awake"


async def test_planner_can_choose_daytime_rest_and_does_not_turn_idle_into_sleep(dm):
    engine, world = dm.service.life_engine, dm.service.world_state
    await world.update_state(energy=80, boredom=100)
    fake = choose_rest(dm, 0)
    assert (await engine.trigger_plan_once())["intents_enqueued"] == 0
    assert not dm.store.runtime.day_rest_end(dm.clock())
    fake.plan_json = json.dumps({**json.loads(fake.plan_json), "rest_minutes": 30})
    assert (await engine.trigger_plan_once())["planning_mode"] == "revision"
    start = dm.clock()
    assert dm.store.runtime.day_rest_end(start) == start + 1800
    assert len(fake.calls) == 2
    assert (await world.experience.view(start))["actual"]["day_rest_source"] == "planner"
    dm.time[0] += 720
    await engine.set_rest_policy(RestHoursConfig(day_rest_enabled=False))
    assert not dm.store.runtime.day_rest_end(dm.clock())
    assert world.current.energy == 91
    await engine.set_rest_policy(RestHoursConfig(day_rest_enabled=True))
    dm.time[0] += 600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 90.7 and not dm.store.runtime.day_rest_end(dm.clock())


async def test_planner_can_choose_rest_with_all_platforms_disabled(dm):
    engine = dm.service.life_engine
    for module in dm.service.modules.active():
        await engine.set_module_enabled(module, False)
    fake = choose_rest(dm, 30)
    await engine.trigger_plan_once()
    assert fake.calls[0][1]["available_actions"] == []
    assert dm.store.runtime.day_rest_end(dm.clock()) == dm.clock() + 1800
    dm.time[0] += 1800
    # Immediately after wake-up, cooldown blocks a back-to-back nap.
    blocked = await engine.trigger_plan_once()
    assert blocked["status"] == "resting" and blocked["reason"] == "no_available_actions"
    assert not dm.store.runtime.day_rest_end(dm.clock())
    # Once the 3h cooldown expires, a second nap is permitted.
    dm.time[0] += DAY_REST_COOLDOWN_SECONDS
    assert (await engine.trigger_plan_once())["planning_mode"] == "revision"
    assert dm.store.runtime.day_rest_end(dm.clock()) == dm.clock() + 1800


async def test_nap_defers_discretionary_work_and_ends_before_daily_digest(dm, monkeypatch):
    monkeypatch.setattr(world_experience, "DAY_REST_PRE_SLEEP_MINUTES", 0)
    engine = dm.service.life_engine
    await engine.set_rest_policy(RestHoursConfig(rest_hours="22:30-09:00"))
    start = dm.time[0] = stamp("2026-11-14T22:04:00")
    await dm.service.world_state.update_state(energy=25)
    choose_rest(dm, 30)
    await engine.trigger_plan_once()
    due = stamp("2026-11-14T22:15:00")
    assert dm.store.runtime.day_rest_end(start) == due
    key = await enqueue(dm, "bili.patrol")
    await engine.gatekeeper.tick(now=start)
    assert await state(dm, key) == ("pending", "day_rest")
    dm.time[0] = due
    await dm.service.world_state.experience.advance(due)
    assert not dm.store.runtime.day_rest_end(due)
    assert await dm._generation_reason(due, automatic=True) is None


async def test_sleep_vs_quiet_time_and_confirmed_energy_charges(dm, message):
    world, engine = dm.service.world_state, dm.service.life_engine
    await engine.set_config({"quiet_hours": "12:00-13:00"})
    now = dm.clock()
    await world.update_state(energy=50)
    dm.time[0] += 600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 49.7
    await engine.set_rest_policy(RestHoursConfig(rest_hours="12:10-13:00"))
    dm.time[0] += 239
    await world.experience.advance(dm.clock())
    assert world.current.energy == 50.6
    dm.time[0] += 9
    await world.experience.advance(dm.clock())
    assert world.current.energy == 50.7
    async with dm.store.transaction():
        for _ in range(2):
            await world.experience.confirmed_tx(message, "one-turn", dm.clock(), [])
    await world.refresh()
    assert world.current.energy == 50.5
    await world.update_state(energy=99)
    dm.time[0] = now + 3500
    await world.experience.advance(dm.clock())
    assert world.current.energy == 100


async def test_work_ends_nap_and_only_confirmed_mc_time_consumes_energy(dm):
    world, engine = dm.service.world_state, dm.service.life_engine
    start = dm.clock()
    await world.update_state(energy=29)
    choose_rest(dm, 30)
    await engine.trigger_plan_once()
    assert dm.store.runtime.day_rest_end(start) == start + 1800
    dm.time[0] += 360
    await world.experience.mc_snapshot("mine", dm.clock())
    assert not dm.store.runtime.day_rest_end(dm.clock()) and world.current.energy == 35
    for _ in range(15):
        dm.time[0] += 30
        await world.experience.mc_snapshot("mine", dm.clock())
    assert world.current.energy == 34
    await world.experience.mc_snapshot("", dm.clock())
    dm.time[0] += 3600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 32


async def test_legacy_forced_nap_ends_on_upgrade_after_settling_elapsed_time(dm):
    world = dm.service.world_state
    start = dm.clock()
    await world.update_state(energy=20)
    async with dm.store.transaction():
        await dm.store.db.execute(
            "UPDATE world_day_rest SET started=?,ends=?,source='low_energy' WHERE id=1",
            (start, start + 5400),
        )
    dm.time[0] += 720
    for _ in range(2):
        reopened = WorldStateManager(dm.s, dm.store, clock=dm.clock)
        await reopened.initialize()
        assert reopened.current.energy == 31
        assert dm.store.runtime.rest_kind(dm.clock()) == "awake"


@pytest.mark.parametrize(("energy", "boredom", "nap"), [(0, 0, 0), (100, 17, 5)])
async def test_daytime_choices_can_include_exploration_and_a_later_activity(
    dm, monkeypatch, energy, boredom, nap
):
    svc = dm.service
    engine = svc.life_engine
    svc.bili_watcher.policy = BiliPolicy(enabled=True)
    svc.bili_watcher.creds["sessdata"] = "test"
    await svc.world_state.update_state(energy=energy, boredom=boredom)
    await engine.tick_gatekeeper_once(now=dm.clock())
    assert dm.store.runtime.rest_kind(dm.clock()) == "awake"
    fake = choose_rest(
        dm,
        nap,
        intents=[
            {"action_name": "bili.patrol", "at_local_time": "15:00", "intent_note": "看看关注更新"}
        ],
    )
    result = await engine.trigger_plan_once()
    assert result["intents_enqueued"] == 1
    payload = fake.calls[0][1]
    assert payload["unconsumed_events"] == []
    metrics = payload["current_world_state"]
    assert (metrics["energy"], metrics["boredom"]) == (energy, boredom)
    assert "low_energy" not in metrics and "exploration_preferred" not in metrics
    assert any(a["name"] == "bili.patrol" and a["effortful"] for a in payload["available_actions"])
    expected_end = dm.clock() + nap * 60 if nap else 0
    assert dm.store.runtime.day_rest_end(dm.clock()) == expected_end
    row = await dm.store.one("SELECT outcome,accepted FROM life_plan_runs WHERE trigger='morning'")
    assert tuple(row) == ("accepted", 1)
    key = result["intent_ids"][0]
    dm.time[0] = stamp("2026-11-14T15:00:00")
    visited = []

    async def patrol(call):
        visited.append(call.ticket["id"])
        return Outcome("done", "patrol_completed")

    handler(dm, monkeypatch, "bili.patrol", patrol)
    await engine.tick_gatekeeper_once()
    assert visited == [key]
    assert await state(dm, key) == ("done", "patrol_completed")


def test_day_rest_energy_delta_piecewise_rates():
    start = 1000.0
    assert day_rest_energy_delta(start, start, start) == 0.0
    assert day_rest_energy_delta(start, start + 10, start) == 0.0
    # 0–10 min: 1 pt / 1 min
    assert day_rest_energy_delta(start, start, start + 60) == pytest.approx(1.0)
    assert day_rest_energy_delta(start, start, start + 600) == pytest.approx(10.0)
    # 10–30 min: 1 pt / 2 min
    assert day_rest_energy_delta(start, start + 600, start + 720) == pytest.approx(1.0)
    assert day_rest_energy_delta(start, start + 600, start + 1800) == pytest.approx(10.0)
    assert day_rest_energy_delta(start, start, start + 1800) == pytest.approx(20.0)
    # > 30 min: 1 pt / 6 min
    assert day_rest_energy_delta(start, start + 1800, start + 2160) == pytest.approx(1.0)
    assert day_rest_energy_delta(start, start, start + 3600) == pytest.approx(25.0)
    assert day_rest_energy_delta(start, start, start + 5400) == pytest.approx(30.0)
    # Cross-boundary spans
    assert day_rest_energy_delta(start, start + 480, start + 720) == pytest.approx(3.0)
    assert day_rest_energy_delta(start, start + 1680, start + 2160) == pytest.approx(2.0)


async def test_day_rest_piecewise_recovery_is_tick_invariant_and_resets_per_nap(dm):
    engine, world = dm.service.life_engine, dm.service.world_state
    await world.update_state(energy=20)
    choose_rest(dm, 90)
    await engine.trigger_plan_once()
    start = dm.clock()

    # Step across 10m and 30m boundaries with an uneven 37s cadence and verify exact milestones.
    for target, expected in (
        (start + 600, 30.0),
        (start + 1800, 40.0),
        (start + 3600, 45.0),
        (start + 5400, 50.0),
    ):
        while dm.time[0] + 37 < target:
            dm.time[0] += 37
            await world.experience.advance(dm.clock())
        dm.time[0] = target
        await world.experience.advance(dm.clock())
        assert world.current.energy == expected

    # After cooldown expires, a fresh nap resets the piecewise curve back to 1 pt / 1 min.
    dm.time[0] += DAY_REST_COOLDOWN_SECONDS
    await world.experience.advance(dm.clock())
    await world.update_state(energy=50)
    choose_rest(dm, 10)
    await engine.trigger_plan_once()
    dm.time[0] += 600
    await world.experience.advance(dm.clock())
    assert world.current.energy == 60.0


async def test_day_rest_enforces_cooldown_daily_cap_and_pre_sleep_cutoff(dm):
    engine, world = dm.service.life_engine, dm.service.world_state
    await engine.set_rest_policy(RestHoursConfig(rest_hours="23:00-08:00"))
    dm.time[0] = stamp("2026-11-15T10:00:00")
    await world.experience.advance(dm.clock())
    await world.update_state(energy=25)

    # Nap #1 at 10:00..10:20 succeeds
    choose_rest(dm, 20)
    await engine.trigger_plan_once()
    assert dm.store.runtime.day_rest_end(dm.clock()) == stamp("2026-11-15T10:20:00")
    prefs = await world.experience.preferences(dm.clock())
    assert prefs["day_rest_count_today"] == 1
    assert prefs["day_rest_remaining_today"] == MAX_DAY_REST_PER_DAY - 1

    # Right after waking at 10:20 -> blocked by 3h cooldown
    dm.time[0] = stamp("2026-11-15T10:20:00")
    await world.experience.advance(dm.clock())
    prefs_cd = await world.experience.preferences(dm.clock())
    assert not prefs_cd["day_rest_available"]
    assert prefs_cd["day_rest_cooldown_minutes"] == DAY_REST_COOLDOWN_SECONDS // 60

    # Nap #2 at 13:20..13:40 succeeds once cooldown elapses
    dm.time[0] = stamp("2026-11-15T13:20:00")
    await world.experience.advance(dm.clock())
    choose_rest(dm, 20)
    await engine.trigger_plan_once()
    assert dm.store.runtime.day_rest_end(dm.clock()) == stamp("2026-11-15T13:40:00")

    # Even after 3h+ cooldown at 17:00, daily cap (2/day) blocks a 3rd nap
    dm.time[0] = stamp("2026-11-15T17:00:00")
    await world.experience.advance(dm.clock())
    prefs_cap = await world.experience.preferences(dm.clock())
    assert not prefs_cap["day_rest_available"]
    assert prefs_cap["day_rest_count_today"] == MAX_DAY_REST_PER_DAY
    assert prefs_cap["day_rest_remaining_today"] == 0

    # Next day resets the daily counter; within 2h of 23:00 bedtime (21:00) is blocked
    dm.time[0] = stamp("2026-11-16T20:59:00")
    await world.experience.advance(dm.clock())
    assert (await world.experience.preferences(dm.clock()))["day_rest_available"]
    dm.time[0] = stamp("2026-11-16T21:00:00")
    await world.experience.advance(dm.clock())
    assert not (await world.experience.preferences(dm.clock()))["day_rest_available"]


async def test_world_day_rest_legacy_table_auto_migrates_day_columns(settings, store):
    now = stamp("2026-11-14T11:00:00")
    async with store.transaction():
        await store.db.execute("DROP TABLE IF EXISTS world_day_rest")
        await store.db.execute(
            "CREATE TABLE world_day_rest ("
            "id INTEGER PRIMARY KEY CHECK(id=1), started REAL NOT NULL, ends REAL NOT NULL, "
            "source TEXT NOT NULL)"
        )
        await store.db.execute("INSERT INTO world_day_rest VALUES (1,0,0,'')")
    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    cols = {r[1] for r in await store.all("PRAGMA table_info(world_day_rest)")}
    assert {"day_key", "day_count"} <= cols
    prefs = await world.experience.preferences(now)
    assert prefs["day_rest_count_today"] == 0
    assert prefs["day_rest_remaining_today"] == MAX_DAY_REST_PER_DAY
