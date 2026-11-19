"""A waking-day itinerary survives restarts and only its future, unclaimed slots can move."""

import asyncio
import json

import aiosqlite
import pytest

from hakimi.life_engine import LifeEngine
from hakimi.planning_journal import PlannerJournal
from hakimi.rest_policy import RestHoursConfig
from hakimi.world_observations import overview
from tests.test_planning_journal import setup_model
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import stamp
from tests.test_v20_life_admin import life_api as life_api


def at(clock, day=15):
    return stamp(f"2026-11-{day:02}T{clock}:00")


def activity(clock):
    return {
        "target_module": "bilibili_surf",
        "action_type": "group_chat_share",
        "target_scope": "onebot:40000",
        "intent_note": "分享公开学习心得",
        "at_local_time": clock,
    }


async def waking_day(dm, *, level="medium"):
    engine = dm.service.life_engine
    dm.time[0] = at("07:29")
    await engine.set_rest_policy(RestHoursConfig(rest_hours="23:20-07:30"))
    await engine.set_config({"active_hours": "09:00-22:30", "morning_thinking_level": level})
    async with dm.store.transaction():
        await dm.store.db.execute(
            "UPDATE life_planner_clock SET next_at=?,explore_at=?", (at("13:00"), at("20:00"))
        )
    await engine.planner.initialize()
    return engine


async def test_wake_anchors_to_sleep_end_not_quiet_end_and_covers_whole_day(dm):
    engine = await waking_day(dm, level="high")
    fake = setup_model(
        dm, intents=[activity(t) for t in ("09:30", "13:00", "16:00", "21:00", "08:15", "22:30")]
    )
    assert engine.planner.next_plan_at == at("07:30")
    assert (await engine.trigger_plan_once(automatic=True))["status"] == "scheduled"
    assert not fake.calls
    dm.time[0] += 60
    result = await engine.trigger_plan_once(automatic=True)
    assert (result["planning_mode"], result["intents_enqueued"]) == ("morning", 4)
    _, payload, _, kwargs = fake.calls[0]
    assert kwargs["morning_thinking"] == "high" and kwargs["budget_lane"] == "proactive"
    assert payload["activity_windows"] == [["09:00", "22:30"]]
    assert payload["today_agenda"] == []
    assert {"action": "qq.owner_digest", "time": "22:15"} in payload["reserved_activities"]
    data = await overview(dm.store, dm.clock(), "Asia/Shanghai")
    assert [row["execute_after"] for row in data["agenda"]] == [
        at(t) for t in ("09:30", "13:00", "16:00", "21:00")
    ]
    assert data["morning_plan"]["reasons"] == {"window_expired": 2}
    assert data["morning_plan"]["thinking_level"] == "high"
    assert not dm.service.sender.onebot.calls
    for _ in range(2):
        reboot = LifeEngine(dm.service, clock=dm.clock)
        await reboot.initialize()
        assert reboot.config.morning_thinking_level == "high"
        assert (await reboot.trigger_plan_once(automatic=True))["status"] == "scheduled"
    assert len(fake.calls) == 1
    dm.time[0] = at("10:00", day=16)  # slept through the next wake, prior signature unchanged
    assert (await reboot.trigger_plan_once(automatic=True))["planning_mode"] == "morning"
    assert len(fake.calls) == 2
    assert await dm.store.one(
        "SELECT 1 FROM life_plan_runs WHERE trigger='morning' AND day='2026-11-16'"
    )


async def test_event_revises_only_future_agenda_and_preserves_other_sources(dm, monkeypatch):
    engine = await waking_day(dm)
    dm.time[0] = at("09:00")
    fake = setup_model(
        dm, intents=[activity(t) for t in ("10:00", "14:00", "16:00", "17:00", "18:00", "19:00")]
    )
    morning = await engine.trigger_plan_once()
    past, move, cancel, invalid, claimed, keep = morning["intent_ids"]
    assert fake.calls[0][3]["morning_thinking"] == "medium"
    ws = dm.service.world_state
    assert await ws.mark_intent(past, "done", now=at("10:00"))
    external = await ws.enqueue_intent(
        {
            "target_module": "example",
            "action_type": "fixed",
            "origin": "timer",
            "execute_after": at("15:00"),
            "expire_at": at("15:30"),
            "intent_note": "独立定时任务",
        }
    )
    dm.time[0] = at("10:05")
    await ws.publish_event("minecraft", "discovery", {}, importance="high", now=dm.clock())
    dm.time[0] += 6
    changes = [
        {"intent_id": move, "operation": "move", "at_local_time": "20:30"},
        {"intent_id": cancel, "operation": "cancel"},
        {"intent_id": invalid, "operation": "move", "at_local_time": "23:45"},
        {"intent_id": claimed, "operation": "cancel"},
        {"intent_id": past, "operation": "cancel"},
        {"intent_id": external, "operation": "cancel"},
    ]
    fake.plan_json = json.dumps(
        {
            **json.loads(fake.plan_json),
            "intents": [activity("12:15"), activity("19:00")],
            "agenda_changes": changes,
        }
    )
    commit = engine.planner.journal.commit

    async def concurrent_claim(*args, **kwargs):
        assert await ws.claim(claimed, at("18:00"))
        return await commit(*args, **kwargs)

    monkeypatch.setattr(engine.planner.journal, "commit", concurrent_claim)
    revision = await engine.trigger_plan_once(automatic=True)
    assert revision["planning_mode"] == "revision" and revision["intents_enqueued"] == 1
    payload = fake.calls[1][1]
    assert payload["planning_mode"] == "revision" and fake.calls[1][3]["morning_thinking"] is None
    assert {i["id"] for i in payload["today_agenda"]} == set(morning["intent_ids"])
    data = await overview(dm.store, dm.clock(), "Asia/Shanghai")
    items = {i["id"]: i for i in data["agenda"]}
    assert items[past]["status"] == "done" and items[claimed]["status"] == "executing"
    assert items[move]["execute_after"] == at("20:30")
    assert items[cancel]["result_reason"] == "agenda_cancelled"
    assert items[invalid]["execute_after"] == at("17:00") and items[keep]["status"] == "pending"
    assert external not in items
    assert (await dm.store.one("SELECT status FROM scheduled_intents WHERE id=?", (external,)))[
        0
    ] == "pending"
    latest = data["recent_plans"][0]
    assert latest["trigger"] == "milestone" and latest["outcome"] == "agenda_revised"
    assert (latest["candidates"], latest["accepted"]) == (8, 3)
    assert latest["reasons"] == {
        "agenda_item_unavailable": 3,
        "invalid_agenda_time": 1,
        "agenda_moved": 1,
        "agenda_cancelled": 1,
        "agenda_duplicate": 1,
    }


async def test_morning_checkpoint_recovered_during_quiet_and_not_generated_twice(dm, monkeypatch):
    engine = await waking_day(dm)
    fake = setup_model(dm, intents=[activity("09:00")])
    dm.time[0] = at("07:30")
    commit = engine.planner.journal.commit
    entered, release = asyncio.Event(), asyncio.Event()

    async def interrupted(*args, **kwargs):
        entered.set()
        await release.wait()
        raise RuntimeError("power cut")

    monkeypatch.setattr(engine.planner.journal, "commit", interrupted)
    first = asyncio.create_task(engine.trigger_plan_once(automatic=True))
    async with asyncio.timeout(5):
        await entered.wait()
        manual = asyncio.create_task(engine.trigger_plan_once())
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(RuntimeError, match="power cut"):
            await first
        with pytest.raises(RuntimeError, match="power cut"):
            await manual
    monkeypatch.setattr(engine.planner.journal, "commit", commit)
    dm.time[0] += 30
    reboot = LifeEngine(dm.service, clock=dm.clock)
    result = await reboot.trigger_plan_once(automatic=True)
    assert result["planning_mode"] == "morning" and result["intents_enqueued"] == 1
    assert len(fake.calls) == 1
    assert (await reboot.trigger_plan_once(automatic=True))["status"] == "scheduled"
    assert (await dm.store.one("SELECT COUNT(*) FROM life_plan_runs WHERE trigger='morning'"))[
        0
    ] == 1


async def test_midnight_tail_is_not_next_morning_and_rest_policy_updates_the_clock(dm):
    engine = await waking_day(dm)
    dm.time[0] = at("00:05", day=16)
    await engine.set_rest_policy(RestHoursConfig(enabled=False))
    await engine.set_config({"active_hours": "08:45-02:00", "quiet_hours": "03:00-06:00"})
    fake = setup_model(dm, intents=[])
    assert (await engine.trigger_plan_once())["status"] == "sleeping"
    assert engine.planner.next_plan_at <= at("08:45", day=16)
    dm.time[0] = at("08:45", day=16)
    result = await engine.trigger_plan_once(automatic=True)
    assert result["planning_mode"] == "morning" and len(fake.calls) == 1


async def test_manual_and_timer_get_the_same_itinerary_while_model_is_busy(dm, monkeypatch):
    engine = await waking_day(dm)
    fake = setup_model(dm, intents=[activity("18:00")])
    dm.time[0] = at("07:30")
    started, release = asyncio.Event(), asyncio.Event()
    generate = fake.generate

    async def delayed(*args, **kwargs):
        started.set()
        await release.wait()
        return await generate(*args, **kwargs)

    monkeypatch.setattr(fake, "generate", delayed)
    async with asyncio.timeout(5):
        timer = asyncio.create_task(engine.trigger_plan_once(automatic=True))
        await started.wait()
        manual = asyncio.create_task(engine.trigger_plan_once())
        await asyncio.sleep(0)
        release.set()
        first, second = await asyncio.gather(timer, manual)
    assert first == second and len(first["intent_ids"]) == len(fake.calls) == 1


async def test_admin_persists_only_supported_morning_levels(life_api):
    app, client, _ = life_api
    url = "/admin/api/life/schedule"
    assert (await client.put(url, json={"morning_thinking_level": "low"})).status_code == 400
    changed = await client.put(url, json={"morning_thinking_level": "high"})
    assert changed.json()["config"]["morning_thinking_level"] == "high"
    engine = LifeEngine(app.state.service)
    await engine.initialize()
    assert engine.config.morning_thinking_level == "high"


async def test_upgrade_and_backup_keep_old_queue_identity_without_adopting_rule_work(dm, tmp_path):
    engine = await waking_day(dm)
    dm.time[0] = at("09:00")
    setup_model(dm, intents=[activity("17:00"), activity("19:00")])
    keep, other = (await engine.trigger_plan_once())["intent_ids"]
    async with dm.store.transaction():
        await dm.store.db.execute("UPDATE life_plan_runs SET trigger='manual'")
        await dm.store.db.execute("UPDATE scheduled_intents SET origin='rule' WHERE id=?", (other,))
        await dm.store.db.execute("DROP TABLE life_agenda_items")
        await dm.store.db.execute("DROP INDEX life_one_morning")
        await dm.store.db.execute("ALTER TABLE life_plan_runs DROP COLUMN thinking_level")
    for _ in range(2):
        await PlannerJournal(dm.store).initialize(dm.clock())
    data = await overview(dm.store, dm.clock(), "Asia/Shanghai")
    assert [r["id"] for r in data["agenda"]] == [keep] and data["morning_plan"] is None
    snapshot = tmp_path / "restored.sqlite3"
    async with dm.store.lock:
        await dm.store.db.execute("VACUUM INTO ?", (str(snapshot),))
    async with aiosqlite.connect(snapshot) as copy:
        assert await copy.execute_fetchall("PRAGMA integrity_check") == [("ok",)]
        assert await copy.execute_fetchall("SELECT day,intent_id FROM life_agenda_items") == [
            ("2026-11-15", keep)
        ]
        assert await copy.execute_fetchall("SELECT COUNT(*) FROM scheduled_intents") == [(2,)]
