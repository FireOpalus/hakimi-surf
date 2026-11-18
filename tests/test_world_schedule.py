"""One world clock, using a real SQLite store and offline QQ/model adapters."""

import asyncio

import pytest

from hakimi.bili_watcher import BiliPolicy
from hakimi.budget_policy import BudgetConfig
from hakimi.life_engine import LifeEngine
from hakimi.rest_policy import RestHoursConfig
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import stamp


@pytest.mark.parametrize(
    "active,quiet,rest,expected",
    [
        ("09:00-22:30", "23:30-08:30", None, ("22:15", "22:30")),
        ("09:00-22:30", "20:00-08:30", None, ("19:45", "20:00")),
        ("09:00-22:30", "23:30-08:30", "21:00-06:00", ("20:45", "21:00")),
        ("08:30-08:35", "23:30-08:30", None, ("08:30", "08:35")),
        ("22:00-02:00", "08:00-09:00", None, ("23:45", "00:00")),
        ("22:00-08:00", "21:00-09:00", None, None),
    ],
)
async def test_report_window(dm, active, quiet, rest, expected):
    engine = dm.service.life_engine
    await engine.set_config({"active_hours": active, "quiet_hours": quiet})
    if rest:
        await dm.store.runtime.set_rest_hours(RestHoursConfig(enabled=True, rest_hours=rest))
    day = dm.local().date()
    window = engine.digest_window(day)
    if expected is None:
        assert window is None
        assert await engine.next_digest_at(dm.clock()) is None
    else:
        assert tuple(dm.local(t).strftime("%H:%M") for t in window) == expected
        assert dm.local(window[1] - 1).date() == day
        assert engine.digest_due(window[0]) and not engine.digest_due(window[1])


async def test_concurrent_manual_and_daily_digest_share_generation_outbox_and_receipt(dm):
    svc = dm.service
    engine = svc.life_engine
    await engine.set_config({"active_hours": "09:00-22:30", "max_proactive_per_day": 0})
    dm.time[0] = stamp("2026-11-14T22:14:58")
    await engine.tick_gatekeeper_once()
    assert not await dm.store.all("SELECT id FROM proactive_schedule")
    assert await engine.next_digest_at(dm.clock()) == dm.clock() + 2
    svc.sender.onebot.ready.clear()
    dm.time[0] += 2
    await asyncio.gather(
        engine.tick_gatekeeper_once(), dm.manual_digest(), engine.tick_gatekeeper_once()
    )
    assert [c[0] for c in svc.ai.calls] == ["screen", "generate"]
    rows = await dm.store.all("SELECT target_scope,status,origin FROM scheduled_intents")
    assert (
        len(rows) == 1 and rows[0]["target_scope"] == "owner:20000" and rows[0]["origin"] == "timer"
    )
    assert (await dm.store.one("SELECT COUNT(*) FROM pending_owner_dms WHERE kind='digest'"))[
        0
    ] == 1
    assert not svc.sender.onebot.calls
    svc.sender.onebot.ready.set()
    await dm.deliver_once()
    await dm.initialize()
    await engine.tick_gatekeeper_once()
    await dm.manual_digest()
    assert len(svc.ai.calls) == 2 and len(svc.sender.onebot.calls) == 1
    assert len((await svc.sessions.snapshot(dm.target()))["recent_messages"]) == 1
    assert dm.local(await engine.next_digest_at(dm.clock())).day == 15


async def test_world_digest_requires_timer_owner_and_budget_and_works_offline(dm):
    engine = dm.service.life_engine
    await engine.set_config({"active_hours": "09:00-22:30", "max_proactive_per_day": 0})
    now = dm.time[0] = stamp("2026-11-14T22:20:00")
    await engine.schedule_due(now)
    ticket = (await dm.service.world_state.list_due_intents(now=now))[0]
    for field, value in [
        ("target_scope", "onebot:40000"),
        ("target_scope", "owner:30000"),
        ("origin", "planner"),
    ]:
        with pytest.raises(ValueError):
            engine.resolve(ticket | {field: value})
    dm.service.sender.onebot.ready.clear()
    await dm.store.db.execute("INSERT INTO meta VALUES ('budget_overrun','1')")
    await engine.tick_gatekeeper_once()
    assert not dm.service.ai.calls
    assert (
        await dm.store.one(
            "SELECT result_reason FROM scheduled_intents WHERE id=?", (ticket["id"],)
        )
    )[0] == "budget_limit"
    await dm.store.db.execute("DELETE FROM meta WHERE key='budget_overrun'")
    dm.time[0] += 86400
    await engine.tick_gatekeeper_once()
    row = await dm.store.one(
        "SELECT id,status,expires_at FROM pending_owner_dms WHERE kind='digest'"
    )
    assert row["status"] == "pending" and dm.local(row["expires_at"]).strftime("%H:%M") == "22:30"
    assert await dm.delivery_status(row["id"]) == "pending"
    await engine.set_config({"enabled": False})
    dm.service.sender.onebot.ready.set()
    await dm.deliver_once()
    assert not dm.service.sender.onebot.calls and await dm.delivery_status(row["id"]) == "skipped"


async def test_migrate_legacy_sqlite_policies_and_guard_against_resurrection(dm):
    store = dm.store
    await store.runtime.save_config(
        "proactive",
        {
            "enabled": False,
            "quiet_hours": "22:15-09:15",
            "daily_digest_enabled": False,
            "max_per_day": 2,
            "daily_digest_time": "20:30",
        },
    )
    await store.runtime.save_config(
        "bili_account",
        {
            "enabled": True,
            "times": ["13:00"],
            "jitter_minutes": 55,
            "allow_like": True,
            "daily_action_limit": 2,
        },
    )
    restored = dm.service.life_engine = LifeEngine(dm.service, clock=dm.clock)
    await restored.initialize()
    await dm.service.bili_watcher.initialize()
    state = await dm.service.bili_watcher.status()
    assert state["config"]["enabled"] and state["config"]["allow_like"]
    assert state["config"]["daily_action_limit"] == 2 and "times" not in state["config"]
    assert restored.config.quiet_hours == "22:15-09:15" and not dm.delivery_enabled
    assert restored.config.action_modes["qq.owner_digest"] == "off"
    assert not await store.one("SELECT 1 FROM admin_config WHERE key='proactive'")
    await restored.set_config(None)
    again = LifeEngine(dm.service, clock=dm.clock)
    await again.initialize()
    assert again.config.owner_delivery_enabled and again.config.quiet_hours == "23:30-08:30"


async def test_bili_and_social_share_one_discretionary_allowance(dm):
    engine = dm.service.life_engine
    await engine.set_config({"max_proactive_per_day": 1})
    now = dm.clock()
    for name, scope in [("bili.patrol", ""), ("qq.speak", "owner:20000")]:
        key = await dm.service.world_state.enqueue_intent(
            dict(
                action_name=name,
                origin="planner",
                target_module="bilibili_surf" if not scope else "qq_social",
                target_scope=scope,
                action_type=name,
                execute_after=now - 1,
                expire_at=now + 600,
            ),
            now=now,
        )
        if not scope:
            await dm.service.world_state.mark_intent(key, "done", now=now)
    await engine.tick_gatekeeper_once()
    assert await engine.gatekeeper._count_today_proactive(now) == 1
    row = await dm.store.one("SELECT result_reason FROM scheduled_intents WHERE id=?", (key,))
    assert row[0] == "daily_proactive_limit" and not dm.service.ai.calls


@pytest.mark.parametrize("patrol,proactive", [(0, 1), (1, 0), (0, 0)])
async def test_world_bili_and_qq_actions_check_their_actual_budget_lanes(
    dm, monkeypatch, patrol, proactive
):
    svc = dm.service
    engine = svc.life_engine
    await engine.set_config({"active_hours": "09:00-22:30"})
    await dm.store.set_budget_config(
        BudgetConfig(patrol_daily_usd=patrol, proactive_daily_usd=proactive)
    )
    now = dm.time[0] = stamp("2026-11-14T22:16:00")
    watcher = svc.bili_watcher
    watcher.policy = BiliPolicy(enabled=True)
    watcher.creds["sessdata"] = "test"
    patrol_calls = []

    async def run(*, slot):
        patrol_calls.append(slot)
        return {"status": "done", "learned": 1}

    monkeypatch.setattr(watcher, "run_once", run)
    await svc.world_state.enqueue_intent(
        dict(
            action_name="bili.patrol",
            origin="planner",
            target_module="bilibili_surf",
            action_type="patrol",
            execute_after=now - 1,
            expire_at=now + 600,
        ),
        now=now,
    )
    await engine.tick_gatekeeper_once()
    rows = {
        r["action_name"]: r
        for r in await dm.store.all(
            "SELECT action_name,status,result_reason FROM scheduled_intents"
        )
    }
    assert len(patrol_calls) == int(bool(patrol))
    assert (rows["bili.patrol"]["status"] == "done") == bool(patrol)
    assert (rows["qq.owner_digest"]["status"] == "done") == bool(proactive)
    assert [c[0] for c in svc.ai.calls] == (["screen", "generate"] if proactive else [])
    if not patrol:
        assert rows["bili.patrol"]["result_reason"] == "budget_limit"
    if not proactive:
        assert rows["qq.owner_digest"]["result_reason"] == "budget_limit"


async def test_unfunded_planner_can_schedule_one_patrol_from_its_own_lane(dm):
    svc, engine = dm.service, dm.service.life_engine
    watcher = svc.bili_watcher
    watcher.policy = BiliPolicy(enabled=True)
    watcher.creds["sessdata"] = "test"
    await dm.store.set_budget_config(BudgetConfig(patrol_daily_usd=1, proactive_daily_usd=0))
    for _ in range(2):
        plan = await engine.trigger_plan_once(now=dm.clock())
        assert plan["source"] == "local_fallback"
    tickets = await dm.store.all("SELECT action_name FROM scheduled_intents")
    assert [t[0] for t in tickets] == ["bili.patrol"]
    assert not svc.ai.calls
