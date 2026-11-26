"""Exercise the production clock, independent lanes and durable activity reservations offline."""

import asyncio
from dataclasses import replace

import pytest

from hakimi.modules.actions import GateDecision, Outcome
from hakimi.modules.scheduling import ActivityQuota
from hakimi.rest_policy import RestHoursConfig
from tests.test_proactive_dm import dm as dm
from tests.test_proactive_dm import stamp


async def enqueue(dm, action, *, scope="", horizon=3600):
    now = dm.clock()
    return await dm.service.world_state.enqueue_intent(
        dict(
            action_name=action,
            action_type=action,
            target_module=dm.service.modules.catalog.owners[action],
            target_scope=scope,
            execute_after=now - 1,
            expire_at=now + horizon,
            origin="planner",
        ),
        now=now,
    )


def handler(dm, monkeypatch, name, run, **changes):
    actions = dm.service.modules.catalog.actions
    monkeypatch.setitem(
        actions, name, replace(actions[name], run=run, unavailable=lambda: None, **changes)
    )


async def state(dm, key):
    return tuple(
        await dm.store.one("SELECT status,result_reason FROM scheduled_intents WHERE id=?", (key,))
    )


async def test_slow_patrol_and_planning_leave_digest_clock_free(dm, monkeypatch):
    engine = dm.service.life_engine
    await engine.set_rest_policy(RestHoursConfig(rest_hours="22:30-09:00"))
    start = dm.time[0] = stamp("2026-11-14T22:14:00")
    started, planner_started, generating, release = (asyncio.Event() for _ in range(4))
    plan_calls, generation_times = [], []

    async def patrol(call):
        started.set()
        await release.wait()
        assert dm.clock() - start == 300
        return Outcome("done", "patrol_completed")

    async def plan(*, now, automatic):
        plan_calls.append(automatic)
        planner_started.set()
        await release.wait()
        engine.planner.next_plan_at = now + 7200

    generate = dm.service.ai.generate

    async def generate_digest(*args, **kwargs):
        generation_times.append(dm.clock())
        generating.set()
        return await generate(*args, **kwargs)

    handler(dm, monkeypatch, "bili.patrol", patrol)
    monkeypatch.setattr(engine, "trigger_plan_once", plan)
    monkeypatch.setattr(dm.service.ai, "generate", generate_digest)
    engine.planner.next_plan_at = start
    # More tickets than the old LIMIT in one module must never hide other lanes.
    patrol_ids = [await enqueue(dm, "bili.patrol") for _ in range(23)]
    runner = asyncio.create_task(engine.run())
    try:
        async with asyncio.timeout(5):
            await started.wait()
            await planner_started.wait()
            dm.time[0] = start + 60
            engine.wakeup.set()
            await generating.wait()
            assert 0 <= generation_times[0] - stamp("2026-11-14T22:15:00") <= 90
            assert (await state(dm, patrol_ids[0]))[0] == "executing"
            assert (await state(dm, patrol_ids[1]))[0] == "pending"
            assert plan_calls == [True]
            dm.time[0] = start + 300
            release.set()
            await asyncio.gather(*(t for _, t in engine.gatekeeper.workers.active.values()))
            assert (await state(dm, patrol_ids[0]))[0] == "done"
    finally:
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
    assert not engine._running and not engine._background and not engine.gatekeeper.workers.active


@pytest.mark.parametrize("first_outcome", ["gated_skip", "not_dispatched", "uncertain", "done"])
async def test_live_reservation_refund_and_conservative_completion(dm, monkeypatch, first_outcome):
    engine = dm.service.life_engine
    await engine.set_config({"max_proactive_per_day": 1})
    started, release = asyncio.Event(), asyncio.Event()
    second_calls = []

    async def first(call):
        started.set()
        await release.wait()
        return Outcome(first_outcome, "simulated")

    async def second(call):
        second_calls.append(call.ticket["id"])
        return Outcome("done", "confirmed")

    handler(dm, monkeypatch, "bili.patrol", first)
    handler(dm, monkeypatch, "mc.idle", second, quotas=(ActivityQuota(),))
    first_id = await enqueue(dm, "bili.patrol")
    await engine.tick_gatekeeper_once(wait=False)
    async with asyncio.timeout(5):
        await started.wait()
        second_id = await enqueue(dm, "mc.idle")
        await engine.tick_gatekeeper_once()
        assert not second_calls
        assert await state(dm, second_id) == ("pending", "daily_capacity_reserved")
        assert not await engine.gatekeeper.daily_limit_allows(
            "mc.idle", "", dm.clock(), engine.config
        )
        release.set()
        await asyncio.gather(*(t for _, t in engine.gatekeeper.workers.active.values()))
        assert (await state(dm, first_id))[0] == (
            "pending" if first_outcome == "not_dispatched" else first_outcome
        )
        dm.time[0] += 61
        await engine.tick_gatekeeper_once()
    if first_outcome in {"gated_skip", "not_dispatched"}:
        assert second_calls == [second_id] and (await state(dm, second_id))[0] == "done"
    else:
        assert not second_calls and await state(dm, second_id) == (
            "gated_skip",
            "daily_proactive_limit",
        )
    assert (await engine.gatekeeper.daily_usage(dm.clock(), engine.config))["counted"] == 1


async def test_recover_prepared_and_submitted_reservations_on_their_local_day(dm):
    ws, gate = dm.service.world_state, dm.service.life_engine.gatekeeper
    before = dm.time[0] = stamp("2026-11-14T23:59:58")
    available, uncertain, expired = [
        await enqueue(dm, "bili.patrol", horizon=n) for n in (200, 200, 1)
    ]

    async def reserve(_):
        return GateDecision()

    for key in (available, uncertain, expired):
        assert await ws.claim(key, before, quota_gate=reserve)
    assert await ws.start_handler(uncertain)
    dm.time[0] += 10
    await ws.recover_actions()
    await ws.recover_actions()
    assert await state(dm, available) == ("pending", "interrupted_before_dispatch")
    assert await state(dm, expired) == ("expired", "interrupted_before_dispatch")
    assert await state(dm, uncertain) == ("uncertain", "interrupted_without_final_receipt")
    assert (await gate.daily_usage(before, gate.config))["counted"] == 1
    assert (await gate.daily_usage(dm.clock(), gate.config))["counted"] == 0


async def test_two_simultaneous_claims_compete_for_one_daily_slot(dm):
    engine, ws, now = dm.service.life_engine, dm.service.world_state, dm.clock()
    await engine.set_config({"max_proactive_per_day": 1})
    keys = [
        await enqueue(dm, "bili.patrol"),
        await enqueue(dm, "social.speak", scope="owner:20000"),
    ]

    async def gated_claim(key):
        return await ws.claim(
            key, now, quota_gate=lambda ticket: engine.gatekeeper.quota_gate_tx(ticket, now)
        )

    rows = await asyncio.gather(*(gated_claim(key) for key in keys))
    assert sorted(row["status"] for row in rows) == ["executing", "pending"]
    pending = next(row for row in rows if row["status"] == "pending")
    assert pending["result_reason"] == "daily_capacity_reserved"
    assert (
        await dm.store.one("SELECT COUNT(*) FROM scheduled_intents WHERE claimed_at=?", (now,))
    )[0] == 1


async def test_manual_and_clock_serialize_audience_and_cancel_running_only(dm, monkeypatch):
    engine = dm.service.life_engine
    active, peak, calls = {}, {}, []
    entered, release = asyncio.Event(), asyncio.Event()

    async def speak(call):
        scope = call.ticket["target_scope"]
        active[scope] = active.get(scope, 0) + 1
        peak[scope] = max(peak.get(scope, 0), active[scope])
        calls.append(call.ticket["id"])
        if len(calls) == 2:
            entered.set()
        try:
            await release.wait()
            return Outcome("done", "ack")
        finally:
            active[scope] -= 1

    handler(dm, monkeypatch, "social.speak", speak, preflight=None)
    ids = [
        await enqueue(dm, "social.speak", scope=s)
        for s in ("owner:20000", "owner:20000", "onebot:40000")
    ]
    runner = asyncio.create_task(engine.run())
    manual = None
    try:
        async with asyncio.timeout(5):
            await entered.wait()
            assert set(calls) == {ids[0], ids[2]}
            waiting = asyncio.Event()
            original = engine.gatekeeper.workers._wait_for_ticket

            async def join(ticket, now):
                waiting.set()
                return await original(ticket, now)

            monkeypatch.setattr(engine.gatekeeper.workers, "_wait_for_ticket", join)
            manual = asyncio.create_task(engine.tick_gatekeeper_once())
            await waiting.wait()
            manual.cancel()
            await asyncio.gather(manual, return_exceptions=True)
            assert active == {"owner:20000": 1, "onebot:40000": 1}
            assert (await state(dm, ids[1]))[0] == "pending"
    finally:
        if manual and not manual.done():
            manual.cancel()
            await asyncio.gather(manual, return_exceptions=True)
        runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
    assert peak == {"owner:20000": 1, "onebot:40000": 1}
    assert await state(dm, ids[0]) == ("uncertain", "interrupted")
    assert await state(dm, ids[2]) == ("uncertain", "interrupted")
    assert (await state(dm, ids[1]))[0] == "pending"
    assert not engine.gatekeeper.workers.active and not engine._background
