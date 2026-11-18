"""Exercise the production action path with temporary SQLite, local fakes and a fixed clock."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from hakimi.adapters import Sender
from hakimi.life_engine import LifeEngine
from hakimi.modules.actions import Action, ActionCatalog, NoArgs, Outcome
from hakimi.modules.base import BaseModule
from hakimi.proactive_policy import ProactiveConfig
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.world_state import WorldStateManager
from tests.fakes import FakeBili
from tests.test_v20_life_engine import LifeGateway, Wire
from tests.test_v20_p4_cross_module_mc import FakeNodeTransport

NOW = datetime(2026, 11, 17, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


@asynccontextmanager
async def setup(settings, store, handler=None):
    def reject_network(req):
        raise AssertionError(f"unexpected network request to {req.url.host}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler or reject_network)) as http:
        wire = Wire()
        node = FakeNodeTransport()
        svc = Service(
            settings,
            store,
            LifeGateway(),
            FakeBili(),
            Sender(settings, http, wire),
            node_transport=node,
        )
        svc.modules.get("minecraft").apply_status({"bridge_ready": True})
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        svc.world_state.clock = lambda: NOW
        await svc.world_state.initialize()
        await svc.proactive.initialize()
        await svc.proactive.set_config(ProactiveConfig(enabled=True, quiet_hours="23:55-00:05"))
        await svc.life_engine.initialize()
        yield svc, wire, node


def ticket(name, *, scope="", origin="planner", arguments=None, verb=None):
    return dict(
        action_name=name,
        target_module="minecraft",
        action_type=verb or name.split(".")[-1],
        target_scope=scope,
        origin=origin,
        arguments=arguments or {},
        execute_after=NOW - 5,
        expire_at=NOW + 3600,
        intent_note="留意近期活动",
    )


def test_registry_is_atomic_and_denies_unregistered_names():
    async def run(call):
        return Outcome("done", "read")

    catalog = ActionCatalog()
    catalog.install("one", (Action(name="one.read", description="Read", run=run),))
    with pytest.raises(ValueError, match="duplicate"):
        catalog.install(
            "two",
            (
                Action(name="two.read", description="Read", run=run),
                Action(name="one.read", description="Duplicate", run=run),
            ),
        )
    assert list(catalog.actions) == ["one.read"]
    with pytest.raises(ValueError, match="unknown_action"):
        catalog.resolve(ticket("other.unregistered"))


async def test_due_tickets_compare_and_swap_and_ambiguous_node_reply(settings, store):
    async with setup(settings, store) as (svc, wire, node):
        engine = svc.life_engine
        ident = await svc.world_state.enqueue_intent(
            ticket("mc.build_plan", arguments={"topic": "红石"})
        )
        due = (await svc.world_state.list_due_intents(now=NOW))[0]
        node.responses["mc.dispatch_goal"] = Rejected("mc_rpc_timeout")
        results = await asyncio.gather(engine.jit.execute(due), engine.jit.execute(due))
        assert sorted(r["status"] for r in results) == ["not_claimed", "uncertain"]
        assert (
            await store.one("SELECT result_reason FROM scheduled_intents WHERE id=?", (ident,))
        )[0] == "mc_rpc_timeout"
        assert not await svc.world_state.list_due_intents(now=NOW + 200)
        assert len(node.calls) == 1 and node.calls[0][1] == {
            "action_type": "build_plan",
            "topic": "红石",
        }
        assert not wire.calls and not svc.ai.calls


async def test_scope_origin_arguments_modes_and_readiness(settings, store):
    async with setup(settings, store) as (svc, wire, node):
        engine = svc.life_engine
        intents = [
            ticket("mc.pause"),
            ticket("mc.goto", arguments={"x": True, "y": 1, "z": 1}),
            ticket("mc.build_plan", scope="owner:20000"),
            ticket("qq.speak", scope="owner:30000"),
            ticket("mc.build_plan", arguments={"host": "127.0.0.1"}),
            ticket("unknown.read"),
            ticket("mc.explore"),
        ]
        ids = [await svc.world_state.enqueue_intent(row) for row in intents]
        await engine.tick_gatekeeper_once(now=NOW)
        reasons = [
            (await store.one("SELECT result_reason FROM scheduled_intents WHERE id=?", (i,)))[0]
            for i in ids
        ]
        assert reasons == [
            "action_origin_forbidden",
            "invalid_action_arguments",
            "invalid_action_audience",
            "unauthorized_scope",
            "invalid_action_arguments",
            "unknown_action",
            "mc_server_not_connected",
        ]
        assert not wire.calls and not svc.ai.calls and not node.calls
        await engine.set_config({"action_modes": {"mc.build_plan": "shadow"}})
        key = await svc.world_state.enqueue_intent(ticket("mc.build_plan"))
        await engine.tick_gatekeeper_once(now=NOW)
        row = await store.one(
            "SELECT status,result_reason,shadow_result FROM scheduled_intents WHERE id=?", (key,)
        )
        assert tuple(row) == ("done", "plan_registered", "mc.build_plan → mc.build_plan")
        assert len(node.calls) == 1
        await engine.set_config({"action_modes": {"mc.build_plan": "off"}})
        await svc.world_state.enqueue_intent(ticket("mc.build_plan"))
        await engine.tick_gatekeeper_once(now=NOW)
        restarted = LifeEngine(svc, clock=lambda: NOW)
        await restarted.initialize()
        assert restarted.config.action_modes == {"mc.build_plan": "off"}
        assert len(node.calls) == 1 and not svc.ai.calls


async def test_claim_recovery_with_partial_and_complete_qq_receipts(settings, store):
    async with setup(settings, store) as (svc, _, __):
        ws = svc.world_state
        keys = [await ws.enqueue_intent(ticket("qq.speak", scope="owner:20000")) for _ in range(3)]
        for i, key in enumerate(keys):
            assert await ws.claim(key, NOW)
            await store.db.execute(
                "UPDATE scheduled_intents SET expected_bubbles=2 WHERE id=?", (key,)
            )
            for pos in range(i):
                await store.db.execute(
                    "INSERT INTO outbox_bubble_receipts VALUES (?,?,?,?,1)",
                    ("jit:" + key, pos, b"already-applied", NOW),
                )
        await store.db.commit()
        await ws.recover_actions()
        rows = [
            tuple(
                await store.one(
                    "SELECT status,result_reason FROM scheduled_intents WHERE id=?", (key,)
                )
            )
            for key in keys
        ]
        assert rows == [
            ("uncertain", "interrupted_without_final_receipt"),
            ("uncertain", "recovered_partial_receipt"),
            ("done", "recovered_complete_receipts"),
        ]
        assert not await ws.list_due_intents(now=NOW + 5)


async def test_upgrade_old_queue_in_place_and_keep_planner_and_rule_cursors_independent(
    settings, store
):
    await store.db.executescript("""
      CREATE TABLE scheduled_intents(id TEXT PRIMARY KEY,created_at REAL,execute_after REAL,
      expire_at REAL,target_module TEXT,target_scope TEXT,action_type TEXT,mood TEXT,
      gate_condition_json TEXT,intent_note_json TEXT,status TEXT,updated_at REAL);
      INSERT INTO scheduled_intents VALUES
      ('before',1,1,2,'minecraft','','idle','','{}','{}','done',1);
    """)
    old = WorldStateManager(settings, store)
    await old.initialize()
    await old.publish_event("bilibili_surf", "bili_video_watched", {"title": "红石"}, now=NOW - 300)
    async with setup(settings, store) as (svc, _, __):
        ws, engine = svc.world_state, svc.life_engine
        # Simulate process loss between persisting a fact and running its in-process subscriber.
        offline = WorldStateManager(settings, store)
        ev = await offline.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {"title": "红石刷铁机"},
            now=NOW,
            business_key="BV-demo",
        )
        await ws.mark_event_consumed(ev.id)
        same = await offline.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {"title": "changed"},
            now=NOW + 5,
            business_key="BV-demo",
        )
        assert same.id == ev.id and same.payload == ev.payload
        await engine.rules.catch_up(NOW)
        second = LifeEngine(svc, clock=lambda: NOW + 65)
        await second.initialize()
        await second.rules.catch_up(NOW + 65)
        await ws.publish_event(
            "bilibili_surf", "bili_video_watched", {"title": "红石刷怪塔"}, now=NOW + 60
        )
        await offline.publish_event(
            "bilibili_surf", "bili_video_watched", {"title": "红石"}, now=NOW - 8000
        )
        await second.rules.catch_up(NOW + 66)
        assert [
            r[0] for r in await store.all("SELECT outcome FROM life_rule_receipts ORDER BY rowid")
        ] == ["enqueued", "cooldown", "expired"]
        row = await store.one("SELECT * FROM scheduled_intents WHERE origin='rule'")
        assert row["source_module"] == "bilibili_surf" and row["source_event_id"] == ev.id
        assert row["action_name"] == "mc.build_plan" and row["target_scope"] == ""
        assert (
            await store.one(
                "SELECT origin,action_name,status FROM scheduled_intents WHERE id='before'"
            )
        )[:] == ("legacy", None, "done")


@pytest.mark.parametrize("notify_owner", [False, True])
async def test_fourth_module_timer_observation_and_optional_owner_report(
    settings, store, notify_owner
):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    settings.weather_time = "14:00"
    settings.weather_owner_report = notify_owner
    requests = []

    def weather(req):
        requests.append(req)
        assert req.url.host == "api.open-meteo.com" and req.url.path == "/v1/forecast"
        assert (
            req.url.params["latitude"] == "31.23"
            and "Authorization" not in req.headers
            and "Cookie" not in req.headers
        )
        return httpx.Response(200, json={"current": {"temperature_2m": 17.5, "weather_code": 3}})

    async with setup(settings, store, weather) as (svc, wire, node):
        engine = svc.life_engine
        assert any(a["name"] == "weather.read" for a in (await engine.status())["actions"])
        svc.ai.chat_json = "现在气温大约 17.5 度。"
        before = await engine.tick_gatekeeper_once(now=NOW)
        assert before["outcomes"] == {"done": 1}
        await engine.tick_gatekeeper_once(now=NOW + 10)
        assert len(requests) == 1 and not svc.ai.calls and not wire.calls and not node.calls
        ev = (await svc.world_state.unconsumed_events())[0]
        assert ev.payload == {"temperature_2m": 17.5, "weather_code": 3}
        assert not await svc.world_state.list_due_intents(now=NOW + 30)
        await engine.tick_gatekeeper_once(now=NOW + 65)
        assert len(wire.calls) == int(notify_owner) and len(svc.ai.calls) == int(notify_owner)
        if notify_owner:
            assert (
                wire.calls[0][0] == "send_private_msg"
                and str(wire.calls[0][1]["user_id"]) == "20000"
            )
            assert "17.5" in svc.ai.calls[0][1]["jit_proactive_intent"]["module_material"]


async def test_planner_discovers_new_action_but_cannot_forge_origin(settings, store):
    called = []

    class Sample(BaseModule):
        module_id = "sample"

        async def _raw_sensor_summary(self):
            return "ready"

        def actions(self):
            async def read(call):
                called.append(call.args)
                return Outcome("done", "sample_read")

            return (
                Action(name="sample.read", description="Read sample", arguments=NoArgs, run=read),
            )

    async with setup(settings, store) as (svc, wire, node):
        svc.modules.register(Sample(settings))
        engine = svc.life_engine
        svc.ai.plan_json = json.dumps(
            dict(
                current_module="sample",
                focus_level="LOW",
                mood="calm",
                energy=80,
                boredom=20,
                intents=[
                    dict(action_name=name, intent_note="查看本次记录", delay_minutes=1)
                    for name in ("sample.read", "mc.pause", "mc.not_registered")
                ],
            )
        )
        plan = await engine.trigger_plan_once(now=NOW)
        assert plan["intents_enqueued"] == 1
        offered = {item["name"] for item in svc.ai.calls[0][1]["available_actions"]}
        assert "sample.read" in offered and "mc.pause" not in offered
        await engine.tick_gatekeeper_once(now=NOW + 65)
        assert len(called) == 1 and not wire.calls and not node.calls


async def test_mode_change_during_generation_stops_outgoing(settings, store):
    async with setup(settings, store) as (svc, wire, _):
        engine = svc.life_engine
        key = await svc.world_state.enqueue_intent(ticket("qq.speak", scope="owner:20000"))
        started, release = asyncio.Event(), asyncio.Event()

        async def generate(*args, **kwargs):
            started.set()
            await release.wait()
            return "稍后再聊"

        svc.ai.generate = generate
        task = asyncio.create_task(engine.tick_gatekeeper_once(now=NOW))
        try:
            async with asyncio.timeout(3):
                await started.wait()
            await engine.set_config({"action_modes": {"qq.speak": "off"}})
        finally:
            release.set()
            await task
        assert not wire.calls
        assert tuple(
            await store.one("SELECT status,result_reason FROM scheduled_intents WHERE id=?", (key,))
        ) == ("gated_skip", "cancelled")


async def test_rule_source_proof_and_shadow_compare_cannot_escalate(settings, store):
    async with setup(settings, store) as (svc, wire, node):
        engine = svc.life_engine
        await svc.world_state.update_state(current_module="minecraft", focus_level="HIGH")
        ev = await svc.world_state.publish_event(
            "minecraft", "progress", {"block": "stone"}, now=NOW
        )
        forged = ticket("qq.speak", scope="owner:20000")
        forged.update(
            source_event_id=ev.id,
            source_module="minecraft",
            gate_condition={"milestone_event_id": ev.id},
        )
        key = await svc.world_state.enqueue_intent(forged)
        await engine.tick_gatekeeper_once(now=NOW)
        assert tuple(
            await store.one("SELECT status,result_reason FROM scheduled_intents WHERE id=?", (key,))
        ) == ("pending", "attention_lock")
        await engine.set_config({"action_modes": {"mc.build_plan": "shadow"}})
        mismatch = ticket("mc.build_plan", verb="idle")
        key = await svc.world_state.enqueue_intent(mismatch)
        await engine.tick_gatekeeper_once(now=NOW)
        row = await store.one(
            "SELECT status,result_reason,shadow_result FROM scheduled_intents WHERE id=?", (key,)
        )
        assert tuple(row) == (
            "gated_skip",
            "incompatible_action",
            "incompatible_action → mc.build_plan",
        )
        assert not wire.calls and not node.calls and not svc.ai.calls


async def test_journal_failure_does_not_block_qq_and_catches_up_after_restart(settings, store):
    async with setup(settings, store) as (svc, wire, node):

        class Journal:
            def __init__(self):
                self.available = False
                self.records = []

            async def world_event(self, key, source, kind, summary, *, now):
                if not self.available:
                    raise RuntimeError("temporary diary outage")
                self.records.append((key, source, kind))

        diary = Journal()
        svc.diary = diary
        ev = await svc.world_state.publish_event(
            "minecraft", "discovery", {"block": "iron"}, importance="high", now=NOW
        )
        assert (
            await store.one(
                "SELECT COUNT(*) FROM scheduled_intents WHERE source_event_id=?", (ev.id,)
            )
        )[0] == 1
        await svc.world_state.mark_event_consumed(ev.id)
        restarted = LifeEngine(svc, clock=lambda: NOW + 5)
        await restarted.initialize()
        diary.available = True
        await restarted.rules.catch_up(NOW + 5)
        await restarted.rules.catch_up(NOW + 6)
        assert diary.records == [("milestone:" + ev.id, "minecraft", "discovery")]
        assert (
            await store.one(
                "SELECT COUNT(*) FROM scheduled_intents WHERE source_event_id=?", (ev.id,)
            )
        )[0] == 1
        assert not node.calls and not wire.calls and not svc.ai.calls
