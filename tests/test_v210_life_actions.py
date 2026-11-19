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
from hakimi.modules.actions import Action, ActionCall, ActionCatalog, NoArgs, Outcome
from hakimi.modules.base import BaseModule, ModuleRegistry
from hakimi.modules.weather import WeatherModule
from hakimi.modules.weather_location import WeatherLocation
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
        await svc.life_engine.set_config({"quiet_hours": "23:55-00:05"})
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


async def test_weather_publishes_without_qq_or_greenhouse(settings, store):
    registry = ModuleRegistry()
    world = WorldStateManager(settings, store, registry)
    seen, requests = [], []
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    settings.weather_enabled = True

    def provider(request):
        requests.append(request)
        data = {"time": int(NOW), "interval": 900, "temperature_2m": 18, "weather_code": 2}
        return httpx.Response(200, json={"current": data})

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        weather = WeatherModule(settings, client, world)
        registry.register(weather)
        await world.initialize()
        world.subscribe(seen.append)
        assert (await weather.read(ActionCall({}, NoArgs(), NOW))).reason == "observation_recorded"
        assert (
            await weather.read(ActionCall({}, NoArgs(), NOW + 10))
        ).reason == "observation_cached"
    assert [module.module_id for module in registry.all()] == ["weather"]
    assert len(requests) == 1 and [event.event_type for event in seen] == ["weather_observed"]
    assert not await world.unconsumed_events()
    assert not await world.list_due_intents(now=NOW)


async def test_changing_weather_location_discards_old_cache_and_inflight_result(settings, store):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    world = WorldStateManager(settings, store, ModuleRegistry())
    await world.initialize()
    started, resume = asyncio.Event(), asyncio.Event()
    readings = []

    async def provider(request):
        latitude = float(request.url.params["latitude"])
        readings.append(latitude)
        if len(readings) == 2:
            started.set()
            await resume.wait()
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": int(NOW),
                    "interval": 900,
                    "temperature_2m": latitude,
                    "weather_code": 2,
                }
            },
        )

    call = ActionCall({}, NoArgs(), NOW)
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        weather = WeatherModule(settings, client, world)
        assert (await weather.read(call)).reason == "observation_recorded"
        await weather.set_location(WeatherLocation(latitude=12, longitude=15))
        assert "等待首次" in await weather.get_sensor_summary()
        pending = asyncio.create_task(weather.read(call))
        await asyncio.wait_for(started.wait(), 2)
        try:
            await weather.set_location(WeatherLocation(latitude=0, longitude=0))
        finally:
            resume.set()
        assert (await pending).reason == "weather_location_changed"
        assert (await weather.read(call)).reason == "observation_recorded"
        assert (await weather.read(call)).reason == "observation_cached"
        last = await world.latest_event("weather", "weather_observed")
        assert last.payload == {
            "time": int(NOW),
            "interval": 900,
            "temperature_2m": 0,
            "weather_code": 2,
        }
        restarted = WeatherModule(settings, client, world)
        await restarted.load_location()
        assert (await restarted.read(call)).reason == "observation_cached"
        assert readings == [31.23, 12, 0]


async def test_on_demand_weather_for_modules_has_no_timer_or_paid_calls(settings, store):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    requests = []
    at = int(NOW)
    current = {
        "time": at,
        "interval": 900,
        "temperature_2m": 17.5,
        "weather_code": 63,
        "precipitation": 0.4,
        "shortwave_radiation": 100.0,
        "is_day": 1,
    }

    async def upstream(req):
        requests.append(req)
        assert req.url.host == "api.open-meteo.com" and req.url.path == "/v1/forecast"
        assert req.url.params["latitude"] == "31.23" and req.url.params["timeformat"] == "unixtime"
        assert "Authorization" not in req.headers and "Cookie" not in req.headers
        await asyncio.sleep(0)
        return httpx.Response(
            200, json={"current": current, "latitude": 31.23, "longitude": 121.47}
        )

    async with setup(settings, store, upstream) as (svc, wire, _):
        engine = svc.life_engine
        assert set(engine.query_options()) == {"weather.read"}
        assert "weather.read" in {a["name"] for a in engine.catalog.view({}, planner=True)}
        assert not svc.weather.schedules() and "weather.owner_report" not in svc.modules.rules
        for ts in (at, at + 3600, at + 7200):
            await engine.tick_gatekeeper_once(now=ts)
            await svc.modules.collect_sensor_summaries()
        assert not requests
        assert (await engine.query("qq.speak")).reason == "unknown_query"

        observed = []

        async def greenhouse(event):
            if event.event_type == "weather_observed":
                # A consumer may query again from the event handler.
                observed.append((await engine.query("weather.read")).details["observation"])

        svc.world_state.subscribe(greenhouse)
        results = await asyncio.wait_for(
            asyncio.gather(*(engine.query("weather.read") for _ in range(4))), 2
        )
        assert len(requests) == 1 and observed == [current]
        assert all(result.details["observation"] == current for result in results)
        assert all(
            "中雨" in result.details["summary"] and "17.5" in result.details["summary"]
            for result in results
        )
        assert sorted(r.reason for r in results) == ["observation_cached"] * 3 + [
            "observation_recorded"
        ]
        assert (
            not await svc.world_state.unconsumed_events()
            and not await svc.world_state.list_due_intents(now=at)
        )
        assert not wire.calls and not svc.ai.calls

    # A restart reuses only the provider interval for this location.
    async with setup(settings, store, upstream) as (svc, wire, _):
        svc.life_engine.clock = lambda: at + 899
        assert (await svc.life_engine.query("weather.read")).reason == "observation_cached"
        assert len(requests) == 1
        svc.life_engine.clock = lambda: at + 900
        stale = await svc.life_engine.query("weather.read")
        assert stale.reason == "weather_stale" and not stale.details
        current.update(time=at + 900, temperature_2m=19.25)
        assert (await svc.life_engine.query("weather.read")).details["observation"][
            "temperature_2m"
        ] == 19.25
        await svc.life_engine.set_config({"action_modes": {"weather.read": "off"}})
        assert "weather.read" not in svc.life_engine.query_options()
        assert (await svc.life_engine.query("weather.read")).reason == "action_disabled"
        await svc.life_engine.set_module_enabled(svc.weather, False)
        assert not svc.life_engine.query_options() and len(requests) == 3


@pytest.mark.parametrize(
    "address,expected", [("93.184.216.34", "done"), ("127.0.0.1", "gated_skip")]
)
async def test_weather_edge_timeout_retries_only_same_tls_host(settings, store, address, expected):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    hosts = []

    def provider(req):
        hosts.append(req.url.host)
        if req.url.host == "api.open-meteo.com":
            raise httpx.ConnectTimeout("simulated edge", request=req)
        if req.url.host == "dns.google":
            assert dict(req.url.params) == {
                "name": "api.open-meteo.com",
                "type": "A",
                "edns_client_subnet": "193.0.0.0/24",
            }
            return httpx.Response(
                200,
                json={
                    "Status": 0,
                    "Answer": [
                        {"name": "other.example.", "type": 1, "data": "8.8.8.8"},
                        {"name": "api.open-meteo.com.", "type": 1, "data": address},
                    ],
                },
            )
        assert req.url.host == "93.184.216.34" and req.url.scheme == "https"
        assert req.headers["Host"] == req.extensions["sni_hostname"] == "api.open-meteo.com"
        assert req.url.params["latitude"] == "31.23"
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": int(NOW),
                    "interval": 900,
                    "temperature_2m": 17,
                    "weather_code": 0,
                }
            },
        )

    async with setup(settings, store, provider) as (svc, _, _):
        result = await svc.life_engine.query("weather.read")
        assert result.status == expected
        assert hosts == ["api.open-meteo.com", "dns.google"] + (
            [address] if expected == "done" else []
        )


async def test_disabling_weather_during_lookup_returns_no_reading(settings, store):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    started, resume = asyncio.Event(), asyncio.Event()

    async def provider(request):
        started.set()
        await resume.wait()
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": int(NOW),
                    "interval": 900,
                    "temperature_2m": 18,
                    "weather_code": 0,
                }
            },
        )

    async with setup(settings, store, provider) as (svc, _, _):
        task = asyncio.create_task(svc.life_engine.query("weather.read"))
        await asyncio.wait_for(started.wait(), 2)
        try:
            await svc.life_engine.set_module_enabled(svc.weather, False)
        finally:
            resume.set()
        result = await task
        assert result.reason == "module_disabled" and not result.details
        assert not await svc.world_state.latest_event("weather", "weather_observed")


async def test_upgrade_retires_only_old_weather_timer_and_reminder(settings, store):
    async with setup(settings, store) as (svc, _, _):
        engine = svc.life_engine
        config = engine.config.model_dump()
        config["module_settings"]["weather"] = {"observation_time": "19:55"}
        config["action_modes"].update({"weather.daily_report": "off", "weather.read": "off"})
        await store.runtime.save_config("life_engine", config)
        pending = await svc.world_state.enqueue_intent(ticket("weather.read", origin="timer"))
        reminder = await svc.world_state.enqueue_intent(
            ticket("qq.speak", origin="rule", scope="owner:10000")
        )
        other = await svc.world_state.enqueue_intent(
            ticket("qq.speak", origin="planner", scope="owner:10000")
        )
        async with store.transaction():
            await store.db.execute(
                "INSERT INTO life_rule_receipts VALUES (?,?,?,?,?,?)",
                ("weather.owner_report", "old-observation", "", NOW, "enqueued", reminder),
            )
            await store.db.execute(
                "UPDATE scheduled_intents SET status='executing',handler_started=0 WHERE id=?",
                (reminder,),
            )
        restarted = LifeEngine(svc, clock=lambda: NOW)
        await restarted.initialize()
        async with svc.weather.lifespan():
            await svc.world_state.recover_actions()
            for ident in (pending, reminder):
                assert (
                    await store.one(
                        "SELECT result_reason FROM scheduled_intents WHERE id=?", (ident,)
                    )
                )[0] == "weather_schedule_retired"
            assert (await store.one("SELECT status FROM scheduled_intents WHERE id=?", (other,)))[
                0
            ] == "pending"
            assert svc.weather.scheduling(restarted.config).model_dump() == {}
            assert restarted.config.action_modes.get("weather.read") == "off"
            assert "weather.daily_report" not in restarted.config.action_modes


@pytest.mark.parametrize(
    "options",
    [
        {"origins": frozenset({"planner"})},
        {"social": True},
        {"budget_lane": "proactive"},
    ],
)
def test_background_sensors_cannot_bypass_chat_or_paid_action_gates(options):
    async def read(call):
        return Outcome("done", "sampled")

    spec = Action(
        name="sample.read",
        description="sample",
        run=read,
        background_sensor=True,
        **({"origins": frozenset({"timer"})} | options),
    )
    with pytest.raises(ValueError, match="background sensors"):
        ActionCatalog().install("sample", (spec,))


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
        await svc.world_state.experience.mc_snapshot("mine", svc.world_state.clock())
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
        svc.diary.world_event = diary.world_event
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
