"""A module installed after startup exercises the same admin, planner and gate as built-ins."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from pydantic import Field

from hakimi.life_engine import LifeEngine
from hakimi.life_rules import Reaction, Rule
from hakimi.modules.actions import Action, DailyAction, GateDecision, Outcome
from hakimi.modules.base import BaseModule, ModuleRegistry
from hakimi.modules.qq_social import QQSocialModule
from hakimi.modules.scheduling import ActivityQuota, LocalActivity, ScheduleOptions, ScheduleTime
from tests.test_v20_life_admin import life_api  # noqa: F401 (shared app fixture)


class ReadingOptions(ScheduleOptions):
    unlimited: bool = Field(False, title="阅读不限次数")
    read_at: ScheduleTime = Field("14:00", title="阅读时间")
    pages: int = Field(3, ge=1, le=12, title="每次阅读页数")


class ReadingModule(BaseModule):
    module_id = "reading"
    schedule_options = ReadingOptions

    def __init__(self, settings):
        super().__init__(settings)
        self.reads = []

    def actions(self):
        async def preflight(ticket, now, options):
            self.reads.append(options.pages)
            return GateDecision()

        async def read(call):
            return Outcome("done", "read")

        return (
            Action(
                name="reading.open",
                description="阅读",
                run=read,
                preflight=preflight,
                window="active",
                quotas=(ActivityQuota("reading_sessions", "unlimited"),),
            ),
        )

    def schedules(self):
        return (DailyAction("reading.open", "14:00", label="午后阅读", time_option="read_at"),)

    def event_rules(self):
        def arrived(event):
            if event.source_module == self.module_id and event.event_type == "book_arrived":
                return Reaction("reading.open", "查看新书", cooldown=0)

        return (Rule("reading.new_book", arrived),)

    def local_activity(self, context, available, unfunded):
        return LocalActivity(
            100,
            {
                "action_name": "reading.open",
                "intent_note": "阅读当日选书",
                "delay_minutes": 1,
                "ttl_minutes": 60,
            },
        )

    async def _raw_sensor_summary(self):
        return "书架就绪"


async def test_register_controls_timer_quota_fallback_and_live_rule(life_api):  # noqa: F811
    app, client, gateway = life_api
    svc = app.state.service
    engine = svc.life_engine
    module = ReadingModule(svc.s)
    svc.modules.register(module)
    noon = datetime(2026, 11, 18, 14, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    current = [noon]
    engine.clock = engine.planner.clock = engine.gatekeeper.clock = engine.jit.clock = lambda: (
        current[0]
    )
    path = "/admin/api/life/modules/reading"
    detail = (await client.get(path)).json()["scheduling"]
    assert detail["fields"]["pages"]["maximum"] == 12
    assert detail["values"] == {"unlimited": False, "pages": 3, "read_at": "14:00"}
    assert detail["timers"] == [
        {"action": "reading.open", "label": "午后阅读", "at": "14:00", "next_at": noon}
    ]
    assert (await client.put(path + "/schedule", json={"read_at": "24:00"})).status_code == 400
    changed = (await client.put(path + "/schedule", json={"read_at": "14:20"})).json()
    assert changed["scheduling"]["timers"][0]["next_at"] == noon + 1200
    assert (await client.put(path + "/schedule", json={"read_at": "14:00"})).is_success
    assert "reading.open" in {a["name"] for a in engine.catalog.view({}, planner=True)}
    assert (await client.put(path + "/schedule", json={"pages": 13})).status_code == 400
    assert (
        await client.put(path + "/schedule", json={"owner_dms_unlimited": True})
    ).status_code == 400
    assert (
        await client.put("/admin/api/life/modules/missing/schedule", json={"pages": 1})
    ).status_code == 404
    await client.put("/admin/api/life", json={"max_proactive_per_day": 0})
    await engine.tick_gatekeeper_once(now=noon)
    assert not module.reads
    assert (
        await svc.store.one(
            "SELECT result_reason FROM scheduled_intents WHERE action_name='reading.open'"
        )
    )[0] == "daily_proactive_limit"

    saved = (
        await client.put(
            path + "/schedule", json={"unlimited": True, "pages": 9, "read_at": "14:00"}
        )
    ).json()
    assert saved["scheduling"]["values"] == {"unlimited": True, "pages": 9, "read_at": "14:00"}
    assert (await client.get("/admin/api/life/overview")).json()["config"][
        "max_proactive_per_day"
    ] == 0
    clock = current[0] = noon + 86400
    await engine.tick_gatekeeper_once(now=clock)
    await engine.tick_gatekeeper_once(now=clock)
    assert module.reads == [9]
    usage = await engine.gatekeeper.daily_usage(clock, engine.config)
    assert usage["reading_sessions"] == usage["total"] == 1 and usage["counted"] == 0
    await client.put(path + "/schedule", json={"unlimited": False})
    assert (await engine.gatekeeper.daily_usage(clock, engine.config))["counted"] == 1
    await client.put(path + "/schedule", json={"unlimited": True})
    assert (await client.delete("/admin/api/life/schedule")).is_success
    assert engine.config.max_proactive_per_day == engine.env_config.max_proactive_per_day
    assert (
        await client.put("/admin/api/life/schedule", json={"module_settings": {}})
    ).status_code == 400
    assert (await client.get(path)).json()["scheduling"]["values"]["pages"] == 9
    before = len(gateway.calls)
    await svc.world_state.publish_event("reading", "book_arrived", {}, now=clock)
    assert await svc.store.one(
        "SELECT 1 FROM scheduled_intents WHERE action_name='reading.open' AND origin='rule'"
    )
    assert len(gateway.calls) == before
    plan = await engine.trigger_plan_once(now=clock)
    assert plan["source"] == "local_fallback"
    assert await svc.store.one(
        "SELECT 1 FROM scheduled_intents WHERE action_name='reading.open' AND origin='planner'"
    )
    # Quiet hours / active window still apply to a module with unlimited uses.
    await client.put("/admin/api/life", json={"active_hours": "18:00-22:00"})
    await engine.tick_gatekeeper_once(now=clock + 121)
    assert await svc.store.one(
        "SELECT 1 FROM scheduled_intents WHERE action_name='reading.open' "
        "AND result_reason='outside_active_hours'"
    )
    assert module.reads == [9]
    defaults = (await client.delete(path + "/schedule")).json()["scheduling"]["values"]
    assert defaults == {"unlimited": False, "pages": 3, "read_at": "14:00"}
    assert engine.config.active_hours == "18:00-22:00"


async def test_flat_production_settings_migrate_and_survive_independent_edits(life_api):  # noqa: F811
    app, client, _ = life_api
    svc = app.state.service
    old = {
        "active_hours": "09:00-22:30",
        "max_proactive_per_day": 10,
        "owner_dms_unlimited": True,
        "bili_patrols_unlimited": True,
        "owner_delivery_enabled": False,
        "group_cooldown_seconds": 1500,
    }
    await svc.store.runtime.save_config("life_engine", old)
    engine = svc.life_engine = LifeEngine(svc)
    await engine.initialize()
    root = "/admin/api/life"

    async def settings_for(module):
        return (await client.get(f"{root}/modules/{module}")).json()["scheduling"]["values"]

    assert (await settings_for("qq_social"))["owner_dms_unlimited"]
    assert (await settings_for("bilibili_surf"))["bili_patrols_unlimited"]
    assert not svc.proactive.delivery_enabled
    assert (await client.put(root, json={"max_proactive_per_day": 12})).is_success
    assert (
        await client.put(
            root + "/modules/qq_social/schedule", json={"group_cooldown_seconds": 1200}
        )
    ).is_success
    await client.delete(root + "/modules/bilibili_surf/schedule")
    restarted = svc.life_engine = LifeEngine(svc)
    await restarted.initialize()
    assert (
        restarted.config.max_proactive_per_day == 12
        and restarted.config.active_hours == "09:00-22:30"
    )
    assert not (await settings_for("bilibili_surf"))["bili_patrols_unlimited"]
    qq = await settings_for("qq_social")
    assert (
        qq["owner_dms_unlimited"]
        and not qq["owner_delivery_enabled"]
        and qq["group_cooldown_seconds"] == 1200
    )
    row = await svc.store.one("SELECT payload FROM admin_config WHERE key='life_engine'")
    stored = svc.store.vault.open("admin:life_engine", row[0])
    assert "group_cooldown_seconds" not in stored and "module_settings" in stored


def test_new_module_cannot_authorize_a_qq_recipient(settings):
    class OtherDelivery(ReadingModule):
        recipient_namespaces = ("reading",)

        def normalize_recipient(self, scope):
            return "owner:30000"

    registry = ModuleRegistry()
    registry.register(QQSocialModule(settings))
    registry.register(OtherDelivery(settings))
    assert registry.normalize_recipient("owner:20000") == "owner:20000"
    assert registry.normalize_recipient("owner:30000") is None
    assert registry.normalize_recipient("reading:any") is None
    other = OtherDelivery(settings)
    other.module_id = "duplicate"
    other.recipient_namespaces = ("owner",)
    with pytest.raises(ValueError, match="duplicate recipient namespace"):
        registry.register(other)
    assert registry.get("duplicate") is None
