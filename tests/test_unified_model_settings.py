"""Registered model scenes, hot selection and migration use the production admin entry points."""

import json

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import generation_plan
from hakimi.modules import BaseModule, ModuleRegistry, ThinkingScene
from hakimi.security import Rejected
from hakimi.thinking_policy import preferences
from tests.test_admin import admin_api as admin_api
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


class Greenhouse(BaseModule):
    module_id = "greenhouse"
    display_name = "温室"
    thinking_scenes = (
        ThinkingScene("greenhouse.report", "湿度简报", default="low", output_tokens=768),
    )

    async def _raw_sensor_summary(self):
        return ""


async def test_registered_plugin_round_trip_and_shared_model_on_actual_wire(admin_api, sdk_pair):
    app, client, _ = admin_api
    svc = app.state.service
    url = "/admin/api/models"
    assert (await client.get(url, headers={"Authorization": ""})).status_code == 401
    policy = {"primary_model": "neon", "levels": {"greenhouse.report": "high"}}
    assert (await client.put(url, json=policy)).status_code == 400
    svc.modules.register(Greenhouse())
    assert (await client.put(url, json=policy)).is_success
    await svc.store.runtime.load_config()
    assert generation_plan(svc.s, "greenhouse.report").output_tokens == 768
    with pytest.raises(Rejected, match="invalid_generation_scenario"):
        generation_plan(svc.s, "missing.report")
    bodies = []

    def mock(request):
        body = json.loads(request.content)
        bodies.append(body)
        return httpx.Response(
            200,
            json=decision_response()
            if request.url.path.endswith("decisions")
            else interaction_response(),
        )

    gateway = GeminiGateway(svc.s, svc.store, *sdk_pair(mock))
    await gateway.generate({"readings": "dry"}, "test", scenario="greenhouse.report")
    assert bodies[0]["model"] == "gemini-4-neon" and bodies[0]["store"] is False
    assert bodies[0]["generation_config"]["thinking_level"] == "high"
    assert bodies[0]["generation_config"]["max_output_tokens"] == 768
    assert bodies[1]["model"] == "gemini-decision-1"
    await gateway.generate(
        {"rows": []},
        "archive",
        scenario="memory_compression",
        budget_lane="memory",
        memory_archive=True,
    )
    assert bodies[2]["service_tier"] == "flex" and bodies[2]["model"] == "gemini-4-neon"
    flex = await svc.store.one(
        "SELECT estimated_amount FROM ledger_usage WHERE model_lane='neon_flex'"
    )
    assert flex[0] == 525  # 100 input and 50 total output tokens, at Flex rates.
    assert (
        await client.put(
            url, json={"primary_model": "neon", "levels": {"greenhouse.report": "xhigh"}}
        )
    ).is_success
    assert (await client.put(url, json={"primary_model": "carbon", "levels": {}})).is_success
    assert generation_plan(svc.s, "greenhouse.report").thinking_level == "high"
    assert (
        await client.put(
            url, json={"primary_model": "carbon", "levels": {"greenhouse.report": "xhigh"}}
        )
    ).status_code == 400
    assert (await client.put(url, json=policy)).is_success
    # Simulate reloading the manifest without this plugin, then installing it again.
    without = ModuleRegistry()
    for module in svc.modules.all():
        if module.module_id != "greenhouse":
            without.register(module)
    svc.modules = without
    night = (await client.delete(url)).json()
    assert "greenhouse" not in {g["id"] for g in night["groups"]}
    assert (await client.put(url, json=policy)).status_code == 400
    without.register(Greenhouse())
    restored = (await client.get(url)).json()
    assert (
        next(g for g in restored["groups"] if g["id"] == "greenhouse")["scenes"][0]["value"]
        == "high"
    )
    assert restored["primary_model"] == "hydrogen"


async def test_migration_and_atomic_rhythm_validation(admin_api):
    app, client, fake = admin_api
    svc = app.state.service
    rt = svc.store.runtime
    await rt.save_config(
        "life_engine",
        {
            "active_hours": "09:00-22:30",
            "morning_thinking_level": "high",
            "max_proactive_per_day": 7,
        },
    )
    await rt.save_config("thinking", {"fast_chat": "low"})
    await rt.load_config()
    await rt.load_config()
    assert preferences(svc.s).scene_levels == {"world.morning": "high"}
    assert preferences(svc.s).fast_chat == "low"
    assert preferences(svc.s).primary_model == "hydrogen"
    stored = await svc.store.one("SELECT payload FROM admin_config WHERE key='life_engine'")
    assert svc.store.vault.open("admin:life_engine", stored[0]) == {"max_proactive_per_day": 7}
    url = "/admin/api/life/rhythm"
    rest = {
        "enabled": True,
        "rest_hours": "23:20-07:30",
        "day_rest_enabled": True,
        "allow_owner_private": True,
    }
    ok = await client.put(url, json={"schedule": {"quiet_hours": "23:30-08:30"}, "rest": rest})
    assert ok.is_success and ok.json()["rest"]["config"] == rest
    failed = await client.put(
        url,
        json={
            "schedule": {"max_proactive_per_day": 999},
            "rest": dict(rest, rest_hours="22:00-07:00"),
        },
    )
    assert failed.status_code == 400
    assert rt.rest_hours.rest_hours == "23:20-07:30"
    assert preferences(svc.s).scene_levels["world.morning"] == "high"
    assert not fake.calls
