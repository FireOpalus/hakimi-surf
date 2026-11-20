"""Tests for Phase 2 /admin/api/life router and admin_page.html CSP/UI."""

from __future__ import annotations

import base64
import hashlib
import re
import time

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.life_admin import LifeConfigMirror
from hakimi.life_admin import router as life_router
from hakimi.security import Rejected
from hakimi.world_state import IntentTicketCreate
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN


class FakeLifeEngine:
    """Minimal LifeEngine double for Stage A standalone router testing."""

    def __init__(self) -> None:
        self.config = LifeConfigMirror()
        self.source = "env"
        self.plan_calls = 0
        self.gatekeeper_calls = 0

    async def status(self) -> dict:
        return {
            "config": self.config.model_dump(mode="json"),
            "source": self.source,
            "proactive_sent_today": 1,
            "next_plan_at": 1_900_000_000.0,
        }

    async def set_config(self, value) -> dict:
        if value is None:
            self.config = LifeConfigMirror()
            self.source = "env"
        else:
            self.config = LifeConfigMirror.model_validate(value.model_dump(mode="json"))
            self.source = "sqlite"
        return await self.status()

    async def trigger_plan_once(self) -> dict:
        self.plan_calls += 1
        return {"status": "planned", "calls": self.plan_calls}

    async def tick_gatekeeper_once(self) -> dict:
        self.gatekeeper_calls += 1
        return {"status": "scanned", "calls": self.gatekeeper_calls}

    async def close(self) -> None:
        pass


def _mount_life_router(app) -> None:
    if not any(getattr(r, "path", "").startswith("/admin/api/life") for r in app.routes):
        app.include_router(life_router)


@pytest.fixture
async def life_api(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    _mount_life_router(app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            yield app, client, gateway


LIFE_ROUTES = [
    ("GET", "/admin/api/life"),
    ("GET", "/admin/api/life/modules"),
    ("GET", "/admin/api/life/overview"),
    ("GET", "/admin/api/life/modules/qq_social"),
    ("GET", "/admin/api/life/modules/minecraft/sensor"),
    ("PUT", "/admin/api/life/modules/qq_social/enabled"),
    ("PUT", "/admin/api/life/modules/qq_social/schedule"),
    ("DELETE", "/admin/api/life/modules/qq_social/schedule"),
    ("POST", "/admin/api/life/modules/weather/location/cities"),
    ("POST", "/admin/api/life/modules/weather/location/detect"),
    ("PUT", "/admin/api/life/modules/weather/location"),
    ("DELETE", "/admin/api/life/modules/weather/location"),
    ("PUT", "/admin/api/life"),
    ("DELETE", "/admin/api/life"),
    ("DELETE", "/admin/api/life/schedule"),
    ("PUT", "/admin/api/life/schedule"),
    ("PUT", "/admin/api/life/rhythm"),
    ("DELETE", "/admin/api/life/rhythm"),
    ("PUT", "/admin/api/life/world"),
    ("PUT", "/admin/api/life/actions/qq.speak"),
    ("POST", "/admin/api/life/plan"),
    ("POST", "/admin/api/life/gatekeeper"),
    ("DELETE", "/admin/api/life/intents/0123456789abcdef0123456789abcdef"),
    ("GET", "/admin/api/life/ledger"),
    ("POST", "/admin/api/life/ledger"),
    ("DELETE", "/admin/api/life/ledger/0123456789abcdef0123456789abcdef"),
    ("GET", "/admin/api/life/voice-companion"),
    ("PUT", "/admin/api/life/voice-companion"),
    ("DELETE", "/admin/api/life/voice-companion"),
    ("POST", "/admin/api/life/voice-companion/disconnect"),
]


async def test_module_browsing_reads_local_registry_and_scopes_actions(life_api, monkeypatch):
    app, client, gateway = life_api
    svc = app.state.service
    root = "/admin/api/life/modules"
    assert (await client.get(root + "/not-installed")).status_code == 404

    reads = []

    async def sensor():
        reads.append("sensor")
        return "本地测试摘要"

    for module in svc.modules.all():
        monkeypatch.setattr(module, "_raw_sensor_summary", sensor)

    catalog = (await client.get(root)).json()
    assert {m["id"] for m in catalog} == {m.module_id for m in svc.modules.all()}
    for entry in catalog:
        assert set(entry) == {
            "id",
            "title",
            "description",
            "action_count",
            "panel",
            "chat_platforms",
            "owner_delivery",
            "region",
            "enabled",
            "configuration_issue",
        }
        details = (await client.get(root + "/" + entry["id"])).json()
        assert details["enabled"] == entry["enabled"] == svc.modules.get(entry["id"]).enabled
        assert details["title"] == entry["title"]
        assert set(details) == {
            "enabled",
            "configuration_issue",
            "id",
            "title",
            "description",
            "panel",
            "chat_platforms",
            "actions",
            "status",
            "region",
            "proactive_paused",
            "scheduling",
        }
        assert entry["owner_delivery"] == (svc.proactive.platform in entry["chat_platforms"])
        assert details["panel"] == entry["panel"] == svc.modules.get(entry["id"]).admin_panel
        assert len(details["actions"]) == entry["action_count"]
        assert all(action["module"] == entry["id"] for action in details["actions"])

    response = await client.put(
        "/admin/api/life/actions/qq.speak", json={"mode": "off", "view": "module"}
    )
    assert response.status_code == 200
    assert response.json()["id"] == "qq_social"
    assert any(a["name"] == "qq.speak" and a["mode"] == "off" for a in response.json()["actions"])
    assert "world_state" not in response.json()
    core = await client.get("/admin/api/life/overview")
    assert core.status_code == 200
    assert reads == []
    assert (await client.get(root + "/weather/sensor")).status_code == 409
    assert (await client.get(root + "/qq_social/sensor")).json() == {"summary": "本地测试摘要"}
    assert reads == ["sensor"]
    assert not gateway.calls


async def test_admin_current_weather_reads_only_on_click_and_honors_access(life_api):
    app, client, gateway = life_api
    root = "/admin/api/life/modules/weather"
    requests = []

    def provider(req):
        requests.append(req)
        if len(requests) > 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": 1800000000,
                    "interval": 900,
                    "temperature_2m": 16.5,
                    "weather_code": 61,
                }
            },
        )

    svc = app.state.service
    svc.life_engine.clock = lambda: 1800000000
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        svc.weather.http = upstream
        assert (await client.get(root + "/current")).status_code == 409
        await client.put(root + "/enabled", json={"enabled": True})
        assert (await client.get(root + "/current")).json()["detail"] == "weather_location_missing"
        detail = (
            await client.put(root + "/location", json={"latitude": 40, "longitude": 120})
        ).json()
        assert detail["scheduling"] == {"fields": {}, "values": {}, "timers": []}
        assert [a["name"] for a in detail["actions"]] == ["weather.read"]
        assert (await client.get(root + "/sensor")).is_success
        assert (
            await client.get(root + "/current", headers={"Authorization": ""})
        ).status_code == 401
        assert not requests
        result = (await client.get(root + "/current")).json()
        assert result["observation"]["temperature_2m"] == 16.5 and "小雨" in result["summary"]
        assert (await svc.life_engine.query("weather.read")).details == result
        assert len(requests) == 1
        svc.life_engine.clock = lambda: 1800000900
        res = await client.get(root + "/current")
        assert res.status_code == 503 and res.json()["detail"] == "weather_unavailable"
        assert len(requests) == 2 and not gateway.calls


async def test_admin_weather_location_previews_and_upstream_failure(life_api):
    app, client, gateway = life_api
    root = "/admin/api/life/modules/weather"
    place = {"latitude": 37.56, "longitude": 126.98}
    calls = []

    def provider(request):
        calls.append(request.url.host)
        assert request.url.scheme == "https"
        assert all(h not in request.headers for h in ("Authorization", "Cookie", "X-Forwarded-For"))
        if request.url.host == "geocoding-api.open-meteo.com":
            assert request.url.path == "/v1/search"
            assert request.url.params["name"] == "首尔" and request.url.params["language"] == "zh"
            return httpx.Response(
                200,
                json={
                    "results": [
                        {**place, "name": "首尔", "admin1": "首尔", "country": "韩国"},
                        {**place, "name": "另一个地点", "latitude": 200},
                    ]
                },
            )
        assert request.url.host == "ipwho.is" and request.url.path == "/"
        if len(calls) == 2:
            return httpx.Response(
                200,
                json={
                    **place,
                    "city": "Seoul",
                    "country": "South Korea",
                    "success": True,
                    "ip": "192.0.2.7",
                },
            )
        return httpx.Response(429)

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
        app.state.service.weather.http = upstream
        res = await client.post(root + "/location/cities", json={"query": " 首尔 "})
        assert res.json() == {"places": [{**place, "label": "首尔 · 韩国", "source": "city"}]}
        res = await client.post(root + "/location/detect", headers={"X-Forwarded-For": "192.0.2.9"})
        candidate = {**place, "label": "Seoul · South Korea", "source": "server_ip"}
        assert res.json() == candidate
        assert (await client.get(root)).json()["status"]["location"] is None
        saved = (await client.put(root + "/location", json=candidate)).json()
        assert not saved["enabled"] and saved["configuration_issue"] is None
        assert saved["status"]["location"] == candidate
        assert (await client.post(root + "/location/detect")).status_code == 503
        assert (await client.get(root)).json()["status"]["location"] == candidate
    assert calls == ["geocoding-api.open-meteo.com", "ipwho.is", "ipwho.is"] and not gateway.calls


async def test_admin_weather_location_survives_restart_and_resets_to_environment(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    root = "/admin/api/life/modules/weather"
    place = {"source": "coordinates", "latitude": 0.0, "longitude": -180.0, "label": ""}
    for first_start in (True, False):
        app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + TOKEN},
            ) as client,
        ):
            loc = (await client.get(root)).json()["status"]["location"]
            if first_start:
                assert loc["source"] == "environment" and loc["latitude"] == 31.23
                for extra in (
                    {"latitude": True},
                    {"longitude": -181},
                    {"latitude": 91},
                    {"latitude": "0"},
                    {"source": "environment"},
                    {"url": "arbitrary"},
                ):
                    assert (
                        await client.put(root + "/location", json=place | extra)
                    ).status_code == 400
                assert (await client.put(root + "/location", json=place)).json()["status"][
                    "location"
                ] == place
            else:
                assert loc == place
                detail = (await client.delete(root + "/location")).json()
                assert detail["status"]["location"] == {
                    "latitude": 31.23,
                    "longitude": 121.47,
                    "label": "",
                    "source": "environment",
                }
            assert not app.state.service.ai.calls


async def test_minecraft_control_returns_only_its_module_snapshot(life_api):
    _, client, gateway = life_api
    response = await client.post(
        "/admin/api/life/mc/goal", json={"action_type": "status", "view": "module"}
    )
    assert response.status_code == 200
    assert set(response.json()) == {"status", "result", "mc_status"}
    assert response.json()["mc_status"]["connected"] is False
    assert not gateway.calls


async def test_admin_workspace_navigation_owns_each_panel_and_control_once(
    admin_frontend, life_api
):
    _, client, _ = life_api
    html = (await client.get("/admin")).text
    navs = dict(re.findall(r'<nav id="(core|modules)-nav"[^>]*>(.*?)</nav>', html, re.S))
    tabs = {key: set(re.findall(r'data-tab="([^"]+)"', nav)) for key, nav in navs.items()}
    panels = dict(re.findall(r'<section id="([^"]+)" class="tab"[^>]*>(.*?)</section>', html, re.S))
    assert tabs["core"].isdisjoint(tabs["modules"])
    assert tabs["core"] | tabs["modules"] == set(panels)
    assert html.count('id="module-heading-switch"') == 1
    assert "module-detail-state" not in html
    assert {"overview", "ledger", "life", "api-routing", "memory"} <= tabs["core"]
    assert {
        "modules",
        "qq",
        "bili",
        "minecraft",
        "group-policy",
        "admission",
        "patrol",
        "notebook",
        "event-ledger",
        "voice-companion",
        "drive-sync",
        "journal",
    } <= tabs["modules"]
    for key in tabs["core"]:
        shortcuts = set(re.findall(r'data-open-tab="([^"]+)"', panels[key]))
        assert shortcuts <= tabs["core"] | {"modules"}, (key, shortcuts)
    for marker, owner in (
        ('id="module-catalog"', "modules"),
        ('id="weather-editor"', "weather"),
        ('id="ws-heartbeat"', "qq"),
        ('id="qq-privacy-card"', "qq"),
        ('id="groups"', "group-policy"),
        ('id="sessions"', "memory"),
        ('id="life-mc-form"', "minecraft"),
        ('data-action="patrol"', "bili"),
        ('data-action="diary"', "journal"),
        ('id="system-catalog"', "modules"),
        ('data-action="notebook"', "notebook"),
        ('id="life-ledger-form"', "event-ledger"),
        ('id="life-ledger-table"', "event-ledger"),
        ('id="vc-config-form"', "voice-companion"),
        ('id="vc-sessions-table"', "voice-companion"),
    ):
        assert [key for key, content in panels.items() if marker in content] == [owner]


@pytest.mark.parametrize("method,path", LIFE_ROUTES)
async def test_life_admin_disabled_without_token(settings, method, path):
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    _mount_life_router(app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
        ) as client:
            res = await client.request(method, path, json={})
    assert res.status_code == 403
    assert res.json()["detail"] == "admin_disabled"


@pytest.mark.parametrize("method,path", LIFE_ROUTES)
async def test_life_admin_authenticates_before_validation_or_effect(life_api, method, path):
    app, client, gateway = life_api
    fake_engine = FakeLifeEngine()
    app.state.service.life_engine = fake_engine
    res = await client.request(
        method,
        path,
        headers={"Authorization": "Bearer wrong"},
        json={"unknown": True},
    )
    assert res.status_code == 401
    assert fake_engine.plan_calls == 0
    assert fake_engine.gatekeeper_calls == 0
    assert not gateway.calls


@pytest.mark.parametrize(
    "headers",
    [
        {"Origin": "https://evil.invalid"},
        {"Host": "attacker.invalid"},
        {"Sec-Fetch-Site": "cross-site"},
    ],
)
async def test_life_admin_origin_and_rebinding_protection(life_api, headers):
    _, client, _ = life_api
    res = await client.get("/admin/api/life", headers=headers)
    assert res.status_code == 403


async def test_admin_page_life_tab_csp_sha256_and_zero_inline_attributes(admin_frontend, life_api):
    _, client, _ = life_api
    res = await client.get("/admin", headers={"Authorization": ""})
    assert res.status_code == 200
    html = res.text

    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(styles) == 1
    assert len(scripts) == 1

    style_hash = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
    script_hash = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
    csp = res.headers["content-security-policy"]
    assert f"'sha256-{style_hash}'" in csp
    assert f"'sha256-{script_hash}'" in csp
    assert "'unsafe-inline'" not in csp

    assert not re.search(r"\sstyle\s*=", html)
    assert not re.search(r"\son[a-z]+\s*=", html, re.I)
    assert "innerHTML" not in html
    assert "unpkg.com" not in html
    assert TOKEN not in html

    for marker in (
        'id="nav-life"',
        'data-tab="life"',
        'id="life"',
        "世界与主动引擎",
        'id="life-enabled"',
        'id="world-rhythm"',
        'id="wake-window"',
        "function moduleSchedulePanel(d)",
        'id="life-max-daily"',
        'id="life-world-module"',
        'id="life-world-focus"',
        'id="life-world-mood"',
        'id="life-world-energy"',
        'id="life-world-boredom"',
        'id="life-plan-now"',
        'id="life-gatekeeper-now"',
        'id="life-intents-table"',
        'id="life-events-table"',
    ):
        assert marker in html


async def test_life_get_baseline_bounds_and_missing_engine_503(life_api):
    app, client, _ = life_api
    svc = app.state.service

    # Without life_engine attached, GET returns defaults + env, while mutations return 503.
    if hasattr(svc, "life_engine"):
        delattr(svc, "life_engine")

    res = await client.get("/admin/api/life")
    assert res.status_code == 200
    data = res.json()
    assert data["source"] == "env"
    assert data["config"] == LifeConfigMirror().model_dump(mode="json")
    assert data["world_state"]["current_module"] == "idle"
    assert data["world_state"]["focus_level"] == "LOW"
    assert data["world_state"]["proactive_suspended"] is False
    module_ids = {m["module_id"] for m in data["world_state"]["modules"]}
    assert {"qq_social", "bilibili_surf", "minecraft"}.issubset(module_ids)
    assert data["nodes"]["onebot_cn_napcat"]["mode"] == svc.s.onebot_mode
    assert data["nodes"]["shanghai_node_agent"]["region"] == "CN"
    assert "enabled" in data["desktop_memory"]
    assert data["intents"] == []
    assert data["events"] == []

    # Invalid config returns 400 even before engine lookup; valid returns 503 when unmounted.
    assert (await client.put("/admin/api/life", json={"unknown": 1})).status_code == 400
    assert (await client.put("/admin/api/life", json={"enabled": False})).status_code == 503
    assert (await client.delete("/admin/api/life")).status_code == 503
    assert (await client.post("/admin/api/life/plan")).status_code == 503
    assert (await client.post("/admin/api/life/gatekeeper")).status_code == 503

    # Verify intent limit 40 and event limit 30 ordered DESC by created_at.
    base_ts = time.time()
    for i in range(45):
        await svc.world_state.enqueue_intent(
            IntentTicketCreate(
                target_module="qq_social",
                target_scope="onebot:40000",
                action_type="group_share",
                execute_after=base_ts + 60,
                expire_at=base_ts + 600,
                mood="curious",
                gate_condition={"seq": i},
                intent_note={"topic": f"note-{i}"},
            ),
            now=base_ts + i,
        )
    for i in range(35):
        await svc.world_state.publish_event(
            "minecraft",
            "milestone",
            {"seq": i},
            importance="normal",
            now=base_ts + i,
        )

    full = (await client.get("/admin/api/life")).json()
    assert len(full["intents"]) == 40
    assert full["intents"][0]["gate_condition"]["seq"] == 44
    assert full["intents"][0]["intent_note"]["topic"] == "note-44"
    assert len(full["events"]) == 30
    assert full["events"][0]["payload"]["seq"] == 34


async def test_life_config_crud_and_strict_validation(life_api):
    app, client, _ = life_api
    original = (await client.get("/admin/api/life")).json()["config"]

    for bad in (
        {},
        {"enabled": "true"},
        {"max_proactive_per_day": True},
        {"max_proactive_per_day": 31},
        {"owner_dms_unlimited": "on"},
        {"bili_patrols_unlimited": 1},
        {"group_cooldown_seconds": 299},
        {"active_hours": "09:00-09:00"},
        {"active_hours": "25:00-01:00"},
        {"planner_min_minutes": 300, "planner_max_minutes": 200},
        {"sleep_min_minutes": 500, "sleep_max_minutes": 400},
        {"extra_field": 123},
    ):
        assert (await client.put("/admin/api/life", json=bad)).status_code == 400

    saved = await client.put(
        "/admin/api/life",
        json={
            "enabled": False,
            "group_cooldown_seconds": 1800,
            "max_proactive_per_day": 3,
            "owner_dms_unlimited": True,
            "bili_patrols_unlimited": True,
        },
    )
    assert saved.status_code == 200
    body = saved.json()
    assert body["source"] == "admin"
    assert body["config"]["enabled"] is False
    assert body["config"]["group_cooldown_seconds"] == 1800
    assert body["config"]["max_proactive_per_day"] == 3
    assert body["config"]["owner_dms_unlimited"] and body["config"]["bili_patrols_unlimited"]
    assert "active_hours" not in body["config"]

    reset = await client.delete("/admin/api/life")
    assert reset.status_code == 200
    assert reset.json()["source"] == "env"
    assert reset.json()["config"] == original


async def test_life_world_attention_lock_toggle_and_validation(life_api):
    app, client, _ = life_api
    svc = app.state.service

    for bad in (
        {},
        {"focus_level": "ULTRA"},
        {"energy": 101},
        {"energy": True},
        {"boredom": -1},
        {"current_module": "   "},
        {"mood": ""},
        {"unexpected": 1},
    ):
        assert (await client.put("/admin/api/life/world", json=bad)).status_code == 400

    denied = await client.put(
        "/admin/api/life/world", json={"current_module": "minecraft", "focus_level": "HIGH"}
    )
    assert denied.status_code == 400 and denied.json()["detail"] == "mc_task_required"
    await svc.world_state.experience.mc_snapshot("explore", svc.world_state.clock())
    # A real task owns high focus. Admin may still adjust other state during it.
    locked = await client.put(
        "/admin/api/life/world",
        json={
            "current_module": "minecraft",
            "focus_level": "HIGH",
            "mood": "focused",
            "energy": 75.4,
            "boredom": 10,
        },
    )
    assert locked.status_code == 200
    ws = locked.json()["world_state"]
    assert ws["current_module"] == "minecraft"
    assert ws["focus_level"] == "HIGH"
    assert ws["mood"] == "focused"
    assert ws["energy"] == 75.4
    assert ws["boredom"] == 10
    assert ws["proactive_suspended"] is True
    mods = {m["module_id"]: m for m in ws["modules"]}
    assert mods["qq_social"]["proactive_paused"] is True
    assert mods["bilibili_surf"]["proactive_paused"] is True
    assert mods["minecraft"]["proactive_paused"] is False
    assert svc.world_state.proactive_suspended() is True

    # Release back to LOW via partial update -> modules resume while preserving other fields.
    unlocked = await client.put("/admin/api/life/world", json={"focus_level": "LOW"})
    assert unlocked.status_code == 200
    ws2 = unlocked.json()["world_state"]
    assert ws2["focus_level"] == "LOW"
    assert ws2["current_module"] == "minecraft"
    assert ws2["energy"] == 75.4
    assert ws2["proactive_suspended"] is False
    mods2 = {m["module_id"]: m for m in ws2["modules"]}
    assert mods2["qq_social"]["proactive_paused"] is False
    assert mods2["bilibili_surf"]["proactive_paused"] is False


async def test_life_manual_plan_and_gatekeeper_actions_and_lock(life_api):
    app, client, _ = life_api
    fake_engine = FakeLifeEngine()
    app.state.service.life_engine = fake_engine

    plan_res = await client.post("/admin/api/life/plan")
    assert plan_res.status_code == 200
    assert plan_res.json() == {"status": "planned", "calls": 1}

    gk_res = await client.post("/admin/api/life/gatekeeper")
    assert gk_res.status_code == 200
    assert gk_res.json() == {"status": "scanned", "calls": 1}

    # Concurrent lock held -> 409 admin_action_busy.
    async with app.state.admin_action_lock:
        busy_plan = await client.post("/admin/api/life/plan")
        busy_gk = await client.post("/admin/api/life/gatekeeper")
    assert busy_plan.status_code == 409
    assert busy_plan.json()["detail"] == "admin_action_busy"
    assert busy_gk.status_code == 409
    assert busy_gk.json()["detail"] == "admin_action_busy"


async def test_life_cancel_intent_validation_404_and_state_transition(life_api):
    app, client, _ = life_api
    svc = app.state.service

    # Invalid intent_id format -> 400
    for bad_id in ("short", "not-hex-0123456789", "ABCDEF0123456789"):
        assert (await client.delete(f"/admin/api/life/intents/{bad_id}")).status_code == 400

    # Non-existent valid hex id -> 404
    missing = await client.delete("/admin/api/life/intents/0123456789abcdef0123456789abcdef")
    assert missing.status_code == 404

    now = time.time()
    intent_id = await svc.world_state.enqueue_intent(
        IntentTicketCreate(
            target_module="qq_social",
            target_scope="onebot:40000",
            action_type="group_share",
            execute_after=now + 120,
            expire_at=now + 1800,
            mood="playful",
            gate_condition={"min_recent": 2},
            intent_note="聊聊刚刷到的视频",
        ),
        now=now,
    )

    cancelled = await client.delete(f"/admin/api/life/intents/{intent_id}")
    assert cancelled.status_code == 200
    row = next(i for i in cancelled.json()["intents"] if i["id"] == intent_id)
    assert row["status"] == "gated_skip"

    # Cancelling again (no longer pending) -> 409
    again = await client.delete(f"/admin/api/life/intents/{intent_id}")
    assert again.status_code == 409


async def test_life_status_reports_proactive_sent_today_and_historical_last_proactive(life_api):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    app, client, _ = life_api
    svc = app.state.service
    assert svc.life_engine is not None
    await svc.life_engine.initialize()

    sent_ts = (
        datetime.fromtimestamp(time.time(), ZoneInfo(svc.s.bot_timezone))
        .replace(hour=12, minute=0, second=0, microsecond=0)
        .timestamp()
    )
    async with svc.store.transaction():
        await svc.store.db.execute(
            "INSERT INTO pending_owner_dms("
            "id,scope,day,kind,created,expires_at,status,sent_at"
            ") VALUES ('dm-1', ?, ?, 'companion', ?, ?, 'sent', ?)",
            (
                "20000",
                svc.proactive.local(sent_ts).date().isoformat(),
                sent_ts,
                sent_ts + 3600.0,
                sent_ts,
            ),
        )
        await svc.store.db.execute(
            "INSERT INTO scheduled_intents("
            "id,created_at,execute_after,expire_at,target_module,target_scope,"
            "action_type,mood,gate_condition_json,intent_note_json,status,updated_at"
            ") VALUES ('0123456789abcdef', ?, ?, ?, 'qq_social', 'onebot:40000', "
            "'group_share', 'happy', '{}', '{}', 'done', ?)",
            (sent_ts - 60.0, sent_ts - 30.0, sent_ts + 600.0, sent_ts + 30.0),
        )

    res = await client.get("/admin/api/life")
    assert res.status_code == 200
    body = res.json()
    assert body["engine"]["proactive_sent_today"] == 1
    assert body["world_state"]["last_proactive_at"] == pytest.approx(sent_ts + 30.0, abs=1.0)


async def test_catalog_action_hot_switch_is_persisted_and_restricts_admin(life_api):
    app, client, gateway = life_api
    path = "/admin/api/life/actions/"
    info = (await client.get("/admin/api/life")).json()
    names = {r["name"] for r in info["engine"]["actions"]}
    assert {"mc.status", "mc.build_plan", "qq.speak", "bili.patrol"} <= names
    assert next(r for r in info["engine"]["actions"] if r["name"] == "weather.read")["reason"] == (
        "module_disabled"
    )
    for name, mode in (
        ("not.registered", "active"),
        ("mc.status", "legacy"),
        ("qq.speak", "invalid"),
    ):
        assert (await client.put(path + name, json={"mode": mode})).status_code == 400
    response = await client.put(path + "mc.status", json={"mode": "off"})
    assert response.status_code == 200 and response.json()["config"]["action_modes"] == {
        "mc.status": "off"
    }
    blocked = await client.post("/admin/api/life/mc/goal", json={"action_type": "status"})
    assert blocked.status_code == 400 and blocked.json()["detail"] == "action_disabled"
    assert (await client.get("/admin/api/life")).json()["config"]["action_modes"] == {
        "mc.status": "off"
    }
    assert not gateway.calls


async def test_module_master_switch_blocks_ingress_actions_and_preserves_settings(
    life_api, message
):
    app, client, gateway = life_api
    svc = app.state.service
    root = "/admin/api/life/modules"
    qq = root + "/qq_social"
    assert (await client.put(qq + "/enabled", json={"enabled": "false"})).status_code == 400
    assert (await client.put(root + "/absent/enabled", json={"enabled": False})).status_code == 404
    await client.put("/admin/api/life/actions/qq.speak", json={"mode": "off"})
    before = (await client.get(qq)).json()
    now = time.time()
    pending = await svc.world_state.enqueue_intent(
        IntentTicketCreate(
            target_module="qq_social",
            target_scope="onebot:40000",
            action_type="group_share",
            execute_after=now + 10,
            expire_at=now + 900,
        )
    )
    for _ in range(2):
        response = await client.put(qq + "/enabled", json={"enabled": False})
        assert response.status_code == 200
        assert not response.json()["enabled"]
        assert all(a["reason"] == "module_disabled" for a in response.json()["actions"])
    assert (await svc.store.one("SELECT status FROM scheduled_intents WHERE id=?", (pending,)))[
        0
    ] == "gated_skip"
    assert await svc.accept(message) == "ignored"
    assert not svc.batch_authorized(message, [message])
    assert not svc.proactive.delivery_enabled
    with pytest.raises(Rejected, match="module_disabled"):
        await svc.sender.onebot.call("send_private_msg", {})
    assert "qq_social" not in {m.module_id for m in svc.modules.active()}
    assert (await client.get(root + "/bilibili_surf")).json()["enabled"]
    assert (await client.put(root + "/bilibili_surf/enabled", json={"enabled": False})).is_success
    with pytest.raises(Rejected, match="module_disabled"):
        await svc.bili.resolve_short("https://b23.tv/hello")
    with pytest.raises(Rejected, match="module_disabled"):
        await svc.bili_watcher.run_once()
    assert (await client.post("/admin/api/actions/patrol")).status_code == 409
    assert (await client.put(root + "/minecraft/enabled", json={"enabled": False})).is_success
    assert await svc.node_transport.on_event({"event": "mc.milestone", "payload": {}}) == "ignored"
    blocked = await client.post("/admin/api/life/mc/goal", json={"action_type": "status"})
    assert blocked.json()["detail"] == "module_disabled"
    assert not {"qq_social", "minecraft", "bilibili_surf"} & {
        m.module_id for m in svc.modules.active()
    }
    assert {row["id"] for row in (await client.get(root)).json()} == {
        m.module_id for m in svc.modules.all()
    }
    after = (await client.put(qq + "/enabled", json={"enabled": True})).json()
    assert after["enabled"] and after["scheduling"]["values"] == before["scheduling"]["values"]
    assert next(a for a in after["actions"] if a["name"] == "qq.speak")["mode"] == "off"
    assert svc.batch_authorized(message, [message])
    assert not gateway.calls


async def test_switch_restores_after_restart_and_weather_waits_for_location(settings):
    settings.admin_token = SecretStr(TOKEN)
    root = "/admin/api/life/modules"
    for first_start in (True, False):
        app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": "Bearer " + TOKEN},
            ) as client,
        ):
            if first_start:
                assert (
                    await client.put(root + "/weather/enabled", json={"enabled": True})
                ).is_success
                assert (
                    await client.put(root + "/qq_social/enabled", json={"enabled": False})
                ).is_success
            rows = {r["id"]: r for r in (await client.get(root)).json()}
            assert rows["weather"]["enabled"] and not rows["qq_social"]["enabled"]
            assert rows["weather"]["configuration_issue"] == "weather_location_missing"
            detail = (await client.get(root + "/weather")).json()
            assert all(t["next_at"] is None for t in detail["scheduling"]["timers"])
            assert all(a["reason"] == "weather_location_missing" for a in detail["actions"])
            assert (await client.get(root + "/weather/sensor")).status_code == 409
            assert not app.state.service.ai.calls
