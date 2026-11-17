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
    ("PUT", "/admin/api/life"),
    ("DELETE", "/admin/api/life"),
    ("PUT", "/admin/api/life/world"),
    ("POST", "/admin/api/life/plan"),
    ("POST", "/admin/api/life/gatekeeper"),
    ("DELETE", "/admin/api/life/intents/0123456789abcdef0123456789abcdef"),
]


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


async def test_admin_page_life_tab_csp_sha256_and_zero_inline_attributes(life_api):
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
        'id="life-active-hours"',
        'id="life-group-cooldown"',
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
    fake_engine = FakeLifeEngine()
    app.state.service.life_engine = fake_engine

    for bad in (
        {},
        {"enabled": "true"},
        {"max_proactive_per_day": True},
        {"max_proactive_per_day": 31},
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
        },
    )
    assert saved.status_code == 200
    body = saved.json()
    assert body["source"] == "sqlite"
    assert body["config"]["enabled"] is False
    assert body["config"]["group_cooldown_seconds"] == 1800
    assert body["config"]["max_proactive_per_day"] == 3
    assert body["config"]["active_hours"] == "09:00-01:00"

    reset = await client.delete("/admin/api/life")
    assert reset.status_code == 200
    assert reset.json()["source"] == "env"
    assert reset.json()["config"] == LifeConfigMirror().model_dump(mode="json")


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

    # Lock focus to HIGH -> qq_social & bilibili_surf pause; minecraft stays unpaused.
    locked = await client.put(
        "/admin/api/life/world",
        json={
            "current_module": "minecraft",
            "focus_level": "HIGH",
            "mood": "focused",
            "energy": 75,
            "boredom": 10,
        },
    )
    assert locked.status_code == 200
    ws = locked.json()["world_state"]
    assert ws["current_module"] == "minecraft"
    assert ws["focus_level"] == "HIGH"
    assert ws["mood"] == "focused"
    assert ws["energy"] == 75
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
    assert ws2["energy"] == 75
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
