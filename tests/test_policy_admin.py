"""Real admin middleware and persistent filter/budget controls, no external calls."""

from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.gateway import FilterManager
from hakimi.security import Rejected
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_admin import admin_api as admin_api

ROUTES = [
    ("GET", "/admin/api/budget"),
    ("PUT", "/admin/api/budget"),
    ("DELETE", "/admin/api/budget"),
    ("GET", "/admin/api/filter"),
    ("PUT", "/admin/api/filter"),
    ("DELETE", "/admin/api/filter"),
    ("POST", "/admin/api/filter/group-activity"),
    ("GET", "/admin/api/decision-audit"),
]


@pytest.mark.parametrize("method,path", ROUTES)
async def test_policy_admin_disabled_without_token(settings, method, path):
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            response = await client.request(method, path, json={})
    assert response.status_code == 403
    assert not gateway.calls


@pytest.mark.parametrize("method,path", ROUTES)
async def test_policy_admin_auth_precedes_validation_or_changes(admin_api, method, path):
    app, client, gateway = admin_api
    response = await client.request(
        method, path, headers={"Authorization": "Bearer wrong"}, json={"unknown": True}
    )
    assert response.status_code == 401
    assert not await app.state.service.store.all("SELECT * FROM admin_config")
    assert not gateway.calls


async def test_filter_hot_persistence_preserves_group_preset_when_saving_global_thresholds(
    admin_api,
):
    app, client, gateway = admin_api
    store = app.state.service.store
    initial = (await client.get("/admin/api/filter")).json()
    assert initial["config"]["injection_threshold"] == 0.78
    assert initial["config"]["unsafe_threshold"] == 0.80
    assert initial["config"]["reply_threshold"] == 0.42
    assert initial["source_thresholds"] == {"injection": 0.20, "unsafe": 0.50}
    changed = await client.post(
        "/admin/api/filter/group-activity",
        json={"platform": "onebot", "group_id": "40000", "preset": "quiet"},
    )
    assert changed.status_code == 200
    assert store.runtime.filters.config.group_activity == {"onebot:40000": "quiet"}
    config = dict(initial["config"])
    config.pop("group_activity")
    config["injection_threshold"] = 0.81
    response = await client.put("/admin/api/filter", json=config)
    assert response.status_code == 200
    assert response.json()["config"]["group_activity"] == {"onebot:40000": "quiet"}
    assert store.runtime.filters.config.injection_threshold == 0.81
    restored = FilterManager(store)
    await restored.initialize()
    assert restored.config == store.runtime.filters.config
    encrypted = (await store.one("SELECT payload FROM admin_config WHERE key='filters'"))[0]
    assert b'"injection_threshold"' not in encrypted and b"40000" not in encrypted
    assert app.state.settings.injection_threshold == 0.20
    assert app.state.settings.unsafe_threshold == 0.50
    assert (await client.delete("/admin/api/filter")).json()["config"] == initial["config"]
    assert not gateway.calls


@pytest.mark.parametrize(
    "path,payload",
    [
        ("filter", {"injection_threshold": "0.78"}),
        ("filter", {"unsafe_threshold": 1}),
        ("filter", {"reply_threshold": 0}),
        ("filter", {"owner_injection_threshold": True}),
        ("filter", {"owner_unsafe_threshold": -1}),
        ("filter", {"unexpected": "sensitive-input"}),
        ("filter", {"group_activity": {"../private": "active"}}),
        ("budget", {"daily_usd": -1}),
        ("budget", {"monthly_usd": True}),
        ("budget", {"daily_usd": "NaN"}),
        ("budget", {"monthly_usd": "Infinity"}),
        ("budget", {"monthly_usd": "10001"}),
    ],
)
async def test_policy_inputs_are_bounded_and_do_not_echo_values(admin_api, path, payload):
    app, client, gateway = admin_api
    response = await client.put("/admin/api/" + path, json=payload)
    assert response.status_code == 400
    assert "sensitive-input" not in response.text
    assert not await app.state.service.store.all("SELECT * FROM admin_config")
    assert not gateway.calls


async def test_group_activity_does_not_expand_admission_or_change_groups_shape(admin_api):
    app, client, gateway = admin_api
    original = (await client.get("/admin/api/groups")).json()
    response = await client.post(
        "/admin/api/filter/group-activity", json={"group_id": "77777", "preset": "active"}
    )
    assert response.status_code == 409
    response = await client.put(
        "/admin/api/filter", json={"group_activity": {"onebot:77777": "active"}}
    )
    assert response.status_code == 409
    response = await client.post(
        "/admin/api/filter/group-activity", json={"group_id": "40000", "preset": "active"}
    )
    assert response.status_code == 200
    assert (await client.get("/admin/api/groups")).json() == original
    assert (
        await client.post(
            "/admin/api/filter/group-activity", json={"group_id": "40000", "preset": None}
        )
    ).status_code == 200
    assert not app.state.service.store.runtime.filters.config.group_activity
    assert not gateway.calls


@pytest.mark.parametrize("zero", ["daily_usd", "monthly_usd"])
async def test_budget_zero_hot_pauses_paid_work_without_disabling_admin(admin_api, zero):
    app, client, gateway = admin_api
    config = {"daily_usd": "2", "monthly_usd": "40", zero: "0"}
    response = await client.put("/admin/api/budget", json=config)
    assert response.status_code == 200
    store = app.state.service.store
    assert getattr(app.state.settings, zero) == 0
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("unit-subject", "chat", Decimal("0.000001"))
    assert (await client.get("/admin/api/dashboard")).json()["budget_status"]["hard_stop"]
    assert (await client.get("/admin/api/decision-audit")).status_code == 200
    reset = await client.delete("/admin/api/budget")
    assert reset.json()["config"] == {"daily_usd": "2", "monthly_usd": "40"}
    assert not (await store.budget_status())["hard_stop"]
    assert not gateway.calls


async def test_budget_survives_real_store_restart_and_env_does_not_override(settings):
    settings.admin_token = SecretStr(TOKEN)
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            response = await client.put(
                "/admin/api/budget", json={"daily_usd": "3.25", "monthly_usd": "18.50"}
            )
            assert response.status_code == 200
            encrypted = (
                await app.state.service.store.one(
                    "SELECT payload FROM admin_config WHERE key='budget'"
                )
            )[0]
            assert b'"daily_usd"' not in encrypted
    settings.daily_usd, settings.monthly_usd = Decimal("9"), Decimal("99")
    restarted = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with restarted.router.lifespan_context(restarted):
        state = await restarted.state.service.store.budget_config_status()
        assert state["config"] == {"daily_usd": "3.25", "monthly_usd": "18.50"}
        assert state["source"] == "admin"
        assert settings.total_usd == Decimal("40")
    assert not gateway.calls


async def test_decision_audit_endpoint_returns_latest30_redacted_encrypted_rows(admin_api, message):
    app, client, gateway = admin_api
    manager = app.state.service.store.runtime.filters
    secret = app.state.settings.gemini_api_key.get_secret_value()
    for i in range(35):
        await manager.record(
            message,
            {
                "current_message": f"消息 {i} token={secret} test@example.com 123456789",
                "private_history": "DO-NOT-RETAIN-OWNER-MEMORY",
            },
            {
                "answers": {
                    "is_prompt_injection": {"noul": 0.12, "confidence": 0.98},
                    "is_unsafe_or_spam": {"noul": 0.05, "confidence": 0.97},
                    "should_reply": {"noul": 0.71, "confidence": 0.95},
                },
                "result": "chat",
                "reason": "allowed",
                "reply_bypassed": True,
            },
            "production",
        )
    response = await client.get("/admin/api/decision-audit")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 30 and "消息 34" in data[0]["excerpt"]
    assert data[0]["scores"]["is_prompt_injection"] == {"noul": 0.12, "confidence": 0.98}
    assert data[0]["surface"] == "private" and data[0]["reply_bypassed"]
    for forbidden in (secret, "test@example.com", "123456789", "DO-NOT-RETAIN-OWNER-MEMORY"):
        assert forbidden not in response.text
    rows = await app.state.service.store.all("SELECT payload FROM decision_audit")
    assert len(rows) == 30 and all(b"excerpt" not in row[0] for row in rows)
    assert not gateway.calls


def test_message_thresholds_are_independent_of_strict_sources_and_prices(settings):
    assert settings.message_injection_threshold == 0.78
    assert settings.message_unsafe_threshold == 0.80
    assert settings.message_reply_threshold == 0.42
    assert settings.message_owner_injection_threshold == 0.90
    assert settings.injection_threshold == 0.20 and settings.unsafe_threshold == 0.50
    assert settings.monthly_usd == Decimal("40")
    assert settings.chat_cached_input_usd_per_m == Decimal("0.075")
    data = settings.model_dump()
    data["chat_cached_input_usd_per_m"] = Decimal("2.00")
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **data)
