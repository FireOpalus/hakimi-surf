"""Tests for centralized Pydantic v2 policy schema registry and converged routes."""

import httpx
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.policy_registry import (
    POLICY_REGISTRY,
    SIMPLE_LIMIT_SPECS,
    export_policy_schemas,
    validate_registry_against_settings,
)
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_admin import admin_api as admin_api


def test_policy_registry_exports_valid_schemas_and_defaults(settings):
    validated = validate_registry_against_settings(settings)
    assert set(validated) == set(POLICY_REGISTRY)

    exported = export_policy_schemas(settings)
    assert exported["version"] == 1
    policies = exported["policies"]
    assert len(policies) == 13
    assert set(policies) == set(POLICY_REGISTRY)

    routes = [entry["route"] for entry in policies.values()]
    assert len(routes) == len(set(routes))
    for key, entry in policies.items():
        assert entry["key"] == key
        assert entry["route"].startswith("/admin/api/")
        assert isinstance(entry["schema"], dict)
        assert isinstance(entry["schema"].get("properties"), dict)
        assert entry["schema"]["properties"]
        assert isinstance(entry["defaults"], dict)

    assert [spec.key for spec in SIMPLE_LIMIT_SPECS] == [
        "rate_limits",
        "generation_caps",
        "patrol_limits",
        "image_limits",
        "content_tools",
    ]


async def test_policy_schema_endpoint_requires_auth(settings):
    app = create_app(settings, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            resp = await client.get("/admin/api/policies/schema")
    assert resp.status_code == 403

    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    app_auth = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app_auth.router.lifespan_context(app_auth):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_auth), base_url="http://127.0.0.1:12141"
        ) as client:
            bad = await client.get(
                "/admin/api/policies/schema",
                headers={"Authorization": "Bearer wrong"},
            )
            ok = await client.get(
                "/admin/api/policies/schema",
                headers={"Authorization": f"Bearer {TOKEN}"},
            )
    assert bad.status_code == 401
    assert ok.status_code == 200
    body = ok.json()
    assert body["version"] == 1
    assert set(body["policies"]) == set(POLICY_REGISTRY)


async def test_converged_simple_limit_routes_roundtrip(admin_api):
    _app, client, _gateway = admin_api

    rate_get = await client.get("/admin/api/rate-limits")
    assert rate_get.status_code == 200
    assert rate_get.json()["source"] == "env"

    caps_get = await client.get("/admin/api/generation-caps")
    assert caps_get.status_code == 200
    orig_caps = caps_get.json()["config"]

    caps_put = await client.put(
        "/admin/api/generation-caps",
        json={"chat_output_tokens": 1024},
    )
    assert caps_put.status_code == 200
    caps_data = caps_put.json()
    assert caps_data["source"] == "admin"
    assert caps_data["config"]["chat_output_tokens"] == 1024
    assert caps_data["config"]["owner_output_tokens"] == orig_caps["owner_output_tokens"]

    caps_del = await client.delete("/admin/api/generation-caps")
    assert caps_del.status_code == 200
    assert caps_del.json()["source"] == "env"

    patrol_put = await client.put(
        "/admin/api/patrol-limits",
        json={"max_subscriptions": 12, "patrol_interval_seconds": 1200},
    )
    assert patrol_put.status_code == 200
    assert patrol_put.json()["config"] == {
        "max_subscriptions": 12,
        "patrol_interval_seconds": 1200,
    }
    patrol_del = await client.delete("/admin/api/patrol-limits")
    assert patrol_del.status_code == 200
    assert patrol_del.json()["source"] == "env"
