"""Hot thinking policy, authenticated persistence and real google-genai wire contracts."""

import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario, generation_plan
from hakimi.thinking_policy import SETTING_FIELDS, ThinkingConfig
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_admin import admin_api as admin_api
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


@pytest.mark.parametrize("scenario", list(Scenario))
def test_every_business_scenario_defaults_to_medium_without_changing_caps(settings, scenario):
    assert set(ThinkingConfig.model_fields) == set(Scenario)
    baseline = generation_plan(settings, scenario)
    assert baseline.thinking_level == "medium"
    for level in ("low", "medium", "high", "xhigh"):
        settings._thinking_override = ThinkingConfig(**{str(scene): level for scene in Scenario})
        assert generation_plan(settings, scenario).thinking_level == level
        if scenario in {Scenario.FAST_CHAT, Scenario.OWNER_PRIVATE}:
            expected = (
                settings.chat_output_tokens
                if scenario == Scenario.FAST_CHAT
                else settings.owner_output_tokens
            )
            assert generation_plan(settings, scenario).output_tokens == expected
        else:
            assert generation_plan(settings, scenario).output_tokens == baseline.output_tokens


@pytest.mark.parametrize("value", ["minimal", "ultra", "xHigh", "", 1, True, None, []])
def test_thinking_policy_rejects_unknown_or_coerced_levels(value):
    with pytest.raises(ValidationError):
        ThinkingConfig(fast_chat=value)


@pytest.mark.parametrize("scenario", list(Scenario))
def test_all_scene_environment_names_are_supported(settings, monkeypatch, scenario):
    monkeypatch.setenv(f"HAKIMI_{scenario.upper()}_THINKING_LEVEL", "xhigh")
    configured = Settings(
        _env_file=None,
        gemini_api_key=settings.gemini_api_key,
        encryption_key=settings.encryption_key,
    )
    assert getattr(configured, SETTING_FIELDS[scenario]) == "xhigh"
    assert generation_plan(configured, scenario).thinking_level == "xhigh"


@pytest.mark.parametrize(
    "legacy,scenario",
    [
        ("OWNER", Scenario.OWNER_PRIVATE),
        ("SUMMARY", Scenario.BILIBILI_SUMMARY),
        ("DIARY", Scenario.DAILY_DIARY),
    ],
)
def test_legacy_environment_names_remain_supported(settings, monkeypatch, legacy, scenario):
    monkeypatch.setenv(f"HAKIMI_{legacy}_THINKING_LEVEL", "low")
    configured = Settings(
        _env_file=None,
        gemini_api_key=settings.gemini_api_key,
        encryption_key=settings.encryption_key,
    )
    assert generation_plan(configured, scenario).thinking_level == "low"


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_thinking_admin_requires_token_before_validation(admin_api, method):
    app, client, gateway = admin_api
    response = await client.request(
        method, "/admin/api/thinking", headers={"Authorization": "Bearer invalid"}, json={}
    )
    assert response.status_code == 401
    assert not await app.state.service.store.all("SELECT * FROM admin_config")
    assert not gateway.calls


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_thinking_admin_disabled_with_empty_token(settings, method):
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            assert (await client.request(method, "/admin/api/thinking", json={})).status_code == 403


async def test_admin_thinking_hot_save_and_reset_are_local_only(admin_api):
    app, client, gateway = admin_api
    initial = (await client.get("/admin/api/thinking")).json()
    assert initial["config"] == ThinkingConfig().model_dump()
    assert initial["source"] == "env" and initial["environment"] == initial["config"]
    assert initial["levels"] == ["low", "medium", "high", "xhigh"]
    changed = dict(initial["config"], fast_chat="xhigh", owner_companion="high")
    response = await client.put("/admin/api/thinking", json=changed)
    assert response.status_code == 200
    assert response.json()["source"] == "sqlite" and response.json()["config"] == changed
    assert generation_plan(app.state.settings, Scenario.FAST_CHAT).thinking_level == "xhigh"
    encrypted = (
        await app.state.service.store.one("SELECT payload FROM admin_config WHERE key='thinking'")
    )[0]
    assert b"fast_chat" not in encrypted and b"xhigh" not in encrypted
    assert not gateway.calls
    response = await client.delete("/admin/api/thinking")
    assert response.json() == initial
    assert generation_plan(app.state.settings, Scenario.FAST_CHAT).thinking_level == "medium"
    assert not gateway.calls


@pytest.mark.parametrize(
    "payload",
    [
        {"fast_chat": "minimal"},
        {"owner_private": "ultra"},
        {"daily_diary": True},
        {"memory_compression": 3},
        {"owner_digest": None},
        {"unexpected": "secret-input"},
        {"config": {"fast_chat": "xhigh"}},
    ],
)
async def test_thinking_invalid_admin_payload_cannot_change_policy(admin_api, payload):
    app, client, gateway = admin_api
    response = await client.put("/admin/api/thinking", json=payload)
    assert response.status_code == 400 and "secret-input" not in response.text
    assert not await app.state.service.store.all("SELECT * FROM admin_config")
    assert generation_plan(app.state.settings, Scenario.FAST_CHAT).thinking_level == "medium"
    assert not gateway.calls


async def test_sqlite_policy_survives_full_restart_and_reset_uses_environment(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_thinking_level = "high"
    config = {str(scene): "xhigh" for scene in Scenario}
    for index in range(2):
        fresh = Settings(_env_file=None, **settings.model_dump())
        app = create_app(fresh, gateway_factory=FakeGateway, start_workers=False)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": "Bearer " + TOKEN},
            ) as client:
                if index == 0:
                    assert (await client.put("/admin/api/thinking", json=config)).status_code == 200
                else:
                    result = (await client.get("/admin/api/thinking")).json()
                    assert result["config"] == config and result["source"] == "sqlite"
                    assert all(
                        generation_plan(fresh, s).thinking_level == "xhigh" for s in Scenario
                    )
                    result = (await client.delete("/admin/api/thinking")).json()
                    assert result["config"]["owner_private"] == "high"
                    assert result["source"] == "env"
                    assert generation_plan(fresh, Scenario.OWNER_PRIVATE).thinking_level == "high"


@pytest.mark.parametrize("scenario", list(Scenario))
async def test_real_sdk_transmits_default_medium_and_hot_xhigh_without_reserve_inflation(
    settings, store, message, sdk_pair, scenario
):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json=(
                decision_response()
                if request.url.path.endswith("decisions")
                else interaction_response()
            ),
        )

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    for level in ("medium", "xhigh"):
        await store.runtime.set_thinking(ThinkingConfig(**{str(scenario): level}))
        await gateway.generate(
            {"current_message": "测试", "thinking_level": "low"},
            "sdk-thinking",
            scenario=scenario,
            message=message,
        )
    interactions = [request for request in requests if request.url.path == "/v1/interactions"]
    assert len(interactions) == 2
    expected_store = scenario in {Scenario.FAST_CHAT, Scenario.OWNER_PRIVATE}
    for request, level in zip(interactions, ("medium", "xhigh"), strict=True):
        data = json.loads(request.content)
        assert data["model"] == "gemini-4-hydrogen" and data["store"] is expected_store
        assert data["generation_config"]["thinking_level"] == level
        assert (
            data["generation_config"]["max_output_tokens"]
            == generation_plan(settings, scenario).output_tokens
        )
    assert len([r for r in requests if r.url.path == "/v1beta/decisions"]) == 2
    reservations = await store.all("SELECT reserved FROM ledger ORDER BY rowid")
    assert reservations[0][0] == reservations[2][0]


@pytest.mark.parametrize(
    "level,expected",
    [
        ("low", "high"),
        ("medium", "high"),
        ("high", "high"),
        ("xhigh", "xhigh"),
    ],
)
async def test_explicit_deep_does_not_downgrade_xhigh(
    settings, store, sdk_pair, message, level, expected
):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json=(
                decision_response()
                if request.url.path.endswith("decisions")
                else interaction_response()
            ),
        )

    await store.runtime.set_thinking(ThinkingConfig(owner_private=level))
    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    await gateway.generate({}, "deep", scenario=Scenario.OWNER_PRIVATE, message=message, deep=True)
    config = json.loads(requests[0].content)["generation_config"]
    assert config["thinking_level"] == expected
    assert config["max_output_tokens"] == settings.high_output_tokens
