"""Real admin middleware and persistent filter/budget controls, no external calls."""

import time
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.gateway import FilterManager
from hakimi.generation_policy import Scenario, generation_plan
from hakimi.security import Rejected
from hakimi.store import Store
from hakimi.thinking_policy import ThinkingConfig
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_admin import admin_api as admin_api

ROUTES = [
    ("GET", "/admin/api/budget"),
    ("PUT", "/admin/api/budget"),
    ("DELETE", "/admin/api/budget"),
    ("GET", "/admin/api/rate-limits"),
    ("PUT", "/admin/api/rate-limits"),
    ("DELETE", "/admin/api/rate-limits"),
    ("GET", "/admin/api/generation-caps"),
    ("PUT", "/admin/api/generation-caps"),
    ("DELETE", "/admin/api/generation-caps"),
    ("GET", "/admin/api/patrol-limits"),
    ("PUT", "/admin/api/patrol-limits"),
    ("DELETE", "/admin/api/patrol-limits"),
    ("GET", "/admin/api/rest-hours"),
    ("PUT", "/admin/api/rest-hours"),
    ("DELETE", "/admin/api/rest-hours"),
    ("GET", "/admin/api/filter"),
    ("PUT", "/admin/api/filter"),
    ("DELETE", "/admin/api/filter"),
    ("POST", "/admin/api/filter/group-activity"),
    ("GET", "/admin/api/decision-audit"),
    ("GET", "/admin/api/bots/thresholds"),
    ("PUT", "/admin/api/bots/thresholds"),
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
        ("rest-hours", {"enabled": "yes"}),
        ("rest-hours", {"enabled": 1}),
        ("rest-hours", {"rest_hours": "23:00-23:00"}),
        ("rest-hours", {"rest_hours": "25:00-08:00"}),
        ("rest-hours", {"allow_owner_private": "no"}),
        ("rest-hours", {"unexpected": "sensitive-input"}),
        ("filter", {"injection_threshold": "0.78"}),
        ("filter", {"unsafe_threshold": 1}),
        ("filter", {"reply_threshold": 0}),
        ("filter", {"owner_injection_threshold": True}),
        ("filter", {"owner_unsafe_threshold": -1}),
        ("filter", {"unexpected": "sensitive-input"}),
        ("filter", {"group_activity": {"../private": "active"}}),
        ("budget", {}),
        ("budget", {"daily_usd": -1}),
        ("budget", {"monthly_usd": True}),
        ("budget", {"daily_usd": "NaN"}),
        ("budget", {"monthly_usd": "Infinity"}),
        ("budget", {"monthly_usd": "10001"}),
        ("budget", {"user_daily_usd": -1}),
        ("budget", {"patrol_daily_usd": True}),
        ("budget", {"memory_daily_usd": "101"}),
        ("budget", {"proactive_daily_usd": "NaN"}),
        ("budget", {"admin_daily_usd": "0.0000001"}),
        ("budget", {"unexpected": "sensitive-input"}),
        ("rate-limits", {"user_requests_per_minute": 0}),
        ("rate-limits", {"user_requests_per_minute": True}),
        ("rate-limits", {"global_requests_per_minute": 1001}),
        ("rate-limits", {"queue_capacity": "200"}),
        ("rate-limits", {"send_delay_min": 2.0, "send_delay_max": 1.0}),
        ("rate-limits", {"send_delay_min": "0.8"}),
        ("rate-limits", {"send_delay_max": True}),
        ("rate-limits", {"unexpected": "sensitive-input"}),
        ("generation-caps", {"carbon_enabled": "yes"}),
        ("generation-caps", {"carbon_enabled": 1}),
        ("generation-caps", {"chat_output_tokens": 127}),
        ("generation-caps", {"chat_output_tokens": True}),
        ("generation-caps", {"owner_output_tokens": 8193}),
        ("generation-caps", {"high_output_tokens": 16385}),
        ("generation-caps", {"unexpected": "sensitive-input"}),
        ("patrol-limits", {"max_subscriptions": 0}),
        ("patrol-limits", {"max_subscriptions": 51}),
        ("patrol-limits", {"max_subscriptions": True}),
        ("patrol-limits", {"patrol_interval_seconds": 899}),
        ("patrol-limits", {"patrol_interval_seconds": "1800"}),
        ("patrol-limits", {"unexpected": "sensitive-input"}),
        ("bots/thresholds", {"bot_reply_threshold": "0.85"}),
        ("bots/thresholds", {"bot_reply_threshold": True}),
        ("bots/thresholds", {"bot_reply_threshold": 0}),
        ("bots/thresholds", {"bot_reply_threshold": 1.01}),
        ("bots/thresholds", {"bot_reply_threshold": "NaN"}),
        ("bots/thresholds", {"bot_reply_threshold": 0.85, "unexpected": "sensitive-input"}),
        ("bots", {"user_id": "60000", "action": "threshold", "threshold": "0.5"}),
        ("bots", {"user_id": "60000", "action": "threshold", "threshold": True}),
        ("bots", {"user_id": "60000", "action": "threshold", "threshold": 0.0}),
        ("bots", {"user_id": "60000", "action": "threshold", "threshold": 1.5}),
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
    assert reset.json()["config"] == {
        "daily_usd": "2",
        "monthly_usd": "40",
        "user_daily_usd": "1",
        "patrol_daily_usd": "0.50",
        "memory_daily_usd": "0.20",
        "proactive_daily_usd": "0.10",
        "admin_daily_usd": "0.20",
    }
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
        assert state["config"] == {
            "daily_usd": "3.25",
            "monthly_usd": "18.50",
            "user_daily_usd": "1",
            "patrol_daily_usd": "0.50",
            "memory_daily_usd": "0.20",
            "proactive_daily_usd": "0.10",
            "admin_daily_usd": "0.20",
        }
        assert state["source"] == "admin"
        assert settings.total_usd == Decimal("40")
    assert not gateway.calls


async def test_sub_budget_hot_update_audit_detail_and_legacy_row(admin_api, settings):
    app, client, gateway = admin_api
    store = app.state.service.store
    await store.reserve("hot-user", "chat", Decimal("0.95"))
    with pytest.raises(Rejected, match="budget_exhausted") as exc_info:
        await store.reserve("hot-user", "chat", Decimal("0.08"))
    assert exc_info.value.audit_detail == "budget_exhausted_user_daily"

    status = await store.budget_status()
    assert status["user_top_daily"] == 950_000
    assert status["user_daily_limit"] == 1_000_000
    assert set(status["sub_budgets"]) == {"patrol", "memory", "proactive", "admin"}

    updated = await client.put(
        "/admin/api/budget",
        json={
            "daily_usd": "5",
            "monthly_usd": "40",
            "user_daily_usd": "2.50",
            "patrol_daily_usd": "1.20",
            "memory_daily_usd": "0.60",
            "proactive_daily_usd": "0.40",
            "admin_daily_usd": "0.80",
        },
    )
    assert updated.status_code == 200
    assert updated.json()["config"]["user_daily_usd"] == "2.50"
    reservation = await store.reserve("hot-user", "chat", Decimal("0.08"))
    assert reservation
    assert not gateway.calls

    # Simulate an existing DB with a legacy 2-key admin:budget row: remaining fields keep .env.
    legacy_settings = settings.model_copy(
        update={
            "data_dir": settings.data_dir.parent / "legacy-budget",
            "user_daily_usd": Decimal("2.50"),
            "proactive_daily_usd": Decimal("0.35"),
        }
    )
    legacy_store = Store(legacy_settings)
    await legacy_store.open()
    try:
        async with legacy_store.transaction():
            sealed = legacy_store.vault.seal(
                "admin:budget", {"daily_usd": "3.00", "monthly_usd": "39"}
            )
            await legacy_store.db.execute("INSERT INTO admin_config VALUES ('budget',?)", (sealed,))
    finally:
        await legacy_store.close()
    upgraded_store = Store(legacy_settings)
    await upgraded_store.open()
    try:
        loaded = await upgraded_store.budget_config_status()
        assert loaded["source"] == "admin"
        assert loaded["config"]["daily_usd"] == "3.00"
        assert loaded["config"]["monthly_usd"] == "39"
        assert loaded["config"]["user_daily_usd"] == "2.50"
        assert loaded["config"]["proactive_daily_usd"] == "0.35"
    finally:
        await upgraded_store.close()


async def test_rate_generation_and_patrol_limits_hot_persist_and_reset(settings, message):
    settings.admin_token = SecretStr(TOKEN)
    gateway = FakeGateway()
    owner_msg = message.model_copy(update={"user_id": "20000", "conversation_id": "20000"})
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        store = app.state.service.store
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            rl_put = await client.put(
                "/admin/api/rate-limits",
                json={
                    "user_requests_per_minute": 1,
                    "global_requests_per_minute": 5,
                    "queue_capacity": 50,
                    "send_delay_min": 0.2,
                    "send_delay_max": 0.5,
                },
            )
            assert rl_put.status_code == 200 and rl_put.json()["source"] == "admin"
            assert await store.admit(owner_msg) == "accepted"
            assert (
                await store.admit(owner_msg.model_copy(update={"event_id": "event-2"})) == "limited"
            )

            gc_put = await client.put(
                "/admin/api/generation-caps",
                json={
                    "carbon_enabled": False,
                    "chat_output_tokens": 512,
                    "owner_output_tokens": 2048,
                    "summary_output_tokens": 1024,
                    "diary_output_tokens": 1536,
                    "high_output_tokens": 4096,
                },
            )
            assert gc_put.status_code == 200 and settings.carbon_enabled is False
            settings._thinking_override = ThinkingConfig(fast_chat="medium")
            assert generation_plan(settings, Scenario.FAST_CHAT).output_tokens == 512
            deep_plan = generation_plan(settings, Scenario.OWNER_PRIVATE, deep=True)
            assert deep_plan.output_tokens == 4096

            await store.add_subscription(owner_msg, "10001")
            await store.poll_started((await store.subscriptions())[0]["id"], time.time() + 3600)
            pl_put = await client.put(
                "/admin/api/patrol-limits",
                json={"max_subscriptions": 1, "patrol_interval_seconds": 900},
            )
            assert pl_put.status_code == 200 and settings.max_subscriptions == 1
            sub_res = await client.post(
                "/admin/api/subscriptions", json={"kind": "up", "uid": "10002"}
            )
            assert sub_res.status_code == 409
            assert (await store.subscriptions())[0]["next_poll"] <= time.time() + 905

            for key, marker in (
                ("rate_limits", b"user_requests_per_minute"),
                ("generation_caps", b"chat_output_tokens"),
                ("patrol_limits", b"max_subscriptions"),
            ):
                blob = (await store.one("SELECT payload FROM admin_config WHERE key=?", (key,)))[0]
                assert marker not in blob

    changed_env = settings.model_copy(
        update={
            "user_requests_per_minute": 88,
            "carbon_enabled": True,
            "chat_output_tokens": 777,
            "max_subscriptions": 15,
            "patrol_interval_seconds": 7200,
        }
    )
    restarted = create_app(changed_env, gateway_factory=lambda *_: gateway, start_workers=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            assert (await client.get("/admin/api/rate-limits")).json()["source"] == "admin"
            assert changed_env.user_requests_per_minute == 1
            assert changed_env.chat_output_tokens == 512
            assert changed_env.max_subscriptions == 1

            rl_del = await client.delete("/admin/api/rate-limits")
            gc_del = await client.delete("/admin/api/generation-caps")
            pl_del = await client.delete("/admin/api/patrol-limits")
            assert rl_del.json()["source"] == "env" and changed_env.user_requests_per_minute == 88
            assert gc_del.json()["source"] == "env" and changed_env.chat_output_tokens == 777
            assert pl_del.json()["source"] == "env" and changed_env.max_subscriptions == 15
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


async def test_bot_thresholds_409_guards_encrypted_persistence_and_reset(settings):
    settings.admin_token = SecretStr(TOKEN)
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        store = app.state.service.store
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            unlisted = await client.put(
                "/admin/api/bots",
                json={"user_id": "69999", "action": "threshold", "threshold": 0.6},
            )
            assert unlisted.status_code == 409
            assert unlisted.json()["detail"] == "not_listed"

            for protected_id in (settings.owner_qq_id, settings.onebot_self_id):
                assert protected_id
                reserved = await client.put(
                    "/admin/api/bots",
                    json={"user_id": protected_id, "action": "threshold", "threshold": 0.6},
                )
                assert reserved.status_code == 409
                assert reserved.json()["detail"] == "reserved_identity"

            add_60000 = await client.put(
                "/admin/api/bots",
                json={"user_id": "60000", "action": "add", "threshold": 0.65},
            )
            add_60001 = await client.put(
                "/admin/api/bots",
                json={"user_id": "60001", "action": "add"},
            )
            assert add_60000.status_code == 200 and add_60001.status_code == 200

            save_default = await client.put(
                "/admin/api/bots/thresholds",
                json={"bot_reply_threshold": 0.92},
            )
            assert save_default.status_code == 200

            th_res = await client.get("/admin/api/bots/thresholds")
            assert th_res.status_code == 200
            th = th_res.json()
            assert th["bot_reply_threshold"] == pytest.approx(0.92)
            assert th["bot_reply_thresholds"] == {"60000": pytest.approx(0.65)}
            assert th["source"] == "admin"

            bots_res = await client.get("/admin/api/bots")
            assert bots_res.status_code == 200
            by_id = {row["user_id"]: row for row in bots_res.json()}
            assert by_id["60000"]["custom_threshold"] == pytest.approx(0.65)
            assert by_id["60000"]["effective_threshold"] == pytest.approx(0.65)
            assert by_id["60000"]["hard_blocked"] is False
            assert by_id["60001"]["custom_threshold"] is None
            assert by_id["60001"]["effective_threshold"] == pytest.approx(0.92)
            assert by_id["60001"]["hard_blocked"] is False

            blob = (await store.one("SELECT payload FROM admin_config WHERE key='admission'"))[0]
            assert b"bot_reply_threshold" not in blob and b"60000" not in blob

    changed_env = settings.model_copy(update={"bot_reply_threshold": 0.75})
    restarted = create_app(changed_env, gateway_factory=lambda *_: gateway, start_workers=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            assert changed_env.bot_reply_threshold == pytest.approx(0.92)
            assert changed_env.bot_reply_thresholds == {"60000": pytest.approx(0.65)}

            clear_60000 = await client.put(
                "/admin/api/bots",
                json={"user_id": "60000", "action": "threshold", "threshold": None},
            )
            assert clear_60000.status_code == 200
            by_id = {row["user_id"]: row for row in (await client.get("/admin/api/bots")).json()}
            assert by_id["60000"]["custom_threshold"] is None
            assert by_id["60000"]["effective_threshold"] == pytest.approx(0.92)

            block_60001 = await client.put(
                "/admin/api/bots",
                json={"user_id": "60001", "action": "threshold", "threshold": 1.0},
            )
            assert block_60001.status_code == 200
            by_id = {row["user_id"]: row for row in (await client.get("/admin/api/bots")).json()}
            assert by_id["60001"]["hard_blocked"] is True

            rm_60001 = await client.put(
                "/admin/api/bots",
                json={"user_id": "60001", "action": "remove"},
            )
            assert rm_60001.status_code == 200
            assert "60001" not in changed_env.bot_reply_thresholds

            reset = await client.delete("/admin/api/admission")
            assert reset.status_code == 200
            assert reset.json()["source"] == "env"
            assert changed_env.bot_reply_threshold == pytest.approx(0.75)
    assert not gateway.calls
