import json

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.admission_policy import AdmissionConfig
from hakimi.app import create_app
from hakimi.bot_server import prompt_context
from hakimi.security import authorize
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.fixture
async def admission_api(settings):
    settings.owner_qq_id = "20000"
    settings.onebot_private_mode = "owner_only"
    settings.admin_token = SecretStr("admission-test-password-never-live")
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer admission-test-password-never-live"},
        ) as client:
            yield app, client


@pytest.mark.parametrize(
    "ids", [["00123"], ["-1"], [123], ["123", "123"], ["１２３"], ["1e5"], ["0"], ["9" * 21]]
)
def test_strict_numeric_ids(ids):
    with pytest.raises(ValidationError):
        AdmissionConfig(allowed_groups=ids)


async def test_groups_add_remove_hot_policy_link_and_empty_is_closed(admission_api, message):
    app, client = admission_api
    s = app.state.settings
    target = message.model_copy(update={"kind": "group", "conversation_id": "77777"})
    assert not authorize(target, s)
    response = await client.post(
        "/admin/api/admission/change",
        json={"list": "allowed_groups", "action": "add", "id": "77777"},
    )
    assert response.status_code == 200
    assert authorize(target, s)
    groups = (await client.get("/admin/api/groups")).json()
    assert {"platform": "onebot", "group_id": "77777", "mode": "at_only"} in groups
    assert (
        await client.put("/admin/api/groups", json={"group_id": "77777", "mode": "smart_reply"})
    ).status_code == 200
    assert (
        await client.post(
            "/admin/api/admission/change",
            json={"list": "allowed_groups", "action": "add", "id": "77777"},
        )
    ).status_code == 409
    config = response.json()["config"] | {"allowed_groups": []}
    assert (await client.put("/admin/api/admission", json=config)).status_code == 200
    assert not authorize(target, s)
    assert not authorize(
        message.model_copy(update={"kind": "group", "conversation_id": "40000"}), s
    )
    assert not app.state.service.ai.calls


async def test_owner_only_legacy_admin_denied_and_friend_public_isolated(admission_api, message):
    app, client = admission_api
    svc = app.state.service
    friend = message.model_copy(
        update={"user_id": "30000", "conversation_id": "30000", "event_id": "friend"}
    )
    legacy = message.model_copy(
        update={"user_id": "80000", "conversation_id": "80000", "event_id": "legacy"}
    )
    svc.s.onebot_owner_ids.add("80000")
    assert authorize(message, svc.s)
    assert not authorize(friend, svc.s) and not authorize(legacy, svc.s)
    assert await svc.accept(friend) == "ignored"
    assert not svc.ai.calls
    config = (await client.get("/admin/api/admission")).json()["config"]
    config["private_mode"] = "owner_and_allowlist"
    assert (await client.put("/admin/api/admission", json=config)).status_code == 200
    assert authorize(friend, svc.s) and not authorize(legacy, svc.s)
    assert prompt_context(friend, svc.s).private_owner is False
    assert prompt_context(message, svc.s).private_owner is True
    svc.s.enable_notebook_snapshot = True
    svc.notebook.text = "owner-notebook-sentinel"
    assert svc.notebook.for_message(message) == "owner-notebook-sentinel"
    assert svc.notebook.for_message(friend) == ""
    await svc.sessions.remember(message, "owner-private-sentinel", "owner answer")
    await svc.sessions.remember(friend, "friend-only", "public answer")
    assert svc.sessions.path(message) != svc.sessions.path(friend)
    assert "owner-private-sentinel" not in json.dumps(await svc.sessions.snapshot(friend))
    await svc.accept(friend)
    await svc.process(await svc.store.claim())
    assert "owner-private-sentinel" not in json.dumps(svc.ai.calls, default=str)
    assert "owner-notebook-sentinel" not in json.dumps(svc.ai.calls, default=str)
    assert (
        await client.post(
            "/admin/api/admission/change",
            json={"list": "allowed_users", "action": "remove", "id": "30000"},
        )
    ).status_code == 200
    assert not authorize(friend, svc.s)


@pytest.mark.parametrize("field", ["allowed_users", "ignored_bot_ids"])
@pytest.mark.parametrize("uid", ["20000", "10000"])
async def test_protected_ids_cannot_be_added_or_removed(admission_api, field, uid):
    app, client = admission_api
    for action in ["add", "remove"]:
        r = await client.post(
            "/admin/api/admission/change", json={"list": field, "action": action, "id": uid}
        )
        assert r.status_code == 409
    config = (await client.get("/admin/api/admission")).json()["config"]
    config[field] = [uid]
    assert (await client.put("/admin/api/admission", json=config)).status_code == 409
    assert app.state.settings.owner_qq_id not in app.state.settings.bot_qq_ids


async def test_bot_blacklist_blocks_existing_admission_and_models(admission_api, message):
    app, client = admission_api
    svc = app.state.service
    config = (await client.get("/admin/api/admission")).json()["config"]
    config.update(private_mode="owner_and_allowlist", allowed_users=["30000"])
    await client.put("/admin/api/admission", json=config)
    friend = message.model_copy(
        update={"user_id": "30000", "conversation_id": "30000", "event_id": "queued"}
    )
    assert await svc.accept(friend) == "accepted"
    job = await svc.store.claim()
    assert (
        await client.put("/admin/api/bots", json={"user_id": "30000", "action": "add"})
    ).status_code == 200
    assert (await client.get("/admin/api/admission")).json()["config"]["ignored_bot_ids"] == [
        "30000"
    ]
    assert not authorize(friend, svc.s)
    await svc.process(job)
    assert not svc.ai.calls
    assert await svc.accept(friend.model_copy(update={"event_id": "new"})) == "ignored"
    group = friend.model_copy(
        update={"kind": "group", "conversation_id": "40000", "addressed": True}
    )
    assert not authorize(group, svc.s)


async def test_muted_users_deny_before_routing_but_owner_protected(settings, store, message):
    settings.muted_user_ids = {"30000", "20000"}
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    assert not authorize(friend, settings)
    assert await svc.accept(friend) == "ignored"
    assert authorize(message, settings)
    assert not svc.ai.calls


async def test_config_persists_restart_and_reset_uses_original_environment(settings):
    settings.owner_qq_id = "20000"
    settings.onebot_private_mode = "owner_only"
    settings.admin_token = SecretStr("admission-test-password-never-live")
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    initial = None
    async with app.router.lifespan_context(app):
        runtime = app.state.service.store.runtime
        initial = runtime.env_admission
        saved = AdmissionConfig(
            allowed_groups=["77777"],
            allowed_users=["33333"],
            ignored_bot_ids=["66666"],
            private_mode="owner_and_allowlist",
        )
        await runtime.set_admission(saved)
    # A fresh settings instance represents the .env snapshot, not hot-mutated prior object.
    restarted = settings.model_copy(
        update={
            "onebot_allowed_groups": set(initial.allowed_groups),
            "onebot_allowed_users": set(initial.allowed_users),
            "bot_qq_ids": set(initial.ignored_bot_ids),
            "onebot_private_mode": initial.private_mode,
        }
    )
    app = create_app(restarted, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        runtime = app.state.service.store.runtime
        assert runtime.admission == saved and runtime.admission_overridden
        await runtime.set_admission(None)
        assert runtime.admission == initial and not runtime.admission_overridden
        assert restarted.onebot_allowed_groups == set(initial.allowed_groups)


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/admin/api/admission"),
        ("PUT", "/admin/api/admission"),
        ("DELETE", "/admin/api/admission"),
        ("POST", "/admin/api/admission/change"),
    ],
)
async def test_new_routes_require_admin_auth(admission_api, method, path):
    app, client = admission_api
    response = await client.request(
        method, path, headers={"Authorization": "Bearer wrong"}, json={}
    )
    assert response.status_code == 401
    assert not app.state.service.ai.calls
