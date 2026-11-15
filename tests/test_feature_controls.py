"""Exercise newly mounted control-plane routes through the real auth middleware."""

import json

import httpx
import pytest

from hakimi.app import create_app
from hakimi.models import Video
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import admin_api as admin_api

CONTROL_ROUTES = [
    ("GET", "/admin/api/moderation"),
    ("PUT", "/admin/api/moderation/users"),
    ("POST", "/admin/api/moderation/proposals/abc123"),
    ("GET", "/admin/api/longnick"),
    ("PUT", "/admin/api/longnick/config"),
    ("DELETE", "/admin/api/longnick/config"),
    ("POST", "/admin/api/longnick"),
    ("GET", "/admin/api/bili-account"),
    ("PUT", "/admin/api/bili-account"),
    ("DELETE", "/admin/api/bili-account"),
    ("POST", "/admin/api/bili-account/credentials"),
    ("POST", "/admin/api/bili-account/test"),
    ("POST", "/admin/api/bili-account/run"),
]


@pytest.mark.parametrize("method,path", CONTROL_ROUTES)
async def test_new_feature_routes_disabled_without_admin_token(settings, method, path):
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            response = await client.request(method, path, json={})
        assert response.status_code == 403
        assert response.json()["detail"] == "admin_disabled"
        assert not gateway.calls


@pytest.mark.parametrize("method,path", CONTROL_ROUTES)
async def test_new_feature_routes_require_auth_before_validation_or_effects(
    admin_api, method, path
):
    app, client, gateway = admin_api
    # Deliberately invalid input must never reach route parsing before authentication.
    response = await client.request(
        method, path, headers={"Authorization": "Bearer wrong"}, json={"unknown": True}
    )
    assert response.status_code == 401
    assert not gateway.calls
    svc = app.state.service
    assert not await svc.store.all("SELECT * FROM admin_config")
    assert not await svc.store.all("SELECT * FROM bili_credentials")
    assert not await svc.store.all("SELECT * FROM muted_users")


async def test_credential_admin_write_is_encrypted_hidden_and_uses_no_api(admin_api):
    app, client, gateway = admin_api
    values = {
        "sessdata": "unit-only-account-cookie-EXAMPLE",
        "bili_jct": "unit-only-csrf-cookie-EXAMPLE",
        "buvid3": "unit-only-device-cookie-EXAMPLE",
    }
    response = await client.post("/admin/api/bili-account/credentials", json=values)
    assert response.status_code == 200
    assert response.json()["credentials"] == {**dict.fromkeys(values, True), "buvid4": False}
    status = await client.get("/admin/api/bili-account")
    assert status.status_code == 200
    svc = app.state.service
    row = await svc.store.one("SELECT payload FROM bili_credentials WHERE id=1")
    assert row is not None
    for secret in values.values():
        assert secret not in response.text + status.text
        assert secret.encode() not in row[0]
        assert secret in svc.secrets
    assert not gateway.calls
    assert not await svc.store.all("SELECT * FROM bili_patrol_runs")
    assert not await svc.store.all("SELECT * FROM bili_actions")
    assert svc.bili_watcher.last_request == 0


async def test_invalid_credential_input_is_not_reflected(admin_api):
    _, client, gateway = admin_api
    secret = "unit-only-cookie-with-sensitive-content; BAD=1"
    response = await client.post("/admin/api/bili-account/credentials", json={"sessdata": secret})
    assert response.status_code == 400
    assert secret not in response.text
    assert not gateway.calls


@pytest.mark.parametrize("path", ["test", "run"])
async def test_bili_admin_actions_fail_closed_without_credentials_or_enablement(admin_api, path):
    app, client, gateway = admin_api
    response = await client.post("/admin/api/bili-account/" + path)
    assert response.status_code == 409
    assert response.json()["detail"] in {
        "bili_credentials_missing",
        "bili_autonomous_disabled",
    }
    assert app.state.service.bili_watcher.last_request == 0
    assert not gateway.calls


@pytest.mark.parametrize("uid", ["10000", "20000"])
async def test_admin_cannot_mute_or_remove_protected_owner_or_self(admin_api, uid):
    app, client, gateway = admin_api
    response = await client.put(
        "/admin/api/moderation/users",
        json={"user_id": uid, "muted": True, "delete_friend": True},
    )
    assert response.status_code == 409
    assert not await app.state.service.store.all("SELECT * FROM muted_users")
    assert not gateway.calls


async def test_admin_mute_is_local_hot_and_does_not_remove_friend_by_default(admin_api, message):
    from hakimi.security import authorize

    app, client, gateway = admin_api
    svc = app.state.service
    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    assert authorize(friend, svc.s)
    response = await client.put("/admin/api/moderation/users", json={"user_id": "30000"})
    assert response.status_code == 200
    assert not authorize(friend, svc.s)
    state = await client.get("/admin/api/moderation")
    assert "30000" in json.dumps(state.json())
    row = await svc.store.one("SELECT * FROM muted_users")
    assert row["delete_status"] in {None, "not_requested"}
    response = await client.put(
        "/admin/api/moderation/users", json={"user_id": "30000", "muted": False}
    )
    assert response.status_code == 200 and authorize(friend, svc.s)
    assert not gateway.calls


@pytest.mark.parametrize("blocked_purpose,expected_strikes", [("message", 1), ("source", 0)])
async def test_only_current_group_message_verdict_is_attributed_to_sender(
    admin_api, message, blocked_purpose, expected_strikes
):
    app, _, gateway = admin_api
    svc = app.state.service
    svc.bili = FakeBili(
        [
            Video(
                bvid="BV1xx411c7mD",
                mid="12345",
                title="Public video",
                description="Untrusted description",
                author="UP",
                published_at=1000,
            )
        ]
    )
    svc.bili_watcher.public_material = svc.bili.material
    gateway.block_purpose = blocked_purpose
    group = message.model_copy(
        update={
            "user_id": "30000",
            "kind": "group",
            "conversation_id": "40000",
            "attention_reason": "mention",
            "mentioned_ids": ["10000"],
            "text": "https://www.bilibili.com/video/BV1xx411c7mD",
        }
    )
    await svc.sessions.remember(group, "previous-member-history-sentinel", "Earlier answer")
    assert await svc.accept(group) == "accepted"
    await svc.process(await svc.store.claim())
    strikes = await svc.store.all("SELECT * FROM user_strikes")
    assert len(strikes) == expected_strikes
    # A bad external source or another member's history must not count against the sharer.
    current = gateway.calls[0]
    assert current[3]["purpose"] == "message"
    assert "previous-member-history-sentinel" not in json.dumps(current)
    assert not any(call[0] == "generate" for call in gateway.calls)
