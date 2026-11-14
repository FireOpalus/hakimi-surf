import hashlib
import hmac
import json
import time
from datetime import UTC, datetime

import httpx
import pytest

from hakimi.app import create_app
from hakimi.security import qq_signing_key
from tests.fakes import FakeGateway


@pytest.fixture
async def api(settings):
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://local"
        ) as client:
            yield app, client, gateway


def ob_payload():
    return {
        "post_type": "message",
        "self_id": 10000,
        "user_id": 20000,
        "message_type": "private",
        "message_id": -123,
        "message": [{"type": "text", "data": {"text": "你好"}}],
        "time": time.time(),
    }


def ob_headers(body, settings):
    return {
        "Content-Type": "application/json",
        "X-Signature": "sha1="
        + hmac.new(
            settings.onebot_secret.get_secret_value().encode(), body, hashlib.sha1
        ).hexdigest(),
    }


def qq_payload():
    return {
        "op": 0,
        "id": "envelope-id",
        "t": "C2C_MESSAGE_CREATE",
        "d": {
            "id": "message-id",
            "author": {"user_openid": "user-openid"},
            "content": "你好",
            "timestamp": datetime.now(UTC).isoformat(),
            "message_type": 0,
        },
    }


def qq_headers(body, settings):
    stamp = str(int(time.time()))
    signature = qq_signing_key(settings.qq_app_secret.get_secret_value()).sign(
        stamp.encode() + body
    )
    return {
        "Content-Type": "application/json",
        "X-Bot-Appid": settings.qq_app_id,
        "X-Signature-Timestamp": stamp,
        "X-Signature-Ed25519": signature.hex(),
    }


async def test_onebot_accepts_once_and_acknowledges_without_model_call(api, settings):
    app, client, ai = api
    body = json.dumps(ob_payload()).encode()
    for _ in range(2):
        response = await client.post(
            "/webhooks/onebot", content=body, headers=ob_headers(body, settings)
        )
        assert response.status_code == 204
    assert not ai.calls
    job = await app.state.service.store.claim()
    assert job["payload"]["text"] == "你好"
    assert await app.state.service.store.claim() is None


async def test_onebot_invalid_signature_and_stale_events(api, settings):
    app, client, _ = api
    body = json.dumps(ob_payload()).encode()
    assert (
        await client.post(
            "/webhooks/onebot", content=body, headers={"Content-Type": "application/json"}
        )
    ).status_code == 401
    payload = ob_payload()
    payload["time"] -= 1000
    body = json.dumps(payload).encode()
    assert (
        await client.post("/webhooks/onebot", content=body, headers=ob_headers(body, settings))
    ).status_code == 400
    assert await app.state.service.store.claim() is None


async def test_body_limit_duplicate_keys_and_wrong_bot(api, settings):
    _, client, _ = api
    oversized = b"x" * (settings.max_body_bytes + 1)
    assert (
        await client.post(
            "/webhooks/onebot", content=oversized, headers=ob_headers(oversized, settings)
        )
    ).status_code == 413
    duplicate = b'{"post_type":"message","post_type":"notice"}'
    assert (
        await client.post(
            "/webhooks/onebot", content=duplicate, headers=ob_headers(duplicate, settings)
        )
    ).status_code == 400
    payload = ob_payload()
    payload["self_id"] = 11111
    body = json.dumps(payload).encode()
    assert (
        await client.post("/webhooks/onebot", content=body, headers=ob_headers(body, settings))
    ).status_code == 400


async def test_queue_full_returns_retryable_http_status(api, settings):
    app, client, _ = api
    app.state.settings.queue_capacity = 1
    payload = ob_payload()
    body = json.dumps(payload).encode()
    assert (
        await client.post("/webhooks/onebot", content=body, headers=ob_headers(body, settings))
    ).status_code == 204
    payload["message_id"] = 124
    body = json.dumps(payload).encode()
    assert (
        await client.post("/webhooks/onebot", content=body, headers=ob_headers(body, settings))
    ).status_code == 503


async def test_qq_url_challenge_never_enqueues_and_has_constrained_signing(api, settings):
    app, client, ai = api
    stamp = str(int(time.time()))
    response = await client.post(
        "/webhooks/qq",
        json={
            "op": 13,
            "d": {
                "event_ts": stamp,
                "plain_token": "ChallengeToken123",
            },
        },
        headers={"X-Bot-Appid": settings.qq_app_id},
    )
    assert response.status_code == 200
    data = response.json()
    qq_signing_key(settings.qq_app_secret.get_secret_value()).public_key().verify(
        bytes.fromhex(data["signature"]), (stamp + data["plain_token"]).encode()
    )
    malicious = await client.post(
        "/webhooks/qq",
        json={
            "op": 13,
            "d": {
                "event_ts": stamp,
                "plain_token": '{"op":0}',
            },
        },
        headers={"X-Bot-Appid": settings.qq_app_id},
    )
    assert malicious.status_code == 400
    assert await app.state.service.store.claim() is None
    assert not ai.calls


async def test_qq_signed_dispatch_and_unsigned_forgery(api, settings):
    app, client, _ = api
    # A verified QQ signature proves the sender, not Owner identity or private admission.
    settings.qq_allowed_users.add("user-openid")
    body = json.dumps(qq_payload()).encode()
    forged = await client.post(
        "/webhooks/qq",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Bot-Appid": settings.qq_app_id,
        },
    )
    assert forged.status_code == 401
    for _ in range(2):
        response = await client.post(
            "/webhooks/qq", content=body, headers=qq_headers(body, settings)
        )
        assert response.json() == {"op": 12}
    job = await app.state.service.store.claim()
    assert job["payload"]["user_id"] == "user-openid"
    assert job["payload"]["event_id"] == "message-id"
    assert await app.state.service.store.claim() is None


async def test_health_no_details_and_documentation_not_public(api):
    _, client, _ = api
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    assert (await client.get("/docs")).status_code == 404
    assert (await client.get("/openapi.json")).status_code == 404
