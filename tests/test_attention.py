import time

import httpx
import pytest
from pydantic import SecretStr

from hakimi.adapters import onebot_message
from hakimi.app import create_app
from hakimi.rich_message_parser import parse_message
from hakimi.security import authorize
from hakimi.service import Service
from hakimi.store import Store
from tests.fakes import FakeBili, FakeGateway, FakeSender


def event(segments, *, group="40000", user="30000", event_id=123):
    return {
        "post_type": "message",
        "message_type": "group",
        "self_id": "10000",
        "user_id": user,
        "group_id": group,
        "message_id": event_id,
        "time": time.time(),
        "message": segments,
    }


@pytest.mark.parametrize("name", ["吉米奈喵", "吉米奈", "哈基米"])
def test_alias_is_explicit_attention(settings, name):
    message = onebot_message(event([{"type": "text", "data": {"text": name + "叫一声"}}]), settings)
    assert message.addressed and message.attention_reason == "alias"
    assert authorize(message, settings)


def test_bili_card_is_not_explicit_without_mention(settings):
    segments = [
        {"type": "json", "data": {"data": '{"url":"https://bilibili.com/video/BV1xx411c7mD"}'}}
    ]
    message = onebot_message(event(segments), settings)
    assert not message.addressed and message.attention_reason == ""
    assert not authorize(message, settings)
    at_segments = [{"type": "at", "data": {"qq": "10000"}}, *segments]
    mentioned = onebot_message(event(at_segments), settings)
    assert mentioned.addressed and mentioned.attention_reason == "mention"
    assert authorize(mentioned, settings)
    settings.group_policies["onebot:40000"] = "muted"
    assert not authorize(mentioned, settings)
    assert not authorize(onebot_message(event(at_segments, group="88888"), settings), settings)


def test_quoted_alias_and_bot_author_fields_do_not_create_attention(settings):
    rich = parse_message(
        [
            {"type": "reply", "data": {"id": "123", "qq": "10000", "text": "哈基米"}},
            {
                "type": "node",
                "data": {
                    "content": [
                        {"type": "text", "data": {"text": "吉米奈喵"}},
                        {"type": "reply", "data": {"id": "999", "qq": "10000"}},
                    ]
                },
            },
        ],
        "10000",
    )
    assert rich.reply_ids == ["123"] and not rich.addressed and not rich.command_text
    message = onebot_message(
        event([{"type": "json", "data": {"data": '{"title":"哈基米"}'}}]), settings
    )
    assert not message.addressed


async def test_receipt_verified_and_scoped_no_unknown_private_bot_bypass(settings, store, message):
    service = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    sent = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    await store.runtime.attention.record_sent(sent, {"message_id": -123})
    segments = [
        {"type": "reply", "data": {"id": "-123", "qq": "10000"}},
        {"type": "text", "data": {"text": "再解释一下"}},
    ]
    quoted = onebot_message(event(segments), settings)
    assert not quoted.addressed
    resolved = await store.runtime.attention.resolve(quoted)
    assert resolved.addressed and resolved.attention_reason == "reply"
    assert await service.accept(quoted) == "accepted"
    job = await store.claim()
    assert job["payload"]["attention_reason"] == "reply"
    for group in ["50000", "88888"]:
        foreign = onebot_message(event(segments, group=group), settings)
        assert not (await store.runtime.attention.resolve(foreign)).addressed
        assert await service.accept(foreign) == "ignored"
    settings.bot_qq_ids.add("30000")
    assert await service.accept(quoted.model_copy(update={"event_id": "blocked"})) == "ignored"
    assert not service.ai.calls


async def test_receipt_persists_restart_and_expires(settings, message):
    store = Store(settings)
    await store.open()
    sent = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    try:
        await store.runtime.attention.record_sent(sent, {"message_id": 123})
    finally:
        await store.close()
    store = Store(settings)
    await store.open()
    try:
        incoming = sent.model_copy(update={"addressed": False, "reply_ids": ["123"]})
        assert (await store.runtime.attention.resolve(incoming)).addressed
        await store.db.execute("UPDATE sent_bot_receipts SET created=0")
        assert not (await store.runtime.attention.resolve(incoming)).addressed
    finally:
        await store.close()


async def test_unconfirmed_or_private_receipt_never_verifies_group_quote(store, message):
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    for receipt in [None, {}, {"message_id": True}, {"message_id": "untrusted"}]:
        await store.runtime.attention.record_sent(group, receipt)
    await store.runtime.attention.record_sent(message, {"message_id": 123})
    assert not await store.all("SELECT * FROM sent_bot_receipts")


async def test_group_relevance_is_separate_from_private_and_cleared_with_session(
    settings, store, message
):
    service = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    settings.group_policies["onebot:40000"] = "smart_reply"
    base = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "addressed": False}
    )
    first = base.model_copy(update={"text": "大家聊新番剧情", "event_id": "previous"})
    assert await service.accept(first) == "accepted"
    # Nothing from an Owner private conversation is eligible as group relevance.
    await store.runtime.filters.remember_group(
        message.model_copy(update={"text": "owner-secret-sentinel"})
    )
    current = base.model_copy(
        update={"text": "这段太有趣了", "event_id": "current", "created_at": time.time()}
    )
    assert await service.accept(current) == "accepted"
    assert await store.runtime.filters.group_context(current) == ["大家聊新番剧情"]
    other_group = current.model_copy(update={"conversation_id": "50000"})
    assert await store.runtime.filters.group_context(other_group) == []
    await service.sessions.clear(current)
    assert await store.runtime.filters.group_context(current) == []


async def test_sandbox_direct_attention_matches_protocol_without_production_context(settings):
    settings.admin_token = SecretStr("attention-test-admin-token")
    gateway = FakeGateway()
    gateway.block_purpose = "message"  # Stop before any video fetch or generation.
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer attention-test-admin-token"},
        ) as client:
            plain_video = await client.post(
                "/admin/api/sandbox",
                json={
                    "identity": "MEMBER",
                    "scene": "group_plain",
                    "group_id": "40000",
                    "text": "BV1xx411c7mD",
                },
            )
            assert plain_video.status_code == 200
            assert plain_video.json()["decisions"]["reason"] == "group_policy_ignored"
            assert not gateway.calls
            for scene, text in [
                ("group_plain", "哈基米，猫娘叫一声"),
                ("group_at", "BV1xx411c7mD"),
            ]:
                gateway.calls.clear()
                response = await client.post(
                    "/admin/api/sandbox",
                    json={
                        "identity": "MEMBER",
                        "scene": scene,
                        "group_id": "40000",
                        "text": text,
                    },
                )
                assert response.status_code == 200
                assert len(gateway.calls) == 1
                assert gateway.calls[0][3]["message"]["addressed"] is True
                assert "relevance_context" not in gateway.calls[0][3]
        assert not await app.state.service.store.all("SELECT * FROM gateway_group_context")
