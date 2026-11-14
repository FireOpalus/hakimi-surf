"""Group membership never depends on the independently restricted private-friend list."""

import json

import httpx
import pytest

from hakimi.bot_server import prompt_context
from hakimi.models import Message
from hakimi.notebook_snapshot import NotebookSnapshot
from hakimi.persona import PUBLIC_PERSONA_PROMPT
from hakimi.security import Rejected, authorize, owner
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender


@pytest.mark.parametrize("private_users", [set(), {"20000"}])
@pytest.mark.parametrize("private_mode", ["owner_only", "owner_and_allowlist"])
@pytest.mark.parametrize("policy,addressed", [("at_only", True), ("smart_reply", False)])
async def test_group_member_replies_without_private_admission_or_owner_context(
    settings, store, message, private_users, private_mode, policy, addressed
):
    settings.onebot_allowed_users = set(private_users)
    settings.onebot_private_mode = private_mode
    settings.allow_group_members = False  # Legacy switch no longer restricts approved groups.
    settings.group_policies["onebot:40000"] = policy
    settings.enable_notebook_snapshot = True
    gateway, sender = FakeGateway(), FakeSender()
    async with httpx.AsyncClient() as http:
        notebook = NotebookSnapshot(settings, http, gateway, store)
        notebook.text = "owner-notebook-private-sentinel"
        service = Service(settings, store, gateway, FakeBili(), sender, notebook=notebook)
        try:
            await service.sessions.remember(message, "owner-private-dialogue-sentinel", "答复")
            group = message.model_copy(
                update={
                    "user_id": "70000",
                    "kind": "group",
                    "conversation_id": "40000",
                    "text": "今天这部新番挺有趣的",
                    "addressed": addressed,
                }
            )
            assert authorize(group, settings)
            assert await service.accept(group) == "accepted"
            await service.process(await store.claim())
            assert await service.deliver_once()
            assert len(sender.sent) == 1 and sender.sent[0][0].conversation_id == "40000"
            generated = [call for call in gateway.calls if call[0] == "generate"]
            assert len(generated) == 1
            trusted_context = prompt_context(
                Message.model_validate(generated[0][3]["message"]), settings
            )
            assert trusted_context.role == "MEMBER"
            assert trusted_context.persona == PUBLIC_PERSONA_PROMPT
            assert notebook.for_message(group) == ""
            assert "owner-notebook-private-sentinel" not in json.dumps(gateway.calls, default=str)
            assert "owner-private-dialogue-sentinel" not in json.dumps(gateway.calls, default=str)
            assert service.sessions.path(group) != service.sessions.path(message)
            previous_calls = len(gateway.calls)
            private = group.model_copy(
                update={
                    "kind": "private",
                    "conversation_id": "70000",
                    "event_id": "member-private",
                    "addressed": True,
                }
            )
            assert not authorize(private, settings)
            assert await service.accept(private) == "ignored"
            assert len(gateway.calls) == previous_calls
        finally:
            await service.stop()


@pytest.mark.parametrize("reason", ["bot", "muted", "self", "unknown_group", "muted_group"])
async def test_group_exclusions_still_stop_before_jobs_models_and_send(
    settings, store, message, reason
):
    settings.onebot_allowed_users = set()
    settings.allow_group_members = False
    settings.group_policies["onebot:40000"] = "smart_reply"
    gateway, sender = FakeGateway(), FakeSender()
    service = Service(settings, store, gateway, FakeBili(), sender)
    group = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "user_id": "70000"}
    )
    if reason == "bot":
        settings.bot_qq_ids.add("70000")
    elif reason == "muted":
        settings.muted_user_ids.add("70000")
    elif reason == "self":
        group = group.model_copy(update={"user_id": settings.onebot_self_id})
    elif reason == "unknown_group":
        group = group.model_copy(update={"conversation_id": "80000"})
    else:
        settings.group_policies["onebot:40000"] = "muted"
    assert not authorize(group, settings)
    assert await service.accept(group) == "ignored"
    assert await store.claim() is None
    assert not gateway.calls and not sender.sent
    assert not await store.all("SELECT * FROM ledger")


@pytest.mark.parametrize("allowed_private", [False, True])
async def test_legacy_onebot_admin_never_has_owner_commands(
    settings, store, message, allowed_private
):
    settings.onebot_owner_ids.add("70000")
    settings.onebot_allowed_users = {"70000"} if allowed_private else set()
    service = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    group = message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "70000",
            "text": "/new-session",
        }
    )
    assert authorize(group, settings) and not owner(group, settings)
    with pytest.raises(Rejected, match="group_owner_only"):
        await service.command(group)
    private = group.model_copy(
        update={"kind": "private", "conversation_id": "70000", "text": "/哈基米 订阅 12345"}
    )
    assert authorize(private, settings) is allowed_private
    assert not owner(private, settings)
    with pytest.raises(Rejected, match="owner_private_only"):
        await service.command(private)
    assert not service.ai.calls


def test_only_validated_numeric_owner_gets_management_authority(settings, message):
    assert owner(message, settings)
    assert not owner(message.model_copy(update={"bot_id": "other"}), settings)
    assert not owner(message.model_copy(update={"conversation_id": "70000"}), settings)
    settings.onebot_enabled = False
    assert not owner(message, settings)


async def test_official_qq_group_members_are_public_and_legacy_openid_has_no_management(
    settings, store, message
):
    settings.qq_allowed_users = set()
    settings.allow_group_members = False
    official = message.model_copy(
        update={
            "platform": "qq",
            "bot_id": settings.qq_app_id,
            "user_id": "user-openid",
            "kind": "group",
            "conversation_id": "group-openid",
            "text": "/new-session",
        }
    )
    assert authorize(official, settings)
    assert not owner(official, settings)
    assert prompt_context(official, settings).role == "MEMBER"
    private = official.model_copy(update={"kind": "private", "conversation_id": "user-openid"})
    assert not authorize(private, settings)
    settings.qq_allowed_users.add("user-openid")
    assert authorize(private, settings) and not owner(private, settings)
    service = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    with pytest.raises(Rejected, match="group_owner_only"):
        await service.command(official)
    assert not service.ai.calls
