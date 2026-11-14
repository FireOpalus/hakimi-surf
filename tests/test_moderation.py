import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hakimi.moderation import Moderation
from hakimi.security import Rejected


@pytest.fixture
async def moderation(settings, store):
    settings.owner_qq_id = "20000"
    wire = SimpleNamespace(call=AsyncMock(return_value=[]))
    service = SimpleNamespace(
        s=settings,
        store=store,
        sender=SimpleNamespace(onebot=wire),
        proactive=SimpleNamespace(alert=AsyncMock()),
    )
    module = Moderation(service, clock=lambda: 1_000_000)
    await module.initialize()
    return module


def offender(message, event="one"):
    return message.model_copy(
        update={"user_id": "30000", "kind": "group", "conversation_id": "40000", "event_id": event}
    )


async def propose(moderation, message):
    result = None
    for i in range(3):
        result = await moderation.record_strike(
            offender(message, str(i)), "prompt_injection", "群友小明"
        )
    assert result
    return result


async def test_three_unique_strikes_propose_only_no_automatic_mute(moderation, message):
    proposal = await propose(moderation, message)
    assert not moderation.s.muted_user_ids
    assert not await moderation.store.all("SELECT * FROM muted_users")
    assert not moderation.service.sender.onebot.call.called
    moderation.service.proactive.alert.assert_awaited_once()
    note = moderation.service.proactive.alert.call_args.args[1]
    assert proposal in note and "30000" in note and "群友小明" in note
    for _ in range(4):
        await moderation.record_strike(offender(message, "repeat"), "content_blocked")
    assert len(await moderation.store.all("SELECT * FROM user_strikes")) == 4
    assert len(await moderation.store.all("SELECT * FROM pending_mute_proposals")) == 1
    assert moderation.service.proactive.alert.await_count == 1


@pytest.mark.parametrize("uid", ["20000", "10000", "020000", True, "invalid"])
async def test_protected_or_invalid_identity_never_struck_or_muted(moderation, message, uid):
    with pytest.raises(Rejected):
        await moderation.set_muted(uid, delete_friend=True)
    if isinstance(uid, str):
        assert (
            await moderation.record_strike(
                offender(message).model_copy(update={"user_id": uid}), "prompt_injection"
            )
            is None
        )
    assert not await moderation.store.all("SELECT * FROM user_strikes")
    assert not moderation.service.sender.onebot.call.called


async def test_private_and_source_reasons_never_form_proposals(moderation, message):
    for i in range(4):
        await moderation.record_strike(
            message.model_copy(
                update={"user_id": "30000", "conversation_id": "30000", "event_id": str(i)}
            ),
            "prompt_injection",
        )
        await moderation.record_strike(offender(message, str(i)), "generation_incomplete")
    assert not await moderation.store.all("SELECT * FROM user_strikes")


async def test_approval_is_explicit_application_mute_and_reject_is_safe(moderation, message):
    proposal = await propose(moderation, message)
    result = await moderation.review(proposal, True)
    assert result["status"] == "approved" and "30000" in moderation.s.muted_user_ids
    assert not moderation.service.sender.onebot.call.called
    with pytest.raises(Rejected, match="proposal_already_resolved_or_expired"):
        await moderation.review(proposal, True, delete_friend=True)
    await moderation.set_muted("30000", False)
    assert "30000" not in moderation.s.muted_user_ids
    await moderation.record_strike(offender(message, "new-after-reject"), "content_blocked")
    assert len(await moderation.store.all("SELECT * FROM pending_mute_proposals")) == 1


async def test_friend_delete_requires_extra_approval_and_actual_friend(moderation, message):
    call = moderation.service.sender.onebot.call
    call.side_effect = [[{"user_id": 30000}, {"user_id": 20000}], {}]
    await moderation.set_muted("30000", True, delete_friend=True)
    assert call.call_args_list[0].args == ("get_friend_list", {})
    assert call.call_args_list[1].args == ("delete_friend", {"user_id": 30000})
    assert (await moderation.status())["muted_users"][0]["delete_status"] == "done"
    await moderation.set_muted("30000", True, delete_friend=True)
    assert call.await_count == 2


async def test_not_friend_and_ambiguous_delete_never_retry(moderation):
    call = moderation.service.sender.onebot.call
    call.return_value = []
    result = await moderation.set_muted("30000", True, delete_friend=True)
    assert result["delete_status"] == "not_friend" and call.await_count == 1
    call.side_effect = [[{"user_id": 30000}], TimeoutError("secret exception")]
    result = await moderation.set_muted("30000", True, delete_friend=True)
    assert result["delete_status"] == "uncertain"
    await moderation.set_muted("30000", True, delete_friend=True)
    assert call.await_count == 3


async def test_owner_private_commands_only_and_quoted_commands_are_data(moderation, message):
    for bad in [
        offender(message),
        message.model_copy(update={"kind": "group", "conversation_id": "40000"}),
    ]:
        with pytest.raises(Rejected, match="owner_private_only"):
            await moderation.command(bad.model_copy(update={"text": "/mute 30000"}))
    assert (
        await moderation.command(
            message.model_copy(update={"text": "/mute 30000", "command_text": ""})
        )
        is None
    )
    await moderation.command(message.model_copy(update={"text": "/mute 30000"}))
    assert "30000" in moderation.s.muted_user_ids
    assert not moderation.service.sender.onebot.call.called
    await moderation.command(message.model_copy(update={"text": "/unmute 30000"}))
    assert not moderation.s.muted_user_ids


async def test_strikes_expire_and_pending_proposal_expires(moderation, message):
    clock = [1_000_000]
    moderation.clock = lambda: clock[0]
    await moderation.record_strike(offender(message, "first"), "prompt_injection")
    clock[0] += 86401
    for i in range(2):
        assert await moderation.record_strike(offender(message, str(i)), "prompt_injection") is None
    proposal = await moderation.record_strike(offender(message, "third"), "prompt_injection")
    clock[0] += 172801
    with pytest.raises(Rejected, match="proposal_already_resolved_or_expired"):
        await moderation.review(proposal, True)
    assert not moderation.s.muted_user_ids


async def test_identity_payload_encrypted_and_restore_cache(moderation, message):
    await propose(moderation, message)
    row = await moderation.store.one("SELECT identity FROM pending_mute_proposals")
    assert "群友小明".encode() not in row[0]
    await moderation.set_muted("30000")
    restored = Moderation(moderation.service)
    moderation.s.muted_user_ids = set()
    await restored.initialize()
    assert moderation.s.muted_user_ids == {"30000"}


async def test_parallel_strikes_create_single_proposal(moderation, message):
    await asyncio.gather(
        *[moderation.record_strike(offender(message, str(i)), "prompt_injection") for i in range(9)]
    )
    assert len(await moderation.store.all("SELECT * FROM pending_mute_proposals")) == 1
    assert moderation.service.proactive.alert.await_count == 1
