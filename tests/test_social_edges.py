"""Regressions at the meme delivery, hot-admission and real-SDK prompt boundaries."""

import asyncio
import json
from urllib.parse import quote

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.persona import OWNER_PERSONA_PROMPT, PUBLIC_PERSONA_PROMPT, SYSTEM_INSTRUCTION
from hakimi.security import Rejected
from tests.test_admin import admin_api as admin_api
from tests.test_memes import picture
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_social_integration import group, process, upload_meme, watch_delay
from tests.test_social_integration import social as social


def incoming(message, surface):
    if surface == "group":
        return group(message)
    return message.model_copy(
        update={"user_id": "30000", "conversation_id": "30000", "event_id": "friend-meme"}
    )


@pytest.mark.parametrize("revocation", ["group", "private", "muted", "bot"])
async def test_send_meme_rechecks_admission_after_async_guard(social, message, revocation):
    await upload_meme(social)
    target = incoming(message, "private" if revocation == "private" else "group")
    image = await social.memes.get_base64("无语盯")
    entered, resume = asyncio.Event(), asyncio.Event()

    async def guard():
        entered.set()
        await resume.wait()
        return True  # A guard's stale admission result cannot override current routing rules.

    pending = asyncio.create_task(social.sender.send_meme(target, image, before_send=guard))
    await asyncio.wait_for(entered.wait(), 2)
    if revocation == "group":
        social.s.onebot_allowed_groups = set()
    elif revocation == "private":
        social.s.onebot_allowed_users = set()
    elif revocation == "muted":
        await social.moderation.set_muted(target.user_id)
    else:
        social.s.bot_qq_ids = {target.user_id}
    resume.set()
    with pytest.raises(Rejected, match="reply_invalidated"):
        await pending
    assert not social.sender.onebot.calls


@pytest.mark.parametrize("surface", ["group", "private"])
async def test_pure_meme_is_remembered_only_after_confirmed_delivery(
    social, message, monkeypatch, surface
):
    await upload_meme(social)
    social.ai.answer = "[MEME:无语盯]"
    target = incoming(message, surface)
    watch_delay(monkeypatch, social.sender.onebot)
    job = await process(social, target)
    assert not any(
        m["role"] == "assistant"
        for m in (await social.sessions.snapshot(target))["recent_messages"]
    )
    assert await social.deliver_once()
    assert social.sender.onebot.events == ["delay:0.6", "image"]
    history = (await social.sessions.snapshot(target))["recent_messages"]
    assert len(history) == 2
    assert history[-1]["text"] == "[本地表情：无语盯]"
    assert (await social.store.one("SELECT status FROM outbox WHERE id=?", (job["id"],)))[
        0
    ] == "sent"


@pytest.mark.parametrize("surface", ["group", "private"])
async def test_frequency_cancelled_pure_meme_does_not_create_false_memory(
    social, message, monkeypatch, surface
):
    await upload_meme(social)
    social.ai.answer = "[MEME:无语盯]"
    target = incoming(message, surface)
    watch_delay(monkeypatch, social.sender.onebot)
    await process(social, target)
    await social.deliver_once()
    before = (await social.sessions.snapshot(target))["recent_messages"]
    second = target.model_copy(update={"event_id": "second-meme"})
    job = await process(social, second)
    assert await social.deliver_once()
    assert [
        m
        for m in (await social.sessions.snapshot(target))["recent_messages"]
        if m["role"] == "assistant"
    ] == [m for m in before if m["role"] == "assistant"]
    assert social.sender.onebot.events == ["delay:0.6", "image"]
    assert (await social.store.one("SELECT status FROM outbox WHERE id=?", (job["id"],)))[
        0
    ] == "cancelled"


@pytest.mark.parametrize("receipt", [None, {}, {"message_id": True}, {"message_id": "invalid"}])
async def test_unconfirmed_pure_meme_cannot_claim_success_or_create_memory(
    social, message, monkeypatch, receipt
):
    await upload_meme(social)
    social.ai.answer = "[MEME:无语盯]"
    target = group(message)
    watch_delay(monkeypatch, social.sender.onebot)

    async def unconfirmed(*_):
        return receipt

    monkeypatch.setattr(social.sender.onebot, "call", unconfirmed)
    job = await process(social, target)
    await social.deliver_once()
    assert not any(
        m["role"] == "assistant"
        for m in (await social.sessions.snapshot(target))["recent_messages"]
    )
    assert (await social.store.one("SELECT status FROM outbox WHERE id=?", (job["id"],)))[
        0
    ] != "sent"


async def test_batch_non_anchor_mute_revokes_pending_delivery(social, message):
    prior = group(message, "batch-prior", user_id="31000", text="最初的问题")
    anchor = group(message, "batch-anchor", text="接着一起聊")
    assert await social.accept(anchor) == "accepted"
    job = await social.store.claim()
    job["batch_messages"] = [prior.model_dump(), anchor.model_dump()]
    job["payload"]["text"] = "\n".join(
        json.dumps({"sender_id": m.user_id, "text": m.text}, ensure_ascii=False)
        for m in (prior, anchor)
    )
    await social.process(job)
    assert len(await social.store.all("SELECT * FROM outbox")) == 1
    await social.moderation.set_muted(prior.user_id)
    assert anchor.user_id not in social.s.muted_user_ids
    assert await social.deliver_once()
    assert not social.sender.onebot.calls
    assert (await social.store.one("SELECT status FROM outbox WHERE id=?", (job["id"],)))[
        0
    ] != "sent"


async def test_sandbox_public_matches_tag_and_style_filter_without_delivery_or_frequency(
    admin_api, monkeypatch
):
    app, client, gateway = admin_api
    await upload_meme(app.state.service)
    answer = "今天这个截图我是真的看懂了~有几处细节还可以继续聊。" * 3 + "[MEME:无语盯]"
    original = gateway.generate

    async def generate(*args, **kwargs):
        await original(*args, **kwargs)
        return answer

    monkeypatch.setattr(gateway, "generate", generate)
    response = await client.post(
        "/admin/api/sandbox",
        json={"identity": "MEMBER", "scene": "group_at", "text": "随便聊聊", "group_id": "40000"},
    )
    assert response.status_code == 200, response.text
    preview = response.json()["preview"]
    assert preview["persona"] == "PUBLIC" and preview["meme_tag"] == "无语盯"
    assert preview["text"] == answer.removesuffix("[MEME:无语盯]")
    assert "[MEME:" not in preview["text"] and "~" in preview["text"]
    owner = await client.post(
        "/admin/api/sandbox", json={"identity": "OWNER", "scene": "private", "text": "随便聊聊"}
    )
    assert owner.status_code == 200
    assert owner.json()["preview"]["persona"] == "OWNER"
    assert owner.json()["preview"]["text"] == app.state.service.output(
        answer.removesuffix("[MEME:无语盯]")
    )
    assert "meme_tag" not in owner.json()["preview"]
    for table in ("meme_reply_frequency", "meme_reply_reservations", "outbox", "jobs"):
        assert not await app.state.service.store.all(f"SELECT * FROM {table}")


async def test_actual_sdk_meme_static_prefix_tracks_admin_edits_and_excludes_public_tasks(
    admin_api, sdk_pair, message
):
    app, client, _ = admin_api
    service = app.state.service
    calls = []

    def handler(request):
        if request.url.path.endswith("interactions"):
            calls.append(json.loads(request.content))
            return httpx.Response(200, json=interaction_response())
        assert request.url.path == "/v1beta/decisions"
        return httpx.Response(200, json=decision_response())

    gateway = GeminiGateway(service.s, service.store, *sdk_pair(handler))
    target = group(message)

    async def generate(scenario=Scenario.FAST_CHAT, msg=target):
        await gateway.generate(
            {"current_message": "动态消息内容只在末尾输入"},
            "sdk-meme-prefix",
            scenario=scenario,
            message=msg,
        )
        body = calls[-1]
        expected_store = (
            scenario in {Scenario.FAST_CHAT, Scenario.OWNER_PRIVATE} and msg is not None
        )
        assert body["model"] == "gemini-4-hydrogen" and body["store"] is expected_store
        assert "动态消息内容只在末尾输入" not in body["system_instruction"]
        return body["system_instruction"]

    empty = await generate()
    assert "表情标签白名单：[]" in empty
    response = await client.post(
        "/admin/api/memes/upload/" + quote("刚加入.png"),
        content=picture(),
        headers={"Content-Type": "image/png"},
    )
    assert response.status_code == 200, response.text
    key = response.json()["id"]
    active = await generate()
    assert active.startswith(SYSTEM_INSTRUCTION + PUBLIC_PERSONA_PROMPT)
    assert '表情标签白名单：["刚加入"]' in active
    assert "【服务器验证的当前发送者】" not in active
    last_raw_input = calls[-1]["input"]
    if isinstance(last_raw_input, list):
        first_item = last_raw_input[0]
        last_input = (
            first_item["content"][0]["text"] if "content" in first_item else first_item["text"]
        )
    else:
        last_input = last_raw_input
    assert "【服务器验证的当前发送者】" in last_input
    owner_group = await generate(msg=target.model_copy(update={"user_id": service.s.owner_qq_id}))
    assert (
        active.partition("【服务器验证的当前发送者】")[0]
        == owner_group.partition("【服务器验证的当前发送者】")[0]
    )
    for scenario, msg in (
        (Scenario.BILI_SELECTION, None),
        (Scenario.DAILY_DIARY, None),
        (Scenario.BILI_LEARNING, None),
        (Scenario.BILIBILI_SUMMARY, target),
        (Scenario.FAST_CHAT, None),
        (Scenario.OWNER_PRIVATE, message),
    ):
        instruction = await generate(scenario, msg)
        assert "表情标签白名单" not in instruction and "[MEME:" not in instruction
        if scenario == Scenario.OWNER_PRIVATE:
            assert instruction.startswith(SYSTEM_INSTRUCTION + OWNER_PERSONA_PROMPT)
    assert (await client.delete(f"/admin/api/memes/{key}")).status_code == 200
    assert await generate() == empty
