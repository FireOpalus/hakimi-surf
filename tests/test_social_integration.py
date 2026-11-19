"""Exercise the real routing/outbox boundary without making HTTP, Gemini or QQ calls."""

import asyncio
import base64
import io
import json
import time

import httpx
import pytest
from PIL import Image

from hakimi.adapters import Sender
from hakimi.generation_policy import Scenario
from hakimi.media import ModelImage
from hakimi.models import ImageRef
from hakimi.notebook_snapshot import NotebookSnapshot
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway


class ReplyGateway(FakeGateway):
    answer = "这图挺有意思。"

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        return self.answer


class Images:
    def __init__(self):
        self.calls = []
        self.failed = False

    async def load_results(self, refs):
        self.calls.append(list(refs))
        return [None if self.failed else ModelImage("/9j/2Q==") for _ in refs]


class Wire:
    def __init__(self):
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls = []
        self.events = []

    async def call(self, action, params):
        self.calls.append((action, params))
        self.events.append(params["message"][0]["type"])
        return {"message_id": len(self.calls)}


@pytest.fixture
async def social(settings, store):
    def deny_network(request):
        pytest.fail(f"Unexpected real-service HTTP request to {request.url.host}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(deny_network)) as http:
        wire, gateway, loader = Wire(), ReplyGateway(), Images()
        svc = Service(
            settings,
            store,
            gateway,
            FakeBili(),
            Sender(settings, http, wire),
            image_loader=loader,
            notebook=NotebookSnapshot(settings, http, gateway, store),
        )
        await svc.memes.initialize()
        await svc.moderation.initialize()
        yield svc
        await svc.stop()


def group(message, event_id="group-question", **changes):
    data = {
        "kind": "group",
        "conversation_id": "40000",
        "user_id": "30000",
        "event_id": event_id,
        "text": "这张图讲的什么？",
        "command_text": "这张图讲的什么？",
        "sender_nickname": "提问的群友",
        "addressed": True,
        "mentioned_ids": ["10000"],
        "created_at": time.time(),
    }
    data.update(changes)
    return message.model_copy(update=data)


async def process(social, incoming):
    assert await social.accept(incoming) == "accepted"
    job = await social.store.claim()
    assert job is not None
    await social.process(job)
    return job


async def test_official_qq_only_confirmed_replies_spend_shared_charge(settings, store, message):
    uid = "friend-openid"
    settings.qq_allowed_users.add(uid)
    target = message.model_copy(
        update={
            "platform": "qq",
            "bot_id": settings.qq_app_id,
            "user_id": uid,
            "conversation_id": uid,
        }
    )
    acks = iter([{"id": "official-message-id"}, {"code": 1}])

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "local-qq-token", "expires_in": 7200})
        return httpx.Response(200, json=next(acks))

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http))
        svc.world_state.clock = lambda: target.created_at
        await svc.world_state.initialize()
        try:
            for key, status in (("first", "sent"), ("second", "uncertain")):
                await store.enqueue_reply(key, target, "你好")
                assert await svc.deliver_once()
                assert (await store.one("SELECT status FROM outbox WHERE id=?", (key,)))[
                    0
                ] == status
            assert await svc.world_state.experience.battery(message, target.created_at) == 98.75
            assert svc.world_state.current.energy == 79.6
        finally:
            await svc.stop()


def image_ref(name="screen"):
    return ImageRef(url=f"https://gchat.qpic.cn/{name}.jpg")


async def upload_meme(social):
    raw = io.BytesIO()
    Image.new("RGB", (24, 16), "purple").save(raw, "JPEG")
    return await social.memes.upload("无语盯.jpg", raw.getvalue())


def watch_delay(monkeypatch, wire, hook=None):
    original_sleep = asyncio.sleep

    async def fast_sleep(delay):
        if delay == 0.6:
            wire.events.append("delay:0.6")
            if hook:
                await hook()
        await original_sleep(0)

    monkeypatch.setattr("hakimi.service.asyncio.sleep", fast_sleep)


@pytest.mark.parametrize("policy", ["at_only", "smart_reply"])
async def test_other_mentions_ignore_before_accounting_images_and_gateway(social, message, policy):
    social.s.group_policies["onebot:40000"] = policy
    incoming = group(
        message,
        mentioned_ids=["99900"],
        addressed=False,
        images=[image_ref()],
    )
    assert await social.accept(incoming) == "ignored"
    assert not social.ai.calls
    assert not social.images.calls
    for table in ("ledger", "jobs", "group_recent_messages", "outbox"):
        assert not await social.store.all(f"SELECT * FROM {table}")


@pytest.mark.parametrize(
    "mentions,text",
    [(["99900", "10000"], "这张图呢？"), (["99900"], "大小姐看看这张图？")],
)
async def test_self_mention_or_wake_word_is_still_eligible(social, message, mentions, text):
    incoming = group(message, mentioned_ids=mentions, text=text, command_text=text)
    await process(social, incoming)
    assert [call[0] for call in social.ai.calls] == ["screen", "generate"]


async def test_prior_three_image_provenance_screening_and_owner_private_isolation(social, message):
    social.s.enable_notebook_snapshot = True
    social.notebook.text = "OWNER-SNAPSHOT-PRIVATE-ONLY"
    await social.sessions.remember(message, "OWNER-HISTORY-PRIVATE-ONLY", "private reply")
    # An old image, other group and Owner private image must never enter this question.
    old = group(message, "old", addressed=False, mentioned_ids=[], images=[image_ref("old")])
    assert await social.accept(old) == "ignored"
    nearby = group(
        message,
        "image-author",
        user_id="31000",
        sender_nickname="发截图的人",
        addressed=False,
        mentioned_ids=[],
        images=[image_ref("nearby")],
        text="刚截的图",
        command_text="刚截的图",
    )
    assert await social.accept(nearby) == "ignored"
    for n in range(5):
        assert (
            await social.accept(group(message, f"nearby-{n}", addressed=False, mentioned_ids=[]))
            == "ignored"
        )
    foreign = group(
        message,
        "foreign-group",
        conversation_id="50000",
        addressed=False,
        mentioned_ids=[],
        images=[image_ref("foreign")],
    )
    assert await social.accept(foreign) == "ignored"
    await social.store.groups.observe(message.model_copy(update={"images": [image_ref("owner")]}))
    assert not social.ai.calls and not social.images.calls
    await process(social, group(message))
    assert [[ref.url for ref in refs] for refs in social.images.calls] == [
        [image_ref("nearby").url]
    ]
    calls = social.ai.calls
    assert [(c[0], c[3].get("purpose")) for c in calls] == [
        ("screen", "message"),
        ("screen", "source"),
        ("generate", None),
    ]
    assert "images" not in calls[0][3]
    assert calls[1][3]["images"] == calls[2][3]["images"]
    assert calls[1][1]["nearby_group_context"] == calls[2][1]["nearby_group_context"]
    assert "发截图的人" in calls[2][1]["image_sources"][0]["sender"]
    assert calls[2][1]["image_sources"][0]["origin"] == "紧邻群消息"
    assert calls[2][3]["scenario"] == Scenario.FAST_CHAT
    captured = repr(calls)
    assert "OWNER-SNAPSHOT-PRIVATE-ONLY" not in captured
    assert "OWNER-HISTORY-PRIVATE-ONLY" not in captured
    assert "owner_notebook_snapshot" not in calls[2][1]


async def test_failed_image_download_becomes_placeholder_without_blocking_reply(social, message):
    social.images.failed = True
    await process(social, group(message, images=[image_ref()]))
    assert [c[0] for c in social.ai.calls] == ["screen", "generate"]
    generated = social.ai.calls[-1]
    assert generated[1]["image_sources"][0]["status"] == "unavailable"
    assert "images" not in generated[3]
    assert len(await social.store.all("SELECT * FROM outbox")) == 1


async def test_current_and_prior_image_authors_are_both_preserved(social, message):
    prior = group(
        message,
        "prior-image",
        user_id="31000",
        sender_nickname="上一位群友",
        addressed=False,
        mentioned_ids=[],
        images=[image_ref("prior")],
    )
    assert await social.accept(prior) == "ignored"
    await process(social, group(message, images=[image_ref("current")]))
    assert [ref.url for refs in social.images.calls for ref in refs] == [
        image_ref("current").url,
        image_ref("prior").url,
    ]
    generated = social.ai.calls[-1]
    sources = generated[1]["image_sources"]
    assert [source["image_number"] for source in sources] == [1, 2]
    assert "提问的群友" in sources[0]["sender"]
    assert "上一位群友" in sources[1]["sender"]
    assert len(generated[3]["images"]) == 2


async def test_unsafe_nearby_image_is_rejected_without_striking_question_author(social, message):
    prior = group(
        message,
        "bad-source",
        user_id="31000",
        addressed=False,
        mentioned_ids=[],
        images=[image_ref()],
    )
    assert await social.accept(prior) == "ignored"
    social.ai.block_purpose = "source"
    await process(social, group(message))
    assert not any(c[0] == "generate" for c in social.ai.calls)
    assert not await social.store.all("SELECT * FROM user_strikes")
    assert not await social.store.all("SELECT * FROM outbox")


async def test_public_reply_strips_tag_bounds_text_then_sends_local_base64(
    social, message, monkeypatch
):
    await upload_meme(social)
    social.ai.answer = "这段话实在有点长~不过图倒是挺有意思。" * 4 + "[MEME:无语盯]"
    wire = social.sender.onebot
    watch_delay(monkeypatch, wire)
    await process(social, group(message))
    assert await social.deliver_once()
    assert wire.events == ["text", "delay:0.6", "image"]
    assert [action for action, _ in wire.calls] == ["send_group_msg", "send_group_msg"]
    sent_text = wire.calls[0][1]["message"][0]["data"]["text"]
    assert sent_text == social.ai.answer.removesuffix("[MEME:无语盯]")
    assert "~" in sent_text and "MEME:" not in sent_text
    data = wire.calls[1][1]["message"][0]["data"]["file"]
    assert data.startswith("base64://")
    decoded = base64.b64decode(data[9:], validate=True)
    with Image.open(io.BytesIO(decoded)) as image:
        assert image.format == "JPEG"
    assert "file://" not in repr(wire.calls)
    assert not data.startswith(("https://", "http://", "file://"))
    snapshot = await social.sessions.snapshot(group(message))
    assert snapshot["recent_messages"][-1]["text"] == sent_text


async def test_unknown_meme_is_dropped_without_image_or_extra_delay(social, message, monkeypatch):
    social.ai.answer = "暂时无语。[MEME:模型编造的表情]"
    wire = social.sender.onebot
    watch_delay(monkeypatch, wire)
    await process(social, group(message))
    assert await social.deliver_once()
    assert wire.events == ["text"]
    assert wire.calls[0][1]["message"][0]["data"]["text"] == "暂时无语"


async def test_admin_delete_during_text_image_delay_revokes_image(social, message, monkeypatch):
    meme = await upload_meme(social)
    social.ai.answer = "看懂了。[MEME:无语盯]"
    wire = social.sender.onebot

    async def remove():
        await social.memes.delete(meme["id"])

    watch_delay(monkeypatch, wire, remove)
    await process(social, group(message))
    assert await social.deliver_once()
    assert wire.events == ["text", "delay:0.6"]
    assert "无语盯" not in social.memes.labels
    assert not social.counters["meme_send_failed"]


async def test_model_cannot_send_meme_more_than_once_per_four_public_replies(
    social, message, monkeypatch
):
    await upload_meme(social)
    social.ai.answer = "懂了。[MEME:无语盯]"
    wire = social.sender.onebot
    watch_delay(monkeypatch, wire)
    image_after_reply = []
    for n in range(8):
        before = sum(event == "image" for event in wire.events)
        await process(social, group(message, f"reply-{n}"))
        assert await social.deliver_once()
        after = sum(event == "image" for event in wire.events)
        if after > before:
            image_after_reply.append(n + 1)
    assert image_after_reply == [1, 5]
    assert sum(event == "text" for event in wire.events) == 8


async def test_owner_private_keeps_original_style_and_never_emits_meme(
    social, message, monkeypatch
):
    await upload_meme(social)
    social.ai.answer = "BOISER，慢慢说喵~" + "你提到的那些细节我们可以再一起仔细看看。" * 3
    wire = social.sender.onebot
    watch_delay(monkeypatch, wire)
    await process(social, message)
    assert social.ai.calls[-1][3]["scenario"] == Scenario.OWNER_PRIVATE
    assert await social.deliver_once()
    assert wire.events == ["text"]
    assert wire.calls[0][0] == "send_private_msg"
    assert wire.calls[0][1]["user_id"] == int(social.s.owner_qq_id)
    sent_text = wire.calls[0][1]["message"][0]["data"]["text"]
    assert sent_text == social.output(social.ai.answer)
    assert len(sent_text) > 30 and "~" in sent_text


async def test_allowlisted_friend_uses_public_style_without_owner_snapshot_or_history(
    social, message, monkeypatch
):
    social.s.enable_notebook_snapshot = True
    social.notebook.text = "OWNER-PRIVATE-NOTEBOOK"
    await social.sessions.remember(message, "OWNER-PRIVATE-RECENT-HISTORY", "owner reply")
    friend = message.model_copy(
        update={"user_id": "30000", "conversation_id": "30000", "event_id": "friend"}
    )
    social.ai.answer = "来了~这事倒挺有趣。"
    wire = social.sender.onebot
    watch_delay(monkeypatch, wire)
    await process(social, friend)
    generated = social.ai.calls[-1]
    assert generated[3]["scenario"] == Scenario.FAST_CHAT
    assert "owner_notebook_snapshot" not in generated[1]
    assert "OWNER-PRIVATE" not in repr(social.ai.calls)
    assert await social.deliver_once()
    assert wire.events == ["text"]
    assert wire.calls[0][1]["user_id"] == 30000
    assert wire.calls[0][1]["message"][0]["data"]["text"] == "来了~这事倒挺有趣"


@pytest.mark.parametrize("revoke_first_member", [False, True])
async def test_at_only_trigger_and_unaddressed_followup_generate_once_with_public_context(
    social, message, monkeypatch, revoke_first_member
):
    social.s.group_policies["onebot:40000"] = "at_only"
    social.s.group_debounce_seconds = 1.8
    social.s.group_debounce_max_seconds = 3.5
    social.s.onebot_allowed_users.clear()
    social.s.enable_notebook_snapshot = True
    social.notebook.text = "OWNER-PRIVATE-SNAPSHOT-SENTINEL"
    await social.sessions.remember(message, "OWNER-PRIVATE-HISTORY-SENTINEL", "private answer")
    current = [time.time()]
    monkeypatch.setattr("hakimi.store.time.time", lambda: current[0])
    social.store.groups.clock = lambda: current[0]
    # The delivery revocation case uses an ordinary first sender who is not the
    # merged representative, proving persisted participants are all rechecked.
    first_uid = "31000" if revoke_first_member else social.s.owner_qq_id
    first = group(
        message,
        "at-only-first",
        user_id=first_uid,
        text="说说刚才那个问题",
        command_text="说说刚才那个问题",
    )
    assert await social.accept(first) == "accepted"
    assert await social.store.claim() is None
    current[0] += 0.8
    followup = group(
        message,
        "at-only-followup",
        addressed=False,
        mentioned_ids=[],
        text="我补充一下，是网络断开了",
        command_text="我补充一下，是网络断开了",
    )
    assert await social.accept(followup) == "accepted"
    current[0] += 1.79
    assert await social.store.claim() is None
    assert not social.ai.calls
    current[0] += 0.02
    job = await social.store.claim()
    assert [m["addressed"] for m in job["batch_messages"]] == [True, False]
    assert job["payload"]["user_id"] == followup.user_id
    await social.process(job)
    assert [call[0] for call in social.ai.calls] == ["screen", "generate"]
    generated = social.ai.calls[-1]
    originals = [json.loads(line) for line in generated[1]["current_message"].splitlines()]
    assert [(item["sender_id"], item["text"]) for item in originals] == [
        (first_uid, first.text),
        (followup.user_id, followup.text),
    ]
    assert generated[3]["scenario"] == Scenario.FAST_CHAT
    assert "owner_notebook_snapshot" not in generated[1]
    assert "OWNER-PRIVATE" not in repr(social.ai.calls)
    assert not await social.store.claim()
    rows = await social.store.all("SELECT * FROM outbox")
    assert len(rows) == 1
    reply = social.store.vault.open(rows[0]["id"], rows[0]["payload"])
    assert [(m["user_id"], m["addressed"]) for m in reply["participants"]] == [
        (first_uid, True),
        (followup.user_id, False),
    ]
    if revoke_first_member:
        social.s.muted_user_ids.add(first_uid)
    assert await social.deliver_once()
    assert not await social.deliver_once()
    wire = social.sender.onebot
    assert len(wire.calls) == (0 if revoke_first_member else 1)
    if not revoke_first_member:
        assert wire.calls[0][0] == "send_group_msg"
        assert wire.calls[0][1]["group_id"] == 40000


@pytest.mark.parametrize("block", [False, True])
async def test_mixed_owner_member_batch_has_one_generation_and_no_false_strikes(
    social, message, block
):
    owner = group(message, "batch-owner", user_id=social.s.owner_qq_id, text="看这个")
    member = group(message, "batch-member", text="接着刚才的话题说")
    assert await social.accept(member) == "accepted"
    job = await social.store.claim()
    # Durable batching has its own clock/transaction tests; exercise the service's batch contract.
    job["batch_messages"] = [owner.model_dump(), member.model_dump()]
    job["payload"]["text"] = "\n".join(
        json.dumps({"sender_id": m.user_id, "text": m.text}, ensure_ascii=False)
        for m in (owner, member)
    )
    if block:
        social.ai.block_purpose = "message"
    await social.process(job)
    assert len([c for c in social.ai.calls if c[0] == "generate"]) == (0 if block else 1)
    assert len(await social.store.all("SELECT * FROM outbox")) == (0 if block else 1)
    assert not await social.store.all("SELECT * FROM user_strikes")
    if not block:
        assert social.ai.calls[-1][3]["scenario"] == Scenario.FAST_CHAT
        assert social.ai.calls[-1][3]["message"]["user_id"] == "30000"
