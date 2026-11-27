import asyncio
import base64
import io
import json
import time
from types import SimpleNamespace as NS

import pytest
from PIL import Image

from hakimi.adapters import notice_message
from hakimi.hot_ring import HotRing
from hakimi.media import ImageLoader, normalize_image
from hakimi.memes import MemeLibrary
from hakimi.models import ImageRef
from hakimi.outbound_actions import parse_actions
from hakimi.outgoing import recover_receipts
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.streaming import cancellable, consume_stream
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_group_pipeline import group


def action(**updates):
    return json.dumps(
        {
            "abstain": False,
            "bubbles": ["好，我看到了"],
            "anchor": {},
            "sticker": {},
            "image_digest": None,
            **updates,
        },
        ensure_ascii=False,
    )


class ActionGateway(FakeGateway):
    def __init__(self, text=None):
        super().__init__()
        self.text = text or action()

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        return self.text


class ActionSender(FakeSender):
    async def send_action_bubble(self, message, segments, *, targets, before_send):
        assert await before_send()
        self.sent.append(segments)
        if self.fail:
            raise TimeoutError
        return {"message_id": len(self.sent) + 100}

    async def send_folded(self, message, text, *, before_send):
        assert await before_send()
        self.sent.append({"forward": text})
        return {"message_id": 500}


@pytest.mark.parametrize(
    "reason,text,images,expected",
    [
        ("mention", "你好", [], 0.6),
        ("reply", "你看这个报错", [], 2.2),
        ("", "你好", [], 2),
        ("", "你看这个报错", [], 3),
        ("mention", "你看这个报错", [ImageRef(file="abc")], 0.6),
    ],
)
async def test_windows_and_four_states(
    settings, store, message, monkeypatch, reason, text, images, expected
):
    settings.group_debounce_seconds = 2
    settings.group_debounce_max_seconds = 5
    now = time.time()
    monkeypatch.setattr("hakimi.store.time.time", lambda: now)
    m = group(message, "10", text=text).model_copy(
        update={"attention_reason": reason, "images": images}
    )
    assert await store.groups.state(m) == "idle"
    await store.admit(m)
    assert await store.groups.state(m) == "debouncing"
    row = await store.one("SELECT ready_at,first_at FROM group_debounce_batches")
    assert row[0] - row[1] == pytest.approx(expected)
    now += expected + 0.01
    job = await store.claim()
    store.groups.begin(job)
    assert await store.groups.state(m) == "gate"
    store.groups.phase(m, "generating")
    assert await store.groups.state(m) == "generating"


async def test_selective_cancel_remerge_latest_history(settings, store, message):
    started, finish = asyncio.Event(), asyncio.Event()

    class Busy(ActionGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            started.set()
            await finish.wait()
            return self.text

    ai = Busy()
    service = Service(settings, store, ai, FakeBili(), ActionSender())
    first = group(message, "10")
    await service.accept(first)
    job = await store.claim()
    task = asyncio.create_task(service.process(job))
    await asyncio.wait_for(started.wait(), 2)
    other = group(message, "11", uid="30001")
    await service.accept(other)
    assert not store.groups.active[job["scope"]].cancel.is_set()
    await service.accept(group(message, "12", text="补充一下"))
    await asyncio.wait_for(task, 2)
    assert not await store.one("SELECT 1 FROM outbox")
    follow = await store.claim()
    assert {m["event_id"] for m in follow["batch_messages"]} == {"10", "11", "12"}
    assert any(
        "今天聊什么" in m["text"]
        for m in (await service.sessions.snapshot(first))["recent_messages"]
    )
    finish.set()
    await service.process(follow)
    assert await service.deliver_once()
    assert len(service.sender.sent) == 1


async def test_abstain_and_ack_only_history(settings, store, message):
    ai, sender = ActionGateway(action(abstain=True)), ActionSender()
    service = Service(settings, store, ai, FakeBili(), sender)
    await service.accept(message)
    await service.process(await store.claim())
    assert not await store.one("SELECT 1 FROM outbox")
    ai.text = action(bubbles=["我收到啦", "接着聊"], image_digest="截图里有红色报错")
    m = message.model_copy(update={"event_id": "two"})
    await service.accept(m)
    await service.process(await store.claim())
    assert not any(
        x["role"] == "assistant" for x in (await service.sessions.snapshot(m))["recent_messages"]
    )
    await service.deliver_once()
    history = (await service.sessions.snapshot(m))["recent_messages"]
    assert [x["text"] for x in history if x["role"] == "assistant"] == [
        "我收到啦",
        "接着聊",
        "[附图摘要，消息two：截图里有红色报错]",
    ]
    await recover_receipts(service)
    assert (await service.sessions.snapshot(m))["recent_messages"] == history


async def test_reanchor_and_recall_between_bubbles(settings, store, message, monkeypatch):
    service = Service(
        settings,
        store,
        ActionGateway(action(bubbles=["第一句", "第二句"], anchor={"reply_id": "10"})),
        FakeBili(),
        ActionSender(),
    )
    m = group(message, "10")
    await service.accept(m)
    await service.process(await store.claim())
    for n in range(5):
        service.hot.observe(group(message, str(20 + n), uid="30001"))

    async def pause(_):
        service.hot.recall(m, "10")

    monkeypatch.setattr("hakimi.outgoing.asyncio.sleep", pause)
    await service.deliver_once()
    assert len(service.sender.sent) == 1
    assert service.sender.sent[0][0] == {"type": "reply", "data": {"id": "10"}}
    assert (await store.one("SELECT status FROM outbox"))[0] == "cancelled"
    assert [
        h["text"]
        for h in (await service.sessions.snapshot(m))["recent_messages"]
        if h["role"] == "assistant"
    ] == ["第一句"]


async def test_no_drift_no_reply_and_uncertain_no_replay(settings, store, message):
    sender = ActionSender()
    sender.fail = True
    service = Service(
        settings, store, ActionGateway(action(anchor={"reply_id": "10"})), FakeBili(), sender
    )
    m = group(message, "10")
    await service.accept(m)
    await service.process(await store.claim())
    await service.deliver_once()
    assert all(p["type"] != "reply" for p in sender.sent[0])
    assert (await store.one("SELECT status FROM outbox"))[0] == "uncertain"
    assert not await service.deliver_once()
    assert not any(
        h["role"] == "assistant" for h in (await service.sessions.snapshot(m))["recent_messages"]
    )


def test_five_fields_unauthorized_targets(settings):
    settings.muted_user_ids = {"30003"}
    actions, _ = parse_actions(
        action(anchor={"reply_id": "forged", "mentions": ["all", "10000"]}),
        public=True,
        allowed_at={"all", "10000"},
        settings=settings,
    )
    assert not actions.anchor.mentions and actions.anchor.reply_id is None
    actions, _ = parse_actions(
        action(
            anchor={"mentions": ["30002", "30003"]},
            sticker={"label": "https://evil"},
            bubbles=["~" + "很长" * 40],
        ),
        public=True,
        allowed_at={"30003"},
        settings=settings,
    )
    assert not actions.anchor.mentions and actions.sticker.label is None
    assert actions.bubbles == ["~" + "很长" * 40]


@pytest.mark.parametrize("tier", [1, 2, 3])
async def test_folded_depth(settings, store, message, tier):
    ai = ActionGateway(action(bubbles=["细节" * 100]))
    ai.intent = RouteDecision(tier=tier)
    service = Service(settings, store, ai, FakeBili(), ActionSender())
    m = group(message, "10")
    await service.accept(m)
    await service.process(await store.claim())
    await service.deliver_once()
    assert len(service.sender.sent[0]["forward"]) == 200


async def test_quick_ack_local_under_one_second(settings, store, message):
    started = time.monotonic()
    signal = asyncio.Event()

    class Fast(ActionSender):
        async def quick_ack(self, message, text, *, before_send):
            assert await before_send()
            assert text in {
                "等我一下下，我仔细看看~",
                "稍等哦，我帮你理一下~",
                "唔，这个问题我想仔细点，等我几秒~",
            }
            assert time.monotonic() - started < 1
            signal.set()

    class Slow(ActionGateway):
        async def generate(self, *args, **kwargs):
            await asyncio.wait_for(signal.wait(), 0.9)
            return await super().generate(*args, **kwargs)

    ai = Slow()
    ai.intent = RouteDecision(tier=4)
    service = Service(settings, store, ai, FakeBili(), Fast())
    await service.accept(message)
    await service.process(await store.claim())
    assert signal.is_set()
    assert not any(
        h["role"] == "assistant"
        for h in (await service.sessions.snapshot(message))["recent_messages"]
    )


def raw_image(color="red", format="PNG"):
    buf = io.BytesIO()
    Image.new("RGB", (800, 300), color).save(buf, format=format)
    return buf.getvalue()


async def test_dual_image_cache_replay_and_old_reply(settings, store, message, monkeypatch):
    loader = ImageLoader(settings)
    calls = []

    async def napcat(ref):
        calls.append(ref.identity)
        return raw_image(), []

    monkeypatch.setattr(loader, "_napcat", napcat)
    ref = ImageRef(file="photo")
    a = (await loader.load_results([ref], scope="group"))[0]
    b = (await loader.load_results([ref], scope="group"))[0]
    assert a is b and calls == ["photo"]
    assert len(a.thumbnail) < len(a.data)
    ring = HotRing()
    m = group(message, "10").model_copy(update={"images": [ref]})
    ring.observe(m)
    for index in range(20):
        ring.observe(group(message, str(30 + index)))
    old = ring.lookup(m, "10")
    assert (await loader.load_results(old.images, scope="group"))[0] is a
    assert len(calls) == 1
    assert ring.lookup(message, "10") is None
    ring.recall(m, "10")
    assert ring.lookup(m, "10") is None


def test_gif_first_last_stitch_and_ring_caps(message):
    raw = io.BytesIO()
    Image.new("RGB", (100, 100), "red").save(
        raw, format="GIF", save_all=True, append_images=[Image.new("RGB", (100, 100), "blue")]
    )
    image = normalize_image(raw.getvalue(), 1000000)
    with Image.open(io.BytesIO(base64.b64decode(image.data))) as img:
        assert img.size == (200, 100)
        assert img.getpixel((50, 50))[0] > 200
        assert img.getpixel((150, 50))[2] > 200
    ring = HotRing(groups=2, byte_limit=4096)
    for n in range(400):
        ring.observe(group(message, str(n)))
    assert sum(r.size for r in ring.rings.values()) <= 4096


async def test_same_label_pool_ten_hashes_no_repeat(store):
    memes = MemeLibrary(store)
    for n in range(12):
        await memes.upload("开心.png", raw_image((n * 20, 0, 0)))
    seen = set()
    for _ in range(11):
        selected = await memes.choose("开心", "scope")
        assert selected["digest"] not in seen
        seen.add(selected["digest"])
        await memes.confirm_choice("scope", selected["digest"])
    assert len(await store.all("SELECT * FROM meme_sent_hashes")) == 10
    restarted = MemeLibrary(store)
    assert len(await restarted.list()) == 12


async def test_stream_completion_usage_and_cancellation():
    class Stream:
        closed = False

        async def aclose(self):
            self.closed = True

        async def __aiter__(self):
            yield NS(event_type="step.start", index=0, step=NS(type="model_output"))
            yield NS(event_type="step.delta", index=0, delta=NS(type="text", text=action()))
            yield NS(
                event_type="interaction.completed",
                interaction=NS(status="completed", usage=NS(total_input_tokens=5)),
            )

    stream = Stream()
    result = await consume_stream(stream)
    assert result.usage.total_input_tokens == 5 and stream.closed
    assert result.service_tier is None and result.usage_metadata is None
    event = asyncio.Event()
    stopped = asyncio.Event()

    async def work():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    task = asyncio.create_task(cancellable(work, event))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    event.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()


def test_notice_identity_and_no_command(settings):
    payload = {
        "self_id": "10000",
        "post_type": "notice",
        "notice_type": "notify",
        "sub_type": "poke",
        "user_id": 30000,
        "target_id": 10000,
        "group_id": 40000,
        "time": time.time(),
    }
    message = notice_message(payload, settings)
    assert message.addressed and message.command_text == ""
    with pytest.raises(Rejected):
        notice_message({**payload, "self_id": 99}, settings)


async def test_recall_persists_before_claim_and_skips_all_api(settings, store, message):
    service = Service(settings, store, ActionGateway(), FakeBili(), ActionSender())
    m = group(message, "123")
    await service.accept(m)
    result = await service.accept_event(
        {
            "post_type": "notice",
            "self_id": 10000,
            "notice_type": "group_recall",
            "group_id": 40000,
            "user_id": 30000,
            "message_id": 123,
            "time": time.time(),
        }
    )
    assert result == "recalled"
    service.hot = HotRing()  # Simulate process-local state loss while SQLite remains.
    await service.process(await store.claim())
    assert not service.ai.calls and not await store.one("SELECT 1 FROM outbox")


async def test_receipt_recovery_is_idempotent_and_group_public_index(settings, store, message):
    from hakimi.outgoing import persist_ack

    service = Service(settings, store, ActionGateway(), FakeBili(), ActionSender())
    now = time.time()
    service.world_state.clock = lambda: now
    await service.world_state.initialize()
    m = group(message, "123")
    epoch = (await service.sessions.snapshot(m))["epoch"]
    reply = {
        "id": "crash-before-b-commit",
        "message": m.model_dump(),
        "participants": [m.model_dump()],
    }
    await persist_ack(service, reply, 0, "已确认正文", {"message_id": 900}, epoch, "一只橘猫")
    assert await service.world_state.experience.battery(message, now) == 98.75
    assert not (await service.sessions.snapshot(m))["recent_messages"]
    await recover_receipts(service)
    original = (await service.sessions.snapshot(m))["recent_messages"]
    assert len(original) == 2
    assert len(await store.all("SELECT * FROM public_interactions")) == 1
    await recover_receipts(service)
    assert (await service.sessions.snapshot(m))["recent_messages"] == original
    await persist_ack(service, reply, 1, "第二个气泡", {"message_id": 901}, epoch)
    await recover_receipts(service)
    assert service.world_state.current.energy == 79.8
    assert await service.world_state.experience.battery(message, now) == 98.75
    with pytest.raises(Rejected):
        await persist_ack(service, dict(reply, id="unconfirmed"), 0, "未确认", {}, epoch)
    assert await store.one("SELECT 1 FROM world_changes WHERE receipt='qq:unconfirmed'") is None


@pytest.mark.parametrize("unsupported", [True, False])
async def test_forward_only_falls_back_on_explicit_unsupported(settings, message, unsupported):
    import httpx

    from hakimi.adapters import Sender

    calls = []

    class Wire:
        async def call(self, action, params):
            calls.append((action, params))
            if action == "send_group_forward_msg":
                raise Rejected(
                    "onebot_action_unsupported" if unsupported else "onebot_delivery_uncertain"
                )
            return {"message_id": 1}

    async def valid():
        return True

    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, Wire())
        if unsupported:
            await sender.send_folded(group(message, "1"), "深度技术说明" * 50, before_send=valid)
            assert calls[-1][0] == "send_group_msg"
            assert calls[-1][1]["message"][0]["data"]["file"].startswith("base64://")
        else:
            with pytest.raises(Rejected):
                await sender.send_folded(group(message, "1"), "文本" * 100, before_send=valid)
            assert len(calls) == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/x",
        "https://[::1]/",
        "http://169.254.169.254/latest",
        "http://user:secret@example.com/",
        "https://example.com:6099/",
        "file:///etc/passwd",
        "https://api.bilibili.com/x",
    ],
)
def test_preview_rejects_private_and_nonpublic_urls(url):
    from hakimi.link_preview import public_url

    assert not public_url(url)


async def test_preview_800ms_hard_timeout(monkeypatch):
    from hakimi.link_preview import preview_links

    class Slow:
        async def __aenter__(self):
            await asyncio.sleep(3)

        async def __aexit__(self, *args):
            pass

    class Client:
        def __init__(self, **kwargs):
            self.connector = kwargs["connector"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.connector.close()

        def get(self, *args, **kwargs):
            return Slow()

    monkeypatch.setattr("hakimi.link_preview.aiohttp.ClientSession", Client)
    start = time.monotonic()
    assert await preview_links("https://example.com/a") == []
    assert time.monotonic() - start < 0.95


async def test_preview_links_follows_safe_redirects_gzip_and_opengraph(monkeypatch):
    import gzip

    from hakimi.link_preview import preview_links

    html = (
        '<html><head><meta property="og:title" content="OpenGraph 标题">'
        '<meta name="og:description" content="OpenGraph 摘要内容"></head></html>'
    )
    gz = gzip.compress(html.encode("utf-8"))
    seen_urls = []

    class FakeContent:
        def __init__(self, data):
            self.data = data

        async def read(self, limit):
            return self.data[:limit]

    class FakeResp:
        def __init__(self, status, headers=None, body=b""):
            self.status = status
            self.headers = headers or {}
            self.content = FakeContent(body)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Client:
        def __init__(self, **kwargs):
            self.connector = kwargs["connector"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.connector.close()

        def get(self, url, **kwargs):
            seen_urls.append(url)
            if url == "https://short.example/go":
                return FakeResp(301, {"Location": "https://dest.example/article"})
            if url == "https://dest.example/article":
                return FakeResp(
                    200,
                    {"Content-Type": "text/html; charset=utf-8", "Content-Encoding": "gzip"},
                    gz,
                )
            if url == "https://evil.example/bounce":
                return FakeResp(302, {"Location": "http://127.0.0.1/admin"})
            return FakeResp(404)

    monkeypatch.setattr("hakimi.link_preview.aiohttp.ClientSession", Client)
    res = await preview_links(
        "看看这个 https://short.example/go，和这个 https://evil.example/bounce"
    )
    assert res == [{"title": "OpenGraph 标题", "description": "OpenGraph 摘要内容"}]
    assert "http://127.0.0.1/admin" not in seen_urls


def test_outgoing_pacing_delay_respects_send_delay_max(settings):
    from types import SimpleNamespace

    from hakimi.outgoing import _pacing_delay

    svc_zero = SimpleNamespace(
        s=settings.model_copy(update={"send_delay_min": 0, "send_delay_max": 0})
    )
    svc_live = SimpleNamespace(
        s=settings.model_copy(update={"send_delay_min": 0.8, "send_delay_max": 2.0})
    )
    assert _pacing_delay(svc_zero, 1.5) == 0.0
    assert _pacing_delay(svc_live, 1.5) == pytest.approx(1.5)


async def test_consume_stream_and_normalize_generate_content_preserve_service_tier():
    from hakimi.gemini import GeminiGateway

    class TierStream:
        closed = False

        async def aclose(self):
            self.closed = True

        async def __aiter__(self):
            yield NS(
                event_type="interaction.start",
                interaction=NS(service_tier="priority"),
            )
            yield NS(event_type="step.start", index=0, step=NS(type="model_output"))
            yield NS(event_type="step.delta", index=0, delta=NS(type="text", text="ok"))
            yield NS(
                event_type="interaction.completed",
                interaction=NS(
                    id="int-1",
                    status="completed",
                    usage=NS(total_input_tokens=10),
                    service_tier="standard",
                    usage_metadata=NS(traffic_type="ON_DEMAND"),
                ),
            )

    stream = TierStream()
    res = await consume_stream(stream)
    assert stream.closed
    assert res.service_tier == "standard"
    assert res.usage_metadata.traffic_type == "ON_DEMAND"
    assert GeminiGateway._vertex_downgraded_to_standard(res) is True

    # Also verify fallback to interaction.start service_tier when completed omits it.
    class StartOnlyTierStream:
        async def __aiter__(self):
            yield NS(event_type="interaction.start", interaction=NS(service_tier="standard"))
            yield NS(event_type="step.start", index=0, step=NS(type="model_output"))
            yield NS(event_type="step.delta", index=0, delta=NS(type="text", text="ok"))
            yield NS(
                event_type="interaction.completed",
                interaction=NS(id="int-2", status="completed", usage=NS(total_input_tokens=10)),
            )

    start_res = await consume_stream(StartOnlyTierStream())
    assert start_res.service_tier == "standard"
    assert GeminiGateway._vertex_downgraded_to_standard(start_res) is True

    # Verify _normalize_generate_content preserves service_tier across chunks.
    chunks = [
        NS(
            candidates=[
                NS(
                    content=NS(parts=[NS(text="hello", thought=False)]),
                    finish_reason=None,
                )
            ],
            usage_metadata=None,
            service_tier="standard",
        ),
        NS(
            candidates=[NS(content=NS(parts=[]), finish_reason="STOP")],
            usage_metadata=NS(
                prompt_token_count=10,
                candidates_token_count=5,
                total_token_count=15,
            ),
        ),
    ]
    gc_res = GeminiGateway._normalize_generate_content(chunks)
    assert gc_res.service_tier == "standard"
    assert gc_res.usage_metadata.total_token_count == 15
    assert GeminiGateway._vertex_downgraded_to_standard(gc_res) is True

    chunks_prio = [
        NS(
            candidates=[NS(content=NS(parts=[NS(text="hi", thought=False)]), finish_reason="STOP")],
            service_tier="priority",
        )
    ]
    gc_prio = GeminiGateway._normalize_generate_content(chunks_prio)
    assert gc_prio.service_tier == "priority"
    assert GeminiGateway._vertex_downgraded_to_standard(gc_prio) is False
