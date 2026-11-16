"""v1.2.0 regressions: adaptive images, digest budget, hot-ring recall, and cache-safe memes."""

import base64
import io
import json
import random
import time

import httpx
import pytest
from PIL import Image

from hakimi.adapters import onebot_message
from hakimi.gemini import GeminiGateway
from hakimi.media import ImageLoader, normalize_image
from hakimi.memory_compressor import check_memory_text
from hakimi.models import ImageRef
from hakimi.outbound_actions import MAX_IMAGE_DIGEST_CHARS, parse_actions
from hakimi.rich_message_parser import parse_forward, parse_message
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeSender
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_streaming_v0100 import ActionGateway, ActionSender, action


def group(
    message, event_id="g-1", *, uid="30000", nickname="群友A", text="聊聊", addressed=True, **extra
):
    updates = {
        "kind": "group",
        "conversation_id": "40000",
        "user_id": uid,
        "sender_nickname": nickname,
        "event_id": event_id,
        "text": text,
        "command_text": text,
        "addressed": addressed,
        "created_at": time.time(),
        **extra,
    }
    return message.model_copy(update=updates)


def _png_bytes(size=(120, 120), color="navy") -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, color).save(buf, format="PNG")
    return buf.getvalue()


def _gif_bytes(size=(800, 600)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, "red").save(
        buf,
        format="GIF",
        save_all=True,
        append_images=[Image.new("RGB", size, "blue")],
    )
    return buf.getvalue()


async def test_gate_loop_records_multi_bubble_reply_and_at_only_bystander_images(
    settings, store, message, monkeypatch
):
    settings.group_policies["onebot:40000"] = "at_only"
    ai = ActionGateway(action(bubbles=["把报错截图发我看看喵", "我帮你分析"]))
    sender = ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)

    async def fake_load(refs, *, scope="legacy"):
        return [normalize_image(_png_bytes(), settings.image_max_pixels) for _ in refs]

    monkeypatch.setattr(svc.images, "load_results", fake_load)

    now = time.time()
    q1 = group(message, "1001", text="我这边启动炸了", addressed=True, created_at=now)
    assert await svc.accept(q1) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()

    ctx_after_reply = await store.runtime.filters.group_context(
        group(message, "probe", created_at=now + 10)
    )
    assert any(
        line.startswith("[哈基米已回复] 把报错截图发我看看喵") and "我帮你分析" in line
        for line in ctx_after_reply
    )
    assert sum(1 for line in ctx_after_reply if line.startswith("[哈基米已回复]")) == 1

    bystander_img = group(
        message,
        "1002",
        uid="30001",
        text="",
        command_text="",
        addressed=False,
        images=[ImageRef(file="err-pic")],
        created_at=now + 2,
    )
    assert await svc.accept(bystander_img) == "ignored"
    bystander_empty = group(
        message,
        "1003",
        uid="30001",
        text="   ",
        command_text="   ",
        addressed=False,
        created_at=now + 3,
    )
    assert await svc.accept(bystander_empty) == "ignored"

    ctx_with_img = await store.runtime.filters.group_context(
        group(message, "probe2", created_at=now + 10)
    )
    assert "[附图]" in ctx_with_img
    assert "" not in ctx_with_img

    # Next smart_reply turn sees bot's own prompt and prior bystander image in gate context.
    settings.group_policies["onebot:40000"] = "smart_reply"
    ai.calls.clear()
    followup_img = group(
        message,
        "1004",
        uid="30002",
        text="",
        command_text="",
        addressed=False,
        images=[ImageRef(file="next-err-pic")],
        created_at=now + 4,
    )
    assert await svc.accept(followup_img) == "accepted"
    await svc.process(await store.claim())
    msg_screen = next(c for c in ai.calls if c[0] == "screen" and c[3].get("purpose") == "message")
    rel = msg_screen[3]["relevance_context"]
    assert any(line.startswith("[哈基米已回复]") for line in rel) and "[附图]" in rel
    await svc.stop()


async def test_image_digest_allows_120_chars_and_blocks_secrets(settings, store, message):
    assert MAX_IMAGE_DIGEST_CHARS == 120
    digest_110 = (
        "终端截图显示Traceback在service.py第482行抛出KeyError:'cfg'，"
        "上方提示Redis连接超时3002ms，右侧窗口开着Docker日志监控面板并持续打印重连失败堆栈。"
    )
    assert 90 < len(digest_110) <= 120
    digest_150 = digest_110 + "还有一长串多余尾部信息" * 5

    ai = ActionGateway(action(bubbles=["看到了~"], image_digest=digest_150))
    svc = Service(settings, store, ai, FakeBili(), ActionSender())
    m = message.model_copy(update={"event_id": "d1"})
    await svc.accept(m)
    await svc.process(await store.claim())
    await svc.deliver_once()
    hist = (await svc.sessions.snapshot(m))["recent_messages"]
    digest_line = next(x["text"] for x in hist if "[附图摘要，消息d1：" in x["text"])
    saved = digest_line.removeprefix("[附图摘要，消息d1：").removesuffix("]")
    assert len(saved) == 120
    assert saved == digest_150[:120]

    parsed, _ = parse_actions(action(bubbles=["ok"], image_digest=f" {digest_110} "), public=True)
    assert parsed.image_digest == digest_110

    with pytest.raises(Rejected, match="credential_in_memory"):
        check_memory_text("截图显示 api_key=sk-1234567890abcdef123456", settings)
    await svc.stop()


def test_adaptive_image_normalization_and_clearer_gate_thumbnails():
    tall = normalize_image(_png_bytes((1080, 3600)), 50_000_000)
    assert tall.resolution == "high" and not tall.tiles
    with Image.open(io.BytesIO(base64.b64decode(tall.data))) as im:
        assert im.size == (768, 2560)
    with Image.open(io.BytesIO(base64.b64decode(tall.gate().data))) as g_im:
        assert g_im.size == (154, 512)
        assert tall.gate().block()["resolution"] == "low"

    desktop = normalize_image(_png_bytes((2560, 1440)), 50_000_000)
    assert desktop.resolution == "high"
    with Image.open(io.BytesIO(base64.b64decode(desktop.data))) as im:
        assert im.size == (1600, 900)
    with Image.open(io.BytesIO(base64.b64decode(desktop.gate().data))) as g_im:
        assert g_im.size == (448, 252)
        assert desktop.gate().block()["resolution"] == "low"

    small = normalize_image(_png_bytes((800, 600)), 50_000_000)
    assert small.resolution == "medium"
    with Image.open(io.BytesIO(base64.b64decode(small.data))) as im:
        assert im.size == (800, 600)

    gif = normalize_image(_gif_bytes((800, 600)), 50_000_000)
    assert gif.resolution == "medium"
    with Image.open(io.BytesIO(base64.b64decode(gif.data))) as im:
        assert im.size == (1152, 432)


async def test_nearby_prefers_non_stickers_and_recalls_prior_hot_ring_images(
    settings, store, message, monkeypatch
):
    settings.group_policies["onebot:40000"] = "at_only"
    ai = ActionGateway(action(bubbles=["看见截图里报了错~"]))
    ai.intent = RouteDecision("chat", tier=1)
    sender = ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)

    loader = ImageLoader(settings)
    monkeypatch.setattr(loader, "_one", lambda _client, ref: _async_model_image(ref.identity))
    svc.images = loader

    async def _async_model_image(ident):
        size = (1080, 2400) if "real" in ident else (120, 120)
        return normalize_image(_png_bytes(size), 12_000_000)

    # 1. Nearby has a real screenshot followed by a sticker; non-sticker wins the 1-slot test.
    msg_real = group(
        message,
        "2001",
        uid="30001",
        nickname="老王",
        addressed=False,
        text="这是报错截图",
        images=[ImageRef(file="real-err")],
        created_at=time.time() - 30,
    )
    msg_sticker = group(
        message,
        "2002",
        uid="30002",
        nickname="小李",
        addressed=False,
        text="[商城贴纸]",
        images=[ImageRef(file="sticker-meme", is_sticker=True)],
        created_at=time.time() - 25,
    )
    await svc.accept(msg_real)
    await svc.accept(msg_sticker)
    imgs, srcs = await svc.message_images(
        group(message, "2003"), [], [msg_real, msg_sticker], limit=1, include_current=False
    )
    assert len(imgs) == 1 and imgs[0].resolution == "high"
    assert "老王" in srcs[0]["sender"]

    # 2. Bury msg_real behind 8 plain text messages so it falls outside 6-message nearby window.
    for i in range(8):
        chatter = group(
            message,
            f"spam-{i}",
            uid="30003",
            nickname="水友",
            addressed=False,
            text=f"水群消息{i}",
            created_at=time.time() - 20 + i,
        )
        assert await svc.accept(chatter) == "ignored"

    # Recall msg_sticker so recalled image is never chosen either; msg_real remains valid.
    await svc.accept_event(
        {
            "post_type": "notice",
            "notice_type": "group_recall",
            "self_id": int(settings.onebot_self_id),
            "group_id": 40000,
            "user_id": 30002,
            "message_id": 2002,
        }
    )

    ai.calls.clear()
    q_recall = group(
        message,
        "2010",
        uid="30000",
        nickname="提问者",
        addressed=True,
        text="帮我看下刚才老王发的那张报错图写的啥",
        created_at=time.time(),
    )
    assert await svc.accept(q_recall) == "accepted"
    job1 = await store.claim()
    await svc.process(job1)
    await store.finish(job1["id"])
    assert await svc.deliver_once()

    purposes = [(c[0], c[3].get("purpose")) for c in ai.calls]
    assert purposes == [("screen", "message"), ("screen", "source"), ("generate", None)]
    assert "historical_image_recall" in ai.calls[1][1]
    assert ai.calls[1][3]["images"][0].block()["resolution"] == "low"
    gen_call = ai.calls[2]
    assert len(gen_call[3]["images"]) == 1
    assert gen_call[3]["images"][0].resolution == "high"
    assert gen_call[3]["route"].tier >= 2
    assert gen_call[1]["image_sources"][0]["origin"] == "历史消息附图"
    assert "老王" in gen_call[1]["image_sources"][0]["sender"]

    # 3. Once msg_real is recalled, it must never be recalled into context.
    await svc.accept_event(
        {
            "post_type": "notice",
            "notice_type": "group_recall",
            "self_id": int(settings.onebot_self_id),
            "group_id": 40000,
            "user_id": 30001,
            "message_id": 2001,
        }
    )
    ai.calls.clear()
    q_after_recall = group(
        message,
        "2011",
        uid="30000",
        addressed=True,
        text="再看下刚才那张图",
        created_at=time.time() + 1,
    )
    assert await svc.accept(q_after_recall) == "accepted"
    job2 = await store.claim()
    await svc.process(job2)
    await store.finish(job2["id"])
    assert await svc.deliver_once()
    assert [(c[0], c[3].get("purpose")) for c in ai.calls] == [
        ("screen", "message"),
        ("generate", None),
    ]
    assert "images" not in ai.calls[1][3]
    await svc.stop()


async def test_pure_image_extends_debounce_and_forward_truncates_over_two_images(
    settings, store, message, monkeypatch
):
    now = time.time()
    monkeypatch.setattr("hakimi.store.time.time", lambda: now)
    store.groups.clock = lambda: now
    settings.group_debounce_seconds = 0.8
    settings.mention_debounce_seconds = 0.6
    settings.ordinary_incomplete_seconds = 3.0
    settings.mention_incomplete_seconds = 2.2
    settings.group_debounce_max_seconds = 5.0

    # Pure image parsed from OneBot array has text="[图片]" and command_text="".
    pure_img = onebot_message(
        {
            "post_type": "message",
            "self_id": int(settings.onebot_self_id),
            "user_id": 30000,
            "message_type": "group",
            "group_id": 40000,
            "message_id": 5001,
            "time": now,
            "message": [
                {"type": "image", "data": {"url": "https://gchat.qpic.cn/p1.png", "subType": 1}},
            ],
        },
        settings,
    )
    assert pure_img.images[0].is_sticker is True
    assert store.groups.delay(pure_img) == pytest.approx(3.0)

    await store.admit(pure_img)
    now += 1.5
    assert await store.claim() is None  # Still waiting for follow-up explanation.
    followup = group(
        message,
        "5002",
        uid="30000",
        text="刚才忘了配字，帮忙看下",
        addressed=True,
        created_at=now,
    )
    await store.admit(followup)
    now += 1.0
    job = await store.claim()
    assert job is not None and len(job["batch_messages"]) == 2
    await store.finish(job["id"])

    # Forward message with >2 images gracefully keeps first 2 and marks truncated.
    fwd = parse_forward(
        {
            "messages": [
                {
                    "sender": {"user_id": "30001", "nickname": "A"},
                    "content": [
                        {"type": "image", "data": {"url": f"https://gchat.qpic.cn/f{i}.png"}}
                    ],
                }
                for i in range(3)
            ]
        },
        settings.onebot_self_id,
        include_authors=True,
    )
    assert len(fwd.images) == 2 and fwd.truncated is True

    class ForwardSender(FakeSender):
        async def forward(self, forward_id):
            return {
                "messages": [
                    {
                        "sender": {"user_id": "30001", "nickname": "A"},
                        "content": [
                            {"type": "image", "data": {"url": f"https://gchat.qpic.cn/x{i}.png"}}
                        ],
                    }
                    for i in range(3)
                ]
            }

    svc = Service(settings, store, ActionGateway(), FakeBili(), ForwardSender())
    svc.sender = ForwardSender()
    expanded = await svc.expand_forwards(
        group(
            message,
            "5003",
            text="看下转发",
            forward_ids=["fwd-3-imgs"],
            images=[ImageRef(url="https://gchat.qpic.cn/main.png")],
        )
    )
    assert len(expanded.images) == 2
    assert expanded.images[0].url == "https://gchat.qpic.cn/main.png"
    assert "truncated" in expanded.context_status

    # Multi-image single segment in OneBot array sets is_sticker on subType=1 vs 0 and truncated.
    rich_multi = parse_message(
        [
            {"type": "image", "data": {"file": "a.jpg", "subType": 0}},
            {"type": "image", "data": {"file": "b.jpg", "subType": 2}},
            {"type": "image", "data": {"file": "c.jpg"}},
        ],
        settings.onebot_self_id,
    )
    assert [i.is_sticker for i in rich_multi.images] == [False, True]
    assert rich_multi.truncated is True
    await svc.stop()


async def test_meme_hot_update_preserves_interaction_chain_and_prefix_hash(
    settings, store, message, sdk_pair
):
    recorded = []
    turn = {"n": 0}

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        recorded.append(body)
        turn["n"] += 1
        resp = interaction_response()
        resp["id"] = f"inter_meme_{turn['n']}"
        resp["steps"][0]["content"][0]["text"] = action(bubbles=[f"回复{turn['n']}"])
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    svc = Service(settings, store, gate, FakeBili(), ActionSender())
    await svc.memes.initialize()
    settings.group_policies = {"onebot:40000": "at_only"}

    m1 = group(message, "6001", text="你好哈基米", addressed=True)
    assert await svc.accept(m1) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()
    assert (await svc.sessions.snapshot(m1))["interaction_chains"]["hydrogen"][
        "interaction_id"
    ] == "inter_meme_1"

    await svc.memes.upload("无语盯.png", _png_bytes((120, 120), "green"))

    m2 = group(message, "6002", text="刚传了个新表情", addressed=True)
    assert await svc.accept(m2) == "accepted"
    await svc.process(await store.claim())

    assert len(recorded) == 2
    assert recorded[1]["system_instruction"] == recorded[0]["system_instruction"]
    assert recorded[1]["previous_interaction_id"] == "inter_meme_1"
    raw_input = recorded[1]["input"][0]
    text_wire = raw_input["content"][0]["text"] if "content" in raw_input else raw_input["text"]
    assert '"meme_labels":["无语盯"]' in text_wire
    await svc.stop()


async def test_neighbour_image_deduplicated_after_reply_while_explicit_recall_works(
    settings, store, message, monkeypatch
):
    settings.group_policies["onebot:40000"] = "at_only"
    ai = ActionGateway(action(bubbles=["看到了，这是配置报错"]))
    sender = ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)

    async def fake_load(refs, *, scope="legacy"):
        return [normalize_image(_png_bytes(), settings.image_max_pixels) for _ in refs]

    monkeypatch.setattr(svc.images, "load_results", fake_load)

    now = time.time()
    bystander = group(
        message,
        "2001",
        uid="30001",
        text="",
        command_text="",
        addressed=False,
        images=[ImageRef(file="bystander-pic")],
        created_at=now,
    )
    assert await svc.accept(bystander) == "ignored"

    q1 = group(message, "2002", text="帮我看下这张报错图", addressed=True, created_at=now + 1)
    assert await svc.accept(q1) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()
    assert len(ai.calls) >= 2
    assert any(c[0] == "screen" and c[3].get("purpose") == "source" for c in ai.calls)
    gen1 = next(c for c in ai.calls if c[0] == "generate")
    assert len(gen1[3].get("images", [])) == 1

    # Next plain-text turn within 6 messages must NOT re-screen or re-upload already answered image.
    ai.calls.clear()
    q2 = group(
        message, "2003", text="原来如此，那应该怎么改配置", addressed=True, created_at=now + 2
    )
    assert await svc.accept(q2) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()
    assert not any(c[0] == "screen" and c[3].get("purpose") == "source" for c in ai.calls)
    gen2 = next(c for c in ai.calls if c[0] == "generate")
    assert not gen2[3].get("images")

    # Explicit recall phrase ("刚才那张图") within 6 messages still recalls via HotRing.
    ai.calls.clear()
    q3 = group(
        message,
        "2004",
        text="再仔细看下刚才那张图右上角写的啥",
        addressed=True,
        created_at=now + 3,
    )
    assert await svc.accept(q3) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()
    assert any(c[0] == "screen" and c[3].get("purpose") == "source" for c in ai.calls)
    gen3 = next(c for c in ai.calls if c[0] == "generate")
    assert len(gen3[3].get("images", [])) == 1
    await svc.stop()


async def test_high_megapixel_draft_jpeg_byte_cap_and_ultra_tall_auto_split(
    settings, store, message, monkeypatch
):
    from hakimi.media import MAX_ENCODED_JPEG_BYTES

    big_jpeg = io.BytesIO()
    Image.new("RGB", (6000, 5000), "teal").save(big_jpeg, format="JPEG", quality=75)
    norm_30mp = normalize_image(big_jpeg.getvalue(), settings.image_max_pixels)
    assert norm_30mp.resolution == "high"
    with Image.open(io.BytesIO(base64.b64decode(norm_30mp.data))) as im:
        assert im.size == (1440, 1200)

    # Dense 2x2-blocky RGB texture exceeds 800 KiB at q=85 subsampling=0 and steps down below cap.
    rng = random.Random(42)
    dense = Image.frombytes("RGB", (800, 600), rng.randbytes(800 * 600 * 3)).resize(
        (1600, 1200), Image.Resampling.NEAREST
    )
    half_clean = Image.new("RGB", (1600, 1200), "white")
    half_clean.paste(dense.crop((0, 0, 1600, 520)), (0, 0))
    dense_png = io.BytesIO()
    half_clean.save(dense_png, format="PNG")
    norm_dense = normalize_image(dense_png.getvalue(), settings.image_max_pixels)
    assert len(base64.b64decode(norm_dense.data)) <= MAX_ENCODED_JPEG_BYTES

    # Ultra-tall screenshot (aspect > 4.5) expands to 2 overlapping tiles when limit=2.
    ultra_model = normalize_image(_png_bytes((1080, 7200)), settings.image_max_pixels)
    assert len(ultra_model.tiles) == 2

    svc = Service(settings, store, ActionGateway(), FakeBili(), ActionSender())

    async def fake_load(refs, *, scope="legacy"):
        return [ultra_model for _ in refs]

    monkeypatch.setattr(svc.images, "load_results", fake_load)
    msg = group(
        message, "7001", text="看长图", images=[ImageRef(file="ultra-long")], addressed=True
    )

    selected_2: list = []
    imgs_2, srcs_2 = await svc.message_images(msg, [msg], [], limit=2, selected_members=selected_2)
    assert len(imgs_2) == 2 and len(selected_2) == 1
    assert srcs_2[0]["slice"] == "1/2" and srcs_2[1]["slice"] == "2/2"
    for tile in imgs_2:
        with Image.open(io.BytesIO(base64.b64decode(tile.data))) as tim:
            assert tim.width >= 700 and tim.height == 2560

    imgs_1, srcs_1 = await svc.message_images(msg, [msg], [], limit=1)
    assert len(imgs_1) == 1 and "slice" not in srcs_1[0]
    await svc.stop()
