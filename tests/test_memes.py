import asyncio
import base64
import io
import os
from urllib.parse import quote

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.memes import (
    MemeLibrary,
    explicit_meme_request,
    extract_meme,
    label_from_filename,
    normalize_meme,
)
from hakimi.outbound_actions import CHAT_RESPONSE_FORMAT, ChatActions
from hakimi.security import Rejected
from tests.fakes import FakeGateway


def picture(fmt="PNG", size=(40, 30), animated=False):
    target = io.BytesIO()
    image = Image.new("RGB", size, "purple")
    if animated:
        image.save(
            target,
            format=fmt,
            save_all=True,
            append_images=[Image.new("RGB", size, "yellow")],
            duration=120,
            loop=0,
        )
    else:
        image.save(target, format=fmt)
    return target.getvalue()


@pytest.mark.parametrize("fmt", ["JPEG", "PNG", "GIF", "WEBP"])
def test_upload_decodes_real_images_and_removes_metadata(fmt):
    normalized = normalize_meme(picture(fmt, (1600, 800)))
    with Image.open(io.BytesIO(normalized.data)) as image:
        assert image.format == "JPEG"
        assert image.size == (1024, 512)
        assert "exif" not in image.info and "icc_profile" not in image.info


@pytest.mark.parametrize("fmt", ["GIF", "WEBP"])
def test_animated_gif_and_webp_preserve_animation_with_transparent_alpha(fmt):
    raw_buf = io.BytesIO()
    f1 = Image.new("RGBA", (600, 400), (120, 40, 200, 0))
    f2 = Image.new("RGBA", (600, 400), (240, 180, 20, 255))
    f1.save(
        raw_buf,
        format=fmt,
        save_all=True,
        append_images=[f2],
        duration=[90, 140],
        loop=0,
    )
    result = normalize_meme(raw_buf.getvalue())
    assert result.was_animated is True
    assert result.first_frame_only is False
    assert result.mime_type == "image/gif"
    assert result.extension == ".gif"
    with Image.open(io.BytesIO(result.data)) as image:
        assert image.format == "GIF"
        assert getattr(image, "is_animated", False) is True
        assert image.n_frames == 2
        assert image.size[0] <= 512 and image.size[1] <= 512


def test_transparent_png_is_preserved_while_opaque_png_stays_jpeg():
    rgba_buf = io.BytesIO()
    rgba = Image.new("RGBA", (80, 80), (0, 0, 0, 0))
    rgba.paste((255, 80, 120, 255), (20, 20, 60, 60))
    rgba.save(rgba_buf, format="PNG")
    res_alpha = normalize_meme(rgba_buf.getvalue())
    assert res_alpha.mime_type == "image/png"
    assert res_alpha.extension == ".png"
    assert res_alpha.first_frame_only is False
    with Image.open(io.BytesIO(res_alpha.data)) as img:
        assert img.format == "PNG"
        assert img.convert("RGBA").getchannel("A").getextrema()[0] == 0

    res_opaque = normalize_meme(picture("PNG"))
    assert res_opaque.mime_type == "image/jpeg"
    assert res_opaque.extension == ".jpg"


@pytest.mark.parametrize(
    "filename",
    [
        "../x.jpg",
        "x/y.jpg",
        "x\\y.jpg",
        "[MEME:x].jpg",
        "x\n.jpg",
        ".jpg",
        "https://evil.invalid/x.jpg",
        "无语<script>.jpg",
        "a" * 25 + ".jpg",
        "x.svg",
    ],
)
def test_label_rejects_paths_and_prompt_markup(filename):
    with pytest.raises(Rejected, match="invalid_meme_filename"):
        label_from_filename(filename)


def test_rejects_oversize_bombs_wrong_formats_and_invalid_images(monkeypatch):
    for raw in (b"not an image", picture("BMP")):
        with pytest.raises(Rejected):
            normalize_meme(raw)
    monkeypatch.setattr("hakimi.memes.MAX_PIXELS", 10)
    with pytest.raises(Rejected, match="meme_image_dimensions"):
        normalize_meme(picture())
    monkeypatch.setattr("hakimi.memes.MAX_UPLOAD_BYTES", 1)
    with pytest.raises(Rejected, match="meme_upload_too_large"):
        normalize_meme(picture())


async def test_library_stable_static_prefix_restart_hot_revoke_and_base64(store):
    library = MemeLibrary(store)
    await library.initialize()
    first = await library.upload("无语盯.png", picture())
    await library.upload("A.webp", picture("WEBP"))
    assert library.labels == ("A", "无语盯")
    assert "meme_labels" in library.protocol_prompt
    restart = MemeLibrary(store)
    await restart.initialize()
    assert restart.labels == library.labels
    assert restart.protocol_prompt == library.protocol_prompt
    assert await restart.list() == await library.list()
    raw, mime = await library.preview(first["id"])
    assert mime == "image/jpeg"
    assert base64.b64decode((await library.get_base64("无语盯"))[9:]) == raw
    assert library.root.stat().st_mode & 0o777 == 0o700
    assert (library.root / (first["id"] + ".jpg")).stat().st_mode & 0o777 == 0o600
    (library.root / "manual.jpg").write_bytes(picture())
    assert await library.get_base64("manual") is None
    await library.delete(first["id"])
    assert await library.get_base64("无语盯") is None
    assert "无语盯" not in library.labels
    assert not (library.root / (first["id"] + ".jpg")).exists()
    await restart.delete((await restart.list())[0]["id"])


@pytest.mark.parametrize("attack", ["symlink", "hardlink", "tamper", "fifo"])
async def test_file_substitution_cannot_send_unregistered_bytes(store, tmp_path, attack):
    library = MemeLibrary(store)
    item = await library.upload("无语.png", picture())
    path = library.root / (item["id"] + ".jpg")
    outside = tmp_path / "sensitive.jpg"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    if attack == "symlink":
        path.symlink_to(outside)
    elif attack == "hardlink":
        os.link(outside, path)
    elif attack == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(b"x" * outside.stat().st_size)
    assert await asyncio.wait_for(library.get_base64("无语"), 2) is None
    with pytest.raises(Rejected, match="meme_file_unavailable"):
        await library.preview(item["id"])
    assert outside.read_bytes()


async def test_root_symlink_is_never_followed(store, tmp_path):
    library = MemeLibrary(store)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    library.root.symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(OSError):
        await library.initialize()
    assert not list(elsewhere.iterdir())


async def test_duplicate_concurrent_uploads_and_quotas(store, monkeypatch):
    library = MemeLibrary(store)
    results = await asyncio.gather(
        library.upload("微笑.png", picture()),
        library.upload("微笑.png", picture()),
        return_exceptions=True,
    )
    assert sum(isinstance(r, dict) for r in results) == 1
    assert sum(isinstance(r, Rejected) for r in results) == 1
    assert len(await library.list()) == 1
    monkeypatch.setattr("hakimi.memes.MAX_MEMES", 1)
    with pytest.raises(Rejected, match="meme_library_full"):
        await library.upload("另一个.png", picture())
    monkeypatch.setattr("hakimi.memes.MAX_MEMES", 100)
    monkeypatch.setattr("hakimi.memes.MAX_LIBRARY_BYTES", 1)
    with pytest.raises(Rejected, match="meme_library_full"):
        await library.upload("另一个.png", picture())


@pytest.mark.parametrize(
    "value,clean,tag",
    [
        ("有点离谱[MEME:无语盯]", "有点离谱", "无语盯"),
        ("[MEME:不存在]", "", "不存在"),
        ("[MEME:https://example.invalid/x.jpg]", "", None),
        ("[MEME:../秘密]", "", None),
        ("[MEME:无语]后面还有话", "后面还有话", None),
        ("[MEME:无语][MEME:盯]", "", None),
        ("[meme:无语]", "", None),
        ("文本[MEME:无语", "文本", None),
    ],
)
def test_strict_single_trailing_protocol(value, clean, tag):
    assert extract_meme(value) == (clean, tag)


async def test_frequency_counts_all_replies_persistently_and_hides_ids(store):
    library = MemeLibrary(store)
    await library.upload("无语.png", picture())
    assert await library.reserve_reply("group:40000", "r1", "无语")
    assert not await library.reserve_reply("group:40000", "r1", "无语")
    assert not await library.reserve_reply("group:40000", "r2", "无语")
    assert not await library.reserve_reply("group:40000", "r3", None)
    restart = MemeLibrary(store)
    assert not await restart.reserve_reply("group:40000", "r4", "不存在")
    assert await restart.reserve_reply("group:40000", "r5", "无语")
    assert await restart.reserve_reply("group:50000", "r1", "无语")
    state = str([dict(row) for row in await store.all("SELECT * FROM meme_reply_frequency")])
    assert "40000" not in state and "50000" not in state


@pytest.fixture
async def meme_api(settings):
    settings.admin_token = SecretStr("test-meme-admin-token")
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer test-meme-admin-token"},
        ) as client:
            yield app, client, gateway


async def test_admin_upload_preview_delete_authenticated_without_model_calls(meme_api):
    app, client, gateway = meme_api
    upload = await client.post(
        "/admin/api/memes/upload/" + quote("无语.png"),
        content=picture(),
        headers={"Content-Type": "image/png"},
    )
    assert upload.status_code == 200, upload.text
    key = upload.json()["id"]
    listing = (await client.get("/admin/api/memes")).json()
    assert listing["items"][0]["label"] == "无语"
    assert "filename" not in listing["items"][0]
    image = await client.get(f"/admin/api/memes/{key}/image")
    assert image.status_code == 200 and image.headers["content-type"] == "image/jpeg"
    assert image.headers["cache-control"] == "no-store"
    assert "img-src blob:" in image.headers["content-security-policy"]
    assert (
        await client.get(f"/admin/api/memes/{key}/image", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (await client.delete(f"/admin/api/memes/{key}")).status_code == 200
    assert (await client.get(f"/admin/api/memes/{key}/image")).status_code == 404
    assert not gateway.calls
    assert not app.state.service.memes.labels


async def test_admin_guards_stream_uploads_before_processing(meme_api):
    app, client, _ = meme_api
    url = "/admin/api/memes/upload/x.png"
    assert (
        await client.post(
            url,
            content=b"x",
            headers={"Authorization": "Bearer wrong", "Content-Type": "image/png"},
        )
    ).status_code == 401
    assert (
        await client.post(
            url,
            content=b"x",
            headers={"Content-Type": "image/png", "Content-Length": str(6 * 1024 * 1024)},
        )
    ).status_code == 413
    assert (
        await client.post(url, content=b"x", headers={"Content-Type": "image/svg+xml"})
    ).status_code == 415
    app.state.settings.admin_token = SecretStr("")
    # AdminAuth snapshots token at construction; create a fresh disabled guard.
    from hakimi.admin_auth import AdminAuth

    app.state.admin_auth = AdminAuth(app.state.settings)
    assert (await client.get("/admin/api/memes")).status_code == 403


async def test_owner_save_meme_command_from_quoted_message_only_allows_owner(
    settings, store, message, monkeypatch
):
    from hakimi.adapters import onebot_message
    from hakimi.security import Rejected
    from hakimi.service import Service
    from tests.fakes import FakeBili, FakeGateway, FakeSender

    settings.onebot_allowed_groups = {"40000"}
    settings.owner_qq_id = "20000"
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:

        async def fake_fetch_raw(ref, *, max_bytes=None):
            return picture()

        monkeypatch.setattr(svc.images, "fetch_raw", fake_fetch_raw)
        non_owner_msg = onebot_message(
            {
                "post_type": "message",
                "message_type": "group",
                "self_id": settings.onebot_self_id,
                "user_id": 30000,
                "group_id": 40000,
                "message_id": 9001,
                "time": int(__import__("time").time()),
                "message": [
                    {"type": "image", "data": {"url": "https://gchat.qpic.cn/test.jpg"}},
                    {"type": "text", "data": {"text": "/存表情 偷图"}},
                ],
            },
            settings,
        )
        assert non_owner_msg.attention_reason == "command"
        with pytest.raises(Rejected, match="group_owner_only"):
            await svc.command(non_owner_msg)
        assert not svc.memes.labels

        owner_msg = onebot_message(
            {
                "post_type": "message",
                "message_type": "group",
                "self_id": settings.onebot_self_id,
                "user_id": 20000,
                "group_id": 40000,
                "message_id": 9002,
                "time": int(__import__("time").time()),
                "message": [
                    {"type": "image", "data": {"url": "https://gchat.qpic.cn/good.jpg"}},
                    {"type": "text", "data": {"text": "/存表情 疑惑"}},
                ],
            },
            settings,
        )
        reply = await svc.command(owner_msg)
        assert "疑惑" in reply
        assert "疑惑" in svc.memes.labels
        dup_reply = await svc.command(owner_msg)
        assert "已经有这张图" in dup_reply
    finally:
        await svc.stop()


async def test_single_image_anti_deadlock_native_sticker_subtype_and_owner_unthrottled(
    settings, store, message
):
    from hakimi.adapters import Sender
    from hakimi.service import Service
    from tests.fakes import FakeBili, FakeGateway
    from tests.test_social_integration import Wire, group, process
    from tests.test_streaming_v0100 import action

    # 1) Single-image label never deadlocks after repeated confirm_choice
    lib = MemeLibrary(store)
    await lib.upload_bytes("独苗喵", picture("PNG"))
    for _ in range(12):
        picked = await lib.choose("独苗喵", "scope-single")
        assert picked is not None
        await lib.confirm_choice("scope-single", picked["digest"])

    # 2) Native sub_type=1 + summary="[动画表情]" on sticker vs omitted on send_card
    settings.owner_qq_id = "20000"
    gif_buf = io.BytesIO()
    Image.new("RGBA", (60, 60), (10, 200, 120, 0)).save(
        gif_buf,
        format="GIF",
        save_all=True,
        append_images=[Image.new("RGBA", (60, 60), (220, 50, 90, 255))],
        duration=100,
        loop=0,
    )

    class StickerGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return action(
                abstain=False,
                bubbles=[""],
                sticker={"label": "动图喵", "timing": "after_text"},
            )

    wire = Wire()
    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, wire)
        svc = Service(settings, store, StickerGw(), FakeBili(), sender)
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            await svc.memes.upload_bytes("动图喵", gif_buf.getvalue())

            # Back-to-back pure-sticker Owner DMs are never throttled
            for n in range(2):
                m = message.model_copy(update={"event_id": f"dm-s-{n}", "text": "发个表情包"})
                await process(svc, m)
                assert await svc.deliver_once()

            dm_calls = [p for act, p in wire.calls if act == "send_private_msg"]
            assert len(dm_calls) == 2
            for params in dm_calls:
                seg = params["message"][0]
                assert seg["type"] == "image"
                assert seg["data"]["file"].startswith("base64://")
                assert seg["data"]["summary"] == "[动画表情]"
                assert seg["data"]["sub_type"] == 1

            # send_card keeps full-size normal image (no sub_type=1)
            wire.calls.clear()
            grp = group(message, "card-1")
            await sender.send_card(grp, "| A | B |\n| --- | --- |\n| 1 | 2 |")
            card_seg = wire.calls[-1][1]["message"][0]
            assert card_seg["type"] == "image"
            assert "sub_type" not in card_seg["data"]
        finally:
            await svc.stop()


async def test_chat_schema_required_fields_and_group_explicit_sticker_bypass(
    settings, store, message
):
    from hakimi.adapters import Sender
    from hakimi.service import Service
    from tests.fakes import FakeBili, FakeGateway
    from tests.test_social_integration import Wire, group, process
    from tests.test_streaming_v0100 import action

    assert set(CHAT_RESPONSE_FORMAT["required"]) == set(ChatActions.model_fields)
    assert set(CHAT_RESPONSE_FORMAT["$defs"]["Sticker"]["required"]) == {"label", "timing"}
    assert set(CHAT_RESPONSE_FORMAT["$defs"]["Anchor"]["required"]) == {"reply_id", "mentions"}

    assert explicit_meme_request("@吉米奈喵 发点表情包")
    assert explicit_meme_request("货呢")
    assert explicit_meme_request("来张猫猫图")
    assert explicit_meme_request("图呢")
    assert not explicit_meme_request("发张截图看看")
    assert not explicit_meme_request("看下报错图")

    class ScriptedStickerGw(FakeGateway):
        def __init__(self):
            super().__init__()
            self.replies = iter(
                [
                    action(
                        abstain=False,
                        bubbles=["哈喽"],
                        sticker={"label": "滑稽", "timing": "after_text"},
                    ),
                    action(
                        abstain=False,
                        bubbles=["继续聊聊"],
                        sticker={"label": "滑稽", "timing": "after_text"},
                    ),
                    action(
                        abstain=False,
                        bubbles=["接好，刚翻出来的存货"],
                        sticker={"label": "滑稽", "timing": "after_text"},
                    ),
                    action(
                        abstain=False,
                        bubbles=[""],
                        sticker={"label": "滑稽", "timing": "after_text"},
                    ),
                ]
            )

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return next(self.replies)

    wire = Wire()
    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, wire)
        svc = Service(settings, store, ScriptedStickerGw(), FakeBili(), sender)
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            await svc.memes.upload_bytes("滑稽", picture("PNG"))

            cases = [
                ("g-m-1", "正常打个招呼", True),
                ("g-m-2", "继续随便聊聊", False),
                ("g-m-3", "@吉米奈喵 发点表情包", True),
                ("g-m-4", "纯表情测试", True),
            ]
            for eid, text, expect_img in cases:
                wire.calls.clear()
                await process(svc, group(message, event_id=eid, text=text, command_text=text))
                assert await svc.deliver_once()
                sent_types = [
                    seg["type"]
                    for act, params in wire.calls
                    if act == "send_group_msg"
                    for seg in params["message"]
                ]
                assert ("image" in sent_types) is expect_img
        finally:
            await svc.stop()
