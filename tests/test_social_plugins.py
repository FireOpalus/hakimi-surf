"""A second authenticated transport exercises the host without a QQ connection."""

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from hakimi.app import create_app
from hakimi.bot_server import assemble_chat_input, is_persona_owner, prompt_context
from hakimi.life_engine import LifeEngine
from hakimi.modules import BaseModule
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.security import Rejected, owner
from hakimi.service import Service
from hakimi.social import OwnerBinding, bound_owner
from hakimi.world_state import MilestoneEvent, private_event_audience
from tests.fakes import FakeBili, FakeGateway, FakeSender


class Forum(BaseModule):
    module_id = "forum_chat"
    display_name = "测试社区"
    chat_platforms = ("forum",)

    def __init__(self, settings):
        super().__init__(settings)
        self.sent = []
        self.online = True
        self.material = []

    async def _raw_sensor_summary(self):
        return ""

    def authorize_chat(self, message, settings, *, bot_exempt=False):
        return message.bot_id == "forum-app" and message.user_id in {"20000", "30000"}

    def permits_memory_mirror(self, message, settings):
        return message.bot_id == "forum-app" and message.conversation_id == "approved-group"

    def chat_connected(self, platform):
        return self.online

    async def send_chat(self, message, text, *, before_send):
        assert self.online
        if not await before_send():
            raise Rejected("reply_invalidated")
        self.sent.append((message, text))
        return "forum-receipt-" + str(len(self.sent))

    async def context_data(self, *, limit, now, message=None):
        self.material.append(message)
        return {"note": "社区公开动态", "role": "MEMBER"}


def attach(service):
    module = Forum(service.s)
    service.modules.register(module)
    return module


@pytest.mark.parametrize("kind", ["private", "group"])
async def test_chat_pipeline_without_qq(settings, store, message, kind):
    settings.onebot_enabled = settings.qq_enabled = False
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    forum = attach(svc)
    incoming = message.model_copy(update={"platform": "forum", "bot_id": "forum-app", "kind": kind})
    qq = svc.modules.get("qq_social")
    try:
        now = incoming.created_at
        svc.world_state.clock = lambda: now
        await svc.world_state.initialize()
        async with store.transaction():
            await store.db.execute("UPDATE world_social_battery SET charge=70")
        generate = svc.ai.generate

        async def slow_reply(*args, **kwargs):
            nonlocal now
            now += 150
            assert await svc.world_state.experience.battery(incoming, now) == 70
            return await generate(*args, **kwargs)

        svc.ai.generate = slow_reply
        assert await svc.accept_from(forum, message) == "ignored"
        assert await svc.accept_from(qq, incoming) == "ignored"
        assert not is_persona_owner(incoming, settings)
        assert await svc.accept_from(forum, incoming) == "accepted"
        qq.enabled = False
        await qq.on_switch(svc)
        job = await store.claim()
        assert job and job["payload"]["platform"] == "forum"
        await svc.process(job)
        assert await svc.deliver_once()
        assert await svc.world_state.experience.battery(message, now) == 68.75
        assert svc.world_state.current.energy == 79.8
        assert [m.user_id for m, _ in forum.sent] == [incoming.user_id]
        payload = next(c[1] for c in svc.ai.calls if c[0] == "generate")
        assert "社区公开动态" in payload["module_context"]["forum_chat"]
        assert "role" not in payload
        prefix = StaticPrefixes().get(
            prompt_context(incoming, settings), incoming.bot_id, platform="forum"
        )
        assert "forum" in prefix and settings.onebot_self_id not in prefix
        wire = assemble_chat_input(payload, incoming, settings)
        assert "社区公开动态" in wire and "forum" in wire
        assert not svc.sender.sent
        assert store.session_scope(incoming) != store.session_scope(message)
        assert all(m is None or not (m.text or m.images or m.files) for m in forum.material)
        history = await svc.sessions.snapshot(incoming)
        assert any(m["role"] == "assistant" for m in history["recent_messages"])
        assert not (await svc.sessions.snapshot(message))["recent_messages"]
        other_kind = "group" if kind == "private" else "private"
        assert not (await svc.sessions.snapshot(incoming.model_copy(update={"kind": other_kind})))[
            "recent_messages"
        ]
    finally:
        await svc.stop()


async def test_owner_grants_and_group_mirror_are_platform_and_account_scoped(
    settings, store, message
):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    forum = attach(svc)
    incoming = message.model_copy(update={"platform": "forum", "bot_id": "forum-app"})
    settings.social_owner_bindings = {
        "forum": OwnerBinding(bot_id="forum-app", user_id=message.user_id, notebook=True)
    }
    assert is_persona_owner(incoming, settings) and not owner(incoming, settings)
    assert bound_owner(incoming, settings, "notebook") and not bound_owner(
        incoming, settings, "mirror"
    )
    assert not bound_owner(incoming.model_copy(update={"bot_id": "fork"}), settings, "notebook")
    group = incoming.model_copy(update={"kind": "group", "conversation_id": "approved-group"})
    assert svc.memory_mirror.allowed(group) and not bound_owner(group, settings, "notebook")
    assert not svc.memory_mirror.allowed(incoming)
    assert private_event_audience(settings, message.user_id) != svc.world_state.experience.audience(
        incoming
    )
    forum.enabled = False
    assert not svc.memory_mirror.allowed(group) and not is_persona_owner(incoming, settings)


async def test_bound_digest_single_destination_and_queue_confirmation(settings, store):
    settings.onebot_enabled = settings.qq_enabled = False
    settings.social_owner_bindings = {"forum": OwnerBinding(bot_id="forum-app", user_id="20000")}
    settings.owner_delivery_platform = "forum"
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    forum = attach(svc)
    dm = svc.proactive
    now = datetime(2026, 11, 14, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    dm.clock = lambda: now
    svc.life_engine = LifeEngine(svc, clock=dm.clock)
    await svc.life_engine.initialize()
    await dm.initialize()
    try:
        slots = [
            s
            for mod in svc.modules.all()
            for s in svc.modules.schedules_for(mod)
            if s.name.endswith(".owner_digest")
        ]
        assert [(s.name, s.target_scope) for s in slots] == [(dm.action_name, dm.target_scope)]
        svc.life_engine.resolve(
            dict(action_name=dm.action_name, target_scope=dm.target_scope, origin="timer")
        )
        with pytest.raises(ValueError):
            svc.life_engine.resolve(
                dict(action_name=dm.action_name, target_scope="owner:20000", origin="timer")
            )
        forum.online = False
        result = await dm.manual_digest()
        assert result["delivery"] == "pending" and not forum.sent
        await dm.initialize()
        forum.online = True
        assert await dm.deliver_once()
        again = await dm.manual_digest()
        assert again["receipt"]["stage"] == "confirmed" and len(forum.sent) == 1
        assert forum.sent[0][0].platform == "forum" and forum.sent[0][0].user_id == "20000"
        assert not svc.sender.sent
    finally:
        await svc.stop()


async def test_module_privacy_api_scopes_and_persistence(settings, message):
    from pydantic import SecretStr

    settings.admin_token = SecretStr("plugin-admin-test")
    settings.onebot_enabled = settings.qq_enabled = False
    settings.social_owner_bindings = {"forum": OwnerBinding(bot_id="forum-app", user_id="20000")}
    settings.owner_delivery_platform = "forum"
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        attach(svc)
        identity = message.model_copy(update={"platform": "forum", "bot_id": "forum-app"})
        key = await svc.store.runtime.register(identity)
        qq_key = await svc.store.runtime.register(message)
        path = "/admin/api/life/modules/forum_chat/privacy"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            assert (await client.get(path)).status_code in {401, 403}
            client.headers["Authorization"] = "Bearer plugin-admin-test"
            dm_route = path.replace("/privacy", "/owner-delivery")
            assert (await client.get(dm_route)).json()["platform"] == "forum"
            assert (await client.get("/admin/api/proactive")).json() == {"assigned": False}
            assert (await client.post("/admin/api/proactive/test")).status_code == 404
            assert (await client.get(dm_route.replace("forum_chat", "weather"))).status_code == 404
            rows = (await client.get(path)).json()
            assert [(r["key"], r["effective_privacy"]) for r in rows] == [(key, "high")]
            assert (
                await client.put(path, json={"key": qq_key, "privacy": "low"})
            ).status_code == 404
            assert (await client.put(path, json={"key": key, "privacy": "low"})).json()[0][
                "effective_privacy"
            ] == "low"
            assert not any(
                r["platform"] == "forum"
                for r in (await client.get("/admin/api/qq/privacy")).json()["sessions"]
            )
            from hakimi.provider_routing import ProviderRoutingManager

            restored = ProviderRoutingManager(svc.store)
            await restored.initialize()
            assert restored.privacy_for(identity) == "low"
            assert (
                restored.privacy_for(identity.model_copy(update={"bot_id": "another-account"}))
                == "high"
            )


async def test_private_chat_exports_require_consumer_grant(settings, store, message, monkeypatch):
    from hakimi.social import permits_chat_export

    settings.roaming_enabled = True
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    forum = attach(svc)
    src = message.model_copy(update={"platform": "forum", "bot_id": "forum-app"})
    try:
        await svc.diary.initialize()
        assert not permits_chat_export(src, settings, "bilibili_surf")
        await svc.diary.group("forum-private", src)
        await svc.diary.bot_reply("forum-private", src, "私聊回复")
        assert not await store.one("SELECT 1 FROM roaming_events")
        monkeypatch.setattr(
            forum, "permits_chat_export", lambda m, consumer: consumer == "patrol_diary"
        )
        await svc.diary.group("forum-consented", src)
        assert await store.one("SELECT 1 FROM roaming_events WHERE event_kind='chat_private'")
        assert not permits_chat_export(src, settings, "bilibili_surf")
    finally:
        await svc.stop()


async def test_public_diary_and_owner_companion_respect_patrol_audience(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    attach(svc)
    other = message.model_copy(update={"platform": "forum", "bot_id": "forum-app"})
    now = svc.proactive.clock()
    public, private = "BV1AA411c7mD", "BV1BB411c7mD"
    try:
        await svc.bili_watcher.initialize()
        await svc.diary.initialize()
        async with store.transaction():
            for bv, note in [(public, "天文台观测"), (private, "点播偏好")]:
                await store.db.execute(
                    "INSERT INTO bili_watched_history VALUES (?, 'test', 'done', ?, ?)",
                    (bv, now, store.vault.seal("bili-learn:" + bv, note)),
                )
            await store.db.execute(
                "INSERT INTO bili_private_share_priority VALUES (?,?,?)",
                (private, store.session_scope(other), now + 86400),
            )
        visible = await svc.modules.context_data(limit=5, now=now, message=other)
        assert {i["bvid"] for i in visible["recent_bili_surfed"]} == {private, public}
        dm = await svc.proactive.companion_context(now)
        assert public in str(dm) and private not in str(dm)
        observer = svc.modules.systems["patrol_diary"]
        event = MilestoneEvent(
            source_module="bilibili_surf",
            event_type="bili_video_watched",
            created_at=now,
            payload={"bvid": public},
        )
        await observer.record_milestone(event, now)
        await observer.record_milestone(event, now)
        for priority in (True, False):
            concealed = event.model_copy(
                update={"payload": {"bvid": private, "private_first": priority}}
            )
            assert not await observer.record_milestone(concealed, now)
        rows = await store.all("SELECT id,payload FROM roaming_events WHERE event_kind='patrol'")
        assert len(rows) == 1 and store.vault.open(rows[0][0], rows[0][1]) == "天文台观测"
        svc.modules.get("bilibili_surf").enabled = False
        assert public not in str(await svc.proactive.companion_context(now))
    finally:
        await svc.stop()


async def test_external_delivery_uncertain_and_rotated_destination(settings, store, monkeypatch):
    settings.social_owner_bindings = {"forum": OwnerBinding(bot_id="forum-app", user_id="20000")}
    settings.owner_delivery_platform = "forum"
    gateway = FakeGateway()
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    forum = attach(svc)
    now = datetime(2026, 11, 14, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    svc.proactive.clock = lambda: now
    svc.life_engine = LifeEngine(svc, clock=svc.proactive.clock)
    await svc.life_engine.initialize()
    await svc.proactive.initialize()

    async def lost_receipt(message, text, *, before_send):
        assert await before_send()
        forum.sent.append((message, text))
        raise TimeoutError()

    async def failed_before_submit(message, text, *, before_send):
        raise ConnectionError()

    monkeypatch.setattr(forum, "send_chat", failed_before_submit)
    monkeypatch.setattr(
        svc.proactive, "_desktop_recent_activity", lambda now: {"private": "desktop"}
    )
    try:
        assert "desktop" not in str(await svc.proactive.companion_context(now))
        queued = await svc.proactive.manual_digest()
        assert queued["delivery"] == "pending" and queued["receipt"]["submitted_at"] is None
        monkeypatch.setattr(forum, "send_chat", lost_receipt)
        assert (await svc.proactive.manual_digest())["receipt"]["stage"] == "uncertain"
        await svc.proactive.initialize()
        assert not await svc.proactive.deliver_once()
        assert len(forum.sent) == 1
    finally:
        await svc.stop()
    settings.owner_delivery_platform = "onebot"
    replacement = Service(settings, store, gateway, FakeBili(), FakeSender())
    replacement.proactive.clock = lambda: now
    replacement.life_engine = LifeEngine(replacement, clock=replacement.proactive.clock)
    try:
        await replacement.life_engine.initialize()
        await replacement.proactive.initialize()
        assert (await replacement.proactive.manual_digest())[
            "reason"
        ] == "already_attempted_on_other_channel"
        assert sum(c[0] == "generate" for c in gateway.calls) == 1
    finally:
        await replacement.stop()


class RichForum(Forum):
    module_id = "rich_forum"
    display_name = "全能测试社区"
    chat_platforms = ("richforum",)
    supports_reply_anchor = True
    supports_sticker = True
    supports_rich_card = True
    supports_emoji_like = True
    supports_typing = True
    supports_quick_ack = True

    def __init__(self, settings):
        super().__init__(settings)
        self.media_calls = []
        self.react_calls = []
        self.typing_calls = []
        self.chat_calls = []

    def authorize_chat(self, message, settings, *, bot_exempt=False):
        return message.bot_id == "rich-app" and message.user_id in {"20000", "30000"}

    async def send_chat(self, message, text, *, reply_to=None, before_send=None):
        if before_send is not None and not await before_send():
            raise Rejected("reply_invalidated")
        self.chat_calls.append((message, text, reply_to))
        return f"rf-chat-{len(self.chat_calls)}"

    async def send_media(
        self,
        message,
        media_bytes: bytes,
        *,
        kind: str = "image",
        mime_type: str = "image/png",
        filename: str = "media.png",
        reply_to: str | None = None,
        before_send=None,
    ) -> str:
        if before_send is not None and not await before_send():
            raise Rejected("reply_invalidated")
        self.media_calls.append(
            {
                "message": message,
                "bytes": media_bytes,
                "kind": kind,
                "mime_type": mime_type,
                "filename": filename,
                "reply_to": reply_to,
            }
        )
        return f"rf-media-{len(self.media_calls)}"

    async def send_emoji_like(
        self, message, target_message_id: str, emoji_id: str, *, emoji=None, before_send=None
    ) -> bool:
        if before_send is not None and not await before_send():
            raise Rejected("reply_invalidated")
        self.react_calls.append((target_message_id, emoji_id, emoji))
        return True

    async def send_typing(self, message) -> None:
        self.typing_calls.append((message.kind, message.conversation_id))


def _make_png_bytes() -> bytes:
    import io

    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (16, 16), "purple").save(buf, format="PNG")
    return buf.getvalue()


def test_emoji_id_to_unicode_covers_all_allowed_ids():
    from hakimi.outbound_actions import (
        ALLOWED_EMOJI_IDS,
        EMOJI_ID_TO_UNICODE,
        emoji_like_to_unicode,
    )

    assert set(EMOJI_ID_TO_UNICODE) == ALLOWED_EMOJI_IDS
    assert emoji_like_to_unicode("76") == "👍"
    assert emoji_like_to_unicode("赞") == "👍"
    assert emoji_like_to_unicode("128049") == "🐱"
    assert emoji_like_to_unicode("猫猫") == "🐱"
    assert emoji_like_to_unicode("66") == "❤"
    assert emoji_like_to_unicode(None) is None
    assert emoji_like_to_unicode("999999") is None


async def test_unified_social_capabilities_helpers_and_guards(settings, store, message):
    import base64

    from hakimi.outgoing import (
        platform_supports_emoji_like,
        platform_supports_quick_ack,
        platform_supports_rich_card,
        platform_supports_sticker,
        platform_supports_typing,
        send_platform_card,
        send_platform_emoji_like,
        send_platform_meme,
        send_platform_typing,
    )

    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    rich = RichForum(settings)
    plain = Forum(settings)
    svc.modules.register(rich)
    svc.modules.register(plain)
    try:
        rich_grp = message.model_copy(
            update={
                "platform": "richforum",
                "bot_id": "rich-app",
                "kind": "group",
                "conversation_id": "grp-1",
                "event_id": "42",
                "addressed": True,
            }
        )
        plain_grp = message.model_copy(
            update={
                "platform": "forum",
                "bot_id": "forum-app",
                "kind": "group",
                "conversation_id": "grp-1",
                "event_id": "43",
                "addressed": True,
            }
        )
        assert platform_supports_sticker(svc, rich_grp) is True
        assert platform_supports_rich_card(svc, rich_grp) is True
        assert platform_supports_emoji_like(svc, rich_grp) is True
        assert platform_supports_typing(svc, rich_grp) is True
        assert platform_supports_quick_ack(svc, rich_grp) is True

        assert platform_supports_sticker(svc, plain_grp) is False
        assert platform_supports_rich_card(svc, plain_grp) is False
        assert platform_supports_emoji_like(svc, plain_grp) is False
        assert platform_supports_typing(svc, plain_grp) is False
        assert platform_supports_quick_ack(svc, plain_grp) is False

        # Typing works in group for non-onebot capability-declaring module
        await send_platform_typing(svc, rich_grp)
        assert rich.typing_calls == [("group", "grp-1")]

        # Emoji like sends unicode mapping and enforces before_send
        ok = await send_platform_emoji_like(svc, rich_grp, "42", "128049")
        assert ok is True
        assert rich.react_calls == [("42", "128049", "🐱")]
        with pytest.raises(Rejected, match="reply_invalidated"):
            await send_platform_emoji_like(
                svc, rich_grp, "42", "128049", before_send=lambda: _async_val(False)
            )

        # Meme validates base64, magic bytes, and 5 MiB limit
        png_raw = _make_png_bytes()
        png_b64 = "base64://" + base64.b64encode(png_raw).decode("ascii")
        receipt = await send_platform_meme(svc, rich_grp, png_b64, as_sticker=True, reply_to="42")
        assert receipt == {"message_id": "rf-media-1"}
        assert rich.media_calls[-1]["kind"] == "sticker"
        assert rich.media_calls[-1]["mime_type"] == "image/png"
        assert rich.media_calls[-1]["reply_to"] == "42"

        gif_b64 = "base64://" + base64.b64encode(b"GIF89a\x01\x00\x01\x00").decode("ascii")
        await send_platform_meme(svc, rich_grp, gif_b64, as_sticker=True)
        assert rich.media_calls[-1]["mime_type"] == "image/gif"
        assert rich.media_calls[-1]["filename"] == "sticker.gif"

        webp_b64 = "base64://" + base64.b64encode(b"RIFF\x04\x00\x00\x00WEBP").decode("ascii")
        await send_platform_meme(svc, rich_grp, webp_b64, as_sticker=True)
        assert rich.media_calls[-1]["mime_type"] == "image/webp"

        with pytest.raises(Rejected, match="meme_delivery_forbidden"):
            await send_platform_meme(svc, rich_grp, "https://example.com/a.png")
        with pytest.raises(Rejected, match="invalid_meme_image"):
            await send_platform_meme(
                svc, rich_grp, "base64://" + base64.b64encode(b"not-an-image").decode("ascii")
            )
        with pytest.raises(Rejected, match="invalid_meme_image"):
            oversized = b"\x89PNG\r\n\x1a\n" + (b"\x00" * (5 * 1024 * 1024))
            await send_platform_meme(
                svc, rich_grp, "base64://" + base64.b64encode(oversized).decode("ascii")
            )

        # Card renders and dispatches via send_media(kind="card") with group-only enforcement
        rich_priv = rich_grp.model_copy(update={"kind": "private", "conversation_id": "20000"})
        with pytest.raises(Rejected, match="card_group_only"):
            await send_platform_card(svc, rich_priv, "| a | b |\n| --- | --- |\n| 1 | 2 |")
        card_receipt = await send_platform_card(
            svc, rich_grp, "| a | b |\n| --- | --- |\n| 1 | 2 |", reply_to="42"
        )
        assert card_receipt["message_id"].startswith("rf-media-")
        assert rich.media_calls[-1]["kind"] == "card"
        assert rich.media_calls[-1]["reply_to"] == "42"
    finally:
        await svc.stop()


async def _async_val(val):
    return val


async def test_rich_non_qq_turn_pipeline_sticker_card_reaction_typing_quick_ack_and_metadata(
    settings, store, message
):
    import asyncio
    import json

    from hakimi.models import AudioRef, ImageRef
    from hakimi.routing import RouteDecision

    png_raw = _make_png_bytes()

    class MockMediaLoader:
        async def load_results(self, refs, **kwargs):
            return [None for _ in refs]

        def failure_status(self, ref):
            return "unavailable"

        async def fetch_raw(self, ref, *, max_bytes=5 * 1024 * 1024):
            return png_raw

    class ScriptedGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.reply_text = ""

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return self.reply_text

    gw = ScriptedGateway()
    svc = Service(
        settings,
        store,
        gw,
        FakeBili(),
        FakeSender(),
        image_loader=MockMediaLoader(),
        audio_loader=MockMediaLoader(),
    )
    rich = RichForum(settings)
    svc.modules.register(rich)
    try:
        await svc.memes.initialize()
        await svc.memes.upload_bytes("开心", png_raw)

        # 1) Turn with tier=3 quick_ack + typing + sticker + save_meme_label + emoji_like
        quick_ack_seen = asyncio.Event()
        orig_send_chat = rich.send_chat

        async def spy_send_chat(msg, text, *, reply_to=None, before_send=None):
            res = await orig_send_chat(msg, text, reply_to=reply_to, before_send=before_send)
            quick_ack_seen.set()
            return res

        rich.send_chat = spy_send_chat
        gw.intent = RouteDecision("chat", tier=3)
        gw.reply_text = json.dumps(
            {
                "abstain": False,
                "bubbles": ["第一句", "第二句"],
                "anchor": {"reply_id": "101", "mentions": []},
                "sticker": {"label": "开心", "timing": "after_text"},
                "image_digest": None,
                "emoji_like": "猫猫",
                "poke_qq": None,
                "save_meme_label": "新猫图",
            },
            ensure_ascii=False,
        )

        orig_generate = gw.generate

        async def wait_ack_generate(payload, subject, **kwargs):
            await asyncio.wait_for(quick_ack_seen.wait(), 0.9)
            return await orig_generate(payload, subject, **kwargs)

        gw.generate = wait_ack_generate

        incoming = message.model_copy(
            update={
                "platform": "richforum",
                "bot_id": "rich-app",
                "kind": "group",
                "conversation_id": "grp-1",
                "user_id": "30000",
                "event_id": "101",
                "sender_nickname": "林",
                "text": "帮我看看这张图 [图片]",
                "addressed": True,
                "images": [
                    ImageRef(
                        file="plugin_media:richforum:img1",
                        sender_id="30000",
                        sender_nickname="林",
                    )
                ],
                "audios": [
                    AudioRef(
                        file="plugin_media:richforum:aud1",
                        sender_id="30000",
                        sender_nickname="林",
                    )
                ],
            }
        )
        assert await svc.accept_from(rich, incoming) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()

        # Verify current_sender and media sources have no QQ: prefix
        gen_payload = next(c[1] for c in gw.calls if c[0] == "generate")
        assert gen_payload["current_sender"] == {
            "nickname": "林",
            "platform": "richforum",
            "user_id": "30000",
        }
        assert gen_payload["image_sources"][0]["sender"] == "林(richforum:30000)"
        assert gen_payload["audio_sources"][0]["sender"] == "林(richforum:30000)"

        # Verify quick_ack fired (1 quick_ack + 2 reply bubbles = 3 chat calls), and not in history
        assert len(rich.chat_calls) == 3
        history_texts = [
            m["text"] for m in (await svc.sessions.snapshot(incoming))["recent_messages"]
        ]
        assert rich.chat_calls[0][1] not in history_texts
        assert rich.typing_calls  # opening + pre-gen + between-bubbles typing
        assert "新猫图" in svc.memes.labels  # save_meme_label succeeded
        assert any(m["kind"] == "sticker" for m in rich.media_calls)
        assert ("101", "128049", "🐱") in rich.react_calls

        # 2) Abstain + emoji_like enqueues and delivers reaction-only on non-onebot
        gw.generate = orig_generate
        gw.intent = RouteDecision("chat", tier=1)
        gw.reply_text = json.dumps(
            {
                "abstain": True,
                "bubbles": [""],
                "anchor": {"reply_id": "102", "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "emoji_like": "赞",
                "poke_qq": None,
            },
            ensure_ascii=False,
        )
        m2 = incoming.model_copy(
            update={"event_id": "102", "text": "哈哈", "images": [], "audios": []}
        )
        assert await svc.accept_from(rich, m2) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert ("102", "76", "👍") in rich.react_calls

        # 3) Borderline ignore triggers lurk emoji on emoji_like-capable non-onebot platform
        gw.intent = RouteDecision("ignore", reply_score=0.25, reply_threshold=0.42)
        m3 = incoming.model_copy(
            update={
                "event_id": "103",
                "text": "哈哈哈笑死我了",
                "addressed": False,
                "attention_reason": "",
                "images": [],
                "audios": [],
            }
        )
        assert await svc.accept_from(rich, m3) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert ("103", "128514", "😂") in rich.react_calls
    finally:
        await svc.stop()


async def test_gemini_generate_injects_meme_prompt_for_sticker_capable_non_qq_platform(
    settings, store, message
):
    from types import SimpleNamespace

    from hakimi.gemini import GeminiGateway
    from hakimi.generation_policy import Scenario
    from hakimi.mentions import AT_INSTRUCTION

    captured: list[dict] = []

    async def fake_create(**kwargs):
        captured.append(kwargs)

        async def _stream():
            yield SimpleNamespace(
                event_type="step.start",
                index=0,
                step=SimpleNamespace(type="model_output", content=[]),
            )
            yield SimpleNamespace(
                event_type="step.delta",
                index=0,
                delta=SimpleNamespace(
                    type="text",
                    text=(
                        '{"abstain":false,"bubbles":["ok"],'
                        '"anchor":{"reply_id":null,"mentions":[]},'
                        '"sticker":{"label":null,"timing":"after_text"},'
                        '"image_digest":null}'
                    ),
                ),
            )
            yield SimpleNamespace(
                event_type="interaction.completed",
                interaction=SimpleNamespace(id="int-1", status="completed", usage=None),
            )

        return _stream()

    gw = GeminiGateway(settings, store)
    gw.chat_client = SimpleNamespace(
        aio=SimpleNamespace(interactions=SimpleNamespace(create=fake_create))
    )
    gw.vertex_chat_client = None

    async def fake_screen(*args, **kwargs):
        return "chat"

    gw.screen = fake_screen
    svc = Service(settings, store, gw, FakeBili(), FakeSender())
    rich = RichForum(settings)
    plain = Forum(settings)
    svc.modules.register(rich)
    svc.modules.register(plain)
    try:
        await svc.memes.initialize()
        await svc.memes.upload_bytes("开心", _make_png_bytes())

        rich_msg = message.model_copy(
            update={
                "platform": "richforum",
                "bot_id": "rich-app",
                "kind": "group",
                "conversation_id": "grp-1",
                "addressed": True,
            }
        )
        plain_msg = rich_msg.model_copy(update={"platform": "forum", "bot_id": "forum-app"})

        await gw.generate(
            {"current_message": "你好", "surface": "group"},
            "subj-rich",
            scenario=Scenario.FAST_CHAT,
            message=rich_msg,
        )
        await gw.generate(
            {"current_message": "你好", "surface": "group"},
            "subj-plain",
            scenario=Scenario.FAST_CHAT,
            message=plain_msg,
        )
        assert len(captured) == 2
        rich_sys = captured[0]["system_instruction"]
        rich_input = captured[0]["input"][0]["text"]
        assert "公开聊天的本地表情协议" in rich_sys
        assert AT_INSTRUCTION not in rich_sys
        assert '"meme_labels":["开心"]' in rich_input

        plain_sys = captured[1]["system_instruction"]
        plain_input = captured[1]["input"][0]["text"]
        assert "公开聊天的本地表情协议" not in plain_sys
        assert "meme_labels" not in plain_input
    finally:
        await svc.stop()


async def test_telegram_possible_alias_debounce_batch_and_screen_prompt(
    settings, store, message, monkeypatch
):
    from types import SimpleNamespace

    from hakimi.gemini import GeminiGateway
    from hakimi.models import Message

    class _TgAtOnlyModule(Forum):
        module_id = "telegram_stub"
        display_name = "Telegram Stub"
        chat_platforms = ("telegram",)

        def authorize_chat(self, msg, cfg, *, bot_exempt=False):
            if msg.bot_id != "tg-bot":
                return False
            policy = cfg.group_policies.get(f"{msg.platform}:{msg.conversation_id}", "at_only")
            if policy == "muted":
                return False
            if msg.kind == "group":
                return bool(msg.invites_group_review or policy == "smart_reply")
            return True

    settings.group_debounce_seconds = 2.0
    settings.group_debounce_max_seconds = 5.0
    settings.group_policies["telegram:-10012345"] = "at_only"

    now = [message.created_at]
    monkeypatch.setattr("hakimi.store.time.time", lambda: now[0])
    store.groups.clock = lambda: now[0]

    captured_questions: list[dict] = []

    async def fake_decisions_create(**kwargs):
        captured_questions.append(kwargs["questions"])
        return SimpleNamespace(
            answers={
                "is_prompt_injection": SimpleNamespace(type="noul", noul=0.01, confidence=0.99),
                "is_unsafe_or_spam": SimpleNamespace(type="noul", noul=0.01, confidence=0.99),
                "should_reply": SimpleNamespace(type="noul", noul=0.85, confidence=0.95),
                "need_memory_recall": SimpleNamespace(type="noul", noul=0.1, confidence=0.95),
                "need_external_lookup": SimpleNamespace(type="noul", noul=0.1, confidence=0.95),
                "reasoning_tier": SimpleNamespace(
                    type="choice",
                    choice="tier_1_low",
                    probabilities={
                        "tier_1_low": 0.9,
                        "tier_2_medium": 0.05,
                        "tier_3_high": 0.03,
                        "tier_4_carbon": 0.02,
                    },
                    confidence=0.95,
                ),
            },
            usage=None,
        )

    gw = GeminiGateway(settings, store)
    gw.decision_client = SimpleNamespace(
        aio=SimpleNamespace(decisions=SimpleNamespace(create=fake_decisions_create))
    )
    gw.free_decision_client = None

    svc = Service(settings, store, gw, FakeBili(), FakeSender())
    tg_mod = _TgAtOnlyModule(settings)
    svc.modules.register(tg_mod)
    try:
        m1 = message.model_copy(
            update={
                "platform": "telegram",
                "bot_id": "tg-bot",
                "kind": "group",
                "conversation_id": "-10012345",
                "user_id": "55501",
                "event_id": "201",
                "text": "猫猫觉得呢",
                "addressed": False,
                "attention_reason": "possible_alias",
                "created_at": now[0],
            }
        )
        m2 = m1.model_copy(
            update={
                "event_id": "202",
                "text": "顺便看看这个细节",
                "attention_reason": "",
                "created_at": now[0] + 0.5,
            }
        )
        assert await svc.accept_from(tg_mod, m1) == "accepted"
        now[0] += 0.5
        assert await svc.accept_from(tg_mod, m2) == "accepted"
        now[0] += 5.0
        job = await store.claim()
        assert job is not None
        merged = Message.model_validate(job["payload"])
        members = [Message.model_validate(item) for item in job.get("batch_messages", [])]
        assert len(members) == 2
        assert not merged.addressed
        assert merged.attention_reason == "possible_alias"
        assert merged.has_possible_alias and merged.invites_group_review
        assert svc.batch_authorized(merged, members)

        # Gate screen() injects mention-only possible_alias instructions for TG at_only group
        await gw.screen(
            {"current_message": merged.text, "surface": "group", "directly_addressed": False},
            job["subject"],
            purpose="message",
            group=True,
            message=merged,
        )
        assert len(captured_questions) == 1
        assert "mention-only mode" in captured_questions[0]["should_reply"]["instructions"]
        assert "猫猫" in captured_questions[0]["should_reply"]["instructions"]

        # Under smart_reply policy, screen() injects third-person vs direct-call nuance note
        settings.group_policies["telegram:-10012345"] = "smart_reply"
        await gw.screen(
            {"current_message": merged.text, "surface": "group", "directly_addressed": False},
            job["subject"],
            purpose="message",
            group=True,
            message=merged,
        )
        assert len(captured_questions) == 2
        smart_instructions = captured_questions[1]["should_reply"]["instructions"]
        assert "third-person cat/tech discussion" in smart_instructions
    finally:
        await svc.stop()
