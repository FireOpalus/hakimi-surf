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
        assert svc.world_state.current.energy == 79.5
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
