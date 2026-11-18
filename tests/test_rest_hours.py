import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from hakimi.adapters import Sender
from hakimi.config import Settings
from hakimi.models import Video
from hakimi.rest_policy import RestHoursConfig
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import admin_api as admin_api  # noqa: F401


def _stamp(value: str) -> float:
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


class CountingBili(FakeBili):
    def __init__(self, videos=None):
        super().__init__(videos)
        self.update_calls = []

    async def updates(self, mid):
        self.update_calls.append(mid)
        return await super().updates(mid)


class LocalWire:
    def __init__(self):
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls = []

    async def call(self, action, params):
        self.calls.append((action, params))
        return {"message_id": len(self.calls)}


def test_settings_defaults_and_env_aliases(tmp_path, monkeypatch):
    key = Fernet.generate_key().decode()
    default_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "default",
        gemini_api_key="test-gemini-key-never-live",
        encryption_key=key,
    )
    assert default_settings.rest_hours_enabled is True
    assert default_settings.rest_hours == "23:00-08:00"
    assert default_settings.rest_allow_owner_private is False
    assert RestHoursConfig.from_settings(default_settings) == RestHoursConfig(
        enabled=True, rest_hours="23:00-08:00", allow_owner_private=False
    )

    monkeypatch.setenv("REST_HOURS_ENABLED", "false")
    monkeypatch.setenv("REST_HOURS", "22:30-07:30")
    monkeypatch.setenv("REST_ALLOW_OWNER_PRIVATE", "true")
    env_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "env",
        gemini_api_key="test-gemini-key-never-live",
        encryption_key=key,
    )
    assert env_settings.rest_hours_enabled is False
    assert env_settings.rest_hours == "22:30-07:30"
    assert env_settings.rest_allow_owner_private is True

    monkeypatch.setenv("REST_HOURS", "23:00-23:00")
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            data_dir=tmp_path / "bad",
            gemini_api_key="test-gemini-key-never-live",
            encryption_key=key,
        )


@pytest.mark.parametrize(
    "config,time_text,expected",
    [
        (RestHoursConfig(), "22:59", False),
        (RestHoursConfig(), "23:00", True),
        (RestHoursConfig(), "00:10", True),
        (RestHoursConfig(), "07:59", True),
        (RestHoursConfig(), "08:00", False),
        (RestHoursConfig(rest_hours="12:30-14:00"), "12:29", False),
        (RestHoursConfig(rest_hours="12:30-14:00"), "12:30", True),
        (RestHoursConfig(rest_hours="12:30-14:00"), "13:59", True),
        (RestHoursConfig(rest_hours="12:30-14:00"), "14:00", False),
        (RestHoursConfig(enabled=False, rest_hours="23:00-08:00"), "00:10", False),
    ],
)
def test_rest_hours_window_boundaries(config, time_text, expected):
    local = datetime.fromisoformat("2026-11-16T" + time_text)
    assert config.active(local) is expected


async def test_accept_skips_llm_during_rest_preserves_context_and_recalls(settings, store, message):
    clock = [_stamp("2026-11-16T22:00:00")]
    store.runtime.clock = lambda: clock[0]
    await store.runtime.set_rest_hours(
        RestHoursConfig(enabled=True, rest_hours="23:00-08:00", allow_owner_private=False)
    )
    gateway = FakeGateway()
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    try:
        # Daytime message goes through normal admission, generation, and history.
        pre_msg = message.model_copy(
            update={
                "event_id": "2001",
                "kind": "group",
                "conversation_id": "40000",
                "user_id": "30000",
                "addressed": True,
                "text": "待会儿要撤回的消息",
            }
        )
        assert await svc.accept(pre_msg) == "accepted"
        await svc.process(await store.claim())
        assert len(gateway.calls) > 0
        gateway.calls.clear()

        # Enter rest hours (23:30): passive context is preserved, zero LLM calls or queued jobs.
        clock[0] = _stamp("2026-11-16T23:30:00")
        rest_group = message.model_copy(
            update={
                "event_id": "2002",
                "kind": "group",
                "conversation_id": "40000",
                "user_id": "30000",
                "addressed": True,
                "text": "睡前还在群里聊天",
            }
        )
        assert await svc.accept(rest_group) == "resting"
        assert svc.counters["admission_resting"] == 1
        assert gateway.calls == []
        assert await store.claim() is None

        ctx = await store.runtime.filters.group_context(
            rest_group.model_copy(update={"event_id": "2003"})
        )
        assert any("睡前还在群里聊天" in item for item in ctx)
        assert svc.hot.lookup(rest_group, "2002") is not None

        # Recalls remain active during rest hours.
        recall_payload = {
            "post_type": "notice",
            "notice_type": "group_recall",
            "self_id": int(settings.onebot_self_id),
            "group_id": 40000,
            "user_id": 30000,
            "message_id": 2001,
        }
        assert await svc.accept_event(recall_payload) == "recalled"
        assert svc.hot.recalled(pre_msg, "2001") is True
        snap_after = await svc.sessions.snapshot(pre_msg)
        assert snap_after["interaction_chains"] == {}
    finally:
        await svc.stop()


async def test_allow_owner_private_only_permits_owner_dm(settings, store, message):
    settings.owner_qq_id = "20000"
    settings.onebot_allowed_users.add("20000")
    clock = [_stamp("2026-11-16T01:30:00")]
    store.runtime.clock = lambda: clock[0]
    await store.runtime.set_rest_hours(
        RestHoursConfig(enabled=True, rest_hours="23:00-08:00", allow_owner_private=True)
    )
    gateway = FakeGateway()
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    try:
        group = message.model_copy(
            update={
                "event_id": "g1",
                "kind": "group",
                "conversation_id": "40000",
                "addressed": True,
            }
        )
        other_dm = message.model_copy(
            update={
                "event_id": "p1",
                "kind": "private",
                "user_id": "30000",
                "conversation_id": "30000",
            }
        )
        owner_dm = message.model_copy(
            update={
                "event_id": "p2",
                "kind": "private",
                "user_id": "20000",
                "conversation_id": "20000",
            }
        )
        assert await svc.accept(group) == "resting"
        assert await svc.accept(other_dm) == "resting"
        assert gateway.calls == []

        assert await svc.accept(owner_dm) == "accepted"
        job = await store.claim()
        assert job is not None
        await svc.process(job)
        assert any(call[0] == "generate" for call in gateway.calls)
    finally:
        await svc.stop()


async def test_in_flight_preparation_aborts_before_screen_when_rest_starts(
    settings, store, message, monkeypatch
):
    store.runtime.clock = lambda: _stamp("2026-11-16T23:30:00")
    gateway = FakeGateway()
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    entered, resume = asyncio.Event(), asyncio.Event()
    original = svc.sessions.snapshot

    async def pause_snapshot(*args, **kwargs):
        entered.set()
        await resume.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(svc.sessions, "snapshot", pause_snapshot)
    target = message.model_copy(
        update={
            "event_id": "prep-1",
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "addressed": True,
        }
    )
    assert await svc.accept(target) == "accepted"
    job = await store.claim()
    processing = asyncio.create_task(svc.handle_message(job))
    try:
        async with asyncio.timeout(3):
            await entered.wait()
        await store.runtime.set_rest_hours(RestHoursConfig(enabled=True, rest_hours="23:00-08:00"))
        resume.set()
        async with asyncio.timeout(3):
            await processing
        assert gateway.calls == []
        assert not await store.all("SELECT * FROM ledger")
        assert not await store.all("SELECT * FROM outbox")
    finally:
        resume.set()
        if not processing.done():
            processing.cancel()
        await asyncio.gather(processing, return_exceptions=True)
        await svc.stop()


async def test_scheduled_patrols_and_companion_pause_during_rest_while_manual_runs(
    settings, store, message
):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    settings.bilibili_enabled = True
    clock = [_stamp("2026-11-16T23:30:00")]
    store.runtime.clock = lambda: clock[0]
    await store.runtime.set_rest_hours(RestHoursConfig(enabled=True, rest_hours="23:00-08:00"))

    video = Video(
        bvid="BV1xx411c7mD",
        mid="12345",
        title="测试视频",
        description="公开描述",
        author="UP",
        published_at=1000,
    )
    bili = CountingBili([video])
    wire = LocalWire()
    http_calls = []

    def handler(req):
        http_calls.append(req.url.path)
        if req.url.path == "/x/web-interface/nav":
            data = {
                "isLogin": True,
                "mid": 123456,
                "wbi_img": {
                    "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
                    "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
                },
            }
            return httpx.Response(200, json={"code": 0, "data": data})
        if req.url.path == "/x/web-interface/wbi/index/top/feed/rcmd":
            return httpx.Response(200, json={"code": 0, "data": {"item": []}})
        return httpx.Response(200, json={"code": 0, "data": {"items": []}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        svc = Service(settings, store, FakeGateway(), bili, Sender(settings, http, wire))
        try:
            await store.add_subscription(message, "12345")
            # Automatic subscription polling skips during rest, force=True overrides.
            await svc.poll_once(force=False)
            assert bili.update_calls == []
            await svc.poll_once(force=True)
            assert bili.update_calls == ["12345"]

            # A closing report must precede both the engine's quiet window and system rest.
            dm = svc.proactive
            dm.clock = lambda: clock[0]
            await dm.initialize()
            engine = svc.life_engine
            await engine.initialize()
            assert engine.digest_window(dm.local().date())[1] == _stamp("2026-11-16T23:00:00")
            await store.runtime.set_rest_hours(
                RestHoursConfig(enabled=False, rest_hours="23:00-08:00")
            )
            assert engine.digest_window(dm.local().date())[1] == _stamp("2026-11-16T23:30:00")
            assert not engine.digest_due(clock[0])
            assert not http_calls
            assert (await dm.test_dm())["status"] == "sent"
            alert_id = await dm.alert("health", "BOISER，运维测试通知")
            assert await dm.deliver_once()
            assert await dm.delivery_status(alert_id) == "sent"
        finally:
            await svc.stop()


async def test_rest_hours_admin_api_dashboard_encryption_reload_and_reset(admin_api):
    app, client, gateway = admin_api
    store = app.state.service.store

    dash = await client.get("/admin/api/dashboard")
    assert dash.status_code == 200
    assert "rest_hours" in dash.json()

    initial = (await client.get("/admin/api/rest-hours")).json()
    assert initial["source"] == "env"
    assert initial["config"]["enabled"] is False
    assert initial["config"]["rest_hours"] == "23:00-08:00"

    updated = await client.put(
        "/admin/api/rest-hours",
        json={"enabled": True, "rest_hours": "22:00-07:30", "allow_owner_private": True},
    )
    assert updated.status_code == 200
    data = updated.json()
    assert data["source"] == "admin"
    assert data["config"] == {
        "day_rest_enabled": True,
        "enabled": True,
        "rest_hours": "22:00-07:30",
        "allow_owner_private": True,
    }

    row = await store.one("SELECT payload FROM admin_config WHERE key='rest_hours'")
    assert row is not None and b"22:00-07:30" not in row[0]

    await store.runtime.load_config()
    assert store.runtime.rest_hours_overridden is True
    assert store.runtime.rest_hours.rest_hours == "22:00-07:30"
    assert store.runtime.rest_hours.allow_owner_private is True

    reset = await client.delete("/admin/api/rest-hours")
    assert reset.status_code == 200
    assert reset.json()["source"] == "env"
    assert reset.json()["config"] == initial["config"]
    assert not await store.one("SELECT payload FROM admin_config WHERE key='rest_hours'")
    assert gateway.calls == []
