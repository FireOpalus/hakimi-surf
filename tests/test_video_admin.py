"""Video control-plane validation and authentication without external video operations."""

import httpx
import pytest
from pydantic import ValidationError

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.video_policy import VideoWatchConfig
from tests.fakes import FakeGateway
from tests.test_admin import admin_api as admin_api

PATH = "/admin/api/bili-account/video-watch"


class VideoControls:
    def __init__(self, settings):
        self.environment = VideoWatchConfig.from_settings(settings)
        self.config = self.environment
        self.overridden = False
        self.changes = []

    async def status(self):
        return {
            "config": self.config.model_dump(),
            "environment": self.environment.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "attempts_today": 0,
            "remaining_today": self.config.daily_limit,
            "recent_attempts": [],
            "remote_cleanup_pending": 0,
        }

    async def set_config(self, value):
        self.changes.append(value)
        self.config = self.environment if value is None else value
        self.overridden = value is not None
        return await self.status()


@pytest.fixture
async def video_admin(admin_api):
    app, client, gateway = admin_api
    service = app.state.service
    original = getattr(service, "video_watch", None)
    controls = VideoControls(service.s)
    service.video_watch = controls
    try:
        yield app, client, gateway, controls
    finally:
        if original is None:
            del service.video_watch
        else:
            service.video_watch = original


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_video_admin_is_disabled_without_token(settings, method):
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            response = await client.request(method, PATH, json={})
    assert response.status_code == 403
    assert response.json()["detail"] == "admin_disabled"
    assert not gateway.calls


@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_video_admin_authenticates_before_validation_or_effect(video_admin, method):
    _, client, gateway, controls = video_admin
    response = await client.request(
        method, PATH, headers={"Authorization": "Bearer wrong"}, json={"unknown": True}
    )
    assert response.status_code == 401
    assert not controls.changes
    assert not gateway.calls


async def test_video_config_updates_and_resets_independently_of_account_patrol(video_admin):
    app, client, gateway, controls = video_admin
    before = app.state.service.bili_watcher.policy.model_dump()
    original = (await client.get(PATH)).json()
    assert original["config"] == {"enabled": False, "max_seconds": 480, "daily_limit": 3}
    desired = {"enabled": True, "max_seconds": 120, "daily_limit": 0}
    saved = await client.put(PATH, json=desired)
    assert saved.status_code == 200
    assert saved.json()["config"] == desired
    assert saved.json()["source"] == "sqlite"
    assert (await client.get(PATH)).json()["remaining_today"] == 0
    assert isinstance(controls.changes[0], VideoWatchConfig)
    assert app.state.service.bili_watcher.policy.model_dump() == before
    restored = await client.delete(PATH)
    assert restored.status_code == 200
    assert restored.json()["config"] == original["config"]
    assert restored.json()["source"] == "env"
    assert controls.changes[-1] is None
    assert not gateway.calls


@pytest.mark.parametrize(
    "payload",
    [
        {"enabled": "true"},
        {"max_seconds": 29},
        {"max_seconds": 481},
        {"max_seconds": True},
        {"max_seconds": 120.5},
        {"daily_limit": -1},
        {"daily_limit": 21},
        {"daily_limit": "3"},
        {"unknown": "untrusted-sensitive-input"},
    ],
)
async def test_video_config_rejects_invalid_or_unbounded_values(video_admin, payload):
    _, client, gateway, controls = video_admin
    response = await client.put(PATH, json=payload)
    assert response.status_code == 400
    assert "untrusted-sensitive-input" not in response.text
    assert not controls.changes
    assert not gateway.calls


def test_video_env_aliases_and_policy_defaults(settings, monkeypatch):
    assert VideoWatchConfig.from_settings(settings) == VideoWatchConfig()
    monkeypatch.setenv("ENABLE_VIDEO_FILE_WATCH", "true")
    monkeypatch.setenv("MAX_VIDEO_WATCH_SECONDS", "180")
    monkeypatch.setenv("DAILY_VIDEO_WATCH_LIMIT", "2")
    env = Settings(
        _env_file=None,
        gemini_api_key=settings.gemini_api_key,
        encryption_key=settings.encryption_key,
    )
    assert VideoWatchConfig.from_settings(env).model_dump() == {
        "enabled": True,
        "max_seconds": 180,
        "daily_limit": 2,
    }
    monkeypatch.setenv("MAX_VIDEO_WATCH_SECONDS", "481")
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            gemini_api_key=settings.gemini_api_key,
            encryption_key=settings.encryption_key,
        )
