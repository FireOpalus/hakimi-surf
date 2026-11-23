"""End-to-end Phase 3 integration tests with external `bilibili_surf` (+ `minecraft`) plugins."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import uvicorn
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.bili_bridge import RemoteBiliClient, RemoteBiliWatcher
from hakimi.budget_policy import PATROL_LANE
from hakimi.config import Settings, is_external_plugin
from hakimi.models import Message
from hakimi.modules.actions import ActionCall
from hakimi.modules.bili_schedule import BiliScheduleOptions
from hakimi.plugin_bus import RemoteModuleProxy
from hakimi.security import Rejected, configured_secrets
from hakimi.video_watch import guard_call
from hakimi_plugin_sdk import PluginRuntimeConfig
from plugins.bilibili.plugin_main import (
    BILIBILI_CAPABILITIES,
    BILIBILI_SCOPES,
    build_bilibili_plugin,
)
from plugins.minecraft.plugin_main import MinecraftPlugin
from tests.fakes import FakeGateway, FakeSender

BV1 = "BV1xx411c7mD"
BV2 = "BV1Q541167Qg"
ADMIN_TOKEN = "test-phase3-integration-admin-token"

NAV_DATA = {
    "isLogin": True,
    "mid": 123456,
    "wbi_img": {
        "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
        "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
    },
}


async def _wait_until(predicate: Any, *, max_wait: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + max_wait
    while True:
        res = predicate()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not met within max_wait")
        await asyncio.sleep(0.02)


async def _noop_sleep(_delay: float) -> None:
    return None


def _base_settings(settings: Settings, **update: Any) -> Settings:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(ADMIN_TOKEN),
            "owner_qq_id": "20000",
            "external_plugins": {"bilibili_surf"},
            **update,
        }
    )


@contextlib.asynccontextmanager
async def _running_app(app: Any) -> AsyncIterator[tuple[int, httpx.AsyncClient]]:
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        if server_task.done():
            server_task.result()
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            trust_env=False,
        ) as client:
            yield port, client
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server_task, timeout=5.0)


def _make_bili_http_client(
    responses: dict[str, Any],
    calls: list[httpx.Request],
    *,
    view_by_bvid: dict[str, dict[str, Any]] | None = None,
) -> httpx.AsyncClient:
    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        if req.url.host in {"b23.tv", "bili2233.cn"}:
            return httpx.Response(
                302, headers={"Location": f"https://www.bilibili.com/video/{BV1}?from=short"}
            )
        if req.url.path == "/x/web-interface/view" and view_by_bvid:
            bv = req.url.params.get("bvid", "")
            if bv in view_by_bvid:
                return httpx.Response(200, json={"code": 0, "data": view_by_bvid[bv]})
        value = responses[req.url.path]
        return httpx.Response(
            200,
            json=value if req.url.host == "aisubtitle.hdslb.com" else {"code": 0, "data": value},
        )

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)


def _default_bili_responses(*, with_subtitles: bool = True) -> dict[str, Any]:
    subtitles = (
        [
            {
                "lan": "zh-CN",
                "subtitle_url": "https://aisubtitle.hdslb.com/bfs/subtitle/test.json",
            }
        ]
        if with_subtitles
        else []
    )
    return {
        "/x/web-interface/nav": NAV_DATA,
        "/x/frontend/finger/spi": {"b_3": "spi-b3", "b_4": "spi-buvid4-value"},
        "/x/web-interface/wbi/index/top/feed/rcmd": {
            "item": [
                {
                    "goto": "av",
                    "bvid": BV1,
                    "title": "我的世界红石自动门教程",
                    "owner": {"name": "红石UP"},
                    "tname": "游戏",
                }
            ]
        },
        "/x/polymer/web-dynamic/v1/feed/all": {"items": []},
        "/x/polymer/web-dynamic/v1/feed/space": {
            "items": [
                {
                    "type": "DYNAMIC_TYPE_AV",
                    "modules": {
                        "module_author": {"mid": "12345", "name": "UP1", "pub_ts": 1700000100},
                        "module_dynamic": {
                            "major": {
                                "type": "MAJOR_TYPE_ARCHIVE",
                                "archive": {
                                    "bvid": BV1,
                                    "title": "UP更新测试视频",
                                    "desc": "简介",
                                    "duration": 90,
                                },
                            }
                        },
                    },
                }
            ]
        },
        "/x/web-interface/view": {
            "bvid": BV1,
            "aid": 100,
            "cid": 200,
            "duration": 60,
            "pubdate": 1700000000,
            "title": "我的世界红石自动门教程",
            "desc": "公开简介内容",
            "owner": {"mid": "12345", "name": "红石UP"},
            "stat": {"view": 1024, "like": 88},
            "tname": "游戏",
        },
        "/x/player/wbi/v2": {"subtitle": {"subtitles": subtitles}},
        "/bfs/subtitle/test.json": {"body": [{"content": "公开字幕文字", "from": 0, "to": 60}]},
        "/x/v2/reply": {"replies": [{"content": {"message": "一条高赞评论"}}]},
        "/x/web-interface/archive/relation": {"like": False},
        "/x/web-interface/archive/like": {},
        "/x/v3/fav/folder/created/list-all": {"list": [{"id": 888}]},
        "/x/v3/fav/resource/deal": {},
        "/room/v1/Room/get_info": {
            "live_status": 1,
            "title": "深夜写代码直播",
            "area_name": "科技",
        },
    }


def _bili_runtime_config(port: int, token: str) -> PluginRuntimeConfig:
    return PluginRuntimeConfig(
        ws_url=f"ws://127.0.0.1:{port}/ws/plugin",
        plugin_id="bilibili_surf",
        token=token,
        display_name="B 站巡逻",
        description="账号、视频订阅与巡逻日记",
        scopes=BILIBILI_SCOPES,
        node_region="KR",
        admin_panel="bili",
        attention_level="MEDIUM",
        capabilities=BILIBILI_CAPABILITIES,
        reconnect_min=0.05,
        reconnect_max=0.2,
    )


@pytest.mark.parametrize("alias", ["bilibili_surf", "bilibili"])
async def test_external_bilibili_aliases_skip_builtin_and_degrade_offline_admin(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, tmp_path: Any, alias: str
) -> None:
    monkeypatch.setenv("HAKIMI_GEMINI_API_KEY", "test-gemini-key-never-live")
    monkeypatch.setenv("HAKIMI_ENCRYPTION_KEY", settings.encryption_key.get_secret_value())
    monkeypatch.setenv("HAKIMI_DATA_DIR", str(tmp_path / f"env_{alias}"))
    monkeypatch.setenv("EXTERNAL_PLUGINS", json.dumps([alias]))
    loaded = Settings(_env_file=None)
    assert is_external_plugin(loaded, "bilibili_surf") is True
    assert is_external_plugin(loaded, "bilibili") is True
    assert is_external_plugin(loaded, "weather") is False

    app = create_app(
        _base_settings(settings, external_plugins={alias}),
        gateway_factory=FakeGateway,
        start_workers=False,
    )
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert svc.modules.get("bilibili_surf") is None
        assert isinstance(svc.bili, RemoteBiliClient)
        assert isinstance(svc.bili_watcher, RemoteBiliWatcher)
        assert "bili.patrol" not in svc.modules.catalog.actions
        assert not (
            {"bilibili_summary", "bili_selection", "bili_learning"}
            & set(svc.modules.thinking_scenes)
        )
        assert svc.node_transport.on_event is not None
        assert svc.node_transport.on_state is not None

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ) as client:
            st = await client.get("/admin/api/bili-account")
            assert st.status_code == 200
            st_body = st.json()
            assert st_body["plugin_offline"] is True
            assert set(st_body["credentials"]) == {"sessdata", "bili_jct", "buvid3", "buvid4"}
            assert all(isinstance(v, bool) for v in st_body["credentials"].values())

            for method, path, body in (
                ("PUT", "/admin/api/bili-account", {"enabled": True}),
                ("DELETE", "/admin/api/bili-account", None),
                (
                    "POST",
                    "/admin/api/bili-account/credentials",
                    {"sessdata": "off-sess-123456"},
                ),
                ("POST", "/admin/api/bili-account/test", None),
                ("POST", "/admin/api/bili-account/run", None),
                (
                    "POST",
                    "/admin/api/sandbox",
                    {"identity": "MEMBER", "scene": "group_at", "text": BV1},
                ),
            ):
                res = await client.request(method, path, json=body)
                assert res.status_code == 503, (method, path, res.text)
                assert res.json()["detail"] == "bilibili_module_unavailable"


async def test_bilibili_hotplug_public_rpcs_chat_flow_and_module_switch(
    settings: Settings, message: Message
) -> None:
    gateway = FakeGateway()
    app = create_app(
        _base_settings(settings),
        gateway_factory=lambda *_: gateway,
        start_workers=False,
    )
    http_calls: list[httpx.Request] = []
    responses = _default_bili_responses(with_subtitles=True)

    async with _running_app(app) as (port, client):
        svc = app.state.service
        svc.sender = FakeSender()

        created = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "bilibili_surf",
                "display_name": "B 站巡逻",
                "scopes": list(BILIBILI_SCOPES),
                "node_region": "KR",
            },
        )
        assert created.status_code == 200
        token = created.json()["token"]

        async with _make_bili_http_client(responses, http_calls) as bili_http:
            plugin = build_bilibili_plugin(
                _bili_runtime_config(port, token),
                http_client=bili_http,
                sleep=_noop_sleep,
            )
            runner = asyncio.create_task(plugin.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        (mod := svc.modules.get("bilibili_surf")) is not None
                        and isinstance(mod, RemoteModuleProxy)
                        and mod._session.ready
                        and plugin.engine._initialized
                    )
                )
                proxy = svc.modules.get("bilibili_surf")
                assert isinstance(proxy, RemoteModuleProxy)
                assert proxy.admin_panel == "bili"
                assert proxy.schedule_options is BiliScheduleOptions
                assert proxy.thinking_scene_keys == {
                    "bilibili_summary",
                    "bili_selection",
                    "bili_learning",
                }
                assert "bili.patrol" in svc.modules.catalog.actions
                patrol_spec = svc.modules.catalog.actions["bili.patrol"]
                assert patrol_spec.budget_lane == PATROL_LANE
                assert patrol_spec.quotas[0].counter == "bili_patrols"

                # Public client RPCs via RemoteBiliClient
                assert await svc.bili.resolve_short("https://b23.tv/abc123") == BV1
                video = await svc.bili.video(BV1)
                assert video.bvid == BV1 and video.title == "我的世界红石自动门教程"
                updates = await svc.bili.updates("12345")
                assert len(updates) == 1 and updates[0].bvid == BV1
                live_info = await svc.bili.live_info("778899")
                assert live_info["live_status"] == 1 and live_info["area"] == "科技"
                assert await svc.bili.live_status("778899") == 1

                # Chat video flow with short URL
                short_msg = message.model_copy(
                    update={
                        "event_id": "evt-short-1",
                        "text": "帮我看看这个视频 https://b23.tv/abc123",
                        "short_urls": ["https://b23.tv/abc123"],
                    }
                )
                assert await svc.accept(short_msg) == "accepted"
                await svc.process(await svc.store.claim())
                assert await svc.deliver_once() and len(svc.sender.sent) == 1
                assert any(
                    c[0] == "screen" and c[3].get("purpose") == "source" for c in gateway.calls
                )
                assert any(c[0] == "generate" for c in gateway.calls)

                # Sandbox with BV works while plugin is online
                sb_res = await client.post(
                    "/admin/api/sandbox",
                    json={"identity": "MEMBER", "scene": "group_at", "text": BV1},
                )
                assert sb_res.status_code == 200
                assert sb_res.json()["preview"]["thinking_level"] == "medium"

                # Disabling module via Life Admin -> 409 module_disabled
                sw_off = await client.put(
                    "/admin/api/life/modules/bilibili_surf/enabled",
                    json={"enabled": False},
                )
                assert sw_off.status_code == 200
                for ep in ("test", "run"):
                    dis_res = await client.post(f"/admin/api/bili-account/{ep}")
                    assert dis_res.status_code == 409
                    assert dis_res.json()["detail"] == "module_disabled"
                with pytest.raises(Rejected, match="module_disabled"):
                    await svc.bili.video(BV1)

                sw_on = await client.put(
                    "/admin/api/life/modules/bilibili_surf/enabled",
                    json={"enabled": True},
                )
                assert sw_on.status_code == 200
                assert (await svc.bili.video(BV1)).bvid == BV1
            finally:
                await plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(runner, timeout=3.0)


async def test_autonomous_patrol_reentrant_video_watch_mc_broadcast_and_reconnect(
    settings: Settings, tmp_path: Any
) -> None:
    class PatrolGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.video_calls: list[tuple[Any, dict[str, Any], str]] = []

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            if str(kwargs.get("scenario")) == "bili_selection":
                return json.dumps(
                    {
                        "selected": [{"bvid": BV1, "like": True}],
                        "mood_longnick": "刚才在 B 站刷到了红石自动门教程",
                    },
                    ensure_ascii=False,
                )
            return "看完觉得红石中继器时序和活塞门结构很实用。"

        async def watch_video(self, path, payload, subject, **kwargs):
            await guard_call(kwargs.get("before_call"))
            self.video_calls.append((path, payload, subject))
            await guard_call(kwargs.get("before_call"))
            return "视频画面展示了红石自动门活塞与粘性方块接线。"

    class FakeDownloader:
        def __init__(self, path: Any, svc_ref: list[Any]) -> None:
            self.path = path
            self.svc_ref = svc_ref
            self.calls: list[tuple[str, dict[str, Any]]] = []
            self.bili_until_during_download: list[float] = []

        async def cleanup_stale(self) -> None:
            return None

        @asynccontextmanager
        async def download(self, bvid: str, **kwargs: Any):
            self.calls.append((bvid, dict(kwargs)))
            if self.svc_ref:
                row = await self.svc_ref[0].store.one(
                    "SELECT bili_until FROM world_rhythm WHERE id=1"
                )
                if row is not None:
                    self.bili_until_during_download.append(float(row["bili_until"]))
            await guard_call(kwargs.get("guard"))
            yield SimpleNamespace(path=self.path, duration=60, bytes=2048)

    gateway = PatrolGateway()

    class _FakeOneBot:
        def __init__(self) -> None:
            self.ready = asyncio.Event()
            self.ready.set()
            self.calls: list[tuple[str, dict[str, Any]]] = []

        async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
            self.calls.append((method, params))
            return {"retcode": 0}

    s = _base_settings(
        settings,
        external_plugins={"bilibili_surf", "minecraft"},
        enable_video_file_watch=True,
        enable_daily_longnick=True,
        onebot_enabled=True,
        onebot_self_id="10000",
        onebot_owner_ids={"20000"},
    )
    app = create_app(s, gateway_factory=lambda *_: gateway, start_workers=False)

    http_calls: list[httpx.Request] = []
    # No subtitles so VideoWatch enters _video -> download_credentials -> bili.ensure_buvid4!
    responses = _default_bili_responses(with_subtitles=False)

    async with _running_app(app) as (port, client):
        svc = app.state.service
        svc.sender = FakeSender()
        svc.sender.onebot = _FakeOneBot()
        svc_ref = [svc]
        fake_dl = FakeDownloader(tmp_path / "patrol_video.mp4", svc_ref)
        svc.video_watch.downloader = fake_dl

        bili_cred = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "bilibili_surf",
                "display_name": "B 站巡逻",
                "scopes": list(BILIBILI_SCOPES),
                "node_region": "KR",
            },
        )
        mc_cred = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "minecraft",
                "display_name": "Minecraft",
                "scopes": ["actions", "sensors", "node_bridge"],
                "node_region": "CN",
            },
        )
        assert bili_cred.status_code == 200 and mc_cred.status_code == 200
        bili_token = bili_cred.json()["token"]
        mc_token = mc_cred.json()["token"]

        mc_plugin = MinecraftPlugin(
            ws_url=f"ws://127.0.0.1:{port}/ws/plugin",
            token=mc_token,
            reconnect_min=0.05,
            reconnect_max=0.2,
        )
        mc_task = asyncio.create_task(mc_plugin.client.run_forever())

        async with _make_bili_http_client(responses, http_calls) as bili_http:
            bili_plugin = build_bilibili_plugin(
                _bili_runtime_config(port, bili_token),
                http_client=bili_http,
                sleep=_noop_sleep,
            )
            bili_task = asyncio.create_task(bili_plugin.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        svc.modules.get("bilibili_surf") is not None
                        and svc.modules.get("minecraft") is not None
                        and bili_plugin.engine._initialized
                    )
                )

                # Configure credentials WITHOUT buvid4 -> SPI autofills buvid4 during patrol!
                secret_cookies = {
                    "sessdata": "test-only-sessdata-secret-1234",
                    "bili_jct": "test-only-jct-secret-1234",
                    "buvid3": "test-only-buvid3-secret-1234",
                }
                cred_res = await client.post(
                    "/admin/api/bili-account/credentials",
                    json=secret_cookies,
                )
                assert cred_res.status_code == 200
                for sec in secret_cookies.values():
                    assert sec not in cred_res.text
                    assert sec in svc.secrets
                    assert sec in configured_secrets(svc.s)
                assert cred_res.json()["credentials"] == {
                    "sessdata": True,
                    "bili_jct": True,
                    "buvid3": True,
                    "buvid4": False,
                }

                pol_res = await client.put(
                    "/admin/api/bili-account",
                    json={"enabled": True, "allow_like": True},
                )
                assert pol_res.status_code == 200
                assert pol_res.json()["config"]["enabled"] is True
                assert pol_res.json()["config"]["allow_like"] is True
                assert pol_res.json()["source"] == "sqlite"

                nav_res = await client.post("/admin/api/bili-account/test")
                assert nav_res.status_code == 200
                assert nav_res.json() == {"status": "valid", "account_mask": "…3456"}

                # Run full autonomous patrol via LifeEngine action catalog
                patrol_action = svc.modules.catalog.actions["bili.patrol"]
                outcome = await patrol_action.run(
                    ActionCall(
                        {"origin": "planner"},
                        patrol_action.arguments(),
                        svc.life_engine.clock(),
                    )
                )
                assert outcome.status == "done"

                # Proves re-entrant VideoWatch -> download_credentials -> bili.ensure_buvid4 worked!
                assert len(fake_dl.calls) == 1
                dl_bvid, dl_kwargs = fake_dl.calls[0]
                assert dl_bvid == BV1
                assert dl_kwargs["cookies"]["sessdata"] == secret_cookies["sessdata"]
                assert dl_kwargs["cookies"]["buvid4"] == "spi-buvid4-value"
                assert len(gateway.video_calls) == 1
                assert (
                    fake_dl.bili_until_during_download and fake_dl.bili_until_during_download[0] > 0
                )
                rhythm_after = await svc.store.one("SELECT bili_until FROM world_rhythm WHERE id=1")
                assert rhythm_after is not None and rhythm_after["bili_until"] == 0

                # Verify world_experience, longnick, patrol_diary, recent_surfed, and MC broadcast
                await _wait_until(
                    lambda: svc.store.one(
                        "SELECT id, action_name, source_module FROM scheduled_intents "
                        "WHERE action_name='mc.build_plan'"
                    )
                )
                mc_intent = await svc.store.one(
                    "SELECT action_name, origin, source_module, arguments_json "
                    "FROM scheduled_intents WHERE action_name='mc.build_plan'"
                )
                assert mc_intent is not None
                assert mc_intent["origin"] == "rule"
                assert mc_intent["source_module"] == "minecraft"
                assert "红石自动门" in json.loads(mc_intent["arguments_json"])["topic"]

                expected_diary_key = svc.store.vault.digest(
                    "diary-event", f"bili-account:{BV1}", "patrol"
                )
                diary_row = await svc.store.one(
                    "SELECT id, payload, event_kind FROM roaming_events WHERE id=?",
                    (expected_diary_key,),
                )
                assert diary_row is not None and diary_row["event_kind"] == "patrol"
                diary_text = svc.store.vault.open(expected_diary_key, diary_row["payload"])
                assert BV1 in diary_text

                assert svc.world_state.current.mood == "curious"
                change_row = await svc.store.one(
                    "SELECT reason FROM world_changes WHERE reason='video_learned'"
                )
                assert change_row is not None

                owner_msg = Message(
                    platform="onebot",
                    bot_id="10000",
                    event_id="rep-check",
                    user_id="20000",
                    kind="private",
                    conversation_id="20000",
                    text="",
                    addressed=True,
                    created_at=svc.life_engine.clock(),
                )
                reports = await svc.store.latest_reports(svc.store.subject(owner_msg))
                assert any(BV1 in rep for rep in reports)

                proxy = svc.modules.get("bilibili_surf")
                assert isinstance(proxy, RemoteModuleProxy)
                ctx_data = await proxy.context_data(limit=4, now=svc.life_engine.clock())
                assert len(ctx_data.get("recent_bili_surfed", [])) == 1
                assert ctx_data["recent_bili_surfed"][0]["bvid"] == BV1

                lk = await svc.bili_watcher.lookup_watched(BV1)
                assert lk is not None and lk["already_watched_in_patrol"] is True

                longnick_status = await svc.longnick.status()
                assert "红石自动门教程" in longnick_status["last_attempted_text"]
                st_online = await client.get("/admin/api/bili-account")
                assert st_online.status_code == 200
                st_data = st_online.json()
                assert st_data["watched_count"] == 1
                assert st_data["actions_today"] == 1
                assert st_data["buvid4_autofill"]["status"] == "configured"
                assert st_data["account_mask"] == "…3456"
                for sec in (*secret_cookies.values(), "spi-buvid4-value"):
                    assert sec not in st_online.text

                # Reset policy to env via DELETE
                del_pol = await client.delete("/admin/api/bili-account")
                assert del_pol.status_code == 200
                assert del_pol.json()["source"] == "env"
            finally:
                await bili_plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(bili_task, timeout=3.0)

            # While disconnected: GET degrades safely; offline KV lookup works; mutations 503
            await _wait_until(lambda: svc.modules.get("bilibili_surf") is None)
            off_get = await client.get("/admin/api/bili-account")
            assert off_get.status_code == 200
            off_body = off_get.json()
            assert off_body["plugin_offline"] is True
            assert off_body["watched_count"] == 1
            assert off_body["credentials"] == {
                "sessdata": True,
                "bili_jct": True,
                "buvid3": True,
                "buvid4": True,
            }
            for sec in (*secret_cookies.values(), "spi-buvid4-value"):
                assert sec not in off_get.text
            assert (await svc.bili_watcher.lookup_watched(BV1)) is not None
            assert len(await svc.bili_watcher.recent_surfed()) == 1

            assert (
                await client.put("/admin/api/bili-account", json={"enabled": True})
            ).status_code == 503
            assert (await client.post("/admin/api/bili-account/test")).status_code == 503
            assert (await client.post("/admin/api/bili-account/run")).status_code == 503

            # Risk cooldown via RemoteBiliWatcher._risk persists & blocks _ensure_current
            with pytest.raises(Rejected, match="bili_account_risk"):
                await svc.bili_watcher._risk("bili_account_risk")
            with pytest.raises(Rejected, match="bili_account_cooling_down"):
                svc.bili_watcher._ensure_current()
            off_risk_get = await client.get("/admin/api/bili-account")
            assert off_risk_get.status_code == 200
            assert off_risk_get.json()["last_error"] == "bili_account_risk"
            assert off_risk_get.json()["cooldown_until"] > svc.bili_watcher.clock()

            # Reconnect hot-plug restores online status & watched state from encrypted KV
            bili_plugin2 = build_bilibili_plugin(
                _bili_runtime_config(port, bili_token),
                http_client=bili_http,
                sleep=_noop_sleep,
            )
            bili_task2 = asyncio.create_task(bili_plugin2.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        svc.modules.get("bilibili_surf") is not None
                        and bili_plugin2.engine._initialized
                    )
                )
                re_get = await client.get("/admin/api/bili-account")
                assert re_get.status_code == 200
                assert re_get.json().get("plugin_offline") is not True
                assert re_get.json()["watched_count"] == 1
                assert re_get.json()["last_error"] == "bili_account_risk"
                assert await svc.bili_watcher.lookup_watched(BV1) is not None
            finally:
                await bili_plugin2.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(bili_task2, timeout=3.0)
                await mc_plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(mc_task, timeout=3.0)


async def test_video_watch_buvid4_autofill_initialize_and_secret_scrubbing(
    settings: Settings, tmp_path: Any
) -> None:
    class WatchGateway(FakeGateway):
        async def watch_video(self, path, payload, subject, **kwargs):
            await guard_call(kwargs.get("before_call"))
            return "原片内容验证通过。"

    class _Dl:
        def __init__(self, path: Any) -> None:
            self.path = path
            self.cookies_seen: list[dict[str, str]] = []

        async def cleanup_stale(self) -> None:
            return None

        @asynccontextmanager
        async def download(self, bvid: str, **kwargs: Any):
            self.cookies_seen.append(dict(kwargs["cookies"]))
            await guard_call(kwargs.get("guard"))
            yield SimpleNamespace(path=self.path, duration=30, bytes=1024)

    s = _base_settings(settings, enable_video_file_watch=True)
    app = create_app(s, gateway_factory=lambda *_: WatchGateway(), start_workers=False)
    http_calls: list[httpx.Request] = []
    responses = _default_bili_responses(with_subtitles=False)

    async with _running_app(app) as (port, client):
        svc = app.state.service
        dl = _Dl(tmp_path / "autofill.mp4")
        svc.video_watch.downloader = dl

        created = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "bilibili_surf",
                "display_name": "B 站巡逻",
                "scopes": list(BILIBILI_SCOPES),
                "node_region": "KR",
            },
        )
        assert created.status_code == 200
        token = created.json()["token"]

        async with _make_bili_http_client(responses, http_calls) as bili_http:
            plugin = build_bilibili_plugin(
                _bili_runtime_config(port, token),
                http_client=bili_http,
                sleep=_noop_sleep,
            )
            runner = asyncio.create_task(plugin.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        svc.modules.get("bilibili_surf") is not None and plugin.engine._initialized
                    )
                )

                # Invalid cookie input must return 400 and never reflect the secret
                bad_secret = "unit-bad-secret-value; EVIL=1"
                bad_res = await client.post(
                    "/admin/api/bili-account/credentials",
                    json={"sessdata": bad_secret},
                )
                assert bad_res.status_code == 400
                assert bad_secret not in bad_res.text

                # Set cookies without buvid4; immediately run VideoWatch without prior nav()
                c_res = await client.post(
                    "/admin/api/bili-account/credentials",
                    json={
                        "sessdata": "autofill-sess-secret-111",
                        "bili_jct": "autofill-jct-secret-222",
                        "buvid3": "autofill-b3-secret-333",
                    },
                )
                assert c_res.status_code == 200
                assert c_res.json()["credentials"]["buvid4"] is False

                mat = {
                    "bvid": BV1,
                    "title": "无字幕视频",
                    "description": "测试 buvid4 自动补全",
                    "duration": 30,
                    "subtitle_excerpt": "",
                    "subtitle_complete": False,
                    "subtitle_status": "unavailable",
                    "hot_comments_unverified": [],
                    "material_scope": "metadata_only",
                }
                analysis = await svc.video_watch.analyze(BV1, mat, "chat:autofill", "chat")
                assert analysis.mode == "video"
                assert len(dl.cookies_seen) == 1
                assert dl.cookies_seen[0]["buvid4"] == "spi-buvid4-value"
                assert "spi-buvid4-value" in configured_secrets(svc.s)

                # bilibili_enabled=False -> RemoteBiliClient raises Rejected("bilibili_disabled")
                svc.s.bilibili_enabled = False
                try:
                    with pytest.raises(Rejected, match="bilibili_disabled"):
                        await svc.bili.video(BV1)
                finally:
                    svc.s.bilibili_enabled = True

                # Fresh RemoteBiliWatcher.initialize() loads blocked_until & secrets from KV
                fresh_watcher = RemoteBiliWatcher(svc)
                await fresh_watcher._kv_set(
                    "account_state",
                    {
                        "blocked_until": fresh_watcher.clock() + 600.0,
                        "last_error": "bili_account_risk",
                    },
                )
                svc.secrets.clear()
                await fresh_watcher.initialize()
                assert fresh_watcher.blocked_until > fresh_watcher.clock()
                assert "autofill-sess-secret-111" in svc.secrets
                assert "spi-buvid4-value" in svc.secrets
            finally:
                await plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(runner, timeout=3.0)


async def test_e2e_bili_private_topic_patrol_cover_thumb_and_offline_filter(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hakimi.session_manager import SessionManager
    from tests.test_v20_p4_cross_module_mc import FakeNodeTransport

    class PrivatePatrolGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.selection_payloads: list[dict[str, Any]] = []

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            if str(kwargs.get("scenario")) == "bili_selection":
                self.selection_payloads.append(dict(payload))
                return json.dumps(
                    {
                        "selected": [{"bvid": BV1, "like": True, "share_owner": True}],
                        "mood_longnick": "不应外泄的私聊心情",
                    },
                    ensure_ascii=False,
                )
            return "这是一个很棒的极光科普视频。"

    gateway = PrivatePatrolGateway()
    s = _base_settings(
        settings,
        external_plugins={"bilibili_surf"},
        onebot_enabled=True,
        onebot_self_id="10000",
        onebot_owner_ids={"20000"},
        onebot_allowed_users={"20000", "30000", "40001"},
        onebot_allowed_groups={"40000"},
    )
    app = create_app(s, gateway_factory=lambda *_: gateway, start_workers=False)
    http_calls: list[httpx.Request] = []
    responses = _default_bili_responses(with_subtitles=True)
    responses["/x/web-interface/view"]["pic"] = "http://i0.hdslb.com/bfs/archive/aurora.jpg"

    async with _running_app(app) as (port, client):
        svc = app.state.service
        svc.sender = FakeSender()
        sessions = svc.sessions = SessionManager(svc.s, svc.store)

        friend_msg = Message(
            platform="onebot",
            bot_id="10000",
            event_id="ev-e2e-priv-1",
            user_id="30000",
            kind="private",
            conversation_id="30000",
            text="帮我找个极光视频 13800138000",
            addressed=True,
            created_at=svc.life_engine.clock(),
        )
        other_friend = Message(
            platform="onebot",
            bot_id="10000",
            event_id="ev-e2e-priv-2",
            user_id="40001",
            kind="private",
            conversation_id="40001",
            text="你好",
            addressed=True,
            created_at=svc.life_engine.clock(),
        )
        group_msg = Message(
            platform="onebot",
            bot_id="10000",
            event_id="ev-e2e-grp-1",
            user_id="30000",
            kind="group",
            conversation_id="40000",
            text="群聊消息",
            addressed=True,
            created_at=svc.life_engine.clock(),
        )
        await sessions.remember(friend_msg, friend_msg.text, "好")

        async def _reg() -> list[tuple[str, Message]]:
            return [
                (svc.store.session_scope(friend_msg), friend_msg),
                (svc.store.session_scope(group_msg), group_msg),
            ]

        monkeypatch.setattr(svc.store.runtime, "sessions", _reg)

        good_jpg = b"\xff\xd8\xff\xe0" + b"j" * 64
        nt = FakeNodeTransport(is_connected=True)
        nt.responses["bili.cover_thumb"] = {
            "mime_type": "image/jpeg",
            "bytes": len(good_jpg),
            "data_b64": base64.b64encode(good_jpg).decode("ascii"),
        }
        svc.node_transport = nt

        created = await client.post(
            "/admin/api/plugins",
            json={
                "plugin_id": "bilibili_surf",
                "display_name": "B 站巡逻",
                "scopes": list(BILIBILI_SCOPES),
                "node_region": "KR",
            },
        )
        assert created.status_code == 200
        token = created.json()["token"]

        async with _make_bili_http_client(responses, http_calls) as bili_http:
            plugin = build_bilibili_plugin(
                _bili_runtime_config(port, token),
                http_client=bili_http,
                sleep=_noop_sleep,
            )
            runner = asyncio.create_task(plugin.client.run_forever())
            try:
                await _wait_until(
                    lambda: (
                        (mod := svc.modules.get("bilibili_surf")) is not None
                        and isinstance(mod, RemoteModuleProxy)
                        and mod._session.ready
                        and plugin.engine._initialized
                    )
                )
                proxy = svc.modules.get("bilibili_surf")
                assert isinstance(proxy, RemoteModuleProxy)
                assert proxy._action_timeouts["bili.patrol"] == 1500.0

                # 1) Cover thumbnail E2E via RemoteBiliWatcher -> bili.cover_thumb -> node_transport
                plugin.engine.set_node_connected(True)
                thumb = await svc.bili_watcher.fetch_cover_thumbnail(
                    "https://i0.hdslb.com/bfs/archive/aurora.jpg"
                )
                assert thumb is not None and thumb["bytes"] == len(good_jpg)
                # http:// and evil host rejected strictly without calling node_transport
                nt_calls_before = len(nt.calls)
                assert (
                    await svc.bili_watcher.fetch_cover_thumbnail(
                        "http://i0.hdslb.com/bfs/archive/aurora.jpg"
                    )
                    is None
                )
                assert (
                    await svc.bili_watcher.fetch_cover_thumbnail(
                        "https://evil.invalid/bfs/archive/aurora.jpg"
                    )
                    is None
                )
                assert len(nt.calls) == nt_calls_before
                plugin.engine.set_node_connected(False)

                # 2) Configure creds & run autonomous patrol driven by private friend request
                assert (
                    await client.post(
                        "/admin/api/bili-account/credentials",
                        json={
                            "sessdata": "e2e-priv-sess-1234",
                            "bili_jct": "e2e-priv-jct-1234",
                            "buvid3": "e2e-priv-b3-1234",
                            "buvid4": "e2e-priv-b4-1234",
                        },
                    )
                ).status_code == 200
                assert (
                    await client.put(
                        "/admin/api/bili-account",
                        json={"enabled": True, "allow_like": True},
                    )
                ).status_code == 200

                run_res = await svc.bili_watcher.run_once(slot="e2e-priv-slot")
                assert run_res == {"status": "done", "learned": 1, "reason": None}
                assert len(gateway.selection_payloads) == 1
                sel_prefs = gateway.selection_payloads[0]["memory_preferences"]
                assert sel_prefs["private_topic_refs"] == ["recent_chat_topics:0"]
                assert "极光视频" in sel_prefs["recent_chat_topics"][0]
                assert "13800138000" not in json.dumps(sel_prefs, ensure_ascii=False)
                assert "30000" not in json.dumps(sel_prefs, ensure_ascii=False)
                # No public POST interactions on private-first patrol
                assert not any(r.method == "POST" for r in http_calls)

                # Verify world event has private_first=True and NO title/summary
                ev_row = await svc.store.one(
                    "SELECT payload_json FROM global_events "
                    "WHERE event_type='bili_video_watched' ORDER BY created_at DESC LIMIT 1"
                )
                assert ev_row is not None
                ev_payload = json.loads(ev_row["payload_json"])
                assert ev_payload["private_first"] is True
                assert ev_payload["owner_private"] is False
                assert "title" not in ev_payload and "summary" not in ev_payload

                # Online lookup_watched & recent_surfed enforce 24h private scope isolation
                lk_friend = await svc.bili_watcher.lookup_watched(BV1, message=friend_msg)
                assert lk_friend is not None
                assert "公开字幕学习" in lk_friend["prior_patrol_notes"]
                assert await svc.bili_watcher.lookup_watched(BV1, message=other_friend) is None
                assert await svc.bili_watcher.lookup_watched(BV1, message=group_msg) is None
                assert await svc.bili_watcher.lookup_watched(BV1) is None

                assert [
                    i["bvid"] for i in await svc.bili_watcher.recent_surfed(message=friend_msg)
                ] == [BV1]
                assert await svc.bili_watcher.recent_surfed(message=other_friend) == []
                assert await svc.bili_watcher.recent_surfed(message=group_msg) == []
            finally:
                await plugin.close()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(runner, timeout=3.0)

        # Offline fallback also enforces 24h private scope isolation
        await _wait_until(lambda: svc.modules.get("bilibili_surf") is None)
        assert await svc.bili_watcher.lookup_watched(BV1, message=friend_msg) is not None
        assert await svc.bili_watcher.lookup_watched(BV1, message=other_friend) is None
        assert await svc.bili_watcher.lookup_watched(BV1, message=group_msg) is None
        assert [i["bvid"] for i in await svc.bili_watcher.recent_surfed(message=friend_msg)] == [
            BV1
        ]
        assert await svc.bili_watcher.recent_surfed(message=other_friend) == []
        assert await svc.bili_watcher.recent_surfed(message=group_msg) == []


async def test_e2e_wave3_all_three_remote_plugins_over_real_ws_and_fallback(
    settings: Settings,
):
    """Real uvicorn /ws/plugin E2E: web_tools + doc_render + song_finder simultaneously online."""
    import io
    import zipfile

    from hakimi import text_card
    from hakimi.models import FileRef
    from hakimi.web_search import WebSearchConfig
    from plugins.doc_render.plugin_main import DocRenderPlugin
    from plugins.song_finder.plugin_main import SongFinderPlugin
    from plugins.web_tools.plugin_main import WebToolsPlugin
    from plugins.web_tools.search import SearchProviderConfig

    s = settings.model_copy(
        update={
            "admin_token": SecretStr(ADMIN_TOKEN),
            "owner_qq_id": "20000",
            "plugin_bus_ws_enabled": True,
            "file_read_enabled": True,
            "web_search_enabled": True,
        }
    )
    gw = FakeGateway()
    screen_calls: list[dict[str, Any]] = []
    orig_screen = gw.screen

    async def _recording_screen(payload, subject, **kwargs):
        screen_calls.append({"payload": payload, "subject": subject, "kwargs": kwargs})
        return await orig_screen(payload, subject, **kwargs)

    gw.screen = _recording_screen  # type: ignore[method-assign]
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with _running_app(app) as (port, client):
        svc = app.state.service
        bus = svc.plugin_bus
        # Verify Service.__init__ wired all Wave 3 plugin_bus handles automatically.
        assert svc.files.plugin_bus is bus
        assert svc.songs.plugin_bus is bus
        assert svc.store.runtime.web_search.plugin_bus is bus
        assert text_card._get_bridge_state()[0] is bus

        await svc.store.runtime.web_search.save(
            WebSearchConfig(enabled=True, daily_limit=10, monthly_google_limit=5000)
        )

        # Fake Colab compute capturing inbox upload + rvc_cover job submission
        inbox_uploads: list[tuple[str, bytes, str]] = []
        submitted_jobs: list[tuple[str, dict[str, Any], str]] = []

        class _StubCompute:
            def configured(self) -> bool:
                return True

            async def upload_drive_inbox(
                self, filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
            ) -> dict[str, Any]:
                inbox_uploads.append((filename, raw, mime_type))
                return {"file_id": f"drive-inbox-{len(inbox_uploads)}", "name": filename}

            async def submit_job(
                self, job_type: str, payload: dict[str, Any], *, requester: str = "owner"
            ) -> dict[str, Any]:
                submitted_jobs.append((job_type, dict(payload), requester))
                return {
                    "id": f"job-{len(submitted_jobs)}",
                    "job_type": job_type,
                    "payload": payload,
                }

        svc.songs.compute = _StubCompute()

        # Provision tokens for all 3 Wave 3 plugins via admin API
        tokens: dict[str, str] = {}
        for pid, disp in (
            ("web_tools", "网页检索与预览"),
            ("doc_render", "文档解析与渲染"),
            ("song_finder", "音源检索器"),
        ):
            resp = await client.post(
                "/admin/api/plugins",
                json={
                    "plugin_id": pid,
                    "display_name": disp,
                    "scopes": ["actions"],
                    "node_region": "KR",
                },
            )
            assert resp.status_code == 200
            tokens[pid] = resp.json()["token"]

        ws_url = f"ws://127.0.0.1:{port}/ws/plugin"

        # 1) Build WebToolsPlugin with mocked public DNS + HTTP transport
        async def _fake_dns(_host: str, _port: int) -> list[str]:
            return ["93.184.216.34"]

        def _web_handler(req: httpx.Request) -> httpx.Response:
            if req.url.path == "/article":
                html = (
                    "<html><head><title>Wave3 Preview Title</title>"
                    '<meta name="description" content="Wave3 safe summary"></head>'
                    "<body>ok</body></html>"
                )
                return httpx.Response(
                    200,
                    headers={"Content-Type": "text/html; charset=utf-8"},
                    text=html,
                )
            if req.url.path == "/search":
                return httpx.Response(
                    200,
                    json={
                        "results": [
                            {
                                "title": "Official Spec",
                                "content": "Wave 3 remote search verified fact.",
                                "url": "https://example.org/spec",
                            }
                        ]
                    },
                )
            return httpx.Response(404)

        web_plugin = WebToolsPlugin(
            ws_url=ws_url,
            token=tokens["web_tools"],
            search_config=SearchProviderConfig(
                default_provider="searxng",
                provider_order=("searxng",),
                searxng_base_url="https://search.example.org",
            ),
            transport=httpx.MockTransport(_web_handler),
            dns_resolver=_fake_dns,
            reconnect_min=0.05,
            reconnect_max=0.2,
        )

        # 2) Build DocRenderPlugin (real sandboxed parser + real Pillow renderer)
        doc_plugin = DocRenderPlugin(
            ws_url=ws_url,
            token=tokens["doc_render"],
            reconnect_min=0.05,
            reconnect_max=0.2,
        )

        # 3) Build SongFinderPlugin with deterministic stub engine returning >150 KiB audio
        chunked_audio = b"ID3" + (b"\xab\xcd" * 80_000)  # 160,003 bytes -> triggers chunked blob

        class _StubSongEngine:
            def status_snapshot(self) -> dict[str, Any]:
                return {"provider_order": ["youtube", "bilibili_public"], "cooldowns": {}}

            async def search(self, **_kw: Any) -> dict[str, Any]:
                return {
                    "candidates": [],
                    "provider_used": "youtube",
                    "skipped_providers": {},
                }

            async def fetch_source(self, *_args: Any, **_kw: Any) -> SimpleNamespace:
                import hashlib

                return SimpleNamespace(
                    provider="youtube",
                    title="星辰大海",
                    uploader="Official Artist",
                    duration_sec=210.0,
                    audio_format="mp3",
                    mime_type="audio/mpeg",
                    filename="xingchen.mp3",
                    byte_size=len(chunked_audio),
                    data=chunked_audio,
                    sha256=hashlib.sha256(chunked_audio).hexdigest(),
                    skipped_providers={},
                )

        song_plugin = SongFinderPlugin(
            ws_url=ws_url,
            token=tokens["song_finder"],
            engine=_StubSongEngine(),  # type: ignore[arg-type]
            reconnect_min=0.05,
            reconnect_max=0.2,
        )

        runners = [
            asyncio.create_task(web_plugin.client.run_forever()),
            asyncio.create_task(doc_plugin.client.run_forever()),
            asyncio.create_task(song_plugin.client.run_forever()),
        ]
        try:
            await _wait_until(
                lambda: (
                    bus.has_tool("web_tools", "web_tools.search")
                    and bus.has_tool("web_tools", "web_tools.preview")
                    and bus.has_tool("doc_render", "doc_render.read_file")
                    and bus.has_tool("doc_render", "doc_render.render_card")
                    and bus.has_tool("song_finder", "song_finder.search")
                    and bus.has_tool("song_finder", "song_finder.fetch_source")
                )
            )

            # Verify /admin/api/plugins overview shows all 3 online with 2 actions each
            ov = (await client.get("/admin/api/plugins")).json()
            by_id = {p["plugin_id"]: p for p in ov["plugins"]}
            for pid in ("web_tools", "doc_render", "song_finder"):
                assert by_id[pid]["online"] is True
                assert by_id[pid]["actions_count"] == 2

            # --- Test 1: web_tools.preview via Service.enrich() ---
            probe_msg = Message(
                platform="onebot",
                bot_id=s.onebot_self_id,
                event_id="e2e-w3-preview",
                user_id="20000",
                conversation_id="20000",
                kind="private",
                text="看看这个 https://example.org/article",
                addressed=True,
                created_at=1_700_000_000.0,
            )
            previews = await svc.enrich(probe_msg)
            assert len(previews) == 1
            assert previews[0]["title"] == "Wave3 Preview Title"

            # --- Test 2: web_tools.search via WebSearch.execute_search() ---
            findings = await svc.store.runtime.web_search.execute_search(
                "Wave 3 architecture",
                "owner:20000",
                ai=svc.ai,
                plugin_bus=bus,
            )
            assert "Wave 3 remote search verified fact" in findings
            assert any(c["kwargs"].get("budget_lane") == "search" for c in screen_calls)
            attempt_row = await svc.store.one(
                "SELECT status, provider FROM web_search_attempts ORDER BY created DESC LIMIT 1"
            )
            assert attempt_row is not None and attempt_row["status"] == "completed"

            # --- Test 3: doc_render.read_file (.docx) via Service.files ---
            docx_buf = io.BytesIO()
            with zipfile.ZipFile(docx_buf, "w") as zf:
                zf.writestr(
                    "word/document.xml",
                    (
                        '<?xml version="1.0" encoding="UTF-8"?>'
                        '<w:document xmlns:w="'
                        'http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                        "<w:body><w:p><w:r>"
                        "<w:t>Wave3 DOCX Remote Parsed OK</w:t>"
                        "</w:r></w:p></w:body>"
                        "</w:document>"
                    ),
                )
            docx_raw = docx_buf.getvalue()

            class _FakeOneBotTransport:
                async def call(self, action: str, _params: dict[str, Any]) -> dict[str, Any]:
                    if action == "get_file":
                        return {
                            "file_size": len(docx_raw),
                            "base64": base64.b64encode(docx_raw).decode("ascii"),
                        }
                    return {}

            svc.files.transport = _FakeOneBotTransport()
            loaded_doc = await svc.files.read_one(
                FileRef(file_id="f-docx-1", name="spec.docx", file_size=len(docx_raw)),
                probe_msg,
                "owner:20000",
            )
            assert "Wave3 DOCX Remote Parsed OK" in loaded_doc.content

            # --- Test 4: doc_render.render_card via worker-thread sync render_card() ---
            rendered_b64 = await asyncio.to_thread(
                text_card.render_card, "# Wave 3\n\n```python\nprint('hello')\n```"
            )
            assert rendered_b64.startswith("base64://")
            rendered_bytes = base64.b64decode(rendered_b64[len("base64://") :])
            assert rendered_bytes.startswith(b"\x89PNG\r\n\x1a\n")

            # --- Test 5: song_finder.fetch_source (>150 KiB chunked) -> inbox -> colab ---
            job = await svc.songs.request_learn_song("星辰大海", auto_search=True)
            assert job["job_type"] == "rvc_cover"
            assert len(inbox_uploads) == 1
            assert inbox_uploads[0][1] == chunked_audio
            assert len(submitted_jobs) == 1
            assert submitted_jobs[0][1]["inbox_file_id"] == "drive-inbox-1"
            assert submitted_jobs[0][1]["auto_search"] is False
        finally:
            await asyncio.gather(
                web_plugin.close(),
                doc_plugin.close(),
                song_plugin.close(),
                return_exceptions=True,
            )
            for r in runners:
                r.cancel()
            await asyncio.gather(*runners, return_exceptions=True)

        # --- Test 6: All 3 offline -> smooth fallback without breakage ---
        await _wait_until(
            lambda: (
                not bus.is_plugin_online("web_tools")
                and not bus.is_plugin_online("doc_render")
                and not bus.is_plugin_online("song_finder")
            )
        )
        fallback_card = await asyncio.to_thread(text_card.render_card, "离线回退卡片测试")
        assert fallback_card.startswith("base64://")
        assert base64.b64decode(fallback_card[len("base64://") :]).startswith(b"\xff\xd8\xff")

        # Offline song_finder fails cleanly at resource-prep stage without polluting Colab queue
        prev_jobs = len(submitted_jobs)
        with pytest.raises(Rejected, match="song_finder_remote_unavailable"):
            await svc.songs.request_learn_song("离线拒绝曲", auto_search=True)
        assert len(submitted_jobs) == prev_jobs

    # After app lifespan stops (Service.stop), process-wide text_card bridge is cleanly unbound.
    assert text_card._get_bridge_state()[0] is None
