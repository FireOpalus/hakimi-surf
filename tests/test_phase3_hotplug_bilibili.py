"""Unit and RPC tests for standalone `plugins/bilibili/` hot-plug package."""

from __future__ import annotations

import ast
import asyncio
import base64
import contextlib
import json
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from hakimi_plugin_sdk import PluginRpcError, PluginRuntimeConfig
from plugins.bilibili.client import (
    BiliClientError,
    BiliPublicClient,
    extract_bvid,
    extract_video_ref,
)
from plugins.bilibili.patrol import (
    BiliPatrolEngine,
    BiliPolicy,
    CredentialUpdate,
    Pick,
    Selection,
    has_voucher,
    learning_source_label,
    subtitle_text,
    subtitle_timestamp,
    validate_cookie,
    video_duration,
    wbi_keys,
    wbi_sign,
)
from plugins.bilibili.plugin_main import (
    BilibiliPlugin,
    build_bilibili_plugin,
    build_bilibili_plugin_from_env,
)

BV1 = "BV1xx411c7mD"
BV2 = "BV1Q541167Qg"
BV3 = "BV19z4y1S7fQ"

NAV_DATA = {
    "isLogin": True,
    "mid": 123456,
    "wbi_img": {
        "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
        "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
    },
}


class _FakePluginClient:
    """In-memory mock of HakimiPluginClient for isolated engine/client tests."""

    def __init__(self, initial_kv: dict[str, Any] | None = None) -> None:
        self.connected = True
        self.kv: dict[str, Any] = dict(initial_kv or {})
        self.node_calls: list[tuple[str, dict[str, Any]]] = []
        self.node_handler: Any = None
        self.status_pushes: list[dict[str, Any]] = []
        self.activities: list[dict[str, Any]] = []
        self.world_events: list[dict[str, Any]] = []
        self.owner_notifications: list[tuple[str, str]] = []
        self.ai_calls: list[tuple[str, dict[str, Any]]] = []
        self.budget_response: dict[str, Any] = {"available": True, "hard_stop": False}
        self.screen_response: dict[str, Any] = {"passed": True}
        self.selection_response: str = json.dumps(
            {
                "selected": [{"bvid": BV1, "like": True}],
                "mood_longnick": "今天刷到了好玩的科技视频",
            }
        )
        self.video_analyze_response: dict[str, Any] = {
            "text": "视频主要讲了异步调度器原理，评论区都在聊并发安全。",
            "mode": "text",
            "material_scope": "partial_subtitle",
            "reason": None,
        }
        self.on_screen_hook: Any = None
        self.on_video_analyze_hook: Any = None

    async def state_get(self, key: str) -> Any:
        return self.kv.get(key)

    async def state_set(self, key: str, value: Any) -> None:
        raw = json.dumps(value, ensure_ascii=False, allow_nan=False)
        assert len(raw.encode("utf-8")) <= 65_536
        self.kv[key] = json.loads(raw)

    async def state_delete(self, key: str) -> None:
        self.kv.pop(key, None)

    async def node_call(
        self, action: str, params: dict[str, Any] | None = None, **_: Any
    ) -> dict[str, Any]:
        payload = dict(params or {})
        self.node_calls.append((action, payload))
        if self.node_handler is None:
            raise PluginRpcError("node_disconnected")
        res = self.node_handler(action, payload)
        if asyncio.iscoroutine(res):
            return await res
        return res

    async def push_status(
        self,
        status: dict[str, Any],
        *,
        configuration_issue: str | None = None,
        unavailable_actions: dict[str, str | None] | None = None,
        current_activity: Any = ...,
    ) -> None:
        entry: dict[str, Any] = {
            "status": dict(status),
            "configuration_issue": configuration_issue,
        }
        if unavailable_actions is not None:
            entry["unavailable_actions"] = dict(unavailable_actions)
        if current_activity is not ...:
            entry["current_activity"] = current_activity
        self.status_pushes.append(entry)

    async def report_activity(
        self,
        active_task: str = "",
        *,
        disconnected: bool = False,
        updated_at: float | None = None,
        kind: str | None = None,
        active: bool | None = None,
    ) -> None:
        self.activities.append(
            {
                "active_task": active_task,
                "disconnected": disconnected,
                "updated_at": updated_at,
                "kind": kind,
                "active": active,
            }
        )

    async def emit_world_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        importance: str = "normal",
        business_key: str | None = None,
        summary: str | None = None,
        report_text: str | None = None,
        mood_longnick: str | None = None,
    ) -> None:
        self.world_events.append(
            {
                "event_type": event_type,
                "payload": dict(payload),
                "importance": importance,
                "business_key": business_key,
                "summary": summary,
                "report_text": report_text,
                "mood_longnick": mood_longnick,
            }
        )

    async def notify_owner(self, text: str, *, code: str = "notify") -> None:
        self.owner_notifications.append((code, text))

    async def ai_budget_check(self, lane: str = "patrol", **_: Any) -> dict[str, Any]:
        self.ai_calls.append(("budget_check", {"lane": lane}))
        return dict(self.budget_response)

    async def ai_screen(self, payload: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        self.ai_calls.append(("screen", {"payload": payload, **kwargs}))
        if self.on_screen_hook is not None:
            res = self.on_screen_hook(payload)
            if asyncio.iscoroutine(res):
                await res
        return dict(self.screen_response)

    async def ai_generate(
        self, scenario: str, payload: dict[str, Any] | None = None, **kwargs: Any
    ) -> str:
        self.ai_calls.append(("generate", {"scenario": scenario, "payload": payload, **kwargs}))
        return self.selection_response

    async def ai_video_analyze(
        self, bvid: str, material: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        self.ai_calls.append(("video_analyze", {"bvid": bvid, "material": material, **kwargs}))
        if self.on_video_analyze_hook is not None:
            res = self.on_video_analyze_hook(bvid, material)
            if asyncio.iscoroutine(res):
                await res
        return dict(self.video_analyze_response)


class _FakeSocket:
    """In-memory WebSocket peer driving real HakimiPluginClient framing."""

    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.sent_event = asyncio.Event()
        self.close_code: int | None = None

    async def push(self, frame: dict[str, Any]) -> None:
        await self.incoming.put(json.dumps(frame, ensure_ascii=False))

    async def recv(self) -> str:
        item = await self.incoming.get()
        if item.startswith("__EOF__:"):
            code = int(item.split(":", 1)[1])
            self.close_code = code
            raise ConnectionError(f"closed:{code}")
        return item

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))
        self.sent_event.set()

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        await self.incoming.put(f"__EOF__:{code}")

    async def wait_sent(self, predicate: Any, *, max_wait: float = 2.0) -> dict[str, Any]:
        async with asyncio.timeout(max_wait):
            while True:
                for item in self.sent:
                    if predicate(item):
                        return item
                self.sent_event.clear()
                await self.sent_event.wait()


def _make_bili_http_responses() -> dict[str, Any]:
    return {
        "/x/web-interface/nav": NAV_DATA,
        "/x/frontend/finger/spi": {"b_3": "spi-b3", "b_4": "spi-buvid4-value"},
        "/x/web-interface/wbi/index/top/feed/rcmd": {
            "item": [
                {
                    "goto": "av",
                    "bvid": BV1,
                    "title": "异步调度器",
                    "owner": {"name": "UP1"},
                    "tname": "科技",
                },
                {
                    "goto": "av",
                    "bvid": BV2,
                    "title": "红石生电",
                    "owner": {"name": "UP2"},
                    "tname": "游戏",
                },
            ]
        },
        "/x/polymer/web-dynamic/v1/feed/all": {"items": []},
        "/x/web-interface/view": {
            "bvid": BV1,
            "aid": 100,
            "cid": 200,
            "duration": 60,
            "pubdate": 1700000000,
            "title": "异步调度器",
            "desc": "公开简介",
            "owner": {"mid": "12345", "name": "UP1"},
            "stat": {"view": 1024, "like": 88},
            "tname": "科技",
        },
        "/x/player/wbi/v2": {
            "subtitle": {
                "subtitles": [
                    {
                        "lan": "zh-CN",
                        "subtitle_url": "https://aisubtitle.hdslb.com/bfs/subtitle/test.json",
                    }
                ]
            }
        },
        "/bfs/subtitle/test.json": {"body": [{"content": "公开字幕文字", "from": 0, "to": 60}]},
        "/x/v2/reply": {"replies": [{"content": {"message": "一条高赞评论"}}]},
        "/x/web-interface/archive/relation": {"like": False},
        "/x/web-interface/archive/like": {},
        "/x/v3/fav/folder/created/list-all": {"list": [{"id": 888}]},
        "/x/v3/fav/resource/deal": {},
        "/room/v1/Room/get_info": {"live_status": 1, "title": "直播标题", "area_name": "科技"},
    }


def _make_mock_http(
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
            json=value if req.url.path.startswith("/bfs/") else {"code": 0, "data": value},
        )

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# 1. Zero hakimi imports & core parsing / WBI / model validation
# ---------------------------------------------------------------------------


def test_zero_hakimi_imports_in_bilibili_plugin() -> None:
    root = Path(__file__).resolve().parents[1]
    bili_dir = root / "plugins" / "bilibili"
    py_files = sorted(bili_dir.glob("*.py"))
    assert len(py_files) == 4

    allowed_third_party = {"hakimi_plugin_sdk", "httpx", "pydantic"}
    stdlib_names = getattr(sys, "stdlib_module_names", frozenset())

    for path in py_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".", 1)[0]
                    assert top != "hakimi", f"{path} imports hakimi"
                    assert top in allowed_third_party or top in stdlib_names
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    continue
                mod = node.module or ""
                top = mod.split(".", 1)[0]
                assert top != "hakimi", f"{path} imports from hakimi"
                assert top in allowed_third_party or top in stdlib_names


def test_extract_bvid_and_validation_helpers() -> None:
    assert extract_bvid(BV1) == BV1
    assert extract_video_ref(BV1) == (BV1, 1)
    assert extract_bvid(f"看看这个 https://www.bilibili.com/video/{BV1}?spm_id_from=333") == BV1
    assert extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=3&spm_id_from=333") == (
        BV1,
        3,
    )
    assert extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=999") == (BV1, 999)
    assert extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=0") == (BV1, 1)
    assert extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=1000") == (BV1, 1)
    assert extract_video_ref(f"https://www.bilibili.com/video/{BV1}?p=abc") == (BV1, 1)
    assert extract_bvid(f"https://m.bilibili.com/video/{BV2}/") == BV2
    assert extract_bvid(f"https://bilibili.com/video/{BV3}") == BV3
    assert extract_video_ref(f"http://www.bilibili.com/video/{BV1}?p=2") == (BV1, 2)
    assert extract_video_ref(f"http://m.bilibili.com:80/video/{BV2}/?p=5") == (BV2, 5)
    assert extract_video_ref(f"https://www.bilibili.com:443/video/{BV3}?p=4") == (BV3, 4)
    assert extract_video_ref(f"https://www.bilibili.com/festival?bvid={BV1}") == (BV1, 1)
    assert extract_video_ref(f"https://www.bilibili.com/festival/spring_2025?bvid={BV2}&p=7") == (
        BV2,
        7,
    )
    assert extract_video_ref(f"http://bilibili.com/festival/my-event/?bvid={BV3}&p=2") == (BV3, 2)
    assert extract_bvid(f"https://www.bilibili.com/video/{BV1} {BV2}") == BV1

    for bad in (
        "https://127.0.0.1/video/" + BV1,
        "http://evil.test/video/" + BV1,
        "https://www.bilibili.com.evil.test/video/" + BV1,
        "https://www.bilibili.com@evil.test/video/" + BV1,
        "https://user:pass@www.bilibili.com/video/" + BV1,
        "https://www.bilibili.com:8443/video/" + BV1,
        "https://www.bilibili.com:80/video/" + BV1,
        "http://www.bilibili.com:443/video/" + BV1,
        "http://www.bilibili.com:8080/video/" + BV1,
        "https://www.bilibili.com/video/" + BV1 + "/../../admin",
        "https://www.bilibili.com/festival/../admin?bvid=" + BV1,
        "https://www.bilibili.com/festival/a/b?bvid=" + BV1,
        "https://www.bilibili.com/festival/bad..slug?bvid=" + BV1,
        "https://www.bilibili.com/festival?bvid=NOTABVID",
        "https://b23.tv/abc123",
    ):
        assert extract_bvid(bad) is None, bad
        assert extract_video_ref(bad) is None, bad

    # WBI fixed vector matching built-in bili_watcher
    values = {"foo": "114", "bar": "514", "zab": 1919810}
    signed = wbi_sign(
        values,
        ("7cd084941338484aae1ad9425b84077c", "4932caff0ff746eab6f01bf08b70ac45"),
        1702204169,
    )
    assert signed["w_rid"] == "8f6f2b5b3d485fe1886cec6a0be8c5d4"
    assert "wts" not in values

    with pytest.raises(BiliClientError, match="bili_wbi_unavailable"):
        wbi_sign({}, ("short", "key"), 1702204169)
    assert wbi_keys(NAV_DATA) == ("a" * 32, "b" * 32)
    assert has_voucher({"nested": [{"v_voucher": "challenge"}]}) is True
    assert has_voucher({"ok": 1}) is False

    assert video_duration(60) == 60.0
    assert video_duration(True) is None
    assert video_duration(0) is None
    assert subtitle_timestamp(0) == 0.0
    assert subtitle_timestamp(False) is None
    assert subtitle_text({"body": [{"content": "字幕", "from": 0, "to": 60}]}, 60) == (
        "字幕",
        True,
        "complete",
    )
    assert subtitle_text({"body": [{"content": "字幕", "from": 50, "to": 60}]}, 60)[1:] == (
        False,
        "partial",
    )
    assert subtitle_text({"body": []}, 60) == ("", False, "unavailable")

    with pytest.raises(ValueError, match="cookie value"):
        validate_cookie("SESSDATA=abc; path=/")
    with pytest.raises(ValidationError):
        BiliPolicy(enabled=True, allow_favorite=True)
    with pytest.raises(ValidationError):
        CredentialUpdate(sessdata="bad\ncookie")
    with pytest.raises(ValidationError):
        Pick(bvid="invalid")
    with pytest.raises(ValidationError):
        Selection(selected=[Pick(bvid=BV1), Pick(bvid=BV2), Pick(bvid=BV3)])


# ---------------------------------------------------------------------------
# 2. BiliPublicClient: CN delegation, local fallback, 412 circuit breaker
# ---------------------------------------------------------------------------


async def test_public_client_cn_delegation_pacing_cache_and_short_links() -> None:
    now_box = [100.0]
    sleeps: list[float] = []

    async def fake_sleep(n: float) -> None:
        sleeps.append(n)
        now_box[0] += n

    fake_client = _FakePluginClient()

    def node_handler(action: str, params: dict[str, Any]) -> dict[str, Any]:
        if action == "bili.resolve_short":
            return {"location": f"https://www.bilibili.com/video/{BV1}?share=1"}
        assert action == "bili.public_get"
        if params["path"] == "/x/web-interface/view":
            return {
                "code": 0,
                "data": {
                    "bvid": BV1,
                    "title": "测试视频",
                    "desc": "简介",
                    "pubdate": 1700000000,
                    "duration": 120,
                    "owner": {"mid": 12345, "name": "测试UP"},
                    "stat": {"view": 500, "like": 42},
                    "tags": [{"tag_name": "科技"}, {"tag_name": "Python"}],
                },
            }
        if params["path"] == "/x/polymer/web-dynamic/v1/feed/space":
            return {
                "code": 0,
                "data": {
                    "items": [
                        {
                            "type": "DYNAMIC_TYPE_AV",
                            "modules": {
                                "module_author": {"mid": "12345", "name": "测试UP", "pub_ts": 200},
                                "module_dynamic": {
                                    "major": {
                                        "type": "MAJOR_TYPE_ARCHIVE",
                                        "archive": {"bvid": BV2, "title": "动态", "desc": "d"},
                                    }
                                },
                            },
                        }
                    ]
                },
            }
        assert params["live"] is True
        return {"code": 0, "data": {"live_status": 1, "title": "开播", "area_name": "数码"}}

    fake_client.node_handler = node_handler
    http_calls: list[httpx.Request] = []
    http = _make_mock_http(_make_bili_http_responses(), http_calls)
    try:
        pub = BiliPublicClient(
            fake_client,
            http,
            clock=lambda: now_box[0],
            sleep=fake_sleep,
            env_defaults={
                "bilibili_enabled": True,
                "bilibili_min_interval_seconds": 3.0,
                "node_bili_delegate_enabled": True,
                "node_bili_fallback_local": True,
            },
        )
        pub.set_node_connected(True)

        v = await pub.video(BV1)
        assert v == {
            "bvid": BV1,
            "mid": "12345",
            "title": "测试视频",
            "description": "简介",
            "author": "测试UP",
            "published_at": 1700000000,
            "duration": 120,
            "views": 500,
            "likes": 42,
            "tags": ["科技", "Python"],
        }
        # Cache hit within TTL causes no extra node_call or sleep.
        v_cached = await pub.video(BV1)
        assert v_cached == v and len(fake_client.node_calls) == 1

        # Pacing enforced on next distinct endpoint.
        ups = await pub.updates("12345")
        assert len(ups) == 1 and ups[0]["bvid"] == BV2
        assert sleeps == [3.0]

        live = await pub.live_info("778899")
        assert live["live_status"] == 1
        assert await pub.live_status("778899") == 1  # cached

        assert await pub.resolve_short("https://b23.tv/AbC123") == BV1
        assert not http_calls

        # bili2233.cn is validated strictly and fetched locally without delegating.
        assert await pub.resolve_short("https://bili2233.cn/XyZ99") == BV1
        assert len(http_calls) == 1 and http_calls[0].url.host == "bili2233.cn"

        for bad_short in (
            "http://b23.tv/abc",
            "https://evil.invalid/abc",
            "https://b23.tv/abc?x=1",
            "https://user@b23.tv/abc",
        ):
            with pytest.raises(BiliClientError, match="invalid_bilibili_short_link"):
                await pub.resolve_short(bad_short)
    finally:
        await http.aclose()


async def test_public_client_local_fallback_and_412_circuit_breaker() -> None:
    now_box = [500.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()
    http = _make_mock_http(responses, http_calls)
    fake_client = _FakePluginClient()

    def failing_transport(_action: str, _params: dict[str, Any]) -> dict[str, Any]:
        raise PluginRpcError("node_rpc_timeout")

    fake_client.node_handler = failing_transport
    try:
        pub = BiliPublicClient(
            fake_client,
            http,
            clock=lambda: now_box[0],
            sleep=lambda _: asyncio.sleep(0),
            env_defaults={"bilibili_min_interval_seconds": 0.0},
        )
        pub.set_node_connected(True)

        # Transport error falls back locally without tripping circuit breaker.
        v = await pub.video(BV1)
        assert v["bvid"] == BV1
        assert pub.blocked_until == 0.0
        assert len(http_calls) == 1

        # With fallback disabled, transport error opens circuit breaker.
        pub.set_fallback_local(False)
        pub.cache.clear()
        with pytest.raises(BiliClientError, match="bilibili_unavailable"):
            await pub.video(BV1)
        assert pub.blocked_until > now_box[0]

        with pytest.raises(BiliClientError, match="bilibili_circuit_open"):
            await pub.video(BV1)

        # Code -352 / -412 from node opens 900s circuit breaker without local fallback.
        pub.blocked_until = 0.0
        pub.set_fallback_local(True)
        http_calls.clear()
        fake_client.node_handler = lambda *_: {"code": -352, "data": None}
        with pytest.raises(BiliClientError, match="bilibili_unavailable"):
            await pub.live_info("778899")
        assert pub.blocked_until == now_box[0] + 900.0
        assert not http_calls

        # Forbidden redirect target raises without opening circuit breaker.
        pub.blocked_until = 0.0
        fake_client.node_handler = lambda *_: {"location": f"https://evil.invalid/video/{BV1}"}
        with pytest.raises(BiliClientError, match="bilibili_short_link_target_forbidden"):
            await pub.resolve_short("https://b23.tv/abc123")
        assert pub.blocked_until == 0.0
    finally:
        await http.aclose()


# ---------------------------------------------------------------------------
# 3. BiliPatrolEngine: seed migration, policy/creds KV, buvid4, run_once, risk
# ---------------------------------------------------------------------------


async def test_patrol_engine_legacy_seed_migration_and_stuck_recovery() -> None:
    now_box = [1_700_000_000.0]
    seed = {
        "watched": [
            {
                "bvid": BV1,
                "run_id": "seed-run-1",
                "status": "done",
                "created": 1_699_990_000.0,
                "summary": "公开文本学习 · " + BV1 + "\n旧巡逻学习笔记",
                "actions": ["like"],
            },
            {
                "bvid": BV2,
                "run_id": "stuck-run",
                "status": "reading",
                "attempts": 1,
                "created": 1_699_995_000.0,
                "summary": "",
            },
            {
                "bvid": BV3,
                "run_id": "stuck-run",
                "status": "analyzing",
                "attempts": 1,
                "created": 1_699_996_000.0,
                "summary": "",
            },
        ],
        "actions": [
            {
                "day": "2023-11-14",
                "bvid": BV1,
                "kind": "like",
                "status": "done",
                "created": 1_699_990_100.0,
            },
            {
                "day": "2023-11-14",
                "bvid": BV2,
                "kind": "favorite",
                "status": "sending",
                "created": 1_699_995_100.0,
            },
        ],
        "runs": [
            {
                "id": "stuck-run",
                "day": "2023-11-14",
                "slot": "life-stuck",
                "status": "working",
                "created": 1_699_995_000.0,
                "reason": None,
            }
        ],
    }
    fake_client = _FakePluginClient(
        {
            "env_defaults": {
                "enabled": False,
                "bot_timezone": "Asia/Shanghai",
                "bilibili_enabled": True,
                "bilibili_min_interval_seconds": 0.0,
                "node_bili_delegate_enabled": True,
                "node_bili_fallback_local": True,
            },
            "watched_seed": seed,
        }
    )
    http = _make_mock_http(_make_bili_http_responses(), [])
    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        assert fake_client.kv["watched_index"]["seeded"] is True
        assert fake_client.kv["watched." + BV1]["summary"].endswith("旧巡逻学习笔记")
        assert engine.recent_runs[0]["status"] == "abandoned"
        assert any(a["status"] == "uncertain" for a in engine.actions_log)

        st = await engine.status()
        assert st["watch_history_counts"] == {
            "done": 1,
            "processing": 0,
            "retryable": 1,
            "uncertain": 1,
            "exhausted": 0,
        }
        assert st["uncertain_actions"] == 1
        assert (await engine.lookup_watched(BV1))["actions_taken"] == ["like"]
        assert (await engine.recent_surfed())[0]["bvid"] == BV1

        # Second initialize does NOT re-import watched_seed.
        fake_client.kv["watched_seed"] = {"watched": [], "runs": [], "actions": []}
        await engine.initialize()
        assert (await engine.status())["watched_count"] == 1
    finally:
        await http.aclose()


async def test_patrol_engine_policy_credentials_and_buvid4_autofill() -> None:
    now_box = [1_700_000_000.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()
    http = _make_mock_http(responses, http_calls)
    fake_client = _FakePluginClient(
        {
            "env_defaults": {"enabled": False, "bot_timezone": "Asia/Shanghai"},
            "credentials": {
                "creds": {
                    "sessdata": "s-secret-cookie",
                    "bili_jct": "c" * 32,
                    "buvid3": "b3-val",
                    "buvid4": "",
                },
                "source": "env",
            },
        }
    )
    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        assert engine.patrol_unavailable() == "bili_patrol_disabled"
        st = await engine.set_policy(BiliPolicy(enabled=True, allow_like=True))
        assert st["source"] == "sqlite" and st["config"]["enabled"] is True
        assert engine.patrol_unavailable() is None

        # nav() auto-fills buvid4 via SPI once and persists into KV credentials.
        nav_res = await engine.nav()
        assert nav_res == {"status": "valid", "account_mask": "…3456"}
        assert [r.url.path for r in http_calls] == [
            "/x/frontend/finger/spi",
            "/x/web-interface/nav",
        ]
        assert engine.creds["buvid4"] == "spi-buvid4-value"
        assert fake_client.kv["credentials"]["creds"]["buvid4"] == "spi-buvid4-value"
        assert fake_client.kv["account_state"]["buvid4_device"]["status"] == "ready"
        assert "s-secret-cookie" not in json.dumps(await engine.status())

        # Reset policy deletes KV "policy" and returns source="env".
        reset_st = await engine.set_policy(None)
        assert "policy" not in fake_client.kv
        assert reset_st["source"] == "env" and reset_st["config"]["enabled"] is False

        # Partial credential update preserves sessdata and clears buvid4_device for re-autofill.
        upd_st = await engine.update_credentials(CredentialUpdate(buvid3="new-b3"))
        assert engine.creds["sessdata"] == "s-secret-cookie"
        assert upd_st["buvid4_autofill"]["status"] == "configured"

        # Clear wipes credentials & marks patrol unavailable.
        await engine.set_policy(BiliPolicy(enabled=True))
        await engine.update_credentials(CredentialUpdate(clear=True))
        assert engine.patrol_unavailable() == "bili_credentials_missing"
    finally:
        await http.aclose()


async def test_patrol_engine_full_run_once_two_picks_events_and_interactions() -> None:
    now_box = [1_700_000_000.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()
    view_by_bvid = {
        BV1: {**responses["/x/web-interface/view"], "bvid": BV1, "aid": 100, "cid": 200},
        BV2: {
            **responses["/x/web-interface/view"],
            "bvid": BV2,
            "aid": 101,
            "cid": 201,
            "title": "红石生电",
        },
    }
    http = _make_mock_http(responses, http_calls, view_by_bvid=view_by_bvid)
    fake_client = _FakePluginClient(
        {
            "env_defaults": {"enabled": False, "bot_timezone": "Asia/Shanghai"},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie-value-123",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3-value",
                    "buvid4": "b4-value",
                },
                "source": "sqlite",
            },
        }
    )
    fake_client.selection_response = json.dumps(
        {
            "selected": [
                {"bvid": BV1, "like": True},
                {"bvid": BV2, "like": False, "share_owner": True},
            ],
            "mood_longnick": "刷到了超酷的调度器和红石教程",
        }
    )

    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()
        await engine.set_policy(BiliPolicy(enabled=True, allow_like=True, daily_action_limit=1))

        res = await engine.run_once(slot="life-1")
        assert res == {"status": "done", "learned": 2, "reason": None}

        # Verify activity & status lifecycle.
        activities_pushed = [
            p["current_activity"] for p in fake_client.status_pushes if "current_activity" in p
        ]
        assert activities_pushed[0] == "bili_patrol"
        assert activities_pushed[-1] is None
        assert [a["active"] for a in fake_client.activities if a["kind"] == "bili_reading"] == [
            True,
            False,
            True,
            False,
            False,  # finally safeguard
        ]

        # Verify world events (mood_longnick only on final pick).
        assert len(fake_client.world_events) == 2
        ev1, ev2 = fake_client.world_events
        assert ev1["event_type"] == "bili_video_watched"
        assert ev1["business_key"] == BV1
        assert ev1["report_text"].startswith(f"公开字幕学习 · {BV1}")
        assert ev1["mood_longnick"] is None

        assert ev2["business_key"] == BV2
        assert ev2["importance"] == "high"
        assert ev2["mood_longnick"] == "刷到了超酷的调度器和红石教程"

        # Verify watched KV, lookup_watched, recent_surfed, and CSRF write.
        assert (await engine.lookup_watched(BV1))["actions_taken"] == ["like"]
        surfed = await engine.recent_surfed()
        assert {item["bvid"] for item in surfed} == {BV1, BV2}
        writes = [r for r in http_calls if r.method == "POST"]
        assert len(writes) == 1 and writes[0].url.path == "/x/web-interface/archive/like"
        assert b"csrf=" + b"j" * 32 in writes[0].content

        # Duplicate slot and exhausted candidates check.
        assert (await engine.run_once(slot="life-1")) == {"status": "already_attempted"}
        assert (await engine.run_once(slot="life-2")) == {
            "status": "empty",
            "learned": 0,
            "reason": None,
        }
    finally:
        await http.aclose()


async def test_patrol_engine_risk_cooldown_notify_and_public_material() -> None:
    now_box = [1_700_000_000.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()

    def risk_handler(req: httpx.Request) -> httpx.Response:
        http_calls.append(req)
        if req.url.path == "/x/web-interface/nav":
            return httpx.Response(200, json={"code": -412, "data": None})
        return httpx.Response(200, json={"code": 0, "data": responses.get(req.url.path, {})})

    http = httpx.AsyncClient(transport=httpx.MockTransport(risk_handler))
    fake_client = _FakePluginClient(
        {
            "policy": {"enabled": True},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3",
                    "buvid4": "b4",
                },
                "source": "sqlite",
            },
        }
    )
    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        with pytest.raises(BiliClientError, match="bili_account_risk"):
            await engine.nav()

        assert engine.blocked_until == now_box[0] + 43200
        assert engine.patrol_unavailable() == "bili_account_cooling_down"
        assert fake_client.kv["account_state"]["last_error"] == "bili_account_risk"
        assert fake_client.status_pushes[-1]["unavailable_actions"] == {
            "bili.patrol": "bili_account_cooling_down"
        }
        assert len(fake_client.owner_notifications) == 1
        assert fake_client.owner_notifications[0][0] == "bili_account_bili_account_risk"

        with pytest.raises(BiliClientError, match="bili_account_cooling_down"):
            await engine.run_once()
        assert len(http_calls) == 1 and not fake_client.ai_calls

        # public_material works anonymously when cooldown cleared & creds cleared,
        # and degrades cleanly on subtitle host failure.
        engine.blocked_until = 0.0
        await engine.update_credentials(CredentialUpdate(clear=True))
        await engine.set_policy(BiliPolicy(enabled=False))

        good_calls: list[httpx.Request] = []
        good_resp = _make_bili_http_responses()
        good_resp["/x/web-interface/nav"] = {**NAV_DATA, "isLogin": False, "mid": 0}
        good_resp["/x/player/wbi/v2"] = {
            "subtitle": {
                "subtitles": [{"lan": "zh-CN", "subtitle_url": "https://evil.invalid/bfs/x.json"}]
            }
        }
        await http.aclose()
        http = _make_mock_http(good_resp, good_calls)
        engine.http = http

        mat = await engine.public_material(
            {"bvid": BV1, "title": "公开标题", "description": "公开简介"}
        )
        assert mat["bvid"] == BV1
        assert mat["material_scope"] == "metadata_only"
        assert mat["material_error"] == "bili_subtitle_host_forbidden"
        assert all(not r.headers.get("cookie") for r in good_calls)
    finally:
        await http.aclose()


# ---------------------------------------------------------------------------
# 4. plugin_main: hello handshake, sensor, action.run, and all 15 custom RPCs
# ---------------------------------------------------------------------------


async def test_plugin_main_hello_and_all_rpcs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAKIMI_PLUGIN_WS_URL", "ws://127.0.0.1:12141/ws/plugin")
    monkeypatch.setenv("HAKIMI_PLUGIN_BILIBILI_TOKEN", "bili-secret-token-123456")
    monkeypatch.setenv("HAKIMI_PLUGIN_BILIBILI_NODE_REGION", "KR")

    sockets: list[_FakeSocket] = []
    sock_ready = asyncio.Event()

    def connect_factory(_url: str, _headers: dict[str, str]) -> _FakeSocket:
        sock = _FakeSocket()
        sockets.append(sock)
        sock_ready.set()
        return sock

    now_box = [1_700_000_000.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()
    http = _make_mock_http(responses, http_calls)

    plugin = build_bilibili_plugin_from_env(
        http_client=http,
        connect_factory=connect_factory,
        clock=lambda: now_box[0],
        public_clock=lambda: now_box[0],
        sleep=lambda _: asyncio.sleep(0),
        reconnect_min=0.01,
        reconnect_max=0.04,
    )
    assert isinstance(plugin, BilibiliPlugin)
    hello = plugin.client.hello_payload()
    assert hello["admin_panel"] == "bili"
    assert hello["attention_level"] == "MEDIUM"
    assert set(hello["scopes"]) == {
        "actions",
        "sensors",
        "thinking_scenes",
        "node_bridge",
        "owner_notify",
    }
    assert hello["capabilities"] == ["context_data", "patrol_entry"]
    assert [s["key"] for s in hello["thinking_scenes"]] == [
        "bilibili_summary",
        "bili_selection",
        "bili_learning",
    ]
    assert hello["thinking_scenes"][0]["output_cap"] == "summary_output_tokens"
    assert hello["actions"][0]["name"] == "bili.patrol"
    assert hello["actions"][0]["budget_lane"] == "patrol"
    assert hello["actions"][0]["timeout_seconds"] == 1500.0
    assert hello["actions"][0]["aliases"] == [
        ["bilibili_surf", "patrol", False],
        ["bilibili_surf", "run_once", False],
    ]

    kv: dict[str, Any] = {
        "env_defaults": {
            "enabled": False,
            "bot_timezone": "Asia/Shanghai",
            "bilibili_enabled": True,
            "bilibili_min_interval_seconds": 0.0,
            "node_bili_delegate_enabled": True,
            "node_bili_fallback_local": True,
        },
        "credentials": {
            "creds": {
                "sessdata": "s-rpc-cookie",
                "bili_jct": "c" * 32,
                "buvid3": "b3-rpc",
                "buvid4": "",
            },
            "source": "sqlite",
        },
    }

    async def handle_kernel_side(sock: _FakeSocket) -> None:
        seen_ids: set[str] = set()
        while True:
            await asyncio.sleep(0.005)
            for frame in list(sock.sent):
                fid = frame.get("id")
                ftype = frame.get("type")
                if not fid or fid in seen_ids or ftype in {"rpc.result", "plugin.hello"}:
                    continue
                seen_ids.add(fid)
                if ftype == "state.get":
                    key = frame["key"]
                    if key in kv:
                        await sock.push(
                            {
                                "type": "kernel.result",
                                "id": fid,
                                "ok": True,
                                "result": {"found": True, "value": kv[key]},
                            }
                        )
                    else:
                        await sock.push(
                            {
                                "type": "kernel.result",
                                "id": fid,
                                "ok": True,
                                "result": {"found": False, "value": None},
                            }
                        )
                elif ftype == "state.set":
                    kv[frame["key"]] = frame["value"]
                    await sock.push(
                        {"type": "kernel.result", "id": fid, "ok": True, "result": {"saved": True}}
                    )
                elif ftype == "state.delete":
                    existed = frame["key"] in kv
                    kv.pop(frame["key"], None)
                    await sock.push(
                        {
                            "type": "kernel.result",
                            "id": fid,
                            "ok": True,
                            "result": {"deleted": existed},
                        }
                    )
                elif ftype == "ai.budget_check":
                    await sock.push(
                        {
                            "type": "kernel.result",
                            "id": fid,
                            "ok": True,
                            "result": {"available": True, "hard_stop": False},
                        }
                    )
                elif ftype == "ai.screen":
                    await sock.push(
                        {"type": "kernel.result", "id": fid, "ok": True, "result": {"passed": True}}
                    )
                elif ftype == "ai.generate":
                    await sock.push(
                        {
                            "type": "ai.result",
                            "id": fid,
                            "ok": True,
                            "text": json.dumps({"selected": [{"bvid": BV1, "like": True}]}),
                        }
                    )
                elif ftype == "ai.video_analyze":
                    await sock.push(
                        {
                            "type": "kernel.result",
                            "id": fid,
                            "ok": True,
                            "result": {
                                "text": "RPC学习总结",
                                "mode": "text",
                                "material_scope": "partial_subtitle",
                                "reason": None,
                            },
                        }
                    )
                elif ftype == "bili.memory_preferences":
                    await sock.push(
                        {
                            "type": "kernel.result",
                            "id": fid,
                            "ok": True,
                            "result": {"preferences": {}, "private_topic": None},
                        }
                    )
                elif ftype == "bili.private_topic_commit":
                    await sock.push(
                        {
                            "type": "kernel.result",
                            "id": fid,
                            "ok": True,
                            "result": {
                                "committed": True,
                                "owner_private": False,
                                "audience": "a" * 64,
                                "session_scope": "s" * 64,
                            },
                        }
                    )
                elif ftype == "node.call":
                    if frame.get("action") == "bili.cover_thumb":
                        await sock.push(
                            {
                                "type": "kernel.result",
                                "id": fid,
                                "ok": True,
                                "result": {
                                    "mime_type": "image/jpeg",
                                    "bytes": 4,
                                    "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
                                },
                            }
                        )
                    else:
                        await sock.push(
                            {
                                "type": "kernel.result",
                                "id": fid,
                                "ok": True,
                                "result": {"location": f"https://www.bilibili.com/video/{BV1}"},
                            }
                        )

    async def rpc(
        sock: _FakeSocket, req_id: str, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        await sock.push(
            {"type": "rpc.request", "id": req_id, "method": method, "params": params or {}}
        )
        return await sock.wait_sent(
            lambda f: f.get("type") == "rpc.result" and f.get("id") == req_id
        )

    await plugin.client.start()
    try:
        await asyncio.wait_for(sock_ready.wait(), timeout=2.0)
        sock = sockets[0]
        await sock.wait_sent(lambda f: f.get("type") == "plugin.hello")
        responder = asyncio.create_task(handle_kernel_side(sock))
        try:
            await sock.push(
                {
                    "type": "plugin.welcome",
                    "plugin_id": "bilibili_surf",
                    "scopes": list(hello["scopes"]),
                    "enabled": True,
                }
            )
            assert await plugin.client.wait_connected(timeout=2.0)
            await sock.wait_sent(lambda f: f.get("type") == "plugin.status")

            # Sensor & node.state toggling
            s_off = await rpc(sock, "r-s1", "sensor.read")
            assert s_off["ok"] and s_off["result"]["summary"].startswith("B站冲浪:")

            await sock.push({"type": "node.state", "connected": True})
            await asyncio.sleep(0.02)
            s_on = await rpc(sock, "r-s2", "sensor.read")
            assert s_on["result"]["summary"].startswith("B站冲浪(CN专线):")

            # Public RPCs
            r_short = await rpc(sock, "r-1", "bili.resolve_short", {"url": "https://b23.tv/abc123"})
            assert r_short["ok"] and r_short["result"] == {"bvid": BV1}
            r_cov = await rpc(
                sock,
                "r-cov",
                "bili.cover_thumb",
                {"url": "https://i0.hdslb.com/bfs/archive/cover.jpg"},
            )
            assert r_cov["ok"] and r_cov["result"]["found"] is True
            assert r_cov["result"]["thumbnail"]["bytes"] == 4
            r_cov_bad = await rpc(
                sock,
                "r-cov-bad",
                "bili.cover_thumb",
                {"url": "http://i0.hdslb.com/bfs/archive/cover.jpg"},
            )
            assert r_cov_bad["ok"] and r_cov_bad["result"] == {"found": False, "thumbnail": None}
            await sock.push({"type": "node.state", "connected": False})
            await asyncio.sleep(0.02)

            r_vid = await rpc(sock, "r-2", "bili.video", {"bvid": BV1})
            assert r_vid["ok"] and r_vid["result"]["bvid"] == BV1

            responses["/x/polymer/web-dynamic/v1/feed/space"] = {"items": []}
            r_up = await rpc(sock, "r-3", "bili.updates", {"mid": "12345"})
            assert r_up["ok"] and r_up["result"] == {"videos": []}

            r_live = await rpc(sock, "r-4", "bili.live_info", {"room_id": "778899"})
            assert r_live["ok"] and r_live["result"]["live_status"] == 1

            r_pm = await rpc(
                sock,
                "r-5",
                "bili.public_material",
                {"video": {"bvid": BV1, "title": "T", "description": "D"}},
            )
            assert r_pm["ok"] and r_pm["result"]["bvid"] == BV1

            # Account & policy RPCs
            r_st = await rpc(sock, "r-6", "bili.account.status")
            assert r_st["ok"] and r_st["result"]["source"] == "env"

            r_bad = await rpc(
                sock, "r-bad", "bili.account.set_policy", {"enabled": True, "allow_favorite": True}
            )
            assert not r_bad["ok"] and r_bad["error"] == "invalid_admin_input"

            r_set = await rpc(
                sock, "r-7", "bili.account.set_policy", {"enabled": True, "allow_like": True}
            )
            assert r_set["ok"] and r_set["result"]["config"]["enabled"] is True

            r_creds = await rpc(
                sock, "r-8", "bili.account.update_credentials", {"buvid3": "b3-updated"}
            )
            assert r_creds["ok"] and r_creds["result"]["credentials"]["buvid3"] is True

            r_buvid4 = await rpc(sock, "r-9", "bili.ensure_buvid4")
            assert r_buvid4["ok"] and r_buvid4["result"]["buvid4_configured"] is True
            assert kv["credentials"]["creds"]["buvid4"] == "spi-buvid4-value"
            assert "spi-buvid4-value" not in json.dumps(r_buvid4)

            r_nav = await rpc(sock, "r-10", "bili.account.nav")
            assert r_nav["ok"] and r_nav["result"]["account_mask"] == "…3456"

            # action.run & account.run_once (keep only BV1 in feed so 2nd run sees empty)
            responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"] = responses[
                "/x/web-interface/wbi/index/top/feed/rcmd"
            ]["item"][:1]
            r_act = await rpc(
                sock,
                "r-11",
                "action.run",
                {"action": "bili.patrol", "arguments": {"slot": "life-rpc-1"}, "origin": "planner"},
            )
            assert r_act["ok"] and r_act["result"]["status"] == "done"

            r_run = await rpc(sock, "r-12", "bili.account.run_once", {"slot": "manual-rpc"})
            assert r_run["ok"] and r_run["result"]["status"] == "empty"

            r_lk = await rpc(sock, "r-13", "bili.lookup_watched", {"bvid": BV1})
            assert r_lk["ok"] and r_lk["result"]["found"] is True
            assert "RPC学习总结" in r_lk["result"]["entry"]["prior_patrol_notes"]

            r_rs = await rpc(sock, "r-14", "bili.recent_surfed", {"limit": 4})
            assert r_rs["ok"] and r_rs["result"]["items"][0]["bvid"] == BV1

            r_ctx = await rpc(sock, "r-ctx", "context.read", {"limit": 4})
            assert r_ctx["ok"] and r_ctx["result"]["recent_bili_surfed"][0]["bvid"] == BV1

            r_pe = await rpc(sock, "r-pe", "patrol.entry", {"bvid": BV1})
            assert r_pe["ok"] and r_pe["result"]["business_key"] == f"bili-account:{BV1}"

            r_reset = await rpc(sock, "r-15", "bili.account.reset_policy")
            assert r_reset["ok"] and r_reset["result"]["source"] == "env"

            r_risk = await rpc(sock, "r-16", "bili.account.risk", {"reason": "bili_account_risk"})
            assert r_risk["ok"] and r_risk["result"]["last_error"] == "bili_account_risk"

            # module.switch off -> action returns gated_skip
            await sock.push({"type": "module.switch", "enabled": False})
            await asyncio.sleep(0.02)
            r_off = await rpc(
                sock, "r-off", "action.run", {"action": "bili.patrol", "arguments": {}}
            )
            assert r_off["ok"] and r_off["result"]["status"] == "gated_skip"
            assert r_off["result"]["reason"] == "module_disabled"
        finally:
            responder.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await responder
    finally:
        await plugin.close()
        await http.aclose()


def test_build_bilibili_plugin_explicit_config_factory() -> None:
    cfg = PluginRuntimeConfig(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        plugin_id="bilibili_surf",
        token="explicit-test-token-1234",
        display_name="B 站巡逻",
        scopes=("actions", "sensors", "thinking_scenes", "node_bridge", "owner_notify"),
        admin_panel="bili",
        attention_level="MEDIUM",
        capabilities=("context_data", "patrol_entry"),
    )
    plugin = build_bilibili_plugin(cfg)
    assert isinstance(plugin, BilibiliPlugin)
    assert plugin.client.config.plugin_id == "bilibili_surf"
    assert base64.b64encode(b"ok").decode() == "b2s="


async def test_patrol_engine_edge_cases_caps_delegated_risk_and_cover() -> None:
    now_box = [1_700_000_000.0]
    http_calls: list[httpx.Request] = []
    responses = _make_bili_http_responses()
    http = _make_mock_http(responses, http_calls)
    fake_client = _FakePluginClient(
        {
            "policy": {"enabled": True, "allow_favorite": True, "favorite_folder_id": "999"},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3",
                    "buvid4": "b4",
                },
                "source": "sqlite",
            },
        }
    )

    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        # 1) Unavailable priority order
        assert engine.patrol_unavailable() is None
        engine.set_module_enabled(False)
        assert engine.patrol_unavailable() == "module_disabled"
        engine.set_module_enabled(True)

        # 2) Index/runs/actions caps (<= 200 / <= 20 / <= 100)
        for i in range(210):
            bv = f"BV1xx411c{i:03d}"
            # Keep valid base58 chars for test index eviction
            bv = "BV1xx411c7" + chr(65 + (i // 26)) + chr(65 + (i % 26))
            engine.watched_index["items"][bv] = {
                "status": "done",
                "run_id": "r",
                "created": float(i),
                "attempts": 1,
                "retry_after": None,
                "actions": [],
                "has_summary": True,
            }
            fake_client.kv[f"watched.{bv}"] = {"bvid": bv, "created": float(i), "summary": "s"}
        await engine._save_watched_index()
        assert len(fake_client.kv["watched_index"]["items"]) == 200
        assert len(fake_client.kv) < 220

        # 3) Cover thumbnail validation via CN node
        engine.set_node_connected(True)
        fake_client.node_handler = lambda action, _params: (
            {
                "mime_type": "image/jpeg",
                "bytes": 4,
                "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
            }
            if action == "bili.cover_thumb"
            else {"envelope": {"code": 0, "data": NAV_DATA}}
        )
        assert (await engine.fetch_cover_thumbnail("https://i0.hdslb.com/bfs/archive/abc.jpg"))[
            "bytes"
        ] == 4
        assert (
            await engine.fetch_cover_thumbnail("https://evil.invalid/bfs/archive/abc.jpg") is None
        )

        # 4) Delegated bili_http_status 412 triggers 12h risk cooldown without local fallback
        def node_412(action: str, _params: dict[str, Any]) -> dict[str, Any]:
            if action == "bili.request":
                raise PluginRpcError("bili_http_status", "412")
            return {}

        fake_client.node_handler = node_412
        with pytest.raises(BiliClientError, match="bili_account_risk"):
            await engine.nav()
        assert engine.blocked_until == now_box[0] + 43200
        assert not http_calls

        # 5) Reset cooldown and test unowned favorite folder rejection during run_once
        engine.blocked_until = 0.0
        engine.set_node_connected(False)
        fake_client.selection_response = json.dumps({"selected": [{"bvid": BV1, "favorite": True}]})
        res = await engine.run_once(slot="life-fav")
        assert res["status"] == "failed" and res["reason"] == "bili_favorite_folder_not_owned"
        assert not any(r.method == "POST" for r in http_calls)

        # 6) Budget hard stop stops run_once before any HTTP/AI call
        http_calls.clear()
        fake_client.ai_calls.clear()
        fake_client.budget_response = {"available": False, "hard_stop": True}
        with pytest.raises(BiliClientError, match="budget_hard_stop"):
            await engine.run_once(slot="life-budget")
        assert not http_calls and len(fake_client.ai_calls) == 1

        # 7) Disable mid-run during screen stops before generation
        fake_client.budget_response = {"available": True, "hard_stop": False}
        fake_client.on_screen_hook = lambda _p: engine.set_policy(BiliPolicy(enabled=False))
        res_mid = await engine.run_once(slot="life-mid")
        assert res_mid["status"] == "failed" and res_mid["reason"] == "bili_settings_changed"
    finally:
        await http.aclose()


async def test_resolve_short_supports_http_page_and_festival_targets() -> None:
    targets = [
        f"http://www.bilibili.com/video/{BV1}?p=3",
        f"https://www.bilibili.com/festival/spring_2025?bvid={BV2}&p=2",
        f"https://m.bilibili.com/festival?bvid={BV3}",
        f"https://www.bilibili.com/festival/../admin?bvid={BV1}",
        f"http://www.bilibili.com:8080/video/{BV1}",
    ]
    idx = [0]

    def handler(_req: httpx.Request) -> httpx.Response:
        loc = targets[idx[0]]
        idx[0] += 1
        return httpx.Response(302, headers={"Location": loc})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        pub = BiliPublicClient(
            None,
            http,
            clock=lambda: 100.0,
            sleep=lambda _: asyncio.sleep(0),
            env_defaults={"bilibili_min_interval_seconds": 0.0},
        )
        assert await pub.resolve_short("https://b23.tv/p1") == BV1
        assert await pub.resolve_short("https://b23.tv/p2") == BV2
        assert await pub.resolve_short("https://b23.tv/p3") == BV3
        with pytest.raises(BiliClientError, match="bilibili_short_link_target_forbidden"):
            await pub.resolve_short("https://b23.tv/p4")
        with pytest.raises(BiliClientError, match="bilibili_short_link_target_forbidden"):
            await pub.resolve_short("https://b23.tv/p5")
    finally:
        await http.aclose()


async def test_learning_source_label_all_four_sources_and_material_cover_cid() -> None:
    assert learning_source_label("video", "metadata_only") == "公开原片学习"
    assert learning_source_label("video", "full_subtitle") == "公开原片学习"
    assert learning_source_label("cn_storyboard", "metadata_only") == "公开关键帧摘要"
    assert learning_source_label("text", "full_subtitle") == "公开字幕学习"
    assert learning_source_label("text", "partial_subtitle") == "公开字幕学习"
    assert learning_source_label("text", "metadata_only") == "公开简介评论摘要"
    assert learning_source_label("metadata", "metadata_only") == "公开简介评论摘要"

    now_box = [1_700_000_000.0]
    responses = _make_bili_http_responses()
    view_by_bvid = {
        BV1: {
            **responses["/x/web-interface/view"],
            "bvid": BV1,
            "aid": 100,
            "cid": 2001,
            "pic": "http://i0.hdslb.com/bfs/archive/cover1.jpg",
        },
        BV2: {
            **responses["/x/web-interface/view"],
            "bvid": BV2,
            "aid": 101,
            "cid": 2002,
            "pic": "//archive.biliimg.com/bfs/new/cover2.png",
        },
        BV3: {
            **responses["/x/web-interface/view"],
            "bvid": BV3,
            "aid": 102,
            "cid": 2003,
            "pic": "https://evil.invalid/bfs/archive/bad.jpg",
        },
    }
    http_calls: list[httpx.Request] = []
    http = _make_mock_http(responses, http_calls, view_by_bvid=view_by_bvid)
    fake_client = _FakePluginClient(
        {
            "policy": {"enabled": True},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3",
                    "buvid4": "b4",
                },
                "source": "sqlite",
            },
        }
    )
    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()
        await engine.nav()

        _, m1 = await engine.material(BV1)
        assert m1["cid"] == 2001
        assert m1["cover_url"] == "https://i0.hdslb.com/bfs/archive/cover1.jpg"

        _, m2 = await engine.material(BV2)
        assert m2["cid"] == 2002
        assert m2["cover_url"] == "https://archive.biliimg.com/bfs/new/cover2.png"

        _, m3 = await engine.material(BV3)
        assert m3["cid"] == 2003
        assert "cover_url" not in m3

        # fetch_cover_thumbnail strictly rejects http:// even though material() normalizes pic
        engine.set_node_connected(True)
        fake_client.node_handler = lambda *_: {
            "mime_type": "image/jpeg",
            "bytes": 4,
            "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
        }
        assert (
            await engine.fetch_cover_thumbnail("http://i0.hdslb.com/bfs/archive/cover1.jpg") is None
        )
        assert not fake_client.node_calls
        assert (await engine.fetch_cover_thumbnail(m1["cover_url"]))["bytes"] == 4

        # Verify run_once emits exact labels for video, cn_storyboard, full_subtitle, metadata_only
        engine.set_node_connected(False)
        for idx, (bv, va_resp, expected_label) in enumerate(
            [
                (
                    BV1,
                    {"text": "原片内容", "mode": "video", "material_scope": "metadata_only"},
                    "公开原片学习",
                ),
                (
                    BV2,
                    {
                        "text": "关键帧内容",
                        "mode": "cn_storyboard",
                        "material_scope": "metadata_only",
                    },
                    "公开关键帧摘要",
                ),
                (
                    BV3,
                    {"text": "简介摘要", "mode": "text", "material_scope": "metadata_only"},
                    "公开简介评论摘要",
                ),
            ]
        ):
            responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"] = [
                {
                    "goto": "av",
                    "bvid": bv,
                    "title": f"标题{idx}",
                    "owner": {"name": "UP"},
                    "tname": "科技",
                }
            ]
            fake_client.selection_response = json.dumps({"selected": [{"bvid": bv}]})
            fake_client.video_analyze_response = va_resp
            res = await engine.run_once(slot=f"label-{idx}")
            assert res == {"status": "done", "learned": 1, "reason": None}
            assert fake_client.world_events[-1]["report_text"].startswith(
                f"{expected_label} · {bv}"
            )
    finally:
        await http.aclose()


async def test_cdn_errors_decoupled_from_12h_cooldown() -> None:
    now_box = [1_700_000_000.0]
    responses = _make_bili_http_responses()
    subtitle_status_code = [403]
    api_status_code = [200]
    spi_status_code = [200]

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.host == "aisubtitle.hdslb.com":
            return httpx.Response(subtitle_status_code[0], text="cdn error")
        if req.url.path == "/x/frontend/finger/spi" and spi_status_code[0] != 200:
            return httpx.Response(spi_status_code[0], text="edge error")
        if req.url.path == "/x/web-interface/view" and api_status_code[0] != 200:
            return httpx.Response(api_status_code[0], text="api edge error")
        value = responses[req.url.path]
        return httpx.Response(200, json={"code": 0, "data": value})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    fake_client = _FakePluginClient(
        {
            "policy": {"enabled": True},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3",
                    "buvid4": "",
                },
                "source": "sqlite",
            },
        }
    )
    try:
        engine = BiliPatrolEngine(
            fake_client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        # 1) SPI HTTP 403 / 567 never triggers 12h cooldown
        for sc in (403, 567):
            spi_status_code[0] = sc
            engine.buvid4_device = None
            await engine.nav()
            assert engine.blocked_until == 0.0
            assert not fake_client.owner_notifications
        spi_status_code[0] = 200
        engine.buvid4_device = None
        await engine.nav()
        assert engine.creds["buvid4"] == "spi-buvid4-value"

        # 2) Subtitle CDN HTTP 403 / 567 / 429 / 412 / 503 never triggers 12h cooldown
        for sc in (403, 567, 429, 412, 503):
            subtitle_status_code[0] = sc
            with pytest.raises(BiliClientError, match="bili_account_unavailable"):
                await engine._request(
                    "https://aisubtitle.hdslb.com/bfs/subtitle/test.json", subtitle=True
                )
            assert engine.blocked_until == 0.0
            assert not fake_client.owner_notifications

            # And public_material degrades cleanly to metadata_only without cooldown
            mat = await engine.public_material({"bvid": BV1, "title": "T", "description": "D"})
            assert mat["subtitle_status"] == "unavailable"
            assert mat["material_scope"] == "metadata_only"
            assert engine.blocked_until == 0.0

        # 3) Delegated subtitle CDN 403 / 567 / 429 / 412 never triggers 12h cooldown
        engine.set_node_connected(True)
        for code_str in ("403", "567", "429", "412"):
            fake_client.node_handler = lambda _act, _p, c=code_str: (_ for _ in ()).throw(
                PluginRpcError("bili_http_status", c)
            )
            with pytest.raises(BiliClientError, match="bili_account_unavailable"):
                await engine._request(
                    "https://aisubtitle.hdslb.com/bfs/subtitle/test.json", subtitle=True
                )
            assert engine.blocked_until == 0.0

        # 4) API HTTP 403 / 567 (local & delegated) never triggers 12h cooldown
        for code_str in ("403", "567"):
            fake_client.node_handler = lambda _act, _p, c=code_str: (_ for _ in ()).throw(
                PluginRpcError("bili_http_status", c)
            )
            with pytest.raises(BiliClientError, match="bili_account_unavailable"):
                await engine._request("/x/web-interface/view", {"bvid": BV1})
            assert engine.blocked_until == 0.0

        engine.set_node_connected(False)
        for sc in (403, 567):
            api_status_code[0] = sc
            with pytest.raises(BiliClientError, match="bili_account_unavailable"):
                await engine._request("/x/web-interface/view", {"bvid": BV1})
            assert engine.blocked_until == 0.0
            assert not fake_client.owner_notifications

        # 5) API HTTP 412 & 429 DO trigger 12h cooldown
        for sc in (412, 429):
            api_status_code[0] = sc
            engine.blocked_until = 0.0
            with pytest.raises(BiliClientError, match="bili_account_risk"):
                await engine._request("/x/web-interface/view", {"bvid": BV1})
            assert engine.blocked_until == now_box[0] + 43200
    finally:
        await http.aclose()


async def test_patrol_engine_memory_preferences_private_isolation_and_revocation() -> None:
    class _PrefPluginClient(_FakePluginClient):
        def __init__(self, initial_kv: dict[str, Any] | None = None) -> None:
            super().__init__(initial_kv)
            self.pref_handler: Any = None
            self.commit_calls: list[tuple[str, str]] = []
            self.commit_handler: Any = None

        async def bili_memory_preferences(self) -> dict[str, Any]:
            if self.pref_handler is None:
                return {"preferences": {}, "private_topic": None}
            res = self.pref_handler()
            if asyncio.iscoroutine(res):
                return await res
            return res

        async def bili_commit_private_topic(self, handle: str, bvid: str) -> dict[str, Any]:
            self.commit_calls.append((handle, bvid))
            if self.commit_handler is None:
                return {
                    "committed": True,
                    "owner_private": False,
                    "audience": "f" * 64,
                    "session_scope": "s" * 64,
                }
            res = self.commit_handler(handle, bvid)
            if asyncio.iscoroutine(res):
                return await res
            return res

    now_box = [1_700_000_000.0]
    responses = _make_bili_http_responses()
    view_by_bvid = {
        BV1: {**responses["/x/web-interface/view"], "bvid": BV1, "aid": 100, "cid": 200},
        BV2: {
            **responses["/x/web-interface/view"],
            "bvid": BV2,
            "aid": 101,
            "cid": 201,
            "title": "极光延时摄影",
        },
        BV3: {
            **responses["/x/web-interface/view"],
            "bvid": BV3,
            "aid": 102,
            "cid": 202,
            "title": "天文望远镜入门",
        },
    }
    http_calls: list[httpx.Request] = []
    http = _make_mock_http(responses, http_calls, view_by_bvid=view_by_bvid)
    client = _PrefPluginClient(
        {
            "policy": {"enabled": True, "allow_like": True},
            "credentials": {
                "creds": {
                    "sessdata": "s-cookie",
                    "bili_jct": "j" * 32,
                    "buvid3": "b3",
                    "buvid4": "b4",
                },
                "source": "sqlite",
            },
        }
    )

    try:
        engine = BiliPatrolEngine(
            client, http, clock=lambda: now_box[0], sleep=lambda _: asyncio.sleep(0)
        )
        await engine.initialize()

        # 1) plugin_unknown_method degrades gracefully + forged private_topic_ref blocked
        client.pref_handler = lambda: (_ for _ in ()).throw(PluginRpcError("plugin_unknown_method"))
        client.selection_response = json.dumps(
            {
                "selected": [
                    {
                        "bvid": BV1,
                        "share_owner": True,
                        "private_topic_ref": "owner:20000",
                    }
                ]
            }
        )
        res1 = await engine.run_once(slot="priv-1")
        assert res1 == {"status": "done", "learned": 1, "reason": None}
        assert client.world_events[-1]["importance"] == "normal"
        assert "title" in client.world_events[-1]["payload"]

        # 2) Non-owner private topic: strips title/summary, no report/mood/POST, 24h scope filter
        friend_scope = "1" * 64
        other_scope = "2" * 64
        friend_audience = "a" * 64
        client.pref_handler = lambda: {
            "preferences": {
                "recent_chat_topics": ["帮我找个极光视频"],
                "private_topic_refs": ["recent_chat_topics:0"],
            },
            "private_topic": {
                "handle": "hdl-friend",
                "owner_private": False,
                "audience": friend_audience,
                "session_scope": friend_scope,
            },
        }
        client.commit_handler = lambda _h, _bv: {
            "committed": True,
            "owner_private": False,
            "audience": friend_audience,
            "session_scope": friend_scope,
        }
        client.selection_response = json.dumps(
            {
                "selected": [{"bvid": BV2, "like": True, "share_owner": True}],
                "mood_longnick": "不应公开的私聊心情",
            }
        )
        http_calls.clear()
        res2 = await engine.run_once(slot="priv-2")
        assert res2 == {"status": "done", "learned": 1, "reason": None}
        assert client.commit_calls == [("hdl-friend", BV2)]
        ev_priv = client.world_events[-1]
        assert set(ev_priv["payload"]) == {
            "bvid",
            "learning_seconds",
            "activity_kind",
            "private_first",
            "owner_private",
            "audience",
        }
        assert ev_priv["payload"]["private_first"] is True
        assert ev_priv["payload"]["owner_private"] is False
        assert ev_priv["payload"]["audience"] == friend_audience
        assert "title" not in ev_priv["payload"] and "summary" not in ev_priv["payload"]
        assert ev_priv["importance"] == "normal"
        assert ev_priv["report_text"] is None
        assert ev_priv["mood_longnick"] is None
        assert not any(r.method == "POST" for r in http_calls)

        # 24h visibility isolation & priority sorting
        assert await engine.lookup_watched(BV2, session_scope=friend_scope) is not None
        assert await engine.lookup_watched(BV2, session_scope=other_scope) is None
        assert await engine.lookup_watched(BV2, session_scope="") is None
        assert [i["bvid"] for i in await engine.recent_surfed(session_scope="")] == [BV1]
        assert [i["bvid"] for i in await engine.recent_surfed(session_scope=other_scope)] == [BV1]
        # Matching private scope sees BV2 prioritized first ahead of BV1
        assert [i["bvid"] for i in await engine.recent_surfed(session_scope=friend_scope)] == [
            BV2,
            BV1,
        ]
        # After 24h expiry, visible publicly
        now_box[0] += 24 * 3600 + 1
        assert await engine.lookup_watched(BV2, session_scope="") is not None

        # 3) Commit revoked -> fails without writing watched.<bvid> or marking done
        responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"] = [
            {
                "goto": "av",
                "bvid": BV3,
                "title": "天文望远镜入门",
                "owner": {"name": "UP3"},
                "tname": "科技",
            }
        ]
        client.pref_handler = lambda: {
            "preferences": {
                "recent_chat_topics": ["想看天文视频"],
                "private_topic_refs": ["recent_chat_topics:0"],
            },
            "private_topic": {
                "handle": "hdl-revoked",
                "owner_private": True,
                "audience": "b" * 64,
                "session_scope": "3" * 64,
            },
        }
        client.commit_handler = lambda _h, _bv: (_ for _ in ()).throw(
            PluginRpcError("bili_private_request_revoked")
        )
        client.selection_response = json.dumps({"selected": [{"bvid": BV3}]})
        res3 = await engine.run_once(slot="priv-3")
        assert res3["status"] == "failed"
        assert res3["reason"] == "bili_private_request_revoked"
        assert res3["learned"] == 0
        assert res3.get("requires_review") is True
        assert f"watched.{BV3}" not in client.kv
        assert await engine.lookup_watched(BV3, session_scope="3" * 64) is None

        # 4) _bounded() dropping memory_preferences clears private_topic
        engine.watched_index["items"].pop(BV3, None)
        engine.max_context_bytes = 600
        client.pref_handler = lambda: {
            "preferences": {
                "recent_chat_topics": ["超长私聊主题" * 80],
                "private_topic_refs": ["recent_chat_topics:0"],
            },
            "private_topic": {
                "handle": "hdl-dropped",
                "owner_private": True,
                "audience": "b" * 64,
                "session_scope": "3" * 64,
            },
        }
        client.commit_calls.clear()
        res4 = await engine.run_once(slot="priv-4")
        assert res4 == {"status": "done", "learned": 1, "reason": None}
        assert not client.commit_calls
        assert "private_first" not in client.world_events[-1]["payload"]

        # 5) Owner private topic: high importance, keeps report_text, no title/summary/mood/POST
        engine.watched_index["items"].pop(BV3, None)
        engine.max_context_bytes = 48_000
        owner_scope = "9" * 64
        owner_aud = "e" * 64
        client.pref_handler = lambda: {
            "preferences": {
                "recent_chat_topics": ["主人想看天文视频"],
                "private_topic_refs": ["recent_chat_topics:0"],
            },
            "private_topic": {
                "handle": "hdl-owner",
                "owner_private": True,
                "audience": owner_aud,
                "session_scope": owner_scope,
            },
        }
        client.commit_handler = lambda _h, _bv: {
            "committed": True,
            "owner_private": True,
            "audience": owner_aud,
            "session_scope": owner_scope,
        }
        client.selection_response = json.dumps(
            {"selected": [{"bvid": BV3, "like": True}], "mood_longnick": "不该发的签名"}
        )
        http_calls.clear()
        res5 = await engine.run_once(slot="priv-5")
        assert res5 == {"status": "done", "learned": 1, "reason": None}
        ev_owner = client.world_events[-1]
        assert ev_owner["importance"] == "high"
        assert ev_owner["payload"]["private_first"] is True
        assert ev_owner["payload"]["owner_private"] is True
        assert ev_owner["payload"]["audience"] == owner_aud
        assert "title" not in ev_owner["payload"] and "summary" not in ev_owner["payload"]
        assert ev_owner["report_text"] is not None and BV3 in ev_owner["report_text"]
        assert ev_owner["mood_longnick"] is None
        assert not any(r.method == "POST" for r in http_calls)
    finally:
        await http.aclose()


def test_bili_plugin_url_validators_reject_invalid_ports_without_crashing() -> None:
    from plugins.bilibili.client import (
        BiliClientError,
        _is_valid_short_link,
        extract_video_ref,
    )
    from plugins.bilibili.patrol import _validate_subtitle_url

    assert _is_valid_short_link("https://b23.tv:99999/AbCd12") is None
    assert _is_valid_short_link("https://b23.tv:abc/AbCd12") is None
    assert _is_valid_short_link("https://b23.tv:443/AbCd12") == "b23.tv"
    assert extract_video_ref(f"http://www.bilibili.com:99999/video/{BV1}?p=2") is None
    assert extract_video_ref(f"https://www.bilibili.com:abc/festival/2026?bvid={BV1}&p=3") is None
    with pytest.raises(BiliClientError) as exc_info:
        _validate_subtitle_url("https://aisubtitle.hdslb.com:99999/bfs/ai_subtitle/demo.json")
    assert exc_info.value.code == "bili_subtitle_host_forbidden"
