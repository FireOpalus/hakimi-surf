"""Stage A tests for Phase 3 CN NodeTransport Bilibili delegation and storyboard fallback."""

import asyncio
import base64
import time
from types import SimpleNamespace

import httpx
import pytest

from hakimi.bili_watcher import SPI_PATH, BiliPolicy, BiliWatcher, CredentialUpdate
from hakimi.bilibili import Bilibili
from hakimi.generation_policy import Scenario
from hakimi.media import ModelImage
from hakimi.modules.bilibili_surf import BilibiliSurfModule
from hakimi.security import Rejected
from hakimi.video_policy import VideoWatchConfig
from hakimi.video_watch import VideoWatch
from tests.fakes import FakeGateway
from tests.test_bili_watcher import BV, NAV
from tests.test_video_watch import Downloader, material

SAMPLE_VIDEO_DATA = {
    "bvid": BV,
    "aid": 100,
    "cid": 200,
    "title": "CN 节点分流测试视频",
    "desc": "公开简介",
    "pubdate": 1700000000,
    "owner": {"mid": "12345", "name": "测试UP"},
}


class _SettingsProxy:
    """Forwards Settings attributes while overriding Phase 3 flags in Stage A."""

    def __init__(self, base, **overrides):
        self._base = base
        for k, v in overrides.items():
            setattr(self, k, v)

    def __getattr__(self, name):
        return getattr(self._base, name)


class FakeNodeTransport:
    def __init__(self, *, is_connected: bool = True, handler=None):
        self._connected = is_connected
        self.handler = handler
        self.calls: list[tuple[str, dict, float | None]] = []

    def connected(self) -> bool:
        return self._connected

    async def call(self, action: str, params: dict, **kwargs) -> dict:
        timeout = kwargs.get("timeout")
        self.calls.append((action, params, timeout))
        if self.handler is None:
            raise Rejected("node_disconnected")
        res = self.handler(action, params, timeout)
        if asyncio.iscoroutine(res):
            return await res
        return res


class _FakeWatcherGateway(FakeGateway):
    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        if kwargs.get("scenario") == Scenario.BILI_SELECTION:
            return f'{{"selected":[{{"bvid":"{BV}","like":true}}]}}'
        return "CN 专线巡逻摘要。"


# ---------------------------------------------------------------------------
# 1. Bilibili public metadata & short link delegation
# ---------------------------------------------------------------------------


async def test_bilibili_delegates_video_updates_live_and_short_link(settings):
    settings.bilibili_enabled = True
    settings.bilibili_min_interval_seconds = 0.0
    http_calls = []

    def http_handler(req):
        http_calls.append(req)
        return httpx.Response(500)

    def node_handler(action, params, _timeout):
        if action == "bili.resolve_short":
            return {"location": f"https://www.bilibili.com/video/{BV}?from=cn"}
        assert action == "bili.public_get"
        if params["path"] == "/x/web-interface/view":
            return {"code": 0, "data": SAMPLE_VIDEO_DATA}
        if params["path"] == "/x/polymer/web-dynamic/v1/feed/space":
            return {
                "code": 0,
                "data": {
                    "items": [
                        {
                            "type": "DYNAMIC_TYPE_AV",
                            "modules": {
                                "module_author": {"mid": "12345", "name": "UP", "pub_ts": 100},
                                "module_dynamic": {
                                    "major": {
                                        "type": "MAJOR_TYPE_ARCHIVE",
                                        "archive": {
                                            "bvid": BV,
                                            "title": "动态标题",
                                            "desc": "动态简介",
                                        },
                                    }
                                },
                            },
                        }
                    ]
                },
            }
        assert params["live"] is True
        return {
            "code": 0,
            "data": {"live_status": 1, "title": "开播啦", "area_name": "科技"},
        }

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
        bili = Bilibili(settings, client, node_transport=nt)
        v = await bili.video(BV)
        ups = await bili.updates("12345")
        live = await bili.live_info("778899")
        resolved = await bili.resolve_short("https://b23.tv/abc123")

    assert v.bvid == BV and len(ups) == 1 and live["live_status"] == 1 and resolved == BV
    assert not http_calls
    assert [c[0] for c in nt.calls] == [
        "bili.public_get",
        "bili.public_get",
        "bili.public_get",
        "bili.resolve_short",
    ]


@pytest.mark.parametrize(
    "err_code",
    ["node_disconnected", "node_not_dispatched", "node_rpc_timeout", "node_unavailable"],
)
async def test_bilibili_transport_errors_fallback_locally_without_circuit_break(settings, err_code):
    settings.bilibili_enabled = True
    settings.bilibili_min_interval_seconds = 0.0
    http_calls = []

    def http_handler(req):
        http_calls.append(req.url.path)
        if req.url.host == "b23.tv":
            return httpx.Response(302, headers={"Location": f"https://www.bilibili.com/video/{BV}"})
        return httpx.Response(200, json={"code": 0, "data": SAMPLE_VIDEO_DATA})

    def node_handler(_action, _params, _timeout):
        raise Rejected(err_code)

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
        bili = Bilibili(settings, client, node_transport=nt)
        v = await bili.video(BV)
        resolved = await bili.resolve_short("https://b23.tv/abc123")

    assert v.bvid == BV and resolved == BV
    assert bili.blocked_until == 0.0
    assert http_calls == ["/x/web-interface/view", "/abc123"]


async def test_bilibili_fallback_disabled_and_delegate_disabled_flags(settings):
    settings.bilibili_enabled = True
    settings.bilibili_min_interval_seconds = 0.0
    http_calls = []

    def http_handler(req):
        http_calls.append(req.url.path)
        return httpx.Response(200, json={"code": 0, "data": SAMPLE_VIDEO_DATA})

    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(Rejected("node_disconnected")),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as client:
        no_fb = Bilibili(
            _SettingsProxy(settings, node_bili_fallback_local=False), client, node_transport=nt
        )
        with pytest.raises(Rejected, match="bilibili_unavailable"):
            await no_fb.video(BV)
        assert no_fb.blocked_until > 0 and not http_calls

        no_del = Bilibili(
            _SettingsProxy(settings, node_bili_delegate_enabled=False), client, node_transport=nt
        )
        nt.calls.clear()
        v = await no_del.video(BV)
        assert v.bvid == BV and not nt.calls and http_calls == ["/x/web-interface/view"]


@pytest.mark.parametrize(
    "case,expected_risk",
    [
        ("http_412", True),
        ("http_403", False),
        ("code_412", True),
        ("code_352", True),
    ],
)
async def test_bilibili_upstream_412_and_risk_codes_open_circuit(settings, case, expected_risk):
    settings.bilibili_enabled = True
    settings.bilibili_min_interval_seconds = 0.0
    runtime_calls = []
    http_calls = []

    class _Runtime:
        async def provider_result(self, name, is_412):
            runtime_calls.append((name, is_412))

    def node_handler(_action, _params, _timeout):
        if case == "http_412":
            raise Rejected("bili_http_status", detail="412")
        if case == "http_403":
            raise Rejected("bili_http_status", detail="403")
        return {"code": -412 if case == "code_412" else -352, "data": None}

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: http_calls.append(req) or httpx.Response(200))
    ) as client:
        bili = Bilibili(settings, client, runtime=_Runtime(), node_transport=nt)
        with pytest.raises(Rejected, match="bilibili_unavailable"):
            await bili.live_info("778899")
        with pytest.raises(Rejected, match="bilibili_circuit_open"):
            await bili.live_info("778899")

    assert not http_calls
    assert runtime_calls == [("bilibili_412", expected_risk)]


async def test_bilibili_resolve_short_forbidden_location_never_falls_back(settings):
    settings.bilibili_enabled = True
    settings.bilibili_min_interval_seconds = 0.0
    http_calls = []
    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: {"location": f"https://evil.invalid/video/{BV}"},
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: http_calls.append(req) or httpx.Response(200))
    ) as client:
        bili = Bilibili(settings, client, node_transport=nt)
        with pytest.raises(Rejected, match="bilibili_short_link_target_forbidden"):
            await bili.resolve_short("https://b23.tv/abc123")
    assert bili.blocked_until == 0.0 and not http_calls


# ---------------------------------------------------------------------------
# 2. BiliWatcher delegation, POST idempotency, risk & cover thumbnails
# ---------------------------------------------------------------------------


def _make_watcher_service(settings, store, message, node_transport=None):
    alerts = []

    async def alert(*args):
        alerts.append(args)

    svc = SimpleNamespace(
        s=settings,
        store=store,
        secrets=[],
        ai=_FakeWatcherGateway(),
        diary=None,
        proactive=SimpleNamespace(alert=alert, target=lambda: message),
        node_transport=node_transport,
    )
    return svc, alerts


async def test_bili_watcher_delegates_buvid4_and_material_with_cookie_isolation(
    settings, store, message
):
    settings.owner_qq_id = "20000"
    rpc_log = []
    http_calls = []

    def node_handler(action, params, _timeout):
        assert action == "bili.request"
        rpc_log.append(params)
        path = params["path"]
        if path == SPI_PATH:
            return {"envelope": {"code": 0, "data": {"b_4": "cn-buvid4-val"}}}
        if path == "/x/web-interface/nav":
            return {"envelope": {"code": 0, "data": NAV}}
        if path == "/x/web-interface/view":
            return {
                "envelope": {
                    "code": 0,
                    "data": {"bvid": BV, "aid": 100, "cid": 200, "duration": 60, "title": "t"},
                }
            }
        if path == "/x/player/wbi/v2":
            return {
                "envelope": {
                    "code": 0,
                    "data": {
                        "subtitle": {
                            "subtitles": [
                                {
                                    "lan": "zh-CN",
                                    "subtitle_url": "https://aisubtitle.hdslb.com/bfs/sub/1.json",
                                }
                            ]
                        }
                    },
                }
            }
        if params["subtitle"]:
            assert params["cookies"] == {}
            return {"envelope": {"body": [{"content": "CN节点字幕", "from": 0, "to": 60}]}}
        if path == "/x/v2/reply":
            return {"envelope": {"code": 0, "data": {"replies": []}}}
        raise AssertionError(f"Unexpected path {path}")

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    svc, _ = _make_watcher_service(settings, store, message, nt)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: http_calls.append(r) or httpx.Response(500))
    ) as http:
        w = BiliWatcher(svc, http, sleep=lambda _: asyncio.sleep(0))
        await w.initialize()
        await w.update_credentials(
            CredentialUpdate(sessdata="sess-cn-test", bili_jct="jct-cn-test", buvid3="b3-cn")
        )
        res = await w.nav()
        _, mat = await w.material(BV)

    assert res == {"status": "valid", "account_mask": "…3456"}
    assert w.creds["buvid4"] == "cn-buvid4-val"
    assert mat["subtitle_excerpt"] == "CN节点字幕"
    assert not http_calls
    spi_req = next(r for r in rpc_log if r["path"] == SPI_PATH)
    assert "buvid4" not in spi_req["cookies"]
    nav_req = next(r for r in rpc_log if r["path"] == "/x/web-interface/nav")
    assert nav_req["cookies"]["buvid4"] == "cn-buvid4-val"
    sub_req = next(r for r in rpc_log if r["subtitle"])
    assert sub_req["cookies"] == {}


async def test_bili_watcher_public_material_anonymous_sends_empty_cookies(settings, store, message):
    rpc_log = []

    def node_handler(_action, params, _timeout):
        rpc_log.append(params)
        if params["path"] == "/x/web-interface/nav":
            return {"envelope": {"code": -101, "data": {**NAV, "isLogin": False}}}
        if params["path"] == "/x/web-interface/view":
            return {
                "envelope": {
                    "code": 0,
                    "data": {"bvid": BV, "aid": 100, "cid": 200, "duration": 60},
                }
            }
        if params["path"] == "/x/player/wbi/v2":
            return {"envelope": {"code": 0, "data": {"subtitle": {"subtitles": []}}}}
        if params["path"] == "/x/v2/reply":
            return {"envelope": {"code": 0, "data": {"replies": []}}}
        raise AssertionError(params)

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    svc, _ = _make_watcher_service(settings, store, message, nt)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(500))) as h:
        w = BiliWatcher(svc, h, sleep=lambda _: asyncio.sleep(0))
        await w.initialize()
        await w.update_credentials(CredentialUpdate(clear=True))
        video = SimpleNamespace(bvid=BV, title="T", description="D")
        mat = await w.public_material(video)

    assert mat["material_scope"] == "metadata_only"
    assert all(r["cookies"] == {} for r in rpc_log)
    assert w.blocked_until == 0.0


async def test_bili_watcher_get_fallback_and_ambiguous_post_never_replayed(
    settings, store, message
):
    settings.owner_qq_id = "20000"
    http_calls = []

    def http_handler(req):
        http_calls.append((req.method, req.url.path))
        if req.url.path == "/x/web-interface/nav":
            return httpx.Response(200, json={"code": 0, "data": NAV})
        if req.url.path == "/x/web-interface/archive/like":
            return httpx.Response(200, json={"code": 0, "data": {}})
        return httpx.Response(200, json={"code": 0, "data": {}})

    # 1) GET timeout falls back cleanly to local HTTP.
    nt_timeout = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(Rejected("node_rpc_timeout")),
    )
    svc, _ = _make_watcher_service(settings, store, message, nt_timeout)
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as http:
        w = BiliWatcher(svc, http, sleep=lambda _: asyncio.sleep(0))
        await w.initialize()
        await w.update_credentials(
            CredentialUpdate(sessdata="s", bili_jct="j", buvid3="b3", buvid4="b4")
        )
        assert (await w.nav())["status"] == "valid"
        assert ("GET", "/x/web-interface/nav") in http_calls

        # 2) Mutating POST with node_rpc_timeout or node_unavailable MUST NOT replay locally!
        http_calls.clear()
        for err in ("node_rpc_timeout", "node_unavailable"):
            nt_timeout.handler = lambda *_, e=err: (_ for _ in ()).throw(Rejected(e))
            with pytest.raises(Rejected, match="bili_account_unavailable"):
                await w._request("/x/web-interface/archive/like", post={"aid": 1, "like": 1})
            assert not http_calls

        # 3) Mutating POST with node_not_dispatched / node_disconnected CAN fall back locally.
        nt_timeout.handler = lambda *_: (_ for _ in ()).throw(Rejected("node_not_dispatched"))
        await w._request("/x/web-interface/archive/like", post={"aid": 1, "like": 1})
        assert http_calls == [("POST", "/x/web-interface/archive/like")]


async def test_bili_watcher_post_timeout_in_patrol_marks_action_uncertain(settings, store, message):
    settings.owner_qq_id = "20000"
    http_calls = []

    def node_handler(_action, params, _timeout):
        path = params["path"]
        if path == "/x/web-interface/nav":
            return {"envelope": {"code": 0, "data": NAV}}
        if path == "/x/web-interface/wbi/index/top/feed/rcmd":
            item = {"goto": "av", "bvid": BV, "title": "T", "owner": {"name": "U"}}
            return {"envelope": {"code": 0, "data": {"item": [item]}}}
        if path == "/x/polymer/web-dynamic/v1/feed/all":
            return {"envelope": {"code": 0, "data": {"items": []}}}
        if path == "/x/web-interface/view":
            return {"envelope": {"code": 0, "data": {"bvid": BV, "aid": 100, "cid": 200}}}
        if path == "/x/player/wbi/v2":
            return {"envelope": {"code": 0, "data": {"subtitle": {"subtitles": []}}}}
        if path == "/x/v2/reply":
            return {"envelope": {"code": 0, "data": {"replies": []}}}
        if path == "/x/web-interface/archive/relation":
            return {"envelope": {"code": 0, "data": {"like": False}}}
        if path == "/x/web-interface/archive/like":
            raise Rejected("node_rpc_timeout")
        raise AssertionError(path)

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    svc, _ = _make_watcher_service(settings, store, message, nt)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: http_calls.append(r) or httpx.Response(200))
    ) as http:
        w = BiliWatcher(svc, http, sleep=lambda _: asyncio.sleep(0))
        await w.initialize()
        await w.update_credentials(
            CredentialUpdate(sessdata="s", bili_jct="j", buvid3="b3", buvid4="b4")
        )
        await w.set_policy(BiliPolicy(enabled=True, allow_like=True))
        res = await w.run_once()

    assert res["status"] == "failed" and res["reason"] == "bili_account_unavailable"
    assert not http_calls
    row = await store.one("SELECT status FROM bili_actions WHERE bvid=? AND kind='like'", (BV,))
    assert row is not None and row[0] == "uncertain"


async def test_bili_watcher_delegated_412_triggers_risk_in_request_and_buvid4(
    settings, store, message
):
    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(Rejected("bili_http_status", detail="412")),
    )
    svc, alerts = _make_watcher_service(settings, store, message, nt)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as h:
        w = BiliWatcher(svc, h, sleep=lambda _: asyncio.sleep(0))
        await w.initialize()
        await w.update_credentials(CredentialUpdate(sessdata="s", bili_jct="j"))
        with pytest.raises(Rejected, match="bili_account_risk"):
            await w.nav()
    assert w.blocked_until > time.time() + 43000
    assert len(alerts) == 1


async def test_bili_watcher_fetch_cover_thumbnail_allowlist_and_validation(
    settings, store, message
):
    good_jpg = b"\xff\xd8\xff\xe0" + b"a" * 128
    good_b64 = base64.b64encode(good_jpg).decode("ascii")
    response_holder = [
        {"mime_type": "image/jpeg", "data_b64": good_b64, "bytes": len(good_jpg), "width": 320}
    ]

    def node_handler(action, params, _timeout):
        assert action == "bili.cover_thumb"
        return response_holder[0]

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    svc, _ = _make_watcher_service(settings, store, message, nt)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as h:
        w = BiliWatcher(svc, h)
        await w.initialize()

        ok = await w.fetch_cover_thumbnail("https://i0.hdslb.com/bfs/archive/cover123.jpg")
        assert ok is not None and ok["bytes"] == len(good_jpg)

        ok2 = await w.fetch_cover_thumbnail("https://archive.biliimg.com/bfs/new/cover-1.png")
        assert ok2 is not None

        # Invalid URLs never trigger RPC
        before = len(nt.calls)
        for bad_url in (
            "https://evil.example/bfs/archive/x.jpg",
            "https://i0.hdslb.com/bfs/../secret.jpg",
            "https://i0.hdslb.com/bfs/archive/x.jpg?token=1",
            "http://i0.hdslb.com/bfs/archive/x.jpg",
        ):
            assert await w.fetch_cover_thumbnail(bad_url) is None
        assert len(nt.calls) == before

        # Bad mime / oversized / bad base64 rejected
        response_holder[0] = {"mime_type": "image/png", "data_b64": good_b64, "bytes": 100}
        assert await w.fetch_cover_thumbnail("https://i1.hdslb.com/bfs/archive/x.jpg") is None

        response_holder[0] = {"mime_type": "image/jpeg", "data_b64": good_b64, "bytes": 98305}
        assert await w.fetch_cover_thumbnail("https://i1.hdslb.com/bfs/archive/x.jpg") is None

        oversized = base64.b64encode(b"x" * 98305).decode("ascii")
        response_holder[0] = {"mime_type": "image/jpeg", "data_b64": oversized, "bytes": 100}
        assert await w.fetch_cover_thumbnail("https://i1.hdslb.com/bfs/archive/x.jpg") is None

        response_holder[0] = {"mime_type": "image/jpeg", "data_b64": "!!!", "bytes": 10}
        assert await w.fetch_cover_thumbnail("https://i1.hdslb.com/bfs/archive/x.jpg") is None

        nt._connected = False
        assert await w.fetch_cover_thumbnail("https://i1.hdslb.com/bfs/archive/x.jpg") is None


# ---------------------------------------------------------------------------
# 3. VideoWatch dual-lane cn_storyboard fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "lane1_error",
    [
        "video_download_restricted",
        "video_download_unavailable",
        "video_download_timeout",
        "video_download_network_forbidden",
    ],
)
async def test_video_watch_lane2_cn_storyboard_fallback_succeeds(
    settings, store, tmp_path, message, lane1_error
):
    storyboard_bytes = b"\xff\xd8\xff\xe0storyboard-3x3" + b"0" * 512
    storyboard_b64 = base64.b64encode(storyboard_bytes).decode("ascii")

    def node_handler(action, params, timeout):
        assert action == "bili.video_storyboard"
        assert timeout == 120.0
        assert params["bvid"] == BV
        assert params["max_seconds"] == 180
        assert params["cookies"] == {"sessdata": "s", "bili_jct": "j"}
        return {
            "bvid": BV,
            "duration": 75.5,
            "mime_type": "image/jpeg",
            "data_b64": storyboard_b64,
            "bytes": len(storyboard_bytes),
            "grid": "3x3",
        }

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = lane1_error
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s", "bili_jct": "j", "buvid4": ""},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings,
        store=store,
        ai=gateway,
        bili_watcher=watcher,
        secrets=[],
        node_transport=nt,
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True, max_seconds=180))

    res = await watch.analyze(BV, material(), "subject", "chat", message=message)
    assert res.mode == "cn_storyboard" and res.reason is None
    assert [c[0] for c in gateway.calls] == ["screen", "screen", "generate"]
    assert gateway.calls[1][1] == {"storyboard_bvid": BV}
    assert gateway.calls[1][3]["purpose"] == "source"
    assert len(gateway.calls[1][3]["images"]) == 1
    gen_call = gateway.calls[2]
    assert gen_call[1]["analysis_mode"] == "cn_keyframe_storyboard"
    assert "3x3 关键帧拼图" in gen_call[1]["storyboard_note"]
    assert gen_call[3]["summary"] is True
    imgs = gen_call[3]["images"]
    assert len(imgs) == 1 and isinstance(imgs[0], ModelImage)
    assert imgs[0].mime_type == "image/jpeg" and imgs[0].data == storyboard_b64

    status = await watch.status()
    assert status["attempts_today"] == 1
    assert status["recent_attempts"][0]["status"] == "done"
    assert status["recent_attempts"][0]["bytes"] == len(storyboard_bytes)
    assert status["recent_attempts"][0]["duration"] == 75.5


@pytest.mark.parametrize("lane1_error", ["video_download_size", "video_download_format"])
async def test_video_watch_lane2_skipped_for_non_fallback_errors_or_offline(
    settings, store, tmp_path, lane1_error
):
    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(AssertionError("should not call")),
    )
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = lane1_error
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[], node_transport=nt
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))

    res = await watch.analyze(BV, material(), "subject", "chat")
    assert res.mode == "text"
    assert not nt.calls

    # Also skipped when offline even on video_download_restricted
    nt._connected = False
    downloader.error = "video_download_restricted"
    res2 = await watch.analyze(BV, material(), "subject", "chat")
    assert res2.mode == "text" and res2.reason == "download_restricted"
    assert not nt.calls


@pytest.mark.parametrize(
    "bad_sb",
    [
        Rejected("node_rpc_timeout"),
        {"mime_type": "image/png", "data_b64": base64.b64encode(b"x").decode()},
        {"mime_type": "image/jpeg", "data_b64": base64.b64encode(b"x" * 262_145).decode()},
        {"mime_type": "image/jpeg", "data_b64": "not-valid-b64!"},
    ],
)
async def test_video_watch_lane2_failure_preserves_lane1_reason(settings, store, tmp_path, bad_sb):
    def node_handler(*_args):
        if isinstance(bad_sb, Exception):
            raise bad_sb
        return bad_sb

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = "video_download_restricted"
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[], node_transport=nt
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))

    res = await watch.analyze(BV, material(), "subject", "chat")
    assert res.mode == "text" and res.reason == "download_restricted"
    assert (await watch.status())["attempts_today"] == 0


async def test_video_watch_lane2_hot_revocation_and_cancellation(
    settings, store, tmp_path, message
):
    entered = asyncio.Event()
    resume = asyncio.Event()
    clock = [1_700_000_000.0]

    async def slow_sb(*_args):
        entered.set()
        await resume.wait()
        return {
            "mime_type": "image/jpeg",
            "data_b64": base64.b64encode(b"jpg").decode(),
        }

    nt = FakeNodeTransport(is_connected=True, handler=slow_sb)
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = "video_download_unavailable"
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[], node_transport=nt
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: clock[0])
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))

    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    task = asyncio.create_task(watch.analyze(BV, material(), "friend", "chat", message=friend))
    await asyncio.wait_for(entered.wait(), 2)
    await store.runtime.change_admission("allowed_users", "remove", "30000")
    resume.set()
    with pytest.raises(Rejected, match="video_watch_invalidated"):
        await task
    assert (await watch.status())["recent_attempts"][0]["reason"] == "request_rejected"

    # Cancellation during storyboard RPC marks uncertain/interrupted
    clock[0] += 10
    entered.clear()
    resume.clear()
    task2 = asyncio.create_task(watch.analyze(BV, material(), "owner", "chat", message=message))
    await asyncio.wait_for(entered.wait(), 2)
    task2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task2
    assert (await watch.status())["recent_attempts"][0]["status"] == "uncertain"
    assert (await watch.status())["recent_attempts"][0]["reason"] == "interrupted"


async def test_watcher_and_video_watch_respect_delegate_and_fallback_flags(
    settings, store, tmp_path, message
):
    http_calls = []

    def http_handler(req):
        http_calls.append(req.url.path)
        return httpx.Response(200, json={"code": 0, "data": NAV})

    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(Rejected("node_disconnected")),
    )
    # 1) BiliWatcher with node_bili_delegate_enabled=False ignores connected node_transport
    svc_off, _ = _make_watcher_service(
        _SettingsProxy(settings, node_bili_delegate_enabled=False), store, message, nt
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(http_handler)) as http:
        w_off = BiliWatcher(svc_off, http, sleep=lambda _: asyncio.sleep(0))
        await w_off.initialize()
        await w_off.update_credentials(
            CredentialUpdate(sessdata="s", bili_jct="j", buvid3="b3", buvid4="b4")
        )
        assert (await w_off.nav())["status"] == "valid"
        assert not nt.calls and http_calls == ["/x/web-interface/nav"]

        # 2) BiliWatcher with node_bili_fallback_local=False fails on node_disconnected
        http_calls.clear()
        svc_nofb, _ = _make_watcher_service(
            _SettingsProxy(settings, node_bili_fallback_local=False), store, message, nt
        )
        w_nofb = BiliWatcher(svc_nofb, http, sleep=lambda _: asyncio.sleep(0))
        await w_nofb.initialize()
        with pytest.raises(Rejected, match="bili_account_unavailable"):
            await w_nofb.nav()
        assert nt.calls and not http_calls

    # 3) VideoWatch with node_bili_delegate_enabled=False skips Lane 2 storyboard
    nt.calls.clear()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = "video_download_restricted"
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "s"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc_vw = SimpleNamespace(
        s=_SettingsProxy(settings, node_bili_delegate_enabled=False),
        store=store,
        ai=FakeGateway(),
        bili_watcher=watcher,
        secrets=[],
        node_transport=nt,
    )
    watch = VideoWatch(svc_vw, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))
    res = await watch.analyze(BV, material(), "subject", "chat")
    assert res.mode == "text" and res.reason == "download_restricted"
    assert not nt.calls


# ---------------------------------------------------------------------------
# 4. BilibiliSurfModule (CN专线) sensor summary indicator
# ---------------------------------------------------------------------------


async def test_bilibili_surf_module_cn_line_indicator(settings):
    nt_on = FakeNodeTransport(is_connected=True)
    nt_off = FakeNodeTransport(is_connected=False)

    mod_direct = BilibiliSurfModule(settings, node_transport=nt_on)
    assert "B站冲浪(CN专线):" in await mod_direct.get_sensor_summary()

    bili_with_nt = SimpleNamespace(node_transport=nt_on, blocked_until=0.0)
    mod_via_bili = BilibiliSurfModule(settings, bili=bili_with_nt)
    assert "B站冲浪(CN专线):" in await mod_via_bili.get_sensor_summary()

    mod_offline = BilibiliSurfModule(settings, node_transport=nt_off)
    s_off = await mod_offline.get_sensor_summary()
    assert s_off.startswith("B站冲浪:") and "(CN专线)" not in s_off

    mod_disabled = BilibiliSurfModule(
        _SettingsProxy(settings, node_bili_delegate_enabled=False), node_transport=nt_on
    )
    s_dis = await mod_disabled.get_sensor_summary()
    assert s_dis.startswith("B站冲浪:") and "(CN专线)" not in s_dis


@pytest.mark.parametrize(
    "lane1_error",
    [
        "video_download_cdn_http_567",
        "video_download_cdn_http_403",
        "video_download_cdn_host_blocked",
        "video_download_no_audio_track",
    ],
)
async def test_video_watch_lane2_rescues_cdn_errors_without_account_cooldown(
    settings, store, tmp_path, message, lane1_error
):
    sb_bytes = b"\xff\xd8\xff\xe0videoshot-3x3" + b"0" * 256
    sb_b64 = base64.b64encode(sb_bytes).decode("ascii")

    def node_handler(action, params, _timeout):
        assert action == "bili.video_storyboard"
        assert params["page"] == 2
        assert params["cid"] == 202
        return {
            "bvid": BV,
            "duration": 60.0,
            "mime_type": "image/jpeg",
            "data_b64": sb_b64,
            "bytes": len(sb_bytes),
            "grid": "3x3",
            "source": "videoshot",
        }

    nt = FakeNodeTransport(is_connected=True, handler=node_handler)
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = lane1_error
    risks = []
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "secret-cookie-must-not-log"},
        blocked_until=0.0,
        _ensure_current=lambda: None,
        _risk=lambda r: risks.append(r),
    )
    svc = SimpleNamespace(
        s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[], node_transport=nt
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))

    mat = {**material(), "cid": 202, "page": 2}
    res = await watch.analyze(BV, mat, "subject", "chat", message=message)
    assert res.mode == "cn_storyboard" and res.reason is None
    assert not risks and watcher.blocked_until == 0.0
    assert downloader.calls[0][1]["page"] == 2


async def test_video_watch_storyboard_failure_logged_and_long_video_uses_videoshot_only(
    settings, store, tmp_path, message, caplog
):
    import logging

    caplog.set_level(logging.INFO, logger="hakimi")
    nt = FakeNodeTransport(
        is_connected=True,
        handler=lambda *_: (_ for _ in ()).throw(Rejected("videoshot_unavailable")),
    )
    gateway = FakeGateway()
    downloader = Downloader(tmp_path / "unused.mp4")
    downloader.error = "video_download_unavailable"
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "top-secret-sessdata-xyz"},
        _ensure_current=lambda: None,
        _risk=lambda r: None,
    )
    svc = SimpleNamespace(
        s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[], node_transport=nt
    )
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: 1_700_000_000.0)
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True, max_seconds=180))

    res = await watch.analyze(BV, material(), "subject", "chat", message=message)
    assert res.mode == "text" and res.reason == "storyboard_err"
    assert (await watch.status())["recent_attempts"][0]["reason"] == "storyboard_err"
    assert "event=video_watch_storyboard_failed" in caplog.text
    assert "videoshot_unavailable" in caplog.text
    assert "top-secret-sessdata-xyz" not in caplog.text

    # Long video (> max_seconds) skips Lane 1 and calls videoshot_only=True on Lane 2.
    sb_bytes = b"\xff\xd8\xff\xe0long-videoshot" + b"1" * 128
    sb_b64 = base64.b64encode(sb_bytes).decode("ascii")
    nt.calls.clear()
    downloader.calls.clear()
    nt.handler = lambda _action, params, _timeout: (
        {
            "bvid": BV,
            "duration": 1800.0,
            "mime_type": "image/jpeg",
            "data_b64": sb_b64,
            "bytes": len(sb_bytes),
            "grid": "3x3",
            "source": "videoshot",
        }
        if params.get("videoshot_only") is True
        else (_ for _ in ()).throw(AssertionError("expected videoshot_only"))
    )
    res_long = await watch.analyze(BV, material(duration=1800), "subject", "chat", message=message)
    assert res_long.mode == "cn_storyboard" and res_long.reason is None
    assert not downloader.calls
    assert nt.calls[0][1]["videoshot_only"] is True
