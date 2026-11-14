"""Subtitle evidence and read-only link collection, with no real Bili requests."""

import time
from types import SimpleNamespace

import httpx
import pytest

from hakimi.bili_watcher import BiliPolicy, CredentialUpdate, subtitle_text, video_duration
from hakimi.models import Video
from hakimi.security import Rejected
from tests.test_bili_watcher import BV, NAV, OTHER
from tests.test_bili_watcher import watcher as watcher


@pytest.fixture
def video():
    return Video(
        bvid=BV,
        mid="12345",
        title="Public science",
        description="Known description",
        author="Public UP",
        published_at=1234,
    )


@pytest.mark.parametrize(
    "duration", [None, True, -1, 0, float("nan"), float("inf"), "480", 10**500]
)
def test_invalid_duration_is_not_usable(duration):
    assert video_duration(duration) is None


def test_full_subtitle_requires_valid_tail_and_is_not_just_old_excerpt():
    body = {"body": [{"content": "字" * 4000, "from": 0, "to": 60}]}
    text, complete, status = subtitle_text(body, 60)
    assert len(text) == 4000 and complete and status == "complete"
    assert subtitle_text(body, None)[1:] == (False, "partial")
    assert subtitle_text(body, 120)[1:] == (False, "partial")


@pytest.mark.parametrize(
    "line",
    [
        {"content": "no timestamps"},
        {"content": "tail only", "from": 58, "to": 60},
        {"content": "bad start", "from": -1, "to": 60},
        {"content": "bad end", "from": 0, "to": float("inf")},
        {"content": "overflow", "from": 0, "to": 10**500},
        {"content": "boolean", "from": False, "to": 60},
        {"content": "out of range", "from": 0, "to": 100},
    ],
)
def test_partial_or_invalid_timing_never_claims_full_coverage(line):
    _, complete, status = subtitle_text({"body": [line]}, 60)
    assert not complete and status == "partial"


@pytest.mark.parametrize("body", [{"body": []}, {"body": [None]}, {}, {"body": "bad"}])
def test_missing_or_malformed_subtitle_is_unavailable(body):
    assert subtitle_text(body, 60) == ("", False, "unavailable")


@pytest.mark.parametrize("by_lines", [False, True])
def test_text_and_line_caps_preserve_honest_excerpt(by_lines):
    body = {
        "body": [{"content": "text" if by_lines else "字" * 12001, "from": 0, "to": 60}]
        * (1001 if by_lines else 1)
    }
    text, complete, status = subtitle_text(body, 60)
    assert 0 < len(text) <= 12000 and not complete and status == "truncated"


async def test_public_read_works_anonymously_with_patrol_disabled(watcher, video):
    await watcher.update_credentials(CredentialUpdate(clear=True))
    watcher.responses["/x/web-interface/nav"] = {**NAV, "isLogin": False, "mid": 0}
    watcher.responses["/x/web-interface/view"]["duration"] = 60
    watcher.responses["/bfs/subtitle/test.json"] = {
        "body": [{"content": "公开科学字幕", "from": 0, "to": 60}]
    }
    material = await watcher.public_material(video)
    assert material["duration"] == 60.0
    assert material["subtitle_complete"] is True and material["subtitle_status"] == "complete"
    assert material["hot_comments_unverified"] == ["一条高赞评论"]
    assert not watcher.policy.enabled and watcher.account is None
    assert all(r.method == "GET" and not r.headers.get("cookie") for r in watcher.calls)
    assert not watcher.svc.ai.calls
    assert watcher.public_read_context.get() is None
    with pytest.raises(Rejected, match="bili_credentials_missing"):
        await watcher.nav()


async def test_public_read_reuses_credentials_but_never_sends_cookie_to_subtitle(watcher, video):
    result = await watcher.public_material(video)
    assert result["subtitle_excerpt"]
    api = [r for r in watcher.calls if r.url.host == "api.bilibili.com"]
    cdn = [r for r in watcher.calls if r.url.host != "api.bilibili.com"]
    assert api and all("SESSDATA=private-cookie-value" in r.headers["cookie"] for r in api)
    assert cdn and all(not r.headers.get("cookie") for r in cdn)
    assert not watcher.svc.ai.calls


async def test_public_context_cannot_make_account_writes_or_homepage_requests(watcher):
    token = watcher.public_read_context.set((watcher.credentials_epoch, None))
    try:
        for path, args in [
            ("/x/web-interface/archive/like", {"post": {"aid": 1, "like": 1}}),
            ("/x/web-interface/wbi/index/top/feed/rcmd", {}),
        ]:
            with pytest.raises(Rejected, match="bili_public_read_only"):
                await watcher._request(path, **args)
    finally:
        watcher.public_read_context.reset(token)
    assert not watcher.calls


async def test_public_context_cannot_override_existing_patrol_revocation(watcher, video):
    await watcher.set_policy(BiliPolicy(enabled=True))
    token = watcher.patrol_context.set((watcher.policy_epoch, watcher.credentials_epoch))
    await watcher.set_policy(BiliPolicy(enabled=False))
    try:
        with pytest.raises(Rejected, match="bili_settings_changed"):
            await watcher.public_material(video)
    finally:
        watcher.patrol_context.reset(token)
    assert not watcher.calls


async def test_public_guard_rechecked_after_jitter_before_request(watcher, video):
    allowed = [True]

    async def sleep(_):
        allowed[0] = False

    watcher.sleep = sleep
    with pytest.raises(Rejected, match="bili_request_revoked"):
        await watcher.public_material(video, guard=lambda: allowed[0])
    assert len(watcher.calls) == 1
    assert watcher.public_read_context.get() is None


async def test_public_credential_rotation_does_not_continue_with_new_cookie(watcher, video):
    original = watcher._request

    async def request(path, *args, **kwargs):
        result = await original(path, *args, **kwargs)
        if path == "/x/web-interface/nav":
            await watcher.update_credentials(CredentialUpdate(sessdata="new-cookie"))
        return result

    watcher._request = request
    with pytest.raises(Rejected, match="bili_settings_changed"):
        await watcher.public_material(video)
    assert len(watcher.calls) == 1


async def test_public_source_failure_returns_only_known_metadata(watcher, video):
    async def fail(*_, **__):
        raise Rejected("bili_account_unavailable")

    watcher._request = fail
    result = await watcher.public_material(video)
    assert result["title"] == video.title and result["description"] == video.description
    assert result["material_error"] == "bili_account_unavailable"
    assert result["subtitle_complete"] is False
    assert not watcher.svc.ai.calls


async def test_player_unavailable_retains_description_and_comments(watcher, video):
    original = watcher.http._transport

    async def handler(req):
        if req.url.path == "/x/player/wbi/v2":
            return httpx.Response(200, json={"code": -404})
        return await original.handle_async_request(req)

    watcher.http._transport = httpx.MockTransport(handler)
    result = await watcher.public_material(video)
    assert result["subtitle_status"] == "unavailable"
    assert result["description"] == "public description"
    assert result["hot_comments_unverified"] == ["一条高赞评论"]


async def test_public_risk_is_never_downgraded_or_followed_by_another_request(watcher, video):
    original = watcher.http._transport

    async def handler(req):
        if req.url.path == "/x/player/wbi/v2":
            return httpx.Response(200, json={"code": -412})
        return await original.handle_async_request(req)

    watcher.http._transport = httpx.MockTransport(handler)
    with pytest.raises(Rejected, match="bili_account_risk"):
        await watcher.public_material(video)
    assert watcher.blocked_until > time.time() + 43000
    assert not any(r.url.path == "/x/v2/reply" for r in watcher.calls)
    assert not watcher.svc.ai.calls


async def test_context_byte_clipping_removes_complete_claim(watcher):
    payload = {
        "description": "desc",
        "subtitle_excerpt": "字" * 12000,
        "subtitle_complete": True,
        "subtitle_status": "complete",
        "evidence": "complete",
    }
    watcher.s.max_context_bytes = 2048
    clipped = watcher._bounded(payload)
    assert clipped["subtitle_complete"] is False and clipped["subtitle_status"] == "truncated"
    assert "字幕节选" in clipped["evidence"]


async def test_patrol_uses_shared_video_analyzer_and_records_evidence(watcher):
    calls = []

    async def analyze(bvid, material, subject, lane, **kwargs):
        calls.append((bvid, material, lane))
        kwargs["guard"]()
        return SimpleNamespace(
            text="已分析提供的视频画面和声音。",
            evidence="已提供短视频文件",
            mode="video",
            reason=None,
        )

    watcher.svc.video_watch = SimpleNamespace(analyze=analyze)
    await watcher.set_policy(BiliPolicy(enabled=True))
    result = await watcher.run_once()
    assert result["learned"] == 1 and result["status"] == "done"
    assert calls[0][0] == BV and calls[0][2] == "patrol"
    assert [c[0] for c in watcher.svc.ai.calls] == ["screen", "generate"]
    row = await watcher.store.one("SELECT summary FROM bili_watched_history WHERE bvid=?", (BV,))
    saved = watcher.store.vault.open("bili-learn:" + BV, row[0])
    assert "已提供短视频文件" in saved and "公开视频学习" in saved


async def test_download_risk_records_metadata_then_stops_writes_and_next_video(watcher):
    original_generate = watcher.svc.ai.generate

    async def generate(*args, **kwargs):
        await original_generate(*args, **kwargs)
        return '{"selected":[{"bvid":"' + BV + '","like":true},{"bvid":"' + OTHER + '"}]}'

    watcher.svc.ai.generate = generate
    watcher.responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"].append(
        {"goto": "av", "bvid": OTHER, "title": "second", "owner": {"name": "UP"}}
    )
    calls = []

    async def analyze(*args, **kwargs):
        calls.append(args[0])
        try:
            await watcher._risk("bili_account_risk")
        except Rejected:
            pass
        return SimpleNamespace(
            text="已有简介摘录。", evidence="没有完成原片分析", mode="metadata", reason="risk"
        )

    watcher.svc.video_watch = SimpleNamespace(analyze=analyze)
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    result = await watcher.run_once()
    assert result == {"status": "failed", "learned": 1, "reason": "bili_account_risk"}
    assert calls == [BV]
    assert not any(r.method == "POST" for r in watcher.calls)
    row = await watcher.store.one("SELECT summary FROM bili_watched_history WHERE bvid=?", (BV,))
    saved = watcher.store.vault.open("bili-learn:" + BV, row[0])
    assert "已有简介摘录" in saved
    assert not await watcher.store.one("SELECT 1 FROM bili_watched_history WHERE bvid=?", (OTHER,))


async def test_anonymous_nav_can_use_public_keys_without_retry_or_identity_switch(watcher, video):
    await watcher.update_credentials(CredentialUpdate(clear=True))
    original = watcher.http._transport
    nav_calls = []

    async def handler(req):
        if req.url.path == "/x/web-interface/nav":
            nav_calls.append(req)
            return httpx.Response(200, json={"code": -101, "data": {**NAV, "isLogin": False}})
        return await original.handle_async_request(req)

    watcher.http._transport = httpx.MockTransport(handler)
    result = await watcher.public_material(video)
    assert result["subtitle_excerpt"]
    assert len(nav_calls) == 1 and not nav_calls[0].headers.get("cookie")
    assert watcher.blocked_until == 0 and not watcher.alerts
    assert not watcher.svc.ai.calls


@pytest.mark.parametrize("case", ["cookie", "voucher", "other_endpoint", "412", "352"])
async def test_anonymous_nav_exception_never_relaxes_other_risk_conditions(watcher, video, case):
    if case != "cookie":
        await watcher.update_credentials(CredentialUpdate(clear=True))
    original = watcher.http._transport
    requested = []

    async def handler(req):
        requested.append(req.url.path)
        if req.url.path == "/x/web-interface/nav":
            code = -412 if case == "412" else -352 if case == "352" else -101
            data = {**NAV, "isLogin": False}
            if case == "voucher":
                data["v_voucher"] = "challenge-do-not-bypass"
            return httpx.Response(200, json={"code": code, "data": data})
        if case == "other_endpoint" and req.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": -101})
        return await original.handle_async_request(req)

    watcher.http._transport = httpx.MockTransport(handler)
    with pytest.raises(Rejected):
        await watcher.public_material(video)
    assert watcher.blocked_until > watcher.clock() + 43000
    assert len(watcher.alerts) == 1 and not watcher.svc.ai.calls
    assert len(requested) == (2 if case == "other_endpoint" else 1)
