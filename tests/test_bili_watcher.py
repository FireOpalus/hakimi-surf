import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from hakimi.bili_watcher import BiliPolicy, BiliWatcher, CredentialUpdate, wbi_sign
from hakimi.budget_policy import BudgetConfig
from hakimi.generation_policy import Scenario
from hakimi.life_rules import milestone_sharing
from hakimi.security import Rejected
from hakimi.session_manager import SessionManager
from hakimi.world_state import MilestoneEvent
from tests.fakes import FakeGateway

BV = "BV1xx411c7mD"
OTHER = "BV1Q541167Qg"
NAV = {
    "isLogin": True,
    "mid": 123456,
    "wbi_img": {
        "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
        "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
    },
}


class Gateway(FakeGateway):
    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        if kwargs["scenario"] == Scenario.BILI_SELECTION:
            return json.dumps({"selected": [{"bvid": BV, "like": True, "favorite": True}]})
        return "标题与字幕节选介绍了一个科学问题，评论仅代表用户观点。"


@pytest.fixture
async def watcher(settings, store, message):
    settings.owner_qq_id = "20000"
    responses = {
        "/x/web-interface/nav": NAV,
        "/x/web-interface/wbi/index/top/feed/rcmd": {
            "item": [{"goto": "av", "bvid": BV, "title": "科学", "owner": {"name": "UP"}}]
        },
        "/x/polymer/web-dynamic/v1/feed/all": {"items": []},
        "/x/web-interface/view": {
            "bvid": BV,
            "aid": 100,
            "cid": 200,
            "title": "science",
            "desc": "public description",
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
        "/bfs/subtitle/test.json": {"body": [{"content": "公开字幕文字"}]},
        "/x/v2/reply": {"replies": [{"content": {"message": "一条高赞评论"}}]},
        "/x/web-interface/archive/like": {},
        "/x/web-interface/archive/relation": {"like": False},
        "/x/v3/fav/folder/created/list-all": {"list": [{"id": 888}]},
        "/x/v3/fav/resource/deal": {},
    }
    calls = []
    alerts = []
    naps = []

    async def alert(*args):
        alerts.append(args)

    async def sleep(n):
        naps.append(n)

    def handler(req):
        calls.append(req)
        value = responses[req.url.path]
        return httpx.Response(
            200, json=value if req.url.path.startswith("/bfs/") else {"code": 0, "data": value}
        )

    svc = SimpleNamespace(
        s=settings,
        store=store,
        secrets=[],
        ai=Gateway(),
        diary=None,
        proactive=SimpleNamespace(alert=alert, target=lambda: message),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        w = BiliWatcher(svc, http, sleep=sleep)
        w.responses, w.calls, w.alerts, w.naps = responses, calls, alerts, naps
        await w.initialize()
        await w.update_credentials(
            CredentialUpdate(
                sessdata="private-cookie-value",
                bili_jct="c" * 32,
                buvid3="device-cookie-value",
                buvid4="device-v4-cookie-value",
            )
        )
        yield w


def test_wbi_fixed_vector_and_no_input_mutation():
    values = {"foo": "114", "bar": "514", "zab": 1919810}
    signed = wbi_sign(
        values, ("7cd084941338484aae1ad9425b84077c", "4932caff0ff746eab6f01bf08b70ac45"), 1702204169
    )
    assert signed["w_rid"] == "8f6f2b5b3d485fe1886cec6a0be8c5d4"
    assert "wts" not in values


@pytest.mark.parametrize("value", ["abc; bad=x", "abc\nX: y", "abc def", "中文"])
def test_invalid_cookie_values(value):
    with pytest.raises(ValidationError):
        CredentialUpdate(sessdata=value)


@pytest.mark.parametrize(
    "data",
    [
        {"times": ["13:00", "13:00"]},
        {"times": ["25:00"]},
        {"allow_favorite": True},
        {"daily_action_limit": 3},
        {"allow_like": "true"},
        {"url": "https://evil.invalid"},
    ],
)
def test_policy_validates_limits(data):
    with pytest.raises(ValidationError):
        BiliPolicy(**data)


async def test_disabled_and_hard_budget_block_before_network_and_model(watcher):
    with pytest.raises(Rejected, match="bili_autonomous_disabled"):
        await watcher.run_once()
    await watcher.set_policy(BiliPolicy(enabled=True))
    await watcher.store.db.execute("INSERT INTO meta VALUES ('budget_overrun','1')")
    with pytest.raises(Rejected, match="budget_hard_stop"):
        await watcher.run_once()
    assert not watcher.calls and not watcher.svc.ai.calls


async def test_credentials_encrypted_masked_hot_and_persisted(watcher):
    status = await watcher.status()
    assert status["credentials"] == {
        "sessdata": True,
        "bili_jct": True,
        "buvid3": True,
        "buvid4": True,
    }
    assert "private-cookie" not in json.dumps(status)
    row = await watcher.store.one("SELECT payload FROM bili_credentials")
    assert b"private-cookie" not in row[0]
    await watcher.update_credentials(CredentialUpdate(buvid3="updated-device"))
    assert watcher.creds["sessdata"] == "private-cookie-value"
    again = BiliWatcher(watcher.svc, watcher.http, sleep=watcher.sleep)
    await again.initialize()
    assert again.creds["buvid3"] == "updated-device"
    await watcher.update_credentials(CredentialUpdate(clear=True))
    assert not any((await watcher.status())["credentials"].values())


async def test_nav_has_cookie_only_to_api_and_validates_login(watcher):
    assert (await watcher.nav())["account_mask"] == "…3456"
    request = watcher.calls[0]
    assert request.url.host == "api.bilibili.com"
    assert "SESSDATA=private-cookie-value" in request.headers["cookie"]
    assert "private-cookie" not in json.dumps(await watcher.status())


@pytest.mark.parametrize(
    "body",
    [
        {"code": -101},
        {"code": -352},
        {"code": -412},
        {"code": 0, "data": {"v_voucher": "private-challenge"}},
    ],
)
async def test_risk_stops_immediately_persists_and_alerts_without_llm(watcher, body):
    watcher.http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body))
    )
    try:
        with pytest.raises(Rejected):
            await watcher.nav()
        assert watcher.blocked_until > time.time() + 43000
        assert len(watcher.alerts) == 1 and not watcher.svc.ai.calls
        with pytest.raises(Rejected, match="cooling_down"):
            await watcher.nav()
        assert len(watcher.alerts) == 1
        await watcher.update_credentials(CredentialUpdate(sessdata="fresh-cookie"))
        assert (await watcher.status())["cooldown_until"] > time.time()
    finally:
        await watcher.http.aclose()


async def test_text_round_dedups_no_media_or_cookies_to_subtitle_and_defaults_readonly(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True))
    result = await watcher.run_once(slot="13:00")
    assert result["status"] == "done" and result["learned"] == 1
    assert [c[0] for c in watcher.svc.ai.calls] == ["screen", "generate", "screen", "generate"]
    assert all(r.method == "GET" for r in watcher.calls)
    subtitle = next(r for r in watcher.calls if r.url.host == "aisubtitle.hdslb.com")
    assert not subtitle.headers["cookie"]
    assert all(0 <= n <= 6 for n in watcher.naps) and len(watcher.naps) == len(watcher.calls) - 1
    assert watcher.svc.ai.calls[-1][1]["material_scope"] == "partial_subtitle"
    before = len(watcher.svc.ai.calls)
    assert (await watcher.run_once(slot="20:30"))["status"] == "empty"
    assert len(watcher.svc.ai.calls) == before
    assert (await watcher.run_once(slot="life-third"))["status"] == "empty"
    assert (await watcher.run_once(slot="life-third"))["status"] == "already_attempted"
    assert len(watcher.svc.ai.calls) == before


async def test_reading_absent_subtitle_is_honest(watcher):
    watcher.responses["/x/player/wbi/v2"] = {"subtitle": {"subtitles": []}}
    await watcher.nav()
    _, data = await watcher.material(BV)
    assert not data["subtitle_excerpt"] and data["material_scope"] == "metadata_only"
    assert not any("/bfs/" in r.url.path for r in watcher.calls)


async def test_subtitle_host_is_not_model_or_remote_redirect_controlled(watcher):
    watcher.responses["/x/player/wbi/v2"]["subtitle"]["subtitles"][0]["subtitle_url"] = (
        "https://localhost/bfs/subtitle/secret"
    )
    await watcher.nav()
    with pytest.raises(Rejected, match="bili_subtitle_host_forbidden"):
        await watcher.material(BV)
    assert all(r.url.host == "api.bilibili.com" for r in watcher.calls)


async def test_risk_during_material_does_not_continue_or_swallow(watcher):
    original = watcher.http._transport

    async def handle(req):
        if req.url.path == "/x/v2/reply":
            return httpx.Response(200, json={"code": -412})
        return await original.handle_async_request(req)

    watcher.http._transport = httpx.MockTransport(handle)
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    result = await watcher.run_once()
    assert result["status"] == "failed" and result["reason"] == "bili_account_risk"
    assert len(watcher.svc.ai.calls) == 2  # No learning generation after risk response.
    assert all(r.method == "GET" for r in watcher.calls)


async def test_writes_only_opt_in_csrf_and_shared_daily_cap(watcher):
    await watcher.set_policy(
        BiliPolicy(
            enabled=True,
            allow_like=True,
            allow_favorite=True,
            favorite_folder_id="888",
            daily_action_limit=1,
        )
    )
    result = await watcher.run_once()
    assert result["status"] == "done"
    writes = [r for r in watcher.calls if r.method == "POST"]
    assert len(writes) == 1 and writes[0].url.path == "/x/web-interface/archive/like"
    assert b"csrf=" + b"c" * 32 in writes[0].content
    assert all(
        not any(word in r.url.path for word in ("playurl", "heartbeat", "comment/add", "follow"))
        for r in watcher.calls
    )


async def test_favorites_reject_unowned_folder_before_mutation(watcher):
    await watcher.set_policy(
        BiliPolicy(enabled=True, allow_favorite=True, favorite_folder_id="999")
    )
    result = await watcher.run_once()
    assert result["reason"] == "bili_favorite_folder_not_owned"
    assert not any(r.method == "POST" for r in watcher.calls)


async def test_source_screen_stops_paid_generator_and_writes(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    watcher.svc.ai.block_purpose = "source"
    assert (await watcher.run_once())["status"] == "failed"
    assert [x[0] for x in watcher.svc.ai.calls] == ["screen"]
    assert not any(r.method == "POST" for r in watcher.calls)


async def test_arbitrary_endpoints_are_unreachable(watcher):
    with pytest.raises(Rejected, match="bili_endpoint_forbidden"):
        await watcher._request("/x/v2/reply/add", post={"message": "bad"})
    with pytest.raises(Rejected, match="bili_endpoint_forbidden"):
        await watcher._request("https://evil.invalid/")
    assert not watcher.calls


async def test_disable_during_decisions_prevents_generation(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True))
    original = watcher.svc.ai.screen

    async def screen(*args, **kwargs):
        result = await original(*args, **kwargs)
        await watcher.set_policy(BiliPolicy(enabled=False))
        return result

    watcher.svc.ai.screen = screen
    assert (await watcher.run_once())["reason"] == "bili_settings_changed"
    assert [c[0] for c in watcher.svc.ai.calls] == ["screen"]


async def test_credential_rotation_during_nav_does_not_restore_old_identity(watcher):
    original = watcher._request

    async def request(*args, **kwargs):
        result = await original(*args, **kwargs)
        await watcher.update_credentials(CredentialUpdate(sessdata="replacement-session"))
        return result

    watcher._request = request
    with pytest.raises(Rejected, match="bili_settings_changed"):
        await watcher.nav()
    assert watcher.account is None and watcher.keys is None
    assert watcher.s.bili_sessdata.get_secret_value() == "replacement-session"
    assert "replacement-session" in watcher.svc.secrets


async def test_already_liked_video_does_not_consume_mutation_quota(watcher):
    watcher.responses["/x/web-interface/archive/relation"] = {"like": True}
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    assert (await watcher.run_once())["status"] == "done"
    assert not any(r.method == "POST" for r in watcher.calls)
    assert (await watcher.status())["actions_today"] == 0


async def test_low_level_subtitle_fetch_still_guards_domain(watcher):
    with pytest.raises(Rejected, match="bili_subtitle_host_forbidden"):
        await watcher._request("https://127.0.0.1/bfs/private", subtitle=True)
    assert not watcher.calls


@pytest.mark.parametrize("source_stage", [1, 2])
async def test_concurrent_nav_risk_stops_next_paid_generation(watcher, source_stage):
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    entered, release = asyncio.Event(), asyncio.Event()
    original_screen = watcher.svc.ai.screen
    original_transport = watcher.http._transport
    screens = 0

    async def screen(*args, **kwargs):
        nonlocal screens
        result = await original_screen(*args, **kwargs)
        screens += 1
        if screens == source_stage:
            entered.set()
            await release.wait()
        return result

    async def handler(request):
        if request.url.path == "/x/web-interface/nav" and entered.is_set():
            return httpx.Response(200, json={"code": -412})
        return await original_transport.handle_async_request(request)

    watcher.svc.ai.screen = screen
    watcher.http._transport = httpx.MockTransport(handler)
    task = asyncio.create_task(watcher.run_once())
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        with pytest.raises(Rejected, match="bili_account_risk"):
            await watcher.nav()
        release.set()
        result = await asyncio.wait_for(task, timeout=2)
        assert result["reason"] == "bili_account_cooling_down"
        assert sum(c[0] == "generate" for c in watcher.svc.ai.calls) == source_stage - 1
        assert len(watcher.alerts) == 1
        assert not any(r.method == "POST" for r in watcher.calls)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("change", ["disable", "cooldown"])
async def test_jitter_rechecks_policy_and_cooldown_before_network(watcher, change):
    await watcher.set_policy(BiliPolicy(enabled=True))

    async def sleep(_):
        if change == "disable":
            await watcher.set_policy(BiliPolicy(enabled=False))
        else:
            watcher.blocked_until = watcher.clock() + 43200

    watcher.sleep = sleep
    result = await watcher.run_once()
    assert result["reason"] == (
        "bili_settings_changed" if change == "disable" else "bili_account_cooling_down"
    )
    assert len(watcher.calls) == 1  # Initial nav only; no feed request after jitter.
    assert not watcher.svc.ai.calls


async def test_credential_rotation_aborts_old_round_before_next_http_request(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True))
    original_request = watcher._request

    async def request(path, *args, **kwargs):
        result = await original_request(path, *args, **kwargs)
        if path.endswith("/rcmd"):
            await watcher.update_credentials(CredentialUpdate(sessdata="replacement-session"))
        return result

    watcher._request = request
    result = await watcher.run_once()
    assert result["reason"] == "bili_settings_changed"
    assert len(watcher.calls) == 2  # No dynamic request using replacement credentials.
    assert not watcher.svc.ai.calls


async def test_disable_then_enable_invalidates_old_round_but_manual_nav_remains_available(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True))
    original_screen = watcher.svc.ai.screen

    async def screen(*args, **kwargs):
        result = await original_screen(*args, **kwargs)
        await watcher.set_policy(BiliPolicy(enabled=False))
        await watcher.set_policy(BiliPolicy(enabled=True))
        return result

    watcher.svc.ai.screen = screen
    assert (await watcher.run_once())["reason"] == "bili_settings_changed"
    assert [c[0] for c in watcher.svc.ai.calls] == ["screen"]
    await watcher.set_policy(BiliPolicy(enabled=False))
    assert (await watcher.nav())["status"] == "valid"


async def test_manual_patrol_with_no_patrol_budget_skips_network_and_model(watcher):
    await watcher.set_policy(BiliPolicy(enabled=True))
    await watcher.store.set_budget_config(BudgetConfig(patrol_daily_usd=0))
    watcher.calls.clear()
    with pytest.raises(Rejected, match="budget_exhausted"):
        await watcher.run_once()
    assert not watcher.calls and not watcher.svc.ai.calls


@pytest.mark.parametrize(
    "sender,reference,expected_importance",
    [
        ("20000", "recent_chat_topics:0", "high"),
        ("30000", "recent_chat_topics:0", "normal"),
        ("30000", "owner:20000", "normal"),
    ],
)
async def test_private_patrol_recommendation_prefers_real_source(
    watcher, message, sender, reference, expected_importance
):
    source = message.model_copy(update={"user_id": sender, "conversation_id": sender})
    other_id = "30000" if sender == "20000" else "20000"
    other_dm = message.model_copy(update={"user_id": other_id, "conversation_id": other_id})
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    sessions = watcher.svc.sessions = SessionManager(watcher.s, watcher.store)
    await sessions.remember(source, "给我找个科学视频", "可以")
    events = []

    async def publish(module, kind, payload, *, importance, business_key):
        events.append(
            MilestoneEvent(
                source_module=module, event_type=kind, importance=importance, payload=payload
            )
        )

    class Picks(Gateway):
        async def generate(self, payload, subject, **kwargs):
            if kwargs["scenario"] == Scenario.BILI_SELECTION:
                assert payload["memory_preferences"]["private_topic_refs"] == [
                    "recent_chat_topics:0"
                ]
                return json.dumps(
                    {
                        "selected": [
                            {
                                "bvid": BV,
                                "private_topic_ref": reference,
                                "share_owner": sender != "20000",
                            }
                        ]
                    }
                )
            return await super().generate(payload, subject, **kwargs)

    watcher.svc.world_state = SimpleNamespace(publish_event=publish)
    watcher.svc.ai = Picks()
    await watcher.set_policy(BiliPolicy(enabled=True))
    assert (await watcher.run_once())["learned"] == 1
    assert [e.importance for e in events] == [expected_importance]
    reaction = milestone_sharing(watcher.s).match(events[0])
    assert (reaction.recipient if reaction else None) == (
        "owner:20000" if expected_importance == "high" else None
    )
    bound = True  # private selection always stays bound to the authenticated source
    assert not events[0].payload.get("private_topic_ref")
    assert bool(events[0].payload.get("private_first")) == bound
    assert await watcher.lookup_watched(BV, message=source)
    assert await watcher.lookup_watched(BV, message=other_dm) is None
    assert await watcher.lookup_watched(BV, message=group) is None
    assert await watcher.lookup_watched(BV) is None
    assert "summary" not in events[0].payload
    assert "title" not in events[0].payload
    if sender != "20000":
        assert not await watcher.store.latest_reports(
            watcher.store.subject(watcher.svc.proactive.target())
        )
    assert bool(await watcher.recent_surfed(message=other_dm)) != bound
    assert bool(await watcher.recent_surfed(message=group)) != bound
    assert bool(await watcher.recent_surfed()) != bound
    assert [r["bvid"] for r in await watcher.recent_surfed(message=source)] == [BV]
    assert [r["bvid"] for r in await watcher.recent_surfed(now=watcher.clock() + 86401)] == [BV]
    if bound:
        row = await watcher.store.one("SELECT session_scope FROM bili_private_share_priority")
        assert row[0] == watcher.store.session_scope(source)
        now = watcher.clock()
        await watcher.store.db.execute(
            "INSERT INTO bili_watched_history VALUES (?, 'other', 'done', ?, ?)",
            (OTHER, now + 1, watcher.store.vault.seal("bili-learn:" + OTHER, "普通视频")),
        )
        assert (await watcher.recent_surfed(message=source, limit=1))[0]["bvid"] == BV
        assert (await watcher.recent_surfed(message=group, limit=1))[0]["bvid"] == OTHER
