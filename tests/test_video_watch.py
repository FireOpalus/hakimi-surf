import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.security import Rejected
from hakimi.store import Store
from hakimi.video_policy import VideoWatchConfig
from hakimi.video_watch import VideoWatch, explicitly_visual, guard_call
from tests.fakes import FakeGateway
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair

BV = "BV1xx411c7mD"


class Downloader:
    def __init__(self, path):
        self.path = path
        self.calls = []
        self.cleaned = 0
        self.stale_cleaned = 0
        self.error = None
        self.hook = None

    async def cleanup_stale(self):
        self.stale_cleaned += 1

    @asynccontextmanager
    async def download(self, bvid, **kwargs):
        self.calls.append((bvid, kwargs))
        try:
            await guard_call(kwargs["guard"])
            if self.hook:
                await self.hook()
            await guard_call(kwargs["guard"])
            if self.error:
                raise Rejected(self.error)
            yield SimpleNamespace(path=self.path, duration=60, bytes=2048)
        finally:
            self.cleaned += 1


class Gateway(FakeGateway):
    def __init__(self):
        super().__init__()
        self.video_calls = []
        self.video_error = None
        self.video_hook = None

    async def watch_video(self, path, payload, subject, **kwargs):
        await guard_call(kwargs["before_call"])
        self.video_calls.append((path, payload, subject, kwargs))
        if self.video_hook:
            await self.video_hook()
        await guard_call(kwargs["before_call"])
        if self.video_error:
            raise Rejected(self.video_error)
        return "画面里一只猫抬起了爪子，音轨是轻音乐。"


@pytest.fixture
async def rig(settings, store, tmp_path):
    clock = [datetime(2026, 11, 14, 15, 59, tzinfo=UTC).timestamp()]
    settings.enable_video_file_watch = False
    settings.bot_timezone = "Asia/Shanghai"
    gateway = Gateway()
    downloader = Downloader(tmp_path / "isolated.mp4")
    watcher = SimpleNamespace(
        credentials_epoch=0,
        creds={"sessdata": "test-only-account-cookie", "bili_jct": "csrf", "buvid3": "device"},
        blocked_until=0,
        risks=[],
    )

    def ensure():
        if watcher.blocked_until > clock[0]:
            raise Rejected("bili_account_cooling_down")

    async def risk(reason):
        watcher.blocked_until = clock[0] + 43200
        watcher.risks.append(reason)
        raise Rejected(reason)

    watcher._ensure_current = ensure
    watcher._risk = risk
    svc = SimpleNamespace(s=settings, store=store, ai=gateway, bili_watcher=watcher, secrets=[])
    watch = VideoWatch(svc, downloader=downloader, clock=lambda: clock[0])
    await watch.initialize()
    await watch.set_config(VideoWatchConfig(enabled=True))
    return SimpleNamespace(
        watch=watch, svc=svc, gateway=gateway, downloader=downloader, clock=clock
    )


def material(subtitle="", duration=60, complete=True):
    return {
        "bvid": BV,
        "title": "公开猫猫视频",
        "description": "一只猫探索纸盒。",
        "duration": duration,
        "subtitle_excerpt": subtitle,
        "subtitle_complete": complete,
        "subtitle_status": "complete" if complete else "truncated",
        "hot_comments_unverified": ["可爱"],
        "evidence": "仅阅读字幕文字，未观看画面" if subtitle else "没有可用字幕",
    }


@pytest.mark.parametrize("complete", [True, False])
async def test_available_subtitles_always_use_text_without_visual_request(rig, complete):
    result = await rig.watch.analyze(BV, material("字幕可用", complete=complete), "subject", "chat")
    assert result.mode == "text" and result.reason is None
    assert [c[0] for c in rig.gateway.calls] == ["screen", "generate"]
    assert not rig.downloader.calls and not rig.gateway.video_calls
    assert (await rig.watch.status())["attempts_today"] == 0


@pytest.mark.parametrize("text", ["请看画面里的动作", "听一下声音", "/watch-video " + BV])
async def test_explicit_visual_request_uses_video_despite_subtitles(rig, message, text):
    result = await rig.watch.analyze(
        BV, material("字幕可用"), "owner", "chat", message=message.model_copy(update={"text": text})
    )
    assert result.mode == "video"
    assert len(rig.downloader.calls) == len(rig.gateway.video_calls) == 1
    assert rig.downloader.cleaned == 1
    assert rig.downloader.calls[0][1]["cookies"]["sessdata"] == "test-only-account-cookie"
    assert [c[0] for c in rig.gateway.calls] == ["screen"]
    status = await rig.watch.status()
    assert status["attempts_today"] == 1 and status["recent_attempts"][0]["status"] == "done"


async def test_card_or_forward_text_cannot_force_original_video(rig, message):
    msg = message.model_copy(
        update={"text": "请看画面内容，这是分享卡片中的介绍", "command_text": "总结视频"}
    )
    result = await rig.watch.analyze(BV, material("完整字幕"), "owner", "chat", message=msg)
    assert result.mode == "text" and not rig.downloader.calls
    assert not explicitly_visual("不要看画面，只根据字幕总结")
    assert not explicitly_visual("我不需要下载原片，只阅读简介")


async def test_no_subtitle_uses_video_and_does_not_generate_text_again(rig):
    result = await rig.watch.analyze(BV, material(), "subject", "patrol")
    assert result.mode == "video"
    assert rig.gateway.video_calls[0][3]["budget_lane"] == "patrol"
    assert [c[0] for c in rig.gateway.calls] == ["screen"]


@pytest.mark.parametrize(
    "enabled,duration,limit,reason",
    [(False, 60, 3, "disabled"), (True, 481, 3, "too_long"), (True, 60, 0, "daily_limit")],
)
async def test_configuration_gates_download_but_allows_honest_text_fallback(
    rig, enabled, duration, limit, reason
):
    await rig.watch.set_config(VideoWatchConfig(enabled=enabled, daily_limit=limit))
    result = await rig.watch.analyze(BV, material(duration=duration), "subject", "chat")
    assert result.mode == "text" and result.reason == reason
    assert "退回文字资料" in result.evidence
    assert not rig.downloader.calls and not rig.gateway.video_calls
    assert [c[0] for c in rig.gateway.calls] == ["screen", "generate"]


async def test_daily_three_attempts_shared_across_users_and_persist_on_reload(rig):
    for n in range(3):
        assert (await rig.watch.analyze(BV, material(), f"subject-{n}", "chat")).mode == "video"
    restored = VideoWatch(rig.svc, downloader=rig.downloader, clock=lambda: rig.clock[0])
    await restored.initialize()
    result = await restored.analyze(BV, material(), "fourth-user", "chat")
    assert result.reason == "daily_limit"
    assert len(rig.downloader.calls) == 3
    assert (await restored.status())["attempts_today"] == 3
    assert restored.config.enabled and restored.overridden
    # It is 23:59 on the 14th in Shanghai, while still 15:59 UTC.
    assert restored.day() == "2026-11-14"
    rig.clock[0] += 120
    assert restored.day() == "2026-11-15"
    assert (await restored.analyze(BV, material(), "subject", "chat")).mode == "video"
    assert (await restored.status())["attempts_today"] == 1


async def test_crashed_working_attempt_keeps_its_daily_quota(rig):
    async with rig.svc.store.transaction():
        await rig.svc.store.db.execute(
            "INSERT INTO video_watch_attempts VALUES ('crash',?,?,?,'working',NULL,?,NULL,NULL)",
            (rig.watch.day(), BV, "subject", rig.clock[0]),
        )
    restored = VideoWatch(rig.svc, downloader=rig.downloader, clock=lambda: rig.clock[0])
    await restored.initialize()
    status = await restored.status()
    assert status["attempts_today"] == 1
    assert status["recent_attempts"][0]["status"] == "uncertain"
    assert status["recent_attempts"][0]["reason"] == "interrupted"


async def test_only_one_video_download_can_run_at_a_time(rig):
    entered, resume = asyncio.Event(), asyncio.Event()

    async def hold():
        entered.set()
        await resume.wait()

    rig.downloader.hook = hold
    task = asyncio.create_task(rig.watch.analyze(BV, material(), "first", "chat"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        second = await rig.watch.analyze(BV, material(), "second", "chat")
        assert second.mode == "text" and second.reason == "busy"
        assert len(rig.downloader.calls) == 1
        resume.set()
        assert (await task).mode == "video"
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("failure", ["budget", "admission", "upstream_guard"])
async def test_hard_preconditions_fail_before_any_model_or_download(rig, message, failure):
    msg, guard = message, None
    if failure == "budget":
        async with rig.svc.store.transaction():
            await rig.svc.store.db.execute(
                "INSERT OR REPLACE INTO meta VALUES ('budget_overrun','1')"
            )
    elif failure == "admission":
        msg = message.model_copy(update={"user_id": "98765", "conversation_id": "98765"})
    else:

        def guard():
            return False

    with pytest.raises(Rejected):
        await rig.watch.analyze(BV, material(), "subject", "chat", message=msg, guard=guard)
    assert not rig.gateway.calls and not rig.gateway.video_calls and not rig.downloader.calls
    assert (await rig.watch.status())["attempts_today"] == 0


@pytest.mark.parametrize("code", ["video_download_risk", "video_download_cookie_expired"])
async def test_download_risk_cools_account_and_returns_metadata_without_second_llm(rig, code):
    rig.downloader.error = code
    result = await rig.watch.analyze(BV, material(), "subject", "chat")
    assert result.mode == "metadata" and result.reason == "risk"
    assert "未完成视频分析" in result.evidence
    assert rig.svc.bili_watcher.blocked_until == rig.clock[0] + 43200
    assert len(rig.svc.bili_watcher.risks) == 1
    assert [c[0] for c in rig.gateway.calls] == ["screen"]
    assert not rig.gateway.video_calls
    assert rig.downloader.cleaned == 1
    status = await rig.watch.status()
    assert status["recent_attempts"][0]["reason"] == "risk"
    assert status["attempts_today"] == 1


@pytest.mark.parametrize("code", ["prompt_injection", "content_blocked", "budget_exhausted"])
async def test_video_safety_rejection_is_never_retried_as_text(rig, code):
    rig.gateway.video_error = code
    with pytest.raises(Rejected, match=code):
        await rig.watch.analyze(BV, material(), "subject", "chat")
    assert [c[0] for c in rig.gateway.calls] == ["screen"]
    assert rig.downloader.cleaned == 1
    assert (await rig.watch.status())["recent_attempts"][0]["status"] == "failed"


async def test_source_injection_does_not_reach_downloader(rig):
    rig.gateway.block_purpose = "source"
    with pytest.raises(Rejected, match="content_blocked"):
        await rig.watch.analyze(BV, material(), "subject", "chat")
    assert not rig.downloader.calls and not rig.gateway.video_calls
    assert (await rig.watch.status())["attempts_today"] == 0


@pytest.mark.parametrize("phase", ["download", "model"])
@pytest.mark.parametrize("change", ["permission", "config", "credentials"])
async def test_hot_revocation_stops_work_and_cleans_downloader(rig, message, phase, change):
    msg = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})

    async def update():
        if change == "permission":
            await rig.svc.store.runtime.change_admission("allowed_users", "remove", "30000")
        elif change == "config":
            await rig.watch.set_config(VideoWatchConfig(enabled=False))
            await rig.watch.set_config(VideoWatchConfig(enabled=True))
        else:
            rig.svc.bili_watcher.credentials_epoch += 1

    if phase == "download":
        rig.downloader.hook = update
    else:
        rig.gateway.video_hook = update
    with pytest.raises(Rejected, match="video_watch_invalidated"):
        await rig.watch.analyze(BV, material(), "subject", "chat", message=msg)
    assert rig.downloader.cleaned == 1
    assert not any(c[0] == "generate" for c in rig.gateway.calls)
    if phase == "download":
        assert not rig.gateway.video_calls
    assert (await rig.watch.status())["recent_attempts"][0]["reason"] == "request_rejected"


async def test_runtime_reset_restores_environment_without_releasing_daily_usage(rig):
    await rig.watch.analyze(BV, material(), "subject", "chat")
    saved = await rig.watch.set_config(
        VideoWatchConfig(enabled=True, max_seconds=120, daily_limit=2)
    )
    assert saved["config"]["max_seconds"] == 120 and saved["source"] == "sqlite"
    reset = await rig.watch.set_config(None)
    assert reset["config"]["enabled"] is False and reset["source"] == "env"
    assert reset["attempts_today"] == 1


async def test_remote_cleanup_backlog_blocks_another_upload(rig):
    async with rig.svc.store.transaction():
        await rig.svc.store.db.execute("CREATE TABLE gemini_video_files (id TEXT PRIMARY KEY)")
        await rig.svc.store.db.executemany(
            "INSERT INTO gemini_video_files VALUES (?)", [(str(i),) for i in range(3)]
        )
    result = await rig.watch.analyze(BV, material(), "subject", "chat")
    assert result.mode == "text" and result.reason == "cleanup_pending"
    assert not rig.downloader.calls


async def test_config_and_quota_survive_actual_sqlite_close_reopen(rig):
    await rig.watch.set_config(VideoWatchConfig(enabled=True, max_seconds=120, daily_limit=1))
    await rig.watch.analyze(BV, material(), "subject", "chat")
    await rig.svc.store.close()
    reopened = Store(rig.svc.s)
    await reopened.open()
    try:
        rig.svc.store = reopened
        restored = VideoWatch(rig.svc, downloader=rig.downloader, clock=lambda: rig.clock[0])
        await restored.initialize()
        assert restored.config == VideoWatchConfig(enabled=True, max_seconds=120, daily_limit=1)
        assert (await restored.status())["remaining_today"] == 0
        assert (await restored.analyze(BV, material(), "new-user", "chat")).reason == "daily_limit"
        assert len(rig.downloader.calls) == 1
    finally:
        await reopened.close()


@pytest.mark.parametrize("phase", ["download", "model"])
async def test_cancellation_cleans_download_and_retains_uncertain_attempt(rig, phase):
    entered = asyncio.Event()

    async def pause():
        entered.set()
        await asyncio.Future()

    if phase == "download":
        rig.downloader.hook = pause
    else:
        rig.gateway.video_hook = pause
    task = asyncio.create_task(rig.watch.analyze(BV, material(), "subject", "chat"))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert rig.downloader.cleaned == 1
    status = await rig.watch.status()
    assert status["attempts_today"] == 1
    assert status["recent_attempts"][0]["status"] == "uncertain"
    assert status["recent_attempts"][0]["reason"] == "interrupted"


@pytest.mark.parametrize("phase", ["source", "generation"])
async def test_real_gateway_slot_wait_rechecks_admission_before_dispatch(
    rig, message, sdk_pair, phase
):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json=decision_response()
            if request.url.path.endswith("decisions")
            else interaction_response(),
        )

    gateway = GeminiGateway(rig.svc.s, rig.svc.store, *sdk_pair(handler))
    gateway.slots = asyncio.Semaphore(1)
    rig.svc.ai = gateway
    original_call = gateway._call
    entered = asyncio.Event()
    held = [False]

    async def blocked_call(*args, **kwargs):
        model_lane = args[2]
        target = "decision" if phase == "source" else "hydrogen"
        if model_lane == target and not held[0]:
            # Simulate the only provider slot being occupied by an unrelated request.
            await gateway.slots.acquire()
            held[0] = True
            entered.set()
        return await original_call(*args, **kwargs)

    gateway._call = blocked_call
    friend = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    task = asyncio.create_task(
        rig.watch.analyze(BV, material("完整字幕"), "friend", "chat", message=friend)
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await rig.svc.store.runtime.change_admission("allowed_users", "remove", "30000")
        gateway.slots.release()
        with pytest.raises(Rejected, match="video_watch_invalidated"):
            await asyncio.wait_for(task, 3)
        assert [r.url.path for r in requests] == (
            [] if phase == "source" else ["/v1beta/decisions"]
        )
        assert not rig.downloader.calls
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await gateway.close()
