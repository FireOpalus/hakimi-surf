"""Patrol history counts, retry boundaries and crash recovery; all providers are fakes."""

import asyncio
from types import SimpleNamespace

import pytest

from hakimi.bili_history import MAX_READ_ATTEMPTS, READ_RETRY_SECONDS
from hakimi.bili_watcher import BiliPolicy, BiliWatcher
from hakimi.modules.bilibili_surf import BilibiliSurfModule
from hakimi.security import Rejected
from hakimi.work_usage import reservation_work
from tests.test_bili_watcher import BV, OTHER
from tests.test_bili_watcher import watcher as watcher


async def test_upgrade_counts_only_confirmed_and_queries_only_current_bvs(watcher):
    store = watcher.store
    rows = [
        (
            f"BV1xx411c7{x:02d}",
            f"old{x}",
            "done",
            1,
            store.vault.seal("bili-learn:" + f"BV1xx411c7{x:02d}", "公开学习结果"),
        )
        for x in range(12)
    ]
    rows.append((BV, "legacy", "attempted", 2, None))
    await store.db.executemany("INSERT INTO bili_watched_history VALUES (?,?,?,?,?)", rows)
    await watcher.initialize()
    await watcher.initialize()
    status = await watcher.status()
    assert status["watched_count"] == 12
    assert status["watch_history_counts"] == {
        "done": 12,
        "processing": 0,
        "retryable": 0,
        "uncertain": 1,
        "exhausted": 0,
    }
    assert (await store.one("SELECT status FROM bili_watched_history WHERE bvid=?", (BV,)))[0] == (
        "uncertain"
    )
    await watcher.nav()
    watcher.responses["/x/web-interface/wbi/index/top/feed/rcmd"]["item"].append(
        {"goto": "av", "bvid": OTHER, "title": "another", "owner": {}}
    )
    statements = []
    await store.db.set_trace_callback(statements.append)
    try:
        assert [c["bvid"] for c in await watcher.candidates()] == [OTHER]
    finally:
        await store.db.set_trace_callback(None)
    history_queries = [q for q in statements if "FROM bili_watched_history" in q]
    assert len(history_queries) == 1
    assert (
        "h.bvid IN (" in history_queries[0]
        and BV in history_queries[0]
        and OTHER in history_queries[0]
    )


async def test_interrupted_subtitle_read_returns_after_cooldown(watcher, monkeypatch):
    now = [watcher.clock()]
    watcher.clock = lambda: now[0]
    await watcher.set_policy(BiliPolicy(enabled=True))
    request = watcher._request
    failed = False

    async def outage(path, *args, **kwargs):
        nonlocal failed
        if kwargs.get("subtitle") and not failed:
            failed = True
            raise Rejected("bili_account_unavailable")
        return await request(path, *args, **kwargs)

    monkeypatch.setattr(watcher, "_request", outage)
    assert (await watcher.run_once())["reason"] == "bili_account_unavailable"
    status = await watcher.status()
    assert status["watch_history_counts"]["retryable"] == 1 and status["watched_count"] == 0
    assert len(watcher.svc.ai.calls) == 2  # only candidate selection
    now[0] += READ_RETRY_SECONDS - 1
    assert (await watcher.run_once())["status"] == "empty"
    assert len(watcher.svc.ai.calls) == 2
    now[0] += 1
    assert (await watcher.run_once())["learned"] == 1
    assert (await watcher.status())["watched_count"] == 1
    assert (await watcher.run_once())["status"] == "empty"
    assert (await watcher.store.one("SELECT attempts FROM bili_watch_progress"))[0] == 2


async def test_paid_handoff_timeout_never_reselects_even_after_restart(watcher):
    calls = []

    async def timeout(*args, **kwargs):
        calls.append(reservation_work("test", "test"))
        status = await watcher.status()
        assert status["watch_history_counts"]["processing"] == 1
        raise TimeoutError

    watcher.svc.video_watch = SimpleNamespace(analyze=timeout)
    await watcher.set_policy(BiliPolicy(enabled=True))
    module = BilibiliSurfModule(watcher.s, watcher=watcher)
    outcome = await module.patrol(SimpleNamespace(ticket={"id": "original"}))
    assert outcome.status == "uncertain"
    assert len(calls) == 1
    assert calls[0].kind == "bili.video_study" and calls[0].root_kind == "bili.account_patrol"
    run = await watcher.store.one("SELECT run_id FROM bili_watched_history WHERE bvid=?", (BV,))
    assert run[0] == calls[0].root_key
    assert (await watcher.status())["watch_history_counts"]["uncertain"] == 1
    restarted = BiliWatcher(watcher.svc, watcher.http, sleep=watcher.sleep)
    await restarted.initialize()
    assert (await restarted.run_once())["status"] == "empty"
    assert len(calls) == 1
    assert (await restarted.status())["watched_count"] == 0


async def test_busy_bv_claim_and_bounded_restart_recovery(watcher):
    now = watcher.clock()
    h = watcher.history
    for attempt in range(MAX_READ_ATTEMPTS):
        r1, r2 = await asyncio.gather(h.claim(BV, "a", now), h.claim(BV, "b", now))
        assert r1 != r2
        assert not await h.start_analysis(BV, "someone-else")
        if attempt == 0:
            assert await h.claim(OTHER, "paid", now)
            assert await h.start_analysis(OTHER, "paid")
        # Simulate loss of the process in both stages before it could mark a failure.
        watcher.clock = lambda timestamp=now: timestamp
        await watcher.initialize()
        assert not await h.start_analysis(BV, "a" if r1 else "b")
        assert await h.blocked_candidates([BV, OTHER], now) == {BV, OTHER}
        now += READ_RETRY_SECONDS
    assert not await h.claim(BV, "last", now)
    assert await h.blocked_candidates([BV, OTHER], now) == {BV, OTHER}
    counts = (await watcher.status())["watch_history_counts"]
    assert counts["retryable"] == counts["exhausted"] == counts["uncertain"] == 1
    assert counts["done"] == counts["processing"] == 0


async def test_risk_in_public_read_keeps_long_cooldown_and_alert(watcher, monkeypatch):
    now = watcher.clock()
    watcher.clock = lambda: now
    await watcher.set_policy(BiliPolicy(enabled=True))
    request = watcher._request

    async def risk(path, *args, **kwargs):
        if kwargs.get("subtitle"):
            await watcher._risk("bili_account_risk")
        return await request(path, *args, **kwargs)

    monkeypatch.setattr(watcher, "_request", risk)
    assert (await watcher.run_once())["reason"] == "bili_account_risk"
    assert (await watcher.status())["watch_history_counts"]["retryable"] == 1
    assert len(watcher.alerts) == 1
    assert watcher.blocked_until == now + 12 * 3600
    assert await watcher.history.blocked_candidates([BV], now + READ_RETRY_SECONDS) == {BV}
    with pytest.raises(Rejected, match="bili_account_cooling_down"):
        await watcher.run_once()
    assert len(watcher.svc.ai.calls) == 2


async def test_uncertain_like_keeps_finished_learning_and_never_replays(watcher, monkeypatch):
    await watcher.set_policy(BiliPolicy(enabled=True, allow_like=True))
    request = watcher._request
    sent = []

    async def lost_receipt(path, *args, **kwargs):
        if kwargs.get("post") is not None:
            sent.append(path)
            raise TimeoutError
        return await request(path, *args, **kwargs)

    monkeypatch.setattr(watcher, "_request", lost_receipt)
    result = await watcher.run_once()
    assert result["learned"] == 1 and result["requires_review"]
    status = await watcher.status()
    assert status["watch_history_counts"]["done"] == status["uncertain_actions"] == 1
    assert status["watch_history_counts"]["uncertain"] == 0
    assert (await watcher.run_once())["status"] == "empty"
    assert sent == ["/x/web-interface/archive/like"]


@pytest.mark.parametrize(("stage", "expected"), [("read", "retryable"), ("model", "uncertain")])
async def test_cancellation_recovers_the_correct_stage(watcher, monkeypatch, stage, expected):
    entered = asyncio.Event()

    async def pause(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    if stage == "read":
        monkeypatch.setattr(watcher, "material", pause)
    else:
        watcher.svc.video_watch = SimpleNamespace(analyze=pause)
    await watcher.set_policy(BiliPolicy(enabled=True))
    task = asyncio.create_task(watcher.run_once())
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    status = await watcher.status()
    assert status["watch_history_counts"][expected] == 1
    assert status["recent_runs"][0]["status"] == "abandoned"
    assert status["watched_count"] == 0


async def test_budget_stop_after_read_leaves_bv_retryable(watcher, monkeypatch):
    await watcher.set_policy(BiliPolicy(enabled=True))
    material = watcher.material

    async def exhaust_after_read(bvid):
        result = await material(bvid)
        await watcher.store.db.execute("INSERT INTO meta VALUES ('budget_overrun','1')")
        return result

    monkeypatch.setattr(watcher, "material", exhaust_after_read)
    assert (await watcher.run_once())["reason"] == "budget_hard_stop"
    assert len(watcher.svc.ai.calls) == 2
    status = await watcher.status()
    assert status["watch_history_counts"]["retryable"] == 1 and status["watched_count"] == 0


async def test_saved_learning_and_later_risk_count_as_an_executed_world_patrol(watcher):
    async def hit_risk_after_learning(*args):
        await watcher._risk("bili_account_risk")

    watcher._interaction = hit_risk_after_learning
    await watcher.set_policy(BiliPolicy(enabled=True))
    module = BilibiliSurfModule(watcher.s, watcher=watcher)
    result = await module.patrol(SimpleNamespace(ticket={"id": "learning-confirmed"}))
    assert result.status == "done" and result.reason == "bili_account_risk"
    assert (await watcher.status())["watched_count"] == 1
