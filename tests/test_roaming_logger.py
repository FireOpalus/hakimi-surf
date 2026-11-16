from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from hakimi.roaming_logger import RoamingLogger, redact_public_summary
from hakimi.security import Rejected
from tests.fakes import FakeGateway


def timestamp(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


class FakeDrive:
    def __init__(self):
        self.calls = []
        self.fail = False

    async def sync_day(self, day, text):
        self.calls.append((day, text))
        if self.fail:
            raise OSError
        return "diary-file-123456"


@pytest.fixture
async def diary(settings, store):
    s = settings.model_copy(update={"roaming_enabled": True, "roaming_group_ids": {"onebot:40000"}})
    instance = RoamingLogger(s, store, FakeDrive())
    await instance.initialize()
    return instance


async def test_diary_generation_dedup_redaction_and_private_exclusion(diary, message):
    now = timestamp("2026-11-14T10:00:00")
    await diary.patrol(
        "patrol-1", "公开简介介绍了科学实验。联系 name@example.com 或 13800138000", now=now
    )
    await diary.injection("blocked-1", now=now)
    await diary.injection("blocked-1", now=now)
    await diary.group("private-1", message.model_copy(update={"text": "主人私聊内容"}), now=now)
    group = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "text": "群友闲聊日常"}
    )
    await diary.group("group-1", group, now=now)
    await diary.bot_reply("r1", group, "哈基米也觉得有趣", now=now)
    text = await diary.render("2026-11-14")
    assert "公开简介介绍了科学实验" in text
    assert "提示词注入试探：1 次" in text
    assert "日常话题：1 次" in text
    assert "私聊 · BOISER" in text and "主人私聊内容" in text
    assert "群聊 40000" in text and "群友闲聊日常" in text
    assert "哈基米:哈基米也觉得有趣" in text
    for secret in (
        "name@example.com",
        "13800138000",
        diary.s.gemini_api_key.get_secret_value(),
    ):
        assert secret not in text


async def test_groups_must_opt_in(diary, message):
    unlisted_group = message.model_copy(update={"kind": "group", "conversation_id": "60000"})
    stranger_dm = message.model_copy(
        update={"user_id": "99999", "conversation_id": "99999", "text": "陌生人私聊"}
    )
    await diary.group("group-1", unlisted_group, now=timestamp("2026-11-14T10:00:00"))
    await diary.group("private-stranger", stranger_dm, now=timestamp("2026-11-14T10:00:00"))
    assert "今天暂无白名单聊天记录" in await diary.render("2026-11-14")


async def test_two_hour_slots_2355_flush_and_restart(diary):
    first = timestamp("2026-11-14T20:00:00")
    await diary.patrol("p1", "first summary", now=first)
    await diary.sync_due(now=first)
    await diary.patrol("p2", "second summary", now=first + 60)
    await diary.sync_due(now=first + 3600)
    assert len(diary.drive.calls) == 1
    await diary.sync_due(now=first + 7200)
    assert len(diary.drive.calls) == 2
    late = timestamp("2026-11-14T23:50:00")
    await diary.injection("i1", now=late)
    await diary.sync_due(now=late)
    assert len(diary.drive.calls) == 2
    recovered = RoamingLogger(diary.s, diary.store, diary.drive)
    await recovered.sync_due(now=timestamp("2026-11-14T23:55:00"))
    assert len(diary.drive.calls) == 3
    await recovered.sync_due(now=timestamp("2026-11-14T23:56:00"))
    assert len(diary.drive.calls) == 3


async def test_late_events_get_previous_day_updated_at_midnight(diary):
    now = timestamp("2026-11-14T23:55:00")
    await diary.sync_due(now=now)
    await diary.injection("late", now=now + 120)
    await diary.sync_due(now=timestamp("2026-11-15T00:00:00"))
    previous = [text for day, text in diary.drive.calls if day == "2026-11-14"]
    assert len(previous) == 2 and "提示词注入试探：1 次" in previous[-1]


async def test_failed_sync_keeps_events_and_retries_with_backoff(diary):
    now = timestamp("2026-11-14T10:00:00")
    diary.drive.fail = True
    await diary.injection("i1", now=now)
    await diary.sync_due(now=now)
    await diary.sync_due(now=now + 1)
    assert len(diary.drive.calls) == 1
    diary.drive.fail = False
    await diary.sync_due(now=now + 61)
    assert len(diary.drive.calls) == 2
    assert "提示词注入试探：1 次" in diary.drive.calls[-1][1]


def test_diary_escapes_external_markdown_html_and_mentions():
    result = redact_public_summary("<img src=x> ![click](https://evil.test) @群友 ```")
    assert "<img" not in result and "![" not in result and "https://" not in result
    assert "@群友" not in result and "```" not in result


async def test_nightly_summary_is_screened_private_free_and_cached_across_drive_retry(
    diary, message
):
    import json

    diary.gateway = FakeGateway()
    morning = timestamp("2026-11-14T10:00:00")
    await diary.patrol("public", "公开视频介绍科学实验", now=morning)
    await diary.group(
        "private",
        message.model_copy(
            update={"user_id": "99999", "conversation_id": "99999", "text": "never-send-unlisted"}
        ),
        now=morning,
    )
    await diary.store.set_memory(diary.store.scope(message), True)
    await diary.store.remember(
        diary.store.scope(message), "never-send-legacy-memory", "owner history"
    )
    await diary.sync_due(now=morning)
    assert diary.gateway.calls == []
    diary.drive.fail = True
    night = timestamp("2026-11-14T23:55:00")
    await diary.sync_due(now=night)
    calls = diary.gateway.calls
    assert [c[0] for c in calls] == ["screen", "generate"]
    assert calls[0][3]["purpose"] == "source"
    assert calls[0][1] == calls[1][1]
    assert calls[1][3] == {"scenario": "daily_diary", "budget_lane": "patrol"}
    assert "never-send-unlisted" not in json.dumps(calls, ensure_ascii=False)
    assert "never-send-legacy-memory" not in json.dumps(calls, ensure_ascii=False)
    assert diary.s.gemini_api_key.get_secret_value() not in json.dumps(calls, ensure_ascii=False)
    row = await diary.store.one("SELECT * FROM roaming_summaries WHERE day='2026-11-14'")
    assert row["status"] == "done" and "简短回复".encode() not in row["payload"]
    recovered = RoamingLogger(diary.s, diary.store, diary.drive, gateway=FakeGateway())
    await recovered.initialize()
    diary.drive.fail = False
    await recovered.sync_due(now=night + 61)
    assert recovered.gateway.calls == []
    assert "每日总结" in diary.drive.calls[-1][1]
    assert "23:55" in diary.drive.calls[-1][1]


async def test_nightly_source_block_never_reaches_model_or_retries(diary):
    diary.gateway = FakeGateway()
    diary.gateway.block_purpose = "source"
    now = timestamp("2026-11-14T23:55:00")
    await diary.patrol("p1", "blocked example", now=now)
    await diary.sync_due(now=now)
    assert [c[0] for c in diary.gateway.calls] == ["screen"]
    assert "当日模型总结未完成" in diary.drive.calls[-1][1]
    await diary.sync_due(now=now + 3600)
    assert [c[0] for c in diary.gateway.calls] == ["screen"]


async def test_cancelled_paid_summary_is_not_repeated_after_restart(diary):
    import asyncio

    started = asyncio.Event()

    class SlowGateway(FakeGateway):
        async def generate(self, *args, **kwargs):
            started.set()
            await asyncio.Future()

    diary.gateway = SlowGateway()
    now = timestamp("2026-11-14T23:55:00")
    await diary.injection("count", now=now)
    task = asyncio.create_task(diary.sync_due(now=now))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    recovered = RoamingLogger(diary.s, diary.store, diary.drive, gateway=FakeGateway())
    await recovered.initialize()
    await recovered.sync_due(now=now + 60)
    assert recovered.gateway.calls == []
    assert "当日模型总结未完成" in diary.drive.calls[-1][1]


async def test_nightly_input_is_bounded_and_reports_omissions(diary):
    import json

    diary.gateway = FakeGateway()
    diary.s.max_context_bytes = 2048
    now = timestamp("2026-11-14T23:55:00")
    for n in range(30):
        await diary.patrol(str(n), "公开摘要" * 200, now=now)
    await diary.sync_due(now=now)
    payload = diary.gateway.calls[0][1]
    assert len(json.dumps(payload, ensure_ascii=False).encode()) <= 2048
    assert payload["omitted_public_summaries"] > 0


async def test_empty_day_has_no_model_cost(diary):
    diary.gateway = FakeGateway()
    await diary.sync_due(now=timestamp("2026-11-14T23:55:00"))
    assert diary.gateway.calls == []


async def test_nightly_budget_failure_preserves_reports_without_repeated_calls(diary):
    class NoBudget(FakeGateway):
        async def generate(self, *args, **kwargs):
            await super().generate(*args, **kwargs)
            raise Rejected("budget_exhausted")

    diary.gateway = NoBudget()
    now = timestamp("2026-11-14T23:55:00")
    await diary.patrol("p1", "public-science-report", now=now)
    await diary.sync_due(now=now)
    assert "public-science-report" in diary.drive.calls[-1][1]
    await diary.sync_due(now=now + 3600)
    assert len(diary.gateway.calls) == 2


async def test_daily_prompt_injection_is_counted_without_storing_attack(diary):
    class BlockingGateway(FakeGateway):
        async def screen(self, *args, **kwargs):
            await super().screen(*args, **kwargs)
            raise Rejected("prompt_injection")

    diary.gateway = BlockingGateway()
    now = timestamp("2026-11-14T23:55:00")
    await diary.patrol("p1", "public summary", now=now)
    await diary.sync_due(now=now)
    assert "提示词注入试探：1 次" in diary.drive.calls[-1][1]
    assert [c[0] for c in diary.gateway.calls] == ["screen"]


async def test_daily_summary_rejects_service_secret_in_generated_text(diary):
    class LeakyGateway(FakeGateway):
        async def generate(self, *args, **kwargs):
            return diary.s.gemini_api_key.get_secret_value()

    diary.gateway = LeakyGateway()
    now = timestamp("2026-11-14T23:55:00")
    await diary.injection("count", now=now)
    await diary.sync_due(now=now)
    assert diary.s.gemini_api_key.get_secret_value() not in diary.drive.calls[-1][1]
    row = await diary.store.one(
        "SELECT status,payload FROM roaming_summaries WHERE day='2026-11-14'"
    )
    assert row["status"] == "failed" and row["payload"] is None


async def test_manual_today_flush_ignores_slot_and_backoff_without_repeating_model(diary):
    now = timestamp("2026-11-14T12:00:00")
    diary.gateway = FakeGateway()
    await diary.patrol("old-day", "old day", now=now - 86400)
    await diary.patrol("today", "today summary", now=now)
    diary.drive.fail = True
    await diary.sync_due(now=now, force=True)
    assert [call[0] for call in diary.drive.calls] == ["2026-11-14"]
    row = await diary.store.one("SELECT last_error FROM roaming_sync WHERE day=?", ("2026-11-14",))
    assert row[0] == "drive_sync_failed"
    assert len(diary.gateway.calls) == 2
    diary.drive.fail = False
    await diary.sync_due(now=now + 1, force=True)
    assert len(diary.drive.calls) == 2
    assert len(diary.gateway.calls) == 2
    row = await diary.store.one(
        "SELECT last_error,last_success FROM roaming_sync WHERE day=?", ("2026-11-14",)
    )
    assert row[0] is None and row[1] == now + 1
    await diary.sync_due(now=now + 2, force=True)
    assert len(diary.drive.calls) == 2
