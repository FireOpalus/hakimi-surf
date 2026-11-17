import json
import random
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import httpx
import pytest

from hakimi.adapters import Sender
from hakimi.hot_ring import HotRing
from hakimi.longnick import Longnick, LongnickConfig
from hakimi.owner_cloud_memory import OwnerCloudMemory
from hakimi.proactive_dm import OwnerProactive
from hakimi.roaming_logger import RoamingLogger
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway


def _stamp(value: str) -> float:
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


def _group_msg(message, **updates):
    return message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "user_id": "30000",
            "addressed": False,
            **updates,
        }
    )


def test_hot_ring_recent_group_activity_filters_bots_recalls_and_window(message):
    ring = HotRing()
    now = 1_700_000_000.0
    assert ring.recent_group_activity(None, now=now) == {
        "count": 0,
        "unique_users": 0,
        "user_ids": [],
        "messages": [],
    }
    probe = _group_msg(message, event_id="probe", created_at=now)
    assert ring.recent_group_activity(probe, now=now)["count"] == 0

    # Valid messages inside 5m window (300s)
    m1 = _group_msg(message, event_id="e1", user_id="30001", created_at=now - 299)
    m2 = _group_msg(message, event_id="e2", user_id="30002", created_at=now - 120)
    m3 = _group_msg(message, event_id="e3", user_id="30001", created_at=now - 10)
    # Excluded: outside window, zero timestamp, self-bot, external bot, recalled, other group
    old = _group_msg(message, event_id="old", user_id="30003", created_at=now - 301)
    zero_ts = _group_msg(message, event_id="zero", user_id="30004", created_at=0)
    self_msg = _group_msg(message, event_id="self", user_id="10000", created_at=now - 50)
    other_bot = _group_msg(message, event_id="bot", user_id="60000", created_at=now - 40)
    recalled_msg = _group_msg(message, event_id="rec", user_id="30005", created_at=now - 20)
    other_group = _group_msg(
        message, event_id="other-g", conversation_id="50000", user_id="30006", created_at=now - 5
    )

    for item in (old, zero_ts, m1, self_msg, other_bot, m2, recalled_msg, m3, other_group):
        ring.observe(item)
    ring.recall(probe, "rec")

    res = ring.recent_group_activity(
        probe, window_seconds=300, now=now, bot_ids={"60000"}
    )
    assert res["count"] == 3
    assert res["unique_users"] == 2
    assert res["user_ids"] == ["30001", "30002"]
    assert [m.event_id for m in res["messages"]] == ["e1", "e2", "e3"]

    wider = ring.recent_group_activity(probe, window_seconds=1800, now=now, bot_ids={"60000"})
    assert wider["count"] == 4
    assert [m.event_id for m in wider["messages"]] == ["old", "e1", "e2", "e3"]


async def test_gateway_5m_liveliness_cooldown_and_affection_fusion(settings, store, message):
    settings.owner_qq_id = "20000"
    settings.bot_qq_ids = {"60000"}
    filters = store.runtime.filters
    aff = store.runtime.affection
    now = 1_700_000_000.0

    # 1. Backward compatibility when hot_ring is None
    probe = _group_msg(message, event_id="probe", created_at=now)
    assert filters.hot_ring is None
    assert filters.thresholds(probe, now=now)["reply"] == 0.42
    assert filters.thresholds(None)["reply"] == 0.42

    # Attach HotRing with 15 non-bot messages within 5m + bot/recalled noise
    hot = HotRing()
    filters.hot_ring = hot
    for idx in range(14):
        hot.observe(
            _group_msg(
                message,
                event_id=f"u-{idx}",
                user_id=f"3100{idx}",
                created_at=now - 60 + idx,
            )
        )
    hot.observe(_group_msg(message, event_id="bot-noise", user_id="60000", created_at=now - 10))
    hot.observe(_group_msg(message, event_id="rec-noise", user_id="32000", created_at=now - 9))
    hot.recall(probe, "rec-noise")
    # Only 14 valid messages -> no liveliness bonus yet
    assert filters.thresholds(probe, now=now)["reply"] == 0.42

    # 15th valid message unlocks -0.08 liveliness bonus
    hot.observe(_group_msg(message, event_id="u-14", user_id="31014", created_at=now - 5))
    assert filters.thresholds(probe, now=now)["reply"] == 0.34

    # Addressed messages, private messages, and uninvited known bots remain unchanged
    assert filters.thresholds(_group_msg(message, addressed=True), now=now)["reply"] == 0.42
    assert filters.thresholds(message, now=now)["reply"] == 0.42
    assert (
        filters.thresholds(
            _group_msg(message, user_id="60000"), known_bot=True, bot_invited=False, now=now
        )["reply"]
        == settings.bot_reply_threshold
    )

    # 2. Owner and Lv.4+ affection extra bonus (-0.12 total, floored at 0.18)
    assert filters.thresholds(probe, owner=True, now=now)["reply"] == 0.18

    aff.config = aff.config.model_copy(update={"enabled": True})
    await aff.initialize()
    await aff.set_user_score("30000", 75)  # Lv.4 familiar -> base 0.34 -> 0.22
    assert filters.thresholds(probe, now=now)["reply"] == 0.22
    await aff.set_user_score("30000", 95)  # Lv.5 bestie -> base 0.28 -> floored at 0.18
    assert filters.thresholds(probe, now=now)["reply"] == 0.18

    # Lv.5 in "active" preset has base 0.28 - 0.14 = 0.14 (< 0.18); must not raise to 0.18
    await filters.set_group_activity("onebot", "40000", "active")
    assert filters.thresholds(probe, now=now)["reply"] == 0.14
    await filters.set_group_activity("onebot", "40000", None)

    # 3. Hostile / blacklisted speakers (level <= 1) NEVER receive liveliness bonus or reduction
    await aff.set_user_score("30000", 10)  # Lv.1 hostile -> 0.95
    assert filters.thresholds(probe, now=now)["reply"] == 0.95
    await aff.set_user_score("30000", 0)  # Lv.0 blacklisted -> 1.42
    assert filters.thresholds(probe, now=now)["reply"] == 1.42

    # 4. Unified cooldown: note_proactive_spoke & unaddressed spoke() share 1500s cooldown
    await aff.set_user_score("30000", 50)  # Lv.3 normal -> 0.42
    filters.note_proactive_spoke(probe, now=now)
    assert filters.thresholds(probe, now=now + 600)["reply"] == 0.54
    assert filters.thresholds(probe, owner=True, now=now + 600)["reply"] == 0.40
    # After 1500s cooldown expires, fresh 5m liveliness bonus applies again
    for idx in range(15):
        hot.observe(
            _group_msg(
                message,
                event_id=f"u2-{idx}",
                user_id=f"3300{idx}",
                created_at=now + 1450 + idx,
            )
        )
    assert filters.thresholds(probe, now=now + 1500)["reply"] == 0.34

    # spoke(addressed=True) does NOT trigger proactive cooldown; spoke(addressed=False) DOES
    other_group = _group_msg(message, conversation_id="50000", user_id="30000", addressed=True)
    filters.spoke(other_group)
    assert filters._group_scope(other_group) not in filters.last_proactive_spoke
    filters.spoke(_group_msg(message, conversation_id="50000", addressed=False))
    assert filters._group_scope(other_group) in filters.last_proactive_spoke

    # Bounded to 2048 keys
    for idx in range(2060):
        filters.note_proactive_spoke(
            _group_msg(message, conversation_id=str(70000 + idx)), now=now + idx
        )
    assert len(filters.last_proactive_spoke) == 2048


async def test_owner_cloud_memory_activity_and_proactive_synergy(settings, store, tmp_path):
    mem_dir = tmp_path / "antigravity_mem"
    mem_dir.mkdir(parents=True)
    secret = settings.gemini_api_key.get_secret_value()
    (mem_dir / "short_term_memory.json").write_text(
        json.dumps(
            [
                {
                    "id": "m1",
                    "timestamp": "2026-11-17 18:10:00",
                    "tag": "老记录",
                    "dialogue": {"user": "hi", "assistant": "hello"},
                },
                {
                    "id": "m2",
                    "timestamp": "2026-11-17 20:15:30",
                    "tag": f"正事开发_{secret}",
                    "dialogue": {"user": "debug", "assistant": "ok"},
                },
                {
                    "id": "m3",
                    "timestamp": "not-a-timestamp",
                    "tag": "坏数据",
                    "dialogue": {"user": "x", "assistant": "y"},
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    settings.antigravity_memory_dir = mem_dir
    settings.enable_antigravity_memory = False

    loader = OwnerCloudMemory(settings)
    disabled_info = loader.latest_activity_info()
    assert disabled_info == {
        "enabled": False,
        "latest_timestamp": 0.0,
        "latest_iso": "",
        "latest_tag": "",
    }

    settings.enable_antigravity_memory = True
    info = loader.latest_activity_info()
    expected_ts = _stamp("2026-11-17T20:15:30")
    assert info["enabled"] is True
    assert info["latest_timestamp"] == pytest.approx(expected_ts)
    assert info["latest_iso"].startswith("2026-11-17T20:15:30")
    assert secret not in info["latest_tag"]
    assert "正事开发" in info["latest_tag"]

    # OwnerProactive integration
    now_recent = expected_ts + 1800  # 30 minutes after desktop activity (< 90m)
    wire = SimpleNamespace(ready=AsyncMock(), call=AsyncMock(return_value={"message_id": 1}))
    wire.ready.is_set = lambda: True
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, wire))
        svc.cloud_memory = loader
        dm = OwnerProactive(svc, clock=lambda: now_recent, rng=random.Random(42))
        svc.proactive = dm
        await dm.initialize()
        try:
            assert dm._desktop_recent_activity(now_recent) is not None
            assert dm._desktop_recent_activity(expected_ts + 5400) is None
            assert dm._desktop_recent_activity(expected_ts - 10) is None

            recent_payload = await dm._companion_payload(now_recent)
            assert "desktop_recent_activity" in recent_payload
            assert "切勿问怎么半天没出现" in recent_payload["desktop_recent_activity"]
            assert "电脑前忙正事" in recent_payload["topic"]

            stale_payload = await dm._companion_payload(expected_ts + 5500)
            assert "desktop_recent_activity" not in stale_payload
        finally:
            await svc.stop()


async def test_longnick_apply_life_block_enforces_20h_cooldown(settings, store):
    settings.owner_qq_id = "20000"
    ready = SimpleNamespace(is_set=lambda: True)
    wire = SimpleNamespace(call=AsyncMock(return_value={}), ready=ready)
    svc = SimpleNamespace(s=settings, store=store, sender=SimpleNamespace(onebot=wire))
    clock = [1_000_000.0]
    longnick = Longnick(svc, clock=lambda: clock[0])
    await longnick.initialize()

    assert (await longnick.apply_life_block("还在安静发呆喵"))["status"] == "disabled_or_absent"
    await longnick.set_config(LongnickConfig(enabled=True, cooldown_hours=20))

    assert (await longnick.apply_life_block("正在矿洞发呆"))["status"] == "success"
    # Even after 6 hours (which would allow apply_patrol), apply_life_block stays in 20h cooldown
    clock[0] += 6 * 3600
    assert (await longnick.apply_life_block("六小时后不想改"))["status"] == "cooldown"

    # After 20 hours, succeeds again
    clock[0] += 14 * 3600
    assert (await longnick.apply_life_block("第二天的新心情"))["status"] == "success"

    # Invalid input returns safe failed status
    clock[0] += 20 * 3600
    res = await longnick.apply_life_block({"bad": "type"})  # type: ignore[arg-type]
    assert res == {"status": "failed", "error": "invalid_life_block_longnick"}


async def test_roaming_logger_world_event_and_backward_compat_render(settings, store):
    s = settings.model_copy(update={"roaming_enabled": True})
    diary = RoamingLogger(s, store)
    await diary.initialize()
    now = _stamp("2026-11-17T14:20:00")

    # 1. Baseline with 0 world_life rows must NOT emit world_events or world section
    await diary.patrol("p-1", "今天看了一个科普视频。", now=now)
    empty_md = await diary.render("2026-11-17")
    assert "## 🌍 今日生活与跨模块轨迹" not in empty_md
    counts = dict(
        await store.all("SELECT kind,n FROM roaming_counts WHERE day=?", ("2026-11-17",))
    )
    rows = await store.all(
        "SELECT id,payload,event_kind FROM roaming_events WHERE day=? ORDER BY rowid",
        ("2026-11-17",),
    )
    summary_without_world = diary._summary_input("2026-11-17", counts, rows)
    assert "world_events" not in summary_without_world

    # 2. Record world_event with secret, PII, URL, and HTML/Markdown injection
    secret = settings.gemini_api_key.get_secret_value()
    await diary.world_event(
        "we-1",
        "minecraft",
        "milestone",
        f"挖到钻石啦 <script> @boiser 13800138000 user@test.com https://evil.test {secret}",
        now=now,
    )
    # Deduplicated on repeat event_id
    await diary.world_event("we-1", "minecraft", "milestone", "重复记录", now=now)

    rendered = await diary.render("2026-11-17")
    assert "## 🌍 今日生活与跨模块轨迹" in rendered
    assert "14:20 · 模块 minecraft/milestone" in rendered
    assert "挖到钻石啦" in rendered
    assert "今天暂无白名单聊天记录" in rendered
    for leaked in (secret, "13800138000", "user@test.com", "evil.test", "<script>"):
        assert leaked not in rendered

    counts2 = dict(
        await store.all("SELECT kind,n FROM roaming_counts WHERE day=?", ("2026-11-17",))
    )
    rows2 = await store.all(
        "SELECT id,payload,event_kind FROM roaming_events WHERE day=? ORDER BY rowid",
        ("2026-11-17",),
    )
    assert counts2["world_event"] == 1
    summary_with_world = diary._summary_input("2026-11-17", counts2, rows2)
    assert len(summary_with_world["world_events"]) == 1
    assert "minecraft/milestone" in summary_with_world["world_events"][0]
    assert summary_with_world["chat_excerpts"] == []
