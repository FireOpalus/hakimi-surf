import asyncio
import json
import random
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import ValidationError

from hakimi.adapters import Sender
from hakimi.proactive_dm import TEST_MESSAGE, OwnerProactive
from hakimi.proactive_policy import ProactiveConfig
from hakimi.roaming_logger import RoamingLogger
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.store import Store
from tests.fakes import FakeBili, FakeGateway


def stamp(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


class Wire:
    def __init__(self):
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls = []
        self.fail = False
        self.confirm = True
        self.hook = None

    async def call(self, action, params):
        self.calls.append((action, params))
        if self.hook:
            await self.hook()
        if self.fail:
            raise TimeoutError("private exception detail must never be exposed")
        return {"message_id": len(self.calls)} if self.confirm else {}


@pytest.fixture
async def dm(settings, store):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    clock = [stamp("2026-11-14T12:00:00")]
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, wire))
        module = OwnerProactive(svc, clock=lambda: clock[0], rng=random.Random(123))
        module.time = clock
        svc.proactive = module
        await module.initialize()
        yield module
        await svc.stop()


async def slot(dm, kind="companion", *, due=None):
    now = dm.clock()
    key = "slot-" + kind
    async with dm.store.transaction():
        await dm.store.db.execute(
            "INSERT INTO proactive_schedule VALUES (?,?,?,?,?,?,'planned',NULL)",
            (
                key,
                dm.scope,
                dm.local().date().isoformat(),
                kind,
                due if due is not None else now,
                now + 7200,
            ),
        )
    return key


async def queued(dm):
    return await dm.store.all("SELECT * FROM pending_owner_dms ORDER BY created,rowid")


@pytest.mark.parametrize("target", [30000, 40000, "20000", True, 20000.0])
async def test_lowest_sender_rejects_non_owner_and_non_numeric_types(dm, target):
    with pytest.raises(Rejected):
        await dm.service.sender.send_owner_dm(target, "must not send")
    assert not dm.service.sender.onebot.calls


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "group", "conversation_id": "40000"},
        {"user_id": "30000", "conversation_id": "30000"},
        {
            "platform": "qq",
            "bot_id": "99999",
            "user_id": "user-openid",
            "conversation_id": "user-openid",
        },
    ],
)
async def test_legacy_proactive_sender_cannot_bypass_target_guard(dm, changes):
    dm.s.onebot_owner_ids.add("30000")  # Even a legacy administrator is not OWNER_QQ_ID.
    dm.s.qq_allow_proactive = True
    with pytest.raises(Rejected):
        await dm.service.sender._send_once(dm.target().model_copy(update=changes), "x", True)
    assert not dm.service.sender.onebot.calls


async def test_zero_token_test_appends_only_confirmed_owner_assistant(dm):
    result = await dm.test_dm()
    assert result["status"] == "sent"
    wire = dm.service.sender.onebot
    assert wire.calls[0][0] == "send_private_msg" and wire.calls[0][1]["user_id"] == 20000
    messages = (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
    assert [(x["role"], x["text"]) for x in messages] == [("assistant", TEST_MESSAGE)]
    group = dm.target().model_copy(update={"kind": "group", "conversation_id": "40000"})
    member = dm.target().model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    assert not (await dm.service.sessions.snapshot(group))["recent_messages"]
    assert not (await dm.service.sessions.snapshot(member))["recent_messages"]
    assert not dm.service.ai.calls and not dm.service.compressor.tasks
    assert not await dm.store.all("SELECT * FROM ledger")
    assert (await queued(dm))[0]["payload"] is None


@pytest.mark.parametrize("mode", ["timeout", "missing_ack"])
async def test_ambiguous_send_never_replays_or_writes_memory(dm, mode):
    wire = dm.service.sender.onebot
    wire.fail, wire.confirm = mode == "timeout", mode != "missing_ack"
    result = await dm.test_dm()
    assert result["status"] == "uncertain"
    assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
    wire.fail, wire.confirm = False, True
    await dm.initialize()
    assert not await dm.deliver_once()
    assert len(wire.calls) == 1


async def test_offline_queue_encrypted_then_fifo_after_reconnect(dm):
    dm.service.sender.onebot.ready.clear()
    first = await dm.test_dm()
    dm.time[0] += 1
    second = await dm.alert("example", "BOISER，测试运维状态")
    assert first["status"] == "pending" and len(await queued(dm)) == 2
    assert TEST_MESSAGE.encode() not in (await queued(dm))[0]["payload"]
    assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]
    dm.service.sender.onebot.ready.set()
    await dm.on_connection(True)
    assert dm.wakeup.is_set()
    await dm.deliver_once()
    await dm.deliver_once()
    assert await dm.delivery_status(first["id"]) == await dm.delivery_status(second) == "sent"
    texts = [params["message"][0]["data"]["text"] for _, params in dm.service.sender.onebot.calls]
    assert texts == [TEST_MESSAGE, "BOISER，测试运维状态"]


async def test_offline_companion_expires_without_next_morning_replay(dm):
    dm.time[0] = stamp("2026-11-14T23:20:00")
    dm.service.sender.onebot.ready.clear()
    key = await slot(dm)
    # Production plans cap expiry at the quiet boundary.
    await dm.store.db.execute(
        "UPDATE proactive_schedule SET expires_at=? WHERE id=?",
        (dm._companion_expiry(dm.clock()), key),
    )
    assert (await dm.generate_slot(key))["status"] == "queued"
    dm.time[0] = stamp("2026-11-15T08:30:00")
    dm.service.sender.onebot.ready.set()
    await dm.deliver_once()
    assert await dm.delivery_status(key) == "expired"
    assert not dm.service.sender.onebot.calls


async def test_alert_expires_at_twelve_hours(dm):
    dm.service.sender.onebot.ready.clear()
    key = await dm.alert("test", "本地模板")
    dm.time[0] += 43200
    dm.service.sender.onebot.ready.set()
    assert not await dm.deliver_once()
    assert await dm.delivery_status(key) == "expired"


@pytest.mark.parametrize(
    "time_text,quiet",
    [("23:29", False), ("23:30", True), ("00:00", True), ("08:29", True), ("08:30", False)],
)
def test_quiet_boundaries(time_text, quiet):
    assert ProactiveConfig().quiet(datetime.fromisoformat("2026-11-14T" + time_text)) is quiet


@pytest.mark.parametrize(
    "override",
    [
        {"max_per_day": 0},
        {"max_per_day": 3},
        {"max_per_day": True},
        {"quiet_hours": "08:30-08:30"},
        {"quiet_hours": "24:00-10:00"},
        {"daily_digest_time": "7:00"},
        {"target": "40000"},
    ],
)
def test_invalid_policy_is_rejected(override):
    with pytest.raises(ValidationError):
        ProactiveConfig(**override)


async def test_local_schedule_persists_no_llm_polling_and_no_quiet_slots(dm):
    await dm.set_config(ProactiveConfig(max_per_day=2))
    before = [dict(r) for r in await dm.store.all("SELECT * FROM proactive_schedule ORDER BY id")]
    assert len(before) == 3
    for r in before:
        if r["kind"] == "companion":
            assert not dm.config.quiet(dm.local(r["due_at"]))
    restored = OwnerProactive(dm.service, clock=dm.clock, rng=random.Random(999))
    await restored.initialize()
    await restored.ensure_plan()
    after = [dict(r) for r in await dm.store.all("SELECT * FROM proactive_schedule ORDER BY id")]
    assert after == before
    assert not dm.service.ai.calls


@pytest.mark.parametrize(
    "changes,skip",
    [
        ({}, True),
        ({"kind": "group", "conversation_id": "40000"}, False),
        ({"user_id": "30000", "conversation_id": "30000"}, False),
    ],
)
async def test_only_owner_private_activity_skips_before_any_paid_call(dm, changes, skip):
    await dm.note_activity(dm.target().model_copy(update=changes))
    key = await slot(dm)
    result = await dm.generate_slot(key)
    assert (result["status"] == "skipped") is skip
    assert bool(dm.service.ai.calls) is not skip
    if skip:
        assert result["reason"] == "owner_recently_active"


async def test_activity_after_generation_and_before_send_cancels_companion(dm):
    key = await slot(dm)
    assert (await dm.generate_slot(key))["status"] == "queued"
    await dm.note_activity(dm.target())
    assert await dm.deliver_once()
    assert await dm.delivery_status(key) == "skipped"
    assert not dm.service.sender.onebot.calls


async def test_delay_crossing_quiet_boundary_prevents_send(dm, monkeypatch):
    dm.time[0] = stamp("2026-11-14T23:29:59")
    key = await dm.enqueue("companion", "喵", expires=dm._companion_expiry(dm.clock()))
    original = dm.service.sender.send

    async def delayed(*args, **kwargs):
        dm.time[0] += 2
        return await original(*args, **kwargs)

    monkeypatch.setattr(dm.service.sender, "send", delayed)
    await dm.deliver_once()
    assert await dm.delivery_status(key) in {"skipped", "expired"}
    assert not dm.service.sender.onebot.calls


async def test_two_hour_companion_expiry_and_daily_cap(dm):
    key = await slot(dm)
    await dm.generate_slot(key)
    await dm.deliver_once()
    assert (await dm.status())["companion_sent_today"] == 1
    assert await dm._eligible("companion", dm.clock()) == "daily_limit"
    await dm.set_config(ProactiveConfig(max_per_day=2))
    dm.service.sender.onebot.ready.clear()
    second = await dm.enqueue("companion", "第二条", expires=dm.clock() + 7200)
    dm.time[0] += 7201
    dm.service.sender.onebot.ready.set()
    await dm.deliver_once()
    assert await dm.delivery_status(second) == "expired"
    assert len(dm.service.sender.onebot.calls) == 1


async def test_missed_greeting_is_skipped_without_generation(dm):
    key = await slot(dm, due=dm.clock() - 301)
    assert (await dm.generate_slot(key))["reason"] == "missed_slot"
    assert not dm.service.ai.calls


async def test_all_model_work_skipped_when_hard_stopped_but_templates_send(dm):
    await dm.store.db.execute("INSERT INTO meta VALUES ('budget_overrun','1')")
    key = await slot(dm)
    assert (await dm.generate_slot(key))["reason"] == "budget_hard_stop"
    assert (await dm.manual_digest())["reason"] == "budget_hard_stop"
    await dm.check_alerts()
    await dm.deliver_once()
    assert (await dm.test_dm())["status"] == "sent"
    assert not dm.service.ai.calls and not await dm.store.all("SELECT * FROM ledger")
    assert len(dm.service.sender.onebot.calls) == 2


async def test_budget_and_provider_alerts_persist_cooldown_and_reset(dm):
    dm.s.daily_usd = Decimal("1")
    dm.s.user_daily_usd = Decimal("2")
    await dm.store.reserve("test", "chat", Decimal(".85"))
    for _ in range(3):
        await dm.store.runtime.provider_result("bilibili_412", True)
    for _ in range(5):
        await dm.store.runtime.provider_result("gemini_chat", True)
    await dm.check_alerts()
    assert len(await queued(dm)) == 3
    await dm.initialize()
    await dm.check_alerts()
    assert len(await queued(dm)) == 3
    await dm.store.reserve("test", "chat", Decimal(".15"))
    await dm.check_alerts()
    assert len(await queued(dm)) == 4
    await dm.store.runtime.provider_result("bilibili_412", False)
    await dm.store.runtime.provider_result("gemini_chat", False)
    dm.time[0] += 21601
    await dm.check_alerts()
    assert len(await queued(dm)) == 4  # Budget once per local day, recovered providers silent.
    assert not dm.service.ai.calls


async def test_long_disconnect_alert_only_after_real_recovery(dm):
    await dm.on_connection(True)
    assert not await queued(dm)
    await dm.on_connection(False)
    dm.time[0] += 299
    await dm.on_connection(True)
    assert not await queued(dm)
    await dm.on_connection(False)
    dm.time[0] += 301
    await dm.on_connection(True)
    assert len(await queued(dm)) == 1
    await dm.on_connection(True)
    assert len(await queued(dm)) == 1
    assert not dm.service.ai.calls


async def test_daily_digest_public_sources_only_once_and_owner_medium(dm):
    svc = dm.service
    svc.diary = RoamingLogger(dm.s, dm.store)
    await svc.diary.initialize()
    dm.s.roaming_enabled = True
    await svc.diary.patrol("video", "公开科学内容", now=dm.clock())
    await svc.sessions.remember(dm.target(), "secret-private-sentinel", "private response")

    # A poisoned notebook accessor must not even be touched by this path.
    class Notebook:
        def for_message(self, _):
            raise AssertionError("Private notebook was accessed")

    svc.notebook = Notebook()
    result = await dm.manual_digest()
    assert result["delivery"] == "sent"
    calls = svc.ai.calls
    assert [c[0] for c in calls] == ["screen", "generate"]
    assert "公开科学内容" in str(calls) and "secret-private-sentinel" not in str(calls)
    assert calls[-1][3]["scenario"] == "owner_digest"
    assert calls[-1][3]["message"]["user_id"] == "20000"
    assert "今日 API 约 $" in svc.sender.onebot.calls[-1][1]["message"][0]["data"]["text"]
    await dm.manual_digest()
    await dm.tick()
    assert len(svc.ai.calls) == 2 and len(svc.sender.onebot.calls) == 1


async def test_failed_source_gate_never_generates_or_retries(dm):
    dm.service.ai.block_purpose = "source"
    result = await dm.manual_digest()
    assert result["status"] == "failed"
    dm.service.ai.block_purpose = None
    await dm.manual_digest()
    assert [c[0] for c in dm.service.ai.calls] == ["screen"]
    assert not dm.service.sender.onebot.calls


async def test_config_disable_cancels_pending_and_survives_store_restart(dm):
    dm.service.sender.onebot.ready.clear()
    result = await dm.test_dm()
    saved = ProactiveConfig(
        enabled=False, daily_digest_time="21:15", quiet_hours="22:00-09:00", max_per_day=2
    )
    await dm.set_config(saved)
    assert await dm.delivery_status(result["id"]) == "cancelled"
    await dm.store.runtime.load_config()
    assert dm.config == saved and dm.store.runtime.proactive_overridden
    await dm.set_config(None)
    assert dm.config == dm.store.runtime.env_proactive
    assert not dm.store.runtime.proactive_overridden
    assert not dm.service.ai.calls


async def test_confirmed_memory_recovery_is_idempotent_and_reset_wins(dm, monkeypatch):
    original = dm.service.sessions.remember_assistant

    async def crash(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("simulate crash between the two databases")

    monkeypatch.setattr(dm.service.sessions, "remember_assistant", crash)
    with pytest.raises(RuntimeError):
        await dm.test_dm()
    row = (await queued(dm))[0]
    assert row["status"] == "delivered"
    assert len((await dm.service.sessions.snapshot(dm.target()))["recent_messages"]) == 1
    monkeypatch.setattr(dm.service.sessions, "remember_assistant", original)
    await dm.initialize()
    assert await dm.delivery_status(row["id"]) == "sent"
    assert len((await dm.service.sessions.snapshot(dm.target()))["recent_messages"]) == 1
    assert len(dm.service.sender.onebot.calls) == 1
    epoch = (await dm.service.sessions.snapshot(dm.target()))["epoch"]
    await dm.service.sessions.clear(dm.target())
    assert not await original(dm.target(), "stale", "different-receipt", expected_epoch=epoch)
    assert not (await dm.service.sessions.snapshot(dm.target()))["recent_messages"]


async def test_owner_reply_waits_for_confirmed_proactive_memory(dm):
    wire = dm.service.sender.onebot
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait_for_ack():
        entered.set()
        await release.wait()

    wire.hook = wait_for_ack
    sending = asyncio.create_task(dm.test_dm())
    await entered.wait()
    msg = dm.target().model_copy(update={"event_id": "reply", "text": "收到，继续说"})
    # Actual admission timestamps use wall clock, unrelated to our scheduler clock.
    import time

    msg = msg.model_copy(update={"created_at": time.time()})
    assert await dm.service.accept(msg) == "accepted"
    job = await dm.store.claim()
    processing = asyncio.create_task(dm.service.handle_message(job))
    await asyncio.sleep(0)
    assert not dm.service.ai.calls
    release.set()
    await sending
    await processing
    assert TEST_MESSAGE in json.dumps(dm.service.ai.calls, ensure_ascii=False, default=str)


async def test_cumulative_budget_settlement_and_pruning_preserves_total(settings):
    settings.total_usd = Decimal(".10")
    store = Store(settings)
    await store.open()
    try:
        reservation = await store.reserve("x", "chat", Decimal(".08"))
        await store.settle(reservation, Decimal(".04"))
        await store.db.execute("UPDATE ledger SET day='2000-01-01'")
        await store.prune()
        assert (await store.budget_status())["total"] == 40000
        with pytest.raises(Rejected, match="budget_exhausted"):
            await store.reserve("x", "chat", Decimal(".07"))
        await store.reserve("x", "chat", Decimal(".06"))
        assert (await store.budget_status())["hard_stop"] is True
    finally:
        await store.close()
    store = Store(settings)
    await store.open()
    try:
        assert (await store.budget_status())["total"] == 100000
    finally:
        await store.close()


async def test_real_bilibili_patrol_412_streak_excludes_backoff_and_resets(dm):
    from hakimi.bilibili import Bilibili

    statuses = iter([412, 412, 412, 200])

    def handler(request):
        return httpx.Response(next(statuses), json={"code": 0, "data": {}})

    dm.s.bilibili_enabled = True
    dm.s.bilibili_min_interval_seconds = 0
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        bili = Bilibili(dm.s, http, runtime=dm.store.runtime)
        for _ in range(3):
            bili.blocked_until = 0
            with pytest.raises(Rejected):
                await bili._get("/x/space/arc/search", {}, "up:123", 0)
        with pytest.raises(Rejected, match="bilibili_circuit_open"):
            await bili._get("/x/space/arc/search", {}, "up:123", 0)
        assert (
            await dm.store.one(
                "SELECT failures FROM operational_health WHERE provider='bilibili_412'"
            )
        )[0] == 3
        bili.blocked_until = 0
        await bili._get("/x/space/arc/search", {}, "up:123", 0)
        assert (
            await dm.store.one(
                "SELECT failures FROM operational_health WHERE provider='bilibili_412'"
            )
        )[0] == 0


async def test_budget_total_85_and_100_alerts_do_not_use_models(dm):
    dm.s.total_usd = Decimal("1")
    dm.s.daily_usd = dm.s.user_daily_usd = Decimal("10")
    await dm.store.reserve("test", "chat", Decimal(".85"))
    await dm.check_alerts()
    await dm.store.reserve("test", "chat", Decimal(".15"))
    await dm.check_alerts()
    await dm.check_alerts()
    keys = [r[0] for r in await dm.store.all("SELECT alert_key FROM alert_cooldown")]
    assert (
        len(keys) == 2
        and any("budget_total_85" in k for k in keys)
        and any("budget_total_100" in k for k in keys)
    )
    assert not dm.service.ai.calls


async def test_monthly_budget_alerts_use_templates_and_deduplicate(dm):
    dm.s.monthly_usd = Decimal("1")
    dm.s.daily_usd = dm.s.user_daily_usd = Decimal("10")
    await dm.store.reserve("test", "chat", Decimal(".85"))
    await dm.check_alerts()
    await dm.store.reserve("test", "chat", Decimal(".15"))
    await dm.check_alerts()
    await dm.check_alerts()
    keys = [r[0] for r in await dm.store.all("SELECT alert_key FROM alert_cooldown")]
    assert len(keys) == 2
    assert any("budget_monthly_85" in key for key in keys)
    assert any("budget_monthly_100" in key for key in keys)
    assert not dm.service.ai.calls


async def test_upgrade_inherits_private_activity_and_migrates_only_owner_outbox(dm):
    import time

    dm.time[0] = time.time()
    target = dm.target().model_copy(update={"event_id": "before-upgrade", "text": "刚聊过"})
    await dm.store.admit(target)
    await dm.store.enqueue_reply("old-owner", target, "旧巡逻摘要", proactive=True)
    other = target.model_copy(update={"user_id": "30000", "conversation_id": "30000"})
    await dm.store.enqueue_reply("old-other", other, "不能主动投递", proactive=True)
    await dm.initialize()
    state = await dm.store.one(
        "SELECT last_private_at FROM owner_dm_state WHERE scope=?", (dm.scope,)
    )
    assert dm.clock() - state[0] < 1
    assert await dm.delivery_status("old-owner") == "pending"
    assert await dm.delivery_status("old-other") == "not_queued"
    assert (await dm.store.one("SELECT status FROM outbox WHERE id='old-other'"))[0] == "cancelled"
    assert not dm.service.ai.calls
