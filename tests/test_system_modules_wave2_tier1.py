"""Targeted tests for Wave 2 Tier 1 resident system modules: proactive_delivery & video_watch."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi.adapters import Sender
from hakimi.life_engine import LifeEngine
from hakimi.modules import ModuleRegistry
from hakimi.proactive_dm import OwnerProactive
from hakimi.service import Service
from hakimi.system_modules import SystemResources, install_system_modules
from hakimi.system_modules.event_ledger import Module as EventLedgerModule
from hakimi.system_modules.proactive_delivery import Module as ProactiveDeliveryModule
from hakimi.system_modules.video_watch import Module as VideoWatchModule
from hakimi.video_policy import VideoWatchConfig
from hakimi.video_watch import VideoWatch
from tests.fakes import FakeBili, FakeGateway


def _stamp(value: str) -> float:
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


class _FakeWire:
    def __init__(self) -> None:
        self.ready = asyncio.Event()
        self.ready.set()
        self.calls: list[tuple[str, dict]] = []

    async def call(self, action: str, params: dict) -> dict:
        self.calls.append((action, params))
        return {"message_id": len(self.calls)}


async def test_wave2_tier1_discovery_topology_and_registry_wiring(settings, store):
    registry = ModuleRegistry()
    assert registry.owner_delivery is None

    # Minimal stub without pre-attached proactive/video_watch or life_engine.
    svc_stub = SimpleNamespace(
        s=settings,
        store=store,
        ai=FakeGateway(),
        sender=None,
        sessions=None,
        modules=registry,
        owner_dialogue_lock=asyncio.Lock(),
    )
    install_system_modules(
        registry,
        SystemResources(settings, store, svc_stub.ai, None, svc_stub),
    )

    names = list(registry.systems.keys())
    assert "event_ledger" in names
    assert "proactive_delivery" in names
    assert "video_watch" in names
    assert names.index("event_ledger") < names.index("proactive_delivery")

    proactive_mod = registry.systems["proactive_delivery"]
    video_mod = registry.systems["video_watch"]
    assert isinstance(proactive_mod, ProactiveDeliveryModule)
    assert isinstance(video_mod, VideoWatchModule)

    assert ProactiveDeliveryModule.requires == ("event_ledger",)
    assert ProactiveDeliveryModule.admin_panel == "proactive"
    assert VideoWatchModule.requires == ()
    assert VideoWatchModule.admin_panel == "bili-account"

    assert proactive_mod.ledger is registry.systems["event_ledger"].ledger
    assert isinstance(proactive_mod.proactive, OwnerProactive)
    assert registry.owner_delivery is proactive_mod.proactive
    assert proactive_mod.workers() == (proactive_mod.run_alerts, proactive_mod.run_delivery)

    assert isinstance(video_mod.watch, VideoWatch)
    assert video_mod.workers() == ()
    assert video_mod.colab_compute is registry.systems["colab_compute"].compute
    assert video_mod.colab_available() is False

    # Configured Colab backend is exposed read-only without changing module dependencies.
    s_colab = settings.model_copy(
        update={
            "colab_worker_url": "http://127.0.0.1:12146",
            "colab_worker_token": SecretStr("colab-worker-secret-token-123456"),
        }
    )
    reg_colab = ModuleRegistry()
    svc_colab = SimpleNamespace(
        s=s_colab,
        store=store,
        ai=FakeGateway(),
        sessions=None,
        modules=reg_colab,
        owner_dialogue_lock=asyncio.Lock(),
    )
    install_system_modules(
        reg_colab,
        SystemResources(s_colab, store, svc_colab.ai, None, svc_colab),
    )
    assert reg_colab.systems["video_watch"].colab_available() is True


async def test_proactive_delivery_workers_dynamic_target_error_isolation_and_cancel(
    settings, store, monkeypatch, caplog
):
    registry = ModuleRegistry()
    ledger_mod = EventLedgerModule(
        SystemResources(settings, store, FakeGateway(), None, SimpleNamespace()),
        registry,
    )
    registry.register_system(ledger_mod)

    svc_stub = SimpleNamespace(s=settings, store=store, owner_dialogue_lock=asyncio.Lock())
    mod = ProactiveDeliveryModule(
        SystemResources(settings, store, FakeGateway(), None, svc_stub),
        registry,
    )

    # 1. Workers resolve service.proactive dynamically so test doubles work transparently.
    started_alerts = asyncio.Event()
    started_delivery = asyncio.Event()
    cancelled: list[str] = []

    class _DoubleProactive:
        async def run(self) -> None:
            started_alerts.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.append("alerts")
                raise

        async def delivery(self) -> None:
            started_delivery.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.append("delivery")
                raise

    svc_stub.proactive = _DoubleProactive()
    tasks = [asyncio.create_task(worker()) for worker in mod.workers()]
    try:
        await asyncio.wait_for(started_alerts.wait(), timeout=2)
        await asyncio.wait_for(started_delivery.wait(), timeout=2)
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(r, asyncio.CancelledError) for r in results)
    assert set(cancelled) == {"alerts", "delivery"}

    # 2. Real OwnerProactive.run / delivery catch Exception, sanitize error, re-raise Cancel.
    svc_stub.proactive = mod.proactive
    tick_count = 0

    async def flaky_tick() -> None:
        nonlocal tick_count
        tick_count += 1
        if tick_count == 1:
            raise RuntimeError("sensitive-token-should-never-surface")

    async def fast_sleep(_sec: float) -> None:
        if tick_count >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(mod.proactive, "tick", flaky_tick)
    monkeypatch.setattr("hakimi.proactive_dm.asyncio.sleep", fast_sleep)

    with caplog.at_level(logging.INFO, logger="hakimi"), pytest.raises(asyncio.CancelledError):
        await mod.run_alerts()
    assert tick_count == 2
    assert mod.proactive.last_worker_error is None
    assert any("event=owner_dm_worker_error" in rec.getMessage() for rec in caplog.records)

    deliver_count = 0

    async def failing_deliver() -> bool:
        nonlocal deliver_count
        deliver_count += 1
        raise TimeoutError("private-timeout-detail")

    async def cancel_sleep(_sec: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(mod.proactive, "deliver_once", failing_deliver)
    monkeypatch.setattr("hakimi.proactive_dm.asyncio.sleep", cancel_sleep)

    with pytest.raises(asyncio.CancelledError):
        await mod.run_delivery()
    assert deliver_count == 1
    assert mod.proactive.last_delivery_error == "TimeoutError"

    st = await mod.status()
    assert st["last_error"] == "TimeoutError"
    assert "private-timeout-detail" not in str(st)


async def test_proactive_delivery_status_outbox_anomalies_and_empty_state(settings, store):
    # 1. Empty state without life_engine and without opened SQLite db.
    unconfigured = settings.model_copy(update={"owner_qq_id": ""})
    fake_store = SimpleNamespace(vault=store.vault, db=None, lock=asyncio.Lock())
    reg_empty = ModuleRegistry()
    mod_empty = ProactiveDeliveryModule(
        SystemResources(unconfigured, fake_store, FakeGateway(), None, SimpleNamespace()),
        reg_empty,
    )
    st_empty = await mod_empty.status()
    assert st_empty["state"] == "unconfigured"
    assert st_empty["configured"] is False
    assert st_empty["outbox"] == {
        "pending": 0,
        "sending": 0,
        "delivered": 0,
        "uncertain": 0,
        "failed": 0,
        "sent_today": 0,
    }
    assert st_empty["ledger_pending"] == 0
    assert "last_error" not in st_empty

    # 2. Populated SQLite store without life_engine (isolated SimpleNamespace service).
    clock_now = _stamp("2026-11-14T15:00:00")
    day = "2026-11-14"
    prev_day = "2026-11-13"
    registry = ModuleRegistry()
    res = SystemResources(settings, store, FakeGateway(), None, SimpleNamespace())
    ledger_mod = EventLedgerModule(res, registry)
    registry.register_system(ledger_mod)
    await ledger_mod.initialize()
    mod = ProactiveDeliveryModule(res, registry)
    mod.proactive.clock = lambda: clock_now
    await mod.initialize()

    scope = mod.proactive.scope
    async with store.transaction():
        rows = [
            (
                "dm-pending",
                scope,
                day,
                "alert",
                None,
                clock_now - 30,
                clock_now + 3600,
                "pending",
                None,
                None,
                None,
                None,
            ),
            (
                "dm-sending",
                scope,
                day,
                "patrol",
                None,
                clock_now - 20,
                clock_now + 3600,
                "sending",
                None,
                None,
                None,
                None,
            ),
            (
                "dm-delivered",
                scope,
                day,
                "alert",
                None,
                clock_now - 15,
                clock_now + 3600,
                "delivered",
                None,
                clock_now - 10,
                None,
                None,
            ),
            (
                "dm-uncertain",
                scope,
                day,
                "alert",
                None,
                clock_now - 10,
                clock_now + 3600,
                "uncertain",
                None,
                None,
                None,
                "send_unconfirmed",
            ),
            (
                "dm-sent-today",
                scope,
                day,
                "test",
                None,
                clock_now - 50,
                clock_now + 3600,
                "sent",
                None,
                clock_now - 40,
                None,
                None,
            ),
            (
                "dm-sent-prev",
                scope,
                prev_day,
                "test",
                None,
                clock_now - 90000,
                clock_now - 50000,
                "sent",
                None,
                clock_now - 89000,
                None,
                None,
            ),
        ]
        await store.db.executemany(
            "INSERT INTO pending_owner_dms VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        await store.db.execute(
            "INSERT INTO proactive_schedule VALUES (?,?,?,?,?,?,?,?)",
            (
                "sched-fail",
                scope,
                day,
                "digest",
                clock_now - 60,
                clock_now + 3600,
                "failed",
                "generation_failed",
            ),
        )
    await ledger_mod.ledger.post(
        "gcp_billing",
        "billing_alert",
        "GCP 赠金余量预警",
        target_module="proactive_delivery",
        dedupe_key="low_credit",
        visibility="owner_private",
        now=clock_now,
    )

    st = await mod.status()
    assert st["configured"] is True
    assert st["state"] == "disabled"  # No life_engine / qq_social in isolated stub
    assert st["outbox"] == {
        "pending": 1,
        "sending": 1,
        "delivered": 1,
        "uncertain": 1,
        "failed": 1,
        "sent_today": 1,
    }
    assert st["ledger_pending"] == 1
    assert st["last_error"] == "send_unconfirmed"
    assert st["last_success"] == pytest.approx(clock_now - 40)
    assert "待投递 3" in st["detail"]
    assert "今日已投递 1" in st["detail"]
    assert "待确认 1" in st["detail"]
    assert "失败 1" in st["detail"]


async def test_proactive_delivery_event_ledger_enqueue_deliver_and_drain(settings, store):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    clock = [_stamp("2026-11-14T12:00:00")]
    wire = _FakeWire()

    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, wire))
        try:
            proactive_mod = svc.modules.systems["proactive_delivery"]
            assert isinstance(proactive_mod, ProactiveDeliveryModule)
            assert proactive_mod.ledger is svc.event_ledger

            svc.proactive.clock = lambda: clock[0]
            svc.event_ledger.clock = lambda: clock[0]
            svc.life_engine = LifeEngine(svc, clock=svc.proactive.clock)
            svc.world_state.clock = svc.proactive.clock
            await svc.event_ledger.initialize()
            await svc.life_engine.initialize()
            await proactive_mod.initialize()

            # 1. Enqueue Colab song cover receipt via unified event_ledger interface.
            res1 = await proactive_mod.enqueue_ledger_alert(
                source_module="song_studio",
                code="song_cover_done:job42",
                text="BOISER，新歌《晴天》我学会啦喵~",
                event_type="song_cover_done",
                payload={"song_title": "晴天"},
            )
            assert res1["status"] == "queued"
            assert res1["dm_id"] is not None
            assert res1["ledger_event_id"] is not None
            assert res1["consumed"] is True
            assert await svc.proactive.delivery_status(res1["dm_id"]) == "pending"
            # Owner-private notice must never leak to public ledger reads.
            assert await svc.event_ledger.read(include_owner_private=False) == []
            settled = await svc.event_ledger.read(
                source_module="song_studio",
                include_owner_private=True,
                include_settled=True,
            )
            assert len(settled) == 1
            assert settled[0].status == "consumed"
            assert settled[0].consumed_by == "proactive_delivery"

            # Cooldown dedupe stays intact on repeated enqueue.
            res_dup = await proactive_mod.enqueue_ledger_alert(
                source_module="song_studio",
                code="song_cover_done:job42",
                text="BOISER，新歌《晴天》我学会啦喵~",
                event_type="song_cover_done",
                record_ledger=False,
            )
            assert res_dup["status"] == "cooldown"
            assert res_dup["dm_id"] is None
            assert res_dup["consumed"] is False

            # 2. Direct deliver_ledger_notice from pre-posted gcp_billing event.
            billing_evt = await svc.event_ledger.post(
                "gcp_billing",
                "billing_alert",
                "BOISER，GCP 赠金余量低于 $20 喵。",
                payload={"alert_code": "gcp_low_credit", "daily": True},
                target_module="proactive_delivery",
                dedupe_key="low_credit",
                visibility="owner_private",
                now=clock[0],
            )
            assert len(await proactive_mod.pending_ledger_notices()) == 1
            res2 = await proactive_mod.deliver_ledger_notice(
                "gcp_billing:low_credit",
                daily=True,
            )
            assert res2["status"] == "queued"
            assert res2["ledger_event_id"] == billing_evt.id
            assert res2["consumed"] is True
            assert await proactive_mod.pending_ledger_notices() == []

            # 3. Deferred alert while delivery disabled -> drained once re-enabled.
            orig_cfg = svc.life_engine.config
            svc.life_engine.config = orig_cfg.model_copy(
                update={
                    "module_settings": {
                        **orig_cfg.module_settings,
                        "social_presence": {"owner_delivery_enabled": False},
                    }
                }
            )
            deferred = await proactive_mod.enqueue_ledger_alert(
                source_module="colab_compute",
                code="colab_cu_warn",
                text="BOISER，Colab 今日 CU 预算已达 80% 喵。",
                event_type="cu_alert",
            )
            assert deferred["status"] == "disabled"
            assert deferred["dm_id"] is None
            assert deferred["consumed"] is False
            assert len(await proactive_mod.pending_ledger_notices()) == 1

            svc.life_engine.config = orig_cfg
            drained = await proactive_mod.drain_due_ledger_notices()
            assert len(drained) == 1
            assert drained[0]["status"] == "queued"
            assert drained[0]["consumed"] is True
            assert await proactive_mod.pending_ledger_notices() == []

            # 4. Patrol kind delivery + missing event handling.
            patrol_evt = await svc.event_ledger.post(
                "bilibili_surf",
                "up_live",
                "BOISER，订阅的直播间开播啦喵~",
                target_module="proactive_delivery",
                visibility="public",
                now=clock[0],
            )
            res_patrol = await proactive_mod.deliver_ledger_notice(patrol_evt.id, kind="patrol")
            assert res_patrol["status"] == "queued"
            assert res_patrol["consumed"] is True

            missing = await proactive_mod.deliver_ledger_notice("does-not-exist")
            assert missing["status"] == "not_found"

            empty_res = await proactive_mod.enqueue_ledger_alert(
                source_module="song_studio",
                code="empty_test",
                text="   ",
            )
            assert empty_res["status"] == "skipped"
            assert empty_res["reason"] == "empty_text"

            # Disabled enqueue path for patrol kind returns rejected cleanly without consuming.
            svc.life_engine.config = orig_cfg.model_copy(
                update={
                    "module_settings": {
                        **orig_cfg.module_settings,
                        "social_presence": {"owner_delivery_enabled": False},
                    }
                }
            )
            patrol_evt2 = await svc.event_ledger.post(
                "bilibili_surf",
                "up_live",
                "BOISER，另一场直播开播啦喵~",
                target_module="proactive_delivery",
                visibility="public",
                now=clock[0],
            )
            rejected_res = await proactive_mod.deliver_ledger_notice(patrol_evt2.id, kind="patrol")
            assert rejected_res["status"] == "rejected"
            assert rejected_res["reason"] == "disabled"
            assert rejected_res["consumed"] is False
            svc.life_engine.config = orig_cfg

            # Verify live status reflects healthy online state and queued outbox items.
            st_live = await proactive_mod.status()
            assert st_live["state"] == "ready"
            assert st_live["connected"] is True
            assert st_live["outbox"]["pending"] == 4
            assert st_live["ledger_pending"] == 1
        finally:
            await svc.stop()

    # Without an attached event_ledger, deliver_ledger_notice fails gracefully.
    no_ledger_mod = ProactiveDeliveryModule(
        SystemResources(settings, store, FakeGateway(), None, SimpleNamespace()),
        ModuleRegistry(),
    )
    assert no_ledger_mod.ledger is None
    assert await no_ledger_mod.pending_ledger_notices() == []
    assert (await no_ledger_mod.deliver_ledger_notice("any-id"))["status"] == "unavailable"


async def test_proactive_delivery_ledger_lookup_is_lazy_and_drain_avoids_reread(
    settings, store, monkeypatch
):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    clock = [_stamp("2026-11-14T13:00:00")]
    wire = _FakeWire()

    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, FakeGateway(), FakeBili(), Sender(settings, http, wire))
        try:
            proactive_mod = svc.modules.systems["proactive_delivery"]
            svc.proactive.clock = lambda: clock[0]
            svc.event_ledger.clock = lambda: clock[0]
            svc.life_engine = LifeEngine(svc, clock=svc.proactive.clock)
            svc.world_state.clock = svc.proactive.clock
            await svc.event_ledger.initialize()
            await svc.life_engine.initialize()
            await proactive_mod.initialize()

            read_calls: list[dict] = []
            orig_read = svc.event_ledger.read

            async def spy_read(*args, **kwargs):
                read_calls.append(dict(kwargs))
                return await orig_read(*args, **kwargs)

            monkeypatch.setattr(svc.event_ledger, "read", spy_read)

            # 1. Targeted hit short-circuits after a single targeted ledger.read().
            targeted_evt = await svc.event_ledger.post(
                "song_studio",
                "song_cover_done",
                "BOISER，新歌《花海》练好啦喵~",
                target_module="proactive_delivery",
                dedupe_key="cover_huahai",
                visibility="owner_private",
                now=clock[0],
            )
            res_targeted = await proactive_mod.deliver_ledger_notice(targeted_evt.id)
            assert res_targeted["status"] == "queued"
            assert res_targeted["consumed"] is True
            assert len(read_calls) == 1
            assert read_calls[0]["target_module"] == "proactive_delivery"

            # 2. Untargeted entry lazily falls back to the broad second ledger.read().
            read_calls.clear()
            broad_evt = await svc.event_ledger.post(
                "gcp_billing",
                "billing_alert",
                "BOISER，GCP 赠金触发阈值喵。",
                target_module="",
                dedupe_key="broad_alert",
                visibility="owner_private",
                now=clock[0],
            )
            res_broad = await proactive_mod.deliver_ledger_notice(broad_evt.id)
            assert res_broad["status"] == "queued"
            assert res_broad["consumed"] is True
            assert len(read_calls) == 2
            assert read_calls[0]["target_module"] == "proactive_delivery"
            assert "target_module" not in read_calls[1]

            # 3. drain_due_ledger_notices reads once regardless of batch size (no per-item reread).
            read_calls.clear()
            for idx in range(3):
                await svc.event_ledger.post(
                    "colab_compute",
                    "job_notice",
                    f"BOISER，批量作业 #{idx} 完成啦喵~",
                    payload={"alert_code": f"batch_job_{idx}"},
                    target_module="proactive_delivery",
                    dedupe_key=f"batch_{idx}",
                    visibility="owner_private",
                    now=clock[0],
                )
            drained = await proactive_mod.drain_due_ledger_notices()
            assert len(drained) == 3
            assert all(item["status"] == "queued" and item["consumed"] for item in drained)
            assert len(read_calls) == 1
        finally:
            await svc.stop()


async def test_video_watch_module_lifecycle_status_quota_colab_and_empty_state(settings, store):
    # 1. Empty state before initialize() and with unopened store (never mutates DB or fs).
    fake_store = SimpleNamespace(db=None, lock=asyncio.Lock())
    reg_empty = ModuleRegistry()
    mod_empty = VideoWatchModule(
        SystemResources(settings, fake_store, FakeGateway(), None, SimpleNamespace()),
        reg_empty,
    )
    st_unopened = await mod_empty.status()
    assert st_unopened["state"] == "disabled"
    assert st_unopened["initialized"] is False
    assert st_unopened["attempts_today"] == 0
    assert st_unopened["remaining_today"] == settings.daily_video_watch_limit
    assert st_unopened["remote_cleanup_pending"] == 0
    assert st_unopened["colab_registered"] is False
    assert st_unopened["colab_available"] is False
    assert mod_empty.watch.initialized is False

    # 2. Opened store before initialize() also stays strictly read-only.
    clock_now = _stamp("2026-11-14T16:00:00")
    reg = ModuleRegistry()
    svc_stub = SimpleNamespace(s=settings, store=store, ai=FakeGateway())
    mod = VideoWatchModule(SystemResources(settings, store, svc_stub.ai, None, svc_stub), reg)
    mod.watch.clock = lambda: clock_now
    st_pre = await mod.status()
    assert st_pre["state"] == "disabled"
    assert st_pre["initialized"] is False
    assert mod.watch.initialized is False

    # 3. Initialize, enable, record attempts/cleanup backlog, and verify quota calculations.
    await mod.initialize()
    assert mod.watch.initialized is True
    await mod.watch.set_config(VideoWatchConfig(enabled=True, max_seconds=600, daily_limit=3))

    day = mod.watch.day()
    async with store.transaction():
        await store.db.executemany(
            "INSERT INTO video_watch_attempts VALUES (?,?,?,?,?,?,?,NULL,NULL)",
            [
                ("a1", day, "BV1xx411c7mD", "s1", "done", None, clock_now - 30),
                ("a2", day, "BV1xx411c7mD", "s2", "failed", "download_format", clock_now - 20),
                ("a3", day, "BV1xx411c7mD", "s3", "uncertain", "interrupted", clock_now - 10),
            ],
        )
        await store.db.execute(
            "CREATE TABLE IF NOT EXISTS gemini_video_files ("
            "id TEXT PRIMARY KEY, payload BLOB NOT NULL, created REAL NOT NULL, "
            "retry_after REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)"
        )
        for idx in range(3):
            await store.db.execute(
                "INSERT INTO gemini_video_files VALUES (?,?,?,0,0)",
                (f"vf-{idx}", b"sealed", clock_now),
            )

    st_active = await mod.status()
    assert st_active["state"] == "ready"
    assert st_active["enabled"] is True
    assert st_active["initialized"] is True
    assert st_active["source"] == "sqlite"
    assert st_active["daily_limit"] == 3
    assert st_active["max_seconds"] == 600
    assert st_active["attempts_today"] == 2  # Local download_format failure does not consume quota
    assert st_active["remaining_today"] == 1
    assert st_active["remote_cleanup_pending"] == 3
    assert st_active["last_error"] == "cleanup_pending"
    assert "今日 2/3" in st_active["detail"]

    # Unlimited daily quota reports remaining_today=None.
    await mod.watch.set_config(VideoWatchConfig(enabled=True, daily_limit="unlimited"))
    st_unlimited = await mod.status()
    assert st_unlimited["daily_limit"] == "unlimited"
    assert st_unlimited["remaining_today"] is None
    assert "不限" in st_unlimited["detail"]

    # 4. Dynamic optional read-only colab_compute binding without strong coupling.
    reg.systems["colab_compute"] = SimpleNamespace(
        compute=SimpleNamespace(configured=lambda: False)
    )
    assert mod.colab_compute is reg.systems["colab_compute"].compute
    assert (await mod.status())["colab_registered"] is True
    assert (await mod.status())["colab_available"] is False

    reg.systems["colab_compute"] = SimpleNamespace(compute=SimpleNamespace(configured=lambda: True))
    st_colab = await mod.status()
    assert st_colab["colab_registered"] is True
    assert st_colab["colab_available"] is True
    assert "Colab 转写就绪" in st_colab["detail"]

    # 5. Non-VideoWatch test stub on service.video_watch never breaks module status().
    svc_stub.video_watch = SimpleNamespace(analyze=lambda *a, **kw: None)
    st_stub = await mod.status()
    assert st_stub["state"] == "ready"
    svc_stub.video_watch = None
    st_none = await mod.status()
    assert st_none["state"] == "ready"
