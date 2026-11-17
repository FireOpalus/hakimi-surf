import asyncio
import json
import time

import httpx

from hakimi.adapters import Sender
from hakimi.app import create_app
from hakimi.bili_watcher import BiliPolicy
from hakimi.config import Settings
from hakimi.models import Message
from hakimi.proactive_policy import ProactiveConfig
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_social_integration import Wire, process


async def test_app_lifespan_initializes_world_state_and_modules(settings):
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert svc.modules.get("qq_social") is not None
        assert svc.modules.get("bilibili_surf") is not None
        assert svc.modules.get("minecraft") is not None
        assert svc.db_backup.drive is None
        assert svc.surf_mirror.drive is None
        row = await svc.store.one("SELECT * FROM world_state WHERE singleton=1")
        assert row is not None
        assert json.loads(row["snapshot_json"]) == {}


async def test_service_start_roaming_enabled_gates_tasks_and_healthz(settings, store, tmp_path):
    async with httpx.AsyncClient() as http:
        svc_off = Service(
            settings, store, FakeGateway(), FakeBili(), Sender(settings, http, Wire())
        )
        svc_off.start()
        try:
            names_off = {t.get_name() for t in svc_off.tasks}
            assert "db-backup" not in names_off
            assert "surf-mirror" not in names_off
            assert "roaming-diary" not in names_off
        finally:
            await svc_off.stop()

    sa_file = tmp_path / "sa.json"
    sa_file.write_text(
        json.dumps(
            {
                "type": "service_account",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_email": "test@example.iam.gserviceaccount.com",
            }
        ),
        encoding="utf-8",
    )
    roam_settings = Settings(
        _env_file=None,
        **{
            **settings.model_dump(),
            "data_dir": tmp_path / "roam_data",
            "roaming_enabled": True,
            "gdrive_roaming_folder_id": "1ExampleRoamingFolderId0000000000",
            "gdrive_service_account_file": sa_file,
        },
    )
    app = create_app(roam_settings, gateway_factory=FakeGateway, start_workers=True)
    async with app.router.lifespan_context(app):
        svc_on = app.state.service
        names_on = {t.get_name() for t in svc_on.tasks}
        assert "roaming-diary" in names_on
        assert "db-backup" in names_on
        assert "surf-mirror" in names_on
        assert svc_on.db_backup.drive is not None
        assert svc_on.surf_mirror.drive is not None
        assert not any(t.done() for t in svc_on.tasks)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://local"
        ) as client:
            resp = await client.get("/healthz")
            assert resp.status_code == 200


async def test_attention_lock_suppresses_non_urgent_ticks_and_keeps_alerts(
    settings, store, monkeypatch
):
    settings.owner_qq_id = "20000"
    settings.onebot_enabled = True
    settings.proactive_dm_enabled = True
    settings.bili_autonomous_enabled = True

    ai = FakeGateway()
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, ai, FakeBili(), Sender(settings, http, wire))
        await svc.proactive.initialize()
        await svc.bili_watcher.initialize()
        await svc.world_state.initialize()
        await svc.proactive.set_config(
            ProactiveConfig(
                enabled=True,
                daily_digest_enabled=False,
                companion_ping_enabled=True,
                max_per_day=2,
                quiet_hours="23:55-00:05",
            )
        )

        # 1. Service.patrol skips poll_once while focus_level == "HIGH"
        poll_calls = 0

        async def fake_poll_once(*, force=False):
            nonlocal poll_calls
            poll_calls += 1

        monkeypatch.setattr(svc, "poll_once", fake_poll_once)
        await svc.world_state.update_state(current_module="minecraft", focus_level="HIGH")
        patrol_task = asyncio.create_task(svc.patrol())
        await asyncio.sleep(0.05)
        assert poll_calls == 0
        patrol_task.cancel()
        await asyncio.gather(patrol_task, return_exceptions=True)

        # 2. BiliWatcher.tick skips run_once under HIGH focus, runs when dropped to LOW
        local = svc.bili_watcher.local()
        day = local.date().isoformat()
        slot_name = "13:00"
        due_min = svc.bili_watcher.slot_due_minute(day, slot_name)
        due_dt = local.replace(hour=due_min // 60, minute=due_min % 60, second=10, microsecond=0)
        due_ts = due_dt.timestamp()
        svc.bili_watcher.clock = lambda: due_ts
        svc.bili_watcher.policy = BiliPolicy(enabled=True, times=[slot_name])
        bili_runs = []

        async def fake_bili_run_once(*, slot="manual"):
            bili_runs.append(slot)
            return {"status": "done"}

        monkeypatch.setattr(svc.bili_watcher, "run_once", fake_bili_run_once)
        await svc.bili_watcher.tick()
        assert bili_runs == []

        await svc.world_state.update_state(current_module="idle", focus_level="LOW")
        await svc.bili_watcher.tick()
        assert bili_runs == [slot_name]

        # 3. OwnerProactive.tick keeps urgent alerts under HIGH focus while skipping companion slots
        now = due_ts
        svc.proactive.clock = lambda: now
        await svc.world_state.update_state(current_module="minecraft", focus_level="HIGH")
        slot_id = "slot-high-focus"
        async with store.transaction():
            await store.db.execute(
                "UPDATE proactive_schedule SET status='skipped' WHERE scope=?",
                (svc.proactive.scope,),
            )
            await store.db.execute(
                "INSERT INTO proactive_schedule VALUES (?,?,?,?,?,?,'planned',NULL)",
                (slot_id, svc.proactive.scope, day, "companion", now - 10, now + 1800),
            )
            await store.db.execute(
                "INSERT OR REPLACE INTO operational_health VALUES ('bilibili_412', 3, ?)",
                (now,),
            )

        gen_calls = []

        async def fake_generate_slot(sid, *, manual=False):
            gen_calls.append(sid)
            return {"status": "queued", "id": sid}

        monkeypatch.setattr(svc.proactive, "generate_slot", fake_generate_slot)
        await svc.proactive.tick()
        assert gen_calls == []

        # Urgent operational alert and WS recovery alert still enqueue while HIGH focus is active
        alert_rows = await store.all(
            "SELECT kind FROM pending_owner_dms WHERE scope=? AND kind='alert'",
            (svc.proactive.scope,),
        )
        assert len(alert_rows) >= 1

        svc.proactive.clock = lambda: now - 700
        await svc.proactive.on_connection(True)
        svc.proactive.clock = lambda: now - 600
        await svc.proactive.on_connection(False)
        svc.proactive.clock = lambda: now
        await svc.proactive.on_connection(True)
        ws_alert_rows = await store.all(
            "SELECT alert_key FROM alert_cooldown WHERE alert_key LIKE ?",
            (svc.proactive.scope + ":ws_recovered%",),
        )
        assert len(ws_alert_rows) == 1

        # Dropping below HIGH allows planned companion slot generation to proceed
        await svc.world_state.update_state(current_module="idle", focus_level="LOW")
        await svc.proactive.tick()
        assert gen_calls == [slot_id]


async def test_service_chat_payload_injects_world_state_summary(settings, store, message):
    settings.owner_qq_id = "20000"
    settings.bot_qq_ids = {"60000"}
    settings.onebot_allowed_users = {"30000", "60000"}
    ai = FakeGateway()
    wire = Wire()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, ai, FakeBili(), Sender(settings, http, wire))
        await svc.world_state.initialize()

        # Baseline idle/LOW: no world_state_summary key injected
        await process(svc, message.model_copy(update={"event_id": "ws-idle-1", "text": "在干嘛"}))
        await svc.deliver_once()
        idle_gen = [c for c in ai.calls if c[0] == "generate"][-1]
        assert "world_state_summary" not in idle_gen[1]

        # Active HIGH MC state: world_state_summary injected
        mc = svc.modules.get("minecraft")
        assert mc is not None
        await mc.update_snapshot(
            {
                "connected": True,
                "dimension": "overworld",
                "pos": (12, -54, 88),
                "health": 16,
                "activity": "矿洞挖矿",
            }
        )
        await svc.world_state.update_state(current_module="minecraft", focus_level="HIGH")
        await process(svc, message.model_copy(update={"event_id": "ws-mc-2", "text": "还在挖矿吗"}))
        mc_gen = [c for c in ai.calls if c[0] == "generate"][-1]
        ws_summary = mc_gen[1]["world_state_summary"]
        assert ws_summary["current_module"] == "minecraft"
        assert ws_summary["focus_level"] == "HIGH"
        assert "MC在线" in ws_summary["sensor"]
        assert "attention_note" in ws_summary

        # Known bots never receive world_state_summary
        bot_msg = Message(
            platform="onebot",
            bot_id="10000",
            event_id="ws-bot-3",
            user_id="60000",
            kind="group",
            conversation_id="40000",
            text="bot ping",
            addressed=True,
            created_at=time.time(),
        )
        assert svc.bot_guard.known_bot(bot_msg)
