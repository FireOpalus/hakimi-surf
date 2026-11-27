"""Wave 3+4 tests: ingress enrich gating, DB/session retention, WAL, sessions backup, TG parity."""

from __future__ import annotations

import asyncio
import json
import logging
import stat
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi import db_backup, dual_memory
from hakimi.app import create_app
from hakimi.colab_compute import SCHEMA as COLAB_SCHEMA
from hakimi.db_backup import (
    DRIVE_BACKUP_PROP,
    DRIVE_SESSIONS_PROP,
    DatabaseBackup,
    verify_sessions_backup,
)
from hakimi.drive_client import API, UPLOAD, FolderDrive
from hakimi.dual_memory import Extraction, RecallIndex
from hakimi.models import ImageRef, Message
from hakimi.modules import BaseModule
from hakimi.modules.bili_watcher import SCHEMA as BILI_SCHEMA
from hakimi.owner_cloud_memory import _sanitize as cloud_sanitize
from hakimi.planning_journal import PlannerJournal
from hakimi.security import redact_secrets
from hakimi.service import Service
from hakimi.session_manager import SessionManager
from hakimi.social import OwnerBinding, owner_target
from hakimi.social_chat_config import RemoteChatConfig, save_remote_chat_config
from hakimi.surf_mirror import SurfMirror, sanitize_mirror_text
from hakimi.video_watch import SCHEMA as VIDEO_WATCH_SCHEMA
from hakimi.voice_companion import SCHEMA as VC_SCHEMA
from hakimi.world_state import IntentTicketCreate, WorldStateManager
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_delivery_resilience import TINY_PNG_B64
from tests.test_voice_companion import _SocketDouble, _StreamMockGateway

TZ = ZoneInfo("Asia/Shanghai")
FOLDER = "backup-folder-wave4-123"
TG_BOT_ID = "tg_bot_888"
TG_OWNER_ID = "tg_owner_555"


def _ts(iso_local: str) -> float:
    return datetime.fromisoformat(iso_local).replace(tzinfo=TZ).timestamp()


class _FakeTelegramModule(BaseModule):
    module_id = "telegram_chat"
    display_name = "Telegram 社交"
    chat_platforms = ("telegram",)

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self.online = True

    async def _raw_sensor_summary(self) -> str:
        return ""

    def authorize_chat(self, message, settings, *, bot_exempt: bool = False) -> bool:
        return message.platform == "telegram" and message.bot_id == TG_BOT_ID

    def chat_connected(self, platform: str) -> bool:
        return platform == "telegram" and self.online


# ---------------------------------------------------------------------------
# Wave 3: Ingress enrich() gating, background done-callback, dead code removal
# ---------------------------------------------------------------------------


async def test_ingress_enrich_only_spawns_on_accepted_and_dead_worker_removed(
    settings, store, message, monkeypatch
):
    assert not hasattr(Service, "gcp_billing_worker")
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())

    enriched_events: list[str] = []

    async def fake_enrich(msg: Message):
        enriched_events.append(msg.event_id)
        return []

    monkeypatch.setattr(svc, "enrich", fake_enrich)

    # 1. Ignored unaddressed group message with image + URL must NOT trigger enrich().
    ignored_group = message.model_copy(
        update={
            "kind": "group",
            "conversation_id": "40000",
            "event_id": "grp-ignored-1",
            "addressed": False,
            "text": "看看这个 https://example.com/a",
            "images": [ImageRef(file="img1.png", url="https://example.com/a.png")],
        }
    )
    res_ignored = await svc.accept(ignored_group)
    assert res_ignored == "ignored"
    await asyncio.sleep(0)
    assert enriched_events == []
    assert svc.store.event_key(ignored_group) not in svc.enrichments

    # 2. Rate-limited private message must NOT trigger enrich().
    settings.user_requests_per_minute = 1
    first_ok = message.model_copy(
        update={
            "event_id": "priv-ok-1",
            "text": "查看 https://example.com/ok",
        }
    )
    second_limited = message.model_copy(
        update={
            "event_id": "priv-limited-2",
            "text": "查看 https://example.com/limited",
        }
    )
    assert await svc.accept(first_ok) == "accepted"
    assert await svc.accept(second_limited) == "limited"
    if svc.background_tasks:
        await asyncio.gather(*list(svc.background_tasks), return_exceptions=True)
    assert enriched_events == ["priv-ok-1"]
    assert svc.store.event_key(first_ok) in svc.enrichments
    assert svc.store.event_key(second_limited) not in svc.enrichments

    # 3. Service.background() consumes unhandled task exceptions cleanly.
    loop = asyncio.get_running_loop()
    unhandled: list[dict] = []
    prev_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: unhandled.append(ctx))
    try:

        async def boom():
            raise RuntimeError("boom-secret-free")

        task = svc.background(boom())
        await asyncio.sleep(0.02)
        assert task.done()
        assert task not in svc.background_tasks
        del task
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(prev_handler)
    assert unhandled == []


# ---------------------------------------------------------------------------
# Wave 4: Main DB retention, WAL checkpoint, janitor wiring
# ---------------------------------------------------------------------------


async def test_world_state_planning_journal_experience_and_store_retention(
    settings, store, message
):
    now = _ts("2026-11-27T12:00:00")
    day = 86400.0

    world = WorldStateManager(settings, store, clock=lambda: now)
    await world.initialize()
    journal = PlannerJournal(store)
    await journal.initialize(now)

    # Seed old & fresh global_events (+ trigger populates life_event_order)
    ev_old_normal = await world.publish_event(
        "bili", "watched", {"title": "old"}, importance="normal", now=now - 35 * day
    )
    await world.mark_event_consumed(ev_old_normal.id)
    ev_mid_high = await world.publish_event(
        "mc", "mc_rare_find", {"block": "diamond"}, importance="high", now=now - 45 * day
    )
    await world.mark_event_consumed(ev_mid_high.id)
    ev_old_high = await world.publish_event(
        "mc", "mc_rare_find", {"block": "ancient"}, importance="high", now=now - 95 * day
    )
    await world.mark_event_consumed(ev_old_high.id)
    ev_recent = await world.publish_event(
        "bili", "watched", {"title": "fresh"}, importance="normal", now=now - 2 * day
    )
    await world.mark_event_consumed(ev_recent.id)

    # Seed old terminal intent & fresh pending intent
    old_intent_id = await world.enqueue_intent(
        IntentTicketCreate(
            target_module="qq",
            action_type="speak",
            execute_after=now - 40 * day,
            expire_at=now - 39 * day,
        ),
        now=now - 40 * day,
    )
    await world.mark_intent(old_intent_id, "done", now=now - 40 * day, expected="pending")
    pending_intent_id = await world.enqueue_intent(
        IntentTicketCreate(
            target_module="qq",
            action_type="speak",
            execute_after=now + 60,
            expire_at=now + 3600,
        ),
        now=now - 40 * day,
    )

    # Seed old & fresh life_rule_receipts
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO life_rule_receipts VALUES ('r1', ?, 'k1', ?, 'enqueued', NULL)",
            (ev_old_normal.id, now - 40 * day),
        )
        await store.db.execute(
            "INSERT INTO life_rule_receipts VALUES ('r1', ?, 'k2', ?, 'enqueued', NULL)",
            (ev_recent.id, now - 2 * day),
        )

    ws_counts = await world.prune(now=now, retention_days=30, milestone_days=90)
    assert ws_counts["global_events"] == 2  # ev_old_normal (>30d) + ev_old_high (>90d)
    assert ws_counts["scheduled_intents"] == 1
    assert ws_counts["life_rule_receipts"] == 1
    remaining_events = {r["id"] for r in await store.all("SELECT id FROM global_events")}
    assert remaining_events == {ev_mid_high.id, ev_recent.id}
    remaining_intents = {r["id"] for r in await store.all("SELECT id FROM scheduled_intents")}
    assert remaining_intents == {pending_intent_id}
    # life_event_order retains anchor/live rows (never empty)
    assert (await store.one("SELECT COUNT(*) FROM life_event_order"))[0] >= 2

    # Seed planning_journal tables (old > 30d vs recent)
    old_day = "2026-10-01"
    new_day = "2026-11-26"
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO life_plan_runs(id,day,started,trigger,source,outcome,next_at,events,"
            "signature,explore_at) VALUES ('run-old',?,?,'timer','llm','accepted',0,'[]','',0)",
            (old_day, now - 40 * day),
        )
        await store.db.execute(
            "INSERT INTO life_plan_runs(id,day,started,trigger,source,outcome,next_at,events,"
            "signature,explore_at) VALUES ('run-prep',?,?,'timer','llm','prepared',0,'[]','',0)",
            (old_day, now - 40 * day),
        )
        await store.db.execute(
            "INSERT INTO life_plan_runs(id,day,started,trigger,source,outcome,next_at,events,"
            "signature,explore_at) VALUES ('run-new',?,?,'timer','llm','accepted',0,'[]','',0)",
            (new_day, now - 1 * day),
        )
        await store.db.execute("INSERT INTO life_agenda_items VALUES (?, 'i-old')", (old_day,))
        await store.db.execute("INSERT INTO life_agenda_items VALUES (?, 'i-new')", (new_day,))
        await store.db.execute(
            "INSERT INTO life_observed_days VALUES (?, ?, ?)",
            (old_day, now - 40 * day, now - 40 * day),
        )
        await store.db.execute(
            "INSERT INTO life_observed_days VALUES (?, ?, ?)",
            (new_day, now - 1 * day, now - 1 * day),
        )
        await store.db.execute(
            "INSERT INTO life_observed_slots VALUES (?,'a',1,2,'lbl',NULL,NULL,NULL)", (old_day,)
        )
        await store.db.execute(
            "INSERT INTO life_observed_slots VALUES (?,'a',1,2,'lbl',NULL,NULL,NULL)", (new_day,)
        )

    pj_counts = await journal.prune(now, retention_days=30)
    assert pj_counts == {
        "life_plan_runs": 1,
        "life_agenda_items": 1,
        "life_observed_days": 1,
        "life_observed_slots": 1,
    }
    remaining_runs = {r["id"] for r in await store.all("SELECT id FROM life_plan_runs")}
    assert remaining_runs == {"run-prep", "run-new"}

    # WorldExperience confirmed non-timer world_changes > 60d pruned, baseline & recent kept
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO world_changes(receipt,at,reason,energy_before,energy_after,"
            "boredom_before,boredom_after) VALUES ('qq:old-chat',?,'chat_delivered',80,79,20,18)",
            (now - 65 * day,),
        )
        await store.db.execute(
            "INSERT INTO world_changes(receipt,at,reason,energy_before,energy_after,"
            "boredom_before,boredom_after) "
            "VALUES ('qq:recent-chat',?,'chat_delivered',79,78,18,16)",
            (now - 10 * day,),
        )
    await world.experience.prune(now, confirmed_retention_days=60)
    receipts = {r["receipt"] for r in await store.all("SELECT receipt FROM world_changes")}
    assert "initial" in receipts
    assert "qq:recent-chat" in receipts
    assert "qq:old-chat" not in receipts

    # Store.prune() orphan outbox_bubble_receipts (applied=0 > 14d) + prune_auxiliary()
    # First run prune_auxiliary BEFORE auxiliary tables exist (must never error!)
    empty_aux = await store.prune_auxiliary(now=now)
    assert all(v == 0 for v in empty_aux.values())

    async with store.lock:
        await store.db.executescript(COLAB_SCHEMA + VC_SCHEMA + VIDEO_WATCH_SCHEMA + BILI_SCHEMA)
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO outbox_bubble_receipts VALUES ('r-old-unapplied', 0, X'00', ?, 0)",
            (time.time() - 15 * day,),
        )
        await store.db.execute(
            "INSERT INTO outbox_bubble_receipts VALUES ('r-fresh-unapplied', 0, X'00', ?, 0)",
            (time.time() - 2 * day,),
        )
        await store.db.execute(
            "INSERT INTO colab_jobs"
            "(id,job_type,status,payload,created_at,finished_at,updated_at,day) "
            "VALUES ('cj-old','rvc_cover','succeeded',X'00',?,?,?,'2026-09-01')",
            (now - 70 * day, now - 70 * day, now - 70 * day),
        )
        await store.db.execute(
            "INSERT INTO colab_jobs(id,job_type,status,payload,created_at,updated_at,day) "
            "VALUES ('cj-active','rvc_cover','running',X'00',?,?,'2026-09-01')",
            (now - 70 * day, now - 70 * day),
        )
        await store.db.execute(
            "INSERT INTO voice_companion_sessions VALUES "
            "('vc-old','a','c','owner_private','',?,?,1,1,0,'ended',X'00')",
            (now - 65 * day, now - 65 * day),
        )
        await store.db.execute(
            "INSERT INTO video_watch_attempts VALUES "
            "('vw-old','2026-10-01','BV1','s','done',NULL,?,10,100)",
            (now - 35 * day,),
        )
        await store.db.execute(
            "INSERT INTO bili_patrol_runs VALUES ('bp-old','2026-09-01','s1','done',?,NULL)",
            (now - 65 * day,),
        )
        await store.db.execute(
            "INSERT INTO bili_actions VALUES ('ba-old','2026-09-01','BV1','like','done',?)",
            (now - 65 * day,),
        )

    await store.prune()
    rem_receipts = {
        r["reply_id"] for r in await store.all("SELECT reply_id FROM outbox_bubble_receipts")
    }
    assert rem_receipts == {"r-fresh-unapplied"}

    aux_counts = await store.prune_auxiliary(now=now)
    assert aux_counts == {
        "colab_jobs": 1,
        "voice_companion_sessions": 1,
        "video_watch_attempts": 1,
        "bili_patrol_runs": 1,
        "bili_actions": 1,
    }
    assert {r["id"] for r in await store.all("SELECT id FROM colab_jobs")} == {"cj-active"}

    ckpt = await store.wal_checkpoint("TRUNCATE")
    assert ckpt is not None and len(ckpt) == 3
    with pytest.raises(ValueError, match="invalid_wal_checkpoint_mode"):
        await store.wal_checkpoint("DROP TABLE jobs")


async def test_service_janitor_throttles_heavy_retention_and_prunes_moderation(
    settings, store, monkeypatch
):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    await svc.world_state.initialize()
    await svc.moderation.initialize()

    mod_calls = 0
    heavy_calls = 0
    orig_mod_prune = svc.moderation.prune

    async def spy_mod_prune():
        nonlocal mod_calls
        mod_calls += 1
        await orig_mod_prune()

    async def spy_heavy(now=None, **kw):
        nonlocal heavy_calls
        heavy_calls += 1
        return {}

    monkeypatch.setattr(svc.moderation, "prune", spy_mod_prune)
    monkeypatch.setattr(svc, "run_retention_maintenance", spy_heavy)

    sleeps = 0

    async def fake_sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("hakimi.service.asyncio.sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        await svc.janitor()
    # Runs once on initial tick, NOT again 60s later on tick 2!
    assert mod_calls == 1
    assert heavy_calls == 1


# ---------------------------------------------------------------------------
# Wave 4: Session DB retention, DualMemory purge/stats/RecallIndex, TG fold
# ---------------------------------------------------------------------------


async def test_session_reply_batch_prune_wal_dual_memory_and_recall_index_cache(
    settings, store, message, monkeypatch
):
    sessions = SessionManager(settings, store)
    msg = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "user_id": "30000"}
    )
    await sessions.remember(msg, "hello", "hi")
    snap = await sessions.snapshot(msg)

    # Seed expired outgoing_receipts, expired received_messages, old done slice, expired state,
    # and old soft-deleted profile/chronicle.
    now = time.time()
    old_ts = now - 70 * 86400.0
    async with sessions.database(msg, preserve_history=True) as (db, key):
        await db.execute("INSERT INTO outgoing_receipts VALUES ('expired-r', ?)", (now - 10,))
        await db.execute(
            "INSERT INTO received_messages VALUES ('expired-ev', ?, ?, ?, 'received')",
            (sessions.vault.seal(key + ":incoming:expired-ev", msg.model_dump()), old_ts, now - 10),
        )
        await db.execute(
            "INSERT INTO cold_history_slices VALUES ('done-old', ?, 0, 1, 2, 'fold', ?)",
            (sessions.vault.seal(key + ":cold:done-old", []), old_ts),
        )
        await db.execute("INSERT INTO memory_slice_jobs(id,status) VALUES ('done-old','done')")
        await db.execute(
            "INSERT INTO cold_history_slices VALUES ('pending-old', ?, 0, 3, 4, 'fold', ?)",
            (sessions.vault.seal(key + ":cold:pending-old", []), old_ts),
        )
        await db.execute(
            "INSERT INTO memory_slice_jobs(id,status) VALUES ('pending-old','pending')"
        )
        await db.execute(
            "INSERT INTO member_profiles VALUES ('30000','state',?, ?, 0, 0, ?, 's1')",
            (
                sessions.vault.seal(key + ":p:30000:state", {"text": "已过期的短期状态"}),
                now - 5,
                now - 100,
            ),
        )
        await db.execute(
            "INSERT INTO member_profiles VALUES ('30001','alias',?, 0, 0, 1, ?, 's1')",
            (
                sessions.vault.seal(key + ":p:30001:alias", {"text": ""}),
                now - 40 * 86400.0,
            ),
        )
        await db.execute(
            "INSERT INTO chronicle_events VALUES ('chr-live', ?, ?, 2, ?, ?, 0, 0, 's1')",
            (
                sessions.vault.seal(
                    key + ":e:chr-live",
                    {
                        "topic": "星露谷",
                        "category": "event",
                        "text": "一起种草莓收获大丰收",
                        "tags": ["星露谷"],
                    },
                ),
                sessions.vault.seal(key + ":v:chr-live", "A" * 1024),
                now,
                now,
            ),
        )

    # 1. stats_in_db excludes expired state profiles
    st = await sessions.dual.stats(msg)
    assert st["dual_entries_count"] == 1  # only chr-live, NOT expired state

    # 2. reply_batch prunes expired outgoing_receipts
    assert await sessions.reply_batch(
        msg,
        "rb-1",
        [{"message_id": "m1", "text": "batch reply"}],
        expected_epoch=snap["epoch"],
    )
    async with sessions.database(msg, preserve_history=True) as (db, _):
        async with db.execute("SELECT id FROM outgoing_receipts") as cur:
            r_ids = {r[0] for r in await cur.fetchall()}
    assert "expired-r" not in r_ids and "rb-1:ack:m1" in r_ids

    # Re-insert an expired receipt and run sessions.prune()
    async with sessions.database(msg, preserve_history=True) as (db, _):
        await db.execute("INSERT INTO outgoing_receipts VALUES ('expired-r2', ?)", (now - 10,))

    await sessions.prune()

    async with sessions.database(msg, preserve_history=True) as (db, _):
        async with db.execute("SELECT id FROM outgoing_receipts") as cur:
            assert "expired-r2" not in {r[0] for r in await cur.fetchall()}
        async with db.execute("SELECT event FROM received_messages") as cur:
            assert "expired-ev" not in {r[0] for r in await cur.fetchall()}
        async with db.execute("SELECT id FROM cold_history_slices") as cur:
            slices = {r[0] for r in await cur.fetchall()}
            assert "done-old" not in slices
            assert "pending-old" in slices
        async with db.execute("SELECT member_id,slot FROM member_profiles") as cur:
            assert await cur.fetchall() == []

    # 3. RecallIndex caches frozenset terms so prefetch() does NOT re-tokenize cards!
    assert await sessions.dual.record_chronicle(
        msg,
        topic="天文社",
        text="大家约定周末去山顶看流星雨",
        category="event",
        tags=["天文", "流星雨"],
    )
    # Prime index cache
    first_hit = await sessions.dual.prefetch(msg, "流星雨", recall=True)
    assert any("流星雨" in item for item in first_hit["chronicle_recall"])

    terms_calls = 0
    orig_terms = dual_memory.terms

    def spy_terms(text: str):
        nonlocal terms_calls
        terms_calls += 1
        return orig_terms(text)

    monkeypatch.setattr(dual_memory, "terms", spy_terms)
    second_hit = await sessions.dual.prefetch(msg, "流星雨", recall=True)
    assert any("流星雨" in item for item in second_hit["chronicle_recall"])
    # terms() called only for query (once in prefetch + once inside vector(query)), NEVER per card!
    assert terms_calls == 2

    # 4. RecallIndex.size handles frozenset without error
    idx = RecallIndex()
    idx.put("k", 1, [("id", "txt", "tag", b"\x00" * 768, 1, now, frozenset({"流星", "天文"}))])
    assert idx.bytes > 0


async def test_session_prune_maintenance_watermark_zero_opens_until_due(
    settings, store, message, monkeypatch
):
    import aiosqlite

    now = [1_800_000_000.0]
    monkeypatch.setattr("hakimi.session_manager.time.time", lambda: now[0])
    monkeypatch.setattr("hakimi.dual_memory.time.time", lambda: now[0])

    settings.memory_ttl_seconds = 30 * 86400
    sessions = SessionManager(settings, store)
    msg = message.model_copy(
        update={"kind": "group", "conversation_id": "40099", "user_id": "30099"}
    )
    await sessions.remember(msg, "hello", "world")
    assert await sessions.reply(msg, "later", "rcpt-9d", expected_epoch=0)

    connect_calls = 0
    orig_connect = aiosqlite.connect

    def spy_connect(*args, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return orig_connect(*args, **kwargs)

    monkeypatch.setattr("hakimi.session_manager.aiosqlite.connect", spy_connect)

    # Before 9-day outgoing_receipts expiry: zero SQLite opens on prune().
    await sessions.prune()
    assert connect_calls == 0

    # Advance past 9-day outgoing_receipts expiry (while 30-day recent_messages remain live):
    # maintenance watermark wakes prune(), deletes expired receipt, and refreshes cache.
    now[0] += 9 * 86400 + 1.0
    await sessions.prune()
    assert connect_calls == 1

    # Immediate subsequent prune() goes back to zero opens (post-checkpoint fingerprint cached).
    await sessions.prune()
    assert connect_calls == 1

    # Verify the expired receipt is gone while recent_messages survived.
    monkeypatch.setattr("hakimi.session_manager.aiosqlite.connect", orig_connect)
    async with sessions.database(msg, preserve_history=True, readonly=True) as (db, _):
        async with db.execute("SELECT id FROM outgoing_receipts") as cur:
            assert await cur.fetchall() == []
        async with db.execute("SELECT COUNT(*) FROM recent_messages") as cur:
            assert (await cur.fetchone())[0] >= 2


async def test_dual_memory_commit_reclaims_expired_state_under_max_entries(
    settings, store, message, monkeypatch
):
    monkeypatch.setattr(dual_memory, "MAX_ENTRIES", 2)
    sessions = SessionManager(settings, store)
    msg = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "user_id": "30000"}
    )
    await sessions.remember(msg, "我今天很开心", "太好了")
    async with sessions.database(msg, preserve_history=True) as (db, key):
        now = time.time()
        await db.execute(
            "INSERT INTO member_profiles VALUES ('30001','state',?, ?, 0, 0, ?, 's0')",
            (sessions.vault.seal(key + ":p:30001:state", {"text": "old1"}), now - 10, now - 20),
        )
        await db.execute(
            "INSERT INTO member_profiles VALUES ('30002','state',?, ?, 0, 0, ?, 's0')",
            (sessions.vault.seal(key + ":p:30002:state", {"text": "old2"}), now - 10, now - 20),
        )
        await db.execute(
            "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
            (
                sessions.vault.seal(
                    key,
                    {
                        "role": "user",
                        "user_id": "30000",
                        "speaker": "A",
                        "text": "我喜欢喝红茶",
                        "event_id": "ev-tea",
                    },
                ),
                now + 86400,
            ),
        )
    await sessions.dual.snapshot_cold(msg, force=True)
    job = await sessions.dual.claim(msg)
    assert job is not None
    tea_seq = next(r["seq"] for r in job["messages"] if "红茶" in r.get("text", ""))
    ext = Extraction.model_validate(
        {
            "profiles": [
                {
                    "member_id": "30000",
                    "slot": "preferences",
                    "text": "喜欢喝红茶",
                    "evidence_seq": tea_seq,
                    "quote": "我喜欢喝红茶",
                    "basis": "self_report",
                }
            ]
        }
    )
    assert await sessions.dual.commit(msg, job, ext)
    recalled = await sessions.dual.prefetch(msg, "红茶")
    assert recalled["speaker_profile"] == {"preferences": "喜欢喝红茶"}


async def test_session_fold_history_passes_platform_and_timezone(settings, store, monkeypatch):
    settings.prefix_history_enabled = True
    settings.history_high_tokens = 20
    settings.bot_timezone = "Asia/Tokyo"
    sessions = SessionManager(settings, store)

    tg_msg = Message(
        platform="telegram",
        bot_id=TG_BOT_ID,
        event_id="tg-fold-1",
        user_id=TG_OWNER_ID,
        kind="private",
        conversation_id=TG_OWNER_ID,
        text="hi",
        addressed=True,
        created_at=time.time(),
    )
    seen_kwargs: list[tuple[str, str, str]] = []
    from hakimi import session_manager as sm_mod

    orig_history_lines = sm_mod.history_lines

    def spy_history_lines(messages, self_id="", timezone_name="Asia/Shanghai", **kw):
        seen_kwargs.append((self_id, timezone_name, kw.get("platform", "")))
        return orig_history_lines(messages, self_id, timezone_name, **kw)

    monkeypatch.setattr(sm_mod, "history_lines", spy_history_lines)
    await sessions.remember(tg_msg, "第一句长一点的测试文本用来触发折叠" * 3, "回复文本" * 3)
    await sessions.remember(tg_msg, "第二句长一点的测试文本用来触发折叠" * 3, "第二条回复" * 3)
    assert seen_kwargs
    assert all(
        sid == TG_BOT_ID and tz == "Asia/Tokyo" and plat == "telegram"
        for sid, tz, plat in seen_kwargs
    )


# ---------------------------------------------------------------------------
# Wave 4: sessions/ backup coverage in DatabaseBackup
# ---------------------------------------------------------------------------


async def test_db_backup_sessions_archive_local_drive_retention_and_size_guard(
    settings, store, message, monkeypatch
):
    roaming_settings = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": FOLDER}
    )
    sessions = SessionManager(roaming_settings, store)
    await sessions.remember(message, "主人私聊内容", "哈基米回复内容")
    group_msg = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    await sessions.remember(group_msg, "群聊消息", "群聊回复")

    backups_dir = Path(roaming_settings.data_dir) / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    old_sess_local = backups_dir / "hakimi-sessions-2026-11-01.tar.gz.enc"
    old_sess_local.write_bytes(b"stale-local-sessions")

    db_marker = store.vault.digest("db-backup", roaming_settings.namespace)
    sess_marker = store.vault.digest("sessions-backup", roaming_settings.namespace)
    uploaded_names: list[str] = []
    deleted_ids: list[str] = []

    def handler(request: httpx.Request):
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(
                200,
                json={
                    "id": FOLDER,
                    "mimeType": "application/vnd.google-apps.folder",
                    "driveId": "shared-drive-id",
                    "capabilities": {"canAddChildren": True},
                    "trashed": False,
                },
            )
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            q = request.url.params["q"]
            if DRIVE_SESSIONS_PROP in q:
                return httpx.Response(
                    200,
                    json={
                        "files": [
                            {
                                "id": "sess-old-id",
                                "name": "hakimi-sessions-2026-11-01.tar.gz.enc",
                                "parents": [FOLDER],
                                "mimeType": "application/octet-stream",
                                "appProperties": {DRIVE_SESSIONS_PROP: sess_marker},
                                "trashed": False,
                            }
                        ]
                    },
                )
            return httpx.Response(200, json={"files": []})
        if request.method == "GET" and request.url.path == "/drive/v3/files/sess-old-id":
            return httpx.Response(
                200,
                json={
                    "id": "sess-old-id",
                    "name": "hakimi-sessions-2026-11-01.tar.gz.enc",
                    "parents": [FOLDER],
                    "mimeType": "application/octet-stream",
                    "appProperties": {DRIVE_SESSIONS_PROP: sess_marker},
                    "trashed": False,
                },
                headers={"ETag": '"v1"'},
            )
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content.decode("latin1")
            if "hakimi-sessions-" in raw:
                uploaded_names.append("sessions")
                return httpx.Response(
                    200,
                    json={
                        "id": "sess-new-id",
                        "name": "hakimi-sessions-2026-11-17.tar.gz.enc",
                        "parents": [FOLDER],
                        "mimeType": "application/octet-stream",
                        "appProperties": {DRIVE_SESSIONS_PROP: sess_marker},
                        "trashed": False,
                    },
                )
            uploaded_names.append("main")
            return httpx.Response(
                200,
                json={
                    "id": "db-new-id",
                    "name": "db-backup-2026-11-17.sqlite3.gz.enc",
                    "parents": [FOLDER],
                    "mimeType": "application/octet-stream",
                    "appProperties": {DRIVE_BACKUP_PROP: db_marker},
                    "trashed": False,
                },
            )
        if request.method == "DELETE" and str(request.url).startswith(f"{API}/"):
            deleted_ids.append(request.url.path.rsplit("/", 1)[-1])
            return httpx.Response(204)
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming_settings, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok
        backup = DatabaseBackup(roaming_settings, store, drive, keep_days=7)

        res = await backup.snapshot_once(now=_ts("2026-11-17T10:00:00"), upload_drive=True)
        assert res["drive_uploaded"] is True
        sb = res["sessions_backup"]
        assert sb is not None
        assert sb["filename"] == "hakimi-sessions-2026-11-17.tar.gz.enc"
        assert sb["session_count"] == 2
        assert sb["drive_uploaded"] is True
        assert sb["drive_file_id"] == "sess-new-id"
        assert sb["pruned_local"] == ["hakimi-sessions-2026-11-01.tar.gz.enc"]
        assert sb["pruned_drive"] == ["hakimi-sessions-2026-11-01.tar.gz.enc"]
        assert not old_sess_local.exists()
        assert uploaded_names == ["main", "sessions"]
        assert deleted_ids == ["sess-old-id"]

        sess_path = Path(sb["path"])
        sess_mode = await asyncio.to_thread(lambda: stat.S_IMODE(sess_path.stat().st_mode))
        assert sess_mode == 0o600
        verified = await asyncio.to_thread(
            verify_sessions_backup,
            sess_path,
            roaming_settings.encryption_key.get_secret_value(),
        )
        assert verified["ok"] is True and verified["session_count"] == 2

        # Size ceiling skips sessions Drive upload without failing main DB backup!
        monkeypatch.setattr(db_backup, "MAX_SESSIONS_BACKUP_UPLOAD_BYTES", 32)
        uploaded_names.clear()
        res_big = await backup.snapshot_once(now=_ts("2026-11-17T12:00:00"), upload_drive=True)
        assert res_big["drive_uploaded"] is True
        assert res_big["sessions_backup"]["drive_uploaded"] is False
        assert res_big["sessions_backup"]["drive_skipped_reason"] == "sessions_backup_too_large"
        assert uploaded_names == ["main"]


# ---------------------------------------------------------------------------
# Wave 4: SurfMirror retention, offline TG session render, warning logs, redact
# ---------------------------------------------------------------------------


async def test_surf_mirror_retention_offline_telegram_and_decrypt_warning(settings, store, caplog):
    tg_settings = settings.model_copy(
        update={
            "onebot_enabled": False,
            "owner_qq_id": "",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=TG_BOT_ID,
                    user_id=TG_OWNER_ID,
                    notebook=True,
                    mirror=True,
                )
            },
        }
    )
    svc = Service(tg_settings, store, FakeGateway(), FakeBili(), FakeSender())
    now = _ts("2026-11-27T15:00:00")

    # Seed local Telegram Owner session DB while Telegram plugin is NOT connected (chat_module=None)
    tg_owner = Message(
        platform="telegram",
        bot_id=TG_BOT_ID,
        user_id=TG_OWNER_ID,
        conversation_id=TG_OWNER_ID,
        kind="private",
        text="",
        addressed=True,
        event_id="tg-seed-1",
        created_at=now,
    )
    await svc.sessions.remember(tg_owner, "TG断线前聊的星露谷攻略", "记住啦喵")
    # Inject a corrupted rolling_state blob to verify warning log (no secrets/plaintext)
    async with svc.sessions.database(tg_owner, preserve_history=True) as (db, _):
        await db.execute(
            "UPDATE rolling_state SET summary=?, expires=?", (b"corrupted-cipher", now + 86400)
        )

    # Seed old daily files and old .mirror_state.json keys (> 30d)
    mirror_dir = Path(tg_settings.data_dir) / "surf_mirror"
    daily_dir = mirror_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    (daily_dir / "2026-10-01.md").write_text("# old", encoding="utf-8")
    (daily_dir / "2026-11-20.md").write_text("# recent", encoding="utf-8")
    (mirror_dir / ".mirror_state.json").write_text(
        json.dumps(
            {
                "local_digests": {"latest": "x", "2026-10-01": "old", "2026-11-20": "rec"},
                "drive_digests": {"latest": "x", "2026-10-01": "old", "2026-11-20": "rec"},
                "drive_file_ids": {
                    "surf-mirror-latest.md": "fid-l",
                    "surf-mirror-2026-10-01.md": "fid-old",
                    "surf-mirror-2026-11-20.md": "fid-rec",
                },
            }
        ),
        encoding="utf-8",
    )

    mirror = SurfMirror(svc, keep_days=30)
    with caplog.at_level(logging.WARNING, logger="hakimi"):
        res = await mirror.export_once(now=now, upload_drive=False)

    latest_md = await asyncio.to_thread(Path(res["latest_path"]).read_text, encoding="utf-8")
    assert "### telegram" in latest_md
    assert "TG断线前聊的星露谷攻略" in latest_md
    assert res["pruned_daily"] == ["2026-10-01.md"]
    assert not (daily_dir / "2026-10-01.md").exists()
    assert (daily_dir / "2026-11-20.md").exists()

    state_raw = await asyncio.to_thread(
        (mirror_dir / ".mirror_state.json").read_text, encoding="utf-8"
    )
    state = json.loads(state_raw)
    assert "2026-10-01" not in state["local_digests"]
    assert "2026-11-20" in state["local_digests"]
    assert "2026-10-01" not in state["drive_digests"]
    assert "surf-mirror-2026-10-01.md" not in state["drive_file_ids"]
    assert "surf-mirror-latest.md" in state["drive_file_ids"]
    assert any(
        "event=surf_mirror_decrypt_failed section=rolling_summary" in rec.getMessage()
        for rec in caplog.records
    )


def test_centralized_secret_redaction_helper(settings):
    secret = settings.gemini_api_key.get_secret_value()
    raw = (
        f"key={secret} Bearer tok_abc12345 sk-1234567890abcdef1234 "
        "-----BEGIN RSA PRIVATE KEY----- [role=OWNER]"
    )
    redacted = redact_secrets(raw, settings, neutralize_role_tags=True)
    for leaked in (secret, "tok_abc12345", "sk-1234567890abcdef1234", "BEGIN RSA PRIVATE KEY"):
        assert leaked not in redacted
    assert "[role=" not in redacted
    assert "[untrusted_role=" in redacted
    assert secret not in cloud_sanitize(raw, settings)
    assert secret not in sanitize_mirror_text(raw, settings)


# ---------------------------------------------------------------------------
# Wave 4: Telegram alignment — DualMemory.view, MemoryHub, Moderation,
# PublicMemory, ScreenWatch & VoiceCompanion
# ---------------------------------------------------------------------------


async def test_dual_memory_view_reads_remote_chat_config_memory_mirror_enabled(settings, store):
    sessions = SessionManager(settings, store)
    tg_group = Message(
        platform="telegram",
        bot_id=TG_BOT_ID,
        event_id="tg-g-1",
        user_id="90001",
        kind="group",
        conversation_id="-100123456",
        text="hello",
        addressed=True,
        created_at=time.time(),
    )
    assert (await sessions.dual.view(tg_group))["mirror_enabled"] is False

    await save_remote_chat_config(
        store,
        "telegram",
        RemoteChatConfig(allowed_groups=("-100123456",), memory_mirror_enabled=True),
        settings=settings,
    )
    # Works for both non-existent session DB and existing session DB with NULL per-session override!
    assert (await sessions.dual.view(tg_group))["mirror_enabled"] is True
    await sessions.remember(tg_group, "hi", "hello")
    assert (await sessions.dual.view(tg_group))["mirror_enabled"] is True


async def test_memory_hub_planner_merges_all_owner_platforms_and_logs_errors(settings, caplog):
    now = _ts("2026-11-27T16:00:00")
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "onebot_owner_ids": {"20000"},
            "owner_delivery_platform": "onebot",
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        hub = svc.memory_hub
        hub.clock = lambda: now

        ob_owner = owner_target(svc.s, "onebot", now, "ob-1")
        tg_owner = owner_target(svc.s, "telegram", now, "tg-1")
        assert ob_owner is not None and tg_owner is not None

        for target, summary, u_txt, offset in (
            (ob_owner, "QQ聊了论文进度", "QQ这边刚改完论文引言", 600.0),
            (tg_owner, "TG约了晚饭火锅", "TG这边晚上想吃火锅", 300.0),
        ):
            async with svc.sessions.database(target) as (db, key):
                await db.execute("UPDATE session_meta SET enabled=1")
                await db.execute(
                    "UPDATE rolling_state SET summary=?",
                    (svc.sessions.vault.seal(key, summary),),
                )
                await db.execute(
                    "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                    (
                        svc.sessions.vault.seal(
                            key, {"role": "user", "text": u_txt, "created_at": now - offset}
                        ),
                        now + 86400,
                    ),
                )

        # Default planner recall (audience=None) includes BOTH OneBot and Telegram sessions!
        merged = await hub.recall_context(None, "", mode="planner", now=now)
        assert "QQ聊了论文进度" in merged["rolling_summary"]
        assert "TG约了晚饭火锅" in merged["rolling_summary"]
        assert any("改完论文引言" in t for t in merged["recent_turns"])
        assert any("想吃火锅" in t for t in merged["recent_turns"])

        # Explicit audience still isolates to that platform only
        only_tg = await hub.recall_context(tg_owner, "", mode="planner", now=now)
        assert "TG约了晚饭火锅" in only_tg["rolling_summary"]
        assert "QQ聊了论文进度" not in only_tg["rolling_summary"]

        # Error logging in voice/screen/write paths (content-free, logs exc type)
        async def broken_post(*args, **kwargs):
            raise RuntimeError("secret-payload-never-logged")

        svc.event_ledger.post = broken_post  # type: ignore[method-assign]
        with caplog.at_level(logging.WARNING, logger="hakimi"):
            res = await hub.record_experience(
                "test_src",
                "摘要",
                visibility="public",
                event_type="ev",
                write_ledger=True,
                now=now,
            )
        assert res.ledger_event_id is None
        assert any(
            "event=memory_hub_ledger_post_failed exc=RuntimeError" in r.getMessage()
            for r in caplog.records
        )
        assert "secret-payload-never-logged" not in caplog.text


async def test_moderation_status_exposes_cross_platform_muted_users(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
            },
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        await svc.moderation.set_muted("30000", True)
        await svc.moderation.set_platform_muted("telegram", "998877", True, source="owner_manual")
        st = await svc.moderation.status()
        assert len(st["muted_users"]) == 1
        assert st["muted_users"][0]["platform"] == "onebot"
        assert st["muted_users"][0]["target"] == "onebot:30000"
        assert len(st["platform_muted_users"]) == 1
        pm = st["platform_muted_users"][0]
        assert pm["platform"] == "telegram"
        assert pm["user_id"] == "998877"
        assert pm["target"] == "telegram:998877"
        assert pm["source"] == "owner_manual"
        assert pm["delete_status"] == "not_supported"
        assert pm["person_id"].startswith("p_")

        # Unmute clears platform_muted_users
        await svc.moderation.set_platform_muted("telegram", "998877", False)
        st2 = await svc.moderation.status()
        assert st2["platform_muted_users"] == []


async def test_public_memory_platform_aware_speakers(settings, store, message):
    settings.social_owner_bindings = {
        "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_ID)
    }
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    svc.modules.register(_FakeTelegramModule(svc.s))
    pm = svc.public_memory

    qq_group = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "user_id": "30000", "text": "QQ群"}
    )
    assert await pm.record("rep-qq", qq_group, [qq_group], "回复QQ", expected_epoch=0)
    qq_recent = await pm.recent_for(message)
    assert len(qq_recent) == 1
    assert qq_recent[0]["speakers"][0]["platform"] == "onebot"
    assert qq_recent[0]["speakers"][0]["user_id"] == "30000"
    assert qq_recent[0]["speakers"][0]["qq"] == "30000"

    tg_group = Message(
        platform="telegram",
        bot_id=TG_BOT_ID,
        event_id="tg-pub-1",
        user_id="888001",
        kind="group",
        conversation_id="-100999",
        text="TG群消息",
        addressed=True,
        created_at=time.time(),
        sender_nickname="Alice",
    )
    assert await pm.record("rep-tg", tg_group, [tg_group], "回复TG", expected_epoch=0)
    tg_owner = owner_target(svc.s, "telegram", time.time(), "tg-owner-read")
    tg_recent = await pm.recent_for(tg_owner)
    assert len(tg_recent) == 1
    sp = tg_recent[0]["speakers"][0]
    assert sp["platform"] == "telegram"
    assert sp["user_id"] == "888001"
    assert "qq" not in sp


async def test_screen_watch_and_voice_companion_pure_telegram_owner_and_init_lock(settings):
    now = [1_795_500_000.0]
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": False,
            "owner_qq_id": "",
            "owner_delivery_platform": "telegram",
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=TG_BOT_ID,
                    user_id=TG_OWNER_ID,
                    notebook=True,
                    mirror=True,
                )
            },
            "screen_ws_enabled": True,
            "screen_ws_token": SecretStr("screen-watch-secret-token-32-chars-minimum"),
            "companion_ws_enabled": True,
            "companion_ws_token": SecretStr("companion-test-secret-token-32-chars-min"),
        }
    )
    gw = _StreamMockGateway(deltas=["TG主人看屏测试成功！"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.modules.register(_FakeTelegramModule(svc.s))
        sw = svc.screen_watch
        vc = svc.voice_companion
        sw.clock = lambda: now[0]
        vc.clock = lambda: now[0]

        # Concurrent initialize() calls are safe under _init_lock
        sw.initialized = False
        vc.initialized = False
        await asyncio.gather(sw.initialize(), sw.initialize(), vc.initialize(), vc.initialize())
        assert sw.initialized and vc.initialized

        sw_sock = _SocketDouble()
        sw_task = asyncio.create_task(
            sw.serve(
                sw_sock.receive,
                sw_sock.send,
                sw_sock.close,
                source_ip="127.0.0.1",
                adapter_id="win",
                client_id="c1",
            )
        )
        await sw_sock.push({"type": "watch.start", "game_title": "星露谷"})
        await sw_sock.recv_until(lambda m: m["type"] == "watch.ready")
        await sw_sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
        await sw_sock.recv_until(lambda m: m["type"] == "frame.ack")

        imgs, game = sw.latest_frames(owner_only=True, now=now[0])
        assert len(imgs) == 1 and game == "星露谷"

        vc_sock = _SocketDouble()
        vc_task = asyncio.create_task(
            vc.serve(
                vc_sock.receive,
                vc_sock.send,
                vc_sock.close,
                source_ip="127.0.0.1",
                adapter_id="desktop",
                client_id="v1",
            )
        )
        await vc_sock.push(
            {"type": "session.start", "mode": "owner_private", "game_title": "星露谷"}
        )
        ready = (await vc_sock.recv_until(lambda m: m["type"] == "session.ready"))[-1]
        assert ready["mode"] == "owner_private"
        assert vc.has_active_owner_session() is True

        await vc_sock.push({"type": "speech.turn", "turn_id": "tg-vc-1", "text": "今天收成不错"})
        ev = await vc_sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "tg-vc-1"
        )
        assert ev[-1]["status"] == "spoken"
        _payload, subject, kw = gw.calls[-1]
        assert subject == f"voice:owner:telegram:{TG_OWNER_ID}"
        assert kw["message"].platform == "telegram"
        assert kw["message"].user_id == TG_OWNER_ID

        await vc_sock.push({"type": "session.stop"})
        await vc_sock.recv_until(lambda m: m["type"] == "session.ended")
        await vc_sock.close()
        await sw_sock.close()
        await asyncio.gather(vc_task, sw_task)
