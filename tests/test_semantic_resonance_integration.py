"""End-to-end integration tests for Pillars A + B + C Wave 2 wiring."""

from __future__ import annotations

import asyncio
import json
import stat
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.db_backup import (
    DRIVE_BACKUP_PROP,
    DRIVE_OWNER_MEMORY_PROP,
    DatabaseBackup,
    verify_encrypted_backup,
)
from hakimi.drive_client import UPLOAD, FolderDrive
from hakimi.dual_memory import Extraction
from hakimi.embeddings import pack_semantic_vector
from hakimi.models import Message
from hakimi.owner_memory import OwnerMemoryStore
from hakimi.service import Service
from hakimi.work_usage import model_work
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN

ZONE = ZoneInfo("Asia/Shanghai")


def _ts(hour: int, minute: int = 0, day: int = 20) -> float:
    return datetime(2026, 11, day, hour, minute, tzinfo=ZONE).timestamp()


class _FakeEmbedder:
    """Deterministic local semantic embedder producing real E1 768-dim blobs."""

    def __init__(self, *, dim: int = 8) -> None:
        self.dim = dim
        self.calls: list[dict[str, Any]] = []

    def _vec(self, text: str) -> bytes:
        lower = text.lower()
        buckets = [0.05] * self.dim
        if any(k in lower for k in ("丝之歌", "空洞骑士", "silksong")):
            buckets[0] += 1.0
        if any(k in lower for k in ("冰岛", "极光")):
            buckets[1] += 1.0
        if any(k in lower for k in ("红石", "电梯", "minecraft")):
            buckets[2] += 1.0
        if any(k in lower for k in ("胶片", "哈苏", "摄影")):
            buckets[3] += 1.0
        return pack_semantic_vector(buckets)

    async def embed_one(
        self,
        text: str,
        *,
        task_type: str = "RETRIEVAL_QUERY",
        budget_lane: str = "memory",
        subject: str = "embeddings",
        purpose: str = "memory_embed",
    ) -> bytes | None:
        self.calls.append(
            {"text": text, "task_type": task_type, "budget_lane": budget_lane, "purpose": purpose}
        )
        return self._vec(text)

    async def embed_texts(
        self,
        texts: list[str] | tuple[str, ...],
        *,
        task_type: str = "RETRIEVAL_DOCUMENT",
        budget_lane: str = "memory",
        subject: str = "embeddings",
        purpose: str = "memory_embed",
    ) -> list[bytes | None]:
        out: list[bytes | None] = []
        for text in texts:
            self.calls.append(
                {
                    "text": text,
                    "task_type": task_type,
                    "budget_lane": budget_lane,
                    "purpose": purpose,
                }
            )
            out.append(self._vec(text))
        return out


class _GatewayWithEmbedder(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.embeddings = _FakeEmbedder()


def _wire_settings(settings: Any) -> Any:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_mode": "http",
            "onebot_api_url": "http://127.0.0.1:3000",
            "onebot_self_id": "10000",
            "owner_qq_id": "20000",
            "onebot_allowed_groups": {"40000"},
            "qq_enabled": False,
            "proactive_dm_enabled": True,
            "quiet_hours": "23:30-08:30",
            "group_policies": {"onebot:40000": "at_only"},
        }
    )


# ---------------------------------------------------------------------------
# 1. Store usage category "embedding" classification + conservation invariant
# ---------------------------------------------------------------------------


async def test_store_usage_category_classifies_embedding_work_across_lanes(store):
    with model_work("embedding.memory_embed", "emb-m1", independent=True):
        rid1 = await store.reserve("embeddings", "memory", Decimal("0.00012"))
        await store.settle(rid1, Decimal("0.00009"), provider="vertex_paid")
    with model_work("embedding.share_resonance", "emb-p1", independent=True):
        rid2 = await store.reserve("embeddings", "proactive", Decimal("0.00010"))
        await store.settle(rid2, Decimal("0.00006"), provider="vertex_paid")
    with model_work("world.plan", "wp-1", independent=True):
        rid3 = await store.reserve("world:planner", "proactive", Decimal("0.00050"))
        await store.settle(rid3, Decimal("0.00040"), provider="vertex_paid")

    overview = await store.usage_overview()
    full = await store.usage()
    assert overview["categories"] == full["categories"]
    by_cat = {c["category"]: c for c in overview["categories"]}
    assert by_cat["embedding"]["amount"] == 150
    assert by_cat["embedding"]["calls"] == 2
    assert by_cat["world_plan"]["amount"] == 400
    assert by_cat["world_plan"]["calls"] == 1
    assert sum(c["amount"] for c in overview["categories"]) == sum(
        row["amount"] for row in overview["lanes"]
    )
    assert sum(c["calls"] for c in overview["categories"]) == sum(
        row["calls"] for row in overview["lanes"]
    )


# ---------------------------------------------------------------------------
# 2. App lifespan wiring + public event subscription + admin endpoints
# ---------------------------------------------------------------------------


async def test_app_lifespan_wiring_public_event_resonance_and_admin_api(settings):
    s = _wire_settings(settings)
    gw = _GatewayWithEmbedder()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        svc = app.state.service
        now_ts = _ts(15, 0)
        svc.world_state.clock = lambda: now_ts
        svc.life_engine.clock = lambda: now_ts
        svc.share_resonance.clock = lambda: now_ts
        qq_mod = svc.modules.get("qq_social")
        if qq_mod is not None:
            qq_mod.connected = lambda: True

        # Verify embedder + Zone 2 owner_memory + share_resonance all wired & initialized
        assert svc.embedder is gw.embeddings
        assert svc.sessions.embedder is gw.embeddings
        assert svc.cloud_memory.embedder is gw.embeddings
        assert svc.owner_memory._initialized is True
        assert svc.share_resonance.ready is True
        assert svc.memory_hub.owner_memory is svc.owner_memory

        # Seed Owner Zone 2 memory & Group A Zone 1 memory
        await svc.owner_memory.record(
            source="owner_chat",
            topic="丝之歌和冰岛",
            text="BOISER私密计划：等丝之歌发售顺便去冰岛看极光",
            entities=("丝之歌", "冰岛"),
            now=now_ts - 300,
        )
        grp_msg = Message(
            platform="onebot",
            bot_id="10000",
            event_id="grp-seed-1",
            user_id="30001",
            conversation_id="40000",
            kind="group",
            text="大家都在等丝之歌发售",
            addressed=True,
            created_at=now_ts - 600,
        )
        await svc.sessions.remember(grp_msg, "大家都在等丝之歌发售", "我也超期待丝之歌！")
        await svc.sessions.dual.snapshot_cold(grp_msg, force=True)
        job = await svc.sessions.dual.claim(grp_msg)
        ext = Extraction.model_validate(
            {
                "profiles": [],
                "chronicles": [
                    {
                        "topic": "丝之歌讨论",
                        "category": "event",
                        "text": "群友们聊过空洞骑士丝之歌的实机演示和期待",
                        "tags": ["丝之歌"],
                        "evidence_seqs": [1],
                    }
                ],
            }
        )
        assert await svc.sessions.dual.commit(grp_msg, job, ext)

        # Publish a private_first event -> MUST NOT enter public resonance
        await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {
                "bvid": "BV1PRIVATE01",
                "title": "丝之歌私密视频",
                "summary": "私聊点播不应该广播",
                "private_first": True,
            },
            now=now_ts,
        )
        if svc.background_tasks:
            await asyncio.gather(*list(svc.background_tasks))
        assert (await svc.store.one("SELECT COUNT(*) FROM pending_shares"))[0] == 0

        # Publish a public Bilibili event -> triggers non-blocking background resonance probe!
        await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {
                "bvid": "BV1PUBLIC001",
                "title": "空洞骑士丝之歌全新实机演示",
                "summary": "空洞骑士丝之歌放出最新Boss战实机演示",
            },
            now=now_ts,
        )
        assert svc.background_tasks  # non-blocking tracked background task spawned
        await asyncio.gather(*list(svc.background_tasks))

        # Both Owner and Group A got strictly isolated pending shares + resonance tickets!
        owner_pending = await svc.share_resonance.pending_for_context("owner:20000", now=now_ts)
        group_pending = await svc.share_resonance.pending_for_context("onebot:40000", now=now_ts)
        assert len(owner_pending) == 1
        assert len(group_pending) == 1
        assert "冰岛" not in json.dumps(group_pending, ensure_ascii=False)
        assert "冰岛" in json.dumps(owner_pending, ensure_ascii=False)

        # Admin API exposes decoupled quotas, resonance_enabled, and read-only resonance/owner stats
        st_res = await client.get("/admin/api/life")
        assert st_res.status_code == 200
        body = st_res.json()
        assert body["config"]["resonance_enabled"] is True
        assert body["config"]["max_owner_proactive_per_day"] is None
        assert body["config"]["max_group_proactive_per_day"] is None
        assert body["resonance"]["ready"] is True
        assert body["resonance"]["pending_count"] == 2
        assert body["resonance"]["owner_pending_count"] == 1
        assert body["resonance"]["group_pending_count"] == 1
        assert body["owner_memory"]["ready"] is True
        assert body["owner_memory"]["active_entries"] >= 1
        # Zero plaintext memory content leaks in admin resonance/owner_memory status
        admin_status_json = json.dumps(
            {"resonance": body["resonance"], "owner_memory": body["owner_memory"]},
            ensure_ascii=False,
        )
        assert "冰岛" not in admin_status_json

        # Admin API updates decoupled quotas & resonance_enabled cleanly
        put_res = await client.put(
            "/admin/api/life/schedule",
            json={
                "max_owner_proactive_per_day": 8,
                "max_group_proactive_per_day": 2,
                "resonance_enabled": False,
            },
        )
        assert put_res.status_code == 200
        updated = put_res.json()
        assert updated["config"]["max_owner_proactive_per_day"] == 8
        assert updated["config"]["max_group_proactive_per_day"] == 2
        assert updated["config"]["resonance_enabled"] is False
        assert updated["resonance"]["enabled"] is False


# ---------------------------------------------------------------------------
# 3. service_turn inbound engagement + pending_for_context isolation + janitor
# ---------------------------------------------------------------------------


async def test_service_turn_inbound_engagement_pending_context_and_maintenance(settings, store):
    s = _wire_settings(settings)
    gw = _GatewayWithEmbedder()
    svc = Service(s, store, gw, FakeBili(), FakeSender())
    now_ref = [_ts(15, 0)]
    svc.world_state.clock = lambda: now_ref[0]
    svc.proactive.clock = lambda: now_ref[0]
    svc.life_engine.clock = lambda: now_ref[0]
    svc.share_resonance.clock = lambda: now_ref[0]
    qq_mod = svc.modules.get("qq_social")
    if qq_mod is not None:
        qq_mod.connected = lambda: True

    await svc.owner_memory.initialize()
    await svc.world_state.initialize()
    await svc.life_engine.initialize()
    await svc.proactive.initialize()
    await svc.share_resonance.initialize()

    try:
        # Lower social battery so we observe engagement recovery on Group A reply
        async with store.transaction():
            await store.db.execute("UPDATE world_social_battery SET charge=60.0 WHERE id=1")

        # Seed a recent proactive share for Group A (5 min ago) so inbound reply triggers engagement
        await svc.share_resonance.record_shared("onebot:40000", now=now_ref[0] - 300)

        # Seed warm pending shares during quiet hours (Owner vs Group A)
        quiet_ts = _ts(1, 0)
        await svc.owner_memory.record(
            source="owner_chat",
            topic="哈苏胶片",
            text="BOISER私密愿望：买一台哈苏500CM胶片相机",
            entities=("哈苏", "胶片"),
            now=quiet_ts - 1000,
        )
        grp_seed = Message(
            platform="onebot",
            bot_id="10000",
            event_id="grp-hist-1",
            user_id="30002",
            conversation_id="40000",
            kind="group",
            text="红石电梯真好用",
            addressed=True,
            created_at=now_ref[0] - 1000,
        )
        await svc.sessions.remember(grp_seed, "红石电梯真好用", "确实超方便！")
        await svc.sessions.dual.snapshot_cold(grp_seed, force=True)
        job_seed = await svc.sessions.dual.claim(grp_seed)
        await svc.sessions.dual.commit(
            grp_seed,
            job_seed,
            Extraction.model_validate(
                {
                    "profiles": [],
                    "chronicles": [
                        {
                            "topic": "红石电梯",
                            "category": "event",
                            "text": "群里讨论过无声红石电梯搭建技巧",
                            "tags": ["红石电梯"],
                            "evidence_seqs": [1],
                        }
                    ],
                }
            ),
        )

        # Create quiet-hours warm pending shares (01:00 -> kept warm, not immediately enqueued)
        await svc.share_resonance.on_public_experience(
            source="bilibili_surf",
            event_type="bili_video_watched",
            title="哈苏500CM胶片实拍",
            summary="用哈苏中画幅胶片拍日落",
            ref="BV1FILM0001",
            novelty=0.6,
            now=quiet_ts,
        )
        await svc.share_resonance.on_public_experience(
            source="bilibili_surf",
            event_type="bili_video_watched",
            title="红石无声电梯教程",
            summary="超紧凑红石电梯设计",
            ref="BV1RED00001",
            novelty=0.7,
            now=quiet_ts,
        )

        # Inbound Group A chat turn at 15:00
        grp_in = Message(
            platform="onebot",
            bot_id="10000",
            event_id="grp-reply-1",
            user_id="30002",
            conversation_id="40000",
            kind="group",
            text="哈哈刚才你分享的太好玩啦！",
            addressed=True,
            created_at=time.time(),
        )
        await svc.handle_message(
            {
                "id": "job-grp-1",
                "kind": "message",
                "scope": store.session_scope(grp_in),
                "subject": store.subject(grp_in),
                "expires": time.time() + 600,
                "payload": grp_in.model_dump(),
            }
        )

        # Engagement credited + battery recovered!
        eng_a = await svc.share_resonance.engagement_state("onebot:40000")
        assert eng_a["streak"] == 1
        assert eng_a["boost"] > 0.0
        assert await svc.world_state.experience.battery(grp_in, now_ref[0]) > 60.0

        # Group A generation payload includes ONLY Group A warm pending share (NEVER Owner's!)
        gen_calls = [c for c in gw.calls if c[0] == "generate"]
        assert len(gen_calls) == 1
        grp_payload = gen_calls[0][1]
        assert "pending_resonance_shares" in grp_payload
        assert any(p["ref"] == "BV1RED00001" for p in grp_payload["pending_resonance_shares"])
        assert "BV1FILM0001" not in json.dumps(grp_payload, ensure_ascii=False)
        assert "500CM" not in json.dumps(grp_payload, ensure_ascii=False)

        # Inbound Owner DM turn at 15:01 receives Owner's warm pending share
        owner_in = Message(
            platform="onebot",
            bot_id="10000",
            event_id="owner-dm-1",
            user_id="20000",
            conversation_id="20000",
            kind="private",
            text="哈基米在干嘛呀",
            addressed=True,
            created_at=time.time(),
        )
        await svc.handle_message(
            {
                "id": "job-owner-1",
                "kind": "message",
                "scope": store.session_scope(owner_in),
                "subject": store.subject(owner_in),
                "expires": time.time() + 600,
                "payload": owner_in.model_dump(),
            }
        )
        owner_gen = [c for c in gw.calls if c[0] == "generate"][-1][1]
        assert any(p["ref"] == "BV1FILM0001" for p in owner_gen["pending_resonance_shares"])

        # Rate-limited semantic maintenance promotes due shares & runs bounded embedding backfill
        maint1 = await svc.run_semantic_maintenance(now_ref[0] + 400)
        assert maint1["promoted"] >= 1
        # Immediate repeat within rate-limit window is a zero-work no-op
        maint2 = await svc.run_semantic_maintenance(now_ref[0] + 410)
        assert maint2 == {
            "promoted": 0,
            "pruned": 0,
            "backfill_sessions": 0,
            "backfill_owner": 0,
            "backfill_cloud": 0,
        }
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# 4. Zone 2 owner_memory.sqlite3 encrypted backup, rotation, and Drive sync
# ---------------------------------------------------------------------------


async def test_db_backup_includes_zone2_owner_memory_with_rotation_and_drive(
    settings, store, tmp_path
):
    folder = "backup-folder-zone2"
    roaming = settings.model_copy(
        update={"roaming_enabled": True, "gdrive_roaming_folder_id": folder}
    )
    om = OwnerMemoryStore(roaming, vault=store.vault)
    await om.initialize()
    try:
        await om.record(
            source="owner_chat",
            topic="备份测试",
            text="验证 Zone 2 owner_memory.sqlite3 加密备份与恢复",
            entities=("备份",),
        )
    finally:
        await om.close()

    # Seed an old expired local owner-memory backup to confirm 7-day rotation
    backups_dir = Path(roaming.data_dir) / "backups"
    old_om_file = backups_dir / "hakimi-owner-memory-2026-11-01.sqlite3.gz.enc"

    def _seed_old() -> None:
        backups_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        old_om_file.write_bytes(b"expired-owner-memory-backup")

    await asyncio.to_thread(_seed_old)

    db_marker = store.vault.digest("db-backup", roaming.namespace)
    om_marker = store.vault.digest("owner-memory-backup", roaming.namespace)
    uploaded_names: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/drive/v3/files/{folder}":
            return httpx.Response(
                200,
                json={
                    "id": folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "driveId": "shared-drive-id",
                    "capabilities": {"canAddChildren": True},
                    "trashed": False,
                },
            )
        if request.method == "GET" and request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": []})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content.decode("latin1")
            if "hakimi-owner-memory-" in raw:
                uploaded_names.append("owner_memory")
                return httpx.Response(
                    200,
                    json={
                        "id": "om-20",
                        "name": "hakimi-owner-memory-2026-11-20.sqlite3.gz.enc",
                        "parents": [folder],
                        "mimeType": "application/octet-stream",
                        "appProperties": {DRIVE_OWNER_MEMORY_PROP: om_marker},
                        "trashed": False,
                    },
                )
            uploaded_names.append("main")
            return httpx.Response(
                200,
                json={
                    "id": "main-20",
                    "name": "db-backup-2026-11-20.sqlite3.gz.enc",
                    "parents": [folder],
                    "mimeType": "application/octet-stream",
                    "appProperties": {DRIVE_BACKUP_PROP: db_marker},
                    "trashed": False,
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(roaming, client, "unused")

        async def _tok():
            return "test-token"

        drive._access_token = _tok
        backup = DatabaseBackup(roaming, store, drive, keep_days=7)
        res = await backup.snapshot_once(now=_ts(12, 0, day=20), upload_drive=True)

    assert sorted(uploaded_names) == ["main", "owner_memory"]
    om_sum = res["owner_memory_backup"]
    assert om_sum is not None
    assert om_sum["drive_uploaded"] is True
    assert om_sum["drive_file_id"] == "om-20"
    assert om_sum["pruned_local"] == ["hakimi-owner-memory-2026-11-01.sqlite3.gz.enc"]

    enc_path = Path(om_sum["path"])

    def _verify_files() -> tuple[bool, bool, int, dict[str, Any]]:
        old_exists = old_om_file.exists()
        new_exists = enc_path.exists()
        mode = stat.S_IMODE(enc_path.stat().st_mode)
        verified_info = verify_encrypted_backup(
            enc_path, roaming.encryption_key.get_secret_value(), work_dir=tmp_path
        )
        return old_exists, new_exists, mode, verified_info

    old_exists, new_exists, mode, verified = await asyncio.to_thread(_verify_files)
    assert not old_exists
    assert new_exists
    assert mode == 0o600
    assert verified["ok"] is True
