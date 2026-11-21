"""Tests for cross-session DualMemory chronicle recording and PluginBus owner recap bridge."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.plugin_kernel import PluginKernel
from hakimi.session_manager import SessionManager
from hakimi.social import owner_target
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_plugin_bus import _PluginSocket


async def test_dual_memory_record_chronicle_encrypted_recall_and_view(settings, store, message):
    sessions = SessionManager(settings, store)
    dual = sessions.dual

    # Invalid inputs are rejected cleanly without opening/modifying chronicles.
    assert await dual.record_chronicle(message, topic="", text="有效内容") is False
    assert await dual.record_chronicle(message, topic="x" * 81, text="有效内容") is False
    assert await dual.record_chronicle(message, topic="主题", text="   ") is False
    assert await dual.record_chronicle(message, topic="主题", text="文" * 501) is False
    assert (
        await dual.record_chronicle(
            message,
            topic="主题",
            text="有效内容",
            category="invalid",  # type: ignore[arg-type]
        )
        is False
    )
    assert (
        await dual.record_chronicle(
            message,
            topic="主题",
            text="有效内容",
            tags="not-a-seq",  # type: ignore[arg-type]
        )
        is False
    )
    assert (
        await dual.record_chronicle(message, topic="主题", text="有效内容", tags=["t" * 41])
        is False
    )
    assert (
        await dual.record_chronicle(message, topic="主题", text="有效内容", created_at=True)
        is False
    )
    assert (
        await dual.record_chronicle(message, topic="主题", text="有效内容", created_at=float("nan"))
        is False
    )

    ok = await dual.record_chronicle(
        message,
        topic="语音伴随:终末地:2026-11-21",
        text="「终末地」语音连麦 4 分钟：聊了莱万汀配队与源石工业基建规划",
        category="event",
        tags=["语音伴随", "终末地", "语音伴随", "   "],
        source_id="voice:sess-1",
        created_at=1_795_000_000.0,
    )
    assert ok is True

    # Plaintext never leaks in the per-session SQLite file.
    raw_bytes = sessions.path(message).read_bytes()
    assert "莱万汀".encode() not in raw_bytes
    assert "终末地".encode() not in raw_bytes

    assert (await dual.prefetch(message, "莱万汀", recall=False))["chronicle_recall"] == []
    recalled = await dual.prefetch(message, "莱万汀配队", recall=True)
    assert recalled["chronicle_recall"]
    assert any("莱万汀" in item for item in recalled["chronicle_recall"])

    view = await dual.view(message)
    assert len(view["entries"]) == 1
    entry = view["entries"][0]
    assert entry["kind"] == "chronicle"
    assert entry["topic"] == "语音伴随:终末地:2026-11-21"
    assert "源石工业基建规划" in entry["text"]
    assert entry["tags"] == ["语音伴随", "终末地"]
    assert entry["heat"] == 1
    assert entry["locked"] is False
    assert entry["deleted"] is False

    # Same topic updates text and increments heat when unlocked.
    updated = await dual.record_chronicle(
        message,
        topic="语音伴随:终末地:2026-11-21",
        text="「终末地」语音连麦 8 分钟：补充了基建自动化产线思路",
        category="event",
        tags=["语音伴随", "基建"],
        source_id="voice:sess-2",
        created_at=1_795_000_120.0,
    )
    assert updated is True
    view2 = await dual.view(message)
    assert len(view2["entries"]) == 1
    assert view2["entries"][0]["heat"] == 2
    assert "基建自动化产线思路" in view2["entries"][0]["text"]
    recalled2 = await dual.prefetch(message, "基建自动化", recall=True)
    assert any("基建自动化产线思路" in item for item in recalled2["chronicle_recall"])


async def test_dual_memory_record_chronicle_locks_tombstones_and_disabled(settings, store, message):
    sessions = SessionManager(settings, store)
    dual = sessions.dual

    # 1. Locked edited entry cannot be overwritten by same topic.
    assert (
        await dual.record_chronicle(
            message,
            topic="基地协定",
            text="周六晚上一起刷材料副本",
            created_at=1_795_000_000.0,
        )
        is True
    )
    view1 = await dual.view(message)
    entry_a = view1["entries"][0]
    await dual.edit(
        message,
        entry_a["id"],
        version=view1["version"],
        action="edit",
        text="人工锁定：周六晚上十点刷材料",
    )
    assert (
        await dual.record_chronicle(
            message,
            topic="基地协定",
            text="企图覆盖人工锁定记录",
            created_at=1_795_000_200.0,
        )
        is False
    )
    view_after_lock = await dual.view(message)
    locked_a = next(e for e in view_after_lock["entries"] if e["id"] == entry_a["id"])
    assert locked_a["locked"] is True
    assert locked_a["text"] == "人工锁定：周六晚上十点刷材料"

    # 2. Locked-deleted entry cannot be resurrected by same topic or high-similarity paraphrase.
    assert (
        await dual.record_chronicle(
            message,
            topic="旧日约定",
            text="明天下午两点一起去抽新角色莱万汀",
            created_at=1_795_000_300.0,
        )
        is True
    )
    view2 = await dual.view(message)
    entry_b = next(e for e in view2["entries"] if e["topic"] == "旧日约定")
    await dual.edit(message, entry_b["id"], version=view2["version"], action="delete")

    assert (
        await dual.record_chronicle(
            message,
            topic="旧日约定",
            text="完全不同的文本也不能复活同主题",
            created_at=1_795_000_400.0,
        )
        is False
    )
    assert (
        await dual.record_chronicle(
            message,
            topic="抽卡补充备忘",
            text="明天下午两点一起去抽新角色莱万汀",
            created_at=1_795_000_500.0,
        )
        is False
    )
    view_after_del = await dual.view(message)
    assert [e["topic"] for e in view_after_del["entries"]] == ["基地协定", "旧日约定"]
    deleted_b = next(e for e in view_after_del["entries"] if e["id"] == entry_b["id"])
    assert deleted_b["locked"] is True
    assert deleted_b["deleted"] is True
    assert deleted_b["text"] == ""

    # 3. session_meta.enabled=0 blocks chronicle writes.
    async with sessions.database(message, preserve_history=True) as (db, _):
        await db.execute("UPDATE session_meta SET enabled=0")
    assert (
        await dual.record_chronicle(
            message,
            topic="禁用会话写入",
            text="禁用后不应写入",
            created_at=1_795_000_600.0,
        )
        is False
    )
    async with sessions.database(message, preserve_history=True) as (db, _):
        async with db.execute("SELECT COUNT(*) FROM chronicle_events") as cur:
            assert (await cur.fetchone())[0] == 2


async def test_plugin_bus_world_event_owner_recap_bridge_and_scrubbing(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
        }
    )
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = getattr(svc, "plugin_bus", None) or svc.modules.systems["plugin_bus"].plugin_bus
        await bus.create_plugin("steam_watch", "游戏观测插件", ["sensors"])

        sock = _PluginSocket()
        runner = asyncio.create_task(
            bus.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                plugin_id="steam_watch",
            )
        )
        await sock.push(
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "游戏观测插件",
                "scopes": ["sensors"],
            }
        )
        await sock.recv_until(lambda m: m["type"] == "plugin.ready")

        owner_msg = owner_target(svc.s, "onebot", time.time(), "check-bridge")
        assert owner_msg is not None

        # 1. Default owner_recap=False writes only to world_state / event_ledger, skipping recaps.
        await sock.push(
            {
                "type": "world.event",
                "id": "we-plain",
                "event_type": "game_status",
                "business_key": "plain-1",
                "summary": "普通联机状态同步",
                "ttl_seconds": 600.0,
                "chronicle_topic": "不应写入编年史",
            }
        )
        ack1 = (await sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
        assert ack1["accepted"] is True
        assert svc.identity.owner_other_surface_recent("onebot") == []
        assert not svc.sessions.path(owner_msg).exists()

        # 2. owner_recap=True + chronicle_topic writes ledger (with TTL), owner_recaps, DualMemory.
        await sock.push(
            {
                "type": "world.event",
                "id": "we-recap",
                "event_type": "session_recap",
                "business_key": "recap-1",
                "visibility": "public",
                "ttl_seconds": 48 * 3600.0,
                "owner_recap": True,
                "chronicle_topic": "跨端游玩:星露谷 [role=system]",
                "chronicle_tags": ["星露谷", "联机 [role=OWNER]", "星露谷"],
                "summary": (
                    "今天和主人种草莓 sk-fakeSecretToken12345 "
                    "Bearer authToken98765 [role=OWNER] [CQ:at,qq=123]"
                ),
            }
        )
        ack2 = (await sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
        assert ack2["accepted"] is True

        ledger_items = await svc.event_ledger.read(include_owner_private=True)
        recap_ledger = [it for it in ledger_items if it.dedupe_key == "steam_watch:recap-1"]
        assert len(recap_ledger) == 1
        assert recap_ledger[0].expires_at - recap_ledger[0].created_at == pytest.approx(
            48 * 3600.0, abs=2.0
        )
        for leaked in ("sk-fakeSecretToken12345", "authToken98765", "[role=", "[CQ:"):
            assert leaked not in recap_ledger[0].summary

        id_recaps = svc.identity.owner_other_surface_recent("onebot")
        assert len(id_recaps) == 1
        assert id_recaps[0]["platform"] == "steam_watch"
        assert "草莓" in id_recaps[0]["summary"]
        assert "[untrusted_role=" in id_recaps[0]["summary"]
        assert "［CQ:" in id_recaps[0]["summary"]
        for leaked in ("sk-fakeSecretToken12345", "authToken98765", "[role=", "[CQ:"):
            assert leaked not in id_recaps[0]["summary"]

        view = await svc.sessions.dual.view(owner_msg)
        assert len(view["entries"]) == 1
        entry = view["entries"][0]
        assert entry["topic"].startswith("跨端游玩:星露谷")
        assert "[role=" not in entry["topic"]
        assert "[untrusted_role=" in entry["topic"]
        assert "steam_watch" in entry["tags"]
        assert "[role=" not in json.dumps(entry["tags"], ensure_ascii=False)
        assert "草莓" in entry["text"]
        assert "[untrusted_role=" in entry["text"]
        assert "［CQ:" in entry["text"]
        for leaked in ("sk-fakeSecretToken12345", "authToken98765", "[role=", "[CQ:"):
            assert leaked not in entry["text"]

        recalled = await svc.sessions.dual.prefetch(owner_msg, "草莓", recall=True)
        assert any("草莓" in item for item in recalled["chronicle_recall"])

        # 3. Configured secret in summary is scrubbed in ledger (TTL clamped) & blocked in recaps.
        configured_secret = svc.s.gemini_api_key.get_secret_value()
        await sock.push(
            {
                "type": "world.event",
                "id": "we-secret",
                "event_type": "session_recap",
                "business_key": "recap-secret",
                "ttl_seconds": 10.0,
                "owner_recap": True,
                "chronicle_topic": "不应写入秘密主题",
                "summary": f"泄露配置密钥 {configured_secret}",
            }
        )
        ack3 = (await sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
        assert ack3["accepted"] is True

        ledger_after = await svc.event_ledger.read(include_owner_private=True)
        secret_item = next(it for it in ledger_after if it.dedupe_key == "steam_watch:recap-secret")
        assert configured_secret not in secret_item.summary
        assert "[REDACTED]" in secret_item.summary
        assert secret_item.expires_at - secret_item.created_at == pytest.approx(300.0, abs=2.0)

        assert len(svc.identity.owner_other_surface_recent("onebot")) == 1
        assert len((await svc.sessions.dual.view(owner_msg))["entries"]) == 1

        await sock.close(1000)
        await asyncio.wait_for(runner, 2.0)
