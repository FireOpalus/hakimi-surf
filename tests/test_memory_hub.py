"""Tests for MemoryHub resident SystemModule, privacy barriers, 4 recall modes, and delegation."""

from __future__ import annotations

import asyncio
import copy
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.models import Message
from hakimi.plugin_bus import RESERVED_PLUGIN_IDS
from hakimi.security import Rejected
from hakimi.social import owner_target
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_delivery_resilience import TINY_PNG_B64
from tests.test_owner_cloud_memory import _populate_memory
from tests.test_plugin_bus import _PluginSocket
from tests.test_screen_watch import _screen_settings
from tests.test_voice_companion import (
    _companion_settings,
    _snapshot_dir,
    _SocketDouble,
    _StreamMockGateway,
)

TZ = ZoneInfo("Asia/Shanghai")


def _ts(iso: str) -> float:
    return datetime.fromisoformat(iso).replace(tzinfo=TZ).timestamp()


def _hub_settings(settings, **update):
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "onebot_owner_ids": {"20000"},
            "onebot_allowed_users": {"30000"},
            "onebot_allowed_groups": {"40000", "50000"},
            **update,
        }
    )


async def _seed_owner_chat_session(
    svc,
    owner_msg: Message,
    *,
    now: float,
    enabled: bool = True,
    summary: str = "上周约好一起打副本",
) -> None:
    async with svc.sessions.database(owner_msg) as (db, key):
        await db.execute("UPDATE session_meta SET enabled=?", (1 if enabled else 0,))
        if summary:
            await db.execute(
                "UPDATE rolling_state SET summary=?",
                (svc.sessions.vault.seal(key, summary),),
            )
        rows = [
            {
                "role": "user",
                "text": "三天前的旧消息不该出现在planner里",
                "created_at": now - 200_000.0,
            },
            {
                "role": "user",
                "text": "/status",
                "created_at": now - 1000.0,
            },
            {
                "role": "user",
                "text": "[协议事件] ignored",
                "created_at": now - 900.0,
            },
            {
                "role": "user",
                "text": "今晚八点一起打莱万汀基建",
                "created_at": now - 600.0,
            },
            {
                "role": "assistant",
                "text": "好呀，八点准时等你！",
                "created_at": now - 590.0,
            },
        ]
        for item in rows:
            await db.execute(
                "INSERT INTO recent_messages(payload, expires) VALUES (?, ?)",
                (svc.sessions.vault.seal(key, item), now + 86400.0),
            )


async def test_memory_hub_system_module_admin_readonly_and_reserved_id(settings):
    s = _hub_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert "memory_hub" in svc.modules.systems
        assert svc.memory_hub is svc.modules.systems["memory_hub"].hub
        assert svc.modules.get("memory_hub") is None
        assert "memory_hub" in RESERVED_PLUGIN_IDS

        with pytest.raises(Rejected, match="reserved_plugin_id"):
            await svc.plugin_bus.create_plugin("memory_hub", "Fake Hub", ["actions"])

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            unauth = await client.get("/admin/api/system-modules", headers={"Authorization": ""})
            assert unauth.status_code == 401

            mods = {
                item["id"]: item for item in (await client.get("/admin/api/system-modules")).json()
            }
            assert "memory_hub" in mods
            mod = mods["memory_hub"]
            assert mod["panel"] is None
            assert "enabled" not in mod
            assert mod["status"]["state"] == "ready"
            assert not gw.calls

            toggle = await client.put(
                "/admin/api/life/modules/memory_hub/enabled", json={"enabled": False}
            )
            assert toggle.status_code == 404


async def test_memory_hub_writes_scrubbing_privacy_barrier_and_cloud_readonly(settings, tmp_path):
    now = 1_795_000_000.0
    mem_dir = _populate_memory(tmp_path, settings.gemini_api_key.get_secret_value())
    before_mem = _snapshot_dir(mem_dir)
    s = _hub_settings(
        settings,
        enable_antigravity_memory=True,
        antigravity_memory_dir=mem_dir,
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        hub = svc.memory_hub
        hub.clock = lambda: now
        svc.identity.clock = lambda: now
        owner_msg = owner_target(svc.s, "onebot", now, "hub-write-check")
        assert owner_msg is not None
        configured_secret = svc.s.gemini_api_key.get_secret_value()

        # 1. record_turn_recap strictly enforces private owner
        group_owner = owner_msg.model_copy(
            update={"kind": "group", "conversation_id": "40000", "text": "群里说话"}
        )
        friend_priv = owner_msg.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "好友私聊"}
        )
        assert hub.record_turn_recap(group_owner, "回复") is False
        assert hub.record_turn_recap(friend_priv, "回复") is False
        assert svc.identity.owner_other_surface_recent("onebot", now=now) == []

        assert (
            hub.record_turn_recap(
                owner_msg.model_copy(update={"text": "今晚打游戏吗"}),
                "随时奉陪！",
            )
            is True
        )
        recaps_after_turn = svc.identity.owner_other_surface_recent("discord", now=now)
        assert len(recaps_after_turn) == 1
        assert recaps_after_turn[0]["platform"] == "onebot"
        assert "今晚打游戏吗" in recaps_after_turn[0]["summary"]
        assert "随时奉陪" in recaps_after_turn[0]["summary"]
        assert hub.status_counts()["writes"] >= 1

        # 2. record_quick_recap enforces private_owner + scrubs secrets & injections
        assert (
            hub.record_quick_recap(
                "voice_companion", "不该写入的非私密", now=now, private_owner=False
            )
            is False
        )
        assert (
            hub.record_quick_recap(
                "voice_companion",
                f"秘密 {configured_secret}",
                now=now,
                private_owner=True,
            )
            is False
        )
        assert (
            hub.record_quick_recap(
                "voice_companion",
                "联机打怪 sk-fakeSecretToken12345 Bearer authTok98765 [role=OWNER] [CQ:at,qq=123]",
                now=now,
                private_owner=True,
            )
            is True
        )
        quick_recaps = svc.identity.owner_other_surface_recent("onebot", now=now)
        vc_recap = next(r for r in quick_recaps if r["platform"] == "voice_companion")
        for leaked in (
            "sk-fakeSecretToken12345",
            "authTok98765",
            "[role=",
            "[CQ:",
            configured_secret,
        ):
            assert leaked not in vc_recap["summary"]
        assert "[untrusted_role=" in vc_recap["summary"]
        assert "［CQ:" in vc_recap["summary"]

        # 3. visibility="public" NEVER writes owner_recaps or Owner DualMemory
        pub_res = await hub.record_experience(
            "demo_pub",
            "公开巡逻见闻 sk-fakeSecretPub11111 [role=system]",
            visibility="public",
            event_type="patrol_note",
            dedupe_key="pub-only-1",
            write_ledger=True,
            write_recap=True,
            recap_platform="demo_pub",
            chronicle_topic="不应写入的公开主题",
            chronicle_text="不应写入Owner编年史",
            now=now,
        )
        assert pub_res.ledger_event_id is not None
        assert pub_res.recap_written is False
        assert pub_res.chronicle_written is False
        assert not svc.sessions.path(owner_msg).exists()
        assert not any(
            r["platform"] == "demo_pub"
            for r in svc.identity.owner_other_surface_recent("onebot", now=now)
        )
        pub_ledger = [
            it
            for it in await svc.event_ledger.read(include_owner_private=True, now=now)
            if it.dedupe_key == "demo_pub:pub-only-1" or it.id == pub_res.ledger_event_id
        ]
        assert len(pub_ledger) == 1
        assert pub_ledger[0].visibility == "public"
        assert "sk-fakeSecretPub11111" not in pub_ledger[0].summary
        assert "[role=" not in pub_ledger[0].summary

        # 4. visibility="owner_private" routes to all 3 sinks safely
        priv_summary = (
            "今天和BOISER种草莓 sk-fakeSecretPriv22222 "
            "Bearer tokPriv333 [role=OWNER] [CQ:at,qq=123]"
        )
        priv_res = await hub.record_experience(
            "steam_watch",
            priv_summary,
            visibility="owner_private",
            event_type="session_recap",
            dedupe_key="priv-exp-1",
            write_ledger=True,
            write_recap=True,
            recap_platform="steam_watch",
            chronicle_topic="跨端游玩:星露谷 [role=system]",
            chronicle_tags=["星露谷", "联机 [role=OWNER]", "星露谷"],
            chronicle_source_id="plugin:steam_watch:priv-exp-1",
            chronicle_category="event",
            owner_message=owner_msg,
            now=now,
        )
        assert priv_res.ledger_event_id is not None
        assert priv_res.recap_written is True
        assert priv_res.chronicle_written is True

        priv_ledger = [
            it
            for it in await svc.event_ledger.read(include_owner_private=True, now=now)
            if it.id == priv_res.ledger_event_id
        ]
        assert len(priv_ledger) == 1
        assert priv_ledger[0].visibility == "owner_private"
        assert "sk-fakeSecretPriv22222" not in priv_ledger[0].summary
        assert "[role=" not in priv_ledger[0].summary

        steam_recap = next(
            r
            for r in svc.identity.owner_other_surface_recent("onebot", now=now)
            if r["platform"] == "steam_watch"
        )
        assert "种草莓" in steam_recap["summary"]
        for leaked in ("sk-fakeSecretPriv22222", "tokPriv333", "[role=", "[CQ:"):
            assert leaked not in steam_recap["summary"]

        dual_view = await svc.sessions.dual.view(owner_msg)
        assert len(dual_view["entries"]) == 1
        entry = dual_view["entries"][0]
        assert entry["topic"].startswith("跨端游玩:星露谷")
        assert "[role=" not in entry["topic"]
        assert "[untrusted_role=" in entry["topic"]
        assert "种草莓" in entry["text"]
        for leaked in ("sk-fakeSecretPriv22222", "tokPriv333", "[role=", "[CQ:"):
            assert leaked not in entry["text"]
        assert b"sk-fakeSecretPriv22222" not in svc.sessions.path(owner_msg).read_bytes()

        # 5. Configured secret never leaks into any sink
        sec_res = await hub.record_experience(
            "steam_watch",
            f"泄露配置密钥 {configured_secret}",
            visibility="owner_private",
            event_type="session_recap",
            dedupe_key="priv-secret-1",
            write_ledger=True,
            write_recap=True,
            chronicle_topic="不应写入秘密主题",
            owner_message=owner_msg,
            now=now,
        )
        assert sec_res.recap_written is False
        assert sec_res.chronicle_written is False
        assert len((await svc.sessions.dual.view(owner_msg))["entries"]) == 1
        if sec_res.ledger_event_id is not None:
            sec_items = [
                it
                for it in await svc.event_ledger.read(include_owner_private=True, now=now)
                if it.id == sec_res.ledger_event_id
            ]
            assert sec_items and configured_secret not in sec_items[0].summary

        assert _snapshot_dir(mem_dir) == before_mem


async def test_memory_hub_recall_context_all_four_modes_and_isolation(settings, tmp_path):
    now = _ts("2026-11-20T14:00:00")
    mem_dir = _populate_memory(tmp_path, settings.gemini_api_key.get_secret_value())
    before_mem = _snapshot_dir(mem_dir)
    s = _hub_settings(
        settings,
        enable_antigravity_memory=True,
        antigravity_memory_dir=mem_dir,
        enable_notebook_snapshot=True,
        notebook_snapshot_path=tmp_path / "owner-nb.md",
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        hub = svc.memory_hub
        hub.clock = lambda: now
        svc.identity.clock = lambda: now
        svc.notebook.text = "OWNER-NOTEBOOK-SENTINEL-" + ("x" * 900)

        owner_msg = owner_target(svc.s, "onebot", now, "hub-recall-check")
        assert owner_msg is not None
        group_msg = owner_msg.model_copy(
            update={"kind": "group", "conversation_id": "40000", "text": "群里聊聊阿尔吉侬"}
        )
        friend_msg = owner_msg.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "好友聊聊阿尔吉侬"}
        )

        # Planner mode before owner session exists returns {} without creating an empty DB
        assert await hub.recall_context(None, "", mode="planner", now=now) == {}
        assert not svc.sessions.path(owner_msg).exists()

        # Seed owner private session, dual memory, ledger, quick recap, and public memory
        await _seed_owner_chat_session(svc, owner_msg, now=now, enabled=True)
        await svc.sessions.dual.record_chronicle(
            owner_msg,
            topic="基建规划",
            text="莱万汀电力站优先级最高",
            tags=["终末地", "基建"],
            source_id="seed:1",
            created_at=now,
        )
        await svc.event_ledger.post(
            "farm",
            "crop_ready",
            "西边地里草莓明天熟了",
            dedupe_key="farm:berry",
            visibility="owner_private",
            now=now,
        )
        assert (
            hub.record_quick_recap(
                "steam_watch", "刚才联机挖了两个小时矿", now=now, private_owner=True
            )
            is True
        )
        await svc.public_memory.record(
            "pub-1",
            group_msg,
            [group_msg],
            "群里刚刚聊到开源项目评测",
            expected_epoch=0,
        )
        await svc.sessions.dual.record_chronicle(
            group_msg,
            topic="群聊公开梗",
            text="大家约定晚上十点打游戏",
            tags=["群聊"],
            source_id="seed:grp",
            created_at=now,
        )
        async with svc.sessions.database(group_msg) as (_db, _):
            pass

        recalls_before = hub.status_counts()["recalls"]

        # Mode 1: chat
        assert (
            await hub.recall_context(owner_msg, "阿尔吉侬", mode="chat", sandbox=True, now=now)
            == {}
        )
        assert hub.status_counts()["recalls"] == recalls_before
        assert await hub.recall_context(None, "阿尔吉侬", mode="chat", now=now) == {}

        chat_owner = await hub.recall_context(
            owner_msg, "阿尔吉侬 莱万汀", mode="chat", recall=True, now=now
        )
        assert "OWNER-NOTEBOOK-SENTINEL" in chat_owner["owner_notebook_snapshot"]
        assert "owner_core_memory" in chat_owner
        assert "owner_recalled_memories" in chat_owner
        assert chat_owner["近期群聊巡逻记忆（只读背景）"]
        assert any(
            item["platform"] == "steam_watch" for item in chat_owner["owner_other_surface_recent"]
        )
        assert "dual_memory_data" in chat_owner

        for non_owner_ctx in (
            await hub.recall_context(
                group_msg, "阿尔吉侬", mode="chat", allow_dual_profile=False, now=now
            ),
            await hub.recall_context(friend_msg, "阿尔吉侬", mode="chat", now=now),
        ):
            for forbidden in (
                "owner_notebook_snapshot",
                "owner_core_memory",
                "owner_recalled_memories",
                "近期群聊巡逻记忆（只读背景）",
                "owner_other_surface_recent",
            ):
                assert forbidden not in non_owner_ctx
            dumped = json.dumps(non_owner_ctx, ensure_ascii=False)
            assert "OWNER-NOTEBOOK-SENTINEL" not in dumped
            assert "永恒契约" not in dumped
        grp_only = await hub.recall_context(
            group_msg, "打游戏", mode="chat", allow_dual_profile=False, now=now
        )
        if isinstance(grp_only.get("dual_memory_data"), dict):
            assert "speaker_profile" not in grp_only["dual_memory_data"]
            assert "member_profiles" not in grp_only["dual_memory_data"]

        # Mode 2: voice
        assert await hub.recall_context(group_msg, "莱万汀", mode="voice", now=now) == {}
        assert await hub.recall_context(friend_msg, "莱万汀", mode="voice", now=now) == {}
        assert await hub.recall_context(None, "莱万汀", mode="voice", now=now) == {}

        voice_owner = await hub.recall_context(owner_msg, "莱万汀", mode="voice", now=now)
        assert len(voice_owner["owner_notebook_snapshot"]) <= 800
        assert "OWNER-NOTEBOOK-SENTINEL" in voice_owner["owner_notebook_snapshot"]
        assert "owner_core_memory" in voice_owner
        assert any("今晚八点一起打莱万汀基建" in t for t in voice_owner["recent_qq_turns"])
        assert all(
            "/status" not in t and "[协议事件]" not in t for t in voice_owner["recent_qq_turns"]
        )
        assert any(
            "莱万汀" in item for item in voice_owner["owner_dual_memory"]["chronicle_recall"]
        )
        assert any(
            it["summary"] == "西边地里草莓明天熟了" for it in voice_owner["pending_ledger_events"]
        )

        # Mode 3: screen
        assert await hub.recall_context(group_msg, "", mode="screen", now=now) == {}
        assert await hub.recall_context(friend_msg, "", mode="screen", now=now) == {}
        assert await hub.recall_context(None, "", mode="screen", now=now) == {}

        screen_owner = await hub.recall_context(owner_msg, "", mode="screen", now=now)
        assert len(screen_owner["owner_notebook_snapshot"]) <= 600
        assert "OWNER-NOTEBOOK-SENTINEL" in screen_owner["owner_notebook_snapshot"]
        assert any(
            it["summary"] == "西边地里草莓明天熟了" for it in screen_owner["pending_ledger_events"]
        )
        assert "owner_dual_memory" not in screen_owner

        # Mode 4: planner
        planner_owner = await hub.recall_context(None, "", mode="planner", now=now)
        assert "上周约好一起打副本" in planner_owner["rolling_summary"]
        assert any("今晚八点一起打莱万汀基建" in t for t in planner_owner["recent_turns"])
        assert all(
            bad not in json.dumps(planner_owner["recent_turns"], ensure_ascii=False)
            for bad in ("旧消息不该出现", "/status", "[协议事件]")
        )

        # Disabled session blocks planner recall
        async with svc.sessions.database(owner_msg, preserve_history=True) as (db, _):
            await db.execute("UPDATE session_meta SET enabled=0")
        assert await hub.recall_context(None, "", mode="planner", now=now) == {}

        assert hub.status_counts()["recalls"] > recalls_before
        assert _snapshot_dir(mem_dir) == before_mem


async def test_memory_hub_end_to_end_delegation_across_all_callers(settings, tmp_path, monkeypatch):
    now = [_ts("2026-11-20T15:00:00")]
    mem_dir = _populate_memory(tmp_path, settings.gemini_api_key.get_secret_value())
    before_mem = _snapshot_dir(mem_dir)
    s = _companion_settings(
        _screen_settings(settings),
        admin_token=SecretStr(TOKEN),
        owner_qq_id="20000",
        onebot_owner_ids={"20000"},
        onebot_allowed_users={"30000"},
        onebot_allowed_groups={"40000"},
        enable_antigravity_memory=True,
        antigravity_memory_dir=mem_dir,
        enable_notebook_snapshot=True,
        notebook_snapshot_path=tmp_path / "owner-nb.md",
    )

    class _MultiGateway(_StreamMockGateway):
        async def generate(self, payload, subject, **kwargs):
            snapshot = copy.deepcopy(payload)
            if kwargs.get("life_plan"):
                self.calls.append((snapshot, subject, kwargs))
                return json.dumps(
                    {
                        "current_module": "idle",
                        "focus_level": "LOW",
                        "mood": "calm",
                        "mood_longnick": "",
                        "rest_minutes": 0,
                        "intents": [],
                        "agenda_changes": [],
                        "ledger_posts": [],
                        "ledger_consumes": [],
                    },
                    ensure_ascii=False,
                )
            res = await super().generate(snapshot, subject, **kwargs)
            return res

    gw = _MultiGateway(deltas=["好的呀，马上帮你看！"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        hub = svc.memory_hub
        hub.clock = lambda: now[0]
        svc.identity.clock = lambda: now[0]
        svc.plugin_bus.clock = lambda: now[0]
        svc.voice_companion.clock = lambda: now[0]
        svc.screen_watch.clock = lambda: now[0]
        svc.notebook.text = "DELEGATION-NOTEBOOK-MARKER"
        await svc.world_state.update_state(mood="curious", energy=82)
        await svc.event_ledger.post(
            "farm",
            "crop_ready",
            "DELEGATION-LEDGER-MARKER",
            dedupe_key="farm:del-1",
            visibility="owner_private",
            now=now[0],
        )

        recorded: list[tuple[str, tuple, dict]] = []
        orig_recall = hub.recall_context
        orig_rec_exp = hub.record_experience
        orig_quick = hub.record_quick_recap
        orig_turn = hub.record_turn_recap

        async def spy_recall(*args, **kwargs):
            recorded.append(("recall_context", args, kwargs))
            return await orig_recall(*args, **kwargs)

        async def spy_rec_exp(*args, **kwargs):
            recorded.append(("record_experience", args, kwargs))
            return await orig_rec_exp(*args, **kwargs)

        def spy_quick(*args, **kwargs):
            recorded.append(("record_quick_recap", args, kwargs))
            return orig_quick(*args, **kwargs)

        def spy_turn(*args, **kwargs):
            recorded.append(("record_turn_recap", args, kwargs))
            return orig_turn(*args, **kwargs)

        monkeypatch.setattr(hub, "recall_context", spy_recall)
        monkeypatch.setattr(hub, "record_experience", spy_rec_exp)
        monkeypatch.setattr(hub, "record_quick_recap", spy_quick)
        monkeypatch.setattr(hub, "record_turn_recap", spy_turn)

        # 1. voice_companion: owner_private delegates; room_public stays isolated
        vc_sock = _SocketDouble()
        vc_task = asyncio.create_task(
            svc.voice_companion.serve(
                vc_sock.receive,
                vc_sock.send,
                vc_sock.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="vc-del",
            )
        )
        await vc_sock.push(
            {"type": "session.start", "mode": "owner_private", "game_title": "终末地"}
        )
        await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
        await vc_sock.push({"type": "speech.turn", "turn_id": "vt-1", "text": "帮我看莱万汀"})
        await vc_sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "vt-1")
        assert any(
            name == "recall_context" and kw.get("mode") == "voice" for name, _, kw in recorded
        )
        assert any(
            name == "record_quick_recap" and args[0] == "voice_companion"
            for name, args, _ in recorded
        )
        assert "DELEGATION-NOTEBOOK-MARKER" in gw.calls[-1][0]["owner_notebook_snapshot"]

        await vc_sock.push({"type": "session.stop"})
        await vc_sock.recv_until(lambda m: m.get("type") == "session.ended")
        assert any(
            name == "record_experience" and args[0] == "voice_companion"
            for name, args, _ in recorded
        )

        recorded.clear()
        await vc_sock.push({"type": "session.start", "mode": "room_public", "game_title": "Apex"})
        await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
        await vc_sock.push(
            {"type": "speech.turn", "turn_id": "vt-pub", "speaker_id": "777", "text": "跳哪"}
        )
        await vc_sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "vt-pub")
        await vc_sock.push({"type": "session.stop"})
        await vc_sock.recv_until(lambda m: m.get("type") == "session.ended")
        assert not any(
            name in {"recall_context", "record_quick_recap", "record_experience"}
            for name, _, _ in recorded
        )
        assert "DELEGATION-NOTEBOOK-MARKER" not in json.dumps(gw.calls[-1][0], ensure_ascii=False)
        await vc_sock.close()
        await asyncio.wait_for(vc_task, 2.0)

        # 2. screen_watch: delegates recall_context(mode="screen") + appends world_state
        await svc.world_state.update_state(mood="curious", energy=82)
        recorded.clear()
        sw_sock = _SocketDouble()
        sw_task = asyncio.create_task(
            svc.screen_watch.serve(
                sw_sock.receive,
                sw_sock.send,
                sw_sock.close,
                source_ip="127.0.0.1",
                adapter_id="win_cap",
                client_id="sw-del",
            )
        )
        await sw_sock.push({"type": "watch.start", "game_title": "终末地"})
        await sw_sock.recv_until(lambda m: m["type"] == "watch.ready")
        now[0] += 30.0
        await sw_sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "sw-1",
            }
        )
        await sw_sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "sw-1"
        )
        assert any(
            name == "recall_context" and kw.get("mode") == "screen" for name, _, kw in recorded
        )
        sw_payload = gw.calls[-1][0]
        assert "DELEGATION-NOTEBOOK-MARKER" in sw_payload["owner_notebook_snapshot"]
        assert any(
            it["summary"] == "DELEGATION-LEDGER-MARKER"
            for it in sw_payload["pending_ledger_events"]
        )
        assert sw_payload["world_state"] == {"mood": "curious", "energy": 82}
        await sw_sock.close()
        await asyncio.wait_for(sw_task, 2.0)

        # 3. life_engine: delegates recall_context(mode="planner")
        recorded.clear()
        owner_msg = owner_target(svc.s, "onebot", now[0], "del-planner")
        assert owner_msg is not None
        await _seed_owner_chat_session(svc, owner_msg, now=now[0], enabled=True)
        engine = LifeEngine(svc, clock=lambda: now[0])
        await engine.initialize()
        await engine.trigger_plan_once(now=now[0])
        assert any(
            name == "recall_context" and kw.get("mode") == "planner" for name, _, kw in recorded
        )
        plan_payload = gw.calls[-1][0]
        assert any(
            "今晚八点一起打莱万汀基建" in t
            for t in plan_payload["owner_recent_context"]["recent_turns"]
        )

        # 4. plugin_bus: world.event with owner_recap=True delegates record_experience
        recorded.clear()
        await svc.plugin_bus.create_plugin("steam_watch", "游戏观测", ["sensors"])
        pb_sock = _PluginSocket()
        pb_task = asyncio.create_task(
            svc.plugin_bus.serve(
                pb_sock.receive,
                pb_sock.send,
                pb_sock.close,
                source_ip="127.0.0.1",
                plugin_id="steam_watch",
            )
        )
        await pb_sock.push(
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "游戏观测",
                "scopes": ["sensors"],
            }
        )
        await pb_sock.recv_until(lambda m: m["type"] == "plugin.ready")
        await pb_sock.push(
            {
                "type": "world.event",
                "id": "we-del-1",
                "event_type": "session_recap",
                "business_key": "del-recap",
                "visibility": "public",
                "owner_recap": True,
                "chronicle_topic": "跨端联动测试",
                "summary": "插件总线回流测试摘要",
            }
        )
        ack = (await pb_sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
        assert ack["accepted"] is True
        assert any(
            name == "record_experience" and args[0] == "steam_watch" for name, args, _ in recorded
        )
        await pb_sock.close(1000)
        await asyncio.wait_for(pb_task, 2.0)

        # 5. service: handle_message delegates recall_context(mode="chat") + record_turn_recap
        recorded.clear()
        incoming = Message(
            platform="onebot",
            bot_id=svc.s.onebot_self_id,
            event_id="svc-del-1",
            user_id="20000",
            kind="private",
            conversation_id="20000",
            text="聊聊阿尔吉侬和刚才的游戏",
            addressed=True,
            created_at=time.time(),
        )
        assert await svc.accept(incoming) == "accepted"
        job = await svc.store.claim()
        assert job is not None
        await svc.process(job)
        assert any(
            name == "recall_context" and kw.get("mode") == "chat" for name, _, kw in recorded
        )
        assert any(name == "record_turn_recap" for name, _, _ in recorded)
        svc_payload = gw.calls[-1][0]
        assert "DELEGATION-NOTEBOOK-MARKER" in svc_payload["owner_notebook_snapshot"]
        assert any(
            item["platform"] == "steam_watch" for item in svc_payload["owner_other_surface_recent"]
        )

        # Sandbox mode must never write turn recaps or leak private owner memory
        recorded.clear()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            sb = await client.post(
                "/admin/api/sandbox",
                json={"identity": "OWNER", "scene": "private", "text": "沙盒测试"},
            )
            assert sb.status_code == 200
        assert not any(name == "record_turn_recap" for name, _, _ in recorded)
        assert "DELEGATION-NOTEBOOK-MARKER" not in json.dumps(gw.calls[-1][0], ensure_ascii=False)

        assert _snapshot_dir(mem_dir) == before_mem
