"""Tests for SocialProactivePolicy, social_speak, outgoing settlement, and PluginBus hooks."""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.hot_ring import HotRing
from hakimi.life_engine import LifeEngine
from hakimi.models import Message
from hakimi.modules.actions import ActionCall, NoArgs
from hakimi.modules.scheduling import ScheduleOptions
from hakimi.outgoing import apply_receipts, persist_ack
from hakimi.plugin_kernel import PluginKernel
from hakimi.social import OwnerBinding
from hakimi.social_policy import DESKTOP_CONTEXT_HINT, SocialProactivePolicy
from hakimi.social_speak import speak_proactive
from hakimi.world_experience import SOCIAL_LOW
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_plugin_telegram import (
    BOT_ID,
    FRIEND_UID,
    GROUP_ID,
    OWNER_UID,
    _FakeTelegramApi,
    _start_connected_plugin,
    _stop_connected_plugin,
    _wait_until,
)

NOW = datetime(2026, 11, 26, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


def _tg_settings(
    settings: Any,
    *,
    notebook: bool = True,
    proactive_dm_enabled: bool = True,
    group_policies: dict[str, str] | None = None,
    enable_antigravity_memory: bool = False,
    antigravity_memory_dir: Path | None = None,
) -> Any:
    updates: dict[str, Any] = {
        "admin_token": SecretStr(TOKEN),
        "onebot_enabled": False,
        "qq_enabled": False,
        "owner_delivery_platform": "telegram",
        "proactive_dm_enabled": proactive_dm_enabled,
        "daily_digest_enabled": True,
        "social_owner_bindings": {
            "telegram": OwnerBinding(
                bot_id=BOT_ID,
                user_id=OWNER_UID,
                notebook=notebook,
                mirror=True,
            )
        },
        "group_policies": (
            group_policies if group_policies is not None else {f"telegram:{GROUP_ID}": "at_only"}
        ),
        "enable_antigravity_memory": enable_antigravity_memory,
    }
    if antigravity_memory_dir is not None:
        updates["antigravity_memory_dir"] = antigravity_memory_dir
    return settings.model_copy(update=updates)


def _tg_group_msg(
    user_id: str,
    *,
    event_id: str,
    created_at: float = NOW - 60,
    text: str = "聊天消息",
    bot_id: str = BOT_ID,
    group_id: str = GROUP_ID,
) -> Message:
    return Message(
        platform="telegram",
        bot_id=bot_id,
        event_id=event_id,
        user_id=user_id,
        conversation_id=group_id,
        kind="group",
        text=text,
        addressed=False,
        created_at=created_at,
    )


def _ticket(
    ticket_id: str,
    *,
    target_scope: str,
    action_type: str = "speak",
    origin: str = "planner",
    gate_condition: dict[str, Any] | None = None,
    source_event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "id": ticket_id,
        "origin": origin,
        "target_module": "telegram",
        "action_type": action_type,
        "action_name": "telegram.speak",
        "target_scope": target_scope,
        "mood": "curious",
        "intent_note": {"note": "test"},
        "gate_condition": gate_condition or {},
        "source_event_id": source_event_id,
    }


async def _set_social_battery(store: Any, charge: float) -> None:
    async with store.transaction():
        await store.db.execute(
            "UPDATE world_social_battery SET charge=? WHERE id=1",
            (charge,),
        )


async def _set_last_private_at(store: Any, scope: str, ts: float) -> None:
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO owner_dm_state(scope,last_private_at) VALUES (?,?) "
            "ON CONFLICT(scope) DO UPDATE SET last_private_at=excluded.last_private_at",
            (scope, ts),
        )


# ---------------------------------------------------------------------------
# 1. SocialProactivePolicy: unauthorized, battery, session busy, group cooldown
# ---------------------------------------------------------------------------


async def test_policy_unauthorized_battery_busy_and_group_cooldown(settings: Any) -> None:
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            policy = SocialProactivePolicy(svc, "telegram", lambda: BOT_ID)
            empty_cfg = ScheduleOptions()
            group_scope = f"telegram:{GROUP_ID}"
            grp_msg = _tg_group_msg("0", event_id="seed")
            session_scope = svc.store.session_scope(grp_msg)

            # 1a. Unauthorized scope (unknown platform / muted group)
            d = await policy.evaluate(
                _ticket("t-unauth-1", target_scope="onebot:40000"), NOW, empty_cfg
            )
            assert d.reason == "unauthorized_scope"

            s.group_policies[group_scope] = "muted"
            d = await policy.evaluate(
                _ticket("t-unauth-2", target_scope=group_scope), NOW, empty_cfg
            )
            assert d.reason == "unauthorized_scope"
            s.group_policies[group_scope] = "at_only"

            # 1b. Social battery low -> delay 900
            await _set_social_battery(svc.store, SOCIAL_LOW - 1)
            d = await policy.evaluate(_ticket("t-bat", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "social_battery_low"
            assert d.delay == 900
            await _set_social_battery(svc.store, 90.0)

            # 1c. Session concurrency busy gates -> delay 150
            svc.ingress_scheduler.entries[session_scope] = object()
            d = await policy.evaluate(_ticket("t-busy-1", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "session_busy"
            assert d.delay == 150
            svc.ingress_scheduler.entries.pop(session_scope, None)

            svc.dialogue_scheduler.entries[session_scope] = object()
            d = await policy.evaluate(_ticket("t-busy-2", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "session_busy"
            assert d.delay == 150
            svc.dialogue_scheduler.entries.pop(session_scope, None)

            svc.store.groups.active[session_scope] = object()
            d = await policy.evaluate(_ticket("t-busy-3", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "session_busy"
            assert d.delay == 150
            svc.store.groups.active.pop(session_scope, None)

            async with svc.owner_dialogue_lock:
                d = await policy.evaluate(
                    _ticket("t-busy-owner", target_scope=svc.proactive.target_scope),
                    NOW,
                    empty_cfg,
                )
                assert d.reason == "session_busy"
                assert d.delay == 150

            # Seed 2 valid group messages so activity passes when cooldown passes
            svc.hot.observe(_tg_group_msg("u1", event_id="e1", created_at=NOW - 120))
            svc.hot.observe(_tg_group_msg("u2", event_id="e2", created_at=NOW - 60))

            # 1d. Group cooldown via filters.last_proactive_spoke (empty cfg falls back to 1500s)
            svc.store.runtime.filters.note_proactive_spoke(grp_msg, now=NOW - 300)
            d = await policy.evaluate(_ticket("t-cd-1", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "group_cooldown"
            svc.store.runtime.filters.last_proactive_spoke.clear()

            # 1e. Group cooldown via world_state.record_group_proactive
            await svc.world_state.record_group_proactive(session_scope, now=NOW - 300)
            d = await policy.evaluate(_ticket("t-cd-2", target_scope=group_scope), NOW, empty_cfg)
            assert d.reason == "group_cooldown"
            # Advance past cooldown -> now allowed
            d = await policy.evaluate(
                _ticket("t-cd-ok", target_scope=group_scope),
                NOW + 1600,
                SimpleNamespace(group_active_window_seconds=3600),
            )
            assert d.reason == ""

            # 1f. Group cooldown via filters.speaking_state != "ready"
            svc.store.runtime.filters.spoke(grp_msg)
            d = await policy.evaluate(
                _ticket("t-cd-speak", target_scope=group_scope),
                NOW + 1600,
                SimpleNamespace(group_active_window_seconds=3600),
            )
            assert d.reason == "group_cooldown"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 2. SocialProactivePolicy: group activity, muted, affection level, high-bond
# ---------------------------------------------------------------------------


async def test_policy_group_activity_muted_affection_and_high_bond(settings: Any) -> None:
    s = _tg_settings(settings).model_copy(update={"affection_enabled": True})
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            policy = SocialProactivePolicy(svc, "telegram", lambda: BOT_ID)
            cfg = ScheduleOptions()
            group_scope = f"telegram:{GROUP_ID}"

            # Empty hot ring -> group_inactive
            d = await policy.evaluate(_ticket("g-empty", target_scope=group_scope), NOW, cfg)
            assert d.reason == "group_inactive"

            # Self message + recalled message + muted user + low-affection (lv<=1) all ignored
            svc.hot.observe(_tg_group_msg(BOT_ID, event_id="self-1", created_at=NOW - 30))
            recalled = _tg_group_msg("u-rec", event_id="rec-1", created_at=NOW - 25)
            svc.hot.observe(recalled)
            svc.hot.recall(recalled, "rec-1")

            muted_pid = await svc.identity.resolve_person_id("telegram", "u-muted", create=True)
            await svc.identity.set_person_muted(muted_pid, True, source="admin")
            svc.hot.observe(_tg_group_msg("u-muted", event_id="m-1", created_at=NOW - 20))

            aff = svc.store.runtime.affection
            await aff.set_user_score("u-low", 10, platform="telegram", reason="test")  # lv 1
            svc.hot.observe(_tg_group_msg("u-low", event_id="low-1", created_at=NOW - 15))

            # Only 1 normal lv2 user -> below min_recent=2 -> still group_inactive
            await aff.set_user_score("u-mid1", 30, platform="telegram", reason="test")  # lv 2
            svc.hot.observe(_tg_group_msg("u-mid1", event_id="mid-1", created_at=NOW - 10))
            d = await policy.evaluate(_ticket("g-one-mid", target_scope=group_scope), NOW, cfg)
            assert d.reason == "group_inactive"

            # Add 2nd lv2 user -> reaches effective_min=2 -> passes!
            await aff.set_user_score("u-mid2", 30, platform="telegram", reason="test")  # lv 2
            svc.hot.observe(_tg_group_msg("u-mid2", event_id="mid-2", created_at=NOW - 5))
            d = await policy.evaluate(_ticket("g-two-mid", target_scope=group_scope), NOW, cfg)
            assert d.reason == ""

            # Reset hot ring; single high-bond (lv >= 4) lowers effective_min to 1 -> passes!
            svc.hot = HotRing()
            await aff.set_user_score("u-high", 70, platform="telegram", reason="test")  # lv 4
            svc.hot.observe(_tg_group_msg("u-high", event_id="hi-1", created_at=NOW - 10))
            d = await policy.evaluate(_ticket("g-high-bond", target_scope=group_scope), NOW, cfg)
            assert d.reason == ""

            # Reset hot ring; single bound Owner message lowers effective_min to 1 -> passes!
            svc.hot = HotRing()
            svc.hot.observe(_tg_group_msg(OWNER_UID, event_id="own-1", created_at=NOW - 10))
            d = await policy.evaluate(_ticket("g-owner-one", target_scope=group_scope), NOW, cfg)
            assert d.reason == ""

            # Fallback `rings`-only HotRing double (no recent_group_activity attr) also works
            real_hot = HotRing()
            real_hot.observe(_tg_group_msg(OWNER_UID, event_id="ring-1", created_at=NOW - 10))
            svc.hot = SimpleNamespace(
                rings=real_hot.rings,
                key=real_hot.key,
                recalled=real_hot.recalled,
            )
            d = await policy.evaluate(_ticket("g-rings-fb", target_scope=group_scope), NOW, cfg)
            assert d.reason == ""
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 3. SocialProactivePolicy: Owner DM 3h window + Desktop Antigravity_Memory synergy
# ---------------------------------------------------------------------------


async def test_policy_owner_dm_3h_window_milestone_and_desktop_synergy(
    settings: Any, tmp_path: Path
) -> None:
    s = _tg_settings(settings, notebook=True)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            policy = SocialProactivePolicy(svc, "telegram", lambda: BOT_ID)
            cfg = ScheduleOptions()
            owner_scope = svc.proactive.target_scope

            # 3a. Owner recently active within 3h -> blocked
            await _set_last_private_at(svc.store, svc.proactive.scope, NOW - 1800)
            d = await policy.evaluate(
                _ticket("dm-recent", target_scope=owner_scope, action_type="owner_companion"),
                NOW,
                cfg,
            )
            assert d.reason == "owner_recent_activity"

            # Non-planner *_milestone_share bypasses 3h window
            d = await policy.evaluate(
                _ticket(
                    "dm-ms-rule",
                    target_scope=owner_scope,
                    action_type="minecraft_milestone_share",
                    origin="rule",
                ),
                NOW,
                cfg,
            )
            assert d.reason == ""

            # Planner *_milestone_share does NOT bypass 3h window
            d = await policy.evaluate(
                _ticket(
                    "dm-ms-plan",
                    target_scope=owner_scope,
                    action_type="minecraft_milestone_share",
                    origin="planner",
                ),
                NOW,
                cfg,
            )
            assert d.reason == "owner_recent_activity"

            # Owner idle >= 3h -> allowed
            await _set_last_private_at(svc.store, svc.proactive.scope, NOW - 3 * 3600 - 1)
            d = await policy.evaluate(
                _ticket("dm-idle", target_scope=owner_scope, action_type="owner_companion"),
                NOW,
                cfg,
            )
            assert d.reason == ""
            assert d.desktop_hint is None

            # 3b. Desktop Antigravity_Memory synergy with notebook=True
            called_desktop = 0

            class _FakeCloudMem:
                def latest_activity_info(self) -> dict[str, Any]:
                    nonlocal called_desktop
                    called_desktop += 1
                    return {
                        "enabled": True,
                        "latest_timestamp": NOW - 600,
                        "latest_iso": "2026-11-26T14:50:00+08:00",
                        "latest_tag": "coding",
                    }

            svc.cloud_memory = _FakeCloudMem()

            # owner_silence_care without allow_desktop_care -> suppressed
            d = await policy.evaluate(
                _ticket("dm-sil", target_scope=owner_scope, action_type="owner_silence_care"),
                NOW,
                cfg,
            )
            assert d.reason == "owner_recent_activity"
            assert called_desktop == 1

            # owner_silence_care WITH allow_desktop_care -> allowed + DESKTOP_CONTEXT_HINT
            d = await policy.evaluate(
                _ticket(
                    "dm-sil-ok",
                    target_scope=owner_scope,
                    action_type="owner_silence_care",
                    gate_condition={"allow_desktop_care": True},
                ),
                NOW,
                cfg,
            )
            assert d.reason == ""
            assert d.desktop_hint == DESKTOP_CONTEXT_HINT

            # owner_desktop_warm_care -> allowed + DESKTOP_CONTEXT_HINT
            d = await policy.evaluate(
                _ticket(
                    "dm-warm",
                    target_scope=owner_scope,
                    action_type="owner_desktop_warm_care",
                ),
                NOW,
                cfg,
            )
            assert d.reason == ""
            assert d.desktop_hint == DESKTOP_CONTEXT_HINT

            # 3c. Read-only file fallback to antigravity_memory_dir/short_term_memory.json
            mem_dir = tmp_path / "ag_mem"
            mem_dir.mkdir()
            stm_path = mem_dir / "short_term_memory.json"
            stm_raw = json.dumps([{"timestamp": "2026-11-26 14:40:00", "content": "working"}])
            stm_path.write_text(stm_raw, encoding="utf-8")
            orig_mtime = stm_path.stat().st_mtime
            svc.cloud_memory = None
            svc.s.enable_antigravity_memory = True
            svc.s.antigravity_memory_dir = mem_dir

            d = await policy.evaluate(
                _ticket(
                    "dm-file-warm",
                    target_scope=owner_scope,
                    action_type="owner_desktop_warm_care",
                ),
                NOW,
                cfg,
            )
            assert d.reason == ""
            assert d.desktop_hint == DESKTOP_CONTEXT_HINT
            # Strictly read-only: file content & mtime unchanged, no extra files created
            assert stm_path.read_text(encoding="utf-8") == stm_raw
            assert stm_path.stat().st_mtime == orig_mtime
            assert list(mem_dir.iterdir()) == [stm_path]

            # 3d. delivery_enabled=False blocks Owner DM
            svc.s.proactive_dm_enabled = False
            d = await policy.evaluate(
                _ticket("dm-disabled", target_scope=owner_scope, action_type="owner_companion"),
                NOW,
                cfg,
            )
            assert d.reason == "owner_recent_activity"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_policy_desktop_synergy_gated_by_notebook_grant(settings: Any) -> None:
    """When OwnerBinding has notebook=False, Antigravity_Memory is never read or hinted."""
    s = _tg_settings(settings, notebook=False)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            called = 0

            class _SpyCloudMem:
                def latest_activity_info(self) -> dict[str, Any]:
                    nonlocal called
                    called += 1
                    return {"enabled": True, "latest_timestamp": NOW - 120}

            svc.cloud_memory = _SpyCloudMem()
            policy = SocialProactivePolicy(svc, "telegram", lambda: BOT_ID)
            owner_scope = svc.proactive.target_scope

            # owner_silence_care is NOT suppressed and cloud_memory is NOT touched
            d1 = await policy.evaluate(
                _ticket("dm-no-nb-1", target_scope=owner_scope, action_type="owner_silence_care"),
                NOW,
                ScheduleOptions(),
            )
            assert d1.reason == ""
            assert d1.desktop_hint is None
            assert called == 0

            # owner_desktop_warm_care gets NO DESKTOP_CONTEXT_HINT when notebook=False
            d2 = await policy.evaluate(
                _ticket(
                    "dm-no-nb-2",
                    target_scope=owner_scope,
                    action_type="owner_desktop_warm_care",
                    gate_condition={"allow_desktop_care": True},
                ),
                NOW,
                ScheduleOptions(),
            )
            assert d2.reason == ""
            assert d2.desktop_hint is None
            assert called == 0
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 4. social_speak: still_allowed, group prefetch recall=False, before_delivery, selected_news
# ---------------------------------------------------------------------------


async def test_social_speak_before_delivery_still_allowed_group_prefetch_and_news(
    settings: Any,
) -> None:
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            owner_scope = svc.proactive.target_scope
            owner_msg = svc.proactive.target()

            # 4a. call.still_allowed() == False before ai.generate -> gated_skip without calling AI
            ai_calls = 0
            orig_generate = svc.ai.generate

            async def _spy_generate(*args: Any, **kwargs: Any) -> Any:
                nonlocal ai_calls
                ai_calls += 1
                return await orig_generate(*args, **kwargs)

            svc.ai.generate = _spy_generate
            out_skip = await speak_proactive(
                svc,
                owner_msg,
                ActionCall(
                    ticket=_ticket("sp-disallowed", target_scope=owner_scope),
                    args=NoArgs(),
                    now=NOW,
                    still_allowed=lambda: False,
                ),
            )
            assert out_skip.status == "gated_skip"
            assert out_skip.reason == "action_no_longer_eligible"
            assert ai_calls == 0

            # 4b. before_delivery double-check: owner chats during ai.generate -> cancelled!
            async def _gen_while_owner_speaks(*args: Any, **kwargs: Any) -> Any:
                await _set_last_private_at(svc.store, svc.proactive.scope, NOW - 10)
                return await orig_generate(*args, **kwargs)

            svc.ai.generate = _gen_while_owner_speaks
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,updated_at,execute_after,expire_at,target_module,"
                    "action_type,action_name,target_scope,status,mood,"
                    "intent_note_json,gate_condition_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "sp-race-1",
                        NOW,
                        NOW,
                        NOW,
                        NOW + 3600,
                        "telegram",
                        "owner_companion",
                        "telegram.speak",
                        owner_scope,
                        "executing",
                        "happy",
                        "{}",
                        "{}",
                    ),
                )
            # Also verify graceful handling when life_engine is temporarily absent
            saved_engine = svc.life_engine
            svc.life_engine = None
            try:
                out_race = await speak_proactive(
                    svc,
                    owner_msg,
                    ActionCall(
                        ticket=_ticket(
                            "sp-race-1",
                            target_scope=owner_scope,
                            action_type="owner_companion",
                        ),
                        args=NoArgs(),
                        now=NOW,
                        still_allowed=lambda: True,
                    ),
                )
            finally:
                svc.life_engine = saved_engine
            assert out_race.status == "gated_skip"
            assert out_race.reason == "cancelled"
            assert fake_api.sent_messages == []

            # Clear owner recent activity and restore generator
            await _set_last_private_at(svc.store, svc.proactive.scope, 0.0)
            svc.ai.generate = orig_generate

            # 4c. Non-private group turn prefetches dual memory with recall=False &
            #     related_user_ids, and selected_news settles cleanly via outgoing.apply_receipts
            prefetch_calls: list[dict[str, Any]] = []
            orig_prefetch = svc.sessions.dual.prefetch

            async def _spy_prefetch(msg: Message, query: str, **kwargs: Any) -> Any:
                prefetch_calls.append({"msg": msg, "query": query, **kwargs})
                return await orig_prefetch(msg, query, **kwargs)

            svc.sessions.dual.prefetch = _spy_prefetch

            grp_msg = _tg_group_msg("0", event_id="jit:sp-news-1", created_at=NOW).model_copy(
                update={"addressed": True}
            )
            svc.hot.observe(_tg_group_msg(FRIEND_UID, event_id="h-1", created_at=NOW - 30))
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT INTO world_news("
                    "event_id,at,expires,audience,source,ref,title,shared_at,intent_id"
                    ") VALUES (?,?,?,?,?,?,?,NULL,NULL)",
                    ("ev-news-1", NOW - 60, NOW + 3600, "", "bilibili", "BV1xx411c7mD", "猫猫冲浪"),
                )
                await svc.store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,updated_at,execute_after,expire_at,target_module,"
                    "action_type,action_name,target_scope,status,mood,"
                    "intent_note_json,gate_condition_json,source_event_id"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "sp-news-1",
                        NOW,
                        NOW,
                        NOW,
                        NOW + 3600,
                        "telegram",
                        "group_share",
                        "telegram.speak",
                        f"telegram:{GROUP_ID}",
                        "executing",
                        "curious",
                        "{}",
                        '{"share_news":true}',
                        "ev-news-1",
                    ),
                )

            out_news = await speak_proactive(
                svc,
                grp_msg,
                ActionCall(
                    ticket=_ticket(
                        "sp-news-1",
                        target_scope=f"telegram:{GROUP_ID}",
                        action_type="group_share",
                        gate_condition={"share_news": True},
                        source_event_id="ev-news-1",
                    ),
                    args=NoArgs(),
                    now=NOW,
                    still_allowed=lambda: True,
                ),
            )
            assert out_news.status == "done"
            assert len(prefetch_calls) == 1
            assert prefetch_calls[0]["recall"] is False
            assert "related_user_ids" in prefetch_calls[0]

            # Dedicated world_news settled atomically via outgoing.apply_receipts
            async with svc.store.lock:
                row = await svc.store.one(
                    "SELECT shared_at, intent_id FROM world_news WHERE event_id=?",
                    ("ev-news-1",),
                )
            assert row is not None
            assert row["intent_id"] == "sp-news-1"
            assert row["shared_at"] is not None
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


# ---------------------------------------------------------------------------
# 5. outgoing.apply_receipts dedicated world_news settlement for *.speak
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("action_name", "expected_bubbles", "should_settle"),
    [
        ("telegram.speak", 1, True),
        ("social.speak", 1, True),
        ("qq.speak", 1, True),
        ("telegram.speak", 2, False),  # only 1 bubble acked out of 2
        ("social.owner_digest", 1, False),  # not a *.speak action
    ],
)
async def test_outgoing_apply_receipts_settles_world_news_for_speak_actions(
    settings: Any,
    action_name: str,
    expected_bubbles: int,
    should_settle: bool,
) -> None:
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.world_state.clock = lambda: NOW
        intent_id = f"settle-{action_name}-{expected_bubbles}"
        reply_id = f"jit:{intent_id}"
        event_id = f"ev-{intent_id}"
        msg = _tg_group_msg("0", event_id=reply_id, created_at=NOW).model_copy(
            update={"addressed": True}
        )
        epoch = (await svc.sessions.snapshot(msg))["epoch"]
        async with svc.store.transaction():
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json,expected_bubbles"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    intent_id,
                    NOW,
                    NOW,
                    NOW,
                    NOW + 3600,
                    "telegram",
                    "group_share",
                    action_name,
                    f"telegram:{GROUP_ID}",
                    "executing",
                    "curious",
                    "{}",
                    "{}",
                    expected_bubbles,
                ),
            )
            await svc.store.db.execute(
                "INSERT INTO world_news("
                "event_id,at,expires,audience,source,ref,title,shared_at,intent_id"
                ") VALUES (?,?,?,?,?,?,?,NULL,?)",
                (
                    event_id,
                    NOW - 60,
                    NOW + 3600,
                    "",
                    "bilibili",
                    "BV9test999",
                    "不相关的标题",
                    intent_id,
                ),
            )

        synthetic = {
            "id": reply_id,
            "message": msg.model_dump(),
            "participants": [msg.model_dump()],
        }
        await persist_ack(svc, synthetic, 0, "随便聊聊喵", {"message_id": "9001"}, epoch)
        await apply_receipts(svc, reply_id)

        async with svc.store.lock:
            row = await svc.store.one(
                "SELECT shared_at FROM world_news WHERE event_id=?",
                (event_id,),
            )
        assert row is not None
        if should_settle:
            assert row["shared_at"] is not None
        else:
            assert row["shared_at"] is None


# ---------------------------------------------------------------------------
# 6. PluginBus: preflight wiring + proactive.on_connection lifecycle hooks
# ---------------------------------------------------------------------------


async def test_plugin_bus_preflight_and_proactive_on_connection_lifecycle(settings: Any) -> None:
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        # Spy on_connection accepting platform kwarg
        conn_events: list[tuple[bool, str | None]] = []

        async def _spy_on_connection(online: bool, *, platform: str | None = None) -> None:
            conn_events.append((online, platform))

        svc.proactive.on_connection = _spy_on_connection
        svc.proactive.wakeup.clear()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            # 6a. Connect triggers wakeup.set() AND on_connection(True, platform="telegram")
            assert svc.proactive.wakeup.is_set() is True
            assert conn_events == [(True, "telegram")]

            # 6b. Unified social.speak has preflight backed by SocialProactivePolicy
            assert "telegram.speak" not in svc.modules.catalog.actions
            spec = svc.modules.catalog.actions["social.speak"]
            assert spec.preflight is not None

            # Inactive group rejected by preflight
            dec_inact = await spec.preflight(
                _ticket("pf-1", target_scope=f"telegram:{GROUP_ID}"), NOW, ScheduleOptions()
            )
            assert dec_inact.reason == "group_inactive"

            # Active group passes preflight
            svc.hot.observe(_tg_group_msg(OWNER_UID, event_id="pf-e1", created_at=NOW - 10))
            dec_ok = await spec.preflight(
                _ticket("pf-2", target_scope=f"telegram:{GROUP_ID}"), NOW, ScheduleOptions()
            )
            assert dec_ok.reason == ""

            # Recent owner DM rejected by preflight
            await _set_last_private_at(svc.store, svc.proactive.scope, NOW - 60)
            dec_dm = await spec.preflight(
                _ticket("pf-3", target_scope=svc.proactive.target_scope), NOW, ScheduleOptions()
            )
            assert dec_dm.reason == "owner_recent_activity"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)

        # 6c. Disconnect calls on_connection(False, platform="telegram")
        await _wait_until(lambda: (False, "telegram") in conn_events)
        assert conn_events == [(True, "telegram"), (False, "telegram")]


async def test_plugin_bus_does_not_flip_offline_when_session_already_replaced(
    settings: Any,
) -> None:
    """Disposing a stale session that was already superseded must NOT emit on_connection(False)."""
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        bus = svc.plugin_bus

        conn_events: list[tuple[bool, str | None]] = []

        async def _spy_on_connection(online: bool, *, platform: str | None = None) -> None:
            conn_events.append((online, platform))

        svc.proactive.on_connection = _spy_on_connection

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            assert conn_events == [(True, "telegram")]
            old_sess = bus._sessions["telegram"]
            # Simulate a newer replacement session owning bus._sessions["telegram"]
            sentinel_replacement = object()
            bus._sessions["telegram"] = sentinel_replacement  # type: ignore[assignment]
            await old_sess.fiber.dispose()
            # Must NOT flip offline and must NOT pop the newer session
            assert bus._sessions["telegram"] is sentinel_replacement
            assert conn_events == [(True, "telegram")]
        finally:
            bus._sessions.pop("telegram", None)
            with contextlib.suppress(Exception):
                await _stop_connected_plugin(plugin, bridge, runner)


async def test_plugin_bus_notify_proactive_connection_legacy_signature_and_platform_filter(
    settings: Any,
) -> None:
    """_notify_proactive_connection handles 1-arg on_connection and ignores mismatched platforms."""
    s = _tg_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        bus = svc.plugin_bus
        calls: list[bool] = []

        async def _legacy_on_connection(online: bool) -> None:
            calls.append(online)

        svc.proactive.on_connection = _legacy_on_connection

        # Unrelated platform ignored
        await bus._notify_proactive_connection(("discord",), True)
        assert calls == []

        # Matching platform invokes legacy 1-arg signature
        await bus._notify_proactive_connection(("telegram",), True)
        await bus._notify_proactive_connection(("telegram",), False)
        assert calls == [True, False]

        # Exceptions inside on_connection are absorbed; CancelledError propagates
        async def _boom(online: bool, *, platform: str | None = None) -> None:
            raise RuntimeError("transient db error")

        svc.proactive.on_connection = _boom
        await bus._notify_proactive_connection(("telegram",), True)

        async def _cancel(online: bool, *, platform: str | None = None) -> None:
            raise asyncio.CancelledError

        svc.proactive.on_connection = _cancel
        with pytest.raises(asyncio.CancelledError):
            await bus._notify_proactive_connection(("telegram",), False)
