"""Tests for Phase 3A (R3 Backend): SocialPresenceModule, social.speak, & SocialScheduleOptions."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import SecretStr

from hakimi.affection import AffectionConfig
from hakimi.app import create_app
from hakimi.life_engine import LifeEngine
from hakimi.models import Message
from hakimi.modules.actions import ActionCall, NoArgs
from hakimi.modules.qq_social import QQSocialModule
from hakimi.modules.scheduling import ScheduleOptions, SocialScheduleOptions
from hakimi.plugin_bus.protocol import RESERVED_PLUGIN_IDS
from hakimi.plugin_kernel import PluginKernel
from hakimi.service import Service
from hakimi.social import OwnerBinding, owner_target_scope
from hakimi.social_policy import SocialProactivePolicy
from hakimi.social_presence import SocialPresenceModule
from hakimi.social_speak import speak_proactive
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_plugin_telegram import (
    BOT_ID,
    GROUP_ID,
    OWNER_UID,
    _FakeTelegramApi,
    _start_connected_plugin,
    _stop_connected_plugin,
)

NOW = datetime(2026, 11, 26, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


def _dual_platform_settings(settings: Any, *, primary: str = "telegram") -> Any:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "onebot_enabled": True,
            "onebot_mode": "ws_reverse",
            "onebot_ws_token": SecretStr("x" * 32),
            "onebot_self_id": "10000",
            "owner_qq_id": "20000",
            "onebot_allowed_groups": {"40000"},
            "qq_enabled": False,
            "owner_delivery_platform": primary,
            "proactive_dm_enabled": True,
            "daily_digest_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(
                    bot_id=BOT_ID,
                    user_id=OWNER_UID,
                    notebook=True,
                    mirror=True,
                )
            },
            "group_policies": {
                f"telegram:{GROUP_ID}": "at_only",
                "onebot:40000": "at_only",
            },
        }
    )


async def test_social_presence_thinking_scenes_empty_and_admin_thinking_stays_9(
    settings: Any,
) -> None:
    assert "social_presence" in RESERVED_PLUGIN_IDS
    assert SocialPresenceModule.thinking_scenes == ()

    s = _dual_platform_settings(settings, primary="onebot")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        svc = app.state.service
        sp = svc.modules.get("social_presence")
        assert isinstance(sp, SocialPresenceModule)
        assert sp.thinking_scenes == ()

        res = await client.get("/admin/api/thinking")
        assert res.status_code == 200
        assert len(res.json()["config"]) == 9


async def test_social_speak_unavailable_and_per_platform_preflight_connectivity(
    settings: Any,
) -> None:
    s = _dual_platform_settings(settings, primary="telegram")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()

        qq_mod = svc.modules.get("qq_social")
        assert isinstance(qq_mod, QQSocialModule)
        spec = svc.modules.catalog.actions["social.speak"]
        assert spec.preflight is not None

        # Only qq_social is registered and offline -> onebot_offline
        assert qq_mod.chat_connected("onebot") is False
        assert qq_mod.chat_connected("qq") is False
        assert spec.unavailable() == "onebot_offline"

        # Connect Telegram while OneBot remains offline -> unavailable() clears
        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            assert spec.unavailable() is None

            # Seed hot activity in both groups
            svc.hot.observe(
                Message(
                    platform="onebot",
                    bot_id="10000",
                    event_id="ob-1",
                    user_id="20000",
                    conversation_id="40000",
                    kind="group",
                    text="群里聊聊",
                    addressed=False,
                    created_at=NOW - 10,
                )
            )
            svc.hot.observe(
                Message(
                    platform="telegram",
                    bot_id=BOT_ID,
                    event_id="tg-1",
                    user_id=OWNER_UID,
                    conversation_id=GROUP_ID,
                    kind="group",
                    text="tg聊聊",
                    addressed=False,
                    created_at=NOW - 10,
                )
            )

            cfg = svc.modules.get("social_presence").scheduling(svc.life_engine.config)

            # OneBot offline -> onebot:40000 defers (onebot_offline, delay=120); telegram proceeds
            ob_dec = await spec.preflight(
                {"id": "p-ob-1", "target_scope": "onebot:40000", "action_type": "group_share"},
                NOW,
                cfg,
            )
            assert ob_dec.reason == "onebot_offline" and ob_dec.delay == 120

            tg_dec = await spec.preflight(
                {"id": "p-tg-1", "target_scope": f"telegram:{GROUP_ID}", "action_type": "speak"},
                NOW,
                cfg,
            )
            assert tg_dec.reason == ""

            # Flip OneBot online and mark Telegram session unready -> vice-versa!
            svc.sender.onebot.ready.set()
            assert qq_mod.chat_connected("onebot") is True
            assert qq_mod.chat_connected("qq") is False

            tg_proxy = svc.modules.get("telegram")
            tg_proxy._session.ready = False
            assert tg_proxy.chat_connected("telegram") is False
            assert spec.unavailable() is None  # OneBot is still connected

            ob_dec2 = await spec.preflight(
                {"id": "p-ob-2", "target_scope": "onebot:40000", "action_type": "group_share"},
                NOW,
                cfg,
            )
            assert ob_dec2.reason == ""

            tg_dec2 = await spec.preflight(
                {"id": "p-tg-2", "target_scope": f"telegram:{GROUP_ID}", "action_type": "speak"},
                NOW,
                cfg,
            )
            assert tg_dec2.reason == "plugin_disconnected" and tg_dec2.delay == 120

            # Both offline -> no_chat_platform_connected
            svc.sender.onebot.ready.clear()
            assert spec.unavailable() == "no_chat_platform_connected"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_social_schedule_options_shared_across_telegram_and_secondary_boundowner(
    settings: Any,
) -> None:
    s = _dual_platform_settings(settings, primary="telegram")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
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
            tg_scope = owner_target_scope(s, "telegram")
            assert tg_scope and tg_scope.startswith("boundowner:")

            # 1. owner_dms_unlimited on social_presence allows Telegram Owner DM even at max=0
            await svc.life_engine.set_config(
                {"max_proactive_per_day": 0, "owner_dms_unlimited": True}
            )
            allowed = await svc.life_engine.gatekeeper.daily_limit_allows(
                "social.speak", tg_scope, NOW, svc.life_engine.config
            )
            assert allowed is True

            await svc.life_engine.set_config(
                {"max_proactive_per_day": 0, "owner_dms_unlimited": False}
            )
            blocked = await svc.life_engine.gatekeeper.daily_limit_allows(
                "social.speak", tg_scope, NOW, svc.life_engine.config
            )
            assert blocked is False

            # 2. OwnerProactive.delivery_enabled for Telegram toggles via social_presence schedule
            assert svc.proactive.delivery_enabled is True
            r_off = await client.put(
                "/admin/api/life/modules/social_presence/schedule",
                json={"owner_delivery_enabled": False},
            )
            assert r_off.status_code == 200
            assert svc.proactive.delivery_enabled is False
            assert svc.modules.permits_recipient(tg_scope, svc.life_engine.config) is False

            r_on = await client.put(
                "/admin/api/life",
                json={"owner_delivery_enabled": True},
            )
            assert r_on.status_code == 200
            assert svc.proactive.delivery_enabled is True
            assert svc.modules.permits_recipient(tg_scope, svc.life_engine.config) is True

            # 3. M1 note #2: secondary boundowner scope honors social_presence
            #    owner_delivery_enabled even when primary owner_delivery_platform is onebot
            s.owner_delivery_platform = "onebot"
            await svc.life_engine.set_config({"owner_delivery_enabled": False})
            assert svc.modules.permits_recipient(tg_scope, svc.life_engine.config) is False
            await svc.life_engine.set_config({"owner_delivery_enabled": True})
            assert svc.modules.permits_recipient(tg_scope, svc.life_engine.config) is True

            # 4. SocialPresenceModule.local_activity emits boundowner for Telegram desktop warm care
            s.owner_delivery_platform = "telegram"
            sp = svc.modules.get("social_presence")
            act_warm = sp.local_activity(
                {
                    "available_groups": [],
                    "desktop_active_recently": True,
                    "owner_dm_available": True,
                    "current_world_state": {"public_unshared_count": 0},
                },
                {"social.speak"},
                False,
            )
            assert act_warm is not None
            assert act_warm.intent["target_scope"] == tg_scope
            assert OWNER_UID not in act_warm.intent["target_scope"]

            # Group news share preserves target_module="bilibili_surf" & omits action_name
            act_news = sp.local_activity(
                {
                    "available_groups": [f"telegram:{GROUP_ID}"],
                    "desktop_active_recently": False,
                    "owner_dm_available": False,
                    "current_world_state": {"public_unshared_count": 1},
                },
                {"social.speak"},
                False,
            )
            assert act_news is not None
            assert act_news.intent["target_module"] == "bilibili_surf"
            assert "action_name" not in act_news.intent
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_m1_note1_normalize_recipient_fail_closed_when_qq_social_disabled(
    settings: Any, store: Any
) -> None:
    s = _dual_platform_settings(settings, primary="onebot")
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        assert svc.modules.normalize_recipient("owner:20000") == "owner:20000"
        assert svc.modules.normalize_recipient("onebot:40000") == "onebot:40000"
        assert svc.modules.normalize_recipient("private:20000") == "owner:20000"

        qq_mod = svc.modules.get("qq_social")
        assert qq_mod is not None
        qq_mod.enabled = False

        assert svc.modules.normalize_recipient("owner:20000") is None
        assert svc.modules.normalize_recipient("onebot:40000") is None
        assert svc.modules.normalize_recipient("private:20000") is None
    finally:
        await svc.stop()


async def test_legacy_sqlite_migration_and_action_mode_alias_compat(settings: Any) -> None:
    s = _dual_platform_settings(settings, primary="onebot")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        svc = app.state.service
        # Seed legacy SQLite config with qq_social module_settings & qq.speak / telegram.speak modes
        await svc.store.runtime.save_config(
            "life_engine",
            {
                "module_settings": {
                    "qq_social": {
                        "owner_delivery_enabled": False,
                        "owner_dms_unlimited": True,
                        "group_cooldown_seconds": 1800,
                    }
                },
                "action_modes": {
                    "qq.speak": "off",
                    "telegram.speak": "off",
                },
            },
        )
        engine = svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await engine.initialize()

        assert "qq_social" not in engine.config.module_settings
        sp_opts = svc.modules.get("social_presence").scheduling(engine.config)
        assert sp_opts.owner_delivery_enabled is False
        assert sp_opts.owner_dms_unlimited is True
        assert sp_opts.group_cooldown_seconds == 1800
        assert engine.config.action_modes == {"social.speak": "off"}

        # Canonical action_modes key overrides legacy alias if both are present
        await svc.store.runtime.save_config(
            "life_engine",
            {
                "action_modes": {
                    "qq.speak": "off",
                    "social.speak": "active",
                }
            },
        )
        engine2 = svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await engine2.initialize()
        assert engine2.config.action_modes == {"social.speak": "active"}

        # PUT /admin/api/life/actions/qq.speak?view=module -> social.speak / social_presence
        res = await client.put(
            "/admin/api/life/actions/qq.speak",
            json={"mode": "off", "view": "module"},
        )
        assert res.status_code == 200
        body = res.json()
        assert body["id"] == "social_presence"
        assert next(a for a in body["actions"] if a["name"] == "social.speak")["mode"] == "off"

        # Historical qq.speak / telegram.speak rows count in GatekeeperWorker._usage_tx
        await engine2.set_config({"max_proactive_per_day": 10, "owner_dms_unlimited": False})
        async with svc.store.transaction():
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json,claimed_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "hist-qq-1",
                    NOW,
                    NOW,
                    NOW,
                    NOW + 3600,
                    "qq_social",
                    "owner_companion",
                    "qq.speak",
                    "owner:20000",
                    "done",
                    "happy",
                    "{}",
                    "{}",
                    NOW,
                ),
            )
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json,claimed_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "hist-tg-1",
                    NOW,
                    NOW,
                    NOW,
                    NOW + 3600,
                    "telegram",
                    "speak",
                    "telegram.speak",
                    "telegram:group-1",
                    "done",
                    "curious",
                    "{}",
                    "{}",
                    NOW,
                ),
            )
        usage = await engine2.gatekeeper.daily_usage(NOW, engine2.config)
        assert usage["total"] == 2
        assert usage["owner_dms"] == 1
        assert usage["counted"] == 2

        # Disabling social_presence cancels pending legacy qq.speak intents as well
        async with svc.store.transaction():
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "pend-qq-1",
                    NOW,
                    NOW,
                    NOW + 60,
                    NOW + 3600,
                    "social_presence",
                    "speak",
                    "qq.speak",
                    "onebot:40000",
                    "pending",
                    "curious",
                    "{}",
                    "{}",
                ),
            )
        await engine2.set_module_enabled(svc.modules.get("social_presence"), False)
        row = await svc.store.one(
            "SELECT status, result_reason FROM scheduled_intents WHERE id='pend-qq-1'"
        )
        assert (
            row is not None
            and row["status"] == "gated_skip"
            and row["result_reason"] == "module_disabled"
        )


async def test_qq_social_pure_adapter_and_owner_digest_timer(settings: Any, store: Any) -> None:
    s = _dual_platform_settings(settings, primary="onebot")
    s.bot_qq_ids = {"88888"}
    s.affection_enabled = True
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        await svc.life_engine.initialize()
        await svc.affection.set_config(AffectionConfig(enabled=True))
        qq_mod = svc.modules.get("qq_social")
        assert isinstance(qq_mod, QQSocialModule)
        assert qq_mod.schedule_options is ScheduleOptions
        assert qq_mod.actions() == ()
        assert qq_mod.schedules() == ()
        assert qq_mod.event_rules() == ()
        assert qq_mod.local_activity({}, {"social.speak"}, False) is None
        assert "qq.owner_digest" in svc.modules.catalog.actions
        assert svc.modules.catalog.owners["qq.owner_digest"] == "qq_social"
        assert svc.modules.schedules_for(qq_mod)[0].name == "qq.owner_digest"

        # planning_context on qq_social no longer emits desktop_active_recently
        ctx = await qq_mod.planning_context(svc, svc.life_engine.config, NOW)
        assert "desktop_active_recently" not in ctx
        assert ctx["available_groups"] == ["onebot:40000"]
        assert ctx["owner_dm_available"] is True

        # SocialPresenceModule owns desktop_active_recently and SocialScheduleOptions
        sp = svc.modules.get("social_presence")
        assert sp.schedule_options is SocialScheduleOptions
        sp_ctx = await sp.planning_context(svc, svc.life_engine.config, NOW)
        assert "desktop_active_recently" in sp_ctx

        # speak_proactive excludes bot_qq_ids from batch_affection on onebot
        s.affection_enabled = True
        grp_msg = Message(
            platform="onebot",
            bot_id="10000",
            event_id="jit:sp-bot-skip",
            user_id="0",
            conversation_id="40000",
            kind="group",
            text="",
            addressed=True,
            created_at=NOW,
        )
        svc.hot.observe(
            Message(
                platform="onebot",
                bot_id="10000",
                event_id="bot-msg-1",
                user_id="88888",
                conversation_id="40000",
                kind="group",
                text="bot message",
                addressed=False,
                created_at=NOW - 30,
            )
        )
        svc.hot.observe(
            Message(
                platform="onebot",
                bot_id="10000",
                event_id="user-msg-1",
                user_id="30000",
                conversation_id="40000",
                kind="group",
                text="hello",
                addressed=False,
                created_at=NOW - 20,
            )
        )
        captured_payloads: list[dict[str, Any]] = []
        orig_gen = svc.ai.generate

        async def _capture_gen(payload, subject, **kwargs):
            captured_payloads.append(payload)
            return await orig_gen(payload, subject, **kwargs)

        svc.ai.generate = _capture_gen
        await speak_proactive(
            svc,
            grp_msg,
            ActionCall(
                ticket={
                    "id": "sp-bot-skip",
                    "origin": "planner",
                    "target_module": "social_presence",
                    "action_type": "group_share",
                    "action_name": "social.speak",
                    "target_scope": "onebot:40000",
                    "mood": "curious",
                    "intent_note": {"note": "test"},
                    "gate_condition": {},
                },
                args=NoArgs(),
                now=NOW,
                still_allowed=lambda: True,
            ),
        )
        assert captured_payloads
        aff_keys = set(captured_payloads[0].get("batch_affection", {}).keys())
        assert "88888" not in aff_keys
        assert "30000" in aff_keys
    finally:
        await svc.stop()


async def test_gatekeeper_defers_unready_remote_group_intent_as_plugin_disconnected(
    settings: Any,
) -> None:
    """Unready remote chat session must defer with plugin_disconnected, not action_disabled."""
    s = _dual_platform_settings(settings, primary="onebot")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.kernel = PluginKernel()
        svc.world_state.clock = lambda: NOW
        svc.proactive.clock = lambda: NOW
        svc.life_engine = LifeEngine(svc, clock=lambda: NOW)
        await svc.life_engine.initialize()
        await svc.proactive.initialize()
        svc.sender.onebot.ready.set()

        fake_api = _FakeTelegramApi()
        plugin, bridge, runner = await _start_connected_plugin(
            svc, svc.plugin_bus, fake_api, allowed_groups={GROUP_ID}, proactive_groups={GROUP_ID}
        )
        try:
            tg_proxy = svc.modules.get("telegram")
            assert tg_proxy is not None
            tg_proxy._session.ready = False

            intent_id = await svc.world_state.enqueue_intent(
                {
                    "origin": "planner",
                    "target_module": "social_presence",
                    "action_type": "speak",
                    "action_name": "social.speak",
                    "target_scope": f"telegram:{GROUP_ID}",
                    "execute_after": NOW - 5,
                    "expire_at": NOW + 3600,
                    "mood": "curious",
                    "intent_note": {"note": "聊聊"},
                    "gate_condition": {},
                }
            )
            due = [
                row
                for row in await svc.world_state.list_due_intents(now=NOW)
                if row["id"] == intent_id
            ]
            assert len(due) == 1

            res = await svc.life_engine.gatekeeper._evaluate_ticket(
                due[0], NOW, svc.life_engine.config
            )
            assert res == "deferred"
            row = await svc.store.one(
                "SELECT status, result_reason FROM scheduled_intents WHERE id=?", (intent_id,)
            )
            assert row is not None
            assert row["status"] == "pending"
            assert row["result_reason"] == "plugin_disconnected"

            # Also verify closed session before unregistration defers instead of permanent skip
            tg_proxy._session.ready = True
            tg_proxy._session.closed = True
            res2 = await svc.life_engine.gatekeeper._evaluate_ticket(
                due[0], NOW, svc.life_engine.config
            )
            assert res2 == "deferred"
            row2 = await svc.store.one(
                "SELECT status, result_reason FROM scheduled_intents WHERE id=?", (intent_id,)
            )
            assert row2 is not None
            assert row2["status"] == "pending"
            assert row2["result_reason"] == "plugin_disconnected"
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_secondary_owner_dm_gate_honors_admin_owner_delivery_override(
    settings: Any,
) -> None:
    """Secondary boundowner DM honors persisted owner_delivery_enabled=True over .env False."""
    s = _dual_platform_settings(settings, primary="onebot").model_copy(
        update={"proactive_dm_enabled": False}
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
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
            tg_scope = owner_target_scope(s, "telegram")
            assert tg_scope and tg_scope.startswith("boundowner:")

            policy = SocialProactivePolicy(svc, "telegram", lambda: BOT_ID)
            sp = svc.modules.get("social_presence")
            assert sp is not None
            ticket = {
                "id": "sec-owner-dm-1",
                "target_scope": tg_scope,
                "action_type": "owner_companion",
                "gate_condition": {},
            }

            # Un-overridden .env proactive_dm_enabled=False still blocks secondary owner DM
            dec_off = await policy.evaluate(ticket, NOW, sp.scheduling(svc.life_engine.config))
            assert dec_off.reason == "owner_recent_activity"

            # Admin override owner_delivery_enabled=True enables both primary and secondary owner DM
            await svc.life_engine.set_config({"owner_delivery_enabled": True})
            cfg = sp.scheduling(svc.life_engine.config)
            dec_eval = await policy.evaluate(ticket, NOW, cfg)
            assert dec_eval.reason == ""

            spec = svc.modules.catalog.actions["social.speak"]
            assert spec.preflight is not None
            dec_pre = await spec.preflight(ticket, NOW, cfg)
            assert dec_pre.reason == ""
        finally:
            await _stop_connected_plugin(plugin, bridge, runner)


async def test_admin_life_status_resolves_executor_module_for_historical_speak_aliases(
    settings: Any,
) -> None:
    """Bug 3: GET /admin/api/life maps historical qq.speak/telegram.speak to social_presence."""
    s = _dual_platform_settings(settings, primary="onebot")
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client,
    ):
        svc = app.state.service
        async with svc.store.transaction():
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "hist-admin-qq",
                    NOW,
                    NOW,
                    NOW + 60,
                    NOW + 3600,
                    "qq_social",
                    "group_share",
                    "qq.speak",
                    "onebot:40000",
                    "pending",
                    "curious",
                    "{}",
                    "{}",
                ),
            )
            await svc.store.db.execute(
                "INSERT INTO scheduled_intents("
                "id,created_at,updated_at,execute_after,expire_at,target_module,"
                "action_type,action_name,target_scope,status,mood,"
                "intent_note_json,gate_condition_json"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "hist-admin-tg",
                    NOW,
                    NOW,
                    NOW + 60,
                    NOW + 3600,
                    "telegram",
                    "speak",
                    "telegram.speak",
                    f"telegram:{GROUP_ID}",
                    "pending",
                    "curious",
                    "{}",
                    "{}",
                ),
            )

        res = await client.get("/admin/api/life")
        assert res.status_code == 200
        by_id = {item["id"]: item for item in res.json()["intents"]}
        assert by_id["hist-admin-qq"]["action_name"] == "qq.speak"
        assert by_id["hist-admin-qq"]["executor_module"] == "social_presence"
        assert by_id["hist-admin-tg"]["action_name"] == "telegram.speak"
        assert by_id["hist-admin-tg"]["executor_module"] == "social_presence"


async def test_disabling_social_presence_cancels_all_legacy_speak_and_null_action_intents(
    settings: Any, store: Any
) -> None:
    """Warning 4: set_module_enabled('social_presence', False) cancels qq/tg/dc/NULL speak rows."""
    s = _dual_platform_settings(settings, primary="onebot")
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        await svc.life_engine.initialize()
        rows = [
            ("pend-qq", "qq_social", "group_share", "qq.speak", "onebot:40000"),
            ("pend-tg", "telegram", "speak", "telegram.speak", f"telegram:{GROUP_ID}"),
            ("pend-dc", "discord", "speak", "discord.speak", "discord:999"),
            ("pend-null-qq", "qq_social", "group_share", None, "onebot:40000"),
            ("pend-unrelated", "minecraft", "explore", "mc.explore", ""),
        ]
        async with svc.store.transaction():
            for row_id, mod, verb, act_name, scope in rows:
                await svc.store.db.execute(
                    "INSERT INTO scheduled_intents("
                    "id,created_at,updated_at,execute_after,expire_at,target_module,"
                    "action_type,action_name,target_scope,status,mood,"
                    "intent_note_json,gate_condition_json"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        row_id,
                        NOW,
                        NOW,
                        NOW + 60,
                        NOW + 3600,
                        mod,
                        verb,
                        act_name,
                        scope,
                        "pending",
                        "curious",
                        "{}",
                        "{}",
                    ),
                )

        sp = svc.modules.get("social_presence")
        assert sp is not None
        await svc.life_engine.set_module_enabled(sp, False)

        for row_id in ("pend-qq", "pend-tg", "pend-dc", "pend-null-qq"):
            got = await svc.store.one(
                "SELECT status, result_reason FROM scheduled_intents WHERE id=?", (row_id,)
            )
            assert got is not None
            assert (got["status"], got["result_reason"]) == ("gated_skip", "module_disabled")

        keep = await svc.store.one(
            "SELECT status, result_reason FROM scheduled_intents WHERE id='pend-unrelated'"
        )
        assert keep is not None
        assert (keep["status"], keep["result_reason"]) == ("pending", "")
    finally:
        await svc.stop()


async def test_catalog_compatibility_resolve_for_social_presence_and_plat_speak(
    settings: Any, store: Any
) -> None:
    """compatibility=True resolves ('social_presence', 'speak', True) & ('<plat>','speak',True)."""
    s = _dual_platform_settings(settings, primary="onebot")
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        cat = svc.modules.catalog
        assert ("social_presence", "speak", True) in cat.aliases

        spec_sp = cat.resolve(
            {
                "target_module": "social_presence",
                "action_type": "speak",
                "target_scope": "owner:20000",
                "origin": "planner",
            },
            compatibility=True,
        )
        assert spec_sp.name == "social.speak"

        spec_dc = cat.resolve(
            {
                "action_name": "discord.speak",
                "target_module": "discord",
                "action_type": "speak",
                "target_scope": "discord:123",
                "origin": "planner",
            },
            compatibility=True,
        )
        assert spec_dc.name == "social.speak"

        with pytest.raises(ValueError, match="incompatible_action"):
            cat.resolve(
                {
                    "action_name": "mc.build_plan",
                    "target_module": "social_presence",
                    "action_type": "speak",
                    "target_scope": "owner:20000",
                    "origin": "planner",
                },
                compatibility=True,
            )
    finally:
        await svc.stop()
