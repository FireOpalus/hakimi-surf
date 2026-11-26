"""Tests for Phase 2 (R1 & R2): unified social addressing, private events, and neutral rules."""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from hakimi.life_engine import LifeEngine
from hakimi.life_rules import RuleDispatcher, milestone_sharing
from hakimi.modules.actions import Action, ActionCatalog, Outcome
from hakimi.modules.base import BaseModule, ModuleRegistry
from hakimi.modules.milestones import milestone_sharing as qq_milestone_sharing
from hakimi.modules.qq_social import QQSocialModule
from hakimi.plugin_bus.proxy import RemoteModuleProxy
from hakimi.service import Service
from hakimi.social import (
    OwnerBinding,
    RecipientResolver,
    is_verified_owner_scope,
    owner_binding,
    owner_dm_scope,
    owner_target_scope,
    resolve_owner_scope,
)
from hakimi.world_state import private_event_audience, private_event_for_owner
from tests.fakes import FakeBili, FakeGateway, FakeSender

TG_BOT_ID = "99887766"
TG_OWNER_UID = "555000111"
TG_GROUP_ID = "-1001234567890"


class _FakeChatPlugin(BaseModule):
    def __init__(
        self,
        settings,
        *,
        module_id: str = "telegram",
        platform: str = "telegram",
        groups: list[str] | None = None,
        connected: bool = True,
        actions: tuple[Action, ...] | None = None,
    ) -> None:
        super().__init__(settings)
        self.module_id = module_id
        self.chat_platforms = (platform,)
        self.recipient_namespaces = (platform,)
        self._platform = platform
        self._groups = groups or []
        self._connected = connected
        self._actions = actions

    def chat_connected(self, platform: str) -> bool:
        return self.enabled and self._connected and platform == self._platform

    def normalize_recipient(self, scope: str) -> str | None:
        raw = str(scope or "").strip()
        return raw if raw in self._groups else None

    def permits_recipient(self, scope: str, config) -> bool:
        return self.enabled and scope in self._groups

    def proactive_groups(self) -> list[str]:
        return list(self._groups) if self.enabled else []

    def actions(self) -> tuple[Action, ...]:
        if self._actions is not None:
            return self._actions

        async def _noop(_call):
            return Outcome("done", "ok")

        return (
            Action(
                name=f"{self._platform}.speak",
                description="fake speak",
                social=True,
                proactive_speak=True,
                run=_noop,
            ),
        )

    async def _raw_sensor_summary(self) -> str:
        return f"{self._platform}:ok"


class _PlanGateway(FakeGateway):
    def __init__(self, plan_payload: dict) -> None:
        super().__init__()
        self._plan_payload = plan_payload

    async def generate(self, payload, subject, **kwargs):
        self.calls.append((subject, payload, kwargs))
        if kwargs.get("life_plan"):
            return json.dumps(self._plan_payload, ensure_ascii=False)
        return await super().generate(payload, subject, **kwargs)


def _configure_telegram(settings, *, keep_onebot: bool = False) -> None:
    if not keep_onebot:
        settings.onebot_enabled = False
        settings.qq_enabled = False
        settings.owner_qq_id = ""
    else:
        settings.onebot_enabled = True
        settings.onebot_self_id = "10000"
        settings.owner_qq_id = "20000"
    settings.owner_delivery_platform = "telegram"
    settings.social_owner_bindings = {
        "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_UID),
    }


def test_unified_owner_addressing_and_resolver_invariants(settings):
    settings.onebot_enabled = True
    settings.onebot_self_id = "10000"
    settings.owner_qq_id = "20000"
    settings.social_owner_bindings = {
        "telegram": OwnerBinding(bot_id=TG_BOT_ID, user_id=TG_OWNER_UID),
    }

    assert owner_target_scope(settings, "onebot") == "owner:20000"
    assert owner_target_scope(settings, "qq") is None

    tg_scope = owner_target_scope(settings, "telegram")
    tg_digest = owner_dm_scope(settings, "telegram")
    assert tg_scope == f"boundowner:{tg_digest}"
    assert TG_OWNER_UID not in tg_scope
    assert not tg_scope.startswith("owner:")

    assert resolve_owner_scope("owner:20000", settings)[0] == "onebot"
    matched_tg = resolve_owner_scope(tg_scope, settings)
    assert matched_tg is not None
    assert matched_tg[0] == "telegram"
    assert matched_tg[1].user_id == TG_OWNER_UID

    # Non-ASCII or malformed inputs never raise TypeError and fail closed.
    assert resolve_owner_scope("owner:主人", settings) is None
    assert is_verified_owner_scope("owner:主人", settings) is False
    assert resolve_owner_scope("boundowner:主人", settings) is None
    assert is_verified_owner_scope("boundowner:主人", settings) is False
    assert is_verified_owner_scope("owner:99999", settings) is False
    assert is_verified_owner_scope("boundowner:deadbeef", settings) is False

    settings.owner_delivery_platform = "telegram"
    resolver = RecipientResolver(settings)
    assert resolver.normalize("owner") == tg_scope
    assert resolver.normalize("owner_dm") == tg_scope
    assert resolver.normalize("private:20000") == "owner:20000"
    assert resolver.normalize("private:99999") is None
    assert resolver.normalize(f"channel:telegram:{TG_GROUP_ID}") is None
    assert resolver.normalize(f"telegram:{TG_OWNER_UID}") is None


async def test_owner_proactive_reuses_social_digest_and_fail_closed_when_disabled(settings, store):
    _configure_telegram(settings)
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
    svc.modules.register(tg)
    try:
        expected = owner_target_scope(settings, "telegram")
        assert svc.proactive.target_scope == expected
        assert TG_OWNER_UID not in svc.proactive.target_scope
        assert is_verified_owner_scope(expected, settings) is True
        assert svc.modules.normalize_recipient("owner") == expected
        assert svc.modules.normalize_recipient(expected) == expected
        tg_group = f"telegram:{TG_GROUP_ID}"
        assert svc.modules.normalize_recipient(tg_group) == tg_group
        assert svc.modules.normalize_recipient(f"channel:telegram:{TG_GROUP_ID}") is None
        assert svc.modules.normalize_recipient(f"telegram:{TG_OWNER_UID}") is None

        # Disabling the module immediately revokes bound owner scope inside Service.
        tg.enabled = False
        assert is_verified_owner_scope(expected, settings) is False
        assert svc.modules.normalize_recipient(expected) is None
        assert svc.modules.permits_recipient(expected, svc.life_engine.config) is False
    finally:
        await svc.stop()


async def test_private_event_for_owner_cross_platform_isolation_and_rule_integration(
    settings, store
):
    _configure_telegram(settings, keep_onebot=True)
    tg_scope = owner_target_scope(settings, "telegram")
    ob_scope = owner_target_scope(settings, "onebot")
    tg_audience = private_event_audience(
        settings, TG_OWNER_UID, platform="telegram", bot_id=TG_BOT_ID
    )
    ob_audience = private_event_audience(settings, "20000", platform="onebot", bot_id="10000")

    tg_payload = {"private_first": True, "owner_private": True, "audience": tg_audience}
    ob_payload = {"private_first": True, "owner_private": True, "audience": ob_audience}

    # Standalone validation
    assert private_event_for_owner(tg_payload, tg_scope, settings) is True
    assert private_event_for_owner(tg_payload, ob_scope, settings) is False
    assert private_event_for_owner(ob_payload, tg_scope, settings) is False
    assert (
        private_event_for_owner(
            {"owner_private": False, "audience": tg_audience}, tg_scope, settings
        )
        is False
    )
    wrong_bot_aud = private_event_audience(
        settings, TG_OWNER_UID, platform="telegram", bot_id="other-bot"
    )
    assert (
        private_event_for_owner(
            {"private_first": True, "owner_private": True, "audience": wrong_bot_aud},
            tg_scope,
            settings,
        )
        is False
    )

    # Service integration: Telegram private_first Bili event -> RuleDispatcher -> evidence()
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
    svc.modules.register(tg)
    try:
        now = datetime(2026, 11, 26, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
        svc.world_state.clock = lambda: now
        svc.life_engine = LifeEngine(svc, clock=lambda: now)
        await svc.life_engine.initialize()

        ev = await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {
                "bvid": "BV1xx411c7mD",
                "private_first": True,
                "owner_private": True,
                "audience": tg_audience,
            },
            importance="high",
            now=now,
        )
        rows = await store.all(
            "SELECT action_name, target_scope FROM scheduled_intents WHERE source_event_id=?",
            (ev.id,),
        )
        assert [(r["action_name"], r["target_scope"]) for r in rows] == [("social.speak", tg_scope)]
        assert TG_OWNER_UID not in rows[0]["target_scope"]

        ticket = await store.one(
            "SELECT * FROM scheduled_intents WHERE source_event_id=?",
            (ev.id,),
        )
        evidence = await svc.life_engine.evidence(dict(ticket))
        assert evidence["source_module"] == "bilibili_surf"
        assert "audience" not in evidence["payload"]

        # Fail closed when Telegram module is disabled
        tg.enabled = False
        assert private_event_for_owner(tg_payload, tg_scope, settings) is False
    finally:
        await svc.stop()


async def test_planner_quota_and_resolve_across_platforms_and_m2_forward_compat(settings, store):
    _configure_telegram(settings, keep_onebot=True)
    # Keep OneBot as primary owner_delivery_platform while Telegram is also bound and active.
    settings.owner_delivery_platform = "onebot"
    tg_scope = owner_target_scope(settings, "telegram")

    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
    svc.modules.register(tg)
    try:
        # GatekeeperWorker.quota_for recognizes non-selected boundowner scopes
        tg_spec = svc.life_engine.catalog.actions["telegram.speak"]
        # Attach an owner_only quota rule on tg_spec clone to verify owner_only selection
        from hakimi.modules.scheduling import ActivityQuota

        quota_spec = Action(
            name="telegram.quota_probe",
            description="probe",
            social=True,
            proactive_speak=True,
            quotas=(ActivityQuota("owner_dms", owner_only=True), ActivityQuota()),
            run=tg_spec.run,
        )
        rule = svc.life_engine.gatekeeper.quota_for(quota_spec, tg_scope)
        assert rule is not None and rule.owner_only is True

        # Disabling tg revokes owner_only quota matching
        tg.enabled = False
        fallback_rule = svc.life_engine.gatekeeper.quota_for(quota_spec, tg_scope)
        assert fallback_rule is not None and fallback_rule.owner_only is False
        tg.enabled = True

        # LifeEngine.resolve accepts telegram.speak with boundowner even when selected is onebot
        spec, _ = svc.life_engine.resolve(
            {"action_name": "telegram.speak", "target_scope": tg_scope, "origin": "planner"}
        )
        assert spec.name == "telegram.speak"

        # Explicit telegram.speak rejects owner:20000; qq.speak aliases social.speak -> tg_scope
        with pytest.raises(ValueError, match="unauthorized_scope"):
            svc.life_engine.resolve(
                {
                    "action_name": "telegram.speak",
                    "target_scope": "owner:20000",
                    "origin": "planner",
                }
            )
        aliased, _ = svc.life_engine.resolve(
            {"action_name": "qq.speak", "target_scope": tg_scope, "origin": "planner"}
        )
        assert aliased.name == "social.speak"

        # Built-in SocialPresenceModule mounts unified social.speak (chat_platforms == ())
        u1, _ = svc.life_engine.resolve(
            {"action_name": "social.speak", "target_scope": "owner:20000", "origin": "planner"}
        )
        u2, _ = svc.life_engine.resolve(
            {"action_name": "social.speak", "target_scope": tg_scope, "origin": "planner"}
        )
        assert u1.name == "social.speak" and u2.name == "social.speak"
    finally:
        await svc.stop()


async def test_life_block_planner_resolves_owner_alias_to_blind_boundowner(settings, store):
    _configure_telegram(settings)
    now = datetime(2026, 11, 26, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    plan_payload = {
        "current_module": "idle",
        "focus_level": "LOW",
        "mood": "cozy",
        "mood_longnick": "",
        "rest_minutes": 0,
        "intents": [
            {
                "action_name": "telegram.speak",
                "arguments_json": "{}",
                "target_scope": "owner",
                "delay_minutes": 10,
                "ttl_minutes": 60,
                "intent_note": "跟BOISER打个招呼问问近况",
                "allow_desktop_care": False,
            }
        ],
        "agenda_changes": [],
        "ledger_posts": [],
        "ledger_consumes": [],
    }
    svc = Service(settings, store, _PlanGateway(plan_payload), FakeBili(), FakeSender())
    tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
    svc.modules.register(tg)
    try:
        svc.world_state.clock = lambda: now
        svc.life_engine = LifeEngine(svc, clock=lambda: now)
        await svc.life_engine.initialize()

        res = await svc.life_engine.planner.plan_once(now=now)
        assert res["intents_enqueued"] == 1
        rows = await store.all("SELECT action_name, target_scope FROM scheduled_intents")
        expected = owner_target_scope(settings, "telegram")
        assert [(r["action_name"], r["target_scope"]) for r in rows] == [
            ("telegram.speak", expected)
        ]
        assert TG_OWNER_UID not in rows[0]["target_scope"]
    finally:
        await svc.stop()


async def test_pure_non_qq_milestone_routing_and_semantic_proactive_gate(settings, store):
    # Pure non-QQ setup: onebot disabled, owner_qq_id empty, qq_social disabled
    _configure_telegram(settings, keep_onebot=False)
    assert QQSocialModule(settings).event_rules() == ()
    assert qq_milestone_sharing is milestone_sharing

    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    qq_mod = svc.modules.get("qq_social")
    assert qq_mod is not None
    qq_mod.enabled = False
    tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
    svc.modules.register(tg)
    try:
        now = datetime(2026, 11, 26, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
        svc.world_state.clock = lambda: now
        svc.life_engine = LifeEngine(svc, clock=lambda: now)
        await svc.life_engine.initialize()

        # 1) Public high-importance Bili milestone routes to social.speak + boundowner
        ev_pub = await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {"bvid": "BV1xx411c7mD"},
            importance="high",
            now=now,
        )
        rows = await store.all(
            "SELECT action_name, target_scope FROM scheduled_intents WHERE source_event_id=?",
            (ev_pub.id,),
        )
        expected = owner_target_scope(settings, "telegram")
        assert [(r["action_name"], r["target_scope"]) for r in rows] == [("social.speak", expected)]

        # 2) Without any Owner bound, bili_video_watched NEVER falls back to group,
        #    while non-bili milestone (minecraft discovery) falls back to Telegram group.
        settings.social_owner_bindings = {}
        ev_bili_no_owner = await svc.world_state.publish_event(
            "bilibili_surf",
            "bili_video_watched",
            {"bvid": "BV1ab411c7mD"},
            importance="high",
            now=now + 700,
        )
        assert (
            await store.one(
                "SELECT COUNT(*) FROM scheduled_intents WHERE source_event_id=?",
                (ev_bili_no_owner.id,),
            )
        )[0] == 0

        ev_mc = await svc.world_state.publish_event(
            "minecraft",
            "discovery",
            {"block": "diamond"},
            importance="high",
            now=now + 1400,
        )
        mc_rows = await store.all(
            "SELECT action_name, target_scope FROM scheduled_intents WHERE source_event_id=?",
            (ev_mc.id,),
        )
        assert [(r["action_name"], r["target_scope"]) for r in mc_rows] == [
            ("social.speak", f"telegram:{TG_GROUP_ID}")
        ]
    finally:
        await svc.stop()


def test_action_proactive_speak_validation_and_semantic_rule_check(settings):
    async def _noop(_call):
        return Outcome("done", "ok")

    cat = ActionCatalog()
    with pytest.raises(ValueError, match="proactive_speak actions must be social"):
        cat.install(
            "bad",
            (
                Action(
                    name="bad.speak",
                    description="bad",
                    social=False,
                    proactive_speak=True,
                    run=_noop,
                ),
            ),
        )

    cat.install(
        "custom",
        (
            Action(
                name="custom.notify",
                description="proactive",
                social=True,
                proactive_speak=True,
                run=_noop,
            ),
            Action(
                name="custom.silent",
                description="non-proactive",
                social=True,
                proactive_speak=False,
                run=_noop,
            ),
        ),
    )

    class _DummyEngine:
        def __init__(self, cfg_settings):
            self.s = cfg_settings
            self.catalog = cat
            self.service = type(
                "Svc",
                (),
                {"world_state": None, "store": None},
            )()
            self.store = None

    dispatcher = RuleDispatcher(_DummyEngine(settings), ModuleRegistry())
    assert dispatcher._is_proactive_speak_action("custom.notify") is True
    assert dispatcher._is_proactive_speak_action("custom.silent") is False
    assert dispatcher._is_proactive_speak_action("missing.action") is False


def test_remote_module_proxy_proactive_groups_respects_ready_enabled_closed(settings):
    class _DummySess:
        def __init__(self, ready: bool, closed: bool) -> None:
            self.ready = ready
            self.closed = closed
            self.plugin_id = "telegram"

    sess = _DummySess(ready=True, closed=False)
    proxy = RemoteModuleProxy(
        settings,
        bus=object(),  # type: ignore[arg-type]
        session=sess,  # type: ignore[arg-type]
        plugin_id="telegram",
        display_name="Telegram",
        description="tg",
        node_region="KR",
        scopes=frozenset({"chat"}),
        chat_platforms=("telegram",),
        thinking_scenes=(),
        actions=(),
        capabilities=(),
        initial_status={"connected": True, "proactive_groups": [TG_GROUP_ID]},
    )
    assert proxy.proactive_groups() == [f"telegram:{TG_GROUP_ID}"]
    proxy.enabled = False
    assert proxy.proactive_groups() == []
    proxy.enabled = True
    sess.ready = False
    assert proxy.proactive_groups() == []
    sess.ready = True
    sess.closed = True
    assert proxy.proactive_groups() == []


async def test_normalize_recipient_owner_alias_fail_closed_when_external_delivery_disabled(
    settings, store
):
    _configure_telegram(settings, keep_onebot=True)
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        # Telegram is selected for owner_delivery_platform, but module is not registered yet.
        assert svc.modules.normalize_recipient("owner") is None
        assert svc.modules.normalize_recipient("owner_dm") is None
        assert RecipientResolver(settings, svc.modules).normalize("owner") is None
        assert RecipientResolver(settings, svc.modules).normalize("owner_dm") is None

        tg = _FakeChatPlugin(settings, groups=[f"telegram:{TG_GROUP_ID}"])
        svc.modules.register(tg)
        expected = owner_target_scope(settings, "telegram")
        assert svc.modules.normalize_recipient("owner") == expected
        assert svc.modules.normalize_recipient("owner_dm") == expected

        # Disabling telegram fails closed instead of falling back to active qq_social/OneBot.
        tg.enabled = False
        assert svc.modules.normalize_recipient("owner") is None
        assert svc.modules.normalize_recipient("owner_dm") is None
        assert RecipientResolver(settings, svc.modules).normalize("owner") is None
        assert RecipientResolver(settings, svc.modules).normalize("owner_dm") is None
    finally:
        await svc.stop()


async def test_onebot_owner_scope_fail_closed_when_qq_social_disabled(settings, store):
    from hakimi.modules.scheduling import ActivityQuota

    settings.onebot_enabled = True
    settings.onebot_self_id = "10000"
    settings.owner_qq_id = "20000"
    settings.owner_delivery_platform = "onebot"

    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    qq_mod = svc.modules.get("qq_social")
    assert qq_mod is not None
    ob_audience = private_event_audience(settings, "20000", platform="onebot", bot_id="10000")
    ob_payload = {"private_first": True, "owner_private": True, "audience": ob_audience}
    try:
        # OwnerProactive.__init__ ran before install_modules and still derived target_scope.
        assert svc.proactive.target_scope == "owner:20000"
        assert is_verified_owner_scope("owner:20000", settings) is True
        assert private_event_for_owner(ob_payload, "owner:20000", settings) is True

        qq_spec = svc.life_engine.catalog.actions["social.speak"]
        quota_spec = Action(
            name="qq.quota_probe",
            description="probe",
            social=True,
            proactive_speak=True,
            quotas=(ActivityQuota("owner_dms", owner_only=True), ActivityQuota()),
            run=qq_spec.run,
        )
        assert svc.life_engine.gatekeeper.quota_for(quota_spec, "owner:20000").owner_only is True

        u_ok, _ = svc.life_engine.resolve(
            {"action_name": "social.speak", "target_scope": "owner:20000", "origin": "planner"}
        )
        assert u_ok.name == "social.speak"

        # Disabling qq_social revokes OneBot owner verification symmetrically with external plugins.
        qq_mod.enabled = False
        assert owner_binding(settings, "onebot") is None
        assert resolve_owner_scope("owner:20000", settings) is None
        assert is_verified_owner_scope("owner:20000", settings) is False
        assert private_event_for_owner(ob_payload, "owner:20000", settings) is False
        assert svc.life_engine.gatekeeper.quota_for(quota_spec, "owner:20000").owner_only is False
        assert svc.modules.normalize_recipient("owner") is None
        assert svc.modules.normalize_recipient("owner_dm") is None
        assert RecipientResolver(settings, svc.modules).normalize("owner:20000") is None
        with pytest.raises(ValueError, match="unauthorized_scope"):
            svc.life_engine.resolve(
                {"action_name": "qq.speak", "target_scope": "owner:20000", "origin": "planner"}
            )
        with pytest.raises(ValueError, match="unauthorized_scope"):
            svc.life_engine.resolve(
                {"action_name": "social.speak", "target_scope": "owner:20000", "origin": "planner"}
            )
        # include_inactive=True remains intact for pre-connect / boot initialization.
        assert owner_target_scope(settings, "onebot", include_inactive=True) == "owner:20000"
    finally:
        await svc.stop()
