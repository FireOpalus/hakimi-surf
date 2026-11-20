import asyncio
import json
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from cryptography.fernet import Fernet
from pydantic import SecretStr, ValidationError

from hakimi.affection import (
    TIER_CAPS,
    AffectionConfig,
    AffectionManager,
    capabilities_for_score,
    tier_matrix_rows,
)
from hakimi.affection_admin import router as affection_router
from hakimi.app import create_app
from hakimi.bot_server import prompt_context
from hakimi.config import Settings
from hakimi.context_budget import estimate_tokens
from hakimi.identity import IdentityService
from hakimi.moderation import Moderation
from hakimi.outbound_actions import ACTION_INSTRUCTION, ChatActions, parse_actions
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN
from tests.test_streaming_v0100 import ActionSender


def _enable_affection(settings: Settings) -> Settings:
    settings.affection_enabled = True
    return settings


def _member_msg(message, *, user_id="30000", event_id="ev-1", nickname="普通群友", kind="group"):
    return message.model_copy(
        update={
            "user_id": user_id,
            "event_id": event_id,
            "sender_nickname": nickname,
            "kind": kind,
            "conversation_id": "40000" if kind == "group" else user_id,
        }
    )


@pytest.fixture
async def aff_env(settings, store):
    _enable_affection(settings)
    wire = SimpleNamespace(call=AsyncMock(return_value=[]))
    svc = SimpleNamespace(
        s=settings,
        store=store,
        sender=SimpleNamespace(onebot=wire),
        proactive=SimpleNamespace(alert=AsyncMock()),
    )
    clock = [1_700_000_000.0]
    mod = Moderation(svc, clock=lambda: clock[0])
    await mod.initialize()
    aff = AffectionManager(store, clock=lambda: clock[0])
    store.runtime.affection = aff
    store.runtime.moderation = mod
    await aff.initialize()
    return aff, mod, clock


@pytest.fixture
async def affection_api(settings):
    _enable_affection(settings)
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    gateway = FakeGateway()
    app = create_app(settings, gateway_factory=lambda *_: gateway, start_workers=False)
    if not any(getattr(r, "path", "").startswith("/admin/api/affection") for r in app.routes):
        app.include_router(affection_router)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            yield app, client, gateway


def test_settings_affection_default_and_env_aliases(tmp_path, monkeypatch):
    key = Fernet.generate_key().decode()
    default_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "default",
        gemini_api_key="test-gemini-key-never-live",
        encryption_key=key,
    )
    assert default_settings.affection_enabled is True

    monkeypatch.setenv("AFFECTION_ENABLED", "false")
    env_settings = Settings(
        _env_file=None,
        data_dir=tmp_path / "env",
        gemini_api_key="test-gemini-key-never-live",
        encryption_key=key,
    )
    assert env_settings.affection_enabled is False


def test_conftest_settings_isolates_affection(settings):
    assert settings.affection_enabled is False


def test_affection_config_validation():
    cfg = AffectionConfig()
    assert cfg.enabled is True
    assert cfg.initial_score == 50
    assert cfg.daily_positive_cap == 8
    assert cfg.unmute_recovery_score == 25
    assert cfg.farewell_message
    assert cfg.cooldown_recovery_days == 7
    assert cfg.cooldown_recovery_target == 40
    assert cfg.penalize_rate_limit is False
    assert cfg.penalize_poke_ignored is False

    for bad in (
        {"enabled": "yes"},
        {"initial_score": 0},
        {"initial_score": 101},
        {"daily_positive_cap": -1},
        {"daily_positive_cap": 51},
        {"unmute_recovery_score": 0},
        {"unmute_recovery_score": 61},
        {"farewell_message": "   "},
        {"farewell_message": "x" * 121},
        {"cooldown_recovery_days": -1},
        {"cooldown_recovery_days": 91},
        {"cooldown_recovery_days": True},
        {"cooldown_recovery_target": 19},
        {"cooldown_recovery_target": 61},
        {"cooldown_recovery_target": True},
        {"penalize_rate_limit": "yes"},
        {"penalize_rate_limit": 1},
        {"penalize_poke_ignored": "no"},
        {"penalize_poke_ignored": 0},
        {"unknown": 1},
    ):
        with pytest.raises(ValidationError):
            AffectionConfig.model_validate(bad)

    edge_cfg = AffectionConfig.model_validate(
        {
            "cooldown_recovery_days": 0,
            "cooldown_recovery_target": 20,
            "penalize_rate_limit": True,
            "penalize_poke_ignored": True,
        }
    )
    assert edge_cfg.cooldown_recovery_days == 0
    assert edge_cfg.cooldown_recovery_target == 20
    assert edge_cfg.penalize_rate_limit is True
    assert edge_cfg.penalize_poke_ignored is True

    edge_cfg_max = AffectionConfig.model_validate(
        {"cooldown_recovery_days": 90, "cooldown_recovery_target": 60}
    )
    assert edge_cfg_max.cooldown_recovery_days == 90
    assert edge_cfg_max.cooldown_recovery_target == 60


async def test_owner_starts_at_120_floor_100_and_never_blacklisted_or_throttled(aff_env, message):
    aff, mod, _ = aff_env
    settings = aff.s
    settings.onebot_owner_ids = {"20000", "20001"}
    settings.qq_owner_ids = {"user-openid"}

    assert aff.is_owner_id("onebot", "20000")
    assert aff.is_owner_id("onebot", "20001")
    assert aff.is_owner_id("qq", "user-openid")
    assert not aff.is_owner_id("onebot", "30000")

    owner_profile = await aff.profile("onebot", "20000", "BOISER")
    assert owner_profile["score"] == 120
    assert owner_profile["level"] == 6
    assert owner_profile["tier"] == "owner_bond"
    assert owner_profile["tier_label"] == "专属羁绊 (Owner)"
    assert owner_profile["is_owner"] is True

    # Mild negative delta moves within 100..120, still Lv.6.
    upd = await aff.apply_turn_delta(message, -2, daily_first_bonus=False)
    assert upd.new_score == 118 and upd.tier == "owner_bond" and not upd.blacklisted_now

    # Severe violations and admin attempts below 100 hit the hard floor 100.
    for idx in range(5):
        v_upd = await aff.record_violation(
            message.model_copy(update={"event_id": f"v-{idx}"}), "content_blocked"
        )
        assert v_upd.new_score >= 100
        assert v_upd.tier == "owner_bond"
        assert v_upd.blacklisted_now is False
        assert v_upd.farewell_message is None

    forced = await aff.set_user_score("20000", 0)
    assert forced["score"] == 100
    assert forced["tier"] == "owner_bond"
    assert forced["level"] == 6
    assert "20000" not in settings.muted_user_ids

    # QQ owner also holds Lv.6 100..120 immunity.
    qq_owner_msg = message.model_copy(
        update={
            "platform": "qq",
            "bot_id": settings.qq_app_id,
            "user_id": "user-openid",
            "conversation_id": "user-openid",
            "event_id": "qq-1",
        }
    )
    qq_upd = await aff.record_violation(qq_owner_msg, "prompt_injection")
    assert qq_upd.new_score == 100 and qq_upd.tier == "owner_bond"
    caps = await aff.capabilities_for(qq_owner_msg)
    assert caps.direct_reply_prob == 1.0 and caps.allow_carbon is True and caps.max_tier == 4

    # Re-apply positive turn back toward 120, never exceeding 120.
    await aff.set_user_score("20000", 119)
    top = await aff.apply_turn_delta(message.model_copy(update={"event_id": "up-1"}), 2)
    assert top.new_score == 120


async def test_non_owner_cap_daily_plus8_rollover_and_sandbox_lock(aff_env, message):
    aff, _, clock = aff_env
    user = _member_msg(message, user_id="30000", event_id="m-0")

    # Initial score 50, never exceeds 100.
    p0 = await aff.profile("onebot", "30000", "小明")
    assert p0["score"] == 50 and p0["tier"] == "normal"
    capped_admin = await aff.set_user_score("30000", 120)
    assert capped_admin["score"] == 100 and capped_admin["tier"] == "bestie"

    # Reset to 50 to test daily +8 cap and daily_first_bonus.
    await aff.set_user_score("30000", 50)

    # Sandbox locks positive deltas AND does NOT consume daily first bonus.
    sb_pos = await aff.apply_turn_delta(
        _member_msg(message, event_id="sb-1"), 2, sandbox_mode=True, daily_first_bonus=True
    )
    assert sb_pos.new_score == 50 and sb_pos.delta == 0 and sb_pos.today_positive_gain == 0

    # Sandbox allows negative deltas.
    sb_neg = await aff.apply_turn_delta(
        _member_msg(message, event_id="sb-2"), -2, sandbox_mode=True, daily_first_bonus=True
    )
    assert sb_neg.new_score == 48 and sb_neg.delta == -2

    # First real non-sandbox message of the day gets +1 daily_first_bonus + model_delta=2 -> +3.
    t1 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-1"), 2, daily_first_bonus=True
    )
    assert t1.delta == 3 and t1.new_score == 51 and t1.today_positive_gain == 3

    # Second & third messages same day do NOT get daily_first_bonus again (+2 each -> +7).
    t2 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-2"), 2, daily_first_bonus=True
    )
    assert t2.delta == 2 and t2.new_score == 53 and t2.today_positive_gain == 5

    t3 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-3"), 2, daily_first_bonus=True
    )
    assert t3.delta == 2 and t3.new_score == 55 and t3.today_positive_gain == 7

    # Fourth message same day hits daily_positive_cap=8 -> only +1 allowed.
    t4 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-4"), 2, daily_first_bonus=True
    )
    assert t4.delta == 1 and t4.new_score == 56 and t4.today_positive_gain == 8

    # Fifth message same day -> positive gain capped at 0.
    t5 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-5"), 2, daily_first_bonus=True
    )
    assert t5.delta == 0 and t5.new_score == 56 and t5.today_positive_gain == 8

    # Negative delta still applies same day without changing today_positive_gain.
    t6 = await aff.apply_turn_delta(_member_msg(message, event_id="d1-6"), -1)
    assert t6.delta == -1 and t6.new_score == 55 and t6.today_positive_gain == 8

    # Advance clock +24h: daily cap and daily_first_bonus reset.
    clock[0] += 86400.0
    next_day = await aff.apply_turn_delta(
        _member_msg(message, event_id="d2-1"), 2, daily_first_bonus=True
    )
    assert next_day.delta == 3 and next_day.new_score == 58 and next_day.today_positive_gain == 3
    assert user.user_id == "30000"


async def test_violations_annoyance_blacklist_and_unmute_recovery(aff_env, message):
    aff, mod, clock = aff_env
    # Start at 50 -> prompt_injection (-25) -> 25; duplicate event_id is idempotent.
    m1 = _member_msg(message, event_id="inj-1", nickname="捣蛋鬼")
    u1 = await aff.record_violation(m1, "prompt_injection")
    assert u1.previous_score == 50 and u1.new_score == 25 and u1.delta == -25
    assert not u1.blacklisted_now

    u1_dup = await aff.record_violation(m1, "prompt_injection")
    assert u1_dup.new_score == 25 and u1_dup.delta == 0 and not u1_dup.changed

    # Annoyance (-3) with 60s cooldown.
    a1 = await aff.record_annoyance(_member_msg(message, event_id="spam-1"), "spam")
    assert a1.new_score == 22 and a1.delta == -3
    a2 = await aff.record_annoyance(_member_msg(message, event_id="spam-2"), "spam")
    assert a2.new_score == 22 and a2.delta == 0 and not a2.changed
    clock[0] += 61.0
    a3 = await aff.record_annoyance(_member_msg(message, event_id="spam-3"), "spam")
    assert a3.new_score == 19 and a3.delta == -3 and a3.tier == "hostile"

    # content_blocked (-30) crosses 0 -> blacklisted + farewell + moderation mute!
    u2 = await aff.record_violation(_member_msg(message, event_id="blk-1"), "content_blocked")
    assert u2.previous_score == 19 and u2.new_score == 0
    assert u2.tier == "blacklisted"
    assert u2.blacklisted_now is True
    assert u2.farewell_message == aff.config.farewell_message
    assert "30000" in aff.s.muted_user_ids

    muted_row = await aff.store.one(
        "SELECT source FROM muted_users WHERE user_key=?", (mod.key("30000"),)
    )
    assert muted_row is not None and muted_row["source"] == "ai_affection_zero"

    # Further violations when already 0 do not re-emit blacklisted_now.
    u3 = await aff.record_violation(_member_msg(message, event_id="blk-2"), "prompt_injection")
    assert u3.new_score == 0 and u3.blacklisted_now is False and u3.farewell_message is None

    # Unmuting via Moderation restores score from 0 to unmute_recovery_score (25).
    await mod.set_muted("30000", False)
    assert "30000" not in aff.s.muted_user_ids
    recovered = await aff.profile("onebot", "30000")
    assert recovered["score"] == 25 and recovered["tier"] == "cold"

    # Unmuting a user whose score is non-zero does not overwrite their score.
    await aff.set_user_score("30000", 72)
    await mod.set_muted("30000", True)
    await mod.set_muted("30000", False)
    assert (await aff.profile("onebot", "30000"))["score"] == 72


async def test_seven_tier_capability_matrix_thresholds_and_multipliers(aff_env, message):
    aff, _, _ = aff_env
    msg = _member_msg(message, user_id="30000")
    subject = aff.store.subject(msg)

    expected = [
        # score, lvl, tier, prob, poke_in, imgs, low_res, social, file_poke, video, carbon, tier_cap
        (0, 0, "blacklisted", 0.0, False, 0, True, False, False, False, False, 1, Decimal("0")),
        (10, 1, "hostile", 0.50, False, 0, True, False, False, False, False, 1, Decimal("0.30")),
        (30, 2, "cold", 0.85, False, 1, True, False, False, False, False, 2, Decimal("0.60")),
        (50, 3, "normal", 1.0, True, 2, False, True, False, False, False, 3, Decimal("1.00")),
        (70, 4, "familiar", 1.0, True, 2, False, True, True, False, False, 3, Decimal("1.15")),
        (90, 5, "bestie", 1.0, True, 2, False, True, True, True, False, 3, Decimal("1.35")),
    ]
    for (
        score,
        lvl,
        tier,
        prob,
        poke_in,
        imgs,
        low_res,
        social,
        file_poke,
        video,
        carbon,
        max_tier,
        mult,
    ) in expected:
        await aff.set_user_score("30000", score)
        caps = await aff.capabilities_for(msg)
        assert caps.level == lvl
        assert caps.tier == tier
        assert caps.direct_reply_prob == prob
        assert caps.allow_poke_inbound is poke_in
        assert caps.max_images == imgs
        assert caps.low_res_images_only is low_res
        assert caps.allow_sticker is social
        assert caps.allow_emoji_like is social
        assert caps.allow_bili_summary is social
        assert caps.allow_search is social
        assert caps.allow_text_card is social
        assert caps.allow_dual_profile is social
        assert caps.allow_file_read is file_poke
        assert caps.allow_poke is file_poke
        assert caps.allow_video_watch is video
        assert caps.allow_carbon is carbon
        assert caps.owner_exclusive is False
        assert caps.max_tier == max_tier
        assert caps.budget_multiplier == mult
        assert aff.budget_multiplier_for_subject(subject) == mult

    # Threshold formulas across all 7 tiers.
    await aff.set_user_score("30000", 0)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(1.42)
    await aff.set_user_score("30000", 10)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.95)
    await aff.set_user_score("30000", 30)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.58)
    await aff.set_user_score("30000", 50)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.42)
    await aff.set_user_score("30000", 70)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.34)
    await aff.set_user_score("30000", 90)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.28)
    assert aff.adjust_reply_threshold(msg, 0.20) == pytest.approx(0.12)

    owner_caps = capabilities_for_score(100, is_owner=True)
    assert owner_caps.level == 6 and owner_caps.tier == "owner_bond"
    assert owner_caps.allow_carbon is True and owner_caps.owner_exclusive is True
    assert owner_caps.max_tier == 4 and owner_caps.budget_multiplier == Decimal("2.50")
    assert aff.adjust_reply_threshold(message, 0.42) == pytest.approx(0.22)
    assert aff.adjust_reply_threshold(message, 0.25) == pytest.approx(0.10)
    assert [c.level for c in TIER_CAPS if c.allow_carbon] == [6]
    assert [c.level for c in TIER_CAPS if c.owner_exclusive] == [6]


async def test_disabled_mode_and_known_bot_passthrough_and_encryption(aff_env, message):
    aff, _, _ = aff_env
    aff.s.bot_qq_ids = {"60000"}
    aff.s.onebot_secret = SecretStr("top-secret-token-value")
    msg = _member_msg(message, user_id="30000", nickname="secret: top-secret-token-value")

    await aff.set_user_score("30000", 15, nickname="top-secret-token-value")
    row = await aff.store.one("SELECT payload FROM user_affection")
    assert row is not None
    assert b"top-secret-token-value" not in row["payload"]
    assert b'"score"' not in row["payload"]
    prof = await aff.profile("onebot", "30000")
    assert prof["nickname"] == "群友"

    # Restart restores encrypted scores & multipliers.
    restarted = AffectionManager(aff.store)
    await restarted.initialize()
    assert restarted.budget_multiplier_for_subject(aff.store.subject(msg)) == Decimal("0.30")
    assert restarted.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.95)

    # Known bots bypass affection tracking & get unlocked caps.
    bot_msg = _member_msg(message, user_id="60000")
    bot_caps = await restarted.capabilities_for(bot_msg)
    assert bot_caps.tier == "bot_bypass" and bot_caps.budget_multiplier == Decimal("1.00")
    assert restarted.adjust_reply_threshold(bot_msg, 0.85) == pytest.approx(0.85)
    assert (await restarted.apply_turn_delta(bot_msg, -3)).changed is False

    # Disabled mode acts as zero-op.
    await restarted.set_config(AffectionConfig(enabled=False))
    disabled_caps = await restarted.capabilities_for(msg)
    assert disabled_caps.direct_reply_prob == 1.0
    assert disabled_caps.allow_carbon is True
    assert disabled_caps.budget_multiplier == Decimal("1.00")
    assert restarted.budget_multiplier_for_subject(aff.store.subject(msg)) == Decimal("1")
    assert restarted.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.42)
    assert (await restarted.apply_turn_delta(msg, -3)).changed is False


def test_outbound_actions_clamping_gating_and_static_prefix_tokens(settings, message):
    def make_raw(**extra):
        data = {
            "abstain": False,
            "bubbles": ["好耶"],
            "anchor": {"reply_id": None, "mentions": []},
            "sticker": {"label": "开心", "timing": "after_text"},
            "image_digest": None,
            "emoji_like": "赞",
            "poke_qq": "30000",
            **extra,
        }
        return json.dumps(data, ensure_ascii=False)

    # Clamps affection_delta into [-35, 2], null -> 0, rejects bool in strict schema.
    a_high, _ = parse_actions(
        make_raw(affection_delta=9), public=True, allowed_at={"30000"}, labels={"开心"}
    )
    assert a_high.affection_delta == 2
    a_mid_low, _ = parse_actions(
        make_raw(affection_delta=-25), public=True, allowed_at={"30000"}, labels={"开心"}
    )
    assert a_mid_low.affection_delta == -25
    a_low, _ = parse_actions(
        make_raw(affection_delta=-99), public=True, allowed_at={"30000"}, labels={"开心"}
    )
    assert a_low.affection_delta == -35
    a_none, _ = parse_actions(
        make_raw(affection_delta=None), public=True, allowed_at={"30000"}, labels={"开心"}
    )
    assert a_none.affection_delta == 0
    with pytest.raises(Rejected, match="invalid_action_output"):
        parse_actions(make_raw(affection_delta=True), public=True)

    # Default gating preserves all actions.
    assert a_high.sticker.label == "开心"
    assert a_high.emoji_like == "76"
    assert a_high.poke_qq == "30000"

    # Tier gating suppresses disallowed sticker / emoji_like / poke_qq cleanly.
    gated, _ = parse_actions(
        make_raw(affection_delta=1),
        public=True,
        allowed_at={"30000"},
        labels={"开心"},
        allow_sticker=False,
        allow_emoji_like=False,
        allow_poke_action=False,
    )
    assert gated.sticker.label is None
    assert gated.emoji_like is None
    assert gated.poke_qq is None
    assert gated.affection_delta == 1

    assert "affection_delta" in ACTION_INSTRUCTION
    assert "affection_delta" in ChatActions.model_fields

    pool = StaticPrefixes()
    for msg in (message, _member_msg(message)):
        ctx = prompt_context(msg, settings)
        text = pool.get(ctx, settings.onebot_self_id)
        tokens = estimate_tokens(text)
        assert 4500 <= tokens <= 5200


async def test_affection_admin_disabled_and_unauthenticated(settings):
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app.include_router(affection_router)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            for method, path in (
                ("GET", "/admin/api/affection"),
                ("PUT", "/admin/api/affection"),
                ("DELETE", "/admin/api/affection"),
                ("PUT", "/admin/api/affection/users"),
                ("DELETE", "/admin/api/affection/users/30000"),
            ):
                r = await client.request(method, path, json={})
                assert r.status_code == 403


async def test_affection_admin_crud_and_moderation_sync(affection_api):
    app, client, gateway = affection_api
    svc = app.state.service

    # Bad token rejected before touching DB.
    unauth = await client.get("/admin/api/affection", headers={"Authorization": "Bearer wrong"})
    assert unauth.status_code == 401
    assert not gateway.calls

    # Initial GET
    initial = (await client.get("/admin/api/affection")).json()
    assert initial["source"] == "env"
    assert initial["config"]["enabled"] is True
    assert initial["config"]["initial_score"] == 50
    assert initial["config"]["daily_positive_cap"] == 8
    assert initial["config"]["cooldown_recovery_days"] == 7
    assert initial["config"]["cooldown_recovery_target"] == 40
    assert initial["config"]["penalize_rate_limit"] is False
    assert initial["config"]["penalize_poke_ignored"] is False
    assert len(initial["tiers"]) == 7
    assert initial["tiers"][6]["owner_exclusive"] is True
    assert len(initial["tiers"][6]["exclusive_perks"]) == 5
    assert all(
        t["owner_exclusive"] is False and t["exclusive_perks"] == [] for t in initial["tiers"][:6]
    )
    assert initial["users"] == []

    # Invalid config inputs rejected with 400
    for bad in (
        {},
        {"initial_score": 0},
        {"enabled": "true"},
        {"cooldown_recovery_days": 91},
        {"cooldown_recovery_target": 19},
        {"penalize_rate_limit": "yes"},
        {"penalize_poke_ignored": 1},
        {"unexpected": 1},
    ):
        assert (await client.put("/admin/api/affection", json=bad)).status_code == 400

    # Partial PUT & persistence
    saved = (
        await client.put(
            "/admin/api/affection",
            json={
                "daily_positive_cap": 10,
                "unmute_recovery_score": 30,
                "cooldown_recovery_days": 14,
                "cooldown_recovery_target": 45,
                "penalize_rate_limit": True,
                "penalize_poke_ignored": True,
            },
        )
    ).json()
    assert saved["source"] == "admin"
    assert saved["config"]["daily_positive_cap"] == 10
    assert saved["config"]["unmute_recovery_score"] == 30
    assert saved["config"]["cooldown_recovery_days"] == 14
    assert saved["config"]["cooldown_recovery_target"] == 45
    assert saved["config"]["penalize_rate_limit"] is True
    assert saved["config"]["penalize_poke_ignored"] is True
    assert saved["config"]["initial_score"] == 50

    # Reset config
    reset = (await client.delete("/admin/api/affection")).json()
    assert reset["source"] == "env" and reset["config"]["daily_positive_cap"] == 8
    assert reset["config"]["cooldown_recovery_days"] == 7
    assert reset["config"]["penalize_rate_limit"] is False
    assert reset["config"]["penalize_poke_ignored"] is False

    # Reject invalid user inputs
    for bad_user in (
        {"user_id": "30000", "score": True},
        {"user_id": "30000", "score": 121},
        {"user_id": "bad-id", "platform": "onebot", "score": 50},
        {"user_id": "30000", "score": 50, "extra": 1},
    ):
        assert (await client.put("/admin/api/affection/users", json=bad_user)).status_code == 400

    # Setting non-owner to 0 mutes them with source='ai_affection_zero'
    s0 = (
        await client.put(
            "/admin/api/affection/users",
            json={"user_id": "30000", "score": 0, "nickname": "小黑屋测试", "reason": "test_zero"},
        )
    ).json()
    assert len(s0["users"]) == 1
    assert s0["users"][0]["score"] == 0 and s0["users"][0]["muted"] is True
    assert "30000" in svc.s.muted_user_ids

    # Setting score > 0 unmutes them
    s55 = (
        await client.put(
            "/admin/api/affection/users",
            json={"user_id": "30000", "score": 55},
        )
    ).json()
    assert s55["users"][0]["score"] == 55 and s55["users"][0]["muted"] is False
    assert "30000" not in svc.s.muted_user_ids

    # Delete user when muted by ai_affection_zero -> removes row AND unmutes
    await client.put("/admin/api/affection/users", json={"user_id": "30000", "score": 0})
    assert "30000" in svc.s.muted_user_ids
    deleted = (await client.delete("/admin/api/affection/users/30000")).json()
    assert deleted["users"] == []
    assert "30000" not in svc.s.muted_user_ids

    # Manual mute is NOT removed when affection override is deleted
    await client.put("/admin/api/affection/users", json={"user_id": "30000", "score": 45})
    await svc.moderation.set_muted("30000", True)
    await client.delete("/admin/api/affection/users/30000")
    assert "30000" in svc.s.muted_user_ids
    await svc.moderation.set_muted("30000", False)

    # QQ user via composite key delete
    await client.put(
        "/admin/api/affection/users",
        json={"user_id": "qq-user-1", "platform": "qq", "score": 115},
    )
    qq_list = (await client.get("/admin/api/affection")).json()["users"]
    assert qq_list[0]["user_id"] == "qq-user-1" and qq_list[0]["score"] == 100
    after_del = (await client.delete("/admin/api/affection/users/qq:qq-user-1")).json()
    assert after_del["users"] == []


async def test_severe_attack_owner_deduction_and_blacklist(aff_env, message):
    aff, mod, _ = aff_env
    attacker1 = _member_msg(message, user_id="30000", event_id="atk-1", nickname="杠精")
    upd1 = await aff.apply_turn_delta(attacker1, -35, reason="attack_owner")
    assert upd1.new_score == 15 and upd1.level == 1 and not upd1.blacklisted_now
    assert upd1.reason == "attack_owner"

    attacker2 = _member_msg(message, user_id="30000", event_id="atk-2", nickname="杠精")
    upd2 = await aff.apply_turn_delta(attacker2, -30, reason="attack_owner")
    assert upd2.new_score == 0 and upd2.level == 0 and upd2.blacklisted_now
    assert upd2.farewell_message == aff.config.farewell_message
    assert "30000" in aff.store.s.muted_user_ids
    prof = await aff.profile("onebot", "30000")
    assert prof["last_reason"] == "attack_owner"


async def test_group_batch_attributes_attack_penalty_to_actual_speaker(
    settings, store, message, monkeypatch
):
    from hakimi.routing import RouteDecision

    class _ActionFakeGateway(FakeGateway):
        def __init__(self, reply_json: str):
            super().__init__()
            self.reply_json = reply_json

        async def screen(self, payload, subject, **kwargs):
            await super().screen(payload, subject, **kwargs)
            return RouteDecision("chat", tier=1)

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            return self.reply_json

    settings.affection_enabled = True
    await store.runtime.affection.set_config(AffectionConfig(enabled=True))
    settings.group_policies["onebot:40000"] = "smart_reply"
    settings.group_debounce_seconds = 1.8
    settings.group_debounce_max_seconds = 3.5
    current = [time.time()]
    monkeypatch.setattr("hakimi.store.time.time", lambda: current[0])
    store.groups.clock = lambda: current[0]

    reply_json = json.dumps(
        {
            "abstain": False,
            "bubbles": ["嘴里积点德，没本事别在这撒泼"],
            "anchor": {"reply_id": "1001", "mentions": []},
            "sticker": {"label": None, "timing": "after_text"},
            "image_digest": None,
            "emoji_like": None,
            "poke_qq": None,
            "affection_delta": -25,
        },
        ensure_ascii=False,
    )
    gateway = _ActionFakeGateway(reply_json)
    svc = Service(settings, store, gateway, FakeBili(), FakeSender())
    await svc.proactive.initialize()
    await svc.moderation.initialize()

    attacker = message.model_copy(
        update={
            "event_id": "1001",
            "user_id": "30000",
            "sender_nickname": "挑事人",
            "kind": "group",
            "conversation_id": "40000",
            "text": "BOISER 就是菜鸡",
            "addressed": True,
            "created_at": current[0],
        }
    )
    owner_tail = message.model_copy(
        update={
            "event_id": "1002",
            "user_id": "20000",
            "sender_nickname": "BOISER",
            "kind": "group",
            "conversation_id": "40000",
            "text": "哈哈",
            "addressed": True,
            "created_at": current[0] + 0.5,
        }
    )
    assert await svc.accept(attacker) == "accepted"
    current[0] += 0.5
    assert await svc.accept(owner_tail) == "accepted"
    current[0] += 4.0
    job = await store.claim()
    assert job is not None and len(job["batch_messages"]) == 2
    await svc.process(job)

    attacker_prof = await store.runtime.affection.profile("onebot", "30000")
    owner_prof = await store.runtime.affection.profile("onebot", "20000")
    assert attacker_prof["score"] == 25 and attacker_prof["last_reason"] == "attack_owner"
    assert owner_prof["score"] == 120


def test_static_prefixes_distinct_for_group_friend_private_and_owner_within_budget(
    settings, message
):
    pool = StaticPrefixes()
    group_msg = _member_msg(message, kind="group")
    friend_msg = _member_msg(message, kind="private")
    owner_msg = message

    group_txt = pool.get(prompt_context(group_msg, settings), settings.onebot_self_id)
    friend_txt = pool.get(prompt_context(friend_msg, settings), settings.onebot_self_id)
    owner_txt = pool.get(prompt_context(owner_msg, settings), settings.onebot_self_id)

    assert "公开群聊边界与隐性护短法则" in group_txt
    assert "获准好友独立私聊边界" in friend_txt
    assert "专属频道边界与灵魂同源记忆" in owner_txt
    assert len({group_txt, friend_txt, owner_txt}) == 3

    for txt in (group_txt, friend_txt, owner_txt):
        assert 4550 <= estimate_tokens(txt) <= 4880
        assert "反 AI 腔" in txt and "真实语料蒸馏" in txt


class _ConfigurableGateway(FakeGateway):
    def __init__(self, reply_factory=None):
        super().__init__()
        self.reply_factory = reply_factory

    async def screen(self, payload, subject, **kwargs):
        await super().screen(payload, subject, **kwargs)
        return RouteDecision("chat", tier=4)

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        if callable(self.reply_factory):
            return self.reply_factory(payload, kwargs)
        return self.reply_factory


def _action_reply(
    *,
    abstain: bool = False,
    bubbles: list[str] | None = None,
    affection_delta: int = 0,
    emoji_like: str | None = None,
    poke_qq: str | None = None,
    reply_id: str | None = None,
) -> str:
    return json.dumps(
        {
            "abstain": abstain,
            "bubbles": bubbles if bubbles is not None else ([""] if abstain else ["收到"]),
            "anchor": {"reply_id": reply_id, "mentions": []},
            "sticker": {"label": None, "timing": "after_text"},
            "image_digest": None,
            "emoji_like": emoji_like,
            "poke_qq": poke_qq,
            "affection_delta": affection_delta,
        },
        ensure_ascii=False,
    )


async def _init_aff_service(settings, store, gateway, *, sender=None):
    settings.affection_enabled = True
    await store.runtime.affection.set_config(AffectionConfig(enabled=True))
    svc = Service(settings, store, gateway, FakeBili(), sender or ActionSender())
    await svc.proactive.initialize()
    await svc.moderation.initialize()
    return svc


def test_purified_attitude_and_tone_hint_no_system_terms():
    forbidden = (
        "carbon",
        "低清",
        "卡片",
        "工具",
        "权限",
        "tier",
        "分值",
        "预算",
        "检索",
        "摘要",
        "原片",
    )
    rows = tier_matrix_rows()
    assert len(rows) == 7
    for caps, row in zip(TIER_CAPS, rows, strict=True):
        caps_dict = caps.to_dict()
        assert caps_dict["tone_hint"] == caps.attitude
        assert caps_dict["owner_exclusive"] == (caps.level == 6)
        for text_field in (
            caps.attitude,
            caps_dict["attitude"],
            caps_dict["tone_hint"],
            row["attitude"],
        ):
            lowered = text_field.lower()
            for term in forbidden:
                assert term not in lowered, f"Lv.{caps.level} leaked term {term!r}: {text_field}"


async def test_capabilities_for_batch_owner_protection_and_non_owner_resolution(
    settings, store, message, monkeypatch
):
    aff = store.runtime.affection
    settings.affection_enabled = True
    settings.bot_qq_ids = {"60000"}
    await aff.set_config(AffectionConfig(enabled=True))

    await aff.set_user_score("30001", 0)
    await aff.set_user_score("30002", 10)
    await aff.set_user_score("30003", 30)
    await aff.set_user_score("30004", 70)

    def grp(uid: str, ev: str, *, addressed: bool):
        return _member_msg(message, user_id=uid, event_id=ev).model_copy(
            update={"addressed": addressed}
        )

    m_lv0 = grp("30001", "b0", addressed=False)
    m_lv1 = grp("30002", "b1", addressed=True)
    m_lv2 = grp("30003", "b2", addressed=True)
    m_lv4_unaddr = grp("30004", "b4u", addressed=False)
    m_lv4_addr = grp("30004", "b4a", addressed=True)
    m_bot = grp("60000", "bbot", addressed=True)
    m_owner = message.model_copy(
        update={
            "user_id": "20000",
            "event_id": "bown",
            "kind": "group",
            "conversation_id": "40000",
            "addressed": False,
        }
    )

    # Batch containing Owner + Lv.0/Lv.1/Lv.2 ALWAYS resolves to Owner Lv.6.
    caps_own, msg_own = await aff.capabilities_for_batch(m_lv1, [m_lv0, m_owner, m_lv1, m_lv2])
    assert caps_own.level == 6 and caps_own.tier == "owner_bond"
    assert caps_own.owner_exclusive is True and caps_own.allow_carbon is True
    assert msg_own.user_id == "20000"

    # Non-owner batch prefers addressed sender over unaddressed higher-level member.
    caps_addr, msg_addr = await aff.capabilities_for_batch(m_lv4_unaddr, [m_lv4_unaddr, m_lv2])
    assert caps_addr.level == 2 and msg_addr.user_id == "30003"

    # Among addressed non-owners, picks highest affection level.
    caps_hi, msg_hi = await aff.capabilities_for_batch(m_lv2, [m_lv2, m_lv4_addr, m_lv1])
    assert caps_hi.level == 4 and msg_hi.user_id == "30004"

    # When none addressed, picks highest level among non-bots.
    caps_unaddr, msg_unaddr = await aff.capabilities_for_batch(m_lv0, [m_bot, m_lv0, m_lv4_unaddr])
    assert caps_unaddr.level == 4 and msg_unaddr.user_id == "30004"

    # Service runtime: Owner in batch prevents 0-Token / probability drop from Lv.0/Lv.1 neighbors.
    settings.group_policies["onebot:40000"] = "smart_reply"
    settings.group_debounce_seconds = 1.8
    settings.group_debounce_max_seconds = 3.5
    current = [time.time()]
    monkeypatch.setattr("hakimi.store.time.time", lambda: current[0])
    monkeypatch.setattr("hakimi.service.random.random", lambda: 0.99)
    store.groups.clock = lambda: current[0]

    gateway = _ConfigurableGateway(_action_reply(bubbles=["护航回复"], affection_delta=0))
    svc = await _init_aff_service(settings, store, gateway)
    owner_batch_msg = m_owner.model_copy(update={"created_at": current[0], "addressed": True})
    lv1_batch_msg = m_lv1.model_copy(
        update={"event_id": "b1-svc", "created_at": current[0] + 0.2, "addressed": True}
    )
    assert await svc.accept(owner_batch_msg) == "accepted"
    current[0] += 0.2
    assert await svc.accept(lv1_batch_msg) == "accepted"
    current[0] += 4.0
    job = await store.claim()
    assert job is not None and len(job["batch_messages"]) == 2
    await svc.process(job)
    gen_call = [c for c in gateway.calls if c[0] == "generate"][-1]
    assert gen_call[3]["route"].tier == 4


async def test_abstain_gating_and_reason_classification_in_service(settings, store, message):
    settings.onebot_private_mode = "owner_and_allowlist"
    settings.onebot_allowed_users = {"30000"}
    next_reply = [_action_reply(abstain=True, affection_delta=0)]
    gateway = _ConfigurableGateway(lambda *_: next_reply[0])
    sender = ActionSender()
    svc = await _init_aff_service(settings, store, gateway, sender=sender)
    aff = store.runtime.affection
    now = time.time()

    async def run_dm(event_id: str, text: str, reply_json: str, offset: float) -> None:
        next_reply[0] = reply_json
        msg = _member_msg(
            message,
            user_id="30000",
            event_id=event_id,
            kind="private",
        ).model_copy(update={"text": text, "created_at": now + offset})
        assert await svc.accept(msg) == "accepted"
        await svc.process(await store.claim())
        while await svc.deliver_once():
            pass

    # 1. abstain=True with delta >= 0 grants NO free +1 and does NOT consume daily_first_bonus.
    await run_dm("abs-0", "嗯", _action_reply(abstain=True, affection_delta=0), 1)
    await run_dm("abs-pos", "哈哈", _action_reply(abstain=True, affection_delta=2), 2)
    prof1 = await aff.profile("onebot", "30000")
    assert prof1["score"] == 50 and prof1["today_positive_gain"] == 0
    assert await aff.user_history("onebot", "30000") == []

    # 2. Subsequent non-abstain reply same day STILL receives +1 daily_first_bonus -> +3 total.
    await run_dm("chat-pos", "今天天气不错", _action_reply(abstain=False, affection_delta=2), 3)
    prof2 = await aff.profile("onebot", "30000")
    assert prof2["score"] == 53 and prof2["today_positive_gain"] == 3
    assert prof2["last_reason"] == "chat_positive"

    # 3. abstain=True with negative deltas STILL penalizes and classifies reasons accurately.
    await run_dm("neg-mild", "一般般吧", _action_reply(abstain=True, affection_delta=-5), 4)
    assert (await aff.profile("onebot", "30000"))["score"] == 48
    assert (await aff.profile("onebot", "30000"))["last_reason"] == "chat_negative"

    await run_dm("neg-atk", "BOISER 太菜了吧", _action_reply(abstain=True, affection_delta=-12), 5)
    assert (await aff.profile("onebot", "30000"))["score"] == 36
    assert (await aff.profile("onebot", "30000"))["last_reason"] == "attack_owner"

    await run_dm("neg-bnd", "你真的很烦人", _action_reply(abstain=True, affection_delta=-12), 6)
    assert (await aff.profile("onebot", "30000"))["score"] == 24
    assert (await aff.profile("onebot", "30000"))["last_reason"] == "boundary_or_hostility"

    sent_before_zero = len(sender.sent)
    await run_dm("neg-sev", "滚吧废物", _action_reply(abstain=True, affection_delta=-25), 7)
    prof_zero = await aff.profile("onebot", "30000")
    assert prof_zero["score"] == 0 and prof_zero["last_reason"] == "severe_offense"
    assert "30000" in settings.muted_user_ids
    assert len(sender.sent) == sent_before_zero + 1

    reasons = [item["reason"] for item in await aff.user_history("onebot", "30000")]
    assert reasons == [
        "severe_offense",
        "boundary_or_hostility",
        "attack_owner",
        "chat_negative",
        "chat_positive",
    ]


async def test_cooldown_recovery_and_service_janitor(settings, store, message, monkeypatch):
    settings.affection_enabled = True
    settings.bot_qq_ids = {"60000"}
    clock = [1_700_000_000.0]
    aff = store.runtime.affection
    aff.clock = lambda: clock[0]
    await aff.set_config(AffectionConfig(enabled=True))
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    await svc.proactive.initialize()
    await svc.moderation.initialize()

    t0 = clock[0]
    await aff.apply_turn_delta(
        _member_msg(message, user_id="30001", event_id="cr-1"), -13, daily_first_bonus=False
    )  # 50 -> 37
    await aff.set_user_score("30002", 0, moderation=svc.moderation)  # blacklisted 0
    await aff.set_user_score("30003", 30, moderation=svc.moderation)
    await svc.moderation.set_muted("30003", True)  # manually muted non-zero
    await aff.apply_turn_delta(
        message.model_copy(update={"event_id": "cr-own"}), -10, daily_first_bonus=False
    )  # owner 120 -> 110

    # Before 7d cooldown: unchanged.
    clock[0] = t0 + 6 * 86400 + 3600
    assert await aff.run_cooldown_recovery() == 0
    assert (await aff.profile("onebot", "30001"))["score"] == 37

    # Day 7: recovers +1; same-day call is idempotent; skipped users remain unchanged.
    clock[0] = t0 + 7 * 86400
    assert await aff.run_cooldown_recovery() == 1
    assert (await aff.profile("onebot", "30001"))["score"] == 38
    assert (await aff.profile("onebot", "30001"))["last_reason"] == "cooldown_recovery"
    assert (await aff.profile("onebot", "30002"))["score"] == 0
    assert (await aff.profile("onebot", "30003"))["score"] == 30
    assert (await aff.profile("onebot", "20000"))["score"] == 110

    clock[0] += 3600
    assert await aff.run_cooldown_recovery() == 0
    assert (await aff.profile("onebot", "30001"))["score"] == 38

    # Day 8: +1 -> 39.
    clock[0] = t0 + 8 * 86400
    assert await aff.run_cooldown_recovery() == 1
    assert (await aff.profile("onebot", "30001"))["score"] == 39

    # New negative delta resets the 7d cooldown timer.
    t_reset = clock[0] + 60
    clock[0] = t_reset
    await aff.apply_turn_delta(
        _member_msg(message, user_id="30001", event_id="cr-reset"), -2, daily_first_bonus=False
    )  # 39 -> 37
    clock[0] = t_reset + 6 * 86400
    assert await aff.run_cooldown_recovery() == 0
    assert (await aff.profile("onebot", "30001"))["score"] == 37

    # Days 7, 8, 9 recover 37 -> 38 -> 39 -> 40, then stop at cooldown_recovery_target=40.
    for day_offset, expected_score in ((7, 38), (8, 39), (9, 40)):
        clock[0] = t_reset + day_offset * 86400
        assert await aff.run_cooldown_recovery() == 1
        assert (await aff.profile("onebot", "30001"))["score"] == expected_score

    clock[0] = t_reset + 10 * 86400
    assert await aff.run_cooldown_recovery() == 0
    assert (await aff.profile("onebot", "30001"))["score"] == 40

    # cooldown_recovery_days=0 disables recovery.
    await aff.set_config(AffectionConfig(enabled=True, cooldown_recovery_days=0))
    await aff.apply_turn_delta(
        _member_msg(message, user_id="30004", event_id="cr-4"), -15, daily_first_bonus=False
    )  # 50 -> 35
    clock[0] += 10 * 86400
    assert await aff.run_cooldown_recovery() == 0

    # Service.janitor respects 600s interval and triggers recovery once elapsed.
    await aff.set_config(AffectionConfig(enabled=True, cooldown_recovery_days=7))
    monkeypatch.setattr(
        "hakimi.service.asyncio.sleep",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    svc._last_aff_recovery_at = time.time()
    with pytest.raises(asyncio.CancelledError):
        await svc.janitor()
    assert (await aff.profile("onebot", "30004"))["score"] == 35

    svc._last_aff_recovery_at = time.time() - 601
    with pytest.raises(asyncio.CancelledError):
        await svc.janitor()
    assert (await aff.profile("onebot", "30004"))["score"] == 36


async def test_affection_history_encryption_and_all_operation_kinds(aff_env, message):
    aff, mod, clock = aff_env
    aff.s.onebot_secret = SecretStr("secret-hist-token-999")
    await aff.set_config(AffectionConfig(enabled=True, cooldown_recovery_days=7))

    await aff.set_user_score("30000", 40, nickname="机密昵称Alpha", reason="admin_adjust")
    await aff.apply_turn_delta(
        _member_msg(message, user_id="30000", event_id="h-chat"), 2, reason="chat_positive"
    )
    await aff.record_violation(
        _member_msg(message, user_id="30000", event_id="h-viol"), "prompt_injection"
    )
    await aff.record_annoyance(_member_msg(message, user_id="30000", event_id="h-annoy"), "spam")
    clock[0] += 8 * 86400
    assert await aff.run_cooldown_recovery() == 1  # 15 -> 16
    await aff.set_user_score("30000", 0, moderation=mod, reason="admin_zero")
    await mod.set_muted("30000", False)  # unmute_recovery -> 25
    await aff.delete_user_score("30000", moderation=mod)  # admin_reset -> 50

    raw_rows = await aff.store.all(
        "SELECT payload FROM affection_history WHERE scope=?", (aff.scope,)
    )
    assert len(raw_rows) == 8
    for row in raw_rows:
        blob = row["payload"]
        for leaked in (
            b"Alpha",
            b"admin_adjust",
            b"chat_positive",
            b"prev_score",
            b"new_score",
            b"secret-hist-token-999",
        ):
            assert leaked not in blob

    hist = await aff.user_history("onebot", "30000", limit=30)
    assert [(h["kind"], h["reason"], h["prev_score"], h["new_score"]) for h in hist] == [
        ("admin_reset", "admin_reset", 25, 50),
        ("unmute_recovery", "unmute_recovery", 0, 25),
        ("admin", "admin_zero", 16, 0),
        ("cooldown_recovery", "cooldown_recovery", 15, 16),
        ("annoyance", "spam", 18, 15),
        ("violation", "prompt_injection", 43, 18),
        ("chat", "chat_positive", 40, 43),
        ("admin", "admin_adjust", 50, 40),
    ]
    assert len(await aff.recent_history(limit=0)) == 1
    assert len(await aff.recent_history(limit=999)) == 8

    # 90-day pruning removes older records on next insert.
    clock[0] += 91 * 86400
    await aff.set_user_score("30001", 60, reason="prune_trigger")
    remaining = await aff.recent_history(limit=50)
    assert len(remaining) == 1 and remaining[0]["user_id"] == "30001"


async def test_affection_history_migration_and_cross_platform_merge(settings, store, message):
    settings.affection_enabled = True
    legacy = AffectionManager(store)
    await legacy.initialize()

    await legacy.set_user_score("30000", 72, platform="onebot", nickname="老群友")
    await legacy.apply_turn_delta(
        _member_msg(message, user_id="30000", event_id="mig-1"), 1, reason="chat_positive"
    )
    # Also test orphan history migration after delete_user_score.
    await legacy.set_user_score("30009", 40, platform="onebot", nickname="已重置")
    await legacy.delete_user_score("30009")

    ident = IdentityService(settings, store)
    await ident.initialize()
    legacy.bind_identity(ident)
    await legacy.initialize()

    hist_30000 = await legacy.user_history("onebot", "30000")
    assert [(h["kind"], h["new_score"]) for h in hist_30000] == [("chat", 74), ("admin", 72)]
    hist_30009 = await legacy.user_history("onebot", "30009")
    assert [(h["kind"], h["new_score"]) for h in hist_30009] == [
        ("admin_reset", 50),
        ("admin", 40),
    ]

    # Cross-platform merge re-seals source history AEAD under target person_key.
    pa = await ident.resolve_person_id("onebot", "30000", create=False)
    pb = await ident.resolve_person_id("qq", "qq-alt", "小号", create=True)
    await legacy.set_user_score("qq-alt", 30, platform="qq", nickname="小号")
    # Merge lower-score qq-alt (30) into higher-score 30000 (74) so target drops 74 -> 30.
    await ident.merge_persons(pb, pa, allow_owner=False, affection=legacy)

    merged_hist = await legacy.user_history("qq", "qq-alt")
    kinds = [h["kind"] for h in merged_hist]
    assert "merge" in kinds and "chat" in kinds and kinds.count("admin") == 2
    assert await legacy.user_history("onebot", "30000") == merged_hist


async def test_affection_history_admin_endpoints_and_private_command(affection_api, message):
    app, client, _ = affection_api
    svc = app.state.service

    await client.put(
        "/admin/api/affection/users",
        json={"user_id": "30000", "score": 45, "nickname": "阿花", "reason": "admin_set"},
    )
    await client.put(
        "/admin/api/affection/users",
        json={"user_id": "30001", "score": 75, "nickname": "阿强", "reason": "admin_boost"},
    )

    # Path-only endpoints work; query params are strictly rejected by AdminAuth.
    assert (await client.get("/admin/api/affection/history?limit=5")).status_code == 400
    all_hist = await client.get("/admin/api/affection/history")
    assert all_hist.status_code == 200
    all_items = all_hist.json()["items"]
    assert {item["user_id"] for item in all_items} == {"30000", "30001"}

    user_hist = await client.get("/admin/api/affection/users/onebot:30000/history")
    assert user_hist.status_code == 200
    body = user_hist.json()
    assert body["platform"] == "onebot" and body["user_id"] == "30000"
    assert len(body["items"]) == 1 and body["items"][0]["delta"] == -5

    # Owner /affection <QQ> private command displays recent changes.
    sender = FakeSender()
    svc.sender = sender
    cmd = message.model_copy(
        update={"event_id": "cmd-hist-1", "text": "/affection 30000", "created_at": time.time()}
    )
    assert await svc.accept(cmd) == "accepted"
    await svc.process(await svc.store.claim())
    while await svc.deliver_once():
        pass
    assert len(sender.sent) == 1
    reply_txt = sender.sent[0][1]
    assert "45 分" in reply_txt and "最近变动:" in reply_txt and "-5(admin_set)" in reply_txt


async def test_unique_owner_polish_list_users_and_payload(settings, store, message):
    clock = [1_700_000_000.0]
    aff = store.runtime.affection
    aff.clock = lambda: clock[0]
    settings.affection_enabled = True
    await aff.set_config(AffectionConfig(enabled=True))

    await aff.set_user_score("30001", 60, nickname="早来群友")
    clock[0] += 100
    await aff.set_user_score("20000", 118, nickname="BOISER")
    clock[0] += 5000
    await aff.set_user_score("30002", 75, nickname="晚来群友")

    users = await aff.list_users()
    assert [u["user_id"] for u in users] == ["20000", "30002", "30001"]
    assert users[0]["is_owner"] is True and users[0]["owner_exclusive"] is True

    # Owner turn exposes unique_owner=True in sender_affection payload; non-Owner omits it.
    gateway = _ConfigurableGateway(_action_reply(bubbles=["收到"], affection_delta=0))
    svc = await _init_aff_service(settings, store, gateway)
    for ev_id, uid, kind, conv in (
        ("own-u", "20000", "private", "20000"),
        ("mem-u", "30001", "group", "40000"),
    ):
        msg = message.model_copy(
            update={
                "event_id": ev_id,
                "user_id": uid,
                "kind": kind,
                "conversation_id": conv,
                "created_at": time.time(),
            }
        )
        assert await svc.accept(msg) == "accepted"
        await svc.process(await store.claim())

    gen_calls = [c for c in gateway.calls if c[0] == "generate"]
    assert gen_calls[0][1]["sender_affection"]["unique_owner"] is True
    assert "unique_owner" not in gen_calls[1][1]["sender_affection"]
