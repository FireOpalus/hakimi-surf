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
    AffectionConfig,
    AffectionManager,
    capabilities_for_score,
)
from hakimi.affection_admin import router as affection_router
from hakimi.app import create_app
from hakimi.bot_server import prompt_context
from hakimi.config import Settings
from hakimi.context_budget import estimate_tokens
from hakimi.moderation import Moderation
from hakimi.outbound_actions import ACTION_INSTRUCTION, ChatActions, parse_actions
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN


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
    assert cfg.daily_positive_cap == 6
    assert cfg.unmute_recovery_score == 25
    assert cfg.farewell_message

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
        {"unknown": 1},
    ):
        with pytest.raises(ValidationError):
            AffectionConfig.model_validate(bad)


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


async def test_non_owner_cap_daily_plus6_rollover_and_sandbox_lock(aff_env, message):
    aff, _, clock = aff_env
    user = _member_msg(message, user_id="30000", event_id="m-0")

    # Initial score 50, never exceeds 100.
    p0 = await aff.profile("onebot", "30000", "小明")
    assert p0["score"] == 50 and p0["tier"] == "normal"
    capped_admin = await aff.set_user_score("30000", 120)
    assert capped_admin["score"] == 100 and capped_admin["tier"] == "bestie"

    # Reset to 50 to test daily +6 cap and daily_first_bonus.
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

    # Second message same day does NOT get daily_first_bonus again.
    t2 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-2"), 2, daily_first_bonus=True
    )
    assert t2.delta == 2 and t2.new_score == 53 and t2.today_positive_gain == 5

    # Third message same day hits daily_positive_cap=6 -> only +1 allowed.
    t3 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-3"), 2, daily_first_bonus=True
    )
    assert t3.delta == 1 and t3.new_score == 54 and t3.today_positive_gain == 6

    # Fourth message same day -> positive gain capped at 0.
    t4 = await aff.apply_turn_delta(
        _member_msg(message, event_id="d1-4"), 2, daily_first_bonus=True
    )
    assert t4.delta == 0 and t4.new_score == 54 and t4.today_positive_gain == 6

    # Negative delta still applies same day without changing today_positive_gain.
    t5 = await aff.apply_turn_delta(_member_msg(message, event_id="d1-5"), -1)
    assert t5.delta == -1 and t5.new_score == 53 and t5.today_positive_gain == 6

    # Advance clock +24h: daily cap and daily_first_bonus reset.
    clock[0] += 86400.0
    next_day = await aff.apply_turn_delta(
        _member_msg(message, event_id="d2-1"), 2, daily_first_bonus=True
    )
    assert next_day.delta == 3 and next_day.new_score == 56 and next_day.today_positive_gain == 3
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
        (0, 0, "blacklisted", 0.0, False, 0, True, False, False, False, False, 1, Decimal("0")),
        (10, 1, "hostile", 0.25, False, 0, True, False, False, False, False, 1, Decimal("0.20")),
        (30, 2, "cold", 0.65, False, 1, True, False, False, False, False, 2, Decimal("0.50")),
        (50, 3, "normal", 1.0, True, 2, False, True, False, False, False, 3, Decimal("1.00")),
        (70, 4, "familiar", 1.0, True, 2, False, True, True, True, False, 3, Decimal("1.00")),
        (90, 5, "bestie", 1.0, True, 2, False, True, True, True, True, 4, Decimal("1.50")),
    ]
    for (
        score,
        lvl,
        tier,
        prob,
        poke_in,
        imgs,
        low_res,
        sticker,
        search,
        poke_out,
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
        assert caps.allow_sticker is sticker
        assert caps.allow_emoji_like is sticker
        assert caps.allow_bili_summary is sticker
        assert caps.allow_search is search
        assert caps.allow_file_read is search
        assert caps.allow_text_card is search
        assert caps.allow_dual_profile is search
        assert caps.allow_poke is poke_out
        assert caps.allow_video_watch is carbon
        assert caps.allow_carbon is carbon
        assert caps.max_tier == max_tier
        assert caps.budget_multiplier == mult
        assert aff.budget_multiplier_for_subject(subject) == mult

    # Threshold formulas across all 7 tiers.
    await aff.set_user_score("30000", 0)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(1.42)
    await aff.set_user_score("30000", 10)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.95)
    await aff.set_user_score("30000", 30)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.64)
    await aff.set_user_score("30000", 50)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.42)
    await aff.set_user_score("30000", 70)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.34)
    await aff.set_user_score("30000", 90)
    assert aff.adjust_reply_threshold(msg, 0.42) == pytest.approx(0.28)
    assert aff.adjust_reply_threshold(msg, 0.20) == pytest.approx(0.12)

    owner_caps = capabilities_for_score(100, is_owner=True)
    assert owner_caps.level == 6 and owner_caps.tier == "owner_bond"
    assert aff.adjust_reply_threshold(message, 0.28) <= 0.28


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
    assert restarted.budget_multiplier_for_subject(aff.store.subject(msg)) == Decimal("0.20")
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
    assert len(initial["tiers"]) == 7
    assert initial["users"] == []

    # Invalid config inputs rejected with 400
    for bad in ({}, {"initial_score": 0}, {"enabled": "true"}, {"unexpected": 1}):
        assert (await client.put("/admin/api/affection", json=bad)).status_code == 400

    # Partial PUT & persistence
    saved = (
        await client.put(
            "/admin/api/affection",
            json={"daily_positive_cap": 10, "unmute_recovery_score": 30},
        )
    ).json()
    assert saved["source"] == "admin"
    assert saved["config"]["daily_positive_cap"] == 10
    assert saved["config"]["unmute_recovery_score"] == 30
    assert saved["config"]["initial_score"] == 50

    # Reset config
    reset = (await client.delete("/admin/api/affection")).json()
    assert reset["source"] == "env" and reset["config"]["daily_positive_cap"] == 6

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
