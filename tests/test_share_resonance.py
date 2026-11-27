"""Tests for Pillar C: ShareResonanceEngine, decoupled quotas, untainted channel & engagement."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from pydantic import SecretStr

from hakimi.life_engine import GROUP_SCRUBBED_INTENT_NOTE, LifeEngine, LifeEngineConfig
from hakimi.models import Message
from hakimi.modules.actions import ActionCall, NoArgs
from hakimi.modules.base import sanitize_sensor_text
from hakimi.owner_memory import OwnerMemoryStore
from hakimi.service import Service
from hakimi.share_resonance import ResonanceConfig, ShareResonanceEngine
from hakimi.social_speak import speak_proactive
from hakimi.world_experience import MAX_ENGAGEMENT_SOCIAL_RECOVERY, SOCIAL_LOW
from tests.fakes import FakeBili, FakeGateway, FakeSender

ZONE = ZoneInfo("Asia/Shanghai")


def _ts(hour: int, minute: int = 0, day: int = 20) -> float:
    return datetime(2026, 11, day, hour, minute, tzinfo=ZONE).timestamp()


def _make_settings(settings: Any, *, groups: set[str] | None = None) -> Any:
    allowed = groups if groups is not None else {"40000", "50000"}
    return settings.model_copy(
        update={
            "onebot_enabled": True,
            "onebot_mode": "http",
            "onebot_api_url": "http://127.0.0.1:3000",
            "onebot_self_id": "10000",
            "owner_qq_id": "20000",
            "onebot_allowed_groups": allowed,
            "qq_enabled": False,
            "proactive_dm_enabled": True,
            "quiet_hours": "23:30-08:30",
            "gemini_api_key": SecretStr("AIzaFakeSecretKeyForRedactionTest99999"),
            "group_policies": {f"onebot:{gid}": "at_only" for gid in allowed},
        }
    )


async def _make_service(
    settings: Any,
    store: Any,
    *,
    now_ref: list[float],
    gateway: Any = None,
    groups: set[str] | None = None,
) -> tuple[Service, LifeEngine]:
    s = _make_settings(settings, groups=groups)
    gw = gateway or FakeGateway()
    sender = FakeSender()
    svc = Service(s, store, gw, FakeBili(), sender)
    svc.world_state.clock = lambda: now_ref[0]
    svc.proactive.clock = lambda: now_ref[0]
    await svc.world_state.initialize()
    await svc.proactive.initialize()
    qq_mod = svc.modules.get("qq_social")
    if qq_mod is not None:
        qq_mod.connected = lambda: True
    engine = LifeEngine(svc, clock=lambda: now_ref[0])
    svc.life_engine = engine
    await engine.initialize()
    return svc, engine


class _SpyEmbedder:
    def __init__(self, blob: bytes | None = b"E1\x02\x00\x00<\x00\x00") -> None:
        self.blob = blob
        self.calls: list[dict[str, Any]] = []

    async def embed_one(
        self,
        text: str,
        *,
        task_type: str = "RETRIEVAL_QUERY",
        budget_lane: str = "proactive",
        subject: str = "",
        purpose: str = "",
    ) -> bytes | None:
        self.calls.append(
            {
                "text": text,
                "task_type": task_type,
                "budget_lane": budget_lane,
                "subject": subject,
                "purpose": purpose,
            }
        )
        return self.blob


class _SpyOwnerMemory:
    def __init__(self, hits: list[dict[str, Any]]) -> None:
        self.hits = hits
        self.calls: list[tuple[str, bytes | None]] = []

    async def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((query_text, query_vector))
        return list(self.hits[:limit])


class _SpyCloudMemory:
    def __init__(self, hits: list[dict[str, Any]]) -> None:
        self.hits = hits
        self.calls: list[tuple[str, bytes | None, Any]] = []

    def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        message: Any = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((query_text, query_vector, message))
        return list(self.hits[:limit])


class _SpyDualMemory:
    def __init__(self, by_scope: dict[str, list[dict[str, Any]]]) -> None:
        self.by_scope = by_scope
        self.calls: list[tuple[str, str, str, bytes | None]] = []

    async def probe_resonance(
        self,
        message: Message,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        scope_key = (
            f"owner:{message.user_id}"
            if message.kind == "private"
            else f"{message.platform}:{message.conversation_id}"
        )
        self.calls.append((message.kind, scope_key, query_text, query_vector))
        return list(self.by_scope.get(scope_key, [])[:limit])


class _CapturingSpeakGateway(FakeGateway):
    def __init__(self, reply_text: str = "刚刷到这个超有意思！") -> None:
        super().__init__()
        self.reply_text = reply_text
        self.plan_json: str | None = None

    async def generate(self, payload, subject, **kwargs):
        self.calls.append(("generate", payload, kwargs))
        if kwargs.get("life_plan") and self.plan_json is not None:
            return self.plan_json
        return self.reply_text


# ---------------------------------------------------------------------------
# 1. Strict One-Way Privacy Isolation + Vault Encryption + Scope Binding
# ---------------------------------------------------------------------------


async def test_one_way_privacy_isolation_and_vault_sealing(settings, store):
    now = [_ts(15, 0)]
    svc, _engine = await _make_service(settings, store, now_ref=now, groups={"40000", "50000"})

    owner_secret_mem = "BOISER私密计划：下周去冰岛看极光并准备求婚戒指"
    group_a_mem = "群A上周大家一起讨论空洞骑士丝之歌Boss打法"
    group_b_mem = "群B独家秘密：周末组织线下桌游聚会"

    embedder = _SpyEmbedder()
    owner_mem = _SpyOwnerMemory(
        [
            {
                "id": "om-1",
                "source": "owner_chat",
                "topic": "丝之歌和冰岛行程",
                "text": owner_secret_mem,
                "score": 0.85,
                "entities": ["丝之歌", "冰岛"],
            }
        ]
    )
    cloud_mem = _SpyCloudMemory(
        [
            {
                "id": "cl-1",
                "source": "cloud:notes",
                "topic": "Owner桌面笔记",
                "text": "Owner云端私密备忘：丝之歌攻略心得",
                "score": 0.80,
                "entities": ["丝之歌"],
            }
        ]
    )
    dual_mem = _SpyDualMemory(
        {
            "onebot:40000": [
                {
                    "id": "ga-1",
                    "source": "chronicle",
                    "topic": "丝之歌讨论",
                    "text": group_a_mem,
                    "score": 0.82,
                    "entities": ["丝之歌"],
                }
            ],
            "onebot:50000": [
                {
                    "id": "gb-1",
                    "source": "chronicle",
                    "topic": "线下桌游",
                    "text": group_b_mem,
                    "score": 0.88,
                    "entities": ["桌游"],
                }
            ],
        }
    )

    resonance = ShareResonanceEngine(
        svc,
        embedder=embedder,
        owner_memory=owner_mem,
        dual_memory=dual_mem,
        owner_cloud_memory=cloud_mem,
        clock=lambda: now[0],
    )
    await resonance.initialize()

    # Include an API key secret in the experience to verify redaction before embedding/storage!
    raw_secret = "AIzaFakeSecretKeyForRedactionTest99999"
    results = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="空洞骑士丝之歌最新实机演示",
        summary=f"刚刷到丝之歌新预告片，附带误入秘钥 {raw_secret}",
        ref="BV1SILKSONG01",
        novelty=0.6,
        now=now[0],
    )

    # Embedder called ONCE with sanitized query text (secret scrubbed)
    assert len(embedder.calls) == 1
    assert raw_secret not in embedder.calls[0]["text"]
    assert embedder.calls[0]["task_type"] == "RETRIEVAL_QUERY"
    assert embedder.calls[0]["budget_lane"] == "proactive"
    assert embedder.calls[0]["purpose"] == "share_resonance"

    # Owner memory & cloud memory probed ONLY ONCE (exclusively for Owner target, NEVER for groups)
    assert len(owner_mem.calls) == 1
    assert len(cloud_mem.calls) == 1
    assert cloud_mem.calls[0][2].kind == "private"

    by_scope = {r["target_scope"]: r for r in results}
    assert {"owner:20000", "onebot:40000", "onebot:50000"} == set(by_scope)

    # Verify strict one-way isolation in sealed payloads & context retrieval
    owner_share = await resonance.load_share_for_scope(
        by_scope["owner:20000"]["share_id"], "owner:20000"
    )
    group_a_share = await resonance.load_share_for_scope(
        by_scope["onebot:40000"]["share_id"], "onebot:40000"
    )
    group_b_share = await resonance.load_share_for_scope(
        by_scope["onebot:50000"]["share_id"], "onebot:50000"
    )

    assert owner_share is not None and owner_share["is_owner"] is True
    assert group_a_share is not None and group_a_share["is_owner"] is False
    assert group_b_share is not None and group_b_share["is_owner"] is False

    # Group A sees ONLY Group A memory (never Owner, Cloud, or Group B memory)
    ga_serialized = json.dumps(group_a_share, ensure_ascii=False)
    assert group_a_mem in ga_serialized
    assert owner_secret_mem not in ga_serialized
    assert "冰岛" not in ga_serialized
    assert "Owner云端私密备忘" not in ga_serialized
    assert group_b_mem not in ga_serialized
    assert raw_secret not in ga_serialized

    # Group B sees ONLY Group B memory
    gb_serialized = json.dumps(group_b_share, ensure_ascii=False)
    assert "周末组织线下桌游聚会" in gb_serialized
    assert owner_secret_mem not in gb_serialized
    assert group_a_mem not in gb_serialized

    # Cross-scope load_share_for_scope MUST reject strictly BEFORE unsealing
    assert (
        await resonance.load_share_for_scope(by_scope["owner:20000"]["share_id"], "onebot:40000")
        is None
    )
    assert (
        await resonance.load_share_for_scope(by_scope["onebot:50000"]["share_id"], "onebot:40000")
        is None
    )
    assert (
        await resonance.load_share_for_scope(by_scope["onebot:40000"]["share_id"], "owner:20000")
        is None
    )

    # Vault sealing at rest: raw SQLite payload_blob must not contain plaintext memories or text
    rows = await store.all("SELECT id, target_scope, payload_blob FROM pending_shares")
    assert len(rows) == 3
    for row in rows:
        blob = bytes(row["payload_blob"])
        assert "丝之歌".encode() not in blob
        assert "冰岛".encode() not in blob
        assert "桌游".encode() not in blob
        assert raw_secret.encode() not in blob

    # pending_for_context strictly scoped
    ctx_a = await resonance.pending_for_context("onebot:40000", now=now[0])
    assert len(ctx_a) == 1
    assert "冰岛" not in json.dumps(ctx_a, ensure_ascii=False)
    assert group_b_mem not in json.dumps(ctx_a, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 2. Untainted Public Resonance Channel vs Planner Taint Wall Preservation
# ---------------------------------------------------------------------------


async def test_untainted_group_resonance_channel_vs_planner_taint_wall(settings, store):
    now = [_ts(15, 0)]
    gw = _CapturingSpeakGateway()
    svc, engine = await _make_service(settings, store, now_ref=now, gateway=gw, groups={"40000"})

    # Seed Owner memory & private ledger event so Planner run is tainted by Owner memory!
    owner_msg = Message(
        platform="onebot",
        bot_id="10000",
        event_id="owner-priv-1",
        user_id="20000",
        conversation_id="20000",
        kind="private",
        text="秘密约定明天两点打BOSS",
        addressed=True,
        created_at=now[0] - 120,
    )
    await svc.sessions.remember(owner_msg, "秘密约定明天两点打BOSS", "好嘟BOISER，我记住啦")
    await svc.event_ledger.post(
        "owner_chat",
        "commitment",
        "明天下午两点私密约好和BOISER去打隐藏BOSS",
        visibility="owner_private",
        now=now[0] - 60,
    )
    await svc.proactive.on_connection(True)

    gw.plan_json = json.dumps(
        {
            "current_module": "idle",
            "focus_level": "LOW",
            "mood": "calm",
            "mood_longnick": "",
            "rest_minutes": 0,
            "intents": [
                {
                    "action_name": "social.speak",
                    "arguments_json": "{}",
                    "target_scope": "onebot:40000",
                    "delay_minutes": 20,
                    "ttl_minutes": 60,
                    "intent_note": "刚才和BOISER私聊约了打隐藏BOSS，去群里聊聊",
                    "allow_desktop_care": False,
                },
                {
                    "action_name": "social.speak",
                    "arguments_json": "{}",
                    "target_scope": "owner",
                    "delay_minutes": 25,
                    "ttl_minutes": 60,
                    "intent_note": "提醒BOISER别忘了隐藏BOSS约定",
                    "allow_desktop_care": False,
                },
            ],
            "agenda_changes": [],
            "ledger_posts": [],
            "ledger_consumes": [],
        },
        ensure_ascii=False,
    )

    plan_res = await engine.trigger_plan_once(now=now[0])
    assert plan_res["intents_enqueued"] == 2

    # Planner group ticket MUST still be scrubbed to GROUP_SCRUBBED_INTENT_NOTE!
    planner_group = await store.one(
        "SELECT origin, intent_note_json FROM scheduled_intents "
        "WHERE origin='planner' AND target_scope='onebot:40000'"
    )
    assert planner_group is not None
    assert json.loads(planner_group["intent_note_json"])["note"] == sanitize_sensor_text(
        GROUP_SCRUBBED_INTENT_NOTE, svc.s
    )

    # Forged resonance group ticket WITHOUT untainted_public_share=True MUST be rejected!
    with pytest.raises(ValueError, match="action_origin_forbidden"):
        await engine.enqueue_resonance_intent(
            action_name="social.speak",
            target_module="social_presence",
            target_scope="onebot:40000",
            action_type="group_share",
            execute_after=now[0],
            expire_at=now[0] + 1800,
            intent_note="未标记纯净来源的群分享",
            gate_condition={"resonance_share_id": "forged"},
            dedupe_key="resonance:forged",
            now=now[0],
        )

    # Pure public resonance ticket WITH untainted_public_share=True keeps its public intent_note
    dual_mem = _SpyDualMemory(
        {
            "onebot:40000": [
                {
                    "id": "g40-1",
                    "source": "chronicle",
                    "topic": "红石电梯",
                    "text": "群里前几天讨论过怎么做无声红石电梯",
                    "score": 0.85,
                    "entities": ["红石电梯"],
                }
            ]
        }
    )
    resonance = ShareResonanceEngine(svc, dual_memory=dual_mem, clock=lambda: now[0])
    await resonance.initialize()
    res = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="超小型红石电梯教学",
        summary="一个三格宽的静音红石电梯设计",
        ref="BV1REDSTONE9",
        novelty=0.7,
        now=now[0],
    )
    group_hit = next(item for item in res if item["target_scope"] == "onebot:40000")
    assert group_hit["enqueued"] is True
    assert group_hit["intent_id"]

    res_ticket = await store.one(
        "SELECT origin, gate_condition_json, intent_note_json FROM scheduled_intents WHERE id=?",
        (group_hit["intent_id"],),
    )
    assert res_ticket["origin"] == "resonance"
    gate = json.loads(res_ticket["gate_condition_json"])
    assert gate["untainted_public_share"] is True
    assert gate["resonance_share_id"] == group_hit["share_id"]
    note = json.loads(res_ticket["intent_note_json"])["note"]
    assert note != GROUP_SCRUBBED_INTENT_NOTE
    assert "红石电梯" in note


# ---------------------------------------------------------------------------
# 3. Immediate Impulse vs Quiet-Hours Warm Pending Pool ("念念不忘")
# ---------------------------------------------------------------------------


async def test_immediate_impulse_vs_quiet_hours_pending_pool(settings, store):
    # Quiet hours: 23:30-08:30 -> test at 01:00 (quiet) then 10:00 (awake)
    now = [_ts(1, 0)]
    svc, engine = await _make_service(settings, store, now_ref=now, groups={"40000"})

    owner_mem = _SpyOwnerMemory(
        [
            {
                "id": "om-cat",
                "source": "owner_chat",
                "topic": "机械键盘轴体",
                "text": "BOISER前阵子在挑静音红轴机械键盘",
                "score": 0.62,
                "entities": ["机械键盘"],
            }
        ]
    )
    # Same 0.50 score + 0.50 novelty => impulse = 0.72*0.50 + 0.28*0.50 = 0.50:
    # exceeds Owner threshold (0.42) but stays below Group threshold (0.54)!
    dual_mem = _SpyDualMemory(
        {
            "onebot:40000": [
                {
                    "id": "g-low",
                    "source": "chronicle",
                    "topic": "外设",
                    "text": "群里顺嘴提过一次键盘",
                    "score": 0.48,
                    "entities": ["键盘"],
                }
            ]
        }
    )

    resonance = ShareResonanceEngine(
        svc,
        embedder=None,  # lexical / fallback path with embedder=None!
        owner_memory=owner_mem,
        dual_memory=dual_mem,
        clock=lambda: now[0],
    )
    await resonance.initialize()

    quiet_res = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="超静音矮轴键盘拆解测评",
        summary="实测声音比普通静音红轴还小一半",
        ref="BV1KEYB00001",
        novelty=0.5,
        now=now[0],
    )
    # Owner triggered, group did not (Owner threshold < Group threshold)
    assert len(quiet_res) == 1
    assert quiet_res[0]["target_scope"] == "owner:20000"
    assert quiet_res[0]["enqueued"] is False
    assert quiet_res[0]["queued_reason"] in {"quiet_hours", "sleeping"}

    # Zero immediate tickets enqueued during quiet hours
    assert (await store.one("SELECT COUNT(*) FROM scheduled_intents"))[0] == 0

    # Kept warm in pending_shares ("念念不忘") for context recall!
    warm = await resonance.pending_for_context("owner:20000", now=now[0] + 60)
    assert len(warm) == 1
    assert warm[0]["experience"]["ref"] == "BV1KEYB00001"

    # Next morning at 10:00 (active window): pop_due_shares promotes it immediately!
    now[0] = _ts(10, 0)
    promoted = await resonance.pop_due_shares("owner:20000", now=now[0])
    assert len(promoted) == 1
    assert promoted[0]["enqueued"] is True
    assert promoted[0]["id"] == quiet_res[0]["share_id"]

    row = await store.one(
        "SELECT origin, target_scope, status FROM scheduled_intents WHERE id=?",
        (promoted[0]["intent_id"],),
    )
    assert row["origin"] == "resonance"
    assert row["target_scope"] == "owner:20000"
    assert row["status"] == "pending"

    # Second pop_due_shares does NOT duplicate while ticket is already pending
    assert await resonance.pop_due_shares("owner:20000", now=now[0] + 10) == []


# ---------------------------------------------------------------------------
# 4. Decoupled Owner / Group Proactive Quotas & Legacy Backward Compatibility
# ---------------------------------------------------------------------------


async def test_decoupled_owner_and_group_proactive_quotas(settings, store):
    now = [_ts(15, 0)]
    svc, engine = await _make_service(settings, store, now_ref=now, groups={"40000"})

    # Backward compatibility: legacy config without new keys validates cleanly
    legacy_cfg = LifeEngineConfig.model_validate({"max_proactive_per_day": 4})
    assert legacy_cfg.max_owner_proactive_per_day is None
    assert legacy_cfg.max_group_proactive_per_day is None
    assert legacy_cfg.resonance_enabled is True

    # Decoupled mode: Owner=3/day, Group=1/day, shared fallback=6/day
    await engine.set_config(
        {
            "max_proactive_per_day": 6,
            "max_owner_proactive_per_day": 3,
            "max_group_proactive_per_day": 1,
        }
    )
    cfg = engine.config
    gate = engine.gatekeeper

    # Record 1 completed group share -> Group hits limit (1/1), Owner still allowed (0/3)!
    g_id = await engine.enqueue_resonance_intent(
        action_name="social.speak",
        target_module="social_presence",
        target_scope="onebot:40000",
        action_type="group_share",
        execute_after=now[0] - 10,
        expire_at=now[0] + 600,
        intent_note="群里分享一下新发现",
        gate_condition={"untainted_public_share": True, "resonance_share_id": "s-g1"},
        dedupe_key="res:g1",
        now=now[0] - 10,
    )
    await svc.world_state.mark_intent(g_id, "done", now=now[0] - 5)

    assert await gate.daily_limit_allows("social.speak", "onebot:40000", now[0], cfg) is False
    assert await gate.daily_limit_allows("social.speak", "owner:20000", now[0], cfg) is True

    # Record 3 completed Owner DMs -> Owner hits limit (3/3), independently of Group!
    for idx in range(3):
        o_id = await engine.enqueue_resonance_intent(
            action_name="social.speak",
            target_module="social_presence",
            target_scope="owner:20000",
            action_type="social_presence_milestone_share",
            execute_after=now[0] - 10,
            expire_at=now[0] + 600,
            intent_note=f"给BOISER分享第{idx + 1}条发现",
            gate_condition={"resonance_share_id": f"s-o{idx}"},
            dedupe_key=f"res:o{idx}",
            now=now[0] - 10,
        )
        await svc.world_state.mark_intent(o_id, "done", now=now[0] - 5)

    assert await gate.daily_limit_allows("social.speak", "owner:20000", now[0], cfg) is False

    # Partial decoupling: Owner dedicated=5, Group falls back to max_proactive_per_day=2
    # Owner's 3 DMs MUST NOT starve Group's fallback budget of 2 (Group has only used 1)!
    await engine.set_config(
        {
            "max_proactive_per_day": 2,
            "max_owner_proactive_per_day": 5,
            "max_group_proactive_per_day": None,
        }
    )
    assert (
        await gate.daily_limit_allows("social.speak", "owner:20000", now[0], engine.config) is True
    )
    assert (
        await gate.daily_limit_allows("social.speak", "onebot:40000", now[0], engine.config) is True
    )

    # Legacy mode (both None): all 4 messages count together against max_proactive_per_day=4 -> full
    await engine.set_config(
        {
            "max_proactive_per_day": 4,
            "max_owner_proactive_per_day": None,
            "max_group_proactive_per_day": None,
        }
    )
    assert (
        await gate.daily_limit_allows("social.speak", "owner:20000", now[0], engine.config) is False
    )
    assert (
        await gate.daily_limit_allows("social.speak", "onebot:40000", now[0], engine.config)
        is False
    )


# ---------------------------------------------------------------------------
# 5. Engagement Positive / Negative Feedback Loop ("捧场正循环") & Battery Recovery
# ---------------------------------------------------------------------------


async def test_engagement_positive_negative_feedback_loop_and_battery(settings, store):
    now = [_ts(15, 0)]
    svc, _engine = await _make_service(settings, store, now_ref=now, groups={"40000"})

    # Lower social battery to 50 so we can observe capped recovery + idempotency
    async with store.transaction():
        await store.db.execute("UPDATE world_social_battery SET charge=50.0 WHERE id=1")

    exp = svc.world_state.experience
    # Direct WorldExperience idempotent & capped helper test
    rec1 = await exp.boost_from_engagement("r1", now=now[0], amount=10.0)
    assert rec1 == pytest.approx(MAX_ENGAGEMENT_SOCIAL_RECOVERY)
    rec1_dup = await exp.boost_from_engagement("r1", now=now[0] + 1, amount=10.0)
    assert rec1_dup == 0.0
    assert await exp.battery(None, now[0]) == pytest.approx(50.0 + MAX_ENGAGEMENT_SOCIAL_RECOVERY)

    resonance = ShareResonanceEngine(svc, clock=lambda: now[0])
    await resonance.initialize()
    scope = "onebot:40000"

    base_cd = resonance.effective_cooldown_seconds(scope, is_owner=False)

    # Positive engagement loop: streak rises, boost caps at +0.15, cooldown shrinks, battery heals
    for i in range(4):
        st = await resonance.record_engagement(
            scope, engaged=True, now=now[0] + i * 10, receipt_id=f"eng-{i}"
        )
    assert st["streak"] == 4
    assert st["ignore_streak"] == 0
    assert st["boost"] == pytest.approx(0.15)  # capped at +0.15
    short_cd = resonance.effective_cooldown_seconds(
        scope, is_owner=False, streak=st["streak"], ignore_streak=st["ignore_streak"]
    )
    assert short_cd < base_cd
    assert await exp.battery(None, now[0] + 30) > 56.0

    # Negative engagement (lazy ignore after 20 min without reply)
    share_t = now[0] + 100
    await resonance.record_shared(scope, now=share_t)
    # 10 minutes later: within 20-min grace window -> no ignore penalty yet
    assert await resonance.check_lazy_ignore(scope, now=share_t + 600) is False
    # 25 minutes later: lazy ignore triggers ONCE
    assert await resonance.check_lazy_ignore(scope, now=share_t + 1500) is True
    # Repeated check at 30 minutes does NOT double-penalize the same share
    assert await resonance.check_lazy_ignore(scope, now=share_t + 1800) is False

    after_ignore = await resonance.engagement_state(scope)
    assert after_ignore["streak"] == 0
    assert after_ignore["ignore_streak"] == 1
    assert after_ignore["boost"] < 0.15

    # Repeated ignores clamp boost at -0.20 and extend cooldown
    for i in range(6):
        neg = await resonance.record_engagement(scope, engaged=False, now=share_t + 2000 + i * 60)
    assert neg["boost"] == pytest.approx(-0.20)
    long_cd = resonance.effective_cooldown_seconds(
        scope, is_owner=False, streak=neg["streak"], ignore_streak=neg["ignore_streak"]
    )
    assert long_cd > base_cd

    # Inbound reply within 15 min of a share auto-credits engagement via check_inbound_engagement
    share_t2 = share_t + 5000
    await resonance.record_shared(scope, now=share_t2)
    credited = await resonance.check_inbound_engagement(scope, now=share_t2 + 300)
    assert credited is True
    # Duplicate inbound reply for the same share does NOT double-credit
    assert await resonance.check_inbound_engagement(scope, now=share_t2 + 360) is False
    final_st = await resonance.engagement_state(scope)
    assert final_st["streak"] == 1
    assert final_st["ignore_streak"] == 0


# ---------------------------------------------------------------------------
# 6. social_speak Resonance Memory Injection (220-token limit) & Delivery Marking
# ---------------------------------------------------------------------------


async def test_social_speak_resonance_injection_and_mark_delivered(settings, store):
    now = [_ts(15, 0)]
    gw = _CapturingSpeakGateway(reply_text="刚刷到超赞的红石电梯教程，想起你们前几天聊过这个！")
    svc, engine = await _make_service(
        settings, store, now_ref=now, gateway=gw, groups={"40000", "50000"}
    )

    long_summary = (
        "这是一段非常详细的公开见闻摘要，" + "包含红石电路延迟中继器与静音活塞设计细节，" * 12
    )
    dual_mem = _SpyDualMemory(
        {
            "onebot:40000": [
                {
                    "id": "m-a1",
                    "source": "chronicle",
                    "topic": "红石电梯",
                    "text": "群友上周问过怎么把红石电梯声音压到最小",
                    "score": 0.90,
                    "entities": ["红石电梯", "静音活塞"],
                }
            ]
        }
    )
    owner_mem = _SpyOwnerMemory(
        [
            {
                "id": "m-owner",
                "source": "owner_chat",
                "topic": "私密日记",
                "text": "BOISER私密备忘绝不能进群",
                "score": 0.95,
                "entities": ["私密"],
            }
        ]
    )
    resonance = ShareResonanceEngine(
        svc, owner_memory=owner_mem, dual_memory=dual_mem, clock=lambda: now[0]
    )
    await resonance.initialize()
    svc.share_resonance = resonance

    hits = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="红石静音电梯原理讲解",
        summary=long_summary,
        ref="BV1ELEVATOR1",
        novelty=0.6,
        now=now[0],
    )
    group_hit = next(h for h in hits if h["target_scope"] == "onebot:40000")
    owner_hit = next(h for h in hits if h["target_scope"] == "owner:20000")

    group_msg = Message(
        platform="onebot",
        bot_id="10000",
        event_id="jit:res-test",
        user_id="0",
        conversation_id="40000",
        kind="group",
        text="",
        addressed=True,
        created_at=now[0],
    )

    # Attempting to pass Owner's share_id into a Group speak call MUST be blocked!
    mismatched_ticket = {
        "id": "t-mismatch",
        "origin": "resonance",
        "target_module": "social_presence",
        "target_scope": "onebot:40000",
        "action_type": "group_share",
        "mood": "pleased",
        "gate_condition": {
            "untainted_public_share": True,
            "resonance_share_id": owner_hit["share_id"],
        },
        "intent_note": {"note": "尝试跨作用域"},
    }
    mismatch_out = await speak_proactive(
        svc, group_msg, ActionCall(mismatched_ticket, NoArgs(), now[0])
    )
    assert mismatch_out.status == "gated_skip"
    assert mismatch_out.reason == "resonance_share_unavailable"
    assert not gw.calls

    # Legitimate Group A resonance share injects local resonance_memories + 220-token material
    valid_ticket = {
        "id": group_hit["intent_id"],
        "origin": "resonance",
        "target_module": "social_presence",
        "target_scope": "onebot:40000",
        "action_type": "group_share",
        "mood": "pleased",
        "gate_condition": {
            "untainted_public_share": True,
            "resonance_share_id": group_hit["share_id"],
        },
        "intent_note": {"note": "红石静音电梯分享"},
    }
    # Mark intent executing so deliver_proactive_jit expected_bubbles update succeeds
    await svc.world_state.claim(group_hit["intent_id"], now[0])
    out = await speak_proactive(svc, group_msg, ActionCall(valid_ticket, NoArgs(), now[0]))
    assert out.status == "done"
    assert len(gw.calls) == 1

    gen_payload = gw.calls[0][1]
    jit = gen_payload["jit_proactive_intent"]
    assert "resonance_memories" in jit and len(jit["resonance_memories"]) == 1
    assert "红石电梯" in jit["resonance_memories"][0]["topic"]
    assert "BOISER私密备忘" not in json.dumps(gen_payload, ensure_ascii=False)
    # Material summary relaxed beyond 90 tokens (len > 100 chars of Chinese text!)
    assert len(jit["module_material"]) > 110
    assert jit["resonance_experience"]["ref"] == "BV1ELEVATOR1"

    # Confirmed delivery marked pending_share as 'delivered' AND recorded last_shared_at!
    assert await resonance.load_share_for_scope(group_hit["share_id"], "onebot:40000") is None
    delivered_row = await resonance.load_share_for_scope(
        group_hit["share_id"], "onebot:40000", include_delivered=True
    )
    assert delivered_row is not None and delivered_row["status"] == "delivered"
    eng = await resonance.engagement_state("onebot:40000")
    assert eng["last_shared_at"] >= now[0]

    # Extended cooldown (45m * 1.35 = 60.75m after lazy ignore) blocks at +3600s;
    # past cooldown (+4000s), low battery (charge=10 -> ~16.7 < SOCIAL_LOW) blocks immediate enqueue
    async with store.transaction():
        await store.db.execute(
            "UPDATE world_social_battery SET charge=? WHERE id=1", (SOCIAL_LOW - 20.0,)
        )
    cd_blocked = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="红石电梯2.0",
        summary="更省材料的静音红石电梯",
        ref="BV1ELEVATOR2",
        novelty=0.6,
        now=now[0] + 3600,
    )
    g_cd = next(h for h in cd_blocked if h["target_scope"] == "onebot:40000")
    assert g_cd["enqueued"] is False
    assert g_cd["queued_reason"] == "cooldown"

    low_bat = await resonance.on_public_experience(
        source="bilibili_surf",
        event_type="bili_video_watched",
        title="红石电梯3.0",
        summary="超级省材料的静音红石电梯",
        ref="BV1ELEVATOR3",
        novelty=0.6,
        now=now[0] + 4000,
    )
    g_low = next(h for h in low_bat if h["target_scope"] == "onebot:40000")
    assert g_low["enqueued"] is False
    assert g_low["queued_reason"] == "social_battery_low"


# ---------------------------------------------------------------------------
# 7. End-to-End Wiring Smoke Test with REAL OwnerMemoryStore (Pillar B + C)
# ---------------------------------------------------------------------------


async def test_integration_with_real_owner_memory_and_dual_memory(settings, store):
    now = [_ts(16, 0)]
    svc, _engine = await _make_service(settings, store, now_ref=now, groups={"40000"})

    owner_store = OwnerMemoryStore(svc.s, clock=lambda: now[0])
    await owner_store.initialize()
    try:
        await owner_store.record(
            source="owner_chat",
            topic="胶片摄影",
            text="BOISER最近迷上了中画幅胶片摄影和哈苏相机",
            entities=("胶片摄影", "哈苏"),
            now=now[0] - 600,
        )

        # Group 40000 has NO memories -> only Owner should resonate!
        resonance = ShareResonanceEngine(
            svc,
            embedder=None,
            owner_memory=owner_store,
            dual_memory=svc.sessions.dual,
            clock=lambda: now[0],
            config=ResonanceConfig(owner_threshold=0.35),
        )
        await resonance.initialize()

        hits = await resonance.on_public_experience(
            source="bilibili_surf",
            event_type="bili_video_watched",
            title="哈苏中画幅胶片摄影实拍体验",
            summary="用哈苏中画幅胶片机扫街拍日落",
            ref="BV1HASSELBLAD",
            novelty=0.55,
            now=now[0],
        )
        assert len(hits) == 1
        assert hits[0]["target_scope"] == "owner:20000"
        assert hits[0]["is_owner"] is True
        assert hits[0]["enqueued"] is True
    finally:
        await owner_store.close()
