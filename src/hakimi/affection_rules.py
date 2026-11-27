"""Affection scoring rules, configuration models, and 7-tier capability tables."""

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from .config import Settings
from .security import Rejected, clean_text, configured_secrets

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_affection (
  user_key TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  subject TEXT NOT NULL,
  payload BLOB NOT NULL,
  updated REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS user_affection_scope ON user_affection(scope, updated);
CREATE TABLE IF NOT EXISTS affection_events (
  event_key TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  user_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  delta INTEGER NOT NULL,
  created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS affection_events_created ON affection_events(created);
CREATE INDEX IF NOT EXISTS affection_events_user ON affection_events(scope, user_key, created);
CREATE TABLE IF NOT EXISTS affection_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scope TEXT NOT NULL,
  user_key TEXT NOT NULL,
  delta INTEGER NOT NULL,
  payload BLOB NOT NULL,
  created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS affection_history_user
  ON affection_history(scope, user_key, created DESC);
CREATE INDEX IF NOT EXISTS affection_history_created ON affection_history(created);
"""

PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
ONEBOT_USER_RE = re.compile(r"^[1-9][0-9]{0,19}$")
QQ_USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
GENERIC_USER_RE = re.compile(r"^[A-Za-z0-9_.:@-]{1,128}$")
DEFAULT_FAREWELL = "话不投机，以后不想再理你了，再见。"
VIOLATION_DELTAS = {
    "prompt_injection": -25,
    "content_blocked": -30,
}
ANNOYANCE_DELTA = -3
ANNOYANCE_COOLDOWN_SECONDS = 60.0


def _sanitize_nickname(value: Any, settings: Settings) -> str:
    text = clean_text(str(value or ""))[:80]
    if any(
        secret and len(secret) >= 8 and secret in text for secret in configured_secrets(settings)
    ):
        return "群友"
    cleaned = re.sub(r"[^\w\u4e00-\u9fff .-]", "", text)[:30].strip()
    return cleaned or "群友"


def _sanitize_reason(value: Any, settings: Settings, default: str = "chat") -> str:
    text = clean_text(str(value or "")).replace("\n", " ").replace("\r", " ")[:64].strip()
    if not text:
        return default
    if any(
        secret and len(secret) >= 8 and secret in text for secret in configured_secrets(settings)
    ):
        return default
    return text


def validate_platform_user(platform: str, user_id: str) -> str:
    """Validate platform and user_id using strict per-platform patterns."""
    if not isinstance(platform, str) or not PLATFORM_ID_RE.fullmatch(platform):
        raise Rejected("invalid_platform")
    if isinstance(user_id, bool) or not isinstance(user_id, str):
        raise Rejected("invalid_user_id")
    uid = user_id.strip()
    if platform == "onebot":
        pattern = ONEBOT_USER_RE
    elif platform == "qq":
        pattern = QQ_USER_RE
    else:
        pattern = GENERIC_USER_RE
    if not pattern.fullmatch(uid):
        raise Rejected("invalid_user_id")
    return uid


_validate_platform_user = validate_platform_user


class AffectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: StrictBool = True
    initial_score: int = Field(default=50, ge=1, le=100)
    daily_positive_cap: int = Field(default=8, ge=0, le=50)
    unmute_recovery_score: int = Field(default=25, ge=1, le=60)
    farewell_message: str = Field(default=DEFAULT_FAREWELL, min_length=1, max_length=120)
    cooldown_recovery_days: int = Field(default=7, ge=0, le=90)
    cooldown_recovery_target: int = Field(default=40, ge=20, le=60)
    penalize_rate_limit: StrictBool = False
    penalize_poke_ignored: StrictBool = False
    decouple_safety_violations: StrictBool = True
    violation_injection_delta: int = Field(default=-8, ge=-50, le=0)
    violation_content_delta: int = Field(default=-10, ge=-50, le=0)
    violation_affection_floor: int = Field(default=15, ge=0, le=50)

    @field_validator("farewell_message")
    @classmethod
    def _clean_farewell(cls, value: str) -> str:
        cleaned = clean_text(value).replace("\n", " ").replace("\r", " ").strip()
        if not cleaned or len(cleaned) > 120:
            raise ValueError("farewell_message must be 1..120 non-empty characters")
        return cleaned

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(enabled=bool(getattr(settings, "affection_enabled", True)))


@dataclass(frozen=True)
class AffectionCapabilities:
    level: int
    tier: str
    tier_label: str
    attitude: str
    direct_reply_prob: float
    allow_poke_inbound: bool
    reply_threshold_delta: float
    max_images: int
    low_res_images_only: bool
    allow_sticker: bool
    allow_emoji_like: bool
    allow_poke: bool
    allow_bili_summary: bool
    allow_search: bool
    allow_file_read: bool
    allow_text_card: bool
    allow_dual_profile: bool
    allow_video_watch: bool
    allow_carbon: bool
    max_tier: int
    budget_multiplier: Decimal
    owner_exclusive: bool = False

    @property
    def lv(self) -> str:
        return f"Lv.{self.level}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "lv": self.lv,
            "tier": self.tier,
            "tier_label": self.tier_label,
            "attitude": self.attitude,
            "tone_hint": self.attitude,
            "direct_reply_prob": self.direct_reply_prob,
            "allow_poke_inbound": self.allow_poke_inbound,
            "reply_threshold_delta": self.reply_threshold_delta,
            "max_images": self.max_images,
            "low_res_images_only": self.low_res_images_only,
            "allow_sticker": self.allow_sticker,
            "allow_emoji_like": self.allow_emoji_like,
            "allow_poke": self.allow_poke,
            "allow_bili_summary": self.allow_bili_summary,
            "allow_search": self.allow_search,
            "allow_file_read": self.allow_file_read,
            "allow_text_card": self.allow_text_card,
            "allow_dual_profile": self.allow_dual_profile,
            "allow_video_watch": self.allow_video_watch,
            "allow_carbon": self.allow_carbon,
            "max_tier": self.max_tier,
            "budget_multiplier": str(self.budget_multiplier),
            "owner_exclusive": self.owner_exclusive,
        }


TIER_CAPS: tuple[AffectionCapabilities, ...] = (
    AffectionCapabilities(
        level=0,
        tier="blacklisted",
        tier_label="拉黑绝交",
        attitude="绝交拉黑：彻底无视该用户，不回应不互动。",
        direct_reply_prob=0.0,
        allow_poke_inbound=False,
        reply_threshold_delta=1.0,
        max_images=0,
        low_res_images_only=True,
        allow_sticker=False,
        allow_emoji_like=False,
        allow_poke=False,
        allow_bili_summary=False,
        allow_search=False,
        allow_file_read=False,
        allow_text_card=False,
        allow_dual_profile=False,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=1,
        budget_multiplier=Decimal("0"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=1,
        tier="hostile",
        tier_label="厌烦防备",
        attitude="明显不太想搭理，语气冷淡简短、带点防备距离感；若对方态度诚恳可淡淡接一句，绝不提好感或规则。",
        direct_reply_prob=0.50,
        allow_poke_inbound=False,
        reply_threshold_delta=0.53,
        max_images=0,
        low_res_images_only=True,
        allow_sticker=False,
        allow_emoji_like=False,
        allow_poke=False,
        allow_bili_summary=False,
        allow_search=False,
        allow_file_read=False,
        allow_text_card=False,
        allow_dual_profile=False,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=1,
        budget_multiplier=Decimal("0.30"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=2,
        tier="cold",
        tier_label="冷淡疏离",
        attitude="保持礼貌距离，回答偏简短克制，不主动热络也不玩亲密梗，绝不提好感或规则。",
        direct_reply_prob=0.85,
        allow_poke_inbound=False,
        reply_threshold_delta=0.16,
        max_images=1,
        low_res_images_only=True,
        allow_sticker=False,
        allow_emoji_like=False,
        allow_poke=False,
        allow_bili_summary=False,
        allow_search=False,
        allow_file_read=False,
        allow_text_card=False,
        allow_dual_profile=False,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=2,
        budget_multiplier=Decimal("0.60"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=3,
        tier="normal",
        tier_label="普通群友",
        attitude="松弛平视、自然接梗，像熟脸网友一样正常聊天吐槽，保持朋友分寸，绝不提好感或规则。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=0.0,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=False,
        allow_bili_summary=True,
        allow_search=True,
        allow_file_read=False,
        allow_text_card=True,
        allow_dual_profile=True,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=3,
        budget_multiplier=Decimal("1.00"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=4,
        tier="familiar",
        tier_label="投缘熟人",
        attitude="熟络合拍、更爱搭话接梗，可以随意打趣和互戳玩闹，但严守非亲密边界，绝不提好感或规则。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=-0.08,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=True,
        allow_bili_summary=True,
        allow_search=True,
        allow_file_read=True,
        allow_text_card=True,
        allow_dual_profile=True,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=3,
        budget_multiplier=Decimal("1.15"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=5,
        tier="bestie",
        tier_label="死党铁磁",
        attitude="高度默契的铁哥们与损友，随意吐槽护短、聊得最开，但始终是朋友而非特殊偏爱，严禁越界亲密，绝不提好感或规则。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=-0.14,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=True,
        allow_bili_summary=True,
        allow_search=True,
        allow_file_read=True,
        allow_text_card=True,
        allow_dual_profile=True,
        allow_video_watch=True,
        allow_carbon=False,
        max_tier=3,
        budget_multiplier=Decimal("1.35"),
        owner_exclusive=False,
    ),
    AffectionCapabilities(
        level=6,
        tier="owner_bond",
        tier_label="专属羁绊 (Owner)",
        attitude=(
            "全天下唯一的 BOISER（唯一专属羁绊）：无条件偏爱、护短且自然亲昵，"
            "私聊可撒娇黏人做专属暖光猫猫，群聊隐性偏向撑场，永远是世界上最特别的那个人。"
        ),
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=-0.20,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=True,
        allow_bili_summary=True,
        allow_search=True,
        allow_file_read=True,
        allow_text_card=True,
        allow_dual_profile=True,
        allow_video_watch=True,
        allow_carbon=True,
        max_tier=4,
        budget_multiplier=Decimal("2.50"),
        owner_exclusive=True,
    ),
)

TIER_SPECS: tuple[dict[str, Any], ...] = (
    {
        "min_score": 0,
        "max_score": 0,
        "score_range": "0",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "彻底无视，禁止互动与消耗额度",
    },
    {
        "min_score": 1,
        "max_score": 19,
        "score_range": "1-19",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "直接回复概率 50%，限 Tier 1，0.3x 预算，禁图与扩展工具",
    },
    {
        "min_score": 20,
        "max_score": 39,
        "score_range": "20-39",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "直接回复概率 85%，门槛 +0.16，0.6x 预算，限低清 1 图与 Tier 2",
    },
    {
        "min_score": 40,
        "max_score": 59,
        "score_range": "40-59",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "全量常规聊天，解锁表情包、贴表情、B 站摘要、联网检索、长卡片与双轨画像",
    },
    {
        "min_score": 60,
        "max_score": 79,
        "score_range": "60-79",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "门槛 -0.08 与 1.15x 预算，解锁主动戳与文件读取",
    },
    {
        "min_score": 80,
        "max_score": 100,
        "score_range": "80-100",
        "owner_only": False,
        "exclusive_perks": [],
        "summary": "门槛 -0.14 与 1.35x 预算，解锁原片看视频（非 Owner 封顶 100，仅 Tier 3）",
    },
    {
        "min_score": 100,
        "max_score": 120,
        "score_range": "100-120",
        "owner_only": True,
        "exclusive_perks": [
            "唯一 100–120 专属分域",
            "Carbon 深度推理 (Tier 4) 独享",
            "2.5x 单日预算倍率",
            "群聊合批绝对优先护航",
            "永不降级与免拉黑静音",
        ],
        "summary": "BOISER 唯一专属：门槛 -0.20、2.5x 预算、Carbon (Tier 4) 独享，100–120 永不降级",
    },
)


def _neutral_unlocked_caps(
    *, tier: str = "disabled", tier_label: str = "功能未启用"
) -> AffectionCapabilities:
    return AffectionCapabilities(
        level=6 if tier == "owner_bond" else 3,
        tier=tier,
        tier_label=tier_label,
        attitude="好感度门槛未启用，保持原有全能力正常互动。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=0.0,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=True,
        allow_bili_summary=True,
        allow_search=True,
        allow_file_read=True,
        allow_text_card=True,
        allow_dual_profile=True,
        allow_video_watch=True,
        allow_carbon=True,
        max_tier=4,
        budget_multiplier=Decimal("1.00"),
        owner_exclusive=(tier == "owner_bond"),
    )


def clamp_score(score: int, *, is_owner: bool) -> int:
    if is_owner:
        return max(100, min(120, int(score)))
    return max(0, min(100, int(score)))


def level_for_score(score: int, *, is_owner: bool) -> int:
    if is_owner:
        return 6
    bounded = clamp_score(score, is_owner=False)
    if bounded == 0:
        return 0
    if bounded < 20:
        return 1
    if bounded < 40:
        return 2
    if bounded < 60:
        return 3
    if bounded < 80:
        return 4
    return 5


def capabilities_for_score(score: int, *, is_owner: bool) -> AffectionCapabilities:
    return TIER_CAPS[level_for_score(score, is_owner=is_owner)]


def tier_matrix_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for caps, spec in zip(TIER_CAPS, TIER_SPECS, strict=True):
        rows.append(
            {
                "level": caps.level,
                "lv": caps.lv,
                "tier": caps.tier,
                "tier_label": caps.tier_label,
                "attitude": caps.attitude,
                "min_score": spec["min_score"],
                "max_score": spec["max_score"],
                "score_range": spec["score_range"],
                "owner_only": spec["owner_only"],
                "owner_exclusive": caps.owner_exclusive,
                "exclusive_perks": list(spec.get("exclusive_perks", ())),
                "summary": spec["summary"],
                "direct_reply_prob": caps.direct_reply_prob,
                "allow_poke_inbound": caps.allow_poke_inbound,
                "reply_threshold_delta": caps.reply_threshold_delta,
                "max_images": caps.max_images,
                "low_res_images_only": caps.low_res_images_only,
                "max_tier": caps.max_tier,
                "budget_multiplier": str(caps.budget_multiplier),
                "capabilities": caps.to_dict(),
            }
        )
    return rows


@dataclass(frozen=True)
class AffectionUpdate:
    user_id: str
    platform: str
    previous_score: int
    new_score: int
    delta: int
    tier: str
    tier_label: str
    level: int
    is_owner: bool
    changed: bool
    blacklisted_now: bool = False
    farewell_message: str | None = None
    reason: str = ""
    today_positive_gain: int = 0
