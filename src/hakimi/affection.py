"""Encrypted per-user affection scoring, 7-tier capability gating, and violation penalties."""

import asyncio
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from .config import Settings
from .models import Message
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


def _validate_platform_user(platform: str, user_id: str) -> str:
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


class AffectionManager:
    """Manages encrypted per-user/person affection scores, capabilities, and moderation sync."""

    def __init__(self, store: Any, *, clock: Any = time.time):
        self.store = store
        self.s: Settings = store.s
        self.clock = clock
        self.env_config = AffectionConfig.from_settings(self.s)
        self.config = self.env_config
        self.overridden = False
        self.initialized = False
        self.init_lock = asyncio.Lock()
        self._lock = asyncio.Lock()
        self._identity: Any = None
        self._migrated_to_person = False
        self._user_scores: dict[str, tuple[int, bool]] = {}
        self._person_scores: dict[str, int] = {}
        self._subject_multipliers: dict[str, Decimal] = {}
        self._annoyance_at: dict[str, float] = {}

    def bind_identity(self, identity: Any) -> None:
        """Bind an IdentityService so affection tracks unified cross-platform person_ids."""
        self._identity = identity

    def _get_identity(self) -> Any:
        if self._identity is not None:
            return self._identity
        runtime = getattr(self.store, "runtime", None)
        return getattr(runtime, "identity", None) if runtime is not None else None

    @property
    def scope(self) -> str:
        return self.store.vault.digest(
            "affection", self.s.namespace, self.s.onebot_self_id, self.s.qq_app_id
        )

    def _bot_id(self, platform: str) -> str:
        if platform == "onebot":
            return self.s.onebot_self_id
        if platform == "qq":
            return self.s.qq_app_id
        binding = self.s.social_owner_bindings.get(platform)
        return binding.bot_id if binding is not None else ""

    def _legacy_user_key(self, platform: str, user_id: str) -> str:
        return self.store.vault.digest("affection-user", self.scope, platform, user_id)

    def _person_key(self, person_id: str) -> str:
        return self.store.vault.digest("affection-person", self.scope, person_id)

    def _user_key(self, platform: str, user_id: str) -> str:
        identity = self._get_identity()
        if identity is not None:
            pid = identity.cached_person_id(platform, user_id)
            if pid is not None:
                return self._person_key(pid)
        return self._legacy_user_key(platform, user_id)

    def _subject(self, platform: str, user_id: str, bot_id: str | None = None) -> str:
        resolved_bot = bot_id if bot_id is not None else self._bot_id(platform)
        return self.store.vault.digest("subject", self.s.namespace, platform, resolved_bot, user_id)

    def _today_str(self, now: float | None = None) -> str:
        stamp = self.clock() if now is None else now
        try:
            tz = ZoneInfo(self.s.bot_timezone)
        except ZoneInfoNotFoundError:
            tz = ZoneInfo("UTC")
        return datetime.fromtimestamp(stamp, UTC).astimezone(tz).date().isoformat()

    def is_owner_id(self, platform: str, user_id: str) -> bool:
        uid = str(user_id or "").strip()
        if not uid:
            return False
        if platform == "onebot":
            owners = {self.s.owner_qq_id, *self.s.onebot_owner_ids} - {""}
            if uid in owners:
                return True
        elif platform == "qq":
            if uid in (set(self.s.qq_owner_ids) - {""}):
                return True
        else:
            binding = self.s.social_owner_bindings.get(platform)
            if binding is not None and uid == binding.user_id:
                return True
        identity = self._get_identity()
        if identity is not None and identity.is_owner_account(platform, uid):
            return True
        return False

    def _is_known_bot(self, platform: str, user_id: str) -> bool:
        return platform == "onebot" and user_id in self.s.bot_qq_ids

    def _seed_owner_caches(self, identity: Any = None) -> None:
        owner_caps = TIER_CAPS[6]
        for uid in {self.s.owner_qq_id, *self.s.onebot_owner_ids} - {""}:
            uk = self._user_key("onebot", uid)
            if uk not in self._user_scores:
                self._user_scores[uk] = (120, True)
            self._subject_multipliers.setdefault(
                self._subject("onebot", uid), owner_caps.budget_multiplier
            )
        for uid in set(self.s.qq_owner_ids) - {""}:
            uk = self._user_key("qq", uid)
            if uk not in self._user_scores:
                self._user_scores[uk] = (120, True)
            self._subject_multipliers.setdefault(
                self._subject("qq", uid), owner_caps.budget_multiplier
            )
        for plat, binding in self.s.social_owner_bindings.items():
            uk = self._user_key(plat, binding.user_id)
            if uk not in self._user_scores:
                self._user_scores[uk] = (120, True)
            self._subject_multipliers.setdefault(
                self._subject(plat, binding.user_id, binding.bot_id), owner_caps.budget_multiplier
            )
            self._subject_multipliers.setdefault(
                self._subject(plat, binding.user_id), owner_caps.budget_multiplier
            )
        if identity is not None and getattr(identity, "initialized", False):
            ok = self._person_key("owner")
            if ok not in self._user_scores:
                self._user_scores[ok] = (120, True)
            self._person_scores.setdefault("owner", self._user_scores[ok][0])
            for plat, uid in identity.linked_accounts_cached("owner"):
                self._subject_multipliers.setdefault(
                    self._subject(plat, uid), owner_caps.budget_multiplier
                )

    def _update_caches(
        self,
        platform: str,
        user_id: str,
        score: int,
        *,
        is_owner: bool,
        bot_id: str | None = None,
        person_id: str | None = None,
        identity: Any = None,
    ) -> AffectionCapabilities:
        bounded = clamp_score(score, is_owner=is_owner)
        caps = capabilities_for_score(bounded, is_owner=is_owner)
        if person_id is not None and identity is not None:
            pk = self._person_key(person_id)
            self._user_scores[pk] = (bounded, is_owner)
            self._person_scores[person_id] = bounded
            accounts = list(identity.linked_accounts_cached(person_id))
            if platform and user_id and (platform, user_id) not in accounts:
                accounts.append((platform, user_id))
            for plat, uid in accounts:
                self._subject_multipliers[self._subject(plat, uid)] = caps.budget_multiplier
                if (
                    bot_id is not None
                    and plat == platform
                    and uid == user_id
                    and bot_id != self._bot_id(plat)
                ):
                    self._subject_multipliers[self._subject(plat, uid, bot_id)] = (
                        caps.budget_multiplier
                    )
            return caps
        uk = self._legacy_user_key(platform, user_id)
        self._user_scores[uk] = (bounded, is_owner)
        self._subject_multipliers[self._subject(platform, user_id, bot_id)] = caps.budget_multiplier
        if bot_id is not None and bot_id != self._bot_id(platform):
            self._subject_multipliers[self._subject(platform, user_id)] = caps.budget_multiplier
        return caps

    async def _reload_caches_unlocked(self, identity: Any = None) -> None:
        async with self.store.lock:
            user_rows = await self.store.all(
                "SELECT user_key, subject, payload FROM user_affection WHERE scope=?",
                (self.scope,),
            )
        next_scores: dict[str, tuple[int, bool]] = {}
        next_person_scores: dict[str, int] = {}
        next_mults: dict[str, Decimal] = {}
        for row in user_rows:
            data = self.store.vault.open("affection:" + row["user_key"], row["payload"])
            if not isinstance(data, dict):
                continue
            platform = str(data.get("platform", "onebot"))
            uid = str(data.get("user_id", ""))
            pid = str(data.get("person_id", "")) if data.get("person_id") else ""
            is_owner = (pid == "owner") or (bool(uid) and self.is_owner_id(platform, uid))
            default_score = 120 if is_owner else self.config.initial_score
            score = clamp_score(int(data.get("score", default_score)), is_owner=is_owner)
            caps = capabilities_for_score(score, is_owner=is_owner)
            next_scores[row["user_key"]] = (score, is_owner)
            if pid:
                next_person_scores[pid] = score
            if uid:
                next_mults[row["subject"]] = caps.budget_multiplier
                next_mults[self._subject(platform, uid)] = caps.budget_multiplier
            if pid and identity is not None and getattr(identity, "initialized", False):
                for plat, acc_uid in identity.linked_accounts_cached(pid):
                    next_mults[self._subject(plat, acc_uid)] = caps.budget_multiplier
        self._user_scores = next_scores
        self._person_scores = next_person_scores
        self._subject_multipliers = next_mults
        self._seed_owner_caches(identity)

    async def _migrate_to_identity_unlocked(self, identity: Any) -> None:
        if identity is None or not getattr(identity, "initialized", False):
            return
        if self._migrated_to_person:
            return
        async with self.store.lock:
            already = await self.store.one(
                "SELECT 1 FROM meta WHERE key='affection_person_migrated_v1'"
            )
            user_rows = (
                []
                if already
                else await self.store.all(
                    "SELECT user_key, subject, payload, updated FROM user_affection WHERE scope=?",
                    (self.scope,),
                )
            )
            hist_rows = (
                []
                if already
                else await self.store.all(
                    "SELECT user_key, payload FROM affection_history WHERE scope=?",
                    (self.scope,),
                )
            )
        if already:
            await self._reload_caches_unlocked(identity)
            self._migrated_to_person = True
            return

        legacy_items: list[tuple[str, dict[str, Any], str]] = []
        for row in user_rows:
            data = self.store.vault.open("affection:" + row["user_key"], row["payload"])
            if not isinstance(data, dict):
                continue
            existing_pid = str(data.get("person_id", ""))
            if existing_pid and row["user_key"] == self._person_key(existing_pid):
                continue
            platform = str(data.get("platform", "onebot"))
            uid = str(data.get("user_id", ""))
            if not uid:
                continue
            nickname = str(data.get("nickname", ""))
            pid = await identity.resolve_person_id(
                platform, uid, nickname, create=True, source="auto"
            )
            if pid is not None:
                legacy_items.append((row["user_key"], data, pid))

        legacy_user_keys = {old_key for old_key, _, _ in legacy_items}
        extra_hist_seen: set[str] = set()
        extra_hist_migrations: list[tuple[str, str]] = []
        for hrow in hist_rows:
            hkey = str(hrow["user_key"])
            if hkey in legacy_user_keys or hkey in extra_hist_seen:
                continue
            hdata = self.store.vault.open("affection-hist:" + hkey, hrow["payload"])
            if not isinstance(hdata, dict):
                continue
            existing_pid = str(hdata.get("person_id") or "")
            if existing_pid and hkey == self._person_key(existing_pid):
                continue
            hplat = str(hdata.get("platform", "onebot"))
            huid = str(hdata.get("user_id", ""))
            if not huid:
                continue
            hpid = await identity.resolve_person_id(hplat, huid, create=False)
            if hpid is not None:
                extra_hist_seen.add(hkey)
                extra_hist_migrations.append((hkey, hpid))

        today = self._today_str()
        now = float(self.clock())
        zero_person_ids: list[str] = []

        async with self._lock:
            async with self.store.transaction():
                if await self.store.one(
                    "SELECT 1 FROM meta WHERE key='affection_person_migrated_v1'"
                ):
                    self._migrated_to_person = True
                    return

                grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {}
                for old_key, data, pid in legacy_items:
                    grouped.setdefault(pid, []).append((old_key, data))

                for pid, items in grouped.items():
                    person_key = self._person_key(pid)
                    existing_prow = await self.store.one(
                        "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                        (person_key, self.scope),
                    )
                    existing_pdata = (
                        self.store.vault.open("affection:" + person_key, existing_prow["payload"])
                        if existing_prow
                        else None
                    )
                    all_entries: list[dict[str, Any]] = [
                        *([existing_pdata] if isinstance(existing_pdata, dict) else []),
                        *(d for _, d in items),
                    ]
                    is_owner = (pid == "owner") or any(
                        self.is_owner_id(
                            str(e.get("platform", "onebot")), str(e.get("user_id", ""))
                        )
                        for e in all_entries
                    )
                    raw_scores = [
                        clamp_score(
                            int(e.get("score", 120 if is_owner else self.config.initial_score)),
                            is_owner=is_owner,
                        )
                        for e in all_entries
                    ]
                    if is_owner:
                        merged_score = clamp_score(max(raw_scores), is_owner=True)
                    elif 0 in raw_scores:
                        merged_score = 0
                        zero_person_ids.append(pid)
                    else:
                        merged_score = clamp_score(min(raw_scores), is_owner=False)

                    latest = max(all_entries, key=lambda e: float(e.get("updated_at", 0.0) or 0.0))
                    rep_platform = str(latest.get("platform", "onebot"))
                    rep_uid = str(latest.get("user_id", ""))
                    same_day_entries = [e for e in all_entries if e.get("daily_day") == today]
                    today_gain = max(
                        (max(0, int(e.get("today_positive_gain", 0))) for e in same_day_entries),
                        default=0,
                    )
                    first_day = next(
                        (
                            str(e.get("daily_first_day", ""))
                            for e in same_day_entries
                            if str(e.get("daily_first_day", ""))
                        ),
                        "",
                    )
                    last_annoyance = max(
                        (float(e.get("last_annoyance_at", 0.0) or 0.0) for e in all_entries),
                        default=0.0,
                    )
                    last_negative = max(
                        (float(e.get("last_negative_at", 0.0) or 0.0) for e in all_entries),
                        default=0.0,
                    )
                    last_recovery_day = max(
                        (str(e.get("last_recovery_day", "") or "") for e in all_entries),
                        default="",
                    )
                    updated_at = max(
                        (float(e.get("updated_at", 0.0) or 0.0) for e in all_entries),
                        default=now,
                    )
                    merged_record = self._normalize_record(
                        {
                            **latest,
                            "score": merged_score,
                            "daily_day": today,
                            "today_positive_gain": today_gain,
                            "daily_first_day": first_day,
                            "last_annoyance_at": last_annoyance,
                            "last_negative_at": last_negative,
                            "last_recovery_day": last_recovery_day,
                            "updated_at": updated_at,
                        },
                        rep_platform,
                        rep_uid,
                        "",
                        today,
                        is_owner=is_owner,
                        person_id=pid,
                    )
                    sealed = self.store.vault.seal("affection:" + person_key, merged_record)
                    subject = self._subject(rep_platform, rep_uid)
                    await self.store.db.execute(
                        "INSERT INTO user_affection(user_key,scope,subject,payload,updated) "
                        "VALUES (?,?,?,?,?) "
                        "ON CONFLICT(user_key) DO UPDATE SET scope=excluded.scope,"
                        "subject=excluded.subject,payload=excluded.payload,updated=excluded.updated",
                        (
                            person_key,
                            self.scope,
                            subject,
                            sealed,
                            float(merged_record["updated_at"]),
                        ),
                    )
                    for old_key, _ in items:
                        if old_key != person_key:
                            await self.store.db.execute(
                                "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                                (old_key, self.scope),
                            )
                            await self.store.db.execute(
                                "UPDATE affection_events SET user_key=? "
                                "WHERE scope=? AND user_key=?",
                                (person_key, self.scope, old_key),
                            )
                            await self._migrate_history_key_in_tx(
                                old_key, person_key, person_id=pid
                            )

                for old_hist_key, pid in extra_hist_migrations:
                    await self._migrate_history_key_in_tx(
                        old_hist_key, self._person_key(pid), person_id=pid
                    )

                await self.store.db.execute(
                    "INSERT OR IGNORE INTO meta VALUES ('affection_person_migrated_v1', ?)",
                    (self.store.vault.seal("meta:affection_person_migrated_v1", {"v": 1}),),
                )

        for pid in zero_person_ids:
            if pid != "owner":
                await identity.set_person_muted(pid, True, source="ai_affection_zero")

        await self._reload_caches_unlocked(identity)
        self._migrated_to_person = True

    async def migrate_to_identity(self) -> None:
        identity = self._get_identity()
        if identity is None or not getattr(identity, "initialized", False):
            return
        async with self.init_lock:
            await self._migrate_to_identity_unlocked(identity)

    async def _ensure_identity_ready(self) -> Any:
        identity = self._get_identity()
        if identity is None:
            return None
        if not getattr(identity, "initialized", False):
            await identity.initialize()
        if not self._migrated_to_person:
            await self.migrate_to_identity()
        return identity

    async def initialize(self) -> None:
        identity = self._get_identity()
        async with self.init_lock:
            if self.initialized:
                if (
                    identity is not None
                    and getattr(identity, "initialized", False)
                    and not self._migrated_to_person
                ):
                    await self._migrate_to_identity_unlocked(identity)
                return
            self.env_config = AffectionConfig.from_settings(self.s)
            async with self.store.transaction():
                await self.store.db.executescript(SCHEMA)
            async with self.store.lock:
                cfg_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='affection'"
                )
            if cfg_row:
                saved = self.store.vault.open("admin:affection", cfg_row[0])
                merged = (
                    {**self.env_config.model_dump(), **saved} if isinstance(saved, dict) else saved
                )
                self.config = AffectionConfig.model_validate(merged)
                self.overridden = True
            else:
                self.config = self.env_config
                self.overridden = False

            await self._reload_caches_unlocked(identity)
            if identity is not None and getattr(identity, "initialized", False):
                await self._migrate_to_identity_unlocked(identity)
            self.initialized = True

    def _default_record(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        is_owner: bool | None = None,
        person_id: str | None = None,
    ) -> dict[str, Any]:
        owner_flag = self.is_owner_id(platform, user_id) if is_owner is None else bool(is_owner)
        score = 120 if owner_flag else self.config.initial_score
        record: dict[str, Any] = {
            "user_id": user_id,
            "platform": platform,
            "nickname": _sanitize_nickname(nickname, self.s),
            "score": score,
            "daily_day": today,
            "today_positive_gain": 0,
            "daily_first_day": "",
            "last_annoyance_at": 0.0,
            "last_negative_at": 0.0,
            "last_recovery_day": "",
            "last_reason": "init",
            "last_delta": 0,
            "updated_at": 0.0,
        }
        if person_id:
            record["person_id"] = person_id
        return record

    def _normalize_record(
        self,
        data: dict[str, Any],
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        is_owner: bool | None = None,
        person_id: str | None = None,
    ) -> dict[str, Any]:
        owner_flag = self.is_owner_id(platform, user_id) if is_owner is None else bool(is_owner)
        default_score = 120 if owner_flag else self.config.initial_score
        score = clamp_score(int(data.get("score", default_score)), is_owner=owner_flag)
        same_day = data.get("daily_day") == today
        today_gain = max(0, int(data.get("today_positive_gain", 0))) if same_day else 0
        first_day = str(data.get("daily_first_day", "")) if same_day else ""
        clean_nick = (
            _sanitize_nickname(nickname, self.s)
            if nickname
            else _sanitize_nickname(data.get("nickname", ""), self.s)
        )
        resolved_pid = person_id or (str(data["person_id"]) if data.get("person_id") else None)
        record: dict[str, Any] = {
            "user_id": user_id,
            "platform": platform,
            "nickname": clean_nick,
            "score": score,
            "daily_day": today,
            "today_positive_gain": today_gain,
            "daily_first_day": first_day,
            "last_annoyance_at": float(data.get("last_annoyance_at", 0.0) or 0.0),
            "last_negative_at": float(data.get("last_negative_at", 0.0) or 0.0),
            "last_recovery_day": str(data.get("last_recovery_day", "") or ""),
            "last_reason": _sanitize_reason(data.get("last_reason", "chat"), self.s),
            "last_delta": int(data.get("last_delta", 0) or 0),
            "updated_at": float(data.get("updated_at", 0.0) or 0.0),
        }
        if resolved_pid:
            record["person_id"] = resolved_pid
        return record

    async def _load_user_unlocked(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
        *,
        person_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        is_owner = (person_id == "owner") or self.is_owner_id(platform, user_id)
        user_key = (
            self._person_key(person_id)
            if person_id is not None
            else self._legacy_user_key(platform, user_id)
        )
        row = await self.store.one(
            "SELECT payload FROM user_affection WHERE user_key=?", (user_key,)
        )
        if not row:
            return (
                self._default_record(
                    platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
                ),
                False,
            )
        opened = self.store.vault.open("affection:" + user_key, row["payload"])
        if not isinstance(opened, dict):
            return (
                self._default_record(
                    platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
                ),
                False,
            )
        return (
            self._normalize_record(
                opened, platform, user_id, nickname, today, is_owner=is_owner, person_id=person_id
            ),
            True,
        )

    async def _save_user_in_tx(
        self,
        platform: str,
        user_id: str,
        record: dict[str, Any],
        *,
        bot_id: str | None = None,
        person_id: str | None = None,
    ) -> None:
        if person_id is not None:
            record = {**record, "person_id": person_id}
            user_key = self._person_key(person_id)
        else:
            user_key = self._legacy_user_key(platform, user_id)
        subject = self._subject(platform, user_id, bot_id)
        sealed = self.store.vault.seal("affection:" + user_key, record)
        await self.store.db.execute(
            "INSERT INTO user_affection(user_key,scope,subject,payload,updated) VALUES (?,?,?,?,?) "
            "ON CONFLICT(user_key) DO UPDATE SET scope=excluded.scope,subject=excluded.subject,"
            "payload=excluded.payload,updated=excluded.updated",
            (user_key, self.scope, subject, sealed, float(record["updated_at"])),
        )

    async def _append_history_in_tx(
        self,
        *,
        user_key: str,
        platform: str,
        user_id: str,
        person_id: str | None,
        nickname: str,
        prev_score: int,
        new_score: int,
        kind: str,
        reason: str,
        now: float,
    ) -> None:
        delta = int(new_score) - int(prev_score)
        if delta == 0:
            return
        await self.store.db.execute(
            "DELETE FROM affection_history WHERE created<?",
            (now - 90 * 86400,),
        )
        payload_data = {
            "user_id": str(user_id),
            "platform": str(platform),
            "person_id": str(person_id) if person_id else None,
            "nickname": _sanitize_nickname(nickname, self.s),
            "prev_score": int(prev_score),
            "new_score": int(new_score),
            "delta": delta,
            "kind": str(kind),
            "reason": _sanitize_reason(reason, self.s, default=kind or "chat"),
        }
        sealed = self.store.vault.seal("affection-hist:" + user_key, payload_data)
        await self.store.db.execute(
            "INSERT INTO affection_history(scope,user_key,delta,payload,created) "
            "VALUES (?,?,?,?,?)",
            (self.scope, user_key, delta, sealed, float(now)),
        )

    async def _migrate_history_key_in_tx(
        self,
        old_key: str,
        new_key: str,
        *,
        person_id: str | None = None,
    ) -> None:
        if old_key == new_key:
            return
        rows = await self.store.all(
            "SELECT id, payload FROM affection_history WHERE scope=? AND user_key=?",
            (self.scope, old_key),
        )
        for row in rows:
            opened = self.store.vault.open("affection-hist:" + old_key, row["payload"])
            if isinstance(opened, dict):
                updated_payload = dict(opened)
                if person_id is not None:
                    updated_payload["person_id"] = person_id
                re_sealed = self.store.vault.seal("affection-hist:" + new_key, updated_payload)
                await self.store.db.execute(
                    "UPDATE affection_history SET user_key=?, payload=? WHERE id=?",
                    (new_key, re_sealed, int(row["id"])),
                )
            else:
                await self.store.db.execute(
                    "DELETE FROM affection_history WHERE id=?",
                    (int(row["id"]),),
                )

    def _build_profile_dict(
        self, record: dict[str, Any], *, person_id: str | None = None
    ) -> dict[str, Any]:
        platform = str(record["platform"])
        uid = str(record["user_id"])
        pid = person_id or (str(record["person_id"]) if record.get("person_id") else None)
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        score = clamp_score(int(record["score"]), is_owner=is_owner)
        caps = capabilities_for_score(score, is_owner=is_owner)
        identity = self._get_identity()
        muted = (not is_owner) and (
            (platform == "onebot" and uid in set(self.s.muted_user_ids))
            or (identity is not None and identity.is_muted(platform, uid))
        )
        out: dict[str, Any] = {
            "user_id": uid,
            "platform": platform,
            "nickname": record["nickname"],
            "score": score,
            "level": caps.level,
            "lv": caps.lv,
            "tier": caps.tier,
            "tier_label": caps.tier_label,
            "attitude": caps.attitude,
            "is_owner": is_owner,
            "owner_exclusive": caps.owner_exclusive,
            "muted": muted,
            "today_positive_gain": int(record["today_positive_gain"]),
            "daily_positive_cap": self.config.daily_positive_cap,
            "last_reason": record["last_reason"],
            "last_delta": int(record.get("last_delta", 0)),
            "last_negative_at": float(record.get("last_negative_at", 0.0) or 0.0),
            "updated_at": float(record.get("updated_at", 0.0)),
            "capabilities": caps.to_dict(),
        }
        if pid is not None:
            out["person_id"] = pid
        return out

    async def person_scores_snapshot(self) -> dict[str, int]:
        await self.initialize()
        await self._ensure_identity_ready()
        snapshot = dict(self._person_scores)
        snapshot.setdefault("owner", 120)
        return snapshot

    async def refresh_person_caches(self, person_id: str) -> None:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        if identity is None:
            return
        is_owner = person_id == "owner"
        pk = self._person_key(person_id)
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                (pk, self.scope),
            )
        if row:
            opened = self.store.vault.open("affection:" + pk, row["payload"])
            if isinstance(opened, dict):
                score = clamp_score(
                    int(opened.get("score", 120 if is_owner else self.config.initial_score)),
                    is_owner=is_owner,
                )
                self._update_caches(
                    str(opened.get("platform", "onebot")),
                    str(opened.get("user_id", "")),
                    score,
                    is_owner=is_owner,
                    person_id=person_id,
                    identity=identity,
                )
                return
        accounts = identity.linked_accounts_cached(person_id)
        default_score = 120 if is_owner else self.config.initial_score
        caps = capabilities_for_score(default_score, is_owner=is_owner)
        self._user_scores[pk] = (default_score, is_owner)
        for plat, uid in accounts:
            self._subject_multipliers[self._subject(plat, uid)] = caps.budget_multiplier

    async def profile(
        self,
        platform: str,
        user_id: str,
        nickname: str = "",
        *,
        subject: str | None = None,
    ) -> dict[str, Any]:
        del subject
        await self.initialize()
        uid = _validate_platform_user(platform, user_id)
        identity = await self._ensure_identity_ready()
        pid = (
            await identity.resolve_person_id(platform, uid, nickname, create=False)
            if identity is not None
            else None
        )
        today = self._today_str()
        async with self.store.lock:
            record, _ = await self._load_user_unlocked(
                platform, uid, nickname, today, person_id=pid
            )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        if pid is not None and identity is not None:
            self._update_caches(
                platform,
                uid,
                record["score"],
                is_owner=is_owner,
                person_id=pid,
                identity=identity,
            )
        elif identity is None:
            self._update_caches(platform, uid, record["score"], is_owner=is_owner)
        return self._build_profile_dict(record, person_id=pid)

    async def capabilities_for(self, message: Message) -> AffectionCapabilities:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        if not self.config.enabled or (
            identity is None and message.platform not in {"onebot", "qq"}
        ):
            is_owner = self.is_owner_id(message.platform, message.user_id)
            return _neutral_unlocked_caps(
                tier="owner_bond" if is_owner else "disabled",
                tier_label="专属羁绊 (Owner)" if is_owner else "功能未启用",
            )
        if self._is_known_bot(message.platform, message.user_id):
            return _neutral_unlocked_caps(tier="bot_bypass", tier_label="Bot 豁免")

        pid = (
            await identity.resolve_person_id(
                message.platform,
                message.user_id,
                message.sender_nickname,
                create=True,
                source="auto",
            )
            if identity is not None
            else None
        )
        is_owner = (pid == "owner") or self.is_owner_id(message.platform, message.user_id)
        uk = (
            self._person_key(pid)
            if pid is not None
            else self._legacy_user_key(message.platform, message.user_id)
        )
        cached = self._user_scores.get(uk)
        if cached is not None:
            caps = capabilities_for_score(cached[0], is_owner=is_owner)
            self._subject_multipliers[
                self._subject(message.platform, message.user_id, message.bot_id)
            ] = caps.budget_multiplier
            return caps
        today = self._today_str()
        async with self.store.lock:
            record, _ = await self._load_user_unlocked(
                message.platform,
                message.user_id,
                message.sender_nickname,
                today,
                person_id=pid,
            )
        return self._update_caches(
            message.platform,
            message.user_id,
            record["score"],
            is_owner=is_owner,
            bot_id=message.bot_id,
            person_id=pid,
            identity=identity,
        )

    async def capabilities_for_batch(
        self, message: Message, members: list[Message] | None = None
    ) -> tuple[AffectionCapabilities, Message]:
        await self.initialize()
        await self._ensure_identity_ready()
        if not self.config.enabled or not members or len(members) <= 1:
            return (await self.capabilities_for(message), message)
        for owner_msg in reversed(members):
            if self.is_owner_id(owner_msg.platform, owner_msg.user_id):
                return (await self.capabilities_for(owner_msg), owner_msg)
        non_bots = [m for m in members if not self._is_known_bot(m.platform, m.user_id)]
        if not non_bots:
            return (await self.capabilities_for(message), message)
        addressed = [m for m in non_bots if m.addressed]
        pool = addressed if addressed else non_bots
        best_caps: AffectionCapabilities | None = None
        best_msg: Message | None = None
        for candidate in reversed(pool):
            caps = await self.capabilities_for(candidate)
            if best_caps is None or caps.level > best_caps.level:
                best_caps = caps
                best_msg = candidate
        if best_caps is None or best_msg is None:
            return (await self.capabilities_for(message), message)
        return (best_caps, best_msg)

    def budget_multiplier_for_subject(self, subject: str) -> Decimal:
        if not self.config.enabled:
            return Decimal("1")
        return self._subject_multipliers.get(subject, Decimal("1"))

    def adjust_reply_threshold(self, message: Message | None, base_threshold: float) -> float:
        base = float(base_threshold)
        if not self.config.enabled or message is None:
            return base
        if self._is_known_bot(message.platform, message.user_id):
            return base
        is_owner = self.is_owner_id(message.platform, message.user_id)
        uk = self._user_key(message.platform, message.user_id)
        cached = self._user_scores.get(uk)
        score = (
            cached[0] if cached is not None else (120 if is_owner else self.config.initial_score)
        )
        lvl = level_for_score(score, is_owner=is_owner)
        if lvl == 0:
            return round(base + 1.0, 4)
        if lvl == 1:
            return 0.95
        if lvl == 2:
            return round(min(0.95, base + 0.16), 4)
        if lvl == 3:
            return round(base, 4)
        if lvl == 4:
            return round(max(0.15, base - 0.08), 4)
        if lvl == 5:
            return round(max(0.12, base - 0.14), 4)
        return round(max(0.10, base - 0.20), 4)

    async def _maybe_mute_zero(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        moderation: Any = None,
        *,
        person_id: str | None = None,
    ) -> None:
        if person_id == "owner" or self.is_owner_id(platform, user_id):
            return
        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        identity = self._get_identity()
        if identity is not None and person_id is not None:
            await identity.set_person_muted(
                person_id, True, moderation=mod, source="ai_affection_zero"
            )
            return
        if platform != "onebot":
            return
        if mod is None:
            return
        if hasattr(mod, "mute_by_affection"):
            await mod.mute_by_affection(user_id, nickname)
        elif hasattr(mod, "set_muted"):
            await mod.set_muted(user_id, True)

    async def apply_turn_delta(
        self,
        message: Message,
        model_delta: int,
        *,
        sandbox_mode: bool = False,
        daily_first_bonus: bool = True,
        reason: str = "chat",
    ) -> AffectionUpdate:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        platform, uid = message.platform, message.user_id
        pid = (
            await identity.resolve_person_id(
                platform, uid, message.sender_nickname, create=True, source="auto"
            )
            if identity is not None
            and not self._is_known_bot(platform, uid)
            and uid != self._bot_id(platform)
            else None
        )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        if (
            not self.config.enabled
            or self._is_known_bot(platform, uid)
            or uid == self._bot_id(platform)
        ):
            score = 120 if is_owner else self.config.initial_score
            caps = capabilities_for_score(score, is_owner=is_owner)
            return AffectionUpdate(
                user_id=uid,
                platform=platform,
                previous_score=score,
                new_score=score,
                delta=0,
                tier=caps.tier,
                tier_label=caps.tier_label,
                level=caps.level,
                is_owner=is_owner,
                changed=False,
                reason="skipped",
                today_positive_gain=0,
            )

        if isinstance(model_delta, bool) or not isinstance(model_delta, int):
            clamped_model = 0
        else:
            clamped_model = max(-35, min(2, model_delta))

        now = float(self.clock())
        today = self._today_str(now)
        clean_reason = _sanitize_reason(reason, self.s, default="chat")
        blacklisted_now = False

        async with self._lock:
            async with self.store.transaction():
                record, existed = await self._load_user_unlocked(
                    platform, uid, message.sender_nickname, today, person_id=pid
                )
                prev_score = int(record["score"])
                today_gain = int(record["today_positive_gain"])
                first_day = str(record["daily_first_day"])

                if prev_score == 0 and not is_owner:
                    new_score = 0
                    effective_delta = 0
                    new_today_gain = today_gain
                    next_first_day = first_day
                else:
                    if sandbox_mode:
                        desired_delta = min(0, clamped_model)
                        next_first_day = first_day
                    else:
                        first_bonus = (
                            1
                            if (daily_first_bonus and clamped_model >= 0 and first_day != today)
                            else 0
                        )
                        desired_delta = clamped_model + first_bonus
                        next_first_day = (
                            today if (daily_first_bonus and clamped_model >= 0) else first_day
                        )

                    if desired_delta > 0:
                        remaining_cap = max(0, self.config.daily_positive_cap - today_gain)
                        effective_delta = min(desired_delta, remaining_cap)
                    else:
                        effective_delta = desired_delta

                    new_score = clamp_score(prev_score + effective_delta, is_owner=is_owner)
                    actual_delta = new_score - prev_score
                    new_today_gain = today_gain + max(0, actual_delta)

                actual_delta = new_score - prev_score
                changed = actual_delta != 0
                should_persist = existed or not sandbox_mode or changed
                if should_persist:
                    updates: dict[str, Any] = {
                        "score": new_score,
                        "daily_day": today,
                        "today_positive_gain": new_today_gain,
                        "daily_first_day": next_first_day,
                        "last_reason": clean_reason
                        if (changed or not existed)
                        else record["last_reason"],
                        "last_delta": actual_delta if changed else record.get("last_delta", 0),
                        "updated_at": now,
                    }
                    if actual_delta < 0:
                        updates["last_negative_at"] = now
                        updates["last_recovery_day"] = ""
                    record.update(updates)
                    await self._save_user_in_tx(
                        platform, uid, record, bot_id=message.bot_id, person_id=pid
                    )
                if changed:
                    user_key = (
                        self._person_key(pid)
                        if pid is not None
                        else self._legacy_user_key(platform, uid)
                    )
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=new_score,
                        kind="chat",
                        reason=clean_reason,
                        now=now,
                    )
                caps = self._update_caches(
                    platform,
                    uid,
                    new_score,
                    is_owner=is_owner,
                    bot_id=message.bot_id,
                    person_id=pid,
                    identity=identity,
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"], person_id=pid)
        return AffectionUpdate(
            user_id=uid,
            platform=platform,
            previous_score=prev_score,
            new_score=new_score,
            delta=actual_delta,
            tier=caps.tier,
            tier_label=caps.tier_label,
            level=caps.level,
            is_owner=is_owner,
            changed=changed,
            blacklisted_now=blacklisted_now,
            farewell_message=self.config.farewell_message if blacklisted_now else None,
            reason=clean_reason,
            today_positive_gain=new_today_gain,
        )

    async def record_violation(
        self,
        message: Message,
        kind: Literal["prompt_injection", "content_blocked"],
    ) -> AffectionUpdate:
        if kind not in VIOLATION_DELTAS:
            raise Rejected("invalid_affection_violation")
        await self.initialize()
        identity = await self._ensure_identity_ready()
        platform, uid = message.platform, message.user_id
        pid = (
            await identity.resolve_person_id(
                platform, uid, message.sender_nickname, create=True, source="auto"
            )
            if identity is not None
            and not self._is_known_bot(platform, uid)
            and uid != self._bot_id(platform)
            else None
        )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        if (
            not self.config.enabled
            or self._is_known_bot(platform, uid)
            or uid == self._bot_id(platform)
        ):
            score = 120 if is_owner else self.config.initial_score
            caps = capabilities_for_score(score, is_owner=is_owner)
            return AffectionUpdate(
                user_id=uid,
                platform=platform,
                previous_score=score,
                new_score=score,
                delta=0,
                tier=caps.tier,
                tier_label=caps.tier_label,
                level=caps.level,
                is_owner=is_owner,
                changed=False,
                reason=kind,
                today_positive_gain=0,
            )

        now = float(self.clock())
        today = self._today_str(now)
        raw_delta = VIOLATION_DELTAS[kind]
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        event_key = self.store.vault.digest("affection-violation", self.store.event_key(message))
        blacklisted_now = False

        async with self._lock:
            async with self.store.transaction():
                await self.store.db.execute(
                    "DELETE FROM affection_events WHERE created<?", (now - 30 * 86400,)
                )
                cur = await self.store.db.execute(
                    "INSERT OR IGNORE INTO affection_events"
                    "(event_key,scope,user_key,kind,delta,created) VALUES (?,?,?,?,?,?)",
                    (event_key, self.scope, user_key, kind, raw_delta, now),
                )
                record, _ = await self._load_user_unlocked(
                    platform, uid, message.sender_nickname, today, person_id=pid
                )
                prev_score = int(record["score"])
                if cur.rowcount != 1:
                    caps = self._update_caches(
                        platform,
                        uid,
                        prev_score,
                        is_owner=is_owner,
                        bot_id=message.bot_id,
                        person_id=pid,
                        identity=identity,
                    )
                    return AffectionUpdate(
                        user_id=uid,
                        platform=platform,
                        previous_score=prev_score,
                        new_score=prev_score,
                        delta=0,
                        tier=caps.tier,
                        tier_label=caps.tier_label,
                        level=caps.level,
                        is_owner=is_owner,
                        changed=False,
                        reason=f"{kind}_duplicate",
                        today_positive_gain=int(record["today_positive_gain"]),
                    )
                new_score = clamp_score(prev_score + raw_delta, is_owner=is_owner)
                actual_delta = new_score - prev_score
                updates: dict[str, Any] = {
                    "score": new_score,
                    "last_reason": kind,
                    "last_delta": actual_delta,
                    "updated_at": now,
                }
                if actual_delta < 0:
                    updates["last_negative_at"] = now
                    updates["last_recovery_day"] = ""
                record.update(updates)
                await self._save_user_in_tx(
                    platform, uid, record, bot_id=message.bot_id, person_id=pid
                )
                if actual_delta != 0:
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=new_score,
                        kind="violation",
                        reason=kind,
                        now=now,
                    )
                caps = self._update_caches(
                    platform,
                    uid,
                    new_score,
                    is_owner=is_owner,
                    bot_id=message.bot_id,
                    person_id=pid,
                    identity=identity,
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"], person_id=pid)
        return AffectionUpdate(
            user_id=uid,
            platform=platform,
            previous_score=prev_score,
            new_score=new_score,
            delta=actual_delta,
            tier=caps.tier,
            tier_label=caps.tier_label,
            level=caps.level,
            is_owner=is_owner,
            changed=actual_delta != 0,
            blacklisted_now=blacklisted_now,
            farewell_message=self.config.farewell_message if blacklisted_now else None,
            reason=kind,
            today_positive_gain=int(record["today_positive_gain"]),
        )

    async def record_annoyance(self, message: Message, reason: str = "spam") -> AffectionUpdate:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        platform, uid = message.platform, message.user_id
        pid = (
            await identity.resolve_person_id(
                platform, uid, message.sender_nickname, create=True, source="auto"
            )
            if identity is not None
            and not self._is_known_bot(platform, uid)
            and uid != self._bot_id(platform)
            else None
        )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        clean_reason = _sanitize_reason(reason, self.s, default="spam")
        if (
            not self.config.enabled
            or self._is_known_bot(platform, uid)
            or uid == self._bot_id(platform)
        ):
            score = 120 if is_owner else self.config.initial_score
            caps = capabilities_for_score(score, is_owner=is_owner)
            return AffectionUpdate(
                user_id=uid,
                platform=platform,
                previous_score=score,
                new_score=score,
                delta=0,
                tier=caps.tier,
                tier_label=caps.tier_label,
                level=caps.level,
                is_owner=is_owner,
                changed=False,
                reason=clean_reason,
                today_positive_gain=0,
            )

        now = float(self.clock())
        today = self._today_str(now)
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        blacklisted_now = False

        async with self._lock:
            async with self.store.transaction():
                record, _ = await self._load_user_unlocked(
                    platform, uid, message.sender_nickname, today, person_id=pid
                )
                prev_score = int(record["score"])
                last_at = max(
                    float(record.get("last_annoyance_at", 0.0) or 0.0),
                    self._annoyance_at.get(user_key, 0.0),
                )
                if now - last_at < ANNOYANCE_COOLDOWN_SECONDS:
                    caps = self._update_caches(
                        platform,
                        uid,
                        prev_score,
                        is_owner=is_owner,
                        bot_id=message.bot_id,
                        person_id=pid,
                        identity=identity,
                    )
                    return AffectionUpdate(
                        user_id=uid,
                        platform=platform,
                        previous_score=prev_score,
                        new_score=prev_score,
                        delta=0,
                        tier=caps.tier,
                        tier_label=caps.tier_label,
                        level=caps.level,
                        is_owner=is_owner,
                        changed=False,
                        reason=f"{clean_reason}_cooldown",
                        today_positive_gain=int(record["today_positive_gain"]),
                    )
                new_score = clamp_score(prev_score + ANNOYANCE_DELTA, is_owner=is_owner)
                actual_delta = new_score - prev_score
                self._annoyance_at[user_key] = now
                updates: dict[str, Any] = {
                    "score": new_score,
                    "last_annoyance_at": now,
                    "last_reason": clean_reason,
                    "last_delta": actual_delta,
                    "updated_at": now,
                }
                if actual_delta < 0:
                    updates["last_negative_at"] = now
                    updates["last_recovery_day"] = ""
                record.update(updates)
                await self._save_user_in_tx(
                    platform, uid, record, bot_id=message.bot_id, person_id=pid
                )
                if actual_delta != 0:
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=new_score,
                        kind="annoyance",
                        reason=clean_reason,
                        now=now,
                    )
                caps = self._update_caches(
                    platform,
                    uid,
                    new_score,
                    is_owner=is_owner,
                    bot_id=message.bot_id,
                    person_id=pid,
                    identity=identity,
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"], person_id=pid)
        return AffectionUpdate(
            user_id=uid,
            platform=platform,
            previous_score=prev_score,
            new_score=new_score,
            delta=actual_delta,
            tier=caps.tier,
            tier_label=caps.tier_label,
            level=caps.level,
            is_owner=is_owner,
            changed=actual_delta != 0,
            blacklisted_now=blacklisted_now,
            farewell_message=self.config.farewell_message if blacklisted_now else None,
            reason=clean_reason,
            today_positive_gain=int(record["today_positive_gain"]),
        )

    async def on_unmute_person(self, person_id: str) -> None:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        if identity is None or person_id == "owner":
            return
        now = float(self.clock())
        today = self._today_str(now)
        user_key = self._person_key(person_id)
        async with self._lock:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=?", (user_key,)
                )
                if not row:
                    return
                opened = self.store.vault.open("affection:" + user_key, row["payload"])
                if not isinstance(opened, dict):
                    return
                plat = str(opened.get("platform", "onebot"))
                uid = str(opened.get("user_id", ""))
                record = self._normalize_record(
                    opened, plat, uid, "", today, is_owner=False, person_id=person_id
                )
                prev_score = int(record["score"])
                if prev_score != 0:
                    return
                recovered = clamp_score(self.config.unmute_recovery_score, is_owner=False)
                record.update(
                    score=recovered,
                    last_reason="unmute_recovery",
                    last_delta=recovered,
                    updated_at=now,
                )
                await self._save_user_in_tx(plat, uid, record, person_id=person_id)
                if recovered != prev_score:
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=plat,
                        user_id=uid,
                        person_id=person_id,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=recovered,
                        kind="unmute_recovery",
                        reason="unmute_recovery",
                        now=now,
                    )
                self._update_caches(
                    plat, uid, recovered, is_owner=False, person_id=person_id, identity=identity
                )

    async def on_unmute(self, user_id: str, platform: str = "onebot") -> None:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        uid = str(user_id or "").strip()
        if not uid or self.is_owner_id(platform, uid) or self._is_known_bot(platform, uid):
            return
        try:
            uid = _validate_platform_user(platform, uid)
        except Rejected:
            return
        if identity is not None:
            pid = await identity.resolve_person_id(platform, uid, create=False)
            if pid is None:
                return
            await self.on_unmute_person(pid)
            return

        now = float(self.clock())
        today = self._today_str(now)
        user_key = self._legacy_user_key(platform, uid)
        async with self._lock:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=?", (user_key,)
                )
                if not row:
                    return
                opened = self.store.vault.open("affection:" + user_key, row["payload"])
                if not isinstance(opened, dict):
                    return
                record = self._normalize_record(opened, platform, uid, "", today)
                prev_score = int(record["score"])
                if prev_score != 0:
                    return
                recovered = clamp_score(self.config.unmute_recovery_score, is_owner=False)
                record.update(
                    score=recovered,
                    last_reason="unmute_recovery",
                    last_delta=recovered,
                    updated_at=now,
                )
                await self._save_user_in_tx(platform, uid, record)
                if recovered != prev_score:
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=None,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=recovered,
                        kind="unmute_recovery",
                        reason="unmute_recovery",
                        now=now,
                    )
                self._update_caches(platform, uid, recovered, is_owner=False)

    async def set_user_score(
        self,
        user_id: str,
        score: int,
        *,
        platform: str = "onebot",
        nickname: str = "",
        reason: str = "admin_set",
        moderation: Any = None,
    ) -> dict[str, Any]:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        uid = _validate_platform_user(platform, user_id)
        if uid == self._bot_id(platform):
            raise Rejected("protected_identity")
        if isinstance(score, bool) or not isinstance(score, int):
            raise Rejected("invalid_affection_score")

        pid = (
            await identity.resolve_person_id(platform, uid, nickname, create=True, source="auto")
            if identity is not None
            else None
        )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        target_score = clamp_score(score, is_owner=is_owner)
        now = float(self.clock())
        today = self._today_str(now)
        clean_reason = _sanitize_reason(reason, self.s, default="admin_set")

        async with self._lock:
            async with self.store.transaction():
                record, _ = await self._load_user_unlocked(
                    platform, uid, nickname, today, person_id=pid
                )
                prev_score = int(record["score"])
                actual_delta = target_score - prev_score
                updates: dict[str, Any] = {
                    "score": target_score,
                    "last_reason": clean_reason,
                    "last_delta": actual_delta,
                    "updated_at": now,
                }
                if actual_delta < 0:
                    updates["last_negative_at"] = now
                    updates["last_recovery_day"] = ""
                record.update(updates)
                await self._save_user_in_tx(platform, uid, record, person_id=pid)
                if actual_delta != 0:
                    user_key = (
                        self._person_key(pid)
                        if pid is not None
                        else self._legacy_user_key(platform, uid)
                    )
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=target_score,
                        kind="admin",
                        reason=clean_reason,
                        now=now,
                    )
                self._update_caches(
                    platform,
                    uid,
                    target_score,
                    is_owner=is_owner,
                    person_id=pid,
                    identity=identity,
                )

        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        if not is_owner:
            if identity is not None and pid is not None:
                if target_score == 0:
                    await identity.set_person_muted(
                        pid, True, moderation=mod, source="ai_affection_zero"
                    )
                elif target_score > 0 and (
                    identity.is_muted(platform, uid)
                    or (platform == "onebot" and uid in set(self.s.muted_user_ids))
                ):
                    await identity.set_person_muted(pid, False, moderation=mod)
            elif platform == "onebot" and mod is not None:
                if target_score == 0:
                    await self._maybe_mute_zero(platform, uid, record["nickname"], moderation=mod)
                elif target_score > 0 and uid in set(self.s.muted_user_ids):
                    if hasattr(mod, "set_muted"):
                        await mod.set_muted(uid, False)

        return self._build_profile_dict(record, person_id=pid)

    async def delete_user_score(
        self,
        user_id: str,
        *,
        platform: str = "onebot",
        moderation: Any = None,
    ) -> bool:
        await self.initialize()
        identity = await self._ensure_identity_ready()
        uid = _validate_platform_user(platform, user_id)
        pid = (
            await identity.resolve_person_id(platform, uid, create=False)
            if identity is not None
            else None
        )
        if identity is not None and pid is None:
            return False
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)

        now = float(self.clock())
        async with self._lock:
            async with self.store.transaction():
                existing_row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                    (user_key, self.scope),
                )
                cur = await self.store.db.execute(
                    "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                    (user_key, self.scope),
                )
                deleted = cur.rowcount > 0
                if deleted and existing_row:
                    opened = self.store.vault.open("affection:" + user_key, existing_row["payload"])
                    if isinstance(opened, dict):
                        prev_score = clamp_score(
                            int(
                                opened.get(
                                    "score",
                                    120 if is_owner else self.config.initial_score,
                                )
                            ),
                            is_owner=is_owner,
                        )
                        reset_score = 120 if is_owner else self.config.initial_score
                        if prev_score != reset_score:
                            await self._append_history_in_tx(
                                user_key=user_key,
                                platform=str(opened.get("platform", platform)),
                                user_id=str(opened.get("user_id", uid)),
                                person_id=pid,
                                nickname=str(opened.get("nickname", "")),
                                prev_score=prev_score,
                                new_score=reset_score,
                                kind="admin_reset",
                                reason="admin_reset",
                                now=now,
                            )
            self._annoyance_at.pop(user_key, None)
            if pid is not None:
                self._person_scores.pop(pid, None)
            if is_owner:
                self._update_caches(
                    platform,
                    uid,
                    120,
                    is_owner=True,
                    person_id=pid,
                    identity=identity,
                )
            else:
                self._user_scores.pop(user_key, None)
                if pid is not None and identity is not None:
                    for plat, acc_uid in identity.linked_accounts_cached(pid):
                        self._subject_multipliers.pop(self._subject(plat, acc_uid), None)
                else:
                    self._subject_multipliers.pop(self._subject(platform, uid), None)

        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        if not is_owner:
            if identity is not None and pid is not None:
                aff_muted = False
                if mod is not None and callable(getattr(mod, "is_affection_muted", None)):
                    for plat, acc_uid in identity.linked_accounts_cached(pid):
                        if plat == "onebot" and await mod.is_affection_muted(acc_uid):
                            aff_muted = True
                            break
                if not aff_muted and hasattr(identity, "person_mute_source"):
                    aff_muted = (await identity.person_mute_source(pid)) == "ai_affection_zero"
                if aff_muted:
                    await identity.set_person_muted(pid, False, moderation=mod)
            elif platform == "onebot" and mod is not None:
                is_aff_muted = getattr(mod, "is_affection_muted", None)
                if callable(is_aff_muted) and await is_aff_muted(uid):
                    await mod.set_muted(uid, False)
        return deleted

    async def merge_person_scores(
        self,
        source_person_id: str,
        target_person_id: str,
        *,
        force_zero: bool = False,
    ) -> int:
        await self.initialize()
        identity = self._get_identity()
        is_owner = target_person_id == "owner"
        src_key = self._person_key(source_person_id)
        dst_key = self._person_key(target_person_id)
        now = float(self.clock())
        today = self._today_str(now)
        merged_score: int | None = None

        async with self._lock:
            async with self.store.transaction():
                src_row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                    (src_key, self.scope),
                )
                dst_row = await self.store.one(
                    "SELECT payload FROM user_affection WHERE user_key=? AND scope=?",
                    (dst_key, self.scope),
                )
                src_data = (
                    self.store.vault.open("affection:" + src_key, src_row["payload"])
                    if src_row
                    else None
                )
                dst_data = (
                    self.store.vault.open("affection:" + dst_key, dst_row["payload"])
                    if dst_row
                    else None
                )
                existing = [d for d in (dst_data, src_data) if isinstance(d, dict) and "score" in d]

                prev_for_merge = (
                    clamp_score(int(dst_data["score"]), is_owner=is_owner)
                    if isinstance(dst_data, dict) and "score" in dst_data
                    else (
                        clamp_score(int(src_data["score"]), is_owner=is_owner)
                        if isinstance(src_data, dict) and "score" in src_data
                        else (120 if is_owner else self.config.initial_score)
                    )
                )

                if is_owner:
                    raw_scores = [clamp_score(int(d["score"]), is_owner=True) for d in existing]
                    merged_score = clamp_score(
                        max(raw_scores) if raw_scores else 120, is_owner=True
                    )
                elif force_zero:
                    merged_score = 0
                elif existing:
                    raw_scores = [clamp_score(int(d["score"]), is_owner=False) for d in existing]
                    merged_score = clamp_score(min(raw_scores), is_owner=False)

                await self._migrate_history_key_in_tx(src_key, dst_key, person_id=target_person_id)

                if merged_score is not None:
                    base = (
                        dst_data
                        if isinstance(dst_data, dict)
                        else (src_data if isinstance(src_data, dict) else {})
                    )
                    rep_plat = str(base.get("platform", ""))
                    rep_uid = str(base.get("user_id", ""))
                    if (not rep_plat or not rep_uid) and identity is not None:
                        linked = identity.linked_accounts_cached(target_person_id)
                        if linked:
                            rep_plat, rep_uid = linked[0]
                    if not rep_plat:
                        rep_plat = "onebot"
                    if not rep_uid:
                        rep_uid = "0"
                    last_negative = max(
                        (float(d.get("last_negative_at", 0.0) or 0.0) for d in existing),
                        default=0.0,
                    )
                    last_recovery_day = max(
                        (str(d.get("last_recovery_day", "") or "") for d in existing),
                        default="",
                    )
                    merge_reason = (
                        "merged_blacklist" if force_zero else str(base.get("last_reason", "merged"))
                    )
                    record = self._normalize_record(
                        {
                            **base,
                            "score": merged_score,
                            "last_negative_at": last_negative,
                            "last_recovery_day": last_recovery_day,
                            "last_reason": merge_reason,
                            "updated_at": now,
                        },
                        rep_plat,
                        rep_uid,
                        "",
                        today,
                        is_owner=is_owner,
                        person_id=target_person_id,
                    )
                    await self._save_user_in_tx(
                        rep_plat, rep_uid, record, person_id=target_person_id
                    )
                    if merged_score != prev_for_merge:
                        await self._append_history_in_tx(
                            user_key=dst_key,
                            platform=rep_plat,
                            user_id=rep_uid,
                            person_id=target_person_id,
                            nickname=record["nickname"],
                            prev_score=prev_for_merge,
                            new_score=merged_score,
                            kind="merge",
                            reason="merged_blacklist" if force_zero else "merged",
                            now=now,
                        )
                    self._update_caches(
                        rep_plat,
                        rep_uid,
                        merged_score,
                        is_owner=is_owner,
                        person_id=target_person_id,
                        identity=identity,
                    )

                if src_row:
                    await self.store.db.execute(
                        "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                        (src_key, self.scope),
                    )
                await self.store.db.execute(
                    "UPDATE affection_events SET user_key=? WHERE scope=? AND user_key=?",
                    (dst_key, self.scope, src_key),
                )

            self._user_scores.pop(src_key, None)
            self._person_scores.pop(source_person_id, None)
            self._annoyance_at.pop(src_key, None)

        return (
            merged_score
            if merged_score is not None
            else (120 if is_owner else self.config.initial_score)
        )

    async def set_config(self, value: AffectionConfig | None) -> dict[str, Any]:
        await self.initialize()
        if value is not None and not isinstance(value, AffectionConfig):
            raise ValueError("Expected a validated AffectionConfig")
        effective = self.env_config if value is None else value
        async with self._lock:
            async with self.store.transaction():
                if value is None:
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='affection'")
                else:
                    await self.store.db.execute(
                        "INSERT INTO admin_config VALUES ('affection',?) ON CONFLICT(key) "
                        "DO UPDATE SET payload=excluded.payload",
                        (self.store.vault.seal("admin:affection", value.model_dump()),),
                    )
            self.config = effective
            self.overridden = value is not None
        return await self.status()

    async def run_cooldown_recovery(self, *, now: float | None = None) -> int:
        await self.initialize()
        if not self.config.enabled or self.config.cooldown_recovery_days <= 0:
            return 0
        identity = await self._ensure_identity_ready()
        target = int(self.config.cooldown_recovery_target)
        days_sec = self.config.cooldown_recovery_days * 86400.0
        ref_now = float(self.clock() if now is None else now)
        today = self._today_str(ref_now)
        muted_uids = set(self.s.muted_user_ids)
        recovered_count = 0

        async with self._lock:
            async with self.store.transaction():
                rows = await self.store.all(
                    "SELECT user_key, payload FROM user_affection WHERE scope=?",
                    (self.scope,),
                )
                for row in rows:
                    user_key = str(row["user_key"])
                    opened = self.store.vault.open("affection:" + user_key, row["payload"])
                    if not isinstance(opened, dict):
                        continue
                    plat = str(opened.get("platform", "onebot"))
                    uid = str(opened.get("user_id", ""))
                    if not uid:
                        continue
                    pid = str(opened["person_id"]) if opened.get("person_id") else None
                    is_owner = (pid == "owner") or self.is_owner_id(plat, uid)
                    if is_owner or self._is_known_bot(plat, uid) or uid == self._bot_id(plat):
                        continue
                    record = self._normalize_record(
                        opened, plat, uid, "", today, is_owner=False, person_id=pid
                    )
                    score = int(record["score"])
                    if score == 0 or score >= target:
                        continue
                    is_muted = (plat == "onebot" and uid in muted_uids) or (
                        identity is not None
                        and (
                            identity.is_muted(plat, uid)
                            or (
                                pid is not None
                                and any(
                                    (p == "onebot" and u in muted_uids) or identity.is_muted(p, u)
                                    for p, u in identity.linked_accounts_cached(pid)
                                )
                            )
                        )
                    )
                    if is_muted:
                        continue

                    last_neg = float(record.get("last_negative_at", 0.0) or 0.0)
                    last_rec_day = str(record.get("last_recovery_day", "") or "")
                    updated_at = float(record.get("updated_at", 0.0) or 0.0)
                    if last_rec_day == today:
                        continue
                    if last_neg > 0:
                        if (ref_now - last_neg) < days_sec:
                            continue
                    elif last_rec_day:
                        pass
                    elif updated_at <= 0 or (ref_now - updated_at) < days_sec:
                        continue

                    new_score = min(target, score + 1)
                    delta = new_score - score
                    if delta <= 0:
                        continue
                    record.update(
                        score=new_score,
                        last_recovery_day=today,
                        last_reason="cooldown_recovery",
                        last_delta=delta,
                        updated_at=ref_now,
                    )
                    await self._save_user_in_tx(plat, uid, record, person_id=pid)
                    saved_key = (
                        self._person_key(pid)
                        if pid is not None
                        else self._legacy_user_key(plat, uid)
                    )
                    await self._append_history_in_tx(
                        user_key=saved_key,
                        platform=plat,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=score,
                        new_score=new_score,
                        kind="cooldown_recovery",
                        reason="cooldown_recovery",
                        now=ref_now,
                    )
                    self._update_caches(
                        plat,
                        uid,
                        new_score,
                        is_owner=False,
                        person_id=pid,
                        identity=identity,
                    )
                    recovered_count += 1
        return recovered_count

    @staticmethod
    def _clamp_history_limit(limit: int, *, default: int) -> int:
        if isinstance(limit, bool) or not isinstance(limit, int):
            return default
        return max(1, min(200, limit))

    def _open_history_row(self, row: Any) -> dict[str, Any] | None:
        user_key = str(row["user_key"])
        opened = self.store.vault.open("affection-hist:" + user_key, row["payload"])
        if not isinstance(opened, dict):
            return None
        return {
            "id": int(row["id"]),
            "user_id": str(opened.get("user_id", "")),
            "platform": str(opened.get("platform", "")),
            "person_id": str(opened["person_id"]) if opened.get("person_id") else None,
            "nickname": str(opened.get("nickname", "")),
            "prev_score": int(opened.get("prev_score", 0)),
            "new_score": int(opened.get("new_score", 0)),
            "delta": int(opened.get("delta", row["delta"])),
            "kind": str(opened.get("kind", "")),
            "reason": str(opened.get("reason", "")),
            "created_at": float(row["created"]),
        }

    async def user_history(
        self,
        platform: str,
        user_id: str,
        *,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        uid = _validate_platform_user(platform, user_id)
        await self.initialize()
        identity = await self._ensure_identity_ready()
        clamped_limit = self._clamp_history_limit(limit, default=30)
        pid = (
            await identity.resolve_person_id(platform, uid, create=False)
            if identity is not None
            else None
        )
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id, user_key, delta, payload, created FROM affection_history "
                "WHERE scope=? AND user_key=? ORDER BY created DESC, id DESC LIMIT ?",
                (self.scope, user_key, clamped_limit),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._open_history_row(row)
            if item is not None:
                items.append(item)
        return items

    async def recent_history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        await self.initialize()
        await self._ensure_identity_ready()
        clamped_limit = self._clamp_history_limit(limit, default=50)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id, user_key, delta, payload, created FROM affection_history "
                "WHERE scope=? ORDER BY created DESC, id DESC LIMIT ?",
                (self.scope, clamped_limit),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._open_history_row(row)
            if item is not None:
                items.append(item)
        return items

    async def list_users(self) -> list[dict[str, Any]]:
        await self.initialize()
        await self._ensure_identity_ready()
        today = self._today_str()
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT user_key, payload FROM user_affection WHERE scope=? "
                "ORDER BY updated DESC LIMIT 500",
                (self.scope,),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            opened = self.store.vault.open("affection:" + row["user_key"], row["payload"])
            if not isinstance(opened, dict):
                continue
            platform = str(opened.get("platform", "onebot"))
            uid = str(opened.get("user_id", ""))
            pid = str(opened["person_id"]) if opened.get("person_id") else None
            if not uid:
                continue
            is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
            record = self._normalize_record(
                opened, platform, uid, "", today, is_owner=is_owner, person_id=pid
            )
            items.append(self._build_profile_dict(record, person_id=pid))
        items.sort(
            key=lambda item: (
                0 if item["is_owner"] else 1,
                -float(item.get("updated_at", 0.0)),
            )
        )
        return items

    async def status(self) -> dict[str, Any]:
        await self.initialize()
        return {
            "config": self.config.model_dump(mode="json"),
            "environment": self.env_config.model_dump(mode="json"),
            "source": "admin" if self.overridden else "env",
            "tiers": tier_matrix_rows(),
            "users": await self.list_users(),
        }
