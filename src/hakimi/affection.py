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
"""

ONEBOT_USER_RE = re.compile(r"^[1-9][0-9]{0,19}$")
QQ_USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
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
    if platform not in {"onebot", "qq"}:
        raise Rejected("invalid_platform")
    if isinstance(user_id, bool) or not isinstance(user_id, str):
        raise Rejected("invalid_user_id")
    uid = user_id.strip()
    pattern = ONEBOT_USER_RE if platform == "onebot" else QQ_USER_RE
    if not pattern.fullmatch(uid):
        raise Rejected("invalid_user_id")
    return uid


class AffectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: StrictBool = True
    initial_score: int = Field(default=50, ge=1, le=100)
    daily_positive_cap: int = Field(default=6, ge=0, le=50)
    unmute_recovery_score: int = Field(default=25, ge=1, le=60)
    farewell_message: str = Field(default=DEFAULT_FAREWELL, min_length=1, max_length=120)

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
    ),
    AffectionCapabilities(
        level=1,
        tier="hostile",
        tier_label="厌烦防备",
        attitude="厌烦防备：懒得搭理，语气冷淡敷衍，不给额外工具与表情互动。",
        direct_reply_prob=0.25,
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
        budget_multiplier=Decimal("0.20"),
    ),
    AffectionCapabilities(
        level=2,
        tier="cold",
        tier_label="冷淡疏离",
        attitude="冷淡疏离：保持礼貌距离，简短接话，限低清看图且不开高级功能。",
        direct_reply_prob=0.65,
        allow_poke_inbound=False,
        reply_threshold_delta=0.22,
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
        budget_multiplier=Decimal("0.50"),
    ),
    AffectionCapabilities(
        level=3,
        tier="normal",
        tier_label="普通群友",
        attitude="普通群友：松弛平视、正常接梗，可使用表情包、贴表情与 B 站摘要。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=0.0,
        max_images=2,
        low_res_images_only=False,
        allow_sticker=True,
        allow_emoji_like=True,
        allow_poke=False,
        allow_bili_summary=True,
        allow_search=False,
        allow_file_read=False,
        allow_text_card=False,
        allow_dual_profile=False,
        allow_video_watch=False,
        allow_carbon=False,
        max_tier=3,
        budget_multiplier=Decimal("1.00"),
    ),
    AffectionCapabilities(
        level=4,
        tier="familiar",
        tier_label="投缘熟人",
        attitude="投缘熟人：熟络热情、主动搭话，解锁回戳、联网检索、读文件与长卡片。",
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
        budget_multiplier=Decimal("1.00"),
    ),
    AffectionCapabilities(
        level=5,
        tier="bestie",
        tier_label="死党铁磁",
        attitude="死党铁磁：高度默契护短、随意开玩笑，全功能与 Carbon 深度推理全开。",
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
        allow_carbon=True,
        max_tier=4,
        budget_multiplier=Decimal("1.50"),
    ),
    AffectionCapabilities(
        level=6,
        tier="owner_bond",
        tier_label="专属羁绊 (Owner)",
        attitude="专属羁绊：全天下唯一的 BOISER，亲昵偏爱且永远拥有 100% 完整权限。",
        direct_reply_prob=1.0,
        allow_poke_inbound=True,
        reply_threshold_delta=-0.15,
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
        budget_multiplier=Decimal("2.00"),
    ),
)

TIER_SPECS: tuple[dict[str, Any], ...] = (
    {
        "min_score": 0,
        "max_score": 0,
        "score_range": "0",
        "owner_only": False,
        "summary": "彻底无视，禁止互动与消耗额度",
    },
    {
        "min_score": 1,
        "max_score": 19,
        "score_range": "1-19",
        "owner_only": False,
        "summary": "直接回复概率 25%，限 Tier 1，禁图与扩展工具",
    },
    {
        "min_score": 20,
        "max_score": 39,
        "score_range": "20-39",
        "owner_only": False,
        "summary": "直接回复概率 65%，限低清 1 图与 Tier 2",
    },
    {
        "min_score": 40,
        "max_score": 59,
        "score_range": "40-59",
        "owner_only": False,
        "summary": "全量常规聊天，解锁表情包、贴表情与 B 站摘要",
    },
    {
        "min_score": 60,
        "max_score": 79,
        "score_range": "60-79",
        "owner_only": False,
        "summary": "解锁回戳、联网搜索、文件读取、文字卡片与双轨画像",
    },
    {
        "min_score": 80,
        "max_score": 100,
        "score_range": "80-100",
        "owner_only": False,
        "summary": "解锁原片看视频、Carbon 深度推理与 1.5x 日限额",
    },
    {
        "min_score": 100,
        "max_score": 120,
        "score_range": "100-120",
        "owner_only": True,
        "summary": "Owner 独享下限 100 永不降级，全能力 100% 解锁",
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
    """Manages encrypted per-user affection scores, capabilities, and moderation sync."""

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
        self._user_scores: dict[str, tuple[int, bool]] = {}
        self._subject_multipliers: dict[str, Decimal] = {}
        self._annoyance_at: dict[str, float] = {}

    @property
    def scope(self) -> str:
        return self.store.vault.digest(
            "affection", self.s.namespace, self.s.onebot_self_id, self.s.qq_app_id
        )

    def _bot_id(self, platform: str) -> str:
        return self.s.onebot_self_id if platform == "onebot" else self.s.qq_app_id

    def _user_key(self, platform: str, user_id: str) -> str:
        return self.store.vault.digest("affection-user", self.scope, platform, user_id)

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
            return uid in owners
        if platform == "qq":
            return uid in (set(self.s.qq_owner_ids) - {""})
        return False

    def _is_known_bot(self, platform: str, user_id: str) -> bool:
        return platform == "onebot" and user_id in self.s.bot_qq_ids

    def _seed_owner_caches(self) -> None:
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

    def _update_caches(
        self,
        platform: str,
        user_id: str,
        score: int,
        *,
        is_owner: bool,
        bot_id: str | None = None,
    ) -> AffectionCapabilities:
        bounded = clamp_score(score, is_owner=is_owner)
        caps = capabilities_for_score(bounded, is_owner=is_owner)
        uk = self._user_key(platform, user_id)
        self._user_scores[uk] = (bounded, is_owner)
        self._subject_multipliers[self._subject(platform, user_id, bot_id)] = caps.budget_multiplier
        if bot_id is not None and bot_id != self._bot_id(platform):
            self._subject_multipliers[self._subject(platform, user_id)] = caps.budget_multiplier
        return caps

    async def initialize(self) -> None:
        async with self.init_lock:
            if self.initialized:
                return
            self.env_config = AffectionConfig.from_settings(self.s)
            async with self.store.transaction():
                await self.store.db.executescript(SCHEMA)
            async with self.store.lock:
                cfg_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='affection'"
                )
                user_rows = await self.store.all(
                    "SELECT user_key, subject, payload FROM user_affection WHERE scope=?",
                    (self.scope,),
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

            next_scores: dict[str, tuple[int, bool]] = {}
            next_mults: dict[str, Decimal] = {}
            for row in user_rows:
                data = self.store.vault.open("affection:" + row["user_key"], row["payload"])
                if not isinstance(data, dict):
                    continue
                platform = str(data.get("platform", "onebot"))
                uid = str(data.get("user_id", ""))
                is_owner = self.is_owner_id(platform, uid)
                default_score = 120 if is_owner else self.config.initial_score
                score = clamp_score(int(data.get("score", default_score)), is_owner=is_owner)
                caps = capabilities_for_score(score, is_owner=is_owner)
                next_scores[row["user_key"]] = (score, is_owner)
                next_mults[row["subject"]] = caps.budget_multiplier
                next_mults[self._subject(platform, uid)] = caps.budget_multiplier
            self._user_scores = next_scores
            self._subject_multipliers = next_mults
            self._seed_owner_caches()
            self.initialized = True

    def _default_record(
        self, platform: str, user_id: str, nickname: str, today: str
    ) -> dict[str, Any]:
        is_owner = self.is_owner_id(platform, user_id)
        score = 120 if is_owner else self.config.initial_score
        return {
            "user_id": user_id,
            "platform": platform,
            "nickname": _sanitize_nickname(nickname, self.s),
            "score": score,
            "daily_day": today,
            "today_positive_gain": 0,
            "daily_first_day": "",
            "last_annoyance_at": 0.0,
            "last_reason": "init",
            "last_delta": 0,
            "updated_at": 0.0,
        }

    def _normalize_record(
        self,
        data: dict[str, Any],
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
    ) -> dict[str, Any]:
        is_owner = self.is_owner_id(platform, user_id)
        default_score = 120 if is_owner else self.config.initial_score
        score = clamp_score(int(data.get("score", default_score)), is_owner=is_owner)
        same_day = data.get("daily_day") == today
        today_gain = max(0, int(data.get("today_positive_gain", 0))) if same_day else 0
        first_day = str(data.get("daily_first_day", "")) if same_day else ""
        clean_nick = (
            _sanitize_nickname(nickname, self.s)
            if nickname
            else _sanitize_nickname(data.get("nickname", ""), self.s)
        )
        return {
            "user_id": user_id,
            "platform": platform,
            "nickname": clean_nick,
            "score": score,
            "daily_day": today,
            "today_positive_gain": today_gain,
            "daily_first_day": first_day,
            "last_annoyance_at": float(data.get("last_annoyance_at", 0.0) or 0.0),
            "last_reason": _sanitize_reason(data.get("last_reason", "chat"), self.s),
            "last_delta": int(data.get("last_delta", 0) or 0),
            "updated_at": float(data.get("updated_at", 0.0) or 0.0),
        }

    async def _load_user_unlocked(
        self,
        platform: str,
        user_id: str,
        nickname: str,
        today: str,
    ) -> tuple[dict[str, Any], bool]:
        user_key = self._user_key(platform, user_id)
        row = await self.store.one(
            "SELECT payload FROM user_affection WHERE user_key=?", (user_key,)
        )
        if not row:
            return self._default_record(platform, user_id, nickname, today), False
        opened = self.store.vault.open("affection:" + user_key, row["payload"])
        if not isinstance(opened, dict):
            return self._default_record(platform, user_id, nickname, today), False
        return self._normalize_record(opened, platform, user_id, nickname, today), True

    async def _save_user_in_tx(
        self,
        platform: str,
        user_id: str,
        record: dict[str, Any],
        *,
        bot_id: str | None = None,
    ) -> None:
        user_key = self._user_key(platform, user_id)
        subject = self._subject(platform, user_id, bot_id)
        sealed = self.store.vault.seal("affection:" + user_key, record)
        await self.store.db.execute(
            "INSERT INTO user_affection(user_key,scope,subject,payload,updated) VALUES (?,?,?,?,?) "
            "ON CONFLICT(user_key) DO UPDATE SET scope=excluded.scope,subject=excluded.subject,"
            "payload=excluded.payload,updated=excluded.updated",
            (user_key, self.scope, subject, sealed, float(record["updated_at"])),
        )

    def _build_profile_dict(self, record: dict[str, Any]) -> dict[str, Any]:
        platform = str(record["platform"])
        uid = str(record["user_id"])
        is_owner = self.is_owner_id(platform, uid)
        score = clamp_score(int(record["score"]), is_owner=is_owner)
        caps = capabilities_for_score(score, is_owner=is_owner)
        muted = (not is_owner) and platform == "onebot" and uid in set(self.s.muted_user_ids)
        return {
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
            "muted": muted,
            "today_positive_gain": int(record["today_positive_gain"]),
            "daily_positive_cap": self.config.daily_positive_cap,
            "last_reason": record["last_reason"],
            "last_delta": int(record.get("last_delta", 0)),
            "updated_at": float(record.get("updated_at", 0.0)),
            "capabilities": caps.to_dict(),
        }

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
        today = self._today_str()
        async with self.store.lock:
            record, _ = await self._load_user_unlocked(platform, uid, nickname, today)
        is_owner = self.is_owner_id(platform, uid)
        self._update_caches(platform, uid, record["score"], is_owner=is_owner)
        return self._build_profile_dict(record)

    async def capabilities_for(self, message: Message) -> AffectionCapabilities:
        await self.initialize()
        if not self.config.enabled or message.platform not in {"onebot", "qq"}:
            is_owner = self.is_owner_id(message.platform, message.user_id)
            return _neutral_unlocked_caps(
                tier="owner_bond" if is_owner else "disabled",
                tier_label="专属羁绊 (Owner)" if is_owner else "功能未启用",
            )
        if self._is_known_bot(message.platform, message.user_id):
            return _neutral_unlocked_caps(tier="bot_bypass", tier_label="Bot 豁免")
        is_owner = self.is_owner_id(message.platform, message.user_id)
        uk = self._user_key(message.platform, message.user_id)
        cached = self._user_scores.get(uk)
        if cached is not None:
            return capabilities_for_score(cached[0], is_owner=is_owner)
        today = self._today_str()
        async with self.store.lock:
            record, _ = await self._load_user_unlocked(
                message.platform, message.user_id, message.sender_nickname, today
            )
        return self._update_caches(
            message.platform,
            message.user_id,
            record["score"],
            is_owner=is_owner,
            bot_id=message.bot_id,
        )

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
            return round(min(0.95, base + 0.22), 4)
        if lvl == 3:
            return round(base, 4)
        if lvl == 4:
            return round(max(0.15, base - 0.08), 4)
        if lvl == 5:
            return round(max(0.12, base - 0.14), 4)
        return round(max(0.10, base - 0.15), 4)

    async def _maybe_mute_zero(
        self, platform: str, user_id: str, nickname: str, moderation: Any = None
    ) -> None:
        if platform != "onebot" or self.is_owner_id(platform, user_id):
            return
        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
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
        platform, uid = message.platform, message.user_id
        is_owner = self.is_owner_id(platform, uid)
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
                    platform, uid, message.sender_nickname, today
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
                    record.update(
                        score=new_score,
                        daily_day=today,
                        today_positive_gain=new_today_gain,
                        daily_first_day=next_first_day,
                        last_reason=clean_reason
                        if (changed or not existed)
                        else record["last_reason"],
                        last_delta=actual_delta if changed else record.get("last_delta", 0),
                        updated_at=now,
                    )
                    await self._save_user_in_tx(platform, uid, record, bot_id=message.bot_id)
                caps = self._update_caches(
                    platform, uid, new_score, is_owner=is_owner, bot_id=message.bot_id
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"])
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
        platform, uid = message.platform, message.user_id
        is_owner = self.is_owner_id(platform, uid)
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
        user_key = self._user_key(platform, uid)
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
                    platform, uid, message.sender_nickname, today
                )
                prev_score = int(record["score"])
                if cur.rowcount != 1:
                    caps = self._update_caches(
                        platform, uid, prev_score, is_owner=is_owner, bot_id=message.bot_id
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
                record.update(
                    score=new_score,
                    last_reason=kind,
                    last_delta=actual_delta,
                    updated_at=now,
                )
                await self._save_user_in_tx(platform, uid, record, bot_id=message.bot_id)
                caps = self._update_caches(
                    platform, uid, new_score, is_owner=is_owner, bot_id=message.bot_id
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"])
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
        platform, uid = message.platform, message.user_id
        is_owner = self.is_owner_id(platform, uid)
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
        user_key = self._user_key(platform, uid)
        blacklisted_now = False

        async with self._lock:
            async with self.store.transaction():
                record, _ = await self._load_user_unlocked(
                    platform, uid, message.sender_nickname, today
                )
                prev_score = int(record["score"])
                last_at = max(
                    float(record.get("last_annoyance_at", 0.0) or 0.0),
                    self._annoyance_at.get(user_key, 0.0),
                )
                if now - last_at < ANNOYANCE_COOLDOWN_SECONDS:
                    caps = self._update_caches(
                        platform, uid, prev_score, is_owner=is_owner, bot_id=message.bot_id
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
                record.update(
                    score=new_score,
                    last_annoyance_at=now,
                    last_reason=clean_reason,
                    last_delta=actual_delta,
                    updated_at=now,
                )
                await self._save_user_in_tx(platform, uid, record, bot_id=message.bot_id)
                caps = self._update_caches(
                    platform, uid, new_score, is_owner=is_owner, bot_id=message.bot_id
                )

        blacklisted_now = (not is_owner) and prev_score > 0 and new_score == 0
        if blacklisted_now:
            await self._maybe_mute_zero(platform, uid, record["nickname"])
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

    async def on_unmute(self, user_id: str, platform: str = "onebot") -> None:
        await self.initialize()
        uid = str(user_id or "").strip()
        if not uid or self.is_owner_id(platform, uid) or self._is_known_bot(platform, uid):
            return
        try:
            uid = _validate_platform_user(platform, uid)
        except Rejected:
            return
        now = float(self.clock())
        today = self._today_str(now)
        user_key = self._user_key(platform, uid)
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
                if record["score"] != 0:
                    return
                recovered = clamp_score(self.config.unmute_recovery_score, is_owner=False)
                record.update(
                    score=recovered,
                    last_reason="unmute_recovery",
                    last_delta=recovered,
                    updated_at=now,
                )
                await self._save_user_in_tx(platform, uid, record)
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
        uid = _validate_platform_user(platform, user_id)
        if uid == self._bot_id(platform):
            raise Rejected("protected_identity")
        if isinstance(score, bool) or not isinstance(score, int):
            raise Rejected("invalid_affection_score")
        is_owner = self.is_owner_id(platform, uid)
        target_score = clamp_score(score, is_owner=is_owner)
        now = float(self.clock())
        today = self._today_str(now)
        clean_reason = _sanitize_reason(reason, self.s, default="admin_set")

        async with self._lock:
            async with self.store.transaction():
                record, _ = await self._load_user_unlocked(platform, uid, nickname, today)
                prev_score = int(record["score"])
                record.update(
                    score=target_score,
                    last_reason=clean_reason,
                    last_delta=target_score - prev_score,
                    updated_at=now,
                )
                await self._save_user_in_tx(platform, uid, record)
                self._update_caches(platform, uid, target_score, is_owner=is_owner)

        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        if not is_owner and platform == "onebot" and mod is not None:
            if target_score == 0:
                await self._maybe_mute_zero(platform, uid, record["nickname"], moderation=mod)
            elif target_score > 0 and uid in set(self.s.muted_user_ids):
                if hasattr(mod, "set_muted"):
                    await mod.set_muted(uid, False)

        return self._build_profile_dict(record)

    async def delete_user_score(
        self,
        user_id: str,
        *,
        platform: str = "onebot",
        moderation: Any = None,
    ) -> bool:
        await self.initialize()
        uid = _validate_platform_user(platform, user_id)
        user_key = self._user_key(platform, uid)
        is_owner = self.is_owner_id(platform, uid)

        async with self._lock:
            async with self.store.transaction():
                cur = await self.store.db.execute(
                    "DELETE FROM user_affection WHERE user_key=? AND scope=?",
                    (user_key, self.scope),
                )
                deleted = cur.rowcount > 0
            self._annoyance_at.pop(user_key, None)
            if is_owner:
                self._update_caches(platform, uid, 120, is_owner=True)
            else:
                self._user_scores.pop(user_key, None)
                self._subject_multipliers.pop(self._subject(platform, uid), None)

        mod = (
            moderation
            if moderation is not None
            else getattr(self.store.runtime, "moderation", None)
        )
        if not is_owner and platform == "onebot" and mod is not None:
            is_aff_muted = getattr(mod, "is_affection_muted", None)
            if callable(is_aff_muted) and await is_aff_muted(uid):
                await mod.set_muted(uid, False)
        return deleted

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

    async def list_users(self) -> list[dict[str, Any]]:
        await self.initialize()
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
            if not uid:
                continue
            record = self._normalize_record(opened, platform, uid, "", today)
            items.append(self._build_profile_dict(record))
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
