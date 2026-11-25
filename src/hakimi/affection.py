"""Encrypted per-user affection scoring, 7-tier capability gating, and violation penalties."""

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .affection_rules import (
    ANNOYANCE_COOLDOWN_SECONDS,
    ANNOYANCE_DELTA,
    DEFAULT_FAREWELL,
    GENERIC_USER_RE,
    ONEBOT_USER_RE,
    PLATFORM_ID_RE,
    QQ_USER_RE,
    SCHEMA,
    TIER_CAPS,
    TIER_SPECS,
    VIOLATION_DELTAS,
    AffectionCapabilities,
    AffectionConfig,
    AffectionUpdate,
    _neutral_unlocked_caps,
    _sanitize_nickname,
    _sanitize_reason,
    _validate_platform_user,
    capabilities_for_score,
    clamp_score,
    level_for_score,
    tier_matrix_rows,
)
from .affection_storage import _AffectionStorageMixin
from .config import Settings
from .models import Message
from .security import Rejected

__all__ = [
    "ANNOYANCE_COOLDOWN_SECONDS",
    "ANNOYANCE_DELTA",
    "DEFAULT_FAREWELL",
    "GENERIC_USER_RE",
    "ONEBOT_USER_RE",
    "PLATFORM_ID_RE",
    "QQ_USER_RE",
    "SCHEMA",
    "TIER_CAPS",
    "TIER_SPECS",
    "VIOLATION_DELTAS",
    "AffectionCapabilities",
    "AffectionConfig",
    "AffectionManager",
    "AffectionUpdate",
    "_neutral_unlocked_caps",
    "_sanitize_nickname",
    "_sanitize_reason",
    "_validate_platform_user",
    "capabilities_for_score",
    "clamp_score",
    "level_for_score",
    "tier_matrix_rows",
]


class AffectionManager(_AffectionStorageMixin):
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
        now = float(self.clock())
        today = self._today_str(now)
        is_owner = (pid == "owner") or self.is_owner_id(platform, uid)
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
        violations_30d = 0
        async with self.store.lock:
            record, _ = await self._load_user_unlocked(
                platform, uid, nickname, today, person_id=pid
            )
            if not is_owner:
                row = await self.store.one(
                    "SELECT COUNT(*) AS cnt FROM affection_events "
                    "WHERE scope=? AND user_key=? AND created>=?",
                    (self.scope, user_key, now - 30 * 86400),
                )
                violations_30d = int(row["cnt"]) if row else 0
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
        return self._build_profile_dict(record, person_id=pid, violations_30d=violations_30d)

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

        if is_owner and self.config.decouple_safety_violations:
            now = float(self.clock())
            today = self._today_str(now)
            async with self.store.lock:
                record, _ = await self._load_user_unlocked(
                    platform, uid, message.sender_nickname, today, person_id=pid
                )
            prev_score = int(record["score"])
            caps = self._update_caches(
                platform,
                uid,
                prev_score,
                is_owner=True,
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
                is_owner=True,
                changed=False,
                blacklisted_now=False,
                reason=f"{kind}_owner_immune",
                today_positive_gain=int(record["today_positive_gain"]),
            )

        now = float(self.clock())
        today = self._today_str(now)
        if self.config.decouple_safety_violations:
            raw_delta = (
                self.config.violation_injection_delta
                if kind == "prompt_injection"
                else self.config.violation_content_delta
            )
        else:
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
                if is_owner:
                    new_score = clamp_score(prev_score + raw_delta, is_owner=True)
                elif self.config.decouple_safety_violations:
                    floor = int(self.config.violation_affection_floor)
                    if prev_score <= floor:
                        new_score = prev_score
                    else:
                        new_score = max(floor, clamp_score(prev_score + raw_delta, is_owner=False))
                else:
                    new_score = clamp_score(prev_score + raw_delta, is_owner=False)
                actual_delta = new_score - prev_score
                updates: dict[str, Any] = {
                    "score": new_score,
                    "safety_strikes_total": int(record.get("safety_strikes_total", 0) or 0) + 1,
                    "last_violation_at": now,
                    "last_negative_at": now,
                    "last_recovery_day": "",
                    "updated_at": now,
                }
                if actual_delta != 0:
                    updates["last_reason"] = kind
                    updates["last_delta"] = actual_delta
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
                elif self.config.decouple_safety_violations and not is_owner:
                    await self._append_history_in_tx(
                        user_key=user_key,
                        platform=platform,
                        user_id=uid,
                        person_id=pid,
                        nickname=record["nickname"],
                        prev_score=prev_score,
                        new_score=new_score,
                        kind="violation",
                        reason=f"{kind}_floored",
                        now=now,
                        allow_zero=True,
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

        violations_30d = 0
        user_key = (
            self._person_key(pid) if pid is not None else self._legacy_user_key(platform, uid)
        )
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
                if not is_owner:
                    ev_row = await self.store.one(
                        "SELECT COUNT(*) AS cnt FROM affection_events "
                        "WHERE scope=? AND user_key=? AND created>=?",
                        (self.scope, user_key, now - 30 * 86400),
                    )
                    violations_30d = int(ev_row["cnt"]) if ev_row else 0
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

        return self._build_profile_dict(record, person_id=pid, violations_30d=violations_30d)

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
