"""Reverse Semantic Resonance Engine & Spontaneous Sharing Urge.

When Hakimi encounters a public experience (e.g. Bilibili video, Minecraft milestone),
this engine probes each eligible audience strictly ONE-WAY:
- Verified Owner target: probes Owner memory + Owner DM dual_memory + Owner cloud memory.
- Each active public group G: probes ONLY group G's own dual_memory — NEVER Owner memory
  and NEVER any other group's memory.

Matched resonance thoughts are Vault-sealed at rest (bound cryptographically to
``(target_scope, share_id)``) in ``pending_shares`` and either enqueued promptly on the
clean untainted ``origin="resonance"`` channel or kept warm for ``pop_due_shares`` /
``pending_for_context`` ("念念不忘"). Positive and negative audience replies drive the
"捧场正循环" engagement feedback loop in ``share_engagement``.
"""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import Message
from .modules.base import sanitize_sensor_text
from .security import Rejected, redact_secrets
from .social import (
    RecipientResolver,
    is_verified_owner_scope,
    owner_binding,
    owner_target,
    owner_target_scope,
)
from .social_speak import resolve_social_message
from .world_experience import ENGAGEMENT_SOCIAL_RECOVERY, SOCIAL_LOW
from .world_state import _check_no_prewritten_speech

log = logging.getLogger("hakimi")

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS pending_shares (
      id TEXT PRIMARY KEY,
      dedupe_key TEXT UNIQUE,
      target_scope TEXT NOT NULL,
      is_owner INTEGER NOT NULL DEFAULT 0 CHECK(is_owner IN (0,1)),
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending','delivered','expired','dismissed')),
      score REAL NOT NULL DEFAULT 0.0,
      novelty REAL NOT NULL DEFAULT 0.5,
      created_at REAL NOT NULL,
      expires_at REAL NOT NULL,
      delivered_at REAL,
      payload_blob BLOB NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pending_shares_scope_status
      ON pending_shares(target_scope, status, expires_at, score DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pending_shares_expires
      ON pending_shares(expires_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS share_engagement (
      target_scope TEXT PRIMARY KEY,
      last_shared_at REAL NOT NULL DEFAULT 0.0,
      last_engaged_at REAL NOT NULL DEFAULT 0.0,
      streak INTEGER NOT NULL DEFAULT 0,
      ignore_streak INTEGER NOT NULL DEFAULT 0,
      boost REAL NOT NULL DEFAULT 0.0,
      updated_at REAL NOT NULL DEFAULT 0.0
    )
    """,
)

_FALLBACK_INTENT_NOTE = "刚看到与近期聊过话题相关的新见闻，顺势自然分享一下"
_STRIP_NOTE_CHARS_RE = re.compile(r"[“”「」『』\"'\n\r\t]+")
_STRIP_PERSONA_RE = re.compile(r"(?:喵[～~]?|\[role=|\[cq:|</untrusted)", re.IGNORECASE)


@runtime_checkable
class EmbeddingClientProtocol(Protocol):
    async def embed_one(
        self,
        text: str,
        *,
        task_type: str = "RETRIEVAL_QUERY",
        budget_lane: str = "proactive",
        subject: str = "share_resonance",
        purpose: str = "share_resonance",
    ) -> bytes | None: ...


@runtime_checkable
class OwnerMemoryProtocol(Protocol):
    async def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class DualMemoryProtocol(Protocol):
    async def probe_resonance(
        self,
        message: Message,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class OwnerCloudMemoryProtocol(Protocol):
    def probe_resonance(
        self,
        query_text: str,
        query_vector: bytes | None = None,
        *,
        limit: int = 2,
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class ResonanceConfig:
    """Tunable thresholds, weights, cooldowns, and feedback loop bounds."""

    resonance_weight: float = 0.72
    novelty_weight: float = 0.28
    owner_threshold: float = 0.42
    group_threshold: float = 0.54
    share_ttl_seconds: float = 24 * 3600.0
    ticket_ttl_seconds: float = 3600.0
    owner_base_cooldown_seconds: float = 30 * 60.0
    group_base_cooldown_seconds: float = 45 * 60.0
    min_cooldown_factor: float = 0.65
    max_cooldown_factor: float = 2.5
    boost_positive_step: float = 0.05
    boost_negative_step: float = 0.06
    boost_max: float = 0.15
    boost_min: float = -0.20
    engagement_window_seconds: float = 15 * 60.0
    ignore_grace_seconds: float = 20 * 60.0
    engagement_battery_recovery: float = ENGAGEMENT_SOCIAL_RECOVERY
    max_probe_groups: int = 16
    max_memories_per_target: int = 2


def _zone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _supports_kwarg(fn: Any, name: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        or (
            p.name == name
            and p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        )
        for p in sig.parameters.values()
    )


def _clamp01(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if num != num:  # NaN
        return default
    return max(0.0, min(1.0, num))


def _seal_context(target_scope: str, share_id: str) -> str:
    return f"share-resonance:{target_scope}:{share_id}"


def _safe_intent_note(title: str, settings: Any) -> str:
    clean = sanitize_sensor_text(title, settings, max_tokens=24)
    clean = _STRIP_NOTE_CHARS_RE.sub(" ", clean)
    clean = _STRIP_PERSONA_RE.sub("", clean)
    clean = " ".join(clean.split()).strip(" :：-—")
    if clean:
        candidate = f"刚看到和之前聊过话题相关的新内容（{clean[:48]}），自然顺嘴分享一下"
        try:
            _check_no_prewritten_speech(candidate)
            return candidate
        except ValueError:
            pass
    return _FALLBACK_INTENT_NOTE


class ShareResonanceEngine:
    """Reverse semantic resonance prober, pending share vault, and engagement loop."""

    def __init__(
        self,
        service: Any,
        *,
        embedder: Any = None,
        owner_memory: Any = None,
        dual_memory: Any = None,
        owner_cloud_memory: Any = None,
        clock: Callable[[], float] | None = None,
        config: ResonanceConfig | None = None,
    ) -> None:
        self.service = service
        self.s = service.s
        self.store = service.store
        self._embedder = embedder
        self._owner_memory = owner_memory
        self._dual_memory = dual_memory
        self._owner_cloud_memory = owner_cloud_memory
        self.clock: Callable[[], float] = clock or getattr(
            getattr(service, "world_state", None), "clock", time.time
        )
        self.config = config or ResonanceConfig()
        self.ready = False
        self._init_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Create pending_shares and share_engagement tables in the main SQLite store."""
        async with self._init_lock:
            if self.ready:
                return
            async with self.store.transaction():
                for stmt in SCHEMA_STATEMENTS:
                    await self.store.db.execute(stmt)
            self.ready = True

    async def _ensure_ready(self) -> None:
        if not self.ready:
            await self.initialize()

    # ------------------------------------------------------------------
    # Duck-typed collaborator resolution
    # ------------------------------------------------------------------

    def _get_embedder(self) -> Any:
        if self._embedder is not None:
            return self._embedder
        emb = getattr(self.service, "embedder", None)
        if emb is not None:
            return emb
        ai = getattr(self.service, "ai", None)
        if ai is not None:
            return getattr(ai, "embeddings", None) or getattr(ai, "embedder", None)
        return None

    def _get_owner_memory(self) -> Any:
        if self._owner_memory is not None:
            return self._owner_memory
        om = getattr(self.service, "owner_memory", None)
        if om is not None:
            return om
        hub = getattr(self.service, "memory_hub", None)
        return getattr(hub, "owner_memory", None) if hub is not None else None

    def _get_dual_memory(self) -> Any:
        if self._dual_memory is not None:
            return self._dual_memory
        sessions = getattr(self.service, "sessions", None)
        return getattr(sessions, "dual", None) if sessions is not None else None

    def _get_owner_cloud_memory(self) -> Any:
        if self._owner_cloud_memory is not None:
            return self._owner_cloud_memory
        return getattr(self.service, "cloud_memory", None)

    # ------------------------------------------------------------------
    # Sanitization & embedding
    # ------------------------------------------------------------------

    def _sanitize_experience(
        self,
        *,
        source: str,
        event_type: str,
        title: str,
        summary: str,
        ref: str,
    ) -> dict[str, str]:
        def _clean(raw: Any, max_tokens: int) -> str:
            scrubbed = redact_secrets(raw, self.s, neutralize_role_tags=True)
            return sanitize_sensor_text(scrubbed, self.s, max_tokens=max_tokens)

        return {
            "source": _clean(source, 16) or "public",
            "event_type": _clean(event_type, 24) or "experience",
            "title": _clean(title, 48),
            "summary": _clean(summary, 140),
            "ref": _clean(ref, 32),
        }

    def _sanitize_memory_hit(self, raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        score = _clamp01(raw.get("score"), 0.0)
        if score <= 0.0:
            return None
        topic = sanitize_sensor_text(
            redact_secrets(raw.get("topic") or "", self.s, neutralize_role_tags=True),
            self.s,
            max_tokens=28,
        )
        text = sanitize_sensor_text(
            redact_secrets(raw.get("text") or "", self.s, neutralize_role_tags=True),
            self.s,
            max_tokens=90,
        )
        if not (topic or text):
            return None
        source = sanitize_sensor_text(
            redact_secrets(raw.get("source") or "", self.s, neutralize_role_tags=True),
            self.s,
            max_tokens=20,
        )
        raw_entities = raw.get("entities") or []
        entities: list[str] = []
        if isinstance(raw_entities, (list, tuple)):
            for item in raw_entities[:6]:
                ent = sanitize_sensor_text(
                    redact_secrets(item or "", self.s, neutralize_role_tags=True),
                    self.s,
                    max_tokens=16,
                )
                if ent and ent not in entities:
                    entities.append(ent)
        return {
            "id": sanitize_sensor_text(str(raw.get("id") or ""), self.s, max_tokens=24),
            "source": source,
            "topic": topic,
            "text": text,
            "score": round(score, 4),
            "entities": entities,
        }

    async def _embed_query(self, query_text: str) -> bytes | None:
        embedder = self._get_embedder()
        if embedder is None or not query_text:
            return None
        fn = getattr(embedder, "embed_one", None)
        if not callable(fn):
            return None
        subject = self.store.vault.digest("share-resonance-embed", self.s.namespace)
        kwargs: dict[str, Any] = {}
        for k, v in (
            ("task_type", "RETRIEVAL_QUERY"),
            ("budget_lane", "proactive"),
            ("subject", subject),
            ("purpose", "share_resonance"),
        ):
            if _supports_kwarg(fn, k):
                kwargs[k] = v
        try:
            res = fn(query_text, **kwargs)
            if inspect.isawaitable(res):
                res = await res
            return bytes(res) if isinstance(res, (bytes, bytearray, memoryview)) and res else None
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("event=share_resonance_embed_fallback", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Target discovery (strictly separated Owner vs Public Groups)
    # ------------------------------------------------------------------

    def _resolve_owner_target(self, now: float) -> tuple[str, Message] | None:
        modules = getattr(self.service, "modules", None)
        preferred = getattr(self.s, "owner_delivery_platform", "") or ""
        order: list[str] = []
        for plat in (preferred, "onebot", *(getattr(self.s, "social_owner_bindings", {}) or {})):
            if not plat or plat == "qq" or plat in order:
                continue
            order.append(plat)
        for plat in order:
            if modules is not None:
                mod = modules.chat_platforms.get(plat)
                if mod is None or mod.inactive_reason is not None:
                    continue
                if owner_binding(self.s, plat) is None:
                    continue
            scope = owner_target_scope(self.s, plat, include_inactive=(modules is None))
            if not scope or not is_verified_owner_scope(
                scope, self.s, include_inactive=(modules is None)
            ):
                continue
            msg = (
                resolve_social_message(
                    self.service, scope, event_id=f"res:probe:{int(now)}", created_at=now
                )
                if modules is not None
                else owner_target(self.s, plat, now, f"res:probe:{int(now)}")
            )
            if msg is not None and msg.kind == "private":
                return scope, msg
        return None

    def _resolve_group_targets(self, now: float) -> list[tuple[str, Message]]:
        modules = getattr(self.service, "modules", None)
        policies = getattr(self.s, "group_policies", {}) or {}
        scopes: list[str] = []
        if modules is not None:
            for mod in modules.active():
                for scope in mod.proactive_groups():
                    if scope and scope not in scopes:
                        scopes.append(scope)
        elif getattr(self.s, "onebot_enabled", False) and getattr(self.s, "onebot_self_id", ""):
            for gid in sorted(getattr(self.s, "onebot_allowed_groups", ()) or ()):
                scope = f"onebot:{gid}"
                if policies.get(scope, "at_only") != "muted":
                    scopes.append(scope)

        targets: list[tuple[str, Message]] = []
        for scope in scopes:
            if len(targets) >= self.config.max_probe_groups:
                break
            if is_verified_owner_scope(scope, self.s, include_inactive=True):
                continue
            if policies.get(scope, "at_only") == "muted":
                continue
            if modules is not None:
                msg = resolve_social_message(
                    self.service, scope, event_id=f"res:probe:{int(now)}", created_at=now
                )
            else:
                plat, sep, gid = scope.partition(":")
                if not sep or not gid:
                    continue
                bot_id = self.s.onebot_self_id if plat == "onebot" else ""
                if not bot_id:
                    continue
                msg = Message(
                    platform=plat,
                    bot_id=bot_id,
                    event_id=f"res:probe:{int(now)}",
                    user_id="0",
                    conversation_id=gid,
                    kind="group",
                    text="",
                    addressed=True,
                    created_at=now,
                )
            if msg is not None and msg.kind == "group":
                targets.append((scope, msg))
        return targets

    # ------------------------------------------------------------------
    # Strictly one-way probing helpers
    # ------------------------------------------------------------------

    async def _call_probe(
        self,
        obj: Any,
        *args: Any,
        limit: int,
        now: float,
        message: Message | None = None,
    ) -> list[dict[str, Any]]:
        if obj is None:
            return []
        fn = getattr(obj, "probe_resonance", None)
        if not callable(fn):
            return []
        kwargs: dict[str, Any] = {}
        if _supports_kwarg(fn, "limit"):
            kwargs["limit"] = limit
        if _supports_kwarg(fn, "now"):
            kwargs["now"] = now
        if message is not None and _supports_kwarg(fn, "message"):
            kwargs["message"] = message
        try:
            if inspect.iscoroutinefunction(fn):
                res = await fn(*args, **kwargs)
            else:
                res = await asyncio.to_thread(fn, *args, **kwargs)
                if inspect.isawaitable(res):
                    res = await res
            return list(res) if isinstance(res, (list, tuple)) else []
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("event=share_resonance_probe_failed", exc_info=True)
            return []

    def _merge_hits(self, raw_hits: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        cleaned: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in raw_hits:
            item = self._sanitize_memory_hit(raw)
            if item is None:
                continue
            dedupe = item["id"] or f"{item['topic']}|{item['text'][:64]}"
            if dedupe in seen:
                continue
            seen.add(dedupe)
            cleaned.append(item)
        cleaned.sort(key=lambda item: float(item["score"]), reverse=True)
        return cleaned[: max(1, limit)]

    async def _probe_owner_target(
        self,
        owner_scope: str,
        owner_msg: Message,
        query_text: str,
        query_vector: bytes | None,
        now: float,
    ) -> list[dict[str, Any]]:
        """Probe ONLY verified Owner stores (owner_memory, owner DM dual_memory, cloud_memory)."""
        if owner_msg.kind != "private" or not is_verified_owner_scope(
            owner_scope, self.s, include_inactive=(getattr(self.service, "modules", None) is None)
        ):
            return []
        limit = self.config.max_memories_per_target
        owner_mem = self._get_owner_memory()
        dual_mem = self._get_dual_memory()
        cloud_mem = self._get_owner_cloud_memory()

        results = await asyncio.gather(
            self._call_probe(owner_mem, query_text, query_vector, limit=limit, now=now),
            self._call_probe(dual_mem, owner_msg, query_text, query_vector, limit=limit, now=now),
            self._call_probe(
                cloud_mem, query_text, query_vector, limit=limit, now=now, message=owner_msg
            ),
        )
        combined: list[dict[str, Any]] = []
        for batch in results:
            combined.extend(batch)
        return self._merge_hits(combined, limit)

    async def _probe_group_target(
        self,
        group_scope: str,
        group_msg: Message,
        query_text: str,
        query_vector: bytes | None,
        now: float,
    ) -> list[dict[str, Any]]:
        """Probe STRICTLY ONLY this group's own dual_memory — NEVER Owner or other groups."""
        if group_msg.kind != "group" or is_verified_owner_scope(
            group_scope, self.s, include_inactive=True
        ):
            return []
        dual_mem = self._get_dual_memory()
        if dual_mem is None:
            return []
        raw_hits = await self._call_probe(
            dual_mem,
            group_msg,
            query_text,
            query_vector,
            limit=self.config.max_memories_per_target,
            now=now,
        )
        return self._merge_hits(raw_hits, self.config.max_memories_per_target)

    # ------------------------------------------------------------------
    # Engagement state & cooldown math ("捧场正循环")
    # ------------------------------------------------------------------

    def effective_cooldown_seconds(
        self,
        target_scope: str,
        *,
        is_owner: bool,
        streak: int = 0,
        ignore_streak: int = 0,
    ) -> float:
        _ = target_scope
        cfg = self.config
        base = cfg.owner_base_cooldown_seconds if is_owner else cfg.group_base_cooldown_seconds
        if streak > 0:
            factor = max(cfg.min_cooldown_factor, 1.0 - 0.08 * streak)
        elif ignore_streak > 0:
            factor = min(cfg.max_cooldown_factor, 1.0 + 0.35 * ignore_streak)
        else:
            factor = 1.0
        return round(base * factor, 2)

    async def _load_engagement_tx(self, target_scope: str) -> dict[str, Any]:
        row = await self.store.one(
            "SELECT target_scope, last_shared_at, last_engaged_at, streak, ignore_streak, "
            "boost, updated_at FROM share_engagement WHERE target_scope=?",
            (target_scope,),
        )
        if row is None:
            return {
                "target_scope": target_scope,
                "last_shared_at": 0.0,
                "last_engaged_at": 0.0,
                "streak": 0,
                "ignore_streak": 0,
                "boost": 0.0,
                "updated_at": 0.0,
            }
        return {
            "target_scope": str(row["target_scope"]),
            "last_shared_at": float(row["last_shared_at"] or 0.0),
            "last_engaged_at": float(row["last_engaged_at"] or 0.0),
            "streak": int(row["streak"] or 0),
            "ignore_streak": int(row["ignore_streak"] or 0),
            "boost": float(row["boost"] or 0.0),
            "updated_at": float(row["updated_at"] or 0.0),
        }

    async def engagement_state(self, target_scope: str) -> dict[str, Any]:
        await self._ensure_ready()
        async with self.store.lock:
            return await self._load_engagement_tx(target_scope)

    async def _apply_lazy_ignore_tx(self, target_scope: str, now: float) -> dict[str, Any]:
        """Lazily apply one ignore penalty if the latest proactive share went unanswered."""
        state = await self._load_engagement_tx(target_scope)
        last_shared = state["last_shared_at"]
        last_engaged = state["last_engaged_at"]
        grace_deadline = last_shared + self.config.ignore_grace_seconds
        if (
            last_shared > 0.0
            and last_engaged < last_shared
            and now >= grace_deadline
            and state["updated_at"] < grace_deadline
        ):
            next_ignore = state["ignore_streak"] + 1
            next_boost = max(
                self.config.boost_min, round(state["boost"] - self.config.boost_negative_step, 4)
            )
            await self.store.db.execute(
                "INSERT INTO share_engagement("
                "target_scope, last_shared_at, last_engaged_at, "
                "streak, ignore_streak, boost, updated_at"
                ") VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(target_scope) DO UPDATE SET "
                "streak=0, ignore_streak=excluded.ignore_streak, boost=excluded.boost, "
                "updated_at=excluded.updated_at",
                (target_scope, last_shared, last_engaged, 0, next_ignore, next_boost, now),
            )
            state["streak"] = 0
            state["ignore_streak"] = next_ignore
            state["boost"] = next_boost
            state["updated_at"] = now
        return state

    async def check_lazy_ignore(self, target_scope: str, *, now: float | None = None) -> bool:
        await self._ensure_ready()
        ts = self.clock() if now is None else float(now)
        async with self.store.transaction():
            before = await self._load_engagement_tx(target_scope)
            after = await self._apply_lazy_ignore_tx(target_scope, ts)
        return after["ignore_streak"] > before["ignore_streak"]

    async def record_shared(self, target_scope: str, *, now: float | None = None) -> None:
        """Record confirmed proactive delivery timestamp for cooldown & engagement tracking."""
        await self._ensure_ready()
        scope = str(target_scope or "").strip()
        if not scope:
            return
        ts = self.clock() if now is None else float(now)
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO share_engagement("
                "target_scope, last_shared_at, last_engaged_at, "
                "streak, ignore_streak, boost, updated_at"
                ") VALUES (?, ?, 0.0, 0, 0, 0.0, ?) "
                "ON CONFLICT(target_scope) DO UPDATE SET "
                "last_shared_at=MAX(share_engagement.last_shared_at, excluded.last_shared_at), "
                "updated_at=MAX(share_engagement.updated_at, excluded.updated_at)",
                (scope, ts, ts),
            )

    async def record_engagement(
        self,
        target_scope: str,
        *,
        engaged: bool,
        now: float | None = None,
        receipt_id: str | None = None,
    ) -> dict[str, Any]:
        """Update engagement boost/streaks and recover mild social battery on warm replies."""
        await self._ensure_ready()
        scope = str(target_scope or "").strip()
        if not scope:
            return {}
        ts = self.clock() if now is None else float(now)
        cfg = self.config
        battery_recovered = 0.0

        async with self.store.transaction():
            state = await self._load_engagement_tx(scope)
            if engaged:
                next_streak = state["streak"] + 1
                next_ignore = 0
                next_boost = min(cfg.boost_max, round(state["boost"] + cfg.boost_positive_step, 4))
                next_engaged_at = max(state["last_engaged_at"], ts)
            else:
                next_streak = 0
                next_ignore = state["ignore_streak"] + 1
                next_boost = max(cfg.boost_min, round(state["boost"] - cfg.boost_negative_step, 4))
                next_engaged_at = state["last_engaged_at"]

            await self.store.db.execute(
                "INSERT INTO share_engagement("
                "target_scope, last_shared_at, last_engaged_at, "
                "streak, ignore_streak, boost, updated_at"
                ") VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(target_scope) DO UPDATE SET "
                "last_engaged_at=excluded.last_engaged_at, "
                "streak=excluded.streak, "
                "ignore_streak=excluded.ignore_streak, "
                "boost=excluded.boost, "
                "updated_at=excluded.updated_at",
                (
                    scope,
                    state["last_shared_at"],
                    next_engaged_at,
                    next_streak,
                    next_ignore,
                    next_boost,
                    ts,
                ),
            )

        if engaged:
            ws = getattr(self.service, "world_state", None)
            exp = getattr(ws, "experience", None) if ws is not None else None
            if exp is not None and hasattr(exp, "boost_from_engagement"):
                key = str(receipt_id or "").strip() or self.store.vault.digest(
                    "share-engage-battery", scope, round(ts, 3)
                )
                is_owner = is_verified_owner_scope(scope, self.s, include_inactive=True)
                label = "owner" if is_owner else "group"
                battery_recovered = float(
                    await exp.boost_from_engagement(
                        key,
                        now=ts,
                        amount=cfg.engagement_battery_recovery,
                        audience_label=label,
                    )
                )

        return {
            "target_scope": scope,
            "engaged": engaged,
            "last_shared_at": state["last_shared_at"],
            "last_engaged_at": next_engaged_at,
            "streak": next_streak,
            "ignore_streak": next_ignore,
            "boost": next_boost,
            "battery_recovered": battery_recovered,
            "updated_at": ts,
        }

    async def check_inbound_engagement(
        self,
        target_scope: str,
        *,
        now: float | None = None,
        receipt_id: str | None = None,
    ) -> bool:
        """Atomically credit engagement when recipient replies within 15 min of a share."""
        await self._ensure_ready()
        scope = str(target_scope or "").strip()
        if not scope:
            return False
        ts = self.clock() if now is None else float(now)
        cfg = self.config

        # Fast read-only precheck avoids opening a write transaction on unrelated turns.
        async with self.store.lock:
            pre = await self._load_engagement_tx(scope)
        pre_shared = pre["last_shared_at"]
        if not (
            pre_shared > 0.0
            and pre["last_engaged_at"] < pre_shared
            and 0.0 <= (ts - pre_shared) <= cfg.engagement_window_seconds
        ):
            return False

        credited_shared_at: float | None = None
        async with self.store.transaction():
            state = await self._load_engagement_tx(scope)
            last_shared = state["last_shared_at"]
            if (
                last_shared > 0.0
                and state["last_engaged_at"] < last_shared
                and 0.0 <= (ts - last_shared) <= cfg.engagement_window_seconds
            ):
                next_streak = state["streak"] + 1
                next_boost = min(cfg.boost_max, round(state["boost"] + cfg.boost_positive_step, 4))
                next_engaged_at = max(state["last_engaged_at"], ts)
                await self.store.db.execute(
                    "INSERT INTO share_engagement("
                    "target_scope, last_shared_at, last_engaged_at, "
                    "streak, ignore_streak, boost, updated_at"
                    ") VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(target_scope) DO UPDATE SET "
                    "last_engaged_at=excluded.last_engaged_at, "
                    "streak=excluded.streak, "
                    "ignore_streak=0, "
                    "boost=excluded.boost, "
                    "updated_at=excluded.updated_at",
                    (scope, last_shared, next_engaged_at, next_streak, 0, next_boost, ts),
                )
                credited_shared_at = last_shared

        if credited_shared_at is None:
            return False

        ws = getattr(self.service, "world_state", None)
        exp = getattr(ws, "experience", None) if ws is not None else None
        if exp is not None and hasattr(exp, "boost_from_engagement"):
            r_key = str(receipt_id or "").strip() or self.store.vault.digest(
                "share-engage-reply", scope, round(credited_shared_at, 3)
            )
            is_owner = is_verified_owner_scope(scope, self.s, include_inactive=True)
            await exp.boost_from_engagement(
                r_key,
                now=ts,
                amount=cfg.engagement_battery_recovery,
                audience_label="owner" if is_owner else "group",
            )
        return True

    # ------------------------------------------------------------------
    # Immediate enqueue gating & ticket creation
    # ------------------------------------------------------------------

    def _speak_action_for_target(
        self, target_msg: Message, *, is_owner: bool
    ) -> tuple[str, str, str]:
        engine = getattr(self.service, "life_engine", None)
        catalog = getattr(engine, "catalog", None) if engine is not None else None
        if catalog is not None and "social.speak" in catalog.actions:
            action_name = "social.speak"
            target_module = catalog.owners.get(action_name, "social_presence")
        else:
            action_name = (
                "qq.speak" if target_msg.platform == "onebot" else f"{target_msg.platform}.speak"
            )
            target_module = (
                catalog.owners.get(action_name, "")
                if catalog is not None
                else ("qq_social" if target_msg.platform == "onebot" else target_msg.platform)
            ) or "social_presence"

        if is_owner:
            if target_module in {"social_presence", "qq_social"}:
                action_type = f"{target_module}_milestone_share"
            else:
                action_type = "speak"
        else:
            action_type = (
                "group_share" if target_module in {"social_presence", "qq_social"} else "speak"
            )
        return action_name, target_module, action_type

    async def _can_receive_now(
        self,
        target_scope: str,
        target_msg: Message,
        *,
        is_owner: bool,
        engagement: dict[str, Any],
        now: float,
    ) -> tuple[bool, str]:
        engine = getattr(self.service, "life_engine", None)
        if engine is None:
            return False, "no_life_engine"
        cfg = getattr(engine, "config", None)
        if cfg is None or not cfg.enabled:
            return False, "engine_disabled"
        if not getattr(cfg, "resonance_enabled", True):
            return False, "resonance_disabled"

        runtime = getattr(self.store, "runtime", None)
        if runtime is not None:
            if runtime.is_resting(now):
                return False, "sleeping"
            if runtime.day_rest_end(now):
                return False, "day_rest"

        local_dt = datetime.fromtimestamp(now, _zone(self.s.bot_timezone))
        if cfg.quiet(local_dt):
            return False, "quiet_hours"

        cooldown = self.effective_cooldown_seconds(
            target_scope,
            is_owner=is_owner,
            streak=engagement["streak"],
            ignore_streak=engagement["ignore_streak"],
        )
        last_shared = float(engagement.get("last_shared_at") or 0.0)
        if last_shared > 0.0 and 0.0 <= (now - last_shared) < cooldown:
            return False, "cooldown"

        ws = getattr(self.service, "world_state", None)
        if ws is not None:
            if not is_owner and hasattr(ws, "group_cooldown_ok"):
                group_scope = self.store.session_scope(target_msg)
                if not await ws.group_cooldown_ok(group_scope, cooldown, now):
                    return False, "group_cooldown"
            exp = getattr(ws, "experience", None)
            if exp is not None and getattr(exp, "ready", False):
                if await exp.battery(target_msg, now) < SOCIAL_LOW:
                    return False, "social_battery_low"

        action_name, _, _ = self._speak_action_for_target(target_msg, is_owner=is_owner)
        gatekeeper = getattr(engine, "gatekeeper", None)
        if gatekeeper is not None and engine.catalog.get(action_name) is not None:
            if not await gatekeeper.daily_limit_allows(action_name, target_scope, now, cfg):
                return False, "daily_proactive_limit"

        modules = getattr(self.service, "modules", None)
        if modules is not None:
            if modules.normalize_recipient(target_scope) != target_scope:
                return False, "unauthorized_scope"
            if not modules.permits_recipient(target_scope, cfg):
                return False, "recipient_disabled"

        return True, ""

    async def _enqueue_share_ticket(
        self,
        *,
        share_id: str,
        target_scope: str,
        target_msg: Message,
        is_owner: bool,
        experience: dict[str, str],
        expires_at: float,
        now: float,
        dedupe_key: str | None = None,
    ) -> str | None:
        engine = getattr(self.service, "life_engine", None)
        if engine is None or not hasattr(engine, "enqueue_resonance_intent"):
            return None
        action_name, target_module, action_type = self._speak_action_for_target(
            target_msg, is_owner=is_owner
        )
        gate_condition: dict[str, Any] = {"resonance_share_id": share_id}
        if not is_owner:
            gate_condition["untainted_public_share"] = True
        ws = getattr(self.service, "world_state", None)
        mood = getattr(getattr(ws, "current", None), "mood", "") or "pleased"
        note = _safe_intent_note(experience.get("title") or experience.get("summary") or "", self.s)
        ticket_expire = max(now + 60.0, min(expires_at, now + self.config.ticket_ttl_seconds))
        try:
            return await engine.enqueue_resonance_intent(
                action_name=action_name,
                target_module=target_module,
                target_scope=target_scope,
                action_type=action_type,
                execute_after=now,
                expire_at=ticket_expire,
                intent_note=note,
                gate_condition=gate_condition,
                dedupe_key=dedupe_key or f"resonance:{share_id}",
                mood=mood,
                source_module=experience.get("source", ""),
                now=now,
            )
        except ValueError:
            log.debug(
                "event=share_resonance_enqueue_rejected scope=%s share_id=%s",
                target_scope,
                share_id,
                exc_info=True,
            )
            return None

    # ------------------------------------------------------------------
    # Core public API: on_public_experience / pop_due_shares / context / load / mark_delivered
    # ------------------------------------------------------------------

    async def _persist_pending_share(
        self,
        *,
        target_scope: str,
        is_owner: bool,
        impulse: float,
        novelty: float,
        ref_or_digest: str,
        experience: dict[str, str],
        memories: list[dict[str, Any]],
        now: float,
    ) -> tuple[str, float, bool, str]:
        """Insert or reuse a deduped Vault-sealed pending_share row."""
        dedupe_key = self.store.vault.digest("share-resonance-dedupe", target_scope, ref_or_digest)
        expires_at = now + self.config.share_ttl_seconds
        async with self.store.transaction():
            existing = await self.store.one(
                "SELECT id, status, expires_at FROM pending_shares WHERE dedupe_key=?",
                (dedupe_key,),
            )
            if existing is not None:
                return (
                    str(existing["id"]),
                    float(existing["expires_at"]),
                    False,
                    str(existing["status"]),
                )
            share_id = uuid.uuid4().hex
            payload = {
                "experience": experience,
                "memories": memories,
                "impulse": round(impulse, 4),
            }
            sealed = self.store.vault.seal(_seal_context(target_scope, share_id), payload)
            await self.store.db.execute(
                "INSERT INTO pending_shares("
                "id, dedupe_key, target_scope, is_owner, status, score, novelty, "
                "created_at, expires_at, delivered_at, payload_blob"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    share_id,
                    dedupe_key,
                    target_scope,
                    int(is_owner),
                    "pending",
                    round(impulse, 4),
                    round(novelty, 4),
                    now,
                    expires_at,
                    None,
                    sealed,
                ),
            )
            return share_id, expires_at, True, "pending"

    async def on_public_experience(
        self,
        *,
        source: str,
        event_type: str,
        title: str,
        summary: str,
        ref: str = "",
        novelty: float = 0.5,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """Probe Owner and active public groups independently for resonance with a public event."""
        await self._ensure_ready()
        ts = self.clock() if now is None else float(now)
        engine = getattr(self.service, "life_engine", None)
        if engine is not None and not getattr(engine.config, "resonance_enabled", True):
            return []

        experience = self._sanitize_experience(
            source=source, event_type=event_type, title=title, summary=summary, ref=ref
        )
        query_text = " ".join(
            part for part in (experience["title"], experience["summary"]) if part
        ).strip()
        if not query_text:
            return []

        norm_novelty = _clamp01(novelty, 0.5)
        ref_or_digest = (
            f"{experience['source']}:{experience['ref']}"
            if experience["ref"]
            else self.store.vault.digest(
                "share-exp",
                experience["source"],
                experience["event_type"],
                experience["title"],
                experience["summary"],
            )
        )

        # Compute ONE semantic query vector across all target probes (None = lexical fallback).
        query_vector = await self._embed_query(query_text)

        owner_target_pair = self._resolve_owner_target(ts)
        group_target_pairs = self._resolve_group_targets(ts)

        candidates: list[tuple[str, Message, bool]] = []
        if owner_target_pair is not None:
            candidates.append((owner_target_pair[0], owner_target_pair[1], True))
        for g_scope, g_msg in group_target_pairs:
            candidates.append((g_scope, g_msg, False))

        if not candidates:
            return []

        async def _evaluate_target(
            target_scope: str, target_msg: Message, is_owner: bool
        ) -> dict[str, Any] | None:
            if is_owner:
                memories = await self._probe_owner_target(
                    target_scope, target_msg, query_text, query_vector, ts
                )
            else:
                memories = await self._probe_group_target(
                    target_scope, target_msg, query_text, query_vector, ts
                )
            if not memories:
                return None

            best_score = max(float(m["score"]) for m in memories)
            if best_score <= 0.0:
                return None

            async with self.store.transaction():
                engagement = await self._apply_lazy_ignore_tx(target_scope, ts)

            cfg = self.config
            impulse = (
                cfg.resonance_weight * best_score
                + cfg.novelty_weight * norm_novelty
                + float(engagement["boost"])
            )
            threshold = cfg.owner_threshold if is_owner else cfg.group_threshold
            if impulse < threshold:
                return None

            share_id, expires_at, created, existing_status = await self._persist_pending_share(
                target_scope=target_scope,
                is_owner=is_owner,
                impulse=impulse,
                novelty=norm_novelty,
                ref_or_digest=ref_or_digest,
                experience=experience,
                memories=memories,
                now=ts,
            )

            intent_id: str | None = None
            enqueued = False
            block_reason = ""
            if existing_status != "pending" or expires_at <= ts:
                block_reason = (
                    f"already_{existing_status}" if existing_status != "pending" else "expired"
                )
            else:
                can_now, block_reason = await self._can_receive_now(
                    target_scope,
                    target_msg,
                    is_owner=is_owner,
                    engagement=engagement,
                    now=ts,
                )
                if can_now:
                    intent_id = await self._enqueue_share_ticket(
                        share_id=share_id,
                        target_scope=target_scope,
                        target_msg=target_msg,
                        is_owner=is_owner,
                        experience=experience,
                        expires_at=expires_at,
                        now=ts,
                    )
                    enqueued = intent_id is not None
                    if not enqueued:
                        block_reason = "enqueue_rejected"

            return {
                "share_id": share_id,
                "target_scope": target_scope,
                "is_owner": is_owner,
                "score": round(impulse, 4),
                "best_resonance": round(best_score, 4),
                "novelty": round(norm_novelty, 4),
                "boost": round(float(engagement["boost"]), 4),
                "created": created,
                "enqueued": enqueued,
                "intent_id": intent_id,
                "queued_reason": "" if enqueued else block_reason,
                "memory_count": len(memories),
            }

        results = await asyncio.gather(
            *(_evaluate_target(scope, msg, is_owner) for scope, msg, is_owner in candidates)
        )
        return [item for item in results if item is not None]

    async def pop_due_shares(
        self,
        target_scope: str | None = None,
        now: float | None = None,
        *,
        limit: int = 2,
    ) -> list[dict[str, Any]]:
        """Promote warm pending shares into immediate tickets once their target can receive."""
        await self._ensure_ready()
        ts = self.clock() if now is None else float(now)
        bounded_limit = max(1, min(int(limit), 10))
        async with self.store.lock:
            has_expired = (
                await self.store.one(
                    "SELECT 1 FROM pending_shares WHERE status='pending' AND expires_at<=? LIMIT 1",
                    (ts,),
                )
                is not None
            )
        if has_expired:
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE pending_shares SET status='expired' "
                    "WHERE status='pending' AND expires_at<=?",
                    (ts,),
                )
        async with self.store.lock:
            if target_scope is not None:
                rows = await self.store.all(
                    "SELECT * FROM pending_shares WHERE target_scope=? AND status='pending' "
                    "AND expires_at>? ORDER BY score DESC, created_at DESC LIMIT ?",
                    (target_scope, ts, bounded_limit * 2),
                )
            else:
                rows = await self.store.all(
                    "SELECT * FROM pending_shares WHERE status='pending' AND expires_at>? "
                    "ORDER BY is_owner DESC, score DESC, created_at DESC LIMIT ?",
                    (ts, bounded_limit * 4),
                )

        promoted: list[dict[str, Any]] = []
        handled_scopes: set[str] = set()
        modules = getattr(self.service, "modules", None)

        for row in rows:
            if len(promoted) >= bounded_limit:
                break
            scope = str(row["target_scope"])
            if scope in handled_scopes:
                continue
            is_owner = bool(row["is_owner"])
            if is_owner and not is_verified_owner_scope(
                scope, self.s, include_inactive=(modules is None)
            ):
                continue
            target_msg = (
                resolve_social_message(
                    self.service, scope, event_id=f"res:due:{row['id']}", created_at=ts
                )
                if modules is not None
                else self._fallback_message_for_scope(scope, is_owner=is_owner, now=ts)
            )
            if target_msg is None:
                continue
            if not is_owner and target_msg.kind != "group":
                continue
            if is_owner and target_msg.kind != "private":
                continue

            async with self.store.transaction():
                engagement = await self._apply_lazy_ignore_tx(scope, ts)

            can_now, _ = await self._can_receive_now(
                scope, target_msg, is_owner=is_owner, engagement=engagement, now=ts
            )
            if not can_now:
                continue

            loaded = self._unseal_row(row, expected_scope=scope)
            if loaded is None:
                continue

            base_key = f"resonance:{loaded['id']}"
            async with self.store.lock:
                has_intents = await self.store.one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_intents'"
                )
                active_ticket = (
                    await self.store.one(
                        "SELECT 1 FROM scheduled_intents WHERE "
                        "(dedupe_key=? OR dedupe_key LIKE ?) "
                        "AND status IN ('pending','executing','done','uncertain') LIMIT 1",
                        (base_key, f"{base_key}:%"),
                    )
                    if has_intents
                    else None
                )
                prior_terminal = (
                    await self.store.one(
                        "SELECT 1 FROM scheduled_intents WHERE dedupe_key=? LIMIT 1",
                        (base_key,),
                    )
                    if has_intents and active_ticket is None
                    else None
                )
            if active_ticket is not None:
                continue
            ticket_dedupe = f"{base_key}:pop:{int(ts)}" if prior_terminal is not None else base_key

            intent_id = await self._enqueue_share_ticket(
                share_id=loaded["id"],
                target_scope=scope,
                target_msg=target_msg,
                is_owner=is_owner,
                experience=loaded["experience"],
                expires_at=loaded["expires_at"],
                now=ts,
                dedupe_key=ticket_dedupe,
            )
            if intent_id is not None:
                handled_scopes.add(scope)
                promoted.append({**loaded, "intent_id": intent_id, "enqueued": True})
        return promoted

    def _fallback_message_for_scope(
        self, scope: str, *, is_owner: bool, now: float
    ) -> Message | None:
        if is_owner:
            hit = RecipientResolver(self.s, None).resolve_owner(scope, include_inactive=True)
            if hit is None:
                return None
            plat, binding, _ = hit
            return Message(
                platform=plat,
                bot_id=binding.bot_id,
                user_id=binding.user_id,
                conversation_id=binding.user_id,
                kind="private",
                text="",
                addressed=True,
                event_id=f"res:due:{int(now)}",
                created_at=now,
            )
        plat, sep, gid = scope.partition(":")
        if not sep or not gid or plat != "onebot" or not self.s.onebot_self_id:
            return None
        return Message(
            platform="onebot",
            bot_id=self.s.onebot_self_id,
            event_id=f"res:due:{int(now)}",
            user_id="0",
            conversation_id=gid,
            kind="group",
            text="",
            addressed=True,
            created_at=now,
        )

    def _unseal_row(self, row: Any, *, expected_scope: str) -> dict[str, Any] | None:
        row_scope = str(row["target_scope"] or "")
        share_id = str(row["id"] or "")
        try:
            if not hmac.compare_digest(row_scope.encode("utf-8"), expected_scope.encode("utf-8")):
                return None
        except UnicodeEncodeError:
            return None
        try:
            payload = self.store.vault.open(
                _seal_context(expected_scope, share_id), row["payload_blob"]
            )
        except (Rejected, Exception):
            return None
        if not isinstance(payload, dict):
            return None
        return {
            "id": share_id,
            "target_scope": row_scope,
            "is_owner": bool(row["is_owner"]),
            "status": str(row["status"]),
            "score": float(row["score"]),
            "novelty": float(row["novelty"]),
            "created_at": float(row["created_at"]),
            "expires_at": float(row["expires_at"]),
            "delivered_at": float(row["delivered_at"]) if row["delivered_at"] is not None else None,
            "experience": dict(payload.get("experience") or {}),
            "memories": list(payload.get("memories") or []),
        }

    async def pending_for_context(
        self,
        target_scope: str,
        *,
        limit: int = 2,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """Return warm unshared thoughts scoped strictly to target_scope for natural chat recall."""
        await self._ensure_ready()
        scope = str(target_scope or "").strip()
        if not scope:
            return []
        ts = self.clock() if now is None else float(now)
        bounded_limit = max(1, min(int(limit), 5))
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM pending_shares WHERE target_scope=? AND status='pending' "
                "AND expires_at>? ORDER BY score DESC, created_at DESC LIMIT ?",
                (scope, ts, bounded_limit),
            )
        items: list[dict[str, Any]] = []
        for row in rows:
            loaded = self._unseal_row(row, expected_scope=scope)
            if loaded is not None:
                items.append(loaded)
        return items

    async def load_share_for_scope(
        self,
        share_id: str,
        expected_scope: str,
        *,
        now: float | None = None,
        include_delivered: bool = False,
    ) -> dict[str, Any] | None:
        """Strictly verify target_scope matches before unsealing and returning a share bundle."""
        await self._ensure_ready()
        sid = str(share_id or "").strip()
        scope = str(expected_scope or "").strip()
        if not sid or not scope:
            return None
        ts = self.clock() if now is None else float(now)
        async with self.store.lock:
            row = await self.store.one("SELECT * FROM pending_shares WHERE id=?", (sid,))
        if row is None:
            return None
        row_scope = str(row["target_scope"] or "")
        try:
            if not hmac.compare_digest(row_scope.encode("utf-8"), scope.encode("utf-8")):
                return None
        except UnicodeEncodeError:
            return None
        if not include_delivered and str(row["status"]) != "pending":
            return None
        if not include_delivered and float(row["expires_at"]) <= ts:
            return None
        return self._unseal_row(row, expected_scope=scope)

    async def mark_delivered(self, share_id: str, *, now: float | None = None) -> bool:
        """Mark a pending share delivered and update target_scope's last_shared_at atomically."""
        await self._ensure_ready()
        sid = str(share_id or "").strip()
        if not sid:
            return False
        ts = self.clock() if now is None else float(now)
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT target_scope, status FROM pending_shares WHERE id=?",
                (sid,),
            )
            if row is None or str(row["status"]) != "pending":
                return False
            scope = str(row["target_scope"])
            cur = await self.store.db.execute(
                "UPDATE pending_shares SET status='delivered', delivered_at=? "
                "WHERE id=? AND status='pending'",
                (ts, sid),
            )
            if not cur.rowcount:
                return False
            await self.store.db.execute(
                "INSERT INTO share_engagement("
                "target_scope, last_shared_at, last_engaged_at, "
                "streak, ignore_streak, boost, updated_at"
                ") VALUES (?, ?, 0.0, 0, 0, 0.0, ?) "
                "ON CONFLICT(target_scope) DO UPDATE SET "
                "last_shared_at=MAX(share_engagement.last_shared_at, excluded.last_shared_at), "
                "updated_at=MAX(share_engagement.updated_at, excluded.updated_at)",
                (scope, ts, ts),
            )
            return True

    async def admin_status(self, *, now: float | None = None) -> dict[str, Any]:
        """Return display-safe non-secret counts for admin observability."""
        engine = getattr(self.service, "life_engine", None)
        cfg = getattr(engine, "config", None) if engine is not None else None
        enabled = bool(getattr(cfg, "resonance_enabled", True))
        if not self.ready:
            return {
                "enabled": enabled,
                "ready": False,
                "pending_count": 0,
                "owner_pending_count": 0,
                "group_pending_count": 0,
                "delivered_count": 0,
                "tracked_scopes": 0,
                "positive_streak_scopes": 0,
                "backoff_scopes": 0,
            }
        ts = self.clock() if now is None else float(now)
        async with self.store.lock:
            pending_row = await self.store.one(
                "SELECT COUNT(*) AS total, "
                "COALESCE(SUM(CASE WHEN is_owner=1 THEN 1 ELSE 0 END), 0) AS owner_n, "
                "COALESCE(SUM(CASE WHEN is_owner=0 THEN 1 ELSE 0 END), 0) AS group_n "
                "FROM pending_shares WHERE status='pending' AND expires_at>?",
                (ts,),
            )
            delivered_row = await self.store.one(
                "SELECT COUNT(*) FROM pending_shares WHERE status='delivered'"
            )
            eng_row = await self.store.one(
                "SELECT COUNT(*) AS total, "
                "COALESCE(SUM(CASE WHEN streak>0 THEN 1 ELSE 0 END), 0) AS pos_n, "
                "COALESCE(SUM(CASE WHEN ignore_streak>0 THEN 1 ELSE 0 END), 0) AS neg_n "
                "FROM share_engagement"
            )
        return {
            "enabled": enabled,
            "ready": True,
            "pending_count": int(pending_row["total"] or 0) if pending_row else 0,
            "owner_pending_count": int(pending_row["owner_n"] or 0) if pending_row else 0,
            "group_pending_count": int(pending_row["group_n"] or 0) if pending_row else 0,
            "delivered_count": int(delivered_row[0] or 0) if delivered_row else 0,
            "tracked_scopes": int(eng_row["total"] or 0) if eng_row else 0,
            "positive_streak_scopes": int(eng_row["pos_n"] or 0) if eng_row else 0,
            "backoff_scopes": int(eng_row["neg_n"] or 0) if eng_row else 0,
        }

    async def prune(self, now: float | None = None, *, retention_days: int = 7) -> int:
        """Expire overdue pending shares and remove old terminal rows (0-QPS write-free)."""
        await self._ensure_ready()
        ts = self.clock() if now is None else float(now)
        cutoff = ts - max(1, int(retention_days)) * 86400.0
        async with self.store.lock:
            has_work = bool(
                await self.store.one(
                    "SELECT 1 FROM pending_shares WHERE "
                    "(status='pending' AND expires_at<=?) "
                    "OR (status!='pending' AND COALESCE(delivered_at, expires_at)<?) LIMIT 1",
                    (ts, cutoff),
                )
            )
        if not has_work:
            return 0
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE pending_shares SET status='expired' "
                "WHERE status='pending' AND expires_at<=?",
                (ts,),
            )
            cur = await self.store.db.execute(
                "DELETE FROM pending_shares "
                "WHERE status!='pending' AND COALESCE(delivered_at, expires_at)<?",
                (cutoff,),
            )
            return max(0, int(cur.rowcount or 0))
