"""Unified privacy-guarded memory hub for cross-module writes and recalls."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from .bot_server import is_persona_owner, untrusted_input
from .config import Settings
from .models import Message
from .modules.base import sanitize_sensor_text
from .owner_memory import OwnerMemoryStore
from .security import Rejected, clean_text, configured_secrets, safe_output
from .social import owner_target

log = logging.getLogger("hakimi")

_PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_DUAL_CATEGORIES = frozenset({"event", "slang", "consensus", "open_question"})
_CHAT_CLOUD_KEYS = ("owner_core_memory", "owner_recalled_memories")
_PLANNER_HISTORY_MAX_AGE_SECONDS = 129600.0
_CAPABILITY_SEAL = object()
_MAX_BG_WRITES = 64


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


@dataclass(frozen=True, slots=True)
class MemoryCapability:
    """Unforgeable per-turn memory access token minted strictly via capability_for()."""

    is_owner: bool
    is_private: bool
    can_read_owner_private: bool
    can_write_owner_private: bool
    can_read_cloud_memory: bool
    surface: Literal["private", "group", "public_task"]
    platform: str
    session_scope: str
    _seal: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _CAPABILITY_SEAL:
            raise PermissionError("MemoryCapability must be constructed via capability_for()")


def capability_for(
    message: Message | None,
    settings: Settings,
    *,
    sandbox: bool = False,
) -> MemoryCapability:
    """Mint a protocol-verified MemoryCapability using is_persona_owner() only."""
    if sandbox or message is None:
        return MemoryCapability(
            is_owner=False,
            is_private=False,
            can_read_owner_private=False,
            can_write_owner_private=False,
            can_read_cloud_memory=False,
            surface="public_task",
            platform="",
            session_scope="",
            _seal=_CAPABILITY_SEAL,
        )
    verified_owner = bool(is_persona_owner(message, settings))
    is_priv = bool(
        message.kind == "private"
        and isinstance(message.conversation_id, str)
        and message.conversation_id == message.user_id
    )
    owner_priv = verified_owner and is_priv
    can_cloud = owner_priv and bool(getattr(settings, "enable_antigravity_memory", False))
    surface: Literal["private", "group", "public_task"] = (
        "private" if message.kind == "private" else "group"
    )
    scope = f"{message.platform}:{message.kind}:{message.conversation_id}"
    return MemoryCapability(
        is_owner=verified_owner,
        is_private=is_priv,
        can_read_owner_private=owner_priv,
        can_write_owner_private=owner_priv,
        can_read_cloud_memory=can_cloud,
        surface=surface,
        platform=str(message.platform or ""),
        session_scope=scope,
        _seal=_CAPABILITY_SEAL,
    )


@dataclass(frozen=True)
class ExperienceWriteResult:
    """Result flags and IDs returned by ``MemoryHub.record_experience``."""

    recap_written: bool = False
    ledger_event_id: str | None = None
    chronicle_written: bool = False
    owner_memory_id: str | None = None


class MemoryHub:
    """Resident facade unifying cross-surface memory writes and context recalls."""

    def __init__(
        self,
        settings: Settings,
        service: Any,
        *,
        owner_memory: OwnerMemoryStore | None = None,
        embedder: Any = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.settings = settings
        self.service = service
        self._explicit_owner_memory = owner_memory
        self._lazy_owner_memory: OwnerMemoryStore | None = None
        self._embedder = embedder
        self.clock = clock
        self._writes: int = 0
        self._recalls: int = 0
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._closed: bool = False

    def capability_for(
        self,
        message: Message | None,
        *,
        sandbox: bool = False,
    ) -> MemoryCapability:
        """Verify protocol-layer Owner identity and mint an unforgeable MemoryCapability."""
        return capability_for(message, self.s, sandbox=sandbox)

    def _embedder_client(self) -> Any:
        if self._embedder is not None:
            return self._embedder
        ai = getattr(self.service, "ai", None)
        sessions = self._sessions()
        for holder, attr in (
            (self.service, "embedder"),
            (self.service, "embeddings"),
            (ai, "embeddings"),
            (ai, "embedder"),
            (sessions, "embedder"),
            (sessions, "embeddings"),
        ):
            cand = getattr(holder, attr, None) if holder is not None else None
            if cand is not None:
                return cand
        return None

    def _owner_memory(self) -> OwnerMemoryStore | None:
        if self._explicit_owner_memory is not None:
            return self._explicit_owner_memory
        svc_om = getattr(self.service, "owner_memory", None)
        if svc_om is not None:
            return svc_om
        if self._lazy_owner_memory is None:
            store = getattr(self.service, "store", None)
            vault = getattr(store, "vault", None) if store is not None else None
            self._lazy_owner_memory = OwnerMemoryStore(
                self.s,
                vault=vault,
                embedder=self._embedder_client(),
                clock=lambda: self.clock(),
            )
        elif self._lazy_owner_memory.embedder is None:
            emb = self._embedder_client()
            if emb is not None:
                self._lazy_owner_memory.embedder = emb
        return self._lazy_owner_memory

    @property
    def owner_memory(self) -> OwnerMemoryStore | None:
        """Public accessor for Zone 2 OwnerMemoryStore (used by ShareResonanceEngine)."""
        return self._owner_memory()

    def _schedule_owner_memory_write(
        self,
        owner_mem: Any,
        *,
        source: str,
        topic: str,
        text: str,
        category: str,
        tags: tuple[str, ...],
        now: float,
    ) -> bool:
        """Offload Zone 2 SQLite write off the asyncio event loop; sync fallback if no loop."""
        if self._closed or owner_mem is None:
            return False
        has_sync = hasattr(owner_mem, "record_sync")
        has_async = hasattr(owner_mem, "record")
        if not has_sync and not has_async:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            if len(self._bg_tasks) >= _MAX_BG_WRITES:
                log.warning(
                    "event=memory_hub_bg_write_dropped reason=queue_full category=%s",
                    category,
                )
                return False

            async def _run_write() -> None:
                try:
                    if has_sync:
                        await asyncio.to_thread(
                            owner_mem.record_sync,
                            source,
                            topic,
                            text,
                            category=category,
                            tags=tags,
                            now=now,
                        )
                    else:
                        await owner_mem.record(
                            source,
                            topic,
                            text,
                            category=category,
                            tags=tags,
                            now=now,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_owner_bg_write_failed category=%s exc=%s",
                        category,
                        type(exc).__name__,
                    )

            task = loop.create_task(_run_write(), name=f"memory-hub-{category}")
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
            return True

        if not has_sync:
            return False
        try:
            return bool(
                owner_mem.record_sync(
                    source,
                    topic,
                    text,
                    category=category,
                    tags=tags,
                    now=now,
                )
            )
        except Exception as exc:
            log.debug(
                "event=memory_hub_owner_sync_write_failed category=%s exc=%s",
                category,
                type(exc).__name__,
            )
            return False

    async def drain_background_writes(
        self,
        *,
        max_wait: float = 5.0,
        cancel_on_timeout: bool = False,
    ) -> int:
        """Await tracked offloaded Zone 2 writes cleanly (bounded by max_wait seconds)."""
        drained = 0
        deadline = time.monotonic() + max(0.0, float(max_wait))
        while self._bg_tasks:
            pending = tuple(self._bg_tasks)
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                if cancel_on_timeout:
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*pending, return_exceptions=True)
                break
            done, still = await asyncio.wait(pending, timeout=remaining)
            for task in done:
                drained += 1
                if not task.cancelled():
                    _ = task.exception()
            if still:
                if cancel_on_timeout:
                    for task in still:
                        task.cancel()
                    await asyncio.gather(*still, return_exceptions=True)
                break
        return drained

    async def close(self, *, max_wait: float = 5.0) -> None:
        """Drain background writes and release lazy Zone 2 resources cleanly."""
        self._closed = True
        await self.drain_background_writes(max_wait=max_wait, cancel_on_timeout=True)
        if self._lazy_owner_memory is not None:
            await self._lazy_owner_memory.close()

    def _sessions(self) -> Any:
        return getattr(self.service, "sessions", None)

    def _public_memory(self) -> Any:
        return getattr(self.service, "public_memory", None)

    def _event_ledger(self) -> Any:
        return getattr(self.service, "event_ledger", None)

    def _identity(self) -> Any:
        return getattr(self.service, "identity", None)

    def _notebook(self) -> Any:
        return getattr(self.service, "notebook", None)

    def _cloud_memory(self) -> Any:
        return getattr(self.service, "cloud_memory", None)

    def _all_secrets(self) -> list[str]:
        seen: dict[str, None] = {}
        svc_secrets = getattr(self.service, "secrets", None)
        if svc_secrets:
            for raw in svc_secrets:
                if isinstance(raw, str) and raw:
                    seen[raw] = None
        for raw in configured_secrets(self.s):
            if raw:
                seen[raw] = None
        return list(seen)

    def _has_configured_owner(self) -> bool:
        return bool(self.s.owner_qq_id or getattr(self.s, "social_owner_bindings", None))

    def _configured_owner_platforms(self) -> list[str]:
        preferred = getattr(self.s, "owner_delivery_platform", "onebot") or "onebot"
        ordered: list[str] = [preferred]
        if "onebot" not in ordered:
            ordered.append("onebot")
        bindings = getattr(self.s, "social_owner_bindings", None) or {}
        for plat in bindings:
            if plat not in ordered and plat != "qq":
                ordered.append(plat)
        return ordered

    def _resolve_owner_targets(self, ts: float, event_id: str) -> list[tuple[str, Message, bool]]:
        """Return `(platform, message, is_verified_binding)` for all configured owner platforms."""
        targets: list[tuple[str, Message, bool]] = []
        bindings = getattr(self.s, "social_owner_bindings", None) or {}
        for plat in self._configured_owner_platforms():
            msg = owner_target(self.s, plat, ts, event_id)
            if msg is not None:
                targets.append((plat, msg, True))
                continue
            if plat != "onebot" and plat != "qq":
                binding = bindings.get(plat)
                if binding is not None:
                    fallback = Message(
                        platform=plat,
                        bot_id=binding.bot_id,
                        user_id=binding.user_id,
                        conversation_id=binding.user_id,
                        kind="private",
                        text="",
                        addressed=True,
                        event_id=event_id,
                        created_at=ts,
                    )
                    targets.append((plat, fallback, True))
        return targets

    def _is_verified_private_owner(self, message: Message | None) -> bool:
        return self.capability_for(message).can_read_owner_private

    def status_counts(self) -> dict[str, int]:
        return {"writes": self._writes, "recalls": self._recalls}

    def record_turn_recap(self, message: Message, reply_text: str) -> bool:
        """Record a concise private-owner chat turn recap into the cross-surface ring and Zone 2."""
        cap = self.capability_for(message)
        identity = self._identity()
        owner_mem = self._owner_memory()
        if (
            not cap.can_write_owner_private
            or not isinstance(message.platform, str)
            or not _PLATFORM_ID_RE.fullmatch(message.platform)
            or (
                (identity is None or not hasattr(identity, "record_owner_recap"))
                and owner_mem is None
            )
        ):
            return False
        user_raw = " ".join(str(message.text or "").split())
        reply_raw = " ".join(str(reply_text or "").split())
        secrets = self._all_secrets()
        try:
            user_scrubbed = (
                safe_output(clean_text(user_raw), secrets, limit=300) if user_raw else ""
            )
            reply_scrubbed = (
                safe_output(clean_text(reply_raw), secrets, limit=300) if reply_raw else ""
            )
        except Rejected:
            return False
        user_part = (
            sanitize_sensor_text(user_scrubbed, self.s, max_tokens=45)[:35].strip()
            if user_scrubbed
            else ""
        )
        reply_part = (
            sanitize_sensor_text(reply_scrubbed, self.s, max_tokens=45)[:38].strip()
            if reply_scrubbed
            else ""
        )
        if user_part and reply_part:
            summary = f"{user_part} -> {reply_part}"
        else:
            summary = reply_part or user_part
        if not summary:
            return False
        wrote = False
        if identity is not None and hasattr(identity, "record_owner_recap"):
            identity.record_owner_recap(message.platform, summary, message=message)
            wrote = True
        if owner_mem is not None and self._schedule_owner_memory_write(
            owner_mem,
            source=f"chat:{message.platform}",
            topic=f"{message.platform}_turn",
            text=summary,
            category="turn_recap",
            tags=(message.platform,),
            now=float(self.clock()),
        ):
            wrote = True
        if wrote:
            self._writes += 1
        return wrote

    def record_quick_recap(
        self,
        platform: str,
        summary: str,
        *,
        message: Message | None = None,
        now: float | None = None,
        private_owner: bool = False,
    ) -> bool:
        """Scrub, sanitize, and record a short owner-private surface recap into ring and Zone 2."""
        identity = self._identity()
        owner_mem = self._owner_memory()
        if (identity is None or not hasattr(identity, "record_owner_recap")) and owner_mem is None:
            return False
        if not private_owner:
            return False
        if message is not None:
            if not self.capability_for(message).can_write_owner_private:
                return False
        elif not self._has_configured_owner():
            return False
        if not isinstance(platform, str) or not _PLATFORM_ID_RE.fullmatch(platform):
            return False
        try:
            scrubbed = safe_output(clean_text(str(summary or "")), self._all_secrets(), limit=260)
        except Rejected:
            return False
        cleaned = sanitize_sensor_text(scrubbed, self.s, max_tokens=90) if scrubbed else ""
        if not cleaned:
            return False
        ts = float(self.clock() if now is None else now)
        wrote = False
        if identity is not None and hasattr(identity, "record_owner_recap"):
            identity.record_owner_recap(
                platform,
                cleaned,
                now=ts,
                message=message,
                private_owner=True,
            )
            wrote = True
        if owner_mem is not None and self._schedule_owner_memory_write(
            owner_mem,
            source=platform,
            topic=f"{platform}_recap",
            text=cleaned,
            category="recap",
            tags=(platform,),
            now=ts,
        ):
            wrote = True
        if wrote:
            self._writes += 1
        return wrote

    def _clean_tags(
        self,
        raw_tags: Sequence[str] | Any,
        *,
        fallback_source: str,
        secrets: list[str],
    ) -> list[str]:
        if isinstance(raw_tags, str | bytes) or not isinstance(raw_tags, Sequence):
            raw_tags = ()
        cleaned: list[str] = []
        for raw_tag in raw_tags:
            if not isinstance(raw_tag, str):
                continue
            try:
                scrubbed = safe_output(clean_text(raw_tag), secrets, limit=40)
            except Rejected:
                continue
            tag = sanitize_sensor_text(scrubbed, self.s, max_tokens=10)[:24].strip()
            if tag and tag not in cleaned:
                cleaned.append(tag)
                if len(cleaned) >= 6:
                    break
        if not cleaned and fallback_source:
            cleaned.append(fallback_source[:24])
        return cleaned

    async def record_experience(
        self,
        source: str,
        summary: str,
        *,
        visibility: Literal["public", "owner_private"] = "owner_private",
        event_type: str | None = None,
        payload: dict[str, Any] | None = None,
        target_module: str = "",
        dedupe_key: str | None = None,
        due_at: float | None = None,
        ttl_seconds: float = 48 * 3600.0,
        write_recap: bool = False,
        recap_platform: str | None = None,
        write_ledger: bool = False,
        chronicle_topic: str | None = None,
        chronicle_text: str | None = None,
        chronicle_tags: Sequence[str] = (),
        chronicle_source_id: str | None = None,
        chronicle_category: Literal[
            "event", "fact", "preference", "slang", "consensus", "open_question"
        ] = "event",
        owner_message: Message | None = None,
        entities: Sequence[str] = (),
        links: Sequence[Any] = (),
        valid_from: float | None = None,
        valid_until: float | None = None,
        supersedes_topic: str | None = None,
        write_owner_memory: bool = True,
        now: float | None = None,
    ) -> ExperienceWriteResult:
        """Persist sanitized experience into ledger, recap ring, DualMemory, and Zone 2."""
        if visibility not in {"public", "owner_private"}:
            return ExperienceWriteResult()

        ts = float(self.clock() if now is None else now)
        secrets = self._all_secrets()
        clean_source = sanitize_sensor_text(clean_text(str(source or "")), self.s, max_tokens=24)[
            :64
        ].strip()
        if not clean_source:
            return ExperienceWriteResult()

        try:
            scrubbed_summary = safe_output(clean_text(str(summary or "")), secrets, limit=260)
        except Rejected:
            return ExperienceWriteResult()

        clean_summary = sanitize_sensor_text(scrubbed_summary, self.s, max_tokens=110)
        clean_recap = sanitize_sensor_text(scrubbed_summary, self.s, max_tokens=90)
        if not clean_summary:
            return ExperienceWriteResult()

        ledger_event_id: str | None = None
        if write_ledger and event_type:
            ledger = self._event_ledger()
            if ledger is not None and hasattr(ledger, "post"):
                try:
                    ev = await ledger.post(
                        clean_source,
                        event_type,
                        clean_summary,
                        payload=payload,
                        target_module=target_module,
                        dedupe_key=dedupe_key,
                        visibility=visibility,
                        due_at=due_at,
                        ttl_seconds=ttl_seconds,
                        now=ts,
                    )
                    ev_id = getattr(ev, "id", None)
                    if isinstance(ev_id, str) and ev_id:
                        ledger_event_id = ev_id
                    elif isinstance(ev, str) and ev:
                        ledger_event_id = ev
                except Exception as exc:
                    log.warning("event=memory_hub_ledger_post_failed exc=%s", type(exc).__name__)
                    ledger_event_id = None

        recap_written = False
        chronicle_written = False
        owner_memory_id: str | None = None

        # Hard privacy barrier: owner recaps, Owner DualMemory, and Zone 2 OwnerMemoryStore
        # never accept public events or non-owner targets. Antigravity_Memory is strictly read-only.
        owner_cap = self.capability_for(owner_message) if owner_message is not None else None
        allow_owner_private = (
            visibility == "owner_private"
            and self._has_configured_owner()
            and (owner_cap is None or owner_cap.can_write_owner_private)
        )
        if allow_owner_private:
            recap_target = (recap_platform or clean_source or "").strip()
            identity = self._identity()
            if (
                write_recap
                and clean_recap
                and identity is not None
                and hasattr(identity, "record_owner_recap")
                and _PLATFORM_ID_RE.fullmatch(recap_target)
            ):
                identity.record_owner_recap(
                    recap_target,
                    clean_recap,
                    now=ts,
                    private_owner=True,
                )
                recap_written = True

            clean_topic = ""
            clean_chronicle_text = ""
            clean_tags = self._clean_tags(
                chronicle_tags,
                fallback_source=clean_source,
                secrets=secrets,
            )
            if isinstance(chronicle_topic, str) and chronicle_topic.strip():
                try:
                    scrubbed_topic = safe_output(clean_text(chronicle_topic), secrets, limit=80)
                except Rejected:
                    scrubbed_topic = ""
                clean_topic = (
                    sanitize_sensor_text(scrubbed_topic, self.s, max_tokens=24)[:80].strip()
                    if scrubbed_topic
                    else ""
                )
                if chronicle_text is not None:
                    try:
                        scrubbed_ct = safe_output(
                            clean_text(str(chronicle_text)), secrets, limit=480
                        )
                    except Rejected:
                        scrubbed_ct = ""
                    clean_chronicle_text = (
                        sanitize_sensor_text(scrubbed_ct, self.s, max_tokens=420)[:500].strip()
                        if scrubbed_ct
                        else ""
                    )
                else:
                    clean_chronicle_text = sanitize_sensor_text(
                        scrubbed_summary, self.s, max_tokens=420
                    )[:500].strip()

                sessions = self._sessions()
                dual_mem = getattr(sessions, "dual", None) if sessions is not None else None
                if (
                    clean_topic
                    and clean_chronicle_text
                    and dual_mem is not None
                    and hasattr(dual_mem, "record_chronicle")
                ):
                    target_msg = owner_message
                    if target_msg is None:
                        for plat in self._configured_owner_platforms():
                            candidate = owner_target(self.s, plat, ts, f"hub:{clean_source}")
                            if candidate is not None:
                                target_msg = candidate
                                break
                    if (
                        target_msg is not None
                        and self.capability_for(target_msg).can_write_owner_private
                    ):
                        effective_category = (
                            chronicle_category
                            if chronicle_category in _DUAL_CATEGORIES
                            else "event"
                        )
                        effective_source_id = (
                            chronicle_source_id
                            or f"hub:{clean_source}:{dedupe_key or event_type or 'exp'}"
                        )[:80]
                        try:
                            chronicle_written = bool(
                                await dual_mem.record_chronicle(
                                    target_msg,
                                    topic=clean_topic,
                                    text=clean_chronicle_text,
                                    category=effective_category,
                                    tags=clean_tags,
                                    source_id=effective_source_id,
                                    created_at=ts,
                                )
                            )
                        except Exception as exc:
                            log.warning(
                                "event=memory_hub_chronicle_write_failed exc=%s",
                                type(exc).__name__,
                            )
                            chronicle_written = False

            owner_mem = self._owner_memory()
            if write_owner_memory and owner_mem is not None and hasattr(owner_mem, "record"):
                om_topic = clean_topic or (event_type or clean_source)
                om_text = clean_chronicle_text or clean_summary
                om_dedupe = f"{clean_source}:{dedupe_key}" if dedupe_key else None
                try:
                    owner_memory_id = await owner_mem.record(
                        clean_source,
                        om_topic,
                        om_text,
                        category=chronicle_category,
                        tags=clean_tags,
                        entities=entities,
                        links=links,
                        valid_from=valid_from,
                        valid_until=valid_until,
                        supersedes_topic=supersedes_topic,
                        dedupe_key=om_dedupe,
                        now=ts,
                    )
                except Exception as exc:
                    log.warning(
                        "event=memory_hub_owner_memory_write_failed exc=%s",
                        type(exc).__name__,
                    )
                    owner_memory_id = None

        if (
            ledger_event_id is not None
            or recap_written
            or chronicle_written
            or owner_memory_id is not None
        ):
            self._writes += 1

        return ExperienceWriteResult(
            recap_written=recap_written,
            ledger_event_id=ledger_event_id,
            chronicle_written=chronicle_written,
            owner_memory_id=owner_memory_id,
        )

    async def _read_owner_recent_session(
        self,
        owner_msg: Message | None,
        *,
        limit: int = 8,
        require_enabled: bool = True,
        include_summary: bool = False,
        verified_binding: bool = False,
    ) -> dict[str, Any]:
        """Read recent owner session state without creating missing SQLite files."""
        if not verified_binding and not self._is_verified_private_owner(owner_msg):
            return {}
        if owner_msg is None or owner_msg.kind != "private":
            return {}
        sessions = self._sessions()
        if sessions is None:
            return {}
        path_fn = getattr(sessions, "path", None)
        if callable(path_fn) and not path_fn(owner_msg).exists():
            return {}
        try:
            if hasattr(sessions, "database") and hasattr(sessions, "vault"):
                async with sessions.database(owner_msg, preserve_history=True, readonly=True) as (
                    db,
                    key,
                ):
                    async with db.execute("SELECT enabled FROM session_meta") as cur:
                        row_en = await cur.fetchone()
                    enabled = bool(row_en and row_en[0])
                    if not enabled:
                        return {"enabled": False, "rolling_summary": "", "recent_messages": []}
                    summary_raw = ""
                    if include_summary:
                        async with db.execute("SELECT summary FROM rolling_state") as cur:
                            sum_row = await cur.fetchone()
                        if sum_row and sum_row[0]:
                            summary_raw = sessions.vault.open(key, sum_row[0])
                    async with db.execute(
                        "SELECT payload FROM recent_messages ORDER BY id DESC LIMIT ?",
                        (int(limit),),
                    ) as cur:
                        msg_rows = await cur.fetchall()
                    recent_msgs = [sessions.vault.open(key, r[0]) for r in reversed(msg_rows)]
                return {
                    "enabled": True,
                    "rolling_summary": str(summary_raw or ""),
                    "recent_messages": recent_msgs,
                }
            if hasattr(sessions, "snapshot"):
                snap = await sessions.snapshot(owner_msg)
                if not isinstance(snap, dict):
                    return {}
                enabled = bool(snap.get("enabled", not require_enabled))
                if not enabled:
                    return {"enabled": False, "rolling_summary": "", "recent_messages": []}
                return {
                    "enabled": True,
                    "rolling_summary": str(snap.get("rolling_summary") or ""),
                    "recent_messages": list(snap.get("recent_messages") or [])[-int(limit) :],
                }
        except Exception as exc:
            log.debug("event=memory_hub_session_read_failed exc=%s", type(exc).__name__)
            return {}
        return {}

    async def recall_context(
        self,
        audience: Message | None,
        query: str = "",
        *,
        mode: Literal["chat", "voice", "screen", "planner"] = "chat",
        recall: bool = False,
        sandbox: bool = False,
        related_user_ids: Sequence[str] = (),
        allow_dual_profile: bool = True,
        query_vector: bytes | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Recall audience-authorized context slices for chat, voice, screen, or planner."""
        if sandbox:
            return {}
        ts = float(self.clock() if now is None else now)
        cap = self.capability_for(audience, sandbox=sandbox)

        if mode == "chat":
            if audience is None:
                return {}
            out: dict[str, Any] = {}
            sessions = self._sessions()
            dual_mem = getattr(sessions, "dual", None) if sessions is not None else None
            if dual_mem is not None and hasattr(dual_mem, "prefetch"):
                prefetch_kwargs: dict[str, Any] = {
                    "recall": recall,
                    "related_user_ids": related_user_ids,
                }
                if query_vector is not None and _supports_kwarg(dual_mem.prefetch, "query_vector"):
                    prefetch_kwargs["query_vector"] = query_vector
                dual_data = await dual_mem.prefetch(audience, query, **prefetch_kwargs)
                if not allow_dual_profile and isinstance(dual_data, dict):
                    dual_data = {
                        k: v
                        for k, v in dual_data.items()
                        if k not in {"speaker_profile", "member_profiles"}
                    }
                out["dual_memory_data"] = dual_data

            if cap.can_read_owner_private:
                notebook = self._notebook()
                if notebook is not None and hasattr(notebook, "for_message"):
                    snapshot = notebook.for_message(audience)
                    if snapshot:
                        out["owner_notebook_snapshot"] = snapshot

                public_memory = self._public_memory()
                if public_memory is not None and hasattr(public_memory, "recent_for"):
                    public_history = list(await public_memory.recent_for(audience) or ())
                    while (
                        public_history
                        and len(untrusted_input({"background": public_history}).encode())
                        > self.s.max_context_bytes // 2
                    ):
                        public_history.pop()
                    if public_history:
                        out["近期群聊巡逻记忆（只读背景）"] = public_history

                cloud_memory = self._cloud_memory()
                if (
                    cap.can_read_cloud_memory
                    and cloud_memory is not None
                    and hasattr(cloud_memory, "for_message")
                ):
                    cloud_kwargs: dict[str, Any] = {"recall": recall}
                    if query_vector is not None and _supports_kwarg(
                        cloud_memory.for_message, "query_vector"
                    ):
                        cloud_kwargs["query_vector"] = query_vector
                    cloud_mem = await asyncio.to_thread(
                        cloud_memory.for_message,
                        audience,
                        query,
                        **cloud_kwargs,
                    )
                    if isinstance(cloud_mem, dict) and cloud_mem:
                        for key in _CHAT_CLOUD_KEYS:
                            if val := cloud_mem.get(key):
                                out[key] = val

                identity = self._identity()
                if identity is not None and hasattr(identity, "owner_other_surface_recent"):
                    other_recent = identity.owner_other_surface_recent(
                        audience.platform,
                        now=now,
                        message=audience,
                    )
                    if other_recent:
                        out["owner_other_surface_recent"] = other_recent

                owner_mem = self._owner_memory()
                if (
                    owner_mem is not None
                    and hasattr(owner_mem, "recall")
                    and (recall or bool(str(query or "").strip()) or query_vector is not None)
                ):
                    if self._bg_tasks:
                        await self.drain_background_writes(max_wait=1.0)
                    try:
                        om_hits = await owner_mem.recall(
                            query,
                            query_vector=query_vector,
                            limit=4,
                            now=ts,
                        )
                        if om_hits:
                            out["owner_memories"] = om_hits
                    except Exception as exc:
                        log.debug(
                            "event=memory_hub_owner_memory_recall_failed mode=chat exc=%s",
                            type(exc).__name__,
                        )

            self._recalls += 1
            return out

        if mode == "voice":
            if not cap.can_read_owner_private:
                return {}
            assert audience is not None
            out = {}
            notebook = self._notebook()
            if notebook is not None and hasattr(notebook, "for_message"):
                try:
                    snap = notebook.for_message(audience)
                    if snap:
                        out["owner_notebook_snapshot"] = str(snap)[:800]
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_notebook_read_failed mode=voice exc=%s",
                        type(exc).__name__,
                    )

            cloud_memory = self._cloud_memory()
            if (
                cap.can_read_cloud_memory
                and cloud_memory is not None
                and hasattr(cloud_memory, "for_message")
            ):
                try:
                    cloud_kwargs = {"recall": False}
                    if query_vector is not None and _supports_kwarg(
                        cloud_memory.for_message, "query_vector"
                    ):
                        cloud_kwargs["query_vector"] = query_vector
                    mem = await asyncio.to_thread(
                        cloud_memory.for_message,
                        audience,
                        query,
                        **cloud_kwargs,
                    )
                    if isinstance(mem, dict) and mem:
                        for key in _CHAT_CLOUD_KEYS:
                            if val := mem.get(key):
                                out[key] = val
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_cloud_read_failed mode=voice exc=%s",
                        type(exc).__name__,
                    )

            sess = await self._read_owner_recent_session(
                audience,
                limit=6,
                require_enabled=False,
                include_summary=False,
            )
            turns: list[str] = []
            for item in sess.get("recent_messages", []):
                if not isinstance(item, dict):
                    continue
                txt = str(item.get("text") or "").strip()
                if not txt or txt.startswith(("[协议事件", "/")):
                    continue
                prefix = "BOISER: " if item.get("role") == "user" else "我: "
                turns.append(prefix + clean_text(txt)[:140])
            if turns:
                out["recent_qq_turns"] = turns[-6:]

            sessions = self._sessions()
            dual_mem = getattr(sessions, "dual", None) if sessions is not None else None
            if dual_mem is not None and hasattr(dual_mem, "prefetch"):
                path_fn = getattr(sessions, "path", None)
                if not callable(path_fn) or path_fn(audience).exists():
                    try:
                        dual = await dual_mem.prefetch(
                            audience,
                            query or "语音伴随",
                            recall=True,
                        )
                        if isinstance(dual, dict) and (
                            dual.get("speaker_profile") or dual.get("chronicle_recall")
                        ):
                            out["owner_dual_memory"] = dual
                    except Exception as exc:
                        log.debug(
                            "event=memory_hub_dual_prefetch_failed mode=voice exc=%s",
                            type(exc).__name__,
                        )

            ledger = self._event_ledger()
            if ledger is not None and hasattr(ledger, "read"):
                try:
                    items = await ledger.read(include_owner_private=True, limit=6, now=ts)
                    if items:
                        out["pending_ledger_events"] = [
                            {"key": it.dedupe_key, "summary": it.summary} for it in items[:6]
                        ]
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_ledger_read_failed mode=voice exc=%s",
                        type(exc).__name__,
                    )

            owner_mem = self._owner_memory()
            if owner_mem is not None and hasattr(owner_mem, "recall"):
                if self._bg_tasks:
                    await self.drain_background_writes(max_wait=1.0)
                try:
                    om_hits = await owner_mem.recall(
                        query or "语音伴随",
                        query_vector=query_vector,
                        limit=3,
                        now=ts,
                    )
                    if om_hits:
                        out["owner_memories"] = om_hits
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_owner_memory_recall_failed mode=voice exc=%s",
                        type(exc).__name__,
                    )

            self._recalls += 1
            return out

        if mode == "screen":
            if not cap.can_read_owner_private:
                return {}
            assert audience is not None
            out = {}
            notebook = self._notebook()
            if notebook is not None and hasattr(notebook, "for_message"):
                try:
                    snap = notebook.for_message(audience)
                    if snap:
                        out["owner_notebook_snapshot"] = str(snap)[:600]
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_notebook_read_failed mode=screen exc=%s",
                        type(exc).__name__,
                    )

            ledger = self._event_ledger()
            if ledger is not None and hasattr(ledger, "read"):
                try:
                    items = await ledger.read(include_owner_private=True, limit=4, now=ts)
                    if items:
                        out["pending_ledger_events"] = [
                            {"key": it.dedupe_key, "summary": it.summary} for it in items[:4]
                        ]
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_ledger_read_failed mode=screen exc=%s",
                        type(exc).__name__,
                    )

            owner_mem = self._owner_memory()
            if owner_mem is not None and hasattr(owner_mem, "recall"):
                if self._bg_tasks:
                    await self.drain_background_writes(max_wait=1.0)
                try:
                    om_hits = await owner_mem.recall(
                        query,
                        query_vector=query_vector,
                        limit=2,
                        now=ts,
                    )
                    if om_hits:
                        out["owner_memories"] = om_hits
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_owner_memory_recall_failed mode=screen exc=%s",
                        type(exc).__name__,
                    )

            self._recalls += 1
            return out

        if mode == "planner":
            if audience is not None:
                if not cap.can_read_owner_private:
                    return {}
                targets = [(audience.platform, audience, False)]
            else:
                targets = self._resolve_owner_targets(ts, "planner-owner-ctx")
                if not targets:
                    return {}

            turn_records: list[tuple[float, int, int, str]] = []
            summaries: list[str] = []
            any_enabled = False
            for plat_idx, (_plat, target_msg, verified_binding) in enumerate(targets):
                snap = await self._read_owner_recent_session(
                    target_msg,
                    limit=8,
                    require_enabled=True,
                    include_summary=True,
                    verified_binding=verified_binding,
                )
                if not snap or not snap.get("enabled"):
                    continue
                any_enabled = True
                for msg_idx, item in enumerate(snap.get("recent_messages", [])[-8:]):
                    if not isinstance(item, dict):
                        continue
                    created = item.get("created_at")
                    created_ts = (
                        float(created)
                        if isinstance(created, int | float)
                        and not isinstance(created, bool)
                        and created > 0
                        else 0.0
                    )
                    if created_ts > 0 and created_ts < ts - _PLANNER_HISTORY_MAX_AGE_SECONDS:
                        continue
                    raw_text = str(item.get("text") or "").strip()
                    if not raw_text or raw_text.startswith(("[协议事件", "/")):
                        continue
                    prefix = "BOISER: " if item.get("role") == "user" else "我: "
                    cleaned = sanitize_sensor_text(raw_text, self.s, max_tokens=45)
                    if cleaned:
                        turn_records.append((created_ts, plat_idx, msg_idx, prefix + cleaned))
                summary_clean = sanitize_sensor_text(
                    str(snap.get("rolling_summary") or ""),
                    self.s,
                    max_tokens=80,
                )
                if summary_clean and summary_clean not in summaries:
                    summaries.append(summary_clean)

            if not any_enabled:
                return {}
            out = {}
            if turn_records:
                turn_records.sort(key=lambda rec: (rec[0], rec[1], rec[2]))
                out["recent_turns"] = [rec[3] for rec in turn_records[-5:]]
            if summaries:
                combined_summary = " | ".join(summaries)
                out["rolling_summary"] = sanitize_sensor_text(
                    combined_summary,
                    self.s,
                    max_tokens=120 if len(summaries) > 1 else 80,
                )
            owner_mem = self._owner_memory()
            if owner_mem is not None and hasattr(owner_mem, "recall"):
                if self._bg_tasks:
                    await self.drain_background_writes(max_wait=1.0)
                try:
                    om_hits = await owner_mem.recall(
                        query,
                        query_vector=query_vector,
                        limit=3,
                        now=ts,
                    )
                    if om_hits:
                        out["owner_memories"] = om_hits
                except Exception as exc:
                    log.debug(
                        "event=memory_hub_owner_memory_recall_failed mode=planner exc=%s",
                        type(exc).__name__,
                    )
            self._recalls += 1
            return out

        return {}
