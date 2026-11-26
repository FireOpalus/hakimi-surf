"""Unified privacy-guarded memory hub for cross-module writes and recalls."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .bot_server import is_persona_owner, untrusted_input
from .config import Settings
from .models import Message
from .modules.base import sanitize_sensor_text
from .security import Rejected, clean_text, configured_secrets, safe_output
from .social import owner_target

_PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
_DUAL_CATEGORIES = frozenset({"event", "slang", "consensus", "open_question"})
_CHAT_CLOUD_KEYS = ("owner_core_memory", "owner_recalled_memories")
_PLANNER_HISTORY_MAX_AGE_SECONDS = 129600.0


@dataclass(frozen=True)
class ExperienceWriteResult:
    """Result flags and IDs returned by ``MemoryHub.record_experience``."""

    recap_written: bool = False
    ledger_event_id: str | None = None
    chronicle_written: bool = False


class MemoryHub:
    """Resident facade unifying cross-surface memory writes and context recalls."""

    def __init__(
        self,
        settings: Settings,
        service: Any,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.settings = settings
        self.service = service
        self.clock = clock
        self._writes: int = 0
        self._recalls: int = 0

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

    def _is_verified_private_owner(self, message: Message | None) -> bool:
        return bool(
            message is not None and message.kind == "private" and is_persona_owner(message, self.s)
        )

    def status_counts(self) -> dict[str, int]:
        return {"writes": self._writes, "recalls": self._recalls}

    def record_turn_recap(self, message: Message, reply_text: str) -> bool:
        """Record a concise private-owner chat turn recap into the cross-surface ring."""
        identity = self._identity()
        if (
            identity is None
            or not hasattr(identity, "record_owner_recap")
            or not self._is_verified_private_owner(message)
            or not isinstance(message.platform, str)
            or not _PLATFORM_ID_RE.fullmatch(message.platform)
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
        identity.record_owner_recap(message.platform, summary, message=message)
        self._writes += 1
        return True

    def record_quick_recap(
        self,
        platform: str,
        summary: str,
        *,
        message: Message | None = None,
        now: float | None = None,
        private_owner: bool = False,
    ) -> bool:
        """Scrub, sanitize, and record a short owner-private surface recap."""
        identity = self._identity()
        if identity is None or not hasattr(identity, "record_owner_recap"):
            return False
        if not private_owner:
            return False
        if message is not None:
            if not self._is_verified_private_owner(message):
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
        identity.record_owner_recap(
            platform,
            cleaned,
            now=ts,
            message=message,
            private_owner=True,
        )
        self._writes += 1
        return True

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
        now: float | None = None,
    ) -> ExperienceWriteResult:
        """Persist sanitized cross-module experience into ledger, recap ring, and DualMemory."""
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
                except Exception:
                    ledger_event_id = None

        recap_written = False
        chronicle_written = False

        # Hard privacy barrier: owner recaps and Owner DualMemory never accept public events
        # or non-owner targets. Antigravity_Memory / owner_cloud_memory is strictly read-only.
        allow_owner_private = (
            visibility == "owner_private"
            and self._has_configured_owner()
            and (owner_message is None or self._is_verified_private_owner(owner_message))
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

                clean_tags = self._clean_tags(
                    chronicle_tags,
                    fallback_source=clean_source,
                    secrets=secrets,
                )
                sessions = self._sessions()
                dual_mem = getattr(sessions, "dual", None) if sessions is not None else None
                if (
                    clean_topic
                    and clean_chronicle_text
                    and dual_mem is not None
                    and hasattr(dual_mem, "record_chronicle")
                ):
                    target_msg = owner_message or owner_target(
                        self.s,
                        getattr(self.s, "owner_delivery_platform", "onebot") or "onebot",
                        ts,
                        f"hub:{clean_source}",
                    )
                    if target_msg is not None and self._is_verified_private_owner(target_msg):
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
                        except Exception:
                            chronicle_written = False

        if ledger_event_id is not None or recap_written or chronicle_written:
            self._writes += 1

        return ExperienceWriteResult(
            recap_written=recap_written,
            ledger_event_id=ledger_event_id,
            chronicle_written=chronicle_written,
        )

    async def _read_owner_recent_session(
        self,
        owner_msg: Message | None,
        *,
        limit: int = 8,
        require_enabled: bool = True,
        include_summary: bool = False,
    ) -> dict[str, Any]:
        """Read recent owner session state without creating missing SQLite files."""
        if not self._is_verified_private_owner(owner_msg):
            return {}
        sessions = self._sessions()
        if sessions is None:
            return {}
        path_fn = getattr(sessions, "path", None)
        if callable(path_fn) and not path_fn(owner_msg).exists():
            return {}
        try:
            if hasattr(sessions, "database") and hasattr(sessions, "vault"):
                async with sessions.database(owner_msg, preserve_history=True) as (db, key):
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
        except Exception:
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
        now: float | None = None,
    ) -> dict[str, Any]:
        """Recall audience-authorized context slices for chat, voice, screen, or planner."""
        if sandbox:
            return {}
        ts = float(self.clock() if now is None else now)

        if mode == "chat":
            if audience is None:
                return {}
            out: dict[str, Any] = {}
            sessions = self._sessions()
            dual_mem = getattr(sessions, "dual", None) if sessions is not None else None
            if dual_mem is not None and hasattr(dual_mem, "prefetch"):
                dual_data = await dual_mem.prefetch(
                    audience,
                    query,
                    recall=recall,
                    related_user_ids=related_user_ids,
                )
                if not allow_dual_profile and isinstance(dual_data, dict):
                    dual_data = {
                        k: v
                        for k, v in dual_data.items()
                        if k not in {"speaker_profile", "member_profiles"}
                    }
                out["dual_memory_data"] = dual_data

            if self._is_verified_private_owner(audience):
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
                if cloud_memory is not None and hasattr(cloud_memory, "for_message"):
                    cloud_mem = await asyncio.to_thread(
                        cloud_memory.for_message,
                        audience,
                        query,
                        recall=recall,
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

            self._recalls += 1
            return out

        if mode == "voice":
            if not self._is_verified_private_owner(audience):
                return {}
            assert audience is not None
            out = {}
            notebook = self._notebook()
            if notebook is not None and hasattr(notebook, "for_message"):
                try:
                    snap = notebook.for_message(audience)
                    if snap:
                        out["owner_notebook_snapshot"] = str(snap)[:800]
                except Exception:
                    pass

            cloud_memory = self._cloud_memory()
            if cloud_memory is not None and hasattr(cloud_memory, "for_message"):
                try:
                    mem = await asyncio.to_thread(
                        cloud_memory.for_message,
                        audience,
                        query,
                        recall=False,
                    )
                    if isinstance(mem, dict) and mem:
                        for key in _CHAT_CLOUD_KEYS:
                            if val := mem.get(key):
                                out[key] = val
                except Exception:
                    pass

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
                    except Exception:
                        pass

            ledger = self._event_ledger()
            if ledger is not None and hasattr(ledger, "read"):
                try:
                    items = await ledger.read(include_owner_private=True, limit=6, now=ts)
                    if items:
                        out["pending_ledger_events"] = [
                            {"key": it.dedupe_key, "summary": it.summary} for it in items[:6]
                        ]
                except Exception:
                    pass

            self._recalls += 1
            return out

        if mode == "screen":
            if not self._is_verified_private_owner(audience):
                return {}
            assert audience is not None
            out = {}
            notebook = self._notebook()
            if notebook is not None and hasattr(notebook, "for_message"):
                try:
                    snap = notebook.for_message(audience)
                    if snap:
                        out["owner_notebook_snapshot"] = str(snap)[:600]
                except Exception:
                    pass

            ledger = self._event_ledger()
            if ledger is not None and hasattr(ledger, "read"):
                try:
                    items = await ledger.read(include_owner_private=True, limit=4, now=ts)
                    if items:
                        out["pending_ledger_events"] = [
                            {"key": it.dedupe_key, "summary": it.summary} for it in items[:4]
                        ]
                except Exception:
                    pass

            self._recalls += 1
            return out

        if mode == "planner":
            owner_msg = audience or owner_target(
                self.s,
                getattr(self.s, "owner_delivery_platform", "onebot") or "onebot",
                ts,
                "planner-owner-ctx",
            )
            if not self._is_verified_private_owner(owner_msg):
                return {}
            snap = await self._read_owner_recent_session(
                owner_msg,
                limit=8,
                require_enabled=True,
                include_summary=True,
            )
            if not snap or not snap.get("enabled"):
                return {}
            turns = []
            for item in snap.get("recent_messages", [])[-8:]:
                if not isinstance(item, dict):
                    continue
                created = item.get("created_at")
                if (
                    isinstance(created, int | float)
                    and not isinstance(created, bool)
                    and created > 0
                    and created < ts - _PLANNER_HISTORY_MAX_AGE_SECONDS
                ):
                    continue
                raw_text = str(item.get("text") or "").strip()
                if not raw_text or raw_text.startswith(("[协议事件", "/")):
                    continue
                prefix = "BOISER: " if item.get("role") == "user" else "我: "
                cleaned = sanitize_sensor_text(raw_text, self.s, max_tokens=45)
                if cleaned:
                    turns.append(prefix + cleaned)
            out = {}
            if turns:
                out["recent_turns"] = turns[-5:]
            summary_clean = sanitize_sensor_text(
                str(snap.get("rolling_summary") or ""),
                self.s,
                max_tokens=80,
            )
            if summary_clean:
                out["rolling_summary"] = summary_clean
            self._recalls += 1
            return out

        return {}
