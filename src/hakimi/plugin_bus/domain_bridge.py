"""Plugin-to-kernel domain bridges: world, memory, ledger, chat, KV state, node relay, Bilibili."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import re
import uuid
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from ..bot_server import is_persona_owner
from ..config import Settings
from ..models import AudioRef, FileRef, ImageRef, Message
from ..modules.actions import reject_reason
from ..modules.base import sanitize_sensor_text
from ..roaming_logger import redact_public_summary
from ..security import Rejected, clean_text, safe_output
from ..social import owner_target, permits_chat_export
from ..world_state import IntentTicketCreate, private_event_audience
from .protocol import (
    _ACTION_NAME_RE,
    _BVID_RE,
    _CODE_RE,
    _EVENT_TYPE_RE,
    _LEGACY_KV_VALUE_BYTES,
    _MAX_KV_VALUE_BYTES,
    _MAX_NODE_BRIDGE_INFLIGHT,
    _NODE_BRIDGE_PLUGIN_ACTIONS,
    _PLUGIN_MEDIA_TOKEN_RE,
    _STATE_KEY_RE,
    _VALID_INBOUND_ATTENTION_REASONS,
    _VAULT_KV_PREFIX,
    PLUGIN_MEDIA_PREFIX,
    RESERVED_CHAT_PLATFORMS,
    _sanitize_payload,
    _scrub_json_secrets,
    _scrub_text_secrets,
    _secret_candidates,
)
from .proxy import _PluginSession

log = logging.getLogger("hakimi")

_BILI_PRIVATE_SHARE_PRIORITY_SECONDS = 24 * 3600
_BILI_PRIVATE_HANDLE_TTL_SECONDS = 1800.0
_BILI_PRIVATE_HANDLE_MAX_COMMITS = 2
_MAX_BILI_PRIVATE_HANDLES = 64

__all__ = [
    "_BILI_PRIVATE_HANDLE_MAX_COMMITS",
    "_BILI_PRIVATE_HANDLE_TTL_SECONDS",
    "_BILI_PRIVATE_SHARE_PRIORITY_SECONDS",
    "_MAX_BILI_PRIVATE_HANDLES",
    "PluginBusDomainBridgeMixin",
]


class PluginBusDomainBridgeMixin:
    """Mixin bridging remote plugin frames to kernel world/memory/ledger/state/node/Bili domains."""

    if TYPE_CHECKING:
        s: Settings
        store: Any
        service: Any
        clock: Callable[[], float]
        notify_cooldown: float
        notify_limit: int
        _notify_history: dict[str, deque[float]]
        _sessions: dict[str, _PluginSession]

        def _all_secrets(self) -> list[str]: ...

        def _max_frame_bytes(self) -> int: ...

        async def _send_frame(self, session: _PluginSession, payload: dict[str, Any]) -> None: ...

    async def _handle_world_event(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_type = frame.get("event_type")
        if not isinstance(raw_type, str) or not _EVENT_TYPE_RE.fullmatch(raw_type):
            raise Rejected("plugin_invalid_schema")
        importance = frame.get("importance", "normal")
        if importance not in ("low", "normal", "high"):
            raise Rejected("plugin_invalid_schema")
        raw_payload = frame.get("payload", {})
        if not isinstance(raw_payload, dict) or len(raw_payload) > 32:
            raise Rejected("plugin_invalid_schema")
        raw_report = frame.get("report_text")
        if raw_report is not None and not isinstance(raw_report, str):
            raise Rejected("plugin_invalid_schema")
        raw_longnick = frame.get("mood_longnick")
        if raw_longnick is not None and not isinstance(raw_longnick, str):
            raise Rejected("plugin_invalid_schema")
        clean_payload = _sanitize_payload(raw_payload, self.s)
        clean_type = sanitize_sensor_text(raw_type, self.s, max_tokens=24)[:48]
        if not clean_type:
            raise Rejected("plugin_invalid_schema")

        raw_key = frame.get("business_key") or frame.get("dedupe_key")
        clean_key = (
            sanitize_sensor_text(str(raw_key), self.s, max_tokens=40)[:128]
            if isinstance(raw_key, str) and raw_key.strip()
            else None
        )

        ws = getattr(self.service, "world_state", None)
        if ws is not None and hasattr(ws, "publish_event"):
            exp = getattr(ws, "experience", None)
            if exp is not None and not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                await ws.initialize()
            await ws.publish_event(
                session.plugin_id,
                clean_type,
                clean_payload,
                importance=importance,
                business_key=clean_key,
            )

        raw_summary = frame.get("summary")
        if isinstance(raw_summary, str) and raw_summary.strip():
            safe_summary = _scrub_text_secrets(raw_summary, _secret_candidates(self._all_secrets()))
            ledger = getattr(self.service, "event_ledger", None)
            if ledger is not None and hasattr(ledger, "post"):
                visibility = frame.get("visibility", "public")
                if visibility not in ("public", "owner_private"):
                    visibility = "public"
                due_at = frame.get("due_at")
                if due_at is not None and (
                    isinstance(due_at, bool)
                    or not isinstance(due_at, int | float)
                    or not math.isfinite(float(due_at))
                ):
                    due_at = None
                raw_ttl = frame.get("ttl_seconds")
                ttl_seconds: float | None = None
                if (
                    raw_ttl is not None
                    and not isinstance(raw_ttl, bool)
                    and isinstance(raw_ttl, int | float)
                    and math.isfinite(float(raw_ttl))
                ):
                    ttl_seconds = max(300.0, min(float(raw_ttl), 7 * 86400.0))
                post_kwargs: dict[str, Any] = {
                    "payload": clean_payload,
                    "dedupe_key": clean_key,
                    "due_at": float(due_at) if due_at is not None else None,
                    "visibility": visibility,
                    "wake_planner": importance == "high" or due_at is not None,
                }
                if ttl_seconds is not None:
                    post_kwargs["ttl_seconds"] = ttl_seconds
                try:
                    await ledger.post(
                        session.plugin_id,
                        clean_type,
                        safe_summary,
                        **post_kwargs,
                    )
                except ValueError:
                    pass

            if frame.get("owner_recap") is True and bool(
                self.s.owner_qq_id or getattr(self.s, "social_owner_bindings", None)
            ):
                now = self.clock()
                raw_topic = frame.get("chronicle_topic")
                topic_arg = raw_topic if isinstance(raw_topic, str) and raw_topic.strip() else None
                raw_tags = frame.get("chronicle_tags")
                clean_tags: list[str] = []
                if isinstance(raw_tags, list | tuple):
                    for raw_tag in raw_tags:
                        if not isinstance(raw_tag, str):
                            continue
                        clean_tag = sanitize_sensor_text(
                            clean_text(raw_tag), self.s, max_tokens=10
                        )[:24].strip()
                        if clean_tag and clean_tag not in clean_tags:
                            clean_tags.append(clean_tag)
                            if len(clean_tags) >= 6:
                                break
                if session.plugin_id not in clean_tags:
                    clean_tags = clean_tags[:5] + [session.plugin_id]
                memory_hub = getattr(self.service, "memory_hub", None)
                if memory_hub is not None and hasattr(memory_hub, "record_experience"):
                    await memory_hub.record_experience(
                        session.plugin_id,
                        raw_summary,
                        visibility="owner_private",
                        event_type=clean_type,
                        dedupe_key=clean_key,
                        write_recap=True,
                        recap_platform=session.plugin_id,
                        write_ledger=False,
                        chronicle_topic=topic_arg,
                        chronicle_tags=clean_tags,
                        chronicle_source_id=f"plugin:{session.plugin_id}:{clean_key or clean_type}",
                        now=now,
                    )
                else:
                    try:
                        scrubbed = safe_output(
                            clean_text(raw_summary), self._all_secrets(), limit=260
                        )
                    except Rejected:
                        scrubbed = ""
                    clean_recap = (
                        sanitize_sensor_text(scrubbed, self.s, max_tokens=90) if scrubbed else ""
                    )
                    if clean_recap:
                        identity = getattr(self.service, "identity", None)
                        if identity is not None and hasattr(identity, "record_owner_recap"):
                            identity.record_owner_recap(
                                session.plugin_id,
                                clean_recap,
                                now=now,
                                private_owner=True,
                            )
                        if topic_arg is not None:
                            clean_topic = sanitize_sensor_text(
                                clean_text(topic_arg), self.s, max_tokens=24
                            )[:80].strip()
                            owner_msg = owner_target(
                                self.s,
                                getattr(self.s, "owner_delivery_platform", "onebot"),
                                now,
                                f"plugin-recap:{session.plugin_id}",
                            )
                            sessions = getattr(self.service, "sessions", None)
                            dual_mem = (
                                getattr(sessions, "dual", None) if sessions is not None else None
                            )
                            if (
                                owner_msg is not None
                                and dual_mem is not None
                                and hasattr(dual_mem, "record_chronicle")
                                and clean_topic
                            ):
                                try:
                                    source_id = (
                                        f"plugin:{session.plugin_id}:{clean_key or clean_type}"
                                    )
                                    await dual_mem.record_chronicle(
                                        owner_msg,
                                        topic=clean_topic,
                                        text=clean_recap,
                                        category="event",
                                        tags=clean_tags,
                                        source_id=source_id,
                                        created_at=now,
                                    )
                                except Exception:
                                    pass

        if session.plugin_id == "bilibili_surf" and clean_type == "bili_video_watched":
            if isinstance(raw_report, str) and raw_report.strip():
                if not raw_payload.get("private_first") or raw_payload.get("owner_private"):
                    report_target = owner_target(
                        self.s,
                        getattr(self.s, "owner_delivery_platform", "onebot"),
                        self.clock(),
                        "patrol-report",
                    )
                    if report_target is not None and hasattr(self.store, "save_report"):
                        try:
                            redacted_report = redact_public_summary(
                                safe_output(raw_report, self._all_secrets(), limit=1100)
                            )
                        except Rejected:
                            redacted_report = ""
                        if redacted_report:
                            await self.store.save_report(
                                self.store.subject(report_target),
                                redacted_report,
                                public=not bool(raw_payload.get("private_first")),
                            )
            if (
                isinstance(raw_longnick, str)
                and raw_longnick.strip()
                and not raw_payload.get("private_first")
                and (longnick := getattr(self.service, "longnick", None)) is not None
                and hasattr(longnick, "apply_patrol")
            ):
                await longnick.apply_patrol(raw_longnick[:80])

        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session, {"type": "world.event.ack", "id": frame["id"], "accepted": True}
            )

    async def _handle_chat_inbound(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "chat" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        platform = frame.get("platform")
        if (
            not isinstance(platform, str)
            or platform in RESERVED_CHAT_PLATFORMS
            or platform not in session.proxy.chat_platforms
        ):
            raise Rejected("plugin_scope_rejected")

        raw_images = frame.get("images") or []
        raw_files = frame.get("files") or []
        raw_audios = frame.get("audios") or []
        if (raw_images or raw_files or raw_audios) and "media" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        if (
            not isinstance(raw_images, list)
            or not isinstance(raw_files, list)
            or not isinstance(raw_audios, list)
        ):
            raise Rejected("plugin_invalid_schema")

        kind = frame.get("kind", "private")
        if kind not in ("private", "group"):
            raise Rejected("plugin_invalid_schema")
        raw_user_id = frame.get("user_id")
        if not isinstance(raw_user_id, str) or not raw_user_id.strip():
            raise Rejected("plugin_invalid_schema")
        user_id = raw_user_id.strip()
        conv_id = str(
            frame.get("conversation_id") or (user_id if kind == "private" else "")
        ).strip()
        if not conv_id or (kind == "private" and conv_id != user_id):
            raise Rejected("plugin_invalid_schema")
        bot_id = str(frame.get("bot_id") or session.plugin_id).strip()[:64]
        event_id = str(frame.get("event_id") or uuid.uuid4().hex).strip()[:256]
        text = clean_text(str(frame.get("text") or ""))[:24000]
        addressed = frame.get("addressed")
        if not isinstance(addressed, bool):
            addressed = kind == "private"
        raw_ts = frame.get("created_at", self.clock())
        created_at = (
            float(raw_ts)
            if isinstance(raw_ts, int | float)
            and not isinstance(raw_ts, bool)
            and math.isfinite(float(raw_ts))
            else self.clock()
        )
        sender_nickname = sanitize_sensor_text(
            str(frame.get("sender_nickname") or ""), self.s, max_tokens=20
        )[:80]
        group_name = sanitize_sensor_text(
            str(frame.get("group_name") or ""), self.s, max_tokens=24
        )[:100]
        raw_reply_ids = frame.get("reply_ids") or []
        if not isinstance(raw_reply_ids, list):
            raise Rejected("plugin_invalid_schema")
        reply_ids = [
            clean_text(str(item)).strip()[:128]
            for item in raw_reply_ids[:2]
            if isinstance(item, str | int) and clean_text(str(item)).strip()
        ]
        raw_cmd_text = frame.get("command_text")
        command_text: str | None = None
        if raw_cmd_text is not None:
            if not isinstance(raw_cmd_text, str):
                raise Rejected("plugin_invalid_schema")
            command_text = clean_text(raw_cmd_text)[:24000]
        raw_attention = frame.get("attention_reason", "")
        if (
            not isinstance(raw_attention, str)
            or raw_attention not in _VALID_INBOUND_ATTENTION_REASONS
        ):
            raw_attention = ""

        def _rewrite_media_item(item: Any, id_field: str) -> dict[str, Any]:
            if not isinstance(item, dict):
                raise Rejected("plugin_invalid_schema")
            cloned = dict(item)
            raw_id = cloned.get(id_field)
            if isinstance(raw_id, str) and raw_id.strip():
                tok = raw_id.strip()
                if not _PLUGIN_MEDIA_TOKEN_RE.fullmatch(tok) or tok in {".", ".."} or ".." in tok:
                    raise Rejected("plugin_invalid_schema")
                cloned[id_field] = f"{PLUGIN_MEDIA_PREFIX}{session.plugin_id}:{tok}"
                cloned["url"] = ""
            return cloned

        try:
            images = [
                ImageRef.model_validate(_rewrite_media_item(item, "file"))
                for item in raw_images[:2]
            ]
            files = [
                FileRef.model_validate(_rewrite_media_item(item, "file_id"))
                for item in raw_files[:2]
            ]
            audios = [
                AudioRef.model_validate(_rewrite_media_item(item, "file"))
                for item in raw_audios[:4]
            ]
            msg = Message(
                platform=platform,
                bot_id=bot_id,
                event_id=event_id,
                user_id=user_id,
                kind=kind,
                conversation_id=conv_id,
                text=text,
                addressed=addressed,
                created_at=created_at,
                sender_nickname=sender_nickname,
                group_name=group_name,
                command_text=command_text,
                reply_ids=reply_ids,
                attention_reason=raw_attention,  # type: ignore[arg-type]
                images=images,
                files=files,
                audios=audios,
            )
        except ValidationError:
            raise Rejected("plugin_invalid_schema") from None

        status = "ignored"
        if hasattr(self.service, "accept_from"):
            status = await self.service.accept_from(session.proxy, msg)
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "chat.inbound.ack", "id": frame["id"], "status": status},
            )

    async def _handle_owner_notify(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "owner_notify" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_text = frame.get("text")
        if not isinstance(raw_text, str):
            raise Rejected("plugin_invalid_schema")
        cleaned = sanitize_sensor_text(clean_text(raw_text), self.s, max_tokens=160)[:500].strip()
        if not cleaned:
            raise Rejected("plugin_invalid_schema")
        raw_code = frame.get("code") or "notify"
        safe_code = (
            raw_code if isinstance(raw_code, str) and _CODE_RE.fullmatch(raw_code) else "notify"
        )

        now = self.clock()
        history = self._notify_history.setdefault(session.plugin_id, deque())
        while history and (now - history[0]) > self.notify_cooldown:
            history.popleft()
        frame_id = frame.get("id") if isinstance(frame.get("id"), str) else None
        if len(history) >= self.notify_limit:
            if frame_id:
                await self._send_frame(
                    session,
                    {
                        "type": "owner.notify.ack",
                        "id": frame_id,
                        "ok": False,
                        "reason": "rate_limited",
                    },
                )
            return

        history.append(now)
        proactive = getattr(self.service, "proactive", None)
        delivered = False
        if proactive is not None:
            try:
                if hasattr(proactive, "alert"):
                    res = await proactive.alert(f"plugin:{session.plugin_id}:{safe_code}", cleaned)
                    delivered = res is not None
                elif hasattr(proactive, "enqueue"):
                    res = await proactive.enqueue("patrol", cleaned)
                    delivered = res is not None
            except Exception:
                delivered = False
        if frame_id:
            await self._send_frame(
                session,
                {
                    "type": "owner.notify.ack",
                    "id": frame_id,
                    "ok": True,
                    "queued": delivered,
                },
            )

    async def _ensure_bili_private_tables(self) -> None:
        async with self.store.transaction():
            await self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS bili_served_private_topics ("
                "receipt TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            await self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS bili_private_share_priority ("
                "bvid TEXT PRIMARY KEY, session_scope TEXT NOT NULL, expires_at REAL NOT NULL)"
            )

    async def _next_bili_private_topic(
        self,
        sessions: Any,
        session_list: list[tuple[str, Message]],
        secrets_list: list[str],
    ) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        for _, msg in session_list:
            if msg.kind != "private" or not permits_chat_export(msg, self.s, "bilibili_surf"):
                continue
            try:
                epoch, turns = await sessions.private_request_window(msg)
            except Exception:
                continue
            candidates: list[dict[str, Any]] = []
            scope = self.store.session_scope(msg)
            for item in turns:
                raw = str(item.get("text", ""))
                if item.get("role") != "user":
                    continue
                video = re.search(r"视频|[Bb]站|哔哩哔哩|bilibili", raw, re.I)
                interest = re.search(r"想了解|感兴趣", raw)
                if not video and not interest:
                    continue
                requested = video and re.search(r"找|想看|推荐|帮我|给我|有没有|求", raw)
                specificity = 2 if requested else int(bool(interest))
                try:
                    text = redact_public_summary(safe_output(raw, secrets_list, limit=80))
                except Rejected:
                    continue
                if not text:
                    continue
                seq = item["seq"]
                event = item.get("event_id")
                receipt = self.store.vault.digest(
                    "bili-private-request-v1",
                    scope,
                    epoch,
                    ("event", event) if event else ("seq", seq),
                )
                stamp = item.get("created_at")
                if (
                    isinstance(stamp, bool)
                    or not isinstance(stamp, int | float)
                    or not math.isfinite(stamp)
                ):
                    stamp = 0
                rank = (specificity, stamp, seq)
                candidates.append(
                    {
                        "source": msg,
                        "epoch": epoch,
                        "receipt": receipt,
                        "text": text,
                        "priority": rank,
                        "scope": scope,
                    }
                )
            if not candidates:
                continue
            keys = tuple(c["receipt"] for c in candidates)
            async with self.store.lock:
                if await self._table_exists("bili_served_private_topics"):
                    rows = await self.store.all(
                        "SELECT receipt FROM bili_served_private_topics WHERE receipt IN ("
                        + ",".join("?" for _ in keys)
                        + ")",
                        keys,
                    )
                    served = {r[0] for r in rows}
                else:
                    served = set()
            for topic in candidates:
                if topic["receipt"] not in served and (
                    best is None or topic["priority"] > best["priority"]
                ):
                    best = topic
        return best

    async def _handle_bili_memory_preferences(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if session.plugin_id != "bilibili_surf" or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return

        await self._ensure_bili_private_tables()
        sessions = getattr(self.service, "sessions", None)
        runtime = getattr(self.store, "runtime", None)
        if sessions is None or runtime is None or not hasattr(runtime, "sessions"):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"preferences": {}, "private_topic": None},
                },
            )
            return

        secrets_list = self._all_secrets()
        try:
            session_list = await runtime.sessions()
        except Exception:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"preferences": {}, "private_topic": None},
                },
            )
            return

        private = await self._next_bili_private_topic(sessions, session_list, secrets_list)
        if private is not None:
            preferred: Message = private["source"]
            owner_private = bool(is_persona_owner(preferred, self.s))
            audience = private_event_audience(
                self.s,
                preferred.user_id,
                platform=preferred.platform,
                bot_id=preferred.bot_id,
            )
            handle_id = uuid.uuid4().hex
            issued_at = self.clock()
            stale_handles = [
                key
                for key, item in session._bili_private_handles.items()
                if issued_at >= float(item.get("expires_at", 0.0))
                or int(item.get("commits", 0)) >= _BILI_PRIVATE_HANDLE_MAX_COMMITS
            ]
            for key in stale_handles:
                session._bili_private_handles.pop(key, None)
            if len(session._bili_private_handles) >= _MAX_BILI_PRIVATE_HANDLES:
                oldest_key = next(iter(session._bili_private_handles))
                session._bili_private_handles.pop(oldest_key, None)
            session._bili_private_handles[handle_id] = {
                "source": preferred,
                "epoch": private["epoch"],
                "receipt": private["receipt"],
                "scope": private["scope"],
                "audience": audience,
                "owner_private": owner_private,
                "issued_at": issued_at,
                "expires_at": issued_at + _BILI_PRIVATE_HANDLE_TTL_SECONDS,
                "commits": 0,
            }
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {
                        "preferences": {
                            "recent_chat_topics": [private["text"]],
                            "private_topic_refs": ["recent_chat_topics:0"],
                        },
                        "private_topic": {
                            "handle": handle_id,
                            "owner_private": owner_private,
                            "audience": audience,
                            "session_scope": private["scope"],
                        },
                    },
                },
            )
            return

        topics: dict[str, list[str]] = {}
        now = self.clock()

        def add_topic(field_name: str, raw_val: str, *, count: int, length: int) -> None:
            values = topics.get(field_name, [])
            if len(values) >= count:
                return
            try:
                text = redact_public_summary(safe_output(raw_val, secrets_list, limit=length))
            except Rejected:
                return
            if text and text not in values:
                topics.setdefault(field_name, []).append(text)

        for _, msg in session_list[:20]:
            if msg.kind != "group" or not permits_chat_export(msg, self.s, "bilibili_surf"):
                continue
            try:
                view = await sessions.dual.view(msg)
                snap = await sessions.snapshot(msg)
            except Exception:
                continue
            for entry in view.get("entries", []):
                if (
                    not entry.get("deleted")
                    and (not entry.get("expires") or entry["expires"] > now)
                    and (entry.get("slot") == "preferences" or entry.get("kind") == "chronicle")
                ):
                    add_topic("community_topics", str(entry.get("text", "")), count=10, length=120)
            for item in reversed(snap.get("recent_messages", [])[-6:]):
                if item.get("role") == "user":
                    add_topic("recent_chat_topics", str(item.get("text", "")), count=5, length=80)

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"preferences": topics, "private_topic": None},
            },
        )

    async def _handle_bili_private_topic_commit(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if session.plugin_id != "bilibili_surf" or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return

        handle = frame.get("handle")
        bvid = frame.get("bvid")
        if not isinstance(handle, str) or not handle.strip() or len(handle) > 128:
            raise Rejected("plugin_invalid_schema")
        if not isinstance(bvid, str) or not _BVID_RE.fullmatch(bvid):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_bvid"},
            )
            return

        now = self.clock()
        entry = session._bili_private_handles.get(handle)
        if entry is None:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return
        if (
            now >= float(entry.get("expires_at", 0.0))
            or int(entry.get("commits", 0)) >= _BILI_PRIVATE_HANDLE_MAX_COMMITS
        ):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return

        sessions = getattr(self.service, "sessions", None)
        preferred: Message = entry["source"]
        try:
            current_epoch = (
                await sessions.current_epoch(preferred)
                if sessions is not None and hasattr(sessions, "current_epoch")
                else None
            )
        except Exception:
            current_epoch = None
        if (
            not permits_chat_export(preferred, self.s, "bilibili_surf")
            or current_epoch != entry["epoch"]
        ):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return

        now = self.clock()
        if now >= float(entry.get("expires_at", 0.0)):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return
        expires_at = now + _BILI_PRIVATE_SHARE_PRIORITY_SECONDS
        try:
            async with self.store.transaction():
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS bili_served_private_topics ("
                    "receipt TEXT PRIMARY KEY) WITHOUT ROWID"
                )
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS bili_private_share_priority ("
                    "bvid TEXT PRIMARY KEY, session_scope TEXT NOT NULL, expires_at REAL NOT NULL)"
                )
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO bili_served_private_topics(receipt) VALUES (?)",
                    (entry["receipt"],),
                )
                await self.store.db.execute(
                    "DELETE FROM bili_private_share_priority WHERE expires_at<=?",
                    (now,),
                )
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO bili_private_share_priority "
                    "(bvid, session_scope, expires_at) VALUES (?,?,?)",
                    (bvid, entry["scope"], expires_at),
                )
        except Exception:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_commit_failed",
                },
            )
            return

        commits = int(entry.get("commits", 0)) + 1
        entry["commits"] = commits
        if commits >= _BILI_PRIVATE_HANDLE_MAX_COMMITS:
            session._bili_private_handles.pop(handle, None)

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {
                    "committed": True,
                    "owner_private": bool(entry["owner_private"]),
                    "audience": str(entry["audience"]),
                    "session_scope": str(entry["scope"]),
                },
            },
        )

    async def _handle_state_get(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return

        aad = f"{_VAULT_KV_PREFIX}{session.plugin_id}:{state_key}"
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
                (session.plugin_id, state_key),
            )
        if row is not None:
            try:
                value = self.store.vault.open(aad, row[0])
            except Exception:
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": "state_corrupt"},
                )
                return
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"found": True, "value": value},
                },
            )
            return

        if session.plugin_id == "weather" and state_key == "location":
            async with self.store.lock:
                legacy_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='weather_location'"
                )
            if legacy_row is not None:
                try:
                    saved = self.store.vault.open("admin:weather_location", legacy_row[0])
                except Exception:
                    saved = None
                if isinstance(saved, dict) and saved.get("place") is not None:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": True,
                            "result": {"found": True, "value": saved["place"]},
                        },
                    )
                    return

        if session.plugin_id == "bilibili_surf":
            fallback = await self._load_bili_state_fallback(session, req_id, state_key)
            if fallback is not None:
                found, val = fallback
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"found": found, "value": val},
                    },
                )
                return
            return

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"found": False, "value": None},
            },
        )

    async def _table_exists(self, table_name: str) -> bool:
        row = await self.store.one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return row is not None

    async def _load_bili_state_fallback(
        self, session: _PluginSession, req_id: str, state_key: str
    ) -> tuple[bool, Any] | None:
        if state_key == "env_defaults":
            return True, {
                "enabled": bool(getattr(self.s, "bili_autonomous_enabled", False)),
                "allow_like": False,
                "allow_favorite": False,
                "favorite_folder_id": "",
                "daily_action_limit": 1,
                "bot_timezone": str(getattr(self.s, "bot_timezone", "Asia/Shanghai")),
                "bilibili_enabled": bool(getattr(self.s, "bilibili_enabled", True)),
                "bilibili_min_interval_seconds": float(
                    getattr(self.s, "bilibili_min_interval_seconds", 3.0)
                ),
                "node_bili_delegate_enabled": bool(
                    getattr(self.s, "node_bili_delegate_enabled", True)
                ),
                "node_bili_fallback_local": bool(getattr(self.s, "node_bili_fallback_local", True)),
            }

        if state_key == "policy":
            async with self.store.lock:
                has_admin = await self._table_exists("admin_config")
                row = (
                    await self.store.one(
                        "SELECT payload FROM admin_config WHERE key='bili_account'"
                    )
                    if has_admin
                    else None
                )
            if row is None:
                return False, None
            try:
                saved = self.store.vault.open("admin:bili_account", row[0])
            except Exception:
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": "state_corrupt"},
                )
                return None
            if not isinstance(saved, dict):
                return False, None
            cleaned = {k: v for k, v in saved.items() if k not in {"times", "jitter_minutes"}}
            return True, cleaned

        if state_key == "credentials":
            cred_keys = ("sessdata", "bili_jct", "buvid3", "buvid4")
            async with self.store.lock:
                has_creds = await self._table_exists("bili_credentials")
                row = (
                    await self.store.one("SELECT payload FROM bili_credentials WHERE id=1")
                    if has_creds
                    else None
                )
            if row is not None:
                try:
                    saved = self.store.vault.open("bili-credentials", row[0])
                except Exception:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "state_corrupt",
                        },
                    )
                    return None
                if isinstance(saved, dict):
                    creds = {k: str(saved.get(k) or "") for k in cred_keys}
                    return True, {"creds": creds, "source": "sqlite"}
                return False, None

            def _secret_str(attr: str) -> str:
                raw = getattr(self.s, attr, "")
                return (
                    raw.get_secret_value() if hasattr(raw, "get_secret_value") else str(raw or "")
                )

            env_creds = {
                "sessdata": _secret_str("bili_sessdata"),
                "bili_jct": _secret_str("bili_bili_jct"),
                "buvid3": _secret_str("bili_buvid3"),
                "buvid4": _secret_str("bili_buvid4"),
            }
            if any(env_creds.values()):
                return True, {"creds": env_creds, "source": "env"}
            return False, None

        if state_key == "account_state":
            async with self.store.lock:
                has_acc = await self._table_exists("bili_account_state")
                has_dev = await self._table_exists("bili_device_state")
                acc_row = (
                    await self.store.one(
                        "SELECT blocked_until, last_error, checked_at, account "
                        "FROM bili_account_state WHERE id=1"
                    )
                    if has_acc
                    else None
                )
                dev_row = (
                    await self.store.one(
                        "SELECT fingerprint, attempted_at, status FROM bili_device_state WHERE id=1"
                    )
                    if has_dev
                    else None
                )
            if acc_row is None and dev_row is None:
                return False, None
            opened_account: Any = None
            if acc_row is not None and acc_row["account"] is not None:
                try:
                    opened_account = self.store.vault.open("bili-account", acc_row["account"])
                except Exception:
                    opened_account = None
            return True, {
                "blocked_until": float(acc_row["blocked_until"]) if acc_row else 0.0,
                "last_error": acc_row["last_error"] if acc_row else None,
                "checked_at": (
                    float(acc_row["checked_at"])
                    if acc_row and acc_row["checked_at"] is not None
                    else None
                ),
                "account": opened_account,
                "buvid4_device": (
                    {
                        "fingerprint": str(dev_row["fingerprint"]),
                        "attempted_at": float(dev_row["attempted_at"]),
                        "status": str(dev_row["status"]),
                    }
                    if dev_row is not None
                    else None
                ),
            }

        if state_key == "watched_seed":
            secrets_list = self._all_secrets()
            async with self.store.lock:
                has_watched = await self._table_exists("bili_watched_history")
                has_actions = await self._table_exists("bili_actions")
                has_runs = await self._table_exists("bili_patrol_runs")
                watched_rows = (
                    await self.store.all(
                        "SELECT bvid, run_id, status, created, summary "
                        "FROM bili_watched_history "
                        "WHERE status='done' AND summary IS NOT NULL "
                        "ORDER BY created DESC LIMIT 50"
                    )
                    if has_watched
                    else []
                )
                action_rows = (
                    await self.store.all(
                        "SELECT day, bvid, kind, status, created "
                        "FROM bili_actions ORDER BY created DESC LIMIT 100"
                    )
                    if has_actions
                    else []
                )
                run_rows = (
                    await self.store.all(
                        "SELECT id, day, slot, status, created, reason "
                        "FROM bili_patrol_runs ORDER BY created DESC LIMIT 10"
                    )
                    if has_runs
                    else []
                )
                done_actions: dict[str, list[str]] = {}
                if has_actions and watched_rows:
                    bvids = [str(r["bvid"]) for r in watched_rows]
                    slots = ",".join("?" for _ in bvids)
                    done_rows = await self.store.all(
                        "SELECT bvid, kind FROM bili_actions "
                        f"WHERE status='done' AND bvid IN ({slots})",
                        tuple(bvids),
                    )
                    for dr in done_rows:
                        done_actions.setdefault(str(dr["bvid"]), []).append(str(dr["kind"]))
            watched_items: list[dict[str, Any]] = []
            for r in watched_rows:
                bvid_val = str(r["bvid"])
                try:
                    raw_summary = self.store.vault.open("bili-learn:" + bvid_val, r["summary"])
                    redacted = redact_public_summary(
                        safe_output(str(raw_summary), secrets_list, limit=1200)
                    )
                except Exception:
                    continue
                watched_items.append(
                    {
                        "bvid": bvid_val,
                        "run_id": str(r["run_id"]),
                        "status": str(r["status"]),
                        "created": float(r["created"]),
                        "summary": redacted,
                        "actions": sorted(set(done_actions.get(bvid_val, []))),
                    }
                )
            actions_list = [
                {
                    "day": str(r["day"]),
                    "bvid": str(r["bvid"]),
                    "kind": str(r["kind"]),
                    "status": str(r["status"]),
                    "created": float(r["created"]),
                }
                for r in action_rows
            ]
            runs_list = [
                {
                    "id": str(r["id"]),
                    "day": str(r["day"]),
                    "slot": str(r["slot"]),
                    "status": str(r["status"]),
                    "created": float(r["created"]),
                    "reason": r["reason"],
                }
                for r in run_rows
            ]
            if not watched_items and not actions_list and not runs_list:
                return False, None
            return True, {
                "watched": watched_items,
                "actions": actions_list,
                "runs": runs_list,
            }

        return False, None

    async def _handle_state_set(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return
        if "value" not in frame:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "invalid_state_value",
                },
            )
            return
        value = frame["value"]
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "invalid_state_value",
                },
            )
            return
        kv_limit = (
            _MAX_KV_VALUE_BYTES if session.plugin_id == "bilibili_surf" else _LEGACY_KV_VALUE_BYTES
        )
        if len(encoded) > kv_limit:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "state_value_too_large",
                },
            )
            return
        aad = f"{_VAULT_KV_PREFIX}{session.plugin_id}:{state_key}"
        blob = self.store.vault.seal(aad, value)
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO plugin_kv_state(plugin_id, state_key, payload, updated) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(plugin_id, state_key) DO UPDATE SET "
                "payload=excluded.payload, updated=excluded.updated",
                (session.plugin_id, state_key, blob, now),
            )
        if (
            session.plugin_id == "bilibili_surf"
            and state_key == "credentials"
            and isinstance(value, dict)
        ):
            creds_obj = value.get("creds") if isinstance(value.get("creds"), dict) else value
            cookie_vals = [
                str(creds_obj.get(k) or "")
                for k in ("sessdata", "bili_jct", "buvid3", "buvid4")
                if isinstance(creds_obj.get(k), str) and creds_obj.get(k)
            ]
            if cookie_vals:
                existing_extra = tuple(getattr(self.s, "_runtime_extra_secrets", ()) or ())
                self.s._runtime_extra_secrets = tuple(
                    dict.fromkeys([*existing_extra, *cookie_vals])
                )
                svc_secrets = getattr(self.service, "secrets", None)
                if isinstance(svc_secrets, list):
                    for val in cookie_vals:
                        if val not in svc_secrets:
                            svc_secrets.append(val)
            bw = getattr(self.service, "bili_watcher", None)
            if bw is not None and hasattr(bw, "credentials_epoch"):
                bw.credentials_epoch += 1
        await self._send_frame(
            session,
            {"type": "kernel.result", "id": req_id, "ok": True, "result": {"saved": True}},
        )

    async def _handle_state_delete(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "DELETE FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
                (session.plugin_id, state_key),
            )
            deleted = cur.rowcount > 0
            if session.plugin_id == "weather" and state_key == "location":
                cur_legacy = await self.store.db.execute(
                    "DELETE FROM admin_config WHERE key='weather_location'"
                )
                deleted = deleted or (cur_legacy.rowcount > 0)
            elif session.plugin_id == "bilibili_surf" and state_key == "policy":
                if await self._table_exists("admin_config"):
                    cur_legacy = await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key='bili_account'"
                    )
                    deleted = deleted or (cur_legacy.rowcount > 0)
        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"deleted": bool(deleted)},
            },
        )

    async def _handle_node_call(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if "node_bridge" not in session.scopes:
            log.warning(
                "event=plugin_node_bridge_rejected plugin=%s reason=scope_forbidden",
                session.plugin_id,
            )
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "scope_forbidden",
                },
            )
            return
        action = frame.get("action")
        params = frame.get("params", {})
        allowed_actions = _NODE_BRIDGE_PLUGIN_ACTIONS.get(session.plugin_id, frozenset())
        if not isinstance(action, str) or action not in allowed_actions:
            log.warning(
                "event=plugin_node_bridge_rejected plugin=%s action=%s",
                session.plugin_id,
                sanitize_sensor_text(str(action), self.s, max_tokens=16)[:64],
            )
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_action_forbidden",
                },
            )
            return
        if not isinstance(params, dict):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_invalid_params",
                },
            )
            return
        if session.inflight_node_calls >= _MAX_NODE_BRIDGE_INFLIGHT:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_backpressure",
                },
            )
            return
        session.inflight_node_calls += 1

        async def _dispatch() -> None:
            try:
                nt = getattr(self.service, "node_transport", None)
                if nt is None:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_disconnected",
                        },
                    )
                    return
                try:
                    call_kwargs: dict[str, Any] = {}
                    if action == "mc.dispatch_goal":
                        try:
                            sig = inspect.signature(nt.call)
                        except (TypeError, ValueError):
                            sig = None
                        if sig is not None and (
                            "timeout" in sig.parameters
                            or any(
                                p.kind == inspect.Parameter.VAR_KEYWORD
                                for p in sig.parameters.values()
                            )
                        ):
                            base_timeout = float(getattr(self.s, "node_ws_rpc_timeout", 15.0))
                            call_kwargs["timeout"] = max(base_timeout, 40.0)
                    data = await nt.call(action, params, **call_kwargs)
                except asyncio.CancelledError:
                    raise
                except Rejected as exc:
                    raw_err = str(exc).strip()
                    code = raw_err if _CODE_RE.fullmatch(raw_err) else "node_rpc_failed"
                    err_frame: dict[str, Any] = {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": code,
                    }
                    detail = getattr(exc, "detail", "")
                    if isinstance(detail, str) and detail and _CODE_RE.fullmatch(detail):
                        err_frame["message"] = detail
                    await self._send_frame(session, err_frame)
                    return
                except Exception:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_rpc_failed",
                        },
                    )
                    return
                raw_obj = data if isinstance(data, dict) else {"data": data}
                if session.plugin_id == "bilibili_surf" and action.startswith("bili."):
                    sanitized = _scrub_json_secrets(raw_obj, self._all_secrets())
                else:
                    sanitized = _sanitize_payload(raw_obj, self.s)
                res_frame: dict[str, Any] = {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": sanitized,
                }
                try:
                    encoded = json.dumps(res_frame, ensure_ascii=False, allow_nan=False).encode(
                        "utf-8"
                    )
                except (TypeError, ValueError):
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_invalid_response",
                        },
                    )
                    return
                if len(encoded) > self._max_frame_bytes():
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_frame_too_large",
                        },
                    )
                    return
                await self._send_frame(session, res_frame)
            finally:
                session.inflight_node_calls = max(0, session.inflight_node_calls - 1)

        if session.closed or session.fiber.state != "active":
            session.inflight_node_calls = max(0, session.inflight_node_calls - 1)
            return
        try:
            session.ctx.spawn(
                _dispatch(), name=f"plugin-node-call:{session.plugin_id}:{req_id[:16]}"
            )
        except RuntimeError:
            session.inflight_node_calls = max(0, session.inflight_node_calls - 1)

    async def relay_node_event(self, frame: dict[str, Any]) -> None:
        if not isinstance(frame, dict):
            return
        sanitized_frame = _sanitize_payload(frame, self.s)
        payload = {"type": "node.event", "frame": sanitized_frame}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and "node_bridge" in session.scopes:
                await self._send_frame(session, payload)

    async def relay_node_state(self, connected: bool) -> None:
        payload = {"type": "node.state", "connected": bool(connected)}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and "node_bridge" in session.scopes:
                await self._send_frame(session, payload)

    async def broadcast_world_event(self, event: Any) -> None:
        if isinstance(event, dict):
            payload = event.get("payload")
            event_type = event.get("event_type")
            event_id = event.get("id") or ""
            created_at = event.get("created_at", self.clock())
            source_module = event.get("source_module") or ""
            importance = event.get("importance") or "normal"
        else:
            payload = getattr(event, "payload", None)
            event_type = getattr(event, "event_type", None)
            event_id = getattr(event, "id", "") or ""
            created_at = getattr(event, "created_at", self.clock())
            source_module = getattr(event, "source_module", "") or ""
            importance = getattr(event, "importance", "normal") or "normal"

        if not isinstance(payload, dict):
            return
        if payload.get("private_first") or payload.get("owner_private"):
            return
        if not isinstance(event_type, str) or not _EVENT_TYPE_RE.fullmatch(event_type):
            return
        clean_ts = (
            float(created_at)
            if isinstance(created_at, int | float)
            and not isinstance(created_at, bool)
            and math.isfinite(float(created_at))
            else self.clock()
        )
        sanitized_dict = {
            "id": sanitize_sensor_text(str(event_id), self.s, max_tokens=24)[:64],
            "created_at": clean_ts,
            "source_module": sanitize_sensor_text(str(source_module), self.s, max_tokens=24)[:64],
            "event_type": sanitize_sensor_text(event_type, self.s, max_tokens=24)[:48],
            "importance": importance if importance in ("low", "normal", "high") else "normal",
            "payload": _sanitize_payload(payload, self.s),
        }
        frame = {"type": "world.event.broadcast", "event": sanitized_dict}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and event_type in session.event_subscriptions:
                await self._send_frame(session, frame)

    async def _handle_plugin_status(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        proxy = session.proxy
        if proxy is None:
            return
        if "status" in frame and frame["status"] is not None:
            raw_status = frame["status"]
            if not isinstance(raw_status, dict):
                raise Rejected("plugin_invalid_schema")
            proxy._remote_status = _sanitize_payload(raw_status, self.s)
        if "configuration_issue" in frame:
            raw_issue = frame["configuration_issue"]
            if raw_issue is None or raw_issue == "":
                proxy._configuration_issue = None
            elif isinstance(raw_issue, str) and _CODE_RE.fullmatch(raw_issue):
                proxy._configuration_issue = raw_issue
            else:
                raise Rejected("plugin_invalid_schema")
        if "unavailable_actions" in frame and frame["unavailable_actions"] is not None:
            raw_unavail = frame["unavailable_actions"]
            if not isinstance(raw_unavail, dict) or len(raw_unavail) > 32:
                raise Rejected("plugin_invalid_schema")
            declared = {a.name for a in proxy.actions()}
            if not raw_unavail:
                proxy._action_unavailable.clear()
            else:
                for act_name, reason in raw_unavail.items():
                    if not isinstance(act_name, str) or not _ACTION_NAME_RE.fullmatch(act_name):
                        raise Rejected("plugin_invalid_schema")
                    if act_name not in declared:
                        raise Rejected("plugin_scope_rejected")
                    if reason is None or reason == "":
                        proxy._action_unavailable.pop(act_name, None)
                    elif isinstance(reason, str) and _CODE_RE.fullmatch(reason):
                        proxy._action_unavailable[act_name] = reason
                    else:
                        raise Rejected("plugin_invalid_schema")
        if "current_activity" in frame:
            raw_act = frame["current_activity"]
            if raw_act is None or raw_act == "":
                proxy._current_activity = None
            elif isinstance(raw_act, str) and _CODE_RE.fullmatch(raw_act):
                proxy._current_activity = raw_act
            else:
                raise Rejected("plugin_invalid_schema")
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "plugin.status.ack", "id": frame["id"], "ok": True},
            )

    async def _handle_world_activity(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_task = frame.get("active_task", "")
        if raw_task is None:
            raw_task = ""
        if not isinstance(raw_task, str):
            raise Rejected("plugin_invalid_schema")
        active_task = sanitize_sensor_text(raw_task, self.s, max_tokens=16)[:32]
        disconnected = frame.get("disconnected", False)
        if not isinstance(disconnected, bool):
            raise Rejected("plugin_invalid_schema")
        raw_ts = frame.get("updated_at")
        if raw_ts is None:
            updated_at = self.clock()
        elif (
            isinstance(raw_ts, int | float)
            and not isinstance(raw_ts, bool)
            and math.isfinite(float(raw_ts))
            and float(raw_ts) >= 0.0
        ):
            updated_at = float(raw_ts)
        else:
            raise Rejected("plugin_invalid_schema")

        kind = frame.get("kind")
        if kind == "bili_reading":
            if session.plugin_id != "bilibili_surf":
                raise Rejected("plugin_scope_rejected")
            active = frame.get("active")
            if not isinstance(active, bool):
                raise Rejected("plugin_invalid_schema")
            ws = getattr(self.service, "world_state", None)
            if ws is not None and hasattr(ws, "experience"):
                exp = ws.experience
                if not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                    await ws.initialize()
                await exp.bili_reading(active, updated_at)
        else:
            if kind is not None and (not isinstance(kind, str) or not _CODE_RE.fullmatch(kind)):
                raise Rejected("plugin_invalid_schema")
            ws = getattr(self.service, "world_state", None)
            if (
                ws is not None
                and hasattr(ws, "experience")
                and (
                    session.plugin_id == "minecraft"
                    or (session.proxy is not None and session.proxy.attention_level == "HIGH")
                )
            ):
                exp = ws.experience
                if not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                    await ws.initialize()
                await exp.mc_snapshot(active_task, updated_at, disconnected=disconnected)
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "world.activity.ack", "id": frame["id"], "accepted": True},
            )

    async def _handle_world_reaction(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "actions" not in session.scopes or "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        frame_id = frame.get("id") if isinstance(frame.get("id"), str) else None

        async def _reject(reason: str) -> None:
            log.warning(
                "event=plugin_reaction_rejected plugin=%s reason=%s",
                session.plugin_id,
                reason,
            )
            if frame_id:
                await self._send_frame(
                    session,
                    {
                        "type": "world.reaction.ack",
                        "id": frame_id,
                        "ok": False,
                        "reason": reason,
                    },
                )

        action_name = frame.get("action_name")
        registry = getattr(self.service, "modules", None)
        if (
            not isinstance(action_name, str)
            or not _ACTION_NAME_RE.fullmatch(action_name)
            or registry is None
            or registry.catalog.owners.get(action_name) != session.plugin_id
        ):
            await _reject("action_not_owned")
            return
        spec = registry.catalog.actions.get(action_name)
        if spec is None:
            await _reject("unknown_action")
            return

        raw_note = frame.get("note") if "note" in frame else frame.get("intent_note", "")
        if not isinstance(raw_note, str):
            await _reject("invalid_note")
            return
        clean_note = sanitize_sensor_text(clean_text(raw_note), self.s, max_tokens=64)[:240].strip()
        if not clean_note:
            await _reject("empty_note")
            return

        raw_args = frame.get("arguments")
        if raw_args is None:
            raw_args = {}
        if not isinstance(raw_args, dict):
            await _reject("invalid_action_arguments")
            return
        clean_args = _sanitize_payload(raw_args, self.s)

        raw_ttl = frame.get("ttl", 7200)
        if (
            isinstance(raw_ttl, bool)
            or not isinstance(raw_ttl, int | float)
            or not math.isfinite(float(raw_ttl))
            or float(raw_ttl) <= 0
            or float(raw_ttl) > 86400
        ):
            await _reject("invalid_ttl")
            return
        ttl = float(raw_ttl)

        raw_delay = frame.get("delay", 0)
        if (
            isinstance(raw_delay, bool)
            or not isinstance(raw_delay, int | float)
            or not math.isfinite(float(raw_delay))
            or float(raw_delay) < 0
            or float(raw_delay) > 3600
        ):
            await _reject("invalid_delay")
            return
        delay = float(raw_delay)

        raw_ev_id = frame.get("source_event_id") or ""
        if not isinstance(raw_ev_id, str):
            await _reject("invalid_source_event_id")
            return
        source_event_id = sanitize_sensor_text(raw_ev_id, self.s, max_tokens=16)[:64]

        raw_legacy_mod = frame.get("legacy_module") or ""
        if not isinstance(raw_legacy_mod, str) or (
            raw_legacy_mod and raw_legacy_mod != session.plugin_id
        ):
            await _reject("invalid_legacy_module")
            return

        raw_legacy_verb = frame.get("legacy_verb") or ""
        if not isinstance(raw_legacy_verb, str):
            await _reject("invalid_legacy_verb")
            return
        legacy_verb = sanitize_sensor_text(raw_legacy_verb, self.s, max_tokens=16)[:64]

        raw_scope = frame.get("target_scope") or ""
        if not isinstance(raw_scope, str):
            await _reject("invalid_action_audience")
            return
        target_scope = sanitize_sensor_text(raw_scope, self.s, max_tokens=24)[:128]

        ws = getattr(self.service, "world_state", None)
        if ws is None or not hasattr(ws, "enqueue_intent"):
            await _reject("world_state_unavailable")
            return
        exp = getattr(ws, "experience", None)
        if exp is not None and not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
            await ws.initialize()

        now = self.clock()
        dedupe_key = (
            f"plugin_rule:{session.plugin_id}:{action_name}:{source_event_id}"[:160]
            if source_event_id
            else None
        )
        mood = str(getattr(getattr(ws, "current", None), "mood", "") or "calm")
        try:
            ticket = IntentTicketCreate(
                action_name=action_name,
                target_scope=target_scope,
                origin="rule",
                target_module=session.plugin_id,
                action_type=legacy_verb or action_name,
                source_event_id=source_event_id,
                source_module=session.plugin_id,
                arguments=clean_args,
                execute_after=now + delay,
                expire_at=now + delay + ttl,
                gate_condition={"milestone_event_id": source_event_id} if source_event_id else {},
                intent_note=clean_note,
                mood=mood,
                dedupe_key=dedupe_key,
            )
            engine = getattr(self.service, "life_engine", None)
            if engine is not None and hasattr(engine, "resolve"):
                engine.resolve(ticket.model_dump())
            else:
                if reason := registry.inactive_reason(session.plugin_id):
                    raise ValueError(reason)
                if "rule" not in spec.origins:
                    raise ValueError("action_origin_forbidden")
                if spec.social != bool(target_scope):
                    raise ValueError("invalid_action_audience")
                spec.arguments.model_validate(clean_args)
        except ValueError as exc:
            await _reject(reject_reason(exc))
            return

        intent_id = await ws.enqueue_intent(ticket, now=now)
        engine = getattr(self.service, "life_engine", None)
        if engine is not None and getattr(engine, "wakeup", None) is not None:
            engine.wakeup.set()
        log.info(
            "event=plugin_reaction_enqueued plugin=%s action=%s",
            session.plugin_id,
            action_name,
        )
        if frame_id:
            await self._send_frame(
                session,
                {
                    "type": "world.reaction.ack",
                    "id": frame_id,
                    "ok": True,
                    "intent_id": intent_id,
                },
            )
