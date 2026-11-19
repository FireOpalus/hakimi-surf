"""Stateful cross-period event ledger with Fernet encryption and visibility scoping."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from .config import Settings
from .modules.base import sanitize_sensor_text
from .store import Store

log = logging.getLogger("hakimi")

MAX_PENDING_EVENTS = 100
MIN_TTL_SECONDS = 300.0
MAX_TTL_SECONDS = 14 * 86400.0
MAX_HORIZON_SECONDS = 30 * 86400.0
SETTLED_RETENTION_SECONDS = 14 * 86400.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS event_ledger (
    id TEXT PRIMARY KEY,
    dedupe_key TEXT UNIQUE,
    source_module TEXT NOT NULL,
    target_module TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'public' CHECK(visibility IN ('public', 'owner_private')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'consumed', 'expired')),
    created_at REAL NOT NULL,
    due_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    consumed_at REAL,
    consumed_by TEXT NOT NULL DEFAULT '',
    body_blob BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_ledger_active
    ON event_ledger(status, visibility, due_at, expires_at);
CREATE INDEX IF NOT EXISTS idx_event_ledger_target
    ON event_ledger(status, target_module, due_at);
"""


def _sanitize_value(
    value: Any, settings: Settings | None, *, key_limit: int = 24, val_limit: int = 60
) -> Any:
    if isinstance(value, str):
        return sanitize_sensor_text(value, settings, max_tokens=val_limit)
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in list(value.items())[:16]:
            ck = sanitize_sensor_text(str(k), settings, max_tokens=key_limit)
            if ck:
                out[ck] = _sanitize_value(v, settings, key_limit=key_limit, val_limit=val_limit)
        return out
    if isinstance(value, list | tuple):
        return [
            _sanitize_value(item, settings, key_limit=key_limit, val_limit=val_limit)
            for item in list(value)[:16]
        ]
    return sanitize_sensor_text(str(value), settings, max_tokens=val_limit)


class LedgerEvent(BaseModel):
    """Decrypted stateful commitment item stored in the event ledger."""

    model_config = ConfigDict(extra="forbid", strict=True)

    id: str
    dedupe_key: str | None = None
    source_module: str
    target_module: str = ""
    event_type: str
    visibility: Literal["public", "owner_private"] = "public"
    status: Literal["pending", "consumed", "expired"] = "pending"
    created_at: float
    due_at: float
    expires_at: float
    updated_at: float
    consumed_at: float | None = None
    consumed_by: str = ""
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_planner_dict(self, now: float, zone: ZoneInfo) -> dict[str, Any]:
        """Compact representation injected into LifeBlockPlanner prompt context."""
        data: dict[str, Any] = {
            "id": self.id,
            "key": self.dedupe_key or self.id,
            "source": self.source_module,
            "target": self.target_module,
            "type": self.event_type,
            "visibility": self.visibility,
            "is_due": self.due_at <= now,
            "due_local": datetime.fromtimestamp(self.due_at, zone).strftime("%Y-%m-%d %H:%M"),
            "summary": self.summary,
        }
        if self.payload:
            data["payload"] = self.payload
        return data

    def to_admin_dict(self) -> dict[str, Any]:
        """JSON-safe projection for authenticated /admin views."""
        return self.model_dump(mode="json")


class EventLedger:
    """Durable, Fernet-sealed stateful ledger for cross-day module and planner commitments."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.s = settings
        self.store = store
        self.clock = clock
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._due_listener: Callable[[float], Any] | None = None

    def set_due_listener(self, listener: Callable[[float], Any] | None) -> None:
        """Attach a wakeup hook invoked when an event is posted or updated."""
        self._due_listener = listener

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            async with self.store.transaction():
                for stmt in SCHEMA.strip().rstrip(";").split(";"):
                    sql = stmt.strip()
                    if sql:
                        await self.store.db.execute(sql)
            self._initialized = True

    def _decode_row(self, row: Any) -> LedgerEvent:
        body = self.store.vault.open(f"event-ledger:{row['id']}", row["body_blob"])
        if not isinstance(body, dict):
            body = {}
        payload = body.get("payload")
        return LedgerEvent(
            id=row["id"],
            dedupe_key=row["dedupe_key"],
            source_module=row["source_module"],
            target_module=row["target_module"],
            event_type=row["event_type"],
            visibility=row["visibility"],
            status=row["status"],
            created_at=float(row["created_at"]),
            due_at=float(row["due_at"]),
            expires_at=float(row["expires_at"]),
            updated_at=float(row["updated_at"]),
            consumed_at=float(row["consumed_at"]) if row["consumed_at"] is not None else None,
            consumed_by=row["consumed_by"] or "",
            summary=str(body.get("summary") or ""),
            payload=payload if isinstance(payload, dict) else {},
        )

    async def post(
        self,
        source_module: str,
        event_type: str,
        summary: str,
        *,
        payload: dict[str, Any] | None = None,
        target_module: str = "",
        dedupe_key: str | None = None,
        due_at: float | None = None,
        ttl_seconds: float = 3 * 86400.0,
        visibility: Literal["public", "owner_private"] = "public",
        now: float | None = None,
    ) -> LedgerEvent:
        """Create or update a stateful commitment event sealed at rest with Fernet."""
        if visibility not in {"public", "owner_private"}:
            raise ValueError("invalid_visibility")
        clean_source = sanitize_sensor_text(source_module, self.s, max_tokens=24)[:64]
        if not clean_source:
            raise ValueError("invalid_source_module")
        clean_target = sanitize_sensor_text(target_module, self.s, max_tokens=24)[:64]
        clean_type = sanitize_sensor_text(event_type, self.s, max_tokens=24)[:64]
        if not clean_type:
            raise ValueError("invalid_event_type")
        clean_summary = sanitize_sensor_text(summary, self.s, max_tokens=110)
        if not clean_summary:
            raise ValueError("empty_summary")

        clean_payload: dict[str, Any] = {}
        if payload is not None:
            if not isinstance(payload, dict):
                raise ValueError("invalid_payload")
            sanitized = _sanitize_value(payload, self.s)
            if not isinstance(sanitized, dict):
                raise ValueError("invalid_payload")
            clean_payload = sanitized
            if len(json.dumps(clean_payload, ensure_ascii=False, separators=(",", ":"))) > 1024:
                raise ValueError("payload_too_large")

        norm_key: str | None = None
        if dedupe_key is not None and str(dedupe_key).strip():
            raw_key = sanitize_sensor_text(str(dedupe_key), self.s, max_tokens=40)[:128]
            if raw_key:
                prefix = f"{clean_source}:"
                norm_key = raw_key if raw_key.startswith(prefix) else f"{prefix}{raw_key}"[:128]

        await self.initialize()
        ts = self.clock() if now is None else float(now)
        effective_due = ts if due_at is None else max(0.0, float(due_at))
        clamped_ttl = max(MIN_TTL_SECONDS, min(float(ttl_seconds), MAX_TTL_SECONDS))
        expires_at = min(ts + MAX_HORIZON_SECONDS, max(ts, effective_due) + clamped_ttl)

        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE event_ledger SET status='expired', updated_at=? "
                "WHERE status='pending' AND expires_at<=?",
                (ts, ts),
            )
            existing = (
                await self.store.one(
                    "SELECT id, created_at, status FROM event_ledger WHERE dedupe_key=?",
                    (norm_key,),
                )
                if norm_key is not None
                else None
            )
            if existing is not None:
                row_id = existing["id"]
                created_at = (
                    float(existing["created_at"]) if existing["status"] == "pending" else ts
                )
                blob = self.store.vault.seal(
                    f"event-ledger:{row_id}",
                    {"summary": clean_summary, "payload": clean_payload},
                )
                await self.store.db.execute(
                    "UPDATE event_ledger SET source_module=?, target_module=?, event_type=?, "
                    "visibility=?, status='pending', created_at=?, due_at=?, expires_at=?, "
                    "updated_at=?, consumed_at=NULL, consumed_by='', body_blob=? WHERE id=?",
                    (
                        clean_source,
                        clean_target,
                        clean_type,
                        visibility,
                        created_at,
                        effective_due,
                        expires_at,
                        ts,
                        blob,
                        row_id,
                    ),
                )
            else:
                row_id = uuid.uuid4().hex
                created_at = ts
                blob = self.store.vault.seal(
                    f"event-ledger:{row_id}",
                    {"summary": clean_summary, "payload": clean_payload},
                )
                await self.store.db.execute(
                    "INSERT INTO event_ledger("
                    "id, dedupe_key, source_module, target_module, event_type, visibility, "
                    "status, created_at, due_at, expires_at, updated_at, consumed_at, "
                    "consumed_by, body_blob"
                    ") VALUES (?,?,?,?,?,?,'pending',?,?,?,?,NULL,'',?)",
                    (
                        row_id,
                        norm_key,
                        clean_source,
                        clean_target,
                        clean_type,
                        visibility,
                        created_at,
                        effective_due,
                        expires_at,
                        ts,
                        blob,
                    ),
                )
            count_row = await self.store.one(
                "SELECT COUNT(*) FROM event_ledger WHERE status='pending'"
            )
            excess = int((count_row[0] if count_row else 0) or 0) - MAX_PENDING_EVENTS
            if excess > 0:
                await self.store.db.execute(
                    "UPDATE event_ledger SET status='expired', updated_at=? "
                    "WHERE id IN ("
                    "SELECT id FROM event_ledger WHERE status='pending' AND id!=? "
                    "ORDER BY updated_at ASC, rowid ASC LIMIT ?"
                    ")",
                    (ts, row_id, excess),
                )

        if self._due_listener is not None and (clean_source != "planner" or effective_due > ts):
            try:
                res = self._due_listener(effective_due)
                if inspect.isawaitable(res):
                    await res
            except Exception:
                log.warning("event=ledger_due_listener_failed")

        return LedgerEvent(
            id=row_id,
            dedupe_key=norm_key,
            source_module=clean_source,
            target_module=clean_target,
            event_type=clean_type,
            visibility=visibility,
            status="pending",
            created_at=created_at,
            due_at=effective_due,
            expires_at=expires_at,
            updated_at=ts,
            consumed_at=None,
            consumed_by="",
            summary=clean_summary,
            payload=clean_payload,
        )

    async def expire_due(self, now: float | None = None) -> int:
        """Transition expired pending entries so stale dead commitments never linger."""
        await self.initialize()
        ts = self.clock() if now is None else float(now)
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE event_ledger SET status='expired', updated_at=? "
                "WHERE status='pending' AND expires_at<=?",
                (ts, ts),
            )
            return int(cur.rowcount or 0)

    async def read(
        self,
        *,
        source_module: str | None = None,
        target_module: str | None = None,
        event_type: str | None = None,
        include_owner_private: bool = False,
        only_due: bool = False,
        include_settled: bool = False,
        limit: int = 20,
        now: float | None = None,
    ) -> list[LedgerEvent]:
        """Read matching ledger entries without consuming them."""
        await self.initialize()
        ts = self.clock() if now is None else float(now)
        await self.expire_due(ts)

        clauses: list[str] = []
        params: list[Any] = []
        if not include_settled:
            clauses.append("status='pending' AND expires_at>?")
            params.append(ts)
        if not include_owner_private:
            clauses.append("visibility='public'")
        if only_due:
            clauses.append("due_at<=?")
            params.append(ts)
        if source_module is not None and source_module.strip():
            clauses.append("source_module=?")
            params.append(sanitize_sensor_text(source_module, self.s, max_tokens=24)[:64])
        if target_module is not None and target_module.strip():
            clauses.append("target_module=?")
            params.append(sanitize_sensor_text(target_module, self.s, max_tokens=24)[:64])
        if event_type is not None and event_type.strip():
            clauses.append("event_type=?")
            params.append(sanitize_sensor_text(event_type, self.s, max_tokens=24)[:64])

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        bounded_limit = max(1, min(int(limit), 50))
        async with self.store.lock:
            rows = await self.store.all(
                f"SELECT * FROM event_ledger {where} "
                "ORDER BY CASE WHEN status='pending' THEN 0 ELSE 1 END, "
                "CASE WHEN due_at<=? THEN 0 ELSE 1 END, "
                "due_at ASC, created_at ASC LIMIT ?",
                (*params, ts, bounded_limit),
            )
        return [self._decode_row(row) for row in rows]

    async def consume(
        self,
        event_id_or_key: str,
        *,
        consumer: str = "",
        allow_owner_private: bool = False,
        allowed_sources: set[str] | frozenset[str] | None = None,
        now: float | None = None,
    ) -> bool:
        """Explicitly mark a pending entry consumed once its work has actually completed."""
        await self.initialize()
        target = str(event_id_or_key or "").strip()
        if not target:
            return False
        ts = self.clock() if now is None else float(now)
        clean_consumer = sanitize_sensor_text(consumer, self.s, max_tokens=24)[:64]
        candidate_keys = [target]
        if clean_consumer and not target.startswith(f"{clean_consumer}:"):
            candidate_keys.append(f"{clean_consumer}:{target}")
        if allowed_sources:
            for src in sorted(allowed_sources):
                prefixed = f"{src}:{target}"
                if not target.startswith(f"{src}:") and prefixed not in candidate_keys:
                    candidate_keys.append(prefixed)
        placeholders = ",".join("?" for _ in candidate_keys)
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT id, source_module, visibility, status FROM event_ledger "
                f"WHERE id=? OR dedupe_key IN ({placeholders}) LIMIT 1",
                (target, *candidate_keys),
            )
            if row is None or row["status"] != "pending":
                return False
            if row["visibility"] == "owner_private" and not allow_owner_private:
                return False
            if allowed_sources is not None and row["source_module"] not in allowed_sources:
                return False
            cur = await self.store.db.execute(
                "UPDATE event_ledger SET status='consumed', consumed_at=?, consumed_by=?, "
                "updated_at=? WHERE id=? AND status='pending'",
                (ts, clean_consumer, ts, row["id"]),
            )
            return cur.rowcount == 1

    async def planner_state(self, now: float) -> tuple[list[LedgerEvent], str, float | None]:
        """Return bounded active items, due digest for idle suppression, and next due timestamp."""
        await self.initialize()
        await self.expire_due(now)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM event_ledger WHERE status='pending' AND expires_at>? "
                "ORDER BY CASE WHEN due_at<=? THEN 0 ELSE 1 END, due_at ASC, created_at ASC",
                (now, now),
            )
        due_pairs = sorted(
            (r["id"], round(float(r["updated_at"]), 3)) for r in rows if float(r["due_at"]) <= now
        )
        due_digest = (
            self.store.vault.digest("ledger-due", json.dumps(due_pairs)) if due_pairs else ""
        )
        next_upcoming = min(
            (float(r["due_at"]) for r in rows if float(r["due_at"]) > now), default=None
        )
        items = [self._decode_row(row) for row in rows[:10]]
        return items, due_digest, next_upcoming

    async def status_counts(self, now: float | None = None) -> tuple[int, int]:
        """Read-only pending/due counts for status checks without mutating SQLite."""
        ts = self.clock() if now is None else float(now)
        async with self.store.lock:
            if not self._initialized:
                exists = await self.store.one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_ledger'"
                )
                if not exists:
                    return 0, 0
            row = await self.store.one(
                "SELECT COUNT(*), COALESCE(SUM(CASE WHEN due_at<=? THEN 1 ELSE 0 END), 0) "
                "FROM event_ledger WHERE status='pending' AND expires_at>?",
                (ts, ts),
            )
        return int((row[0] if row else 0) or 0), int((row[1] if row else 0) or 0)

    async def admin_list(
        self, now: float | None = None, *, limit: int = 30
    ) -> list[dict[str, Any]]:
        """List recent ledger items for authenticated /admin views."""
        items = await self.read(
            include_owner_private=True, include_settled=True, limit=limit, now=now
        )
        return [item.to_admin_dict() for item in items]

    async def run_cleanup(self) -> None:
        """Background loop expiring overdue items and pruning old settled records."""
        while True:
            try:
                now = self.clock()
                await self.expire_due(now)
                cutoff = now - SETTLED_RETENTION_SECONDS
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM event_ledger "
                        "WHERE status IN ('consumed', 'expired') AND updated_at<?",
                        (cutoff,),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("event=event_ledger_cleanup_failed")
            await asyncio.sleep(600.0)
