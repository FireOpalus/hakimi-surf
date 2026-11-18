"""WorldStateManager, plaintext JSON SQLite schemas, strict models, and Attention Lock."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .config import Settings
from .context_budget import estimate_tokens, truncate_tokens
from .modules.actions import Origin
from .modules.base import (
    ATTENTION_RANK,
    AttentionLevel,
    ModuleRegistry,
    sanitize_sensor_text,
)
from .store import Store

log = logging.getLogger("hakimi")

SCHEMA = """
CREATE TABLE IF NOT EXISTS world_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  current_module TEXT NOT NULL DEFAULT 'idle',
  focus_level TEXT NOT NULL DEFAULT 'LOW' CHECK(focus_level IN ('LOW','MEDIUM','HIGH')),
  mood TEXT NOT NULL DEFAULT 'calm',
  energy INTEGER NOT NULL DEFAULT 80 CHECK(energy BETWEEN 0 AND 100),
  boredom INTEGER NOT NULL DEFAULT 20 CHECK(boredom BETWEEN 0 AND 100),
  last_proactive_at REAL NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL DEFAULT 0,
  snapshot_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS scheduled_intents (
  id TEXT PRIMARY KEY,
  created_at REAL NOT NULL,
  execute_after REAL NOT NULL,
  expire_at REAL NOT NULL,
  target_module TEXT NOT NULL,
  target_scope TEXT NOT NULL DEFAULT '',
  action_type TEXT NOT NULL,
  mood TEXT NOT NULL DEFAULT '',
  gate_condition_json TEXT NOT NULL DEFAULT '{}',
  intent_note_json TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK(status IN ('pending','gated_skip','executing','done','expired','uncertain')),
  updated_at REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_scheduled_intents_due ON scheduled_intents(status, execute_after);
CREATE TABLE IF NOT EXISTS global_events (
  id TEXT PRIMARY KEY,
  created_at REAL NOT NULL,
  source_module TEXT NOT NULL,
  event_type TEXT NOT NULL,
  importance TEXT NOT NULL DEFAULT 'normal' CHECK(importance IN ('low','normal','high')),
  consumed INTEGER NOT NULL DEFAULT 0 CHECK(consumed IN (0,1)),
  payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_global_events_unconsumed ON global_events(consumed, created_at);
CREATE TABLE IF NOT EXISTS group_proactive_cooldown (
  scope TEXT PRIMARY KEY,
  last_proactive_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS life_event_order (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE
);
CREATE TRIGGER IF NOT EXISTS life_sequence_new_event AFTER INSERT ON global_events BEGIN
  INSERT INTO life_event_order(event_id) VALUES(NEW.id);
END;
CREATE TABLE IF NOT EXISTS life_rule_cursors (rule_id TEXT PRIMARY KEY, last_row INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS life_rule_receipts (
  rule_id TEXT NOT NULL, event_id TEXT NOT NULL, cooldown_key TEXT NOT NULL,
  created_at REAL NOT NULL, outcome TEXT NOT NULL, intent_id TEXT, PRIMARY KEY(rule_id,event_id)
);
CREATE INDEX IF NOT EXISTS idx_life_rule_cooldown
ON life_rule_receipts(rule_id,cooldown_key,created_at) WHERE intent_id IS NOT NULL;
"""

_VALID_INTENT_STATUSES = frozenset(
    {"pending", "gated_skip", "executing", "done", "expired", "uncertain"}
)
_FORBIDDEN_SPEECH_KEYS = frozenset(
    {
        "speech",
        "dialogue",
        "message",
        "text",
        "reply",
        "script",
        "content",
        "utterance",
        "say",
    }
)
_SPEECH_DIRECTIVE_RE = re.compile(
    r"(?i)^\s*(?:say|speak|reply|send|tell|ask|whisper|dialogue|speech|utterance|"
    r"文案|话术|台词|回复内容|发送内容|直接发|原话|回复|说)\s*[:：]"
)
_QUOTED_DIALOGUE_RE = re.compile(r"[“「『][^”」』]{2,}[”」』]|\"[^\"]{4,}\"")
_VOCATIVE_SPEECH_RE = re.compile(
    r"(?i)^\s*(?:嗨|哈喽|早安|晚安|在吗|主人[，,！!]|BOISER[，,！!]|hello\b|hey\b|hi\b)"
)
_PERSONA_SIGN_RE = re.compile(r"(?:喵[～~]|\[role=|\[cq:|</untrusted)", re.IGNORECASE)

ATTENTION_SWITCH_NOTE = "当前正高专注投入活动中，被动收到消息临时切屏回复"


def _sanitize_payload(
    value: Any, settings: Settings | None = None, *, key_limit: int = 32, val_limit: int = 90
) -> Any:
    if isinstance(value, str):
        return sanitize_sensor_text(value, settings, max_tokens=val_limit)
    if isinstance(value, dict):
        return {
            sanitize_sensor_text(str(k), settings, max_tokens=key_limit): _sanitize_payload(
                v, settings, key_limit=key_limit, val_limit=val_limit
            )
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _sanitize_payload(item, settings, key_limit=key_limit, val_limit=val_limit)
            for item in value
        ]
    return value


class PrivateEventScope(ValueError):
    """A scoped event cannot be material for this recipient or action."""


def private_event_for_owner(payload: dict, recipient: str, settings: Settings) -> bool:
    return bool(
        settings.owner_qq_id
        and payload.get("owner_private") is True
        and recipient == "owner:" + settings.owner_qq_id
    )


def _check_no_prewritten_speech(text: str) -> None:
    if len(text) > 240:
        raise ValueError("intent_note exceeds 240 characters")
    if (
        _SPEECH_DIRECTIVE_RE.search(text)
        or _QUOTED_DIALOGUE_RE.search(text)
        or _VOCATIVE_SPEECH_RE.search(text)
        or _PERSONA_SIGN_RE.search(text)
    ):
        raise ValueError("intent_note must not contain pre-written dialogue or markers")


def _validate_dict_no_speech(mapping: dict[str, Any], *, check_values: bool) -> None:
    for key, val in mapping.items():
        if str(key).strip().lower() in _FORBIDDEN_SPEECH_KEYS:
            raise ValueError(f"forbidden speech field: {key}")
        if isinstance(val, dict):
            _validate_dict_no_speech(val, check_values=check_values)
        elif isinstance(val, (list, tuple)):
            for item in val:
                if isinstance(item, dict):
                    _validate_dict_no_speech(item, check_values=check_values)
                elif check_values and isinstance(item, str):
                    _check_no_prewritten_speech(item)
        elif check_values and isinstance(val, str):
            _check_no_prewritten_speech(val)


class WorldStateSnapshot(BaseModel):
    """Serializable state of Hakimi's current activity, attention lock, and inner drives."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    current_module: str = Field(default="idle", min_length=1, max_length=64)
    focus_level: AttentionLevel = "LOW"
    mood: str = Field(default="calm", min_length=1, max_length=64)
    energy: int = Field(default=80, ge=0, le=100)
    boredom: int = Field(default=20, ge=0, le=100)
    last_proactive_at: float = Field(default=0.0, ge=0.0)
    updated_at: float = Field(default=0.0, ge=0.0)
    details: dict[str, Any] = Field(default_factory=dict)


class IntentTicketCreate(BaseModel):
    """Strict intent ticket schema; rejects pre-written dialogue fields and scripts."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    target_module: str = Field(min_length=1, max_length=64)
    target_scope: str = Field(default="", max_length=128)
    action_type: str = Field(min_length=1, max_length=64)
    action_name: str | None = Field(default=None, max_length=64)
    arguments: dict[str, Any] = Field(default_factory=dict)
    origin: Origin = "legacy"
    source_module: str = Field(default="", max_length=64)
    source_event_id: str = Field(default="", max_length=64)
    dedupe_key: str | None = Field(default=None, max_length=160)
    execute_after: float = Field(ge=0.0)
    expire_at: float = Field(ge=0.0)
    mood: str = Field(default="", max_length=64)
    gate_condition: dict[str, Any] = Field(default_factory=dict)
    intent_note: str | dict[str, Any] = ""

    @field_validator("gate_condition")
    @classmethod
    def _validate_gate_condition(cls, value: dict[str, Any]) -> dict[str, Any]:
        _validate_dict_no_speech(value, check_values=False)
        return value

    @field_validator("intent_note")
    @classmethod
    def _validate_intent_note(cls, value: str | dict[str, Any]) -> str | dict[str, Any]:
        if isinstance(value, str):
            _check_no_prewritten_speech(value)
            return value
        _validate_dict_no_speech(value, check_values=True)
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if len(encoded) > 240:
            raise ValueError("intent_note exceeds 240 characters")
        return value

    @model_validator(mode="after")
    def _check_window(self) -> IntentTicketCreate:
        if self.expire_at < self.execute_after:
            raise ValueError("expire_at must be >= execute_after")
        return self


class MilestoneEvent(BaseModel):
    """Global milestone event with all payload strings neutralized via sanitize_sensor_text."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    id: str = Field(default_factory=lambda: uuid.uuid4().hex, min_length=1, max_length=64)
    created_at: float = Field(default_factory=time.time, ge=0.0)
    source_module: str = Field(min_length=1, max_length=64)
    event_type: str = Field(min_length=1, max_length=64)
    importance: Literal["low", "normal", "high"] = "normal"
    consumed: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _sanitize_fields(self) -> MilestoneEvent:
        self.source_module = (
            sanitize_sensor_text(self.source_module, None, max_tokens=24) or "unknown"
        )
        self.event_type = sanitize_sensor_text(self.event_type, None, max_tokens=24) or "event"
        self.payload = _sanitize_payload(self.payload, None)
        return self


class WorldStateManager:
    """Manages persistent world state, scheduled intents, global events, and Attention Lock."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        registry: ModuleRegistry | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.s = settings
        self.store = store
        self.registry = registry
        self.clock = clock
        self.current = WorldStateSnapshot()
        self._subscribers: list[Callable[[MilestoneEvent], Any]] = []
        self._sensor_overrides: dict[str, str] = {}

    async def initialize(self) -> WorldStateSnapshot:
        """Create plaintext JSON tables, ensure singleton row 1 exists, and load state."""
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
        now = self.clock()
        async with self.store.transaction():
            existing = {
                row["name"] for row in await self.store.all("PRAGMA table_info(scheduled_intents)")
            }
            for name, declaration in {
                "action_name": "TEXT",
                "expected_bubbles": "INTEGER",
                "arguments_json": "TEXT NOT NULL DEFAULT '{}'",
                "source_module": "TEXT NOT NULL DEFAULT ''",
                "source_event_id": "TEXT NOT NULL DEFAULT ''",
                "origin": "TEXT NOT NULL DEFAULT 'legacy'",
                "dedupe_key": "TEXT",
                "result_reason": "TEXT NOT NULL DEFAULT ''",
                "shadow_result": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if name not in existing:
                    await self.store.db.execute(
                        f"ALTER TABLE scheduled_intents ADD COLUMN {name} {declaration}"
                    )
            await self.store.db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_intent_dedupe "
                "ON scheduled_intents(dedupe_key)"
            )
            await self.store.db.execute(
                "INSERT OR IGNORE INTO world_state("
                "singleton, current_module, focus_level, mood, energy, boredom, "
                "last_proactive_at, updated_at, snapshot_json"
                ") VALUES (1, 'idle', 'LOW', 'calm', 80, 20, 0, ?, '{}')",
                (now,),
            )
            row = await self.store.one("SELECT * FROM world_state WHERE singleton=1")
        if row is not None:
            raw_snap = json.loads(row["snapshot_json"] or "{}")
            details = raw_snap.get("details", {}) if isinstance(raw_snap, dict) else {}
            if not isinstance(details, dict):
                details = {}
            self.current = WorldStateSnapshot(
                current_module=row["current_module"],
                focus_level=row["focus_level"],
                mood=row["mood"],
                energy=int(row["energy"]),
                boredom=int(row["boredom"]),
                last_proactive_at=float(row["last_proactive_at"]),
                updated_at=float(row["updated_at"]),
                details=_sanitize_payload(details, self.settings),
            )
        await self._sync_attention_lock(prev_focus="LOW", new_focus=self.current.focus_level)
        return self.current

    async def _sync_attention_lock(
        self, *, prev_focus: AttentionLevel, new_focus: AttentionLevel
    ) -> None:
        if self.registry is None:
            return
        if new_focus == "HIGH":
            for module in self.registry.all():
                if module.attention_level != "HIGH":
                    await module.pause_proactive()
        elif prev_focus == "HIGH" and new_focus != "HIGH":
            for module in self.registry.all():
                if module.attention_level != "HIGH":
                    await module.resume_proactive()

    def set_sensor_override(self, module_id: str, summary: str) -> None:
        """Set or clear an explicit sensor override for chat_sensor_payload."""
        key = sanitize_sensor_text(module_id, self.settings, max_tokens=16)
        if not key:
            return
        cleaned = sanitize_sensor_text(summary, self.settings, max_tokens=60)
        if cleaned:
            self._sensor_overrides[key] = cleaned
        else:
            self._sensor_overrides.pop(key, None)

    def clear_sensor_overrides(self) -> None:
        """Clear all explicit active sensor overrides."""
        self._sensor_overrides.clear()

    async def update_state(
        self,
        snapshot: WorldStateSnapshot | None = None,
        *,
        current_module: str | None = None,
        focus_level: AttentionLevel | None = None,
        mood: str | None = None,
        energy: int | None = None,
        boredom: int | None = None,
        last_proactive_at: float | None = None,
        details: dict[str, Any] | None = None,
        sensor_overrides: dict[str, str] | None = None,
    ) -> WorldStateSnapshot:
        """Persist updated world state, refresh self.current, and enforce Attention Lock."""
        prev_focus = self.current.focus_level
        base = snapshot.model_dump() if snapshot is not None else self.current.model_dump()
        if current_module is not None:
            base["current_module"] = sanitize_sensor_text(
                current_module, self.settings, max_tokens=16
            )
        if focus_level is not None:
            base["focus_level"] = focus_level
        if mood is not None:
            base["mood"] = sanitize_sensor_text(mood, self.settings, max_tokens=16)
        if energy is not None:
            base["energy"] = energy
        if boredom is not None:
            base["boredom"] = boredom
        if last_proactive_at is not None:
            base["last_proactive_at"] = last_proactive_at
        if details is not None:
            base["details"] = _sanitize_payload(details, self.settings)
        else:
            base["details"] = _sanitize_payload(base.get("details") or {}, self.settings)
        base["updated_at"] = self.clock()
        next_snap = WorldStateSnapshot.model_validate(base)
        snapshot_json = json.dumps(next_snap.model_dump(), ensure_ascii=False)

        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE world_state SET current_module=?, focus_level=?, mood=?, "
                "energy=?, boredom=?, last_proactive_at=?, updated_at=?, snapshot_json=? "
                "WHERE singleton=1",
                (
                    next_snap.current_module,
                    next_snap.focus_level,
                    next_snap.mood,
                    next_snap.energy,
                    next_snap.boredom,
                    next_snap.last_proactive_at,
                    next_snap.updated_at,
                    snapshot_json,
                ),
            )
        self.current = next_snap
        if sensor_overrides is not None:
            self._sensor_overrides.clear()
            for key, val in sensor_overrides.items():
                self.set_sensor_override(key, val)
        await self._sync_attention_lock(prev_focus=prev_focus, new_focus=next_snap.focus_level)
        return self.current

    def proactive_suspended(self) -> bool:
        """Return True iff world focus_level is currently HIGH."""
        return self.current.focus_level == "HIGH"

    async def enqueue_intent(self, ticket: IntentTicketCreate, *, now: float | None = None) -> str:
        async with self.store.transaction():
            return await self.insert_intent_tx(ticket, now=self.clock() if now is None else now)

    async def insert_intent_tx(self, ticket: IntentTicketCreate, *, now: float) -> str:
        """Caller owns the transaction, so a rule receipt and its ticket commit together."""
        validated = IntentTicketCreate.model_validate(ticket)
        if validated.dedupe_key:
            existing = await self.store.one(
                "SELECT id FROM scheduled_intents WHERE dedupe_key=?", (validated.dedupe_key,)
            )
            if existing:
                return existing[0]
        intent_id = uuid.uuid4().hex
        gate_json = json.dumps(
            _sanitize_payload(validated.gate_condition, self.settings),
            ensure_ascii=False,
        )
        if isinstance(validated.intent_note, dict):
            note_obj: dict[str, Any] = _sanitize_payload(validated.intent_note, self.settings)
        elif validated.intent_note:
            note_obj = {"note": sanitize_sensor_text(validated.intent_note, self.settings)}
        else:
            note_obj = {}
        note_json = json.dumps(note_obj, ensure_ascii=False)

        await self.store.db.execute(
            "INSERT INTO scheduled_intents(id,created_at,execute_after,expire_at,target_module,"
            "target_scope,action_type,mood,gate_condition_json,intent_note_json,updated_at,"
            "action_name,arguments_json,origin,source_module,source_event_id,dedupe_key)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                intent_id,
                now,
                validated.execute_after,
                validated.expire_at,
                validated.target_module,
                validated.target_scope,
                validated.action_type,
                validated.mood,
                gate_json,
                note_json,
                now,
                validated.action_name,
                json.dumps(_sanitize_payload(validated.arguments, self.s), ensure_ascii=False),
                validated.origin,
                validated.source_module,
                validated.source_event_id,
                validated.dedupe_key,
            ),
        )
        return intent_id

    async def list_due_intents(
        self, now: float | None = None, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Return pending intents whose execution window is currently active."""
        current = self.clock() if now is None else now
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM scheduled_intents "
                "WHERE status='pending' AND execute_after <= ? AND expire_at > ? "
                "ORDER BY CASE WHEN origin='timer' AND action_name='qq.owner_digest' "
                "THEN 0 ELSE 1 END, "
                "execute_after,created_at LIMIT ?",
                (current, current, max(1, int(limit))),
            )
        return [self.intent_from_row(row) for row in rows]

    @staticmethod
    def intent_from_row(row) -> dict:
        ticket = dict(row)
        for key in ("arguments", "gate_condition", "intent_note"):
            ticket[key] = json.loads(ticket.pop(key + "_json") or "{}")
        return ticket

    async def claim(self, intent_id: str, now: float) -> dict | None:
        """Claim and load the same persisted action inside SQLite's write transaction."""
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE scheduled_intents SET status='executing',updated_at=? "
                "WHERE id=? AND status='pending' AND execute_after<=? AND expire_at>? RETURNING *",
                (now, intent_id, now, now),
            )
            row = await cur.fetchone()
            await cur.close()
            return self.intent_from_row(row) if row else None

    async def mark_intent(
        self,
        intent_id: str,
        status: str,
        *,
        now: float | None = None,
        reason: str = "",
        expected: str = "pending",
        retry_at: float | None = None,
    ) -> bool:
        if status not in _VALID_INTENT_STATUSES:
            raise ValueError("invalid intent status")
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE scheduled_intents SET status=?,result_reason=?,updated_at=?,"
                "execute_after=COALESCE(?,execute_after) WHERE id=? AND status=?",
                (
                    status,
                    sanitize_sensor_text(reason, self.s, max_tokens=32),
                    self.clock() if now is None else now,
                    retry_at,
                    intent_id,
                    expected,
                ),
            )
            return cur.rowcount == 1

    async def recover_actions(self) -> None:
        # Boot runs after QQ receipt application. A partial ACK cannot confirm a whole intent.
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE scheduled_intents SET status='done',"
                "result_reason='recovered_complete_receipts',"
                "updated_at=? WHERE status='executing' AND expected_bubbles>0 "
                "AND expected_bubbles="
                "(SELECT COUNT(*) FROM outbox_bubble_receipts "
                "WHERE reply_id='jit:'||scheduled_intents.id "
                "AND applied=1 AND position>=0 AND position<scheduled_intents.expected_bubbles)",
                (self.clock(),),
            )
            await self.store.db.execute(
                "UPDATE scheduled_intents SET status='uncertain', updated_at=?, result_reason=CASE "
                "WHEN EXISTS(SELECT 1 FROM outbox_bubble_receipts b "
                "WHERE b.reply_id='jit:'||scheduled_intents.id)"
                " THEN 'recovered_partial_receipt' ELSE 'interrupted_without_final_receipt' END "
                "WHERE status='executing' AND NOT (action_name IS 'qq.owner_digest' "
                "AND result_reason='owner_digest_awaiting_delivery' AND EXISTS ("
                "SELECT 1 FROM owner_digest_intents i JOIN pending_owner_dms p ON p.id=i.digest_id "
                "WHERE i.intent_id=scheduled_intents.id AND p.status='pending'))",
                (self.clock(),),
            )

    def subscribe(self, handler: Callable[[MilestoneEvent], Any]) -> None:
        """Register an in-process listener invoked when milestone events are published."""
        if handler not in self._subscribers:
            self._subscribers.append(handler)

    async def publish_event(
        self,
        source_module: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        importance: Literal["low", "normal", "high"] = "normal",
        *,
        now: float | None = None,
        business_key: str | None = None,
    ) -> MilestoneEvent:
        """Sanitize, persist to global_events as plaintext JSON, and notify subscribers."""
        sanitized_payload = _sanitize_payload(payload or {}, self.settings)
        event = MilestoneEvent(
            id=(
                self.store.vault.digest(
                    "life-event", f"{source_module}:{event_type}:{business_key}"
                )
                if business_key
                else uuid.uuid4().hex
            ),
            created_at=self.clock() if now is None else now,
            source_module=source_module,
            event_type=event_type,
            importance=importance,
            consumed=False,
            payload=sanitized_payload,
        )
        payload_json = json.dumps(event.payload, ensure_ascii=False)
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "INSERT OR IGNORE INTO global_events("
                "id, created_at, source_module, event_type, importance, consumed, payload_json"
                ") VALUES (?,?,?,?,?,?,?)",
                (
                    event.id,
                    event.created_at,
                    event.source_module,
                    event.event_type,
                    event.importance,
                    0,
                    payload_json,
                ),
            )
            if not cur.rowcount:
                row = await self.store.one("SELECT * FROM global_events WHERE id=?", (event.id,))
                return self.event_from_row(row)
        for handler in list(self._subscribers):
            try:
                res = handler(event)
                if inspect.isawaitable(res):
                    await res
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("event=world_event_subscriber_failed")
        return event

    async def unconsumed_events(self, limit: int = 50) -> list[MilestoneEvent]:
        """Load unconsumed MilestoneEvents from SQLite ordered oldest-first."""
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM global_events WHERE consumed=0 ORDER BY created_at ASC LIMIT ?",
                (max(1, int(limit)),),
            )
        return [self.event_from_row(row) for row in rows]

    @staticmethod
    def event_from_row(row) -> MilestoneEvent:
        return MilestoneEvent(
            id=row["id"],
            created_at=float(row["created_at"]),
            source_module=row["source_module"],
            event_type=row["event_type"],
            importance=row["importance"],
            consumed=bool(row["consumed"]),
            payload=json.loads(row["payload_json"]),
        )

    async def mark_event_consumed(self, event_id: str) -> bool:
        """Mark a global event row as consumed."""
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE global_events SET consumed=1 WHERE id=?",
                (event_id,),
            )
            return cur.rowcount > 0

    async def record_group_proactive(self, scope: str, now: float | None = None) -> None:
        """Record the timestamp of the latest proactive group interaction for cooldown gating."""
        ts = self.clock() if now is None else now
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO group_proactive_cooldown(scope, last_proactive_at) VALUES (?, ?) "
                "ON CONFLICT(scope) DO UPDATE SET last_proactive_at=excluded.last_proactive_at",
                (scope, ts),
            )

    async def group_cooldown_ok(
        self, scope: str, cooldown_seconds: float, now: float | None = None
    ) -> bool:
        """Return True when the group scope is past its proactive cooldown window."""
        ts = self.clock() if now is None else now
        async with self.store.lock:
            row = await self.store.one(
                "SELECT last_proactive_at FROM group_proactive_cooldown WHERE scope=?",
                (scope,),
            )
        if row is None:
            return True
        return (ts - float(row["last_proactive_at"])) >= cooldown_seconds

    def _active_sensor_overrides(self) -> dict[str, str]:
        overrides = {k: v for k, v in self._sensor_overrides.items() if v}
        detail_overrides = self.current.details.get("sensor_overrides")
        if isinstance(detail_overrides, dict):
            for k, v in detail_overrides.items():
                cleaned = sanitize_sensor_text(str(v), self.settings, max_tokens=60)
                if cleaned:
                    overrides[sanitize_sensor_text(str(k), self.settings, max_tokens=16)] = cleaned
        return overrides

    async def chat_sensor_payload(self) -> dict[str, Any] | None:
        """Return compact <=100-token sensor payload, or None on idle/LOW baseline."""
        overrides = self._active_sensor_overrides()
        if (
            self.current.current_module == "idle"
            and self.current.focus_level == "LOW"
            and not overrides
        ):
            return None

        sensor_parts: list[str] = []
        if overrides:
            sensor_parts.extend(overrides.values())
        elif self.registry is not None:
            active_mod = (
                self.registry.get(self.current.current_module)
                if self.current.current_module != "idle"
                else None
            )
            if active_mod is not None:
                summary = await active_mod.get_sensor_summary()
                if summary:
                    sensor_parts.append(summary)
            else:
                summaries = await self.registry.collect_sensor_summaries()
                ordered = sorted(
                    summaries.items(),
                    key=lambda item: ATTENTION_RANK.get(
                        getattr(self.registry.get(item[0]), "attention_level", "LOW"), 0
                    ),
                    reverse=True,
                )
                for _, summary in ordered:
                    if summary:
                        sensor_parts.append(summary)

        sensor_text = sanitize_sensor_text(" | ".join(sensor_parts), self.settings, max_tokens=45)
        payload: dict[str, Any] = {
            "current_module": sanitize_sensor_text(
                self.current.current_module, self.settings, max_tokens=16
            )
            or "idle",
            "focus_level": self.current.focus_level,
            "mood": sanitize_sensor_text(self.current.mood, self.settings, max_tokens=16) or "calm",
            "energy": self.current.energy,
            "boredom": self.current.boredom,
            "sensor": sensor_text,
        }
        if self.current.focus_level == "HIGH":
            payload["attention_note"] = ATTENTION_SWITCH_NOTE

        def _encoded_tokens() -> int:
            return estimate_tokens(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

        if _encoded_tokens() > 100 and payload["sensor"]:
            lo, hi = 0, len(payload["sensor"])
            orig = payload["sensor"]
            while lo < hi:
                mid = (lo + hi + 1) // 2
                payload["sensor"] = truncate_tokens(orig, mid) if mid > 0 else ""
                if _encoded_tokens() <= 100:
                    lo = mid
                else:
                    hi = mid - 1
            payload["sensor"] = truncate_tokens(orig, lo) if lo > 0 else ""
        if _encoded_tokens() > 100:
            payload["sensor"] = ""
        if _encoded_tokens() > 100:
            payload["mood"] = truncate_tokens(payload["mood"], 6)
        if _encoded_tokens() > 100:
            payload["current_module"] = truncate_tokens(payload["current_module"], 6)
        return payload
