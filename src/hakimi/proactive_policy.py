"""Local-only scheduling policy. No message or model output can change it."""

import re


def minute_of_day(value: str) -> int:
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
        raise ValueError("Use HH:MM")
    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


def hours_bounds(value: str) -> tuple[int, int]:
    parts = value.split("-")
    if len(parts) != 2 or parts[0] == parts[1]:
        raise ValueError("Use two distinct HH:MM times")
    return minute_of_day(parts[0]), minute_of_day(parts[1])


def within_hours(minute: int, bounds: tuple[int, int]) -> bool:
    start, end = bounds
    return start <= minute < end if start < end else minute >= start or minute < end


SCHEMA = """
CREATE TABLE IF NOT EXISTS operational_health (
  provider TEXT PRIMARY KEY, failures INTEGER NOT NULL DEFAULT 0, changed REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS owner_dm_state (
  scope TEXT PRIMARY KEY, last_private_at REAL NOT NULL DEFAULT 0,
  ever_online INTEGER NOT NULL DEFAULT 0, online INTEGER NOT NULL DEFAULT 0,
  disconnected_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS proactive_days (
  scope TEXT NOT NULL, day TEXT NOT NULL, PRIMARY KEY(scope,day)
);
CREATE TABLE IF NOT EXISTS proactive_schedule (
  id TEXT PRIMARY KEY, scope TEXT NOT NULL, day TEXT NOT NULL, kind TEXT NOT NULL,
  due_at REAL NOT NULL, expires_at REAL NOT NULL, status TEXT NOT NULL,
  reason TEXT, UNIQUE(scope,day,kind,id)
);
CREATE INDEX IF NOT EXISTS proactive_due ON proactive_schedule(status,due_at);
CREATE TABLE IF NOT EXISTS pending_owner_dms (
  id TEXT PRIMARY KEY, scope TEXT NOT NULL, day TEXT NOT NULL, kind TEXT NOT NULL,
  payload BLOB, created REAL NOT NULL, expires_at REAL NOT NULL, status TEXT NOT NULL,
  session_epoch INTEGER, sent_at REAL, receipt BLOB, reason TEXT
);
CREATE INDEX IF NOT EXISTS owner_dm_delivery ON pending_owner_dms(status,created);
CREATE TABLE IF NOT EXISTS owner_digest_receipts (
  id TEXT PRIMARY KEY, generated_at REAL, payload BLOB, submitted_at REAL
);
CREATE TABLE IF NOT EXISTS owner_digest_intents (
  intent_id TEXT PRIMARY KEY, digest_id TEXT NOT NULL, scheduled_for REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS owner_digest_intent_lookup ON owner_digest_intents(digest_id);
CREATE TABLE IF NOT EXISTS alert_cooldown (
  alert_key TEXT PRIMARY KEY, queued_at REAL NOT NULL, cooldown_until REAL NOT NULL,
  dm_id TEXT NOT NULL, sent_at REAL
);
CREATE TABLE IF NOT EXISTS budget_totals (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), amount INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_times (id TEXT PRIMARY KEY, created REAL NOT NULL);
CREATE INDEX IF NOT EXISTS ledger_times_created ON ledger_times(created);
"""
