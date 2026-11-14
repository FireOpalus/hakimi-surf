"""Durable per-session debounce and a bounded, encrypted nearby-group window."""

import html
import json
import time

from .attention import WAKE_WORDS
from .bot_server import is_persona_owner
from .models import Message
from .security import authorize

SCHEMA = """
CREATE TABLE IF NOT EXISTS group_debounce_batches (
 id TEXT PRIMARY KEY, scope TEXT NOT NULL, first_at REAL NOT NULL,
 ready_at REAL NOT NULL, closed INTEGER NOT NULL DEFAULT 0,
 count INTEGER NOT NULL, chars INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS group_debounce_scope ON group_debounce_batches(scope,closed,ready_at);
CREATE TABLE IF NOT EXISTS group_debounce_items (
 job_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS group_debounce_items_batch ON group_debounce_items(batch_id);
CREATE TABLE IF NOT EXISTS group_recent_messages (
 event TEXT PRIMARY KEY, scope TEXT NOT NULL, at REAL NOT NULL,
 event_at REAL NOT NULL, payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS group_recent_scope ON group_recent_messages(scope,at);
"""
MAX_BATCH_MESSAGES = 8
MAX_BATCH_CHARS = 8000
CONTEXT_TTL = 300


def other_mentions(message: Message | None) -> bool:
    """Genuine top-level mentions only; replying to a bot or sharing a link is not an exemption."""
    if message is None or message.platform != "onebot" or message.kind != "group":
        return False
    if not message.mentioned_ids or message.bot_id in message.mentioned_ids:
        return False
    direct = message.command_text if message.command_text is not None else message.text
    return not any(word in direct for word in WAKE_WORDS)


def direct_command(message):
    direct = message.command_text if message.command_text is not None else message.text
    return direct.lstrip().startswith("/")


def record_line(message):
    # Both nickname and text remain JSON data; only actual protocol ids establish authorship.
    return json.dumps(
        {
            "sender_id": message.user_id,
            "sender_nickname": message.sender_nickname,
            "text": message.text,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


class GroupPipeline:
    def __init__(self, store, *, clock=None):
        self.store, self.s = store, store.s
        self.clock = clock or time.time

    async def initialize(self):
        await self.store.db.executescript(SCHEMA)

    def message_size(self, message):
        return (
            len(
                html.escape(
                    json.dumps(record_line(message), ensure_ascii=False), quote=False
                ).encode()
            )
            + 2
        )

    async def continuation_in_transaction(self, message, now):
        """Only an already addressed, still-open batch may absorb an at_only follow-up."""
        if (
            message.kind != "group"
            or other_mentions(message)
            or direct_command(message)
            or not authorize(message.model_copy(update={"addressed": True}), self.s)
            or self.s.group_debounce_seconds <= 0
            or self.s.group_debounce_max_seconds <= 0
        ):
            return False
        batch = await self.store.one(
            "SELECT b.* FROM group_debounce_batches b WHERE scope=? AND closed=0 "
            "AND ready_at>? ORDER BY first_at DESC,rowid DESC LIMIT 1",
            (self.store.session_scope(message), now),
        )
        if (
            not batch
            or batch["count"] >= MAX_BATCH_MESSAGES
            or batch["chars"] + self.message_size(message)
            > min(MAX_BATCH_CHARS, self.s.max_context_bytes // 2)
        ):
            return False
        rows = await self.store.all(
            "SELECT j.id,j.payload FROM jobs j JOIN group_debounce_items i ON i.job_id=j.id "
            "WHERE i.batch_id=? AND j.status='pending' AND j.expires>=?",
            (batch["id"], now),
        )
        # A historical smart_reply batch cannot turn into an at_only trigger after a
        # policy edit. The invitation must come from an actual admitted original.
        return any(self.store.vault.open(r["id"], r["payload"])["addressed"] for r in rows)

    async def can_continue(self, message):
        # Advisory only; Store.admit repeats this check atomically with job insertion.
        async with self.store.lock:
            return await self.continuation_in_transaction(message, self.clock())

    async def register_in_transaction(self, message, job_id, now):
        scope = self.store.session_scope(message)
        delay = self.s.group_debounce_seconds
        maximum = self.s.group_debounce_max_seconds
        standalone = direct_command(message) or self.store.bot_guard.known_bot(message)
        if standalone:
            # Older group work must become eligible before a subsequent reset/command.
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET ready_at=MIN(ready_at,?),closed=1 "
                "WHERE scope=? AND closed=0",
                (now, scope),
            )
        if delay <= 0 or maximum <= 0:
            return
        # Count the escaped UTF-8 representation as it will enter current_message,
        # reserving half the main context limit for provenance/history and framing.
        limit = min(MAX_BATCH_CHARS, self.s.max_context_bytes // 2)
        size = self.message_size(message)
        if size > limit:
            return  # Never trim content before the safety gate just to fit a batch.
        busy = bool(
            await self.store.one(
                "SELECT 1 FROM jobs WHERE scope=? AND status='working' LIMIT 1", (scope,)
            )
        )
        batch = (
            None
            if standalone
            else await self.store.one(
                "SELECT b.* FROM group_debounce_batches b WHERE scope=? AND closed=0 "
                "AND (ready_at>? OR ?=1) AND EXISTS "
                "(SELECT 1 FROM group_debounce_items i JOIN jobs j "
                "ON j.id=i.job_id WHERE i.batch_id=b.id AND j.status='pending') "
                "ORDER BY first_at DESC,rowid DESC LIMIT 1",
                (scope, now, int(busy)),
            )
        )
        if batch and (batch["count"] >= MAX_BATCH_MESSAGES or batch["chars"] + size > limit):
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET closed=1,ready_at=MIN(ready_at,?) WHERE id=?",
                (now, batch["id"]),
            )
            batch = None
        if batch:
            batch_id = batch["id"]
            ready_at = min(now + delay, batch["first_at"] + maximum)
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET ready_at=?,count=count+1,chars=chars+? "
                "WHERE id=?",
                (ready_at, size, batch_id),
            )
        else:
            batch_id = job_id
            await self.store.db.execute(
                "INSERT INTO group_debounce_batches VALUES (?,?,?,?,0,1,?)",
                (batch_id, scope, now, now + min(delay, maximum), size),
            )
        await self.store.db.execute(
            "INSERT INTO group_debounce_items VALUES (?,?)", (job_id, batch_id)
        )
        if standalone:
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET closed=1 WHERE id=?", (batch_id,)
            )

    async def claim_in_transaction(self, row):
        item = await self.store.one(
            "SELECT batch_id FROM group_debounce_items WHERE job_id=?", (row["id"],)
        )
        job = dict(row)
        job["payload"] = self.store.vault.open(row["id"], row["payload"])
        if not item:
            await self.store.db.execute("UPDATE jobs SET status='working' WHERE id=?", (row["id"],))
            return job
        batch_id = item["batch_id"]
        rows = await self.store.all(
            "SELECT j.* FROM jobs j JOIN group_debounce_items i ON i.job_id=j.id "
            "WHERE i.batch_id=? AND j.status='pending' ORDER BY j.created,j.rowid",
            (batch_id,),
        )
        messages = [
            Message.model_validate(self.store.vault.open(r["id"], r["payload"])) for r in rows
        ]
        await self.store.db.execute(
            "UPDATE jobs SET status='working' WHERE id IN "
            "(SELECT job_id FROM group_debounce_items WHERE batch_id=?) AND status='pending'",
            (batch_id,),
        )
        await self.store.db.execute(
            "UPDATE group_debounce_batches SET closed=1 WHERE id=?", (batch_id,)
        )
        job["batch_messages"] = [m.model_dump() for m in messages]
        job["expires"] = min(r["expires"] for r in rows)
        if len(messages) > 1:
            ordinary = [m for m in messages if not is_persona_owner(m, self.s)]
            anchor = (ordinary or messages)[-1]
            addressed = next((m for m in messages if m.addressed), None)

            def unique(field, limit):
                items = []
                for m in messages:
                    for value in getattr(m, field):
                        if value not in items:
                            items.append(value)
                return items[:limit]

            merged = anchor.model_copy(
                update={
                    "text": "\n".join(record_line(m) for m in messages),
                    "command_text": "",  # No stitched input can become a privileged slash command.
                    "event_id": Message.model_validate(job["payload"]).event_id,
                    "created_at": min(m.created_at for m in messages),
                    "addressed": bool(addressed),
                    "attention_reason": addressed.attention_reason if addressed else "",
                    "images": unique("images", 2),
                    "forward_ids": unique("forward_ids", 2),
                    "video_ids": unique("video_ids", 3),
                    "short_urls": unique("short_urls", 3),
                }
            )
            job["payload"] = merged.model_dump()
            job["subject"] = self.store.subject(merged)
        return job

    async def finish_in_transaction(self, job_id, status):
        await self.store.db.execute(
            "UPDATE jobs SET status=?,payload=NULL WHERE status='working' AND id IN "
            "(SELECT job_id FROM group_debounce_items WHERE batch_id="
            "(SELECT batch_id FROM group_debounce_items WHERE job_id=?))",
            (status, job_id),
        )

    async def observe(self, message):
        if (
            message.kind != "group"
            or other_mentions(message)
            or not authorize(message.model_copy(update={"addressed": True}), self.s)
        ):
            return
        scope, event = self.store.session_scope(message), self.store.event_key(message)
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR IGNORE INTO group_recent_messages VALUES (?,?,?,?,?)",
                (
                    event,
                    scope,
                    now,
                    message.created_at,
                    self.store.vault.seal("group-recent:" + event, message.model_dump()),
                ),
            )
            await self.store.db.execute(
                "DELETE FROM group_recent_messages WHERE at<?", (now - CONTEXT_TTL,)
            )
            await self.store.db.execute(
                "DELETE FROM group_recent_messages WHERE scope=? AND event NOT IN "
                "(SELECT event FROM group_recent_messages WHERE scope=? "
                "ORDER BY at DESC,rowid DESC LIMIT 30)",
                (scope, scope),
            )
            await self.store.db.execute(
                "DELETE FROM group_recent_messages WHERE rowid NOT IN "
                "(SELECT rowid FROM group_recent_messages ORDER BY at DESC,rowid DESC LIMIT 1000)"
            )

    async def previous_messages(self, message, batch_messages=None, *, limit=3):
        if message.kind != "group":
            return []
        originals = (
            [Message.model_validate(m) for m in batch_messages] if batch_messages else [message]
        )
        first = originals[0]
        scope, event = self.store.session_scope(first), self.store.event_key(first)
        async with self.store.lock:
            boundary = await self.store.one(
                "SELECT at,rowid FROM group_recent_messages WHERE event=? AND scope=?",
                (event, scope),
            )
            at, rowid = (
                (boundary[0], boundary[1]) if boundary else (self.clock(), 9223372036854775807)
            )
            rows = await self.store.all(
                "SELECT event,payload FROM group_recent_messages WHERE scope=? AND at>=? "
                "AND event_at<=? AND (at<? OR (at=? AND rowid<?)) "
                "ORDER BY at DESC,rowid DESC LIMIT ?",
                (
                    scope,
                    self.clock() - CONTEXT_TTL,
                    first.created_at,
                    at,
                    at,
                    rowid,
                    max(0, min(3, limit)),
                ),
            )
        return [
            Message.model_validate(
                self.store.vault.open("group-recent:" + r["event"], r["payload"])
            )
            for r in reversed(rows)
        ]

    async def clear_recent(self, message):
        scope = self.store.session_scope(message)
        async with self.store.transaction():
            await self.store.db.execute(
                "DELETE FROM group_recent_messages WHERE scope=?",
                (scope,),
            )
            # A reset also discards accepted inputs that have not started processing.
            # Keep working jobs and their batch references for epoch checks and finish().
            await self.store.db.execute(
                "UPDATE jobs SET status='cancelled',payload=NULL "
                "WHERE kind='message' AND scope=? AND status='pending'",
                (scope,),
            )
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET closed=1 WHERE scope=?",
                (scope,),
            )

    async def prune_in_transaction(self, now):
        await self.store.db.execute(
            "DELETE FROM group_recent_messages WHERE at<?", (now - CONTEXT_TTL,)
        )
        await self.store.db.execute(
            "DELETE FROM group_debounce_batches WHERE first_at<? AND NOT EXISTS "
            "(SELECT 1 FROM group_debounce_items i JOIN jobs j ON j.id=i.job_id "
            "WHERE i.batch_id=group_debounce_batches.id AND j.status IN ('pending','working'))",
            (now - 86400,),
        )
        await self.store.db.execute(
            "DELETE FROM group_debounce_items WHERE batch_id NOT IN "
            "(SELECT id FROM group_debounce_batches)"
        )
