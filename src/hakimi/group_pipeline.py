"""Durable per-session debounce and a bounded, encrypted nearby-group window."""

import asyncio
import html
import json
import re
import time
from dataclasses import dataclass, field

from .attention import WAKE_WORDS
from .bot_server import is_persona_owner
from .debounce_policy import DebounceConfig
from .models import Message
from .security import authorize

SCHEMA = """
CREATE TABLE IF NOT EXISTS message_recalls (event TEXT PRIMARY KEY, at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS local_quick_acks (scope TEXT PRIMARY KEY, at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS outbox_bubble_receipts (
 reply_id TEXT NOT NULL, position INTEGER NOT NULL, payload BLOB NOT NULL,
 created REAL NOT NULL, applied INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(reply_id,position));
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
MAX_BATCH_MESSAGES = 16
MAX_BATCH_CHARS = 24000
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
            "event_id": message.event_id,
            "sender_id": message.user_id,
            "sender_nickname": message.sender_nickname,
            "text": message.text,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


@dataclass
class ActiveBatch:
    job: dict
    members: list
    phase: str = "gate"
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    reason: str = ""
    sealed: bool = False


class GroupPipeline:
    def __init__(self, store, *, clock=None):
        self.store, self.s = store, store.s
        self.clock = clock or time.time
        self.active = {}

    @property
    def config(self):
        return self.store.runtime.debounce_override or DebounceConfig.from_settings(self.s)

    def delay(self, message, config=None):
        config = config or self.config
        explicit = message.attention_reason in {"mention", "reply"} or message.kind == "private"
        raw = message.command_text if message.command_text is not None else message.text
        spoken = re.sub(r"\[(?:图片|商城贴纸|提及机器人|提及群友|附图)\]", "", raw).strip()
        pure_image = bool(message.images) and not spoken
        incomplete = pure_image or (
            not message.images
            and bool(
                re.search(
                    r"(?:你看这个(?:报错|图|问题)?|看下这个|等下|等等|还有|比如|例如|看这里)"
                    r"[：:，,。 ]*$",
                    raw,
                )
            )
        )
        if incomplete:
            return (
                config.mention_incomplete_seconds
                if explicit
                else config.ordinary_incomplete_seconds
            )
        return config.mention_debounce_seconds if explicit else config.group_debounce_seconds

    async def retime_pending_in_transaction(self, config):
        """Re-time only open, unclaimed work; never interrupt an in-flight reply."""
        now = self.clock()
        batches = await self.store.all(
            "SELECT * FROM group_debounce_batches WHERE closed=0 AND ready_at>?", (now,)
        )
        for batch in batches:
            rows = await self.store.all(
                "SELECT j.* FROM jobs j JOIN group_debounce_items i ON i.job_id=j.id "
                "WHERE i.batch_id=? ORDER BY j.created DESC,j.rowid DESC",
                (batch["id"],),
            )
            if not rows or any(row["status"] != "pending" for row in rows):
                continue
            latest = rows[0]
            message = Message.model_validate(self.store.vault.open(latest["id"], latest["payload"]))
            ready = min(
                latest["created"] + self.delay(message, config),
                batch["first_at"] + config.group_debounce_max_seconds,
            )
            if config.group_debounce_seconds <= 0:
                ready = now
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET ready_at=? WHERE id=?", (ready, batch["id"])
            )

    def begin(self, job):
        members = [Message.model_validate(m) for m in job.get("batch_messages", [])]
        members = members or [Message.model_validate(job["payload"])]
        batch = ActiveBatch(job, members)
        self.active[job["scope"]] = batch
        return batch

    def phase(self, message, value):
        batch = self.active.get(self.store.session_scope(message))
        if batch:
            batch.phase = value

    def interrupt(self, message):
        batch = self.active.get(self.store.session_scope(message))
        if (
            batch
            and batch.phase == "generating"
            and not batch.sealed
            and not direct_command(message)
            and not self.store.bot_guard.known_bot(message)
            and message.user_id
            == next((m.user_id for m in batch.members if m.addressed), batch.members[0].user_id)
            and not any(
                direct_command(m) or self.store.bot_guard.known_bot(m) for m in batch.members
            )
        ):
            batch.reason = "followup"
            batch.cancel.set()

    def recall(self, message, event_id):
        batch = self.active.get(self.store.session_scope(message))
        if batch and any(event_id in {m.event_id, *m.reply_ids} for m in batch.members):
            batch.reason = "recall"
            batch.cancel.set()

    async def state(self, message):
        scope = self.store.session_scope(message)
        if scope in self.active:
            return self.active[scope].phase
        async with self.store.lock:
            row = await self.store.one(
                "SELECT 1 FROM jobs WHERE scope=? AND status='pending' LIMIT 1", (scope,)
            )
        return "debouncing" if row else "idle"

    async def requeue_interrupted(self, job):
        # Old originals already passed the gate and are in append-only B. Keep at most
        # six current originals; superseded older ones remain visible as history.
        async with self.store.transaction():
            rows = await self.store.all(
                "SELECT * FROM jobs WHERE scope=? AND status IN ('working','pending') "
                "ORDER BY created,rowid",
                (job["scope"],),
            )
            eligible = []
            for row in rows:
                msg = Message.model_validate(self.store.vault.open(row["id"], row["payload"]))
                if direct_command(msg) or self.store.bot_guard.known_bot(msg):
                    break
                eligible.append((row, msg))
            if not eligible:
                return
            selected = eligible[-MAX_BATCH_MESSAGES:]
            batch_id = selected[0][0]["id"]
            now = self.clock()
            for row, _ in eligible[:-MAX_BATCH_MESSAGES]:
                await self.store.db.execute(
                    "UPDATE jobs SET status='superseded',payload=NULL WHERE id=?", (row["id"],)
                )
            await self.store.db.execute(
                "INSERT OR REPLACE INTO group_debounce_batches VALUES (?,?,?,?,0,?,?)",
                (
                    batch_id,
                    job["scope"],
                    now,
                    now + min(self.delay(selected[-1][1]), self.config.group_debounce_max_seconds),
                    len(selected),
                    sum(self.message_size(m) for _, m in selected),
                ),
            )
            for row, _ in selected:
                await self.store.db.execute(
                    "UPDATE jobs SET status='pending' WHERE id=?", (row["id"],)
                )
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO group_debounce_items VALUES (?,?)",
                    (row["id"], batch_id),
                )

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
            or self.config.group_debounce_seconds <= 0
            or self.config.group_debounce_max_seconds <= 0
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
        delay = self.delay(message)
        maximum = self.config.group_debounce_max_seconds
        standalone = direct_command(message) or self.store.bot_guard.known_bot(message)
        if standalone:
            # Older group work must become eligible before a subsequent reset/command.
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET ready_at=MIN(ready_at,?),closed=1 "
                "WHERE scope=? AND closed=0",
                (now, scope),
            )
        if self.config.group_debounce_seconds <= 0 or delay <= 0 or maximum <= 0:
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
        count = (batch["count"] + 1) if batch else 1
        if (
            standalone
            or count >= MAX_BATCH_MESSAGES
            or now >= (batch["first_at"] + maximum if batch else now + maximum)
        ):
            await self.store.db.execute(
                "UPDATE group_debounce_batches SET closed=1,ready_at=MIN(ready_at,?) WHERE id=?",
                (now, batch_id),
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
            addressed = next(
                (m for m in messages if m.attention_reason == "mention"), None
            ) or next((m for m in messages if m.addressed), None)

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
                    "forward_ids": unique("forward_ids", 4),
                    "video_ids": unique("video_ids", 5),
                    "short_urls": unique("short_urls", 5),
                    "context_status": unique("context_status", 5),
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
            or self.store.bot_guard.known_bot(message)
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
                "ORDER BY at DESC,rowid DESC LIMIT 50)",
                (scope, scope),
            )
            await self.store.db.execute(
                "DELETE FROM group_recent_messages WHERE rowid NOT IN "
                "(SELECT rowid FROM group_recent_messages ORDER BY at DESC,rowid DESC LIMIT 1000)"
            )

    async def previous_messages(self, message, batch_messages=None, *, limit=6):
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
                    max(0, min(8, limit)),
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
