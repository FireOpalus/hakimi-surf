"""Verify quoted bot replies using successful transport receipts, never quoted author text."""

import re
import time

from .security import authorize

SCHEMA = """
CREATE TABLE IF NOT EXISTS sent_bot_receipts (
  id TEXT PRIMARY KEY, created REAL NOT NULL);
CREATE INDEX IF NOT EXISTS sent_bot_receipts_created ON sent_bot_receipts(created);
"""
RETENTION_SECONDS = 7 * 86400
WAKE_WORDS = ("吉米奈喵", "吉米奈", "哈基米", "大小姐")


def direct_attention(kind, *, mentioned=False, text="", has_video=False):
    if kind == "private":
        return "private"
    if mentioned:
        return "mention"
    if any(alias in text for alias in WAKE_WORDS):
        return "alias"
    return "bilibili" if has_video else ""


class AttentionRouter:
    def __init__(self, store):
        self.store = store

    async def initialize(self):
        await self.store.db.executescript(SCHEMA)

    def key(self, message, message_id):
        return self.store.vault.digest(
            "sent-bot-receipt", *message.session_parts(self.store.s.namespace), message_id
        )

    async def record_sent(self, message, receipt):
        if message.platform != "onebot" or message.kind != "group" or not isinstance(receipt, dict):
            return
        raw = receipt.get("message_id")
        if isinstance(raw, bool) or not re.fullmatch(r"-?[0-9]{1,30}", str(raw)):
            return
        now = time.time()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR REPLACE INTO sent_bot_receipts VALUES (?,?)",
                (self.key(message, str(raw)), now),
            )
            await self.store.db.execute(
                "DELETE FROM sent_bot_receipts WHERE created<?", (now - RETENTION_SECONDS,)
            )
            await self.store.db.execute(
                "DELETE FROM sent_bot_receipts WHERE id NOT IN "
                "(SELECT id FROM sent_bot_receipts ORDER BY created DESC LIMIT 10000)"
            )
        return True

    async def resolve(self, message):
        if (
            message is None
            or message.platform != "onebot"
            or message.kind != "group"
            or message.addressed
            or not message.reply_ids
        ):
            return message
        candidate = message.model_copy(update={"addressed": True, "attention_reason": "reply"})
        # Unknown groups, muted users and blocked bots cannot use quotes to bypass admission.
        if not authorize(candidate, self.store.s):
            return message
        async with self.store.lock:
            for reply_id in message.reply_ids:
                row = await self.store.one(
                    "SELECT created FROM sent_bot_receipts WHERE id=?",
                    (self.key(message, reply_id),),
                )
                if row and row[0] >= time.time() - RETENTION_SECONDS:
                    return candidate
        return message
