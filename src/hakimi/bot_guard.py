"""Explicit Owner invitation with a durable two-turn, ten-minute group circuit breaker."""

import re
import time
from typing import TYPE_CHECKING

from .admission_policy import effective_bot_reply_threshold, is_bot_hard_blocked
from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
from .security import Rejected, authorize, register_bot_authorizer

if TYPE_CHECKING:
    from .store import Store

PERMIT_SECONDS = 120
COOLDOWN_SECONDS = 600
MAX_TURNS = 2

# Retain legacy tables for old databases/admin queries; they never authorize new events.
SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_interactions (
 scope TEXT PRIMARY KEY, target TEXT NOT NULL, topic TEXT NOT NULL,
 authorized_by TEXT NOT NULL, permit_expires REAL NOT NULL,
 consumed INTEGER NOT NULL CHECK(consumed IN (0,1)),
 cooldown_until REAL NOT NULL, issued_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_turns (
 job_id TEXT PRIMARY KEY, scope TEXT NOT NULL, target TEXT NOT NULL,
 topic TEXT NOT NULL, authorized_by TEXT NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_dialogue_windows (
 scope TEXT PRIMARY KEY, target TEXT NOT NULL, authorized_by TEXT NOT NULL,
 started REAL NOT NULL, expires REAL NOT NULL, used INTEGER NOT NULL DEFAULT 0,
 permit_until REAL NOT NULL, issued_at REAL NOT NULL, invitation TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bot_authorized_events (
 job_id TEXT PRIMARY KEY, scope TEXT NOT NULL, target TEXT NOT NULL,
 authorized_by TEXT NOT NULL, expires REAL NOT NULL, window_started REAL NOT NULL
);
"""


class BotInteractionGuard:
    def __init__(self, store: "Store", settings: Settings):
        self.store, self.s = store, settings
        self._events = {}
        register_bot_authorizer(settings, self.authorize_cached)

    def known_bot(self, message: Message) -> bool:
        return message.platform == "onebot" and message.user_id in self.s.bot_qq_ids

    def reply_threshold(self, message: Message) -> float:
        return effective_bot_reply_threshold(message.user_id, self.s)

    def hard_blocked(self, message: Message) -> bool:
        return is_bot_hard_blocked(message.user_id, self.s)

    def owner_key(self) -> str:
        return self.store.vault.digest(
            "bot-authorizer", self.s.namespace, self.s.onebot_self_id, self.s.owner_qq_id
        )

    @staticmethod
    def event_identity(message):
        return (
            message.platform,
            message.bot_id,
            message.kind,
            message.conversation_id,
            message.user_id,
            message.event_id,
        )

    def authorize_cached(self, message):
        entry = self._events.get(self.event_identity(message))
        return bool(
            message.kind == "group"
            and entry is not None
            and entry[0] > time.time()
            and (entry[1] == "owner" or not self.hard_blocked(message))
        )

    def is_owner_invited(self, message: Message) -> bool:
        entry = self._events.get(self.event_identity(message))
        return bool(
            self.known_bot(message)
            and message.kind == "group"
            and entry is not None
            and entry[0] > time.time()
            and entry[1] == "owner"
        )

    def _cache(self, message, expires, *, mode="owner"):
        now = time.time()
        self._events = {k: v for k, v in self._events.items() if v[0] > now}
        if len(self._events) >= 512:
            self._events.pop(next(iter(self._events)))
        self._events[self.event_identity(message)] = (expires, mode)

    def _group_allowed(self, message):
        return (
            self.known_bot(message)
            and message.kind == "group"
            and authorize(message, self.s, _bot_exempt=True)
        )

    def _base_allowed(self, message):
        return self._group_allowed(message) and bool(self.s.owner_qq_id)

    def invitation_target(self, message):
        """Only a real Owner's direct imperative and one genuine top-level @ qualifies."""
        if (
            not is_persona_owner(message, self.s)
            or message.kind != "group"
            or not authorize(message, self.s)
        ):
            return None
        direct = (message.command_text or "").strip()
        # Conservatively refuse negations, quoted examples, questions and ambiguous invitations.
        if not direct or re.search(
            r"不要|别|禁止|不许|不用|停止|取消|如果|假如|假设|比如|例如|转述|[？?\n\"“”]",
            direct,
        ):
            return None
        explicit = bool(
            re.match(r"^/bot-interact\s+\S", direct)
            or re.match(
                r"^(?:(?:哈基米|吉米奈喵|吉米奈|大小姐)[，,：:\s]*)?"
                r"(?:去|请|帮我|麻烦|你和|你跟|跟它|跟他|跟她|和它|和他|和她)"
                r".{0,32}(?:互动|聊聊|聊天|聊一下|对话|打个招呼|打招呼|讨论)",
                direct,
            )
        )
        targets = set(message.mentioned_ids) - {self.s.onebot_self_id}
        if not explicit or len(targets) != 1:
            return None
        target = next(iter(targets))
        if target not in self.s.bot_qq_ids or target in getattr(self.s, "muted_user_ids", set()):
            return None
        return target

    async def invite(self, message: Message) -> set[str]:
        target = self.invitation_target(message)
        if target is None:
            return set()
        now = time.time()
        if message.created_at + self.s.event_ttl_seconds <= now:
            return set()
        scope = self.store.session_scope(message)
        candidate = message.model_copy(update={"user_id": target, "addressed": True})
        if not authorize(candidate, self.s, _bot_exempt=True):
            return set()
        async with self.store.transaction():
            row = await self.store.one("SELECT * FROM bot_dialogue_windows WHERE scope=?", (scope,))
            if row and row["expires"] > now:
                if row["used"] >= MAX_TURNS:
                    return set()
                started, expires, used = row["started"], row["expires"], row["used"]
            else:
                started, expires, used = now, now + COOLDOWN_SECONDS, 0
            await self.store.db.execute(
                "INSERT OR REPLACE INTO bot_dialogue_windows VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    scope,
                    self.store.subject(candidate),
                    self.owner_key(),
                    started,
                    expires,
                    used,
                    min(expires, now + PERMIT_SECONDS),
                    now,
                    self.store.event_key(message),
                ),
            )
        return {target}

    async def grant(self, message: Message) -> str:
        if not is_persona_owner(message, self.s) or message.kind != "group":
            raise Rejected("bot_owner_group_only")
        targets = await self.invite(message)
        if targets:
            return "已授权这位机器人本群互动；10分钟内最多2轮，仍需通过安全检查。"
        return "请真实 @ 一个目标并明确说要互动；本群达到2轮后需等待10分钟窗口结束。"

    async def _receipt(self, message, now, *, owner_only: bool = False):
        row = await self.store.one(
            "SELECT * FROM bot_authorized_events WHERE job_id=?", (self.store.event_key(message),)
        )
        if not row or (
            row["expires"] <= now
            or row["scope"] != self.store.session_scope(message)
            or row["target"] != self.store.subject(message)
        ):
            return None
        if self.s.owner_qq_id and row["authorized_by"] == self.owner_key():
            return row
        if (
            not owner_only
            and row["authorized_by"] == "threshold"
            and not self.hard_blocked(message)
        ):
            return row
        return None

    async def prepare(self, message: Message) -> bool:
        """Called at ingress (and restored job processing), before ordinary authorize()."""
        if not self.known_bot(message):
            return True
        if not self._group_allowed(message):
            return False
        now = time.time()
        if message.created_at + self.s.event_ttl_seconds <= now:
            return False
        async with self.store.lock:
            receipt = await self._receipt(message, now)
            if receipt:
                mode = "threshold" if receipt["authorized_by"] == "threshold" else "owner"
                self._cache(message, receipt["expires"], mode=mode)
                return True
            row = await self.available_in_transaction(message, now) if self.s.owner_qq_id else None
            if row is not None:
                expires = min(row["expires"], message.created_at + self.s.event_ttl_seconds)
                self._cache(message, expires, mode="owner")
                return True
        if self.hard_blocked(message):
            return False
        self._cache(message, message.created_at + self.s.event_ttl_seconds, mode="threshold")
        return True

    async def available_in_transaction(self, message: Message, now: float):
        if not self._base_allowed(message) or message.created_at + self.s.event_ttl_seconds <= now:
            return None
        row = await self.store.one(
            "SELECT * FROM bot_dialogue_windows WHERE scope=?", (self.store.session_scope(message),)
        )
        if (
            not row
            or row["used"] >= MAX_TURNS
            or row["expires"] <= now
            or row["permit_until"] <= now
            or row["target"] != self.store.subject(message)
            or row["authorized_by"] != self.owner_key()
            or message.created_at < int(row["issued_at"])
        ):
            return None
        return row

    async def consume_in_transaction(self, message: Message, job_id: str, now: float) -> bool:
        if not self.known_bot(message):
            return True
        if not self._group_allowed(message) or message.created_at + self.s.event_ttl_seconds <= now:
            return False
        row = await self.available_in_transaction(message, now) if self.s.owner_qq_id else None
        if row is not None:
            expires = min(row["expires"], message.created_at + self.s.event_ttl_seconds)
            await self.store.db.execute(
                "UPDATE bot_dialogue_windows SET used=used+1 WHERE scope=?", (row["scope"],)
            )
            await self.store.db.execute(
                "INSERT INTO bot_authorized_events VALUES (?,?,?,?,?,?)",
                (
                    job_id,
                    row["scope"],
                    row["target"],
                    row["authorized_by"],
                    expires,
                    row["started"],
                ),
            )
            self._cache(message, expires, mode="owner")
            return True
        if self.hard_blocked(message):
            return False
        expires = message.created_at + self.s.event_ttl_seconds
        await self.store.db.execute(
            "INSERT INTO bot_authorized_events VALUES (?,?,?,?,?,?)",
            (
                job_id,
                self.store.session_scope(message),
                self.store.subject(message),
                "threshold",
                expires,
                now,
            ),
        )
        self._cache(message, expires, mode="threshold")
        return True

    async def may_process(self, message: Message, job_id: str) -> bool:
        if not self.known_bot(message):
            return True
        if not self._group_allowed(message) or self.store.event_key(message) != job_id:
            return False
        async with self.store.lock:
            row = await self._receipt(message, time.time())
        if row:
            mode = "threshold" if row["authorized_by"] == "threshold" else "owner"
            self._cache(message, row["expires"], mode=mode)
        return bool(row)

    async def allowed_reply_target(self, message, target, *, invitation_event=None):
        """Recheck before sending: only this Owner invitation or consumed Bot turn may @Bot."""
        if target not in self.s.bot_qq_ids or message.kind != "group":
            return False
        candidate = message.model_copy(update={"user_id": target, "addressed": True})
        if not authorize(candidate, self.s, _bot_exempt=True):
            return False
        now = time.time()
        async with self.store.lock:
            if self.known_bot(message) and target == message.user_id:
                return bool(await self._receipt(message, now, owner_only=True))
            row = await self.store.one(
                "SELECT * FROM bot_dialogue_windows WHERE scope=?",
                (self.store.session_scope(message),),
            )
        return bool(
            is_persona_owner(message, self.s)
            and row
            and row["expires"] > now
            and row["permit_until"] > now
            and row["used"] < MAX_TURNS
            and row["target"] == self.store.subject(candidate)
            and row["authorized_by"] == self.owner_key()
            and row["invitation"] == (invitation_event or self.store.event_key(message))
        )

    async def extend_cooldown(self, message: Message, job_id: str, *, before_send=False):
        # The fixed window is never extended or reset by replies, reconnects or repeated invites.
        return None
