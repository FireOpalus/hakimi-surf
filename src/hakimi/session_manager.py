"""One encrypted SQLite file per conversation; no cross-file history queries."""

import asyncio
import os
import time
from contextlib import asynccontextmanager

import aiosqlite

from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
from .public_memory import PublicMemory
from .security import Rejected
from .store import Store

SCHEMA = """
CREATE TABLE IF NOT EXISTS session_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity BLOB NOT NULL,
  enabled INTEGER NOT NULL CHECK(enabled IN (0,1)), migrated INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS turns (
  id INTEGER PRIMARY KEY, payload BLOB NOT NULL, expires REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS turns_expiry ON turns(expires);
CREATE TABLE IF NOT EXISTS recent_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS rolling_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), summary BLOB, expires REAL NOT NULL DEFAULT 0,
  epoch INTEGER NOT NULL DEFAULT 0, version INTEGER NOT NULL DEFAULT 0,
  attempt TEXT, retry_after REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS outgoing_receipts (id TEXT PRIMARY KEY, expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS received_messages (
 event TEXT PRIMARY KEY, payload BLOB NOT NULL, received REAL NOT NULL,
 expires REAL NOT NULL, state TEXT NOT NULL DEFAULT 'received'
);
"""


class SessionManager:
    def __init__(self, settings: Settings, store: Store):
        self.s, self.store, self.vault = settings, store, store.vault
        self.root = settings.data_dir / "sessions"
        self.locks = [asyncio.Lock() for _ in range(32)]
        self.create_lock = asyncio.Lock()

    def key(self, message: Message) -> str:
        if message.kind == "private" and message.conversation_id != message.user_id:
            raise Rejected("invalid_private_scope")
        return self.vault.digest("session-v2", *message.session_parts(self.s.namespace))

    def path(self, message: Message):
        return self.root / message.kind / (self.key(message) + ".sqlite3")

    @asynccontextmanager
    async def database(self, message: Message):
        key, path = self.key(message), self.path(message)
        await self.store.runtime.register(message)
        async with self.locks[int(key[:8], 16) % len(self.locks)]:
            async with self.create_lock:
                if path.is_symlink() or path.parent.is_symlink() or self.root.is_symlink():
                    raise Rejected("invalid_session_path")
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                self.root.chmod(0o700)
                path.parent.chmod(0o700)
                if (
                    not path.exists()
                    and len(list(self.root.glob("*/*.sqlite3"))) >= self.s.session_max_files
                ):
                    raise Rejected("session_capacity")
                fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                path.chmod(0o600)
            async with aiosqlite.connect(path, isolation_level=None) as db:
                await db.execute("PRAGMA journal_mode=DELETE")
                await db.execute("PRAGMA synchronous=FULL")
                await db.execute("PRAGMA secure_delete=ON")
                await db.execute("PRAGMA busy_timeout=5000")
                await db.executescript(SCHEMA)
                await db.execute("BEGIN IMMEDIATE")
                try:
                    async with db.execute(
                        "SELECT identity,enabled,migrated FROM session_meta"
                    ) as cur:
                        row = await cur.fetchone()
                    if row:
                        if self.vault.open(key, row[0]) != message.session_parts(self.s.namespace):
                            raise Rejected("session_identity_mismatch")
                    else:
                        await db.execute(
                            "INSERT INTO session_meta VALUES (1,?,?,0)",
                            (
                                self.vault.seal(key, message.session_parts(self.s.namespace)),
                                int(self.s.session_default_enabled),
                            ),
                        )
                    if not row or not row[2]:
                        await self._migrate_private(db, message, key)
                        await db.execute("UPDATE session_meta SET migrated=1")
                    async with db.execute("SELECT 1 FROM rolling_state") as cur:
                        migrated = await cur.fetchone()
                    if not migrated:
                        await db.execute("INSERT INTO rolling_state(singleton) VALUES (1)")
                        async with db.execute(
                            "SELECT payload,expires FROM turns ORDER BY id"
                        ) as cur:
                            old = await cur.fetchall()
                        for item in old:
                            turn = self.vault.open(key, item[0])
                            for role in ("user", "assistant"):
                                await db.execute(
                                    "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                                    (
                                        self.vault.seal(
                                            key,
                                            {
                                                "role": role,
                                                "text": turn[role],
                                                "speaker": "历史成员"
                                                if role == "user"
                                                else "哈基米",
                                            },
                                        ),
                                        item[1],
                                    ),
                                )
                        await db.execute("DELETE FROM turns")
                    await db.execute("DELETE FROM turns WHERE expires<=?", (time.time(),))
                    await db.execute("DELETE FROM recent_messages WHERE expires<=?", (time.time(),))
                    await db.execute(
                        "UPDATE rolling_state SET "
                        "summary=NULL,version=version+1 WHERE summary IS NOT NULL AND expires<=?",
                        (time.time(),),
                    )
                    yield db, key
                    await db.commit()
                except BaseException:
                    await db.rollback()
                    raise

    async def _migrate_private(self, db, message: Message, key: str):
        # Legacy group memory was per-user. Never merge it into a shared group session.
        if message.kind != "private":
            return
        scope = self.store.scope(message)
        async with self.store.lock:
            consent = await self.store.one("SELECT enabled FROM consent WHERE scope=?", (scope,))
            rows = await self.store.all(
                "SELECT payload,expires FROM memory WHERE scope=? AND expires>? "
                "ORDER BY id DESC LIMIT ?",
                (scope, time.time(), self.s.session_turns),
            )
        if consent is not None:
            await db.execute("UPDATE session_meta SET enabled=?", (consent[0],))
        if consent and consent[0]:
            for row in reversed(rows):
                await db.execute(
                    "INSERT INTO turns(payload,expires) VALUES (?,?)",
                    (
                        self.vault.seal(key, self.vault.open(scope, row[0])),
                        row[1],
                    ),
                )
        # Legacy rows expire in the original Store. Keeping them until then makes migration
        # crash-safe without a transaction spanning two independent SQLite files.

    async def history(self, message: Message) -> list[dict]:
        snapshot = await self.snapshot(message)
        items = snapshot["recent_messages"]
        # Odd-sized windows can start with an assistant message; never relabel roles.
        return [
            {"user": items[n]["text"], "assistant": items[n + 1]["text"]}
            for n in range(len(items) - 1)
            if items[n]["role"] == "user" and items[n + 1]["role"] == "assistant"
        ]

    async def snapshot(self, message: Message) -> dict:
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                enabled = bool((await cur.fetchone())[0])
            async with db.execute(
                "SELECT summary,epoch,version,retry_after FROM rolling_state"
            ) as cur:
                state = await cur.fetchone()
            async with db.execute("SELECT payload FROM recent_messages ORDER BY id") as cur:
                rows = await cur.fetchall()
            return {
                "rolling_summary": self.vault.open(key, state[0]) if enabled and state[0] else "",
                "recent_messages": [self.vault.open(key, row[0]) for row in rows]
                if enabled
                else [],
                "epoch": state[1],
                "version": state[2],
                "retry_after": state[3],
                "enabled": enabled,
            }

    async def remember(self, message: Message, user: str, assistant: str, *, expected_epoch=None):
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                epoch = (await cur.fetchone())[0]
            if expected_epoch is not None and epoch != expected_epoch:
                return False
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return True
            # Images, URLs of QQ CDN attachments, authors and identities are not persisted here.
            speaker = (
                "BOISER"
                if is_persona_owner(message, self.s)
                else "成员-" + self.store.subject(message)[:8]
            )
            for role, text in [("user", user), ("assistant", assistant)]:
                await db.execute(
                    "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                    (
                        self.vault.seal(
                            key,
                            {
                                "role": role,
                                "text": text,
                                "speaker": speaker if role == "user" else "哈基米",
                            },
                        ),
                        time.time() + self.s.memory_ttl_seconds,
                    ),
                )
            await db.execute(
                "DELETE FROM recent_messages WHERE id NOT IN (SELECT id "
                "FROM recent_messages ORDER BY id DESC LIMIT ?)",
                (
                    max(
                        self.s.session_turns * 2,
                        self.store.runtime.compression.window(message.kind).hard_limit,
                    ),
                ),
            )
            return True

    async def receive(self, message, *, expected_epoch=None):
        """Persist untrusted incoming data before debounce/LLM, outside trusted hot history."""
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                epoch = (await cur.fetchone())[0]
            if expected_epoch is not None and epoch != expected_epoch:
                return None
            event = self.store.event_key(message)
            await db.execute(
                "INSERT INTO received_messages VALUES (?,?,?,?,'received') "
                "ON CONFLICT(event) DO UPDATE SET payload=excluded.payload",
                (
                    event,
                    self.vault.seal(key + ":incoming:" + event, message.model_dump()),
                    time.time(),
                    time.time() + min(self.s.memory_ttl_seconds, 86400),
                ),
            )
            await db.execute("DELETE FROM received_messages WHERE expires<?", (time.time(),))
            await db.execute(
                "DELETE FROM received_messages WHERE event NOT IN "
                "(SELECT event FROM received_messages ORDER BY received DESC,rowid DESC LIMIT 200)"
            )
            return epoch

    async def lookup_received(self, message, reply_id):
        # Reference identifiers cannot select another session's physical database.
        reference = message.model_copy(update={"event_id": str(reply_id)})
        event = self.store.event_key(reference)
        async with self.database(message) as (db, key):
            async with db.execute(
                "SELECT payload FROM received_messages WHERE event=? AND expires>?",
                (event, time.time()),
            ) as cur:
                row = await cur.fetchone()
            return (
                Message.model_validate(self.vault.open(key + ":incoming:" + event, row[0]))
                if row
                else None
            )

    async def receive_outgoing(
        self, target, receipt_id, text, *, expected_epoch, on_confirmed=None
    ):
        """Cache an acknowledged bot message in the target DB, not the bot's private DB."""
        sent = target.model_copy(
            update={
                "event_id": str(receipt_id),
                "user_id": target.bot_id,
                "sender_nickname": "吉米奈喵",
                "text": text[:8000],
                "command_text": "",
                "reply_ids": [],
                "forward_ids": [],
                "images": [],
                "mentioned_ids": [],
                "created_at": time.time(),
                "context_expanded": True,
            }
        )
        event = self.store.event_key(sent)
        async with self.database(target) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != expected_epoch:
                    return False
            await db.execute(
                "INSERT OR IGNORE INTO received_messages VALUES (?,?,?,?,'screened')",
                (
                    event,
                    self.vault.seal(key + ":incoming:" + event, sent.model_dump()),
                    time.time(),
                    time.time() + min(self.s.memory_ttl_seconds, 86400),
                ),
            )
            # Keep ancillary receipt/context writes under the same session lock as reset.
            # The callback must not re-enter this SessionManager's database methods.
            if on_confirmed is not None:
                await on_confirmed()
            return True

    async def admit_history(self, message, members, *, expected_epoch):
        """Only screened originals may move from the receipt journal into model history."""
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != expected_epoch:
                    return False
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                enabled = (await cur.fetchone())[0]
            for member in members:
                if self.key(member) != key:
                    raise Rejected("session_identity_mismatch")
                event = self.store.event_key(member)
                async with db.execute(
                    "SELECT state FROM received_messages WHERE event=?", (event,)
                ) as cur:
                    old = await cur.fetchone()
                if old and old[0] == "screened":
                    continue
                if enabled:
                    text = member.text
                    if member.kind == "group":
                        text = f"[{member.sender_nickname or '群友'}(QQ:{member.user_id})]: {text}"
                    await db.execute(
                        "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                        (
                            self.vault.seal(
                                key,
                                {
                                    "role": "user",
                                    "text": text,
                                    "speaker": member.sender_nickname or "群友",
                                    "user_id": member.user_id,
                                    "event_id": member.event_id,
                                },
                            ),
                            time.time() + self.s.memory_ttl_seconds,
                        ),
                    )
                await db.execute(
                    "UPDATE received_messages SET state='screened' WHERE event=?", (event,)
                )
            await db.execute(
                "DELETE FROM recent_messages WHERE id NOT IN (SELECT id FROM recent_messages "
                "ORDER BY id DESC LIMIT ?)",
                (self.store.runtime.compression.window(message.kind).hard_limit,),
            )
            return True

    async def reply(self, message, text, receipt_id, *, expected_epoch):
        """Append an assistant turn once, without duplicating already stored user messages."""
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != expected_epoch:
                    return False
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return True
            async with db.execute(
                "SELECT 1 FROM outgoing_receipts WHERE id=?", (receipt_id,)
            ) as cur:
                if await cur.fetchone():
                    return True
            await db.execute(
                "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                (
                    self.vault.seal(
                        key, {"role": "assistant", "text": text, "speaker": "吉米奈喵"}
                    ),
                    time.time() + self.s.memory_ttl_seconds,
                ),
            )
            await db.execute(
                "INSERT INTO outgoing_receipts VALUES (?,?)", (receipt_id, time.time() + 9 * 86400)
            )
            await db.execute("DELETE FROM outgoing_receipts WHERE expires<?", (time.time(),))
            return True

    async def clear(self, message: Message, *, enabled: bool | None = None):
        async with self.database(message) as (db, _):
            await db.execute("DELETE FROM turns")
            await db.execute("DELETE FROM recent_messages")
            await db.execute("DELETE FROM received_messages")
            await db.execute(
                "UPDATE rolling_state SET "
                "summary=NULL,expires=0,epoch=epoch+1,version=version+1,attempt=NULL,retry_after=0"
            )
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                epoch = (await cur.fetchone())[0]
            # Commit the main-DB tombstone before the physical-session reset commits.
            # A crash between commits can suppress a fresh public entry, never revive an
            # erased entry. Retrying reset catches up the local epoch safely.
            await PublicMemory(self.store).clear_group(message, epoch=epoch)
            if enabled is not None:
                await db.execute("UPDATE session_meta SET enabled=?", (int(enabled),))
        # Remove old private data too; it must not survive an explicit erasure request.
        if message.kind == "private":
            await self.store.set_memory(self.store.scope(message), False)
        await self.store.runtime.filters.clear_group(message)
        await self.store.groups.clear_recent(message)
        await self.store.cancel_session_replies(message)

    async def remember_assistant(self, message, text, receipt_id, *, expected_epoch):
        if message.kind != "private" or not is_persona_owner(message, self.s):
            raise Rejected("owner_dm_target_forbidden")
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != expected_epoch:
                    return False
            async with db.execute(
                "SELECT 1 FROM outgoing_receipts WHERE id=?", (receipt_id,)
            ) as cur:
                if await cur.fetchone():
                    return True
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return False
            await db.execute(
                "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                (
                    self.vault.seal(key, {"role": "assistant", "text": text, "speaker": "哈基米"}),
                    time.time() + self.s.memory_ttl_seconds,
                ),
            )
            await db.execute(
                "INSERT INTO outgoing_receipts VALUES (?,?)", (receipt_id, time.time() + 9 * 86400)
            )
            await db.execute("DELETE FROM outgoing_receipts WHERE expires<?", (time.time(),))
            await db.execute(
                "DELETE FROM recent_messages WHERE id NOT IN "
                "(SELECT id FROM recent_messages ORDER BY id DESC LIMIT ?)",
                (max(self.s.session_turns * 2, self.store.runtime.compression.private.hard_limit),),
            )
            return True

    async def enable(self, message: Message):
        async with self.database(message) as (db, _):
            await db.execute("UPDATE session_meta SET enabled=1")

    async def edit_summary(self, message, summary, expected_version):
        if len(summary) > 800:
            raise Rejected("summary_too_long")
        async with self.database(message) as (db, key):
            cur = await db.execute(
                "UPDATE rolling_state SET "
                "summary=?,expires=?,epoch=epoch+1,version=version+1,"
                "attempt=NULL,retry_after=0 WHERE version=?",
                (
                    self.vault.seal(key, summary),
                    time.time() + self.s.summary_ttl_seconds,
                    expected_version,
                ),
            )
            if cur.rowcount != 1:
                raise Rejected("session_changed")
        await self.store.cancel_session_replies(message)

    async def compression_snapshot(self, message, *, force=False):
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return None
            async with db.execute(
                "SELECT summary,epoch,version,attempt,retry_after FROM rolling_state"
            ) as cur:
                state = await cur.fetchone()
            async with db.execute("SELECT id,payload FROM recent_messages ORDER BY id") as cur:
                rows = await cur.fetchall()
            texts = [self.vault.open(key, row[1]) for row in rows]
            config = self.store.runtime.compression
            policy = config.window(message.kind)
            if len(rows) <= policy.keep or (
                not force
                and (
                    not config.enabled
                    or not policy.due(len(rows), sum(len(x["text"]) for x in texts))
                )
            ):
                return None
            if state[4] > time.time():
                return None
            ids = [row[0] for row in rows[: -policy.keep]]
            digest = self.vault.digest("compress", key, state[1], state[2], ids)
            if state[3] == digest:
                return None  # Ambiguous paid attempts never replay unchanged input.
            await db.execute(
                "UPDATE rolling_state SET attempt=?,retry_after=?", (digest, time.time() + 600)
            )
            return {
                "old_summary": self.vault.open(key, state[0]) if state[0] else "",
                "messages": texts[: -policy.keep],
                "ids": ids,
                "epoch": state[1],
                "version": state[2],
                "policy": policy.model_dump(),
                "force": force,
            }

    async def commit_compression(self, message, snapshot, summary):
        if len(summary) > 800:
            raise Rejected("summary_too_long")
        async with self.store.runtime.compression_lock, self.database(message) as (db, key):
            async with db.execute("SELECT epoch,version FROM rolling_state") as cur:
                state = await cur.fetchone()
            async with db.execute(
                "SELECT id FROM recent_messages WHERE id<=? ORDER BY id", (snapshot["ids"][-1],)
            ) as cur:
                ids = [row[0] for row in await cur.fetchall()]
            config = self.store.runtime.compression
            if (
                state != (snapshot["epoch"], snapshot["version"])
                or ids != snapshot["ids"]
                or config.window(message.kind).model_dump() != snapshot["policy"]
                or (not snapshot["force"] and not config.enabled)
            ):
                return False
            await db.execute(
                "UPDATE rolling_state SET summary=?,expires=?,version=version+1,retry_after=0",
                (self.vault.seal(key, summary), time.time() + self.s.summary_ttl_seconds),
            )
            await db.execute("DELETE FROM recent_messages WHERE id<=?", (snapshot["ids"][-1],))
            return True

    async def index_existing(self):
        for path in self.root.glob("*/*.sqlite3"):
            if path.is_symlink() or len(path.stem) != 64:
                continue
            async with aiosqlite.connect(path) as db:
                async with db.execute("SELECT identity FROM session_meta") as cur:
                    row = await cur.fetchone()
            if row:
                ns, platform, bot, kind, conv = self.vault.open(path.stem, row[0])
                if ns != self.s.namespace:
                    continue
                msg = Message(
                    platform=platform,
                    bot_id=bot,
                    kind=kind,
                    conversation_id=conv,
                    user_id=conv if kind == "private" else "0",
                    event_id="index",
                    text="",
                    addressed=True,
                    created_at=time.time(),
                )
                if self.key(msg) != path.stem:
                    raise Rejected("session_identity_mismatch")
                await self.store.runtime.register(msg)

    async def prune(self):
        # Expiry erasure also applies to sessions that receive no further messages.
        for path in self.root.glob("*/*.sqlite3"):
            if path.is_symlink() or len(path.stem) != 64:
                continue
            async with self.locks[int(path.stem[:8], 16) % len(self.locks)]:
                async with aiosqlite.connect(path) as db:
                    await db.execute("PRAGMA secure_delete=ON")
                    await db.execute("DELETE FROM turns WHERE expires<=?", (time.time(),))
                    async with db.execute(
                        "SELECT name FROM sqlite_master WHERE name='rolling_state'"
                    ) as cur:
                        if await cur.fetchone():
                            await db.execute(
                                "DELETE FROM recent_messages WHERE expires<=?", (time.time(),)
                            )
                            await db.execute(
                                "UPDATE rolling_state SET "
                                "summary=NULL,version=version+1 "
                                "WHERE summary IS NOT NULL AND expires<=?",
                                (time.time(),),
                            )
                    await db.commit()
