"""One encrypted SQLite file per conversation; no cross-file history queries."""

import asyncio
import os
import time
from contextlib import asynccontextmanager

import aiosqlite

from .bot_server import history_lines, is_persona_owner
from .config import Settings
from .context_budget import estimate_tokens
from .dual_memory import SCHEMA as DUAL_SCHEMA
from .dual_memory import DualMemory
from .models import Message
from .outbound_actions import MAX_IMAGE_DIGEST_CHARS
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
CREATE TABLE IF NOT EXISTS history_folds (
 id INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB NOT NULL,
 epoch INTEGER NOT NULL, expires REAL NOT NULL, attempted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS outgoing_receipts (id TEXT PRIMARY KEY, expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS received_messages (
 event TEXT PRIMARY KEY, payload BLOB NOT NULL, received REAL NOT NULL,
 expires REAL NOT NULL, state TEXT NOT NULL DEFAULT 'received'
);
CREATE TABLE IF NOT EXISTS interaction_chains (
 lane TEXT PRIMARY KEY, payload BLOB NOT NULL, expires REAL NOT NULL
);
"""


class SessionManager:
    def __init__(self, settings: Settings, store: Store):
        self.s, self.store, self.vault = settings, store, store.vault
        self.root = settings.data_dir / "sessions"
        self.locks = [asyncio.Lock() for _ in range(32)]
        self.create_lock = asyncio.Lock()
        self.dual = DualMemory(self)

    def key(self, message: Message) -> str:
        if message.kind == "private" and message.conversation_id != message.user_id:
            raise Rejected("invalid_private_scope")
        return self.vault.digest("session-v2", *message.session_parts(self.s.namespace))

    def path(self, message: Message):
        return self.root / message.kind / (self.key(message) + ".sqlite3")

    @asynccontextmanager
    async def database(self, message: Message, *, preserve_history=False):
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
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("PRAGMA synchronous=FULL")
                await db.execute("PRAGMA secure_delete=ON")
                await db.execute("PRAGMA busy_timeout=5000")
                await db.executescript(SCHEMA + DUAL_SCHEMA)
                async with db.execute("PRAGMA table_info(rolling_state)") as cur:
                    columns = {row[1] for row in await cur.fetchall()}
                if "trim_version" not in columns:
                    await db.execute(
                        "ALTER TABLE rolling_state ADD COLUMN trim_version INTEGER NOT NULL "
                        "DEFAULT 0"
                    )
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
                    await self.dual.migrate(db, key)
                    if not preserve_history:
                        if self.s.prefix_history_enabled:
                            async with db.execute(
                                "SELECT id,payload FROM recent_messages WHERE expires<=? "
                                "ORDER BY id",
                                (time.time(),),
                            ) as cur:
                                expired_rows = [
                                    {**self.vault.open(key, r[1]), "seq": r[0]}
                                    for r in await cur.fetchall()
                                ]
                            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                                epoch = (await cur.fetchone())[0]
                            await self.dual.append_slice(db, key, expired_rows, epoch, "expiry")
                        expired = await db.execute(
                            "DELETE FROM recent_messages WHERE expires<=?", (time.time(),)
                        )
                        if expired.rowcount:
                            await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")
                            await db.execute("DELETE FROM interaction_chains")
                        await db.execute(
                            "UPDATE rolling_state SET "
                            "summary=NULL,version=version+1 WHERE summary IS NOT NULL AND "
                            "expires<=?",
                            (time.time(),),
                        )
                        await db.execute(
                            "DELETE FROM history_folds WHERE expires<=?", (time.time(),)
                        )
                        await db.execute(
                            "DELETE FROM interaction_chains WHERE expires<=?", (time.time(),)
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

    async def current_epoch(self, message: Message) -> int:
        async with self.database(message) as (db, _key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                return (await cur.fetchone())[0]

    async def private_request_window(self, message: Message) -> tuple[int, list[dict]]:
        """Six recent entries in one consenting private session, without opening summaries."""
        if message.kind != "private":
            raise Rejected("invalid_private_scope")
        async with self.database(message, preserve_history=True) as (db, key):
            async with db.execute("SELECT epoch,enabled FROM rolling_state,session_meta") as cur:
                epoch, enabled = await cur.fetchone()
            if not enabled:
                return epoch, []
            async with db.execute(
                "SELECT id,payload FROM recent_messages WHERE expires>? ORDER BY id DESC LIMIT 6",
                (time.time(),),
            ) as cur:
                return epoch, [
                    {**self.vault.open(key, blob), "seq": seq} for seq, blob in await cur.fetchall()
                ]

    async def snapshot(self, message: Message) -> dict:
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                enabled = bool((await cur.fetchone())[0])
            await self._fold_history(db, key, message)
            async with db.execute(
                "SELECT summary,epoch,version,retry_after,trim_version FROM rolling_state"
            ) as cur:
                state = await cur.fetchone()
            async with db.execute("SELECT id,payload FROM recent_messages ORDER BY id") as cur:
                rows = await cur.fetchall()
            async with db.execute("SELECT ready FROM memory_control") as cur:
                dual_ready = bool((await cur.fetchone())[0])
            chains = {}
            if enabled:
                async with db.execute(
                    "SELECT lane,payload,expires FROM interaction_chains WHERE expires>?",
                    (time.time(),),
                ) as cur:
                    for lane, blob, exp in await cur.fetchall():
                        chains[lane] = {
                            **self.vault.open(f"{key}:chain:{lane}", blob),
                            "expires": exp,
                        }
            return {
                "dual_ready": dual_ready,
                "rolling_summary": self.vault.open(key, state[0])
                if enabled and state[0] and not dual_ready
                else "",
                "recent_messages": [{**self.vault.open(key, row[1]), "seq": row[0]} for row in rows]
                if enabled
                else [],
                "interaction_chains": chains,
                "epoch": state[1],
                "version": state[2],
                "retry_after": state[3],
                "history_revision": state[4],
                "enabled": enabled,
            }

    async def admin_list_summary(self, message: Message) -> dict:
        """Return lightweight session list metadata in a single DB open without decrypting logs."""
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                enabled = bool((await cur.fetchone())[0])
            await self._fold_history(db, key, message)
            async with db.execute("SELECT summary,epoch,version FROM rolling_state") as cur:
                summary_blob, epoch, version = await cur.fetchone()
            async with db.execute("SELECT ready FROM memory_control") as cur:
                dual_ready = bool((await cur.fetchone())[0])
            if enabled:
                async with db.execute("SELECT COUNT(*) FROM recent_messages") as cur:
                    recent_count = (await cur.fetchone())[0]
                summary_chars = (
                    len(self.vault.open(key, summary_blob))
                    if summary_blob and not dual_ready
                    else 0
                )
                dual_stats = await self.dual.stats_in_db(db, key, epoch, version)
            else:
                recent_count = 0
                summary_chars = 0
                dual_stats = {"dual_entries_count": 0, "dual_chars": 0}
        return {
            "enabled": enabled,
            "dual_ready": dual_ready,
            "summary_chars": summary_chars,
            "recent_count": recent_count,
            **dual_stats,
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
                                "created_at": time.time(),
                            },
                        ),
                        time.time() + self.s.memory_ttl_seconds,
                    ),
                )
            if self.s.prefix_history_enabled:
                await self._fold_history(db, key, message)
            else:
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
                    if member.images:
                        text += " [附图]"
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
                                    "created_at": member.created_at or time.time(),
                                },
                            ),
                            time.time() + self.s.memory_ttl_seconds,
                        ),
                    )
                await db.execute(
                    "UPDATE received_messages SET state='screened' WHERE event=?", (event,)
                )
            if self.s.prefix_history_enabled:
                await self._fold_history(db, key, message)
            else:
                trimmed = await db.execute(
                    "DELETE FROM recent_messages WHERE id NOT IN (SELECT id FROM recent_messages "
                    "ORDER BY id DESC LIMIT ?)",
                    (self.store.runtime.compression.window(message.kind).hard_limit,),
                )
                if trimmed.rowcount:
                    await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")
                    await db.execute("DELETE FROM interaction_chains")
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
                        key,
                        {
                            "role": "assistant",
                            "text": text,
                            "speaker": "吉米奈喵",
                            "created_at": time.time(),
                        },
                    ),
                    time.time() + self.s.memory_ttl_seconds,
                ),
            )
            await db.execute(
                "INSERT INTO outgoing_receipts VALUES (?,?)", (receipt_id, time.time() + 9 * 86400)
            )
            await db.execute("DELETE FROM outgoing_receipts WHERE expires<?", (time.time(),))
            await self._fold_history(db, key, message)
            return True

    async def reply_batch(self, message, reply_id, acknowledgements, *, expected_epoch):
        """One SQLite commit for all verified bubbles and the append-only visual note."""
        async with self.database(message) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != expected_epoch:
                    return False
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return True

            async def append(receipt, body):
                cursor = await db.execute(
                    "INSERT OR IGNORE INTO outgoing_receipts VALUES (?,?)",
                    (receipt, time.time() + 9 * 86400),
                )
                if cursor.rowcount:
                    await db.execute(
                        "INSERT INTO recent_messages(payload,expires) VALUES (?,?)",
                        (
                            self.vault.seal(
                                key,
                                {
                                    "role": "assistant",
                                    "text": body,
                                    "speaker": "吉米奈喵",
                                    "created_at": time.time(),
                                    "reply_id": reply_id,
                                },
                            ),
                            time.time() + self.s.memory_ttl_seconds,
                        ),
                    )

            for item in acknowledgements:
                await append(reply_id + ":ack:" + item["message_id"], item["text"])
            digest = next((v["digest"] for v in acknowledgements if v.get("digest")), None)
            if digest:
                await append(
                    reply_id + ":image-digest",
                    "[附图摘要，消息"
                    + message.event_id
                    + "："
                    + digest[:MAX_IMAGE_DIGEST_CHARS]
                    + "]",
                )
            await self._fold_history(db, key, message)
            return True

    async def clear(self, message: Message, *, enabled: bool | None = None):
        hot = getattr(self.store.runtime, "hot_ring", None)
        if hot is not None:
            hot.clear(message)
        async with self.database(message) as (db, _):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                epoch = (await cur.fetchone())[0] + 1
            # The main tombstone commits first, before ANY local deletion; failed
            # resets fail closed.
            await PublicMemory(self.store).clear_group(message, epoch=epoch)
            await self.store.cancel_session_replies(message)
            for table in (
                "member_profiles",
                "chronicle_events",
                "memory_slice_jobs",
                "cold_history_slices",
                "memory_covered",
                "outgoing_receipts",
            ):
                await db.execute("DELETE FROM " + table)
            await db.execute(
                "UPDATE memory_control SET "
                "ready=0,mirror_enabled=0,mirror_payload=NULL,last_nightly=NULL"
            )
            session_key = self.key(message)
            self.dual.index.discard(session_key)
            self.dual._stats_cache.pop(session_key, None)
            await db.execute("DELETE FROM turns")
            await db.execute("DELETE FROM recent_messages")
            await db.execute("DELETE FROM received_messages")
            await db.execute("DELETE FROM history_folds")
            await db.execute("DELETE FROM interaction_chains")
            await db.execute(
                "UPDATE rolling_state SET "
                "summary=NULL,expires=0,epoch=epoch+1,version=version+1,attempt=NULL,retry_after=0"
            )
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
                    self.vault.seal(
                        key,
                        {
                            "role": "assistant",
                            "text": text,
                            "speaker": "哈基米",
                            "created_at": time.time(),
                        },
                    ),
                    time.time() + self.s.memory_ttl_seconds,
                ),
            )
            await db.execute(
                "INSERT INTO outgoing_receipts VALUES (?,?)", (receipt_id, time.time() + 9 * 86400)
            )
            await db.execute("DELETE FROM outgoing_receipts WHERE expires<?", (time.time(),))
            if self.s.prefix_history_enabled:
                await self._fold_history(db, key, message)
            else:
                trimmed = await db.execute(
                    "DELETE FROM recent_messages WHERE id NOT IN "
                    "(SELECT id FROM recent_messages ORDER BY id DESC LIMIT ?)",
                    (
                        max(
                            self.s.session_turns * 2,
                            self.store.runtime.compression.private.hard_limit,
                        ),
                    ),
                )
                if trimmed.rowcount:
                    await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")
                    await db.execute("DELETE FROM interaction_chains")
            return True

    async def enable(self, message: Message):
        async with self.database(message) as (db, _):
            await db.execute("UPDATE session_meta SET enabled=1")

    async def edit_summary(self, message, summary, expected_version):
        if len(summary) > 800:
            raise Rejected("summary_too_long")
        async with self.database(message) as (db, key):
            async with db.execute("SELECT ready FROM memory_control") as ready_cursor:
                if (await ready_cursor.fetchone())[0]:
                    raise Rejected("legacy_summary_retired")
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
            await db.execute("DELETE FROM history_folds")
            await db.execute("DELETE FROM interaction_chains")
        await self.store.cancel_session_replies(message)

    async def _fold_history(self, db, key, message):
        if not self.s.prefix_history_enabled:
            return
        async with db.execute("SELECT id,payload FROM recent_messages ORDER BY id") as cur:
            rows = await cur.fetchall()
        texts = [{**self.vault.open(key, r[1]), "seq": r[0]} for r in rows]
        rendered = history_lines(texts, self.s.onebot_self_id)
        high_tokens = (
            self.store.runtime.compression.history_high_tokens
            if self.store.runtime.compression_overridden
            else self.s.history_high_tokens
        )
        if len(rows) < 2 or (
            estimate_tokens(rendered) < high_tokens
            and len(rendered.encode()) <= self.s.history_max_bytes
        ):
            return
        # One atomic step, never a one-message sliding window. Payload is durably
        # encrypted before removal, so a restart cannot lose the compression hand-off.
        target, running, half = estimate_tokens(rendered) / 2, 0, 0
        for entry in texts[:-1]:
            running += estimate_tokens(history_lines([entry], self.s.onebot_self_id))
            half += 1
            if running >= target:
                break
        async with db.execute("SELECT epoch FROM rolling_state") as cur:
            epoch = (await cur.fetchone())[0]
        fold_cursor = await db.execute(
            "INSERT INTO history_folds(payload,epoch,expires) VALUES (?,?,?)",
            (
                self.vault.seal(key + ":fold", texts[:half]),
                epoch,
                time.time() + self.s.summary_ttl_seconds,
            ),
        )
        await self.dual.append_slice(
            db, key, texts[:half], epoch, "fold", ident="legacy-" + str(fold_cursor.lastrowid)
        )
        await db.execute("DELETE FROM recent_messages WHERE id<=?", (rows[half - 1][0],))
        await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")
        await db.execute("DELETE FROM interaction_chains")
        # Bounded offline backlog; uncompressed tails never become trusted instructions.
        await db.execute(
            "DELETE FROM history_folds WHERE id NOT IN "
            "(SELECT id FROM history_folds ORDER BY id DESC LIMIT 16)"
        )

    async def compression_snapshot(self, message, *, force=False):
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return None
            async with db.execute(
                "SELECT summary,epoch,version,attempt,retry_after FROM rolling_state"
            ) as cur:
                state = await cur.fetchone()
            if self.s.prefix_history_enabled:
                async with db.execute(
                    "SELECT id,payload FROM history_folds WHERE attempted=0 AND epoch=? "
                    "ORDER BY id LIMIT 1",
                    (state[1],),
                ) as cur:
                    fold = await cur.fetchone()
                if fold and self.store.runtime.compression.enabled:
                    await db.execute("UPDATE history_folds SET attempted=1 WHERE id=?", (fold[0],))
                    texts = self.vault.open(key + ":fold", fold[1])
                    return {
                        "old_summary": self.vault.open(key, state[0]) if state[0] else "",
                        "messages": texts,
                        "ids": [t["seq"] for t in texts],
                        "epoch": state[1],
                        "version": state[2],
                        "fold_id": fold[0],
                        "force": force,
                    }
                if not force:
                    return None
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
                "attempt_id": digest,
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
            if "fold_id" in snapshot:
                async with db.execute(
                    "SELECT 1 FROM history_folds WHERE id=? AND epoch=?",
                    (snapshot["fold_id"], snapshot["epoch"]),
                ) as cur:
                    exists = await cur.fetchone()
                if (
                    state != (snapshot["epoch"], snapshot["version"])
                    or not exists
                    or not self.store.runtime.compression.enabled
                ):
                    return False
                await db.execute(
                    "UPDATE rolling_state SET summary=?,expires=?,version=version+1,retry_after=0",
                    (self.vault.seal(key, summary), time.time() + self.s.summary_ttl_seconds),
                )
                await db.execute("DELETE FROM history_folds WHERE id=?", (snapshot["fold_id"],))
                await db.execute("DELETE FROM interaction_chains")
                return True
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
            await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")
            await db.execute("DELETE FROM interaction_chains")
            return True

    async def commit_chain(
        self,
        message: Message,
        *,
        lane: str,
        interaction_id: str,
        expected_epoch: int,
        trim_version: int,
        system_digest: str,
        covered_seq: int,
        source_ids: list[str] | None = None,
        reply_id: str | None = None,
        turns: int = 1,
        context_digests: dict[str, str] | None = None,
        seen_nearby: list[str] | None = None,
    ) -> bool:
        if not interaction_id or not self.s.interaction_store_enabled:
            return False
        owned_events = set(source_ids or ())
        async with self.database(message) as (db, key):
            async with db.execute("SELECT enabled FROM session_meta") as cur:
                if not (await cur.fetchone())[0]:
                    return False
            async with db.execute("SELECT epoch,trim_version FROM rolling_state") as cur:
                epoch, current_trim = await cur.fetchone()
            if (epoch, current_trim) != (expected_epoch, trim_version):
                return False
            final_covered_seq = int(covered_seq)
            async with db.execute(
                "SELECT id,payload FROM recent_messages WHERE id>? ORDER BY id",
                (final_covered_seq,),
            ) as cur:
                for row_id, blob in await cur.fetchall():
                    item = self.vault.open(key, blob)
                    if (item.get("event_id") and item.get("event_id") in owned_events) or (
                        reply_id and item.get("reply_id") == reply_id
                    ):
                        final_covered_seq = row_id
                    else:
                        break
            ttl = min(self.s.interaction_chain_ttl_seconds, self.s.memory_ttl_seconds)
            expires = time.time() + ttl
            await db.execute(
                "INSERT OR REPLACE INTO interaction_chains(lane,payload,expires) VALUES (?,?,?)",
                (
                    lane,
                    self.vault.seal(
                        f"{key}:chain:{lane}",
                        {
                            "interaction_id": interaction_id,
                            "epoch": expected_epoch,
                            "trim_version": trim_version,
                            "system_digest": system_digest,
                            "covered_seq": final_covered_seq,
                            "turns": int(turns),
                            "context_digests": dict(context_digests or {}),
                            "seen_nearby": list(seen_nearby or [])[:16],
                        },
                    ),
                    expires,
                ),
            )
            return True

    async def invalidate_chains(
        self, message: Message, *, recalled_event_id: str | None = None
    ) -> None:
        if not self.path(message).exists():
            return
        async with self.database(message) as (db, key):
            await db.execute("DELETE FROM interaction_chains")
            if recalled_event_id:
                async with db.execute("SELECT id,payload FROM recent_messages ORDER BY id") as cur:
                    rows = await cur.fetchall()
                to_delete = []
                for row_id, blob in rows:
                    if self.vault.open(key, blob).get("event_id") == recalled_event_id:
                        to_delete.append(row_id)
                if to_delete:
                    placeholders = ",".join("?" for _ in to_delete)
                    await db.execute(
                        f"DELETE FROM recent_messages WHERE id IN ({placeholders})",
                        tuple(to_delete),
                    )
                    await db.execute("UPDATE rolling_state SET trim_version=trim_version+1")

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
        if self.s.prefix_history_enabled:
            # The same transactional expiry path preserves a cold slice before pruning.
            for _, message in await self.store.runtime.sessions():
                async with self.database(message):
                    pass
            return
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
                            deleted = await db.execute(
                                "DELETE FROM recent_messages WHERE expires<=?", (time.time(),)
                            )
                            if deleted.rowcount:
                                await db.execute(
                                    "UPDATE rolling_state SET trim_version=trim_version+1"
                                )
                            await db.execute(
                                "UPDATE rolling_state SET "
                                "summary=NULL,version=version+1 "
                                "WHERE summary IS NOT NULL AND expires<=?",
                                (time.time(),),
                            )
                            async with db.execute(
                                "SELECT name FROM sqlite_master WHERE name='interaction_chains'"
                            ) as chain_cur:
                                if await chain_cur.fetchone():
                                    if deleted.rowcount:
                                        await db.execute("DELETE FROM interaction_chains")
                                    else:
                                        await db.execute(
                                            "DELETE FROM interaction_chains WHERE expires<=?",
                                            (time.time(),),
                                        )
                    await db.commit()
