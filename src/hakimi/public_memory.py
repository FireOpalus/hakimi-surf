"""One-way public interaction index. Never reads private session databases."""

import time

from .bot_server import is_persona_owner
from .models import Message
from .security import authorize

SCHEMA = """
CREATE TABLE IF NOT EXISTS public_interactions (
 id TEXT PRIMARY KEY, scope TEXT NOT NULL, created REAL NOT NULL, payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS public_interactions_created ON public_interactions(created);
CREATE TABLE IF NOT EXISTS public_session_epochs (
 scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL CHECK(epoch>=0)
);
"""


class PublicMemory:
    def __init__(self, store):
        self.store, self.s = store, store.s

    async def record(self, reply_id, message, members, text, *, expected_epoch):
        if message.kind != "group" or not authorize(message, self.s):
            return False
        scope = self.store.session_scope(message)
        data = {
            "message": message.model_dump(),
            "session_epoch": expected_epoch,
            "group_name": getattr(message, "group_name", "") or "群 " + message.conversation_id,
            "speakers": [
                {
                    "nickname": m.sender_nickname,
                    "platform": m.platform,
                    "user_id": m.user_id,
                    **({"qq": m.user_id} if m.platform in {"onebot", "qq"} else {}),
                    "excerpt": m.text[:240],
                }
                for m in members[:8]
            ],
            "bot_reply": text[:400],
        }
        async with self.store.transaction():
            # The reset tombstone and this insert share one SQLite transaction boundary.
            # An ACK or task that resumes after a reset cannot resurrect the old group.
            await self.store.db.execute(
                "INSERT OR IGNORE INTO public_session_epochs VALUES (?,?)",
                (scope, expected_epoch),
            )
            current = await self.store.one(
                "SELECT epoch FROM public_session_epochs WHERE scope=?", (scope,)
            )
            if current[0] != expected_epoch:
                return False
            await self.store.db.execute(
                "INSERT OR IGNORE INTO public_interactions VALUES (?,?,?,?)",
                (
                    reply_id,
                    scope,
                    time.time(),
                    self.store.vault.seal("public-interaction:" + reply_id, data),
                ),
            )
            await self.store.db.execute(
                "DELETE FROM public_interactions WHERE created<?", (time.time() - 43200,)
            )
            await self.store.db.execute(
                "DELETE FROM public_interactions WHERE id NOT IN "
                "(SELECT id FROM public_interactions ORDER BY created DESC LIMIT 500)"
            )
        return True

    async def recent_for(self, message):
        if message.kind != "private" or not is_persona_owner(message, self.s):
            return []
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT p.id,p.payload,p.created,p.scope,e.epoch FROM public_interactions p "
                "JOIN public_session_epochs e ON e.scope=p.scope WHERE p.created>=? "
                "ORDER BY p.created DESC LIMIT 100",
                (time.time() - 43200,),
            )
        result = []
        for row in rows:
            data = self.store.vault.open("public-interaction:" + row["id"], row["payload"])
            origin = Message.model_validate(data.pop("message"))
            epoch = data.pop("session_epoch", None)
            if (
                origin.kind != "group"
                or (origin.platform, origin.bot_id) != (message.platform, message.bot_id)
                or epoch != row["epoch"]
                or self.store.session_scope(origin) != row["scope"]
                or not authorize(origin.model_copy(update={"addressed": True}), self.s)
            ):
                continue
            data["time"] = row["created"]
            result.append(data)
            if len(result) == 15:
                break
        return result

    async def clear_group(self, message, *, epoch):
        if message.kind == "group":
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO public_session_epochs VALUES (?,?) "
                    "ON CONFLICT(scope) DO UPDATE SET epoch=MAX(epoch,excluded.epoch)",
                    (self.store.session_scope(message), epoch),
                )
                await self.store.db.execute(
                    "DELETE FROM public_interactions WHERE scope=?",
                    (self.store.session_scope(message),),
                )
