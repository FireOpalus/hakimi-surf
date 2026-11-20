"""Application-only mutes and human-approved, explicitly optional friend removal."""

import asyncio
import logging
import re
import time
import uuid

from .bot_server import is_persona_owner
from .security import Rejected, clean_text, configured_secrets

SCHEMA = """
CREATE TABLE IF NOT EXISTS muted_users (
  user_key TEXT PRIMARY KEY, scope TEXT NOT NULL, identity BLOB NOT NULL,
  created REAL NOT NULL, source TEXT NOT NULL, delete_status TEXT NOT NULL DEFAULT 'not_requested'
);
CREATE TABLE IF NOT EXISTS user_strikes (
  event_key TEXT PRIMARY KEY, scope TEXT NOT NULL, user_key TEXT NOT NULL,
  reason TEXT NOT NULL, created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS strikes_user ON user_strikes(scope,user_key,created);
CREATE TABLE IF NOT EXISTS pending_mute_proposals (
  id TEXT PRIMARY KEY, scope TEXT NOT NULL, user_key TEXT NOT NULL, identity BLOB NOT NULL,
  reason TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL,
  status TEXT NOT NULL, reviewed REAL
);
CREATE INDEX IF NOT EXISTS proposals_user ON pending_mute_proposals(scope,user_key,created);
"""
REASONS = {"prompt_injection": "提示词注入试探", "content_blocked": "不安全或骚扰内容"}
log = logging.getLogger("hakimi")


class Moderation:
    def __init__(self, service, *, clock=time.time):
        self.service, self.s, self.store = service, service.s, service.store
        self.clock = clock
        self.lock = asyncio.Lock()
        self.init_lock = asyncio.Lock()
        self.initialized = False
        self.scope = self.store.vault.digest("moderation", self.s.namespace, self.s.onebot_self_id)

    def target_id(self, value):
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise Rejected("invalid_qq_id")
        uid = str(value)
        if not re.fullmatch(r"[1-9][0-9]{0,19}", uid):
            raise Rejected("invalid_qq_id")
        protected = {int(v) for v in (self.s.owner_qq_id, self.s.onebot_self_id) if v}
        if int(uid) in protected:
            raise Rejected("protected_identity")
        return uid

    def key(self, uid):
        return self.store.vault.digest(self.scope, uid)

    def nickname(self, value):
        text = clean_text(str(value))[:80]
        if any(
            secret and len(secret) >= 8 and secret in text for secret in configured_secrets(self.s)
        ):
            return "群友"
        return re.sub(r"[^\w\u4e00-\u9fff .-]", "", text)[:30].strip() or "群友"

    async def initialize(self):
        async with self.init_lock:
            if not self.initialized:
                await self._initialize()
                self.initialized = True

    async def _initialize(self):
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
        async with self.store.transaction():
            for uid in {self.s.owner_qq_id, self.s.onebot_self_id} - {""}:
                key = self.key(uid)
                for table in ("muted_users", "user_strikes", "pending_mute_proposals"):
                    await self.store.db.execute(
                        f"DELETE FROM {table} WHERE scope=? AND user_key=?", (self.scope, key)
                    )
        async with self.store.lock:
            rows = await self.store.all("SELECT * FROM muted_users WHERE scope=?", (self.scope,))
        ids = set()
        for row in rows:
            identity = self.store.vault.open(row["user_key"], row["identity"])
            try:
                ids.add(self.target_id(identity["user_id"]))
            except Rejected:
                # Configuration changes must never mute the new Owner or the bot itself.
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM muted_users WHERE user_key=?", (row["user_key"],)
                    )
        identity = getattr(self.store.runtime, "identity", None)
        if identity is not None and getattr(identity, "initialized", False):
            ids = ids | identity.muted_onebot_user_ids()
        self.s.muted_user_ids = ids
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE muted_users SET delete_status='uncertain' "
                "WHERE scope=? AND delete_status='attempted'",
                (self.scope,),
            )
        if identity is not None and getattr(identity, "initialized", False):
            for uid in sorted(ids):
                await identity.sync_account_muted("onebot", uid, True, moderation=self)

    async def record_strike(self, message, reason, nickname=""):
        # Only evidence about this CURRENT authenticated group message belongs here.
        # The caller must not pass a failed check of history, sources or generated output.
        if (
            message.platform != "onebot"
            or message.bot_id != self.s.onebot_self_id
            or message.kind != "group"
            or reason not in REASONS
        ):
            return None
        try:
            uid = self.target_id(message.user_id)
        except Rejected:
            return None
        if uid in self.s.muted_user_ids:
            return None
        await self.initialize()
        now, user_key = self.clock(), self.key(uid)
        nickname = self.nickname(nickname or getattr(message, "sender_nickname", ""))
        proposal = None
        async with self.lock, self.store.transaction():
            await self.store.db.execute("DELETE FROM user_strikes WHERE created<?", (now - 86400,))
            cur = await self.store.db.execute(
                "INSERT OR IGNORE INTO user_strikes VALUES (?,?,?,?,?)",
                (self.store.event_key(message), self.scope, user_key, reason, now),
            )
            if cur.rowcount != 1:
                return None
            count = (
                await self.store.one(
                    "SELECT COUNT(*) FROM user_strikes WHERE scope=? AND user_key=? AND created>=?",
                    (self.scope, user_key, now - 86400),
                )
            )[0]
            recent = await self.store.one(
                "SELECT 1 FROM pending_mute_proposals WHERE scope=? AND user_key=? "
                "AND (created>? OR (status='pending' AND expires>?))",
                (self.scope, user_key, now - 86400, now),
            )
            if count >= 3 and not recent:
                proposal = uuid.uuid4().hex[:12]
                identity = {"user_id": uid, "nickname": nickname}
                await self.store.db.execute(
                    "INSERT INTO pending_mute_proposals VALUES (?,?,?,?,?,?,?,'pending',NULL)",
                    (
                        proposal,
                        self.scope,
                        user_key,
                        self.store.vault.seal(proposal, identity),
                        reason,
                        now,
                        now + 172800,
                    ),
                )
        if proposal and self.s.owner_delivery_platform == "onebot":
            text = (
                f"BOISER，哈基米想请你评评理喵。{nickname}，QQ {uid}，"
                f"在 24 小时内至少 3 次触发安全拦截，最近类别：{REASONS[reason]}。"
                "目前还没有屏蔽，也没有删除好友。\n"
                f"申请编号：{proposal}\n/mute-approve {proposal} 批准应用层屏蔽\n"
                f"/mute-reject {proposal} 驳回\n"
                "如需同时移除好友，请明确在批准命令末尾加 --delete-friend。"
            )
            try:
                await self.service.proactive.alert("mute_proposal_" + proposal, text)
            except Exception:
                log.info("event=moderation_notice_failed")
        return proposal

    async def _mute(self, uid, source):
        uid = self.target_id(uid)
        key = self.key(uid)
        await self.store.db.execute(
            "INSERT OR IGNORE INTO muted_users(user_key,scope,identity,created,source) "
            "VALUES (?,?,?,?,?)",
            (key, self.scope, self.store.vault.seal(key, {"user_id": uid}), self.clock(), source),
        )

    async def mute_by_affection(self, uid: str, nickname: str = "") -> bool:
        await self.initialize()
        if isinstance(uid, bool) or not isinstance(uid, (str, int)):
            return False
        uid_str = str(uid)
        if uid_str in (set(self.s.onebot_owner_ids) - {""}):
            return False
        try:
            uid = self.target_id(uid_str)
        except Rejected:
            return False
        key = self.key(uid)
        clean_nick = self.nickname(nickname) if nickname else ""
        identity = {"user_id": uid, **({"nickname": clean_nick} if clean_nick else {})}
        async with self.lock:
            async with self.store.transaction():
                cur = await self.store.db.execute(
                    "INSERT OR IGNORE INTO muted_users(user_key,scope,identity,created,source) "
                    "VALUES (?,?,?,?,?)",
                    (
                        key,
                        self.scope,
                        self.store.vault.seal(key, identity),
                        self.clock(),
                        "ai_affection_zero",
                    ),
                )
            self.s.muted_user_ids = set(self.s.muted_user_ids) | {uid}
        identity = getattr(self.store.runtime, "identity", None)
        if identity is not None:
            await identity.sync_account_muted(
                "onebot",
                uid,
                True,
                source="ai_affection_zero",
                nickname=clean_nick,
                moderation=self,
            )
        return cur.rowcount == 1

    async def is_affection_muted(self, uid: str) -> bool:
        await self.initialize()
        try:
            uid = self.target_id(uid)
        except Rejected:
            return False
        async with self.store.lock:
            row = await self.store.one(
                "SELECT source FROM muted_users WHERE user_key=? AND scope=?",
                (self.key(uid), self.scope),
            )
        return bool(row and row["source"] == "ai_affection_zero")

    async def set_muted(self, uid, muted=True, *, delete_friend=False):
        await self.initialize()
        uid = self.target_id(uid)
        if delete_friend and not muted:
            raise Rejected("invalid_mute_action")
        async with self.lock:
            async with self.store.transaction():
                if muted:
                    await self._mute(uid, "manual")
                else:
                    await self.store.db.execute(
                        "DELETE FROM muted_users WHERE user_key=?", (self.key(uid),)
                    )
            ids = set(self.s.muted_user_ids)
            ids.add(uid) if muted else ids.discard(uid)
            self.s.muted_user_ids = ids
            deletion = await self._delete_friend(uid) if delete_friend else "not_requested"
        identity = getattr(self.store.runtime, "identity", None)
        if identity is not None:
            await identity.sync_account_muted(
                "onebot", uid, bool(muted), source="manual", moderation=self
            )
        if not muted:
            aff = getattr(self.store.runtime, "affection", None)
            if aff is not None:
                await aff.on_unmute(uid)
        return {"user_id": uid, "muted": muted, "delete_status": deletion}

    async def review(self, proposal, approve, *, delete_friend=False):
        await self.initialize()
        if not re.fullmatch(r"[a-f0-9]{12}", proposal):
            raise Rejected("invalid_proposal_id")
        if delete_friend and not approve:
            raise Rejected("invalid_mute_action")
        async with self.lock:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT * FROM pending_mute_proposals WHERE id=? AND scope=?",
                    (proposal, self.scope),
                )
                if not row:
                    raise Rejected("proposal_not_found")
                identity = self.store.vault.open(proposal, row["identity"])
                uid = self.target_id(identity["user_id"])
                if row["status"] != "pending" or row["expires"] <= self.clock():
                    raise Rejected("proposal_already_resolved_or_expired")
                await self.store.db.execute(
                    "UPDATE pending_mute_proposals SET status=?,reviewed=? WHERE id=?",
                    ("approved" if approve else "rejected", self.clock(), proposal),
                )
                if approve:
                    await self._mute(uid, "proposal")
            if approve:
                self.s.muted_user_ids = set(self.s.muted_user_ids) | {uid}
            deletion = await self._delete_friend(uid) if delete_friend else "not_requested"
        if approve:
            identity = getattr(self.store.runtime, "identity", None)
            if identity is not None:
                await identity.sync_account_muted(
                    "onebot", uid, True, source="proposal", moderation=self
                )
        return {
            "id": proposal,
            "status": "approved" if approve else "rejected",
            "delete_status": deletion,
        }

    async def _delete_friend(self, uid):
        # This is never called from strike collection or any model output. The sole
        # callers have an explicit human approval including the deletion option.
        uid = self.target_id(uid)
        key = self.key(uid)
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT delete_status FROM muted_users WHERE user_key=?", (key,)
            )
            if not row:
                raise Rejected("friend_delete_requires_mute_approval")
            if row[0] in {"attempted", "done", "uncertain"}:
                return row[0]
            await self.store.db.execute(
                "UPDATE muted_users SET delete_status='requested' WHERE user_key=?", (key,)
            )
        status = "unavailable"
        try:
            friends = await self.service.sender.onebot.call("get_friend_list", {})
            if isinstance(friends, dict):
                friends = friends.get("friends")
            if not isinstance(friends, list):
                raise Rejected("onebot_invalid_friend_list")
            present = any(
                isinstance(item, dict) and str(item.get("user_id")) == uid for item in friends
            )
            if not present:
                status = "not_friend"
            else:
                # Repeat the numeric hard guard at the actual destructive action boundary.
                self.target_id(uid)
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE muted_users SET delete_status='attempted' WHERE user_key=?", (key,)
                    )
                status = "uncertain"
                await self.service.sender.onebot.call("delete_friend", {"user_id": int(uid)})
                status = "done"
        except asyncio.CancelledError:
            raise
        except Exception:
            log.info("event=friend_delete_unavailable")
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE muted_users SET delete_status=? WHERE user_key=?", (status, key)
            )
        return status

    async def command(self, message):
        direct = message.command_text if message.command_text is not None else message.text
        if not re.match(r"^/(?:mute-approve|mute-reject|mute|unmute)(?:\s|$)", direct):
            return None
        if (
            message.platform != "onebot"
            or message.kind != "private"
            or not is_persona_owner(message, self.s)
        ):
            raise Rejected("owner_private_only")
        match = re.fullmatch(
            r"/(mute-approve|mute-reject|mute|unmute) ([a-f0-9]+)( --delete-friend)?", direct
        )
        if not match:
            return (
                "用法：/mute <QQ>、/unmute <QQ>、/mute-approve <编号> 或 /mute-reject <编号>。"
                "仅批准或屏蔽可加 --delete-friend。"
            )
        action, target, option = match.groups()
        if action in {"mute-approve", "mute-reject"}:
            result = await self.review(target, action == "mute-approve", delete_friend=bool(option))
            text = (
                "已批准应用层屏蔽。" if result["status"] == "approved" else "已驳回，本次不屏蔽。"
            )
        else:
            result = await self.set_muted(target, action == "mute", delete_friend=bool(option))
            text = (
                "已在应用层屏蔽该 QQ。"
                if result["muted"]
                else "已解除应用层屏蔽，仍需满足准入白名单。"
            )
        if option:
            text += "好友移除状态：" + result["delete_status"] + "。"
        return text

    async def status(self):
        await self.prune()
        async with self.store.lock:
            muted = await self.store.all(
                "SELECT * FROM muted_users WHERE scope=? ORDER BY created DESC LIMIT 1000",
                (self.scope,),
            )
            proposals = await self.store.all(
                "SELECT * FROM pending_mute_proposals WHERE scope=? "
                "ORDER BY created DESC LIMIT 100",
                (self.scope,),
            )
        return {
            "muted_users": [
                {
                    **self.store.vault.open(r["user_key"], r["identity"]),
                    "created": r["created"],
                    "source": r["source"],
                    "delete_status": r["delete_status"],
                }
                for r in muted
            ],
            "proposals": [
                {
                    **self.store.vault.open(r["id"], r["identity"]),
                    "id": r["id"],
                    "reason": REASONS[r["reason"]],
                    "created": r["created"],
                    "expires": r["expires"],
                    "status": r["status"],
                }
                for r in proposals
            ],
        }

    async def prune(self):
        await self.initialize()
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute("DELETE FROM user_strikes WHERE created<?", (now - 86400,))
            await self.store.db.execute(
                "UPDATE pending_mute_proposals SET status='expired' "
                "WHERE status='pending' AND expires<=?",
                (now,),
            )
            await self.store.db.execute(
                "DELETE FROM pending_mute_proposals WHERE created<?", (now - 8 * 86400,)
            )
