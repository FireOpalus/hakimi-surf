"""Opt-in PUBLIC group Markdown mirror. Never exports or queries a private session."""

import asyncio
import hashlib
import re
import time
from contextlib import nullcontext
from datetime import datetime
from zoneinfo import ZoneInfo

from .memory_compressor import check_memory_text
from .security import Rejected
from .social import chat_module, chat_registry
from .work_usage import model_work

HEADER = (
    "# 哈基米 · 群聊双轨记忆\n\n修改条目正文或删除整个条目可同步回本地；请保留文件与条目锚点。\n"
)
PATTERN = re.compile(r"<!-- entry:([a-z0-9:]+) -->\n## [^\n]+\n([^<>]*?)\n<!-- /entry -->\n?", re.S)


def visible_entries(view):
    return {
        e["id"]: e["text"]
        for e in view["entries"]
        if not e["deleted"] and (not e["expires"] or e["expires"] > time.time())
    }


def render(scope, entries):
    body = f"<!-- hakimi-memory:1:{scope} -->\n" + HEADER
    for ident, text in sorted(entries.items()):
        # Markdown is human-readable plain content, never executable markup or remote media.
        clean = text.replace("<", "＜").replace(">", "＞").replace("\r", "").strip()
        body += f"\n<!-- entry:{ident} -->\n## {ident}\n{clean}\n<!-- /entry -->\n"
    return body


def parse(scope, markdown, known):
    prefix = f"<!-- hakimi-memory:1:{scope} -->\n" + HEADER
    if not markdown.startswith(prefix) or len(markdown.encode()) > 262144:
        raise Rejected("memory_mirror_format_invalid")
    remaining = markdown[len(prefix) :]
    entries = {}
    for match in PATTERN.finditer(remaining):
        ident, text = match.groups()
        if (
            ident not in known
            or ident in entries
            or not text.strip()
            or len(text.strip()) > (300 if ident.startswith("p:") else 500)
        ):
            raise Rejected("memory_mirror_entry_invalid")
        entries[ident] = text.strip()
    if PATTERN.sub("", remaining).strip():
        raise Rejected("memory_mirror_format_invalid")
    return entries


def fingerprint(markdown):
    return hashlib.sha256(markdown.encode()).hexdigest()


class MemoryMirror:
    def __init__(self, service, *, drive=None, transfer=nullcontext):
        self.drive = drive if drive is not None else getattr(service, "drive", None)
        self.transfer = transfer
        self.svc = service
        self.s, self.sessions, self.store = service.s, service.sessions, service.store
        self.locks = [asyncio.Lock() for _ in range(32)]
        self.last_scan = float("-inf")

    def allowed(self, message):
        if message.kind != "group":
            return False
        if chat_registry(self.s) is None:
            # Legacy standalone maintenance callers have only the configured QQ transports.
            from .modules.platforms.qq.chat_policy import permits_memory_mirror

            return permits_memory_mirror(message, self.s)
        module = chat_module(self.s, message.platform)
        return bool(module and module.permits_memory_mirror(message, self.s))

    async def configure(self, message, enabled, version):
        if message.kind != "group" or (enabled and not self.allowed(message)):
            raise Rejected("memory_mirror_group_only")
        async with self.sessions.database(message, preserve_history=True) as (db, _):
            cur = await db.execute(
                "UPDATE rolling_state SET version=version+1 WHERE version=?", (version,)
            )
            if cur.rowcount != 1:
                raise Rejected("session_changed")
            await db.execute("UPDATE memory_control SET mirror_enabled=?", (int(enabled),))
        return {"status": "saved"}

    async def state(self, message):
        async with self.sessions.database(message, preserve_history=True) as (db, key):
            async with db.execute("SELECT mirror_payload FROM memory_control") as cur:
                row = (await cur.fetchone())[0]
            return self.store.vault.open(key + ":mirror", row) if row else {}

    async def save_state(self, message, state, epoch):
        async with self.sessions.database(message, preserve_history=True) as (db, key):
            async with db.execute("SELECT epoch FROM rolling_state") as cur:
                if (await cur.fetchone())[0] != epoch:
                    raise Rejected("session_changed")
            await db.execute(
                "UPDATE memory_control SET mirror_payload=?",
                (self.store.vault.seal(key + ":mirror", state),),
            )

    async def sync(self, message, *, push=False):
        if not self.allowed(message):
            raise Rejected("memory_mirror_group_only")
        key = self.sessions.key(message)
        lock = self.locks[int(key[:8], 16) % len(self.locks)]
        async with lock:
            view = await self.sessions.dual.view(message)
            if not view["mirror_enabled"] or self.drive is None:
                raise Rejected("memory_mirror_disabled")
            state = await self.state(message)
            try:
                with model_work("memory.mirror_review", independent=True):
                    async with self.transfer():
                        # Another group can hold the Drive transfer while consent is revoked.
                        fresh = await self.sessions.dual.view(message)
                        if not self.allowed(message) or not fresh["mirror_enabled"]:
                            raise Rejected("memory_mirror_disabled")
                        if fresh["epoch"] != view["epoch"]:
                            raise Rejected("session_changed")
                        return await self._sync(message, key, fresh, state, push)
            except Exception as exc:
                # Only fixed internal labels, never remote text, URLs, secrets
                # or arbitrary exceptions.
                label = str(exc) if isinstance(exc, Rejected) else "memory_mirror_failed"
                state["last_error"] = label
                await self.save_state(message, state, view["epoch"])
                await self.store.runtime.event("memory_mirror_failed", "memory")
                raise

    async def _sync(self, message, scope, view, state, push):
        drive = self.drive
        head = await drive.memory_head(scope)
        if head:
            if not state.get("entries") and not state.get("digest"):
                # Unknown pre-existing files (including after /new-session) are never imported.
                raise Rejected("memory_mirror_unbound")
            remote = await drive.read_memory(scope, head)
            digest = fingerprint(remote)
            if digest != state.get("digest"):
                check_memory_text(remote, self.s)
                updated = parse(scope, remote, state.get("entries", {}))
                subject = self.store.vault.digest("memory-mirror-admin", scope)
                if state.get("review_digest") != digest:
                    # Persist before paid screening; unknown or failed attempts
                    # never replay unchanged data.
                    state.update(review_digest=digest, review_passed=False)
                    await self.save_state(message, state, view["epoch"])
                    chunk = ""
                    for line in remote.splitlines(keepends=True):
                        if len((chunk + line).encode()) > 5000:
                            await self.svc.ai.screen(
                                {"group_memory_markdown": chunk},
                                subject,
                                purpose="source",
                                budget_lane="admin",
                                audit_source="memory",
                            )
                            chunk = ""
                        chunk += line
                    if chunk:
                        await self.svc.ai.screen(
                            {"group_memory_markdown": chunk},
                            subject,
                            purpose="source",
                            budget_lane="admin",
                            audit_source="memory",
                        )
                    state["review_passed"] = True
                    await self.save_state(message, state, view["epoch"])
                if not state.get("review_passed"):
                    raise Rejected("memory_mirror_review_not_confirmed")
                current = {e["id"]: e for e in view["entries"]}
                edits = []
                for ident, previous in state.get("entries", {}).items():
                    if ident in updated and updated[ident] == previous:
                        continue
                    if (
                        ident not in current
                        or current[ident]["text"] != previous
                        or current[ident]["deleted"]
                    ):
                        raise Rejected("memory_mirror_local_conflict")
                    edits.append(
                        {
                            "id": ident,
                            "action": "edit" if ident in updated else "delete",
                            "text": updated.get(ident, ""),
                            "expires": current[ident]["expires"],
                        }
                    )
                if edits:
                    await self.sessions.dual.edit_many(message, edits, view["version"])
                state.update(
                    entries=updated, digest=digest, last_success=time.time(), last_error=None
                )
                await self.save_state(message, state, view["epoch"])
                view = await self.sessions.dual.view(message)
        elif state.get("digest"):
            # Removing the entire file is a conflict; never recreate it
            # automatically or erase all local data.
            raise Rejected("memory_mirror_remote_missing")
        if push:
            fresh = await self.sessions.dual.view(message)
            if (
                not self.allowed(message)
                or not fresh["mirror_enabled"]
                or fresh["epoch"] != view["epoch"]
            ):
                raise Rejected("session_changed")
            entries = visible_entries(fresh)
            markdown = render(scope, entries)
            # Compare rendered entries too: harmless human whitespace changes aren't overwritten.
            if head and entries == state.get("entries"):
                state.update(
                    export_day=datetime.now(ZoneInfo(self.s.bot_timezone)).date().isoformat(),
                    last_success=time.time(),
                    last_error=None,
                )
                await self.save_state(message, state, fresh["epoch"])
                return {"status": "unchanged"}
            check_memory_text(markdown, self.s)
            file_id = await drive.write_memory(scope, markdown, head)
            # Persist only the version actually exported; concurrent local changes
            # are uploaded next round.
            state.update(
                entries=entries,
                digest=fingerprint(markdown),
                file_id=file_id,
                export_day=datetime.now(ZoneInfo(self.s.bot_timezone)).date().isoformat(),
                last_success=time.time(),
                last_error=None,
            )
            await self.save_state(message, state, fresh["epoch"])
            return {"status": "pushed"}
        if head:
            state.update(last_success=time.time(), last_error=None)
            await self.save_state(message, state, view["epoch"])
            return {"status": "pulled"}
        if state.get("last_error") is not None:
            state["last_error"] = None
            await self.save_state(message, state, view["epoch"])
        return {"status": "not_exported"}

    async def scheduled(self):
        if time.monotonic() - self.last_scan < 3600:
            return
        self.last_scan = time.monotonic()
        for _, message in await self.store.runtime.sessions():
            if not self.allowed(message):
                continue
            view = await self.sessions.dual.view(message)
            if not view["mirror_enabled"]:
                continue
            try:
                state = await self.state(message)
                now = datetime.now(ZoneInfo(self.s.bot_timezone))
                due = (
                    now.strftime("%H:%M") >= self.s.memory_archive_time
                    and state.get("export_day") != now.date().isoformat()
                    and not view["jobs"].get("pending", 0)
                )
                await self.sync(message, push=due)
            except Exception:
                # Mirror errors do not interrupt chat or requeue paid extraction.
                pass

    async def run(self):
        while True:
            try:
                await self.scheduled()
            except Exception:
                await self.store.runtime.event("memory_mirror_failed", "memory")
            await asyncio.sleep(3600)
