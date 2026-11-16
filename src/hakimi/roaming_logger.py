"""A diary of public patrols and allowlisted group/private chats."""

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .bot_server import is_persona_owner
from .config import Settings
from .drive_client import FolderDrive
from .gemini import GeminiGateway
from .generation_policy import Scenario
from .models import Message
from .security import Rejected, authorize, clean_text, configured_secrets, safe_output
from .store import Store

SCHEMA = """
CREATE TABLE IF NOT EXISTS roaming_events (
  id TEXT PRIMARY KEY, day TEXT NOT NULL, payload BLOB NOT NULL,
  event_kind TEXT NOT NULL DEFAULT 'patrol'
);
CREATE INDEX IF NOT EXISTS roaming_events_day ON roaming_events(day);
CREATE TABLE IF NOT EXISTS roaming_receipts (id TEXT PRIMARY KEY, day TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS roaming_counts (
  day TEXT NOT NULL, kind TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY(day,kind)
);
CREATE TABLE IF NOT EXISTS roaming_sync (
  day TEXT PRIMARY KEY, digest TEXT, file_id TEXT, slot TEXT,
  next_attempt REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS roaming_summaries (
  day TEXT PRIMARY KEY, source_digest TEXT NOT NULL, payload BLOB,
  status TEXT NOT NULL CHECK(status IN ('attempted','done','failed')), created REAL NOT NULL
);
"""
GROUP_TEXT = {
    "group_chat": "群友与哈基米聊了聊日常话题",
    "group_image": "群友发来图片，一起进行图片交流",
    "group_bilibili": "群友分享了 B 站公开内容",
    "private_chat": "私聊日常交流",
}
_MARKDOWN_SAFE = str.maketrans({"<": "＜", ">": "＞", "[": "［", "]": "］", "`": "｀", "!": "！"})


def redact_public_summary(text: str) -> str:
    # Defense in depth; callers may supply only already-screened public metadata summaries.
    text = clean_text(text)[:3000]
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[联系方式已隐去]", text)
    text = re.sub(r"(?<!\w)\d{5,}(?!\w)", "[号码已隐去]", text)
    text = re.sub(r"@[\w\u4e00-\u9fff-]+", "[称呼已隐去]", text)
    # Diaries are plain Markdown; disable HTML, images, links from external text and fences.
    text = re.sub(r"https?://\S+", "[链接省略]", text)
    return text.translate(_MARKDOWN_SAFE)


def format_chat_entry(
    text: str, secrets: list[str], *, limit: int = 1200, scrub_secrets: bool = False
) -> str:
    """Sanitize chat/summary text for Drive Markdown without stripping speakers."""
    cleaned = clean_text(text)
    if scrub_secrets:
        candidates = {
            val for s in secrets if s and len(s) >= 8 for val in (s, clean_text(s)) if len(val) >= 8
        }
        for secret in sorted(candidates, key=len, reverse=True):
            if secret in cleaned:
                cleaned = cleaned.replace(secret, "[密钥已隐去]")
    cleaned = safe_output(cleaned, secrets, limit=limit)
    return cleaned.translate(_MARKDOWN_SAFE)


class RoamingLogger:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        drive: FolderDrive | None = None,
        *,
        gateway: GeminiGateway | None = None,
    ):
        self.s, self.store, self.drive = settings, store, drive
        self.gateway = gateway
        self.zone = ZoneInfo(settings.roaming_timezone)
        self.sync_lock = asyncio.Lock()

    async def initialize(self):
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
            columns = {row[1] for row in await self.store.all("PRAGMA table_info(roaming_sync)")}
            for name, kind in [("last_error", "TEXT"), ("last_success", "REAL")]:
                if name not in columns:
                    await self.store.db.execute(
                        f"ALTER TABLE roaming_sync ADD COLUMN {name} {kind}"
                    )
            event_cols = {
                row[1] for row in await self.store.all("PRAGMA table_info(roaming_events)")
            }
            if "event_kind" not in event_cols:
                await self.store.db.execute(
                    "ALTER TABLE roaming_events "
                    "ADD COLUMN event_kind TEXT NOT NULL DEFAULT 'patrol'"
                )

    def local(self, now: float | None = None) -> datetime:
        return datetime.fromtimestamp(time.time() if now is None else now, self.zone)

    async def _record(
        self,
        event_id: str,
        kind: str,
        text: str | None = None,
        *,
        event_kind: str = "patrol",
        now=None,
    ):
        if not self.s.roaming_enabled:
            return
        day = self.local(now).date().isoformat()
        key = self.store.vault.digest("diary-event", event_id, kind)
        async with self.store.transaction():
            if await self.store.one("SELECT 1 FROM roaming_receipts WHERE id=?", (key,)):
                return
            await self.store.db.execute("INSERT INTO roaming_receipts VALUES (?,?)", (key, day))
            if text is not None:
                count = await self.store.one(
                    "SELECT COUNT(*) FROM roaming_events WHERE day=?", (day,)
                )
                if count[0] < self.s.roaming_max_events_per_day:
                    await self.store.db.execute(
                        "INSERT INTO roaming_events(id,day,payload,event_kind) VALUES (?,?,?,?)",
                        (key, day, self.store.vault.seal(key, text), event_kind),
                    )
                else:
                    kind = "patrol_omitted"
            await self.store.db.execute(
                "INSERT INTO roaming_counts VALUES (?,?,1) "
                "ON CONFLICT(day,kind) DO UPDATE SET n=n+1",
                (day, kind),
            )
            await self.store.db.execute(
                "INSERT OR IGNORE INTO roaming_sync(day) VALUES (?)", (day,)
            )

    async def injection(self, event_id: str, *, now=None):
        # No attempted prompt, user ID, group ID or private text enters the diary.
        await self._record(event_id, "injection", now=now)

    async def patrol(self, event_id: str, public_summary: str, *, now=None):
        await self._record(
            event_id, "patrol", redact_public_summary(public_summary), event_kind="patrol", now=now
        )

    def _chat_scope_label(self, message: Message) -> tuple[str, str]:
        if message.kind == "group":
            name = getattr(message, "group_name", "") or message.conversation_id
            return "chat_group", f"群聊 {name}"
        if is_persona_owner(message, self.s):
            return "chat_private", "私聊 · BOISER"
        label = message.sender_nickname or message.user_id
        return "chat_private", f"私聊 · {label}"

    async def group(
        self, event_id: str, message: Message, *, members: list[Message] | None = None, now=None
    ):
        if not authorize(message.model_copy(update={"addressed": True}), self.s):
            return
        if message.kind == "group":
            kind = (
                "group_bilibili"
                if message.video_ids or message.short_urls
                else "group_image"
                if message.images
                else "group_chat"
            )
        else:
            kind = "private_chat"
        event_kind, scope_label = self._chat_scope_label(message)
        ts = self.local(now if now is not None else message.created_at).strftime("%H:%M")
        lines = []
        for item in (members or [message])[:8]:
            speaker = (
                "BOISER"
                if is_persona_owner(item, self.s)
                else (item.sender_nickname or item.user_id)
            )
            body = str(item.text or "").strip()
            if item.images and "[附图]" not in body:
                body = f"{body} [附图]".strip()
            if body:
                lines.append(f"[{ts} · {scope_label}] {speaker}：{body}")
        text = None
        if lines:
            try:
                text = format_chat_entry(
                    "\n".join(lines), configured_secrets(self.s), scrub_secrets=True
                )
            except Rejected:
                text = None
        await self._record(event_id, kind, text, event_kind=event_kind, now=now)

    async def bot_reply(self, reply_id: str, message: Message, text: str, *, now=None):
        if not text or not text.strip():
            return
        if not authorize(message.model_copy(update={"addressed": True}), self.s):
            return
        event_kind, scope_label = self._chat_scope_label(message)
        ts = self.local(now).strftime("%H:%M")
        raw = f"[{ts} · {scope_label}] 哈基米：{text.strip()}"
        try:
            formatted = format_chat_entry(raw, configured_secrets(self.s))
        except Rejected:
            return
        await self._record(
            "reply:" + reply_id, "bot_reply", formatted, event_kind=event_kind, now=now
        )

    def _summary_input(self, day: str, counts: dict, rows: list) -> dict:
        patrol_rows = [
            r
            for r in rows
            if ("event_kind" not in r.keys() or (r["event_kind"] or "patrol") == "patrol")
        ]
        chat_rows = [
            r
            for r in rows
            if "event_kind" in r.keys() and (r["event_kind"] or "patrol") != "patrol"
        ]
        payload = {
            "day": day,
            "group_activity": {label: counts.get(kind, 0) for kind, label in GROUP_TEXT.items()},
            "injection_attempts": counts.get("injection", 0),
            "public_summaries": [],
            "chat_excerpts": [],
            "omitted_public_summaries": counts.get("patrol_omitted", 0) + len(patrol_rows),
        }
        for row in reversed(patrol_rows[-20:]):
            excerpt = self.store.vault.open(row["id"], row["payload"])[:500]
            payload["public_summaries"].append(excerpt)
            payload["omitted_public_summaries"] -= 1
            if len(json.dumps(payload, ensure_ascii=False).encode()) > self.s.max_context_bytes:
                payload["public_summaries"].pop()
                payload["omitted_public_summaries"] += 1
                break
        for row in reversed(chat_rows[-25:]):
            excerpt = self.store.vault.open(row["id"], row["payload"])[:400]
            payload["chat_excerpts"].append(excerpt)
            if len(json.dumps(payload, ensure_ascii=False).encode()) > self.s.max_context_bytes:
                payload["chat_excerpts"].pop()
                break
        payload["public_summaries"].reverse()
        payload["chat_excerpts"].reverse()
        return payload

    async def _daily_summary(self, day: str, now: float):
        if self.gateway is None:
            return
        key = self.store.vault.digest("daily-summary", self.s.namespace, day)
        async with self.store.transaction():
            if await self.store.one("SELECT 1 FROM roaming_summaries WHERE day=?", (day,)):
                return
            counts = dict(
                await self.store.all("SELECT kind,n FROM roaming_counts WHERE day=?", (day,))
            )
            rows = await self.store.all(
                "SELECT id,payload,event_kind FROM roaming_events WHERE day=? ORDER BY rowid",
                (day,),
            )
            if not counts and not rows:
                return
            payload = self._summary_input(day, counts, rows)
            # Persist BEFORE any paid call: crashes, ambiguous failures and Drive retries
            # must never automatically pay for a second summary of the same day.
            await self.store.db.execute(
                "INSERT INTO roaming_summaries VALUES (?,?,NULL,'attempted',?)",
                (day, self.store.vault.digest("diary-input", payload), now),
            )
        subject = self.store.vault.digest("diary-budget", self.s.namespace)
        try:
            await self.gateway.screen(payload, subject, purpose="source", budget_lane="patrol")
            generated = await self.gateway.generate(
                payload, subject, scenario=Scenario.DAILY_DIARY, budget_lane="patrol"
            )
            summary = format_chat_entry(generated, configured_secrets(self.s), limit=3000)
        except asyncio.CancelledError:
            # Keep attempted on cancellation/restart; do not repeat ambiguous paid work.
            raise
        except Exception as exc:
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE roaming_summaries SET status='failed' WHERE day=?", (day,)
                )
            if isinstance(exc, Rejected) and str(exc) == "prompt_injection":
                await self.injection("daily-summary:" + day, now=now)
            logging.getLogger("hakimi").info("event=diary_summary_unavailable")
        else:
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE roaming_summaries SET status='done',payload=? WHERE day=?",
                    (self.store.vault.seal(key, summary), day),
                )

    async def render(self, day: str) -> str:
        async with self.store.lock:
            counts = dict(
                await self.store.all("SELECT kind,n FROM roaming_counts WHERE day=?", (day,))
            )
            rows = await self.store.all(
                "SELECT id,payload,event_kind FROM roaming_events WHERE day=? ORDER BY rowid",
                (day,),
            )
        patrol_rows = [r for r in rows if (r["event_kind"] or "patrol") == "patrol"]
        chat_rows = [r for r in rows if (r["event_kind"] or "patrol") != "patrol"]
        lines = [
            f"# 分身巡逻日记 · {day}",
            "",
            f"时区：{self.s.roaming_timezone}",
            "",
            "## B 站公开内容巡逻",
            "",
            "仅基于公开标题和简介，未观看全片。",
            "",
        ]
        omitted = counts.get("patrol_omitted", 0)
        used_bytes = 0
        if patrol_rows:
            for index, row in enumerate(patrol_rows):
                summary = self.store.vault.open(row["id"], row["payload"])
                quoted = ["> " + line for line in summary.splitlines()]
                used_bytes += len("\n".join(quoted).encode()) + 2
                if used_bytes > 196608:
                    omitted += len(patrol_rows) - index
                    break
                lines += quoted
                lines.append("")
        else:
            lines += ["今天暂无新的公开内容摘要。", ""]
        lines += ["## 白名单聊天记录（群聊与私聊）", ""]
        activities = [
            f"- {text}：{counts[kind]} 次。"
            for kind, text in GROUP_TEXT.items()
            if counts.get(kind)
        ]
        if activities:
            lines += activities
            lines.append("")
        if chat_rows:
            for index, row in enumerate(chat_rows):
                entry = self.store.vault.open(row["id"], row["payload"])
                quoted = ["> " + line for line in entry.splitlines()]
                used_bytes += len("\n".join(quoted).encode()) + 2
                if used_bytes > 196608:
                    omitted += len(chat_rows) - index
                    break
                lines += quoted
                lines.append("")
        elif not activities:
            lines += ["今天暂无白名单聊天记录。", ""]
        if omitted:
            lines += [f"另有 {omitted} 条记录因日记长度限制省略。", ""]
        lines += [
            "## 安全拦截",
            "",
            f"检测到的提示词注入试探：{counts.get('injection', 0)} 次。",
            "",
        ]
        async with self.store.lock:
            summary = await self.store.one("SELECT * FROM roaming_summaries WHERE day=?", (day,))
        if summary:
            lines += ["## 每日总结", ""]
            if summary["status"] == "done":
                key = self.store.vault.digest("daily-summary", self.s.namespace, day)
                text = self.store.vault.open(key, summary["payload"])
                lines += ["> " + line for line in text.splitlines()]
                snapshot = self.local(summary["created"]).strftime("%Y-%m-%d %H:%M")
                lines += ["", f"基于 {snapshot} 的日志快照；后续记录见上方明细。", ""]
            else:
                lines += ["当日模型总结未完成；以上保留已记录的日志明细。", ""]
        return "\n".join(lines)

    async def sync_due(self, *, now: float | None = None, force=False):
        if not self.s.roaming_enabled or self.drive is None:
            return
        now = time.time() if now is None else now
        local = self.local(now)
        day = local.date().isoformat()
        # The 23:55 flush has its own persistent slot, independent of the two-hour cadence.
        slot = (
            "final"
            if (local.hour, local.minute) >= (23, 55)
            else str(
                int(
                    (
                        local - local.replace(hour=0, minute=0, second=0, microsecond=0)
                    ).total_seconds()
                )
                // self.s.roaming_interval_seconds
            )
        )
        async with self.sync_lock:
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO roaming_sync(day) VALUES (?)", (day,)
                )
                rows = await self.store.all(
                    "SELECT * FROM roaming_sync WHERE day=?"
                    if force
                    else "SELECT * FROM roaming_sync WHERE day>=? AND next_attempt<=? ORDER BY day",
                    (day,) if force else ((local.date() - timedelta(days=7)).isoformat(), now),
                )
            for row in rows:
                current_slot = slot if row["day"] == day else "closed"
                # Finished days are rechecked: events can arrive after the final flush.
                if not force and row["day"] == day and row["slot"] == current_slot:
                    continue
                if force or current_slot in {"final", "closed"}:
                    await self._daily_summary(row["day"], now)
                markdown = await self.render(row["day"])
                digest = hashlib.sha256(markdown.encode()).hexdigest()
                if digest == row["digest"]:
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET slot=? WHERE day=?", (current_slot, row["day"])
                        )
                    continue
                try:
                    file_id = await self.drive.sync_day(row["day"], markdown)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    delay = min(7200, 60 * 2 ** min(row["failures"], 7))
                    reason = (
                        str(exc)
                        if isinstance(exc, Rejected)
                        and str(exc)
                        in {
                            "drive_credentials_invalid",
                            "drive_shared_folder_required",
                            "drive_request_failed",
                            "drive_file_outside_scope",
                            "drive_ambiguous_file",
                            "drive_token_failed",
                            "drive_folder_unavailable",
                            "drive_oauth_credentials_invalid",
                            "drive_oauth_folder_mismatch",
                            "drive_oauth_scope_mismatch",
                            "drive_oauth_reauthorization_required",
                            "drive_oauth_client_invalid",
                        }
                        else "drive_sync_failed"
                    )
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET "
                            "next_attempt=?,failures=failures+1,last_error=? "
                            "WHERE day=?",
                            (now + delay, reason, row["day"]),
                        )
                    logging.getLogger("hakimi").info("event=diary_sync_pending")
                else:
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET digest=?,file_id=?,slot=?,"
                            "next_attempt=0,failures=0,last_error=NULL,last_success=? WHERE day=?",
                            (digest, file_id, current_slot, now, row["day"]),
                        )

    async def run(self):
        while True:
            try:
                await self.sync_due()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("hakimi").info("event=diary_worker_error")
            await asyncio.sleep(30)

    async def prune(self):
        cutoff = (self.local().date() - timedelta(days=8)).isoformat()
        async with self.store.transaction():
            for table in (
                "roaming_events",
                "roaming_receipts",
                "roaming_counts",
                "roaming_sync",
                "roaming_summaries",
            ):
                await self.store.db.execute(f"DELETE FROM {table} WHERE day<?", (cutoff,))
