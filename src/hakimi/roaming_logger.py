"""A diary of public patrols and allowlisted group/private chats."""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .bot_server import is_persona_owner
from .config import Settings
from .gemini import GeminiGateway
from .generation_policy import Scenario
from .models import Message
from .security import Rejected, authorize, clean_text, configured_secrets, safe_output
from .store import Store
from .work_usage import model_work

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
CREATE TABLE IF NOT EXISTS roaming_days (day TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS roaming_public_summaries (day TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS roaming_summaries (
  day TEXT PRIMARY KEY, source_digest TEXT NOT NULL, payload BLOB,
  status TEXT NOT NULL CHECK(status IN ('attempted','done','failed')), created REAL NOT NULL
);
"""
# The same eligibility check feeds offline summarization and upload readiness.
VISIBLE_COUNTS = (
    "(? OR kind IN ('patrol','world_event','group_chat','group_bilibili',"
    "'group_image','injection','patrol_omitted'))"
)
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


class PatrolDiary:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        *,
        gateway: GeminiGateway | None = None,
    ):
        self.s, self.store = settings, store
        self.gateway = gateway
        self.zone = ZoneInfo(settings.roaming_timezone)
        self.summary_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self):
        if self._initialized:
            return
        async with self.store.transaction():
            if self._initialized:
                return
            new_index = not await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='roaming_days'"
            )
            for statement in SCHEMA.strip().rstrip(";").split(";"):
                await self.store.db.execute(statement)
            await self.store.db.execute(
                "INSERT OR IGNORE INTO roaming_days SELECT day FROM roaming_counts "
                "UNION SELECT day FROM roaming_events UNION SELECT day FROM roaming_summaries"
            )
            if await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='roaming_sync'"
            ):
                # Import every legacy date once, and recognize old-binary writes after a rollback.
                since = "" if new_index else (self.local().date() - timedelta(days=8)).isoformat()
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO roaming_days SELECT day FROM roaming_sync WHERE day>=?",
                    (since,),
                )
            event_cols = {
                row[1] for row in await self.store.all("PRAGMA table_info(roaming_events)")
            }
            if "event_kind" not in event_cols:
                await self.store.db.execute(
                    "ALTER TABLE roaming_events "
                    "ADD COLUMN event_kind TEXT NOT NULL DEFAULT 'patrol'"
                )

        self._initialized = True

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
        await self.initialize()
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
                "INSERT OR IGNORE INTO roaming_days(day) VALUES (?)", (day,)
            )

    async def injection(self, event_id: str, *, now=None):
        # No attempted prompt, user ID, group ID or private text enters the diary.
        await self._record(event_id, "injection", now=now)

    async def patrol(self, event_id: str, public_summary: str, *, now=None):
        await self._record(
            event_id, "patrol", redact_public_summary(public_summary), event_kind="patrol", now=now
        )

    async def world_event(
        self, event_id: str, source_module: str, event_type: str, summary: str, *, now=None
    ):
        ts = self.local(now).strftime("%H:%M")
        raw = f"[{ts} · 模块 {source_module}/{event_type}] {summary}"
        for secret in sorted(
            (s for s in configured_secrets(self.s) if s and len(s) >= 6),
            key=len,
            reverse=True,
        ):
            raw = raw.replace(secret, "[密钥已隐去]")
        await self._record(
            event_id,
            "world_event",
            redact_public_summary(raw),
            event_kind="world_life",
            now=now,
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
        if not self.s.roaming_enabled:
            if message.kind == "group":
                await self._record(event_id, kind, now=now)
            return
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
        if not self.s.roaming_enabled or not text or not text.strip():
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

    async def _source_rows(self, day, *, archive_chats):
        counts = await self.store.all(
            f"SELECT kind,n FROM roaming_counts WHERE day=? AND {VISIBLE_COUNTS}",
            (day, archive_chats),
        )
        rows = await self.store.all(
            "SELECT id,payload,event_kind FROM roaming_events WHERE day=? "
            "AND (? OR event_kind IN ('patrol','world_life')) ORDER BY rowid",
            (day, archive_chats),
        )
        return dict(counts), rows

    def _summary_input(self, day: str, counts: dict, rows: list) -> dict:
        patrol_rows = [r for r in rows if "event_kind" in r.keys() and r["event_kind"] == "patrol"]
        world_rows = [
            r for r in rows if "event_kind" in r.keys() and r["event_kind"] == "world_life"
        ]
        chat_rows = [
            r
            for r in rows
            if self.s.roaming_enabled and r["event_kind"] in {"chat_group", "chat_private"}
        ]
        payload = {
            "day": day,
            "group_activity": {
                label: counts.get(kind, 0)
                for kind, label in GROUP_TEXT.items()
                if kind != "private_chat" or self.s.roaming_enabled
            },
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
        if world_rows:
            world_events: list[str] = []
            payload["world_events"] = world_events
            for row in reversed(world_rows[-20:]):
                excerpt = self.store.vault.open(row["id"], row["payload"])[:400]
                world_events.append(excerpt)
                if len(json.dumps(payload, ensure_ascii=False).encode()) > self.s.max_context_bytes:
                    world_events.pop()
                    break
            if world_events:
                world_events.reverse()
            else:
                payload.pop("world_events", None)
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
            counts, rows = await self._source_rows(day, archive_chats=self.s.roaming_enabled)
            if not counts and not rows:
                return
            payload = self._summary_input(day, counts, rows)
            # Persist BEFORE any paid call: crashes, ambiguous failures and Drive retries
            # must never automatically pay for a second summary of the same day.
            await self.store.db.execute(
                "INSERT INTO roaming_summaries VALUES (?,?,NULL,'attempted',?)",
                (day, self.store.vault.digest("diary-input", payload), now),
            )
            if not payload["chat_excerpts"] and not (
                self.s.roaming_enabled and counts.get("private_chat")
            ):
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO roaming_public_summaries(day) VALUES (?)", (day,)
                )
        subject = self.store.vault.digest("diary-budget", self.s.namespace)
        try:
            with model_work("diary.daily_summary", key):
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

    async def public_digest(self, day):
        await self.initialize()
        async with self.store.lock:
            counts, rows = await self._source_rows(day, archive_chats=False)
        payload = self._summary_input(day, counts, rows)
        payload["group_activity"].pop(GROUP_TEXT["private_chat"], None)
        return payload

    async def render(self, day: str) -> str:
        await self.initialize()
        async with self.store.lock:
            counts, rows = await self._source_rows(day, archive_chats=self.s.roaming_enabled)
        patrol_rows = [r for r in rows if (r["event_kind"] or "patrol") == "patrol"]
        world_rows = [r for r in rows if (r["event_kind"] or "patrol") == "world_life"]
        chat_rows = [
            r
            for r in rows
            if self.s.roaming_enabled and r["event_kind"] in {"chat_private", "chat_group"}
        ]
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
            if counts.get(kind) and (kind != "private_chat" or self.s.roaming_enabled)
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
        if world_rows:
            lines += ["## 🌍 今日生活与跨模块轨迹", ""]
            for index, row in enumerate(world_rows):
                entry = self.store.vault.open(row["id"], row["payload"])
                quoted = ["> " + line for line in entry.splitlines()]
                used_bytes += len("\n".join(quoted).encode()) + 2
                if used_bytes > 196608:
                    omitted += len(world_rows) - index
                    break
                lines += quoted
                lines.append("")
        if omitted:
            lines += [f"另有 {omitted} 条记录因日记长度限制省略。", ""]
        lines += [
            "## 安全拦截",
            "",
            f"检测到的提示词注入试探：{counts.get('injection', 0)} 次。",
            "",
        ]
        async with self.store.lock:
            summary = await self.store.one(
                "SELECT * FROM roaming_summaries WHERE day=? AND (? OR day IN "
                "(SELECT day FROM roaming_public_summaries))",
                (day, self.s.roaming_enabled),
            )
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

    async def prepare_due(self, *, now=None, force=False):
        await self.initialize()
        now = time.time() if now is None else now
        local = self.local(now)
        today = local.date().isoformat()
        async with self.summary_lock:
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO roaming_days(day) VALUES (?)", (today,)
                )
                if force:
                    days = [(today,)]
                else:
                    end = (
                        today
                        if (local.hour, local.minute) >= (23, 55)
                        else (local.date() - timedelta(days=1)).isoformat()
                    )
                    days = await self.store.all(
                        "SELECT DISTINCT day FROM roaming_counts WHERE day BETWEEN ? AND ? "
                        f"AND {VISIBLE_COUNTS} AND day NOT IN "
                        "(SELECT day FROM roaming_summaries) ORDER BY day",
                        (
                            (local.date() - timedelta(days=7)).isoformat(),
                            end,
                            self.s.roaming_enabled,
                        ),
                    )
            for (day,) in days:
                await self._daily_summary(day, now)

    async def markdown_for_sync(self, day, *, final):
        if not final or self.gateway is None:
            return await self.render(day)
        # Let the local producer commit its final attempt before the uploader seals this slot.
        async with self.summary_lock:
            async with self.store.lock:
                pending = await self.store.one(
                    f"SELECT 1 FROM roaming_counts WHERE day=? AND {VISIBLE_COUNTS} "
                    "AND day NOT IN (SELECT day FROM roaming_summaries) LIMIT 1",
                    (day, self.s.roaming_enabled),
                )
            return None if pending else await self.render(day)

    async def run_local(self):
        last_prune = None
        while True:
            try:
                await self.prepare_due()
                day = self.local().date()
                if last_prune != day:
                    await self.prune()
                    last_prune = day
            except Exception:
                logging.getLogger("hakimi").info("event=diary_worker_error")
            await asyncio.sleep(30)

    async def prune(self):
        cutoff = (self.local().date() - timedelta(days=8)).isoformat()
        async with self.store.transaction():
            for table in (
                "roaming_public_summaries",
                "roaming_events",
                "roaming_receipts",
                "roaming_counts",
                "roaming_days",
                "roaming_summaries",
            ):
                await self.store.db.execute(f"DELETE FROM {table} WHERE day<?", (cutoff,))


class RoamingLogger(PatrolDiary):
    """Compatibility entry point for integrations that supply a Drive client directly."""

    def __init__(self, settings, store, drive=None, *, gateway=None):
        from .diary_sync import DiaryDriveSync

        super().__init__(settings, store, gateway=gateway)
        self.drive = drive
        self._sync = DiaryDriveSync(self, drive, store)

    async def sync_due(self, *, now=None, force=False):
        now = time.time() if now is None else now
        await self.prepare_due(now=now, force=force)
        return await self._sync.sync_due(now=now, force=force)

    async def run(self):
        async with asyncio.TaskGroup() as workers:
            workers.create_task(self.run_local())
            if self.drive is not None:
                workers.create_task(self._sync.run())
