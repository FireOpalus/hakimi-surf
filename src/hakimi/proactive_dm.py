"""Owner-only, durable proactive delivery. Schedules never poll a language model.

The outbox is at-most-once after a OneBot request is submitted: an absent ACK is
ambiguous, not permission to send again. Confirmed ACKs have a separate durable
state so a crash between delivery and memory persistence can repair memory only.
"""

import asyncio
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .bot_server import is_persona_owner, untrusted_input
from .budget_policy import PROACTIVE_LANE, paid_lane_available
from .digest_receipts import DigestReceipts, world_outcome
from .generation_policy import Scenario
from .models import Message
from .modules.actions import ActionCall, Outcome
from .output_links import filter_reply_links
from .reply_style import trim_complete_text
from .roaming_logger import redact_public_summary
from .security import Rejected, configured_secrets, safe_output
from .work_usage import model_work

log = logging.getLogger("hakimi")
TEST_MESSAGE = "BOISER，这是哈基米的主动私信通路测试喵~"


class OwnerProactive:
    def __init__(self, service, *, clock=time.time, rng=None):
        self.service, self.s, self.store = service, service.s, service.store
        self.clock, self.rng = clock, rng or random.SystemRandom()
        self.zone = ZoneInfo(self.s.bot_timezone)
        self.scope = self.store.vault.digest(
            "owner-dm", self.s.namespace, self.s.onebot_self_id, self.s.owner_qq_id
        )
        self.owner, self.bot = self.s.owner_qq_id, self.s.onebot_self_id
        self.digests = DigestReceipts(self.store, self.scope, self.owner)
        self.policy_lock = asyncio.Lock()
        self.delivery_lock = asyncio.Lock()
        self.wakeup = asyncio.Event()

    @property
    def delivery_enabled(self):
        return (
            self.service.modules.get("qq_social")
            .scheduling(self.service.life_engine.config)
            .owner_delivery_enabled
        )

    @property
    def configured(self):
        return bool(
            self.s.onebot_enabled
            and self.owner
            and self.owner == self.s.owner_qq_id
            and self.bot
            and self.bot == self.s.onebot_self_id
        )

    def target(self):
        if not self.configured:
            raise Rejected("configure_owner_qq_id")
        return Message(
            platform="onebot",
            bot_id=self.s.onebot_self_id,
            event_id="owner-proactive",
            user_id=self.s.owner_qq_id,
            conversation_id=self.s.owner_qq_id,
            kind="private",
            text="",
            addressed=True,
            created_at=self.clock(),
        )

    def local(self, now=None):
        return datetime.fromtimestamp(self.clock() if now is None else now, self.zone)

    def day_bounds(self, now):
        local = self.local(now)
        start = datetime.combine(local.date(), datetime.min.time(), self.zone)
        end = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), self.zone)
        return start.timestamp(), end.timestamp()

    def connected(self):
        transport = getattr(self.service.sender, "onebot", None)
        if self.s.onebot_mode == "http":
            return transport is not None
        ready = getattr(transport, "ready", None)
        return bool(ready and ready.is_set())

    async def initialize(self):
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR IGNORE INTO owner_dm_state(scope) VALUES (?)", (self.scope,)
            )
            # An owner/config change invalidates every unsent private body from another scope.
            await self.store.db.execute(
                "UPDATE owner_digest_receipts SET payload=NULL WHERE payload IS NOT NULL AND "
                "id IN (SELECT id FROM proactive_schedule WHERE scope!=?)",
                (self.scope,),
            )
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='cancelled',reason='owner_changed' "
                "WHERE scope!=? AND status='generated'",
                (self.scope,),
            )
            # Existing queued digests have no known generation timestamp; future submits
            # and confirmations can still be timed without reconstructing historical data.
            await self.store.db.execute(
                "INSERT OR IGNORE INTO owner_digest_receipts(id) SELECT id FROM pending_owner_dms "
                "WHERE kind='digest' AND scope=?",
                (self.scope,),
            )
            await self.digests.enqueue_generated_tx(now)
            # A server restart is also an offline interval; preserve an earlier disconnect.
            await self.store.db.execute(
                "UPDATE owner_dm_state SET online=0,disconnected_at=CASE "
                "WHEN ever_online=1 AND disconnected_at=0 THEN ? ELSE disconnected_at END "
                "WHERE scope=?",
                (now, self.scope),
            )
            await self.store.db.execute(
                "UPDATE pending_owner_dms SET status=CASE status WHEN 'delivered' THEN 'sent' "
                "WHEN 'sending' THEN 'uncertain' ELSE 'cancelled' END,payload=NULL,receipt=NULL,"
                "reason='owner_changed' WHERE scope!=? AND status IN "
                "('pending','sending','delivered')",
                (self.scope,),
            )
            await self.store.db.execute(
                "UPDATE pending_owner_dms SET status='uncertain',payload=NULL,"
                "reason='restart_after_submit' WHERE status='sending'"
            )
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='failed',reason='generation_interrupted' "
                "WHERE status='generating' AND id NOT IN (SELECT id FROM pending_owner_dms)"
            )
            # Unattempted old timers yield to the world schedule. Paid/ambiguous daily
            # attempts and all delivery receipts retain their original daily idempotency key.
            await self.store.db.execute("DELETE FROM proactive_schedule WHERE status='planned'")
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='queued' WHERE status='generating' "
                "AND id IN (SELECT id FROM pending_owner_dms)"
            )
            await self.store.db.execute(
                "UPDATE pending_owner_dms SET status='cancelled',payload=NULL, "
                "reason='world_scheduler' "
                "WHERE kind='companion' AND status='pending'"
            )
            if self.configured:
                # Existing authenticated job timestamps cover the first upgrade's recent
                # activity window without opening or disclosing any private message body.
                await self.store.db.execute(
                    "UPDATE owner_dm_state SET last_private_at=MAX(last_private_at,"
                    "COALESCE((SELECT MAX(created) FROM jobs WHERE kind='message' AND scope=?),0)) "
                    "WHERE scope=?",
                    (self.store.session_scope(self.target()), self.scope),
                )
            # Move only old, never-submitted proactive outbox rows into the durable queue.
            for row in await self.store.all("SELECT * FROM outbox WHERE status='pending'"):
                legacy = self.store.vault.open(row["id"], row["payload"])
                if not legacy.get("proactive"):
                    continue
                target = Message.model_validate(legacy["message"])
                allowed = (
                    self.delivery_enabled
                    and target.kind == "private"
                    and is_persona_owner(target, self.s)
                    and row["expires"] > now
                )
                if allowed:
                    await self._insert(
                        row["id"],
                        "patrol",
                        self.service.output(legacy["text"]),
                        row["expires"],
                        now,
                    )
                await self.store.db.execute(
                    "UPDATE outbox SET status=?,payload=NULL WHERE id=?",
                    ("migrated" if allowed else "cancelled", row["id"]),
                )
            await self.digests.recover_links_tx(now)
        # Complete confirmed deliveries before workers can read Owner memory.
        async with self.service.owner_dialogue_lock:
            await self._repair_confirmed()

    async def note_activity(self, message):
        if message.kind != "private" or not is_persona_owner(message, self.s):
            return
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO owner_dm_state(scope,last_private_at) VALUES (?,?) "
                "ON CONFLICT(scope) DO UPDATE SET last_private_at=excluded.last_private_at",
                (self.scope, self.clock()),
            )

    async def on_connection(self, online):
        now, lost = self.clock(), 0
        async with self.store.transaction():
            row = await self.store.one("SELECT * FROM owner_dm_state WHERE scope=?", (self.scope,))
            if row and online and row["ever_online"] and row["disconnected_at"]:
                lost = max(0, now - row["disconnected_at"])
            await self.store.db.execute(
                "INSERT INTO owner_dm_state(scope,ever_online,online,disconnected_at) "
                "VALUES (?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                "ever_online=MAX(ever_online,excluded.ever_online),online=excluded.online,"
                "disconnected_at=CASE WHEN excluded.online=1 THEN 0 "
                "WHEN disconnected_at=0 THEN excluded.disconnected_at ELSE disconnected_at END",
                (self.scope, int(online), int(online), 0 if online else now),
            )
        if online:
            if lost >= self.s.proactive_ws_alert_after_seconds:
                await self.alert(
                    "ws_recovered",
                    f"BOISER，NapCat 连接已恢复喵。刚才中断约 {int(lost // 60)} 分钟，"
                    "仅补发尚未过期的待发消息。",
                )
            self.wakeup.set()

    def _delivery_reason(self, kind, now, *, automatic=False):
        if not self.configured or not self.delivery_enabled:
            return "disabled"
        if kind == "digest":
            engine = self.service.life_engine
            local = self.local(now)
            if (
                not engine.config.enabled
                or engine.config.action_modes.get("qq.owner_digest") == "off"
                or not engine.config.is_active_time(local)
                or engine.config.quiet(local)
                or self.store.runtime.is_resting(now)
                or (automatic and not engine.digest_due(now))
            ):
                return "outside_digest_window"
        return None

    async def _generation_reason(self, now, *, automatic):
        if reason := self._delivery_reason("digest", now, automatic=automatic):
            return reason
        budget = await self.store.budget_status()
        if budget["hard_stop"]:
            return "budget_hard_stop"
        if not paid_lane_available(budget, PROACTIVE_LANE):
            return "budget_exhausted"
        return None

    async def _insert(self, key, kind, text, expires, now):
        # Caller holds store transaction. No queue row can choose a destination.
        payload = {"owner": self.s.owner_qq_id, "bot": self.s.onebot_self_id, "text": text}
        await self.store.db.execute(
            "INSERT OR IGNORE INTO pending_owner_dms"
            "(id,scope,day,kind,payload,created,expires_at,status) VALUES "
            "(?,?,?,?,?,?,?,'pending')",
            (
                key,
                self.scope,
                self.local(now).date().isoformat(),
                kind,
                self.store.vault.seal(key, payload),
                now,
                expires,
            ),
        )

    async def enqueue(self, kind, text, *, key=None, expires=None):
        now = self.clock()
        if kind not in {"test", "patrol"}:
            raise Rejected("invalid_owner_dm_kind")
        maximum = now + 43200
        expires = min(expires, maximum) if expires is not None else maximum
        key = key or uuid.uuid4().hex
        async with self.policy_lock:
            reason = self._delivery_reason(kind, now)
            if expires is not None and expires <= now:
                reason = "expired"
            if reason:
                raise Rejected(reason)
            async with self.store.transaction():
                await self._insert(
                    key,
                    kind,
                    self.service.output(text),
                    expires or now + 43200,
                    now,
                )
        self.wakeup.set()
        return key

    async def alert(self, code, text, *, daily=False):
        now = self.clock()
        key = self.scope + ":" + code
        if daily:
            key += ":" + self.local(now).date().isoformat()
        async with self.policy_lock:
            if not self.configured or not self.delivery_enabled:
                return None
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT cooldown_until FROM alert_cooldown WHERE alert_key=?", (key,)
                )
                if row and row[0] > now:
                    return None
                dm_id = uuid.uuid4().hex
                await self._insert(dm_id, "alert", text, now + 43200, now)
                cooldown = (
                    self.day_bounds(now)[1]
                    if daily
                    else now + self.s.proactive_alert_cooldown_seconds
                )
                await self.store.db.execute(
                    "INSERT INTO alert_cooldown VALUES (?,?,?,?,NULL) ON CONFLICT(alert_key) "
                    "DO UPDATE SET "
                    "queued_at=excluded.queued_at,cooldown_until=excluded.cooldown_until,"
                    "dm_id=excluded.dm_id,sent_at=NULL",
                    (key, now, cooldown, dm_id),
                )
        self.wakeup.set()
        return dm_id

    async def check_alerts(self):
        if not self.configured or not self.delivery_enabled:
            return
        budget = await self.store.budget_status()
        for kind, label in (
            ("daily", f"{self.s.bot_timezone} 日预算"),
            ("monthly", f"{self.s.bot_timezone} 自然月预算"),
            ("total", "本地累计预算"),
        ):
            amount, limit = budget[kind], budget[kind + "_limit"]
            if limit <= 0:
                continue
            for level in (100, 85):
                if amount * 100 >= limit * level:
                    await self.alert(
                        f"budget_{kind}_{level}",
                        f"BOISER，{label}已达 {level}% {'硬熔断线' if level == 100 else '预警线'}："
                        f"账本约 ${amount / 1000000:.4f} / ${limit / 1000000:.2f}（含费用预留）。"
                        + ("付费生成已暂停。" if level == 100 else "请留意余量喵。"),
                        daily=True,
                    )
                    break  # Crossing both thresholds at once needs only the stronger alert.
        if budget["circuit"]:
            await self.alert(
                "budget_circuit",
                "BOISER，账本发现实际用量超出保守预留，付费调用已熔断；请在后台检查费用。",
                daily=True,
            )
        async with self.store.lock:
            health = dict(await self.store.all("SELECT provider,failures FROM operational_health"))
        if health.get("bilibili_412", 0) >= 3:
            await self.alert(
                "bilibili_412",
                "BOISER，B 站巡逻已连续至少 3 次遇到 412 风控。"
                "程序已退避，请稍后检查网络与巡逻状态喵。",
            )
        if max(health.get("gemini_chat", 0), health.get("gemini_decision", 0)) >= 5:
            await self.alert(
                "gemini_errors",
                "BOISER，Gemini 同一接口已连续至少 5 次调用失败。请检查 API 可用性、配置与账单。",
            )

    async def _public_digest(self, now):
        day = self.local(now).date().isoformat()
        start, end = self.day_bounds(now)
        payload = {
            "day": day,
            "timezone": self.s.bot_timezone,
            "public_summaries": [],
            "group_activity": {},
        }
        # Read the same public diary data; never call render/generation or any Session here.
        diary = self.service.diary
        if diary and self.s.roaming_timezone == self.s.bot_timezone:
            async with self.store.lock:
                counts = dict(
                    await self.store.all("SELECT kind,n FROM roaming_counts WHERE day=?", (day,))
                )
                rows = await self.store.all(
                    "SELECT id,payload,event_kind FROM roaming_events WHERE day=? "
                    "AND event_kind IN ('patrol','world_life') ORDER BY rowid",
                    (day,),
                )
            counts.pop("private_chat", None)
            payload.update(diary._summary_input(day, counts, rows))
            payload["group_activity"].pop("私聊日常交流", None)
        if not payload["public_summaries"]:
            subject = self.store.subject(self.target())
            async with self.store.lock:
                rows = await self.store.all(
                    "SELECT payload FROM reports WHERE public=1 AND subject=? "
                    "AND created>=? AND created<? "
                    "ORDER BY id DESC LIMIT 10",
                    (subject, start, end),
                )
            payload["public_summaries"] = [
                redact_public_summary(self.store.vault.open(subject, row[0]))[:500]
                for row in reversed(rows)
            ]
        async with self.store.lock:
            amount = (
                await self.store.one(
                    "SELECT COALESCE(SUM(l.amount),0) FROM ledger l "
                    "LEFT JOIN ledger_times t ON t.id=l.id "
                    "WHERE (t.created>=? AND t.created<?) OR (t.id IS NULL AND l.day=?)",
                    (start, end, self.local(now).date().isoformat()),
                )
            )[0]
        payload["approximate_api_usd_including_reservations"] = f"{amount / 1000000:.4f}"
        while (
            len(untrusted_input(payload).encode()) > self.s.max_context_bytes
            and payload["public_summaries"]
        ):
            payload["public_summaries"].pop(0)
            payload["omitted_public_summaries"] = payload.get("omitted_public_summaries", 0) + 1
        return payload

    def _desktop_recent_activity(self, now: float) -> dict | None:
        cloud_mem = getattr(self.service, "cloud_memory", None)
        if cloud_mem is None or not getattr(self.s, "enable_antigravity_memory", False):
            return None
        info_fn = getattr(cloud_mem, "latest_activity_info", None)
        if not callable(info_fn):
            return None
        try:
            info = info_fn()
        except Exception:
            return None
        if not isinstance(info, dict) or not info.get("enabled"):
            return None
        latest_ts = info.get("latest_timestamp")
        if (
            isinstance(latest_ts, (int, float))
            and not isinstance(latest_ts, bool)
            and latest_ts > 0
            and 0 < (now - latest_ts) < 5400
        ):
            return info
        return None

    async def companion_context(self, now: float) -> dict:
        local_dt = self.local(now)
        weekday = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")[local_dt.weekday()]
        hour = local_dt.hour
        day_phase = (
            "早晨"
            if hour < 10
            else "上午"
            if hour < 12
            else "午休午后"
            if hour < 14
            else "下午摸鱼时段"
            if hour < 18
            else "傍晚"
            if hour < 20
            else "夜晚"
        )
        secrets = getattr(self.service, "secrets", None) or configured_secrets(self.s)
        target_msg = self.target()
        memory_hints: list[str] = []
        recent_topics: list[str] = []
        sessions = getattr(self.service, "sessions", None)
        if sessions is not None:
            try:
                view = await sessions.dual.view(target_msg)
                for entry in view.get("entries", []):
                    if entry.get("deleted") or (entry.get("expires") and entry["expires"] <= now):
                        continue
                    is_hint = (
                        entry.get("kind") == "profile"
                        and entry.get("slot") in {"state", "preferences"}
                    ) or entry.get("kind") == "chronicle"
                    if not is_hint:
                        continue
                    topic = f"{entry.get('topic')}: " if entry.get("topic") else ""
                    text = safe_output(f"{topic}{entry.get('text', '')}", secrets, limit=120)
                    if text and text not in memory_hints:
                        memory_hints.append(text)
                    if len(memory_hints) >= 4:
                        break
            except Exception:
                pass
            try:
                snap = await sessions.snapshot(target_msg)
                for item in reversed(snap.get("recent_messages", [])[-8:]):
                    if item.get("role") != "user":
                        continue
                    created = item.get("created_at")
                    if (
                        isinstance(created, (int, float))
                        and not isinstance(created, bool)
                        and created > 0
                        and created < now - 172800
                    ):
                        continue
                    raw_text = str(item.get("text", "")).strip()
                    if not raw_text or raw_text.startswith(("[协议事件", "/")):
                        continue
                    cleaned = safe_output(raw_text, secrets, limit=100)
                    if cleaned and cleaned not in recent_topics:
                        recent_topics.append(cleaned)
                    if len(recent_topics) >= 2:
                        break
            except Exception:
                pass
        bili_finds: list[dict] = []
        try:
            watcher = getattr(self.service, "bili_watcher", None)
            if watcher is not None and hasattr(watcher, "recent_surfed"):
                for item in await watcher.recent_surfed(limit=2, now=now, message=target_msg):
                    bvid = item.get("bvid", "")
                    gist = str(item.get("notes", ""))[:150]
                    if bvid and gist:
                        bili_finds.append({"bvid": bvid, "gist": gist})
            else:
                async with self.store.lock:
                    has_bili = await self.store.one(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='table' AND name='bili_watched_history'"
                    )
                    rows = (
                        await self.store.all(
                            "SELECT bvid,summary FROM bili_watched_history "
                            "WHERE status='done' AND summary IS NOT NULL AND created>=? "
                            "ORDER BY created DESC LIMIT 2",
                            (now - 172800,),
                        )
                        if has_bili
                        else []
                    )
                for bvid, sealed in rows:
                    gist = redact_public_summary(
                        safe_output(
                            self.store.vault.open("bili-learn:" + bvid, sealed), secrets, limit=150
                        )
                    )
                    if gist:
                        bili_finds.append({"bvid": bvid, "gist": gist})
        except Exception:
            pass
        desktop_activity = await asyncio.to_thread(self._desktop_recent_activity, now)
        topics = ["结合星期与时段自然打个招呼", "一句不催促的摸鱼问候"]
        if memory_hints or recent_topics:
            topics.append("顺口挂念一下近期聊到的话题或状态")
        if bili_finds:
            topics.append("顺手分享刚在 B 站刷到的有趣内容")
        care_topic = "知冷知热地递杯水轻声问候刚刚还在电脑前忙正事的 BOISER"
        payload: dict = {
            "task": "one_short_companion_greeting",
            "local_time": local_dt.strftime("%H:%M"),
            "weekday": weekday,
            "day_phase": day_phase,
            "topic": care_topic if desktop_activity else self.rng.choice(topics),
        }
        if desktop_activity:
            payload["desktop_recent_activity"] = (
                "BOISER 近期刚在电脑端与本体交流忙正事，"
                "请用知冷知热、不打扰的口吻顺手递杯水或轻声问候，切勿问怎么半天没出现"
            )
        if memory_hints:
            payload["owner_memory_hints"] = memory_hints
        if recent_topics:
            payload["recent_private_topics"] = recent_topics
        if bili_finds:
            payload["recent_bili_finds"] = bili_finds
        for key in (
            "recent_private_topics",
            "owner_memory_hints",
            "recent_bili_finds",
            "desktop_recent_activity",
        ):
            if len(untrusted_input(payload).encode()) <= self.s.max_context_bytes:
                break
            payload.pop(key, None)
        return payload

    async def prepare_digest(self, *, automatic=False, expires=None, intent_id=None):
        now = self.clock()
        day = self.local(now).date().isoformat()
        key = self.store.vault.digest(self.scope, day, "digest")
        closing = self.service.life_engine.digest_window(self.local(now).date())
        day_end = self.day_bounds(now)[1]
        expires = min(
            day_end,
            closing[1] if closing else day_end,
            expires if expires is not None else day_end,
            now + 43200,
        )
        async with self.policy_lock:
            # Joining the same paid result does not depend on another ledger read.
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT status,reason FROM proactive_schedule WHERE id=?", (key,)
                )
                if row:
                    await self.digests.link_tx(key, intent_id)
                    await self.digests.sync_intents_tx(key, now)
                    return {"status": row["status"], "reason": row["reason"], "id": key}
            reason = await self._generation_reason(now, automatic=automatic)
            if reason or expires <= now:
                return {"status": "skipped", "reason": reason or "expired", "id": key}
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO proactive_schedule VALUES (?,?,?,'digest',?,?,'generating',NULL)",
                    (key, self.scope, day, now, expires),
                )
                await self.digests.link_tx(key, intent_id)
        daily_longnick = None
        try:
            payload = await self._public_digest(now)
            owner_message = self.target()
            subject = self.store.subject(owner_message)
            requested_longnick = self.service.longnick.config.enabled
            with model_work("qq.owner_digest", key):
                await self.service.ai.screen(
                    payload, subject, purpose="source", budget_lane=PROACTIVE_LANE
                )
                after_screen = self.clock()
                reason = await self._generation_reason(after_screen, automatic=automatic)
                if reason or after_screen >= expires:
                    raise Rejected(reason or "expired")
                generated = await self.service.ai.generate(
                    payload,
                    subject,
                    scenario=Scenario.OWNER_DIGEST,
                    message=owner_message,
                    budget_lane=PROACTIVE_LANE,
                    **({"with_longnick": True} if requested_longnick else {}),
                )
            if requested_longnick:
                try:
                    value = json.loads(generated)
                    if isinstance(value, dict) and isinstance(value.get("digest"), str):
                        generated = value["digest"]
                        daily_longnick = value.get("daily_longnick")
                except (ValueError, TypeError):
                    pass  # A malformed optional signature must not suppress the digest.
            generated = filter_reply_links(self.service.output(generated), public=False)
            body = trim_complete_text(generated, 450)
            cost = payload["approximate_api_usd_including_reservations"]
            text = self.service.output(
                f"{day} 巡逻小报\n{body}\n今日 API 约 ${cost}（本地估算，含预留，不含本次汇报）。"
            )
        except asyncio.CancelledError:
            raise  # A paid call without a durable result must never be repeated automatically.
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, Rejected)
                and str(exc)
                in {
                    "budget_hard_stop",
                    "budget_exhausted",
                    "budget_circuit_open",
                    "disabled",
                    "outside_digest_window",
                    "expired",
                    "prompt_injection",
                    "content_blocked",
                    "gemini_unavailable",
                }
                else "generation_failed"
            )
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE proactive_schedule SET status='failed',reason=? WHERE id=?",
                    (reason, key),
                )
                await self.digests.sync_intents_tx(key, self.clock())
            log.info("event=owner_dm_failed")
            return {"status": "failed", "reason": reason, "id": key}
        # The daily result is durable before the queue handoff. Subsequent work is local
        # and can resume after a crash even if this model call just exhausted the budget.
        encrypted = self.store.vault.seal(
            key, {"owner": self.owner, "bot": self.bot, "text": text, "world_digest": automatic}
        )
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO owner_digest_receipts(id,generated_at,payload) VALUES (?,?,?)",
                (key, self.clock(), encrypted),
            )
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='generated' WHERE id=?", (key,)
            )
        async with self.store.transaction():
            await self.digests.enqueue_generated_tx(self.clock(), key)
        self.wakeup.set()
        if daily_longnick is not None:
            try:
                await self.service.longnick.apply_digest(daily_longnick)
            except Exception:
                log.warning("event=owner_digest_longnick_failed")
        return {"status": "queued", "id": key}

    async def world_digest(self, call: ActionCall) -> Outcome:
        if call.ticket["target_scope"] != "owner:" + self.s.owner_qq_id or not call.still_allowed():
            return Outcome("gated_skip", "unauthorized_scope")
        result = await self.prepare_digest(
            automatic=True, expires=call.ticket["expire_at"], intent_id=call.ticket["id"]
        )
        async with self.store.lock:
            receipt = await self.digests.get_tx(result["id"])
        if receipt:
            return world_outcome(receipt["stage"])
        return Outcome("gated_skip", result.get("reason") or result["status"])

    async def manual_digest(self):
        result = await self.prepare_digest()
        if result["status"] in {"queued", "generated"}:
            await self.deliver_once()
        async with self.store.lock:
            receipt = await self.digests.get_tx(result["id"], connected=self.connected())
        return {**result, "delivery": await self.delivery_status(result["id"]), "receipt": receipt}

    async def test_dm(self):
        # Fixed template, not even Decisions or a post-send compression call.
        key = await self.enqueue("test", TEST_MESSAGE, expires=self.clock() + 7200)
        await self.deliver_once()
        return {"id": key, "status": await self.delivery_status(key)}

    async def delivery_status(self, key):
        async with self.store.lock:
            row = await self.store.one(
                "SELECT status FROM pending_owner_dms WHERE id=? AND scope=?", (key, self.scope)
            )
        return row[0] if row else "not_queued"

    async def _expire_and_disable(self):
        now = self.clock()
        async with self.store.transaction():
            await self.digests.enqueue_generated_tx(now)
            changed = await self.store.all(
                "UPDATE pending_owner_dms SET status='expired',payload=NULL,reason='expired' "
                "WHERE status='pending' AND scope=? AND expires_at<=? RETURNING id,kind",
                (self.scope, now),
            )
            if not self.configured or not self.delivery_enabled:
                changed += await self.store.all(
                    "UPDATE pending_owner_dms SET status='cancelled',payload=NULL, "
                    "reason='disabled' WHERE scope=? AND status='pending' RETURNING id,kind",
                    (self.scope,),
                )
            for row in changed:
                if row["kind"] == "digest":
                    await self.digests.sync_intents_tx(row["id"], now)

    async def _finish_memory(self, row):
        payload = self.store.vault.open(row["id"], row["payload"])
        if payload["owner"] != self.s.owner_qq_id or payload["bot"] != self.s.onebot_self_id:
            raise Rejected("owner_dm_target_forbidden")
        remembered = await self.service.sessions.remember_assistant(
            self.target(), payload["text"], row["id"], expected_epoch=row["session_epoch"]
        )
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE pending_owner_dms SET status='sent',payload=NULL,reason=? WHERE id=?",
                (None if remembered else "memory_reset_or_disabled", row["id"]),
            )
            await self.store.db.execute(
                "UPDATE proactive_schedule SET status='sent' WHERE id=?", (row["id"],)
            )
            await self.store.db.execute(
                "UPDATE alert_cooldown SET sent_at=? WHERE dm_id=?", (row["sent_at"], row["id"])
            )

    async def _repair_confirmed(self):
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM pending_owner_dms WHERE scope=? AND status='delivered' ORDER "
                "BY created",
                (self.scope,),
            )
        for row in rows:
            await self._finish_memory(row)

    async def deliver_once(self):
        async with self.delivery_lock, self.service.owner_dialogue_lock:
            await self._repair_confirmed()
            async with self.policy_lock:
                await self._expire_and_disable()
                if not self.connected():
                    return False
                async with self.store.lock:
                    row = await self.store.one(
                        "SELECT * FROM pending_owner_dms WHERE scope=? AND status='pending' "
                        "ORDER BY created,rowid LIMIT 1",
                        (self.scope,),
                    )
                if not row:
                    return False
                key = row["id"]
                payload = self.store.vault.open(key, row["payload"])
                if (
                    payload["owner"] != self.s.owner_qq_id
                    or payload["bot"] != self.s.onebot_self_id
                ):
                    raise Rejected("owner_dm_target_forbidden")
                submitted = False

                async def before_send():
                    nonlocal submitted
                    if not self.configured or not self.connected():
                        return False
                    epoch = await self.service.sessions.current_epoch(self.target())
                    async with self.store.transaction():
                        now = self.clock()
                        reason = self._delivery_reason(
                            row["kind"], now, automatic=payload.get("world_digest", False)
                        )
                        if row["expires_at"] <= now:
                            reason = "expired"
                        if reason:
                            await self.store.db.execute(
                                "UPDATE pending_owner_dms SET "
                                "status='skipped',payload=NULL,reason=? WHERE id=?",
                                (reason, key),
                            )
                            if row["kind"] == "digest":
                                await self.digests.sync_intents_tx(key, now)
                            return False
                        if not self.connected():
                            return False
                        cur = await self.store.db.execute(
                            "UPDATE pending_owner_dms SET status='sending',session_epoch=? "
                            "WHERE id=? AND status='pending'",
                            (epoch, key),
                        )
                        if cur.rowcount != 1:
                            return False
                        if row["kind"] == "digest":
                            await self.store.db.execute(
                                "UPDATE owner_digest_receipts SET submitted_at=? WHERE id=?",
                                (now, key),
                            )
                    submitted = True
                    return True

                try:
                    receipt = await self.service.sender.send_owner_dm(
                        int(self.s.owner_qq_id), payload["text"], before_send=before_send
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if isinstance(exc, Rejected) and str(exc) == "onebot_not_dispatched":
                        # The transport proves nothing was submitted; retain the durable message.
                        async with self.store.transaction():
                            await self.store.db.execute(
                                "UPDATE pending_owner_dms SET status='pending',session_epoch=NULL "
                                "WHERE id=? AND status='sending' AND payload IS NOT NULL",
                                (key,),
                            )
                            if row["kind"] == "digest":
                                await self.store.db.execute(
                                    "UPDATE owner_digest_receipts SET submitted_at=NULL WHERE id=?",
                                    (key,),
                                )
                        return False
                    if submitted:
                        async with self.store.transaction():
                            await self.store.db.execute(
                                "UPDATE pending_owner_dms SET "
                                "status='uncertain',payload=NULL,reason='send_unconfirmed' "
                                "WHERE id=?",
                                (key,),
                            )
                            if row["kind"] == "digest":
                                await self.digests.sync_intents_tx(key, self.clock())
                        log.info("event=owner_dm_uncertain")
                    return submitted
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE pending_owner_dms SET status='delivered',sent_at=?,receipt=? "
                        "WHERE id=?",
                        (self.clock(), self.store.vault.seal("receipt:" + key, receipt), key),
                    )
                    if row["kind"] == "digest":
                        await self.digests.sync_intents_tx(key, self.clock())
                    confirmed = await self.store.one(
                        "SELECT * FROM pending_owner_dms WHERE id=?", (key,)
                    )
                await self._finish_memory(confirmed)
                ws = getattr(self.service, "world_state", None)
                if ws is not None and hasattr(ws, "update_state"):
                    async with self.store.lock:
                        has_ws = await self.store.one(
                            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='world_state'"
                        )
                    if has_ws:
                        await ws.update_state(last_proactive_at=self.clock())
                # No compression here: alert/test delivery must remain strictly zero-token.
                return True

    async def tick(self):
        await self.check_alerts()
        await self._expire_and_disable()

    async def run(self):
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.info("event=owner_dm_worker_error")
            await asyncio.sleep(10)

    async def delivery(self):
        while True:
            try:
                self.wakeup.clear()
                sent = await self.deliver_once()
                if sent:
                    await asyncio.sleep(2)
                else:
                    try:
                        await asyncio.wait_for(self.wakeup.wait(), 5)
                    except TimeoutError:
                        pass
            except asyncio.CancelledError:
                raise
            except Exception:
                log.info("event=owner_dm_worker_error")
                await asyncio.sleep(5)

    async def status(self):
        now = self.clock()
        day = self.local(now).date().isoformat()
        async with self.store.lock:
            pending = await self.store.all(
                "SELECT kind,status,COUNT(*) AS n FROM pending_owner_dms WHERE scope=? AND "
                "status IN ('pending','uncertain','delivered') GROUP BY kind,status",
                (self.scope,),
            )
            alerts = await self.store.all(
                "SELECT a.alert_key,a.queued_at,a.cooldown_until,a.sent_at,p.status FROM "
                "alert_cooldown a LEFT JOIN pending_owner_dms p ON p.id=a.dm_id WHERE "
                "a.alert_key LIKE ? ORDER BY a.queued_at DESC LIMIT 20",
                (self.scope + ":%",),
            )
        return {
            "scheduler": "life_engine",
            "delivery_enabled": self.delivery_enabled,
            "timezone": self.s.bot_timezone,
            "configured": self.configured,
            "connected": self.connected(),
            "day": day,
            "next_digest_at": await self.service.life_engine.next_digest_at(now),
            "digest_history": await self.digest_history(),
            "queue": [dict(row) for row in pending],
            "alerts": [
                {**dict(row), "alert_key": row["alert_key"][len(self.scope) + 1 :]}
                for row in alerts
            ],
            "budget": await self.store.budget_status(),
            "proactive_daily_limit_usd": str(self.s.proactive_daily_usd),
        }

    async def digest_history(self):
        return await self.digests.history(connected=self.connected())

    async def has_digest_attempt(self, now):
        return await self.digest_attempted(self.local(now).date().isoformat())

    async def digest_attempted(self, day):
        async with self.store.lock:
            return bool(
                await self.store.one(
                    "SELECT 1 FROM proactive_schedule WHERE scope=? AND day=? AND "
                    "kind='digest' AND status!='planned'",
                    (self.scope, day),
                )
            )

    async def prune(self):
        now = self.clock()
        day = self.local(now - 9 * 86400).date().isoformat()
        async with self.store.transaction():
            await self.store.db.execute(
                "DELETE FROM pending_owner_dms WHERE status!='delivered' AND "
                "((kind!='digest' AND created<?) OR (kind='digest' AND day<?))",
                (now - 8 * 86400, day),
            )
            await self.store.db.execute(
                "DELETE FROM alert_cooldown WHERE cooldown_until<?", (now - 8 * 86400,)
            )
            for statement in (
                "DELETE FROM owner_digest_intents WHERE digest_id IN ",
                "DELETE FROM owner_digest_receipts WHERE id IN ",
            ):
                await self.store.db.execute(
                    statement + "(SELECT id FROM proactive_schedule WHERE day<?)", (day,)
                )
            await self.store.db.execute("DELETE FROM proactive_schedule WHERE day<?", (day,))
            await self.store.db.execute("DELETE FROM proactive_days WHERE day<?", (day,))
