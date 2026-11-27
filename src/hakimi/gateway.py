"""Hot inbound filters and bounded encrypted diagnostic/relevance data."""

import asyncio
import re
import time
import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .admission_policy import effective_bot_reply_threshold
from .affection import level_for_score
from .provider_diagnostics import LANES, PROVIDERS, append_attempts, safe_attempts
from .security import Rejected, clean_text, configured_secrets
from .social import chat_module
from .social_chat_config import cached_remote_chat_config, load_remote_chat_config
from .streaming import upstream_error_code, upstream_http_status

PRESETS = {"quiet": 0.65, "natural": 0.42, "active": 0.28}
OWNER_REPLY_CEILING = 0.28
SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_audit (
 id TEXT PRIMARY KEY, at REAL NOT NULL, payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS gateway_group_context (
 scope TEXT NOT NULL, event TEXT NOT NULL, at REAL NOT NULL, event_at REAL NOT NULL,
 payload BLOB NOT NULL,
 PRIMARY KEY(scope,event)
);
CREATE INDEX IF NOT EXISTS gateway_group_context_scope_at ON gateway_group_context(scope, at);
CREATE INDEX IF NOT EXISTS gateway_group_context_at ON gateway_group_context(at);
"""


class FilterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    injection_threshold: float = Field(default=0.78, ge=0.01, le=0.99)
    unsafe_threshold: float = Field(default=0.80, ge=0.01, le=0.99)
    reply_threshold: float = Field(default=0.42, ge=0.01, le=0.99)
    owner_injection_threshold: float = Field(default=0.90, ge=0.01, le=0.99)
    owner_unsafe_threshold: float = Field(default=0.90, ge=0.01, le=0.99)
    group_activity: dict[str, Literal["quiet", "natural", "active"]] = Field(default_factory=dict)

    @field_validator("group_activity")
    @classmethod
    def group_keys(cls, values):
        if len(values) > 1000 or any(
            not re.fullmatch(
                r"(?:onebot:[1-9][0-9]{0,19}|qq:[A-Za-z0-9_-]{1,128}|telegram:-?[0-9]{1,20})",
                key,
            )
            for key in values
        ):
            raise ValueError("Invalid group activity keys")
        return values

    @classmethod
    def from_settings(cls, settings):
        return cls(
            **{
                name: getattr(settings, "message_" + name, field.default)
                for name, field in cls.model_fields.items()
                if name != "group_activity"
            }
        )


def redact_excerpt(text, settings, *, limit=180):
    text = clean_text(str(text))[:8000]
    for secret in configured_secrets(settings):
        if secret and len(secret) >= 6:
            text = text.replace(secret, "[凭证已隐去]")
    text = re.sub(
        r"(?i)(?:bearer\s+[^\s]+|(?:api[_-]?key|token|password|passwd|secret|cookie|sessdata|"
        r"bili_jct|authorization|密码|密钥|口令)[\s\"']*[:=：][\s\"']*[^\s,;，；]+)",
        "[凭证已隐去]",
        text,
    )
    text = re.sub(r"https?://\S+", "[链接]", text)
    text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[邮箱]", text)
    text = re.sub(r"[A-Za-z0-9_./+=-]{20,}", "[长标识已隐去]", text)
    text = re.sub(r"\d{5,}", "[数字已隐去]", text)
    return text.replace("[CQ:", "［CQ:")[:limit]


def _safe_detail(detail: str) -> str:
    text = str(detail or "").strip()
    return text if re.fullmatch(r"[a-z0-9_]{1,64}", text) else ""


_WEB_SEARCH_STATUSES = {"grounded", "unavailable", "disabled", "skipped_query"}
_WEB_SEARCH_TRIGGERS = {"explicit", "gate", "model"}
_WEB_SEARCH_PROVIDERS = {"google", "deepseek", "remote", "searxng", "tavily", "brave", "duckduckgo"}
_WEB_SEARCH_FALLBACKS = {"deepseek", "google"}


def _safe_web_search_audit(value: dict | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    status = value.get("status")
    if status not in _WEB_SEARCH_STATUSES:
        return None
    clean: dict[str, Any] = {"status": status}
    trigger = value.get("trigger")
    if trigger in _WEB_SEARCH_TRIGGERS:
        clean["trigger"] = trigger
    provider = value.get("provider")
    if provider in _WEB_SEARCH_PROVIDERS:
        clean["provider"] = provider
    fallback = value.get("fallback")
    if fallback in _WEB_SEARCH_FALLBACKS:
        clean["fallback"] = fallback
    if value.get("retried") is True:
        clean["retried"] = True
    if reason := _safe_detail(value.get("reason", "")):
        clean["reason"] = reason
    return clean


class FilterManager:
    def __init__(self, store):
        self.store, self.s = store, store.s
        self.env_config = FilterConfig.from_settings(self.s)
        self.config = self.env_config
        self.overridden = False
        self.initialized = False
        self.init_lock, self.lock = asyncio.Lock(), asyncio.Lock()
        self.hot_ring = None
        self.proactive_cooldown_seconds = 1500
        self.last_proactive_spoke: dict[str, float] = {}

    async def initialize(self):
        async with self.init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                for stmt in SCHEMA.split(";"):
                    sql = stmt.strip()
                    if sql:
                        await self.store.db.execute(sql)
                row = await self.store.one("SELECT payload FROM admin_config WHERE key='filters'")
                if row:
                    self.config = FilterConfig.model_validate(
                        self.store.vault.open("admin:filters", row[0])
                    )
                    self.overridden = True
            self.initialized = True

    async def set_config(self, value):
        await self.initialize()
        async with self.lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='filters'")
                self.config, self.overridden = self.env_config, False
            else:
                config = FilterConfig.model_validate(value)
                await self.store.runtime.save_config("filters", config.model_dump())
                self.config, self.overridden = config, True
        return await self.status()

    async def set_group_activity(self, platform, group_id, preset):
        await self.initialize()
        if platform == "onebot":
            groups = set(self.s.onebot_allowed_groups)
        elif platform == "qq":
            groups = set(self.s.qq_allowed_groups)
        elif platform == "telegram":
            remote_cfg = cached_remote_chat_config(self.s, platform)
            if remote_cfg is None:
                try:
                    remote_cfg = await load_remote_chat_config(
                        self.store, platform, settings=self.s
                    )
                except Exception:
                    remote_cfg = None
            if remote_cfg is not None:
                groups = set(remote_cfg.allowed_groups)
            else:
                mod = chat_module(self.s, platform)
                groups = (
                    set(mod.allowed_groups())
                    if mod is not None and hasattr(mod, "allowed_groups")
                    else set()
                )
        else:
            groups = set()
        if platform not in {"onebot", "qq", "telegram"} or group_id not in groups:
            raise Rejected("group_not_allowlisted")
        if preset is not None and preset not in PRESETS:
            raise Rejected("invalid_activity_preset")
        async with self.lock:
            values = self.config.model_dump()
            key = f"{platform}:{group_id}"
            if preset is None:
                values["group_activity"].pop(key, None)
            else:
                values["group_activity"][key] = preset
            config = FilterConfig.model_validate(values)
            await self.store.runtime.save_config("filters", config.model_dump())
            self.config, self.overridden = config, True
        return await self.status()

    @staticmethod
    def _speaker_affection_level(aff, message) -> int | None:
        if aff is None or message is None:
            return None
        cfg = getattr(aff, "config", None)
        if cfg is not None and not getattr(cfg, "enabled", True):
            return None
        is_bot_fn = getattr(aff, "_is_known_bot", None)
        if callable(is_bot_fn) and is_bot_fn(message.platform, message.user_id):
            return None
        is_owner_fn = getattr(aff, "is_owner_id", None)
        user_key_fn = getattr(aff, "_user_key", None)
        user_scores = getattr(aff, "_user_scores", None)
        if callable(is_owner_fn) and callable(user_key_fn) and isinstance(user_scores, dict):
            if is_owner_fn(message.platform, message.user_id):
                return 6
            cached = user_scores.get(user_key_fn(message.platform, message.user_id))
            score = (
                cached[0]
                if cached is not None
                else (int(getattr(cfg, "initial_score", 50)) if cfg is not None else 50)
            )
            return level_for_score(score, is_owner=False)
        return None

    def thresholds(
        self,
        message=None,
        *,
        owner=False,
        known_bot=False,
        bot_invited=False,
        now: float | None = None,
    ):
        config = self.config
        preset = (
            config.group_activity.get(f"{message.platform}:{message.conversation_id}")
            if (message is not None and message.kind == "group")
            else None
        )
        reply = PRESETS[preset] if preset else config.reply_threshold
        aff = getattr(self.store.runtime, "affection", None)
        if not (known_bot and not bot_invited) and aff is not None:
            reply = aff.adjust_reply_threshold(message, reply)
        if owner:
            reply = min(reply, OWNER_REPLY_CEILING)
        elif known_bot and not bot_invited:
            reply = effective_bot_reply_threshold(message.user_id if message else "", self.s)
        if (
            self.hot_ring is not None
            and message is not None
            and message.kind == "group"
            and not message.addressed
            and not (known_bot and not bot_invited)
        ):
            aff_level = self._speaker_affection_level(aff, message)
            if aff_level is None or aff_level > 1:
                ref_now = time.time() if now is None else float(now)
                scope = self._group_scope(message)
                last_at = getattr(self, "last_proactive_spoke", {}).get(scope, 0.0)
                if last_at > 0 and (ref_now - last_at) < self.proactive_cooldown_seconds:
                    reply = round(min(0.95, reply + 0.12), 4)
                else:
                    act = self.hot_ring.recent_group_activity(
                        message,
                        window_seconds=300,
                        now=ref_now,
                        bot_ids=self.s.bot_qq_ids,
                    )
                    if act["count"] >= 15:
                        bonus = 0.08 + (
                            0.04 if (owner or (aff_level is not None and aff_level >= 4)) else 0.0
                        )
                        reply = round(min(reply, max(0.18, reply - bonus)), 4)
        return {
            "injection": config.owner_injection_threshold if owner else config.injection_threshold,
            "unsafe": config.owner_unsafe_threshold if owner else config.unsafe_threshold,
            "reply": reply,
        }

    async def status(self):
        await self.initialize()
        return {
            "config": self.config.model_dump(),
            "environment": self.env_config.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "presets": PRESETS,
            "owner_reply_ceiling": OWNER_REPLY_CEILING,
            "bot_reply_threshold": self.s.bot_reply_threshold,
            "environment_bot_reply_threshold": self.store.runtime.env_admission.bot_reply_threshold,
            "source_thresholds": {
                "injection": self.s.injection_threshold,
                "unsafe": self.s.unsafe_threshold,
            },
        }

    async def record(
        self,
        message,
        payload,
        trace,
        audit_source,
        *,
        audit_id: str | None = None,
        job_id: str | None = None,
    ):
        await self.initialize()
        key, now = audit_id or uuid.uuid4().hex, time.time()
        user_id = message.user_id if message else ""
        tracked = bool(audit_id or job_id) and trace.get("result") in {"chat", "bilibili_watch"}
        value = {
            "at": now,
            "source": audit_source
            if audit_source in {"production", "sandbox", "memory"}
            else "production",
            "surface": message.kind if message else "public_task",
            "platform": message.platform if message else None,
            "group_id": message.conversation_id if message and message.kind == "group" else None,
            "user_id_mask": "…" + user_id[-4:] if len(user_id) > 4 else "[匿名]",
            "excerpt": redact_excerpt(payload.get("current_message", ""), self.s),
            "attention_reason": getattr(message, "attention_reason", "") if message else "",
            "explicit_reply": bool(trace.get("reply_bypassed")),
            "purpose": "message",
            "scores": {
                key: trace.get("answers", {}).get(key)
                for key in (
                    "is_prompt_injection",
                    "is_unsafe_or_spam",
                    "should_reply",
                    "need_memory_recall",
                    "need_external_lookup",
                    "reasoning_tier",
                )
            },
            "tier": trace.get("tier"),
            "privacy_level": trace.get("privacy_level"),
            "decision_tier": trace.get("decision_tier"),
            "sandbox_mode": trace.get("sandbox_mode", False),
            "thresholds": trace.get("thresholds", {}),
            "result": trace.get("result", "blocked"),
            "reason": trace.get("reason", "invalid_decision"),
            "reply_bypassed": bool(trace.get("reply_bypassed")),
            "execution_status": "processing" if tracked else "",
            "execution_detail": "",
        }
        if attempts := safe_attempts(trace.get("provider_diagnostics")):
            value["provider_diagnostics"] = attempts
        if job_id:
            value["job_id"] = job_id
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR REPLACE INTO decision_audit VALUES (?,?,?)",
                (key, now, self.store.vault.seal("decision-audit:" + key, value)),
            )
            await self.store.db.execute(
                "DELETE FROM decision_audit WHERE id NOT IN "
                "(SELECT id FROM decision_audit ORDER BY at DESC,rowid DESC LIMIT 30)"
            )
        return key

    async def update_audit(
        self, audit_id: str | None, status: str, detail: str = "", *, diagnostics=None
    ):
        if not audit_id or not re.fullmatch(r"[a-z0-9_]{1,32}", status):
            return
        await self.initialize()
        async with self.store.transaction():
            row = await self.store.one("SELECT payload FROM decision_audit WHERE id=?", (audit_id,))
            if not row:
                return
            value = self.store.vault.open("decision-audit:" + audit_id, row["payload"])
            if value.get("result") in {"ignore", "blocked"}:
                return
            value["execution_status"] = status
            value["execution_detail"] = _safe_detail(detail)
            if diagnostics:
                value["provider_diagnostics"] = append_attempts(
                    value.get("provider_diagnostics"), diagnostics
                )
            await self.store.db.execute(
                "UPDATE decision_audit SET payload=? WHERE id=?",
                (self.store.vault.seal("decision-audit:" + audit_id, value), audit_id),
            )

    async def record_generation(
        self,
        audit_id: str | None,
        *,
        thinking_level: str | None = None,
        model_lane: str | None = None,
        provider_incident: dict | None = None,
        diagnostics=None,
        web_search: dict | None = None,
    ):
        if not audit_id:
            return
        valid_level = (
            thinking_level
            if thinking_level in {"none", "minimal", "low", "medium", "high", "xhigh"}
            else None
        )
        valid_lane = model_lane if model_lane in LANES else None
        incident = None
        if provider_incident and provider_incident.get("provider") in {
            "vertex_paid",
            "gemini_paid",
        }:
            incident = {
                "provider": provider_incident["provider"],
                "code": upstream_error_code(provider_incident.get("code")),
            }
            if status := upstream_http_status(incident["code"]):
                incident["http_status"] = status
            fallback = provider_incident.get("fallback")
            if fallback in {
                "succeeded",
                "failed",
                "cancelled",
                "partial_output",
                "disabled",
                "ineligible",
                "not_dispatched",
            }:
                incident["fallback"] = fallback
            if provider_incident.get("fallback_target") in {"vertex_priority", "gemini_paid"}:
                incident["fallback_target"] = provider_incident["fallback_target"]
            if provider_incident.get("fallback_code"):
                incident["fallback_code"] = upstream_error_code(provider_incident["fallback_code"])
            if detail := _safe_detail(provider_incident.get("fallback_detail", "")):
                incident["fallback_detail"] = detail
        attempts = safe_attempts(diagnostics)
        ws = _safe_web_search_audit(web_search)
        if not valid_level and not valid_lane and not incident and not attempts and not ws:
            return
        await self.initialize()
        async with self.store.transaction():
            row = await self.store.one("SELECT payload FROM decision_audit WHERE id=?", (audit_id,))
            if not row:
                return
            value = self.store.vault.open("decision-audit:" + audit_id, row["payload"])
            if valid_level:
                value["thinking_level"] = valid_level
            if valid_lane:
                value["model_lane"] = valid_lane
            if attempts:
                value["provider_diagnostics"] = append_attempts(
                    value.get("provider_diagnostics"), attempts
                )
            if incident:
                value["provider_incident"] = incident
            if ws:
                value["web_search"] = ws
            await self.store.db.execute(
                "UPDATE decision_audit SET payload=? WHERE id=?",
                (self.store.vault.seal("decision-audit:" + audit_id, value), audit_id),
            )

    async def _legacy_call(self, job_id):
        # Old failures have no exception metadata. Only report facts tied to this exact
        # QQ job; another conversation can make a paid call at the same timestamp.
        row = await self.store.one(
            "SELECT t.created,p.provider,a.model_lane,u.input_tokens FROM ledger_work w "
            "JOIN ledger_times t ON t.id=w.id JOIN ledger_provider p ON p.id=w.id "
            "JOIN ledger_attribution a ON a.id=w.id LEFT JOIN ledger_usage u ON u.id=w.id "
            "WHERE w.root_kind='qq.reply' AND w.root_id=? AND w.kind=w.root_kind "
            "AND w.work_id=w.root_id ORDER BY t.created DESC,t.rowid DESC LIMIT 1",
            (job_id,),
        )
        if (
            row
            and row["input_tokens"] is None
            and row["provider"] in PROVIDERS
            and row["model_lane"] in LANES
        ):
            return {
                "started_at": row["created"],
                "provider": row["provider"],
                "model_lane": row["model_lane"],
            }
        return None

    async def recent_audit(self, limit=30):
        await self.initialize()
        limit = max(1, min(30, int(limit)))
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id,payload FROM decision_audit ORDER BY at DESC,rowid DESC LIMIT ?",
                (limit,),
            )
            items = []
            for row in rows:
                item = self.store.vault.open("decision-audit:" + row["id"], row["payload"])
                job_id = item.pop("job_id", None)
                if (
                    job_id
                    and not item.get("provider_diagnostics")
                    and "gemini_unavailable" in (item.get("reason"), item.get("execution_detail"))
                ):
                    if legacy := await self._legacy_call(job_id):
                        item["legacy_ledger_call"] = legacy
                if job_id and item.get("execution_status") in {"processing", "queued", "sending"}:
                    outbox = await self.store.one("SELECT status FROM outbox WHERE id=?", (job_id,))
                    if outbox:
                        status = outbox["status"]
                        if status == "pending":
                            item["execution_status"] = "queued"
                        elif status in {
                            "sending",
                            "sent",
                            "cancelled",
                            "blocked",
                            "uncertain",
                            "expired",
                        }:
                            item["execution_status"] = status
                    else:
                        job = await self.store.one("SELECT status FROM jobs WHERE id=?", (job_id,))
                        if job and job["status"] in {"rejected", "cancelled", "failed"}:
                            item["execution_status"] = job["status"]
                        elif job and job["status"] == "done":
                            item["execution_status"] = "no_reply"
                items.append(item)
            return items

    def _group_scope(self, message):
        return self.store.vault.digest("gateway-group", *message.session_parts(self.s.namespace))

    async def remember_group(self, message):
        if message.kind != "group":
            return
        await self.initialize()
        scope = self._group_scope(message)
        event = self.store.event_key(message)
        now = time.time()
        raw = str(message.text or "")
        if message.images and "[附图]" not in raw:
            raw = f"{raw.strip()} [附图]" if raw.strip() else "[附图]"
        text = redact_excerpt(raw, self.s, limit=320)
        if not text:
            return
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR IGNORE INTO gateway_group_context VALUES (?,?,?,?,?)",
                (
                    scope,
                    event,
                    now,
                    message.created_at,
                    self.store.vault.seal("group-context:" + scope + event, text),
                ),
            )
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE at<?", (now - 86400,)
            )
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE scope=? AND event NOT IN "
                "(SELECT event FROM gateway_group_context WHERE scope=? "
                "ORDER BY at DESC,rowid DESC LIMIT 24)",
                (scope, scope),
            )
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE rowid NOT IN "
                "(SELECT rowid FROM gateway_group_context ORDER BY at DESC,rowid DESC LIMIT 10000)"
            )

    async def group_context(self, message, *, limit: int = 10):
        if message.kind != "group":
            return []
        limit = max(1, min(24, int(limit)))
        await self.initialize()
        scope, event = self._group_scope(message), self.store.event_key(message)
        async with self.store.lock:
            current = await self.store.one(
                "SELECT at,rowid FROM gateway_group_context WHERE scope=? AND event=?",
                (scope, event),
            )
            rows = await self.store.all(
                "SELECT event,payload FROM gateway_group_context "
                "WHERE scope=? AND event<>? AND at>=? AND event_at<=? "
                "AND (at<? OR (at=? AND rowid<?)) "
                "ORDER BY at DESC,rowid DESC LIMIT ?",
                (
                    scope,
                    event,
                    time.time() - 86400,
                    message.created_at,
                    current[0] if current else time.time(),
                    current[0] if current else time.time(),
                    current[1] if current else 9223372036854775807,
                    limit,
                ),
            )
        return [
            self.store.vault.open("group-context:" + scope + r["event"], r["payload"])
            for r in reversed(rows)
        ]

    async def speaking_state(self, message):
        """Locally bucket timing/density; the zero-thought classifier receives no arithmetic."""
        await self.initialize()
        scope, now = self._group_scope(message), time.time()
        async with self.store.lock:
            row = await self.store.one(
                "SELECT COUNT(*) FROM gateway_group_context WHERE scope=? AND at>?",
                (scope, now - 60),
            )
        last = getattr(self, "last_spoke", {}).get(scope, 0)
        elapsed = now - last
        return (
            "cooling_heavy"
            if elapsed < 15 or row[0] >= 8
            else "cooling_light"
            if elapsed < 60
            else "ready"
        )

    def note_proactive_spoke(self, message, *, now: float | None = None):
        if message is None or getattr(message, "kind", None) != "group":
            return
        if not hasattr(self, "last_proactive_spoke"):
            self.last_proactive_spoke = {}
        scope = self._group_scope(message)
        self.last_proactive_spoke.pop(scope, None)
        self.last_proactive_spoke[scope] = time.time() if now is None else float(now)
        while len(self.last_proactive_spoke) > 2048:
            self.last_proactive_spoke.pop(next(iter(self.last_proactive_spoke)))

    def spoke(self, message):
        if message.kind != "group":
            return
        if not hasattr(self, "last_spoke"):
            self.last_spoke = {}
        self.last_spoke[self._group_scope(message)] = time.time()
        if len(self.last_spoke) > 2048:
            self.last_spoke.pop(next(iter(self.last_spoke)))
        if not getattr(message, "addressed", True):
            self.note_proactive_spoke(message)

    async def clear_group(self, message):
        if message.kind != "group":
            return
        await self.initialize()
        async with self.store.transaction():
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE scope=?", (self._group_scope(message),)
            )
