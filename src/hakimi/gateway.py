"""Hot inbound filters and bounded encrypted diagnostic/relevance data."""

import asyncio
import re
import time
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .security import Rejected, clean_text, configured_secrets

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
            not re.fullmatch(r"(?:onebot:[1-9][0-9]{0,19}|qq:[A-Za-z0-9_-]{1,128})", key)
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


class FilterManager:
    def __init__(self, store):
        self.store, self.s = store, store.s
        self.env_config = FilterConfig.from_settings(self.s)
        self.config = self.env_config
        self.overridden = False
        self.initialized = False
        self.init_lock, self.lock = asyncio.Lock(), asyncio.Lock()

    async def initialize(self):
        async with self.init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                await self.store.db.executescript(SCHEMA)
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
        groups = self.s.onebot_allowed_groups if platform == "onebot" else self.s.qq_allowed_groups
        if platform not in {"onebot", "qq"} or group_id not in groups:
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

    def thresholds(self, message=None, *, owner=False):
        config = self.config
        preset = (
            config.group_activity.get(f"{message.platform}:{message.conversation_id}")
            if (message is not None and message.kind == "group")
            else None
        )
        reply = PRESETS[preset] if preset else config.reply_threshold
        return {
            "injection": config.owner_injection_threshold if owner else config.injection_threshold,
            "unsafe": config.owner_unsafe_threshold if owner else config.unsafe_threshold,
            "reply": min(reply, OWNER_REPLY_CEILING) if owner else reply,
        }

    async def status(self):
        await self.initialize()
        return {
            "config": self.config.model_dump(),
            "environment": self.env_config.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "presets": PRESETS,
            "owner_reply_ceiling": OWNER_REPLY_CEILING,
            "source_thresholds": {
                "injection": self.s.injection_threshold,
                "unsafe": self.s.unsafe_threshold,
            },
        }

    async def record(self, message, payload, trace, audit_source):
        await self.initialize()
        key, now = uuid.uuid4().hex, time.time()
        user_id = message.user_id if message else ""
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
                for key in ("is_prompt_injection", "is_unsafe_or_spam", "should_reply")
            },
            "thresholds": trace.get("thresholds", {}),
            "result": trace.get("result", "blocked"),
            "reason": trace.get("reason", "invalid_decision"),
            "reply_bypassed": bool(trace.get("reply_bypassed")),
        }
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO decision_audit VALUES (?,?,?)",
                (key, now, self.store.vault.seal("decision-audit:" + key, value)),
            )
            await self.store.db.execute(
                "DELETE FROM decision_audit WHERE id NOT IN "
                "(SELECT id FROM decision_audit ORDER BY at DESC,rowid DESC LIMIT 30)"
            )

    async def recent_audit(self, limit=30):
        await self.initialize()
        limit = max(1, min(30, int(limit)))
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id,payload FROM decision_audit ORDER BY at DESC,rowid DESC LIMIT ?",
                (limit,),
            )
        return [
            self.store.vault.open("decision-audit:" + row["id"], row["payload"]) for row in rows
        ]

    def _group_scope(self, message):
        return self.store.vault.digest("gateway-group", *message.session_parts(self.s.namespace))

    async def remember_group(self, message):
        if message.kind != "group":
            return
        await self.initialize()
        scope = self._group_scope(message)
        event = self.store.event_key(message)
        now = time.time()
        text = redact_excerpt(message.text, self.s, limit=240)
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
                "ORDER BY at DESC,rowid DESC LIMIT 10)",
                (scope, scope),
            )
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE rowid NOT IN "
                "(SELECT rowid FROM gateway_group_context ORDER BY at DESC,rowid DESC LIMIT 10000)"
            )

    async def group_context(self, message):
        if message.kind != "group":
            return []
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
                "ORDER BY at DESC,rowid DESC LIMIT 10",
                (
                    scope,
                    event,
                    time.time() - 86400,
                    message.created_at,
                    current[0] if current else time.time(),
                    current[0] if current else time.time(),
                    current[1] if current else 9223372036854775807,
                ),
            )
        return [
            self.store.vault.open("group-context:" + scope + r["event"], r["payload"])
            for r in reversed(rows)
        ]

    async def clear_group(self, message):
        if message.kind != "group":
            return
        await self.initialize()
        async with self.store.transaction():
            await self.store.db.execute(
                "DELETE FROM gateway_group_context WHERE scope=?", (self._group_scope(message),)
            )
