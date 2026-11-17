"""A bounded OneBot signature capability; never exposed as a model tool."""

import asyncio
import logging
import re
import time

from pydantic import BaseModel, ConfigDict, Field

from .bot_server import is_persona_owner
from .security import Rejected, clean_text, configured_secrets, safe_output

SCHEMA = """
CREATE TABLE IF NOT EXISTS longnick_state (
  scope TEXT PRIMARY KEY, payload BLOB, last_success REAL NOT NULL DEFAULT 0,
  last_attempt REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'never', last_error TEXT
);
"""
log = logging.getLogger("hakimi")


class LongnickConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    enabled: bool = False
    cooldown_hours: int = Field(default=20, ge=20, le=168)


class Longnick:
    def __init__(self, service, *, clock=time.time):
        self.service, self.s, self.store = service, service.s, service.store
        self.clock = clock
        self.scope = self.store.vault.digest("longnick", self.s.namespace, self.s.onebot_self_id)
        self.env_config = LongnickConfig(
            enabled=getattr(self.s, "enable_daily_longnick", False),
            cooldown_hours=getattr(self.s, "longnick_cooldown_hours", 20),
        )
        self.config = self.env_config
        self.overridden = False
        self.lock = asyncio.Lock()
        self.init_lock = asyncio.Lock()
        self.initialized = False

    async def initialize(self):
        async with self.init_lock:
            if not self.initialized:
                await self._initialize()
                self.initialized = True

    async def _initialize(self):
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR IGNORE INTO longnick_state(scope) VALUES (?)", (self.scope,)
            )
            await self.store.db.execute(
                "UPDATE longnick_state SET status='uncertain',last_error='interrupted' "
                "WHERE scope=? AND status='attempted'",
                (self.scope,),
            )
            row = await self.store.one("SELECT payload FROM admin_config WHERE key='longnick'")
        if row:
            self.config = LongnickConfig.model_validate(
                self.store.vault.open("admin:longnick", row[0])
            )
            self.overridden = True

    async def set_config(self, value: LongnickConfig | None):
        await self.initialize()
        async with self.lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='longnick'")
                self.config, self.overridden = self.env_config, False
            else:
                await self.store.runtime.save_config("longnick", value.model_dump())
                self.config, self.overridden = value, True
        return await self.status()

    def normalize(self, raw):
        if not isinstance(raw, str) or len(raw) > 2000:
            raise Rejected("invalid_longnick")
        secrets = configured_secrets(self.s)
        # Check complete input BEFORE truncating; a prefix of a credential must not leak.
        if any(s and len(s) >= 8 and s in raw for s in secrets):
            raise Rejected("credential_in_longnick")
        text = clean_text(raw)
        text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"[`*_#~>\[\]<>]", "", text)
        text = " ".join(text.split())[:50].strip()
        if not text:
            raise Rejected("invalid_longnick")
        return safe_output(text, secrets, limit=50)

    async def update(self, raw, *, automatic=False, cooldown_seconds=None):
        await self.initialize()
        text = self.normalize(raw)
        async with self.lock:
            now = self.clock()
            if not self.s.onebot_enabled or not self.s.owner_qq_id:
                raise Rejected("configure_owner_qq_id")
            if automatic and not self.config.enabled:
                return {"status": "disabled"}
            async with self.store.lock:
                row = await self.store.one(
                    "SELECT * FROM longnick_state WHERE scope=?", (self.scope,)
                )
            auto_cooldown = (
                self.config.cooldown_hours * 3600
                if cooldown_seconds is None
                else float(cooldown_seconds)
            )
            if automatic and max(row["last_success"], row["last_attempt"]) + auto_cooldown > now:
                return {"status": "cooldown"}
            # Explicit human changes may override the daily cadence, but not double clicks.
            if row["last_attempt"] and row["last_attempt"] + 60 > now:
                return {"status": "cooldown"}
            if row["status"] == "uncertain" and row["payload"]:
                prior = self.store.vault.open(self.scope, row["payload"])
                if prior["text"] == text:
                    return {"status": "uncertain", "error": "previous_attempt_unconfirmed"}
            transport = self.service.sender.onebot
            ready = getattr(transport, "ready", None)
            if self.s.onebot_mode != "http" and (ready is None or not ready.is_set()):
                return {"status": "offline"}
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE longnick_state SET payload=?,last_attempt=?,status='attempted',"
                    "last_error=NULL WHERE scope=?",
                    (
                        self.store.vault.seal(self.scope, {"text": text, "automatic": automatic}),
                        now,
                        self.scope,
                    ),
                )
            status, error = "uncertain", "napcat_rejected_or_unavailable"
            try:
                response = await transport.call("set_self_longnick", {"longNick": text})
                # NapCat may report business errors inside an otherwise successful envelope.
                if response is not None and not isinstance(response, dict):
                    raise Rejected("invalid_longnick_response")
                response = response or {}
                if any(
                    response.get(k) not in (None, 0, "0", "")
                    for k in ("result", "errCode", "errMsg")
                ):
                    status, error = "failed", "napcat_longnick_rejected"
                else:
                    status, error = "success", None
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE longnick_state SET status=?,last_error=?,last_success=CASE "
                    "WHEN ?='success' THEN ? ELSE last_success END WHERE scope=?",
                    (status, error, status, self.clock(), self.scope),
                )
            if status != "success":
                log.info("event=longnick_update_failed")
            return {"status": status, "error": error}

    async def _apply_auto(self, value, *, cooldown_seconds=None, error_code):
        try:
            await self.initialize()
            if not self.config.enabled or not value:
                return {"status": "disabled_or_absent"}
            return await self.update(value, automatic=True, cooldown_seconds=cooldown_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.info("event=longnick_update_failed")
            return {"status": "failed", "error": error_code}

    async def apply_digest(self, value):
        # Safe hook after the sole daily digest call. Signature errors never abort the DM.
        return await self._apply_auto(value, error_code="invalid_daily_longnick")

    async def apply_patrol(self, value):
        # Safe hook after Bilibili patrol. 6h cooldown avoids clashing with nightly digest.
        cooldown = min(self.config.cooldown_hours * 3600, 6 * 3600)
        return await self._apply_auto(
            value, cooldown_seconds=cooldown, error_code="invalid_patrol_longnick"
        )

    async def apply_life_block(self, value: str | None):
        # Safe hook after LifeBlockPlanner wakes up; enforces full >=20h cooldown.
        if not self.initialized:
            try:
                await self.initialize()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.info("event=longnick_update_failed")
                return {"status": "failed", "error": "invalid_life_block_longnick"}
        return await self._apply_auto(
            value,
            cooldown_seconds=self.config.cooldown_hours * 3600,
            error_code="invalid_life_block_longnick",
        )

    async def command(self, message):
        direct = message.command_text if message.command_text is not None else message.text
        if not re.match(r"^/set-sign(?:\s|$)", direct):
            return None
        if message.kind != "private" or not is_persona_owner(message, self.s):
            raise Rejected("owner_private_only")
        raw = direct[len("/set-sign") :].strip()
        if not raw:
            return "用法：/set-sign <50 字以内的纯文本签名>。"
        result = await self.update(raw)
        return "签名更新状态：" + result["status"] + "。"

    async def status(self):
        await self.initialize()
        async with self.store.lock:
            row = await self.store.one("SELECT * FROM longnick_state WHERE scope=?", (self.scope,))
        return {
            "config": self.config.model_dump(),
            "source": "admin" if self.overridden else "env",
            "environment": self.env_config.model_dump(),
            "last_success": row["last_success"],
            "last_attempt": row["last_attempt"],
            "status": row["status"],
            "last_error": row["last_error"],
            "last_attempted_text": self.store.vault.open(self.scope, row["payload"])["text"]
            if row["payload"]
            else "",
            "next_auto_after": max(row["last_success"], row["last_attempt"])
            + self.config.cooldown_hours * 3600
            if row["last_attempt"]
            else None,
        }
