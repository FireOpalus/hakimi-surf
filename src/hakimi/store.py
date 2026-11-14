import asyncio
import fcntl
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite
import anyio

from .bot_guard import SCHEMA as BOT_SCHEMA
from .bot_guard import BotInteractionGuard
from .budget_calendar import SCHEMA as CALENDAR_SCHEMA
from .budget_calendar import initialize_calendar
from .budget_policy import BudgetConfig
from .config import Settings
from .group_pipeline import GroupPipeline
from .models import Message, Video
from .proactive_policy import SCHEMA as PROACTIVE_SCHEMA
from .public_memory import SCHEMA as PUBLIC_MEMORY_SCHEMA
from .runtime_state import SCHEMA as RUNTIME_SCHEMA
from .runtime_state import RuntimeState
from .security import Rejected, Vault
from .usage_accounting import TokenUsage

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, scope TEXT NOT NULL, subject TEXT NOT NULL, kind TEXT NOT NULL,
  payload BLOB, status TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, created);
CREATE TABLE IF NOT EXISTS counters (
  key TEXT PRIMARY KEY, n INTEGER NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS consent (scope TEXT PRIMARY KEY, enabled INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS memory (
  id INTEGER PRIMARY KEY, scope TEXT NOT NULL, payload BLOB NOT NULL, expires REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS memory_scope ON memory(scope, id);
CREATE TABLE IF NOT EXISTS ledger (
  id TEXT PRIMARY KEY, day TEXT NOT NULL, subject TEXT NOT NULL, lane TEXT NOT NULL,
  amount INTEGER NOT NULL, reserved INTEGER NOT NULL, status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ledger_day ON ledger(day, subject);
CREATE TABLE IF NOT EXISTS ledger_usage (
  id TEXT PRIMARY KEY, input_tokens INTEGER, output_tokens INTEGER,
  cached_tokens INTEGER, thought_tokens INTEGER, model_lane TEXT,
  estimated_amount INTEGER
);
CREATE TABLE IF NOT EXISTS usage_aggregates (
  period TEXT PRIMARY KEY, charged_amount INTEGER NOT NULL DEFAULT 0,
  calls INTEGER NOT NULL DEFAULT 0, known_usage_calls INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
  cached_tokens INTEGER NOT NULL DEFAULT 0, thought_tokens INTEGER NOT NULL DEFAULT 0,
  interaction_calls INTEGER NOT NULL DEFAULT 0, interaction_input_tokens INTEGER NOT NULL DEFAULT 0,
  cache_known_calls INTEGER NOT NULL DEFAULT 0, cache_known_input_tokens INTEGER NOT NULL DEFAULT 0,
  thought_known_calls INTEGER NOT NULL DEFAULT 0,
  estimated_amount INTEGER NOT NULL DEFAULT 0, estimated_calls INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS outbox (
  id TEXT PRIMARY KEY, payload BLOB, status TEXT NOT NULL,
  created REAL NOT NULL, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
  id TEXT PRIMARY KEY, subject TEXT NOT NULL, mid TEXT NOT NULL, target BLOB NOT NULL,
  initialized INTEGER NOT NULL DEFAULT 0, next_poll REAL NOT NULL DEFAULT 0,
  UNIQUE(subject, mid)
);
CREATE TABLE IF NOT EXISTS seen (
  subscription TEXT NOT NULL, bvid TEXT NOT NULL, created REAL NOT NULL,
  PRIMARY KEY(subscription, bvid)
);
CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY, subject TEXT NOT NULL, payload BLOB NOT NULL, created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS group_policies (
  scope TEXT PRIMARY KEY, mode TEXT NOT NULL CHECK(mode IN ('at_only','smart_reply','muted'))
);
"""


def microdollars(value: Decimal) -> int:
    return int((value * 1_000_000).to_integral_value(rounding=ROUND_CEILING))


class Store:
    """Single-host SQLite journal. One service process, multiple async consumers.

    One connection plus a transaction lock prevents coroutine transaction interleaving.
    flock deliberately rejects a second process before crash recovery is performed.
    """

    def __init__(self, settings: Settings):
        self.s = settings
        self.vault = Vault(settings.encryption_key.get_secret_value())
        self.lock = asyncio.Lock()
        self.db: aiosqlite.Connection
        self._process_lock = None
        self.bot_guard = BotInteractionGuard(self, settings)
        self.runtime = RuntimeState(self)
        self.groups = GroupPipeline(self)
        self.env_budget = BudgetConfig.from_settings(settings)
        self.budget_overridden = False
        self.budget_zone = ZoneInfo(settings.bot_timezone)

    def local_now(self):
        return datetime.now(UTC).astimezone(self.budget_zone)

    async def open(self) -> None:
        self.s.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._process_lock = (self.s.data_dir / "service.lock").open("a+")
        try:
            fcntl.flock(self._process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._process_lock.close()
            self._process_lock = None
            raise RuntimeError("Run exactly one service process per data directory") from None
        self.db = await aiosqlite.connect(self.s.data_dir / "hakimi.sqlite3", isolation_level=None)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=FULL")
        await self.db.execute("PRAGMA secure_delete=ON")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.executescript(
            SCHEMA
            + BOT_SCHEMA
            + RUNTIME_SCHEMA
            + PROACTIVE_SCHEMA
            + PUBLIC_MEMORY_SCHEMA
            + CALENDAR_SCHEMA
        )
        (self.s.data_dir / "hakimi.sqlite3").chmod(0o600)
        async with self.transaction():
            await self.db.execute(
                "INSERT OR IGNORE INTO budget_totals SELECT 1,COALESCE(SUM(amount),0) FROM ledger"
            )
            await self._initialize_usage_aggregates()
            await initialize_calendar(self)
            row = await self.one("SELECT value FROM meta WHERE key='key_check'")
            if row:
                self.vault.open("key_check", row[0])
            else:
                await self.db.execute(
                    "INSERT INTO meta VALUES ('key_check', ?)",
                    (self.vault.seal("key_check", "ok"),),
                )
            namespace = await self.one("SELECT value FROM meta WHERE key='namespace'")
            if namespace and self.vault.open("namespace", namespace[0]) != self.s.namespace:
                raise RuntimeError("A namespace change requires a separate data directory")
            await self.db.execute(
                "INSERT OR IGNORE INTO meta VALUES ('namespace',?)",
                (self.vault.seal("namespace", self.s.namespace),),
            )
            # A crash may have occurred after charging/sending. Never blindly repeat it.
            await self.db.execute(
                "UPDATE jobs SET status='abandoned',payload=NULL WHERE status='working'"
            )
            await self.db.execute(
                "UPDATE outbox SET status='uncertain',payload=NULL WHERE status='sending'"
            )
            # Serialize all members of a group against the new shared conversation session.
            for job in await self.all(
                "SELECT id,payload FROM jobs WHERE kind='message' AND status='pending'"
            ):
                message = Message.model_validate(self.vault.open(job["id"], job["payload"]))
                await self.db.execute(
                    "UPDATE jobs SET scope=? WHERE id=?", (self.session_scope(message), job["id"])
                )
            for group, mode in self.s.group_policies.items():
                await self.db.execute(
                    "INSERT INTO group_policies VALUES (?,?) ON CONFLICT(scope) "
                    "DO UPDATE SET mode=excluded.mode",
                    (self.policy_key(group), mode),
                )
            for platform, groups in (
                ("onebot", self.s.onebot_allowed_groups),
                ("qq", self.s.qq_allowed_groups),
            ):
                for group in groups:
                    name = f"{platform}:{group}"
                    row = await self.one(
                        "SELECT mode FROM group_policies WHERE scope=?", (self.policy_key(name),)
                    )
                    if row:
                        self.s.group_policies[name] = row[0]
        await self.runtime.load_config()
        await self._load_budget_config()
        await self.groups.initialize()

    @staticmethod
    def _periods(day):
        return ("day:" + day, "month:" + day[:7], "lifetime")

    async def _initialize_usage_aggregates(self):
        """Migration counts only retained rows; pruned token/call history is unknown."""
        if await self.one("SELECT 1 FROM meta WHERE key='usage_history_v1'"):
            return
        total = (await self.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
        retained, calls = await self.one("SELECT COALESCE(SUM(amount),0),COUNT(*) FROM ledger")
        for row in await self.all(
            "SELECT day,SUM(amount) AS amount,COUNT(*) AS n FROM ledger GROUP BY day"
        ):
            for period in ("day:" + row["day"], "month:" + row["day"][:7]):
                await self.db.execute(
                    "INSERT INTO usage_aggregates(period,charged_amount,calls) VALUES (?,?,?) "
                    "ON CONFLICT(period) DO UPDATE SET "
                    "charged_amount=charged_amount+excluded.charged_amount,"
                    "calls=calls+excluded.calls",
                    (period, row["amount"], row["n"]),
                )
        await self.db.execute(
            "INSERT INTO usage_aggregates(period,charged_amount,calls) VALUES ('lifetime',?,?)",
            (total, calls),
        )
        history = {
            "since_utc": datetime.now(UTC).isoformat(),
            "legacy_unattributed_microdollars": max(0, total - retained),
            # An old DB may have pruned zero-cost rows too; totals alone cannot prove completeness.
            "lifetime_call_count_complete": not bool(
                await self.one("SELECT 1 FROM meta WHERE key='key_check'")
            ),
        }
        await self.db.execute(
            "INSERT INTO meta VALUES ('usage_history_v1',?)",
            (self.vault.seal("usage-history", history),),
        )

    async def _load_budget_config(self):
        async with self.lock:
            row = await self.one("SELECT payload FROM admin_config WHERE key='budget'")
        if row:
            self._apply_budget(BudgetConfig.model_validate(self.vault.open("admin:budget", row[0])))
            self.budget_overridden = True

    def _apply_budget(self, value):
        self.s.daily_usd = value.daily_usd
        self.s.monthly_usd = value.monthly_usd

    async def budget_config_status(self):
        async with self.lock:
            return {
                "config": BudgetConfig.from_settings(self.s).model_dump(mode="json"),
                "environment": self.env_budget.model_dump(mode="json"),
                "source": "admin" if self.budget_overridden else "env",
            }

    async def set_budget_config(self, value: BudgetConfig | None):
        if value is not None and not isinstance(value, BudgetConfig):
            raise ValueError("Expected a validated BudgetConfig")
        async with self.transaction():
            if value is None:
                await self.db.execute("DELETE FROM admin_config WHERE key='budget'")
            else:
                await self.db.execute(
                    "INSERT INTO admin_config VALUES ('budget',?) ON CONFLICT(key) "
                    "DO UPDATE SET payload=excluded.payload",
                    (self.vault.seal("admin:budget", value.model_dump(mode="json")),),
                )
        self._apply_budget(value or self.env_budget)
        self.budget_overridden = value is not None
        return await self.budget_config_status()

    async def _charge_aggregates(self, day, delta, calls=0):
        for period in self._periods(day):
            await self.db.execute(
                "INSERT INTO usage_aggregates(period,charged_amount,calls) VALUES (?,?,?) "
                "ON CONFLICT(period) DO UPDATE SET "
                "charged_amount=charged_amount+excluded.charged_amount,"
                "calls=calls+excluded.calls",
                (period, delta, calls),
            )

    async def _calendar_carry(self, period):
        row = await self.one("SELECT amount FROM budget_calendar_carry WHERE period=?", (period,))
        return row[0] if row else 0

    async def close(self) -> None:
        if hasattr(self, "db"):
            await self.db.close()
        if self._process_lock is not None:
            self._process_lock.close()
            self._process_lock = None

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            try:
                # SQLite may execute BEGIN even if awaiting its result is cancelled.
                await self.db.execute("BEGIN IMMEDIATE")
                yield
                await self.db.commit()
            except BaseException:
                # Keep the lock until cleanup survives AnyIO's repeated cancellation.
                with anyio.CancelScope(shield=True):
                    await self.db.rollback()
                raise

    async def one(self, sql: str, args: tuple = ()) -> aiosqlite.Row | None:
        async with self.db.execute(sql, args) as cur:
            return await cur.fetchone()

    async def all(self, sql: str, args: tuple = ()) -> list[aiosqlite.Row]:
        async with self.db.execute(sql, args) as cur:
            return await cur.fetchall()

    def scope(self, message: Message) -> str:
        return self.vault.digest("scope", *message.scope_parts(self.s.namespace))

    def subject(self, message: Message) -> str:
        return self.vault.digest("subject", *message.subject_parts(self.s.namespace))

    def session_scope(self, message: Message) -> str:
        return self.vault.digest("session-v2", *message.session_parts(self.s.namespace))

    def policy_key(self, group: str) -> str:
        platform = group.partition(":")[0]
        bot = self.s.onebot_self_id if platform == "onebot" else self.s.qq_app_id
        return self.vault.digest("policy", self.s.namespace, bot, group)

    def event_key(self, message: Message) -> str:
        return self.vault.digest(
            "event",
            self.s.namespace,
            message.platform,
            message.bot_id,
            message.kind,
            message.conversation_id,
            message.event_id,
        )

    async def admit(self, message: Message, *, continuation_only: bool = False) -> str:
        now = time.time()
        scope, subject = self.session_scope(message), self.subject(message)
        job_id = self.event_key(message)
        async with self.transaction():
            if await self.one("SELECT 1 FROM jobs WHERE id=?", (job_id,)):
                return "duplicate"
            if continuation_only and not await self.groups.continuation_in_transaction(
                message, now
            ):
                return "ignored"
            if self.bot_guard.known_bot(message):
                if await self.bot_guard.available_in_transaction(message, now) is None:
                    return "ignored"
            n = await self.one("SELECT COUNT(*) FROM jobs WHERE status IN ('pending','working')")
            if n[0] >= self.s.queue_capacity:
                return "full"
            bucket = int(now // 60)
            keys = [
                (f"user:{subject}:{bucket}", self.s.user_requests_per_minute),
                (f"global:{bucket}", self.s.global_requests_per_minute),
            ]
            for key, limit in keys:
                row = await self.one("SELECT n FROM counters WHERE key=?", (key,))
                if row and row[0] >= limit:
                    return "limited"
            # Grant consumption, rate counters and job insertion commit together.
            if not await self.bot_guard.consume_in_transaction(message, job_id, now):
                return "ignored"
            for key, _ in keys:
                await self.db.execute(
                    "INSERT INTO counters VALUES (?,1,?) ON CONFLICT(key) DO UPDATE SET n=n+1",
                    (key, now + 120),
                )
            await self.db.execute(
                "INSERT INTO jobs VALUES (?,?,?,?,?,'pending',?,?)",
                (
                    job_id,
                    scope,
                    subject,
                    "message",
                    self.vault.seal(job_id, message.model_dump()),
                    now,
                    min(
                        now + self.s.event_ttl_seconds,
                        message.created_at + self.s.event_ttl_seconds,
                    ),
                ),
            )
            await self.groups.register_in_transaction(message, job_id, now)
        return "accepted"

    async def claim(self) -> dict[str, Any] | None:
        async with self.transaction():
            now = time.time()
            await self.db.execute(
                "UPDATE jobs SET status='expired',payload=NULL "
                "WHERE status='pending' AND expires<?",
                (now,),
            )
            row = await self.one(
                "SELECT * FROM jobs j WHERE status='pending' AND NOT EXISTS "
                "(SELECT 1 FROM jobs active WHERE active.scope=j.scope "
                "AND active.status='working') "
                "AND NOT EXISTS (SELECT 1 FROM group_debounce_items i "
                "JOIN group_debounce_batches b ON b.id=i.batch_id "
                "WHERE i.job_id=j.id AND b.ready_at>?) "
                "ORDER BY created, rowid LIMIT 1",
                (now,),
            )
            if not row:
                return None
            return await self.groups.claim_in_transaction(row)

    async def finish(self, job_id: str, status: str = "done") -> None:
        async with self.transaction():
            await self.groups.finish_in_transaction(job_id, status)
            await self.db.execute(
                "UPDATE jobs SET status=?,payload=NULL WHERE id=?", (status, job_id)
            )

    async def memory_enabled(self, scope: str) -> bool:
        async with self.lock:
            row = await self.one("SELECT enabled FROM consent WHERE scope=?", (scope,))
            return bool(row and row[0])

    async def set_memory(self, scope: str, enabled: bool) -> None:
        async with self.transaction():
            await self.db.execute(
                "INSERT INTO consent VALUES (?,?) ON CONFLICT(scope) "
                "DO UPDATE SET enabled=excluded.enabled",
                (scope, int(enabled)),
            )
            if not enabled:
                await self.db.execute("DELETE FROM memory WHERE scope=?", (scope,))

    async def history(self, scope: str) -> list[dict]:
        async with self.lock:
            rows = await self.all(
                "SELECT payload FROM memory WHERE scope=? AND expires>? ORDER BY id DESC LIMIT ?",
                (scope, time.time(), self.s.memory_turns),
            )
            return [self.vault.open(scope, row[0]) for row in reversed(rows)]

    async def remember(self, scope: str, user: str, assistant: str) -> None:
        async with self.transaction():
            consent = await self.one("SELECT enabled FROM consent WHERE scope=?", (scope,))
            if not consent or not consent[0]:
                return
            await self.db.execute(
                "INSERT INTO memory(scope,payload,expires) VALUES (?,?,?)",
                (
                    scope,
                    self.vault.seal(scope, {"user": user, "assistant": assistant}),
                    time.time() + self.s.memory_ttl_seconds,
                ),
            )
            await self.db.execute(
                "DELETE FROM memory WHERE scope=? AND id NOT IN "
                "(SELECT id FROM memory WHERE scope=? ORDER BY id DESC LIMIT ?)",
                (scope, scope, self.s.memory_turns),
            )

    async def reserve(self, subject: str, lane: str, usd: Decimal) -> str:
        if not isinstance(usd, Decimal) or not usd.is_finite() or usd < 0:
            raise Rejected("invalid_budget_amount")
        now = self.local_now()
        day = now.date().isoformat()
        amount, reservation = microdollars(usd), uuid.uuid4().hex
        async with self.transaction():
            if await self.one("SELECT 1 FROM meta WHERE key='budget_overrun'"):
                raise Rejected("budget_circuit_open")
            total = (await self.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
            month = await self.one(
                "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("month:" + day[:7],)
            )
            monthly_total = (month[0] if month else 0) + await self._calendar_carry(
                "month:" + day[:7]
            )
            monthly_limit = microdollars(getattr(self.s, "monthly_usd", Decimal("40")))
            daily_row = await self.one(
                "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("day:" + day,)
            )
            global_total = (daily_row[0] if daily_row else 0) + await self._calendar_carry(
                "day:" + day
            )
            personal = (
                await self.one(
                    "SELECT COALESCE(SUM(amount),0) FROM ledger WHERE day=? AND subject=?",
                    (day, subject),
                )
            )[0]
            patrol = (
                await self.one(
                    "SELECT COALESCE(SUM(amount),0) FROM ledger WHERE day=? AND lane='patrol'",
                    (day,),
                )
            )[0]
            lane_total = (
                await self.one(
                    "SELECT COALESCE(SUM(amount),0) FROM ledger WHERE day=? AND lane=?", (day, lane)
                )
            )[0]
            extra_limit = {
                "memory": self.s.memory_daily_usd,
                "admin": self.s.admin_daily_usd,
                "proactive": self.s.proactive_daily_usd,
            }.get(lane)
            if (
                self.s.daily_usd <= 0
                or monthly_limit <= 0
                or global_total + amount > microdollars(self.s.daily_usd)
                or monthly_total + amount > monthly_limit
                or personal + amount > microdollars(self.s.user_daily_usd)
                or (lane == "patrol" and patrol + amount > microdollars(self.s.patrol_daily_usd))
                or (extra_limit is not None and lane_total + amount > microdollars(extra_limit))
                or (self.s.total_usd > 0 and total + amount > microdollars(self.s.total_usd))
            ):
                raise Rejected("budget_exhausted")
            await self.db.execute(
                "INSERT INTO ledger VALUES (?,?,?,?,?,?,'reserved')",
                (reservation, day, subject, lane, amount, amount),
            )
            await self.db.execute(
                "INSERT INTO ledger_times VALUES (?,?)", (reservation, now.timestamp())
            )
            await self.db.execute("UPDATE budget_totals SET amount=amount+?", (amount,))
            await self._charge_aggregates(day, amount, 1)
        return reservation

    async def settle(
        self,
        reservation: str,
        actual: Decimal | None,
        *,
        usage: TokenUsage | None = None,
        discounted_actual: Decimal | None = None,
    ) -> None:
        # No usage or an ambiguous failure: keep the entire reservation, including on restart.
        if actual is not None and (
            not isinstance(actual, Decimal) or not actual.is_finite() or actual < 0
        ):
            raise Rejected("invalid_budget_amount")
        if not isinstance(usage, TokenUsage):
            usage = None
        # Unknown/malformed discount metadata must not interfere with conservative settlement.
        if (
            not isinstance(discounted_actual, Decimal)
            or not discounted_actual.is_finite()
            or discounted_actual < 0
            or actual is None
            or discounted_actual > actual
        ):
            discounted_actual = None
        overrun = False
        async with self.transaction():
            row = await self.one("SELECT * FROM ledger WHERE id=?", (reservation,))
            if row is None or row["status"] != "reserved":
                return
            amount = row["amount"] if actual is None else microdollars(actual)
            await self.db.execute(
                "UPDATE budget_totals SET amount=amount+?", (amount - row["amount"],)
            )
            await self._charge_aggregates(row["day"], amount - row["amount"])
            overrun = amount > row["reserved"]
            await self.db.execute(
                "UPDATE ledger SET amount=?,status=? WHERE id=?",
                (
                    amount,
                    "unknown" if actual is None else "settled",
                    reservation,
                ),
            )
            estimated = microdollars(discounted_actual) if discounted_actual is not None else None
            await self.db.execute(
                "INSERT INTO ledger_usage VALUES (?,?,?,?,?,?,?)",
                (
                    reservation,
                    usage.input_tokens if usage else None,
                    usage.output_tokens if usage else None,
                    usage.cached_tokens if usage else None,
                    usage.thought_tokens if usage else None,
                    usage.model_lane if usage else None,
                    estimated,
                ),
            )
            interaction = bool(usage and usage.model_lane == "interaction")
            cache_known = bool(interaction and usage.cached_tokens is not None)
            for period in self._periods(row["day"]):
                await self.db.execute(
                    "UPDATE usage_aggregates SET known_usage_calls=known_usage_calls+?,"
                    "input_tokens=input_tokens+?,output_tokens=output_tokens+?,"
                    "thought_tokens=thought_tokens+?,cached_tokens=cached_tokens+?,"
                    "interaction_calls=interaction_calls+?,interaction_input_tokens=interaction_input_tokens+?,"
                    "cache_known_calls=cache_known_calls+?,cache_known_input_tokens=cache_known_input_tokens+?,"
                    "thought_known_calls=thought_known_calls+?,estimated_amount=estimated_amount+?,"
                    "estimated_calls=estimated_calls+? WHERE period=?",
                    (
                        int(usage is not None),
                        usage.input_tokens if usage else 0,
                        usage.output_tokens if usage else 0,
                        (usage.thought_tokens or 0) if usage else 0,
                        usage.cached_tokens if cache_known else 0,
                        int(interaction),
                        usage.input_tokens if interaction else 0,
                        int(cache_known),
                        usage.input_tokens if cache_known else 0,
                        int(bool(usage and usage.thought_tokens is not None)),
                        estimated or 0,
                        int(estimated is not None),
                        period,
                    ),
                )
            if overrun:
                await self.db.execute("INSERT OR REPLACE INTO meta VALUES ('budget_overrun','1')")
        if overrun:
            raise Rejected("budget_reservation_exceeded")

    async def usage(self) -> dict:
        day = self.local_now().date().isoformat()
        async with self.lock:
            rows = await self.all(
                "SELECT lane,SUM(amount) AS amount,COUNT(*) AS calls "
                "FROM ledger WHERE day=? GROUP BY lane",
                (day,),
            )
            statuses = await self.all("SELECT status,COUNT(*) AS n FROM jobs GROUP BY status")
            token_usage, cost_estimates = {}, {}
            history_row = await self.one("SELECT value FROM meta WHERE key='usage_history_v1'")
            history = self.vault.open("usage-history", history_row[0])
            calendar = await self.one("SELECT value FROM meta WHERE key='budget_calendar_v2'")
            history["calendar"] = self.vault.open("budget-calendar", calendar[0])
            for label, period in zip(
                ("today", "month", "lifetime"), self._periods(day), strict=True
            ):
                observed = await self.one(
                    "SELECT * FROM usage_aggregates WHERE period=?", (period,)
                )
                stats = dict(observed) if observed else {}
                keys = (
                    "calls",
                    "known_usage_calls",
                    "input_tokens",
                    "output_tokens",
                    "thought_tokens",
                    "cached_tokens",
                    "interaction_calls",
                    "interaction_input_tokens",
                    "cache_known_calls",
                    "cache_known_input_tokens",
                    "thought_known_calls",
                )
                tokens = {key: stats.get(key, 0) for key in keys}
                tokens["unknown_usage_calls"] = tokens["calls"] - tokens["known_usage_calls"]
                tokens["cache_hit_ratio"] = (
                    tokens["cached_tokens"] / tokens["interaction_input_tokens"]
                    if tokens["interaction_input_tokens"]
                    else None
                )
                tokens["cache_observed_hit_ratio"] = (
                    tokens["cached_tokens"] / tokens["cache_known_input_tokens"]
                    if tokens["cache_known_input_tokens"]
                    else None
                )
                tokens["cache_coverage_complete"] = (
                    tokens["interaction_calls"] == tokens["cache_known_calls"]
                    and tokens["unknown_usage_calls"] == 0
                    and (label != "lifetime" or history["lifetime_call_count_complete"])
                )
                for key in ("input_tokens", "output_tokens", "cached_tokens", "thought_tokens"):
                    tokens["total_" + key] = tokens[key]
                token_usage[label] = tokens
                estimates = stats.get("estimated_calls", 0)
                cost_estimates[label] = {
                    "charged_microdollars": stats.get("charged_amount", 0),
                    "calendar_carry_microdollars": await self._calendar_carry(period),
                    "estimated_microdollars": stats.get("estimated_amount", 0)
                    if estimates
                    else None,
                    "estimated_calls": estimates,
                    "unknown_estimate_calls": tokens["calls"] - estimates,
                    "complete": estimates == tokens["calls"]
                    and (label != "lifetime" or history["lifetime_call_count_complete"]),
                }
            return {
                "day_local": day,
                "month_local": day[:7],
                "timezone": self.budget_zone.key,
                # Deprecated alias for existing clients; the bucket is now explicitly local.
                "day_utc": day,
                "lanes": [dict(r) for r in rows],
                "jobs": [dict(r) for r in statuses],
                "token_usage": token_usage,
                "cost_estimates": cost_estimates,
                "history": history,
            }

    async def budget_status(self):
        day = self.local_now().date().isoformat()
        async with self.lock:
            day_row = await self.one(
                "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("day:" + day,)
            )
            daily_observed = day_row[0] if day_row else 0
            daily_carry = await self._calendar_carry("day:" + day)
            daily = daily_observed + daily_carry
            total = (await self.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
            month = await self.one(
                "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("month:" + day[:7],)
            )
            monthly_observed = month[0] if month else 0
            monthly_carry = await self._calendar_carry("month:" + day[:7])
            monthly = monthly_observed + monthly_carry
            circuit = bool(await self.one("SELECT 1 FROM meta WHERE key='budget_overrun'"))
            daily_limit, total_limit = (
                microdollars(self.s.daily_usd),
                microdollars(self.s.total_usd),
            )
            monthly_limit = microdollars(getattr(self.s, "monthly_usd", Decimal("40")))
        return {
            "day_local": day,
            "month_local": day[:7],
            "timezone": self.budget_zone.key,
            "day_utc": day,
            "month_utc": day[:7],
            "daily": daily,
            "daily_observed": daily_observed,
            "daily_calendar_carry": daily_carry,
            "daily_limit": daily_limit,
            "monthly": monthly,
            "monthly_observed": monthly_observed,
            "monthly_calendar_carry": monthly_carry,
            "monthly_limit": monthly_limit,
            "total": total,
            "total_limit": total_limit,
            "hard_stop": circuit
            or daily >= daily_limit
            or monthly >= monthly_limit
            or (total_limit > 0 and total >= total_limit),
            "circuit": circuit,
        }

    async def enqueue_reply(
        self,
        job_id: str,
        message: Message,
        text: str,
        proactive: bool = False,
        *,
        session_epoch=None,
        meme_tag=None,
        meme_eligible=False,
        participants=None,
        segments=None,
        allowed_at_qqs=(),
    ) -> None:
        now = time.time()
        payload = {"message": message.model_dump(), "text": text, "proactive": proactive}
        if meme_eligible:
            payload.update(meme_tag=meme_tag, meme_eligible=True)
        if session_epoch is not None:
            payload["session_epoch"] = session_epoch
        if participants:
            payload["participants"] = [m.model_dump() for m in participants]
        if segments:
            payload["segments"] = segments
            payload["allowed_at_qqs"] = sorted(set(allowed_at_qqs))
        expires = now + 3600 if proactive else message.created_at + self.s.event_ttl_seconds
        async with self.transaction():
            if not proactive:
                job = await self.one("SELECT expires FROM jobs WHERE id=?", (job_id,))
                if job:
                    # Video jobs alone can extend their server-owned deadline, bounded to 15m.
                    expires = min(max(expires, job["expires"]), message.created_at + 900)
            await self.db.execute(
                "INSERT OR IGNORE INTO outbox VALUES (?,?,'pending',?,?)",
                (job_id, self.vault.seal(job_id, payload), now, expires),
            )

    async def claim_reply(self) -> dict | None:
        async with self.transaction():
            await self.db.execute(
                "UPDATE outbox SET status='expired',payload=NULL "
                "WHERE status='pending' AND expires<?",
                (time.time(),),
            )
            row = await self.one(
                "SELECT * FROM outbox WHERE status='pending' ORDER BY created LIMIT 1"
            )
            if not row:
                return None
            await self.db.execute("UPDATE outbox SET status='sending' WHERE id=?", (row["id"],))
            return {"id": row["id"], **self.vault.open(row["id"], row["payload"])}

    async def finish_reply(self, reply_id: str, status: str) -> bool:
        async with self.transaction():
            cursor = await self.db.execute(
                "UPDATE outbox SET status=?,payload=NULL WHERE id=? AND status='sending'",
                (status, reply_id),
            )
            return cursor.rowcount == 1

    async def cancel_session_replies(self, message: Message):
        # Jobs already carry a HMAC session scope. Never decrypt other sessions' outboxes.
        async with self.transaction():
            await self.db.execute(
                "UPDATE outbox SET status='cancelled',payload=NULL "
                "WHERE status IN ('pending','sending') AND id IN "
                "(SELECT id FROM jobs WHERE kind='message' AND scope=?)",
                (self.session_scope(message),),
            )

    async def add_subscription(self, message: Message, mid: str) -> None:
        subject = self.subject(message)
        sub_id = self.vault.digest("subscription", subject, mid)
        # Keep only a delivery address, never a historical incoming message as a reply ticket.
        target = message.model_copy(update={"text": "", "event_id": "patrol"})
        async with self.transaction():
            if await self.one("SELECT 1 FROM subscriptions WHERE id=?", (sub_id,)):
                return
            count = (await self.one("SELECT COUNT(*) FROM subscriptions"))[0]
            if count >= self.s.max_subscriptions:
                raise Rejected("subscription_limit")
            await self.db.execute(
                "INSERT INTO subscriptions(id,subject,mid,target) VALUES (?,?,?,?)",
                (sub_id, subject, mid, self.vault.seal(sub_id, target.model_dump())),
            )

    async def subscriptions(self, subject: str | None = None) -> list[dict]:
        async with self.lock:
            sql = "SELECT * FROM subscriptions"
            rows = await self.all(
                sql + (" WHERE subject=?" if subject else ""), (subject,) if subject else ()
            )
            return [{**dict(r), "target": self.vault.open(r["id"], r["target"])} for r in rows]

    async def remove_subscription(self, subject: str, mid: str) -> None:
        sub_id = self.vault.digest("subscription", subject, mid)
        async with self.transaction():
            await self.db.execute("DELETE FROM subscriptions WHERE id=?", (sub_id,))
            await self.db.execute("DELETE FROM seen WHERE subscription=?", (sub_id,))
            await self.db.execute(
                "UPDATE jobs SET status='cancelled',payload=NULL "
                "WHERE scope=? AND status='pending'",
                (sub_id,),
            )

    async def poll_started(self, sub_id: str, next_poll: float) -> None:
        async with self.transaction():
            await self.db.execute(
                "UPDATE subscriptions SET next_poll=? WHERE id=?", (next_poll, sub_id)
            )

    async def ingest(self, sub_id: str, videos: list[Video]) -> int:
        now = time.time()
        async with self.transaction():
            sub = await self.one("SELECT * FROM subscriptions WHERE id=?", (sub_id,))
            if not sub:
                return 0
            known = {
                r[0]
                for r in await self.all("SELECT bvid FROM seen WHERE subscription=?", (sub_id,))
            }
            fresh = [v for v in videos if v.bvid not in known]
            if sub["initialized"]:
                fresh = sorted(fresh, key=lambda v: v.published_at)[: self.s.patrol_batch_size]
                count = (
                    await self.one(
                        "SELECT COUNT(*) FROM jobs WHERE status IN ('pending','working')"
                    )
                )[0]
                if not fresh or count >= self.s.queue_capacity:
                    return 0
                job_id = self.vault.digest("patrol", sub_id, [v.bvid for v in fresh])
                payload = {"subscription": sub_id, "videos": [v.model_dump() for v in fresh]}
                await self.db.execute(
                    "INSERT OR IGNORE INTO jobs VALUES (?,?,?,?,?,'pending',?,?)",
                    (
                        job_id,
                        sub_id,
                        sub["subject"],
                        "patrol",
                        self.vault.seal(job_id, payload),
                        now,
                        now + 86400,
                    ),
                )
            for video in fresh:
                await self.db.execute(
                    "INSERT OR IGNORE INTO seen VALUES (?,?,?)", (sub_id, video.bvid, now)
                )
            await self.db.execute("UPDATE subscriptions SET initialized=1 WHERE id=?", (sub_id,))
            return len(fresh) if sub["initialized"] else 0

    async def save_report(self, subject: str, text: str) -> None:
        async with self.transaction():
            await self.db.execute(
                "INSERT INTO reports(subject,payload,created) VALUES (?,?,?)",
                (subject, self.vault.seal(subject, text), time.time()),
            )
            await self.db.execute(
                "DELETE FROM reports WHERE subject=? AND id NOT IN "
                "(SELECT id FROM reports WHERE subject=? ORDER BY id DESC LIMIT 10)",
                (subject, subject),
            )

    async def latest_reports(self, subject: str) -> list[str]:
        async with self.lock:
            rows = await self.all(
                "SELECT payload FROM reports WHERE subject=? AND created>? "
                "ORDER BY id DESC LIMIT 3",
                (subject, time.time() - 604800),
            )
            return [self.vault.open(subject, row[0]) for row in rows]

    async def prune(self) -> None:
        now = time.time()
        async with self.transaction():
            await self.groups.prune_in_transaction(now)
            await self.db.execute("DELETE FROM bot_turns WHERE expires<?", (now,))
            await self.db.execute("DELETE FROM bot_authorized_events WHERE expires<?", (now,))
            await self.db.execute("DELETE FROM bot_dialogue_windows WHERE expires<?", (now,))
            await self.db.execute(
                "DELETE FROM bot_interactions WHERE permit_expires<? AND cooldown_until<?",
                (now, now),
            )
            await self.db.execute("DELETE FROM memory WHERE expires<?", (now,))
            await self.db.execute("DELETE FROM counters WHERE expires<?", (now,))
            await self.db.execute(
                "DELETE FROM jobs WHERE created<? AND status != 'working'", (now - 86400,)
            )
            await self.db.execute("DELETE FROM outbox WHERE created<?", (now - 86400,))
            await self.db.execute("DELETE FROM reports WHERE created<?", (now - 604800,))
            # Per-UP bounded history; avoid indefinitely accumulating public IDs.
            for row in await self.all("SELECT id FROM subscriptions"):
                await self.db.execute(
                    "DELETE FROM seen WHERE subscription=? AND bvid NOT IN "
                    "(SELECT bvid FROM seen WHERE subscription=? "
                    "ORDER BY created DESC LIMIT 500)",
                    (row[0], row[0]),
                )
            cutoff = datetime.fromtimestamp(now - 32 * 86400, UTC).date().isoformat()
            await self.db.execute("DELETE FROM ledger WHERE day<?", (cutoff,))
            await self.db.execute(
                "DELETE FROM ledger_usage WHERE id NOT IN (SELECT id FROM ledger)"
            )
            await self.db.execute(
                "DELETE FROM ledger_times WHERE id NOT IN (SELECT id FROM ledger)"
            )
