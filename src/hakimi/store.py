import asyncio
import fcntl
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any
from zoneinfo import ZoneInfo

import aiosqlite
import anyio

from . import model_accounting
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
from .thinking_policy import INTERACTION_LANES, PRIMARY_MODELS
from .usage_accounting import TokenUsage
from .work_usage import reservation_work

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, scope TEXT NOT NULL, subject TEXT NOT NULL, kind TEXT NOT NULL,
  payload BLOB, status TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, created);
CREATE INDEX IF NOT EXISTS jobs_scope_status ON jobs(scope, status);
CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created);
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
CREATE TABLE IF NOT EXISTS owner_digest_review_holds (
  id TEXT PRIMARY KEY, day TEXT NOT NULL, subject TEXT NOT NULL, amount INTEGER NOT NULL
);
CREATE VIEW IF NOT EXISTS budget_commitments AS
  SELECT day,subject,lane,amount FROM ledger UNION ALL
  SELECT day,subject,'proactive',amount FROM owner_digest_review_holds;
CREATE TABLE IF NOT EXISTS ledger_usage (
  id TEXT PRIMARY KEY, input_tokens INTEGER, output_tokens INTEGER,
  cached_tokens INTEGER, thought_tokens INTEGER, model_lane TEXT,
  estimated_amount INTEGER
);
CREATE TABLE IF NOT EXISTS ledger_work (
  id TEXT PRIMARY KEY, kind TEXT NOT NULL, work_id TEXT NOT NULL,
  root_kind TEXT NOT NULL, root_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ledger_work_action ON ledger_work(kind, work_id);
CREATE INDEX IF NOT EXISTS ledger_work_root ON ledger_work(root_kind, root_id);
CREATE TABLE IF NOT EXISTS ledger_provider (
  id TEXT PRIMARY KEY, provider TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_usage_aggregates (
  period TEXT NOT NULL, provider TEXT NOT NULL, calls INTEGER NOT NULL DEFAULT 0,
  charged_amount INTEGER NOT NULL DEFAULT 0, estimated_amount INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
  cached_tokens INTEGER NOT NULL DEFAULT 0, thought_tokens INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(period, provider)
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
CREATE INDEX IF NOT EXISTS outbox_status_created ON outbox(status, created);
CREATE INDEX IF NOT EXISTS outbox_status_expires ON outbox(status, expires);
CREATE INDEX IF NOT EXISTS outbox_created ON outbox(created);
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
  id INTEGER PRIMARY KEY, subject TEXT NOT NULL, payload BLOB NOT NULL, created REAL NOT NULL,
  public INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS group_policies (
  scope TEXT PRIMARY KEY, mode TEXT NOT NULL CHECK(mode IN ('at_only','smart_reply','muted'))
);
"""


VALID_PROVIDERS = frozenset({"gemini_paid", "gemini_free", "vertex_paid"})


def _resolve_provider(provider: str | None) -> str:
    if provider is None:
        return "gemini_paid"
    if provider not in VALID_PROVIDERS:
        raise Rejected("invalid_provider")
    return provider


def microdollars(value: Decimal) -> int:
    return int((value * 1_000_000).to_integral_value(rounding=ROUND_CEILING))


def _classify_usage_category(lane: str, work_kind: str, subject: str) -> str:
    kind = (work_kind or "").strip()
    if kind.startswith("embedding."):
        return "embedding"
    if lane == "chat":
        if kind.startswith("voice.companion"):
            return "voice_companion"
        return "chat"
    if lane == "proactive":
        if kind.startswith("world.plan"):
            return "world_plan"
        if (
            kind.startswith("mc.")
            or kind.startswith("plugin.minecraft")
            or subject == "plugin:minecraft"
        ):
            return "minecraft"
        if kind.startswith("qq.speak") or kind.endswith(".speak"):
            return "qq_proactive"
        if kind.startswith(("qq.owner_digest", "social.owner_digest")):
            return "owner_digest"
        if kind.startswith("screen.watch"):
            return "screen_watch"
        if kind.startswith("voice.companion"):
            return "voice_companion"
        return "proactive_other"
    if lane == "patrol":
        if kind.startswith("diary."):
            return "patrol_diary"
        return "patrol"
    if lane in ("memory", "memory_flex", "admin"):
        return lane
    return lane


@dataclass(frozen=True)
class DigestReviewCredit:
    """One local authorization for a daily digest's mandatory Decisions output review."""

    id: str
    usd: Decimal


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
        self.job_wakeup = asyncio.Event()
        self.reply_wakeup = asyncio.Event()

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
            if "diagnostics" not in {
                r["name"] for r in await self.all("PRAGMA table_info(audit_events)")
            }:
                await self.db.execute("ALTER TABLE audit_events ADD COLUMN diagnostics TEXT")
            if "public" not in {r["name"] for r in await self.all("PRAGMA table_info(reports)")}:
                await self.db.execute(
                    "ALTER TABLE reports ADD COLUMN public INTEGER NOT NULL DEFAULT 0"
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
            # Only unsubmitted output checks have holds. An actual review atomically
            # converts its hold to a normal reservation and follows normal crash accounting.
            for hold in await self.all("SELECT * FROM owner_digest_review_holds"):
                await self._release_digest_review_tx(hold)
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
        await model_accounting.initialize(self)
        await self._reconcile_bill_aligned_v1()
        await model_accounting.initialize_thinking(self)
        await self.runtime.load_config()
        await self._load_budget_config()
        await self.groups.initialize()
        # Only new, explicitly attributed reservations can be recovered by model.
        # Historical unattributed ledger rows and their amounts remain untouched.
        pending = await self.all(
            "SELECT l.id FROM ledger l JOIN ledger_attribution a ON a.id=l.id "
            "WHERE l.status='reserved'"
        )
        for row in pending:
            await self.settle(row[0], None)
        await self._reconcile_search_unbilled_v1()

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

    def _reconciled_unknown_microdollars(
        self, lane: str, model_lane: str | None, reserved: int
    ) -> int:
        if reserved <= 0:
            return 0
        if model_lane in {"hydrogen", "interaction", "chat"}:
            in_rate, out_rate = self.s.chat_input_usd_per_m, self.s.chat_output_usd_per_m
            if lane == "search":
                fixed_ceiling = microdollars(
                    (Decimal(16000) * in_rate + Decimal(4096) * out_rate) / 1_000_000
                    + Decimal("0.14")
                )
                text_part = max(0, reserved - fixed_ceiling) // 3
                est = text_part + microdollars(
                    (Decimal(2048) * in_rate + Decimal(256) * out_rate) / 1_000_000
                    + Decimal("0.014")
                )
                return max(1, min(reserved, est))
        elif model_lane == "memory_flex":
            out_rate = self.s.chat_output_usd_per_m / 2
        elif model_lane == "carbon":
            out_rate = self.s.carbon_output_usd_per_m
        else:
            return reserved
        cap_micro = 0
        for cap in (16384, 8192, 4096):
            candidate = microdollars(Decimal(cap) * out_rate / 1_000_000)
            if reserved > candidate + 1000:
                cap_micro = candidate
                break
        input_part = max(0, reserved - cap_micro) // 3
        out_part = microdollars(Decimal(256) * out_rate / 1_000_000)
        return max(1, min(reserved, input_part + out_part))

    async def _reconcile_bill_aligned_v1(self) -> None:
        async with self.transaction():
            if await self.one("SELECT 1 FROM meta WHERE key='bill_aligned_v1'"):
                return
            rows = await self.all(
                "SELECT l.id,l.day,l.lane,l.reserved,l.amount,l.status,"
                "u.id AS uid,u.input_tokens,u.estimated_amount,"
                "COALESCE(t.model_lane,u.model_lane,a.model_lane) AS model_lane,"
                "t.thinking_level AS thinking_level "
                "FROM ledger l "
                "LEFT JOIN ledger_usage u ON u.id=l.id "
                "LEFT JOIN ledger_attribution a ON a.id=l.id "
                "LEFT JOIN ledger_thinking t ON t.id=l.id "
                "WHERE l.status<>'reserved'"
            )
            for row in rows:
                status = row["status"]
                amount = row["amount"]
                reserved = row["reserved"]
                est = row["estimated_amount"]
                if status == "settled":
                    if est is not None and 0 <= est <= amount:
                        new_amount, new_est = est, est
                    elif est is None and (amount < reserved or row["input_tokens"] is not None):
                        new_amount, new_est = amount, amount
                    else:
                        new_amount, new_est = amount, est
                elif status == "unknown" and amount == reserved > 0:
                    new_amount = self._reconciled_unknown_microdollars(
                        row["lane"], row["model_lane"], reserved
                    )
                    new_est = est
                else:
                    new_amount, new_est = amount, est
                d_amount = new_amount - amount
                d_est = (new_est or 0) - (est or 0)
                d_est_calls = int(new_est is not None) - int(est is not None)
                if not (d_amount or d_est or d_est_calls):
                    continue
                if d_amount:
                    await self.db.execute(
                        "UPDATE ledger SET amount=? WHERE id=?", (new_amount, row["id"])
                    )
                    await self.db.execute(
                        "UPDATE budget_totals SET amount=MAX(0,amount+?)", (d_amount,)
                    )
                if d_est or d_est_calls:
                    if row["uid"] is not None:
                        await self.db.execute(
                            "UPDATE ledger_usage SET estimated_amount=? WHERE id=?",
                            (new_est, row["id"]),
                        )
                    elif new_est is not None:
                        await self.db.execute(
                            "INSERT INTO ledger_usage VALUES (?,NULL,NULL,NULL,NULL,?,?)",
                            (row["id"], row["model_lane"], new_est),
                        )
                lane_key = model_accounting.lane_name(row["model_lane"])
                thinking_key = model_accounting.thinking_name(row["thinking_level"], lane_key)
                for period in self._periods(row["day"]):
                    await self.db.execute(
                        "UPDATE usage_aggregates SET charged_amount=MAX(0,charged_amount+?),"
                        "estimated_amount=MAX(0,estimated_amount+?),"
                        "estimated_calls=MAX(0,estimated_calls+?) WHERE period=?",
                        (d_amount, d_est, d_est_calls, period),
                    )
                    await self.db.execute(
                        "UPDATE model_usage_aggregates SET charged_amount=MAX(0,charged_amount+?),"
                        "estimated_amount=MAX(0,estimated_amount+?),"
                        "estimated_calls=MAX(0,estimated_calls+?) "
                        "WHERE period=? AND model_lane=?",
                        (d_amount, d_est, d_est_calls, period, lane_key),
                    )
                    await self.db.execute(
                        "UPDATE thinking_usage_aggregates "
                        "SET charged_amount=MAX(0,charged_amount+?),"
                        "estimated_amount=MAX(0,estimated_amount+?),"
                        "estimated_calls=MAX(0,estimated_calls+?) "
                        "WHERE period=? AND model_lane=? AND thinking_level=?",
                        (d_amount, d_est, d_est_calls, period, lane_key, thinking_key),
                    )
            await self.db.execute("INSERT INTO meta VALUES ('bill_aligned_v1','1')")

    async def _reconcile_search_unbilled_v1(self) -> None:
        async with self.transaction():
            if await self.one("SELECT 1 FROM meta WHERE key='search_unbilled_v1'"):
                return
            rows = await self.all(
                "SELECT l.id,l.day,l.reserved,l.amount,u.estimated_amount,"
                "COALESCE(t.model_lane,u.model_lane,a.model_lane) AS model_lane,"
                "t.thinking_level AS thinking_level "
                "FROM ledger l "
                "LEFT JOIN ledger_usage u ON u.id=l.id "
                "LEFT JOIN ledger_attribution a ON a.id=l.id "
                "LEFT JOIN ledger_thinking t ON t.id=l.id "
                "WHERE l.lane='search'"
            )
            for row in rows:
                d_amount = -int(row["amount"] or 0)
                est = row["estimated_amount"]
                d_est = -int(est) if est is not None else 0
                if d_amount:
                    await self.db.execute(
                        "UPDATE budget_totals SET amount=MAX(0,amount+?)", (d_amount,)
                    )
                if row["reserved"] or row["amount"]:
                    await self.db.execute(
                        "UPDATE ledger SET amount=0,reserved=0 WHERE id=?", (row["id"],)
                    )
                if est:
                    await self.db.execute(
                        "UPDATE ledger_usage SET estimated_amount=0 WHERE id=?", (row["id"],)
                    )
                if d_amount or d_est:
                    lane_key = model_accounting.lane_name(row["model_lane"])
                    thinking_key = model_accounting.thinking_name(row["thinking_level"], lane_key)
                    for period in self._periods(row["day"]):
                        await self.db.execute(
                            "UPDATE usage_aggregates SET charged_amount=MAX(0,charged_amount+?),"
                            "estimated_amount=MAX(0,estimated_amount+?) WHERE period=?",
                            (d_amount, d_est, period),
                        )
                        await self.db.execute(
                            "UPDATE model_usage_aggregates "
                            "SET charged_amount=MAX(0,charged_amount+?),"
                            "estimated_amount=MAX(0,estimated_amount+?) "
                            "WHERE period=? AND model_lane=?",
                            (d_amount, d_est, period, lane_key),
                        )
                        await self.db.execute(
                            "UPDATE thinking_usage_aggregates "
                            "SET charged_amount=MAX(0,charged_amount+?),"
                            "estimated_amount=MAX(0,estimated_amount+?) "
                            "WHERE period=? AND model_lane=? AND thinking_level=?",
                            (d_amount, d_est, period, lane_key, thinking_key),
                        )
            await self.db.execute("INSERT INTO meta VALUES ('search_unbilled_v1','1')")

    async def _load_budget_config(self):
        async with self.lock:
            row = await self.one("SELECT payload FROM admin_config WHERE key='budget'")
        if row:
            saved = self.vault.open("admin:budget", row[0])
            merged = (
                {**self.env_budget.model_dump(mode="json"), **saved}
                if isinstance(saved, dict)
                else saved
            )
            self._apply_budget(BudgetConfig.model_validate(merged))
            self.budget_overridden = True

    def _apply_budget(self, value):
        # Update all fields synchronously without await so readers never see a torn snapshot.
        for name in BudgetConfig.model_fields:
            setattr(self.s, name, getattr(value, name))

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

    async def _read_manual_balance_unlocked(self) -> dict | None:
        row = await self.one("SELECT payload FROM admin_config WHERE key='account_balance'")
        if not row:
            return None
        return self.vault.open("admin:account_balance", row[0])

    @staticmethod
    def _gcp_hard_stop_exhausted(gcp: Any, manual_set_micro: int | None) -> bool:
        if gcp is None or not callable(getattr(gcp, "hard_stop_exhausted", None)):
            return False
        try:
            return bool(gcp.hard_stop_exhausted(manual_set_micro=manual_set_micro))
        except TypeError:
            return bool(gcp.hard_stop_exhausted())

    async def _account_balance_unlocked(self) -> dict:
        total = (await self.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
        data = await self._read_manual_balance_unlocked()
        manual_configured = data is not None
        set_micro: int | None = None
        spent_since = 0
        manual_remaining_micro: int | None = None
        updated_at = None
        if data is not None:
            set_micro = int(data["set_microdollars"])
            base_total = int(data["baseline_total_microdollars"])
            spent_since = max(0, total - base_total)
            manual_remaining_micro = max(0, set_micro - spent_since)
            updated_at = data.get("updated_at")

        gcp = getattr(self.runtime, "gcp_billing", None)
        if gcp is not None:
            sync_local = getattr(gcp, "sync_local_spend_unlocked", None)
            if callable(sync_local):
                await sync_local(self._paid_spend_since_unlocked)
            if hasattr(gcp, "set_manual_balance_microdollars"):
                gcp.set_manual_balance_microdollars(set_micro)

        gcp_details = (
            gcp.bigquery_balance_details()
            if gcp is not None and hasattr(gcp, "bigquery_balance_details")
            else {"synced": False}
        )
        gcp_synced = bool(gcp_details.get("synced"))
        gcp_signed_micro = (
            int(gcp_details["signed_microdollars"])
            if gcp_synced and gcp_details.get("signed_microdollars") is not None
            else None
        )

        if (
            manual_configured
            and gcp_synced
            and set_micro is not None
            and gcp_signed_micro is not None
        ):
            mode = "merged"
            remaining_micro: int | None = max(0, gcp_signed_micro + set_micro)
        elif gcp_synced and gcp_signed_micro is not None:
            mode = "gcp"
            remaining_micro = max(0, gcp_signed_micro)
        elif manual_configured:
            mode = "manual"
            remaining_micro = manual_remaining_micro
        else:
            mode = "none"
            remaining_micro = None

        exhausted = self._gcp_hard_stop_exhausted(gcp, set_micro) if gcp_synced else False

        return {
            "configured": mode != "none",
            "mode": mode,
            "remaining_microdollars": remaining_micro,
            "remaining_usd": (
                f"{Decimal(remaining_micro) / Decimal(1_000_000):.4f}"
                if remaining_micro is not None
                else None
            ),
            "set_microdollars": set_micro,
            "set_usd": (
                f"{Decimal(set_micro) / Decimal(1_000_000):.2f}" if set_micro is not None else None
            ),
            "spent_since_set_microdollars": spent_since,
            "updated_at": updated_at,
            "manual_configured": manual_configured,
            "manual_remaining_usd": (
                f"{Decimal(manual_remaining_micro) / Decimal(1_000_000):.4f}"
                if manual_remaining_micro is not None
                else None
            ),
            "gcp_synced": gcp_synced,
            "gcp_settled_remaining_usd": (
                gcp_details.get("settled_remaining_usd") if gcp_synced else None
            ),
            "gcp_unsettled_local_usd": (
                gcp_details.get("unsettled_local_usd") if gcp_synced else None
            ),
            "gcp_realtime_remaining_usd": (
                gcp_details.get("realtime_remaining_usd") if gcp_synced else None
            ),
            "exhausted": exhausted,
        }

    async def account_balance_status(self) -> dict:
        async with self.lock:
            return await self._account_balance_unlocked()

    async def set_account_balance(self, balance_usd: Decimal) -> dict:
        if (
            not isinstance(balance_usd, Decimal)
            or not balance_usd.is_finite()
            or not Decimal(0) <= balance_usd <= Decimal("100000")
        ):
            raise Rejected("invalid_budget_amount")
        async with self.transaction():
            total = (await self.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
            payload = {
                "set_microdollars": microdollars(balance_usd),
                "baseline_total_microdollars": int(total),
                "updated_at": time.time(),
            }
            await self.db.execute(
                "INSERT INTO admin_config VALUES ('account_balance',?) ON CONFLICT(key) "
                "DO UPDATE SET payload=excluded.payload",
                (self.vault.seal("admin:account_balance", payload),),
            )
            return await self._account_balance_unlocked()

    async def clear_account_balance(self) -> dict:
        async with self.transaction():
            await self.db.execute("DELETE FROM admin_config WHERE key='account_balance'")
            return await self._account_balance_unlocked()

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
            if self.bot_guard.known_bot(message) and self.bot_guard.hard_blocked(message):
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
        self.job_wakeup.set()
        return "accepted"

    async def _next_job_delay(self, *, max_wait: float = 10.0) -> float:
        now = time.time()
        async with self.lock:
            if not await self.one("SELECT 1 FROM jobs WHERE status='pending' LIMIT 1"):
                return max_wait
            deadlines: list[float] = []
            debounce_row = await self.one(
                "SELECT MIN(b.ready_at) FROM group_debounce_batches b "
                "JOIN group_debounce_items i ON i.batch_id=b.id "
                "JOIN jobs j ON j.id=i.job_id "
                "WHERE j.status='pending' AND b.ready_at>?",
                (now,),
            )
            if debounce_row and debounce_row[0] is not None:
                deadlines.append(float(debounce_row[0]))
            outbox_row = await self.one(
                "SELECT MIN(o.expires) FROM outbox o JOIN jobs prev ON prev.id=o.id "
                "WHERE o.status IN ('pending','sending') AND o.expires>=? "
                "AND EXISTS (SELECT 1 FROM jobs j WHERE j.scope=prev.scope AND j.status='pending')",
                (now,),
            )
            if outbox_row and outbox_row[0] is not None:
                deadlines.append(float(outbox_row[0]))
            exp_row = await self.one("SELECT MIN(expires) FROM jobs WHERE status='pending'")
            if exp_row and exp_row[0] is not None:
                deadlines.append(float(exp_row[0]))
        if not deadlines:
            return max_wait
        return min(max_wait, max(0.01, min(deadlines) - now))

    async def wait_for_job(self, *, max_wait: float = 10.0) -> None:
        if self.job_wakeup.is_set():
            return
        delay = await self._next_job_delay(max_wait=max_wait)
        if self.job_wakeup.is_set():
            return
        try:
            await asyncio.wait_for(self.job_wakeup.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def wait_for_reply(self, *, max_wait: float = 10.0) -> None:
        if self.reply_wakeup.is_set():
            return
        try:
            await asyncio.wait_for(self.reply_wakeup.wait(), timeout=max_wait)
        except TimeoutError:
            pass

    async def claim(self) -> dict[str, Any] | None:
        now = time.time()
        async with self.lock:
            if not await self.one("SELECT 1 FROM jobs WHERE status='pending' LIMIT 1"):
                return None
            has_expired = await self.one(
                "SELECT 1 FROM jobs WHERE status='pending' AND expires<? LIMIT 1",
                (now,),
            )
            if not has_expired:
                candidate = await self.one(
                    "SELECT 1 FROM jobs j WHERE status='pending' AND NOT EXISTS "
                    "(SELECT 1 FROM jobs active WHERE active.scope=j.scope "
                    "AND active.status='working') "
                    "AND NOT EXISTS (SELECT 1 FROM outbox o JOIN jobs prev ON prev.id=o.id "
                    "WHERE prev.scope=j.scope AND o.status IN ('pending','sending') "
                    "AND o.expires>=?) "
                    "AND NOT EXISTS (SELECT 1 FROM group_debounce_items i "
                    "JOIN group_debounce_batches b ON b.id=i.batch_id "
                    "WHERE i.job_id=j.id AND b.ready_at>?) "
                    "ORDER BY created, rowid LIMIT 1",
                    (now, now),
                )
                if not candidate:
                    return None
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
                "AND NOT EXISTS (SELECT 1 FROM outbox o JOIN jobs prev ON prev.id=o.id "
                "WHERE prev.scope=j.scope AND o.status IN ('pending','sending') AND o.expires>=?) "
                "AND NOT EXISTS (SELECT 1 FROM group_debounce_items i "
                "JOIN group_debounce_batches b ON b.id=i.batch_id "
                "WHERE i.job_id=j.id AND b.ready_at>?) "
                "ORDER BY created, rowid LIMIT 1",
                (now, now),
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
        self.job_wakeup.set()

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

    async def _paid_spend_since_unlocked(self, since_ts: float) -> int:
        row = await self.one(
            "SELECT COALESCE(SUM(l.amount),0) FROM ledger l "
            "JOIN ledger_times t ON t.id=l.id "
            "WHERE l.lane<>'search' AND l.status<>'reserved' AND t.created>?",
            (float(since_ts or 0),),
        )
        return int(row[0] or 0) if row else 0

    async def paid_spend_since(self, since_ts: float) -> int:
        async with self.lock:
            return await self._paid_spend_since_unlocked(since_ts)

    async def _budget_refusal(self, day: str, subject: str, lane: str, amount: int) -> str:
        if await self.one("SELECT 1 FROM meta WHERE key='budget_overrun'"):
            return "circuit"
        if lane == "search":
            return ""
        gcp = getattr(self.runtime, "gcp_billing", None)
        if amount > 0 and gcp is not None:
            sync_local = getattr(gcp, "sync_local_spend_unlocked", None)
            if callable(sync_local):
                await sync_local(self._paid_spend_since_unlocked)
            manual_data = await self._read_manual_balance_unlocked()
            manual_set_micro = (
                int(manual_data["set_microdollars"]) if manual_data is not None else None
            )
            if hasattr(gcp, "set_manual_balance_microdollars"):
                gcp.set_manual_balance_microdollars(manual_set_micro)
            if self._gcp_hard_stop_exhausted(gcp, manual_set_micro):
                return "gcp_credit_exhausted"
        month = await self.one(
            "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("month:" + day[:7],)
        )
        monthly_total = (month[0] if month else 0) + await self._calendar_carry("month:" + day[:7])
        monthly_limit = microdollars(getattr(self.s, "monthly_usd", Decimal("40")))
        daily_row = await self.one(
            "SELECT charged_amount FROM usage_aggregates WHERE period=?", ("day:" + day,)
        )
        global_total = (daily_row[0] if daily_row else 0) + await self._calendar_carry("day:" + day)
        personal = (
            await self.one(
                "SELECT COALESCE(SUM(amount),0) FROM budget_commitments WHERE day=? AND subject=?",
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
                "SELECT COALESCE(SUM(amount),0) FROM budget_commitments WHERE day=? AND lane=?",
                (day, lane),
            )
        )[0]
        extra_limit = {
            "memory": self.s.memory_daily_usd,
            "admin": self.s.admin_daily_usd,
            "proactive": self.s.proactive_daily_usd,
        }.get(lane)
        if self.s.daily_usd <= 0 or global_total + amount > microdollars(self.s.daily_usd):
            return "daily"
        if monthly_limit <= 0 or monthly_total + amount > monthly_limit:
            return "monthly"
        aff = getattr(self.runtime, "affection", None)
        if aff is not None and hasattr(aff, "budget_multiplier_for_subject"):
            raw_mult = aff.budget_multiplier_for_subject(subject)
            user_mult = max(
                Decimal("0"),
                raw_mult if isinstance(raw_mult, Decimal) else Decimal(str(raw_mult)),
            )
        else:
            user_mult = Decimal("1")
        if personal + amount > microdollars(self.s.user_daily_usd * user_mult):
            return "user_daily"
        if lane == "patrol" and patrol + amount > microdollars(self.s.patrol_daily_usd):
            return "patrol_daily"
        if extra_limit is not None and lane_total + amount > microdollars(extra_limit):
            return f"{lane}_daily"
        return ""

    async def can_reserve(self, subject: str, lane: str, usd: Decimal) -> bool:
        if not isinstance(usd, Decimal) or not usd.is_finite() or usd < 0:
            return False
        day = self.local_now().date().isoformat()
        amount = microdollars(usd)
        async with self.lock:
            return not await self._budget_refusal(day, subject, lane, amount)

    async def _release_digest_review_tx(self, hold):
        await self.db.execute("DELETE FROM owner_digest_review_holds WHERE id=?", (hold["id"],))
        await self.db.execute("UPDATE budget_totals SET amount=amount-?", (hold["amount"],))
        await self._charge_aggregates(hold["day"], -hold["amount"])

    async def release_digest_review(self, credit: DigestReviewCredit):
        async with self.transaction():
            hold = await self.one(
                "SELECT * FROM owner_digest_review_holds WHERE id=?", (credit.id,)
            )
            if hold:
                await self._release_digest_review_tx(hold)

    async def reserve(
        self,
        subject: str,
        lane: str,
        usd: Decimal,
        *,
        model_lane: str | None = None,
        thinking_level: str | None = None,
        provider: str | None = None,
        digest_review: DigestReviewCredit | None = None,
    ) -> str:
        if not isinstance(usd, Decimal) or not usd.is_finite() or usd < 0:
            raise Rejected("invalid_budget_amount")
        resolved_provider = _resolve_provider(provider)
        now = self.local_now()
        day = now.date().isoformat()
        amount = 0 if lane == "search" else microdollars(usd)
        reservation = uuid.uuid4().hex
        async with self.transaction():
            hold = None
            extra = 0
            if digest_review is not None:
                if (
                    lane != "proactive"
                    or (model_lane not in PRIMARY_MODELS and model_lane != "decision")
                    or not digest_review.usd.is_finite()
                    or digest_review.usd < 0
                ):
                    raise Rejected("invalid_digest_review")
                hold = await self.one(
                    "SELECT * FROM owner_digest_review_holds WHERE id=?", (digest_review.id,)
                )
                if hold and (hold["day"] != day or hold["subject"] != subject):
                    raise Rejected("invalid_digest_review")
                if model_lane == "decision":
                    if resolved_provider not in {"gemini_paid", "gemini_free"}:
                        raise Rejected("invalid_digest_review")
                    if not hold or amount > hold["amount"]:
                        raise Rejected("digest_review_unfunded")
                    # A free first attempt may fall back; only the paid attempt spends the hold.
                    if resolved_provider != "gemini_free":
                        await self._release_digest_review_tx(hold)
                elif hold is None:
                    extra = microdollars(digest_review.usd)
            if not (digest_review is not None and model_lane == "decision"):
                budget_detail = await self._budget_refusal(day, subject, lane, amount + extra)
                if budget_detail == "circuit":
                    raise Rejected("budget_circuit_open")
                if budget_detail:
                    raise Rejected("budget_exhausted", budget_detail)
            if digest_review is not None and model_lane != "decision" and hold is None:
                await self.db.execute(
                    "INSERT INTO owner_digest_review_holds VALUES (?,?,?,?)",
                    (digest_review.id, day, subject, extra),
                )
                await self.db.execute("UPDATE budget_totals SET amount=amount+?", (extra,))
                await self._charge_aggregates(day, extra)
            await self.db.execute(
                "INSERT INTO ledger VALUES (?,?,?,?,?,?,'reserved')",
                (reservation, day, subject, lane, amount, amount),
            )
            work = reservation_work(lane, reservation)
            await self.db.execute(
                "INSERT INTO ledger_work VALUES (?,?,?,?,?)",
                (reservation, work.kind, work.key, work.root_kind, work.root_key),
            )
            await self.db.execute(
                "INSERT INTO ledger_times VALUES (?,?)", (reservation, now.timestamp())
            )
            await self.db.execute(
                "INSERT OR REPLACE INTO ledger_provider VALUES (?,?)",
                (reservation, resolved_provider),
            )
            if model_lane is not None:
                await self.db.execute(
                    "INSERT INTO ledger_attribution VALUES (?,?)",
                    (reservation, model_accounting.lane_name(model_lane)),
                )
            if model_lane is not None or thinking_level is not None:
                lane_key = model_accounting.lane_name(model_lane)
                await self.db.execute(
                    "INSERT OR REPLACE INTO ledger_thinking VALUES (?,?,?)",
                    (
                        reservation,
                        lane_key,
                        model_accounting.thinking_name(thinking_level, lane_key),
                    ),
                )
            await self.db.execute("UPDATE budget_totals SET amount=amount+?", (amount,))
            await self._charge_aggregates(day, amount, 1)
        return reservation

    async def _accumulate_provider_usage(
        self,
        day: str,
        provider: str,
        amount: int,
        estimated: int | None,
        usage: TokenUsage | None,
    ) -> None:
        in_tok = usage.input_tokens if usage else 0
        out_tok = usage.output_tokens if usage else 0
        cached_tok = (usage.cached_tokens or 0) if usage else 0
        thought_tok = (usage.thought_tokens or 0) if usage else 0
        est_amt = estimated or 0
        for period in self._periods(day):
            await self.db.execute(
                "INSERT INTO provider_usage_aggregates VALUES (?,?,1,?,?,?,?,?,?) "
                "ON CONFLICT(period,provider) DO UPDATE SET calls=calls+1,"
                "charged_amount=charged_amount+excluded.charged_amount,"
                "estimated_amount=estimated_amount+excluded.estimated_amount,"
                "input_tokens=input_tokens+excluded.input_tokens,"
                "output_tokens=output_tokens+excluded.output_tokens,"
                "cached_tokens=cached_tokens+excluded.cached_tokens,"
                "thought_tokens=thought_tokens+excluded.thought_tokens",
                (period, provider, amount, est_amt, in_tok, out_tok, cached_tok, thought_tok),
            )

    async def settle(
        self,
        reservation: str,
        actual: Decimal | None,
        *,
        usage: TokenUsage | None = None,
        discounted_actual: Decimal | None = None,
        fallback_actual: Decimal | None = None,
        model_lane: str | None = None,
        thinking_level: str | None = None,
        provider: str | None = None,
    ) -> None:
        # Unknown/ambiguous failures settle via bounded fallback (or keep reserve on crash).
        if actual is not None and (
            not isinstance(actual, Decimal) or not actual.is_finite() or actual < 0
        ):
            raise Rejected("invalid_budget_amount")
        if provider is not None and provider not in VALID_PROVIDERS:
            raise Rejected("invalid_provider")
        if not isinstance(usage, TokenUsage):
            usage = None
        if (
            not isinstance(discounted_actual, Decimal)
            or not discounted_actual.is_finite()
            or discounted_actual < 0
            or actual is None
            or discounted_actual > actual
        ):
            discounted_actual = None
        if (
            not isinstance(fallback_actual, Decimal)
            or not fallback_actual.is_finite()
            or fallback_actual < 0
        ):
            fallback_actual = None
        overrun = False
        async with self.transaction():
            row = await self.one("SELECT * FROM ledger WHERE id=?", (reservation,))
            if row is None or row["status"] != "reserved":
                return
            attribution = await self.one(
                "SELECT model_lane FROM ledger_attribution WHERE id=?", (reservation,)
            )
            thinking_row = await self.one(
                "SELECT model_lane,thinking_level FROM ledger_thinking WHERE id=?",
                (reservation,),
            )
            provider_row = await self.one(
                "SELECT provider FROM ledger_provider WHERE id=?", (reservation,)
            )
            resolved_provider = (
                provider
                or (
                    provider_row[0] if provider_row and provider_row[0] in VALID_PROVIDERS else None
                )
                or "gemini_paid"
            )
            model_lane = (
                model_lane
                or (usage.model_lane if usage else None)
                or (attribution[0] if attribution else None)
                or (thinking_row[0] if thinking_row else None)
            )
            resolved_thinking = model_accounting.thinking_name(
                thinking_level
                or (usage.thinking_level if usage else None)
                or (thinking_row[1] if thinking_row else None),
                model_lane,
            )
            if row["lane"] == "search":
                amount = 0
                estimated = 0 if actual is not None else None
                status = "settled" if actual is not None else "unknown"
                delta_amount = -int(row["amount"] or 0)
            elif actual is not None:
                billable = discounted_actual if discounted_actual is not None else actual
                amount = microdollars(billable)
                estimated = amount
                status = "settled"
                delta_amount = amount - row["amount"]
            else:
                amount = (
                    row["amount"]
                    if fallback_actual is None
                    else min(row["reserved"], microdollars(fallback_actual))
                )
                estimated = None
                status = "unknown"
                delta_amount = amount - row["amount"]
            await self.db.execute(
                "UPDATE budget_totals SET amount=MAX(0,amount+?)", (delta_amount,)
            )
            await self._charge_aggregates(row["day"], delta_amount)
            overrun = False if row["lane"] == "search" else amount > row["reserved"]
            await self.db.execute(
                "UPDATE ledger SET amount=?,status=? WHERE id=?",
                (amount, status, reservation),
            )
            await self.db.execute(
                "INSERT INTO ledger_usage VALUES (?,?,?,?,?,?,?)",
                (
                    reservation,
                    usage.input_tokens if usage else None,
                    usage.output_tokens if usage else None,
                    usage.cached_tokens if usage else None,
                    usage.thought_tokens if usage else None,
                    model_lane,
                    estimated,
                ),
            )
            interaction = bool(usage and usage.model_lane in INTERACTION_LANES)
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
            await self.db.execute(
                "INSERT OR REPLACE INTO ledger_thinking VALUES (?,?,?)",
                (reservation, model_accounting.lane_name(model_lane), resolved_thinking),
            )
            await self.db.execute(
                "INSERT OR REPLACE INTO ledger_provider VALUES (?,?)",
                (reservation, resolved_provider),
            )
            await model_accounting.accumulate(
                self,
                row["day"],
                model_lane or (usage.model_lane if usage else None),
                amount,
                estimated,
                {
                    name: getattr(usage, name, None)
                    for name in ("input_tokens", "output_tokens", "cached_tokens", "thought_tokens")
                },
                thinking_level=resolved_thinking,
            )
            await self._accumulate_provider_usage(
                row["day"], resolved_provider, amount, estimated, usage
            )
            if overrun:
                await self.db.execute("INSERT OR REPLACE INTO meta VALUES ('budget_overrun','1')")
        if overrun:
            raise Rejected("budget_reservation_exceeded")

    async def record_cache_usage(self, lane, usage):
        day = self.local_now().date().isoformat()
        async with self.transaction():
            for period in self._periods(day):
                await self.db.execute(
                    "INSERT INTO cache_aggregates VALUES (?,?,1,?,?) "
                    "ON CONFLICT(period,model_lane) DO UPDATE SET calls=calls+1, "
                    "cached_tokens=cached_tokens+excluded.cached_tokens, "
                    "eligible_input_tokens=eligible_input_tokens+excluded.eligible_input_tokens",
                    (period, lane, usage.cached_tokens, max(1, usage.input_tokens - 2050)),
                )

    async def _usage_categories(self, day: str) -> list[dict[str, Any]]:
        rows = await self.all(
            "SELECT l.lane AS lane,"
            " COALESCE(NULLIF(w.root_kind,''), NULLIF(w.kind,''), '') AS work_kind,"
            " l.subject AS subject,"
            " SUM(l.amount) AS amount,"
            " COUNT(*) AS calls "
            "FROM ledger l LEFT JOIN ledger_work w ON w.id=l.id "
            "WHERE l.day=? AND l.lane<>'search' "
            "GROUP BY l.lane, COALESCE(NULLIF(w.root_kind,''), NULLIF(w.kind,''), ''), l.subject",
            (day,),
        )
        buckets: dict[str, dict[str, Any]] = {}
        lane_weights: dict[str, dict[str, list[int]]] = {}
        for row in rows:
            lane = str(row["lane"])
            work_kind = str(row["work_kind"] or "")
            subject = str(row["subject"] or "")
            amount = int(row["amount"] or 0)
            calls = int(row["calls"] or 0)
            category = _classify_usage_category(lane, work_kind, subject)
            entry = buckets.get(category)
            if entry is None:
                entry = {"category": category, "lane": lane, "amount": 0, "calls": 0}
                buckets[category] = entry
                lane_weights[category] = {}
            entry["amount"] += amount
            entry["calls"] += calls
            lw = lane_weights[category].setdefault(lane, [0, 0])
            lw[0] += amount
            lw[1] += calls
        for category, entry in buckets.items():
            weights = lane_weights[category]
            if len(weights) > 1:
                entry["lane"] = max(
                    weights.items(),
                    key=lambda item: (item[1][0], item[1][1], item[0] == "chat", item[0]),
                )[0]
        return sorted(
            buckets.values(),
            key=lambda item: (-item["amount"], -item["calls"], item["category"]),
        )

    async def usage_overview(self) -> dict:
        day = self.local_now().date().isoformat()
        async with self.lock:
            rows = await self.all(
                "SELECT lane,SUM(amount) AS amount,COUNT(*) AS calls "
                "FROM ledger WHERE day=? AND lane<>'search' GROUP BY lane",
                (day,),
            )
            categories = await self._usage_categories(day)
            statuses = await self.all("SELECT status,COUNT(*) AS n FROM jobs GROUP BY status")
        return {
            "day": day,
            "day_local": day,
            "month_local": day[:7],
            "timezone": self.budget_zone.key,
            "budget_timezone": self.budget_zone.key,
            # Deprecated alias for existing clients; the bucket is now explicitly local.
            "day_utc": day,
            "lanes": [dict(r) for r in rows],
            "categories": categories,
            "jobs": [dict(r) for r in statuses],
        }

    async def usage(self) -> dict:
        day = self.local_now().date().isoformat()
        async with self.lock:
            rows = await self.all(
                "SELECT lane,SUM(amount) AS amount,COUNT(*) AS calls "
                "FROM ledger WHERE day=? AND lane<>'search' GROUP BY lane",
                (day,),
            )
            categories = await self._usage_categories(day)
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
                cache_rows = await self.all(
                    "SELECT * FROM cache_aggregates WHERE period=?", (period,)
                )
                eligible = sum(r["eligible_input_tokens"] for r in cache_rows)
                tokens["effective_cache_hit_ratio"] = (
                    sum(r["cached_tokens"] for r in cache_rows) / eligible if eligible else None
                )
                tokens["effective_cache_calls"] = sum(r["calls"] for r in cache_rows)
                tokens["by_model"] = [
                    dict(r)
                    for r in await self.all(
                        "SELECT * FROM model_usage_aggregates WHERE period=? ORDER BY model_lane",
                        (period,),
                    )
                ]
                tokens["by_provider"] = [
                    dict(r)
                    for r in await self.all(
                        "SELECT * FROM provider_usage_aggregates WHERE period=? ORDER BY provider",
                        (period,),
                    )
                ]
                thinking_rows = await self.all(
                    "SELECT * FROM thinking_usage_aggregates WHERE period=?", (period,)
                )
                tokens["by_thinking"], tokens["thinking_summary"] = (
                    model_accounting.summarize_thinking(thinking_rows)
                )
                tokens["effective_cache_by_model"] = [
                    {
                        **dict(r),
                        "effective_cache_hit_ratio": r["cached_tokens"]
                        / max(1, r["eligible_input_tokens"]),
                    }
                    for r in cache_rows
                ]
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
            busts = await self.one(
                "SELECT COALESCE(SUM(n),0) FROM daily_metrics "
                "WHERE day=? AND code='cache_prefix_busted' AND source='production'",
                (day,),
            )
            return {
                "cache_health": {
                    "target": 0.9,
                    "tail_tokens": 2050,
                    "prefix_busts_today": busts[0],
                },
                "recent_work": [
                    dict(row)
                    for row in await self.all(
                        "SELECT COALESCE(w.kind,'legacy') AS kind,w.work_id,w.root_kind,w.root_id,"
                        "COUNT(*) AS calls,SUM(l.amount) AS charged_microdollars,"
                        "SUM(u.estimated_amount) AS estimated_microdollars,"
                        "COUNT(u.estimated_amount) AS known_calls "
                        "FROM ledger l LEFT JOIN ledger_work w ON w.id=l.id "
                        "LEFT JOIN ledger_usage u ON u.id=l.id "
                        "LEFT JOIN ledger_times t ON t.id=l.id "
                        "WHERE l.day=? GROUP BY w.kind,w.work_id,w.root_kind,w.root_id "
                        "ORDER BY MAX(t.created) DESC LIMIT 60",
                        (day,),
                    )
                ],
                "day_local": day,
                "month_local": day[:7],
                "timezone": self.budget_zone.key,
                # Deprecated alias for existing clients; the bucket is now explicitly local.
                "day_utc": day,
                "lanes": [dict(r) for r in rows],
                "categories": categories,
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
            user_top_row = await self.one(
                "SELECT COALESCE(MAX(total),0) FROM ("
                "SELECT SUM(amount) AS total FROM budget_commitments WHERE day=? GROUP BY subject"
                ")",
                (day,),
            )
            lane_rows = await self.all(
                "SELECT lane, COALESCE(SUM(amount),0) AS total "
                "FROM budget_commitments WHERE day=? "
                "AND lane IN ('patrol','memory','proactive','admin') "
                "GROUP BY lane",
                (day,),
            )
            lane_used = {row["lane"]: row["total"] for row in lane_rows}
            daily_limit = microdollars(self.s.daily_usd)
            monthly_limit = microdollars(getattr(self.s, "monthly_usd", Decimal("40")))
            account_balance = await self._account_balance_unlocked()
            gcp = getattr(self.runtime, "gcp_billing", None)
            manual_set_micro = account_balance["set_microdollars"]
            if gcp is not None:
                try:
                    gcp_summary = gcp.summary_for_budget(manual_set_micro=manual_set_micro)
                except TypeError:
                    gcp_summary = gcp.summary_for_budget()
                gcp_exhausted = self._gcp_hard_stop_exhausted(gcp, manual_set_micro)
            else:
                gcp_summary = None
                gcp_exhausted = False
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
            "account_balance": account_balance,
            "gcp_billing": gcp_summary,
            "user_top_daily": user_top_row[0] if user_top_row else 0,
            "user_daily_limit": microdollars(self.s.user_daily_usd),
            "sub_budgets": {
                "patrol": {
                    "used": lane_used.get("patrol", 0),
                    "limit": microdollars(self.s.patrol_daily_usd),
                },
                "memory": {
                    "used": lane_used.get("memory", 0),
                    "limit": microdollars(self.s.memory_daily_usd),
                },
                "proactive": {
                    "used": lane_used.get("proactive", 0),
                    "limit": microdollars(self.s.proactive_daily_usd),
                },
                "admin": {
                    "used": lane_used.get("admin", 0),
                    "limit": microdollars(self.s.admin_daily_usd),
                },
            },
            "hard_stop": circuit
            or daily >= daily_limit
            or monthly >= monthly_limit
            or gcp_exhausted,
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
        actions=None,
        external_search=False,
        audit_id=None,
        consumed_image_events=(),
        consumed_file_events=(),
    ) -> None:
        batch = self.groups.active.get(self.session_scope(message))
        if batch and batch.job["id"] == job_id:
            if batch.cancel.is_set():
                raise asyncio.CancelledError
            batch.sealed = True  # No follow-up may cancel/requeue a committed outbox.
        now = time.time()
        payload = {"message": message.model_dump(), "text": text, "proactive": proactive}
        if audit_id:
            payload["audit_id"] = audit_id
        if external_search:
            payload["external_search"] = True
        if actions is not None:
            payload.update(actions=actions, allowed_at_qqs=sorted(set(allowed_at_qqs)))
        if meme_eligible:
            payload.update(meme_tag=meme_tag, meme_eligible=True)
        if session_epoch is not None:
            payload["session_epoch"] = session_epoch
        if participants:
            payload["participants"] = [m.model_dump() for m in participants]
        if segments:
            payload["segments"] = segments
            payload["allowed_at_qqs"] = sorted(set(allowed_at_qqs))
        if consumed_image_events:
            payload["consumed_image_events"] = list(dict.fromkeys(consumed_image_events))
        if consumed_file_events:
            payload["consumed_file_events"] = list(dict.fromkeys(consumed_file_events))
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
        self.reply_wakeup.set()

    async def cancel_platform_work(self, platforms):
        cancelled_any = False
        async with self.transaction():
            for table, predicate in (("jobs", "kind='message' AND "), ("outbox", "")):
                pending = await self.all(
                    f"SELECT id,payload FROM {table} WHERE {predicate}status='pending'"
                )
                ids = []
                for row in pending:
                    payload = self.vault.open(row["id"], row["payload"])
                    message = payload["message"] if table == "outbox" else payload
                    if message["platform"] in platforms:
                        ids.append((row["id"],))
                if ids:
                    cancelled_any = True
                    await self.db.executemany(
                        f"UPDATE {table} SET status='cancelled',payload=NULL WHERE id=?", ids
                    )
        if cancelled_any:
            self.job_wakeup.set()

    async def claim_reply(self) -> dict | None:
        async with self.lock:
            if not await self.one("SELECT 1 FROM outbox WHERE status='pending' LIMIT 1"):
                return None
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
            updated = cursor.rowcount == 1
        if updated:
            self.job_wakeup.set()
        return updated

    async def cancel_session_replies(self, message: Message):
        # Jobs already carry a HMAC session scope. Never decrypt other sessions' outboxes.
        async with self.transaction():
            cursor = await self.db.execute(
                "UPDATE outbox SET status='cancelled',payload=NULL "
                "WHERE status IN ('pending','sending') AND id IN "
                "(SELECT id FROM jobs WHERE kind='message' AND scope=?)",
                (self.session_scope(message),),
            )
            changed = cursor.rowcount > 0
        if changed:
            self.job_wakeup.set()

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
            ingested = len(fresh) if sub["initialized"] else 0
        if ingested:
            self.job_wakeup.set()
        return ingested

    async def save_report(self, subject: str, text: str, *, public: bool = False) -> None:
        async with self.transaction():
            await self.db.execute(
                "INSERT INTO reports(subject,payload,created,public) VALUES (?,?,?,?)",
                (subject, self.vault.seal(subject, text), time.time(), int(public)),
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
            await self.db.execute("DELETE FROM message_recalls WHERE at<?", (now - 86400,))
            await self.db.execute(
                "DELETE FROM outbox_bubble_receipts WHERE (applied=1 AND created<?) OR created<?",
                (now - 9 * 86400, now - 14 * 86400),
            )
            await self.db.execute("DELETE FROM local_quick_acks WHERE at<?", (now - 86400,))
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
            ledger_cur = await self.db.execute("DELETE FROM ledger WHERE day<?", (cutoff,))
            if ledger_cur.rowcount:
                await self.db.execute(
                    "DELETE FROM ledger_attribution WHERE id NOT IN (SELECT id FROM ledger)"
                )
                await self.db.execute(
                    "DELETE FROM ledger_work WHERE id NOT IN (SELECT id FROM ledger)"
                )
                await self.db.execute(
                    "DELETE FROM ledger_usage WHERE id NOT IN (SELECT id FROM ledger)"
                )
                await self.db.execute(
                    "DELETE FROM ledger_thinking WHERE id NOT IN (SELECT id FROM ledger)"
                )
                await self.db.execute(
                    "DELETE FROM ledger_times WHERE id NOT IN (SELECT id FROM ledger)"
                )
                await self.db.execute(
                    "DELETE FROM ledger_provider WHERE id NOT IN (SELECT id FROM ledger)"
                )

    async def prune_auxiliary(
        self,
        now: float | None = None,
        *,
        video_retention_days: int = 30,
        aux_retention_days: int = 60,
    ) -> dict[str, int]:
        """Prune old rows from optional subsystem tables when present in this SQLite file."""
        ts = time.time() if now is None else float(now)
        video_cutoff = ts - max(1, int(video_retention_days)) * 86400.0
        aux_cutoff = ts - max(1, int(aux_retention_days)) * 86400.0
        counts = {
            "colab_jobs": 0,
            "voice_companion_sessions": 0,
            "video_watch_attempts": 0,
            "bili_patrol_runs": 0,
            "bili_actions": 0,
        }
        queries: tuple[tuple[str, str, str, tuple[float, ...]], ...] = (
            (
                "colab_jobs",
                "SELECT 1 FROM colab_jobs "
                "WHERE status IN ('succeeded','failed','cancelled','timed_out','orphaned') "
                "AND COALESCE(finished_at, updated_at, created_at)<? LIMIT 1",
                "DELETE FROM colab_jobs "
                "WHERE status IN ('succeeded','failed','cancelled','timed_out','orphaned') "
                "AND COALESCE(finished_at, updated_at, created_at)<?",
                (aux_cutoff,),
            ),
            (
                "voice_companion_sessions",
                "SELECT 1 FROM voice_companion_sessions "
                "WHERE COALESCE(NULLIF(ended_at, 0), started_at)<? LIMIT 1",
                "DELETE FROM voice_companion_sessions "
                "WHERE COALESCE(NULLIF(ended_at, 0), started_at)<?",
                (aux_cutoff,),
            ),
            (
                "video_watch_attempts",
                "SELECT 1 FROM video_watch_attempts WHERE created<? LIMIT 1",
                "DELETE FROM video_watch_attempts WHERE created<?",
                (video_cutoff,),
            ),
            (
                "bili_patrol_runs",
                "SELECT 1 FROM bili_patrol_runs WHERE created<? LIMIT 1",
                "DELETE FROM bili_patrol_runs WHERE created<?",
                (aux_cutoff,),
            ),
            (
                "bili_actions",
                "SELECT 1 FROM bili_actions WHERE created<? LIMIT 1",
                "DELETE FROM bili_actions WHERE created<?",
                (aux_cutoff,),
            ),
        )
        async with self.lock:
            tables = {
                row[0]
                for row in await self.all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            active_queries = []
            for table, check_sql, delete_sql, params in queries:
                if table in tables and await self.one(check_sql, params):
                    active_queries.append((table, delete_sql, params))
        if not active_queries:
            return counts
        async with self.transaction():
            for table, delete_sql, params in active_queries:
                cur = await self.db.execute(delete_sql, params)
                counts[table] = max(0, int(cur.rowcount or 0))
        return counts

    async def wal_checkpoint(self, mode: str = "TRUNCATE") -> tuple[int, int, int] | None:
        """Run PRAGMA wal_checkpoint(mode) outside an explicit transaction."""
        normalized = str(mode or "TRUNCATE").upper()
        if normalized not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("invalid_wal_checkpoint_mode")
        async with self.lock:
            row = await self.one(f"PRAGMA wal_checkpoint({normalized})")
        if row is None:
            return None
        return (int(row[0]), int(row[1]), int(row[2]))
