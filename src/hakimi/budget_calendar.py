"""One-time UTC→BOT_TIMEZONE rebucketing with preserved history and conservative carry."""

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_calendar_legacy (
 period TEXT PRIMARY KEY, timezone TEXT NOT NULL, payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS budget_calendar_carry (
 period TEXT PRIMARY KEY, amount INTEGER NOT NULL
);
"""
METRICS = (
    "charged_amount",
    "calls",
    "known_usage_calls",
    "input_tokens",
    "output_tokens",
    "cached_tokens",
    "thought_tokens",
    "interaction_calls",
    "interaction_input_tokens",
    "cache_known_calls",
    "cache_known_input_tokens",
    "thought_known_calls",
    "estimated_amount",
    "estimated_calls",
)


def observations(row):
    known = row["input_tokens"] is not None and row["output_tokens"] is not None
    interaction = known and row["model_lane"] in {"interaction", "hydrogen", "carbon"}
    cache_known = interaction and row["cached_tokens"] is not None
    return {
        "charged_amount": row["amount"],
        "calls": 1,
        "known_usage_calls": int(known),
        "input_tokens": row["input_tokens"] or 0,
        "output_tokens": row["output_tokens"] or 0,
        "cached_tokens": (row["cached_tokens"] or 0) if cache_known else 0,
        "thought_tokens": row["thought_tokens"] or 0,
        "interaction_calls": int(interaction),
        "interaction_input_tokens": (row["input_tokens"] or 0) if interaction else 0,
        "cache_known_calls": int(cache_known),
        "cache_known_input_tokens": (row["input_tokens"] or 0) if cache_known else 0,
        "thought_known_calls": int(known and row["thought_tokens"] is not None),
        "estimated_amount": row["estimated_amount"] or 0,
        "estimated_calls": int(row["estimated_amount"] is not None),
    }


def utc_day_intersects(old_day, start, end):
    old_start = datetime.fromisoformat(old_day).replace(tzinfo=UTC)
    return old_start < end and old_start + timedelta(days=1) > start


async def initialize_calendar(store):
    """Run inside Store.open's transaction; never alter the lifetime aggregate or total."""
    row = await store.one("SELECT value FROM meta WHERE key='budget_calendar_v2'")
    if row:
        info = store.vault.open("budget-calendar", row[0])
        if info["timezone"] != store.s.bot_timezone:
            raise RuntimeError("BOT_TIMEZONE differs from the persisted budget calendar")
        return
    zone = ZoneInfo(store.s.bot_timezone)
    local = store.local_now()
    today, month = local.date().isoformat(), local.strftime("%Y-%m")
    day_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start = day_start.replace(day=1)
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    bounds = {
        "day:" + today: (
            day_start.astimezone(UTC),
            (day_start + timedelta(days=1)).astimezone(UTC),
        ),
        "month:" + month: (month_start.astimezone(UTC), month_end.astimezone(UTC)),
    }
    old = [dict(r) for r in await store.all("SELECT * FROM usage_aggregates")]
    for stats in old:
        await store.db.execute(
            "INSERT INTO usage_calendar_legacy VALUES (?,'UTC',?)",
            (stats["period"], store.vault.seal("usage-calendar:" + stats["period"], stats)),
        )
    rows = await store.all(
        "SELECT l.id,l.day,l.amount,t.created,u.input_tokens,u.output_tokens,"
        "u.cached_tokens,u.thought_tokens,u.model_lane,u.estimated_amount "
        "FROM ledger l LEFT JOIN ledger_times t ON t.id=l.id "
        "LEFT JOIN ledger_usage u ON u.id=l.id"
    )
    retained_by_utc = defaultdict(int)
    rebuilt = defaultdict(lambda: dict.fromkeys(METRICS, 0))
    carry = dict.fromkeys(bounds, 0)
    missing_times = 0
    for row in rows:
        retained_by_utc[row["day"]] += row["amount"]
        if row["created"] is not None:
            day = datetime.fromtimestamp(row["created"], zone).date().isoformat()
        else:
            # Unknown timestamps remain explicitly approximate: keep the old date label,
            # and charge overlapping current periods too when that label would omit them.
            missing_times += 1
            day = row["day"]
            for period, (start, end) in bounds.items():
                assigned = "day:" + day if period.startswith("day:") else "month:" + day[:7]
                if period != assigned and utc_day_intersects(row["day"], start, end):
                    carry[period] += row["amount"]
        await store.db.execute("UPDATE ledger SET day=? WHERE id=?", (day, row["id"]))
        values = observations(row)
        for period in ("day:" + day, "month:" + day[:7]):
            for metric in METRICS:
                rebuilt[period][metric] += values[metric]
    # Preserve UTC historical details separately; only retained timestamped rows can
    # yield exact local-calendar token/cache/thought statistics after pruning.
    old_days = {r["period"][4:]: r for r in old if r["period"].startswith("day:")}
    unknown_amount = 0
    for day, stats in old_days.items():
        missing = max(0, stats["charged_amount"] - retained_by_utc[day])
        unknown_amount += missing
        for period, (start, end) in bounds.items():
            if missing and utc_day_intersects(day, start, end):
                carry[period] += missing
    total = (await store.one("SELECT amount FROM budget_totals WHERE singleton=1"))[0]
    # Very old pre-aggregate ledgers may have lifetime spending with no date at all.
    # Keep a conservative one-period carry instead of silently granting fresh budget.
    unassigned = max(0, total - sum(r["charged_amount"] for r in old_days.values()))
    for period in carry:
        carry[period] += unassigned
    await store.db.execute("DELETE FROM usage_aggregates WHERE period<>'lifetime'")
    columns, marks = ",".join(METRICS), ",".join("?" for _ in METRICS)
    for period, stats in rebuilt.items():
        await store.db.execute(
            f"INSERT INTO usage_aggregates(period,{columns}) VALUES (?,{marks})",
            (period, *(stats[m] for m in METRICS)),
        )
    for period, amount in carry.items():
        if amount:
            await store.db.execute(
                "INSERT INTO budget_calendar_carry VALUES (?,?)", (period, amount)
            )
    lifetime = next((r for r in old if r["period"] == "lifetime"), {})
    pruned_calls = max(0, lifetime.get("calls", 0) - len(rows))
    info = {
        "timezone": store.s.bot_timezone,
        "migrated_at_utc": local.astimezone(UTC).isoformat(),
        "previous_calendar": "UTC",
        "missing_timestamp_calls": missing_times,
        "pruned_calls": pruned_calls,
        "unassigned_microdollars": unassigned,
        "retained_periods_exact": missing_times == 0 and pruned_calls == 0 and unassigned == 0,
        "legacy_aggregate_archive": "usage_calendar_legacy",
        "migration_day_carry_microdollars": carry["day:" + today],
        "migration_month_carry_microdollars": carry["month:" + month],
        "note": "Lifetime totals are preserved. Local period statistics use retained rows; "
        "unknown UTC history remains archived. Carry is conservative and expires at the "
        "local day/month boundary, without changing lifetime spending.",
    }
    await store.db.execute(
        "INSERT INTO meta VALUES ('budget_calendar_v2',?)",
        (store.vault.seal("budget-calendar", info),),
    )
