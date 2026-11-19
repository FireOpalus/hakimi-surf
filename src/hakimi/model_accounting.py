"""Additive model/cache aggregates. Never rewrites the original financial ledger."""

from .thinking_policy import FLEX_MODELS, MODEL_LANES, PRIMARY_MODELS

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_attribution (
 id TEXT PRIMARY KEY, model_lane TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS model_usage_aggregates (
 period TEXT NOT NULL, model_lane TEXT NOT NULL, calls INTEGER NOT NULL DEFAULT 0,
 charged_amount INTEGER NOT NULL DEFAULT 0, estimated_amount INTEGER NOT NULL DEFAULT 0,
 estimated_calls INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0,
 cached_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
 thought_tokens INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(period,model_lane)
);
CREATE TABLE IF NOT EXISTS cache_aggregates (
 period TEXT NOT NULL, model_lane TEXT NOT NULL, calls INTEGER NOT NULL DEFAULT 0,
 cached_tokens INTEGER NOT NULL DEFAULT 0, eligible_input_tokens INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(period,model_lane)
);
CREATE TABLE IF NOT EXISTS ledger_thinking (
 id TEXT PRIMARY KEY, model_lane TEXT NOT NULL, thinking_level TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS thinking_usage_aggregates (
 period TEXT NOT NULL, model_lane TEXT NOT NULL, thinking_level TEXT NOT NULL,
 calls INTEGER NOT NULL DEFAULT 0,
 charged_amount INTEGER NOT NULL DEFAULT 0, estimated_amount INTEGER NOT NULL DEFAULT 0,
 estimated_calls INTEGER NOT NULL DEFAULT 0, input_tokens INTEGER NOT NULL DEFAULT 0,
 cached_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
 thought_tokens INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(period,model_lane,thinking_level)
);
"""

LANE_ORDER = {
    name: i for i, name in enumerate((*PRIMARY_MODELS, *FLEX_MODELS, "decision", "unknown"))
}
THINKING_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
    "xhigh": 3,
    "minimal": 4,
    "none": 5,
    "unknown": 6,
}
VALID_THINKING = {"none", "minimal", "low", "medium", "high", "xhigh"}
SUM_FIELDS = (
    "calls",
    "charged_amount",
    "estimated_amount",
    "estimated_calls",
    "input_tokens",
    "cached_tokens",
    "output_tokens",
    "thought_tokens",
)


def lane_name(value):
    return (
        "hydrogen"
        if value in {"interaction", "chat"}
        else value
        if value in MODEL_LANES
        else "unknown"
    )


def thinking_name(value, lane=None):
    if value in VALID_THINKING:
        return value
    if lane_name(lane) == "decision":
        return "none"
    return "unknown"


def _usage_tuple(amount, estimated, usage):
    return (
        amount,
        estimated or 0,
        int(estimated is not None),
        usage.get("input_tokens") or 0,
        usage.get("cached_tokens") or 0,
        usage.get("output_tokens") or 0,
        usage.get("thought_tokens") or 0,
    )


async def accumulate_thinking(store, day, lane, thinking_level, amount, estimated, usage):
    resolved_lane = lane_name(lane)
    resolved_level = thinking_name(thinking_level, resolved_lane)
    counts = _usage_tuple(amount, estimated, usage)
    for period in store._periods(day):
        await store.db.execute(
            "INSERT INTO thinking_usage_aggregates VALUES (?,?,?,1,?,?,?,?,?,?,?) "
            "ON CONFLICT(period,model_lane,thinking_level) DO UPDATE SET calls=calls+1, "
            "charged_amount=charged_amount+excluded.charged_amount, "
            "estimated_amount=estimated_amount+excluded.estimated_amount, "
            "estimated_calls=estimated_calls+excluded.estimated_calls, "
            "input_tokens=input_tokens+excluded.input_tokens, "
            "cached_tokens=cached_tokens+excluded.cached_tokens, "
            "output_tokens=output_tokens+excluded.output_tokens, "
            "thought_tokens=thought_tokens+excluded.thought_tokens",
            (period, resolved_lane, resolved_level, *counts),
        )


def summarize_thinking(rows):
    by_thinking = sorted(
        (dict(row) for row in rows),
        key=lambda r: (
            LANE_ORDER.get(r["model_lane"], 99),
            THINKING_ORDER.get(r["thinking_level"], 99),
        ),
    )
    grouped = {}
    for item in by_thinking:
        level = item["thinking_level"]
        bucket = grouped.setdefault(
            level, {"thinking_level": level, **{field: 0 for field in SUM_FIELDS}}
        )
        for field in SUM_FIELDS:
            bucket[field] += item.get(field) or 0
    summary = sorted(grouped.values(), key=lambda r: THINKING_ORDER.get(r["thinking_level"], 99))
    return by_thinking, summary


async def accumulate(
    store, day, lane, amount, estimated, usage, *, thinking_level=None, include_thinking=True
):
    counts = _usage_tuple(amount, estimated, usage)
    for period in store._periods(day):
        await store.db.execute(
            "INSERT INTO model_usage_aggregates VALUES (?,?,1,?,?,?,?,?,?,?) "
            "ON CONFLICT(period,model_lane) DO UPDATE SET calls=calls+1, "
            "charged_amount=charged_amount+excluded.charged_amount, "
            "estimated_amount=estimated_amount+excluded.estimated_amount, "
            "estimated_calls=estimated_calls+excluded.estimated_calls, "
            "input_tokens=input_tokens+excluded.input_tokens, "
            "cached_tokens=cached_tokens+excluded.cached_tokens, "
            "output_tokens=output_tokens+excluded.output_tokens, "
            "thought_tokens=thought_tokens+excluded.thought_tokens",
            (period, lane_name(lane), *counts),
        )
    if include_thinking:
        await accumulate_thinking(store, day, lane, thinking_level, amount, estimated, usage)


async def initialize(store):
    await store.db.executescript(SCHEMA)
    # Store.open holds the process flock. Marker and reconstruction commit atomically.
    async with store.transaction():
        if await store.one("SELECT value FROM meta WHERE key='model_usage_v090'"):
            return
        rows = await store.all(
            "SELECT l.day,l.amount,u.* FROM ledger l "
            "LEFT JOIN ledger_usage u ON u.id=l.id WHERE l.status<>'reserved'"
        )
        for row in rows:
            value = dict(row)
            await accumulate(
                store,
                value["day"],
                value["model_lane"],
                value["amount"],
                value["estimated_amount"],
                value,
                include_thinking=False,
            )
        await store.db.execute("INSERT INTO meta VALUES ('model_usage_v090','1')")


async def initialize_thinking(store):
    await store.db.executescript(SCHEMA)
    # Store.open holds the process flock. Backfill and meta marker commit atomically.
    async with store.transaction():
        if await store.one("SELECT value FROM meta WHERE key='thinking_usage_v1'"):
            return
        rows = await store.all(
            "SELECT l.id,l.day,l.amount,"
            "COALESCE(t.model_lane,u.model_lane,a.model_lane) AS resolved_lane,"
            "t.thinking_level AS recorded_thinking,"
            "u.estimated_amount,u.input_tokens,u.cached_tokens,"
            "u.output_tokens,u.thought_tokens "
            "FROM ledger l "
            "LEFT JOIN ledger_usage u ON u.id=l.id "
            "LEFT JOIN ledger_attribution a ON a.id=l.id "
            "LEFT JOIN ledger_thinking t ON t.id=l.id "
            "WHERE l.status<>'reserved'"
        )
        for row in rows:
            value = dict(row)
            lane = lane_name(value["resolved_lane"])
            level = thinking_name(value["recorded_thinking"], lane)
            await store.db.execute(
                "INSERT OR REPLACE INTO ledger_thinking VALUES (?,?,?)",
                (value["id"], lane, level),
            )
            await accumulate_thinking(
                store,
                value["day"],
                lane,
                level,
                value["amount"],
                value["estimated_amount"],
                value,
            )
        await store.db.execute("INSERT INTO meta VALUES ('thinking_usage_v1','1')")
