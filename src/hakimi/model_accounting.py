"""Additive model/cache aggregates. Never rewrites the original financial ledger."""

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
"""


def lane_name(value):
    return (
        "hydrogen"
        if value in {"interaction", "chat"}
        else value
        if value in {"decision", "hydrogen", "carbon", "memory_flex"}
        else "unknown"
    )


async def accumulate(store, day, lane, amount, estimated, usage):
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
            (
                period,
                lane_name(lane),
                amount,
                estimated or 0,
                int(estimated is not None),
                usage.get("input_tokens") or 0,
                usage.get("cached_tokens") or 0,
                usage.get("output_tokens") or 0,
                usage.get("thought_tokens") or 0,
            ),
        )


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
            )
        await store.db.execute("INSERT INTO meta VALUES ('model_usage_v090','1')")
