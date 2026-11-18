"""Durable planning clock, paid-result checkpoints, and atomic event acknowledgements."""

import asyncio
import json
import logging
import uuid

RECEIPT_READ_TIMEOUT = 2.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS life_planner_clock (
    id INTEGER PRIMARY KEY CHECK(id=1), next_at REAL NOT NULL, signal_at REAL,
    signature TEXT NOT NULL DEFAULT '', explore_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS life_plan_runs (
    id TEXT PRIMARY KEY, day TEXT NOT NULL, started REAL NOT NULL, trigger TEXT NOT NULL,
    source TEXT NOT NULL, outcome TEXT NOT NULL, candidates INTEGER NOT NULL DEFAULT 0,
    accepted INTEGER NOT NULL DEFAULT 0, reasons TEXT NOT NULL DEFAULT '{}',
    avoided INTEGER NOT NULL DEFAULT 0, next_at REAL NOT NULL, events TEXT NOT NULL,
    signature TEXT NOT NULL, explore_at REAL NOT NULL, checkpoint BLOB
);
CREATE INDEX IF NOT EXISTS life_plans_day ON life_plan_runs(day,started);
CREATE INDEX IF NOT EXISTS life_plans_outcome ON life_plan_runs(outcome,started);
CREATE TABLE IF NOT EXISTS life_observed_days (
    day TEXT PRIMARY KEY, first_tick REAL NOT NULL, last_tick REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS life_observed_slots (
    day TEXT NOT NULL, action TEXT NOT NULL, due REAL NOT NULL, until_at REAL NOT NULL,
    label TEXT NOT NULL, started REAL, confirmed REAL, stage TEXT, PRIMARY KEY(day,action)
);
"""


class PlannerJournal:
    def __init__(self, store):
        self.store = store
        self.ready = False
        self.start_lock = asyncio.Lock()

    async def initialize(self, now, *, initial_interval=0):
        async with self.start_lock:
            if not self.ready:
                async with self.store.lock:
                    await self.store.db.executescript(SCHEMA)
                async with self.store.transaction():
                    columns = {
                        r["name"] for r in await self.store.all("PRAGMA table_info(life_plan_runs)")
                    }
                    for name in ("plan_module", "plan_focus", "plan_mood"):
                        if name not in columns:
                            await self.store.db.execute(
                                f"ALTER TABLE life_plan_runs ADD COLUMN {name} TEXT"
                            )
                    # On the first upgrade, the preceding release may already have paid
                    # for a plan. Honor its minimum interval when no saved clock exists.
                    last = await self.store.one(
                        "SELECT MAX(t.created) FROM ledger_work w JOIN ledger_times t ON t.id=w.id "
                        "WHERE w.kind='world.plan' AND t.created<=?",
                        (now,),
                    )
                    initial = max(now + 120, (last[0] or 0) + initial_interval)
                    await self.store.db.execute(
                        "INSERT OR IGNORE INTO life_planner_clock(id,next_at) VALUES (1,?)",
                        (initial,),
                    )
                    # An uncheckpointed provider request has an unknown result. Preserve the
                    # wake reserved before dispatch and leave its event IDs unconsumed.
                    await self.store.db.execute(
                        "UPDATE life_plan_runs SET outcome='interrupted',reasons=? "
                        "WHERE outcome='running'",
                        ('{"process_interrupted":1}',),
                    )
                self.ready = True
        return await self.clock()

    async def clock(self):
        async with self.store.lock:
            row = dict(await self.store.one("SELECT * FROM life_planner_clock WHERE id=1"))
            last = await self.store.one(
                "SELECT started,source FROM life_plan_runs ORDER BY started DESC,rowid DESC LIMIT 1"
            )
            checkpoint = await self.store.one(
                "SELECT 1 FROM life_plan_runs WHERE outcome='prepared' LIMIT 1"
            )
        if row["signal_at"] is not None:
            row["next_at"] = min(row["next_at"], row["signal_at"])
        if checkpoint:
            row["next_at"] = 0
        row["last_at"], row["last_source"] = tuple(last) if last else (0, "none")
        return row

    async def signal(self, now):
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE life_planner_clock SET signal_at=MIN(COALESCE(signal_at,?),?) WHERE id=1",
                (now, now),
            )

    async def pause(self, day, now, next_at, reason):
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE life_planner_clock SET next_at=?,signal_at=NULL WHERE id=1", (next_at,)
            )
            await self.store.db.execute(
                "INSERT INTO life_plan_runs(id,day,started,trigger,source,outcome,"
                "next_at,events,signature,explore_at) "
                "VALUES (?,?,?,'timer',?,?,?,'[]','',0)",
                (uuid.uuid4().hex, day, now, reason, reason, next_at),
            )

    async def prepared(self):
        async with self.store.lock:
            row = await self.store.one(
                "SELECT * FROM life_plan_runs WHERE outcome='prepared' ORDER BY started LIMIT 1"
            )
        if row is None:
            return None
        return dict(row), self.store.vault.open("world-plan:" + row["id"], row["checkpoint"])

    async def start(
        self, day, now, *, trigger, next_at, signature, explore_at, events, prior_signal=None
    ):
        run_id = uuid.uuid4().hex
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO life_plan_runs(id,day,started,trigger,source,outcome,"
                "next_at,events,signature,explore_at)"
                " VALUES (?,?,?,?,'pending','running',?,?,?,?)",
                (run_id, day, now, trigger, next_at, json.dumps(events), signature, explore_at),
            )
            # Reserve the next wake before a possibly billable dispatch.
            await self.store.db.execute(
                "UPDATE life_planner_clock SET next_at=?,signal_at=CASE WHEN signal_at IS ? "
                "THEN NULL ELSE signal_at END WHERE id=1",
                (next_at, prior_signal),
            )
        return run_id

    async def checkpoint(self, run_id, source, plan, reasons):
        sealed = self.store.vault.seal("world-plan:" + run_id, plan)
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE life_plan_runs SET source=?,outcome='prepared',checkpoint=?,reasons=? "
                "WHERE id=?",
                (source, sealed, json.dumps(reasons), run_id),
            )

    async def commit(
        self,
        world,
        run_id,
        *,
        tickets,
        candidates,
        source,
        outcome,
        reasons,
        avoided,
        plan=(None, None, None),
        day_rest=None,
    ):
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT * FROM life_plan_runs WHERE id=? AND outcome IN ('running','prepared')",
                (run_id,),
            )
            if row is None:
                return []
            if day_rest:
                started, ends = day_rest
                if await world.experience.start_day_rest_tx(started, ends, "planner"):
                    outcome = "day_rest"
                else:
                    reasons["day_rest_unavailable"] = 1
            ids = [await world.insert_intent_tx(ticket, now=row["started"]) for ticket in tickets]
            await self.store.db.executemany(
                "UPDATE global_events SET consumed=1 WHERE id=?",
                [(event,) for event in json.loads(row["events"])],
            )
            await self.store.db.execute(
                "UPDATE life_plan_runs SET outcome=?,source=?,reasons=?,candidates=?,accepted=?,"
                "avoided=?,checkpoint=NULL,plan_module=?,plan_focus=?,plan_mood=? WHERE id=?",
                (
                    outcome,
                    source,
                    json.dumps(reasons),
                    candidates,
                    len(ids),
                    int(avoided),
                    *plan,
                    run_id,
                ),
            )
            await self.store.db.execute(
                "UPDATE life_planner_clock SET next_at=?,signature=?,explore_at=? WHERE id=1",
                (row["next_at"], row["signature"], row["explore_at"]),
            )
            if await world.remaining_milestone_tx():
                # A high event can arrive after reading the batch or beyond its 20-row limit.
                follow_up = row["started"] + 60
                await self.store.db.execute(
                    "UPDATE life_planner_clock SET signal_at=MIN(COALESCE(signal_at,?),?) "
                    "WHERE id=1",
                    (follow_up, follow_up),
                )
        return ids

    async def observe_tick(self, day, now):
        """Throttle receipt polling across ticks and restarts using the last recorded sample."""
        if not self.ready:
            await self.initialize(now)
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "INSERT INTO life_observed_days(day,first_tick,last_tick) VALUES (?,?,?) "
                "ON CONFLICT(day) DO UPDATE SET last_tick=excluded.last_tick "
                "WHERE last_tick<=excluded.last_tick-900",
                (day, now, now),
            )
            return cur.rowcount == 1

    async def observe_slot(self, day, slot, start, end):
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "INSERT OR IGNORE INTO life_observed_slots(day,action,due,until_at,label) "
                "VALUES (?,?,?,?,?)",
                (day, slot.name, start, end, slot.label or slot.name),
            )
            return cur.rowcount == 1

    async def sample_receipts(self, name, read):
        try:
            async with asyncio.timeout(RECEIPT_READ_TIMEOUT):
                receipts = await read()
            await self.record_receipts(name, receipts)
        except Exception:
            logging.getLogger("hakimi").warning("event=world_receipt_sample_failed action=%s", name)

    async def record_receipts(self, name, receipts):
        # Only a window actually observed while the timer was enabled can be measured.
        # The module may prune its outbox; confirmed evidence and its timestamps persist.
        if not receipts:
            return
        async with self.store.transaction():
            await self.store.db.executemany(
                "UPDATE life_observed_slots SET started=COALESCE(MIN(started,?),started,?), "
                "confirmed=COALESCE(confirmed,?), stage=CASE WHEN confirmed IS NULL "
                "THEN ? ELSE stage END WHERE day=? AND action=?",
                [(r.started, r.started, r.confirmed, r.stage, r.day, name) for r in receipts],
            )
