"""Read-only admin projection from real scheduler samples, executions and ledger rows."""

import json
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

PLAN_LIMIT = 30
BASELINE_DAYS = 7

_PLAN_SELECT = """
SELECT r.id,r.day,r.started,r.trigger,r.source,r.outcome,r.candidates,r.accepted,
       r.reasons,r.avoided,r.next_at,COUNT(l.id) AS calls,
       COUNT(u.estimated_amount) AS estimated_calls,
       SUM(u.estimated_amount) AS estimated_microdollars,
       COUNT(*) FILTER (WHERE l.status='unknown') AS unknown_calls,
       COALESCE(SUM(l.amount) FILTER (WHERE l.status='unknown'),0) AS unknown_budget_microdollars,
       COUNT(*) FILTER (WHERE l.status='reserved') AS open_reservations,
       COALESCE(SUM(l.reserved) FILTER (WHERE l.status='reserved'),0) AS reserved_microdollars
FROM life_plan_runs r
LEFT JOIN ledger_work w ON w.kind='world.plan' AND w.work_id=r.id
LEFT JOIN ledger l ON l.id=w.id
LEFT JOIN ledger_usage u ON u.id=l.id
"""


def _plan(row):
    r = dict(row)
    r["reasons"] = json.loads(r["reasons"])
    cost = {
        k: r.pop(k)
        for k in (
            "calls",
            "estimated_calls",
            "estimated_microdollars",
            "unknown_calls",
            "unknown_budget_microdollars",
            "open_reservations",
            "reserved_microdollars",
        )
    }
    budget_closed = "planner_budget_closed" in r["reasons"]
    cost["missing"] = not cost["calls"] and not (
        r["source"] in {"disabled", "sleeping", "day_rest", "local_idle"} or budget_closed
    )
    if not cost["calls"] and not cost["missing"]:
        cost["estimated_microdollars"] = 0
    r["cost"] = cost
    return r


def _window(row, now):
    r = dict(row)
    started, confirmed = r["started"], r["confirmed"]
    r["closed"] = r["until_at"] <= now
    r["start_delay_seconds"] = (
        max(0, started - r["due"]) if started is not None and started <= now else None
    )
    r["within_90s"] = (
        r["start_delay_seconds"] <= 90 if r["start_delay_seconds"] is not None else None
    )
    r["delivery_confirmed"] = (
        r["stage"] == "confirmed" and confirmed is not None and confirmed <= now
    )
    r["successful"] = bool(
        r["closed"] and r["within_90s"] and r["delivery_confirmed"] and confirmed < r["until_at"]
    )
    return r


async def overview(store, now, timezone):
    local = datetime.fromtimestamp(now, ZoneInfo(timezone))
    date = local.date()
    today = date.isoformat()
    lower = datetime.combine(date, time.min, local.tzinfo).timestamp()
    upper = datetime.combine(date + timedelta(days=1), time.min, local.tzinfo).timestamp()
    async with store.lock:
        if not await store.one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='life_observed_days'"
        ):
            return None
        sampled = [
            dict(r)
            for r in await store.all(
                "SELECT day,first_tick,last_tick FROM life_observed_days WHERE day<? "
                "ORDER BY day DESC LIMIT ?",
                (today, BASELINE_DAYS),
            )
        ]
        today_sample = await store.one("SELECT * FROM life_observed_days WHERE day=?", (today,))
        days = [today, *(s["day"] for s in sampled)]
        slots = ",".join("?" for _ in days)
        rows = await store.all(
            _PLAN_SELECT + f" WHERE r.day IN ({slots}) OR r.id IN ("
            "SELECT id FROM life_plan_runs ORDER BY started DESC,rowid DESC LIMIT ?) "
            "GROUP BY r.id ORDER BY r.started DESC,r.rowid DESC",
            (*days, PLAN_LIMIT),
        )
        actions = [
            dict(r)
            for r in await store.all(
                "SELECT COALESCE(NULLIF(action_name,''),action_type) AS action,status,"
                "COUNT(*) AS count "
                "FROM scheduled_intents WHERE updated_at>=? AND updated_at<? "
                "GROUP BY action,status ORDER BY action,status",
                (lower, upper),
            )
        ]
        next_job = await store.one(
            "SELECT COALESCE(NULLIF(action_name,''),action_type) AS action,execute_after,expire_at "
            "FROM scheduled_intents WHERE status='pending' AND expire_at>? "
            "ORDER BY execute_after,created_at LIMIT 1",
            (now,),
        )
        closed = await store.all(
            "SELECT day,action,label,due,until_at,started,confirmed,stage FROM (SELECT w.*, "
            "ROW_NUMBER() OVER(PARTITION BY action ORDER BY day DESC) AS n "
            "FROM life_observed_slots w WHERE until_at<=?) WHERE n<=? ORDER BY due DESC",
            (now, BASELINE_DAYS),
        )
        live = await store.all(
            "SELECT * FROM life_observed_slots WHERE due<=? AND until_at>? ORDER BY due",
            (now, now),
        )

    plans = [_plan(r) for r in rows]
    by_day = defaultdict(list)
    for plan in plans:
        by_day[plan["day"]].append(plan)
    windows = [_window(r, now) for r in closed]
    progress = {}
    for w in windows:
        item = progress.setdefault(
            w["action"],
            {"label": w["label"], "closed": 0, "timely": 0, "confirmed": 0, "successful": 0},
        )
        item["closed"] += 1
        item["timely"] += int(bool(w["within_90s"]))
        item["confirmed"] += int(w["delivery_confirmed"])
        item["successful"] += int(w["successful"])

    def day_summary(day, sample):
        group = by_day[day]
        costs = [r["cost"] for r in group]
        known = [
            c["estimated_microdollars"] for c in costs if c["estimated_microdollars"] is not None
        ]
        return {
            "day": day,
            "observed": sample is not None,
            "first_tick": sample["first_tick"] if sample else None,
            "last_tick": sample["last_tick"] if sample else None,
            "runs": len(group),
            "avoided": sum(r["avoided"] for r in group),
            "candidates": sum(r["candidates"] for r in group),
            "accepted": sum(r["accepted"] for r in group),
            "outcomes": dict(Counter(r["outcome"] for r in group)),
            "cost": {
                "calls": sum(c["calls"] for c in costs),
                "estimated_microdollars": sum(known) if known or not group else None,
                "unknown_calls": sum(c["unknown_calls"] for c in costs),
                "unknown_budget_microdollars": sum(c["unknown_budget_microdollars"] for c in costs),
                "reserved_microdollars": sum(c["reserved_microdollars"] for c in costs),
                "open_reservations": sum(c["open_reservations"] for c in costs),
                "missing": sum(c["missing"] for c in costs),
            },
        }

    past = [day_summary(day["day"], day) for day in sampled]
    observed = {d["day"] for d in sampled}
    calendar = [(date - timedelta(days=i)).isoformat() for i in range(1, BASELINE_DAYS + 1)]
    for action, summary in progress.items():
        seen = {w["day"] for w in windows if w["action"] == action}
        summary["unobserved_recent_days"] = [d for d in calendar if d not in seen]
    return {
        "today": day_summary(today, today_sample),
        "today_actions": actions,
        "next_action": dict(next_job) if next_job else None,
        "recent_plans": plans[:PLAN_LIMIT],
        "baseline_days": past,
        "unobserved_recent_days": [d for d in calendar if d not in observed],
        "delivery_windows": windows,
        "open_windows": [_window(r, now) for r in live],
        "delivery_progress": progress,
    }
