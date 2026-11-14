"""Local maintenance only. No network API and no access to plaintext messages."""

import argparse
import asyncio
import json
import sqlite3
from datetime import UTC, datetime

from .config import Settings
from .store import Store


async def clear_circuit(settings: Settings) -> None:
    store = Store(settings)
    try:
        await store.open()  # Refuses to mutate while the service is running.
        async with store.transaction():
            await store.db.execute("DELETE FROM meta WHERE key='budget_overrun'")
        print("Budget circuit cleared. Existing charges and reservations were retained.")
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["status", "clear-budget-circuit"])
    args = parser.parse_args()
    settings = Settings()
    if args.command == "clear-budget-circuit":
        asyncio.run(clear_circuit(settings))
        return
    path = (settings.data_dir / "hakimi.sqlite3").resolve()
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2) as db:
        day = datetime.now(UTC).date().isoformat()
        report = {
            "day_utc": day,
            "budget_micro_usd": db.execute(
                "SELECT COALESCE(SUM(amount),0) FROM ledger WHERE day=?", (day,)
            ).fetchone()[0],
            "budget_circuit_open": bool(
                db.execute("SELECT 1 FROM meta WHERE key='budget_overrun'").fetchone()
            ),
            "jobs": dict(db.execute("SELECT status,COUNT(*) FROM jobs GROUP BY status")),
            "outbox": dict(db.execute("SELECT status,COUNT(*) FROM outbox GROUP BY status")),
            "subscriptions": db.execute("SELECT COUNT(*) FROM subscriptions").fetchone()[0],
        }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
