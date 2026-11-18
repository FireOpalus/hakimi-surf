"""Durable per-video claims for small-account patrols."""

MAX_READ_ATTEMPTS = 3
READ_RETRY_SECONDS = 3600

SCHEMA = """
CREATE TABLE IF NOT EXISTS bili_watch_progress (
 bvid TEXT PRIMARY KEY REFERENCES bili_watched_history(bvid) ON DELETE CASCADE,
 attempts INTEGER NOT NULL CHECK (attempts > 0), retry_after REAL
);
CREATE INDEX IF NOT EXISTS bili_watch_state ON bili_watched_history(run_id, status);
"""


class WatchHistory:
    def __init__(self, store):
        self.store = store

    async def recover(self, now: float, blocked_until: float, *, run_id: str | None = None):
        """Reading can restart. After a model handoff there may already be a charge."""
        scope = " AND run_id=?" if run_id is not None else ""
        args = (run_id,) if run_id is not None else ()
        async with self.store.transaction():
            if run_id is None:
                # Older rows have no checkpoint proving where an attempt stopped.
                await self.store.db.execute(
                    "UPDATE bili_watched_history SET status='uncertain' "
                    "WHERE status='attempted' OR (status='done' AND COALESCE(length(summary),0)=0)"
                )
            await self.store.db.execute(
                "UPDATE bili_watch_progress SET retry_after="
                "CASE WHEN attempts < ? THEN ? END WHERE bvid IN "
                "(SELECT bvid FROM bili_watched_history WHERE status='reading'" + scope + ")",
                (MAX_READ_ATTEMPTS, max(now + READ_RETRY_SECONDS, blocked_until), *args),
            )
            await self.store.db.execute(
                "UPDATE bili_watched_history SET status=CASE "
                "WHEN status='reading' AND EXISTS (SELECT 1 FROM bili_watch_progress p "
                "WHERE p.bvid=bili_watched_history.bvid) THEN 'retryable' ELSE 'uncertain' END "
                "WHERE status IN ('reading','analyzing')" + scope,
                args,
            )

    async def blocked_candidates(self, candidates: list[str], now: float) -> set[str]:
        if not candidates:
            return set()
        slots = ",".join("?" for _ in candidates)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT h.bvid FROM bili_watched_history h "
                f"LEFT JOIN bili_watch_progress p ON p.bvid=h.bvid WHERE h.bvid IN ({slots}) "
                "AND NOT (h.status='retryable' AND p.retry_after IS NOT NULL "
                "AND p.retry_after<=? AND p.attempts<?)",
                (*candidates, now, MAX_READ_ATTEMPTS),
            )
        return {row[0] for row in rows}

    async def claim(self, bvid: str, run_id: str, now: float) -> bool:
        # Repeat the candidate eligibility check inside the write transaction. A second
        # patrol may already have claimed the same BV while its selection model was running.
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "INSERT INTO bili_watched_history(bvid,run_id,status,created) "
                "VALUES (?,?,'reading',?) ON CONFLICT(bvid) DO UPDATE SET "
                "run_id=excluded.run_id,status='reading',created=excluded.created "
                "WHERE bili_watched_history.status='retryable' AND EXISTS ("
                "SELECT 1 FROM bili_watch_progress p WHERE p.bvid=excluded.bvid "
                "AND p.retry_after<=? AND p.attempts<?)",
                (bvid, run_id, now, now, MAX_READ_ATTEMPTS),
            )
            if cur.rowcount != 1:
                return False
            await self.store.db.execute(
                "INSERT INTO bili_watch_progress(bvid,attempts) VALUES (?,1) "
                "ON CONFLICT(bvid) DO UPDATE SET attempts=attempts+1,retry_after=NULL",
                (bvid,),
            )
        return True

    async def start_analysis(self, bvid: str, run_id: str) -> bool:
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE bili_watched_history SET status='analyzing' "
                "WHERE bvid=? AND run_id=? AND status='reading'",
                (bvid, run_id),
            )
            return cur.rowcount == 1

    async def counts(self) -> dict[str, int]:
        """Called under the store's read lock; exhausted is a subset of retryable."""
        row = await self.store.one(
            "SELECT "
            "COUNT(*) FILTER (WHERE h.status='done' AND length(h.summary)>0) AS done, "
            "COUNT(*) FILTER (WHERE h.status IN ('reading','analyzing')) AS processing, "
            "COUNT(*) FILTER (WHERE h.status='retryable') AS retryable, "
            "COUNT(*) FILTER (WHERE h.status IN ('uncertain','attempted') OR "
            "(h.status='done' AND COALESCE(length(h.summary),0)=0)) AS uncertain, "
            "COUNT(*) FILTER (WHERE h.status='retryable' AND p.attempts>=?) AS exhausted "
            "FROM bili_watched_history h LEFT JOIN bili_watch_progress p ON p.bvid=h.bvid",
            (MAX_READ_ATTEMPTS,),
        )
        return dict(row)
