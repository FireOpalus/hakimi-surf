"""Read prepared local diary pages and deliver only to the authorized Drive folder."""

import asyncio
import hashlib
import logging
import time
from datetime import timedelta

from .security import Rejected

SCHEMA = """
CREATE TABLE IF NOT EXISTS roaming_sync (
  day TEXT PRIMARY KEY, digest TEXT, file_id TEXT, slot TEXT,
  next_attempt REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
"""


class DiaryDriveSync:
    def __init__(self, diary, drive, store):
        self.diary, self.drive, self.store = diary, drive, store
        self.sync_lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self):
        if self._initialized:
            return
        async with self.store.transaction():
            if self._initialized:
                return
            await self.store.db.execute(SCHEMA)
            columns = {r[1] for r in await self.store.all("PRAGMA table_info(roaming_sync)")}
            for name, kind in (("last_error", "TEXT"), ("last_success", "REAL")):
                if name not in columns:
                    await self.store.db.execute(
                        f"ALTER TABLE roaming_sync ADD COLUMN {name} {kind}"
                    )
        self._initialized = True

    async def sync_due(self, *, now: float | None = None, force=False):
        if self.drive is None:
            return "unconfigured"
        await self.diary.initialize()
        await self.initialize()
        now = time.time() if now is None else now
        local = self.diary.local(now)
        day = local.date().isoformat()
        # The 23:55 flush has its own persistent slot, independent of the two-hour cadence.
        slot = (
            "final"
            if (local.hour, local.minute) >= (23, 55)
            else str(
                int(
                    (
                        local - local.replace(hour=0, minute=0, second=0, microsecond=0)
                    ).total_seconds()
                )
                // self.diary.s.roaming_interval_seconds
            )
        )
        outcome = "unchanged"
        async with self.sync_lock:
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO roaming_sync(day) SELECT day FROM roaming_days "
                    "WHERE day BETWEEN ? AND ?",
                    ((local.date() - timedelta(days=7)).isoformat(), day),
                )
                await self.store.db.execute(
                    "DELETE FROM roaming_sync WHERE day NOT IN (SELECT day FROM roaming_days)"
                )
                rows = await self.store.all(
                    "SELECT * FROM roaming_sync WHERE day=?"
                    if force
                    else (
                        "SELECT * FROM roaming_sync WHERE day BETWEEN ? AND ? "
                        "AND next_attempt<=? ORDER BY day"
                    ),
                    (day,) if force else ((local.date() - timedelta(days=7)).isoformat(), day, now),
                )
            for row in rows:
                current_slot = slot if row["day"] == day else "closed"
                # Finished days are rechecked: events can arrive after the final flush.
                if not force and row["day"] == day and row["slot"] == current_slot:
                    continue
                markdown = await self.diary.markdown_for_sync(
                    row["day"], final=force or current_slot in {"final", "closed"}
                )
                if markdown is None:
                    outcome = "pending"
                    continue
                digest = hashlib.sha256(markdown.encode()).hexdigest()
                if digest == row["digest"]:
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET slot=?,next_attempt=0,failures=0,"
                            "last_error=NULL WHERE day=?",
                            (current_slot, row["day"]),
                        )
                    continue
                try:
                    file_id = await self.drive.sync_day(row["day"], markdown)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    delay = min(7200, 60 * 2 ** min(row["failures"], 7))
                    reason = (
                        str(exc)
                        if isinstance(exc, Rejected)
                        and str(exc)
                        in {
                            "drive_credentials_invalid",
                            "drive_shared_folder_required",
                            "drive_request_failed",
                            "drive_file_outside_scope",
                            "drive_ambiguous_file",
                            "drive_token_failed",
                            "drive_folder_unavailable",
                            "drive_oauth_credentials_invalid",
                            "drive_oauth_folder_mismatch",
                            "drive_oauth_scope_mismatch",
                            "drive_oauth_reauthorization_required",
                            "drive_oauth_client_invalid",
                        }
                        else "drive_sync_failed"
                    )
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET "
                            "next_attempt=?,failures=failures+1,last_error=? "
                            "WHERE day=?",
                            (now + delay, reason, row["day"]),
                        )
                    outcome = "pending"
                    logging.getLogger("hakimi").info("event=diary_sync_pending")
                else:
                    outcome = "uploaded"
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE roaming_sync SET digest=?,file_id=?,slot=?,"
                            "next_attempt=0,failures=0,last_error=NULL,last_success=? WHERE day=?",
                            (digest, file_id, current_slot, now, row["day"]),
                        )

        return outcome

    async def status(self):
        async with self.store.lock:
            failed = await self.store.one(
                "SELECT last_error FROM roaming_sync WHERE last_error IS NOT NULL "
                "ORDER BY day DESC LIMIT 1"
            )
            last = await self.store.one("SELECT MAX(last_success) FROM roaming_sync")
        if failed:
            detail = "同步待重试"
            if failed[0] in {
                "drive_credentials_invalid",
                "drive_oauth_credentials_invalid",
                "drive_oauth_reauthorization_required",
                "drive_oauth_client_invalid",
                "drive_oauth_scope_mismatch",
            }:
                detail = "请检查 Drive 授权，日记已保存在本地"
            return {"state": "retrying", "detail": detail, "last_error": failed[0]}
        return {
            "state": "ready",
            "detail": "已同步" if last[0] else "已配置，等待首次同步",
            "last_success": last[0],
        }

    async def run(self):
        while True:
            try:
                await self.sync_due()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("hakimi").info("event=diary_worker_error")
            await asyncio.sleep(30)
