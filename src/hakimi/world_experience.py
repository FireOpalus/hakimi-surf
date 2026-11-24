"""Local, receipt-based drives, a shared social battery and audience-scoped discoveries."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta
from math import trunc
from zoneinfo import ZoneInfo

from .modules.base import sanitize_sensor_text
from .proactive_policy import hours_bounds

MC_TASKS = frozenset({"goto", "explore", "mine", "gather", "follow"})
MC_ONLINE_TASKS = frozenset({"online", "companion"})
NEWS_TTL = 48 * 3600
MC_ROUTINE_NEWS_TTL = 6 * 3600
_MC_PROACTIVE_RE = re.compile(
    r"方块世界|minecraft|\bmc\b|原木|木头|木板|木棍|木镐|石镐|铁镐|木剑|石剑|铁剑|"
    r"挖矿|采矿|矿洞|工作台|熔炉|烤肉|苦力怕|骷髅|僵尸|末影人|蜘蛛|小木屋|掩体|出生点|重生点",
    re.I,
)
_MC_TITLE_KEYWORDS: tuple[tuple[re.Pattern[str], re.Pattern[str]], ...] = (
    (re.compile(r"_log|_planks|\blog\b", re.I), re.compile(r"木头|原木|木板|砍树|砍了")),
    (re.compile(r"\bstick\b", re.I), re.compile(r"木棍")),
    (re.compile(r"pickaxe", re.I), re.compile(r"木镐|石镐|铁镐|钻石镐|镐")),
    (re.compile(r"sword", re.I), re.compile(r"木剑|石剑|铁剑|钻石剑|剑")),
    (re.compile(r"cobblestone|\bstone\b", re.I), re.compile(r"石头|圆石")),
    (re.compile(r"coal", re.I), re.compile(r"煤")),
    (re.compile(r"iron", re.I), re.compile(r"铁")),
    (re.compile(r"diamond", re.I), re.compile(r"钻")),
    (re.compile(r"furnace", re.I), re.compile(r"熔炉")),
    (re.compile(r"crafting_table", re.I), re.compile(r"工作台")),
    (re.compile(r"bread|wheat|hay_block", re.I), re.compile(r"面包|小麦|干草")),
    (
        re.compile(r"beef|pork|mutton|chicken|steak", re.I),
        re.compile(r"烤肉|牛排|猪排|羊肉|鸡肉|肉"),
    ),
)
INTEREST_TTL = 9000
SOCIAL_LOW = 30
CHAT_SOCIAL_COST = 1.25
OWNER_CHAT_SOCIAL_COST = 0.0
CHAT_ENERGY_COST = 0.2
CHAT_BOREDOM_DELTA = -2
OWNER_CHAT_BOREDOM_DELTA = -3
AWAKE_SECONDS_PER_POINT = 600
REST_SECONDS_PER_POINT = 120
HOURLY_RATES = {
    "sleep": (15, -4),
    "day_rest": (10, -4),  # Energy uses piecewise DAY_REST_ENERGY_SEGMENTS; tail is 10/hr.
    "idle": (-2, 24),
    "mc_work": (-8, -6),
    "mc_play": (-4, -10),
}
DAY_REST_ENERGY_SEGMENTS: tuple[tuple[int, int | None, float], ...] = (
    (0, 600, 60.0),  # 0–10 min: 1 pt / 1 min
    (600, 1800, 30.0),  # 10–30 min: 1 pt / 2 min
    (1800, None, 10.0),  # > 30 min: 1 pt / 6 min
)
MAX_DAY_REST_MINUTES = 90
MAX_DAY_REST_PER_DAY = 2
DAY_REST_COOLDOWN_SECONDS = 3 * 3600
DAY_REST_PRE_SLEEP_MINUTES = 120
NON_MILESTONE_MC_ACTIONS = frozenset({"chat", "observe", "goto", "follow", "shelter", "chest"})

SCHEMA = """
CREATE TABLE IF NOT EXISTS world_rhythm (
 id INTEGER PRIMARY KEY CHECK(id=1), settled REAL NOT NULL, energy_fraction REAL DEFAULT 0,
 boredom_fraction REAL DEFAULT 0, tired INTEGER DEFAULT 0, mc_task TEXT DEFAULT '',
 mc_until REAL DEFAULT 0, mc_seen REAL DEFAULT 0, mood_until REAL DEFAULT 0,
 bili_until REAL DEFAULT 0, last_owner_at REAL DEFAULT 0,
 interest_topic TEXT DEFAULT '', interest_until REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS world_day_rest (
 id INTEGER PRIMARY KEY CHECK(id=1), started REAL NOT NULL, ends REAL NOT NULL,
 source TEXT NOT NULL, day_key TEXT DEFAULT '', day_count INTEGER DEFAULT 0
);
INSERT OR IGNORE INTO world_day_rest(id,started,ends,source) VALUES (1,0,0,'');
CREATE TABLE IF NOT EXISTS world_changes (
 receipt TEXT PRIMARY KEY, at REAL NOT NULL, reason TEXT NOT NULL, energy_before REAL,
 energy_after REAL, boredom_before INTEGER, boredom_after INTEGER,
 audience_label TEXT, social_before REAL, social_after REAL, seconds REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS world_changes_at ON world_changes(at);
CREATE TABLE IF NOT EXISTS world_social (
 scope TEXT PRIMARY KEY, label TEXT NOT NULL, charge REAL NOT NULL, at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS world_social_battery (
 id INTEGER PRIMARY KEY CHECK(id=1), charge REAL NOT NULL CHECK(charge BETWEEN 0 AND 100),
 last_reply_at REAL, active_turns INTEGER NOT NULL DEFAULT 0 CHECK(active_turns>=0)
);
CREATE TABLE IF NOT EXISTS world_news (
 event_id TEXT PRIMARY KEY, at REAL NOT NULL, expires REAL NOT NULL, audience TEXT NOT NULL,
 source TEXT NOT NULL, ref TEXT NOT NULL, title TEXT NOT NULL, shared_at REAL, intent_id TEXT
);
CREATE INDEX IF NOT EXISTS world_news_available ON world_news(audience,shared_at,expires);
CREATE TRIGGER IF NOT EXISTS world_snapshot_fields
AFTER UPDATE OF current_module,focus_level,mood,energy,boredom,last_proactive_at,updated_at
ON world_state BEGIN
 UPDATE world_state SET snapshot_json=json_set(snapshot_json,
  '$.current_module',NEW.current_module,'$.focus_level',NEW.focus_level,'$.mood',NEW.mood,
  '$.energy',NEW.energy,'$.boredom',NEW.boredom,'$.updated_at',NEW.updated_at,
  '$.last_proactive_at',NEW.last_proactive_at) WHERE singleton=1;
END;
"""


def bounded(value):
    return min(100.0, max(0.0, value))


def energy_parts(points, remainder, delta):
    """Apply whole tenths immediately and carry sub-tenth timed work between ticks."""
    total = bounded(points + remainder + delta)
    if total in (0, 100):
        return total, 0.0
    tenths = trunc(round(delta * 10, 8))
    pending = (remainder + delta) * 10 - tenths
    visible = round(points + (tenths + trunc(round(pending, 8))) / 10, 1)
    return visible, round(total - visible, 12)


def day_rest_energy_delta(started: float, left: float, right: float) -> float:
    """Integrate piecewise daytime nap energy recovery over [left, right] since nap start."""
    if right <= left:
        return 0.0
    t0 = max(0.0, left - started)
    t1 = max(t0, right - started)
    delta = 0.0
    for seg_start, seg_end, hourly_rate in DAY_REST_ENERGY_SEGMENTS:
        lo = max(t0, seg_start)
        hi = t1 if seg_end is None else min(t1, seg_end)
        if hi > lo:
            delta += (hi - lo) * hourly_rate / 3600.0
    return delta


class WorldExperience:
    def __init__(self, world):
        self.world = world
        self.store = world.store
        self.s = world.settings
        self.ready = False
        self.zone = ZoneInfo(self.s.bot_timezone)

    async def initialize_tx(self, now):
        cols = {row[1] for row in await self.store.all("PRAGMA table_info(world_rhythm)")}
        added_owner = False
        if cols:
            if "last_owner_at" not in cols:
                await self.store.db.execute(
                    "ALTER TABLE world_rhythm ADD COLUMN last_owner_at REAL DEFAULT 0"
                )
                added_owner = True
            if "interest_topic" not in cols:
                await self.store.db.execute(
                    "ALTER TABLE world_rhythm ADD COLUMN interest_topic TEXT DEFAULT ''"
                )
            if "interest_until" not in cols:
                await self.store.db.execute(
                    "ALTER TABLE world_rhythm ADD COLUMN interest_until REAL DEFAULT 0"
                )
        rest_cols = {row[1] for row in await self.store.all("PRAGMA table_info(world_day_rest)")}
        if rest_cols:
            if "day_key" not in rest_cols:
                await self.store.db.execute(
                    "ALTER TABLE world_day_rest ADD COLUMN day_key TEXT DEFAULT ''"
                )
            if "day_count" not in rest_cols:
                await self.store.db.execute(
                    "ALTER TABLE world_day_rest ADD COLUMN day_count INTEGER DEFAULT 0"
                )
        first = await self.store.db.execute(
            "INSERT OR IGNORE INTO world_rhythm(id,settled,tired) "
            "SELECT 1,?,energy<30 FROM world_state WHERE singleton=1",
            (now,),
        )
        if not await self.store.one("SELECT 1 FROM world_social_battery WHERE id=1"):
            # Carry forward the last active wallet at the existing world-clock checkpoint.
            legacy = await self.store.one(
                "SELECT charge,at FROM world_social ORDER BY at DESC,scope LIMIT 1"
            )
            rhythm = await self.store.one("SELECT settled FROM world_rhythm WHERE id=1")
            charge = (
                bounded(
                    legacy["charge"] + max(0, rhythm[0] - legacy["at"]) / AWAKE_SECONDS_PER_POINT
                )
                if legacy
                else 100.0
            )
            await self.store.db.execute(
                "INSERT INTO world_social_battery(id,charge,last_reply_at) VALUES (1,?,?)",
                (charge, legacy["at"] if legacy else None),
            )
            # Older databases stored only whole energy points plus a signed remainder.
            row = await self._values()
            visible, remainder = energy_parts(row["energy"], 0, row["energy_fraction"])
            await self.store.db.execute(
                "UPDATE world_state SET energy=? WHERE singleton=1", (visible,)
            )
            await self.store.db.execute(
                "UPDATE world_rhythm SET energy_fraction=?,"
                "tired=(? < 30 OR (tired AND ? < 45)) WHERE id=1",
                (remainder, visible, visible),
            )
        if first.rowcount or added_owner:
            await self.store.db.execute(
                "UPDATE world_rhythm SET last_owner_at=COALESCE("
                "(SELECT last_reply_at FROM world_social_battery WHERE id=1), ?) "
                "WHERE id=1 AND (last_owner_at IS NULL OR last_owner_at<=0)",
                (now,),
            )
        if first.rowcount:
            await self.store.db.execute(
                "INSERT INTO world_changes(receipt,at,reason,energy_before,energy_after,"
                "boredom_before,boredom_after) SELECT 'initial',?,'baseline',energy,energy,"
                "boredom,boredom FROM world_state WHERE singleton=1",
                (now,),
            )
            # Legacy snapshots combined planned activity and actual focus.
            await self.store.db.execute(
                "UPDATE world_state SET current_module='idle',focus_level='LOW' WHERE singleton=1"
            )

        elif not self.ready:
            # End a pre-upgrade forced nap at startup. Account for the time already spent asleep.
            await self.store.db.execute(
                "UPDATE world_day_rest SET ends=MAX(started,?) "
                "WHERE source='low_energy' AND ends>?",
                (now, now),
            )
            # The last checkpoint ends any interrupted turn. Downtime is idle time.
            await self.store.db.execute("UPDATE world_social_battery SET active_turns=0 WHERE id=1")
            await self.advance_tx(now)
            row = await self._values()
            if row["mc_task"]:
                await self._release_mc_tx()
            if row["bili_until"]:
                await self.finish_read_tx()

    def periods(self, start, end, row):
        """Split actual elapsed time at sleep, nap and confirmed work boundaries."""
        rest = self.store.runtime.rest_hours
        cuts = {start, end}
        cuts.update(
            row[key]
            for key in ("mc_until", "bili_until", "started", "ends")
            if start < row[key] < end
        )
        day = datetime.fromtimestamp(start, self.zone).date()
        last = datetime.fromtimestamp(end, self.zone).date()
        minutes = hours_bounds(rest.rest_hours) if rest.enabled else ()
        while day <= last:
            midnight = datetime.combine(day, time.min, self.zone)
            for m in minutes:
                point = (midnight + timedelta(minutes=m)).timestamp()
                if start < point < end:
                    cuts.add(point)
            day += timedelta(days=1)
        points = sorted(cuts)
        for left, right in zip(points, points[1:], strict=False):
            kind = (
                ("mc_play" if row["mc_task"] in MC_ONLINE_TASKS else "mc_work")
                if left < row["mc_until"]
                else "bili_read"
                if left < row["bili_until"]
                else "sleep"
                if rest.active(datetime.fromtimestamp(left, self.zone))
                else "day_rest"
                if rest.day_rest_enabled and row["started"] <= left < row["ends"]
                else "idle"
            )
            yield kind, left, right

    async def _values(self):
        return await self.store.one(
            "SELECT r.*,w.energy,w.boredom,w.focus_level,w.mood,d.started,d.ends,d.source,"
            "d.day_key,d.day_count,s.charge,s.last_reply_at,s.active_turns "
            "FROM world_rhythm r JOIN world_state w ON w.singleton=r.id "
            "JOIN world_day_rest d ON d.id=r.id "
            "JOIN world_social_battery s ON s.id=r.id WHERE r.id=1"
        )

    async def _change_tx(
        self, receipt, now, reason, energy_delta=0, boredom_delta=0, *, seconds=0, social=None
    ):
        row = await self._values()
        next_energy, remainder = energy_parts(row["energy"], row["energy_fraction"], energy_delta)
        boredom = bounded(row["boredom"] + row["boredom_fraction"] + boredom_delta)
        next_boredom = round(boredom)
        if receipt or (next_energy, next_boredom) != (row["energy"], row["boredom"]):
            inserted = await self.store.db.execute(
                "INSERT OR IGNORE INTO world_changes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt or f"time:{now}:{reason}",
                    now,
                    reason,
                    row["energy"],
                    next_energy,
                    row["boredom"],
                    next_boredom,
                    *(social or (None, None, None)),
                    seconds,
                ),
            )
            if not inserted.rowcount:
                return False
        await self.store.db.execute(
            "UPDATE world_rhythm SET energy_fraction=?,boredom_fraction=?,tired=? WHERE id=1",
            (
                remainder,
                boredom - next_boredom,
                next_energy < 30 or (row["tired"] and next_energy < 45),
            ),
        )
        if (next_energy, next_boredom) != (row["energy"], row["boredom"]):
            await self.store.db.execute(
                "UPDATE world_state SET "
                "energy=?,boredom=?,updated_at=MAX(updated_at,?) WHERE singleton=1",
                (next_energy, next_boredom, now),
            )
        return True

    async def advance_tx(self, now):
        row = await self._values()
        start = row["settled"]
        if now < start:
            return
        charge = row["charge"]
        for reason, left, right in self.periods(start, now, row):
            seconds = right - left
            if not row["active_turns"]:
                charge = self._recharge(charge, reason, seconds)
            if reason == "bili_read":
                continue  # Debit reading time only on a confirmed learning receipt.
            energy_rate, boredom_rate = HOURLY_RATES[reason]
            energy_delta = (
                day_rest_energy_delta(row["started"], left, right)
                if reason == "day_rest"
                else energy_rate * seconds / 3600
            )
            boredom_delta = boredom_rate * seconds / 3600
            await self._change_tx(None, right, reason, energy_delta, boredom_delta, seconds=seconds)
        await self.store.db.execute("UPDATE world_rhythm SET settled=? WHERE id=1", (now,))
        if charge != row["charge"]:
            await self.store.db.execute(
                "UPDATE world_social_battery SET charge=? WHERE id=1", (charge,)
            )
        if row["mc_task"] and now >= row["mc_until"]:
            await self._release_mc_tx()
        sleep = self.store.runtime.is_resting(now)
        napping = (
            self.store.runtime.rest_hours.day_rest_enabled and row["started"] <= now < row["ends"]
        )
        if sleep or napping or row["mood_until"] <= now or row["mood"] in {"sleeping", "resting"}:
            mood = "sleeping" if sleep else "resting" if napping else "calm"
            await self.store.db.execute(
                "UPDATE world_state SET mood=? WHERE singleton=1 AND "
                "current_module='idle' AND mood!=?",
                (mood, mood),
            )

    async def advance(self, now):
        if not self.ready:
            return
        async with self.store.transaction():
            await self.advance_tx(now)
        await self.world.refresh()

    def _day_rest_usage(self, row, now: float) -> tuple[str, int, int]:
        today_iso = datetime.fromtimestamp(now, self.zone).date().isoformat()
        count_today = int(row["day_count"] or 0) if (row["day_key"] or "") == today_iso else 0
        cooldown_remaining = 0
        if row["source"] == "planner" and row["ends"] > 0 and row["ends"] <= now:
            elapsed = now - row["ends"]
            if 0 <= elapsed < DAY_REST_COOLDOWN_SECONDS:
                cooldown_remaining = int(DAY_REST_COOLDOWN_SECONDS - elapsed)
        return today_iso, count_today, cooldown_remaining

    def _near_bedtime_for_day_rest(self, now: float) -> bool:
        if DAY_REST_PRE_SLEEP_MINUTES <= 0:
            return False
        rest = self.store.runtime.rest_hours
        if not rest.enabled:
            return False
        local_dt = datetime.fromtimestamp(now, self.zone)
        if rest.active(local_dt):
            return True
        sleep_min, _ = hours_bounds(rest.rest_hours)
        cur_min = local_dt.hour * 60 + local_dt.minute
        mins_until_sleep = (sleep_min - cur_min) % 1440
        return 0 < mins_until_sleep <= DAY_REST_PRE_SLEEP_MINUTES

    async def _can_rest_tx(self, row, now):
        if (
            now < row["settled"]
            or row["started"] <= now < row["ends"]
            or row["mc_until"] > now
            or row["bili_until"] > now
            or row["focus_level"] == "HIGH"
            or not self.store.runtime.rest_hours.day_rest_enabled
            or self.store.runtime.is_resting(now)
            or self._near_bedtime_for_day_rest(now)
        ):
            return False
        _, count_today, cooldown_sec = self._day_rest_usage(row, now)
        if count_today >= MAX_DAY_REST_PER_DAY or cooldown_sec > 0:
            return False
        return not await self.store.one(
            "SELECT 1 FROM scheduled_intents WHERE status='executing' AND expire_at>? LIMIT 1",
            (now,),
        )

    async def start_day_rest_tx(self, now, until):
        """The caller owns the transaction and has bounded the nap to the active window."""
        await self.advance_tx(now)
        row = await self._values()
        if until <= now or not await self._can_rest_tx(row, now):
            return False
        today_iso, count_today, _ = self._day_rest_usage(row, now)
        await self.store.db.execute(
            "UPDATE world_day_rest SET started=?,ends=?,source='planner',day_key=?,day_count=? "
            "WHERE id=1",
            (now, until, today_iso, count_today + 1),
        )
        await self.store.db.execute(
            "UPDATE world_state SET mood='resting' WHERE singleton=1 AND current_module='idle'"
        )
        return True

    async def end_day_rest_tx(self, now):
        await self.store.db.execute(
            "UPDATE world_day_rest SET ends=? WHERE id=1 AND started<=? AND ends>?", (now, now, now)
        )

    async def finish_read_tx(self):
        await self.store.db.execute("UPDATE world_rhythm SET bili_until=0 WHERE id=1")

    async def bili_reading(self, active, now):
        async with self.store.transaction():
            await self.advance_tx(now)
            if active:
                await self.end_day_rest_tx(now)
            await self.store.db.execute(
                "UPDATE world_rhythm SET bili_until=? WHERE id=1", (now + 3600 if active else 0,)
            )
        await self.world.refresh()

    async def _release_mc_tx(self):
        await self.store.db.execute("UPDATE world_rhythm SET mc_task='',mc_until=0 WHERE id=1")
        await self.store.db.execute(
            "UPDATE world_state SET current_module='idle',focus_level='LOW',mood='calm' "
            "WHERE singleton=1 AND current_module='minecraft' "
            "AND focus_level IN ('HIGH','MEDIUM')"
        )

    async def mc_snapshot(self, task, observed, *, disconnected=False):
        now = self.world.clock()
        async with self.store.transaction():
            row = await self._values()
            observed = min(observed, now)
            if not disconnected and observed < row["mc_seen"]:
                return
            await self.store.db.execute("UPDATE world_rhythm SET mc_seen=? WHERE id=1", (observed,))
            if not task and not row["mc_task"] and not disconnected:
                return
            await self.advance_tx(now)
            if task in MC_TASKS and observed + 40 > now and not disconnected:
                await self.end_day_rest_tx(now)
                await self.store.db.execute(
                    "UPDATE world_rhythm SET mc_task=?,mc_until=? WHERE id=1", (task, observed + 40)
                )
                await self.store.db.execute(
                    "UPDATE world_state SET "
                    "current_module='minecraft',focus_level='HIGH',mood='focused',"
                    "updated_at=? WHERE singleton=1",
                    (now,),
                )
            elif task in MC_ONLINE_TASKS and observed + 40 > now and not disconnected:
                await self.end_day_rest_tx(now)
                await self.store.db.execute(
                    "UPDATE world_rhythm SET mc_task=?,mc_until=? WHERE id=1", (task, observed + 40)
                )
                await self.store.db.execute(
                    "UPDATE world_state SET "
                    "current_module='minecraft',focus_level='MEDIUM',mood='playful',"
                    "updated_at=? WHERE singleton=1",
                    (now,),
                )
            elif row["mc_task"] or disconnected:
                await self._release_mc_tx()
        await self.world.refresh()

    async def event_tx(self, event):
        data = event.payload
        video = data.get("activity_kind") == "video_learning" or (
            event.source_module,
            event.event_type,
        ) == ("bilibili_surf", "bili_video_watched")
        mc = event.source_module == "minecraft" and event.event_type in {
            "mc_goal_completed",
            "mc_rare_find",
        }
        if not (video or mc):
            return
        if (
            mc
            and event.event_type == "mc_goal_completed"
            and str(data.get("action_type") or "").strip().lower() in NON_MILESTONE_MC_ACTIONS
        ):
            return
        now = event.created_at
        await self.advance_tx(now)
        seconds = max(0, min(3600, float(data.get("learning_seconds") or 0))) if video else 0
        reason = "video_learned" if video else event.event_type
        if not await self._change_tx(
            "event:" + event.id,
            now,
            reason,
            -2 - seconds / 300 if video else 0,
            -7 - seconds / 600 if video else -6,
            seconds=seconds,
        ):
            return
        await self.store.db.execute(
            "UPDATE world_state SET mood=? WHERE singleton=1 AND focus_level!='HIGH'",
            ("curious" if video else "pleased",),
        )
        await self.store.db.execute(
            "UPDATE world_rhythm SET mood_until=? WHERE id=1", (now + 3600,)
        )
        private = bool(data.get("private_first"))
        audience = data.get("audience", "") if private else ""
        if not private:
            raw_topic = str(data.get("title") or data.get("summary") or data.get("block") or "")
            topic = sanitize_sensor_text(raw_topic, self.s, max_tokens=20)
            if topic:
                await self.store.db.execute(
                    "UPDATE world_rhythm SET interest_topic=?,interest_until=? WHERE id=1",
                    (topic, now + INTEREST_TTL),
                )
        if private and not audience:
            return
        title = (
            ""
            if private
            else str(data.get("title") or data.get("summary") or data.get("block") or "")[:100]
        )
        news_ttl = MC_ROUTINE_NEWS_TTL if event.event_type == "mc_goal_completed" else NEWS_TTL
        await self.store.db.execute(
            "INSERT OR IGNORE INTO world_news(event_id,at,expires,audience,source,ref,title) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                event.id,
                now,
                now + news_ttl,
                audience,
                event.source_module,
                str(data.get("bvid") or ""),
                title,
            ),
        )

    def audience(self, message):
        from .world_state import private_event_audience

        return (
            private_event_audience(
                self.s, message.user_id, platform=message.platform, bot_id=message.bot_id
            )
            if (
                message.kind == "private"
                and message.user_id == message.conversation_id
                and (message.platform != "onebot" or message.bot_id == self.s.onebot_self_id)
            )
            else ""
        )

    @staticmethod
    def _recharge(charge, kind, seconds):
        interval = (
            REST_SECONDS_PER_POINT if kind in {"sleep", "day_rest"} else AWAKE_SECONDS_PER_POINT
        )
        return bounded(charge + seconds / interval)

    def _charge_at(self, row, now):
        charge = row["charge"]
        if charge < 100 and not row["active_turns"] and now > row["settled"]:
            for kind, start, end in self.periods(row["settled"], now, row):
                charge = self._recharge(charge, kind, end - start)
        return charge

    @asynccontextmanager
    async def chatting(self):
        """Exclude the union of live reply intervals from recharge, across all sessions."""
        if not self.ready:
            yield
            return
        async with self.store.transaction():
            await self.advance_tx(self.world.clock())
            await self.store.db.execute(
                "UPDATE world_social_battery SET active_turns=active_turns+1 WHERE id=1"
            )
        try:
            yield
        finally:
            async with self.store.transaction():
                await self.advance_tx(self.world.clock())
                await self.store.db.execute(
                    "UPDATE world_social_battery SET active_turns=active_turns-1 WHERE id=1"
                )
            await self.world.refresh()

    async def battery(self, message, now):
        """The platform gate may supply its target, but the balance belongs to the bot."""
        if not self.ready:
            return 100
        async with self.store.lock:
            return self._charge_at(await self._values(), now)

    @staticmethod
    def _owner_longing(last_owner_at: float, now: float) -> int:
        if not last_owner_at or last_owner_at <= 0 or now - last_owner_at <= 7200:
            return 0
        hours_apart = (now - last_owner_at) / 3600.0
        return min(100, round((hours_apart - 2.0) * 10))

    def circadian_tag(self, now: float, *, energy: float = 80.0) -> str:
        local_dt = datetime.fromtimestamp(now, self.zone)
        rest = self.store.runtime.rest_hours
        if rest.enabled:
            if rest.active(local_dt):
                return "late_night_catch"
            sleep_min, wake_min = hours_bounds(rest.rest_hours)
            cur_min = local_dt.hour * 60 + local_dt.minute
            if 0 <= (cur_min - wake_min) % 1440 < 25:
                return "just_woke_up"
            if energy < 25 or 0 < (sleep_min - cur_min) % 1440 <= 30:
                return "bedtime_drowsy"
        return "bedtime_drowsy" if energy < 25 else ""

    async def touch_owner_tx(self, now: float) -> None:
        await self.store.db.execute(
            "UPDATE world_rhythm SET last_owner_at=MAX(COALESCE(last_owner_at,0),?) WHERE id=1",
            (now,),
        )

    async def touch_owner(self, now: float | None = None) -> None:
        if not self.ready:
            return
        ts = self.world.clock() if now is None else now
        async with self.store.transaction():
            await self.touch_owner_tx(ts)

    def _classify_message_owner(self, message) -> tuple[bool, bool]:
        if message is None:
            return False, False
        from .bot_server import is_persona_owner
        from .models import Message

        msg_obj = message
        if isinstance(message, dict):
            try:
                msg_obj = Message.model_validate(message)
            except Exception:
                return False, False
        if not isinstance(msg_obj, Message):
            return False, False
        owner_any = bool(is_persona_owner(msg_obj, self.s))
        owner_dm = bool(owner_any and msg_obj.kind == "private")
        return owner_any, owner_dm

    async def spend_chat_tx(self, reply, now, *, message=None):
        # Keep the original receipt namespace so replaying a pre-upgrade ACK is free.
        receipt = "qq:" + reply
        if await self.store.one("SELECT 1 FROM world_changes WHERE receipt=?", (receipt,)):
            return
        await self.advance_tx(now)
        owner_any, owner_dm = self._classify_message_owner(message)
        social_cost = OWNER_CHAT_SOCIAL_COST if owner_dm else CHAT_SOCIAL_COST
        boredom_delta = OWNER_CHAT_BOREDOM_DELTA if owner_dm else CHAT_BOREDOM_DELTA
        charge = (await self.store.one("SELECT charge FROM world_social_battery WHERE id=1"))[0]
        after = bounded(charge - social_cost)
        if await self._change_tx(
            receipt,
            now,
            "chat_delivered",
            -CHAT_ENERGY_COST,
            boredom_delta,
            social=(None, charge, after),
        ):
            await self.store.db.execute(
                "UPDATE world_social_battery SET charge=?,"
                "last_reply_at=MAX(COALESCE(last_reply_at,?),?) WHERE id=1",
                (after, now, now),
            )
            if owner_any:
                await self.touch_owner_tx(now)

    @staticmethod
    def _news_item_mentioned(item, texts, *, proactive: bool) -> bool:
        ref = item["ref"] or ""
        title = item["title"] or ""
        if any((ref and ref in text) or (len(title) >= 4 and title in text) for text in texts):
            return True
        if item["source"] != "minecraft":
            return False
        if proactive and any(_MC_PROACTIVE_RE.search(text) for text in texts):
            return True
        for title_pat, reply_pat in _MC_TITLE_KEYWORDS:
            if title_pat.search(title) and any(reply_pat.search(text) for text in texts):
                return True
        return False

    async def confirmed_tx(self, message, reply, now, texts, *, dedicated=None):
        await self.spend_chat_tx(reply, now, message=message)
        audience = self.audience(message)
        proactive = isinstance(reply, str) and reply.startswith("jit:")
        rows = await self.store.all(
            "SELECT event_id,source,ref,title FROM world_news "
            "WHERE shared_at IS NULL AND expires>? AND at<=? AND audience IN ('',?)",
            (now, now, audience),
        )
        for item in rows:
            if item["event_id"] == dedicated or self._news_item_mentioned(
                item, texts, proactive=proactive
            ):
                await self.store.db.execute(
                    "UPDATE world_news SET shared_at=? WHERE event_id=?", (now, item["event_id"])
                )

    async def reserve_news(self, message, ticket, now):
        """A targeted sharing turn owns one item while its dispatch is in flight."""
        event = ticket.get("source_event_id")
        audience = self.audience(message)
        async with self.store.transaction():
            if event and not await self.store.one(
                "SELECT 1 FROM world_news WHERE event_id=?", (event,)
            ):
                return {}  # An existing milestone rule with no discovery attached.
            row = await self.store.one(
                "SELECT event_id,source,ref,title FROM world_news n WHERE shared_at IS NULL "
                "AND expires>? AND audience IN ('',?) AND (? IS NULL OR event_id=?) "
                "AND (intent_id=? OR NOT EXISTS (SELECT 1 FROM scheduled_intents t "
                "WHERE t.id=n.intent_id AND t.status IN ('executing','uncertain') "
                "AND t.expire_at>?)) "
                "ORDER BY (audience!='') DESC,at DESC LIMIT 1",
                (now, audience, event or None, event, ticket["id"], now),
            )
            if not row:
                return None
            await self.store.db.execute(
                "UPDATE world_news SET intent_id=? WHERE event_id=?",
                (ticket["id"], row["event_id"]),
            )
        return {k: row[k] for k in ("source", "ref", "title")}

    async def filter_digest(self, payload, now):
        async with self.store.lock:
            shared = await self.store.all(
                "SELECT ref,title FROM world_news WHERE audience='' AND shared_at "
                "IS NOT NULL AND expires>?",
                (now,),
            )
        for field in ("public_summaries", "world_events"):
            if field in payload:
                payload[field] = [
                    entry
                    for entry in payload[field]
                    if not any(
                        (r["ref"] and r["ref"] in entry)
                        or (len(r["title"]) >= 4 and r["title"] in entry)
                        for r in shared
                    )
                ]

    async def news(self, message, now, limit=3):
        if not self.ready:
            return []
        audience = self.audience(message)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT source,ref,title FROM world_news WHERE audience IN ('',?) "
                "AND shared_at IS NULL AND expires>? AND NOT EXISTS "
                "(SELECT 1 FROM scheduled_intents t WHERE t.id=world_news.intent_id "
                "AND t.status IN ('executing','uncertain') AND t.expire_at>?) "
                "ORDER BY (audience!='') DESC,at DESC LIMIT ?",
                (audience, now, now, limit),
            )
        return [dict(r) for r in rows]

    async def sensor_context(self, now: float) -> dict:
        async with self.store.lock:
            row = await self._values()
            charge = round(self._charge_at(row, now), 1)
        interest = row["interest_topic"] if row["interest_until"] > now else ""
        return {
            "social_battery": charge,
            "owner_longing": self._owner_longing(row["last_owner_at"], now),
            "interest_lingering": interest,
            "circadian_tag": self.circadian_tag(now, energy=row["energy"]),
        }

    async def preferences(self, now):
        async with self.store.lock:
            row = await self._values()
            can_rest = await self._can_rest_tx(row, now)
            charge = round(self._charge_at(row, now), 1)
            count = (
                await self.store.one(
                    "SELECT COUNT(*) FROM world_news WHERE audience='' AND "
                    "shared_at IS NULL AND expires>?",
                    (now,),
                )
            )[0]
        interest = row["interest_topic"] if row["interest_until"] > now else ""
        _, count_today, cooldown_sec = self._day_rest_usage(row, now)
        return {
            "day_rest_available": can_rest,
            "day_rest_count_today": count_today,
            "day_rest_remaining_today": max(0, MAX_DAY_REST_PER_DAY - count_today),
            "day_rest_cooldown_minutes": (cooldown_sec + 59) // 60 if cooldown_sec > 0 else 0,
            "public_unshared_count": count,
            "social_battery": charge,
            "owner_longing": self._owner_longing(row["last_owner_at"], now),
            "interest_lingering": interest,
            "circadian_tag": self.circadian_tag(now, energy=row["energy"]),
        }

    async def prune(self, now):
        if not self.ready:
            return
        async with self.store.transaction():
            # Keep confirmed activity once by its durable source ID. Time samples are disposable.
            await self.store.db.execute(
                "DELETE FROM world_changes WHERE reason IN "
                "('rest','sleep','day_rest','idle','mc_work','mc_play') AND at<?",
                (now - 14 * 86400,),
            )
            await self.store.db.execute("DELETE FROM world_social WHERE at<?", (now - 30 * 86400,))
            await self.store.db.execute("DELETE FROM world_news WHERE expires<?", (now - 86400,))

    async def view(self, now):
        async with self.store.lock:
            row = await self._values()
            changes = await self.store.all(
                "SELECT * FROM world_changes ORDER BY at DESC,rowid DESC LIMIT 20"
            )
            charge = round(self._charge_at(row, now), 2)
            news = await self.store.all(
                "SELECT source,ref,title,at,audience!='' AS private FROM world_news "
                "WHERE shared_at IS NULL AND expires>? ORDER BY at DESC LIMIT 10",
                (now,),
            )
            total = (
                await self.store.one(
                    "SELECT COUNT(*) FROM world_news WHERE shared_at IS NULL AND expires>?", (now,)
                )
            )[0]
            last = await self.store.one(
                "SELECT reason,at FROM world_changes WHERE reason IN "
                "('video_learned','mc_goal_completed','mc_rare_find',"
                "'qq_delivered','chat_delivered') "
                "ORDER BY at DESC,rowid DESC LIMIT 1"
            )
            plan = await self.store.one(
                "SELECT started,source,outcome,plan_module,plan_focus,plan_mood,accepted "
                "FROM life_plan_runs "
                "WHERE outcome NOT IN ('running','prepared') ORDER BY started "
                "DESC,rowid DESC LIMIT 1"
            )
        active = None
        if self.world.registry:
            for module in self.world.registry.active():
                active = await module.current_activity()
                if active:
                    break
        mc = row["mc_task"] if row["mc_until"] > now else ""
        interest = row["interest_topic"] if row["interest_until"] > now else ""
        return {
            "plan": dict(plan) if plan else None,
            "actual": {
                "state": ("mc_play" if mc in MC_ONLINE_TASKS else "mc_work")
                if mc
                else "bili_read"
                if row["bili_until"] > now
                else active
                if active
                else "sleep"
                if self.store.runtime.is_resting(now)
                else "day_rest"
                if row["started"] <= now < row["ends"]
                else "chatting"
                if row["active_turns"]
                else "idle",
                "mc_task": mc,
                "day_rest_until": row["ends"] if row["started"] <= now < row["ends"] else None,
                "day_rest_source": row["source"] if row["started"] <= now < row["ends"] else None,
                "last": dict(last) if last else None,
            },
            "low_energy": bool(row["tired"]),
            "owner_longing": self._owner_longing(row["last_owner_at"], now),
            "interest_lingering": interest,
            "circadian_tag": self.circadian_tag(now, energy=row["energy"]),
            "social_battery": {
                "charge": charge,
                "last_reply_at": row["last_reply_at"],
                "chatting": bool(row["active_turns"]),
            },
            # Older admin pages read a list. Expose only the shared balance during rollout.
            "social": [{"label": "共享", "charge": charge, "at": row["last_reply_at"]}],
            "news_count": total,
            "news": [
                {
                    "source": r["source"],
                    "at": r["at"],
                    "label": "私聊专属见闻"
                    if r["private"]
                    else r["title"] or r["ref"] or "MC 新发现",
                }
                for r in news
            ],
            "changes": [{k: r[k] for k in r.keys() if k != "receipt"} for r in changes],
        }
