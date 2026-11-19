"""Local, receipt-based drives, conversation batteries and audience-scoped discoveries."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from .proactive_policy import hours_bounds

MC_TASKS = frozenset({"goto", "explore", "mine", "gather", "follow"})
NEWS_TTL = 48 * 3600
SOCIAL_LOW = 30
HOURLY_RATES = {"sleep": (15, -2), "day_rest": (10, -2), "idle": (0, 4), "mc_work": (-8, -4)}
MAX_DAY_REST_MINUTES = 90

SCHEMA = """
CREATE TABLE IF NOT EXISTS world_rhythm (
 id INTEGER PRIMARY KEY CHECK(id=1), settled REAL NOT NULL, energy_fraction REAL DEFAULT 0,
 boredom_fraction REAL DEFAULT 0, tired INTEGER DEFAULT 0, mc_task TEXT DEFAULT '',
 mc_until REAL DEFAULT 0, mc_seen REAL DEFAULT 0, mood_until REAL DEFAULT 0,
 bili_until REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS world_day_rest (
 id INTEGER PRIMARY KEY CHECK(id=1), started REAL NOT NULL, ends REAL NOT NULL, source TEXT NOT NULL
);
INSERT OR IGNORE INTO world_day_rest VALUES (1,0,0,'');
CREATE TABLE IF NOT EXISTS world_changes (
 receipt TEXT PRIMARY KEY, at REAL NOT NULL, reason TEXT NOT NULL, energy_before INTEGER,
 energy_after INTEGER, boredom_before INTEGER, boredom_after INTEGER,
 audience_label TEXT, social_before INTEGER, social_after INTEGER, seconds REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS world_changes_at ON world_changes(at);
CREATE TABLE IF NOT EXISTS world_social (
 scope TEXT PRIMARY KEY, label TEXT NOT NULL, charge REAL NOT NULL, at REAL NOT NULL
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


def charge_at(row, now):
    return bounded(row["charge"] + max(0, now - row["at"]) / 300) if row else 100.0


class WorldExperience:
    def __init__(self, world):
        self.world = world
        self.store = world.store
        self.s = world.settings
        self.ready = False
        self.zone = ZoneInfo(self.s.bot_timezone)

    async def initialize_tx(self, now):
        first = await self.store.db.execute(
            "INSERT OR IGNORE INTO world_rhythm(id,settled,tired) "
            "SELECT 1,?,energy<30 FROM world_state WHERE singleton=1",
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
            # A fresh process needs fresh positive evidence to resume a live task.
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
                "mc_work"
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
            "SELECT r.*,w.energy,w.boredom,w.focus_level,w.mood,d.started,d.ends,d.source "
            "FROM world_rhythm r JOIN world_state w ON w.singleton=r.id "
            "JOIN world_day_rest d ON d.id=r.id WHERE r.id=1"
        )

    async def _change_tx(
        self, receipt, now, reason, energy_delta=0, boredom_delta=0, *, seconds=0, social=None
    ):
        row = await self._values()
        energy = bounded(row["energy"] + row["energy_fraction"] + energy_delta)
        boredom = bounded(row["boredom"] + row["boredom_fraction"] + boredom_delta)
        # Earn or spend complete points; retain fractional work across ticks and restarts.
        whole = int(energy_delta)
        fraction = row["energy_fraction"] + (energy_delta - whole)
        next_energy = (
            int(energy) if energy in (0, 100) else row["energy"] + whole + int(round(fraction, 9))
        )
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
                energy - next_energy,
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
        for reason, left, right in self.periods(start, now, row):
            seconds = right - left
            if reason == "bili_read":
                continue  # Debit reading time only on a confirmed learning receipt.
            rates = HOURLY_RATES[reason]
            await self._change_tx(
                None, right, reason, *(r * seconds / 3600 for r in rates), seconds=seconds
            )
        await self.store.db.execute("UPDATE world_rhythm SET settled=? WHERE id=1", (now,))
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

    async def _can_rest_tx(self, row, now):
        if (
            now < row["settled"]
            or row["started"] <= now < row["ends"]
            or row["mc_until"] > now
            or row["bili_until"] > now
            or row["focus_level"] == "HIGH"
            or not self.store.runtime.rest_hours.day_rest_enabled
            or self.store.runtime.is_resting(now)
        ):
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
        await self.store.db.execute(
            "UPDATE world_day_rest SET started=?,ends=?,source='planner' WHERE id=1", (now, until)
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
            "WHERE singleton=1 AND current_module='minecraft' AND focus_level='HIGH'"
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
        now = event.created_at
        await self.advance_tx(now)
        seconds = max(0, min(3600, float(data.get("learning_seconds") or 0))) if video else 0
        reason = "video_learned" if video else event.event_type
        if not await self._change_tx(
            "event:" + event.id,
            now,
            reason,
            -2 - seconds / 300 if video else -1,
            -7 - seconds / 600 if video else -6,
            seconds=seconds,
        ):
            return
        await self.store.db.execute(
            "UPDATE world_state SET mood=? WHERE singleton=1 AND focus_level!='HIGH'",
            ("curious" if video else "pleased",),
        )
        await self.store.db.execute(
            "UPDATE world_rhythm SET mood_until=? WHERE id=1", (now + 1800,)
        )
        private = bool(data.get("private_first"))
        audience = data.get("audience", "") if private else ""
        if private and not audience:
            return
        title = (
            ""
            if private
            else str(data.get("title") or data.get("summary") or data.get("block") or "")[:100]
        )
        await self.store.db.execute(
            "INSERT OR IGNORE INTO world_news(event_id,at,expires,audience,source,ref,title) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                event.id,
                now,
                now + NEWS_TTL,
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

    async def battery(self, message, now):
        if not self.ready:
            return 100
        async with self.store.lock:
            row = await self.store.one(
                "SELECT * FROM world_social WHERE scope=?", (self.store.session_scope(message),)
            )
        return round(charge_at(row, now))

    async def confirmed_tx(self, message, reply, now, texts, *, dedicated=None):
        scope = self.store.session_scope(message)
        row = await self.store.one("SELECT * FROM world_social WHERE scope=?", (scope,))
        charge = charge_at(row, now)
        label = (
            ("群聊" if message.kind == "group" else "私聊") + " · " + message.conversation_id[-4:]
        )
        if (
            message.kind == "private"
            and self.audience(message)
            and message.user_id == self.s.owner_qq_id
        ):
            label = "Owner 私聊"
        # Multiple bubbles, crash recovery and replays all share one conversation turn.
        await self.advance_tx(now)
        if await self._change_tx(
            "qq:" + reply,
            now,
            "qq_delivered",
            -1,
            -2,
            social=(label, round(charge), round(bounded(charge - 12))),
        ):
            await self.store.db.execute(
                "INSERT INTO world_social VALUES (?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                "charge=excluded.charge,at=excluded.at,label=excluded.label",
                (scope, label, bounded(charge - 12), max(now, row["at"] if row else now)),
            )
        audience = self.audience(message)
        rows = await self.store.all(
            "SELECT event_id,ref,title FROM world_news WHERE shared_at IS NULL AND expires>? "
            "AND at<=? AND audience IN ('',?)",
            (now, now, audience),
        )
        for item in rows:
            if item["event_id"] == dedicated or any(
                (item["ref"] and item["ref"] in text)
                or (len(item["title"]) >= 4 and item["title"] in text)
                for text in texts
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

    async def preferences(self, now):
        async with self.store.lock:
            row = await self._values()
            can_rest = await self._can_rest_tx(row, now)
            count = (
                await self.store.one(
                    "SELECT COUNT(*) FROM world_news WHERE audience='' AND "
                    "shared_at IS NULL AND expires>?",
                    (now,),
                )
            )[0]
        return {
            "day_rest_available": can_rest,
            "public_unshared_count": count,
        }

    async def prune(self, now):
        if not self.ready:
            return
        async with self.store.transaction():
            # Keep confirmed activity once by its durable source ID. Time samples are disposable.
            await self.store.db.execute(
                "DELETE FROM world_changes WHERE reason IN "
                "('rest','sleep','day_rest','idle','mc_work') AND at<?",
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
            social = await self.store.all("SELECT * FROM world_social ORDER BY at DESC LIMIT 10")
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
                "('video_learned','mc_goal_completed','mc_rare_find','qq_delivered') "
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
        return {
            "plan": dict(plan) if plan else None,
            "actual": {
                "state": "mc_work"
                if mc
                else "bili_read"
                if row["bili_until"] > now
                else active
                if active
                else "sleep"
                if self.store.runtime.is_resting(now)
                else "day_rest"
                if row["started"] <= now < row["ends"]
                else "idle",
                "mc_task": mc,
                "day_rest_until": row["ends"] if row["started"] <= now < row["ends"] else None,
                "day_rest_source": row["source"] if row["started"] <= now < row["ends"] else None,
                "last": dict(last) if last else None,
            },
            "low_energy": bool(row["tired"]),
            "social": [
                {"label": r["label"], "charge": round(charge_at(r, now)), "at": r["at"]}
                for r in social
            ],
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
