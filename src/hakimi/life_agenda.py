"""The planning day and the execution windows on the bot's local calendar."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

from .proactive_policy import hours_bounds, minute_of_day, within_hours

MORNING_INTENT_LIMIT = 6
REVISION_INTENT_LIMIT = 3


async def agenda_items(store, day: str) -> list[dict]:
    """Read the itinerary and live execution status within the caller's SQLite lock."""
    return [
        dict(row)
        for row in await store.all(
            "SELECT i.id,i.action_name,i.execute_after,i.expire_at,i.status,i.result_reason,"
            "json_extract(i.intent_note_json,'$.note') AS note "
            "FROM life_agenda_items a JOIN scheduled_intents i ON i.id=a.intent_id "
            "WHERE a.day=? AND i.origin='planner' ORDER BY i.execute_after,i.rowid",
            (day,),
        )
    ]


@dataclass(frozen=True)
class AgendaDay:
    day: date
    zone: ZoneInfo
    wake: float
    windows: tuple[tuple[float, float], ...]

    def morning_due_at(self, now: float) -> float | None:
        if self.windows and now < self.windows[-1][1]:
            return max(now, self.wake)
        return None

    def window(
        self, *, at: str, earliest: float, relative: float, ttl: float
    ) -> tuple[float, float] | None:
        """An absolute request must fit. A relative suggestion can wait for opening."""
        times = []
        if at:
            try:
                minute = minute_of_day(at)
            except ValueError:
                return None
            local = datetime.combine(self.day, time(minute // 60, minute % 60), self.zone)
            for fold in (0, 1):
                stamp = local.replace(fold=fold).timestamp()
                if datetime.fromtimestamp(stamp, self.zone).strftime("%H:%M") == at:
                    times.append(stamp)
        for start, end in self.windows:
            for stamp in times if at else [max(start, relative)]:
                if start <= stamp < end and stamp >= earliest:
                    return stamp, min(end, stamp + ttl)
        return None


@lru_cache(maxsize=64)
def agenda_day(day: date, zone: ZoneInfo, active: str, quiet: str, rest: str | None) -> AgendaDay:
    active_span, quiet_span = hours_bounds(active), hours_bounds(quiet)
    rest_span = hours_bounds(rest) if rest else None
    wake_minute = rest_span[1] if rest_span else active_span[0]
    midnight = datetime.combine(day, time.min, zone)
    wake = (midnight + timedelta(minutes=wake_minute)).timestamp()
    tomorrow = int((midnight + timedelta(days=1)).timestamp())
    windows = []
    opened = None
    for stamp in range(int(wake), tomorrow, 60):
        wall = datetime.fromtimestamp(stamp, zone)
        minute = wall.hour * 60 + wall.minute
        if (
            within_hours(minute, active_span)
            and not within_hours(minute, quiet_span)
            and not (rest_span and within_hours(minute, rest_span))
        ):
            if opened is None:
                opened = stamp
        elif opened is not None:
            windows.append((opened, stamp))
            opened = None
    if opened is not None:
        windows.append((opened, tomorrow))
    return AgendaDay(day, zone, wake, tuple(windows))
