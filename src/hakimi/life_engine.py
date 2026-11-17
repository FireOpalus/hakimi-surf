"""Autonomous LifeEngine: LifeBlockPlanner, zero-Token GatekeeperWorker, and JITExecutor."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import stat
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .bot_server import is_persona_owner
from .config import Settings
from .gemini import WireLifePlan
from .generation_policy import Scenario
from .models import Message
from .modules.base import sanitize_sensor_text
from .outgoing import deliver_proactive_jit
from .proactive_policy import minute_of_day
from .reply_style import trim_complete_text
from .routing import RouteDecision
from .security import authorize
from .world_state import (
    IntentTicketCreate,
    MilestoneEvent,
    _check_no_prewritten_speech,
)

log = logging.getLogger("hakimi")

JIT_TRIGGER_MESSAGE = (
    "[系统主动意图触发：请结合 jit_proactive_intent 与最新上下文自然临场发言，"
    "若当前语境不适合插话请将 abstain 设为 true]"
)
DESKTOP_CONTEXT_HINT = "BOISER 近期刚在电脑端忙正事，默契关心一句注意休息喝水，绝不问怎么没回消息"
_GENERIC_OWNER_SILENCE_ACTIONS = frozenset(
    {
        "owner_silence_care",
        "owner_companion",
        "owner_desktop_warm_care",
        "companion",
        "check_in",
    }
)
_INTERNAL_MC_ACTIONS = frozenset(
    {
        "build_plan",
        "explore",
        "mine",
        "gather",
        "goto",
        "follow",
        "look_signs",
        "observe",
        "stop",
        "idle",
    }
)


class LifeEngineConfig(BaseModel):
    """Persistent configuration for LifeBlockPlanner and GatekeeperWorker."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = True
    active_hours: str = "09:00-01:00"
    planner_min_minutes: int = Field(default=120, ge=30, le=720)
    planner_max_minutes: int = Field(default=240, ge=60, le=720)
    sleep_min_minutes: int = Field(default=360, ge=120, le=960)
    sleep_max_minutes: int = Field(default=480, ge=180, le=960)
    group_cooldown_seconds: int = Field(default=1500, ge=300, le=86400)
    group_active_window_seconds: int = Field(default=1800, ge=60, le=7200)
    group_min_recent_messages: int = Field(default=2, ge=1, le=50)
    desktop_active_suppress_seconds: int = Field(default=7200, ge=300, le=86400)
    max_proactive_per_day: int = Field(default=6, ge=0, le=30)

    @field_validator("active_hours")
    @classmethod
    def _validate_active_hours(cls, value: str) -> str:
        parts = value.split("-")
        if len(parts) != 2 or minute_of_day(parts[0]) == minute_of_day(parts[1]):
            raise ValueError("active_hours must be two different HH:MM times")
        return value

    @model_validator(mode="after")
    def _validate_ranges(self) -> Self:
        if self.planner_max_minutes < self.planner_min_minutes:
            raise ValueError("planner_max_minutes must be >= planner_min_minutes")
        if self.sleep_max_minutes < self.sleep_min_minutes:
            raise ValueError("sleep_max_minutes must be >= sleep_min_minutes")
        return self

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        kwargs: dict[str, Any] = {}
        for field_name in cls.model_fields:
            attr = f"life_engine_{field_name}"
            if hasattr(settings, attr):
                kwargs[field_name] = getattr(settings, attr)
        return cls(**kwargs)

    def is_active_time(self, local: datetime) -> bool:
        start, end = map(minute_of_day, self.active_hours.split("-"))
        current = local.hour * 60 + local.minute
        return start <= current < end if start < end else current >= start or current < end


def _zone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _day_bounds(stamp: float, tz_name: str) -> tuple[float, float]:
    zone = _zone(tz_name)
    local = datetime.fromtimestamp(stamp, zone)
    start = datetime.combine(local.date(), datetime.min.time(), zone)
    end = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), zone)
    return start.timestamp(), end.timestamp()


def _allowlisted_groups(settings: Settings) -> list[str]:
    return sorted(
        gid
        for gid in settings.onebot_allowed_groups
        if settings.group_policies.get(f"onebot:{gid}", "at_only") != "muted"
    )


def _normalize_target_scope(raw_scope: str, settings: Settings) -> str | None:
    scope = str(raw_scope or "").strip()
    if not scope:
        return None
    owner_aliases = {"owner", "owner_dm"}
    if settings.owner_qq_id:
        owner_aliases.update(
            {
                f"owner:{settings.owner_qq_id}",
                f"private:{settings.owner_qq_id}",
                f"onebot:{settings.owner_qq_id}",
            }
        )
    if scope in owner_aliases:
        return (
            f"owner:{settings.owner_qq_id}"
            if (settings.onebot_enabled and settings.owner_qq_id)
            else None
        )
    gid = scope.split(":", 1)[1].strip() if scope.startswith(("onebot:", "group:")) else scope
    if (
        settings.onebot_enabled
        and gid in settings.onebot_allowed_groups
        and settings.group_policies.get(f"onebot:{gid}", "at_only") != "muted"
    ):
        return f"onebot:{gid}"
    return None


def _scope_to_message(
    target_scope: str, settings: Settings, *, event_id: str, created_at: float
) -> Message | None:
    if target_scope.startswith("owner:"):
        uid = target_scope.split(":", 1)[1]
        if not settings.onebot_enabled or not settings.owner_qq_id or uid != settings.owner_qq_id:
            return None
        return Message(
            platform="onebot",
            bot_id=settings.onebot_self_id,
            event_id=event_id,
            user_id=settings.owner_qq_id,
            conversation_id=settings.owner_qq_id,
            kind="private",
            text="",
            addressed=True,
            created_at=created_at,
        )
    if target_scope.startswith("onebot:"):
        gid = target_scope.split(":", 1)[1]
        if (
            not settings.onebot_enabled
            or gid not in settings.onebot_allowed_groups
            or settings.group_policies.get(f"onebot:{gid}", "at_only") == "muted"
        ):
            return None
        return Message(
            platform="onebot",
            bot_id=settings.onebot_self_id,
            event_id=event_id,
            user_id="0",
            conversation_id=gid,
            kind="group",
            text="",
            addressed=True,
            created_at=created_at,
        )
    return None


def _parse_memory_timestamp(raw_ts: Any, tz_name: str) -> float:
    text = str(raw_ts or "").strip()
    if not text:
        return 0.0
    zone = _zone(tz_name)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=zone).timestamp()
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone)
    return dt.timestamp()


def _read_desktop_latest_ts_sync(memory_dir: Path, tz_name: str) -> float:
    path = memory_dir / "short_term_memory.json"
    if path.is_symlink():
        return 0.0
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return 0.0
    try:
        with os.fdopen(fd, "rb") as file:
            st = os.fstat(file.fileno())
            if not stat.S_ISREG(st.st_mode):
                return 0.0
            raw = file.read(262_144).decode("utf-8", errors="ignore")
            mtime = float(st.st_mtime)
    except OSError:
        return 0.0
    latest = 0.0
    try:
        items = json.loads(raw)
    except ValueError:
        items = []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            parsed = _parse_memory_timestamp(item.get("timestamp"), tz_name)
            if parsed > latest:
                latest = parsed
    return latest if latest > 0.0 else mtime


async def _desktop_latest_activity(service: Any) -> dict[str, Any]:
    cloud_mem = getattr(service, "cloud_memory", None)
    if cloud_mem is not None and hasattr(cloud_mem, "latest_activity_info"):
        info = await asyncio.to_thread(cloud_mem.latest_activity_info)
        return {
            "enabled": bool(info.get("enabled", False)),
            "latest_timestamp": float(info.get("latest_timestamp", 0.0) or 0.0),
            "latest_iso": str(info.get("latest_iso", "") or ""),
            "latest_tag": str(info.get("latest_tag", "") or ""),
        }
    if not getattr(service.s, "enable_antigravity_memory", False):
        return {"enabled": False, "latest_timestamp": 0.0, "latest_iso": "", "latest_tag": ""}
    ts = await asyncio.to_thread(
        _read_desktop_latest_ts_sync,
        Path(service.s.antigravity_memory_dir),
        service.s.bot_timezone,
    )
    return {"enabled": True, "latest_timestamp": ts, "latest_iso": "", "latest_tag": ""}


class LifeBlockPlanner:
    """Macro 2~4h life block planner + circadian sleep scheduler + milestone impulse subscriber."""

    def __init__(
        self,
        service: Any,
        engine: LifeEngine,
        *,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
    ) -> None:
        self.service = service
        self.s: Settings = service.s
        self.store = service.store
        self.engine = engine
        self.clock = clock
        self.rng = rng or random.SystemRandom()
        self.next_plan_at: float = 0.0
        self.last_plan_at: float = 0.0
        self.last_plan_source: str = "none"
        self._impulse_cooldowns: dict[str, float] = {}
        self._recorded_milestone_diary_ids: dict[str, float] = {}

    @property
    def config(self) -> LifeEngineConfig:
        return self.engine.config

    def _local(self, stamp: float) -> datetime:
        return datetime.fromtimestamp(stamp, _zone(self.s.bot_timezone))

    def _next_morning_timestamp(self, now: float, cfg: LifeEngineConfig) -> float | None:
        base_min = int(now // 60) * 60
        for step in range(5, 24 * 60 + 5, 5):
            cand = float(base_min + step * 60)
            if cfg.is_active_time(self._local(cand)) and not self.store.runtime.is_resting(cand):
                refined = cand
                for back in range(1, 5):
                    prev_t = cand - back * 60
                    if (
                        prev_t > now
                        and cfg.is_active_time(self._local(prev_t))
                        and not self.store.runtime.is_resting(prev_t)
                    ):
                        refined = prev_t
                return refined
        return None

    def _schedule_sleep_wakeup(self, now: float, cfg: LifeEngineConfig) -> float:
        sleep_min = cfg.sleep_min_minutes * 60.0
        sleep_max = cfg.sleep_max_minutes * 60.0
        raw_wake = now + self.rng.uniform(sleep_min, sleep_max)
        morning_ts = self._next_morning_timestamp(now, cfg)
        if morning_ts is not None and morning_ts <= now + sleep_max:
            cap = max(
                morning_ts,
                min(now + sleep_max, morning_ts + self.rng.uniform(0.0, 1800.0)),
            )
            wake_at = max(morning_ts, min(raw_wake, cap))
        else:
            wake_at = raw_wake
        self.next_plan_at = wake_at
        return wake_at

    def _fallback_plan(
        self,
        *,
        local_dt: datetime,
        current_energy: int,
        current_boredom: int,
        events: list[MilestoneEvent],
        bili_surfed: list[dict[str, Any]],
        groups: list[str],
        owner_available: bool,
        desktop_recent: bool,
    ) -> WireLifePlan:
        hour = local_dt.hour
        if bili_surfed or hour in {13, 14, 20, 21}:
            module = "bilibili_surf"
            focus: Literal["LOW", "MEDIUM", "HIGH"] = "MEDIUM"
            mood = "curious"
        else:
            module = "idle"
            focus = "LOW"
            mood = "calm"
        energy = max(15, min(100, current_energy - 5))
        boredom = max(0, min(95, current_boredom + 8))
        intents: list[dict[str, Any]] = []
        if events and owner_available:
            ev = events[0]
            intents.append(
                {
                    "target_module": ev.source_module,
                    "target_scope": f"owner:{self.s.owner_qq_id}",
                    "action_type": f"{ev.source_module}_milestone_share"[:64],
                    "delay_minutes": 10,
                    "ttl_minutes": 90,
                    "intent_note": f"注意到模块 {ev.source_module} 刚有进展动态，找机会自然聊一句",
                    "allow_desktop_care": False,
                }
            )
        elif bili_surfed and groups:
            intents.append(
                {
                    "target_module": "bilibili_surf",
                    "target_scope": f"onebot:{groups[0]}",
                    "action_type": "group_chat_share",
                    "delay_minutes": 15,
                    "ttl_minutes": 90,
                    "intent_note": "刚才刷到一条挺有意思的公开视频，看看群里氛围合适就顺嘴聊两句",
                    "allow_desktop_care": False,
                }
            )
        elif desktop_recent and owner_available:
            intents.append(
                {
                    "target_module": "qq_social",
                    "target_scope": f"owner:{self.s.owner_qq_id}",
                    "action_type": "owner_desktop_warm_care",
                    "delay_minutes": 20,
                    "ttl_minutes": 90,
                    "intent_note": "留意到对方刚在电脑端忙完正事，合适时轻声关心一句注意休息喝水",
                    "allow_desktop_care": True,
                }
            )
        return WireLifePlan.model_validate(
            {
                "current_module": module,
                "focus_level": focus,
                "mood": mood,
                "energy": energy,
                "boredom": boredom,
                "mood_longnick": "",
                "intents": intents,
            }
        )

    async def plan_once(self, now: float | None = None) -> dict[str, Any]:
        ts = self.clock() if now is None else now
        cfg = self.config
        ws = self.service.world_state
        if not cfg.enabled:
            self.next_plan_at = ts + 1800.0
            return {"status": "disabled", "next_plan_at": self.next_plan_at, "intents_enqueued": 0}

        local_dt = self._local(ts)
        if not cfg.is_active_time(local_dt) or self.store.runtime.is_resting(ts):
            current = ws.current
            await ws.update_state(
                current_module="idle",
                focus_level="LOW",
                mood="sleeping",
                energy=min(100, current.energy + 25),
                boredom=max(0, current.boredom - 15),
            )
            wake_at = self._schedule_sleep_wakeup(ts, cfg)
            self.last_plan_at = ts
            self.last_plan_source = "sleeping"
            return {"status": "sleeping", "next_plan_at": wake_at, "intents_enqueued": 0}

        current = ws.current
        events = await ws.unconsumed_events(limit=20)
        groups = _allowlisted_groups(self.s)
        watcher = getattr(self.service, "bili_watcher", None)
        bili_surfed = (
            await watcher.recent_surfed(limit=4, now=ts)
            if watcher is not None and hasattr(watcher, "recent_surfed")
            else []
        )
        modules_reg = getattr(self.service, "modules", None)
        sensors = await modules_reg.collect_sensor_summaries() if modules_reg is not None else {}
        known_modules = (
            {"idle", *(m.module_id for m in modules_reg.all())}
            if modules_reg is not None
            else {"idle", "qq_social", "bilibili_surf", "minecraft"}
        )
        owner_available = bool(self.s.onebot_enabled and self.s.owner_qq_id)
        desktop_info = await _desktop_latest_activity(self.service)
        desktop_recent = bool(
            desktop_info["enabled"]
            and desktop_info["latest_timestamp"] > 0
            and 0 <= (ts - desktop_info["latest_timestamp"]) < cfg.desktop_active_suppress_seconds
        )

        planner_payload: dict[str, Any] = {
            "local_time": local_dt.strftime("%Y-%m-%d %H:%M"),
            "weekday": ("周一", "周二", "周三", "周四", "周五", "周六", "周日")[local_dt.weekday()],
            "current_world_state": {
                "current_module": current.current_module,
                "focus_level": current.focus_level,
                "mood": current.mood,
                "energy": current.energy,
                "boredom": current.boredom,
            },
            "module_sensors": sensors,
            "unconsumed_events": [
                {
                    "id": ev.id,
                    "source_module": ev.source_module,
                    "event_type": ev.event_type,
                    "importance": ev.importance,
                    "payload": ev.payload,
                }
                for ev in events
            ],
            "recent_bili_surfed": bili_surfed,
            "available_groups": [f"onebot:{gid}" for gid in groups[:20]],
            "owner_dm_available": owner_available,
            "desktop_active_recently": desktop_recent,
            "available_modules": sorted(known_modules),
        }

        plan: WireLifePlan | None = None
        plan_source = "local_fallback"
        budget = await self.store.budget_status()
        pro_sub = budget.get("sub_budgets", {}).get("proactive") or {}
        budget_ok = not budget.get("hard_stop") and (
            not pro_sub
            or (pro_sub.get("limit", 0) > 0 and pro_sub.get("used", 0) < pro_sub["limit"])
        )
        if budget_ok:
            subject = self.store.vault.digest("life-planner", self.s.namespace)
            try:
                raw_plan = await self.service.ai.generate(
                    planner_payload,
                    subject,
                    budget_lane="proactive",
                    life_plan=True,
                )
                plan = WireLifePlan.model_validate_json(raw_plan)
                plan_source = "llm"
            except asyncio.CancelledError:
                raise
            except Exception:
                plan = None

        if plan is None:
            plan = self._fallback_plan(
                local_dt=local_dt,
                current_energy=current.energy,
                current_boredom=current.boredom,
                events=events,
                bili_surfed=bili_surfed,
                groups=groups,
                owner_available=owner_available,
                desktop_recent=desktop_recent,
            )
            plan_source = "local_fallback"

        resolved_module = sanitize_sensor_text(plan.current_module, self.s, max_tokens=16) or "idle"
        if resolved_module not in known_modules:
            resolved_module = "idle"
        resolved_focus: Literal["LOW", "MEDIUM", "HIGH"] = plan.focus_level
        resolved_mood = sanitize_sensor_text(plan.mood, self.s, max_tokens=16) or "calm"
        resolved_energy = max(0, min(100, int(plan.energy)))
        resolved_boredom = max(0, min(100, int(plan.boredom)))

        await ws.update_state(
            current_module=resolved_module,
            focus_level=resolved_focus,
            mood=resolved_mood,
            energy=resolved_energy,
            boredom=resolved_boredom,
        )

        for ev in events:
            await ws.mark_event_consumed(ev.id)

        longnick_text = str(plan.mood_longnick or "").strip()
        if longnick_text:
            longnick = getattr(self.service, "longnick", None)
            if longnick is not None:
                if hasattr(longnick, "apply_life_block"):
                    await longnick.apply_life_block(longnick_text)
                elif hasattr(longnick, "apply_patrol"):
                    await longnick.apply_patrol(longnick_text)

        diary = getattr(self.service, "diary", None)
        if diary is not None and hasattr(diary, "world_event"):
            await diary.world_event(
                f"life-block:{int(ts)}",
                resolved_module,
                "life_block",
                (
                    f"切换状态为 {resolved_module}({resolved_focus})"
                    f" · 心情 {resolved_mood}，精力 {resolved_energy}"
                ),
                now=ts,
            )

        enqueued_ids: list[str] = []
        for item in plan.intents[:3]:
            raw_note = str(item.intent_note or "").strip()
            if not raw_note:
                continue
            try:
                _check_no_prewritten_speech(raw_note)
            except ValueError:
                continue
            bounded_note = trim_complete_text(raw_note, 120)
            target_module = (
                sanitize_sensor_text(item.target_module, self.s, max_tokens=16) or "qq_social"
            )
            action_type = (
                sanitize_sensor_text(item.action_type, self.s, max_tokens=24) or "proactive_share"
            )
            raw_scope = str(item.target_scope or "").strip()
            if (
                (target_module == "bilibili_surf" and action_type in {"patrol", "run_once"})
                or (target_module == "minecraft" and action_type in _INTERNAL_MC_ACTIONS)
            ) and not raw_scope:
                resolved_scope = ""
            else:
                resolved_scope = _normalize_target_scope(raw_scope, self.s) or ""
                if not resolved_scope:
                    continue

            delay_sec = max(60.0, min(float(item.delay_minutes) * 60.0, 4.0 * 3600.0))
            ttl_sec = max(600.0, min(float(item.ttl_minutes) * 60.0, 6.0 * 3600.0))
            execute_after = ts + delay_sec
            expire_at = execute_after + ttl_sec
            gate_cond = {"allow_desktop_care": True} if item.allow_desktop_care else {}
            try:
                ticket = IntentTicketCreate(
                    target_module=target_module,
                    target_scope=resolved_scope,
                    action_type=action_type,
                    execute_after=execute_after,
                    expire_at=expire_at,
                    mood=resolved_mood,
                    gate_condition=gate_cond,
                    intent_note=bounded_note,
                )
            except (ValidationError, ValueError):
                continue
            enqueued_ids.append(await ws.enqueue_intent(ticket, now=ts))

        self.next_plan_at = ts + self.rng.uniform(
            cfg.planner_min_minutes * 60.0,
            cfg.planner_max_minutes * 60.0,
        )
        self.last_plan_at = ts
        self.last_plan_source = plan_source
        return {
            "status": "planned",
            "source": plan_source,
            "current_module": resolved_module,
            "focus_level": resolved_focus,
            "mood": resolved_mood,
            "intents_enqueued": len(enqueued_ids),
            "intent_ids": enqueued_ids,
            "next_plan_at": self.next_plan_at,
        }

    async def _record_milestone_diary(self, event: MilestoneEvent, *, now: float) -> None:
        diary = getattr(self.service, "diary", None)
        if diary is None or not hasattr(diary, "world_event"):
            return
        if event.id in self._recorded_milestone_diary_ids:
            return
        self._recorded_milestone_diary_ids[event.id] = now
        while len(self._recorded_milestone_diary_ids) > 512:
            self._recorded_milestone_diary_ids.pop(next(iter(self._recorded_milestone_diary_ids)))
        if event.payload:
            summary_raw = json.dumps(event.payload, ensure_ascii=False, separators=(",", ":"))
            raw_summary = f"重要动态 {event.event_type}: {summary_raw}"
        else:
            raw_summary = f"重要动态 {event.event_type}"
        summary = trim_complete_text(raw_summary, 120)
        try:
            await diary.world_event(
                f"milestone:{event.id}",
                event.source_module,
                event.event_type,
                summary,
                now=now,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("event=milestone_diary_failed")

    async def on_milestone_event(self, event: MilestoneEvent) -> str | None:
        """Derive a zero-Token fuzzy intent ticket on milestone events."""
        if not self.config.enabled:
            return None
        now = max(self.clock(), float(event.created_at or 0.0))
        if event.source_module == "bilibili_surf" and event.event_type == "bili_mc_inspiration":
            await self._record_milestone_diary(event, now=now)
            cooldown_key = "bilibili_surf:mc_goal"
            last_at = self._impulse_cooldowns.get(cooldown_key, 0.0)
            if now - last_at < 600.0:
                return None
            self._impulse_cooldowns[cooldown_key] = now
            if len(self._impulse_cooldowns) > 256:
                self._impulse_cooldowns.pop(next(iter(self._impulse_cooldowns)))
            note = trim_complete_text(
                "刚在B站刷到MC相关建筑或红石视频灵感，同步给MC躯干筹备建造目标",
                120,
            )
            ticket = IntentTicketCreate(
                target_module="minecraft",
                target_scope="",
                action_type="build_plan",
                execute_after=now + 60.0,
                expire_at=now + 7200.0,
                mood=self.service.world_state.current.mood,
                gate_condition={
                    "milestone_event_id": event.id,
                    "bvid": str(event.payload.get("bvid", ""))[:20],
                },
                intent_note=note,
            )
            return await self.service.world_state.enqueue_intent(ticket, now=now)
        if event.importance != "high":
            return None
        await self._record_milestone_diary(event, now=now)
        last_at = self._impulse_cooldowns.get(event.source_module, 0.0)
        if now - last_at < 600.0:
            return None
        if self.s.onebot_enabled and self.s.owner_qq_id:
            target_scope = f"owner:{self.s.owner_qq_id}"
        else:
            groups = _allowlisted_groups(self.s)
            if not groups or not self.s.onebot_enabled:
                return None
            target_scope = f"onebot:{groups[0]}"
        self._impulse_cooldowns[event.source_module] = now
        if len(self._impulse_cooldowns) > 256:
            self._impulse_cooldowns.pop(next(iter(self._impulse_cooldowns)))

        note = trim_complete_text(
            f"模块 {event.source_module} 刚触发重要动态（{event.event_type}），"
            "找个合适契机结合上下文顺手聊一句",
            120,
        )
        action_type = f"{event.source_module}_milestone_share"[:64]
        ticket = IntentTicketCreate(
            target_module=event.source_module,
            target_scope=target_scope,
            action_type=action_type,
            execute_after=now + 90.0,
            expire_at=now + 2700.0,
            mood=self.service.world_state.current.mood,
            gate_condition={"milestone_event_id": event.id, "importance": "high"},
            intent_note=note,
        )
        return await self.service.world_state.enqueue_intent(ticket, now=now)


class GatekeeperWorker:
    """Zero-Token pure Python hard gates for scheduled_intents (60s cadence)."""

    def __init__(
        self,
        service: Any,
        engine: LifeEngine,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.service = service
        self.s: Settings = service.s
        self.store = service.store
        self.engine = engine
        self.clock = clock

    @property
    def config(self) -> LifeEngineConfig:
        return self.engine.config

    async def _expire_stale(self, now: float) -> int:
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE scheduled_intents SET status='expired', updated_at=? "
                "WHERE status='pending' AND expire_at<=?",
                (now, now),
            )
            return int(cur.rowcount or 0)

    async def _defer_intent(self, intent_id: str, next_execute_after: float, *, now: float) -> None:
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE scheduled_intents SET execute_after=?, updated_at=? "
                "WHERE id=? AND status='pending'",
                (next_execute_after, now, intent_id),
            )

    async def _count_today_proactive(self, now: float) -> int:
        day_start, day_end = _day_bounds(now, self.s.bot_timezone)
        async with self.store.lock:
            row = await self.store.one(
                "SELECT COUNT(*) FROM scheduled_intents "
                "WHERE target_scope != '' AND status IN ('done','uncertain') "
                "AND updated_at>=? AND updated_at<?",
                (day_start, day_end),
            )
        return int(row[0] or 0) if row else 0

    async def _group_activity_ok(
        self, group_msg: Message, now: float, cfg: LifeEngineConfig
    ) -> bool:
        hot = getattr(self.service, "hot", None)
        if hot is None:
            return False
        if hasattr(hot, "recent_group_activity"):
            act = hot.recent_group_activity(
                group_msg,
                window_seconds=cfg.group_active_window_seconds,
                now=now,
                bot_ids=self.s.bot_qq_ids,
            )
            recent_items = list(act.get("messages", []))
        else:
            ring = hot.rings.get(hot.key(group_msg))
            if not ring:
                return False
            recent_items = []
            for _, item, _ in reversed(list(ring.messages.values())):
                if (
                    hot.recalled(group_msg, item.event_id)
                    or item.user_id == item.bot_id
                    or item.user_id in self.s.bot_qq_ids
                ):
                    continue
                if (
                    item.created_at > 0
                    and 0.0 <= (now - item.created_at) <= cfg.group_active_window_seconds
                ):
                    recent_items.append(item)
            recent_items.reverse()

        aff = getattr(self.store.runtime, "affection", None)
        aff_enabled = bool(aff is not None and aff.config.enabled)
        eligible_count = 0
        has_high_bond = False
        muted = set(getattr(self.s, "muted_user_ids", set()))

        for item in recent_items:
            is_owner = is_persona_owner(item, self.s) or (
                aff is not None and aff.is_owner_id(item.platform, item.user_id)
            )
            if not is_owner and item.user_id in muted:
                continue
            if aff_enabled and not is_owner:
                caps = await aff.capabilities_for(item)
                if caps.level <= 1:
                    continue
                if caps.level >= 4:
                    has_high_bond = True
            elif is_owner:
                has_high_bond = True
            eligible_count += 1

        effective_min = 1 if has_high_bond else cfg.group_min_recent_messages
        return eligible_count >= effective_min

    async def _owner_dm_gate(
        self, ticket: dict[str, Any], now: float, cfg: LifeEngineConfig
    ) -> tuple[bool, str | None]:
        proactive = self.service.proactive
        if not proactive.configured:
            return False, None
        async with self.store.lock:
            row = await self.store.one(
                "SELECT last_private_at FROM owner_dm_state WHERE scope=?",
                (proactive.scope,),
            )
        last_private_at = float(row["last_private_at"]) if row and row["last_private_at"] else 0.0
        action_type = str(ticket.get("action_type", ""))
        is_generic = action_type in _GENERIC_OWNER_SILENCE_ACTIONS or not action_type.endswith(
            "_milestone_share"
        )
        if last_private_at > 0 and 0.0 <= (now - last_private_at) < 3600.0 and is_generic:
            return False, None

        desktop = await _desktop_latest_activity(self.service)
        desktop_ts = float(desktop.get("latest_timestamp", 0.0) or 0.0)
        desktop_recent = bool(
            desktop.get("enabled")
            and desktop_ts > 0.0
            and 0.0 <= (now - desktop_ts) < cfg.desktop_active_suppress_seconds
        )
        if desktop_recent:
            gate_cond = ticket.get("gate_condition") or {}
            allow_desktop_care = bool(gate_cond.get("allow_desktop_care"))
            if action_type == "owner_silence_care" and not allow_desktop_care:
                return False, None
            if allow_desktop_care or action_type == "owner_desktop_warm_care":
                return True, DESKTOP_CONTEXT_HINT
        return True, None

    async def _evaluate_ticket(
        self,
        ticket: dict[str, Any],
        now: float,
        cfg: LifeEngineConfig,
        *,
        budget: dict[str, Any],
        today_count: int,
    ) -> str:
        ws = self.service.world_state
        intent_id = ticket["id"]
        local_dt = datetime.fromtimestamp(now, _zone(self.s.bot_timezone))

        # Gate 2: Quiet / rest hours
        if self.store.runtime.is_resting(now) or self.service.proactive.config.quiet(local_dt):
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"

        # Gate 3: Attention Lock
        if ws.proactive_suspended() and ticket["target_module"] != ws.current.current_module:
            remaining = ticket["expire_at"] - now
            if remaining <= 120.0:
                await ws.mark_intent(intent_id, "gated_skip", now=now)
                return "gated_skip"
            defer_by = min(900.0, max(60.0, remaining * 0.5))
            await self._defer_intent(intent_id, now + defer_by, now=now)
            return "deferred"

        # Internal non-chat MC goal (zero LLM tokens; gated by node_transport instead of QQ)
        if (
            ticket["target_module"] == "minecraft"
            and ticket["action_type"] in _INTERNAL_MC_ACTIONS
            and not ticket.get("target_scope")
        ):
            nt = getattr(self.service, "node_transport", None)
            if nt is None or not nt.connected():
                if now + 120.0 < ticket["expire_at"]:
                    await self._defer_intent(intent_id, now + 120.0, now=now)
                    return "deferred"
                await ws.mark_intent(intent_id, "gated_skip", now=now)
                return "gated_skip"
            res = await self.engine.jit.execute(ticket, now=now)
            return str(res.get("status", "done"))

        # Gate 4: Daily budget circuit breaker & daily proactive cap
        pro_sub = budget.get("sub_budgets", {}).get("proactive") or {}
        if budget.get("hard_stop") or (
            pro_sub and (pro_sub.get("limit", 0) <= 0 or pro_sub.get("used", 0) >= pro_sub["limit"])
        ):
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"
        if today_count >= cfg.max_proactive_per_day:
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"

        # Internal non-chat module goal (e.g. bilibili_surf patrol)
        if (
            ticket["target_module"] == "bilibili_surf"
            and ticket["action_type"] in {"patrol", "run_once"}
            and not ticket.get("target_scope")
        ):
            res = await self.engine.jit.execute(ticket, now=now)
            return str(res.get("status", "done"))

        # Gate 1b: OneBot transport connected for chat intents
        if not self.service.proactive.connected():
            if now + 120.0 < ticket["expire_at"]:
                await self._defer_intent(intent_id, now + 120.0, now=now)
                return "deferred"
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"

        target_msg = _scope_to_message(
            ticket.get("target_scope", ""),
            self.s,
            event_id=f"gate:{intent_id}",
            created_at=now,
        )
        if target_msg is None:
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"

        # Gate 5: Session concurrency lock avoidance
        scope = self.store.session_scope(target_msg)
        group_busy = False
        if target_msg.kind == "group":
            group_busy = (
                scope in self.store.groups.active
                or (await self.store.groups.state(target_msg)) != "idle"
            )
        busy = (
            scope in self.service.ingress_scheduler.entries
            or scope in self.service.dialogue_scheduler.entries
            or (target_msg.kind == "private" and self.service.owner_dialogue_lock.locked())
            or group_busy
        )
        if busy:
            if now + 150.0 < ticket["expire_at"]:
                await self._defer_intent(intent_id, now + 150.0, now=now)
                return "deferred"
            await ws.mark_intent(intent_id, "gated_skip", now=now)
            return "gated_skip"

        # Gate 6: Group gates
        desktop_hint: str | None = None
        if target_msg.kind == "group":
            cooldown_ok = await ws.group_cooldown_ok(scope, cfg.group_cooldown_seconds, now)
            filters = self.store.runtime.filters
            last_pro_map = getattr(filters, "last_proactive_spoke", None)
            if isinstance(last_pro_map, dict):
                last_pro = float(last_pro_map.get(filters._group_scope(target_msg), 0.0) or 0.0)
                if 0.0 < last_pro <= now + 5.0 and (now - last_pro) < cfg.group_cooldown_seconds:
                    cooldown_ok = False
            speaking = await filters.speaking_state(target_msg)
            if not cooldown_ok or speaking != "ready":
                await ws.mark_intent(intent_id, "gated_skip", now=now)
                return "gated_skip"
            if not await self._group_activity_ok(target_msg, now, cfg):
                await ws.mark_intent(intent_id, "gated_skip", now=now)
                return "gated_skip"
        else:
            # Gate 7: Owner DM + Desktop Antigravity_Memory synergy gate
            ok, desktop_hint = await self._owner_dm_gate(ticket, now, cfg)
            if not ok:
                await ws.mark_intent(intent_id, "gated_skip", now=now)
                return "gated_skip"

        res = await self.engine.jit.execute(ticket, desktop_context_hint=desktop_hint, now=now)
        return str(res.get("status", "done"))

    async def tick(self, now: float | None = None) -> dict[str, Any]:
        ts = self.clock() if now is None else now
        expired = await self._expire_stale(ts)
        cfg = self.config
        if not cfg.enabled:
            return {"status": "disabled", "expired": expired, "checked": 0, "executed": 0}

        due = await self.service.world_state.list_due_intents(now=ts, limit=20)
        if not due:
            return {"status": "idle", "expired": expired, "checked": 0, "executed": 0}

        budget = await self.store.budget_status()
        today_count = await self._count_today_proactive(ts)
        results: dict[str, int] = {}
        executed = 0
        for ticket in due:
            outcome = await self._evaluate_ticket(
                ticket,
                ts,
                cfg,
                budget=budget,
                today_count=today_count,
            )
            results[outcome] = results.get(outcome, 0) + 1
            if outcome in {"done", "uncertain"}:
                executed += 1
                if ticket.get("target_scope"):
                    today_count += 1
                budget = await self.store.budget_status()
        return {
            "status": "ok",
            "expired": expired,
            "checked": len(due),
            "executed": executed,
            "outcomes": results,
        }


class JITExecutor:
    """Just-In-Time context assembly, Tier-1 generation, and safe at-most-once delivery."""

    def __init__(
        self,
        service: Any,
        engine: LifeEngine,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.service = service
        self.s: Settings = service.s
        self.store = service.store
        self.engine = engine
        self.clock = clock

    async def _lookup_milestone_event(
        self, gate_condition: Any
    ) -> tuple[str, dict[str, Any]] | None:
        if not isinstance(gate_condition, dict):
            return None
        event_id = str(gate_condition.get("milestone_event_id") or "").strip()
        if not event_id:
            return None
        async with self.store.lock:
            has_table = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='global_events'"
            )
            if not has_table:
                return None
            row = await self.store.one(
                "SELECT event_type, payload_json FROM global_events WHERE id=?",
                (event_id,),
            )
        if row is None:
            return None
        try:
            parsed = json.loads(row["payload_json"] or "{}")
        except ValueError:
            parsed = {}
        return str(row["event_type"] or ""), parsed if isinstance(parsed, dict) else {}

    async def execute(
        self,
        ticket: dict[str, Any],
        *,
        desktop_context_hint: str | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        ts = self.clock() if now is None else now
        ws = self.service.world_state
        intent_id = ticket["id"]
        await ws.mark_intent(intent_id, "executing", now=ts)

        target_module = str(ticket.get("target_module", ""))
        action_type = str(ticket.get("action_type", ""))
        target_scope = str(ticket.get("target_scope", ""))

        material_summary = ""
        modules_reg = getattr(self.service, "modules", None)
        if (
            target_module == "minecraft"
            and action_type in _INTERNAL_MC_ACTIONS
            and not target_scope
        ):
            mc_mod = modules_reg.get("minecraft") if modules_reg is not None else None
            if mc_mod is None:
                await ws.mark_intent(intent_id, "gated_skip", now=self.clock())
                return {"status": "gated_skip", "reason": "module_unavailable"}
            topic = str(ticket.get("topic") or "").strip()
            if not topic:
                ev_info = await self._lookup_milestone_event(ticket.get("gate_condition"))
                if ev_info is not None:
                    _, ev_payload = ev_info
                    raw_topic = str(
                        ev_payload.get("title")
                        or ev_payload.get("topic")
                        or ev_payload.get("summary")
                        or ""
                    )
                    topic = sanitize_sensor_text(raw_topic, self.s, max_tokens=20)
            goal_res = await mc_mod.dispatch_goal({**ticket, "topic": topic})
            final_mc_status: Literal["done", "gated_skip"] = "done" if goal_res.ok else "gated_skip"
            await ws.mark_intent(intent_id, final_mc_status, now=self.clock())
            return {"status": final_mc_status, "reason": goal_res.reason}

        if modules_reg is not None and target_module == "minecraft" and target_scope:
            mc_mod = modules_reg.get("minecraft")
            sensor_part = await mc_mod.get_sensor_summary() if mc_mod is not None else ""
            ev_info = await self._lookup_milestone_event(ticket.get("gate_condition"))
            if ev_info is not None:
                ev_type, ev_payload = ev_info
                ev_detail = (
                    json.dumps(ev_payload, ensure_ascii=False, separators=(",", ":"))
                    if ev_payload
                    else ""
                )
                ev_part = f"事件:{ev_type}" + (f" {ev_detail}" if ev_detail else "")
                raw_material = " | ".join(p for p in (sensor_part, ev_part) if p)
            else:
                raw_material = sensor_part
            if raw_material:
                material_summary = sanitize_sensor_text(raw_material, self.s, max_tokens=90)
        elif modules_reg is not None and (
            target_module == "bilibili_surf"
            or action_type in {"patrol", "run_once", "bili_patrol", "fetch_material"}
        ):
            module = modules_reg.get(target_module)
            if module is not None:
                goal_res = await module.dispatch_goal(ticket)
                if (
                    target_module == "bilibili_surf"
                    and action_type in {"patrol", "run_once"}
                    and not target_scope
                ):
                    final_status: Literal["done", "gated_skip"] = (
                        "done" if goal_res.ok else "gated_skip"
                    )
                    await ws.mark_intent(intent_id, final_status, now=self.clock())
                    return {"status": final_status, "reason": goal_res.reason}
                if goal_res.summary:
                    material_summary = sanitize_sensor_text(goal_res.summary, self.s, max_tokens=90)

        target_msg = _scope_to_message(
            target_scope,
            self.s,
            event_id=f"jit:{intent_id}",
            created_at=ts,
        )
        if target_msg is None or not authorize(target_msg, self.s):
            await ws.mark_intent(intent_id, "gated_skip", now=self.clock())
            return {"status": "gated_skip", "reason": "unauthorized_scope"}

        private_owner = target_msg.kind == "private" and is_persona_owner(target_msg, self.s)
        session = await self.service.sessions.snapshot(target_msg)
        recent = list(session.get("recent_messages", []))[-10:]
        if not recent and target_msg.kind == "group" and getattr(self.service, "hot", None):
            ring = self.service.hot.rings.get(self.service.hot.key(target_msg))
            if ring:
                for seq, item, _ in list(ring.messages.values())[-10:]:
                    if self.service.hot.recalled(target_msg, item.event_id):
                        continue
                    recent.append(
                        {
                            "seq": seq,
                            "role": "assistant" if item.user_id == item.bot_id else "user",
                            "text": item.text,
                            "speaker": item.sender_nickname
                            or ("吉米奈喵" if item.user_id == item.bot_id else "群友"),
                            "user_id": item.user_id,
                            "event_id": item.event_id,
                            "created_at": item.created_at,
                        }
                    )

        batch_affection: dict[str, dict[str, Any]] = {}
        aff = getattr(self.store.runtime, "affection", None)
        if aff is not None and aff.config.enabled:
            seen_uids: set[str] = set()
            for item in reversed(recent):
                uid = str(item.get("user_id", "")).strip()
                if (
                    not uid
                    or uid in seen_uids
                    or uid in {"0", target_msg.bot_id}
                    or uid in self.s.bot_qq_ids
                ):
                    continue
                seen_uids.add(uid)
                try:
                    prof = await aff.profile(target_msg.platform, uid)
                    batch_affection[uid] = {
                        "score": prof["score"],
                        "lv": prof["lv"],
                        "tier": prof["tier"],
                        "attitude": prof["attitude"],
                    }
                except Exception:
                    continue
                if len(batch_affection) >= 6:
                    break

        ws_summary = await ws.chat_sensor_payload()
        jit_intent: dict[str, Any] = {
            "intent_id": intent_id,
            "target_module": target_module,
            "action_type": action_type,
            "mood": ticket.get("mood") or ws.current.mood,
            "intent_note": ticket.get("intent_note") or {},
        }
        if material_summary:
            jit_intent["module_material"] = material_summary
        if desktop_context_hint:
            jit_intent["desktop_context_hint"] = desktop_context_hint

        payload: dict[str, Any] = {
            "current_message": JIT_TRIGGER_MESSAGE,
            "current_sender": {
                "nickname": "BOISER" if private_owner else "系统主动触发",
                "qq": target_msg.user_id,
            },
            "surface": target_msg.kind,
            "directly_addressed": True,
            "recent_messages": recent,
            "rolling_summary": session.get("rolling_summary", ""),
            "session_epoch": session["epoch"],
            "session_enabled": False,
            "history_revision": session.get("history_revision", 0),
            "interaction_chains": {},
            "allowed_reply_ids": [],
            "jit_proactive_intent": jit_intent,
        }
        if batch_affection:
            payload["batch_affection"] = batch_affection
        if ws_summary:
            payload["world_state_summary"] = ws_summary

        watcher = getattr(self.service, "bili_watcher", None)
        if watcher is not None and hasattr(watcher, "recent_surfed"):
            surfed = await watcher.recent_surfed(limit=3, now=ts)
            if surfed:
                payload["recent_bili_surfed"] = surfed

        if private_owner:
            cloud_mem = getattr(self.service, "cloud_memory", None)
            if cloud_mem is not None:
                mem_data = await asyncio.to_thread(
                    cloud_mem.for_message, target_msg, "", recall=False
                )
                if mem_data:
                    payload.update(mem_data)
            notebook = getattr(self.service, "notebook", None)
            if notebook is not None:
                snap = notebook.for_message(target_msg)
                if snap:
                    payload["owner_notebook_snapshot"] = snap
            dual_data = await self.service.sessions.dual.prefetch(
                target_msg,
                "",
                recall=True,
                related_user_ids=[self.s.owner_qq_id],
            )
            if dual_data:
                payload["dual_memory_data"] = dual_data
        else:
            dual_data = await self.service.sessions.dual.prefetch(
                target_msg,
                "",
                recall=False,
                related_user_ids=list(batch_affection.keys()),
            )
            if dual_data:
                payload["dual_memory_data"] = dual_data

        subject = self.store.subject(target_msg)
        scenario = Scenario.OWNER_PRIVATE if private_owner else Scenario.FAST_CHAT
        route = RouteDecision("chat", tier=1, need_memory_recall=private_owner)
        try:
            generated = await self.service.ai.generate(
                payload,
                subject,
                scenario=scenario,
                message=target_msg,
                route=route,
                budget_lane="proactive",
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            await ws.mark_intent(intent_id, "gated_skip", now=self.clock())
            return {"status": "gated_skip", "reason": "generation_failed"}

        delivery = await deliver_proactive_jit(
            self.service,
            target_msg,
            generated,
            reply_id=f"jit:{intent_id}",
        )
        end_ts = self.clock()
        deliv_status = str(delivery.get("status", ""))
        if deliv_status == "sent":
            await ws.mark_intent(intent_id, "done", now=end_ts)
            return {"status": "done", "delivery": delivery}
        if deliv_status == "uncertain":
            await ws.mark_intent(intent_id, "uncertain", now=end_ts)
            return {"status": "uncertain", "delivery": delivery}
        if deliv_status == "not_dispatched":
            if end_ts + 120.0 < ticket["expire_at"]:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE scheduled_intents SET status='pending', execute_after=?, "
                        "updated_at=? WHERE id=?",
                        (end_ts + 120.0, end_ts, intent_id),
                    )
                return {"status": "deferred", "delivery": delivery}
            await ws.mark_intent(intent_id, "expired", now=end_ts)
            return {"status": "expired", "delivery": delivery}
        await ws.mark_intent(intent_id, "gated_skip", now=end_ts)
        return {"status": "gated_skip", "delivery": delivery}


class LifeEngine:
    """Coordinates LifeBlockPlanner, GatekeeperWorker, and JITExecutor with healthcheck safety."""

    def __init__(
        self,
        service: Any,
        *,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
    ) -> None:
        self.service = service
        self.s: Settings = service.s
        self.store = service.store
        self.clock = clock
        self.rng = rng or random.SystemRandom()
        self.env_config = LifeEngineConfig.from_settings(self.s)
        self.config: LifeEngineConfig = self.env_config
        self.overridden: bool = False
        self.initialized: bool = False
        self._running: bool = False
        self._init_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()
        self._plan_lock = asyncio.Lock()
        self._gate_lock = asyncio.Lock()
        self.wakeup = asyncio.Event()
        self.planner = LifeBlockPlanner(service, self, clock=self.clock, rng=self.rng)
        self.gatekeeper = GatekeeperWorker(service, self, clock=self.clock)
        self.jit = JITExecutor(service, self, clock=self.clock)

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.lock:
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='life_engine'"
                )
            if row:
                saved = self.store.vault.open("admin:life_engine", row[0])
                merged = (
                    {**self.env_config.model_dump(), **saved} if isinstance(saved, dict) else saved
                )
                self.config = LifeEngineConfig.model_validate(merged)
                self.overridden = True
            else:
                self.config = self.env_config
                self.overridden = False
            world_state = getattr(self.service, "world_state", None)
            if world_state is not None:
                async with self.store.lock:
                    has_intents_table = await self.store.one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='scheduled_intents'"
                    )
                if not has_intents_table and hasattr(world_state, "initialize"):
                    await world_state.initialize()
                world_state.subscribe(self.planner.on_milestone_event)
            if self.planner.next_plan_at <= 0.0:
                self.planner.next_plan_at = self.clock() + 120.0
            self.initialized = True

    async def status(self) -> dict[str, Any]:
        await self.initialize()
        proactive_sent_today = await self.gatekeeper._count_today_proactive(self.clock())
        return {
            "config": self.config.model_dump(),
            "source": "admin" if self.overridden else "env",
            "environment": self.env_config.model_dump(),
            "timezone": self.s.bot_timezone,
            "next_plan_at": self.planner.next_plan_at,
            "last_plan_at": self.planner.last_plan_at,
            "last_plan_source": self.planner.last_plan_source,
            "proactive_sent_today": proactive_sent_today,
            "running": self._running,
        }

    async def set_config(self, value: LifeEngineConfig | dict[str, Any] | None) -> dict[str, Any]:
        await self.initialize()
        async with self._config_lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='life_engine'")
                self.config = self.env_config
                self.overridden = False
            else:
                cfg = LifeEngineConfig.model_validate(value)
                await self.store.runtime.save_config("life_engine", cfg.model_dump())
                self.config = cfg
                self.overridden = True
        self.wakeup.set()
        return await self.status()

    async def trigger_plan_once(self, *, now: float | None = None) -> dict[str, Any]:
        await self.initialize()
        async with self._plan_lock:
            return await self.planner.plan_once(now=now)

    async def tick_gatekeeper_once(self, *, now: float | None = None) -> dict[str, Any]:
        await self.initialize()
        async with self._gate_lock:
            return await self.gatekeeper.tick(now=now)

    async def run(self) -> None:
        self._running = True
        last_gate_tick = 0.0
        while True:
            try:
                if not self.initialized:
                    await self.initialize()
                now = self.clock()
                if self.config.enabled:
                    if self.planner.next_plan_at <= 0.0 or now >= self.planner.next_plan_at:
                        await self.trigger_plan_once(now=now)
                    if now - last_gate_tick >= 60.0:
                        await self.tick_gatekeeper_once(now=now)
                        last_gate_tick = self.clock()
            except asyncio.CancelledError:
                self._running = False
                raise
            except Exception:
                log.warning("event=life_engine_error")
            self.wakeup.clear()
            try:
                await asyncio.wait_for(self.wakeup.wait(), 60.0)
            except TimeoutError:
                pass
