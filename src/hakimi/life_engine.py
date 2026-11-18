"""Autonomous LifeEngine: LifeBlockPlanner, zero-Token GatekeeperWorker, and JITExecutor."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from datetime import time as day_time
from functools import lru_cache
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .budget_policy import PROACTIVE_LANE, paid_lane_available
from .config import Settings
from .gemini import WireLifePlan
from .life_config_compat import (
    LegacyScheduleAccess,
    import_old_proactive,
    legacy_view,
    migrate_flat,
)
from .life_defaults import fallback_plan
from .life_rules import RuleDispatcher
from .modules.actions import ActionCall, GateDecision, Mode, Outcome, reject_reason
from .modules.base import sanitize_sensor_text
from .modules.qq_delivery import JIT_TRIGGER_MESSAGE as JIT_TRIGGER_MESSAGE
from .modules.qq_policy import DESKTOP_CONTEXT_HINT as DESKTOP_CONTEXT_HINT
from .proactive_policy import hours_bounds, within_hours
from .reply_style import trim_complete_text
from .world_state import (
    IntentTicketCreate,
    MilestoneEvent,
    PrivateEventScope,
    _check_no_prewritten_speech,
    private_event_for_owner,
)

log = logging.getLogger("hakimi")


class LifeEngineConfig(LegacyScheduleAccess, BaseModel):
    """Persistent configuration for LifeBlockPlanner and GatekeeperWorker."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    action_modes: dict[str, Mode] = Field(default_factory=dict)
    enabled: bool = True
    quiet_hours: str = "23:30-08:30"
    active_hours: str = "09:00-01:00"
    planner_min_minutes: int = Field(default=120, ge=30, le=720)
    planner_max_minutes: int = Field(default=240, ge=60, le=720)
    sleep_min_minutes: int = Field(default=360, ge=120, le=960)
    sleep_max_minutes: int = Field(default=480, ge=180, le=960)
    max_proactive_per_day: int = Field(default=6, ge=0, le=30)

    module_settings: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _read_legacy_flat_settings(cls, value):
        return migrate_flat(value)

    @field_validator("active_hours", "quiet_hours")
    @classmethod
    def _validate_hours(cls, value: str) -> str:
        hours_bounds(value)
        return value

    @model_validator(mode="after")
    def _validate_ranges(self) -> Self:
        if self.planner_max_minutes < self.planner_min_minutes:
            raise ValueError("planner_max_minutes must be >= planner_min_minutes")
        if self.sleep_max_minutes < self.sleep_min_minutes:
            raise ValueError("sleep_max_minutes must be >= sleep_min_minutes")
        return self

    @classmethod
    def from_settings(cls, settings: Settings, registry) -> Self:
        return cls(
            quiet_hours=settings.quiet_hours,
            module_settings={
                module.module_id: module.schedule_options.from_settings(settings).model_dump()
                for module in registry.all()
                if module.schedule_options.model_fields
            },
            action_modes={
                spec.name: spec.default_mode
                for spec in registry.catalog.actions.values()
                if spec.default_mode != "active"
            },
        )

    def is_active_time(self, local: datetime) -> bool:
        return within_hours(local.hour * 60 + local.minute, hours_bounds(self.active_hours))

    def quiet(self, local: datetime) -> bool:
        return within_hours(local.hour * 60 + local.minute, hours_bounds(self.quiet_hours))


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


@lru_cache(maxsize=64)
def _digest_window(day: date, zone: ZoneInfo, active: str, quiet: str, rest: str | None):
    """Last 15 consecutive available minutes of a local reporting day, including DST days."""
    start = int(datetime.combine(day, day_time.min, zone).timestamp())
    end = int(datetime.combine(day + timedelta(days=1), day_time.min, zone).timestamp())
    active_span, quiet_span = hours_bounds(active), hours_bounds(quiet)
    rest_span = hours_bounds(rest) if rest else None
    closing = None
    for stamp in range(end - 60, start - 1, -60):
        local = datetime.fromtimestamp(stamp, zone)
        minute = local.hour * 60 + local.minute
        available = (
            within_hours(minute, active_span)
            and not within_hours(minute, quiet_span)
            and not (rest_span and within_hours(minute, rest_span))
        )
        if available:
            if closing is None:
                closing = stamp + 60
            if closing - stamp >= 900:
                return stamp, closing
        elif closing is not None:
            return stamp + 60, closing
    return (start, closing) if closing is not None else None


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

    async def plan_once(self, now: float | None = None) -> dict[str, Any]:
        ts = self.clock() if now is None else now
        cfg = self.config
        ws = self.service.world_state
        if not cfg.enabled:
            self.next_plan_at = ts + 1800.0
            return {"status": "disabled", "next_plan_at": self.next_plan_at, "intents_enqueued": 0}

        local_dt = self._local(ts)
        if (
            not cfg.is_active_time(local_dt)
            or cfg.quiet(local_dt)
            or self.store.runtime.is_resting(ts)
        ):
            current = ws.current
            await ws.update_state(
                current_module=current.current_module if ws.proactive_suspended() else "idle",
                focus_level=current.focus_level if ws.proactive_suspended() else "LOW",
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
        modules_reg = self.service.modules
        module_data = await modules_reg.context_data(limit=4, now=ts)
        sensors = await modules_reg.collect_sensor_summaries()
        known_modules = {"idle", *(m.module_id for m in modules_reg.all())}
        planning_context = {}
        for module in modules_reg.all():
            planning_context.update(await module.planning_context(self.service, cfg, ts))

        budget = await self.store.budget_status()
        available_actions = [
            action
            for action in self.engine.catalog.view(cfg.action_modes, planner=True)
            if not action["budget_lane"] or paid_lane_available(budget, action["budget_lane"])
        ]
        offered = {action["name"] for action in available_actions}
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
                if not ev.payload.get("private_first")
            ],
            **module_data,
            "available_actions": available_actions,
            **planning_context,
            "available_modules": sorted(known_modules),
        }

        plan: WireLifePlan | None = None
        plan_source = "local_fallback"
        planner_funded = paid_lane_available(budget, PROACTIVE_LANE)
        if planner_funded:
            subject = self.store.vault.digest("life-planner", self.s.namespace)
            try:
                raw_plan = await self.service.ai.generate(
                    planner_payload,
                    subject,
                    budget_lane=PROACTIVE_LANE,
                    life_plan=True,
                )
                plan = WireLifePlan.model_validate_json(raw_plan)
                plan_source = "llm"
            except asyncio.CancelledError:
                raise
            except Exception:
                plan = None

        if plan is None:
            plan = await fallback_plan(
                self.engine, planner_payload, current, local_dt, ts, offered, planner_funded
            )
            plan_source = "local_fallback"

        resolved_module = sanitize_sensor_text(plan.current_module, self.s, max_tokens=16) or "idle"
        if resolved_module not in known_modules:
            resolved_module = "idle"
        resolved_focus: Literal["LOW", "MEDIUM", "HIGH"] = plan.focus_level
        if ws.proactive_suspended():
            resolved_module, resolved_focus = ws.current.current_module, ws.current.focus_level
        elif resolved_focus == "HIGH":
            resolved_focus = "MEDIUM"
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
            raw_scope = item.target_scope.strip()
            resolved_scope = modules_reg.normalize_recipient(raw_scope) if raw_scope else ""
            if resolved_scope is None:
                continue
            catalog = self.engine.catalog
            target_module, action_type = item.target_module, item.action_type
            if item.action_name:
                target_module = catalog.owners.get(item.action_name, "")
                spec = catalog.actions.get(item.action_name)
                if not spec:
                    continue
                alias = next((a for a in spec.aliases if a[0] == target_module), None)
                action_type = alias[1] if alias else item.action_name
            try:
                arguments = json.loads(item.arguments_json)
                spec, args = self.engine.resolve(
                    {
                        "action_name": item.action_name,
                        "origin": "planner",
                        "arguments": arguments,
                        "target_module": target_module,
                        "action_type": action_type,
                        "target_scope": resolved_scope,
                    }
                )
            except ValueError:
                continue

            if spec.name not in offered:
                continue

            delay_sec = max(60.0, min(float(item.delay_minutes) * 60.0, 4.0 * 3600.0))
            ttl_sec = max(600.0, min(float(item.ttl_minutes) * 60.0, 6.0 * 3600.0))
            execute_after = ts + delay_sec
            expire_at = execute_after + ttl_sec
            gate_cond = {"allow_desktop_care": True} if item.allow_desktop_care else {}
            try:
                ticket = IntentTicketCreate(
                    origin="planner",
                    action_name=spec.name,
                    arguments=args.model_dump(),
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

    async def on_milestone_event(self, event: MilestoneEvent) -> str | None:
        return await self.engine.rules.notify(event, self.clock())


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
                "UPDATE scheduled_intents SET status='expired', result_reason='ttl_expired', "
                "updated_at=? "
                "WHERE status='pending' AND expire_at<=?",
                (now, now),
            )
            return int(cur.rowcount or 0)

    def quota_for(self, spec, scope):
        owner = bool(self.s.owner_qq_id and scope == f"owner:{self.s.owner_qq_id}")
        return next((rule for rule in spec.quota_rules() if not rule.owner_only or owner), None)

    def is_unlimited(self, spec, rule, cfg):
        if not rule.unlimited_option:
            return False
        options = self.engine.options_for(spec, cfg)
        return getattr(options, rule.unlimited_option)

    async def daily_usage(self, now: float, cfg: LifeEngineConfig) -> dict[str, int]:
        start, end = _day_bounds(now, self.s.bot_timezone)
        catalog = self.engine.catalog
        usage = {
            q.counter: 0
            for spec in catalog.actions.values()
            for q in spec.quota_rules()
            if q.counter
        }
        usage.update(total=0, counted=0)
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT action_name,target_module,action_type,target_scope,COUNT(*) AS n "
                "FROM scheduled_intents "
                "WHERE status IN ('done','uncertain') AND updated_at>=? AND updated_at<? "
                "GROUP BY action_name,target_module,action_type,target_scope",
                (start, end),
            )
        for row in rows:
            name = row["action_name"] or catalog.old_name(row)
            spec = catalog.actions.get(name)
            if not spec:
                # Preserve historical deliveries from actions no longer registered.
                if row["target_scope"]:
                    usage["total"] += row["n"]
                    usage["counted"] += row["n"]
                continue
            rule = self.quota_for(spec, row["target_scope"])
            if rule is None:
                continue
            usage["total"] += row["n"]
            if rule.counter:
                usage[rule.counter] += row["n"]
            if not self.is_unlimited(spec, rule, cfg):
                usage["counted"] += row["n"]
        return usage

    async def daily_limit_allows(
        self, action: str, scope: str, now: float, cfg: LifeEngineConfig
    ) -> bool:
        spec = self.engine.catalog.actions[action]
        rule = self.quota_for(spec, scope)
        if rule is None or self.is_unlimited(spec, rule, cfg):
            return True
        return (await self.daily_usage(now, cfg))["counted"] < cfg.max_proactive_per_day

    async def _evaluate_ticket(
        self,
        ticket: dict[str, Any],
        now: float,
        cfg: LifeEngineConfig,
    ) -> str:
        ws = self.service.world_state
        intent_id = ticket["id"]
        local_dt = datetime.fromtimestamp(now, _zone(self.s.bot_timezone))

        async def reject(reason):
            await ws.mark_intent(intent_id, "gated_skip", reason=reason, now=now)
            return "gated_skip"

        async def defer(reason, seconds):
            if now + seconds >= ticket["expire_at"]:
                return await reject(reason)
            await ws.mark_intent(
                intent_id, "pending", reason=reason, now=now, retry_at=now + seconds
            )
            return "deferred"

        await self.engine.compare_shadow(ticket)
        try:
            spec, _ = self.engine.resolve(ticket)
        except ValueError as exc:
            return await reject(reject_reason(exc))
        if self.store.runtime.is_resting(now) or cfg.quiet(local_dt):
            return await reject("quiet_hours")
        if spec.window == "closing" and not self.engine.closing_due(now):
            return await reject("outside_digest_window")
        if spec.window == "active" and not cfg.is_active_time(local_dt):
            return await reject("outside_active_hours")
        if (
            not spec.interrupts_focus
            and ws.proactive_suspended()
            and self.engine.catalog.owners[spec.name] != ws.current.current_module
        ):
            try:
                evidence = await self.engine.evidence(ticket) if spec.social else {}
            except ValueError:
                return await reject("private_event_scope")
            if evidence.get("source_module") != ws.current.current_module:
                remaining = ticket["expire_at"] - now
                if remaining <= 120:
                    return await reject("attention_lock")
                return await defer("attention_lock", min(900, max(60, remaining * 0.5)))
        if spec.budget_lane is not None:
            budget = await self.store.budget_status()
            if not paid_lane_available(budget, spec.budget_lane):
                return await reject("budget_limit")
        if not await self.daily_limit_allows(spec.name, ticket["target_scope"], now, cfg):
            return await reject("daily_proactive_limit")
        if reason := spec.unavailable():
            return await defer(reason, 120)

        decision = (
            await spec.preflight(ticket, now, self.engine.options_for(spec, cfg))
            if spec.preflight
            else GateDecision()
        )
        if decision.reason:
            if decision.delay:
                return await defer(decision.reason, decision.delay)
            return await reject(decision.reason)
        desktop_hint = decision.desktop_hint
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

        results: dict[str, int] = {}
        executed = 0
        for ticket in due:
            outcome = await self._evaluate_ticket(
                ticket,
                ts,
                cfg,
            )
            results[outcome] = results.get(outcome, 0) + 1
            if outcome in {"done", "uncertain"}:
                executed += 1
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
        ticket = await ws.claim(intent_id, ts)
        if ticket is None:
            return {"status": "not_claimed"}
        try:
            spec, args = self.engine.resolve(ticket)
        except ValueError as exc:
            result = Outcome("gated_skip", reject_reason(exc))
        else:
            try:
                evidence = await self.engine.evidence(ticket)

                def still_allowed() -> bool:
                    current = max(ts, self.clock())
                    local = datetime.fromtimestamp(current, _zone(self.s.bot_timezone))
                    return (
                        self.engine.config.enabled
                        and (
                            spec.window == "any"
                            or (
                                self.engine.config.is_active_time(local)
                                and (spec.window != "closing" or self.engine.closing_due(current))
                            )
                        )
                        and not self.engine.config.quiet(local)
                        and not self.store.runtime.is_resting(current)
                        and self.engine.config.action_modes.get(spec.name) != "off"
                        and (
                            not spec.social
                            or (
                                self.service.modules.normalize_recipient(ticket["target_scope"])
                                == ticket["target_scope"]
                                and self.service.modules.permits_recipient(
                                    ticket["target_scope"], self.engine.config
                                )
                            )
                        )
                        and current < ticket["expire_at"]
                    )

                if still_allowed():
                    result = await spec.run(
                        ActionCall(ticket, args, ts, evidence, desktop_context_hint, still_allowed)
                    )
                else:
                    result = Outcome("gated_skip", "action_no_longer_eligible")
            except PrivateEventScope:
                result = Outcome("gated_skip", "private_event_scope")
            except asyncio.CancelledError:
                await asyncio.shield(
                    ws.mark_intent(
                        intent_id,
                        "uncertain",
                        expected="executing",
                        reason="interrupted",
                        now=self.clock(),
                    )
                )
                raise
            except Exception:
                log.warning("event=life_action_failed action=%s", spec.name)
                result = Outcome("uncertain", "handler_interrupted")
        end = self.clock()
        status = result.status
        retry_at = None
        if status == "not_dispatched":
            retry_at = end + 120 if end + 120 < ticket["expire_at"] else None
            status = "pending" if retry_at else "expired"
        await ws.mark_intent(
            intent_id,
            status,
            expected="executing",
            reason=result.reason,
            retry_at=retry_at,
            now=end,
        )
        return {
            "status": "deferred" if status == "pending" else status,
            "reason": result.reason,
            **result.details,
        }


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
        self.env_config = LifeEngineConfig.from_settings(self.s, service.modules)
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
        self.catalog = service.modules.catalog
        self.rules = RuleDispatcher(self, service.modules)

    def options_for(self, spec, cfg):
        module = self.service.modules.get(self.catalog.owners[spec.name])
        return module.scheduling(cfg)

    def resolve(self, ticket: dict, *, admin: bool = False):
        if (ticket.get("origin") == "admin") != admin:
            raise ValueError("action_origin_forbidden")
        spec = self.catalog.resolve(ticket)
        mode = self.config.action_modes.get(spec.name, "active")
        if mode == "off":
            raise ValueError("action_disabled")
        if mode in {"legacy", "shadow"}:
            spec = self.catalog.resolve(ticket, compatibility=True)
        if spec.owner_only and ticket.get("target_scope") != "owner:" + self.s.owner_qq_id:
            raise ValueError("unauthorized_scope")
        if spec.social:
            scope = ticket["target_scope"]
            if self.service.modules.normalize_recipient(scope) != scope:
                raise ValueError("unauthorized_scope")
            if not self.service.modules.permits_recipient(scope, self.config):
                raise ValueError("action_disabled")
        return spec, spec.arguments.model_validate(ticket.get("arguments", {}))

    async def compare_shadow(self, ticket: dict):
        name = ticket.get("action_name") or self.catalog.old_name(ticket)
        if self.config.action_modes.get(name) != "shadow":
            return
        results = []
        for compat in (True, False):
            try:
                spec = self.catalog.resolve(ticket, compatibility=compat)
                spec.arguments.model_validate(ticket.get("arguments", {}))
                results.append(spec.name)
            except ValueError as exc:
                results.append(reject_reason(exc))
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE scheduled_intents SET shadow_result=? WHERE id=? AND status='pending'",
                (" → ".join(results), ticket["id"]),
            )

    async def evidence(self, ticket: dict) -> dict:
        # A planner cannot supply a reference; pre-migration rows used a local gate_condition.
        if ticket["origin"] == "legacy":
            reference = (ticket.get("gate_condition") or {}).get("milestone_event_id")
            if not reference:
                return {}
            # Old internal build plans referenced the video event by ID; social evidence
            # must match its claimed source before it can cross a focus lock.
            async with self.store.lock:
                row = await self.store.one(
                    "SELECT * FROM global_events WHERE id=? AND (?='' OR source_module=?)",
                    (reference, ticket["target_scope"], ticket["target_module"]),
                )
        elif ticket["origin"] == "rule" and ticket["source_event_id"]:
            async with self.store.lock:
                row = await self.store.one(
                    "SELECT e.* FROM global_events e JOIN life_rule_receipts r ON r.event_id=e.id "
                    "WHERE r.intent_id=? AND e.id=? AND e.source_module=?",
                    (ticket["id"], ticket["source_event_id"], ticket["source_module"]),
                )
        else:
            return {}
        if not row:
            return {}
        payload = json.loads(row["payload_json"])
        if payload.get("private_first") and not private_event_for_owner(
            payload, ticket["target_scope"], self.s
        ):
            raise PrivateEventScope("private_event_scope")
        return {
            "source_module": row["source_module"],
            "event_type": row["event_type"],
            "payload": payload,
        }

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='life_engine'"
                )
                legacy = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='proactive'"
                )
                saved = self.store.vault.open("admin:life_engine", row[0]) if row else {}
                saved = migrate_flat(saved)
                merged = {**self.env_config.model_dump(), **saved}
                merged["module_settings"] = self.env_config.module_settings | saved.get(
                    "module_settings", {}
                )
                merged["action_modes"] = self.env_config.action_modes | saved.get(
                    "action_modes", {}
                )
                if legacy:
                    import_old_proactive(
                        self.store.vault.open("admin:proactive", legacy[0]), saved, merged
                    )
                merged["action_modes"] = {
                    name: mode
                    for name, mode in merged["action_modes"].items()
                    if name in self.catalog.actions
                }
                self.config = LifeEngineConfig.model_validate(merged)
                self.overridden = bool(row or legacy)
                if legacy:
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO admin_config VALUES ('life_engine',?)",
                        (self.store.vault.seal("admin:life_engine", self.config.model_dump()),),
                    )
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='proactive'")
            world_state = getattr(self.service, "world_state", None)
            if world_state is not None:
                async with self.store.lock:
                    has_intents_table = await self.store.one(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='scheduled_intents'"
                    )
                if not has_intents_table and hasattr(world_state, "initialize"):
                    await world_state.initialize()
                await self.rules.initialize()
                world_state.subscribe(self.planner.on_milestone_event)
            if self.planner.next_plan_at <= 0.0:
                self.planner.next_plan_at = self.clock() + 120.0
            self.initialized = True

    async def status(self) -> dict[str, Any]:
        await self.initialize()
        usage = await self.gatekeeper.daily_usage(self.clock(), self.config)
        return {
            "actions": self.catalog.view(self.config.action_modes),
            "config": legacy_view(self.config, self.service.modules),
            "source": "admin" if self.overridden else "env",
            "environment": self.env_config.model_dump(),
            "timezone": self.s.bot_timezone,
            "next_plan_at": self.planner.next_plan_at,
            "next_digest_at": await self.next_digest_at(self.clock()),
            "last_plan_at": self.planner.last_plan_at,
            "last_plan_source": self.planner.last_plan_source,
            "proactive_sent_today": usage["total"],
            "daily_activity_usage": usage,
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
                for module_id in cfg.module_settings:
                    module = self.service.modules.get(module_id)
                    if module is None:
                        if cfg.module_settings[module_id] != self.config.module_settings.get(
                            module_id
                        ):
                            raise ValueError("unknown_module_settings")
                    else:
                        module.scheduling(cfg)
                for name, mode in cfg.action_modes.items():
                    spec = self.catalog.actions.get(name)
                    if not spec or (mode in {"legacy", "shadow"} and not spec.aliases):
                        raise ValueError("unsupported_action_mode")
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
            ts = self.clock() if now is None else now
            if self.config.enabled:
                await self.rules.catch_up(ts)
                await self.schedule_due(ts)
            return await self.gatekeeper.tick(now=ts)

    def closing_window(self, local_day: date) -> tuple[int, int] | None:
        rest = self.store.runtime.rest_hours
        return _digest_window(
            local_day,
            _zone(self.s.bot_timezone),
            self.config.active_hours,
            self.config.quiet_hours,
            rest.rest_hours if rest.enabled else None,
        )

    def closing_due(self, stamp: float) -> bool:
        window = self.closing_window(
            datetime.fromtimestamp(stamp, _zone(self.s.bot_timezone)).date()
        )
        return bool(window and window[0] <= stamp < window[1])

    # Public 2.1 compatibility entry points for the QQ delivery service.
    digest_window = closing_window
    digest_due = closing_due

    async def next_digest_at(self, now):
        from .life_config_compat import next_owner_digest

        return await next_owner_digest(self, now)

    def slot_window(self, module, slot, day):
        at = slot.effective_time(module.scheduling(self.config))
        if at is None:
            return self.closing_window(day)
        start = datetime.combine(
            day, day_time.fromisoformat(at), _zone(self.s.bot_timezone)
        ).timestamp()
        return start, start + slot.ttl

    async def next_slot_at(self, module, slot, now):
        if not self.config.enabled or (slot.enabled and not slot.enabled()):
            return None
        if not self.slot_available(module, slot):
            return None
        today = datetime.fromtimestamp(now, _zone(self.s.bot_timezone)).date()
        for day in (today, today + timedelta(days=1)):
            window = self.slot_window(module, slot, day)
            if (
                not window
                or now >= window[1]
                or (slot.attempted and await slot.attempted(day.isoformat()))
            ):
                continue
            async with self.store.lock:
                finished = await self.store.one(
                    "SELECT 1 FROM scheduled_intents WHERE dedupe_key=? "
                    "AND status NOT IN ('pending','executing')",
                    (f"timer:{slot.name}:{day}:{slot.at or int(window[0])}",),
                )
            if not finished:
                return max(now, window[0])
        return None

    def slot_available(self, module, slot):
        spec = self.catalog.actions.get(slot.name)
        if not spec or self.catalog.owners[slot.name] != module.module_id:
            return False
        try:
            self.resolve(
                dict(
                    action_name=slot.name,
                    target_module=module.module_id,
                    action_type=slot.name,
                    target_scope=slot.target_scope,
                    origin="timer",
                )
            )
        except ValueError:
            return False
        return True

    async def schedule_due(self, now: float) -> None:
        local = datetime.fromtimestamp(now, _zone(self.s.bot_timezone))
        for module in self.service.modules.all():
            for slot in module.schedules():
                window = self.slot_window(module, slot, local.date())
                if not window or not window[0] <= now < window[1]:
                    continue
                if not self.slot_available(module, slot) or (slot.enabled and not slot.enabled()):
                    continue
                start, end = window
                ticket = IntentTicketCreate(
                    action_name=slot.name,
                    target_module=module.module_id,
                    action_type=slot.name,
                    origin="timer",
                    target_scope=slot.target_scope,
                    execute_after=start,
                    expire_at=end,
                    dedupe_key=f"timer:{slot.name}:{local.date()}:{slot.at or int(start)}",
                )
                await self.service.world_state.enqueue_intent(ticket, now=now)

    async def run(self) -> None:
        self._running = True
        last_gate_tick = 0.0
        while True:
            try:
                if not self.initialized:
                    await self.initialize()
                now = self.clock()
                if self.config.enabled:
                    if now - last_gate_tick >= 60.0:
                        await self.tick_gatekeeper_once(now=now)
                        last_gate_tick = self.clock()
                    now = self.clock()
                    if self.planner.next_plan_at <= 0.0 or now >= self.planner.next_plan_at:
                        await self.trigger_plan_once(now=now)
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
