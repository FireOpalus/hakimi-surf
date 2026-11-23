"""Autonomous LifeEngine: LifeBlockPlanner, zero-Token GatekeeperWorker, and JITExecutor."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from datetime import time as day_time
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .budget_policy import PROACTIVE_LANE, paid_lane_available
from .config import Settings
from .gemini import WireLifePlan
from .life_agenda import MORNING_INTENT_LIMIT, REVISION_INTENT_LIMIT, agenda_day, agenda_items
from .life_config_compat import (
    LegacyScheduleAccess,
    import_old_proactive,
    legacy_view,
    migrate_flat,
)
from .life_defaults import fallback_plan
from .life_rules import RuleDispatcher
from .life_workers import ActionWorkers
from .modules.actions import ActionCall, GateDecision, HourlyAction, Mode, Outcome, reject_reason
from .modules.base import sanitize_sensor_text
from .planning_journal import PlannerJournal
from .proactive_policy import hours_bounds, within_hours
from .reply_style import trim_complete_text
from .thinking_policy import preferences as model_preferences
from .thinking_policy import supported_level
from .work_usage import model_work
from .world_experience import MAX_DAY_REST_MINUTES
from .world_state import (
    IntentTicketCreate,
    MilestoneEvent,
    PrivateEventScope,
    _check_no_prewritten_speech,
    private_event_for_owner,
)

log = logging.getLogger("hakimi")

JIT_TRIGGER_MESSAGE = (
    "[系统主动意图触发：这是你自己先前排定的主动发言时刻。请结合 "
    "jit_proactive_intent、待分享见闻与近期上下文，用自然松弛的口语开口；"
    "私聊可自然开启新话头或挂念日常，群聊若正逢严肃争执或完全插不上嘴可将 abstain 设为 true]"
)
DESKTOP_CONTEXT_HINT = "BOISER 近期刚在电脑端忙正事，默契关心一句注意休息喝水，绝不问怎么没回消息"
GROUP_SCRUBBED_INTENT_NOTE = "去群里看看大家在聊什么，顺势自然接话或结合公开见闻互动"
NEAR_HORIZON_SECONDS = 3 * 3600
ACTIVE_SESSION_REVIEW_SECONDS = 60 * 60
ACTIVE_SESSION_EVENT_COOLDOWN_SECONDS = 15 * 60
SESSION_EXIT_COOLDOWN_SECONDS = 120.0


def _parse_local_due(raw: str, local_now: datetime, zone: ZoneInfo) -> float:
    val = str(raw or "").strip()
    if not val:
        return local_now.timestamp()
    if len(val) >= 11:
        try:
            return datetime.strptime(val[:16], "%Y-%m-%d %H:%M").replace(tzinfo=zone).timestamp()
        except ValueError:
            pass
    if ":" in val:
        try:
            clock_t = day_time.fromisoformat(val[:5])
            candidate = datetime.combine(local_now.date(), clock_t, zone)
            if candidate <= local_now:
                candidate += timedelta(days=1)
            return candidate.timestamp()
        except ValueError:
            pass
    return local_now.timestamp()


class LifeEngineConfig(LegacyScheduleAccess, BaseModel):
    """Persistent configuration for LifeBlockPlanner and GatekeeperWorker."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    action_modes: dict[str, Mode] = Field(default_factory=dict)
    enabled: bool = True
    quiet_hours: str = "23:30-08:30"
    active_hours: str = Field(default="09:00-01:00", exclude=True)
    morning_thinking_level: Literal["medium", "high"] = Field(default="medium", exclude=True)
    planner_min_minutes: int = Field(default=60, ge=30, le=720)
    planner_max_minutes: int = Field(default=120, ge=60, le=720)
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


class LifeBlockPlanner:
    """Write one waking-day itinerary and revisit it on events or periodic reviews."""

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
        self.journal = PlannerJournal(self.store)
        self.ledger = getattr(service, "event_ledger", None)
        if self.ledger is not None:
            self.ledger.set_due_listener(self._on_ledger_due)
        self.next_plan_at: float = 0.0
        self.last_plan_at: float = 0.0
        self.last_plan_source: str = "none"

    async def _on_ledger_due(self, due_at: float) -> None:
        now = self.clock()
        if not self.planning_enabled or self.store.runtime.is_resting(due_at):
            return
        wake = max(now + 1.0, due_at)
        if not self.journal.ready:
            await self.journal.initialize(
                now, initial_interval=self.config.planner_min_minutes * 60
            )
        await self.journal.signal(wake)
        if self.next_plan_at <= 0 or wake < self.next_plan_at:
            self.next_plan_at = wake
        self.engine.wakeup.set()

    async def request_replan(self, *, delay_seconds: float = 35.0) -> bool:
        """Schedule an early revision plan after an explicit external/owner trigger."""
        now = self.clock()
        if not self.planning_enabled or self.store.runtime.is_resting(now):
            return False
        delay = max(1.0, min(300.0, float(delay_seconds)))
        wake = now + delay
        if not self.journal.ready:
            await self.journal.initialize(
                now, initial_interval=self.config.planner_min_minutes * 60
            )
        await self.journal.signal(wake)
        if self.next_plan_at <= 0 or wake < self.next_plan_at:
            self.next_plan_at = wake
        self.engine.wakeup.set()
        return True

    async def _collect_owner_planning_context(self, ts: float) -> dict[str, Any]:
        hub = getattr(self.service, "memory_hub", None)
        if hub is None or not hasattr(hub, "recall_context"):
            return {}
        try:
            ctx = await hub.recall_context(None, "", mode="planner", now=ts)
            return dict(ctx) if isinstance(ctx, dict) else {}
        except Exception:
            return {}

    async def _recent_completed_intents(self, ts: float) -> list[dict[str, str]]:
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT COALESCE(NULLIF(action_name,''), action_type) AS action, "
                "updated_at, intent_note_json FROM scheduled_intents "
                "WHERE status='done' AND updated_at>=? "
                "AND (target_scope='' OR target_scope LIKE 'owner:%') "
                "ORDER BY updated_at DESC LIMIT 5",
                (ts - 86400.0,),
            )
        items: list[dict[str, str]] = []
        for row in rows:
            try:
                parsed = json.loads(row["intent_note_json"] or "{}")
            except ValueError:
                parsed = {}
            raw_note = parsed.get("note") if isinstance(parsed, dict) else parsed
            note_clean = sanitize_sensor_text(str(raw_note or ""), self.s, max_tokens=36)
            items.append(
                {
                    "time": self._local(float(row["updated_at"])).strftime("%m-%d %H:%M"),
                    "action": str(row["action"] or ""),
                    "note": note_clean,
                }
            )
        return items

    @property
    def config(self) -> LifeEngineConfig:
        return self.engine.config

    @property
    def planning_enabled(self) -> bool:
        return self.config.enabled and (
            self.store.runtime.rest_hours.day_rest_enabled or bool(self.service.modules.active())
        )

    def _local(self, stamp: float) -> datetime:
        return datetime.fromtimestamp(stamp, _zone(self.s.bot_timezone))

    def calendar(self, day, *, social=False):
        rest = self.store.runtime.rest_hours
        return agenda_day(
            day,
            _zone(self.s.bot_timezone),
            rest.rest_hours if rest.enabled else None,
            self.config.quiet_hours if social else None,
        )

    async def sync_wake(self, now, state=None):
        """A saved review clock cannot push the next morning past wake-up."""
        state = state or await self.journal.clock()
        self.next_plan_at = state["next_at"]
        if not self.planning_enabled:
            return False
        today = self.calendar(self._local(now).date())
        seen = await self.journal.morning(today.day.isoformat())
        awake_at = max(now, self.store.runtime.day_rest_end(now))
        due = None if seen else today.morning_due_at(awake_at)
        if due is None:
            tomorrow = self.calendar(today.day + timedelta(days=1))
            due = tomorrow.morning_due_at(awake_at)
        if due is not None:
            self.next_plan_at = min(self.next_plan_at, due)
        return due is not None and due <= now

    def _schedule_sleep_wakeup(self, now: float, cfg: LifeEngineConfig) -> float:
        sleep_min = cfg.sleep_min_minutes * 60.0
        sleep_max = cfg.sleep_max_minutes * 60.0
        raw_wake = now + self.rng.uniform(sleep_min, sleep_max)
        today = self._local(now).date()
        morning_ts = next(
            (
                start
                for d in (today, today + timedelta(days=1))
                for start, _ in self.calendar(d).windows
                if start > now
            ),
            None,
        )
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

    async def initialize(self):
        state = await self.journal.initialize(
            self.clock(), initial_interval=self.config.planner_min_minutes * 60
        )
        await self.sync_wake(self.clock(), state)
        self.last_plan_at, self.last_plan_source = state["last_at"], state["last_source"]

    async def plan_once(
        self, now: float | None = None, *, automatic: bool = False
    ) -> dict[str, Any]:
        entered = self.clock()
        ts = entered if now is None else now
        state = await self.journal.initialize(
            ts, initial_interval=self.config.planner_min_minutes * 60
        )
        day = self._local(ts).date().isoformat()
        cfg = self.config
        ws = self.service.world_state
        morning = await self.sync_wake(ts, state)
        if automatic and ts < self.next_plan_at:
            return {"status": "not_due", "next_plan_at": self.next_plan_at, "intents_enqueued": 0}
        if not self.planning_enabled:
            reason = "disabled" if not cfg.enabled else "no_active_modules"
            self.next_plan_at = ts + 1800.0
            await self.journal.pause(day, ts, self.next_plan_at, reason)
            return {"status": reason, "next_plan_at": self.next_plan_at, "intents_enqueued": 0}

        await ws.experience.advance(ts)
        if nap_end := self.store.runtime.day_rest_end(ts):
            # An encrypted checkpoint, if any, stays available for the next wake.
            self.next_plan_at = nap_end
            await self.journal.pause(day, ts, nap_end, "day_rest")
            self.last_plan_at, self.last_plan_source = ts, "day_rest"
            return {"status": "day_rest", "next_plan_at": nap_end, "intents_enqueued": 0}
        local_dt = self._local(ts)
        calendar = self.calendar(local_dt.date())
        checkpoint = await self.journal.prepared() if state["prepared"] else None
        if self.store.runtime.is_resting(ts):
            await self.engine.exit_active_session(ts, reason="sleeping")
            wake_at = self._schedule_sleep_wakeup(ts, cfg)
            self.last_plan_at = ts
            self.last_plan_source = "sleeping"
            await self.journal.pause(day, ts, wake_at, "sleeping")
            await self.sync_wake(ts, {"next_at": wake_at})
            return {"status": "sleeping", "next_plan_at": self.next_plan_at, "intents_enqueued": 0}

        current = ws.current
        events = await ws.unconsumed_events(limit=20)
        modules_reg = self.service.modules
        module_data = await modules_reg.context_data(limit=4, now=ts)
        sensors = await modules_reg.collect_sensor_summaries()
        known_modules = {"idle", *(m.module_id for m in modules_reg.active())}
        planning_context = {}
        module_fingerprints: dict[str, str] = {}
        for module in modules_reg.active():
            planning_context.update(await module.planning_context(self.service, cfg, ts))
            fp = sanitize_sensor_text(module.planning_fingerprint(), self.s, max_tokens=32)
            if fp:
                module_fingerprints[module.module_id] = fp

        if self.ledger is not None:
            ledger_items, ledger_due_digest, next_ledger_due = await self.ledger.planner_state(ts)
        else:
            ledger_items, ledger_due_digest, next_ledger_due = ([], "", None)
        owner_ctx = await self._collect_owner_planning_context(ts)
        tainted_by_owner_memory = bool(owner_ctx) or any(
            item.visibility == "owner_private" for item in ledger_items
        )

        budget = await self.store.budget_status()
        preferences = await ws.experience.preferences(ts)
        preferences["day_rest_available"] = (
            preferences["day_rest_available"] and self.engine.day_rest_limit(ts) > ts
        )
        available_actions = [
            action
            for action in self.engine.catalog.view(cfg.action_modes, planner=True)
            if not action["budget_lane"] or paid_lane_available(budget, action["budget_lane"])
        ]
        offered = {action["name"] for action in available_actions}
        active_session = self.engine.active_session_exit()
        near_horizon = await self._near_horizon_summary(calendar, ts)
        zone = _zone(self.s.bot_timezone)
        current_world_state: dict[str, Any] = {
            "current_module": current.current_module,
            "focus_level": current.focus_level,
            "mood": current.mood,
            **preferences,
            "energy": current.energy,
            "boredom": current.boredom,
        }
        if active_session is not None:
            current_world_state["active_session_exit_action"] = active_session[1].name
        planner_payload: dict[str, Any] = {
            "local_time": local_dt.strftime("%Y-%m-%d %H:%M"),
            "weekday": ("周一", "周二", "周三", "周四", "周五", "周六", "周日")[local_dt.weekday()],
            "current_world_state": current_world_state,
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
        if ledger_items:
            planner_payload["pending_ledger_events"] = [
                item.to_planner_dict(ts, zone) for item in ledger_items
            ]
        if owner_ctx:
            planner_payload["owner_recent_context"] = owner_ctx
        if morning:
            recent_done = await self._recent_completed_intents(ts)
            if recent_done:
                planner_payload["recent_completed_intents"] = recent_done

        def _make_signature(near_has_pending: bool) -> str:
            return self.store.vault.digest(
                "planner-opportunities",
                json.dumps(
                    {
                        "actions": sorted(offered),
                        "day_rest_available": preferences["day_rest_available"],
                        "modules": sorted(known_modules),
                        "audiences": planning_context.get("available_groups", []),
                        "owner_available": planning_context.get("owner_dm_available", False),
                        "focus_lock": current.current_module if ws.proactive_suspended() else None,
                        "funded": paid_lane_available(budget, PROACTIVE_LANE),
                        "ledger_due": ledger_due_digest,
                        "module_fp": module_fingerprints,
                        "near_horizon_has_pending": near_has_pending,
                    },
                    sort_keys=True,
                ),
            )

        signature = _make_signature(near_horizon["has_pending"])
        public_events = planner_payload["unconsumed_events"]
        if checkpoint:
            row, body = checkpoint
            run_id, scheduled_at, next_at = row["id"], row["started"], row["next_at"]
            morning = row["trigger"] == "morning"
            calendar = self.calendar(date.fromisoformat(row["day"]))
            plan = WireLifePlan.model_validate(body)
            plan_source = row["source"]
            reasons = json.loads(row["reasons"])
        else:
            scheduled_at = ts
            next_at = ts + self.rng.uniform(
                cfg.planner_min_minutes * 60, cfg.planner_max_minutes * 60
            )
            if (
                next_ledger_due is not None
                and ts < next_ledger_due < next_at
                and not self.store.runtime.is_resting(next_ledger_due)
            ):
                next_at = max(ts + 5.0, next_ledger_due)
            if active_session is not None:
                next_at = min(next_at, ts + ACTIVE_SESSION_REVIEW_SECONDS)
            planner_funded = paid_lane_available(budget, PROACTIVE_LANE)
            restless_bored = current.boredom >= 70
            owner_longing_wake = preferences.get("owner_longing", 0) >= 65 and bool(
                planning_context.get("owner_dm_available")
            )
            quiet = bool(
                automatic
                and not morning
                and not public_events
                and not restless_bored
                and not owner_longing_wake
                and active_session is None
                and state["signature"] == signature
                and ts < state["explore_at"]
                and planner_funded
            )
            trigger = (
                "morning"
                if morning
                else "manual"
                if not automatic
                else "milestone"
                if public_events
                else "active_session_review"
                if active_session is not None
                else "opportunities_changed"
                if state["signature"] != signature
                else "unchanged"
                if quiet
                else "exploration"
            )
            explore_at = (
                state["explore_at"]
                if quiet
                else ts
                + self.rng.uniform(cfg.planner_max_minutes * 120, cfg.planner_max_minutes * 180)
            )
            policy = model_preferences(self.s)
            thinking_level = supported_level(
                policy.primary_model,
                policy.scene_levels.get("world.morning" if morning else "world.revision", "medium"),
            )
            run_id = await self.journal.start(
                day,
                ts,
                trigger=trigger,
                next_at=next_at,
                signature=signature,
                explore_at=explore_at,
                events=[ev.id for ev in events],
                prior_signal=state["signal_at"],
                thinking_level=thinking_level,
            )
            if run_id is None:
                await self.sync_wake(ts)
                return {
                    "status": "not_due",
                    "next_plan_at": self.next_plan_at,
                    "intents_enqueued": 0,
                }
            can_choose_rest = planner_funded and preferences["day_rest_available"]
            if quiet or (not offered and not can_choose_rest):
                reason = "unchanged_state" if quiet else "no_available_actions"
                await self.journal.commit(
                    ws,
                    run_id,
                    tickets=[],
                    candidates=0,
                    source="local_idle",
                    outcome=reason,
                    reasons={reason: 1},
                    avoided=planner_funded,
                )
                self.last_plan_at, self.last_plan_source = ts, "local_idle"
                await self.sync_wake(ts)
                return {
                    "status": "resting",
                    "reason": reason,
                    "intents_enqueued": 0,
                    "next_plan_at": self.next_plan_at,
                }

            plan = None
            reasons = {}
            plan_source = "local_fallback"
            if planner_funded:
                subject = self.store.vault.digest("life-planner", self.s.namespace)
                try:
                    planner_payload.update(
                        await self.agenda_context(calendar, ts, morning, near_horizon=near_horizon)
                    )
                    with model_work("world.plan", run_id):
                        raw_plan = await self.service.ai.generate(
                            {
                                k: v
                                for k, v in planner_payload.items()
                                if k != "desktop_active_recently"
                            },
                            subject,
                            budget_lane=PROACTIVE_LANE,
                            life_plan=True,
                            morning_thinking=thinking_level if morning else None,
                        )
                    plan = WireLifePlan.model_validate_json(raw_plan)
                    if plan.agenda_changes:
                        editable = {
                            i["id"]
                            for i in planner_payload["today_agenda"]
                            if i["status"] == "pending" and not morning
                        }
                        valid = [edit for edit in plan.agenda_changes if edit.intent_id in editable]
                        if ignored := len(plan.agenda_changes) - len(valid):
                            reasons["agenda_item_unavailable"] = ignored
                        plan = plan.model_copy(update={"agenda_changes": valid})
                    plan_source = "llm"
                except asyncio.CancelledError:
                    raise
                except Exception:
                    reasons["model_unavailable_or_invalid"] = 1
            else:
                reasons["planner_budget_closed"] = 1
            if plan is None:
                plan = await fallback_plan(
                    self.engine, planner_payload, local_dt, ts, offered, planner_funded
                )
            await self.journal.checkpoint(run_id, plan_source, plan.model_dump(), reasons)

        resolved_module = sanitize_sensor_text(plan.current_module, self.s, max_tokens=16) or "idle"
        if resolved_module not in known_modules or (
            resolved_module != "idle" and modules_reg.inactive_reason(resolved_module)
        ):
            resolved_module = "idle"
        resolved_focus: Literal["LOW", "MEDIUM", "HIGH"] = plan.focus_level
        if resolved_focus == "HIGH":
            resolved_focus = "MEDIUM"
        resolved_mood = sanitize_sensor_text(plan.mood, self.s, max_tokens=16) or "calm"

        tickets = []
        # Forged IDs were discarded before checkpointing. They are still audit candidates.
        edit_candidates = len(plan.agenda_changes) + reasons.get("agenda_item_unavailable", 0)

        def skip(reason):
            reasons[reason] = reasons.get(reason, 0) + 1

        limit = MORNING_INTENT_LIMIT if morning else REVISION_INTENT_LIMIT
        if len(plan.intents) > limit:
            reasons["round_cap"] = len(plan.intents) - limit
        applied_at = ts + max(0, self.clock() - entered)
        calendar = self.calendar(calendar.day)
        for index, item in enumerate(plan.intents[:limit]):
            raw_note = str(item.intent_note or "").strip()
            if not raw_note:
                skip("empty_note")
                continue
            try:
                _check_no_prewritten_speech(raw_note)
            except ValueError:
                skip("prewritten_speech")
                continue
            bounded_note = trim_complete_text(raw_note, 120)
            raw_scope = item.target_scope.strip()
            resolved_scope = modules_reg.normalize_recipient(raw_scope) if raw_scope else ""
            if resolved_scope is None:
                skip("recipient_unavailable")
                continue
            catalog = self.engine.catalog
            target_module, action_type = item.target_module, item.action_type
            if item.action_name:
                target_module = catalog.owners.get(item.action_name, "")
                spec = catalog.actions.get(item.action_name)
                if not spec:
                    skip("unknown_action")
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
                skip("invalid_action")
                continue

            if spec.name not in offered:
                skip("unavailable_action")
                continue

            is_owner_scope = bool(
                resolved_scope
                and (
                    resolved_scope.startswith("owner:") or resolved_scope.startswith("boundowner:")
                )
            )
            if spec.social and not is_owner_scope and tainted_by_owner_memory:
                bounded_note = GROUP_SCRUBBED_INTENT_NOTE

            ttl_sec = max(600.0, min(float(item.ttl_minutes) * 60.0, 6.0 * 3600.0))
            window = self.calendar(calendar.day, social=spec.social).window(
                at=item.at_local_time,
                earliest=scheduled_at + 1,
                relative=scheduled_at + max(1, item.delay_minutes) * 60,
                ttl=ttl_sec,
            )
            if not window or window[1] <= applied_at:
                skip("window_expired")
                continue
            execute_after, expire_at = window
            gate_cond = {"allow_desktop_care": True} if item.allow_desktop_care else {}
            if (
                plan_source == "local_fallback"
                and action_type == "group_chat_share"
                and target_module == "bilibili_surf"
            ):
                gate_cond["share_news"] = True
            try:
                ticket = IntentTicketCreate(
                    origin="planner",
                    dedupe_key=f"plan:{run_id}:{index}",
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
                skip("invalid_ticket")
                continue
            tickets.append(ticket)

        changes = []
        agenda_snapshot: list[dict] | None = None
        if plan.agenda_changes and not morning and plan_source == "llm":
            async with self.store.lock:
                items = await agenda_items(self.store, calendar.day.isoformat())
            agenda_snapshot = items
            pending = {
                i["id"]: i
                for i in items
                if i["status"] == "pending" and i["execute_after"] > applied_at
            }
            if len(plan.agenda_changes) > MORNING_INTENT_LIMIT:
                reasons["round_cap"] = (
                    reasons.get("round_cap", 0) + len(plan.agenda_changes) - MORNING_INTENT_LIMIT
                )
            for change in plan.agenda_changes[:MORNING_INTENT_LIMIT]:
                item = pending.pop(change.intent_id, None)
                if not item:
                    skip("agenda_item_unavailable")
                    continue
                window = None
                if change.operation == "move":
                    spec = self.engine.catalog.actions.get(item["action_name"])
                    if spec is None:
                        skip("unavailable_action")
                        continue
                    window = (
                        self.calendar(calendar.day, social=spec.social).window(
                            at=change.at_local_time,
                            earliest=applied_at + 1,
                            relative=0,
                            ttl=item["expire_at"] - item["execute_after"],
                        )
                        if change.at_local_time
                        else None
                    )
                    if not window:
                        skip("invalid_agenda_time")
                        continue
                changes.append((change.intent_id, window))

        outcome = (
            "accepted"
            if tickets
            else "all_filtered"
            if plan.intents or edit_candidates
            else "model_chose_idle"
            if plan_source == "llm"
            else "local_idle"
        )
        day_rest = None
        if plan.rest_minutes and resolved_module == "idle":
            # A delayed/checkpointed plan cannot backdate rest or restart an expired nap.
            decision_at = ts + max(0, self.clock() - entered)
            end = min(
                self.engine.day_rest_limit(decision_at), scheduled_at + plan.rest_minutes * 60
            )
            day_rest = (decision_at, end)
        if agenda_snapshot is None:
            async with self.store.lock:
                agenda_snapshot = await agenda_items(self.store, calendar.day.isoformat())
        horizon_end = ts + NEAR_HORIZON_SECONDS
        change_map = dict(changes)
        post_near_has_pending = any(
            ticket.expire_at > ts and ticket.execute_after <= horizon_end for ticket in tickets
        ) or any(
            (
                change_map[item["id"]] is not None
                and change_map[item["id"]][1] > ts
                and change_map[item["id"]][0] <= horizon_end
            )
            if item["id"] in change_map
            else (item["expire_at"] > ts and item["execute_after"] <= horizon_end)
            for item in agenda_snapshot
            if item["status"] in ("pending", "executing")
        )
        enqueued_ids = await self.journal.commit(
            ws,
            run_id,
            tickets=tickets,
            candidates=len(plan.intents) + edit_candidates,
            source=plan_source,
            outcome=outcome,
            reasons=reasons,
            avoided=False,
            plan=(resolved_module, resolved_focus, resolved_mood),
            day_rest=day_rest,
            agenda_changes=changes,
            applied_at=applied_at,
            final_signature=_make_signature(post_near_has_pending),
        )
        if day_rest:
            await ws.refresh()
            await self.engine.exit_active_session(applied_at, reason="day_rest")
        if plan_source == "llm" and self.ledger is not None:
            for post_item in plan.ledger_posts[:4]:
                if not str(post_item.summary or "").strip():
                    continue
                vis: Literal["public", "owner_private"] = (
                    "owner_private" if tainted_by_owner_memory else post_item.visibility
                )
                due_ts = _parse_local_due(post_item.due_at_local, local_dt, zone)
                ttl_sec = max(1, min(int(post_item.ttl_hours), 336)) * 3600.0
                try:
                    await self.ledger.post(
                        "planner",
                        "self_memo",
                        post_item.summary,
                        target_module=post_item.target_module,
                        dedupe_key=post_item.key or None,
                        due_at=due_ts,
                        ttl_seconds=ttl_sec,
                        visibility=vis,
                        now=ts,
                    )
                except ValueError:
                    continue
            for consume_ref in plan.ledger_consumes[:6]:
                await self.ledger.consume(
                    consume_ref,
                    consumer="planner",
                    allow_owner_private=True,
                    allowed_sources={"planner", "qq_owner"},
                    now=ts,
                )
        await self.sync_wake(applied_at)

        longnick_text = str(plan.mood_longnick or "").strip()
        if longnick_text and any(
            start <= applied_at < end
            for start, end in self.calendar(calendar.day, social=True).windows
        ):
            longnick = getattr(self.service, "longnick", None)
            if longnick is not None:
                if hasattr(longnick, "apply_life_block"):
                    await longnick.apply_life_block(longnick_text)
                elif hasattr(longnick, "apply_patrol"):
                    await longnick.apply_patrol(longnick_text)

        diary = getattr(self.service, "diary", None)
        if diary is not None and hasattr(diary, "world_event"):
            await diary.world_event(
                f"life-block:{run_id}",
                resolved_module,
                "life_block",
                (f"时段计划 {resolved_module}({resolved_focus}) · 预期心情 {resolved_mood}"),
                now=ts,
            )

        self.last_plan_at = ts
        self.last_plan_source = plan_source
        return {
            "status": "planned",
            "planning_mode": "morning" if morning else "revision",
            "source": plan_source,
            "current_module": resolved_module,
            "focus_level": resolved_focus,
            "mood": resolved_mood,
            "intents_enqueued": len(enqueued_ids),
            "intent_ids": enqueued_ids,
            "next_plan_at": self.next_plan_at,
        }

    async def _near_horizon_summary(
        self, calendar, now: float, *, rows: list[dict] | None = None
    ) -> dict[str, Any]:
        horizon_end = now + NEAR_HORIZON_SECONDS
        if rows is None:
            async with self.store.lock:
                rows = await agenda_items(self.store, calendar.day.isoformat())
        pending_count = sum(
            1
            for r in rows
            if r["status"] in ("pending", "executing")
            and r["expire_at"] > now
            and r["execute_after"] <= horizon_end
        )
        awake_sec = sum(
            max(0.0, min(b, horizon_end) - max(a, now))
            for a, b in calendar.windows
            if b > now and a < horizon_end
        )
        return {
            "window_hours": 3,
            "window_end": self._local(horizon_end).strftime("%H:%M"),
            "pending_count": pending_count,
            "has_pending": pending_count > 0,
            "awake_minutes_left": round(awake_sec / 60),
        }

    async def agenda_context(
        self,
        calendar,
        now,
        morning,
        *,
        near_horizon: dict[str, Any] | None = None,
    ):
        async with self.store.lock:
            rows = await agenda_items(self.store, calendar.day.isoformat())
        remaining = [r for r in rows if r["status"] in ("pending", "executing")]
        history = [r for r in rows if r["status"] not in ("pending", "executing")]
        reserved = []
        for module in self.service.modules.active():
            for slot in self.service.modules.schedules_for(module):
                at = await self.engine.next_slot_at(module, slot, now)
                if at is not None and self._local(at).date() == calendar.day:
                    reserved.append(
                        {"action": slot.name, "time": self._local(at).strftime("%H:%M")}
                    )
        return {
            "planning_mode": "morning" if morning else "revision",
            "outbound_quiet_hours": self.config.quiet_hours,
            "activity_windows": [
                [
                    self._local(a).strftime("%H:%M"),
                    self._local(b).strftime("%H:%M")
                    if self._local(b).date() == calendar.day
                    else "24:00",
                ]
                for a, b in calendar.windows
                if b > now
            ],
            "near_horizon": (
                near_horizon
                if near_horizon is not None
                else await self._near_horizon_summary(calendar, now, rows=rows)
            ),
            "today_agenda": [
                {
                    "id": r["id"],
                    "action": r["action_name"],
                    "time": self._local(r["execute_after"]).strftime("%H:%M"),
                    "status": r["status"],
                    "note": r["note"],
                }
                for r in (history[-6:] + remaining[:16])
            ],
            "reserved_activities": reserved,
        }

    async def on_milestone_event(self, event: MilestoneEvent) -> str | None:
        result = await self.engine.rules.notify(event, self.clock())
        if (
            self.engine.config.enabled
            and event.importance == "high"
            and not event.consumed
            and not event.payload.get("private_first")
        ):
            now = self.clock()
            await self.journal.initialize(
                now, initial_interval=self.config.planner_min_minutes * 60
            )
            # Coalesce bursts; when already inside an active session of the same module,
            # avoid thrashing the macro planner every 60s on intra-session high events.
            active_session = self.engine.active_session_exit()
            same_active_module = (
                active_session is not None and active_session[0] == event.source_module
            )
            min_gap = ACTIVE_SESSION_EVENT_COOLDOWN_SECONDS if same_active_module else 60
            wake = max(now + 5, self.last_plan_at + min_gap)
            await self.journal.signal(wake)
            self.next_plan_at = min(self.next_plan_at or wake, wake)
            self.engine.wakeup.set()
        return result


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
        self.workers = ActionWorkers(engine, self._evaluate_ticket)

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
        async with self.store.lock:
            usage, _ = await self._usage_tx(now, cfg)
            return usage

    async def _usage_tx(self, now, cfg):
        """Caller owns the SQLite lock. Settled usage and live reservations share a snapshot."""
        start, end = _day_bounds(now, self.s.bot_timezone)
        catalog = self.engine.catalog
        usage = {
            q.counter: 0
            for spec in catalog.actions.values()
            for q in spec.quota_rules()
            if q.counter
        }
        usage.update(total=0, counted=0)
        reserved = 0
        rows = await self.store.all(
            "SELECT action_name,target_module,action_type,target_scope,COUNT(*) AS n,"
            "SUM(status='executing') AS held FROM scheduled_intents "
            "WHERE status IN ('executing','done','uncertain') "
            "AND COALESCE(claimed_at,updated_at)>=? AND COALESCE(claimed_at,updated_at)<? "
            "GROUP BY action_name,target_module,action_type,target_scope",
            (start, end),
        )
        for row in rows:
            name = row["action_name"] or catalog.old_name(row)
            spec = catalog.actions.get(name)
            settled = row["n"] - row["held"]
            if not spec:
                # Preserve historical deliveries from actions no longer registered.
                if row["target_scope"]:
                    usage["total"] += settled
                    usage["counted"] += settled
                    reserved += row["held"]
                continue
            rule = self.quota_for(spec, row["target_scope"])
            if rule is None:
                continue
            usage["total"] += settled
            if rule.counter:
                usage[rule.counter] += settled
            if not self.is_unlimited(spec, rule, cfg):
                usage["counted"] += settled
                reserved += row["held"]
        return usage, reserved

    async def daily_limit_allows(
        self, action: str, scope: str, now: float, cfg: LifeEngineConfig, *, settled_only=False
    ) -> bool:
        spec = self.engine.catalog.actions[action]
        rule = self.quota_for(spec, scope)
        if rule is None or self.is_unlimited(spec, rule, cfg):
            return True
        async with self.store.lock:
            usage, held = await self._usage_tx(now, cfg)
        return usage["counted"] + (0 if settled_only else held) < cfg.max_proactive_per_day

    async def quota_gate_tx(self, ticket, now):
        try:
            spec, _ = self.engine.resolve(ticket)
        except ValueError as exc:
            return GateDecision(reject_reason(exc))
        cfg = self.config
        rule = self.quota_for(spec, ticket["target_scope"])
        if rule is None or self.is_unlimited(spec, rule, cfg):
            return GateDecision()
        usage, held = await self._usage_tx(now, cfg)
        if usage["counted"] >= cfg.max_proactive_per_day:
            return GateDecision("daily_proactive_limit")
        if usage["counted"] + held >= cfg.max_proactive_per_day:
            # A running action may yet be proven unsent and release its reservation.
            return GateDecision("daily_capacity_reserved", delay=60)
        return GateDecision()

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
        if not spec.background_sensor and self.store.runtime.is_resting(now):
            return await reject("sleeping")
        if spec.social and cfg.quiet(local_dt):
            return await reject("quiet_hours")
        if not spec.background_sensor and spec.window != "closing":
            if nap_end := self.store.runtime.day_rest_end(now):
                return await defer("day_rest", nap_end - now)
        if spec.window == "closing" and not self.engine.closing_due(now):
            return await reject("outside_digest_window")
        if (
            not spec.background_sensor
            and not spec.interrupts_focus
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
        if spec.budget_lane is not None and not (
            spec.reuses_paid_work and await spec.reuses_paid_work(now)
        ):
            budget = await self.store.budget_status()
            if not paid_lane_available(budget, spec.budget_lane):
                return await reject("budget_limit")
        if not await self.daily_limit_allows(
            spec.name, ticket["target_scope"], now, cfg, settled_only=True
        ):
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

    async def tick(self, now: float | None = None, *, wait=True) -> dict[str, Any]:
        ts = self.clock() if now is None else now
        expired = await self._expire_stale(ts)
        cfg = self.config
        if not cfg.enabled:
            return {"status": "disabled", "expired": expired, "checked": 0, "executed": 0}

        due = await self.service.world_state.list_due_intents(
            now=ts, limit=20, distinct_lanes=not wait
        )
        if not due:
            return {"status": "idle", "expired": expired, "checked": 0, "executed": 0}

        results = await self.workers.dispatch(due, ts, wait=wait)
        return {
            "status": "ok",
            "expired": expired,
            "checked": len(due),
            "executed": results.get("done", 0) + results.get("uncertain", 0),
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
        ticket = await ws.claim(
            intent_id, ts, quota_gate=lambda row: self.engine.gatekeeper.quota_gate_tx(row, ts)
        )
        if ticket is None:
            return {"status": "not_claimed"}
        if ticket["status"] != "executing":
            return {
                "status": "deferred" if ticket["status"] == "pending" else ticket["status"],
                "reason": ticket["result_reason"],
            }
        try:
            spec, args = self.engine.resolve(ticket)
        except ValueError as exc:
            result = Outcome("gated_skip", reject_reason(exc))
        else:
            entered_handler = False
            try:
                evidence = await self.engine.evidence(ticket)

                def still_allowed() -> bool:
                    current = max(ts, self.clock())
                    local = datetime.fromtimestamp(current, _zone(self.s.bot_timezone))
                    return (
                        self.engine.config.enabled
                        and not self.engine.catalog.inactive_reason(
                            self.engine.catalog.owners[spec.name]
                        )
                        and not (
                            ticket.get("source_module")
                            and self.service.modules.get(ticket["source_module"])
                            and self.service.modules.inactive_reason(ticket["source_module"])
                        )
                        and (spec.window != "closing" or self.engine.closing_due(current))
                        and (
                            spec.background_sensor
                            or (
                                not (spec.social and self.engine.config.quiet(local))
                                and not self.store.runtime.is_resting(current)
                                and (
                                    spec.window == "closing"
                                    or not self.store.runtime.day_rest_end(current)
                                )
                            )
                        )
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

                if still_allowed() and await ws.start_handler(intent_id) and still_allowed():
                    entered_handler = True
                    with model_work(spec.name, intent_id):
                        async with ws.experience.chatting() if spec.social else nullcontext():
                            result = await spec.run(
                                ActionCall(
                                    ticket, args, ts, evidence, desktop_context_hint, still_allowed
                                )
                            )
                else:
                    result = Outcome("gated_skip", "action_no_longer_eligible")
            except PrivateEventScope:
                result = Outcome("gated_skip", "private_event_scope")
            except asyncio.CancelledError:
                end = self.clock()
                status = (
                    "uncertain"
                    if entered_handler
                    else ("pending" if end < ticket["expire_at"] else "expired")
                )
                await asyncio.shield(
                    ws.mark_intent(
                        intent_id,
                        status,
                        expected="executing",
                        reason="interrupted" if entered_handler else "interrupted_before_dispatch",
                        now=end,
                    )
                )
                raise
            except Exception:
                log.warning("event=life_action_failed action=%s", spec.name)
                result = (
                    Outcome("uncertain", "handler_interrupted")
                    if entered_handler
                    else Outcome("not_dispatched", "preparation_interrupted")
                )
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
        self._gate_lock = asyncio.Lock()
        self._background: dict[str, asyncio.Task] = {}
        self._session_exit_attempts: dict[str, float] = {}
        self.wakeup = asyncio.Event()
        self.planner = LifeBlockPlanner(service, self, clock=self.clock, rng=self.rng)
        self.gatekeeper = GatekeeperWorker(service, self, clock=self.clock)
        self.jit = JITExecutor(service, self, clock=self.clock)
        self.catalog = service.modules.catalog
        self.rules = RuleDispatcher(self, service.modules)

    def active_session_exit(self) -> tuple[Any, Any] | None:
        active_id = self.service.world_state.current.current_module
        if not active_id or active_id == "idle":
            self._session_exit_attempts.clear()
            return None
        module = self.service.modules.get(active_id)
        exit_action = getattr(module, "session_exit_action", None) if module is not None else None
        if not isinstance(exit_action, str) or not exit_action or module.inactive_reason:
            return None
        spec = self.catalog.actions.get(exit_action)
        if (
            spec is None
            or self.catalog.owners.get(exit_action) != active_id
            or spec.social
            or self.config.action_modes.get(exit_action) == "off"
        ):
            return None
        return (module, spec)

    async def exit_active_session(self, now: float, *, reason: str) -> bool:
        resolved = self.active_session_exit()
        if resolved is None:
            return False
        module, spec = resolved
        last_attempt = self._session_exit_attempts.get(module.module_id)
        if last_attempt is not None and now - last_attempt < SESSION_EXIT_COOLDOWN_SECONDS:
            return False
        try:
            args = spec.arguments()
        except (ValidationError, TypeError):
            return False
        self._session_exit_attempts[module.module_id] = now
        call = ActionCall(
            {
                "origin": "timer",
                "action_name": spec.name,
                "target_module": module.module_id,
                "target_scope": "",
                "arguments": args.model_dump(),
                "exit_reason": reason,
            },
            args,
            now,
            still_allowed=lambda: True,
        )
        try:
            outcome = await spec.run(call)
        except Exception:
            log.warning(
                "event=session_exit_failed module=%s action=%s reason=%s",
                module.module_id,
                spec.name,
                reason,
                exc_info=True,
            )
            return False
        return outcome.status == "done"

    async def set_module_enabled(self, module, enabled: bool) -> None:
        await self.initialize()
        async with self._config_lock:
            changed = module.enabled != enabled
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO module_switches VALUES (?,?) ON CONFLICT(module_id) "
                    "DO UPDATE SET enabled=excluded.enabled",
                    (module.module_id, int(enabled)),
                )
                if not enabled:
                    names = [
                        n for n, owner in self.catalog.owners.items() if owner == module.module_id
                    ]
                    await self.store.db.execute(
                        "UPDATE scheduled_intents SET status='gated_skip',"
                        "result_reason='module_disabled',updated_at=? WHERE status='pending' AND "
                        "(target_module=? OR source_module=? OR action_name IN "
                        "(SELECT value FROM json_each(?)))",
                        (self.clock(), module.module_id, module.module_id, json.dumps(names)),
                    )
                    await self.store.db.execute(
                        "UPDATE world_state SET current_module='idle',focus_level='LOW' "
                        "WHERE singleton=1 AND current_module=?",
                        (module.module_id,),
                    )
            module.enabled = enabled
            await self.planner.sync_wake(self.clock())
            self.wakeup.set()
            if changed:
                await module.on_switch(self.service)
                await self.service.world_state.refresh()

    def options_for(self, spec, cfg):
        module = self.service.modules.get(self.catalog.owners[spec.name])
        return module.scheduling(cfg)

    def query_options(self) -> dict[str, str]:
        """Trusted, read-only capabilities available to this turn's router."""
        return {
            spec.name: spec.description
            for spec in self.catalog.actions.values()
            if "query" in spec.origins and not self._query_issue(spec)
        }

    def _query_issue(self, spec) -> str | None:
        if self.config.action_modes.get(spec.name) == "off":
            return "action_disabled"
        return self.catalog.inactive_reason(self.catalog.owners[spec.name]) or spec.unavailable()

    async def query(self, name: str) -> Outcome:
        """Serve an immediate read for chat, an installed module or the admin console."""
        spec = self.catalog.actions.get(name)
        if not spec or "query" not in spec.origins:
            return Outcome("gated_skip", "unknown_query")
        if reason := self._query_issue(spec):
            return Outcome("gated_skip", reason)
        call = ActionCall(
            {"origin": "query", "action_name": name},
            spec.arguments(),
            self.clock(),
            still_allowed=lambda: not self._query_issue(spec),
        )
        result = await spec.run(call)
        if reason := self._query_issue(spec):
            return Outcome("gated_skip", reason)
        return result

    def resolve(self, ticket: dict, *, admin: bool = False):
        if (ticket.get("origin") == "admin") != admin:
            raise ValueError("action_origin_forbidden")
        spec = self.catalog.resolve(ticket)
        source = self.service.modules.get(ticket.get("source_module", ""))
        if source and source.inactive_reason:
            raise ValueError(source.inactive_reason)
        mode = self.config.action_modes.get(spec.name, "active")
        if mode == "off":
            raise ValueError("action_disabled")
        if mode in {"legacy", "shadow"}:
            spec = self.catalog.resolve(ticket, compatibility=True)
        recipient = ticket.get("target_scope", "")
        selected = getattr(self.service, "proactive", None)
        if recipient.startswith("boundowner:") and (
            not selected or spec.name != selected.action_name
        ):
            raise ValueError("unauthorized_scope")
        if spec.owner_only and recipient != (
            selected.target_scope
            if selected and spec.name == selected.action_name
            else "owner:" + self.s.owner_qq_id
        ):
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
            "payload": {k: v for k, v in payload.items() if k != "audience"},
        }

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                for switch in await self.store.all("SELECT module_id,enabled FROM module_switches"):
                    if module := self.service.modules.get(switch["module_id"]):
                        module.enabled = bool(switch["enabled"])
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
                if (not has_intents_table or not world_state.experience.ready) and hasattr(
                    world_state, "initialize"
                ):
                    await world_state.initialize()
                if self.planner.ledger is not None:
                    await self.planner.ledger.initialize()
                await self.rules.initialize()
                await self.planner.initialize()
                await self.trim_day_rest(self.clock())
                world_state.subscribe(self.planner.on_milestone_event)
            elif self.planner.next_plan_at <= 0.0:
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
            await self.service.world_state.experience.advance(self.clock())
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
            await self.trim_day_rest(self.clock())
            await self.planner.sync_wake(self.clock())
        self.wakeup.set()
        return await self.status()

    async def trigger_plan_once(
        self, *, now: float | None = None, automatic: bool = False
    ) -> dict[str, Any]:
        await self.initialize()
        running = self._background.get("plan-work")
        if running and not running.done():
            return await asyncio.shield(running)
        if automatic and self.planner.next_plan_at > (self.clock() if now is None else now):
            return {"status": "scheduled", "next_plan_at": self.planner.next_plan_at}
        task = self._start_background(
            "plan-work", self.planner.plan_once, now=now, automatic=automatic
        )
        return await asyncio.shield(task)

    def _start_background(self, name, call, *args, **kwargs):
        if current := self._background.get(name):
            if not current.done():
                return current
        task = asyncio.create_task(call(*args, **kwargs), name=f"life-{name}")
        self._background[name] = task

        def finished(worker):
            if self._background.get(name) is worker:
                self._background.pop(name)
            if not worker.cancelled() and (error := worker.exception()) is not None:
                log.warning(
                    "event=life_background_failed work=%s type=%s", name, type(error).__name__
                )

        task.add_done_callback(finished)
        return task

    async def tick_gatekeeper_once(
        self, *, now: float | None = None, wait: bool = True
    ) -> dict[str, Any]:
        await self.initialize()
        ts = self.clock() if now is None else now
        await self.service.world_state.experience.advance(ts)
        if self.config.enabled:
            if self.store.runtime.is_resting(ts):
                await self.exit_active_session(ts, reason="sleeping")
            elif self.store.runtime.day_rest_end(ts):
                await self.exit_active_session(ts, reason="day_rest")
            async with self._gate_lock:
                await self.schedule_due(ts)
        rules = self._start_background("rules", self.rules.catch_up, ts)
        if wait:
            await asyncio.shield(rules)
        return await self.gatekeeper.tick(now=ts, wait=wait)

    def day_rest_limit(self, now):
        """First minute unavailable for a daytime nap, including the reserved daily report."""
        runtime = self.store.runtime
        if not self.config.enabled or not runtime.rest_hours.day_rest_enabled:
            return now
        end = now + MAX_DAY_REST_MINUTES * 60
        point = now
        while point < end:
            if runtime.is_resting(point) or self.closing_due(point):
                return point
            point = (int(point // 60) + 1) * 60
        return end

    async def trim_day_rest(self, now):
        start, end = self.store.runtime.day_rest
        if start <= now < end:
            limit = self.day_rest_limit(now)
            if limit < end:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE world_day_rest SET ends=MIN(ends,?) WHERE id=1 AND started=?",
                        (limit, start),
                    )
                await self.service.world_state.refresh()

    async def set_rhythm(self, schedule, rest):
        """Commit waking and outbound schedules together, preserving module preferences."""
        await self.initialize()
        runtime = self.store.runtime
        async with self._config_lock, runtime.rest_hours_lock:
            now = self.clock()
            fields = self.env_config.model_dump(exclude={"module_settings", "action_modes"})
            if schedule is not None and schedule.keys() - fields.keys():
                raise ValueError("unknown_schedule_field")
            if schedule is not None and "quiet_hours" not in schedule and rest is not None:
                schedule = {**schedule, "quiet_hours": rest.rest_hours}
            cfg = LifeEngineConfig.model_validate(
                {**self.config.model_dump(), **(fields if schedule is None else schedule)}
            )
            overridden = schedule is not None or cfg != self.env_config
            await self.service.world_state.experience.advance(now)
            async with self.store.transaction():
                if overridden:
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO admin_config VALUES ('life_engine',?)",
                        (self.store.vault.seal("admin:life_engine", cfg.model_dump()),),
                    )
                else:
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='life_engine'")
                await runtime.persist_rest_hours(rest)
            self.config = cfg
            self.overridden = overridden
            runtime.apply_rest_override(rest)
            await self.trim_day_rest(now)
            await self.planner.sync_wake(now)
        self.wakeup.set()

    async def set_rest_policy(self, value):
        async with self._config_lock:
            now = self.clock()
            await self.service.world_state.experience.advance(now)
            await self.store.runtime.set_rest_hours(value)
            await self.trim_day_rest(now)
            if self.planner.journal.ready:
                await self.planner.sync_wake(now)
        self.wakeup.set()
        return self.store.runtime.rest_hours_status(now)

    def closing_window(self, local_day: date) -> tuple[int, int] | None:
        windows = self.planner.calendar(local_day, social=True).windows
        if windows:
            start, end = windows[-1]
            return int(max(start, end - 900)), int(end)
        return None

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

    def slot_occurrences(self, module, slot, now):
        local = datetime.fromtimestamp(now, _zone(self.s.bot_timezone))
        if isinstance(slot, HourlyAction):
            start = int(local.replace(minute=0, second=0, microsecond=0).timestamp())
            for stamp in (start, start + 3600):
                day = datetime.fromtimestamp(stamp, local.tzinfo).date()
                yield day, stamp, stamp + 3600, f"timer:{slot.name}:hour:{stamp}"
            return
        for day in (local.date(), local.date() + timedelta(days=1)):
            if window := self.slot_window(module, slot, day):
                start, end = window
                yield day, start, end, f"timer:{slot.name}:{day}:{slot.at or int(start)}"

    async def next_slot_at(self, module, slot, now):
        if not self.config.enabled or (slot.enabled and not slot.enabled()):
            return None
        if not self.slot_available(module, slot):
            return None
        for day, start, end, key in self.slot_occurrences(module, slot, now):
            if now >= end or (slot.attempted and await slot.attempted(day.isoformat())):
                continue
            async with self.store.lock:
                finished = await self.store.one(
                    "SELECT 1 FROM scheduled_intents WHERE dedupe_key=? "
                    "AND status NOT IN ('pending','executing')",
                    (key,),
                )
            if not finished:
                return max(now, start)
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
        day = local.date().isoformat()
        journal = self.planner.journal
        poll = await journal.observe_tick(day, now)
        readers = {}
        for module in self.service.modules.active():
            for slot in self.service.modules.schedules_for(module):
                if poll and slot.receipts:
                    readers[slot.name] = slot.receipts
                occurrence = next(self.slot_occurrences(module, slot, now), None)
                if occurrence is None:
                    continue
                _, start, end, key = occurrence
                if not start <= now < end:
                    continue
                if not self.slot_available(module, slot) or (slot.enabled and not slot.enabled()):
                    continue
                if slot.receipts and await journal.observe_slot(day, slot, start, end):
                    readers[slot.name] = slot.receipts
                ticket = IntentTicketCreate(
                    action_name=slot.name,
                    target_module=module.module_id,
                    action_type=slot.name,
                    origin="timer",
                    target_scope=slot.target_scope,
                    execute_after=start,
                    expire_at=end,
                    dedupe_key=key,
                )
                await self.service.world_state.enqueue_intent(ticket, now=now)
        if readers:
            await asyncio.gather(*(journal.sample_receipts(k, read) for k, read in readers.items()))

    async def run(self) -> None:
        self._running = True
        self.gatekeeper.workers.stopping = False
        next_gate = 0.0
        try:
            while True:
                self.wakeup.clear()
                try:
                    if not self.initialized:
                        await self.initialize()
                    now = self.clock()
                    if self.config.enabled:
                        if now >= next_gate:
                            next_gate = now + 60
                            await self.tick_gatekeeper_once(now=now, wait=False)
                        if now >= self.planner.next_plan_at and "plan-work" not in self._background:
                            self._start_background(
                                "planner", self.trigger_plan_once, now=now, automatic=True
                            )
                    else:
                        self._start_background("rules", self.rules.catch_up, now)
                except Exception:
                    log.warning("event=life_engine_error")
                now = self.clock()
                delay = 60.0
                if self.config.enabled and next_gate:
                    delay = max(0.1, min(delay, next_gate - now))
                    if self.planner.next_plan_at > now:
                        delay = min(delay, self.planner.next_plan_at - now)
                try:
                    await asyncio.wait_for(self.wakeup.wait(), delay)
                except TimeoutError:
                    pass
        finally:
            self._running = False
            await self.close()

    async def close(self) -> None:
        tasks = list(self._background.values())
        for task in tasks:
            task.cancel()
        await self.gatekeeper.workers.close()
        await asyncio.gather(*tasks, return_exceptions=True)
