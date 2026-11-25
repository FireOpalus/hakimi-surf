"""Layer 1 cruise/chat routing, state serialization, and fast survival helpers for `McPlayBrain`."""

from __future__ import annotations

import logging
import math
from typing import Any, Literal

from hakimi_plugin_sdk import PluginRpcError
from hakimi_plugin_sdk.protocol import ScreenDecisionResult, parse_screen_decision_result

from .play_brain_models import (
    _CHAT_DETAIL_HINTS,
    _COOKABLE_RAW_FOODS,
    _EVADE_FAST_MILESTONES,
    _FAST_SURVIVAL_MILESTONES,
    _HOSTILE_ENTITY_TOKENS,
    _MC_CHAT_INTENT_TEMPLATE,
    _MC_CRUISE_GATE_TEMPLATE,
    _PLAYER_NAME_RE,
    _READY_SAFE_FOODS,
    _is_transient_ai_error,
    _sanitize_text,
)

log = logging.getLogger("hakimi_plugin_minecraft")


class _PlayBrainCruiseMixin:
    """Mixin providing Layer 1 gate screening, cruise scheduling, and state snapshots."""

    def _compute_cruise_interval(self, snap: Any, now: float) -> float:
        base_interval = (
            self.config.cruise_interval_seconds
            if snap.nearby_players
            else self.config.solo_cruise_interval_seconds
        )
        self._prune_calls(now)
        if self._first_cruise_pending:
            return min(
                base_interval,
                max(0.01, self.config.initial_cruise_delay_seconds),
            )
        reflex = set(getattr(snap, "reflex_modes", ()) or ())
        activity = str(getattr(snap, "activity", "") or "").strip()
        tod = str(getattr(snap, "time_of_day", "") or "").strip().lower()
        is_sheltered_hold = ("sheltered" in reflex or activity == "掩体避险") and tod in (
            "night",
            "sunset",
            "dusk",
            "",
        )
        if is_sheltered_hold:
            return base_interval * max(1.0, float(self.config.sheltered_cruise_multiplier))
        is_evade_hold = (
            self._evade_backoff_active
            or "evade_active" in reflex
            or activity in ("躲避怪物", "举盾躲避", "远离苦力怕", "远离威胁")
        )
        if is_evade_hold:
            return base_interval * max(1.0, float(self.config.evade_cruise_multiplier))
        if self._chain_next_step and len(self._call_times) < max(
            1, self.config.max_calls_per_10m - 2
        ):
            return min(
                base_interval,
                max(0.01, self.config.chained_step_interval_seconds),
            )
        if self._recovery_retry_pending and len(self._call_times) < max(
            1, self.config.max_calls_per_10m - 2
        ):
            return min(
                base_interval,
                max(0.01, self.config.recovery_step_interval_seconds),
            )
        return base_interval

    async def _check_budget_and_rate(self, now: float) -> str | None:
        self._prune_calls(now)
        if len(self._call_times) >= max(1, self.config.max_calls_per_10m):
            return "rate_limited"
        try:
            budget = await self.plugin.client.ai_budget_check("proactive")
        except PluginRpcError as exc:
            return exc.code
        if isinstance(budget, dict) and (
            budget.get("hard_stop") or not budget.get("available", True)
        ):
            return "budget_exhausted"
        return None

    @staticmethod
    def _has_creeper_or_critical_hp(snap: Any) -> bool:
        hp = getattr(snap, "health", None)
        if isinstance(hp, (int, float)) and not isinstance(hp, bool) and hp <= 7:
            return True
        entities = getattr(snap, "nearby_entities", ()) or ()
        return any("creeper" in str(ent).lower() for ent in entities)

    @staticmethod
    def _inventory_count(snap: Any, item_id: str) -> int:
        inv = getattr(snap, "inventory_summary", ()) or ()
        prefix = f"{item_id}:"
        total = 0
        for entry in inv:
            if isinstance(entry, str) and entry.startswith(prefix):
                try:
                    total += int(entry.split(":", 1)[1])
                except ValueError:
                    pass
        return total

    def _record_last_turn_snapshot(self, snap: Any, action: str) -> None:
        pos = getattr(snap, "pos", None)
        pos_tuple = (
            (float(pos[0]), float(pos[1]), float(pos[2]))
            if isinstance(pos, (list, tuple))
            and len(pos) == 3
            and all(isinstance(v, (int, float)) and math.isfinite(float(v)) for v in pos)
            else None
        )
        self._last_turn_snapshot = {
            "ts": self._now(),
            "health": getattr(snap, "health", None),
            "food": getattr(snap, "food", None),
            "pos": pos_tuple,
            "action": action,
        }

    def _compute_since_last_turn(self, snap: Any) -> str:
        prev = self._last_turn_snapshot
        if not prev:
            return ""
        parts: list[str] = []
        elapsed = max(0, int(round(self._now() - float(prev.get("ts") or 0.0))))
        if elapsed > 0:
            parts.append(f"elapsed={elapsed}s")
        prev_hp = prev.get("health")
        cur_hp = getattr(snap, "health", None)
        if isinstance(prev_hp, (int, float)) and isinstance(cur_hp, (int, float)):
            hp_note = "(unchanged)" if abs(cur_hp - prev_hp) < 0.05 else ""
            parts.append(f"hp:{prev_hp:.1f}->{cur_hp:.1f}{hp_note}")
        prev_food = prev.get("food")
        cur_food = getattr(snap, "food", None)
        if isinstance(prev_food, (int, float)) and isinstance(cur_food, (int, float)):
            parts.append(f"food:{int(round(prev_food))}->{int(round(cur_food))}")
        prev_pos = prev.get("pos")
        cur_pos = getattr(snap, "pos", None)
        if (
            isinstance(prev_pos, tuple)
            and isinstance(cur_pos, (list, tuple))
            and len(cur_pos) == 3
            and all(isinstance(v, (int, float)) and math.isfinite(float(v)) for v in cur_pos)
        ):
            moved = math.dist(prev_pos, (float(cur_pos[0]), float(cur_pos[1]), float(cur_pos[2])))
            parts.append(f"moved:{moved:.1f}m")
        return ", ".join(parts)[:96]

    def _state_payload(self, snap: Any) -> dict[str, Any]:
        raw_eq = getattr(snap, "equipment", None)
        equipment = dict(raw_eq) if isinstance(raw_eq, dict) else {}
        raw_inv = getattr(snap, "inventory_summary", None)
        inv = list(raw_inv[:16]) if isinstance(raw_inv, (list, tuple)) else []
        raw_res = getattr(snap, "nearby_resources", None)
        resources = list(raw_res[:12]) if isinstance(raw_res, (list, tuple)) else []
        raw_reflex = getattr(snap, "reflex_modes", None)
        reflex_modes = list(raw_reflex[:8]) if isinstance(raw_reflex, (list, tuple)) else []
        payload: dict[str, Any] = {
            "dimension": snap.dimension,
            "pos": list(snap.pos) if isinstance(snap.pos, tuple) else snap.pos,
            "health": snap.health,
            "food": snap.food,
            "time_of_day": getattr(snap, "time_of_day", ""),
            "activity": snap.activity,
            "active_task": snap.active_task,
            "equipment": equipment,
            "inventory_summary": inv,
            "free_slots": getattr(snap, "free_slots", None),
            "combat_mode": getattr(snap, "combat_mode", "evade"),
            "guard_target": getattr(snap, "guard_target", ""),
            "nearby_resources": resources,
            "nearby_entities": list(snap.nearby_entities[:5]),
            "nearby_players": list(snap.nearby_players[:6]),
            "recent_chat_or_sign": snap.recent_chat_or_sign,
        }
        if snap.food is not None:
            payload["can_natural_regen"] = bool(snap.food >= 18)
        raw_safe_food = getattr(snap, "safe_food_count", None)
        if isinstance(raw_safe_food, int) and not isinstance(raw_safe_food, bool):
            payload["safe_food_count"] = raw_safe_food
        if getattr(snap, "in_water", False):
            payload["in_water"] = True
        if reflex_modes:
            payload["reflex_modes"] = reflex_modes
        since_last = self._compute_since_last_turn(snap)
        if since_last:
            payload["since_last_turn"] = since_last
        waypoints = self._nearby_waypoints_for_state(snap)
        if waypoints:
            payload["waypoints"] = waypoints
        return payload

    def _append_recent_turn(
        self,
        *,
        trigger: str,
        input_summary: str,
        say: str,
        action: str,
        reason: str,
        outcome: str = "",
    ) -> None:
        max_turns = max(1, self.config.max_history_turns)
        clean_outcome = _sanitize_text(outcome, max_tokens=30)[:80] if outcome else ""
        entry: dict[str, Any] = {
            "trigger": trigger,
            "input": _sanitize_text(input_summary, max_tokens=40),
            "say": say,
            "action": action,
            "reason": reason,
        }
        if clean_outcome:
            entry["outcome"] = clean_outcome
        self.recent_turns.append(entry)
        if len(self.recent_turns) > max_turns:
            self.recent_turns = self.recent_turns[-max_turns:]
        self.action_log.append(
            {
                "ts": round(float(self.plugin.clock()), 1),
                "trigger": _sanitize_text(trigger, max_tokens=10)[:24],
                "action": _sanitize_text(action, max_tokens=10)[:24],
                "outcome": clean_outcome,
                "reason": _sanitize_text(reason, max_tokens=20)[:60],
                "say": _sanitize_text(say, max_tokens=30)[:80] if say else "",
            }
        )

    def _has_ready_food(self, snap: Any) -> bool:
        return any(self._inventory_count(snap, food_id) >= 1 for food_id in _READY_SAFE_FOODS)

    @staticmethod
    def _has_nearby_hostile(snap: Any) -> bool:
        entities = getattr(snap, "nearby_entities", ()) or ()
        for ent in entities:
            lower = str(ent).lower()
            if "(neutral)" in lower:
                continue
            if any(tok in lower for tok in _HOSTILE_ENTITY_TOKENS):
                return True
        return False

    def _has_actionable_food_option(self, snap: Any) -> bool:
        if (
            self._inventory_count(snap, "wheat") >= 3
            or self._inventory_count(snap, "hay_block") >= 1
        ):
            return True
        return any(self._inventory_count(snap, food_id) >= 1 for food_id in _COOKABLE_RAW_FOODS)

    def _has_basic_tool(self, snap: Any) -> bool:
        equip = getattr(snap, "equipment", None)
        main_hand = str(equip.get("main_hand") or "").lower() if isinstance(equip, dict) else ""
        if main_hand.endswith(("_pickaxe", "_sword", "_axe")):
            return True
        inv = getattr(snap, "inventory_summary", ()) or ()
        for entry in inv:
            item_id = str(entry).split(":", 1)[0].strip().lower()
            if item_id.endswith(("_pickaxe", "_sword", "_axe")):
                return True
        return False

    def _recent_death_count(self, now: float, *, window_seconds: float = 300.0) -> int:
        cutoff = now - window_seconds
        while self._recent_death_times and self._recent_death_times[0] < cutoff:
            self._recent_death_times.popleft()
        return len(self._recent_death_times)

    async def _try_fast_survival_milestone(
        self,
        *,
        event_batch: list[dict[str, Any]],
        snap: Any,
        now: float,
    ) -> bool:
        if not event_batch:
            return False
        etypes = [str(ev.get("event_type") or "") for ev in event_batch]
        allowed_fast = _FAST_SURVIVAL_MILESTONES | _EVADE_FAST_MILESTONES
        if not all(et in allowed_fast for et in etypes):
            return False
        cur_mode = getattr(snap, "combat_mode", "evade")
        has_players = bool(getattr(snap, "nearby_players", ()) or ())
        if any(et in _EVADE_FAST_MILESTONES for et in etypes):
            # When already in evade mode (or solo death), handle at 0-Token instead of waking
            # Layer 2 repeatedly to emit no-op evade decisions.
            if cur_mode != "evade" and (has_players or "mc_death" not in etypes):
                return False
        # Allow Layer 2 to craft bread or smelt raw meat on mc_low_food unless currently
        # sheltering at night or actively evading threats.
        if "mc_low_food" in etypes and self._has_actionable_food_option(snap):
            reflex_now = set(getattr(snap, "reflex_modes", ()) or ())
            tod_now = str(getattr(snap, "time_of_day", "") or "").strip().lower()
            in_shelter_night = "sheltered" in reflex_now and tod_now in (
                "night",
                "sunset",
                "dusk",
                "",
            )
            if not (in_shelter_night or "evade_active" in reflex_now or self._evade_backoff_active):
                return False

        clean_say = ""
        if "mc_death" in etypes:
            primary = "mc_death"
            self._chain_next_step = False
            self._recovery_retry_pending = False
            self._recent_death_times.append(now)
            reflex = set(getattr(snap, "reflex_modes", ()) or ())
            if (
                "sheltered" not in reflex
                and self._recent_death_count(now, window_seconds=180.0) >= 2
                and (now - self._last_death_escalate_at) >= 90.0
            ):
                self._last_death_escalate_at = now
                return False
            if cur_mode != "evade":
                await self._dispatch_goal_safe({"action_type": "evade"})
            if has_players and (now - self._last_death_chat_at) >= 60.0:
                self._last_death_chat_at = now
                clean_say = "呜哇不小心被怪打回重生点了，我先小心躲开喵~"
        elif "mc_health_alert" in etypes:
            primary = "mc_health_alert"
            self._chain_next_step = False
            self._recovery_retry_pending = False
            if (now - self._last_evade_flee_at) >= 15.0:
                self._last_evade_flee_at = now
                await self._dispatch_goal_safe({"action_type": "evade"})
            if (
                has_players
                and (now - self._last_combat_retreat_chat_at)
                >= self.config.combat_retreat_cooldown_seconds
            ):
                self._last_combat_retreat_chat_at = now
                clean_say = "血量告急！我先往远处拉开距离躲一下喵~"
        elif "mc_creeper_alert" in etypes:
            primary = "mc_creeper_alert"
            if (
                has_players
                and (now - self._last_creeper_alert_chat_at)
                >= self.config.creeper_alert_cooldown_seconds
            ):
                self._last_creeper_alert_chat_at = now
                clean_say = "小心！附近有苦力怕，我先拉开距离躲着点喵！"
        elif "mc_combat_retreat" in etypes:
            primary = "mc_combat_retreat"
            if (
                has_players
                and (now - self._last_combat_retreat_chat_at)
                >= self.config.combat_retreat_cooldown_seconds
            ):
                self._last_combat_retreat_chat_at = now
                clean_say = "血量有点危险，我先撤退吃口东西回回血喵！"
        else:
            primary = "mc_low_food"
            low_ev = next(
                (ev for ev in reversed(event_batch) if ev.get("event_type") == primary), {}
            )
            safe_cnt = low_ev.get("safe_food_count")
            if (
                has_players
                and (now - self._last_low_food_chat_at) >= self.config.low_food_cooldown_seconds
            ):
                self._last_low_food_chat_at = now
                clean_say = (
                    "背包里没吃的啦，好饿喵~"
                    if safe_cnt == 0
                    else "包里吃的快见底啦，待会儿得找点食物补充一下喵~"
                )

        if clean_say:
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
        self._last_reason = f"fast_milestone:{primary}"
        self._record_turn_recap(event_batch=event_batch, clean_say=clean_say)
        await self._push_status_safe()
        return True

    async def _screen_if_needed(
        self,
        *,
        trigger: str,
        snap: Any,
        chat_batch: list[tuple[str, str]],
        event_batch: list[dict[str, Any]],
    ) -> tuple[Literal["passed", "blocked", "transient"], str, ScreenDecisionResult | None]:
        template_id: str | None = None
        if trigger == "cruise":
            template_id = _MC_CRUISE_GATE_TEMPLATE
            screen_payload: dict[str, Any] = {
                "trigger": trigger,
                "session_intent": _sanitize_text(self.plugin.session_intent, max_tokens=40),
                "state": self._state_payload(snap),
                "recent_turns": list(self.recent_turns[-self.config.max_history_turns :]),
            }
        elif trigger == "player_chat" and chat_batch:
            template_id = _MC_CHAT_INTENT_TEMPLATE
            screen_payload = {
                "trigger": trigger,
                "player_chats": [{"player": p, "message": m} for p, m in chat_batch],
                "combat_mode": getattr(snap, "combat_mode", "evade"),
                "guard_target": getattr(snap, "guard_target", ""),
            }
            if snap.recent_chat_or_sign:
                screen_payload["recent_chat_or_sign"] = snap.recent_chat_or_sign
        else:
            if not chat_batch and not snap.recent_chat_or_sign:
                return "passed", "", None
            screen_payload = {"trigger": trigger}
            if chat_batch:
                screen_payload["player_chats"] = [
                    {"player": p, "message": m} for p, m in chat_batch
                ]
            if snap.recent_chat_or_sign:
                screen_payload["recent_chat_or_sign"] = snap.recent_chat_or_sign
            if event_batch:
                screen_payload["events"] = event_batch

        screen_kwargs: dict[str, Any] = {"budget_lane": "proactive", "purpose": "source"}
        if template_id is not None:
            screen_kwargs["decision_template"] = template_id

        try:
            res = await self.plugin.client.ai_screen(screen_payload, **screen_kwargs)
        except PluginRpcError as exc:
            if _is_transient_ai_error(exc.code):
                return "transient", exc.code, None
            return "blocked", exc.code, None

        parsed = parse_screen_decision_result(res)
        if not parsed.passed:
            return "blocked", "passed_false", None
        return "passed", "", parsed

    @staticmethod
    def _receipt_ok(res: dict[str, Any] | None) -> bool:
        return isinstance(res, dict) and res.get("ok") is True and res.get("accepted", True) is True

    def _needs_model_judgment_on_idle(self, snap: Any) -> str:
        """Return a non-empty escalation reason when Layer 1 should not silently skip Layer 2."""
        if str(getattr(snap, "active_task", "") or "").strip():
            return ""
        reflex = set(getattr(snap, "reflex_modes", ()) or ())
        if "sheltered" in reflex:
            return ""
        if getattr(snap, "in_water", False):
            return "in_water_needs_land"
        hp = getattr(snap, "health", None)
        food = getattr(snap, "food", None)
        raw_safe = getattr(snap, "safe_food_count", None)
        no_safe_food = (
            raw_safe == 0 if isinstance(raw_safe, int) else not self._has_ready_food(snap)
        )
        if (
            isinstance(hp, (int, float))
            and not isinstance(hp, bool)
            and hp <= 10
            and isinstance(food, (int, float))
            and not isinstance(food, bool)
            and food < 18
            and no_safe_food
        ):
            return "low_hp_cannot_regen_no_food"
        if len(self.recent_turns) >= 2 and all(
            t.get("action") in ("stay", "idle", "observe") for t in self.recent_turns[-2:]
        ):
            return "stalled_idle_turns"
        if not getattr(snap, "nearby_players", ()):
            now = self._now()
            if (
                self._recent_death_count(now, window_seconds=300.0) >= 2
                and (now - self._last_death_escalate_at) >= 90.0
            ):
                self._last_death_escalate_at = now
                return "recent_deaths_need_shelter_or_gear"
            if self._consecutive_cruise_skips >= 2 and (
                not self._has_basic_tool(snap)
                or bool(self._active_plan)
                or bool(self.plugin.session_intent)
            ):
                return "solo_progression_after_skips"
        return ""

    async def _route_cruise_decision(
        self,
        parsed: ScreenDecisionResult | None,
        *,
        snap: Any,
    ) -> tuple[Literal["skip", "handled", "escalate"], dict[str, Any] | None]:
        if parsed is None or not parsed.decisions:
            return "escalate", None

        min_conf = self.config.decision_min_confidence
        direct_conf = self.config.decision_direct_action_confidence
        replan = parsed.get_noul("should_replan", min_confidence=min_conf)
        stance = parsed.get_choice("stance", min_confidence=min_conf)
        stance_conf = parsed.confidence_of("stance")
        cur_mode = getattr(snap, "combat_mode", "evade")
        cur_guard = getattr(snap, "guard_target", "")

        # 1. High-confidence emergency defensive retreat takes precedence over no-replan skip.
        if stance == "retreat_eat" and stance_conf >= direct_conf:
            has_ready_food = self._has_ready_food(snap)
            has_hostile = self._has_nearby_hostile(snap)
            is_night = str(getattr(snap, "time_of_day", "") or "").lower() in {"night", "dusk"}
            in_water = bool(getattr(snap, "in_water", False))
            if cur_mode == "evade" and (
                in_water or (not has_ready_food and (not has_hostile or is_night))
            ):
                return (
                    "escalate",
                    {
                        "template": _MC_CRUISE_GATE_TEMPLATE,
                        "stance": "retreat_eat",
                        "already_evading": True,
                        "hint": (
                            "in_water_needs_land"
                            if in_water
                            else "already_in_evade_choose_shelter_or_gather"
                        ),
                    },
                )
            if cur_mode != "evade" or has_hostile:
                res = await self._dispatch_goal_result({"action_type": "evade"})
                if not self._receipt_ok(res):
                    reason = str((res or {}).get("reason") or "dispatch_rejected")
                    self._last_reason = f"cruise_gate:retreat_rejected:{reason}"[:60]
                    await self._push_status_safe()
                    return "handled", None
            self.layer1_routed_count += 1
            self._evade_backoff_active = True
            self._chain_next_step = False
            self._recovery_retry_pending = False
            self._last_reason = "cruise_gate:retreat_eat"
            self._append_recent_turn(
                trigger="cruise",
                input_summary="cruise",
                say="",
                action="evade",
                reason="cruise_gate:retreat_eat",
            )
            await self._push_status_safe()
            return "handled", None

        # 2. Confident keep_current or should_replan=False -> skip Layer 2 unless stuck/stalled.
        if stance == "keep_current" or replan is False:
            escalate_hint = self._needs_model_judgment_on_idle(snap)
            if escalate_hint:
                return (
                    "escalate",
                    {
                        "template": _MC_CRUISE_GATE_TEMPLATE,
                        "stance": stance or "no_replan",
                        "hint": escalate_hint,
                    },
                )
            self._chain_next_step = False
            self._recovery_retry_pending = False
            self._consecutive_cruise_skips += 1
            self.cruise_skipped_count += 1
            self._last_reason = (
                "cruise_gate:keep_current" if stance == "keep_current" else "cruise_gate:no_replan"
            )
            await self._push_status_safe()
            return "skip", None

        # 3. High-confidence pure combat mode switches -> handle at Layer 1 without waking Layer 2.
        if stance in ("guard_player", "fight_hostiles") and stance_conf >= direct_conf:
            if self._has_creeper_or_critical_hp(snap):
                if cur_mode == "guard":
                    await self._dispatch_goal_result({"action_type": "evade"})
                self.layer1_routed_count += 1
                self._evade_backoff_active = True
                self._chain_next_step = False
                self._recovery_retry_pending = False
                self._last_reason = "cruise_gate:safety_conservative_evade"
                await self._push_status_safe()
                return "handled", None

            target = ""
            if stance == "guard_player":
                nearby = getattr(snap, "nearby_players", ()) or ()
                target = next((p for p in nearby if _PLAYER_NAME_RE.fullmatch(str(p))), "")
            if cur_mode == "guard" and (not target or cur_guard == target):
                self.cruise_skipped_count += 1
                self._last_reason = f"cruise_gate:{stance}:already_active"
                await self._push_status_safe()
                return "skip", None

            params: dict[str, Any] = {"action_type": "guard"}
            if target:
                params["target_player"] = target
            res = await self._dispatch_goal_result(params)
            if not self._receipt_ok(res):
                reason = str((res or {}).get("reason") or "dispatch_rejected")
                log.warning(
                    "event=mc_brain_cruise_dispatch_rejected stance=%s reason=%s", stance, reason
                )
                self._last_reason = f"cruise_gate:{stance}_rejected:{reason}"[:60]
                await self._push_status_safe()
                return "handled", None
            self.layer1_routed_count += 1
            self._evade_backoff_active = False
            self._last_reason = f"cruise_gate:{stance}"
            self._append_recent_turn(
                trigger="cruise",
                input_summary="cruise",
                say="",
                action="guard",
                reason=self._last_reason,
            )
            await self._push_status_safe()
            return "handled", None

        # 4. Parameterized actions (gather/craft_gear) or mid-confidence stances -> Layer 2.
        if stance is not None:
            return (
                "escalate",
                {
                    "template": _MC_CRUISE_GATE_TEMPLATE,
                    "stance": stance,
                    "confidence": round(stance_conf, 3),
                },
            )

        # 5. Low-confidence stance degradation: escalate if should_replan is confident True,
        # otherwise fall back to conservative mode without spending Layer 2 tokens.
        if replan is True:
            return (
                "escalate",
                {
                    "template": _MC_CRUISE_GATE_TEMPLATE,
                    "stance": "uncertain",
                    "confidence": round(stance_conf, 3),
                },
            )

        hp = getattr(snap, "health", None)
        if (
            isinstance(hp, (int, float))
            and not isinstance(hp, bool)
            and hp <= 10
            and cur_mode == "guard"
        ):
            await self._dispatch_goal_safe({"action_type": "evade"})
            self._consecutive_cruise_skips += 1
            self.cruise_skipped_count += 1
            self._last_reason = "cruise_gate:low_confidence_conservative"
            await self._push_status_safe()
            return "skip", None
        escalate_hint = self._needs_model_judgment_on_idle(snap)
        if escalate_hint:
            return (
                "escalate",
                {
                    "template": _MC_CRUISE_GATE_TEMPLATE,
                    "stance": "uncertain",
                    "confidence": round(stance_conf, 3),
                    "hint": escalate_hint,
                },
            )
        self._consecutive_cruise_skips += 1
        self.cruise_skipped_count += 1
        self._last_reason = "cruise_gate:low_confidence_conservative"
        await self._push_status_safe()
        return "skip", None

    async def _route_chat_intent(
        self,
        parsed: ScreenDecisionResult | None,
        *,
        snap: Any,
        chat_batch: list[tuple[str, str]],
        speaking_player: str,
    ) -> tuple[Literal["handled", "escalate"], dict[str, Any] | None]:
        if parsed is None or not parsed.decisions:
            return "escalate", None

        min_conf = self.config.decision_min_confidence
        direct_conf = self.config.decision_direct_action_confidence
        intent = parsed.get_choice("chat_intent", min_confidence=min_conf)
        intent_conf = parsed.confidence_of("chat_intent")
        if intent is None:
            return "escalate", None

        combined_msg = " ".join(msg for _, msg in chat_batch).strip()
        is_simple_mode_cmd = len(combined_msg) <= 24 and not any(
            hint in combined_msg for hint in _CHAT_DETAIL_HINTS
        )
        input_summary = "; ".join(f"{p}:{m}" for p, m in chat_batch)

        if intent == "evade_retreat" and intent_conf >= direct_conf and is_simple_mode_cmd:
            res = await self._dispatch_goal_result({"action_type": "evade"})
            if not self._receipt_ok(res):
                reason = str((res or {}).get("reason") or "dispatch_rejected")
                log.warning(
                    "event=mc_brain_chat_intent_rejected intent=evade_retreat reason=%s", reason
                )
                self._last_reason = f"chat_intent:evade_rejected:{reason}"[:60]
                await self._push_status_safe()
                return "handled", None
            clean_say = "好哒，我先切到躲避模式注意安全喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self.layer1_routed_count += 1
            self._evade_backoff_active = True
            self._chain_next_step = False
            self._recovery_retry_pending = False
            self._last_reason = "chat_intent:evade_retreat"
            self._append_recent_turn(
                trigger="player_chat",
                input_summary=input_summary,
                say=clean_say,
                action="evade",
                reason=self._last_reason,
            )
            self._record_turn_recap(chat_batch=chat_batch, clean_say=clean_say)
            await self._push_status_safe()
            return "handled", None

        if (
            intent == "guard_player"
            and intent_conf >= direct_conf
            and is_simple_mode_cmd
            and not self._has_creeper_or_critical_hp(snap)
        ):
            target = speaking_player.strip()
            if not _PLAYER_NAME_RE.fullmatch(target):
                nearby = getattr(snap, "nearby_players", ()) or ()
                target = next((p for p in nearby if _PLAYER_NAME_RE.fullmatch(str(p))), "")
            params: dict[str, Any] = {"action_type": "guard"}
            if _PLAYER_NAME_RE.fullmatch(target):
                params["target_player"] = target
            res = await self._dispatch_goal_result(params)
            if not self._receipt_ok(res):
                reason = str((res or {}).get("reason") or "dispatch_rejected")
                log.warning(
                    "event=mc_brain_chat_intent_rejected intent=guard_player reason=%s", reason
                )
                self._last_reason = f"chat_intent:guard_rejected:{reason}"[:60]
                await self._push_status_safe()
                return "handled", None
            clean_say = "收到，猫猫切到护卫模式护着你喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self.layer1_routed_count += 1
            self._evade_backoff_active = False
            self._last_reason = "chat_intent:guard_player"
            self._append_recent_turn(
                trigger="player_chat",
                input_summary=input_summary,
                say=clean_say,
                action="guard",
                reason=self._last_reason,
            )
            self._record_turn_recap(chat_batch=chat_batch, clean_say=clean_say)
            await self._push_status_safe()
            return "handled", None

        return (
            "escalate",
            {
                "template": _MC_CHAT_INTENT_TEMPLATE,
                "chat_intent": intent,
                "confidence": round(intent_conf, 3),
            },
        )

    def _build_generate_payload(
        self,
        *,
        trigger: str,
        snap: Any,
        chat_batch: list[tuple[str, str]],
        event_batch: list[dict[str, Any]],
        routing_hint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "trigger": trigger,
            "session_intent": _sanitize_text(self.plugin.session_intent, max_tokens=40),
            "state": self._state_payload(snap),
            "recent_turns": list(self.recent_turns[-self.config.max_history_turns :]),
        }
        if self._active_plan:
            payload["active_plan"] = self._active_plan
        if chat_batch:
            payload["player_chats"] = [{"player": p, "message": m} for p, m in chat_batch]
        if event_batch:
            payload["events"] = event_batch
        if routing_hint:
            payload["routing_hint"] = dict(routing_hint)
        return payload

    async def _handle_unavailable(
        self,
        *,
        chat_batch: list[tuple[str, str]],
        snap: Any,
        speaking_player: str,
        reason: str,
    ) -> None:
        if chat_batch:
            await self._run_local_fallback(
                chat_batch, snap=snap, speaking_player=speaking_player, reason=reason
            )
        else:
            self._last_reason = _sanitize_text(reason, max_tokens=20)
            await self._push_status_safe()
