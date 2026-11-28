"""Action dispatch, outcome formatting, decision execution, and fallback for `McPlayBrain`."""

from __future__ import annotations

import logging
import math
from typing import Any

from hakimi_plugin_sdk import PluginRpcError

from .play_brain_models import (
    _BLOCK_TYPE_RE,
    _CHAINABLE_ACTIONS,
    _GOTO_RELAY_PLAN_PREFIX,
    _HUNTABLE_MOBS,
    _ITEM_NAME_RE,
    _LOCAL_EVADE_KEYWORDS,
    _LOCAL_FOLLOW_KEYWORDS,
    _LOCAL_GREETING_KEYWORDS,
    _LOCAL_GUARD_KEYWORDS,
    _LOCAL_STOP_KEYWORDS,
    _PLAYER_NAME_RE,
    _SHELTER_BLOCKED_REASONS,
    McBrainDecision,
    _is_threat_interrupt_reason,
    _sanitize_chat_say,
    _sanitize_text,
)

log = logging.getLogger("hakimi_plugin_minecraft")


class _PlayBrainActionsMixin:
    """Mixin providing bridge goal dispatch, outcome formatting, and Layer 2 decision apply."""

    async def _dispatch_goal_result(self, params: dict[str, Any]) -> dict[str, Any] | None:
        try:
            res = await self.plugin._call_bridge("mc.dispatch_goal", params)
            return res if isinstance(res, dict) else {}
        except PluginRpcError as exc:
            log.warning(
                "event=mc_brain_dispatch_failed action=%s code=%s",
                params.get("action_type"),
                exc.code,
            )
            return None

    async def _dispatch_goal_safe(self, params: dict[str, Any]) -> bool:
        return (await self._dispatch_goal_result(params)) is not None

    @staticmethod
    def _format_gather_outcome(res: dict[str, Any] | None, action: str, block: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            collected = res.get("collected_count")
            target = str(res.get("block") or block or "block").strip()
            if isinstance(collected, int) and not isinstance(collected, bool) and collected > 0:
                return f"{action}:{target}x{collected}"
            return f"{action}:{target}"
        reason = res.get("reason")
        if reason == "tool_tier_too_low":
            req_tool = str(res.get("required_tool") or "").strip()
            if _ITEM_NAME_RE.fullmatch(req_tool):
                return f"{action}:need_{req_tool}"
        if isinstance(reason, str) and reason.strip():
            return f"{action}:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_hunt_outcome(res: dict[str, Any] | None, target_mob: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            hunted = res.get("hunted_count")
            mob = str(res.get("target_mob") or target_mob or "mob").strip()
            if isinstance(hunted, int) and not isinstance(hunted, bool) and hunted > 0:
                return f"hunted:{mob}x{hunted}"
            return f"hunted:{mob}"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"hunt:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_sleep_outcome(res: dict[str, Any] | None) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            return "slept:home"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"sleep:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_chest_outcome(res: dict[str, Any] | None, mode: str, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            if mode == "inspect":
                items = res.get("chest_summary")
                if isinstance(items, list) and items:
                    return f"chest:{','.join(str(x) for x in items[:4])}"[:80]
                return "chest:empty"
            moved = res.get("moved_count")
            itm = str(res.get("item_name") or item_name or "items").strip()
            if isinstance(moved, int) and not isinstance(moved, bool) and moved > 0:
                return f"chest:{mode}:{itm}x{moved}"
            return f"chest:{mode}:ok"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"chest:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_place_outcome(res: dict[str, Any] | None, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            return f"placed:{item_name}"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"place:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_give_outcome(res: dict[str, Any] | None, target_player: str, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            given = res.get("given_count")
            qty = f"x{given}" if isinstance(given, int) and not isinstance(given, bool) else ""
            return f"gave:{item_name}{qty}@{target_player}"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"give:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_smelt_outcome(res: dict[str, Any] | None, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        raw_missing = res.get("missing_materials")
        if isinstance(raw_missing, list) and raw_missing:
            parts: list[str] = []
            for entry in raw_missing[:4]:
                if isinstance(entry, dict):
                    mat = str(entry.get("item") or "").strip()
                    shortfall = entry.get("shortfall")
                    if (
                        _ITEM_NAME_RE.fullmatch(mat)
                        and isinstance(shortfall, int)
                        and shortfall > 0
                    ):
                        parts.append(f"{mat}x{shortfall}")
            if parts:
                return f"missing:{','.join(parts)}"
        if res.get("ok") is True:
            smelted = res.get("smelted_count")
            out_item = str(res.get("output_item") or item_name).strip()
            if isinstance(smelted, int) and not isinstance(smelted, bool) and smelted > 0:
                return f"smelted:{out_item}x{smelted}"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"smelt:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_shelter_outcome(res: dict[str, Any] | None, mode: str) -> str:
        if not isinstance(res, dict):
            return ""
        if res.get("ok") is True:
            eff = str(res.get("mode") or mode or "enter").strip()
            return "shelter:exited" if eff == "exit" else "shelter:entered"
        reason = res.get("reason")
        if reason == "tool_tier_too_low":
            req_tool = str(res.get("required_tool") or "").strip()
            if _ITEM_NAME_RE.fullmatch(req_tool):
                return f"shelter:need_{req_tool}"
        if isinstance(reason, str) and reason.strip():
            return f"shelter:{reason.strip()[:32]}"
        return ""

    @staticmethod
    def _format_acquire_outcome(res: dict[str, Any] | None, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        target = str(res.get("item_name") or item_name or "item").strip()
        steps = res.get("steps_completed")
        step_cnt = len(steps) if isinstance(steps, list) else 0
        if res.get("ok") is True:
            if res.get("already_owned") is True:
                return f"acquire:{target}:already_owned"
            acq = res.get("acquired_count")
            qty = (
                f"x{acq}" if isinstance(acq, int) and not isinstance(acq, bool) and acq > 0 else ""
            )
            suffix = f"({step_cnt}steps)" if step_cnt > 1 else ""
            return f"acquired:{target}{qty}{suffix}"[:80]
        reason = str(res.get("reason") or "failed").strip()[:28]
        blocker = str(res.get("blocker_item") or "").strip()
        blocker_part = f":{blocker}" if _ITEM_NAME_RE.fullmatch(blocker) else ""
        wood_logs = res.get("wood_shortfall_logs")
        logs_suffix = (
            f",need_logs:{wood_logs}"
            if isinstance(wood_logs, int) and not isinstance(wood_logs, bool) and wood_logs > 0
            else ""
        )
        partial_part = f"(partial:{step_cnt}steps)" if step_cnt > 0 else ""
        return f"acquire:{reason}{blocker_part}{logs_suffix}{partial_part}"[:80]

    @staticmethod
    def _format_craft_outcome(res: dict[str, Any] | None, item_name: str) -> str:
        if not isinstance(res, dict):
            return ""
        wood_logs = res.get("wood_shortfall_logs")
        logs_suffix = (
            f",need_logs:{wood_logs}"
            if isinstance(wood_logs, int) and not isinstance(wood_logs, bool) and wood_logs > 0
            else ""
        )
        raw_missing = res.get("missing_materials")
        if isinstance(raw_missing, list) and raw_missing:
            parts: list[str] = []
            for entry in raw_missing[:4]:
                if isinstance(entry, dict):
                    mat = str(entry.get("item") or "").strip()
                    shortfall = entry.get("shortfall")
                    if (
                        _ITEM_NAME_RE.fullmatch(mat)
                        and isinstance(shortfall, int)
                        and shortfall > 0
                    ):
                        parts.append(f"{mat}x{shortfall}")
            if parts:
                return f"missing:{','.join(parts)}{logs_suffix}"
        if res.get("ok") is True:
            crafted = res.get("crafted_count")
            if isinstance(crafted, int) and not isinstance(crafted, bool) and crafted > 0:
                return f"crafted:{item_name}x{crafted}"
        reason = res.get("reason")
        if isinstance(reason, str) and reason.strip():
            return f"craft:{reason.strip()[:32]}{logs_suffix}"
        return ""

    @staticmethod
    def _is_bed_item(item_name: str) -> bool:
        norm = str(item_name or "").strip().lower()
        return norm == "bed" or norm.endswith("_bed")

    @staticmethod
    def _inventory_log_count(snap: Any) -> int:
        inv = getattr(snap, "inventory_summary", ()) or ()
        total = 0
        for entry in inv:
            if not isinstance(entry, str) or ":" not in entry:
                continue
            item_id, raw_cnt = entry.split(":", 1)
            item_id = item_id.strip().lower()
            if item_id.endswith(("_log", "_stem", "_wood")) or item_id == "bamboo_block":
                try:
                    total += int(raw_cnt)
                except ValueError:
                    pass
        return total

    @classmethod
    def _inventory_wood_planks_equivalent(cls, snap: Any) -> int:
        inv = getattr(snap, "inventory_summary", ()) or ()
        planks = 0
        for entry in inv:
            if not isinstance(entry, str) or ":" not in entry:
                continue
            item_id, raw_cnt = entry.split(":", 1)
            item_id = item_id.strip().lower()
            if item_id.endswith("_planks"):
                try:
                    planks += int(raw_cnt)
                except ValueError:
                    pass
        return planks + cls._inventory_log_count(snap) * 4

    def _can_acquire_in_shelter(self, snap: Any, item_name: str) -> bool:
        """Allow only in-inventory 2x2 crafts with already-owned materials inside a shelter pit."""
        norm = str(item_name or "").strip().lower()
        if not norm or self._is_bed_item(norm):
            return False
        if norm == "stick":
            return self._inventory_wood_planks_equivalent(snap) >= 2
        if norm == "torch":
            has_fuel = (
                self._inventory_count(snap, "coal") + self._inventory_count(snap, "charcoal") >= 1
            )
            has_stick = (
                self._inventory_count(snap, "stick") >= 1
                or self._inventory_wood_planks_equivalent(snap) >= 2
            )
            return has_fuel and has_stick
        if norm.endswith("_planks"):
            return self._inventory_log_count(snap) >= 1
        return False

    @staticmethod
    def _extract_pos_tuple(raw: Any) -> tuple[float, float, float] | None:
        if isinstance(raw, dict) and all(k in raw for k in ("x", "y", "z")):
            raw = (raw["x"], raw["y"], raw["z"])
        if isinstance(raw, (list, tuple)) and len(raw) == 3:
            vals: list[float] = []
            for v in raw:
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    return None
                fv = float(v)
                if not math.isfinite(fv):
                    return None
                vals.append(fv)
            return (vals[0], vals[1], vals[2])
        return None

    @staticmethod
    def _format_goto_partial_outcome(
        *,
        is_segmented: bool,
        moved_int: int | None,
        target_coords: str,
        way_coords: str,
        rem_dist: float,
    ) -> str:
        if not is_segmented:
            if moved_int is not None:
                return f"goto:partial({moved_int}m)"
            return f"goto:partial({target_coords})"
        rem_int = int(round(rem_dist))
        if moved_int is not None:
            return f"goto:partial({moved_int}m,rem={rem_int}m)"
        return f"goto:partial({way_coords},rem={rem_int}m)"

    def _compute_goto_relay_waypoint(
        self,
        cur_pos: tuple[float, float, float],
        target: tuple[float, float, float],
    ) -> tuple[tuple[float, float, float], bool]:
        """Compute next safe goto waypoint using XZ-first progress, then Y convergence."""
        cx, cy, cz = cur_pos
        tx, ty, tz = target
        seg_len = max(16.0, min(64.0, float(self.config.goto_segment_distance)))
        y_horiz_thresh = max(4.0, float(self.config.goto_y_converge_horiz))
        dist = math.dist(cur_pos, target)
        if dist <= seg_len:
            return target, False
        dx = tx - cx
        dy = ty - cy
        dz = tz - cz
        horiz = math.hypot(dx, dz)
        if horiz > seg_len:
            ratio = seg_len / horiz
            way_x = float(round(cx + dx * ratio))
            way_z = float(round(cz + dz * ratio))
            way_y = float(round(cy))
            return (way_x, way_y, way_z), True
        if horiz > y_horiz_thresh:
            return (float(round(tx)), float(round(cy)), float(round(tz))), True
        step_y = max(-seg_len, min(seg_len, dy))
        way_y = float(round(cy + step_y))
        return (float(round(tx)), way_y, float(round(tz))), True

    async def _apply_decision(
        self,
        decision: McBrainDecision,
        *,
        snap: Any,
        speaking_player: str,
        trigger: str,
        input_summary: str,
        chat_batch: list[tuple[str, str]] | None = None,
        event_batch: list[dict[str, Any]] | None = None,
    ) -> None:
        clean_say = _sanitize_chat_say(decision.say, max_len=120)
        if clean_say:
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})

        action = decision.action
        outcome_note = ""
        action_ok = False
        waypoints_dirty = False
        last_res: dict[str, Any] | None = None
        shelter_guarded = False
        relay_terminated = False
        cur_dim = str(getattr(snap, "dimension", "") or "overworld").strip().lower() or "overworld"

        if action == "stay":
            await self._dispatch_goal_safe({"action_type": "stop"})
        elif action == "follow":
            candidate = decision.target_player.strip() or speaking_player.strip()
            if _PLAYER_NAME_RE.fullmatch(candidate):
                res = await self._dispatch_goal_result(
                    {"action_type": "follow", "target_player": candidate}
                )
                last_res = res
                action_ok = self._receipt_ok(res)
                if not action_ok and isinstance(res, dict) and res.get("reason"):
                    outcome_note = f"follow:{str(res['reason'])[:32]}"
            else:
                log.warning("event=mc_brain_follow_rejected target=%r", candidate)
        elif action in ("gather", "mine"):
            block = decision.block_type.strip()
            if _BLOCK_TYPE_RE.fullmatch(block) and len(block) <= 48:
                params: dict[str, Any] = {"action_type": action}
                if block:
                    params["block_type"] = block
                if decision.count > 0:
                    params["count"] = max(1, min(8, decision.count))
                gather_res = await self._dispatch_goal_result(params)
                last_res = gather_res
                action_ok = self._receipt_ok(gather_res)
                outcome_note = self._format_gather_outcome(gather_res, action, block)
            else:
                log.warning("event=mc_brain_block_rejected block=%r", block)
        elif action == "craft":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                if self._is_sheltered(snap) and self._is_bed_item(raw_item):
                    shelter_guarded = True
                    action_ok = False
                    outcome_note = f"craft:blocked_in_shelter:{raw_item}"[:80]
                    log.info("event=mc_brain_craft_blocked_in_shelter item=%s", raw_item)
                else:
                    craft_count = max(1, min(16, decision.count if decision.count > 0 else 1))
                    craft_res = await self._dispatch_goal_result(
                        {"action_type": "craft", "item_name": raw_item, "count": craft_count}
                    )
                    last_res = craft_res
                    action_ok = self._receipt_ok(craft_res)
                    outcome_note = self._format_craft_outcome(craft_res, raw_item)
            else:
                log.warning("event=mc_brain_craft_rejected item=%r", raw_item)
        elif action == "acquire":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                if self._is_sheltered(snap) and not self._can_acquire_in_shelter(snap, raw_item):
                    shelter_guarded = True
                    action_ok = False
                    outcome_note = f"acquire:blocked_in_shelter:{raw_item}"[:80]
                    log.info(
                        "event=mc_brain_acquire_blocked_in_shelter item=%s tod=%s",
                        raw_item,
                        getattr(snap, "time_of_day", ""),
                    )
                else:
                    acq_count = max(1, min(16, decision.count if decision.count > 0 else 1))
                    acq_res = await self._dispatch_goal_result(
                        {"action_type": "acquire", "item_name": raw_item, "count": acq_count}
                    )
                    last_res = acq_res
                    action_ok = self._receipt_ok(acq_res)
                    outcome_note = self._format_acquire_outcome(acq_res, raw_item)
            else:
                log.warning("event=mc_brain_acquire_rejected item=%r", raw_item)
        elif action == "smelt":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                smelt_count = max(1, min(16, decision.count if decision.count > 0 else 1))
                smelt_res = await self._dispatch_goal_result(
                    {"action_type": "smelt", "item_name": raw_item, "count": smelt_count}
                )
                last_res = smelt_res
                action_ok = self._receipt_ok(smelt_res)
                outcome_note = self._format_smelt_outcome(smelt_res, raw_item)
                if isinstance(smelt_res, dict) and smelt_res.get("furnace_pos") is not None:
                    if self.record_waypoint(
                        "furnace", smelt_res.get("furnace_pos"), dimension=cur_dim
                    ):
                        waypoints_dirty = True
            else:
                log.warning("event=mc_brain_smelt_rejected item=%r", raw_item)
        elif action == "hunt":
            raw_mob = decision.target_mob.strip().lower()
            if raw_mob.startswith("minecraft:"):
                raw_mob = raw_mob[10:]
            if not raw_mob or raw_mob in _HUNTABLE_MOBS:
                hunt_params: dict[str, Any] = {
                    "action_type": "hunt",
                    "count": max(1, min(3, decision.count if decision.count > 0 else 1)),
                }
                if raw_mob:
                    hunt_params["target_mob"] = raw_mob
                hunt_res = await self._dispatch_goal_result(hunt_params)
                last_res = hunt_res
                action_ok = self._receipt_ok(hunt_res)
                outcome_note = self._format_hunt_outcome(hunt_res, raw_mob)
            else:
                log.warning("event=mc_brain_hunt_rejected mob=%r", raw_mob)
        elif action == "sleep":
            sleep_res = await self._dispatch_goal_result({"action_type": "sleep"})
            last_res = sleep_res
            action_ok = self._receipt_ok(sleep_res)
            outcome_note = self._format_sleep_outcome(sleep_res)
            if action_ok and isinstance(sleep_res, dict) and sleep_res.get("bed_pos") is not None:
                if self.record_waypoint("home", sleep_res.get("bed_pos"), dimension=cur_dim):
                    waypoints_dirty = True
        elif action == "chest":
            chest_mode = (
                decision.mode if decision.mode in ("deposit", "withdraw", "inspect") else "deposit"
            )
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if not raw_item or _ITEM_NAME_RE.fullmatch(raw_item):
                chest_params: dict[str, Any] = {
                    "action_type": "chest",
                    "mode": chest_mode,
                    "count": max(1, min(64, decision.count if decision.count > 0 else 16)),
                }
                if raw_item:
                    chest_params["item_name"] = raw_item
                chest_res = await self._dispatch_goal_result(chest_params)
                last_res = chest_res
                action_ok = self._receipt_ok(chest_res)
                outcome_note = self._format_chest_outcome(chest_res, chest_mode, raw_item)
                if isinstance(chest_res, dict) and chest_res.get("chest_pos") is not None:
                    if self.record_waypoint("chest", chest_res.get("chest_pos"), dimension=cur_dim):
                        waypoints_dirty = True
            else:
                log.warning("event=mc_brain_chest_rejected item=%r", raw_item)
        elif action == "place":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                place_res = await self._dispatch_goal_result(
                    {"action_type": "place", "item_name": raw_item}
                )
                last_res = place_res
                action_ok = self._receipt_ok(place_res)
                outcome_note = self._format_place_outcome(place_res, raw_item)
                if (
                    action_ok
                    and isinstance(place_res, dict)
                    and place_res.get("placed_pos") is not None
                ):
                    wp_name = (
                        "home"
                        if raw_item.endswith("_bed")
                        else "chest"
                        if raw_item in ("chest", "barrel")
                        else "furnace"
                        if raw_item in ("furnace", "smoker", "blast_furnace")
                        else "workbench"
                        if raw_item == "crafting_table"
                        else ""
                    )
                    if wp_name and self.record_waypoint(
                        wp_name, place_res.get("placed_pos"), dimension=cur_dim
                    ):
                        waypoints_dirty = True
            else:
                log.warning("event=mc_brain_place_rejected item=%r", raw_item)
        elif action == "give":
            candidate = decision.target_player.strip() or speaking_player.strip()
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _PLAYER_NAME_RE.fullmatch(candidate) and _ITEM_NAME_RE.fullmatch(raw_item):
                give_count = max(1, min(64, decision.count if decision.count > 0 else 1))
                give_res = await self._dispatch_goal_result(
                    {
                        "action_type": "give",
                        "target_player": candidate,
                        "item_name": raw_item,
                        "count": give_count,
                    }
                )
                last_res = give_res
                action_ok = self._receipt_ok(give_res)
                outcome_note = self._format_give_outcome(give_res, candidate, raw_item)
            else:
                log.warning("event=mc_brain_give_rejected target=%r item=%r", candidate, raw_item)
        elif action == "shelter":
            shelter_mode = decision.mode if decision.mode in ("auto", "enter", "exit") else "auto"
            shelter_res = await self._dispatch_goal_result(
                {"action_type": "shelter", "mode": shelter_mode}
            )
            last_res = shelter_res
            action_ok = self._receipt_ok(shelter_res)
            outcome_note = self._format_shelter_outcome(shelter_res, shelter_mode)
        elif action == "guard":
            candidate = decision.target_player.strip() or speaking_player.strip()
            guard_params: dict[str, Any] = {"action_type": "guard"}
            if _PLAYER_NAME_RE.fullmatch(candidate):
                guard_params["target_player"] = candidate
            res = await self._dispatch_goal_result(guard_params)
            last_res = res
            action_ok = self._receipt_ok(res)
        elif action == "evade":
            prev_action = self.recent_turns[-1].get("action") if self.recent_turns else ""
            res = await self._dispatch_goal_result({"action_type": "evade"})
            last_res = res
            action_ok = self._receipt_ok(res)
            if isinstance(res, dict):
                already = bool(res.get("already_evading")) or (
                    getattr(snap, "combat_mode", "") == "evade"
                )
                fled = bool(res.get("fled_to"))
                if already and prev_action == "evade":
                    outcome_note = "evade:already_active_try_shelter_or_gather"
                elif fled:
                    outcome_note = "evade:fled"
                elif already:
                    outcome_note = "evade:already_active_no_threat"
                else:
                    outcome_note = "evade:active"
        elif action == "goto":
            cur_pos = self._extract_pos_tuple(getattr(snap, "pos", None))
            if (
                cur_pos is not None
                and abs(decision.x) <= 30_000_000
                and abs(decision.z) <= 30_000_000
                and -2048 <= decision.y <= 2048
            ):
                target = (float(decision.x), float(decision.y), float(decision.z))
                dist = math.dist(target, cur_pos)
                if dist <= self.config.max_goto_distance:
                    existing_relay = self._goto_relay
                    same_relay = (
                        isinstance(existing_relay, dict)
                        and existing_relay.get("dimension") == cur_dim
                        and isinstance(existing_relay.get("target"), tuple)
                        and math.dist(existing_relay["target"], target) <= 3.0
                    )
                    cur_horiz = math.hypot(target[0] - cur_pos[0], target[2] - cur_pos[2])
                    relay: dict[str, Any] = (
                        existing_relay
                        if same_relay and isinstance(existing_relay, dict)
                        else {
                            "target": target,
                            "dimension": cur_dim,
                            "segments": 0,
                            "stalls": 0,
                            "best_dist": dist,
                            "best_horiz": cur_horiz,
                            "auto_plan": "",
                        }
                    )
                    max_segments = max(1, int(self.config.max_goto_relay_segments))
                    max_stalls = max(1, int(self.config.max_goto_relay_stalls))
                    if same_relay and int(relay.get("segments") or 0) >= max_segments:
                        self._goto_relay = None
                        relay_terminated = True
                        action_ok = False
                        outcome_note = "goto:relay_limit_reached"
                    else:
                        waypoint, is_segmented = self._compute_goto_relay_waypoint(cur_pos, target)
                        goto_res = await self._dispatch_goal_result(
                            {
                                "action_type": "goto",
                                "x": float(waypoint[0]),
                                "y": float(waypoint[1]),
                                "z": float(waypoint[2]),
                            }
                        )
                        last_res = goto_res
                        action_ok = self._receipt_ok(goto_res)
                        res_reason = (
                            str(goto_res.get("reason") or "").strip()
                            if isinstance(goto_res, dict)
                            else ""
                        )
                        goto_threat_intr = (not action_ok) and _is_threat_interrupt_reason(
                            res_reason
                        )
                        target_coords = (
                            f"{int(round(target[0]))},{int(round(target[1]))},"
                            f"{int(round(target[2]))}"
                        )
                        way_coords = (
                            f"{int(round(waypoint[0]))},{int(round(waypoint[1]))},"
                            f"{int(round(waypoint[2]))}"
                        )
                        raw_moved = (
                            goto_res.get("moved_blocks") if isinstance(goto_res, dict) else None
                        )
                        moved_int = (
                            raw_moved
                            if isinstance(raw_moved, int)
                            and not isinstance(raw_moved, bool)
                            and raw_moved >= 0
                            else None
                        )
                        snap_pos_after: tuple[float, float, float] | None = None
                        if isinstance(goto_res, dict) and isinstance(
                            goto_res.get("snapshot"), dict
                        ):
                            snap_pos_after = self._extract_pos_tuple(
                                goto_res["snapshot"].get("pos")
                            )
                        if snap_pos_after is None:
                            live_pos = self._extract_pos_tuple(
                                getattr(self.plugin.snapshot, "pos", None)
                            )
                            if live_pos is not None and math.dist(live_pos, cur_pos) > 0.05:
                                snap_pos_after = live_pos

                        is_partial_receipt = bool(
                            action_ok and isinstance(goto_res, dict) and goto_res.get("partial")
                        )
                        prev_start_pos = relay.get("last_start_pos")
                        relay["last_start_pos"] = cur_pos
                        if goto_threat_intr:
                            if is_segmented or same_relay:
                                self._goto_relay = relay
                            outcome_note = f"goto:{res_reason[:32]}"
                        elif is_segmented or is_partial_receipt:
                            relay["segments"] = int(relay.get("segments") or 0) + 1
                            prev_best = float(relay.get("best_dist", dist))
                            prev_best_horiz = float(relay.get("best_horiz", cur_horiz))
                            rem_horiz = cur_horiz
                            if snap_pos_after is not None:
                                rem_dist = math.dist(snap_pos_after, target)
                                rem_horiz = math.hypot(
                                    target[0] - snap_pos_after[0],
                                    target[2] - snap_pos_after[2],
                                )
                                progressed = (rem_dist <= prev_best - 3.0) or (
                                    rem_horiz <= prev_best_horiz - 3.0
                                )
                            elif action_ok and moved_int is not None:
                                rem_dist = max(0.0, dist - float(moved_int))
                                rem_horiz = max(0.0, cur_horiz - float(moved_int))
                                progressed = moved_int >= 4
                            elif action_ok:
                                stuck_same_origin = (
                                    same_relay
                                    and isinstance(prev_start_pos, tuple)
                                    and math.dist(cur_pos, prev_start_pos) < 3.0
                                )
                                rem_dist = (
                                    dist if stuck_same_origin else math.dist(waypoint, target)
                                )
                                rem_horiz = (
                                    cur_horiz
                                    if stuck_same_origin
                                    else math.hypot(
                                        target[0] - waypoint[0], target[2] - waypoint[2]
                                    )
                                )
                                progressed = (not stuck_same_origin) and (
                                    (rem_dist <= prev_best - 3.0)
                                    or (rem_horiz <= prev_best_horiz - 3.0)
                                )
                            else:
                                rem_dist = dist
                                progressed = False

                            auto_plan = f"{_GOTO_RELAY_PLAN_PREFIX}{target_coords})"
                            partial_note = self._format_goto_partial_outcome(
                                is_segmented=is_segmented,
                                moved_int=moved_int,
                                target_coords=target_coords,
                                way_coords=way_coords,
                                rem_dist=rem_dist,
                            )
                            if action_ok and rem_dist <= 4.5:
                                self._goto_relay = None
                                outcome_note = f"goto:({target_coords})"
                            elif action_ok and progressed:
                                relay["best_dist"] = min(prev_best, rem_dist)
                                relay["best_horiz"] = min(prev_best_horiz, rem_horiz)
                                relay["stalls"] = 0
                                if relay["segments"] >= max_segments:
                                    self._goto_relay = None
                                    relay_terminated = True
                                    action_ok = False
                                    outcome_note = "goto:relay_limit_reached"
                                else:
                                    relay["auto_plan"] = auto_plan
                                    self._goto_relay = relay
                                    outcome_note = partial_note
                            else:
                                relay["stalls"] = int(relay.get("stalls") or 0) + 1
                                if relay["stalls"] >= max_stalls:
                                    self._goto_relay = None
                                    relay_terminated = True
                                    action_ok = False
                                    outcome_note = "goto:relay_stalled"
                                elif action_ok:
                                    relay["auto_plan"] = auto_plan
                                    self._goto_relay = relay
                                    outcome_note = partial_note
                                else:
                                    self._goto_relay = relay
                                    outcome_note = (
                                        f"goto:{res_reason[:32]}" if res_reason else "goto:failed"
                                    )
                        else:
                            if action_ok:
                                self._goto_relay = None
                                outcome_note = f"goto:({target_coords})"
                            elif same_relay:
                                relay["stalls"] = int(relay.get("stalls") or 0) + 1
                                if relay["stalls"] >= max_stalls:
                                    self._goto_relay = None
                                    relay_terminated = True
                                    outcome_note = "goto:relay_stalled"
                                else:
                                    self._goto_relay = relay
                                    outcome_note = (
                                        f"goto:{res_reason[:32]}" if res_reason else "goto:failed"
                                    )
                            elif res_reason:
                                outcome_note = f"goto:{res_reason[:32]}"
                            else:
                                outcome_note = "goto:failed"
                else:
                    self._goto_relay = None
                    outcome_note = f"goto:distance_exceeded({int(round(dist))}m)"
                    log.warning(
                        "event=mc_brain_goto_rejected reason=distance_exceeded dist=%.1f",
                        dist,
                    )
            else:
                outcome_note = "goto:invalid_pos_or_bounds"
                log.warning("event=mc_brain_goto_rejected reason=invalid_pos_or_bounds")
        elif action in ("explore", "look_signs", "observe", "idle"):
            res = await self._dispatch_goal_result({"action_type": action})
            last_res = res
            action_ok = self._receipt_ok(res)
            if action == "explore" and action_ok and isinstance(res, dict):
                raw_moved = res.get("moved_blocks")
                if isinstance(raw_moved, int) and not isinstance(raw_moved, bool):
                    outcome_note = f"explored:{raw_moved}m"
            elif not action_ok and isinstance(res, dict) and res.get("reason"):
                outcome_note = f"{action}:{str(res['reason'])[:32]}"

        res_reason_norm = (
            str(last_res.get("reason") or "").strip().lower() if isinstance(last_res, dict) else ""
        )
        is_threat_interrupt = (not action_ok) and _is_threat_interrupt_reason(res_reason_norm)
        is_shelter_blocked = shelter_guarded or (
            (not action_ok) and res_reason_norm in _SHELTER_BLOCKED_REASONS
        )
        is_still_in_water = (
            action == "explore" and (not action_ok) and res_reason_norm == "still_in_water"
        )

        if action not in ("goto", "none", "observe") and not is_threat_interrupt:
            self._goto_relay = None

        clean_next_goal = _sanitize_text(decision.next_goal, max_tokens=25)[:60]
        if clean_next_goal:
            self._active_plan = clean_next_goal
        elif (
            action == "goto"
            and action_ok
            and isinstance(self._goto_relay, dict)
            and self._goto_relay.get("auto_plan")
        ):
            self._active_plan = str(self._goto_relay["auto_plan"])[:60]
        elif (
            relay_terminated or (action == "goto" and action_ok and self._goto_relay is None)
        ) and self._active_plan.startswith(_GOTO_RELAY_PLAN_PREFIX):
            self._active_plan = ""
        elif action in ("idle", "none"):
            self._active_plan = ""

        if action_ok:
            self._consecutive_failures = 0
            self._threat_interrupt_pending = False
            self._last_threat_mob = ""
        elif is_threat_interrupt:
            # Survival interruption: re-evaluate threat/evade promptly without failure backoff.
            self._threat_interrupt_pending = True
            raw_tm = (
                str(last_res.get("threat_mob") or "").strip().lower()
                if isinstance(last_res, dict)
                else ""
            )
            if raw_tm.startswith("minecraft:"):
                raw_tm = raw_tm[10:]
            self._last_threat_mob = raw_tm if _ITEM_NAME_RE.fullmatch(raw_tm) else ""
            if self._last_threat_mob and outcome_note and self._last_threat_mob not in outcome_note:
                outcome_note = f"{outcome_note}({self._last_threat_mob})"[:80]
            latest_snap = self.plugin.snapshot
            if getattr(latest_snap, "combat_mode", "evade") == "guard" and (
                self._has_creeper_or_critical_hp(latest_snap)
            ):
                await self._dispatch_goal_safe({"action_type": "evade"})
        elif is_shelter_blocked:
            self._threat_interrupt_pending = False
            self._last_threat_mob = ""
        elif action not in ("none", "idle", "stay", "observe"):
            self._consecutive_failures += 1
            self._threat_interrupt_pending = False
            self._last_threat_mob = ""

        self._evade_backoff_active = False if is_threat_interrupt else (action == "evade")
        self._chain_next_step = bool(
            action_ok and action in _CHAINABLE_ACTIONS and bool(self._active_plan)
        )
        if is_threat_interrupt:
            self._recovery_retry_pending = True
        elif is_shelter_blocked or relay_terminated:
            self._recovery_retry_pending = False
        elif is_still_in_water:
            self._recovery_retry_pending = self._consecutive_failures <= 3
        else:
            self._recovery_retry_pending = bool(
                (not action_ok)
                and action in (_CHAINABLE_ACTIONS | {"explore", "goto", "shelter"})
                and self._consecutive_failures <= 2
            )
        if waypoints_dirty:
            await self._save_waypoints_safe()

        clean_reason = _sanitize_text(decision.reason, max_tokens=30)[:60]
        self._last_reason = clean_reason or action
        self._append_recent_turn(
            trigger=trigger,
            input_summary=input_summary,
            say=clean_say,
            action=action,
            reason=clean_reason,
            outcome=outcome_note,
        )
        self._record_last_turn_snapshot(self.plugin.snapshot, action)
        self._record_turn_recap(
            chat_batch=chat_batch,
            event_batch=event_batch,
            clean_say=clean_say,
        )
        await self._push_status_safe()

    async def _run_local_fallback(
        self,
        chat_batch: list[tuple[str, str]],
        *,
        snap: Any,
        speaking_player: str,
        reason: str,
    ) -> None:
        combined = " ".join(msg for _, msg in chat_batch)
        clean_say = ""
        if any(kw in combined for kw in _LOCAL_STOP_KEYWORDS):
            await self._dispatch_goal_safe({"action_type": "stop"})
            clean_say = "好哒，我乖乖站在这里等你喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self._last_reason = f"fallback_stop:{reason}"[:60]
        elif any(kw in combined for kw in _LOCAL_EVADE_KEYWORDS):
            await self._dispatch_goal_safe({"action_type": "evade"})
            self._evade_backoff_active = True
            self._chain_next_step = False
            self._recovery_retry_pending = False
            clean_say = "好哒，我先切到躲避模式注意安全喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self._last_reason = f"fallback_evade:{reason}"[:60]
        elif any(kw in combined for kw in _LOCAL_GUARD_KEYWORDS):
            target = speaking_player.strip()
            if not _PLAYER_NAME_RE.fullmatch(target):
                nearby = getattr(snap, "nearby_players", ()) or ()
                target = next((p for p in nearby if _PLAYER_NAME_RE.fullmatch(str(p))), "")
            params: dict[str, Any] = {"action_type": "guard"}
            if _PLAYER_NAME_RE.fullmatch(target):
                params["target_player"] = target
            await self._dispatch_goal_safe(params)
            clean_say = "收到，猫猫切到护卫模式护着你喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self._last_reason = f"fallback_guard:{reason}"[:60]
        elif any(kw in combined for kw in _LOCAL_FOLLOW_KEYWORDS):
            target = speaking_player.strip()
            if not _PLAYER_NAME_RE.fullmatch(target):
                nearby = getattr(snap, "nearby_players", ()) or ()
                target = next((p for p in nearby if _PLAYER_NAME_RE.fullmatch(str(p))), "")
            clean_say = "来啦来啦！这就跟上你喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            if _PLAYER_NAME_RE.fullmatch(target):
                await self._dispatch_goal_safe({"action_type": "follow", "target_player": target})
            self._last_reason = f"fallback_follow:{reason}"[:60]
        elif any(kw in combined for kw in _LOCAL_GREETING_KEYWORDS):
            clean_say = "在呢！猫猫看到你啦喵~"
            await self._dispatch_goal_safe({"action_type": "chat", "message": clean_say})
            self._last_reason = f"fallback_chat:{reason}"[:60]
        else:
            self._last_reason = f"fallback_noop:{reason}"[:60]
        self._record_turn_recap(chat_batch=chat_batch, clean_say=clean_say)
        await self._push_status_safe()

    async def _push_status_safe(self) -> None:
        if not self.plugin.client.connected:
            return
        try:
            await self.plugin.client.push_status(self.plugin.status_dict())
        except Exception:
            pass
