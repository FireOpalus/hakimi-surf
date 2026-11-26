"""Action dispatch, outcome formatting, decision execution, and fallback for `McPlayBrain`."""

from __future__ import annotations

import logging
import math
from typing import Any

from hakimi_plugin_sdk import PluginRpcError

from .play_brain_models import (
    _BLOCK_TYPE_RE,
    _CHAINABLE_ACTIONS,
    _HUNTABLE_MOBS,
    _ITEM_NAME_RE,
    _LOCAL_EVADE_KEYWORDS,
    _LOCAL_FOLLOW_KEYWORDS,
    _LOCAL_GREETING_KEYWORDS,
    _LOCAL_GUARD_KEYWORDS,
    _LOCAL_STOP_KEYWORDS,
    _PLAYER_NAME_RE,
    McBrainDecision,
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
        cur_dim = str(getattr(snap, "dimension", "") or "overworld")

        if action == "stay":
            await self._dispatch_goal_safe({"action_type": "stop"})
        elif action == "follow":
            candidate = decision.target_player.strip() or speaking_player.strip()
            if _PLAYER_NAME_RE.fullmatch(candidate):
                res = await self._dispatch_goal_result(
                    {"action_type": "follow", "target_player": candidate}
                )
                action_ok = self._receipt_ok(res)
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
                action_ok = self._receipt_ok(gather_res)
                outcome_note = self._format_gather_outcome(gather_res, action, block)
            else:
                log.warning("event=mc_brain_block_rejected block=%r", block)
        elif action == "craft":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                craft_count = max(1, min(16, decision.count if decision.count > 0 else 1))
                craft_res = await self._dispatch_goal_result(
                    {"action_type": "craft", "item_name": raw_item, "count": craft_count}
                )
                action_ok = self._receipt_ok(craft_res)
                outcome_note = self._format_craft_outcome(craft_res, raw_item)
            else:
                log.warning("event=mc_brain_craft_rejected item=%r", raw_item)
        elif action == "acquire":
            raw_item = decision.item_name.strip()
            if raw_item.startswith("minecraft:"):
                raw_item = raw_item[10:]
            if _ITEM_NAME_RE.fullmatch(raw_item):
                acq_count = max(1, min(16, decision.count if decision.count > 0 else 1))
                acq_res = await self._dispatch_goal_result(
                    {"action_type": "acquire", "item_name": raw_item, "count": acq_count}
                )
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
                action_ok = self._receipt_ok(hunt_res)
                outcome_note = self._format_hunt_outcome(hunt_res, raw_mob)
            else:
                log.warning("event=mc_brain_hunt_rejected mob=%r", raw_mob)
        elif action == "sleep":
            sleep_res = await self._dispatch_goal_result({"action_type": "sleep"})
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
                action_ok = self._receipt_ok(give_res)
                outcome_note = self._format_give_outcome(give_res, candidate, raw_item)
            else:
                log.warning("event=mc_brain_give_rejected target=%r item=%r", candidate, raw_item)
        elif action == "shelter":
            shelter_mode = decision.mode if decision.mode in ("auto", "enter", "exit") else "auto"
            shelter_res = await self._dispatch_goal_result(
                {"action_type": "shelter", "mode": shelter_mode}
            )
            action_ok = self._receipt_ok(shelter_res)
            outcome_note = self._format_shelter_outcome(shelter_res, shelter_mode)
        elif action == "guard":
            candidate = decision.target_player.strip() or speaking_player.strip()
            guard_params: dict[str, Any] = {"action_type": "guard"}
            if _PLAYER_NAME_RE.fullmatch(candidate):
                guard_params["target_player"] = candidate
            res = await self._dispatch_goal_result(guard_params)
            action_ok = self._receipt_ok(res)
        elif action == "evade":
            prev_action = self.recent_turns[-1].get("action") if self.recent_turns else ""
            res = await self._dispatch_goal_result({"action_type": "evade"})
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
            cur_pos = getattr(snap, "pos", None)
            if (
                isinstance(cur_pos, tuple)
                and len(cur_pos) == 3
                and all(isinstance(v, (int, float)) and math.isfinite(float(v)) for v in cur_pos)
                and abs(decision.x) <= 30_000_000
                and abs(decision.z) <= 30_000_000
                and -2048 <= decision.y <= 2048
            ):
                dist = math.dist(
                    (float(decision.x), float(decision.y), float(decision.z)),
                    (float(cur_pos[0]), float(cur_pos[1]), float(cur_pos[2])),
                )
                if dist <= self.config.max_goto_distance:
                    goto_res = await self._dispatch_goal_result(
                        {
                            "action_type": "goto",
                            "x": float(decision.x),
                            "y": float(decision.y),
                            "z": float(decision.z),
                        }
                    )
                    action_ok = self._receipt_ok(goto_res)
                    coords = f"{int(decision.x)},{int(decision.y)},{int(decision.z)}"
                    if action_ok:
                        if isinstance(goto_res, dict) and goto_res.get("partial"):
                            raw_moved = goto_res.get("moved_blocks")
                            if isinstance(raw_moved, int) and not isinstance(raw_moved, bool):
                                outcome_note = f"goto:partial({raw_moved}m)"
                            else:
                                outcome_note = f"goto:partial({coords})"
                        else:
                            outcome_note = f"goto:({coords})"
                    elif isinstance(goto_res, dict) and goto_res.get("reason"):
                        outcome_note = f"goto:{str(goto_res['reason'])[:32]}"
                    else:
                        outcome_note = "goto:failed"
                else:
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
            action_ok = self._receipt_ok(res)
            if action == "explore" and action_ok and isinstance(res, dict):
                raw_moved = res.get("moved_blocks")
                if isinstance(raw_moved, int) and not isinstance(raw_moved, bool):
                    outcome_note = f"explored:{raw_moved}m"
            elif not action_ok and isinstance(res, dict) and res.get("reason"):
                outcome_note = f"{action}:{str(res['reason'])[:32]}"

        clean_next_goal = _sanitize_text(decision.next_goal, max_tokens=25)[:60]
        if clean_next_goal:
            self._active_plan = clean_next_goal
        elif action in ("idle", "none"):
            self._active_plan = ""
        if action_ok:
            self._consecutive_failures = 0
        elif action not in ("none", "idle", "stay", "observe"):
            self._consecutive_failures += 1
        self._evade_backoff_active = action == "evade"
        self._chain_next_step = bool(
            action_ok and action in _CHAINABLE_ACTIONS and bool(self._active_plan)
        )
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
