"""Real-time Layer 2 Minecraft play brain with mandatory kernel security audit."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import re
import time
from collections import deque
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from hakimi_plugin_sdk import PluginRpcError

from .play_brain_actions import _PlayBrainActionsMixin
from .play_brain_cruise import _PlayBrainCruiseMixin
from .play_brain_models import (
    _ACTION_LOG_LIMIT,
    _COLLAPSE_WS_RE,
    _HANDLED_MILESTONE_EVENTS,
    _MAX_WAYPOINTS,
    _PINNED_WAYPOINTS,
    _PLAYER_NAME_RE,
    _WAYPOINT_NAME_RE,
    _WAYPOINTS_STATE_KEY,
    MC_PLAY_ALLOWED_ACTIONS,
    MC_PLAY_RESPONSE_SCHEMA,
    MC_PLAY_TASK_INSTRUCTION,
    McBrainConfig,
    McBrainDecision,
    _clean_text,
    _sanitize_text,
    _scrub_patterns,
    _strip_json_fences,
)

if TYPE_CHECKING:
    from .plugin_main import MinecraftPlugin

__all__ = [
    "MC_PLAY_ALLOWED_ACTIONS",
    "MC_PLAY_RESPONSE_SCHEMA",
    "MC_PLAY_TASK_INSTRUCTION",
    "McBrainConfig",
    "McBrainDecision",
    "McPlayBrain",
    "_sanitize_text",
]

log = logging.getLogger("hakimi_plugin_minecraft")
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class McPlayBrain(_PlayBrainCruiseMixin, _PlayBrainActionsMixin):
    """Real-time asynchronous play loop for `MinecraftPlugin` (`mc.play` scene)."""

    def __init__(
        self,
        plugin: MinecraftPlugin,
        config: McBrainConfig | None = None,
    ) -> None:
        self.plugin = plugin
        self.config = config or McBrainConfig()

        self.screen_blocked_count: int = 0
        self.cruise_skipped_count: int = 0
        self.layer1_routed_count: int = 0
        self.recent_turns: list[dict[str, Any]] = []
        self.action_log: deque[dict[str, Any]] = deque(maxlen=_ACTION_LOG_LIMIT)
        self._waypoints: dict[str, dict[str, Any]] = {}
        self._active_plan: str = ""
        self._chain_next_step: bool = False
        self._recovery_retry_pending: bool = False
        self._consecutive_failures: int = 0
        self._first_cruise_pending: bool = True
        self._last_turn_snapshot: dict[str, Any] | None = None
        self._last_reason: str = ""
        self._call_times: deque[float] = deque()

        self._task: asyncio.Task[None] | None = None
        self._waypoint_save_tasks: set[asyncio.Task[None]] = set()
        self._wake_event = asyncio.Event()
        self._wake_seq: int = 0
        self._stop_requested: bool = False
        self._decision_lock = asyncio.Lock()

        self._prev_ready: bool = False
        self._prev_connected: bool = False

        self._pending_chats: list[tuple[str, str]] = []
        self._pending_events: list[dict[str, Any]] = []
        self._chat_first_at: float | None = None
        self._chat_ready_at: float | None = None

        self._last_chat_handled_at: float = -1e9
        self._last_event_handled_at: float = -1e9
        self._last_decision_at: float = 0.0
        self._last_creeper_alert_chat_at: float = -1e9
        self._last_low_food_chat_at: float = -1e9
        self._last_combat_retreat_chat_at: float = -1e9
        self._last_evade_flee_at: float = -1e9
        self._last_death_chat_at: float = -1e9
        self._last_death_escalate_at: float = -1e9
        self._recent_death_times: deque[float] = deque(maxlen=8)
        self._consecutive_cruise_skips: int = 0
        self._evade_backoff_active: bool = False

        self._last_mono: float = time.monotonic()
        self._last_clk: float = float(plugin.clock())
        self._virtual_now: float = 0.0

        self._session_started_at: float | None = None
        self._session_players: list[str] = []
        self._session_chat_highlights: list[str] = []
        self._session_milestones: list[str] = []
        self._session_recap_flushed: bool = False

    def _now(self) -> float:
        cur_mono = time.monotonic()
        cur_clk = float(self.plugin.clock())
        dm = max(0.0, cur_mono - self._last_mono)
        dc = max(0.0, cur_clk - self._last_clk)
        self._last_mono = cur_mono
        self._last_clk = cur_clk
        self._virtual_now += max(dm, dc)
        return self._virtual_now

    def _wake(self) -> None:
        self._wake_seq += 1
        self._wake_event.set()

    def _prune_calls(self, now: float) -> None:
        cutoff = now - 600.0
        while self._call_times and self._call_times[0] <= cutoff:
            self._call_times.popleft()

    def is_ready(self) -> bool:
        """Return True when brain execution is permitted and the MC bot is connected."""
        p = self.plugin
        return bool(
            p.enable_brain
            and p.enabled
            and not p.paused
            and p.snapshot.connected
            and p.bridge_ready
        )

    def telemetry(self) -> dict[str, Any]:
        """Return bounded telemetry fields exposed in `MinecraftPlugin.status_dict()`."""
        now = self._now()
        self._prune_calls(now)
        active = bool(self._task is not None and not self._task.done() and self.is_ready())
        return {
            "brain_enabled": bool(self.plugin.enable_brain),
            "brain_active": active,
            "session_intent": _sanitize_text(self.plugin.session_intent, max_tokens=40),
            "active_plan": self._active_plan,
            "waypoints": self._telemetry_waypoints(),
            "action_log": list(self.action_log)[-_ACTION_LOG_LIMIT:],
            "brain_calls_recent": len(self._call_times),
            "brain_last_reason": _sanitize_text(self._last_reason, max_tokens=30),
            "brain_screen_blocked_count": int(self.screen_blocked_count),
            "brain_cruise_skipped_count": int(self.cruise_skipped_count),
            "brain_layer1_routed_count": int(self.layer1_routed_count),
        }

    @staticmethod
    def _coerce_waypoint_pos(raw: Any) -> list[int] | None:
        if isinstance(raw, (list, tuple)) and len(raw) == 3:
            coords: list[int] = []
            for v in raw:
                if isinstance(v, bool) or not isinstance(v, (int, float)):
                    return None
                fv = float(v)
                if not math.isfinite(fv) or abs(fv) > 30_000_000:
                    return None
                coords.append(int(round(fv)))
            return coords
        return None

    def record_waypoint(
        self,
        name: str,
        pos: Any,
        *,
        dimension: str = "",
    ) -> bool:
        """Record or update a named spatial landmark in bounded memory."""
        clean_name = re.sub(r"[^a-z0-9_]", "_", str(name or "").strip().lower()).strip("_")[:32]
        if not clean_name or not _WAYPOINT_NAME_RE.fullmatch(clean_name):
            return False
        coords = self._coerce_waypoint_pos(pos)
        if coords is None:
            return False
        dim = (
            _sanitize_text(
                str(dimension or getattr(self.plugin.snapshot, "dimension", "") or "overworld"),
                max_tokens=12,
            )
            or "overworld"
        )
        self._waypoints[clean_name] = {
            "name": clean_name,
            "dimension": dim,
            "pos": coords,
            "updated_at": round(float(self.plugin.clock()), 1),
        }
        if len(self._waypoints) > _MAX_WAYPOINTS:
            removable = [
                (k, float(v.get("updated_at") or 0.0))
                for k, v in self._waypoints.items()
                if k not in _PINNED_WAYPOINTS
            ]
            if not removable:
                removable = [
                    (k, float(v.get("updated_at") or 0.0)) for k, v in self._waypoints.items()
                ]
            removable.sort(key=lambda pair: pair[1])
            while len(self._waypoints) > _MAX_WAYPOINTS and removable:
                evict_key, _ = removable.pop(0)
                if evict_key != clean_name:
                    self._waypoints.pop(evict_key, None)
        return True

    async def load_waypoints(self) -> None:
        """Best-effort load persisted waypoints from plugin KV store on connect."""
        state_get = getattr(self.plugin.client, "state_get", None)
        if not callable(state_get) or not getattr(self.plugin.client, "connected", False):
            return
        try:
            raw = await state_get(_WAYPOINTS_STATE_KEY)
        except Exception:
            return
        if not isinstance(raw, list):
            return
        for entry in raw[:_MAX_WAYPOINTS]:
            if not isinstance(entry, dict):
                continue
            self.record_waypoint(
                str(entry.get("name") or ""),
                entry.get("pos"),
                dimension=str(entry.get("dimension") or ""),
            )

    async def _save_waypoints_safe(self) -> None:
        state_set = getattr(self.plugin.client, "state_set", None)
        if not callable(state_set) or not getattr(self.plugin.client, "connected", False):
            return
        try:
            await state_set(_WAYPOINTS_STATE_KEY, list(self._waypoints.values())[:_MAX_WAYPOINTS])
        except Exception:
            pass

    def _schedule_waypoints_save(self) -> None:
        if not getattr(self.plugin.client, "connected", False):
            return
        if not callable(getattr(self.plugin.client, "state_set", None)):
            return
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._save_waypoints_safe(), name="mc-save-waypoints")
        except RuntimeError:
            return
        self._waypoint_save_tasks.add(task)
        task.add_done_callback(self._waypoint_save_tasks.discard)

    def _telemetry_waypoints(self) -> list[dict[str, Any]]:
        ordered = sorted(
            self._waypoints.values(),
            key=lambda w: (
                0 if w["name"] in _PINNED_WAYPOINTS else 1,
                -float(w.get("updated_at") or 0.0),
            ),
        )
        return [
            {
                "name": str(w["name"]),
                "dimension": str(w.get("dimension") or "overworld"),
                "pos": list(w["pos"]),
                "updated_at": float(w.get("updated_at") or 0.0),
            }
            for w in ordered[:12]
        ]

    def _nearby_waypoints_for_state(self, snap: Any) -> list[dict[str, Any]]:
        if not self._waypoints:
            return []
        cur_dim = str(getattr(snap, "dimension", "") or "").strip()
        cur_pos = getattr(snap, "pos", None)
        has_pos = (
            isinstance(cur_pos, tuple)
            and len(cur_pos) == 3
            and all(isinstance(v, (int, float)) and math.isfinite(float(v)) for v in cur_pos)
        )
        ranked: list[tuple[int, float, dict[str, Any]]] = []
        for wp in self._waypoints.values():
            wdim = str(wp.get("dimension") or "overworld")
            if cur_dim and cur_dim != "unknown" and wdim != cur_dim:
                continue
            wpos = wp["pos"]
            item: dict[str, Any] = {"name": wp["name"], "pos": list(wpos)}
            dist = 999999.0
            if has_pos:
                dist = math.dist(
                    (float(cur_pos[0]), float(cur_pos[1]), float(cur_pos[2])),
                    (float(wpos[0]), float(wpos[1]), float(wpos[2])),
                )
                item["dist"] = round(dist, 1)
            priority = 0 if wp["name"] in _PINNED_WAYPOINTS else 1
            ranked.append((priority, dist, item))
        ranked.sort(key=lambda entry: (entry[0], entry[1]))
        return [entry[2] for entry in ranked[:8]]

    def start(self) -> None:
        """Start the background play loop idempotently when `enable_brain` is enabled."""
        if not self.plugin.enable_brain:
            return
        if self._task is not None and not self._task.done():
            return
        self._stop_requested = False
        now_ready = self.is_ready()
        self._prev_ready = now_ready
        self._prev_connected = bool(self.plugin.snapshot.connected)
        if now_ready:
            self._last_decision_at = self._now()
        self._wake()
        self._task = asyncio.create_task(self._run_loop(), name="mc-play-brain")

    async def stop(self) -> None:
        """Stop and cancel the background play loop cleanly and flush pending waypoint saves."""
        self._stop_requested = True
        self._wake()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        pending_saves = list(self._waypoint_save_tasks)
        if pending_saves:
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(3.0):
                    await asyncio.gather(*pending_saves, return_exceptions=True)
            for save_task in pending_saves:
                if not save_task.done():
                    save_task.cancel()
            still_pending = [t for t in pending_saves if not t.done()]
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)
            self._waypoint_save_tasks.clear()

    def on_disconnect(self) -> None:
        """Clear session-scoped turns and pending triggers on disconnect."""
        self.recent_turns.clear()
        self._pending_chats.clear()
        self._pending_events.clear()
        self._active_plan = ""
        self._chain_next_step = False
        self._recovery_retry_pending = False
        self._consecutive_failures = 0
        self._consecutive_cruise_skips = 0
        self._evade_backoff_active = False
        self._recent_death_times.clear()
        self._first_cruise_pending = True
        self._last_turn_snapshot = None
        self._chat_first_at = None
        self._chat_ready_at = None
        self._session_started_at = None
        self._session_players.clear()
        self._session_chat_highlights.clear()
        self._session_milestones.clear()
        self._session_recap_flushed = False
        self._prev_connected = False
        self._prev_ready = False
        self._wake()

    def note_session_connected(self, now: float | None = None) -> None:
        """Record active Minecraft session start timestamp if not already set."""
        if self._session_started_at is None:
            self._session_started_at = self._now() if now is None else float(now)
            self._first_cruise_pending = True
        self._session_recap_flushed = False

    def note_players(self, players: Sequence[str]) -> None:
        """Record sanitized, deduplicated player names seen during this session."""
        if not players:
            return
        for raw in players:
            if len(self._session_players) >= 8:
                break
            raw_clean = _clean_text(str(raw or ""))
            if not raw_clean:
                continue
            scrubbed = _scrub_patterns(raw_clean)
            if _PLAYER_NAME_RE.fullmatch(scrubbed):
                clean = scrubbed
            else:
                clean = _sanitize_text(scrubbed, max_tokens=8)[:16].strip()
            if clean and clean not in self._session_players:
                self._session_players.append(clean)

    def _append_chat_highlight(self, entry: str) -> None:
        if not entry:
            return
        self._session_chat_highlights.append(entry)
        if len(self._session_chat_highlights) > 8:
            self._session_chat_highlights = self._session_chat_highlights[-8:]

    def _append_milestone(self, text: str) -> None:
        if not text:
            return
        if self._session_milestones and self._session_milestones[-1] == text:
            return
        self._session_milestones.append(text)
        if len(self._session_milestones) > 6:
            self._session_milestones = self._session_milestones[-6:]

    def record_direct_chat(self, text: str) -> None:
        """Record an outbound direct MC chat line into session recap highlights."""
        if not (self.plugin.snapshot.connected or self._session_started_at is not None):
            return
        clean = _sanitize_text(text, max_tokens=40)
        if not clean:
            return
        self._append_chat_highlight(f"我:{clean[:44]}")

    def _record_turn_recap(
        self,
        *,
        chat_batch: list[tuple[str, str]] | None = None,
        event_batch: list[dict[str, Any]] | None = None,
        clean_say: str = "",
    ) -> None:
        say_safe = _sanitize_text(clean_say, max_tokens=40) if clean_say else ""
        chats = list(chat_batch or ())
        if chats:
            last_idx = len(chats) - 1
            for idx, (player, msg) in enumerate(chats):
                speaker = _scrub_patterns(_clean_text(player))[:16] or "玩家"
                msg_clean = _scrub_patterns(_clean_text(msg))
                if not msg_clean:
                    continue
                if idx == last_idx and say_safe:
                    self._append_chat_highlight(f"{speaker}:{msg_clean[:34]}->{say_safe[:40]}")
                else:
                    self._append_chat_highlight(f"{speaker}:{msg_clean[:40]}")
        elif say_safe:
            self._append_chat_highlight(f"我:{say_safe[:44]}")

        for ev in event_batch or ():
            etype = str(ev.get("event_type") or "")
            if etype == "mc_rare_find":
                block = _scrub_patterns(_clean_text(str(ev.get("block") or "")))[:32]
                note = f"发现{block}" if block else "发现稀有方块"
            elif etype == "mc_death":
                note = "意外阵亡"
            elif etype == "mc_health_alert":
                note = "血量告急"
            elif etype == "mc_low_food":
                note = "食物告急"
            elif etype == "mc_creeper_alert":
                note = "遇到苦力怕预警"
            elif etype == "mc_combat_retreat":
                note = "残血后撤回血"
            else:
                note = ""
            if note:
                self._append_milestone(note)

    def build_session_recap(
        self, *, now: float | None = None, reason: str = "disconnect"
    ) -> dict[str, Any] | None:
        """Build a bounded, sanitized MC session recap payload or return None if trivial."""
        if self._session_recap_flushed:
            return None
        now_ts = self._now() if now is None else float(now)
        started_at = now_ts if self._session_started_at is None else float(self._session_started_at)
        elapsed = max(0.0, now_ts - started_at)
        if (
            not self._session_chat_highlights
            and not self._session_milestones
            and (not self._session_players or elapsed < 60.0)
        ):
            return None

        mins = max(1, round(elapsed / 60.0))

        def _format_summary(players: Sequence[str], chats: Sequence[str]) -> str:
            if players:
                header = f"MC游玩小记（{mins}分钟，同服: {'、'.join(players)}）"
            else:
                header = f"MC游玩小记（{mins}分钟）"
            body_parts: list[str] = []
            if chats:
                body_parts.append(f"聊了「{' / '.join(chats)}」")
            if self._session_milestones:
                body_parts.append(f"见闻: {'、'.join(self._session_milestones)}")
            raw = f"{header}：{'；'.join(body_parts)}" if body_parts else header
            cleaned = _COLLAPSE_WS_RE.sub(" ", _scrub_patterns(_clean_text(raw))).strip()
            return cleaned

        summary = _format_summary(self._session_players, self._session_chat_highlights)
        if len(summary) > 240:
            summary = _format_summary(self._session_players[:4], self._session_chat_highlights[-4:])
        if len(summary) > 240:
            summary = summary[:239].rstrip() + "…"

        clock_ts = float(self.plugin.clock())
        date_ts = started_at if started_at >= 1_000_000_000 else clock_ts
        if not math.isfinite(date_ts) or date_ts < 0:
            date_ts = time.time()
        local_date = datetime.fromtimestamp(date_ts, tz=_SHANGHAI_TZ).strftime("%Y-%m-%d")
        started_int = int(started_at)
        business_key = f"mc-recap:{started_int}"
        topic = f"MC游玩:{local_date}:{started_int}"[:80]
        tags = list(
            dict.fromkeys(t for t in ("minecraft", "MC游玩", *self._session_players[:2]) if t)
        )
        clean_reason = _sanitize_text(reason, max_tokens=16)[:32] or "disconnect"
        payload = {
            "duration_minutes": mins,
            "players": list(self._session_players),
            "chat_count": len(self._session_chat_highlights),
            "milestone_count": len(self._session_milestones),
            "reason": clean_reason,
        }
        return {
            "summary": summary,
            "business_key": business_key,
            "topic": topic,
            "tags": tags,
            "payload": payload,
        }

    async def flush_session_recap(self, *, reason: str = "disconnect") -> bool:
        """Emit session recap world event once and clear session recap buffers."""
        recap = self.build_session_recap(reason=reason)
        self._session_recap_flushed = True
        self._session_started_at = None
        self._session_players.clear()
        self._session_chat_highlights.clear()
        self._session_milestones.clear()
        if recap is None or not self.plugin.client.connected:
            return False
        try:
            await self.plugin.client.emit_world_event(
                "mc_session_recap",
                recap["payload"],
                importance="normal",
                business_key=recap["business_key"],
                summary=recap["summary"],
                visibility="public",
                ttl_seconds=48 * 3600.0,
                owner_recap=True,
                chronicle_topic=recap["topic"],
                chronicle_tags=recap["tags"],
            )
            return True
        except Exception as exc:
            log.warning("event=mc_session_recap_emit_failed reason=%s err=%s", reason, exc)
            return False

    def on_state_change(self) -> None:
        """Notify the loop of enabled/paused/bridge state transitions."""
        now_ready = self.is_ready()
        if not self._prev_ready and now_ready:
            self._last_decision_at = self._now()
        self._prev_ready = now_ready
        self._wake()

    def on_snapshot(self, snap: Any) -> None:
        """Handle snapshot updates: clear turns on disconnect and wake on connect."""
        connected = bool(getattr(snap, "connected", False))
        if (self._prev_connected and not connected) or (not connected and self.recent_turns):
            self.on_disconnect()
            return
        self._prev_connected = connected
        if getattr(snap, "combat_mode", "evade") == "guard":
            self._evade_backoff_active = False
        now_ready = self.is_ready()
        if not self._prev_ready and now_ready:
            self._last_decision_at = self._now()
        self._prev_ready = now_ready
        self._wake()

    def on_milestone(self, event_type: str, event_data: dict[str, Any] | None = None) -> None:
        """Queue sanitized player chat or world milestone wakes."""
        if not self.plugin.enable_brain:
            return
        etype = _sanitize_text(str(event_type or ""), max_tokens=16)
        data = dict(event_data) if isinstance(event_data, dict) else {}

        if etype == "mc_player_chat":
            raw_player = str(data.get("player") or data.get("username") or "").strip()
            clean_player = _clean_text(raw_player)
            if not _PLAYER_NAME_RE.fullmatch(clean_player):
                clean_player = _sanitize_text(clean_player, max_tokens=10)[:16]
            raw_msg = str(data.get("message") or data.get("text") or "").strip()
            clean_msg = _sanitize_text(raw_msg, max_tokens=60)
            if not clean_msg:
                return
            now = self._now()
            self._pending_chats.append((clean_player, clean_msg))
            if len(self._pending_chats) > 6:
                self._pending_chats = self._pending_chats[-6:]
            if self._chat_first_at is None:
                self._chat_first_at = now
            max_coalesce = self._chat_first_at + max(self.config.chat_debounce_seconds * 2.5, 0.05)
            self._chat_ready_at = min(
                max_coalesce, now + max(0.0, self.config.chat_debounce_seconds)
            )
            self._wake()
            return

        if etype in _HANDLED_MILESTONE_EVENTS:
            if etype == "mc_death" and data.get("pos") is not None:
                if self.record_waypoint(
                    "last_death",
                    data.get("pos"),
                    dimension=str(data.get("dimension") or ""),
                ):
                    self._schedule_waypoints_save()
            elif etype == "mc_rare_find" and data.get("pos") is not None and data.get("block"):
                clean_blk = re.sub(r"[^a-z0-9_]", "_", str(data.get("block") or "").lower())[:24]
                if clean_blk and self.record_waypoint(
                    f"rare_{clean_blk}",
                    data.get("pos"),
                    dimension=str(data.get("dimension") or ""),
                ):
                    self._schedule_waypoints_save()
            sanitized: dict[str, Any] = {"event_type": etype}
            if data.get("player"):
                sanitized["player"] = _sanitize_text(str(data["player"]), max_tokens=10)[:16]
            if data.get("block"):
                sanitized["block"] = _sanitize_text(str(data["block"]), max_tokens=16)[:48]
            if data.get("dimension"):
                sanitized["dimension"] = _sanitize_text(str(data["dimension"]), max_tokens=12)
            if data.get("activity"):
                sanitized["activity"] = _sanitize_text(str(data["activity"]), max_tokens=16)
            raw_hp = data.get("health")
            if isinstance(raw_hp, (int, float)) and not isinstance(raw_hp, bool):
                fhp = float(raw_hp)
                if math.isfinite(fhp):
                    sanitized["health"] = fhp
            raw_food = data.get("food")
            if isinstance(raw_food, (int, float)) and not isinstance(raw_food, bool):
                ffood = float(raw_food)
                if math.isfinite(ffood):
                    sanitized["food"] = ffood
            raw_safe_food = data.get("safe_food_count")
            if (
                isinstance(raw_safe_food, int)
                and not isinstance(raw_safe_food, bool)
                and raw_safe_food >= 0
            ):
                sanitized["safe_food_count"] = raw_safe_food
            raw_dist = data.get("distance")
            if isinstance(raw_dist, (int, float)) and not isinstance(raw_dist, bool):
                fdist = float(raw_dist)
                if math.isfinite(fdist) and fdist >= 0:
                    sanitized["distance"] = round(fdist, 1)
            raw_resume_hp = data.get("resume_health")
            if (
                isinstance(raw_resume_hp, int)
                and not isinstance(raw_resume_hp, bool)
                and raw_resume_hp >= 0
            ):
                sanitized["resume_health"] = raw_resume_hp
            if data.get("combat_mode") in ("evade", "guard"):
                sanitized["combat_mode"] = data["combat_mode"]
            if data.get("action"):
                sanitized["action"] = _sanitize_text(str(data["action"]), max_tokens=12)[:32]
            if data.get("reason"):
                sanitized["reason"] = _sanitize_text(str(data["reason"]), max_tokens=12)[:32]
            self._pending_events.append(sanitized)
            if len(self._pending_events) > 5:
                self._pending_events = self._pending_events[-5:]
            self._wake()

    async def _run_loop(self) -> None:
        try:
            while not self._stop_requested:
                seq = self._wake_seq
                if not self.is_ready():
                    if self._wake_seq != seq:
                        continue
                    self._wake_event.clear()
                    if self._wake_seq != seq or self.is_ready() or self._stop_requested:
                        continue
                    await self._wake_event.wait()
                    continue

                now = self._now()
                wait_seconds = 0.0
                trigger: str | None = None

                if self._pending_chats:
                    ready_at = self._chat_ready_at if self._chat_ready_at is not None else now
                    cooldown_at = self._last_chat_handled_at + self.config.chat_cooldown_seconds
                    target_at = max(ready_at, cooldown_at)
                    if now >= target_at:
                        trigger = "player_chat"
                    else:
                        wait_seconds = max(0.0, target_at - now)
                elif self._pending_events:
                    target_at = self._last_event_handled_at + self.config.event_cooldown_seconds
                    if now >= target_at:
                        trigger = "milestone_event"
                    else:
                        wait_seconds = max(0.0, target_at - now)
                else:
                    snap = self.plugin.snapshot
                    interval = self._compute_cruise_interval(snap, now)
                    target_at = self._last_decision_at + max(0.01, interval)
                    if now >= target_at:
                        trigger = "cruise"
                    else:
                        wait_seconds = max(0.0, target_at - now)

                if trigger is not None:
                    await self._execute_cycle(trigger)
                    continue

                if self._wake_seq != seq:
                    continue
                self._wake_event.clear()
                if self._wake_seq != seq or self._stop_requested:
                    continue
                try:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=wait_seconds)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise

    def _pop_trigger_batch(
        self, trigger: str, now: float
    ) -> tuple[list[tuple[str, str]], list[dict[str, Any]], str]:
        chat_batch: list[tuple[str, str]] = []
        event_batch: list[dict[str, Any]] = []
        speaking_player = ""
        if trigger == "player_chat":
            chat_batch = list(self._pending_chats)
            self._pending_chats.clear()
            self._chat_first_at = None
            self._chat_ready_at = None
            speaking_player = next((p for p, _ in reversed(chat_batch) if p), "")
            if chat_batch:
                self._last_chat_handled_at = now
        elif trigger == "milestone_event":
            event_batch = list(self._pending_events)
            self._pending_events.clear()
            speaking_player = next(
                (str(ev.get("player") or "") for ev in reversed(event_batch) if ev.get("player")),
                "",
            )
            if event_batch:
                self._last_event_handled_at = now
        return chat_batch, event_batch, speaking_player

    async def _execute_cycle(self, trigger: str) -> None:
        async with self._decision_lock:
            if not self.is_ready():
                return
            now = self._now()
            snap = self.plugin.snapshot
            chat_batch, event_batch, speaking_player = self._pop_trigger_batch(trigger, now)
            if trigger == "player_chat" and not chat_batch:
                return
            if trigger == "milestone_event" and not event_batch:
                return

            if trigger == "milestone_event" and await self._try_fast_survival_milestone(
                event_batch=event_batch, snap=snap, now=now
            ):
                return

            self._last_decision_at = now
            self._first_cruise_pending = False
            self._recovery_retry_pending = False

            unavailable_reason = await self._check_budget_and_rate(now)
            if unavailable_reason is not None:
                await self._handle_unavailable(
                    chat_batch=chat_batch,
                    snap=snap,
                    speaking_player=speaking_player,
                    reason=unavailable_reason,
                )
                return

            screen_status, screen_reason, screen_parsed = await self._screen_if_needed(
                trigger=trigger, snap=snap, chat_batch=chat_batch, event_batch=event_batch
            )
            if screen_status == "blocked":
                self.screen_blocked_count += 1
                self._last_reason = (
                    "screen_blocked"
                    if screen_reason == "passed_false"
                    else f"screen_blocked:{screen_reason}"[:60]
                )
                log.warning(
                    "event=mc_brain_screen_blocked trigger=%s reason=%s count=%d",
                    trigger,
                    screen_reason,
                    self.screen_blocked_count,
                )
                await self._push_status_safe()
                return
            if screen_status == "transient":
                await self._handle_unavailable(
                    chat_batch=chat_batch,
                    snap=snap,
                    speaking_player=speaking_player,
                    reason=screen_reason,
                )
                return

            routing_hint: dict[str, Any] | None = None
            if trigger == "cruise":
                cruise_outcome, routing_hint = await self._route_cruise_decision(
                    screen_parsed, snap=snap
                )
                if cruise_outcome in ("skip", "handled"):
                    return
            elif trigger == "player_chat":
                chat_outcome, routing_hint = await self._route_chat_intent(
                    screen_parsed,
                    snap=snap,
                    chat_batch=chat_batch,
                    speaking_player=speaking_player,
                )
                if chat_outcome == "handled":
                    return

            gen_payload = self._build_generate_payload(
                trigger=trigger,
                snap=snap,
                chat_batch=chat_batch,
                event_batch=event_batch,
                routing_hint=routing_hint,
            )
            self._consecutive_cruise_skips = 0
            self._call_times.append(now)
            try:
                raw_out = await self.plugin.client.ai_generate(
                    "mc.play",
                    gen_payload,
                    task_instruction=MC_PLAY_TASK_INSTRUCTION,
                    response_schema=MC_PLAY_RESPONSE_SCHEMA,
                    budget_lane="proactive",
                )
            except PluginRpcError as exc:
                await self._handle_unavailable(
                    chat_batch=chat_batch,
                    snap=snap,
                    speaking_player=speaking_player,
                    reason=exc.code,
                )
                return

            try:
                parsed = json.loads(_strip_json_fences(str(raw_out or "")))
                decision = McBrainDecision.model_validate(parsed)
            except (ValueError, TypeError, ValidationError) as exc:
                log.warning("event=mc_brain_invalid_decision trigger=%s err=%s", trigger, exc)
                self._last_reason = "invalid_decision"
                await self._push_status_safe()
                return

            input_summary = (
                "; ".join(f"{p}:{m}" for p, m in chat_batch)
                if chat_batch
                else ",".join(str(ev.get("event_type") or "") for ev in event_batch)
                if event_batch
                else "cruise"
            )
            await self._apply_decision(
                decision,
                snap=snap,
                speaking_player=speaking_player,
                trigger=trigger,
                input_summary=input_summary,
                chat_batch=chat_batch,
                event_batch=event_batch,
            )
