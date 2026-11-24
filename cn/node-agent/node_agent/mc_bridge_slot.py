"""Mineflayer sidecar WebSocket bridge slot for the domestic node agent."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import math
import random
import re
import time
import uuid
from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect

from .bili_collector import CollectorError

log = logging.getLogger("node_agent.mc_bridge")

McEventCallback = Callable[[str, dict[str, Any]], Any]

_BRIDGE_MAX_FRAME_BYTES = 262_144
_BRIDGE_EVENT_QUEUE_SIZE = 64
_RPC_ERROR_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_SNAPSHOT_META_KEYS = ("bridge_ready", "mc_enabled", "mc_configured", "paused")
_EQUIPMENT_SLOTS = ("main_hand", "off_hand", "head", "chest", "legs", "feet")
_CANONICAL_ITEM_ID_RE = re.compile(r"^[a-z0-9_]{1,48}$")
_INVENTORY_ENTRY_RE = re.compile(r"^([a-z0-9_]{1,48}):([1-9][0-9]{0,4})$")
_PLAYER_ID_RE = re.compile(r"^[A-Za-z0-9_]{1,16}$")
_VALID_COMBAT_MODES = frozenset({"evade", "guard"})
_VALID_TIME_OF_DAY = frozenset({"day", "sunset", "night", "sunrise"})


def _empty_equipment() -> dict[str, str]:
    return {slot: "" for slot in _EQUIPMENT_SLOTS}


def _sanitize_item_id(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    cleaned = raw.strip().lower()
    if cleaned.startswith("minecraft:"):
        cleaned = cleaned[len("minecraft:") :]
    return cleaned if _CANONICAL_ITEM_ID_RE.fullmatch(cleaned) else ""


def _sanitize_equipment(raw: Any) -> dict[str, str]:
    out = _empty_equipment()
    if not isinstance(raw, dict):
        return out
    for slot in _EQUIPMENT_SLOTS:
        out[slot] = _sanitize_item_id(raw.get(slot))
    return out


def _sanitize_inventory_summary(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for entry in raw:
        if len(out) >= 16:
            break
        if not isinstance(entry, str):
            continue
        cleaned = entry.strip().lower()
        if cleaned.startswith("minecraft:"):
            cleaned = cleaned[len("minecraft:") :]
        match = _INVENTORY_ENTRY_RE.fullmatch(cleaned)
        if not match:
            continue
        item_id, count_str = match.group(1), match.group(2)
        if item_id in seen:
            continue
        seen.add(item_id)
        out.append(f"{item_id}:{count_str}")
    return out


def _sanitize_free_slots(raw: Any) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    if 0 <= raw <= 36:
        return raw
    return None


def _sanitize_combat_mode(raw: Any, fallback: str = "evade") -> str:
    safe_fallback = fallback if fallback in _VALID_COMBAT_MODES else "evade"
    if not isinstance(raw, str):
        return safe_fallback
    cleaned = raw.strip().lower()
    return cleaned if cleaned in _VALID_COMBAT_MODES else safe_fallback


def _sanitize_guard_target(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    cleaned = raw.strip()
    return cleaned if _PLAYER_ID_RE.fullmatch(cleaned) else ""


def _sanitize_time_of_day(raw: Any) -> str:
    if not isinstance(raw, str):
        return ""
    cleaned = raw.strip().lower()
    return cleaned if cleaned in _VALID_TIME_OF_DAY else ""


def _sanitize_nearby_resources(raw: Any) -> list[str]:
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for entry in raw:
        if len(out) >= 12:
            break
        clean = _sanitize_item_id(entry)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError("duplicate_key")
        obj[key] = value
    return obj


def _reject_constant(token: str) -> Any:
    raise ValueError(f"invalid_json_constant:{token}")


def decode_node_frame(raw: str | bytes, max_bytes: int) -> dict[str, Any]:
    """Decode one bounded JSON object frame while rejecting duplicate keys or NaN/Infinity."""
    if isinstance(raw, bytes):
        if len(raw) > max_bytes:
            raise CollectorError("node_frame_too_large")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise CollectorError("node_invalid_json") from None
    elif isinstance(raw, str):
        encoded = raw.encode("utf-8")
        if len(encoded) > max_bytes:
            raise CollectorError("node_frame_too_large")
        text = raw
    else:
        raise CollectorError("node_invalid_json")

    try:
        parsed = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (ValueError, TypeError, RecursionError):
        raise CollectorError("node_invalid_json") from None
    if not isinstance(parsed, dict):
        raise CollectorError("node_invalid_json")
    return parsed


def compute_backoff_step(
    delay: float,
    *,
    max_delay: float,
    rng: random.Random | None = None,
) -> tuple[float, float]:
    """Return `(sleep_seconds, next_delay)` for exponential backoff with half-full jitter."""
    bounded = min(max(delay, 0.01), max_delay)
    sampler = rng if rng is not None else random
    sleep_for = sampler.uniform(bounded / 2.0, bounded)
    next_delay = min(bounded * 2.0, max_delay)
    return sleep_for, next_delay


class McBridgeSlot:
    """Phase 3 stub when disabled; persistent local WS bridge to hakimi-mc-agent when enabled."""

    def __init__(
        self,
        *,
        enabled: bool = False,
        bridge_url: str = "ws://hakimi-mc-agent:8791/ws",
        token: str = "",
        rpc_timeout: float = 35.0,
        max_frame_bytes: int = _BRIDGE_MAX_FRAME_BYTES,
        reconnect_base: float = 0.5,
        reconnect_max: float = 15.0,
        on_event: McEventCallback | None = None,
    ) -> None:
        self.enabled: bool = bool(enabled)
        self.bridge_url: str = bridge_url
        self._token: str = token
        self.rpc_timeout: float = max(0.01, float(rpc_timeout))
        self.max_frame_bytes: int = int(max_frame_bytes)
        self.reconnect_base: float = max(0.01, float(reconnect_base))
        self.reconnect_max: float = max(self.reconnect_base, float(reconnect_max))
        self.on_event: McEventCallback | None = on_event

        self._connected: bool = False
        self._paused: bool = False
        self._bridge_ready: bool = False
        self._mc_enabled: bool = False
        self._mc_configured: bool = False
        self._activity: str = ""
        self._updated_at: float = 0.0
        self._latest_snapshot: dict[str, Any] = self._default_snapshot()

        self._active_ws: Any = None
        self._send_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self.backoff_history: list[float] = []

    def _default_snapshot(self) -> dict[str, Any]:
        return {
            "connected": False,
            "bridge_ready": False,
            "mc_enabled": False,
            "mc_configured": False,
            "paused": self._paused,
            "dimension": "unknown",
            "pos": None,
            "health": None,
            "food": None,
            "equipment": _empty_equipment(),
            "inventory_summary": [],
            "free_slots": None,
            "combat_mode": "evade",
            "guard_target": "",
            "time_of_day": "",
            "nearby_resources": [],
            "activity": "",
            "nearby_entities": [],
            "recent_chat_or_sign": "",
            "updated_at": 0.0,
        }

    def current_snapshot(self) -> dict[str, Any]:
        """Return a copied snapshot dict safe for upstream node.hello/mc.snapshot sync."""
        snap = dict(self._latest_snapshot)
        if isinstance(snap.get("nearby_entities"), list):
            snap["nearby_entities"] = list(snap["nearby_entities"])
        if isinstance(snap.get("nearby_players"), list):
            snap["nearby_players"] = list(snap["nearby_players"])
        if isinstance(snap.get("equipment"), dict):
            snap["equipment"] = dict(snap["equipment"])
        else:
            snap["equipment"] = _empty_equipment()
        if isinstance(snap.get("inventory_summary"), list):
            snap["inventory_summary"] = list(snap["inventory_summary"])
        else:
            snap["inventory_summary"] = []
        if isinstance(snap.get("nearby_resources"), list):
            snap["nearby_resources"] = list(snap["nearby_resources"])
        else:
            snap["nearby_resources"] = []
        return snap

    def start(self) -> None:
        """Start the persistent sidecar bridge loop when enabled."""
        if not self.enabled:
            return
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop(), name="mc-bridge-loop")

    async def close(self) -> None:
        """Stop the bridge loop, fail pending RPCs, and close active sidecar WS."""
        self._stop.set()
        self._fail_pending("mc_bridge_disconnected")
        ws = self._active_ws
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()
        task = self._task
        if task is not None:
            if task is not asyncio.current_task() and not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                if task is not asyncio.current_task():
                    await task
        self._task = None

    def _fail_pending(self, reason: str) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(CollectorError(reason))

    def _disconnected_status(self, *, reason: str | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {
            "connected": False,
            "paused": self._paused,
            "bridge_ready": False,
            "mc_enabled": False,
            "mc_configured": False,
            "activity": self._activity,
            "phase": "phase4_live",
            "updated_at": self._updated_at,
        }
        if reason is not None:
            out["reason"] = reason
        return out

    def _apply_snapshot_dict(
        self,
        raw: dict[str, Any],
        *,
        default_bridge_ready: bool | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        bridge_ready = (
            bool(raw["bridge_ready"])
            if "bridge_ready" in raw
            else (self._bridge_ready if default_bridge_ready is None else default_bridge_ready)
        )
        connected = bool(raw.get("connected", self._connected))
        paused = bool(raw.get("paused", self._paused))
        mc_enabled = bool(raw.get("mc_enabled", self._mc_enabled))
        mc_configured = bool(raw.get("mc_configured", self._mc_configured))
        activity = str(raw["activity"]) if isinstance(raw.get("activity"), str) else self._activity
        raw_ts = raw.get("updated_at")
        updated_at = (
            float(raw_ts)
            if isinstance(raw_ts, (int, float))
            and not isinstance(raw_ts, bool)
            and math.isfinite(raw_ts)
            else now
        )

        self._bridge_ready = bridge_ready
        self._connected = connected
        self._paused = paused
        self._mc_enabled = mc_enabled
        self._mc_configured = mc_configured
        self._activity = activity
        self._updated_at = updated_at

        prev_combat_mode = _sanitize_combat_mode(self._latest_snapshot.get("combat_mode"), "evade")
        combat_mode = (
            _sanitize_combat_mode(raw["combat_mode"], prev_combat_mode)
            if "combat_mode" in raw
            else prev_combat_mode
        )
        if connected:
            equipment = (
                _sanitize_equipment(raw["equipment"])
                if "equipment" in raw
                else _sanitize_equipment(self._latest_snapshot.get("equipment"))
            )
            inventory_summary = (
                _sanitize_inventory_summary(raw["inventory_summary"])
                if "inventory_summary" in raw
                else _sanitize_inventory_summary(self._latest_snapshot.get("inventory_summary"))
            )
            free_slots = (
                _sanitize_free_slots(raw["free_slots"])
                if "free_slots" in raw
                else _sanitize_free_slots(self._latest_snapshot.get("free_slots"))
            )
            guard_target = (
                (
                    _sanitize_guard_target(raw["guard_target"])
                    if "guard_target" in raw
                    else _sanitize_guard_target(self._latest_snapshot.get("guard_target"))
                )
                if combat_mode == "guard"
                else ""
            )
            time_of_day = (
                _sanitize_time_of_day(raw["time_of_day"])
                if "time_of_day" in raw
                else _sanitize_time_of_day(self._latest_snapshot.get("time_of_day"))
            )
            nearby_resources = (
                _sanitize_nearby_resources(raw["nearby_resources"])
                if "nearby_resources" in raw
                else _sanitize_nearby_resources(self._latest_snapshot.get("nearby_resources"))
            )
        else:
            equipment = _empty_equipment()
            inventory_summary = []
            free_slots = None
            guard_target = ""
            time_of_day = ""
            nearby_resources = []

        merged = dict(self._latest_snapshot)
        merged.update(raw)
        merged["connected"] = connected
        merged["bridge_ready"] = bridge_ready
        merged["mc_enabled"] = mc_enabled
        merged["mc_configured"] = mc_configured
        merged["paused"] = paused
        merged["activity"] = activity
        merged["updated_at"] = updated_at
        merged["equipment"] = equipment
        merged["inventory_summary"] = inventory_summary
        merged["free_slots"] = free_slots
        merged["combat_mode"] = combat_mode
        merged["guard_target"] = guard_target
        merged["time_of_day"] = time_of_day
        merged["nearby_resources"] = nearby_resources
        if isinstance(merged.get("nearby_entities"), list):
            merged["nearby_entities"] = list(merged["nearby_entities"])
        self._latest_snapshot = merged
        return self.current_snapshot()

    def _update_from_rpc_data(self, data: dict[str, Any]) -> None:
        raw_snap = data.get("snapshot")
        if isinstance(raw_snap, dict):
            merged = dict(raw_snap)
            for key in _SNAPSHOT_META_KEYS:
                if key in data and key not in merged:
                    merged[key] = data[key]
            default_ready = bool(data.get("bridge_ready", True))
            data["snapshot"] = self._apply_snapshot_dict(merged, default_bridge_ready=default_ready)
            return

        patch: dict[str, Any] = {}
        for key in (
            "connected",
            "bridge_ready",
            "mc_enabled",
            "mc_configured",
            "paused",
            "activity",
            "combat_mode",
            "guard_target",
            "updated_at",
        ):
            if key in data:
                patch[key] = data[key]
        if "bridge_ready" not in patch:
            patch["bridge_ready"] = True
        self._apply_snapshot_dict(patch, default_bridge_ready=True)

    async def _emit_event(self, event_name: str, payload: dict[str, Any]) -> None:
        if event_name not in {"mc.snapshot", "mc.milestone"}:
            return
        callback = self.on_event
        if callback is None:
            return
        try:
            res = callback(event_name, payload)
            if inspect.isawaitable(res):
                await res
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "event=mc_bridge_on_event_failed event=%s err=%s",
                event_name,
                type(exc).__name__,
            )

    def _enqueue_event(
        self,
        queue: asyncio.Queue[tuple[str, dict[str, Any]] | None],
        item: tuple[str, dict[str, Any]],
    ) -> None:
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
                queue.task_done()
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(item)

    def _handle_frame(
        self,
        frame: dict[str, Any],
        events: asyncio.Queue[tuple[str, dict[str, Any]] | None],
    ) -> None:
        frame_type = frame.get("type")
        if frame_type == "rpc_response":
            req_id = frame.get("id")
            future = self._pending.get(req_id) if isinstance(req_id, str) else None
            if future is not None and not future.done():
                future.set_result(frame)
            return

        if frame_type != "event":
            return

        event_name = frame.get("event")
        payload = frame.get("payload")
        if not isinstance(event_name, str) or not isinstance(payload, dict):
            return

        if event_name == "mc.hello":
            merged: dict[str, Any] = {}
            if isinstance(payload.get("snapshot"), dict):
                merged.update(payload["snapshot"])
            for key in _SNAPSHOT_META_KEYS:
                if key in payload:
                    merged[key] = payload[key]
            merged.setdefault("bridge_ready", True)
            self._apply_snapshot_dict(merged, default_bridge_ready=True)
            return

        if event_name == "mc.snapshot":
            snap = self._apply_snapshot_dict(payload, default_bridge_ready=True)
            self._enqueue_event(events, ("mc.snapshot", snap))
            return

        if event_name == "mc.milestone":
            self._enqueue_event(events, ("mc.milestone", dict(payload)))

    async def _serve_connection(self, ws: Any) -> None:
        events: asyncio.Queue[tuple[str, dict[str, Any]] | None] = asyncio.Queue(
            maxsize=_BRIDGE_EVENT_QUEUE_SIZE
        )

        async def consume_events() -> None:
            while True:
                item = await events.get()
                try:
                    if item is None:
                        return
                    ev_name, ev_payload = item
                    await self._emit_event(ev_name, ev_payload)
                finally:
                    events.task_done()

        consumer = asyncio.create_task(consume_events(), name="mc-bridge-events")
        self._active_ws = ws
        cancelled = False
        try:
            async for raw in ws:
                if self._stop.is_set():
                    break
                try:
                    frame = decode_node_frame(raw, self.max_frame_bytes)
                except CollectorError as exc:
                    log.info("event=mc_bridge_bad_frame err=%s", exc.code)
                    continue
                self._handle_frame(frame, events)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            self._active_ws = None
            self._fail_pending("mc_bridge_disconnected")

            if cancelled or self._stop.is_set():
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
            else:
                if events.full():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        events.get_nowait()
                        events.task_done()
                with contextlib.suppress(asyncio.QueueFull):
                    events.put_nowait(None)
                try:
                    await asyncio.wait_for(consumer, timeout=2.0)
                except (TimeoutError, Exception):
                    consumer.cancel()
                    await asyncio.gather(consumer, return_exceptions=True)

            was_ready = self._bridge_ready
            self._bridge_ready = False
            self._connected = False
            self._mc_enabled = False
            self._mc_configured = False
            if was_ready:
                now = time.time()
                self._activity = "MC容器重连中"
                self._updated_at = now
                synthetic = {
                    **self.current_snapshot(),
                    "connected": False,
                    "bridge_ready": False,
                    "mc_enabled": False,
                    "mc_configured": False,
                    "paused": self._paused,
                    "pos": None,
                    "health": None,
                    "food": None,
                    "equipment": _empty_equipment(),
                    "inventory_summary": [],
                    "free_slots": None,
                    "combat_mode": _sanitize_combat_mode(
                        self._latest_snapshot.get("combat_mode"), "evade"
                    ),
                    "guard_target": "",
                    "time_of_day": "",
                    "nearby_resources": [],
                    "nearby_entities": [],
                    "activity": "MC容器重连中",
                    "updated_at": now,
                }
                self._latest_snapshot = synthetic
                if not cancelled and not self._stop.is_set():
                    await self._emit_event("mc.snapshot", dict(synthetic))

    async def _run_loop(self) -> None:
        delay = min(self.reconnect_base, self.reconnect_max)
        while not self._stop.is_set():
            connected_at = asyncio.get_running_loop().time()
            try:
                async with connect(
                    self.bridge_url,
                    additional_headers={"Authorization": f"Bearer {self._token}"},
                    ping_interval=20.0,
                    ping_timeout=20.0,
                    open_timeout=5.0,
                    close_timeout=3.0,
                    max_size=self.max_frame_bytes,
                    max_queue=16,
                    compression=None,
                    proxy=None,
                ) as ws:
                    await self._serve_connection(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.info("event=mc_bridge_reconnect err=%s", type(exc).__name__)

            if self._stop.is_set():
                break
            if asyncio.get_running_loop().time() - connected_at > 60.0:
                delay = min(self.reconnect_base, self.reconnect_max)

            sleep_for, next_delay = compute_backoff_step(delay, max_delay=self.reconnect_max)
            self.backoff_history.append(sleep_for)
            if len(self.backoff_history) > 64:
                self.backoff_history.pop(0)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
            except TimeoutError:
                pass
            delay = next_delay

    async def _rpc(
        self, action: str, params: dict[str, Any]
    ) -> tuple[bool, dict[str, Any], str | None]:
        ws = self._active_ws
        if ws is None:
            return False, {}, "mc_bridge_disconnected"

        req_id = uuid.uuid4().hex
        try:
            raw = json.dumps(
                {
                    "type": "rpc_request",
                    "id": req_id,
                    "action": action,
                    "params": params,
                },
                allow_nan=False,
                ensure_ascii=False,
            )
        except (TypeError, ValueError):
            return False, {}, "mc_invalid_params"

        if len(raw.encode("utf-8")) > self.max_frame_bytes:
            return False, {}, "node_frame_too_large"

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = future
        try:
            async with asyncio.timeout(self.rpc_timeout):
                async with self._send_lock:
                    if ws is not self._active_ws:
                        raise CollectorError("mc_bridge_disconnected")
                    await ws.send(raw)
                resp = await future
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return False, {}, "mc_rpc_timeout"
        except CollectorError as exc:
            return False, {}, exc.code
        except Exception:
            return False, {}, "mc_bridge_disconnected"
        finally:
            self._pending.pop(req_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

        if not isinstance(resp, dict) or resp.get("type") != "rpc_response":
            return False, {}, "mc_invalid_response"
        if resp.get("ok") is False:
            raw_err = resp.get("error")
            err_code = (
                raw_err
                if isinstance(raw_err, str) and _RPC_ERROR_RE.fullmatch(raw_err)
                else "mc_rpc_failed"
            )
            return False, {}, err_code
        if resp.get("ok") is not True or not isinstance(resp.get("data"), dict):
            return False, {}, "mc_invalid_response"

        data = dict(resp["data"])
        self._update_from_rpc_data(data)
        return True, data, None

    async def status(self) -> dict[str, Any]:
        """Return current bridge readiness and pause state."""
        if not self.enabled:
            return {
                "connected": self._connected,
                "paused": self._paused,
                "bridge_ready": False,
                "activity": self._activity,
                "phase": "phase4_stub",
                "updated_at": self._updated_at,
            }

        ok, data, err = await self._rpc("mc.status", {})
        if not ok:
            reason = None if err == "mc_bridge_disconnected" else err
            return self._disconnected_status(reason=reason)
        data.setdefault("phase", "phase4_live")
        data.setdefault("bridge_ready", True)
        return data

    async def dispatch_goal(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Dispatch a Mineflayer goal to the sidecar when enabled and connected."""
        clean_params = dict(params) if isinstance(params, dict) else {}
        if not self.enabled:
            return {
                "ok": False,
                "accepted": False,
                "bridge_ready": False,
                "paused": self._paused,
                "reason": "mc_paused" if self._paused else "mc_bridge_not_configured",
            }

        ok, data, err = await self._rpc("mc.dispatch_goal", clean_params)
        if not ok:
            is_disc = err == "mc_bridge_disconnected"
            return {
                "ok": False,
                "accepted": False,
                "bridge_ready": False if is_disc else self._bridge_ready,
                "paused": self._paused,
                "reason": err or "mc_bridge_disconnected",
            }
        return data

    async def _set_paused(self, paused: bool) -> dict[str, Any]:
        if not self.enabled:
            self._paused = paused
            self._updated_at = time.time()
            return {
                "ok": True,
                "connected": self._connected,
                "paused": self._paused,
                "bridge_ready": False,
            }

        action = "mc.pause" if paused else "mc.resume"
        ok, data, err = await self._rpc(action, {})
        if not ok:
            is_disc = err == "mc_bridge_disconnected"
            return {
                "ok": False,
                "connected": False if is_disc else self._connected,
                "paused": self._paused,
                "bridge_ready": False if is_disc else self._bridge_ready,
                "reason": err or "mc_bridge_disconnected",
            }
        return data

    async def pause(self) -> dict[str, Any]:
        """Pause the Mineflayer bridge slot or sidecar."""
        return await self._set_paused(True)

    async def resume(self) -> dict[str, Any]:
        """Resume the Mineflayer bridge slot or sidecar."""
        return await self._set_paused(False)
