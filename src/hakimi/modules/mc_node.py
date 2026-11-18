"""domestic MC adapter owns the fixed RPC surface and translates authenticated node events."""

from __future__ import annotations

import math
from typing import Any, Literal

from ..security import Rejected
from ..world_state import WorldStateManager
from .base import sanitize_sensor_text
from .minecraft_slot import MinecraftSlotModule

_MC_ALLOWED_DISPATCH_FIELDS = (
    "action_type",
    "topic",
    "block_type",
    "x",
    "y",
    "z",
    "target_player",
    "intent_note",
    "mood",
    "gate_condition",
)


class MinecraftNodeAdapter:
    def __init__(self, module: MinecraftSlotModule, transport, background):
        self.module = module
        self.s = module.settings
        self.world_state = module.world_state
        self.store = self.world_state.store
        self.transport = transport
        self.background = background

    async def _ensure_world_state_ready(self) -> WorldStateManager:
        async with self.store.lock:
            ready = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='world_state'"
            )
        if not ready:
            await self.world_state.initialize()
        return self.world_state

    async def _sync_mc_world_focus(self, module):
        world = await self._ensure_world_state_ready()
        snap = module.snapshot
        task = (
            snap.active_task if snap.connected and not module.paused and module.bridge_ready else ""
        )
        await world.experience.mc_snapshot(task, snap.updated_at)

    async def _apply_mc_status_data(
        self, mc_mod: MinecraftSlotModule, data: dict[str, Any]
    ) -> None:
        if isinstance(data.get("snapshot"), dict):
            snap = dict(data["snapshot"])
            for flag in ("bridge_ready", "mc_enabled", "mc_configured", "paused"):
                if flag in data:
                    snap[flag] = data[flag]
            await mc_mod.update_snapshot(snap)
        mc_mod.apply_status(data)
        await self._sync_mc_world_focus(mc_mod)

    async def _sync_mc_node_status(self) -> None:
        nt = self.transport()
        if nt is None or not nt.connected():
            return
        try:
            data = await nt.call("mc.status", {})
        except Exception:
            return
        mc_mod = self.module
        if isinstance(data, dict):
            await self._apply_mc_status_data(mc_mod, data)

    async def on_state(self, connected: bool) -> None:
        if connected:
            self.background(self._sync_mc_node_status())
            return
        self.module.bridge_ready = False
        if self.module.snapshot.connected:
            await self.module.update_snapshot(
                {"connected": False, "bridge_ready": False, "activity": "节点连接断开"}
            )
        ws = await self._ensure_world_state_ready()
        await ws.experience.mc_snapshot("", ws.clock(), disconnected=True)

    async def dispatch(self, ticket: dict[str, Any]) -> dict[str, Any]:
        module, transport = self.module, self.transport()
        if not transport or not transport.connected():
            return {
                "ok": False,
                "reason": "node_disconnected",
                "artifacts": {"not_dispatched": True},
                "summary": await module.get_sensor_summary(),
            }
        verb = str(ticket["action_type"])
        try:
            if verb in {"status", "pause", "resume"}:
                data = await transport.call("mc." + verb, {})
            else:
                params = {
                    key: ticket[key]
                    for key in _MC_ALLOWED_DISPATCH_FIELDS
                    if ticket.get(key) is not None
                }
                for axis in ("x", "y", "z"):
                    value = params.pop(axis, None)
                    if type(value) in (int, float) and math.isfinite(value):
                        params[axis] = float(value)
                data = await transport.call("mc.dispatch_goal", params)
        except Rejected as exc:
            return {"ok": False, "reason": str(exc), "summary": await module.get_sensor_summary()}

        if not isinstance(data, dict):
            return {
                "ok": False,
                "reason": "mc_invalid_receipt",
                "summary": await module.get_sensor_summary(),
            }
        if any(
            k in data
            for k in (
                "snapshot",
                "bridge_ready",
                "mc_enabled",
                "mc_configured",
                "paused",
                "connected",
                "activity",
                "phase",
            )
        ):
            await self._apply_mc_status_data(module, data)
        if "ok" in data:
            ok = data["ok"] is True and data.get("accepted", True) is True
        elif verb == "status":
            ok = data.get("bridge_ready") is True
        else:
            ok = data.get("accepted") is True
        artifacts = {
            k: getattr(module, k) for k in ("bridge_ready", "mc_enabled", "mc_configured", "paused")
        }
        artifacts["connected"] = module.snapshot.connected
        if "accepted" in data:
            artifacts["accepted"] = data["accepted"] is True
        if "phase" in data:
            artifacts["phase"] = sanitize_sensor_text(str(data["phase"]), self.s, max_tokens=12)
        return {
            "ok": ok,
            "summary": data.get("summary") or await module.get_sensor_summary(),
            "reason": data.get("reason") or (None if ok else "mc_invalid_receipt"),
            "artifacts": artifacts,
        }

    async def on_event(self, frame: dict[str, Any]) -> str:
        event_name = frame.get("event")
        payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
        if event_name == "mc.snapshot":
            mc_mod = self.module
            await mc_mod.update_snapshot(payload)
            await self._sync_mc_world_focus(mc_mod)
        elif event_name == "mc.milestone":
            raw_type = payload.get("event_type") or payload.get("type") or "mc_milestone"
            clean_type = (
                sanitize_sensor_text(str(raw_type), self.s, max_tokens=16) or "mc_milestone"
            )
            raw_imp = payload.get("importance", "normal")
            importance: Literal["low", "normal", "high"] = (
                raw_imp if raw_imp in ("low", "normal", "high") else "normal"
            )
            event_data = (
                dict(payload["payload"])
                if isinstance(payload.get("payload"), dict)
                else {
                    k: v
                    for k, v in payload.items()
                    if k not in {"event_type", "type", "importance", "event_id"}
                }
            )
            ws_mgr = await self._ensure_world_state_ready()
            await ws_mgr.publish_event(
                "minecraft",
                clean_type,
                event_data,
                importance=importance,
                business_key=payload.get("event_id") or frame.get("id"),
            )
        return "accepted"
