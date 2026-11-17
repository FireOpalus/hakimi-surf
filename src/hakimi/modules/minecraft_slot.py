"""Minecraft Mineflayer capability slot (CN node, HIGH attention)."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..config import Settings
from .base import AttentionLevel, BaseModule, GoalResult, NodeRegion, sanitize_sensor_text

if TYPE_CHECKING:
    from ..world_state import MilestoneEvent, WorldStateManager


def _fmt_num(val: int | float) -> str:
    fval = float(val)
    return str(int(fval)) if fval.is_integer() else str(round(fval, 1))


def _normalize_pos(
    pos: Any, settings: Settings | None
) -> tuple[float, float, float] | str | None:
    if pos is None:
        return None
    if isinstance(pos, dict) and all(k in pos for k in ("x", "y", "z")):
        try:
            return (float(pos["x"]), float(pos["y"]), float(pos["z"]))
        except (TypeError, ValueError):
            return sanitize_sensor_text(str(pos), settings, max_tokens=16) or None
    if isinstance(pos, (list, tuple)) and len(pos) == 3:
        try:
            return (float(pos[0]), float(pos[1]), float(pos[2]))
        except (TypeError, ValueError):
            return sanitize_sensor_text(str(pos), settings, max_tokens=16) or None
    if isinstance(pos, str):
        cleaned = sanitize_sensor_text(pos, settings, max_tokens=16)
        return cleaned or None
    return sanitize_sensor_text(str(pos), settings, max_tokens=16) or None


class MinecraftSnapshot(BaseModel):
    """Sanitized snapshot of the remote CN Mineflayer agent."""

    model_config = ConfigDict(extra="forbid")

    connected: bool = False
    dimension: str = ""
    pos: tuple[float, float, float] | str | None = None
    health: float | None = Field(default=None, ge=0)
    food: float | None = Field(default=None, ge=0)
    activity: str = ""
    nearby_entities: list[str] = Field(default_factory=list)
    recent_chat_or_sign: str = ""
    updated_at: float = 0.0


class MinecraftSlotModule(BaseModule):
    """Tracks sanitized remote Mineflayer snapshots and publishes MilestoneEvents."""

    module_id: str = "minecraft"
    node_region: NodeRegion = "CN"
    attention_level: AttentionLevel = "HIGH"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        world_state: WorldStateManager | None = None,
        remote_dispatcher: Callable[..., Any] | None = None,
    ) -> None:
        super().__init__(settings)
        self.world_state = world_state
        self.remote_dispatcher = remote_dispatcher
        self.snapshot = MinecraftSnapshot()
        self._has_snapshot = False

    def bind_world_state(self, world_state: WorldStateManager) -> None:
        """Attach a WorldStateManager for publishing MilestoneEvents."""
        self.world_state = world_state

    def _sanitize_snapshot(
        self, raw: dict[str, Any] | MinecraftSnapshot
    ) -> MinecraftSnapshot:
        data = raw.model_dump() if isinstance(raw, MinecraftSnapshot) else dict(raw)
        connected = bool(data.get("connected", False))
        dimension = sanitize_sensor_text(
            str(data.get("dimension") or ""), self.settings, max_tokens=12
        )
        pos = _normalize_pos(data.get("pos"), self.settings)

        raw_health = data.get("health")
        health = (
            float(raw_health)
            if isinstance(raw_health, (int, float)) and not isinstance(raw_health, bool)
            else None
        )
        raw_food = data.get("food")
        food = (
            float(raw_food)
            if isinstance(raw_food, (int, float)) and not isinstance(raw_food, bool)
            else None
        )
        activity = sanitize_sensor_text(
            str(data.get("activity") or ""), self.settings, max_tokens=20
        )

        raw_entities = data.get("nearby_entities") or []
        entities: list[str] = []
        if isinstance(raw_entities, (list, tuple)):
            for item in raw_entities[:5]:
                clean_item = sanitize_sensor_text(str(item), self.settings, max_tokens=10)
                if clean_item:
                    entities.append(clean_item)
        elif isinstance(raw_entities, str) and raw_entities.strip():
            clean_item = sanitize_sensor_text(raw_entities, self.settings, max_tokens=12)
            if clean_item:
                entities.append(clean_item)

        raw_chat = data.get("recent_chat_or_sign")
        if isinstance(raw_chat, (list, tuple)):
            joined = " / ".join(str(x) for x in raw_chat if x is not None)
        else:
            joined = str(raw_chat or "")
        recent_chat_or_sign = sanitize_sensor_text(joined, self.settings, max_tokens=20)

        raw_ts = data.get("updated_at")
        updated_at = (
            float(raw_ts)
            if isinstance(raw_ts, (int, float)) and not isinstance(raw_ts, bool)
            else time.time()
        )
        return MinecraftSnapshot(
            connected=connected,
            dimension=dimension,
            pos=pos,
            health=health,
            food=food,
            activity=activity,
            nearby_entities=entities,
            recent_chat_or_sign=recent_chat_or_sign,
            updated_at=updated_at,
        )

    def apply_snapshot(
        self, raw: dict[str, Any] | MinecraftSnapshot
    ) -> MinecraftSnapshot:
        """Sanitize and store a remote Mineflayer snapshot synchronously."""
        sanitized = self._sanitize_snapshot(raw)
        self.snapshot = sanitized
        self._has_snapshot = True
        return sanitized

    async def publish_milestone(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        importance: Literal["low", "normal", "high"] = "normal",
    ) -> MilestoneEvent | None:
        """Publish a sanitized MilestoneEvent via the attached WorldStateManager."""
        if self.world_state is None:
            return None
        return await self.world_state.publish_event(
            self.module_id,
            event_type,
            payload if payload is not None else {},
            importance=importance,
        )

    async def update_snapshot(
        self,
        raw: dict[str, Any] | MinecraftSnapshot,
        *,
        milestone: dict[str, Any] | None = None,
    ) -> MinecraftSnapshot:
        """Sanitize snapshot, update local slot state, and publish milestone events."""
        prev = self.snapshot
        was_initialized = self._has_snapshot
        curr = self.apply_snapshot(raw)

        if self.world_state is not None:
            explicit = milestone or (raw.get("milestone") if isinstance(raw, dict) else None)
            if isinstance(explicit, dict) and explicit.get("event_type"):
                imp = explicit.get("importance", "normal")
                if imp not in ("low", "normal", "high"):
                    imp = "normal"
                await self.publish_milestone(
                    str(explicit["event_type"]),
                    dict(explicit.get("payload") or {}),
                    importance=imp,
                )
            elif not prev.connected and curr.connected:
                await self.publish_milestone(
                    "mc_connected",
                    {"dimension": curr.dimension, "activity": curr.activity},
                    importance="normal",
                )
            elif was_initialized and prev.connected and not curr.connected:
                await self.publish_milestone(
                    "mc_disconnected",
                    {"activity": curr.activity},
                    importance="normal",
                )
            elif (
                prev.connected
                and curr.connected
                and prev.dimension
                and curr.dimension
                and prev.dimension != curr.dimension
            ):
                await self.publish_milestone(
                    "dimension_changed",
                    {"from": prev.dimension, "to": curr.dimension},
                    importance="normal",
                )

            if (
                curr.connected
                and curr.health is not None
                and prev.health is not None
                and (
                    (prev.health > 0 and curr.health <= 0)
                    or (prev.health > 4 and curr.health <= 4)
                )
            ):
                await self.publish_milestone(
                    "mc_health_alert",
                    {"health": curr.health, "dimension": curr.dimension},
                    importance="high",
                )
        return curr

    async def _raw_sensor_summary(self) -> str:
        snap = self.snapshot
        if not snap.connected:
            if snap.activity:
                return f"MC离线 {snap.activity}"
            return "MC离线" if self._has_snapshot else ""

        parts = ["MC在线"]
        if snap.dimension:
            parts.append(f"维度:{snap.dimension}")
        if snap.pos is not None:
            if isinstance(snap.pos, tuple):
                x, y, z = snap.pos
                parts.append(f"坐标:({_fmt_num(x)}, {_fmt_num(y)}, {_fmt_num(z)})")
            else:
                parts.append(f"坐标:{snap.pos}")
        if snap.health is not None:
            parts.append(f"生命:{_fmt_num(snap.health)}/20")
        if snap.activity:
            parts.append(f"正在:{snap.activity}")
        if snap.food is not None:
            parts.append(f"饱食:{_fmt_num(snap.food)}/20")
        if snap.nearby_entities:
            parts.append(f"附近:{','.join(snap.nearby_entities)}")
        if snap.recent_chat_or_sign:
            parts.append(f"见闻:{snap.recent_chat_or_sign}")
        return " ".join(parts)

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        action_type = str(ticket.get("action_type", "")).strip()
        if action_type == "status":
            return GoalResult(
                ok=True,
                module_id=self.module_id,
                summary=await self.get_sensor_summary(),
                artifacts={"connected": self.snapshot.connected},
            )
        if self.remote_dispatcher is None:
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                summary=await self.get_sensor_summary(),
                reason=(
                    "remote_node_not_connected"
                    if not self.snapshot.connected
                    else "remote_dispatcher_unconfigured"
                ),
            )
        res = self.remote_dispatcher(ticket)
        if inspect.isawaitable(res):
            res = await res
        if isinstance(res, GoalResult):
            return GoalResult(
                ok=res.ok,
                module_id=self.module_id,
                summary=sanitize_sensor_text(res.summary, self.settings, max_tokens=90),
                artifacts=res.artifacts,
                reason=(
                    sanitize_sensor_text(res.reason, self.settings, max_tokens=20)
                    if res.reason is not None
                    else None
                ),
            )
        if isinstance(res, dict):
            summary = sanitize_sensor_text(
                str(res.get("summary", "")), self.settings, max_tokens=90
            )
            if not summary:
                summary = await self.get_sensor_summary()
            reason = (
                sanitize_sensor_text(str(res["reason"]), self.settings, max_tokens=20)
                if res.get("reason") is not None
                else None
            )
            return GoalResult(
                ok=bool(res.get("ok", True)),
                module_id=self.module_id,
                summary=summary,
                artifacts=dict(res.get("artifacts") or {}),
                reason=reason,
            )
        return GoalResult(
            ok=True,
            module_id=self.module_id,
            summary=await self.get_sensor_summary(),
        )
