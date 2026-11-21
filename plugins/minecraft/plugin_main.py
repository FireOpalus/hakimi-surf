"""Standalone hot-plug Minecraft bridge plugin (CN node, HIGH attention)."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import signal
import time
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from hakimi_plugin_sdk import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
    ThinkingSceneSpec,
)

from .play_brain import McBrainConfig, McPlayBrain, _sanitize_text

log = logging.getLogger("hakimi_plugin_minecraft")

_MC_THEME = re.compile(
    r"我的世界|minecraft|\bmc\b|红石|生电|建筑教程|生存建筑|刷铁机|下界|末地|苦力怕",
    re.I,
)

_MC_ALLOWED_DISPATCH_FIELDS = (
    "action_type",
    "topic",
    "block_type",
    "x",
    "y",
    "z",
    "target_player",
    "message",
    "sleep_server",
    "intent_note",
    "mood",
    "gate_condition",
)

_BRIDGE_FLAG_KEYS = ("bridge_ready", "mc_enabled", "mc_configured", "paused")
_NON_SNAPSHOT_KEYS = frozenset(
    {
        "bridge_ready",
        "mc_enabled",
        "mc_configured",
        "paused",
        "phase",
        "snapshot",
        "milestone",
        "reason",
        "accepted",
        "ok",
    }
)
_SNAPSHOT_FIELD_KEYS = frozenset(
    {
        "connected",
        "dimension",
        "pos",
        "health",
        "food",
        "activity",
        "active_task",
        "nearby_entities",
        "nearby_players",
        "recent_chat_or_sign",
        "updated_at",
    }
)
_BRAIN_MILESTONE_EVENTS = frozenset(
    {"mc_player_chat", "mc_health_alert", "mc_death", "mc_rare_find", "mc_player_spotted"}
)
_UNCERTAIN_REASONS = frozenset(
    {
        "mc_rpc_timeout",
        "mc_invalid_response",
        "node_rpc_timeout",
        "node_disconnected",
        "mc_invalid_receipt",
        "node_invalid_response",
        "mc_bridge_timeout",
        "mc_bridge_disconnected",
        "mc_bridge_invalid_response",
        "node_rpc_failed",
        "plugin_rpc_timeout",
        "plugin_disconnected",
    }
)
_STATUS_SYNC_KEYS = (
    "snapshot",
    "bridge_ready",
    "mc_enabled",
    "mc_configured",
    "paused",
    "connected",
    "activity",
    "phase",
)


def _fmt_num(val: int | float) -> str:
    fval = float(val)
    return str(int(fval)) if fval.is_integer() else str(round(fval, 1))


def _normalize_pos(pos: Any) -> tuple[float, float, float] | str | None:
    if pos is None:
        return None
    if isinstance(pos, dict) and all(k in pos for k in ("x", "y", "z")):
        try:
            x, y, z = float(pos["x"]), float(pos["y"]), float(pos["z"])
            if math.isfinite(x) and math.isfinite(y) and math.isfinite(z):
                return (x, y, z)
        except (TypeError, ValueError):
            pass
        return _sanitize_text(str(pos), max_tokens=16) or None
    if isinstance(pos, (list, tuple)) and len(pos) == 3:
        try:
            x, y, z = float(pos[0]), float(pos[1]), float(pos[2])
            if math.isfinite(x) and math.isfinite(y) and math.isfinite(z):
                return (x, y, z)
        except (TypeError, ValueError):
            pass
        return _sanitize_text(str(pos), max_tokens=16) or None
    if isinstance(pos, str):
        return _sanitize_text(pos, max_tokens=16) or None
    return _sanitize_text(str(pos), max_tokens=16) or None


class MinecraftSnapshot(BaseModel):
    """Sanitized snapshot of the remote CN Mineflayer agent."""

    model_config = ConfigDict(extra="forbid")

    connected: bool = False
    dimension: str = ""
    pos: tuple[float, float, float] | str | None = None
    health: float | None = Field(default=None, ge=0)
    food: float | None = Field(default=None, ge=0)
    active_task: str = ""
    activity: str = ""
    nearby_entities: list[str] = Field(default_factory=list)
    nearby_players: tuple[str, ...] = ()
    recent_chat_or_sign: str = ""
    updated_at: float = 0.0


class NoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class ConnectArgs(NoArgs):
    intent: str = Field(default="", max_length=80)


class BuildArgs(NoArgs):
    topic: str = Field(default="", max_length=80)


class BlockArgs(NoArgs):
    block_type: str = Field(default="", max_length=48, pattern=r"^[a-z0-9_:]*$")


class PositionArgs(NoArgs):
    x: float = Field(ge=-30000000, le=30000000)
    y: float = Field(ge=-2048, le=2048)
    z: float = Field(ge=-30000000, le=30000000)


class FollowArgs(NoArgs):
    target_player: str = Field(pattern=r"^[A-Za-z0-9_]{1,16}$")


class ChatArgs(NoArgs):
    message: str = Field(min_length=1, max_length=120)

    @field_validator("message")
    @classmethod
    def _validate_message(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("empty_chat_message")
        if cleaned.startswith("/"):
            raise ValueError("mc_command_forbidden")
        return cleaned


class DisconnectArgs(NoArgs):
    sleep_server: bool = False


_CONNECT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "intent": {"type": "string", "maxLength": 80, "default": ""},
    },
    "required": [],
    "additionalProperties": False,
}
_BUILD_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "topic": {"type": "string", "maxLength": 80, "default": ""},
    },
    "required": [],
    "additionalProperties": False,
}
_BLOCK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "block_type": {"type": "string", "maxLength": 48, "default": ""},
    },
    "required": [],
    "additionalProperties": False,
}
_POSITION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "x": {"type": "number", "minimum": -30000000, "maximum": 30000000},
        "y": {"type": "number", "minimum": -2048, "maximum": 2048},
        "z": {"type": "number", "minimum": -30000000, "maximum": 30000000},
    },
    "required": ["x", "y", "z"],
    "additionalProperties": False,
}
_FOLLOW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "target_player": {"type": "string", "minLength": 1, "maxLength": 16},
    },
    "required": ["target_player"],
    "additionalProperties": False,
}
_CHAT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "message": {"type": "string", "minLength": 1, "maxLength": 120},
    },
    "required": ["message"],
    "additionalProperties": False,
}
_DISCONNECT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "sleep_server": {"type": "boolean", "default": False},
    },
    "required": [],
    "additionalProperties": False,
}

_ACTION_SPECS: dict[str, tuple[str, dict[str, Any], type[BaseModel]]] = {
    "connect": (
        "登录进入 Minecraft 服务器上线游玩（如服务端休眠会自动唤醒）",
        _CONNECT_SCHEMA,
        ConnectArgs,
    ),
    "disconnect": ("主动退出 Minecraft 服务器下线待机休息", _DISCONNECT_SCHEMA, DisconnectArgs),
    "build_plan": ("在记事本里记下接下来想在 Minecraft 里搭的建筑点子", _BUILD_SCHEMA, BuildArgs),
    "explore": ("在 Minecraft 世界里四处探索地形与新发现", {}, NoArgs),
    "mine": ("在游戏里挖掘指定矿石或方块", _BLOCK_SCHEMA, BlockArgs),
    "gather": ("在游戏里收集指定资源方块", _BLOCK_SCHEMA, BlockArgs),
    "goto": ("在游戏里前往指定坐标位置", _POSITION_SCHEMA, PositionArgs),
    "follow": ("在游戏里跟随指定玩家一起行动", _FOLLOW_SCHEMA, FollowArgs),
    "chat": ("在游戏公屏里发一句短消息和在线玩家搭话（不可发/指令）", _CHAT_SCHEMA, ChatArgs),
    "observe": ("在游戏里环顾观察周围环境与生物", {}, NoArgs),
    "look_signs": ("在游戏里查看附近的告示牌文字", {}, NoArgs),
    "stop": ("停下当前正在执行的 Minecraft 动作", {}, NoArgs),
    "idle": ("在已连线的游戏世界里原地待机歇会儿", {}, NoArgs),
    "status": ("读取桥接与游戏实时状态", {}, NoArgs),
    "pause": ("暂停游戏自主动作", {}, NoArgs),
    "resume": ("恢复游戏自主动作", {}, NoArgs),
}
_OWNER_CHAT_VERBS = frozenset({"connect", "disconnect"})
_MACRO_PLANNER_VERBS = frozenset({"build_plan"})
_MICRO_GAME_VERBS = frozenset(
    {
        "explore",
        "mine",
        "gather",
        "goto",
        "follow",
        "chat",
        "observe",
        "look_signs",
        "stop",
        "idle",
    }
)
_ADMIN_ONLY_VERBS = frozenset({"status", "pause", "resume"})
_EFFORTFUL_VERBS = frozenset({"explore", "goto", "mine", "gather", "follow"})
_OWNER_CHAT_ORIGINS = ["planner", "owner_chat", "rule", "timer", "legacy", "admin"]
_MACRO_PLANNER_ORIGINS = ["planner", "rule", "timer", "legacy", "admin"]
_MICRO_GAME_ORIGINS = ["rule", "legacy", "admin"]


class MinecraftPlugin:
    """Out-of-process Minecraft capability plugin wrapping HakimiPluginClient."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        mc_bridge_url: str = "",
        http: httpx.AsyncClient | None = None,
        clock: Any = None,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        enable_brain: bool = False,
        brain_config: McBrainConfig | None = None,
    ) -> None:
        self.mc_bridge_url = mc_bridge_url.strip()
        self._owns_http = http is None
        self.http = http if http is not None else httpx.AsyncClient(follow_redirects=False)
        self.clock = clock if clock is not None else time.time

        self.enabled: bool = True
        self.node_online: bool = bool(self.mc_bridge_url)
        self.snapshot = MinecraftSnapshot()
        self._has_snapshot = False
        self.bridge_ready: bool = False
        self.mc_enabled: bool = False
        self.mc_configured: bool = False
        self.paused: bool = False
        self.enable_brain: bool = bool(enable_brain)
        self.session_intent: str = ""
        self._sync_lock = asyncio.Lock()
        self._snapshot_seq: int = 0
        self.brain = McPlayBrain(self, config=brain_config)

        if self.enable_brain:
            scopes: tuple[str, ...] = ("actions", "sensors", "node_bridge", "thinking_scenes")
            thinking_scenes: tuple[ThinkingSceneSpec, ...] = (
                ThinkingSceneSpec(
                    key="mc.play",
                    label="MC 实时游玩",
                    default="low",
                    levels=("low", "medium", "high"),
                    output_tokens=512,
                ),
            )
        else:
            scopes = ("actions", "sensors", "node_bridge")
            thinking_scenes = ()

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id="minecraft",
            token=token,
            display_name="Minecraft",
            description="游戏连接、活动目标与建造计划",
            scopes=scopes,
            node_region="CN",
            admin_panel="minecraft",
            attention_level="HIGH",
            event_subscriptions=("bili_video_watched", "bili_mc_inspiration"),
            reconnect_min=reconnect_min,
            reconnect_max=reconnect_max,
        )
        action_specs = tuple(
            self._build_action_spec(verb, desc, schema)
            for verb, (desc, schema, _) in _ACTION_SPECS.items()
        )
        self.client = HakimiPluginClient(
            config,
            actions=action_specs,
            thinking_scenes=thinking_scenes,
            initial_status=self.status_dict(),
            connect_factory=connect_factory,
        )
        self._bind_handlers()

    def _build_action_spec(self, verb: str, description: str, schema: dict[str, Any]) -> ActionSpec:
        admin_only = verb in _ADMIN_ONLY_VERBS
        if admin_only:
            origins = ["admin"]
        elif verb in _OWNER_CHAT_VERBS:
            origins = list(_OWNER_CHAT_ORIGINS)
        elif verb in _MACRO_PLANNER_VERBS:
            origins = list(_MACRO_PLANNER_ORIGINS)
        elif verb in _MICRO_GAME_VERBS:
            origins = list(_MICRO_GAME_ORIGINS)
        else:
            raise ValueError(f"unclassified_mc_verb:{verb}")

        async def _run(params: dict[str, Any]) -> dict[str, Any]:
            return await self._handle_action(verb, params)

        return ActionSpec(
            name=f"mc.{verb}",
            description=description,
            arguments=schema,
            origins=origins,
            effortful=verb in _EFFORTFUL_VERBS,
            interrupts_focus=False,
            window="any",
            aliases=[] if admin_only else [["minecraft", verb, False]],
            handler=_run,
        )

    def _bind_handlers(self) -> None:
        self.client.on_connect(self._on_connect)
        self.client.sensor(self._sensor_summary)
        self.client.rpc("mc.goal")(self._rpc_goal)
        self.client.on_node_event(self._on_node_event)
        self.client.on_node_state(self._on_node_state)
        self.client.on_switch(self._on_switch)
        self.client.on_world_event(self._on_world_event)

    async def close(self) -> None:
        await self.brain.stop()
        await self.client.stop()
        if self._owns_http:
            await self.http.aclose()

    def _update_bridge_flags(self, raw: Any) -> None:
        if not isinstance(raw, dict):
            return
        nested = raw.get("snapshot")
        if isinstance(nested, dict):
            for key in _BRIDGE_FLAG_KEYS:
                if key in nested:
                    setattr(self, key, bool(nested[key]))
        for key in _BRIDGE_FLAG_KEYS:
            if key in raw:
                setattr(self, key, bool(raw[key]))

    def _sanitize_snapshot(self, raw: dict[str, Any] | MinecraftSnapshot) -> MinecraftSnapshot:
        raw_dict = raw.model_dump() if isinstance(raw, MinecraftSnapshot) else dict(raw)
        if isinstance(raw_dict.get("snapshot"), dict) and not any(
            k in raw_dict for k in _SNAPSHOT_FIELD_KEYS
        ):
            source = dict(raw_dict["snapshot"])
        else:
            source = raw_dict
        data = {k: v for k, v in source.items() if k not in _NON_SNAPSHOT_KEYS}
        connected = bool(data.get("connected", False))
        dimension = _sanitize_text(str(data.get("dimension") or ""), max_tokens=12)
        pos = _normalize_pos(data.get("pos"))

        raw_health = data.get("health")
        health = (
            float(raw_health)
            if isinstance(raw_health, (int, float))
            and not isinstance(raw_health, bool)
            and math.isfinite(float(raw_health))
            and float(raw_health) >= 0
            else None
        )
        raw_food = data.get("food")
        food = (
            float(raw_food)
            if isinstance(raw_food, (int, float))
            and not isinstance(raw_food, bool)
            and math.isfinite(float(raw_food))
            and float(raw_food) >= 0
            else None
        )
        activity = _sanitize_text(str(data.get("activity") or ""), max_tokens=20)

        raw_entities = data.get("nearby_entities") or []
        entities: list[str] = []
        if isinstance(raw_entities, (list, tuple)):
            for item in raw_entities[:5]:
                clean_item = _sanitize_text(str(item), max_tokens=10)
                if clean_item:
                    entities.append(clean_item)
        elif isinstance(raw_entities, str) and raw_entities.strip():
            clean_item = _sanitize_text(raw_entities, max_tokens=12)
            if clean_item:
                entities.append(clean_item)

        raw_chat = data.get("recent_chat_or_sign")
        if isinstance(raw_chat, (list, tuple)):
            joined = " / ".join(str(x) for x in raw_chat if x is not None)
        else:
            joined = str(raw_chat or "")
        recent_chat_or_sign = _sanitize_text(joined, max_tokens=20)

        raw_ts = data.get("updated_at")
        updated_at = (
            float(raw_ts)
            if isinstance(raw_ts, (int, float))
            and not isinstance(raw_ts, bool)
            and math.isfinite(float(raw_ts))
            else float(self.clock())
        )
        raw_players = data.get("nearby_players") or []
        players: list[str] = []
        if isinstance(raw_players, (list, tuple)):
            for item in raw_players[:6]:
                clean_player = _sanitize_text(str(item), max_tokens=10)
                if clean_player and clean_player not in players:
                    players.append(clean_player)
        elif isinstance(raw_players, str) and raw_players.strip():
            clean_player = _sanitize_text(raw_players, max_tokens=10)
            if clean_player:
                players.append(clean_player)

        return MinecraftSnapshot(
            connected=connected,
            dimension=dimension,
            pos=pos,
            health=health,
            food=food,
            activity=activity,
            active_task=str(data.get("active_task") or "")[:16],
            nearby_entities=entities,
            nearby_players=tuple(players),
            recent_chat_or_sign=recent_chat_or_sign,
            updated_at=updated_at,
        )

    def apply_snapshot(self, raw: dict[str, Any] | MinecraftSnapshot) -> MinecraftSnapshot:
        """Sanitize and store a remote Mineflayer snapshot synchronously."""
        self._update_bridge_flags(raw)
        sanitized = self._sanitize_snapshot(raw)
        self.snapshot = sanitized
        self._has_snapshot = True
        self._snapshot_seq += 1
        return sanitized

    def apply_status(self, raw: dict[str, Any]) -> MinecraftSnapshot:
        """Apply bridge status flags and optional snapshot without altering _has_snapshot."""
        if not isinstance(raw, dict):
            return self.snapshot
        self._update_bridge_flags(raw)
        nested = raw.get("snapshot")
        if isinstance(nested, dict):
            sanitized = self._sanitize_snapshot(nested)
            self.snapshot = sanitized
            self._has_snapshot = True
            self._snapshot_seq += 1
            return sanitized
        stripped = {k: v for k, v in raw.items() if k not in _NON_SNAPSHOT_KEYS}
        snapshot_fields = {k: v for k, v in stripped.items() if k in _SNAPSHOT_FIELD_KEYS}
        if snapshot_fields:
            merged = {**self.snapshot.model_dump(), **snapshot_fields}
            sanitized = self._sanitize_snapshot(merged)
            self.snapshot = sanitized
            self._snapshot_seq += 1
            if sanitized.connected:
                self._has_snapshot = True
        return self.snapshot

    async def _emit_milestone(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        importance: Literal["low", "normal", "high"] = "normal",
        business_key: str | None = None,
    ) -> None:
        if not self.client.connected:
            return
        await self.client.emit_world_event(
            event_type,
            payload if payload is not None else {},
            importance=importance,
            business_key=business_key,
        )

    async def update_snapshot(
        self,
        raw: dict[str, Any] | MinecraftSnapshot,
        *,
        milestone: dict[str, Any] | None = None,
    ) -> MinecraftSnapshot:
        """Sanitize snapshot, update local state, and emit transition milestones."""
        prev = self.snapshot
        was_initialized = self._has_snapshot
        curr = self.apply_snapshot(raw)
        if curr.connected:
            self.brain.note_session_connected()
            self.brain.note_players(curr.nearby_players)
        if was_initialized and prev.connected and not curr.connected:
            self.session_intent = ""

        explicit = milestone or (raw.get("milestone") if isinstance(raw, dict) else None)
        if isinstance(explicit, dict) and explicit.get("event_type"):
            imp_raw = explicit.get("importance", "normal")
            imp: Literal["low", "normal", "high"] = (
                imp_raw if imp_raw in ("low", "normal", "high") else "normal"
            )
            exp_type = _sanitize_text(str(explicit["event_type"]), max_tokens=16)
            exp_payload = dict(explicit.get("payload") or {})
            await self._emit_milestone(
                exp_type or str(explicit["event_type"]),
                exp_payload,
                importance=imp,
                business_key=explicit.get("event_id"),
            )
            if exp_type in _BRAIN_MILESTONE_EVENTS:
                self.brain.on_milestone(exp_type, exp_payload)
        elif not prev.connected and curr.connected:
            await self._emit_milestone(
                "mc_connected",
                {"dimension": curr.dimension, "activity": curr.activity},
                importance="normal",
            )
        elif was_initialized and prev.connected and not curr.connected:
            await self._emit_milestone(
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
            await self._emit_milestone(
                "dimension_changed",
                {"from": prev.dimension, "to": curr.dimension},
                importance="normal",
            )

        if (
            curr.connected
            and curr.health is not None
            and prev.health is not None
            and ((prev.health > 0 and curr.health <= 0) or (prev.health > 4 and curr.health <= 4))
        ):
            alert_payload = {"health": curr.health, "dimension": curr.dimension}
            await self._emit_milestone(
                "mc_health_alert",
                alert_payload,
                importance="high",
            )
            self.brain.on_milestone("mc_health_alert", alert_payload)
        if was_initialized and prev.connected and not curr.connected:
            await self.brain.flush_session_recap(reason="disconnected")
        self.brain.on_snapshot(curr)
        return curr

    def status_dict(self) -> dict[str, Any]:
        """Return a strictly sanitized status dictionary matching MinecraftSlotModule."""
        snap = self.snapshot
        if isinstance(snap.pos, tuple):
            pos_val: list[float] | str | None = [float(v) for v in snap.pos]
        elif isinstance(snap.pos, str):
            pos_val = _sanitize_text(snap.pos, max_tokens=16) or None
        else:
            pos_val = None
        return {
            "bridge_ready": bool(self.bridge_ready),
            "mc_enabled": bool(self.mc_enabled),
            "mc_configured": bool(self.mc_configured),
            "connected": bool(snap.connected),
            "paused": bool(self.paused),
            "dimension": _sanitize_text(snap.dimension, max_tokens=12),
            "pos": pos_val,
            "health": snap.health,
            "food": snap.food,
            "activity": _sanitize_text(snap.activity, max_tokens=20),
            "nearby_entities": [
                clean
                for item in snap.nearby_entities[:5]
                if (clean := _sanitize_text(item, max_tokens=10))
            ],
            "nearby_players": [
                clean
                for item in snap.nearby_players[:6]
                if (clean := _sanitize_text(item, max_tokens=10))
            ],
            "recent_chat_or_sign": _sanitize_text(snap.recent_chat_or_sign, max_tokens=20),
            "updated_at": float(snap.updated_at),
            **self.brain.telemetry(),
        }

    def format_sensor_summary(self) -> str:
        """Format sensor text identically to MinecraftSlotModule._raw_sensor_summary()."""
        snap = self.snapshot
        if not snap.connected:
            if self.bridge_ready:
                if not snap.activity or snap.activity == "待机":
                    return "MC躯干就绪(待机)"
                return f"MC待机 {snap.activity}".strip()
            if snap.activity:
                return "MC离线" if snap.activity == "离线" else f"MC离线 {snap.activity}".strip()
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
        return _sanitize_text(" ".join(parts), max_tokens=90)

    async def _sensor_summary(self, _params: dict[str, Any] | None = None) -> str:
        return self.format_sensor_summary()

    async def _push_status_and_activity(self) -> None:
        if not self.client.connected:
            return
        await self.client.push_status(self.status_dict())
        snap = self.snapshot
        if snap.connected and not self.paused and self.bridge_ready:
            task = snap.active_task or "online"
            disconnected = False
        else:
            task = ""
            disconnected = not snap.connected
        await self.client.report_activity(
            task, updated_at=snap.updated_at, disconnected=disconnected
        )

    async def _apply_status_data(
        self, data: dict[str, Any], *, allow_snapshot: bool = True
    ) -> None:
        if allow_snapshot:
            prev_connected = self.snapshot.connected
            has_nested = isinstance(data.get("snapshot"), dict)
            if has_nested:
                snap = dict(data["snapshot"])
                for flag in _BRIDGE_FLAG_KEYS:
                    if flag in data:
                        snap[flag] = data[flag]
                await self.update_snapshot(snap)
            self.apply_status(data)
            if not has_nested:
                if self.snapshot.connected:
                    self.brain.note_session_connected()
                    self.brain.note_players(self.snapshot.nearby_players)
                elif prev_connected and not self.snapshot.connected:
                    await self.brain.flush_session_recap(reason="disconnected")
                    self.brain.on_snapshot(self.snapshot)
        else:
            self._update_bridge_flags(data)
        self.brain.on_state_change()
        await self._push_status_and_activity()

    async def _call_bridge(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        if self.mc_bridge_url:
            endpoint = self.mc_bridge_url.rstrip("/")
            if not endpoint.endswith("/rpc"):
                endpoint = f"{endpoint}/rpc"
            try:
                resp = await self.http.post(
                    endpoint,
                    json={"action": action, "params": params},
                    timeout=15.0,
                    follow_redirects=False,
                )
                resp.raise_for_status()
                data = resp.json()
            except httpx.TimeoutException as exc:
                raise PluginRpcError("mc_bridge_timeout", "mc bridge timed out") from exc
            except httpx.HTTPError as exc:
                raise PluginRpcError("mc_bridge_disconnected", "mc bridge unavailable") from exc
            except ValueError as exc:
                raise PluginRpcError("mc_bridge_invalid_response", "invalid bridge json") from exc
            if not isinstance(data, dict):
                raise PluginRpcError("mc_bridge_invalid_response", "invalid bridge shape")
            return data

        data = await self.client.node_call(action, params)
        self.node_online = True
        return data

    async def sync_status(self) -> None:
        """Best-effort sync of mc.status from CN bridge or local HTTP bridge."""
        if not self.enabled:
            return
        seq = self._snapshot_seq
        async with self._sync_lock:
            if not self.enabled:
                return
            try:
                data = await self._call_bridge("mc.status", {})
            except PluginRpcError as exc:
                if exc.code in {"node_disconnected", "node_not_dispatched"}:
                    self.node_online = False
                return
            if isinstance(data, dict) and self.enabled:
                bare_stale = (
                    self._has_snapshot and "snapshot" not in data and "updated_at" not in data
                )
                allow_snapshot = (self._snapshot_seq == seq) and not bare_stale
                await self._apply_status_data(data, allow_snapshot=allow_snapshot)

    async def _on_connect(self, welcome: dict[str, Any]) -> None:
        if "enabled" in welcome:
            self.enabled = bool(welcome["enabled"])
        if self.enable_brain:
            self.brain.start()
        await self.client.push_status(self.status_dict())
        await self.sync_status()

    async def _on_node_event(self, frame: dict[str, Any]) -> None:
        if not self.enabled or not isinstance(frame, dict):
            return
        self.node_online = True
        event_name = frame.get("event")
        payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
        if event_name == "mc.snapshot":
            await self.update_snapshot(payload)
            await self._push_status_and_activity()
        elif event_name == "mc.milestone":
            raw_type = payload.get("event_type") or payload.get("type") or "mc_milestone"
            clean_type = _sanitize_text(str(raw_type), max_tokens=16) or "mc_milestone"
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
            business_key = payload.get("event_id") or frame.get("id")
            await self._emit_milestone(
                clean_type,
                event_data,
                importance=importance,
                business_key=str(business_key) if business_key else None,
            )
            if clean_type in _BRAIN_MILESTONE_EVENTS:
                self.brain.on_milestone(clean_type, event_data)

    async def _on_node_state(self, connected: bool) -> None:
        if connected:
            self.node_online = True
            if self.enabled:
                await self.sync_status()
            else:
                await self._stop_remote_goal_best_effort()
            return
        self.node_online = False
        self.bridge_ready = False
        if self.snapshot.connected:
            await self.update_snapshot(
                {"connected": False, "bridge_ready": False, "activity": "节点连接断开"}
            )
        self.brain.on_state_change()
        if self.client.connected:
            await self.client.push_status(self.status_dict())
            await self.client.report_activity("", disconnected=True)

    async def _stop_remote_goal_best_effort(self) -> None:
        try:
            res = await self._call_bridge("mc.dispatch_goal", {"action_type": "stop"})
            if not isinstance(res, dict) or not res.get("ok"):
                log.warning("event=mc_module_stop_not_confirmed")
        except PluginRpcError:
            log.warning("event=mc_module_stop_not_confirmed")

    async def _on_switch(self, enabled: bool) -> None:
        self.enabled = bool(enabled)
        self.brain.on_state_change()
        if not self.enabled:
            if self.client.connected:
                await self.client.report_activity("", disconnected=True)
            await self._stop_remote_goal_best_effort()
        else:
            await self.sync_status()

    async def _on_world_event(self, event: dict[str, Any]) -> None:
        if not self.enabled or not isinstance(event, dict):
            return
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if payload.get("private_first"):
            return
        source_module = str(event.get("source_module") or "")
        event_type = str(event.get("event_type") or "")
        if payload.get("activity_kind") != "video_learning" and (
            source_module != "bilibili_surf"
            or event_type not in {"bili_video_watched", "bili_mc_inspiration"}
        ):
            return
        topic = str(payload.get("title", ""))
        summary = str(payload.get("summary", ""))
        if event_type == "bili_mc_inspiration" or _MC_THEME.search(f"{topic} {summary}"):
            await self.client.enqueue_reaction(
                "mc.build_plan",
                "刚刷到对胃口的 MC 视频，顺手记下想搭的建造灵感",
                ttl=7200,
                arguments={"topic": topic[:80]},
                legacy_module="minecraft",
                legacy_verb="build_plan",
                source_event_id=str(event.get("id") or ""),
            )

    def _goal_result(
        self,
        *,
        ok: bool,
        summary: str | None = None,
        artifacts: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": bool(ok),
            "module_id": "minecraft",
            "summary": _sanitize_text(summary or "", max_tokens=90)
            if summary
            else self.format_sensor_summary(),
            "artifacts": dict(artifacts or {}),
            "reason": _sanitize_text(reason, max_tokens=20) if reason is not None else None,
        }

    async def dispatch_goal(self, ticket: dict[str, Any]) -> dict[str, Any]:
        """Validate and dispatch a Minecraft goal or control command, returning GoalResult dict."""
        if not self.enabled:
            return self._goal_result(ok=False, reason="module_disabled")
        verb = str(ticket.get("action_type") or "").strip()
        if verb not in _ACTION_SPECS:
            return self._goal_result(ok=False, reason="unknown_action")

        _, _, args_cls = _ACTION_SPECS[verb]
        raw_args = {
            k: ticket[k] for k in args_cls.model_fields if k in ticket and ticket[k] is not None
        }
        try:
            validated = args_cls.model_validate(raw_args)
        except ValidationError:
            return self._goal_result(
                ok=False,
                reason="invalid_action_arguments",
                artifacts={"not_dispatched": True},
            )

        clean_ticket = {**ticket, **validated.model_dump()}
        was_node_online = self.node_online
        try:
            if verb in _ADMIN_ONLY_VERBS:
                data = await self._call_bridge(f"mc.{verb}", {})
            elif verb == "connect":
                data = await self._call_bridge("mc.dispatch_goal", {"action_type": "connect"})
            else:
                params = {
                    key: clean_ticket[key]
                    for key in _MC_ALLOWED_DISPATCH_FIELDS
                    if clean_ticket.get(key) is not None
                }
                for axis in ("x", "y", "z"):
                    val = params.pop(axis, None)
                    if type(val) in (int, float) and math.isfinite(val):
                        params[axis] = float(val)
                data = await self._call_bridge("mc.dispatch_goal", params)
        except PluginRpcError as exc:
            code = exc.code
            if code in {"node_disconnected", "node_not_dispatched"}:
                self.node_online = False
                if code == "node_not_dispatched" or not was_node_online:
                    return self._goal_result(
                        ok=False,
                        reason=code,
                        artifacts={"not_dispatched": True},
                    )
            return self._goal_result(ok=False, reason=code)

        if not isinstance(data, dict):
            return self._goal_result(ok=False, reason="mc_invalid_receipt")

        if "ok" in data:
            ok = data["ok"] is True and data.get("accepted", True) is True
        elif verb == "status":
            ok = data.get("bridge_ready") is True
        else:
            ok = data.get("accepted") is True

        if verb == "connect" and ok:
            self.session_intent = _sanitize_text(validated.intent, max_tokens=40)[:80]
            self.brain.note_session_connected()
        elif verb == "chat" and ok and isinstance(validated, ChatArgs):
            self.brain.record_direct_chat(validated.message)
        elif verb == "disconnect":
            await self.brain.flush_session_recap(reason="disconnect")
            self.session_intent = ""
            self.brain.on_disconnect()

        if any(k in data for k in _STATUS_SYNC_KEYS):
            await self._apply_status_data(data)
        elif verb in ("connect", "disconnect") and self.client.connected:
            await self.client.push_status(self.status_dict())

        if verb in ("pause", "resume"):
            self.brain.on_state_change()

        artifacts: dict[str, Any] = {k: getattr(self, k) for k in _BRIDGE_FLAG_KEYS}
        artifacts["connected"] = self.snapshot.connected
        if "accepted" in data:
            artifacts["accepted"] = data["accepted"] is True
        if "phase" in data:
            artifacts["phase"] = _sanitize_text(str(data["phase"]), max_tokens=12)

        return self._goal_result(
            ok=ok,
            summary=data.get("summary") or self.format_sensor_summary(),
            artifacts=artifacts,
            reason=data.get("reason") or (None if ok else "mc_invalid_receipt"),
        )

    async def _handle_action(self, verb: str, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        args = dict(raw_args) if isinstance(raw_args, dict) else {}
        evidence = params.get("evidence")
        if (
            verb == "build_plan"
            and not args.get("topic")
            and isinstance(evidence, dict)
            and isinstance(evidence.get("payload"), dict)
        ):
            args["topic"] = _sanitize_text(str(evidence["payload"].get("title", "")), max_tokens=20)

        res = await self.dispatch_goal({"action_type": verb, **args})
        reason = res.get("reason")
        artifacts = res.get("artifacts") if isinstance(res.get("artifacts"), dict) else {}
        if res.get("ok"):
            status = "done"
            out_reason = "plan_registered" if verb == "build_plan" else "mc_acknowledged"
        elif artifacts.get("not_dispatched") or reason == "node_not_dispatched":
            status = "not_dispatched"
            out_reason = str(reason or "node_not_dispatched")
        elif reason in _UNCERTAIN_REASONS:
            status = "uncertain"
            out_reason = str(reason)
        else:
            status = "gated_skip"
            out_reason = str(reason or "mc_rejected")
        return {
            "status": status,
            "reason": out_reason,
            "details": {"result": res},
        }

    async def _rpc_goal(self, params: dict[str, Any]) -> dict[str, Any]:
        return await self.dispatch_goal(params)


def build_minecraft_plugin_from_env(**overrides: Any) -> MinecraftPlugin:
    """Create a MinecraftPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_MINECRAFT_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_MINECRAFT_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    raw_brain = (os.getenv("MC_BRAIN_ENABLED") or "true").strip().lower()
    enable_brain = raw_brain not in {"0", "false", "no", "off"}
    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "mc_bridge_url": os.getenv("MC_BRIDGE_URL") or "",
        "enable_brain": enable_brain,
    }
    kwargs.update(overrides)
    return MinecraftPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the Minecraft remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    plugin = build_minecraft_plugin_from_env()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        asyncio.create_task(plugin.client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await plugin.client.run_forever()
    finally:
        await plugin.close()


if __name__ == "__main__":
    asyncio.run(main())
