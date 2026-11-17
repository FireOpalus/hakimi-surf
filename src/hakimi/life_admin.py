"""Authenticated control plane for the v2.0 WorldState and dual-layer LifeEngine."""

from __future__ import annotations

import inspect
import json
import re
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .admin_routes import body, service
from .proactive_policy import minute_of_day

router = APIRouter(prefix="/admin/api/life")

INTENT_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
LIFE_INTENT_LIMIT = 40
LIFE_EVENT_LIMIT = 30


class LifeConfigMirror(BaseModel):
    """Strict mirror of LifeEngineConfig used for standalone validation and defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = True
    active_hours: str = "09:00-01:00"
    planner_min_minutes: int = Field(default=120, ge=30, le=720)
    planner_max_minutes: int = Field(default=240, ge=60, le=720)
    sleep_min_minutes: int = Field(default=360, ge=120, le=960)
    sleep_max_minutes: int = Field(default=480, ge=180, le=960)
    group_cooldown_seconds: int = Field(default=1500, ge=300, le=86400)
    group_active_window_seconds: int = Field(default=1800, ge=60, le=7200)
    group_min_recent_messages: int = Field(default=2, ge=1, le=50)
    desktop_active_suppress_seconds: int = Field(default=7200, ge=300, le=86400)
    max_proactive_per_day: int = Field(default=6, ge=0, le=30)

    @field_validator("active_hours")
    @classmethod
    def _valid_active_hours(cls, value: str) -> str:
        parts = value.split("-")
        if len(parts) != 2 or minute_of_day(parts[0]) == minute_of_day(parts[1]):
            raise ValueError("Active hours must be two different HH:MM times")
        return value

    @model_validator(mode="after")
    def _valid_ranges(self) -> LifeConfigMirror:
        if self.planner_min_minutes > self.planner_max_minutes:
            raise ValueError("planner_min_minutes must be <= planner_max_minutes")
        if self.sleep_min_minutes > self.sleep_max_minutes:
            raise ValueError("sleep_min_minutes must be <= sleep_max_minutes")
        return self


class WorldStateAdminUpdate(BaseModel):
    """Strict manual world-state / Attention Lock update from the admin console."""

    model_config = ConfigDict(extra="forbid", strict=True)

    current_module: str | None = Field(default=None, min_length=1, max_length=64)
    focus_level: Literal["LOW", "MEDIUM", "HIGH"] | None = None
    mood: str | None = Field(default=None, min_length=1, max_length=64)
    energy: int | None = Field(default=None, ge=0, le=100)
    boredom: int | None = Field(default=None, ge=0, le=100)

    @field_validator("current_module", "mood")
    @classmethod
    def _non_blank(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @model_validator(mode="after")
    def _require_any_field(self) -> WorldStateAdminUpdate:
        if all(
            v is None
            for v in (
                self.current_module,
                self.focus_level,
                self.mood,
                self.energy,
                self.boredom,
            )
        ):
            raise ValueError("at least one field must be provided")
        return self


def _life_config_model() -> type[BaseModel]:
    try:
        from .life_engine import LifeEngineConfig
    except ImportError:
        return LifeConfigMirror
    return LifeEngineConfig


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _parse_json_value(raw: Any, default: Any = None) -> Any:
    fallback = {} if default is None else default
    if raw is None or raw == "":
        return fallback
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except ValueError:
        return fallback


async def _table_exists(store: Any, name: str) -> bool:
    row = await store.one(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return row is not None


async def _collect_modules_status(svc: Any) -> list[dict[str, Any]]:
    registry = getattr(svc, "modules", None)
    if registry is None:
        return []
    items: list[dict[str, Any]] = []
    for mod in registry.all():
        try:
            summary = await mod.get_sensor_summary()
        except Exception:
            summary = ""
        items.append(
            {
                "module_id": mod.module_id,
                "node_region": mod.node_region,
                "attention_level": mod.attention_level,
                "proactive_paused": bool(mod.proactive_paused),
                "sensor_summary": summary,
            }
        )
    return items


async def _read_intents(store: Any) -> list[dict[str, Any]]:
    async with store.lock:
        if not await _table_exists(store, "scheduled_intents"):
            return []
        rows = await store.all(
            "SELECT id, created_at, execute_after, expire_at, target_module, target_scope, "
            "action_type, mood, gate_condition_json, intent_note_json, status, updated_at "
            "FROM scheduled_intents ORDER BY created_at DESC LIMIT ?",
            (LIFE_INTENT_LIMIT,),
        )
    return [
        {
            "id": row["id"],
            "created_at": float(row["created_at"]),
            "execute_after": float(row["execute_after"]),
            "expire_at": float(row["expire_at"]),
            "target_module": row["target_module"],
            "target_scope": row["target_scope"],
            "action_type": row["action_type"],
            "mood": row["mood"],
            "gate_condition": _parse_json_value(row["gate_condition_json"], {}),
            "intent_note": _parse_json_value(row["intent_note_json"], {}),
            "status": row["status"],
            "updated_at": float(row["updated_at"]),
        }
        for row in rows
    ]


async def _read_events(store: Any) -> list[dict[str, Any]]:
    async with store.lock:
        if not await _table_exists(store, "global_events"):
            return []
        rows = await store.all(
            "SELECT id, created_at, source_module, event_type, importance, consumed, payload_json "
            "FROM global_events ORDER BY created_at DESC LIMIT ?",
            (LIFE_EVENT_LIMIT,),
        )
    return [
        {
            "id": row["id"],
            "created_at": float(row["created_at"]),
            "source_module": row["source_module"],
            "event_type": row["event_type"],
            "importance": row["importance"],
            "consumed": bool(row["consumed"]),
            "payload": _parse_json_value(row["payload_json"], {}),
        }
        for row in rows
    ]


async def _build_life_status(svc: Any) -> dict[str, Any]:
    engine = getattr(svc, "life_engine", None)
    engine_meta: dict[str, Any] = {}
    if engine is not None and hasattr(engine, "status"):
        raw_status = await _maybe_await(engine.status())
        if isinstance(raw_status, dict):
            config = dict(raw_status.get("config") or LifeConfigMirror().model_dump(mode="json"))
            source = str(raw_status.get("source") or "env")
            engine_meta = {k: v for k, v in raw_status.items() if k not in {"config", "source"}}
        else:
            config = LifeConfigMirror().model_dump(mode="json")
            source = "env"
    else:
        config = LifeConfigMirror().model_dump(mode="json")
        source = "env"

    ws = getattr(svc, "world_state", None)
    if ws is not None:
        snap = ws.current
        world_state_data = {
            "current_module": snap.current_module,
            "focus_level": snap.focus_level,
            "mood": snap.mood,
            "energy": snap.energy,
            "boredom": snap.boredom,
            "last_proactive_at": snap.last_proactive_at,
            "updated_at": snap.updated_at,
            "proactive_suspended": bool(ws.proactive_suspended()),
            "modules": await _collect_modules_status(svc),
        }
    else:
        world_state_data = {
            "current_module": "idle",
            "focus_level": "LOW",
            "mood": "calm",
            "energy": 80,
            "boredom": 20,
            "last_proactive_at": 0.0,
            "updated_at": 0.0,
            "proactive_suspended": False,
            "modules": await _collect_modules_status(svc),
        }

    proactive_mgr = getattr(svc, "proactive", None)
    onebot_connected = bool(proactive_mgr.connected()) if proactive_mgr is not None else False
    node_transport = getattr(svc, "node_transport", None)
    status_fn = getattr(node_transport, "status", None)
    raw_node_status = status_fn() if callable(status_fn) else {}
    if not isinstance(raw_node_status, dict):
        raw_node_status = {}
    raw_node_conn = getattr(node_transport, "connected", False)
    node_connected = bool(
        raw_node_conn() if callable(raw_node_conn) else raw_node_status.get("online", raw_node_conn)
    )
    node_configured = bool(
        raw_node_status.get("configured", getattr(svc.s, "node_ws_enabled", False))
    )
    bili_delegate_active = bool(
        node_connected and getattr(svc.s, "node_bili_delegate_enabled", True)
    )
    nodes = {
        "onebot_cn_napcat": {
            "connected": onebot_connected,
            "mode": svc.s.onebot_mode,
        },
        "shanghai_node_agent": {
            "configured": node_configured,
            "connected": node_connected,
            "node_id": raw_node_status.get("node_id") or getattr(svc.s, "node_id", "shanghai-cn-1"),
            "region": raw_node_status.get("region", "CN"),
            "source_ip": raw_node_status.get("source_ip"),
            "connected_at": raw_node_status.get("connected_at"),
            "last_heartbeat_at": raw_node_status.get("last_heartbeat_at"),
            "capabilities": list(raw_node_status.get("capabilities") or []),
            "rpc_calls": int(raw_node_status.get("rpc_calls", 0) or 0),
            "rpc_errors": int(raw_node_status.get("rpc_errors", 0) or 0),
            "bili_delegate_active": bili_delegate_active,
            "phase_note": "CN 反向 WSS 躯干专线",
        },
    }

    cloud_memory = getattr(svc, "cloud_memory", None)
    if cloud_memory is not None and hasattr(cloud_memory, "latest_activity_info"):
        desktop_memory = await _maybe_await(cloud_memory.latest_activity_info())
    else:
        desktop_memory = {"enabled": bool(svc.s.enable_antigravity_memory)}

    intents = await _read_intents(svc.store)
    events = await _read_events(svc.store)

    return {
        "config": config,
        "source": source,
        "timezone": svc.s.bot_timezone,
        "engine": engine_meta,
        "world_state": world_state_data,
        "nodes": nodes,
        "desktop_memory": desktop_memory,
        "intents": intents,
        "events": events,
    }


@router.get("")
async def life_status(request: Request):
    return await _build_life_status(service(request))


@router.put("")
async def life_save_config(request: Request):
    from .app import json_object, read_body

    raw = json_object(await read_body(request, request.app.state.settings))
    if not raw:
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    async with request.app.state.admin_config_lock:
        engine = getattr(svc, "life_engine", None)
        if engine is not None and getattr(engine, "config", None) is not None:
            current = engine.config.model_dump(mode="json")
        elif engine is not None and hasattr(engine, "status"):
            st = await _maybe_await(engine.status())
            current = dict(
                (st.get("config") if isinstance(st, dict) else None)
                or LifeConfigMirror().model_dump(mode="json")
            )
        else:
            current = LifeConfigMirror().model_dump(mode="json")
        model_cls = _life_config_model()
        try:
            value = model_cls.model_validate({**current, **raw})
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        if engine is None:
            raise HTTPException(503, "life_engine_unavailable")
        await _maybe_await(engine.set_config(value))
        return await _build_life_status(svc)


@router.delete("")
async def life_reset_config(request: Request):
    svc = service(request)
    async with request.app.state.admin_config_lock:
        engine = getattr(svc, "life_engine", None)
        if engine is None:
            raise HTTPException(503, "life_engine_unavailable")
        await _maybe_await(engine.set_config(None))
        return await _build_life_status(svc)


@router.put("/world")
async def life_update_world(request: Request):
    value = await body(request, WorldStateAdminUpdate)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        ws = getattr(svc, "world_state", None)
        if ws is None:
            raise HTTPException(503, "world_state_unavailable")
        try:
            await ws.update_state(
                current_module=value.current_module,
                focus_level=value.focus_level,
                mood=value.mood,
                energy=value.energy,
                boredom=value.boredom,
            )
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        return await _build_life_status(svc)


@router.post("/plan")
async def life_trigger_plan(request: Request):
    svc = service(request)
    engine = getattr(svc, "life_engine", None)
    if engine is None:
        raise HTTPException(503, "life_engine_unavailable")
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        res = await _maybe_await(engine.trigger_plan_once())
        return res if isinstance(res, dict) else {"status": "ok", "result": res}


@router.post("/gatekeeper")
async def life_trigger_gatekeeper(request: Request):
    svc = service(request)
    engine = getattr(svc, "life_engine", None)
    if engine is None:
        raise HTTPException(503, "life_engine_unavailable")
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        res = await _maybe_await(engine.tick_gatekeeper_once())
        return res if isinstance(res, dict) else {"status": "ok", "result": res}


@router.delete("/intents/{intent_id}")
async def life_cancel_intent(request: Request, intent_id: str):
    if not INTENT_ID_RE.fullmatch(intent_id):
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    async with request.app.state.admin_config_lock:
        ws = getattr(svc, "world_state", None)
        if ws is None:
            raise HTTPException(503, "world_state_unavailable")
        async with svc.store.lock:
            if not await _table_exists(svc.store, "scheduled_intents"):
                raise HTTPException(404, "intent_not_found")
            row = await svc.store.one(
                "SELECT status FROM scheduled_intents WHERE id=?",
                (intent_id,),
            )
        if row is None:
            raise HTTPException(404, "intent_not_found")
        if row["status"] != "pending":
            raise HTTPException(409, "intent_not_pending")
        changed = await ws.mark_intent(intent_id, "gated_skip")
        if not changed:
            raise HTTPException(404, "intent_not_found")
        return await _build_life_status(svc)
