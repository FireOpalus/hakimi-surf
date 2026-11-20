"""Authenticated control plane for the world state, registered life actions and routing modes."""

from __future__ import annotations

import inspect
import json
import re
from collections import Counter
from typing import Any, Literal, NoReturn

from fastapi import APIRouter, HTTPException, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from . import world_observations
from .admin_routes import body, service
from .life_engine import LifeEngineConfig
from .modules.actions import ActionCall, HourlyAction, Mode, reject_reason
from .modules.weather_location import (
    LocationLookupUnavailable,
    WeatherLocation,
    find_cities,
    locate_server,
)
from .rest_policy import RestHoursConfig
from .screen_watch import ScreenWatchConfig
from .security import Rejected
from .voice_companion import VoiceCompanionConfig

router = APIRouter(prefix="/admin/api/life")


INTENT_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
LEDGER_ID_RE = re.compile(r"^[A-Za-z0-9:._-]{1,128}$")
LIFE_INTENT_LIMIT = 40
LIFE_EVENT_LIMIT = 30


LifeConfigMirror = LifeEngineConfig


class WorldStateAdminUpdate(BaseModel):
    """Strict manual world-state / Attention Lock update from the admin console."""

    model_config = ConfigDict(extra="forbid", strict=True)

    current_module: str | None = Field(default=None, min_length=1, max_length=64)
    focus_level: Literal["LOW", "MEDIUM", "HIGH"] | None = None
    mood: str | None = Field(default=None, min_length=1, max_length=64)
    energy: float | None = Field(default=None, ge=0, le=100, multiple_of=0.1, allow_inf_nan=False)
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


class LedgerEventAdminCreate(BaseModel):
    """Strict manual event creation payload for the built-in event ledger."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    source_module: str = Field(default="owner_admin", min_length=1, max_length=32)
    target_module: str = Field(default="", max_length=32)
    event_type: str = Field(default="manual_todo", min_length=1, max_length=32)
    summary: str = Field(min_length=1, max_length=200)
    dedupe_key: str = Field(default="", max_length=80)
    due_in_minutes: int = Field(default=0, ge=0, le=43200)
    ttl_hours: int = Field(default=72, ge=1, le=336)
    visibility: Literal["public", "owner_private"] = "owner_private"

    @field_validator("source_module", "event_type", "summary")
    @classmethod
    def _non_blank_ledger(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    @field_validator("target_module", "dedupe_key")
    @classmethod
    def _strip_ledger_opt(cls, value: str) -> str:
        return value.strip()


class MinecraftAdminGoalRequest(BaseModel):
    """Strict admin control request for Mineflayer goals and pause/resume."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    view: Literal["life", "module"] = "life"
    action_type: Literal[
        "status",
        "explore",
        "mine",
        "goto",
        "build_plan",
        "chat",
        "look_signs",
        "stop",
        "idle",
        "pause",
        "resume",
    ]
    topic: str = Field(default="", max_length=80)
    block_type: str = Field(default="", max_length=48)
    message: str = Field(default="", max_length=120)
    x: float | None = None
    y: float | None = None
    z: float | None = None

    @field_validator("topic", "block_type", "message")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        return value.strip()


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


def _collect_mc_agent_status(svc: Any) -> dict[str, Any]:
    registry = getattr(svc, "modules", None)
    mc_mod = registry.get("minecraft") if registry is not None else None
    raw: dict[str, Any] = {}
    if mc_mod is not None and hasattr(mc_mod, "status_dict"):
        candidate = mc_mod.status_dict()
        if isinstance(candidate, dict):
            raw = candidate
    return {
        "bridge_ready": bool(raw.get("bridge_ready", False)),
        "mc_enabled": bool(raw.get("mc_enabled", False)),
        "mc_configured": bool(raw.get("mc_configured", False)),
        "connected": bool(raw.get("connected", False)),
        "paused": bool(raw.get("paused", False)),
        "dimension": str(raw.get("dimension") or ""),
        "pos": raw.get("pos"),
        "health": raw.get("health"),
        "food": raw.get("food"),
        "activity": str(raw.get("activity") or ""),
        "nearby_entities": list(raw.get("nearby_entities") or []),
        "recent_chat_or_sign": str(raw.get("recent_chat_or_sign") or ""),
        "updated_at": float(raw.get("updated_at", 0.0) or 0.0),
    }


async def _collect_modules_status(svc: Any, *, include_sensors: bool) -> list[dict[str, Any]]:
    registry = getattr(svc, "modules", None)
    if registry is None:
        return []
    items: list[dict[str, Any]] = []
    for mod in registry.all():
        try:
            summary = (
                await mod.get_sensor_summary()
                if include_sensors and not mod.inactive_reason
                else ""
            )
        except Exception:
            summary = ""
        items.append(
            {
                "module_id": mod.module_id,
                "enabled": mod.enabled,
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
            "action_type, mood, gate_condition_json, intent_note_json, status, updated_at, "
            "action_name,source_module,source_event_id,origin,result_reason,shadow_result "
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
            "action_name": row["action_name"],
            "source_module": row["source_module"],
            "source_event_id": row["source_event_id"],
            "origin": row["origin"],
            "result_reason": row["result_reason"],
            "shadow_result": row["shadow_result"],
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
            "payload": (
                {"private_first": True}
                if (payload := _parse_json_value(row["payload_json"], {})).get("private_first")
                else payload
            ),
        }
        for row in rows
    ]


async def _build_life_status(svc: Any, *, include_sensors: bool = True) -> dict[str, Any]:
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
        await ws.experience.advance(ws.clock())
        snap = ws.current
        last_proactive_at = snap.last_proactive_at
    else:
        snap = None
        last_proactive_at = 0.0

    store = getattr(svc, "store", None)
    if last_proactive_at <= 0.0 and store is not None:
        async with store.lock:
            candidates: list[float] = []
            if await _table_exists(store, "pending_owner_dms"):
                row = await store.one(
                    "SELECT MAX(COALESCE(sent_at, created)) FROM pending_owner_dms "
                    "WHERE status IN ('sent','delivered')"
                )
                if row and row[0]:
                    candidates.append(float(row[0]))
            if await _table_exists(store, "scheduled_intents"):
                row = await store.one(
                    "SELECT MAX(updated_at) FROM scheduled_intents "
                    "WHERE status='done' AND target_scope!=''"
                )
                if row and row[0]:
                    candidates.append(float(row[0]))
            if candidates:
                last_proactive_at = max(candidates)

    if snap is not None and ws is not None:
        experience = await ws.experience.view(ws.clock())
        world_state_data = {
            "experience": experience,
            "current_module": snap.current_module,
            "focus_level": snap.focus_level,
            "mood": snap.mood,
            "energy": snap.energy,
            "boredom": snap.boredom,
            "last_proactive_at": last_proactive_at,
            "updated_at": snap.updated_at,
            "proactive_suspended": bool(ws.proactive_suspended()),
            "modules": await _collect_modules_status(svc, include_sensors=include_sensors),
        }
    else:
        world_state_data = {
            "current_module": "idle",
            "focus_level": "LOW",
            "mood": "calm",
            "energy": 80,
            "boredom": 20,
            "last_proactive_at": last_proactive_at,
            "updated_at": 0.0,
            "proactive_suspended": False,
            "modules": await _collect_modules_status(svc, include_sensors=include_sensors),
        }

    proactive_mgr = getattr(svc, "proactive", None)
    onebot_connected = bool(proactive_mgr and proactive_mgr.onebot_connected())
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
        "shanghai_mc_agent": _collect_mc_agent_status(svc),
    }

    cloud_memory = getattr(svc, "cloud_memory", None)
    if cloud_memory is not None and hasattr(cloud_memory, "latest_activity_info"):
        desktop_memory = await _maybe_await(cloud_memory.latest_activity_info())
    else:
        desktop_memory = {"enabled": bool(svc.s.enable_antigravity_memory)}

    intents = await _read_intents(svc.store)
    catalog = getattr(engine, "catalog", None)
    if catalog is not None:
        for item in intents:
            name = item["action_name"] or catalog.old_name(item)
            item.update(action_name=name, executor_module=catalog.owners.get(name))
    events = await _read_events(svc.store)
    async with svc.store.lock:
        receipts = (
            await svc.store.all(
                "SELECT rule_id,event_id,outcome,intent_id,created_at FROM life_rule_receipts "
                "ORDER BY created_at DESC,rowid DESC LIMIT 30"
            )
            if await _table_exists(svc.store, "life_rule_receipts")
            else []
        )

    observations = None
    if engine is not None and hasattr(engine, "clock"):
        now = engine.clock()
        observations = await world_observations.overview(svc.store, now, svc.s.bot_timezone)
        if observations is not None:
            upcoming = observations["next_action"]
            for module in svc.modules.active():
                for slot in svc.modules.schedules_for(module):
                    due = await engine.next_slot_at(module, slot, now)
                    if due is not None and (upcoming is None or due < upcoming["execute_after"]):
                        upcoming = {
                            "action": slot.name,
                            "label": slot.label or slot.name,
                            "execute_after": due,
                            "origin": "timer",
                        }
            observations["next_action"] = upcoming

    ledger = getattr(svc, "event_ledger", None)
    now_ledger = engine.clock() if engine is not None and hasattr(engine, "clock") else None
    ledger_events = await ledger.admin_list(now=now_ledger, limit=30) if ledger is not None else []

    return {
        "config": config,
        "source": source,
        "timezone": svc.s.bot_timezone,
        "engine": engine_meta,
        "observation": observations,
        "digest_history": await proactive_mgr.digest_history()
        if hasattr(proactive_mgr, "digest_history")
        else [],
        "world_state": world_state_data,
        "nodes": nodes,
        "desktop_memory": desktop_memory,
        "intents": intents,
        "rule_receipts": [dict(row) for row in receipts],
        "events": events,
        "ledger_events": ledger_events,
    }


@router.get("")
async def life_status(request: Request):
    return await _build_life_status(service(request))


@router.get("/overview")
async def life_overview(request: Request):
    return await _build_life_status(service(request), include_sensors=False)


@router.get("/modules")
async def module_directory(request: Request):
    """Read the local registry without polling sensors or loading conversation data."""
    svc = service(request)
    registry = svc.modules
    counts = Counter(registry.catalog.owners.values())
    return [
        {
            "id": module.module_id,
            "title": module.display_name or module.module_id,
            "description": module.description,
            "region": module.node_region,
            "panel": module.admin_panel,
            "chat_platforms": list(module.chat_platforms),
            "owner_delivery": registry.chat_platforms.get(svc.proactive.platform) is module,
            "action_count": counts[module.module_id],
            "enabled": module.enabled,
            "configuration_issue": module.configuration_issue,
        }
        for module in registry.all()
    ]


@router.get("/modules/{module_id}")
async def module_detail(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    engine = getattr(svc, "life_engine", None)
    modes = engine.config.action_modes if engine else {}
    actions = svc.modules.catalog.view(modes, module_id=module_id)
    options = module.scheduling(engine.config)
    return {
        "id": module_id,
        "title": module.display_name or module_id,
        "description": module.description,
        "enabled": module.enabled,
        "configuration_issue": module.configuration_issue,
        "region": module.node_region,
        "panel": module.admin_panel,
        "chat_platforms": list(module.chat_platforms),
        "proactive_paused": module.proactive_paused,
        "actions": actions,
        "status": module.admin_status(request),
        "scheduling": {
            "fields": module.schedule_options.model_json_schema()["properties"],
            "values": options.model_dump(),
            "timers": [
                {
                    "action": slot.name,
                    "label": slot.label or slot.name,
                    "at": slot.effective_time(options),
                    **({"cadence": "hourly"} if isinstance(slot, HourlyAction) else {}),
                    "next_at": await engine.next_slot_at(module, slot, engine.clock()),
                }
                for slot in svc.modules.schedules_for(module)
            ],
        },
    }


class ModuleSwitch(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool


@router.put("/modules/{module_id}/enabled")
async def set_module_switch(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    value = await body(request, ModuleSwitch)
    async with request.app.state.admin_config_lock:
        await svc.life_engine.set_module_enabled(module, value.enabled)
        return await module_detail(request, module_id)


@router.put("/modules/{module_id}/schedule")
@router.delete("/modules/{module_id}/schedule")
async def module_schedule(request: Request, module_id: str):
    from .app import json_object, read_body

    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    raw = None
    if request.method == "PUT":
        raw = json_object(await read_body(request, svc.s))
        if not raw:
            raise HTTPException(400, "invalid_admin_input")
    async with request.app.state.admin_config_lock:
        engine = svc.life_engine
        await engine.initialize()
        try:
            options = (
                module.schedule_options.from_settings(svc.s)
                if raw is None
                else module.schedule_options.model_validate(
                    {**module.scheduling(engine.config).model_dump(), **raw}
                )
            )
            config = engine.config.model_dump()
            config["module_settings"][module_id] = options.model_dump()
            await engine.set_config(config)
        except ValueError:
            raise HTTPException(400, "invalid_module_settings") from None
        return await module_detail(request, module_id)


@router.get("/modules/{module_id}/sensor")
async def module_sensor(request: Request, module_id: str):
    module = service(request).modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    if module.inactive_reason:
        raise HTTPException(409, module.inactive_reason)
    try:
        summary = await module.get_sensor_summary()
    except Exception:
        raise HTTPException(503, "module_sensor_unavailable") from None
    return {"summary": summary}


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
        try:
            value = LifeEngineConfig.model_validate({**current, **raw})
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        if engine is None:
            raise HTTPException(503, "life_engine_unavailable")
        try:
            await _maybe_await(engine.set_config(value))
        except ValueError:
            raise HTTPException(400, "unsupported_action_mode") from None
        return await _build_life_status(svc)


class ActionModeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Mode
    view: Literal["life", "module"] = "life"


@router.put("/actions/{name}")
async def life_action_mode(request: Request, name: str):
    value = await body(request, ActionModeUpdate)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        engine = svc.life_engine
        await engine.initialize()
        config = engine.config.model_dump()
        config["action_modes"][name] = value.mode
        try:
            await engine.set_config(config)
        except ValueError:
            raise HTTPException(400, "unsupported_action_mode") from None
        if value.view == "module":
            return await module_detail(request, engine.catalog.owners[name])
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


class WorldRhythmUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schedule: dict[str, Any]
    rest: RestHoursConfig


@router.put("/rhythm")
@router.delete("/rhythm")
async def world_rhythm(request: Request):
    svc = service(request)
    update = await body(request, WorldRhythmUpdate) if request.method == "PUT" else None
    async with request.app.state.admin_config_lock:
        try:
            await svc.life_engine.set_rhythm(
                update.schedule if update else None, update.rest if update else None
            )
        except ValueError:
            raise HTTPException(400, "invalid_world_rhythm") from None
        return {
            "world": await _build_life_status(svc, include_sensors=False),
            "rest": svc.store.runtime.rest_hours_status(),
        }


@router.put("/schedule")
@router.delete("/schedule")
async def world_schedule(request: Request):
    from .app import json_object, read_body

    svc = service(request)
    global_fields = LifeEngineConfig.model_fields.keys() - {"module_settings", "action_modes"}
    engine = svc.life_engine
    if request.method == "PUT":
        values = json_object(await read_body(request, svc.s))
        if not values or values.keys() - global_fields:
            raise HTTPException(400, "invalid_world_settings")
    else:
        values = engine.env_config.model_dump(include=global_fields)
    async with request.app.state.admin_config_lock:
        await engine.initialize()
        try:
            await engine.set_config({**engine.config.model_dump(), **values})
        except ValueError:
            raise HTTPException(400, "invalid_world_settings") from None
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
            await ws.experience.advance(ws.clock())
            await ws.update_state(
                current_module=value.current_module,
                focus_level=value.focus_level,
                mood=value.mood,
                energy=value.energy,
                boredom=value.boredom,
            )
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        except ValueError as exc:
            if str(exc) != "mc_task_required":
                raise
            raise HTTPException(400, "mc_task_required") from None
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


async def _build_ledger_payload(svc) -> dict:
    ledger = getattr(svc, "event_ledger", None)
    if ledger is None:
        raise HTTPException(503, "event_ledger_unavailable")
    engine = getattr(svc, "life_engine", None)
    now = engine.clock() if engine is not None and hasattr(engine, "clock") else ledger.clock()
    pending, due = await ledger.status_counts(now=now)
    items = await ledger.admin_list(now=now, limit=50)
    consumed = sum(1 for item in items if item.get("status") == "consumed")
    expired = sum(1 for item in items if item.get("status") == "expired")
    modules = sorted(
        {m.module_id for m in svc.modules.all()}
        if getattr(svc, "modules", None) is not None
        else set()
    )
    return {
        "timezone": svc.s.bot_timezone,
        "counts": {
            "pending": pending,
            "due": due,
            "consumed": consumed,
            "expired": expired,
        },
        "ledger_events": items,
        "modules": modules,
    }


@router.get("/ledger")
async def life_ledger_status(request: Request):
    return await _build_ledger_payload(service(request))


@router.post("/ledger")
async def life_create_ledger_event(request: Request):
    value = await body(request, LedgerEventAdminCreate)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        ledger = getattr(svc, "event_ledger", None)
        if ledger is None:
            raise HTTPException(503, "event_ledger_unavailable")
        engine = getattr(svc, "life_engine", None)
        now = engine.clock() if engine is not None and hasattr(engine, "clock") else ledger.clock()
        due_at = now + value.due_in_minutes * 60.0
        try:
            await ledger.post(
                value.source_module,
                value.event_type,
                value.summary,
                target_module=value.target_module,
                dedupe_key=value.dedupe_key or None,
                due_at=due_at,
                ttl_seconds=value.ttl_hours * 3600.0,
                visibility=value.visibility,
                now=now,
            )
        except ValueError:
            raise HTTPException(400, "invalid_admin_input") from None
        return await _build_ledger_payload(svc)


@router.delete("/ledger/{item_id}")
async def life_consume_ledger_event(request: Request, item_id: str):
    if not LEDGER_ID_RE.fullmatch(item_id):
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    async with request.app.state.admin_config_lock:
        ledger = getattr(svc, "event_ledger", None)
        if ledger is None:
            raise HTTPException(503, "event_ledger_unavailable")
        engine = getattr(svc, "life_engine", None)
        now = engine.clock() if engine is not None and hasattr(engine, "clock") else ledger.clock()
        consumed = await ledger.consume(
            item_id, consumer="admin", allow_owner_private=True, now=now
        )
        if not consumed:
            raise HTTPException(404, "ledger_event_not_found")
        return await _build_ledger_payload(svc)


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
        changed = await ws.mark_intent(intent_id, "gated_skip", reason="admin_cancelled")
        if not changed:
            raise HTTPException(409, "intent_not_pending")
        return await _build_life_status(svc)


@router.post("/mc/goal")
async def life_mc_goal(request: Request):
    value = await body(request, MinecraftAdminGoalRequest)
    svc = service(request)
    registry = getattr(svc, "modules", None)
    mc_mod = registry.get("minecraft") if registry is not None else None
    if mc_mod is None or not hasattr(mc_mod, "dispatch_goal"):
        raise HTTPException(503, "minecraft_module_unavailable")
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        args = value.model_dump(
            exclude={"action_type", "view"}, exclude_defaults=True, exclude_none=True
        )
        ticket = {
            "action_name": "mc." + value.action_type,
            "target_scope": "",
            "origin": "admin",
            "action_type": value.action_type,
            "target_module": "minecraft",
            "arguments": args,
        }
        try:
            spec, validated = svc.life_engine.resolve(ticket, admin=True)
        except ValueError as exc:
            raise HTTPException(400, reject_reason(exc)) from None
        result = await spec.run(ActionCall(ticket, validated, svc.life_engine.clock()))
        goal_result = result.details.get("result")
        if not isinstance(goal_result, dict):
            goal_result = {
                "ok": result.status == "done",
                "module_id": "minecraft",
                "summary": "",
                "artifacts": {},
                "reason": result.reason,
            }
        response = {
            "status": "ok" if result.status == "done" else "unavailable",
            "result": goal_result,
        }
        if value.view == "module":
            response["mc_status"] = _collect_mc_agent_status(svc)
        else:
            response["life"] = await _build_life_status(svc)
        return response


@router.get("/voice-companion")
async def life_voice_companion_status(request: Request):
    svc = service(request)
    vc = getattr(svc, "voice_companion", None)
    if vc is None:
        raise HTTPException(503, "voice_companion_unavailable")
    return await vc.admin_status()


@router.put("/voice-companion")
async def life_voice_companion_update(request: Request):
    value = await body(request, VoiceCompanionConfig)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        vc = getattr(svc, "voice_companion", None)
        if vc is None:
            raise HTTPException(503, "voice_companion_unavailable")
        return await vc.set_config(value)


@router.delete("/voice-companion")
async def life_voice_companion_reset(request: Request):
    svc = service(request)
    async with request.app.state.admin_config_lock:
        vc = getattr(svc, "voice_companion", None)
        if vc is None:
            raise HTTPException(503, "voice_companion_unavailable")
        return await vc.set_config(None)


@router.post("/voice-companion/disconnect")
async def life_voice_companion_disconnect(request: Request):
    svc = service(request)
    vc = getattr(svc, "voice_companion", None)
    if vc is None:
        raise HTTPException(503, "voice_companion_unavailable")
    return await vc.disconnect_active()


@router.get("/screen-watch")
async def life_screen_watch_status(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.admin_status()


@router.put("/screen-watch")
async def life_screen_watch_update(request: Request):
    value = await body(request, ScreenWatchConfig)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        sw = getattr(svc, "screen_watch", None)
        if sw is None:
            raise HTTPException(503, "screen_watch_unavailable")
        return await sw.set_config(value)


@router.delete("/screen-watch")
async def life_screen_watch_reset(request: Request):
    svc = service(request)
    async with request.app.state.admin_config_lock:
        sw = getattr(svc, "screen_watch", None)
        if sw is None:
            raise HTTPException(503, "screen_watch_unavailable")
        return await sw.set_config(None)


@router.post("/screen-watch/clear")
async def life_screen_watch_clear(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.clear_frames_admin()


@router.post("/screen-watch/disconnect")
async def life_screen_watch_disconnect(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.disconnect_active()


class WeatherCityQuery(BaseModel):
    """Strict city geocoding lookup input."""

    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)
    query: str = Field(min_length=2, max_length=80)


def _weather_module(svc: Any) -> Any:
    registry = getattr(svc, "modules", None)
    if registry is not None:
        return registry.get("weather")
    return getattr(svc, "weather", None)


def _raise_weather_rpc_http_error(exc: Rejected) -> NoReturn:
    code = str(exc)
    if code in {
        "invalid_admin_input",
        "plugin_invalid_params",
        "plugin_invalid_schema",
        "invalid_state_value",
    }:
        raise HTTPException(400, code) from None
    if code in {
        "weather_unavailable",
        "weather_stale",
        "weather_city_lookup_unavailable",
        "weather_ip_lookup_unavailable",
        "plugin_disconnected",
        "plugin_rpc_timeout",
        "plugin_rpc_queue_full",
        "plugin_rpc_failed",
        "plugin_internal_error",
        "rpc_method_not_found",
    }:
        raise HTTPException(503, code) from None
    raise HTTPException(409, code) from None


@router.get("/modules/weather/current")
async def current_weather(request: Request):
    svc = service(request)
    if _weather_module(svc) is None:
        raise HTTPException(503, "weather_module_unavailable")
    engine = getattr(svc, "life_engine", None)
    if engine is None:
        raise HTTPException(503, "life_engine_unavailable")
    result = await engine.query("weather.read")
    if result.status != "done":
        status = (
            503
            if result.reason
            in {"weather_unavailable", "weather_stale", "plugin_disconnected", "plugin_rpc_timeout"}
            else 409
        )
        raise HTTPException(status, result.reason)
    return result.details


@router.post("/modules/weather/location/cities")
async def weather_cities(request: Request):
    query = await body(request, WeatherCityQuery)
    svc = service(request)
    mod = _weather_module(svc)
    if mod is None:
        raise HTTPException(503, "weather_module_unavailable")
    if hasattr(mod, "call_custom_rpc"):
        try:
            return await mod.call_custom_rpc("weather.location.cities", {"query": query.query})
        except Rejected as exc:
            _raise_weather_rpc_http_error(exc)
    try:
        return {"places": await find_cities(mod.http, query.query)}
    except LocationLookupUnavailable:
        raise HTTPException(503, "weather_city_lookup_unavailable") from None


@router.post("/modules/weather/location/detect")
async def weather_detect_location(request: Request):
    svc = service(request)
    mod = _weather_module(svc)
    if mod is None:
        raise HTTPException(503, "weather_module_unavailable")
    if hasattr(mod, "call_custom_rpc"):
        try:
            return await mod.call_custom_rpc("weather.location.detect", {})
        except Rejected as exc:
            _raise_weather_rpc_http_error(exc)
    try:
        return await locate_server(mod.http)
    except LocationLookupUnavailable:
        raise HTTPException(503, "weather_ip_lookup_unavailable") from None


@router.put("/modules/weather/location")
@router.delete("/modules/weather/location")
async def weather_set_location(request: Request):
    svc = service(request)
    place = await body(request, WeatherLocation) if request.method == "PUT" else None
    async with request.app.state.admin_config_lock:
        mod = _weather_module(svc)
        if mod is None:
            raise HTTPException(503, "weather_module_unavailable")
        if hasattr(mod, "call_custom_rpc"):
            try:
                if place is not None:
                    await mod.call_custom_rpc("weather.location.set", place.model_dump())
                else:
                    await mod.call_custom_rpc("weather.location.reset", {})
            except Rejected as exc:
                _raise_weather_rpc_http_error(exc)
        else:
            await mod.set_location(place)
        if (engine := getattr(svc, "life_engine", None)) is not None:
            engine.wakeup.set()
        return await module_detail(request, "weather")
