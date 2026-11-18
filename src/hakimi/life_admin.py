"""Authenticated control plane for the world state, registered life actions and routing modes."""

from __future__ import annotations

import inspect
import json
import re
from collections import Counter
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

from . import world_observations
from .admin_routes import body, onebot_connection, service
from .life_engine import LifeEngineConfig
from .modules.actions import ActionCall, Mode, reject_reason

router = APIRouter(prefix="/admin/api/life")


INTENT_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
LIFE_INTENT_LIMIT = 40
LIFE_EVENT_LIMIT = 30


LifeConfigMirror = LifeEngineConfig


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
        "look_signs",
        "stop",
        "idle",
        "pause",
        "resume",
    ]
    topic: str = Field(default="", max_length=80)
    block_type: str = Field(default="", max_length=48)
    x: float | None = None
    y: float | None = None
    z: float | None = None

    @field_validator("topic", "block_type")
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
            summary = await mod.get_sensor_summary() if include_sensors else ""
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
        world_state_data = {
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
            for module in svc.modules.all():
                for slot in module.schedules():
                    due = await engine.next_slot_at(module, slot, now)
                    if due is not None and (upcoming is None or due < upcoming["execute_after"]):
                        upcoming = {
                            "action": slot.name,
                            "label": slot.label or slot.name,
                            "execute_after": due,
                            "origin": "timer",
                        }
            observations["next_action"] = upcoming

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
            "region": module.node_region,
            "action_count": counts[module.module_id],
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
    status = {}
    if module_id == "qq_social":
        status = onebot_connection(request, svc)
    elif module_id == "bilibili_surf":
        status = {
            "autonomous": svc.bili_watcher.policy.enabled,
            "backend": module.backend_mode,
            "cn_delegate": bool(
                svc.s.node_bili_delegate_enabled
                and svc.node_transport
                and svc.node_transport.connected()
            ),
        }
    elif module_id == "minecraft":
        status = _collect_mc_agent_status(svc)
    return {
        "id": module_id,
        "region": module.node_region,
        "proactive_paused": module.proactive_paused,
        "actions": actions,
        "status": status,
        "scheduling": {
            "fields": module.schedule_options.model_json_schema()["properties"],
            "values": options.model_dump(),
            "timers": [
                {
                    "action": slot.name,
                    "label": slot.label or slot.name,
                    "at": slot.effective_time(options),
                    "next_at": await engine.next_slot_at(module, slot, engine.clock()),
                }
                for slot in module.schedules()
            ],
        },
    }


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
        response = {
            "status": "ok" if result.status == "done" else "unavailable",
            "result": result.details["result"],
        }
        if value.view == "module":
            response["mc_status"] = _collect_mc_agent_status(svc)
        else:
            response["life"] = await _build_life_status(svc)
        return response
