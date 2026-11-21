"""Authenticated control plane for registered life modules, schedules, sensors, and weather."""

from __future__ import annotations

from collections import Counter
from typing import Any, NoReturn

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from .admin_common import body, domain_lock, partial_body, service
from .modules.actions import HourlyAction
from .modules.weather_location import (
    LocationLookupUnavailable,
    WeatherLocation,
    find_cities,
    locate_server,
)
from .security import Rejected

router = APIRouter(prefix="/admin/api/life")


class ModuleSwitch(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool


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


@router.put("/modules/{module_id}/enabled")
async def set_module_switch(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    value = await body(request, ModuleSwitch)
    async with domain_lock(request, "life"):
        await svc.life_engine.set_module_enabled(module, value.enabled)
        return await module_detail(request, module_id)


@router.put("/modules/{module_id}/schedule")
@router.delete("/modules/{module_id}/schedule")
async def module_schedule(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None:
        raise HTTPException(404, "module_not_registered")
    async with domain_lock(request, "life"):
        engine = svc.life_engine
        await engine.initialize()
        if request.method == "PUT":
            options = await partial_body(
                request,
                module.scheduling(engine.config).model_dump(),
                module.schedule_options,
                error_detail="invalid_module_settings",
            )
        else:
            options = module.schedule_options.from_settings(svc.s)
        try:
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
    async with domain_lock(request, "weather"):
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
