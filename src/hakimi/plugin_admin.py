"""Authenticated control plane for remote plugin credentials and active WS sessions."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .admin_routes import body, service
from .plugin_bus import (
    PLUGIN_ID_RE,
    RESERVED_PLUGIN_IDS,
    VALID_SCOPES,
    PluginBus,
)
from .security import Rejected

router = APIRouter(prefix="/admin/api/plugins")

_VALID_SCOPE_SET = frozenset(VALID_SCOPES)
_ERROR_STATUS = {
    "invalid_admin_input": 400,
    "invalid_plugin_id": 400,
    "reserved_plugin_id": 400,
    "invalid_plugin_scope": 400,
    "plugin_not_found": 404,
    "plugin_already_exists": 409,
    "plugin_bus_unavailable": 503,
}


class PluginCreateBody(BaseModel):
    """Strict payload to issue credentials for a new remote plugin."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plugin_id: str = Field(min_length=2, max_length=48)
    display_name: str = Field(min_length=1, max_length=64)
    scopes: list[str] = Field(max_length=16)
    node_region: Literal["KR", "CN"] = "KR"

    @field_validator("plugin_id")
    @classmethod
    def _check_plugin_id(cls, value: str) -> str:
        if not PLUGIN_ID_RE.fullmatch(value) or value in RESERVED_PLUGIN_IDS:
            raise ValueError("invalid plugin_id")
        return value

    @field_validator("display_name")
    @classmethod
    def _check_display_name(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("blank display_name")
        return stripped

    @field_validator("scopes")
    @classmethod
    def _check_scopes(cls, value: list[str]) -> list[str]:
        if any(scope not in _VALID_SCOPE_SET for scope in value):
            raise ValueError("invalid scope")
        return list(dict.fromkeys(value))


class PluginUpdateBody(BaseModel):
    """Strict partial update payload for a remote plugin credential."""

    model_config = ConfigDict(extra="forbid", strict=True)

    display_name: str | None = Field(default=None, min_length=1, max_length=64)
    scopes: list[str] | None = Field(default=None, max_length=16)
    enabled: bool | None = None
    node_region: Literal["KR", "CN"] | None = None

    @field_validator("display_name")
    @classmethod
    def _check_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("blank display_name")
        return stripped

    @field_validator("scopes")
    @classmethod
    def _check_scopes(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if any(scope not in _VALID_SCOPE_SET for scope in value):
            raise ValueError("invalid scope")
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def _require_any_field(self) -> PluginUpdateBody:
        if all(v is None for v in (self.display_name, self.scopes, self.enabled, self.node_region)):
            raise ValueError("at least one field must be provided")
        return self


def _bus(request: Request) -> PluginBus:
    svc = service(request)
    bus = getattr(svc, "plugin_bus", None)
    if bus is not None:
        return bus
    modules = getattr(svc, "modules", None)
    systems = getattr(modules, "systems", None) or {}
    sys_mod = systems.get("plugin_bus")
    if sys_mod is not None and getattr(sys_mod, "plugin_bus", None) is not None:
        return sys_mod.plugin_bus
    raise HTTPException(503, "plugin_bus_unavailable")


def _validate_path_plugin_id(plugin_id: str) -> str:
    if not PLUGIN_ID_RE.fullmatch(plugin_id):
        raise HTTPException(400, "invalid_admin_input")
    if plugin_id in RESERVED_PLUGIN_IDS:
        raise HTTPException(404, "plugin_not_found")
    return plugin_id


def _http_from_rejected(exc: Rejected) -> HTTPException:
    code = str(exc).strip()
    status = _ERROR_STATUS.get(code, 400)
    detail = code if code in _ERROR_STATUS else "invalid_admin_input"
    return HTTPException(status, detail)


@router.get("")
async def plugins_overview(request: Request):
    bus = _bus(request)
    return await bus.overview()


@router.post("")
async def plugins_create(request: Request):
    value = await body(request, PluginCreateBody)
    bus = _bus(request)
    async with request.app.state.admin_config_lock:
        try:
            token, state = await bus.create_plugin(
                value.plugin_id,
                value.display_name,
                value.scopes,
                node_region=value.node_region,
            )
        except Rejected as exc:
            raise _http_from_rejected(exc) from None
        return {
            "plugin_id": value.plugin_id,
            "token": token,
            "ws_path": state["ws_path"],
            "state": state,
        }


@router.put("/{plugin_id}")
async def plugins_update(request: Request, plugin_id: str):
    pid = _validate_path_plugin_id(plugin_id)
    value = await body(request, PluginUpdateBody)
    bus = _bus(request)
    async with request.app.state.admin_config_lock:
        try:
            return await bus.update_plugin(
                pid,
                display_name=value.display_name,
                scopes=value.scopes,
                enabled=value.enabled,
                node_region=value.node_region,
            )
        except Rejected as exc:
            raise _http_from_rejected(exc) from None


@router.post("/{plugin_id}/rotate")
async def plugins_rotate_token(request: Request, plugin_id: str):
    pid = _validate_path_plugin_id(plugin_id)
    bus = _bus(request)
    async with request.app.state.admin_config_lock:
        try:
            token, state = await bus.rotate_token(pid)
        except Rejected as exc:
            raise _http_from_rejected(exc) from None
        return {
            "plugin_id": pid,
            "token": token,
            "state": state,
        }


@router.post("/{plugin_id}/disconnect")
async def plugins_disconnect(request: Request, plugin_id: str):
    pid = _validate_path_plugin_id(plugin_id)
    bus = _bus(request)
    async with request.app.state.admin_config_lock:
        try:
            return await bus.disconnect_plugin(pid)
        except Rejected as exc:
            raise _http_from_rejected(exc) from None


@router.delete("/{plugin_id}")
async def plugins_delete(request: Request, plugin_id: str):
    pid = _validate_path_plugin_id(plugin_id)
    bus = _bus(request)
    async with request.app.state.admin_config_lock:
        try:
            return await bus.delete_plugin(pid)
        except Rejected as exc:
            raise _http_from_rejected(exc) from None
