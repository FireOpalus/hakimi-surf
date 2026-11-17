"""Authenticated control plane for provider routing, privacy tiers, and API credentials."""

import inspect

from fastapi import APIRouter, Request

from .admin_routes import body, service
from .provider_routing import (
    ProviderCredentialsUpdate,
    ProviderRoutingConfig,
    ProviderRoutingManager,
    SessionPrivacyUpdate,
)
from .security import configured_secrets

router = APIRouter(prefix="/admin/api/providers")


def _routing_manager(svc) -> ProviderRoutingManager:
    routing = getattr(svc.store.runtime, "provider_routing", None)
    if routing is None:
        routing = ProviderRoutingManager(svc.store)
        svc.store.runtime.provider_routing = routing
    return routing


async def _reload_ai_clients(svc) -> None:
    reloader = getattr(getattr(svc, "ai", None), "reload_provider_clients", None)
    if reloader is not None:
        res = reloader()
        if inspect.isawaitable(res):
            await res


@router.get("")
async def provider_status(request: Request):
    svc = service(request)
    return await _routing_manager(svc).status()


@router.put("")
async def provider_save(request: Request):
    value = await body(request, ProviderRoutingConfig)
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        await routing.initialize()
        preserved = {}
        for dict_field in (
            "group_privacy",
            "private_privacy",
            "group_hydrogen_provider",
            "private_hydrogen_provider",
        ):
            if dict_field not in value.model_fields_set:
                preserved[dict_field] = dict(getattr(routing.config, dict_field))
        if preserved:
            value = value.model_copy(update=preserved)
        result = await routing.set_config(value)
        await _reload_ai_clients(svc)
        return result


@router.delete("")
async def provider_reset(request: Request):
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        result = await routing.set_config(None)
        await _reload_ai_clients(svc)
        return result


@router.post("/credentials")
async def provider_credentials(request: Request):
    value = await body(request, ProviderCredentialsUpdate)
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        result = await routing.update_credentials(value)
        svc.secrets = configured_secrets(svc.s)
        await _reload_ai_clients(svc)
        return result


@router.post("/session-policy")
async def provider_session_policy(request: Request):
    value = await body(request, SessionPrivacyUpdate)
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        return await routing.set_session_policy(value)
