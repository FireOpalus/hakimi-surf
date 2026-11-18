"""Authenticated control plane for provider routing, privacy tiers, and API credentials."""

import inspect
from typing import Literal

from fastapi import APIRouter, HTTPException, Request

from .admin_routes import body, service
from .provider_routing import (
    QQ_PRIVACY_FIELDS,
    PrivacyDefaults,
    ProviderCredentialsUpdate,
    ProviderRoutingConfig,
    ProviderRoutingManager,
    ProviderView,
    SessionPrivacyUpdate,
)
from .security import configured_secrets

router = APIRouter(prefix="/admin/api")


class LegacySessionPrivacyUpdate(SessionPrivacyUpdate):
    # A previously opened console may still submit the retired selector with a privacy edit.
    hydrogen_provider: Literal["default", "gemini", "vertex"] | None = None


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


@router.get("/providers")
async def provider_status(request: Request):
    return await _routing_manager(service(request)).status()


@router.get("/global/providers")
async def global_provider_status(request: Request):
    return await _routing_manager(service(request)).status(view="global")


@router.put("/providers")
async def provider_save(request: Request):
    return await _save_policy(request, "full")


@router.put("/global/providers")
async def global_provider_save(request: Request):
    return await _save_policy(request, "global")


async def _save_policy(request: Request, view: ProviderView):
    value = await body(request, ProviderRoutingConfig)
    if view == "global" and value.model_fields_set.intersection(QQ_PRIVACY_FIELDS):
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        await routing.initialize()
        preserved = routing.config.model_dump(
            include=set(QQ_PRIVACY_FIELDS) - value.model_fields_set
        )
        if preserved:
            value = value.model_copy(update=preserved)
        result = await routing.set_config(value, view=view)
        await _reload_ai_clients(svc)
        return result


@router.delete("/providers")
async def provider_reset(request: Request):
    return await _reset_policy(request, "full")


@router.delete("/global/providers")
async def global_provider_reset(request: Request):
    return await _reset_policy(request, "global")


async def _reset_policy(request: Request, view: ProviderView):
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        await routing.initialize()
        privacy = routing.config.model_dump(include=set(QQ_PRIVACY_FIELDS))
        reset = routing.env_config.model_copy(update=privacy)
        result = await routing.set_config(None if reset == routing.env_config else reset, view=view)
        await _reload_ai_clients(svc)
        return result


@router.post("/providers/credentials")
async def provider_credentials(request: Request):
    return await _save_credentials(request, "full")


@router.post("/global/providers/credentials")
async def global_provider_credentials(request: Request):
    return await _save_credentials(request, "global")


async def _save_credentials(request: Request, view: ProviderView):
    value = await body(request, ProviderCredentialsUpdate)
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        result = await routing.update_credentials(value, view=view)
        svc.secrets = configured_secrets(svc.s)
        await _reload_ai_clients(svc)
        return result


@router.get("/qq/privacy")
async def qq_privacy(request: Request):
    return await _routing_manager(service(request)).privacy_status()


@router.put("/qq/privacy")
async def qq_privacy_defaults(request: Request):
    value = await body(request, PrivacyDefaults)
    routing = _routing_manager(service(request))
    async with request.app.state.admin_config_lock:
        await routing.set_privacy_defaults(value)
        return await routing.privacy_status()


@router.post("/providers/session-policy")
async def provider_session_policy(request: Request):
    legacy = await body(request, LegacySessionPrivacyUpdate)
    value = SessionPrivacyUpdate.model_validate(legacy.model_dump(exclude={"hydrogen_provider"}))
    routing = _routing_manager(service(request))
    async with request.app.state.admin_config_lock:
        await routing.set_session_policy(value)
        return await routing.status()


@router.post("/qq/privacy")
async def qq_session_privacy(request: Request):
    value = await body(request, SessionPrivacyUpdate)
    svc = service(request)
    routing = _routing_manager(svc)
    async with request.app.state.admin_config_lock:
        await routing.set_session_policy(value)
        return await routing.privacy_status()
