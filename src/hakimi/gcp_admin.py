"""Authenticated control plane for GCP Cloud Billing BigQuery & Cloud Monitoring."""

import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from .admin_routes import body, service
from .gcp_billing import (
    GcpBillingConfig,
    GcpBillingCredentialsUpdate,
    GcpBillingManager,
)

router = APIRouter(prefix="/admin/api/gcp-billing")


async def _manager(request: Request) -> GcpBillingManager:
    store = service(request).store
    mgr = getattr(store.runtime, "gcp_billing", None)
    if mgr is not None:
        return mgr
    lock = getattr(store.runtime, "_gcp_billing_init_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        store.runtime._gcp_billing_init_lock = lock
    async with lock:
        mgr = getattr(store.runtime, "gcp_billing", None)
        if mgr is None:
            mgr = GcpBillingManager(store)
            await mgr.initialize()
            store.runtime.gcp_billing = mgr
        return mgr


@router.get("")
async def gcp_billing_status(request: Request):
    mgr = await _manager(request)
    return await mgr.status()


@router.put("")
async def gcp_billing_save(request: Request):
    from .app import json_object, read_body

    raw = json_object(await read_body(request, request.app.state.settings))
    if not raw:
        raise HTTPException(400, "invalid_admin_input")
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        current = mgr.config.model_dump(mode="json")
        try:
            value = GcpBillingConfig.model_validate({**current, **raw})
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        return await mgr.set_config(value)


@router.delete("")
async def gcp_billing_reset(request: Request):
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        return await mgr.set_config(None)


@router.post("/credentials")
async def gcp_billing_credentials(request: Request):
    value = await body(request, GcpBillingCredentialsUpdate)
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        return await mgr.update_credentials(value)


@router.post("/sync-bigquery")
async def gcp_billing_sync_bigquery(request: Request):
    request.app.state.admin_auth.costly()
    mgr = await _manager(request)
    transport = getattr(request.app.state, "gcp_transport", None)
    return await mgr.sync_bigquery(transport=transport)


@router.post("/sync-monitoring")
async def gcp_billing_sync_monitoring(request: Request):
    request.app.state.admin_auth.costly()
    mgr = await _manager(request)
    transport = getattr(request.app.state, "gcp_transport", None)
    return await mgr.sync_monitoring(transport=transport)
