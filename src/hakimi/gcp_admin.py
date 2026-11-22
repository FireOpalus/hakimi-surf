"""Authenticated control plane for GCP Cloud Billing BigQuery & Cloud Monitoring."""

import asyncio
from typing import Any

from fastapi import APIRouter, Request

from .admin_common import body, domain_lock, partial_body, service
from .gcp_billing import (
    GcpBillingConfig,
    GcpBillingCredentialsUpdate,
    GcpBillingManager,
)

router = APIRouter(prefix="/admin/api/gcp-billing")


def _micro_to_usd(value: Any) -> float:
    return round(int(value or 0) / 1_000_000, 6)


async def _attach_reconciliation(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    """Attach read-only billing vs local-estimate reconciliation summary."""
    store = service(request).store
    flush = getattr(getattr(store, "runtime", None), "flush", None)
    if callable(flush):
        await flush()
    async with store.lock:
        usage_row = await store.one(
            "SELECT calls, charged_amount, estimated_amount, estimated_calls "
            "FROM usage_aggregates WHERE period='lifetime'"
        )
        vertex_row = await store.one(
            "SELECT calls, charged_amount, estimated_amount "
            "FROM provider_usage_aggregates WHERE period='lifetime' AND provider='vertex_paid'"
        )

    lifetime_calls = int(usage_row["calls"] or 0) if usage_row else 0
    local_charged_usd = _micro_to_usd(usage_row["charged_amount"]) if usage_row else 0.0
    estimated_calls = int(usage_row["estimated_calls"] or 0) if usage_row else 0
    local_estimated_usd = (
        _micro_to_usd(usage_row["estimated_amount"]) if usage_row and estimated_calls > 0 else None
    )
    use_estimated = (
        local_estimated_usd is not None and lifetime_calls > 0 and estimated_calls == lifetime_calls
    )
    local_reference_usd = local_estimated_usd if use_estimated else local_charged_usd
    local_reference_basis = "estimated" if use_estimated else "charged"

    vertex_calls = int(vertex_row["calls"] or 0) if vertex_row else 0
    local_vertex_charged_usd = _micro_to_usd(vertex_row["charged_amount"]) if vertex_row else 0.0
    local_vertex_estimated_usd = (
        _micro_to_usd(vertex_row["estimated_amount"])
        if vertex_row and vertex_calls > 0 and int(vertex_row["estimated_amount"] or 0) > 0
        else None
    )

    bq = payload.get("bigquery") if isinstance(payload.get("bigquery"), dict) else {}
    mon = payload.get("monitoring") if isinstance(payload.get("monitoring"), dict) else {}
    initial_credits = float(bq.get("initial_credits_usd") or 0.0)
    credits_used = float(bq.get("credits_used_usd") or 0.0)
    credit_utilization_ratio = (
        round(min(max(credits_used / initial_credits, 0.0), 10.0), 6)
        if initial_credits > 0
        else None
    )

    if not bq.get("synced"):
        reconciliation = {
            "status": "not_synced",
            "billing_actual_usd": None,
            "billing_net_usd": None,
            "billing_plus_unsettled_usd": None,
            "local_estimated_usd": local_estimated_usd,
            "local_charged_usd": local_charged_usd,
            "local_reference_usd": local_reference_usd,
            "local_reference_basis": local_reference_basis,
            "local_vertex_charged_usd": local_vertex_charged_usd,
            "local_vertex_estimated_usd": local_vertex_estimated_usd,
            "cost_delta_usd": None,
            "cost_delta_after_unsettled_usd": None,
            "cost_delta_ratio": None,
            "credit_utilization_ratio": credit_utilization_ratio,
            "monitoring_status": str(mon.get("status") or "not_synced"),
            "last_sync_at": bq.get("last_sync_at"),
        }
    else:
        billing_actual_usd = round(float(bq.get("gross_cost_usd") or 0.0), 6)
        billing_net_usd = round(float(bq.get("net_cost_usd") or 0.0), 6)
        unsettled_usd = round(float(bq.get("unsettled_local_usd") or 0.0), 6)
        billing_plus_unsettled_usd = round(billing_actual_usd + unsettled_usd, 6)
        cost_delta_usd = round(billing_actual_usd - local_reference_usd, 6)
        cost_delta_after_unsettled_usd = round(billing_plus_unsettled_usd - local_reference_usd, 6)
        denom = max(abs(billing_actual_usd), abs(local_reference_usd))
        cost_delta_ratio = 0.0 if denom == 0 else round(abs(cost_delta_usd) / denom, 6)
        after_unsettled_denom = max(abs(billing_plus_unsettled_usd), abs(local_reference_usd))
        if abs(cost_delta_usd) <= 0.05 or cost_delta_ratio <= 0.05:
            status = "in_sync"
        elif (
            cost_delta_usd < 0
            and unsettled_usd > 0
            and abs(cost_delta_after_unsettled_usd) <= max(0.05, 0.10 * after_unsettled_denom)
        ):
            status = "export_lag"
        else:
            status = "drift_detected"
        reconciliation = {
            "status": status,
            "billing_actual_usd": billing_actual_usd,
            "billing_net_usd": billing_net_usd,
            "billing_plus_unsettled_usd": billing_plus_unsettled_usd,
            "local_estimated_usd": local_estimated_usd,
            "local_charged_usd": local_charged_usd,
            "local_reference_usd": local_reference_usd,
            "local_reference_basis": local_reference_basis,
            "local_vertex_charged_usd": local_vertex_charged_usd,
            "local_vertex_estimated_usd": local_vertex_estimated_usd,
            "cost_delta_usd": cost_delta_usd,
            "cost_delta_after_unsettled_usd": cost_delta_after_unsettled_usd,
            "cost_delta_ratio": cost_delta_ratio,
            "credit_utilization_ratio": credit_utilization_ratio,
            "monitoring_status": str(mon.get("status") or "not_synced"),
            "last_sync_at": bq.get("last_sync_at"),
        }

    return {**payload, "reconciliation": reconciliation}


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
    return await _attach_reconciliation(request, await mgr.status())


@router.put("")
async def gcp_billing_save(request: Request):
    async with domain_lock(request, "gcp_billing"):
        mgr = await _manager(request)
        current = mgr.config.model_dump(mode="json")
        value = await partial_body(request, current, GcpBillingConfig)
        return await _attach_reconciliation(request, await mgr.set_config(value))


@router.delete("")
async def gcp_billing_reset(request: Request):
    async with domain_lock(request, "gcp_billing"):
        mgr = await _manager(request)
        return await _attach_reconciliation(request, await mgr.set_config(None))


@router.post("/credentials")
async def gcp_billing_credentials(request: Request):
    value = await body(request, GcpBillingCredentialsUpdate)
    async with domain_lock(request, "gcp_billing"):
        mgr = await _manager(request)
        return await _attach_reconciliation(request, await mgr.update_credentials(value))


@router.post("/sync-bigquery")
async def gcp_billing_sync_bigquery(request: Request):
    request.app.state.admin_auth.costly()
    mgr = await _manager(request)
    transport = getattr(request.app.state, "gcp_transport", None)
    return await _attach_reconciliation(request, await mgr.sync_bigquery(transport=transport))


@router.post("/sync-monitoring")
async def gcp_billing_sync_monitoring(request: Request):
    request.app.state.admin_auth.costly()
    mgr = await _manager(request)
    transport = getattr(request.app.state, "gcp_transport", None)
    return await _attach_reconciliation(request, await mgr.sync_monitoring(transport=transport))
