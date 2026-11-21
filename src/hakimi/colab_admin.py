"""Authenticated control plane for Google Colab GPU scheduling and Drive inbox."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from .admin_common import body, domain_lock, partial_body, service
from .colab_compute import ColabComputeHub, ColabConfig, GpuType
from .security import Rejected

router = APIRouter(prefix="/admin/api/colab")

_STATUS_MAP = {
    "colab_unconfigured": 503,
    "colab_worker_unavailable": 503,
    "colab_drive_not_configured": 503,
    "colab_job_not_found": 404,
    "colab_invalid_job": 400,
    "colab_drive_invalid_file": 400,
    "song_source_required": 400,
    "colab_disabled": 409,
    "colab_daily_job_limit": 409,
    "colab_daily_cu_budget": 409,
    "colab_job_limit_reached": 409,
    "colab_job_not_cancellable": 409,
}


class ColabJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: Literal["rvc_cover", "rvc_train"]
    payload: dict[str, Any]
    gpu: GpuType | None = None


def _compute(request: Request) -> ColabComputeHub:
    svc = service(request)
    comp = getattr(svc, "colab_compute", None)
    if comp is None:
        mod = svc.modules.systems.get("colab_compute")
        comp = getattr(mod, "compute", None) if mod is not None else None
    if comp is None:
        raise HTTPException(503, "colab_unconfigured")
    return comp


def _raise_http(exc: Rejected) -> None:
    code = str(exc)
    raise HTTPException(_STATUS_MAP.get(code, 409), code) from None


@router.get("")
async def colab_overview(request: Request):
    compute = _compute(request)
    return await compute.admin_overview()


@router.put("")
async def colab_update_config(request: Request):
    compute = _compute(request)
    async with domain_lock(request, "colab_compute"):
        await compute.initialize()
        current = compute.config.model_dump(mode="json")
        cfg = await partial_body(request, current, ColabConfig)
        await compute.set_config(cfg)
        return await compute.admin_overview()


@router.delete("")
async def colab_reset_config(request: Request):
    compute = _compute(request)
    async with domain_lock(request, "colab_compute"):
        await compute.set_config(None)
        return await compute.admin_overview()


@router.post("/jobs")
async def colab_submit_job(request: Request):
    compute = _compute(request)
    req = await body(request, ColabJobRequest)
    async with domain_lock(request, "colab_compute"):
        try:
            return await compute.submit_job(
                req.job_type,
                req.payload,
                requester="admin",
                gpu=req.gpu,
            )
        except Rejected as exc:
            _raise_http(exc)


@router.post("/jobs/{job_id}/cancel")
async def colab_cancel_job(request: Request, job_id: str):
    compute = _compute(request)
    async with domain_lock(request, "colab_compute"):
        try:
            return await compute.cancel_job(job_id)
        except Rejected as exc:
            _raise_http(exc)


@router.post("/terminate-sessions")
async def colab_terminate_sessions(request: Request):
    request.app.state.admin_auth.costly()
    compute = _compute(request)
    async with domain_lock(request, "colab_compute"):
        try:
            return await compute.emergency_terminate_sessions()
        except Rejected as exc:
            _raise_http(exc)


@router.get("/drive/inbox")
async def colab_drive_inbox(request: Request):
    compute = _compute(request)
    try:
        items = await compute.list_drive_inbox()
    except Rejected as exc:
        _raise_http(exc)
    return {"items": items}
