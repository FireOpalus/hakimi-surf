"""Authenticated control plane for the user affection scoring system."""

import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .admin_routes import body, service
from .affection import (
    PLATFORM_ID_RE,
    AffectionConfig,
    AffectionManager,
    _validate_platform_user,
)
from .models import PlatformId
from .security import Rejected

router = APIRouter(prefix="/admin/api/affection")


class UserAffectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    user_id: str = Field(min_length=1, max_length=128)
    platform: PlatformId = "onebot"
    score: int = Field(ge=0, le=120)
    nickname: str = Field(default="", max_length=80)
    reason: str = Field(default="admin_set", min_length=1, max_length=64)

    @model_validator(mode="after")
    def _validate_user_id(self) -> "UserAffectionInput":
        if self.user_id != self.user_id.strip():
            raise ValueError("Invalid user_id for platform")
        try:
            _validate_platform_user(self.platform, self.user_id)
        except Rejected:
            raise ValueError("Invalid user_id for platform") from None
        return self


async def _manager(request: Request) -> AffectionManager:
    store = service(request).store
    mgr = getattr(store.runtime, "affection", None)
    if mgr is None:
        lock = getattr(store.runtime, "_affection_init_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            store.runtime._affection_init_lock = lock
        async with lock:
            mgr = getattr(store.runtime, "affection", None)
            if mgr is None:
                mgr = AffectionManager(store)
                store.runtime.affection = mgr
    await mgr.initialize()
    return mgr


@router.get("")
async def affection_status(request: Request):
    mgr = await _manager(request)
    return await mgr.status()


@router.put("")
async def affection_save(request: Request):
    from .app import json_object, read_body

    raw = json_object(await read_body(request, request.app.state.settings))
    if not raw:
        raise HTTPException(400, "invalid_admin_input")
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        current = mgr.config.model_dump(mode="json")
        try:
            value = AffectionConfig.model_validate({**current, **raw})
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        return await mgr.set_config(value)


@router.delete("")
async def affection_reset(request: Request):
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        return await mgr.set_config(None)


@router.put("/users")
async def affection_set_user(request: Request):
    value = await body(request, UserAffectionInput)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        await mgr.set_user_score(
            value.user_id,
            value.score,
            platform=value.platform,
            nickname=value.nickname,
            reason=value.reason,
            moderation=getattr(svc, "moderation", None),
        )
        return await mgr.status()


def _resolve_platform_user(user_id: str, platform: str = "onebot") -> tuple[str, str]:
    if not PLATFORM_ID_RE.fullmatch(platform):
        raise HTTPException(400, "invalid_admin_input")
    resolved_platform = platform
    resolved_uid = user_id
    if platform != "onebot":
        if user_id.startswith(platform + ":"):
            resolved_uid = user_id[len(platform) + 1 :]
    elif ":" in user_id:
        maybe_plat, maybe_uid = user_id.split(":", 1)
        if not PLATFORM_ID_RE.fullmatch(maybe_plat):
            raise HTTPException(400, "invalid_admin_input")
        resolved_platform, resolved_uid = maybe_plat, maybe_uid
    if resolved_uid != resolved_uid.strip():
        raise HTTPException(400, "invalid_admin_input")
    try:
        _validate_platform_user(resolved_platform, resolved_uid)
    except Rejected:
        raise HTTPException(400, "invalid_admin_input") from None
    return resolved_platform, resolved_uid


@router.get("/history")
async def affection_recent_history(request: Request, limit: int = 50):
    clamped_limit = max(1, min(200, limit))
    mgr = await _manager(request)
    return {"items": await mgr.recent_history(limit=clamped_limit)}


@router.get("/users/{user_id}/history")
async def affection_user_history(
    request: Request,
    user_id: str,
    platform: str = "onebot",
    limit: int = 30,
):
    resolved_platform, resolved_uid = _resolve_platform_user(user_id, platform)
    clamped_limit = max(1, min(200, limit))
    mgr = await _manager(request)
    return {
        "platform": resolved_platform,
        "user_id": resolved_uid,
        "items": await mgr.user_history(resolved_platform, resolved_uid, limit=clamped_limit),
    }


@router.delete("/users/{user_id}")
async def affection_delete_user(
    request: Request,
    user_id: str,
    platform: str = "onebot",
):
    resolved_platform, resolved_uid = _resolve_platform_user(user_id, platform)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        await mgr.delete_user_score(
            resolved_uid,
            platform=resolved_platform,
            moderation=getattr(svc, "moderation", None),
        )
        return await mgr.status()
