"""Authenticated control plane for the user affection scoring system."""

import asyncio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .admin_common import body, domain_lock, partial_body, service
from .affection import (
    PLATFORM_ID_RE,
    AffectionConfig,
    AffectionManager,
    _validate_platform_user,
)
from .models import PlatformId
from .security import Rejected

router = APIRouter(prefix="/admin/api/affection")

_DEFAULT_RECENT_HISTORY_LIMIT = 50
_DEFAULT_USER_HISTORY_LIMIT = 30


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
    async with domain_lock(request, "affection"):
        mgr = await _manager(request)
        current = mgr.config.model_dump(mode="json")
        value = await partial_body(request, current, AffectionConfig)
        return await mgr.set_config(value)


@router.delete("")
async def affection_reset(request: Request):
    async with domain_lock(request, "affection"):
        mgr = await _manager(request)
        return await mgr.set_config(None)


@router.put("/users")
async def affection_set_user(request: Request):
    value = await body(request, UserAffectionInput)
    svc = service(request)
    async with domain_lock(request, "affection"):
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
async def affection_recent_history(request: Request):
    mgr = await _manager(request)
    return {"items": await mgr.recent_history(limit=_DEFAULT_RECENT_HISTORY_LIMIT)}


@router.get("/users/{user_id}/history")
async def affection_user_history(request: Request, user_id: str):
    resolved_platform, resolved_uid = _resolve_platform_user(user_id)
    mgr = await _manager(request)
    return {
        "platform": resolved_platform,
        "user_id": resolved_uid,
        "items": await mgr.user_history(
            resolved_platform, resolved_uid, limit=_DEFAULT_USER_HISTORY_LIMIT
        ),
    }


@router.delete("/users/{user_id}")
async def affection_delete_user(request: Request, user_id: str):
    resolved_platform, resolved_uid = _resolve_platform_user(user_id)
    svc = service(request)
    async with domain_lock(request, "affection"):
        mgr = await _manager(request)
        await mgr.delete_user_score(
            resolved_uid,
            platform=resolved_platform,
            moderation=getattr(svc, "moderation", None),
        )
        return await mgr.status()
