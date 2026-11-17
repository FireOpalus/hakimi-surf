"""Authenticated control plane for the user affection scoring system."""

import asyncio
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .admin_routes import body, service
from .affection import (
    ONEBOT_USER_RE,
    QQ_USER_RE,
    AffectionConfig,
    AffectionManager,
)

router = APIRouter(prefix="/admin/api/affection")


class UserAffectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    user_id: str = Field(min_length=1, max_length=128)
    platform: Literal["onebot", "qq"] = "onebot"
    score: int = Field(ge=0, le=120)
    nickname: str = Field(default="", max_length=80)
    reason: str = Field(default="admin_set", min_length=1, max_length=64)

    @model_validator(mode="after")
    def _validate_user_id(self) -> "UserAffectionInput":
        pattern = ONEBOT_USER_RE if self.platform == "onebot" else QQ_USER_RE
        if not pattern.fullmatch(self.user_id):
            raise ValueError("Invalid user_id for platform")
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


@router.delete("/users/{user_id}")
async def affection_delete_user(
    request: Request,
    user_id: str,
    platform: Literal["onebot", "qq"] = "onebot",
):
    resolved_platform = platform
    resolved_uid = user_id
    if user_id.startswith(("onebot:", "qq:")):
        resolved_platform, resolved_uid = user_id.split(":", 1)
    pattern = ONEBOT_USER_RE if resolved_platform == "onebot" else QQ_USER_RE
    if not pattern.fullmatch(resolved_uid):
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    async with request.app.state.admin_config_lock:
        mgr = await _manager(request)
        await mgr.delete_user_score(
            resolved_uid,
            platform=resolved_platform,
            moderation=getattr(svc, "moderation", None),
        )
        return await mgr.status()
