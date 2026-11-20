"""Authenticated control plane for unified cross-platform identity management."""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

from .admin_routes import body, service
from .identity import IdentityService, validate_platform_user
from .models import PlatformId
from .security import Rejected

router = APIRouter(prefix="/admin/api/identity")

ADMIN_PERSON_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class PersonUpdateInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    display_name: str | None = Field(default=None, min_length=1, max_length=64)
    l2_tags: list[str] | None = Field(default=None, max_length=16)
    muted: StrictBool | None = None

    @field_validator("display_name")
    @classmethod
    def _non_blank_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("display_name must not be blank")
        return stripped

    @model_validator(mode="after")
    def _require_any(self) -> PersonUpdateInput:
        if self.display_name is None and self.l2_tags is None and self.muted is None:
            raise ValueError("at least one field must be provided")
        return self


class MergePersonsInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    source_person_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]{1,64}$")
    target_person_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]{1,64}$")


class LinkAccountInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    person_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]{1,64}$")
    platform: PlatformId
    user_id: str = Field(min_length=1, max_length=128)
    nickname: str = Field(default="", max_length=80)

    @model_validator(mode="after")
    def _validate_account(self) -> LinkAccountInput:
        if self.user_id != self.user_id.strip():
            raise ValueError("user_id must not have surrounding whitespace")
        try:
            validate_platform_user(self.platform, self.user_id)
        except Rejected:
            raise ValueError("Invalid user_id for platform") from None
        return self


class UnlinkAccountInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    platform: PlatformId
    user_id: str = Field(min_length=1, max_length=128)

    @model_validator(mode="after")
    def _validate_account(self) -> UnlinkAccountInput:
        if self.user_id != self.user_id.strip():
            raise ValueError("user_id must not have surrounding whitespace")
        try:
            validate_platform_user(self.platform, self.user_id)
        except Rejected:
            raise ValueError("Invalid user_id for platform") from None
        return self


async def _identity(request: Request) -> tuple[IdentityService, Any, Any]:
    svc = service(request)
    sys_mod = getattr(getattr(svc, "modules", None), "systems", {}).get("identity")
    ident = getattr(sys_mod, "identity", None) if sys_mod is not None else None
    if ident is None:
        ident = getattr(svc.store.runtime, "identity", None)
    if ident is None:
        raise HTTPException(503, "identity_unavailable")
    await ident.initialize()
    aff = getattr(svc.store.runtime, "affection", None)
    if aff is not None and hasattr(aff, "initialize"):
        await aff.initialize()
    mod = getattr(svc, "moderation", None)
    return ident, aff, mod


def _map_rejected(exc: Rejected) -> HTTPException:
    code = str(exc)
    if code in {"person_not_found", "account_not_found"}:
        return HTTPException(404, code)
    if code in {"invalid_platform", "invalid_user_id", "invalid_link_source"}:
        return HTTPException(400, "invalid_admin_input")
    return HTTPException(409, code)


@router.get("/persons")
async def identity_list_persons(request: Request):
    ident, aff, _ = await _identity(request)
    return await ident.list_persons(affection=aff)


@router.put("/persons/{person_id}")
async def identity_update_person(request: Request, person_id: str):
    if not ADMIN_PERSON_ID_RE.fullmatch(person_id):
        raise HTTPException(400, "invalid_admin_input")
    value = await body(request, PersonUpdateInput)
    async with request.app.state.admin_config_lock:
        ident, aff, mod = await _identity(request)
        try:
            await ident.update_person(
                person_id,
                display_name=value.display_name,
                l2_tags=value.l2_tags,
                muted=value.muted,
                moderation=mod,
            )
        except Rejected as exc:
            raise _map_rejected(exc) from None
        return await ident.list_persons(affection=aff)


@router.post("/merge")
async def identity_merge_persons(request: Request):
    value = await body(request, MergePersonsInput)
    async with request.app.state.admin_config_lock:
        ident, aff, mod = await _identity(request)
        try:
            await ident.merge_persons(
                value.source_person_id,
                value.target_person_id,
                allow_owner=True,
                affection=aff,
                moderation=mod,
            )
        except Rejected as exc:
            raise _map_rejected(exc) from None
        return await ident.list_persons(affection=aff)


@router.post("/link")
async def identity_link_account(request: Request):
    value = await body(request, LinkAccountInput)
    async with request.app.state.admin_config_lock:
        ident, aff, mod = await _identity(request)
        try:
            await ident.link_account(
                value.person_id,
                value.platform,
                value.user_id,
                nickname=value.nickname,
                source="admin",
                affection=aff,
                moderation=mod,
            )
        except Rejected as exc:
            raise _map_rejected(exc) from None
        return await ident.list_persons(affection=aff)


@router.post("/unlink")
async def identity_unlink_account(request: Request):
    value = await body(request, UnlinkAccountInput)
    async with request.app.state.admin_config_lock:
        ident, aff, mod = await _identity(request)
        try:
            await ident.unlink_account(value.platform, value.user_id, moderation=mod)
        except Rejected as exc:
            raise _map_rejected(exc) from None
        return await ident.list_persons(affection=aff)
