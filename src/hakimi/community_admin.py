"""ADMIN_TOKEN-protected human controls, mounted under the existing admin middleware."""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .admin_common import body, domain_lock, service
from .identity import PLATFORM_ID_RE, validate_platform_user
from .modules.longnick import LongnickConfig
from .security import Rejected

router = APIRouter(prefix="/admin/api")


def _parse_mute_target_format(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("Invalid moderation target")
    if ":" in value:
        platform, _, uid = value.partition(":")
        if (
            not platform
            or not uid
            or platform != platform.strip()
            or uid != uid.strip()
            or not PLATFORM_ID_RE.fullmatch(platform)
        ):
            raise ValueError("Invalid moderation target")
        try:
            clean_uid = validate_platform_user(platform, uid)
        except Rejected:
            raise ValueError("Invalid moderation target") from None
        return platform, clean_uid
    try:
        clean_uid = validate_platform_user("onebot", value)
    except Rejected:
        raise ValueError("Invalid moderation target") from None
    return "onebot", clean_uid


class MuteInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    user_id: str = Field(min_length=1, max_length=180)
    muted: bool = True
    delete_friend: bool = False

    @model_validator(mode="after")
    def _validate_target(self) -> "MuteInput":
        _parse_mute_target_format(self.user_id)
        return self


class ReviewInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    approve: bool
    delete_friend: bool = False


class SignatureInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(min_length=1, max_length=2000)


@router.get("/moderation")
async def moderation_status(request: Request):
    return await service(request).moderation.status()


@router.put("/moderation/users")
async def mute(request: Request):
    value = await body(request, MuteInput)
    mod = service(request).moderation
    platform, uid = mod.parse_target(value.user_id)
    if platform == "onebot":
        if value.delete_friend:
            request.app.state.admin_auth.costly()
        return await mod.set_muted(uid, value.muted, delete_friend=value.delete_friend)
    return await mod.set_platform_muted(
        platform,
        uid,
        value.muted,
        delete_friend=value.delete_friend,
        source="manual",
    )


@router.delete("/moderation/mute/{user_id}")
async def unmute(request: Request, user_id: str):
    try:
        platform, uid = _parse_mute_target_format(user_id)
    except ValueError:
        raise HTTPException(400, "invalid_admin_input") from None
    mod = service(request).moderation
    platform, uid = mod.parse_target(f"{platform}:{uid}")
    if platform == "onebot":
        return await mod.set_muted(uid, False)
    return await mod.set_platform_muted(platform, uid, False, source="manual")


@router.post("/moderation/proposals/{proposal}")
async def review(request: Request, proposal: str):
    value = await body(request, ReviewInput)
    if value.delete_friend:
        request.app.state.admin_auth.costly()
    return await service(request).moderation.review(
        proposal, value.approve, delete_friend=value.delete_friend
    )


@router.get("/longnick")
async def longnick_status(request: Request):
    return await service(request).longnick.status()


@router.put("/longnick/config")
async def longnick_config(request: Request):
    value = await body(request, LongnickConfig)
    async with domain_lock(request, "community"):
        return await service(request).longnick.set_config(value)


@router.delete("/longnick/config")
async def longnick_reset(request: Request):
    async with domain_lock(request, "community"):
        return await service(request).longnick.set_config(None)


@router.post("/longnick")
async def longnick_update(request: Request):
    value = await body(request, SignatureInput)
    request.app.state.admin_auth.costly()
    return await service(request).longnick.update(value.text)
