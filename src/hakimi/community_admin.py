"""ADMIN_TOKEN-protected human controls, mounted under the existing admin middleware."""

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from .admin_routes import body, service
from .modules.longnick import LongnickConfig

router = APIRouter(prefix="/admin/api")


class MuteInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    user_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    muted: bool = True
    delete_friend: bool = False


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
    if value.delete_friend:
        request.app.state.admin_auth.costly()
    return await service(request).moderation.set_muted(
        value.user_id, value.muted, delete_friend=value.delete_friend
    )


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
    return await service(request).longnick.set_config(await body(request, LongnickConfig))


@router.delete("/longnick/config")
async def longnick_reset(request: Request):
    return await service(request).longnick.set_config(None)


@router.post("/longnick")
async def longnick_update(request: Request):
    value = await body(request, SignatureInput)
    request.app.state.admin_auth.costly()
    return await service(request).longnick.update(value.text)
