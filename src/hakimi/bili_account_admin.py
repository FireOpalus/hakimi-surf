"""Authenticated control plane; credential values never appear in GET responses."""

from fastapi import APIRouter, HTTPException, Request

from .admin_routes import body, service
from .modules.bili_watcher import BiliPolicy, CredentialUpdate
from .video_policy import VideoWatchConfig

router = APIRouter(prefix="/admin/api/bili-account")


@router.get("")
async def status(request: Request):
    return await service(request).bili_watcher.status()


@router.put("")
async def policy(request: Request):
    value = await body(request, BiliPolicy)
    return await service(request).bili_watcher.set_policy(value)


@router.delete("")
async def reset(request: Request):
    return await service(request).bili_watcher.set_policy(None)


@router.post("/credentials")
async def credentials(request: Request):
    value = await body(request, CredentialUpdate)
    return await service(request).bili_watcher.update_credentials(value)


@router.get("/video-watch")
async def video_watch_status(request: Request):
    return await service(request).video_watch.status()


@router.put("/video-watch")
async def video_watch_config(request: Request):
    value = await body(request, VideoWatchConfig)
    return await service(request).video_watch.set_config(value)


@router.delete("/video-watch")
async def video_watch_reset(request: Request):
    return await service(request).video_watch.set_config(None)


@router.post("/{action}")
async def action(request: Request, action: str):
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        svc = service(request)
        watcher = svc.bili_watcher
        if action in {"test", "run"} and svc.modules.inactive_reason("bilibili_surf"):
            raise HTTPException(409, "module_disabled")
        if action == "test":
            return await watcher.nav()
        if action == "run":
            return await watcher.run_once()
        raise HTTPException(404)
