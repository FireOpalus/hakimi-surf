"""Authenticated control plane for voice companion, voice gateway, and screen watch."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from .admin_common import body, domain_lock, service
from .screen_watch import ScreenWatchConfig
from .voice_companion import VoiceCompanionConfig
from .voice_gateway_admin import VoiceGatewayConfigUpdate

router = APIRouter(prefix="/admin/api/life")


@router.get("/voice-companion")
async def life_voice_companion_status(request: Request):
    svc = service(request)
    vc = getattr(svc, "voice_companion", None)
    if vc is None:
        raise HTTPException(503, "voice_companion_unavailable")
    return await vc.admin_status()


@router.put("/voice-companion")
async def life_voice_companion_update(request: Request):
    value = await body(request, VoiceCompanionConfig)
    svc = service(request)
    async with domain_lock(request, "voice_companion"):
        vc = getattr(svc, "voice_companion", None)
        if vc is None:
            raise HTTPException(503, "voice_companion_unavailable")
        return await vc.set_config(value)


@router.delete("/voice-companion")
async def life_voice_companion_reset(request: Request):
    svc = service(request)
    async with domain_lock(request, "voice_companion"):
        vc = getattr(svc, "voice_companion", None)
        if vc is None:
            raise HTTPException(503, "voice_companion_unavailable")
        return await vc.set_config(None)


@router.post("/voice-companion/disconnect")
async def life_voice_companion_disconnect(request: Request):
    svc = service(request)
    vc = getattr(svc, "voice_companion", None)
    if vc is None:
        raise HTTPException(503, "voice_companion_unavailable")
    return await vc.disconnect_active()


@router.get("/voice-companion/gateway")
async def life_voice_companion_gateway_status(request: Request):
    svc = service(request)
    gw = getattr(svc, "voice_gateway_admin", None)
    if gw is None:
        raise HTTPException(503, "voice_gateway_admin_unavailable")
    return await gw.status()


@router.put("/voice-companion/gateway")
async def life_voice_companion_gateway_update(request: Request):
    value = await body(request, VoiceGatewayConfigUpdate)
    svc = service(request)
    gw = getattr(svc, "voice_gateway_admin", None)
    if gw is None:
        raise HTTPException(503, "voice_gateway_admin_unavailable")
    async with domain_lock(request, "voice_gateway"):
        return await gw.update(value)


@router.delete("/voice-companion/gateway")
async def life_voice_companion_gateway_reset(request: Request):
    svc = service(request)
    gw = getattr(svc, "voice_gateway_admin", None)
    if gw is None:
        raise HTTPException(503, "voice_gateway_admin_unavailable")
    async with domain_lock(request, "voice_gateway"):
        return await gw.reset()


@router.get("/voice-companion/gateway/voices")
async def life_voice_companion_gateway_voices(request: Request):
    svc = service(request)
    gw = getattr(svc, "voice_gateway_admin", None)
    if gw is None:
        raise HTTPException(503, "voice_gateway_admin_unavailable")
    return await gw.list_breezeblue_voices()


@router.post("/voice-companion/gateway/test")
async def life_voice_companion_gateway_test(request: Request):
    request.app.state.admin_auth.costly()
    svc = service(request)
    gw = getattr(svc, "voice_gateway_admin", None)
    if gw is None:
        raise HTTPException(503, "voice_gateway_admin_unavailable")
    return await gw.test_tts(svc.voice_reply)


@router.get("/screen-watch")
async def life_screen_watch_status(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.admin_status()


@router.put("/screen-watch")
async def life_screen_watch_update(request: Request):
    value = await body(request, ScreenWatchConfig)
    svc = service(request)
    async with domain_lock(request, "screen_watch"):
        sw = getattr(svc, "screen_watch", None)
        if sw is None:
            raise HTTPException(503, "screen_watch_unavailable")
        return await sw.set_config(value)


@router.delete("/screen-watch")
async def life_screen_watch_reset(request: Request):
    svc = service(request)
    async with domain_lock(request, "screen_watch"):
        sw = getattr(svc, "screen_watch", None)
        if sw is None:
            raise HTTPException(503, "screen_watch_unavailable")
        return await sw.set_config(None)


@router.post("/screen-watch/clear")
async def life_screen_watch_clear(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.clear_frames_admin()


@router.post("/screen-watch/disconnect")
async def life_screen_watch_disconnect(request: Request):
    svc = service(request)
    sw = getattr(svc, "screen_watch", None)
    if sw is None:
        raise HTTPException(503, "screen_watch_unavailable")
    return await sw.disconnect_active()
