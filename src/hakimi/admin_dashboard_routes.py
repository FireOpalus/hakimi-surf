"""Authenticated dashboard, overview, logs, diaries, notebook, subscriptions, delivery routes."""

import asyncio
import json
import time
from datetime import date
from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from .admin_common import body, domain_lock, service
from .models import Message
from .provider_routing import ProviderView
from .security import Rejected
from .thinking_policy import preferences

router = APIRouter(prefix="/admin")


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TextInput(Input):
    text: str = Field(max_length=4000)


class Subscription(Input):
    kind: Literal["up", "live"]
    uid: str = Field(pattern=r"^[1-9][0-9]{0,19}$")


def owner_target(s):
    if not s.onebot_enabled or not s.owner_qq_id:
        raise Rejected("configure_owner_qq_id")
    return Message(
        platform="onebot",
        bot_id=s.onebot_self_id,
        user_id=s.owner_qq_id,
        kind="private",
        conversation_id=s.owner_qq_id,
        event_id="admin-patrol",
        text="",
        addressed=True,
        created_at=time.time(),
    )


def _admin_page_url() -> str:
    from . import admin_routes

    return admin_routes.ADMIN_PAGE_URL


@router.head("")
@router.head("/")
async def page_access():
    """The private frontend checks access here before serving the login document."""
    return Response(status_code=204)


@router.get("")
@router.get("/")
async def page(request: Request):
    """Check console access or serve the independent UI through a legacy SSH tunnel."""
    if request.headers.get("accept") == "application/json":
        return Response(status_code=204)
    url = _admin_page_url()
    http = getattr(service(request), "http", None)
    try:
        if http is not None:
            doc = await http.get(url, headers={"Host": "127.0.0.1"}, timeout=5)
        else:
            async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
                doc = await client.get(url, headers={"Host": "127.0.0.1"})
    except httpx.RequestError:
        raise HTTPException(503, "admin_ui_unavailable") from None
    policy = doc.headers.get("Content-Security-Policy")
    if doc.status_code != 200 or not policy:
        raise HTTPException(503, "admin_ui_unavailable")
    return Response(
        doc.content, media_type="text/html", headers={"Content-Security-Policy": policy}
    )


def onebot_connection(request: Request, svc) -> dict:
    transport = request.app.state.onebot
    ws = (
        "http_mode"
        if svc.s.onebot_mode == "http"
        else "online"
        if transport.ready.is_set()
        else "reconnecting"
    )
    return {
        "onebot": ws,
        "onebot_detail": transport.status()
        if hasattr(transport, "status")
        else {"online": None, "last_heartbeat_at": None, "source_ip": None, "connected_at": None},
    }


@router.get("/api/dashboard")
async def dashboard(request: Request):
    snapshot = await _dashboard(request, "full")
    snapshot.update(onebot_connection(request, service(request)))
    return snapshot


@router.get("/api/global/dashboard")
async def global_dashboard(request: Request):
    return await _dashboard(request, "global")


@router.get("/api/global/overview")
async def global_overview(request: Request):
    return await _dashboard(request, "global", overview=True)


async def _dashboard(request: Request, view: ProviderView, *, overview: bool = False):
    svc = service(request)
    await svc.store.runtime.flush()
    usage = await (svc.store.usage_overview() if overview else svc.store.usage())
    async with svc.store.lock:
        metrics = await svc.store.all(
            "SELECT code,source,n FROM daily_metrics WHERE day=?", (usage["day_utc"],)
        )
        count = await svc.store.one(
            "SELECT n FROM counters WHERE key=?", (f"global:{int(time.time() // 60)}",)
        )
        circuit = bool(await svc.store.one("SELECT 1 FROM meta WHERE key='budget_overrun'"))
    notebook = svc.notebook.status() if svc.notebook else {"enabled": False}
    notebook.pop("text", None)
    routing = svc.store.runtime.provider_routing
    await routing.initialize()
    return {
        "uptime_seconds": int(time.monotonic() - svc.started_at),
        "primary_model": preferences(svc.s).primary_model,
        "pipeline": {
            "gate": sum(b.phase == "gate" for b in svc.store.groups.active.values()),
            "generating": sum(b.phase == "generating" for b in svc.store.groups.active.values()),
            "hot_sessions": len(svc.hot.rings),
            "image_cache_bytes": getattr(svc.images, "cache_bytes", 0),
        },
        "usage": usage,
        "budget_status": await svc.store.budget_status(),
        "daily_limit_usd": str(svc.s.daily_usd),
        "user_limit_usd": str(svc.s.user_daily_usd),
        "patrol_limit_usd": str(svc.s.patrol_daily_usd),
        "memory_limit_usd": str(svc.s.memory_daily_usd),
        "proactive_limit_usd": str(svc.s.proactive_daily_usd),
        "admin_limit_usd": str(svc.s.admin_daily_usd),
        "minute_requests": count[0] if count else 0,
        "minute_limit": svc.s.global_requests_per_minute,
        "budget_circuit_open": circuit,
        "metrics": [dict(row) for row in metrics],
        "notebook": notebook,
        "rest_hours": svc.store.runtime.rest_hours_status(),
        "provider_routing": routing.config.for_admin(view),
    }


@router.get("/api/logs")
async def logs(request: Request):
    store = service(request).store
    await store.runtime.flush()
    async with store.lock:
        rows = await store.all(
            "SELECT at,code,source,diagnostics FROM audit_events ORDER BY id DESC LIMIT 50"
        )
    return [
        {
            "at": row["at"],
            "code": row["code"],
            "source": row["source"],
            **(
                {"provider_diagnostics": json.loads(row["diagnostics"])}
                if row["diagnostics"]
                else {}
            ),
        }
        for row in rows
    ]


@router.post("/api/actions/{action}")
async def action(request: Request, action: str):
    svc = service(request)
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        if action == "patrol":
            if svc.modules.inactive_reason("bilibili_surf"):
                raise HTTPException(409, "module_disabled")
            await svc.poll_once(force=True)
            return {
                "status": "polled_updates_queued",
                "note": "模型摘要按原工作队列处理；首次巡逻只建立基线。",
            }
        if action == "diary":
            now = time.time()
            await svc.diary.prepare_due(now=now, force=True)
            state = await svc.diary_sync.sync_due(now=now, force=True)
            notes = {
                "uploaded": "本地日记已整理，Drive 已确认同步。",
                "unchanged": "本地日记已整理，Drive 上已是当前版本。",
                "unconfigured": "本地日记已整理，Drive 等待配置。",
                "pending": "本地日记已整理，Drive 同步待重试。",
            }
            return {"status": state, "note": notes[state]}
        if action == "notebook":
            return await svc.notebook.refresh()
        raise HTTPException(404)


def owner_delivery_for(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None or svc.modules.chat_platforms.get(svc.proactive.platform) is not module:
        raise HTTPException(404, "owner_delivery_not_assigned")
    return svc.proactive


@router.get("/api/life/modules/{module_id}/owner-delivery")
async def module_delivery_status(request: Request, module_id: str):
    return await owner_delivery_for(request, module_id).status()


@router.get("/api/proactive")
async def proactive_status(request: Request):
    if service(request).proactive.platform != "onebot":
        return {"assigned": False}
    return await module_delivery_status(request, "qq_social")


@router.post("/api/proactive/{action}")
async def proactive_action(request: Request, action: str):
    return await module_delivery_action(request, "qq_social", action)


@router.post("/api/life/modules/{module_id}/owner-delivery/{action}")
async def module_delivery_action(request: Request, module_id: str, action: str):
    dm = owner_delivery_for(request, module_id)
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        if action == "test":
            return await dm.test_dm()
        if action == "digest":
            return await dm.manual_digest()
        raise HTTPException(404)


@router.get("/api/notebook")
async def notebook(request: Request):
    svc = service(request)
    status = dict(svc.notebook.status())
    cloud = getattr(svc, "cloud_memory", None)
    if cloud is not None and hasattr(cloud, "admin_preview"):
        status["antigravity_memory"] = await asyncio.to_thread(cloud.admin_preview)
    return status


@router.put("/api/notebook")
async def notebook_write(request: Request):
    data = await body(request, TextInput)
    request.app.state.admin_auth.costly()
    async with domain_lock(request, "notebook"):
        return await service(request).notebook.update_local(data.text)


@router.get("/api/subscriptions")
async def subscriptions(request: Request):
    store = service(request).store
    result = [
        {"id": row["id"], "kind": "up", "uid": row["mid"], "next_poll": row["next_poll"]}
        for row in await store.subscriptions()
    ]
    async with store.lock:
        rows = await store.all("SELECT room_id,last_status,next_poll FROM live_watches")
    return result + [
        {
            "id": row["room_id"],
            "kind": "live",
            "uid": row["room_id"],
            "last_status": row["last_status"],
            "next_poll": row["next_poll"],
        }
        for row in rows
    ]


@router.post("/api/subscriptions")
async def add_subscription(request: Request):
    data = await body(request, Subscription)
    svc = service(request)
    target = owner_target(svc.s)
    async with domain_lock(request, "subscriptions"):
        current = await subscriptions(request)
        if len(current) >= svc.s.max_subscriptions:
            raise Rejected("subscription_limit")
        if data.kind == "up":
            await svc.store.add_subscription(target, data.uid)
        else:
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT OR IGNORE INTO live_watches(room_id,target) VALUES (?,?)",
                    (data.uid, svc.store.vault.seal("live:" + data.uid, target.model_dump())),
                )
    return {"status": "saved"}


@router.delete("/api/subscriptions/{kind}/{key}")
async def delete_subscription(request: Request, kind: str, key: str):
    store = service(request).store
    async with domain_lock(request, "subscriptions"):
        if kind == "up":
            row = next((r for r in await store.subscriptions() if r["id"] == key), None)
            if not row:
                raise HTTPException(404)
            await store.remove_subscription(row["subject"], row["mid"])
        elif kind == "live":
            async with store.transaction():
                await store.db.execute("DELETE FROM live_watches WHERE room_id=?", (key,))
        else:
            raise HTTPException(404)
    return {"status": "deleted"}


@router.get("/api/system-modules")
async def system_modules(request: Request):
    modules = list(service(request).modules.systems.values())
    statuses = await asyncio.gather(*(module.status() for module in modules)) if modules else []
    return [
        {
            "id": module.module_id,
            "title": module.display_name,
            "description": module.description,
            "panel": module.admin_panel,
            "status": status,
        }
        for module, status in zip(modules, statuses, strict=True)
    ]


@router.get("/api/drive-sync")
async def drive_sync(request: Request):
    return await service(request).drive_sync.overview()


@router.get("/api/diaries")
async def diaries(request: Request):
    svc = service(request)
    async with svc.store.lock:
        return [
            dict(row)
            for row in await svc.store.all(
                "SELECT d.day,s.slot,COALESCE(s.next_attempt,0) AS next_attempt,"
                "COALESCE(s.failures,0) AS failures,s.last_error,s.last_success "
                "FROM roaming_days d LEFT JOIN roaming_sync s ON d.day=s.day "
                "ORDER BY d.day DESC LIMIT 8"
            )
        ]


@router.get("/api/diaries/status")
async def diary_status(request: Request):
    return await service(request).diary_sync.status()


@router.get("/api/diaries/{day}")
async def diary(request: Request, day: str):
    try:
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError
    except ValueError:
        raise HTTPException(400, "invalid_day") from None
    svc = service(request)
    return {"name": day + ".md", "markdown": await svc.diary.render(day)}
