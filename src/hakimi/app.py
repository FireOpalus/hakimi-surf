import asyncio
import hmac
import json
import logging
import re
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.exception_handlers import http_exception_handler as default_http_exception_handler
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .admin_auth import AdminAuth
from .admin_routes import router as admin_router
from .affection_admin import router as affection_router
from .bili_account_admin import router as bili_account_router
from .colab_admin import router as colab_router
from .community_admin import router as community_router
from .config import Settings
from .gcp_admin import router as gcp_billing_router
from .gemini import GeminiGateway
from .identity_admin import router as identity_router
from .life_admin import router as life_router
from .meme_admin import router as meme_router
from .modules import platform_entries
from .modules.adapters import Sender, qq_message
from .modules.bilibili import Bilibili
from .modules.onebot_transport import HTTPTransport, WebSocketTransport, websocket_source_ip
from .node_transport import NodeTransport
from .plugin_admin import router as plugin_router
from .provider_admin import router as provider_router
from .security import Rejected, check_fresh, qq_signing_key, verify_onebot, verify_qq
from .service import Service
from .song_admin import router as song_router
from .store import Store


async def read_body(request: Request, settings: Settings) -> bytes:
    if request.headers.get("content-encoding", "identity") != "identity":
        raise HTTPException(415, "compressed requests are not supported")
    if request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
        raise HTTPException(415, "application/json required")
    body = bytearray()
    try:
        async with asyncio.timeout(5):
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > settings.max_body_bytes:
                    raise HTTPException(413, "request too large")
    except TimeoutError:
        raise HTTPException(408, "request timed out") from None
    return bytes(body)


def json_object(body: bytes) -> dict:
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            body,
            object_pairs_hook=unique_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, RecursionError, UnicodeError):
        raise HTTPException(400, "invalid JSON") from None


def create_app(
    settings: Settings | None = None, *, gateway_factory=GeminiGateway, start_workers: bool = True
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        s = settings or Settings()
        if (
            s.onebot_enabled
            and s.onebot_mode != "http"
            and len(s.onebot_ws_token.get_secret_value()) < 32
        ):
            raise RuntimeError("OneBot WebSocket requires a >=32-character access token")
        if s.node_ws_enabled and len(s.node_ws_token.get_secret_value()) < 32:
            raise RuntimeError("Node WebSocket requires a >=32-character access token")
        if s.companion_ws_enabled and len(s.companion_ws_token.get_secret_value()) < 32:
            raise RuntimeError("Companion WebSocket requires a >=32-character access token")
        if s.screen_ws_enabled and len(s.screen_ws_token.get_secret_value()) < 32:
            raise RuntimeError("Screen Watch WebSocket requires a >=32-character access token")
        # HTTP debug logs may contain tokens, IDs and request metadata.
        for name in ("httpx", "httpcore", "google.genai", "websockets", "aiohttp"):
            logging.getLogger(name).setLevel(logging.CRITICAL)
        store = Store(s)
        try:
            await store.open()
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10),
                trust_env=False,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            ) as http:
                ai = gateway_factory(s, store)
                onebot = (
                    HTTPTransport(s, http) if s.onebot_mode == "http" else WebSocketTransport(s)
                )
                node_transport = NodeTransport(s)
                async with AsyncExitStack() as resources:
                    resources.push_async_callback(ai.close)
                    resources.push_async_callback(node_transport.close)
                    if isinstance(onebot, WebSocketTransport):
                        resources.push_async_callback(onebot.close)
                    service = Service(
                        s,
                        store,
                        ai,
                        Bilibili(s, http, runtime=store.runtime, node_transport=node_transport),
                        Sender(s, http, onebot),
                        http=http,
                        node_transport=node_transport,
                    )
                    for module in service.modules.systems.values():
                        if module.module_id == "proactive_delivery":
                            continue
                        await module.initialize()
                    if "song_studio" not in service.modules.systems and hasattr(service, "songs"):
                        await service.songs.initialize()
                    if "voice_gateway" not in service.modules.systems and hasattr(
                        service, "voice_gateway_admin"
                    ):
                        await service.voice_gateway_admin.initialize()
                        await service.voice_gateway_admin.sync_to_gateway()
                    service.refresh_secrets()
                    if (cc := getattr(service, "colab_compute", None)) is not None:
                        resources.push_async_callback(cc.close)
                    await service.sessions.index_existing()
                    await service.moderation.initialize()
                    await service.longnick.initialize()
                    await service.bili_watcher.initialize()
                    service.refresh_secrets()
                    if "video_watch" not in service.modules.systems and hasattr(
                        service, "video_watch"
                    ):
                        await service.video_watch.initialize()
                    if "meme_library" not in service.modules.systems and hasattr(service, "memes"):
                        await service.memes.initialize()
                    await service.world_state.initialize()
                    await service.life_engine.initialize()
                    for module in service.modules.all():
                        await resources.enter_async_context(module.lifespan())
                    if "proactive_delivery" in service.modules.systems:
                        await service.modules.systems["proactive_delivery"].initialize()
                    else:
                        await service.proactive.initialize()
                    from .outgoing import recover_receipts

                    await recover_receipts(service)
                    await service.world_state.recover_actions()
                    if hasattr(ai, "cleanup_video_files"):
                        await ai.cleanup_video_files()
                    if isinstance(onebot, WebSocketTransport):
                        onebot.on_state = service.proactive.on_connection
                    app.state.settings, app.state.service = s, service
                    app.state.onebot = onebot
                    app.state.admin_auth = AdminAuth(s)
                    app.state.admin_action_lock = asyncio.Lock()
                    app.state.admin_config_lock = asyncio.Lock()
                    app.state.sandbox_lock = asyncio.Lock()
                    logging.getLogger("hakimi").addHandler(store.runtime.sink)
                    old_log_level = logging.getLogger("hakimi").level
                    logging.getLogger("hakimi").setLevel(logging.INFO)

                    async def ws_event(payload):
                        try:
                            return await service.accept_event(payload)
                        except (Rejected, KeyError, TypeError, ValueError, AttributeError):
                            return "ignored"

                    app.state.ws_event = ws_event
                    if start_workers:
                        service.start()
                        if (
                            s.onebot_enabled
                            and isinstance(onebot, WebSocketTransport)
                            and s.onebot_mode == "ws_forward"
                        ):
                            onebot.start_forward(ws_event)
                    try:
                        yield
                    finally:
                        await service.stop()
                        logging.getLogger("hakimi").removeHandler(store.runtime.sink)
                        logging.getLogger("hakimi").setLevel(old_log_level)
                        await store.runtime.flush()
        finally:
            await store.close()

    app = FastAPI(
        title="哈基米跨平台冲浪助手",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.include_router(admin_router)
    app.include_router(community_router)
    app.include_router(bili_account_router)
    app.include_router(meme_router)
    app.include_router(song_router)
    app.include_router(colab_router)
    app.include_router(provider_router)
    app.include_router(gcp_billing_router)
    app.include_router(affection_router)
    app.include_router(identity_router)
    app.include_router(plugin_router)
    app.include_router(life_router)
    for platform, entry in platform_entries():
        if platform.name == "weather":
            continue
        if (router := getattr(entry, "admin_router", None)) is not None:
            if any(not isinstance(route, APIRoute) for route in router.routes):
                raise ValueError(
                    f"{platform.path}: admin_router supports authenticated HTTP routes only"
                )
            app.include_router(router, prefix=f"/admin/api/life/modules/{platform.name}")

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(request: Request, exc: StarletteHTTPException):
        headers = getattr(exc, "headers", None)
        if headers and "X-Invalid-Field" in headers:
            return JSONResponse(
                {"detail": exc.detail, "invalid_field": headers["X-Invalid-Field"]},
                status_code=exc.status_code,
                headers=headers,
            )
        return await default_http_exception_handler(request, exc)

    @app.middleware("http")
    async def protect_admin(request: Request, call_next):
        if request.url.path != "/admin" and not request.url.path.startswith("/admin/"):
            return await call_next(request)
        try:
            request.app.state.admin_auth.check(request)
            response = await call_next(request)
        except HTTPException as exc:
            response = JSONResponse(
                {"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers
            )
        except Rejected as exc:
            response = JSONResponse({"detail": str(exc)}, status_code=409)
        except Exception:
            await request.app.state.service.store.runtime.event("admin_action_failed")
            response = JSONResponse({"detail": "admin_action_failed"}, status_code=503)
        response.headers.update(
            {
                "Cache-Control": "no-store",
                "Referrer-Policy": "no-referrer",
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
            }
        )
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; img-src blob:; media-src blob:; base-uri 'none'; "
            "frame-ancestors 'none'; form-action 'none'",
        )
        return response

    @app.get("/healthz")
    async def health(request: Request):
        service = request.app.state.service
        if any(t.done() for t in service.tasks):
            raise HTTPException(503, "worker unavailable")
        return {"status": "ok"}

    @app.post("/webhooks/onebot")
    async def onebot(request: Request):
        s = request.app.state.settings
        if not s.onebot_enabled or not s.onebot_webhook_enabled:
            raise HTTPException(404)
        body = await read_body(request, s)
        try:
            verify_onebot(
                body, request.headers.get("x-signature", ""), s.onebot_secret.get_secret_value()
            )
        except Rejected:
            raise HTTPException(401, "authentication failed") from None
        payload = json_object(body)
        try:
            # Signed body is authoritative; optional X-Self-ID must agree if present.
            if request.headers.get("x-self-id", str(payload.get("self_id"))) != str(
                payload.get("self_id")
            ):
                raise Rejected("wrong_bot")
            if str(payload.get("self_id")) != s.onebot_self_id:
                raise Rejected("wrong_bot")
        except (Rejected, KeyError, TypeError, ValueError, AttributeError):
            raise HTTPException(400, "invalid event") from None
        try:
            result = await request.app.state.service.accept_event(payload)
        except (Rejected, KeyError, TypeError, ValueError, AttributeError):
            raise HTTPException(400, "invalid event") from None
        if result == "full":
            raise HTTPException(503, "queue full", headers={"Retry-After": "5"})
        # Never use OneBot quick-action fields; only the authenticated outbox sends messages.
        return Response(status_code=204)

    @app.websocket("/ws/onebot{suffix:path}")
    async def onebot_reverse(websocket: WebSocket, suffix: str = ""):
        s = websocket.app.state.settings
        expected = "Bearer " + s.onebot_ws_token.get_secret_value()
        source_ip = websocket_source_ip(websocket, s)
        if (
            not s.onebot_enabled
            or s.onebot_mode != "ws_reverse"
            or websocket.url.path != s.onebot_ws_path
            or not hmac.compare_digest(
                websocket.headers.get("authorization", "").encode(), expected.encode()
            )
            or websocket.headers.get("x-self-id") != s.onebot_self_id
            or websocket.headers.get("x-client-role", "").lower() != "universal"
            or websocket.query_params
        ):
            logging.getLogger("hakimi").warning(
                "event=onebot_auth_rejected source_ip=%s", source_ip
            )
            await websocket.close(code=1008)
            return
        await websocket.accept()
        await websocket.app.state.onebot.serve(
            websocket.receive_text,
            websocket.send_text,
            websocket.close,
            websocket.app.state.ws_event,
            source_ip=source_ip,
        )

    @app.websocket("/ws/node{suffix:path}")
    async def node_reverse(websocket: WebSocket, suffix: str = ""):
        s = websocket.app.state.settings
        expected = "Bearer " + s.node_ws_token.get_secret_value()
        source_ip = websocket_source_ip(websocket, s)
        node_id = websocket.headers.get("x-node-id", "")
        region = websocket.headers.get("x-node-region", "")
        if (
            not s.node_ws_enabled
            or websocket.url.path != s.node_ws_path
            or not hmac.compare_digest(
                websocket.headers.get("authorization", "").encode(), expected.encode()
            )
            or not hmac.compare_digest(node_id.encode(), s.node_id.encode())
            or region != "CN"
            or websocket.query_params
        ):
            logging.getLogger("hakimi").warning("event=node_auth_rejected source_ip=%s", source_ip)
            await websocket.close(code=1008)
            return
        await websocket.accept()
        await websocket.app.state.service.node_transport.serve(
            websocket.receive_text,
            websocket.send_text,
            websocket.close,
            source_ip=source_ip,
            node_id=node_id,
            region=region,
        )

    @app.websocket("/ws/companion{suffix:path}")
    async def companion_reverse(websocket: WebSocket, suffix: str = ""):
        s = websocket.app.state.settings
        expected = "Bearer " + s.companion_ws_token.get_secret_value()
        source_ip = websocket_source_ip(websocket, s)
        adapter_id = websocket.headers.get("x-companion-adapter", "")
        client_id = websocket.headers.get("x-companion-client-id", "")
        if (
            not s.companion_ws_enabled
            or websocket.url.path != s.companion_ws_path
            or not hmac.compare_digest(
                websocket.headers.get("authorization", "").encode(), expected.encode()
            )
            or bool(websocket.query_params)
            or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", adapter_id)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", client_id)
        ):
            logging.getLogger("hakimi").warning(
                "event=companion_auth_rejected source_ip=%s", source_ip
            )
            await websocket.close(code=1008)
            return
        await websocket.accept()
        await websocket.app.state.service.voice_companion.serve(
            websocket.receive_text,
            websocket.send_text,
            websocket.close,
            source_ip=source_ip,
            adapter_id=adapter_id,
            client_id=client_id,
        )

    @app.websocket("/ws/screen{suffix:path}")
    async def screen_watch_reverse(websocket: WebSocket, suffix: str = ""):
        s = websocket.app.state.settings
        expected = "Bearer " + s.screen_ws_token.get_secret_value()
        source_ip = websocket_source_ip(websocket, s)
        adapter_id = websocket.headers.get("x-screen-adapter", "")
        client_id = websocket.headers.get("x-screen-client-id", "")
        if (
            not s.screen_ws_enabled
            or websocket.url.path != s.screen_ws_path
            or not hmac.compare_digest(
                websocket.headers.get("authorization", "").encode(), expected.encode()
            )
            or bool(websocket.query_params)
            or not re.fullmatch(r"[a-z][a-z0-9_-]{1,31}", adapter_id)
            or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", client_id)
        ):
            logging.getLogger("hakimi").warning(
                "event=screen_watch_auth_rejected source_ip=%s", source_ip
            )
            await websocket.close(code=1008)
            return
        await websocket.accept()
        await websocket.app.state.service.screen_watch.serve(
            websocket.receive_text,
            websocket.send_text,
            websocket.close,
            source_ip=source_ip,
            adapter_id=adapter_id,
            client_id=client_id,
        )

    @app.websocket("/ws/plugin{suffix:path}")
    async def plugin_reverse(websocket: WebSocket, suffix: str = ""):
        s = websocket.app.state.settings
        source_ip = websocket_source_ip(websocket, s)
        plugin_id = websocket.headers.get("x-plugin-id", "")
        auth_header = websocket.headers.get("authorization", "")
        token = auth_header[7:] if auth_header.startswith("Bearer ") else ""
        service = getattr(websocket.app.state, "service", None)
        bus = getattr(service, "plugin_bus", None) if service is not None else None
        cred = None
        if (
            s.plugin_bus_ws_enabled
            and websocket.url.path == s.plugin_bus_ws_path
            and not websocket.query_params
            and bus is not None
            and plugin_id
            and token
        ):
            cred = await bus.authenticate(plugin_id, token)
        if cred is None:
            logging.getLogger("hakimi").warning(
                "event=plugin_auth_rejected source_ip=%s", source_ip
            )
            await websocket.close(code=1008)
            return
        await websocket.accept()
        await bus.serve(
            websocket.receive_text,
            websocket.send_text,
            websocket.close,
            source_ip=source_ip,
            plugin_id=plugin_id,
        )

    @app.post("/webhooks/qq")
    async def qq(request: Request):
        s = request.app.state.settings
        if not s.qq_enabled:
            raise HTTPException(404)
        body = await read_body(request, s)
        if request.headers.get("x-bot-appid") != s.qq_app_id:
            raise HTTPException(401, "authentication failed")
        payload = json_object(body)
        if payload.get("op") == 13:
            # QQ's documented URL challenge has no signature headers. It cannot enqueue events.
            # Sign only the constrained challenge format, never arbitrary request bodies.
            try:
                data = payload["d"]
                token, timestamp = data["plain_token"], data["event_ts"]
                if (
                    not isinstance(token, str)
                    or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", token)
                    or not isinstance(timestamp, str)
                    or not re.fullmatch(r"[0-9]{10,11}", timestamp)
                ):
                    raise ValueError
                check_fresh(int(timestamp), s.signature_max_age)
                signature = (
                    qq_signing_key(s.qq_app_secret.get_secret_value())
                    .sign((timestamp + token).encode("ascii"))
                    .hex()
                )
                return {"plain_token": token, "signature": signature}
            except (Rejected, KeyError, ValueError, TypeError):
                raise HTTPException(400, "invalid challenge") from None
        try:
            verify_qq(
                body,
                request.headers.get("x-signature-timestamp", ""),
                request.headers.get("x-signature-ed25519", ""),
                s.qq_app_secret.get_secret_value(),
                s.signature_max_age,
            )
        except Rejected:
            raise HTTPException(401, "authentication failed") from None
        try:
            message = qq_message(payload, s)
        except (Rejected, KeyError, TypeError, ValueError, AttributeError):
            raise HTTPException(400, "invalid event") from None
        result = await request.app.state.service.accept(message)
        if result == "full":
            raise HTTPException(503, "queue full", headers={"Retry-After": "5"})
        return {"op": 12}

    return app
