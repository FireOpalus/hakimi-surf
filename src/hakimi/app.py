import asyncio
import hashlib
import hmac
import json
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.responses import JSONResponse

from .adapters import Sender, qq_message
from .admin_auth import AdminAuth
from .admin_routes import router as admin_router
from .affection_admin import router as affection_router
from .bili_account_admin import router as bili_account_router
from .bilibili import Bilibili
from .community_admin import router as community_router
from .config import Settings
from .drive_client import FolderDrive
from .gcp_admin import router as gcp_billing_router
from .gemini import GeminiGateway
from .life_admin import router as life_router
from .meme_admin import router as meme_router
from .node_transport import NodeTransport
from .notebook_snapshot import NotebookSnapshot
from .onebot_transport import HTTPTransport, WebSocketTransport, websocket_source_ip
from .provider_admin import router as provider_router
from .roaming_logger import RoamingLogger
from .security import Rejected, check_fresh, qq_signing_key, verify_onebot, verify_qq
from .service import Service
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
        if not (s.onebot_enabled or s.qq_enabled):
            raise RuntimeError("Enable and configure at least one QQ adapter")
        if s.onebot_mode != "http" and len(s.onebot_ws_token.get_secret_value()) < 32:
            raise RuntimeError("OneBot WebSocket requires a >=32-character access token")
        if s.node_ws_enabled and len(s.node_ws_token.get_secret_value()) < 32:
            raise RuntimeError("Node WebSocket requires a >=32-character access token")
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
                try:
                    diary = None
                    drive = None
                    if s.roaming_enabled:
                        drive = FolderDrive(
                            s, http, store.vault.digest("diary-writer", s.namespace)
                        )
                        diary = RoamingLogger(s, store, drive, gateway=ai)
                        await diary.initialize()
                    service = Service(
                        s,
                        store,
                        ai,
                        Bilibili(s, http, runtime=store.runtime, node_transport=node_transport),
                        Sender(s, http, onebot),
                        diary=diary,
                        notebook=NotebookSnapshot(s, http, ai, store),
                        drive=drive,
                        node_transport=node_transport,
                    )
                    await service.sessions.index_existing()
                    await service.proactive.initialize()
                    await service.moderation.initialize()
                    await service.longnick.initialize()
                    await service.bili_watcher.initialize()
                    await service.video_watch.initialize()
                    await service.memes.initialize()
                    await service.world_state.initialize()
                    await service.life_engine.initialize()
                    from .outgoing import recover_receipts

                    await recover_receipts(service)
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
                        if isinstance(onebot, WebSocketTransport) and s.onebot_mode == "ws_forward":
                            onebot.start_forward(ws_event)
                    try:
                        yield
                    finally:
                        await service.stop()
                        logging.getLogger("hakimi").removeHandler(store.runtime.sink)
                        logging.getLogger("hakimi").setLevel(old_log_level)
                        await store.runtime.flush()
                finally:
                    if isinstance(onebot, WebSocketTransport):
                        await onebot.close()
                    await node_transport.close()
                    await ai.close()
        finally:
            await store.close()

    app = FastAPI(
        title="哈基米跨平台冲浪助手",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.include_router(admin_router)
    app.include_router(community_router)
    app.include_router(bili_account_router)
    app.include_router(meme_router)
    app.include_router(provider_router)
    app.include_router(gcp_billing_router)
    app.include_router(affection_router)
    app.include_router(life_router)
    html = Path(__file__).with_name("admin_page.html").read_text()
    import base64

    hashes = {}
    for tag in ("script", "style"):
        content = re.search(rf"<{tag}>(.*?)</{tag}>", html, re.S)[1]
        hashes[tag] = base64.b64encode(hashlib.sha256(content.encode()).digest()).decode()

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
                "Content-Security-Policy": (
                    f"default-src 'none'; script-src 'sha256-{hashes['script']}'; "
                    f"style-src 'sha256-{hashes['style']}'; connect-src 'self'; img-src blob:; "
                    "base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
                ),
            }
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
