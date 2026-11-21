"""WebSocket client for Hakimi's /ws/companion real-time voice protocol."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import logging
import re
import time
import urllib.request
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlparse

from .config import VoiceClientConfig

log = logging.getLogger("voice_client.ws")

FrameCallback = Callable[[dict[str, Any]], Awaitable[Any] | Any]
ConnectFactory = Callable[..., Any]

_TURN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class CompanionWsError(RuntimeError):
    """Error raised by CompanionWsClient without leaking secret tokens or URL paths."""


def _sanitize_text(text: str, config: VoiceClientConfig) -> str:
    """Redact WS token and secret URL path suffix from error/log text."""
    out = str(text)
    if config.ws_token:
        out = out.replace(config.ws_token, "[REDACTED]")
    if config.ws_url:
        out = out.replace(config.ws_url, "[REDACTED_URL]")
        parsed = urlparse(config.ws_url)
        if parsed.path and len(parsed.path) > 1:
            out = out.replace(parsed.path, "/[REDACTED_PATH]")
    return out


def _safe_endpoint_label(ws_url: str) -> str:
    """Return scheme://netloc only so the secret path suffix is never logged."""
    if not ws_url:
        return "(custom-factory)"
    parsed = urlparse(ws_url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return "(configured-ws)"


def _resolve_ws_proxy(ws_url: str) -> str | bool | None:
    """Resolve system/env proxy without failing on macOS SOCKS entries lacking python-socks."""
    if importlib.util.find_spec("python_socks") is not None:
        return True
    parsed = urlparse(ws_url)
    host = parsed.hostname or ""
    if host:
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        if urllib.request.proxy_bypass(f"{host}:{port}") or urllib.request.proxy_bypass(host):
            return None
    proxies = urllib.request.getproxies()
    keys = ("wss", "https", "http") if parsed.scheme == "wss" else ("ws", "https", "http")
    for key in keys:
        proxy_url = str(proxies.get(key) or "").strip()
        if not proxy_url:
            continue
        proxy_scheme = urlparse(proxy_url).scheme.lower()
        if proxy_scheme.startswith("socks"):
            continue
        if proxy_scheme in {"http", "https"}:
            return proxy_url
    return None


class CompanionWsClient:
    """Async client for Hakimi's /ws/companion bidirectional JSON frame protocol."""

    def __init__(
        self,
        config: VoiceClientConfig,
        connect_factory: ConnectFactory | None = None,
        on_turn_start: FrameCallback | None = None,
        on_speak_chunk: FrameCallback | None = None,
        on_turn_end: FrameCallback | None = None,
        on_error: FrameCallback | None = None,
        *,
        on_config_sync: FrameCallback | None = None,
        on_disconnect: Callable[[], Awaitable[Any] | Any] | None = None,
    ) -> None:
        self._config = config
        self._connect_factory = connect_factory
        self._on_turn_start = on_turn_start
        self._on_speak_chunk = on_speak_chunk
        self._on_turn_end = on_turn_end
        self._on_error = on_error
        self._on_config_sync = on_config_sync
        self._on_disconnect = on_disconnect

        self._ws: Any = None
        self._send_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._closing = False

        self._ready_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._stop_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._pong_waiters: deque[tuple[float, asyncio.Future[float]]] = deque()

        self.session_id: str = ""
        self.session_mode: str = config.session_mode
        self.server_config: dict[str, Any] | None = None
        self.active_turn_id: str = ""

    @property
    def connected(self) -> bool:
        """Whether the WebSocket connection and reader task are currently active."""
        return (
            self._ws is not None
            and not self._closing
            and self._reader_task is not None
            and not self._reader_task.done()
        )

    async def __aenter__(self) -> CompanionWsClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def connect(self) -> None:
        """Open the authenticated /ws/companion WebSocket and start reader/keepalive tasks."""
        if self.connected:
            return
        if self._ws is not None:
            await self.close()

        self._closing = False
        headers = {
            "Authorization": f"Bearer {self._config.ws_token}",
            "X-Companion-Adapter": self._config.adapter_id,
            "X-Companion-Client-Id": self._config.client_id,
        }

        if self._connect_factory is not None:
            try:
                sig = inspect.signature(self._connect_factory)
                needs_args = any(
                    p.default is inspect.Parameter.empty
                    and p.kind
                    in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    )
                    for p in sig.parameters.values()
                )
                raw = (
                    self._connect_factory(self._config.ws_url, additional_headers=headers)
                    if needs_args
                    else self._connect_factory()
                )
                self._ws = await raw if inspect.isawaitable(raw) else raw
            except Exception as exc:
                raise CompanionWsError(
                    f"Failed to open companion connection: {_sanitize_text(str(exc), self._config)}"
                ) from None
        else:
            self._config.require_ws()
            from websockets.asyncio.client import connect as ws_connect

            try:
                self._ws = await ws_connect(
                    self._config.ws_url,
                    additional_headers=headers,
                    max_size=4_194_304,
                    ping_interval=None,
                    proxy=_resolve_ws_proxy(self._config.ws_url),
                )
            except Exception as exc:
                raise CompanionWsError(
                    f"Failed to connect to {_safe_endpoint_label(self._config.ws_url)}: "
                    f"{_sanitize_text(str(exc), self._config)}"
                ) from None

        log.info(
            "Connected companion WS endpoint=%s adapter=%s client=%s",
            _safe_endpoint_label(self._config.ws_url),
            self._config.adapter_id,
            self._config.client_id,
        )
        self._reader_task = asyncio.create_task(self._reader_loop(), name="companion-ws-reader")
        if self._config.ping_interval_seconds > 0:
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="companion-ws-keepalive"
            )

    async def start_session(
        self,
        mode: str | None = None,
        game_title: str | None = None,
        *,
        timeout: float = 10.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        """Send session.start and wait for session.ready."""
        if not self.connected:
            raise CompanionWsError("WebSocket is not connected")
        if self._ready_waiter is not None and not self._ready_waiter.done():
            raise CompanionWsError("session.start already in progress")

        req_mode = (mode or self._config.session_mode).strip() or self._config.session_mode
        req_game = self._config.game_title if game_title is None else game_title

        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._ready_waiter = fut
        try:
            await self._send_frame(
                {
                    "type": "session.start",
                    "mode": req_mode,
                    "game_title": req_game,
                }
            )
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            if self._ready_waiter is fut:
                self._ready_waiter = None

    async def ping(self, timeout: float = 10.0) -> float:  # noqa: ASYNC109
        """Send ping, wait for pong, and return round-trip latency in milliseconds."""
        if not self.connected:
            raise CompanionWsError("WebSocket is not connected")
        fut: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        entry = (time.monotonic(), fut)
        self._pong_waiters.append(entry)
        try:
            await self._send_frame({"type": "ping"})
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            try:
                self._pong_waiters.remove(entry)
            except ValueError:
                pass

    async def send_speech_turn(
        self,
        text: str,
        *,
        turn_id: str | None = None,
        frames: list[dict[str, Any]] | None = None,
    ) -> str:
        """Send a speech.turn frame and return the assigned turn_id."""
        if not self.connected:
            raise CompanionWsError("WebSocket is not connected")
        clean = text.strip()
        if not clean:
            raise ValueError("speech text must not be empty")

        if turn_id is None:
            tid = f"t-{uuid.uuid4().hex[:12]}"
        else:
            tid = turn_id.strip()
            if not _TURN_ID_RE.fullmatch(tid):
                raise ValueError("turn_id must match ^[A-Za-z0-9_.-]{1,64}$")

        payload: dict[str, Any] = {
            "type": "speech.turn",
            "turn_id": tid,
            "text": clean,
            "speaker_id": self._config.speaker_id,
            "speaker_name": self._config.speaker_name,
        }
        if frames:
            payload["frames"] = frames

        self.active_turn_id = tid
        await self._send_frame(payload)
        return tid

    async def cancel_turn(self, turn_id: str = "") -> None:
        """Send turn.cancel for a specific turn_id or whichever turn is currently active."""
        if not self.connected:
            return
        await self._send_frame({"type": "turn.cancel", "turn_id": turn_id.strip()})

    async def stop_session(self, timeout: float = 5.0) -> dict[str, Any]:  # noqa: ASYNC109
        """Send session.stop and wait for session.ended."""
        if not self.connected:
            return {}
        if self._stop_waiter is not None and not self._stop_waiter.done():
            raise CompanionWsError("session.stop already in progress")

        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._stop_waiter = fut
        try:
            await self._send_frame({"type": "session.stop"})
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            if self._stop_waiter is fut:
                self._stop_waiter = None

    async def close(self) -> None:
        """Cancel background tasks and close the WebSocket cleanly."""
        self._closing = True
        tasks = [
            t for t in (self._keepalive_task, self._reader_task) if t is not None and not t.done()
        ]
        self._keepalive_task = None
        self._reader_task = None

        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                async with asyncio.timeout(2.0):
                    await ws.close()
            except Exception:
                pass

        self._fail_waiters(CompanionWsError("Companion WebSocket closed"))
        self.session_id = ""
        self.active_turn_id = ""

    async def _send_frame(self, frame: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            raise CompanionWsError("WebSocket is not connected")
        raw = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
        async with self._send_lock:
            ws = self._ws
            if ws is None:
                raise CompanionWsError("WebSocket is not connected")
            try:
                await ws.send(raw)
            except Exception as exc:
                raise CompanionWsError(
                    f"Failed to send WS frame: {_sanitize_text(str(exc), self._config)}"
                ) from None

    async def _invoke(
        self,
        cb: Callable[..., Any] | None,
        *args: Any,
    ) -> None:
        if cb is None:
            return
        try:
            res = cb(*args)
            if inspect.isawaitable(res):
                await res
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Companion callback error: %s",
                _sanitize_text(str(exc), self._config),
            )

    def _fail_waiters(self, exc: BaseException) -> None:
        if self._ready_waiter is not None and not self._ready_waiter.done():
            self._ready_waiter.set_exception(exc)
        if self._stop_waiter is not None and not self._stop_waiter.done():
            self._stop_waiter.set_exception(exc)
        while self._pong_waiters:
            _, fut = self._pong_waiters.popleft()
            if not fut.done():
                fut.set_exception(exc)

    async def _keepalive_loop(self) -> None:
        interval = max(0.01, float(self._config.ping_interval_seconds))
        try:
            while not self._closing and self._ws is not None:
                await asyncio.sleep(interval)
                if self._closing or not self.connected:
                    break
                try:
                    await self.ping(timeout=min(10.0, interval))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.debug(
                        "Companion keepalive ping failed: %s",
                        _sanitize_text(str(exc), self._config),
                    )
        except asyncio.CancelledError:
            raise

    async def _reader_loop(self) -> None:
        try:
            while not self._closing and self._ws is not None:
                raw = await self._ws.recv()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                frame = json.loads(raw)
                if not isinstance(frame, dict):
                    continue
                await self._dispatch_frame(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closing:
                log.info(
                    "Companion WS disconnected: %s",
                    _sanitize_text(str(exc), self._config),
                )
                self._fail_waiters(
                    CompanionWsError(
                        f"Companion WS disconnected: {_sanitize_text(str(exc), self._config)}"
                    )
                )
                await self._invoke(self._on_disconnect)
        finally:
            if not self._closing:
                self._fail_waiters(CompanionWsError("Companion WS reader ended"))

    async def _dispatch_frame(self, frame: dict[str, Any]) -> None:
        typ = str(frame.get("type") or "").strip()
        if typ == "session.ready":
            self.session_id = str(frame.get("session_id") or "")
            self.session_mode = str(frame.get("mode") or self.session_mode)
            cfg = frame.get("config")
            if isinstance(cfg, dict):
                self.server_config = cfg
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_result(frame)
        elif typ == "pong":
            if self._pong_waiters:
                sent_at, fut = self._pong_waiters.popleft()
                if not fut.done():
                    fut.set_result(max(0.0, (time.monotonic() - sent_at) * 1000.0))
        elif typ == "turn.start":
            self.active_turn_id = str(frame.get("turn_id") or "")
            await self._invoke(self._on_turn_start, frame)
        elif typ == "speak.chunk":
            await self._invoke(self._on_speak_chunk, frame)
        elif typ == "turn.end":
            tid = str(frame.get("turn_id") or "")
            if tid and tid == self.active_turn_id:
                self.active_turn_id = ""
            await self._invoke(self._on_turn_end, frame)
        elif typ == "session.ended":
            self.session_id = ""
            self.active_turn_id = ""
            if self._stop_waiter is not None and not self._stop_waiter.done():
                self._stop_waiter.set_result(frame)
        elif typ == "config.sync":
            cfg = frame.get("config")
            if isinstance(cfg, dict):
                self.server_config = cfg
            await self._invoke(self._on_config_sync, frame)
        elif typ == "error":
            reason = str(frame.get("reason") or "companion_error")
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_exception(CompanionWsError(reason))
            if self._stop_waiter is not None and not self._stop_waiter.done():
                self._stop_waiter.set_exception(CompanionWsError(reason))
            await self._invoke(self._on_error, frame)
