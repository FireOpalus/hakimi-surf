"""WebSocket client for the universal Hakimi Voice Gateway (/ws/voice)."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import inspect
import json
import logging
import re
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from .companion_ws import _resolve_ws_proxy, _safe_endpoint_label
from .config import VoiceClientConfig

log = logging.getLogger("voice_client.gateway_ws")

FrameCallback = Callable[[dict[str, Any]], Awaitable[Any] | Any]
TtsChunkCallback = Callable[[str, int, str, bytes], Awaitable[Any] | Any]
SynthesizeChunkCallback = Callable[..., Awaitable[Any] | Any]
ConnectFactory = Callable[..., Any]

_TURN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_GATEWAY_MAX_WS_FRAME_BYTES = 16_777_216


class VoiceGatewayError(RuntimeError):
    """Error raised by VoiceGatewayWsClient without leaking secret tokens or URL paths."""


def _sanitize_gateway_text(text: str, config: VoiceClientConfig) -> str:
    """Redact gateway/WS tokens and secret URL path suffixes from error or log text."""
    out = str(text)
    for token in (config.gateway_token, config.ws_token):
        if token:
            out = out.replace(token, "[REDACTED]")
    for raw_url in (config.gateway_url, config.ws_url):
        if not raw_url:
            continue
        out = out.replace(raw_url, "[REDACTED_URL]")
        parsed = urlparse(raw_url)
        if parsed.path and len(parsed.path) > 1:
            out = out.replace(parsed.path, "/[REDACTED_PATH]")
    return out


def _accepts_two_positional_args(cb: Callable[..., Any]) -> bool:
    try:
        sig = inspect.signature(cb)
    except (TypeError, ValueError):
        return False
    positional = 0
    for param in sig.parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


@dataclass
class _SynthWaiter:
    req_id: str
    fmt: str
    future: asyncio.Future[tuple[bytes, str]]
    on_chunk: SynthesizeChunkCallback | None = None
    on_chunk_takes_fmt: bool = False
    chunks: list[bytes] = field(default_factory=list)


class VoiceGatewayWsClient:
    """Async client for the universal Voice Gateway mixed JSON/binary WS protocol."""

    def __init__(
        self,
        config: VoiceClientConfig,
        connect_factory: ConnectFactory | None = None,
        on_stt_final: FrameCallback | None = None,
        on_turn_start: FrameCallback | None = None,
        on_text_chunk: FrameCallback | None = None,
        on_tts_start: FrameCallback | None = None,
        on_tts_chunk: TtsChunkCallback | None = None,
        on_tts_end: FrameCallback | None = None,
        on_turn_end: FrameCallback | None = None,
        on_error: FrameCallback | None = None,
        *,
        on_disconnect: Callable[[], Awaitable[Any] | Any] | None = None,
    ) -> None:
        self._config = config
        self._connect_factory = connect_factory
        self._on_stt_final = on_stt_final
        self._on_turn_start = on_turn_start
        self._on_text_chunk = on_text_chunk
        self._on_tts_start = on_tts_start
        self._on_tts_chunk = on_tts_chunk
        self._on_tts_end = on_tts_end
        self._on_turn_end = on_turn_end
        self._on_error = on_error
        self._on_disconnect = on_disconnect

        self._ws: Any = None
        self._send_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._closing = False

        self._ready_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._stop_waiter: asyncio.Future[dict[str, Any]] | None = None
        self._pong_waiters: dict[str, tuple[float, asyncio.Future[dict[str, Any]]]] = {}
        self._pong_fifo: deque[str] = deque()
        self._stt_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._stt_fifo: deque[str] = deque()
        self._synth_waiters: dict[str, _SynthWaiter] = {}
        self._active_tts_stream: dict[str, Any] | None = None

        self.session_id: str = ""
        self.session_mode: str = config.session_mode
        self.output_format: str = config.gateway_output_format or config.tts_format
        self.stt_provider: str = config.stt_provider
        self.tts_provider: str = config.tts_provider
        self.tts_model: str = ""
        self.upstream_config: dict[str, Any] | None = None
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

    async def __aenter__(self) -> VoiceGatewayWsClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def connect(self) -> None:
        """Open the authenticated Voice Gateway WebSocket and start reader/keepalive tasks."""
        if self.connected:
            return
        if self._ws is not None:
            await self.close()

        self._closing = False
        headers = {
            "Authorization": f"Bearer {self._config.gateway_token}",
            "X-Voice-Adapter": self._config.adapter_id,
            "X-Voice-Client-Id": self._config.client_id,
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
                    self._connect_factory(self._config.gateway_url, additional_headers=headers)
                    if needs_args
                    else self._connect_factory()
                )
                self._ws = await raw if inspect.isawaitable(raw) else raw
            except Exception as exc:
                raise VoiceGatewayError(
                    "Failed to open voice gateway connection: "
                    f"{_sanitize_gateway_text(str(exc), self._config)}"
                ) from None
        else:
            self._config.require_gateway()
            from websockets.asyncio.client import connect as ws_connect

            try:
                self._ws = await ws_connect(
                    self._config.gateway_url,
                    additional_headers=headers,
                    max_size=_GATEWAY_MAX_WS_FRAME_BYTES,
                    ping_interval=None,
                    proxy=_resolve_ws_proxy(self._config.gateway_url),
                )
            except Exception as exc:
                raise VoiceGatewayError(
                    f"Failed to connect to {_safe_endpoint_label(self._config.gateway_url)}: "
                    f"{_sanitize_gateway_text(str(exc), self._config)}"
                ) from None

        log.info(
            "Connected voice gateway endpoint=%s adapter=%s client=%s",
            _safe_endpoint_label(self._config.gateway_url),
            self._config.adapter_id,
            self._config.client_id,
        )
        self._reader_task = asyncio.create_task(self._reader_loop(), name="voice-gateway-reader")
        if self._config.ping_interval_seconds > 0:
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="voice-gateway-keepalive"
            )

    async def start_session(
        self,
        mode: str | None = None,
        game_title: str | None = None,
        *,
        output_format: str | None = None,
        tts_buffer_mode: str | None = None,
        timeout: float = 10.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        """Send session.start and wait for session.ready."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        if self._ready_waiter is not None and not self._ready_waiter.done():
            raise VoiceGatewayError("session.start already in progress")

        req_mode = (mode or self._config.session_mode).strip() or self._config.session_mode
        req_game = self._config.game_title if game_title is None else game_title
        req_fmt = (
            self._config.gateway_output_format if output_format is None else output_format
        ).strip()
        raw_buf_mode = (tts_buffer_mode or self._config.tts_buffer_mode).strip()
        req_buf_mode = raw_buf_mode or self._config.tts_buffer_mode

        payload: dict[str, Any] = {
            "type": "session.start",
            "mode": req_mode,
            "game_title": req_game,
            "tts_buffer_mode": req_buf_mode,
        }
        if req_fmt:
            payload["output_format"] = req_fmt

        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._ready_waiter = fut
        try:
            await self._send_frame(payload)
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            if self._ready_waiter is fut:
                self._ready_waiter = None

    async def ping_info(self, timeout: float = 10.0) -> dict[str, Any]:  # noqa: ASYNC109
        """Send ping, wait for pong, and return RTT plus pong metadata."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        req_id = f"p-{uuid.uuid4().hex[:10]}"
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pong_waiters[req_id] = (time.monotonic(), fut)
        self._pong_fifo.append(req_id)
        try:
            await self._send_frame({"type": "ping", "req_id": req_id})
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            self._pong_waiters.pop(req_id, None)
            with contextlib.suppress(ValueError):
                self._pong_fifo.remove(req_id)

    async def ping(self, timeout: float = 10.0) -> float:  # noqa: ASYNC109
        """Send ping, wait for pong, and return round-trip latency in milliseconds."""
        info = await self.ping_info(timeout=timeout)
        return float(info["rtt_ms"])

    def _normalize_turn_id(self, turn_id: str | None) -> str:
        if turn_id is None:
            return f"t-{uuid.uuid4().hex[:12]}"
        tid = turn_id.strip()
        if not _TURN_ID_RE.fullmatch(tid):
            raise ValueError("turn_id must match ^[A-Za-z0-9_.-]{1,64}$")
        return tid

    def _resolve_speaker(self, speaker_id: str | None, speaker_name: str | None) -> tuple[str, str]:
        sid = self._config.speaker_id if speaker_id is None else str(speaker_id).strip()
        sname = self._config.speaker_name if speaker_name is None else str(speaker_name).strip()
        return sid, sname

    async def send_audio_turn(
        self,
        audio_bytes: bytes,
        *,
        fmt: str = "wav",
        sample_rate: int = 16000,
        channels: int = 1,
        turn_id: str | None = None,
        frames: list[dict[str, Any]] | None = None,
        speaker_id: str | None = None,
        speaker_name: str | None = None,
        stream: bool = False,
        chunk_size: int = 32768,
    ) -> str:
        """Send one audio turn (base64 audio.turn or binary audio.stream.*) and return turn_id."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        if not audio_bytes:
            raise ValueError("audio_bytes must not be empty")

        tid = self._normalize_turn_id(turn_id)
        sid, sname = self._resolve_speaker(speaker_id, speaker_name)

        if not stream:
            payload: dict[str, Any] = {
                "type": "audio.turn",
                "turn_id": tid,
                "format": fmt,
                "sample_rate": int(sample_rate),
                "channels": int(channels),
                "audio_b64": base64.b64encode(audio_bytes).decode("ascii"),
                "speaker_id": sid,
                "speaker_name": sname,
            }
            if frames:
                payload["frames"] = frames
            self.active_turn_id = tid
            await self._send_frame(payload)
            return tid

        stream_id = f"s-{uuid.uuid4().hex[:12]}"
        start_frame: dict[str, Any] = {
            "type": "audio.stream.start",
            "stream_id": stream_id,
            "turn_id": tid,
            "format": fmt,
            "sample_rate": int(sample_rate),
            "channels": int(channels),
            "speaker_id": sid,
            "speaker_name": sname,
        }
        if frames:
            start_frame["frames"] = frames
        end_frame = {"type": "audio.stream.end", "stream_id": stream_id, "turn_id": tid}

        step = max(1024, int(chunk_size))
        self.active_turn_id = tid
        async with self._send_lock:
            ws = self._ws
            if ws is None:
                raise VoiceGatewayError("WebSocket is not connected")
            try:
                await ws.send(json.dumps(start_frame, ensure_ascii=False, separators=(",", ":")))
                for offset in range(0, len(audio_bytes), step):
                    await ws.send(audio_bytes[offset : offset + step])
                await ws.send(json.dumps(end_frame, ensure_ascii=False, separators=(",", ":")))
            except Exception as exc:
                detail = _sanitize_gateway_text(str(exc), self._config)
                raise VoiceGatewayError(f"Failed to send audio stream: {detail}") from None
        return tid

    async def send_text_turn(
        self,
        text: str,
        *,
        turn_id: str | None = None,
        frames: list[dict[str, Any]] | None = None,
        speaker_id: str | None = None,
        speaker_name: str | None = None,
    ) -> str:
        """Send a text.turn frame and return the assigned turn_id."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        clean = text.strip()
        if not clean:
            raise ValueError("speech text must not be empty")

        tid = self._normalize_turn_id(turn_id)
        sid, sname = self._resolve_speaker(speaker_id, speaker_name)
        payload: dict[str, Any] = {
            "type": "text.turn",
            "turn_id": tid,
            "text": clean,
            "speaker_id": sid,
            "speaker_name": sname,
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

    async def transcribe_once(
        self,
        audio_bytes: bytes,
        *,
        fmt: str = "wav",
        sample_rate: int = 16000,
        channels: int = 1,
        timeout: float = 30.0,  # noqa: ASYNC109
    ) -> str:
        """Send a standalone stt.transcribe RPC (no session required) and return text."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        if not audio_bytes:
            return ""
        req_id = f"stt-{uuid.uuid4().hex[:12]}"
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._stt_waiters[req_id] = fut
        self._stt_fifo.append(req_id)
        try:
            await self._send_frame(
                {
                    "type": "stt.transcribe",
                    "req_id": req_id,
                    "format": fmt,
                    "sample_rate": int(sample_rate),
                    "channels": int(channels),
                    "audio_b64": base64.b64encode(audio_bytes).decode("ascii"),
                }
            )
            async with asyncio.timeout(timeout):
                result = await fut
            return str(result.get("text") or "").strip()
        finally:
            self._stt_waiters.pop(req_id, None)
            with contextlib.suppress(ValueError):
                self._stt_fifo.remove(req_id)

    async def synthesize_once(
        self,
        text: str,
        *,
        output_format: str | None = None,
        on_chunk: SynthesizeChunkCallback | None = None,
        timeout: float = 60.0,  # noqa: ASYNC109
    ) -> tuple[bytes, str]:
        """Send standalone tts.synthesize RPC (no session required) and return (bytes, fmt)."""
        if not self.connected:
            raise VoiceGatewayError("WebSocket is not connected")
        clean = text.strip()
        if not clean:
            raise ValueError("tts text must not be empty")

        req_id = f"tts-{uuid.uuid4().hex[:12]}"
        req_fmt = (
            (self._config.gateway_output_format if output_format is None else output_format)
            .strip()
            .lower()
        )
        fut: asyncio.Future[tuple[bytes, str]] = asyncio.get_running_loop().create_future()
        waiter = _SynthWaiter(
            req_id=req_id,
            fmt=req_fmt or self.output_format or "mp3",
            future=fut,
            on_chunk=on_chunk,
            on_chunk_takes_fmt=_accepts_two_positional_args(on_chunk) if on_chunk else False,
        )
        self._synth_waiters[req_id] = waiter
        try:
            payload: dict[str, Any] = {
                "type": "tts.synthesize",
                "req_id": req_id,
                "text": clean,
            }
            if req_fmt:
                payload["output_format"] = req_fmt
            await self._send_frame(payload)
            async with asyncio.timeout(timeout):
                return await fut
        finally:
            self._synth_waiters.pop(req_id, None)

    async def stop_session(self, timeout: float = 5.0) -> dict[str, Any]:  # noqa: ASYNC109
        """Send session.stop and wait for session.ended."""
        if not self.connected:
            return {}
        if self._stop_waiter is not None and not self._stop_waiter.done():
            raise VoiceGatewayError("session.stop already in progress")

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

        self._fail_waiters(VoiceGatewayError("Voice Gateway WebSocket closed"))
        self.session_id = ""
        self.active_turn_id = ""
        self._active_tts_stream = None

    async def _send_frame(self, frame: dict[str, Any]) -> None:
        if self._ws is None:
            raise VoiceGatewayError("WebSocket is not connected")
        raw = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
        async with self._send_lock:
            ws = self._ws
            if ws is None:
                raise VoiceGatewayError("WebSocket is not connected")
            try:
                await ws.send(raw)
            except Exception as exc:
                raise VoiceGatewayError(
                    f"Failed to send WS frame: {_sanitize_gateway_text(str(exc), self._config)}"
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
                "Voice Gateway callback error: %s",
                _sanitize_gateway_text(str(exc), self._config),
            )

    def _fail_waiters(self, exc: BaseException) -> None:
        if self._ready_waiter is not None and not self._ready_waiter.done():
            self._ready_waiter.set_exception(exc)
        if self._stop_waiter is not None and not self._stop_waiter.done():
            self._stop_waiter.set_exception(exc)
        for _, fut in list(self._pong_waiters.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pong_waiters.clear()
        self._pong_fifo.clear()
        for fut in list(self._stt_waiters.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._stt_waiters.clear()
        self._stt_fifo.clear()
        for waiter in list(self._synth_waiters.values()):
            if not waiter.future.done():
                waiter.future.set_exception(exc)
        self._synth_waiters.clear()

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
                        "Voice Gateway keepalive ping failed: %s",
                        _sanitize_gateway_text(str(exc), self._config),
                    )
        except asyncio.CancelledError:
            raise

    async def _reader_loop(self) -> None:
        try:
            while not self._closing and self._ws is not None:
                raw = await self._ws.recv()
                if isinstance(raw, (bytes, bytearray)):
                    await self._handle_binary_frame(bytes(raw))
                    continue
                if not isinstance(raw, str):
                    continue
                frame = json.loads(raw)
                if not isinstance(frame, dict):
                    continue
                await self._dispatch_frame(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._closing:
                sanitized = _sanitize_gateway_text(str(exc), self._config)
                log.info("Voice Gateway WS disconnected: %s", sanitized)
                self._fail_waiters(VoiceGatewayError(f"Voice Gateway WS disconnected: {sanitized}"))
                await self._invoke(self._on_disconnect)
        finally:
            if not self._closing:
                self._fail_waiters(VoiceGatewayError("Voice Gateway WS reader ended"))

    async def _handle_binary_frame(self, chunk: bytes) -> None:
        ctx = self._active_tts_stream
        if ctx is None or not chunk:
            return
        req_id = str(ctx.get("req_id") or "")
        if req_id and req_id in self._synth_waiters:
            waiter = self._synth_waiters[req_id]
            waiter.chunks.append(chunk)
            if waiter.on_chunk is not None:
                if waiter.on_chunk_takes_fmt:
                    await self._invoke(waiter.on_chunk, chunk, waiter.fmt)
                else:
                    await self._invoke(waiter.on_chunk, chunk)
            return
        await self._invoke(
            self._on_tts_chunk,
            str(ctx.get("turn_id") or ""),
            int(ctx.get("seq") or 0),
            str(ctx.get("format") or self.output_format or "mp3"),
            chunk,
        )

    async def _dispatch_frame(self, frame: dict[str, Any]) -> None:
        typ = str(frame.get("type") or "").strip()
        if typ == "session.ready":
            self.session_id = str(frame.get("session_id") or "")
            self.session_mode = str(frame.get("mode") or self.session_mode)
            if frame.get("output_format"):
                self.output_format = str(frame["output_format"])
            if frame.get("stt_provider"):
                self.stt_provider = str(frame["stt_provider"])
            if frame.get("tts_provider"):
                self.tts_provider = str(frame["tts_provider"])
            if frame.get("tts_model"):
                self.tts_model = str(frame["tts_model"])
            cfg = frame.get("upstream_config")
            if isinstance(cfg, dict):
                self.upstream_config = cfg
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_result(frame)
        elif typ == "pong":
            req_id = str(frame.get("req_id") or "")
            entry = self._pong_waiters.pop(req_id, None) if req_id else None
            if entry is not None:
                with contextlib.suppress(ValueError):
                    self._pong_fifo.remove(req_id)
            elif self._pong_fifo:
                fallback_id = self._pong_fifo.popleft()
                entry = self._pong_waiters.pop(fallback_id, None)
            if entry is not None:
                sent_at, fut = entry
                if not fut.done():
                    rtt_ms = max(0.0, (time.monotonic() - sent_at) * 1000.0)
                    fut.set_result({**frame, "rtt_ms": rtt_ms})
        elif typ == "stt.result":
            req_id = str(frame.get("req_id") or "")
            fut = self._stt_waiters.pop(req_id, None) if req_id else None
            if fut is not None:
                with contextlib.suppress(ValueError):
                    self._stt_fifo.remove(req_id)
            elif self._stt_fifo:
                fallback_id = self._stt_fifo.popleft()
                fut = self._stt_waiters.pop(fallback_id, None)
            if fut is not None and not fut.done():
                fut.set_result(frame)
        elif typ == "stt.final":
            await self._invoke(self._on_stt_final, frame)
        elif typ == "turn.start":
            self.active_turn_id = str(frame.get("turn_id") or "")
            await self._invoke(self._on_turn_start, frame)
        elif typ == "text.chunk":
            await self._invoke(self._on_text_chunk, frame)
        elif typ == "tts.start":
            req_id = str(frame.get("req_id") or "")
            fmt = str(frame.get("format") or self.output_format or "mp3").strip().lower() or "mp3"
            self._active_tts_stream = {
                "req_id": req_id,
                "turn_id": str(frame.get("turn_id") or ""),
                "seq": int(frame.get("seq") or 0),
                "format": fmt,
            }
            if req_id and req_id in self._synth_waiters:
                self._synth_waiters[req_id].fmt = fmt
            else:
                await self._invoke(self._on_tts_start, frame)
        elif typ == "tts.end":
            ctx = self._active_tts_stream
            self._active_tts_stream = None
            req_id = str(frame.get("req_id") or (ctx.get("req_id") if ctx else "") or "")
            waiter = self._synth_waiters.pop(req_id, None) if req_id else None
            if waiter is not None:
                if not waiter.future.done():
                    status = str(frame.get("status") or "").lower()
                    if status == "error":
                        reason = _sanitize_gateway_text(
                            str(frame.get("detail") or frame.get("reason") or "tts_error"),
                            self._config,
                        )
                        waiter.future.set_exception(VoiceGatewayError(reason))
                    else:
                        waiter.future.set_result((b"".join(waiter.chunks), waiter.fmt))
            else:
                await self._invoke(self._on_tts_end, frame)
        elif typ == "turn.end":
            tid = str(frame.get("turn_id") or "")
            if tid and tid == self.active_turn_id:
                self.active_turn_id = ""
            await self._invoke(self._on_turn_end, frame)
        elif typ == "session.ended":
            self.session_id = ""
            self.active_turn_id = ""
            self._active_tts_stream = None
            if self._stop_waiter is not None and not self._stop_waiter.done():
                self._stop_waiter.set_result(frame)
        elif typ == "error":
            raw_reason = str(frame.get("detail") or frame.get("reason") or "gateway_error")
            reason = _sanitize_gateway_text(raw_reason, self._config)
            req_id = str(frame.get("req_id") or "")
            if req_id:
                if (
                    self._active_tts_stream is not None
                    and self._active_tts_stream.get("req_id") == req_id
                ):
                    self._active_tts_stream = None
                pong_entry = self._pong_waiters.pop(req_id, None)
                if pong_entry is not None:
                    with contextlib.suppress(ValueError):
                        self._pong_fifo.remove(req_id)
                    if not pong_entry[1].done():
                        pong_entry[1].set_exception(VoiceGatewayError(reason))
                stt_fut = self._stt_waiters.pop(req_id, None)
                if stt_fut is not None:
                    with contextlib.suppress(ValueError):
                        self._stt_fifo.remove(req_id)
                    if not stt_fut.done():
                        stt_fut.set_exception(VoiceGatewayError(reason))
                synth_waiter = self._synth_waiters.pop(req_id, None)
                if synth_waiter is not None and not synth_waiter.future.done():
                    synth_waiter.future.set_exception(VoiceGatewayError(reason))
            if self._ready_waiter is not None and not self._ready_waiter.done():
                self._ready_waiter.set_exception(VoiceGatewayError(reason))
            if self._stop_waiter is not None and not self._stop_waiter.done():
                self._stop_waiter.set_exception(VoiceGatewayError(reason))
            await self._invoke(self._on_error, frame)
