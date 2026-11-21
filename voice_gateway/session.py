"""Session coordinator bridging client WS audio/RPCs with upstream /ws/companion."""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import dataclasses
import json
import logging
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from voice_client.companion_ws import CompanionWsClient, ConnectFactory
from voice_client.providers import STTProvider, TTSProvider

from .audio_codec import normalize_input_to_wav, output_format_meta, transcode_audio_stream
from .config import VoiceGatewayConfig
from .protocol import (
    DEFAULT_ADAPTER_ID,
    DEFAULT_CLIENT_ID,
    ProtocolError,
    build_error_frame,
    decode_json_frame,
    generate_id,
    validate_id,
)

log = logging.getLogger("voice_gateway.session")


def _join_turn_chunks(parts: list[str]) -> str:
    """Join streamed text chunks while avoiding extra spaces between CJK characters."""
    out: list[str] = []
    for raw in parts:
        piece = str(raw or "").strip()
        if not piece:
            continue
        if out:
            prev = out[-1][-1]
            curr = piece[0]
            if prev.isascii() and prev.isalnum() and curr.isascii() and curr.isalnum():
                out.append(" ")
        out.append(piece)
    return "".join(out)


async def _chunk_bytes(data: bytes, chunk_size: int = 16384) -> AsyncIterator[bytes]:
    for idx in range(0, len(data), chunk_size):
        chunk = data[idx : idx + chunk_size]
        if chunk:
            yield chunk


def _optional_str(value: Any, *, max_len: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProtocolError("invalid_field", "Expected string field")
    clean = value.strip()
    if not clean:
        return None
    return clean[:max_len]


def _decode_audio_b64(frame: dict[str, Any], *, max_audio_bytes: int) -> bytes:
    raw = frame.get("audio_b64")
    if raw is None:
        raw = frame.get("data")
    if not isinstance(raw, str) or not raw.strip():
        raise ProtocolError("missing_audio", "Missing base64 audio payload")
    compact = "".join(raw.split())
    max_b64_len = ((max_audio_bytes + 2) // 3) * 4 + 4
    if len(compact) > max_b64_len:
        raise ProtocolError("audio_too_large", "Audio payload exceeds max_audio_bytes")
    try:
        decoded = base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError):
        raise ProtocolError("invalid_audio_b64", "Invalid base64 audio payload") from None
    if not decoded:
        raise ProtocolError("empty_audio", "Audio payload must not be empty")
    if len(decoded) > max_audio_bytes:
        raise ProtocolError("audio_too_large", "Audio payload exceeds max_audio_bytes")
    return decoded


@dataclass
class _UploadStream:
    stream_id: str
    turn_id: str
    fmt: str
    sample_rate: int
    channels: int
    speaker_id: str | None
    speaker_name: str | None
    frames: list[Any] | None
    buffer: bytearray = field(default_factory=bytearray)
    aborted: bool = False


@dataclass
class _TurnState:
    turn_id: str
    sent_at: float
    stt_ms: float | None = None
    text_parts: list[str] = field(default_factory=list)
    first_tts_ttfb_ms: float | None = None
    cancelled: bool = False
    ended: bool = False


@dataclass
class _TtsJob:
    turn_id: str
    seq: int
    text: str
    output_format: str
    req_id: str = ""
    final_turn_payload: dict[str, Any] | None = None
    cancelled: bool = False


@dataclass
class _TtsExecutionState:
    started_wire: bool = False
    total_bytes: int = 0
    ttfb_ms: float = 0.0


class VoiceGatewaySession:
    """Manages one client WS session, local STT/TTS pipelines, and upstream CompanionWsClient."""

    def __init__(
        self,
        config: VoiceGatewayConfig,
        *,
        stt_provider: STTProvider,
        tts_provider: TTSProvider,
        send_text: Callable[[str], Awaitable[Any]],
        send_bytes: Callable[[bytes], Awaitable[Any]],
        close_ws: Callable[[int], Awaitable[Any]] | None = None,
        adapter_id: str = DEFAULT_ADAPTER_ID,
        client_id: str = DEFAULT_CLIENT_ID,
        upstream_connect_factory: ConnectFactory | None = None,
    ) -> None:
        self._config = config
        self._stt = stt_provider
        self._tts = tts_provider
        self._send_text = send_text
        self._send_bytes = send_bytes
        self._close_ws = close_ws
        self._adapter_id = adapter_id
        self._client_id = client_id
        self._upstream_connect_factory = upstream_connect_factory

        self._ws_send_lock = asyncio.Lock()
        self._upstream: CompanionWsClient | None = None
        self._session_active = False
        self._session_id = ""
        self._session_mode: str = config.session_mode
        self._output_format: str = config.default_output_format
        self._tts_buffer_mode: str = config.tts_buffer_mode
        self._closing = False
        self._closing_upstream = False

        self._upload_stream: _UploadStream | None = None
        self._turns: dict[str, _TurnState] = {}
        self._cancelled_turns: deque[str] = deque(maxlen=64)
        self._audio_turn_tasks: dict[str, asyncio.Task[None]] = {}
        self._bg_tasks: set[asyncio.Task[None]] = set()

        self._tts_queue: deque[_TtsJob] = deque()
        self._tts_wake = asyncio.Event()
        self._tts_worker_task: asyncio.Task[None] | None = None
        self._active_tts_job: _TtsJob | None = None
        self._active_tts_task: asyncio.Task[None] | None = None

    @property
    def session_active(self) -> bool:
        """Whether an upstream /ws/companion session has been started and is ready."""
        return self._session_active and self._upstream is not None and self._upstream.connected

    async def start(self) -> None:
        """Start the serialized TTS worker loop for this client connection."""
        if self._tts_worker_task is None:
            self._tts_worker_task = asyncio.create_task(
                self._tts_worker_loop(), name="gateway-tts-worker"
            )

    async def close(self) -> None:
        """Stop background tasks, terminate active TTS/transcoding, and close upstream."""
        if self._closing:
            return
        self._closing = True
        self._upload_stream = None

        for task in list(self._audio_turn_tasks.values()):
            if not task.done():
                task.cancel()
        for task in list(self._bg_tasks):
            if not task.done():
                task.cancel()
        if self._active_tts_task is not None and not self._active_tts_task.done():
            self._active_tts_task.cancel()

        self._tts_queue.clear()
        self._tts_wake.set()
        if self._tts_worker_task is not None and not self._tts_worker_task.done():
            self._tts_worker_task.cancel()

        tasks = [
            t
            for t in (
                *self._audio_turn_tasks.values(),
                *self._bg_tasks,
                self._active_tts_task,
                self._tts_worker_task,
            )
            if t is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._audio_turn_tasks.clear()
        self._bg_tasks.clear()
        self._active_tts_task = None
        self._tts_worker_task = None
        await self._close_upstream()

    async def _close_upstream(self) -> None:
        upstream = self._upstream
        self._upstream = None
        self._session_active = False
        self._session_id = ""
        if upstream is not None:
            self._closing_upstream = True
            try:
                await upstream.close()
            finally:
                self._closing_upstream = False

    def _spawn_bg_task(self, coro: Any) -> asyncio.Task[None]:
        task = asyncio.create_task(coro)
        self._bg_tasks.add(task)

        def _done(done_task: asyncio.Task[None]) -> None:
            self._bg_tasks.discard(done_task)
            if not done_task.cancelled():
                exc = done_task.exception()
                if exc is not None:
                    log.warning(
                        "Gateway background task error: %s",
                        self._config.redact_secrets(f"{type(exc).__name__}: {exc}"),
                    )

        task.add_done_callback(_done)
        return task

    async def _send_json(self, frame: dict[str, Any]) -> None:
        if self._closing:
            return
        raw = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
        async with self._ws_send_lock:
            if self._closing:
                return
            await self._send_text(raw)

    async def _send_raw_bytes(self, data: bytes) -> None:
        if self._closing or not data:
            return
        async with self._ws_send_lock:
            if self._closing:
                return
            await self._send_bytes(data)

    async def _send_error(
        self,
        reason: str,
        detail: str = "",
        *,
        req_id: str = "",
        turn_id: str = "",
        stream_id: str = "",
    ) -> None:
        await self._send_json(
            build_error_frame(
                reason,
                detail,
                config=self._config,
                req_id=req_id,
                turn_id=turn_id,
                stream_id=stream_id,
            )
        )

    async def handle_text_frame(self, raw: str) -> None:
        """Decode and dispatch an incoming JSON text frame from the client WebSocket."""
        if self._closing:
            return
        await self.start()
        try:
            frame = decode_json_frame(raw, max_bytes=self._config.max_ws_frame_bytes)
            typ = str(frame.get("type") or "").strip()
            if typ == "ping":
                await self._handle_ping(frame)
            elif typ == "session.start":
                await self._handle_session_start(frame)
            elif typ == "session.stop":
                await self._handle_session_stop(frame)
            elif typ == "stt.transcribe":
                await self._handle_stt_transcribe(frame)
            elif typ == "tts.synthesize":
                await self._handle_tts_synthesize(frame)
            elif typ == "audio.turn":
                await self._handle_audio_turn(frame)
            elif typ == "audio.stream.start":
                await self._handle_audio_stream_start(frame)
            elif typ == "audio.stream.end":
                await self._handle_audio_stream_end(frame)
            elif typ == "text.turn":
                await self._handle_text_turn(frame)
            elif typ == "turn.cancel":
                await self._handle_turn_cancel(frame)
            else:
                await self._send_error("unknown_frame", f"Unsupported frame type: {typ}")
        except ProtocolError as exc:
            await self._send_error(exc.reason, exc.detail)
        except Exception as exc:
            await self._send_error("gateway_error", f"{type(exc).__name__}: {exc}")

    async def handle_binary_frame(self, data: bytes) -> None:
        """Append incoming raw audio bytes to the active streaming upload buffer."""
        if self._closing:
            return
        stream = self._upload_stream
        if stream is None:
            await self._send_error("no_active_audio_stream", "No active audio.stream.start")
            return
        if stream.aborted:
            return
        if len(stream.buffer) + len(data) > self._config.max_audio_bytes:
            stream.aborted = True
            stream.buffer.clear()
            await self._send_error(
                "audio_too_large",
                "Streaming audio upload exceeds max_audio_bytes",
                turn_id=stream.turn_id,
                stream_id=stream.stream_id,
            )
            return
        stream.buffer.extend(data)

    async def _handle_ping(self, frame: dict[str, Any]) -> None:
        req_id = validate_id(frame.get("req_id"), field_name="req_id", allow_empty=True)
        upstream_ms: float | None = None
        if self._upstream is not None and self._upstream.connected:
            try:
                rtt = await self._upstream.ping(timeout=5.0)
                upstream_ms = round(rtt, 2)
            except Exception:
                upstream_ms = None
        pong: dict[str, Any] = {
            "type": "pong",
            "ts": time.time(),
            "upstream_ms": upstream_ms,
        }
        if req_id:
            pong["req_id"] = req_id
        await self._send_json(pong)

    async def _handle_session_start(self, frame: dict[str, Any]) -> None:
        raw_mode = frame.get("mode")
        mode = str(raw_mode).strip() if raw_mode is not None else self._config.session_mode
        if mode not in {"owner_private", "room_public"}:
            raise ProtocolError("invalid_mode", "mode must be owner_private or room_public")

        raw_game = frame.get("game_title")
        if raw_game is not None and not isinstance(raw_game, str):
            raise ProtocolError("invalid_game_title", "game_title must be a string")
        game_title = raw_game.strip() if raw_game is not None else self._config.game_title

        raw_fmt = frame.get("output_format")
        output_format = (
            str(raw_fmt).strip().lower()
            if raw_fmt is not None and str(raw_fmt).strip()
            else self._config.default_output_format
        )
        try:
            output_format_meta(output_format)
        except ValueError as exc:
            raise ProtocolError("invalid_output_format", str(exc)) from None

        raw_buf = frame.get("tts_buffer_mode")
        tts_buffer_mode = (
            str(raw_buf).strip().lower()
            if raw_buf is not None and str(raw_buf).strip()
            else self._config.tts_buffer_mode
        )
        if tts_buffer_mode not in {"turn", "chunk"}:
            raise ProtocolError(
                "invalid_tts_buffer_mode", "tts_buffer_mode must be 'turn' or 'chunk'"
            )

        await self._cancel_local_tts()
        self._upload_stream = None
        await self._close_upstream()

        base_cfg = self._config.to_client_config(
            adapter_id=self._adapter_id,
            client_id=self._client_id,
        )
        client_cfg = dataclasses.replace(
            base_cfg,
            session_mode=mode,  # type: ignore[arg-type]
            game_title=game_title,
            tts_buffer_mode=tts_buffer_mode,  # type: ignore[arg-type]
        )
        upstream = CompanionWsClient(
            client_cfg,
            connect_factory=self._upstream_connect_factory,
            on_turn_start=self._on_upstream_turn_start,
            on_speak_chunk=self._on_upstream_speak_chunk,
            on_turn_end=self._on_upstream_turn_end,
            on_error=self._on_upstream_error,
            on_disconnect=self._on_upstream_disconnect,
        )
        try:
            await upstream.connect()
            ready = await upstream.start_session(mode=mode, game_title=game_title)
        except Exception as exc:
            with contextlib.suppress(Exception):
                await upstream.close()
            await self._send_error(
                "upstream_connect_failed",
                f"{type(exc).__name__}: {exc}",
            )
            return

        self._upstream = upstream
        self._session_active = True
        self._session_id = str(ready.get("session_id") or "")
        self._session_mode = str(ready.get("mode") or mode)
        self._output_format = output_format
        self._tts_buffer_mode = tts_buffer_mode

        upstream_config = ready.get("config")
        await self._send_json(
            {
                "type": "session.ready",
                "session_id": self._session_id,
                "mode": self._session_mode,
                "output_format": self._output_format,
                "tts_buffer_mode": self._tts_buffer_mode,
                "stt_provider": self._config.stt_provider,
                "tts_provider": self._config.tts_provider,
                "tts_model": self._config.resolved_tts_model(),
                "upstream_config": upstream_config if isinstance(upstream_config, dict) else {},
            }
        )

    async def _handle_session_stop(self, frame: dict[str, Any]) -> None:
        self._upload_stream = None
        for task in list(self._audio_turn_tasks.values()):
            if not task.done():
                task.cancel()
        self._audio_turn_tasks.clear()
        await self._cancel_local_tts()

        sid = self._session_id
        upstream = self._upstream
        if upstream is not None:
            self._closing_upstream = True
            try:
                if upstream.connected:
                    with contextlib.suppress(Exception):
                        ended = await upstream.stop_session(timeout=5.0)
                        if isinstance(ended, dict) and ended.get("session_id"):
                            sid = str(ended["session_id"])
            finally:
                with contextlib.suppress(Exception):
                    await upstream.close()
                self._closing_upstream = False
        self._upstream = None
        self._session_active = False
        self._session_id = ""
        await self._send_json({"type": "session.ended", "session_id": sid})

    async def _handle_stt_transcribe(self, frame: dict[str, Any]) -> None:
        req_id = validate_id(frame.get("req_id"), field_name="req_id", allow_empty=True)
        try:
            fmt = str(frame.get("format") or "wav").strip().lower()
            sample_rate = int(frame.get("sample_rate") or 16000)
            channels = int(frame.get("channels") or 1)
            raw_audio = _decode_audio_b64(frame, max_audio_bytes=self._config.max_audio_bytes)
        except ProtocolError as exc:
            await self._send_error(exc.reason, exc.detail, req_id=req_id)
            return
        except (TypeError, ValueError) as exc:
            await self._send_error("invalid_audio_params", str(exc), req_id=req_id)
            return

        async def _run_stt() -> None:
            t0 = time.perf_counter()
            try:
                wav_bytes = await normalize_input_to_wav(
                    raw_audio,
                    fmt,
                    sample_rate=sample_rate,
                    channels=channels,
                    ffmpeg_bin=self._config.ffmpeg_bin,
                )
                text = (await self._stt.transcribe(wav_bytes)).strip()
            except Exception as exc:
                await self._send_error("stt_failed", f"{type(exc).__name__}: {exc}", req_id=req_id)
                return
            stt_ms = (time.perf_counter() - t0) * 1000.0
            payload: dict[str, Any] = {
                "type": "stt.result",
                "req_id": req_id,
                "text": text,
                "stt_ms": round(stt_ms, 2),
            }
            await self._send_json(payload)

        self._spawn_bg_task(_run_stt())

    async def _handle_tts_synthesize(self, frame: dict[str, Any]) -> None:
        req_id = validate_id(frame.get("req_id"), field_name="req_id", allow_empty=True)
        raw_text = frame.get("text")
        if not isinstance(raw_text, str) or not raw_text.strip():
            await self._send_error("empty_text", "TTS text must not be empty", req_id=req_id)
            return
        fmt = str(frame.get("output_format") or self._output_format).strip().lower()
        try:
            output_format_meta(fmt)
        except ValueError as exc:
            await self._send_error("invalid_output_format", str(exc), req_id=req_id)
            return

        self._enqueue_tts_job(
            _TtsJob(
                turn_id="",
                seq=1,
                text=raw_text.strip(),
                output_format=fmt,
                req_id=req_id,
            )
        )

    async def _handle_audio_turn(self, frame: dict[str, Any]) -> None:
        raw_tid = frame.get("turn_id")
        turn_id = (
            validate_id(raw_tid, field_name="turn_id")
            if raw_tid is not None and str(raw_tid).strip()
            else generate_id("t")
        )
        if not self.session_active:
            await self._send_error(
                "session_not_started",
                "Call session.start before sending audio.turn",
                turn_id=turn_id,
            )
            return
        try:
            fmt = str(frame.get("format") or "wav").strip().lower()
            sample_rate = int(frame.get("sample_rate") or 16000)
            channels = int(frame.get("channels") or 1)
            raw_audio = _decode_audio_b64(frame, max_audio_bytes=self._config.max_audio_bytes)
            speaker_id = _optional_str(frame.get("speaker_id"), max_len=64)
            speaker_name = _optional_str(frame.get("speaker_name"), max_len=48)
            frames = frame.get("frames")
            if frames is not None and not isinstance(frames, list):
                raise ProtocolError("invalid_frames", "frames must be a list")
        except ProtocolError as exc:
            await self._send_error(exc.reason, exc.detail, turn_id=turn_id)
            return
        except (TypeError, ValueError) as exc:
            await self._send_error("invalid_audio_params", str(exc), turn_id=turn_id)
            return

        self._spawn_audio_turn(
            turn_id=turn_id,
            raw_audio=raw_audio,
            fmt=fmt,
            sample_rate=sample_rate,
            channels=channels,
            speaker_id=speaker_id,
            speaker_name=speaker_name,
            frames=frames,
        )

    async def _handle_audio_stream_start(self, frame: dict[str, Any]) -> None:
        raw_sid = frame.get("stream_id")
        stream_id = (
            validate_id(raw_sid, field_name="stream_id")
            if raw_sid is not None and str(raw_sid).strip()
            else generate_id("s")
        )
        raw_tid = frame.get("turn_id")
        turn_id = (
            validate_id(raw_tid, field_name="turn_id")
            if raw_tid is not None and str(raw_tid).strip()
            else generate_id("t")
        )
        if not self.session_active:
            await self._send_error(
                "session_not_started",
                "Call session.start before audio.stream.start",
                turn_id=turn_id,
                stream_id=stream_id,
            )
            return
        if self._upload_stream is not None and not self._upload_stream.aborted:
            await self._send_error(
                "stream_already_active",
                "An audio upload stream is already active",
                turn_id=turn_id,
                stream_id=stream_id,
            )
            return
        try:
            fmt = str(frame.get("format") or "pcm_16k").strip().lower()
            sample_rate = int(frame.get("sample_rate") or 16000)
            channels = int(frame.get("channels") or 1)
            speaker_id = _optional_str(frame.get("speaker_id"), max_len=64)
            speaker_name = _optional_str(frame.get("speaker_name"), max_len=48)
            frames = frame.get("frames")
            if frames is not None and not isinstance(frames, list):
                raise ProtocolError("invalid_frames", "frames must be a list")
        except ProtocolError as exc:
            await self._send_error(exc.reason, exc.detail, turn_id=turn_id, stream_id=stream_id)
            return
        except (TypeError, ValueError) as exc:
            await self._send_error(
                "invalid_audio_params", str(exc), turn_id=turn_id, stream_id=stream_id
            )
            return

        self._upload_stream = _UploadStream(
            stream_id=stream_id,
            turn_id=turn_id,
            fmt=fmt,
            sample_rate=sample_rate,
            channels=channels,
            speaker_id=speaker_id,
            speaker_name=speaker_name,
            frames=frames,
        )

    async def _handle_audio_stream_end(self, frame: dict[str, Any]) -> None:
        stream = self._upload_stream
        if stream is None:
            await self._send_error("no_active_audio_stream", "No active audio stream to end")
            return
        req_sid = validate_id(frame.get("stream_id"), field_name="stream_id", allow_empty=True)
        if req_sid and req_sid != stream.stream_id:
            await self._send_error(
                "stream_not_found",
                "stream_id does not match active upload stream",
                stream_id=req_sid,
            )
            return

        self._upload_stream = None
        if stream.aborted:
            return
        raw_audio = bytes(stream.buffer)
        stream.buffer.clear()
        if not raw_audio:
            await self._send_error(
                "empty_audio",
                "Streaming audio payload is empty",
                turn_id=stream.turn_id,
                stream_id=stream.stream_id,
            )
            return

        self._spawn_audio_turn(
            turn_id=stream.turn_id,
            raw_audio=raw_audio,
            fmt=stream.fmt,
            sample_rate=stream.sample_rate,
            channels=stream.channels,
            speaker_id=stream.speaker_id,
            speaker_name=stream.speaker_name,
            frames=stream.frames,
        )

    def _spawn_audio_turn(
        self,
        *,
        turn_id: str,
        raw_audio: bytes,
        fmt: str,
        sample_rate: int,
        channels: int,
        speaker_id: str | None,
        speaker_name: str | None,
        frames: list[Any] | None,
    ) -> None:
        async def _runner() -> None:
            t0 = time.perf_counter()
            try:
                wav_bytes = await normalize_input_to_wav(
                    raw_audio,
                    fmt,
                    sample_rate=sample_rate,
                    channels=channels,
                    ffmpeg_bin=self._config.ffmpeg_bin,
                )
                raw_text = await self._stt.transcribe(wav_bytes)
            except asyncio.CancelledError:
                await self._emit_turn_end(
                    turn_id,
                    status="cancelled",
                    reason="barge_in",
                )
                raise
            except Exception as exc:
                await self._send_error(
                    "stt_failed", f"{type(exc).__name__}: {exc}", turn_id=turn_id
                )
                await self._emit_turn_end(turn_id, status="error", reason="stt_failed")
                return

            stt_ms = (time.perf_counter() - t0) * 1000.0
            clean_text = raw_text.strip()
            if turn_id in self._cancelled_turns or self._closing:
                await self._emit_turn_end(
                    turn_id,
                    status="cancelled",
                    reason="barge_in",
                    metrics={"stt_ms": round(stt_ms, 2)},
                )
                return

            await self._send_json(
                {
                    "type": "stt.final",
                    "turn_id": turn_id,
                    "text": clean_text,
                    "stt_ms": round(stt_ms, 2),
                }
            )
            if not clean_text:
                await self._emit_turn_end(
                    turn_id,
                    status="skipped",
                    reason="empty_speech",
                    metrics={"stt_ms": round(stt_ms, 2)},
                )
                return

            await self._dispatch_upstream_speech(
                turn_id=turn_id,
                text=clean_text,
                stt_ms=stt_ms,
                speaker_id=speaker_id,
                speaker_name=speaker_name,
                frames=frames,
            )

        task = self._spawn_bg_task(_runner())
        self._audio_turn_tasks[turn_id] = task
        task.add_done_callback(lambda _: self._audio_turn_tasks.pop(turn_id, None))

    async def _handle_text_turn(self, frame: dict[str, Any]) -> None:
        raw_tid = frame.get("turn_id")
        turn_id = (
            validate_id(raw_tid, field_name="turn_id")
            if raw_tid is not None and str(raw_tid).strip()
            else generate_id("t")
        )
        if not self.session_active:
            await self._send_error(
                "session_not_started",
                "Call session.start before sending text.turn",
                turn_id=turn_id,
            )
            return

        raw_text = frame.get("text")
        if not isinstance(raw_text, str):
            await self._send_error("invalid_text", "text must be a string", turn_id=turn_id)
            return
        clean_text = raw_text.strip()
        if not clean_text:
            await self._emit_turn_end(turn_id, status="skipped", reason="empty_speech")
            return

        speaker_id = _optional_str(frame.get("speaker_id"), max_len=64)
        speaker_name = _optional_str(frame.get("speaker_name"), max_len=48)
        frames = frame.get("frames")
        if frames is not None and not isinstance(frames, list):
            await self._send_error("invalid_frames", "frames must be a list", turn_id=turn_id)
            return

        await self._dispatch_upstream_speech(
            turn_id=turn_id,
            text=clean_text,
            stt_ms=None,
            speaker_id=speaker_id,
            speaker_name=speaker_name,
            frames=frames,
        )

    async def _dispatch_upstream_speech(
        self,
        *,
        turn_id: str,
        text: str,
        stt_ms: float | None,
        speaker_id: str | None,
        speaker_name: str | None,
        frames: list[Any] | None,
    ) -> None:
        if not self.session_active or self._upstream is None:
            await self._send_error(
                "session_not_started",
                "Upstream companion session is not active",
                turn_id=turn_id,
            )
            await self._emit_turn_end(turn_id, status="error", reason="session_not_started")
            return

        await self._cancel_local_tts(exclude_turn_id=turn_id)
        self._register_turn(turn_id, stt_ms=stt_ms)

        try:
            await self._upstream.send_speech_turn(
                text,
                turn_id=turn_id,
                frames=frames,
                speaker_id=speaker_id,
                speaker_name=speaker_name,
            )
        except Exception as exc:
            await self._send_error(
                "upstream_send_failed", f"{type(exc).__name__}: {exc}", turn_id=turn_id
            )
            await self._emit_turn_end(turn_id, status="error", reason="upstream_send_failed")

    async def _handle_turn_cancel(self, frame: dict[str, Any]) -> None:
        raw_tid = frame.get("turn_id")
        target_id = validate_id(raw_tid, field_name="turn_id", allow_empty=True)

        if target_id:
            audio_task = self._audio_turn_tasks.get(target_id)
            if audio_task is not None and not audio_task.done():
                audio_task.cancel()
        else:
            for audio_task in list(self._audio_turn_tasks.values()):
                if not audio_task.done():
                    audio_task.cancel()

        await self._cancel_local_tts(target_turn_id=target_id or None)
        if self._upstream is not None and self._upstream.connected:
            with contextlib.suppress(Exception):
                await self._upstream.cancel_turn(target_id)

    def _register_turn(self, turn_id: str, *, stt_ms: float | None = None) -> _TurnState:
        state = self._turns.get(turn_id)
        if state is None:
            state = _TurnState(turn_id=turn_id, sent_at=time.perf_counter(), stt_ms=stt_ms)
            self._turns[turn_id] = state
        elif stt_ms is not None:
            state.stt_ms = stt_ms
        if len(self._turns) > 64:
            for old_tid in list(self._turns):
                if old_tid != turn_id and self._turns[old_tid].ended:
                    self._turns.pop(old_tid, None)
                if len(self._turns) <= 48:
                    break
        return state

    async def _emit_turn_end(
        self,
        turn_id: str,
        *,
        status: str,
        reason: str = "",
        metrics: dict[str, float] | None = None,
    ) -> None:
        state = self._turns.get(turn_id)
        if state is not None:
            if state.ended:
                return
            state.ended = True
        payload: dict[str, Any] = {
            "type": "turn.end",
            "turn_id": turn_id,
            "status": status,
            "reason": reason,
        }
        if metrics:
            payload["metrics"] = metrics
        await self._send_json(payload)

    def _build_turn_metrics(
        self,
        state: _TurnState | None,
        *,
        llm_ms: float | None = None,
        tts_ttfb_ms: float | None = None,
    ) -> dict[str, float]:
        metrics: dict[str, float] = {}
        if state is not None and state.stt_ms is not None:
            metrics["stt_ms"] = round(state.stt_ms, 2)
        if llm_ms is not None:
            metrics["llm_ms"] = round(llm_ms, 2)
        if tts_ttfb_ms is not None:
            metrics["tts_ttfb_ms"] = round(tts_ttfb_ms, 2)
        elif state is not None and state.first_tts_ttfb_ms is not None:
            metrics["tts_ttfb_ms"] = round(state.first_tts_ttfb_ms, 2)
        return metrics

    async def _on_upstream_turn_start(self, frame: dict[str, Any]) -> None:
        turn_id = str(frame.get("turn_id") or "").strip()
        if not turn_id or turn_id in self._cancelled_turns:
            return
        self._register_turn(turn_id)
        await self._send_json({"type": "turn.start", "turn_id": turn_id})

    async def _on_upstream_speak_chunk(self, frame: dict[str, Any]) -> None:
        turn_id = str(frame.get("turn_id") or "").strip()
        if not turn_id or turn_id in self._cancelled_turns:
            return
        seq = int(frame.get("seq") or 0)
        text = str(frame.get("text") or "").strip()
        if not text:
            return
        state = self._register_turn(turn_id)
        if state.cancelled or state.ended:
            return

        await self._send_json(
            {
                "type": "text.chunk",
                "turn_id": turn_id,
                "seq": seq,
                "text": text,
            }
        )
        if self._tts_buffer_mode == "turn":
            state.text_parts.append(text)
        else:
            self._enqueue_tts_job(
                _TtsJob(
                    turn_id=turn_id,
                    seq=seq,
                    text=text,
                    output_format=self._output_format,
                )
            )

    async def _on_upstream_turn_end(self, frame: dict[str, Any]) -> None:
        turn_id = str(frame.get("turn_id") or "").strip()
        status = str(frame.get("status") or "").strip().lower() or "spoken"
        reason = str(frame.get("reason") or "").strip()

        state = self._turns.get(turn_id)
        llm_ms = (time.perf_counter() - state.sent_at) * 1000.0 if state is not None else None
        parts = list(state.text_parts) if state is not None else []
        if state is not None:
            state.text_parts.clear()

        if status == "cancelled" or turn_id in self._cancelled_turns:
            await self._cancel_local_tts(target_turn_id=turn_id)
            await self._emit_turn_end(
                turn_id,
                status="cancelled",
                reason=reason or "barge_in",
                metrics=self._build_turn_metrics(state, llm_ms=llm_ms),
            )
            return

        if status in {"silent", "skipped", "error"}:
            await self._cancel_local_tts(target_turn_id=turn_id)
            await self._emit_turn_end(
                turn_id,
                status=status,
                reason=reason,
                metrics=self._build_turn_metrics(state, llm_ms=llm_ms),
            )
            return

        final_payload = {
            "turn_id": turn_id,
            "status": status,
            "reason": reason,
            "llm_ms": llm_ms,
        }
        if self._tts_buffer_mode == "turn":
            joined = _join_turn_chunks(parts)
            if joined:
                self._enqueue_tts_job(
                    _TtsJob(
                        turn_id=turn_id,
                        seq=1,
                        text=joined,
                        output_format=self._output_format,
                        final_turn_payload=final_payload,
                    )
                )
                return
            await self._emit_turn_end(
                turn_id,
                status=status,
                reason=reason,
                metrics=self._build_turn_metrics(state, llm_ms=llm_ms),
            )
            return

        # Chunk mode: enqueue a zero-text barrier so turn.end fires only after all chunks finish.
        self._enqueue_tts_job(
            _TtsJob(
                turn_id=turn_id,
                seq=0,
                text="",
                output_format=self._output_format,
                final_turn_payload=final_payload,
            )
        )

    async def _on_upstream_error(self, frame: dict[str, Any]) -> None:
        reason = str(frame.get("reason") or "upstream_error")
        detail = str(frame.get("detail") or reason)
        turn_id = str(frame.get("turn_id") or "")
        await self._send_error(reason, detail, turn_id=turn_id)

    async def _on_upstream_disconnect(self) -> None:
        if self._closing or self._closing_upstream:
            return
        self._session_active = False
        self._upstream = None
        await self._cancel_local_tts()
        await self._send_error("upstream_disconnected", "Upstream companion WebSocket closed")

    def _enqueue_tts_job(self, job: _TtsJob) -> None:
        if self._closing:
            return
        self._tts_queue.append(job)
        self._tts_wake.set()

    async def _cancel_local_tts(
        self,
        *,
        target_turn_id: str | None = None,
        exclude_turn_id: str = "",
    ) -> None:
        if target_turn_id:
            if target_turn_id not in self._cancelled_turns:
                self._cancelled_turns.append(target_turn_id)
            state = self._turns.get(target_turn_id)
            if state is not None:
                state.cancelled = True
                state.text_parts.clear()
        else:
            for tid, state in self._turns.items():
                if tid != exclude_turn_id and not state.ended:
                    state.cancelled = True
                    state.text_parts.clear()
                    if tid not in self._cancelled_turns:
                        self._cancelled_turns.append(tid)

        kept: deque[_TtsJob] = deque()
        deferred_cancellations: list[str] = []
        while self._tts_queue:
            job = self._tts_queue.popleft()
            matches = (
                job.turn_id == target_turn_id
                if target_turn_id
                else (not job.turn_id or job.turn_id != exclude_turn_id)
            )
            if matches:
                job.cancelled = True
                if job.final_turn_payload is not None and job.turn_id:
                    deferred_cancellations.append(job.turn_id)
            else:
                kept.append(job)
        self._tts_queue = kept

        active = self._active_tts_job
        if active is not None:
            active_matches = (
                active.turn_id == target_turn_id
                if target_turn_id
                else (not active.turn_id or active.turn_id != exclude_turn_id)
            )
            if active_matches:
                active.cancelled = True
                if self._active_tts_task is not None and not self._active_tts_task.done():
                    self._active_tts_task.cancel()

        for cancelled_tid in deferred_cancellations:
            tstate = self._turns.get(cancelled_tid)
            llm_ms = (time.perf_counter() - tstate.sent_at) * 1000.0 if tstate else None
            await self._emit_turn_end(
                cancelled_tid,
                status="cancelled",
                reason="barge_in",
                metrics=self._build_turn_metrics(tstate, llm_ms=llm_ms),
            )

    async def _tts_worker_loop(self) -> None:
        while not self._closing:
            if not self._tts_queue:
                self._tts_wake.clear()
                if self._closing:
                    break
                await self._tts_wake.wait()
                continue

            job = self._tts_queue.popleft()
            if (
                job.cancelled
                or self._closing
                or (job.turn_id and job.turn_id in self._cancelled_turns)
            ):
                if job.final_turn_payload is not None and job.turn_id and not self._closing:
                    tstate = self._turns.get(job.turn_id)
                    await self._emit_turn_end(
                        job.turn_id,
                        status="cancelled",
                        reason="barge_in",
                        metrics=self._build_turn_metrics(
                            tstate,
                            llm_ms=job.final_turn_payload.get("llm_ms"),
                        ),
                    )
                continue

            if not job.text:
                if job.final_turn_payload is not None and job.turn_id and not self._closing:
                    tstate = self._turns.get(job.turn_id)
                    await self._emit_turn_end(
                        job.turn_id,
                        status=str(job.final_turn_payload["status"]),
                        reason=str(job.final_turn_payload["reason"]),
                        metrics=self._build_turn_metrics(
                            tstate,
                            llm_ms=job.final_turn_payload.get("llm_ms"),
                        ),
                    )
                continue

            exec_state = _TtsExecutionState()
            self._active_tts_job = job
            task = asyncio.create_task(self._execute_tts_job(job, exec_state))
            self._active_tts_task = task
            try:
                await task
            except asyncio.CancelledError:
                if self._closing:
                    raise
                if exec_state.started_wire:
                    await self._send_json(
                        {
                            "type": "tts.end",
                            "req_id": job.req_id,
                            "turn_id": job.turn_id,
                            "seq": job.seq,
                            "bytes": exec_state.total_bytes,
                            "status": "cancelled",
                            "reason": "barge_in",
                            "tts_ttfb_ms": round(exec_state.ttfb_ms, 2),
                        }
                    )
                if job.final_turn_payload is not None and job.turn_id:
                    tstate = self._turns.get(job.turn_id)
                    await self._emit_turn_end(
                        job.turn_id,
                        status="cancelled",
                        reason="barge_in",
                        metrics=self._build_turn_metrics(
                            tstate,
                            llm_ms=job.final_turn_payload.get("llm_ms"),
                            tts_ttfb_ms=exec_state.ttfb_ms or None,
                        ),
                    )
            except Exception as exc:
                if exec_state.started_wire:
                    await self._send_json(
                        {
                            "type": "tts.end",
                            "req_id": job.req_id,
                            "turn_id": job.turn_id,
                            "seq": job.seq,
                            "bytes": exec_state.total_bytes,
                            "status": "error",
                            "reason": "tts_failed",
                            "tts_ttfb_ms": round(exec_state.ttfb_ms, 2),
                        }
                    )
                await self._send_error(
                    "tts_failed",
                    f"{type(exc).__name__}: {exc}",
                    req_id=job.req_id,
                    turn_id=job.turn_id,
                )
                if job.final_turn_payload is not None and job.turn_id:
                    tstate = self._turns.get(job.turn_id)
                    await self._emit_turn_end(
                        job.turn_id,
                        status="error",
                        reason="tts_failed",
                        metrics=self._build_turn_metrics(
                            tstate,
                            llm_ms=job.final_turn_payload.get("llm_ms"),
                            tts_ttfb_ms=exec_state.ttfb_ms or None,
                        ),
                    )
            finally:
                if self._active_tts_job is job:
                    self._active_tts_job = None
                if self._active_tts_task is task:
                    self._active_tts_task = None

    async def _execute_tts_job(self, job: _TtsJob, state: _TtsExecutionState) -> None:
        t0 = time.perf_counter()
        synth_stream = getattr(self._tts, "synthesize_stream", None)
        has_synth = callable(getattr(self._tts, "synthesize", None))
        if (self._config.tts_streaming and callable(synth_stream)) or not has_synth:
            assert callable(synth_stream)
            raw_fmt = getattr(self._tts, "response_format", "mp3") or "mp3"
            src_fmt = str(raw_fmt).lstrip(".").lower()
            raw_iter = synth_stream(job.text)
        else:
            audio_bytes, raw_ext = await self._tts.synthesize(job.text)
            src_fmt = raw_ext.lstrip(".").lower() or "mp3"
            raw_iter = _chunk_bytes(audio_bytes)

        transcoded = transcode_audio_stream(
            raw_iter,
            src_fmt=src_fmt,
            dst_fmt=job.output_format,
            ffmpeg_bin=self._config.ffmpeg_bin,
        )
        try:
            try:
                first_chunk = await anext(transcoded)
            except StopAsyncIteration:
                if job.req_id:
                    await self._send_error(
                        "tts_empty_audio",
                        "TTS provider returned empty audio",
                        req_id=job.req_id,
                    )
                if job.final_turn_payload is not None and job.turn_id:
                    tstate = self._turns.get(job.turn_id)
                    await self._emit_turn_end(
                        job.turn_id,
                        status=str(job.final_turn_payload["status"]),
                        reason=str(job.final_turn_payload["reason"]),
                        metrics=self._build_turn_metrics(
                            tstate,
                            llm_ms=job.final_turn_payload.get("llm_ms"),
                        ),
                    )
                return

            if (
                job.cancelled
                or self._closing
                or (job.turn_id and job.turn_id in self._cancelled_turns)
            ):
                raise asyncio.CancelledError

            state.ttfb_ms = (time.perf_counter() - t0) * 1000.0
            if job.turn_id:
                tstate = self._turns.get(job.turn_id)
                if tstate is not None and tstate.first_tts_ttfb_ms is None:
                    tstate.first_tts_ttfb_ms = state.ttfb_ms

            mime_type, sample_rate, channels = output_format_meta(job.output_format)
            await self._send_json(
                {
                    "type": "tts.start",
                    "req_id": job.req_id,
                    "turn_id": job.turn_id,
                    "seq": job.seq,
                    "format": job.output_format,
                    "mime_type": mime_type,
                    "sample_rate": sample_rate,
                    "channels": channels,
                }
            )
            state.started_wire = True

            await self._send_raw_bytes(first_chunk)
            state.total_bytes += len(first_chunk)

            async for chunk in transcoded:
                if (
                    job.cancelled
                    or self._closing
                    or (job.turn_id and job.turn_id in self._cancelled_turns)
                ):
                    raise asyncio.CancelledError
                await self._send_raw_bytes(chunk)
                state.total_bytes += len(chunk)

            await self._send_json(
                {
                    "type": "tts.end",
                    "req_id": job.req_id,
                    "turn_id": job.turn_id,
                    "seq": job.seq,
                    "bytes": state.total_bytes,
                    "status": "ok",
                    "reason": "",
                    "tts_ttfb_ms": round(state.ttfb_ms, 2),
                }
            )

            if job.final_turn_payload is not None and job.turn_id:
                tstate = self._turns.get(job.turn_id)
                await self._emit_turn_end(
                    job.turn_id,
                    status=str(job.final_turn_payload["status"]),
                    reason=str(job.final_turn_payload["reason"]),
                    metrics=self._build_turn_metrics(
                        tstate,
                        llm_ms=job.final_turn_payload.get("llm_ms"),
                        tts_ttfb_ms=state.ttfb_ms,
                    ),
                )
        finally:
            with contextlib.suppress(Exception):
                await transcoded.aclose()
