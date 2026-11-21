"""FastAPI HTTP and WebSocket application for hakimi-voice-gateway."""

import contextlib
import email.policy
import hmac
import logging
import time
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from email.parser import BytesParser
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.responses import StreamingResponse
from starlette.websockets import WebSocketDisconnect

from voice_client.companion_ws import ConnectFactory
from voice_client.providers import (
    STTProvider,
    TTSProvider,
    build_stt_provider,
    build_tts_provider,
)

from .audio_codec import normalize_input_to_wav, output_format_meta, transcode_audio_stream
from .config import VoiceGatewayConfig, load_gateway_config
from .protocol import (
    ProtocolError,
    safe_error_detail,
    validate_adapter_id,
    validate_client_id,
)
from .session import VoiceGatewaySession

log = logging.getLogger("voice_gateway.app")


def _require_bearer_http(request: Request, config: VoiceGatewayConfig) -> None:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer authorization")
    expected = f"Bearer {config.gateway_token.strip()}".encode()
    if not hmac.compare_digest(auth.encode("utf-8"), expected):
        raise HTTPException(status_code=403, detail="Invalid Bearer token")


async def _chunk_bytes(data: bytes, chunk_size: int = 16384) -> AsyncIterator[bytes]:
    for idx in range(0, len(data), chunk_size):
        chunk = data[idx : idx + chunk_size]
        if chunk:
            yield chunk


async def _prepend_first(
    first_chunk: bytes,
    rest: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    try:
        yield first_chunk
        async for chunk in rest:
            yield chunk
    finally:
        with contextlib.suppress(Exception):
            await rest.aclose()


def create_gateway_app(
    config: VoiceGatewayConfig | None = None,
    *,
    stt_provider: STTProvider | None = None,
    tts_provider: TTSProvider | None = None,
    upstream_connect_factory: ConnectFactory | None = None,
) -> FastAPI:
    """Build the FastAPI application for the standalone KR audio gateway."""
    resolved_config = config if config is not None else load_gateway_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved_config.require_gateway_auth()
        for name in ("httpx", "httpcore", "websockets"):
            logging.getLogger(name).setLevel(logging.CRITICAL)

        client_cfg = resolved_config.to_client_config()
        stt = stt_provider if stt_provider is not None else build_stt_provider(client_cfg)
        tts = tts_provider if tts_provider is not None else build_tts_provider(client_cfg)
        owns_stt = stt_provider is None
        owns_tts = tts_provider is None

        async with AsyncExitStack() as stack:
            if owns_stt and hasattr(stt, "aclose"):
                stack.push_async_callback(stt.aclose)
            if owns_tts and hasattr(tts, "aclose"):
                stack.push_async_callback(tts.aclose)

            app.state.config = resolved_config
            app.state.stt = stt
            app.state.tts = tts
            app.state.upstream_connect_factory = upstream_connect_factory
            yield

    app = FastAPI(
        title="hakimi-voice-gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, str]:
        cfg: VoiceGatewayConfig = request.app.state.config
        return {
            "status": "ok",
            "stt_provider": cfg.stt_provider,
            "tts_provider": cfg.tts_provider,
        }

    @app.post("/v1/stt")
    async def post_stt(request: Request) -> dict[str, Any]:
        cfg: VoiceGatewayConfig = request.app.state.config
        _require_bearer_http(request, cfg)

        raw_audio, fmt, sample_rate, channels = await _parse_stt_multipart(
            request,
            max_audio_bytes=cfg.max_audio_bytes,
        )

        t0 = time.perf_counter()
        try:
            wav_bytes = await normalize_input_to_wav(
                raw_audio,
                fmt,
                sample_rate=sample_rate,
                channels=channels,
                ffmpeg_bin=cfg.ffmpeg_bin,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=safe_error_detail(exc, cfg)) from None
        except (RuntimeError, TimeoutError) as exc:
            raise HTTPException(status_code=500, detail=safe_error_detail(exc, cfg)) from None

        stt: STTProvider = request.app.state.stt
        try:
            text = (await stt.transcribe(wav_bytes)).strip()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=safe_error_detail(exc, cfg)) from None

        stt_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "text": text,
            "stt_ms": round(stt_ms, 2),
            "provider": cfg.stt_provider,
        }

    @app.post("/v1/tts")
    async def post_tts(request: Request) -> Response:
        cfg: VoiceGatewayConfig = request.app.state.config
        _require_bearer_http(request, cfg)

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON request body") from None
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")

        raw_text = body.get("text")
        if not isinstance(raw_text, str) or not raw_text.strip():
            raise HTTPException(status_code=400, detail="text must not be empty")
        text = raw_text.strip()

        raw_fmt = body.get("output_format")
        output_fmt = (
            str(raw_fmt).strip().lower()
            if raw_fmt is not None and str(raw_fmt).strip()
            else cfg.default_output_format
        )
        try:
            mime_type, sample_rate, channels = output_format_meta(output_fmt)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=safe_error_detail(exc, cfg)) from None

        stream_flag = body.get("stream", True)
        if not isinstance(stream_flag, bool):
            raise HTTPException(status_code=400, detail="stream must be a boolean")

        tts: TTSProvider = request.app.state.tts
        synth_stream = getattr(tts, "synthesize_stream", None)
        has_synth = callable(getattr(tts, "synthesize", None))
        try:
            if (cfg.tts_streaming and callable(synth_stream)) or not has_synth:
                assert callable(synth_stream)
                src_fmt = str(getattr(tts, "response_format", "mp3") or "mp3").lstrip(".").lower()
                raw_iter = synth_stream(text)
            else:
                audio_bytes, raw_ext = await tts.synthesize(text)
                src_fmt = raw_ext.lstrip(".").lower() or "mp3"
                raw_iter = _chunk_bytes(audio_bytes)

            transcoded = transcode_audio_stream(
                raw_iter,
                src_fmt=src_fmt,
                dst_fmt=output_fmt,
                ffmpeg_bin=cfg.ffmpeg_bin,
            )
            first_chunk = await anext(transcoded)
        except StopAsyncIteration:
            raise HTTPException(
                status_code=502,
                detail="TTS provider returned empty audio",
            ) from None
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=502, detail=safe_error_detail(exc, cfg)) from None

        headers = {
            "X-Audio-Format": output_fmt,
            "X-Sample-Rate": str(sample_rate),
            "X-Channels": str(channels),
        }
        if stream_flag:
            return StreamingResponse(
                _prepend_first(first_chunk, transcoded),
                media_type=mime_type,
                headers=headers,
            )

        buf = bytearray(first_chunk)
        try:
            async for chunk in transcoded:
                buf.extend(chunk)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=safe_error_detail(exc, cfg)) from None
        finally:
            with contextlib.suppress(Exception):
                await transcoded.aclose()

        return Response(content=bytes(buf), media_type=mime_type, headers=headers)

    @app.websocket("/{full_path:path}")
    async def voice_ws(websocket: WebSocket, full_path: str = "") -> None:
        cfg: VoiceGatewayConfig = websocket.app.state.config
        expected_bearer = f"Bearer {cfg.gateway_token.strip()}".encode()
        actual_bearer = websocket.headers.get("authorization", "").encode("utf-8")

        if (
            websocket.url.path != cfg.ws_path
            or bool(websocket.query_params)
            or not hmac.compare_digest(actual_bearer, expected_bearer)
        ):
            log.warning("event=voice_gateway_auth_rejected")
            await websocket.close(code=1008)
            return

        try:
            adapter_id = validate_adapter_id(websocket.headers.get("x-voice-adapter"))
            client_id = validate_client_id(websocket.headers.get("x-voice-client-id"))
        except ProtocolError:
            log.warning("event=voice_gateway_header_rejected")
            await websocket.close(code=1008)
            return

        await websocket.accept()
        log.info("event=voice_gateway_connected adapter=%s client=%s", adapter_id, client_id)

        session = VoiceGatewaySession(
            cfg,
            stt_provider=websocket.app.state.stt,
            tts_provider=websocket.app.state.tts,
            send_text=websocket.send_text,
            send_bytes=websocket.send_bytes,
            close_ws=websocket.close,
            adapter_id=adapter_id,
            client_id=client_id,
            upstream_connect_factory=websocket.app.state.upstream_connect_factory,
        )
        await session.start()
        try:
            while True:
                try:
                    msg = await websocket.receive()
                except WebSocketDisconnect:
                    break
                msg_type = msg.get("type")
                if msg_type == "websocket.disconnect":
                    break
                if msg_type != "websocket.receive":
                    continue
                raw_bytes = msg.get("bytes")
                if raw_bytes is not None:
                    if len(raw_bytes) > cfg.max_ws_frame_bytes:
                        await session._send_error(
                            "frame_too_large",
                            "Binary frame exceeds max_ws_frame_bytes",
                        )
                        continue
                    await session.handle_binary_frame(raw_bytes)
                    continue
                raw_text = msg.get("text")
                if raw_text is not None:
                    await session.handle_text_frame(raw_text)
        finally:
            await session.close()
            log.info("event=voice_gateway_disconnected adapter=%s client=%s", adapter_id, client_id)

    return app


async def _parse_stt_multipart(
    request: Request,
    *,
    max_audio_bytes: int,
) -> tuple[bytes, str, int, int]:
    """Parse POST /v1/stt multipart/form-data using stdlib BytesParser (no python-multipart)."""
    content_type = request.headers.get("content-type", "").strip()
    if (
        "\r" in content_type
        or "\n" in content_type
        or "multipart/form-data" not in content_type.lower()
        or "boundary=" not in content_type.lower()
    ):
        raise HTTPException(status_code=400, detail="multipart/form-data with boundary required")

    max_body = max_audio_bytes + 65536
    body_buf = bytearray()
    async for chunk in request.stream():
        body_buf.extend(chunk)
        if len(body_buf) > max_body:
            raise HTTPException(status_code=413, detail="Audio exceeds max_audio_bytes")

    raw_mime = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode(
        "latin-1", errors="replace"
    )
    try:
        msg = BytesParser(policy=email.policy.default).parsebytes(raw_mime + bytes(body_buf))
    except Exception:
        raise HTTPException(status_code=400, detail="Malformed multipart/form-data body") from None

    if not msg.is_multipart():
        raise HTTPException(status_code=400, detail="Malformed multipart/form-data body")

    file_bytes: bytes | None = None
    fmt = "wav"
    sample_rate = 16000
    channels = 1

    for part in msg.iter_parts():
        if part.is_multipart():
            continue
        field_name = part.get_param("name", header="content-disposition")
        if not isinstance(field_name, str):
            continue
        payload = part.get_payload(decode=True)
        raw_part = payload if isinstance(payload, bytes) else b""
        if field_name == "file":
            file_bytes = raw_part
        elif field_name == "format":
            val = raw_part.decode("utf-8", errors="replace").strip()
            if val:
                fmt = val
        elif field_name == "sample_rate":
            raw_sr = raw_part.decode("utf-8", errors="replace").strip()
            if raw_sr:
                try:
                    sample_rate = int(raw_sr)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid sample_rate") from None
        elif field_name == "channels":
            raw_ch = raw_part.decode("utf-8", errors="replace").strip()
            if raw_ch:
                try:
                    channels = int(raw_ch)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid channels") from None

    if file_bytes is None:
        raise HTTPException(status_code=400, detail="Missing 'file' multipart field")
    if len(file_bytes) > max_audio_bytes:
        raise HTTPException(status_code=413, detail="Audio exceeds max_audio_bytes")
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Empty audio upload")
    return file_bytes, fmt, sample_rate, channels
