"""FastAPI HTTP and WebSocket application for hakimi-voice-gateway."""

import asyncio
import contextlib
import email.policy
import hmac
import logging
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from email.parser import BytesParser
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.websockets import WebSocketDisconnect

from voice_client.companion_ws import ConnectFactory
from voice_client.providers import (
    BreezeBlueTTS,
    ProviderError,
    STTProvider,
    TTSProvider,
    build_stt_provider,
    build_tts_provider,
)

from .audio_codec import (
    normalize_input_to_wav,
    output_format_meta,
    probe_audio_duration_ms,
    transcode_audio_stream,
)
from .config import VoiceGatewayConfig, load_gateway_config
from .protocol import (
    ProtocolError,
    safe_error_detail,
    validate_adapter_id,
    validate_client_id,
)
from .session import VoiceGatewaySession

log = logging.getLogger("voice_gateway.app")

_STT_PROVIDERS = frozenset(
    {"siliconflow", "fish", "fish_audio", "openai", "openai_compatible", "groq"}
)
_TTS_PROVIDERS = frozenset(
    {"siliconflow", "fish", "fish_audio", "breezeblue", "breeze_blue", "breeze", "edge", "edge_tts"}
)
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _require_bearer_http(request: Request, config: VoiceGatewayConfig) -> None:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer authorization")
    expected = f"Bearer {config.gateway_token.strip()}".encode()
    if not hmac.compare_digest(auth.encode("utf-8"), expected):
        raise HTTPException(status_code=403, detail="Invalid Bearer token")


def _validate_provider_base_url(url: str, field: str) -> str:
    cleaned = str(url or "").strip().rstrip("/")
    if not cleaned:
        raise ValueError(f"{field} must not be empty")
    parsed = urlsplit(cleaned)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Invalid {field} URL")
    if parsed.scheme == "http":
        host = (parsed.hostname or "").lower()
        is_internal = bool(host) and "." not in host and ":" not in host
        if host not in _LOOPBACK_HOSTS and not is_internal:
            raise ValueError(f"{field} requires HTTPS for external hosts")
    return cleaned


def _public_gateway_config(cfg: VoiceGatewayConfig, *, overridden: bool) -> dict[str, Any]:
    return {
        "overridden": overridden,
        "stt_provider": cfg.stt_provider,
        "tts_provider": cfg.tts_provider,
        "tts_model": cfg.resolved_tts_model(),
        "tts_format": cfg.tts_format,
        "tts_speed": cfg.tts_speed,
        "tts_gain": cfg.tts_gain,
        "tts_buffer_mode": cfg.tts_buffer_mode,
        "tts_streaming": cfg.tts_streaming,
        "siliconflow": {
            "configured": bool(cfg.siliconflow_api_key.strip()),
            "base_url": cfg.siliconflow_base_url,
            "stt_model": cfg.siliconflow_stt_model,
            "tts_model": cfg.siliconflow_tts_model,
            "tts_voice": cfg.siliconflow_tts_voice,
        },
        "fish": {
            "configured": bool(cfg.fish_api_key.strip()),
            "base_url": cfg.fish_base_url,
            "reference_id": cfg.fish_reference_id,
            "model": cfg.fish_model,
            "latency": cfg.fish_latency,
            "asr_language": cfg.fish_asr_language,
            "asr_model": cfg.fish_asr_model,
        },
        "breezeblue": {
            "configured": bool(cfg.breezeblue_api_key.strip()),
            "base_url": cfg.breezeblue_base_url,
            "voice_id": cfg.breezeblue_voice_id,
            "model_id": cfg.breezeblue_model_id,
            "language_code": cfg.breezeblue_language_code,
            "instructions": cfg.breezeblue_instructions,
            "guidance_scale": cfg.breezeblue_guidance_scale,
            "speaker_boost": cfg.breezeblue_speaker_boost,
        },
        "openai_stt": {
            "configured": bool(cfg.openai_stt_api_key.strip()),
            "base_url": cfg.openai_stt_base_url,
            "model": cfg.openai_stt_model,
        },
        "edge_tts": {
            "voice": cfg.edge_tts_voice,
        },
    }


def _apply_gateway_override(base: VoiceGatewayConfig, body: dict[str, Any]) -> VoiceGatewayConfig:
    updates: dict[str, Any] = {}

    if "stt_provider" in body and body["stt_provider"] is not None:
        stt = str(body["stt_provider"]).strip().lower()
        if stt not in _STT_PROVIDERS:
            raise ValueError(f"Unsupported stt_provider: {stt!r}")
        updates["stt_provider"] = stt

    if "tts_provider" in body and body["tts_provider"] is not None:
        tts = str(body["tts_provider"]).strip().lower()
        if tts not in _TTS_PROVIDERS:
            raise ValueError(f"Unsupported tts_provider: {tts!r}")
        updates["tts_provider"] = tts

    if "tts_speed" in body and body["tts_speed"] is not None:
        if isinstance(body["tts_speed"], bool):
            raise ValueError("tts_speed must be a number")
        speed = float(body["tts_speed"])
        if not math.isfinite(speed) or not (0.25 <= speed <= 4.0):
            raise ValueError("tts_speed out of range (0.25..4.0)")
        updates["tts_speed"] = speed

    if "tts_gain" in body and body["tts_gain"] is not None:
        if isinstance(body["tts_gain"], bool):
            raise ValueError("tts_gain must be a number")
        gain = float(body["tts_gain"])
        if not math.isfinite(gain) or not (-10.0 <= gain <= 10.0):
            raise ValueError("tts_gain out of range (-10.0..10.0)")
        updates["tts_gain"] = gain

    if "tts_format" in body and body["tts_format"] is not None:
        fmt = str(body["tts_format"]).strip().lower()
        if fmt not in {"mp3", "wav"}:
            raise ValueError(f"Unsupported tts_format: {fmt!r}")
        updates["tts_format"] = fmt

    if "tts_buffer_mode" in body and body["tts_buffer_mode"] is not None:
        buf = str(body["tts_buffer_mode"]).strip().lower()
        if buf not in {"turn", "chunk"}:
            raise ValueError(f"Unsupported tts_buffer_mode: {buf!r}")
        updates["tts_buffer_mode"] = buf

    if "tts_streaming" in body and body["tts_streaming"] is not None:
        if not isinstance(body["tts_streaming"], bool):
            raise ValueError("tts_streaming must be a boolean")
        updates["tts_streaming"] = body["tts_streaming"]

    if "fish_latency" in body and body["fish_latency"] is not None:
        lat = str(body["fish_latency"]).strip().lower()
        if lat not in {"balanced", "normal"}:
            raise ValueError(f"Unsupported fish_latency: {lat!r}")
        updates["fish_latency"] = lat

    url_fields = (
        ("siliconflow_base_url", base.siliconflow_base_url),
        ("fish_base_url", base.fish_base_url),
        ("breezeblue_base_url", base.breezeblue_base_url),
        ("openai_stt_base_url", base.openai_stt_base_url),
    )
    for field_name, default_val in url_fields:
        if field_name in body and body[field_name] is not None:
            raw_url = str(body[field_name]).strip()
            updates[field_name] = _validate_provider_base_url(raw_url or default_val, field_name)

    str_fields = (
        "siliconflow_stt_model",
        "siliconflow_tts_model",
        "siliconflow_tts_voice",
        "fish_reference_id",
        "fish_model",
        "fish_asr_language",
        "fish_asr_model",
        "breezeblue_voice_id",
        "breezeblue_model_id",
        "breezeblue_language_code",
        "breezeblue_instructions",
        "openai_stt_model",
        "edge_tts_voice",
    )
    for field_name in str_fields:
        if field_name in body and body[field_name] is not None:
            updates[field_name] = str(body[field_name]).strip()

    if "breezeblue_guidance_scale" in body:
        raw_gs = body["breezeblue_guidance_scale"]
        if raw_gs is None or (isinstance(raw_gs, str) and not raw_gs.strip()):
            updates["breezeblue_guidance_scale"] = None
        else:
            if isinstance(raw_gs, bool):
                raise ValueError("breezeblue_guidance_scale must be a number")
            gs = float(raw_gs)
            if not math.isfinite(gs) or not (1.0 <= gs <= 10.0):
                raise ValueError("breezeblue_guidance_scale out of range (1.0..10.0)")
            updates["breezeblue_guidance_scale"] = gs

    if "breezeblue_speaker_boost" in body:
        raw_sb = body["breezeblue_speaker_boost"]
        if raw_sb is None:
            updates["breezeblue_speaker_boost"] = None
        else:
            if not isinstance(raw_sb, bool):
                raise ValueError("breezeblue_speaker_boost must be a boolean")
            updates["breezeblue_speaker_boost"] = raw_sb

    secret_pairs = (
        ("siliconflow_api_key", "clear_siliconflow_api_key"),
        ("fish_api_key", "clear_fish_api_key"),
        ("breezeblue_api_key", "clear_breezeblue_api_key"),
        ("openai_stt_api_key", "clear_openai_stt_api_key"),
    )
    for key_field, clear_field in secret_pairs:
        if body.get(clear_field) is True:
            updates[key_field] = ""
        elif isinstance(body.get(key_field), str) and body[key_field].strip():
            updates[key_field] = body[key_field].strip()

    return replace(base, **updates)


async def _close_if_owned(provider: Any, owned: bool) -> None:
    if owned and provider is not None and hasattr(provider, "aclose"):
        with contextlib.suppress(Exception):
            await provider.aclose()


class ProviderBundle:
    """Ref-counted owner of a (config, stt, tts) snapshot with deferred provider closing."""

    def __init__(
        self,
        config: VoiceGatewayConfig,
        stt: STTProvider,
        tts: TTSProvider,
        *,
        owns_stt: bool,
        owns_tts: bool,
        closing_tasks: set[asyncio.Task[None]] | None = None,
    ) -> None:
        self.config = config
        self.stt = stt
        self.tts = tts
        self.owns_stt = owns_stt
        self.owns_tts = owns_tts
        self._closing_tasks = closing_tasks
        self._refs: int = 0
        self._retired: bool = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def active_refs(self) -> int:
        return self._refs

    @property
    def retired(self) -> bool:
        return self._retired

    def acquire(self) -> "ProviderBundle":
        self._refs += 1
        return self

    def _ensure_close_task(self) -> asyncio.Task[None]:
        if self._close_task is None:
            task = asyncio.create_task(self._close_providers())
            self._close_task = task
            if self._closing_tasks is not None:
                self._closing_tasks.add(task)
                task.add_done_callback(self._closing_tasks.discard)
        return self._close_task

    async def release(self) -> None:
        if self._refs > 0:
            self._refs -= 1
        if self._retired and self._refs == 0:
            task = self._ensure_close_task()
            await asyncio.shield(task)

    async def retire(self) -> None:
        self._retired = True
        if self._refs == 0:
            task = self._ensure_close_task()
            await asyncio.shield(task)

    async def _close_providers(self) -> None:
        await _close_if_owned(self.tts, self.owns_tts)
        await _close_if_owned(self.stt, self.owns_stt)


async def _chunk_bytes(data: bytes, chunk_size: int = 16384) -> AsyncIterator[bytes]:
    for idx in range(0, len(data), chunk_size):
        chunk = data[idx : idx + chunk_size]
        if chunk:
            yield chunk


async def _prepend_first(
    first_chunk: bytes,
    rest: AsyncIterator[bytes],
    *,
    on_close: Callable[[], Awaitable[None]] | None = None,
) -> AsyncIterator[bytes]:
    try:
        yield first_chunk
        async for chunk in rest:
            yield chunk
    finally:
        try:
            with contextlib.suppress(Exception):
                await rest.aclose()
        finally:
            if on_close is not None:
                await on_close()


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
        try:
            tts = tts_provider if tts_provider is not None else build_tts_provider(client_cfg)
        except Exception:
            await _close_if_owned(stt, stt_provider is None)
            raise

        closing_tasks: set[asyncio.Task[None]] = set()
        initial_bundle = ProviderBundle(
            resolved_config,
            stt,
            tts,
            owns_stt=stt_provider is None,
            owns_tts=tts_provider is None,
            closing_tasks=closing_tasks,
        )
        app.state.base_config = resolved_config
        app.state.config = resolved_config
        app.state.overridden = False
        app.state.config_lock = asyncio.Lock()
        app.state.closing_tasks = closing_tasks
        app.state.provider_bundle = initial_bundle
        app.state.owns_stt = stt_provider is None
        app.state.owns_tts = tts_provider is None
        app.state.stt = stt
        app.state.tts = tts
        app.state.upstream_connect_factory = upstream_connect_factory
        app.state.ffmpeg_semaphore = asyncio.Semaphore(
            max(1, resolved_config.max_ffmpeg_concurrency)
        )
        try:
            yield
        finally:
            await app.state.provider_bundle.retire()
            pending_close = list(app.state.closing_tasks)
            if pending_close:
                await asyncio.gather(*pending_close, return_exceptions=True)

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

    @app.get("/v1/admin/config")
    async def get_admin_config(request: Request) -> dict[str, Any]:
        _require_bearer_http(request, request.app.state.base_config)
        return _public_gateway_config(
            request.app.state.config,
            overridden=bool(request.app.state.overridden),
        )

    @app.post("/v1/admin/config")
    async def post_admin_config(request: Request) -> dict[str, Any]:
        base_cfg: VoiceGatewayConfig = request.app.state.base_config
        _require_bearer_http(request, base_cfg)
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON request body") from None
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")

        new_cfg = base_cfg
        old_bundle: ProviderBundle | None = None
        failed_stt: STTProvider | None = None
        try:
            new_cfg = _apply_gateway_override(base_cfg, body)
            async with request.app.state.config_lock:
                if new_cfg == request.app.state.config and (
                    request.app.state.overridden
                    or (request.app.state.owns_stt and request.app.state.owns_tts)
                ):
                    request.app.state.overridden = True
                    return _public_gateway_config(request.app.state.config, overridden=True)

                client_cfg = new_cfg.to_client_config()
                new_stt = build_stt_provider(client_cfg)
                try:
                    new_tts = build_tts_provider(client_cfg)
                except Exception:
                    failed_stt = new_stt
                    raise

                new_bundle = ProviderBundle(
                    new_cfg,
                    new_stt,
                    new_tts,
                    owns_stt=True,
                    owns_tts=True,
                    closing_tasks=request.app.state.closing_tasks,
                )
                old_bundle = request.app.state.provider_bundle
                request.app.state.provider_bundle = new_bundle
                request.app.state.config = new_cfg
                request.app.state.stt = new_stt
                request.app.state.tts = new_tts
                request.app.state.owns_stt = True
                request.app.state.owns_tts = True
                request.app.state.overridden = True
        except ValueError as exc:
            if failed_stt is not None:
                await _close_if_owned(failed_stt, True)
            raise HTTPException(status_code=400, detail=safe_error_detail(exc, new_cfg)) from None
        except Exception:
            if failed_stt is not None:
                await _close_if_owned(failed_stt, True)
            raise

        if old_bundle is not None:
            await old_bundle.retire()
        return _public_gateway_config(new_cfg, overridden=True)

    @app.delete("/v1/admin/config")
    async def delete_admin_config(request: Request) -> dict[str, Any]:
        base_cfg: VoiceGatewayConfig = request.app.state.base_config
        _require_bearer_http(request, base_cfg)
        old_bundle: ProviderBundle | None = None
        failed_stt: STTProvider | None = None
        try:
            async with request.app.state.config_lock:
                if not request.app.state.overridden and request.app.state.config == base_cfg:
                    return _public_gateway_config(request.app.state.config, overridden=False)
                if (
                    request.app.state.config == base_cfg
                    and request.app.state.owns_stt
                    and request.app.state.owns_tts
                ):
                    request.app.state.overridden = False
                    return _public_gateway_config(request.app.state.config, overridden=False)

                client_cfg = base_cfg.to_client_config()
                new_stt = build_stt_provider(client_cfg)
                try:
                    new_tts = build_tts_provider(client_cfg)
                except Exception:
                    failed_stt = new_stt
                    raise

                new_bundle = ProviderBundle(
                    base_cfg,
                    new_stt,
                    new_tts,
                    owns_stt=True,
                    owns_tts=True,
                    closing_tasks=request.app.state.closing_tasks,
                )
                old_bundle = request.app.state.provider_bundle
                request.app.state.provider_bundle = new_bundle
                request.app.state.config = base_cfg
                request.app.state.stt = new_stt
                request.app.state.tts = new_tts
                request.app.state.owns_stt = True
                request.app.state.owns_tts = True
                request.app.state.overridden = False
        except Exception:
            if failed_stt is not None:
                await _close_if_owned(failed_stt, True)
            raise

        if old_bundle is not None:
            await old_bundle.retire()
        return _public_gateway_config(base_cfg, overridden=False)

    @app.get("/v1/admin/voices")
    async def get_admin_voices(request: Request) -> dict[str, Any]:
        _require_bearer_http(request, request.app.state.base_config)
        cfg: VoiceGatewayConfig = request.app.state.config
        api_key = cfg.breezeblue_api_key.strip()
        if not api_key:
            raise HTTPException(status_code=400, detail="Missing BREEZEBLUE_API_KEY")
        client = BreezeBlueTTS(
            api_key=api_key,
            voice_id="unused",
            base_url=cfg.breezeblue_base_url,
        )
        try:
            raw_voices = await client.list_voices()
        except ProviderError as exc:
            raise HTTPException(status_code=502, detail=safe_error_detail(exc, cfg)) from None
        finally:
            await _close_if_owned(client, True)

        items = []
        for entry in raw_voices:
            vid = str(entry.get("voice_id") or entry.get("id") or "").strip()
            if not vid:
                continue
            items.append(
                {
                    "voice_id": vid,
                    "name": str(entry.get("name") or "").strip(),
                    "voice_type": str(entry.get("voice_type") or "").strip(),
                    "description": str(entry.get("description") or "").strip(),
                }
            )
        items.sort(
            key=lambda item: (
                0 if item["voice_type"] == "personal" else 1,
                item["name"],
                item["voice_id"],
            )
        )
        return {"voices": items}

    @app.post("/v1/stt")
    async def post_stt(request: Request) -> dict[str, Any]:
        _require_bearer_http(request, request.app.state.base_config)
        bundle: ProviderBundle = request.app.state.provider_bundle.acquire()
        try:
            cfg = bundle.config
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
                    ffmpeg_semaphore=request.app.state.ffmpeg_semaphore,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=safe_error_detail(exc, cfg)) from None
            except (RuntimeError, TimeoutError) as exc:
                raise HTTPException(status_code=500, detail=safe_error_detail(exc, cfg)) from None

            stt = bundle.stt
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
        finally:
            await bundle.release()

    @app.post("/v1/tts")
    async def post_tts(request: Request) -> Response:
        _require_bearer_http(request, request.app.state.base_config)
        bundle: ProviderBundle = request.app.state.provider_bundle.acquire()
        released = False

        async def _release_once() -> None:
            nonlocal released
            if not released:
                released = True
                await bundle.release()

        handed_off = False
        try:
            cfg = bundle.config
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
            if len(text) > cfg.max_tts_chars:
                raise HTTPException(
                    status_code=400,
                    detail=f"text exceeds max_tts_chars ({cfg.max_tts_chars})",
                )

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

            tts = bundle.tts
            synth_stream = getattr(tts, "synthesize_stream", None)
            has_synth = callable(getattr(tts, "synthesize", None))
            try:
                if (cfg.tts_streaming and callable(synth_stream)) or not has_synth:
                    assert callable(synth_stream)
                    src_fmt = (
                        str(getattr(tts, "response_format", "mp3") or "mp3").lstrip(".").lower()
                    )
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
                    ffmpeg_semaphore=request.app.state.ffmpeg_semaphore,
                )
                try:
                    first_chunk = await anext(transcoded)
                except BaseException:
                    with contextlib.suppress(Exception):
                        await transcoded.aclose()
                    raise
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
                handed_off = True
                return StreamingResponse(
                    _prepend_first(first_chunk, transcoded, on_close=_release_once),
                    media_type=mime_type,
                    headers=headers,
                    background=BackgroundTask(_release_once),
                )

            buf = bytearray()
            try:
                if len(first_chunk) > cfg.max_tts_audio_bytes:
                    raise HTTPException(
                        status_code=502,
                        detail="TTS audio exceeds max_tts_audio_bytes",
                    )
                buf.extend(first_chunk)
                async for chunk in transcoded:
                    if len(buf) + len(chunk) > cfg.max_tts_audio_bytes:
                        raise HTTPException(
                            status_code=502,
                            detail="TTS audio exceeds max_tts_audio_bytes",
                        )
                    buf.extend(chunk)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=502, detail=safe_error_detail(exc, cfg)) from None
            finally:
                with contextlib.suppress(Exception):
                    await transcoded.aclose()

            audio_bytes = bytes(buf)
            duration_ms = await probe_audio_duration_ms(
                audio_bytes,
                output_fmt,
                ffmpeg_bin=cfg.ffmpeg_bin,
                ffmpeg_semaphore=request.app.state.ffmpeg_semaphore,
            )
            if duration_ms is not None and duration_ms > 0:
                headers["X-Audio-Duration-Ms"] = str(duration_ms)
            return Response(content=audio_bytes, media_type=mime_type, headers=headers)
        finally:
            if not handed_off:
                await _release_once()

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

        @asynccontextmanager
        async def _lease_ws_providers() -> AsyncIterator[
            tuple[VoiceGatewayConfig, STTProvider, TTSProvider]
        ]:
            bundle: ProviderBundle = websocket.app.state.provider_bundle.acquire()
            try:
                yield bundle.config, bundle.stt, bundle.tts
            finally:
                await bundle.release()

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
            ffmpeg_semaphore=websocket.app.state.ffmpeg_semaphore,
            provider_lease=_lease_ws_providers,
            config_getter=lambda: websocket.app.state.config,
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
