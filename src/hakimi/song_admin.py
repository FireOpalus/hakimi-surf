"""Authenticated bounded audio uploads, Drive storage/inbox, and Colab SVC/RVC cover controls."""

from __future__ import annotations

import asyncio
from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from .admin_common import body, domain_lock, read_binary_upload, service
from .security import Rejected
from .songs import (
    ALLOWED_SONG_EXTENSIONS,
    ALLOWED_SONG_MIMES,
    AVAILABLE_COVER_ENGINES,
    DEFAULT_COVER_ENGINE,
    MAX_BACKING_GAIN_DB,
    MAX_REF_VOICE_UPLOAD_BYTES,
    MAX_SONG_DURATION_SECONDS,
    MAX_SONG_LIBRARY_BYTES,
    MAX_SONG_PITCH,
    MAX_SONG_UPLOAD_BYTES,
    MAX_SONGS,
    MIN_BACKING_GAIN_DB,
    MIN_SONG_PITCH,
    _safe_learn_upload_name,
    validate_clip_window,
    validate_song_title,
)

router = APIRouter(prefix="/admin/api/songs")

MAX_ADMIN_INBOX_UPLOAD_BYTES = MAX_SONG_UPLOAD_BYTES
MAX_ADMIN_REF_VOICE_UPLOAD_BYTES = MAX_REF_VOICE_UPLOAD_BYTES

_BAD_REQUEST_CODES = frozenset(
    {
        "invalid_song_title",
        "invalid_song_meta",
        "invalid_song_pitch",
        "invalid_song_engine",
        "invalid_song_take",
        "invalid_ref_voice",
        "invalid_song_audio",
        "invalid_song_clip",
        "invalid_song_duration",
        "invalid_drive_file_id",
        "song_source_required",
        "colab_invalid_job",
        "colab_gpu_not_allowed",
        "colab_drive_invalid_file",
        "song_drive_invalid_file",
        "song_drive_file_outside_scope",
        "song_drive_file_too_large",
        "invalid_admin_input",
        "invalid_query",
        "duration_out_of_range",
        "ssrf_blocked",
    }
)
_PAYLOAD_TOO_LARGE_CODES = frozenset(
    {
        "song_upload_too_large",
        "song_inbox_upload_too_large",
        "song_ref_voice_too_large",
        "colab_inbox_upload_too_large",
        "download_too_large",
    }
)
_NOT_FOUND_CODES = frozenset(
    {
        "song_not_found",
        "song_take_not_found",
        "ref_voice_not_found",
        "model_not_found",
        "song_drive_not_found",
        "source_not_found",
    }
)
_CONFLICT_CODES = frozenset(
    {
        "song_title_exists",
        "song_already_exists",
        "song_library_full",
        "song_take_unavailable",
        "svc_all_engines_failed",
        "colab_disabled",
        "colab_daily_job_limit",
        "colab_daily_cu_budget",
        "colab_daily_limit_reached",
        "colab_daily_limit",
        "colab_budget_exceeded",
        "colab_job_limit_reached",
    }
)
_UNAVAILABLE_CODES = frozenset(
    {
        "colab_unconfigured",
        "colab_worker_unavailable",
        "colab_worker_timeout",
        "colab_worker_error",
        "colab_drive_not_configured",
        "colab_request_failed",
        "song_drive_not_configured",
        "song_drive_credentials_invalid",
        "song_drive_insufficient_scope",
        "song_drive_reauth_required",
        "song_drive_token_failed",
        "song_drive_folder_unavailable",
        "song_drive_forbidden",
        "song_drive_request_failed",
        "song_drive_upload_failed",
        "song_drive_download_failed",
        "song_drive_network_error",
        "song_drive_network_timeout",
        "song_drive_invalid_response",
        "song_drive_response_too_large",
        "song_drive_disallowed_endpoint",
        "song_finder_remote_unavailable",
        "all_providers_in_cooldown",
        "transcode_failed",
    }
)


class LearnSongBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    title: str = Field(min_length=1, max_length=64)
    artist: str = Field(default="", max_length=80)
    tags: str = Field(default="", max_length=120)
    pitch: int = Field(default=0, ge=MIN_SONG_PITCH, le=MAX_SONG_PITCH)
    engine: str = Field(default=DEFAULT_COVER_ENGINE, max_length=32)
    ref_voice: str = Field(default="", max_length=160)
    ref_audio_file_id: str = Field(default="", max_length=128)
    svc_steps: int | None = Field(default=None, ge=10, le=100)
    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    index_rate: float | int | None = Field(default=None, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float | int | None = Field(default=None, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float | int | None = Field(default=None, ge=0.0, le=1.0, allow_inf_nan=False)
    backing_gain_db: float | int | None = Field(
        default=None,
        ge=MIN_BACKING_GAIN_DB,
        le=MAX_BACKING_GAIN_DB,
        allow_inf_nan=False,
    )
    skip_backing: bool | None = None
    skip_dereverb: bool | None = None
    inbox_filename: str = Field(default="", max_length=200)
    inbox_file_id: str = Field(default="", max_length=256)
    auto_search: bool = True
    gpu: str | None = Field(default=None, max_length=16)


class SongStorageBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    drive_songs_folder_id: str = Field(default="", max_length=128)


class SearchSourcesBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    query: str = Field(min_length=1, max_length=160)
    artist: str = Field(default="", max_length=60)
    strategy: str = Field(default="auto", max_length=32)
    limit: int = Field(default=6, ge=1, le=10)


class FetchToInboxBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    query: str = Field(default="", max_length=160)
    title: str = Field(default="", max_length=64)
    artist: str = Field(default="", max_length=60)
    candidate_id: str = Field(default="", max_length=64)
    strategy: str = Field(default="auto", max_length=32)


def _map_song_rejected(exc: Rejected) -> HTTPException:
    code = str(exc)
    if code in _BAD_REQUEST_CODES:
        return HTTPException(400, code)
    if code in _PAYLOAD_TOO_LARGE_CODES:
        return HTTPException(413, code)
    if code in _NOT_FOUND_CODES:
        return HTTPException(404, code)
    if code == "compressed_song_upload_forbidden":
        return HTTPException(415, code)
    if code in {
        "song_upload_timeout",
        "song_inbox_upload_timeout",
        "song_ref_voice_upload_timeout",
    }:
        return HTTPException(408, code)
    if code in _UNAVAILABLE_CODES:
        return HTTPException(503, code)
    if code in _CONFLICT_CODES:
        return HTTPException(409, code)
    return HTTPException(409, code)


def library(request: Request):
    svc = service(request)
    lib = getattr(svc, "songs", None) or getattr(svc.store.runtime, "songs", None)
    if lib is None:
        raise HTTPException(503, "song_studio_unavailable")
    return lib


def _decode_header(request: Request, name: str, *, error_code: str) -> str:
    raw = request.headers.get(name, "")
    if len(raw) > 512:
        raise Rejected(error_code)
    return unquote(raw)


def _float_header(request: Request, name: str, *, default: float) -> float:
    raw = request.headers.get(name, "").strip()
    if not raw:
        return default
    if len(raw) > 32:
        raise Rejected("invalid_song_clip")
    try:
        return float(raw)
    except ValueError:
        raise Rejected("invalid_song_clip") from None


def _parse_stream_audio_headers(
    request: Request,
    *,
    max_bytes: int,
    invalid_name_code: str,
    invalid_audio_code: str,
    too_large_code: str,
    fallback_stem: str,
) -> tuple[str, str, int | None]:
    raw_header = request.headers.get("x-drive-filename", "")
    if len(raw_header) > 512:
        raise HTTPException(400, invalid_name_code)
    decoded_name = unquote(raw_header).strip()
    if (
        not decoded_name
        or "/" in decoded_name
        or "\\" in decoded_name
        or ".." in decoded_name
        or any(ord(ch) < 32 for ch in decoded_name)
    ):
        raise HTTPException(400, invalid_name_code)
    _, dot, ext = decoded_name.rpartition(".")
    if not dot or ext.lower() not in ALLOWED_SONG_EXTENSIONS:
        raise HTTPException(400, invalid_audio_code)
    try:
        safe_name, mime_type = _safe_learn_upload_name(decoded_name, fallback_stem)
    except Rejected as exc:
        raise _map_song_rejected(exc) from None

    if request.headers.get("content-encoding", "identity") != "identity":
        raise HTTPException(415, "compressed_song_upload_forbidden")
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type not in ALLOWED_SONG_MIMES:
        raise HTTPException(415, invalid_audio_code)

    raw_len = request.headers.get("content-length")
    declared: int | None = None
    if raw_len is not None and raw_len.strip() != "":
        try:
            declared = int(raw_len.strip())
        except ValueError:
            raise HTTPException(400, "invalid_content_length") from None
        if declared < 0:
            raise HTTPException(400, "invalid_content_length")
        if declared > max_bytes:
            raise HTTPException(413, too_large_code)
        if declared == 0:
            raise HTTPException(400, invalid_audio_code)
    return safe_name, mime_type, declared


@router.get("")
async def songs(request: Request):
    lib = library(request)
    items = await lib.list()
    stats = lib.cache_stats()
    drive_status = await lib.drive_storage_status(probe=False)
    return {
        "items": items,
        "max_upload_bytes": MAX_SONG_UPLOAD_BYTES,
        "max_duration_seconds": MAX_SONG_DURATION_SECONDS,
        "max_songs": MAX_SONGS,
        "total_bytes": sum(int(item["byte_size"]) for item in items),
        "max_library_bytes": MAX_SONG_LIBRARY_BYTES,
        "loaded_prompt_count": len(lib.titles),
        **stats,
        "drive_configured": bool(drive_status.get("configured", False)),
        "drive_songs_folder_id": str(drive_status.get("drive_songs_folder_id") or ""),
        "drive_config_source": str(drive_status.get("source") or "none"),
        "drive_folders": drive_status.get("folders"),
        "drive_error": drive_status.get("error"),
        "learn_gpu_policy": lib.learn_gpu_policy(),
        "default_engine": DEFAULT_COVER_ENGINE,
        "available_engines": list(AVAILABLE_COVER_ENGINES),
        "rvc_defaults": lib.rvc_defaults(),
    }


@router.get("/storage")
async def song_storage(request: Request):
    lib = library(request)
    return await lib.drive_storage_status(probe=True)


@router.put("/storage")
async def update_song_storage(request: Request):
    payload = await body(request, SongStorageBody)
    async with domain_lock(request, "songs"):
        try:
            return await library(request).set_drive_folder_id(payload.drive_songs_folder_id)
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.delete("/storage")
async def reset_song_storage(request: Request):
    async with domain_lock(request, "songs"):
        try:
            return await library(request).set_drive_folder_id(None)
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.get("/inbox")
async def song_inbox(request: Request):
    try:
        return await library(request).list_inbox()
    except Rejected as exc:
        raise _map_song_rejected(exc) from None


@router.post("/inbox")
async def upload_song_inbox(request: Request):
    safe_name, mime_type, declared = _parse_stream_audio_headers(
        request,
        max_bytes=MAX_ADMIN_INBOX_UPLOAD_BYTES,
        invalid_name_code="song_drive_invalid_file",
        invalid_audio_code="invalid_song_audio",
        too_large_code="song_inbox_upload_too_large",
        fallback_stem="track",
    )
    try:
        async with asyncio.timeout(300):
            res = await library(request).upload_inbox_stream(
                safe_name,
                request.stream(),
                mime_type=mime_type,
                content_length=declared,
                max_bytes=MAX_ADMIN_INBOX_UPLOAD_BYTES,
            )
    except TimeoutError:
        raise HTTPException(408, "song_inbox_upload_timeout") from None
    except Rejected as exc:
        raise _map_song_rejected(exc) from None
    return {"file_id": res["file_id"], "name": res["name"], "size": res["size"]}


@router.get("/voices")
async def song_voices(request: Request):
    try:
        return await library(request).list_voices()
    except Rejected as exc:
        raise _map_song_rejected(exc) from None


@router.post("/ref-voices")
async def upload_song_ref_voice(request: Request):
    safe_name, mime_type, declared = _parse_stream_audio_headers(
        request,
        max_bytes=MAX_ADMIN_REF_VOICE_UPLOAD_BYTES,
        invalid_name_code="invalid_ref_voice",
        invalid_audio_code="invalid_ref_voice",
        too_large_code="song_ref_voice_too_large",
        fallback_stem="ref_voice",
    )
    try:
        async with asyncio.timeout(180):
            res = await library(request).upload_ref_voice_stream(
                safe_name,
                request.stream(),
                mime_type=mime_type,
                content_length=declared,
                max_bytes=MAX_ADMIN_REF_VOICE_UPLOAD_BYTES,
            )
    except TimeoutError:
        raise HTTPException(408, "song_ref_voice_upload_timeout") from None
    except Rejected as exc:
        raise _map_song_rejected(exc) from None
    return {"file_id": res["file_id"], "name": res["name"], "size": res["size"]}


@router.post("/upload")
async def upload(request: Request):
    title = validate_song_title(
        _decode_header(request, "x-song-title", error_code="invalid_song_title")
    )
    artist = _decode_header(request, "x-song-artist", error_code="invalid_song_meta")
    tags = _decode_header(request, "x-song-tags", error_code="invalid_song_meta")
    filename = _decode_header(request, "x-song-filename", error_code="invalid_song_meta")
    start_sec, duration_sec = validate_clip_window(
        _float_header(request, "x-song-start", default=0.0),
        _float_header(request, "x-song-duration", default=MAX_SONG_DURATION_SECONDS),
    )
    raw = await read_binary_upload(
        request,
        allowed_mimes=ALLOWED_SONG_MIMES,
        max_bytes=MAX_SONG_UPLOAD_BYTES,
        timeout_seconds=180,
        compressed_error="compressed_song_upload_forbidden",
        mime_error="song_audio_required",
        size_error="song_upload_too_large",
        timeout_error="song_upload_timeout",
    )
    return await library(request).upload(
        title,
        raw,
        artist=artist,
        tags=tags,
        filename=filename,
        start_sec=start_sec,
        duration_sec=duration_sec,
    )


@router.post("/search-sources")
async def search_sources(request: Request):
    payload = await body(request, SearchSourcesBody)
    try:
        return await library(request).search_remote_sources(
            payload.query,
            artist=payload.artist,
            strategy=payload.strategy,
            limit=payload.limit,
        )
    except Rejected as exc:
        raise _map_song_rejected(exc) from None


@router.post("/fetch-to-inbox")
async def fetch_to_inbox(request: Request):
    payload = await body(request, FetchToInboxBody)
    async with domain_lock(request, "songs"):
        try:
            return await library(request).fetch_remote_source_to_inbox(
                payload.query,
                title=payload.title,
                artist=payload.artist,
                candidate_id=payload.candidate_id,
                strategy=payload.strategy,
            )
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.post("/learn")
async def learn(request: Request):
    payload = await body(request, LearnSongBody)
    async with domain_lock(request, "songs"):
        try:
            return await library(request).request_learn_song(
                payload.title,
                artist=payload.artist,
                tags=payload.tags,
                pitch=payload.pitch,
                engine=payload.engine,
                ref_voice=payload.ref_voice,
                ref_audio_file_id=payload.ref_audio_file_id,
                svc_steps=payload.svc_steps,
                model_name=payload.model_name,
                index_rate=(float(payload.index_rate) if payload.index_rate is not None else None),
                protect=(float(payload.protect) if payload.protect is not None else None),
                volume_envelope=(
                    float(payload.volume_envelope) if payload.volume_envelope is not None else None
                ),
                backing_gain_db=(
                    float(payload.backing_gain_db) if payload.backing_gain_db is not None else None
                ),
                skip_backing=payload.skip_backing,
                skip_dereverb=payload.skip_dereverb,
                inbox_filename=payload.inbox_filename,
                inbox_file_id=payload.inbox_file_id,
                auto_search=payload.auto_search,
                gpu=payload.gpu,
                requester="admin",
            )
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.post("/cache/evict")
async def evict_cache(request: Request):
    async with domain_lock(request, "songs"):
        try:
            return await library(request).evict_cache()
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.get("/{song_id}/takes/{take_key}/audio")
async def preview_take(request: Request, song_id: str, take_key: str):
    try:
        raw, mime = await library(request).take_audio_bytes(song_id, take_key)
    except Rejected as exc:
        raise _map_song_rejected(exc) from None
    return Response(raw, media_type=mime)


@router.post("/{song_id}/takes/{take_key}/activate")
async def activate_take(request: Request, song_id: str, take_key: str):
    async with domain_lock(request, "songs"):
        try:
            return await library(request).switch_take(song_id, take_key, by_title=False)
        except Rejected as exc:
            raise _map_song_rejected(exc) from None


@router.get("/{song_id}/audio")
async def preview(request: Request, song_id: str):
    try:
        raw, mime = await library(request).audio_bytes(song_id)
    except Rejected:
        raise HTTPException(404, "song_audio_unavailable") from None
    return Response(raw, media_type=mime)


@router.delete("/{song_id}")
async def remove(request: Request, song_id: str):
    try:
        await library(request).delete(song_id)
    except Rejected as exc:
        if str(exc) == "song_not_found":
            raise HTTPException(404, "song_not_found") from None
        raise
    return {"status": "deleted"}
