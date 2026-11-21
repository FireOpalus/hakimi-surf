"""Authenticated bounded audio uploads and Colab cover controls for the songbook."""

from __future__ import annotations

from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from .admin_common import body, domain_lock, read_binary_upload, service
from .security import Rejected
from .songs import (
    ALLOWED_SONG_MIMES,
    MAX_SONG_DURATION_SECONDS,
    MAX_SONG_LIBRARY_BYTES,
    MAX_SONG_PITCH,
    MAX_SONG_UPLOAD_BYTES,
    MAX_SONGS,
    MIN_SONG_PITCH,
    validate_clip_window,
    validate_song_title,
)

router = APIRouter(prefix="/admin/api/songs")

_BAD_REQUEST_CODES = frozenset(
    {
        "invalid_song_title",
        "invalid_song_meta",
        "invalid_song_pitch",
        "invalid_song_audio",
        "invalid_song_clip",
        "invalid_song_duration",
        "invalid_drive_file_id",
        "song_source_required",
        "song_upload_too_large",
        "colab_invalid_job",
        "colab_drive_invalid_file",
        "invalid_admin_input",
    }
)
_CONFLICT_CODES = frozenset(
    {
        "song_title_exists",
        "song_already_exists",
        "song_library_full",
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
    }
)


class LearnSongBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    title: str = Field(min_length=1, max_length=64)
    artist: str = Field(default="", max_length=80)
    tags: str = Field(default="", max_length=120)
    pitch: int = Field(default=0, ge=MIN_SONG_PITCH, le=MAX_SONG_PITCH)
    inbox_filename: str = Field(default="", max_length=200)
    inbox_file_id: str = Field(default="", max_length=256)
    auto_search: bool = True


def _map_song_rejected(exc: Rejected) -> HTTPException:
    code = str(exc)
    if code in _BAD_REQUEST_CODES:
        return HTTPException(400, code)
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


@router.get("")
async def songs(request: Request):
    lib = library(request)
    items = await lib.list()
    stats = lib.cache_stats()
    return {
        "items": items,
        "max_upload_bytes": MAX_SONG_UPLOAD_BYTES,
        "max_duration_seconds": MAX_SONG_DURATION_SECONDS,
        "max_songs": MAX_SONGS,
        "total_bytes": sum(int(item["byte_size"]) for item in items),
        "max_library_bytes": MAX_SONG_LIBRARY_BYTES,
        "loaded_prompt_count": len(lib.titles),
        **stats,
    }


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
        timeout_seconds=30,
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
                inbox_filename=payload.inbox_filename,
                inbox_file_id=payload.inbox_file_id,
                auto_search=payload.auto_search,
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
