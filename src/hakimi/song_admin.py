"""Authenticated bounded audio uploads for the cover songbook; protected by admin middleware."""

from __future__ import annotations

from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Request, Response

from .admin_common import read_binary_upload
from .security import Rejected
from .songs import (
    ALLOWED_SONG_MIMES,
    MAX_SONG_DURATION_SECONDS,
    MAX_SONG_LIBRARY_BYTES,
    MAX_SONG_UPLOAD_BYTES,
    MAX_SONGS,
    validate_clip_window,
    validate_song_title,
)

router = APIRouter(prefix="/admin/api/songs")


def library(request: Request):
    return request.app.state.service.songs


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
    return {
        "items": items,
        "max_upload_bytes": MAX_SONG_UPLOAD_BYTES,
        "max_duration_seconds": MAX_SONG_DURATION_SECONDS,
        "max_songs": MAX_SONGS,
        "total_bytes": sum(int(item["byte_size"]) for item in items),
        "max_library_bytes": MAX_SONG_LIBRARY_BYTES,
        "loaded_prompt_count": len(lib.titles),
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
