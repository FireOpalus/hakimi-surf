"""Authenticated bounded binary uploads; the admin middleware protects every route."""

import asyncio

from fastapi import APIRouter, HTTPException, Request, Response

from .memes import MAX_UPLOAD_BYTES, label_from_filename
from .security import Rejected

router = APIRouter(prefix="/admin/api/memes")


def library(request):
    return request.app.state.service.memes


@router.get("")
async def memes(request: Request):
    lib = library(request)
    return {
        "items": await lib.list(),
        "max_upload_bytes": MAX_UPLOAD_BYTES,
        "loaded_prompt_count": len(lib.labels),
    }


@router.post("/upload/{filename}")
async def upload(request: Request, filename: str):
    label_from_filename(filename)
    if request.headers.get("content-encoding", "identity") != "identity":
        raise HTTPException(415, "compressed_meme_upload_forbidden")
    if request.headers.get("content-type", "").split(";")[0] not in {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "application/octet-stream",
    }:
        raise HTTPException(415, "meme_image_required")
    try:
        declared = int(request.headers.get("content-length", "0"))
    except ValueError:
        raise HTTPException(400, "invalid_content_length") from None
    if declared > MAX_UPLOAD_BYTES or declared < 0:
        raise HTTPException(413, "meme_upload_too_large")
    raw = bytearray()
    try:
        async with asyncio.timeout(10):
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "meme_upload_too_large")
    except TimeoutError:
        raise HTTPException(408, "meme_upload_timeout") from None
    return await library(request).upload(filename, bytes(raw))


@router.get("/{key}/image")
async def preview(request: Request, key: str):
    try:
        raw, mime = await library(request).preview(key)
    except Rejected:
        raise HTTPException(404, "meme_image_unavailable") from None
    return Response(raw, media_type=mime)


@router.delete("/{key}")
async def remove(request: Request, key: str):
    await library(request).delete(key)
    return {"status": "deleted"}
