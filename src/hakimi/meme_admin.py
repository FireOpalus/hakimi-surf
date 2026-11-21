"""Authenticated bounded binary uploads; the admin middleware protects every route."""

from fastapi import APIRouter, HTTPException, Request, Response

from .admin_common import read_binary_upload
from .memes import MAX_UPLOAD_BYTES, label_from_filename
from .security import Rejected

router = APIRouter(prefix="/admin/api/memes")

_ALLOWED_MEME_MIMES = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "application/octet-stream",
    }
)


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
    raw = await read_binary_upload(
        request,
        allowed_mimes=_ALLOWED_MEME_MIMES,
        max_bytes=MAX_UPLOAD_BYTES,
        timeout_seconds=10,
        compressed_error="compressed_meme_upload_forbidden",
        mime_error="meme_image_required",
        size_error="meme_upload_too_large",
        timeout_error="meme_upload_timeout",
    )
    return await library(request).upload(filename, raw)


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
