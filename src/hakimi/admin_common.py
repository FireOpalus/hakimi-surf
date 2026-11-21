"""Shared helpers for authenticated admin control-plane routers."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Collection
from typing import Any

from fastapi import HTTPException, Request
from pydantic import BaseModel, ValidationError

_SAFE_FIELD_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,39}$")


def service(request: Request):
    """Return the active Service instance bound to the FastAPI application state."""
    return request.app.state.service


def domain_lock(request: Request, domain: str) -> asyncio.Lock:
    """Return a lazily-created per-domain asyncio.Lock stored on app.state.admin_domain_locks."""
    state = request.app.state
    locks = getattr(state, "admin_domain_locks", None)
    if locks is None:
        locks = {}
        state.admin_domain_locks = locks
    lock = locks.get(domain)
    if lock is None:
        lock = asyncio.Lock()
        locks[domain] = lock
    return lock


def _validation_error_field(exc: ValidationError) -> str | None:
    """Extract a safe schema field path from ValidationError without reflecting values."""
    for err in exc.errors():
        if err.get("type") == "extra_forbidden":
            return "unknown_field"
        loc = err.get("loc") or ()
        parts: list[str] = []
        for item in loc:
            part = str(item)
            if not _SAFE_FIELD_PART_RE.fullmatch(part):
                break
            parts.append(part)
        if parts:
            joined = ".".join(parts)
            if len(joined) <= 64:
                return joined
    return None


def invalid_input_http_error(
    exc: ValidationError | None = None, *, detail: str = "invalid_admin_input"
) -> HTTPException:
    """Build a 400 HTTPException carrying a safe X-Invalid-Field hint when available."""
    field = _validation_error_field(exc) if exc is not None else None
    headers = {"X-Invalid-Field": field} if field else None
    return HTTPException(400, detail, headers=headers)


async def body(
    request: Request,
    model: type[BaseModel],
    *,
    error_detail: str = "invalid_admin_input",
) -> Any:
    """Read, strictly parse, and validate a JSON object request body against a Pydantic model."""
    from .app import json_object, read_body

    try:
        return model.model_validate(
            json_object(await read_body(request, request.app.state.settings))
        )
    except ValidationError as exc:
        raise invalid_input_http_error(exc, detail=error_detail) from None


async def partial_body(
    request: Request,
    current: dict[str, Any],
    model: type[BaseModel],
    *,
    allow_empty: bool = False,
    error_detail: str = "invalid_admin_input",
) -> Any:
    """Merge a non-empty JSON object patch over current config and validate with model."""
    from .app import json_object, read_body

    raw = json_object(await read_body(request, request.app.state.settings))
    if not raw and not allow_empty:
        raise HTTPException(400, error_detail)
    try:
        return model.model_validate({**current, **raw})
    except ValidationError as exc:
        raise invalid_input_http_error(exc, detail=error_detail) from None


async def read_binary_upload(
    request: Request,
    *,
    allowed_mimes: Collection[str],
    max_bytes: int,
    timeout_seconds: float,
    compressed_error: str,
    mime_error: str,
    size_error: str,
    timeout_error: str,
) -> bytes:
    """Stream a bounded, uncompressed binary upload with strict MIME, size, and timeout checks."""
    if request.headers.get("content-encoding", "identity") != "identity":
        raise HTTPException(415, compressed_error)
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type not in allowed_mimes:
        raise HTTPException(415, mime_error)
    try:
        declared = int(request.headers.get("content-length", "0"))
    except ValueError:
        raise HTTPException(400, "invalid_content_length") from None
    if declared > max_bytes or declared < 0:
        raise HTTPException(413, size_error)
    raw = bytearray()
    try:
        async with asyncio.timeout(timeout_seconds):
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > max_bytes:
                    raise HTTPException(413, size_error)
    except TimeoutError:
        raise HTTPException(408, timeout_error) from None
    return bytes(raw)
