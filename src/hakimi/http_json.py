"""Bounded JSON reads for trusted upstream endpoints."""

import json

import httpx

from .security import Rejected


async def bounded_json(
    client: httpx.AsyncClient, method: str, url: str, *, max_bytes: int = 1_048_576, **kwargs
) -> dict:
    async with client.stream(method, url, **kwargs) as response:
        response.raise_for_status()
        if 300 <= response.status_code < 400:
            raise Rejected("redirect_not_allowed")
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > max_bytes:
                raise Rejected("upstream_body_too_large")
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise Rejected("upstream_invalid_shape")
    return obj
