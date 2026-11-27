"""Bounded SSE assembly. Never publish partial JSON, thoughts, or unverified tool output."""

import asyncio
import inspect
import re
from types import SimpleNamespace

from .security import Rejected


def upstream_error_code(value: object) -> str:
    """Keep only bounded machine codes. Provider error messages can contain private input."""
    if isinstance(value, (str, int)):
        code = str(value)
        if re.fullmatch(r"[45][0-9]{2}|[a-z][a-z0-9_]{0,47}|[A-Z][A-Z0-9_]{0,47}", code):
            return code
    return "unknown"


_UPSTREAM_ERROR_HTTP_STATUSES = {
    "bad_request": 400,
    "invalid_argument": 400,
    "failed_precondition": 400,
    "out_of_range": 400,
    "unauthenticated": 401,
    "unauthorized": 401,
    "permission_denied": 403,
    "forbidden": 403,
    "not_found": 404,
    "already_exists": 409,
    "aborted": 409,
    "conflict": 409,
    "payload_too_large": 413,
    "request_entity_too_large": 413,
    "unprocessable_entity": 422,
    "rate_limit_exceeded": 429,
    "quota_exceeded": 429,
    "too_many_requests": 429,
    "resource_exhausted": 429,
    "rate_limited": 429,
}


def upstream_http_status(code: str | None) -> int | None:
    if not code:
        return None
    if re.fullmatch(r"[45][0-9]{2}", code):
        return int(code)
    return _UPSTREAM_ERROR_HTTP_STATUSES.get(code.lower())


class StreamProgress:
    """Tracks stream execution state and assembled output for interrupted billing fallback."""

    def __init__(self, *, on_text=None) -> None:
        self.dispatched = False
        self.started = False
        self.text_parts: list[str] = []
        self.thought_steps = 0
        self.has_non_text_output = False
        self.error_code: str | None = None
        self._on_text = on_text

    async def emit_text(self, chunk: str) -> None:
        if not chunk:
            return
        self.text_parts.append(chunk)
        if self._on_text is not None:
            res = self._on_text(chunk)
            if inspect.isawaitable(res):
                await res

    @property
    def output_text(self) -> str:
        return "".join(self.text_parts)

    @property
    def has_emitted_output(self) -> bool:
        return bool(self.output_text or self.has_non_text_output)


async def consume_stream(stream, progress: StreamProgress | None = None):
    steps = {}
    length = 0
    service_tier = None
    usage_metadata = None
    try:
        async for event in stream:
            if progress is not None:
                progress.started = True
            ev_interaction = getattr(event, "interaction", None)
            if ev_interaction is not None:
                ev_tier = getattr(ev_interaction, "service_tier", None)
                if ev_tier is not None:
                    service_tier = ev_tier
                ev_um = getattr(ev_interaction, "usage_metadata", None)
                if ev_um is not None:
                    usage_metadata = ev_um
            kind = event.event_type
            if kind == "step.start":
                step = event.step
                if step.type not in {"thought", "model_output"}:
                    raise Rejected("unexpected_model_action")
                if step.type == "thought" and progress is not None:
                    progress.thought_steps += 1
                if len(steps) >= 32 or event.index in steps:
                    raise Rejected("invalid_stream")
                pieces = []
                if step.type == "model_output":
                    for part in getattr(step, "content", []) or []:
                        if part.type != "text":
                            raise Rejected("unexpected_model_content")
                        pieces.append(part.text or "")
                    if progress is not None and pieces:
                        await progress.emit_text("".join(pieces))
                steps[event.index] = (step.type, pieces)
            elif kind == "step.delta":
                if event.index not in steps:
                    raise Rejected("invalid_stream")
                step_type, pieces = steps[event.index]
                if step_type == "model_output":
                    if event.delta.type != "text":
                        raise Rejected("unexpected_model_content")
                    text = event.delta.text or ""
                    length += len(text)
                    if length > 16000:
                        raise Rejected("invalid_output")
                    pieces.append(text)
                    if progress is not None and text:
                        await progress.emit_text(text)
            elif kind == "error":
                code = upstream_error_code(getattr(event.error, "code", None))
                if progress is not None:
                    progress.error_code = code
                if code.lower() in {"gateway_timeout", "deadline_exceeded"}:
                    raise TimeoutError
                raise Rejected("stream_provider_error")
            elif kind == "interaction.completed":
                interaction = event.interaction
                # Lifecycle events omit steps; assemble only final-text deltas locally.
                return SimpleNamespace(
                    id=getattr(interaction, "id", None),
                    status=interaction.status,
                    usage=interaction.usage,
                    usage_metadata=getattr(interaction, "usage_metadata", None) or usage_metadata,
                    service_tier=getattr(interaction, "service_tier", None) or service_tier,
                    steps=[
                        SimpleNamespace(
                            type="model_output",
                            content=[SimpleNamespace(type="text", text="".join(parts))],
                        )
                        for _, (typ, parts) in sorted(steps.items())
                        if typ == "model_output"
                    ],
                )
        raise Rejected("stream_missing_completion")
    finally:
        close = getattr(stream, "aclose", None) or getattr(stream, "close", None)
        if close:
            value = close()
            if inspect.isawaitable(value):
                await value


async def cancellable(operation, event):
    if event is None:
        return await operation()
    if event.is_set():
        raise asyncio.CancelledError
    work = asyncio.create_task(operation())
    cancelled = asyncio.create_task(event.wait())
    try:
        done, _ = await asyncio.wait({work, cancelled}, return_when=asyncio.FIRST_COMPLETED)
        if cancelled in done:
            raise asyncio.CancelledError
        return await work
    finally:
        work.cancel()
        cancelled.cancel()
        await asyncio.gather(work, cancelled, return_exceptions=True)
