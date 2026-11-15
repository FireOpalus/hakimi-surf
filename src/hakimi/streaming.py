"""Bounded SSE assembly. Never publish partial JSON, thoughts, or unverified tool output."""

import asyncio
import inspect
from types import SimpleNamespace

from .security import Rejected


class StreamProgress:
    """Tracks stream execution state and assembled output for interrupted billing fallback."""

    def __init__(self) -> None:
        self.dispatched = False
        self.started = False
        self.text_parts: list[str] = []
        self.thought_steps = 0

    @property
    def output_text(self) -> str:
        return "".join(self.text_parts)


async def consume_stream(stream, progress: StreamProgress | None = None):
    steps = {}
    length = 0
    try:
        async for event in stream:
            if progress is not None:
                progress.started = True
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
                        progress.text_parts.extend(pieces)
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
                        progress.text_parts.append(text)
            elif kind == "error":
                if getattr(event.error, "code", "") in {"gateway_timeout", "deadline_exceeded"}:
                    raise TimeoutError
                raise Rejected("stream_provider_error")
            elif kind == "interaction.completed":
                interaction = event.interaction
                # Lifecycle events omit steps; assemble only final-text deltas locally.
                return SimpleNamespace(
                    id=getattr(interaction, "id", None),
                    status=interaction.status,
                    usage=interaction.usage,
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
