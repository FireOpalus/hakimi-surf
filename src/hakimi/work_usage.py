"""Identify the unit of work responsible for a ledger reservation, across async calls."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelWork:
    kind: str
    key: str
    root_kind: str
    root_key: str


_current: ContextVar[ModelWork | None] = ContextVar("hakimi_model_work", default=None)


@contextmanager
def model_work(kind: str, key: str | None = None) -> Iterator[ModelWork]:
    """Use only application action names and opaque IDs. Nested calls retain their root."""
    parent = _current.get()
    if parent is not None and key is None:
        yield parent
        return
    key = key or uuid.uuid4().hex
    work = ModelWork(
        kind, key, parent.root_kind if parent else kind, parent.root_key if parent else key
    )
    token = _current.set(work)
    try:
        yield work
    finally:
        _current.reset(token)


def reservation_work(lane: str, reservation: str) -> ModelWork:
    return _current.get() or ModelWork(lane, reservation, lane, reservation)
