"""Lightweight Python-Cordis lifecycle kernel for fibers, services, and reactive coeffects."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal

log = logging.getLogger("hakimi")

FiberState = Literal["pending", "active", "disposing", "disposed"]
Disposer = Callable[[], Any | Awaitable[Any]]
ReadyCallback = Callable[[Any], Any]
LostCallback = Callable[[], Any]


async def _run_awaitable(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


def _close_if_coroutine(obj: Any) -> None:
    if inspect.iscoroutine(obj):
        obj.close()


@dataclass(slots=True)
class _ServiceListener:
    owner_id: str
    fiber: PluginFiber
    on_ready: ReadyCallback | None = None
    on_lost: LostCallback | None = None


class PluginFiber:
    """Isolated lifecycle scope tracking async tasks and LIFO cleanup effects."""

    def __init__(
        self,
        owner_id: str,
        *,
        state: FiberState = "active",
        created_at: float | None = None,
        on_disposed: Callable[[PluginFiber], None] | None = None,
    ) -> None:
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id must be a non-empty string")
        self.owner_id: str = owner_id
        self.state: FiberState = state
        self.created_at: float = time.time() if created_at is None else created_at
        self.disposed_at: float | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._disposers: list[Disposer] = []
        self._on_disposed = on_disposed
        self._dispose_done = asyncio.Event()
        self._disposing_task: asyncio.Task[Any] | None = None

    def effect(self, cleanup: Disposer) -> None:
        """Register a sync or async disposer to run in strict LIFO order on dispose()."""
        if self.state in ("disposing", "disposed"):
            raise RuntimeError(f"cannot register effect on {self.state} fiber '{self.owner_id}'")
        if not callable(cleanup):
            raise TypeError("cleanup must be callable")
        self._disposers.append(cleanup)

    def spawn(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Create a tracked asyncio.Task auto-discarded on finish and cancelled on dispose()."""
        if not inspect.iscoroutine(coro):
            raise TypeError("spawn requires a coroutine object")
        if self.state in ("disposing", "disposed"):
            coro.close()
            raise RuntimeError(f"cannot spawn task on {self.state} fiber '{self.owner_id}'")
        try:
            task = asyncio.create_task(coro, name=name)
        except Exception:
            coro.close()
            raise
        self._tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        try:
            self._tasks.remove(task)
        except ValueError:
            pass
        if self.state != "disposing" and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                log.warning(
                    "event=fiber_task_failed owner=%s task=%s error=%s",
                    self.owner_id,
                    task.get_name(),
                    type(exc).__name__,
                    exc_info=exc,
                )

    async def dispose(self) -> None:
        """Idempotently cancel tracked tasks (LIFO) then run registered disposers (LIFO)."""
        if self.state == "disposed":
            return
        if self.state == "disposing":
            if self._disposing_task is asyncio.current_task():
                return
            await self._dispose_done.wait()
            return

        self.state = "disposing"
        self._disposing_task = asyncio.current_task()
        try:
            current = asyncio.current_task()
            while self._tasks:
                task = self._tasks.pop()
                if task is current:
                    continue
                if not task.done():
                    task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    log.warning(
                        "event=fiber_task_dispose_error owner=%s task=%s error=%s",
                        self.owner_id,
                        task.get_name(),
                        type(exc).__name__,
                        exc_info=exc,
                    )

            while self._disposers:
                cleanup = self._disposers.pop()
                try:
                    result = cleanup()
                    if inspect.isawaitable(result):
                        await result
                except Exception as exc:
                    log.warning(
                        "event=fiber_disposer_failed owner=%s error=%s",
                        self.owner_id,
                        type(exc).__name__,
                        exc_info=exc,
                    )
        finally:
            self.state = "disposed"
            self.disposed_at = time.time()
            self._disposing_task = None
            if self._on_disposed is not None:
                try:
                    self._on_disposed(self)
                except Exception as exc:
                    log.warning(
                        "event=fiber_on_disposed_error owner=%s error=%s",
                        self.owner_id,
                        type(exc).__name__,
                        exc_info=exc,
                    )
            self._dispose_done.set()

    def __repr__(self) -> str:
        return (
            f"PluginFiber(owner_id={self.owner_id!r}, state={self.state!r}, "
            f"tasks={len(self._tasks)}, effects={len(self._disposers)})"
        )


class PluginContext:
    """Plugin-scoped capability handle wrapping a PluginFiber and its PluginKernel."""

    def __init__(self, fiber: PluginFiber, kernel: PluginKernel) -> None:
        self.fiber = fiber
        self.kernel = kernel

    @property
    def owner_id(self) -> str:
        return self.fiber.owner_id

    def provide(self, name: str, service: Any) -> None:
        """Register a non-core service owned by this fiber and unprovide it on dispose."""
        if self.fiber.state in ("disposing", "disposed"):
            raise RuntimeError(
                f"cannot provide service '{name}' on {self.fiber.state} fiber '{self.owner_id}'"
            )
        self.kernel._provide_plugin(name, service, self.fiber)

    def get(self, name: str, default: Any = None) -> Any:
        """Return registered service by name, or default if absent."""
        return self.kernel.get(name, default)

    def has(self, name: str) -> bool:
        """Return True if a service with name is currently registered."""
        return self.kernel.has(name)

    def on_service(
        self,
        name: str,
        on_ready: ReadyCallback | None = None,
        on_lost: LostCallback | None = None,
    ) -> None:
        """Subscribe to reactive service readiness and loss for the lifetime of this fiber."""
        if self.fiber.state in ("disposing", "disposed"):
            raise RuntimeError(
                f"cannot subscribe to '{name}' on {self.fiber.state} fiber '{self.owner_id}'"
            )
        if not isinstance(name, str) or not name.strip():
            raise ValueError("service name must be a non-empty string")
        listener = _ServiceListener(
            owner_id=self.fiber.owner_id,
            fiber=self.fiber,
            on_ready=on_ready,
            on_lost=on_lost,
        )
        self.kernel._add_listener(name, listener)
        self.fiber.effect(lambda: self.kernel._remove_listener(name, listener))
        if on_ready is not None and self.kernel.has(name):
            self.kernel._notify_ready_one(name, self.kernel.get(name), listener)

    def effect(self, cleanup: Disposer) -> None:
        """Delegate disposer registration to the underlying fiber."""
        self.fiber.effect(cleanup)

    def spawn(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Delegate task creation to the underlying fiber."""
        return self.fiber.spawn(coro, name=name)


class PluginKernel:
    """Cordis-style service registry and hierarchical fiber lifecycle coordinator."""

    def __init__(self) -> None:
        self._services: dict[str, Any] = {}
        self._core_services: set[str] = set()
        self._service_owners: dict[str, str] = {}
        self._fibers: dict[str, PluginFiber] = {}
        self._listeners: dict[str, list[_ServiceListener]] = {}
        self._disposed_count: int = 0

    def provide_core(self, name: str, service: Any) -> None:
        """Register a protected core service that plugins cannot overwrite or unprovide."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("service name must be a non-empty string")
        if name in self._service_owners:
            owner = self._service_owners[name]
            raise ValueError(f"service '{name}' is already provided by plugin '{owner}'")
        self._core_services.add(name)
        self._services[name] = service
        self._notify_ready(name, service)

    def fork(self, owner_id: str) -> PluginContext:
        """Create an active PluginFiber for owner_id and return its PluginContext."""
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id must be a non-empty string")
        existing = self._fibers.get(owner_id)
        if existing is not None and existing.state != "disposed":
            raise ValueError(f"active fiber already exists for owner '{owner_id}'")
        fiber = PluginFiber(
            owner_id,
            state="active",
            on_disposed=self._on_fiber_disposed,
        )
        self._fibers[owner_id] = fiber
        return PluginContext(fiber, self)

    def get(self, name: str, default: Any = None) -> Any:
        """Return a registered core or plugin service, or default if missing."""
        return self._services.get(name, default)

    def has(self, name: str) -> bool:
        """Return True if a service with name is currently registered."""
        return name in self._services

    def fibers(self) -> dict[str, PluginFiber]:
        """Return a snapshot mapping active/disposing owner_ids to their PluginFiber."""
        return dict(self._fibers)

    async def dispose_fiber(self, owner_id: str) -> bool:
        """Dispose the fiber for owner_id if present and not already disposed."""
        fiber = self._fibers.get(owner_id)
        if fiber is None or fiber.state == "disposed":
            return False
        was_live = fiber.state in ("pending", "active")
        await fiber.dispose()
        return was_live

    async def dispose_all(self) -> None:
        """Dispose all non-core fibers in strict reverse creation order (LIFO)."""
        for fiber in reversed(list(self._fibers.values())):
            await fiber.dispose()

    def status_summary(self) -> dict[str, Any]:
        """Return a display-safe JSON-serializable summary of kernel state."""
        return {
            "core_services": sorted(self._core_services),
            "services": sorted(self._services.keys()),
            "service_owners": dict(self._service_owners),
            "fiber_count": len(self._fibers),
            "disposed_count": self._disposed_count,
            "fibers": {
                owner_id: {
                    "owner_id": fiber.owner_id,
                    "state": fiber.state,
                    "created_at": fiber.created_at,
                    "disposed_at": fiber.disposed_at,
                    "pending_tasks": len(fiber._tasks),
                    "effect_count": len(fiber._disposers),
                }
                for owner_id, fiber in self._fibers.items()
            },
        }

    def _on_fiber_disposed(self, fiber: PluginFiber) -> None:
        if self._fibers.get(fiber.owner_id) is fiber:
            self._fibers.pop(fiber.owner_id, None)
        self._disposed_count += 1

    def _provide_plugin(self, name: str, service: Any, fiber: PluginFiber) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("service name must be a non-empty string")
        if name in self._core_services:
            raise ValueError(f"cannot overwrite protected core service '{name}'")
        current_owner = self._service_owners.get(name)
        if current_owner is not None and current_owner != fiber.owner_id:
            raise ValueError(f"service '{name}' is already provided by '{current_owner}'")
        first_provide = current_owner is None
        self._services[name] = service
        self._service_owners[name] = fiber.owner_id
        if first_provide:
            fiber.effect(lambda: self._unprovide_plugin(name, fiber.owner_id))
        self._notify_ready(name, service)

    async def _unprovide_plugin(self, name: str, owner_id: str) -> None:
        if name in self._core_services:
            return
        if self._service_owners.get(name) != owner_id:
            return
        self._service_owners.pop(name, None)
        self._services.pop(name, None)
        await self._notify_lost(name)

    def _add_listener(self, name: str, listener: _ServiceListener) -> None:
        self._listeners.setdefault(name, []).append(listener)

    def _remove_listener(self, name: str, listener: _ServiceListener) -> None:
        listeners = self._listeners.get(name)
        if not listeners:
            return
        try:
            listeners.remove(listener)
        except ValueError:
            pass
        if not listeners:
            self._listeners.pop(name, None)

    def _notify_ready(self, name: str, service: Any) -> None:
        for listener in list(self._listeners.get(name, ())):
            if listener not in self._listeners.get(name, ()):
                continue
            self._notify_ready_one(name, service, listener)

    def _notify_ready_one(
        self,
        name: str,
        service: Any,
        listener: _ServiceListener,
    ) -> None:
        if listener.on_ready is None or listener.fiber.state in ("disposing", "disposed"):
            return
        try:
            result = listener.on_ready(service)
        except Exception as exc:
            log.warning(
                "event=service_on_ready_failed service=%s listener=%s error=%s",
                name,
                listener.owner_id,
                type(exc).__name__,
                exc_info=exc,
            )
            return
        if inspect.isawaitable(result):
            coro = result if inspect.iscoroutine(result) else _run_awaitable(result)
            try:
                listener.fiber.spawn(coro, name=f"on_ready:{name}")
            except Exception as exc:
                _close_if_coroutine(coro)
                log.warning(
                    "event=service_on_ready_spawn_failed service=%s listener=%s error=%s",
                    name,
                    listener.owner_id,
                    type(exc).__name__,
                    exc_info=exc,
                )

    async def _notify_lost(self, name: str) -> None:
        for listener in list(self._listeners.get(name, ())):
            if listener not in self._listeners.get(name, ()):
                continue
            if listener.on_lost is None or listener.fiber.state in ("disposing", "disposed"):
                continue
            try:
                result = listener.on_lost()
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                log.warning(
                    "event=service_on_lost_failed service=%s listener=%s error=%s",
                    name,
                    listener.owner_id,
                    type(exc).__name__,
                    exc_info=exc,
                )
