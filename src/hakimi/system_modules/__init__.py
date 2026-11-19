"""Versioned, always-loaded internal services. Only code shipped in this package is discovered."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from graphlib import TopologicalSorter
from importlib import import_module
from pkgutil import iter_modules
from typing import TYPE_CHECKING, Any

from ..thinking_policy import ThinkingScene

if TYPE_CHECKING:
    import httpx

    from ..config import Settings
    from ..drive_client import FolderDrive
    from ..gemini import GeminiGateway
    from ..modules import ModuleRegistry
    from ..roaming_logger import PatrolDiary
    from ..store import Store
    from ..world_state import MilestoneEvent


@dataclass(frozen=True)
class SystemResources:
    settings: Settings
    store: Store
    gateway: GeminiGateway
    http: httpx.AsyncClient | None
    diary: PatrolDiary | None = None
    drive: FolderDrive | None = None


class SystemModule:
    module_id: str
    display_name: str
    description: str
    requires: tuple[str, ...] = ()
    thinking_scenes: tuple[ThinkingScene, ...] = ()
    admin_panel: str | None = None

    def __init__(self, resources: SystemResources, registry: ModuleRegistry):
        self.resources = resources

    async def initialize(self) -> None:
        """Prepare local storage before any background worker starts."""

    def workers(self) -> tuple[Callable[[], Coroutine[Any, Any, None]], ...]:
        return ()

    def public_event_observers(
        self,
    ) -> dict[str, Callable[[MilestoneEvent, float], Coroutine[Any, Any, bool]]]:
        return {}

    async def status(self) -> dict:
        return {"state": "ready", "detail": "就绪"}


def install_system_modules(registry: ModuleRegistry, resources: SystemResources) -> None:
    entries = {}
    for entry in iter_modules(__path__):
        if entry.name.startswith("_"):
            continue
        cls = import_module(f".{entry.name}", __name__).Module
        if not issubclass(cls, SystemModule) or cls.module_id != entry.name:
            raise ValueError(f"Invalid system module: {entry.name}")
        entries[entry.name] = cls
    dependencies = {name: cls.requires for name, cls in entries.items()}
    for name in TopologicalSorter(dependencies).static_order():
        if name not in entries:
            raise ValueError(f"Missing system module dependency: {name}")
        registry.register_system(entries[name](resources, registry))
