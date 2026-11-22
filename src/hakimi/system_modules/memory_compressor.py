"""Resident system module for background session memory compression and archival."""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ..memory_compressor import MemoryCompressor
from ..session_manager import SessionManager
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources


class Module(SystemModule):
    module_id = "memory_compressor"
    display_name = "记忆归档与压缩"
    description = (
        "托管本地会话历史滚动压缩、双轨长期记忆提炼与夜间冷归档调度，"
        "与统一记忆中枢协同生命周期，严守云端个人记忆只读边界。"
    )
    requires = ("memory_hub",)
    admin_panel = "memory"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        sessions = getattr(resources.service, "sessions", None) or SessionManager(
            resources.settings, resources.store
        )
        self.compressor = MemoryCompressor(
            resources.settings,
            sessions,
            resources.gateway,
            resources.store,
        )
        hub_mod = registry.systems.get("memory_hub") if registry is not None else None
        self.memory_hub = getattr(hub_mod, "hub", None)

    async def initialize(self) -> None:
        return None

    def _active_compressor(self) -> Any:
        target = getattr(self.resources.service, "compressor", None)
        return target if target is not None else self.compressor

    async def run_archive(self) -> None:
        await self._active_compressor().run()

    def workers(self) -> tuple[Callable[[], Coroutine[Any, Any, None]], ...]:
        return (self.run_archive,)

    async def status(self) -> dict[str, Any]:
        target = self._active_compressor()
        if hasattr(target, "status_summary"):
            info = dict(target.status_summary())
        else:
            runtime = getattr(self.resources.store, "runtime", None)
            comp_cfg = getattr(runtime, "compression", None) if runtime is not None else None
            info = {
                "prefix_history_enabled": bool(self.resources.settings.prefix_history_enabled),
                "compression_enabled": bool(getattr(comp_cfg, "enabled", True)),
                "active_tasks": len(getattr(target, "tasks", ())),
                "backoff_slices": len(getattr(target, "slice_backoff", ())),
                "archive_time": str(self.resources.settings.memory_archive_time),
            }
        if not info["compression_enabled"]:
            state = "disabled"
            detail = "已暂停压缩"
        else:
            state = "ready"
            mode = "双轨归档" if info["prefix_history_enabled"] else "滚动摘要"
            parts = [mode, f"每日 {info['archive_time']}"]
            if info["active_tasks"]:
                parts.append(f"在途 {info['active_tasks']} 项")
            if info["backoff_slices"]:
                parts.append(f"退避 {info['backoff_slices']} 片")
            detail = " · ".join(parts)
        return {
            "state": state,
            "detail": detail,
            **info,
        }
