"""Resident system module providing unified privacy-guarded MemoryHub facade."""

from __future__ import annotations

from ..memory_hub import MemoryHub
from . import SystemModule


class Module(SystemModule):
    module_id = "memory_hub"
    display_name = "统一记忆中枢"
    description = (
        "统一收口跨模块体验沉淀（短时 recap / 待办账本 / DualMemory 编年史）"
        "与多端上下文召回，强隔离 Owner 隐私与公开边界。"
    )
    requires = ("event_ledger", "identity", "owner_notebook")
    admin_panel = None

    def __init__(self, resources, registry) -> None:
        super().__init__(resources, registry)
        self.hub = MemoryHub(resources.settings, resources.service)

    async def status(self) -> dict:
        counts = self.hub.status_counts()
        return {
            "state": "ready",
            "detail": f"写入 {counts['writes']} 次 · 召回 {counts['recalls']} 次",
        }
