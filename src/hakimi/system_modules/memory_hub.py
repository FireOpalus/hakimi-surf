"""Resident system module providing unified privacy-guarded MemoryHub facade."""

from __future__ import annotations

from ..memory_hub import MemoryHub
from ..owner_memory import OwnerMemoryStore
from . import SystemModule


class Module(SystemModule):
    module_id = "memory_hub"
    display_name = "统一记忆中枢"
    description = (
        "统一收口跨模块体验沉淀（短时 recap / 待办账本 / DualMemory 编年史 / Zone 2 Owner 语义库）"
        "与多端上下文召回，强隔离 Owner 隐私与公开边界。"
    )
    requires = ("event_ledger", "identity", "owner_notebook")
    admin_panel = None

    def __init__(self, resources, registry) -> None:
        super().__init__(resources, registry)
        gw = resources.gateway
        if (
            gw is not None
            and getattr(gw, "_owned", True) is False
            and getattr(gw, "_owned_embeddings", False) is True
        ):
            embedder = None
        else:
            embedder = getattr(gw, "embeddings", None) or getattr(gw, "embedder", None)
        self.owner_memory = OwnerMemoryStore(
            resources.settings,
            vault=resources.store.vault,
            embedder=embedder,
        )
        self.hub = MemoryHub(
            resources.settings,
            resources.service,
            owner_memory=self.owner_memory,
            embedder=embedder,
        )

    async def initialize(self) -> None:
        await self.owner_memory.initialize()

    async def status(self) -> dict:
        counts = self.hub.status_counts()
        detail = f"写入 {counts['writes']} 次 · 召回 {counts['recalls']} 次"
        if getattr(self.owner_memory, "ready", getattr(self.owner_memory, "_initialized", False)):
            try:
                om_stats = await self.owner_memory.stats()
                active = int(om_stats.get("active_entries", 0) or 0)
                detail = f"{detail} · Owner 语义库 {active} 条"
            except Exception:
                pass
        return {
            "state": "ready",
            "detail": detail,
        }
