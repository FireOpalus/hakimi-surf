"""Resident system module for the local allowlisted meme library."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..memes import MemeLibrary
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources


class Module(SystemModule):
    module_id = "meme_library"
    display_name = "本地表情包库"
    description = (
        "管理本地白名单表情包、多变体图库与公开回复频率节流，预留后续云盘备份与热缓存扩展位。"
    )
    requires = ()
    admin_panel = "memes"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        runtime = getattr(resources.store, "runtime", None)
        existing = getattr(runtime, "memes", None) if runtime is not None else None
        self.library = (
            existing if isinstance(existing, MemeLibrary) else MemeLibrary(resources.store)
        )
        if runtime is not None:
            runtime.memes = self.library

    async def initialize(self) -> None:
        await self.library.initialize()

    async def status(self) -> dict[str, Any]:
        info = self.library.status_summary()
        state = "ready" if info["initialized"] else "starting"
        if not info["initialized"]:
            detail = "等待初始化"
        elif info["total_count"] == 0:
            dir_note = "目录就绪" if info["directory_ready"] else "目录未就绪"
            detail = f"暂无本地表情 · {dir_note}"
        else:
            size_mb = info["total_bytes"] / (1024 * 1024)
            limit_mb = info["max_bytes"] / (1024 * 1024)
            parts = [f"标签 {info['labels_count']} 个", f"素材 {info['total_count']} 张"]
            if info["variants_count"]:
                parts.append(f"变体 {info['variants_count']} 张")
            parts.append(f"{size_mb:.1f}/{limit_mb:.0f} MiB")
            if not info["directory_ready"]:
                parts.append("目录未就绪")
            detail = " · ".join(parts)
        return {
            "state": state,
            "detail": detail,
            **info,
        }
