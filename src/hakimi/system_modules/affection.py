"""Resident system module for encrypted per-user affection scoring and tier gating."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..affection import AffectionManager
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources


class Module(SystemModule):
    module_id = "affection"
    display_name = "好感度与分层权限"
    description = (
        "管理跨平台加密用户好感度、七档能力分级门控、违规扣分与拉黑冷却恢复，"
        "并正向绑定统一身份实现跨端人格继承。"
    )
    requires = ("identity",)
    admin_panel = "community"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        runtime = getattr(resources.store, "runtime", None)
        existing = getattr(runtime, "affection", None) if runtime is not None else None
        self.manager = (
            existing
            if isinstance(existing, AffectionManager)
            else AffectionManager(resources.store)
        )
        if runtime is not None:
            runtime.affection = self.manager
        identity_mod = registry.systems.get("identity") if registry is not None else None
        identity_svc = getattr(identity_mod, "identity", None)
        if identity_svc is not None:
            self.manager.bind_identity(identity_svc)

    async def initialize(self) -> None:
        await self.manager.initialize()

    async def status(self) -> dict[str, Any]:
        info = self.manager.status_summary()
        if not info["initialized"]:
            state = "starting"
            detail = "等待初始化" if info["enabled"] else "已关闭 · 等待初始化"
        elif not info["enabled"]:
            state = "disabled"
            detail = f"已关闭 · 已建档 {info['tracked_count']} 人"
        else:
            state = "ready"
            detail = f"已建档 {info['tracked_count']} 人"
            if info["blacklisted_count"]:
                detail += f" · 已拉黑 {info['blacklisted_count']} 人"
            if info["identity_bound"]:
                detail += " · 身份已绑定"
        return {
            "state": state,
            "detail": detail,
            **info,
        }
