"""Resident system module for unified cross-platform identity and /bind pairing."""

from __future__ import annotations

from ..identity import IdentityService
from . import SystemModule


class Module(SystemModule):
    module_id = "identity"
    display_name = "统一身份与绑定"
    description = (
        "跨平台统一自然人身份与账户绑定、L1 好感度与黑名单继承、"
        "一次性 /bind 绑定码及 Owner 跨端最近动态缓存。"
    )
    requires = ()
    admin_panel = "identity-center"

    def __init__(self, resources, registry) -> None:
        super().__init__(resources, registry)
        self.identity = IdentityService(resources.settings, resources.store, resources.service)
        if resources.store.runtime is not None:
            resources.store.runtime.identity = self.identity

    async def initialize(self) -> None:
        await self.identity.initialize()

    async def status(self) -> dict:
        info = self.identity.status_summary()
        detail = f"已建档 {info['persons_count']} 人 · {info['accounts_count']} 账号"
        if info["muted_persons_count"]:
            detail += f" · 已屏蔽 {info['muted_persons_count']} 人"
        return {"state": "ready", "detail": detail}
