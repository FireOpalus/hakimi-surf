"""Resident system module managing remote plugin credentials and reverse WS connections."""

from __future__ import annotations

from ..plugin_bus import PluginBus
from . import SystemModule


class Module(SystemModule):
    module_id = "plugin_bus"
    display_name = "远程插件总线"
    description = (
        "为外部插件进程提供分层授权凭据、反向 WebSocket 通道与受隔离的代理模块生命周期管理。"
    )
    requires = ("event_ledger",)
    admin_panel = "plugin-bus"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.plugin_bus = PluginBus(resources.settings, resources.store, resources.service)

    async def initialize(self):
        await self.plugin_bus.initialize()

    async def status(self):
        info = await self.plugin_bus.status_summary()
        online = info["online_count"]
        registered = info["registered_count"]
        detail = f"在线 {online} / 已登记 {registered} 个插件"
        if not info["ws_enabled"]:
            detail = f"待机 · {detail}（未启用插件 WS）"
        return {"state": "ready", "detail": detail}
