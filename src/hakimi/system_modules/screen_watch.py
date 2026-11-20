"""Resident system module for Owner-private standalone screen watching."""

from __future__ import annotations

from ..screen_watch import ScreenWatch
from ..thinking_policy import ThinkingScene
from . import SystemModule


class Module(SystemModule):
    module_id = "screen_watch"
    display_name = "屏幕伴看"
    description = (
        "独立接收 Owner 桌面低频关键帧（仅内存短 TTL 缓存，绝不落盘），"
        "供语音伴随联动并支持独立看屏吐槽。"
    )
    requires = ("event_ledger", "owner_notebook")
    admin_panel = "screen-watch"
    thinking_scenes = (
        ThinkingScene(
            "screen.watch",
            "看屏主动吐槽",
            default="low",
            levels=("low", "medium"),
            output_tokens=384,
        ),
    )

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.screen_watch = ScreenWatch(resources.settings, resources.store, resources.service)

    async def initialize(self):
        await self.screen_watch.initialize()

    async def status(self):
        info = self.screen_watch.status_summary()
        if not self.screen_watch.config.enabled:
            detail = "已暂停采集（隐私总开关关闭）"
        elif info["online"]:
            if info["paused"]:
                detail = f"已连接 · 已暂停采集 ({info['adapter_id']})"
            else:
                detail = f"已连接 · {info['adapter_id']} · 缓存 {info['buffered_frames']} 帧"
        elif not info["ws_configured"]:
            detail = "待机 · 未启用看屏 WS"
        else:
            detail = "等待看屏端连接"
        return {"state": "ready", "detail": detail}
