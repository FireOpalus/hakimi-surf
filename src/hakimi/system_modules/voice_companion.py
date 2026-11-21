"""Resident system module for real-time voice companion hub."""

from __future__ import annotations

from ..thinking_policy import ThinkingScene
from ..voice_companion import VoiceCompanion
from . import SystemModule


class Module(SystemModule):
    module_id = "voice_companion"
    display_name = "语音伴随"
    description = (
        "为桌面端与第三方语音软件提供统一的实时语音对话、流式口播分句与记忆沉淀中枢；"
        "Owner 连麦时自动联动屏幕伴看。"
    )
    requires = ("event_ledger", "owner_notebook", "screen_watch", "voice_gateway")
    admin_panel = "voice-companion"
    thinking_scenes = (
        ThinkingScene(
            "voice.companion",
            "实时语音伴随",
            default="low",
            levels=("low", "medium"),
            output_tokens=512,
        ),
    )

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.companion = VoiceCompanion(resources.settings, resources.store, resources.service)

    async def initialize(self):
        await self.companion.initialize()

    async def status(self):
        info = self.companion.status_summary()
        if info["online"]:
            detail = f"已连接 · {info['adapter_id']}"
        elif not info["ws_configured"]:
            detail = "待机 · 未启用伴随 WS"
        else:
            detail = "等待伴随端连接"
        return {"state": "ready", "detail": detail}
