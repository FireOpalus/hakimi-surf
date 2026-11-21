"""Resident system module for the external voice synthesis gateway."""

from __future__ import annotations

from ..voice_gateway_admin import VoiceGatewayAdminManager
from ..voice_reply import VoiceReplyClient
from . import SystemModule


class Module(SystemModule):
    module_id = "voice_gateway"
    display_name = "语音合成网关"
    description = "为跨平台语音回复与语音伴随提供统一的 TTS 网关客户端及加密配置热同步。"
    admin_panel = "voice-companion"

    def __init__(self, resources, registry) -> None:
        super().__init__(resources, registry)
        self.reply = VoiceReplyClient(resources.settings, http=resources.http)
        self.admin = VoiceGatewayAdminManager(
            resources.settings, resources.store, http=resources.http
        )

    async def initialize(self) -> None:
        await self.admin.initialize()
        if self.admin.configured():
            try:
                await self.admin.sync_to_gateway()
            except Exception:
                pass

    async def status(self) -> dict:
        admin_ok = self.admin.configured()
        reply_ok = self.reply.configured()
        if not admin_ok:
            return {"state": "unconfigured", "detail": "未配置语音网关地址或凭据"}
        has_override = getattr(self.admin, "_override", None) is not None
        source = "已持久化覆写" if has_override else "环境默认"
        reply_note = "语音回复已开启" if reply_ok else "语音回复未开启（仅伴随可用）"
        return {"state": "ready", "detail": f"{source} · {reply_note}"}
