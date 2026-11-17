"""QQ social capability module (KR node, LOW attention)."""

from __future__ import annotations

from typing import Any

from ..config import Settings
from ..hot_ring import HotRing
from .base import AttentionLevel, BaseModule, GoalResult, NodeRegion


class QQSocialModule(BaseModule):
    """Summarizes active QQ transport and group ring activity without leaking private content."""

    module_id: str = "qq_social"
    node_region: NodeRegion = "KR"
    attention_level: AttentionLevel = "LOW"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        hot_ring: HotRing | None = None,
        transport: Any = None,
        service: Any = None,
    ) -> None:
        super().__init__(settings)
        self._hot_ring = hot_ring
        self._transport = transport
        self._service = service

    def _resolve_connection_label(self) -> str:
        transport = self._transport or getattr(
            getattr(self._service, "sender", None), "onebot", None
        )
        if transport is not None:
            status_fn = getattr(transport, "status", None)
            if callable(status_fn):
                status = status_fn()
                if isinstance(status, dict) and "online" in status:
                    return "在线" if status["online"] else "离线"
            ready = getattr(transport, "ready", None)
            is_set = getattr(ready, "is_set", None)
            if callable(is_set):
                return "在线" if is_set() else "离线"
            return "HTTP就绪"
        settings = self.settings or getattr(self._service, "s", None)
        if settings is not None:
            if getattr(settings, "onebot_enabled", False) or getattr(settings, "qq_enabled", False):
                return "已启用"
            return "未启用"
        return "待机"

    async def _raw_sensor_summary(self) -> str:
        conn_label = self._resolve_connection_label()
        hot_ring = self._hot_ring or getattr(self._service, "hot", None)
        rings = getattr(hot_ring, "rings", {}) if hot_ring is not None else {}
        active_groups = 0
        group_buffered = 0
        for key, ring in rings.items():
            # Strictly aggregate group metadata only; never inspect or expose private owner chats.
            if isinstance(key, tuple) and len(key) >= 3 and key[2] == "group":
                active_groups += 1
                messages = getattr(ring, "messages", None)
                if messages is not None:
                    group_buffered += len(messages)
        parts = [f"QQ:{conn_label}", f"活跃群:{active_groups}", f"群缓冲:{group_buffered}"]
        if self.proactive_paused:
            parts.append("主动暂停")
        return " ".join(parts)

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        action_type = str(ticket.get("action_type", "")).strip()
        if self.proactive_paused and action_type != "status":
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                summary="QQ主动互动已因高专注锁定暂停",
                reason="proactive_paused",
            )
        summary = await self.get_sensor_summary()
        return GoalResult(
            ok=True,
            module_id=self.module_id,
            summary=summary,
            artifacts={"action_type": action_type[:64]},
        )
