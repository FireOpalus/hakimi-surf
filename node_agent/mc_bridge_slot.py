"""Phase 4 Mineflayer bridge slot stub for the CN node agent."""

from __future__ import annotations

import time
from typing import Any


class McBridgeSlot:
    """Stub capability slot reserved for Phase 4 Mineflayer integration."""

    def __init__(self) -> None:
        self._connected: bool = False
        self._paused: bool = False
        self._activity: str = ""
        self._updated_at: float = 0.0

    async def status(self) -> dict[str, Any]:
        """Return current bridge readiness and pause state."""
        return {
            "connected": self._connected,
            "paused": self._paused,
            "bridge_ready": False,
            "activity": self._activity,
            "phase": "phase4_stub",
            "updated_at": self._updated_at,
        }

    async def dispatch_goal(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Accept a future Phase 4 goal payload and report stub readiness."""
        _ = params if isinstance(params, dict) else {}
        return {
            "ok": False,
            "accepted": False,
            "bridge_ready": False,
            "paused": self._paused,
            "reason": "mc_paused" if self._paused else "mc_bridge_not_configured",
        }

    async def pause(self) -> dict[str, Any]:
        """Mark the Mineflayer bridge slot paused."""
        self._paused = True
        self._updated_at = time.time()
        return {
            "ok": True,
            "connected": self._connected,
            "paused": self._paused,
            "bridge_ready": False,
        }

    async def resume(self) -> dict[str, Any]:
        """Mark the Mineflayer bridge slot resumed."""
        self._paused = False
        self._updated_at = time.time()
        return {
            "ok": True,
            "connected": self._connected,
            "paused": self._paused,
            "bridge_ready": False,
        }
