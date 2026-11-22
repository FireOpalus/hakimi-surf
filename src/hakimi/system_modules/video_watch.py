"""Resident system module for dual-track QQ/Bilibili video watching and quota status."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..video_watch import LOCAL_FAILURE_SQL, VideoWatch
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources


class Module(SystemModule):
    module_id = "video_watch"
    display_name = "双轨看片引擎"
    description = (
        "协调跨 QQ 与 B 站字幕优先、受限原片下载、境内关键帧拼图兜底与每日配额控制，"
        "并预留 Colab 长视频转写只读挂接位。"
    )
    requires = ()
    admin_panel = "bili-account"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        self._registry = registry
        existing = getattr(resources.service, "video_watch", None)
        self.watch = (
            existing
            if isinstance(existing, VideoWatch)
            else VideoWatch(
                resources.service,
                settings=resources.settings,
                store=resources.store,
            )
        )

    def _target(self) -> VideoWatch:
        current = getattr(self.resources.service, "video_watch", None)
        return current if isinstance(current, VideoWatch) else self.watch

    @property
    def colab_compute(self) -> Any | None:
        """Optional read-only handle to ColabComputeHub for future long-video transcription."""
        systems = getattr(self._registry, "systems", None)
        if not isinstance(systems, dict):
            return None
        colab_mod = systems.get("colab_compute")
        return getattr(colab_mod, "compute", None)

    def colab_available(self) -> bool:
        """Return whether an optional Colab compute backend is registered and configured."""
        compute = self.colab_compute
        if compute is None:
            return False
        configured_fn = getattr(compute, "configured", None)
        if callable(configured_fn):
            try:
                return bool(configured_fn())
            except Exception:
                return False
        return True

    async def initialize(self) -> None:
        await self._target().initialize()

    async def _readonly_snapshot(self, watch: VideoWatch) -> dict[str, Any]:
        """Read-only status fallback before initialize() so status() never mutates storage."""
        used = 0
        pending = 0
        store = getattr(watch, "store", None)
        if store is not None and getattr(store, "db", None) is not None:
            async with store.lock:
                tables = {
                    row[0]
                    for row in await store.all(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name IN ('video_watch_attempts', 'gemini_video_files')"
                    )
                }
                if "video_watch_attempts" in tables:
                    used_row = await store.one(
                        "SELECT COUNT(*) FROM video_watch_attempts "
                        f"WHERE day=? AND {LOCAL_FAILURE_SQL}",
                        (watch.day(),),
                    )
                    if used_row:
                        used = int(used_row[0] or 0)
                if "gemini_video_files" in tables:
                    pending_row = await store.one("SELECT COUNT(*) FROM gemini_video_files")
                    if pending_row:
                        pending = int(pending_row[0] or 0)
        cfg = watch.config
        return {
            "config": cfg.model_dump(),
            "source": "sqlite" if watch.overridden else "env",
            "attempts_today": used,
            "remaining_today": (
                None if cfg.daily_limit == "unlimited" else max(0, cfg.daily_limit - used)
            ),
            "remote_cleanup_pending": pending,
        }

    async def status(self) -> dict[str, Any]:
        watch = self._target()
        store = getattr(watch, "store", None)
        if watch.initialized and store is not None and getattr(store, "db", None) is not None:
            raw = await watch.status()
        else:
            raw = await self._readonly_snapshot(watch)

        cfg = (
            raw.get("config") if isinstance(raw.get("config"), dict) else watch.config.model_dump()
        )
        enabled = bool(cfg.get("enabled", False))
        daily_limit = cfg.get("daily_limit", watch.config.daily_limit)
        max_seconds = cfg.get("max_seconds", watch.config.max_seconds)
        attempts_today = int(raw.get("attempts_today", 0) or 0)
        remaining_today = raw.get("remaining_today")
        remote_cleanup_pending = int(raw.get("remote_cleanup_pending", 0) or 0)
        colab_avail = self.colab_available()

        if not enabled:
            state = "disabled"
            parts = ["原片看片未开启（仅字幕与热评摘要）"]
            if remote_cleanup_pending:
                parts.append(f"待清理远端文件 {remote_cleanup_pending}")
            if colab_avail:
                parts.append("Colab 转写就绪")
            detail = " · ".join(parts)
        else:
            state = "ready"
            quota_text = (
                f"今日已用 {attempts_today} 次（不限）"
                if daily_limit == "unlimited"
                else f"今日 {attempts_today}/{daily_limit}（剩余 {remaining_today}）"
            )
            parts = [f"已开启 · {quota_text}", f"单片上限 {max_seconds}s"]
            if remote_cleanup_pending:
                parts.append(f"待清理远端文件 {remote_cleanup_pending}")
            if colab_avail:
                parts.append("Colab 转写就绪")
            detail = " · ".join(parts)

        result: dict[str, Any] = {
            "state": state,
            "detail": detail,
            "enabled": enabled,
            "initialized": bool(watch.initialized),
            "source": raw.get("source", "sqlite" if watch.overridden else "env"),
            "daily_limit": daily_limit,
            "max_seconds": max_seconds,
            "attempts_today": attempts_today,
            "remaining_today": remaining_today,
            "remote_cleanup_pending": remote_cleanup_pending,
            "busy": bool(watch.video_lock.locked()),
            "colab_registered": self.colab_compute is not None,
            "colab_available": colab_avail,
        }
        if remote_cleanup_pending >= 3:
            result["last_error"] = "cleanup_pending"
        return result
