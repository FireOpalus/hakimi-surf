"""Resident system module for Cover Songbook, Drive primary storage, and Colab RVC learning."""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ..songs import SongLibrary
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources

log = logging.getLogger("hakimi")

_SUCCESS_STATUSES = frozenset({"succeeded", "completed", "ok", "done"})


class Module(SystemModule):
    module_id = "song_studio"
    display_name = "歌单与翻唱工坊"
    description = (
        "管理猫猫翻唱歌单、Google Drive 音频主存、本地有界 LRU 热缓存，"
        "并与 Colab 算力工坊联动完成异步学歌入库。"
    )
    requires = ("colab_compute", "drive_sync")
    admin_panel = "songs"
    drive_usage = "猫猫翻唱歌单与音频缓存"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        colab_module = registry.systems.get("colab_compute")
        self.compute = getattr(colab_module, "compute", None)
        self.songs = SongLibrary(
            resources.store,
            compute=self.compute,
            settings=resources.settings,
        )
        resources.store.runtime.songs = self.songs
        self._hook_registered = False

    async def initialize(self) -> None:
        await self.songs.initialize()
        if (
            not self._hook_registered
            and self.compute is not None
            and hasattr(self.compute, "register_completion_hook")
        ):
            self.compute.register_completion_hook("rvc_cover", self._on_rvc_cover_completed)
            self._hook_registered = True

    async def _notify_owner(self, code: str, text: str) -> None:
        svc = getattr(self.resources, "service", None)
        proactive = getattr(svc, "proactive", None) if svc is not None else None
        if proactive is None:
            return
        alert_fn = getattr(proactive, "alert", None)
        if callable(alert_fn):
            try:
                await alert_fn(code, text)
            except Exception:
                log.info("event=song_studio_notify_failed")
            return
        enqueue_fn = getattr(proactive, "enqueue", None)
        if callable(enqueue_fn):
            try:
                await enqueue_fn("patrol", text, key=code)
            except Exception:
                log.info("event=song_studio_notify_failed")

    async def _on_rvc_cover_completed(self, job: dict[str, Any]) -> None:
        if not isinstance(job, dict):
            return
        job_id = str(job.get("id") or job.get("job_id") or "").strip()
        status = str(job.get("status") or "").strip().lower()
        payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
        result = job.get("result") if isinstance(job.get("result"), dict) else {}
        error_code = str(job.get("error_code") or result.get("error_code") or "").strip()
        title_hint = str(result.get("title") or payload.get("title") or "").strip()

        if status in _SUCCESS_STATUSES:
            raw_bytes = result.get("chorus_bytes")
            chorus_bytes = bytes(raw_bytes) if isinstance(raw_bytes, (bytes, bytearray)) else None
            artist = (
                str(result["artist"])
                if result.get("artist") is not None
                else str(payload.get("artist") or "")
            )
            tags = (
                str(result["tags"])
                if result.get("tags") is not None
                else str(payload.get("tags") or "")
            )
            try:
                byte_size = int(result.get("byte_size") or 0)
                duration_ms = int(result.get("duration_ms") or 0)
                entry = await self.songs.register_colab_cover(
                    title=str(result.get("title") or payload.get("title") or ""),
                    artist=artist,
                    tags=tags,
                    digest=str(result.get("digest") or ""),
                    byte_size=byte_size,
                    duration_ms=duration_ms,
                    drive_chorus_id=str(result.get("drive_chorus_id") or ""),
                    drive_full_id=str(result.get("drive_full_id") or ""),
                    chorus_bytes=chorus_bytes,
                    replace=True,
                )
            except Exception:
                log.warning("event=song_cover_register_failed")
                title_part = f"《{title_hint}》" if title_hint else "新歌"
                await self._notify_owner(
                    f"song_cover_failed:{job_id or title_hint or 'unknown'}",
                    f"BOISER，学唱{title_part}成品入库校验失败，请到后台 Colab 面板查看结果喵。",
                )
                return

            artist_note = f"（{entry['artist']}）" if entry.get("artist") else ""
            await self._notify_owner(
                f"song_cover_done:{job_id or entry['id']}",
                f"BOISER，新歌《{entry['title']}》{artist_note}我学会啦！现在随时可以点唱喵~",
            )
            return

        if status == "cancelled":
            return

        title_part = f"《{title_hint}》" if title_hint else "这首歌"
        if error_code == "source_not_found":
            message = (
                f"BOISER，学唱{title_part}时没找到干净原版伴奏/原唱，"
                "可以把音频文件发给我或放进云盘 Hakimi_Songs/inbox/ 再让我学喵~"
            )
        else:
            reason = f"（原因：{error_code}）" if error_code else ""
            message = (
                f"BOISER，学唱{title_part}没有完成{reason}，"
                "请稍后到后台 Colab 面板查看详情或重试喵。"
            )
        await self._notify_owner(
            f"song_cover_failed:{job_id or title_hint or 'unknown'}",
            message,
        )

    def workers(self) -> tuple[Callable[[], Coroutine[Any, Any, None]], ...]:
        return (self.songs.run_cache_evictor,)

    async def status(self) -> dict[str, Any]:
        stats = self.songs.cache_stats()
        total = len(self.songs.titles)
        cache_mb = stats["cache_bytes"] / (1024 * 1024)
        limit_mb = stats["cache_max_bytes"] / (1024 * 1024)
        detail = (
            f"曲目 {total} 首 · Drive 备份 {stats['drive_backed_count']} 首 · "
            f"热缓存 {stats['cached_count']} 首 ({cache_mb:.1f}/{limit_mb:.0f} MiB)"
        )
        return {
            "state": "ready" if self.songs.initialized else "starting",
            "detail": detail,
            "songs_count": total,
            **stats,
        }
