"""Resident system module for Cover Songbook, Drive primary storage, and Colab RVC learning."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ..security import Rejected
from ..song_drive import SongDriveStore, compute_weight_cache_key
from ..songs import (
    RVC_COVER_ALLOWED_GPUS,
    RVC_COVER_DEFAULT_GPU,
    RVC_COVER_FALLBACK_GPUS,
    RVC_COVER_TIMEOUT_SECONDS,
    RVC_TRAIN_ALLOWED_GPUS,
    RVC_TRAIN_DEFAULT_GPU,
    RVC_TRAIN_FALLBACK_GPUS,
    RVC_TRAIN_TIMEOUT_SECONDS,
    RvcCoverJobPayload,
    RvcTrainJobPayload,
    SongLibrary,
    validate_rvc_cover_payload,
    validate_rvc_train_payload,
)
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources

log = logging.getLogger("hakimi")

_SUCCESS_STATUSES = frozenset({"succeeded", "completed", "ok", "done"})


def _filter_supported_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return kwargs
    params = sig.parameters
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return kwargs
    return {
        key: value
        for key, value in kwargs.items()
        if key in params
        and params[key].kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
    }


__all__ = [
    "Module",
    "RvcCoverJobPayload",
    "RvcTrainJobPayload",
    "validate_rvc_cover_payload",
    "validate_rvc_train_payload",
]


class Module(SystemModule):
    module_id = "song_studio"
    display_name = "歌单与翻唱工坊"
    description = (
        "管理猫猫翻唱歌单、Google Drive Hakimi_Songs/ 音频主存与原曲收件箱、本地有界 LRU 热缓存，"
        "并动态向 Colab 算力工坊注册翻唱/训练作业完成异步学歌入库。"
    )
    requires = ("colab_compute", "drive_sync")
    admin_panel = "songs"
    drive_usage = "猫猫翻唱歌单与音频缓存"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        colab_module = registry.systems.get("colab_compute")
        self.compute = getattr(colab_module, "compute", None)
        pb_module = registry.systems.get("plugin_bus")
        plugin_bus = getattr(pb_module, "plugin_bus", None) or getattr(
            resources.service, "plugin_bus", None
        )
        self.drive = SongDriveStore(resources.settings, resources.store)
        self.songs = SongLibrary(
            resources.store,
            compute=self.compute,
            drive=self.drive,
            settings=resources.settings,
            plugin_bus=plugin_bus,
        )
        resources.store.runtime.songs = self.songs
        self._hook_registered = False

    async def initialize(self) -> None:
        if self.songs.plugin_bus is None:
            svc = getattr(self.resources, "service", None)
            pb = getattr(svc, "plugin_bus", None) if svc is not None else None
            if pb is not None:
                self.songs.plugin_bus = pb
        await self.drive.initialize()
        await self.songs.initialize()
        if not self._hook_registered and self.compute is not None:
            register_fn = getattr(self.compute, "register_job_type", None)
            if callable(register_fn):
                register_fn(
                    "rvc_cover",
                    **_filter_supported_kwargs(
                        register_fn,
                        {
                            "validator": validate_rvc_cover_payload,
                            "pre_dispatch": self._prepare_rvc_cover_dispatch,
                            "on_complete": self._on_rvc_cover_completed,
                            "owner_module": self.module_id,
                            "default_gpu": RVC_COVER_DEFAULT_GPU,
                            "fallback_gpus": RVC_COVER_FALLBACK_GPUS,
                            "allowed_gpus": RVC_COVER_ALLOWED_GPUS,
                            "timeout_seconds": RVC_COVER_TIMEOUT_SECONDS,
                        },
                    ),
                )
                register_fn(
                    "rvc_train",
                    **_filter_supported_kwargs(
                        register_fn,
                        {
                            "validator": validate_rvc_train_payload,
                            "pre_dispatch": self._prepare_rvc_train_dispatch,
                            "owner_module": self.module_id,
                            "default_gpu": RVC_TRAIN_DEFAULT_GPU,
                            "fallback_gpus": RVC_TRAIN_FALLBACK_GPUS,
                            "allowed_gpus": RVC_TRAIN_ALLOWED_GPUS,
                            "timeout_seconds": RVC_TRAIN_TIMEOUT_SECONDS,
                        },
                    ),
                )
                self._hook_registered = True
            elif hasattr(self.compute, "register_completion_hook"):
                self.compute.register_completion_hook("rvc_cover", self._on_rvc_cover_completed)
                self._hook_registered = True

    def _active_drive(self) -> SongDriveStore | Any:
        drive = getattr(self.songs, "drive", None) or self.drive
        if drive is None or not drive.configured():
            raise Rejected("song_drive_not_configured")
        return drive

    async def _resolve_weight_cache(self, drive: Any, mode: str) -> tuple[str, str, str]:
        cache_folder_id = ""
        cache_weights_file_id = ""
        cache_weights_sha256 = ""
        try:
            cache_folder_id = await drive.ensure_cache_folder()
            cache_key = compute_weight_cache_key(mode)
            manifest = await drive.resolve_cache_manifest(cache_key, expected_kind="weights")
            if isinstance(manifest, dict):
                cache_weights_file_id = str(manifest.get("archive_file_id") or "")
                cache_weights_sha256 = str(manifest.get("sha256") or "")
        except asyncio.CancelledError:
            raise
        except Exception:
            return "", "", ""
        return cache_folder_id, cache_weights_file_id, cache_weights_sha256

    async def _prepare_rvc_cover_dispatch(self, job: dict[str, Any]) -> dict[str, Any]:
        """Build transient, in-memory Drive dispatch context for an `rvc_cover` job."""
        drive = self._active_drive()
        payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}

        tree = await drive.ensure_folder_tree()
        token_info = await drive.get_short_lived_token(min_ttl_seconds=1800)
        access_token = str(token_info.get("access_token") or "").strip()
        if not access_token:
            raise Rejected("song_drive_token_failed")

        req_inbox_id = str(payload.get("inbox_file_id") or "").strip()
        req_inbox_name = str(payload.get("inbox_filename") or "").strip()
        inbox_file_id = ""
        inbox_name = ""
        if req_inbox_id or req_inbox_name:
            inbox_meta = await drive.resolve_inbox_file(
                file_id=req_inbox_id,
                filename=req_inbox_name,
            )
            inbox_file_id = str(inbox_meta.get("id") or "")
            inbox_name = str(inbox_meta.get("name") or req_inbox_name)

        model_name = str(payload.get("model_name") or "hakimi").strip() or "hakimi"
        pth_file_id = ""
        index_file_id = ""
        remote_models = await drive.resolve_model_files(model_name)
        if isinstance(remote_models, dict):
            pth_meta = remote_models.get("pth")
            idx_meta = remote_models.get("index")
            if isinstance(pth_meta, dict) and isinstance(idx_meta, dict):
                pth_file_id = str(pth_meta.get("id") or "")
                index_file_id = str(idx_meta.get("id") or "")

        (
            cache_folder_id,
            cache_weights_file_id,
            cache_weights_sha256,
        ) = await self._resolve_weight_cache(drive, "cover")

        return {
            "drive": {
                "access_token": access_token,
                "folders": {
                    "root": tree.root,
                    "models": tree.models,
                    "songs": tree.songs,
                    "inbox": tree.inbox,
                    "cache": cache_folder_id,
                },
                "files": {
                    "inbox_file_id": inbox_file_id,
                    "inbox_name": inbox_name,
                    "pth_file_id": pth_file_id,
                    "index_file_id": index_file_id,
                    "cache_weights_file_id": cache_weights_file_id,
                    "cache_weights_sha256": cache_weights_sha256,
                },
            }
        }

    async def _prepare_rvc_train_dispatch(self, job: dict[str, Any]) -> dict[str, Any]:
        """Build transient, in-memory Drive dispatch context for an `rvc_train` job."""
        _ = job
        drive = self._active_drive()
        tree = await drive.ensure_folder_tree()
        token_info = await drive.get_short_lived_token(min_ttl_seconds=1800)
        access_token = str(token_info.get("access_token") or "").strip()
        if not access_token:
            raise Rejected("song_drive_token_failed")

        (
            cache_folder_id,
            cache_weights_file_id,
            cache_weights_sha256,
        ) = await self._resolve_weight_cache(drive, "all")

        return {
            "drive": {
                "access_token": access_token,
                "folders": {
                    "root": tree.root,
                    "models": tree.models,
                    "songs": tree.songs,
                    "inbox": tree.inbox,
                    "cache": cache_folder_id,
                },
                "files": {
                    "inbox_file_id": "",
                    "inbox_name": "",
                    "pth_file_id": "",
                    "index_file_id": "",
                    "cache_weights_file_id": cache_weights_file_id,
                    "cache_weights_sha256": cache_weights_sha256,
                },
            }
        }

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
        finder_tag = " · 独立找歌在线" if stats.get("song_finder_online") else ""
        detail = (
            f"曲目 {total} 首 · Drive 备份 {stats['drive_backed_count']} 首 · "
            f"热缓存 {stats['cached_count']} 首 ({cache_mb:.1f}/{limit_mb:.0f} MiB){finder_tag}"
        )
        return {
            "state": "ready" if self.songs.initialized else "starting",
            "detail": detail,
            "songs_count": total,
            **stats,
        }
