"""Out-of-process host worker for generic Google Colab GPU compute jobs.

Runs strictly outside the `hakimi-bot` container on loopback HTTP, authenticated by
a constant-time Bearer token (`hmac.compare_digest`). Manages only Colab GPU
sessions (`secrets/colab-adc.json`), warm session pooling, CU accounting, and
pluggable job dispatch via `scripts/colab_jobs/`. Never reads, writes, or stores
Google Drive OAuth credential files or refresh tokens. Any Drive I/O needed by a
whitelisted GPU job uses strictly in-memory, ephemeral access tokens scoped to
explicitly authorized folder IDs supplied per-dispatch via `dispatch_context`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hmac
import importlib.util
import ipaddress
import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 12146
MAX_JOB_BODY_BYTES = 64 * 1024
MAX_JOB_HISTORY = 100

WARM_ALLOWED_GPUS: frozenset[str] = frozenset({"L4", "T4"})
WARM_FORBIDDEN_GPUS: frozenset[str] = frozenset({"H100", "A100", "G4"})
DEFAULT_WARM_IDLE_TTL_SECONDS = 360
MIN_WARM_IDLE_TTL_SECONDS = 60
MAX_WARM_IDLE_TTL_SECONDS = 900
DEFAULT_WARM_MIN_CU_BALANCE = 1.0

GpuName = Literal["H100", "A100", "G4", "L4", "T4"]
VALID_JOB_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")

_ALLOWED_WORKER_ERROR_CODES = frozenset(
    {
        "drive_context_required",
        "colab_drive_not_configured",
        "drive_insufficient_scope",
        "drive_file_outside_scope",
        "drive_not_found",
        "drive_file_too_large",
        "drive_token_failed",
    }
)


def _load_sibling_module(name: str, filename: str):
    if name in sys.modules:
        return sys.modules[name]
    mod_path = Path(__file__).resolve().with_name(filename)
    spec = importlib.util.spec_from_file_location(name, mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load sibling module {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_sibling_package(name: str):
    if name in sys.modules:
        return sys.modules[name]
    pkg_dir = Path(__file__).resolve().parent / name
    init_path = pkg_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        name,
        init_path,
        submodule_search_locations=[str(pkg_dir)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load sibling package {pkg_dir}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


colab_rvc_mod = _load_sibling_module("colab_rvc", "colab_rvc.py")
colab_jobs_mod = _load_sibling_package("colab_jobs")

FOLDER_MIME: str = colab_jobs_mod.FOLDER_MIME
VALID_JOB_TYPE_RE: re.Pattern[str] = colab_jobs_mod.VALID_JOB_TYPE_RE
TransportFn = colab_jobs_mod.TransportFn
EphemeralDriveError = colab_jobs_mod.EphemeralDriveError
EphemeralDriveClient = colab_jobs_mod.EphemeralDriveClient
DriveFoldersContext = colab_jobs_mod.DriveFoldersContext
DriveFilesContext = colab_jobs_mod.DriveFilesContext
DriveDispatchContext = colab_jobs_mod.DriveDispatchContext
DispatchContext = colab_jobs_mod.DispatchContext
WarmSessionClaim = colab_jobs_mod.WarmSessionClaim
WorkerJobContext = colab_jobs_mod.WorkerJobContext
JobValidationError = colab_jobs_mod.JobValidationError
JobHandler = colab_jobs_mod.JobHandler
register_job_handler = colab_jobs_mod.register_job_handler
unregister_job_handler = colab_jobs_mod.unregister_job_handler
get_job_handler = colab_jobs_mod.get_job_handler
registered_job_types = colab_jobs_mod.registered_job_types
_scrub_secret = colab_jobs_mod.scrub_secret


class WorkerJobSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=r"^[a-f0-9]{16,64}$")
    job_type: str = Field(pattern=r"^[a-z][a-z0-9_]{1,31}$")
    gpu_candidates: list[GpuName] = Field(
        default_factory=lambda: ["L4", "T4"], min_length=1, max_length=5
    )
    timeout_seconds: int = Field(default=1800, ge=120, le=7200)
    allow_warm_keepalive: bool = True
    payload: dict[str, Any]
    dispatch_context: DispatchContext | None = None


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _clamp_warm_ttl(raw: int) -> int:
    val = int(raw)
    if val <= 0:
        return 0
    return max(MIN_WARM_IDLE_TTL_SECONDS, min(MAX_WARM_IDLE_TTL_SECONDS, val))


def _sanitize_warm_gpus(raw_gpus: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Strictly intersect warm-eligible GPUs with WARM_ALLOWED_GPUS (`L4`, `T4` only)."""
    out: list[str] = []
    for item in raw_gpus:
        g = str(item or "").strip().upper()
        if g in WARM_ALLOWED_GPUS and g not in WARM_FORBIDDEN_GPUS and g not in out:
            out.append(g)
    return tuple(out)


@dataclass
class WarmSessionEntry:
    session_name: str
    gpu: str
    idle_ttl_seconds: int
    created_at: float
    last_used_at: float
    expires_at: float
    reuse_count: int = 0
    setup_ready: bool = True

    def to_dict(self, *, now: float | None = None) -> dict[str, Any]:
        ts = time.time() if now is None else now
        remaining = max(0, int(round(self.expires_at - ts)))
        return {
            "session_name": self.session_name,
            "gpu": self.gpu,
            "idle_ttl_seconds": self.idle_ttl_seconds,
            "remaining_ttl_seconds": remaining,
            "created_at": round(self.created_at, 3),
            "last_used_at": round(self.last_used_at, 3),
            "expires_at": round(self.expires_at, 3),
            "reuse_count": self.reuse_count,
            "setup_ready": self.setup_ready,
        }


class WarmSessionPool:
    """Thread-safe short-term warm session registry strictly limited to L4/T4 and bounded TTL."""

    def __init__(self, *, max_sessions: int = 1) -> None:
        self._max_sessions = max(1, min(2, int(max_sessions)))
        self._lock = threading.Lock()
        self._entries: dict[str, WarmSessionEntry] = {}

    def snapshot(self, *, now: float | None = None) -> list[dict[str, Any]]:
        ts = time.time() if now is None else now
        with self._lock:
            active = [e for e in self._entries.values() if e.expires_at > ts]
        active.sort(key=lambda e: e.expires_at)
        return [e.to_dict(now=ts) for e in active]

    def put(self, entry: WarmSessionEntry) -> list[WarmSessionEntry]:
        """Park a validated warm session; returns any refused/evicted entries to stop."""
        if (
            entry.gpu not in WARM_ALLOWED_GPUS
            or entry.gpu in WARM_FORBIDDEN_GPUS
            or entry.idle_ttl_seconds <= 0
            or entry.idle_ttl_seconds > MAX_WARM_IDLE_TTL_SECONDS
        ):
            return [entry]
        evicted: list[WarmSessionEntry] = []
        with self._lock:
            prev = self._entries.get(entry.gpu)
            if prev is not None and prev.session_name != entry.session_name:
                evicted.append(prev)
            self._entries[entry.gpu] = entry
            while len(self._entries) > self._max_sessions:
                oldest_gpu = min(
                    (g for g in self._entries if g != entry.gpu),
                    key=lambda g: self._entries[g].last_used_at,
                )
                evicted.append(self._entries.pop(oldest_gpu))
        return evicted

    def claim_matching(
        self,
        gpu_candidates: list[str],
        *,
        now: float | None = None,
    ) -> tuple[WarmSessionEntry | None, list[WarmSessionEntry]]:
        """Atomically claim a matching warm session, draining expired or incompatible GPUs."""
        ts = time.time() if now is None else now
        to_stop: list[WarmSessionEntry] = []
        claimed: WarmSessionEntry | None = None
        with self._lock:
            for g in list(self._entries):
                if self._entries[g].expires_at <= ts:
                    to_stop.append(self._entries.pop(g))
            for cand in gpu_candidates:
                if cand in self._entries and cand in WARM_ALLOWED_GPUS:
                    claimed = self._entries.pop(cand)
                    break
            if claimed is None and self._entries:
                # Drain warm sessions on incompatible GPUs before cold-booting another GPU.
                for g in list(self._entries):
                    to_stop.append(self._entries.pop(g))
        return claimed, to_stop

    def pop_expired(self, *, now: float | None = None) -> list[WarmSessionEntry]:
        ts = time.time() if now is None else now
        expired: list[WarmSessionEntry] = []
        with self._lock:
            for g in list(self._entries):
                if self._entries[g].expires_at <= ts:
                    expired.append(self._entries.pop(g))
        return expired

    def pop_all(self) -> list[WarmSessionEntry]:
        with self._lock:
            items = list(self._entries.values())
            self._entries.clear()
        return items

    def remove_by_session_name(self, session_name: str) -> list[WarmSessionEntry]:
        """Remove any parked entry matching `session_name` so stopped handles never linger."""
        target = str(session_name or "").strip()
        if not target:
            return []
        removed: list[WarmSessionEntry] = []
        with self._lock:
            for g in list(self._entries):
                if self._entries[g].session_name == target:
                    removed.append(self._entries.pop(g))
        return removed


@dataclass
class WorkerConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    token: str = ""
    adc_path: Path = field(default_factory=lambda: REPO_ROOT / "secrets" / "colab-adc.json")
    work_root: Path = field(default_factory=lambda: REPO_ROOT / "data" / "colab_worker")
    model_dir: Path = field(default_factory=lambda: REPO_ROOT / "data" / "rvc_models")
    dataset_dir: Path = field(
        default_factory=lambda: REPO_ROOT / "data" / "rvc_datasets" / "hakimi_breezeblue"
    )
    drive_direct_enabled: bool = True
    warm_pool_enabled: bool = True
    warm_idle_ttl_seconds: int = DEFAULT_WARM_IDLE_TTL_SECONDS
    warm_allowed_gpus: tuple[str, ...] = ("L4", "T4")
    warm_max_sessions: int = 1
    warm_min_cu_balance: float = DEFAULT_WARM_MIN_CU_BALANCE
    cache_enabled: bool = True
    cache_venv_enabled: bool = False
    cache_writeback: bool = True

    def __post_init__(self) -> None:
        self.warm_allowed_gpus = _sanitize_warm_gpus(self.warm_allowed_gpus)
        self.warm_idle_ttl_seconds = _clamp_warm_ttl(self.warm_idle_ttl_seconds)
        self.warm_max_sessions = max(1, min(2, int(self.warm_max_sessions)))
        self.warm_min_cu_balance = max(0.0, float(self.warm_min_cu_balance))
        if self.warm_idle_ttl_seconds <= 0 or not self.warm_allowed_gpus:
            self.warm_pool_enabled = False


@dataclass
class WorkerJobRecord:
    """In-memory job status record (never stores ephemeral dispatch_context or tokens)."""

    job_id: str
    job_type: str
    status: Literal["running", "succeeded", "failed", "cancelled"]
    gpu_candidates: list[str]
    session_name: str
    assigned_gpu: str | None = None
    cu_used: float = 0.0
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    cancelled: bool = False
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status,
            "assigned_gpu": self.assigned_gpu,
            "cu_used": self.cu_used,
            "result": self.result,
            "error_code": self.error_code,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


def _is_allowed_bind_host(host: str, *, allow_private: bool = False) -> bool:
    cleaned = host.strip()
    if cleaned in {"127.0.0.1", "::1", "localhost"}:
        return True
    if not allow_private:
        return False
    try:
        ip = ipaddress.ip_address(cleaned)
        return ip.is_loopback or ip.is_private
    except ValueError:
        return False


class WorkerState:
    """Thread-safe / asyncio-safe coordinator for host Colab GPU compute operations."""

    def __init__(
        self,
        config: WorkerConfig,
        *,
        drive_transport: TransportFn | None = None,
        usage_fn: Callable[[], dict[str, Any]] | None = None,
        stop_all_fn: Callable[[list[str] | None], dict[str, Any]] | None = None,
        stop_session_fn: Callable[[str], None] | None = None,
        probe_session_fn: Callable[[str], dict[str, Any]] | None = None,
        cover_runner: Callable[..., dict[str, Any]] | None = None,
        train_runner: Callable[..., dict[str, Any]] | None = None,
        dataset_builder: Callable[..., dict[str, Any]] | None = None,
        job_overrides: dict[str, Any] | None = None,
        handlers: Sequence[JobHandler] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cfg = config
        self._drive_transport = drive_transport
        self._usage_fn = usage_fn or colab_rvc_mod.parse_colab_usage
        self._stop_all_fn = stop_all_fn or colab_rvc_mod.stop_all_sessions
        self._stop_session_fn = stop_session_fn or colab_rvc_mod.stop_session
        self._probe_session_fn = probe_session_fn or colab_rvc_mod.probe_warm_session
        self._clock = clock

        overrides: dict[str, Any] = dict(job_overrides or {})
        if cover_runner is not None:
            overrides["cover_runner"] = cover_runner
        if train_runner is not None:
            overrides["train_runner"] = train_runner
        if dataset_builder is not None:
            overrides["dataset_builder"] = dataset_builder
        self._job_overrides = overrides

        self.lock = asyncio.Lock()
        self.jobs: dict[str, WorkerJobRecord] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.running_job_id: str | None = None
        self._inflight_lock = threading.Lock()
        self._inflight_threads: set[str] = set()
        self.warm_pool = WarmSessionPool(max_sessions=config.warm_max_sessions)
        self._reaper_task: asyncio.Task[None] | None = None
        self._local_handlers: dict[str, JobHandler] = {}

        for handler in handlers or ():
            self.register_job_handler(handler)

    def _is_thread_inflight(self, job_id: str | None = None) -> bool:
        with self._inflight_lock:
            if job_id is None:
                return bool(self._inflight_threads)
            return job_id in self._inflight_threads

    @property
    def worker_draining(self) -> bool:
        with self._inflight_lock:
            for jid in self._inflight_threads:
                rec = self.jobs.get(jid)
                if rec is None or rec.status != "running":
                    return True
            return False

    def register_job_handler(self, handler: JobHandler) -> None:
        """Register a per-worker `JobHandler` override/extension."""
        if not isinstance(handler, JobHandler):
            raise TypeError("handler must inherit from JobHandler")
        if not isinstance(handler.job_type, str) or not VALID_JOB_TYPE_RE.fullmatch(
            handler.job_type
        ):
            raise ValueError(f"Invalid worker job_type: {handler.job_type!r}")
        if not (
            isinstance(handler.payload_model, type) and issubclass(handler.payload_model, BaseModel)
        ):
            raise TypeError("handler.payload_model must be a Pydantic BaseModel subclass")
        if not isinstance(handler.allow_warm_keepalive, bool):
            raise TypeError("handler.allow_warm_keepalive must be a bool")
        self._local_handlers[handler.job_type] = handler

    def _get_handler(self, job_type: str) -> JobHandler | None:
        local = self._local_handlers.get(job_type)
        if local is not None:
            return local
        return get_job_handler(job_type)

    def supported_job_types(self) -> tuple[str, ...]:
        return tuple(sorted(set(registered_job_types()) | set(self._local_handlers)))

    def _trim_history(self) -> None:
        if len(self.jobs) <= MAX_JOB_HISTORY:
            return
        finished = sorted(
            (j for j in self.jobs.values() if j.status != "running"),
            key=lambda j: j.started_at,
        )
        for old in finished[: max(0, len(self.jobs) - MAX_JOB_HISTORY)]:
            self.jobs.pop(old.job_id, None)

    def _can_keep_warm(
        self,
        *,
        job_type: str,
        gpu: str,
        allow_warm_keepalive: bool,
        cu_balance: float | None,
    ) -> bool:
        if not self.cfg.warm_pool_enabled or not allow_warm_keepalive:
            return False
        if not (0 < self.cfg.warm_idle_ttl_seconds <= MAX_WARM_IDLE_TTL_SECONDS):
            return False
        handler = self._get_handler(job_type)
        if handler is None or not handler.allow_warm_keepalive:
            return False
        norm_gpu = str(gpu or "").strip().upper()
        if (
            norm_gpu not in WARM_ALLOWED_GPUS
            or norm_gpu in WARM_FORBIDDEN_GPUS
            or norm_gpu not in self.cfg.warm_allowed_gpus
        ):
            return False
        if cu_balance is not None and cu_balance < self.cfg.warm_min_cu_balance:
            return False
        return True

    def _stop_sessions_sync(self, entries: list[WarmSessionEntry]) -> None:
        for entry in entries:
            try:
                self._stop_session_fn(entry.session_name)
            except Exception:
                pass

    def _build_job_context(
        self,
        record: WorkerJobRecord,
        dispatch_context: DispatchContext | None,
        *,
        allow_warm_keepalive: bool,
        cu_balance_hint: float | None,
    ) -> WorkerJobContext:
        def _claim_warm() -> WarmSessionClaim:
            claimed, to_stop = self.warm_pool.claim_matching(
                record.gpu_candidates, now=self._clock()
            )
            if to_stop:
                self._stop_sessions_sync(to_stop)
            if claimed is None:
                return WarmSessionClaim()
            try:
                probe = self._probe_session_fn(claimed.session_name) or {}
            except Exception:
                probe = {"healthy": False, "setup_ready": False}
            if probe.get("healthy"):
                record.session_name = claimed.session_name
                setup_ready = bool(probe.get("setup_ready"))
                sep_ready = bool(probe["sep_ready"]) if "sep_ready" in probe else setup_ready
                svc_ready = bool(probe["svc_ready"]) if "svc_ready" in probe else setup_ready
                rvc_ready = bool(probe["rvc_ready"]) if "rvc_ready" in probe else setup_ready
                return WarmSessionClaim(
                    entry=claimed,
                    reused=True,
                    gpu=claimed.gpu,
                    setup_ready=setup_ready,
                    sep_ready=sep_ready,
                    svc_ready=svc_ready,
                    rvc_ready=rvc_ready,
                )
            self._stop_sessions_sync([claimed])
            return WarmSessionClaim()

        def _park_or_stop_warm(assigned_gpu: str, prior_warm: Any | None) -> None:
            if not record.cancelled and self._can_keep_warm(
                job_type=record.job_type,
                gpu=assigned_gpu,
                allow_warm_keepalive=allow_warm_keepalive,
                cu_balance=cu_balance_hint,
            ):
                now = self._clock()
                entry = WarmSessionEntry(
                    session_name=record.session_name,
                    gpu=assigned_gpu,
                    idle_ttl_seconds=self.cfg.warm_idle_ttl_seconds,
                    created_at=prior_warm.created_at if prior_warm else now,
                    last_used_at=now,
                    expires_at=now + self.cfg.warm_idle_ttl_seconds,
                    reuse_count=(prior_warm.reuse_count + 1) if prior_warm else 0,
                    setup_ready=True,
                )
                evicted = self.warm_pool.put(entry)
                if evicted:
                    self._stop_sessions_sync(evicted)
            else:
                try:
                    self._stop_session_fn(record.session_name)
                except Exception:
                    pass

        def _drain_warm() -> None:
            idle_warm = self.warm_pool.pop_all()
            if idle_warm:
                self._stop_sessions_sync(idle_warm)

        return WorkerJobContext(
            job_id=record.job_id,
            job_type=record.job_type,
            gpu_candidates=tuple(record.gpu_candidates),
            session_name=record.session_name,
            config=self.cfg,
            dispatch_context=dispatch_context,
            allow_warm_keepalive=allow_warm_keepalive,
            cu_balance_hint=cu_balance_hint,
            drive_transport=self._drive_transport,
            stop_session_fn=self._stop_session_fn,
            clock=self._clock,
            overrides=dict(self._job_overrides),
            cancel_event=record.cancel_event,
            _is_cancelled_fn=lambda: record.cancelled or record.cancel_event.is_set(),
            _can_keep_warm_fn=lambda gpu: self._can_keep_warm(
                job_type=record.job_type,
                gpu=gpu,
                allow_warm_keepalive=allow_warm_keepalive,
                cu_balance=cu_balance_hint,
            ),
            _claim_warm_fn=_claim_warm,
            _park_warm_fn=_park_or_stop_warm,
            _drain_warm_fn=_drain_warm,
        )

    def warm_pool_summary(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        now = self._clock()
        sessions = self.warm_pool.snapshot(now=now)
        pool_info = {
            "enabled": self.cfg.warm_pool_enabled,
            "allowed_gpus": list(self.cfg.warm_allowed_gpus),
            "idle_ttl_seconds": self.cfg.warm_idle_ttl_seconds,
            "max_ttl_seconds": MAX_WARM_IDLE_TTL_SECONDS,
            "active_count": len(sessions),
        }
        return sessions, pool_info

    def health_payload(self) -> dict[str, Any]:
        sessions, pool_info = self.warm_pool_summary()
        return {
            "ok": True,
            "warm_active_count": len(sessions),
            "warm_sessions": sessions,
            "warm_pool": pool_info,
        }

    async def reap_expired_warm_sessions(self) -> int:
        expired = self.warm_pool.pop_expired(now=self._clock())
        if not expired:
            return 0
        await asyncio.to_thread(self._stop_sessions_sync, expired)
        return len(expired)

    async def _reaper_loop(self, interval_seconds: float = 5.0) -> None:
        sleep_sec = max(0.5, min(30.0, float(interval_seconds)))
        while True:
            try:
                await asyncio.sleep(sleep_sec)
                await self.reap_expired_warm_sessions()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    def start_reaper(self, interval_seconds: float = 5.0) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.create_task(self._reaper_loop(interval_seconds))

    async def shutdown(self) -> int:
        """Stop background reaper and terminate every warm-kept Colab session cleanly."""
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task
        self._reaper_task = None
        remaining = self.warm_pool.pop_all()
        if remaining:
            await asyncio.to_thread(self._stop_sessions_sync, remaining)
        return len(remaining)

    async def status_payload(self) -> dict[str, Any]:
        await self.reap_expired_warm_sessions()
        usage = await asyncio.to_thread(self._usage_fn)
        warm_sessions, warm_pool = self.warm_pool_summary()
        raw_err = usage.get("error")
        err = _scrub_secret(str(raw_err), self.cfg.token) if raw_err else None
        usage_active = max(0, int(usage.get("active_sessions") or 0))
        untracked_active = max(0, usage_active - len(warm_sessions))
        return {
            "reachable": bool(usage.get("reachable", True)),
            "compute_units": usage.get("compute_units"),
            "usage_rate": usage.get("usage_rate"),
            "active_sessions": max(usage_active, len(warm_sessions)),
            "untracked_active_sessions": untracked_active,
            "running_job_id": self.running_job_id,
            "worker_draining": self.worker_draining,
            "supported_job_types": list(self.supported_job_types()),
            "warm_sessions": warm_sessions,
            "warm_pool": warm_pool,
            "error": err,
        }

    async def submit_job(self, req: WorkerJobSubmit) -> dict[str, Any]:
        handler = self._get_handler(req.job_type)
        if handler is None:
            raise HTTPException(400, "colab_invalid_job")
        try:
            parsed = handler.validate_payload(req.payload)
        except JobValidationError as exc:
            raise HTTPException(400, exc.code) from None
        except ValidationError:
            raise HTTPException(400, "colab_invalid_job") from None

        async with self.lock:
            if self.running_job_id is not None:
                active = self.jobs.get(self.running_job_id)
                if active is not None and active.status == "running":
                    raise HTTPException(409, "worker_busy")
                if self._is_thread_inflight(self.running_job_id):
                    raise HTTPException(409, "worker_draining")
                self.running_job_id = None
            if self._is_thread_inflight():
                raise HTTPException(409, "worker_draining")

            session_name = f"hakimi-w-{req.job_id[:8]}"
            record = WorkerJobRecord(
                job_id=req.job_id,
                job_type=req.job_type,
                status="running",
                gpu_candidates=list(req.gpu_candidates),
                session_name=session_name,
            )
            self.jobs[req.job_id] = record
            self.running_job_id = req.job_id
            self._trim_history()
            # Security invariant: dispatch_context is passed strictly as a local argument
            # and never stored on WorkerJobRecord or WorkerState.
            task = asyncio.create_task(
                self._execute_job(
                    record,
                    handler,
                    parsed,
                    req.timeout_seconds,
                    req.dispatch_context,
                    allow_warm_keepalive=req.allow_warm_keepalive,
                )
            )
            self.tasks[req.job_id] = task
        return {"job_id": req.job_id, "status": "running"}

    async def _execute_job(
        self,
        record: WorkerJobRecord,
        handler: JobHandler,
        parsed: Any,
        timeout_seconds: int,
        dispatch_context: DispatchContext | None,
        *,
        allow_warm_keepalive: bool = True,
    ) -> None:
        ephemeral_token = (
            dispatch_context.drive.access_token
            if (dispatch_context is not None and dispatch_context.drive is not None)
            else ""
        )
        cu_before: float | None = None
        try:
            usage_before = await asyncio.to_thread(self._usage_fn)
            if isinstance(usage_before.get("compute_units"), (int, float)):
                cu_before = float(usage_before["compute_units"])
        except Exception:
            cu_before = None

        job_ctx: WorkerJobContext | None = None
        loop = asyncio.get_running_loop()

        def _clear_draining_on_loop(jid: str) -> None:
            rec = self.jobs.get(jid)
            if self.running_job_id == jid and (rec is None or rec.status != "running"):
                self.running_job_id = None

        def _run_sync_tracked(ctx: WorkerJobContext, parsed_arg: Any) -> tuple[dict[str, Any], str]:
            try:
                return handler.run_sync(ctx, parsed_arg)
            finally:
                with self._inflight_lock:
                    self._inflight_threads.discard(record.job_id)
                if record.cancelled or record.cancel_event.is_set():
                    if self.warm_pool.remove_by_session_name(record.session_name):
                        try:
                            self._stop_session_fn(record.session_name)
                        except Exception:
                            pass
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(_clear_draining_on_loop, record.job_id)

        try:
            job_ctx = self._build_job_context(
                record,
                dispatch_context,
                allow_warm_keepalive=allow_warm_keepalive,
                cu_balance_hint=cu_before,
            )
            if record.cancelled or record.cancel_event.is_set():
                raise asyncio.CancelledError
            with self._inflight_lock:
                self._inflight_threads.add(record.job_id)
            try:
                async with asyncio.timeout(timeout_seconds):
                    res, assigned_gpu = await asyncio.to_thread(_run_sync_tracked, job_ctx, parsed)
            except (TimeoutError, asyncio.CancelledError):
                # Thread may still be draining cooperatively; cancel_event wakes it up.
                record.cancelled = True
                record.cancel_event.set()
                raise
            if record.cancelled or record.cancel_event.is_set():
                record.status = "cancelled"
                record.error_code = record.error_code or "cancelled"
                if self.warm_pool.remove_by_session_name(record.session_name):
                    await asyncio.to_thread(self._stop_session_fn, record.session_name)
            else:
                record.status = "succeeded"
                record.result = res
                record.assigned_gpu = assigned_gpu
        except TimeoutError:
            record.cancelled = True
            record.cancel_event.set()
            record.status = "failed"
            record.error_code = "job_timeout"
            record.error = "Job timed out"
            record.result = {"error_code": "job_timeout"}
            self.warm_pool.remove_by_session_name(record.session_name)
            await asyncio.to_thread(self._stop_session_fn, record.session_name)
        except asyncio.CancelledError:
            record.cancelled = True
            record.cancel_event.set()
            record.status = "cancelled"
            record.error_code = record.error_code or "cancelled"
            record.result = {"error_code": record.error_code}
            self.warm_pool.remove_by_session_name(record.session_name)
            await asyncio.to_thread(self._stop_session_fn, record.session_name)
        except Exception as exc:
            allowed_codes = _ALLOWED_WORKER_ERROR_CODES | frozenset(handler.allowed_error_codes)
            err_code = getattr(exc, "code", None) or "colab_worker_error"
            if not isinstance(err_code, str) or err_code not in allowed_codes:
                err_code = "colab_worker_error"
            if record.cancelled or record.cancel_event.is_set():
                record.status = "cancelled"
                record.error_code = record.error_code or "cancelled"
            else:
                record.status = "failed"
                record.error_code = err_code
                record.error = _scrub_secret(str(exc), self.cfg.token, ephemeral_token)
                record.result = {"error_code": err_code}
        finally:
            if job_ctx is not None:
                job_ctx.dispatch_context = None
            dispatch_context = None
            ephemeral_token = ""
            try:
                usage_after = await asyncio.to_thread(self._usage_fn)
                raw_cu = usage_after.get("compute_units")
                if cu_before is not None and isinstance(raw_cu, (int, float)):
                    record.cu_used = max(0.0, round(cu_before - float(raw_cu), 4))
                if (
                    isinstance(raw_cu, (int, float))
                    and float(raw_cu) < self.cfg.warm_min_cu_balance
                ):
                    drained = self.warm_pool.pop_all()
                    if drained:
                        await asyncio.to_thread(self._stop_sessions_sync, drained)
            except Exception:
                pass
            record.finished_at = self._clock()
            async with self.lock:
                if self.running_job_id == record.job_id and not self._is_thread_inflight(
                    record.job_id
                ):
                    self.running_job_id = None
                self.tasks.pop(record.job_id, None)

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        if not VALID_JOB_ID_RE.fullmatch(job_id):
            raise HTTPException(400, "invalid_job_id")
        async with self.lock:
            record = self.jobs.get(job_id)
            if record is None:
                raise HTTPException(404, "job_not_found")
            if record.status != "running":
                return record.to_dict()
            record.cancelled = True
            record.cancel_event.set()
            record.status = "cancelled"
            record.error_code = "cancelled"
            record.finished_at = self._clock()
            task = self.tasks.get(job_id)
            if self.running_job_id == job_id and not self._is_thread_inflight(job_id):
                self.running_job_id = None
        if task is not None and not task.done():
            task.cancel()
        self.warm_pool.remove_by_session_name(record.session_name)
        await asyncio.to_thread(self._stop_session_fn, record.session_name)
        return record.to_dict()

    async def terminate_all_sessions(self) -> dict[str, Any]:
        cancelled_jobs = 0
        extra_sessions: list[str] = []
        async with self.lock:
            for job_id, record in self.jobs.items():
                if record.status == "running":
                    record.cancelled = True
                    record.cancel_event.set()
                    record.status = "cancelled"
                    record.error_code = "emergency_terminated"
                    record.finished_at = self._clock()
                    cancelled_jobs += 1
                    extra_sessions.append(record.session_name)
                    task = self.tasks.get(job_id)
                    if task is not None and not task.done():
                        task.cancel()
            if not self._is_thread_inflight(self.running_job_id):
                self.running_job_id = None
        warm_entries = self.warm_pool.pop_all()
        for entry in warm_entries:
            if entry.session_name not in extra_sessions:
                extra_sessions.append(entry.session_name)
        stop_res = await asyncio.to_thread(self._stop_all_fn, extra_sessions)
        return {
            "terminated_sessions": int(stop_res.get("terminated_sessions", 0)),
            "cancelled_jobs": cancelled_jobs,
            "detail": f"sessions={','.join(stop_res.get('sessions') or []) or 'none'}",
        }


def create_worker_app(
    config: WorkerConfig,
    *,
    drive_transport: TransportFn | None = None,
    usage_fn: Callable[[], dict[str, Any]] | None = None,
    stop_all_fn: Callable[[list[str] | None], dict[str, Any]] | None = None,
    stop_session_fn: Callable[[str], None] | None = None,
    probe_session_fn: Callable[[str], dict[str, Any]] | None = None,
    cover_runner: Callable[..., dict[str, Any]] | None = None,
    train_runner: Callable[..., dict[str, Any]] | None = None,
    dataset_builder: Callable[..., dict[str, Any]] | None = None,
    job_overrides: dict[str, Any] | None = None,
    handlers: Sequence[JobHandler] | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    state = WorkerState(
        config,
        drive_transport=drive_transport,
        usage_fn=usage_fn,
        stop_all_fn=stop_all_fn,
        stop_session_fn=stop_session_fn,
        probe_session_fn=probe_session_fn,
        cover_runner=cover_runner,
        train_runner=train_runner,
        dataset_builder=dataset_builder,
        job_overrides=job_overrides,
        handlers=handlers,
        clock=clock,
    )

    @contextlib.asynccontextmanager
    async def _lifespan(_app: FastAPI):
        state.start_reaper()
        try:
            yield
        finally:
            await state.shutdown()

    app = FastAPI(
        title="Hakimi Colab GPU Worker",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=_lifespan,
    )
    app.state.worker_state = state

    def _auth(request: Request) -> None:
        if request.query_params:
            raise HTTPException(400, "query_params_forbidden")
        tok = config.token.strip()
        if not tok:
            raise HTTPException(503, "worker_token_unconfigured")
        expected = f"Bearer {tok}".encode()
        got = request.headers.get("authorization", "").encode("utf-8", errors="ignore")
        if not hmac.compare_digest(got, expected):
            raise HTTPException(401, "authentication_required")

    async def _read_bounded_bytes(request: Request, max_bytes: int) -> bytes:
        try:
            declared = int(request.headers.get("content-length", "0"))
        except ValueError:
            raise HTTPException(400, "invalid_content_length") from None
        if declared < 0 or declared > max_bytes:
            raise HTTPException(413, "payload_too_large")
        buf = bytearray()
        async for chunk in request.stream():
            buf.extend(chunk)
            if len(buf) > max_bytes:
                raise HTTPException(413, "payload_too_large")
        return bytes(buf)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return state.health_payload()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return state.health_payload()

    @app.get("/v1/status")
    async def v1_status(request: Request) -> dict[str, Any]:
        _auth(request)
        return await state.status_payload()

    @app.post("/v1/jobs", status_code=202)
    async def v1_submit_job(request: Request) -> dict[str, Any]:
        _auth(request)
        raw = await _read_bounded_bytes(request, MAX_JOB_BODY_BYTES)
        try:
            obj = json.loads(raw.decode("utf-8"))
            req = WorkerJobSubmit.model_validate(obj)
        except (ValueError, UnicodeDecodeError, ValidationError):
            raise HTTPException(400, "colab_invalid_job") from None
        try:
            return await state.submit_job(req)
        except ValidationError:
            raise HTTPException(400, "colab_invalid_job") from None

    @app.get("/v1/jobs/{job_id}")
    async def v1_get_job(request: Request, job_id: str) -> dict[str, Any]:
        _auth(request)
        if not VALID_JOB_ID_RE.fullmatch(job_id):
            raise HTTPException(400, "invalid_job_id")
        record = state.jobs.get(job_id)
        if record is None:
            raise HTTPException(404, "job_not_found")
        return record.to_dict()

    @app.post("/v1/jobs/{job_id}/cancel")
    async def v1_cancel_job(request: Request, job_id: str) -> dict[str, Any]:
        _auth(request)
        return await state.cancel_job(job_id)

    @app.post("/v1/sessions/terminate")
    async def v1_terminate_sessions(request: Request) -> dict[str, Any]:
        _auth(request)
        return await state.terminate_all_sessions()

    return app


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hakimi Colab GPU HTTP worker.")
    parser.add_argument("--host", default=os.environ.get("COLAB_WORKER_HOST", DEFAULT_HOST))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("COLAB_WORKER_PORT", DEFAULT_PORT))
    )
    parser.add_argument("--allow-private", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not _is_allowed_bind_host(args.host, allow_private=args.allow_private):
        raise SystemExit(
            "Refusing non-loopback host bind without --allow-private (private IP only)."
        )
    token = os.environ.get("COLAB_WORKER_TOKEN", "").strip()
    if len(token) < 16:
        raise SystemExit("COLAB_WORKER_TOKEN (>=16 chars) is required.")
    raw_warm_ttl = os.environ.get("COLAB_WARM_IDLE_TTL_SECONDS", "").strip()
    try:
        warm_ttl = int(raw_warm_ttl) if raw_warm_ttl else DEFAULT_WARM_IDLE_TTL_SECONDS
    except ValueError:
        warm_ttl = DEFAULT_WARM_IDLE_TTL_SECONDS
    raw_warm_gpus = os.environ.get("COLAB_WARM_ALLOWED_GPUS", "L4,T4").strip()
    warm_gpus = tuple(g.strip().upper() for g in raw_warm_gpus.split(",") if g.strip())
    raw_min_cu = os.environ.get("COLAB_WARM_MIN_CU_BALANCE", "").strip()
    try:
        warm_min_cu = float(raw_min_cu) if raw_min_cu else DEFAULT_WARM_MIN_CU_BALANCE
    except ValueError:
        warm_min_cu = DEFAULT_WARM_MIN_CU_BALANCE

    cfg = WorkerConfig(
        host=args.host,
        port=args.port,
        token=token,
        drive_direct_enabled=_parse_bool_env("COLAB_DRIVE_DIRECT_ENABLED", True),
        warm_pool_enabled=_parse_bool_env("COLAB_WARM_POOL_ENABLED", True),
        warm_idle_ttl_seconds=warm_ttl,
        warm_allowed_gpus=warm_gpus or ("L4", "T4"),
        warm_min_cu_balance=warm_min_cu,
        cache_enabled=_parse_bool_env("COLAB_CACHE_ENABLED", True),
        cache_venv_enabled=_parse_bool_env("COLAB_CACHE_VENV_ENABLED", False),
        cache_writeback=_parse_bool_env("COLAB_CACHE_WRITEBACK", True),
    )
    app = create_worker_app(cfg)
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="warning", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
