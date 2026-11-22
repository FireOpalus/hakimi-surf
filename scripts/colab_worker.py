"""Out-of-process host worker for Google Colab GPU jobs and Hakimi_Songs/ Drive proxy.

Runs strictly outside the `hakimi-bot` container on loopback HTTP, authenticated by
a constant-time Bearer token (`hmac.compare_digest`). Decouples Colab GPU auth
(`secrets/colab-adc.json`) from Drive OAuth (`secrets/colab-drive-oauth.json` or
read-only `secrets/roaming-oauth.json`) and never logs or returns raw credentials.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import importlib.util
import inspect
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 12146
MAX_INBOX_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_SONG_UPLOAD_BYTES = 30 * 1024 * 1024
MAX_JOB_BODY_BYTES = 64 * 1024
MAX_JOB_HISTORY = 100

WARM_ALLOWED_GPUS: frozenset[str] = frozenset({"L4", "T4"})
WARM_FORBIDDEN_GPUS: frozenset[str] = frozenset({"H100", "A100", "G4"})
DEFAULT_WARM_IDLE_TTL_SECONDS = 360
MIN_WARM_IDLE_TTL_SECONDS = 60
MAX_WARM_IDLE_TTL_SECONDS = 900
DEFAULT_WARM_MIN_CU_BALANCE = 1.0

GpuName = Literal["H100", "A100", "G4", "L4", "T4"]
VALID_DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
VALID_FOLDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{0,128}$")
VALID_JOB_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
VALID_MODEL_NAME_RE = re.compile(r"^[a-z0-9_-]{1,32}$")


def _load_sibling_module(name: str, filename: str):
    if name in sys.modules:
        return sys.modules[name]
    mod_path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load sibling module {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


colab_drive_mod = _load_sibling_module("colab_drive", "colab_drive.py")
colab_remote_mod = _load_sibling_module("colab_remote_rvc", "colab_remote_rvc.py")
colab_rvc_mod = _load_sibling_module("colab_rvc", "colab_rvc.py")

ColabDriveClient = colab_drive_mod.ColabDriveClient
ColabDriveError = colab_drive_mod.ColabDriveError
resolve_drive_credentials_path = colab_drive_mod.resolve_drive_credentials_path
sanitize_drive_filename = colab_drive_mod.sanitize_drive_filename
scrub_drive_secrets = colab_drive_mod.scrub_drive_secrets


class RvcCoverPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=80)
    artist: str = Field(default="", max_length=60)
    tags: str = Field(default="", max_length=120)
    pitch: int = Field(default=0, ge=-24, le=24)
    auto_search: bool = True
    query: str = Field(default="", max_length=120)
    inbox_file_id: str = Field(default="", max_length=128)
    inbox_filename: str = Field(default="", max_length=160)
    chorus_anchor: str = Field(default="", max_length=120)
    start_sec: str | float | int = "auto"
    duration_sec: float = Field(default=50.0, ge=5.0, le=56.0, allow_inf_nan=False)
    fallback_start_sec: float = Field(default=60.0, ge=0.0, le=600.0, allow_inf_nan=False)
    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    index_rate: float = Field(default=0.25, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float = Field(default=0.28, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float = Field(default=0.25, ge=0.0, le=1.0, allow_inf_nan=False)

    @field_validator("title")
    @classmethod
    def _clean_title(cls, v: str) -> str:
        cleaned = " ".join(v.split()).strip()
        if not cleaned:
            raise ValueError("title cannot be blank")
        return cleaned

    @field_validator("inbox_file_id")
    @classmethod
    def _check_inbox_id(cls, v: str) -> str:
        cleaned = v.strip()
        if cleaned and not VALID_DRIVE_ID_RE.fullmatch(cleaned):
            raise ValueError("invalid inbox_file_id")
        return cleaned

    @field_validator("inbox_filename")
    @classmethod
    def _check_inbox_filename(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            return ""
        return sanitize_drive_filename(cleaned)

    @field_validator("start_sec")
    @classmethod
    def _check_start_sec(cls, v: str | float | int) -> str:
        if isinstance(v, bool):
            raise ValueError("invalid start_sec")
        if isinstance(v, (int, float)):
            val = float(v)
            if not (0.0 <= val <= 600.0):
                raise ValueError("start_sec out of range")
            return f"{val:.2f}"
        raw = str(v).strip()
        if not raw or raw.lower() == "auto":
            return "auto"
        val = float(raw)
        if not (0.0 <= val <= 600.0):
            raise ValueError("start_sec out of range")
        return f"{val:.2f}"


class RvcTrainPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    epochs: int = Field(default=150, ge=10, le=1000)
    save_every: int = Field(default=30, ge=5, le=500)
    batch_size: int = Field(default=12, ge=1, le=32)
    sample_rate: Literal[32000, 40000, 48000] = 48000
    force_dataset: bool = False


class WorkerJobSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(pattern=r"^[a-f0-9]{16,64}$")
    job_type: Literal["rvc_cover", "rvc_train"]
    gpu_candidates: list[GpuName] = Field(
        default_factory=lambda: ["L4", "T4"], min_length=1, max_length=5
    )
    timeout_seconds: int = Field(default=1800, ge=120, le=7200)
    drive_songs_folder_id: str = Field(default="", max_length=128)
    allow_warm_keepalive: bool = True
    payload: dict[str, Any]

    @field_validator("drive_songs_folder_id")
    @classmethod
    def _check_folder_id(cls, v: str) -> str:
        cleaned = v.strip()
        if cleaned == "root" or (cleaned and not VALID_FOLDER_ID_RE.fullmatch(cleaned)):
            raise ValueError("invalid drive_songs_folder_id")
        return cleaned


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
    drive_cred_path: Path | None = None
    drive_songs_folder_id: str = ""
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
    job_id: str
    job_type: Literal["rvc_cover", "rvc_train"]
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


def _scrub_secret(text: str, *secrets: str) -> str:
    cleaned = " ".join(scrub_drive_secrets(text, *secrets).split())
    return cleaned[:240]


def _filter_runner_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Pass only keyword arguments accepted by `fn` (preserving test stub compatibility)."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return kwargs
    params = sig.parameters
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return kwargs
    return {k: v for k, v in kwargs.items() if k in params}


def _runner_accepts_direct_drive(fn: Callable[..., Any]) -> bool:
    if fn is colab_rvc_mod.run_cover_pipeline or fn is colab_rvc_mod.run_train_pipeline:
        return True
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return "drive_access_token" in sig.parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _probe_audio_duration_ms(path: Path, fallback_sec: float = 50.0) -> int:
    ffprobe = shutil.which("ffprobe") or "ffprobe"
    try:
        proc = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        if proc.returncode == 0:
            val = float((proc.stdout or "").strip())
            if 0.5 <= val <= 120.0:
                return int(round(val * 1000.0))
    except Exception:
        pass
    return int(round(max(1.0, min(60.0, fallback_sec)) * 1000.0))


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
    """Thread-safe / asyncio-safe coordinator for host Colab + Drive operations."""

    def __init__(
        self,
        config: WorkerConfig,
        *,
        drive_factory: Callable[[str], Any] | None = None,
        usage_fn: Callable[[], dict[str, Any]] | None = None,
        stop_all_fn: Callable[[list[str] | None], dict[str, Any]] | None = None,
        stop_session_fn: Callable[[str], None] | None = None,
        probe_session_fn: Callable[[str], dict[str, Any]] | None = None,
        cover_runner: Callable[..., dict[str, Any]] | None = None,
        train_runner: Callable[..., dict[str, Any]] | None = None,
        clean_search_fn: Callable[..., Path] | None = None,
        dataset_builder: Callable[..., dict[str, Any]] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cfg = config
        self._drive_factory = drive_factory or self._default_drive_factory
        self._usage_fn = usage_fn or colab_rvc_mod.parse_colab_usage
        self._stop_all_fn = stop_all_fn or colab_rvc_mod.stop_all_sessions
        self._stop_session_fn = stop_session_fn or colab_rvc_mod.stop_session
        self._probe_session_fn = probe_session_fn or colab_rvc_mod.probe_warm_session
        self._cover_runner = cover_runner or colab_rvc_mod.run_cover_pipeline
        self._train_runner = train_runner or colab_rvc_mod.run_train_pipeline
        self._clean_search_fn = clean_search_fn or colab_rvc_mod.search_and_download_clean_source
        self._dataset_builder = dataset_builder or colab_rvc_mod.build_dataset
        self._clock = clock
        self.lock = asyncio.Lock()
        self.jobs: dict[str, WorkerJobRecord] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.running_job_id: str | None = None
        self._cached_folders: dict[str, str] | None = None
        self.warm_pool = WarmSessionPool(max_sessions=config.warm_max_sessions)
        self._reaper_task: asyncio.Task[None] | None = None

    def _default_drive_factory(self, root_folder_id: str = "") -> Any:
        cred_path = resolve_drive_credentials_path(self.cfg.drive_cred_path, repo_root=REPO_ROOT)
        return ColabDriveClient(
            cred_path,
            root_folder_id=root_folder_id or self.cfg.drive_songs_folder_id,
        )

    def get_drive(self, root_folder_id: str = "") -> Any:
        effective_root = root_folder_id.strip() or self.cfg.drive_songs_folder_id
        client = self._drive_factory(effective_root)
        tree = client.ensure_folder_tree(effective_root)
        self._cached_folders = tree.as_dict()
        return client

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
        if job_type != "rvc_cover":
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

    def _issue_short_lived_drive_token(self, drive: Any) -> str:
        """Issue an in-memory short-lived access_token only (never persists or logs tokens)."""
        if not self.cfg.drive_direct_enabled:
            return ""
        getter = getattr(drive, "get_short_lived_token", None)
        if not callable(getter):
            return ""
        try:
            tok_res = getter()
        except Exception:
            return ""
        if isinstance(tok_res, dict):
            return str(tok_res.get("access_token") or "").strip()
        if isinstance(tok_res, str):
            return tok_res.strip()
        return ""

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
        drive_folders = self._cached_folders
        drive_err: str | None = None
        try:
            client = await asyncio.to_thread(self.get_drive, "")
            drive_folders = client.ensure_folder_tree().as_dict()
            self._cached_folders = drive_folders
        except ColabDriveError as exc:
            drive_err = exc.code
        except Exception as exc:
            drive_err = f"drive_error:{type(exc).__name__}"

        warm_sessions, warm_pool = self.warm_pool_summary()
        raw_err = usage.get("error") or drive_err
        err = _scrub_secret(str(raw_err), self.cfg.token) if raw_err else None
        return {
            "reachable": bool(usage.get("reachable", True)),
            "compute_units": usage.get("compute_units"),
            "usage_rate": usage.get("usage_rate"),
            "active_sessions": max(int(usage.get("active_sessions") or 0), len(warm_sessions)),
            "running_job_id": self.running_job_id,
            "drive_folders": drive_folders,
            "warm_sessions": warm_sessions,
            "warm_pool": warm_pool,
            "error": err,
        }

    async def submit_job(self, req: WorkerJobSubmit) -> dict[str, Any]:
        if req.job_type == "rvc_cover":
            parsed: RvcCoverPayload | RvcTrainPayload = RvcCoverPayload.model_validate(req.payload)
            if not parsed.inbox_file_id and not parsed.inbox_filename and not parsed.auto_search:
                raise HTTPException(400, "song_source_required")
        else:
            parsed = RvcTrainPayload.model_validate(req.payload)

        async with self.lock:
            if self.running_job_id is not None:
                active = self.jobs.get(self.running_job_id)
                if active is not None and active.status == "running":
                    raise HTTPException(409, "worker_busy")
                self.running_job_id = None

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
            task = asyncio.create_task(
                self._execute_job(
                    record,
                    parsed,
                    req.timeout_seconds,
                    req.drive_songs_folder_id,
                    allow_warm_keepalive=req.allow_warm_keepalive,
                )
            )
            self.tasks[req.job_id] = task
        return {"job_id": req.job_id, "status": "running"}

    async def _execute_job(
        self,
        record: WorkerJobRecord,
        parsed: RvcCoverPayload | RvcTrainPayload,
        timeout_seconds: int,
        drive_songs_folder_id: str,
        *,
        allow_warm_keepalive: bool = True,
    ) -> None:
        cu_before: float | None = None
        try:
            usage_before = await asyncio.to_thread(self._usage_fn)
            if isinstance(usage_before.get("compute_units"), (int, float)):
                cu_before = float(usage_before["compute_units"])
        except Exception:
            cu_before = None

        try:
            async with asyncio.timeout(timeout_seconds):
                if record.job_type == "rvc_cover":
                    assert isinstance(parsed, RvcCoverPayload)
                    res, assigned_gpu = await asyncio.to_thread(
                        self._run_cover_sync,
                        record,
                        parsed,
                        drive_songs_folder_id,
                        allow_warm_keepalive,
                        cu_before,
                    )
                else:
                    assert isinstance(parsed, RvcTrainPayload)
                    res, assigned_gpu = await asyncio.to_thread(
                        self._run_train_sync, record, parsed, drive_songs_folder_id
                    )
            if record.cancelled:
                record.status = "cancelled"
                record.error_code = record.error_code or "cancelled"
                if self.warm_pool.remove_by_session_name(record.session_name):
                    await asyncio.to_thread(self._stop_session_fn, record.session_name)
            else:
                record.status = "succeeded"
                record.result = res
                record.assigned_gpu = assigned_gpu
        except TimeoutError:
            record.status = "failed"
            record.error_code = "job_timeout"
            record.error = "Job timed out"
            record.result = {"error_code": "job_timeout"}
            self.warm_pool.remove_by_session_name(record.session_name)
            await asyncio.to_thread(self._stop_session_fn, record.session_name)
        except asyncio.CancelledError:
            record.status = "cancelled"
            record.error_code = "cancelled"
            record.result = {"error_code": "cancelled"}
            self.warm_pool.remove_by_session_name(record.session_name)
            await asyncio.to_thread(self._stop_session_fn, record.session_name)
        except Exception as exc:
            err_code = getattr(exc, "code", None) or "colab_worker_error"
            if err_code not in {
                "source_not_found",
                "model_not_found",
                "drive_insufficient_scope",
                "drive_file_outside_scope",
                "drive_credentials_missing",
                "drive_credentials_invalid",
            }:
                err_code = "colab_worker_error"
            if record.cancelled:
                record.status = "cancelled"
                record.error_code = "cancelled"
            else:
                record.status = "failed"
                record.error_code = err_code
                record.error = _scrub_secret(str(exc), self.cfg.token)
                record.result = {"error_code": err_code}
        finally:
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
                if self.running_job_id == record.job_id:
                    self.running_job_id = None
                self.tasks.pop(record.job_id, None)

    def _run_cover_sync(
        self,
        record: WorkerJobRecord,
        payload: RvcCoverPayload,
        drive_songs_folder_id: str,
        allow_warm_keepalive: bool = True,
        cu_balance_hint: float | None = None,
    ) -> tuple[dict[str, Any], str]:
        drive = self.get_drive(drive_songs_folder_id)
        tree = drive.ensure_folder_tree()
        slug = colab_rvc_mod._slugify(payload.title)

        # 0) Check warm session pool (P1)
        prior_warm: WarmSessionEntry | None = None
        warm_reused = False
        reused_gpu = ""
        reused_setup_ready = False
        claimed, to_stop = self.warm_pool.claim_matching(record.gpu_candidates, now=self._clock())
        if to_stop:
            self._stop_sessions_sync(to_stop)
        if claimed is not None:
            probe = {}
            try:
                probe = self._probe_session_fn(claimed.session_name) or {}
            except Exception:
                probe = {"healthy": False, "setup_ready": False}
            if probe.get("healthy"):
                prior_warm = claimed
                warm_reused = True
                reused_gpu = claimed.gpu
                reused_setup_ready = bool(probe.get("setup_ready"))
                record.session_name = claimed.session_name
            else:
                self._stop_sessions_sync([claimed])

        # 1) Issue in-memory short-lived Drive access token (P0)
        drive_token = ""
        token_fallback = False
        direct_supported = _runner_accepts_direct_drive(self._cover_runner)
        if self.cfg.drive_direct_enabled and direct_supported:
            drive_token = self._issue_short_lived_drive_token(drive)
            if not drive_token:
                token_fallback = True

        with tempfile.TemporaryDirectory(prefix=f"hakimi_job_{record.job_id[:8]}_") as tmp:
            tmp_dir = Path(tmp)
            source_dir = tmp_dir / "source"
            cover_dir = tmp_dir / "cover"
            source_dir.mkdir(parents=True, exist_ok=True)
            cover_dir.mkdir(parents=True, exist_ok=True)

            # 2) Resolve input audio source from inbox (direct Drive ID when possible) or search
            source_path: Path | None = None
            drive_source_file_id = ""
            drive_source_ext = ".m4a"
            ensure_local_source_fn: Callable[[], Path] | None = None

            if payload.inbox_file_id or payload.inbox_filename:
                inbox_meta = (
                    drive.resolve_inbox_file(file_id=payload.inbox_file_id)
                    if payload.inbox_file_id
                    else drive.resolve_inbox_file(filename=payload.inbox_filename)
                )
                drive_source_ext = Path(inbox_meta.get("name") or "source.m4a").suffix or ".m4a"

                def _dl_inbox_local() -> Path:
                    return drive.download_file(
                        inbox_meta["id"],
                        source_dir / f"inbox_source{drive_source_ext}",
                        allowed_roles=("inbox",),
                    )

                if drive_token:
                    drive_source_file_id = str(inbox_meta["id"])
                    ensure_local_source_fn = _dl_inbox_local
                else:
                    source_path = _dl_inbox_local()
            else:
                query = payload.query.strip() or payload.title
                source_path = self._clean_search_fn(query, source_dir, artist=payload.artist)

            if record.cancelled:
                raise RuntimeError("cancelled")

            # 3) Resolve RVC model weights (direct Drive ID when available, else local fallback)
            model_subdir = self.cfg.model_dir / payload.model_name
            pth_path: Path | None = None
            index_path: Path | None = None
            drive_pth_file_id = ""
            drive_index_file_id = ""
            ensure_local_model_fn: Callable[[], tuple[Path, Path]] | None = None

            if drive_token and hasattr(drive, "resolve_model_files"):
                try:
                    remote_models = drive.resolve_model_files(payload.model_name)
                except ColabDriveError:
                    remote_models = None
                if remote_models is not None:
                    drive_pth_file_id = str(remote_models["pth"]["id"])
                    drive_index_file_id = str(remote_models["index"]["id"])

                    def _dl_model_local() -> tuple[Path, Path]:
                        return drive.ensure_model_files(payload.model_name, model_subdir)

                    ensure_local_model_fn = _dl_model_local

            if not (drive_pth_file_id and drive_index_file_id):
                pth_path, index_path = drive.ensure_model_files(payload.model_name, model_subdir)

            # 4) Resolve version-keyed weight cache in Hakimi_Songs/cache/ (P2)
            cache_folder_id = ""
            cache_weights_file_id = ""
            cache_weights_sha256 = ""
            if self.cfg.cache_enabled and drive_token and hasattr(drive, "ensure_cache_folder"):
                try:
                    cache_folder_id = drive.ensure_cache_folder()
                    w_key = colab_remote_mod.compute_weight_cache_key("cover")
                    w_manifest = drive.resolve_cache_manifest(w_key, expected_kind="weights")
                    if w_manifest is not None:
                        cache_weights_file_id = str(w_manifest["archive_file_id"])
                        cache_weights_sha256 = str(w_manifest["sha256"])
                except Exception:
                    cache_folder_id = ""
                    cache_weights_file_id = ""
                    cache_weights_sha256 = ""

            # 5) Decide warm keep-alive eligibility (strictly L4/T4, bounded TTL, healthy CU)
            if warm_reused:
                want_keep_warm = self._can_keep_warm(
                    job_type="rvc_cover",
                    gpu=reused_gpu,
                    allow_warm_keepalive=allow_warm_keepalive,
                    cu_balance=cu_balance_hint,
                )
            else:
                want_keep_warm = any(
                    self._can_keep_warm(
                        job_type="rvc_cover",
                        gpu=g,
                        allow_warm_keepalive=allow_warm_keepalive,
                        cu_balance=cu_balance_hint,
                    )
                    for g in record.gpu_candidates
                )

            # 6) Run Colab GPU cover pipeline
            gpu_spec = ",".join(record.gpu_candidates)
            runner_kwargs: dict[str, Any] = {
                "session": record.session_name,
                "gpu_spec": gpu_spec,
                "source_path": source_path,
                "pth_path": pth_path,
                "index_path": index_path,
                "cover_dir": cover_dir,
                "model_name": payload.model_name,
                "pitch": payload.pitch,
                "index_rate": payload.index_rate,
                "protect": payload.protect,
                "volume_envelope": payload.volume_envelope,
                "start_sec": payload.start_sec,
                "duration_sec": payload.duration_sec,
                "chorus_anchor": payload.chorus_anchor,
                "fallback_start_sec": payload.fallback_start_sec,
                "ensure_local_source_fn": ensure_local_source_fn,
                "ensure_local_model_fn": ensure_local_model_fn,
                "drive_access_token": drive_token,
                "drive_source_file_id": drive_source_file_id,
                "drive_source_ext": drive_source_ext,
                "drive_pth_file_id": drive_pth_file_id,
                "drive_index_file_id": drive_index_file_id,
                "drive_songs_folder_id": tree.songs if drive_token else "",
                "cover_prefix": slug,
                "song_title": payload.title[:80],
                "cache_folder_id": cache_folder_id,
                "cache_weights_file_id": cache_weights_file_id,
                "cache_weights_sha256": cache_weights_sha256,
                "cache_venv_enabled": self.cfg.cache_venv_enabled,
                "cache_writeback": self.cfg.cache_writeback,
                "reuse_session": warm_reused,
                "reused_gpu": reused_gpu,
                "reused_setup_ready": reused_setup_ready,
                "keep_session_on_success": want_keep_warm,
            }
            pipe_res = self._cover_runner(
                **_filter_runner_kwargs(self._cover_runner, runner_kwargs)
            )
            assigned_gpu = str(
                pipe_res.get("assigned_gpu") or reused_gpu or record.gpu_candidates[0]
            )

            # 7) Park warm session ONLY if strictly eligible; otherwise guarantee termination
            if pipe_res.get("session_kept"):
                if not record.cancelled and self._can_keep_warm(
                    job_type="rvc_cover",
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

            # 8) Assemble result from direct Drive outputs or fallback local upload
            meta = dict(pipe_res.get("meta") or {})
            drive_outputs = pipe_res.get("drive_outputs")
            chorus_out = drive_outputs.get("chorus") if isinstance(drive_outputs, dict) else None
            full_out = drive_outputs.get("full") if isinstance(drive_outputs, dict) else None
            if (
                isinstance(chorus_out, dict)
                and chorus_out.get("file_id")
                and chorus_out.get("sha256")
                and int(chorus_out.get("size") or 0) > 0
            ):
                digest = str(chorus_out["sha256"])
                byte_size = int(chorus_out["size"])
                duration_ms = int(
                    chorus_out.get("duration_ms")
                    or round(float(meta.get("chorus_duration_sec", payload.duration_sec)) * 1000.0)
                )
                drive_chorus_id = str(chorus_out["file_id"])
                drive_full_id = str((full_out or {}).get("file_id") or "")
            else:
                chorus_mp3 = Path(pipe_res["chorus_mp3"])
                full_mp3 = Path(pipe_res["full_mp3"])
                chorus_bytes = chorus_mp3.read_bytes()
                digest = _sha256_bytes(chorus_bytes)
                byte_size = len(chorus_bytes)
                duration_ms = _probe_audio_duration_ms(
                    chorus_mp3, float(meta.get("chorus_duration_sec", payload.duration_sec))
                )
                chorus_up = drive.upload_file(
                    "songs",
                    chorus_mp3,
                    drive_name=f"{slug}_{digest[:8]}_chorus.mp3",
                    mime_type="audio/mpeg",
                    replace_existing=True,
                    app_properties={"song_title": payload.title[:80], "kind": "chorus"},
                )
                drive_chorus_id = str(chorus_up["file_id"])
                drive_full_id = ""
                if full_mp3.is_file():
                    full_up = drive.upload_file(
                        "songs",
                        full_mp3,
                        drive_name=f"{slug}_{digest[:8]}_full.mp3",
                        mime_type="audio/mpeg",
                        replace_existing=True,
                        app_properties={"song_title": payload.title[:80], "kind": "full"},
                    )
                    drive_full_id = str(full_up["file_id"])

            raw_accel = pipe_res.get("acceleration")
            accel = dict(raw_accel) if isinstance(raw_accel, dict) else {}
            accel.setdefault("drive_direct", bool(drive_outputs))
            accel["drive_direct_fallback"] = bool(
                accel.get("drive_direct_fallback") or token_fallback
            )
            accel.setdefault("warm_reused", warm_reused)
            accel.setdefault("weight_cache_hit", None)
            accel.setdefault("weight_cache_written", False)
            accel.setdefault("venv_cache_hit", None)
            accel.setdefault("venv_cache_written", False)

            result = {
                "title": payload.title,
                "artist": payload.artist,
                "tags": payload.tags,
                "digest": digest,
                "byte_size": byte_size,
                "duration_ms": duration_ms,
                "drive_chorus_id": drive_chorus_id,
                "drive_full_id": drive_full_id,
                "chorus_start_sec": float(meta.get("chorus_start_sec", 0.0)),
                "chorus_start_reason": str(meta.get("chorus_start_reason", "")),
                "acceleration": accel,
            }
            return result, assigned_gpu

    def _run_train_sync(
        self,
        record: WorkerJobRecord,
        payload: RvcTrainPayload,
        drive_songs_folder_id: str,
    ) -> tuple[dict[str, Any], str]:
        # Drain any warm cover sessions before allocating a heavy training GPU.
        idle_warm = self.warm_pool.pop_all()
        if idle_warm:
            self._stop_sessions_sync(idle_warm)

        drive = self.get_drive(drive_songs_folder_id)
        tree = drive.ensure_folder_tree()
        tar_path = self.cfg.dataset_dir / "dataset.tar.gz"
        if payload.force_dataset or not tar_path.is_file():
            if self._dataset_builder is colab_rvc_mod.build_dataset:
                self._dataset_builder(
                    dataset_dir=self.cfg.dataset_dir,
                    force=payload.force_dataset,
                    extra_dry_dir=colab_rvc_mod.EXTRA_DRY_DIR,
                    extra_songs_dir=colab_rvc_mod.EXTRA_SONGS_DIR,
                )
            else:
                self._dataset_builder(dataset_dir=self.cfg.dataset_dir, force=payload.force_dataset)

        drive_token = ""
        token_fallback = False
        if self.cfg.drive_direct_enabled and _runner_accepts_direct_drive(self._train_runner):
            drive_token = self._issue_short_lived_drive_token(drive)
            if not drive_token:
                token_fallback = True

        cache_folder_id = ""
        cache_weights_file_id = ""
        cache_weights_sha256 = ""
        if self.cfg.cache_enabled and drive_token and hasattr(drive, "ensure_cache_folder"):
            try:
                cache_folder_id = drive.ensure_cache_folder()
                w_key = colab_remote_mod.compute_weight_cache_key("all")
                w_manifest = drive.resolve_cache_manifest(w_key, expected_kind="weights")
                if w_manifest is not None:
                    cache_weights_file_id = str(w_manifest["archive_file_id"])
                    cache_weights_sha256 = str(w_manifest["sha256"])
            except Exception:
                cache_folder_id = ""
                cache_weights_file_id = ""
                cache_weights_sha256 = ""

        model_subdir = self.cfg.model_dir / payload.model_name
        train_kwargs: dict[str, Any] = {
            "session": record.session_name,
            "gpu_spec": ",".join(record.gpu_candidates),
            "dataset_tar": tar_path,
            "model_name": payload.model_name,
            "epochs": payload.epochs,
            "save_every": payload.save_every,
            "batch_size": payload.batch_size,
            "sample_rate": payload.sample_rate,
            "model_dir": model_subdir,
            "drive_access_token": drive_token,
            "drive_models_folder_id": tree.models if drive_token else "",
            "cache_folder_id": cache_folder_id,
            "cache_weights_file_id": cache_weights_file_id,
            "cache_weights_sha256": cache_weights_sha256,
            "cache_venv_enabled": self.cfg.cache_venv_enabled,
            "cache_writeback": self.cfg.cache_writeback,
        }
        pipe_res = self._train_runner(**_filter_runner_kwargs(self._train_runner, train_kwargs))
        drive_outputs = pipe_res.get("drive_outputs")
        pth_out = drive_outputs.get("pth") if isinstance(drive_outputs, dict) else None
        idx_out = drive_outputs.get("index") if isinstance(drive_outputs, dict) else None
        if (
            isinstance(pth_out, dict)
            and isinstance(idx_out, dict)
            and pth_out.get("file_id")
            and idx_out.get("file_id")
        ):
            drive_pth_id = str(pth_out["file_id"])
            drive_index_id = str(idx_out["file_id"])
            pth_sha256 = str(pth_out.get("sha256") or "")
            index_sha256 = str(idx_out.get("sha256") or "")
        else:
            pth_path = Path(pipe_res["pth_path"])
            idx_path = Path(pipe_res["index_path"])
            pth_up = drive.upload_file(
                "models", pth_path, drive_name=f"{payload.model_name}.pth", replace_existing=True
            )
            idx_up = drive.upload_file(
                "models", idx_path, drive_name=f"{payload.model_name}.index", replace_existing=True
            )
            drive_pth_id = str(pth_up["file_id"])
            drive_index_id = str(idx_up["file_id"])
            pth_sha256 = _sha256_bytes(pth_path.read_bytes())
            index_sha256 = _sha256_bytes(idx_path.read_bytes())

        raw_accel = pipe_res.get("acceleration")
        accel = dict(raw_accel) if isinstance(raw_accel, dict) else {}
        accel.setdefault("drive_direct", bool(drive_outputs))
        accel["drive_direct_fallback"] = bool(accel.get("drive_direct_fallback") or token_fallback)
        accel["warm_reused"] = False
        accel.setdefault("weight_cache_hit", None)
        accel.setdefault("weight_cache_written", False)
        accel.setdefault("venv_cache_hit", None)
        accel.setdefault("venv_cache_written", False)

        result = {
            "model_name": payload.model_name,
            "drive_pth_id": drive_pth_id,
            "drive_index_id": drive_index_id,
            "pth_sha256": pth_sha256,
            "index_sha256": index_sha256,
            "acceleration": accel,
        }
        return result, str(pipe_res.get("assigned_gpu") or record.gpu_candidates[0])

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
            record.status = "cancelled"
            record.error_code = "cancelled"
            record.finished_at = self._clock()
            task = self.tasks.get(job_id)
            if self.running_job_id == job_id:
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
                    record.status = "cancelled"
                    record.error_code = "emergency_terminated"
                    record.finished_at = self._clock()
                    cancelled_jobs += 1
                    extra_sessions.append(record.session_name)
                    task = self.tasks.get(job_id)
                    if task is not None and not task.done():
                        task.cancel()
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
    drive_factory: Callable[[str], Any] | None = None,
    usage_fn: Callable[[], dict[str, Any]] | None = None,
    stop_all_fn: Callable[[list[str] | None], dict[str, Any]] | None = None,
    stop_session_fn: Callable[[str], None] | None = None,
    probe_session_fn: Callable[[str], dict[str, Any]] | None = None,
    cover_runner: Callable[..., dict[str, Any]] | None = None,
    train_runner: Callable[..., dict[str, Any]] | None = None,
    clean_search_fn: Callable[..., Path] | None = None,
    dataset_builder: Callable[..., dict[str, Any]] | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    state = WorkerState(
        config,
        drive_factory=drive_factory,
        usage_fn=usage_fn,
        stop_all_fn=stop_all_fn,
        stop_session_fn=stop_session_fn,
        probe_session_fn=probe_session_fn,
        cover_runner=cover_runner,
        train_runner=train_runner,
        clean_search_fn=clean_search_fn,
        dataset_builder=dataset_builder,
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

    @app.get("/v1/drive/inbox")
    async def v1_list_inbox(request: Request) -> dict[str, Any]:
        _auth(request)
        try:
            drive = await asyncio.to_thread(state.get_drive, "")
            items = await asyncio.to_thread(drive.list_inbox)
            return {"items": items}
        except ColabDriveError as exc:
            status = 503 if "scope" in exc.code or "cred" in exc.code else 400
            raise HTTPException(status, exc.code) from None

    @app.post("/v1/drive/inbox")
    async def v1_upload_inbox(request: Request) -> dict[str, Any]:
        _auth(request)
        raw_len = request.headers.get("content-length")
        if raw_len is not None and raw_len.strip() != "":
            try:
                declared = int(raw_len.strip())
            except ValueError:
                raise HTTPException(400, "invalid_content_length") from None
            if declared < 0:
                raise HTTPException(400, "invalid_content_length")
            if declared > MAX_INBOX_UPLOAD_BYTES:
                raise HTTPException(413, "payload_too_large")
            if declared == 0:
                raise HTTPException(400, "colab_drive_invalid_file")

        raw_name = unquote(request.headers.get("x-drive-filename", ""))
        mime = request.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
        try:
            clean_name = sanitize_drive_filename(raw_name)
        except ColabDriveError as exc:
            status = 503 if "scope" in exc.code or "cred" in exc.code else 400
            raise HTTPException(status, exc.code) from None

        tmp_path: Path | None = None
        try:
            tmp_file = tempfile.NamedTemporaryFile(prefix="hakimi_inbox_", delete=False)
            tmp_path = Path(tmp_file.name)
            total = 0
            with tmp_file:
                async for chunk in request.stream():
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > MAX_INBOX_UPLOAD_BYTES:
                        raise HTTPException(413, "payload_too_large")
                    await asyncio.to_thread(tmp_file.write, chunk)
            if total == 0:
                raise HTTPException(400, "colab_drive_invalid_file")
            drive = await asyncio.to_thread(state.get_drive, "")
            res = await asyncio.to_thread(
                drive.upload_file,
                "inbox",
                tmp_path,
                drive_name=clean_name,
                mime_type=mime or "application/octet-stream",
            )
            return {"file_id": res["file_id"], "name": res["name"], "size": res["size"]}
        except ColabDriveError as exc:
            status = 503 if "scope" in exc.code or "cred" in exc.code else 400
            raise HTTPException(status, exc.code) from None
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

    @app.get("/v1/drive/songs/{file_id}")
    async def v1_download_song(request: Request, file_id: str) -> Response:
        _auth(request)
        if not VALID_DRIVE_ID_RE.fullmatch(file_id):
            raise HTTPException(400, "colab_drive_invalid_file")
        try:
            drive = await asyncio.to_thread(state.get_drive, "")
            data = await asyncio.to_thread(
                drive.download_bytes,
                file_id,
                allowed_roles=("songs",),
                max_bytes=MAX_SONG_UPLOAD_BYTES,
            )
            return Response(content=data, media_type="audio/mpeg")
        except ColabDriveError as exc:
            if exc.code == "drive_not_found":
                raise HTTPException(404, exc.code) from None
            if exc.code in {"drive_file_outside_scope", "drive_invalid_id", "drive_file_too_large"}:
                raise HTTPException(400, exc.code) from None
            raise HTTPException(503, exc.code) from None

    @app.post("/v1/drive/songs")
    async def v1_upload_song(request: Request) -> dict[str, Any]:
        _auth(request)
        raw_name = unquote(request.headers.get("x-drive-filename", ""))
        title = unquote(request.headers.get("x-song-title", ""))[:80]
        mime = request.headers.get("content-type", "audio/mpeg").split(";")[0].strip()
        raw = await _read_bounded_bytes(request, MAX_SONG_UPLOAD_BYTES)
        if not raw:
            raise HTTPException(400, "colab_drive_invalid_file")
        try:
            clean_name = sanitize_drive_filename(raw_name)
            drive = await asyncio.to_thread(state.get_drive, "")
            props = {"song_title": title} if title else None
            res = await asyncio.to_thread(
                drive.upload_bytes,
                "songs",
                clean_name,
                raw,
                mime_type=mime or "audio/mpeg",
                app_properties=props,
            )
            return {"file_id": res["file_id"], "name": res["name"]}
        except ColabDriveError as exc:
            status = 503 if "scope" in exc.code or "cred" in exc.code else 400
            raise HTTPException(status, exc.code) from None

    return app


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hakimi Colab GPU & Drive HTTP worker.")
    parser.add_argument("--host", default=os.environ.get("COLAB_WORKER_HOST", DEFAULT_HOST))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("COLAB_WORKER_PORT", DEFAULT_PORT))
    )
    parser.add_argument("--allow-private", action="store_true")
    parser.add_argument(
        "--drive-cred-path",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--songs-folder-id",
        default=os.environ.get("COLAB_DRIVE_SONGS_FOLDER_ID", ""),
    )
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
        drive_cred_path=args.drive_cred_path,
        drive_songs_folder_id=args.songs_folder_id.strip(),
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
