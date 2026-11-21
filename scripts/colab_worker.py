"""Out-of-process host worker for Google Colab GPU jobs and Hakimi_Songs/ Drive proxy.

Runs strictly outside the `hakimi-bot` container on loopback HTTP, authenticated by
a constant-time Bearer token (`hmac.compare_digest`). Decouples Colab GPU auth
(`secrets/colab-adc.json`) from Drive OAuth (`secrets/colab-drive-oauth.json` or
read-only `secrets/roaming-oauth.json`) and never logs or returns raw credentials.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import importlib.util
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
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
MAX_INBOX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_SONG_UPLOAD_BYTES = 30 * 1024 * 1024
MAX_JOB_BODY_BYTES = 64 * 1024
MAX_JOB_HISTORY = 100

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
colab_rvc_mod = _load_sibling_module("colab_rvc", "colab_rvc.py")

ColabDriveClient = colab_drive_mod.ColabDriveClient
ColabDriveError = colab_drive_mod.ColabDriveError
resolve_drive_credentials_path = colab_drive_mod.resolve_drive_credentials_path
sanitize_drive_filename = colab_drive_mod.sanitize_drive_filename


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
    index_rate: float = Field(default=0.55, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float = Field(default=0.33, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float = Field(default=0.85, ge=0.0, le=1.0, allow_inf_nan=False)

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
    epochs: int = Field(default=250, ge=10, le=1000)
    save_every: int = Field(default=50, ge=5, le=500)
    batch_size: int = Field(default=8, ge=1, le=32)
    sample_rate: Literal[32000, 40000, 48000] = 40000
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
    payload: dict[str, Any]

    @field_validator("drive_songs_folder_id")
    @classmethod
    def _check_folder_id(cls, v: str) -> str:
        cleaned = v.strip()
        if cleaned == "root" or (cleaned and not VALID_FOLDER_ID_RE.fullmatch(cleaned)):
            raise ValueError("invalid drive_songs_folder_id")
        return cleaned


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


def _scrub_secret(text: str, token: str) -> str:
    cleaned = " ".join(str(text or "").split())
    if token and len(token) >= 6:
        cleaned = cleaned.replace(token, "[REDACTED]")
    return cleaned[:240]


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
        cover_runner: Callable[..., dict[str, Any]] | None = None,
        train_runner: Callable[..., dict[str, Any]] | None = None,
        clean_search_fn: Callable[..., Path] | None = None,
        dataset_builder: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self.cfg = config
        self._drive_factory = drive_factory or self._default_drive_factory
        self._usage_fn = usage_fn or colab_rvc_mod.parse_colab_usage
        self._stop_all_fn = stop_all_fn or colab_rvc_mod.stop_all_sessions
        self._cover_runner = cover_runner or colab_rvc_mod.run_cover_pipeline
        self._train_runner = train_runner or colab_rvc_mod.run_train_pipeline
        self._clean_search_fn = clean_search_fn or colab_rvc_mod.search_and_download_clean_source
        self._dataset_builder = dataset_builder or colab_rvc_mod.build_dataset
        self.lock = asyncio.Lock()
        self.jobs: dict[str, WorkerJobRecord] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.running_job_id: str | None = None
        self._cached_folders: dict[str, str] | None = None

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

    async def status_payload(self) -> dict[str, Any]:
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

        err = usage.get("error") or drive_err
        return {
            "reachable": bool(usage.get("reachable", True)),
            "compute_units": usage.get("compute_units"),
            "usage_rate": usage.get("usage_rate"),
            "active_sessions": int(usage.get("active_sessions") or 0),
            "running_job_id": self.running_job_id,
            "drive_folders": drive_folders,
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
                self._execute_job(record, parsed, req.timeout_seconds, req.drive_songs_folder_id)
            )
            self.tasks[req.job_id] = task
        return {"job_id": req.job_id, "status": "running"}

    async def _execute_job(
        self,
        record: WorkerJobRecord,
        parsed: RvcCoverPayload | RvcTrainPayload,
        timeout_seconds: int,
        drive_songs_folder_id: str,
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
                        self._run_cover_sync, record, parsed, drive_songs_folder_id
                    )
                else:
                    assert isinstance(parsed, RvcTrainPayload)
                    res, assigned_gpu = await asyncio.to_thread(
                        self._run_train_sync, record, parsed, drive_songs_folder_id
                    )
            if record.cancelled:
                record.status = "cancelled"
                record.error_code = record.error_code or "cancelled"
            else:
                record.status = "succeeded"
                record.result = res
                record.assigned_gpu = assigned_gpu
        except TimeoutError:
            record.status = "failed"
            record.error_code = "job_timeout"
            record.error = "Job timed out"
            record.result = {"error_code": "job_timeout"}
            await asyncio.to_thread(colab_rvc_mod.stop_session, record.session_name)
        except asyncio.CancelledError:
            record.status = "cancelled"
            record.error_code = "cancelled"
            record.result = {"error_code": "cancelled"}
            await asyncio.to_thread(colab_rvc_mod.stop_session, record.session_name)
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
            except Exception:
                pass
            record.finished_at = time.time()
            async with self.lock:
                if self.running_job_id == record.job_id:
                    self.running_job_id = None
                self.tasks.pop(record.job_id, None)

    def _run_cover_sync(
        self,
        record: WorkerJobRecord,
        payload: RvcCoverPayload,
        drive_songs_folder_id: str,
    ) -> tuple[dict[str, Any], str]:
        drive = self.get_drive(drive_songs_folder_id)
        with tempfile.TemporaryDirectory(prefix=f"hakimi_job_{record.job_id[:8]}_") as tmp:
            tmp_dir = Path(tmp)
            source_dir = tmp_dir / "source"
            cover_dir = tmp_dir / "cover"
            source_dir.mkdir(parents=True, exist_ok=True)
            cover_dir.mkdir(parents=True, exist_ok=True)

            # 1) Resolve input audio source from inbox or clean auto-search
            if payload.inbox_file_id:
                meta = drive.resolve_inbox_file(file_id=payload.inbox_file_id)
                ext = Path(meta.get("name") or "source.m4a").suffix or ".m4a"
                source_path = drive.download_file(
                    meta["id"], source_dir / f"inbox_source{ext}", allowed_roles=("inbox",)
                )
            elif payload.inbox_filename:
                meta = drive.resolve_inbox_file(filename=payload.inbox_filename)
                ext = Path(meta.get("name") or "source.m4a").suffix or ".m4a"
                source_path = drive.download_file(
                    meta["id"], source_dir / f"inbox_source{ext}", allowed_roles=("inbox",)
                )
            else:
                query = payload.query.strip() or payload.title
                source_path = self._clean_search_fn(query, source_dir, artist=payload.artist)

            if record.cancelled:
                raise RuntimeError("cancelled")

            # 2) Ensure RVC model weights locally or from Hakimi_Songs/models/
            model_subdir = self.cfg.model_dir / payload.model_name
            pth_path, index_path = drive.ensure_model_files(payload.model_name, model_subdir)

            # 3) Run Colab GPU cover pipeline (always stops session in finally)
            gpu_spec = ",".join(record.gpu_candidates)
            pipe_res = self._cover_runner(
                session=record.session_name,
                gpu_spec=gpu_spec,
                source_path=source_path,
                pth_path=pth_path,
                index_path=index_path,
                cover_dir=cover_dir,
                model_name=payload.model_name,
                pitch=payload.pitch,
                index_rate=payload.index_rate,
                protect=payload.protect,
                volume_envelope=payload.volume_envelope,
                start_sec=payload.start_sec,
                duration_sec=payload.duration_sec,
                chorus_anchor=payload.chorus_anchor,
                fallback_start_sec=payload.fallback_start_sec,
            )

            chorus_mp3 = Path(pipe_res["chorus_mp3"])
            full_mp3 = Path(pipe_res["full_mp3"])
            meta = dict(pipe_res.get("meta") or {})
            chorus_bytes = chorus_mp3.read_bytes()
            digest = _sha256_bytes(chorus_bytes)
            byte_size = len(chorus_bytes)
            duration_ms = _probe_audio_duration_ms(
                chorus_mp3, float(meta.get("chorus_duration_sec", payload.duration_sec))
            )

            slug = colab_rvc_mod._slugify(payload.title)
            chorus_up = drive.upload_file(
                "songs",
                chorus_mp3,
                drive_name=f"{slug}_{digest[:8]}_chorus.mp3",
                mime_type="audio/mpeg",
                replace_existing=True,
                app_properties={"song_title": payload.title[:80], "kind": "chorus"},
            )
            full_id = ""
            if full_mp3.is_file():
                full_up = drive.upload_file(
                    "songs",
                    full_mp3,
                    drive_name=f"{slug}_{digest[:8]}_full.mp3",
                    mime_type="audio/mpeg",
                    replace_existing=True,
                    app_properties={"song_title": payload.title[:80], "kind": "full"},
                )
                full_id = str(full_up["file_id"])

            result = {
                "title": payload.title,
                "artist": payload.artist,
                "tags": payload.tags,
                "digest": digest,
                "byte_size": byte_size,
                "duration_ms": duration_ms,
                "drive_chorus_id": str(chorus_up["file_id"]),
                "drive_full_id": full_id,
                "chorus_start_sec": float(meta.get("chorus_start_sec", 0.0)),
                "chorus_start_reason": str(meta.get("chorus_start_reason", "")),
            }
            return result, str(pipe_res.get("assigned_gpu") or record.gpu_candidates[0])

    def _run_train_sync(
        self,
        record: WorkerJobRecord,
        payload: RvcTrainPayload,
        drive_songs_folder_id: str,
    ) -> tuple[dict[str, Any], str]:
        drive = self.get_drive(drive_songs_folder_id)
        tar_path = self.cfg.dataset_dir / "dataset.tar.gz"
        if payload.force_dataset or not tar_path.is_file():
            self._dataset_builder(dataset_dir=self.cfg.dataset_dir, force=payload.force_dataset)

        model_subdir = self.cfg.model_dir / payload.model_name
        pipe_res = self._train_runner(
            session=record.session_name,
            gpu_spec=",".join(record.gpu_candidates),
            dataset_tar=tar_path,
            model_name=payload.model_name,
            epochs=payload.epochs,
            save_every=payload.save_every,
            batch_size=payload.batch_size,
            sample_rate=payload.sample_rate,
            model_dir=model_subdir,
        )
        pth_path = Path(pipe_res["pth_path"])
        idx_path = Path(pipe_res["index_path"])
        pth_up = drive.upload_file(
            "models", pth_path, drive_name=f"{payload.model_name}.pth", replace_existing=True
        )
        idx_up = drive.upload_file(
            "models", idx_path, drive_name=f"{payload.model_name}.index", replace_existing=True
        )
        result = {
            "model_name": payload.model_name,
            "drive_pth_id": str(pth_up["file_id"]),
            "drive_index_id": str(idx_up["file_id"]),
            "pth_sha256": _sha256_bytes(pth_path.read_bytes()),
            "index_sha256": _sha256_bytes(idx_path.read_bytes()),
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
            record.finished_at = time.time()
            task = self.tasks.get(job_id)
            if self.running_job_id == job_id:
                self.running_job_id = None
        if task is not None and not task.done():
            task.cancel()
        await asyncio.to_thread(colab_rvc_mod.stop_session, record.session_name)
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
                    record.finished_at = time.time()
                    cancelled_jobs += 1
                    extra_sessions.append(record.session_name)
                    task = self.tasks.get(job_id)
                    if task is not None and not task.done():
                        task.cancel()
            self.running_job_id = None
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
    cover_runner: Callable[..., dict[str, Any]] | None = None,
    train_runner: Callable[..., dict[str, Any]] | None = None,
    clean_search_fn: Callable[..., Path] | None = None,
    dataset_builder: Callable[..., dict[str, Any]] | None = None,
) -> FastAPI:
    state = WorkerState(
        config,
        drive_factory=drive_factory,
        usage_fn=usage_fn,
        stop_all_fn=stop_all_fn,
        cover_runner=cover_runner,
        train_runner=train_runner,
        clean_search_fn=clean_search_fn,
        dataset_builder=dataset_builder,
    )
    app = FastAPI(title="Hakimi Colab GPU Worker", docs_url=None, redoc_url=None, openapi_url=None)
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
    async def healthz() -> dict[str, bool]:
        return {"ok": True}

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
        raw_name = unquote(request.headers.get("x-drive-filename", ""))
        mime = request.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
        raw = await _read_bounded_bytes(request, MAX_INBOX_UPLOAD_BYTES)
        if not raw:
            raise HTTPException(400, "colab_drive_invalid_file")
        try:
            clean_name = sanitize_drive_filename(raw_name)
            drive = await asyncio.to_thread(state.get_drive, "")
            res = await asyncio.to_thread(
                drive.upload_bytes,
                "inbox",
                clean_name,
                raw,
                mime_type=mime or "application/octet-stream",
            )
            return {"file_id": res["file_id"], "name": res["name"], "size": res["size"]}
        except ColabDriveError as exc:
            status = 503 if "scope" in exc.code or "cred" in exc.code else 400
            raise HTTPException(status, exc.code) from None

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
    cfg = WorkerConfig(
        host=args.host,
        port=args.port,
        token=token,
        drive_cred_path=args.drive_cred_path,
        drive_songs_folder_id=args.songs_folder_id.strip(),
    )
    app = create_worker_app(cfg)
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="warning", access_log=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
