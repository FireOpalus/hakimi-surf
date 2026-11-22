"""RVC voice-conversion cover and training GPU job handlers for `colab_worker`."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import math
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import (
    EphemeralDriveError,
    JobHandler,
    JobValidationError,
    WorkerJobContext,
    register_job_handler,
    reject_bool_num,
    sanitize_filename,
    validate_drive_id,
)

DEFAULT_BACKING_GAIN_DB = -14.0
MIN_BACKING_GAIN_DB = -24.0
MAX_BACKING_GAIN_DB = -6.0


def _load_colab_rvc_module():
    if "colab_rvc" in sys.modules:
        return sys.modules["colab_rvc"]
    mod_path = Path(__file__).resolve().parents[1] / "colab_rvc.py"
    spec = importlib.util.spec_from_file_location("colab_rvc", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load sibling module {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["colab_rvc"] = mod
    spec.loader.exec_module(mod)
    return mod


colab_rvc_mod = _load_colab_rvc_module()


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
    index_rate: float = Field(default=0.15, ge=0.0, le=1.0, allow_inf_nan=False)
    protect: float = Field(default=0.42, ge=0.0, le=0.5, allow_inf_nan=False)
    volume_envelope: float = Field(default=0.12, ge=0.0, le=1.0, allow_inf_nan=False)
    backing_gain_db: float = Field(
        default=DEFAULT_BACKING_GAIN_DB,
        ge=MIN_BACKING_GAIN_DB,
        le=MAX_BACKING_GAIN_DB,
        allow_inf_nan=False,
    )
    skip_backing: bool = True
    skip_dereverb: bool = True

    @field_validator("title")
    @classmethod
    def _clean_title(cls, v: str) -> str:
        cleaned = " ".join(v.split()).strip()
        if not cleaned:
            raise ValueError("title cannot be blank")
        return cleaned

    @field_validator(
        "pitch",
        "duration_sec",
        "fallback_start_sec",
        "index_rate",
        "protect",
        "volume_envelope",
        "backing_gain_db",
        mode="before",
    )
    @classmethod
    def _check_numeric_not_bool(cls, v: Any, info: Any) -> Any:
        return reject_bool_num(v, name=str(info.field_name))

    @field_validator("skip_backing", "skip_dereverb", mode="before")
    @classmethod
    def _check_bool_flags(cls, v: Any, info: Any) -> bool:
        if not isinstance(v, bool):
            raise ValueError(f"{info.field_name} must be a boolean")
        return v

    @field_validator("inbox_file_id")
    @classmethod
    def _check_inbox_id(cls, v: str) -> str:
        return validate_drive_id(v, allow_empty=True)

    @field_validator("inbox_filename")
    @classmethod
    def _check_inbox_filename(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            return ""
        return sanitize_filename(cleaned)

    @field_validator("start_sec")
    @classmethod
    def _check_start_sec(cls, v: str | float | int) -> str:
        if isinstance(v, bool):
            raise ValueError("invalid start_sec")
        if isinstance(v, (int, float)):
            val = float(v)
            if not math.isfinite(val) or not (0.0 <= val <= 600.0):
                raise ValueError("start_sec out of range")
            return f"{val:.2f}"
        raw = str(v).strip()
        if not raw or raw.lower() == "auto":
            return "auto"
        val = float(raw)
        if not math.isfinite(val) or not (0.0 <= val <= 600.0):
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

    @field_validator("epochs", "save_every", "batch_size", "sample_rate", mode="before")
    @classmethod
    def _check_int_not_bool(cls, v: Any, info: Any) -> Any:
        return reject_bool_num(v, name=str(info.field_name))


class RvcCoverHandler(JobHandler):
    """Colab GPU handler for RVC cover generation with 3-tier acceleration."""

    job_type = "rvc_cover"
    payload_model = RvcCoverPayload
    allow_warm_keepalive = True
    allowed_error_codes = frozenset({"source_not_found", "model_not_found"})

    def validate_payload(self, payload: dict[str, Any]) -> RvcCoverPayload:
        if not isinstance(payload, dict):
            raise JobValidationError("colab_invalid_job", "payload must be an object")
        parsed = RvcCoverPayload.model_validate(payload)
        if not parsed.inbox_file_id and not parsed.inbox_filename and not parsed.auto_search:
            raise JobValidationError("song_source_required")
        return parsed

    def run_sync(
        self,
        ctx: WorkerJobContext,
        parsed_payload: RvcCoverPayload,
    ) -> tuple[dict[str, Any], str]:
        payload = parsed_payload
        drive_ctx, drive = ctx.build_ephemeral_drive(required_roles=("songs",))
        songs_folder_id = drive.folder_for("songs")
        slug = colab_rvc_mod._slugify(payload.title)

        cover_runner = ctx.overrides.get("cover_runner") or colab_rvc_mod.run_cover_pipeline
        clean_search_fn = (
            ctx.overrides.get("clean_search_fn") or colab_rvc_mod.search_and_download_clean_source
        )

        # 0) Check warm session pool (Tier 2)
        warm_claim = ctx.claim_warm_session()
        prior_warm = warm_claim.entry
        warm_reused = warm_claim.reused
        reused_gpu = warm_claim.gpu
        reused_setup_ready = warm_claim.setup_ready

        # 1) Use per-dispatch ephemeral Drive access token in RAM (Tier 1)
        drive_token = ""
        token_fallback = False
        direct_supported = _runner_accepts_direct_drive(cover_runner)
        if ctx.config.drive_direct_enabled and direct_supported:
            drive_token = drive_ctx.access_token
            if not drive_token:
                token_fallback = True

        with tempfile.TemporaryDirectory(prefix=f"hakimi_job_{ctx.job_id[:8]}_") as tmp:
            tmp_dir = Path(tmp)
            source_dir = tmp_dir / "source"
            cover_dir = tmp_dir / "cover"
            tmp_models_dir = tmp_dir / "models"
            source_dir.mkdir(parents=True, exist_ok=True)
            cover_dir.mkdir(parents=True, exist_ok=True)

            # 2) Resolve input audio source from dispatch_context inbox or search
            source_path: Path | None = None
            drive_source_file_id = ""
            drive_source_ext = ".m4a"
            ensure_local_source_fn: Callable[[], Path] | None = None

            ctx_inbox_id = drive_ctx.files.inbox_file_id
            if payload.inbox_file_id and ctx_inbox_id and payload.inbox_file_id != ctx_inbox_id:
                raise EphemeralDriveError(
                    "drive_file_outside_scope",
                    "Payload inbox_file_id does not match authorized dispatch_context",
                )
            resolved_inbox_id = ctx_inbox_id or payload.inbox_file_id
            resolved_inbox_name = drive_ctx.files.inbox_name or payload.inbox_filename

            if resolved_inbox_id or payload.inbox_filename:
                if not resolved_inbox_id:
                    raise EphemeralDriveError(
                        "source_not_found",
                        "Inbox file ID was not resolved in dispatch_context",
                    )
                inbox_meta = drive.verify_file_in_folders(
                    resolved_inbox_id, allowed_roles=("inbox",)
                )
                effective_inbox_name = (
                    resolved_inbox_name or str(inbox_meta.get("name") or "") or "source.m4a"
                )
                drive_source_ext = Path(effective_inbox_name).suffix or ".m4a"

                def _dl_inbox_local() -> Path:
                    return drive.download_file(
                        resolved_inbox_id,
                        source_dir / f"inbox_source{drive_source_ext}",
                        allowed_roles=("inbox",),
                    )

                if drive_token:
                    drive_source_file_id = resolved_inbox_id
                    ensure_local_source_fn = _dl_inbox_local
                else:
                    source_path = _dl_inbox_local()
            else:
                query = payload.query.strip() or payload.title
                source_path = clean_search_fn(query, source_dir, artist=payload.artist)

            if ctx.is_cancelled():
                raise RuntimeError("cancelled")

            # 3) Resolve RVC model weights from dispatch_context or local fallback
            model_subdir = ctx.config.model_dir / payload.model_name
            pth_path: Path | None = None
            index_path: Path | None = None
            drive_pth_file_id = ""
            drive_index_file_id = ""
            ensure_local_model_fn: Callable[[], tuple[Path, Path]] | None = None

            ctx_pth_id = drive_ctx.files.pth_file_id
            ctx_index_id = drive_ctx.files.index_file_id
            if ctx_pth_id and ctx_index_id:
                drive.verify_file_in_folders(ctx_pth_id, allowed_roles=("models",))
                drive.verify_file_in_folders(ctx_index_id, allowed_roles=("models",))

                def _dl_model_local() -> tuple[Path, Path]:
                    pth_dst = drive.download_file(
                        ctx_pth_id,
                        tmp_models_dir / f"{payload.model_name}.pth",
                        allowed_roles=("models",),
                    )
                    idx_dst = drive.download_file(
                        ctx_index_id,
                        tmp_models_dir / f"{payload.model_name}.index",
                        allowed_roles=("models",),
                    )
                    return pth_dst, idx_dst

                if drive_token:
                    drive_pth_file_id = ctx_pth_id
                    drive_index_file_id = ctx_index_id
                    ensure_local_model_fn = _dl_model_local
                else:
                    pth_path, index_path = _dl_model_local()
            else:
                local_pth = model_subdir / f"{payload.model_name}.pth"
                local_idx = model_subdir / f"{payload.model_name}.index"
                if local_pth.is_file() and local_idx.is_file():
                    pth_path, index_path = local_pth, local_idx
                else:
                    raise EphemeralDriveError(
                        "model_not_found",
                        f"Model files not found for {payload.model_name}",
                    )

            # 4) Version-keyed weight cache metadata from dispatch_context (Tier 3)
            cache_folder_id = (
                drive_ctx.folders.cache if (ctx.config.cache_enabled and drive_token) else ""
            )
            cache_weights_file_id = (
                drive_ctx.files.cache_weights_file_id
                if (ctx.config.cache_enabled and drive_token and cache_folder_id)
                else ""
            )
            cache_weights_sha256 = (
                drive_ctx.files.cache_weights_sha256 if cache_weights_file_id else ""
            )
            if cache_weights_file_id:
                try:
                    drive.verify_file_in_folders(cache_weights_file_id, allowed_roles=("cache",))
                except EphemeralDriveError:
                    cache_weights_file_id = ""
                    cache_weights_sha256 = ""

            # 5) Decide warm keep-alive eligibility (strictly L4/T4, bounded TTL, healthy CU)
            if warm_reused:
                want_keep_warm = ctx.can_keep_warm(reused_gpu)
            else:
                want_keep_warm = any(ctx.can_keep_warm(g) for g in ctx.gpu_candidates)

            # 6) Run Colab GPU cover pipeline
            gpu_spec = ",".join(ctx.gpu_candidates)
            runner_kwargs: dict[str, Any] = {
                "session": ctx.session_name,
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
                "backing_gain_db": payload.backing_gain_db,
                "skip_backing": payload.skip_backing,
                "skip_dereverb": payload.skip_dereverb,
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
                # colab_rvc.run_cover_pipeline parameter name for direct songs/ output folder
                "drive_songs_folder_id": songs_folder_id if drive_token else "",
                "cover_prefix": slug,
                "song_title": payload.title[:80],
                "cache_folder_id": cache_folder_id,
                "cache_weights_file_id": cache_weights_file_id,
                "cache_weights_sha256": cache_weights_sha256,
                "cache_venv_enabled": ctx.config.cache_venv_enabled,
                "cache_writeback": ctx.config.cache_writeback,
                "reuse_session": warm_reused,
                "reused_gpu": reused_gpu,
                "reused_setup_ready": reused_setup_ready,
                "keep_session_on_success": want_keep_warm,
            }
            pipe_res = cover_runner(**_filter_runner_kwargs(cover_runner, runner_kwargs))
            assigned_gpu = str(pipe_res.get("assigned_gpu") or reused_gpu or ctx.gpu_candidates[0])

            # 7) Park warm session ONLY if strictly eligible; otherwise guarantee termination
            if pipe_res.get("session_kept"):
                ctx.park_or_stop_warm_session(assigned_gpu, prior_warm=prior_warm)

            # 8) Assemble result from direct Drive outputs or fallback ephemeral upload
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


class RvcTrainHandler(JobHandler):
    """Colab GPU handler for RVC v2 voice model training."""

    job_type = "rvc_train"
    payload_model = RvcTrainPayload
    allow_warm_keepalive = False
    allowed_error_codes = frozenset({"model_not_found"})

    def run_sync(
        self,
        ctx: WorkerJobContext,
        parsed_payload: RvcTrainPayload,
    ) -> tuple[dict[str, Any], str]:
        payload = parsed_payload
        drive_ctx, drive = ctx.build_ephemeral_drive(required_roles=("models",))
        models_folder_id = drive.folder_for("models")

        dataset_builder = ctx.overrides.get("dataset_builder") or colab_rvc_mod.build_dataset
        train_runner = ctx.overrides.get("train_runner") or colab_rvc_mod.run_train_pipeline

        # Drain any warm cover sessions before allocating a heavy training GPU.
        ctx.drain_warm_sessions()

        tar_path = ctx.config.dataset_dir / "dataset.tar.gz"
        if payload.force_dataset or not tar_path.is_file():
            if dataset_builder is colab_rvc_mod.build_dataset:
                dataset_builder(
                    dataset_dir=ctx.config.dataset_dir,
                    force=payload.force_dataset,
                    extra_dry_dir=colab_rvc_mod.EXTRA_DRY_DIR,
                    extra_songs_dir=colab_rvc_mod.EXTRA_SONGS_DIR,
                )
            else:
                dataset_builder(dataset_dir=ctx.config.dataset_dir, force=payload.force_dataset)

        drive_token = ""
        token_fallback = False
        if ctx.config.drive_direct_enabled and _runner_accepts_direct_drive(train_runner):
            drive_token = drive_ctx.access_token
            if not drive_token:
                token_fallback = True

        cache_folder_id = (
            drive_ctx.folders.cache if (ctx.config.cache_enabled and drive_token) else ""
        )
        cache_weights_file_id = (
            drive_ctx.files.cache_weights_file_id
            if (ctx.config.cache_enabled and drive_token and cache_folder_id)
            else ""
        )
        cache_weights_sha256 = drive_ctx.files.cache_weights_sha256 if cache_weights_file_id else ""
        if cache_weights_file_id:
            try:
                drive.verify_file_in_folders(cache_weights_file_id, allowed_roles=("cache",))
            except EphemeralDriveError:
                cache_weights_file_id = ""
                cache_weights_sha256 = ""

        model_subdir = ctx.config.model_dir / payload.model_name
        train_kwargs: dict[str, Any] = {
            "session": ctx.session_name,
            "gpu_spec": ",".join(ctx.gpu_candidates),
            "dataset_tar": tar_path,
            "model_name": payload.model_name,
            "epochs": payload.epochs,
            "save_every": payload.save_every,
            "batch_size": payload.batch_size,
            "sample_rate": payload.sample_rate,
            "model_dir": model_subdir,
            "drive_access_token": drive_token,
            "drive_models_folder_id": models_folder_id if drive_token else "",
            "cache_folder_id": cache_folder_id,
            "cache_weights_file_id": cache_weights_file_id,
            "cache_weights_sha256": cache_weights_sha256,
            "cache_venv_enabled": ctx.config.cache_venv_enabled,
            "cache_writeback": ctx.config.cache_writeback,
        }
        pipe_res = train_runner(**_filter_runner_kwargs(train_runner, train_kwargs))
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
        return result, str(pipe_res.get("assigned_gpu") or ctx.gpu_candidates[0])


register_job_handler(RvcCoverHandler(), replace=True)
register_job_handler(RvcTrainHandler(), replace=True)

__all__ = [
    "DEFAULT_BACKING_GAIN_DB",
    "MIN_BACKING_GAIN_DB",
    "MAX_BACKING_GAIN_DB",
    "RvcCoverPayload",
    "RvcTrainPayload",
    "RvcCoverHandler",
    "RvcTrainHandler",
]
