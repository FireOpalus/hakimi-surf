"""Loopback-isolated Google Colab GPU scheduler and Hakimi_Songs/ Drive proxy client."""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .config import Settings
from .security import Rejected, configured_secrets
from .store import Store

log = logging.getLogger("hakimi")

GpuType = Literal["H100", "A100", "G4", "L4", "T4"]
VALID_GPUS: tuple[GpuType, ...] = ("H100", "A100", "G4", "L4", "T4")
VALID_JOB_TYPES = frozenset({"rvc_cover", "rvc_train"})
MAX_QUEUED_JOBS = 20
MAX_INBOX_UPLOAD_BYTES = 25 * 1024 * 1024
MAX_SONG_UPLOAD_BYTES = 30 * 1024 * 1024
MAX_DRIVE_DOWNLOAD_BYTES = 30 * 1024 * 1024

_CONFIG_KEY = "colab_compute_config"
_VAULT_CFG_CTX = "admin:colab_compute_config"
_FOLDER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,128}$")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_JOB_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
_SAFE_FILENAME_RE = re.compile(r"^[^\x00-\x1f/\\]{1,160}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS colab_jobs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  requester TEXT NOT NULL DEFAULT 'owner',
  preferred_gpu TEXT NOT NULL DEFAULT 'L4',
  assigned_gpu TEXT,
  payload BLOB NOT NULL,
  result BLOB,
  error_code TEXT,
  error TEXT,
  cu_used REAL NOT NULL DEFAULT 0.0,
  created_at REAL NOT NULL,
  started_at REAL,
  finished_at REAL,
  updated_at REAL NOT NULL,
  day TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_colab_jobs_status ON colab_jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_colab_jobs_day ON colab_jobs(day, created_at);
"""


def _reject_bool_num(value: Any, *, name: str) -> Any:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric, not bool")
    return value


class ColabConfig(BaseModel):
    """Runtime-configurable Colab GPU scheduling and budget limits."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    preferred_gpu: GpuType = "L4"
    fallback_gpus: list[GpuType] = Field(default_factory=lambda: ["L4", "T4"], max_length=5)
    daily_job_limit: int = Field(default=5, ge=0, le=50)
    daily_cu_budget: float = Field(default=10.0, ge=0.0, le=200.0, allow_inf_nan=False)
    job_timeout_seconds: int = Field(default=1800, ge=120, le=7200)
    auto_terminate_orphan_minutes: int = Field(default=20, ge=5, le=120)
    drive_songs_folder_id: str = Field(default="", max_length=128)

    @field_validator("enabled")
    @classmethod
    def _check_enabled(cls, value: Any) -> bool:
        if not isinstance(value, bool):
            raise ValueError("enabled must be a boolean")
        return value

    @field_validator(
        "daily_job_limit",
        "job_timeout_seconds",
        "auto_terminate_orphan_minutes",
        mode="before",
    )
    @classmethod
    def _check_int_not_bool(cls, value: Any, info: Any) -> Any:
        return _reject_bool_num(value, name=str(info.field_name))

    @field_validator("daily_cu_budget", mode="before")
    @classmethod
    def _check_float_not_bool(cls, value: Any) -> Any:
        return _reject_bool_num(value, name="daily_cu_budget")

    @field_validator("fallback_gpus")
    @classmethod
    def _dedup_fallbacks(cls, value: list[GpuType]) -> list[GpuType]:
        out: list[GpuType] = []
        for gpu in value:
            if gpu not in out:
                out.append(gpu)
        return out

    @field_validator("drive_songs_folder_id", mode="before")
    @classmethod
    def _check_folder_id(cls, value: Any) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("drive_songs_folder_id must be a string")
        cleaned = value.strip()
        if not cleaned:
            return ""
        if cleaned == "root" or not _FOLDER_ID_RE.fullmatch(cleaned):
            raise ValueError("invalid drive_songs_folder_id")
        return cleaned


class _RvcCoverJobPayload(BaseModel):
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
    def _clean_title(cls, value: str) -> str:
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            raise ValueError("title cannot be empty")
        return cleaned

    @field_validator("pitch", mode="before")
    @classmethod
    def _check_pitch(cls, value: Any) -> Any:
        return _reject_bool_num(value, name="pitch")

    @field_validator("inbox_file_id")
    @classmethod
    def _check_inbox_id(cls, value: str) -> str:
        cleaned = value.strip()
        if cleaned and not _DRIVE_FILE_ID_RE.fullmatch(cleaned):
            raise ValueError("invalid inbox_file_id")
        return cleaned

    @field_validator("inbox_filename")
    @classmethod
    def _check_inbox_filename(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return ""
        if (
            ".." in cleaned
            or "/" in cleaned
            or "\\" in cleaned
            or not _SAFE_FILENAME_RE.fullmatch(cleaned)
        ):
            raise ValueError("invalid inbox_filename")
        return cleaned


class _RvcTrainJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str = Field(default="hakimi", pattern=r"^[a-z0-9_-]{1,32}$")
    epochs: int = Field(default=250, ge=10, le=1000)
    save_every: int = Field(default=50, ge=5, le=500)
    batch_size: int = Field(default=8, ge=1, le=32)
    sample_rate: Literal[32000, 40000, 48000] = 40000
    force_dataset: bool = False


def _sanitize_filename(filename: str) -> str:
    if not isinstance(filename, str):
        raise Rejected("colab_drive_invalid_file")
    cleaned = Path(filename.strip()).name.strip()
    if (
        not cleaned
        or cleaned in {".", ".."}
        or ".." in cleaned
        or "/" in filename
        or "\\" in filename
        or not _SAFE_FILENAME_RE.fullmatch(cleaned)
    ):
        raise Rejected("colab_drive_invalid_file")
    return cleaned


class ColabComputeHub:
    """Thin in-process coordinator communicating with `scripts/colab_worker.py` via HTTP."""

    def __init__(
        self,
        settings: Settings,
        store: Store,
        service: Any,
        *,
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.store = store
        self.service = service
        self._injected_http = http
        self._owned_http: httpx.AsyncClient | None = None
        self._clock = clock
        self._init_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self.initialized = False
        self._closed = False
        self._overridden = False
        self.config = self._default_config()
        self._hooks: dict[str, list[Callable[[dict[str, Any]], Awaitable[None]]]] = {}

        # Non-blocking cached operational summary for SystemModule.status()
        self._active_jobs = 0
        self._queued_jobs = 0
        self._last_cu_balance: float | None = None
        self._last_error: str | None = None
        self._worker_reachable = False
        self._last_status_poll_at = 0.0
        self._orphan_seen_since: float | None = None

    def _default_config(self) -> ColabConfig:
        folder_id = str(getattr(self.s, "colab_drive_songs_folder_id", "") or "").strip()
        if folder_id and _FOLDER_ID_RE.fullmatch(folder_id) and folder_id != "root":
            return ColabConfig(drive_songs_folder_id=folder_id)
        return ColabConfig()

    def _worker_url(self) -> str:
        return str(getattr(self.s, "colab_worker_url", "") or "").strip().rstrip("/")

    def _worker_token(self) -> str:
        raw = getattr(self.s, "colab_worker_token", "")
        if hasattr(raw, "get_secret_value"):
            return str(raw.get_secret_value() or "").strip()
        return str(raw or "").strip()

    def configured(self) -> bool:
        return bool(self._worker_url() and self._worker_token())

    def _local_day(self, ts: float | None = None) -> str:
        now = self._clock() if ts is None else ts
        return datetime.fromtimestamp(now, ZoneInfo(self.s.bot_timezone)).date().isoformat()

    def _scrub(self, text: Any) -> str:
        out = " ".join(str(text or "").split())
        secrets = list(configured_secrets(self.s))
        tok = self._worker_token()
        if tok:
            secrets.append(tok)
        for sec in sorted({s for s in secrets if s and len(s) >= 6}, key=len, reverse=True):
            out = out.replace(sec, "[REDACTED]")
        return out[:220]

    async def _http(self) -> httpx.AsyncClient:
        if self._injected_http is not None:
            return self._injected_http
        if self._owned_http is None or self._owned_http.is_closed:
            self._owned_http = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),
                trust_env=False,
                follow_redirects=False,
            )
        return self._owned_http

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout_sec: float = 8.0,
    ) -> httpx.Response:
        if not self.configured():
            raise Rejected("colab_unconfigured")
        client = await self._http()
        url = f"{self._worker_url()}{path}"
        req_headers = {"Authorization": f"Bearer {self._worker_token()}"}
        if headers:
            req_headers.update(headers)
        try:
            return await client.request(
                method,
                url,
                headers=req_headers,
                json=json_body,
                content=content,
                timeout=httpx.Timeout(timeout_sec),
            )
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            self._worker_reachable = False
            self._last_error = self._scrub(type(exc).__name__)
            raise Rejected("colab_worker_unavailable") from None

    async def initialize(self) -> None:
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.lock:
                await self.store.db.executescript(SCHEMA)
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS admin_config "
                    "(key TEXT PRIMARY KEY, payload BLOB NOT NULL)"
                )
                row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key=?", (_CONFIG_KEY,)
                )
            default_cfg = self._default_config()
            if row and row[0]:
                try:
                    raw = self.store.vault.open(_VAULT_CFG_CTX, row[0])
                    if isinstance(raw, dict):
                        merged = {**default_cfg.model_dump(mode="json"), **raw}
                        self.config = ColabConfig.model_validate(merged)
                        self._overridden = True
                    else:
                        self.config = default_cfg
                        self._overridden = False
                except Exception:
                    self.config = default_cfg
                    self._overridden = False
            else:
                self.config = default_cfg
                self._overridden = False
            await self._refresh_cached_counts()
            self.initialized = True

    async def _refresh_cached_counts(self) -> None:
        async with self.store.lock:
            row_active = await self.store.one(
                "SELECT COUNT(*) FROM colab_jobs WHERE status='running'"
            )
            row_queued = await self.store.one(
                "SELECT COUNT(*) FROM colab_jobs WHERE status='queued'"
            )
        self._active_jobs = int(row_active[0]) if row_active else 0
        self._queued_jobs = int(row_queued[0]) if row_queued else 0

    def _seal_blob(self, job_id: str, kind: str, data: dict[str, Any] | None) -> bytes | None:
        if data is None:
            return None
        return self.store.vault.seal(f"colab:{kind}:{job_id}", data)

    def _open_blob(self, job_id: str, kind: str, blob: bytes | None) -> dict[str, Any] | None:
        if not blob:
            return None
        try:
            opened = self.store.vault.open(f"colab:{kind}:{job_id}", blob)
            return opened if isinstance(opened, dict) else None
        except Exception:
            return None

    def _row_to_job(self, row: Any) -> dict[str, Any]:
        job_id = str(row["id"])
        payload = self._open_blob(job_id, "payload", row["payload"]) or {}
        result = self._open_blob(job_id, "result", row["result"])
        return {
            "id": job_id,
            "job_type": str(row["job_type"]),
            "status": str(row["status"]),
            "requester": str(row["requester"]),
            "preferred_gpu": str(row["preferred_gpu"]),
            "assigned_gpu": row["assigned_gpu"],
            "payload": payload,
            "result": result,
            "error_code": row["error_code"],
            "error": row["error"],
            "cu_used": float(row["cu_used"] or 0.0),
            "created_at": float(row["created_at"]),
            "started_at": float(row["started_at"]) if row["started_at"] is not None else None,
            "finished_at": float(row["finished_at"]) if row["finished_at"] is not None else None,
            "updated_at": float(row["updated_at"]),
        }

    async def status_summary(self) -> dict[str, Any]:
        """Return fast, non-blocking cached status summary without performing HTTP requests."""
        if not self.initialized:
            await self.initialize()
        is_cfg = self.configured()
        if not is_cfg:
            state = "unconfigured"
            detail = "等待配置 Colab Worker 地址与凭证"
        elif not self.config.enabled:
            state = "ready"
            detail = "已暂停调度（管理开关关闭）"
        elif self._active_jobs > 0:
            state = "ready"
            detail = f"运行中 {self._active_jobs} 项 · 排队 {self._queued_jobs} 项"
        elif self._queued_jobs > 0:
            state = "ready"
            detail = f"排队等待 {self._queued_jobs} 项作业"
        elif self._last_cu_balance is not None:
            state = "ready"
            detail = f"就绪 · 余额 {self._last_cu_balance:.2f} CU"
        elif self._last_error:
            state = "ready"
            detail = f"待机 · {self._last_error}"
        else:
            state = "ready"
            detail = "就绪 · 待命调度"

        return {
            "state": state,
            "detail": detail,
            "configured": is_cfg,
            "active_jobs": self._active_jobs,
            "queued_jobs": self._queued_jobs,
            "last_cu_balance": self._last_cu_balance,
            "last_error": self._last_error,
        }

    async def _poll_worker_status(self) -> dict[str, Any]:
        if not self.configured():
            return {
                "reachable": False,
                "compute_units": None,
                "active_sessions": 0,
                "running_job_id": None,
                "drive_folders": None,
                "error": "colab_unconfigured",
            }
        try:
            resp = await self._request("GET", "/v1/status", timeout_sec=5.0)
            if resp.status_code != 200:
                self._worker_reachable = False
                err = f"worker_http_{resp.status_code}"
                self._last_error = err
                return {
                    "reachable": False,
                    "compute_units": self._last_cu_balance,
                    "active_sessions": 0,
                    "running_job_id": None,
                    "drive_folders": None,
                    "error": err,
                }
            data = resp.json()
            reachable = bool(data.get("reachable", True))
            cu = data.get("compute_units")
            if isinstance(cu, (int, float)):
                self._last_cu_balance = float(cu)
            self._worker_reachable = reachable
            self._last_error = data.get("error")
            self._last_status_poll_at = self._clock()
            return {
                "reachable": reachable,
                "compute_units": float(cu) if isinstance(cu, (int, float)) else None,
                "active_sessions": int(data.get("active_sessions") or 0),
                "running_job_id": data.get("running_job_id"),
                "drive_folders": data.get("drive_folders"),
                "error": data.get("error"),
            }
        except Rejected as exc:
            return {
                "reachable": False,
                "compute_units": self._last_cu_balance,
                "active_sessions": 0,
                "running_job_id": None,
                "drive_folders": None,
                "error": str(exc),
            }
        except Exception as exc:
            err = self._scrub(type(exc).__name__)
            self._last_error = err
            return {
                "reachable": False,
                "compute_units": self._last_cu_balance,
                "active_sessions": 0,
                "running_job_id": None,
                "drive_folders": None,
                "error": err,
            }

    async def _today_usage(self) -> dict[str, Any]:
        today = self._local_day()
        async with self.store.lock:
            row_cnt = await self.store.one(
                "SELECT COUNT(*) FROM colab_jobs "
                "WHERE day=? AND (status != 'cancelled' OR started_at IS NOT NULL)",
                (today,),
            )
            row_cu = await self.store.one(
                "SELECT COALESCE(SUM(cu_used), 0.0) FROM colab_jobs WHERE day=?",
                (today,),
            )
        jobs_count = int(row_cnt[0]) if row_cnt and row_cnt[0] is not None else 0
        cu_used = float(row_cu[0]) if row_cu and row_cu[0] is not None else 0.0
        return {
            "jobs_count": jobs_count,
            "cu_used": round(cu_used, 4),
            "daily_job_limit": self.config.daily_job_limit,
            "daily_cu_budget": self.config.daily_cu_budget,
        }

    async def admin_overview(self) -> dict[str, Any]:
        await self.initialize()
        worker = await self._poll_worker_status()
        today = await self._today_usage()
        jobs = await self.list_jobs(limit=30)
        return {
            "configured": self.configured(),
            "source": "sqlite" if self._overridden else "default",
            "config": self.config.model_dump(mode="json"),
            "worker": worker,
            "today": today,
            "jobs": jobs,
        }

    async def set_config(self, cfg: ColabConfig | None) -> dict[str, Any]:
        await self.initialize()
        async with self._op_lock:
            if cfg is None:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key=?", (_CONFIG_KEY,)
                    )
                self.config = self._default_config()
                self._overridden = False
            else:
                validated = ColabConfig.model_validate(cfg)
                payload = validated.model_dump(mode="json")
                sealed = self.store.vault.seal(_VAULT_CFG_CTX, payload)
                async with self.store.transaction():
                    await self.store.db.execute(
                        "INSERT OR REPLACE INTO admin_config VALUES (?, ?)", (_CONFIG_KEY, sealed)
                    )
                self.config = validated
                self._overridden = True
        self._wake.set()
        return self.config.model_dump(mode="json")

    def _validate_job_inputs(
        self,
        job_type: str,
        payload: dict[str, Any],
        gpu: str | None,
    ) -> tuple[dict[str, Any], GpuType]:
        if job_type not in VALID_JOB_TYPES or not isinstance(payload, dict):
            raise Rejected("colab_invalid_job")
        try:
            if job_type == "rvc_cover":
                parsed = _RvcCoverJobPayload.model_validate(payload)
                if (
                    not parsed.inbox_file_id
                    and not parsed.inbox_filename
                    and not parsed.auto_search
                ):
                    raise Rejected("song_source_required")
            else:
                parsed = _RvcTrainJobPayload.model_validate(payload)
        except ValidationError:
            raise Rejected("colab_invalid_job") from None

        if gpu is None:
            chosen_gpu: GpuType = self.config.preferred_gpu
        elif gpu in VALID_GPUS:
            chosen_gpu = gpu  # type: ignore[assignment]
        else:
            raise Rejected("colab_invalid_job")
        return parsed.model_dump(mode="json"), chosen_gpu

    async def submit_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        requester: str = "owner",
        gpu: str | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("colab_unconfigured")
        if not self.config.enabled:
            raise Rejected("colab_disabled")

        clean_payload, chosen_gpu = self._validate_job_inputs(job_type, payload, gpu)
        clean_requester = str(requester or "owner")[:32]
        now = self._clock()
        day = self._local_day(now)
        job_id = uuid.uuid4().hex

        async with self._op_lock:
            async with self.store.lock:
                row_today_cnt = await self.store.one(
                    "SELECT COUNT(*) FROM colab_jobs "
                    "WHERE day=? AND (status != 'cancelled' OR started_at IS NOT NULL)",
                    (day,),
                )
                row_today_cu = await self.store.one(
                    "SELECT COALESCE(SUM(cu_used), 0.0) FROM colab_jobs WHERE day=?",
                    (day,),
                )
                row_pending = await self.store.one(
                    "SELECT COUNT(*) FROM colab_jobs WHERE status IN ('queued', 'running')"
                )
            today_count = (
                int(row_today_cnt[0]) if row_today_cnt and row_today_cnt[0] is not None else 0
            )
            today_cu = (
                float(row_today_cu[0]) if row_today_cu and row_today_cu[0] is not None else 0.0
            )
            pending_count = int(row_pending[0]) if row_pending and row_pending[0] is not None else 0

            if today_count >= self.config.daily_job_limit:
                raise Rejected("colab_daily_job_limit")
            if self.config.daily_cu_budget <= 0 or today_cu >= self.config.daily_cu_budget:
                raise Rejected("colab_daily_cu_budget")
            if pending_count >= MAX_QUEUED_JOBS:
                raise Rejected("colab_job_limit_reached")

            sealed_payload = self._seal_blob(job_id, "payload", clean_payload)
            assert sealed_payload is not None
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO colab_jobs ("
                    "id, job_type, status, requester, preferred_gpu, assigned_gpu, "
                    "payload, result, error_code, error, cu_used, created_at, "
                    "started_at, finished_at, updated_at, day"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        job_id,
                        job_type,
                        "queued",
                        clean_requester,
                        chosen_gpu,
                        None,
                        sealed_payload,
                        None,
                        None,
                        None,
                        0.0,
                        now,
                        None,
                        None,
                        now,
                        day,
                    ),
                )
            await self._refresh_cached_counts()

        self._wake.set()
        return {
            "id": job_id,
            "job_type": job_type,
            "status": "queued",
            "requester": clean_requester,
            "preferred_gpu": chosen_gpu,
            "assigned_gpu": None,
            "payload": clean_payload,
            "result": None,
            "error_code": None,
            "error": None,
            "cu_used": 0.0,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "updated_at": now,
        }

    async def list_jobs(self, limit: int = 30) -> list[dict[str, Any]]:
        await self.initialize()
        bounded = max(1, min(int(limit), 100))
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT * FROM colab_jobs ORDER BY created_at DESC LIMIT ?", (bounded,)
            )
        return [self._row_to_job(r) for r in rows]

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        await self.initialize()
        if not isinstance(job_id, str) or not _JOB_ID_RE.fullmatch(job_id):
            raise Rejected("colab_job_not_found")
        async with self._op_lock:
            async with self.store.lock:
                row = await self.store.one("SELECT * FROM colab_jobs WHERE id=?", (job_id,))
            if row is None:
                raise Rejected("colab_job_not_found")
            job = self._row_to_job(row)
            if job["status"] in {"succeeded", "failed", "cancelled"}:
                raise Rejected("colab_job_not_cancellable")
            was_running = job["status"] == "running"
            if was_running and self.configured():
                try:
                    await self._request("POST", f"/v1/jobs/{job_id}/cancel", timeout_sec=8.0)
                except Rejected:
                    pass
            now = self._clock()
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE colab_jobs SET status='cancelled', error_code='cancelled', "
                    "finished_at=?, updated_at=? WHERE id=?",
                    (now, now, job_id),
                )
                row = await self.store.one("SELECT * FROM colab_jobs WHERE id=?", (job_id,))
            await self._refresh_cached_counts()
        assert row is not None
        return self._row_to_job(row)

    async def emergency_terminate_sessions(self) -> dict[str, Any]:
        await self.initialize()
        if not self.configured():
            raise Rejected("colab_unconfigured")
        resp = await self._request("POST", "/v1/sessions/terminate", timeout_sec=20.0)
        if resp.status_code >= 400:
            raise Rejected("colab_worker_unavailable")
        try:
            worker_data = resp.json()
        except ValueError:
            worker_data = {}

        now = self._clock()
        cancelled_jobs = 0
        async with self._op_lock:
            async with self.store.lock:
                running_rows = await self.store.all(
                    "SELECT id FROM colab_jobs WHERE status='running'"
                )
            if running_rows:
                async with self.store.transaction():
                    for r in running_rows:
                        await self.store.db.execute(
                            "UPDATE colab_jobs SET status='cancelled', "
                            "error_code='emergency_terminated', finished_at=?, updated_at=? "
                            "WHERE id=?",
                            (now, now, r["id"]),
                        )
                        cancelled_jobs += 1
            await self._refresh_cached_counts()

        self._orphan_seen_since = None
        return {
            "terminated_sessions": int(worker_data.get("terminated_sessions") or 0),
            "cancelled_jobs": cancelled_jobs or int(worker_data.get("cancelled_jobs") or 0),
            "detail": str(worker_data.get("detail") or "ok")[:120],
        }

    async def list_drive_inbox(self) -> list[dict[str, Any]]:
        await self.initialize()
        resp = await self._request("GET", "/v1/drive/inbox", timeout_sec=10.0)
        if resp.status_code == 503:
            raise Rejected("colab_drive_not_configured")
        if resp.status_code >= 400:
            raise Rejected("colab_worker_unavailable")
        data = resp.json()
        raw_items = data.get("items") if isinstance(data, dict) else data
        if not isinstance(raw_items, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "id": str(item.get("id") or ""),
                    "name": str(item.get("name") or ""),
                    "size": int(item.get("size") or 0),
                    "modified_time": str(item.get("modified_time") or ""),
                }
            )
        return out

    async def upload_drive_inbox(
        self,
        filename: str,
        raw: bytes,
        *,
        mime_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        await self.initialize()
        clean_name = _sanitize_filename(filename)
        if not isinstance(raw, (bytes, bytearray)) or not raw or len(raw) > MAX_INBOX_UPLOAD_BYTES:
            raise Rejected("colab_drive_invalid_file")
        resp = await self._request(
            "POST",
            "/v1/drive/inbox",
            content=bytes(raw),
            headers={
                "Content-Type": mime_type or "application/octet-stream",
                "X-Drive-Filename": quote(clean_name, safe=""),
            },
            timeout_sec=60.0,
        )
        if resp.status_code == 400 or resp.status_code == 413:
            raise Rejected("colab_drive_invalid_file")
        if resp.status_code == 503:
            raise Rejected("colab_drive_not_configured")
        if resp.status_code >= 400:
            raise Rejected("colab_worker_unavailable")
        res = resp.json()
        return {
            "file_id": str(res["file_id"]),
            "name": str(res.get("name") or clean_name),
            "size": int(res.get("size") or len(raw)),
        }

    async def download_drive_song(self, file_id: str) -> bytes:
        await self.initialize()
        if not isinstance(file_id, str) or not _DRIVE_FILE_ID_RE.fullmatch(file_id.strip()):
            raise Rejected("colab_drive_invalid_file")
        clean_id = file_id.strip()
        resp = await self._request("GET", f"/v1/drive/songs/{clean_id}", timeout_sec=60.0)
        if resp.status_code in {400, 404}:
            raise Rejected("colab_drive_invalid_file")
        if resp.status_code == 503:
            raise Rejected("colab_drive_not_configured")
        if resp.status_code >= 400:
            raise Rejected("colab_worker_unavailable")
        data = resp.content
        if not data or len(data) > MAX_DRIVE_DOWNLOAD_BYTES:
            raise Rejected("colab_drive_invalid_file")
        return data

    async def upload_drive_song(
        self,
        filename: str,
        raw: bytes,
        *,
        title: str = "",
        mime_type: str = "audio/mpeg",
    ) -> dict[str, Any]:
        await self.initialize()
        clean_name = _sanitize_filename(filename)
        if not isinstance(raw, (bytes, bytearray)) or not raw or len(raw) > MAX_SONG_UPLOAD_BYTES:
            raise Rejected("colab_drive_invalid_file")
        headers = {
            "Content-Type": mime_type or "audio/mpeg",
            "X-Drive-Filename": quote(clean_name, safe=""),
        }
        if title.strip():
            headers["X-Song-Title"] = quote(title.strip()[:80], safe="")
        resp = await self._request(
            "POST",
            "/v1/drive/songs",
            content=bytes(raw),
            headers=headers,
            timeout_sec=60.0,
        )
        if resp.status_code in {400, 413}:
            raise Rejected("colab_drive_invalid_file")
        if resp.status_code == 503:
            raise Rejected("colab_drive_not_configured")
        if resp.status_code >= 400:
            raise Rejected("colab_worker_unavailable")
        res = resp.json()
        return {
            "file_id": str(res["file_id"]),
            "name": str(res.get("name") or clean_name),
        }

    def register_completion_hook(
        self,
        job_type: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._hooks.setdefault(job_type, []).append(callback)

    async def _fire_hooks(self, job: dict[str, Any]) -> None:
        callbacks = list(self._hooks.get(job["job_type"], ()))
        for cb in callbacks:
            try:
                await cb(job)
            except Exception as exc:
                log.warning("colab completion hook failed (%s)", type(exc).__name__)

    async def _finalize_job(
        self,
        job_id: str,
        *,
        status: Literal["succeeded", "failed", "cancelled"],
        result: dict[str, Any] | None = None,
        error_code: str | None = None,
        error: str | None = None,
        assigned_gpu: str | None = None,
        cu_used: float = 0.0,
    ) -> dict[str, Any] | None:
        now = self._clock()
        clean_err = self._scrub(error) if error else None
        if status == "failed" and error_code:
            res_obj = dict(result) if isinstance(result, dict) else {}
            res_obj.setdefault("error_code", error_code)
            result = res_obj
        sealed_res = self._seal_blob(job_id, "result", result)
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE colab_jobs SET status=?, assigned_gpu=COALESCE(?, assigned_gpu), "
                "result=?, error_code=?, error=?, cu_used=?, finished_at=?, updated_at=? "
                "WHERE id=?",
                (
                    status,
                    assigned_gpu,
                    sealed_res,
                    error_code,
                    clean_err,
                    max(0.0, float(cu_used or 0.0)),
                    now,
                    now,
                    job_id,
                ),
            )
            row = await self.store.one("SELECT * FROM colab_jobs WHERE id=?", (job_id,))
        await self._refresh_cached_counts()
        return self._row_to_job(row) if row is not None else None

    async def scheduler_step(self) -> None:
        """Perform one serial scheduling, timeout, orphan-check, and completion-hook pass."""
        await self.initialize()
        if not self.configured() or self._closed:
            return

        finished_to_notify: list[dict[str, Any]] = []
        async with self._op_lock:
            now = self._clock()
            async with self.store.lock:
                running_rows = await self.store.all(
                    "SELECT * FROM colab_jobs WHERE status='running' ORDER BY created_at ASC"
                )

            if running_rows:
                self._orphan_seen_since = None
                for row in running_rows:
                    job = self._row_to_job(row)
                    job_id = job["id"]
                    started = job["started_at"] or job["created_at"]
                    if now - started > self.config.job_timeout_seconds:
                        try:
                            await self._request(
                                "POST", f"/v1/jobs/{job_id}/cancel", timeout_sec=8.0
                            )
                        except Rejected:
                            pass
                        done = await self._finalize_job(
                            job_id,
                            status="failed",
                            error_code="job_timeout",
                            error=f"Exceeded {self.config.job_timeout_seconds}s timeout",
                        )
                        if done is not None:
                            finished_to_notify.append(done)
                        continue

                    try:
                        resp = await self._request("GET", f"/v1/jobs/{job_id}", timeout_sec=5.0)
                    except Rejected:
                        continue

                    if resp.status_code == 404:
                        done = await self._finalize_job(
                            job_id,
                            status="failed",
                            error_code="worker_job_lost",
                            error="Worker restarted or lost job state",
                        )
                        if done is not None:
                            finished_to_notify.append(done)
                        continue
                    if resp.status_code != 200:
                        continue

                    w_job = resp.json()
                    w_status = str(w_job.get("status") or "")
                    if w_status in {"succeeded", "failed", "cancelled"}:
                        done = await self._finalize_job(
                            job_id,
                            status=w_status,  # type: ignore[arg-type]
                            result=w_job.get("result"),
                            error_code=w_job.get("error_code"),
                            error=w_job.get("error"),
                            assigned_gpu=w_job.get("assigned_gpu"),
                            cu_used=float(w_job.get("cu_used") or 0.0),
                        )
                        if done is not None and w_status in {"succeeded", "failed"}:
                            finished_to_notify.append(done)
            else:
                # Orphan Colab session watchdog when no job is running.
                if self._orphan_seen_since is not None or now - self._last_status_poll_at >= 60.0:
                    worker = await self._poll_worker_status()
                    if (
                        worker.get("reachable")
                        and int(worker.get("active_sessions") or 0) > 0
                        and not worker.get("running_job_id")
                    ):
                        if self._orphan_seen_since is None:
                            self._orphan_seen_since = now
                        elif (
                            now - self._orphan_seen_since
                            >= self.config.auto_terminate_orphan_minutes * 60.0
                        ):
                            try:
                                await self._request(
                                    "POST", "/v1/sessions/terminate", timeout_sec=20.0
                                )
                            except Rejected:
                                pass
                            self._orphan_seen_since = None
                    else:
                        self._orphan_seen_since = None

                # Serial dispatch of next queued job if enabled.
                if self.config.enabled:
                    async with self.store.lock:
                        next_row = await self.store.one(
                            "SELECT * FROM colab_jobs WHERE status='queued' "
                            "ORDER BY created_at ASC LIMIT 1"
                        )
                    if next_row is not None:
                        next_job = self._row_to_job(next_row)
                        usage = await self._today_usage()
                        if usage["cu_used"] >= self.config.daily_cu_budget:
                            done = await self._finalize_job(
                                next_job["id"],
                                status="failed",
                                error_code="colab_daily_cu_budget",
                                error="Daily CU budget exhausted",
                            )
                            if done is not None:
                                finished_to_notify.append(done)
                        else:
                            gpu_candidates = [next_job["preferred_gpu"]]
                            for fb in self.config.fallback_gpus:
                                if fb not in gpu_candidates:
                                    gpu_candidates.append(fb)
                            body = {
                                "job_id": next_job["id"],
                                "job_type": next_job["job_type"],
                                "gpu_candidates": gpu_candidates,
                                "timeout_seconds": self.config.job_timeout_seconds,
                                "drive_songs_folder_id": self.config.drive_songs_folder_id,
                                "payload": next_job["payload"],
                            }
                            try:
                                disp = await self._request(
                                    "POST", "/v1/jobs", json_body=body, timeout_sec=8.0
                                )
                                if disp.status_code in {200, 202}:
                                    async with self.store.transaction():
                                        await self.store.db.execute(
                                            "UPDATE colab_jobs SET status='running', started_at=?, "
                                            "updated_at=? WHERE id=?",
                                            (now, now, next_job["id"]),
                                        )
                                    await self._refresh_cached_counts()
                                elif disp.status_code == 400:
                                    done = await self._finalize_job(
                                        next_job["id"],
                                        status="failed",
                                        error_code="colab_invalid_job",
                                        error="Worker rejected job payload",
                                    )
                                    if done is not None:
                                        finished_to_notify.append(done)
                            except Rejected:
                                pass

        for done_job in finished_to_notify:
            await self._fire_hooks(done_job)

    async def run_scheduler(self) -> None:
        await self.initialize()
        while not self._closed:
            try:
                await self.scheduler_step()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = self._scrub(type(exc).__name__)
                log.warning("colab_compute scheduler error (%s)", type(exc).__name__)

            interval = 5.0 if (self._active_jobs > 0 or self._queued_jobs > 0) else 20.0
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
                self._wake.clear()
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                raise

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._owned_http is not None and not self._owned_http.is_closed:
            await self._owned_http.aclose()
            self._owned_http = None
