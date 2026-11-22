"""Loopback-isolated Google Colab generic GPU compute scheduler."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .config import Settings
from .security import Rejected, configured_secrets
from .store import Store

log = logging.getLogger("hakimi")

GpuType = Literal["H100", "A100", "G4", "L4", "T4"]
VALID_GPUS: tuple[GpuType, ...] = ("H100", "A100", "G4", "L4", "T4")
MAX_QUEUED_JOBS = 20

_CONFIG_KEY = "colab_compute_config"
_VAULT_CFG_CTX = "admin:colab_compute_config"
_JOB_ID_RE = re.compile(r"^[a-f0-9]{16,64}$")
_JOB_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
_OAUTH_TOKEN_RE = re.compile(r"(?:ya29\.[A-Za-z0-9._-]+|1//[A-Za-z0-9._-]+)")
_BEARER_TOKEN_RE = re.compile(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]{8,}")
_DEFAULT_WARM_ALLOWED_GPUS: tuple[str, ...] = ("L4", "T4")
_ACCELERATION_SAMPLE_LIMIT = 200

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


@dataclass(frozen=True)
class _RegisteredJobSpec:
    job_type: str
    validator: Callable[[dict[str, Any]], dict[str, Any]]
    pre_dispatch: Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]] | None = None


def _safe_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool) or value is None:
        return default
    try:
        if isinstance(value, float) and not math.isfinite(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool) or value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _default_warm_pool() -> dict[str, Any]:
    return {
        "enabled": False,
        "allowed_gpus": list(_DEFAULT_WARM_ALLOWED_GPUS),
        "idle_ttl_seconds": 360,
        "max_ttl_seconds": 900,
        "active_count": 0,
    }


def _normalize_warm_sessions(raw: Any, *, now: float) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        session_name = str(item.get("session_name") or "").strip()
        gpu = str(item.get("gpu") or "").strip().upper()
        if not session_name or not gpu:
            continue
        created_at = _safe_float(item.get("created_at"), 0.0)
        last_used_at = _safe_float(item.get("last_used_at"), created_at)
        idle_ttl_seconds = max(0, _safe_int(item.get("idle_ttl_seconds"), 0))
        raw_expires = item.get("expires_at")
        if raw_expires is not None and not isinstance(raw_expires, bool):
            expires_at = _safe_float(raw_expires, 0.0)
        elif last_used_at > 0.0 and idle_ttl_seconds > 0:
            expires_at = last_used_at + float(idle_ttl_seconds)
        else:
            rem_fallback = _safe_float(item.get("remaining_ttl_seconds"), 0.0)
            expires_at = (now + rem_fallback) if rem_fallback > 0.0 else 0.0
        if not math.isfinite(expires_at) or expires_at <= now:
            continue
        remaining_ttl = max(0, int(round(expires_at - now)))
        out.append(
            {
                "session_name": session_name,
                "gpu": gpu,
                "idle_ttl_seconds": idle_ttl_seconds,
                "remaining_ttl_seconds": remaining_ttl,
                "created_at": created_at,
                "last_used_at": last_used_at,
                "expires_at": expires_at,
                "reuse_count": max(0, _safe_int(item.get("reuse_count"), 0)),
                "setup_ready": bool(item.get("setup_ready", False)),
            }
        )
    return out


def _normalize_warm_pool(raw: Any, *, active_count: int) -> dict[str, Any]:
    base = _default_warm_pool()
    base["active_count"] = max(0, int(active_count))
    if not isinstance(raw, dict):
        return base
    if "enabled" in raw:
        base["enabled"] = bool(raw.get("enabled"))
    raw_gpus = raw.get("allowed_gpus")
    if isinstance(raw_gpus, (list, tuple)):
        gpus: list[str] = []
        for g in raw_gpus:
            cleaned = str(g or "").strip().upper()
            if cleaned and cleaned not in gpus:
                gpus.append(cleaned)
        base["allowed_gpus"] = gpus
    if "idle_ttl_seconds" in raw:
        base["idle_ttl_seconds"] = max(0, _safe_int(raw.get("idle_ttl_seconds"), 360))
    if "max_ttl_seconds" in raw:
        base["max_ttl_seconds"] = max(0, _safe_int(raw.get("max_ttl_seconds"), 900))
    return base


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _accumulate_acceleration(counters: dict[str, int], acc: dict[str, Any]) -> None:
    counters["measured_jobs"] += 1

    dd_used = _first_present(acc, "drive_direct", "drive_direct_used")
    dd_fallback = _first_present(acc, "drive_direct_fallback", "drive_fallback")
    dd_mode = (
        str(_first_present(acc, "drive_transfer_mode", "transfer_mode", "drive_direct_mode") or "")
        .strip()
        .lower()
    )
    if dd_used is True or dd_mode in {"direct", "drive_direct"}:
        counters["drive_direct_jobs"] += 1
    elif isinstance(dd_used, str):
        norm_dd = dd_used.strip().lower()
        if norm_dd in {"direct", "drive_direct", "hit", "used"}:
            counters["drive_direct_jobs"] += 1
        elif norm_dd in {"fallback", "drive_fallback", "ws_fallback"}:
            counters["drive_direct_fallbacks"] += 1
    if dd_fallback is True or dd_mode in {"fallback", "drive_fallback", "ws_fallback"}:
        counters["drive_direct_fallbacks"] += 1

    warm_val = _first_present(
        acc, "warm_reused", "warm_session_reused", "warm_reuse", "session_reused", "warm_hit"
    )
    session_mode = str(_first_present(acc, "session_mode", "warm_mode") or "").strip().lower()
    if warm_val is True or session_mode in {"warm", "reused", "warm_reuse"}:
        counters["warm_reuse_hits"] += 1
    elif warm_val is False or session_mode in {"cold", "cold_start"}:
        counters["warm_cold_starts"] += 1

    cache_sub = acc.get("cache_status") if isinstance(acc.get("cache_status"), dict) else {}
    weight_hit = _first_present(acc, "weight_cache_hit", "cache_weight_hit")
    if weight_hit is None:
        weight_hit = _first_present(cache_sub, "weight_cache_hit", "cache_weight_hit")
    if weight_hit is True:
        counters["cache_weight_hits"] += 1
    elif weight_hit is False:
        counters["cache_weight_misses"] += 1

    venv_hit = _first_present(acc, "venv_cache_hit", "cache_venv_hit")
    if venv_hit is None:
        venv_hit = _first_present(cache_sub, "venv_cache_hit", "cache_venv_hit")
    if venv_hit is True:
        counters["cache_venv_hits"] += 1
    elif venv_hit is False:
        counters["cache_venv_misses"] += 1


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
        self._job_types: dict[str, _RegisteredJobSpec] = {}
        self._hooks: dict[str, list[Callable[[dict[str, Any]], Awaitable[None]]]] = {}

        # Non-blocking cached operational summary for SystemModule.status()
        self._active_jobs = 0
        self._queued_jobs = 0
        self._last_cu_balance: float | None = None
        self._last_error: str | None = None
        self._worker_reachable = False
        self._last_status_poll_at = 0.0
        self._orphan_seen_since: float | None = None
        self._last_warm_sessions: list[dict[str, Any]] = []
        self._last_warm_pool: dict[str, Any] = _default_warm_pool()

    def _default_config(self) -> ColabConfig:
        return ColabConfig()

    def register_job_type(
        self,
        job_type: str,
        *,
        validator: Callable[[dict[str, Any]], dict[str, Any]],
        pre_dispatch: Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]] | None = None,
        on_complete: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        """Register a whitelisted GPU job type with validator and optional lifecycle callbacks."""
        if not isinstance(job_type, str) or not _JOB_TYPE_RE.fullmatch(job_type):
            raise ValueError(f"Invalid Colab job_type: {job_type!r}")
        if not callable(validator):
            raise TypeError("validator must be callable")
        if pre_dispatch is not None and not callable(pre_dispatch):
            raise TypeError("pre_dispatch must be callable or None")
        if on_complete is not None and not callable(on_complete):
            raise TypeError("on_complete must be callable or None")
        self._job_types[job_type] = _RegisteredJobSpec(
            job_type=job_type,
            validator=validator,
            pre_dispatch=pre_dispatch,
        )
        if on_complete is not None:
            hooks = self._hooks.setdefault(job_type, [])
            if on_complete not in hooks:
                hooks.append(on_complete)

    def is_registered_job_type(self, job_type: str) -> bool:
        return isinstance(job_type, str) and job_type in self._job_types

    def registered_job_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._job_types))

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
        out = _OAUTH_TOKEN_RE.sub("[REDACTED]", out)
        out = _BEARER_TOKEN_RE.sub(r"\1[REDACTED]", out)
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
                        cleaned_raw = {
                            k: v for k, v in raw.items() if k in ColabConfig.model_fields
                        }
                        merged = {**default_cfg.model_dump(mode="json"), **cleaned_raw}
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

    def _live_warm_sessions(self, now: float | None = None) -> list[dict[str, Any]]:
        current = self._clock() if now is None else now
        live: list[dict[str, Any]] = []
        for item in self._last_warm_sessions:
            expires_at = _safe_float(item.get("expires_at"), 0.0)
            if not math.isfinite(expires_at) or expires_at <= current:
                continue
            live.append(
                {
                    **item,
                    "remaining_ttl_seconds": max(0, int(round(expires_at - current))),
                }
            )
        return live

    async def status_summary(self) -> dict[str, Any]:
        """Return fast, non-blocking cached status summary without performing HTTP requests."""
        if not self.initialized:
            await self.initialize()
        is_cfg = self.configured()
        warm_sessions = self._live_warm_sessions() if is_cfg else []
        warm_count = len(warm_sessions)
        warm_gpus = "/".join(dict.fromkeys(s["gpu"] for s in warm_sessions if s.get("gpu")))
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
        elif warm_count > 0:
            state = "ready"
            gpu_label = f" {warm_gpus}" if warm_gpus else ""
            if self._last_cu_balance is not None:
                detail = (
                    f"热机待命{gpu_label}（{warm_count} 会话） · "
                    f"余额 {self._last_cu_balance:.2f} CU"
                )
            else:
                detail = f"热机待命{gpu_label}（{warm_count} 会话）"
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
            "warm_active_count": warm_count,
            "warm_sessions": warm_sessions,
        }

    async def _poll_worker_status(self) -> dict[str, Any]:
        if not self.configured():
            self._last_warm_sessions = []
            self._last_warm_pool = _default_warm_pool()
            return {
                "reachable": False,
                "compute_units": None,
                "active_sessions": 0,
                "running_job_id": None,
                "warm_sessions": [],
                "supported_job_types": [],
                "error": "colab_unconfigured",
            }
        try:
            resp = await self._request("GET", "/v1/status", timeout_sec=5.0)
            if resp.status_code != 200:
                self._worker_reachable = False
                err = f"worker_http_{resp.status_code}"
                self._last_error = err
                self._last_warm_sessions = []
                self._last_warm_pool = {**self._last_warm_pool, "active_count": 0}
                return {
                    "reachable": False,
                    "compute_units": self._last_cu_balance,
                    "active_sessions": 0,
                    "running_job_id": None,
                    "warm_sessions": [],
                    "supported_job_types": [],
                    "error": err,
                }
            data = resp.json()
            if not isinstance(data, dict):
                data = {}
            now = self._clock()
            reachable = bool(data.get("reachable", True))
            cu = data.get("compute_units")
            has_cu = isinstance(cu, (int, float)) and not isinstance(cu, bool)
            if has_cu:
                self._last_cu_balance = float(cu)
            raw_err = data.get("error")
            clean_err = self._scrub(raw_err) if raw_err else None
            warm_sessions = _normalize_warm_sessions(data.get("warm_sessions"), now=now)
            warm_pool = _normalize_warm_pool(data.get("warm_pool"), active_count=len(warm_sessions))
            raw_types = data.get("supported_job_types")
            supported_types: list[str] = []
            if isinstance(raw_types, (list, tuple)):
                for item in raw_types:
                    cleaned = str(item or "").strip()
                    if _JOB_TYPE_RE.fullmatch(cleaned) and cleaned not in supported_types:
                        supported_types.append(cleaned)
            self._worker_reachable = reachable
            self._last_error = clean_err
            self._last_status_poll_at = now
            self._last_warm_sessions = warm_sessions
            self._last_warm_pool = warm_pool
            return {
                "reachable": reachable,
                "compute_units": float(cu) if has_cu else None,
                "active_sessions": max(0, _safe_int(data.get("active_sessions"), 0)),
                "running_job_id": data.get("running_job_id"),
                "warm_sessions": [dict(item) for item in warm_sessions],
                "supported_job_types": supported_types,
                "error": clean_err,
            }
        except Rejected as exc:
            self._last_warm_sessions = []
            self._last_warm_pool = {**self._last_warm_pool, "active_count": 0}
            return {
                "reachable": False,
                "compute_units": self._last_cu_balance,
                "active_sessions": 0,
                "running_job_id": None,
                "warm_sessions": [],
                "supported_job_types": [],
                "error": str(exc),
            }
        except Exception as exc:
            err = self._scrub(type(exc).__name__)
            self._last_error = err
            self._last_warm_sessions = []
            self._last_warm_pool = {**self._last_warm_pool, "active_count": 0}
            return {
                "reachable": False,
                "compute_units": self._last_cu_balance,
                "active_sessions": 0,
                "running_job_id": None,
                "warm_sessions": [],
                "supported_job_types": [],
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

    async def _compute_analytics(
        self,
        warm_sessions: list[dict[str, Any]],
        warm_pool: dict[str, Any],
    ) -> dict[str, Any]:
        tz = ZoneInfo(self.s.bot_timezone)
        today_date = datetime.fromtimestamp(self._clock(), tz).date()
        days_30 = [
            (today_date - timedelta(days=offset)).isoformat() for offset in range(29, -1, -1)
        ]
        start_day, end_day = days_30[0], days_30[-1]

        async with self.store.lock:
            summary_row = await self.store.one(
                "SELECT "
                "COUNT(*) AS total_jobs, "
                "COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0) "
                "AS succeeded_jobs, "
                "COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) "
                "AS failed_jobs, "
                "COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0) "
                "AS cancelled_jobs, "
                "COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0) "
                "AS running_jobs, "
                "COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) "
                "AS queued_jobs, "
                "COALESCE(SUM(cu_used), 0.0) AS total_cu_used "
                "FROM colab_jobs"
            )
            trend_rows = await self.store.all(
                "SELECT "
                "day, "
                "COUNT(*) AS jobs, "
                "COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded, "
                "COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed, "
                "COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled, "
                "COALESCE(SUM(cu_used), 0.0) AS cu_used "
                "FROM colab_jobs "
                "WHERE day >= ? AND day <= ? "
                "GROUP BY day",
                (start_day, end_day),
            )
            gpu_rows = await self.store.all(
                "SELECT "
                "COALESCE(NULLIF(assigned_gpu, ''), preferred_gpu) AS gpu, "
                "COUNT(*) AS jobs, "
                "COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded, "
                "COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed, "
                "COALESCE(SUM(cu_used), 0.0) AS cu_used "
                "FROM colab_jobs "
                "WHERE status != 'cancelled' OR started_at IS NOT NULL "
                "GROUP BY COALESCE(NULLIF(assigned_gpu, ''), preferred_gpu) "
                "ORDER BY cu_used DESC, jobs DESC, gpu ASC"
            )
            acc_rows = await self.store.all(
                "SELECT id, result FROM colab_jobs "
                "WHERE status NOT IN ('queued', 'running') "
                "ORDER BY COALESCE(finished_at, updated_at, created_at) DESC, created_at DESC "
                "LIMIT ?",
                (_ACCELERATION_SAMPLE_LIMIT,),
            )

        trend_map: dict[str, Any] = {str(r["day"]): r for r in trend_rows}
        cu_trend: list[dict[str, Any]] = []
        raw_cu_by_day: list[float] = []
        for day_str in days_30:
            row = trend_map.get(day_str)
            if row is None:
                raw_cu_by_day.append(0.0)
                cu_trend.append(
                    {
                        "date": day_str,
                        "jobs": 0,
                        "succeeded": 0,
                        "failed": 0,
                        "cancelled": 0,
                        "cu_used": 0.0,
                    }
                )
            else:
                raw_cu = float(row["cu_used"] or 0.0)
                raw_cu_by_day.append(raw_cu)
                cu_trend.append(
                    {
                        "date": day_str,
                        "jobs": int(row["jobs"] or 0),
                        "succeeded": int(row["succeeded"] or 0),
                        "failed": int(row["failed"] or 0),
                        "cancelled": int(row["cancelled"] or 0),
                        "cu_used": round(raw_cu, 4),
                    }
                )

        total_jobs = int(summary_row["total_jobs"] or 0) if summary_row else 0
        succeeded_jobs = int(summary_row["succeeded_jobs"] or 0) if summary_row else 0
        failed_jobs = int(summary_row["failed_jobs"] or 0) if summary_row else 0
        cancelled_jobs = int(summary_row["cancelled_jobs"] or 0) if summary_row else 0
        running_jobs = int(summary_row["running_jobs"] or 0) if summary_row else 0
        queued_jobs = int(summary_row["queued_jobs"] or 0) if summary_row else 0
        total_cu_used = float(summary_row["total_cu_used"] or 0.0) if summary_row else 0.0
        terminal_denom = succeeded_jobs + failed_jobs
        success_rate = round(succeeded_jobs / terminal_denom, 4) if terminal_denom > 0 else None

        summary = {
            "total_jobs": total_jobs,
            "succeeded_jobs": succeeded_jobs,
            "failed_jobs": failed_jobs,
            "cancelled_jobs": cancelled_jobs,
            "running_jobs": running_jobs,
            "queued_jobs": queued_jobs,
            "success_rate": success_rate,
            "total_cu_used": round(total_cu_used, 4),
            "last_14d_cu_used": round(sum(raw_cu_by_day[-14:]), 4),
            "last_30d_cu_used": round(sum(raw_cu_by_day), 4),
            "last_14d_jobs": sum(item["jobs"] for item in cu_trend[-14:]),
            "last_30d_jobs": sum(item["jobs"] for item in cu_trend),
        }

        total_gpu_cu = sum(float(r["cu_used"] or 0.0) for r in gpu_rows)
        gpu_breakdown: list[dict[str, Any]] = []
        for row in gpu_rows:
            raw_cu = float(row["cu_used"] or 0.0)
            gpu_breakdown.append(
                {
                    "gpu": str(row["gpu"] or ""),
                    "jobs": int(row["jobs"] or 0),
                    "succeeded": int(row["succeeded"] or 0),
                    "failed": int(row["failed"] or 0),
                    "cu_used": round(raw_cu, 4),
                    "cu_share": round(raw_cu / total_gpu_cu, 4) if total_gpu_cu > 0.0 else 0.0,
                }
            )

        counters = {
            "drive_direct_jobs": 0,
            "drive_direct_fallbacks": 0,
            "warm_reuse_hits": 0,
            "warm_cold_starts": 0,
            "cache_weight_hits": 0,
            "cache_weight_misses": 0,
            "cache_venv_hits": 0,
            "cache_venv_misses": 0,
            "measured_jobs": 0,
        }
        for row in acc_rows:
            blob = row["result"]
            if not blob:
                continue
            opened = self._open_blob(str(row["id"]), "result", blob)
            if not isinstance(opened, dict):
                continue
            acc = opened.get("acceleration")
            if isinstance(acc, dict) and acc:
                _accumulate_acceleration(counters, acc)

        acceleration_stats = {
            "warm_pool_enabled": bool(warm_pool.get("enabled", False)),
            "warm_allowed_gpus": list(
                warm_pool.get("allowed_gpus", list(_DEFAULT_WARM_ALLOWED_GPUS))
            ),
            "warm_idle_ttl_seconds": int(warm_pool.get("idle_ttl_seconds", 360)),
            "warm_max_ttl_seconds": int(warm_pool.get("max_ttl_seconds", 900)),
            "warm_active_count": len(warm_sessions),
            **counters,
        }

        return {
            "summary": summary,
            "cu_trend": cu_trend,
            "gpu_breakdown": gpu_breakdown,
            "warm_sessions": [dict(item) for item in warm_sessions],
            "acceleration_stats": acceleration_stats,
        }

    async def admin_overview(self) -> dict[str, Any]:
        await self.initialize()
        worker = await self._poll_worker_status()
        today = await self._today_usage()
        jobs = await self.list_jobs(limit=30)
        warm_sessions = list(worker.get("warm_sessions") or [])
        analytics = await self._compute_analytics(warm_sessions, self._last_warm_pool)
        return {
            "configured": self.configured(),
            "source": "sqlite" if self._overridden else "default",
            "config": self.config.model_dump(mode="json"),
            "supported_job_types": list(self.registered_job_types()),
            "worker": worker,
            "today": today,
            "jobs": jobs,
            **analytics,
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
        if (
            not isinstance(job_type, str)
            or job_type not in self._job_types
            or not isinstance(payload, dict)
        ):
            raise Rejected("colab_invalid_job")
        spec = self._job_types[job_type]
        try:
            validated = spec.validator(dict(payload))
        except Rejected:
            raise
        except (ValidationError, ValueError, TypeError):
            raise Rejected("colab_invalid_job") from None
        if isinstance(validated, BaseModel):
            clean_payload = validated.model_dump(mode="json")
        elif isinstance(validated, dict):
            clean_payload = dict(validated)
        else:
            raise Rejected("colab_invalid_job")

        if gpu is None:
            chosen_gpu: GpuType = self.config.preferred_gpu
        elif gpu in VALID_GPUS:
            chosen_gpu = gpu  # type: ignore[assignment]
        else:
            raise Rejected("colab_invalid_job")
        return clean_payload, chosen_gpu

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
        self._last_warm_sessions = []
        self._last_warm_pool = {**self._last_warm_pool, "active_count": 0}
        return {
            "terminated_sessions": int(worker_data.get("terminated_sessions") or 0),
            "cancelled_jobs": cancelled_jobs or int(worker_data.get("cancelled_jobs") or 0),
            "detail": str(worker_data.get("detail") or "ok")[:120],
        }

    def register_completion_hook(
        self,
        job_type: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        hooks = self._hooks.setdefault(job_type, [])
        if callback not in hooks:
            hooks.append(callback)

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
        clean_code = self._scrub(error_code)[:64] if error_code else None
        if isinstance(result, dict):
            res_obj = dict(result)
            if res_obj.get("error_code"):
                res_obj["error_code"] = self._scrub(res_obj["error_code"])[:64]
            if status == "failed" and clean_code:
                res_obj.setdefault("error_code", clean_code)
            result = res_obj
        elif status == "failed" and clean_code:
            result = {"error_code": clean_code}
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
                    clean_code,
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
                                self._last_warm_sessions = []
                                self._last_warm_pool = {**self._last_warm_pool, "active_count": 0}
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
                            spec = self._job_types.get(next_job["job_type"])
                            if spec is None:
                                done = await self._finalize_job(
                                    next_job["id"],
                                    status="failed",
                                    error_code="colab_invalid_job",
                                    error="Unregistered job type",
                                )
                                if done is not None:
                                    finished_to_notify.append(done)
                            else:
                                dispatch_ctx: dict[str, Any] | None = None
                                pre_failed = False
                                if spec.pre_dispatch is not None:
                                    try:
                                        dispatch_ctx = await spec.pre_dispatch(dict(next_job))
                                    except Rejected as exc:
                                        err_code = str(exc) or "colab_pre_dispatch_failed"
                                        done = await self._finalize_job(
                                            next_job["id"],
                                            status="failed",
                                            error_code=err_code,
                                            error=err_code,
                                        )
                                        if done is not None:
                                            finished_to_notify.append(done)
                                        pre_failed = True
                                    except asyncio.CancelledError:
                                        raise
                                    except Exception as exc:
                                        done = await self._finalize_job(
                                            next_job["id"],
                                            status="failed",
                                            error_code="colab_pre_dispatch_failed",
                                            error=self._scrub(type(exc).__name__),
                                        )
                                        if done is not None:
                                            finished_to_notify.append(done)
                                        pre_failed = True
                                if not pre_failed and dispatch_ctx is not None:
                                    if not isinstance(dispatch_ctx, dict):
                                        done = await self._finalize_job(
                                            next_job["id"],
                                            status="failed",
                                            error_code="colab_pre_dispatch_failed",
                                            error="Invalid dispatch context",
                                        )
                                        if done is not None:
                                            finished_to_notify.append(done)
                                        pre_failed = True
                                if not pre_failed:
                                    gpu_candidates = [next_job["preferred_gpu"]]
                                    for fb in self.config.fallback_gpus:
                                        if fb not in gpu_candidates:
                                            gpu_candidates.append(fb)
                                    body: dict[str, Any] = {
                                        "job_id": next_job["id"],
                                        "job_type": next_job["job_type"],
                                        "gpu_candidates": gpu_candidates,
                                        "timeout_seconds": self.config.job_timeout_seconds,
                                        "allow_warm_keepalive": bool(
                                            self.config.enabled
                                            and (self.config.daily_cu_budget - usage["cu_used"])
                                            >= 0.5
                                        ),
                                        "payload": next_job["payload"],
                                    }
                                    if dispatch_ctx:
                                        body["dispatch_context"] = dispatch_ctx
                                    try:
                                        disp = await self._request(
                                            "POST", "/v1/jobs", json_body=body, timeout_sec=8.0
                                        )
                                        if disp.status_code in {200, 202}:
                                            async with self.store.transaction():
                                                await self.store.db.execute(
                                                    "UPDATE colab_jobs SET status='running', "
                                                    "started_at=?, updated_at=? WHERE id=?",
                                                    (now, now, next_job["id"]),
                                                )
                                            await self._refresh_cached_counts()
                                        elif disp.status_code == 400:
                                            worker_code = "colab_invalid_job"
                                            try:
                                                err_data = disp.json()
                                                if isinstance(err_data, dict):
                                                    detail = str(
                                                        err_data.get("detail") or ""
                                                    ).strip()
                                                    if detail in {
                                                        "colab_invalid_job",
                                                        "drive_context_required",
                                                        "colab_drive_not_configured",
                                                        "song_source_required",
                                                    }:
                                                        worker_code = detail
                                            except ValueError:
                                                pass
                                            done = await self._finalize_job(
                                                next_job["id"],
                                                status="failed",
                                                error_code=worker_code,
                                                error="Worker rejected job payload",
                                            )
                                            if done is not None:
                                                finished_to_notify.append(done)
                                    except Rejected:
                                        pass
                                    finally:
                                        dispatch_ctx = None
                                        body.pop("dispatch_context", None)

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
