"""GCP Cloud Billing BigQuery export balance reconciliation and Cloud Monitoring calibration."""

import asyncio
import hashlib
import json
import re
import time
from datetime import UTC
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from typing import Any

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt
from google.oauth2 import service_account
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .security import Rejected

TOKEN_URI = "https://oauth2.googleapis.com/token"
BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery.readonly"
MONITORING_SCOPE = "https://www.googleapis.com/auth/monitoring.read"
BIGQUERY_API_BASE = "https://bigquery.googleapis.com/bigquery/v2"
MONITORING_API_BASE = "https://monitoring.googleapis.com/v3"
MONITORING_METRIC_TYPE = "aiplatform.googleapis.com/publisher/online_serving/token_count"

MAX_SERVICE_ACCOUNT_BYTES = 65536
MAX_UPSTREAM_JSON_BYTES = 2_097_152
MAX_TOKEN_JSON_BYTES = 16384
MAX_POLL_ATTEMPTS = 10
MAX_PAGES = 20

BIGQUERY_TABLE_RE = re.compile(
    r"^[a-z][a-z0-9-]{4,29}(?::[a-z0-9.-]+)?\.[A-Za-z0-9_]{1,256}\.[A-Za-z0-9_]{1,256}$"
)
DATASET_TABLE_RE = re.compile(r"^[A-Za-z0-9_]{1,256}\.[A-Za-z0-9_]{1,256}$")
PROJECT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
LOCATION_RE = re.compile(r"^[A-Za-z0-9_-]{0,64}$")
SA_EMAIL_RE = re.compile(r"^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+\.iam\.gserviceaccount\.com$")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SAFE_PATH_PROJECT_RE = re.compile(r"^[A-Za-z0-9.:_-]{1,64}$")


def validate_bigquery_table(table: str, default_project: str = "") -> str:
    """Validate and normalize a BigQuery billing export table identifier."""
    if not isinstance(table, str) or not table or "`" in table:
        raise Rejected("invalid_bigquery_table")
    if any(ch.isspace() for ch in table):
        raise Rejected("invalid_bigquery_table")
    if table.count(".") == 1 and ":" not in table:
        if not isinstance(default_project, str) or not default_project:
            raise Rejected("invalid_bigquery_table")
        if any(ch.isspace() for ch in default_project) or "`" in default_project:
            raise Rejected("invalid_bigquery_table")
        candidate = f"{default_project}.{table}"
    else:
        candidate = table
    if not BIGQUERY_TABLE_RE.fullmatch(candidate):
        raise Rejected("invalid_bigquery_table")
    return candidate


def validate_service_account_json(raw: str | dict[str, Any]) -> dict[str, Any]:
    """Strictly validate a GCP service account JSON key without allowing delegation."""
    if isinstance(raw, str):
        encoded = raw.encode("utf-8", errors="strict")
        if len(encoded) > MAX_SERVICE_ACCOUNT_BYTES:
            raise Rejected("gcp_credentials_invalid")
        try:
            info = json.loads(raw)
        except ValueError:
            raise Rejected("gcp_credentials_invalid") from None
    elif isinstance(raw, dict):
        info = dict(raw)
        if len(json.dumps(info).encode("utf-8")) > MAX_SERVICE_ACCOUNT_BYTES:
            raise Rejected("gcp_credentials_invalid")
    else:
        raise Rejected("gcp_credentials_invalid")

    if not isinstance(info, dict):
        raise Rejected("gcp_credentials_invalid")
    if info.get("type") != "service_account" or info.get("token_uri") != TOKEN_URI:
        raise Rejected("gcp_credentials_invalid")
    if info.get("subject"):
        raise Rejected("gcp_credentials_invalid")
    client_email = info.get("client_email")
    if (
        not isinstance(client_email, str)
        or not SA_EMAIL_RE.fullmatch(client_email)
        or any(ch.isspace() for ch in client_email)
    ):
        raise Rejected("gcp_credentials_invalid")
    private_key = info.get("private_key")
    if not isinstance(private_key, str) or not private_key.strip():
        raise Rejected("gcp_credentials_invalid")
    project_id = info.get("project_id", "")
    if project_id is not None and not isinstance(project_id, str):
        raise Rejected("gcp_credentials_invalid")
    try:
        key = serialization.load_pem_private_key(private_key.encode("utf-8"), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise Rejected("gcp_credentials_invalid")
        service_account.Credentials.from_service_account_info(info, scopes=[BIGQUERY_SCOPE])
    except Rejected:
        raise
    except Exception:
        raise Rejected("gcp_credentials_invalid") from None
    return info


def _reject_non_bool(value: Any) -> bool:
    if not isinstance(value, bool):
        raise ValueError("must be a boolean")
    return value


def _reject_non_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("must be an integer")
    return value


def _parse_decimal_input(value: Any) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
        raise ValueError("must be a finite decimal amount")
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation:
        raise ValueError("must be a finite decimal amount") from None
    if not dec.is_finite():
        raise ValueError("must be a finite decimal amount")
    return dec


class CreditGrantRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    name_match: str = Field(min_length=1, max_length=200)
    initial_usd: Decimal = Field(ge=0, le=Decimal("1000000"))

    @field_validator("name_match", mode="before")
    @classmethod
    def _clean_match(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("name_match must be a string")
        cleaned = value.strip()
        if not cleaned or any(ord(ch) < 32 for ch in cleaned):
            raise ValueError("name_match must be non-empty")
        return cleaned

    @field_validator("initial_usd", mode="before")
    @classmethod
    def _check_amount(cls, value: Any) -> Decimal:
        return _parse_decimal_input(value)


class GcpBillingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    bigquery_enabled: bool = False
    billing_project: str = Field(default="", max_length=64)
    billing_table: str = Field(default="", max_length=600)
    bigquery_location: str = Field(default="US", max_length=64)
    initial_credits_usd: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("1000000"))
    credit_grants: list[CreditGrantRule] = Field(default_factory=list, max_length=50)
    bigquery_sync_interval_seconds: int = Field(default=1800, ge=60, le=86400)
    enforce_credit_hard_stop: bool = False
    monitoring_enabled: bool = False
    monitoring_project: str = Field(default="", max_length=64)
    monitoring_sync_interval_seconds: int = Field(default=300, ge=60, le=86400)

    @field_validator(
        "bigquery_enabled",
        "enforce_credit_hard_stop",
        "monitoring_enabled",
        mode="before",
    )
    @classmethod
    def _check_bool(cls, value: Any) -> bool:
        return _reject_non_bool(value)

    @field_validator(
        "bigquery_sync_interval_seconds",
        "monitoring_sync_interval_seconds",
        mode="before",
    )
    @classmethod
    def _check_int(cls, value: Any) -> int:
        return _reject_non_int(value)

    @field_validator("initial_credits_usd", mode="before")
    @classmethod
    def _check_initial_credits(cls, value: Any) -> Decimal:
        return _parse_decimal_input(value)

    @field_validator("billing_project", "monitoring_project", mode="before")
    @classmethod
    def _check_project(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("project must be a string")
        cleaned = value.strip()
        if cleaned and (".." in cleaned or not PROJECT_ID_RE.fullmatch(cleaned)):
            raise ValueError("invalid GCP project ID")
        return cleaned

    @field_validator("bigquery_location", mode="before")
    @classmethod
    def _check_location(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("location must be a string")
        cleaned = value.strip()
        if not LOCATION_RE.fullmatch(cleaned):
            raise ValueError("invalid BigQuery location")
        return cleaned

    @field_validator("billing_table", mode="before")
    @classmethod
    def _check_table_shape(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("billing_table must be a string")
        if not value:
            return ""
        if any(ch.isspace() for ch in value) or "`" in value:
            raise ValueError("invalid_bigquery_table")
        if not (DATASET_TABLE_RE.fullmatch(value) or BIGQUERY_TABLE_RE.fullmatch(value)):
            raise ValueError("invalid_bigquery_table")
        return value

    @model_validator(mode="after")
    def _validate_combined_table(self) -> "GcpBillingConfig":
        if self.billing_table and self.billing_project:
            try:
                validate_bigquery_table(self.billing_table, default_project=self.billing_project)
            except Rejected as exc:
                raise ValueError(str(exc)) from None
        return self

    @classmethod
    def from_settings(cls, settings: Any) -> "GcpBillingConfig":
        billing_project = str(getattr(settings, "gcp_billing_project", "") or "").strip()
        billing_table = str(getattr(settings, "gcp_billing_table", "") or "").strip()
        monitoring_project = str(getattr(settings, "vertex_project", "") or "").strip()
        return cls(
            billing_project=billing_project,
            billing_table=billing_table,
            monitoring_project=monitoring_project,
        )


class GcpBillingCredentialsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    gcp_service_account_json: str = Field(default="", max_length=MAX_SERVICE_ACCOUNT_BYTES)
    clear: bool = False


def build_credit_breakdown_sql(full_table: str) -> str:
    return (
        "SELECT\n"
        "  COALESCE(NULLIF(c.full_name, ''), c.name, c.id) AS credit_name,\n"
        "  ROUND(ABS(SUM(c.amount)), 4) AS used_usd\n"
        "FROM\n"
        f"  `{full_table}`,\n"
        "  UNNEST(credits) AS c\n"
        "GROUP BY\n"
        "  credit_name\n"
        "ORDER BY\n"
        "  used_usd DESC"
    )


def build_cost_summary_sql(full_table: str) -> str:
    return (
        "SELECT\n"
        "  ROUND(COALESCE(SUM(cost), 0), 4) AS gross_cost_usd,\n"
        "  ROUND(COALESCE(SUM(cost) + SUM((SELECT COALESCE(SUM(c.amount), 0) "
        "FROM UNNEST(credits) AS c)), 0), 4) AS net_cost_usd,\n"
        "  UNIX_SECONDS(MAX(usage_end_time)) AS latest_usage_end_ts,\n"
        "  ANY_VALUE(currency) AS currency\n"
        "FROM\n"
        f"  `{full_table}`"
    )


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "" or isinstance(value, bool):
        return default
    try:
        dec = Decimal(str(value))
    except InvalidOperation:
        return default
    return dec if dec.is_finite() else default


_UNSET = object()


def microdollars_signed(value: Decimal) -> int:
    """Convert a signed USD Decimal to integer microdollars, preserving negative signs."""
    if value >= 0:
        return int((value * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
    return -int(((-value) * 1_000_000).to_integral_value(rounding=ROUND_CEILING))


def _to_optional_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num


def _match_credit_grant(credit_name: str, grants: list[CreditGrantRule]) -> CreditGrantRule | None:
    needle = credit_name.strip().lower()
    for rule in grants:
        if rule.name_match.strip().lower() == needle:
            return rule
    best: CreditGrantRule | None = None
    for rule in grants:
        pattern = rule.name_match.strip().lower()
        if pattern in needle and (best is None or len(pattern) > len(best.name_match.strip())):
            best = rule
    if best is not None:
        return best
    for rule in grants:
        if rule.name_match.strip() == "*":
            return rule
    return None


def _parse_bq_rows(data: dict[str, Any], default_names: tuple[str, ...]) -> list[dict[str, Any]]:
    raw_rows = data.get("rows")
    if not isinstance(raw_rows, list):
        return []
    schema = data.get("schema")
    fields = schema.get("fields") if isinstance(schema, dict) else None
    if isinstance(fields, list) and fields:
        names = [str(f.get("name") or f"col_{idx}") for idx, f in enumerate(fields)]
    else:
        names = list(default_names)
    parsed: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        cells = row.get("f")
        if not isinstance(cells, list):
            continue
        item: dict[str, Any] = {}
        for idx, name in enumerate(names):
            cell = cells[idx] if idx < len(cells) and isinstance(cells[idx], dict) else {}
            item[name] = cell.get("v")
        parsed.append(item)
    return parsed


def _parse_point_int(point: dict[str, Any]) -> int:
    val = point.get("value")
    if not isinstance(val, dict):
        return 0
    if val.get("int64Value") is not None and not isinstance(val.get("int64Value"), bool):
        try:
            return int(val["int64Value"])
        except (TypeError, ValueError):
            return 0
    if val.get("doubleValue") is not None and not isinstance(val.get("doubleValue"), bool):
        try:
            return int(round(float(val["doubleValue"])))
        except (TypeError, ValueError):
            return 0
    return 0


def _classify_monitoring_drift(local_total: int, official_total: int) -> tuple[int, float, str]:
    delta_tokens = local_total - official_total
    if local_total == 0 and official_total == 0:
        return 0, 0.0, "in_sync"
    denom = max(local_total, official_total, 1)
    drift_ratio = round(abs(delta_tokens) / denom, 6)
    if abs(delta_tokens) == 0 or drift_ratio <= 0.02:
        status = "in_sync"
    elif delta_tokens > 0 and drift_ratio <= 0.20:
        status = "normal_lag"
    else:
        status = "drift_detected"
    return delta_tokens, drift_ratio, status


class GcpBillingManager:
    """Manages GCP BigQuery billing sync, credit balance accounting, and Cloud Monitoring checks."""

    def __init__(
        self,
        store: Any,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Any = None,
    ):
        self.store = store
        self.s = store.s
        self.env = GcpBillingConfig.from_settings(store.s)
        self.override: GcpBillingConfig | None = None
        self._dedicated_sa: dict[str, Any] | None = None
        self._bq_snapshot: dict[str, Any] | None = None
        self._mon_snapshot: dict[str, Any] | None = None
        self._unsettled_local_microdollars: int = 0
        self._manual_set_microdollars: int | None = None
        self._bq_last_error: str | None = None
        self._mon_last_error: str | None = None
        self._tokens: dict[tuple[str, str], tuple[str, float]] = {}
        self._cached_sa_project: str = ""
        self._token_lock = asyncio.Lock()
        self._bq_sync_lock = asyncio.Lock()
        self._mon_sync_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()
        self._transport = transport
        self._clock = clock or time.time

    @property
    def config(self) -> GcpBillingConfig:
        return self.override or self.env

    def set_transport(self, transport: httpx.AsyncBaseTransport | None) -> None:
        self._transport = transport

    async def initialize(self) -> None:
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT key, payload FROM admin_config WHERE key IN "
                "('gcp_billing', 'gcp_billing_credentials', "
                "'gcp_billing_snapshot', 'gcp_monitoring_snapshot', 'account_balance')"
            )
        loaded = {
            row["key"]: self.store.vault.open("admin:" + row["key"], row["payload"]) for row in rows
        }
        self.override = None
        self._dedicated_sa = None
        self._bq_snapshot = None
        self._mon_snapshot = None
        self._manual_set_microdollars = None
        if "gcp_billing" in loaded:
            merged = (
                {**self.env.model_dump(mode="json"), **loaded["gcp_billing"]}
                if isinstance(loaded["gcp_billing"], dict)
                else loaded["gcp_billing"]
            )
            self.override = GcpBillingConfig.model_validate(merged)
        if "gcp_billing_credentials" in loaded:
            raw_cred = loaded["gcp_billing_credentials"]
            sa_raw = (
                raw_cred.get("gcp_service_account_json")
                if isinstance(raw_cred, dict) and "gcp_service_account_json" in raw_cred
                else raw_cred
            )
            if sa_raw:
                self._dedicated_sa = validate_service_account_json(sa_raw)
        if isinstance(loaded.get("gcp_billing_snapshot"), dict):
            self._bq_snapshot = loaded["gcp_billing_snapshot"]
        if isinstance(loaded.get("gcp_monitoring_snapshot"), dict):
            self._mon_snapshot = loaded["gcp_monitoring_snapshot"]
        if (
            isinstance(loaded.get("account_balance"), dict)
            and "set_microdollars" in loaded["account_balance"]
        ):
            self._manual_set_microdollars = int(loaded["account_balance"]["set_microdollars"])
        if self._bq_snapshot is not None:
            watermark = self._bq_snapshot.get("latest_usage_end_ts") or 0
            self._unsettled_local_microdollars = await self.store.paid_spend_since(watermark)
        else:
            self._unsettled_local_microdollars = 0
        await self._resolve_credentials()

    async def _resolve_credentials(self) -> tuple[dict[str, Any] | None, str]:
        if self._dedicated_sa is not None:
            self._cached_sa_project = str(self._dedicated_sa.get("project_id") or "")
            return self._dedicated_sa, "gcp_billing"
        provider_routing = getattr(self.store.runtime, "provider_routing", None)
        if provider_routing is not None and hasattr(provider_routing, "service_account_info"):
            info, _ = provider_routing.service_account_info()
            if isinstance(info, dict):
                try:
                    validated = validate_service_account_json(info)
                    self._cached_sa_project = str(validated.get("project_id") or "")
                    return validated, "provider_credentials"
                except Rejected:
                    pass
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM admin_config WHERE key='provider_credentials'"
            )
        if row:
            data = self.store.vault.open("admin:provider_credentials", row[0])
            raw_sa = data.get("gcp_service_account_json") if isinstance(data, dict) else None
            if raw_sa:
                try:
                    validated = validate_service_account_json(raw_sa)
                    self._cached_sa_project = str(validated.get("project_id") or "")
                    return validated, "provider_credentials"
                except Rejected:
                    self._cached_sa_project = ""
                    return None, "none"
        self._cached_sa_project = ""
        return None, "none"

    async def _read_local_provider_totals_unlocked(
        self, day: str
    ) -> tuple[dict[str, int], dict[str, int]]:
        rows = await self.store.all(
            "SELECT provider, calls, input_tokens, output_tokens, cached_tokens, thought_tokens "
            "FROM provider_usage_aggregates WHERE period=?",
            ("day:" + day,),
        )
        vertex = {
            "calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_tokens": 0,
            "thought_tokens": 0,
            "total_tokens": 0,
        }
        all_prov = {
            "calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_tokens": 0,
            "thought_tokens": 0,
            "total_tokens": 0,
        }
        for row in rows:
            calls = int(row["calls"] or 0)
            in_tok = int(row["input_tokens"] or 0)
            out_tok = int(row["output_tokens"] or 0)
            cached_tok = int(row["cached_tokens"] or 0)
            thought_tok = int(row["thought_tokens"] or 0)
            total_tok = in_tok + out_tok + thought_tok
            all_prov["calls"] += calls
            all_prov["input_tokens"] += in_tok
            all_prov["output_tokens"] += out_tok
            all_prov["cached_tokens"] += cached_tok
            all_prov["thought_tokens"] += thought_tok
            all_prov["total_tokens"] += total_tok
            if row["provider"] == "vertex_paid":
                vertex["calls"] += calls
                vertex["input_tokens"] += in_tok
                vertex["output_tokens"] += out_tok
                vertex["cached_tokens"] += cached_tok
                vertex["thought_tokens"] += thought_tok
                vertex["total_tokens"] += total_tok
        return vertex, all_prov

    async def sync_local_spend_unlocked(self, paid_spend_fn: Any) -> None:
        if self._bq_snapshot is None:
            return
        watermark = self._bq_snapshot.get("latest_usage_end_ts") or 0
        self._unsettled_local_microdollars = int(await paid_spend_fn(watermark) or 0)

    def _effective_initial_credits(self) -> Decimal:
        cfg = self.config
        if cfg.initial_credits_usd > 0:
            return cfg.initial_credits_usd
        return sum((rule.initial_usd for rule in cfg.credit_grants), Decimal("0"))

    def _compute_bigquery_view(self, sa_project: str = "") -> dict[str, Any]:
        cfg = self.config
        effective_sa_project = sa_project or self._cached_sa_project
        default_proj = cfg.billing_project.strip() or effective_sa_project
        full_table: str | None = None
        if cfg.billing_table:
            try:
                full_table = validate_bigquery_table(
                    cfg.billing_table, default_project=default_proj
                )
            except Rejected:
                full_table = None
        effective_initial = self._effective_initial_credits()
        sql_preview = (
            f"-- Query 1: Credit Breakdown\n{build_credit_breakdown_sql(full_table)}\n\n"
            f"-- Query 2: Cost Summary & Watermark\n{build_cost_summary_sql(full_table)}"
            if full_table
            else None
        )
        snap = self._bq_snapshot
        snap_table = str(snap.get("full_table") or "") if snap else ""
        table_matches = bool(
            snap
            and cfg.billing_table
            and (
                (full_table is not None and snap_table == full_table)
                or (
                    full_table is None
                    and cfg.billing_table.count(".") == 1
                    and snap_table.endswith("." + cfg.billing_table)
                )
            )
        )
        if snap is None or not table_matches:
            unsettled_usd = 0.0
            return {
                "configured": bool(full_table),
                "synced": False,
                "last_sync_at": None,
                "last_error": self._bq_last_error,
                "full_table": full_table,
                "currency": None,
                "gross_cost_usd": None,
                "net_cost_usd": None,
                "latest_usage_end_ts": None,
                "initial_credits_usd": round(float(effective_initial), 4),
                "credits_used_usd": 0.0,
                "settled_remaining_usd": None,
                "unsettled_local_usd": unsettled_usd,
                "realtime_remaining_usd": None,
                "exhausted": False,
                "credits": [],
                "sql_preview": sql_preview,
            }

        raw_credits = snap.get("raw_credits") or []
        credits_out: list[dict[str, Any]] = []
        total_used = Decimal("0")
        for row in raw_credits:
            name = str(row.get("credit_name") or "Unknown Credit")
            used = _to_decimal(row.get("used_usd"))
            total_used += used
            matched = _match_credit_grant(name, cfg.credit_grants)
            if matched is not None:
                init_val: float | None = round(float(matched.initial_usd), 4)
                rem_val: float | None = round(
                    float(max(Decimal("0"), matched.initial_usd - used)), 4
                )
            else:
                init_val = None
                rem_val = None
            credits_out.append(
                {
                    "credit_name": name,
                    "used_usd": round(float(used), 4),
                    "initial_usd": init_val,
                    "remaining_usd": rem_val,
                }
            )
        settled_remaining = effective_initial - total_used
        unsettled_dec = Decimal(self._unsettled_local_microdollars) / Decimal(1_000_000)
        realtime_remaining = settled_remaining - unsettled_dec
        exhausted = bool(
            cfg.bigquery_enabled
            and cfg.enforce_credit_hard_stop
            and effective_initial > 0
            and realtime_remaining <= 0
        )
        return {
            "configured": bool(full_table),
            "synced": True,
            "last_sync_at": snap.get("last_sync_at"),
            "last_error": self._bq_last_error,
            "full_table": snap.get("full_table") or full_table,
            "currency": snap.get("currency"),
            "gross_cost_usd": round(float(_to_decimal(snap.get("gross_cost_usd"))), 4),
            "net_cost_usd": round(float(_to_decimal(snap.get("net_cost_usd"))), 4),
            "latest_usage_end_ts": snap.get("latest_usage_end_ts"),
            "initial_credits_usd": round(float(effective_initial), 4),
            "credits_used_usd": round(float(total_used), 4),
            "settled_remaining_usd": round(float(settled_remaining), 4),
            "unsettled_local_usd": round(float(unsettled_dec), 6),
            "realtime_remaining_usd": round(float(realtime_remaining), 6),
            "exhausted": exhausted,
            "credits": credits_out,
            "sql_preview": sql_preview,
        }

    def _compute_monitoring_view(
        self,
        local_vertex: dict[str, int],
        local_all: dict[str, int],
        sa_project: str = "",
    ) -> dict[str, Any]:
        cfg = self.config
        mon_project = cfg.monitoring_project.strip() or cfg.billing_project.strip() or sa_project
        snap = self._mon_snapshot
        if snap is None:
            return {
                "configured": bool(mon_project),
                "synced": False,
                "last_sync_at": None,
                "last_error": self._mon_last_error,
                "status": "not_synced",
                "window_start_utc": None,
                "window_end_utc": None,
                "official": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "thought_tokens": 0,
                    "cached_tokens": 0,
                    "total_tokens": 0,
                    "by_model": [],
                },
                "local_vertex_paid": local_vertex,
                "local_all_providers": local_all,
                "delta_tokens": None,
                "drift_ratio": None,
            }
        official = snap.get("official") or {}
        official_total = int(official.get("total_tokens") or 0)
        local_total = int(local_vertex.get("total_tokens") or 0)
        delta_tokens, drift_ratio, status = _classify_monitoring_drift(local_total, official_total)
        return {
            "configured": bool(mon_project),
            "synced": True,
            "last_sync_at": snap.get("last_sync_at"),
            "last_error": self._mon_last_error,
            "status": status,
            "window_start_utc": snap.get("window_start_utc"),
            "window_end_utc": snap.get("window_end_utc"),
            "official": official,
            "local_vertex_paid": local_vertex,
            "local_all_providers": local_all,
            "delta_tokens": delta_tokens,
            "drift_ratio": drift_ratio,
        }

    def set_manual_balance_microdollars(self, amount_micro: int | None) -> None:
        self._manual_set_microdollars = amount_micro

    def bigquery_balance_details(self) -> dict[str, Any]:
        """Return synced BigQuery credit details including signed microdollars."""
        bq = self._compute_bigquery_view()
        if not bq["synced"]:
            return {
                "synced": False,
                "signed_microdollars": None,
                "settled_remaining_usd": None,
                "unsettled_local_usd": None,
                "realtime_remaining_usd": None,
            }
        effective_initial = self._effective_initial_credits()
        raw_credits = (self._bq_snapshot or {}).get("raw_credits") or []
        total_used = sum((_to_decimal(row.get("used_usd")) for row in raw_credits), Decimal("0"))
        settled_micro = microdollars_signed(effective_initial - total_used)
        unsettled_micro = int(self._unsettled_local_microdollars or 0)
        return {
            "synced": True,
            "signed_microdollars": settled_micro - unsettled_micro,
            "settled_remaining_usd": bq["settled_remaining_usd"],
            "unsettled_local_usd": bq["unsettled_local_usd"],
            "realtime_remaining_usd": bq["realtime_remaining_usd"],
        }

    def hard_stop_exhausted(self, *, manual_set_micro: Any = _UNSET) -> bool:
        cfg = self.config
        if not (cfg.bigquery_enabled and cfg.enforce_credit_hard_stop):
            return False
        details = self.bigquery_balance_details()
        if not details["synced"]:
            return False
        manual = self._manual_set_microdollars if manual_set_micro is _UNSET else manual_set_micro
        signed_micro = int(details["signed_microdollars"])
        if manual is not None:
            return signed_micro + int(manual) <= 0
        if self._effective_initial_credits() <= 0:
            return False
        return signed_micro <= 0

    def summary_for_budget(self, *, manual_set_micro: Any = _UNSET) -> dict[str, Any]:
        bq = self._compute_bigquery_view()
        mon_status = "not_synced"
        if self._mon_snapshot is not None:
            mon_status = str(self._mon_snapshot.get("status") or "in_sync")
        return {
            "configured": bq["configured"],
            "bigquery_enabled": self.config.bigquery_enabled,
            "enforce_credit_hard_stop": self.config.enforce_credit_hard_stop,
            "synced": bq["synced"],
            "initial_credits_usd": bq["initial_credits_usd"],
            "credits_used_usd": bq["credits_used_usd"],
            "settled_remaining_usd": bq["settled_remaining_usd"],
            "unsettled_local_usd": bq["unsettled_local_usd"],
            "realtime_remaining_usd": bq["realtime_remaining_usd"],
            "exhausted": self.hard_stop_exhausted(manual_set_micro=manual_set_micro),
            "latest_usage_end_ts": bq["latest_usage_end_ts"],
            "last_sync_at": bq["last_sync_at"],
            "last_error": bq["last_error"],
            "monitoring_enabled": self.config.monitoring_enabled,
            "monitoring_status": mon_status,
        }

    async def status(self) -> dict[str, Any]:
        sa_info, sa_source = await self._resolve_credentials()
        sa_project = str(sa_info.get("project_id") or "") if sa_info else ""
        sa_email = str(sa_info.get("client_email") or "") if sa_info else ""
        if self._bq_snapshot is not None:
            watermark = self._bq_snapshot.get("latest_usage_end_ts") or 0
            self._unsettled_local_microdollars = await self.store.paid_spend_since(watermark)
        day = self.store.local_now().date().isoformat()
        async with self.store.lock:
            local_vertex, local_all = await self._read_local_provider_totals_unlocked(day)
            bal_row = await self.store.one(
                "SELECT payload FROM admin_config WHERE key='account_balance'"
            )
        if bal_row:
            bal_data = self.store.vault.open("admin:account_balance", bal_row[0])
            self._manual_set_microdollars = (
                int(bal_data["set_microdollars"])
                if isinstance(bal_data, dict) and "set_microdollars" in bal_data
                else None
            )
        else:
            self._manual_set_microdollars = None
        bigquery = self._compute_bigquery_view(sa_project=sa_project)
        monitoring = self._compute_monitoring_view(local_vertex, local_all, sa_project=sa_project)
        if self._mon_snapshot is not None:
            self._mon_snapshot["status"] = monitoring["status"]
        return {
            "config": self.config.model_dump(mode="json"),
            "environment": self.env.model_dump(mode="json"),
            "source": "admin" if self.override is not None else "env",
            "credentials": {
                "service_account_configured": sa_info is not None,
                "service_account_source": sa_source,
                "service_account_email": sa_email or None,
                "service_account_project": sa_project or None,
            },
            "bigquery": bigquery,
            "monitoring": monitoring,
        }

    async def set_config(self, value: GcpBillingConfig | None) -> dict[str, Any]:
        if value is not None and not isinstance(value, GcpBillingConfig):
            raise ValueError("Expected a validated GcpBillingConfig")
        async with self._config_lock:
            async with self.store.transaction():
                if value is None:
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='gcp_billing'")
                else:
                    await self.store.db.execute(
                        "INSERT INTO admin_config VALUES ('gcp_billing',?) ON CONFLICT(key) "
                        "DO UPDATE SET payload=excluded.payload",
                        (
                            self.store.vault.seal(
                                "admin:gcp_billing", value.model_dump(mode="json")
                            ),
                        ),
                    )
            self.override = value
            self._bq_last_error = None
            self._mon_last_error = None
        return await self.status()

    async def update_credentials(self, update: GcpBillingCredentialsUpdate) -> dict[str, Any]:
        if not isinstance(update, GcpBillingCredentialsUpdate):
            raise ValueError("Expected a validated GcpBillingCredentialsUpdate")
        if not update.clear and not update.gcp_service_account_json.strip():
            raise Rejected("gcp_credentials_invalid")
        async with self._config_lock:
            if update.clear:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key='gcp_billing_credentials'"
                    )
                self._dedicated_sa = None
            else:
                info = validate_service_account_json(update.gcp_service_account_json)
                payload = {"gcp_service_account_json": info}
                async with self.store.transaction():
                    await self.store.db.execute(
                        "INSERT INTO admin_config VALUES ('gcp_billing_credentials',?) "
                        "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                        (self.store.vault.seal("admin:gcp_billing_credentials", payload),),
                    )
                self._dedicated_sa = info
            self._tokens.clear()
        return await self.status()

    async def _bounded_json(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        max_bytes: int = MAX_UPSTREAM_JSON_BYTES,
        error_code: str = "gcp_request_failed",
        **kwargs: Any,
    ) -> dict[str, Any]:
        async with client.stream(method, url, follow_redirects=False, **kwargs) as response:
            if response.status_code != 200:
                raise Rejected(error_code)
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > max_bytes:
                    raise Rejected(error_code)
        try:
            data = json.loads(body)
        except ValueError:
            raise Rejected(error_code) from None
        if not isinstance(data, dict):
            raise Rejected(error_code)
        return data

    async def _access_token(
        self,
        scope: str,
        sa_info: dict[str, Any],
        client: httpx.AsyncClient,
    ) -> str:
        fingerprint = hashlib.sha256(
            (
                str(sa_info.get("client_email", "")) + "|" + str(sa_info.get("private_key", ""))
            ).encode("utf-8")
        ).hexdigest()[:16]
        cache_key = (scope, fingerprint)
        async with self._token_lock:
            cached = self._tokens.get(cache_key)
            if cached and cached[0] and time.monotonic() < cached[1]:
                return cached[0]
            try:
                creds = await asyncio.to_thread(
                    service_account.Credentials.from_service_account_info,
                    sa_info,
                    scopes=[scope],
                )
                now = int(self._clock())
                assertion = jwt.encode(
                    creds.signer,
                    {
                        "iss": creds.service_account_email,
                        "scope": scope,
                        "aud": TOKEN_URI,
                        "iat": now,
                        "exp": now + 3600,
                    },
                ).decode("ascii")
            except Exception:
                raise Rejected("gcp_credentials_invalid") from None
            token = await self._bounded_json(
                client,
                "POST",
                TOKEN_URI,
                max_bytes=MAX_TOKEN_JSON_BYTES,
                error_code="gcp_token_failed",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
            )
            access_token = token.get("access_token")
            try:
                expires_in = int(token.get("expires_in", 0))
            except (TypeError, ValueError):
                expires_in = 0
            if (
                not isinstance(access_token, str)
                or not access_token
                or not 60 <= expires_in <= 3600
            ):
                raise Rejected("gcp_token_failed")
            self._tokens[cache_key] = (access_token, time.monotonic() + expires_in - 30)
            return access_token

    async def _execute_bigquery_query(
        self,
        client: httpx.AsyncClient,
        token: str,
        billing_project: str,
        sql: str,
        default_columns: tuple[str, ...],
        *,
        mocked: bool = False,
    ) -> list[dict[str, Any]]:
        if not SAFE_PATH_PROJECT_RE.fullmatch(billing_project):
            raise Rejected("invalid_bigquery_table")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body: dict[str, Any] = {
            "query": sql,
            "useLegacySql": False,
            "timeoutMs": 15000,
        }
        if self.config.bigquery_location:
            body["location"] = self.config.bigquery_location
        url = f"{BIGQUERY_API_BASE}/projects/{billing_project}/queries"
        data = await self._bounded_json(
            client,
            "POST",
            url,
            headers=headers,
            json=body,
            error_code="gcp_billing_request_failed",
        )
        if data.get("errors"):
            raise Rejected("gcp_billing_request_failed")
        job_ref = data.get("jobReference") if isinstance(data.get("jobReference"), dict) else {}
        job_id = str(job_ref.get("jobId") or "")
        job_proj = str(job_ref.get("projectId") or billing_project)
        job_loc = str(job_ref.get("location") or self.config.bigquery_location or "")
        attempts = 0
        while not data.get("jobComplete", False):
            attempts += 1
            if attempts > MAX_POLL_ATTEMPTS:
                raise Rejected("gcp_billing_query_timeout")
            if not JOB_ID_RE.fullmatch(job_id) or not SAFE_PATH_PROJECT_RE.fullmatch(job_proj):
                raise Rejected("gcp_billing_request_failed")
            params: dict[str, Any] = {"timeoutMs": 5000}
            if job_loc and LOCATION_RE.fullmatch(job_loc):
                params["location"] = job_loc
            await asyncio.sleep(0 if mocked else 0.3)
            data = await self._bounded_json(
                client,
                "GET",
                f"{BIGQUERY_API_BASE}/projects/{job_proj}/queries/{job_id}",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                error_code="gcp_billing_request_failed",
            )
            if data.get("errors"):
                raise Rejected("gcp_billing_request_failed")
        rows = _parse_bq_rows(data, default_columns)
        page_token = data.get("pageToken")
        pages = 0
        while isinstance(page_token, str) and page_token:
            pages += 1
            if pages >= MAX_PAGES:
                break
            if not JOB_ID_RE.fullmatch(job_id) or not SAFE_PATH_PROJECT_RE.fullmatch(job_proj):
                break
            params = {"pageToken": page_token}
            if job_loc and LOCATION_RE.fullmatch(job_loc):
                params["location"] = job_loc
            page = await self._bounded_json(
                client,
                "GET",
                f"{BIGQUERY_API_BASE}/projects/{job_proj}/queries/{job_id}",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                error_code="gcp_billing_request_failed",
            )
            if page.get("errors"):
                raise Rejected("gcp_billing_request_failed")
            rows.extend(_parse_bq_rows(page, default_columns))
            page_token = page.get("pageToken")
        return rows

    async def sync_bigquery(
        self, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> dict[str, Any]:
        async with self._bq_sync_lock:
            sa_info, _ = await self._resolve_credentials()
            if sa_info is None:
                self._bq_last_error = "gcp_credentials_missing"
                raise Rejected("gcp_credentials_missing")
            sa_project = str(sa_info.get("project_id") or "").strip()
            default_proj = self.config.billing_project.strip() or sa_project
            try:
                full_table = validate_bigquery_table(
                    self.config.billing_table, default_project=default_proj
                )
            except Rejected as exc:
                self._bq_last_error = str(exc)
                raise
            billing_project = (
                self.config.billing_project.strip()
                or sa_project
                or full_table.split(".", 1)[0].split(":", 1)[0]
            )
            credit_sql = build_credit_breakdown_sql(full_table)
            summary_sql = build_cost_summary_sql(full_table)
            effective_transport = transport or self._transport
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(30.0),
                    trust_env=False,
                    follow_redirects=False,
                    transport=effective_transport,
                ) as client:
                    token = await self._access_token(BIGQUERY_SCOPE, sa_info, client)
                    credit_rows = await self._execute_bigquery_query(
                        client,
                        token,
                        billing_project,
                        credit_sql,
                        ("credit_name", "used_usd"),
                        mocked=effective_transport is not None,
                    )
                    summary_rows = await self._execute_bigquery_query(
                        client,
                        token,
                        billing_project,
                        summary_sql,
                        ("gross_cost_usd", "net_cost_usd", "latest_usage_end_ts", "currency"),
                        mocked=effective_transport is not None,
                    )
            except Rejected as exc:
                self._bq_last_error = str(exc)
                raise
            except (httpx.HTTPError, TimeoutError, ValueError):
                self._bq_last_error = "gcp_billing_request_failed"
                raise Rejected("gcp_billing_request_failed") from None

            raw_credits = []
            for row in credit_rows:
                name = str(row.get("credit_name") or "Unknown Credit")
                used = round(float(_to_decimal(row.get("used_usd"))), 4)
                raw_credits.append({"credit_name": name, "used_usd": used})
            summary_row = summary_rows[0] if summary_rows else {}
            latest_ts = _to_optional_float(summary_row.get("latest_usage_end_ts"))
            snapshot = {
                "full_table": full_table,
                "last_sync_at": float(self._clock()),
                "gross_cost_usd": round(float(_to_decimal(summary_row.get("gross_cost_usd"))), 4),
                "net_cost_usd": round(float(_to_decimal(summary_row.get("net_cost_usd"))), 4),
                "latest_usage_end_ts": latest_ts,
                "currency": str(summary_row["currency"]) if summary_row.get("currency") else None,
                "raw_credits": raw_credits,
            }
            unsettled_micro = await self.store.paid_spend_since(latest_ts or 0)
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO admin_config VALUES ('gcp_billing_snapshot',?) "
                    "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                    (self.store.vault.seal("admin:gcp_billing_snapshot", snapshot),),
                )
            self._bq_snapshot = snapshot
            self._unsettled_local_microdollars = unsettled_micro
            self._bq_last_error = None
        return await self.status()

    async def sync_monitoring(
        self, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> dict[str, Any]:
        async with self._mon_sync_lock:
            sa_info, _ = await self._resolve_credentials()
            if sa_info is None:
                self._mon_last_error = "gcp_credentials_missing"
                raise Rejected("gcp_credentials_missing")
            sa_project = str(sa_info.get("project_id") or "").strip()
            monitoring_project = (
                self.config.monitoring_project.strip()
                or self.config.billing_project.strip()
                or sa_project
            )
            if not monitoring_project or not SAFE_PATH_PROJECT_RE.fullmatch(monitoring_project):
                self._mon_last_error = "gcp_monitoring_project_required"
                raise Rejected("gcp_monitoring_project_required")

            local_now = self.store.local_now()
            local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_iso = local_midnight.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            end_iso = local_now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            base_params: list[tuple[str, str]] = [
                ("filter", f'metric.type="{MONITORING_METRIC_TYPE}"'),
                ("interval.startTime", start_iso),
                ("interval.endTime", end_iso),
                ("aggregation.alignmentPeriod", "60s"),
                ("aggregation.perSeriesAligner", "ALIGN_SUM"),
                ("aggregation.crossSeriesReducer", "REDUCE_SUM"),
                ("aggregation.groupByFields", "metric.labels.type"),
                ("aggregation.groupByFields", "resource.labels.model_user_id"),
                ("view", "FULL"),
            ]
            effective_transport = transport or self._transport
            all_series: list[dict[str, Any]] = []
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(30.0),
                    trust_env=False,
                    follow_redirects=False,
                    transport=effective_transport,
                ) as client:
                    token = await self._access_token(MONITORING_SCOPE, sa_info, client)
                    headers = {"Authorization": f"Bearer {token}"}
                    url = f"{MONITORING_API_BASE}/projects/{monitoring_project}/timeSeries"
                    page_token = ""
                    for _ in range(MAX_PAGES):
                        params = list(base_params)
                        if page_token:
                            params.append(("pageToken", page_token))
                        data = await self._bounded_json(
                            client,
                            "GET",
                            url,
                            headers=headers,
                            params=params,
                            error_code="gcp_monitoring_request_failed",
                        )
                        series = data.get("timeSeries")
                        if isinstance(series, list):
                            all_series.extend(s for s in series if isinstance(s, dict))
                        next_tok = data.get("nextPageToken")
                        if not isinstance(next_tok, str) or not next_tok:
                            break
                        page_token = next_tok
            except Rejected as exc:
                self._mon_last_error = str(exc)
                raise
            except (httpx.HTTPError, TimeoutError, ValueError):
                self._mon_last_error = "gcp_monitoring_request_failed"
                raise Rejected("gcp_monitoring_request_failed") from None

            input_tokens = 0
            output_tokens = 0
            thought_tokens = 0
            cached_tokens = 0
            by_model_map: dict[str, dict[str, int]] = {}
            for ts in all_series:
                metric_labels = (
                    ts.get("metric", {}).get("labels", {})
                    if isinstance(ts.get("metric"), dict)
                    else {}
                )
                res_labels = (
                    ts.get("resource", {}).get("labels", {})
                    if isinstance(ts.get("resource"), dict)
                    else {}
                )
                tok_type = str(metric_labels.get("type") or "").strip().lower()
                model_id = (
                    str(
                        res_labels.get("model_user_id")
                        or metric_labels.get("model_user_id")
                        or "unknown"
                    ).strip()
                    or "unknown"
                )
                points = ts.get("points") if isinstance(ts.get("points"), list) else []
                pts_sum = sum(_parse_point_int(p) for p in points if isinstance(p, dict))
                bucket = by_model_map.setdefault(
                    model_id,
                    {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "thought_tokens": 0,
                        "cached_tokens": 0,
                    },
                )
                if tok_type in {"input", "prompt", "input_tokens"}:
                    input_tokens += pts_sum
                    bucket["input_tokens"] += pts_sum
                elif tok_type in {"output", "candidates", "output_tokens"}:
                    output_tokens += pts_sum
                    bucket["output_tokens"] += pts_sum
                elif tok_type in {"thought", "thoughts", "thought_tokens", "reasoning"}:
                    thought_tokens += pts_sum
                    bucket["thought_tokens"] += pts_sum
                elif tok_type in {
                    "cached",
                    "cached_content",
                    "cache_read",
                    "cached_input",
                    "cached_tokens",
                }:
                    cached_tokens += pts_sum
                    bucket["cached_tokens"] += pts_sum

            by_model = []
            for model_id in sorted(by_model_map):
                m = by_model_map[model_id]
                by_model.append(
                    {
                        "model": model_id,
                        **m,
                        "total_tokens": m["input_tokens"]
                        + m["output_tokens"]
                        + m["thought_tokens"],
                    }
                )
            official_total = input_tokens + output_tokens + thought_tokens
            official = {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "thought_tokens": thought_tokens,
                "cached_tokens": cached_tokens,
                "total_tokens": official_total,
                "by_model": by_model,
            }
            day = local_now.date().isoformat()
            async with self.store.lock:
                local_vertex, _ = await self._read_local_provider_totals_unlocked(day)
            _, _, drift_status = _classify_monitoring_drift(
                local_vertex["total_tokens"], official_total
            )
            snapshot = {
                "last_sync_at": float(self._clock()),
                "window_start_utc": start_iso,
                "window_end_utc": end_iso,
                "status": drift_status,
                "official": official,
            }
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT INTO admin_config VALUES ('gcp_monitoring_snapshot',?) "
                    "ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
                    (self.store.vault.seal("admin:gcp_monitoring_snapshot", snapshot),),
                )
            self._mon_snapshot = snapshot
            self._mon_last_error = None
        return await self.status()

    async def tick(self) -> None:
        now = float(self._clock())
        cfg = self.config
        if cfg.bigquery_enabled:
            last_bq = (
                float(self._bq_snapshot.get("last_sync_at") or 0)
                if self._bq_snapshot is not None
                else 0.0
            )
            if not self._bq_snapshot or now - last_bq >= cfg.bigquery_sync_interval_seconds:
                try:
                    await self.sync_bigquery()
                except Rejected as exc:
                    self._bq_last_error = str(exc)
                except Exception:
                    self._bq_last_error = "gcp_billing_request_failed"
        if cfg.monitoring_enabled:
            last_mon = (
                float(self._mon_snapshot.get("last_sync_at") or 0)
                if self._mon_snapshot is not None
                else 0.0
            )
            if not self._mon_snapshot or now - last_mon >= cfg.monitoring_sync_interval_seconds:
                try:
                    await self.sync_monitoring()
                except Rejected as exc:
                    self._mon_last_error = str(exc)
                except Exception:
                    self._mon_last_error = "gcp_monitoring_request_failed"
