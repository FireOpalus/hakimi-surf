"""Standalone Bilibili account patrol engine backed by HakimiPluginClient KV & AI RPCs."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import inspect
import json
import logging
import math
import random
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlencode, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from hakimi_plugin_sdk import PluginRpcError

from .client import (
    BVID,
    NODE_TRANSPORT_ERRORS,
    BiliClientError,
    BiliPublicClient,
    bounded_json,
    clean_text,
)

log = logging.getLogger("hakimi_plugin_bilibili")

MIXIN = (
    46,
    47,
    18,
    2,
    53,
    8,
    23,
    32,
    15,
    50,
    10,
    31,
    58,
    3,
    45,
    35,
    27,
    43,
    5,
    49,
    33,
    9,
    42,
    19,
    29,
    28,
    14,
    39,
    12,
    38,
    41,
    13,
    37,
    48,
    7,
    16,
    24,
    55,
    40,
    61,
    26,
    17,
    0,
    1,
    60,
    51,
    30,
    4,
    22,
    25,
    54,
    21,
    56,
    59,
    6,
    63,
    57,
    62,
    11,
    36,
    20,
    34,
    44,
    52,
)

GET_PATHS = frozenset(
    {
        "/x/web-interface/nav",
        "/x/web-interface/wbi/index/top/feed/rcmd",
        "/x/polymer/web-dynamic/v1/feed/all",
        "/x/web-interface/view",
        "/x/player/wbi/v2",
        "/x/v2/reply",
        "/x/web-interface/archive/relation",
        "/x/v3/fav/folder/created/list-all",
    }
)
POST_PATHS = frozenset({"/x/web-interface/archive/like", "/x/v3/fav/resource/deal"})
PUBLIC_READ_PATHS = frozenset(
    {"/x/web-interface/nav", "/x/web-interface/view", "/x/player/wbi/v2", "/x/v2/reply"}
)
SUBTITLE_HOSTS = frozenset({"aisubtitle.hdslb.com", "i0.hdslb.com", "i1.hdslb.com", "i2.hdslb.com"})
COVER_URL_RE = re.compile(
    r"^https://(?:i[0-2]\.hdslb\.com|archive\.biliimg\.com)/bfs/[A-Za-z0-9_./-]+$"
)

CREDENTIAL_KEYS = ("sessdata", "bili_jct", "buvid3", "buvid4")
COOKIE_NAMES = {
    "sessdata": "SESSDATA",
    "bili_jct": "bili_jct",
    "buvid3": "buvid3",
    "buvid4": "buvid4",
}
USER_AGENT = "HakimiSurf/0.4 text patrol"
SPI_PATH = "/x/frontend/finger/spi"
MAX_COVER_THUMB_BYTES = 98_304
MAX_READ_ATTEMPTS = 3
READ_RETRY_SECONDS = 3600
RISK_COOLDOWN_SECONDS = 43200
MAX_WATCHED_INDEX_ENTRIES = 200
MAX_RECENT_RUNS = 20
MAX_ACTIONS_LOG = 100

_MARKDOWN_SAFE = str.maketrans({"<": "＜", ">": "＞", "[": "［", "]": "］", "`": "｀", "!": "！"})
_PUBLIC_DEGRADE_ERRORS = frozenset(
    {
        "bili_account_unavailable",
        "bili_account_upstream_error",
        "bili_video_schema_changed",
        "bili_video_mismatch",
        "bili_wbi_unavailable",
        "bili_subtitle_host_forbidden",
        "redirect_not_allowed",
        "upstream_body_too_large",
    }
)


def validate_cookie(value: str) -> str:
    """Reject raw Cookie headers, whitespace, or control bytes."""
    if (
        not isinstance(value, str)
        or len(value) > 4096
        or any(ord(c) < 33 or ord(c) > 126 or c in ";\r\n" for c in value)
    ):
        raise ValueError("Paste only the cookie value, not the complete Cookie header")
    return value


class BiliPolicy(BaseModel):
    """Strict autonomous Bilibili patrol and interaction policy."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = False
    allow_like: bool = False
    allow_favorite: bool = False
    favorite_folder_id: str = Field(default="", pattern=r"^[1-9][0-9]{0,19}$|^$")
    daily_action_limit: int = Field(default=1, ge=1, le=2)

    @model_validator(mode="after")
    def folder_required(self) -> BiliPolicy:
        if self.allow_favorite and not self.favorite_folder_id:
            raise ValueError("An explicit owned favorite folder is required")
        return self


class CredentialUpdate(BaseModel):
    """Strict credential update payload with cookie-header rejection."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    sessdata: str = ""
    bili_jct: str = ""
    buvid3: str = ""
    buvid4: str = ""
    clear: bool = False

    @field_validator(*CREDENTIAL_KEYS)
    @classmethod
    def valid(cls, value: str) -> str:
        return validate_cookie(value)


class Pick(BaseModel):
    """Single selected patrol candidate."""

    model_config = ConfigDict(extra="forbid", strict=True)

    bvid: str = Field(pattern="^" + BVID + "$")
    like: bool = False
    favorite: bool = False
    share_owner: bool = False
    private_topic_ref: str = Field(default="", max_length=48)


class Selection(BaseModel):
    """Model candidate selection output."""

    model_config = ConfigDict(extra="forbid", strict=True)

    selected: list[Pick] = Field(max_length=2)
    mood_longnick: str = Field(default="", max_length=80)


def wbi_sign(
    params: dict[str, Any], keys: tuple[str, str] | list[str] | None, now: float
) -> dict[str, Any]:
    """Sign Bilibili WBI query parameters using current img_key + sub_key."""
    if not keys:
        raise BiliClientError("bili_wbi_unavailable")
    raw = "".join(keys)
    if not re.fullmatch("[a-fA-F0-9]{64}", raw):
        raise BiliClientError("bili_wbi_unavailable")
    mixin = "".join(raw[index] for index in MIXIN)[:32]
    values: dict[str, Any] = {**params, "wts": str(int(now))}
    values = {k: re.sub("[!'()*]", "", str(v)) for k, v in sorted(values.items())}
    values["w_rid"] = hashlib.md5((urlencode(values, quote_via=quote) + mixin).encode()).hexdigest()
    return values


def wbi_keys(data: dict[str, Any]) -> tuple[str, str]:
    """Extract 32-hex WBI image/sub keys from `/x/web-interface/nav` response data."""
    keys: list[str] = []
    wbi_img = data.get("wbi_img") if isinstance(data, dict) else None
    if not isinstance(wbi_img, dict):
        raise BiliClientError("bili_wbi_unavailable")
    for name in ("img_url", "sub_url"):
        url = str(wbi_img.get(name, "") or "")
        key = urlsplit(url).path.rsplit("/", 1)[-1].split(".")[0]
        if not re.fullmatch("[a-fA-F0-9]{32}", key):
            raise BiliClientError("bili_wbi_unavailable")
        keys.append(key)
    return (keys[0], keys[1])


def has_voucher(value: Any) -> bool:
    """Recursively detect Bilibili `v_voucher` risk-challenge payloads."""
    if isinstance(value, dict):
        return bool(value.get("v_voucher")) or any(has_voucher(v) for v in value.values())
    if isinstance(value, list):
        return any(has_voucher(v) for v in value)
    return False


def video_duration(value: Any) -> float | None:
    """Return finite positive video duration in seconds, or None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return float(value) if math.isfinite(value) and value > 0 else None
    except OverflowError:
        return None


def subtitle_timestamp(value: Any) -> float | None:
    """Return finite non-negative subtitle timestamp in seconds, or None."""
    if value == 0 and not isinstance(value, bool):
        return 0.0
    return video_duration(value)


def subtitle_text(body: Any, duration: float | None) -> tuple[str, bool, str]:
    """Bound subtitle text without describing clipped or untimed transcripts as complete."""
    lines = body.get("body") if isinstance(body, dict) else None
    if not isinstance(lines, list) or not lines:
        return "", False, "unavailable"
    parts: list[str] = []
    starts: list[float] = []
    ends: list[float] = []
    valid = True
    for line in lines[:1000]:
        if not isinstance(line, dict) or not isinstance(line.get("content"), str):
            valid = False
            continue
        text = clean_text(line["content"])
        if text:
            parts.append(text)
        start, end = subtitle_timestamp(line.get("from")), subtitle_timestamp(line.get("to"))
        if (
            start is None
            or end is None
            or end <= start
            or (duration is not None and end > duration + 5)
        ):
            valid = False
        else:
            starts.append(start)
            ends.append(end)
    joined = " ".join(parts)
    clipped = len(lines) > 1000 or len(joined) > 12000
    joined = joined[:12000]
    if not joined:
        return "", False, "unavailable"
    complete = bool(
        not clipped
        and valid
        and duration is not None
        and ends
        and min(starts) <= max(5, duration * 0.05)
        and max(ends) >= min(duration - 5, duration * 0.95)
    )
    status = "truncated" if clipped else "complete" if complete else "partial"
    return joined, complete, status


def _validate_subtitle_url(url: str) -> None:
    if (
        not isinstance(url, str)
        or len(url) > 2048
        or any(ord(c) < 32 or ord(c) == 127 for c in url)
    ):
        raise BiliClientError("bili_subtitle_host_forbidden")
    try:
        parsed = urlsplit(url)
    except ValueError:
        raise BiliClientError("bili_subtitle_host_forbidden") from None
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or host not in SUBTITLE_HOSTS
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.path.startswith("/bfs/")
        or ".." in parsed.path.split("/")
    ):
        raise BiliClientError("bili_subtitle_host_forbidden")


def _truncate_text(text: str, limit: int) -> str:
    cleaned = clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    excerpt = cleaned[: max(0, limit - 1)]
    boundary = max(excerpt.rfind(mark) for mark in ("\n", "。", "！", "？", "!", "?"))
    if boundary >= 0:
        excerpt = excerpt[: boundary + 1]
    return excerpt.rstrip() + "…"


def _sanitize_summary(text: str, secrets: list[str], *, limit: int = 1000) -> str:
    cleaned = clean_text(text)
    if not cleaned or any(s and len(s) >= 8 and s in cleaned for s in secrets):
        raise BiliClientError("invalid_output")
    cleaned = (
        cleaned.replace("[CQ:", "［CQ:")
        .replace("@everyone", "＠everyone")
        .replace("@all", "＠all")
        .replace("@全体成员", "＠全体成员")
    )
    cleaned = _truncate_text(cleaned, limit)
    cleaned = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[联系方式已隐去]", cleaned)
    cleaned = re.sub(r"(?<!\w)\d{5,}(?!\w)", "[号码已隐去]", cleaned)
    cleaned = re.sub(r"@[\w\u4e00-\u9fff-]+", "[称呼已隐去]", cleaned)
    cleaned = re.sub(r"https?://\S+", "[链接省略]", cleaned)
    return cleaned.translate(_MARKDOWN_SAFE)[:3000]


def _resolve_timezone(tz_raw: Any) -> tuple[str, ZoneInfo]:
    candidate = str(tz_raw or "").strip() or "Asia/Shanghai"
    try:
        return candidate, ZoneInfo(candidate)
    except ZoneInfoNotFoundError:
        return "Asia/Shanghai", ZoneInfo("Asia/Shanghai")


class BiliPatrolEngine:
    """Autonomous Bilibili patrol, WBI/SPI session manager, and KV persistence engine."""

    def __init__(
        self,
        plugin_client: Any,
        http_client: httpx.AsyncClient | None = None,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        public_client: BiliPublicClient | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.client = plugin_client
        self.public_client = public_client
        self.clock = clock
        self.sleep = sleep
        self.rng = rng or random.SystemRandom()

        self.timezone = "Asia/Shanghai"
        self.zone = ZoneInfo(self.timezone)
        self.max_context_bytes = 48_000
        self.node_bili_delegate_enabled = True
        self.node_bili_fallback_local = True
        self.node_connected = False
        self.module_enabled: Callable[[], bool] | bool = True

        self.env_policy = BiliPolicy()
        self.policy = self.env_policy
        self.overridden = False
        self.creds: dict[str, str] = {key: "" for key in CREDENTIAL_KEYS}
        self.creds_source = "env"

        self.blocked_until = 0.0
        self.last_error: str | None = None
        self.checked_at: float | None = None
        self.account: dict[str, Any] | None = None
        self.buvid4_device: dict[str, Any] | None = None

        self.keys: tuple[str, str] | None = None
        self.credentials_epoch = 0
        self.policy_epoch = 0
        self.last_request = 0.0
        self._has_requested = False

        self.watched_index: dict[str, Any] = {"items": {}, "seeded": False}
        self.recent_runs: list[dict[str, Any]] = []
        self.actions_log: list[dict[str, Any]] = []
        self._watched_cache: dict[str, dict[str, Any]] = {}
        self._initialized = False
        self._current_activity: str | None = None

        self._owns_http = http_client is None
        self.http = (
            http_client
            if http_client is not None
            else httpx.AsyncClient(
                timeout=15,
                trust_env=False,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=1),
            )
        )

        self.init_lock = asyncio.Lock()
        self.network_lock = asyncio.Lock()
        self.run_lock = asyncio.Lock()
        self.policy_lock = asyncio.Lock()
        self.state_lock = asyncio.Lock()
        self.patrol_context: ContextVar[tuple[int, int] | None] = ContextVar(
            "bili_patrol_context", default=None
        )
        self.public_read_context: ContextVar[tuple[int, Any] | None] = ContextVar(
            "bili_public_read_context", default=None
        )

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    def _is_module_enabled(self) -> bool:
        if callable(self.module_enabled):
            return bool(self.module_enabled())
        return bool(self.module_enabled)

    def _client_connected(self) -> bool:
        return self.client is not None and bool(getattr(self.client, "connected", True))

    def _can_delegate(self) -> bool:
        return bool(
            self.client is not None and self.node_connected and self.node_bili_delegate_enabled
        )

    def set_node_connected(self, connected: bool) -> None:
        self.node_connected = bool(connected)
        if self.public_client is not None:
            self.public_client.set_node_connected(self.node_connected)

    def set_module_enabled(self, enabled: bool) -> None:
        self.module_enabled = bool(enabled)
        if self.public_client is not None:
            self.public_client.set_module_enabled(bool(enabled))

    def _secrets(self) -> list[str]:
        return [v for v in self.creds.values() if v]

    def local(self, now: float | None = None) -> datetime:
        return datetime.fromtimestamp(self.clock() if now is None else now, self.zone)

    def _device_fingerprint(self) -> str:
        raw = json.dumps(
            ["bili-device-credentials", *(self.creds[key] for key in CREDENTIAL_KEYS[:-1])],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(raw).hexdigest()

    def _extract_rpc_error(self, exc: BaseException) -> tuple[str, str]:
        if isinstance(exc, PluginRpcError):
            return exc.code, getattr(exc, "detail", "") or exc.message
        return str(exc) or type(exc).__name__, ""

    def patrol_unavailable(self) -> str | None:
        """Return unavailability code matching built-in BilibiliSurfModule semantics."""
        if not self._is_module_enabled():
            return "module_disabled"
        if not self.policy.enabled:
            return "bili_patrol_disabled"
        if not self.creds.get("sessdata"):
            return "bili_credentials_missing"
        if self.blocked_until > self.clock():
            return "bili_account_cooling_down"
        return None

    def status_snapshot(self) -> dict[str, Any]:
        """Return sanitized runtime status dict pushed to PluginBus."""
        return {
            "autonomous": bool(self.policy.enabled),
            "backend": "local",
            "cn_delegate": bool(self.node_connected and self.node_bili_delegate_enabled),
            "node_connected": bool(self.node_connected),
            "account_mask": ("…" + self.account["mid"][-4:]) if self.account else None,
            "cooldown_until": self.blocked_until,
            "last_error": self.last_error,
        }

    async def _push_runtime_status(self, *, current_activity: Any = ...) -> None:
        if current_activity is not ...:
            self._current_activity = current_activity
        if not self._client_connected():
            return
        unavailable = {"bili.patrol": self.patrol_unavailable()}
        effective_activity = self._current_activity if current_activity is ... else current_activity
        if effective_activity is None and current_activity is ...:
            await self.client.push_status(
                self.status_snapshot(),
                unavailable_actions=unavailable,
            )
        else:
            await self.client.push_status(
                self.status_snapshot(),
                unavailable_actions=unavailable,
                current_activity=effective_activity,
            )

    def _account_state_payload(self) -> dict[str, Any]:
        return {
            "blocked_until": self.blocked_until,
            "last_error": self.last_error,
            "checked_at": self.checked_at,
            "account": self.account,
            "buvid4_device": self.buvid4_device,
        }

    async def _save_account_state(self) -> None:
        if self.client is None:
            return
        await self.client.state_set("account_state", self._account_state_payload())

    def _prune_watched_index(self) -> list[str]:
        items: dict[str, dict[str, Any]] = self.watched_index.setdefault("items", {})
        if len(items) <= MAX_WATCHED_INDEX_ENTRIES:
            return []
        evicted: list[str] = []
        # Evict oldest finished entries first, then oldest non-active entries.
        ordered = sorted(
            items.items(),
            key=lambda kv: (
                1 if kv[1].get("status") in {"reading", "analyzing"} else 0,
                0 if kv[1].get("status") == "done" else 1,
                float(kv[1].get("created") or 0.0),
            ),
        )
        excess = len(items) - MAX_WATCHED_INDEX_ENTRIES
        for bvid, _ in ordered[:excess]:
            items.pop(bvid, None)
            self._watched_cache.pop(bvid, None)
            evicted.append(bvid)
        return evicted

    async def _save_watched_index(self) -> None:
        evicted = self._prune_watched_index()
        if self.client is None:
            return
        await self.client.state_set("watched_index", self.watched_index)
        for bvid in evicted:
            with contextlib.suppress(Exception):
                await self.client.state_delete(f"watched.{bvid}")

    async def _save_recent_runs(self) -> None:
        self.recent_runs = sorted(
            self.recent_runs,
            key=lambda r: float(r.get("created") or 0.0),
            reverse=True,
        )[:MAX_RECENT_RUNS]
        if self.client is not None:
            await self.client.state_set("recent_runs", self.recent_runs)

    async def _save_actions_log(self) -> None:
        self.actions_log = sorted(
            self.actions_log,
            key=lambda a: float(a.get("created") or 0.0),
            reverse=True,
        )[:MAX_ACTIONS_LOG]
        if self.client is not None:
            await self.client.state_set("actions_log", self.actions_log)

    def _apply_env_defaults(self, raw_env: Any) -> None:
        if not isinstance(raw_env, dict):
            return
        tz_name, zone = _resolve_timezone(raw_env.get("bot_timezone"))
        self.timezone = tz_name
        self.zone = zone
        if "node_bili_delegate_enabled" in raw_env:
            self.node_bili_delegate_enabled = bool(raw_env["node_bili_delegate_enabled"])
        if "node_bili_fallback_local" in raw_env:
            self.node_bili_fallback_local = bool(raw_env["node_bili_fallback_local"])
        raw_max_ctx = raw_env.get("max_context_bytes")
        if (
            isinstance(raw_max_ctx, int)
            and not isinstance(raw_max_ctx, bool)
            and raw_max_ctx >= 4096
        ):
            self.max_context_bytes = raw_max_ctx

        if isinstance(raw_env.get("policy"), dict):
            try:
                self.env_policy = BiliPolicy.model_validate(raw_env["policy"])
            except ValidationError:
                self.env_policy = BiliPolicy()
        else:
            enabled = bool(raw_env.get("bili_autonomous_enabled", raw_env.get("enabled", False)))
            try:
                self.env_policy = BiliPolicy(
                    enabled=enabled,
                    allow_like=bool(raw_env.get("allow_like", False)),
                    allow_favorite=bool(raw_env.get("allow_favorite", False)),
                    favorite_folder_id=str(raw_env.get("favorite_folder_id", "")),
                    daily_action_limit=int(raw_env.get("daily_action_limit", 1)),
                )
            except (TypeError, ValueError, ValidationError):
                self.env_policy = BiliPolicy(enabled=enabled)

        if self.public_client is not None:
            self.public_client.apply_env_defaults(raw_env)

    def _apply_credentials_payload(self, raw_creds: Any) -> None:
        if not isinstance(raw_creds, dict):
            return
        raw_inner = raw_creds.get("creds")
        creds_map = raw_inner if isinstance(raw_inner, dict) else raw_creds
        parsed = {key: str(creds_map.get(key) or "") for key in CREDENTIAL_KEYS}
        for val in parsed.values():
            validate_cookie(val)
        self.creds = parsed
        src = str(raw_creds.get("source") or "sqlite").strip()
        self.creds_source = src if src in {"env", "sqlite"} else "sqlite"

    def _apply_account_state_payload(self, raw_state: Any) -> None:
        if not isinstance(raw_state, dict):
            return
        raw_blocked = raw_state.get("blocked_until", 0.0)
        self.blocked_until = (
            float(raw_blocked)
            if isinstance(raw_blocked, (int, float))
            and not isinstance(raw_blocked, bool)
            and math.isfinite(float(raw_blocked))
            and float(raw_blocked) >= 0.0
            else 0.0
        )
        raw_err = raw_state.get("last_error")
        self.last_error = str(raw_err) if isinstance(raw_err, str) and raw_err else None
        raw_checked = raw_state.get("checked_at")
        self.checked_at = (
            float(raw_checked)
            if isinstance(raw_checked, (int, float))
            and not isinstance(raw_checked, bool)
            and math.isfinite(float(raw_checked))
            else None
        )
        raw_acc = raw_state.get("account")
        self.account = dict(raw_acc) if isinstance(raw_acc, dict) and raw_acc.get("mid") else None
        raw_dev = raw_state.get("buvid4_device")
        self.buvid4_device = dict(raw_dev) if isinstance(raw_dev, dict) else None

    async def _import_watched_seed(self, seed: Any) -> None:
        if not isinstance(seed, dict):
            self.watched_index = {"items": {}, "seeded": True}
            await self._save_watched_index()
            return

        items: dict[str, dict[str, Any]] = {}
        raw_watched = seed.get("watched")
        if raw_watched is None:
            raw_watched = seed.get("history")
        if raw_watched is None:
            raw_watched = seed.get("items")

        watched_rows: list[dict[str, Any]] = []
        if isinstance(raw_watched, dict):
            for bv, row in raw_watched.items():
                if isinstance(row, dict):
                    watched_rows.append({"bvid": bv, **row})
        elif isinstance(raw_watched, list):
            watched_rows = [row for row in raw_watched if isinstance(row, dict)]

        actions_by_bv: dict[str, list[str]] = {}
        raw_actions = (
            seed.get("actions") if seed.get("actions") is not None else seed.get("actions_log")
        )
        imported_actions: list[dict[str, Any]] = []
        if isinstance(raw_actions, list):
            for act in raw_actions:
                if not isinstance(act, dict):
                    continue
                bvid = str(act.get("bvid") or "")
                kind = str(act.get("kind") or "")
                status = str(act.get("status") or "done")
                day = str(act.get("day") or "")
                created = float(act.get("created") or 0.0)
                if not re.fullmatch(BVID, bvid) or not kind:
                    continue
                imported_actions.append(
                    {
                        "day": day or self.local(created).date().isoformat(),
                        "bvid": bvid,
                        "kind": kind,
                        "status": status,
                        "created": created,
                    }
                )
                if status == "done":
                    actions_by_bv.setdefault(bvid, []).append(kind)

        for row in watched_rows:
            bvid = str(row.get("bvid") or "")
            if not re.fullmatch(BVID, bvid):
                continue
            status = str(row.get("status") or "done")
            created = float(row.get("created") or 0.0)
            run_id = str(row.get("run_id") or "seed")
            attempts = int(row.get("attempts") or 1)
            retry_after = row.get("retry_after")
            summary = str(row.get("summary") or "")
            row_actions = list(
                dict.fromkeys(
                    [*(str(a) for a in (row.get("actions") or [])), *actions_by_bv.get(bvid, [])]
                )
            )
            has_summary = bool(summary)
            if status == "done" and not has_summary and "summary" in row:
                status = "uncertain"
            items[bvid] = {
                "status": status,
                "run_id": run_id,
                "created": created,
                "attempts": max(1, attempts),
                "retry_after": (
                    float(retry_after) if isinstance(retry_after, (int, float)) else None
                ),
                "actions": sorted(row_actions),
                "has_summary": has_summary,
            }
            if summary:
                doc = {
                    "bvid": bvid,
                    "created": created,
                    "summary": summary,
                    "actions": sorted(row_actions),
                }
                self._watched_cache[bvid] = doc
                await self.client.state_set(f"watched.{bvid}", doc)

        raw_runs = seed.get("runs") if seed.get("runs") is not None else seed.get("recent_runs")
        imported_runs: list[dict[str, Any]] = []
        if isinstance(raw_runs, list):
            for run in raw_runs:
                if not isinstance(run, dict):
                    continue
                created = float(run.get("created") or 0.0)
                run_id = str(run.get("id") or uuid.uuid4().hex)
                day = str(run.get("day") or self.local(created).date().isoformat())
                slot = str(run.get("slot") or f"manual-{run_id}")
                status = str(run.get("status") or "done")
                reason = str(run["reason"]) if run.get("reason") is not None else None
                imported_runs.append(
                    {
                        "id": run_id,
                        "day": day,
                        "slot": slot,
                        "status": status,
                        "created": created,
                        "reason": reason,
                    }
                )

        self.watched_index = {"items": items, "seeded": True}
        if imported_runs:
            self.recent_runs = imported_runs
        if imported_actions:
            self.actions_log = imported_actions

    def _recover_stuck_state(self) -> bool:
        changed = False
        now = self.clock()

        for run in self.recent_runs:
            if run.get("status") == "working":
                run["status"] = "abandoned"
                changed = True

        for act in self.actions_log:
            if act.get("status") == "sending":
                act["status"] = "uncertain"
                changed = True

        for item in self.watched_index.get("items", {}).values():
            st = item.get("status")
            if st == "attempted" or (st == "done" and item.get("has_summary") is False):
                item["status"] = "uncertain"
                changed = True
            elif st == "reading":
                attempts = int(item.get("attempts") or 0)
                if attempts > 0:
                    item["status"] = "retryable"
                    item["retry_after"] = (
                        max(now + READ_RETRY_SECONDS, self.blocked_until)
                        if attempts < MAX_READ_ATTEMPTS
                        else None
                    )
                else:
                    item["status"] = "uncertain"
                changed = True
            elif st == "analyzing":
                item["status"] = "uncertain"
                changed = True

        return changed

    async def initialize(self) -> None:
        """Load KV state, migrate legacy watched_seed if absent, and recover interrupted work."""
        async with self.init_lock:
            env_defaults = await self.client.state_get("env_defaults")
            self._apply_env_defaults(env_defaults)

            raw_policy = await self.client.state_get("policy")
            if isinstance(raw_policy, dict):
                cleaned_policy = {
                    k: v for k, v in raw_policy.items() if k not in {"times", "jitter_minutes"}
                }
                self.policy = BiliPolicy.model_validate(cleaned_policy)
                self.overridden = True
            else:
                self.policy = self.env_policy
                self.overridden = False

            raw_creds = await self.client.state_get("credentials")
            if raw_creds is not None:
                self._apply_credentials_payload(raw_creds)

            raw_account = await self.client.state_get("account_state")
            if raw_account is not None:
                self._apply_account_state_payload(raw_account)

            raw_index = await self.client.state_get("watched_index")
            seeded_now = False
            if raw_index is None:
                raw_seed = await self.client.state_get("watched_seed")
                await self._import_watched_seed(raw_seed)
                seeded_now = True
            elif isinstance(raw_index, dict):
                items = raw_index.get("items")
                self.watched_index = {
                    "items": dict(items) if isinstance(items, dict) else {},
                    "seeded": bool(raw_index.get("seeded", True)),
                }
                raw_runs = await self.client.state_get("recent_runs")
                if isinstance(raw_runs, list):
                    self.recent_runs = [dict(r) for r in raw_runs if isinstance(r, dict)]
                raw_actions = await self.client.state_get("actions_log")
                if isinstance(raw_actions, list):
                    self.actions_log = [dict(a) for a in raw_actions if isinstance(a, dict)]

            recovered = False
            if not self.run_lock.locked():
                recovered = self._recover_stuck_state()

            if seeded_now or recovered:
                await self._save_watched_index()
                await self._save_recent_runs()
                await self._save_actions_log()

            self._initialized = True

    def _ensure_current(self, *, epoch: int | None = None, allow_cooldown: bool = False) -> None:
        if not self._is_module_enabled():
            raise BiliClientError("module_disabled")
        if not allow_cooldown and self.blocked_until > self.clock():
            raise BiliClientError("bili_account_cooling_down")
        if epoch is not None and epoch != self.credentials_epoch:
            raise BiliClientError("bili_settings_changed")
        public = self.public_read_context.get()
        if public is not None and public[0] != self.credentials_epoch:
            raise BiliClientError("bili_settings_changed")
        context = self.patrol_context.get()
        if context is not None and (
            not self.policy.enabled or context != (self.policy_epoch, self.credentials_epoch)
        ):
            raise BiliClientError("bili_settings_changed")

    async def _ensure_request_current(self) -> None:
        self._ensure_current()
        public = self.public_read_context.get()
        if public is not None and public[1] is not None:
            try:
                result = public[1]()
                if inspect.isawaitable(result):
                    result = await result
                if result is False:
                    raise BiliClientError("bili_request_revoked")
            except asyncio.CancelledError:
                raise
            except Exception:
                raise BiliClientError("bili_request_revoked") from None
        self._ensure_current()

    async def set_policy(self, value_or_none: BiliPolicy | dict[str, Any] | None) -> dict[str, Any]:
        """Persist or reset autonomous patrol policy in encrypted KV."""
        async with self.policy_lock:
            if value_or_none is None:
                await self.client.state_delete("policy")
                self.policy, self.overridden = self.env_policy, False
            else:
                parsed = (
                    value_or_none
                    if isinstance(value_or_none, BiliPolicy)
                    else BiliPolicy.model_validate(value_or_none)
                )
                await self.client.state_set("policy", parsed.model_dump())
                self.policy, self.overridden = parsed, True
            self.policy_epoch += 1
        await self._push_runtime_status()
        return await self.status()

    async def update_credentials(
        self, update_dict: CredentialUpdate | dict[str, Any]
    ) -> dict[str, Any]:
        """Update or clear Bilibili small-account cookies in encrypted KV."""
        update = (
            update_dict
            if isinstance(update_dict, CredentialUpdate)
            else CredentialUpdate.model_validate(update_dict)
        )
        async with self.policy_lock, self.network_lock:
            value = (
                {key: "" for key in CREDENTIAL_KEYS}
                if update.clear
                else {key: getattr(update, key) or self.creds[key] for key in CREDENTIAL_KEYS}
            )
            await self.client.state_set("credentials", {"creds": value, "source": "sqlite"})
            self.checked_at = None
            self.account = None
            self.buvid4_device = None
            await self._save_account_state()
            self.creds, self.creds_source, self.keys = value, "sqlite", None
            self.credentials_epoch += 1
        await self._push_runtime_status()
        return await self.status()

    async def _pace_request(self) -> None:
        if self._has_requested or self.last_request > 0:
            delay = max(0.0, self.rng.uniform(2, 6) - (time.monotonic() - self.last_request))
            if delay > 0:
                await self.sleep(delay)
        await self._ensure_request_current()
        self.last_request = time.monotonic()
        self._has_requested = True

    def _headers(self, *, subtitle: bool = False) -> dict[str, str]:
        return {
            "Referer": "https://www.bilibili.com/",
            "User-Agent": USER_AGENT,
            "Cookie": ""
            if subtitle
            else "; ".join(
                COOKIE_NAMES[key] + "=" + value for key, value in self.creds.items() if value
            ),
        }

    async def _risk(self, reason: str) -> None:
        self.blocked_until = self.clock() + RISK_COOLDOWN_SECONDS
        self.last_error = reason
        await self._save_account_state()
        with contextlib.suppress(Exception):
            await self._push_runtime_status()
        if self._client_connected():
            try:
                await self.client.notify_owner(
                    "BOISER，小号巡逻遇到凭证过期或平台风控，已暂停 12 小时喵。"
                    "请先登录 B 站检查状态，再在后台更新凭证喵。",
                    code="bili_account_" + reason,
                )
            except Exception:
                log.info("event=bili_account_alert_failed")
        raise BiliClientError(reason)

    async def trigger_risk(self, reason: str = "bili_account_risk") -> dict[str, Any]:
        """Record a 12-hour risk cooldown from an external caller (e.g. VideoWatch)."""
        safe_reason = (
            reason
            if reason in {"bili_account_risk", "bili_cookie_expired"}
            else "bili_account_risk"
        )
        try:
            await self._risk(safe_reason)
        except BiliClientError:
            pass
        return {"cooldown_until": self.blocked_until, "last_error": self.last_error}

    async def _delegated_bili_request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        post: dict[str, Any] | None = None,
        subtitle: bool = False,
        cookies: dict[str, str],
    ) -> dict[str, Any] | None:
        try:
            res = await self.client.node_call(
                "bili.request",
                {
                    "method": "POST" if post is not None else "GET",
                    "path": path,
                    "params": params or {},
                    "post": post,
                    "subtitle": bool(subtitle),
                    "cookies": cookies,
                },
            )
        except (PluginRpcError, TimeoutError, OSError) as exc:
            code, detail = self._extract_rpc_error(exc)
            if code in NODE_TRANSPORT_ERRORS:
                if (
                    post is None
                    or code in {"node_disconnected", "node_not_dispatched", "plugin_disconnected"}
                ) and self.node_bili_fallback_local:
                    return None
                raise BiliClientError("bili_account_unavailable") from None
            if code == "bili_http_status" or code.startswith("bili_http_status"):
                combined = f"{code}:{detail}"
                if any(s in combined for s in ("401", "403", "412", "429")):
                    await self._risk("bili_account_risk")
                raise BiliClientError("bili_account_unavailable") from None
            raise BiliClientError("bili_account_unavailable") from None
        except Exception:
            raise BiliClientError("bili_account_unavailable") from None

        envelope = res.get("envelope") if isinstance(res, dict) else None
        if not isinstance(envelope, dict):
            raise BiliClientError("bili_account_unavailable")
        return envelope

    async def _ensure_buvid4_locked(self) -> None:
        """Best-effort SPI `buvid4` lookup under network_lock, persisted before HTTP call."""
        if self.creds["buvid4"] or not (self.creds["sessdata"] and self.creds["bili_jct"]):
            return
        await self._ensure_request_current()
        fingerprint = self._device_fingerprint()
        if self.buvid4_device and self.buvid4_device.get("fingerprint") == fingerprint:
            return
        self.buvid4_device = {
            "fingerprint": fingerprint,
            "attempted_at": self.clock(),
            "status": "attempted",
        }
        await self._save_account_state()
        await self._pace_request()

        status, value = "unavailable", ""
        obj: dict[str, Any] | None = None
        try:
            if self._can_delegate():
                obj = await self._delegated_bili_request(
                    SPI_PATH,
                    cookies={k: v for k, v in self.creds.items() if v},
                )
            if obj is None:
                obj = await bounded_json(
                    self.http,
                    "GET",
                    "https://api.bilibili.com" + SPI_PATH,
                    headers=self._headers(),
                    max_bytes=16384,
                    timeout=5,
                )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {401, 403, 412, 429}:
                await self._risk("bili_account_risk")
            obj = None
        except BiliClientError as exc:
            if exc.code in {"bili_account_risk", "bili_cookie_expired"}:
                raise
            obj = None
        except (httpx.HTTPError, ValueError, RecursionError):
            obj = None
        finally:
            self.last_request = time.monotonic()
            self._has_requested = True

        await self._ensure_request_current()
        if obj is not None:
            code = obj.get("code")
            if has_voucher(obj) or code in (-101, -352, -412):
                await self._risk("bili_cookie_expired" if code == -101 else "bili_account_risk")
            data = obj.get("data")
            if (
                isinstance(code, int)
                and not isinstance(code, bool)
                and code == 0
                and isinstance(data, dict)
            ):
                try:
                    value = validate_cookie(data.get("b_4", ""))
                except ValueError:
                    value = ""
                if value:
                    status = "ready"

        if value:
            updated = {**self.creds, "buvid4": value}
            await self.client.state_set("credentials", {"creds": updated, "source": "sqlite"})
            self.creds, self.creds_source = updated, "sqlite"
        if self.buvid4_device and self.buvid4_device.get("fingerprint") == fingerprint:
            self.buvid4_device["status"] = status
        await self._save_account_state()

    async def ensure_buvid4(self, guard: Callable[[], Any] | None = None) -> dict[str, str]:
        """Refresh `buvid4` if missing and return current Bilibili cookie dictionary."""

        async def valid() -> None:
            await self._ensure_request_current()
            if guard is not None:
                res = guard()
                if inspect.isawaitable(res):
                    res = await res
                if res is False:
                    raise BiliClientError("bili_request_revoked")
            await self._ensure_request_current()

        await valid()
        async with self.network_lock:
            await valid()
            await self._ensure_buvid4_locked()
            await valid()
            return dict(self.creds)

    async def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        post: dict[str, Any] | None = None,
        subtitle: bool = False,
    ) -> dict[str, Any]:
        public = self.public_read_context.get() is not None
        if public and (post is not None or (not subtitle and path not in PUBLIC_READ_PATHS)):
            raise BiliClientError("bili_public_read_only")
        if subtitle:
            if post is not None:
                raise BiliClientError("bili_subtitle_host_forbidden")
            _validate_subtitle_url(path)
        elif path not in (POST_PATHS if post is not None else GET_PATHS):
            raise BiliClientError("bili_endpoint_forbidden")

        await self._ensure_request_current()
        async with self.network_lock:
            await self._ensure_request_current()
            if not self.creds["sessdata"] and not public:
                raise BiliClientError("bili_credentials_missing")
            if not subtitle:
                await self._ensure_buvid4_locked()
            await self._pace_request()
            headers = self._headers(subtitle=subtitle)
            url = path if subtitle else "https://api.bilibili.com" + path
            try:
                obj: dict[str, Any] | None = None
                if self._can_delegate():
                    req_cookies = (
                        {}
                        if (subtitle or (public and not self.creds["sessdata"]))
                        else {k: v for k, v in self.creds.items() if v}
                    )
                    obj = await self._delegated_bili_request(
                        path,
                        params,
                        post=post,
                        subtitle=subtitle,
                        cookies=req_cookies,
                    )
                if obj is None:
                    obj = await bounded_json(
                        self.http,
                        "POST" if post is not None else "GET",
                        url,
                        params=params,
                        headers=headers,
                        data=post,
                        max_bytes=524288,
                    )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {401, 403, 412, 429}:
                    await self._risk("bili_account_risk")
                raise BiliClientError("bili_account_unavailable") from None
            except BiliClientError:
                raise
            except (httpx.HTTPError, ValueError):
                raise BiliClientError("bili_account_unavailable") from None
            finally:
                self.last_request = time.monotonic()
                self._has_requested = True

            anonymous_nav = False
            if (
                public
                and not self.creds["sessdata"]
                and not subtitle
                and post is None
                and path == "/x/web-interface/nav"
                and obj.get("code") == -101
                and isinstance(obj.get("data"), dict)
                and obj["data"].get("isLogin") is False
                and not has_voucher(obj)
            ):
                try:
                    wbi_keys(obj["data"])
                    anonymous_nav = True
                except (BiliClientError, AttributeError, TypeError, ValueError):
                    pass
            if has_voucher(obj) or (obj.get("code") in {-101, -352, -412} and not anonymous_nav):
                await self._risk(
                    "bili_cookie_expired" if obj.get("code") == -101 else "bili_account_risk"
                )
            await self._ensure_request_current()
            if subtitle:
                return obj
            if obj.get("code") != 0 and not anonymous_nav:
                raise BiliClientError("bili_account_upstream_error")
            data = obj.get("data")
            return data if isinstance(data, dict) else {}

    async def fetch_cover_thumbnail(self, cover_url: str) -> dict[str, Any] | None:
        """Fetch a bounded JPEG cover thumbnail via CN node when online."""
        if (
            not self._is_module_enabled()
            or not isinstance(cover_url, str)
            or ".." in cover_url
            or not COVER_URL_RE.fullmatch(cover_url)
            or not self._can_delegate()
        ):
            return None
        try:
            res = await self.client.node_call("bili.cover_thumb", {"url": cover_url})
        except (PluginRpcError, TimeoutError, ValueError):
            return None
        if not isinstance(res, dict) or res.get("mime_type") != "image/jpeg":
            return None
        n_bytes = res.get("bytes")
        data_b64 = res.get("data_b64")
        if (
            isinstance(n_bytes, bool)
            or not isinstance(n_bytes, int)
            or not (1 <= n_bytes <= MAX_COVER_THUMB_BYTES)
            or not isinstance(data_b64, str)
            or not data_b64
        ):
            return None
        try:
            raw = base64.b64decode(data_b64, validate=True)
        except ValueError:
            return None
        if not (1 <= len(raw) <= MAX_COVER_THUMB_BYTES):
            return None
        return dict(res)

    async def nav(self) -> dict[str, str]:
        """Validate account session via `/x/web-interface/nav` and refresh WBI keys."""
        epoch = self.credentials_epoch
        data = await self._request("/x/web-interface/nav")
        self._ensure_current(epoch=epoch)
        if data.get("isLogin") is not True:
            await self._risk("bili_cookie_expired")
        mid = str(data.get("mid", ""))
        if not re.fullmatch("[1-9][0-9]{0,19}", mid):
            raise BiliClientError("bili_account_schema_changed")
        keys = wbi_keys(data)
        async with self.policy_lock:
            self._ensure_current(epoch=epoch)
            self.keys = keys
            self.account = {"mid": mid}
            self.checked_at = self.clock()
            self.last_error = None
            await self._save_account_state()
        await self._push_runtime_status()
        return {"status": "valid", "account_mask": "…" + mid[-4:]}

    def _blocked_candidates(self, bvids: list[str], now: float) -> set[str]:
        items = self.watched_index.get("items", {})
        blocked: set[str] = set()
        for bv in bvids:
            entry = items.get(bv)
            if not entry:
                continue
            retry_after = entry.get("retry_after")
            attempts = int(entry.get("attempts") or 0)
            is_retry_ready = (
                entry.get("status") == "retryable"
                and retry_after is not None
                and float(retry_after) <= now
                and attempts < MAX_READ_ATTEMPTS
            )
            if not is_retry_ready:
                blocked.add(bv)
        return blocked

    async def candidates(self) -> list[dict[str, Any]]:
        """Collect deduplicated candidate videos from home feed and subscribed dynamic feed."""
        self._ensure_current()
        feed = await self._request(
            "/x/web-interface/wbi/index/top/feed/rcmd",
            wbi_sign(
                {
                    "ps": 12,
                    "fresh_idx": 1,
                    "fresh_type": 4,
                    "feed_version": "V8",
                    "homepage_ver": 1,
                    "version": 1,
                },
                self.keys,
                self.clock(),
            ),
        )
        dynamic = await self._request(
            "/x/polymer/web-dynamic/v1/feed/all", {"type": "video", "platform": "web"}
        )
        found: dict[str, dict[str, Any]] = {}
        for row in (feed.get("item") or [])[:10]:
            if not isinstance(row, dict) or row.get("goto") != "av":
                continue
            bv = row.get("bvid", "")
            if not isinstance(bv, str) or not re.fullmatch(BVID, bv):
                continue
            found[bv] = {
                "bvid": bv,
                "title": clean_text(str(row.get("title", "")))[:120],
                "up": clean_text(str((row.get("owner") or {}).get("name", "")))[:50],
                "category": clean_text(str(row.get("tname", "")))[:30],
                "tags": [],
            }
        for row in (dynamic.get("items") or [])[:5]:
            if not isinstance(row, dict) or row.get("type") != "DYNAMIC_TYPE_AV":
                continue
            modules = row.get("modules") or {}
            dynamic_mod = modules.get("module_dynamic") or {}
            archive = (dynamic_mod.get("major") or {}).get("archive") or {}
            bv = archive.get("bvid", "")
            if not isinstance(bv, str) or not re.fullmatch(BVID, bv):
                continue
            found[bv] = {
                "bvid": bv,
                "title": clean_text(str(archive.get("title", "")))[:120],
                "up": clean_text(str((modules.get("module_author") or {}).get("name", "")))[:50],
                "category": "关注动态",
                "tags": [],
            }
        seen = self._blocked_candidates(list(found), self.clock())
        return [v for k, v in found.items() if k not in seen][:15]

    async def material(self, bvid: str) -> tuple[int, dict[str, Any]]:
        """Collect bounded public video metadata, WBI subtitles, and hot comments."""
        self._ensure_current()
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            raise BiliClientError("invalid_bvid")
        info = await self._request("/x/web-interface/view", {"bvid": bvid})
        if info.get("bvid") != bvid:
            raise BiliClientError("bili_video_mismatch")
        aid, cid = info.get("aid"), info.get("cid")
        if any(isinstance(x, bool) or not isinstance(x, int) or x <= 0 for x in (aid, cid)):
            raise BiliClientError("bili_video_schema_changed")
        assert isinstance(aid, int)
        duration = video_duration(info.get("duration"))
        transcript, complete, subtitle_status = "", False, "missing"
        subtitles: list[Any] = []
        try:
            player = await self._request(
                "/x/player/wbi/v2",
                wbi_sign({"bvid": bvid, "cid": cid}, self.keys, self.clock()),
            )
            subtitles = (player.get("subtitle") or {}).get("subtitles") or []
        except BiliClientError as exc:
            if self.patrol_context.get() is not None or exc.code not in {
                "bili_account_unavailable",
                "bili_account_upstream_error",
            }:
                raise
            subtitles, subtitle_status = [], "unavailable"

        if subtitles:
            sub = next(
                (s for s in subtitles if str(s.get("lan", "")).startswith(("zh", "ai-zh"))),
                subtitles[0],
            )
            url = str(sub.get("subtitle_url", ""))
            url = "https:" + url if url.startswith("//") else url
            _validate_subtitle_url(url)
            try:
                body = await self._request(url, subtitle=True)
                transcript, complete, subtitle_status = subtitle_text(body, duration)
            except BiliClientError as exc:
                if self.patrol_context.get() is not None or exc.code not in {
                    "bili_account_unavailable",
                    "bili_account_upstream_error",
                    "upstream_body_too_large",
                }:
                    raise
                subtitle_status = "unavailable"

        comments: list[str] = []
        try:
            replies = await self._request(
                "/x/v2/reply", {"type": 1, "oid": aid, "sort": 1, "ps": 5, "pn": 1}
            )
            raw_replies: list[Any] = []
            top_reply = (replies.get("upper") or {}).get("top")
            if isinstance(top_reply, dict):
                raw_replies.append(top_reply)
            raw_replies.extend((replies.get("hots") or replies.get("replies") or [])[:5])
            seen_comments: set[str] = set()
            for r in raw_replies:
                if not isinstance(r, dict):
                    continue
                msg = clean_text(str((r.get("content") or {}).get("message", "")))[:250].strip()
                if msg and msg not in seen_comments:
                    seen_comments.add(msg)
                    comments.append(msg)
                if len(comments) >= 5:
                    break
        except BiliClientError as exc:
            if exc.code not in {"bili_account_unavailable", "bili_account_upstream_error"}:
                raise

        return aid, {
            "bvid": bvid,
            "title": clean_text(str(info.get("title", "")))[:150],
            "description": clean_text(str(info.get("desc", "")))[:1500],
            "duration": duration,
            "subtitle_excerpt": transcript,
            "subtitle_complete": complete,
            "subtitle_status": subtitle_status,
            "hot_comments_unverified": comments,
            "material_scope": (
                "full_subtitle"
                if complete
                else "partial_subtitle"
                if transcript
                else "metadata_only"
            ),
        }

    async def public_material(
        self, video_dict: dict[str, Any] | Any, guard: Callable[[], Any] | None = None
    ) -> dict[str, Any]:
        """Read one validated public video without enabling autonomous account patrol."""
        if isinstance(video_dict, dict):
            bvid = str(video_dict.get("bvid") or "")
            title = str(video_dict.get("title") or "")
            description = str(video_dict.get("description") or "")
        else:
            bvid = str(getattr(video_dict, "bvid", "") or "")
            title = str(getattr(video_dict, "title", "") or "")
            description = str(getattr(video_dict, "description", "") or "")

        if not re.fullmatch(BVID, bvid):
            raise BiliClientError("invalid_bvid")

        fallback: dict[str, Any] = {
            "bvid": bvid,
            "title": clean_text(title)[:150],
            "description": clean_text(description)[:1500],
            "duration": None,
            "subtitle_excerpt": "",
            "subtitle_complete": False,
            "subtitle_status": "unavailable",
            "hot_comments_unverified": [],
            "material_scope": "metadata_only",
        }
        token = self.public_read_context.set((self.credentials_epoch, guard))
        try:
            await self._ensure_request_current()
            if self.creds["sessdata"]:
                await self.nav()
            else:
                data = await self._request("/x/web-interface/nav")
                await self._ensure_request_current()
                self.keys = wbi_keys(data)
            _, material = await self.material(bvid)
            await self._ensure_request_current()
            return material
        except BiliClientError as exc:
            if exc.code not in _PUBLIC_DEGRADE_ERRORS:
                raise
            await self._ensure_request_current()
            fallback["material_error"] = exc.code
            return fallback
        except (KeyError, TypeError, ValueError, AttributeError):
            await self._ensure_request_current()
            fallback["material_error"] = "bili_public_schema_changed"
            return fallback
        finally:
            self.public_read_context.reset(token)

    def _actions_for_day(self, day: str) -> int:
        return sum(1 for item in self.actions_log if item.get("day") == day)

    def _has_action(self, bvid: str, kind: str) -> bool:
        return any(
            item.get("bvid") == bvid and item.get("kind") == kind for item in self.actions_log
        )

    async def _interaction(self, bvid: str, aid: int, pick: Pick, epoch: int) -> None:
        for kind, requested, path, data in (
            ("like", pick.like, "/x/web-interface/archive/like", {"aid": aid, "like": 1}),
            ("favorite", pick.favorite, "/x/v3/fav/resource/deal", {"rid": aid, "type": 2}),
        ):
            async with self.policy_lock:
                self._ensure_current(epoch=epoch)
                cfg = self.policy
                if (
                    not cfg.enabled
                    or not requested
                    or not getattr(cfg, "allow_" + kind)
                    or epoch != self.credentials_epoch
                ):
                    continue
                if not self.creds["bili_jct"]:
                    raise BiliClientError("bili_csrf_missing")
                budget = await self.client.ai_budget_check("patrol")
                if budget.get("hard_stop"):
                    raise BiliClientError("budget_hard_stop")
                day = self.local().date().isoformat()
                if self._actions_for_day(day) >= cfg.daily_action_limit:
                    return
                if kind == "like":
                    relation = await self._request(
                        "/x/web-interface/archive/relation", {"bvid": bvid}
                    )
                    if relation.get("like"):
                        continue
                if kind == "favorite":
                    if not self.account or not self.account.get("mid"):
                        raise BiliClientError("bili_account_schema_changed")
                    folders = await self._request(
                        "/x/v3/fav/folder/created/list-all",
                        {"up_mid": self.account["mid"], "type": 2, "rid": aid},
                    )
                    if not any(
                        str(f.get("id")) == cfg.favorite_folder_id
                        for f in (folders.get("list") or [])
                    ):
                        raise BiliClientError("bili_favorite_folder_not_owned")
                    data["add_media_ids"] = cfg.favorite_folder_id
                    data["del_media_ids"] = ""

                if self._has_action(bvid, kind):
                    continue
                if self._actions_for_day(day) >= cfg.daily_action_limit:
                    return

                entry = {
                    "day": day,
                    "bvid": bvid,
                    "kind": kind,
                    "status": "sending",
                    "created": self.clock(),
                }
                self.actions_log.insert(0, entry)
                await self._save_actions_log()

                post_data = {**data, "csrf": self.creds["bili_jct"]}
                try:
                    await self._request(path, post=post_data)
                except BaseException:
                    entry["status"] = "uncertain"
                    await self._save_actions_log()
                    raise

                entry["status"] = "done"
                await self._save_actions_log()

                idx_item = self.watched_index.get("items", {}).get(bvid)
                if idx_item is not None:
                    acts = list(idx_item.get("actions") or [])
                    if kind not in acts:
                        acts.append(kind)
                        idx_item["actions"] = sorted(acts)
                    await self._save_watched_index()

                doc = self._watched_cache.get(bvid)
                if doc is None:
                    doc = await self.client.state_get(f"watched.{bvid}")
                if isinstance(doc, dict):
                    doc_acts = list(doc.get("actions") or [])
                    if kind not in doc_acts:
                        doc_acts.append(kind)
                        doc["actions"] = sorted(doc_acts)
                    self._watched_cache[bvid] = doc
                    await self.client.state_set(f"watched.{bvid}", doc)

    def _bounded(self, payload: dict[str, Any]) -> dict[str, Any]:
        def wire_len(obj: dict[str, Any]) -> int:
            return len(json.dumps(obj, ensure_ascii=False, allow_nan=False).encode("utf-8"))

        if wire_len(payload) > self.max_context_bytes:
            for key in ("subtitle_excerpt", "description"):
                if key in payload and isinstance(payload[key], str):
                    if key == "subtitle_excerpt" and len(payload[key]) > 500:
                        payload["subtitle_complete"] = False
                        payload["subtitle_status"] = "truncated"
                        payload["material_scope"] = "partial_subtitle"
                    payload[key] = payload[key][:500]
            if "hot_comments_unverified" in payload and isinstance(
                payload["hot_comments_unverified"], list
            ):
                payload["hot_comments_unverified"] = payload["hot_comments_unverified"][:1]
        if wire_len(payload) > self.max_context_bytes and "memory_preferences" in payload:
            payload.pop("memory_preferences", None)
        if wire_len(payload) > self.max_context_bytes:
            raise BiliClientError("context_too_large")
        return payload

    def _actions_by_bvid(self, bvids: list[str]) -> dict[str, list[str]]:
        wanted = set(bvids)
        grouped: dict[str, set[str]] = {}
        for act in self.actions_log:
            if act.get("status") == "done" and act.get("bvid") in wanted and act.get("kind"):
                grouped.setdefault(str(act["bvid"]), set()).add(str(act["kind"]))
        items = self.watched_index.get("items", {})
        for bv in bvids:
            for kind in (items.get(bv) or {}).get("actions") or []:
                grouped.setdefault(bv, set()).add(str(kind))
        return {k: sorted(v) for k, v in grouped.items()}

    async def recent_surfed(
        self,
        limit: int = 4,
        max_age: float = 172800.0,
        now: float | None = None,
    ) -> list[dict[str, Any]]:
        """Return up to `limit` recently watched public patrol notes."""
        current = self.clock() if now is None else float(now)
        items = self.watched_index.get("items", {})
        eligible = [
            (bv, item)
            for bv, item in items.items()
            if item.get("status") == "done"
            and item.get("has_summary", True)
            and float(item.get("created") or 0.0) >= current - max_age
        ]
        eligible.sort(key=lambda pair: float(pair[1].get("created") or 0.0), reverse=True)
        selected = eligible[: max(1, int(limit))]
        actions_map = self._actions_by_bvid([bv for bv, _ in selected])

        results: list[dict[str, Any]] = []
        for bv, item in selected:
            doc = self._watched_cache.get(bv)
            if doc is None and self.client is not None:
                raw_doc = await self.client.state_get(f"watched.{bv}")
                if isinstance(raw_doc, dict):
                    doc = raw_doc
                    self._watched_cache[bv] = doc
            if not isinstance(doc, dict):
                continue
            raw_summary = str(doc.get("summary") or "")
            if not raw_summary:
                continue
            notes = _truncate_text(raw_summary, 220)
            if not notes:
                continue
            created = float(doc.get("created") or item.get("created") or 0.0)
            entry: dict[str, Any] = {
                "bvid": bv,
                "watched_at": datetime.fromtimestamp(created, self.zone).strftime("%m-%d %H:%M"),
                "notes": notes,
            }
            doc_acts = [str(a) for a in (doc.get("actions") or [])]
            acts = sorted(set([*doc_acts, *actions_map.get(bv, [])]))
            if acts:
                entry["actions"] = acts
            results.append(entry)
        return results

    async def lookup_watched(self, bvid: str, limit: int = 360) -> dict[str, Any] | None:
        """Look up prior patrol notes and actions for one BV ID."""
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            return None
        item = self.watched_index.get("items", {}).get(bvid)
        if item is not None and (item.get("status") != "done" or not item.get("has_summary", True)):
            return None

        doc = self._watched_cache.get(bvid)
        if doc is None and self.client is not None:
            raw_doc = await self.client.state_get(f"watched.{bvid}")
            if isinstance(raw_doc, dict):
                doc = raw_doc
                self._watched_cache[bvid] = doc
        if not isinstance(doc, dict):
            return None
        raw_summary = str(doc.get("summary") or "")
        if not raw_summary:
            return None

        created = float(doc.get("created") or (item.get("created") if item else 0.0) or 0.0)
        actions = self._actions_by_bvid([bvid]).get(bvid, [])
        doc_actions = [str(a) for a in (doc.get("actions") or [])]
        merged_actions = sorted(set([*actions, *doc_actions]))
        notes = _truncate_text(raw_summary, min(1200, max(1, int(limit))))
        if not notes:
            return None
        return {
            "already_watched_in_patrol": True,
            "watched_at": datetime.fromtimestamp(created, self.zone).strftime("%Y-%m-%d %H:%M"),
            "actions_taken": merged_actions,
            "prior_patrol_notes": notes,
        }

    def _history_counts(self) -> dict[str, int]:
        done = 0
        processing = 0
        retryable = 0
        uncertain = 0
        exhausted = 0
        for item in self.watched_index.get("items", {}).values():
            st = item.get("status")
            if st == "done" and item.get("has_summary", True):
                done += 1
            elif st in {"reading", "analyzing"}:
                processing += 1
            elif st == "retryable":
                retryable += 1
                if int(item.get("attempts") or 0) >= MAX_READ_ATTEMPTS:
                    exhausted += 1
            elif st in {"uncertain", "attempted"} or (
                st == "done" and not item.get("has_summary", True)
            ):
                uncertain += 1
        return {
            "done": done,
            "processing": processing,
            "retryable": retryable,
            "uncertain": uncertain,
            "exhausted": exhausted,
        }

    async def status(self) -> dict[str, Any]:
        """Return sanitized admin status matching built-in BiliWatcher.status()."""
        day = self.local().date().isoformat()
        runs = sorted(
            self.recent_runs,
            key=lambda r: float(r.get("created") or 0.0),
            reverse=True,
        )[:10]
        actions_today = self._actions_for_day(day)
        watched = self._history_counts()
        uncertain_actions = sum(1 for act in self.actions_log if act.get("status") == "uncertain")
        return {
            "config": self.policy.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "environment": self.env_policy.model_dump(),
            "credentials": {key: bool(value) for key, value in self.creds.items()},
            "credential_source": self.creds_source,
            "buvid4_autofill": {
                "status": (
                    "configured"
                    if self.creds["buvid4"]
                    else str(self.buvid4_device.get("status") or "not_attempted")
                    if self.buvid4_device
                    else "not_attempted"
                ),
                "attempted_at": (
                    self.buvid4_device.get("attempted_at") if self.buvid4_device else None
                ),
            },
            "account_mask": ("…" + self.account["mid"][-4:]) if self.account else None,
            "checked_at": self.checked_at,
            "cooldown_until": self.blocked_until,
            "last_error": self.last_error,
            "recent_runs": runs,
            "actions_today": actions_today,
            "watched_count": watched["done"],
            "watch_history_counts": watched,
            "uncertain_actions": uncertain_actions,
            "scheduler": "life_engine",
            "timezone": self.timezone,
            "heartbeat_supported": False,
        }

    async def _claim_bvid(self, bvid: str, run_id: str, now: float) -> bool:
        async with self.state_lock:
            items = self.watched_index.setdefault("items", {})
            existing = items.get(bvid)
            if existing is None:
                items[bvid] = {
                    "status": "reading",
                    "run_id": run_id,
                    "created": now,
                    "attempts": 1,
                    "retry_after": None,
                    "actions": [],
                    "has_summary": False,
                }
                await self._save_watched_index()
                return True

            retry_after = existing.get("retry_after")
            attempts = int(existing.get("attempts") or 0)
            if (
                existing.get("status") == "retryable"
                and retry_after is not None
                and float(retry_after) <= now
                and attempts < MAX_READ_ATTEMPTS
            ):
                existing["run_id"] = run_id
                existing["status"] = "reading"
                existing["created"] = now
                existing["attempts"] = attempts + 1
                existing["retry_after"] = None
                await self._save_watched_index()
                return True
            return False

    async def _start_analysis(self, bvid: str, run_id: str) -> bool:
        async with self.state_lock:
            item = self.watched_index.get("items", {}).get(bvid)
            if item and item.get("run_id") == run_id and item.get("status") == "reading":
                item["status"] = "analyzing"
                await self._save_watched_index()
                return True
            return False

    async def _recover_run(self, run_id: str) -> None:
        async with self.state_lock:
            now = self.clock()
            changed = False
            for item in self.watched_index.get("items", {}).values():
                if item.get("run_id") != run_id:
                    continue
                st = item.get("status")
                if st == "reading":
                    attempts = int(item.get("attempts") or 0)
                    item["status"] = "retryable" if attempts > 0 else "uncertain"
                    item["retry_after"] = (
                        max(now + READ_RETRY_SECONDS, self.blocked_until)
                        if attempts > 0 and attempts < MAX_READ_ATTEMPTS
                        else None
                    )
                    changed = True
                elif st == "analyzing":
                    item["status"] = "uncertain"
                    changed = True
            if changed:
                await self._save_watched_index()

    async def _finish(
        self, run_id: str, status: str, completed: int, reason: str | None = None
    ) -> dict[str, Any]:
        result: dict[str, Any] = {"status": status, "learned": completed, "reason": reason}
        for run in self.recent_runs:
            if run.get("id") == run_id:
                run["status"] = status
                run["reason"] = reason
                break
        await self._save_recent_runs()
        if status == "failed":
            items = self.watched_index.get("items", {})
            run_items = [(bv, item) for bv, item in items.items() if item.get("run_id") == run_id]
            learned = sum(
                1
                for _, item in run_items
                if item.get("status") == "done" and item.get("has_summary", True)
            )
            result["learned"] = learned
            run_bvids = {bv for bv, _ in run_items}
            has_uncertain = any(item.get("status") == "uncertain" for _, item in run_items) or any(
                act.get("bvid") in run_bvids and act.get("status") == "uncertain"
                for act in self.actions_log
            )
            if has_uncertain:
                result["requires_review"] = True
        return result

    async def _check_patrol_budget(self) -> None:
        budget = await self.client.ai_budget_check("patrol")
        if budget.get("hard_stop"):
            raise BiliClientError("budget_hard_stop")
        if not budget.get("available", True):
            raise BiliClientError("budget_exhausted")

    async def run_once(self, *, slot: str | None = None) -> dict[str, Any]:
        """Run one full autonomous patrol round with selection, reading, analysis, and events."""
        if not self._is_module_enabled():
            raise BiliClientError("module_disabled")
        key = uuid.uuid4().hex
        return await self._run_once(slot, key)

    async def _run_once(self, slot: str | None, key: str) -> dict[str, Any]:
        if self.run_lock.locked():
            return {"status": "busy"}
        async with self.run_lock:
            if not self._is_module_enabled():
                raise BiliClientError("module_disabled")
            if not self.policy.enabled:
                raise BiliClientError("bili_autonomous_disabled")
            if not self.creds.get("sessdata"):
                raise BiliClientError("bili_credentials_missing")
            if self.blocked_until > self.clock():
                raise BiliClientError("bili_account_cooling_down")
            await self._check_patrol_budget()

            day = self.local().date().isoformat()
            effective_slot = slot or ("manual-" + key)
            if any(
                r.get("day") == day and r.get("slot") == effective_slot for r in self.recent_runs
            ):
                return {"status": "already_attempted"}

            self.recent_runs.insert(
                0,
                {
                    "id": key,
                    "day": day,
                    "slot": effective_slot,
                    "status": "working",
                    "created": self.clock(),
                    "reason": None,
                },
            )
            await self._save_recent_runs()
            await self._push_runtime_status(current_activity="bili_patrol")

            reason: str | None = None
            completed = 0
            token = self.patrol_context.set((self.policy_epoch, self.credentials_epoch))
            try:
                epoch = self.credentials_epoch
                await self.nav()
                candidates = await self.candidates()
                self._ensure_current()
                if not candidates:
                    return await self._finish(key, "empty", 0)

                payload = self._bounded({"candidates": candidates})
                self._ensure_current()
                screen_res = await self.client.ai_screen(
                    payload, budget_lane="patrol", purpose="source"
                )
                if screen_res.get("passed") is False:
                    raise BiliClientError("ai_screen_rejected")
                self._ensure_current()
                raw = await self.client.ai_generate("bili_selection", payload, budget_lane="patrol")
                self._ensure_current()
                selection = Selection.model_validate(json.loads(raw))
                allowed = {v["bvid"] for v in candidates}
                seen: set[str] = set()
                for pick in selection.selected:
                    if pick.bvid not in allowed or pick.bvid in seen:
                        raise BiliClientError("bili_invalid_selection")
                    seen.add(pick.bvid)

                picks = selection.selected[:2]
                for idx, pick in enumerate(picks):
                    self._ensure_current()
                    if not await self._claim_bvid(pick.bvid, key, self.clock()):
                        continue
                    learning_start = self.clock()
                    if self._client_connected():
                        await self.client.report_activity(
                            kind="bili_reading", active=True, updated_at=learning_start
                        )
                    aid, material = await self.material(pick.bvid)
                    material = self._bounded(material)
                    await self._check_patrol_budget()
                    self._ensure_current()
                    if not await self._start_analysis(pick.bvid, key):
                        raise BiliClientError("bili_watch_claim_lost")

                    res = await self.client.ai_video_analyze(
                        pick.bvid, material, budget_lane="patrol"
                    )
                    raw_summary = str(res.get("text") or "")
                    mode = str(res.get("mode") or "text")
                    stop_for_risk = res.get("reason") == "risk" or self.blocked_until > self.clock()
                    label = (
                        "公开视频学习"
                        if mode == "video"
                        else "公开资料摘要"
                        if mode == "metadata"
                        else "公开文本学习"
                    )

                    self._ensure_current(epoch=epoch, allow_cooldown=stop_for_risk)
                    summary = _sanitize_summary(raw_summary, self._secrets(), limit=1000)
                    text = f"{label} · {pick.bvid}\n{summary}"
                    learned_at = self.clock()
                    title_str = str(material.get("title", ""))

                    async with self.state_lock:
                        item = self.watched_index.get("items", {}).get(pick.bvid)
                        if (
                            item is None
                            or item.get("run_id") != key
                            or item.get("status") not in {"analyzing", "uncertain"}
                        ):
                            raise BiliClientError("bili_watch_claim_lost")
                        existing_actions = list(item.get("actions") or [])
                        doc = {
                            "bvid": pick.bvid,
                            "created": learned_at,
                            "summary": text,
                            "actions": existing_actions,
                        }
                        self._watched_cache[pick.bvid] = doc
                        await self.client.state_set(f"watched.{pick.bvid}", doc)
                        item["status"] = "done"
                        item["created"] = learned_at
                        item["has_summary"] = True
                        await self._save_watched_index()

                    mood_nick = (
                        selection.mood_longnick.strip()
                        if idx == len(picks) - 1
                        and not stop_for_risk
                        and selection.mood_longnick.strip()
                        else None
                    )
                    if self._client_connected():
                        await self.client.emit_world_event(
                            "bili_video_watched",
                            {
                                "bvid": pick.bvid,
                                "learning_seconds": max(0.0, learned_at - learning_start),
                                "activity_kind": "video_learning",
                                "title": title_str[:80],
                                "summary": summary[:140],
                            },
                            importance="high" if pick.share_owner else "normal",
                            business_key=pick.bvid,
                            report_text=text,
                            mood_longnick=mood_nick,
                        )
                        await self.client.report_activity(
                            kind="bili_reading", active=False, updated_at=self.clock()
                        )
                    completed += 1
                    if stop_for_risk:
                        reason = "bili_account_risk"
                        break
                    await self._interaction(pick.bvid, aid, pick, epoch)
            except asyncio.CancelledError:
                await self._recover_run(key)
                await self._finish(key, "abandoned", completed, "bili_patrol_cancelled")
                raise
            except Exception as exc:
                await self._recover_run(key)
                if isinstance(exc, PluginRpcError) and re.fullmatch(r"[a-z_0-9]{1,80}", exc.code):
                    reason = exc.code
                else:
                    reason = "bili_patrol_failed"
                if reason not in {
                    "gemini_unavailable",
                    "gemini_circuit_open",
                    "budget_hard_stop",
                    "budget_exhausted",
                    "budget_circuit_open",
                }:
                    log.info("event=bili_account_failed reason=%s", reason)
            finally:
                self.patrol_context.reset(token)
                with contextlib.suppress(Exception):
                    if self._client_connected():
                        await self.client.report_activity(
                            kind="bili_reading", active=False, updated_at=self.clock()
                        )
                with contextlib.suppress(Exception):
                    if self._client_connected():
                        await self._push_runtime_status(current_activity=None)

            return await self._finish(key, "failed" if reason else "done", completed, reason)
