"""Bounded public-text collection and opt-in small-account patrol.

Optional video analysis is delegated to the shared VideoWatch service. No CAPTCHA
solver, comment writer, follow API or synthetic playback exists here.
WBI references: docs/BILI_ACCOUNT.md. Source content never selects a URL or tool.
"""

import asyncio
import base64
import hashlib
import inspect
import json
import logging
import math
import random
import re
import time
import uuid
from contextvars import ContextVar
from datetime import datetime
from urllib.parse import quote, urlencode, urlsplit
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from .bilibili import BVID, NODE_TRANSPORT_ERRORS, bounded_json
from .bot_server import is_persona_owner, untrusted_input
from .generation_policy import Scenario
from .models import Video
from .proactive_policy import minute_of_day
from .reply_style import trim_complete_text
from .roaming_logger import redact_public_summary
from .security import Rejected, authorize, clean_text, configured_secrets, safe_output

_MC_INSPIRATION_RE = re.compile(
    r"(?i)(?:我的世界|minecraft|(?:^|[^a-z0-9])mc(?:$|[^a-z0-9])|红石|生电|建筑教程|生存建筑|刷铁机|下界|末地|苦力怕)"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS bili_credentials (
 id INTEGER PRIMARY KEY CHECK(id=1), payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS bili_device_state (
 id INTEGER PRIMARY KEY CHECK(id=1), fingerprint TEXT NOT NULL,
 attempted_at REAL NOT NULL, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bili_account_state (
 id INTEGER PRIMARY KEY CHECK(id=1), blocked_until REAL NOT NULL DEFAULT 0,
 last_error TEXT, checked_at REAL, account BLOB
);
INSERT OR IGNORE INTO bili_account_state(id) VALUES (1);
CREATE TABLE IF NOT EXISTS bili_patrol_runs (
 id TEXT PRIMARY KEY, day TEXT NOT NULL, slot TEXT NOT NULL, status TEXT NOT NULL,
 created REAL NOT NULL, reason TEXT, UNIQUE(day,slot)
);
CREATE TABLE IF NOT EXISTS bili_watched_history (
 bvid TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL,
 summary BLOB
);
CREATE TABLE IF NOT EXISTS bili_actions (
 id TEXT PRIMARY KEY, day TEXT NOT NULL, bvid TEXT NOT NULL, kind TEXT NOT NULL,
 status TEXT NOT NULL, created REAL NOT NULL, UNIQUE(bvid,kind)
);
"""
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
CREDENTIAL_KEYS = ("sessdata", "bili_jct", "buvid3", "buvid4")
COOKIE_NAMES = {
    "sessdata": "SESSDATA",
    "bili_jct": "bili_jct",
    "buvid3": "buvid3",
    "buvid4": "buvid4",
}
USER_AGENT = "HakimiSurf/0.4 text patrol"
SPI_PATH = "/x/frontend/finger/spi"
COVER_URL_RE = re.compile(
    r"^https://(?:i[0-2]\.hdslb\.com|archive\.biliimg\.com)/bfs/[A-Za-z0-9_./-]+$"
)
MAX_COVER_THUMB_BYTES = 98_304


def secret_value(value):
    return value.get_secret_value() if hasattr(value, "get_secret_value") else str(value or "")


def validate_cookie(value):
    if (
        not isinstance(value, str)
        or len(value) > 4096
        or any(ord(c) < 33 or ord(c) > 126 or c in ";\r\n" for c in value)
    ):
        raise ValueError("Paste only the cookie value, not the complete Cookie header")
    return value


class BiliPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    enabled: bool = False
    times: list[str] = Field(default_factory=lambda: ["13:00", "20:30"], min_length=1, max_length=2)
    jitter_minutes: int = Field(default=25, ge=0, le=60)
    allow_like: bool = False
    allow_favorite: bool = False
    favorite_folder_id: str = Field(default="", pattern=r"^[1-9][0-9]{0,19}$|^$")
    daily_action_limit: int = Field(default=1, ge=1, le=2)

    @field_validator("times")
    @classmethod
    def times_valid(cls, value):
        for item in value:
            minute_of_day(item)
        if len(set(value)) != len(value):
            raise ValueError("Duplicate times")
        return sorted(value)

    @model_validator(mode="after")
    def folder_required(self):
        if self.allow_favorite and not self.favorite_folder_id:
            raise ValueError("An explicit owned favorite folder is required")
        return self


class CredentialUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)
    sessdata: str = ""
    bili_jct: str = ""
    buvid3: str = ""
    buvid4: str = ""
    clear: bool = False

    @field_validator(*CREDENTIAL_KEYS)
    @classmethod
    def valid(cls, value):
        return validate_cookie(value)


class Pick(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    bvid: str = Field(pattern="^" + BVID + "$")
    like: bool = False
    favorite: bool = False
    share_owner: bool = False


class Selection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    selected: list[Pick] = Field(max_length=2)
    mood_longnick: str = Field(default="", max_length=80)


def wbi_sign(params, keys, now):
    raw = "".join(keys)
    if not re.fullmatch("[a-fA-F0-9]{64}", raw):
        raise Rejected("bili_wbi_unavailable")
    mixin = "".join(raw[index] for index in MIXIN)[:32]
    values = {**params, "wts": str(int(now))}
    values = {k: re.sub("[!'()*]", "", str(v)) for k, v in sorted(values.items())}
    values["w_rid"] = hashlib.md5((urlencode(values, quote_via=quote) + mixin).encode()).hexdigest()
    return values


def has_voucher(value):
    if isinstance(value, dict):
        return bool(value.get("v_voucher")) or any(has_voucher(v) for v in value.values())
    if isinstance(value, list):
        return any(has_voucher(v) for v in value)
    return False


def video_duration(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return float(value) if math.isfinite(value) and value > 0 else None
    except OverflowError:
        return None


def subtitle_timestamp(value):
    if value == 0 and not isinstance(value, bool):
        return 0.0
    return video_duration(value)


def subtitle_text(body, duration):
    """Bound text without describing a clipped or untimed transcript as complete."""
    lines = body.get("body") if isinstance(body, dict) else None
    if not isinstance(lines, list) or not lines:
        return "", False, "unavailable"
    parts, starts, ends = [], [], []
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
    text = " ".join(parts)
    clipped = len(lines) > 1000 or len(text) > 12000
    text = text[:12000]
    if not text:
        return "", False, "unavailable"
    complete = bool(
        not clipped
        and valid
        and duration is not None
        and ends
        and min(starts) <= max(5, duration * 0.05)
        and max(ends) >= min(duration - 5, duration * 0.95)
    )
    return text, complete, "truncated" if clipped else "complete" if complete else "partial"


def wbi_keys(data):
    keys = []
    for name in ("img_url", "sub_url"):
        url = data.get("wbi_img", {}).get(name, "")
        key = urlsplit(url).path.rsplit("/", 1)[-1].split(".")[0]
        if not re.fullmatch("[a-fA-F0-9]{32}", key):
            raise Rejected("bili_wbi_unavailable")
        keys.append(key)
    return tuple(keys)


class BiliWatcher:
    def __init__(self, service, client=None, *, clock=time.time, sleep=asyncio.sleep, rng=None):
        self.svc, self.s, self.store = service, service.s, service.store
        self.clock, self.sleep, self.rng = clock, sleep, rng or random.SystemRandom()
        self.zone = ZoneInfo(self.s.bot_timezone)
        self.env_policy = BiliPolicy(
            enabled=getattr(self.s, "bili_autonomous_enabled", False),
            times=getattr(self.s, "bili_patrol_times", ["13:00", "20:30"]),
        )
        self.policy, self.overridden = self.env_policy, False
        self.creds = {
            key: secret_value(getattr(self.s, "bili_" + key, "")) for key in CREDENTIAL_KEYS
        }
        # Historical setting name requested by the user is bili_bili_jct.
        self.creds["bili_jct"] = secret_value(
            getattr(self.s, "bili_bili_jct", self.creds["bili_jct"])
        )
        self.creds_source = "env"
        for value in self.creds.values():
            validate_cookie(value)
        proxy = secret_value(getattr(self.s, "bili_proxy", ""))
        if proxy and urlsplit(proxy).scheme not in {"http", "https"}:
            raise ValueError("BILI_PROXY must use http(s)")
        self.owns_client = client is None
        self.http = client or httpx.AsyncClient(
            timeout=15,
            trust_env=False,
            follow_redirects=False,
            proxy=proxy or None,
            limits=httpx.Limits(max_connections=1),
        )
        self.network_lock, self.run_lock = asyncio.Lock(), asyncio.Lock()
        self.policy_lock = asyncio.Lock()
        self.last_request = 0.0
        self.blocked_until = 0.0
        self.keys = None
        self.account = None
        self.credentials_epoch = 0
        self.policy_epoch = 0
        # A manual nav test has no patrol context and remains available when patrol is off.
        # Each running round carries its original policy/credential versions across awaits.
        self.patrol_context = ContextVar("bili_patrol_context", default=None)
        self.public_read_context = ContextVar("bili_public_read_context", default=None)

    async def initialize(self):
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
            row = await self.store.one("SELECT payload FROM bili_credentials WHERE id=1")
            if row:
                saved = self.store.vault.open("bili-credentials", row[0])
                self.creds = {key: saved.get(key, "") for key in CREDENTIAL_KEYS}
                for value in self.creds.values():
                    validate_cookie(value)
                self.creds_source = "sqlite"
            policy = await self.store.one(
                "SELECT payload FROM admin_config WHERE key='bili_account'"
            )
            if policy:
                self.policy = BiliPolicy.model_validate(
                    self.store.vault.open("admin:bili_account", policy[0])
                )
                self.overridden = True
            state = await self.store.one("SELECT * FROM bili_account_state WHERE id=1")
            self.blocked_until = state["blocked_until"]
            self.account = (
                self.store.vault.open("bili-account", state["account"])
                if state["account"]
                else None
            )
            await self.store.db.execute(
                "UPDATE bili_patrol_runs SET status='abandoned' WHERE status='working'"
            )
            await self.store.db.execute(
                "UPDATE bili_actions SET status='uncertain' WHERE status='sending'"
            )
        self._protect_secrets()

    def _protect_secrets(self):
        # Existing output guard / incoming secret detector are shared by all features.
        for key, value in self.creds.items():
            setattr(self.s, "bili_" + key, SecretStr(value))
            if value and value not in self.svc.secrets:
                self.svc.secrets.append(value)

    async def close(self):
        if self.owns_client:
            await self.http.aclose()

    def local(self, now=None):
        return datetime.fromtimestamp(self.clock() if now is None else now, self.zone)

    def _ensure_current(self, *, epoch=None, allow_cooldown=False):
        if not allow_cooldown and self.blocked_until > self.clock():
            raise Rejected("bili_account_cooling_down")
        if epoch is not None and epoch != self.credentials_epoch:
            raise Rejected("bili_settings_changed")
        public = self.public_read_context.get()
        if public is not None and public[0] != self.credentials_epoch:
            raise Rejected("bili_settings_changed")
        context = self.patrol_context.get()
        if context is not None and (
            not self.policy.enabled or context != (self.policy_epoch, self.credentials_epoch)
        ):
            raise Rejected("bili_settings_changed")

    async def _ensure_request_current(self):
        self._ensure_current()
        public = self.public_read_context.get()
        if public is not None and public[1] is not None:
            try:
                result = public[1]()
                if inspect.isawaitable(result):
                    result = await result
                if result is False:
                    raise Rejected("bili_request_revoked")
            except asyncio.CancelledError:
                raise
            except Exception:
                raise Rejected("bili_request_revoked") from None
        self._ensure_current()

    async def set_policy(self, value):
        async with self.policy_lock:
            if value is None:
                async with self.store.transaction():
                    await self.store.db.execute("DELETE FROM admin_config WHERE key='bili_account'")
                self.policy, self.overridden = self.env_policy, False
            else:
                await self.store.runtime.save_config("bili_account", value.model_dump())
                self.policy, self.overridden = value, True
            self.policy_epoch += 1
        return await self.status()

    async def update_credentials(self, update):
        async with self.policy_lock, self.network_lock:
            value = (
                {key: "" for key in CREDENTIAL_KEYS}
                if update.clear
                else {key: getattr(update, key) or self.creds[key] for key in CREDENTIAL_KEYS}
            )
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO bili_credentials VALUES (1,?)",
                    (self.store.vault.seal("bili-credentials", value),),
                )
                await self.store.db.execute(
                    "UPDATE bili_account_state SET checked_at=NULL,account=NULL WHERE id=1"
                )
                # An explicit credential update authorizes one new device lookup attempt.
                await self.store.db.execute("DELETE FROM bili_device_state")
            self.creds, self.creds_source, self.keys, self.account = value, "sqlite", None, None
            self.credentials_epoch += 1
            self._protect_secrets()
        return await self.status()

    def _node_transport(self):
        return getattr(self.svc, "node_transport", None)

    def _can_delegate(self) -> bool:
        nt = self._node_transport()
        return bool(
            nt is not None
            and nt.connected()
            and getattr(self.s, "node_bili_delegate_enabled", True)
        )

    async def _delegated_bili_request(
        self,
        path: str,
        params: dict | None = None,
        *,
        post: dict | None = None,
        subtitle: bool = False,
        cookies: dict[str, str],
    ) -> dict | None:
        """Run one bili.request RPC via CN node; return None when local fallback applies."""
        nt = self._node_transport()
        try:
            res = await nt.call(
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
        except Rejected as exc:
            code = str(exc)
            if code in NODE_TRANSPORT_ERRORS:
                if (
                    post is None or code in {"node_disconnected", "node_not_dispatched"}
                ) and getattr(self.s, "node_bili_fallback_local", True):
                    return None
                raise Rejected("bili_account_unavailable") from None
            if code == "bili_http_status":
                if getattr(exc, "detail", "") in {"401", "403", "412", "429"}:
                    await self._risk("bili_account_risk")
                raise Rejected("bili_account_unavailable") from None
            raise Rejected("bili_account_unavailable") from None
        except (httpx.HTTPError, ValueError):
            raise Rejected("bili_account_unavailable") from None
        envelope = res.get("envelope") if isinstance(res, dict) else None
        if not isinstance(envelope, dict):
            raise Rejected("bili_account_unavailable")
        return envelope

    def _headers(self, *, subtitle=False):
        return {
            "Referer": "https://www.bilibili.com/",
            "User-Agent": USER_AGENT,
            "Cookie": ""
            if subtitle
            else "; ".join(
                COOKIE_NAMES[key] + "=" + value for key, value in self.creds.items() if value
            ),
        }

    async def _pace_request(self):
        if self.last_request:
            await self.sleep(
                max(0, self.rng.uniform(2, 6) - (time.monotonic() - self.last_request))
            )
        await self._ensure_request_current()
        self.last_request = time.monotonic()

    async def _ensure_buvid4_locked(self):
        """Best-effort device lookup, called only under the account network lock.

        Persist the attempt before dispatch so restarts cannot turn failures into polling.
        This is a standard public device-id endpoint, never a CAPTCHA-solving fallback.
        """
        if self.creds["buvid4"] or not (self.creds["sessdata"] and self.creds["bili_jct"]):
            return
        await self._ensure_request_current()
        fingerprint = self.store.vault.digest(
            "bili-device-credentials", *(self.creds[key] for key in CREDENTIAL_KEYS[:-1])
        )
        async with self.store.transaction():
            old = await self.store.one("SELECT fingerprint FROM bili_device_state WHERE id=1")
            if old and old[0] == fingerprint:
                return
            await self.store.db.execute(
                "INSERT OR REPLACE INTO bili_device_state VALUES (1,?,?,'attempted')",
                (fingerprint, self.clock()),
            )
        await self._pace_request()
        status, value = "unavailable", ""
        try:
            obj = None
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
                    follow_redirects=False,
                )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {401, 403, 412, 429}:
                await self._risk("bili_account_risk")
            obj = None
        except Rejected as exc:
            if str(exc) in {"bili_account_risk", "bili_cookie_expired"}:
                raise
            # Bad availability or payloads must not make a missing optional cookie mandatory.
            obj = None
        except (httpx.HTTPError, ValueError, RecursionError):
            # Bad availability or payloads must not make a missing optional cookie mandatory.
            obj = None
        finally:
            self.last_request = time.monotonic()
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
        # update_credentials also holds network_lock, so no old response can replace
        # a newer credential set; policy/authorization revocation is checked above.
        async with self.store.transaction():
            if value:
                updated = {**self.creds, "buvid4": value}
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO bili_credentials VALUES (1,?)",
                    (self.store.vault.seal("bili-credentials", updated),),
                )
            await self.store.db.execute(
                "UPDATE bili_device_state SET status=? WHERE id=1 AND fingerprint=?",
                (status, fingerprint),
            )
        if value:
            self.creds, self.creds_source = updated, "sqlite"
            self._protect_secrets()

    async def download_credentials(self, guard=None):
        """Return the same refreshed cookie snapshot for a bounded original-video download."""

        async def valid():
            await self._ensure_request_current()
            if guard is not None:
                result = guard()
                if inspect.isawaitable(result):
                    result = await result
                if result is False:
                    raise Rejected("bili_request_revoked")
            await self._ensure_request_current()

        await valid()
        async with self.network_lock:
            await valid()
            await self._ensure_buvid4_locked()
            await valid()
            return dict(self.creds)

    async def _risk(self, reason):
        self.blocked_until = self.clock() + 43200
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE bili_account_state SET blocked_until=?,last_error=? WHERE id=1",
                (self.blocked_until, reason),
            )
        try:
            await self.svc.proactive.alert(
                "bili_account_" + reason,
                "BOISER，小号巡逻遇到凭证过期或平台风控，已暂停 12 小时喵。"
                "请先登录 B 站检查状态，再在后台更新凭证喵。",
            )
        except Exception:
            logging.getLogger("hakimi").info("event=bili_account_alert_failed")
        raise Rejected(reason)

    async def _request(self, path, params=None, *, post=None, subtitle=False):
        public = self.public_read_context.get() is not None
        if public and (post is not None or (not subtitle and path not in PUBLIC_READ_PATHS)):
            raise Rejected("bili_public_read_only")
        if subtitle:
            url = urlsplit(path)
            if (
                post is not None
                or url.scheme != "https"
                or url.hostname
                not in {"aisubtitle.hdslb.com", "i0.hdslb.com", "i1.hdslb.com", "i2.hdslb.com"}
                or url.port not in {None, 443}
                or url.username
                or url.password
                or not url.path.startswith("/bfs/")
            ):
                raise Rejected("bili_subtitle_host_forbidden")
        if not subtitle and (path not in (POST_PATHS if post is not None else GET_PATHS)):
            raise Rejected("bili_endpoint_forbidden")
        await self._ensure_request_current()
        async with self.network_lock:
            await self._ensure_request_current()
            if not self.creds["sessdata"] and not public:
                raise Rejected("bili_credentials_missing")
            if not subtitle:
                await self._ensure_buvid4_locked()
            await self._pace_request()
            headers = self._headers(subtitle=subtitle)
            url = path if subtitle else "https://api.bilibili.com" + path
            try:
                obj = None
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
                        follow_redirects=False,
                    )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {401, 403, 412, 429}:
                    await self._risk("bili_account_risk")
                raise Rejected("bili_account_unavailable") from None
            except (httpx.HTTPError, ValueError):
                raise Rejected("bili_account_unavailable") from None
            finally:
                self.last_request = time.monotonic()
            # Logged-out nav can still expose its public WBI image keys. This is
            # not an expired credential when no session cookie was sent. Never
            # generalize the exception to authenticated requests or another API.
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
                except (Rejected, AttributeError, TypeError, ValueError):
                    pass
            if has_voucher(obj) or (obj.get("code") in {-101, -352, -412} and not anonymous_nav):
                await self._risk(
                    "bili_cookie_expired" if obj.get("code") == -101 else "bili_account_risk"
                )
            await self._ensure_request_current()
            if subtitle:
                return obj
            if obj.get("code") != 0 and not anonymous_nav:
                raise Rejected("bili_account_upstream_error")
            return obj.get("data") or {}

    async def fetch_cover_thumbnail(self, cover_url: str) -> dict | None:
        """Fetch a bounded JPEG cover thumbnail via CN node when online."""
        if (
            not isinstance(cover_url, str)
            or ".." in cover_url
            or not COVER_URL_RE.fullmatch(cover_url)
            or not self._can_delegate()
        ):
            return None
        nt = self._node_transport()
        try:
            res = await nt.call("bili.cover_thumb", {"url": cover_url})
        except (Rejected, TimeoutError, ValueError):
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

    async def nav(self):
        epoch = self.credentials_epoch
        data = await self._request("/x/web-interface/nav")
        self._ensure_current(epoch=epoch)
        if data.get("isLogin") is not True:
            await self._risk("bili_cookie_expired")
        mid = str(data.get("mid", ""))
        if not re.fullmatch("[1-9][0-9]{0,19}", mid):
            raise Rejected("bili_account_schema_changed")
        keys = wbi_keys(data)
        async with self.policy_lock:
            self._ensure_current(epoch=epoch)
            self.keys = keys
            self.account = {"mid": mid}
            async with self.store.transaction():
                await self.store.db.execute(
                    "UPDATE bili_account_state SET checked_at=?,account=?,last_error=NULL "
                    "WHERE id=1",
                    (self.clock(), self.store.vault.seal("bili-account", self.account)),
                )
            return {"status": "valid", "account_mask": "…" + mid[-4:]}

    async def public_material(self, video: Video, guard=None):
        """Read one validated public video without enabling autonomous account patrol."""
        fallback = {
            "bvid": video.bvid,
            "title": clean_text(video.title)[:150],
            "description": clean_text(video.description)[:1500],
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
            _, material = await self.material(video.bvid)
            await self._ensure_request_current()
            return material
        except Rejected as exc:
            # Only source availability failures may degrade; revocation/risk must stop work.
            if str(exc) not in {
                "bili_account_unavailable",
                "bili_account_upstream_error",
                "bili_video_schema_changed",
                "bili_video_mismatch",
                "bili_wbi_unavailable",
                "bili_subtitle_host_forbidden",
                "redirect_not_allowed",
                "upstream_body_too_large",
            }:
                raise
            await self._ensure_request_current()
            fallback["material_error"] = str(exc)
            return fallback
        except (KeyError, TypeError, ValueError, AttributeError):
            await self._ensure_request_current()
            fallback["material_error"] = "bili_public_schema_changed"
            return fallback
        finally:
            self.public_read_context.reset(token)

    async def candidates(self):
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
        found = {}
        for row in feed.get("item", [])[:10]:
            if row.get("goto") != "av":
                continue
            bv = row.get("bvid", "")
            if not re.fullmatch(BVID, bv):
                continue
            found[bv] = {
                "bvid": bv,
                "title": clean_text(str(row.get("title", "")))[:120],
                "up": clean_text(str((row.get("owner") or {}).get("name", "")))[:50],
                "category": clean_text(str(row.get("tname", "")))[:30],
                "tags": [],
            }
        for row in dynamic.get("items", [])[:5]:
            if row.get("type") != "DYNAMIC_TYPE_AV":
                continue
            modules = row.get("modules") or {}
            archive = ((modules.get("module_dynamic") or {}).get("major") or {}).get(
                "archive"
            ) or {}
            bv = archive.get("bvid", "")
            if not re.fullmatch(BVID, bv):
                continue
            found[bv] = {
                "bvid": bv,
                "title": clean_text(str(archive.get("title", "")))[:120],
                "up": clean_text(str((modules.get("module_author") or {}).get("name", "")))[:50],
                "category": "关注动态",
                "tags": [],
            }
        async with self.store.lock:
            seen = {r[0] for r in await self.store.all("SELECT bvid FROM bili_watched_history")}
        return [v for k, v in found.items() if k not in seen][:15]

    async def material(self, bvid):
        self._ensure_current()
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            raise Rejected("invalid_bvid")
        info = await self._request("/x/web-interface/view", {"bvid": bvid})
        if info.get("bvid") != bvid:
            raise Rejected("bili_video_mismatch")
        aid, cid = info.get("aid"), info.get("cid")
        if any(isinstance(x, bool) or not isinstance(x, int) or x <= 0 for x in (aid, cid)):
            raise Rejected("bili_video_schema_changed")
        duration = video_duration(info.get("duration"))
        transcript, complete, subtitle_status = "", False, "missing"
        try:
            player = await self._request(
                "/x/player/wbi/v2", wbi_sign({"bvid": bvid, "cid": cid}, self.keys, self.clock())
            )
            subtitles = (player.get("subtitle") or {}).get("subtitles") or []
        except Rejected as exc:
            if str(exc) not in {"bili_account_unavailable", "bili_account_upstream_error"}:
                raise
            subtitles, subtitle_status = [], "unavailable"
        if subtitles:
            sub = next(
                (s for s in subtitles if str(s.get("lan", "")).startswith(("zh", "ai-zh"))),
                subtitles[0],
            )
            url = str(sub.get("subtitle_url", ""))
            url = "https:" + url if url.startswith("//") else url
            u = urlsplit(url)
            if (
                u.scheme != "https"
                or u.hostname
                not in {"aisubtitle.hdslb.com", "i0.hdslb.com", "i1.hdslb.com", "i2.hdslb.com"}
                or u.port not in {None, 443}
                or u.username
                or u.password
                or not u.path.startswith("/bfs/")
            ):
                raise Rejected("bili_subtitle_host_forbidden")
            try:
                body = await self._request(url, subtitle=True)
                transcript, complete, subtitle_status = subtitle_text(body, duration)
            except Rejected as exc:
                if str(exc) not in {
                    "bili_account_unavailable",
                    "bili_account_upstream_error",
                    "upstream_body_too_large",
                }:
                    raise
                subtitle_status = "unavailable"
        comments = []
        try:
            replies = await self._request(
                "/x/v2/reply", {"type": 1, "oid": aid, "sort": 1, "ps": 5, "pn": 1}
            )
            raw_replies = []
            top_reply = (replies.get("upper") or {}).get("top")
            if isinstance(top_reply, dict):
                raw_replies.append(top_reply)
            raw_replies.extend((replies.get("hots") or replies.get("replies") or [])[:5])
            seen_comments = set()
            for r in raw_replies:
                if not isinstance(r, dict):
                    continue
                msg = clean_text(str((r.get("content") or {}).get("message", "")))[:250].strip()
                if msg and msg not in seen_comments:
                    seen_comments.add(msg)
                    comments.append(msg)
                if len(comments) >= 5:
                    break
        except Rejected as exc:
            if str(exc) not in {"bili_account_unavailable", "bili_account_upstream_error"}:
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
            "material_scope": "full_subtitle"
            if complete
            else "partial_subtitle"
            if transcript
            else "metadata_only",
        }

    async def _interaction(self, bvid, aid, pick, epoch):
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
                    raise Rejected("bili_csrf_missing")
                if (await self.store.budget_status())["hard_stop"]:
                    raise Rejected("budget_hard_stop")
                day = self.local().date().isoformat()
                async with self.store.lock:
                    used = (
                        await self.store.one(
                            "SELECT COUNT(*) FROM bili_actions WHERE day=?", (day,)
                        )
                    )[0]
                if used >= cfg.daily_action_limit:
                    return
                if kind == "like":
                    relation = await self._request(
                        "/x/web-interface/archive/relation", {"bvid": bvid}
                    )
                    if relation.get("like"):
                        continue
                # Verify the designated folder belongs to this account before adding.
                if kind == "favorite":
                    folders = await self._request(
                        "/x/v3/fav/folder/created/list-all",
                        {"up_mid": self.account["mid"], "type": 2, "rid": aid},
                    )
                    if not any(
                        str(f.get("id")) == cfg.favorite_folder_id for f in folders.get("list", [])
                    ):
                        raise Rejected("bili_favorite_folder_not_owned")
                    data["add_media_ids"] = cfg.favorite_folder_id
                    data["del_media_ids"] = ""
                key = self.store.vault.digest("bili-action", bvid, kind)
                day = self.local().date().isoformat()
                async with self.store.transaction():
                    if await self.store.one("SELECT 1 FROM bili_actions WHERE id=?", (key,)):
                        continue
                    count = (
                        await self.store.one(
                            "SELECT COUNT(*) FROM bili_actions WHERE day=?", (day,)
                        )
                    )[0]
                    if count >= cfg.daily_action_limit:
                        return
                    await self.store.db.execute(
                        "INSERT INTO bili_actions VALUES (?,?,?,?,'sending',?)",
                        (key, day, bvid, kind, self.clock()),
                    )
                data["csrf"] = self.creds["bili_jct"]
                try:
                    await self._request(path, post=data)
                except BaseException:
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE bili_actions SET status='uncertain' WHERE id=?", (key,)
                        )
                    raise
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE bili_actions SET status='done' WHERE id=?", (key,)
                    )

    def _bounded(self, payload):
        # Fields are bounded before screening; the generator receives exactly those fields.
        if len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            for key in ("subtitle_excerpt", "description"):
                if key in payload:
                    if key == "subtitle_excerpt" and len(payload[key]) > 500:
                        payload["subtitle_complete"] = False
                        payload["subtitle_status"] = "truncated"
                        payload["material_scope"] = "partial_subtitle"
                    payload[key] = payload[key][:500]
            if "hot_comments_unverified" in payload:
                payload["hot_comments_unverified"] = payload["hot_comments_unverified"][:1]
        if (
            len(untrusted_input(payload).encode()) > self.s.max_context_bytes
            and "memory_preferences" in payload
        ):
            payload.pop("memory_preferences", None)
        if len(untrusted_input(payload).encode()) > self.s.max_context_bytes:
            raise Rejected("context_too_large")
        return payload

    async def _memory_preferences(self) -> dict:
        sessions = getattr(self.svc, "sessions", None)
        runtime = getattr(self.store, "runtime", None)
        if sessions is None or runtime is None or not hasattr(runtime, "sessions"):
            return {}
        secrets = getattr(self.svc, "secrets", None) or configured_secrets(self.s)
        owner_interests: list[str] = []
        community_topics: list[str] = []
        recent_topics: list[str] = []
        now = self.clock()
        try:
            session_list = await runtime.sessions()
        except Exception:
            return {}
        for _, msg in session_list[:20]:
            try:
                if not authorize(msg.model_copy(update={"addressed": True}), self.s):
                    continue
                view = await sessions.dual.view(msg)
            except Exception:
                continue
            entries = [
                e
                for e in view.get("entries", [])
                if not e.get("deleted") and (not e.get("expires") or e["expires"] > now)
            ]
            owner_private = msg.kind == "private" and is_persona_owner(msg, self.s)
            if owner_private:
                for entry in entries:
                    if (
                        entry.get("kind") == "profile"
                        and entry.get("slot") in {"preferences", "state"}
                    ) or entry.get("kind") == "chronicle":
                        topic = f"{entry.get('topic')}: " if entry.get("topic") else ""
                        text = redact_public_summary(
                            safe_output(f"{topic}{entry.get('text', '')}", secrets, limit=140)
                        )
                        if text and text not in owner_interests:
                            owner_interests.append(text)
                        if len(owner_interests) >= 8:
                            break
            else:
                profiles = [
                    e
                    for e in entries
                    if e.get("kind") == "profile" and e.get("slot") == "preferences"
                ]
                chronicles = sorted(
                    (e for e in entries if e.get("kind") == "chronicle"),
                    key=lambda item: int(item.get("heat", 1)),
                    reverse=True,
                )
                for entry in [*profiles[:4], *chronicles[:6]]:
                    topic = f"{entry.get('topic')}: " if entry.get("topic") else ""
                    text = redact_public_summary(
                        safe_output(f"{topic}{entry.get('text', '')}", secrets, limit=120)
                    )
                    if text and text not in community_topics and len(community_topics) < 10:
                        community_topics.append(text)
            if len(owner_interests) >= 8 and len(community_topics) >= 10:
                break
        if not owner_interests and not community_topics:
            for _, msg in session_list[:10]:
                try:
                    if not authorize(msg.model_copy(update={"addressed": True}), self.s):
                        continue
                    snap = await sessions.snapshot(msg)
                except Exception:
                    continue
                for item in reversed(snap.get("recent_messages", [])[-6:]):
                    if item.get("role") != "user":
                        continue
                    text = redact_public_summary(
                        safe_output(str(item.get("text", "")), secrets, limit=80)
                    )
                    if text and text not in recent_topics:
                        recent_topics.append(text)
                    if len(recent_topics) >= 5:
                        break
                if len(recent_topics) >= 5:
                    break
        result = {}
        if owner_interests:
            result["owner_interests"] = owner_interests[:8]
        if community_topics:
            result["community_topics"] = community_topics[:10]
        elif recent_topics:
            result["recent_chat_topics"] = recent_topics[:5]
        return result

    async def _actions_by_bvid(self, bvids: list[str]) -> dict[str, list[str]]:
        if not bvids:
            return {}
        has_actions = await self.store.one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bili_actions'"
        )
        if not has_actions:
            return {}
        placeholders = ",".join("?" for _ in bvids)
        rows = await self.store.all(
            f"SELECT bvid,kind FROM bili_actions WHERE status='done' AND bvid IN ({placeholders})",
            tuple(bvids),
        )
        grouped: dict[str, list[str]] = {}
        for row in rows:
            grouped.setdefault(row[0], []).append(str(row[1]))
        return {k: sorted(set(v)) for k, v in grouped.items()}

    def _open_watched_notes(self, bvid: str, sealed, *, limit: int) -> str:
        secrets = getattr(self.svc, "secrets", None) or configured_secrets(self.s)
        raw = self.store.vault.open("bili-learn:" + bvid, sealed)
        return redact_public_summary(safe_output(str(raw), secrets, limit=limit))

    async def recent_surfed(
        self, *, limit: int = 4, max_age: float = 172800.0, now: float | None = None
    ) -> list[dict]:
        current = self.clock() if now is None else now
        async with self.store.lock:
            has_bili = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bili_watched_history'"
            )
            if not has_bili:
                return []
            rows = await self.store.all(
                "SELECT bvid,created,summary FROM bili_watched_history "
                "WHERE status='done' AND summary IS NOT NULL AND created>=? "
                "ORDER BY created DESC LIMIT ?",
                (current - max_age, max(1, int(limit))),
            )
            actions = await self._actions_by_bvid([r[0] for r in rows])
        items: list[dict] = []
        for bvid, created, sealed in rows:
            try:
                notes = self._open_watched_notes(bvid, sealed, limit=220)
            except Exception:
                continue
            if not notes:
                continue
            entry = {
                "bvid": bvid,
                "watched_at": datetime.fromtimestamp(created, self.zone).strftime("%m-%d %H:%M"),
                "notes": notes,
            }
            if actions.get(bvid):
                entry["actions"] = actions[bvid]
            items.append(entry)
        return items

    async def lookup_watched(self, bvid: str) -> dict | None:
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            return None
        async with self.store.lock:
            has_bili = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bili_watched_history'"
            )
            if not has_bili:
                return None
            row = await self.store.one(
                "SELECT created,summary FROM bili_watched_history "
                "WHERE bvid=? AND status='done' AND summary IS NOT NULL",
                (bvid,),
            )
            if not row:
                return None
            actions = await self._actions_by_bvid([bvid])
        try:
            notes = self._open_watched_notes(bvid, row[1], limit=360)
        except Exception:
            return None
        return {
            "already_watched_in_patrol": True,
            "watched_at": datetime.fromtimestamp(row[0], self.zone).strftime("%Y-%m-%d %H:%M"),
            "actions_taken": actions.get(bvid, []),
            "prior_patrol_notes": notes,
        }

    async def _maybe_share_owner(self, day: str, bvid: str, title: str, summary: str) -> bool:
        proactive = getattr(self.svc, "proactive", None)
        now = self.clock()
        if (
            not self.s.owner_qq_id
            or proactive is None
            or not hasattr(proactive, "config")
            or not hasattr(proactive, "enqueue")
            or not getattr(proactive, "configured", True)
            or not proactive.config.enabled
            or self.store.runtime.is_resting(now)
            or proactive.config.quiet(self.local(now))
        ):
            return False
        share_key = self.store.vault.digest("bili-instant-share", self.s.namespace, day)
        async with self.store.lock:
            has_dms = await self.store.one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pending_owner_dms'"
            )
            if has_dms and await self.store.one(
                "SELECT 1 FROM pending_owner_dms WHERE id=?", (share_key,)
            ):
                return False
        secrets = getattr(self.svc, "secrets", None) or configured_secrets(self.s)
        clean_title = redact_public_summary(safe_output(title, secrets, limit=80)) if title else ""
        title_part = f"《{clean_title}》" if clean_title else bvid
        header = f"诶我刚摸鱼刷B站刷到个超对胃口的视频{title_part}，感觉你会感兴趣！\n"
        footer = f"\nhttps://www.bilibili.com/video/{bvid}"
        body_budget = max(120, self.s.send_max_chars - len(header) - len(footer) - 8)
        body = trim_complete_text(summary.strip(), body_budget)
        share_text = f"{header}{body}{footer}"
        expires = (
            proactive._companion_expiry(now)
            if hasattr(proactive, "_companion_expiry")
            else now + 7200
        )
        try:
            await proactive.enqueue("patrol", share_text, key=share_key, expires=expires)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    def slot_due_minute(self, day: str, slot: str) -> int:
        base = minute_of_day(slot)
        jitter = self.policy.jitter_minutes
        if jitter <= 0:
            return base
        digest = hashlib.blake2b(
            f"{self.s.namespace}:{day}:{slot}".encode(), digest_size=4
        ).digest()
        span = 2 * jitter + 1
        offset = (int.from_bytes(digest, "big") % span) - jitter
        return max(0, min(1439, base + offset))

    async def run_once(self, *, slot="manual"):
        if self.run_lock.locked():
            return {"status": "busy"}
        async with self.run_lock:
            if not self.policy.enabled:
                raise Rejected("bili_autonomous_disabled")
            if not self.creds["sessdata"]:
                raise Rejected("bili_credentials_missing")
            if self.blocked_until > self.clock():
                raise Rejected("bili_account_cooling_down")
            if (await self.store.budget_status())["hard_stop"]:
                raise Rejected("budget_hard_stop")
            day = self.local().date().isoformat()
            key = uuid.uuid4().hex
            async with self.store.transaction():
                # Manual and scheduled work share a hard maximum of two attempted rounds/day.
                count = (
                    await self.store.one(
                        "SELECT COUNT(*) FROM bili_patrol_runs WHERE day=?", (day,)
                    )
                )[0]
                if count >= len(self.policy.times):
                    return {"status": "daily_limit"}
                cur = await self.store.db.execute(
                    "INSERT OR IGNORE INTO bili_patrol_runs VALUES (?,?,?,'working',?,NULL)",
                    (key, day, slot, self.clock()),
                )
                if cur.rowcount != 1:
                    return {"status": "already_attempted"}
            reason = None
            completed = 0
            shared_today = False
            token = self.patrol_context.set((self.policy_epoch, self.credentials_epoch))
            try:
                epoch = self.credentials_epoch
                await self.nav()
                candidates = await self.candidates()
                self._ensure_current()
                if not candidates:
                    return await self._finish(key, "empty", 0)
                prefs = await self._memory_preferences()
                raw_payload = {
                    "candidates": candidates,
                    **({"memory_preferences": prefs} if prefs else {}),
                }
                payload = self._bounded(raw_payload)
                subject = self.store.vault.digest("bili-account-budget", self.s.namespace)
                self._ensure_current()
                await self.svc.ai.screen(payload, subject, purpose="source", budget_lane="patrol")
                self._ensure_current()
                raw = await self.svc.ai.generate(
                    payload, subject, scenario=Scenario.BILI_SELECTION, budget_lane="patrol"
                )
                self._ensure_current()
                selection = Selection.model_validate(json.loads(raw))
                allowed = {v["bvid"] for v in candidates}
                seen = set()
                for pick in selection.selected:
                    if pick.bvid not in allowed or pick.bvid in seen:
                        raise Rejected("bili_invalid_selection")
                    seen.add(pick.bvid)
                for pick in selection.selected:
                    self._ensure_current()
                    async with self.store.transaction():
                        cur = await self.store.db.execute(
                            "INSERT OR IGNORE INTO bili_watched_history "
                            "VALUES (?,?,'attempted',?,NULL)",
                            (pick.bvid, key, self.clock()),
                        )
                        if cur.rowcount != 1:
                            continue
                    aid, material = await self.material(pick.bvid)
                    material = self._bounded(material)
                    self._ensure_current()
                    analyzer = getattr(self.svc, "video_watch", None)
                    stop_for_risk = False
                    if analyzer is not None:
                        result = await analyzer.analyze(
                            pick.bvid,
                            material,
                            subject,
                            "patrol",
                            guard=lambda: self._ensure_current(allow_cooldown=True),
                        )
                        summary = result.text
                        stop_for_risk = result.reason == "risk" or self.blocked_until > self.clock()
                        label = (
                            "公开视频学习"
                            if result.mode == "video"
                            else "公开资料摘要"
                            if result.mode == "metadata"
                            else "公开文本学习"
                        )
                    else:
                        await self.svc.ai.screen(
                            material, subject, purpose="source", budget_lane="patrol"
                        )
                        self._ensure_current()
                        summary = await self.svc.ai.generate(
                            material,
                            subject,
                            scenario=Scenario.BILI_LEARNING,
                            budget_lane="patrol",
                        )
                        label = "公开文本学习"
                    # Persist the fallback summary, then stop Bili requests on risk.
                    # The fallback model used only material screened before download.
                    self._ensure_current(epoch=epoch, allow_cooldown=stop_for_risk)
                    summary = redact_public_summary(
                        safe_output(summary, self.svc.secrets, limit=1000)
                    )
                    text = f"{label} · {pick.bvid}\n{summary}"
                    async with self.store.transaction():
                        await self.store.db.execute(
                            "UPDATE bili_watched_history SET status='done',summary=? WHERE bvid=?",
                            (self.store.vault.seal("bili-learn:" + pick.bvid, text), pick.bvid),
                        )
                    ws_mgr = getattr(self.svc, "world_state", None)
                    if ws_mgr is not None:
                        if hasattr(ws_mgr, "store") and hasattr(ws_mgr, "initialize"):
                            async with self.store.lock:
                                has_events_table = await self.store.one(
                                    "SELECT 1 FROM sqlite_master "
                                    "WHERE type='table' AND name='global_events'"
                                )
                            if not has_events_table:
                                await ws_mgr.initialize()
                        title_str = str(material.get("title", ""))
                        is_mc = bool(_MC_INSPIRATION_RE.search(f"{title_str}\n{summary}"))
                        event_type = "bili_mc_inspiration" if is_mc else "bili_video_watched"
                        importance = (
                            "normal" if is_mc else ("high" if pick.share_owner else "normal")
                        )
                        await ws_mgr.publish_event(
                            "bilibili_surf",
                            event_type,
                            {
                                "bvid": pick.bvid,
                                "title": title_str[:80],
                                "summary": summary[:140],
                            },
                            importance=importance,
                        )
                    if self.svc.diary:
                        await self.svc.diary.patrol("bili-account:" + pick.bvid, text)
                    if self.s.owner_qq_id:
                        await self.store.save_report(
                            self.store.subject(self.svc.proactive.target()), text
                        )
                    completed += 1
                    if stop_for_risk:
                        reason = "bili_account_risk"
                        break
                    await self._interaction(pick.bvid, aid, pick, epoch)
                    if pick.share_owner and not shared_today:
                        shared_today = await self._maybe_share_owner(
                            day, pick.bvid, str(material.get("title", "")), summary
                        )
                if completed > 0 and not reason and selection.mood_longnick.strip():
                    longnick = getattr(self.svc, "longnick", None)
                    if longnick is not None and hasattr(longnick, "apply_patrol"):
                        await longnick.apply_patrol(selection.mood_longnick)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = (
                    str(exc)
                    if isinstance(exc, Rejected) and re.fullmatch(r"[a-z_]{1,80}", str(exc))
                    else "bili_patrol_failed"
                )
                if reason not in {
                    "gemini_unavailable",
                    "gemini_circuit_open",
                    "budget_hard_stop",
                    "budget_exhausted",
                    "budget_circuit_open",
                }:
                    logging.getLogger("hakimi").info("event=bili_account_failed")
            finally:
                self.patrol_context.reset(token)
            return await self._finish(key, "failed" if reason else "done", completed, reason)

    async def _finish(self, key, status, completed, reason=None):
        async with self.store.transaction():
            await self.store.db.execute(
                "UPDATE bili_patrol_runs SET status=?,reason=? WHERE id=?", (status, reason, key)
            )
        return {"status": status, "learned": completed, "reason": reason}

    async def tick(self):
        world_state = getattr(self.svc, "world_state", None)
        modules = getattr(self.svc, "modules", None)
        bili_mod = modules.get("bilibili_surf") if modules is not None else None
        if (world_state is not None and world_state.proactive_suspended()) or (
            bili_mod is not None and getattr(bili_mod, "proactive_paused", False)
        ):
            return
        if not self.policy.enabled or self.store.runtime.is_resting(self.clock()):
            return
        local = self.local()
        day = local.date().isoformat()
        for value in self.policy.times:
            minute = self.slot_due_minute(day, value)
            due = local.replace(hour=minute // 60, minute=minute % 60, second=0, microsecond=0)
            if 0 <= self.clock() - due.timestamp() < 300:
                await self.run_once(slot=value)

    async def run(self):
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("hakimi").info("event=bili_account_failed")
            await self.sleep(30)

    async def status(self):
        day = self.local().date().isoformat()
        async with self.store.lock:
            state = await self.store.one("SELECT * FROM bili_account_state WHERE id=1")
            device = await self.store.one(
                "SELECT attempted_at,status FROM bili_device_state WHERE id=1"
            )
            runs = [
                dict(r)
                for r in await self.store.all(
                    "SELECT day,slot,status,created,reason FROM bili_patrol_runs "
                    "ORDER BY created DESC LIMIT 10"
                )
            ]
            actions = (
                await self.store.one("SELECT COUNT(*) FROM bili_actions WHERE day=?", (day,))
            )[0]
            watched = (await self.store.one("SELECT COUNT(*) FROM bili_watched_history"))[0]
        today_schedule = [
            f"{m // 60:02d}:{m % 60:02d}"
            for v in self.policy.times
            for m in (self.slot_due_minute(day, v),)
        ]
        return {
            "config": self.policy.model_dump(),
            "source": "sqlite" if self.overridden else "env",
            "environment": self.env_policy.model_dump(),
            "credentials": {key: bool(value) for key, value in self.creds.items()},
            "credential_source": self.creds_source,
            "buvid4_autofill": {
                "status": "configured"
                if self.creds["buvid4"]
                else device["status"]
                if device
                else "not_attempted",
                "attempted_at": device["attempted_at"] if device else None,
            },
            "account_mask": ("…" + self.account["mid"][-4:]) if self.account else None,
            "checked_at": state["checked_at"],
            "cooldown_until": state["blocked_until"],
            "last_error": state["last_error"],
            "recent_runs": runs,
            "actions_today": actions,
            "watched_count": watched,
            "today_schedule": today_schedule,
            "timezone": self.s.bot_timezone,
            "heartbeat_supported": False,
        }
