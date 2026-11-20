"""Standalone anonymous read-only public Bilibili metadata client."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import unicodedata
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hakimi_plugin_sdk import PluginRpcError

log = logging.getLogger("hakimi_plugin_bilibili")

BVID = r"BV[1-9A-HJ-NP-Za-km-z]{10}"
BV_RE = re.compile(rf"(?<![A-Za-z0-9])({BVID})(?![A-Za-z0-9])")
URL_RE = re.compile(r"https?://[^\s<>\"'，。！？）]+", re.IGNORECASE)

API_ORIGIN = "https://api.bilibili.com"
LIVE_API_ORIGIN = "https://api.live.bilibili.com"
PUBLIC_API_PATHS = frozenset({"/x/web-interface/view", "/x/polymer/web-dynamic/v1/feed/space"})
LIVE_API_PATHS = frozenset({"/room/v1/Room/get_info"})
CANONICAL_VIDEO_HOSTS = frozenset({"www.bilibili.com", "bilibili.com", "m.bilibili.com"})
SHORT_LINK_HOSTS = frozenset({"b23.tv", "bili2233.cn"})

NODE_TRANSPORT_ERRORS = frozenset(
    {
        "node_disconnected",
        "node_not_dispatched",
        "node_rpc_timeout",
        "node_unavailable",
        "plugin_disconnected",
        "plugin_rpc_timeout",
    }
)
CIRCUIT_BREAK_SECONDS = 900.0
_DEFAULT_HEADERS = {
    "User-Agent": "HakimiSurf/0.1 (public metadata reader)",
    "Referer": "https://www.bilibili.com/",
}


class BiliClientError(PluginRpcError):
    """Constant-code Bilibili client failure compatible with PluginRpcError."""

    def __init__(self, code: str, detail: str = "", message: str = "") -> None:
        super().__init__(code, message)
        self.detail = detail


def clean_text(text: str) -> str:
    """Normalize NFKC text while stripping control characters except newline/tab."""
    return "".join(
        c
        for c in unicodedata.normalize("NFKC", str(text or ""))
        if c in "\n\t" or not unicodedata.category(c).startswith("C")
    ).strip()


async def bounded_json(
    http: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_bytes: int = 1_048_576,
    **kwargs: Any,
) -> dict[str, Any]:
    """Stream one JSON object response up to `max_bytes` without following redirects."""
    kwargs.setdefault("follow_redirects", False)
    async with http.stream(method, url, **kwargs) as response:
        if 300 <= response.status_code < 400:
            raise BiliClientError("redirect_not_allowed")
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > max_bytes:
                raise BiliClientError("upstream_body_too_large")
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise BiliClientError("upstream_invalid_shape")
    return obj


def _extract_url_bvid(raw_url: str) -> str | None:
    if not isinstance(raw_url, str) or not raw_url:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in raw_url):
        return None
    try:
        u = urlsplit(raw_url)
    except ValueError:
        return None
    if (
        u.scheme != "https"
        or u.hostname not in CANONICAL_VIDEO_HOSTS
        or u.username is not None
        or u.password is not None
        or u.port not in {None, 443}
    ):
        return None
    match = re.fullmatch(rf"/video/({BVID})/?", u.path)
    return match[1] if match else None


def extract_bvid(text: str) -> str | None:
    """Extract a canonical Bilibili BV ID from strict HTTPS Bilibili URLs or bare text."""
    if not isinstance(text, str) or not text:
        return None
    urls = URL_RE.findall(text)
    candidates: list[str] = []
    for raw in urls:
        bvid = _extract_url_bvid(raw)
        if bvid:
            candidates.append(bvid)
    if not candidates:
        candidates = BV_RE.findall(URL_RE.sub(" ", text))
    return candidates[0] if candidates else None


def _is_valid_short_link(url: str) -> str | None:
    """Validate strict HTTPS short links (`b23.tv`, `bili2233.cn`) and return hostname."""
    if not isinstance(url, str) or not url:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in url):
        return None
    try:
        u = urlsplit(url)
    except ValueError:
        return None
    host = u.hostname or ""
    if (
        u.scheme != "https"
        or host not in SHORT_LINK_HOSTS
        or u.username is not None
        or u.password is not None
        or u.port not in {None, 443}
        or u.query
        or u.fragment
        or not re.fullmatch(r"/[A-Za-z0-9]{1,32}", u.path)
    ):
        return None
    return host


def _optional_non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and math.isfinite(value) and value >= 0:
        return int(value)
    return None


def _extract_tags(data: dict[str, Any]) -> list[str]:
    raw_tags = data.get("tags")
    tags: list[str] = []
    if isinstance(raw_tags, list):
        for item in raw_tags[:10]:
            label = (
                clean_text(str(item.get("tag_name", "")))
                if isinstance(item, dict)
                else clean_text(str(item))
            )
            if label and label not in tags:
                tags.append(label[:32])
    elif isinstance(data.get("tname"), str):
        tname = clean_text(data["tname"])[:32]
        if tname:
            tags.append(tname)
    return tags


class PublicVideo(BaseModel):
    """Validated public Bilibili video metadata payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bvid: str = Field(pattern=rf"^{BVID}$")
    mid: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    title: str = Field(max_length=200)
    description: str = Field(max_length=2000)
    author: str = Field(max_length=100)
    published_at: int = 0
    duration: int | None = None
    views: int | None = None
    likes: int | None = None
    tags: list[str] = Field(default_factory=list)


class BiliPublicClient:
    """Anonymous, read-only public Bilibili metadata client with CN node delegation."""

    def __init__(
        self,
        plugin_client: Any = None,
        http_client: httpx.AsyncClient | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        env_defaults: dict[str, Any] | None = None,
    ) -> None:
        self.plugin_client = plugin_client
        self._owns_http = http_client is None
        self.http = (
            http_client
            if http_client is not None
            else httpx.AsyncClient(timeout=15.0, trust_env=False, follow_redirects=False)
        )
        self.clock = clock
        self.sleep = sleep
        self.module_enabled: Callable[[], bool] | bool = True
        self.node_connected: bool = False

        self.bilibili_enabled: bool = True
        self.bilibili_min_interval_seconds: float = 3.0
        self.node_bili_delegate_enabled: bool = True
        self.node_bili_fallback_local: bool = True
        if env_defaults is not None:
            self.apply_env_defaults(env_defaults)

        self.lock = asyncio.Lock()
        self.last_request: float = 0.0
        self._has_requested: bool = False
        self.blocked_until: float = 0.0
        self.cache: dict[str, tuple[float, dict[str, Any]]] = {}

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    def apply_env_defaults(self, defaults: dict[str, Any] | None) -> None:
        """Apply configuration defaults returned from kernel `env_defaults`."""
        if not isinstance(defaults, dict):
            return
        if "bilibili_enabled" in defaults:
            self.set_enabled(bool(defaults["bilibili_enabled"]))
        if "bilibili_min_interval_seconds" in defaults:
            self.set_min_interval(defaults["bilibili_min_interval_seconds"])
        if "node_bili_delegate_enabled" in defaults:
            self.set_delegate_enabled(bool(defaults["node_bili_delegate_enabled"]))
        if "node_bili_fallback_local" in defaults:
            self.set_fallback_local(bool(defaults["node_bili_fallback_local"]))

    def set_enabled(self, enabled: bool) -> None:
        self.bilibili_enabled = bool(enabled)

    def set_min_interval(self, seconds: float) -> None:
        val = float(seconds)
        if not math.isfinite(val) or val < 0.0:
            raise ValueError("bilibili_min_interval_seconds must be non-negative and finite")
        self.bilibili_min_interval_seconds = min(val, 30.0)

    def set_delegate_enabled(self, enabled: bool) -> None:
        self.node_bili_delegate_enabled = bool(enabled)

    def set_fallback_local(self, enabled: bool) -> None:
        self.node_bili_fallback_local = bool(enabled)

    def set_node_connected(self, connected: bool) -> None:
        self.node_connected = bool(connected)

    def set_module_enabled(self, enabled: bool) -> None:
        self.module_enabled = bool(enabled)

    def _is_module_enabled(self) -> bool:
        if callable(self.module_enabled):
            return bool(self.module_enabled())
        return bool(self.module_enabled)

    def _can_delegate(self) -> bool:
        return bool(
            self.plugin_client is not None
            and self.node_connected
            and self.node_bili_delegate_enabled
        )

    async def _pace_locked(self) -> None:
        now = self.clock()
        if self._has_requested or self.last_request > 0.0:
            delay = self.bilibili_min_interval_seconds - (now - self.last_request)
            if delay > 0:
                await self.sleep(delay)
        if not self._is_module_enabled():
            raise BiliClientError("module_disabled")
        self.last_request = self.clock()
        self._has_requested = True

    def _extract_error_code(self, exc: BaseException) -> tuple[str, str]:
        if isinstance(exc, PluginRpcError):
            return exc.code, getattr(exc, "detail", "") or exc.message
        return str(exc) or type(exc).__name__, ""

    async def _get(
        self,
        path: str,
        params: dict[str, str],
        cache_key: str,
        ttl: int,
        *,
        live: bool = False,
    ) -> dict[str, Any]:
        allowed_paths = LIVE_API_PATHS if live else PUBLIC_API_PATHS
        if path not in allowed_paths:
            raise BiliClientError("bili_endpoint_forbidden")
        if not self._is_module_enabled():
            raise BiliClientError("module_disabled")
        if not self.bilibili_enabled:
            raise BiliClientError("bilibili_disabled")

        async with self.lock:
            now = self.clock()
            if self.blocked_until > now:
                raise BiliClientError("bilibili_circuit_open")
            cached = self.cache.get(cache_key)
            if cached and cached[0] > now:
                return cached[1]
            await self._pace_locked()

            try:
                result: dict[str, Any] | None = None
                if self._can_delegate():
                    try:
                        raw_res = await self.plugin_client.node_call(
                            "bili.public_get",
                            {"path": path, "params": params, "live": live},
                        )
                        result = raw_res if isinstance(raw_res, dict) else None
                    except (PluginRpcError, TimeoutError, OSError) as exc:
                        code, detail = self._extract_error_code(exc)
                        if code in NODE_TRANSPORT_ERRORS and self.node_bili_fallback_local:
                            result = None
                        else:
                            if "412" in f"{code}:{detail}":
                                log.warning("event=bilibili_http_412")
                            elif "403" in f"{code}:{detail}":
                                log.warning("event=bilibili_http_403")
                            elif "429" in f"{code}:{detail}":
                                log.warning("event=bilibili_http_429")
                            raise BiliClientError("bilibili_unavailable") from None

                if result is None:
                    origin = LIVE_API_ORIGIN if live else API_ORIGIN
                    result = await bounded_json(
                        self.http,
                        "GET",
                        origin + path,
                        params=params,
                        headers=dict(_DEFAULT_HEADERS),
                    )

                code = result.get("code") if isinstance(result, dict) else None
                if isinstance(code, bool) or code != 0 or not isinstance(result.get("data"), dict):
                    if not isinstance(code, bool) and code in {-412, -352}:
                        log.warning("event=bilibili_http_412")
                    raise BiliClientError("bilibili_unavailable")
            except BiliClientError as exc:
                if exc.code in {"module_disabled", "bilibili_disabled", "bilibili_circuit_open"}:
                    raise
                self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                raise BiliClientError("bilibili_unavailable") from None
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in {403, 412, 429}:
                    log.warning("event=bilibili_http_%d", exc.response.status_code)
                self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                raise BiliClientError("bilibili_unavailable") from None
            except Exception:
                self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                raise BiliClientError("bilibili_unavailable") from None

            if len(self.cache) >= 128:
                self.cache.pop(next(iter(self.cache)))
            self.cache[cache_key] = (self.clock() + ttl, result["data"])
            return result["data"]

    async def resolve_short(self, url: str) -> str | None:
        """Resolve only a strict HTTPS short link header without following redirects."""
        if not self._is_module_enabled():
            raise BiliClientError("module_disabled")
        if not self.bilibili_enabled:
            raise BiliClientError("bilibili_disabled")
        host = _is_valid_short_link(url)
        if host is None:
            raise BiliClientError("invalid_bilibili_short_link")

        async with self.lock:
            if self.blocked_until > self.clock():
                raise BiliClientError("bilibili_circuit_open")
            await self._pace_locked()

            location: str | None = None
            if host == "b23.tv" and self._can_delegate():
                try:
                    resp = await self.plugin_client.node_call("bili.resolve_short", {"url": url})
                except (PluginRpcError, TimeoutError, OSError) as exc:
                    code, _ = self._extract_error_code(exc)
                    if code in NODE_TRANSPORT_ERRORS and self.node_bili_fallback_local:
                        resp = None
                    else:
                        self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                        raise BiliClientError("bilibili_short_link_unavailable") from None
                except Exception:
                    self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                    raise BiliClientError("bilibili_short_link_unavailable") from None

                if resp is not None:
                    if not isinstance(resp, dict):
                        self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                        raise BiliClientError("bilibili_short_link_unavailable")
                    location = str(resp.get("location") or "")

            if location is None:
                try:
                    async with self.http.stream(
                        "GET",
                        url,
                        headers={"User-Agent": _DEFAULT_HEADERS["User-Agent"]},
                        follow_redirects=False,
                    ) as response:
                        if response.status_code not in {301, 302, 303, 307, 308}:
                            self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                            raise BiliClientError("bilibili_short_link_unavailable")
                        location = response.headers.get("location", "")
                except BiliClientError:
                    raise
                except Exception:
                    self.blocked_until = self.clock() + CIRCUIT_BREAK_SECONDS
                    raise BiliClientError("bilibili_short_link_unavailable") from None

            bvid = _extract_url_bvid(location.strip())
            if not bvid:
                raise BiliClientError("bilibili_short_link_target_forbidden")
            return bvid

    async def video(self, bvid: str) -> dict[str, Any]:
        """Fetch and normalize public metadata for one Bilibili video."""
        if not isinstance(bvid, str) or not re.fullmatch(BVID, bvid):
            raise BiliClientError("invalid_bvid")
        data = await self._get("/x/web-interface/view", {"bvid": bvid}, bvid, 600)
        try:
            if data["bvid"] != bvid:
                raise ValueError("bvid_mismatch")
            owner = data["owner"]
            raw_pub = data.get("pubdate", 0)
            if isinstance(raw_pub, bool):
                raise ValueError("invalid_pubdate")
            stat = data.get("stat") if isinstance(data.get("stat"), dict) else {}
            video = PublicVideo(
                bvid=bvid,
                mid=str(owner["mid"]),
                title=clean_text(str(data["title"]))[:200],
                description=clean_text(str(data.get("desc", "")))[:2000],
                author=clean_text(str(owner["name"]))[:100],
                published_at=int(raw_pub),
                duration=_optional_non_negative_int(data.get("duration")),
                views=_optional_non_negative_int(stat.get("view", data.get("views"))),
                likes=_optional_non_negative_int(stat.get("like", data.get("likes"))),
                tags=_extract_tags(data),
            )
            return video.model_dump()
        except (KeyError, TypeError, ValueError, ValidationError):
            raise BiliClientError("bilibili_schema_changed") from None

    async def updates(self, mid: str) -> list[dict[str, Any]]:
        """Fetch recent archive uploads for a creator MID."""
        if not isinstance(mid, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", mid):
            raise BiliClientError("invalid_mid")
        data = await self._get(
            "/x/polymer/web-dynamic/v1/feed/space", {"host_mid": mid}, "up:" + mid, 600
        )
        items = data.get("items")
        if not isinstance(items, list):
            raise BiliClientError("bilibili_schema_changed")
        videos: dict[str, PublicVideo] = {}
        try:
            for item in items[:50]:
                if not isinstance(item, dict) or item.get("type") != "DYNAMIC_TYPE_AV":
                    continue
                modules = item["modules"]
                author = modules["module_author"]
                major = modules["module_dynamic"]["major"]
                if str(author["mid"]) != mid or major.get("type") != "MAJOR_TYPE_ARCHIVE":
                    continue
                archive = major["archive"]
                pub_ts = author["pub_ts"]
                if isinstance(pub_ts, bool):
                    raise ValueError("invalid_pub_ts")
                video = PublicVideo(
                    bvid=archive["bvid"],
                    mid=mid,
                    title=clean_text(str(archive["title"]))[:200],
                    description=clean_text(str(archive.get("desc", "")))[:2000],
                    author=clean_text(str(author["name"]))[:100],
                    published_at=int(pub_ts),
                    duration=_optional_non_negative_int(
                        archive.get("duration", archive.get("duration_text"))
                    ),
                )
                videos[video.bvid] = video
        except (KeyError, TypeError, ValueError, ValidationError):
            raise BiliClientError("bilibili_schema_changed") from None
        ordered = sorted(videos.values(), key=lambda v: v.published_at, reverse=True)
        return [v.model_dump() for v in ordered]

    async def live_info(self, room_id: str) -> dict[str, Any]:
        """Fetch sanitized live room status for one numeric room ID."""
        if not isinstance(room_id, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", room_id):
            raise BiliClientError("invalid_room_id")
        data = await self._get(
            "/room/v1/Room/get_info", {"room_id": room_id}, "live:" + room_id, 60, live=True
        )
        status = data.get("live_status")
        if isinstance(status, bool) or status not in {0, 1, 2}:
            raise BiliClientError("invalid_live_status")
        return {
            "room_id": room_id,
            "live_status": status,
            "title": clean_text(str(data.get("title", "")))[:120],
            "area": clean_text(str(data.get("area_name", "")))[:40],
        }

    async def live_status(self, room_id: str) -> int:
        """Return integer live_status (0=offline, 1=live, 2=round)."""
        return int((await self.live_info(room_id))["live_status"])
