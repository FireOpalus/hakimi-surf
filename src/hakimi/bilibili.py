import asyncio
import json
import logging
import re
import time
from urllib.parse import urlsplit

import httpx

from .config import Settings
from .models import Video
from .security import Rejected, clean_text

BVID = r"BV[1-9A-HJ-NP-Za-km-z]{10}"
BV_RE = re.compile(rf"(?<![A-Za-z0-9])({BVID})(?![A-Za-z0-9])")
URL_RE = re.compile(r"https?://[^\s<>\"'，。！？）]+", re.IGNORECASE)
API_ORIGIN = "https://api.bilibili.com"
NODE_TRANSPORT_ERRORS = frozenset(
    {"node_disconnected", "node_not_dispatched", "node_rpc_timeout", "node_unavailable"}
)


def extract_bvid(text: str) -> str | None:
    urls = URL_RE.findall(text)
    candidates = []
    for raw in urls:
        try:
            u = urlsplit(raw)
            if (
                u.scheme != "https"
                or u.hostname not in {"www.bilibili.com", "bilibili.com", "m.bilibili.com"}
                or u.username
                or u.password
                or u.port not in {None, 443}
            ):
                continue
            match = re.fullmatch(rf"/video/({BVID})/?", u.path)
            if match:
                candidates.append(match[1])
        except ValueError:
            continue
    if not candidates:
        candidates = BV_RE.findall(URL_RE.sub(" ", text))
    return candidates[0] if candidates else None


async def bounded_json(
    client: httpx.AsyncClient, method: str, url: str, *, max_bytes: int = 1_048_576, **kwargs
) -> dict:
    async with client.stream(method, url, **kwargs) as response:
        response.raise_for_status()
        if 300 <= response.status_code < 400:
            raise Rejected("redirect_not_allowed")
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > max_bytes:
                raise Rejected("upstream_body_too_large")
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise Rejected("upstream_invalid_shape")
    return obj


class Bilibili:
    """Anonymous, read-only public metadata. No cookies, login, media or comment fetching."""

    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient,
        runtime=None,
        node_transport=None,
    ):
        self.s, self.http = settings, client
        self.runtime = runtime
        self.node_transport = node_transport
        self.lock = asyncio.Lock()
        self.last_request = 0.0
        self.blocked_until = 0.0
        self.cache: dict[str, tuple[float, dict]] = {}

    def _can_delegate(self) -> bool:
        return bool(
            self.node_transport is not None
            and self.node_transport.connected()
            and getattr(self.s, "node_bili_delegate_enabled", True)
        )

    async def _get(
        self, path: str, params: dict[str, str], cache_key: str, ttl: int, *, live=False
    ) -> dict:
        if not self.s.bilibili_enabled:
            raise Rejected("bilibili_disabled")
        async with self.lock:
            now = time.monotonic()
            if self.blocked_until > now:
                raise Rejected("bilibili_circuit_open")
            cached = self.cache.get(cache_key)
            if cached and cached[0] > now:
                return cached[1]
            delay = self.s.bilibili_min_interval_seconds - (now - self.last_request)
            if delay > 0:
                await asyncio.sleep(delay)
            self.last_request = time.monotonic()
            risk_412 = False
            try:
                result = None
                if self._can_delegate():
                    try:
                        result = await self.node_transport.call(
                            "bili.public_get",
                            {"path": path, "params": params, "live": live},
                        )
                    except Rejected as exc:
                        if str(exc) in NODE_TRANSPORT_ERRORS and getattr(
                            self.s, "node_bili_fallback_local", True
                        ):
                            result = None
                        else:
                            raise
                if result is None:
                    result = await bounded_json(
                        self.http,
                        "GET",
                        ("https://api.live.bilibili.com" if live else API_ORIGIN) + path,
                        params=params,
                        headers={
                            "User-Agent": "HakimiSurf/0.1 (public metadata reader)",
                            "Referer": "https://www.bilibili.com/",
                        },
                    )
                code = result.get("code") if isinstance(result, dict) else None
                if (
                    isinstance(code, bool)
                    or code != 0
                    or not isinstance(result.get("data"), dict)
                ):
                    if not isinstance(code, bool) and code in {-412, -352}:
                        risk_412 = True
                        logging.getLogger("hakimi").warning("event=bilibili_http_412")
                    raise Rejected("bilibili_unavailable")
            except Rejected as exc:
                if str(exc) == "bili_http_status":
                    status_detail = getattr(exc, "detail", "")
                    risk_412 = status_detail == "412"
                    if status_detail in {"403", "412", "429"}:
                        logging.getLogger("hakimi").warning(
                            "event=bilibili_http_%s", status_detail
                        )
                self.blocked_until = time.monotonic() + 900
                raise Rejected("bilibili_unavailable") from None
            except httpx.HTTPStatusError as exc:
                risk_412 = exc.response.status_code == 412
                if exc.response.status_code in {403, 412, 429}:
                    logging.getLogger("hakimi").warning(
                        "event=bilibili_http_%d", exc.response.status_code
                    )
                self.blocked_until = time.monotonic() + 900
                raise Rejected("bilibili_unavailable") from None
            except Exception:
                # Includes 403/412/429 and risk-control codes (-352/-412). Never bypass them.
                self.blocked_until = time.monotonic() + 900
                raise Rejected("bilibili_unavailable") from None
            finally:
                if self.runtime and cache_key.startswith(("up:", "live:")):
                    await self.runtime.provider_result("bilibili_412", risk_412)
            if len(self.cache) >= 128:
                self.cache.pop(next(iter(self.cache)))
            self.cache[cache_key] = (time.monotonic() + ttl, result["data"])
            return result["data"]

    async def live_info(self, room_id: str) -> dict:
        if not re.fullmatch(r"[1-9][0-9]{0,19}", room_id):
            raise Rejected("invalid_room_id")
        data = await self._get(
            "/room/v1/Room/get_info", {"room_id": room_id}, "live:" + room_id, 60, live=True
        )
        status = data.get("live_status")
        if isinstance(status, bool) or status not in {0, 1, 2}:
            raise Rejected("invalid_live_status")
        return {
            "room_id": room_id,
            "live_status": status,
            "title": clean_text(str(data.get("title", "")))[:120],
            "area": clean_text(str(data.get("area_name", "")))[:40],
        }

    async def live_status(self, room_id: str) -> int:
        return (await self.live_info(room_id))["live_status"]

    async def video(self, bvid: str) -> Video:
        if not re.fullmatch(BVID, bvid):
            raise Rejected("invalid_bvid")
        data = await self._get("/x/web-interface/view", {"bvid": bvid}, bvid, 600)
        try:
            if data["bvid"] != bvid:
                raise ValueError
            return Video(
                bvid=bvid,
                mid=str(data["owner"]["mid"]),
                title=clean_text(data["title"])[:200],
                description=clean_text(data.get("desc", ""))[:2000],
                author=clean_text(data["owner"]["name"])[:100],
                published_at=int(data["pubdate"]),
            )
        except (KeyError, TypeError, ValueError):
            raise Rejected("bilibili_schema_changed") from None

    async def resolve_short(self, url: str) -> str:
        """Read only b23's redirect header. Never follow it, even to a Bilibili host."""
        if not self.s.bilibili_enabled or not re.fullmatch(
            r"https://b23\.tv/[A-Za-z0-9]{1,32}", url
        ):
            raise Rejected("invalid_bilibili_short_link")
        async with self.lock:
            if self.blocked_until > time.monotonic():
                raise Rejected("bilibili_circuit_open")
            delay = self.s.bilibili_min_interval_seconds - (time.monotonic() - self.last_request)
            if delay > 0:
                await asyncio.sleep(delay)
            self.last_request = time.monotonic()
            if self._can_delegate():
                resp = None
                try:
                    resp = await self.node_transport.call(
                        "bili.resolve_short", {"url": url}
                    )
                except Rejected as exc:
                    if str(exc) in NODE_TRANSPORT_ERRORS and getattr(
                        self.s, "node_bili_fallback_local", True
                    ):
                        resp = None
                    else:
                        self.blocked_until = time.monotonic() + 900
                        raise Rejected("bilibili_short_link_unavailable") from None
                except Exception:
                    self.blocked_until = time.monotonic() + 900
                    raise Rejected("bilibili_short_link_unavailable") from None
                if resp is not None:
                    if not isinstance(resp, dict):
                        self.blocked_until = time.monotonic() + 900
                        raise Rejected("bilibili_short_link_unavailable")
                    bvid = extract_bvid(str(resp.get("location") or ""))
                    if not bvid:
                        raise Rejected("bilibili_short_link_target_forbidden")
                    return bvid
            async with self.http.stream("GET", url, follow_redirects=False) as response:
                if response.status_code not in {301, 302, 303, 307, 308}:
                    self.blocked_until = time.monotonic() + 900
                    raise Rejected("bilibili_short_link_unavailable")
                bvid = extract_bvid(response.headers.get("location", ""))
                if not bvid:
                    raise Rejected("bilibili_short_link_target_forbidden")
                return bvid

    async def updates(self, mid: str) -> list[Video]:
        if not re.fullmatch(r"[1-9][0-9]{0,19}", mid):
            raise Rejected("invalid_mid")
        data = await self._get(
            "/x/polymer/web-dynamic/v1/feed/space", {"host_mid": mid}, "up:" + mid, 600
        )
        if not isinstance(data.get("items"), list):
            raise Rejected("bilibili_schema_changed")
        videos: dict[str, Video] = {}
        try:
            for item in data["items"][:50]:
                if item.get("type") != "DYNAMIC_TYPE_AV":
                    continue
                modules = item["modules"]
                author = modules["module_author"]
                major = modules["module_dynamic"]["major"]
                if str(author["mid"]) != mid or major.get("type") != "MAJOR_TYPE_ARCHIVE":
                    continue
                archive = major["archive"]
                video = Video(
                    bvid=archive["bvid"],
                    mid=mid,
                    title=clean_text(archive["title"])[:200],
                    description=clean_text(archive.get("desc", ""))[:2000],
                    author=clean_text(author["name"])[:100],
                    published_at=int(author["pub_ts"]),
                )
                videos[video.bvid] = video
        except (KeyError, TypeError, ValueError):
            raise Rejected("bilibili_schema_changed") from None
        return sorted(videos.values(), key=lambda v: v.published_at, reverse=True)
