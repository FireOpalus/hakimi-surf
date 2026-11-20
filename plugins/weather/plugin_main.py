"""Standalone hot-plug Open-Meteo weather observation plugin."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import time
from datetime import datetime
from ipaddress import IPv4Address
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hakimi_plugin_sdk import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
)

log = logging.getLogger("hakimi_plugin_weather")

CONDITIONS: dict[int, str] = {
    0: "晴",
    1: "大部晴朗",
    2: "局部多云",
    3: "阴",
    45: "雾",
    48: "雾凇",
    51: "小毛毛雨",
    53: "毛毛雨",
    55: "较强毛毛雨",
    56: "轻微冻毛毛雨",
    57: "较强冻毛毛雨",
    61: "小雨",
    63: "中雨",
    65: "大雨",
    66: "轻微冻雨",
    67: "强冻雨",
    71: "小雪",
    73: "中雪",
    75: "大雪",
    77: "米雪",
    80: "小阵雨",
    81: "阵雨",
    82: "强阵雨",
    85: "小阵雪",
    86: "强阵雪",
    95: "雷暴",
    96: "雷暴伴轻微冰雹",
    99: "雷暴伴强冰雹",
}

CURRENT_FIELDS = "temperature_2m,weather_code,precipitation,shortwave_radiation,is_day"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
IP_LOOKUP_URL = "https://ipwho.is/"


class _WeatherLookupError(Exception):
    """Internal constant-code upstream weather or geocoding failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class Coordinates(BaseModel):
    """Validated geographic coordinates."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, str_strip_whitespace=True)

    latitude: float = Field(ge=-90, le=90, allow_inf_nan=False)
    longitude: float = Field(ge=-180, le=180, allow_inf_nan=False)

    @property
    def point(self) -> tuple[float, float]:
        return self.latitude, self.longitude


class WeatherLocation(Coordinates):
    """Admin-selected or environment-derived weather observation location."""

    source: Literal["city", "coordinates", "server_ip"] = "coordinates"
    label: str = Field(default="", max_length=160, pattern=r"^[^\x00-\x1f\x7f]*$")


class WeatherCityQuery(BaseModel):
    """Strict city geocoding lookup input."""

    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)

    query: str = Field(min_length=2, max_length=80)


class Observation(BaseModel):
    """Strict Open-Meteo current observation payload."""

    time: int = Field(strict=True, ge=0)
    interval: int = Field(strict=True, gt=0, le=3600)
    temperature_2m: float = Field(strict=True, ge=-100, le=80, allow_inf_nan=False)
    weather_code: int = Field(strict=True, ge=0, le=99)
    precipitation: float | None = Field(None, ge=0, strict=True, allow_inf_nan=False)
    shortwave_radiation: float | None = Field(None, ge=0, strict=True, allow_inf_nan=False)
    is_day: int | None = Field(None, strict=True, ge=0, le=1)

    def fresh_at(self, now: float) -> bool:
        return self.time <= now < self.time + self.interval

    def describe(self, zone: str) -> str:
        at = datetime.fromtimestamp(self.time, ZoneInfo(zone))
        condition = CONDITIONS.get(self.weather_code, f"天气代码 {self.weather_code}")
        rain = f"，时段降水 {self.precipitation:g} 毫米" if self.precipitation is not None else ""
        return f"{at:%m-%d %H:%M}，{condition}，{self.temperature_2m:g} °C{rain}"


async def _bounded_json(
    http: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_bytes: int = 65_536,
    **kwargs: Any,
) -> dict[str, Any]:
    async with http.stream(method, url, **kwargs) as response:
        if 300 <= response.status_code < 400:
            raise _WeatherLookupError("redirect_not_allowed")
        response.raise_for_status()
        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > max_bytes:
                raise _WeatherLookupError("upstream_body_too_large")
    obj = json.loads(body)
    if not isinstance(obj, dict):
        raise _WeatherLookupError("upstream_invalid_shape")
    return obj


def _place_from_row(
    data: Any, source: Literal["city", "server_ip"], *parts: str
) -> WeatherLocation | None:
    if not isinstance(data, dict):
        return None
    names = [v.strip() for key in parts if isinstance(v := data.get(key), str) and v.strip()]
    try:
        return WeatherLocation(
            source=source,
            label=" · ".join(dict.fromkeys(names))[:160],
            latitude=data.get("latitude"),
            longitude=data.get("longitude"),
        )
    except ValidationError:
        return None


async def find_cities(http: httpx.AsyncClient, query: str) -> list[WeatherLocation]:
    """Query Open-Meteo geocoding API for matching Chinese/global city previews."""
    try:
        data = await _bounded_json(
            http,
            "GET",
            GEOCODING_URL,
            params={"name": query, "language": "zh", "count": 6},
            timeout=5,
            follow_redirects=False,
            max_bytes=65_536,
        )
    except (httpx.HTTPError, ValueError, _WeatherLookupError) as exc:
        raise _WeatherLookupError("weather_city_lookup_unavailable") from exc
    rows = data.get("results", [])
    if not isinstance(rows, list) or data.get("error"):
        raise _WeatherLookupError("weather_city_lookup_unavailable")
    return [
        place
        for row in rows[:6]
        if (place := _place_from_row(row, "city", "name", "admin1", "country")) and place.label
    ]


async def locate_server(http: httpx.AsyncClient) -> WeatherLocation:
    """Estimate server exit city via ipwho.is without forwarding any client IP."""
    try:
        data = await _bounded_json(
            http,
            "GET",
            IP_LOOKUP_URL,
            params={"fields": "success,city,region,country,latitude,longitude"},
            timeout=5,
            follow_redirects=False,
            max_bytes=65_536,
        )
    except (httpx.HTTPError, ValueError, _WeatherLookupError) as exc:
        raise _WeatherLookupError("weather_ip_lookup_unavailable") from exc
    place = _place_from_row(data, "server_ip", "city", "region", "country")
    if data.get("success") is not True or place is None:
        raise _WeatherLookupError("weather_ip_lookup_unavailable")
    return place


def _parse_env_coordinates(lat_raw: str | None, lon_raw: str | None) -> Coordinates | None:
    if lat_raw is None or lon_raw is None:
        return None
    lat_str = lat_raw.strip()
    lon_str = lon_raw.strip()
    if not lat_str or not lon_str:
        return None
    try:
        lat = float(lat_str)
        lon = float(lon_str)
        return Coordinates(latitude=lat, longitude=lon)
    except (ValueError, ValidationError):
        log.warning("event=weather_invalid_env_coordinates")
        return None


def _resolve_timezone(tz_raw: str | None) -> str:
    candidate = (tz_raw or "").strip() or "Asia/Shanghai"
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        log.warning("event=weather_invalid_timezone tz=%s", candidate)
        return "Asia/Shanghai"


class WeatherPlugin:
    """Stateful out-of-process weather observation plugin wrapping HakimiPluginClient."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        env_latitude: str | None = None,
        env_longitude: str | None = None,
        timezone: str = "Asia/Shanghai",
        http: httpx.AsyncClient | None = None,
        clock: Any = None,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
    ) -> None:
        self.timezone = _resolve_timezone(timezone)
        self._env_location = _parse_env_coordinates(env_latitude, env_longitude)
        self._override: WeatherLocation | None = None
        self._revision = 0
        self._cached_observation: Observation | None = None
        self._cached_revision = -1
        self._location_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._owns_http = http is None
        self.http = http if http is not None else httpx.AsyncClient(follow_redirects=False)
        self.clock = clock if clock is not None else time.time

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id="weather",
            token=token,
            display_name="天气观测",
            description="AI 和其他模块可按需查询已设地点的当前天气",
            scopes=("actions", "sensors"),
            node_region="KR",
            admin_panel="weather",
            attention_level="LOW",
            reconnect_min=reconnect_min,
            reconnect_max=reconnect_max,
        )
        self.client = HakimiPluginClient(
            config,
            actions=(
                ActionSpec(
                    name="weather.read",
                    description=(
                        "查询哈基米已设地点的当前实况天气、气温与降水"
                        "（用于回答“你那边下雨了吗/今天冷不冷/外面天气咋样”；不查异地城市或未来多日预报）"
                    ),
                    arguments={},
                    origins=["planner", "query"],
                    handler=self._handle_read_action,
                ),
            ),
            initial_status={"location": self.location_status()},
            configuration_issue=self.configuration_issue,
            connect_factory=connect_factory,
        )
        self._bind_handlers()

    @property
    def location(self) -> Coordinates | None:
        return self._override or self._env_location

    @property
    def configuration_issue(self) -> str | None:
        return "weather_location_missing" if self.location is None else None

    def location_status(self) -> dict[str, Any] | None:
        if self._override is not None:
            return self._override.model_dump()
        if self._env_location is not None:
            return {**self._env_location.model_dump(), "source": "environment", "label": ""}
        return None

    def _bind_handlers(self) -> None:
        self.client.on_connect(self._on_connect)
        self.client.sensor(self._sensor_summary)
        self.client.rpc("weather.current")(self._rpc_current)
        self.client.rpc("weather.location.get")(self._rpc_location_get)
        self.client.rpc("weather.location.set")(self._rpc_location_set)
        self.client.rpc("weather.location.reset")(self._rpc_location_reset)
        self.client.rpc("weather.location.cities")(self._rpc_location_cities)
        self.client.rpc("weather.location.detect")(self._rpc_location_detect)

    async def close(self) -> None:
        await self.client.stop()
        if self._owns_http:
            await self.http.aclose()

    def _apply_override_locked(self, place: WeatherLocation | None) -> None:
        if self._override == place:
            return
        current = self.location
        nxt: Coordinates | None = place or self._env_location
        changed = (current.point if current else None) != (nxt.point if nxt else None)
        self._override = place
        if changed:
            self._revision += 1
            self._cached_observation = None

    async def _on_connect(self, _welcome: dict[str, Any]) -> None:
        raw_loc = await self.client.state_get("location")
        parsed: WeatherLocation | None = None
        if raw_loc is not None:
            try:
                parsed = WeatherLocation.model_validate(raw_loc)
            except ValidationError:
                log.warning("event=weather_invalid_saved_location")
                parsed = None
        async with self._location_lock:
            self._apply_override_locked(parsed)
        await self.client.push_status(
            {"location": self.location_status()},
            configuration_issue=self.configuration_issue,
        )

    async def _sensor_summary(self, _params: dict[str, Any] | None = None) -> str:
        if self.location is None:
            return "尚未设置观测地点"
        obs = self._cached_observation if self._cached_revision == self._revision else None
        if obs is None:
            return "等待首次天气查询"
        if not obs.fresh_at(self.clock()):
            return "上次天气数据已过期，可按需查询"
        return "最近查询：" + obs.describe(self.timezone)

    def _outcome(self, sample: Observation, reason: str) -> dict[str, Any]:
        return {
            "status": "done",
            "reason": reason,
            "details": {
                "summary": sample.describe(self.timezone),
                "source": "Open-Meteo",
                "observation": sample.model_dump(exclude_none=True),
            },
        }

    async def _fetch_current(self, place: Coordinates, expected_revision: int) -> dict[str, Any]:
        host = "api.open-meteo.com"
        options: dict[str, Any] = {
            "params": {
                "latitude": place.latitude,
                "longitude": place.longitude,
                "current": CURRENT_FIELDS,
                "timeformat": "unixtime",
                "forecast_days": 1,
            },
            "timeout": 5,
            "max_bytes": 16_384,
            "follow_redirects": False,
        }
        try:
            return await _bounded_json(self.http, "GET", f"https://{host}/v1/forecast", **options)
        except httpx.ConnectTimeout:
            if expected_revision != self._revision:
                raise _WeatherLookupError("weather_location_changed") from None
            dns = await _bounded_json(
                self.http,
                "GET",
                "https://dns.google/resolve",
                params={"name": host, "type": "A", "edns_client_subnet": "193.0.0.0/24"},
                timeout=3,
                max_bytes=4096,
                follow_redirects=False,
            )
        if dns.get("Status") == 0:
            for answer in dns.get("Answer", []):
                if (
                    not isinstance(answer, dict)
                    or answer.get("type") != 1
                    or answer.get("name") not in (host, host + ".")
                ):
                    continue
                address = IPv4Address(answer["data"])
                if address.is_global and not address.is_multicast:
                    if expected_revision != self._revision:
                        raise _WeatherLookupError("weather_location_changed")
                    return await _bounded_json(
                        self.http,
                        "GET",
                        f"https://{address}/v1/forecast",
                        headers={"Host": host, "Connection": "close"},
                        extensions={"sni_hostname": host},
                        **options,
                    )
        raise _WeatherLookupError("weather_unavailable")

    async def read(self, *, base_now: float | None = None) -> dict[str, Any]:
        """Execute weather observation read with location gating, caching, and event emission."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        anchor_now = self.clock() if base_now is None else float(base_now)

        def now() -> float:
            return anchor_now + (loop.time() - started)

        async with self._read_lock:
            place, revision = self.location, self._revision
            if place is None:
                return {"status": "gated_skip", "reason": "weather_location_missing", "details": {}}
            if (
                self._cached_observation is not None
                and self._cached_revision == revision
                and self._cached_observation.fresh_at(now())
            ):
                return self._outcome(self._cached_observation, "observation_cached")

            try:
                data = await self._fetch_current(place, revision)
                observed = Observation.model_validate(data["current"])
            except _WeatherLookupError as exc:
                code = exc.code if exc.code == "weather_location_changed" else "weather_unavailable"
                if revision != self._revision:
                    code = "weather_location_changed"
                return {"status": "gated_skip", "reason": code, "details": {}}
            except (httpx.HTTPError, ValueError, KeyError, TypeError):
                reason = (
                    "weather_location_changed"
                    if revision != self._revision
                    else "weather_unavailable"
                )
                return {"status": "gated_skip", "reason": reason, "details": {}}

            if not observed.fresh_at(now()):
                return {"status": "gated_skip", "reason": "weather_stale", "details": {}}

            async with self._location_lock:
                if revision != self._revision:
                    return {
                        "status": "gated_skip",
                        "reason": "weather_location_changed",
                        "details": {},
                    }
                self._cached_observation = observed
                self._cached_revision = revision

        if self.client.connected:
            await self.client.emit_world_event(
                "weather_observed",
                observed.model_dump(exclude_none=True),
                importance="low",
                business_key=f"sample:{place.point}:{revision}:{observed.time}",
            )
        return self._outcome(observed, "observation_recorded")

    async def _handle_read_action(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_now = params.get("now")
        base_now = (
            float(raw_now)
            if isinstance(raw_now, int | float)
            and not isinstance(raw_now, bool)
            and math.isfinite(float(raw_now))
            else None
        )
        return await self.read(base_now=base_now)

    async def _rpc_current(self, _params: dict[str, Any]) -> dict[str, Any]:
        result = await self.read()
        if result["status"] != "done":
            reason = str(result.get("reason") or "weather_unavailable")
            raise PluginRpcError(reason, reason)
        return dict(result["details"])

    async def _rpc_location_get(self, _params: dict[str, Any]) -> dict[str, Any]:
        return {"location": self.location_status()}

    async def _rpc_location_set(self, params: dict[str, Any]) -> dict[str, Any]:
        raw = (
            params["location"]
            if isinstance(params.get("location"), dict) and "latitude" not in params
            else params
        )
        try:
            place = WeatherLocation.model_validate(raw)
        except ValidationError as exc:
            raise PluginRpcError("invalid_admin_input", "invalid weather location") from exc

        await self.client.state_set("location", place.model_dump())
        async with self._location_lock:
            self._apply_override_locked(place)
            self._cached_observation = None
        loc = self.location_status()
        await self.client.push_status({"location": loc}, configuration_issue=None)
        return {"location": loc}

    async def _rpc_location_reset(self, _params: dict[str, Any]) -> dict[str, Any]:
        await self.client.state_delete("location")
        async with self._location_lock:
            self._apply_override_locked(None)
            self._cached_observation = None
        loc = self.location_status()
        await self.client.push_status(
            {"location": loc},
            configuration_issue=self.configuration_issue,
        )
        return {"location": loc}

    async def _rpc_location_cities(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            query = WeatherCityQuery.model_validate(params)
        except ValidationError as exc:
            raise PluginRpcError("invalid_admin_input", "invalid city query") from exc
        try:
            places = await find_cities(self.http, query.query)
        except _WeatherLookupError as exc:
            raise PluginRpcError(exc.code, exc.code) from exc
        return {"places": [place.model_dump() for place in places]}

    async def _rpc_location_detect(self, _params: dict[str, Any]) -> dict[str, Any]:
        try:
            place = await locate_server(self.http)
        except _WeatherLookupError as exc:
            raise PluginRpcError(exc.code, exc.code) from exc
        return place.model_dump()


def build_weather_plugin_from_env(**overrides: Any) -> WeatherPlugin:
    """Create a WeatherPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_WEATHER_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_WEATHER_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "env_latitude": os.getenv("HAKIMI_WEATHER_LATITUDE"),
        "env_longitude": os.getenv("HAKIMI_WEATHER_LONGITUDE"),
        "timezone": os.getenv("TZ") or "Asia/Shanghai",
    }
    kwargs.update(overrides)
    return WeatherPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the weather remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    plugin = build_weather_plugin_from_env()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        asyncio.create_task(plugin.client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await plugin.client.run_forever()
    finally:
        await plugin.close()


if __name__ == "__main__":
    asyncio.run(main())
