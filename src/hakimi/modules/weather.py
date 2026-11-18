"""Optional fixed-location Open-Meteo reader. Only numeric observations enter shared facts."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, Field

from .actions import Action, ActionCall, DailyAction, HourlyAction, Outcome
from .base import BaseModule
from .scheduling import ScheduleOptions, ScheduleTime


class Observation(BaseModel):
    time: int = Field(strict=True, ge=0)
    interval: int = Field(strict=True, gt=0, le=3600)
    temperature_2m: float = Field(strict=True, ge=-100, le=80, allow_inf_nan=False)
    weather_code: int = Field(strict=True, ge=0, le=99)
    precipitation: float | None = Field(None, ge=0, strict=True, allow_inf_nan=False)
    shortwave_radiation: float | None = Field(None, ge=0, strict=True, allow_inf_nan=False)
    is_day: int | None = Field(None, strict=True, ge=0, le=1)


class WeatherScheduleOptions(ScheduleOptions):
    observation_time: ScheduleTime = Field("09:30", title="每日 Owner 天气提醒时间（HH:MM）")

    @classmethod
    def from_settings(cls, settings):
        return cls(observation_time=settings.weather_time)


class WeatherModule(BaseModule):
    module_id = "weather"
    schedule_options = WeatherScheduleOptions
    current_fields = "temperature_2m,weather_code,precipitation,shortwave_radiation,is_day"

    def __init__(self, settings, http, world):
        super().__init__(settings)
        self.http = http
        self.world = world

    def actions(self):
        return (
            Action(
                name="weather.read",
                description="每小时读取配置地点的当前天气",
                origins=frozenset({"timer"}),
                run=self.read,
                background_sensor=True,
            ),
            Action(
                name="weather.daily_report",
                description="用最近一次天气观测准备每日 Owner 提醒",
                origins=frozenset({"timer"}),
                run=self.daily_report,
            ),
        )

    def schedules(self):
        return (
            HourlyAction("weather.read", label="天气采样"),
            DailyAction(
                "weather.daily_report",
                self.settings.weather_time,
                label="Owner 天气提醒",
                time_option="observation_time",
                enabled=lambda: self.settings.weather_owner_report,
            ),
        )

    async def _raw_sensor_summary(self):
        sample = await self.world.latest_event(self.module_id, "weather_observed")
        if sample is None:
            return "等待首次天气观测"
        at = datetime.fromtimestamp(
            sample.payload.get("time", sample.created_at), ZoneInfo(self.settings.bot_timezone)
        )
        temperature = sample.payload["temperature_2m"]
        code = sample.payload["weather_code"]
        return f"最近天气 {at:%m-%d %H:%M}，{temperature} °C，天气代码 {code}"

    async def read(self, call: ActionCall) -> Outcome:
        local = datetime.fromtimestamp(call.now, ZoneInfo(self.settings.bot_timezone))
        hour_start = local.replace(minute=0, second=0, microsecond=0).timestamp()
        latest = await self.world.latest_event(self.module_id, "weather_observed")
        if latest and hour_start <= latest.created_at <= call.now:
            return Outcome("done", "observation_cached")
        try:
            response = await self.http.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": self.settings.weather_latitude,
                    "longitude": self.settings.weather_longitude,
                    "current": self.current_fields,
                    "timeformat": "unixtime",
                },
                follow_redirects=False,
                timeout=5,
            )
            response.raise_for_status()
            observed = Observation.model_validate(response.json()["current"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return Outcome("gated_skip", "weather_unavailable")
        if not 0 <= call.now - observed.time < 3600:
            return Outcome("gated_skip", "weather_stale")
        await self.world.publish_event(
            self.module_id,
            "weather_observed",
            observed.model_dump(exclude_none=True),
            importance="low",
            now=call.now,
            business_key=f"sample:{observed.time}",
            include_in_plan=False,
        )
        return Outcome("done", "observation_recorded")

    async def daily_report(self, call: ActionCall) -> Outcome:
        sample = await self.world.latest_event(self.module_id, "weather_observed")
        if (
            sample is None
            or not 0 <= call.now - sample.payload.get("time", sample.created_at) < 3600
        ):
            # At the top of the hour both timers can become due together.
            return Outcome("not_dispatched", "awaiting_weather_sample")
        day = datetime.fromtimestamp(call.now, ZoneInfo(self.settings.bot_timezone)).date()
        await self.world.publish_event(
            self.module_id,
            "weather_report_due",
            sample.payload,
            now=call.now,
            business_key=str(day),
            include_in_plan=False,
        )
        return Outcome("done", "weather_report_ready")
