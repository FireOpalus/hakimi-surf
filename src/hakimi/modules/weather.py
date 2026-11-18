"""Optional fixed-location Open-Meteo reader. Only numeric observations enter shared facts."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, Field

from ..life_rules import Reaction, Rule
from .actions import Action, ActionCall, DailyAction, Outcome
from .base import BaseModule
from .scheduling import ScheduleOptions, ScheduleTime


class Observation(BaseModel):
    temperature_2m: float = Field(strict=True, ge=-100, le=80, allow_inf_nan=False)
    weather_code: int = Field(strict=True, ge=0, le=99)


class WeatherScheduleOptions(ScheduleOptions):
    observation_time: ScheduleTime = Field("09:30", title="观测时间（HH:MM）")

    @classmethod
    def from_settings(cls, settings):
        return cls(observation_time=settings.weather_time)


class WeatherModule(BaseModule):
    module_id = "weather"
    schedule_options = WeatherScheduleOptions

    def __init__(self, settings, http, publish):
        super().__init__(settings)
        self.http = http
        self.publish = publish

    def actions(self):
        return (
            Action(
                name="weather.read",
                description="读取配置地点的当前气温和天气代码",
                origins=frozenset({"timer"}),
                run=self.read,
            ),
        )

    def schedules(self):
        return (
            DailyAction(
                "weather.read",
                self.settings.weather_time,
                label="天气观测",
                time_option="observation_time",
            ),
        )

    def event_rules(self):
        def report(ev):
            if (
                self.settings.weather_owner_report
                and self.settings.owner_qq_id
                and ev.source_module == self.module_id
                and ev.event_type == "weather_observed"
            ):
                return Reaction(
                    "qq.speak",
                    "根据天气观测给 Owner 简短提醒",
                    recipient="owner:" + self.settings.owner_qq_id,
                    cooldown=0,
                    ttl=1800,
                    legacy_module="qq_social",
                    legacy_verb="proactive_share",
                )
            return None

        return (Rule("weather.owner_report", report),)

    async def _raw_sensor_summary(self):
        return "定时天气观测已开启"

    async def read(self, call: ActionCall) -> Outcome:
        try:
            response = await self.http.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": self.settings.weather_latitude,
                    "longitude": self.settings.weather_longitude,
                    "current": "temperature_2m,weather_code",
                },
                follow_redirects=False,
                timeout=5,
            )
            response.raise_for_status()
            observed = Observation.model_validate(response.json()["current"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            return Outcome("gated_skip", "weather_unavailable")
        day = datetime.fromtimestamp(call.now, ZoneInfo(self.settings.bot_timezone)).date()
        await self.publish(
            self.module_id,
            "weather_observed",
            observed.model_dump(),
            now=call.now,
            business_key=str(day),
        )
        return Outcome("done", "observation_recorded")
