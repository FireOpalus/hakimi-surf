"""Hot-configurable daily rest hours policy; local wall clock only."""

from datetime import datetime
from typing import Self

from pydantic import BaseModel, ConfigDict, StrictBool, field_validator

from .proactive_policy import minute_of_day


class RestHoursConfig(BaseModel):
    """Daily rest window in BOT_TIMEZONE where interactive chat and scheduled patrols pause."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: StrictBool = True
    rest_hours: str = "23:00-08:00"
    allow_owner_private: StrictBool = False
    day_rest_enabled: StrictBool = True

    @field_validator("rest_hours")
    @classmethod
    def valid_rest_hours(cls, value: str) -> str:
        parts = value.split("-")
        if len(parts) != 2 or minute_of_day(parts[0]) == minute_of_day(parts[1]):
            raise ValueError("Rest hours must be two different HH:MM times")
        return value

    @classmethod
    def from_settings(cls, settings) -> Self:
        return cls(
            enabled=settings.rest_hours_enabled,
            rest_hours=settings.rest_hours,
            allow_owner_private=settings.rest_allow_owner_private,
            day_rest_enabled=settings.day_rest_enabled,
        )

    def active(self, local: datetime) -> bool:
        """Return whether the configured rest window covers the given local datetime."""
        if not self.enabled:
            return False
        start, end = map(minute_of_day, self.rest_hours.split("-"))
        current = local.hour * 60 + local.minute
        return start <= current < end if start < end else current >= start or current < end
