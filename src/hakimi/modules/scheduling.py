"""Typed scheduling controls and quota declarations owned by registered modules."""

from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

CLOCK_PATTERN = r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$"
ScheduleTime = Annotated[str, Field(pattern=CLOCK_PATTERN)]


class ScheduleOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, allow_inf_nan=False)

    @classmethod
    def from_settings(cls, settings):
        return cls()


@dataclass(frozen=True)
class ActivityQuota:
    """First matching rule applies. An empty option always uses the shared allowance."""

    counter: str = ""
    unlimited_option: str = ""
    owner_only: bool = False


@dataclass(frozen=True)
class LocalActivity:
    """An optional offline suggestion. The planner still validates the chosen action."""

    priority: int
    intent: dict[str, Any]
    one_outstanding: bool = False
    focus: tuple[Literal["LOW", "MEDIUM", "HIGH"], str] | None = None
