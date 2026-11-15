"""Hot-configurable runtime rate limits, send delays, output caps, and patrol limits."""

from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)


class _LimitsBase(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    @classmethod
    def from_settings(cls, settings) -> Self:
        return cls(**{name: getattr(settings, name) for name in cls.model_fields})


class RateLimitConfig(_LimitsBase):
    """Per-user and global RPM caps, pending queue capacity, and send delay window."""

    user_requests_per_minute: StrictInt = Field(default=6, ge=1, le=100)
    global_requests_per_minute: StrictInt = Field(default=30, ge=1, le=1000)
    queue_capacity: StrictInt = Field(default=200, ge=1, le=10000)
    send_delay_min: float = Field(default=0.8, ge=0, le=5)
    send_delay_max: float = Field(default=2.0, ge=0, le=10)

    @field_validator("send_delay_min", "send_delay_max", mode="before")
    @classmethod
    def finite_number(cls, value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("Delay must be a finite number")
        return float(value)

    @model_validator(mode="after")
    def delay_order(self) -> Self:
        if self.send_delay_max < self.send_delay_min:
            raise ValueError("send_delay_max must be >= send_delay_min")
        return self


class GenerationCapsConfig(_LimitsBase):
    """Carbon auto-expert switch and per-scenario generation max_output_tokens caps."""

    carbon_enabled: StrictBool = True
    chat_output_tokens: StrictInt = Field(default=1536, ge=128, le=4096)
    owner_output_tokens: StrictInt = Field(default=4096, ge=128, le=8192)
    summary_output_tokens: StrictInt = Field(default=4096, ge=128, le=8192)
    diary_output_tokens: StrictInt = Field(default=4096, ge=128, le=8192)
    high_output_tokens: StrictInt = Field(default=8192, ge=128, le=16384)


class PatrolLimitConfig(_LimitsBase):
    """Subscription count cap and Bilibili patrol polling interval."""

    max_subscriptions: StrictInt = Field(default=10, ge=1, le=50)
    patrol_interval_seconds: StrictInt = Field(default=1800, ge=900, le=86400)
