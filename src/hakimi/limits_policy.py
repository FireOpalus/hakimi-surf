"""Hot-configurable runtime rate limits, send delays, output caps, and patrol limits."""

from typing import Literal, Self

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
    """Output ceilings and the legacy auto-expert flag retained for older clients."""

    carbon_enabled: StrictBool = True
    chat_output_tokens: StrictInt = Field(title="常规对话与记忆压缩", default=1536, ge=128, le=4096)
    owner_output_tokens: StrictInt = Field(title="Owner 私聊", default=4096, ge=128, le=8192)
    summary_output_tokens: StrictInt = Field(title="内容摘要", default=4096, ge=128, le=8192)
    diary_output_tokens: StrictInt = Field(title="巡逻日记", default=4096, ge=128, le=8192)
    high_output_tokens: StrictInt = Field(
        title="单次深度分析（/deep）", default=8192, ge=128, le=16384
    )


class PatrolLimitConfig(_LimitsBase):
    """Subscription count cap and Bilibili patrol polling interval."""

    max_subscriptions: StrictInt = Field(default=10, ge=1, le=50)
    patrol_interval_seconds: StrictInt = Field(default=1800, ge=900, le=86400)


class ImageLimitsConfig(_LimitsBase):
    """Hot-configurable inbound image byte cap, pixel ceiling, and resize edges."""

    image_max_bytes: StrictInt = Field(default=16_777_216, ge=1_048_576, le=20_971_520)
    image_max_pixels: StrictInt = Field(default=50_000_000, ge=65536, le=80_000_000)
    image_standard_max_edge: StrictInt = Field(default=1600, ge=512, le=2560)
    image_tall_max_edge: StrictInt = Field(default=2560, ge=1024, le=4096)

    @model_validator(mode="after")
    def edge_order(self) -> Self:
        if self.image_tall_max_edge < self.image_standard_max_edge:
            raise ValueError("image_tall_max_edge must be >= image_standard_max_edge")
        return self


class ContentToolsConfig(_LimitsBase):
    """Hot-configurable text/code file reader limits and rich Markdown card mode."""

    file_read_enabled: StrictBool = True
    file_max_bytes: StrictInt = Field(default=131_072, ge=16_384, le=262_144)
    file_daily_limit_per_user: StrictInt = Field(default=15, ge=0, le=100)
    text_card_mode: Literal["rich_auto", "fallback_only"] = "rich_auto"
