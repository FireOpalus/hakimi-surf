"""One validated snapshot for all debounce clocks; no timer polling or LLM calls."""

from pydantic import BaseModel, ConfigDict, Field


class DebounceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    group_debounce_seconds: float = Field(default=2.0, ge=0, le=30)
    mention_debounce_seconds: float = Field(default=0.6, ge=0, le=30)
    ordinary_incomplete_seconds: float = Field(default=3.0, ge=0, le=30)
    mention_incomplete_seconds: float = Field(default=2.2, ge=0, le=30)
    group_debounce_max_seconds: float = Field(default=5.0, ge=0, le=30)

    @classmethod
    def from_settings(cls, settings):
        return cls(**{name: getattr(settings, name) for name in cls.model_fields})
