"""Validated provider token observations, never inferred from local text lengths."""

from dataclasses import dataclass
from typing import Literal

VALID_THINKING_LEVELS = ("none", "minimal", "low", "medium", "high", "xhigh")
ThinkingLevelValue = Literal["none", "minimal", "low", "medium", "high", "xhigh"]


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    output_tokens: int
    cached_tokens: int | None = None
    thought_tokens: int | None = None
    model_lane: Literal["interaction", "decision", "hydrogen", "carbon", "memory_flex"] = (
        "interaction"
    )
    thinking_level: ThinkingLevelValue | None = None

    def __post_init__(self):
        for name in ("input_tokens", "output_tokens", "cached_tokens", "thought_tokens"):
            value = getattr(self, name)
            if value is None and name in {"cached_tokens", "thought_tokens"}:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 100_000_000
            ):
                raise ValueError("Invalid observed token count")
        if self.cached_tokens is not None and self.cached_tokens > self.input_tokens:
            raise ValueError("Cached tokens cannot exceed input tokens")
        if self.model_lane not in {"interaction", "decision", "hydrogen", "carbon", "memory_flex"}:
            raise ValueError("Invalid model lane")
        if self.thinking_level is not None and self.thinking_level not in VALID_THINKING_LEVELS:
            raise ValueError("Invalid thinking level")
