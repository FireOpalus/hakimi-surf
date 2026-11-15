"""Server-owned OneBot admission rules; identities never come from model output."""

import math
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DEFAULT_BOT_REPLY_THRESHOLD = 0.85


def _validate_threshold_number(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Bot reply threshold must be a finite number")
    result = float(value)
    if not math.isfinite(result) or not (0.01 <= result <= 1.00):
        raise ValueError("Bot reply threshold must be between 0.01 and 1.00")
    return result


def effective_bot_reply_threshold(user_id: str, settings) -> float:
    overrides = getattr(settings, "bot_reply_thresholds", {}) or {}
    default = getattr(settings, "bot_reply_threshold", DEFAULT_BOT_REPLY_THRESHOLD)
    raw = overrides.get(user_id, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return DEFAULT_BOT_REPLY_THRESHOLD


def is_bot_hard_blocked(user_id: str, settings) -> bool:
    return effective_bot_reply_threshold(user_id, settings) >= 1.0


class AdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    allowed_groups: tuple[str, ...] = ()
    allowed_users: tuple[str, ...] = ()
    ignored_bot_ids: tuple[str, ...] = ()
    private_mode: Literal["owner_only", "owner_and_allowlist"] = "owner_only"
    bot_reply_threshold: float = Field(default=DEFAULT_BOT_REPLY_THRESHOLD, ge=0.01, le=1.00)
    bot_reply_thresholds: dict[str, float] = Field(default_factory=dict)

    @field_validator("allowed_groups", "allowed_users", "ignored_bot_ids", mode="before")
    @classmethod
    def identifiers(cls, values):
        if not isinstance(values, (list, tuple)) or len(values) > 1000:
            raise ValueError("Expected at most 1000 numeric identifiers")
        if any(not isinstance(v, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", v) for v in values):
            raise ValueError("QQ identifiers must be decimal strings without leading zeroes")
        if len(set(values)) != len(values):
            raise ValueError("Duplicate identifiers")
        return tuple(sorted(values))

    @field_validator("bot_reply_threshold", mode="before")
    @classmethod
    def valid_default_threshold(cls, value):
        return _validate_threshold_number(value)

    @field_validator("bot_reply_thresholds", mode="before")
    @classmethod
    def valid_bot_thresholds(cls, values):
        if not isinstance(values, dict) or len(values) > 1000:
            raise ValueError("Expected at most 1000 per-bot threshold overrides")
        cleaned = {}
        for key, value in sorted(values.items()):
            if not isinstance(key, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", key):
                raise ValueError("Bot identifiers must be decimal strings without leading zeroes")
            cleaned[key] = _validate_threshold_number(value)
        return cleaned

    @model_validator(mode="after")
    def bot_thresholds_subset(self):
        if not set(self.bot_reply_thresholds).issubset(set(self.ignored_bot_ids)):
            raise ValueError("Bot threshold target must be in ignored_bot_ids")
        return self

    @classmethod
    def from_settings(cls, settings):
        reserved = {settings.owner_qq_id, settings.onebot_self_id}
        bots = set(settings.bot_qq_ids)
        overrides = {
            k: v
            for k, v in (getattr(settings, "bot_reply_thresholds", {}) or {}).items()
            if k in bots
        }
        return cls(
            allowed_groups=sorted(settings.onebot_allowed_groups),
            allowed_users=sorted(settings.onebot_allowed_users - reserved),
            ignored_bot_ids=sorted(bots),
            private_mode=getattr(settings, "onebot_private_mode", "owner_only"),
            bot_reply_threshold=getattr(
                settings, "bot_reply_threshold", DEFAULT_BOT_REPLY_THRESHOLD
            ),
            bot_reply_thresholds=overrides,
        )
