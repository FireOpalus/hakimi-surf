"""Server-owned OneBot admission rules; identities never come from model output."""

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


class AdmissionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    allowed_groups: tuple[str, ...] = ()
    allowed_users: tuple[str, ...] = ()
    ignored_bot_ids: tuple[str, ...] = ()
    private_mode: Literal["owner_only", "owner_and_allowlist"] = "owner_only"

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

    @classmethod
    def from_settings(cls, settings):
        reserved = {settings.owner_qq_id, settings.onebot_self_id}
        return cls(
            allowed_groups=sorted(settings.onebot_allowed_groups),
            allowed_users=sorted(settings.onebot_allowed_users - reserved),
            ignored_bot_ids=sorted(settings.bot_qq_ids),
            private_mode=getattr(settings, "onebot_private_mode", "owner_only"),
        )
