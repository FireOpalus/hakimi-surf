"""Validated, immutable hot-window policies shared by env, admin and session storage."""

from pydantic import BaseModel, ConfigDict, Field, model_validator


class WindowPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    messages: int = Field(default=40, ge=2, le=500)
    chars: int = Field(default=6000, ge=1, le=64000)
    keep: int = Field(default=16, ge=1, le=499)

    @model_validator(mode="after")
    def check_window(self):
        if self.keep >= self.messages:
            raise ValueError("keep must be less than messages")
        return self

    def due(self, count, chars):
        return count > self.keep and (chars >= self.chars or count >= self.messages)

    @property
    def hard_limit(self):
        return self.messages * 2


class GroupWindowPolicy(WindowPolicy):
    messages: int = Field(default=60, ge=2, le=500)
    keep: int = Field(default=25, ge=1, le=499)
    min_chars: int = Field(default=1200, ge=0, le=64000)
    max_messages: int = Field(default=90, ge=2, le=500)

    @model_validator(mode="after")
    def check_deferral(self):
        if self.max_messages < self.messages:
            raise ValueError("max_messages must be at least messages")
        if self.min_chars > self.chars:
            raise ValueError("min_chars must not exceed chars")
        return self

    def due(self, count, chars):
        return count > self.keep and (
            chars >= self.chars
            or (count >= self.messages and (chars >= self.min_chars or count >= self.max_messages))
        )

    @property
    def hard_limit(self):
        return self.max_messages * 2


class CompressionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    enabled: bool = True
    group: GroupWindowPolicy = Field(default_factory=GroupWindowPolicy)
    private: WindowPolicy = Field(default_factory=WindowPolicy)

    def window(self, kind):
        return self.group if kind == "group" else self.private

    @classmethod
    def from_settings(cls, settings):
        return cls(
            enabled=settings.compression_enabled,
            group=GroupWindowPolicy(
                messages=settings.group_compression_messages,
                chars=settings.group_compression_chars,
                keep=settings.group_compression_keep,
                min_chars=settings.group_compression_min_chars,
                max_messages=settings.group_compression_max_messages,
            ),
            private=WindowPolicy(
                messages=settings.private_compression_messages,
                chars=settings.private_compression_chars,
                keep=settings.private_compression_keep,
            ),
        )
