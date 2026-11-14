"""Validated per-scenario reasoning effort; never selected by model/user payloads."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

ThinkingLevel = Literal["low", "medium", "high", "xhigh"]
THINKING_LEVELS = ("low", "medium", "high", "xhigh")
SETTING_FIELDS = {
    "fast_chat": "fast_chat_thinking_level",
    "owner_private": "owner_thinking_level",
    "bilibili_summary": "summary_thinking_level",
    "daily_diary": "diary_thinking_level",
    "memory_compression": "memory_compression_thinking_level",
    "owner_digest": "owner_digest_thinking_level",
    "owner_companion": "owner_companion_thinking_level",
    "bili_selection": "bili_selection_thinking_level",
    "bili_learning": "bili_learning_thinking_level",
}


class ThinkingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    fast_chat: ThinkingLevel = "medium"
    owner_private: ThinkingLevel = "medium"
    bilibili_summary: ThinkingLevel = "medium"
    daily_diary: ThinkingLevel = "medium"
    memory_compression: ThinkingLevel = "medium"
    owner_digest: ThinkingLevel = "medium"
    owner_companion: ThinkingLevel = "medium"
    bili_selection: ThinkingLevel = "medium"
    bili_learning: ThinkingLevel = "medium"

    @classmethod
    def from_settings(cls, settings):
        return cls(**{scene: getattr(settings, field) for scene, field in SETTING_FIELDS.items()})
