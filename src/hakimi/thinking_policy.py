"""Validated per-scenario reasoning effort; never selected by model/user payloads."""

import re
from dataclasses import dataclass
from typing import Literal, get_args

from pydantic import BaseModel, ConfigDict, Field

ThinkingLevel = Literal["low", "medium", "high", "xhigh"]
ChatThinkingLevel = Literal["auto", "low", "medium", "high", "xhigh"]
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

    fast_chat: ChatThinkingLevel = "auto"
    owner_private: ChatThinkingLevel = "auto"
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


PrimaryModel = Literal["hydrogen", "neon", "carbon"]
PRIMARY_MODELS = {"hydrogen": "Hydrogen", "neon": "Neon", "carbon": "Carbon"}
FLEX_MODELS = {"memory_flex": "hydrogen", "neon_flex": "neon", "carbon_flex": "carbon"}
GENERATION_LANES = frozenset((*PRIMARY_MODELS, *FLEX_MODELS))
MODEL_LANES = GENERATION_LANES | {"decision"}
INTERACTION_LANES = GENERATION_LANES | {"interaction"}


def supported_level(model: PrimaryModel, level: str) -> str:
    return "high" if model == "carbon" and level == "xhigh" else level


OutputCap = Literal[
    "chat_output_tokens", "owner_output_tokens", "summary_output_tokens", "diary_output_tokens"
]


@dataclass(frozen=True)
class ThinkingScene:
    key: str
    label: str
    default: ChatThinkingLevel = "medium"
    levels: tuple[str, ...] = THINKING_LEVELS
    output_tokens: int = 2048

    output_cap: OutputCap | None = None

    def __post_init__(self):
        if (
            not re.fullmatch(r"[a-z][a-z0-9_.]{0,63}", self.key)
            or not self.label.strip()
            or len(self.label) > 80
            or self.default not in self.levels
            or not set(self.levels) <= {"auto", *THINKING_LEVELS}
            or len(set(self.levels)) != len(self.levels)
            or ("auto" in self.levels and self.key not in {"fast_chat", "owner_private"})
            or (self.output_cap is not None and self.output_cap not in get_args(OutputCap))
            or not 128 <= self.output_tokens <= 16384
        ):
            raise ValueError("Invalid thinking scene declaration")


CORE_THINKING_SCENES = (
    ThinkingScene("world.morning", "晨间行程规划", levels=("medium", "high")),
    ThinkingScene("world.revision", "日内行程调整"),
    ThinkingScene("memory_compression", "滚动记忆压缩", output_cap="chat_output_tokens"),
    ThinkingScene("web.search", "联网检索"),
)


class ModelPreferences(ThinkingConfig):
    primary_model: PrimaryModel = "hydrogen"
    scene_levels: dict[str, ChatThinkingLevel] = Field(default_factory=dict)

    @classmethod
    def from_settings(cls, settings):
        return cls(
            **ThinkingConfig.from_settings(settings).model_dump(),
            primary_model=settings.primary_model,
        )

    def level(self, scene: ThinkingScene):
        if scene.key in SETTING_FIELDS:
            return getattr(self, scene.key)
        saved = self.scene_levels.get(scene.key, scene.default)
        return saved if saved in scene.levels else scene.default


def preferences(settings) -> ModelPreferences:
    value = settings._thinking_override
    if isinstance(value, ModelPreferences):
        return value
    if value is not None:
        return ModelPreferences(**value.model_dump(), primary_model=settings.primary_model)
    return ModelPreferences.from_settings(settings)


class ModelSettingsUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    primary_model: PrimaryModel
    levels: dict[str, ChatThinkingLevel] = Field(max_length=256)
