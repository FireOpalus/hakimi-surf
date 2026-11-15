"""Server-selected scenarios. Neither model output nor message text sets this policy."""

from dataclasses import dataclass
from enum import StrEnum

from .config import Settings
from .security import Rejected
from .thinking_policy import ThinkingConfig, ThinkingLevel


class Scenario(StrEnum):
    FAST_CHAT = "fast_chat"
    OWNER_PRIVATE = "owner_private"
    BILIBILI_SUMMARY = "bilibili_summary"
    DAILY_DIARY = "daily_diary"
    MEMORY_COMPRESSION = "memory_compression"
    OWNER_DIGEST = "owner_digest"
    OWNER_COMPANION = "owner_companion"
    BILI_SELECTION = "bili_selection"
    BILI_LEARNING = "bili_learning"


@dataclass(frozen=True)
class GenerationPlan:
    thinking_level: ThinkingLevel
    output_tokens: int
    model_lane: str = "hydrogen"


def generation_plan(
    settings: Settings, scenario: Scenario, *, deep: bool = False, route=None
) -> GenerationPlan:
    caps = {
        Scenario.FAST_CHAT: settings.chat_output_tokens,
        Scenario.OWNER_PRIVATE: settings.owner_output_tokens,
        Scenario.BILIBILI_SUMMARY: settings.summary_output_tokens,
        Scenario.DAILY_DIARY: settings.diary_output_tokens,
        Scenario.MEMORY_COMPRESSION: settings.chat_output_tokens,
        Scenario.OWNER_DIGEST: 4096,
        Scenario.OWNER_COMPANION: 512,
        Scenario.BILI_SELECTION: 1536,
        Scenario.BILI_LEARNING: 4096,
    }
    if scenario not in caps:
        raise Rejected("invalid_generation_scenario")
    policy = settings._thinking_override or ThinkingConfig.from_settings(settings)
    level, cap = getattr(policy, str(scenario)), caps[scenario]
    if scenario in {Scenario.FAST_CHAT, Scenario.OWNER_PRIVATE}:
        # Security sandbox has precedence over both manual controls and /deep.
        if route is not None and getattr(route, "sandbox_mode", False):
            return GenerationPlan("low", 1536)
        if level == "auto":
            tier = getattr(route, "tier", 2)
            if not deep:
                return {
                    1: GenerationPlan("low", 1536),
                    2: GenerationPlan("medium", 4096),
                    3: GenerationPlan("high", 8192),
                    4: GenerationPlan("medium", 12288, "carbon"),
                }[tier]
            level = "high"
    if deep:
        if scenario not in {
            Scenario.OWNER_PRIVATE,
            Scenario.BILIBILI_SUMMARY,
            Scenario.DAILY_DIARY,
        }:
            raise Rejected("deep_not_allowed_for_scenario")
        level = "xhigh" if level == "xhigh" else "high"
        # Preserve the existing explicit /deep cap. Changing a level in /admin alone
        # never changes output/thought token caps or ledger reservation multipliers.
        cap = settings.high_output_tokens
    return GenerationPlan(level, cap)
