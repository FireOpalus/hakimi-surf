"""Built-in platform-neutral proactive social capability module (`social_presence`)."""

from __future__ import annotations

from typing import Any

from .budget_policy import PROACTIVE_LANE
from .modules.actions import Action, ActionCall, Alias, GateDecision, Outcome
from .modules.base import AttentionLevel, BaseModule, NodeRegion
from .modules.scheduling import ActivityQuota, LocalActivity, SocialScheduleOptions
from .social import owner_target_scope
from .social_policy import SocialProactivePolicy, _desktop_latest_activity
from .social_speak import resolve_social_message, speak_proactive

_LEGACY_SPEAK_VERBS: dict[str, tuple[str, ...]] = {
    "qq_social": (
        "group_share",
        "group_chat_share",
        "owner_silence_care",
        "owner_desktop_warm_care",
        "owner_companion",
        "companion",
        "check_in",
        "minecraft_milestone_share",
        "qq_social_milestone_share",
        "proactive_share",
    ),
    "social_presence": (
        "speak",
        "group_share",
        "group_chat_share",
        "owner_silence_care",
        "owner_desktop_warm_care",
        "owner_companion",
        "companion",
        "check_in",
        "social_presence_milestone_share",
        "proactive_share",
    ),
    "bilibili_surf": (
        "group_share",
        "group_chat_share",
        "bili_share",
        "bilibili_surf_milestone_share",
    ),
    "minecraft": (
        "mc_share",
        "minecraft_milestone_share",
        "group_share",
    ),
    "telegram": ("speak",),
}

_LEGACY_SPEAK_ALIASES: tuple[Alias, ...] = tuple(
    (module, verb, True) for module, verbs in _LEGACY_SPEAK_VERBS.items() for verb in verbs
)


class SocialPresenceModule(BaseModule):
    """Owns unified `social.speak` scheduling, cooldowns, and desktop suppression."""

    module_id: str = "social_presence"
    display_name = "社交主动交互"
    description = "跨平台主动发言调度、频率冷却与静默保护"
    node_region: NodeRegion = "KR"
    attention_level: AttentionLevel = "LOW"
    thinking_scenes = ()
    chat_platforms = ()
    recipient_namespaces = ()
    schedule_options = SocialScheduleOptions

    def __init__(self, service: Any) -> None:
        super().__init__(getattr(service, "s", None))
        self._service = service

    def actions(self) -> tuple[Action, ...]:
        return (
            Action(
                name="social.speak",
                description=(
                    "结合近期见闻、B 站冲浪收获或当前会话，去获准的群聊冒泡接话，"
                    "或给 BOISER 发条日常私聊"
                ),
                social=True,
                proactive_speak=True,
                budget_lane=PROACTIVE_LANE,
                window="active",
                quotas=(
                    ActivityQuota("owner_dms", "owner_dms_unlimited", owner_only=True),
                    ActivityQuota(),
                ),
                run=self.speak,
                preflight=self.preflight,
                unavailable=self.unavailable,
                aliases=_LEGACY_SPEAK_ALIASES,
                name_aliases=("qq.speak",),
            ),
        )

    def event_rules(self) -> tuple[Any, ...]:
        if self.settings is None:
            return ()
        from .life_rules import milestone_sharing

        return (milestone_sharing(self.settings),)

    def unavailable(self) -> str | None:
        registry = getattr(self._service, "modules", None)
        if registry is None:
            return "no_chat_platform_connected"
        offline_reasons: set[str] = set()
        for plat, mod in registry.chat_platforms.items():
            if mod.inactive_reason is not None:
                continue
            if mod.chat_connected(plat):
                return None
            offline_reasons.add(mod.offline_reason(plat))
        if not offline_reasons:
            return "no_chat_platform_connected"
        if len(offline_reasons) == 1:
            return next(iter(offline_reasons))
        return "no_chat_platform_connected"

    async def preflight(self, ticket: dict[str, Any], now: float, cfg: Any) -> GateDecision:
        target_scope = str(ticket.get("target_scope") or "")
        target_msg = resolve_social_message(
            self._service,
            target_scope,
            event_id=f"gate:{ticket['id']}",
            created_at=now,
        )
        if target_msg is None:
            return GateDecision("unauthorized_scope")
        registry = getattr(self._service, "modules", None)
        mod = registry.chat_platforms.get(target_msg.platform) if registry is not None else None
        if mod is None or mod.inactive_reason is not None:
            reason = (
                mod.inactive_reason
                if (mod is not None and mod.inactive_reason)
                else "unauthorized_scope"
            )
            return GateDecision(reason)
        if not mod.chat_connected(target_msg.platform):
            return GateDecision(mod.offline_reason(target_msg.platform), delay=120)
        policy = SocialProactivePolicy(
            self._service,
            target_msg.platform,
            lambda m=target_msg: m.bot_id,
        )
        return await policy.evaluate(ticket, now, cfg, target_msg=target_msg)

    async def speak(self, call: ActionCall) -> Outcome:
        target_scope = str(call.ticket.get("target_scope") or "")
        target_msg = resolve_social_message(
            self._service,
            target_scope,
            event_id=f"jit:{call.ticket['id']}",
            created_at=call.now,
        )
        if target_msg is None:
            return Outcome("gated_skip", "unauthorized_scope")
        registry = getattr(self._service, "modules", None)
        mod = registry.chat_platforms.get(target_msg.platform) if registry is not None else None
        if mod is None or mod.inactive_reason is not None:
            reason = (
                mod.inactive_reason
                if (mod is not None and mod.inactive_reason)
                else "unauthorized_scope"
            )
            return Outcome("gated_skip", reason)
        if not mod.chat_connected(target_msg.platform):
            return Outcome("not_dispatched", mod.offline_reason(target_msg.platform))
        return await speak_proactive(self._service, target_msg, call)

    async def planning_context(self, service: Any, config: Any, now: float) -> dict[str, Any]:
        options = self.scheduling(config)
        desktop = await _desktop_latest_activity(service)
        return {
            "desktop_active_recently": bool(
                desktop["enabled"]
                and desktop["latest_timestamp"] > 0
                and 0 <= now - desktop["latest_timestamp"] < options.desktop_active_suppress_seconds
            )
        }

    def local_activity(
        self, context: dict[str, Any], available: set[str], unfunded: bool
    ) -> LocalActivity | None:
        if "social.speak" not in available:
            return None
        groups = context.get("available_groups", [])
        if context.get("current_world_state", {}).get("public_unshared_count", 0) and groups:
            return LocalActivity(
                20,
                {
                    "target_module": "bilibili_surf",
                    "action_type": "group_chat_share",
                    "target_scope": groups[0],
                    "delay_minutes": 15,
                    "ttl_minutes": 90,
                    "intent_note": "有一条还没分享的公开见闻，群里聊得上就说两句",
                },
            )
        if context.get("desktop_active_recently") and context.get("owner_dm_available"):
            platform = (
                getattr(self.settings, "owner_delivery_platform", "onebot")
                if self.settings is not None
                else "onebot"
            )
            target_scope = (
                owner_target_scope(self.settings, platform) if self.settings is not None else None
            )
            if target_scope:
                return LocalActivity(
                    10,
                    {
                        "target_module": self.module_id,
                        "action_type": "owner_desktop_warm_care",
                        "target_scope": target_scope,
                        "delay_minutes": 20,
                        "ttl_minutes": 90,
                        "allow_desktop_care": True,
                        "intent_note": "留意到对方刚在电脑端忙完正事，合适时关心一句注意休息喝水",
                    },
                )
        return None

    async def social_material(self, ticket: dict[str, Any]) -> str:
        return ""

    async def _raw_sensor_summary(self) -> str:
        registry = getattr(self._service, "modules", None)
        if registry is None:
            return "社交主动:就绪"
        seen: set[str] = set()
        active = 0
        online = 0
        for plat, mod in registry.chat_platforms.items():
            if mod.inactive_reason is not None:
                continue
            if mod.module_id not in seen:
                seen.add(mod.module_id)
                active += 1
            if mod.chat_connected(plat):
                online += 1
        return f"社交通道:{active} 已连:{online}"
