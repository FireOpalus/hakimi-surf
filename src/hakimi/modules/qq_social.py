"""QQ social capability module (KR node, LOW attention)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..budget_policy import PROACTIVE_LANE
from ..config import Settings
from ..hot_ring import HotRing
from .actions import Action, ActionCall, DailyAction, Handler, Outcome
from .base import AttentionLevel, BaseModule, GoalResult, NodeRegion
from .qq_schedule import QQScheduleOptions
from .scheduling import ActivityQuota, LocalActivity


class QQSocialModule(BaseModule):
    """Summarizes active QQ transport and group ring activity without leaking private content."""

    module_id: str = "qq_social"
    schedule_options = QQScheduleOptions
    recipient_namespaces = ("owner", "onebot")
    node_region: NodeRegion = "KR"
    attention_level: AttentionLevel = "LOW"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        hot_ring: HotRing | None = None,
        transport: Any = None,
        service: Any = None,
        speak: Handler | None = None,
        digest: Handler | None = None,
        preflight=None,
        proactive=None,
        connected: Callable[[], bool] | None = None,
    ) -> None:
        super().__init__(settings)
        self._hot_ring = hot_ring or getattr(service, "hot", None)
        self._transport = transport or getattr(getattr(service, "sender", None), "onebot", None)
        self.digest = digest or self._offline
        self.speak = speak or self._offline
        self.proactive = proactive or getattr(service, "proactive", None)
        self.preflight = preflight
        self.connected = connected or (lambda: False)
        if service is not None:
            # Older composition roots supplied the Service object.
            from .qq_delivery import LifeSpeaker
            from .qq_policy import QQPolicy

            self.digest = service.proactive.world_digest
            self.speak = LifeSpeaker(service).speak
            self.preflight = QQPolicy(service).evaluate
            self.connected = service.proactive.connected

    def actions(self) -> tuple[Action, ...]:
        aliases = {
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
            "bilibili_surf": (
                "group_share",
                "group_chat_share",
                "bili_share",
                "bilibili_surf_milestone_share",
            ),
            "minecraft": ("mc_share", "minecraft_milestone_share", "group_share"),
        }
        return (
            Action(
                name="qq.speak",
                description="结合最新会话向获准的群或 Owner 临场发言",
                social=True,
                budget_lane=PROACTIVE_LANE,
                window="active",
                quotas=(
                    ActivityQuota("owner_dms", "owner_dms_unlimited", owner_only=True),
                    ActivityQuota(),
                ),
                run=self.speak,
                preflight=self.preflight,
                unavailable=lambda: None if self.connected() else "onebot_offline",
                aliases=tuple(
                    (module, verb, True) for module, verbs in aliases.items() for verb in verbs
                ),
            ),
            Action(
                name="qq.owner_digest",
                description="在活跃时段结束前汇总当天的公开巡逻记录，投递到 Owner 私聊",
                social=True,
                owner_only=True,
                budget_lane=PROACTIVE_LANE,
                origins=frozenset({"timer"}),
                reuses_paid_work=self.proactive.has_digest_attempt if self.proactive else None,
                window="closing",
                interrupts_focus=True,
                quotas=(),
                default_mode="off"
                if self.settings and not self.settings.daily_digest_enabled
                else "active",
                run=self.digest,
            ),
        )

    def schedules(self):
        if self.settings and self.settings.onebot_enabled and self.settings.owner_qq_id:
            return (
                DailyAction(
                    "qq.owner_digest",
                    at=None,
                    target_scope="owner:" + self.settings.owner_qq_id,
                    label="Owner 每日汇报",
                    enabled=(lambda: self.proactive.configured) if self.proactive else None,
                    attempted=(lambda day: self.proactive.digest_attempted(day))
                    if self.proactive
                    else None,
                ),
            )
        return ()

    def normalize_recipient(self, scope):
        from .qq_audience import _normalize_target_scope

        return _normalize_target_scope(scope, self.settings) if self.settings else None

    def permits_recipient(self, scope, config):
        return not scope.startswith("owner:") or self.scheduling(config).owner_delivery_enabled

    async def planning_context(self, service, config, now):
        from .qq_audience import _allowlisted_groups
        from .qq_policy import _desktop_latest_activity

        options = self.scheduling(config)
        desktop = await _desktop_latest_activity(service)
        return {
            "available_groups": [
                "onebot:" + group for group in _allowlisted_groups(self.settings)[:20]
            ],
            "owner_dm_available": bool(
                options.owner_delivery_enabled
                and self.settings.onebot_enabled
                and self.settings.owner_qq_id
            ),
            "desktop_active_recently": bool(
                desktop["enabled"]
                and desktop["latest_timestamp"] > 0
                and 0 <= now - desktop["latest_timestamp"] < options.desktop_active_suppress_seconds
            ),
        }

    def local_activity(self, context, available, unfunded):
        if "qq.speak" not in available:
            return None
        groups = context.get("available_groups", [])
        if context.get("recent_bili_surfed") and groups:
            return LocalActivity(
                20,
                {
                    "target_module": "bilibili_surf",
                    "action_type": "group_chat_share",
                    "target_scope": groups[0],
                    "delay_minutes": 15,
                    "ttl_minutes": 90,
                    "intent_note": "刚刷到一条有意思的公开视频，看看群里气氛合适就聊两句",
                },
            )
        if context["desktop_active_recently"] and context["owner_dm_available"]:
            return LocalActivity(
                10,
                {
                    "target_module": self.module_id,
                    "action_type": "owner_desktop_warm_care",
                    "target_scope": "owner:" + self.settings.owner_qq_id,
                    "delay_minutes": 20,
                    "ttl_minutes": 90,
                    "allow_desktop_care": True,
                    "intent_note": "留意到对方刚在电脑端忙完正事，合适时关心一句注意休息喝水",
                },
            )
        return None

    def event_rules(self):
        from ..life_rules import milestone_sharing

        return (milestone_sharing(self.settings),)

    async def _offline(self, call: ActionCall) -> Outcome:
        return Outcome("not_dispatched", "onebot_offline")

    def _resolve_connection_label(self) -> str:
        transport = self._transport
        if transport is not None:
            status_fn = getattr(transport, "status", None)
            if callable(status_fn):
                status = status_fn()
                if isinstance(status, dict) and "online" in status:
                    return "在线" if status["online"] else "离线"
            ready = getattr(transport, "ready", None)
            is_set = getattr(ready, "is_set", None)
            if callable(is_set):
                return "在线" if is_set() else "离线"
            return "HTTP就绪"
        settings = self.settings
        if settings is not None:
            if getattr(settings, "onebot_enabled", False) or getattr(settings, "qq_enabled", False):
                return "已启用"
            return "未启用"
        return "待机"

    async def _raw_sensor_summary(self) -> str:
        conn_label = self._resolve_connection_label()
        hot_ring = self._hot_ring
        rings = getattr(hot_ring, "rings", {}) if hot_ring is not None else {}
        active_groups = 0
        group_buffered = 0
        for key, ring in rings.items():
            # Strictly aggregate group metadata only; never inspect or expose private owner chats.
            if isinstance(key, tuple) and len(key) >= 3 and key[2] == "group":
                active_groups += 1
                messages = getattr(ring, "messages", None)
                if messages is not None:
                    group_buffered += len(messages)
        parts = [f"QQ:{conn_label}", f"活跃群:{active_groups}", f"群缓冲:{group_buffered}"]
        if self.proactive_paused:
            parts.append("主动暂停")
        return " ".join(parts)

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        action_type = str(ticket.get("action_type", "")).strip()
        if self.proactive_paused and action_type != "status":
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                summary="QQ主动互动已因高专注锁定暂停",
                reason="proactive_paused",
            )
        summary = await self.get_sensor_summary()
        return GoalResult(
            ok=True,
            module_id=self.module_id,
            summary=summary,
            artifacts={"action_type": action_type[:64]},
        )
