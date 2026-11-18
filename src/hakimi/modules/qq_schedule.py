from pydantic import Field

from .scheduling import ScheduleOptions


class QQScheduleOptions(ScheduleOptions):
    owner_delivery_enabled: bool = Field(True, title="允许向 Owner 投递私信与运维告警")
    owner_dms_unlimited: bool = Field(False, title="Owner 主动私信不限次数")
    group_cooldown_seconds: int = Field(1500, ge=300, le=86400, title="群聊主动冷却（秒）")
    group_active_window_seconds: int = Field(1800, ge=60, le=7200, title="群聊活跃观察窗口（秒）")
    group_min_recent_messages: int = Field(2, ge=1, le=50, title="群聊最少有效消息数")
    desktop_active_suppress_seconds: int = Field(
        7200, ge=300, le=86400, title="桌面活跃抑制窗口（秒）"
    )

    @classmethod
    def from_settings(cls, settings):
        if settings is None:
            return cls()
        return cls(
            owner_delivery_enabled=settings.proactive_dm_enabled,
            owner_dms_unlimited=settings.life_engine_owner_dms_unlimited,
        )
