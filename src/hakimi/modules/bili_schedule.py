from pydantic import Field

from .scheduling import ScheduleOptions


class BiliScheduleOptions(ScheduleOptions):
    bili_patrols_unlimited: bool = Field(False, title="B 站自主巡逻不限次数")

    @classmethod
    def from_settings(cls, settings):
        return cls(
            bili_patrols_unlimited=bool(settings and settings.life_engine_bili_patrols_unlimited)
        )
