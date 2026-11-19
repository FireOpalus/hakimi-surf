import json

from ..reply_style import trim_complete_text
from ..roaming_logger import PatrolDiary
from ..thinking_policy import ThinkingScene
from . import SystemModule


class Module(SystemModule):
    module_id = "patrol_diary"
    display_name = "巡逻日记"
    description = "在本地整理巡逻记录，并按日生成摘要。"
    admin_panel = "journal"
    thinking_scenes = (ThinkingScene("daily_diary", "每日总结", output_cap="diary_output_tokens"),)

    def __init__(self, resources, registry):
        self.diary = resources.diary or PatrolDiary(
            resources.settings, resources.store, gateway=resources.gateway
        )

    async def initialize(self):
        await self.diary.initialize()

    def workers(self):
        return (self.diary.run_local,)

    def public_event_observers(self):
        return {"diary.milestone": self.record_milestone}

    async def record_milestone(self, event, now):
        if (
            event.importance != "high"
            or now >= event.created_at + 86400
            or event.event_type in {"bili_video_watched", "bili_mc_inspiration"}
        ):
            return False
        data = json.dumps(event.payload, ensure_ascii=False, separators=(",", ":"))
        summary = trim_complete_text(f"重要动态 {event.event_type} {data}", 120)
        await self.diary.world_event(
            "milestone:" + event.id,
            event.source_module,
            event.event_type,
            summary,
            now=event.created_at,
        )
        return True

    async def status(self):
        day = self.diary.local().date().isoformat()
        async with self.diary.store.lock:
            row = await self.diary.store.one(
                "SELECT status FROM roaming_summaries WHERE day=?", (day,)
            )
        detail = {
            "done": "今日总结已保存",
            "attempted": "今日总结已登记，等待确认",
            "failed": "今日总结未完成，已保留本地明细",
        }.get(row[0] if row else None, "本地记录中")
        return {"state": "ready", "detail": detail}
