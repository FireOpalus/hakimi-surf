import json

from ..diary_sync import DiaryDriveSync
from ..reply_style import trim_complete_text
from ..roaming_logger import PatrolDiary
from ..security import Rejected
from ..thinking_policy import ThinkingScene
from . import SystemModule


class Module(SystemModule):
    module_id = "patrol_diary"
    display_name = "巡逻日记"
    description = "在本地整理巡逻记录，并按日生成摘要。"
    requires = ("drive_sync",)
    drive_usage = "每日巡逻日记"
    admin_panel = "journal"
    thinking_scenes = (ThinkingScene("daily_diary", "每日总结", output_cap="diary_output_tokens"),)

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.registry = registry
        self.provider = registry.systems["drive_sync"]
        self.diary = resources.diary or PatrolDiary(
            resources.settings, resources.store, gateway=resources.gateway
        )
        self.delivery = DiaryDriveSync(
            self.diary, self.provider.drive, resources.store, write_day=self.write_day
        )

    async def write_day(self, day, markdown):
        return await self.provider.sync_file(self, day + ".md", markdown)

    async def write_drive_text(self, drive, key, content):
        if not key.endswith(".md"):
            raise Rejected("drive_invalid_diary")
        return await drive.sync_day(key[:-3], content)

    async def initialize(self):
        await self.diary.initialize()
        await self.delivery.initialize()
        last_success, last_error = await self.delivery.last_receipt()
        await self.provider.import_receipt(self, last_success, last_error)

    def workers(self):
        return (
            (self.diary.run_local, self.delivery.run)
            if self.provider.drive is not None
            else (self.diary.run_local,)
        )

    def public_event_observers(self):
        return {"diary.milestone": self.record_milestone}

    async def record_milestone(self, event, now):
        if event.payload.get("private_first") or now >= event.created_at + 86400:
            return False
        source = self.registry.get(event.source_module)
        if source and source.inactive_reason:
            return False
        entry = await source.public_patrol_entry(event) if source else None
        if entry is not None:
            key, summary = entry
            if not summary:
                return False
            await self.diary.patrol(key, summary, now=event.created_at)
            return True
        if summary := event.payload.get("patrol_summary"):
            await self.diary.patrol("milestone:" + event.id, summary, now=event.created_at)
            return True
        if event.importance != "high":
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
