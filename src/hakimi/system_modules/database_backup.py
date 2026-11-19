from functools import partial

from ..db_backup import DatabaseBackup
from . import SystemModule


class Module(SystemModule):
    module_id = "database_backup"
    display_name = "数据库备份"
    description = "每日校验并加密保存 SQLite 快照，保留最近七天。"
    requires = ("drive_sync",)
    drive_accepts_files = False
    drive_usage = "加密数据库备份"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        provider = registry.systems["drive_sync"]
        self.backup = DatabaseBackup(
            resources.settings,
            resources.store,
            provider.drive if self.drive_permitted else None,
            transfer=partial(provider.transfer, self),
        )

    @property
    def drive_permitted(self):
        return self.resources.settings.roaming_enabled

    def workers(self):
        return (self.backup.run,) if self.drive_permitted else ()

    async def status(self):
        return {"state": "ready", "detail": "按日备份" if self.drive_permitted else "等待归档授权"}
