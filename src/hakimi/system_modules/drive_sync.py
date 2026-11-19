from ..diary_sync import DiaryDriveSync
from ..drive_client import FolderDrive
from ..security import Rejected
from . import SystemModule


class Module(SystemModule):
    module_id = "drive_sync"
    display_name = "Google Drive 同步"
    description = "将日记同步到已授权的隔离文件夹。"
    requires = ("patrol_diary",)
    admin_panel = "journal"

    def __init__(self, resources, registry):
        diary = registry.systems["patrol_diary"].diary
        drive = resources.drive if resources.drive is not None else getattr(diary, "drive", None)
        self.issue = None
        s = resources.settings
        if drive is None:
            path = (
                s.gdrive_oauth_credentials_file
                if s.gdrive_auth_mode == "oauth"
                else s.gdrive_service_account_file
            )
            if not s.gdrive_roaming_folder_id or not path or resources.http is None:
                self.issue = "等待配置隔离文件夹和对应凭据"
            else:
                try:
                    drive = FolderDrive(
                        s, resources.http, resources.store.vault.digest("diary-writer", s.namespace)
                    )
                except Rejected:
                    self.issue = "隔离文件夹配置无效"
        self.sync = DiaryDriveSync(diary, drive, resources.store)

    async def initialize(self):
        await self.sync.initialize()

    def workers(self):
        return (self.sync.run,) if self.sync.drive is not None else ()

    async def status(self):
        if self.issue:
            return {"state": "unconfigured", "detail": self.issue}
        return await self.sync.status()
