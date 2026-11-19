from functools import partial

from ..surf_mirror import SurfMirror
from . import SystemModule


class Module(SystemModule):
    module_id = "surf_mirror"
    display_name = "分身镜像"
    description = "整理仅供 Owner 查阅的近期动态，保存最新镜像和每日副本。"
    requires = ("drive_sync",)
    drive_usage = "Owner 分身镜像"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        provider = registry.systems["drive_sync"]
        self.mirror = SurfMirror(
            resources.service,
            provider.drive if self.drive_permitted else None,
            write_file=partial(provider.sync_file, self),
        )

    async def write_drive_text(self, drive, key, content):
        return await self.mirror.write_drive_text(drive, key, content)

    @property
    def drive_permitted(self):
        return self.resources.settings.roaming_enabled

    def workers(self):
        return (self.mirror.run,) if self.drive_permitted else ()

    async def status(self):
        return {
            "state": "ready",
            "detail": "按内容变化更新" if self.drive_permitted else "等待归档授权",
        }
