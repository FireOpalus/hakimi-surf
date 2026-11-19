from functools import partial

from ..memory_mirror import MemoryMirror
from . import SystemModule


class Module(SystemModule):
    module_id = "group_memory_mirror"
    display_name = "群记忆镜像"
    description = "按群授权同步记忆条目，核对远端版本并审核改动。"
    requires = ("drive_sync",)
    drive_accepts_files = False
    drive_usage = "已授权群的记忆条目"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.provider = registry.systems["drive_sync"]
        self.mirror = MemoryMirror(
            resources.service,
            drive=self.provider.drive,
            transfer=partial(self.provider.transfer, self),
        )

    def workers(self):
        return (self.mirror.run,) if self.provider.drive is not None else ()

    async def status(self):
        return {
            "state": "ready",
            "detail": "按群授权" if self.provider.drive is not None else "等待配置 Drive",
        }
