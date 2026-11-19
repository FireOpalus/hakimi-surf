from ..notebook_snapshot import NotebookSnapshot
from ..owner_cloud_memory import OwnerCloudMemory
from . import SystemModule


class Module(SystemModule):
    module_id = "owner_notebook"
    admin_panel = "notebook"
    display_name = "Owner 只读记忆"
    description = "为 Owner 私聊提供获准读取的记忆和单文件快照。"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.cloud_memory = OwnerCloudMemory(resources.settings)
        self.snapshot = resources.notebook
        if self.snapshot is None and resources.http is not None:
            self.snapshot = NotebookSnapshot(
                resources.settings, resources.http, resources.gateway, resources.store
            )

    def workers(self):
        if self.snapshot is not None:
            return (self.snapshot.run,)
        return ()

    async def status(self):
        s = self.resources.settings
        if s.enable_notebook_snapshot and self.snapshot is not None:
            detail = "只读快照等待重试" if self.snapshot.last_error else "已授权只读快照"
        elif s.enable_antigravity_memory:
            detail = "只读记忆桥已配置"
        else:
            detail = "等待只读授权"
        return {"state": "ready", "detail": detail}
