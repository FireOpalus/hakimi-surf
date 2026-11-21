"""Resident system module for loopback-isolated Colab GPU & Drive asset scheduling."""

from __future__ import annotations

from ..colab_compute import ColabComputeHub
from . import SystemModule


class Module(SystemModule):
    module_id = "colab_compute"
    display_name = "Colab 云端算力"
    description = (
        "通过进程外 Worker 调度 Google Colab GPU 翻唱与 RVC 训练作业，"
        "并托管 Hakimi_Songs/ 云盘音频目录。"
    )
    admin_panel = "colab-compute"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.compute = ColabComputeHub(
            resources.settings,
            resources.store,
            resources.service,
            http=resources.http,
        )

    async def initialize(self) -> None:
        await self.compute.initialize()

    def workers(self):
        return (self.compute.run_scheduler,)

    async def status(self) -> dict:
        return await self.compute.status_summary()
