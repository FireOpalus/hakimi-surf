"""Resident system module for loopback-isolated Google Colab GPU compute scheduling."""

from __future__ import annotations

from ..colab_compute import ColabComputeHub
from . import SystemModule


class Module(SystemModule):
    module_id = "colab_compute"
    display_name = "Colab 云端算力"
    description = (
        "通过进程外 Worker 调度 Google Colab 通用 GPU 云端算力，"
        "统一管理 Compute Units 预算、热机保活与已注册作业队列。"
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
