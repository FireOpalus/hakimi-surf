from ..event_ledger import EventLedger
from . import SystemModule


class Module(SystemModule):
    module_id = "event_ledger"
    admin_panel = "event-ledger"
    display_name = "跨日待办账本"
    description = "为各能力模块与日程规划器提供跨时段/跨日的待办事件送入、读取与核销接口。"

    def __init__(self, resources, registry):
        super().__init__(resources, registry)
        self.ledger = EventLedger(resources.settings, resources.store)

    async def initialize(self):
        await self.ledger.initialize()

    def workers(self):
        return (self.ledger.run_cleanup,)

    async def status(self):
        pending, due = await self.ledger.status_counts()
        detail = f"待办 {pending} 条 · 已到点 {due} 条" if pending else "待办 0 条"
        return {"state": "ready", "detail": detail}
