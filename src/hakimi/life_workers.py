"""Run selected SQLite intents independently across modules and QQ audiences."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter

log = logging.getLogger("hakimi")


class ActionWorkers:
    def __init__(self, engine, evaluate):
        self.engine = engine
        self.evaluate = evaluate
        self.active: dict[tuple[str, str], tuple[str, asyncio.Task[str]]] = {}
        # Leave a slot for a closing timer when all ordinary workers are busy.
        self.capacity = 8
        self.stopping = False

    def lane(self, ticket):
        if scope := ticket["target_scope"]:
            return "recipient", scope
        catalog = self.engine.catalog
        name = ticket["action_name"] or catalog.old_name(ticket)
        return "module", catalog.owners.get(name, ticket["target_module"])

    async def _run(self, ticket, now):
        try:
            return await self.evaluate(ticket, max(now, self.engine.clock()), self.engine.config)
        except Exception as exc:
            log.warning("event=life_worker_failed id=%s type=%s", ticket["id"], type(exc).__name__)
            return "error"

    def submit(self, ticket, now):
        """Start, or join the same ticket. Busy work stays in SQLite until it can run."""
        lane = self.lane(ticket)
        if busy := self.active.get(lane):
            return busy[1] if busy[0] == ticket["id"] else None
        spec = self.engine.catalog.actions.get(ticket["action_name"])
        closing = ticket["origin"] == "timer" and spec is not None and spec.window == "closing"
        if self.stopping or len(self.active) >= self.capacity + int(closing):
            return None
        task = asyncio.create_task(self._run(ticket, now), name=f"life-action-{ticket['id']}")
        self.active[lane] = (ticket["id"], task)
        task.add_done_callback(lambda _: self.active.pop(lane, None))
        return task

    async def _wait_for_ticket(self, ticket, now):
        while not self.stopping:
            if task := self.submit(ticket, now):
                # A disconnected admin request must not cancel a running module action.
                return await asyncio.shield(task)
            busy = self.active.get(self.lane(ticket))
            tasks = [busy[1]] if busy else [task for _, task in self.active.values()]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        return "stopped"

    async def dispatch(self, tickets, now, *, wait):
        if not wait:
            return {"started": sum(self.submit(ticket, now) is not None for ticket in tickets)}
        # Preserve the drain-once behaviour of the admin endpoint and order within each lane.
        lanes = {}
        for ticket in tickets:
            lanes.setdefault(self.lane(ticket), []).append(ticket)

        async def drain(tickets):
            return [await self._wait_for_ticket(ticket, now) for ticket in tickets]

        batches = await asyncio.gather(*(drain(rows) for rows in lanes.values()))
        return dict(Counter(outcome for batch in batches for outcome in batch))

    async def close(self):
        self.stopping = True
        tasks = [task for _, task in self.active.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
