"""Resident system module for GCP Cloud Billing & Cloud Monitoring reconciliation."""

from __future__ import annotations

import asyncio

from ..gcp_billing import GcpBillingManager
from . import SystemModule


class Module(SystemModule):
    module_id = "gcp_billing"
    display_name = "GCP 账单对账"
    description = "定期对账 GCP BigQuery 账单与 Cloud Monitoring 用量，支持赠金余量与硬停保护。"
    admin_panel = "ledger"

    def __init__(self, resources, registry) -> None:
        super().__init__(resources, registry)
        existing = getattr(resources.store.runtime, "gcp_billing", None)
        self.manager = (
            existing
            if isinstance(existing, GcpBillingManager)
            else GcpBillingManager(resources.store)
        )
        resources.store.runtime.gcp_billing = self.manager

    async def initialize(self) -> None:
        await self.manager.initialize()

    async def run_sync(self) -> None:
        while True:
            try:
                await self.manager.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(30)

    def workers(self):
        return (self.run_sync,)

    async def status(self) -> dict:
        summary = self.manager.summary_for_budget()
        bq_on = bool(summary.get("bigquery_enabled"))
        mon_on = bool(summary.get("monitoring_enabled"))
        if not bq_on and not mon_on:
            detail = "待机 · 未开启自动对账"
            state = "unconfigured" if not summary.get("configured") else "ready"
        else:
            parts = []
            if bq_on:
                if summary.get("synced"):
                    rem = summary.get("realtime_remaining_usd")
                    rem_text = f"剩余 ${rem:.2f}" if isinstance(rem, (int, float)) else "已对账"
                    parts.append(f"BigQuery {rem_text}")
                else:
                    parts.append("BigQuery 等待对账")
            if mon_on:
                parts.append(f"Monitoring {summary.get('monitoring_status') or 'not_synced'}")
            detail = " · ".join(parts)
            state = "ready"
        result = {"state": state, "detail": detail}
        if summary.get("last_error"):
            result["last_error"] = summary["last_error"]
        if summary.get("last_sync_at"):
            result["last_success"] = summary["last_sync_at"]
        return result
