"""Resident system module for Owner proactive delivery, alerts, and ledger notices."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal

from ..proactive_dm import OwnerProactive
from ..security import Rejected
from . import SystemModule

if TYPE_CHECKING:
    from ..modules import ModuleRegistry
    from . import SystemResources

log = logging.getLogger("hakimi")

_ERROR_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_VALID_NOTICE_KINDS = frozenset({"alert", "patrol", "screen_watch", "test"})


def _safe_error_code(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip()
    return cleaned if _ERROR_CODE_RE.fullmatch(cleaned) else None


class Module(SystemModule):
    module_id = "proactive_delivery"
    display_name = "主动私信与投递"
    description = (
        "管理 Owner 主动私信持久化出站队列、运维告警巡检、每日汇报投递及跨日待办账本通知。"
    )
    requires = ("event_ledger",)
    admin_panel = "proactive"

    def __init__(self, resources: SystemResources, registry: ModuleRegistry) -> None:
        super().__init__(resources, registry)
        self._registry = registry
        ledger_module = registry.systems.get("event_ledger")
        self.ledger = getattr(ledger_module, "ledger", None)
        existing = getattr(resources.service, "proactive", None)
        self.proactive = (
            existing
            if isinstance(existing, OwnerProactive)
            else OwnerProactive(
                resources.service,
                settings=resources.settings,
                store=resources.store,
            )
        )
        if getattr(registry, "owner_delivery", None) is None:
            registry.owner_delivery = self.proactive

    def _target(self) -> Any:
        current = getattr(self.resources.service, "proactive", None)
        return current if current is not None else self.proactive

    @staticmethod
    def _safe_delivery_enabled(target: Any) -> bool:
        try:
            return bool(target.delivery_enabled)
        except AttributeError:
            return False

    @staticmethod
    def _safe_connected(target: Any) -> bool:
        try:
            return bool(target.connected())
        except AttributeError:
            return False

    async def initialize(self) -> None:
        await self._target().initialize()

    async def run_alerts(self) -> None:
        target = self._target()
        await target.run()

    async def run_delivery(self) -> None:
        target = self._target()
        await target.delivery()

    def workers(self) -> tuple[Callable[[], Coroutine[Any, Any, None]], ...]:
        return (self.run_alerts, self.run_delivery)

    async def pending_ledger_notices(
        self,
        *,
        only_due: bool = True,
        limit: int = 20,
        allow_owner_private: bool = True,
    ) -> list[Any]:
        """Read pending event_ledger commitments addressed to proactive_delivery."""
        if self.ledger is None:
            return []
        return await self.ledger.read(
            target_module=self.module_id,
            include_owner_private=allow_owner_private,
            only_due=only_due,
            limit=limit,
        )

    async def enqueue_ledger_alert(
        self,
        *,
        source_module: str,
        code: str,
        text: str,
        event_type: str = "owner_alert",
        summary: str | None = None,
        payload: dict[str, Any] | None = None,
        daily: bool = False,
        dedupe_key: str | None = None,
        visibility: Literal["public", "owner_private"] = "owner_private",
        record_ledger: bool = True,
        consume_on_enqueue: bool = True,
        ttl_seconds: float = 86400.0,
    ) -> dict[str, Any]:
        """Deliver an Owner alert and optionally record/settle its event_ledger entry."""
        message_text = str(text or "").strip()
        if not message_text:
            return {
                "status": "skipped",
                "reason": "empty_text",
                "dm_id": None,
                "ledger_event_id": None,
                "consumed": False,
            }

        ledger_event = None
        if record_ledger and self.ledger is not None:
            ledger_payload = dict(payload or {})
            ledger_payload.setdefault("alert_code", code)
            ledger_payload.setdefault("daily", bool(daily))
            ledger_event = await self.ledger.post(
                source_module,
                event_type,
                summary or message_text,
                payload=ledger_payload,
                target_module=self.module_id,
                dedupe_key=dedupe_key or f"{event_type}:{code}",
                ttl_seconds=ttl_seconds,
                visibility=visibility,
            )

        target = self._target()
        dm_id = await target.alert(code, message_text, daily=daily)
        if dm_id is None:
            reason = (
                "disabled"
                if (not target.configured or not self._safe_delivery_enabled(target))
                else "cooldown"
            )
            return {
                "status": reason,
                "reason": reason,
                "dm_id": None,
                "ledger_event_id": ledger_event.id if ledger_event is not None else None,
                "consumed": False,
            }

        consumed = False
        if consume_on_enqueue and ledger_event is not None and self.ledger is not None:
            consumed = await self.ledger.consume(
                ledger_event.id,
                consumer=self.module_id,
                allow_owner_private=True,
            )
        return {
            "status": "queued",
            "reason": None,
            "dm_id": dm_id,
            "ledger_event_id": ledger_event.id if ledger_event is not None else None,
            "consumed": consumed,
        }

    async def _find_pending_ledger_event(
        self, lookup: str, *, allow_owner_private: bool
    ) -> Any | None:
        if self.ledger is None:
            return None
        targeted = await self.ledger.read(
            target_module=self.module_id,
            include_owner_private=allow_owner_private,
            limit=50,
        )
        for item in targeted:
            if item.id == lookup or (item.dedupe_key and item.dedupe_key == lookup):
                return item
        broad = await self.ledger.read(
            include_owner_private=allow_owner_private,
            limit=50,
        )
        for item in broad:
            if item.id == lookup or (item.dedupe_key and item.dedupe_key == lookup):
                return item
        return None

    async def _deliver_ledger_event(
        self,
        event: Any,
        *,
        text: str | None = None,
        kind: Literal["alert", "patrol", "screen_watch", "test"] = "alert",
        code: str | None = None,
        daily: bool = False,
        expires: float | None = None,
        consume: bool = True,
        allow_owner_private: bool = True,
    ) -> dict[str, Any]:
        message_text = (text if text is not None else event.summary).strip()
        if not message_text:
            return {
                "status": "skipped",
                "reason": "empty_text",
                "dm_id": None,
                "ledger_event_id": event.id,
                "consumed": False,
            }

        target = self._target()
        if kind == "alert":
            payload_code = (
                str(event.payload.get("alert_code") or "").strip()
                if isinstance(event.payload, dict)
                else ""
            )
            alert_code = (
                code
                or payload_code
                or event.dedupe_key
                or f"ledger:{event.source_module}:{event.event_type}:{event.id}"
            )
            dm_id = await target.alert(alert_code, message_text, daily=daily)
            if dm_id is None:
                reason = (
                    "disabled"
                    if (not target.configured or not self._safe_delivery_enabled(target))
                    else "cooldown"
                )
                return {
                    "status": reason,
                    "reason": reason,
                    "dm_id": None,
                    "ledger_event_id": event.id,
                    "consumed": False,
                }
        else:
            dm_key = code or f"ledger:{event.id}"
            try:
                dm_id = await target.enqueue(kind, message_text, key=dm_key, expires=expires)
            except Rejected as exc:
                return {
                    "status": "rejected",
                    "reason": str(exc),
                    "dm_id": None,
                    "ledger_event_id": event.id,
                    "consumed": False,
                }

        consumed = False
        if consume and self.ledger is not None:
            consumed = await self.ledger.consume(
                event.id,
                consumer=self.module_id,
                allow_owner_private=allow_owner_private,
            )
        return {
            "status": "queued",
            "reason": None,
            "dm_id": dm_id,
            "ledger_event_id": event.id,
            "consumed": consumed,
        }

    async def deliver_ledger_notice(
        self,
        event_id_or_key: str,
        *,
        text: str | None = None,
        kind: Literal["alert", "patrol", "screen_watch", "test"] = "alert",
        code: str | None = None,
        daily: bool = False,
        expires: float | None = None,
        consume: bool = True,
        allow_owner_private: bool = True,
    ) -> dict[str, Any]:
        """Enqueue a pending event_ledger entry into OwnerProactive and settle it on success."""
        if self.ledger is None:
            return {
                "status": "unavailable",
                "reason": "event_ledger_unavailable",
                "dm_id": None,
                "ledger_event_id": None,
                "consumed": False,
            }
        lookup = str(event_id_or_key or "").strip()
        if not lookup:
            return {
                "status": "not_found",
                "reason": "ledger_event_not_found",
                "dm_id": None,
                "ledger_event_id": None,
                "consumed": False,
            }

        event = await self._find_pending_ledger_event(
            lookup, allow_owner_private=allow_owner_private
        )
        if event is None:
            return {
                "status": "not_found",
                "reason": "ledger_event_not_found",
                "dm_id": None,
                "ledger_event_id": None,
                "consumed": False,
            }
        return await self._deliver_ledger_event(
            event,
            text=text,
            kind=kind,
            code=code,
            daily=daily,
            expires=expires,
            consume=consume,
            allow_owner_private=allow_owner_private,
        )

    async def drain_due_ledger_notices(
        self, *, limit: int = 10, allow_owner_private: bool = True
    ) -> list[dict[str, Any]]:
        """Deliver due event_ledger items targeted at proactive_delivery."""
        items = await self.pending_ledger_notices(
            only_due=True, limit=limit, allow_owner_private=allow_owner_private
        )
        results: list[dict[str, Any]] = []
        for item in items:
            payload = item.payload if isinstance(item.payload, dict) else {}
            raw_kind = str(payload.get("delivery_kind") or "alert").strip()
            kind = raw_kind if raw_kind in _VALID_NOTICE_KINDS else "alert"
            raw_code = payload.get("alert_code")
            alert_code = (
                raw_code.strip() if isinstance(raw_code, str) and raw_code.strip() else None
            )
            results.append(
                await self._deliver_ledger_event(
                    item,
                    kind=kind,  # type: ignore[arg-type]
                    code=alert_code,
                    daily=bool(payload.get("daily", False)),
                    consume=True,
                    allow_owner_private=allow_owner_private,
                )
            )
        return results

    async def status(self) -> dict[str, Any]:
        target = self._target()
        now = target.clock()
        day = target.local(now).date().isoformat()
        configured = bool(target.configured)
        delivery_enabled = self._safe_delivery_enabled(target)
        connected = self._safe_connected(target)

        pending_count = 0
        sending_count = 0
        delivered_count = 0
        uncertain_count = 0
        failed_count = 0
        sent_today_count = 0
        last_success: float | None = None
        last_error = _safe_error_code(
            getattr(target, "last_delivery_error", None)
            or getattr(target, "last_worker_error", None)
        )
        ledger_pending = 0

        store = getattr(target, "store", None)
        if store is not None and getattr(store, "db", None) is not None:
            async with store.lock:
                tables = {
                    row[0]
                    for row in await store.all(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name IN ('pending_owner_dms', 'proactive_schedule', 'event_ledger')"
                    )
                }
                if "pending_owner_dms" in tables:
                    rows = await store.all(
                        "SELECT status, COUNT(*) AS n FROM pending_owner_dms "
                        "WHERE scope=? "
                        "AND status IN ('pending', 'sending', 'delivered', 'uncertain', 'failed') "
                        "GROUP BY status",
                        (target.scope,),
                    )
                    counts = {row["status"]: int(row["n"]) for row in rows}
                    pending_count = counts.get("pending", 0)
                    sending_count = counts.get("sending", 0)
                    delivered_count = counts.get("delivered", 0)
                    uncertain_count = counts.get("uncertain", 0)
                    failed_count = counts.get("failed", 0)

                    sent_row = await store.one(
                        "SELECT COUNT(*) FROM pending_owner_dms "
                        "WHERE scope=? AND day=? AND status='sent'",
                        (target.scope, day),
                    )
                    if sent_row:
                        sent_today_count = int(sent_row[0] or 0)

                    last_sent_row = await store.one(
                        "SELECT MAX(sent_at) FROM pending_owner_dms "
                        "WHERE scope=? AND status='sent' AND sent_at IS NOT NULL",
                        (target.scope,),
                    )
                    if last_sent_row and last_sent_row[0] is not None:
                        last_success = float(last_sent_row[0])

                    if not last_error:
                        err_row = await store.one(
                            "SELECT reason FROM pending_owner_dms "
                            "WHERE scope=? AND status IN ('uncertain', 'failed') "
                            "AND reason IS NOT NULL ORDER BY created DESC LIMIT 1",
                            (target.scope,),
                        )
                        if err_row:
                            last_error = _safe_error_code(err_row[0])

                if "proactive_schedule" in tables:
                    sched_fail = await store.one(
                        "SELECT COUNT(*) FROM proactive_schedule "
                        "WHERE scope=? AND day=? AND status='failed'",
                        (target.scope, day),
                    )
                    if sched_fail:
                        failed_count += int(sched_fail[0] or 0)
                    if not last_error:
                        sched_err = await store.one(
                            "SELECT reason FROM proactive_schedule "
                            "WHERE scope=? AND day=? AND status='failed' AND reason IS NOT NULL "
                            "ORDER BY due_at DESC LIMIT 1",
                            (target.scope, day),
                        )
                        if sched_err:
                            last_error = _safe_error_code(sched_err[0])

                if "event_ledger" in tables:
                    ledger_row = await store.one(
                        "SELECT COUNT(*) FROM event_ledger "
                        "WHERE target_module=? AND status='pending' AND expires_at>?",
                        (self.module_id, now),
                    )
                    if ledger_row:
                        ledger_pending = int(ledger_row[0] or 0)

        inflight_total = pending_count + sending_count + delivered_count
        if not configured:
            state = "unconfigured"
            detail = "未配置 Owner 投递目标"
        else:
            state = "ready" if delivery_enabled else "disabled"
            prefix = "已停用投递" if not delivery_enabled else ("在线" if connected else "等待连接")
            parts = [prefix, f"待投递 {inflight_total}", f"今日已投递 {sent_today_count}"]
            if uncertain_count:
                parts.append(f"待确认 {uncertain_count}")
            if failed_count:
                parts.append(f"失败 {failed_count}")
            detail = " · ".join(parts)

        result: dict[str, Any] = {
            "state": state,
            "detail": detail,
            "platform": target.platform,
            "configured": configured,
            "delivery_enabled": delivery_enabled,
            "connected": connected,
            "day": day,
            "outbox": {
                "pending": pending_count,
                "sending": sending_count,
                "delivered": delivered_count,
                "uncertain": uncertain_count,
                "failed": failed_count,
                "sent_today": sent_today_count,
            },
            "ledger_pending": ledger_pending,
        }
        if last_error:
            result["last_error"] = last_error
        if last_success is not None:
            result["last_success"] = last_success
        return result
