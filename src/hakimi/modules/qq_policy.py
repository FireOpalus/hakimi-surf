"""OneBot conversation gates and private desktop activity timestamps."""

from __future__ import annotations

import asyncio
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from ..bot_server import is_persona_owner
from ..models import Message
from .actions import GateDecision
from .qq_audience import _scope_to_message

if TYPE_CHECKING:
    from ..life_engine import LifeEngineConfig

DESKTOP_CONTEXT_HINT = "BOISER 近期刚在电脑端忙正事，默契关心一句注意休息喝水，绝不问怎么没回消息"


def _parse_memory_timestamp(raw_ts: Any, tz_name: str) -> float:
    text = str(raw_ts or "").strip()
    if not text:
        return 0.0
    zone = ZoneInfo(tz_name)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=zone).timestamp()
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone)
    return dt.timestamp()


def _read_desktop_latest_ts_sync(memory_dir: Path, tz_name: str) -> float:
    path = memory_dir / "short_term_memory.json"
    if path.is_symlink():
        return 0.0
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return 0.0
    try:
        with os.fdopen(fd, "rb") as file:
            st = os.fstat(file.fileno())
            if not stat.S_ISREG(st.st_mode):
                return 0.0
            raw = file.read(262_144).decode("utf-8", errors="ignore")
            mtime = float(st.st_mtime)
    except OSError:
        return 0.0
    latest = 0.0
    try:
        items = json.loads(raw)
    except ValueError:
        items = []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            parsed = _parse_memory_timestamp(item.get("timestamp"), tz_name)
            if parsed > latest:
                latest = parsed
    return latest if latest > 0.0 else mtime


async def _desktop_latest_activity(service: Any) -> dict[str, Any]:
    cloud_mem = getattr(service, "cloud_memory", None)
    if cloud_mem is not None and hasattr(cloud_mem, "latest_activity_info"):
        info = await asyncio.to_thread(cloud_mem.latest_activity_info)
        return {
            "enabled": bool(info.get("enabled", False)),
            "latest_timestamp": float(info.get("latest_timestamp", 0.0) or 0.0),
            "latest_iso": str(info.get("latest_iso", "") or ""),
            "latest_tag": str(info.get("latest_tag", "") or ""),
        }
    if not getattr(service.s, "enable_antigravity_memory", False):
        return {"enabled": False, "latest_timestamp": 0.0, "latest_iso": "", "latest_tag": ""}
    ts = await asyncio.to_thread(
        _read_desktop_latest_ts_sync,
        Path(service.s.antigravity_memory_dir),
        service.s.bot_timezone,
    )
    return {"enabled": True, "latest_timestamp": ts, "latest_iso": "", "latest_tag": ""}


class QQPolicy:
    def __init__(self, service):
        self.service = service
        self.s = service.s
        self.store = service.store

    async def evaluate(self, ticket, now, cfg) -> GateDecision:
        intent_id = ticket["id"]
        ws = self.service.world_state
        target_msg = _scope_to_message(
            ticket.get("target_scope", ""),
            self.s,
            event_id=f"gate:{intent_id}",
            created_at=now,
        )
        if target_msg is None:
            return GateDecision("unauthorized_scope")

        # Gate 5: Session concurrency lock avoidance
        scope = self.store.session_scope(target_msg)
        group_busy = False
        if target_msg.kind == "group":
            group_busy = (
                scope in self.store.groups.active
                or (await self.store.groups.state(target_msg)) != "idle"
            )
        busy = (
            scope in self.service.ingress_scheduler.entries
            or scope in self.service.dialogue_scheduler.entries
            or (target_msg.kind == "private" and self.service.owner_dialogue_lock.locked())
            or group_busy
        )
        if busy:
            return GateDecision("session_busy", delay=150)

        # Gate 6: Group gates
        desktop_hint: str | None = None
        if target_msg.kind == "group":
            cooldown_ok = await ws.group_cooldown_ok(scope, cfg.group_cooldown_seconds, now)
            filters = self.store.runtime.filters
            last_pro_map = getattr(filters, "last_proactive_spoke", None)
            if isinstance(last_pro_map, dict):
                last_pro = float(last_pro_map.get(filters._group_scope(target_msg), 0.0) or 0.0)
                if 0.0 < last_pro <= now + 5.0 and (now - last_pro) < cfg.group_cooldown_seconds:
                    cooldown_ok = False
            speaking = await filters.speaking_state(target_msg)
            if not cooldown_ok or speaking != "ready":
                return GateDecision("group_cooldown")
            if not await self._group_activity_ok(target_msg, now, cfg):
                return GateDecision("group_inactive")
        else:
            # Gate 7: Owner DM + Desktop Antigravity_Memory synergy gate
            ok, desktop_hint = await self._owner_dm_gate(ticket, now, cfg)
            if not ok:
                return GateDecision("owner_recent_activity")

        return GateDecision(desktop_hint=desktop_hint)

    async def _group_activity_ok(
        self, group_msg: Message, now: float, cfg: LifeEngineConfig
    ) -> bool:
        hot = getattr(self.service, "hot", None)
        if hot is None:
            return False
        if hasattr(hot, "recent_group_activity"):
            act = hot.recent_group_activity(
                group_msg,
                window_seconds=cfg.group_active_window_seconds,
                now=now,
                bot_ids=self.s.bot_qq_ids,
            )
            recent_items = list(act.get("messages", []))
        else:
            ring = hot.rings.get(hot.key(group_msg))
            if not ring:
                return False
            recent_items = []
            for _, item, _ in reversed(list(ring.messages.values())):
                if (
                    hot.recalled(group_msg, item.event_id)
                    or item.user_id == item.bot_id
                    or item.user_id in self.s.bot_qq_ids
                ):
                    continue
                if (
                    item.created_at > 0
                    and 0.0 <= (now - item.created_at) <= cfg.group_active_window_seconds
                ):
                    recent_items.append(item)
            recent_items.reverse()

        aff = getattr(self.store.runtime, "affection", None)
        aff_enabled = bool(aff is not None and aff.config.enabled)
        eligible_count = 0
        has_high_bond = False
        muted = set(getattr(self.s, "muted_user_ids", set()))

        for item in recent_items:
            is_owner = is_persona_owner(item, self.s) or (
                aff is not None and aff.is_owner_id(item.platform, item.user_id)
            )
            if not is_owner and item.user_id in muted:
                continue
            if aff_enabled and not is_owner:
                caps = await aff.capabilities_for(item)
                if caps.level <= 1:
                    continue
                if caps.level >= 4:
                    has_high_bond = True
            elif is_owner:
                has_high_bond = True
            eligible_count += 1

        effective_min = 1 if has_high_bond else cfg.group_min_recent_messages
        return eligible_count >= effective_min

    async def _owner_dm_gate(
        self, ticket: dict[str, Any], now: float, cfg: LifeEngineConfig
    ) -> tuple[bool, str | None]:
        proactive = self.service.proactive
        if not proactive.configured or not cfg.owner_delivery_enabled:
            return False, None
        if await self.owner_recently_active(ticket, now):
            return False, None
        action_type = str(ticket.get("action_type", ""))
        desktop = await _desktop_latest_activity(self.service)
        desktop_ts = float(desktop.get("latest_timestamp", 0.0) or 0.0)
        desktop_recent = bool(
            desktop.get("enabled")
            and desktop_ts > 0.0
            and 0.0 <= (now - desktop_ts) < cfg.desktop_active_suppress_seconds
        )
        if desktop_recent:
            gate_cond = ticket.get("gate_condition") or {}
            allow_desktop_care = bool(gate_cond.get("allow_desktop_care"))
            if action_type == "owner_silence_care" and not allow_desktop_care:
                return False, None
            if allow_desktop_care or action_type == "owner_desktop_warm_care":
                return True, DESKTOP_CONTEXT_HINT
        return True, None

    async def owner_recently_active(self, ticket: dict, now: float) -> bool:
        if ticket.get("origin") != "planner" and ticket["action_type"].endswith("_milestone_share"):
            return False
        async with self.store.lock:
            row = await self.store.one(
                "SELECT last_private_at FROM owner_dm_state WHERE scope=?",
                (self.service.proactive.scope,),
            )
        return bool(row and row[0] and 0 <= now - row[0] < 3 * 3600)
