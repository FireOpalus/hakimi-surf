"""Platform-agnostic proactive social gates and private desktop activity timestamps."""

from __future__ import annotations

import asyncio
import json
import os
import stat
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .bot_server import is_persona_owner
from .models import Message
from .modules.actions import GateDecision
from .social import bound_owner, owner_binding, owner_dm_scope
from .social_speak import resolve_social_target
from .world_experience import SOCIAL_LOW

DESKTOP_CONTEXT_HINT = "BOISER 近期刚在电脑端忙正事，默契关心一句注意休息喝水，绝不问怎么没回消息"

DEFAULT_GROUP_COOLDOWN_SECONDS = 1500
DEFAULT_GROUP_ACTIVE_WINDOW_SECONDS = 1800
DEFAULT_GROUP_MIN_RECENT_MESSAGES = 2
DEFAULT_DESKTOP_ACTIVE_SUPPRESS_SECONDS = 7200


def _zone(tz_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_memory_timestamp(raw_ts: Any, tz_name: str) -> float:
    text = str(raw_ts or "").strip()
    if not text:
        return 0.0
    zone = _zone(tz_name)
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
    """Strictly read-only inspection of Antigravity_Memory/short_term_memory.json."""
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


class SocialProactivePolicy:
    """Platform-agnostic preflight policy for proactive social speaking."""

    def __init__(
        self,
        service: Any,
        platform: str,
        bot_id_getter: Callable[[], str] | str = "",
    ) -> None:
        self.service = service
        self.s = service.s
        self.store = service.store
        self.platform = str(platform or "").strip()
        self._bot_id_getter = bot_id_getter

    def _bot_id(self) -> str:
        raw = self._bot_id_getter() if callable(self._bot_id_getter) else self._bot_id_getter
        return str(raw or "").strip()

    def _option_int(self, cfg: Any, field: str, default: int) -> int:
        val = getattr(cfg, field, None) if cfg is not None else None
        if val is None:
            engine = getattr(self.service, "life_engine", None)
            engine_cfg = getattr(engine, "config", None) if engine is not None else None
            if engine_cfg is not None:
                val = getattr(engine_cfg, field, None)
        if val is None or isinstance(val, bool):
            return default
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    async def evaluate(self, ticket: dict[str, Any], now: float, cfg: Any = None) -> GateDecision:
        intent_id = ticket["id"]
        ws = self.service.world_state
        target_msg = resolve_social_target(
            self.service,
            self.platform,
            self._bot_id(),
            str(ticket.get("target_scope", "") or ""),
            event_id=f"gate:{intent_id}",
            created_at=now,
        )
        if target_msg is None:
            return GateDecision("unauthorized_scope")

        if await ws.experience.battery(target_msg, now) < SOCIAL_LOW:
            return GateDecision("social_battery_low", delay=900)

        # Session concurrency lock avoidance
        scope = self.store.session_scope(target_msg)
        group_busy = False
        if target_msg.kind == "group":
            groups = getattr(self.store, "groups", None)
            if groups is not None:
                active = getattr(groups, "active", ())
                group_busy = scope in active or (await groups.state(target_msg)) != "idle"
        ingress_entries = getattr(getattr(self.service, "ingress_scheduler", None), "entries", ())
        dialogue_entries = getattr(getattr(self.service, "dialogue_scheduler", None), "entries", ())
        owner_lock = getattr(self.service, "owner_dialogue_lock", None)
        owner_locked = bool(owner_lock is not None and owner_lock.locked())
        busy = (
            scope in ingress_entries
            or scope in dialogue_entries
            or (target_msg.kind == "private" and owner_locked)
            or group_busy
        )
        if busy:
            return GateDecision("session_busy", delay=150)

        # Group gates vs. Owner private DM gates
        desktop_hint: str | None = None
        if target_msg.kind == "group":
            cooldown_seconds = self._option_int(
                cfg, "group_cooldown_seconds", DEFAULT_GROUP_COOLDOWN_SECONDS
            )
            cooldown_ok = await ws.group_cooldown_ok(scope, cooldown_seconds, now)
            filters = getattr(getattr(self.store, "runtime", None), "filters", None)
            if filters is not None:
                last_pro_map = getattr(filters, "last_proactive_spoke", None)
                if isinstance(last_pro_map, dict) and hasattr(filters, "_group_scope"):
                    last_pro = float(last_pro_map.get(filters._group_scope(target_msg), 0.0) or 0.0)
                    if 0.0 < last_pro <= now + 5.0 and (now - last_pro) < cooldown_seconds:
                        cooldown_ok = False
                speaking = (
                    await filters.speaking_state(target_msg)
                    if hasattr(filters, "speaking_state")
                    else "ready"
                )
            else:
                speaking = "ready"
            if not cooldown_ok or speaking != "ready":
                return GateDecision("group_cooldown")
            if not await self._group_activity_ok(target_msg, now, cfg):
                return GateDecision("group_inactive")
        else:
            ok, desktop_hint = await self._owner_dm_gate(target_msg, ticket, now, cfg)
            if not ok:
                return GateDecision("owner_recent_activity")

        return GateDecision(desktop_hint=desktop_hint)

    async def _group_activity_ok(self, group_msg: Message, now: float, cfg: Any) -> bool:
        hot = getattr(self.service, "hot", None)
        if hot is None:
            return False
        window_seconds = self._option_int(
            cfg, "group_active_window_seconds", DEFAULT_GROUP_ACTIVE_WINDOW_SECONDS
        )
        min_recent = self._option_int(
            cfg, "group_min_recent_messages", DEFAULT_GROUP_MIN_RECENT_MESSAGES
        )
        excluded_bots: set[str] = (
            set(getattr(self.s, "bot_qq_ids", ()))
            if group_msg.platform in {"onebot", "qq"}
            else set()
        )
        if group_msg.bot_id:
            excluded_bots.add(group_msg.bot_id)

        if hasattr(hot, "recent_group_activity"):
            act = hot.recent_group_activity(
                group_msg,
                window_seconds=window_seconds,
                now=now,
                bot_ids=excluded_bots,
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
                    or item.user_id in excluded_bots
                ):
                    continue
                if item.created_at > 0 and 0.0 <= (now - item.created_at) <= window_seconds:
                    recent_items.append(item)
            recent_items.reverse()

        identity = getattr(self.service, "identity", None) or getattr(
            getattr(self.store, "runtime", None), "identity", None
        )
        bot_guard = getattr(self.service, "bot_guard", None)
        aff = getattr(getattr(self.store, "runtime", None), "affection", None)
        aff_enabled = bool(
            aff is not None and getattr(getattr(aff, "config", None), "enabled", False)
        )
        eligible_count = 0
        has_high_bond = False
        onebot_muted = set(getattr(self.s, "muted_user_ids", set()))

        for item in recent_items:
            if (
                identity is not None
                and hasattr(identity, "is_bot_account")
                and identity.is_bot_account(item.platform, item.user_id, bot_id=item.bot_id)
            ):
                continue
            if (
                bot_guard is not None
                and hasattr(bot_guard, "known_bot")
                and bot_guard.known_bot(item)
            ):
                continue
            is_owner = is_persona_owner(item, self.s) or (
                aff is not None
                and hasattr(aff, "is_owner_id")
                and aff.is_owner_id(item.platform, item.user_id)
            )
            if not is_owner:
                if (
                    identity is not None
                    and hasattr(identity, "is_muted")
                    and identity.is_muted(item.platform, item.user_id)
                ):
                    continue
                if item.platform == "onebot" and item.user_id in onebot_muted:
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

        effective_min = 1 if has_high_bond else min_recent
        return eligible_count >= effective_min

    async def _owner_dm_gate(
        self,
        target_msg: Message,
        ticket: dict[str, Any],
        now: float,
        cfg: Any,
    ) -> tuple[bool, str | None]:
        proactive = getattr(self.service, "proactive", None)
        if target_msg.platform == getattr(proactive, "platform", None):
            if proactive is None or not proactive.configured or not proactive.delivery_enabled:
                return False, None
        else:
            if owner_binding(self.s, target_msg.platform) is None:
                return False, None
            engine = getattr(self.service, "life_engine", None)
            if engine is None:
                if not getattr(self.s, "proactive_dm_enabled", True):
                    return False, None
            elif not getattr(
                engine, "overridden", False
            ) and engine.config.module_settings == getattr(
                getattr(engine, "env_config", None), "module_settings", None
            ):
                if not getattr(self.s, "proactive_dm_enabled", True):
                    return False, None
            else:
                modules = getattr(self.service, "modules", None)
                social_mod = modules.get("social_presence") if modules is not None else None
                if social_mod is not None:
                    if not social_mod.scheduling(engine.config).owner_delivery_enabled:
                        return False, None
                else:
                    from .modules.scheduling import SocialScheduleOptions

                    defaults = SocialScheduleOptions.from_settings(self.s).model_dump()
                    raw = engine.config.module_settings.get("social_presence", {})
                    if not SocialScheduleOptions.model_validate(
                        {**defaults, **raw}
                    ).owner_delivery_enabled:
                        return False, None
        if getattr(cfg, "owner_delivery_enabled", None) is False:
            return False, None
        if await self.owner_recently_active(ticket, now, platform=target_msg.platform):
            return False, None
        # Desktop Antigravity_Memory synergy gate is strictly gated by notebook grant.
        if not bound_owner(target_msg, self.s, "notebook"):
            return True, None
        action_type = str(ticket.get("action_type", "") or "")
        desktop = await _desktop_latest_activity(self.service)
        desktop_ts = float(desktop.get("latest_timestamp", 0.0) or 0.0)
        suppress_seconds = self._option_int(
            cfg,
            "desktop_active_suppress_seconds",
            DEFAULT_DESKTOP_ACTIVE_SUPPRESS_SECONDS,
        )
        desktop_recent = bool(
            desktop.get("enabled")
            and desktop_ts > 0.0
            and 0.0 <= (now - desktop_ts) < suppress_seconds
        )
        if desktop_recent:
            gate_cond = ticket.get("gate_condition") or {}
            allow_desktop_care = bool(gate_cond.get("allow_desktop_care"))
            if action_type == "owner_silence_care" and not allow_desktop_care:
                return False, None
            if allow_desktop_care or action_type == "owner_desktop_warm_care":
                return True, DESKTOP_CONTEXT_HINT
        return True, None

    async def owner_recently_active(
        self,
        ticket: dict[str, Any],
        now: float,
        *,
        platform: str | None = None,
    ) -> bool:
        if ticket.get("origin") != "planner" and str(ticket.get("action_type", "")).endswith(
            "_milestone_share"
        ):
            return False
        plat = platform or self.platform
        proactive = getattr(self.service, "proactive", None)
        scope = owner_dm_scope(self.s, plat) or (
            getattr(proactive, "scope", None)
            if plat == getattr(proactive, "platform", None)
            else None
        )
        if not scope:
            return False
        async with self.store.lock:
            row = await self.store.one(
                "SELECT last_private_at FROM owner_dm_state WHERE scope=?",
                (scope,),
            )
        return bool(row and row[0] and 0 <= now - row[0] < 3 * 3600)
