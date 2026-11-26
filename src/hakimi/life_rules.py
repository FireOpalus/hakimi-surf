"""Local event subscriptions. Each rule owns an independent durable cursor and receipt."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from importlib import import_module
from typing import TYPE_CHECKING, Any

from .modules.actions import reject_reason
from .social import chat_module, chat_registry, owner_binding, owner_target_scope
from .world_state import IntentTicketCreate, MilestoneEvent, private_event_for_owner

if TYPE_CHECKING:
    from .life_engine import LifeEngine


@dataclass(frozen=True)
class Reaction:
    action: str
    note: str
    recipient: str = ""
    delay: int = 60
    ttl: int = 2700
    cooldown: int = 600
    arguments: dict = field(default_factory=dict)
    legacy_module: str = ""
    legacy_verb: str = ""


@dataclass(frozen=True)
class Rule:
    name: str
    match: Callable[[MilestoneEvent], Reaction | None]


_COMPAT_RULE_PROVIDERS: dict[str, Callable[..., Any]] = {}
_RULE_MODULE_STEMS = ("inspiration", "milestones")


def register_compat_rule_provider(name: str, provider: Callable[..., Any]) -> None:
    """Register a rule factory for legacy direct callers without hardcoding platform imports."""
    key = str(name or "").strip()
    if not key:
        raise ValueError("rule provider name must be non-empty")
    if not callable(provider):
        raise TypeError("rule provider must be callable")
    _COMPAT_RULE_PROVIDERS[key] = provider


def _resolve_compat_rule_provider(name: str) -> Callable[..., Any]:
    provider = _COMPAT_RULE_PROVIDERS.get(name)
    if provider is not None:
        return provider
    from .modules import PLATFORMS
    from .modules.manifest import CATALOG

    for platform in PLATFORMS:
        base_dir = CATALOG / platform.path
        for stem in _RULE_MODULE_STEMS:
            if not (base_dir / f"{stem}.py").is_file():
                continue
            mod = import_module(f"hakimi.modules.platforms.{platform.name}.{stem}")
            candidate = getattr(mod, name, None)
            if callable(candidate):
                _COMPAT_RULE_PROVIDERS[name] = candidate
                return candidate
    raise LookupError(f"rule_provider_not_found:{name}")


def _allowlisted_groups_for_settings(settings) -> list[str]:
    registry = chat_registry(settings)
    if registry is not None:
        groups: list[str] = []
        for module in registry.active():
            for scope in module.proactive_groups():
                if scope and scope not in groups:
                    groups.append(scope)
        return groups
    if not getattr(settings, "onebot_enabled", False):
        return []
    policies = getattr(settings, "group_policies", {}) or {}
    return [
        f"onebot:{gid}"
        for gid in sorted(getattr(settings, "onebot_allowed_groups", ()) or ())
        if policies.get(f"onebot:{gid}", "at_only") != "muted"
    ]


def _owner_candidates_for_settings(settings) -> list[tuple[str, str]]:
    registry = chat_registry(settings)
    order: list[str] = []
    preferred = getattr(settings, "owner_delivery_platform", "") or ""
    for plat in (preferred, "onebot", *(getattr(settings, "social_owner_bindings", {}) or {})):
        if not plat or plat == "qq" or plat in order:
            continue
        order.append(plat)
    candidates: list[tuple[str, str]] = []
    for plat in order:
        if registry is not None:
            if chat_module(settings, plat) is None or owner_binding(settings, plat) is None:
                continue
        scope = owner_target_scope(settings, plat, include_inactive=(registry is None))
        if scope:
            candidates.append((plat, scope))
    return candidates


def _speak_action_for_platform(settings, platform: str) -> str:
    registry = chat_registry(settings)
    if registry is not None:
        if "social.speak" in registry.catalog.actions:
            return "social.speak"
        mod = chat_module(settings, platform)
        if mod is not None:
            for name, spec in registry.catalog.actions.items():
                if registry.catalog.owners.get(name) == mod.module_id and spec.proactive_speak:
                    return name
    return "qq.speak" if platform == "onebot" else f"{platform}.speak"


# Compatibility wrapper for callers of the pre-module rule API.
def build_from_video(event):
    return _resolve_compat_rule_provider("build_from_video")(event)


def milestone_sharing(settings) -> Rule:
    def match(ev):
        if ev.importance != "high" or ev.event_type == "bili_mc_inspiration":
            return None
        owner_candidates = _owner_candidates_for_settings(settings)
        if ev.payload.get("private_first"):
            chosen = next(
                (
                    (plat, scope)
                    for plat, scope in owner_candidates
                    if private_event_for_owner(ev.payload, scope, settings)
                ),
                None,
            )
            if chosen is None:
                return None
            plat, recipient = chosen
        elif owner_candidates:
            plat, recipient = owner_candidates[0]
        else:
            if ev.event_type == "bili_video_watched":
                return None
            groups = _allowlisted_groups_for_settings(settings)
            if not groups:
                return None
            recipient = groups[0]
            plat = recipient.partition(":")[0]
        action_name = _speak_action_for_platform(settings, plat)
        return Reaction(
            action_name,
            "刚遇到一件挺有意思的新动态，看气氛顺嘴跟群友或 BOISER 唠两句",
            recipient,
            delay=90,
            legacy_module=ev.source_module,
            legacy_verb=ev.source_module + "_milestone_share",
        )

    return Rule("social.milestone", match)


class RuleDispatcher:
    def __init__(self, engine: LifeEngine, registry):
        self.engine = engine
        self.world = engine.service.world_state
        self.store = engine.store
        self.registry = registry
        self.kernel_rules: dict[str, Rule] = {
            "social.milestone": milestone_sharing(engine.s),
        }
        self.subscriptions = {}
        self.observer_lock = asyncio.Lock()
        self.registration_lock = asyncio.Lock()

    def _combined_rules(self) -> dict[str, Rule]:
        return {**self.kernel_rules, **self.registry.rules}

    def _is_proactive_speak_action(self, action_name: str) -> bool:
        spec = self.engine.catalog.get(action_name)
        if spec is None:
            return False
        return bool(
            spec.social
            and (
                spec.proactive_speak
                or spec.name in {"social.speak", "qq.speak"}
                or spec.name.endswith(".speak")
            )
        )

    async def initialize(self, *, before_event=None) -> None:
        combined_rules = self._combined_rules()
        expected_keys = combined_rules.keys() | self.registry.public_event_observers.keys()
        if set(self.subscriptions) == expected_keys:
            return
        async with self.registration_lock:
            combined_rules = self._combined_rules()
            expected_keys = combined_rules.keys() | self.registry.public_event_observers.keys()
            for stale in set(self.subscriptions) - expected_keys:
                self.subscriptions.pop(stale, None)
            new = {
                name: partial(self.apply, rule)
                for name, rule in combined_rules.items()
                if name not in self.subscriptions
            }
            new.update(
                {
                    name: partial(self.observe_public, name, callback)
                    for name, callback in self.registry.public_event_observers.items()
                    if name not in self.subscriptions
                }
            )
            if not new:
                return
            async with self.store.transaction():
                for name in new:
                    await self.store.db.execute(
                        "INSERT OR IGNORE INTO life_rule_cursors "
                        "SELECT ?,COALESCE(?,MAX(seq),0) FROM life_event_order",
                        (name, before_event),
                    )
            self.subscriptions.update(new)

    async def apply(self, rule: Rule, row, now: float) -> str | None:
        ev = self.world.event_from_row(row)
        source = self.registry.get(ev.source_module)
        owner = self.registry.rule_owners.get(rule.name)
        if (owner is not None and self.registry.inactive_reason(owner)) or (
            source and source.inactive_reason
        ):
            return None
        reaction = rule.match(ev)
        if not reaction or (
            ev.payload.get("private_first")
            and (
                not self._is_proactive_speak_action(reaction.action)
                or not private_event_for_owner(ev.payload, reaction.recipient, self.engine.s)
            )
        ):
            return None
        ts = max(now, ev.created_at)
        async with self.store.transaction():
            cursor = await self.store.one(
                "SELECT last_row FROM life_rule_cursors WHERE rule_id=?", (rule.name,)
            )
            if not cursor or row["seq"] <= cursor[0]:
                return None
            seen = await self.store.one(
                "SELECT 1 FROM life_rule_receipts WHERE rule_id=? AND event_id=?",
                (rule.name, ev.id),
            )
            if seen:
                return None
            prev = await self.store.one(
                "SELECT MAX(created_at) FROM life_rule_receipts "
                "WHERE rule_id=? AND cooldown_key=? AND intent_id IS NOT NULL",
                (rule.name, ev.source_module),
            )
            intent_id = None
            if ts >= ev.created_at + reaction.ttl:
                reason = "expired"
            elif prev[0] is not None and ts < prev[0] + reaction.cooldown:
                reason = "cooldown"
            else:
                catalog = self.engine.catalog
                canon_action = catalog.canonical_name(reaction.action) or reaction.action
                module = catalog.owners.get(canon_action, "")
                try:
                    ticket = IntentTicketCreate(
                        action_name=canon_action,
                        target_scope=reaction.recipient,
                        origin="rule",
                        target_module=reaction.legacy_module or module,
                        action_type=reaction.legacy_verb or canon_action,
                        source_event_id=ev.id,
                        source_module=ev.source_module,
                        arguments=reaction.arguments,
                        execute_after=max(ts, ev.created_at + reaction.delay),
                        expire_at=ev.created_at + reaction.ttl,
                        gate_condition={"milestone_event_id": ev.id, "importance": ev.importance},
                        intent_note=reaction.note,
                        mood=self.world.current.mood,
                        dedupe_key=f"rule:{rule.name}:{ev.id}",
                    )
                    self.engine.resolve(ticket.model_dump())
                except ValueError as exc:
                    reason = reject_reason(exc)
                else:
                    intent_id = await self.world.insert_intent_tx(ticket, now=ts)
                    reason = "enqueued"
            await self.store.db.execute(
                "INSERT INTO life_rule_receipts VALUES (?,?,?,?,?,?)",
                (rule.name, ev.id, ev.source_module, ts, reason, intent_id),
            )
        return intent_id

    async def observe_public(self, name, callback, row, now):
        event = self.world.event_from_row(row)
        if event.payload.get("private_first"):
            return
        async with self.observer_lock:
            async with self.store.lock:
                pending = await self.store.one(
                    "SELECT 1 FROM life_rule_cursors c WHERE c.rule_id=? "
                    "AND c.last_row<? AND NOT EXISTS(SELECT 1 FROM life_rule_receipts r "
                    "WHERE r.rule_id=c.rule_id AND r.event_id=?)",
                    (name, row["seq"], event.id),
                )
            if not pending or not await callback(event, now):
                return
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO life_rule_receipts VALUES (?,?,?,?,'recorded',NULL)",
                    (name, event.id, event.source_module, now),
                )

    def active_subscriptions(self):
        return [
            (name, consume)
            for name, consume in self.subscriptions.items()
            if self.engine.config.enabled or name in self.registry.public_event_observers
        ]

    async def notify(self, event: MilestoneEvent, now: float) -> str | None:
        async with self.store.lock:
            row = await self.store.one(
                "SELECT o.seq,e.* FROM life_event_order o JOIN global_events e ON e.id=o.event_id "
                "WHERE e.id=?",
                (event.id,),
            )
        first = None
        if row:
            await self.initialize(before_event=row["seq"] - 1)
            for name, consume in self.active_subscriptions():
                try:
                    created = await consume(row, now)
                    first = first or created
                except Exception:
                    logging.getLogger("hakimi").warning(
                        "event=life_subscription_retry rule=%s", name
                    )
        return first

    async def catch_up(self, now: float) -> None:
        # Live publication can finish out of order. Only the ordered scan advances a cursor.
        await self.initialize()
        for name, consume in self.active_subscriptions():
            async with self.store.lock:
                rows = await self.store.all(
                    "SELECT o.seq,e.* FROM life_event_order o "
                    "JOIN global_events e ON e.id=o.event_id "
                    "WHERE seq>(SELECT last_row FROM life_rule_cursors WHERE rule_id=?) "
                    "ORDER BY seq LIMIT 50",
                    (name,),
                )
            try:
                for row in rows:
                    await consume(row, now)
            except Exception:
                logging.getLogger("hakimi").warning("event=life_subscription_retry rule=%s", name)
                continue
            if rows:
                async with self.store.transaction():
                    await self.store.db.execute(
                        "UPDATE life_rule_cursors SET last_row=MAX(last_row,?) WHERE rule_id=?",
                        (rows[-1]["seq"], name),
                    )
