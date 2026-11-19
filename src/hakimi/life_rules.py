"""Local event subscriptions. Each rule owns an independent durable cursor and receipt."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING

from .modules.actions import reject_reason
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


_MC_THEME = re.compile(
    r"我的世界|minecraft|\bmc\b|红石|生电|建筑教程|生存建筑|刷铁机|下界|末地|苦力怕", re.I
)


def build_from_video(event: MilestoneEvent) -> Reaction | None:
    if event.payload.get("private_first"):
        return None
    if event.source_module != "bilibili_surf" or event.event_type not in {
        "bili_video_watched",
        "bili_mc_inspiration",
    }:
        return None
    topic = str(event.payload.get("title", ""))
    if event.event_type == "bili_mc_inspiration" or _MC_THEME.search(
        topic + " " + str(event.payload.get("summary", ""))
    ):
        return Reaction(
            "mc.build_plan",
            "根据已看视频登记建造灵感",
            ttl=7200,
            arguments={"topic": topic[:80]},
            legacy_module="minecraft",
            legacy_verb="build_plan",
        )
    return None


def milestone_sharing(settings) -> Rule:
    from .modules.qq_audience import _allowlisted_groups

    def match(ev):
        if ev.importance != "high" or ev.event_type == "bili_mc_inspiration":
            return None
        if ev.event_type == "bili_video_watched" and not settings.owner_qq_id:
            return None
        if settings.owner_qq_id:
            recipient = "owner:" + settings.owner_qq_id
        else:
            groups = _allowlisted_groups(settings)
            recipient = "onebot:" + groups[0] if groups else ""
        if ev.payload.get("private_first") and not private_event_for_owner(
            ev.payload, recipient, settings
        ):
            return None
        if not recipient:
            return None
        return Reaction(
            "qq.speak",
            "结合近期活动分享重要进展",
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
        self.subscriptions = {}
        self.observer_lock = asyncio.Lock()
        self.registration_lock = asyncio.Lock()

    async def initialize(self, *, before_event=None) -> None:
        if len(self.subscriptions) == len(self.registry.rules) + len(
            self.registry.public_event_observers
        ):
            return
        async with self.registration_lock:
            new = {
                name: partial(self.apply, rule)
                for name, rule in self.registry.rules.items()
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
        if self.registry.inactive_reason(self.registry.rule_owners[rule.name]) or (
            source and source.inactive_reason
        ):
            return None
        reaction = rule.match(ev)
        if not reaction or (
            ev.payload.get("private_first")
            and (
                reaction.action != "qq.speak"
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
                module = catalog.owners.get(reaction.action, "")
                try:
                    ticket = IntentTicketCreate(
                        action_name=reaction.action,
                        target_scope=reaction.recipient,
                        origin="rule",
                        target_module=reaction.legacy_module or module,
                        action_type=reaction.legacy_verb or reaction.action,
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
