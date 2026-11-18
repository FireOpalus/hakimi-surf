"""Local event subscriptions. Each rule owns an independent durable cursor and receipt."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING

from .modules.actions import reject_reason
from .reply_style import trim_complete_text
from .world_state import IntentTicketCreate, MilestoneEvent

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
    def __init__(self, engine: LifeEngine, rules: list[Rule]):
        self.engine = engine
        self.world = engine.service.world_state
        self.store = engine.store
        self.subscriptions = {rule.name: partial(self.apply, rule) for rule in rules}
        self.journal_lock = asyncio.Lock()
        if len(self.subscriptions) != len(rules) or "diary.milestone" in self.subscriptions:
            raise ValueError("duplicate event rule")
        self.subscriptions["diary.milestone"] = self.record_diary

    async def initialize(self) -> None:
        async with self.store.transaction():
            for name in self.subscriptions:
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO life_rule_cursors "
                    "SELECT ?,COALESCE(MAX(seq),0) FROM life_event_order",
                    (name,),
                )

    async def apply(self, rule: Rule, row, now: float) -> str | None:
        ev = self.world.event_from_row(row)
        reaction = rule.match(ev)
        if not reaction:
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

    async def record_diary(self, row, now: float) -> None:
        journal = getattr(getattr(self.engine.service, "diary", None), "world_event", None)
        if journal is None:
            return
        ev = self.world.event_from_row(row)
        if (
            ev.importance != "high"
            or now >= ev.created_at + 86400
            or ev.event_type in {"bili_video_watched", "bili_mc_inspiration"}
        ):
            return
        async with self.journal_lock:
            async with self.store.lock:
                pending = await self.store.one(
                    "SELECT 1 FROM life_rule_cursors c WHERE c.rule_id='diary.milestone' "
                    "AND c.last_row<? AND NOT EXISTS(SELECT 1 FROM life_rule_receipts r "
                    "WHERE r.rule_id=c.rule_id AND r.event_id=?)",
                    (row["seq"], ev.id),
                )
            if not pending:
                return
            data = json.dumps(ev.payload, ensure_ascii=False, separators=(",", ":"))
            summary = trim_complete_text(f"重要动态 {ev.event_type} {data}", 120)
            # The diary writer deduplicates this stable ID in its own transaction.
            await journal(
                "milestone:" + ev.id, ev.source_module, ev.event_type, summary, now=ev.created_at
            )
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO life_rule_receipts "
                    "VALUES ('diary.milestone',?,?,?,'recorded',NULL)",
                    (ev.id, ev.source_module, now),
                )

    async def notify(self, event: MilestoneEvent, now: float) -> str | None:
        if not self.engine.config.enabled:
            return None
        async with self.store.lock:
            row = await self.store.one(
                "SELECT o.seq,e.* FROM life_event_order o JOIN global_events e ON e.id=o.event_id "
                "WHERE e.id=?",
                (event.id,),
            )
        first = None
        if row:
            for name, consume in self.subscriptions.items():
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
        for name, consume in self.subscriptions.items():
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
