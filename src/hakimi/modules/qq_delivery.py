"""JIT conversation and OneBot delivery for the single proactive social action."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from ..bot_server import is_persona_owner
from ..budget_policy import PROACTIVE_LANE
from ..generation_policy import Scenario
from ..outgoing import deliver_proactive_jit
from ..routing import RouteDecision
from ..security import authorize
from .actions import ActionCall, Outcome
from .base import sanitize_sensor_text
from .qq_audience import _scope_to_message
from .qq_policy import QQPolicy

JIT_TRIGGER_MESSAGE = (
    "[系统主动意图触发：请结合 jit_proactive_intent 与最新上下文自然临场发言，"
    "若当前语境不适合插话请将 abstain 设为 true]"
)


class LifeSpeaker:
    def __init__(self, service):
        self.service = service
        self.s = service.s
        self.store = service.store

    async def speak(self, call: ActionCall) -> Outcome:
        ticket, ts = call.ticket, call.now
        ws = self.service.world_state
        intent_id = ticket["id"]
        target_scope, action_type = ticket["target_scope"], ticket["action_type"]
        target_module = call.evidence.get("source_module") or ticket["target_module"]
        desktop_context_hint = call.desktop_hint
        target_msg = _scope_to_message(
            target_scope,
            self.s,
            event_id=f"jit:{intent_id}",
            created_at=ts,
        )
        if target_msg is None or not authorize(target_msg, self.s):
            return Outcome("gated_skip", "unauthorized_scope")

        module = self.service.modules.get(target_module)
        parts = [await module.social_material(ticket)] if module else []
        if call.evidence:
            parts.append(
                "事件:"
                + call.evidence["event_type"]
                + " "
                + json.dumps(call.evidence["payload"], ensure_ascii=False, separators=(",", ":"))
            )
        material_summary = sanitize_sensor_text(
            " | ".join(filter(None, parts)), self.s, max_tokens=90
        )
        private_owner = target_msg.kind == "private" and is_persona_owner(target_msg, self.s)
        session = await self.service.sessions.snapshot(target_msg)
        recent = list(session.get("recent_messages", []))[-10:]
        if not recent and target_msg.kind == "group" and getattr(self.service, "hot", None):
            ring = self.service.hot.rings.get(self.service.hot.key(target_msg))
            if ring:
                for seq, item, _ in list(ring.messages.values())[-10:]:
                    if self.service.hot.recalled(target_msg, item.event_id):
                        continue
                    recent.append(
                        {
                            "seq": seq,
                            "role": "assistant" if item.user_id == item.bot_id else "user",
                            "text": item.text,
                            "speaker": item.sender_nickname
                            or ("吉米奈喵" if item.user_id == item.bot_id else "群友"),
                            "user_id": item.user_id,
                            "event_id": item.event_id,
                            "created_at": item.created_at,
                        }
                    )

        batch_affection: dict[str, dict[str, Any]] = {}
        aff = getattr(self.store.runtime, "affection", None)
        if aff is not None and aff.config.enabled:
            seen_uids: set[str] = set()
            for item in reversed(recent):
                uid = str(item.get("user_id", "")).strip()
                if (
                    not uid
                    or uid in seen_uids
                    or uid in {"0", target_msg.bot_id}
                    or uid in self.s.bot_qq_ids
                ):
                    continue
                seen_uids.add(uid)
                try:
                    prof = await aff.profile(target_msg.platform, uid)
                    batch_affection[uid] = {
                        "score": prof["score"],
                        "lv": prof["lv"],
                        "tier": prof["tier"],
                        "attitude": prof["attitude"],
                    }
                except Exception:
                    continue
                if len(batch_affection) >= 6:
                    break

        ws_summary = await ws.chat_sensor_payload()
        jit_intent: dict[str, Any] = {
            "intent_id": intent_id,
            "target_module": target_module,
            "action_type": action_type,
            "mood": ticket.get("mood") or ws.current.mood,
            "intent_note": ticket.get("intent_note") or {},
        }
        if material_summary:
            jit_intent["module_material"] = material_summary
        if desktop_context_hint:
            jit_intent["desktop_context_hint"] = desktop_context_hint

        payload: dict[str, Any] = {
            "current_message": JIT_TRIGGER_MESSAGE,
            "current_sender": {
                "nickname": "BOISER" if private_owner else "系统主动触发",
                "qq": target_msg.user_id,
            },
            "surface": target_msg.kind,
            "directly_addressed": True,
            "recent_messages": recent,
            "rolling_summary": session.get("rolling_summary", ""),
            "session_epoch": session["epoch"],
            "session_enabled": False,
            "history_revision": session.get("history_revision", 0),
            "interaction_chains": {},
            "allowed_reply_ids": [],
            "jit_proactive_intent": jit_intent,
        }
        if batch_affection:
            payload["batch_affection"] = batch_affection
        if ws_summary:
            payload["world_state_summary"] = ws_summary

        payload.update(await self.service.modules.context_data(limit=3, now=ts, message=target_msg))

        if private_owner:
            if not call.evidence:
                payload["companion_context"] = await self.service.proactive.companion_context(ts)
            cloud_mem = getattr(self.service, "cloud_memory", None)
            if cloud_mem is not None:
                mem_data = await asyncio.to_thread(
                    cloud_mem.for_message, target_msg, "", recall=False
                )
                if mem_data:
                    payload.update(mem_data)
            notebook = getattr(self.service, "notebook", None)
            if notebook is not None:
                snap = notebook.for_message(target_msg)
                if snap:
                    payload["owner_notebook_snapshot"] = snap
            dual_data = await self.service.sessions.dual.prefetch(
                target_msg,
                "",
                recall=True,
                related_user_ids=[self.s.owner_qq_id],
            )
            if dual_data:
                payload["dual_memory_data"] = dual_data
        else:
            dual_data = await self.service.sessions.dual.prefetch(
                target_msg,
                "",
                recall=False,
                related_user_ids=list(batch_affection.keys()),
            )
            if dual_data:
                payload["dual_memory_data"] = dual_data

        subject = self.store.subject(target_msg)
        scenario = Scenario.OWNER_PRIVATE if private_owner else Scenario.FAST_CHAT
        route = RouteDecision("chat", tier=1, need_memory_recall=private_owner)
        if _scope_to_message(target_scope, self.s, event_id=intent_id, created_at=ts) is None:
            return Outcome("gated_skip", "unauthorized_scope")
        if not call.still_allowed():
            return Outcome("gated_skip", "action_no_longer_eligible")
        try:
            generated = await self.service.ai.generate(
                payload,
                subject,
                scenario=scenario,
                message=target_msg,
                route=route,
                budget_lane=PROACTIVE_LANE,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return Outcome("gated_skip", "generation_failed")

        async def owner_available():
            return not await QQPolicy(self.service).owner_recently_active(
                ticket, max(ts, self.service.life_engine.clock())
            )

        delivery = await deliver_proactive_jit(
            self.service,
            target_msg,
            generated,
            reply_id=f"jit:{intent_id}",
            intent_id=intent_id,
            still_allowed=call.still_allowed,
            before_delivery=owner_available if private_owner else None,
        )
        status = delivery["status"]
        if status == "sent":
            return Outcome("done", "qq_acknowledged", {"delivery": delivery})
        if status in ("not_dispatched", "uncertain"):
            return Outcome(status, status, {"delivery": delivery})
        return Outcome("gated_skip", status, {"delivery": delivery})
