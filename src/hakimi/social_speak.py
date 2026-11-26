"""Platform-agnostic JIT proactive social speaking for any authenticated chat transport."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from .bot_server import is_persona_owner
from .budget_policy import PROACTIVE_LANE
from .generation_policy import Scenario
from .models import Message
from .modules.actions import ActionCall, Outcome
from .modules.base import sanitize_sensor_text
from .outgoing import deliver_proactive_jit
from .routing import RouteDecision
from .security import authorize
from .social import owner_target

JIT_TRIGGER_MESSAGE = (
    "[系统主动意图触发：这是你自己先前排定的主动发言时刻。请结合 "
    "jit_proactive_intent、待分享见闻与近期上下文，用自然松弛的口语开口；"
    "私聊可自然开启新话头或挂念日常，群聊若正逢严肃争执或完全插不上嘴可将 abstain 设为 true]"
)


def resolve_social_target(
    service: Any,
    platform: str,
    bot_id: str,
    target_scope: str,
    *,
    event_id: str,
    created_at: float,
) -> Message | None:
    """Resolve a canonical proactive scope (`boundowner:...` or `<plat>:<gid>`) to Message."""
    scope = str(target_scope or "").strip()
    if not scope:
        return None
    proactive = getattr(service, "proactive", None)
    if (
        proactive is not None
        and proactive.platform == platform
        and scope in {proactive.target_scope, "owner", "owner_dm"}
        and proactive.configured
    ):
        return owner_target(service.s, platform, created_at, event_id)
    prefix = f"{platform}:"
    if scope.startswith(prefix):
        gid = scope[len(prefix) :].strip()
        if not gid or not bot_id:
            return None
        if service.s.group_policies.get(scope, "at_only") == "muted":
            return None
        return Message(
            platform=platform,
            bot_id=bot_id,
            event_id=event_id,
            user_id="0",
            conversation_id=gid,
            kind="group",
            text="",
            addressed=True,
            created_at=created_at,
        )
    return None


async def speak_proactive(
    service: Any,
    target_msg: Message | None,
    call: ActionCall,
) -> Outcome:
    """Generate and deliver a JIT proactive turn over any authenticated chat module."""
    ticket, ts = call.ticket, call.now
    ws = service.world_state
    intent_id = ticket["id"]
    action_type = ticket["action_type"]
    target_module = call.evidence.get("source_module") or ticket["target_module"]
    desktop_context_hint = call.desktop_hint

    if target_msg is None or not authorize(target_msg, service.s):
        return Outcome("gated_skip", "unauthorized_scope")

    selected_news = {}
    if ticket.get("source_event_id") or ticket.get("gate_condition", {}).get("share_news"):
        selected_news = await ws.experience.reserve_news(target_msg, ticket, ts)
        if selected_news is None:
            return Outcome("gated_skip", "news_already_shared_or_expired")

    module = service.modules.get(target_module)
    if module and module.inactive_reason:
        return Outcome("gated_skip", module.inactive_reason)
    parts = [await module.social_material(ticket)] if module else []
    if call.evidence:
        parts.append(
            "事件:"
            + call.evidence["event_type"]
            + " "
            + json.dumps(call.evidence["payload"], ensure_ascii=False, separators=(",", ":"))
        )
    material_summary = sanitize_sensor_text(
        " | ".join(filter(None, parts)), service.s, max_tokens=90
    )
    private_owner = target_msg.kind == "private" and is_persona_owner(target_msg, service.s)
    session = await service.sessions.snapshot(target_msg)
    recent = list(session.get("recent_messages", []))[-10:]
    if not recent and target_msg.kind == "group" and getattr(service, "hot", None):
        ring = service.hot.rings.get(service.hot.key(target_msg))
        if ring:
            for seq, item, _ in list(ring.messages.values())[-10:]:
                if service.hot.recalled(target_msg, item.event_id):
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
    aff = getattr(service.store.runtime, "affection", None)
    if aff is not None and aff.config.enabled:
        seen_uids: set[str] = set()
        for item in reversed(recent):
            uid = str(item.get("user_id", "")).strip()
            if not uid or uid in seen_uids or uid in {"0", target_msg.bot_id}:
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

    ws_summary = await ws.chat_sensor_payload(target_msg)
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
            "user_id": target_msg.user_id,
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
    news = [selected_news] if selected_news else await ws.experience.news(target_msg, ts, limit=2)
    if news:
        payload["unshared_world_news"] = news
    if batch_affection:
        payload["batch_affection"] = batch_affection
    if ws_summary:
        payload["world_state_summary"] = ws_summary

    payload.update(await service.modules.context_data(limit=3, now=ts, message=target_msg))

    if private_owner:
        if not call.evidence and service.proactive.matches(target_msg):
            payload["companion_context"] = await service.proactive.companion_context(ts)
        cloud_mem = getattr(service, "cloud_memory", None)
        if cloud_mem is not None:
            mem_data = await asyncio.to_thread(cloud_mem.for_message, target_msg, "", recall=False)
            if mem_data:
                payload.update(mem_data)
        notebook = getattr(service, "notebook", None)
        if notebook is not None:
            snap = notebook.for_message(target_msg)
            if snap:
                payload["owner_notebook_snapshot"] = snap
        dual_data = await service.sessions.dual.prefetch(
            target_msg,
            "",
            recall=True,
            related_user_ids=[target_msg.user_id],
        )
    else:
        dual_data = await service.sessions.dual.prefetch(target_msg, "", recall=True)
    if dual_data:
        payload["dual_memory_data"] = dual_data

    route = RouteDecision("chat", tier=1, need_memory_recall=private_owner)
    scenario = Scenario.OWNER_PRIVATE if private_owner else Scenario.FAST_CHAT
    subject = service.store.subject(target_msg)
    try:
        raw_reply = await service.ai.generate(
            payload,
            subject,
            scenario=scenario,
            message=target_msg,
            route=route,
            budget_lane=PROACTIVE_LANE,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        return Outcome("not_dispatched", f"gen_failed:{type(exc).__name__}"[:48])

    res = await deliver_proactive_jit(
        service,
        target_msg,
        raw_reply,
        reply_id=f"jit:{intent_id}",
        intent_id=intent_id,
        still_allowed=call.still_allowed,
    )
    status = str(res.get("status", "not_dispatched"))
    if status == "sent":
        if selected_news and selected_news.get("id"):
            await ws.experience.mark_news_shared(selected_news["id"], ts)
        return Outcome("done", "proactive_sent", {"delivery": res})
    if status in ("not_dispatched", "uncertain"):
        return Outcome(status, status, {"delivery": res})
    return Outcome("gated_skip", status, {"delivery": res})
