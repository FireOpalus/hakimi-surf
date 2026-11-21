"""ACK-only multi-bubble delivery, with conservative ambiguity and recall semantics."""

import asyncio
import random
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import nullcontext

from .bot_server import is_persona_owner
from .memes import explicit_meme_request
from .models import Message
from .modules.adapters import farewell_send_bypass
from .outbound_actions import bubble_segments, parse_actions, valid_receipt
from .output_links import filter_reply_links
from .security import Rejected, authorize, configured_secrets, safe_output
from .social import chat_module, confirmed_message_id
from .text_card import should_render_rich_card

PRE_DISPATCH_REJECTED = frozenset(
    {
        "onebot_disconnected",
        "onebot_not_dispatched",
        "onebot_unavailable",
        "plugin_disconnected",
        "plugin_not_dispatched",
        "module_disabled",
        "delivery_not_authorized",
        "reply_expired",
    }
)

_VOICE_PRE_DISPATCH_FALLBACK = frozenset(
    {
        "voice_payload_too_large",
        "voice_not_supported",
        "invalid_voice_audio",
    }
)


def _resolve_platform_voice_capability(service, message: Message) -> tuple[bool, str]:
    cap_fn = getattr(service, "platform_voice_capability", None)
    if callable(cap_fn):
        return cap_fn(message)
    if message.platform == "onebot":
        return (hasattr(service.sender, "send_voice"), "mp3")
    if message.platform == "qq":
        return (False, "")
    module = chat_module(service.s, message.platform)
    return (
        bool(module and getattr(module, "supports_voice", False)),
        str(getattr(module, "preferred_voice_format", "ogg_opus") or "ogg_opus"),
    )


async def _send_text_bubble(
    service,
    message: Message,
    text: str,
    *,
    anchor: dict,
    include_anchor: bool,
    targets,
    before_send,
    apply_output: bool = True,
) -> dict:
    if message.platform == "onebot" and hasattr(service.sender, "send_action_bubble"):
        return await service.sender.send_action_bubble(
            message,
            bubble_segments(text, anchor, include_anchor=include_anchor),
            targets=targets,
            before_send=before_send,
        )
    rendered = service.output(text) if (apply_output and hasattr(service, "output")) else text
    if message.platform not in {"onebot", "qq"}:
        module = chat_module(service.s, message.platform)
        if module is not None:
            ident = await module.send_chat(message, rendered, before_send=before_send)
            return {"message_id": confirmed_message_id(ident)}
    return await service.sender.send(message, rendered, before_send=before_send)


async def _deliver_voice_or_fallback(
    service,
    message: Message,
    voice_text: str,
    *,
    anchor: dict,
    include_anchor: bool,
    targets,
    valid: Callable[[], Awaitable[bool]],
    acknowledge: Callable[[str, dict], Awaitable[None]],
    before_dispatch: Callable[[], Awaitable[bool]] | None = None,
    proactive: bool = False,
    explicit_voice: bool = False,
    delay_before: bool = False,
    apply_output: bool = True,
) -> None:
    if delay_before:
        if hasattr(service.sender, "typing"):
            await service.sender.typing(message)
        await asyncio.sleep(
            _pacing_delay(
                service,
                min(2.2, 0.8 + len(voice_text) * 0.025 + random.uniform(0, 0.2)),
            )
        )

    voice_client = getattr(service, "voice_reply", None)
    supported, preferred_fmt = _resolve_platform_voice_capability(service, message)
    scope = service.store.session_scope(message)
    eligible = (
        voice_client is not None
        and supported
        and voice_client.allowed_for_message(message)
        and voice_client.can_send_now(scope, explicit=explicit_voice)[0]
    )

    fallback_to_text = not eligible
    receipt: dict | None = None
    if eligible:
        assert voice_client is not None
        try:
            synth = await voice_client.synthesize(voice_text, output_format=preferred_fmt)
        except Rejected:
            fallback_to_text = True
        else:
            if not await valid():
                raise Rejected("reply_invalidated")
            voice_gate = before_dispatch if before_dispatch is not None else valid
            try:
                if message.platform == "onebot" and hasattr(service.sender, "send_voice"):
                    receipt = await service.sender.send_voice(
                        message,
                        synth.audio_bytes,
                        fmt=synth.fmt,
                        duration_ms=synth.duration_ms,
                        proactive=proactive,
                        before_send=voice_gate,
                    )
                else:
                    module = chat_module(service.s, message.platform)
                    if module is None or not getattr(module, "supports_voice", False):
                        fallback_to_text = True
                    else:
                        ident = await module.send_voice(
                            message,
                            synth.audio_bytes,
                            fmt=synth.fmt,
                            mime_type=synth.mime_type,
                            duration_ms=synth.duration_ms,
                            before_send=voice_gate,
                        )
                        receipt = {"message_id": confirmed_message_id(ident)}
            except Rejected as exc:
                if str(exc) in _VOICE_PRE_DISPATCH_FALLBACK:
                    fallback_to_text = True
                    receipt = None
                else:
                    raise

            if not fallback_to_text and receipt is not None:
                await acknowledge(f"[语音] {voice_text}", receipt)
                voice_client.note_sent(scope)
                return

    if not await valid():
        raise Rejected("reply_invalidated")
    text_gate = before_dispatch if before_dispatch is not None else valid
    receipt = await _send_text_bubble(
        service,
        message,
        voice_text,
        anchor=anchor,
        include_anchor=include_anchor,
        targets=targets,
        before_send=text_gate,
        apply_output=apply_output,
    )
    await acknowledge(voice_text, receipt)


def _pacing_delay(service, seconds: float) -> float:
    if float(getattr(service.s, "send_delay_max", 0.0) or 0.0) <= 0.0:
        return 0.0
    return seconds


def safe_delivery_detail(exc: Rejected) -> str:
    code = str(exc).strip()
    return code if re.fullmatch(r"[a-z0-9_]{1,64}", code) else "delivery_uncertain"


async def is_affection_muted_only(service, message: Message) -> bool:
    """Return True only if the message sender is muted due to zero affection (never manual mute)."""
    if message.platform == "onebot":
        return await service.moderation.is_affection_muted(message.user_id)
    identity = getattr(service, "identity", None)
    if identity is not None:
        if not identity.is_muted(message.platform, message.user_id):
            return False
        pid = identity.cached_person_id(message.platform, message.user_id)
        if pid is None:
            pid = await identity.resolve_person_id(message.platform, message.user_id, create=False)
        if pid is None:
            return False
        return (await identity.person_mute_source(pid)) == "ai_affection_zero"
    return True


async def persist_ack(service, reply, position, text, receipt, epoch, digest=None):
    if not valid_receipt(receipt, platform=reply["message"]["platform"]):
        raise Rejected("onebot_send_unconfirmed")
    data = {
        "message": reply["message"],
        "epoch": epoch,
        "text": text,
        "message_id": str(receipt["message_id"]),
        "digest": digest,
        "participants": reply.get("participants") or [reply["message"]],
        "consumed_image_events": reply.get("consumed_image_events") or [],
        "consumed_file_events": reply.get("consumed_file_events") or [],
    }
    world = getattr(service, "world_state", None)
    async with service.store.transaction():
        now = world.clock() if world and world.experience.ready else time.time()
        await service.store.db.execute(
            "INSERT OR IGNORE INTO outbox_bubble_receipts VALUES (?,?,?,?,0)",
            (
                reply["id"],
                position,
                service.store.vault.seal(f"ack:{reply['id']}:{position}", data),
                now,
            ),
        )
        if world and world.experience.ready:
            await world.experience.spend_chat_tx(reply["id"], now, message=reply["message"])
    return data


async def apply_receipts(service, reply_id):
    async with service.store.lock:
        rows = await service.store.all(
            "SELECT * FROM outbox_bubble_receipts WHERE reply_id=? AND applied=0 ORDER BY position",
            (reply_id,),
        )
    if not rows:
        return []
    data = [service.store.vault.open(f"ack:{reply_id}:{r['position']}", r["payload"]) for r in rows]
    message = Message.model_validate(data[0]["message"])
    epoch = data[0]["epoch"]
    appended = await service.sessions.reply_batch(message, reply_id, data, expected_epoch=epoch)
    if appended:
        remembered_group = False
        combined_text = "\n".join(d["text"] for d in data)
        for item in data:
            receipt = {"message_id": item["message_id"]}

            async def remember_sent(receipt=receipt):
                nonlocal remembered_group
                if await service.store.runtime.attention.record_sent(message, receipt):
                    if not remembered_group and message.kind == "group":
                        remembered_group = True
                        await service.store.runtime.filters.remember_group(
                            message.model_copy(
                                update={
                                    "event_id": "sent:" + str(reply_id),
                                    "user_id": message.bot_id,
                                    "images": [],
                                    "text": "[哈基米已回复] " + service.output(combined_text),
                                    "created_at": time.time(),
                                }
                            )
                        )

            await service.sessions.receive_outgoing(
                message,
                item["message_id"],
                item["text"],
                expected_epoch=epoch,
                on_confirmed=remember_sent,
            )
        service.store.runtime.filters.spoke(message)
        if message.kind == "group":
            members = [
                Message.model_validate(m) for m in data[0].get("participants", [data[0]["message"]])
            ]
            await service.public_memory.record(
                reply_id, message, members, "\n".join(d["text"] for d in data), expected_epoch=epoch
            )
            consumed = data[0].get("consumed_image_events") or []
            if consumed:
                await service.store.groups.mark_images_consumed(message, consumed)
            consumed_files = data[0].get("consumed_file_events") or []
            if consumed_files:
                await service.store.groups.mark_files_consumed(message, consumed_files)
        if service.diary and combined_text.strip():
            await service.diary.bot_reply(reply_id, message, combined_text)
    # Idempotent B receipts/public index make crashes before this marker safe to recover.
    async with service.store.transaction():
        world = getattr(service, "world_state", None)
        if world and world.experience.ready:
            dedicated = None
            if reply_id.startswith("jit:"):
                ticket = await service.store.one(
                    "SELECT n.event_id FROM scheduled_intents t JOIN world_news n "
                    "ON n.intent_id=t.id WHERE t.id=? "
                    "AND action_name='qq.speak' AND expected_bubbles>0 AND expected_bubbles="
                    "(SELECT COUNT(*) FROM outbox_bubble_receipts WHERE reply_id=?)",
                    (reply_id[4:], reply_id),
                )
                dedicated = ticket[0] if ticket else None
            await world.experience.confirmed_tx(
                message,
                reply_id,
                min(r["created"] for r in rows),
                [item["text"] for item in data],
                dedicated=dedicated,
            )
        await service.store.db.executemany(
            "UPDATE outbox_bubble_receipts SET applied=1 WHERE reply_id=? AND position=?",
            [(reply_id, row["position"]) for row in rows],
        )
    if world and world.experience.ready:
        await world.refresh()
    return data if appended else []


async def recover_receipts(service):
    async with service.store.lock:
        rows = await service.store.all(
            "SELECT DISTINCT reply_id FROM outbox_bubble_receipts WHERE applied=0"
        )
    for row in rows:
        await apply_receipts(service, row[0])


async def deliver_actions(service, reply):
    store = service.store
    message = Message.model_validate(reply["message"])
    members = [Message.model_validate(m) for m in reply.get("participants", [])] or [message]
    actions = reply["actions"]
    epoch = reply.get("session_epoch", -1)
    identity = getattr(service, "identity", None)
    farewell_bypass_mute = bool(actions.get("farewell_bypass_mute")) and (
        await is_affection_muted_only(service, message)
    )
    raw_bypass_ids = {
        str(u).strip()
        for u in (actions.get("farewell_bypass_user_ids") or [])
        if isinstance(u, str) and str(u).strip()
    }
    if actions.get("farewell_bypass_mute"):
        raw_bypass_ids.add(message.user_id)
    status, detail, confirmed, position = "sent", "", [], 0
    sticker_choice = None

    async def valid():
        if await service.recalled(members):
            return False
        if time.time() > message.created_at + service.s.event_ttl_seconds:
            return False
        if any(
            service.hot.recalled(m) or any(service.hot.recalled(m, ref) for ref in m.reply_ids)
            for m in members
        ):
            return False
        verified_bypass_pairs: set[tuple[str, str]] = set()
        verified_bypass_uids: set[str] = set()
        if raw_bypass_ids:
            for m in [message, *members]:
                if (
                    m.user_id in raw_bypass_ids
                    and (m.platform, m.user_id) not in verified_bypass_pairs
                    and await is_affection_muted_only(service, m)
                ):
                    verified_bypass_pairs.add((m.platform, m.user_id))
                    verified_bypass_uids.add(m.user_id)
        if identity is not None and any(
            identity.is_muted(m.platform, m.user_id)
            and (m.platform, m.user_id) not in verified_bypass_pairs
            for m in members
        ):
            return False
        if not service.batch_authorized(
            message, members, bypass_mute_user_ids=verified_bypass_uids
        ):
            return False
        if not await service.bot_guard.may_process(message, reply["id"]):
            return False
        async with store.lock:
            row = await store.one("SELECT status FROM outbox WHERE id=?", (reply["id"],))
        return bool(
            row
            and row[0] == "sending"
            and (await service.sessions.snapshot(message))["epoch"] == epoch
        )

    async def targets():
        allowed = set(reply.get("allowed_at_qqs", [])) - {
            service.s.onebot_self_id,
            "all",
            "0",
            *service.s.muted_user_ids,
        }
        for bot in allowed & service.s.bot_qq_ids:
            if not await service.bot_guard.allowed_reply_target(message, bot):
                allowed.discard(bot)
        return allowed

    async def acknowledge(text, receipt):
        nonlocal position
        data = await persist_ack(
            service, reply, position, text, receipt, epoch, actions.get("image_digest")
        )
        confirmed.append(data)
        position += 1

    async def sticker():
        nonlocal sticker_choice
        if not sticker_choice:
            return

        async def image_valid():
            return await valid() and await service.memes.choice_valid(sticker_choice)

        if not await image_valid():
            raise Rejected("reply_invalidated")
        receipt = await service.sender.send_meme(
            message, sticker_choice["data"], before_send=image_valid
        )
        await acknowledge("[本地表情：" + actions["sticker"]["label"] + "]", receipt)
        await service.memes.confirm_choice(store.session_scope(message), sticker_choice["digest"])

    try:
        with farewell_send_bypass(message) if farewell_bypass_mute else nullcontext():
            if not await valid():
                raise Rejected("reply_invalidated")
            await service.bot_guard.extend_cooldown(message, reply["id"], before_send=True)
            label = actions.get("sticker", {}).get("label")
            emoji_like = actions.get("emoji_like")
            poke_qq = actions.get("poke_qq")
            anchor = dict(actions.get("anchor", {}))
            reaction_target_id = anchor.get("reply_id") or next(
                (m.event_id for m in reversed(members) if m.event_id.lstrip("-").isdigit()),
                None,
            )

            async def _try_react_and_poke() -> bool:
                did_any = False
                if (
                    emoji_like
                    and reaction_target_id
                    and message.platform == "onebot"
                    and hasattr(service.sender, "send_emoji_like")
                ):
                    try:
                        if await service.sender.send_emoji_like(
                            message, reaction_target_id, emoji_like, before_send=valid
                        ):
                            did_any = True
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                if (
                    poke_qq
                    and message.platform == "onebot"
                    and hasattr(service.sender, "send_poke")
                ):
                    try:
                        allowed_poke_pool = (
                            {message.user_id}
                            - {service.s.onebot_self_id, *service.s.muted_user_ids}
                            if message.kind == "private"
                            else await targets()
                        )
                        if poke_qq in allowed_poke_pool and await service.sender.send_poke(
                            message, poke_qq, before_send=valid
                        ):
                            did_any = True
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                return did_any

            if actions.get("abstain"):
                if await _try_react_and_poke():
                    status, detail = "sent", "reaction_only"
                else:
                    status, detail = "cancelled", "empty_reply"
            else:
                is_public = bool(actions.get("public", message.kind == "group"))
                has_text_bubbles = any(b.strip() for b in actions.get("bubbles", [])[:3])
                voice_text = str(actions.get("voice_text") or "").strip()
                has_reply_body = has_text_bubbles or bool(voice_text)
                if message.platform == "onebot":
                    force_sticker = (not has_reply_body and bool(label)) or explicit_meme_request(
                        *(m.text for m in members), message.text
                    )
                    if await service.memes.reserve_reply(
                        store.session_scope(message),
                        reply["id"],
                        label,
                        public=is_public,
                        force=force_sticker,
                    ):
                        sticker_choice = await service.memes.choose(
                            label, store.session_scope(message)
                        )
                if actions["sticker"].get("timing") == "before_text":
                    await sticker()
                    if sticker_choice and has_reply_body:
                        await asyncio.sleep(_pacing_delay(service, 0.6))
                if message.kind != "group" or (
                    service.bot_guard.known_bot(message)
                    and not service.bot_guard.is_owner_invited(message)
                ):
                    anchor = {}
                else:
                    drift = service.hot.intervening(
                        message, actions.get("watermark", 0), {m.user_id for m in members}
                    )
                    if drift == 0:
                        anchor["reply_id"] = None
                    elif drift >= 3:
                        anchor["reply_id"] = next(
                            (
                                m.event_id
                                for m in reversed(members)
                                if m.event_id.lstrip("-").isdigit()
                                and (
                                    not service.bot_guard.known_bot(m)
                                    or service.bot_guard.is_owner_invited(m)
                                )
                            ),
                            None,
                        )
                folded = actions.get("folded") if message.kind == "group" else None
                combined_bubbles = "\n".join(b for b in actions.get("bubbles", [])[:3] if b.strip())
                card_source = folded or combined_bubbles
                if (
                    actions.get("allow_text_card", True)
                    and message.kind == "group"
                    and message.platform == "onebot"
                    and card_source
                    and hasattr(service.sender, "send_card")
                    and should_render_rich_card(card_source, service.s.text_card_mode)
                ):
                    receipt = await service.sender.send_card(
                        message, card_source, before_send=valid
                    )
                    await acknowledge(card_source, receipt)
                elif folded:
                    receipt = await service.sender.send_folded(message, folded, before_send=valid)
                    await acknowledge(folded, receipt)
                else:
                    for text in actions.get("bubbles", [])[:3]:
                        if not text.strip():
                            continue
                        if confirmed:
                            if hasattr(service.sender, "typing"):
                                await service.sender.typing(message)
                            await asyncio.sleep(
                                _pacing_delay(
                                    service,
                                    min(2.2, 0.8 + len(text) * 0.025 + random.uniform(0, 0.2)),
                                )
                            )
                        if not await valid():
                            raise Rejected("reply_invalidated")
                        receipt = await _send_text_bubble(
                            service,
                            message,
                            text,
                            anchor=anchor,
                            include_anchor=not confirmed,
                            targets=targets,
                            before_send=valid,
                        )
                        await acknowledge(text, receipt)
                if voice_text:
                    await _deliver_voice_or_fallback(
                        service,
                        message,
                        voice_text,
                        anchor=anchor,
                        include_anchor=not confirmed,
                        targets=targets,
                        valid=valid,
                        acknowledge=acknowledge,
                        proactive=bool(reply.get("proactive")),
                        explicit_voice=bool(actions.get("explicit_voice")),
                        delay_before=bool(confirmed),
                    )
                if actions["sticker"].get("timing") != "before_text" and sticker_choice:
                    if confirmed:
                        await asyncio.sleep(_pacing_delay(service, 0.6))
                    await sticker()
                did_reaction = await _try_react_and_poke()
                if not confirmed and not did_reaction:
                    status, detail = "cancelled", "empty_reply"
    except asyncio.CancelledError:
        # ACKs already journalled; restart applies them once. Unknown sends stay uncertain.
        raise
    except Rejected as exc:
        code = str(exc).strip()
        if code == "reply_invalidated":
            status, detail = "cancelled", "reply_invalidated"
        elif code in PRE_DISPATCH_REJECTED and not confirmed:
            status, detail = "failed", code
        else:
            status, detail = "uncertain", safe_delivery_detail(exc)
            service.record("delivery_uncertain")
    except Exception:
        status, detail = "uncertain", "delivery_uncertain"
        service.record("delivery_uncertain")
    finally:
        applied = await asyncio.shield(apply_receipts(service, reply["id"]))
        commit = actions.get("chain_commit")
        if applied:
            service.store.runtime.filters.spoke(message)
            service.compressor.schedule(message)
            await service.bot_guard.extend_cooldown(message, reply["id"])
            if commit and status == "sent":
                await asyncio.shield(
                    service.sessions.commit_chain(
                        message,
                        lane=commit["lane"],
                        interaction_id=commit["interaction_id"],
                        expected_epoch=commit["epoch"],
                        trim_version=commit["trim_version"],
                        system_digest=commit["system_digest"],
                        covered_seq=commit["covered_seq"],
                        source_ids=commit.get("source_ids"),
                        reply_id=reply["id"],
                        turns=commit.get("turns", 1),
                        context_digests=commit.get("context_digests"),
                        seen_nearby=commit.get("seen_nearby"),
                    )
                )
            elif commit:
                await asyncio.shield(service.sessions.invalidate_chains(message))
        elif commit:
            await asyncio.shield(service.sessions.invalidate_chains(message))
    finished = await store.finish_reply(reply["id"], status)
    if finished:
        await service._finish_delivery_audit(reply, status, detail)
    else:
        await service._finish_delivery_audit(reply, "cancelled", "session_reset")
    return True


async def _record_proactive_side_effects(service, message: Message) -> None:
    if hasattr(service.store.runtime.filters, "note_proactive_spoke"):
        service.store.runtime.filters.note_proactive_spoke(message)
    world_state = getattr(service, "world_state", None)
    if world_state is not None:
        now_ts = world_state.clock() if hasattr(world_state, "clock") else time.time()
        if message.kind == "group":
            await world_state.record_group_proactive(
                service.store.session_scope(message), now=now_ts
            )
        await world_state.update_state(last_proactive_at=now_ts)


async def deliver_proactive_jit(
    service,
    message: Message,
    raw_action_or_text: str,
    *,
    reply_id: str,
    intent_id: str | None = None,
    still_allowed: Callable[[], bool] = lambda: True,
    before_delivery: Callable[[], Awaitable[bool]] | None = None,
) -> dict:
    """Deliver JIT proactive bubbles sequentially with at-most-once ambiguity safety."""
    from .modules.qq_audience import allowed_proactive_message

    is_group = message.kind == "group"
    public_chat = is_group or not is_persona_owner(message, service.s)
    secrets = getattr(service, "secrets", None) or configured_secrets(service.s)
    raw = str(raw_action_or_text or "")

    abstain = False
    emoji_like: str | None = None
    poke_qq: str | None = None
    anchor_dict: dict = {"reply_id": None, "mentions": []}
    bubbles: list[str] = []
    voice_text: str | None = None

    voice_client = getattr(service, "voice_reply", None)
    voice_supported, _ = _resolve_platform_voice_capability(service, message)
    allow_voice_proactive = (
        voice_client is not None
        and voice_supported
        and voice_client.allowed_for_message(message)
        and voice_client.can_send_now(service.store.session_scope(message), explicit=False)[0]
    )

    if raw.lstrip().startswith("{"):
        labels = getattr(getattr(service, "memes", None), "labels", ())
        allowed_poke = {message.user_id} if message.kind == "private" else ()
        actions, _ = parse_actions(
            raw,
            public=public_chat,
            allowed_at=(),
            allowed_reply=(),
            labels=labels,
            settings=service.s,
            allowed_poke=allowed_poke,
            allow_longnick=False,
            allow_sticker=False,
            allow_voice=allow_voice_proactive,
        )
        abstain = bool(actions.abstain)
        emoji_like = actions.emoji_like
        poke_qq = actions.poke_qq
        voice_text = actions.voice_text if not abstain else None
        anchor_dict = {"reply_id": None, "mentions": list(actions.anchor.mentions)}
        if not abstain:
            for item in actions.bubbles[:3]:
                if not item.strip():
                    continue
                filtered = filter_reply_links(item, public=is_group).strip()
                if not filtered:
                    continue
                bubbles.append(
                    safe_output(safe_output(filtered, secrets, limit=218), secrets, limit=220)
                )
    else:
        filtered = filter_reply_links(raw, public=is_group).strip()
        if filtered:
            bubbles = [safe_output(safe_output(filtered, secrets, limit=218), secrets, limit=220)]

    reaction_target_id = (
        str(message.event_id) if re.fullmatch(r"-?[0-9]{1,30}", str(message.event_id)) else None
    )
    can_emoji = bool(
        emoji_like
        and reaction_target_id
        and message.platform == "onebot"
        and hasattr(service.sender, "send_emoji_like")
    )
    can_poke = bool(
        poke_qq
        and message.platform == "onebot"
        and message.kind == "private"
        and poke_qq == message.user_id
        and hasattr(service.sender, "send_poke")
    )

    if (abstain or (not bubbles and not voice_text)) and not can_emoji and not can_poke:
        return {"status": "abstained", "bubbles_sent": 0}

    identity = getattr(service, "identity", None)

    def permitted() -> bool:
        return (
            still_allowed()
            and allowed_proactive_message(message, service.s)
            and authorize(message, service.s)
            and (identity is None or not identity.is_muted(message.platform, message.user_id))
        )

    if not permitted():
        return {"status": "cancelled", "bubbles_sent": 0}

    epoch = (await service.sessions.snapshot(message))["epoch"]
    synthetic_reply = {
        "id": reply_id,
        "message": message.model_dump(),
        "participants": [message.model_dump()],
    }

    if intent_id is not None:
        # Only an entirely text-based dispatch can be recovered from bubble ACKs alone.
        expected = (
            (len(bubbles) + (1 if voice_text else 0)) if not (can_emoji or can_poke) else None
        )
        async with service.store.transaction():
            cur = await service.store.db.execute(
                "UPDATE scheduled_intents SET expected_bubbles=? WHERE id=? AND status='executing'",
                (expected, intent_id),
            )
            if cur.rowcount != 1:
                return {"status": "cancelled", "bubbles_sent": 0}

    confirmed: list[dict] = []
    position = 0
    dispatched = False
    did_reaction = False
    applied_done = False

    async def valid() -> bool:
        current_epoch = (await service.sessions.snapshot(message))["epoch"]
        return (
            current_epoch == epoch
            and permitted()
            and (before_delivery is None or await before_delivery())
        )

    async def targets() -> set[str]:
        return set()

    async def apply_confirmed() -> None:
        nonlocal applied_done
        if confirmed and not applied_done:
            applied_done = True
            await asyncio.shield(apply_receipts(service, reply_id))
            await asyncio.shield(_record_proactive_side_effects(service, message))

    async def before_bubble_send() -> bool:
        nonlocal dispatched
        if not await valid():
            return False
        dispatched = True
        return True

    try:
        for index, text in enumerate(bubbles[:3]):
            if index:
                if hasattr(service.sender, "typing"):
                    await service.sender.typing(message)
                await asyncio.sleep(
                    _pacing_delay(
                        service,
                        min(2.2, 0.8 + len(text) * 0.025 + random.uniform(0, 0.2)),
                    )
                )
            if not await valid():
                if not confirmed:
                    return {"status": "cancelled", "bubbles_sent": 0}
                await apply_confirmed()
                return {"status": "uncertain", "bubbles_sent": len(confirmed)}

            receipt = await _send_text_bubble(
                service,
                message,
                text,
                anchor=anchor_dict,
                include_anchor=not confirmed,
                targets=targets,
                before_send=before_bubble_send,
                apply_output=False,
            )
            dispatched = True
            if not valid_receipt(receipt, platform=message.platform):
                raise Rejected("onebot_send_unconfirmed")
            data = await persist_ack(service, synthetic_reply, position, text, receipt, epoch)
            confirmed.append(data)
            position += 1

        if voice_text:

            async def ack_voice(ack_text: str, ack_receipt: dict) -> None:
                nonlocal position, dispatched
                dispatched = True
                if not valid_receipt(ack_receipt, platform=message.platform):
                    raise Rejected("onebot_send_unconfirmed")
                data = await persist_ack(
                    service, synthetic_reply, position, ack_text, ack_receipt, epoch
                )
                confirmed.append(data)
                position += 1

            await _deliver_voice_or_fallback(
                service,
                message,
                voice_text,
                anchor=anchor_dict,
                include_anchor=not confirmed,
                targets=targets,
                valid=valid,
                acknowledge=ack_voice,
                before_dispatch=before_bubble_send,
                proactive=True,
                explicit_voice=False,
                delay_before=bool(confirmed),
                apply_output=False,
            )

        if can_emoji:
            try:
                if await service.sender.send_emoji_like(
                    message, reaction_target_id, emoji_like, before_send=valid
                ):
                    did_reaction = True
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        if can_poke:
            try:
                if await service.sender.send_poke(message, poke_qq, before_send=valid):
                    did_reaction = True
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

        if confirmed:
            await apply_confirmed()
            return {
                "status": "sent",
                "bubbles_sent": len(confirmed),
                "reply_id": reply_id,
                "message_ids": [d["message_id"] for d in confirmed],
            }
        if did_reaction:
            await _record_proactive_side_effects(service, message)
            return {
                "status": "sent",
                "bubbles_sent": 0,
                "reaction_only": True,
                "reply_id": reply_id,
            }
        return {"status": "abstained", "bubbles_sent": 0}
    except asyncio.CancelledError:
        if confirmed:
            await apply_confirmed()
        raise
    except Rejected as exc:
        if str(exc) == "onebot_not_dispatched" and not confirmed:
            return {"status": "not_dispatched", "bubbles_sent": 0}
        if str(exc) == "reply_invalidated" and not confirmed and not dispatched:
            return {"status": "cancelled", "bubbles_sent": 0}
        if confirmed:
            await apply_confirmed()
        return {"status": "uncertain", "bubbles_sent": len(confirmed)}
    except Exception:
        if confirmed:
            await apply_confirmed()
        return {"status": "uncertain", "bubbles_sent": len(confirmed)}
