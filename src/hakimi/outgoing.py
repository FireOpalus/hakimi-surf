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
from .text_card import should_render_rich_card


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
    farewell_bypass_mute = bool(actions.get("farewell_bypass_mute")) and (
        message.platform != "onebot" or await service.moderation.is_affection_muted(message.user_id)
    )
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
        bypass_uid = None
        if farewell_bypass_mute and (
            message.platform != "onebot"
            or await service.moderation.is_affection_muted(message.user_id)
        ):
            bypass_uid = message.user_id
        if not service.batch_authorized(message, members, bypass_mute_user_id=bypass_uid):
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
                if message.platform == "onebot":
                    force_sticker = (not has_text_bubbles and bool(label)) or explicit_meme_request(
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
                    if sticker_choice and has_text_bubbles:
                        await asyncio.sleep(0.6)
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
                    for index, text in enumerate(actions.get("bubbles", [])[:3]):
                        if not text.strip():
                            continue
                        if index:
                            if hasattr(service.sender, "typing"):
                                await service.sender.typing(message)
                            await asyncio.sleep(
                                min(2.2, 0.8 + len(text) * 0.025 + random.uniform(0, 0.2))
                            )
                        if not await valid():
                            raise Rejected("reply_invalidated")
                        if message.platform == "onebot" and hasattr(
                            service.sender, "send_action_bubble"
                        ):
                            receipt = await service.sender.send_action_bubble(
                                message,
                                bubble_segments(text, anchor, include_anchor=index == 0),
                                targets=targets,
                                before_send=valid,
                            )
                        else:
                            receipt = await service.sender.send(
                                message, service.output(text), before_send=valid
                            )
                        await acknowledge(text, receipt)
                if actions["sticker"].get("timing") != "before_text" and sticker_choice:
                    if has_text_bubbles:
                        await asyncio.sleep(0.6)
                    await sticker()
                did_reaction = await _try_react_and_poke()
                if not confirmed and not did_reaction:
                    status, detail = "cancelled", "empty_reply"
    except asyncio.CancelledError:
        # ACKs already journalled; restart applies them once. Unknown sends stay uncertain.
        raise
    except Rejected as exc:
        if str(exc) == "reply_invalidated":
            status, detail = "cancelled", "reply_invalidated"
        else:
            status, detail = "uncertain", "delivery_uncertain"
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
        )
        abstain = bool(actions.abstain)
        emoji_like = actions.emoji_like
        poke_qq = actions.poke_qq
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

    if (abstain or not bubbles) and not can_emoji and not can_poke:
        return {"status": "abstained", "bubbles_sent": 0}

    def permitted() -> bool:
        return (
            still_allowed()
            and allowed_proactive_message(message, service.s)
            and authorize(message, service.s)
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
        expected = len(bubbles) if not (can_emoji or can_poke) else None
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

    try:
        for index, text in enumerate(bubbles[:3]):
            if index:
                if hasattr(service.sender, "typing"):
                    await service.sender.typing(message)
                await asyncio.sleep(min(2.2, 0.8 + len(text) * 0.025 + random.uniform(0, 0.2)))
            if not await valid():
                if not confirmed:
                    return {"status": "cancelled", "bubbles_sent": 0}
                await apply_confirmed()
                return {"status": "uncertain", "bubbles_sent": len(confirmed)}

            async def before_bubble_send() -> bool:
                nonlocal dispatched
                if not await valid():
                    return False
                dispatched = True
                return True

            if message.platform == "onebot" and hasattr(service.sender, "send_action_bubble"):
                receipt = await service.sender.send_action_bubble(
                    message,
                    bubble_segments(text, anchor_dict, include_anchor=index == 0),
                    targets=targets,
                    before_send=before_bubble_send,
                )
            else:
                receipt = await service.sender.send(
                    message,
                    text,
                    before_send=before_bubble_send,
                )
            dispatched = True
            if not valid_receipt(receipt, platform=message.platform):
                raise Rejected("onebot_send_unconfirmed")
            data = await persist_ack(service, synthetic_reply, position, text, receipt, epoch)
            confirmed.append(data)
            position += 1

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
