"""ACK-only multi-bubble delivery, with conservative ambiguity and recall semantics."""

import asyncio
import random
import time

from .models import Message
from .outbound_actions import bubble_segments, valid_receipt
from .security import Rejected


async def persist_ack(service, reply, position, text, receipt, epoch, digest=None):
    if not valid_receipt(receipt):
        raise Rejected("onebot_send_unconfirmed")
    data = {
        "message": reply["message"],
        "epoch": epoch,
        "text": text,
        "message_id": str(receipt["message_id"]),
        "digest": digest,
        "participants": reply.get("participants") or [reply["message"]],
    }
    async with service.store.transaction():
        await service.store.db.execute(
            "INSERT OR IGNORE INTO outbox_bubble_receipts VALUES (?,?,?,?,0)",
            (
                reply["id"],
                position,
                service.store.vault.seal(f"ack:{reply['id']}:{position}", data),
                time.time(),
            ),
        )
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
        for item in data:
            receipt = {"message_id": item["message_id"]}
            await service.sessions.receive_outgoing(
                message,
                item["message_id"],
                item["text"],
                expected_epoch=epoch,
                on_confirmed=lambda receipt=receipt: service.store.runtime.attention.record_sent(
                    message, receipt
                ),
            )
        service.store.runtime.filters.spoke(message)
        if message.kind == "group":
            members = [
                Message.model_validate(m) for m in data[0].get("participants", [data[0]["message"]])
            ]
            await service.public_memory.record(
                reply_id, message, members, "\n".join(d["text"] for d in data), expected_epoch=epoch
            )
    # Idempotent B receipts/public index make crashes before this marker safe to recover.
    async with service.store.transaction():
        await service.store.db.execute(
            "UPDATE outbox_bubble_receipts SET applied=1 WHERE reply_id=?", (reply_id,)
        )
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
    status, confirmed, position = "sent", [], 0
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
        if not service.batch_authorized(message, members):
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
        if not await valid():
            raise Rejected("reply_invalidated")
        await service.bot_guard.extend_cooldown(message, reply["id"], before_send=True)
        label = actions.get("sticker", {}).get("label")
        if actions.get("public") and message.platform == "onebot":
            if await service.memes.reserve_reply(store.session_scope(message), reply["id"], label):
                sticker_choice = await service.memes.choose(label, store.session_scope(message))
        if actions["sticker"].get("timing") == "before_text":
            await sticker()
            if sticker_choice:
                await asyncio.sleep(0.6)
        anchor = dict(actions.get("anchor", {}))
        if message.kind != "group":
            anchor = {}
        else:
            drift = service.hot.intervening(
                message, actions.get("watermark", 0), {m.user_id for m in members}
            )
            if drift == 0:
                anchor["reply_id"] = None
            elif drift >= 3:
                anchor["reply_id"] = next(
                    (m.event_id for m in reversed(members) if m.event_id.lstrip("-").isdigit()),
                    None,
                )
        folded = actions.get("folded") if message.kind == "group" else None
        if folded:
            receipt = await service.sender.send_folded(message, folded, before_send=valid)
            await acknowledge(folded, receipt)
        else:
            for index, text in enumerate(actions.get("bubbles", [])[:3]):
                if not text.strip():
                    continue
                if index:
                    if hasattr(service.sender, "typing"):
                        await service.sender.typing(message)
                    await asyncio.sleep(min(2.2, 0.8 + len(text) * 0.025 + random.uniform(0, 0.2)))
                if not await valid():
                    raise Rejected("reply_invalidated")
                if message.platform == "onebot" and hasattr(service.sender, "send_action_bubble"):
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
            await asyncio.sleep(0.6)
            await sticker()
        if not confirmed:
            status = "cancelled"
    except asyncio.CancelledError:
        # ACKs already journalled; restart applies them once. Unknown sends stay uncertain.
        raise
    except Rejected as exc:
        status = "cancelled" if str(exc) == "reply_invalidated" else "uncertain"
    except Exception:
        status = "uncertain"
        service.record("delivery_uncertain")
    finally:
        applied = await asyncio.shield(apply_receipts(service, reply["id"]))
        if applied:
            service.store.runtime.filters.spoke(message)
            service.compressor.schedule(message)
            await service.bot_guard.extend_cooldown(message, reply["id"])
    await store.finish_reply(reply["id"], status)
    return True
