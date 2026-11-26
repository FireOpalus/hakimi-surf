"""Affection target attribution, farewell queuing, and outbound delivery mixin for `Service`."""

from __future__ import annotations

import asyncio
import random
import re
import time
from contextlib import nullcontext

from .bot_server import is_persona_owner
from .memes import explicit_meme_request
from .models import Message
from .modules.adapters import farewell_send_bypass
from .outgoing import (
    PRE_DISPATCH_REJECTED,
    is_affection_muted_only,
    safe_delivery_detail,
    send_module_chat,
)
from .security import Rejected
from .session_manager import session_epoch
from .social import chat_module


class _ServiceDeliveryMixin:
    """Mixin handling affection target classification, quick acks, and outbox delivery."""

    _ALLOWED_AFFECTION_REASONS = frozenset(
        {
            "attack_owner",
            "severe_offense",
            "boundary_or_hostility",
            "chat_negative",
            "chat_positive",
            "chat",
        }
    )

    def _classify_affection_reason(
        self, target: Message, delta: int, model_reason: str | None = None
    ) -> str:
        tag = model_reason.strip() if isinstance(model_reason, str) else ""
        if tag in self._ALLOWED_AFFECTION_REASONS:
            if delta <= -10 and tag in {
                "attack_owner",
                "severe_offense",
                "boundary_or_hostility",
                "chat_negative",
            }:
                return tag
            if -9 <= delta < 0 and tag == "chat_negative":
                return tag
            if delta > 0 and tag in {"chat_positive", "chat"}:
                return tag
        target_text = target.text or ""
        lowered = target_text.lower()
        owner_qq = str(getattr(self.s, "owner_qq_id", "") or "").strip()
        mentions_owner = (
            ("boiser" in lowered)
            or ("主人" in target_text)
            or (bool(owner_qq) and owner_qq in target_text)
        )
        if delta <= -10 and mentions_owner:
            return "attack_owner"
        if delta <= -20:
            return "severe_offense"
        if delta <= -10:
            return "boundary_or_hostility"
        if delta < 0:
            return "chat_negative"
        if delta > 0:
            return "chat_positive"
        return "chat"

    def _select_affection_target(
        self, message: Message, members: list[Message], actions
    ) -> tuple[Message, int, str]:
        target = message
        raw_delta = getattr(actions, "affection_delta", 0)
        delta = 0 if isinstance(raw_delta, bool) else int(raw_delta or 0)
        if len(members) > 1:
            if actions.anchor.reply_id:
                matched = next((m for m in members if m.event_id == actions.anchor.reply_id), None)
                if matched is not None:
                    target = matched
            elif actions.anchor.mentions:
                matched = next(
                    (m for m in members if m.user_id == actions.anchor.mentions[0]), None
                )
                if matched is not None:
                    target = matched
        if is_persona_owner(target, self.s) and (
            delta <= -10 or (delta < 0 and any(not is_persona_owner(m, self.s) for m in members))
        ):
            non_owners = [m for m in members if not is_persona_owner(m, self.s)]
            unique_non_owners = {m.user_id for m in non_owners}
            if len(unique_non_owners) == 1:
                target = non_owners[-1]
            else:
                delta = 0
        reason = self._classify_affection_reason(target, delta)
        return target, delta, reason

    def _select_affection_targets(
        self, message: Message, members: list[Message], actions
    ) -> list[tuple[Message, int, str]]:
        raw_deltas = getattr(actions, "affection_deltas", None)
        if raw_deltas:
            member_by_uid: dict[str, Message] = {}
            for m in members:
                if not self.bot_guard.known_bot(m):
                    member_by_uid[m.user_id] = m
            if not self.bot_guard.known_bot(message) and message.user_id not in member_by_uid:
                member_by_uid[message.user_id] = message
            has_non_owner = any(
                not is_persona_owner(m, self.s)
                for m in (members or [message])
                if not self.bot_guard.known_bot(m)
            )
            results: list[tuple[Message, int, str]] = []
            for item in raw_deltas:
                if isinstance(item, dict):
                    tid = str(item.get("target_id") or "").strip()
                    raw_d = item.get("delta", 0)
                    raw_reason = item.get("reason")
                else:
                    tid = str(getattr(item, "target_id", "") or "").strip()
                    raw_d = getattr(item, "delta", 0)
                    raw_reason = getattr(item, "reason", None)
                t = member_by_uid.get(tid)
                if t is None:
                    continue
                d = 0 if isinstance(raw_d, bool) else int(raw_d or 0)
                if d == 0:
                    continue
                if is_persona_owner(t, self.s) and d < 0 and has_non_owner:
                    continue
                results.append((t, d, self._classify_affection_reason(t, d, raw_reason)))
            if results:
                return results
        non_owner_uids = {
            m.user_id
            for m in members
            if not is_persona_owner(m, self.s) and not self.bot_guard.known_bot(m)
        }
        raw_single_delta = getattr(actions, "affection_delta", 0)
        single_delta = 0 if isinstance(raw_single_delta, bool) else int(raw_single_delta or 0)
        if (
            len(members) > 1
            and len(non_owner_uids) >= 2
            and not actions.anchor.reply_id
            and not actions.anchor.mentions
            and single_delta <= -10
        ):
            self.counters["affection_ambiguous_penalty_skipped"] += 1
            return []
        target, delta, reason = self._select_affection_target(message, members, actions)
        return [(target, delta, reason)]

    async def _alert_affection_blacklisted(self, message: Message, update) -> None:
        self.record("affection_blacklisted")
        proactive = getattr(self, "proactive", None)
        alert_fn = getattr(proactive, "alert", None) if proactive is not None else None
        if not callable(alert_fn):
            return
        nick = self.moderation.nickname(message.sender_nickname or getattr(update, "nickname", ""))
        code = f"affection_zero_{message.platform}_{message.user_id}"
        recovery = self.store.runtime.affection.config.unmute_recovery_score
        hint = (
            f"如需恢复互动，可私聊发送 /unmute {message.user_id}"
            f"（恢复至 {recovery} 分）或前往后台调整。"
            if message.platform == "onebot"
            else "如需恢复互动，请前往后台好感度面板调整分值。"
        )
        text = (
            f"BOISER，{nick}（{message.platform}:{message.user_id}）因「{update.reason}」"
            f"好感度已降至 0 分，哈基米已发告别语并在应用层静音（绝不删好友）。\n{hint}"
        )
        try:
            await alert_fn(code, text)
        except Exception:
            pass

    async def _enqueue_affection_farewell(
        self, job, message: Message, members: list[Message], session, watermark, audit_id, update
    ) -> None:
        epoch = (
            session["epoch"] if session is not None else await session_epoch(self.sessions, message)
        )
        aff = self.store.runtime.affection
        farewell = self.output(update.farewell_message or aff.config.farewell_message)
        public = not (message.kind == "private" and is_persona_owner(message, self.s))
        target_id = next(
            (m.event_id for m in reversed(members) if re.fullmatch(r"-?[0-9]{1,30}", m.event_id)),
            None,
        )
        await self.store.enqueue_reply(
            job["id"],
            message,
            farewell,
            session_epoch=epoch,
            participants=members,
            allowed_at_qqs=set(),
            audit_id=audit_id,
            actions={
                "abstain": False,
                "bubbles": [farewell],
                "anchor": {"reply_id": target_id, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "emoji_like": None,
                "poke_qq": None,
                "save_meme_label": None,
                "set_longnick": None,
                "affection_delta": update.delta,
                "folded": None,
                "watermark": watermark,
                "source_ids": [m.event_id for m in members],
                "public": public,
                "allow_text_card": False,
                "farewell_bypass_mute": True,
            },
            consumed_image_events=[],
            consumed_file_events=[],
        )
        if audit_id:
            await self.store.runtime.filters.update_audit(audit_id, "queued")

    async def quick_ack(self, job, message, guard):
        try:
            async with self.store.transaction():
                row = await self.store.one(
                    "SELECT at FROM local_quick_acks WHERE scope=?", (job["scope"],)
                )
                if row and time.time() - row[0] < 60:
                    return
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO local_quick_acks VALUES (?,?)",
                    (job["scope"], time.time()),
                )

            async def valid():
                try:
                    await guard()
                    active = self.store.groups.active.get(job["scope"])
                    return active is not None and not active.cancel.is_set()
                except Rejected:
                    return False

            phrases = (
                (
                    "等我一下下，我仔细看看~",
                    "稍等哦，我帮你理一下~",
                    "唔，这个问题我想仔细点，等我几秒~",
                )
                if message.kind == "private" and is_persona_owner(message, self.s)
                else ("稍等我捋一下", "等下，我看眼细节", "让我想想怎么讲", "稍等，我看下上下文")
            )
            receipt = await self.sender.quick_ack(
                message, random.choice(phrases), before_send=valid
            )
            if receipt:
                await self.store.runtime.attention.record_sent(message, receipt)
        except Exception:
            self.record("quick_ack_unavailable")

    async def _finish_delivery_audit(self, reply: dict, status: str, detail: str = ""):
        audit_id = reply.get("audit_id")
        if audit_id:
            await self.store.runtime.filters.update_audit(audit_id, status, detail)

    async def deliver_once(self) -> bool:
        reply = await self.store.claim_reply()
        if not reply:
            return False
        async with (
            self.world_state.experience.chatting() if not reply["proactive"] else nullcontext()
        ):
            return await self._deliver_reply(reply)

    async def _deliver_reply(self, reply) -> bool:
        reply_actions = reply.get("actions") or {}
        message = Message.model_validate(reply["message"])
        farewell_bypass = bool(reply_actions.get("farewell_bypass_mute")) and (
            await is_affection_muted_only(self, message)
        )
        raw_bypass_ids = {
            str(u).strip()
            for u in (reply_actions.get("farewell_bypass_user_ids") or [])
            if isinstance(u, str) and str(u).strip()
        }
        if reply_actions.get("farewell_bypass_mute"):
            raw_bypass_ids.add(message.user_id)
        if reply_actions and not (
            bool(reply_actions.get("farewell_bypass_mute"))
            and "bubbles" not in reply_actions
            and reply.get("text")
        ):
            from .outgoing import deliver_actions

            return await deliver_actions(self, reply)
        status = "sent"
        detail = ""
        text_confirmed = False
        image_confirmed = False
        participants = [Message.model_validate(m) for m in reply.get("participants", [])]
        for original in participants or [message]:
            if self.bot_guard.known_bot(original):
                await self.bot_guard.prepare(original)
        delivery_epoch = await session_epoch(self.sessions, message)
        if "session_epoch" in reply and delivery_epoch != reply["session_epoch"]:
            await self.store.finish_reply(reply["id"], "cancelled")
            await self._finish_delivery_audit(reply, "cancelled", "epoch_changed")
            return True
        if not await self.bot_guard.may_process(message, reply["id"]):
            await self.store.finish_reply(reply["id"], "blocked")
            await self._finish_delivery_audit(reply, "blocked", "bot_cooldown")
            return True
        try:
            with farewell_send_bypass(message) if farewell_bypass else nullcontext():
                await self.bot_guard.extend_cooldown(message, reply["id"], before_send=True)

                async def valid_after_delay():
                    if not chat_module(self.s, message.platform):
                        return False
                    async with self.store.lock:
                        row = await self.store.one(
                            "SELECT status FROM outbox WHERE id=?", (reply["id"],)
                        )
                    if not row or row[0] != "sending":
                        return False
                    if (
                        not reply["proactive"]
                        and time.time() - message.created_at > self.s.event_ttl_seconds
                    ):
                        return False
                    if any(
                        self.hot.recalled(m)
                        or any(self.hot.recalled(m, ref) for ref in m.reply_ids)
                        for m in participants or [message]
                    ):
                        return False
                    if not await self.bot_guard.may_process(message, reply["id"]):
                        return False
                    bypass_uids: set[str] = set()
                    if raw_bypass_ids:
                        for candidate in [message, *(participants or [message])]:
                            if (
                                candidate.user_id in raw_bypass_ids
                                and candidate.user_id not in bypass_uids
                                and await is_affection_muted_only(self, candidate)
                            ):
                                bypass_uids.add(candidate.user_id)
                    return (
                        await session_epoch(self.sessions, message) == delivery_epoch
                    ) and self.batch_authorized(
                        message,
                        participants or [message],
                        bypass_mute_user_ids=bypass_uids,
                    )

                receipt = None
                image_confirmed = False

                async def current_at_targets():
                    targets = set(reply.get("allowed_at_qqs", []))
                    for target in list(targets & self.s.bot_qq_ids):
                        if not await self.bot_guard.allowed_reply_target(message, target):
                            targets.discard(target)
                    return targets - {
                        self.s.onebot_self_id,
                        "all",
                        "0",
                        *getattr(self.s, "muted_user_ids", set()),
                    }

                if message.platform not in {"onebot", "qq"}:
                    module = chat_module(self.s, message.platform)
                    if (
                        reply["proactive"]
                        or not module
                        or not reply["text"]
                        or not await valid_after_delay()
                    ):
                        await self.store.finish_reply(reply["id"], "cancelled")
                        await self._finish_delivery_audit(reply, "cancelled", "unauthorized")
                        return True
                    reply_to = (
                        message.event_id
                        if message.kind == "group"
                        and isinstance(message.event_id, str)
                        and re.fullmatch(r"-?[0-9]{1,30}", message.event_id)
                        else None
                    )
                    receipt = await send_module_chat(
                        module,
                        message,
                        self.output(reply["text"]),
                        reply_to=reply_to,
                        before_send=valid_after_delay,
                    )
                elif (
                    reply.get("segments")
                    and any(s["type"] == "at" for s in reply["segments"])
                    and hasattr(self.sender, "send_segments")
                ):
                    receipt = await self.sender.send_segments(
                        message,
                        reply["segments"],
                        targets=current_at_targets,
                        before_send=valid_after_delay,
                    )
                elif reply["text"]:
                    receipt = await self.sender.send(
                        message,
                        self.output(reply["text"]),
                        reply["proactive"],
                        before_send=valid_after_delay,
                    )
            if receipt is not None or reply["text"]:
                from .outbound_actions import valid_receipt

                if not valid_receipt(receipt, platform=message.platform):
                    raise Rejected("onebot_send_unconfirmed")
                text_confirmed = True
            if text_confirmed:
                retained_text = reply["text"]
                if not reply["proactive"] and self.world_state.experience.ready:
                    async with self.store.transaction():
                        await self.world_state.experience.confirmed_tx(
                            message, reply["id"], self.world_state.clock(), [retained_text]
                        )
                await self.sessions.reply(
                    message, retained_text, reply["id"], expected_epoch=delivery_epoch
                )

                async def remember_sent():
                    if await self.store.runtime.attention.record_sent(message, receipt):
                        await self.store.runtime.filters.remember_group(
                            message.model_copy(
                                update={
                                    "event_id": "sent:" + str(receipt["message_id"]),
                                    "user_id": message.bot_id,
                                    "text": "[哈基米已回复] " + self.output(retained_text),
                                    "created_at": time.time(),
                                }
                            )
                        )

                retained = await self.sessions.receive_outgoing(
                    message,
                    receipt["message_id"],
                    retained_text,
                    expected_epoch=delivery_epoch,
                    on_confirmed=remember_sent,
                )
                if not retained:
                    await self.store.finish_reply(reply["id"], "cancelled")
                    await self._finish_delivery_audit(reply, "cancelled", "epoch_changed")
                    return True
            if reply.get("meme_eligible") and not reply["proactive"] and await valid_after_delay():
                force_meme = explicit_meme_request(*(m.text for m in participants), message.text)
                allowed = await self.memes.reserve_reply(
                    self.store.session_scope(message),
                    reply["id"],
                    reply.get("meme_tag"),
                    force=force_meme,
                )
                if allowed:
                    try:
                        await self._sleep(0.6)
                        data = await self.memes.get_base64(reply["meme_tag"])

                        async def image_still_allowed():
                            return (
                                await valid_after_delay()
                                and await self.memes.get_base64(reply["meme_tag"]) == data
                            )

                        if data and await image_still_allowed():
                            image_receipt = await self.sender.send_meme(
                                message, data, before_send=image_still_allowed
                            )
                            image_confirmed = True  # Sender requires a valid OneBot message_id.
                            if not text_confirmed and self.world_state.experience.ready:
                                async with self.store.transaction():
                                    await self.world_state.experience.spend_chat_tx(
                                        reply["id"], self.world_state.clock()
                                    )
                            await self.sessions.receive_outgoing(
                                message,
                                image_receipt["message_id"],
                                "[本地表情：" + reply["meme_tag"] + "]",
                                expected_epoch=delivery_epoch,
                                on_confirmed=lambda: self.store.runtime.attention.record_sent(
                                    message, image_receipt
                                ),
                            )
                            if image_confirmed and not reply["text"]:
                                await self.sessions.reply(
                                    message,
                                    "[本地表情：" + reply["meme_tag"] + "]",
                                    reply["id"],
                                    expected_epoch=delivery_epoch,
                                )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.record("meme_send_failed")
            if not text_confirmed and not image_confirmed:
                status = "cancelled"
        except asyncio.CancelledError:
            # Leave 'sending': startup recovery will mark it uncertain without resending.
            raise
        except Rejected as exc:
            code = str(exc).strip()
            if code == "reply_invalidated":
                status, detail = "cancelled", "reply_invalidated"
            elif code in PRE_DISPATCH_REJECTED and not text_confirmed and not image_confirmed:
                status, detail = "failed", code
            else:
                status, detail = "uncertain", safe_delivery_detail(exc)
                self.record("delivery_uncertain")
        except Exception:
            status, detail = "uncertain", "delivery_uncertain"
            self.record("delivery_uncertain")
        await self.bot_guard.extend_cooldown(message, reply["id"])
        if not await self.store.finish_reply(reply["id"], status):
            await self._finish_delivery_audit(reply, "cancelled", "session_reset")
            return True  # A reset cancelled this delivery while the transport awaited its ACK.
        await self._finish_delivery_audit(reply, status, detail)
        if (
            status == "sent"
            and text_confirmed
            and not reply["proactive"]
            and message.kind == "group"
        ):
            await self.public_memory.record(
                reply["id"],
                message,
                participants or [message],
                reply["text"],
                expected_epoch=delivery_epoch,
            )
        if status == "sent" and text_confirmed and not reply["proactive"] and self.diary:
            await self.diary.bot_reply(reply["id"], message, reply["text"])
        if status == "sent" and not reply["proactive"] and "session_epoch" in reply:
            if reply.get("consumed_image_events"):
                await self.store.groups.mark_images_consumed(
                    message, reply["consumed_image_events"]
                )
            if reply.get("consumed_file_events"):
                await self.store.groups.mark_files_consumed(message, reply["consumed_file_events"])
            await self.proactive.note_activity(message)
            self.store.runtime.filters.spoke(message)
            self.compressor.schedule(message)
        return True

    async def delivery(self):
        while True:
            try:
                sent = await self.deliver_once()
                await self._sleep(2 if sent else 0.25)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.record("delivery_worker_error")
                await self._sleep(2)
