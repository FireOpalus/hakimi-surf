"""Expand admitted OneBot references before journaling, without crossing conversation scopes."""

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable

from .models import Message
from .rich_message_parser import RichMessage, parse_forward, parse_message
from .security import Rejected, clean_text

Lookup = Callable[[Message, str], Awaitable[Message | dict | None]]
log = logging.getLogger("hakimi")


def nickname(value) -> str:
    return re.sub(r"[\r\n\[\]]", " ", clean_text(str(value or "群友")))[:80]


def sender_id(value) -> str:
    value = str(value or "")
    return value if re.fullmatch(r"[1-9][0-9]{0,19}", value) else ""


def annotate_images(images, uid, name, origin):
    return [
        ref.model_copy(update={"sender_id": uid, "sender_nickname": name, "origin": origin})
        for ref in images
    ]


class MessageContextResolver:
    def __init__(self, settings, transport, lookup: Lookup | None = None):
        self.s, self.transport, self.lookup = settings, transport, lookup

    async def _rpc(self, action, params):
        if self.transport is None:
            raise Rejected("onebot_disconnected")
        async with asyncio.timeout(8):
            return await self.transport.call(action, params)

    def _same_scope(self, current: Message, found: Message) -> bool:
        return current.session_parts(self.s.namespace) == found.session_parts(self.s.namespace)

    def _remote_message(self, current, ref, value):
        if not isinstance(value, dict):
            raise Rejected("reply_invalid_response")
        # An adapter must supply a real peer/type: a message id alone grants no cross-chat access.
        if str(value.get("message_id", "")) != ref:
            raise Rejected("reply_scope_mismatch")
        kind = value.get("message_type")
        sender = value.get("sender") or {}
        if not isinstance(sender, dict):
            raise Rejected("reply_invalid_response")
        uid = sender_id(sender.get("user_id", value.get("user_id")))
        if kind != current.kind or not uid:
            raise Rejected("reply_scope_mismatch")
        if current.kind == "group":
            if str(value.get("group_id", "")) != current.conversation_id:
                raise Rejected("reply_scope_mismatch")
        elif uid != current.conversation_id:
            # A bot-authored private message additionally needs the real destination.
            target = str(value.get("target_id", value.get("peer_id", "")))
            if uid != current.bot_id or target != current.conversation_id:
                raise Rejected("reply_scope_mismatch")
        rich = parse_message(value.get("message", []), current.bot_id, max_chars=5000)
        name = nickname(sender.get("card") or sender.get("nickname"))
        return current.model_copy(
            update={
                "event_id": ref,
                "user_id": uid,
                "sender_nickname": name,
                "text": rich.text,
                "command_text": "",
                "reply_ids": rich.reply_ids,
                "images": rich.images,
                "video_ids": rich.video_ids,
                "short_urls": rich.short_urls,
                "forward_ids": rich.forward_ids,
            }
        )

    async def _reply(self, message, ref):
        found = await self.lookup(message, ref) if self.lookup is not None else None
        if isinstance(found, Message):
            if found.event_id != ref or not self._same_scope(message, found):
                raise Rejected("reply_scope_mismatch")
            return found
        if found is None:
            found = await self._rpc("get_msg", {"message_id": int(ref)})
        return self._remote_message(message, ref, found)

    async def resolve(self, message: Message) -> Message:
        if message.platform != "onebot" or message.context_expanded:
            return message
        text = message.text
        statuses = set(message.context_status)
        images = annotate_images(
            message.images, message.user_id, message.sender_nickname, "当前消息"
        )
        videos, shorts, forwards = list(message.video_ids), list(message.short_urls), []

        def append_text(block):
            nonlocal text
            available = 8000 - len(text) - 1
            if len(block) > max(0, available):
                statuses.add("truncated")
            if available > 0:
                text += "\n" + block[:available]

        def merge(rich):
            for target, values, cap, key in (
                (images, rich.images, 2, lambda x: x.identity),
                (videos, rich.video_ids, 3, lambda x: x),
                (shorts, rich.short_urls, 3, lambda x: x),
            ):
                for value in values:
                    if len(target) < cap and key(value) not in [key(old) for old in target]:
                        target.append(value)

        pending = [(ref, 0) for ref in message.reply_ids[:2]]
        seen = set()
        for ref, depth in pending:
            if ref in seen or len(seen) >= 4:
                continue
            seen.add(ref)
            try:
                original = await self._reply(message, ref)
                if depth < 1:
                    pending.extend((nested, depth + 1) for nested in original.reply_ids[:2])
                name = nickname(original.sender_nickname)
                author = (
                    "我自己(吉米奈喵)"
                    if original.user_id == message.bot_id
                    else f"{name}(QQ:{original.user_id})"
                )
                append_text(f'[引用 {author} 的消息: "{original.text}"]')
                merge(
                    RichMessage(
                        images=annotate_images(original.images, original.user_id, name, "引用消息"),
                        video_ids=original.video_ids,
                        short_urls=original.short_urls,
                    )
                )
                forwards.extend(original.forward_ids)
            except (Rejected, TimeoutError, OSError, ValueError, TypeError):
                log.warning("event=message_context_unavailable kind=reply")
                statuses.add("reply_unavailable")
        forwards = list(dict.fromkeys([*message.forward_ids, *forwards]))[:2]
        for ref in forwards:
            try:
                value = await self._rpc("get_forward_msg", {"id": ref})
                rich = parse_forward(value, message.bot_id, include_authors=True)
                append_text(rich.text)
                merge(rich)
            except (Rejected, TimeoutError, OSError, ValueError, TypeError):
                log.warning("event=message_context_unavailable kind=forward")
                statuses.add("forward_unavailable")
        return message.model_copy(
            update={
                "text": text,
                "images": images[:2],
                "video_ids": videos[:3],
                "short_urls": shorts[:3],
                "forward_ids": [],
                "context_expanded": True,
                "context_status": sorted(statuses),
            }
        )
