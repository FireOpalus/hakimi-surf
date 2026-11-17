import asyncio
import base64
import binascii
import random
import re
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime

import httpx

from .attention import direct_attention
from .bilibili import bounded_json
from .config import Settings
from .mentions import revalidate_segments
from .models import Message
from .onebot_transport import HTTPTransport, OneBotTransport
from .outbound_actions import ALLOWED_EMOJI_IDS
from .rich_message_parser import parse_file_ref, parse_message
from .security import Rejected, authorize, check_fresh, clean_text, configured_secrets, safe_output


def identifier(value, *, numeric: bool = False) -> str:
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise Rejected("invalid_identifier")
    text = str(value)
    pattern = r"[0-9]{1,20}" if numeric else r"[A-Za-z0-9_.=+\-]{1,256}"
    if not re.fullmatch(pattern, text) or text in {".", ".."}:
        raise Rejected("invalid_identifier")
    return text


def onebot_message(payload: dict, settings: Settings) -> Message | None:
    if payload.get("post_type") != "message" or payload.get("message_type") not in {
        "group",
        "private",
    }:
        return None
    bot = identifier(payload.get("self_id"), numeric=True)
    if bot != settings.onebot_self_id:
        raise Rejected("wrong_bot")
    user = identifier(payload.get("user_id"), numeric=True)
    # v11 message_id is a signed int in some implementations.
    event_id = identifier(payload.get("message_id"))
    kind = payload["message_type"]
    conv = identifier(payload.get("group_id"), numeric=True) if kind == "group" else user
    created = float(payload["time"])
    check_fresh(created, settings.signature_max_age)
    rich = parse_message(payload.get("message"), bot, max_chars=settings.max_message_chars)
    if not rich.text:
        return None
    reason = direct_attention(
        kind,
        mentioned=rich.addressed,
        text=rich.command_text,
        has_video=bool(rich.video_ids or rich.short_urls),
    )
    return Message(
        platform="onebot",
        bot_id=bot,
        event_id=event_id,
        user_id=user,
        kind=kind,
        conversation_id=conv,
        text=rich.text,
        command_text=rich.command_text,
        mentioned_ids=rich.mentioned_ids,
        reply_ids=rich.reply_ids,
        attention_reason=reason,
        addressed=bool(reason),
        images=rich.images,
        files=rich.files,
        video_ids=rich.video_ids,
        short_urls=rich.short_urls,
        forward_ids=rich.forward_ids,
        created_at=created,
        group_name=clean_text(str(payload.get("group_name", "")))[:100] if kind == "group" else "",
        sender_nickname=clean_text(str((payload.get("sender") or {}).get("nickname", "")))[:80]
        if isinstance(payload.get("sender"), dict)
        else "",
    )


def qq_message(payload: dict, settings: Settings) -> Message | None:
    if payload.get("op") != 0 or payload.get("t") not in {
        "C2C_MESSAGE_CREATE",
        "GROUP_AT_MESSAGE_CREATE",
    }:
        return None
    data = payload["d"]
    if not isinstance(data, dict) or data.get("message_type", 0) != 0:
        return None
    author = data["author"]
    if author.get("bot"):
        return None
    kind = "private" if payload["t"] == "C2C_MESSAGE_CREATE" else "group"
    user = identifier(
        author.get("user_openid" if kind == "private" else "member_openid") or author.get("id")
    )
    event_id = identifier(data["id"])
    conv = identifier(data["group_openid"]) if kind == "group" else user
    date = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
    if date.tzinfo is None:
        raise Rejected("missing_timezone")
    created = date.timestamp()
    check_fresh(created, settings.signature_max_age)
    text = clean_text(data.get("content", ""))
    if not text or len(text) > settings.max_message_chars:
        return None
    # Only directly addressed group events are subscribed; ordinary group events are ignored.
    return Message(
        platform="qq",
        bot_id=settings.qq_app_id,
        event_id=event_id,
        user_id=user,
        kind=kind,
        conversation_id=conv,
        text=text,
        addressed=True,
        created_at=created,
    )


_FAREWELL_BYPASS: ContextVar[tuple[str, str] | None] = ContextVar("farewell_bypass", default=None)


@contextmanager
def farewell_send_bypass(message: Message):
    """Permit one plain-text farewell delivery to a just-blacklisted user in this task."""
    token = _FAREWELL_BYPASS.set((message.platform, message.user_id))
    try:
        yield
    finally:
        _FAREWELL_BYPASS.reset(token)


class Sender:
    def __init__(
        self, settings: Settings, client: httpx.AsyncClient, onebot: OneBotTransport | None = None
    ):
        self.s, self.http = settings, client
        self.onebot = onebot or HTTPTransport(settings, client)
        self.send_lock = asyncio.Lock()
        self.token = ""
        self.token_expires = 0.0
        self.token_lock = asyncio.Lock()

    def _authorized(self, message: Message, *, proactive: bool = False) -> bool:
        if authorize(message, self.s):
            return True
        if (
            not proactive
            and _FAREWELL_BYPASS.get() == (message.platform, message.user_id)
            and message.user_id not in getattr(self.s, "bot_qq_ids", set())
            and message.user_id in getattr(self.s, "muted_user_ids", set())
        ):
            cfg = self.s.model_copy(
                update={"muted_user_ids": set(self.s.muted_user_ids) - {message.user_id}}
            )
            return authorize(message, cfg)
        return False

    @property
    def qq_origin(self) -> str:
        return "https://api.bot.qq.com"

    async def _qq_token(self) -> str:
        async with self.token_lock:
            if self.token and time.monotonic() < self.token_expires:
                return self.token
            data = await bounded_json(
                self.http,
                "POST",
                "https://api.bot.qq.com/app/getAppAccessToken",
                max_bytes=16384,
                json={
                    "appId": self.s.qq_app_id,
                    "clientSecret": self.s.qq_app_secret.get_secret_value(),
                },
            )
            if data.get("code", 0) != 0 or not isinstance(data.get("access_token"), str):
                raise Rejected("qq_token_failed")
            expires = int(data.get("expires_in", 0))
            if not 60 <= expires <= 86400:
                raise Rejected("qq_token_failed")
            self.token, self.token_expires = data["access_token"], time.monotonic() + expires - 30
            return self.token

    async def send(
        self, message: Message, text: str, proactive: bool = False, *, before_send=None
    ) -> dict | None:
        async with self.send_lock:
            await asyncio.sleep(random.uniform(self.s.send_delay_min, self.s.send_delay_max))
            if before_send is not None and not await before_send():
                raise Rejected("reply_invalidated")
            return await self._send_once(message, text, proactive, before_send=before_send)

    async def send_segments(self, message, segments, *, targets, before_send=None):
        if message.platform != "onebot" or message.kind != "group":
            raise Rejected("mention_group_only")
        async with self.send_lock:
            await asyncio.sleep(random.uniform(self.s.send_delay_min, self.s.send_delay_max))
            if before_send is not None and not await before_send():
                raise Rejected("reply_invalidated")
            allowed = await targets()
            if (
                not self._authorized(message)
                or time.time() - message.created_at > self.s.event_ttl_seconds
            ):
                raise Rejected("reply_invalidated")
            parts = revalidate_segments(segments, message, self.s, allowed_at_qqs=allowed)
            for part in parts:
                if part["type"] == "text" and part["data"]["text"].strip():
                    part["data"]["text"] = safe_output(
                        part["data"]["text"],
                        configured_secrets(self.s),
                        limit=self.s.send_max_chars,
                    )
            if not any(
                p["type"] == "at" or p.get("data", {}).get("text", "").strip() for p in parts
            ):
                raise Rejected("empty_reply")
            return await self.onebot.call(
                "send_group_msg",
                {"group_id": int(message.conversation_id), "auto_escape": True, "message": parts},
            )

    async def quick_ack(self, message, text, *, before_send):
        # Best effort: never wait behind the global sender or connectivity for > 900 ms.
        async with asyncio.timeout(0.9):
            async with self.send_lock:
                if not await before_send():
                    return
                return await self._send_once(message, text, False, before_send=before_send)

    async def typing(self, message):
        if message.platform != "onebot" or message.kind != "private":
            return  # NapCat's input-status API is a private-peer action.
        from .onebot_transport import HTTPTransport, WebSocketTransport

        if not (
            isinstance(self.onebot, (HTTPTransport, WebSocketTransport))
            or getattr(self.onebot, "supports_typing", False)
        ):
            return
        try:
            async with asyncio.timeout(0.3):
                await self.onebot.call(
                    "set_input_status", {"user_id": int(message.user_id), "event_type": 1}
                )
        except Exception:
            pass

    async def send_action_bubble(self, message, segments, *, targets, before_send):
        async with self.send_lock:
            if not await before_send() or not self._authorized(message):
                raise Rejected("reply_invalidated")
            allowed = await targets()
            parts = revalidate_segments(segments, message, self.s, allowed_at_qqs=allowed)
            if message.kind == "group":
                reply = next(
                    (
                        p
                        for p in segments
                        if p.get("type") == "reply"
                        and re.fullmatch(r"-?[0-9]{1,30}", str(p.get("data", {}).get("id", "")))
                    ),
                    None,
                )
                if reply:
                    parts.insert(0, reply)
            for part in parts:
                if part["type"] == "text" and part["data"]["text"].strip():
                    part["data"]["text"] = safe_output(
                        part["data"]["text"], configured_secrets(self.s), limit=6000
                    )
            if not await before_send():
                raise Rejected("reply_invalidated")
            return await self.onebot.call(
                "send_group_msg" if message.kind == "group" else "send_private_msg",
                {
                    "group_id" if message.kind == "group" else "user_id": int(
                        message.conversation_id
                    ),
                    "auto_escape": True,
                    "message": parts,
                },
            )

    async def send_folded(self, message, text, *, before_send):
        if message.platform != "onebot" or message.kind != "group":
            raise Rejected("forward_group_only")
        text = safe_output(text, configured_secrets(self.s), limit=8000)
        async with self.send_lock:
            if not await before_send() or not self._authorized(message):
                raise Rejected("reply_invalidated")
            try:
                return await self.onebot.call(
                    "send_group_forward_msg",
                    {
                        "group_id": int(message.conversation_id),
                        "messages": [
                            {
                                "type": "node",
                                "data": {
                                    "user_id": int(self.s.onebot_self_id),
                                    "nickname": "吉米奈喵",
                                    "content": [{"type": "text", "data": {"text": text}}],
                                },
                            }
                        ],
                    },
                )
            except Rejected as exc:
                if str(exc) != "onebot_action_unsupported":
                    raise  # Unknown delivery can NEVER become an extra image send.
        from .text_card import render_card

        image = await asyncio.to_thread(render_card, text)
        return await self.send_meme(message, image, before_send=before_send, as_sticker=False)

    async def send_card(self, message, text, *, before_send=None):
        if message.platform != "onebot" or message.kind != "group":
            raise Rejected("card_group_only")
        text = safe_output(text, configured_secrets(self.s), limit=8000)
        if before_send is not None and not await before_send():
            raise Rejected("reply_invalidated")
        if not authorize(message, self.s):
            raise Rejected("reply_invalidated")
        from .text_card import render_card

        image = await asyncio.to_thread(render_card, text)
        return await self.send_meme(message, image, before_send=before_send, as_sticker=False)

    async def send_owner_dm(self, target_user_id: int, text: str, *, before_send=None):
        if (
            isinstance(target_user_id, bool)
            or not isinstance(target_user_id, int)
            or not self.s.owner_qq_id
            or target_user_id != int(self.s.owner_qq_id)
        ):
            raise Rejected("owner_dm_target_forbidden")
        message = Message(
            platform="onebot",
            bot_id=self.s.onebot_self_id,
            event_id="owner-proactive",
            user_id=self.s.owner_qq_id,
            conversation_id=self.s.owner_qq_id,
            kind="private",
            text="",
            addressed=True,
            created_at=time.time(),
        )
        return await self.send(message, text, proactive=True, before_send=before_send)

    async def send_meme(
        self, message: Message, data: str, *, before_send=None, as_sticker: bool = True
    ):
        """Only a local allowlist blob reaches this path; never accept an image URL or file path."""
        if (
            message.platform != "onebot"
            or not isinstance(data, str)
            or not data.startswith("base64://")
            or len(data) > 7 * 1024 * 1024
        ):
            raise Rejected("meme_delivery_forbidden")
        try:
            raw = base64.b64decode(data[9:], validate=True)
        except (ValueError, binascii.Error):
            raise Rejected("invalid_meme_image") from None
        valid_magic = (
            raw.startswith(b"\xff\xd8\xff")
            or raw.startswith(b"\x89PNG\r\n\x1a\n")
            or raw.startswith((b"GIF87a", b"GIF89a"))
        )
        if not raw or len(raw) > 5 * 1024 * 1024 or not valid_magic:
            raise Rejected("invalid_meme_image")
        async with self.send_lock:
            if before_send is not None and not await before_send():
                raise Rejected("reply_invalidated")
            if (
                not authorize(message, self.s)
                or time.time() - message.created_at > self.s.event_ttl_seconds
            ):
                raise Rejected("reply_invalidated")
            action = "send_group_msg" if message.kind == "group" else "send_private_msg"
            field = "group_id" if message.kind == "group" else "user_id"
            seg_data: dict[str, str | int] = {"file": data}
            if as_sticker:
                seg_data["summary"] = "[动画表情]"
                seg_data["sub_type"] = 1
            receipt = await self.onebot.call(
                action,
                {
                    field: int(message.conversation_id),
                    "message": [{"type": "image", "data": seg_data}],
                },
            )
            if (
                not isinstance(receipt, dict)
                or isinstance(receipt.get("message_id"), bool)
                or not re.fullmatch(r"-?[0-9]{1,30}", str(receipt.get("message_id", "")))
            ):
                raise Rejected("onebot_send_unconfirmed")
            return receipt

    async def send_emoji_like(
        self, message: Message, target_message_id: str, emoji_id: str, *, before_send=None
    ) -> bool:
        if (
            message.platform != "onebot"
            or not re.fullmatch(r"-?[0-9]{1,30}", str(target_message_id))
            or emoji_id not in ALLOWED_EMOJI_IDS
        ):
            return False
        async with self.send_lock:
            if before_send is not None and not await before_send():
                raise Rejected("reply_invalidated")
            if (
                not authorize(message, self.s)
                or time.time() - message.created_at > self.s.event_ttl_seconds
            ):
                raise Rejected("reply_invalidated")
            await self.onebot.call(
                "set_msg_emoji_like",
                {"message_id": int(target_message_id), "emoji_id": str(emoji_id), "set": True},
            )
            return True

    async def send_poke(self, message: Message, target_qq: str, *, before_send=None) -> bool:
        target = str(target_qq or "").strip()
        if (
            message.platform != "onebot"
            or not re.fullmatch(r"[1-9][0-9]{0,19}", target)
            or target == self.s.onebot_self_id
        ):
            return False
        async with self.send_lock:
            if before_send is not None and not await before_send():
                raise Rejected("reply_invalidated")
            if (
                not authorize(message, self.s)
                or time.time() - message.created_at > self.s.event_ttl_seconds
            ):
                raise Rejected("reply_invalidated")
            if message.kind == "group":
                try:
                    await self.onebot.call(
                        "group_poke",
                        {"group_id": int(message.conversation_id), "user_id": int(target)},
                    )
                except Rejected as exc:
                    if str(exc) != "onebot_action_unsupported":
                        raise
                    await self.onebot.call(
                        "send_poke",
                        {"group_id": int(message.conversation_id), "user_id": int(target)},
                    )
            else:
                try:
                    await self.onebot.call("friend_poke", {"user_id": int(target)})
                except Rejected as exc:
                    if str(exc) != "onebot_action_unsupported":
                        raise
                    await self.onebot.call("send_poke", {"user_id": int(target)})
            return True

    async def forward(self, forward_id: str) -> dict:
        return await self.onebot.call("get_forward_msg", {"id": identifier(forward_id)})

    async def _send_once(
        self, message: Message, text: str, proactive: bool, *, before_send=None
    ) -> dict | None:
        # Recheck current configuration before every delivery, including restored outbox jobs.
        if not self._authorized(message, proactive=proactive):
            raise Rejected("delivery_not_authorized")
        if proactive:
            if (
                message.platform != "onebot"
                or message.kind != "private"
                or not self.s.owner_qq_id
                or not message.conversation_id.isdecimal()
                or int(message.conversation_id) != int(self.s.owner_qq_id)
                or message.user_id != self.s.owner_qq_id
            ):
                raise Rejected("proactive_not_authorized")
        elif time.time() - message.created_at > self.s.event_ttl_seconds:
            raise Rejected("reply_expired")
        if message.platform == "onebot":
            action = "send_private_msg" if message.kind == "private" else "send_group_msg"
            field = "user_id" if message.kind == "private" else "group_id"
            params = {
                field: int(message.conversation_id),
                "auto_escape": True,
                "message": [{"type": "text", "data": {"text": text}}],
            }
            if proactive and hasattr(self.onebot, "send_onebot_action_with_retry"):

                async def still_owner():
                    if before_send is not None and not await before_send():
                        return False
                    return (
                        bool(self.s.owner_qq_id)
                        and message.user_id == self.s.owner_qq_id
                        and int(message.conversation_id) == int(self.s.owner_qq_id)
                        and authorize(message, self.s)
                    )

                receipt = await self.onebot.send_onebot_action_with_retry(
                    action, params, before_send=still_owner
                )
            else:
                receipt = await self.onebot.call(action, params)
            if proactive and (
                isinstance(receipt.get("message_id"), bool)
                or not re.fullmatch(r"-?[0-9]{1,30}", str(receipt.get("message_id", "")))
            ):
                raise Rejected("onebot_send_unconfirmed")
            return receipt
        else:
            dest = identifier(message.conversation_id)
            route = "users" if message.kind == "private" else "groups"
            body = {"content": text, "msg_type": 0}
            if not proactive:
                body.update(msg_id=message.event_id, msg_seq=1)
            data = await bounded_json(
                self.http,
                "POST",
                f"{self.qq_origin}/v2/{route}/{dest}/messages",
                max_bytes=65536,
                headers={
                    "Authorization": "QQBot " + await self._qq_token(),
                    "X-Union-Appid": self.s.qq_app_id,
                },
                json=body,
            )
            if data.get("code", 0) != 0 or data.get("err_code", 0) != 0 or not data.get("id"):
                raise Rejected("qq_send_failed")


def notice_message(payload, settings):
    """Authenticated protocol notices become untrusted action descriptions, never commands."""
    if str(payload.get("self_id")) != settings.onebot_self_id:
        raise Rejected("wrong_bot")
    notice = payload.get("notice_type")
    subtype = payload.get("sub_type")
    recall = notice in {"group_recall", "friend_recall"}
    poke = notice == "notify" and subtype == "poke"
    reaction = notice in {"group_msg_emoji_like", "group_msg_emoji_like_update", "msg_emoji_like"}
    upload = notice in {"group_upload", "offline_file"}
    if not (recall or poke or reaction or upload):
        return None
    raw_actor = (
        payload.get("sender_id")
        if poke and payload.get("sender_id") is not None
        else payload.get("user_id", payload.get("operator_id"))
    )
    user = identifier(raw_actor, numeric=True)
    if user == settings.onebot_self_id:
        return None
    kind = "group" if payload.get("group_id") else "private"
    created = float(payload.get("time", time.time()))
    check_fresh(created, settings.signature_max_age)
    if upload:
        file_info = payload.get("file") if isinstance(payload.get("file"), dict) else None
        ref = parse_file_ref(file_info) if file_info else None
        if ref is None:
            return None
        conv = identifier(payload.get("group_id"), numeric=True) if kind == "group" else user
        event_id = f"notice:{notice}:{user}:{ref.identity[:48]}:{int(created)}"
        return Message(
            platform="onebot",
            bot_id=settings.onebot_self_id,
            event_id=event_id,
            user_id=user,
            kind=kind,
            conversation_id=conv,
            text=f"[文件：{ref.name or '未命名'}]",
            command_text="",
            addressed=(kind == "private"),
            attention_reason="private" if kind == "private" else "",
            files=[ref],
            created_at=created,
        )
    target = str(payload.get("target_id", ""))
    direct = target == settings.onebot_self_id
    if poke and kind == "private" and not direct:
        return None
    if reaction and kind == "private":
        return None
    ref = identifier(payload.get("message_id", "0"))
    text = (
        "[协议事件：撤回消息]"
        if recall
        else "[协议事件：戳了戳你]"
        if poke and direct
        else "[协议事件：戳了戳另一位群友]"
        if poke
        else "[协议事件：对消息贴了表情]"
    )
    is_addressed = direct if poke else (kind == "private" if recall else False)
    reason = (
        "private" if (is_addressed and kind == "private") else ("mention" if is_addressed else "")
    )
    sender_info = payload.get("sender")
    nickname = (
        clean_text(str(sender_info.get("nickname", "")))[:80]
        if isinstance(sender_info, dict)
        else ""
    )
    return Message(
        platform="onebot",
        bot_id=settings.onebot_self_id,
        event_id=f"notice:{notice}:{user}:{ref}:{created}:{target}",
        user_id=user,
        kind=kind,
        conversation_id=identifier(payload.get("group_id"), numeric=True)
        if kind == "group"
        else user,
        text=text,
        command_text="",
        addressed=is_addressed,
        attention_reason=reason,
        reply_ids=[ref] if reaction and ref != "0" else [],
        sender_nickname=nickname,
        created_at=created,
    )
