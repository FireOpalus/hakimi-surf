"""Standalone hot-plug Telegram social chat plugin."""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
import mimetypes
import os
import re
import signal
import time
from collections import OrderedDict
from pathlib import PurePosixPath
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx

from hakimi_plugin_sdk import (
    BlobResult,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
)
from hakimi_plugin_sdk.bili_common import extract_url_bvid

from .tg_api import DEFAULT_TG_API_BASE, TelegramApiError, TelegramBotApi

log = logging.getLogger("hakimi_plugin_telegram")

_MAX_SEEN_MEDIA_IDS = 512
_MAX_FETCH_BYTES = 20 * 1024 * 1024
_MAX_PHOTO_FILE_BYTES = 20 * 1024 * 1024
_MAX_TEXT_CHARS = 24000
_MAX_QUOTED_TEXT_CHARS = 2000
_MAX_INBOUND_IMAGES = 2
_MAX_INBOUND_FILES = 2
_MAX_INBOUND_AUDIOS = 4
_AUDIO_CMD_PREFIXES = ("/学歌", "/learn-song")
_ALLOWED_AUDIO_EXTENSIONS = frozenset(
    {".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus", ".aac", ".webm"}
)
_NUMERIC_ID_RE = re.compile(r"^-?[0-9]{1,24}\Z")
_B23_PATH_RE = re.compile(r"^/[A-Za-z0-9]{1,32}\Z")


def _parse_id_set(raw: str | None) -> set[str]:
    out: set[str] = set()
    if not raw:
        return out
    for part in str(raw).replace(";", ",").replace("\n", ",").split(","):
        tok = part.strip()
        if not tok:
            continue
        if ":" in tok:
            plat, _, ident = tok.partition(":")
            if plat.strip().lower() == "telegram":
                tok = ident.strip()
        if _NUMERIC_ID_RE.fullmatch(tok):
            out.add(tok)
    return out


def _utf16_slice(text: str, offset: int, length: int) -> str:
    """Slice Python str using Telegram Bot API UTF-16 code unit offsets."""
    if offset < 0 or length <= 0:
        return ""
    encoded = text.encode("utf-16-le")
    start = offset * 2
    end = (offset + length) * 2
    if start >= len(encoded):
        return ""
    return encoded[start:end].decode("utf-16-le", errors="ignore")


def _parse_reply_id(raw: Any) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if re.fullmatch(r"[0-9]{1,20}", text):
        return int(text)
    return None


def _guess_audio_ext(mime_type: str, fallback: str = "ogg") -> str:
    low = (mime_type or "").lower()
    if "mpeg" in low or "mp3" in low:
        return "mp3"
    if "wav" in low:
        return "wav"
    if "flac" in low:
        return "flac"
    if "mp4" in low or "m4a" in low:
        return "m4a"
    if "aac" in low:
        return "aac"
    if "webm" in low:
        return "webm"
    if "ogg" in low or "opus" in low:
        return "ogg"
    return fallback


def _is_learn_song_caption(text: str) -> bool:
    stripped = (text or "").strip().lower()
    for prefix in _AUDIO_CMD_PREFIXES:
        if (
            stripped == prefix
            or stripped.startswith(prefix + " ")
            or stripped.startswith(prefix + "@")
        ):
            return True
    return False


def _format_sender_name(sender: dict[str, Any] | None, fallback_id: str = "") -> str:
    if not isinstance(sender, dict):
        cleaned = re.sub(r"[\r\n\[\]]", " ", str(fallback_id or "")).strip()
        return cleaned[:80] or "群友"
    first_name = str(sender.get("first_name") or "").strip()
    last_name = str(sender.get("last_name") or "").strip()
    raw = (
        f"{first_name} {last_name}".strip()
        if first_name or last_name
        else str(sender.get("username") or fallback_id or "群友")
    )
    return re.sub(r"[\r\n\[\]]", " ", raw).strip()[:80] or "群友"


def _extract_bili_entity_urls(entities: list[dict[str, Any]], existing_text: str) -> list[str]:
    """Extract canonical Bilibili video or b23.tv URLs hidden inside Telegram text_link entities."""
    out: list[str] = []
    seen: set[str] = set()
    for ent in entities:
        if not isinstance(ent, dict) or ent.get("type") != "text_link":
            continue
        raw_url = ent.get("url")
        if not isinstance(raw_url, str):
            continue
        cleaned = raw_url.strip()
        if not cleaned or len(cleaned) > 1024:
            continue
        bvid = extract_url_bvid(cleaned)
        if bvid:
            canonical = f"https://www.bilibili.com/video/{bvid}"
            if bvid not in existing_text and canonical not in seen:
                seen.add(canonical)
                out.append(canonical)
        else:
            try:
                parsed = urlsplit(cleaned)
                port = parsed.port
            except ValueError:
                continue
            if (
                parsed.scheme in {"https", "http"}
                and (parsed.hostname or "").lower() == "b23.tv"
                and not parsed.username
                and not parsed.password
                and (
                    (parsed.scheme == "https" and port in {None, 443})
                    or (parsed.scheme == "http" and port in {None, 80})
                )
                and _B23_PATH_RE.fullmatch(parsed.path)
            ):
                canonical = f"https://b23.tv{parsed.path}"
                if canonical not in existing_text and canonical not in seen:
                    seen.add(canonical)
                    out.append(canonical)
        if len(out) >= 3:
            break
    return out


class TelegramPlugin:
    """Stateful out-of-process Telegram Bot plugin wrapping HakimiPluginClient."""

    def __init__(
        self,
        *,
        ws_url: str,
        token: str,
        bot_token: str,
        owner_user_id: str = "",
        allowed_users: set[str] | None = None,
        allowed_groups: set[str] | None = None,
        proactive_groups: set[str] | None = None,
        private_mode: Literal["owner_only", "owner_and_allowlist"] = "owner_only",
        poll_timeout: int = 25,
        api_base: str = DEFAULT_TG_API_BASE,
        http: httpx.AsyncClient | None = None,
        api: TelegramBotApi | None = None,
        clock: Any = None,
        connect_factory: Any = None,
        reconnect_min: float = 1.0,
        reconnect_max: float = 30.0,
        enable_polling: bool = True,
    ) -> None:
        clean_owner = str(owner_user_id or "").strip()
        if clean_owner and not _NUMERIC_ID_RE.fullmatch(clean_owner):
            raise ValueError("TG_OWNER_USER_ID must be a numeric Telegram user ID")
        self.owner_user_id = clean_owner
        self.allowed_users: set[str] = {
            u for u in (allowed_users or set()) if _NUMERIC_ID_RE.fullmatch(u)
        }
        self.allowed_groups: set[str] = {
            g for g in (allowed_groups or set()) if _NUMERIC_ID_RE.fullmatch(g)
        }
        if proactive_groups is None:
            self.proactive_groups: set[str] = set(self.allowed_groups)
        else:
            self.proactive_groups = {
                g
                for g in proactive_groups
                if _NUMERIC_ID_RE.fullmatch(g) and g in self.allowed_groups
            }
        self.private_mode: Literal["owner_only", "owner_and_allowlist"] = (
            "owner_and_allowlist" if private_mode == "owner_and_allowlist" else "owner_only"
        )
        self.poll_timeout = max(5, min(int(poll_timeout), 50))
        self.enable_polling = bool(enable_polling)
        self.clock = clock if clock is not None else time.time

        self._owns_api = api is None
        self.api = (
            api if api is not None else TelegramBotApi(bot_token, api_base=api_base, http=http)
        )
        self.bot_id: str = ""
        self.bot_username: str = ""
        self._offset: int | None = None
        self._module_enabled: bool = True
        self._stopped: bool = False
        self._poll_task: asyncio.Task[None] | None = None
        self._media_registry: OrderedDict[str, tuple[str, str]] = OrderedDict()
        self._rx_count: int = 0
        self._tx_count: int = 0
        self._last_error: str = ""

        config = PluginRuntimeConfig(
            ws_url=ws_url,
            plugin_id="telegram",
            token=token,
            display_name="Telegram 社交",
            description="Telegram Bot 收发消息、媒体拉取、语音泡泡、翻唱歌曲投递与自主冒泡",
            scopes=("chat", "media", "sensors"),
            node_region="KR",
            chat_platforms=("telegram",),
            capabilities=("voice", "voice_blob", "reply_anchor", "proactive_speak"),
            voice_format="ogg_opus",
            attention_level="LOW",
            reconnect_min=reconnect_min,
            reconnect_max=reconnect_max,
        )
        self.client = HakimiPluginClient(
            config,
            initial_status=self._status_snapshot(connected=False),
            connect_factory=connect_factory,
        )
        self._bind_handlers()

    def _bind_handlers(self) -> None:
        self.client.on_connect(self._on_connect)
        self.client.on_switch(self._on_switch)
        self.client.sensor(self._sensor_summary)
        self.client.on_chat_delivery(self._handle_chat_delivery)
        self.client.on_voice_delivery(self._handle_voice_delivery)
        self.client.on_media_fetch(self._handle_media_fetch)

    def _status_snapshot(self, *, connected: bool) -> dict[str, Any]:
        return {
            "connected": bool(connected),
            "bot_id": self.bot_id,
            "bot_username": self.bot_username,
            "owner_configured": bool(self.owner_user_id),
            "private_mode": self.private_mode,
            "allowed_groups_count": len(self.allowed_groups),
            "proactive_groups": sorted(self.proactive_groups),
            "rx_count": self._rx_count,
            "tx_count": self._tx_count,
            "last_error": self._last_error,
        }

    def _remember_media(self, file_id: str, kind: str, filename_hint: str = "") -> None:
        if not file_id:
            return
        self._media_registry[file_id] = (kind, filename_hint)
        self._media_registry.move_to_end(file_id)
        while len(self._media_registry) > _MAX_SEEN_MEDIA_IDS:
            self._media_registry.popitem(last=False)

    def _private_allowed(self, user_id: str) -> bool:
        if not user_id or not _NUMERIC_ID_RE.fullmatch(user_id):
            return False
        if self.owner_user_id and user_id == self.owner_user_id:
            return True
        if self.private_mode == "owner_and_allowlist" and user_id in self.allowed_users:
            return True
        return False

    def _group_allowed(self, chat_id: str) -> bool:
        return bool(chat_id and chat_id in self.allowed_groups)

    def _delivery_allowed(self, params: dict[str, Any]) -> tuple[str, str]:
        if str(params.get("platform") or "") != "telegram":
            raise PluginRpcError("delivery_not_authorized", "platform mismatch")
        kind = str(params.get("kind") or "")
        conv_id = str(params.get("conversation_id") or "").strip()
        user_id = str(params.get("user_id") or "").strip()
        if kind == "private":
            if (
                not conv_id
                or (user_id and user_id != conv_id)
                or not self._private_allowed(conv_id)
            ):
                raise PluginRpcError("delivery_not_authorized", "private recipient not allowlisted")
            return kind, conv_id
        if kind == "group":
            if not self._group_allowed(conv_id):
                raise PluginRpcError("delivery_not_authorized", "group recipient not allowlisted")
            return kind, conv_id
        raise PluginRpcError("delivery_not_authorized", "unsupported chat kind")

    async def close(self) -> None:
        self._stopped = True
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        await self.client.stop()
        if self._owns_api:
            await self.api.close()

    async def _on_switch(self, enabled: bool) -> None:
        self._module_enabled = bool(enabled)

    async def _on_connect(self, welcome: dict[str, Any]) -> None:
        self._module_enabled = bool(welcome.get("enabled", True))
        try:
            me = await self.api.get_me()
            self.bot_id = str(me["id"])
            self.bot_username = str(me.get("username") or "").strip()
            self._last_error = ""
        except TelegramApiError as exc:
            self._last_error = exc.code
            log.warning("event=tg_get_me_failed code=%s", exc.code)
            await self.client.push_status(
                self._status_snapshot(connected=False),
                configuration_issue="tg_bot_auth_failed",
            )
            return

        try:
            saved_offset = await self.client.state_get("tg_update_offset")
            if (
                isinstance(saved_offset, int)
                and not isinstance(saved_offset, bool)
                and saved_offset >= 0
            ):
                if self._offset is None or saved_offset > self._offset:
                    self._offset = saved_offset
        except Exception:
            pass

        await self.client.push_status(
            self._status_snapshot(connected=True),
            configuration_issue=None,
        )
        log.info(
            "event=tg_connected bot_id=%s bot_username=%s owner_configured=%s groups=%d",
            self.bot_id,
            self.bot_username,
            bool(self.owner_user_id),
            len(self.allowed_groups),
        )
        if self.enable_polling and (self._poll_task is None or self._poll_task.done()):
            self._poll_task = asyncio.create_task(self._poll_loop(), name="tg-poll-loop")

    async def _sensor_summary(self, _params: dict[str, Any] | None = None) -> str:
        state = "在线" if (self.bot_id and not self._last_error) else "异常"
        handle = f"(@{self.bot_username})" if self.bot_username else ""
        return (
            f"Telegram:{state}{handle} 授权群:{len(self.allowed_groups)} "
            f"收信:{self._rx_count} 发信:{self._tx_count}"
        )

    def _normalize_command_and_mention(
        self,
        raw_text: str,
        entities: list[dict[str, Any]],
        *,
        kind: Literal["private", "group"],
        reply_to_bot: bool,
    ) -> tuple[str, str | None, bool, str]:
        """Return (text, command_text, addressed, attention_reason)."""
        bot_uname_low = self.bot_username.lower()
        mentioned_bot = False
        explicit_other_bot_cmd = False
        cmd_addressed_us = False
        normalized_text = raw_text

        for ent in entities:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type")
            off = ent.get("offset")
            length = ent.get("length")
            if etype == "text_mention":
                user = ent.get("user")
                if isinstance(user, dict) and str(user.get("id") or "") == self.bot_id:
                    mentioned_bot = True
            elif etype == "mention" and isinstance(off, int) and isinstance(length, int):
                piece = _utf16_slice(raw_text, off, length)
                if bot_uname_low and piece.lower() == f"@{bot_uname_low}":
                    mentioned_bot = True
            elif etype == "bot_command" and off == 0 and isinstance(length, int):
                cmd_piece = _utf16_slice(raw_text, 0, length)
                if "@" in cmd_piece:
                    cmd_head, _, target_bot = cmd_piece.partition("@")
                    if bot_uname_low and target_bot.lower() == bot_uname_low:
                        cmd_addressed_us = True
                        normalized_text = cmd_head + raw_text[len(cmd_piece) :]
                    else:
                        explicit_other_bot_cmd = True

        # Fallback when Telegram omits bot_command entity (e.g. non-ASCII `/学歌@bot`).
        if normalized_text.startswith("/") and not explicit_other_bot_cmd:
            head, sep, tail = normalized_text.partition(" ")
            if "@" in head:
                cmd_head, _, target_bot = head.partition("@")
                if bot_uname_low and target_bot.lower() == bot_uname_low:
                    cmd_addressed_us = True
                    normalized_text = f"{cmd_head}{sep}{tail}" if sep else cmd_head
                elif target_bot:
                    explicit_other_bot_cmd = True

        command_text: str | None = None
        if normalized_text.startswith("/") and not explicit_other_bot_cmd:
            command_text = normalized_text

        if kind == "private":
            reason = "command" if command_text else "private"
            return normalized_text, command_text, True, reason

        # Group / supergroup addressing rules
        if explicit_other_bot_cmd:
            return raw_text, None, False, ""
        if cmd_addressed_us or (
            command_text is not None
            and (
                command_text.startswith("/哈基米")
                or re.match(r"^/(?:存表情|偷表情|save-meme)(?:\s|$)", command_text)
            )
        ):
            return normalized_text, command_text, True, "command"
        if mentioned_bot:
            return normalized_text, command_text, True, "mention"
        if reply_to_bot:
            return normalized_text, command_text, True, "reply"
        return normalized_text, command_text, False, ""

    def _extract_media_from_message(
        self,
        msg: dict[str, Any],
        *,
        sender_id: str,
        sender_nickname: str,
        is_learn_song: bool,
        is_reply: bool,
        images: list[dict[str, Any]],
        files: list[dict[str, Any]],
        audios: list[dict[str, Any]],
    ) -> list[str]:
        """Extract bounded photo/sticker/voice/audio/document refs and return placeholder tags."""
        tags: list[str] = []
        seen_images = {str(item.get("file") or "") for item in images}
        seen_files = {str(item.get("file_id") or "") for item in files}
        seen_audios = {str(item.get("file") or "") for item in audios}

        def _origin(direct_label: str) -> str:
            return "引用消息" if is_reply else direct_label

        # 1. Photos (pick largest <= 20 MiB)
        photos = msg.get("photo")
        if isinstance(photos, list) and photos:
            tags.append("[图片]")
            if len(images) < _MAX_INBOUND_IMAGES:
                candidates = [
                    p for p in photos if isinstance(p, dict) and isinstance(p.get("file_id"), str)
                ]
                eligible = [
                    p
                    for p in candidates
                    if not isinstance(p.get("file_size"), int)
                    or p["file_size"] <= _MAX_PHOTO_FILE_BYTES
                ]
                chosen = eligible[-1] if eligible else (candidates[-1] if candidates else None)
                if chosen is not None:
                    fid = chosen["file_id"].strip()
                    if fid and fid not in seen_images:
                        seen_images.add(fid)
                        self._remember_media(fid, "image", "photo.jpg")
                        images.append(
                            {
                                "file": fid,
                                "file_size": chosen.get("file_size"),
                                "sender_id": sender_id,
                                "sender_nickname": sender_nickname,
                                "origin": _origin("telegram_photo"),
                                "is_sticker": False,
                            }
                        )

        # 2. Stickers (static or thumbnail fallback)
        sticker = msg.get("sticker")
        if isinstance(sticker, dict):
            tags.append("[贴纸]")
            if len(images) < _MAX_INBOUND_IMAGES:
                if not sticker.get("is_animated") and not sticker.get("is_video"):
                    fid = str(sticker.get("file_id") or "").strip()
                    fsize = sticker.get("file_size")
                else:
                    thumb = sticker.get("thumbnail") or sticker.get("thumb")
                    fid = str(thumb.get("file_id") or "").strip() if isinstance(thumb, dict) else ""
                    fsize = thumb.get("file_size") if isinstance(thumb, dict) else None
                if fid and fid not in seen_images:
                    seen_images.add(fid)
                    self._remember_media(fid, "image", "sticker.webp")
                    images.append(
                        {
                            "file": fid,
                            "file_size": fsize,
                            "sender_id": sender_id,
                            "sender_nickname": sender_nickname,
                            "origin": _origin("telegram_sticker"),
                            "is_sticker": True,
                        }
                    )

        # 3. Voice notes -> Gemini native audio listening
        voice = msg.get("voice")
        if isinstance(voice, dict):
            tags.append("[语音]")
            fid = str(voice.get("file_id") or "").strip()
            if fid and fid not in seen_audios and len(audios) < _MAX_INBOUND_AUDIOS:
                seen_audios.add(fid)
                self._remember_media(fid, "audio", "voice.ogg")
                raw_dur = voice.get("duration")
                audios.append(
                    {
                        "file": fid,
                        "file_size": voice.get("file_size"),
                        "duration": float(raw_dur)
                        if isinstance(raw_dur, int | float) and raw_dur >= 0
                        else None,
                        "sender_id": sender_id,
                        "sender_nickname": sender_nickname,
                        "origin": _origin("telegram_voice"),
                    }
                )

        # 4. Audio files (route to `files` for `/学歌` command, else `audios` for Gemini listening)
        audio_obj = msg.get("audio")
        if isinstance(audio_obj, dict):
            mime = str(audio_obj.get("mime_type") or "")
            fname = str(audio_obj.get("file_name") or "").strip()
            if not fname:
                title = str(audio_obj.get("title") or "audio").strip()[:80]
                ext = _guess_audio_ext(mime, "mp3")
                fname = f"{title}.{ext}"
            tags.append(f"[音频：{fname[:60]}]")
            fid = str(audio_obj.get("file_id") or "").strip()
            if fid:
                suffix = PurePosixPath(fname.lower()).suffix
                if is_learn_song and suffix in _ALLOWED_AUDIO_EXTENSIONS:
                    if fid not in seen_files and len(files) < _MAX_INBOUND_FILES:
                        seen_files.add(fid)
                        self._remember_media(fid, "file", fname)
                        files.append(
                            {
                                "name": fname[:255],
                                "file_id": fid,
                                "file_size": audio_obj.get("file_size"),
                                "sender_id": sender_id,
                                "sender_nickname": sender_nickname,
                                "origin": _origin("telegram_audio_cmd"),
                            }
                        )
                elif fid not in seen_audios and len(audios) < _MAX_INBOUND_AUDIOS:
                    seen_audios.add(fid)
                    self._remember_media(fid, "audio", fname)
                    raw_dur = audio_obj.get("duration")
                    audios.append(
                        {
                            "file": fid,
                            "file_size": audio_obj.get("file_size"),
                            "duration": (
                                float(raw_dur)
                                if isinstance(raw_dur, int | float) and raw_dur >= 0
                                else None
                            ),
                            "sender_id": sender_id,
                            "sender_nickname": sender_nickname,
                            "origin": _origin("telegram_audio"),
                        }
                    )

        # 5. Documents: image -> images, `/学歌` audio -> files, audio -> audios, else -> files
        doc = msg.get("document")
        if isinstance(doc, dict):
            mime = str(doc.get("mime_type") or "").lower()
            fname = str(doc.get("file_name") or "").strip() or "attachment.bin"
            suffix = PurePosixPath(fname.lower()).suffix
            if mime.startswith("image/"):
                tags.append("[图片]")
            elif mime.startswith("audio/") or suffix in _ALLOWED_AUDIO_EXTENSIONS:
                tags.append(f"[音频：{fname[:60]}]")
            else:
                tags.append(f"[文件：{fname[:60]}]")
            fid = str(doc.get("file_id") or "").strip()
            if fid:
                if mime.startswith("image/"):
                    if fid not in seen_images and len(images) < _MAX_INBOUND_IMAGES:
                        seen_images.add(fid)
                        self._remember_media(fid, "image", fname)
                        images.append(
                            {
                                "file": fid,
                                "file_size": doc.get("file_size"),
                                "sender_id": sender_id,
                                "sender_nickname": sender_nickname,
                                "origin": _origin("telegram_doc_image"),
                                "is_sticker": False,
                            }
                        )
                elif (
                    mime.startswith("audio/") or suffix in _ALLOWED_AUDIO_EXTENSIONS
                ) and not is_learn_song:
                    if fid not in seen_audios and len(audios) < _MAX_INBOUND_AUDIOS:
                        seen_audios.add(fid)
                        self._remember_media(fid, "audio", fname)
                        audios.append(
                            {
                                "file": fid,
                                "file_size": doc.get("file_size"),
                                "sender_id": sender_id,
                                "sender_nickname": sender_nickname,
                                "origin": _origin("telegram_doc_audio"),
                            }
                        )
                elif fid not in seen_files and len(files) < _MAX_INBOUND_FILES:
                    seen_files.add(fid)
                    self._remember_media(fid, "file", fname)
                    files.append(
                        {
                            "name": fname[:255],
                            "file_id": fid,
                            "file_size": doc.get("file_size"),
                            "sender_id": sender_id,
                            "sender_nickname": sender_nickname,
                            "origin": _origin("telegram_document"),
                        }
                    )
        return tags

    def parse_update(self, update: dict[str, Any]) -> dict[str, Any] | None:
        """Convert one Telegram Update dict into `chat.inbound` kwargs, or None if dropped."""
        msg = update.get("message")
        if not isinstance(msg, dict):
            return None
        chat = msg.get("chat")
        sender = msg.get("from")
        if not isinstance(chat, dict) or not isinstance(sender, dict):
            return None
        if sender.get("is_bot") is True:
            return None

        chat_type = str(chat.get("type") or "")
        if chat_type == "private":
            kind: Literal["private", "group"] = "private"
        elif chat_type in {"group", "supergroup"}:
            kind = "group"
        else:
            return None

        user_id = str(sender.get("id") or "").strip()
        chat_id = str(chat.get("id") or "").strip()
        if not user_id or not chat_id:
            return None
        if self.bot_id and user_id == self.bot_id:
            return None

        if kind == "private":
            if chat_id != user_id or not self._private_allowed(user_id):
                log.info("event=tg_inbound_dropped kind=private user_id=%s", user_id)
                return None
        else:
            if not self._group_allowed(chat_id):
                log.info("event=tg_inbound_dropped kind=group chat_id=%s", chat_id)
                return None

        sender_nickname = _format_sender_name(sender, user_id)
        group_name = str(chat.get("title") or "").strip()[:100] if kind == "group" else ""

        reply_msg = msg.get("reply_to_message")
        reply_ids: list[str] = []
        reply_to_bot = False
        reply_same_chat = False
        if isinstance(reply_msg, dict):
            reply_chat = reply_msg.get("chat")
            reply_same_chat = (
                not isinstance(reply_chat, dict)
                or reply_chat.get("id") is None
                or str(reply_chat.get("id")).strip() == chat_id
            )
            if reply_same_chat and reply_msg.get("message_id") is not None:
                reply_ids.append(str(reply_msg["message_id"]))
            reply_from = reply_msg.get("from")
            if (
                reply_same_chat
                and isinstance(reply_from, dict)
                and self.bot_id
                and str(reply_from.get("id") or "") == self.bot_id
            ):
                reply_to_bot = True

        raw_text = str(
            msg.get("text") if msg.get("text") is not None else (msg.get("caption") or "")
        )
        entities = (
            msg.get("entities") if msg.get("text") is not None else msg.get("caption_entities")
        )
        ent_list = entities if isinstance(entities, list) else []

        text, command_text, addressed, attention_reason = self._normalize_command_and_mention(
            raw_text,
            ent_list,
            kind=kind,
            reply_to_bot=reply_to_bot,
        )
        direct_text_before_extra = text
        hidden_bili_urls = _extract_bili_entity_urls(ent_list, text)
        if hidden_bili_urls:
            extra_links = " ".join(hidden_bili_urls)
            text = f"{text} {extra_links}".strip() if text else extra_links

        images: list[dict[str, Any]] = []
        files: list[dict[str, Any]] = []
        audios: list[dict[str, Any]] = []
        is_learn_song = _is_learn_song_caption(command_text or direct_text_before_extra)

        self._extract_media_from_message(
            msg,
            sender_id=user_id,
            sender_nickname=sender_nickname,
            is_learn_song=is_learn_song,
            is_reply=False,
            images=images,
            files=files,
            audios=audios,
        )

        # Expand single-level reply_to_message (text + media); never grant command authority.
        if isinstance(reply_msg, dict) and reply_same_chat:
            reply_from = reply_msg.get("from")
            reply_sender_chat = reply_msg.get("sender_chat")
            if isinstance(reply_from, dict):
                reply_uid = str(reply_from.get("id") or "").strip()
                reply_name = _format_sender_name(reply_from, reply_uid)
            elif isinstance(reply_sender_chat, dict):
                reply_uid = str(reply_sender_chat.get("id") or "").strip()
                reply_name = (
                    re.sub(
                        r"[\r\n\[\]]",
                        " ",
                        str(
                            reply_sender_chat.get("title")
                            or reply_sender_chat.get("username")
                            or reply_uid
                            or "频道"
                        ),
                    ).strip()[:80]
                    or "频道"
                )
            else:
                reply_uid = ""
                reply_name = "群友"

            reply_tags = self._extract_media_from_message(
                reply_msg,
                sender_id=reply_uid,
                sender_nickname=reply_name,
                is_learn_song=is_learn_song,
                is_reply=True,
                images=images,
                files=files,
                audios=audios,
            )
            q_raw_text = str(
                reply_msg.get("text")
                if reply_msg.get("text") is not None
                else (reply_msg.get("caption") or "")
            ).strip()
            q_entities = (
                reply_msg.get("entities")
                if reply_msg.get("text") is not None
                else reply_msg.get("caption_entities")
            )
            q_ent_list = q_entities if isinstance(q_entities, list) else []
            q_hidden_urls = _extract_bili_entity_urls(q_ent_list, q_raw_text)
            if q_hidden_urls:
                q_extra = " ".join(q_hidden_urls)
                q_raw_text = f"{q_raw_text} {q_extra}".strip() if q_raw_text else q_extra

            q_parts = [part for part in (q_raw_text, " ".join(reply_tags)) if part]
            if q_parts:
                q_body = " ".join(q_parts)[:_MAX_QUOTED_TEXT_CHARS]
                if self.bot_id and reply_uid == self.bot_id:
                    author = "我自己(吉米奈喵)"
                elif reply_uid:
                    author = f"{reply_name}(telegram:{reply_uid})"
                else:
                    author = reply_name
                quote_line = f'[引用 {author} 的消息: "{q_body}"]'
                if text:
                    available = _MAX_TEXT_CHARS - len(text) - 1
                    if available > 0:
                        text = f"{text}\n{quote_line[:available]}"
                else:
                    text = quote_line[:_MAX_TEXT_CHARS]
                # Lock command_text strictly to direct top-level text so quotes never invoke cmds.
                if command_text is None:
                    command_text = (
                        "" if direct_text_before_extra.startswith("/") else direct_text_before_extra
                    )

        if not text and not images and not files and not audios:
            return None

        raw_date = msg.get("date")
        created_at = (
            float(raw_date)
            if isinstance(raw_date, int | float) and not isinstance(raw_date, bool)
            else float(self.clock())
        )
        return {
            "platform": "telegram",
            "bot_id": self.bot_id or "telegram",
            "event_id": str(
                msg.get("message_id") or f"tg-{update.get('update_id', int(created_at))}"
            ),
            "kind": kind,
            "user_id": user_id,
            "conversation_id": chat_id,
            "text": text,
            "addressed": addressed,
            "created_at": created_at,
            "sender_nickname": sender_nickname,
            "group_name": group_name,
            "command_text": command_text,
            "reply_ids": reply_ids,
            "attention_reason": attention_reason,
            "images": images,
            "files": files,
            "audios": audios,
        }

    async def process_update(self, update: dict[str, Any]) -> bool:
        """Parse one Telegram update and forward it over the plugin bus if admitted."""
        parsed = self.parse_update(update)
        if parsed is None:
            return False
        if not self._module_enabled or not self.client.connected:
            return False
        await self.client.emit_chat_inbound(**parsed)
        self._rx_count += 1
        return True

    async def _poll_loop(self) -> None:
        backoff = 1.0
        while not self._stopped:
            if not self.client.connected or not self._module_enabled or not self.bot_id:
                await asyncio.sleep(1.0)
                continue
            try:
                updates = await self.api.get_updates(
                    offset=self._offset,
                    timeout=self.poll_timeout,
                )
                backoff = 1.0
                if self._last_error:
                    self._last_error = ""
                    with contextlib.suppress(Exception):
                        await self.client.push_status(
                            self._status_snapshot(connected=True),
                            configuration_issue=None,
                        )
            except asyncio.CancelledError:
                raise
            except TelegramApiError as exc:
                self._last_error = exc.code
                sleep_for = exc.retry_after if exc.retry_after is not None else backoff
                log.warning("event=tg_poll_error code=%s sleep=%.1f", exc.code, sleep_for)
                backoff = min(backoff * 2.0, 30.0)
                await asyncio.sleep(sleep_for)
                continue
            except Exception as exc:
                self._last_error = type(exc).__name__
                log.warning("event=tg_poll_exception err=%s", type(exc).__name__)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
                continue

            if not updates:
                continue

            max_offset: int | None = None
            for upd in updates:
                uid = upd.get("update_id")
                if isinstance(uid, int) and not isinstance(uid, bool):
                    next_off = uid + 1
                    if max_offset is None or next_off > max_offset:
                        max_offset = next_off
                try:
                    await self.process_update(upd)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("event=tg_update_process_failed err=%s", type(exc).__name__)

            if max_offset is not None and (self._offset is None or max_offset > self._offset):
                self._offset = max_offset
                with contextlib.suppress(Exception):
                    await self.client.state_set("tg_update_offset", self._offset)

    async def _handle_chat_delivery(self, params: dict[str, Any]) -> dict[str, Any]:
        _kind, chat_id = self._delivery_allowed(params)
        text = str(params.get("text") or "")
        if not text.strip():
            raise PluginRpcError("empty_message", "text is empty")
        reply_to = _parse_reply_id(params.get("reply_to"))
        try:
            msg_id = await self.api.send_message(
                chat_id,
                text,
                reply_to_message_id=reply_to,
            )
        except TelegramApiError as exc:
            raise PluginRpcError(exc.code, exc.description) from None
        self._tx_count += 1
        return {"message_id": msg_id}

    async def _handle_voice_delivery(self, params: dict[str, Any]) -> dict[str, Any]:
        _kind, chat_id = self._delivery_allowed(params)
        raw_audio: bytes | None = None
        if isinstance(params.get("blob"), (bytes, bytearray)):
            raw_audio = bytes(params["blob"])
        elif isinstance(params.get("audio_b64"), str) and params["audio_b64"]:
            try:
                raw_audio = base64.b64decode(params["audio_b64"], validate=True)
            except (ValueError, binascii.Error):
                raise PluginRpcError("invalid_voice_audio", "invalid base64 audio") from None
        if not raw_audio:
            raise PluginRpcError("invalid_voice_audio", "empty audio payload")

        fmt = str(params.get("format") or "ogg_opus").strip()
        mime_type = str(
            params.get("mime_type") or ("audio/ogg" if fmt == "ogg_opus" else f"audio/{fmt}")
        ).strip()
        ext = "ogg" if fmt == "ogg_opus" else fmt
        raw_dur_ms = params.get("duration_ms")
        duration_sec = (
            max(1, round(float(raw_dur_ms) / 1000.0))
            if isinstance(raw_dur_ms, int | float)
            and not isinstance(raw_dur_ms, bool)
            and raw_dur_ms > 0
            else None
        )
        reply_to = _parse_reply_id(params.get("reply_to"))
        title = str(params.get("title") or "").strip() or None
        kind_hint = str(params.get("kind_hint") or "").strip().lower()
        is_song = kind_hint == "song" or title is not None

        try:
            if is_song:
                fname = str(params.get("blob_filename") or "").strip() or f"{title or 'song'}.{ext}"
                msg_id = await self.api.send_audio(
                    chat_id,
                    raw_audio,
                    filename=fname,
                    mime_type=mime_type,
                    title=title or "哈基米翻唱",
                    performer="哈基米",
                    duration=duration_sec,
                    reply_to_message_id=reply_to,
                )
            else:
                fname = str(params.get("blob_filename") or "").strip() or f"voice.{ext}"
                msg_id = await self.api.send_voice(
                    chat_id,
                    raw_audio,
                    filename=fname,
                    mime_type=mime_type,
                    duration=duration_sec,
                    reply_to_message_id=reply_to,
                )
        except TelegramApiError as exc:
            raise PluginRpcError(exc.code, exc.description) from None
        self._tx_count += 1
        return {"message_id": msg_id}

    async def _handle_media_fetch(self, params: dict[str, Any]) -> BlobResult:
        media_id = str(params.get("media_id") or "").strip()
        if not media_id or media_id not in self._media_registry:
            raise PluginRpcError("media_not_found", "unknown or unallowlisted media_id")
        kind, hint_name = self._media_registry[media_id]
        raw_cap = params.get("max_bytes")
        cap = (
            min(max(1, int(raw_cap)), _MAX_FETCH_BYTES)
            if isinstance(raw_cap, int) and not isinstance(raw_cap, bool)
            else _MAX_FETCH_BYTES
        )
        try:
            raw_bytes, tg_name = await self.api.get_file_bytes(media_id, max_bytes=cap)
        except TelegramApiError as exc:
            raise PluginRpcError(exc.code, exc.description) from None
        fname = hint_name or tg_name
        guessed_mime = mimetypes.guess_type(fname)[0] or (
            "audio/ogg" if kind == "audio" else "application/octet-stream"
        )
        return BlobResult(
            data={"ok": True, "kind": kind},
            blob=raw_bytes,
            mime_type=guessed_mime,
            filename=fname,
        )


def build_telegram_plugin_from_env(**overrides: Any) -> TelegramPlugin:
    """Create a TelegramPlugin instance from environment variables."""
    ws_url = os.getenv("HAKIMI_PLUGIN_WS_URL") or "ws://127.0.0.1:12141/ws/plugin"
    token = (
        os.getenv("HAKIMI_PLUGIN_TELEGRAM_TOKEN") or os.getenv("HAKIMI_PLUGIN_TOKEN") or ""
    ).strip()
    if not token and "token" not in overrides:
        raise RuntimeError("HAKIMI_PLUGIN_TELEGRAM_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    bot_token = (os.getenv("TG_BOT_TOKEN") or "").strip()
    if not bot_token and "bot_token" not in overrides and "api" not in overrides:
        raise RuntimeError("TG_BOT_TOKEN is required")

    allowed_groups = _parse_id_set(os.getenv("TG_ALLOWED_GROUPS"))
    raw_proactive = os.getenv("TG_PROACTIVE_GROUPS")
    proactive_groups = (
        _parse_id_set(raw_proactive)
        if raw_proactive is not None and raw_proactive.strip()
        else None
    )
    raw_mode = (os.getenv("TG_PRIVATE_MODE") or "owner_only").strip().lower()
    private_mode: Literal["owner_only", "owner_and_allowlist"] = (
        "owner_and_allowlist" if raw_mode == "owner_and_allowlist" else "owner_only"
    )
    poll_timeout_raw = (os.getenv("TG_POLL_TIMEOUT") or "25").strip()
    try:
        poll_timeout = int(poll_timeout_raw)
    except ValueError:
        poll_timeout = 25

    kwargs: dict[str, Any] = {
        "ws_url": ws_url,
        "token": token,
        "bot_token": bot_token,
        "owner_user_id": (os.getenv("TG_OWNER_USER_ID") or "").strip(),
        "allowed_users": _parse_id_set(os.getenv("TG_ALLOWED_USERS")),
        "allowed_groups": allowed_groups,
        "proactive_groups": proactive_groups,
        "private_mode": private_mode,
        "poll_timeout": poll_timeout,
        "api_base": (os.getenv("TG_API_BASE") or DEFAULT_TG_API_BASE).strip(),
    }
    kwargs.update(overrides)
    return TelegramPlugin(**kwargs)


async def main() -> None:
    """Async CLI entrypoint for the Telegram remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Never let httpx/httpcore log Telegram Bot API request URLs (which embed TG_BOT_TOKEN).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    plugin = build_telegram_plugin_from_env()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        asyncio.create_task(plugin.client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await plugin.client.run_forever()
    finally:
        await plugin.close()


if __name__ == "__main__":
    asyncio.run(main())
