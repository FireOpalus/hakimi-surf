"""Minimal async Telegram Bot API client (pure httpx, strict token redaction)."""

from __future__ import annotations

import json
import logging
from pathlib import PurePosixPath
from typing import Any

import httpx

log = logging.getLogger("hakimi_plugin_telegram")

DEFAULT_TG_API_BASE = "https://api.telegram.org"
MAX_MESSAGE_CHARS = 4000
MAX_JSON_RESPONSE_BYTES = 2 * 1024 * 1024


class TelegramApiError(Exception):
    """Sanitized Telegram Bot API failure with constant snake_case error code."""

    def __init__(
        self,
        code: str,
        description: str = "",
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        self.code = code
        self.description = description
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(description or code)


def split_telegram_text(text: str, *, limit: int = MAX_MESSAGE_CHARS) -> list[str]:
    """Split text into Telegram-safe chunks (<= 4000 chars) preserving line boundaries."""
    raw = str(text or "")
    if not raw:
        return []
    if len(raw) <= limit:
        return [raw]
    chunks: list[str] = []
    remaining = raw
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = remaining.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunk = remaining[:cut].rstrip()
        if not chunk:
            chunk = remaining[:limit]
            cut = limit
        chunks.append(chunk)
        remaining = remaining[cut:].lstrip("\n")
    return chunks


class TelegramBotApi:
    """Async Telegram Bot API transport that never exposes its bot token in logs or errors."""

    def __init__(
        self,
        token: str,
        *,
        api_base: str = DEFAULT_TG_API_BASE,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        clean_token = str(token or "").strip()
        if not clean_token or ":" not in clean_token:
            raise ValueError("invalid TG_BOT_TOKEN")
        self._token = clean_token
        self._base = str(api_base or DEFAULT_TG_API_BASE).rstrip("/")
        self._owns_http = http is None
        self.http = http if http is not None else httpx.AsyncClient(follow_redirects=False)

    def __repr__(self) -> str:
        return f"TelegramBotApi(base={self._base!r}, token='[REDACTED]')"

    def _scrub(self, text: str) -> str:
        if not text:
            return ""
        return text.replace(self._token, "[REDACTED]")

    def _method_url(self, method: str) -> str:
        return f"{self._base}/bot{self._token}/{method}"

    def _file_url(self, file_path: str) -> str:
        return f"{self._base}/file/bot{self._token}/{file_path}"

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    async def _request(
        self,
        method: str,
        *,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        timeout: float | httpx.Timeout = 15.0,  # noqa: ASYNC109
    ) -> Any:
        url = self._method_url(method)
        try:
            resp = await self.http.post(
                url,
                json=json_body,
                data=data,
                files=files,
                timeout=timeout,
            )
        except httpx.TimeoutException:
            raise TelegramApiError("tg_timeout", f"{method} timed out") from None
        except httpx.HTTPError as exc:
            raise TelegramApiError(
                "tg_network_error",
                self._scrub(f"{method} network error: {type(exc).__name__}"),
            ) from None

        if len(resp.content) > MAX_JSON_RESPONSE_BYTES:
            raise TelegramApiError(
                "tg_response_too_large",
                f"{method} response exceeded limit",
                status_code=resp.status_code,
            )

        try:
            payload = resp.json()
        except ValueError:
            raise TelegramApiError(
                "tg_invalid_json",
                f"{method} HTTP {resp.status_code}",
                status_code=resp.status_code,
            ) from None

        if not isinstance(payload, dict):
            raise TelegramApiError("tg_invalid_shape", f"{method} non-object response")

        if resp.status_code == 429 or payload.get("error_code") == 429:
            params = payload.get("parameters")
            retry_after = (
                float(params["retry_after"])
                if isinstance(params, dict)
                and isinstance(params.get("retry_after"), int | float)
                and not isinstance(params.get("retry_after"), bool)
                else 3.0
            )
            raise TelegramApiError(
                "tg_rate_limited",
                self._scrub(str(payload.get("description") or "rate limited")),
                status_code=429,
                retry_after=max(1.0, min(retry_after, 120.0)),
            )

        if not payload.get("ok"):
            desc = self._scrub(str(payload.get("description") or f"HTTP {resp.status_code}"))
            err_code = payload.get("error_code") or resp.status_code
            code = "tg_unauthorized" if err_code in (401, 403) else "tg_api_error"
            if err_code == 400:
                code = "tg_bad_request"
            raise TelegramApiError(code, desc, status_code=int(err_code))

        return payload.get("result")

    async def get_me(self) -> dict[str, Any]:
        res = await self._request("getMe", timeout=10.0)
        if not isinstance(res, dict) or "id" not in res:
            raise TelegramApiError("tg_invalid_shape", "getMe missing bot id")
        return res

    async def get_updates(
        self,
        *,
        offset: int | None = None,
        timeout: int = 25,  # noqa: ASYNC109
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "timeout": int(timeout),
            "limit": int(limit),
            "allowed_updates": ["message"],
        }
        if offset is not None:
            body["offset"] = int(offset)
        http_timeout = httpx.Timeout(
            connect=10.0,
            read=float(timeout) + 15.0,
            write=10.0,
            pool=10.0,
        )
        res = await self._request("getUpdates", json_body=body, timeout=http_timeout)
        if not isinstance(res, list):
            return []
        return [item for item in res if isinstance(item, dict)]

    async def get_file_bytes(self, file_id: str, *, max_bytes: int) -> tuple[bytes, str]:
        cap = max(1, int(max_bytes))
        info = await self._request("getFile", json_body={"file_id": file_id}, timeout=15.0)
        if not isinstance(info, dict):
            raise TelegramApiError("tg_invalid_shape", "getFile invalid result")
        raw_size = info.get("file_size")
        if isinstance(raw_size, int) and raw_size > cap:
            raise TelegramApiError("media_too_large", "telegram file exceeds max_bytes")
        file_path = str(info.get("file_path") or "").strip()
        if (
            not file_path
            or file_path.startswith("/")
            or ".." in file_path
            or "://" in file_path
            or "\\" in file_path
        ):
            raise TelegramApiError("tg_invalid_file_path", "invalid telegram file_path")

        url = self._file_url(file_path)
        buf = bytearray()
        try:
            async with self.http.stream(
                "GET",
                url,
                timeout=httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=10.0),
                follow_redirects=False,
            ) as resp:
                if 300 <= resp.status_code < 400:
                    raise TelegramApiError("tg_redirect_blocked", "redirect not allowed")
                if resp.status_code >= 400:
                    raise TelegramApiError(
                        "tg_file_download_failed",
                        f"file download HTTP {resp.status_code}",
                        status_code=resp.status_code,
                    )
                async for chunk in resp.aiter_bytes():
                    buf.extend(chunk)
                    if len(buf) > cap:
                        raise TelegramApiError("media_too_large", "telegram file exceeded cap")
        except TelegramApiError:
            raise
        except httpx.TimeoutException:
            raise TelegramApiError("tg_timeout", "file download timed out") from None
        except httpx.HTTPError as exc:
            raise TelegramApiError(
                "tg_network_error",
                self._scrub(f"file download error: {type(exc).__name__}"),
            ) from None

        filename = PurePosixPath(file_path).name or "media.bin"
        return bytes(buf), filename

    async def _send_with_reply_fallback(
        self,
        method: str,
        *,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        timeout: float = 15.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        try:
            res = await self._request(
                method,
                json_body=json_body,
                data=data,
                files=files,
                timeout=timeout,
            )
        except TelegramApiError as exc:
            has_reply = (json_body is not None and "reply_parameters" in json_body) or (
                data is not None and "reply_parameters" in data
            )
            if exc.status_code == 400 and has_reply and "repl" in exc.description.lower():
                retry_json = (
                    {k: v for k, v in json_body.items() if k != "reply_parameters"}
                    if json_body is not None
                    else None
                )
                retry_data = (
                    {k: v for k, v in data.items() if k != "reply_parameters"}
                    if data is not None
                    else None
                )
                res = await self._request(
                    method,
                    json_body=retry_json,
                    data=retry_data,
                    files=files,
                    timeout=timeout,
                )
            else:
                raise
        if not isinstance(res, dict) or "message_id" not in res:
            raise TelegramApiError("tg_invalid_shape", f"{method} missing message_id")
        return res

    async def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        reply_to_message_id: int | None = None,
    ) -> str:
        chunks = split_telegram_text(text)
        if not chunks:
            raise TelegramApiError("tg_empty_text", "empty message")
        last_id = ""
        for idx, chunk in enumerate(chunks):
            body: dict[str, Any] = {
                "chat_id": chat_id,
                "text": chunk,
            }
            if idx == 0 and reply_to_message_id is not None:
                body["reply_parameters"] = {
                    "message_id": int(reply_to_message_id),
                    "allow_sending_without_reply": True,
                }
            res = await self._send_with_reply_fallback("sendMessage", json_body=body, timeout=15.0)
            last_id = str(res["message_id"])
        return last_id

    async def send_voice(
        self,
        chat_id: str,
        audio_bytes: bytes,
        *,
        filename: str = "voice.ogg",
        mime_type: str = "audio/ogg",
        duration: int | None = None,
        reply_to_message_id: int | None = None,
    ) -> str:
        data: dict[str, Any] = {"chat_id": str(chat_id)}
        if duration is not None and duration > 0:
            data["duration"] = str(int(duration))
        if reply_to_message_id is not None:
            data["reply_parameters"] = json.dumps(
                {
                    "message_id": int(reply_to_message_id),
                    "allow_sending_without_reply": True,
                },
                separators=(",", ":"),
            )
        res = await self._send_with_reply_fallback(
            "sendVoice",
            data=data,
            files={"voice": (filename, audio_bytes, mime_type)},
            timeout=30.0,
        )
        return str(res["message_id"])

    async def send_audio(
        self,
        chat_id: str,
        audio_bytes: bytes,
        *,
        filename: str = "song.ogg",
        mime_type: str = "audio/ogg",
        title: str | None = None,
        performer: str = "哈基米",
        duration: int | None = None,
        reply_to_message_id: int | None = None,
    ) -> str:
        data: dict[str, Any] = {"chat_id": str(chat_id)}
        if title:
            data["title"] = str(title)[:120]
        if performer:
            data["performer"] = str(performer)[:64]
        if duration is not None and duration > 0:
            data["duration"] = str(int(duration))
        if reply_to_message_id is not None:
            data["reply_parameters"] = json.dumps(
                {
                    "message_id": int(reply_to_message_id),
                    "allow_sending_without_reply": True,
                },
                separators=(",", ":"),
            )
        res = await self._send_with_reply_fallback(
            "sendAudio",
            data=data,
            files={"audio": (filename, audio_bytes, mime_type)},
            timeout=60.0,
        )
        return str(res["message_id"])

    async def send_chat_action(self, chat_id: str, action: str = "typing") -> None:
        try:
            await self._request(
                "sendChatAction",
                json_body={"chat_id": str(chat_id), "action": action},
                timeout=5.0,
            )
        except TelegramApiError:
            pass
