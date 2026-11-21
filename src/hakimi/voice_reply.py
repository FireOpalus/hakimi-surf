"""Centralized TTS synthesis, sanitization, and policy gating for voice replies."""

from __future__ import annotations

import re
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
from .security import Rejected, clean_text, configured_secrets, safe_output

MAX_VOICE_AUDIO_BYTES = 2 * 1024 * 1024
_MAX_COOLDOWN_SCOPES = 512

_UNSPOKEN_CONTENT_RE = re.compile(r"```|https?://", re.IGNORECASE)
_TAG_SEGMENT_RE = re.compile(r"\[(?:CQ|AT):[^\]]*\]", re.IGNORECASE)
_MD_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]*\)")
_MD_MARKERS_RE = re.compile(r"~~+|[`*_#>]")
_EXPLICIT_VOICE_RE = re.compile(
    r"(?:发(?:一条|个|段|条)?语音|语音(?:回复|回我|说|讲|聊聊|消息)|"
    r"用语音|念给我听|读给我听|说给我听|听(?:听)?你的声音|听你声音|"
    r"voice\s*(?:message|note|reply))",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SynthesizedVoice:
    audio_bytes: bytes
    fmt: str
    mime_type: str
    duration_ms: int | None = None


def sanitize_voice_text(
    raw: str | None,
    *,
    settings: Settings | None = None,
    max_chars: int = 120,
) -> str | None:
    """Normalize spoken copy and reject secrets, URLs, code blocks, or oversized text."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    secrets = configured_secrets(settings) if settings is not None else []
    if any(secret and len(secret) >= 6 and secret in raw for secret in secrets):
        return None
    if _UNSPOKEN_CONTENT_RE.search(raw):
        return None

    text = clean_text(raw)
    text = _TAG_SEGMENT_RE.sub("", text)
    text = _MD_LINK_RE.sub(r"\1", text)
    text = _MD_MARKERS_RE.sub("", text)
    text = " ".join(text.split()).strip()
    if not text or len(text) > max_chars:
        return None
    if any(secret and len(secret) >= 6 and secret in text for secret in secrets):
        return None
    try:
        cleaned = safe_output(text, secrets, limit=max_chars).strip()
    except Rejected:
        return None
    if not cleaned or len(cleaned) > max_chars:
        return None
    return cleaned


def explicit_voice_request(*texts: str) -> bool:
    """Return True when user text explicitly asks for a voice reply."""
    for item in texts:
        if isinstance(item, str) and item and _EXPLICIT_VOICE_RE.search(item):
            return True
    return False


class VoiceReplyClient:
    """Policy gate and HTTP TTS client backed by hakimi-voice-gateway."""

    def __init__(
        self,
        settings: Settings,
        *,
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.http = http
        self.clock = clock
        self._last_sent: OrderedDict[str, float] = OrderedDict()
        self._daily_day: str = ""
        self._daily_count: int = 0

    def configured(self) -> bool:
        return bool(
            self.s.voice_reply_enabled
            and self.s.voice_gateway_url.strip()
            and self.s.voice_gateway_token.get_secret_value().strip()
        )

    def _local_day(self, now: float) -> str:
        return datetime.fromtimestamp(now, ZoneInfo(self.s.bot_timezone)).date().isoformat()

    def _roll_day(self, now: float) -> None:
        day = self._local_day(now)
        if day != self._daily_day:
            self._daily_day = day
            self._daily_count = 0

    def allowed_for_message(self, message: Message, *, sandbox: bool = False) -> bool:
        if sandbox or not self.configured():
            return False
        mode = self.s.voice_reply_mode
        if mode == "owner_only":
            return message.kind == "private" and is_persona_owner(message, self.s)
        if mode == "allowlist":
            if message.kind == "private" and is_persona_owner(message, self.s):
                return True
            if message.kind != "group":
                return False
            scoped = f"{message.platform}:{message.conversation_id}"
            allowed = self.s.voice_reply_allowed_groups
            return scoped in allowed or (
                message.platform == "onebot" and message.conversation_id in allowed
            )
        return mode == "all"

    def can_send_now(
        self,
        scope: str,
        *,
        explicit: bool = False,
        now: float | None = None,
    ) -> tuple[bool, str]:
        ts = self.clock() if now is None else now
        self._roll_day(ts)
        if self._daily_count >= self.s.voice_reply_daily_limit:
            return (False, "daily_limit")
        cooldown = float(self.s.voice_reply_cooldown_seconds or 0.0)
        if not explicit and cooldown > 0.0:
            prev = self._last_sent.get(scope)
            if prev is not None and (ts - prev) < cooldown:
                return (False, "cooldown")
        return (True, "ok")

    def note_sent(self, scope: str, *, now: float | None = None) -> None:
        ts = self.clock() if now is None else now
        self._roll_day(ts)
        self._daily_count += 1
        self._last_sent[scope] = ts
        self._last_sent.move_to_end(scope)
        while len(self._last_sent) > _MAX_COOLDOWN_SCOPES:
            self._last_sent.popitem(last=False)

    async def synthesize(
        self,
        text: str,
        *,
        output_format: str = "mp3",
    ) -> SynthesizedVoice:
        if not self.configured():
            raise Rejected("voice_not_configured")
        token = self.s.voice_gateway_token.get_secret_value().strip()
        url = f"{self.s.voice_gateway_url.rstrip('/')}/v1/tts"
        payload = {"text": text, "output_format": output_format, "stream": False}
        headers = {"Authorization": f"Bearer {token}"}
        timeout = httpx.Timeout(self.s.voice_reply_timeout_seconds)

        try:
            if self.http is not None:
                resp = await self.http.post(url, json=payload, headers=headers, timeout=timeout)
            else:
                async with httpx.AsyncClient(
                    trust_env=False, follow_redirects=False, timeout=timeout
                ) as client:
                    resp = await client.post(url, json=payload, headers=headers)
        except (httpx.HTTPError, TimeoutError, ValueError):
            raise Rejected("voice_tts_failed") from None

        if resp.status_code != 200:
            raise Rejected("voice_tts_failed")
        audio_bytes = bytes(resp.content)
        if not audio_bytes or len(audio_bytes) > MAX_VOICE_AUDIO_BYTES:
            raise Rejected("voice_tts_failed")

        fmt = (resp.headers.get("x-audio-format") or output_format).strip().lower()
        raw_mime = resp.headers.get("content-type") or "audio/mpeg"
        mime_type = raw_mime.split(";")[0].strip().lower() or "audio/mpeg"
        duration_ms: int | None = None
        raw_dur = (resp.headers.get("x-audio-duration-ms") or "").strip()
        if raw_dur.isdigit():
            parsed_dur = int(raw_dur)
            if parsed_dur > 0:
                duration_ms = parsed_dur
        return SynthesizedVoice(
            audio_bytes=audio_bytes,
            fmt=fmt,
            mime_type=mime_type,
            duration_ms=duration_ms,
        )
