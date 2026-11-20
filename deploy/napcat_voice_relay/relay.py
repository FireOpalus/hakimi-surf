"""Transparent Shanghai-side OneBot v11 WebSocket relay for QQ voice-to-text (fetch_ptt_text).

Runs inside the Shanghai NapCat Docker network and sits between NodeNapCat and the
offshore Hakimi engine so voice transcription runs locally via QQ native STT without
modifying or restarting the backend engine.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import signal
import time
import uuid
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import Server, ServerConnection
from websockets.asyncio.server import serve as ws_serve

log = logging.getLogger("napcat_voice_relay")

INTERNAL_ECHO_PREFIX = "__hakimi_stt_"
_FORWARD_HEADERS = ("authorization", "x-self-id", "x-client-role", "user-agent")
_CQ_RECORD_RE = re.compile(r"\[CQ:record(?:,[^\]]*)?\]")
_CQ_ESCAPE_RE = re.compile(r"\[CQ:", re.IGNORECASE)
_CTRL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")
_WS_COLLAPSE_RE = re.compile(r"\s+")
_PERMANENT_STT_PHRASES = (
    "消息不存在",
    "已被撤回",
    "消息中不包含语音",
    "参数message_id不能为空",
)
_VALID_SCOPES = frozenset({"all", "allowlist", "private"})


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None or not raw.strip():
        return default
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_int(raw: str | None, default: int, *, low: int, high: int) -> int:
    if raw is None or not raw.strip():
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return max(low, min(high, parsed))


def _parse_float(raw: str | None, default: float, *, low: float, high: float) -> float:
    if raw is None or not raw.strip():
        return default
    try:
        parsed = float(raw.strip())
    except ValueError:
        return default
    return max(low, min(high, parsed))


def _parse_id_set(raw: str | None) -> frozenset[str]:
    if not raw:
        return frozenset()
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _parse_delays(raw: str | None, default: tuple[float, ...]) -> tuple[float, ...]:
    if raw is None or not raw.strip():
        return default
    out: list[float] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            val = float(text)
        except ValueError:
            continue
        if 0.05 <= val <= 15.0:
            out.append(val)
    return tuple(out[:5]) or default


@dataclass(frozen=True)
class RelayConfig:
    """Validated runtime configuration loaded from environment variables."""

    upstream_url: str
    listen_host: str = "0.0.0.0"
    listen_port: int = 3001
    stt_enabled: bool = True
    quote_stt_enabled: bool = True
    scope: Literal["all", "allowlist", "private"] = "all"
    allowed_groups: frozenset[str] = frozenset()
    ignored_groups: frozenset[str] = frozenset()
    concurrency: int = 2
    user_rpm: int = 6
    global_rpm: int = 18
    retry_delays: tuple[float, ...] = (0.8, 1.5, 2.2)
    attempt_timeout: float = 6.0
    total_timeout: float = 12.0
    get_msg_timeout: float = 4.5
    max_chars: int = 2000
    cache_ttl: float = 600.0
    cache_max: int = 512
    circuit_fail_threshold: int = 5
    circuit_cooldown: float = 120.0
    prefix: str = "[语音转文字] "
    failure_text: str = "[语音消息：未能识别文字内容]"
    rate_limited_text: str = "[语音消息：语音识别繁忙，暂未识别]"
    max_frame_bytes: int = 16_777_216

    def __post_init__(self) -> None:
        parsed = urlsplit(self.upstream_url.strip())
        if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
            raise ValueError("UPSTREAM_ONEBOT_URL must be a valid ws:// or wss:// URL")
        if self.scope not in _VALID_SCOPES:
            raise ValueError(f"Unsupported VOICE_STT_SCOPE: {self.scope}")

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> RelayConfig:
        env = os.environ if environ is None else environ
        upstream = (env.get("UPSTREAM_ONEBOT_URL") or "").strip()
        if not upstream:
            fallback = (env.get("ONEBOT_URL") or "").strip()
            if fallback and "napcat-voice-relay" not in fallback:
                upstream = fallback
        raw_scope = (env.get("VOICE_STT_SCOPE") or "all").strip().lower()
        scope: Literal["all", "allowlist", "private"] = (
            raw_scope if raw_scope in _VALID_SCOPES else "all"  # type: ignore[assignment]
        )
        return cls(
            upstream_url=upstream,
            listen_host=(env.get("RELAY_LISTEN_HOST") or "0.0.0.0").strip() or "0.0.0.0",
            listen_port=_parse_int(env.get("RELAY_LISTEN_PORT"), 3001, low=1, high=65535),
            stt_enabled=_parse_bool(env.get("VOICE_STT_ENABLED"), True),
            quote_stt_enabled=_parse_bool(env.get("VOICE_STT_QUOTE_ENABLED"), True),
            scope=scope,
            allowed_groups=_parse_id_set(env.get("VOICE_STT_ALLOWED_GROUPS")),
            ignored_groups=_parse_id_set(env.get("VOICE_STT_IGNORED_GROUPS")),
            concurrency=_parse_int(env.get("VOICE_STT_CONCURRENCY"), 2, low=1, high=8),
            user_rpm=_parse_int(env.get("VOICE_STT_USER_RPM"), 6, low=1, high=60),
            global_rpm=_parse_int(env.get("VOICE_STT_GLOBAL_RPM"), 18, low=1, high=120),
            retry_delays=_parse_delays(env.get("VOICE_STT_RETRY_DELAYS"), (0.8, 1.5, 2.2)),
            attempt_timeout=_parse_float(
                env.get("VOICE_STT_ATTEMPT_TIMEOUT"), 6.0, low=1.0, high=20.0
            ),
            total_timeout=_parse_float(
                env.get("VOICE_STT_TOTAL_TIMEOUT"), 12.0, low=2.0, high=45.0
            ),
            get_msg_timeout=_parse_float(
                env.get("VOICE_STT_GET_MSG_TIMEOUT"), 4.5, low=1.0, high=7.5
            ),
            max_chars=_parse_int(env.get("VOICE_STT_MAX_CHARS"), 2000, low=100, high=8000),
            cache_ttl=_parse_float(env.get("VOICE_STT_CACHE_TTL"), 600.0, low=30.0, high=3600.0),
            cache_max=_parse_int(env.get("VOICE_STT_CACHE_MAX"), 512, low=32, high=4096),
            circuit_fail_threshold=_parse_int(
                env.get("VOICE_STT_CIRCUIT_FAIL_THRESHOLD"), 5, low=2, high=20
            ),
            circuit_cooldown=_parse_float(
                env.get("VOICE_STT_CIRCUIT_COOLDOWN"), 120.0, low=10.0, high=600.0
            ),
            prefix=env.get("VOICE_STT_PREFIX", "[语音转文字] "),
            failure_text=env.get("VOICE_STT_FAILURE_TEXT", "[语音消息：未能识别文字内容]"),
            rate_limited_text=env.get(
                "VOICE_STT_RATE_LIMITED_TEXT", "[语音消息：语音识别繁忙，暂未识别]"
            ),
            max_frame_bytes=_parse_int(
                env.get("RELAY_MAX_FRAME_BYTES"), 16_777_216, low=65_536, high=67_108_864
            ),
        )


def clean_transcript(raw: Any, *, max_chars: int = 2000) -> str:
    """Sanitize QQ STT output and prevent CQ-tag smuggling."""
    if not isinstance(raw, str):
        return ""
    cleaned = _CTRL_RE.sub("", raw)
    cleaned = _WS_COLLAPSE_RE.sub(" ", cleaned).strip()
    cleaned = _CQ_ESCAPE_RE.sub("［CQ:", cleaned)
    return cleaned[:max_chars].strip()


def message_has_record(message: Any) -> bool:
    """Return True when a OneBot message array or CQ string contains a voice segment."""
    if isinstance(message, list):
        return any(isinstance(seg, dict) and seg.get("type") == "record" for seg in message)
    if isinstance(message, str):
        return "[CQ:record" in message
    return False


def rewrite_message_records(message: Any, replacement_text: str) -> Any:
    """Replace record segments with plain text segments while preserving other segments."""
    if isinstance(message, list):
        rewritten: list[Any] = []
        for seg in message:
            if isinstance(seg, dict) and seg.get("type") == "record":
                rewritten.append({"type": "text", "data": {"text": replacement_text}})
            else:
                rewritten.append(seg)
        return rewritten
    if isinstance(message, str):
        return _CQ_RECORD_RE.sub(lambda _: replacement_text, message)
    return message


def normalize_message_id(raw_id: Any) -> int | str | None:
    """Normalize an OneBot message_id for JSON RPC parameters."""
    if isinstance(raw_id, bool) or raw_id is None:
        return None
    if isinstance(raw_id, int):
        return raw_id
    text = str(raw_id).strip()
    if not text or len(text) > 64:
        return None
    if re.fullmatch(r"-?[0-9]{1,20}", text):
        return int(text)
    return text


class _TranscriptCache:
    """Bounded in-memory TTL cache for transcribed voice message IDs."""

    def __init__(self, *, max_entries: int, ttl_seconds: float, clock: Callable[[], float]) -> None:
        self._max = max(1, int(max_entries))
        self._ttl = max(1.0, float(ttl_seconds))
        self._clock = clock
        self._items: OrderedDict[str, tuple[str, float]] = OrderedDict()

    def get(self, key: str) -> str | None:
        now = self._clock()
        entry = self._items.get(key)
        if entry is None:
            return None
        text, expires_at = entry
        if expires_at <= now:
            self._items.pop(key, None)
            return None
        self._items.move_to_end(key)
        return text

    def set(self, key: str, text: str) -> None:
        now = self._clock()
        self._items[key] = (text, now + self._ttl)
        self._items.move_to_end(key)
        while len(self._items) > self._max:
            self._items.popitem(last=False)


class _RateGuard:
    """Sliding-window per-user / global rate limiter and STT circuit breaker."""

    def __init__(self, config: RelayConfig, clock: Callable[[], float]) -> None:
        self._cfg = config
        self._clock = clock
        self._global_hits: deque[float] = deque()
        self._user_hits: dict[str, deque[float]] = {}
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def allow_request(self, user_id: str) -> tuple[bool, str]:
        now = self._clock()
        if self._circuit_open_until > now:
            return False, "circuit_open"
        cutoff = now - 60.0
        while self._global_hits and self._global_hits[0] <= cutoff:
            self._global_hits.popleft()
        if len(self._global_hits) >= self._cfg.global_rpm:
            return False, "global_rate_limit"

        if user_id:
            bucket = self._user_hits.setdefault(user_id, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= self._cfg.user_rpm:
                return False, "user_rate_limit"
            bucket.append(now)

        self._global_hits.append(now)
        if len(self._user_hits) > 512:
            stale = [uid for uid, dq in self._user_hits.items() if not dq or dq[-1] <= cutoff]
            for uid in stale:
                self._user_hits.pop(uid, None)
        return True, "ok"

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def record_failure(self) -> bool:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._cfg.circuit_fail_threshold:
            self._circuit_open_until = self._clock() + self._cfg.circuit_cooldown
            self._consecutive_failures = 0
            return True
        return False


class _BridgeSession:
    """Manages one bidirectional NapCat <-> Upstream WebSocket pair."""

    def __init__(
        self,
        client_ws: ServerConnection,
        upstream_ws: Any,
        config: RelayConfig,
        cache: _TranscriptCache,
        guard: _RateGuard,
        *,
        clock: Callable[[], float],
    ) -> None:
        self.client_ws = client_ws
        self.upstream_ws = upstream_ws
        self.config = config
        self.cache = cache
        self.guard = guard
        self.clock = clock
        self._client_send_lock = asyncio.Lock()
        self._upstream_send_lock = asyncio.Lock()
        self._stt_semaphore = asyncio.Semaphore(config.concurrency)
        self._pending_stt: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._inflight_stt: dict[str, asyncio.Task[str | None]] = {}
        self._pending_get_msg: dict[str, tuple[str, float]] = {}
        self._recalled_ids: OrderedDict[str, float] = OrderedDict()
        self._bg_tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def _spawn(self, coro: Any, *, name: str) -> None:
        if self._closed:
            if asyncio.iscoroutine(coro):
                coro.close()
            return
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._on_bg_done)

    def _on_bg_done(self, task: asyncio.Task[None]) -> None:
        self._bg_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.warning("event=relay_bg_error err=%s", type(exc).__name__)

    async def _send_client(self, raw: str) -> None:
        if self._closed:
            return
        async with self._client_send_lock:
            await self.client_ws.send(raw)

    async def _send_upstream(self, raw: str) -> None:
        if self._closed:
            return
        async with self._upstream_send_lock:
            await self.upstream_ws.send(raw)

    def _mark_recalled(self, msg_id: Any) -> None:
        key = str(msg_id or "").strip()
        if not key:
            return
        self._recalled_ids[key] = self.clock()
        self._recalled_ids.move_to_end(key)
        while len(self._recalled_ids) > 256:
            self._recalled_ids.popitem(last=False)

    def _is_recalled(self, msg_id: str) -> bool:
        return msg_id in self._recalled_ids

    def _event_in_scope(self, payload: dict[str, Any]) -> bool:
        if not self.config.stt_enabled:
            return False
        kind = payload.get("message_type")
        if kind == "private":
            return True
        if kind != "group":
            return False
        group_id = str(payload.get("group_id") or "").strip()
        if group_id in self.config.ignored_groups:
            return False
        if self.config.scope == "private":
            return False
        if self.config.scope == "allowlist":
            return group_id in self.config.allowed_groups
        return True

    async def _fetch_ptt_once(
        self, msg_id: int | str, wait_seconds: float
    ) -> tuple[bool, str, bool]:
        if len(self._pending_stt) >= 32:
            return False, "stt_backpressure", False
        echo = f"{INTERNAL_ECHO_PREFIX}{uuid.uuid4().hex}"
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending_stt[echo] = future
        req_frame = json.dumps(
            {
                "action": "fetch_ptt_text",
                "params": {"message_id": msg_id},
                "echo": echo,
            },
            ensure_ascii=False,
        )
        try:
            async with asyncio.timeout(max(0.2, wait_seconds)):
                await self._send_client(req_frame)
                resp = await future
        except TimeoutError:
            return False, "stt_timeout", False
        except asyncio.CancelledError:
            raise
        except Exception:
            return False, "stt_transport_error", False
        finally:
            self._pending_stt.pop(echo, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

        status = resp.get("status")
        retcode = resp.get("retcode")
        if status == "ok" and retcode == 0:
            data = resp.get("data")
            raw_text = data.get("text") if isinstance(data, dict) else data
            cleaned = clean_transcript(raw_text, max_chars=self.config.max_chars)
            if cleaned:
                return True, cleaned, False
            return False, "empty_transcript", False

        detail = f"{resp.get('message', '')} {resp.get('wording', '')}".strip()
        if retcode in {404, 1404} or any(phrase in detail for phrase in _PERMANENT_STT_PHRASES):
            return False, "stt_permanent_error", True
        return False, "stt_not_ready", False

    async def _run_transcribe(
        self,
        msg_id: int | str,
        *,
        total_wait_s: float,
        delays: tuple[float, ...],
    ) -> str | None:
        key = str(msg_id)
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        deadline = self.clock() + max(0.5, total_wait_s)
        async with self._stt_semaphore:
            cached = self.cache.get(key)
            if cached is not None:
                return cached
            last_reason = "stt_not_ready"
            max_attempts = 1 + len(delays)
            for attempt in range(max_attempts):
                if self._closed or self._is_recalled(key):
                    return None
                remaining = deadline - self.clock()
                if remaining <= 0.15:
                    last_reason = "stt_deadline"
                    break
                ok, value, permanent = await self._fetch_ptt_once(
                    msg_id, min(self.config.attempt_timeout, remaining)
                )
                if ok:
                    self.cache.set(key, value)
                    self.guard.record_success()
                    log.info("event=voice_stt_ok msg_id=%s attempt=%d", key, attempt + 1)
                    return value
                last_reason = value
                if permanent:
                    break
                if attempt < len(delays):
                    sleep_for = min(delays[attempt], max(0.0, deadline - self.clock() - 0.15))
                    if sleep_for <= 0.0:
                        break
                    await asyncio.sleep(sleep_for)

            opened = self.guard.record_failure()
            log.warning("event=voice_stt_failed msg_id=%s reason=%s", key, last_reason)
            if opened:
                log.warning(
                    "event=voice_stt_circuit_open cooldown=%ss",
                    int(self.config.circuit_cooldown),
                )
            return None

    async def _transcribe_coalesced(
        self,
        raw_msg_id: Any,
        *,
        total_wait_s: float,
        delays: tuple[float, ...],
    ) -> str | None:
        msg_id = normalize_message_id(raw_msg_id)
        if msg_id is None:
            return None
        key = str(msg_id)
        if cached := self.cache.get(key):
            return cached
        existing = self._inflight_stt.get(key)
        if existing is not None and not existing.done():
            return await asyncio.shield(existing)
        task = asyncio.create_task(
            self._run_transcribe(msg_id, total_wait_s=total_wait_s, delays=delays),
            name=f"stt-{key}",
        )
        self._inflight_stt[key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if self._inflight_stt.get(key) is task and task.done():
                self._inflight_stt.pop(key, None)

    def _apply_transcript(self, container: dict[str, Any], text: str | None, fallback: str) -> None:
        replacement = f"{self.config.prefix}{text}" if text else fallback
        container["message"] = rewrite_message_records(container.get("message"), replacement)
        if (
            isinstance(container.get("raw_message"), str)
            and "[CQ:record" in container["raw_message"]
        ):
            container["raw_message"] = rewrite_message_records(
                container["raw_message"], replacement
            )

    async def _process_voice_event(self, payload: dict[str, Any], raw_fallback: str) -> None:
        msg_id = str(payload.get("message_id") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()
        allowed, limit_reason = self.guard.allow_request(user_id)
        if not allowed:
            log.info("event=voice_stt_rate_limited reason=%s", limit_reason)
            self._apply_transcript(payload, None, self.config.rate_limited_text)
            await self._send_upstream(json.dumps(payload, ensure_ascii=False))
            return

        transcript = await self._transcribe_coalesced(
            payload.get("message_id"),
            total_wait_s=self.config.total_timeout,
            delays=self.config.retry_delays,
        )
        if msg_id and self._is_recalled(msg_id):
            return
        if not message_has_record(payload.get("message")):
            await self._send_upstream(raw_fallback)
            return
        self._apply_transcript(payload, transcript, self.config.failure_text)
        await self._send_upstream(json.dumps(payload, ensure_ascii=False))

    async def _process_get_msg_response(
        self, frame: dict[str, Any], req_msg_id: str, raw_fallback: str
    ) -> None:
        data = frame.get("data")
        if not isinstance(data, dict) or not message_has_record(data.get("message")):
            await self._send_upstream(raw_fallback)
            return

        target_id = data.get("message_id", req_msg_id)
        key = str(target_id or req_msg_id)
        cached = self.cache.get(key)
        if cached is not None:
            self._apply_transcript(data, cached, self.config.failure_text)
            log.info("event=voice_get_msg_rewritten msg_id=%s source=cache", key)
            await self._send_upstream(json.dumps(frame, ensure_ascii=False))
            return

        if not (self.config.stt_enabled and self.config.quote_stt_enabled):
            await self._send_upstream(raw_fallback)
            return

        uid = str(data.get("user_id") or (data.get("sender") or {}).get("user_id") or "")
        allowed, limit_reason = self.guard.allow_request(uid)
        if not allowed:
            log.info("event=voice_stt_rate_limited reason=%s", limit_reason)
            self._apply_transcript(data, None, self.config.rate_limited_text)
            await self._send_upstream(json.dumps(frame, ensure_ascii=False))
            return

        transcript = await self._transcribe_coalesced(
            target_id,
            total_wait_s=self.config.get_msg_timeout,
            delays=(0.6, 1.2),
        )
        self._apply_transcript(data, transcript, self.config.failure_text)
        log.info("event=voice_get_msg_rewritten msg_id=%s source=rpc", key)
        await self._send_upstream(json.dumps(frame, ensure_ascii=False))

    def _prune_pending_get_msg(self) -> None:
        cutoff = self.clock() - 30.0
        stale = [k for k, (_, ts) in self._pending_get_msg.items() if ts <= cutoff]
        for k in stale:
            self._pending_get_msg.pop(k, None)
        while len(self._pending_get_msg) > 256:
            oldest = next(iter(self._pending_get_msg))
            self._pending_get_msg.pop(oldest, None)

    async def _pump_upstream_to_client(self) -> None:
        async for raw in self.upstream_ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            if '"get_msg"' in raw:
                try:
                    frame = json.loads(raw)
                except ValueError:
                    frame = None
                if (
                    isinstance(frame, dict)
                    and frame.get("action") == "get_msg"
                    and frame.get("echo") is not None
                ):
                    params = frame.get("params")
                    msg_id = str(params.get("message_id", "")) if isinstance(params, dict) else ""
                    self._prune_pending_get_msg()
                    self._pending_get_msg[str(frame["echo"])] = (msg_id, self.clock())
            await self._send_client(raw)

    async def _pump_client_to_upstream(self) -> None:
        async for raw in self.client_ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            maybe_internal = INTERNAL_ECHO_PREFIX in raw
            maybe_get_msg = bool(self._pending_get_msg)
            maybe_recall = '"group_recall"' in raw or '"friend_recall"' in raw
            maybe_voice = '"record"' in raw or "[CQ:record" in raw
            if not (maybe_internal or maybe_get_msg or maybe_recall or maybe_voice):
                await self._send_upstream(raw)
                continue

            try:
                frame = json.loads(raw)
            except ValueError:
                await self._send_upstream(raw)
                continue
            if not isinstance(frame, dict):
                await self._send_upstream(raw)
                continue

            echo = frame.get("echo")
            if isinstance(echo, str) and echo.startswith(INTERNAL_ECHO_PREFIX):
                fut = self._pending_stt.get(echo)
                if fut is not None and not fut.done():
                    fut.set_result(frame)
                continue

            if echo is not None and str(echo) in self._pending_get_msg:
                req_msg_id, _ = self._pending_get_msg.pop(str(echo))
                data = frame.get("data")
                if (
                    frame.get("status") == "ok"
                    and frame.get("retcode") == 0
                    and isinstance(data, dict)
                    and message_has_record(data.get("message"))
                ):
                    self._spawn(
                        self._process_get_msg_response(frame, req_msg_id, raw),
                        name=f"voice-get-msg-{req_msg_id}",
                    )
                    continue
                await self._send_upstream(raw)
                continue

            if frame.get("post_type") == "notice" and frame.get("notice_type") in {
                "group_recall",
                "friend_recall",
            }:
                self._mark_recalled(frame.get("message_id"))
                await self._send_upstream(raw)
                continue

            if (
                frame.get("post_type") == "message"
                and str(frame.get("user_id", "")) != str(frame.get("self_id", ""))
                and message_has_record(frame.get("message"))
                and self._event_in_scope(frame)
            ):
                self._spawn(
                    self._process_voice_event(frame, raw),
                    name=f"voice-msg-{frame.get('message_id', 'unknown')}",
                )
                continue

            await self._send_upstream(raw)

    async def run(self) -> None:
        upstream_task = asyncio.create_task(
            self._pump_upstream_to_client(), name="relay-up-to-down"
        )
        client_task = asyncio.create_task(self._pump_client_to_upstream(), name="relay-down-to-up")
        try:
            done, _ = await asyncio.wait(
                [upstream_task, client_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                if not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        raise exc
        finally:
            self._closed = True
            for fut in list(self._pending_stt.values()):
                if not fut.done():
                    fut.cancel()
            self._pending_stt.clear()
            tasks = [upstream_task, client_task, *self._inflight_stt.values(), *self._bg_tasks]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


class VoiceRelayServer:
    """Asyncio WebSocket relay server bridging NapCat reverse WS to offshore Hakimi."""

    def __init__(
        self,
        config: RelayConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.clock = clock
        self.cache = _TranscriptCache(
            max_entries=config.cache_max,
            ttl_seconds=config.cache_ttl,
            clock=clock,
        )
        self.guard = _RateGuard(config, clock)
        self._server: Server | None = None
        self._active_sessions: set[_BridgeSession] = set()

    @property
    def bound_port(self) -> int | None:
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._server = await ws_serve(
            self._handle_client,
            self.config.listen_host,
            self.config.listen_port,
            ping_interval=None,
            ping_timeout=None,
            max_size=self.config.max_frame_bytes,
            max_queue=64,
            compression=None,
        )
        log.info(
            "event=relay_started port=%s scope=%s enabled=%s",
            self.bound_port,
            self.config.scope,
            self.config.stt_enabled,
        )

    async def close(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        for session in list(self._active_sessions):
            with contextlib.suppress(Exception):
                await session.client_ws.close(1001, "relay_shutdown")
            with contextlib.suppress(Exception):
                await session.upstream_ws.close(1001, "relay_shutdown")

    async def _handle_client(self, client_ws: ServerConnection) -> None:
        headers: dict[str, str] = {}
        req_headers = getattr(getattr(client_ws, "request", None), "headers", None)
        if req_headers is not None:
            for key in _FORWARD_HEADERS:
                val = req_headers.get(key)
                if isinstance(val, str) and val:
                    headers[key] = val
        self_id = headers.get("x-self-id", "unknown")

        upstream_connected = False
        try:
            async with ws_connect(
                self.config.upstream_url,
                additional_headers=headers,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=10.0,
                close_timeout=3.0,
                max_size=self.config.max_frame_bytes,
                max_queue=64,
                compression=None,
                proxy=None,
            ) as upstream_ws:
                upstream_connected = True
                session = _BridgeSession(
                    client_ws,
                    upstream_ws,
                    self.config,
                    self.cache,
                    self.guard,
                    clock=self.clock,
                )
                self._active_sessions.add(session)
                log.info("event=session_connected self_id=%s", self_id)
                try:
                    await session.run()
                finally:
                    self._active_sessions.discard(session)
                    with contextlib.suppress(Exception):
                        await client_ws.close(1000, "session_ended")
                    log.info("event=session_closed self_id=%s", self_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not upstream_connected:
                log.warning(
                    "event=upstream_connect_failed self_id=%s err=%s",
                    self_id,
                    type(exc).__name__,
                )
            with contextlib.suppress(Exception):
                await client_ws.close(1011, "upstream_unavailable")


async def serve_forever(config: RelayConfig | None = None) -> None:
    cfg = config or RelayConfig.from_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    server = VoiceRelayServer(cfg)
    await server.start()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)
    try:
        await stop_event.wait()
    finally:
        await server.close()


def main() -> None:
    asyncio.run(serve_forever())


if __name__ == "__main__":
    main()
