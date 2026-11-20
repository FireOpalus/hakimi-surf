"""Owner-private standalone screen-watching perception service (strictly RAM-only frames)."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import math
import re
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import anyio
from pydantic import BaseModel, ConfigDict, Field

from .media import ModelImage, normalize_image
from .models import Message
from .modules.base import sanitize_sensor_text
from .security import Rejected, clean_text, configured_secrets, safe_output

log = logging.getLogger("hakimi")

_CONFIG_KEY = "screen_watch_config"
_VAULT_CTX_CFG = "admin:screen_watch_config"
_MAX_BASE64_FRAME_CHARS = 2_800_000
_SILENT_TOKEN = "[SILENT]"


class ScreenWatchConfig(BaseModel):
    """Hot-configurable privacy and commentary settings for screen watching."""

    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = True
    proactive_commentary_enabled: bool = True
    min_pulse_interval_seconds: int = Field(default=25, ge=10, le=300)
    max_frames_per_query: int = Field(default=2, ge=1, le=2)
    frame_ttl_seconds: int = Field(default=15, ge=5, le=60)
    deliver_to_qq_when_no_voice: bool = False


def strip_markdown_noise(text: str) -> str:
    """Strip common Markdown formatting tokens from spoken/overlay output."""
    cleaned = text.replace("**", "").replace("__", "").replace("`", "")
    cleaned = re.sub(r"(?m)^[ 	]*#{1,6}[ 	]*", "", cleaned)
    cleaned = re.sub(r"(?m)^[ 	]*[-*+][ 	]+", "", cleaned)
    return cleaned


def decode_ws_json_frame(
    raw: str | bytes, limit: int, *, error_prefix: str = "screen"
) -> dict[str, Any]:
    """Decode and strictly validate one JSON object WebSocket frame."""
    try:
        size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
    except UnicodeError:
        raise Rejected(f"{error_prefix}_invalid_json") from None
    if size > limit:
        raise Rejected(f"{error_prefix}_frame_too_large")

    def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        for k, v in items:
            if k in obj:
                raise Rejected(f"{error_prefix}_invalid_json")
            obj[k] = v
        return obj

    try:
        val = json.loads(
            raw,
            object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(val, dict):
            raise ValueError
        return val
    except (ValueError, TypeError, RecursionError, UnicodeError):
        raise Rejected(f"{error_prefix}_invalid_json") from None


def decode_b64_frames(
    raw_frames: Any,
    max_frames: int,
    settings: Any,
    *,
    error_prefix: str = "screen",
) -> list[ModelImage]:
    """Decode up to 2 base64 image payloads into normalized in-memory ModelImages."""
    if not raw_frames:
        return []
    if not isinstance(raw_frames, list):
        raise Rejected(f"{error_prefix}_invalid_frames")
    if len(raw_frames) > 2:
        raise Rejected(f"{error_prefix}_too_many_frames")
    out: list[ModelImage] = []
    for item in raw_frames[: max(1, min(int(max_frames), 2))]:
        raw_b64 = item.get("data") if isinstance(item, dict) else item
        if not isinstance(raw_b64, str) or not raw_b64.strip():
            raise Rejected(f"{error_prefix}_invalid_frames")
        b64 = raw_b64.strip()
        if b64.startswith("data:"):
            _, _, b64 = b64.partition(",")
        elif b64.startswith("base64://"):
            b64 = b64[9:]
        if not b64 or len(b64) > _MAX_BASE64_FRAME_CHARS:
            raise Rejected(f"{error_prefix}_frame_too_large")
        try:
            decoded = base64.b64decode(b64, validate=True)
        except (ValueError, binascii.Error):
            raise Rejected(f"{error_prefix}_invalid_frames") from None
        if not decoded or len(decoded) > settings.image_max_bytes:
            raise Rejected(f"{error_prefix}_frame_too_large")
        out.append(
            normalize_image(
                decoded,
                settings.image_max_pixels,
                standard_max_edge=1024,
                tall_max_edge=1024,
            )
        )
    return out


@dataclass(frozen=True)
class _BufferedFrame:
    captured_at: float
    received_at: float
    game_title: str
    change_score: float
    image: ModelImage


@dataclass
class _ScreenPeer:
    receive: Callable[[], Any]
    send: Callable[[str], Any]
    close: Callable[[int], Any]
    runner: asyncio.Task[Any]
    source_ip: str
    adapter_id: str
    client_id: str
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    commentary_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    active_commentary_task: asyncio.Task[Any] | None = None
    active_commentary_cancel: asyncio.Event | None = None
    closed: bool = False


class ScreenWatch:
    """Owner-private visual perception hub with short-TTL RAM-only frame buffering."""

    def __init__(
        self,
        settings: Any,
        store: Any,
        service: Any,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.s = settings
        self.store = store
        self.service = service
        self.clock = clock
        self.config = ScreenWatchConfig()
        self.overridden = False
        self.initialized = False
        self.peer_lock = asyncio.Lock()
        self._peer: _ScreenPeer | None = None
        self._retired: set[asyncio.Task[Any]] = set()
        self._frames: deque[_BufferedFrame] = deque(maxlen=6)
        self._recent_commentaries: deque[dict[str, Any]] = deque(maxlen=12)
        self.source_ip: str | None = None
        self.connected_at: float | None = None
        self._paused: bool = False
        self._game_title: str = ""
        self._window_title: str = ""
        self._last_commentary_at: float = 0.0
        self.frames_received: int = 0
        self.routed_voice_count: int = 0
        self.standalone_commentary_count: int = 0

    async def initialize(self) -> None:
        if self.initialized:
            return
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM admin_config WHERE key=?", (_CONFIG_KEY,)
            )
            if row and row[0]:
                try:
                    data = self.store.vault.open(_VAULT_CTX_CFG, row[0])
                    self.config = ScreenWatchConfig.model_validate(data)
                    self.overridden = True
                except Exception:
                    self.config = ScreenWatchConfig()
                    self.overridden = False
        self.initialized = True

    def _prune_frames(self, now: float) -> None:
        ttl = float(self.config.frame_ttl_seconds)
        while self._frames and (now - self._frames[0].received_at) > ttl:
            self._frames.popleft()

    def clear_frames(self) -> int:
        """Immediately wipe all buffered screen frames from RAM."""
        count = len(self._frames)
        self._frames.clear()
        return count

    def note_spoken(self, now: float | None = None) -> None:
        """Reset proactive screen commentary cooldown after a spoken reply."""
        ts = self.clock() if now is None else float(now)
        self._last_commentary_at = max(self._last_commentary_at, ts)

    def latest_frames(
        self,
        *,
        max_age: float | None = None,
        limit: int | None = None,
        owner_only: bool = True,
        now: float | None = None,
    ) -> tuple[list[ModelImage], str]:
        """Return fresh RAM-buffered Owner screen frames (never exposed outside Owner scope)."""
        if not owner_only or not self.config.enabled or self._paused or not self.s.owner_qq_id:
            return [], ""
        ts = self.clock() if now is None else float(now)
        self._prune_frames(ts)
        if not self._frames:
            return [], self._game_title
        effective_ttl = float(self.config.frame_ttl_seconds)
        if max_age is not None:
            effective_ttl = min(effective_ttl, max(0.0, float(max_age)))
        cap = max(1, min(int(limit or self.config.max_frames_per_query), 2))
        eligible = [f for f in self._frames if (ts - f.received_at) <= effective_ttl]
        if not eligible:
            return [], self._game_title
        selected = eligible[-cap:]
        game = self._game_title or selected[-1].game_title
        return [f.image for f in selected], game

    def status_summary(self) -> dict[str, Any]:
        now = self.clock()
        self._prune_frames(now)
        peer = self._peer
        online = bool(peer is not None and peer.ready.is_set() and not peer.closed)
        return {
            "online": online,
            "ws_configured": bool(
                self.s.screen_ws_enabled and len(self.s.screen_ws_token.get_secret_value()) >= 32
            ),
            "paused": self._paused if online else False,
            "adapter_id": peer.adapter_id if online and peer else None,
            "client_id": peer.client_id if online and peer else None,
            "source_ip": peer.source_ip if online and peer else None,
            "connected_at": self.connected_at if online else None,
            "game_title": self._game_title if online else "",
            "window_title": self._window_title if online else "",
            "buffered_frames": len(self._frames),
            "frames_received": self.frames_received,
            "routed_voice_count": self.routed_voice_count,
            "standalone_commentary_count": self.standalone_commentary_count,
        }

    async def admin_status(self) -> dict[str, Any]:
        await self.initialize()
        return {
            **self.status_summary(),
            "timezone": self.s.bot_timezone,
            "source": "sqlite" if self.overridden else "default",
            "config": self.config.model_dump(),
            "recent_commentaries": list(self._recent_commentaries),
        }

    async def set_config(self, update: ScreenWatchConfig | None) -> dict[str, Any]:
        await self.initialize()
        if update is None:
            self.config = ScreenWatchConfig()
            self.overridden = False
            async with self.store.transaction():
                await self.store.db.execute("DELETE FROM admin_config WHERE key=?", (_CONFIG_KEY,))
        else:
            self.config = ScreenWatchConfig.model_validate(update.model_dump())
            self.overridden = True
            sealed = self.store.vault.seal(_VAULT_CTX_CFG, self.config.model_dump())
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO admin_config VALUES (?,?)",
                    (_CONFIG_KEY, sealed),
                )
        peer = self._peer
        if not self.config.enabled:
            self.clear_frames()
            if peer is not None:
                await self._cancel_active_commentary(peer)
        if peer is not None and peer.ready.is_set() and not peer.closed:
            await self._send_frame(
                peer, {"type": "config.sync", "config": self.config.model_dump()}
            )
        return await self.admin_status()

    async def clear_frames_admin(self) -> dict[str, Any]:
        await self.initialize()
        self.clear_frames()
        return await self.admin_status()

    async def disconnect_active(self, reason: str = "admin_disconnect") -> dict[str, Any]:
        await self.initialize()
        async with self.peer_lock:
            peer = self._peer
        if peer is not None:
            await self._retire_peer(peer, reason=reason)
        return await self.admin_status()

    async def close(self) -> None:
        self.clear_frames()
        async with self.peer_lock:
            peer = self._peer
            self._peer = None
        if peer is not None:
            await self._retire_peer(peer, reason="service_stop")
        retired = list(self._retired)
        if retired:
            await asyncio.gather(*retired, return_exceptions=True)

    async def _send_frame(self, peer: _ScreenPeer, frame: dict[str, Any]) -> None:
        if peer.closed:
            return
        payload = json.dumps(frame, ensure_ascii=False, separators=(",", ":"))
        async with peer.send_lock:
            if peer.closed:
                return
            try:
                async with asyncio.timeout(5.0):
                    await peer.send(payload)
            except Exception:
                pass

    async def _cancel_active_commentary(self, peer: _ScreenPeer) -> None:
        if peer.active_commentary_cancel is not None:
            peer.active_commentary_cancel.set()
        task = peer.active_commentary_task
        if task is not None and not task.done():
            task.cancel()
            try:
                async with asyncio.timeout(2.0):
                    await asyncio.gather(task, return_exceptions=True)
            except TimeoutError:
                pass
        peer.active_commentary_task = None
        peer.active_commentary_cancel = None

    async def _retire_peer(self, peer: _ScreenPeer, *, reason: str = "replaced") -> None:
        peer.closed = True
        peer.ready.clear()
        await self._cancel_active_commentary(peer)
        if peer is self._peer:
            self.clear_frames()
        try:
            async with asyncio.timeout(2.0):
                await peer.close(1012)
        except Exception:
            pass
        if peer.runner is not asyncio.current_task() and not peer.runner.done():
            peer.runner.cancel()

    def _make_owner_message(self, now: float) -> Message:
        return Message(
            platform="onebot",
            bot_id=self.s.onebot_self_id,
            event_id="sw-" + uuid.uuid4().hex[:12],
            user_id=self.s.owner_qq_id,
            kind="private",
            conversation_id=self.s.owner_qq_id,
            text="[独立看屏脉冲]",
            addressed=True,
            created_at=now,
            sender_nickname="BOISER",
        )

    async def _gather_owner_context(self, owner_msg: Message, now: float) -> dict[str, Any]:
        ctx: dict[str, Any] = {}
        notebook = getattr(self.service, "notebook", None)
        if notebook is not None and hasattr(notebook, "for_message"):
            try:
                snap = notebook.for_message(owner_msg)
                if snap:
                    ctx["owner_notebook_snapshot"] = str(snap)[:600]
            except Exception:
                pass
        ledger = getattr(self.service, "event_ledger", None)
        if ledger is not None and hasattr(ledger, "read"):
            try:
                items = await ledger.read(include_owner_private=True, limit=4, now=now)
                if items:
                    ctx["pending_ledger_events"] = [
                        {"key": it.dedupe_key, "summary": it.summary} for it in items[:4]
                    ]
            except Exception:
                pass
        ws = getattr(self.service, "world_state", None)
        if ws is not None and getattr(ws, "current", None) is not None:
            ctx["world_state"] = {
                "mood": ws.current.mood,
                "energy": ws.current.energy,
            }
        return ctx

    async def _run_standalone_commentary(
        self,
        peer: _ScreenPeer,
        *,
        tid: str,
        frames: list[ModelImage],
        game_title: str,
        window_title: str,
        change_score: float,
        cancel_event: asyncio.Event,
    ) -> None:
        secrets = (
            list(self.service.secrets)
            if getattr(self.service, "secrets", None)
            else configured_secrets(self.s)
        )
        now = self.clock()
        owner_msg = self._make_owner_message(now)
        payload: dict[str, Any] = {
            "event_kind": "screen_watch",
            "game_title": game_title,
            "window_title": window_title,
            "change_score": change_score,
            "allow_silent": True,
            "recent_commentaries": [c["text"] for c in list(self._recent_commentaries)[:4]],
        }
        payload.update(await self._gather_owner_context(owner_msg, now))

        try:
            raw = await self.service.ai.generate(
                payload,
                f"screen:owner:{self.s.owner_qq_id}",
                scenario="screen.watch",
                budget_lane="proactive",
                images=frames or None,
                message=owner_msg,
                audit_source="production",
                cancel_event=cancel_event,
            )
            if cancel_event.is_set():
                raise asyncio.CancelledError
            trimmed = str(raw or "").strip()
            if not trimmed or trimmed.upper() == _SILENT_TOKEN:
                await self._send_frame(
                    peer,
                    {
                        "type": "commentary.result",
                        "turn_id": tid,
                        "status": "silent",
                        "reason": "no_comment",
                    },
                )
                return

            clean_reply = safe_output(
                " ".join(clean_text(strip_markdown_noise(trimmed)).split()),
                secrets,
                limit=240,
            )
            delivered_qq = False
            proactive = getattr(self.service, "proactive", None)
            if self.config.deliver_to_qq_when_no_voice and proactive is not None:
                try:
                    await proactive.enqueue("screen_watch", clean_reply, expires=now + 300)
                    delivered_qq = True
                except Exception:
                    delivered_qq = False

            self.standalone_commentary_count += 1
            self._recent_commentaries.appendleft(
                {
                    "at": round(now, 3),
                    "game_title": game_title,
                    "channel": "qq_dm" if delivered_qq else "overlay",
                    "text": clean_reply,
                }
            )
            await self._send_frame(
                peer,
                {
                    "type": "commentary.ready",
                    "turn_id": tid,
                    "text": clean_reply,
                    "game_title": game_title,
                    "delivered_qq": delivered_qq,
                },
            )
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "sent_qq" if delivered_qq else "overlay",
                },
            )
        except asyncio.CancelledError:
            with anyio.CancelScope(shield=True):
                await self._send_frame(
                    peer,
                    {
                        "type": "commentary.result",
                        "turn_id": tid,
                        "status": "cancelled",
                        "reason": "watch_paused_or_stopped",
                    },
                )
            raise
        except Rejected as exc:
            code = str(exc).strip()
            reason = code if re.fullmatch(r"[a-z0-9_]{1,64}", code) else "screen_watch_rejected"
            await self._send_frame(
                peer,
                {"type": "commentary.result", "turn_id": tid, "status": "error", "reason": reason},
            )
        except Exception:
            if hasattr(self.service, "record"):
                self.service.record("screen_watch_pulse_failed")
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "error",
                    "reason": "screen_watch_pulse_failed",
                },
            )

    async def _handle_commentary_request(
        self,
        peer: _ScreenPeer,
        *,
        tid: str,
        change_score: float,
        now: float,
    ) -> None:
        if not self.s.owner_qq_id:
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "skipped",
                    "reason": "owner_not_configured",
                },
            )
            return
        if not self.config.proactive_commentary_enabled:
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "skipped",
                    "reason": "proactive_disabled",
                },
            )
            return
        if now - self._last_commentary_at < self.config.min_pulse_interval_seconds:
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "skipped",
                    "reason": "cooldown",
                },
            )
            return
        query_frames, effective_game = self.latest_frames(owner_only=True, now=now)
        if not query_frames:
            await self._send_frame(
                peer,
                {
                    "type": "commentary.result",
                    "turn_id": tid,
                    "status": "skipped",
                    "reason": "no_frames",
                },
            )
            return

        vc = getattr(self.service, "voice_companion", None)
        if (
            vc is not None
            and hasattr(vc, "has_active_owner_session")
            and vc.has_active_owner_session()
        ):
            outcome = await vc.trigger_screen_commentary(
                query_frames, game_title=effective_game, turn_id=tid
            )
            if outcome == "dispatched":
                self._last_commentary_at = now
                self.routed_voice_count += 1
                self._recent_commentaries.appendleft(
                    {
                        "at": round(now, 3),
                        "game_title": effective_game,
                        "channel": "voice_companion",
                        "text": f"[已转发至语音伴随口播 · turn {tid}]",
                    }
                )
                await self._send_frame(
                    peer,
                    {"type": "commentary.result", "turn_id": tid, "status": "routed_voice"},
                )
                return
            if outcome == "busy":
                await self._send_frame(
                    peer,
                    {
                        "type": "commentary.result",
                        "turn_id": tid,
                        "status": "skipped",
                        "reason": "voice_busy",
                    },
                )
                return

        async with peer.commentary_lock:
            if peer.active_commentary_task is not None and not peer.active_commentary_task.done():
                await self._send_frame(
                    peer,
                    {
                        "type": "commentary.result",
                        "turn_id": tid,
                        "status": "skipped",
                        "reason": "busy",
                    },
                )
                return
            self._last_commentary_at = now
            cancel_event = asyncio.Event()
            task = asyncio.create_task(
                self._run_standalone_commentary(
                    peer,
                    tid=tid,
                    frames=query_frames,
                    game_title=effective_game,
                    window_title=self._window_title,
                    change_score=change_score,
                    cancel_event=cancel_event,
                )
            )
            peer.active_commentary_task = task
            peer.active_commentary_cancel = cancel_event

    async def serve(
        self,
        receive: Callable[[], Any],
        send: Callable[[str], Any],
        close: Callable[[int], Any],
        *,
        source_ip: str = "unknown",
        adapter_id: str = "unknown",
        client_id: str = "unknown",
    ) -> None:
        await self.initialize()
        peer = _ScreenPeer(
            receive=receive,
            send=send,
            close=close,
            runner=asyncio.current_task(),  # type: ignore[arg-type]
            source_ip=source_ip,
            adapter_id=adapter_id,
            client_id=client_id,
        )
        async with self.peer_lock:
            old = self._peer
            self._peer = peer
            self.source_ip = source_ip
            self.connected_at = self.clock()
            self._paused = False
            self._game_title = ""
            self._window_title = ""
            self.clear_frames()
            if old is not None:
                retire = asyncio.create_task(self._retire_peer(old, reason="superseded"))
                self._retired.add(retire)
                retire.add_done_callback(self._retired.discard)
            peer.ready.set()
        if hasattr(self.service, "record"):
            self.service.record("screen_watch_connected")
        try:
            while not peer.closed:
                try:
                    raw = await receive()
                except Exception:
                    break
                frame = decode_ws_json_frame(
                    raw, self.s.screen_ws_max_frame_bytes, error_prefix="screen"
                )
                typ = str(frame.get("type") or "").strip()
                if typ == "ping":
                    await self._send_frame(peer, {"type": "pong", "ts": self.clock()})
                elif typ == "watch.start":
                    await self._cancel_active_commentary(peer)
                    now = self.clock()
                    self._paused = False
                    self._game_title = sanitize_sensor_text(
                        str(frame.get("game_title") or ""), self.s, max_tokens=16
                    )
                    self._window_title = sanitize_sensor_text(
                        str(frame.get("window_title") or ""), self.s, max_tokens=20
                    )
                    self._last_commentary_at = max(self._last_commentary_at, now)
                    self.clear_frames()
                    await self._send_frame(
                        peer,
                        {
                            "type": "watch.ready",
                            "game_title": self._game_title,
                            "window_title": self._window_title,
                            "paused": False,
                            "config": self.config.model_dump(),
                        },
                    )
                elif typ == "watch.pause":
                    self._paused = True
                    await self._cancel_active_commentary(peer)
                    cleared = self.clear_frames()
                    await self._send_frame(peer, {"type": "watch.paused", "cleared": cleared})
                elif typ == "watch.resume":
                    self._paused = False
                    self._last_commentary_at = max(self._last_commentary_at, self.clock())
                    await self._send_frame(peer, {"type": "watch.resumed"})
                elif typ == "watch.stop":
                    await self._cancel_active_commentary(peer)
                    cleared = self.clear_frames()
                    self._paused = False
                    self._game_title = ""
                    self._window_title = ""
                    await self._send_frame(peer, {"type": "watch.stopped", "cleared": cleared})
                elif typ == "frame.push":
                    want_commentary = bool(frame.get("request_commentary"))
                    raw_tid = str(frame.get("turn_id") or "").strip()
                    tid = (
                        raw_tid
                        if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", raw_tid)
                        else uuid.uuid4().hex[:12]
                    )
                    if not self.config.enabled:
                        self.clear_frames()
                        await self._send_frame(
                            peer,
                            {
                                "type": "frame.ack",
                                "accepted": 0,
                                "buffered": 0,
                                "reason": "screen_watch_disabled",
                            },
                        )
                        if want_commentary:
                            await self._send_frame(
                                peer,
                                {
                                    "type": "commentary.result",
                                    "turn_id": tid,
                                    "status": "skipped",
                                    "reason": "screen_watch_disabled",
                                },
                            )
                        continue
                    if self._paused:
                        self.clear_frames()
                        await self._send_frame(
                            peer,
                            {
                                "type": "frame.ack",
                                "accepted": 0,
                                "buffered": 0,
                                "reason": "screen_watch_paused",
                            },
                        )
                        if want_commentary:
                            await self._send_frame(
                                peer,
                                {
                                    "type": "commentary.result",
                                    "turn_id": tid,
                                    "status": "skipped",
                                    "reason": "screen_watch_paused",
                                },
                            )
                        continue
                    now = self.clock()
                    self._prune_frames(now)
                    try:
                        decoded = decode_b64_frames(
                            frame.get("frames"), 2, self.s, error_prefix="screen"
                        )
                    except Rejected as exc:
                        await self._send_frame(
                            peer,
                            {
                                "type": "frame.ack",
                                "accepted": 0,
                                "buffered": len(self._frames),
                                "reason": str(exc),
                            },
                        )
                        continue
                    if not decoded:
                        await self._send_frame(
                            peer,
                            {
                                "type": "frame.ack",
                                "accepted": 0,
                                "buffered": len(self._frames),
                                "reason": "empty_frames",
                            },
                        )
                        continue

                    if "game_title" in frame:
                        self._game_title = sanitize_sensor_text(
                            str(frame.get("game_title") or ""), self.s, max_tokens=16
                        )
                    if "window_title" in frame:
                        self._window_title = sanitize_sensor_text(
                            str(frame.get("window_title") or ""), self.s, max_tokens=20
                        )
                    raw_score = frame.get("change_score", 0.0)
                    change_score = (
                        float(raw_score)
                        if isinstance(raw_score, (int, float))
                        and not isinstance(raw_score, bool)
                        and math.isfinite(float(raw_score))
                        else 0.0
                    )
                    change_score = max(0.0, min(1.0, round(change_score, 4)))
                    raw_cap = frame.get("captured_at", now)
                    captured_at = (
                        float(raw_cap)
                        if isinstance(raw_cap, (int, float))
                        and not isinstance(raw_cap, bool)
                        and math.isfinite(float(raw_cap))
                        else now
                    )
                    for img in decoded:
                        self._frames.append(
                            _BufferedFrame(
                                captured_at=captured_at,
                                received_at=now,
                                game_title=self._game_title,
                                change_score=change_score,
                                image=img,
                            )
                        )
                    self._prune_frames(now)
                    self.frames_received += len(decoded)
                    await self._send_frame(
                        peer,
                        {
                            "type": "frame.ack",
                            "accepted": len(decoded),
                            "buffered": len(self._frames),
                        },
                    )
                    if want_commentary:
                        await self._handle_commentary_request(
                            peer, tid=tid, change_score=change_score, now=now
                        )
                else:
                    await self._send_frame(
                        peer, {"type": "error", "reason": "screen_unknown_frame"}
                    )
        except asyncio.CancelledError:
            raise
        except Rejected as exc:
            await self._send_frame(peer, {"type": "error", "reason": str(exc)})
        finally:
            peer.closed = True
            peer.ready.clear()
            await self._cancel_active_commentary(peer)
            with anyio.CancelScope(shield=True):
                async with self.peer_lock:
                    if peer is self._peer:
                        self._peer = None
                        self.source_ip = None
                        self._paused = False
                        self._game_title = ""
                        self._window_title = ""
                        self.clear_frames()
                if hasattr(self.service, "record"):
                    self.service.record("screen_watch_disconnected")
                try:
                    async with asyncio.timeout(2.0):
                        await close(1012)
                except Exception:
                    pass
