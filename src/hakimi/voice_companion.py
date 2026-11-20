"""Adapter-agnostic real-time voice & gameplay screen companion core."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import anyio
from pydantic import BaseModel, ConfigDict, Field

from .media import ModelImage, normalize_image
from .models import Message
from .modules.base import sanitize_sensor_text
from .security import Rejected, clean_text, configured_secrets, safe_output

log = logging.getLogger("hakimi")

SCHEMA = """
CREATE TABLE IF NOT EXISTS voice_companion_sessions (
  id TEXT PRIMARY KEY,
  adapter_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  game_title TEXT NOT NULL DEFAULT '',
  started_at REAL NOT NULL,
  ended_at REAL NOT NULL DEFAULT 0,
  turns INTEGER NOT NULL DEFAULT 0,
  spoken_turns INTEGER NOT NULL DEFAULT 0,
  frames_analyzed INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  body_blob BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_vc_sessions_started ON voice_companion_sessions(started_at DESC);
"""

_CONFIG_KEY = "voice_companion_config"
_VAULT_CTX_CFG = "admin:voice_companion_config"
_VAULT_CTX_SESSION = "voice-companion-session:"
_STRONG_ENDS = frozenset("。！？!?~～\n")
_SOFT_ENDS = frozenset("，,；;、")
_MAX_CHUNK_CHARS = 60
_SOFT_SPLIT_MIN_CHARS = 14
_MAX_BASE64_FRAME_CHARS = 2_800_000
_SILENT_TOKEN = "[SILENT]"


class VoiceCompanionConfig(BaseModel):
    """Hot-configurable settings for the built-in voice companion hub."""

    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = True
    proactive_watch_enabled: bool = True
    min_pulse_interval_seconds: int = Field(default=25, ge=10, le=300)
    max_frames_per_turn: int = Field(default=2, ge=1, le=2)
    save_recap_to_ledger: bool = True
    tts_style_hint: str = Field(default="活泼软萌、口语短句", max_length=80)


def _strip_markdown_noise(text: str) -> str:
    cleaned = text.replace("**", "").replace("__", "").replace("`", "")
    cleaned = re.sub(r"(?m)^[ 	]*#{1,6}[ 	]*", "", cleaned)
    cleaned = re.sub(r"(?m)^[ 	]*[-*+][ 	]+", "", cleaned)
    return cleaned


class SentenceStreamBuffer:
    """Incrementally slices streamed LLM text into natural, secret-scrubbed TTS sentences."""

    def __init__(self, secrets: list[str], *, allow_silent: bool = False) -> None:
        self._secrets = list(secrets)
        self._allow_silent = allow_silent
        self._buf = ""
        self._silent = False
        self._started_speaking = False

    @property
    def silent(self) -> bool:
        return self._silent

    def _emit_clean(self, raw: str) -> list[str]:
        normalized = " ".join(clean_text(_strip_markdown_noise(raw)).split()).strip()
        if not normalized:
            return []
        scrubbed = safe_output(normalized, self._secrets, limit=300).strip()
        if not scrubbed:
            return []
        self._started_speaking = True
        return [scrubbed]

    def _take_ready_chunks(self) -> list[str]:
        out: list[str] = []
        while self._buf:
            cut = -1
            for idx, ch in enumerate(self._buf):
                if ch in _STRONG_ENDS:
                    if sum(1 for c in self._buf[: idx + 1] if not c.isspace()) >= 2:
                        cut = idx + 1
                        break
                elif ch in _SOFT_ENDS and idx + 1 >= _SOFT_SPLIT_MIN_CHARS:
                    cut = idx + 1
                    break
            if cut == -1 and len(self._buf) >= _MAX_CHUNK_CHARS:
                space_idx = self._buf.rfind(" ", 0, _MAX_CHUNK_CHARS)
                cut = space_idx + 1 if space_idx >= 12 else _MAX_CHUNK_CHARS
            if cut == -1:
                break
            piece, self._buf = self._buf[:cut], self._buf[cut:].lstrip()
            out.extend(self._emit_clean(piece))
        return out

    def feed(self, delta: str) -> list[str]:
        """Accept one streamed text delta and return zero or more complete spoken chunks."""
        if not delta or self._silent:
            return []
        self._buf += _strip_markdown_noise(delta)
        if self._allow_silent and not self._started_speaking:
            probe = self._buf.strip()
            if not probe:
                return []
            upper = probe.upper()
            if upper == _SILENT_TOKEN:
                self._silent = True
                self._buf = ""
                return []
            if len(probe) < len(_SILENT_TOKEN) + 2 and _SILENT_TOKEN.startswith(upper):
                return []
            if upper.startswith(_SILENT_TOKEN):
                self._silent = True
                self._buf = ""
                return []
        return self._take_ready_chunks()

    def flush(self) -> list[str]:
        """Flush remaining buffered text at turn end."""
        if self._silent:
            self._buf = ""
            return []
        probe = self._buf.strip()
        if self._allow_silent and not self._started_speaking and probe.upper() == _SILENT_TOKEN:
            self._silent = True
            self._buf = ""
            return []
        out = self._take_ready_chunks()
        if self._buf.strip():
            out.extend(self._emit_clean(self._buf))
        self._buf = ""
        return out


def decode_companion_frame(raw: str | bytes, limit: int) -> dict[str, Any]:
    """Decode and validate one incoming companion WS JSON object frame."""
    try:
        size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
    except UnicodeError:
        raise Rejected("companion_invalid_json") from None
    if size > limit:
        raise Rejected("companion_frame_too_large")

    def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        for k, v in items:
            if k in obj:
                raise Rejected("companion_invalid_json")
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
        raise Rejected("companion_invalid_json") from None


def _decode_frames(raw_frames: Any, max_frames: int, settings: Any) -> list[ModelImage]:
    if not raw_frames:
        return []
    if not isinstance(raw_frames, list):
        raise Rejected("companion_invalid_frames")
    if len(raw_frames) > 2:
        raise Rejected("companion_too_many_frames")
    out: list[ModelImage] = []
    for item in raw_frames[: max(1, min(int(max_frames), 2))]:
        raw_b64 = item.get("data") if isinstance(item, dict) else item
        if not isinstance(raw_b64, str) or not raw_b64.strip():
            raise Rejected("companion_invalid_frames")
        b64 = raw_b64.strip()
        if b64.startswith("data:"):
            _, _, b64 = b64.partition(",")
        elif b64.startswith("base64://"):
            b64 = b64[9:]
        if not b64 or len(b64) > _MAX_BASE64_FRAME_CHARS:
            raise Rejected("companion_frame_too_large")
        try:
            decoded = base64.b64decode(b64, validate=True)
        except (ValueError, binascii.Error):
            raise Rejected("companion_invalid_frames") from None
        if not decoded or len(decoded) > settings.image_max_bytes:
            raise Rejected("companion_frame_too_large")
        out.append(
            normalize_image(
                decoded,
                settings.image_max_pixels,
                standard_max_edge=1024,
                tall_max_edge=1024,
            )
        )
    return out


@dataclass
class _CompanionSession:
    id: str
    adapter_id: str
    client_id: str
    mode: Literal["owner_private", "room_public"]
    game_title: str
    started_at: float
    turns: int = 0
    spoken_turns: int = 0
    frames_analyzed: int = 0
    last_pulse_at: float = 0.0
    finalized: bool = False
    recent_turns: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=16))
    highlights: deque[str] = field(default_factory=lambda: deque(maxlen=8))


@dataclass
class _CompanionPeer:
    receive: Callable[[], Any]
    send: Callable[[str], Any]
    close: Callable[[int], Any]
    runner: asyncio.Task[Any]
    source_ip: str
    adapter_id: str
    client_id: str
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    turn_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    active_turn_task: asyncio.Task[Any] | None = None
    active_turn_cancel: asyncio.Event | None = None
    active_turn_id: str = ""
    session: _CompanionSession | None = None
    closed: bool = False


class VoiceCompanion:
    """Manages authenticated companion WS sessions, sentence streaming, and encrypted recaps."""

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
        self.config = VoiceCompanionConfig()
        self.overridden = False
        self.initialized = False
        self.peer_lock = asyncio.Lock()
        self._peer: _CompanionPeer | None = None
        self._retired: set[asyncio.Task[Any]] = set()
        self.source_ip: str | None = None
        self.connected_at: float | None = None

    async def initialize(self) -> None:
        if self.initialized:
            return
        async with self.store.lock:
            await self.store.db.executescript(SCHEMA)
            row = await self.store.one(
                "SELECT payload FROM admin_config WHERE key=?", (_CONFIG_KEY,)
            )
            if row and row[0]:
                try:
                    data = self.store.vault.open(_VAULT_CTX_CFG, row[0])
                    self.config = VoiceCompanionConfig.model_validate(data)
                    self.overridden = True
                except Exception:
                    self.config = VoiceCompanionConfig()
                    self.overridden = False
        self.initialized = True

    def status_summary(self) -> dict[str, Any]:
        peer = self._peer
        online = bool(peer is not None and peer.ready.is_set() and not peer.closed)
        sess = peer.session if (online and peer is not None) else None
        return {
            "online": online,
            "ws_configured": bool(
                self.s.companion_ws_enabled
                and len(self.s.companion_ws_token.get_secret_value()) >= 32
            ),
            "adapter_id": peer.adapter_id if online and peer else None,
            "client_id": peer.client_id if online and peer else None,
            "source_ip": peer.source_ip if online and peer else None,
            "connected_at": self.connected_at if online else None,
            "active_session": (
                {
                    "id": sess.id,
                    "mode": sess.mode,
                    "game_title": sess.game_title,
                    "started_at": sess.started_at,
                    "turns": sess.turns,
                    "spoken_turns": sess.spoken_turns,
                    "frames_analyzed": sess.frames_analyzed,
                }
                if sess is not None
                else None
            ),
        }

    async def admin_status(self) -> dict[str, Any]:
        await self.initialize()
        summary = self.status_summary()
        sessions: list[dict[str, Any]] = []
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT id, adapter_id, client_id, mode, game_title, started_at, ended_at, "
                "turns, spoken_turns, frames_analyzed, status, body_blob "
                "FROM voice_companion_sessions ORDER BY started_at DESC LIMIT 20"
            )
        for row in rows:
            sid = row["id"]
            body: dict[str, Any] = {}
            try:
                opened = self.store.vault.open(_VAULT_CTX_SESSION + sid, row["body_blob"])
                if isinstance(opened, dict):
                    body = opened
            except Exception:
                body = {"summary": ""}
            sessions.append(
                {
                    "id": sid,
                    "adapter_id": row["adapter_id"],
                    "client_id": row["client_id"],
                    "mode": row["mode"],
                    "game_title": row["game_title"],
                    "started_at": row["started_at"],
                    "ended_at": row["ended_at"],
                    "turns": int(row["turns"]),
                    "spoken_turns": int(row["spoken_turns"]),
                    "frames_analyzed": int(row["frames_analyzed"]),
                    "status": row["status"],
                    "summary": str(body.get("summary") or ""),
                }
            )
        return {
            **summary,
            "timezone": self.s.bot_timezone,
            "source": "sqlite" if self.overridden else "default",
            "config": self.config.model_dump(),
            "sessions": sessions,
        }

    async def set_config(self, update: VoiceCompanionConfig | None) -> dict[str, Any]:
        await self.initialize()
        if update is None:
            self.config = VoiceCompanionConfig()
            self.overridden = False
            async with self.store.transaction():
                await self.store.db.execute("DELETE FROM admin_config WHERE key=?", (_CONFIG_KEY,))
        else:
            self.config = VoiceCompanionConfig.model_validate(update.model_dump())
            self.overridden = True
            sealed = self.store.vault.seal(_VAULT_CTX_CFG, self.config.model_dump())
            async with self.store.transaction():
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO admin_config VALUES (?,?)",
                    (_CONFIG_KEY, sealed),
                )
        peer = self._peer
        if peer is not None and peer.ready.is_set() and not peer.closed:
            await self._send_frame(
                peer, {"type": "config.sync", "config": self.config.model_dump()}
            )
        return await self.admin_status()

    async def disconnect_active(self, reason: str = "admin_disconnect") -> dict[str, Any]:
        await self.initialize()
        async with self.peer_lock:
            peer = self._peer
        if peer is not None:
            await self._retire_peer(peer, reason=reason)
        return await self.admin_status()

    async def close(self) -> None:
        async with self.peer_lock:
            peer = self._peer
            self._peer = None
        if peer is not None:
            await self._retire_peer(peer, reason="service_stop")
        retired = list(self._retired)
        if retired:
            await asyncio.gather(*retired, return_exceptions=True)

    async def _send_frame(self, peer: _CompanionPeer, frame: dict[str, Any]) -> None:
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

    async def _retire_peer(self, peer: _CompanionPeer, *, reason: str = "replaced") -> None:
        peer.closed = True
        peer.ready.clear()
        if peer.active_turn_cancel is not None:
            peer.active_turn_cancel.set()
        if peer.active_turn_task is not None and not peer.active_turn_task.done():
            peer.active_turn_task.cancel()
        if peer.session is not None and not peer.session.finalized:
            await self._finalize_session(peer.session, status=reason)
        try:
            async with asyncio.timeout(2.0):
                await peer.close(1012)
        except Exception:
            pass
        if peer.runner is not asyncio.current_task() and not peer.runner.done():
            peer.runner.cancel()

    async def _persist_session_row(
        self, session: _CompanionSession, *, ended_at: float, status: str, summary: str
    ) -> None:
        await self.initialize()
        blob = self.store.vault.seal(
            _VAULT_CTX_SESSION + session.id,
            {"summary": summary, "highlights": list(session.highlights)},
        )
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT OR REPLACE INTO voice_companion_sessions "
                "(id, adapter_id, client_id, mode, game_title, started_at, ended_at, "
                "turns, spoken_turns, frames_analyzed, status, body_blob) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    session.id,
                    session.adapter_id,
                    session.client_id,
                    session.mode,
                    session.game_title,
                    session.started_at,
                    ended_at,
                    session.turns,
                    session.spoken_turns,
                    session.frames_analyzed,
                    status,
                    blob,
                ),
            )

    def _build_session_recap(self, session: _CompanionSession, ended_at: float) -> str:
        secrets = (
            list(self.service.secrets)
            if getattr(self.service, "secrets", None)
            else configured_secrets(self.s)
        )
        mins = max(1, round(max(0.0, ended_at - session.started_at) / 60.0))
        title = session.game_title or "陪玩连麦"
        head = f"「{title}」语音伴随 {mins} 分钟（对话 {session.spoken_turns} 轮"
        if session.frames_analyzed:
            head += f"，看屏 {session.frames_analyzed} 帧"
        head += "）"
        if session.highlights:
            tail = "；".join(list(session.highlights)[-3:])
            raw = f"{head}：{tail}"
        else:
            raw = head
        cleaned = " ".join(clean_text(raw).split())
        return safe_output(cleaned, secrets, limit=180)

    async def _finalize_session(self, session: _CompanionSession, *, status: str = "ended") -> None:
        if session.finalized:
            return
        session.finalized = True
        now = self.clock()
        summary = self._build_session_recap(session, now)
        try:
            await self._persist_session_row(session, ended_at=now, status=status, summary=summary)
        except Exception:
            pass

        ws = getattr(self.service, "world_state", None)
        if ws is not None and getattr(ws, "current", None) is not None:
            if ws.current.current_module == "voice_companion":
                try:
                    await ws.update_state(
                        current_module="idle",
                        focus_level="LOW",
                        mood="calm",
                        details={},
                    )
                except Exception:
                    pass

        if session.spoken_turns < 1:
            return
        if session.mode == "owner_private":
            ledger = getattr(self.service, "event_ledger", None)
            if self.config.save_recap_to_ledger and ledger is not None:
                try:
                    await ledger.post(
                        "voice_companion",
                        "companion_recap",
                        summary,
                        dedupe_key=f"recap:{session.id}",
                        visibility="owner_private",
                        due_at=now,
                        ttl_seconds=48 * 3600.0,
                        now=now,
                    )
                except Exception:
                    pass
        elif ws is not None and hasattr(ws, "publish_event"):
            try:
                await ws.publish_event(
                    "voice_companion",
                    "session_ended",
                    {
                        "adapter": session.adapter_id,
                        "turns": session.spoken_turns,
                    },
                    importance="low",
                    now=now,
                )
            except Exception:
                pass

    async def _gather_owner_context(
        self, owner_msg: Message, speech_text: str, now: float
    ) -> dict[str, Any]:
        ctx: dict[str, Any] = {}
        notebook = getattr(self.service, "notebook", None)
        if notebook is not None and hasattr(notebook, "for_message"):
            try:
                snap = notebook.for_message(owner_msg)
                if snap:
                    ctx["owner_notebook_snapshot"] = str(snap)[:800]
            except Exception:
                pass
        cloud_mem = getattr(self.service, "cloud_memory", None)
        if cloud_mem is not None and hasattr(cloud_mem, "for_message"):
            try:
                mem = await asyncio.to_thread(
                    cloud_mem.for_message, owner_msg, speech_text, recall=False
                )
                if isinstance(mem, dict) and mem:
                    for key in ("owner_core_memory", "owner_recalled_memories"):
                        if val := mem.get(key):
                            ctx[key] = val
            except Exception:
                pass
        sessions = getattr(self.service, "sessions", None)
        if sessions is not None and hasattr(sessions, "database") and hasattr(sessions, "vault"):
            path_fn = getattr(sessions, "path", None)
            if not callable(path_fn) or path_fn(owner_msg).exists():
                try:
                    async with sessions.database(owner_msg, preserve_history=True) as (db, key):
                        async with db.execute(
                            "SELECT payload FROM recent_messages ORDER BY id DESC LIMIT 6"
                        ) as cur:
                            rows = await cur.fetchall()
                    turns: list[str] = []
                    for row in reversed(rows):
                        item = sessions.vault.open(key, row[0])
                        txt = str(item.get("text") or "").strip()
                        if not txt or txt.startswith(("[协议事件", "/")):
                            continue
                        prefix = "BOISER: " if item.get("role") == "user" else "我: "
                        turns.append(prefix + clean_text(txt)[:140])
                    if turns:
                        ctx["recent_qq_turns"] = turns[-6:]
                except Exception:
                    pass
        ledger = getattr(self.service, "event_ledger", None)
        if ledger is not None and hasattr(ledger, "read"):
            try:
                items = await ledger.read(include_owner_private=True, limit=6, now=now)
                if items:
                    ctx["pending_ledger_events"] = [
                        {"key": it.dedupe_key, "summary": it.summary} for it in items[:6]
                    ]
            except Exception:
                pass
        return ctx

    def _make_turn_message(
        self,
        session: _CompanionSession,
        speaker_id: str,
        speaker_name: str,
        text: str,
        now: float,
    ) -> Message:
        if session.mode == "owner_private" and self.s.owner_qq_id:
            return Message(
                platform="onebot",
                bot_id=self.s.onebot_self_id,
                event_id="vc-" + uuid.uuid4().hex[:12],
                user_id=self.s.owner_qq_id,
                kind="private",
                conversation_id=self.s.owner_qq_id,
                text=text or "[看屏观察]",
                addressed=True,
                created_at=now,
                sender_nickname="BOISER",
            )
        safe_speaker = re.sub(r"[^A-Za-z0-9_.-]", "_", speaker_id)[:64] or "member"
        return Message(
            platform="companion",
            bot_id=session.adapter_id,
            event_id="vc-" + uuid.uuid4().hex[:12],
            user_id=safe_speaker,
            kind="group",
            conversation_id=f"{session.adapter_id}:{session.id}",
            text=text or "[看屏观察]",
            addressed=True,
            created_at=now,
            sender_nickname=sanitize_sensor_text(speaker_name, self.s, max_tokens=16),
        )

    async def _run_turn(
        self,
        peer: _CompanionPeer,
        session: _CompanionSession,
        *,
        turn_id: str,
        event_kind: Literal["speech", "screen_pulse"],
        speech_text: str,
        speaker_id: str,
        speaker_name: str,
        frames: list[ModelImage],
        cancel_event: asyncio.Event,
    ) -> None:
        secrets = (
            list(self.service.secrets)
            if getattr(self.service, "secrets", None)
            else configured_secrets(self.s)
        )
        allow_silent = event_kind == "screen_pulse"
        is_owner = session.mode == "owner_private" and bool(self.s.owner_qq_id)
        now = self.clock()
        session.turns += 1
        session.frames_analyzed += len(frames)

        msg = self._make_turn_message(session, speaker_id, speaker_name, speech_text, now)
        payload: dict[str, Any] = {
            "event_kind": event_kind,
            "game_title": session.game_title,
            "allow_silent": allow_silent,
            "tts_style_hint": self.config.tts_style_hint,
            "recent_voice_turns": list(session.recent_turns),
        }
        if event_kind == "speech":
            payload["speech_text"] = speech_text
        if not is_owner:
            payload["speaker_id"] = msg.user_id
            if msg.sender_nickname:
                payload["speaker_name"] = msg.sender_nickname
        else:
            ws = getattr(self.service, "world_state", None)
            if ws is not None and getattr(ws, "current", None) is not None:
                payload["world_state"] = {
                    "mood": ws.current.mood,
                    "energy": ws.current.energy,
                }
            payload.update(await self._gather_owner_context(msg, speech_text, now))

        buffer = SentenceStreamBuffer(secrets, allow_silent=allow_silent)
        seq = 0

        async def emit_chunk(chunk_text: str) -> None:
            nonlocal seq
            if cancel_event.is_set():
                return
            seq += 1
            await self._send_frame(
                peer,
                {
                    "type": "speak.chunk",
                    "turn_id": turn_id,
                    "seq": seq,
                    "text": chunk_text,
                    "final": False,
                },
            )

        streamed_any = False

        async def on_delta(delta: str) -> None:
            nonlocal streamed_any
            if delta:
                streamed_any = True
            for chunk in buffer.feed(delta):
                await emit_chunk(chunk)

        await self._send_frame(peer, {"type": "turn.start", "turn_id": turn_id, "kind": event_kind})
        subject = (
            f"voice:owner:{self.s.owner_qq_id}"
            if is_owner
            else f"voice:{session.adapter_id}:{session.id}"
        )
        budget_lane = "proactive" if allow_silent else "chat"
        try:
            full_text = await self.service.ai.generate(
                payload,
                subject,
                scenario="voice.companion",
                budget_lane=budget_lane,
                images=frames or None,
                message=msg,
                audit_source="production",
                cancel_event=cancel_event,
                on_text=on_delta if is_owner else None,
            )
            if cancel_event.is_set():
                raise asyncio.CancelledError
            if (not is_owner or not streamed_any) and full_text:
                for chunk in buffer.feed(full_text):
                    await emit_chunk(chunk)
            for chunk in buffer.flush():
                await emit_chunk(chunk)
            spoken = not buffer.silent and full_text.strip().upper() != _SILENT_TOKEN and seq > 0
            if spoken:
                session.spoken_turns += 1
                session.last_pulse_at = max(session.last_pulse_at, now)
                clean_reply = safe_output(
                    " ".join(clean_text(_strip_markdown_noise(full_text)).split()),
                    secrets,
                    limit=240,
                )
                entry: dict[str, Any] = {"at": round(now)}
                if speech_text:
                    entry["user"] = speech_text[:120]
                entry["assistant"] = clean_reply
                session.recent_turns.append(entry)
                if event_kind == "speech" and speech_text:
                    session.highlights.append(f"{speech_text[:40]} -> {clean_reply[:60]}")
                elif clean_reply:
                    session.highlights.append(clean_reply[:70])
                await self._send_frame(
                    peer, {"type": "turn.end", "turn_id": turn_id, "status": "spoken"}
                )
            else:
                if event_kind == "speech" and speech_text:
                    session.recent_turns.append({"at": round(now), "user": speech_text[:120]})
                await self._send_frame(
                    peer,
                    {
                        "type": "turn.end",
                        "turn_id": turn_id,
                        "status": "silent",
                        "reason": "no_comment",
                    },
                )
        except asyncio.CancelledError:
            with anyio.CancelScope(shield=True):
                await self._send_frame(
                    peer,
                    {
                        "type": "turn.end",
                        "turn_id": turn_id,
                        "status": "cancelled",
                        "reason": "barge_in",
                    },
                )
            raise
        except Rejected as exc:
            code = str(exc).strip()
            reason = code if re.fullmatch(r"[a-z0-9_]{1,64}", code) else "companion_rejected"
            await self._send_frame(
                peer,
                {"type": "turn.end", "turn_id": turn_id, "status": "error", "reason": reason},
            )
        except Exception:
            if hasattr(self.service, "record"):
                self.service.record("companion_turn_failed")
            await self._send_frame(
                peer,
                {
                    "type": "turn.end",
                    "turn_id": turn_id,
                    "status": "error",
                    "reason": "companion_turn_failed",
                },
            )

    async def _cancel_active_turn(self, peer: _CompanionPeer) -> None:
        if peer.active_turn_cancel is not None:
            peer.active_turn_cancel.set()
        task = peer.active_turn_task
        if task is not None and not task.done():
            task.cancel()
            try:
                async with asyncio.timeout(2.0):
                    await asyncio.gather(task, return_exceptions=True)
            except TimeoutError:
                pass
        peer.active_turn_task = None
        peer.active_turn_cancel = None
        peer.active_turn_id = ""

    async def _start_turn(
        self,
        peer: _CompanionPeer,
        session: _CompanionSession,
        *,
        turn_id: str,
        event_kind: Literal["speech", "screen_pulse"],
        speech_text: str,
        speaker_id: str,
        speaker_name: str,
        frames: list[ModelImage],
    ) -> None:
        async with peer.turn_lock:
            if event_kind == "speech":
                await self._cancel_active_turn(peer)
            elif peer.active_turn_task is not None and not peer.active_turn_task.done():
                await self._send_frame(
                    peer,
                    {"type": "turn.end", "turn_id": turn_id, "status": "skipped", "reason": "busy"},
                )
                return
            cancel_event = asyncio.Event()
            task = asyncio.create_task(
                self._run_turn(
                    peer,
                    session,
                    turn_id=turn_id,
                    event_kind=event_kind,
                    speech_text=speech_text,
                    speaker_id=speaker_id,
                    speaker_name=speaker_name,
                    frames=frames,
                    cancel_event=cancel_event,
                )
            )
            peer.active_turn_task = task
            peer.active_turn_cancel = cancel_event
            peer.active_turn_id = turn_id

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
        peer = _CompanionPeer(
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
            if old is not None:
                retire = asyncio.create_task(self._retire_peer(old, reason="superseded"))
                self._retired.add(retire)
                retire.add_done_callback(self._retired.discard)
            peer.ready.set()
        if hasattr(self.service, "record"):
            self.service.record("companion_connected")
        try:
            while not peer.closed:
                try:
                    raw = await receive()
                except Exception:
                    break
                frame = decode_companion_frame(raw, self.s.companion_ws_max_frame_bytes)
                typ = str(frame.get("type") or "").strip()
                if typ == "ping":
                    await self._send_frame(peer, {"type": "pong", "ts": self.clock()})
                elif typ == "session.start":
                    await self._cancel_active_turn(peer)
                    if peer.session is not None and not peer.session.finalized:
                        await self._finalize_session(peer.session, status="restarted")
                    req_mode = str(frame.get("mode") or "owner_private").strip()
                    mode: Literal["owner_private", "room_public"] = (
                        "owner_private"
                        if req_mode == "owner_private" and bool(self.s.owner_qq_id)
                        else "room_public"
                    )
                    game_title = sanitize_sensor_text(
                        str(frame.get("game_title") or ""), self.s, max_tokens=16
                    )
                    now = self.clock()
                    sess = _CompanionSession(
                        id=uuid.uuid4().hex,
                        adapter_id=peer.adapter_id,
                        client_id=peer.client_id,
                        mode=mode,
                        game_title=game_title,
                        started_at=now,
                        last_pulse_at=now,
                    )
                    peer.session = sess
                    ws = getattr(self.service, "world_state", None)
                    if ws is not None and hasattr(ws, "update_state"):
                        try:
                            await ws.update_state(
                                current_module="voice_companion",
                                focus_level="HIGH",
                                mood="excited",
                                details={"adapter": peer.adapter_id, "mode": mode},
                            )
                        except Exception:
                            pass
                    await self._persist_session_row(
                        sess,
                        ended_at=0.0,
                        status="active",
                        summary=f"正在游玩 {game_title}" if game_title else "伴随连麦进行中",
                    )
                    await self._send_frame(
                        peer,
                        {
                            "type": "session.ready",
                            "session_id": sess.id,
                            "mode": sess.mode,
                            "config": self.config.model_dump(),
                        },
                    )
                elif typ == "turn.cancel":
                    target_id = str(frame.get("turn_id") or "").strip()
                    if not target_id or target_id == peer.active_turn_id:
                        await self._cancel_active_turn(peer)
                elif typ in {"speech.turn", "screen.pulse"}:
                    sess = peer.session
                    if sess is None or sess.finalized:
                        await self._send_frame(
                            peer,
                            {"type": "error", "reason": "companion_session_not_started"},
                        )
                        continue
                    raw_tid = str(frame.get("turn_id") or "").strip()
                    tid = (
                        raw_tid
                        if re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", raw_tid)
                        else uuid.uuid4().hex[:12]
                    )
                    if not self.config.enabled:
                        await self._send_frame(
                            peer,
                            {
                                "type": "turn.end",
                                "turn_id": tid,
                                "status": "skipped",
                                "reason": "companion_disabled",
                            },
                        )
                        continue
                    event_kind: Literal["speech", "screen_pulse"] = (
                        "speech" if typ == "speech.turn" else "screen_pulse"
                    )
                    if event_kind == "screen_pulse":
                        if not self.config.proactive_watch_enabled:
                            await self._send_frame(
                                peer,
                                {
                                    "type": "turn.end",
                                    "turn_id": tid,
                                    "status": "skipped",
                                    "reason": "proactive_disabled",
                                },
                            )
                            continue
                        now = self.clock()
                        if now - sess.last_pulse_at < self.config.min_pulse_interval_seconds:
                            await self._send_frame(
                                peer,
                                {
                                    "type": "turn.end",
                                    "turn_id": tid,
                                    "status": "skipped",
                                    "reason": "cooldown",
                                },
                            )
                            continue
                        sess.last_pulse_at = now
                    speech_text = ""
                    if event_kind == "speech":
                        speech_text = clean_text(str(frame.get("text") or ""))[:1000]
                        if not speech_text:
                            await self._send_frame(
                                peer,
                                {
                                    "type": "turn.end",
                                    "turn_id": tid,
                                    "status": "skipped",
                                    "reason": "empty_speech",
                                },
                            )
                            continue
                    speaker_id = str(frame.get("speaker_id") or "speaker").strip()[:64]
                    speaker_name = str(frame.get("speaker_name") or "").strip()[:48]
                    try:
                        frames = _decode_frames(
                            frame.get("frames"), self.config.max_frames_per_turn, self.s
                        )
                    except Rejected as exc:
                        await self._send_frame(
                            peer,
                            {
                                "type": "turn.end",
                                "turn_id": tid,
                                "status": "error",
                                "reason": str(exc),
                            },
                        )
                        continue
                    await self._start_turn(
                        peer,
                        sess,
                        turn_id=tid,
                        event_kind=event_kind,
                        speech_text=speech_text,
                        speaker_id=speaker_id,
                        speaker_name=speaker_name,
                        frames=frames,
                    )
                elif typ == "session.stop":
                    await self._cancel_active_turn(peer)
                    if peer.session is not None:
                        sid = peer.session.id
                        await self._finalize_session(peer.session, status="ended")
                        await self._send_frame(peer, {"type": "session.ended", "session_id": sid})
                        peer.session = None
                else:
                    await self._send_frame(
                        peer, {"type": "error", "reason": "companion_unknown_frame"}
                    )
        except asyncio.CancelledError:
            raise
        except Rejected as exc:
            await self._send_frame(peer, {"type": "error", "reason": str(exc)})
        finally:
            peer.closed = True
            peer.ready.clear()
            await self._cancel_active_turn(peer)
            if peer.session is not None and not peer.session.finalized:
                await self._finalize_session(peer.session, status="disconnected")
            with anyio.CancelScope(shield=True):
                async with self.peer_lock:
                    if peer is self._peer:
                        self._peer = None
                        self.source_ip = None
                if hasattr(self.service, "record"):
                    self.service.record("companion_disconnected")
                try:
                    async with asyncio.timeout(2.0):
                        await close(1012)
                except Exception:
                    pass
