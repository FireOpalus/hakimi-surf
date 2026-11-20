"""Remote Plugin Bus: scoped credentials, reverse WebSocket RPC/events, and RemoteModuleProxy."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import math
import re
import secrets
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import anyio
from pydantic import BaseModel, ConfigDict, Field, ValidationError, create_model, field_validator

from .budget_policy import PROACTIVE_LANE, paid_lane_available
from .config import Settings
from .media import ModelImage
from .models import FileRef, ImageRef, Message
from .modules.actions import Action, ActionCall, NoArgs, Outcome
from .modules.adapters import _FAREWELL_BYPASS
from .modules.base import BaseModule, NodeRegion, sanitize_sensor_text
from .plugin_kernel import PluginContext, PluginFiber, PluginKernel
from .screen_watch import decode_b64_frames, decode_ws_json_frame
from .security import Rejected, clean_text, configured_secrets, safe_output
from .social import confirmed_message_id
from .thinking_policy import CORE_THINKING_SCENES, THINKING_LEVELS, ThinkingScene

log = logging.getLogger("hakimi")

VALID_SCOPES = (
    "actions",
    "sensors",
    "thinking_scenes",
    "chat",
    "media",
    "owner_notify",
)
_VALID_SCOPES_SET = frozenset(VALID_SCOPES)

RESERVED_PLUGIN_IDS = frozenset(
    {
        "qq_social",
        "bilibili_surf",
        "minecraft",
        "weather",
        "identity",
        "plugin_bus",
        "event_ledger",
        "patrol_diary",
        "drive_sync",
        "database_backup",
        "surf_mirror",
        "group_memory_mirror",
        "owner_notebook",
        "screen_watch",
        "voice_companion",
        "admin",
        "system",
        "core",
    }
)
RESERVED_CHAT_PLATFORMS = frozenset({"onebot", "qq"})
_RESERVED_ACTION_PREFIXES = frozenset(
    {
        "qq",
        "bili",
        "bilibili",
        "mc",
        "minecraft",
        "weather",
        "social",
        "system",
        "admin",
        "core",
        "world",
        "identity",
        "plugin_bus",
    }
)
_RESERVED_SCENE_KEYS = frozenset(
    {
        "fast_chat",
        "owner_private",
        "bilibili_summary",
        "daily_diary",
        "memory_compression",
        "owner_digest",
        "owner_companion",
        "bili_selection",
        "bili_learning",
        "voice.companion",
        "screen.watch",
        *(scene.key for scene in CORE_THINKING_SCENES),
    }
)

PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}\Z")
PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}\Z")
_ACTION_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,30}\.[a-z][a-z0-9_]{0,30}\Z")
_FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}\Z")
_CAPABILITY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}\Z")
_EVENT_TYPE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,48}\Z")
_CODE_RE = re.compile(r"^[a-z0-9_.-]{1,64}\Z")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS plugin_credentials (
  plugin_id TEXT PRIMARY KEY,
  token_salt TEXT NOT NULL,
  token_hash TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  payload BLOB NOT NULL,
  updated REAL NOT NULL
);
"""

_VAULT_CRED_PREFIX = "plugin-cred:"
_DEFAULT_MAX_FRAME_BYTES = 262_144
_DEFAULT_HEARTBEAT_SECONDS = 20.0
_DEFAULT_IDLE_TIMEOUT_SECONDS = 60.0
_DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 15.0
_DEFAULT_RPC_TIMEOUT_SECONDS = 15.0
_SENSOR_RPC_TIMEOUT_SECONDS = 3.5
_MAX_PENDING_RPCS = 32
_NOTIFY_WINDOW_SECONDS = 600.0
_NOTIFY_MAX_PER_WINDOW = 3
_VALID_OUTCOME_STATUSES = frozenset(
    {"done", "gated_skip", "not_dispatched", "uncertain", "executing", "expired"}
)


def _hash_token(index_key: bytes, salt: str, token: str) -> str:
    return hmac.new(index_key, f"{salt}:{token}".encode(), hashlib.sha256).hexdigest()


def _sanitize_payload(
    value: Any,
    settings: Settings | None,
    *,
    key_limit: int = 24,
    val_limit: int = 90,
    depth: int = 0,
) -> Any:
    if depth > 4:
        return "[truncated]"
    if isinstance(value, str):
        return sanitize_sensor_text(value, settings, max_tokens=val_limit)
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in list(value.items())[:24]:
            ck = sanitize_sensor_text(str(k), settings, max_tokens=key_limit)
            if ck:
                out[ck] = _sanitize_payload(
                    v, settings, key_limit=key_limit, val_limit=val_limit, depth=depth + 1
                )
        return out
    if isinstance(value, list | tuple):
        return [
            _sanitize_payload(
                item, settings, key_limit=key_limit, val_limit=val_limit, depth=depth + 1
            )
            for item in list(value)[:24]
        ]
    return sanitize_sensor_text(str(value), settings, max_tokens=val_limit)


def _validate_plugin_id(plugin_id: str) -> str:
    if not isinstance(plugin_id, str) or not PLUGIN_ID_RE.fullmatch(plugin_id):
        raise Rejected("invalid_plugin_id")
    if plugin_id in RESERVED_PLUGIN_IDS:
        raise Rejected("reserved_plugin_id")
    return plugin_id


def _validate_scopes(scopes: list[str] | tuple[str, ...]) -> list[str]:
    if not isinstance(scopes, list | tuple) or len(scopes) > len(VALID_SCOPES) * 2:
        raise Rejected("invalid_plugin_scope")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in scopes:
        if not isinstance(item, str) or item not in _VALID_SCOPES_SET:
            raise Rejected("invalid_plugin_scope")
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _validate_display_name(display_name: str, settings: Settings | None) -> str:
    if not isinstance(display_name, str):
        raise Rejected("invalid_admin_input")
    cleaned = sanitize_sensor_text(clean_text(display_name), settings, max_tokens=32)[:64].strip()
    if not cleaned:
        raise Rejected("invalid_admin_input")
    return cleaned


def _validate_node_region(node_region: str) -> NodeRegion:
    if node_region not in ("KR", "CN"):
        raise Rejected("invalid_admin_input")
    return node_region


class _RemoteArgsBase(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class _PluginActionDecl(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    name: str = Field(min_length=3, max_length=63)
    description: str = Field(min_length=1, max_length=240)
    arguments: dict[str, Any] = Field(default_factory=dict)
    origins: list[Literal["planner", "admin", "query"]] | None = None
    effortful: bool = False
    interrupts_focus: bool = False
    window: Literal["any", "active", "closing"] = "any"
    social: bool = False
    owner_only: bool = False

    @field_validator("name")
    @classmethod
    def _check_name(cls, val: str) -> str:
        if not _ACTION_NAME_RE.fullmatch(val):
            raise ValueError("invalid action name")
        prefix = val.split(".", 1)[0]
        if prefix in _RESERVED_ACTION_PREFIXES:
            raise ValueError("reserved action prefix")
        return val


class _PluginThinkingSceneDecl(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    key: str = Field(min_length=2, max_length=64)
    label: str = Field(min_length=1, max_length=80)
    default: Literal["low", "medium", "high", "xhigh"] = "medium"
    levels: list[Literal["low", "medium", "high", "xhigh"]] = Field(
        default_factory=lambda: list(THINKING_LEVELS), min_length=1, max_length=4
    )
    output_tokens: int = Field(default=2048, ge=128, le=16384)


class _PluginHelloFrame(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    type: Literal["plugin.hello"]
    protocol_version: int
    display_name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=240)
    scopes: list[str] = Field(default_factory=list, max_length=16)
    actions: list[_PluginActionDecl] = Field(default_factory=list, max_length=32)
    thinking_scenes: list[_PluginThinkingSceneDecl] = Field(default_factory=list, max_length=16)
    chat_platforms: list[str] = Field(default_factory=list, max_length=8)
    capabilities: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("protocol_version")
    @classmethod
    def _check_version(cls, val: int) -> int:
        if isinstance(val, bool) or val != 1:
            raise ValueError("unsupported protocol_version")
        return val


def _build_action_args_model(action_name: str, raw_args: dict[str, Any]) -> type[BaseModel]:
    if not raw_args:
        return NoArgs
    if not isinstance(raw_args, dict):
        raise Rejected("plugin_invalid_schema")

    if "properties" in raw_args and raw_args.get("type") in (None, "object"):
        allowed_top = {"type", "properties", "required", "additionalProperties"}
        if set(raw_args) - allowed_top:
            raise Rejected("plugin_invalid_schema")
        if raw_args.get("additionalProperties") is True:
            raise Rejected("plugin_invalid_schema")
        props = raw_args.get("properties")
        raw_req = raw_args.get("required") or []
        if not isinstance(props, dict) or not isinstance(raw_req, list):
            raise Rejected("plugin_invalid_schema")
        if not all(isinstance(k, str) for k in raw_req):
            raise Rejected("plugin_invalid_schema")
        required_set = set(raw_req)
    else:
        props = raw_args
        required_set = None

    if not props:
        return NoArgs
    if len(props) > 16:
        raise Rejected("plugin_invalid_schema")

    fields: dict[str, Any] = {}
    base_attrs = set(dir(_RemoteArgsBase))
    for fname, spec in props.items():
        if (
            not isinstance(fname, str)
            or not _FIELD_NAME_RE.fullmatch(fname)
            or fname in base_attrs
            or not isinstance(spec, dict)
        ):
            raise Rejected("plugin_invalid_schema")
        ftype_raw = spec.get("type", "string")
        enum_vals = spec.get("enum")
        if enum_vals is not None:
            if (
                not isinstance(enum_vals, list)
                or not enum_vals
                or len(enum_vals) > 16
                or not all(isinstance(v, str | int) and not isinstance(v, bool) for v in enum_vals)
            ):
                raise Rejected("plugin_invalid_schema")
            py_type: Any = Literal[tuple(enum_vals)]  # type: ignore[valid-type]
        elif ftype_raw in ("string", "str"):
            py_type = str
        elif ftype_raw in ("integer", "int"):
            py_type = int
        elif ftype_raw in ("number", "float"):
            py_type = float
        elif ftype_raw in ("boolean", "bool"):
            py_type = bool
        elif ftype_raw in ("array", "list"):
            py_type = list[Any]
        elif ftype_raw in ("object", "dict"):
            py_type = dict[str, Any]
        else:
            raise Rejected("plugin_invalid_schema")

        field_kwargs: dict[str, Any] = {}
        desc = spec.get("description")
        if isinstance(desc, str) and desc.strip():
            field_kwargs["description"] = desc.strip()[:120]
        if py_type is str:
            min_len = spec.get("min_length", spec.get("minLength"))
            max_len = spec.get("max_length", spec.get("maxLength"))
            if isinstance(min_len, int) and not isinstance(min_len, bool) and min_len >= 0:
                field_kwargs["min_length"] = min(min_len, 4096)
            if isinstance(max_len, int) and not isinstance(max_len, bool) and max_len >= 1:
                field_kwargs["max_length"] = min(max_len, 4096)
            else:
                field_kwargs["max_length"] = 2048
        elif py_type in (int, float):
            ge_v = spec.get("ge", spec.get("minimum"))
            le_v = spec.get("le", spec.get("maximum"))
            if isinstance(ge_v, int | float) and not isinstance(ge_v, bool) and math.isfinite(ge_v):
                field_kwargs["ge"] = ge_v
            if isinstance(le_v, int | float) and not isinstance(le_v, bool) and math.isfinite(le_v):
                field_kwargs["le"] = le_v

        has_default = "default" in spec
        is_required = (
            (fname in required_set)
            if required_set is not None
            else (
                spec.get("required") is True
                or (not has_default and spec.get("required") is not False)
            )
        )
        if has_default and not is_required:
            default_val = spec["default"]
            ann = (py_type | None) if default_val is None else py_type
            fields[fname] = (ann, Field(default=default_val, **field_kwargs))
        elif is_required:
            fields[fname] = (py_type, Field(..., **field_kwargs))
        else:
            fields[fname] = (py_type | None, Field(default=None, **field_kwargs))

    safe_cls_name = "RemoteArgs_" + re.sub(r"[^A-Za-z0-9_]", "_", action_name)
    model = create_model(safe_cls_name, __base__=_RemoteArgsBase, **fields)
    return model


async def _call_before_send(before_send: Callable[..., Any], delivery_id: str) -> bool:
    sig = inspect.signature(before_send)
    positional = [
        p
        for p in sig.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    if has_varargs or len(positional) >= 1:
        res = before_send(delivery_id)
    else:
        res = before_send()
    if inspect.isawaitable(res):
        res = await res
    return bool(res)


@dataclass
class _PendingRpc:
    future: asyncio.Future[dict[str, Any]]
    method: str
    delivery_id: str | None = None


@dataclass
class _PluginSession:
    plugin_id: str
    receive: Callable[[], Awaitable[str | bytes]]
    send: Callable[[str], Awaitable[None]]
    close: Callable[[int], Awaitable[None]]
    source_ip: str
    fiber: PluginFiber
    ctx: PluginContext
    scopes: frozenset[str]
    node_region: NodeRegion
    connected_at: float
    last_seen_at: float
    proxy: RemoteModuleProxy | None = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: dict[str, _PendingRpc] = field(default_factory=dict)
    uncertain_deliveries: set[str] = field(default_factory=set)
    ready: bool = False
    closed: bool = False
    close_code: int = 1012


class RemoteModuleProxy(BaseModule):
    """Dynamic BaseModule proxy backed by an authenticated remote plugin WS session."""

    def __init__(
        self,
        settings: Settings | None,
        bus: PluginBus,
        session: _PluginSession,
        *,
        plugin_id: str,
        display_name: str,
        description: str,
        node_region: NodeRegion,
        scopes: frozenset[str],
        chat_platforms: tuple[str, ...],
        thinking_scenes: tuple[ThinkingScene, ...],
        actions: tuple[Action, ...],
        capabilities: tuple[str, ...],
        enabled: bool = True,
    ) -> None:
        super().__init__(settings)
        self.module_id = plugin_id
        self.display_name = display_name
        self.description = description
        self.node_region = node_region
        self._bus = bus
        self._session = session
        self._scopes = scopes
        self.chat_platforms = chat_platforms
        self.thinking_scenes = thinking_scenes
        self.thinking_scene_keys = frozenset(scene.key for scene in thinking_scenes)
        self._actions = actions
        self.capabilities = capabilities
        cap_set = set(capabilities)
        self.supports_sticker = bool({"sticker", "supports_sticker"} & cap_set)
        self.supports_emoji_like = bool({"emoji_like", "supports_emoji_like"} & cap_set)
        self.supports_poke = bool({"poke", "supports_poke"} & cap_set)
        self.supports_reply_anchor = bool({"reply_anchor", "supports_reply_anchor"} & cap_set)
        self.enabled = enabled
        self._last_sensor_summary: str = ""

    def actions(self) -> tuple[Action, ...]:
        return self._actions

    async def _raw_sensor_summary(self) -> str:
        if "sensors" not in self._scopes:
            return ""
        if not self._session.ready or self._session.closed:
            return self._last_sensor_summary
        try:
            resp = await self._bus._rpc(
                self._session,
                "sensor.read",
                {},
                timeout=_SENSOR_RPC_TIMEOUT_SECONDS,
            )
            if not resp.get("ok", True):
                return self._last_sensor_summary
            raw = resp.get("result") if "result" in resp else resp.get("data")
            if isinstance(raw, dict):
                text = str(raw.get("summary") or raw.get("text") or "")
            elif isinstance(raw, str):
                text = raw
            elif raw is None:
                text = ""
            else:
                text = str(raw)
            self._last_sensor_summary = text
            return text
        except asyncio.CancelledError:
            raise
        except Exception:
            return self._last_sensor_summary

    def authorize_chat(self, message: Message, settings: Any, *, bot_exempt: bool = False) -> bool:
        if (
            "chat" not in self._scopes
            or not self.enabled
            or not self._session.ready
            or self._session.closed
            or message.platform not in self.chat_platforms
            or message.user_id == message.bot_id
        ):
            return False
        if message.kind == "private" and message.conversation_id != message.user_id:
            return False
        runtime = getattr(getattr(self._bus.store, "runtime", None), "identity", None) or getattr(
            self._bus.service, "identity", None
        )
        if (
            runtime is not None
            and hasattr(runtime, "is_muted")
            and runtime.is_muted(message.platform, message.user_id)
            and _FAREWELL_BYPASS.get() != (message.platform, message.user_id)
        ):
            return False
        group_policies = getattr(settings, "group_policies", {}) or {}
        policy = group_policies.get(f"{message.platform}:{message.conversation_id}", "at_only")
        if policy == "muted":
            return False
        if message.kind == "group":
            return bool(message.addressed or policy == "smart_reply")
        return True

    def chat_connected(self, platform: str) -> bool:
        return bool(
            self.enabled
            and "chat" in self._scopes
            and platform in self.chat_platforms
            and self._session.ready
            and not self._session.closed
        )

    async def send_chat(self, message: Message, text: str, *, before_send=None) -> str:
        if "chat" not in self._scopes or message.platform not in self.chat_platforms:
            raise Rejected("delivery_not_authorized")
        if not self.enabled:
            raise Rejected("module_disabled")
        delivery_id = uuid.uuid4().hex
        if before_send is not None:
            allowed = await _call_before_send(before_send, delivery_id)
            if not allowed:
                raise Rejected("reply_invalidated")
        if not self._session.ready or self._session.closed:
            raise Rejected("plugin_disconnected")
        params = {
            "delivery_id": delivery_id,
            "platform": message.platform,
            "bot_id": message.bot_id,
            "kind": message.kind,
            "conversation_id": message.conversation_id,
            "user_id": message.user_id,
            "text": text,
            "reply_to": message.reply_ids[0] if message.reply_ids else None,
        }
        try:
            resp = await self._bus._rpc(
                self._session,
                "delivery.send",
                params,
                delivery_id=delivery_id,
            )
        except asyncio.CancelledError:
            self._session.uncertain_deliveries.add(delivery_id)
            self._bus.uncertain_deliveries.append(delivery_id)
            raise
        except Rejected as exc:
            self._session.uncertain_deliveries.add(delivery_id)
            self._bus.uncertain_deliveries.append(delivery_id)
            if str(exc) in {
                "plugin_disconnected",
                "plugin_rpc_timeout",
                "plugin_delivery_uncertain",
            }:
                raise Rejected("plugin_delivery_uncertain") from None
            raise
        if not resp.get("ok", False):
            self._session.uncertain_deliveries.add(delivery_id)
            self._bus.uncertain_deliveries.append(delivery_id)
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_delivery_failed"
            raise Rejected(code)
        res = resp.get("result") if "result" in resp else resp.get("data")
        msg_id = res.get("message_id") if isinstance(res, dict) else res
        return confirmed_message_id(msg_id)

    async def on_switch(self, service: Any) -> None:
        if (
            not self.enabled
            and self.chat_platforms
            and hasattr(service, "store")
            and hasattr(service.store, "cancel_platform_work")
        ):
            await service.store.cancel_platform_work(self.chat_platforms)

    def admin_status(self, request: Any) -> dict[str, Any]:
        return {
            "online": bool(self._session.ready and not self._session.closed),
            "node_region": self.node_region,
            "scopes": sorted(self._scopes),
            "capabilities": list(self.capabilities),
            "supports_sticker": self.supports_sticker,
            "supports_emoji_like": self.supports_emoji_like,
            "supports_poke": self.supports_poke,
            "supports_reply_anchor": self.supports_reply_anchor,
        }


class PluginBus:
    """Credential store and reverse WebSocket transport for remote plugin modules."""

    def __init__(
        self,
        settings: Settings,
        store: Any,
        service: Any,
        *,
        clock: Callable[[], float] = time.time,
        heartbeat_interval: float | None = None,
        idle_timeout: float | None = None,
        handshake_timeout: float | None = None,
        rpc_timeout: float | None = None,
        notify_cooldown: float = _NOTIFY_WINDOW_SECONDS,
        notify_limit: int = _NOTIFY_MAX_PER_WINDOW,
    ) -> None:
        self.s = settings
        self.store = store
        self.service = service
        self.clock = clock
        self.heartbeat_interval = float(
            getattr(settings, "plugin_bus_ws_heartbeat_seconds", _DEFAULT_HEARTBEAT_SECONDS)
            if heartbeat_interval is None
            else heartbeat_interval
        )
        self.idle_timeout = float(
            getattr(settings, "plugin_bus_ws_idle_timeout_seconds", _DEFAULT_IDLE_TIMEOUT_SECONDS)
            if idle_timeout is None
            else idle_timeout
        )
        self.handshake_timeout = float(
            _DEFAULT_HANDSHAKE_TIMEOUT_SECONDS if handshake_timeout is None else handshake_timeout
        )
        self.rpc_timeout = float(
            getattr(settings, "plugin_bus_rpc_timeout", _DEFAULT_RPC_TIMEOUT_SECONDS)
            if rpc_timeout is None
            else rpc_timeout
        )
        self.notify_cooldown = float(notify_cooldown)
        self.notify_limit = int(notify_limit)
        self.initialized = False
        self._init_lock = asyncio.Lock()
        self._connect_locks: dict[str, asyncio.Lock] = {}
        self._sessions: dict[str, _PluginSession] = {}
        self._fallback_kernel = PluginKernel()
        self._notify_history: dict[str, deque[float]] = {}
        self.uncertain_deliveries: deque[str] = deque(maxlen=128)

    def _connect_lock(self, plugin_id: str) -> asyncio.Lock:
        lock = self._connect_locks.get(plugin_id)
        if lock is None:
            lock = asyncio.Lock()
            self._connect_locks[plugin_id] = lock
        return lock

    def _kernel(self) -> PluginKernel:
        k = getattr(self.service, "kernel", None)
        return k if isinstance(k, PluginKernel) else self._fallback_kernel

    def _max_frame_bytes(self) -> int:
        return int(getattr(self.s, "plugin_bus_ws_max_frame_bytes", _DEFAULT_MAX_FRAME_BYTES))

    def _ws_enabled(self) -> bool:
        return bool(getattr(self.s, "plugin_bus_ws_enabled", False))

    def _ws_path(self) -> str:
        return str(getattr(self.s, "plugin_bus_ws_path", "/ws/plugin"))

    async def initialize(self) -> None:
        if self.initialized:
            return
        async with self._init_lock:
            if self.initialized:
                return
            async with self.store.transaction():
                await self.store.db.executescript(_SCHEMA)
            self.initialized = True

    def _open_cred_row(self, row: Any) -> dict[str, Any]:
        plugin_id = str(row["plugin_id"])
        data = self.store.vault.open(_VAULT_CRED_PREFIX + plugin_id, row["payload"])
        if not isinstance(data, dict):
            raise Rejected("plugin_credential_corrupt")
        return {
            "plugin_id": plugin_id,
            "enabled": bool(row["enabled"]),
            "display_name": str(data.get("display_name") or plugin_id),
            "allowed_scopes": list(data.get("allowed_scopes") or []),
            "node_region": data.get("node_region", "KR"),
            "created_at": float(data.get("created_at") or row["updated"]),
            "last_seen_at": (
                float(data["last_seen_at"]) if data.get("last_seen_at") is not None else None
            ),
            "updated": float(row["updated"]),
        }

    async def _load_cred(self, plugin_id: str) -> dict[str, Any] | None:
        await self.initialize()
        async with self.store.lock:
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (plugin_id,),
            )
        if row is None:
            return None
        return self._open_cred_row(row)

    async def create_plugin(
        self,
        plugin_id: str,
        display_name: str,
        scopes: list[str],
        *,
        node_region: Literal["KR", "CN"] = "KR",
    ) -> tuple[str, dict[str, Any]]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        modules = getattr(self.service, "modules", None)
        if modules is not None and pid in getattr(modules, "systems", {}):
            raise Rejected("reserved_plugin_id")
        clean_name = _validate_display_name(display_name, self.s)
        clean_scopes = _validate_scopes(scopes)
        region = _validate_node_region(node_region)

        token = secrets.token_urlsafe(32)
        salt = secrets.token_hex(16)
        token_hash = _hash_token(self.store.vault.index_key, salt, token)
        now = self.clock()
        payload_blob = self.store.vault.seal(
            _VAULT_CRED_PREFIX + pid,
            {
                "display_name": clean_name,
                "allowed_scopes": clean_scopes,
                "node_region": region,
                "created_at": now,
                "last_seen_at": None,
            },
        )
        async with self.store.transaction():
            existing = await self.store.one(
                "SELECT 1 FROM plugin_credentials WHERE plugin_id=?", (pid,)
            )
            if existing is not None:
                raise Rejected("plugin_already_exists")
            await self.store.db.execute(
                "INSERT INTO plugin_credentials"
                "(plugin_id, token_salt, token_hash, enabled, payload, updated) "
                "VALUES (?,?,?,?,?,?)",
                (pid, salt, token_hash, 1, payload_blob, now),
            )
        return token, await self.overview()

    async def update_plugin(
        self,
        plugin_id: str,
        *,
        display_name: str | None = None,
        scopes: list[str] | None = None,
        enabled: bool | None = None,
        node_region: Literal["KR", "CN"] | None = None,
    ) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        clean_name = (
            _validate_display_name(display_name, self.s) if display_name is not None else None
        )
        clean_scopes = _validate_scopes(scopes) if scopes is not None else None
        region = _validate_node_region(node_region) if node_region is not None else None
        if enabled is not None and not isinstance(enabled, bool):
            raise Rejected("invalid_admin_input")

        now = self.clock()
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (pid,),
            )
            if row is None:
                raise Rejected("plugin_not_found")
            cred = self._open_cred_row(row)
            new_enabled = cred["enabled"] if enabled is None else enabled
            new_name = cred["display_name"] if clean_name is None else clean_name
            new_scopes = cred["allowed_scopes"] if clean_scopes is None else clean_scopes
            new_region = cred["node_region"] if region is None else region
            blob = self.store.vault.seal(
                _VAULT_CRED_PREFIX + pid,
                {
                    "display_name": new_name,
                    "allowed_scopes": new_scopes,
                    "node_region": new_region,
                    "created_at": cred["created_at"],
                    "last_seen_at": cred["last_seen_at"],
                },
            )
            await self.store.db.execute(
                "UPDATE plugin_credentials SET enabled=?, payload=?, updated=? WHERE plugin_id=?",
                (int(new_enabled), blob, now, pid),
            )

        session = self._sessions.get(pid)
        if session is not None:
            scopes_narrowed = clean_scopes is not None and not session.scopes.issubset(
                set(new_scopes)
            )
            if not new_enabled or scopes_narrowed:
                await self._dispose_session(pid, close_code=1008)
            elif session.proxy is not None:
                if clean_name is not None:
                    session.proxy.display_name = new_name
                if region is not None:
                    session.proxy.node_region = new_region
                    session.node_region = new_region
        return await self.overview()

    async def rotate_token(self, plugin_id: str) -> tuple[str, dict[str, Any]]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        token = secrets.token_urlsafe(32)
        salt = secrets.token_hex(16)
        token_hash = _hash_token(self.store.vault.index_key, salt, token)
        now = self.clock()
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "UPDATE plugin_credentials "
                "SET token_salt=?, token_hash=?, updated=? WHERE plugin_id=?",
                (salt, token_hash, now, pid),
            )
            if cur.rowcount == 0:
                raise Rejected("plugin_not_found")
        await self._dispose_session(pid, close_code=1008)
        return token, await self.overview()

    async def delete_plugin(self, plugin_id: str) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        await self._dispose_session(pid, close_code=1008)
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "DELETE FROM plugin_credentials WHERE plugin_id=?", (pid,)
            )
            if cur.rowcount == 0:
                raise Rejected("plugin_not_found")
        return await self.overview()

    async def disconnect_plugin(self, plugin_id: str) -> dict[str, Any]:
        await self.initialize()
        pid = _validate_plugin_id(plugin_id)
        if await self._load_cred(pid) is None:
            raise Rejected("plugin_not_found")
        await self._dispose_session(pid, close_code=1012)
        return await self.overview()

    async def authenticate(self, plugin_id: str, token: str) -> dict[str, Any] | None:
        await self.initialize()
        if not isinstance(plugin_id, str) or not isinstance(token, str) or not token:
            return None
        try:
            pid = _validate_plugin_id(plugin_id)
        except Rejected:
            return None
        async with self.store.lock:
            row = await self.store.one(
                "SELECT plugin_id, token_salt, token_hash, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (pid,),
            )
        if row is None or not row["enabled"]:
            return None
        expected = _hash_token(self.store.vault.index_key, row["token_salt"], token)
        if not hmac.compare_digest(expected, row["token_hash"]):
            return None
        return self._open_cred_row(row)

    async def status_summary(self) -> dict[str, Any]:
        await self.initialize()
        async with self.store.lock:
            row = await self.store.one("SELECT COUNT(*) FROM plugin_credentials")
        registered_count = int(row[0]) if row else 0
        online_count = sum(1 for s in self._sessions.values() if s.ready and not s.closed)
        return {
            "ws_enabled": self._ws_enabled(),
            "ws_path": self._ws_path(),
            "registered_count": registered_count,
            "online_count": online_count,
        }

    async def overview(self) -> dict[str, Any]:
        await self.initialize()
        async with self.store.lock:
            rows = await self.store.all(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials ORDER BY plugin_id ASC"
            )
        plugins: list[dict[str, Any]] = []
        for row in rows:
            cred = self._open_cred_row(row)
            pid = cred["plugin_id"]
            sess = self._sessions.get(pid)
            online = bool(sess is not None and sess.ready and not sess.closed)
            proxy = sess.proxy if sess is not None and online else None
            plugins.append(
                {
                    "plugin_id": pid,
                    "display_name": cred["display_name"],
                    "enabled": cred["enabled"],
                    "online": online,
                    "scopes": list(cred["allowed_scopes"]),
                    "node_region": cred["node_region"],
                    "connected_at": sess.connected_at if online and sess is not None else None,
                    "last_seen_at": (
                        sess.last_seen_at if online and sess is not None else cred["last_seen_at"]
                    ),
                    "remote_ip": sess.source_ip if online and sess is not None else None,
                    "actions_count": len(proxy.actions()) if proxy is not None else 0,
                    "chat_platforms": list(proxy.chat_platforms) if proxy is not None else [],
                    "thinking_scenes": (
                        [s.key for s in proxy.thinking_scenes] if proxy is not None else []
                    ),
                }
            )
        return {
            "ws_enabled": self._ws_enabled(),
            "ws_path": self._ws_path(),
            "allowed_scopes_catalog": list(VALID_SCOPES),
            "plugins": plugins,
        }

    async def _touch_last_seen(self, plugin_id: str, ts: float) -> None:
        async with self.store.transaction():
            row = await self.store.one(
                "SELECT plugin_id, enabled, payload, updated "
                "FROM plugin_credentials WHERE plugin_id=?",
                (plugin_id,),
            )
            if row is None:
                return
            cred = self._open_cred_row(row)
            blob = self.store.vault.seal(
                _VAULT_CRED_PREFIX + plugin_id,
                {
                    "display_name": cred["display_name"],
                    "allowed_scopes": cred["allowed_scopes"],
                    "node_region": cred["node_region"],
                    "created_at": cred["created_at"],
                    "last_seen_at": ts,
                },
            )
            await self.store.db.execute(
                "UPDATE plugin_credentials SET payload=?, updated=? WHERE plugin_id=?",
                (blob, ts, plugin_id),
            )

    async def _load_module_switch(self, module_id: str) -> bool:
        async with self.store.lock:
            row = await self.store.one(
                "SELECT enabled FROM module_switches WHERE module_id=?",
                (module_id,),
            )
        return True if row is None else bool(row[0])

    async def _dispose_session(self, plugin_id: str, *, close_code: int = 1012) -> None:
        sess = self._sessions.get(plugin_id)
        if sess is not None:
            sess.close_code = close_code
            await sess.fiber.dispose()
        else:
            kernel = self._kernel()
            await kernel.dispose_fiber(f"remote:{plugin_id}")

    async def close(self) -> None:
        for pid in list(self._sessions):
            await self._dispose_session(pid, close_code=1012)

    async def _send_raw(
        self,
        send: Callable[[str], Awaitable[None]],
        send_lock: asyncio.Lock,
        payload: dict[str, Any],
    ) -> None:
        wire = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        async with send_lock:
            await send(wire)

    async def _send_frame(self, session: _PluginSession, payload: dict[str, Any]) -> None:
        if session.closed:
            return
        try:
            await self._send_raw(session.send, session.send_lock, payload)
        except Exception:
            pass

    async def _rpc(
        self,
        session: _PluginSession,
        method: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        delivery_id: str | None = None,
    ) -> dict[str, Any]:
        if session.closed or not session.ready:
            raise Rejected("plugin_disconnected")
        if len(session.pending) >= _MAX_PENDING_RPCS:
            raise Rejected("plugin_backpressure")
        effective_timeout = self.rpc_timeout if timeout is None else float(timeout)
        req_id = uuid.uuid4().hex
        try:
            frame = json.dumps(
                {
                    "type": "rpc.request",
                    "id": req_id,
                    "method": method,
                    "params": params,
                },
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError):
            raise Rejected("plugin_invalid_params") from None
        if len(frame.encode("utf-8")) > self._max_frame_bytes():
            raise Rejected("plugin_frame_too_large")

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        session.pending[req_id] = _PendingRpc(future=future, method=method, delivery_id=delivery_id)
        dispatched = False
        try:
            async with asyncio.timeout(effective_timeout):
                async with session.send_lock:
                    if session.closed or not session.ready:
                        raise Rejected("plugin_disconnected")
                    dispatched = True
                    await session.send(frame)
                return await future
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise Rejected("plugin_rpc_timeout") from None
        except Rejected:
            raise
        except Exception:
            raise Rejected(
                "plugin_disconnected" if dispatched else "plugin_not_dispatched"
            ) from None
        finally:
            session.pending.pop(req_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    def _build_actions(
        self,
        session: _PluginSession,
        decls: list[_PluginActionDecl],
    ) -> tuple[Action, ...]:
        registry = getattr(self.service, "modules", None)
        existing_actions = set(registry.catalog.actions) if registry is not None else set()
        seen_names: set[str] = set()
        actions: list[Action] = []
        for decl in decls:
            if decl.name in seen_names or decl.name in existing_actions:
                raise Rejected("plugin_duplicate_action")
            seen_names.add(decl.name)
            if decl.social and "chat" not in session.scopes:
                raise Rejected("plugin_scope_rejected")
            args_model = _build_action_args_model(decl.name, decl.arguments)
            origins = (
                frozenset(decl.origins)
                if decl.origins is not None
                else frozenset({"planner", "admin"})
            )
            if "query" in origins and (decl.social or decl.owner_only or args_model is not NoArgs):
                raise Rejected("plugin_invalid_schema")
            desc = sanitize_sensor_text(clean_text(decl.description), self.s, max_tokens=80)
            if not desc:
                raise Rejected("plugin_invalid_schema")

            async def _run(call: ActionCall, action_name: str = decl.name) -> Outcome:
                proxy = session.proxy
                if proxy is None or not proxy.enabled:
                    return Outcome("gated_skip", "module_disabled")
                if not call.still_allowed():
                    return Outcome("gated_skip", "action_no_longer_eligible")
                args_dict = call.args.model_dump(mode="json")
                try:
                    resp = await self._rpc(
                        session,
                        "action.run",
                        {
                            "action": action_name,
                            "arguments": args_dict,
                            "origin": str(call.ticket.get("origin") or "planner"),
                            "target_scope": str(call.ticket.get("target_scope") or ""),
                            "now": call.now,
                        },
                    )
                except Rejected as exc:
                    code = str(exc)
                    if code in {"plugin_disconnected", "plugin_rpc_timeout"}:
                        return Outcome("uncertain", code)
                    return Outcome("not_dispatched", code)
                if not resp.get("ok", False):
                    raw_err = str(resp.get("error") or "")
                    reason = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_action_failed"
                    return Outcome("not_dispatched", reason)
                raw_res = resp.get("result") if "result" in resp else resp.get("data")
                if isinstance(raw_res, dict):
                    raw_status = str(raw_res.get("status") or "done")
                    status = raw_status if raw_status in _VALID_OUTCOME_STATUSES else "done"
                    raw_reason = str(raw_res.get("reason") or "ok")
                    reason = raw_reason if _CODE_RE.fullmatch(raw_reason) else "ok"
                    raw_details = raw_res.get("details")
                    if isinstance(raw_details, dict):
                        details = _sanitize_payload(raw_details, self.s)
                    else:
                        extra = {
                            k: v
                            for k, v in raw_res.items()
                            if k not in {"status", "reason", "details"}
                        }
                        details = _sanitize_payload(extra, self.s) if extra else {}
                    return Outcome(status=status, reason=reason, details=details)  # type: ignore[arg-type]
                if raw_res is None:
                    return Outcome("done", "ok")
                return Outcome(
                    "done",
                    "ok",
                    details={"summary": sanitize_sensor_text(str(raw_res), self.s, max_tokens=90)},
                )

            actions.append(
                Action(
                    name=decl.name,
                    description=desc,
                    run=_run,
                    arguments=args_model,
                    social=decl.social,
                    owner_only=decl.owner_only,
                    effortful=decl.effortful,
                    interrupts_focus=decl.interrupts_focus,
                    window=decl.window,
                    origins=origins,
                    unavailable=lambda s=session: (
                        None if s.ready and not s.closed else "plugin_offline"
                    ),
                )
            )
        return tuple(actions)

    def _build_thinking_scenes(
        self,
        decls: list[_PluginThinkingSceneDecl],
    ) -> tuple[ThinkingScene, ...]:
        registry = getattr(self.service, "modules", None)
        existing_keys = set(registry.thinking_scenes) if registry is not None else set()
        seen: set[str] = set()
        scenes: list[ThinkingScene] = []
        for decl in decls:
            if (
                decl.key in _RESERVED_SCENE_KEYS
                or decl.key in seen
                or decl.key in existing_keys
                or decl.key.split(".", 1)[0] in _RESERVED_ACTION_PREFIXES
            ):
                raise Rejected("plugin_scope_rejected")
            seen.add(decl.key)
            clean_label = sanitize_sensor_text(clean_text(decl.label), self.s, max_tokens=32)[:80]
            if not clean_label:
                raise Rejected("plugin_invalid_schema")
            try:
                scene = ThinkingScene(
                    key=decl.key,
                    label=clean_label,
                    default=decl.default,
                    levels=tuple(decl.levels),
                    output_tokens=decl.output_tokens,
                )
            except ValueError:
                raise Rejected("plugin_invalid_schema") from None
            scenes.append(scene)
        return tuple(scenes)

    def _validate_hello(
        self,
        raw_frame: dict[str, Any],
        cred: dict[str, Any],
    ) -> tuple[_PluginHelloFrame, frozenset[str], tuple[str, ...], tuple[str, ...]]:
        try:
            hello = _PluginHelloFrame.model_validate(raw_frame)
        except ValidationError:
            raise Rejected("plugin_invalid_schema") from None

        try:
            requested_scopes = _validate_scopes(hello.scopes)
        except Rejected:
            raise Rejected("plugin_scope_rejected") from None

        allowed_scopes = set(cred["allowed_scopes"])
        scope_set = frozenset(requested_scopes)
        if not scope_set.issubset(allowed_scopes):
            raise Rejected("plugin_scope_rejected")
        if hello.actions and "actions" not in scope_set:
            raise Rejected("plugin_scope_rejected")
        if hello.thinking_scenes and "thinking_scenes" not in scope_set:
            raise Rejected("plugin_scope_rejected")
        if hello.chat_platforms and "chat" not in scope_set:
            raise Rejected("plugin_scope_rejected")

        registry = getattr(self.service, "modules", None)
        existing_platforms = set(registry.chat_platforms) if registry is not None else set()
        seen_platforms: set[str] = set()
        clean_platforms: list[str] = []
        for plat in hello.chat_platforms:
            if not isinstance(plat, str) or not PLATFORM_ID_RE.fullmatch(plat):
                raise Rejected("plugin_invalid_schema")
            if (
                plat in RESERVED_CHAT_PLATFORMS
                or plat in seen_platforms
                or plat in existing_platforms
            ):
                raise Rejected("plugin_scope_rejected")
            seen_platforms.add(plat)
            clean_platforms.append(plat)

        clean_caps: list[str] = []
        seen_caps: set[str] = set()
        for cap in hello.capabilities:
            if not isinstance(cap, str) or not _CAPABILITY_RE.fullmatch(cap):
                raise Rejected("plugin_invalid_schema")
            if cap not in seen_caps:
                seen_caps.add(cap)
                clean_caps.append(cap)

        return hello, scope_set, tuple(clean_platforms), tuple(clean_caps)

    async def _handle_world_event(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_type = frame.get("event_type")
        if not isinstance(raw_type, str) or not _EVENT_TYPE_RE.fullmatch(raw_type):
            raise Rejected("plugin_invalid_schema")
        importance = frame.get("importance", "normal")
        if importance not in ("low", "normal", "high"):
            raise Rejected("plugin_invalid_schema")
        raw_payload = frame.get("payload", {})
        if not isinstance(raw_payload, dict) or len(raw_payload) > 32:
            raise Rejected("plugin_invalid_schema")
        clean_payload = _sanitize_payload(raw_payload, self.s)
        clean_type = sanitize_sensor_text(raw_type, self.s, max_tokens=24)[:48]
        if not clean_type:
            raise Rejected("plugin_invalid_schema")

        raw_key = frame.get("business_key") or frame.get("dedupe_key")
        clean_key = (
            sanitize_sensor_text(str(raw_key), self.s, max_tokens=40)[:128]
            if isinstance(raw_key, str) and raw_key.strip()
            else None
        )

        ws = getattr(self.service, "world_state", None)
        if ws is not None and hasattr(ws, "publish_event"):
            exp = getattr(ws, "experience", None)
            if exp is not None and not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                await ws.initialize()
            await ws.publish_event(
                session.plugin_id,
                clean_type,
                clean_payload,
                importance=importance,
                business_key=clean_key,
            )

        raw_summary = frame.get("summary")
        if isinstance(raw_summary, str) and raw_summary.strip():
            ledger = getattr(self.service, "event_ledger", None)
            if ledger is not None and hasattr(ledger, "post"):
                visibility = frame.get("visibility", "public")
                if visibility not in ("public", "owner_private"):
                    visibility = "public"
                due_at = frame.get("due_at")
                if due_at is not None and (
                    isinstance(due_at, bool)
                    or not isinstance(due_at, int | float)
                    or not math.isfinite(float(due_at))
                ):
                    due_at = None
                try:
                    await ledger.post(
                        session.plugin_id,
                        clean_type,
                        raw_summary,
                        payload=clean_payload,
                        dedupe_key=clean_key,
                        due_at=float(due_at) if due_at is not None else None,
                        visibility=visibility,
                    )
                except ValueError:
                    pass

        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session, {"type": "world.event.ack", "id": frame["id"], "accepted": True}
            )

    async def _handle_chat_inbound(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "chat" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        platform = frame.get("platform")
        if (
            not isinstance(platform, str)
            or platform in RESERVED_CHAT_PLATFORMS
            or platform not in session.proxy.chat_platforms
        ):
            raise Rejected("plugin_scope_rejected")

        raw_images = frame.get("images") or []
        raw_files = frame.get("files") or []
        if (raw_images or raw_files) and "media" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        if not isinstance(raw_images, list) or not isinstance(raw_files, list):
            raise Rejected("plugin_invalid_schema")

        kind = frame.get("kind", "private")
        if kind not in ("private", "group"):
            raise Rejected("plugin_invalid_schema")
        raw_user_id = frame.get("user_id")
        if not isinstance(raw_user_id, str) or not raw_user_id.strip():
            raise Rejected("plugin_invalid_schema")
        user_id = raw_user_id.strip()
        conv_id = str(
            frame.get("conversation_id") or (user_id if kind == "private" else "")
        ).strip()
        if not conv_id or (kind == "private" and conv_id != user_id):
            raise Rejected("plugin_invalid_schema")
        bot_id = str(frame.get("bot_id") or session.plugin_id).strip()[:64]
        event_id = str(frame.get("event_id") or uuid.uuid4().hex).strip()[:256]
        text = clean_text(str(frame.get("text") or ""))[:24000]
        addressed = frame.get("addressed")
        if not isinstance(addressed, bool):
            addressed = kind == "private"
        raw_ts = frame.get("created_at", self.clock())
        created_at = (
            float(raw_ts)
            if isinstance(raw_ts, int | float)
            and not isinstance(raw_ts, bool)
            and math.isfinite(float(raw_ts))
            else self.clock()
        )
        sender_nickname = sanitize_sensor_text(
            str(frame.get("sender_nickname") or ""), self.s, max_tokens=20
        )[:80]
        group_name = sanitize_sensor_text(
            str(frame.get("group_name") or ""), self.s, max_tokens=24
        )[:100]

        try:
            images = [ImageRef.model_validate(item) for item in raw_images[:2]]
            files = [FileRef.model_validate(item) for item in raw_files[:2]]
            msg = Message(
                platform=platform,
                bot_id=bot_id,
                event_id=event_id,
                user_id=user_id,
                kind=kind,
                conversation_id=conv_id,
                text=text,
                addressed=addressed,
                created_at=created_at,
                sender_nickname=sender_nickname,
                group_name=group_name,
                images=images,
                files=files,
            )
        except ValidationError:
            raise Rejected("plugin_invalid_schema") from None

        status = "ignored"
        if hasattr(self.service, "accept_from"):
            status = await self.service.accept_from(session.proxy, msg)
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "chat.inbound.ack", "id": frame["id"], "status": status},
            )

    async def _handle_owner_notify(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "owner_notify" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_text = frame.get("text")
        if not isinstance(raw_text, str):
            raise Rejected("plugin_invalid_schema")
        cleaned = sanitize_sensor_text(clean_text(raw_text), self.s, max_tokens=160)[:500].strip()
        if not cleaned:
            raise Rejected("plugin_invalid_schema")
        raw_code = frame.get("code") or "notify"
        safe_code = (
            raw_code if isinstance(raw_code, str) and _CODE_RE.fullmatch(raw_code) else "notify"
        )

        now = self.clock()
        history = self._notify_history.setdefault(session.plugin_id, deque())
        while history and (now - history[0]) > self.notify_cooldown:
            history.popleft()
        frame_id = frame.get("id") if isinstance(frame.get("id"), str) else None
        if len(history) >= self.notify_limit:
            if frame_id:
                await self._send_frame(
                    session,
                    {
                        "type": "owner.notify.ack",
                        "id": frame_id,
                        "ok": False,
                        "reason": "rate_limited",
                    },
                )
            return

        history.append(now)
        proactive = getattr(self.service, "proactive", None)
        delivered = False
        if proactive is not None:
            try:
                if hasattr(proactive, "alert"):
                    res = await proactive.alert(f"plugin:{session.plugin_id}:{safe_code}", cleaned)
                    delivered = res is not None
                elif hasattr(proactive, "enqueue"):
                    res = await proactive.enqueue("patrol", cleaned)
                    delivered = res is not None
            except Exception:
                delivered = False
        if frame_id:
            await self._send_frame(
                session,
                {
                    "type": "owner.notify.ack",
                    "id": frame_id,
                    "ok": True,
                    "queued": delivered,
                },
            )

    async def _handle_ai_generate(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id:
            raise Rejected("plugin_invalid_schema")
        scenario = frame.get("scenario") or frame.get("scene")
        if not isinstance(scenario, str) or scenario not in session.proxy.thinking_scene_keys:
            await self._send_frame(
                session,
                {
                    "type": "ai.result",
                    "id": req_id,
                    "ok": False,
                    "error": "plugin_scope_rejected",
                },
            )
            raise Rejected("plugin_scope_rejected")

        raw_images = frame.get("images")
        if raw_images and "media" not in session.scopes:
            await self._send_frame(
                session,
                {
                    "type": "ai.result",
                    "id": req_id,
                    "ok": False,
                    "error": "plugin_scope_rejected",
                },
            )
            raise Rejected("plugin_scope_rejected")

        if not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return

        raw_payload = frame.get("payload")
        raw_prompt = frame.get("prompt")
        if raw_payload is not None and not isinstance(raw_payload, dict):
            raise Rejected("plugin_invalid_schema")
        if raw_prompt is not None and not isinstance(raw_prompt, str):
            raise Rejected("plugin_invalid_schema")

        sanitized_payload = _sanitize_payload(raw_payload or {}, self.s)
        if isinstance(raw_prompt, str) and raw_prompt.strip():
            sanitized_payload["prompt"] = sanitize_sensor_text(raw_prompt, self.s, max_tokens=400)
        if not sanitized_payload:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "empty_payload"},
            )
            return
        sanitized_payload["plugin_id"] = session.plugin_id

        images: list[ModelImage] = []
        if raw_images:
            images = decode_b64_frames(raw_images, 2, self.s, error_prefix="plugin")

        if hasattr(self.store, "budget_status"):
            budget = await self.store.budget_status()
            if budget.get("hard_stop") or not paid_lane_available(budget, PROACTIVE_LANE):
                await self._send_frame(
                    session,
                    {"type": "ai.result", "id": req_id, "ok": False, "error": "budget_exhausted"},
                )
                return

        ai = getattr(self.service, "ai", None)
        if ai is None or not hasattr(ai, "generate"):
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "ai_unavailable"},
            )
            return

        secrets_list = (
            list(self.service.secrets)
            if getattr(self.service, "secrets", None)
            else configured_secrets(self.s)
        )
        try:
            raw_text = await ai.generate(
                sanitized_payload,
                f"plugin:{session.plugin_id}",
                scenario=scenario,
                budget_lane=PROACTIVE_LANE,
                images=images or None,
                audit_source="production",
            )
            clean_reply = safe_output(str(raw_text or ""), secrets_list, limit=1800)
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": True, "text": clean_reply},
            )
        except Rejected as exc:
            raw_err = str(exc).strip()
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_generate_rejected"
            await self._send_frame(
                session, {"type": "ai.result", "id": req_id, "ok": False, "error": code}
            )
        except Exception:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "ai_generate_failed"},
            )

    async def serve(
        self,
        receive: Callable[[], Awaitable[str | bytes]],
        send: Callable[[str], Awaitable[None]],
        close: Callable[[int], Awaitable[None]],
        *,
        source_ip: str,
        plugin_id: str,
    ) -> None:
        await self.initialize()
        try:
            pid = _validate_plugin_id(plugin_id)
        except Rejected:
            log.warning("event=plugin_scope_rejected plugin=%s reason=invalid_plugin_id", plugin_id)
            try:
                await close(1008)
            except Exception:
                pass
            return

        cred = await self._load_cred(pid)
        if cred is None or not cred["enabled"]:
            log.warning("event=plugin_scope_rejected plugin=%s reason=unauthorized", pid)
            try:
                await close(1008)
            except Exception:
                pass
            return

        max_frame_bytes = self._max_frame_bytes()
        hs_lock = asyncio.Lock()
        loop = asyncio.get_running_loop()
        last_rx = loop.time()

        # Phase 1: Wait for valid plugin.hello (allowing ping/pong during handshake).
        hello_raw: dict[str, Any] | None = None
        hs_deadline = loop.time() + self.handshake_timeout
        try:
            while True:
                remaining = max(0.01, hs_deadline - loop.time())
                async with asyncio.timeout(remaining):
                    raw = await receive()
                last_rx = loop.time()
                frame = decode_ws_json_frame(raw, max_frame_bytes, error_prefix="plugin")
                ftype = frame.get("type")
                if ftype == "ping":
                    await self._send_raw(send, hs_lock, {"type": "pong", "ts": self.clock()})
                    continue
                if ftype != "plugin.hello":
                    raise Rejected("plugin_scope_rejected")
                hello_raw = frame
                break
        except asyncio.CancelledError:
            raise
        except (TimeoutError, Rejected) as exc:
            reason = "plugin_handshake_timeout" if isinstance(exc, TimeoutError) else str(exc)
            log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, reason)
            try:
                await self._send_raw(send, hs_lock, {"type": "error", "reason": reason})
            except Exception:
                pass
            try:
                await close(1008)
            except Exception:
                pass
            return
        except Exception:
            try:
                await close(1012)
            except Exception:
                pass
            return

        # Phase 2: Validate credential, scope enforcement, and mount RemoteModuleProxy under fiber.
        session: _PluginSession | None = None
        ctx: PluginContext | None = None
        async with self._connect_lock(pid):
            cred = await self._load_cred(pid)
            if cred is None or not cred["enabled"]:
                log.warning("event=plugin_scope_rejected plugin=%s reason=disabled", pid)
                try:
                    await self._send_raw(
                        send, hs_lock, {"type": "error", "reason": "plugin_disabled"}
                    )
                except Exception:
                    pass
                try:
                    await close(1008)
                except Exception:
                    pass
                return

            if pid in self._sessions:
                await self._dispose_session(pid, close_code=1012)

            try:
                hello, scope_set, chat_platforms, capabilities = self._validate_hello(
                    hello_raw, cred
                )
            except Rejected as exc:
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, str(exc))
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                try:
                    await self._send_raw(send, hs_lock, {"type": "error", "reason": str(exc)})
                except Exception:
                    pass
                try:
                    await close(1008)
                except Exception:
                    pass
                return

            kernel = self._kernel()
            if f"remote:{pid}" in kernel.fibers():
                await kernel.dispose_fiber(f"remote:{pid}")

            ctx = kernel.fork(f"remote:{pid}")
            now = self.clock()
            session = _PluginSession(
                plugin_id=pid,
                receive=receive,
                send=send,
                close=close,
                source_ip=source_ip,
                fiber=ctx.fiber,
                ctx=ctx,
                scopes=scope_set,
                node_region=cred["node_region"],
                connected_at=now,
                last_seen_at=now,
            )

            # Register LIFO cleanup effects (reverse execution order:
            # 3rd registered -> runs 1st: cancel pending RPCs + mark in-flight deliveries uncertain
            # 2nd registered -> runs 2nd: unregister proxy & refresh _thinking_scenes
            # 1st registered -> runs 3rd: clear session entry, close WS, log disconnected)
            async def _cleanup_session_and_close(sess: _PluginSession = session) -> None:
                if self._sessions.get(sess.plugin_id) is sess:
                    self._sessions.pop(sess.plugin_id, None)
                sess.ready = False
                if not sess.closed:
                    sess.closed = True
                    try:
                        async with asyncio.timeout(2.0):
                            await sess.close(sess.close_code)
                    except Exception:
                        pass
                log.info("event=plugin_disconnected plugin=%s", sess.plugin_id)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_disconnected", emit_audit=False)

            def _unregister_proxy(sess: _PluginSession = session) -> None:
                modules = getattr(self.service, "modules", None)
                if modules is not None and sess.proxy is not None:
                    try:
                        if modules.get(sess.plugin_id) is sess.proxy:
                            modules.unregister(sess.plugin_id)
                    finally:
                        self.s._thinking_scenes = modules.thinking_scenes

            def _cancel_pending_rpcs(sess: _PluginSession = session) -> None:
                sess.ready = False
                for pending in list(sess.pending.values()):
                    if pending.delivery_id is not None:
                        sess.uncertain_deliveries.add(pending.delivery_id)
                        self.uncertain_deliveries.append(pending.delivery_id)
                    if not pending.future.done():
                        err = (
                            "plugin_delivery_uncertain"
                            if pending.delivery_id is not None
                            else "plugin_disconnected"
                        )
                        pending.future.set_exception(Rejected(err))

            ctx.effect(_cleanup_session_and_close)
            ctx.effect(_unregister_proxy)
            ctx.effect(_cancel_pending_rpcs)

            try:
                actions = self._build_actions(session, hello.actions)
                scenes = self._build_thinking_scenes(hello.thinking_scenes)
                saved_enabled = await self._load_module_switch(pid)
                clean_disp = sanitize_sensor_text(
                    clean_text(hello.display_name), self.s, max_tokens=32
                )[:64]
                clean_desc = sanitize_sensor_text(
                    clean_text(hello.description), self.s, max_tokens=80
                )[:240]
                proxy = RemoteModuleProxy(
                    self.s,
                    self,
                    session,
                    plugin_id=pid,
                    display_name=clean_disp or cred["display_name"],
                    description=clean_desc,
                    node_region=cred["node_region"],
                    scopes=scope_set,
                    chat_platforms=chat_platforms,
                    thinking_scenes=scenes,
                    actions=actions,
                    capabilities=capabilities,
                    enabled=saved_enabled,
                )
                session.proxy = proxy
                modules = getattr(self.service, "modules", None)
                if modules is not None:
                    modules.register(proxy)
                    self.s._thinking_scenes = modules.thinking_scenes
                self._sessions[pid] = session
                session.ready = True
                await self._touch_last_seen(pid, now)
            except (Rejected, ValueError) as exc:
                reason = str(exc) if isinstance(exc, Rejected) else "plugin_registration_conflict"
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, reason)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                session.close_code = 1008
                await self._send_frame(session, {"type": "error", "reason": reason})
                await ctx.fiber.dispose()
                return

        await self._send_frame(
            session,
            {
                "type": "plugin.ready",
                "plugin_id": pid,
                "protocol_version": 1,
                "enabled": session.proxy.enabled if session.proxy else True,
                "scopes": sorted(session.scopes),
                "node_region": session.node_region,
            },
        )

        # Phase 3: Active message loop, 20s heartbeat, and 60s idle timeout on tracked fiber tasks.
        async def _reader() -> None:
            nonlocal last_rx
            while True:
                remaining = max(0.01, self.idle_timeout - (loop.time() - last_rx))
                try:
                    async with asyncio.timeout(remaining):
                        raw = await receive()
                except TimeoutError:
                    if (loop.time() - last_rx) >= self.idle_timeout:
                        session.close_code = 1000
                        return
                    continue
                last_rx = loop.time()
                session.last_seen_at = self.clock()
                frame = decode_ws_json_frame(raw, max_frame_bytes, error_prefix="plugin")
                ftype = frame.get("type")
                if ftype == "ping":
                    pong: dict[str, Any] = {"type": "pong", "ts": self.clock()}
                    if isinstance(frame.get("id"), str):
                        pong["id"] = frame["id"]
                    await self._send_frame(session, pong)
                elif ftype == "pong":
                    continue
                elif ftype == "rpc.result":
                    req_id = frame.get("id")
                    pending = session.pending.get(req_id) if isinstance(req_id, str) else None
                    if pending is not None and not pending.future.done():
                        pending.future.set_result(frame)
                elif ftype == "world.event":
                    await self._handle_world_event(session, frame)
                elif ftype == "chat.inbound":
                    await self._handle_chat_inbound(session, frame)
                elif ftype == "owner.notify":
                    await self._handle_owner_notify(session, frame)
                elif ftype == "ai.generate":
                    await self._handle_ai_generate(session, frame)
                else:
                    raise Rejected("plugin_unknown_frame")

        async def _heartbeat() -> None:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                await self._send_frame(session, {"type": "ping", "ts": self.clock()})

        reader_task = ctx.spawn(_reader(), name=f"plugin-reader:{pid}")
        heartbeat_task = ctx.spawn(_heartbeat(), name=f"plugin-heartbeat:{pid}")
        try:
            done, _ = await asyncio.wait(
                [reader_task, heartbeat_task], return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if not t.cancelled():
                    t.result()
        except asyncio.CancelledError:
            raise
        except Rejected as exc:
            code = str(exc)
            if code == "plugin_scope_rejected":
                log.warning("event=plugin_scope_rejected plugin=%s reason=frame_scope", pid)
                if hasattr(self.service, "record"):
                    self.service.record("plugin_scope_rejected", emit_audit=False)
                session.close_code = 1008
            elif code in {"plugin_invalid_schema", "plugin_invalid_json", "plugin_frame_too_large"}:
                log.warning("event=plugin_scope_rejected plugin=%s reason=%s", pid, code)
                session.close_code = 1008
            else:
                session.close_code = 1012
            await self._send_frame(session, {"type": "error", "reason": code})
        except Exception:
            session.close_code = 1012
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await self._touch_last_seen(pid, session.last_seen_at)
                except Exception:
                    pass
                await ctx.fiber.dispose()
