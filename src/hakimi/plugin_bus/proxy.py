"""Remote plugin session state and dynamic BaseModule proxy."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from hakimi_plugin_sdk.protocol import BLOB_FIELD, sanitize_blob_meta_str

from ..config import Settings
from ..models import Message
from ..modules.actions import Action, NoArgs
from ..modules.adapters import _FAREWELL_BYPASS
from ..modules.base import AttentionLevel, BaseModule, GoalResult, NodeRegion, sanitize_sensor_text
from ..modules.bili_schedule import BiliScheduleOptions
from ..modules.scheduling import LocalActivity
from ..plugin_kernel import PluginContext, PluginFiber
from ..security import Rejected
from ..social import bound_owner, confirmed_message_id, owner_binding
from ..social_chat_config import (
    RemoteChatConfig,
    cache_remote_chat_config,
    cached_remote_chat_config,
    normalize_remote_chat_ids,
)
from ..thinking_policy import ThinkingScene
from .protocol import (
    _CODE_RE,
    _CUSTOM_RPC_RE,
    _MAX_MEDIA_IMAGE_BYTES,
    _MAX_PLUGIN_AUDIO_BLOB_BYTES,
    _MAX_VOICE_AUDIO_BYTES,
    _RESERVED_CONTROL_RPCS,
    _SENSOR_RPC_TIMEOUT_SECONDS,
    _VALID_MEDIA_KINDS,
    _VALID_VOICE_FORMATS,
    CONFIG_UPDATE_RPC,
    RESERVED_CHAT_PLATFORMS,
    _sanitize_payload,
    _scrub_json_secrets,
)

if TYPE_CHECKING:
    from .bus import PluginBus

_RESERVED_DELIVERY_RPCS = _RESERVED_CONTROL_RPCS


def _is_valid_image_magic(raw: bytes) -> bool:
    if raw.startswith(b"\xff\xd8\xff"):
        return True
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return True
    if raw.startswith((b"GIF87a", b"GIF89a")):
        return True
    return len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"


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
    event_subscriptions: frozenset[str] = frozenset()
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending: dict[str, _PendingRpc] = field(default_factory=dict)
    uncertain_deliveries: set[str] = field(default_factory=set)
    _bili_private_handles: dict[str, dict[str, Any]] = field(default_factory=dict)
    inflight_node_calls: int = 0
    inflight_ai_calls: int = 0
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
        attention_level: AttentionLevel = "LOW",
        admin_panel: str | None = None,
        configuration_issue: str | None = None,
        initial_status: dict[str, Any] | None = None,
        action_timeouts: dict[str, float] | None = None,
        session_exit_action: str | None = None,
        voice_format: str | None = None,
    ) -> None:
        super().__init__(settings)
        self.module_id = plugin_id
        self.display_name = display_name
        self.description = description
        self.node_region = node_region
        self.attention_level = attention_level
        self.admin_panel = admin_panel
        if plugin_id == "bilibili_surf":
            self.schedule_options = BiliScheduleOptions
        self._bus = bus
        self._session = session
        self._scopes = scopes
        self.chat_platforms = chat_platforms
        self.recipient_namespaces = (
            tuple(
                plat
                for plat in chat_platforms
                if plat not in {"boundowner", "owner"} and re.fullmatch(r"[a-z][a-z0-9_]*", plat)
            )
            if ("chat" in scopes and chat_platforms)
            else ()
        )
        self.thinking_scenes = thinking_scenes
        self.thinking_scene_keys = frozenset(scene.key for scene in thinking_scenes)
        self._actions = actions
        self.capabilities = capabilities
        cap_set = set(capabilities)
        self.supports_sticker = bool({"sticker", "supports_sticker"} & cap_set)
        self.supports_rich_card = bool({"rich_card", "supports_rich_card"} & cap_set)
        self.supports_emoji_like = bool({"emoji_like", "supports_emoji_like"} & cap_set)
        self.supports_poke = bool({"poke", "supports_poke"} & cap_set)
        self.supports_reply_anchor = bool({"reply_anchor", "supports_reply_anchor"} & cap_set)
        self.supports_voice = bool({"voice", "supports_voice"} & cap_set)
        self.supports_voice_blob = bool({"voice_blob", "supports_voice_blob"} & cap_set)
        self.supports_media_blob = bool(
            {"media_blob", "supports_media_blob", "voice_blob", "supports_voice_blob"} & cap_set
        )
        self.supports_typing = bool({"typing", "supports_typing"} & cap_set)
        self.supports_quick_ack = bool({"quick_ack", "supports_quick_ack"} & cap_set)
        self.preferred_voice_format = voice_format or "ogg_opus"
        self.enabled = enabled
        self._configuration_issue: str | None = configuration_issue
        self._remote_status: dict[str, Any] = (
            _sanitize_payload(initial_status, settings) if isinstance(initial_status, dict) else {}
        )
        self._remote_configs: dict[str, RemoteChatConfig] = {}
        self._last_sensor_summary: str = ""
        self._action_unavailable: dict[str, str] = {}
        self._current_activity: str | None = None
        self._action_timeouts: dict[str, float] = dict(action_timeouts or {})
        self.session_exit_action: str | None = session_exit_action

    @property
    def configuration_issue(self) -> str | None:
        return self._configuration_issue

    def status_dict(self) -> dict[str, Any]:
        return _sanitize_payload(self._remote_status, self.settings)

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

    def _resolve_chat_platform(self, platform: str | None = None) -> str:
        if not self.chat_platforms:
            return ""
        if platform is None:
            return self.chat_platforms[0]
        return platform if platform in self.chat_platforms else ""

    def _effective_remote_config(
        self, platform: str, settings: Any = None
    ) -> RemoteChatConfig | None:
        cfg = self._remote_configs.get(platform)
        if cfg is not None:
            return cfg
        return cached_remote_chat_config(
            settings if settings is not None else self.settings, platform
        )

    def _configured_allowed_groups(self, platform: str, settings: Any = None) -> set[str] | None:
        cfg = self._effective_remote_config(platform, settings)
        if cfg is not None:
            return set(cfg.allowed_groups)
        if "allowed_groups" in self._remote_status:
            raw = self._remote_status.get("allowed_groups")
            return set(normalize_remote_chat_ids(raw, strict=False, platform=platform))
        return None

    def allowed_groups(self, platform: str | None = None) -> list[str]:
        if "chat" not in self._scopes or not self.chat_platforms:
            return []
        plat = self._resolve_chat_platform(platform)
        if not plat:
            return []
        configured = self._configured_allowed_groups(plat)
        if configured is not None:
            return sorted(configured)
        raw_pro = self._remote_status.get("proactive_groups")
        if isinstance(raw_pro, list):
            return list(normalize_remote_chat_ids(raw_pro, strict=False, platform=plat))
        return []

    def apply_remote_config(
        self,
        config: RemoteChatConfig | dict[str, Any] | None,
        *,
        platform: str | None = None,
    ) -> None:
        """Apply or clear a server-side RemoteChatConfig override in proxy memory."""
        plat = self._resolve_chat_platform(platform)
        if not plat:
            return
        if config is None:
            self._remote_configs.pop(plat, None)
            cache_remote_chat_config(self.settings, plat, None)
            env_raw = self._remote_status.get("env_defaults")
            if isinstance(env_raw, dict):
                env_cfg = RemoteChatConfig.from_untrusted(env_raw, platform=plat)
                self._remote_status["allowed_groups"] = list(env_cfg.allowed_groups)
                self._remote_status["proactive_groups"] = list(env_cfg.proactive_groups)
                self._remote_status["allowed_users"] = list(env_cfg.allowed_users)
                self._remote_status["private_mode"] = env_cfg.private_mode
                self._remote_status["allowed_groups_count"] = len(env_cfg.allowed_groups)
            self._remote_status["memory_mirror_enabled"] = False
            self._remote_status["config_source"] = "env"
            return

        cfg = (
            config
            if isinstance(config, RemoteChatConfig)
            else RemoteChatConfig.model_validate(config)
        )
        self._remote_configs[plat] = cfg
        cache_remote_chat_config(self.settings, plat, cfg)
        self._remote_status["allowed_groups"] = list(cfg.allowed_groups)
        self._remote_status["proactive_groups"] = list(cfg.proactive_groups)
        self._remote_status["allowed_users"] = list(cfg.allowed_users)
        self._remote_status["private_mode"] = cfg.private_mode
        self._remote_status["memory_mirror_enabled"] = bool(cfg.memory_mirror_enabled)
        self._remote_status["allowed_groups_count"] = len(cfg.allowed_groups)
        self._remote_status["config_source"] = "admin"

    async def push_config(
        self,
        config: RemoteChatConfig | dict[str, Any] | None,
        *,
        platform: str | None = None,
        reset_to_env: bool = False,
    ) -> bool:
        """Apply config locally and hot-push via `config.update` RPC when plugin is online."""
        plat = self._resolve_chat_platform(platform)
        if reset_to_env or config is None:
            self.apply_remote_config(None, platform=plat or None)
            params: dict[str, Any] = {"reset_to_env": True}
            if plat:
                params["platform"] = plat
        else:
            cfg = (
                config
                if isinstance(config, RemoteChatConfig)
                else RemoteChatConfig.model_validate(config)
            )
            self.apply_remote_config(cfg, platform=plat or None)
            params = cfg.to_rpc_params(platform=plat or None)

        if not self._session.ready or self._session.closed:
            return False
        try:
            resp = await self._bus._rpc(
                self._session,
                CONFIG_UPDATE_RPC,
                params,
                timeout=min(self._bus.rpc_timeout, 8.0),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        if not isinstance(resp, dict) or not resp.get("ok", False):
            return False
        raw_res = resp.get("result") if "result" in resp else resp.get("data")
        if isinstance(raw_res, dict) and isinstance(raw_res.get("status"), dict):
            self._remote_status = _sanitize_payload(raw_res["status"], self.settings)
        return True

    def channel_prompt(self, self_id: str, platform: str) -> str:
        """Build a channel identity and capability prompt block from declared flags."""
        plat = str(platform or "").strip() or (
            self.chat_platforms[0] if self.chat_platforms else self.module_id
        )
        ident = str(self_id or "").strip() or self._bot_id()
        return super().channel_prompt(ident, plat)

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
            known_groups = self._configured_allowed_groups(message.platform, settings)
            if known_groups is not None and message.conversation_id not in known_groups:
                return False
            return bool(
                message.invites_group_review
                or message.attention_reason == "possible_alias"
                or policy == "smart_reply"
            )
        cfg = self._effective_remote_config(message.platform, settings)
        if cfg is not None:
            if bound_owner(message, settings):
                return True
            return (
                cfg.private_mode == "owner_and_allowlist" and message.user_id in cfg.allowed_users
            )
        return True

    def chat_connected(self, platform: str) -> bool:
        return bool(
            self.enabled
            and "chat" in self._scopes
            and platform in self.chat_platforms
            and self._session.ready
            and not self._session.closed
            and self._remote_status.get("connected", True) is not False
        )

    def bot_id_for(self, platform: str) -> str:
        return self._bot_id() if platform in self.chat_platforms else ""

    def _bot_id(self) -> str:
        raw_bot_id = self._remote_status.get("bot_id")
        if isinstance(raw_bot_id, str) and raw_bot_id.strip():
            return raw_bot_id.strip()[:64]
        if self.settings is not None:
            for plat in self.chat_platforms:
                binding = owner_binding(self.settings, plat)
                if binding is not None and binding.bot_id:
                    return binding.bot_id
        return self.module_id

    def _proactive_groups(self) -> list[str]:
        if "chat" not in self._scopes or not self.chat_platforms:
            return []
        primary = self.chat_platforms[0]
        cfg = self._effective_remote_config(primary)
        if cfg is not None:
            raw_groups: Any = list(cfg.proactive_groups)
        else:
            raw_groups = self._remote_status.get("proactive_groups")
        if not isinstance(raw_groups, list):
            return []
        policies = getattr(self.settings, "group_policies", {}) or {}
        allowed_plats = set(self.chat_platforms)
        out: list[str] = []
        seen: set[str] = set()
        for item in raw_groups:
            if not isinstance(item, str):
                continue
            cleaned = item.strip()
            if not cleaned:
                continue
            if ":" in cleaned:
                plat, _, gid = cleaned.partition(":")
                gid = gid.strip()
                if plat not in allowed_plats or not gid:
                    continue
                scope = f"{plat}:{gid}"
            else:
                scope = f"{primary}:{cleaned}"
            if policies.get(scope, "at_only") == "muted":
                continue
            if scope not in seen:
                seen.add(scope)
                out.append(scope)
                if len(out) >= 100:
                    break
        return out

    def proactive_groups(self) -> list[str]:
        if not self.enabled or not self._session.ready or self._session.closed:
            return []
        return self._proactive_groups()

    def normalize_recipient(self, scope: str) -> str | None:
        raw = str(scope or "").strip()
        if not raw or ":" not in raw:
            return None
        plat, _, gid = raw.partition(":")
        gid = gid.strip()
        if plat not in self.chat_platforms or not gid:
            return None
        canonical = f"{plat}:{gid}"
        return canonical if canonical in self._proactive_groups() else None

    def permits_recipient(self, scope: str, config: Any) -> bool:
        if not self.enabled:
            return False
        plat, sep, gid = str(scope or "").partition(":")
        if not sep or plat not in self.chat_platforms or not gid:
            return False
        return scope in self._proactive_groups()

    def allowed_proactive(self, message: Message, settings: Any) -> bool:
        if not self.chat_connected(message.platform):
            return False
        if message.kind == "private":
            return bound_owner(message, settings)
        if message.kind == "group":
            scope = f"{message.platform}:{message.conversation_id}"
            policies = getattr(settings, "group_policies", {}) or {}
            if policies.get(scope, "at_only") == "muted":
                return False
            return scope in self._proactive_groups()
        return False

    def permits_chat_export(self, message: Message, consumer: str) -> bool:
        return message.platform in self.chat_platforms and consumer in {
            "bilibili_surf",
            "patrol_diary",
        }

    def permits_memory_mirror(self, message: Message, settings: Any) -> bool:
        if (
            message is None
            or message.kind != "group"
            or message.platform not in self.chat_platforms
            or message.platform in RESERVED_CHAT_PLATFORMS
        ):
            return False
        cfg = self._effective_remote_config(message.platform, settings)
        if cfg is None or cfg.memory_mirror_enabled is not True:
            return False
        if message.conversation_id not in set(self.allowed_groups(message.platform)):
            return False
        return bool(self.authorize_chat(message.model_copy(update={"addressed": True}), settings))

    async def planning_context(self, service: Any, config: Any, now: float) -> dict[str, Any]:
        if (
            "chat" not in self._scopes
            or not self.chat_platforms
            or not self.enabled
            or not self._session.ready
            or self._session.closed
            or self._remote_status.get("connected", True) is False
        ):
            return {}
        groups = self._proactive_groups()[:20]
        reg = getattr(self, "_registry", None) or getattr(service, "modules", None)
        engine = getattr(service, "life_engine", None)
        delivery_enabled = (
            reg.owner_delivery_enabled(config, settings=self.settings, engine=engine)
            if reg is not None
            else bool(getattr(getattr(service, "proactive", None), "delivery_enabled", False))
        )
        owner_dm = bool(
            delivery_enabled
            and self.settings is not None
            and any(owner_binding(self.settings, plat) is not None for plat in self.chat_platforms)
        )
        ctx: dict[str, Any] = {}
        if groups:
            ctx["available_groups"] = groups
        if owner_dm:
            ctx["owner_dm_available"] = True
        return ctx

    async def _deliver_rpc(
        self,
        method: str,
        payload: dict[str, Any],
        *,
        before_send=None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> str:
        delivery_id = uuid.uuid4().hex
        if before_send is not None:
            allowed = await _call_before_send(before_send, delivery_id)
            if not allowed:
                raise Rejected("reply_invalidated")
        if not self._session.ready or self._session.closed:
            raise Rejected("plugin_disconnected")
        params = {"delivery_id": delivery_id, **payload}
        try:
            resp = await self._bus._rpc(
                self._session,
                method,
                params,
                timeout=timeout,
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

    async def send_chat(
        self,
        message: Message,
        text: str,
        *,
        reply_to: str | None = None,
        before_send=None,
    ) -> str:
        if "chat" not in self._scopes or message.platform not in self.chat_platforms:
            raise Rejected("delivery_not_authorized")
        if not self.enabled:
            raise Rejected("module_disabled")
        effective_reply_to = (
            reply_to.strip()
            if self.supports_reply_anchor and isinstance(reply_to, str) and reply_to.strip()
            else None
        )
        return await self._deliver_rpc(
            "delivery.send",
            {
                "platform": message.platform,
                "bot_id": message.bot_id,
                "kind": message.kind,
                "conversation_id": message.conversation_id,
                "user_id": message.user_id,
                "text": text,
                "reply_to": effective_reply_to,
            },
            before_send=before_send,
        )

    async def send_voice(
        self,
        message: Message,
        audio_bytes: bytes,
        *,
        fmt: str,
        mime_type: str,
        duration_ms: int | None = None,
        title: str | None = None,
        reply_to: str | None = None,
        before_send=None,
    ) -> str:
        if "chat" not in self._scopes or message.platform not in self.chat_platforms:
            raise Rejected("delivery_not_authorized")
        if not self.supports_voice:
            raise Rejected("voice_not_supported")
        if not self.enabled:
            raise Rejected("module_disabled")
        if (
            fmt not in _VALID_VOICE_FORMATS
            or not isinstance(audio_bytes, (bytes, bytearray))
            or len(audio_bytes) <= 0
        ):
            raise Rejected("invalid_voice_audio")
        raw_audio = bytes(audio_bytes)
        if len(raw_audio) > _MAX_PLUGIN_AUDIO_BLOB_BYTES:
            raise Rejected("voice_payload_too_large")

        clean_title = (
            sanitize_sensor_text(str(title), self.settings, max_tokens=24)[:120].strip()
            if isinstance(title, str) and title.strip()
            else None
        )
        effective_reply_to = (
            reply_to.strip()
            if self.supports_reply_anchor and isinstance(reply_to, str) and reply_to.strip()
            else None
        )
        base_payload: dict[str, Any] = {
            "platform": message.platform,
            "bot_id": message.bot_id,
            "kind": message.kind,
            "conversation_id": message.conversation_id,
            "user_id": message.user_id,
            "format": fmt,
            "mime_type": mime_type,
            "duration_ms": duration_ms,
            "reply_to": effective_reply_to,
            "kind_hint": "song" if clean_title is not None else "voice",
        }
        if clean_title is not None:
            base_payload["title"] = clean_title

        if len(raw_audio) <= _MAX_VOICE_AUDIO_BYTES:
            inline_payload = {
                **base_payload,
                "audio_b64": base64.b64encode(raw_audio).decode("ascii"),
            }
            probe = json.dumps(
                {
                    "type": "rpc.request",
                    "id": "0" * 32,
                    "method": "delivery.send_voice",
                    "params": {"delivery_id": "0" * 32, **inline_payload},
                },
                ensure_ascii=False,
                allow_nan=False,
            )
            if len(probe.encode("utf-8")) <= self._bus._max_frame_bytes():
                return await self._deliver_rpc(
                    "delivery.send_voice",
                    inline_payload,
                    before_send=before_send,
                )

        if not self.supports_voice_blob:
            raise Rejected("voice_payload_too_large")
        if not self._session.ready or self._session.closed:
            raise Rejected("plugin_disconnected")

        upload_timeout = max(self._bus.rpc_timeout, 60.0 if len(raw_audio) > 512_000 else 30.0)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + upload_timeout
        ext = "ogg" if fmt == "ogg_opus" else fmt
        blob_filename = f"{clean_title}.{ext}" if clean_title else f"voice.{ext}"
        blob_desc, uploaded_transfer_id = await self._bus._prepare_request_blob(
            self._session,
            raw_audio,
            rpc_params={"delivery_id": "0" * 32, **base_payload},
            blob_mime_type=mime_type,
            blob_filename=blob_filename,
            req_cap=_MAX_PLUGIN_AUDIO_BLOB_BYTES,
            deadline=deadline,
        )
        blob_payload = {**base_payload, BLOB_FIELD: blob_desc}
        try:
            rem = max(5.0, deadline - loop.time())
            return await self._deliver_rpc(
                "delivery.send_voice",
                blob_payload,
                before_send=before_send,
                timeout=rem,
            )
        except Exception:
            if uploaded_transfer_id is not None:
                await self._bus._best_effort_release_blob(
                    self._session, transfer_id=uploaded_transfer_id
                )
            raise

    async def send_media(
        self,
        message: Message,
        media_bytes: bytes,
        *,
        kind: Literal["sticker", "card", "image"] = "image",
        mime_type: str = "image/png",
        filename: str = "media.png",
        reply_to: str | None = None,
        before_send=None,
    ) -> str:
        if "chat" not in self._scopes or message.platform not in self.chat_platforms:
            raise Rejected("delivery_not_authorized")
        if not self.enabled:
            raise Rejected("module_disabled")
        if kind not in _VALID_MEDIA_KINDS:
            raise Rejected("invalid_media_kind")
        if kind == "sticker" and not self.supports_sticker:
            raise Rejected("media_not_supported")
        if kind == "card" and not self.supports_rich_card:
            raise Rejected("media_not_supported")
        if kind == "image" and not (self.supports_sticker or self.supports_rich_card):
            raise Rejected("media_not_supported")
        if not isinstance(media_bytes, (bytes, bytearray)) or len(media_bytes) <= 0:
            raise Rejected("invalid_media_image")
        raw_media = bytes(media_bytes)
        if len(raw_media) > _MAX_MEDIA_IMAGE_BYTES:
            raise Rejected("media_payload_too_large")
        if not _is_valid_image_magic(raw_media):
            raise Rejected("invalid_media_image")

        clean_mime = sanitize_blob_meta_str(mime_type, max_len=96, default="image/png")
        clean_filename = sanitize_blob_meta_str(
            filename, max_len=160, default="media.png", is_filename=True
        )
        effective_reply_to = (
            reply_to.strip()
            if self.supports_reply_anchor and isinstance(reply_to, str) and reply_to.strip()
            else None
        )
        base_payload: dict[str, Any] = {
            "platform": message.platform,
            "bot_id": message.bot_id,
            "kind": message.kind,
            "conversation_id": message.conversation_id,
            "user_id": message.user_id,
            "media_kind": kind,
            "mime_type": clean_mime,
            "filename": clean_filename,
            "reply_to": effective_reply_to,
        }

        inline_payload = {
            **base_payload,
            "media_b64": base64.b64encode(raw_media).decode("ascii"),
        }
        probe = json.dumps(
            {
                "type": "rpc.request",
                "id": "0" * 32,
                "method": "delivery.send_media",
                "params": {"delivery_id": "0" * 32, **inline_payload},
            },
            ensure_ascii=False,
            allow_nan=False,
        )
        if len(probe.encode("utf-8")) <= self._bus._max_frame_bytes():
            return await self._deliver_rpc(
                "delivery.send_media",
                inline_payload,
                before_send=before_send,
            )

        if not self.supports_media_blob:
            raise Rejected("media_payload_too_large")
        if not self._session.ready or self._session.closed:
            raise Rejected("plugin_disconnected")

        upload_timeout = max(self._bus.rpc_timeout, 60.0 if len(raw_media) > 512_000 else 30.0)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + upload_timeout
        blob_desc, uploaded_transfer_id = await self._bus._prepare_request_blob(
            self._session,
            raw_media,
            rpc_params={"delivery_id": "0" * 32, **base_payload},
            blob_mime_type=clean_mime,
            blob_filename=clean_filename,
            req_cap=_MAX_MEDIA_IMAGE_BYTES,
            deadline=deadline,
        )
        blob_payload = {**base_payload, BLOB_FIELD: blob_desc}
        try:
            rem = max(5.0, deadline - loop.time())
            return await self._deliver_rpc(
                "delivery.send_media",
                blob_payload,
                before_send=before_send,
                timeout=rem,
            )
        except Exception:
            if uploaded_transfer_id is not None:
                await self._bus._best_effort_release_blob(
                    self._session, transfer_id=uploaded_transfer_id
                )
            raise

    async def send_emoji_like(
        self,
        message: Message,
        target_message_id: str,
        emoji_id: str,
        *,
        emoji: str | None = None,
        before_send=None,
    ) -> bool:
        if (
            "chat" not in self._scopes
            or message.platform not in self.chat_platforms
            or not self.enabled
            or not self.supports_emoji_like
            or not self._session.ready
            or self._session.closed
        ):
            return False
        target_id = str(target_message_id or "").strip()
        if not target_id or len(target_id) > 128:
            return False
        clean_emoji_id = str(emoji_id or "").strip()
        clean_emoji = str(emoji or "").strip()
        if not clean_emoji_id and not clean_emoji:
            return False
        if before_send is not None:
            allowed = await _call_before_send(before_send, uuid.uuid4().hex)
            if not allowed:
                raise Rejected("reply_invalidated")
        if not self._session.ready or self._session.closed:
            return False
        try:
            resp = await self._bus._rpc(
                self._session,
                "delivery.react",
                {
                    "platform": message.platform,
                    "bot_id": message.bot_id,
                    "kind": message.kind,
                    "conversation_id": message.conversation_id,
                    "user_id": message.user_id,
                    "message_id": target_id,
                    "emoji_id": clean_emoji_id,
                    "emoji": clean_emoji,
                },
                timeout=min(self._bus.rpc_timeout, 8.0),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        if not isinstance(resp, dict) or not resp.get("ok", False):
            return False
        res = resp.get("result") if "result" in resp else resp.get("data")
        if isinstance(res, dict):
            return bool(res.get("applied", True))
        return True

    async def send_typing(self, message: Message) -> None:
        if (
            "chat" not in self._scopes
            or message.platform not in self.chat_platforms
            or not self.enabled
            or not self.supports_typing
            or not self._session.ready
            or self._session.closed
        ):
            return None
        try:
            await self._bus._rpc(
                self._session,
                "delivery.typing",
                {
                    "platform": message.platform,
                    "bot_id": message.bot_id,
                    "kind": message.kind,
                    "conversation_id": message.conversation_id,
                    "user_id": message.user_id,
                },
                timeout=min(self._bus.rpc_timeout, 2.0),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        return None

    async def call_custom_rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout: float = 15.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        if (
            not isinstance(method, str)
            or not _CUSTOM_RPC_RE.fullmatch(method)
            or method in _RESERVED_DELIVERY_RPCS
        ):
            raise Rejected("plugin_invalid_method")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise Rejected("plugin_invalid_params")
        resp = await self._bus._rpc(self._session, method, params, timeout=timeout)
        if not resp.get("ok", False):
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_rpc_failed"
            raise Rejected(code)
        raw_res = resp.get("result") if "result" in resp else resp.get("data")
        if raw_res is None:
            return {}
        if isinstance(raw_res, dict):
            return _sanitize_payload(raw_res, self.settings)
        return {"value": _sanitize_payload(raw_res, self.settings)}

    async def call_raw_rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float = 15.0,  # noqa: ASYNC109
    ) -> Any:
        if (
            not isinstance(method, str)
            or not _CUSTOM_RPC_RE.fullmatch(method)
            or method in _RESERVED_DELIVERY_RPCS
        ):
            raise Rejected("plugin_invalid_method")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise Rejected("plugin_invalid_params")
        resp = await self._bus._rpc(self._session, method, params, timeout=timeout)
        if not resp.get("ok", False):
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_rpc_failed"
            raise Rejected(code)
        raw_res = resp.get("result") if "result" in resp else resp.get("data")
        return _scrub_json_secrets(raw_res, self._bus._all_secrets())

    def local_activity(
        self, context: dict, available: set[str], unfunded: bool
    ) -> LocalActivity | None:
        if self.module_id == "bilibili_surf" and unfunded and "bili.patrol" in available:
            return LocalActivity(
                30,
                {
                    "action_name": "bili.patrol",
                    "delay_minutes": 1,
                    "ttl_minutes": 60,
                    "intent_note": "用可用的巡逻额度去看看首页和关注动态",
                },
                one_outstanding=True,
                focus=(self.attention_level, "curious"),
            )
        return None

    def local_focus(self, context: dict, hour: int) -> tuple[AttentionLevel, str] | None:
        if self.module_id == "bilibili_surf" and (
            context.get("recent_bili_surfed") or hour in {13, 14, 20, 21}
        ):
            return (self.attention_level, "curious")
        return None

    async def current_activity(self) -> str | None:
        return self._current_activity

    def planning_fingerprint(self) -> str:
        st = self.status_dict()
        if not isinstance(st, dict):
            return ""
        parts: list[str] = []
        for key in ("connected", "paused", "bridge_ready"):
            if key in st and isinstance(st[key], bool):
                parts.append(f"{key}={int(st[key])}")
        return ",".join(parts)

    async def context_data(self, *, limit: int, now: float, message: Message | None = None) -> dict:
        if self.module_id == "bilibili_surf":
            bw = getattr(self._bus.service, "bili_watcher", None)
            if bw is not None and hasattr(bw, "recent_surfed"):
                return {
                    "recent_bili_surfed": await bw.recent_surfed(
                        limit=limit, now=now, message=message
                    )
                }
            return {}
        if "context_data" in self.capabilities and self._session.ready and not self._session.closed:
            try:
                resp = await self._bus._rpc(
                    self._session,
                    "context.read",
                    {"limit": int(limit), "now": float(now)},
                    timeout=min(self._bus.rpc_timeout, _SENSOR_RPC_TIMEOUT_SECONDS),
                )
                if resp.get("ok", True):
                    raw = resp.get("result") if "result" in resp else resp.get("data")
                    if isinstance(raw, dict):
                        return _sanitize_payload(raw, self.settings)
            except asyncio.CancelledError:
                raise
            except Exception:
                return {}
        return {}

    async def public_patrol_entry(self, event: Any) -> tuple[str, str] | None:
        if self.module_id != "bilibili_surf":
            return None
        if getattr(event, "event_type", None) not in {
            "bili_video_watched",
            "bili_mc_inspiration",
        }:
            return None
        payload = getattr(event, "payload", None)
        if not isinstance(payload, dict):
            payload = {}
        bvid = str(payload.get("bvid") or "")
        entry = None
        bw = getattr(self._bus.service, "bili_watcher", None)
        if bw is not None and hasattr(bw, "lookup_watched") and not payload.get("private_first"):
            entry = await bw.lookup_watched(bvid, limit=1200)
        return "bili-account:" + bvid, entry["prior_patrol_notes"] if entry else ""

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        if not self.enabled:
            return GoalResult(ok=False, module_id=self.module_id, reason="module_disabled")
        if not self._session.ready or self._session.closed:
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                reason="plugin_disconnected",
                summary=sanitize_sensor_text(
                    self._last_sensor_summary, self.settings, max_tokens=90
                ),
            )
        if not isinstance(ticket, dict):
            return GoalResult(ok=False, module_id=self.module_id, reason="invalid_action_arguments")
        action_name = str(ticket.get("action_name") or "").strip()
        action_type = str(ticket.get("action_type") or "").strip()
        matched_spec: Action | None = None
        if action_name:
            matched_spec = next((a for a in self._actions if a.name == action_name), None)
        elif action_type:
            if "." in action_type:
                action_name = action_type
                matched_spec = next((a for a in self._actions if a.name == action_name), None)
            else:
                for spec in self._actions:
                    if any(
                        alias[0] == self.module_id and alias[1] == action_type
                        for alias in spec.aliases
                    ):
                        matched_spec = spec
                        action_name = spec.name
                        break
                if not action_name:
                    for spec in self._actions:
                        if spec.name.endswith("." + action_type):
                            matched_spec = spec
                            action_name = spec.name
                            break
                if not action_name:
                    prefix = "mc" if self.module_id == "minecraft" else self.module_id
                    action_name = f"{prefix}.{action_type}"
        else:
            return GoalResult(ok=False, module_id=self.module_id, reason="invalid_action_arguments")

        if isinstance(ticket.get("arguments"), dict):
            args_dict = dict(ticket["arguments"])
        elif matched_spec is not None and matched_spec.arguments is not NoArgs:
            args_dict = {
                k: ticket[k]
                for k in matched_spec.arguments.model_fields
                if k in ticket and ticket[k] is not None
            }
        else:
            envelope_keys = {
                "id",
                "action_name",
                "action_type",
                "origin",
                "target_module",
                "target_scope",
                "now",
                "mood",
                "gate_condition",
                "intent_note",
                "source_module",
                "source_event_id",
                "dedupe_key",
                "execute_after",
                "expire_at",
            }
            args_dict = {
                k: v for k, v in ticket.items() if k not in envelope_keys and v is not None
            }

        try:
            resp = await self._bus._rpc(
                self._session,
                "action.run",
                {
                    "action": action_name,
                    "arguments": args_dict,
                    "origin": str(ticket.get("origin") or "admin"),
                    "target_scope": str(ticket.get("target_scope") or ""),
                    "now": float(ticket.get("now") or self._bus.clock()),
                },
                timeout=self._action_timeouts.get(action_name),
            )
        except Rejected as exc:
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                reason=sanitize_sensor_text(str(exc), self.settings, max_tokens=20)
                or "plugin_rpc_failed",
                summary=sanitize_sensor_text(
                    self._last_sensor_summary, self.settings, max_tokens=90
                ),
            )
        if not resp.get("ok", False):
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_action_failed"
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                reason=code,
                summary=sanitize_sensor_text(
                    self._last_sensor_summary, self.settings, max_tokens=90
                ),
            )

        raw_res: Any
        if "result" in resp and resp["result"] is not None:
            raw_res = resp["result"]
        elif "data" in resp and resp["data"] is not None:
            raw_res = resp["data"]
        elif any(k in resp for k in ("status", "reason", "details", "summary", "artifacts")):
            raw_res = {k: v for k, v in resp.items() if k not in {"type", "id", "ok"}}
        else:
            raw_res = resp.get("result")

        if not isinstance(raw_res, dict):
            return GoalResult(
                ok=True,
                module_id=self.module_id,
                summary=sanitize_sensor_text(
                    str(raw_res) if raw_res is not None else self._last_sensor_summary,
                    self.settings,
                    max_tokens=90,
                ),
            )

        details = raw_res.get("details")
        inner = details.get("result") if isinstance(details, dict) else None
        if isinstance(inner, dict) and "ok" in inner:
            ok = bool(inner.get("ok")) and raw_res.get("status", "done") == "done"
            summary = sanitize_sensor_text(
                str(inner.get("summary") or ""), self.settings, max_tokens=90
            )
            raw_art = inner.get("artifacts")
            artifacts = (
                _sanitize_payload(raw_art, self.settings) if isinstance(raw_art, dict) else {}
            )
            raw_reason = inner.get("reason")
            if raw_reason is None and not ok:
                raw_reason = raw_res.get("reason")
            reason = (
                sanitize_sensor_text(str(raw_reason), self.settings, max_tokens=20)
                if raw_reason is not None
                else None
            )
            return GoalResult(
                ok=ok,
                module_id=self.module_id,
                summary=summary,
                artifacts=artifacts,
                reason=reason,
            )

        if "status" in raw_res:
            ok = raw_res.get("status") == "done"
            det = details if isinstance(details, dict) else {}
            summary = sanitize_sensor_text(
                str(det.get("summary") or raw_res.get("summary") or ""),
                self.settings,
                max_tokens=90,
            )
            raw_art = (
                det["artifacts"]
                if isinstance(det.get("artifacts"), dict)
                else {k: v for k, v in det.items() if k != "summary"}
            )
            artifacts = (
                _sanitize_payload(raw_art, self.settings) if isinstance(raw_art, dict) else {}
            )
            raw_reason = raw_res.get("reason")
            reason = (
                sanitize_sensor_text(str(raw_reason), self.settings, max_tokens=20)
                if raw_reason is not None and (not ok or str(raw_reason) != "ok")
                else None
            )
            return GoalResult(
                ok=ok,
                module_id=self.module_id,
                summary=summary,
                artifacts=artifacts,
                reason=reason,
            )

        ok = bool(raw_res.get("ok", True))
        summary = sanitize_sensor_text(
            str(raw_res.get("summary") or ""), self.settings, max_tokens=90
        )
        raw_art = raw_res.get("artifacts")
        artifacts = _sanitize_payload(raw_art, self.settings) if isinstance(raw_art, dict) else {}
        raw_reason = raw_res.get("reason")
        reason = (
            sanitize_sensor_text(str(raw_reason), self.settings, max_tokens=20)
            if raw_reason is not None
            else None
        )
        return GoalResult(
            ok=ok,
            module_id=self.module_id,
            summary=summary,
            artifacts=artifacts,
            reason=reason,
        )

    async def on_switch(self, service: Any) -> None:
        if (
            not self.enabled
            and self.chat_platforms
            and hasattr(service, "store")
            and hasattr(service.store, "cancel_platform_work")
        ):
            await service.store.cancel_platform_work(self.chat_platforms)
        if not self.enabled and self.module_id == "minecraft":
            world = getattr(service, "world_state", None)
            if world is not None and hasattr(world, "experience"):
                exp = world.experience
                if not getattr(exp, "ready", True) and hasattr(world, "initialize"):
                    await world.initialize()
                await exp.mc_snapshot("", world.clock(), disconnected=True)
        if not self.enabled and self.module_id == "bilibili_surf":
            bw = getattr(service, "bili_watcher", None)
            if bw is not None and hasattr(bw, "policy_epoch"):
                bw.policy_epoch += 1
            world = getattr(service, "world_state", None)
            if world is not None and hasattr(world, "experience"):
                exp = world.experience
                if not getattr(exp, "ready", True) and hasattr(world, "initialize"):
                    await world.initialize()
                ts = world.clock() if hasattr(world, "clock") else self._bus.clock()
                await exp.bili_reading(False, ts)
        if self._session.ready and not self._session.closed:
            await self._bus._send_frame(
                self._session,
                {"type": "module.switch", "enabled": bool(self.enabled)},
            )

    def admin_status(self, request: Any = None) -> dict[str, Any]:
        base = {
            "online": bool(self._session.ready and not self._session.closed),
            "node_region": self.node_region,
            "scopes": sorted(self._scopes),
            "capabilities": list(self.capabilities),
            "supports_sticker": self.supports_sticker,
            "supports_rich_card": self.supports_rich_card,
            "supports_emoji_like": self.supports_emoji_like,
            "supports_poke": self.supports_poke,
            "supports_reply_anchor": self.supports_reply_anchor,
            "supports_voice": self.supports_voice,
            "supports_typing": self.supports_typing,
            "supports_quick_ack": self.supports_quick_ack,
            "preferred_voice_format": self.preferred_voice_format,
        }
        return {**self.status_dict(), **base}
