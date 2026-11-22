"""Remote Plugin Bus: scoped credentials, reverse WebSocket RPC/events, and RemoteModuleProxy."""

from __future__ import annotations

import asyncio
import base64
import copy
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

from hakimi_plugin_sdk.protocol import (
    BLOB_FIELD,
    BLOB_HARD_MAX_BYTES,
    BLOB_RPC_GET,
    BLOB_RPC_PUT,
    BLOB_RPC_RELEASE,
    ToolCallResult,
    compute_chunk_raw_bytes,
    compute_inline_max_bytes,
    decode_blob_chunk,
    encode_blob_chunk,
    sanitize_blob_meta_str,
    sha256_hex,
    validate_blob_descriptor,
)

from .bot_server import is_persona_owner, untrusted_input
from .budget_policy import PATROL_LANE, PROACTIVE_LANE, paid_lane_available
from .config import Settings
from .generation_policy import Scenario
from .media import ModelImage
from .models import FileRef, ImageRef, Message
from .modules.actions import Action, ActionCall, NoArgs, Outcome, reject_reason
from .modules.adapters import _FAREWELL_BYPASS
from .modules.base import (
    AttentionLevel,
    BaseModule,
    GoalResult,
    NodeRegion,
    sanitize_sensor_text,
)
from .modules.bili_schedule import BiliScheduleOptions
from .modules.scheduling import ActivityQuota, LocalActivity
from .outbound_actions import require_all_schema_fields
from .plugin_kernel import PluginContext, PluginFiber, PluginKernel
from .roaming_logger import redact_public_summary
from .screen_watch import decode_b64_frames, decode_ws_json_frame
from .security import Rejected, clean_text, configured_secrets, safe_output
from .social import confirmed_message_id, owner_target, permits_chat_export
from .thinking_policy import CORE_THINKING_SCENES, THINKING_LEVELS, OutputCap, ThinkingScene
from .work_usage import model_work
from .world_state import IntentTicketCreate, private_event_audience

log = logging.getLogger("hakimi")

VALID_SCOPES = (
    "actions",
    "sensors",
    "thinking_scenes",
    "chat",
    "media",
    "owner_notify",
    "node_bridge",
)
_VALID_SCOPES_SET = frozenset(VALID_SCOPES)

RESERVED_PLUGIN_IDS = frozenset(
    {
        "qq_social",
        "identity",
        "memory_hub",
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
        "voice_gateway",
        "gcp_billing",
        "colab_compute",
        "song_studio",
        "proactive_delivery",
        "video_watch",
        "affection",
        "memory_compressor",
        "meme_library",
        "admin",
        "system",
        "core",
    }
)
HOTPLUG_PILOT_PLUGIN_IDS = frozenset({"weather", "minecraft", "bilibili_surf"})
_HOTPLUG_ADMIN_PANELS: dict[str, str] = {
    "weather": "weather",
    "minecraft": "minecraft",
    "bilibili_surf": "bili",
}
RESERVED_CHAT_PLATFORMS = frozenset({"onebot", "qq"})
_RESERVED_ACTION_PREFIXES = frozenset(
    {
        "qq",
        "social",
        "system",
        "admin",
        "core",
        "world",
        "identity",
        "plugin_bus",
    }
)
_SCOPED_ACTION_PREFIX_OWNERS: dict[str, str] = {
    "weather": "weather",
    "mc": "minecraft",
    "minecraft": "minecraft",
    "bili": "bilibili_surf",
    "bilibili": "bilibili_surf",
    "web_tools": "web_tools",
    "doc_render": "doc_render",
    "song_finder": "song_finder",
}
_SCOPED_SCENE_OWNERS: dict[str, str] = {
    "bilibili_summary": "bilibili_surf",
    "bili_selection": "bilibili_surf",
    "bili_learning": "bilibili_surf",
}
_NODE_BRIDGE_PLUGIN_ACTIONS: dict[str, frozenset[str]] = {
    "minecraft": frozenset(
        {
            "mc.status",
            "mc.dispatch_goal",
            "mc.pause",
            "mc.resume",
        }
    ),
    "bilibili_surf": frozenset(
        {
            "bili.public_get",
            "bili.resolve_short",
            "bili.request",
            "bili.cover_thumb",
            "bili.video_storyboard",
        }
    ),
}
_NODE_BRIDGE_ALLOWED_ACTIONS = frozenset().union(*_NODE_BRIDGE_PLUGIN_ACTIONS.values())
_CORE_ADMIN_PANELS = frozenset(
    {
        "login",
        "overview",
        "modules",
        "qq",
        "group-policy",
        "bili",
        "ledger",
        "api-routing",
        "reply-settings",
        "thinking",
        "filter",
        "sandbox",
        "admission",
        "memory",
        "community",
        "proactive",
        "life",
        "memes",
        "bili-account",
        "patrol",
        "drive-sync",
        "journal",
        "notebook",
        "event-ledger",
        "voice-companion",
        "screen-watch",
        "identity-center",
        "plugin-bus",
        "logs",
        "weather",
        "minecraft",
        "admin",
        "system",
        "core",
    }
)
_RESERVED_SCENE_KEYS = frozenset(
    {
        "fast_chat",
        "owner_private",
        "daily_diary",
        "memory_compression",
        "owner_digest",
        "owner_companion",
        "voice.companion",
        "screen.watch",
        *(scene.key for scene in CORE_THINKING_SCENES),
    }
)
_BVID_RE = re.compile(r"^BV[1-9A-HJ-NP-Za-km-z]{10}\Z")

PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}\Z")
PLATFORM_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}\Z")
_ACTION_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,30}\.[a-z][a-z0-9_]{0,30}\Z")
_FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,47}\Z")
_CAPABILITY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}\Z")
_EVENT_TYPE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,48}\Z")
_CODE_RE = re.compile(r"^[a-z0-9_.-]{1,64}\Z")
_ADMIN_PANEL_RE = re.compile(r"^[a-z][a-z0-9_-]{1,47}\Z")
_ALIAS_VERB_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}\Z")
_STATE_KEY_RE = re.compile(r"^[a-zA-Z0-9_.-]{1,64}\Z")
_CUSTOM_RPC_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,63}\Z")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS plugin_credentials (
  plugin_id TEXT PRIMARY KEY,
  token_salt TEXT NOT NULL,
  token_hash TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  payload BLOB NOT NULL,
  updated REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS plugin_kv_state (
  plugin_id TEXT NOT NULL,
  state_key TEXT NOT NULL,
  payload BLOB NOT NULL,
  updated REAL NOT NULL,
  PRIMARY KEY(plugin_id, state_key)
);
"""

_VAULT_CRED_PREFIX = "plugin-cred:"
_VAULT_KV_PREFIX = "plugin-kv:"
_LEGACY_KV_VALUE_BYTES = 16_384
_MAX_KV_VALUE_BYTES = 65_536
_MAX_NODE_BRIDGE_INFLIGHT = 16
_MAX_AI_CALLS_INFLIGHT = 4
_DEFAULT_MAX_FRAME_BYTES = 262_144
_DEFAULT_HEARTBEAT_SECONDS = 20.0
_DEFAULT_IDLE_TIMEOUT_SECONDS = 60.0
_DEFAULT_HANDSHAKE_TIMEOUT_SECONDS = 15.0
_DEFAULT_RPC_TIMEOUT_SECONDS = 15.0
_SENSOR_RPC_TIMEOUT_SECONDS = 3.5
_MAX_PENDING_RPCS = 32
_NOTIFY_WINDOW_SECONDS = 600.0
_NOTIFY_MAX_PER_WINDOW = 3
_BILI_PRIVATE_SHARE_PRIORITY_SECONDS = 24 * 3600
_BILI_PRIVATE_HANDLE_TTL_SECONDS = 1800.0
_BILI_PRIVATE_HANDLE_MAX_COMMITS = 2
_MAX_BILI_PRIVATE_HANDLES = 64
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
    if depth > 6:
        return "[truncated]"
    if isinstance(value, str):
        return sanitize_sensor_text(value, settings, max_tokens=val_limit)
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in list(value.items())[:32]:
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


def _secret_candidates(secrets_list: list[str]) -> list[str]:
    cands: set[str] = set()
    for raw in secrets_list:
        if not raw:
            continue
        for val in (raw, clean_text(raw)):
            if len(val) >= 6:
                cands.add(val)
    return sorted(cands, key=len, reverse=True)


def _scrub_text_secrets(text: str, candidates: list[str]) -> str:
    if not text or not candidates:
        return text
    out = text
    for secret in candidates:
        if secret in out:
            out = out.replace(secret, "[REDACTED]")
    return out


def _scrub_json_secrets(
    value: Any,
    secrets_list: list[str],
    *,
    _candidates: list[str] | None = None,
    depth: int = 0,
) -> Any:
    if depth > 32:
        return "[truncated]"
    cands = _secret_candidates(secrets_list) if _candidates is None else _candidates
    if isinstance(value, str):
        return _scrub_text_secrets(value, cands)
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            ck = _scrub_text_secrets(str(k), cands)
            out[ck] = _scrub_json_secrets(v, secrets_list, _candidates=cands, depth=depth + 1)
        return out
    if isinstance(value, list | tuple):
        return [
            _scrub_json_secrets(item, secrets_list, _candidates=cands, depth=depth + 1)
            for item in value
        ]
    return _scrub_text_secrets(str(value), cands)


def _bound_ai_payload(
    payload: dict[str, Any], secrets_list: list[str], max_bytes: int
) -> dict[str, Any]:
    cleaned = _scrub_json_secrets(payload, secrets_list)
    if not isinstance(cleaned, dict):
        raise Rejected("plugin_invalid_schema")
    if len(untrusted_input(cleaned).encode("utf-8")) <= max_bytes:
        return cleaned

    bounded = copy.deepcopy(cleaned)

    def _find_longest_str(node: Any) -> tuple[int, Any, Any] | None:
        best: tuple[int, Any, Any] | None = None
        if isinstance(node, dict):
            for k, v in node.items():
                if isinstance(v, str):
                    cand = (len(v), node, k)
                    if best is None or cand[0] > best[0]:
                        best = cand
                elif isinstance(v, dict | list):
                    sub = _find_longest_str(v)
                    if sub is not None and (best is None or sub[0] > best[0]):
                        best = sub
        elif isinstance(node, list):
            for idx, item in enumerate(node):
                if isinstance(item, str):
                    cand = (len(item), node, idx)
                    if best is None or cand[0] > best[0]:
                        best = cand
                elif isinstance(item, dict | list):
                    sub = _find_longest_str(item)
                    if sub is not None and (best is None or sub[0] > best[0]):
                        best = sub
        return best

    def _find_longest_list(node: Any) -> list[Any] | None:
        best: list[Any] | None = None
        if isinstance(node, dict):
            for v in node.values():
                if isinstance(v, list):
                    if best is None or len(v) > len(best):
                        best = v
                    sub = _find_longest_list(v)
                    if sub is not None and len(sub) > len(best):
                        best = sub
                elif isinstance(v, dict):
                    sub = _find_longest_list(v)
                    if sub is not None and (best is None or len(sub) > len(best)):
                        best = sub
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, dict | list):
                    sub = _find_longest_list(item)
                    if sub is not None and (best is None or len(sub) > len(best)):
                        best = sub
        return best

    while len(untrusted_input(bounded).encode("utf-8")) > max_bytes:
        longest = _find_longest_str(bounded)
        if longest is not None and longest[0] > 32:
            length, container, key = longest
            container[key] = container[key][: max(16, length // 2)]
            if container is bounded and key == "subtitle_excerpt":
                bounded["subtitle_complete"] = False
                bounded["subtitle_status"] = "truncated"
                bounded["material_scope"] = "partial_subtitle" if bounded[key] else "metadata_only"
            continue
        longest_list = _find_longest_list(bounded)
        if longest_list is not None and len(longest_list) > 0:
            longest_list.pop()
            continue
        raise Rejected("context_too_large")
    return bounded


_MAX_PLUGIN_TASK_INSTRUCTION_CHARS = 2400
_MAX_PLUGIN_RESPONSE_SCHEMA_BYTES = 8192
_MAX_PLUGIN_RESPONSE_SCHEMA_DEPTH = 8


def _json_nesting_depth(node: Any, depth: int = 1) -> int:
    if depth > _MAX_PLUGIN_RESPONSE_SCHEMA_DEPTH:
        return depth
    if isinstance(node, dict):
        nested = [
            _json_nesting_depth(v, depth + 1) for v in node.values() if isinstance(v, dict | list)
        ]
        return max(nested, default=depth)
    if isinstance(node, list):
        nested = [
            _json_nesting_depth(item, depth + 1) for item in node if isinstance(item, dict | list)
        ]
        return max(nested, default=depth)
    return depth


def _normalize_plugin_schema_node(node: Any, *, is_mapping_container: bool = False) -> Any:
    if isinstance(node, dict):
        if not all(isinstance(k, str) for k in node):
            raise Rejected("plugin_invalid_schema")
        if not is_mapping_container:
            if "properties" in node and not isinstance(node["properties"], dict):
                raise Rejected("plugin_invalid_schema")
            if "required" in node and (
                not isinstance(node["required"], list)
                or not all(isinstance(item, str) for item in node["required"])
            ):
                raise Rejected("plugin_invalid_schema")
            if "additionalProperties" in node and node["additionalProperties"] is not False:
                raise Rejected("plugin_invalid_schema")
        out: dict[str, Any] = {}
        for k, v in node.items():
            child_is_map = (not is_mapping_container) and k in {
                "properties",
                "$defs",
                "definitions",
            }
            out[k] = _normalize_plugin_schema_node(v, is_mapping_container=child_is_map)
        if not is_mapping_container and out.get("type") == "object":
            out.setdefault("additionalProperties", False)
        return out
    if isinstance(node, list):
        return [_normalize_plugin_schema_node(item, is_mapping_container=False) for item in node]
    return node


def _validate_plugin_task_instruction(raw: Any, secrets_list: list[str]) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise Rejected("plugin_invalid_schema")
    cleaned = clean_text(raw)
    if not cleaned:
        return None
    if len(cleaned) > _MAX_PLUGIN_TASK_INSTRUCTION_CHARS:
        raise Rejected("plugin_invalid_schema")
    if any(secret in cleaned for secret in _secret_candidates(secrets_list)):
        raise Rejected("plugin_invalid_schema")
    return cleaned


def _validate_plugin_response_schema(raw: Any, secrets_list: list[str]) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or raw.get("type") != "object":
        raise Rejected("plugin_invalid_schema")
    try:
        serialized = json.dumps(raw, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise Rejected("plugin_invalid_schema") from None
    if len(serialized.encode("utf-8")) > _MAX_PLUGIN_RESPONSE_SCHEMA_BYTES:
        raise Rejected("plugin_invalid_schema")
    if any(secret in serialized for secret in _secret_candidates(secrets_list)):
        raise Rejected("plugin_invalid_schema")
    if _json_nesting_depth(raw) > _MAX_PLUGIN_RESPONSE_SCHEMA_DEPTH:
        raise Rejected("plugin_invalid_schema")
    normalized = _normalize_plugin_schema_node(raw)
    result = require_all_schema_fields(normalized)
    if not isinstance(result, dict):
        raise Rejected("plugin_invalid_schema")
    return result


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
    origins: (
        list[Literal["planner", "admin", "query", "rule", "legacy", "timer", "owner_chat"]] | None
    ) = None
    effortful: bool = False
    interrupts_focus: bool = False
    window: Literal["any", "active", "closing"] = "any"
    social: bool = False
    owner_only: bool = False
    aliases: list[list[Any]] = Field(default_factory=list, max_length=4)
    budget_lane: Literal["patrol", "proactive"] | None = None
    quotas: list[dict[str, str]] | None = Field(default=None, max_length=4)
    timeout_seconds: float | None = Field(default=None, ge=1.0, le=1800.0)

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
    output_cap: OutputCap | None = None


_VALID_VOICE_FORMATS = frozenset({"ogg_opus", "mp3", "wav"})
_MAX_VOICE_AUDIO_BYTES = 2 * 1024 * 1024


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
    admin_panel: str | None = None
    attention_level: AttentionLevel = "LOW"
    configuration_issue: str | None = None
    initial_status: dict[str, Any] = Field(default_factory=dict)
    event_subscriptions: list[str] = Field(default_factory=list, max_length=16)
    session_exit_action: str | None = None
    voice_format: str | None = None

    @field_validator("protocol_version")
    @classmethod
    def _check_version(cls, val: int) -> int:
        if isinstance(val, bool) or val != 1:
            raise ValueError("unsupported protocol_version")
        return val

    @field_validator("admin_panel")
    @classmethod
    def _check_admin_panel(cls, val: str | None) -> str | None:
        if val is None:
            return None
        if not _ADMIN_PANEL_RE.fullmatch(val):
            raise ValueError("invalid admin_panel")
        return val

    @field_validator("configuration_issue")
    @classmethod
    def _check_configuration_issue(cls, val: str | None) -> str | None:
        if val is None or val == "":
            return None
        if not _CODE_RE.fullmatch(val):
            raise ValueError("invalid configuration_issue")
        return val

    @field_validator("initial_status")
    @classmethod
    def _check_initial_status(cls, val: dict[str, Any]) -> dict[str, Any]:
        if len(val) > 64:
            raise ValueError("initial_status too large")
        return val

    @field_validator("event_subscriptions")
    @classmethod
    def _check_event_subscriptions(cls, val: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for item in val:
            if not isinstance(item, str) or not _EVENT_TYPE_RE.fullmatch(item):
                raise ValueError("invalid event_subscription")
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        return deduped

    @field_validator("session_exit_action")
    @classmethod
    def _check_session_exit_action(cls, val: str | None) -> str | None:
        if val is None or val == "":
            return None
        if not _ACTION_NAME_RE.fullmatch(val):
            raise ValueError("invalid session_exit_action")
        return val

    @field_validator("voice_format")
    @classmethod
    def _check_voice_format(cls, val: str | None) -> str | None:
        if val is None or val == "":
            return None
        if val not in _VALID_VOICE_FORMATS:
            raise ValueError("invalid voice_format")
        return val


def _build_action_args_model(action_name: str, raw_args: dict[str, Any]) -> type[BaseModel]:
    if not raw_args:
        return NoArgs
    if not isinstance(raw_args, dict):
        raise Rejected("plugin_invalid_schema")

    if "properties" in raw_args and raw_args.get("type") in (None, "object"):
        allowed_top = {
            "type",
            "properties",
            "required",
            "additionalProperties",
            "title",
            "description",
        }
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
        self.thinking_scenes = thinking_scenes
        self.thinking_scene_keys = frozenset(scene.key for scene in thinking_scenes)
        self._actions = actions
        self.capabilities = capabilities
        cap_set = set(capabilities)
        self.supports_sticker = bool({"sticker", "supports_sticker"} & cap_set)
        self.supports_emoji_like = bool({"emoji_like", "supports_emoji_like"} & cap_set)
        self.supports_poke = bool({"poke", "supports_poke"} & cap_set)
        self.supports_reply_anchor = bool({"reply_anchor", "supports_reply_anchor"} & cap_set)
        self.supports_voice = bool({"voice", "supports_voice"} & cap_set)
        self.preferred_voice_format = voice_format or "ogg_opus"
        self.enabled = enabled
        self._configuration_issue: str | None = configuration_issue
        self._remote_status: dict[str, Any] = (
            _sanitize_payload(initial_status, settings) if isinstance(initial_status, dict) else {}
        )
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

    async def _deliver_rpc(
        self,
        method: str,
        payload: dict[str, Any],
        *,
        before_send=None,
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

    async def send_chat(self, message: Message, text: str, *, before_send=None) -> str:
        if "chat" not in self._scopes or message.platform not in self.chat_platforms:
            raise Rejected("delivery_not_authorized")
        if not self.enabled:
            raise Rejected("module_disabled")
        return await self._deliver_rpc(
            "delivery.send",
            {
                "platform": message.platform,
                "bot_id": message.bot_id,
                "kind": message.kind,
                "conversation_id": message.conversation_id,
                "user_id": message.user_id,
                "text": text,
                "reply_to": message.reply_ids[0] if message.reply_ids else None,
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
            or not (0 < len(audio_bytes) <= _MAX_VOICE_AUDIO_BYTES)
        ):
            raise Rejected("invalid_voice_audio")
        audio_b64 = base64.b64encode(bytes(audio_bytes)).decode("ascii")
        payload = {
            "platform": message.platform,
            "bot_id": message.bot_id,
            "kind": message.kind,
            "conversation_id": message.conversation_id,
            "user_id": message.user_id,
            "audio_b64": audio_b64,
            "format": fmt,
            "mime_type": mime_type,
            "duration_ms": duration_ms,
            "reply_to": message.reply_ids[0] if message.reply_ids else None,
        }
        probe = json.dumps(
            {
                "type": "rpc.request",
                "id": "0" * 32,
                "method": "delivery.send_voice",
                "params": {"delivery_id": "0" * 32, **payload},
            },
            ensure_ascii=False,
            allow_nan=False,
        )
        if len(probe.encode("utf-8")) > self._bus._max_frame_bytes():
            raise Rejected("voice_payload_too_large")
        return await self._deliver_rpc(
            "delivery.send_voice",
            payload,
            before_send=before_send,
        )

    async def call_custom_rpc(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout: float = 15.0,  # noqa: ASYNC109
    ) -> dict[str, Any]:
        if (
            not isinstance(method, str)
            or not _CUSTOM_RPC_RE.fullmatch(method)
            or method in {"delivery.send", "delivery.send_voice"}
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
            or method in {"delivery.send", "delivery.send_voice"}
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
            "supports_emoji_like": self.supports_emoji_like,
            "supports_poke": self.supports_poke,
            "supports_reply_anchor": self.supports_reply_anchor,
            "supports_voice": self.supports_voice,
            "preferred_voice_format": self.preferred_voice_format,
        }
        return {**self.status_dict(), **base}


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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._blob_transfer_sem = asyncio.Semaphore(4)

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop | None:
        """Return the active event loop bound to this PluginBus, if running."""
        loop = self._loop
        if loop is not None and not loop.is_closed() and loop.is_running():
            return loop
        return None

    def _connect_lock(self, plugin_id: str) -> asyncio.Lock:
        lock = self._connect_locks.get(plugin_id)
        if lock is None:
            lock = asyncio.Lock()
            self._connect_locks[plugin_id] = lock
        return lock

    def _kernel(self) -> PluginKernel:
        k = getattr(self.service, "kernel", None)
        return k if isinstance(k, PluginKernel) else self._fallback_kernel

    def _all_secrets(self) -> list[str]:
        seen: dict[str, None] = {}
        for raw in configured_secrets(self.s):
            if raw:
                seen[raw] = None
        for raw in getattr(self.service, "secrets", ()) or ():
            if isinstance(raw, str) and raw:
                seen[raw] = None
        return list(seen)

    def _ensure_plugin_not_reserved(self, pid: str) -> None:
        if pid in RESERVED_PLUGIN_IDS:
            raise Rejected("reserved_plugin_id")
        modules = getattr(self.service, "modules", None)
        if modules is not None:
            if pid in getattr(modules, "systems", {}):
                raise Rejected("reserved_plugin_id")
            existing = modules.get(pid) if hasattr(modules, "get") else None
            if existing is not None and not isinstance(existing, RemoteModuleProxy):
                raise Rejected("reserved_plugin_id")

    def _max_frame_bytes(self) -> int:
        return int(getattr(self.s, "plugin_bus_ws_max_frame_bytes", _DEFAULT_MAX_FRAME_BYTES))

    def _ws_enabled(self) -> bool:
        return bool(getattr(self.s, "plugin_bus_ws_enabled", False))

    def _ws_path(self) -> str:
        return str(getattr(self.s, "plugin_bus_ws_path", "/ws/plugin"))

    async def initialize(self) -> None:
        self._loop = asyncio.get_running_loop()
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
        self._ensure_plugin_not_reserved(pid)
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
            self._ensure_plugin_not_reserved(pid)
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

    def is_plugin_online(self, plugin_id: str, *, require_enabled: bool = True) -> bool:
        """Return True when `plugin_id` has an active ready session (and is enabled if required)."""
        if not isinstance(plugin_id, str) or not plugin_id:
            return False
        sess = self._sessions.get(plugin_id)
        if sess is None or not sess.ready or sess.closed:
            return False
        if require_enabled and (sess.proxy is None or not sess.proxy.enabled):
            return False
        return True

    def has_tool(self, plugin_id: str, action_name: str, *, require_enabled: bool = True) -> bool:
        """Return True when `plugin_id` is online and declares `action_name` under `actions`."""
        if not isinstance(action_name, str) or not action_name:
            return False
        if not self.is_plugin_online(plugin_id, require_enabled=require_enabled):
            return False
        sess = self._sessions.get(plugin_id)
        if sess is None or "actions" not in sess.scopes or sess.proxy is None:
            return False
        return any(spec.name == action_name for spec in sess.proxy.actions())

    async def _best_effort_release_blob(
        self,
        session: _PluginSession,
        *,
        transfer_id: str | None = None,
        blob_id: str | None = None,
    ) -> None:
        if session.closed or not session.ready:
            return
        params: dict[str, Any] = {}
        if transfer_id:
            params["transfer_id"] = transfer_id
        if blob_id:
            params["blob_id"] = blob_id
        if not params:
            return
        try:
            await self._rpc(session, BLOB_RPC_RELEASE, params, timeout=1.5)
        except Exception:
            pass

    async def _upload_blob_chunks(
        self,
        session: _PluginSession,
        raw: bytes,
        *,
        digest: str,
        mime_type: str,
        filename: str,
        deadline: float,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        max_frame = self._max_frame_bytes()
        chunk_size = compute_chunk_raw_bytes(max_frame)
        total_chunks = max(1, (len(raw) + chunk_size - 1) // chunk_size)
        transfer_id = uuid.uuid4().hex
        completed = False
        async with self._blob_transfer_sem:
            try:
                for seq in range(total_chunks):
                    rem = deadline - loop.time()
                    if rem <= 0:
                        raise Rejected("plugin_rpc_timeout")
                    start = seq * chunk_size
                    end = min(len(raw), start + chunk_size)
                    put_resp = await self._rpc(
                        session,
                        BLOB_RPC_PUT,
                        {
                            "transfer_id": transfer_id,
                            "seq": seq,
                            "total_chunks": total_chunks,
                            "total_bytes": len(raw),
                            "sha256": digest,
                            "data_b64": encode_blob_chunk(raw[start:end]),
                            "mime_type": mime_type,
                            "filename": filename,
                        },
                        timeout=min(rem, 15.0),
                    )
                    if not put_resp.get("ok", False):
                        raw_err = str(put_resp.get("error") or "")
                        code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_blob_failed"
                        raise Rejected(code)
                completed = True
            finally:
                if not completed:
                    await self._best_effort_release_blob(session, transfer_id=transfer_id)
        return {
            "encoding": "chunked",
            "transfer_id": transfer_id,
            "byte_size": len(raw),
            "sha256": digest,
            "total_chunks": total_chunks,
            "mime_type": mime_type,
            "filename": filename,
        }

    async def _download_blob_chunks(
        self,
        session: _PluginSession,
        desc: dict[str, Any],
        *,
        deadline: float,
    ) -> bytes:
        loop = asyncio.get_running_loop()
        blob_id = desc["blob_id"]
        total_chunks = int(desc["total_chunks"])
        expected_bytes = int(desc["byte_size"])
        expected_digest = str(desc["sha256"])
        chunk_cap = compute_chunk_raw_bytes(self._max_frame_bytes())
        buf = bytearray()
        hasher = hashlib.sha256()
        completed = False
        async with self._blob_transfer_sem:
            try:
                for seq in range(total_chunks):
                    rem = deadline - loop.time()
                    if rem <= 0:
                        raise Rejected("plugin_rpc_timeout")
                    get_resp = await self._rpc(
                        session,
                        BLOB_RPC_GET,
                        {"blob_id": blob_id, "seq": seq},
                        timeout=min(rem, 15.0),
                    )
                    if not get_resp.get("ok", False):
                        raw_err = str(get_resp.get("error") or "")
                        code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_blob_failed"
                        raise Rejected(code)
                    chunk_res = get_resp.get("result")
                    if not isinstance(chunk_res, dict) or chunk_res.get("seq") != seq:
                        raise Rejected("plugin_blob_corrupt")
                    try:
                        chunk_bytes = decode_blob_chunk(
                            chunk_res.get("data_b64"), max_raw_bytes=chunk_cap
                        )
                    except ValueError as exc:
                        raise Rejected(
                            "plugin_blob_too_large"
                            if str(exc) == "blob_too_large"
                            else "plugin_blob_corrupt"
                        ) from None
                    if len(buf) + len(chunk_bytes) > expected_bytes:
                        raise Rejected("plugin_blob_too_large")
                    buf.extend(chunk_bytes)
                    hasher.update(chunk_bytes)
                if len(buf) != expected_bytes or hasher.hexdigest() != expected_digest:
                    raise Rejected("plugin_blob_corrupt")
                completed = True
                return bytes(buf)
            finally:
                if not completed:
                    await self._best_effort_release_blob(session, blob_id=blob_id)

    async def invoke_tool(
        self,
        plugin_id: str,
        action_name: str,
        arguments: dict[str, Any] | None = None,
        *,
        blob: bytes | None = None,
        blob_mime_type: str = "application/octet-stream",
        blob_filename: str = "",
        max_request_blob_bytes: int = 15 * 1024 * 1024,
        max_response_blob_bytes: int = 0,
        timeout: float | None = None,  # noqa: ASYNC109
        require_enabled: bool = True,
    ) -> ToolCallResult:
        """Invoke a declared remote plugin tool with bounded bidirectional chunked blob transfer."""
        if not isinstance(plugin_id, str) or not PLUGIN_ID_RE.fullmatch(plugin_id):
            raise Rejected("invalid_plugin_id")
        if not isinstance(action_name, str) or not _ACTION_NAME_RE.fullmatch(action_name):
            raise Rejected("plugin_invalid_method")
        if arguments is not None and not isinstance(arguments, dict):
            raise Rejected("plugin_invalid_params")
        sess = self._sessions.get(plugin_id)
        if sess is None or not sess.ready or sess.closed:
            raise Rejected("plugin_offline")
        if "actions" not in sess.scopes:
            raise Rejected("plugin_scope_rejected")
        proxy = sess.proxy
        if proxy is None or (require_enabled and not proxy.enabled):
            raise Rejected("plugin_disabled")
        if not any(spec.name == action_name for spec in proxy.actions()):
            raise Rejected("action_not_found")

        req_cap = min(max(0, int(max_request_blob_bytes)), BLOB_HARD_MAX_BYTES)
        resp_cap = min(max(0, int(max_response_blob_bytes)), BLOB_HARD_MAX_BYTES)
        declared_timeout = proxy._action_timeouts.get(action_name)
        effective_timeout = float(
            timeout
            if timeout is not None
            else (declared_timeout if declared_timeout is not None else self.rpc_timeout)
        )
        if not math.isfinite(effective_timeout) or effective_timeout <= 0:
            effective_timeout = self.rpc_timeout

        loop = asyncio.get_running_loop()
        deadline = loop.time() + effective_timeout
        rpc_params: dict[str, Any] = {
            "action": action_name,
            "arguments": dict(arguments or {}),
            "origin": "kernel",
            "target_scope": "",
            "now": self.clock(),
        }

        uploaded_transfer_id: str | None = None
        if blob is not None:
            if not isinstance(blob, (bytes, bytearray)):
                raise Rejected("plugin_invalid_params")
            raw_req = bytes(blob)
            if len(raw_req) > req_cap:
                raise Rejected("plugin_blob_too_large")
            req_digest = sha256_hex(raw_req)
            clean_mime = sanitize_blob_meta_str(
                blob_mime_type, max_len=96, default="application/octet-stream"
            )
            clean_fname = sanitize_blob_meta_str(
                blob_filename, max_len=160, default="", is_filename=True
            )
            max_frame = self._max_frame_bytes()
            inline_cap = compute_inline_max_bytes(max_frame)
            inlined = False
            if len(raw_req) <= inline_cap:
                candidate_desc = {
                    "encoding": "inline_b64",
                    "data_b64": encode_blob_chunk(raw_req),
                    "byte_size": len(raw_req),
                    "sha256": req_digest,
                    "mime_type": clean_mime,
                    "filename": clean_fname,
                }
                test_params = {**rpc_params, BLOB_FIELD: candidate_desc}
                try:
                    encoded = json.dumps(
                        {
                            "type": "rpc.request",
                            "id": "0" * 32,
                            "method": "action.run",
                            "params": test_params,
                        },
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                except (TypeError, ValueError):
                    raise Rejected("plugin_invalid_params") from None
                if len(encoded.encode("utf-8")) <= max_frame:
                    rpc_params[BLOB_FIELD] = candidate_desc
                    inlined = True
            if not inlined:
                chunk_desc = await self._upload_blob_chunks(
                    sess,
                    raw_req,
                    digest=req_digest,
                    mime_type=clean_mime,
                    filename=clean_fname,
                    deadline=deadline,
                )
                uploaded_transfer_id = str(chunk_desc["transfer_id"])
                rpc_params[BLOB_FIELD] = chunk_desc

        try:
            rem = deadline - loop.time()
            if rem <= 0:
                raise Rejected("plugin_rpc_timeout")
            resp = await self._rpc(sess, "action.run", rpc_params, timeout=rem)
        except Exception:
            if uploaded_transfer_id is not None:
                await self._best_effort_release_blob(sess, transfer_id=uploaded_transfer_id)
            raise

        if not resp.get("ok", False):
            raw_err = str(resp.get("error") or "")
            code = raw_err if _CODE_RE.fullmatch(raw_err) else "plugin_action_failed"
            raise Rejected(code)

        raw_res = resp.get("result") if "result" in resp else resp.get("data")
        if raw_res is None:
            res_dict: dict[str, Any] = {}
        elif isinstance(raw_res, dict):
            res_dict = dict(raw_res)
        else:
            res_dict = {"value": raw_res}

        resp_blob: bytes | None = None
        resp_mime = ""
        resp_filename = ""
        if BLOB_FIELD in res_dict:
            raw_blob_desc = res_dict.pop(BLOB_FIELD)
            raw_blob_id = (
                raw_blob_desc.get("blob_id")
                if isinstance(raw_blob_desc, dict) and isinstance(raw_blob_desc.get("blob_id"), str)
                else None
            )
            if resp_cap <= 0:
                if raw_blob_id:
                    await self._best_effort_release_blob(sess, blob_id=raw_blob_id)
                raise Rejected("plugin_blob_too_large")
            try:
                desc = validate_blob_descriptor(
                    raw_blob_desc, max_bytes=resp_cap, id_field="blob_id"
                )
            except ValueError as exc:
                if raw_blob_id:
                    await self._best_effort_release_blob(sess, blob_id=raw_blob_id)
                raise Rejected(
                    "plugin_blob_too_large"
                    if str(exc) == "blob_too_large"
                    else "plugin_blob_invalid"
                ) from None

            resp_mime = desc["mime_type"]
            resp_filename = desc["filename"]
            if desc["encoding"] == "inline_b64":
                try:
                    resp_blob = decode_blob_chunk(desc["data_b64"], max_raw_bytes=resp_cap)
                except ValueError as exc:
                    raise Rejected(
                        "plugin_blob_too_large"
                        if str(exc) == "blob_too_large"
                        else "plugin_blob_corrupt"
                    ) from None
                if len(resp_blob) != desc["byte_size"] or sha256_hex(resp_blob) != desc["sha256"]:
                    raise Rejected("plugin_blob_corrupt")
            else:
                resp_blob = await self._download_blob_chunks(sess, desc, deadline=deadline)

        clean_data = _scrub_json_secrets(res_dict, self._all_secrets())
        if not isinstance(clean_data, dict):
            clean_data = {"value": clean_data}
        return ToolCallResult(
            data=clean_data,
            blob=resp_blob,
            blob_mime_type=resp_mime,
            blob_filename=resp_filename,
        )

    def _build_actions(
        self,
        session: _PluginSession,
        decls: list[_PluginActionDecl],
    ) -> tuple[tuple[Action, ...], dict[str, float]]:
        registry = getattr(self.service, "modules", None)
        existing_actions = set(registry.catalog.actions) if registry is not None else set()
        existing_aliases = set(registry.catalog.aliases) if registry is not None else set()
        seen_names: set[str] = set()
        seen_aliases: set[tuple[str, str, bool]] = set()
        actions: list[Action] = []
        action_timeouts: dict[str, float] = {}
        for decl in decls:
            prefix = decl.name.split(".", 1)[0]
            required_owner = _SCOPED_ACTION_PREFIX_OWNERS.get(prefix)
            if prefix in _RESERVED_ACTION_PREFIXES or (
                required_owner is not None and session.plugin_id != required_owner
            ):
                raise Rejected("plugin_scope_rejected")
            if decl.budget_lane == PATROL_LANE and session.plugin_id != "bilibili_surf":
                raise Rejected("plugin_scope_rejected")
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
            if not origins:
                raise Rejected("plugin_invalid_schema")
            if "query" in origins and (
                decl.social
                or decl.owner_only
                or decl.budget_lane is not None
                or bool(decl.quotas)
                or args_model is not NoArgs
            ):
                raise Rejected("plugin_invalid_schema")
            parsed_quotas: tuple[ActivityQuota, ...] | None
            if decl.quotas is None:
                parsed_quotas = () if "query" in origins else None
            else:
                if len(decl.quotas) > 4:
                    raise Rejected("plugin_invalid_schema")
                q_list: list[ActivityQuota] = []
                for q_item in decl.quotas:
                    if not isinstance(q_item, dict):
                        raise Rejected("plugin_invalid_schema")
                    if set(q_item) - {"name", "unlimited_flag"} or "name" not in q_item:
                        raise Rejected("plugin_invalid_schema")
                    q_name = q_item["name"]
                    if (
                        not isinstance(q_name, str)
                        or not _FIELD_NAME_RE.fullmatch(q_name)
                        or q_name in {"total", "counted"}
                    ):
                        raise Rejected("plugin_invalid_schema")
                    q_flag = q_item.get("unlimited_flag", "")
                    if not isinstance(q_flag, str) or (
                        q_flag and not _FIELD_NAME_RE.fullmatch(q_flag)
                    ):
                        raise Rejected("plugin_invalid_schema")
                    q_list.append(ActivityQuota(counter=q_name, unlimited_option=q_flag))
                parsed_quotas = tuple(q_list)
            parsed_aliases: list[tuple[str, str, bool]] = []
            for raw_alias in decl.aliases:
                if not isinstance(raw_alias, list) or len(raw_alias) != 3:
                    raise Rejected("plugin_invalid_schema")
                target_mod, action_type, has_recipient = raw_alias
                if (
                    not isinstance(target_mod, str)
                    or not isinstance(action_type, str)
                    or not isinstance(has_recipient, bool)
                ):
                    raise Rejected("plugin_invalid_schema")
                if target_mod != session.plugin_id:
                    raise Rejected("plugin_scope_rejected")
                if not _ALIAS_VERB_RE.fullmatch(action_type) or has_recipient != decl.social:
                    raise Rejected("plugin_invalid_schema")
                alias = (target_mod, action_type, has_recipient)
                if alias in seen_aliases or alias in existing_aliases:
                    raise Rejected("plugin_duplicate_action")
                seen_aliases.add(alias)
                parsed_aliases.append(alias)
            desc = sanitize_sensor_text(clean_text(decl.description), self.s, max_tokens=80)
            if not desc:
                raise Rejected("plugin_invalid_schema")
            if decl.timeout_seconds is not None:
                action_timeouts[decl.name] = float(decl.timeout_seconds)

            async def _run(
                call: ActionCall,
                action_name: str = decl.name,
                action_timeout: float | None = decl.timeout_seconds,
            ) -> Outcome:
                proxy = session.proxy
                if proxy is None or not proxy.enabled:
                    return Outcome("gated_skip", "module_disabled")
                if not call.still_allowed():
                    return Outcome("gated_skip", "action_no_longer_eligible")
                args_dict = call.args.model_dump(mode="json")
                rpc_params: dict[str, Any] = {
                    "action": action_name,
                    "arguments": args_dict,
                    "origin": str(call.ticket.get("origin") or "planner"),
                    "target_scope": str(call.ticket.get("target_scope") or ""),
                    "now": call.now,
                }
                if exit_reason := str(call.ticket.get("exit_reason") or "").strip():
                    clean_exit_reason = sanitize_sensor_text(exit_reason, self.s, max_tokens=12)[
                        :32
                    ]
                    if clean_exit_reason:
                        rpc_params["exit_reason"] = clean_exit_reason
                try:
                    resp = await self._rpc(
                        session,
                        "action.run",
                        rpc_params,
                        timeout=action_timeout,
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
                if "result" in resp and resp["result"] is not None:
                    raw_res = resp["result"]
                elif "data" in resp and resp["data"] is not None:
                    raw_res = resp["data"]
                elif any(k in resp for k in ("status", "reason", "details")):
                    raw_res = {k: v for k, v in resp.items() if k not in {"type", "id", "ok"}}
                else:
                    raw_res = resp.get("result")
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
                    budget_lane=decl.budget_lane,
                    effortful=decl.effortful,
                    interrupts_focus=decl.interrupts_focus,
                    window=decl.window,
                    quotas=parsed_quotas,
                    origins=origins,
                    aliases=tuple(parsed_aliases),
                    unavailable=lambda s=session, n=decl.name: (
                        "plugin_offline"
                        if not (s.ready and not s.closed)
                        else (s.proxy._action_unavailable.get(n) if s.proxy is not None else None)
                    ),
                )
            )
        return tuple(actions), action_timeouts

    def _build_thinking_scenes(
        self,
        plugin_id: str,
        decls: list[_PluginThinkingSceneDecl],
    ) -> tuple[ThinkingScene, ...]:
        registry = getattr(self.service, "modules", None)
        existing_keys = set(registry.thinking_scenes) if registry is not None else set()
        seen: set[str] = set()
        scenes: list[ThinkingScene] = []
        for decl in decls:
            if decl.key in _SCOPED_SCENE_OWNERS:
                if (
                    plugin_id != _SCOPED_SCENE_OWNERS[decl.key]
                    or decl.key in seen
                    or decl.key in existing_keys
                ):
                    raise Rejected("plugin_scope_rejected")
            else:
                prefix = decl.key.split(".", 1)[0]
                required_owner = _SCOPED_ACTION_PREFIX_OWNERS.get(prefix)
                if (
                    decl.key in _RESERVED_SCENE_KEYS
                    or decl.key in seen
                    or decl.key in existing_keys
                    or prefix in _RESERVED_ACTION_PREFIXES
                    or (required_owner is not None and plugin_id != required_owner)
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
                    output_cap=decl.output_cap,
                )
            except ValueError:
                raise Rejected("plugin_invalid_schema") from None
            scenes.append(scene)
        return tuple(scenes)

    def _validate_hello(
        self,
        raw_frame: dict[str, Any],
        cred: dict[str, Any],
        *,
        plugin_id: str,
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

        if hello.admin_panel is not None:
            if _HOTPLUG_ADMIN_PANELS.get(plugin_id) == hello.admin_panel:
                pass
            elif hello.admin_panel in _CORE_ADMIN_PANELS:
                raise Rejected("plugin_scope_rejected")

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
        raw_report = frame.get("report_text")
        if raw_report is not None and not isinstance(raw_report, str):
            raise Rejected("plugin_invalid_schema")
        raw_longnick = frame.get("mood_longnick")
        if raw_longnick is not None and not isinstance(raw_longnick, str):
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
            safe_summary = _scrub_text_secrets(raw_summary, _secret_candidates(self._all_secrets()))
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
                raw_ttl = frame.get("ttl_seconds")
                ttl_seconds: float | None = None
                if (
                    raw_ttl is not None
                    and not isinstance(raw_ttl, bool)
                    and isinstance(raw_ttl, int | float)
                    and math.isfinite(float(raw_ttl))
                ):
                    ttl_seconds = max(300.0, min(float(raw_ttl), 7 * 86400.0))
                post_kwargs: dict[str, Any] = {
                    "payload": clean_payload,
                    "dedupe_key": clean_key,
                    "due_at": float(due_at) if due_at is not None else None,
                    "visibility": visibility,
                }
                if ttl_seconds is not None:
                    post_kwargs["ttl_seconds"] = ttl_seconds
                try:
                    await ledger.post(
                        session.plugin_id,
                        clean_type,
                        safe_summary,
                        **post_kwargs,
                    )
                except ValueError:
                    pass

            if frame.get("owner_recap") is True and self.s.owner_qq_id:
                now = self.clock()
                raw_topic = frame.get("chronicle_topic")
                topic_arg = raw_topic if isinstance(raw_topic, str) and raw_topic.strip() else None
                raw_tags = frame.get("chronicle_tags")
                clean_tags: list[str] = []
                if isinstance(raw_tags, list | tuple):
                    for raw_tag in raw_tags:
                        if not isinstance(raw_tag, str):
                            continue
                        clean_tag = sanitize_sensor_text(
                            clean_text(raw_tag), self.s, max_tokens=10
                        )[:24].strip()
                        if clean_tag and clean_tag not in clean_tags:
                            clean_tags.append(clean_tag)
                            if len(clean_tags) >= 6:
                                break
                if session.plugin_id not in clean_tags:
                    clean_tags = clean_tags[:5] + [session.plugin_id]
                memory_hub = getattr(self.service, "memory_hub", None)
                if memory_hub is not None and hasattr(memory_hub, "record_experience"):
                    await memory_hub.record_experience(
                        session.plugin_id,
                        raw_summary,
                        visibility="owner_private",
                        event_type=clean_type,
                        dedupe_key=clean_key,
                        write_recap=True,
                        recap_platform=session.plugin_id,
                        write_ledger=False,
                        chronicle_topic=topic_arg,
                        chronicle_tags=clean_tags,
                        chronicle_source_id=f"plugin:{session.plugin_id}:{clean_key or clean_type}",
                        now=now,
                    )
                else:
                    try:
                        scrubbed = safe_output(
                            clean_text(raw_summary), self._all_secrets(), limit=260
                        )
                    except Rejected:
                        scrubbed = ""
                    clean_recap = (
                        sanitize_sensor_text(scrubbed, self.s, max_tokens=90) if scrubbed else ""
                    )
                    if clean_recap:
                        identity = getattr(self.service, "identity", None)
                        if identity is not None and hasattr(identity, "record_owner_recap"):
                            identity.record_owner_recap(
                                session.plugin_id,
                                clean_recap,
                                now=now,
                                private_owner=True,
                            )
                        if topic_arg is not None:
                            clean_topic = sanitize_sensor_text(
                                clean_text(topic_arg), self.s, max_tokens=24
                            )[:80].strip()
                            owner_msg = owner_target(
                                self.s,
                                getattr(self.s, "owner_delivery_platform", "onebot"),
                                now,
                                f"plugin-recap:{session.plugin_id}",
                            )
                            sessions = getattr(self.service, "sessions", None)
                            dual_mem = (
                                getattr(sessions, "dual", None) if sessions is not None else None
                            )
                            if (
                                owner_msg is not None
                                and dual_mem is not None
                                and hasattr(dual_mem, "record_chronicle")
                                and clean_topic
                            ):
                                try:
                                    source_id = (
                                        f"plugin:{session.plugin_id}:{clean_key or clean_type}"
                                    )
                                    await dual_mem.record_chronicle(
                                        owner_msg,
                                        topic=clean_topic,
                                        text=clean_recap,
                                        category="event",
                                        tags=clean_tags,
                                        source_id=source_id,
                                        created_at=now,
                                    )
                                except Exception:
                                    pass

        if session.plugin_id == "bilibili_surf" and clean_type == "bili_video_watched":
            if isinstance(raw_report, str) and raw_report.strip():
                if not raw_payload.get("private_first") or raw_payload.get("owner_private"):
                    report_target = owner_target(
                        self.s,
                        getattr(self.s, "owner_delivery_platform", "onebot"),
                        self.clock(),
                        "patrol-report",
                    )
                    if report_target is not None and hasattr(self.store, "save_report"):
                        try:
                            redacted_report = redact_public_summary(
                                safe_output(raw_report, self._all_secrets(), limit=1100)
                            )
                        except Rejected:
                            redacted_report = ""
                        if redacted_report:
                            await self.store.save_report(
                                self.store.subject(report_target),
                                redacted_report,
                                public=not bool(raw_payload.get("private_first")),
                            )
            if (
                isinstance(raw_longnick, str)
                and raw_longnick.strip()
                and not raw_payload.get("private_first")
                and (longnick := getattr(self.service, "longnick", None)) is not None
                and hasattr(longnick, "apply_patrol")
            ):
                await longnick.apply_patrol(raw_longnick[:80])

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

    def _validate_ai_lane(
        self, session: _PluginSession, raw_lane: Any, *, default: str = PROACTIVE_LANE
    ) -> str:
        lane = default if raw_lane is None else raw_lane
        if not isinstance(lane, str):
            raise Rejected("plugin_invalid_schema")
        if lane == PROACTIVE_LANE or (lane == PATROL_LANE and session.plugin_id == "bilibili_surf"):
            return lane
        raise Rejected("plugin_scope_rejected")

    async def _check_ai_budget(self, lane: str) -> tuple[bool, bool]:
        if not hasattr(self.store, "budget_status"):
            return True, False
        budget = await self.store.budget_status()
        hard_stop = bool(budget.get("hard_stop"))
        available = (not hard_stop) and bool(paid_lane_available(budget, lane))
        return available, hard_stop

    async def _spawn_ai_call(
        self,
        session: _PluginSession,
        req_id: str,
        *,
        result_type: str,
        task_name: str,
        worker: Callable[[], Awaitable[None]],
    ) -> None:
        if session.proxy is None or not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": result_type, "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return
        if session.inflight_ai_calls >= _MAX_AI_CALLS_INFLIGHT:
            await self._send_frame(
                session,
                {"type": result_type, "id": req_id, "ok": False, "error": "ai_backpressure"},
            )
            return
        if session.closed or session.fiber.state != "active":
            return
        session.inflight_ai_calls += 1

        async def _runner() -> None:
            try:
                await worker()
            finally:
                session.inflight_ai_calls = max(0, session.inflight_ai_calls - 1)

        try:
            session.ctx.spawn(_runner(), name=task_name)
        except RuntimeError:
            session.inflight_ai_calls = max(0, session.inflight_ai_calls - 1)

    async def _handle_ai_budget_check(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)

        async def _run_budget_check() -> None:
            try:
                available, hard_stop = await self._check_ai_budget(lane)
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"available": available, "hard_stop": hard_stop},
                    },
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "budget_check_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-budget:{session.plugin_id}:{req_id[:16]}",
            worker=_run_budget_check,
        )

    async def _handle_ai_screen(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)
        raw_purpose = frame.get("purpose", "source")
        if raw_purpose is not None and raw_purpose != "source":
            raise Rejected("plugin_scope_rejected")
        raw_images = frame.get("images")
        if raw_images is not None and not isinstance(raw_images, list):
            raise Rejected("plugin_invalid_schema")
        if raw_images and "media" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_payload = frame.get("payload")
        if not isinstance(raw_payload, dict):
            raise Rejected("plugin_invalid_schema")
        if not raw_payload:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "empty_payload"},
            )
            return

        async def _run_screen() -> None:
            try:
                images: list[ModelImage] = (
                    decode_b64_frames(raw_images, 2, self.s, error_prefix="plugin")
                    if raw_images
                    else []
                )
                available, _ = await self._check_ai_budget(lane)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return
                ai = getattr(self.service, "ai", None)
                if ai is None or not hasattr(ai, "screen"):
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "ai_unavailable",
                        },
                    )
                    return
                max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                bounded_payload = _bound_ai_payload(raw_payload, self._all_secrets(), max_bytes)
                with model_work(f"plugin.{session.plugin_id}.screen", req_id, independent=True):
                    await ai.screen(
                        bounded_payload,
                        f"plugin:{session.plugin_id}",
                        purpose="source",
                        budget_lane=lane,
                        images=images or None,
                    )
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"passed": True},
                    },
                )
            except asyncio.CancelledError:
                raise
            except Rejected as exc:
                raw_err = str(exc).strip()
                code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_screen_rejected"
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": code},
                )
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "ai_screen_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-screen:{session.plugin_id}:{req_id[:16]}",
            worker=_run_screen,
        )

    async def _handle_ai_generate(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
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
        if raw_images is not None and not isinstance(raw_images, list):
            raise Rejected("plugin_invalid_schema")
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

        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)

        raw_payload = frame.get("payload")
        raw_prompt = frame.get("prompt")
        if raw_payload is not None and not isinstance(raw_payload, dict):
            raise Rejected("plugin_invalid_schema")
        if raw_prompt is not None and not isinstance(raw_prompt, str):
            raise Rejected("plugin_invalid_schema")

        payload_in: dict[str, Any] = dict(raw_payload or {})
        if isinstance(raw_prompt, str) and raw_prompt.strip():
            payload_in["prompt"] = raw_prompt.strip()
        if not payload_in:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "empty_payload"},
            )
            return

        try:
            resolved_scenario: Scenario | str = Scenario(scenario)
        except ValueError:
            resolved_scenario = scenario

        raw_task_instruction = frame.get("task_instruction")
        raw_response_schema = frame.get("response_schema")
        if isinstance(raw_task_instruction, str) and not clean_text(raw_task_instruction):
            raw_task_instruction = None
        if isinstance(resolved_scenario, Scenario) and (
            raw_task_instruction is not None or raw_response_schema is not None
        ):
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

        precheck_secrets = self._all_secrets()
        try:
            plugin_instruction = _validate_plugin_task_instruction(
                raw_task_instruction, precheck_secrets
            )
            validated_schema = _validate_plugin_response_schema(
                raw_response_schema, precheck_secrets
            )
        except Rejected as exc:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": str(exc)},
            )
            return

        is_bili_scene = scenario in _SCOPED_SCENE_OWNERS or scenario.startswith(
            ("bili_", "bilibili_")
        )
        out_limit = 4000 if is_bili_scene else 1800

        async def _run_generate() -> None:
            try:
                images: list[ModelImage] = (
                    decode_b64_frames(raw_images, 2, self.s, error_prefix="plugin")
                    if raw_images
                    else []
                )
                available, _ = await self._check_ai_budget(lane)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "ai.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return

                ai = getattr(self.service, "ai", None)
                if ai is None or not hasattr(ai, "generate"):
                    await self._send_frame(
                        session,
                        {"type": "ai.result", "id": req_id, "ok": False, "error": "ai_unavailable"},
                    )
                    return

                secrets_list = self._all_secrets()
                max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                bounded_payload = _bound_ai_payload(payload_in, secrets_list, max_bytes)
                bounded_payload["plugin_id"] = session.plugin_id

                extra_kwargs: dict[str, Any] = {}
                if plugin_instruction is not None:
                    extra_kwargs["plugin_instruction"] = plugin_instruction
                if validated_schema is not None:
                    extra_kwargs["response_schema"] = validated_schema

                with model_work(scenario, req_id, independent=True):
                    raw_text = await ai.generate(
                        bounded_payload,
                        f"plugin:{session.plugin_id}",
                        scenario=resolved_scenario,
                        budget_lane=lane,
                        images=images or None,
                        audit_source="production",
                        **extra_kwargs,
                    )
                clean_reply = safe_output(str(raw_text or ""), secrets_list, limit=out_limit)
                await self._send_frame(
                    session,
                    {"type": "ai.result", "id": req_id, "ok": True, "text": clean_reply},
                )
            except asyncio.CancelledError:
                raise
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

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="ai.result",
            task_name=f"plugin-ai-generate:{session.plugin_id}:{req_id[:16]}",
            worker=_run_generate,
        )

    async def _handle_ai_video_analyze(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if (
            "thinking_scenes" not in session.scopes
            or session.proxy is None
            or session.plugin_id != "bilibili_surf"
        ):
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        bvid = frame.get("bvid")
        material = frame.get("material")
        if not isinstance(bvid, str) or not isinstance(material, dict):
            raise Rejected("plugin_invalid_schema")
        if not _BVID_RE.fullmatch(bvid):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_bvid"},
            )
            return
        self._validate_ai_lane(session, frame.get("budget_lane"), default=PATROL_LANE)

        async def _run_video_analyze() -> None:
            try:
                available, _ = await self._check_ai_budget(PATROL_LANE)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return

                secrets_list = self._all_secrets()
                subject = f"plugin:{session.plugin_id}"
                clean_material = _scrub_json_secrets(material, secrets_list)
                if not isinstance(clean_material, dict):
                    raise Rejected("plugin_invalid_schema")

                video_watch = getattr(self.service, "video_watch", None)
                if video_watch is not None and hasattr(video_watch, "analyze"):
                    with model_work(f"plugin.{session.plugin_id}.video", req_id, independent=True):
                        res = await video_watch.analyze(bvid, clean_material, subject, PATROL_LANE)
                    out_text = redact_public_summary(
                        safe_output(str(res.text or ""), secrets_list, limit=1000)
                    )
                    result_obj = {
                        "text": out_text,
                        "mode": str(res.mode),
                        "material_scope": str(res.material_scope),
                        "reason": res.reason,
                    }
                else:
                    ai = getattr(self.service, "ai", None)
                    if ai is None or not hasattr(ai, "screen") or not hasattr(ai, "generate"):
                        await self._send_frame(
                            session,
                            {
                                "type": "kernel.result",
                                "id": req_id,
                                "ok": False,
                                "error": "ai_unavailable",
                            },
                        )
                        return
                    max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                    bounded_mat = _bound_ai_payload(clean_material, secrets_list, max_bytes)
                    with model_work(f"plugin.{session.plugin_id}.video", req_id, independent=True):
                        await ai.screen(
                            bounded_mat,
                            subject,
                            purpose="source",
                            budget_lane=PATROL_LANE,
                        )
                        raw_text = await ai.generate(
                            bounded_mat,
                            subject,
                            scenario=Scenario.BILI_LEARNING,
                            budget_lane=PATROL_LANE,
                            audit_source="production",
                        )
                    out_text = redact_public_summary(
                        safe_output(str(raw_text or ""), secrets_list, limit=1000)
                    )
                    result_obj = {
                        "text": out_text,
                        "mode": "text",
                        "material_scope": str(bounded_mat.get("material_scope") or "metadata_only"),
                        "reason": None,
                    }

                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": result_obj,
                    },
                )
            except asyncio.CancelledError:
                raise
            except Rejected as exc:
                raw_err = str(exc).strip()
                code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_video_analyze_rejected"
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": code},
                )
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "ai_video_analyze_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-video:{session.plugin_id}:{req_id[:16]}",
            worker=_run_video_analyze,
        )

    async def _ensure_bili_private_tables(self) -> None:
        async with self.store.transaction():
            await self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS bili_served_private_topics ("
                "receipt TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            await self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS bili_private_share_priority ("
                "bvid TEXT PRIMARY KEY, session_scope TEXT NOT NULL, expires_at REAL NOT NULL)"
            )

    async def _next_bili_private_topic(
        self,
        sessions: Any,
        session_list: list[tuple[str, Message]],
        secrets_list: list[str],
    ) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        for _, msg in session_list:
            if msg.kind != "private" or not permits_chat_export(msg, self.s, "bilibili_surf"):
                continue
            try:
                epoch, turns = await sessions.private_request_window(msg)
            except Exception:
                continue
            candidates: list[dict[str, Any]] = []
            scope = self.store.session_scope(msg)
            for item in turns:
                raw = str(item.get("text", ""))
                if item.get("role") != "user":
                    continue
                video = re.search(r"视频|[Bb]站|哔哩哔哩|bilibili", raw, re.I)
                interest = re.search(r"想了解|感兴趣", raw)
                if not video and not interest:
                    continue
                requested = video and re.search(r"找|想看|推荐|帮我|给我|有没有|求", raw)
                specificity = 2 if requested else int(bool(interest))
                try:
                    text = redact_public_summary(safe_output(raw, secrets_list, limit=80))
                except Rejected:
                    continue
                if not text:
                    continue
                seq = item["seq"]
                event = item.get("event_id")
                receipt = self.store.vault.digest(
                    "bili-private-request-v1",
                    scope,
                    epoch,
                    ("event", event) if event else ("seq", seq),
                )
                stamp = item.get("created_at")
                if (
                    isinstance(stamp, bool)
                    or not isinstance(stamp, int | float)
                    or not math.isfinite(stamp)
                ):
                    stamp = 0
                rank = (specificity, stamp, seq)
                candidates.append(
                    {
                        "source": msg,
                        "epoch": epoch,
                        "receipt": receipt,
                        "text": text,
                        "priority": rank,
                        "scope": scope,
                    }
                )
            if not candidates:
                continue
            keys = tuple(c["receipt"] for c in candidates)
            async with self.store.lock:
                if await self._table_exists("bili_served_private_topics"):
                    rows = await self.store.all(
                        "SELECT receipt FROM bili_served_private_topics WHERE receipt IN ("
                        + ",".join("?" for _ in keys)
                        + ")",
                        keys,
                    )
                    served = {r[0] for r in rows}
                else:
                    served = set()
            for topic in candidates:
                if topic["receipt"] not in served and (
                    best is None or topic["priority"] > best["priority"]
                ):
                    best = topic
        return best

    async def _handle_bili_memory_preferences(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if session.plugin_id != "bilibili_surf" or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return

        await self._ensure_bili_private_tables()
        sessions = getattr(self.service, "sessions", None)
        runtime = getattr(self.store, "runtime", None)
        if sessions is None or runtime is None or not hasattr(runtime, "sessions"):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"preferences": {}, "private_topic": None},
                },
            )
            return

        secrets_list = self._all_secrets()
        try:
            session_list = await runtime.sessions()
        except Exception:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"preferences": {}, "private_topic": None},
                },
            )
            return

        private = await self._next_bili_private_topic(sessions, session_list, secrets_list)
        if private is not None:
            preferred: Message = private["source"]
            owner_private = bool(is_persona_owner(preferred, self.s))
            audience = private_event_audience(
                self.s,
                preferred.user_id,
                platform=preferred.platform,
                bot_id=preferred.bot_id,
            )
            handle_id = uuid.uuid4().hex
            issued_at = self.clock()
            stale_handles = [
                key
                for key, item in session._bili_private_handles.items()
                if issued_at >= float(item.get("expires_at", 0.0))
                or int(item.get("commits", 0)) >= _BILI_PRIVATE_HANDLE_MAX_COMMITS
            ]
            for key in stale_handles:
                session._bili_private_handles.pop(key, None)
            if len(session._bili_private_handles) >= _MAX_BILI_PRIVATE_HANDLES:
                oldest_key = next(iter(session._bili_private_handles))
                session._bili_private_handles.pop(oldest_key, None)
            session._bili_private_handles[handle_id] = {
                "source": preferred,
                "epoch": private["epoch"],
                "receipt": private["receipt"],
                "scope": private["scope"],
                "audience": audience,
                "owner_private": owner_private,
                "issued_at": issued_at,
                "expires_at": issued_at + _BILI_PRIVATE_HANDLE_TTL_SECONDS,
                "commits": 0,
            }
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {
                        "preferences": {
                            "recent_chat_topics": [private["text"]],
                            "private_topic_refs": ["recent_chat_topics:0"],
                        },
                        "private_topic": {
                            "handle": handle_id,
                            "owner_private": owner_private,
                            "audience": audience,
                            "session_scope": private["scope"],
                        },
                    },
                },
            )
            return

        topics: dict[str, list[str]] = {}
        now = self.clock()

        def add_topic(field_name: str, raw_val: str, *, count: int, length: int) -> None:
            values = topics.get(field_name, [])
            if len(values) >= count:
                return
            try:
                text = redact_public_summary(safe_output(raw_val, secrets_list, limit=length))
            except Rejected:
                return
            if text and text not in values:
                topics.setdefault(field_name, []).append(text)

        for _, msg in session_list[:20]:
            if msg.kind != "group" or not permits_chat_export(msg, self.s, "bilibili_surf"):
                continue
            try:
                view = await sessions.dual.view(msg)
                snap = await sessions.snapshot(msg)
            except Exception:
                continue
            for entry in view.get("entries", []):
                if (
                    not entry.get("deleted")
                    and (not entry.get("expires") or entry["expires"] > now)
                    and (entry.get("slot") == "preferences" or entry.get("kind") == "chronicle")
                ):
                    add_topic("community_topics", str(entry.get("text", "")), count=10, length=120)
            for item in reversed(snap.get("recent_messages", [])[-6:]):
                if item.get("role") == "user":
                    add_topic("recent_chat_topics", str(item.get("text", "")), count=5, length=80)

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"preferences": topics, "private_topic": None},
            },
        )

    async def _handle_bili_private_topic_commit(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if session.plugin_id != "bilibili_surf" or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return

        handle = frame.get("handle")
        bvid = frame.get("bvid")
        if not isinstance(handle, str) or not handle.strip() or len(handle) > 128:
            raise Rejected("plugin_invalid_schema")
        if not isinstance(bvid, str) or not _BVID_RE.fullmatch(bvid):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_bvid"},
            )
            return

        now = self.clock()
        entry = session._bili_private_handles.get(handle)
        if entry is None:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return
        if (
            now >= float(entry.get("expires_at", 0.0))
            or int(entry.get("commits", 0)) >= _BILI_PRIVATE_HANDLE_MAX_COMMITS
        ):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return

        sessions = getattr(self.service, "sessions", None)
        preferred: Message = entry["source"]
        try:
            current_epoch = (
                await sessions.current_epoch(preferred)
                if sessions is not None and hasattr(sessions, "current_epoch")
                else None
            )
        except Exception:
            current_epoch = None
        if (
            not permits_chat_export(preferred, self.s, "bilibili_surf")
            or current_epoch != entry["epoch"]
        ):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return

        now = self.clock()
        if now >= float(entry.get("expires_at", 0.0)):
            session._bili_private_handles.pop(handle, None)
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_request_revoked",
                },
            )
            return
        expires_at = now + _BILI_PRIVATE_SHARE_PRIORITY_SECONDS
        try:
            async with self.store.transaction():
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS bili_served_private_topics ("
                    "receipt TEXT PRIMARY KEY) WITHOUT ROWID"
                )
                await self.store.db.execute(
                    "CREATE TABLE IF NOT EXISTS bili_private_share_priority ("
                    "bvid TEXT PRIMARY KEY, session_scope TEXT NOT NULL, expires_at REAL NOT NULL)"
                )
                await self.store.db.execute(
                    "INSERT OR IGNORE INTO bili_served_private_topics(receipt) VALUES (?)",
                    (entry["receipt"],),
                )
                await self.store.db.execute(
                    "DELETE FROM bili_private_share_priority WHERE expires_at<=?",
                    (now,),
                )
                await self.store.db.execute(
                    "INSERT OR REPLACE INTO bili_private_share_priority "
                    "(bvid, session_scope, expires_at) VALUES (?,?,?)",
                    (bvid, entry["scope"], expires_at),
                )
        except Exception:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "bili_private_commit_failed",
                },
            )
            return

        commits = int(entry.get("commits", 0)) + 1
        entry["commits"] = commits
        if commits >= _BILI_PRIVATE_HANDLE_MAX_COMMITS:
            session._bili_private_handles.pop(handle, None)

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {
                    "committed": True,
                    "owner_private": bool(entry["owner_private"]),
                    "audience": str(entry["audience"]),
                    "session_scope": str(entry["scope"]),
                },
            },
        )

    async def _handle_state_get(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return

        aad = f"{_VAULT_KV_PREFIX}{session.plugin_id}:{state_key}"
        async with self.store.lock:
            row = await self.store.one(
                "SELECT payload FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
                (session.plugin_id, state_key),
            )
        if row is not None:
            try:
                value = self.store.vault.open(aad, row[0])
            except Exception:
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": "state_corrupt"},
                )
                return
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": {"found": True, "value": value},
                },
            )
            return

        if session.plugin_id == "weather" and state_key == "location":
            async with self.store.lock:
                legacy_row = await self.store.one(
                    "SELECT payload FROM admin_config WHERE key='weather_location'"
                )
            if legacy_row is not None:
                try:
                    saved = self.store.vault.open("admin:weather_location", legacy_row[0])
                except Exception:
                    saved = None
                if isinstance(saved, dict) and saved.get("place") is not None:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": True,
                            "result": {"found": True, "value": saved["place"]},
                        },
                    )
                    return

        if session.plugin_id == "bilibili_surf":
            fallback = await self._load_bili_state_fallback(session, req_id, state_key)
            if fallback is not None:
                found, val = fallback
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"found": found, "value": val},
                    },
                )
                return
            return

        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"found": False, "value": None},
            },
        )

    async def _table_exists(self, table_name: str) -> bool:
        row = await self.store.one(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return row is not None

    async def _load_bili_state_fallback(
        self, session: _PluginSession, req_id: str, state_key: str
    ) -> tuple[bool, Any] | None:
        if state_key == "env_defaults":
            return True, {
                "enabled": bool(getattr(self.s, "bili_autonomous_enabled", False)),
                "allow_like": False,
                "allow_favorite": False,
                "favorite_folder_id": "",
                "daily_action_limit": 1,
                "bot_timezone": str(getattr(self.s, "bot_timezone", "Asia/Shanghai")),
                "bilibili_enabled": bool(getattr(self.s, "bilibili_enabled", True)),
                "bilibili_min_interval_seconds": float(
                    getattr(self.s, "bilibili_min_interval_seconds", 3.0)
                ),
                "node_bili_delegate_enabled": bool(
                    getattr(self.s, "node_bili_delegate_enabled", True)
                ),
                "node_bili_fallback_local": bool(getattr(self.s, "node_bili_fallback_local", True)),
            }

        if state_key == "policy":
            async with self.store.lock:
                has_admin = await self._table_exists("admin_config")
                row = (
                    await self.store.one(
                        "SELECT payload FROM admin_config WHERE key='bili_account'"
                    )
                    if has_admin
                    else None
                )
            if row is None:
                return False, None
            try:
                saved = self.store.vault.open("admin:bili_account", row[0])
            except Exception:
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": "state_corrupt"},
                )
                return None
            if not isinstance(saved, dict):
                return False, None
            cleaned = {k: v for k, v in saved.items() if k not in {"times", "jitter_minutes"}}
            return True, cleaned

        if state_key == "credentials":
            cred_keys = ("sessdata", "bili_jct", "buvid3", "buvid4")
            async with self.store.lock:
                has_creds = await self._table_exists("bili_credentials")
                row = (
                    await self.store.one("SELECT payload FROM bili_credentials WHERE id=1")
                    if has_creds
                    else None
                )
            if row is not None:
                try:
                    saved = self.store.vault.open("bili-credentials", row[0])
                except Exception:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "state_corrupt",
                        },
                    )
                    return None
                if isinstance(saved, dict):
                    creds = {k: str(saved.get(k) or "") for k in cred_keys}
                    return True, {"creds": creds, "source": "sqlite"}
                return False, None

            def _secret_str(attr: str) -> str:
                raw = getattr(self.s, attr, "")
                return (
                    raw.get_secret_value() if hasattr(raw, "get_secret_value") else str(raw or "")
                )

            env_creds = {
                "sessdata": _secret_str("bili_sessdata"),
                "bili_jct": _secret_str("bili_bili_jct"),
                "buvid3": _secret_str("bili_buvid3"),
                "buvid4": _secret_str("bili_buvid4"),
            }
            if any(env_creds.values()):
                return True, {"creds": env_creds, "source": "env"}
            return False, None

        if state_key == "account_state":
            async with self.store.lock:
                has_acc = await self._table_exists("bili_account_state")
                has_dev = await self._table_exists("bili_device_state")
                acc_row = (
                    await self.store.one(
                        "SELECT blocked_until, last_error, checked_at, account "
                        "FROM bili_account_state WHERE id=1"
                    )
                    if has_acc
                    else None
                )
                dev_row = (
                    await self.store.one(
                        "SELECT fingerprint, attempted_at, status FROM bili_device_state WHERE id=1"
                    )
                    if has_dev
                    else None
                )
            if acc_row is None and dev_row is None:
                return False, None
            opened_account: Any = None
            if acc_row is not None and acc_row["account"] is not None:
                try:
                    opened_account = self.store.vault.open("bili-account", acc_row["account"])
                except Exception:
                    opened_account = None
            return True, {
                "blocked_until": float(acc_row["blocked_until"]) if acc_row else 0.0,
                "last_error": acc_row["last_error"] if acc_row else None,
                "checked_at": (
                    float(acc_row["checked_at"])
                    if acc_row and acc_row["checked_at"] is not None
                    else None
                ),
                "account": opened_account,
                "buvid4_device": (
                    {
                        "fingerprint": str(dev_row["fingerprint"]),
                        "attempted_at": float(dev_row["attempted_at"]),
                        "status": str(dev_row["status"]),
                    }
                    if dev_row is not None
                    else None
                ),
            }

        if state_key == "watched_seed":
            secrets_list = self._all_secrets()
            async with self.store.lock:
                has_watched = await self._table_exists("bili_watched_history")
                has_actions = await self._table_exists("bili_actions")
                has_runs = await self._table_exists("bili_patrol_runs")
                watched_rows = (
                    await self.store.all(
                        "SELECT bvid, run_id, status, created, summary "
                        "FROM bili_watched_history "
                        "WHERE status='done' AND summary IS NOT NULL "
                        "ORDER BY created DESC LIMIT 50"
                    )
                    if has_watched
                    else []
                )
                action_rows = (
                    await self.store.all(
                        "SELECT day, bvid, kind, status, created "
                        "FROM bili_actions ORDER BY created DESC LIMIT 100"
                    )
                    if has_actions
                    else []
                )
                run_rows = (
                    await self.store.all(
                        "SELECT id, day, slot, status, created, reason "
                        "FROM bili_patrol_runs ORDER BY created DESC LIMIT 10"
                    )
                    if has_runs
                    else []
                )
                done_actions: dict[str, list[str]] = {}
                if has_actions and watched_rows:
                    bvids = [str(r["bvid"]) for r in watched_rows]
                    slots = ",".join("?" for _ in bvids)
                    done_rows = await self.store.all(
                        "SELECT bvid, kind FROM bili_actions "
                        f"WHERE status='done' AND bvid IN ({slots})",
                        tuple(bvids),
                    )
                    for dr in done_rows:
                        done_actions.setdefault(str(dr["bvid"]), []).append(str(dr["kind"]))
            watched_items: list[dict[str, Any]] = []
            for r in watched_rows:
                bvid_val = str(r["bvid"])
                try:
                    raw_summary = self.store.vault.open("bili-learn:" + bvid_val, r["summary"])
                    redacted = redact_public_summary(
                        safe_output(str(raw_summary), secrets_list, limit=1200)
                    )
                except Exception:
                    continue
                watched_items.append(
                    {
                        "bvid": bvid_val,
                        "run_id": str(r["run_id"]),
                        "status": str(r["status"]),
                        "created": float(r["created"]),
                        "summary": redacted,
                        "actions": sorted(set(done_actions.get(bvid_val, []))),
                    }
                )
            actions_list = [
                {
                    "day": str(r["day"]),
                    "bvid": str(r["bvid"]),
                    "kind": str(r["kind"]),
                    "status": str(r["status"]),
                    "created": float(r["created"]),
                }
                for r in action_rows
            ]
            runs_list = [
                {
                    "id": str(r["id"]),
                    "day": str(r["day"]),
                    "slot": str(r["slot"]),
                    "status": str(r["status"]),
                    "created": float(r["created"]),
                    "reason": r["reason"],
                }
                for r in run_rows
            ]
            if not watched_items and not actions_list and not runs_list:
                return False, None
            return True, {
                "watched": watched_items,
                "actions": actions_list,
                "runs": runs_list,
            }

        return False, None

    async def _handle_state_set(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return
        if "value" not in frame:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "invalid_state_value",
                },
            )
            return
        value = frame["value"]
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "invalid_state_value",
                },
            )
            return
        kv_limit = (
            _MAX_KV_VALUE_BYTES if session.plugin_id == "bilibili_surf" else _LEGACY_KV_VALUE_BYTES
        )
        if len(encoded) > kv_limit:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "state_value_too_large",
                },
            )
            return
        aad = f"{_VAULT_KV_PREFIX}{session.plugin_id}:{state_key}"
        blob = self.store.vault.seal(aad, value)
        now = self.clock()
        async with self.store.transaction():
            await self.store.db.execute(
                "INSERT INTO plugin_kv_state(plugin_id, state_key, payload, updated) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(plugin_id, state_key) DO UPDATE SET "
                "payload=excluded.payload, updated=excluded.updated",
                (session.plugin_id, state_key, blob, now),
            )
        if (
            session.plugin_id == "bilibili_surf"
            and state_key == "credentials"
            and isinstance(value, dict)
        ):
            creds_obj = value.get("creds") if isinstance(value.get("creds"), dict) else value
            cookie_vals = [
                str(creds_obj.get(k) or "")
                for k in ("sessdata", "bili_jct", "buvid3", "buvid4")
                if isinstance(creds_obj.get(k), str) and creds_obj.get(k)
            ]
            if cookie_vals:
                existing_extra = tuple(getattr(self.s, "_runtime_extra_secrets", ()) or ())
                self.s._runtime_extra_secrets = tuple(
                    dict.fromkeys([*existing_extra, *cookie_vals])
                )
                svc_secrets = getattr(self.service, "secrets", None)
                if isinstance(svc_secrets, list):
                    for val in cookie_vals:
                        if val not in svc_secrets:
                            svc_secrets.append(val)
            bw = getattr(self.service, "bili_watcher", None)
            if bw is not None and hasattr(bw, "credentials_epoch"):
                bw.credentials_epoch += 1
        await self._send_frame(
            session,
            {"type": "kernel.result", "id": req_id, "ok": True, "result": {"saved": True}},
        )

    async def _handle_state_delete(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        state_key = frame.get("key")
        if not isinstance(state_key, str) or not _STATE_KEY_RE.fullmatch(state_key):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_state_key"},
            )
            return
        async with self.store.transaction():
            cur = await self.store.db.execute(
                "DELETE FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
                (session.plugin_id, state_key),
            )
            deleted = cur.rowcount > 0
            if session.plugin_id == "weather" and state_key == "location":
                cur_legacy = await self.store.db.execute(
                    "DELETE FROM admin_config WHERE key='weather_location'"
                )
                deleted = deleted or (cur_legacy.rowcount > 0)
            elif session.plugin_id == "bilibili_surf" and state_key == "policy":
                if await self._table_exists("admin_config"):
                    cur_legacy = await self.store.db.execute(
                        "DELETE FROM admin_config WHERE key='bili_account'"
                    )
                    deleted = deleted or (cur_legacy.rowcount > 0)
        await self._send_frame(
            session,
            {
                "type": "kernel.result",
                "id": req_id,
                "ok": True,
                "result": {"deleted": bool(deleted)},
            },
        )

    async def _handle_node_call(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        if "node_bridge" not in session.scopes:
            log.warning(
                "event=plugin_node_bridge_rejected plugin=%s reason=scope_forbidden",
                session.plugin_id,
            )
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "scope_forbidden",
                },
            )
            return
        action = frame.get("action")
        params = frame.get("params", {})
        allowed_actions = _NODE_BRIDGE_PLUGIN_ACTIONS.get(session.plugin_id, frozenset())
        if not isinstance(action, str) or action not in allowed_actions:
            log.warning(
                "event=plugin_node_bridge_rejected plugin=%s action=%s",
                session.plugin_id,
                sanitize_sensor_text(str(action), self.s, max_tokens=16)[:64],
            )
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_action_forbidden",
                },
            )
            return
        if not isinstance(params, dict):
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_invalid_params",
                },
            )
            return
        if session.inflight_node_calls >= _MAX_NODE_BRIDGE_INFLIGHT:
            await self._send_frame(
                session,
                {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": False,
                    "error": "node_backpressure",
                },
            )
            return
        session.inflight_node_calls += 1

        async def _dispatch() -> None:
            try:
                nt = getattr(self.service, "node_transport", None)
                if nt is None:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_disconnected",
                        },
                    )
                    return
                try:
                    data = await nt.call(action, params)
                except asyncio.CancelledError:
                    raise
                except Rejected as exc:
                    raw_err = str(exc).strip()
                    code = raw_err if _CODE_RE.fullmatch(raw_err) else "node_rpc_failed"
                    err_frame: dict[str, Any] = {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": code,
                    }
                    detail = getattr(exc, "detail", "")
                    if isinstance(detail, str) and detail and _CODE_RE.fullmatch(detail):
                        err_frame["message"] = detail
                    await self._send_frame(session, err_frame)
                    return
                except Exception:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_rpc_failed",
                        },
                    )
                    return
                raw_obj = data if isinstance(data, dict) else {"data": data}
                if session.plugin_id == "bilibili_surf" and action.startswith("bili."):
                    sanitized = _scrub_json_secrets(raw_obj, self._all_secrets())
                else:
                    sanitized = _sanitize_payload(raw_obj, self.s)
                res_frame: dict[str, Any] = {
                    "type": "kernel.result",
                    "id": req_id,
                    "ok": True,
                    "result": sanitized,
                }
                try:
                    encoded = json.dumps(res_frame, ensure_ascii=False, allow_nan=False).encode(
                        "utf-8"
                    )
                except (TypeError, ValueError):
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_invalid_response",
                        },
                    )
                    return
                if len(encoded) > self._max_frame_bytes():
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "node_frame_too_large",
                        },
                    )
                    return
                await self._send_frame(session, res_frame)
            finally:
                session.inflight_node_calls = max(0, session.inflight_node_calls - 1)

        if session.closed or session.fiber.state != "active":
            session.inflight_node_calls = max(0, session.inflight_node_calls - 1)
            return
        try:
            session.ctx.spawn(
                _dispatch(), name=f"plugin-node-call:{session.plugin_id}:{req_id[:16]}"
            )
        except RuntimeError:
            session.inflight_node_calls = max(0, session.inflight_node_calls - 1)

    async def relay_node_event(self, frame: dict[str, Any]) -> None:
        if not isinstance(frame, dict):
            return
        sanitized_frame = _sanitize_payload(frame, self.s)
        payload = {"type": "node.event", "frame": sanitized_frame}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and "node_bridge" in session.scopes:
                await self._send_frame(session, payload)

    async def relay_node_state(self, connected: bool) -> None:
        payload = {"type": "node.state", "connected": bool(connected)}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and "node_bridge" in session.scopes:
                await self._send_frame(session, payload)

    async def broadcast_world_event(self, event: Any) -> None:
        if isinstance(event, dict):
            payload = event.get("payload")
            event_type = event.get("event_type")
            event_id = event.get("id") or ""
            created_at = event.get("created_at", self.clock())
            source_module = event.get("source_module") or ""
            importance = event.get("importance") or "normal"
        else:
            payload = getattr(event, "payload", None)
            event_type = getattr(event, "event_type", None)
            event_id = getattr(event, "id", "") or ""
            created_at = getattr(event, "created_at", self.clock())
            source_module = getattr(event, "source_module", "") or ""
            importance = getattr(event, "importance", "normal") or "normal"

        if not isinstance(payload, dict):
            return
        if payload.get("private_first") or payload.get("owner_private"):
            return
        if not isinstance(event_type, str) or not _EVENT_TYPE_RE.fullmatch(event_type):
            return
        clean_ts = (
            float(created_at)
            if isinstance(created_at, int | float)
            and not isinstance(created_at, bool)
            and math.isfinite(float(created_at))
            else self.clock()
        )
        sanitized_dict = {
            "id": sanitize_sensor_text(str(event_id), self.s, max_tokens=24)[:64],
            "created_at": clean_ts,
            "source_module": sanitize_sensor_text(str(source_module), self.s, max_tokens=24)[:64],
            "event_type": sanitize_sensor_text(event_type, self.s, max_tokens=24)[:48],
            "importance": importance if importance in ("low", "normal", "high") else "normal",
            "payload": _sanitize_payload(payload, self.s),
        }
        frame = {"type": "world.event.broadcast", "event": sanitized_dict}
        for session in list(self._sessions.values()):
            if session.ready and not session.closed and event_type in session.event_subscriptions:
                await self._send_frame(session, frame)

    async def _handle_plugin_status(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        proxy = session.proxy
        if proxy is None:
            return
        if "status" in frame and frame["status"] is not None:
            raw_status = frame["status"]
            if not isinstance(raw_status, dict):
                raise Rejected("plugin_invalid_schema")
            proxy._remote_status = _sanitize_payload(raw_status, self.s)
        if "configuration_issue" in frame:
            raw_issue = frame["configuration_issue"]
            if raw_issue is None or raw_issue == "":
                proxy._configuration_issue = None
            elif isinstance(raw_issue, str) and _CODE_RE.fullmatch(raw_issue):
                proxy._configuration_issue = raw_issue
            else:
                raise Rejected("plugin_invalid_schema")
        if "unavailable_actions" in frame and frame["unavailable_actions"] is not None:
            raw_unavail = frame["unavailable_actions"]
            if not isinstance(raw_unavail, dict) or len(raw_unavail) > 32:
                raise Rejected("plugin_invalid_schema")
            declared = {a.name for a in proxy.actions()}
            if not raw_unavail:
                proxy._action_unavailable.clear()
            else:
                for act_name, reason in raw_unavail.items():
                    if not isinstance(act_name, str) or not _ACTION_NAME_RE.fullmatch(act_name):
                        raise Rejected("plugin_invalid_schema")
                    if act_name not in declared:
                        raise Rejected("plugin_scope_rejected")
                    if reason is None or reason == "":
                        proxy._action_unavailable.pop(act_name, None)
                    elif isinstance(reason, str) and _CODE_RE.fullmatch(reason):
                        proxy._action_unavailable[act_name] = reason
                    else:
                        raise Rejected("plugin_invalid_schema")
        if "current_activity" in frame:
            raw_act = frame["current_activity"]
            if raw_act is None or raw_act == "":
                proxy._current_activity = None
            elif isinstance(raw_act, str) and _CODE_RE.fullmatch(raw_act):
                proxy._current_activity = raw_act
            else:
                raise Rejected("plugin_invalid_schema")
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "plugin.status.ack", "id": frame["id"], "ok": True},
            )

    async def _handle_world_activity(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_task = frame.get("active_task", "")
        if raw_task is None:
            raw_task = ""
        if not isinstance(raw_task, str):
            raise Rejected("plugin_invalid_schema")
        active_task = sanitize_sensor_text(raw_task, self.s, max_tokens=16)[:32]
        disconnected = frame.get("disconnected", False)
        if not isinstance(disconnected, bool):
            raise Rejected("plugin_invalid_schema")
        raw_ts = frame.get("updated_at")
        if raw_ts is None:
            updated_at = self.clock()
        elif (
            isinstance(raw_ts, int | float)
            and not isinstance(raw_ts, bool)
            and math.isfinite(float(raw_ts))
            and float(raw_ts) >= 0.0
        ):
            updated_at = float(raw_ts)
        else:
            raise Rejected("plugin_invalid_schema")

        kind = frame.get("kind")
        if kind == "bili_reading":
            if session.plugin_id != "bilibili_surf":
                raise Rejected("plugin_scope_rejected")
            active = frame.get("active")
            if not isinstance(active, bool):
                raise Rejected("plugin_invalid_schema")
            ws = getattr(self.service, "world_state", None)
            if ws is not None and hasattr(ws, "experience"):
                exp = ws.experience
                if not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                    await ws.initialize()
                await exp.bili_reading(active, updated_at)
        else:
            if kind is not None and (not isinstance(kind, str) or not _CODE_RE.fullmatch(kind)):
                raise Rejected("plugin_invalid_schema")
            ws = getattr(self.service, "world_state", None)
            if (
                ws is not None
                and hasattr(ws, "experience")
                and (
                    session.plugin_id == "minecraft"
                    or (session.proxy is not None and session.proxy.attention_level == "HIGH")
                )
            ):
                exp = ws.experience
                if not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
                    await ws.initialize()
                await exp.mc_snapshot(active_task, updated_at, disconnected=disconnected)
        if isinstance(frame.get("id"), str) and frame["id"]:
            await self._send_frame(
                session,
                {"type": "world.activity.ack", "id": frame["id"], "accepted": True},
            )

    async def _handle_world_reaction(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "actions" not in session.scopes or "sensors" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        frame_id = frame.get("id") if isinstance(frame.get("id"), str) else None

        async def _reject(reason: str) -> None:
            log.warning(
                "event=plugin_reaction_rejected plugin=%s reason=%s",
                session.plugin_id,
                reason,
            )
            if frame_id:
                await self._send_frame(
                    session,
                    {
                        "type": "world.reaction.ack",
                        "id": frame_id,
                        "ok": False,
                        "reason": reason,
                    },
                )

        action_name = frame.get("action_name")
        registry = getattr(self.service, "modules", None)
        if (
            not isinstance(action_name, str)
            or not _ACTION_NAME_RE.fullmatch(action_name)
            or registry is None
            or registry.catalog.owners.get(action_name) != session.plugin_id
        ):
            await _reject("action_not_owned")
            return
        spec = registry.catalog.actions.get(action_name)
        if spec is None:
            await _reject("unknown_action")
            return

        raw_note = frame.get("note") if "note" in frame else frame.get("intent_note", "")
        if not isinstance(raw_note, str):
            await _reject("invalid_note")
            return
        clean_note = sanitize_sensor_text(clean_text(raw_note), self.s, max_tokens=64)[:240].strip()
        if not clean_note:
            await _reject("empty_note")
            return

        raw_args = frame.get("arguments")
        if raw_args is None:
            raw_args = {}
        if not isinstance(raw_args, dict):
            await _reject("invalid_action_arguments")
            return
        clean_args = _sanitize_payload(raw_args, self.s)

        raw_ttl = frame.get("ttl", 7200)
        if (
            isinstance(raw_ttl, bool)
            or not isinstance(raw_ttl, int | float)
            or not math.isfinite(float(raw_ttl))
            or float(raw_ttl) <= 0
            or float(raw_ttl) > 86400
        ):
            await _reject("invalid_ttl")
            return
        ttl = float(raw_ttl)

        raw_delay = frame.get("delay", 0)
        if (
            isinstance(raw_delay, bool)
            or not isinstance(raw_delay, int | float)
            or not math.isfinite(float(raw_delay))
            or float(raw_delay) < 0
            or float(raw_delay) > 3600
        ):
            await _reject("invalid_delay")
            return
        delay = float(raw_delay)

        raw_ev_id = frame.get("source_event_id") or ""
        if not isinstance(raw_ev_id, str):
            await _reject("invalid_source_event_id")
            return
        source_event_id = sanitize_sensor_text(raw_ev_id, self.s, max_tokens=16)[:64]

        raw_legacy_mod = frame.get("legacy_module") or ""
        if not isinstance(raw_legacy_mod, str) or (
            raw_legacy_mod and raw_legacy_mod != session.plugin_id
        ):
            await _reject("invalid_legacy_module")
            return

        raw_legacy_verb = frame.get("legacy_verb") or ""
        if not isinstance(raw_legacy_verb, str):
            await _reject("invalid_legacy_verb")
            return
        legacy_verb = sanitize_sensor_text(raw_legacy_verb, self.s, max_tokens=16)[:64]

        raw_scope = frame.get("target_scope") or ""
        if not isinstance(raw_scope, str):
            await _reject("invalid_action_audience")
            return
        target_scope = sanitize_sensor_text(raw_scope, self.s, max_tokens=24)[:128]

        ws = getattr(self.service, "world_state", None)
        if ws is None or not hasattr(ws, "enqueue_intent"):
            await _reject("world_state_unavailable")
            return
        exp = getattr(ws, "experience", None)
        if exp is not None and not getattr(exp, "ready", True) and hasattr(ws, "initialize"):
            await ws.initialize()

        now = self.clock()
        dedupe_key = (
            f"plugin_rule:{session.plugin_id}:{action_name}:{source_event_id}"[:160]
            if source_event_id
            else None
        )
        mood = str(getattr(getattr(ws, "current", None), "mood", "") or "calm")
        try:
            ticket = IntentTicketCreate(
                action_name=action_name,
                target_scope=target_scope,
                origin="rule",
                target_module=session.plugin_id,
                action_type=legacy_verb or action_name,
                source_event_id=source_event_id,
                source_module=session.plugin_id,
                arguments=clean_args,
                execute_after=now + delay,
                expire_at=now + delay + ttl,
                gate_condition={"milestone_event_id": source_event_id} if source_event_id else {},
                intent_note=clean_note,
                mood=mood,
                dedupe_key=dedupe_key,
            )
            engine = getattr(self.service, "life_engine", None)
            if engine is not None and hasattr(engine, "resolve"):
                engine.resolve(ticket.model_dump())
            else:
                if reason := registry.inactive_reason(session.plugin_id):
                    raise ValueError(reason)
                if "rule" not in spec.origins:
                    raise ValueError("action_origin_forbidden")
                if spec.social != bool(target_scope):
                    raise ValueError("invalid_action_audience")
                spec.arguments.model_validate(clean_args)
        except ValueError as exc:
            await _reject(reject_reason(exc))
            return

        intent_id = await ws.enqueue_intent(ticket, now=now)
        engine = getattr(self.service, "life_engine", None)
        if engine is not None and getattr(engine, "wakeup", None) is not None:
            engine.wakeup.set()
        log.info(
            "event=plugin_reaction_enqueued plugin=%s action=%s",
            session.plugin_id,
            action_name,
        )
        if frame_id:
            await self._send_frame(
                session,
                {
                    "type": "world.reaction.ack",
                    "id": frame_id,
                    "ok": True,
                    "intent_id": intent_id,
                },
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
            self._ensure_plugin_not_reserved(pid)
        except Rejected as exc:
            log.warning("event=plugin_scope_rejected plugin=%s reason=%s", plugin_id, str(exc))
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
                self._ensure_plugin_not_reserved(pid)
                hello, scope_set, chat_platforms, capabilities = self._validate_hello(
                    hello_raw, cred, plugin_id=pid
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
                event_subscriptions=frozenset(hello.event_subscriptions),
            )

            # Register LIFO cleanup effects (reverse execution order:
            # 3rd registered -> runs 1st: cancel pending RPCs + mark in-flight deliveries uncertain
            # 2nd registered -> runs 2nd: unregister proxy & refresh _thinking_scenes
            # 1st registered -> runs 3rd: clear session entry, close WS, log disconnected)
            async def _cleanup_session_and_close(sess: _PluginSession = session) -> None:
                if self._sessions.get(sess.plugin_id) is sess:
                    self._sessions.pop(sess.plugin_id, None)
                sess._bili_private_handles.clear()
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
                actions, action_timeouts = self._build_actions(session, hello.actions)
                if hello.session_exit_action is not None:
                    exit_spec = next(
                        (spec for spec in actions if spec.name == hello.session_exit_action),
                        None,
                    )
                    if exit_spec is None or exit_spec.social:
                        raise Rejected("plugin_invalid_schema")
                    try:
                        exit_spec.arguments()
                    except (ValidationError, TypeError, ValueError):
                        raise Rejected("plugin_invalid_schema") from None
                scenes = self._build_thinking_scenes(pid, hello.thinking_scenes)
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
                    attention_level=hello.attention_level,
                    admin_panel=hello.admin_panel,
                    configuration_issue=hello.configuration_issue,
                    initial_status=hello.initial_status,
                    action_timeouts=action_timeouts,
                    session_exit_action=hello.session_exit_action,
                    voice_format=hello.voice_format,
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

        enabled_now = session.proxy.enabled if session.proxy else True
        scopes_now = sorted(session.scopes)
        await self._send_frame(
            session,
            {
                "type": "plugin.welcome",
                "plugin_id": pid,
                "scopes": scopes_now,
                "enabled": enabled_now,
            },
        )
        await self._send_frame(
            session,
            {
                "type": "plugin.ready",
                "plugin_id": pid,
                "protocol_version": 1,
                "enabled": enabled_now,
                "scopes": scopes_now,
                "node_region": session.node_region,
            },
        )
        nt = getattr(self.service, "node_transport", None)
        if (
            nt is not None
            and "node_bridge" in session.scopes
            and callable(getattr(nt, "connected", None))
        ):
            await self._send_frame(
                session,
                {"type": "node.state", "connected": bool(nt.connected())},
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
                elif ftype == "ai.budget_check":
                    await self._handle_ai_budget_check(session, frame)
                elif ftype == "ai.screen":
                    await self._handle_ai_screen(session, frame)
                elif ftype == "ai.generate":
                    await self._handle_ai_generate(session, frame)
                elif ftype == "ai.video_analyze":
                    await self._handle_ai_video_analyze(session, frame)
                elif ftype == "state.get":
                    await self._handle_state_get(session, frame)
                elif ftype == "state.set":
                    await self._handle_state_set(session, frame)
                elif ftype == "state.delete":
                    await self._handle_state_delete(session, frame)
                elif ftype == "node.call":
                    await self._handle_node_call(session, frame)
                elif ftype == "plugin.status":
                    await self._handle_plugin_status(session, frame)
                elif ftype == "world.activity":
                    await self._handle_world_activity(session, frame)
                elif ftype == "world.reaction":
                    await self._handle_world_reaction(session, frame)
                elif ftype == "bili.memory_preferences":
                    await self._handle_bili_memory_preferences(session, frame)
                elif ftype == "bili.private_topic_commit":
                    await self._handle_bili_private_topic_commit(session, frame)
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
