"""Protocol constants, SQLite schema, frame models, and sanitization/validation helpers."""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import math
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, create_model, field_validator

from ..bot_server import untrusted_input
from ..config import Settings
from ..modules.actions import NoArgs
from ..modules.base import AttentionLevel, NodeRegion, sanitize_sensor_text
from ..outbound_actions import require_all_schema_fields
from ..security import Rejected, clean_text
from ..thinking_policy import CORE_THINKING_SCENES, THINKING_LEVELS, OutputCap

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
SCHEMA = _SCHEMA

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
_VALID_OUTCOME_STATUSES = frozenset(
    {"done", "gated_skip", "not_dispatched", "uncertain", "executing", "expired"}
)

_MAX_PLUGIN_TASK_INSTRUCTION_CHARS = 2400
_MAX_PLUGIN_RESPONSE_SCHEMA_BYTES = 8192
_MAX_PLUGIN_RESPONSE_SCHEMA_DEPTH = 8

_VALID_VOICE_FORMATS = frozenset({"ogg_opus", "mp3", "wav"})
_MAX_VOICE_AUDIO_BYTES = 2 * 1024 * 1024


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
