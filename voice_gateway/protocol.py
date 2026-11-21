"""Protocol validators, safe JSON frame decoding, and scrubbed error frame builders."""

from __future__ import annotations

import json
import re
import uuid
from typing import Any

from .config import VoiceGatewayConfig, redact_secrets

ADAPTER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{1,31}$")
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
REASON_RE = re.compile(r"^[a-z0-9_.-]{1,64}$")

DEFAULT_ADAPTER_ID = "voice-client"
DEFAULT_CLIENT_ID = "default"


class ProtocolError(ValueError):
    """Protocol validation error carrying a safe constant reason code and scrubbed detail."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason if REASON_RE.fullmatch(reason) else "protocol_error"
        self.detail = detail


def validate_adapter_id(value: str | None, *, default: str = DEFAULT_ADAPTER_ID) -> str:
    """Validate X-Voice-Adapter header when present, falling back to default when omitted."""
    if value is None:
        return default
    clean = value.strip()
    if not ADAPTER_ID_RE.fullmatch(clean):
        raise ProtocolError("invalid_adapter_id", "Invalid X-Voice-Adapter header")
    return clean


def validate_client_id(value: str | None, *, default: str = DEFAULT_CLIENT_ID) -> str:
    """Validate X-Voice-Client-Id header when present, falling back to default when omitted."""
    if value is None:
        return default
    clean = value.strip()
    if not CLIENT_ID_RE.fullmatch(clean):
        raise ProtocolError("invalid_client_id", "Invalid X-Voice-Client-Id header")
    return clean


def validate_id(
    value: Any,
    *,
    field_name: str = "id",
    allow_empty: bool = False,
) -> str:
    """Validate turn_id, stream_id, or req_id against ^[A-Za-z0-9_.-]{1,64}$."""
    if value is None:
        if allow_empty:
            return ""
        raise ProtocolError(f"invalid_{field_name}", f"Missing {field_name}")
    if not isinstance(value, str):
        raise ProtocolError(f"invalid_{field_name}", f"{field_name} must be a string")
    clean = value.strip()
    if not clean:
        if allow_empty:
            return ""
        raise ProtocolError(f"invalid_{field_name}", f"{field_name} must not be empty")
    if not ID_RE.fullmatch(clean):
        raise ProtocolError(f"invalid_{field_name}", f"{field_name} format is invalid")
    return clean


def generate_id(prefix: str = "id") -> str:
    """Generate a compact protocol-compliant identifier."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _reject_json_constant(val: str) -> Any:
    raise ValueError(f"Invalid JSON constant: {val}")


def decode_json_frame(
    raw: str | bytes,
    *,
    max_bytes: int = 16_777_216,
) -> dict[str, Any]:
    """Decode a bounded JSON object frame while rejecting non-standard constants."""
    raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
    if len(raw_bytes) > max_bytes:
        raise ProtocolError("frame_too_large", "WebSocket frame exceeds max_ws_frame_bytes")

    try:
        text = raw_bytes.decode("utf-8")
        obj = json.loads(text, parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise ProtocolError("invalid_json", "Invalid JSON WebSocket frame") from None

    if not isinstance(obj, dict):
        raise ProtocolError("invalid_frame", "WebSocket JSON frame must be an object")

    frame_type = obj.get("type")
    if not isinstance(frame_type, str) or not frame_type.strip():
        raise ProtocolError("missing_frame_type", "WebSocket frame missing string 'type'")

    return obj


def safe_error_detail(
    detail: str | BaseException,
    config: VoiceGatewayConfig | None = None,
    *,
    limit: int = 240,
) -> str:
    """Return single-line secret-scrubbed error detail bounded to limit characters."""
    raw = str(detail)
    scrubbed = redact_secrets(raw, config)
    collapsed = " ".join(scrubbed.split())
    if len(collapsed) > limit:
        return collapsed[: max(0, limit - 3)] + "..."
    return collapsed


def build_error_frame(
    reason: str,
    detail: str = "",
    *,
    config: VoiceGatewayConfig | None = None,
    req_id: str = "",
    turn_id: str = "",
    stream_id: str = "",
) -> dict[str, Any]:
    """Construct a secret-scrubbed gateway error frame."""
    clean_reason = reason.strip() if REASON_RE.fullmatch(reason.strip()) else "gateway_error"
    frame: dict[str, Any] = {
        "type": "error",
        "reason": clean_reason,
        "detail": safe_error_detail(detail, config) if detail else clean_reason,
    }
    if req_id:
        frame["req_id"] = req_id
    if turn_id:
        frame["turn_id"] = turn_id
    if stream_id:
        frame["stream_id"] = stream_id
    return frame
