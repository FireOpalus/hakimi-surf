"""Standalone KR universal audio gateway (`hakimi-voice-gateway`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .audio_codec import (
    OUTPUT_FORMATS,
    normalize_input_to_wav,
    output_format_meta,
    transcode_audio_stream,
)
from .config import VoiceGatewayConfig, load_gateway_config, redact_secrets
from .protocol import (
    ADAPTER_ID_RE,
    CLIENT_ID_RE,
    DEFAULT_ADAPTER_ID,
    DEFAULT_CLIENT_ID,
    ID_RE,
    ProtocolError,
    build_error_frame,
    decode_json_frame,
    validate_adapter_id,
    validate_client_id,
    validate_id,
)
from .session import VoiceGatewaySession

if TYPE_CHECKING:
    from .app import create_gateway_app

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_ID_RE",
    "CLIENT_ID_RE",
    "DEFAULT_ADAPTER_ID",
    "DEFAULT_CLIENT_ID",
    "ID_RE",
    "OUTPUT_FORMATS",
    "ProtocolError",
    "VoiceGatewayConfig",
    "VoiceGatewaySession",
    "__version__",
    "build_error_frame",
    "create_gateway_app",
    "decode_json_frame",
    "load_gateway_config",
    "normalize_input_to_wav",
    "output_format_meta",
    "redact_secrets",
    "transcode_audio_stream",
    "validate_adapter_id",
    "validate_client_id",
    "validate_id",
]


def __getattr__(name: str) -> Any:
    if name == "create_gateway_app":
        from .app import create_gateway_app

        return create_gateway_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
