"""Standalone lightweight runtime SDK for out-of-process Hakimi remote plugins."""

from .client import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
    ThinkingSceneSpec,
)
from .protocol import (
    BLOB_CHUNK_RAW_BYTES,
    BLOB_HARD_MAX_BYTES,
    BLOB_INLINE_MAX_BYTES,
    BlobResult,
    ToolCallResult,
)

__all__ = [
    "BLOB_CHUNK_RAW_BYTES",
    "BLOB_HARD_MAX_BYTES",
    "BLOB_INLINE_MAX_BYTES",
    "ActionSpec",
    "BlobResult",
    "HakimiPluginClient",
    "PluginRpcError",
    "PluginRuntimeConfig",
    "ThinkingSceneSpec",
    "ToolCallResult",
]
