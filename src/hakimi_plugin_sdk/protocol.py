"""Shared bounded binary/blob transfer protocol helpers for Hakimi remote plugins."""

from __future__ import annotations

import base64
import binascii
import hashlib
import math
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

BLOB_INLINE_MAX_BYTES = 96 * 1024
BLOB_CHUNK_RAW_BYTES = 128 * 1024
BLOB_HARD_MAX_BYTES = 40 * 1024 * 1024
BLOB_TRANSFER_TTL_SECONDS = 60.0
MAX_CONCURRENT_BLOBS_PER_PLUGIN = 2
MAX_BUFFERED_BLOB_BYTES_PER_PLUGIN = 48 * 1024 * 1024

BLOB_FIELD = "_blob"
BLOB_RPC_PUT = "blob.put"
BLOB_RPC_GET = "blob.get"
BLOB_RPC_RELEASE = "blob.release"

BLOB_TRANSFER_ID_RE = re.compile(r"^[a-f0-9]{16,64}\Z")
SHA256_HEX_RE = re.compile(r"^[a-f0-9]{64}\Z")
DECISION_TEMPLATE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}\Z")
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f]")


@dataclass(frozen=True)
class BlobResult:
    """Return value from a plugin handler carrying metadata plus a bounded binary blob."""

    data: dict[str, Any]
    blob: bytes
    mime_type: str = "application/octet-stream"
    filename: str = ""


@dataclass(frozen=True)
class ToolCallResult:
    """Kernel-side result returned by `PluginBus.invoke_tool`."""

    data: dict[str, Any]
    blob: bytes | None = None
    blob_mime_type: str = ""
    blob_filename: str = ""


def sha256_hex(data: bytes) -> str:
    """Compute lowercase SHA-256 hex digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def compute_chunk_raw_bytes(max_frame_bytes: int) -> int:
    """Return safe raw bytes per chunk so base64 + JSON envelope stays strictly under frame cap."""
    cap = max(16_384, int(max_frame_bytes))
    available_b64 = max(4096, cap - 4096)
    raw_cap = (available_b64 // 4) * 3
    return min(BLOB_CHUNK_RAW_BYTES, max(4096, raw_cap))


def compute_inline_max_bytes(max_frame_bytes: int) -> int:
    """Return max raw bytes that can be safely inlined inside one JSON RPC frame."""
    cap = max(16_384, int(max_frame_bytes))
    reserve = max(16_384, cap // 4)
    usable_b64 = max(0, cap - reserve)
    raw_cap = (usable_b64 // 4) * 3
    return min(BLOB_INLINE_MAX_BYTES, raw_cap)


def sanitize_blob_meta_str(
    value: Any,
    *,
    max_len: int,
    default: str = "",
    is_filename: bool = False,
) -> str:
    """Sanitize optional blob metadata strings (stripping control chars and path traversal)."""
    if not isinstance(value, str):
        return default
    cleaned = _CTRL_RE.sub("", value).strip()
    if is_filename:
        cleaned = PurePosixPath(cleaned.replace("\\", "/")).name.strip()
        if cleaned in {"", ".", ".."}:
            return default
    if not cleaned:
        return default
    return cleaned[:max_len]


def encode_blob_chunk(data: bytes) -> str:
    """Encode raw bytes to ASCII base64."""
    return base64.b64encode(data).decode("ascii")


def decode_blob_chunk(raw_b64: Any, *, max_raw_bytes: int) -> bytes:
    """Strictly decode bounded base64 chunk, rejecting malformed or oversized payloads."""
    if not isinstance(raw_b64, str):
        raise ValueError("blob_invalid")
    if max_raw_bytes < 0:
        raise ValueError("blob_too_large")
    max_b64_len = 4 * ((max_raw_bytes + 2) // 3)
    if len(raw_b64) > max_b64_len:
        raise ValueError("blob_too_large")
    try:
        decoded = base64.b64decode(raw_b64.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError):
        raise ValueError("blob_invalid") from None
    if len(decoded) > max_raw_bytes:
        raise ValueError("blob_too_large")
    return decoded


def validate_blob_descriptor(
    raw: Any,
    *,
    max_bytes: int,
    id_field: str = "blob_id",
) -> dict[str, Any]:
    """Validate an inline or chunked `_blob` descriptor against strict schema & size caps."""
    if not isinstance(raw, dict):
        raise ValueError("blob_invalid")
    encoding = raw.get("encoding")
    if encoding not in {"inline_b64", "chunked"}:
        raise ValueError("blob_invalid")

    byte_size = raw.get("byte_size")
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
        raise ValueError("blob_invalid")
    if byte_size > max_bytes or byte_size > BLOB_HARD_MAX_BYTES:
        raise ValueError("blob_too_large")

    digest = raw.get("sha256")
    if not isinstance(digest, str) or not SHA256_HEX_RE.fullmatch(digest):
        raise ValueError("blob_invalid")

    mime_type = sanitize_blob_meta_str(
        raw.get("mime_type"),
        max_len=96,
        default="application/octet-stream",
    )
    filename = sanitize_blob_meta_str(
        raw.get("filename"),
        max_len=160,
        default="",
        is_filename=True,
    )

    if encoding == "inline_b64":
        data_b64 = raw.get("data_b64")
        if not isinstance(data_b64, str):
            raise ValueError("blob_invalid")
        return {
            "encoding": "inline_b64",
            "data_b64": data_b64,
            "byte_size": byte_size,
            "sha256": digest,
            "mime_type": mime_type,
            "filename": filename,
        }

    handle = raw.get(id_field)
    if not isinstance(handle, str) or not BLOB_TRANSFER_ID_RE.fullmatch(handle):
        raise ValueError("blob_invalid")
    total_chunks = raw.get("total_chunks")
    if isinstance(total_chunks, bool) or not isinstance(total_chunks, int) or total_chunks < 1:
        raise ValueError("blob_invalid")
    max_allowed_chunks = max(2, (max(byte_size, 1) + 4095) // 4096 + 2)
    if total_chunks > max_allowed_chunks:
        raise ValueError("blob_invalid")

    out: dict[str, Any] = {
        "encoding": "chunked",
        id_field: handle,
        "byte_size": byte_size,
        "sha256": digest,
        "total_chunks": total_chunks,
        "mime_type": mime_type,
        "filename": filename,
    }
    if "chunk_size" in raw and raw["chunk_size"] is not None:
        chunk_size = raw["chunk_size"]
        if (
            isinstance(chunk_size, bool)
            or not isinstance(chunk_size, int)
            or not (4096 <= chunk_size <= BLOB_CHUNK_RAW_BYTES)
        ):
            raise ValueError("blob_invalid")
        out["chunk_size"] = chunk_size
    return out


@dataclass(frozen=True)
class DecisionAnswer:
    """Structured single-question decision answer returned by kernel `ai.screen`."""

    kind: str
    confidence: float
    confident: bool = False
    value: bool | None = None
    noul: float | None = None
    choice: str | None = None
    probabilities: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ScreenDecisionResult:
    """Parsed `ai.screen` result carrying safety pass flag and optional template decisions."""

    passed: bool
    template: str | None = None
    decisions: dict[str, DecisionAnswer] = field(default_factory=dict)

    def confidence_of(self, key: str) -> float:
        ans = self.decisions.get(key)
        return ans.confidence if ans is not None else 0.0

    def get_noul(self, key: str, *, min_confidence: float | None = None) -> bool | None:
        ans = self.decisions.get(key)
        if ans is None or ans.kind != "noul" or ans.value is None:
            return None
        ok = ans.confident if min_confidence is None else ans.confidence >= min_confidence
        return ans.value if ok else None

    def get_choice(self, key: str, *, min_confidence: float | None = None) -> str | None:
        ans = self.decisions.get(key)
        if ans is None or ans.kind != "choice" or not ans.choice:
            return None
        ok = ans.confident if min_confidence is None else ans.confidence >= min_confidence
        return ans.choice if ok else None


def _parse_unit_float(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    val = float(raw)
    if not math.isfinite(val) or not 0.0 <= val <= 1.0:
        return None
    return val


def parse_screen_decision_result(raw: Any) -> ScreenDecisionResult:
    """Parse a raw `ai.screen` dict into a validated `ScreenDecisionResult`.

    Safety-first: if `passed` is not strictly `True`, any attached decisions are discarded.
    Malformed individual decision entries are ignored so callers fail closed / conservatively.
    """
    if not isinstance(raw, dict) or raw.get("passed") is not True:
        return ScreenDecisionResult(passed=False)

    raw_tpl = raw.get("template")
    template = (
        raw_tpl if isinstance(raw_tpl, str) and DECISION_TEMPLATE_ID_RE.fullmatch(raw_tpl) else None
    )
    raw_decisions = raw.get("decisions")
    if not isinstance(raw_decisions, dict):
        return ScreenDecisionResult(passed=True, template=template)

    parsed: dict[str, DecisionAnswer] = {}
    for key, item in raw_decisions.items():
        if not isinstance(key, str) or not isinstance(item, dict):
            continue
        kind = item.get("type")
        conf = _parse_unit_float(item.get("confidence"))
        if conf is None:
            continue
        confident = bool(item.get("confident", False))
        if kind == "noul":
            prob = _parse_unit_float(item.get("noul"))
            raw_val = item.get("value")
            if prob is None:
                continue
            val = raw_val if isinstance(raw_val, bool) else (prob >= 0.5)
            parsed[key] = DecisionAnswer(
                kind="noul",
                confidence=conf,
                confident=confident,
                value=val,
                noul=prob,
            )
        elif kind == "choice":
            choice = item.get("choice")
            if not isinstance(choice, str) or not choice.strip():
                continue
            raw_probs = item.get("probabilities")
            probs: dict[str, float] = {}
            if isinstance(raw_probs, dict):
                valid_probs = True
                for opt, p_raw in raw_probs.items():
                    p_val = _parse_unit_float(p_raw)
                    if not isinstance(opt, str) or p_val is None:
                        valid_probs = False
                        break
                    probs[opt] = p_val
                if not valid_probs:
                    continue
            parsed[key] = DecisionAnswer(
                kind="choice",
                confidence=conf,
                confident=confident,
                choice=choice,
                probabilities=probs,
            )
    return ScreenDecisionResult(passed=True, template=template, decisions=parsed)
