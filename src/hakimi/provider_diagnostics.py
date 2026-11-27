"""Content-free facts about model call failures, shared by both admin audit views."""

from __future__ import annotations

import json
import re
import socket
import ssl

import httpx
from google.genai._interactions import APIConnectionError, APITimeoutError

from .thinking_policy import FLEX_MODELS, MODEL_LANES

STAGES = {"decision_message", "decision_source", "decision_output", "generation", "search"}
PROVIDERS = {"gemini_free", "gemini_paid", "vertex_paid"}
LANES = MODEL_LANES
CAUSES = {
    "deadline",
    "connect_timeout",
    "read_timeout",
    "write_timeout",
    "pool_timeout",
    "sdk_timeout",
    "timeout",
    "http",
    "dns",
    "tls",
    "connection",
    "transport",
    "stream_error",
    "circuit_open",
    "unknown",
}
# SDK error messages, HTTP bodies and even arbitrary provider-supplied identifiers can
# contain request data. Only known protocol codes may leave the call boundary.
SAFETY_UPSTREAM_CODES = frozenset(
    {
        "PROHIBITED_CONTENT",
        "SAFETY",
        "IMAGE_SAFETY",
        "SAFETY_BLOCKED",
        "CONTENT_BLOCKED",
        "CONTENT_FILTERED",
        "CONTENT_POLICY_VIOLATION",
        "POLICY_VIOLATION",
        "BLOCKLIST",
        "SPII",
        "RECITATION",
    }
)
CODES = {
    "ABORTED",
    "ALREADY_EXISTS",
    "API_KEY_INVALID",
    "BAD_REQUEST",
    "CANCELLED",
    "DATA_LOSS",
    "DEADLINE_EXCEEDED",
    "FAILED_PRECONDITION",
    "INTERNAL",
    "INVALID_ARGUMENT",
    "INVALID_IMAGE",
    "INVALID_REQUEST",
    "NOT_FOUND",
    "OUT_OF_RANGE",
    "PAYLOAD_TOO_LARGE",
    "PERMISSION_DENIED",
    "REQUEST_TOO_LARGE",
    "RESOURCE_EXHAUSTED",
    "UNAUTHENTICATED",
    "UNAVAILABLE",
    "UNIMPLEMENTED",
    "UNKNOWN",
    "UNSUPPORTED_MEDIA_TYPE",
    "GATEWAY_TIMEOUT",
    "TOO_MANY_REQUESTS",
    "RATE_LIMIT_EXCEEDED",
    "QUOTA_EXCEEDED",
    "RATE_LIMITED",
    "SERVER_ERROR",
    *SAFETY_UPSTREAM_CODES,
}
NUMERIC_CODES = {str(code) for code in range(400, 600)}
_MAX_BODY_PARSE_CHARS = 16384
_MAX_MSG_CLASSIFY_CHARS = 4096
_MSG_IMAGE_SAFETY_RE = re.compile(r"\bimage[_\s-]?safety\b", re.I)
_MSG_PROHIBITED_RE = re.compile(r"\bprohibited[_\s-]?content\b", re.I)
_MSG_SAFETY_BLOCKED_RE = re.compile(
    r"\b(?:"
    r"safety[_\s-]?block(?:ed)?|"
    r"content[_\s-]?policy[_\s-]?violation|"
    r"policy[_\s-]?violation|"
    r"safety\s+(?:filter|policy|check|settings?|violation)|"
    r"blocked\s+(?:by|for|due\s+to)\s+(?:our\s+)?(?:safety|content|policy)|"
    r"violat(?:es|ing)\s+(?:our\s+)?(?:usage\s+|content\s+|safety\s+)?polic|"
    r"sexually\s+explicit|"
    r"nsfw|"
    r"csam|"
    r"child\s+safety"
    r")\b",
    re.I,
)
_MSG_INVALID_IMAGE_RE = re.compile(
    r"\b(?:"
    r"unable\s+to\s+process\s+input\s+image|"
    r"invalid\s+image|"
    r"corrupt(?:ed)?\s+image|"
    r"unsupported\s+(?:image|mime|media)"
    r")\b",
    re.I,
)
EXCEPTION_TYPES = {
    "TimeoutError",
    "APITimeoutError",
    "ReadTimeout",
    "WriteTimeout",
    "PoolTimeout",
    "ConnectTimeout",
    "TimeoutException",
    "ConnectError",
    "NetworkError",
    "ReadError",
    "WriteError",
    "RemoteProtocolError",
    "HTTPStatusError",
    "APIError",
    "APIConnectionError",
    "ServerError",
    "ClientError",
    "InternalServerError",
    "RateLimitError",
    "APIStatusError",
    "AuthenticationError",
    "PermissionDeniedError",
    "BadRequestError",
    "SSLError",
    "gaierror",
}
MAX_ATTEMPTS = 6


def _normalize_protocol_code(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip().replace("-", "_").upper()
    if normalized in CODES:
        return normalized
    return None


def _extract_body_code(body: object, *, http_status: int | None) -> str | None:
    """Extract only allowlisted protocol/safety codes from an API error body."""
    if isinstance(body, str):
        stripped = body.strip()
        if not stripped or len(stripped) > _MAX_BODY_PARSE_CHARS or stripped[0] not in "{[":
            return None
        try:
            body = json.loads(stripped)
        except ValueError:
            return None
    if isinstance(body, list):
        body = body[0] if len(body) == 1 and isinstance(body[0], dict) else None
    if not isinstance(body, dict):
        return None
    err = body.get("error") if isinstance(body.get("error"), dict) else body
    candidates: list[str] = []
    for details_container in (err.get("details"), body.get("details")):
        if isinstance(details_container, list):
            for item in details_container[:8]:
                if not isinstance(item, dict):
                    continue
                for field in ("reason", "code", "status"):
                    if norm := _normalize_protocol_code(item.get(field)):
                        candidates.append(norm)
    for container in (err, body):
        for field in ("reason", "status", "code"):
            if norm := _normalize_protocol_code(container.get(field)):
                candidates.append(norm)
    for cand in candidates:
        if cand in SAFETY_UPSTREAM_CODES:
            return cand
    if http_status == 400:
        msg = err.get("message")
        if isinstance(msg, str) and msg and len(msg) <= _MAX_MSG_CLASSIFY_CHARS:
            if _MSG_IMAGE_SAFETY_RE.search(msg):
                return "IMAGE_SAFETY"
            if _MSG_PROHIBITED_RE.search(msg):
                return "PROHIBITED_CONTENT"
            if _MSG_SAFETY_BLOCKED_RE.search(msg):
                return "SAFETY_BLOCKED"
            if _MSG_INVALID_IMAGE_RE.search(msg):
                return "INVALID_IMAGE"
    return candidates[0] if candidates else None


def error_facts(exc, *, expired, http_status, stream_code):
    """Classify the causal chain by type only. Never stringify an exception."""
    code = stream_code or getattr(exc, "status", None) or getattr(exc, "code", None)
    name = type(exc).__name__
    result = {"exception_type": name} if name in EXCEPTION_TYPES else {}
    body = getattr(exc, "body", None)
    if body is None and exc is not None:
        cause = getattr(exc, "__cause__", None)
        if cause is not None:
            body = getattr(cause, "body", None)
    body_code = _extract_body_code(body, http_status=http_status)
    if body_code is not None:
        code = body_code
    if isinstance(code, str):
        normalized = code.strip().replace("-", "_").upper()
        if normalized in CODES or normalized in NUMERIC_CODES:
            result["upstream_code"] = normalized
    if isinstance(http_status, int) and 400 <= http_status <= 599:
        result["http_status"] = int(http_status)
    if expired:
        return {"cause": "deadline", **result}
    if result.get("http_status"):
        return {"cause": "http", **result}
    if stream_code:
        return {"cause": "stream_error", **result}
    chain = []
    while exc is not None and len(chain) < 8 and not any(exc is item for item in chain):
        chain.append(exc)
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
    for cls, cause in (
        (socket.gaierror, "dns"),
        (ssl.SSLError, "tls"),
        (httpx.ConnectTimeout, "connect_timeout"),
        (httpx.ReadTimeout, "read_timeout"),
        (httpx.WriteTimeout, "write_timeout"),
        (httpx.PoolTimeout, "pool_timeout"),
        (APITimeoutError, "sdk_timeout"),
        (httpx.TimeoutException, "timeout"),
        (TimeoutError, "timeout"),
        (httpx.ConnectError, "connection"),
        (ConnectionError, "connection"),
        (APIConnectionError, "connection"),
        (httpx.TransportError, "transport"),
    ):
        if any(isinstance(item, cls) for item in chain):
            return {"cause": cause, **result}
    return {"cause": "unknown", **result}


def safe_attempts(attempts):
    """Enforce the same field and value allowlists at both storage boundaries."""
    clean = []
    required = {"stage": STAGES, "provider": PROVIDERS, "model_lane": LANES, "cause": CAUSES}
    if not isinstance(attempts, (list, tuple)):
        return []
    for item in attempts[-MAX_ATTEMPTS:]:
        if not isinstance(item, dict) or not all(
            isinstance(item.get(key), str) and item[key] in choices
            for key, choices in required.items()
        ):
            continue
        row = {key: item[key] for key in required}
        for key in ("elapsed_ms", "timeout_ms", "cooldown_ms"):
            if type(item.get(key)) is int and 0 <= item[key] <= 86_400_000:
                row[key] = item[key]
        if type(item.get("http_status")) is int and 400 <= item["http_status"] <= 599:
            row["http_status"] = item["http_status"]
        number, due = item.get("retry_number"), item.get("next_retry_at")
        if (
            row["model_lane"] in FLEX_MODELS
            and row.get("http_status") == 503
            and type(number) is int
            and 1 <= number <= 4
            and type(due) is int
            and 0 < due <= 2**32
        ):
            row.update(retry_number=number, next_retry_at=due)
        for key, choices in (
            ("upstream_code", CODES | NUMERIC_CODES),
            ("exception_type", EXCEPTION_TYPES),
            ("service_tier", {"standard", "priority"}),
            ("stream_phase", {"waiting", "started", "partial_output"}),
            (
                "fallback",
                {
                    "succeeded",
                    "failed",
                    "cancelled",
                    "not_dispatched",
                    "disabled",
                    "ineligible",
                    "partial_output",
                },
            ),
        ):
            if isinstance(item.get(key), str) and item[key] in choices:
                row[key] = item[key]
        clean.append(row)
    return clean


def append_attempts(existing, incoming):
    """The outer job handler may receive the same failures already recorded by its gateway."""
    old, new = safe_attempts(existing), safe_attempts(incoming)
    if not new or old[-len(new) :] == new:
        return old
    return (old + new)[-MAX_ATTEMPTS:]
