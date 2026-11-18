"""Content-free facts about model call failures, shared by both admin audit views."""

from __future__ import annotations

import socket
import ssl

import httpx
from google.genai._interactions import APIConnectionError, APITimeoutError

STAGES = {"decision_message", "decision_source", "decision_output", "generation", "search"}
PROVIDERS = {"gemini_free", "gemini_paid", "vertex_paid"}
LANES = {"decision", "hydrogen", "carbon", "memory_flex"}
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
CODES = {
    "ABORTED",
    "ALREADY_EXISTS",
    "CANCELLED",
    "DATA_LOSS",
    "DEADLINE_EXCEEDED",
    "FAILED_PRECONDITION",
    "INTERNAL",
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "OUT_OF_RANGE",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "UNAUTHENTICATED",
    "UNAVAILABLE",
    "UNIMPLEMENTED",
    "UNKNOWN",
    "GATEWAY_TIMEOUT",
    "TOO_MANY_REQUESTS",
    "RATE_LIMIT_EXCEEDED",
    "QUOTA_EXCEEDED",
    "RATE_LIMITED",
    "SERVER_ERROR",
}
NUMERIC_CODES = {str(code) for code in range(400, 600)}
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


def error_facts(exc, *, expired, http_status, stream_code):
    """Classify the causal chain by type only. Never stringify an exception."""
    code = stream_code or getattr(exc, "status", None) or getattr(exc, "code", None)
    name = type(exc).__name__
    result = {"exception_type": name} if name in EXCEPTION_TYPES else {}
    body = getattr(exc, "body", None)
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        status = body["error"].get("status")
        if isinstance(status, str) and status.upper() in CODES:
            code = status
    if isinstance(code, str) and (code.upper() in CODES or code in NUMERIC_CODES):
        result["upstream_code"] = code.upper()
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
            row["model_lane"] == "memory_flex"
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
