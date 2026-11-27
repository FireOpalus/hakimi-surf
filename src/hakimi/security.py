import base64
import hashlib
import hmac
import json
import re
import time
import unicodedata
import weakref
from collections.abc import Iterable
from typing import Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
from .platform_hooks import get_platform_hooks
from .reply_style import clean_chinese_punctuation

# Server-only per-settings hooks; a copied sandbox Settings has no registration.
_BOT_AUTHORIZERS = {}


def register_bot_authorizer(settings, callback):
    key = id(settings)
    _BOT_AUTHORIZERS[key] = (
        weakref.ref(settings, lambda _: _BOT_AUTHORIZERS.pop(key, None)),
        weakref.WeakMethod(callback),
    )


def bot_event_authorized(message, settings):
    entry = _BOT_AUTHORIZERS.get(id(settings))
    callback = entry[1]() if entry and entry[0]() is settings else None
    return bool(callback and callback(message))


class Rejected(Exception):
    """Only constant reason codes belong in this exception, never input or credentials."""

    def __init__(self, code: str, detail: str = "", *, audited: bool = False):
        super().__init__(code)
        self.detail = detail
        self.audited = audited
        self.provider_diagnostics: list[dict] = []

    @property
    def audit_detail(self) -> str:
        return f"{self}_{self.detail}" if self.detail else str(self)


def set_runtime_secret_bucket(
    settings: Settings, source: str, secrets: Iterable[str]
) -> tuple[str, ...]:
    """Replace one named runtime-secret bucket without clobbering other sources."""
    bucket = str(source or "").strip()
    if not bucket:
        raise ValueError("runtime secret bucket source must be non-empty")
    cleaned = tuple(
        dict.fromkeys(val for raw in secrets if isinstance(raw, str) and (val := raw.strip()))
    )
    existing = getattr(settings, "_runtime_secret_buckets", None)
    next_buckets = dict(existing) if isinstance(existing, dict) else {}
    if cleaned:
        next_buckets[bucket] = cleaned
    else:
        next_buckets.pop(bucket, None)
    settings._runtime_secret_buckets = next_buckets
    return cleaned


def configured_secrets(settings: Settings) -> list[str]:
    fixed = [
        token.get_secret_value()
        for token in (
            settings.gemini_api_key,
            settings.gemini_free_api_key,
            settings.vertex_api_key,
            settings.encryption_key,
            settings.onebot_secret,
            settings.onebot_access_token,
            settings.onebot_ws_token,
            settings.node_ws_token,
            settings.companion_ws_token,
            settings.screen_ws_token,
            settings.qq_app_secret,
            settings.admin_token,
            settings.bili_sessdata,
            settings.bili_bili_jct,
            settings.bili_buvid3,
            settings.bili_buvid4,
            settings.bili_proxy,
            settings.deepseek_api_key,
            settings.voice_gateway_token,
            settings.colab_worker_token,
        )
    ]
    buckets = getattr(settings, "_runtime_secret_buckets", None)
    bucket_values: list[str] = []
    if isinstance(buckets, dict):
        for items in buckets.values():
            if items:
                bucket_values.extend(val for val in items if isinstance(val, str) and val)
    legacy_extra = [
        val
        for val in (getattr(settings, "_runtime_extra_secrets", ()) or ())
        if isinstance(val, str) and val
    ]
    seen = {val for val in fixed if val}
    extras: list[str] = []
    for val in (*bucket_values, *legacy_extra):
        if val not in seen:
            seen.add(val)
            extras.append(val)
    return [*fixed, *extras]


class Vault:
    def __init__(self, key: str):
        self.cipher = Fernet(key.encode("ascii"))
        self.index_key = hmac.digest(base64.urlsafe_b64decode(key), b"hakimi-index-v1", "sha256")

    def digest(self, *parts: Any) -> str:
        raw = json.dumps(parts, ensure_ascii=True, separators=(",", ":")).encode()
        return hmac.new(self.index_key, raw, hashlib.sha256).hexdigest()

    def seal(self, context: str, data: Any) -> bytes:
        raw = json.dumps({"context": context, "data": data}, ensure_ascii=False).encode()
        return self.cipher.encrypt(raw)

    def open(self, context: str, token: bytes) -> Any:
        obj = json.loads(self.cipher.decrypt(token))
        if not hmac.compare_digest(obj["context"], context):
            raise Rejected("ciphertext_context_mismatch")
        return obj["data"]


def check_fresh(timestamp: float, max_age: int, now: float | None = None) -> None:
    now = time.time() if now is None else now
    if not (now - max_age <= timestamp <= now + 30):
        raise Rejected("stale_timestamp")


def verify_onebot(body: bytes, signature: str, secret: str) -> None:
    expected = "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise Rejected("invalid_signature")


def qq_signing_key(secret: str) -> Ed25519PrivateKey:
    seed = secret.encode("utf-8")
    if not seed:
        raise Rejected("missing_secret")
    seed = (seed * ((32 // len(seed)) + 1))[:32]
    return Ed25519PrivateKey.from_private_bytes(seed)


def verify_qq(body: bytes, timestamp: str, signature: str, secret: str, max_age: int) -> None:
    try:
        if not re.fullmatch(r"[0-9]{10,11}", timestamp):
            raise ValueError
        check_fresh(int(timestamp), max_age)
        qq_signing_key(secret).public_key().verify(
            bytes.fromhex(signature), timestamp.encode("ascii") + body
        )
    except Exception as exc:
        raise Rejected("invalid_signature") from exc


CREDENTIAL_RE = re.compile(
    r"(?i)(?:(?:api[_ -]?key|password|passwd|token|secret|sessdata|bili_jct|密钥|密码)"
    r"\s*[:=：]\s*\S{6,}|bearer\s+\S+|\bsk-[A-Za-z0-9_-]{16,}|\bAIza[0-9A-Za-z_-]{20,}"
    r"|-----BEGIN [^-]+PRIVATE KEY-----)"
)
ROLE_TAG_RE = re.compile(r"\[role=", re.IGNORECASE)


def clean_text(text: str) -> str:
    # Normalize only text, never signed bytes or identity fields.
    return "".join(
        c
        for c in unicodedata.normalize("NFKC", text)
        if c in "\n\t" or not unicodedata.category(c).startswith("C")
    ).strip()


def redact_secrets(
    text: Any,
    settings: Settings | None = None,
    *,
    replacement: str = "[凭证已隐去]",
    min_secret_len: int = 6,
    neutralize_role_tags: bool = False,
) -> str:
    """Scrub configured secrets and common credential patterns from arbitrary text."""
    cleaned = str(text or "")
    if settings is not None:
        candidates = {
            val
            for secret in configured_secrets(settings)
            if secret and len(secret) >= min_secret_len
            for val in (secret, clean_text(secret))
            if len(val) >= min_secret_len
        }
        for secret in sorted(candidates, key=len, reverse=True):
            if secret in cleaned:
                cleaned = cleaned.replace(secret, replacement)
    cleaned = CREDENTIAL_RE.sub(replacement, cleaned)
    if neutralize_role_tags:
        cleaned = ROLE_TAG_RE.sub("[untrusted_role=", cleaned)
    return cleaned


def owner(message: Message, settings: Settings) -> bool:
    # Privileged QQ commands require the verified numeric OneBot identity.
    # Legacy administrator lists and QQ OpenIDs cannot prove OWNER_QQ_ID ownership.
    return message.platform == "onebot" and is_persona_owner(message, settings)


def authorize(message: Message, settings: Settings, *, _bot_exempt=False) -> bool:
    from .social import chat_registry

    registry = chat_registry(settings)
    if registry is not None:
        module = registry.chat_platforms.get(message.platform)
        if (
            not module
            or module.inactive_reason
            or message.user_id == message.bot_id
            or (message.kind == "private" and message.conversation_id != message.user_id)
        ):
            return False
        return module.authorize_chat(message, settings, bot_exempt=_bot_exempt)
    # Detached settings (including the isolated admin sandbox) use registered platform hooks.
    if message.user_id == message.bot_id or (
        message.kind == "private" and message.conversation_id != message.user_id
    ):
        return False
    hooks = get_platform_hooks(message.platform)
    if hooks is None:
        return False
    return bool(hooks.authorize(message, settings, _bot_exempt=_bot_exempt))


def safe_output(text: str, secrets: list[str], limit: int = 1800) -> str:
    text = clean_text(text)
    if not text or any(s and len(s) >= 8 and s in text for s in secrets):
        raise Rejected("invalid_output")
    # Text-only transport plus neutralization: model text cannot become CQ/QQ tags.
    text = (
        text.replace("[CQ:", "［CQ:")
        .replace("@everyone", "＠everyone")
        .replace("@all", "＠all")
        .replace("@全体成员", "＠全体成员")
    )
    text = clean_chinese_punctuation(text)
    if len(text) <= limit:
        return text
    excerpt = text[: max(0, limit - 1)]
    boundary = max(excerpt.rfind(mark) for mark in ("\n", "。", "！", "？", "!", "?"))
    if boundary >= 0:
        excerpt = excerpt[: boundary + 1]
    return excerpt.rstrip() + "…"
