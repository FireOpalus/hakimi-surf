import base64
import hashlib
import hmac
import json
import re
import time
import unicodedata
import weakref
from typing import Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .bot_server import is_persona_owner
from .config import Settings
from .models import Message
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

    @property
    def audit_detail(self) -> str:
        return f"{self}_{self.detail}" if self.detail else str(self)


def configured_secrets(settings: Settings) -> list[str]:
    return [
        token.get_secret_value()
        for token in (
            settings.gemini_api_key,
            settings.encryption_key,
            settings.onebot_secret,
            settings.onebot_access_token,
            settings.onebot_ws_token,
            settings.qq_app_secret,
            settings.admin_token,
            settings.bili_sessdata,
            settings.bili_bili_jct,
            settings.bili_buvid3,
            settings.bili_buvid4,
            settings.bili_proxy,
            settings.deepseek_api_key,
        )
    ]


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


def clean_text(text: str) -> str:
    # Normalize only text, never signed bytes or identity fields.
    return "".join(
        c
        for c in unicodedata.normalize("NFKC", text)
        if c in "\n\t" or not unicodedata.category(c).startswith("C")
    ).strip()


def owner(message: Message, settings: Settings) -> bool:
    # Management, private persona and snapshots share the same verified numeric identity.
    # Legacy administrator lists and QQ OpenIDs cannot prove OWNER_QQ_ID ownership.
    return is_persona_owner(message, settings)


def authorize(message: Message, settings: Settings, *, _bot_exempt=False) -> bool:
    if message.platform == "onebot":
        enabled, bot, users, groups = (
            settings.onebot_enabled,
            settings.onebot_self_id,
            settings.onebot_allowed_users,
            settings.onebot_allowed_groups,
        )
    else:
        enabled, bot, users, groups = (
            settings.qq_enabled,
            settings.qq_app_id,
            settings.qq_allowed_users,
            settings.qq_allowed_groups,
        )
    if not enabled or message.bot_id != bot or message.user_id == bot:
        return False
    if message.platform == "onebot":
        if not re.fullmatch(r"[0-9]{1,20}", message.user_id):
            return False
        real_owner = bool(settings.owner_qq_id and message.user_id == settings.owner_qq_id)
        if not real_owner and (
            (
                message.user_id in settings.bot_qq_ids
                and not _bot_exempt
                and not bot_event_authorized(message, settings)
            )
            or message.user_id in getattr(settings, "muted_user_ids", set())
        ):
            return False
        if message.kind == "private":
            return message.conversation_id == message.user_id and (
                real_owner
                or (
                    getattr(settings, "onebot_private_mode", "owner_only") == "owner_and_allowlist"
                    and message.user_id in users
                )
            )
    if message.kind == "private":
        trusted_user = owner(message, settings) or message.user_id in users
        return trusted_user and message.conversation_id == message.user_id
    # Group admission is controlled by the group list, never by the private-user list.
    # at_only/smart_reply still decide whether this admitted group message invites a reply.
    policy = settings.group_policies.get(f"{message.platform}:{message.conversation_id}", "at_only")
    return (
        message.conversation_id in groups
        and policy != "muted"
        and (message.addressed or policy == "smart_reply")
    )


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
