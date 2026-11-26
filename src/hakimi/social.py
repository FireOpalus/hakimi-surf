"""Host-owned identities and grants for authenticated social transports."""

import hmac
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import Message, PlatformId
from .platform_hooks import get_platform_hooks


class OwnerBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bot_id: Annotated[str, Field(min_length=1, max_length=64)]
    user_id: Annotated[str, Field(min_length=1, max_length=128)]
    notebook: bool = False
    mirror: bool = False

    @model_validator(mode="after")
    def distinct_recipient(self):
        if self.bot_id == self.user_id:
            raise ValueError("An owner binding cannot point to the bot itself")
        return self


def chat_registry(settings):
    ref = getattr(settings, "_chat_registry", None)
    return ref() if ref else None


def chat_module(settings, platform):
    registry = chat_registry(settings)
    module = registry.chat_platforms.get(platform) if registry else None
    return module if module and not module.inactive_reason else None


def permits_chat_export(message, settings, consumer):
    from .security import authorize

    if not authorize(message.model_copy(update={"addressed": True}), settings):
        return False
    if chat_registry(settings) is None:
        # Standalone callers without a host registry resolve via kernel platform hooks.
        hooks = get_platform_hooks(message.platform)
        return bool(hooks and hooks.permits_export(message, consumer))
    module = chat_module(settings, message.platform)
    return bool(module and module.permits_chat_export(message, consumer))


def _configured_onebot_owner_binding(settings) -> OwnerBinding | None:
    if settings.onebot_enabled and settings.onebot_self_id and settings.owner_qq_id:
        return OwnerBinding(
            bot_id=settings.onebot_self_id,
            user_id=settings.owner_qq_id,
            notebook=True,
            mirror=True,
        )
    return None


def owner_binding(settings, platform: PlatformId):
    if platform == "onebot":
        if chat_registry(settings) is not None and not chat_module(settings, "onebot"):
            return None
        return _configured_onebot_owner_binding(settings)
    # Official QQ open IDs cannot establish ownership of the configured OneBot account.
    if platform == "qq" or not chat_module(settings, platform):
        return None
    return settings.social_owner_bindings.get(platform)


def bound_owner(message, settings, grant: Literal["notebook", "mirror"] | None = None):
    if message is None:
        return False
    binding = owner_binding(settings, message.platform)
    if binding is None or message.bot_id != binding.bot_id or message.user_id != binding.user_id:
        return False
    if (
        grant
        and chat_registry(settings) is not None
        and not chat_module(settings, message.platform)
    ):
        return False
    if message.kind == "private":
        return message.conversation_id == binding.user_id and (
            grant is None or getattr(binding, grant)
        )
    return grant is None


def owner_target(settings, platform, now, event_id):
    binding = owner_binding(settings, platform)
    if binding is None:
        return None
    return Message(
        platform=platform,
        bot_id=binding.bot_id,
        user_id=binding.user_id,
        conversation_id=binding.user_id,
        kind="private",
        text="",
        addressed=True,
        event_id=event_id,
        created_at=now,
    )


def confirmed_message_id(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        from .security import Rejected

        raise Rejected("chat_send_unconfirmed")
    return value


def _resolve_owner_binding(
    settings, platform: str, *, include_inactive: bool = False
) -> OwnerBinding | None:
    if platform == "onebot":
        if include_inactive or chat_registry(settings) is None:
            return _configured_onebot_owner_binding(settings)
        return owner_binding(settings, "onebot")
    if platform == "qq":
        return None
    if include_inactive or chat_registry(settings) is None:
        bindings = getattr(settings, "social_owner_bindings", None) or {}
        return bindings.get(platform)
    return owner_binding(settings, platform)


def owner_dm_digest(settings, platform: str, bot_id: str, user_id: str) -> str:
    from .security import Vault

    raw_key = settings.encryption_key
    secret_key = (
        raw_key.get_secret_value() if hasattr(raw_key, "get_secret_value") else str(raw_key)
    )
    identity = (
        (settings.namespace, bot_id, user_id)
        if platform == "onebot"
        else (settings.namespace, platform, bot_id, user_id)
    )
    return Vault(secret_key).digest("owner-dm", *identity)


def owner_dm_scope(settings, platform: PlatformId, *, include_inactive: bool = False) -> str | None:
    binding = _resolve_owner_binding(settings, platform, include_inactive=include_inactive)
    if binding is None:
        return None
    return owner_dm_digest(settings, platform, binding.bot_id, binding.user_id)


def owner_target_scope(
    settings, platform: PlatformId, *, include_inactive: bool = False
) -> str | None:
    binding = _resolve_owner_binding(settings, platform, include_inactive=include_inactive)
    if binding is None:
        return None
    if platform == "onebot":
        return f"owner:{binding.user_id}"
    return f"boundowner:{owner_dm_digest(settings, platform, binding.bot_id, binding.user_id)}"


def resolve_owner_scope(
    scope: str, settings, *, include_inactive: bool = False
) -> tuple[PlatformId, OwnerBinding] | None:
    if not isinstance(scope, str) or not scope:
        return None
    try:
        scope_bytes = scope.encode("utf-8")
    except UnicodeEncodeError:
        return None
    if scope.startswith("owner:"):
        binding = _resolve_owner_binding(settings, "onebot", include_inactive=include_inactive)
        if binding is None:
            return None
        expected = f"owner:{binding.user_id}"
        if hmac.compare_digest(scope_bytes, expected.encode("utf-8")):
            return "onebot", binding
        return None
    if scope.startswith("boundowner:"):
        bindings = getattr(settings, "social_owner_bindings", None) or {}
        for platform in bindings:
            if platform in {"onebot", "qq"}:
                continue
            binding = _resolve_owner_binding(settings, platform, include_inactive=include_inactive)
            if binding is None:
                continue
            expected = (
                f"boundowner:{owner_dm_digest(settings, platform, binding.bot_id, binding.user_id)}"
            )
            if hmac.compare_digest(scope_bytes, expected.encode("utf-8")):
                return platform, binding
        return None
    return None


def is_verified_owner_scope(scope: str, settings, *, include_inactive: bool = False) -> bool:
    return resolve_owner_scope(scope, settings, include_inactive=include_inactive) is not None


class RecipientResolver:
    """Canonical kernel target_scope resolver for Owner DMs and <platform>:<id> groups."""

    def __init__(self, settings, registry=None) -> None:
        self.settings = settings
        self._registry = registry

    @property
    def registry(self):
        return self._registry if self._registry is not None else chat_registry(self.settings)

    def resolve_owner(
        self, scope: str, *, include_inactive: bool = False
    ) -> tuple[PlatformId, OwnerBinding, str] | None:
        raw = str(scope or "").strip()
        if not raw:
            return None
        if raw in {"owner", "owner_dm"}:
            platform = getattr(self.settings, "owner_delivery_platform", "onebot") or "onebot"
            binding = _resolve_owner_binding(
                self.settings, platform, include_inactive=include_inactive
            )
            if binding is None:
                return None
            if platform == "onebot":
                canonical = f"owner:{binding.user_id}"
            else:
                digest = owner_dm_digest(self.settings, platform, binding.bot_id, binding.user_id)
                canonical = f"boundowner:{digest}"
            return platform, binding, canonical
        if raw.startswith("private:"):
            uid = raw[len("private:") :].strip()
            binding = _resolve_owner_binding(
                self.settings, "onebot", include_inactive=include_inactive
            )
            if binding is not None:
                try:
                    uid_bytes = uid.encode("utf-8")
                except UnicodeEncodeError:
                    return None
                if hmac.compare_digest(uid_bytes, binding.user_id.encode("utf-8")):
                    return "onebot", binding, f"owner:{binding.user_id}"
            return None
        matched = resolve_owner_scope(raw, self.settings, include_inactive=include_inactive)
        if matched is None:
            return None
        platform, binding = matched
        return platform, binding, raw

    def is_owner_scope(self, scope: str, *, include_inactive: bool = False) -> bool:
        return self.resolve_owner(scope, include_inactive=include_inactive) is not None

    def normalize(self, raw_scope: str) -> str | None:
        raw = str(raw_scope or "").strip()
        if not raw or raw.startswith("channel:"):
            return None
        reg = self.registry
        owner_hit = self.resolve_owner(raw)
        if owner_hit is not None:
            platform, _, canonical = owner_hit
            if reg is None:
                return canonical
            if platform == "onebot":
                return reg.normalize_recipient(canonical)
            mod = reg.chat_platforms.get(platform)
            if mod is not None and mod.inactive_reason is None:
                return canonical
            proactive = getattr(reg, "owner_delivery", None)
            if proactive is not None and proactive.external_scope(canonical):
                return canonical
            return None
        if raw in {"owner", "owner_dm"} or raw.startswith(("owner:", "boundowner:", "private:")):
            return None
        return reg.normalize_recipient(raw) if reg is not None else None
