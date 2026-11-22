"""Host-owned identities and grants for authenticated social transports."""

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


def owner_binding(settings, platform: PlatformId):
    if platform == "onebot":
        if settings.onebot_enabled and settings.onebot_self_id and settings.owner_qq_id:
            return OwnerBinding(
                bot_id=settings.onebot_self_id,
                user_id=settings.owner_qq_id,
                notebook=True,
                mirror=True,
            )
        return None
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
