"""Recipient validation shared by planning, admission and final delivery."""

from ..config import Settings
from ..models import Message


def _allowlisted_groups(settings: Settings) -> list[str]:
    return sorted(
        gid
        for gid in settings.onebot_allowed_groups
        if settings.group_policies.get(f"onebot:{gid}", "at_only") != "muted"
    )


def _normalize_target_scope(raw_scope: str, settings: Settings) -> str | None:
    scope = str(raw_scope or "").strip()
    if not scope:
        return None
    owner_aliases = {"owner", "owner_dm"}
    if settings.owner_qq_id:
        owner_aliases.update(
            {
                f"owner:{settings.owner_qq_id}",
                f"private:{settings.owner_qq_id}",
            }
        )
    if scope in owner_aliases:
        return (
            f"owner:{settings.owner_qq_id}"
            if (settings.onebot_enabled and settings.owner_qq_id)
            else None
        )
    gid = scope.split(":", 1)[1].strip() if scope.startswith(("onebot:", "group:")) else scope
    if (
        settings.onebot_enabled
        and gid in settings.onebot_allowed_groups
        and settings.group_policies.get(f"onebot:{gid}", "at_only") != "muted"
    ):
        return f"onebot:{gid}"
    return None


def _scope_to_message(
    target_scope: str, settings: Settings, *, event_id: str, created_at: float
) -> Message | None:
    if target_scope.startswith("owner:"):
        uid = target_scope.split(":", 1)[1]
        if not settings.onebot_enabled or not settings.owner_qq_id or uid != settings.owner_qq_id:
            return None
        return Message(
            platform="onebot",
            bot_id=settings.onebot_self_id,
            event_id=event_id,
            user_id=settings.owner_qq_id,
            conversation_id=settings.owner_qq_id,
            kind="private",
            text="",
            addressed=True,
            created_at=created_at,
        )
    if target_scope.startswith("onebot:"):
        gid = target_scope.split(":", 1)[1]
        if (
            not settings.onebot_enabled
            or gid not in settings.onebot_allowed_groups
            or settings.group_policies.get(f"onebot:{gid}", "at_only") == "muted"
        ):
            return None
        return Message(
            platform="onebot",
            bot_id=settings.onebot_self_id,
            event_id=event_id,
            user_id="0",
            conversation_id=gid,
            kind="group",
            text="",
            addressed=True,
            created_at=created_at,
        )
    return None


def allowed_proactive_message(message: Message, settings: Settings) -> bool:
    if message.platform != "onebot":
        return False
    if message.kind == "private":
        return bool(
            settings.owner_qq_id
            and message.user_id == message.conversation_id == settings.owner_qq_id
        )
    return (
        message.conversation_id in settings.onebot_allowed_groups
        and settings.group_policies.get("onebot:" + message.conversation_id, "at_only") != "muted"
    )
