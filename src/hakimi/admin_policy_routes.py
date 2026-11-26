"""Authenticated policy, limits, routing-model, search, filter, group, admission, and bot routes."""

import time
from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .admin_common import body, domain_lock, invalid_input_http_error, partial_body, service
from .admin_dashboard_routes import Input
from .admission_policy import (
    AdmissionConfig,
    _validate_threshold_number,
    effective_bot_reply_threshold,
    is_bot_hard_blocked,
)
from .budget_policy import BudgetConfig
from .debounce_policy import DebounceConfig
from .gateway import FilterConfig
from .models import Message, PlatformId
from .policy_registry import SIMPLE_LIMIT_SPECS, PolicySpec, export_policy_schemas
from .rest_policy import RestHoursConfig
from .security import Rejected
from .social import chat_module
from .social_chat_config import (
    RemoteChatConfig,
    cached_remote_chat_config,
    delete_remote_chat_config,
    load_all_remote_chat_configs,
    load_remote_chat_config,
    save_remote_chat_config,
    validate_remote_chat_platform,
)
from .thinking_policy import ModelSettingsUpdate, ThinkingConfig
from .web_search import WebSearchConfig

router = APIRouter(prefix="/admin/api")


class GroupPolicy(Input):
    platform: PlatformId = "onebot"
    group_id: str = Field(min_length=1, max_length=128)
    mode: Literal["at_only", "smart_reply", "muted"]


class GroupActivity(Input):
    platform: PlatformId = "onebot"
    group_id: str = Field(min_length=1, max_length=128)
    preset: Literal["quiet", "natural", "active"] | None = None


class BotChange(Input):
    user_id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")
    action: Literal["add", "remove", "threshold"]
    threshold: float | None = Field(default=None, ge=0.01, le=1.00)

    @field_validator("threshold", mode="before")
    @classmethod
    def valid_threshold(cls, value):
        if value is None:
            return None
        return _validate_threshold_number(value)


class BotThresholdConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    bot_reply_threshold: float = Field(ge=0.01, le=1.00)

    @field_validator("bot_reply_threshold", mode="before")
    @classmethod
    def valid_threshold(cls, value):
        return _validate_threshold_number(value)


class AdmissionChange(Input):
    list: Literal["allowed_groups", "allowed_users", "ignored_bot_ids"]
    action: Literal["add", "remove"]
    id: str = Field(pattern=r"^[1-9][0-9]{0,19}$")


class AccountBalanceBody(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    balance_usd: Decimal = Field(ge=0, le=Decimal("100000"))

    @field_validator("balance_usd", mode="before")
    @classmethod
    def reject_bool(cls, value):
        if isinstance(value, bool):
            raise ValueError("bool is not a valid balance")
        return value


class DeepSeekCredentialsBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    api_key: str = Field(default="", max_length=512)
    base_url: str = Field(default="", max_length=256)
    model: str = Field(default="", max_length=80)
    clear: bool = False


@router.get("/budget")
async def budget_config(request: Request):
    return await service(request).store.budget_config_status()


@router.put("/budget")
async def budget_save(request: Request):
    svc = service(request)
    async with domain_lock(request, "budget"):
        current = BudgetConfig.from_settings(svc.s).model_dump(mode="json")
        value = await partial_body(request, current, BudgetConfig)
        return await svc.store.set_budget_config(value)


@router.delete("/budget")
async def budget_reset(request: Request):
    async with domain_lock(request, "budget"):
        return await service(request).store.set_budget_config(None)


@router.get("/account-balance")
async def account_balance_status(request: Request):
    return await service(request).store.account_balance_status()


@router.put("/account-balance")
async def account_balance_save(request: Request):
    value = await body(request, AccountBalanceBody)
    async with domain_lock(request, "budget"):
        return await service(request).store.set_account_balance(value.balance_usd)


@router.delete("/account-balance")
async def account_balance_clear(request: Request):
    async with domain_lock(request, "budget"):
        return await service(request).store.clear_account_balance()


@router.get("/policies/schema")
async def policies_schema(request: Request):
    return export_policy_schemas(service(request).store.s)


def _mount_simple_limit_routes(spec: PolicySpec) -> None:
    async def get_status(request: Request):
        return service(request).store.runtime.policy_status(spec.key)

    async def save_policy(request: Request):
        value = await body(request, spec.model)
        async with domain_lock(request, spec.lock):
            runtime = service(request).store.runtime
            if spec.partial_update:
                current = getattr(runtime, spec.attr)
                merged = current.model_dump() | value.model_dump(exclude_unset=True)
                value = spec.model.model_validate(merged)
            return await runtime.set_policy(spec.key, value)

    async def reset_policy(request: Request):
        async with domain_lock(request, spec.lock):
            return await service(request).store.runtime.set_policy(spec.key, None)

    get_status.__name__ = f"{spec.key}_status"
    save_policy.__name__ = f"{spec.key}_save"
    reset_policy.__name__ = f"{spec.key}_reset"
    globals()[get_status.__name__] = get_status
    globals()[save_policy.__name__] = save_policy
    globals()[reset_policy.__name__] = reset_policy
    router.get(spec.route)(get_status)
    router.put(spec.route)(save_policy)
    router.delete(spec.route)(reset_policy)


for _spec in SIMPLE_LIMIT_SPECS:
    _mount_simple_limit_routes(_spec)


@router.get("/rest-hours")
@router.get("/life/rest-hours")
async def rest_hours_status(request: Request):
    return service(request).store.runtime.rest_hours_status()


@router.put("/rest-hours")
@router.put("/life/rest-hours")
async def rest_hours_save(request: Request):
    value = await body(request, RestHoursConfig)
    svc = service(request)
    async with domain_lock(request, "rest_hours"):
        if "day_rest_enabled" not in value.model_fields_set:
            value = value.model_copy(
                update={"day_rest_enabled": svc.store.runtime.rest_hours.day_rest_enabled}
            )
        return await svc.life_engine.set_rest_policy(value)


@router.delete("/rest-hours")
@router.delete("/life/rest-hours")
async def rest_hours_reset(request: Request):
    svc = service(request)
    async with domain_lock(request, "rest_hours"):
        return await svc.life_engine.set_rest_policy(None)


@router.get("/filter")
async def filter_status(request: Request):
    return await service(request).store.runtime.filters.status()


@router.get("/models")
async def model_settings(request: Request):
    svc = service(request)
    return svc.store.runtime.model_settings_status(svc.modules)


@router.put("/models")
@router.delete("/models")
async def save_model_settings(request: Request):
    svc = service(request)
    update = await body(request, ModelSettingsUpdate) if request.method == "PUT" else None
    async with domain_lock(request, "models"):
        try:
            return await svc.store.runtime.set_model_settings(svc.modules, update)
        except ValueError:
            raise HTTPException(400, "invalid_model_settings") from None


@router.get("/thinking")
async def thinking_status(request: Request):
    return service(request).store.runtime.thinking_status()


@router.get("/debounce")
async def debounce_status(request: Request):
    return service(request).store.runtime.debounce_status()


@router.get("/web-search")
async def web_search_status(request: Request):
    return await service(request).store.runtime.web_search.status()


@router.put("/web-search")
async def web_search_save(request: Request):
    value = await body(request, WebSearchConfig)
    async with domain_lock(request, "web_search"):
        return await service(request).store.runtime.web_search.save(value)


@router.delete("/web-search")
async def web_search_reset(request: Request):
    async with domain_lock(request, "web_search"):
        return await service(request).store.runtime.web_search.save(None)


@router.post("/web-search/deepseek")
async def web_search_deepseek_save(request: Request):
    value = await body(request, DeepSeekCredentialsBody)
    svc = service(request)
    async with domain_lock(request, "web_search"):
        result = await svc.store.runtime.web_search.save_deepseek(
            api_key=value.api_key,
            base_url=value.base_url,
            model=value.model,
            clear=value.clear,
        )
        svc.refresh_secrets()
        return result


@router.put("/debounce")
async def debounce_save(request: Request):
    value = await body(request, DebounceConfig)
    async with domain_lock(request, "debounce"):
        return await service(request).store.runtime.set_debounce(value)


@router.delete("/debounce")
async def debounce_reset(request: Request):
    async with domain_lock(request, "debounce"):
        return await service(request).store.runtime.set_debounce(None)


@router.put("/thinking")
async def thinking_save(request: Request):
    value = await body(request, ThinkingConfig)
    async with domain_lock(request, "models"):
        return await service(request).store.runtime.set_thinking(value)


@router.delete("/thinking")
async def thinking_reset(request: Request):
    async with domain_lock(request, "models"):
        return await service(request).store.runtime.set_thinking(None)


def _plugin_bus(svc: Any) -> Any:
    bus = getattr(svc, "plugin_bus", None)
    if bus is not None:
        return bus
    modules = getattr(svc, "modules", None)
    systems = getattr(modules, "systems", None) or {}
    sys_mod = systems.get("plugin_bus")
    return getattr(sys_mod, "plugin_bus", None) if sys_mod is not None else None


def _validate_social_platform_param(platform: str) -> str:
    try:
        return validate_remote_chat_platform(platform)
    except Rejected:
        raise HTTPException(400, "invalid_platform") from None


def _remote_allowed_groups_sync(
    settings: Any, platform: str, *, remote_config: RemoteChatConfig | None = None
) -> set[str]:
    cfg = (
        remote_config
        if remote_config is not None
        else cached_remote_chat_config(settings, platform)
    )
    if cfg is not None:
        return set(cfg.allowed_groups)
    mod = chat_module(settings, platform)
    if mod is not None and hasattr(mod, "allowed_groups"):
        return set(mod.allowed_groups())
    return set()


def allowed_activity_group(
    settings, platform, group_id, *, remote_config: RemoteChatConfig | None = None
):
    if platform == "onebot":
        allowed = settings.onebot_allowed_groups
    elif platform == "qq":
        allowed = settings.qq_allowed_groups
    else:
        allowed = _remote_allowed_groups_sync(settings, platform, remote_config=remote_config)
    if group_id not in allowed:
        raise Rejected("group_not_allowlisted")


async def _social_chat_config_payload(
    svc: Any,
    platform: str,
    *,
    saved: RemoteChatConfig | None = None,
    saved_loaded: bool = False,
) -> dict[str, Any]:
    bus = _plugin_bus(svc)
    proxy = bus.get_chat_proxy(platform) if bus is not None else None
    if proxy is None:
        modules = getattr(svc, "modules", None)
        chat_plats = getattr(modules, "chat_platforms", None) or {}
        candidate = chat_plats.get(platform)
        if candidate is not None and platform not in {"onebot", "qq"}:
            proxy = candidate
    online = False
    if proxy is not None:
        if bus is not None and hasattr(bus, "is_plugin_online"):
            online = bool(bus.is_plugin_online(proxy.module_id, require_enabled=False))
        elif hasattr(proxy, "admin_status"):
            online = bool(proxy.admin_status().get("online", False))
        elif hasattr(proxy, "chat_connected"):
            online = bool(proxy.chat_connected(platform))

    plugin_id = str(getattr(proxy, "module_id", "") or platform)
    status = proxy.status_dict() if proxy is not None and hasattr(proxy, "status_dict") else {}
    binding = (getattr(svc.s, "social_owner_bindings", None) or {}).get(platform)
    bot_id = str(status.get("bot_id") or "").strip() or (
        binding.bot_id if binding is not None else ""
    )
    bot_username = str(status.get("bot_username") or "").strip()
    owner_configured = bool(status.get("owner_configured") or binding is not None)
    caps = list(getattr(proxy, "capabilities", ()) or ()) if proxy is not None else []
    if not caps and isinstance(status.get("capabilities"), list):
        caps = [c for c in status["capabilities"] if isinstance(c, str)]

    effective_saved = (
        saved
        if saved_loaded
        else await load_remote_chat_config(svc.store, platform, settings=svc.s)
    )
    env_raw = status.get("env_defaults")
    if isinstance(env_raw, dict):
        env_cfg = RemoteChatConfig.from_untrusted(env_raw, platform=platform)
    elif proxy is not None and effective_saved is None:
        env_cfg = RemoteChatConfig.from_untrusted(status, platform=platform)
    else:
        env_cfg = RemoteChatConfig()

    if effective_saved is not None:
        active_cfg = effective_saved
        source = "admin"
    else:
        active_cfg = env_cfg
        source = "env"

    return {
        "platform": platform,
        "plugin_id": plugin_id,
        "online": online,
        "bot_id": bot_id,
        "bot_username": bot_username,
        "owner_configured": owner_configured,
        "source": source,
        "capabilities": caps,
        "config": active_cfg.to_dict(),
        "environment": env_cfg.to_dict(),
    }


@router.get("/social-chat/{platform}/config")
async def social_chat_config_get(request: Request, platform: str):
    plat = _validate_social_platform_param(platform)
    svc = service(request)
    return await _social_chat_config_payload(svc, plat)


@router.put("/social-chat/{platform}/config")
async def social_chat_config_save(request: Request, platform: str):
    plat = _validate_social_platform_param(platform)
    value = await body(request, RemoteChatConfig)
    svc = service(request)
    async with domain_lock(request, "groups"):
        saved = await save_remote_chat_config(svc.store, plat, value, settings=svc.s)
        bus = _plugin_bus(svc)
        pushed = False
        if bus is not None and hasattr(bus, "push_remote_chat_config"):
            pushed = await bus.push_remote_chat_config(plat, saved)
        else:
            mod = chat_module(svc.s, plat)
            if mod is not None and hasattr(mod, "push_config"):
                pushed = await mod.push_config(saved, platform=plat)
        payload = await _social_chat_config_payload(svc, plat, saved=saved, saved_loaded=True)
        return {**payload, "pushed": bool(pushed)}


@router.delete("/social-chat/{platform}/config")
async def social_chat_config_reset(request: Request, platform: str):
    plat = _validate_social_platform_param(platform)
    svc = service(request)
    async with domain_lock(request, "groups"):
        await delete_remote_chat_config(svc.store, plat, settings=svc.s)
        bus = _plugin_bus(svc)
        pushed = False
        if bus is not None and hasattr(bus, "push_remote_chat_config"):
            pushed = await bus.push_remote_chat_config(plat, None, reset_to_env=True)
        else:
            mod = chat_module(svc.s, plat)
            if mod is not None and hasattr(mod, "push_config"):
                pushed = await mod.push_config(None, platform=plat, reset_to_env=True)
        payload = await _social_chat_config_payload(svc, plat, saved=None, saved_loaded=True)
        return {**payload, "pushed": bool(pushed)}


@router.put("/filter")
async def filter_save(request: Request):
    value = await body(request, FilterConfig)
    svc = service(request)
    manager = svc.store.runtime.filters
    async with domain_lock(request, "filter"):
        if "group_activity" not in value.model_fields_set:
            value = value.model_copy(update={"group_activity": dict(manager.config.group_activity)})
        else:
            loaded_remote: dict[str, RemoteChatConfig | None] = {}
            for key in value.group_activity:
                platform, group_id = key.split(":", 1)
                remote_cfg = None
                if platform not in {"onebot", "qq"}:
                    if platform not in loaded_remote:
                        loaded_remote[platform] = await load_remote_chat_config(
                            svc.store, platform, settings=svc.s
                        )
                    remote_cfg = loaded_remote[platform]
                allowed_activity_group(svc.s, platform, group_id, remote_config=remote_cfg)
        return await manager.set_config(value)


@router.delete("/filter")
async def filter_reset(request: Request):
    async with domain_lock(request, "filter"):
        return await service(request).store.runtime.filters.set_config(None)


@router.post("/filter/group-activity")
async def group_activity(request: Request):
    value = await body(request, GroupActivity)
    svc = service(request)
    remote_cfg = (
        await load_remote_chat_config(svc.store, value.platform, settings=svc.s)
        if value.platform not in {"onebot", "qq"}
        else None
    )
    allowed_activity_group(svc.s, value.platform, value.group_id, remote_config=remote_cfg)
    async with domain_lock(request, "filter"):
        return await svc.store.runtime.filters.set_group_activity(
            value.platform, value.group_id, value.preset
        )


@router.get("/decision-audit")
async def decision_audit(request: Request):
    return await service(request).store.runtime.filters.recent_audit(limit=30)


@router.get("/groups")
async def groups(request: Request):
    svc = service(request)
    s = svc.s
    platform_groups: list[tuple[str, set[str]]] = [
        ("onebot", set(s.onebot_allowed_groups)),
        ("qq", set(s.qq_allowed_groups)),
    ]
    persisted = await load_all_remote_chat_configs(svc.store, settings=s)
    remote_platforms: set[str] = set(persisted)
    modules = getattr(svc, "modules", None)
    chat_plats = getattr(modules, "chat_platforms", None) or {}
    for plat in chat_plats:
        if plat not in {"onebot", "qq"}:
            remote_platforms.add(plat)
    for plat in sorted(remote_platforms):
        if plat in persisted:
            gids = set(persisted[plat].allowed_groups)
        else:
            mod = chat_plats.get(plat)
            gids = (
                set(mod.allowed_groups())
                if mod is not None and hasattr(mod, "allowed_groups")
                else set()
            )
        platform_groups.append((plat, gids))
    return [
        {
            "platform": platform,
            "group_id": gid,
            "mode": s.group_policies.get(f"{platform}:{gid}", "at_only"),
        }
        for platform, ids in platform_groups
        for gid in sorted(ids)
    ]


@router.get("/admission")
async def admission(request: Request):
    svc = service(request)
    state = svc.store.runtime
    return {
        "config": state.admission.model_dump(mode="json"),
        "environment": state.env_admission.model_dump(mode="json"),
        "source": "admin" if state.admission_overridden else "env",
        "owner_qq_id": svc.s.owner_qq_id,
        "onebot_self_id": svc.s.onebot_self_id,
        # Effective policy; the legacy environment flag no longer restricts group members.
        "allow_group_members": True,
        "group_membership_policy": "all_non_blocked_members",
        "all_groups_allowed": False,
    }


@router.put("/admission")
async def save_admission(request: Request):
    value = await body(request, AdmissionConfig)
    async with domain_lock(request, "admission"):
        await service(request).store.runtime.set_admission(value)
        return await admission(request)


@router.post("/admission/change")
async def change_admission(request: Request):
    data = await body(request, AdmissionChange)
    async with domain_lock(request, "admission"):
        try:
            await service(request).store.runtime.change_admission(data.list, data.action, data.id)
        except ValidationError as exc:
            raise invalid_input_http_error(exc) from None
        return await admission(request)


@router.delete("/admission")
async def reset_admission(request: Request):
    async with domain_lock(request, "admission"):
        await service(request).store.runtime.set_admission(None)
        return await admission(request)


@router.put("/groups")
async def policy(request: Request):
    data = await body(request, GroupPolicy)
    svc = service(request)
    async with domain_lock(request, "groups"):
        remote_cfg = (
            await load_remote_chat_config(svc.store, data.platform, settings=svc.s)
            if data.platform not in {"onebot", "qq"}
            else None
        )
        allowed_activity_group(svc.s, data.platform, data.group_id, remote_config=remote_cfg)
        key = f"{data.platform}:{data.group_id}"
        await svc.store.runtime.save_config("policy:" + key, data.mode)
        svc.s.group_policies[key] = data.mode
        return {"status": "saved"}


def _bot_thresholds_status(svc):
    state = svc.store.runtime
    return {
        "bot_reply_threshold": state.admission.bot_reply_threshold,
        "environment_bot_reply_threshold": state.env_admission.bot_reply_threshold,
        "bot_reply_thresholds": dict(state.admission.bot_reply_thresholds),
        "source": "admin" if state.admission_overridden else "env",
    }


@router.get("/bots")
async def bots(request: Request):
    svc = service(request)
    result = []
    now = time.time()
    async with svc.store.lock:
        rows = await svc.store.all(
            "SELECT scope,target,used,expires,permit_until FROM bot_dialogue_windows "
            "WHERE expires>? AND authorized_by=?",
            (now, svc.bot_guard.owner_key()),
        )
    for uid in sorted(svc.s.bot_qq_ids):
        item = {
            "user_id": uid,
            "custom_threshold": svc.s.bot_reply_thresholds.get(uid),
            "effective_threshold": effective_bot_reply_threshold(uid, svc.s),
            "hard_blocked": is_bot_hard_blocked(uid, svc.s),
            "groups": [],
        }
        for group in svc.s.onebot_allowed_groups:
            m = Message(
                platform="onebot",
                bot_id=svc.s.onebot_self_id,
                user_id=uid,
                kind="group",
                conversation_id=group,
                event_id="admin",
                text="",
                addressed=True,
                created_at=time.time(),
            )
            for row in rows:
                if row["scope"] == svc.store.session_scope(m):
                    item["groups"].append(
                        {
                            "group_id": group,
                            "cooldown_seconds": max(0, int(row["expires"] - now))
                            if row["used"] >= 2
                            else 0,
                            "is_target": row["target"] == svc.store.subject(m),
                            "consumed": row["used"] >= 2,
                            "used": row["used"],
                            "max_turns": 2,
                            "expires_at": row["expires"],
                            "window_seconds": max(0, int(row["expires"] - now)),
                            "permit_seconds": max(0, int(row["permit_until"] - now)),
                        }
                    )
        result.append(item)
    return result


@router.put("/bots")
async def bot_change(request: Request):
    data = await body(request, BotChange)
    svc = service(request)
    async with domain_lock(request, "admission"):
        try:
            if data.action in {"add", "remove"}:
                await svc.store.runtime.change_admission(
                    "ignored_bot_ids", data.action, data.user_id
                )
                if data.action == "add" and data.threshold is not None:
                    await svc.store.runtime.set_bot_thresholds(
                        user_id=data.user_id, threshold=data.threshold
                    )
            else:
                await svc.store.runtime.set_bot_thresholds(
                    user_id=data.user_id,
                    threshold=data.threshold,
                    clear_user_override=data.threshold is None,
                )
        except ValidationError as exc:
            raise invalid_input_http_error(exc) from None
        return {"status": "saved"}


@router.get("/bots/thresholds")
async def bot_thresholds(request: Request):
    return _bot_thresholds_status(service(request))


@router.put("/bots/thresholds")
async def save_bot_thresholds(request: Request):
    data = await body(request, BotThresholdConfig)
    svc = service(request)
    async with domain_lock(request, "admission"):
        try:
            await svc.store.runtime.set_bot_thresholds(default_threshold=data.bot_reply_threshold)
        except ValidationError as exc:
            raise invalid_input_http_error(exc) from None
        return _bot_thresholds_status(svc)
