"""Authenticated policy, limits, routing-model, search, filter, group, admission, and bot routes."""

import time
from decimal import Decimal
from typing import Literal

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
from .limits_policy import (
    ContentToolsConfig,
    GenerationCapsConfig,
    ImageLimitsConfig,
    PatrolLimitConfig,
    RateLimitConfig,
)
from .models import Message
from .rest_policy import RestHoursConfig
from .security import Rejected
from .thinking_policy import ModelSettingsUpdate, ThinkingConfig
from .web_search import WebSearchConfig

router = APIRouter(prefix="/admin/api")


class GroupPolicy(Input):
    platform: Literal["onebot", "qq"] = "onebot"
    group_id: str = Field(min_length=1, max_length=128)
    mode: Literal["at_only", "smart_reply", "muted"]


class GroupActivity(Input):
    platform: Literal["onebot", "qq"] = "onebot"
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


@router.get("/rate-limits")
async def rate_limits_status(request: Request):
    return service(request).store.runtime.rate_limits_status()


@router.put("/rate-limits")
async def rate_limits_save(request: Request):
    value = await body(request, RateLimitConfig)
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_rate_limits(value)


@router.delete("/rate-limits")
async def rate_limits_reset(request: Request):
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_rate_limits(None)


@router.get("/image-limits")
async def image_limits_status(request: Request):
    return service(request).store.runtime.image_limits_status()


@router.put("/image-limits")
async def image_limits_save(request: Request):
    value = await body(request, ImageLimitsConfig)
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_image_limits(value)


@router.delete("/image-limits")
async def image_limits_reset(request: Request):
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_image_limits(None)


@router.get("/content-tools")
async def content_tools_status(request: Request):
    return service(request).store.runtime.content_tools_status()


@router.put("/content-tools")
async def content_tools_save(request: Request):
    value = await body(request, ContentToolsConfig)
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_content_tools(value)


@router.delete("/content-tools")
async def content_tools_reset(request: Request):
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_content_tools(None)


@router.get("/generation-caps")
async def generation_caps_status(request: Request):
    return service(request).store.runtime.generation_caps_status()


@router.put("/generation-caps")
async def generation_caps_save(request: Request):
    value = await body(request, GenerationCapsConfig)
    async with domain_lock(request, "limits"):
        runtime = service(request).store.runtime
        merged = runtime.generation_caps.model_dump() | value.model_dump(exclude_unset=True)
        return await runtime.set_generation_caps(GenerationCapsConfig.model_validate(merged))


@router.delete("/generation-caps")
async def generation_caps_reset(request: Request):
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_generation_caps(None)


@router.get("/patrol-limits")
async def patrol_limits_status(request: Request):
    return service(request).store.runtime.patrol_limits_status()


@router.put("/patrol-limits")
async def patrol_limits_save(request: Request):
    value = await body(request, PatrolLimitConfig)
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_patrol_limits(value)


@router.delete("/patrol-limits")
async def patrol_limits_reset(request: Request):
    async with domain_lock(request, "limits"):
        return await service(request).store.runtime.set_patrol_limits(None)


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
    async with domain_lock(request, "web_search"):
        return await service(request).store.runtime.web_search.save_deepseek(
            api_key=value.api_key,
            base_url=value.base_url,
            model=value.model,
            clear=value.clear,
        )


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


def allowed_activity_group(settings, platform, group_id):
    allowed = settings.onebot_allowed_groups if platform == "onebot" else settings.qq_allowed_groups
    if group_id not in allowed:
        raise Rejected("group_not_allowlisted")


@router.put("/filter")
async def filter_save(request: Request):
    value = await body(request, FilterConfig)
    svc = service(request)
    manager = svc.store.runtime.filters
    async with domain_lock(request, "filter"):
        if "group_activity" not in value.model_fields_set:
            value = value.model_copy(update={"group_activity": dict(manager.config.group_activity)})
        else:
            for key in value.group_activity:
                platform, group_id = key.split(":", 1)
                allowed_activity_group(svc.s, platform, group_id)
        return await manager.set_config(value)


@router.delete("/filter")
async def filter_reset(request: Request):
    async with domain_lock(request, "filter"):
        return await service(request).store.runtime.filters.set_config(None)


@router.post("/filter/group-activity")
async def group_activity(request: Request):
    value = await body(request, GroupActivity)
    svc = service(request)
    allowed_activity_group(svc.s, value.platform, value.group_id)
    async with domain_lock(request, "filter"):
        return await svc.store.runtime.filters.set_group_activity(
            value.platform, value.group_id, value.preset
        )


@router.get("/decision-audit")
async def decision_audit(request: Request):
    return await service(request).store.runtime.filters.recent_audit(limit=30)


@router.get("/groups")
async def groups(request: Request):
    s = service(request).s
    return [
        {
            "platform": platform,
            "group_id": gid,
            "mode": s.group_policies.get(f"{platform}:{gid}", "at_only"),
        }
        for platform, ids in [("onebot", s.onebot_allowed_groups), ("qq", s.qq_allowed_groups)]
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
        allowed = (
            svc.s.onebot_allowed_groups if data.platform == "onebot" else svc.s.qq_allowed_groups
        )
        if data.group_id not in allowed:
            raise Rejected("group_not_allowlisted")
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
