"""Small, authenticated control plane; simulation never calls Service.accept or Sender."""

import asyncio
import json
import re
import time
from datetime import date
from decimal import Decimal
from typing import Literal

import httpx
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .admission_policy import (
    AdmissionConfig,
    _validate_threshold_number,
    effective_bot_reply_threshold,
    is_bot_hard_blocked,
)
from .attention import can_watch_video, classify_attention
from .bot_server import prompt_context
from .budget_policy import BudgetConfig
from .compression_policy import CompressionConfig
from .debounce_policy import DebounceConfig
from .gateway import FilterConfig
from .generation_policy import Scenario, generation_plan
from .limits_policy import (
    ContentToolsConfig,
    GenerationCapsConfig,
    ImageLimitsConfig,
    PatrolLimitConfig,
    RateLimitConfig,
)
from .memory_compressor import check_memory_text
from .mentions import parse_reply
from .models import Message
from .modules.bilibili import extract_bvid
from .outbound_actions import parse_actions
from .output_links import filter_reply_links
from .provider_routing import ProviderView
from .rest_policy import RestHoursConfig
from .rich_message_parser import parse_message
from .security import Rejected, authorize, configured_secrets
from .thinking_policy import ModelSettingsUpdate, ThinkingConfig, preferences
from .web_search import WebSearchConfig
from .work_usage import model_work

router = APIRouter(prefix="/admin")


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Sandbox(Input):
    identity: Literal["OWNER", "MEMBER", "BOT"]
    scene: Literal["group_at", "group_plain", "private"]
    text: str = Field(min_length=1, max_length=2000)
    group_id: str = Field(default="", pattern=r"^[0-9]{0,20}$")


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


class SummaryEdit(Input):
    text: str = Field(max_length=800)
    version: int = Field(ge=0)


class TextInput(Input):
    text: str = Field(max_length=4000)


class Subscription(Input):
    kind: Literal["up", "live"]
    uid: str = Field(pattern=r"^[1-9][0-9]{0,19}$")


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


async def body(request, model):
    from .app import json_object, read_body

    try:
        return model.model_validate(
            json_object(await read_body(request, request.app.state.settings))
        )
    except ValidationError:
        raise HTTPException(400, "invalid_admin_input") from None


def service(request):
    return request.app.state.service


def owner_target(s):
    if not s.onebot_enabled or not s.owner_qq_id:
        raise Rejected("configure_owner_qq_id")
    return Message(
        platform="onebot",
        bot_id=s.onebot_self_id,
        user_id=s.owner_qq_id,
        kind="private",
        conversation_id=s.owner_qq_id,
        event_id="admin-patrol",
        text="",
        addressed=True,
        created_at=time.time(),
    )


ADMIN_PAGE_URL = "http://hakimi-admin:8080/admin"


@router.head("")
@router.head("/")
async def page_access():
    """The private frontend checks access here before serving the login document."""
    return Response(status_code=204)


@router.get("")
@router.get("/")
async def page(request: Request):
    """Check console access or serve the independent UI through a legacy SSH tunnel."""
    if request.headers.get("accept") == "application/json":
        return Response(status_code=204)
    try:
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            doc = await client.get(ADMIN_PAGE_URL, headers={"Host": "127.0.0.1"})
    except httpx.RequestError:
        raise HTTPException(503, "admin_ui_unavailable") from None
    policy = doc.headers.get("Content-Security-Policy")
    if doc.status_code != 200 or not policy:
        raise HTTPException(503, "admin_ui_unavailable")
    return Response(
        doc.content, media_type="text/html", headers={"Content-Security-Policy": policy}
    )


def onebot_connection(request: Request, svc) -> dict:
    transport = request.app.state.onebot
    ws = (
        "http_mode"
        if svc.s.onebot_mode == "http"
        else "online"
        if transport.ready.is_set()
        else "reconnecting"
    )
    return {
        "onebot": ws,
        "onebot_detail": transport.status()
        if hasattr(transport, "status")
        else {"online": None, "last_heartbeat_at": None, "source_ip": None, "connected_at": None},
    }


@router.get("/api/dashboard")
async def dashboard(request: Request):
    snapshot = await _dashboard(request, "full")
    snapshot.update(onebot_connection(request, service(request)))
    return snapshot


@router.get("/api/global/dashboard")
async def global_dashboard(request: Request):
    return await _dashboard(request, "global")


async def _dashboard(request: Request, view: ProviderView):
    svc = service(request)
    await svc.store.runtime.flush()
    usage = await svc.store.usage()
    async with svc.store.lock:
        metrics = await svc.store.all(
            "SELECT code,source,n FROM daily_metrics WHERE day=?", (usage["day_utc"],)
        )
        count = await svc.store.one(
            "SELECT n FROM counters WHERE key=?", (f"global:{int(time.time() // 60)}",)
        )
        circuit = bool(await svc.store.one("SELECT 1 FROM meta WHERE key='budget_overrun'"))
    notebook = svc.notebook.status() if svc.notebook else {"enabled": False}
    notebook.pop("text", None)
    routing = svc.store.runtime.provider_routing
    await routing.initialize()
    return {
        "uptime_seconds": int(time.monotonic() - svc.started_at),
        "primary_model": preferences(svc.s).primary_model,
        "pipeline": {
            "gate": sum(b.phase == "gate" for b in svc.store.groups.active.values()),
            "generating": sum(b.phase == "generating" for b in svc.store.groups.active.values()),
            "hot_sessions": len(svc.hot.rings),
            "image_cache_bytes": getattr(svc.images, "cache_bytes", 0),
        },
        "usage": usage,
        "budget_status": await svc.store.budget_status(),
        "daily_limit_usd": str(svc.s.daily_usd),
        "user_limit_usd": str(svc.s.user_daily_usd),
        "patrol_limit_usd": str(svc.s.patrol_daily_usd),
        "memory_limit_usd": str(svc.s.memory_daily_usd),
        "proactive_limit_usd": str(svc.s.proactive_daily_usd),
        "admin_limit_usd": str(svc.s.admin_daily_usd),
        "minute_requests": count[0] if count else 0,
        "minute_limit": svc.s.global_requests_per_minute,
        "budget_circuit_open": circuit,
        "metrics": [dict(row) for row in metrics],
        "notebook": notebook,
        "rest_hours": svc.store.runtime.rest_hours_status(),
        "provider_routing": routing.config.for_admin(view),
    }


@router.get("/api/budget")
async def budget_config(request: Request):
    return await service(request).store.budget_config_status()


@router.put("/api/budget")
async def budget_save(request: Request):
    from .app import json_object, read_body

    raw = json_object(await read_body(request, request.app.state.settings))
    if not raw:
        raise HTTPException(400, "invalid_admin_input")
    svc = service(request)
    async with request.app.state.admin_config_lock:
        current = BudgetConfig.from_settings(svc.s).model_dump(mode="json")
        try:
            value = BudgetConfig.model_validate({**current, **raw})
        except ValidationError:
            raise HTTPException(400, "invalid_admin_input") from None
        return await svc.store.set_budget_config(value)


@router.delete("/api/budget")
async def budget_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.set_budget_config(None)


@router.get("/api/account-balance")
async def account_balance_status(request: Request):
    return await service(request).store.account_balance_status()


@router.put("/api/account-balance")
async def account_balance_save(request: Request):
    value = await body(request, AccountBalanceBody)
    async with request.app.state.admin_config_lock:
        return await service(request).store.set_account_balance(value.balance_usd)


@router.delete("/api/account-balance")
async def account_balance_clear(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.clear_account_balance()


@router.get("/api/rate-limits")
async def rate_limits_status(request: Request):
    return service(request).store.runtime.rate_limits_status()


@router.put("/api/rate-limits")
async def rate_limits_save(request: Request):
    value = await body(request, RateLimitConfig)
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_rate_limits(value)


@router.delete("/api/rate-limits")
async def rate_limits_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_rate_limits(None)


@router.get("/api/image-limits")
async def image_limits_status(request: Request):
    return service(request).store.runtime.image_limits_status()


@router.put("/api/image-limits")
async def image_limits_save(request: Request):
    value = await body(request, ImageLimitsConfig)
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_image_limits(value)


@router.delete("/api/image-limits")
async def image_limits_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_image_limits(None)


@router.get("/api/content-tools")
async def content_tools_status(request: Request):
    return service(request).store.runtime.content_tools_status()


@router.put("/api/content-tools")
async def content_tools_save(request: Request):
    value = await body(request, ContentToolsConfig)
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_content_tools(value)


@router.delete("/api/content-tools")
async def content_tools_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_content_tools(None)


@router.get("/api/generation-caps")
async def generation_caps_status(request: Request):
    return service(request).store.runtime.generation_caps_status()


@router.put("/api/generation-caps")
async def generation_caps_save(request: Request):
    value = await body(request, GenerationCapsConfig)
    async with request.app.state.admin_config_lock:
        runtime = service(request).store.runtime
        merged = runtime.generation_caps.model_dump() | value.model_dump(exclude_unset=True)
        return await runtime.set_generation_caps(GenerationCapsConfig.model_validate(merged))


@router.delete("/api/generation-caps")
async def generation_caps_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_generation_caps(None)


@router.get("/api/patrol-limits")
async def patrol_limits_status(request: Request):
    return service(request).store.runtime.patrol_limits_status()


@router.put("/api/patrol-limits")
async def patrol_limits_save(request: Request):
    value = await body(request, PatrolLimitConfig)
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_patrol_limits(value)


@router.delete("/api/patrol-limits")
async def patrol_limits_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.set_patrol_limits(None)


@router.get("/api/rest-hours")
@router.get("/api/life/rest-hours")
async def rest_hours_status(request: Request):
    return service(request).store.runtime.rest_hours_status()


@router.put("/api/rest-hours")
@router.put("/api/life/rest-hours")
async def rest_hours_save(request: Request):
    value = await body(request, RestHoursConfig)
    svc = service(request)
    async with request.app.state.admin_config_lock:
        if "day_rest_enabled" not in value.model_fields_set:
            value = value.model_copy(
                update={"day_rest_enabled": svc.store.runtime.rest_hours.day_rest_enabled}
            )
        return await svc.life_engine.set_rest_policy(value)


@router.delete("/api/rest-hours")
@router.delete("/api/life/rest-hours")
async def rest_hours_reset(request: Request):
    svc = service(request)
    async with request.app.state.admin_config_lock:
        return await svc.life_engine.set_rest_policy(None)


@router.get("/api/filter")
async def filter_status(request: Request):
    return await service(request).store.runtime.filters.status()


@router.get("/api/models")
async def model_settings(request: Request):
    svc = service(request)
    return svc.store.runtime.model_settings_status(svc.modules)


@router.put("/api/models")
@router.delete("/api/models")
async def save_model_settings(request: Request):
    svc = service(request)
    update = await body(request, ModelSettingsUpdate) if request.method == "PUT" else None
    try:
        return await svc.store.runtime.set_model_settings(svc.modules, update)
    except ValueError:
        raise HTTPException(400, "invalid_model_settings") from None


@router.get("/api/thinking")
async def thinking_status(request: Request):
    return service(request).store.runtime.thinking_status()


@router.get("/api/debounce")
async def debounce_status(request: Request):
    return service(request).store.runtime.debounce_status()


@router.get("/api/web-search")
async def web_search_status(request: Request):
    return await service(request).store.runtime.web_search.status()


@router.put("/api/web-search")
async def web_search_save(request: Request):
    return await service(request).store.runtime.web_search.save(
        await body(request, WebSearchConfig)
    )


@router.delete("/api/web-search")
async def web_search_reset(request: Request):
    return await service(request).store.runtime.web_search.save(None)


@router.post("/api/web-search/deepseek")
async def web_search_deepseek_save(request: Request):
    value = await body(request, DeepSeekCredentialsBody)
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.web_search.save_deepseek(
            api_key=value.api_key,
            base_url=value.base_url,
            model=value.model,
            clear=value.clear,
        )


@router.put("/api/debounce")
async def debounce_save(request: Request):
    return await service(request).store.runtime.set_debounce(await body(request, DebounceConfig))


@router.delete("/api/debounce")
async def debounce_reset(request: Request):
    return await service(request).store.runtime.set_debounce(None)


@router.put("/api/thinking")
async def thinking_save(request: Request):
    value = await body(request, ThinkingConfig)
    return await service(request).store.runtime.set_thinking(value)


@router.delete("/api/thinking")
async def thinking_reset(request: Request):
    return await service(request).store.runtime.set_thinking(None)


def allowed_activity_group(settings, platform, group_id):
    allowed = settings.onebot_allowed_groups if platform == "onebot" else settings.qq_allowed_groups
    if group_id not in allowed:
        raise Rejected("group_not_allowlisted")


@router.put("/api/filter")
async def filter_save(request: Request):
    value = await body(request, FilterConfig)
    svc = service(request)
    manager = svc.store.runtime.filters
    async with request.app.state.admin_config_lock:
        if "group_activity" not in value.model_fields_set:
            value = value.model_copy(update={"group_activity": dict(manager.config.group_activity)})
        else:
            for key in value.group_activity:
                platform, group_id = key.split(":", 1)
                allowed_activity_group(svc.s, platform, group_id)
        return await manager.set_config(value)


@router.delete("/api/filter")
async def filter_reset(request: Request):
    async with request.app.state.admin_config_lock:
        return await service(request).store.runtime.filters.set_config(None)


@router.post("/api/filter/group-activity")
async def group_activity(request: Request):
    value = await body(request, GroupActivity)
    svc = service(request)
    allowed_activity_group(svc.s, value.platform, value.group_id)
    async with request.app.state.admin_config_lock:
        return await svc.store.runtime.filters.set_group_activity(
            value.platform, value.group_id, value.preset
        )


@router.get("/api/decision-audit")
async def decision_audit(request: Request):
    return await service(request).store.runtime.filters.recent_audit(limit=30)


@router.get("/api/logs")
async def logs(request: Request):
    store = service(request).store
    await store.runtime.flush()
    async with store.lock:
        rows = await store.all(
            "SELECT at,code,source,diagnostics FROM audit_events ORDER BY id DESC LIMIT 50"
        )
    return [
        {
            "at": row["at"],
            "code": row["code"],
            "source": row["source"],
            **(
                {"provider_diagnostics": json.loads(row["diagnostics"])}
                if row["diagnostics"]
                else {}
            ),
        }
        for row in rows
    ]


@router.post("/api/actions/{action}")
async def action(request: Request, action: str):
    svc = service(request)
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        if action == "patrol":
            if svc.modules.inactive_reason("bilibili_surf"):
                raise HTTPException(409, "module_disabled")
            await svc.poll_once(force=True)
            return {
                "status": "polled_updates_queued",
                "note": "模型摘要按原工作队列处理；首次巡逻只建立基线。",
            }
        if action == "diary":
            now = time.time()
            await svc.diary.prepare_due(now=now, force=True)
            state = await svc.diary_sync.sync_due(now=now, force=True)
            notes = {
                "uploaded": "本地日记已整理，Drive 已确认同步。",
                "unchanged": "本地日记已整理，Drive 上已是当前版本。",
                "unconfigured": "本地日记已整理，Drive 等待配置。",
                "pending": "本地日记已整理，Drive 同步待重试。",
            }
            return {"status": state, "note": notes[state]}
        if action == "notebook":
            return await svc.notebook.refresh()
        raise HTTPException(404)


@router.get("/api/groups")
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


@router.get("/api/admission")
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


@router.put("/api/admission")
async def save_admission(request: Request):
    await service(request).store.runtime.set_admission(await body(request, AdmissionConfig))
    return await admission(request)


@router.post("/api/admission/change")
async def change_admission(request: Request):
    data = await body(request, AdmissionChange)
    try:
        await service(request).store.runtime.change_admission(data.list, data.action, data.id)
    except ValidationError:
        raise HTTPException(400, "invalid_admin_input") from None
    return await admission(request)


@router.delete("/api/admission")
async def reset_admission(request: Request):
    await service(request).store.runtime.set_admission(None)
    return await admission(request)


@router.put("/api/groups")
async def policy(request: Request):
    data = await body(request, GroupPolicy)
    svc = service(request)
    allowed = svc.s.onebot_allowed_groups if data.platform == "onebot" else svc.s.qq_allowed_groups
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


@router.get("/api/bots")
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


@router.put("/api/bots")
async def bot_change(request: Request):
    data = await body(request, BotChange)
    svc = service(request)
    try:
        if data.action in {"add", "remove"}:
            await svc.store.runtime.change_admission("ignored_bot_ids", data.action, data.user_id)
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
    except ValidationError:
        raise HTTPException(400, "invalid_admin_input") from None
    return {"status": "saved"}


@router.get("/api/bots/thresholds")
async def bot_thresholds(request: Request):
    return _bot_thresholds_status(service(request))


@router.put("/api/bots/thresholds")
async def save_bot_thresholds(request: Request):
    data = await body(request, BotThresholdConfig)
    svc = service(request)
    try:
        await svc.store.runtime.set_bot_thresholds(default_threshold=data.bot_reply_threshold)
    except ValidationError:
        raise HTTPException(400, "invalid_admin_input") from None
    return _bot_thresholds_status(svc)


async def _dual_stats(svc, msg, enabled: bool) -> dict:
    dual_mgr = getattr(svc.sessions, "dual", None)
    if not (enabled and dual_mgr is not None and hasattr(dual_mgr, "view")):
        return {"dual_entries_count": 0, "dual_chars": 0}
    view = await dual_mgr.view(msg)
    active = [e for e in view.get("entries", []) if not e.get("deleted")]
    return {
        "dual_entries_count": len(active),
        "dual_chars": sum(len(str(e.get("text", ""))) for e in active),
        "flex_retry": view.get("flex_retry"),
        "archive_failed_slices": view.get("jobs", {}).get("failed", 0),
        "archive_unconfirmed_slices": view.get("jobs", {}).get("unconfirmed", 0),
    }


@router.get("/api/sessions")
async def sessions(request: Request):
    svc = service(request)
    result = []
    for key, msg in await svc.store.runtime.sessions():
        snap = await svc.sessions.snapshot(msg)
        stats = await _dual_stats(svc, msg, bool(snap["enabled"]))
        result.append(
            {
                "id": key,
                "platform": msg.platform,
                "kind": msg.kind,
                "conversation_id": msg.conversation_id,
                "enabled": snap["enabled"],
                "dual_ready": bool(snap.get("dual_ready")),
                "summary_chars": len(snap["rolling_summary"]),
                "recent_count": len(snap["recent_messages"]),
                **stats,
            }
        )
    return result


@router.get("/api/compression")
async def compression_config(request: Request):
    runtime = service(request).store.runtime
    return {
        "config": runtime.compression.model_dump(),
        "history_strategy": "prefix_half" if runtime.s.prefix_history_enabled else "legacy_window",
        "history_high_tokens_estimate": runtime.s.history_high_tokens,
        "history_max_bytes": runtime.s.history_max_bytes,
        "source": "admin" if runtime.compression_overridden else "env",
        "environment": runtime.env_compression.model_dump(),
    }


def owner_delivery_for(request: Request, module_id: str):
    svc = service(request)
    module = svc.modules.get(module_id)
    if module is None or svc.modules.chat_platforms.get(svc.proactive.platform) is not module:
        raise HTTPException(404, "owner_delivery_not_assigned")
    return svc.proactive


@router.get("/api/life/modules/{module_id}/owner-delivery")
async def module_delivery_status(request: Request, module_id: str):
    return await owner_delivery_for(request, module_id).status()


@router.get("/api/proactive")
async def proactive_status(request: Request):
    if service(request).proactive.platform != "onebot":
        return {"assigned": False}
    return await module_delivery_status(request, "qq_social")


@router.post("/api/proactive/{action}")
async def proactive_action(request: Request, action: str):
    return await module_delivery_action(request, "qq_social", action)


@router.post("/api/life/modules/{module_id}/owner-delivery/{action}")
async def module_delivery_action(request: Request, module_id: str, action: str):
    dm = owner_delivery_for(request, module_id)
    request.app.state.admin_auth.costly()
    lock = request.app.state.admin_action_lock
    if lock.locked():
        raise HTTPException(409, "admin_action_busy")
    async with lock:
        if action == "test":
            return await dm.test_dm()
        if action == "digest":
            return await dm.manual_digest()
        raise HTTPException(404)


@router.put("/api/compression")
async def save_compression_config(request: Request):
    data = await body(request, CompressionConfig)
    async with request.app.state.admin_config_lock:
        await service(request).store.runtime.set_compression(data)
    return await compression_config(request)


@router.delete("/api/compression")
async def reset_compression_config(request: Request):
    async with request.app.state.admin_config_lock:
        await service(request).store.runtime.set_compression(None)
    return await compression_config(request)


async def session_message(request, key):
    if not re.fullmatch(r"[a-f0-9]{64}", key):
        raise HTTPException(404)
    return await service(request).store.runtime.session(key)


@router.get("/api/sessions/{key}")
async def session_detail(request: Request, key: str):
    svc = service(request)
    msg = await session_message(request, key)
    snap = await svc.sessions.snapshot(msg)
    # Admin needs summary and hot-window count, not automatic disclosure of raw conversations.
    snap["recent_count"] = len(snap.pop("recent_messages"))
    snap.update(await _dual_stats(svc, msg, bool(snap["enabled"])))
    return snap


@router.put("/api/sessions/{key}/summary")
async def edit_summary(request: Request, key: str):
    data = await body(request, SummaryEdit)
    svc = service(request)
    msg = await session_message(request, key)
    if (await svc.sessions.snapshot(msg))["version"] != data.version:
        raise Rejected("session_changed")
    request.app.state.admin_auth.costly()
    subject = svc.store.vault.digest("admin-summary-validation")
    await svc.compressor.validate_summary(data.text, subject)
    await svc.sessions.edit_summary(msg, data.text, data.version)
    return {"status": "saved"}


@router.post("/api/sessions/{key}/reset")
async def reset_session(request: Request, key: str):
    svc = service(request)
    await svc.sessions.clear(await session_message(request, key))
    return {"status": "reset"}


@router.post("/api/sessions/{key}/compress")
async def compress_session(request: Request, key: str):
    request.app.state.admin_auth.costly()
    svc = service(request)
    return await svc.compressor.compress(await session_message(request, key), force=True)


@router.get("/api/notebook")
async def notebook(request: Request):
    svc = service(request)
    status = dict(svc.notebook.status())
    cloud = getattr(svc, "cloud_memory", None)
    if cloud is not None and hasattr(cloud, "admin_preview"):
        status["antigravity_memory"] = await asyncio.to_thread(cloud.admin_preview)
    return status


@router.put("/api/notebook")
async def notebook_write(request: Request):
    data = await body(request, TextInput)
    request.app.state.admin_auth.costly()
    return await service(request).notebook.update_local(data.text)


@router.get("/api/subscriptions")
async def subscriptions(request: Request):
    store = service(request).store
    result = [
        {"id": row["id"], "kind": "up", "uid": row["mid"], "next_poll": row["next_poll"]}
        for row in await store.subscriptions()
    ]
    async with store.lock:
        rows = await store.all("SELECT room_id,last_status,next_poll FROM live_watches")
    return result + [
        {
            "id": row["room_id"],
            "kind": "live",
            "uid": row["room_id"],
            "last_status": row["last_status"],
            "next_poll": row["next_poll"],
        }
        for row in rows
    ]


@router.post("/api/subscriptions")
async def add_subscription(request: Request):
    data = await body(request, Subscription)
    svc = service(request)
    target = owner_target(svc.s)
    async with request.app.state.admin_config_lock:
        current = await subscriptions(request)
        if len(current) >= svc.s.max_subscriptions:
            raise Rejected("subscription_limit")
        if data.kind == "up":
            await svc.store.add_subscription(target, data.uid)
        else:
            async with svc.store.transaction():
                await svc.store.db.execute(
                    "INSERT OR IGNORE INTO live_watches(room_id,target) VALUES (?,?)",
                    (data.uid, svc.store.vault.seal("live:" + data.uid, target.model_dump())),
                )
    return {"status": "saved"}


@router.delete("/api/subscriptions/{kind}/{key}")
async def delete_subscription(request: Request, kind: str, key: str):
    store = service(request).store
    if kind == "up":
        row = next((r for r in await store.subscriptions() if r["id"] == key), None)
        if not row:
            raise HTTPException(404)
        await store.remove_subscription(row["subject"], row["mid"])
    elif kind == "live":
        async with store.transaction():
            await store.db.execute("DELETE FROM live_watches WHERE room_id=?", (key,))
    else:
        raise HTTPException(404)
    return {"status": "deleted"}


@router.get("/api/system-modules")
async def system_modules(request: Request):
    return [
        {
            "id": module.module_id,
            "title": module.display_name,
            "description": module.description,
            "panel": module.admin_panel,
            "status": await module.status(),
        }
        for module in service(request).modules.systems.values()
    ]


@router.get("/api/drive-sync")
async def drive_sync(request: Request):
    return await service(request).drive_sync.overview()


@router.get("/api/diaries")
async def diaries(request: Request):
    svc = service(request)
    async with svc.store.lock:
        return [
            dict(row)
            for row in await svc.store.all(
                "SELECT d.day,s.slot,COALESCE(s.next_attempt,0) AS next_attempt,"
                "COALESCE(s.failures,0) AS failures,s.last_error,s.last_success "
                "FROM roaming_days d LEFT JOIN roaming_sync s ON d.day=s.day "
                "ORDER BY d.day DESC LIMIT 8"
            )
        ]


@router.get("/api/diaries/status")
async def diary_status(request: Request):
    return await service(request).diary_sync.status()


@router.get("/api/diaries/{day}")
async def diary(request: Request, day: str):
    try:
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError
    except ValueError:
        raise HTTPException(400, "invalid_day") from None
    svc = service(request)
    return {"name": day + ".md", "markdown": await svc.diary.render(day)}


@router.post("/api/sandbox")
async def sandbox(request: Request):
    with model_work("admin.sandbox", independent=True) as work:
        return await _sandbox(request, work.key)


async def _sandbox(request: Request, audit_id: str):
    data = await body(request, Sandbox)
    svc = service(request)
    request.app.state.admin_auth.costly()
    lock = request.app.state.sandbox_lock
    if lock.locked():
        raise HTTPException(409, "sandbox_busy")
    async with lock:
        # A private copied configuration: synthetic IDs cannot touch real scopes or grants.
        s = svc.s.model_copy(deep=True)
        if not s.onebot_enabled:
            raise Rejected("sandbox_requires_onebot")
        owner_id = s.owner_qq_id or "990000001"
        member_id = next(
            (
                str(i)
                for i in range(990000002, 990000020)
                if str(i) not in {owner_id, s.onebot_self_id}
            ),
            None,
        )
        bot_id = next(
            str(i)
            for i in range(990000020, 990000040)
            if str(i) not in {owner_id, s.onebot_self_id, member_id}
        )
        s.owner_qq_id = owner_id
        s.onebot_owner_ids = {owner_id}
        s.onebot_allowed_users = {member_id, bot_id}
        s.bot_qq_ids = {bot_id}
        group = data.group_id or next(iter(sorted(s.onebot_allowed_groups)), "990000100")
        mode = svc.s.group_policies.get("onebot:" + group, "at_only")
        s.onebot_allowed_groups = {group}
        s.group_policies = {"onebot:" + group: mode}
        uid = {"OWNER": owner_id, "MEMBER": member_id, "BOT": bot_id}[data.identity]
        kind = "private" if data.scene == "private" else "group"
        rich = parse_message([{"type": "text", "data": {"text": data.text}}], s.onebot_self_id)
        attention, addressed = classify_attention(
            kind, mentioned=data.scene == "group_at", text=rich.command_text
        )
        msg = Message(
            platform="onebot",
            bot_id=s.onebot_self_id,
            user_id=uid,
            kind=kind,
            conversation_id=uid if kind == "private" else group,
            event_id="sandbox",
            text=data.text,
            command_text="",
            addressed=addressed,
            attention_reason=attention,
            created_at=time.time(),
        )
        context = prompt_context(msg, s)
        scenario = Scenario.OWNER_PRIVATE if context.private_owner else Scenario.FAST_CHAT
        preview = {
            "thinking_level": generation_plan(s, scenario).thinking_level,
            "persona": "OWNER" if context.private_owner else "PUBLIC",
            "text": "",
        }
        result = {
            "decisions": {"result": "not_called", "answers": {}},
            "preview": preview,
            "sandbox": True,
            "group_policy": mode,
        }
        if data.identity == "BOT":
            if kind != "group" or is_bot_hard_blocked(bot_id, svc.s):
                result["decisions"]["reason"] = "known_bot_ignored"
                return result
            if not authorize(msg, s, _bot_exempt=True):
                result["decisions"]["reason"] = "group_policy_ignored"
                return result
        elif not authorize(msg, s):
            result["decisions"]["reason"] = "group_policy_ignored"
            return result
        if any(
            secret and len(secret) >= 8 and secret in data.text
            for secret in configured_secrets(svc.s)
        ):
            raise Rejected("credential_in_input")
        # Production gateway recomputes identity. For OWNER simulation with no configured Owner,
        # keep PUBLIC; never allow synthetic identity to unlock production private context.
        actual_context = prompt_context(msg, svc.s)
        preview["persona"] = "OWNER" if actual_context.private_owner else "PUBLIC"
        direct = data.text.strip()
        if direct == "/new-session" or direct.startswith(("/bot-interact", svc.s.command_prefix)):
            result["decisions"]["reason"] = "sandbox_stateful_command_skipped"
            preview["thinking_level"] = None
            preview["text"] = "沙盒不执行会修改生产状态的斜杠指令。"
            return result
        deep_match = re.fullmatch(r"/deep(?:\s+(.*))?", direct, re.DOTALL)
        deep = deep_match is not None
        if deep and (not actual_context.private_owner or not deep_match[1]):
            result["decisions"]["reason"] = "owner_private_deep_required"
            preview["thinking_level"] = None
            return result
        preview["thinking_level"] = generation_plan(s, scenario, deep=deep).thinking_level
        subject = svc.store.vault.digest("admin-sandbox-budget")
        payload = {
            "current_message": data.text,
            "surface": kind,
            "directly_addressed": msg.addressed,
            "recent_messages": [],
            "rolling_summary": "",
        }
        rejection = None
        try:
            intent = await svc.ai.screen(
                payload,
                subject,
                purpose="message",
                group=kind == "group",
                message=msg,
                trace=result["decisions"],
                budget_lane="admin",
                audit_source="sandbox",
                audit_id=audit_id,
                known_bot=data.identity == "BOT",
            )
            if result["decisions"].get("result") == "not_called":
                result["decisions"]["result"] = intent
            if intent != "ignore":
                bvid = (
                    extract_bvid(data.text)
                    if data.identity != "BOT"
                    and not getattr(intent, "sandbox_mode", False)
                    and can_watch_video(msg)
                    else None
                )
                if bvid:
                    video = await svc.bili.video(bvid)
                    payload = {
                        "task": "summarize_public_metadata",
                        "material_scope": "metadata_only",
                        "untrusted_sources": [video.model_dump(exclude={"mid"})],
                    }
                    result["decisions"]["source"] = {}
                    await svc.ai.screen(
                        payload,
                        subject,
                        purpose="source",
                        budget_lane="admin",
                        audit_source="sandbox",
                        trace=result["decisions"]["source"],
                    )
                    scenario = Scenario.BILIBILI_SUMMARY
                    preview["thinking_level"] = generation_plan(
                        s, scenario, deep=deep
                    ).thinking_level
                generated = await svc.ai.generate(
                    payload,
                    subject,
                    scenario=scenario,
                    deep=deep,
                    message=msg,
                    budget_lane="admin",
                    audit_source="sandbox",
                    route=intent,
                    generation_trace=preview,
                )
                public = not actual_context.private_owner
                sandbox_abstain = False
                if bvid:
                    summary = filter_reply_links(generated, public=public)
                    preview["text"] = parse_reply(
                        svc.output(f"{summary}\n{video.url}"), msg, svc.s
                    ).text
                elif generated.lstrip().startswith("{"):
                    if any(
                        secret and len(secret) >= 8 and secret in generated
                        for secret in configured_secrets(svc.s)
                    ):
                        raise Rejected("invalid_output")
                    actions, folded = parse_actions(
                        generated,
                        public=public,
                        settings=svc.s,
                        labels=svc.memes.labels,
                        tier=getattr(intent, "tier", 2),
                    )
                    preview["actions"] = actions.model_dump()
                    sandbox_abstain = actions.abstain
                    body_text = folded or "\n".join(actions.bubbles)
                    preview["text"] = (
                        "" if sandbox_abstain or not body_text.strip() else svc.output(body_text)
                    )
                else:
                    parsed = parse_reply(svc.output(generated), msg, svc.s)
                    preview["text"] = parsed.text
                    if parsed.meme_tag in svc.memes.labels:
                        preview["meme_tag"] = parsed.meme_tag
                if sandbox_abstain:
                    await svc.store.runtime.filters.update_audit(
                        audit_id, "abstained", "model_abstain"
                    )
                elif preview["text"] or preview.get("meme_tag"):
                    await svc.store.runtime.filters.update_audit(audit_id, "sent")
                else:
                    await svc.store.runtime.filters.update_audit(
                        audit_id, "cancelled", "empty_reply"
                    )
        except Rejected as exc:
            rejection = exc
            result["decisions"].setdefault("reason", str(exc))
            result["decisions"]["final_result"] = "blocked_or_unavailable"
            if not preview["text"]:
                preview["status"] = "blocked_or_unavailable"
        finally:
            if preview.get("provider") or preview.get("provider_diagnostics"):
                await svc.store.runtime.filters.record_generation(
                    audit_id,
                    thinking_level=preview.get("thinking_level"),
                    model_lane=preview.get("model_lane"),
                    provider_incident=preview.get("provider_incident"),
                    diagnostics=preview.get("provider_diagnostics"),
                )
            if rejection is not None:
                await svc.store.runtime.filters.update_audit(
                    audit_id,
                    "rejected",
                    rejection.audit_detail,
                    diagnostics=rejection.provider_diagnostics,
                )
        return result


class MemoryEdit(Input):
    id: str = Field(
        pattern=r"^(?:p:[1-9][0-9]{0,19}:(?:alias|preferences|boundaries|state)|e:[a-f0-9]{24})$"
    )
    version: int = Field(ge=0)
    action: Literal["edit", "delete", "unlock"]
    text: str = Field(default="", max_length=500)
    expires: float = Field(default=0, ge=0)


class MemoryMirrorConfig(Input):
    enabled: bool
    version: int = Field(ge=0)


@router.get("/api/sessions/{key}/memory")
async def dual_memory_detail(request: Request, key: str):
    svc = service(request)
    msg = await session_message(request, key)
    return await svc.sessions.dual.view(msg)


class MemoryQuery(Input):
    query: str = Field(default="", max_length=200)


@router.post("/api/sessions/{key}/memory/search")
async def dual_memory_search(request: Request, key: str):
    data = await body(request, MemoryQuery)
    svc = service(request)
    return await svc.sessions.dual.view(await session_message(request, key), data.query)


@router.put("/api/sessions/{key}/memory")
async def dual_memory_edit(request: Request, key: str):
    data = await body(request, MemoryEdit)
    svc, msg = service(request), await session_message(request, key)
    if (await svc.sessions.dual.view(msg))["version"] != data.version:
        raise Rejected("session_changed")
    if data.action == "edit":
        check_memory_text(data.text, svc.s)
        request.app.state.admin_auth.costly()
        with model_work("memory.entry_edit", independent=True):
            await svc.ai.screen(
                {"proposed_memory": data.text},
                svc.store.vault.digest("admin-memory", key),
                purpose="source",
                budget_lane="admin",
                audit_source="memory",
            )
    return await svc.sessions.dual.edit(
        msg, data.id, version=data.version, action=data.action, text=data.text, expires=data.expires
    )


@router.put("/api/sessions/{key}/memory/mirror")
async def dual_memory_mirror_config(request: Request, key: str):
    data = await body(request, MemoryMirrorConfig)
    svc = service(request)
    return await svc.memory_mirror.configure(
        await session_message(request, key), data.enabled, data.version
    )


@router.post("/api/sessions/{key}/memory/{direction}")
async def dual_memory_sync(request: Request, key: str, direction: Literal["push", "pull"]):
    request.app.state.admin_auth.costly()
    svc = service(request)
    return await svc.memory_mirror.sync(
        await session_message(request, key), push=direction == "push"
    )
