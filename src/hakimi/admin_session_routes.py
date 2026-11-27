"""Authenticated session inspection, compression, dual-memory governance, and sandbox routes."""

import asyncio
import re
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import ConfigDict, Field, ValidationError

from hakimi_plugin_sdk.bili_common import extract_bvid

from .admin_common import body, domain_lock, invalid_input_http_error, service
from .admin_dashboard_routes import Input
from .admission_policy import is_bot_hard_blocked
from .attention import can_watch_video, classify_attention
from .bot_server import prompt_context
from .compression_policy import CompressionConfig
from .generation_policy import Scenario, generation_plan
from .memory_compressor import check_memory_text
from .mentions import parse_reply
from .models import Message
from .outbound_actions import parse_actions
from .output_links import filter_reply_links
from .rich_message_parser import parse_message
from .security import Rejected, authorize, configured_secrets
from .work_usage import model_work

router = APIRouter(prefix="/admin/api")


class Sandbox(Input):
    identity: Literal["OWNER", "MEMBER", "BOT"]
    scene: Literal["group_at", "group_plain", "private"]
    text: str = Field(min_length=1, max_length=2000)
    group_id: str = Field(default="", pattern=r"^[0-9]{0,20}$")


class SummaryEdit(Input):
    text: str = Field(max_length=800)
    version: int = Field(ge=0)


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


class MemoryQuery(Input):
    query: str = Field(default="", max_length=200)


class SessionCompressOptions(Input):
    model_config = ConfigDict(extra="forbid", strict=True)
    retry_failed: bool = False


async def _optional_compress_options(request: Request) -> SessionCompressOptions:
    if request.headers.get("content-encoding", "identity") != "identity":
        raise HTTPException(415, "compressed requests are not supported")
    raw = bytearray()
    max_bytes = request.app.state.settings.max_body_bytes
    try:
        async with asyncio.timeout(5):
            async for chunk in request.stream():
                raw.extend(chunk)
                if len(raw) > max_bytes:
                    raise HTTPException(413, "request too large")
    except TimeoutError:
        raise HTTPException(408, "request timed out") from None
    if not raw or not raw.strip():
        return SessionCompressOptions()
    if request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
        raise HTTPException(415, "application/json required")
    from .app import json_object

    try:
        return SessionCompressOptions.model_validate(json_object(bytes(raw)))
    except ValidationError as exc:
        raise invalid_input_http_error(exc) from None


async def _dual_stats(svc, msg, enabled: bool) -> dict:
    dual_mgr = getattr(svc.sessions, "dual", None)
    if not (enabled and dual_mgr is not None):
        return {"dual_entries_count": 0, "dual_chars": 0}
    if hasattr(dual_mgr, "stats"):
        return await dual_mgr.stats(msg)
    if not hasattr(dual_mgr, "view"):
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


@router.get("/sessions")
async def sessions(request: Request):
    svc = service(request)
    entries = await svc.store.runtime.sessions()
    if not entries:
        return []
    use_fast_summary = hasattr(svc.sessions, "admin_list_summary")
    sem = asyncio.Semaphore(8)

    async def _summarize(key: str, msg: Message) -> dict:
        async with sem:
            if use_fast_summary:
                summary = await svc.sessions.admin_list_summary(msg)
                return {
                    "id": key,
                    "platform": msg.platform,
                    "kind": msg.kind,
                    "conversation_id": msg.conversation_id,
                    **summary,
                }
            snap = await svc.sessions.snapshot(msg)
            stats = await _dual_stats(svc, msg, bool(snap["enabled"]))
            return {
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

    return list(await asyncio.gather(*(_summarize(key, msg) for key, msg in entries)))


@router.get("/compression")
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


@router.put("/compression")
async def save_compression_config(request: Request):
    data = await body(request, CompressionConfig)
    async with domain_lock(request, "compression"):
        await service(request).store.runtime.set_compression(data)
        return await compression_config(request)


@router.delete("/compression")
async def reset_compression_config(request: Request):
    async with domain_lock(request, "compression"):
        await service(request).store.runtime.set_compression(None)
        return await compression_config(request)


async def session_message(request, key):
    if not re.fullmatch(r"[a-f0-9]{64}", key):
        raise HTTPException(404)
    return await service(request).store.runtime.session(key)


@router.get("/sessions/{key}")
async def session_detail(request: Request, key: str):
    svc = service(request)
    msg = await session_message(request, key)
    snap = await svc.sessions.snapshot(msg)
    # Admin needs summary and hot-window count, not automatic disclosure of raw conversations.
    snap["recent_count"] = len(snap.pop("recent_messages"))
    snap.update(await _dual_stats(svc, msg, bool(snap["enabled"])))
    return snap


@router.put("/sessions/{key}/summary")
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


@router.post("/sessions/{key}/reset")
async def reset_session(request: Request, key: str):
    svc = service(request)
    await svc.sessions.clear(await session_message(request, key))
    return {"status": "reset"}


@router.post("/sessions/{key}/compress")
async def compress_session(request: Request, key: str):
    options = await _optional_compress_options(request)
    msg = await session_message(request, key)
    request.app.state.admin_auth.costly()
    svc = service(request)
    requeued = 0
    if options.retry_failed:
        dual = getattr(svc.sessions, "dual", None)
        if dual is not None and hasattr(dual, "requeue_failed"):
            requeued = await dual.requeue_failed(msg)
    result = await svc.compressor.compress(msg, force=True)
    if options.retry_failed and isinstance(result, dict):
        return {**result, "requeued_failed": requeued}
    return result


@router.post("/sandbox")
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
                    try:
                        video = await svc.bili.video(bvid)
                    except Rejected as exc:
                        if str(exc) in {
                            "bilibili_module_unavailable",
                            "bilibili_unavailable",
                            "plugin_disconnected",
                            "plugin_rpc_timeout",
                        }:
                            rejection = exc
                            raise HTTPException(503, "bilibili_module_unavailable") from None
                        raise
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


@router.get("/sessions/{key}/memory")
async def dual_memory_detail(request: Request, key: str):
    svc = service(request)
    msg = await session_message(request, key)
    return await svc.sessions.dual.view(msg)


@router.post("/sessions/{key}/memory/search")
async def dual_memory_search(request: Request, key: str):
    data = await body(request, MemoryQuery)
    svc = service(request)
    return await svc.sessions.dual.view(await session_message(request, key), data.query)


@router.put("/sessions/{key}/memory")
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


@router.put("/sessions/{key}/memory/mirror")
async def dual_memory_mirror_config(request: Request, key: str):
    data = await body(request, MemoryMirrorConfig)
    svc = service(request)
    return await svc.memory_mirror.configure(
        await session_message(request, key), data.enabled, data.version
    )


@router.post("/sessions/{key}/memory/{direction}")
async def dual_memory_sync(request: Request, key: str, direction: Literal["push", "pull"]):
    request.app.state.admin_auth.costly()
    svc = service(request)
    return await svc.memory_mirror.sync(
        await session_message(request, key), push=direction == "push"
    )
