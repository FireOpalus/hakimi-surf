"""AI gating, budget checking, and model_work-isolated forwarding for PluginBus."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from ..budget_policy import PATROL_LANE, PROACTIVE_LANE, paid_lane_available
from ..config import Settings
from ..generation_policy import Scenario
from ..media import ModelImage
from ..roaming_logger import redact_public_summary
from ..screen_watch import decode_b64_frames
from ..security import Rejected, clean_text, safe_output
from ..work_usage import model_work
from .protocol import (
    _BVID_RE,
    _CODE_RE,
    _MAX_AI_CALLS_INFLIGHT,
    _SCOPED_SCENE_OWNERS,
    _bound_ai_payload,
    _scrub_json_secrets,
    _validate_plugin_response_schema,
    _validate_plugin_task_instruction,
)
from .proxy import _PluginSession

__all__ = ["PluginBusAIGatewayMixin"]


class PluginBusAIGatewayMixin:
    """Mixin handling ai.budget_check, ai.screen, ai.generate, and ai.video_analyze frames."""

    if TYPE_CHECKING:
        s: Settings
        store: Any
        service: Any

        def _all_secrets(self) -> list[str]: ...

        async def _send_frame(self, session: _PluginSession, payload: dict[str, Any]) -> None: ...

    def _validate_ai_lane(
        self, session: _PluginSession, raw_lane: Any, *, default: str = PROACTIVE_LANE
    ) -> str:
        lane = default if raw_lane is None else raw_lane
        if not isinstance(lane, str):
            raise Rejected("plugin_invalid_schema")
        if lane == PROACTIVE_LANE or (lane == PATROL_LANE and session.plugin_id == "bilibili_surf"):
            return lane
        raise Rejected("plugin_scope_rejected")

    async def _check_ai_budget(self, lane: str) -> tuple[bool, bool]:
        if not hasattr(self.store, "budget_status"):
            return True, False
        budget = await self.store.budget_status()
        hard_stop = bool(budget.get("hard_stop"))
        available = (not hard_stop) and bool(paid_lane_available(budget, lane))
        return available, hard_stop

    async def _spawn_ai_call(
        self,
        session: _PluginSession,
        req_id: str,
        *,
        result_type: str,
        task_name: str,
        worker: Callable[[], Awaitable[None]],
    ) -> None:
        if session.proxy is None or not session.proxy.enabled:
            await self._send_frame(
                session,
                {"type": result_type, "id": req_id, "ok": False, "error": "module_disabled"},
            )
            return
        if session.inflight_ai_calls >= _MAX_AI_CALLS_INFLIGHT:
            await self._send_frame(
                session,
                {"type": result_type, "id": req_id, "ok": False, "error": "ai_backpressure"},
            )
            return
        if session.closed or session.fiber.state != "active":
            return
        session.inflight_ai_calls += 1

        async def _runner() -> None:
            try:
                await worker()
            finally:
                session.inflight_ai_calls = max(0, session.inflight_ai_calls - 1)

        try:
            session.ctx.spawn(_runner(), name=task_name)
        except RuntimeError:
            session.inflight_ai_calls = max(0, session.inflight_ai_calls - 1)

    async def _handle_ai_budget_check(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)

        async def _run_budget_check() -> None:
            try:
                available, hard_stop = await self._check_ai_budget(lane)
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"available": available, "hard_stop": hard_stop},
                    },
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "budget_check_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-budget:{session.plugin_id}:{req_id[:16]}",
            worker=_run_budget_check,
        )

    async def _handle_ai_screen(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)
        raw_purpose = frame.get("purpose", "source")
        if raw_purpose is not None and raw_purpose != "source":
            raise Rejected("plugin_scope_rejected")
        raw_images = frame.get("images")
        if raw_images is not None and not isinstance(raw_images, list):
            raise Rejected("plugin_invalid_schema")
        if raw_images and "media" not in session.scopes:
            raise Rejected("plugin_scope_rejected")
        raw_payload = frame.get("payload")
        if not isinstance(raw_payload, dict):
            raise Rejected("plugin_invalid_schema")
        if not raw_payload:
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "empty_payload"},
            )
            return

        async def _run_screen() -> None:
            try:
                images: list[ModelImage] = (
                    decode_b64_frames(raw_images, 2, self.s, error_prefix="plugin")
                    if raw_images
                    else []
                )
                available, _ = await self._check_ai_budget(lane)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return
                ai = getattr(self.service, "ai", None)
                if ai is None or not hasattr(ai, "screen"):
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "ai_unavailable",
                        },
                    )
                    return
                max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                bounded_payload = _bound_ai_payload(raw_payload, self._all_secrets(), max_bytes)
                with model_work(f"plugin.{session.plugin_id}.screen", req_id, independent=True):
                    await ai.screen(
                        bounded_payload,
                        f"plugin:{session.plugin_id}",
                        purpose="source",
                        budget_lane=lane,
                        images=images or None,
                    )
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": {"passed": True},
                    },
                )
            except asyncio.CancelledError:
                raise
            except Rejected as exc:
                raw_err = str(exc).strip()
                code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_screen_rejected"
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": code},
                )
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "ai_screen_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-screen:{session.plugin_id}:{req_id[:16]}",
            worker=_run_screen,
        )

    async def _handle_ai_generate(self, session: _PluginSession, frame: dict[str, Any]) -> None:
        if "thinking_scenes" not in session.scopes or session.proxy is None:
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        scenario = frame.get("scenario") or frame.get("scene")
        if not isinstance(scenario, str) or scenario not in session.proxy.thinking_scene_keys:
            await self._send_frame(
                session,
                {
                    "type": "ai.result",
                    "id": req_id,
                    "ok": False,
                    "error": "plugin_scope_rejected",
                },
            )
            raise Rejected("plugin_scope_rejected")

        raw_images = frame.get("images")
        if raw_images is not None and not isinstance(raw_images, list):
            raise Rejected("plugin_invalid_schema")
        if raw_images and "media" not in session.scopes:
            await self._send_frame(
                session,
                {
                    "type": "ai.result",
                    "id": req_id,
                    "ok": False,
                    "error": "plugin_scope_rejected",
                },
            )
            raise Rejected("plugin_scope_rejected")

        lane = self._validate_ai_lane(session, frame.get("budget_lane"), default=PROACTIVE_LANE)

        raw_payload = frame.get("payload")
        raw_prompt = frame.get("prompt")
        if raw_payload is not None and not isinstance(raw_payload, dict):
            raise Rejected("plugin_invalid_schema")
        if raw_prompt is not None and not isinstance(raw_prompt, str):
            raise Rejected("plugin_invalid_schema")

        payload_in: dict[str, Any] = dict(raw_payload or {})
        if isinstance(raw_prompt, str) and raw_prompt.strip():
            payload_in["prompt"] = raw_prompt.strip()
        if not payload_in:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": "empty_payload"},
            )
            return

        try:
            resolved_scenario: Scenario | str = Scenario(scenario)
        except ValueError:
            resolved_scenario = scenario

        raw_task_instruction = frame.get("task_instruction")
        raw_response_schema = frame.get("response_schema")
        if isinstance(raw_task_instruction, str) and not clean_text(raw_task_instruction):
            raw_task_instruction = None
        if isinstance(resolved_scenario, Scenario) and (
            raw_task_instruction is not None or raw_response_schema is not None
        ):
            await self._send_frame(
                session,
                {
                    "type": "ai.result",
                    "id": req_id,
                    "ok": False,
                    "error": "plugin_scope_rejected",
                },
            )
            raise Rejected("plugin_scope_rejected")

        precheck_secrets = self._all_secrets()
        try:
            plugin_instruction = _validate_plugin_task_instruction(
                raw_task_instruction, precheck_secrets
            )
            validated_schema = _validate_plugin_response_schema(
                raw_response_schema, precheck_secrets
            )
        except Rejected as exc:
            await self._send_frame(
                session,
                {"type": "ai.result", "id": req_id, "ok": False, "error": str(exc)},
            )
            return

        is_bili_scene = scenario in _SCOPED_SCENE_OWNERS or scenario.startswith(
            ("bili_", "bilibili_")
        )
        out_limit = 4000 if is_bili_scene else 1800

        async def _run_generate() -> None:
            try:
                images: list[ModelImage] = (
                    decode_b64_frames(raw_images, 2, self.s, error_prefix="plugin")
                    if raw_images
                    else []
                )
                available, _ = await self._check_ai_budget(lane)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "ai.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return

                ai = getattr(self.service, "ai", None)
                if ai is None or not hasattr(ai, "generate"):
                    await self._send_frame(
                        session,
                        {"type": "ai.result", "id": req_id, "ok": False, "error": "ai_unavailable"},
                    )
                    return

                secrets_list = self._all_secrets()
                max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                bounded_payload = _bound_ai_payload(payload_in, secrets_list, max_bytes)
                bounded_payload["plugin_id"] = session.plugin_id

                extra_kwargs: dict[str, Any] = {}
                if plugin_instruction is not None:
                    extra_kwargs["plugin_instruction"] = plugin_instruction
                if validated_schema is not None:
                    extra_kwargs["response_schema"] = validated_schema

                with model_work(scenario, req_id, independent=True):
                    raw_text = await ai.generate(
                        bounded_payload,
                        f"plugin:{session.plugin_id}",
                        scenario=resolved_scenario,
                        budget_lane=lane,
                        images=images or None,
                        audit_source="production",
                        **extra_kwargs,
                    )
                clean_reply = safe_output(str(raw_text or ""), secrets_list, limit=out_limit)
                await self._send_frame(
                    session,
                    {"type": "ai.result", "id": req_id, "ok": True, "text": clean_reply},
                )
            except asyncio.CancelledError:
                raise
            except Rejected as exc:
                raw_err = str(exc).strip()
                code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_generate_rejected"
                await self._send_frame(
                    session, {"type": "ai.result", "id": req_id, "ok": False, "error": code}
                )
            except Exception:
                await self._send_frame(
                    session,
                    {"type": "ai.result", "id": req_id, "ok": False, "error": "ai_generate_failed"},
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="ai.result",
            task_name=f"plugin-ai-generate:{session.plugin_id}:{req_id[:16]}",
            worker=_run_generate,
        )

    async def _handle_ai_video_analyze(
        self, session: _PluginSession, frame: dict[str, Any]
    ) -> None:
        if (
            "thinking_scenes" not in session.scopes
            or session.proxy is None
            or session.plugin_id != "bilibili_surf"
        ):
            raise Rejected("plugin_scope_rejected")
        req_id = frame.get("id")
        if not isinstance(req_id, str) or not req_id.strip() or len(req_id) > 128:
            raise Rejected("plugin_invalid_schema")
        bvid = frame.get("bvid")
        material = frame.get("material")
        if not isinstance(bvid, str) or not isinstance(material, dict):
            raise Rejected("plugin_invalid_schema")
        if not _BVID_RE.fullmatch(bvid):
            await self._send_frame(
                session,
                {"type": "kernel.result", "id": req_id, "ok": False, "error": "invalid_bvid"},
            )
            return
        self._validate_ai_lane(session, frame.get("budget_lane"), default=PATROL_LANE)

        async def _run_video_analyze() -> None:
            try:
                available, _ = await self._check_ai_budget(PATROL_LANE)
                if not available:
                    await self._send_frame(
                        session,
                        {
                            "type": "kernel.result",
                            "id": req_id,
                            "ok": False,
                            "error": "budget_exhausted",
                        },
                    )
                    return

                secrets_list = self._all_secrets()
                subject = f"plugin:{session.plugin_id}"
                clean_material = _scrub_json_secrets(material, secrets_list)
                if not isinstance(clean_material, dict):
                    raise Rejected("plugin_invalid_schema")

                video_watch = getattr(self.service, "video_watch", None)
                if video_watch is not None and hasattr(video_watch, "analyze"):
                    with model_work(f"plugin.{session.plugin_id}.video", req_id, independent=True):
                        res = await video_watch.analyze(bvid, clean_material, subject, PATROL_LANE)
                    out_text = redact_public_summary(
                        safe_output(str(res.text or ""), secrets_list, limit=1000)
                    )
                    result_obj = {
                        "text": out_text,
                        "mode": str(res.mode),
                        "material_scope": str(res.material_scope),
                        "reason": res.reason,
                    }
                else:
                    ai = getattr(self.service, "ai", None)
                    if ai is None or not hasattr(ai, "screen") or not hasattr(ai, "generate"):
                        await self._send_frame(
                            session,
                            {
                                "type": "kernel.result",
                                "id": req_id,
                                "ok": False,
                                "error": "ai_unavailable",
                            },
                        )
                        return
                    max_bytes = int(getattr(self.s, "max_context_bytes", 48_000))
                    bounded_mat = _bound_ai_payload(clean_material, secrets_list, max_bytes)
                    with model_work(f"plugin.{session.plugin_id}.video", req_id, independent=True):
                        await ai.screen(
                            bounded_mat,
                            subject,
                            purpose="source",
                            budget_lane=PATROL_LANE,
                        )
                        raw_text = await ai.generate(
                            bounded_mat,
                            subject,
                            scenario=Scenario.BILI_LEARNING,
                            budget_lane=PATROL_LANE,
                            audit_source="production",
                        )
                    out_text = redact_public_summary(
                        safe_output(str(raw_text or ""), secrets_list, limit=1000)
                    )
                    result_obj = {
                        "text": out_text,
                        "mode": "text",
                        "material_scope": str(bounded_mat.get("material_scope") or "metadata_only"),
                        "reason": None,
                    }

                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": True,
                        "result": result_obj,
                    },
                )
            except asyncio.CancelledError:
                raise
            except Rejected as exc:
                raw_err = str(exc).strip()
                code = raw_err if _CODE_RE.fullmatch(raw_err) else "ai_video_analyze_rejected"
                await self._send_frame(
                    session,
                    {"type": "kernel.result", "id": req_id, "ok": False, "error": code},
                )
            except Exception:
                await self._send_frame(
                    session,
                    {
                        "type": "kernel.result",
                        "id": req_id,
                        "ok": False,
                        "error": "ai_video_analyze_failed",
                    },
                )

        await self._spawn_ai_call(
            session,
            req_id,
            result_type="kernel.result",
            task_name=f"plugin-ai-video:{session.plugin_id}:{req_id[:16]}",
            worker=_run_video_analyze,
        )
