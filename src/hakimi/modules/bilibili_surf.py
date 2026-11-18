"""Bilibili surf capability module (KR local or CN remote_node, MEDIUM attention)."""

from __future__ import annotations

import inspect
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from ..config import Settings
from ..security import Rejected
from .actions import Action, ActionCall, Outcome
from .base import AttentionLevel, BaseModule, GoalResult, NodeRegion, sanitize_sensor_text

if TYPE_CHECKING:
    from ..bili_watcher import BiliWatcher
    from ..bilibili import Bilibili

BackendMode = Literal["local", "remote_node"]


class BilibiliSurfModule(BaseModule):
    """Wraps local BiliWatcher/Bilibili or delegates to a remote CN node fetcher."""

    module_id: str = "bilibili_surf"
    node_region: NodeRegion = "KR"
    attention_level: AttentionLevel = "MEDIUM"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        watcher: BiliWatcher | None = None,
        bili: Bilibili | None = None,
        backend_mode: BackendMode = "local",
        remote_fetcher: Callable[..., Any] | None = None,
        node_transport: Any | None = None,
    ) -> None:
        super().__init__(settings)
        if backend_mode not in ("local", "remote_node"):
            raise ValueError(f"Unsupported backend_mode: {backend_mode}")
        self.watcher = watcher
        self.bili = bili
        self.backend_mode: BackendMode = backend_mode
        self.remote_fetcher = remote_fetcher
        self.node_transport = node_transport

    def actions(self) -> tuple[Action, ...]:
        return (
            Action(
                name="bili.patrol",
                description="按既有订阅、凭证、冷却及预算执行一轮巡逻",
                budgeted=True,
                run=self.patrol,
                aliases=(("bilibili_surf", "patrol", False), ("bilibili_surf", "run_once", False)),
                unavailable=self.patrol_unavailable,
            ),
        )

    def patrol_unavailable(self) -> str | None:
        if self.backend_mode == "remote_node":
            return None if self.remote_fetcher else "remote_fetcher_unconfigured"
        if self.watcher is None or not self.watcher.policy.enabled:
            return "bili_patrol_disabled"
        if not self.watcher.creds.get("sessdata"):
            return "bili_credentials_missing"
        if self.watcher.blocked_until > self.watcher.clock():
            return "bili_account_cooling_down"
        return None

    async def patrol(self, call: ActionCall) -> Outcome:
        res = await self.dispatch_goal(
            {"action_type": "patrol", "slot": "life-" + call.ticket["id"]}
        )
        run = res.artifacts.get("result", {})
        status = run.get("status")
        if res.reason == "bili_invalid_receipt":
            return Outcome("uncertain", res.reason)
        if not res.ok or (self.backend_mode == "local" and status not in {"done", "empty"}):
            return Outcome(
                "gated_skip", res.reason or run.get("reason") or status or "patrol_unavailable"
            )
        return Outcome("done", res.reason or status or "patrol_completed")

    async def social_material(self, ticket: dict) -> str:
        if self.backend_mode == "remote_node":
            return await self.get_sensor_summary()
        return (await self.dispatch_goal({"id": ticket["id"], "action_type": "status"})).summary

    async def context_data(self, *, limit: int, now: float) -> dict:
        if self.watcher:
            return {"recent_bili_surfed": await self.watcher.recent_surfed(limit=limit, now=now)}
        return {}

    async def _call_remote(self, request: dict[str, Any]) -> Any:
        if self.remote_fetcher is None:
            return None
        sig = inspect.signature(self.remote_fetcher)
        positional = [
            p
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL)
        ]
        result = self.remote_fetcher(request) if positional else self.remote_fetcher()
        if inspect.isawaitable(result):
            return await result
        return result

    async def _raw_sensor_summary(self) -> str:
        paused_tag = " 主动暂停" if self.proactive_paused else ""
        if self.backend_mode == "remote_node":
            if self.remote_fetcher is None:
                return f"B站冲浪(remote_node):未连接{paused_tag}"
            data = await self._call_remote({"action": "status"})
            if isinstance(data, dict):
                raw_summary = data.get("summary")
                if raw_summary:
                    clean_sub = sanitize_sensor_text(str(raw_summary), self.settings, max_tokens=60)
                    return f"B站冲浪(CN):{clean_sub}{paused_tag}"
                status_str = sanitize_sensor_text(
                    str(data.get("status", "就绪")), self.settings, max_tokens=20
                )
                watched = int(data.get("watched_count", 0) or 0)
                patrols = int(data.get("patrol_count", 0) or 0)
                return f"B站冲浪(CN):{status_str} 巡逻:{patrols} 已看:{watched}{paused_tag}"
            clean_val = sanitize_sensor_text(str(data or "就绪"), self.settings, max_tokens=60)
            return f"B站冲浪(CN):{clean_val}{paused_tag}"

        recent_count = 0
        watched_count = 0
        last_run_status = ""
        if self.watcher is not None:
            try:
                recent = await self.watcher.recent_surfed(limit=4)
                recent_count = len(recent)
            except Exception:
                recent_count = 0
            try:
                st = await self.watcher.status()
                watched_count = int(st.get("watched_count", 0) or 0)
                runs = st.get("recent_runs") or []
                if runs and isinstance(runs[0], dict):
                    last_run_status = str(runs[0].get("status", ""))
            except Exception:
                watched_count = max(watched_count, recent_count)

        state = "待机"
        watcher_enabled = bool(getattr(getattr(self.watcher, "policy", None), "enabled", False))
        if self.bili is not None and getattr(self.bili, "blocked_until", 0.0) > time.monotonic():
            state = "冷却中"
        elif watcher_enabled or (
            self.settings is not None and getattr(self.settings, "bili_autonomous_enabled", False)
        ):
            state = "巡逻开启"
        elif self.settings is not None:
            if getattr(self.settings, "bilibili_enabled", False):
                state = "手动可看"
            else:
                state = "未启用"

        nt = (
            self.node_transport
            if self.node_transport is not None
            else getattr(self.bili, "node_transport", None)
        )
        if nt is None and self.watcher is not None:
            nt = getattr(getattr(self.watcher, "svc", None), "node_transport", None)
        delegate_enabled = (
            True
            if self.settings is None
            else bool(getattr(self.settings, "node_bili_delegate_enabled", True))
        )
        prefix = (
            "B站冲浪(CN专线)"
            if (nt is not None and nt.connected() and delegate_enabled)
            else "B站冲浪"
        )
        parts = [f"{prefix}:{state}", f"近期:{recent_count}", f"已看:{watched_count}"]
        if last_run_status:
            parts.append(f"上次:{last_run_status}")
        if self.proactive_paused:
            parts.append("主动暂停")
        return " ".join(parts)

    async def dispatch_goal(self, ticket: dict[str, Any]) -> GoalResult:
        action_type = str(ticket.get("action_type", "")).strip()
        if self.proactive_paused and action_type != "status":
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                summary="B站主动巡逻已因高专注锁定暂停",
                reason="proactive_paused",
            )
        if self.backend_mode == "remote_node":
            if self.remote_fetcher is None:
                return GoalResult(
                    ok=False,
                    module_id=self.module_id,
                    summary=await self.get_sensor_summary(),
                    reason="remote_fetcher_unconfigured",
                )
            res = await self._call_remote({"action": "dispatch", **ticket})
            if isinstance(res, dict):
                summary = sanitize_sensor_text(
                    str(res.get("summary", "")), self.settings, max_tokens=90
                )
                if not summary:
                    summary = await self.get_sensor_summary()
                reason = (
                    sanitize_sensor_text(str(res["reason"]), self.settings, max_tokens=20)
                    if res.get("reason") is not None
                    else None
                )
                return GoalResult(
                    ok=res.get("ok") is True,
                    module_id=self.module_id,
                    summary=summary,
                    artifacts={"backend_mode": "remote_node"},
                    reason=reason if isinstance(res.get("ok"), bool) else "bili_invalid_receipt",
                )
            return GoalResult(
                ok=False,
                module_id=self.module_id,
                reason="bili_invalid_receipt",
                summary="巡逻回执不完整",
                artifacts={"backend_mode": "remote_node"},
            )

        if action_type in {"patrol", "run_once"} and self.watcher is not None:
            slot = str(ticket["slot"])[:80] if ticket.get("slot") else None
            try:
                res = await self.watcher.run_once(slot=slot)
            except Rejected as exc:
                return GoalResult(
                    ok=False,
                    module_id=self.module_id,
                    summary=await self.get_sensor_summary(),
                    artifacts={"backend_mode": "local"},
                    reason=str(exc),
                )
            return GoalResult(
                ok=True,
                module_id=self.module_id,
                summary=await self.get_sensor_summary(),
                artifacts={
                    "backend_mode": "local",
                    "result": res if isinstance(res, dict) else {},
                },
            )
        return GoalResult(
            ok=True,
            module_id=self.module_id,
            summary=await self.get_sensor_summary(),
            artifacts={"backend_mode": "local", "action_type": action_type[:64]},
        )
