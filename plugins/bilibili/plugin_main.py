"""Standalone hot-plug Bilibili surf & patrol remote plugin."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import signal
import time
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import httpx
from pydantic import ValidationError

from hakimi_plugin_sdk import (
    ActionSpec,
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
    ThinkingSceneSpec,
)

from .client import BiliClientError, BiliPublicClient
from .patrol import BiliPatrolEngine, BiliPolicy, CredentialUpdate

log = logging.getLogger("hakimi_plugin_bilibili")

BILIBILI_SCOPES: tuple[str, ...] = (
    "actions",
    "sensors",
    "thinking_scenes",
    "node_bridge",
    "owner_notify",
)
BILIBILI_CAPABILITIES: tuple[str, ...] = ("context_data", "patrol_entry")
BILIBILI_THINKING_SCENES: tuple[ThinkingSceneSpec, ...] = (
    ThinkingSceneSpec(
        key="bilibili_summary",
        label="视频摘要",
        output_cap="summary_output_tokens",
    ),
    ThinkingSceneSpec(
        key="bili_selection",
        label="候选挑选",
        output_tokens=1536,
    ),
    ThinkingSceneSpec(
        key="bili_learning",
        label="学习心得",
        output_tokens=4096,
    ),
)


class BilibiliPlugin:
    """Out-of-process Bilibili surf and small-account patrol plugin wrapper."""

    def __init__(
        self,
        config: PluginRuntimeConfig,
        *,
        http_client: httpx.AsyncClient | None = None,
        connect_factory: Any = None,
        clock: Callable[[], float] = time.time,
        public_clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self._owns_http = http_client is None
        self.http = (
            http_client
            if http_client is not None
            else httpx.AsyncClient(
                timeout=15.0,
                trust_env=False,
                follow_redirects=False,
                limits=httpx.Limits(max_connections=2),
            )
        )
        self.client = HakimiPluginClient(
            config,
            actions=(
                ActionSpec(
                    name="bili.patrol",
                    description=(
                        "去 B 站首页推荐和关注动态逛一圈，挑 1～2 个感兴趣的视频细看并记下冲浪槽点"
                    ),
                    arguments={},
                    origins=["planner", "rule", "timer", "legacy", "admin"],
                    effortful=True,
                    window="active",
                    budget_lane="patrol",
                    quotas=[{"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}],
                    aliases=[
                        ["bilibili_surf", "patrol", False],
                        ["bilibili_surf", "run_once", False],
                    ],
                    timeout_seconds=1500.0,
                    handler=self._handle_patrol_action,
                ),
            ),
            thinking_scenes=BILIBILI_THINKING_SCENES,
            initial_status={"autonomous": False, "backend": "local", "cn_delegate": False},
            connect_factory=connect_factory,
        )
        self.public_client = BiliPublicClient(
            self.client,
            self.http,
            clock=public_clock if public_clock is not None else time.monotonic,
            sleep=sleep,
        )
        self.engine = BiliPatrolEngine(
            self.client,
            self.http,
            clock=clock,
            sleep=sleep,
            public_client=self.public_client,
        )
        self._bind_handlers()

    def _bind_handlers(self) -> None:
        self.client.sensor(self.sensor_summary)
        self.client.on_connect(self._on_connect)
        self.client.on_switch(self._on_switch)
        self.client.on_node_state(self._on_node_state)

        self.client.rpc("bili.resolve_short")(self._rpc_resolve_short)
        self.client.rpc("bili.video")(self._rpc_video)
        self.client.rpc("bili.updates")(self._rpc_updates)
        self.client.rpc("bili.live_info")(self._rpc_live_info)
        self.client.rpc("bili.public_material")(self._rpc_public_material)
        self.client.rpc("bili.lookup_watched")(self._rpc_lookup_watched)
        self.client.rpc("bili.recent_surfed")(self._rpc_recent_surfed)
        self.client.rpc("bili.ensure_buvid4")(self._rpc_ensure_buvid4)
        self.client.rpc("bili.cover_thumb")(self._rpc_cover_thumb)

        self.client.rpc("bili.account.status")(self._rpc_account_status)
        self.client.rpc("bili.account.set_policy")(self._rpc_account_set_policy)
        self.client.rpc("bili.account.reset_policy")(self._rpc_account_reset_policy)
        self.client.rpc("bili.account.update_credentials")(self._rpc_account_update_credentials)
        self.client.rpc("bili.account.nav")(self._rpc_account_nav)
        self.client.rpc("bili.account.run_once")(self._rpc_account_run_once)
        self.client.rpc("bili.account.risk")(self._rpc_account_risk)

        self.client.rpc("context.read")(self._rpc_context_read)
        self.client.rpc("patrol.entry")(self._rpc_patrol_entry)

    async def close(self) -> None:
        await self.client.stop()
        if self._owns_http:
            await self.http.aclose()

    async def sensor_summary(self, _params: dict[str, Any] | None = None) -> str:
        """Format sensor line matching built-in BilibiliSurfModule."""
        recent_count = 0
        watched_count = 0
        uncertain_count = 0
        last_run_status = ""
        try:
            recent_count = len(await self.engine.recent_surfed(limit=4))
        except Exception:
            recent_count = 0
        try:
            st = await self.engine.status()
            watched_count = int(st.get("watched_count", 0) or 0)
            uncertain_count = int((st.get("watch_history_counts") or {}).get("uncertain", 0))
            runs = st.get("recent_runs") or []
            if runs and isinstance(runs[0], dict):
                last_run_status = str(runs[0].get("status", ""))
        except Exception:
            watched_count = max(watched_count, recent_count)

        state = "待机"
        if (
            self.public_client.blocked_until > self.public_client.clock()
            or self.engine.blocked_until > self.engine.clock()
        ):
            state = "冷却中"
        elif self.engine.policy.enabled:
            state = "巡逻开启"
        elif self.public_client.bilibili_enabled:
            state = "手动可看"
        else:
            state = "未启用"

        prefix = (
            "B站冲浪(CN专线)"
            if (self.public_client.node_connected and self.public_client.node_bili_delegate_enabled)
            else "B站冲浪"
        )
        parts = [f"{prefix}:{state}", f"近期:{recent_count}", f"已看:{watched_count}"]
        if uncertain_count:
            parts.append(f"待核实:{uncertain_count}")
        if last_run_status:
            parts.append(f"上次:{last_run_status}")
        return " ".join(parts)

    async def _on_connect(self, _welcome: dict[str, Any]) -> None:
        await self.engine.initialize()
        await self.engine._push_runtime_status()

    async def _on_switch(self, enabled: bool) -> None:
        flag = bool(enabled)
        self.engine.set_module_enabled(flag)
        if not flag:
            self.engine.policy_epoch += 1
        await self.engine._push_runtime_status()

    async def _on_node_state(self, connected: bool) -> None:
        self.engine.set_node_connected(bool(connected))
        await self.engine._push_runtime_status()

    async def _handle_patrol_action(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_args = params.get("arguments")
        args = dict(raw_args) if isinstance(raw_args, dict) else {}
        action_type = str(params.get("action_type") or args.get("action_type") or "").strip()
        if action_type == "status":
            summary = await self.sensor_summary()
            return {"status": "done", "reason": "ok", "details": {"summary": summary}}

        raw_slot = params.get("slot") or args.get("slot")
        if not raw_slot and params.get("ticket_id"):
            raw_slot = f"life-{params['ticket_id']}"
        slot = str(raw_slot)[:80] if isinstance(raw_slot, str) and raw_slot.strip() else None

        try:
            run = await self.engine.run_once(slot=slot)
        except BiliClientError as exc:
            return {
                "status": "gated_skip",
                "reason": exc.code,
                "details": {"summary": await self.sensor_summary()},
            }
        except PluginRpcError as exc:
            return {
                "status": "gated_skip",
                "reason": exc.code,
                "details": {"summary": await self.sensor_summary()},
            }

        summary = await self.sensor_summary()
        run_status = str(run.get("status") or "")
        run_reason = run.get("reason")
        if run.get("requires_review"):
            return {
                "status": "uncertain",
                "reason": str(run_reason or "bili_review_required"),
                "details": {"summary": summary, "result": run},
            }
        if run_status == "failed" and int(run.get("learned") or 0) > 0:
            return {
                "status": "done",
                "reason": str(run_reason or "patrol_partly_completed"),
                "details": {"summary": summary, "result": run},
            }
        if run_status not in {"done", "empty"}:
            return {
                "status": "gated_skip",
                "reason": str(run_reason or run_status or "patrol_unavailable"),
                "details": {"summary": summary, "result": run},
            }
        return {
            "status": "done",
            "reason": str(run_reason or run_status or "patrol_completed"),
            "details": {"summary": summary, "result": run},
        }

    async def _rpc_resolve_short(self, params: dict[str, Any]) -> dict[str, Any]:
        url = str(params.get("url") or "")
        bvid = await self.public_client.resolve_short(url)
        return {"bvid": bvid}

    async def _rpc_video(self, params: dict[str, Any]) -> dict[str, Any]:
        bvid = str(params.get("bvid") or "")
        return await self.public_client.video(bvid)

    async def _rpc_updates(self, params: dict[str, Any]) -> dict[str, Any]:
        mid = str(params.get("mid") or "")
        return {"videos": await self.public_client.updates(mid)}

    async def _rpc_live_info(self, params: dict[str, Any]) -> dict[str, Any]:
        room_id = str(params.get("room_id") or "")
        return await self.public_client.live_info(room_id)

    async def _rpc_public_material(self, params: dict[str, Any]) -> dict[str, Any]:
        video_obj = params.get("video") if isinstance(params.get("video"), dict) else params
        return await self.engine.public_material(video_obj)

    async def _rpc_lookup_watched(self, params: dict[str, Any]) -> dict[str, Any]:
        bvid = str(params.get("bvid") or "")
        raw_limit = params.get("limit", 360)
        limit = (
            int(raw_limit)
            if isinstance(raw_limit, int) and not isinstance(raw_limit, bool) and raw_limit > 0
            else 360
        )
        session_scope = str(params.get("session_scope") or "").strip()
        entry = await self.engine.lookup_watched(bvid, limit=limit, session_scope=session_scope)
        return {"found": entry is not None, "entry": entry}

    async def _rpc_recent_surfed(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_limit = params.get("limit", 4)
        raw_max_age = params.get("max_age", 172800.0)
        raw_now = params.get("now")
        limit = (
            int(raw_limit)
            if isinstance(raw_limit, int) and not isinstance(raw_limit, bool) and raw_limit > 0
            else 4
        )
        max_age = (
            float(raw_max_age)
            if isinstance(raw_max_age, (int, float))
            and not isinstance(raw_max_age, bool)
            and math.isfinite(float(raw_max_age))
            else 172800.0
        )
        now = (
            float(raw_now)
            if isinstance(raw_now, (int, float))
            and not isinstance(raw_now, bool)
            and math.isfinite(float(raw_now))
            else None
        )
        session_scope = str(params.get("session_scope") or "").strip()
        items = await self.engine.recent_surfed(
            limit=limit, max_age=max_age, now=now, session_scope=session_scope
        )
        return {"items": items}

    async def _rpc_ensure_buvid4(self, _params: dict[str, Any]) -> dict[str, Any]:
        creds = await self.engine.ensure_buvid4()
        return {
            "ok": True,
            "buvid4_configured": bool(creds.get("buvid4")),
            "credentials": {k: bool(v) for k, v in creds.items()},
        }

    async def _rpc_cover_thumb(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_url = params.get("cover_url")
        if not isinstance(raw_url, str) or not raw_url:
            raw_url = params.get("url")
        if not isinstance(raw_url, str) or not raw_url:
            return {"found": False, "thumbnail": None}
        thumb = await self.engine.fetch_cover_thumbnail(raw_url)
        return {"found": thumb is not None, "thumbnail": thumb}

    async def _rpc_account_status(self, _params: dict[str, Any]) -> dict[str, Any]:
        return await self.engine.status()

    async def _rpc_account_set_policy(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            policy = BiliPolicy.model_validate(params)
        except ValidationError as exc:
            raise PluginRpcError("invalid_admin_input", "invalid bilibili policy") from exc
        return await self.engine.set_policy(policy)

    async def _rpc_account_reset_policy(self, _params: dict[str, Any]) -> dict[str, Any]:
        return await self.engine.set_policy(None)

    async def _rpc_account_update_credentials(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            update = CredentialUpdate.model_validate(params)
        except ValidationError as exc:
            raise PluginRpcError("invalid_admin_input", "invalid bilibili credentials") from exc
        return await self.engine.update_credentials(update)

    async def _rpc_account_nav(self, _params: dict[str, Any]) -> dict[str, Any]:
        return await self.engine.nav()

    async def _rpc_account_run_once(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_slot = params.get("slot")
        slot = str(raw_slot)[:80] if isinstance(raw_slot, str) and raw_slot.strip() else None
        return await self.engine.run_once(slot=slot)

    async def _rpc_account_risk(self, params: dict[str, Any]) -> dict[str, Any]:
        reason = str(params.get("reason") or "bili_account_risk")
        return await self.engine.trigger_risk(reason)

    async def _rpc_context_read(self, params: dict[str, Any]) -> dict[str, Any]:
        raw_limit = params.get("limit", 4)
        limit = (
            int(raw_limit)
            if isinstance(raw_limit, int) and not isinstance(raw_limit, bool) and raw_limit > 0
            else 4
        )
        raw_now = params.get("now")
        now = (
            float(raw_now)
            if isinstance(raw_now, (int, float))
            and not isinstance(raw_now, bool)
            and math.isfinite(float(raw_now))
            else None
        )
        session_scope = str(params.get("session_scope") or "").strip()
        return {
            "recent_bili_surfed": await self.engine.recent_surfed(
                limit=limit, now=now, session_scope=session_scope
            )
        }

    async def _rpc_patrol_entry(self, params: dict[str, Any]) -> dict[str, Any]:
        bvid = str(params.get("bvid") or "")
        raw_limit = params.get("limit", 1200)
        limit = (
            int(raw_limit)
            if isinstance(raw_limit, int) and not isinstance(raw_limit, bool) and raw_limit > 0
            else 1200
        )
        session_scope = str(params.get("session_scope") or "").strip()
        entry = await self.engine.lookup_watched(bvid, limit=limit, session_scope=session_scope)
        return {
            "business_key": f"bili-account:{bvid}",
            "prior_patrol_notes": entry["prior_patrol_notes"] if entry else "",
            "entry": entry,
        }


def bilibili_config_from_env(**overrides: Any) -> PluginRuntimeConfig:
    """Build PluginRuntimeConfig for `bilibili_surf` from environment variables."""
    ws_url = str(
        overrides.pop("ws_url", None)
        or os.getenv("HAKIMI_PLUGIN_WS_URL")
        or "ws://127.0.0.1:12141/ws/plugin"
    )
    token = str(
        overrides.pop("token", None)
        or os.getenv("HAKIMI_PLUGIN_BILIBILI_TOKEN")
        or os.getenv("HAKIMI_PLUGIN_TOKEN")
        or ""
    ).strip()
    if not token:
        raise RuntimeError("HAKIMI_PLUGIN_BILIBILI_TOKEN (or HAKIMI_PLUGIN_TOKEN) is required")
    raw_region = (
        str(
            overrides.pop("node_region", None)
            or os.getenv("HAKIMI_PLUGIN_BILIBILI_NODE_REGION")
            or "KR"
        )
        .strip()
        .upper()
    )
    node_region: Literal["KR", "CN"] = "CN" if raw_region == "CN" else "KR"
    return PluginRuntimeConfig(
        ws_url=ws_url,
        plugin_id="bilibili_surf",
        token=token,
        display_name="B 站巡逻",
        description="账号、视频订阅与巡逻日记",
        scopes=BILIBILI_SCOPES,
        node_region=node_region,
        admin_panel="bili",
        attention_level="MEDIUM",
        capabilities=BILIBILI_CAPABILITIES,
        **overrides,
    )


def build_bilibili_plugin(
    config: PluginRuntimeConfig | None = None,
    **kwargs: Any,
) -> BilibiliPlugin:
    """Factory creating a BilibiliPlugin from explicit config or env defaults."""
    runtime_cfg = config if config is not None else bilibili_config_from_env()
    return BilibiliPlugin(runtime_cfg, **kwargs)


def build_bilibili_plugin_from_env(**overrides: Any) -> BilibiliPlugin:
    """Create a BilibiliPlugin instance from environment variables."""
    plugin_keys = {"http_client", "connect_factory", "clock", "public_clock", "sleep"}
    plugin_kwargs = {k: overrides.pop(k) for k in list(overrides) if k in plugin_keys}
    config = bilibili_config_from_env(**overrides)
    return build_bilibili_plugin(config, **plugin_kwargs)


async def main() -> None:
    """Async CLI entrypoint for the Bilibili remote plugin container."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    plugin = build_bilibili_plugin_from_env()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        asyncio.create_task(plugin.client.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await plugin.client.run_forever()
    finally:
        await plugin.close()


if __name__ == "__main__":
    asyncio.run(main())
