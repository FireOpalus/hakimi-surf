"""Phase 3 tests for PluginBus bilibili_surf extensions, AI RPCs, KV fallbacks, and hooks."""

from __future__ import annotations

import asyncio
import base64
import io
import json
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from PIL import Image
from pydantic import SecretStr

import hakimi.modules
from hakimi.app import create_app
from hakimi.generation_policy import Scenario
from hakimi.models import Message
from hakimi.modules.actions import ActionCall
from hakimi.modules.base import BaseModule
from hakimi.modules.bili_schedule import BiliScheduleOptions
from hakimi.modules.scheduling import LocalActivity
from hakimi.plugin_bus import (
    _NODE_BRIDGE_ALLOWED_ACTIONS,
    HOTPLUG_PILOT_PLUGIN_IDS,
    RESERVED_PLUGIN_IDS,
    PluginBus,
    RemoteModuleProxy,
)
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected, configured_secrets
from hakimi.video_watch import VideoAnalysis
from hakimi.world_state import MilestoneEvent
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN
from tests.test_v20_p4_cross_module_mc import FakeNodeTransport

BV1 = "BV1xx411c7mD"
BV2 = "BV1Q541167Qg"


def _make_png_b64() -> str:
    buf = io.BytesIO()
    Image.new("RGB", (4, 4), color=(255, 0, 0)).save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


_VALID_PNG_B64 = _make_png_b64()


class _Phase3Socket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.closed_codes: list[int] = []

    async def push(self, obj: dict[str, Any] | str) -> None:
        raw = obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)
        await self.incoming.put(raw)

    async def receive(self) -> str:
        item = await self.incoming.get()
        if item == "__EOF__":
            raise RuntimeError("eof")
        return item

    async def send(self, raw: str) -> None:
        await self.outgoing.put(json.loads(raw))

    async def close(self, code: int = 1000) -> None:
        self.closed_codes.append(code)
        await self.incoming.put("__EOF__")

    async def recv_until(self, predicate: Any, *, max_wait: float = 3.0) -> list[dict[str, Any]]:
        seen: list[dict[str, Any]] = []
        async with asyncio.timeout(max_wait):
            while True:
                msg = await self.outgoing.get()
                seen.append(msg)
                if predicate(msg):
                    return seen


def _phase3_settings(settings: Any, **update: Any) -> Any:
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            **update,
        }
    )


def _get_bus(svc: Any) -> PluginBus:
    bus = getattr(svc, "plugin_bus", None)
    return bus if bus is not None else svc.modules.systems["plugin_bus"].plugin_bus


@pytest.fixture
async def phase3_env(settings: Any):
    s = _phase3_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = _get_bus(svc)
        yield app, svc, bus, gw


def _bili_hello(**overrides: Any) -> dict[str, Any]:
    hello: dict[str, Any] = {
        "type": "plugin.hello",
        "protocol_version": 1,
        "display_name": "B 站巡逻",
        "description": "账号、视频订阅与巡逻日记",
        "scopes": ["actions", "sensors", "thinking_scenes", "node_bridge", "owner_notify"],
        "admin_panel": "bili",
        "attention_level": "MEDIUM",
        "capabilities": ["context_data", "patrol_entry"],
        "thinking_scenes": [
            {
                "key": "bilibili_summary",
                "label": "视频摘要",
                "output_cap": "summary_output_tokens",
            },
            {
                "key": "bili_selection",
                "label": "候选挑选",
                "output_tokens": 1536,
            },
            {
                "key": "bili_learning",
                "label": "学习心得",
                "output_tokens": 4096,
            },
        ],
        "actions": [
            {
                "name": "bili.patrol",
                "description": "去 B 站逛一圈",
                "budget_lane": "patrol",
                "effortful": True,
                "window": "active",
                "quotas": [{"name": "bili_patrols", "unlimited_flag": "bili_patrols_unlimited"}],
                "origins": ["planner", "rule", "timer", "legacy", "admin"],
                "aliases": [
                    ["bilibili_surf", "patrol", False],
                    ["bilibili_surf", "run_once", False],
                ],
                "timeout_seconds": 180.0,
            }
        ],
    }
    hello.update(overrides)
    return hello


def test_install_modules_expands_bilibili_surf_alias() -> None:
    seen_excludes: list[frozenset[str]] = []
    orig = hakimi.modules.platform_entries

    def _spy(exclude: set[str] | frozenset[str] = frozenset()):
        seen_excludes.append(frozenset(exclude))
        return iter([])

    hakimi.modules.platform_entries = _spy
    try:
        hakimi.modules.install_modules(
            SimpleNamespace(s=SimpleNamespace(external_plugins=("bilibili_surf",)))
        )
        hakimi.modules.install_modules(
            SimpleNamespace(s=SimpleNamespace(external_plugins="bilibili_surf"))
        )
        hakimi.modules.install_modules(
            SimpleNamespace(
                s=SimpleNamespace(external_plugins=("weather", "minecraft", "bilibili_surf"))
            )
        )
    finally:
        hakimi.modules.platform_entries = orig

    assert seen_excludes == [
        frozenset({"bilibili_surf", "bilibili"}),
        frozenset({"bilibili_surf", "bilibili"}),
        frozenset({"weather", "minecraft", "bilibili_surf", "bilibili"}),
    ]
    real_names = [
        p.name for p, _ in hakimi.modules.platform_entries(exclude={"bilibili", "bilibili_surf"})
    ]
    assert "bilibili" not in real_names
    assert {"qq", "weather", "minecraft"} <= set(real_names)


async def test_dynamic_reservation_scoped_panels_actions_scenes_and_node_bridge(
    phase3_env: Any,
) -> None:
    _, svc, bus, _ = phase3_env

    assert "bilibili_surf" not in RESERVED_PLUGIN_IDS
    assert HOTPLUG_PILOT_PLUGIN_IDS == frozenset({"weather", "minecraft", "bilibili_surf"})
    assert {"mc.status", "bili.request", "bili.cover_thumb"} <= _NODE_BRIDGE_ALLOWED_ACTIONS

    # Built-in BilibiliSurfModule blocks remote bilibili_surf registration
    assert svc.modules.get("bilibili_surf") is not None
    with pytest.raises(Rejected, match="reserved_plugin_id"):
        await bus.create_plugin("bilibili_surf", "B 站巡逻", ["actions", "sensors"])

    # Once built-in bilibili_surf is unregistered, bilibili_surf can be created
    assert svc.modules.unregister("bilibili_surf") is True
    token, _ = await bus.create_plugin(
        "bilibili_surf",
        "B 站巡逻",
        ["actions", "sensors", "thinking_scenes", "node_bridge", "owner_notify"],
        node_region="KR",
    )
    assert (await bus.authenticate("bilibili_surf", token)) is not None

    # Re-registering a built-in module re-blocks authenticate & serve dynamically
    class _BuiltinBili(BaseModule):
        module_id = "bilibili_surf"

        async def _raw_sensor_summary(self) -> str:
            return ""

    svc.modules.register(_BuiltinBili(svc.s))
    assert await bus.authenticate("bilibili_surf", token) is None
    sock_blocked = _Phase3Socket()
    await bus.serve(
        sock_blocked.receive,
        sock_blocked.send,
        sock_blocked.close,
        source_ip="127.0.0.1",
        plugin_id="bilibili_surf",
    )
    assert sock_blocked.closed_codes == [1008]
    assert svc.modules.unregister("bilibili_surf") is True

    await bus.create_plugin(
        "other_p3",
        "Other",
        ["actions", "sensors", "thinking_scenes", "node_bridge"],
    )

    # Scoped rejections for foreign plugin
    for bad_hello in (
        {"admin_panel": "bili"},
        {"actions": [{"name": "bili.patrol", "description": "Hijack"}]},
        {"actions": [{"name": "bilibili.read", "description": "Hijack"}]},
        {"actions": [{"name": "other_p3.run", "description": "Patrol", "budget_lane": "patrol"}]},
        {"thinking_scenes": [{"key": "bilibili_summary", "label": "Hijack"}]},
        {"thinking_scenes": [{"key": "bili_selection", "label": "Hijack"}]},
        {"thinking_scenes": [{"key": "bili_learning", "label": "Hijack"}]},
        {
            "actions": [
                {
                    "name": "other_p3.q",
                    "description": "Bad quota",
                    "quotas": [{"name": "total"}],
                }
            ]
        },
        {
            "actions": [
                {
                    "name": "other_p3.q2",
                    "description": "Bad quota flag",
                    "quotas": [{"name": "ok_cnt", "unlimited_flag": "missing_flag"}],
                }
            ]
        },
    ):
        s_bad = _Phase3Socket()
        t = asyncio.create_task(
            bus.serve(
                s_bad.receive, s_bad.send, s_bad.close, source_ip="127.0.0.1", plugin_id="other_p3"
            )
        )
        await s_bad.push(
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Other",
                "scopes": ["actions", "sensors", "thinking_scenes", "node_bridge"],
                **bad_hello,
            }
        )
        await asyncio.wait_for(t, 2.0)
        assert s_bad.closed_codes == [1008]

    # bilibili_surf cannot claim weather panel or overview
    for wrong_panel in ("weather", "overview"):
        s_bad2 = _Phase3Socket()
        t2 = asyncio.create_task(
            bus.serve(
                s_bad2.receive,
                s_bad2.send,
                s_bad2.close,
                source_ip="127.0.0.1",
                plugin_id="bilibili_surf",
            )
        )
        await s_bad2.push(_bili_hello(admin_panel=wrong_panel))
        await asyncio.wait_for(t2, 2.0)
        assert s_bad2.closed_codes == [1008]

    # Connect bilibili_surf + other_p3 + FakeNodeTransport
    nt = FakeNodeTransport(is_connected=True)
    svc.node_transport = nt
    secret = configured_secrets(svc.s)[0]
    nt.responses["bili.request"] = {
        "envelope": {
            "code": 0,
            "data": {
                "desc": ("这是一段超长简介，包含WBI地址与特殊符号 <tag> & " + "a" * 300),
                "wbi_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png?token=xyz",
                "secret_leak": f"oops={secret}",
                "nan_metric": float("nan"),
                "items": [{"idx": i, "txt": f"条目{i}"} for i in range(35)],
            },
        }
    }
    nt.responses["bili.cover_thumb"] = {
        "mime_type": "image/jpeg",
        "bytes": 4096,
        "data_b64": "A" * 1200,
    }
    nt.responses["bili.public_get"] = Rejected("bili_http_status", detail="412")

    b_sock = _Phase3Socket()
    o_sock = _Phase3Socket()
    b_run = asyncio.create_task(
        bus.serve(
            b_sock.receive,
            b_sock.send,
            b_sock.close,
            source_ip="127.0.0.1",
            plugin_id="bilibili_surf",
        )
    )
    o_run = asyncio.create_task(
        bus.serve(
            o_sock.receive,
            o_sock.send,
            o_sock.close,
            source_ip="127.0.0.1",
            plugin_id="other_p3",
        )
    )
    await b_sock.push(_bili_hello())
    await o_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Other",
            "scopes": ["node_bridge"],
        }
    )

    # Initial node.state sent right after plugin.ready
    b_init = await b_sock.recv_until(lambda m: m.get("type") == "node.state")
    assert any(m.get("type") == "plugin.ready" for m in b_init)
    assert b_init[-1] == {"type": "node.state", "connected": True}
    await o_sock.recv_until(lambda m: m.get("type") == "node.state")

    b_proxy = svc.modules.get("bilibili_surf")
    assert isinstance(b_proxy, RemoteModuleProxy)
    assert b_proxy.admin_panel == "bili"
    assert b_proxy.schedule_options is BiliScheduleOptions
    assert svc.modules.thinking_scenes["bilibili_summary"].output_cap == "summary_output_tokens"
    assert svc.modules.thinking_scenes["bili_selection"].output_tokens == 1536
    assert svc.modules.thinking_scenes["bili_learning"].output_tokens == 4096
    patrol_spec = svc.modules.catalog.actions["bili.patrol"]
    assert patrol_spec.budget_lane == "patrol"
    assert patrol_spec.quota_rules()[0].counter == "bili_patrols"
    assert patrol_spec.quota_rules()[0].unlimited_option == "bili_patrols_unlimited"

    # Per-plugin node_bridge allowlist: other_p3 and bili cannot cross domains
    await o_sock.push({"type": "node.call", "id": "o-mc", "action": "mc.status", "params": {}})
    assert (await o_sock.recv_until(lambda m: m.get("id") == "o-mc"))[-1][
        "error"
    ] == "node_action_forbidden"

    await b_sock.push({"type": "node.call", "id": "b-mc", "action": "mc.status", "params": {}})
    assert (await b_sock.recv_until(lambda m: m.get("id") == "b-mc"))[-1][
        "error"
    ] == "node_action_forbidden"

    # Lossless secret-scrubbing on bili.* node.call (preserves >90 tokens & >24 items)
    await b_sock.push({"type": "node.call", "id": "b-req", "action": "bili.request", "params": {}})
    b_req = (await b_sock.recv_until(lambda m: m.get("id") == "b-req"))[-1]
    assert b_req["ok"] is True
    env = b_req["result"]["envelope"]
    assert env["data"]["desc"].endswith("a" * 300)
    assert "<tag> & " in env["data"]["desc"]
    assert env["data"]["wbi_url"].endswith("?token=xyz")
    assert secret not in env["data"]["secret_leak"]
    assert "[REDACTED]" in env["data"]["secret_leak"]
    assert env["data"]["nan_metric"] == 0.0
    assert len(env["data"]["items"]) == 35

    await b_sock.push(
        {"type": "node.call", "id": "b-cov", "action": "bili.cover_thumb", "params": {}}
    )
    b_cov = (await b_sock.recv_until(lambda m: m.get("id") == "b-cov"))[-1]
    assert b_cov["ok"] is True and b_cov["result"]["data_b64"] == "A" * 1200

    # Rejected("bili_http_status", detail="412") propagates error + message="412"
    await b_sock.push(
        {"type": "node.call", "id": "b-412", "action": "bili.public_get", "params": {}}
    )
    b_412 = (await b_sock.recv_until(lambda m: m.get("id") == "b-412"))[-1]
    assert b_412 == {
        "type": "kernel.result",
        "id": "b-412",
        "ok": False,
        "error": "bili_http_status",
        "message": "412",
    }

    # Oversized response bounded cleanly by _max_frame_bytes
    nt.responses["bili.request"] = {"envelope": {"big": "x" * 300_000}}
    await b_sock.push({"type": "node.call", "id": "b-big", "action": "bili.request", "params": {}})
    b_big = (await b_sock.recv_until(lambda m: m.get("id") == "b-big"))[-1]
    assert b_big == {
        "type": "kernel.result",
        "id": "b-big",
        "ok": False,
        "error": "node_frame_too_large",
    }

    await b_sock.close(1000)
    await o_sock.close(1000)
    await asyncio.gather(b_run, o_run)


async def test_proxy_hooks_timeouts_status_and_raw_rpc(phase3_env: Any) -> None:
    _, svc, bus, _ = phase3_env
    svc.modules.unregister("bilibili_surf")
    bus.rpc_timeout = 0.05

    await bus.create_plugin(
        "bilibili_surf",
        "B 站巡逻",
        ["actions", "sensors", "thinking_scenes", "node_bridge", "owner_notify"],
    )
    await bus.create_plugin("ctx_plug", "Ctx", ["actions", "sensors"])

    b_sock = _Phase3Socket()
    c_sock = _Phase3Socket()
    b_run = asyncio.create_task(
        bus.serve(
            b_sock.receive,
            b_sock.send,
            b_sock.close,
            source_ip="127.0.0.1",
            plugin_id="bilibili_surf",
        )
    )
    c_run = asyncio.create_task(
        bus.serve(
            c_sock.receive,
            c_sock.send,
            c_sock.close,
            source_ip="127.0.0.1",
            plugin_id="ctx_plug",
        )
    )
    await b_sock.push(_bili_hello())
    await c_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Ctx",
            "scopes": ["actions", "sensors"],
            "capabilities": ["context_data"],
        }
    )
    await b_sock.recv_until(lambda m: m["type"] == "plugin.ready")
    await c_sock.recv_until(lambda m: m["type"] == "plugin.ready")

    b_proxy = svc.modules.get("bilibili_surf")
    c_proxy = svc.modules.get("ctx_plug")
    assert isinstance(b_proxy, RemoteModuleProxy)
    assert isinstance(c_proxy, RemoteModuleProxy)

    # 1. plugin.status updates unavailable_actions and current_activity
    patrol_action = svc.modules.catalog.actions["bili.patrol"]
    assert patrol_action.unavailable() is None
    assert await b_proxy.current_activity() is None

    await b_sock.push(
        {
            "type": "plugin.status",
            "id": "st-1",
            "status": {"autonomous": True},
            "unavailable_actions": {"bili.patrol": "bili_credentials_missing"},
            "current_activity": "bili_patrol",
        }
    )
    assert (await b_sock.recv_until(lambda m: m.get("id") == "st-1"))[-1]["ok"] is True
    assert patrol_action.unavailable() == "bili_credentials_missing"
    assert await b_proxy.current_activity() == "bili_patrol"

    await b_sock.push(
        {
            "type": "plugin.status",
            "id": "st-2",
            "unavailable_actions": {"bili.patrol": None},
            "current_activity": None,
        }
    )
    await b_sock.recv_until(lambda m: m.get("id") == "st-2")
    assert patrol_action.unavailable() is None
    assert await b_proxy.current_activity() is None

    # 2. Action.run and dispatch_goal honor timeout_seconds (> bus.rpc_timeout 0.05s)
    async def _slow_reply(delay: float, summary: str) -> None:
        req = (await b_sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
        await asyncio.sleep(delay)
        await b_sock.push(
            {
                "type": "rpc.result",
                "id": req["id"],
                "ok": True,
                "result": {"status": "done", "reason": "ok", "details": {"summary": summary}},
            }
        )

    responder = asyncio.create_task(_slow_reply(0.12, "巡逻完成"))
    out = await patrol_action.run(
        ActionCall({"origin": "planner"}, patrol_action.arguments(), 100.0)
    )
    await responder
    assert out.status == "done" and out.details["summary"] == "巡逻完成"

    responder2 = asyncio.create_task(_slow_reply(0.12, "goal完成"))
    goal_res = await b_proxy.dispatch_goal({"action_type": "patrol"})
    await responder2
    assert goal_res.ok is True and goal_res.summary == "goal完成"

    # 3. local_activity, local_focus, context_data, public_patrol_entry, call_raw_rpc
    act = b_proxy.local_activity({}, {"bili.patrol"}, True)
    assert isinstance(act, LocalActivity)
    assert act.intent["action_name"] == "bili.patrol"
    assert b_proxy.local_activity({}, {"bili.patrol"}, False) is None
    assert b_proxy.local_focus({"recent_bili_surfed": [{"bvid": BV1}]}, 9) == ("MEDIUM", "curious")
    assert b_proxy.local_focus({}, 20) == ("MEDIUM", "curious")
    assert b_proxy.local_focus({}, 10) is None
    assert c_proxy.local_activity({}, {"bili.patrol"}, True) is None
    assert c_proxy.local_focus({}, 20) is None

    # Delegate context_data & public_patrol_entry to service.bili_watcher
    async def _fake_recent_surfed(*, limit: int, now: float, message: Any = None):
        return [{"bvid": BV1, "notes": f"limit={limit}"}]

    async def _fake_lookup_watched(bvid: str, *, limit: int = 360):
        return {"prior_patrol_notes": f"notes:{bvid}:{limit}"}

    svc.bili_watcher.recent_surfed = _fake_recent_surfed
    svc.bili_watcher.lookup_watched = _fake_lookup_watched

    assert await b_proxy.context_data(limit=3, now=500.0) == {
        "recent_bili_surfed": [{"bvid": BV1, "notes": "limit=3"}]
    }
    pub_ev = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        payload={"bvid": BV1},
    )
    priv_ev = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        payload={"bvid": BV1, "private_first": True},
    )
    assert await b_proxy.public_patrol_entry(pub_ev) == (
        f"bili-account:{BV1}",
        f"notes:{BV1}:1200",
    )
    assert await b_proxy.public_patrol_entry(priv_ev) == (f"bili-account:{BV1}", "")

    # Non-bilibili plugin with context_data capability calls RPC context.read
    ctx_task = asyncio.create_task(c_proxy.context_data(limit=2, now=400.0))
    c_req = (await c_sock.recv_until(lambda m: m.get("method") == "context.read"))[-1]
    assert c_req["params"] == {"limit": 2, "now": 400.0}
    await c_sock.push(
        {
            "type": "rpc.result",
            "id": c_req["id"],
            "ok": True,
            "result": {"hint": f"ok {configured_secrets(svc.s)[0]}"},
        }
    )
    ctx_res = await ctx_task
    assert "[REDACTED]" in ctx_res["hint"]

    # call_raw_rpc preserves long lists/strings and rejects delivery.send
    with pytest.raises(Rejected, match="plugin_invalid_method"):
        await b_proxy.call_raw_rpc("delivery.send", {})
    raw_task = asyncio.create_task(b_proxy.call_raw_rpc("bili.video", {"bvid": BV1}))
    r_req = (await b_sock.recv_until(lambda m: m.get("method") == "bili.video"))[-1]
    await b_sock.push(
        {
            "type": "rpc.result",
            "id": r_req["id"],
            "ok": True,
            "result": {"bvid": BV1, "description": "long-" + "z" * 400, "tags": list(range(30))},
        }
    )
    raw_res = await raw_task
    assert len(raw_res["description"]) == 405
    assert len(raw_res["tags"]) == 30

    # 4. world.activity(kind="bili_reading") and world.event report_text + mood_longnick
    await svc.world_state.initialize()
    await b_sock.push(
        {
            "type": "world.activity",
            "id": "wa-bili",
            "kind": "bili_reading",
            "active": True,
            "updated_at": 1000.0,
        }
    )
    assert (await b_sock.recv_until(lambda m: m.get("id") == "wa-bili"))[-1]["accepted"] is True
    async with svc.store.lock:
        rhythm = await svc.store.one("SELECT bili_until FROM world_rhythm WHERE id=1")
    assert rhythm is not None and rhythm[0] == pytest.approx(4600.0)

    longnick_calls: list[str] = []

    async def _fake_apply_patrol(val: str):
        longnick_calls.append(val)
        return {"status": "updated"}

    svc.longnick.apply_patrol = _fake_apply_patrol
    msg_owner = Message(
        platform="onebot",
        bot_id="10000",
        event_id="ev-1",
        user_id="20000",
        kind="private",
        conversation_id="20000",
        text="hi",
        addressed=True,
        created_at=100.0,
    )
    await b_sock.push(
        {
            "type": "world.event",
            "id": "we-bili",
            "event_type": "bili_video_watched",
            "payload": {"bvid": BV1, "title": "测试视频"},
            "report_text": "看片笔记：https://bilibili.com/video/BV1 13800138000",
            "mood_longnick": "今天看到好玩的硬核科普",
        }
    )
    assert (await b_sock.recv_until(lambda m: m.get("id") == "we-bili"))[-1]["accepted"] is True
    reports = await svc.store.latest_reports(svc.store.subject(msg_owner))
    assert reports and "［链接省略］" in reports[0] and "［号码已隐去］" in reports[0]
    assert longnick_calls == ["今天看到好玩的硬核科普"]

    # 5. on_switch(False) bumps policy_epoch, clears bili_reading, and pushes module.switch
    prev_epoch = svc.bili_watcher.policy_epoch
    await svc.life_engine.set_module_enabled(b_proxy, False)
    sw = (await b_sock.recv_until(lambda m: m.get("type") == "module.switch"))[-1]
    assert sw == {"type": "module.switch", "enabled": False}
    assert svc.bili_watcher.policy_epoch == prev_epoch + 1
    async with svc.store.lock:
        rhythm_off = await svc.store.one("SELECT bili_until FROM world_rhythm WHERE id=1")
    assert rhythm_off is not None and rhythm_off[0] == 0

    # Non-bilibili plugin cannot send kind="bili_reading" -> 1008
    await c_sock.push(
        {
            "type": "world.activity",
            "kind": "bili_reading",
            "active": True,
            "updated_at": 1000.0,
        }
    )
    await asyncio.wait_for(c_run, 2.0)
    assert c_sock.closed_codes == [1008]

    await b_sock.close(1000)
    await asyncio.wait_for(b_run, 2.0)


async def test_nonblocking_ai_rpcs_reentrancy_and_video_analyze(phase3_env: Any) -> None:
    _, svc, bus, gw = phase3_env
    svc.modules.unregister("bilibili_surf")

    await bus.create_plugin(
        "bilibili_surf",
        "B 站巡逻",
        ["actions", "sensors", "thinking_scenes", "media", "node_bridge"],
    )
    await bus.create_plugin("other_ai", "OtherAI", ["thinking_scenes"])

    b_sock = _Phase3Socket()
    o_sock = _Phase3Socket()
    b_run = asyncio.create_task(
        bus.serve(
            b_sock.receive,
            b_sock.send,
            b_sock.close,
            source_ip="127.0.0.1",
            plugin_id="bilibili_surf",
        )
    )
    o_run = asyncio.create_task(
        bus.serve(
            o_sock.receive,
            o_sock.send,
            o_sock.close,
            source_ip="127.0.0.1",
            plugin_id="other_ai",
        )
    )
    await b_sock.push(
        _bili_hello(scopes=["actions", "sensors", "thinking_scenes", "media", "node_bridge"])
    )
    await o_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "OtherAI",
            "scopes": ["thinking_scenes"],
            "thinking_scenes": [{"key": "other_ai.brief", "label": "简报"}],
        }
    )
    await b_sock.recv_until(lambda m: m["type"] == "plugin.ready")
    await o_sock.recv_until(lambda m: m["type"] == "plugin.ready")

    b_proxy = svc.modules.get("bilibili_surf")
    assert isinstance(b_proxy, RemoteModuleProxy)

    # 1. ai.budget_check
    await b_sock.push({"type": "ai.budget_check", "id": "bc-1", "budget_lane": "patrol"})
    bc1 = (await b_sock.recv_until(lambda m: m.get("id") == "bc-1"))[-1]
    assert bc1 == {
        "type": "kernel.result",
        "id": "bc-1",
        "ok": True,
        "result": {"available": True, "hard_stop": False},
    }

    # Foreign plugin cannot use patrol lane -> 1008
    await o_sock.push({"type": "ai.budget_check", "id": "bc-bad", "budget_lane": "patrol"})
    await asyncio.wait_for(o_run, 2.0)
    assert o_sock.closed_codes == [1008]

    # 2. ai.screen preserves >90 tokens, scrubs secrets, passes images
    long_desc = "视频简介超长详情 " + "内容" * 120
    secret = configured_secrets(svc.s)[0]
    await b_sock.push(
        {
            "type": "ai.screen",
            "id": "scr-1",
            "budget_lane": "patrol",
            "purpose": "source",
            "payload": {"title": "测试", "description": f"{long_desc} {secret}"},
            "images": [{"mime_type": "image/png", "data": _VALID_PNG_B64}],
        }
    )
    scr1 = (await b_sock.recv_until(lambda m: m.get("id") == "scr-1"))[-1]
    assert scr1 == {"type": "kernel.result", "id": "scr-1", "ok": True, "result": {"passed": True}}
    last_screen = gw.calls[-1]
    assert last_screen[0] == "screen"
    assert last_screen[3]["budget_lane"] == "patrol"
    assert len(last_screen[3]["images"]) == 1
    assert long_desc in last_screen[1]["description"]
    assert secret not in last_screen[1]["description"]

    gw.block_purpose = "source"
    await b_sock.push(
        {
            "type": "ai.screen",
            "id": "scr-blk",
            "budget_lane": "patrol",
            "purpose": "source",
            "payload": {"title": "拦截"},
        }
    )
    scr_blk = (await b_sock.recv_until(lambda m: m.get("id") == "scr-blk"))[-1]
    assert scr_blk == {
        "type": "kernel.result",
        "id": "scr-blk",
        "ok": False,
        "error": "content_blocked",
    }
    gw.block_purpose = None

    # 3. ai.generate coerces Scenario, allows patrol lane, uses limit=4000 for bili_*
    orig_gen = gw.generate

    async def _long_gen(payload: dict[str, Any], subject: str, **kwargs: Any) -> str:
        await orig_gen(payload, subject, **kwargs)
        return "长输出" * 800  # 2400 chars (> 1800, <= 4000)

    gw.generate = _long_gen
    await b_sock.push(
        {
            "type": "ai.generate",
            "id": "gen-1",
            "scenario": "bilibili_summary",
            "budget_lane": "patrol",
            "payload": {"title": "摘要", "subtitle_excerpt": long_desc},
        }
    )
    gen1 = (await b_sock.recv_until(lambda m: m.get("id") == "gen-1"))[-1]
    assert gen1["ok"] is True and len(gen1["text"]) == 2400
    assert gw.calls[-1][3]["scenario"] is Scenario.BILIBILI_SUMMARY
    assert gw.calls[-1][3]["budget_lane"] == "patrol"
    assert long_desc in gw.calls[-1][1]["subtitle_excerpt"]
    gw.generate = orig_gen

    # 4. ai.video_analyze via video_watch WITH re-entrant RPC over the same WebSocket!
    reentrant_result: dict[str, Any] = {}

    async def _reentrant_analyze(bvid: str, material: dict[str, Any], subject: str, lane: str):
        assert bvid == BV1
        assert lane == "patrol"
        res = await b_proxy.call_raw_rpc("bili.ensure_buvid4", {})
        reentrant_result.update(res)
        return VideoAnalysis(
            text="视频讲解了核心内容 https://b23.tv/abc @up 13800138000",
            material_scope="video",
            mode="video",
            reason=None,
        )

    svc.video_watch = SimpleNamespace(analyze=_reentrant_analyze)
    await b_sock.push(
        {
            "type": "ai.video_analyze",
            "id": "va-1",
            "bvid": BV1,
            "material": {"title": "测试", "description": "desc"},
            "budget_lane": "patrol",
        }
    )
    # While ai.video_analyze is suspended on bili.ensure_buvid4, _reader() delivers the rpc.request!
    rpc_req = (await b_sock.recv_until(lambda m: m.get("method") == "bili.ensure_buvid4"))[-1]
    await b_sock.push(
        {"type": "rpc.result", "id": rpc_req["id"], "ok": True, "result": {"buvid4": "B4OK"}}
    )
    va1 = (await b_sock.recv_until(lambda m: m.get("id") == "va-1"))[-1]
    assert reentrant_result == {"buvid4": "B4OK"}
    assert va1["ok"] is True
    assert va1["result"]["mode"] == "video"
    assert va1["result"]["material_scope"] == "video"
    assert secret not in va1["result"]["text"]
    assert "［链接省略］" in va1["result"]["text"]
    assert "［号码已隐去］" in va1["result"]["text"]

    # Secret returned by video_watch.analyze is blocked by safe_output -> invalid_output
    async def _leaky_analyze(bvid: str, material: dict[str, Any], subject: str, lane: str):
        return VideoAnalysis(text=f"leaked {secret}", material_scope="video", mode="video")

    svc.video_watch = SimpleNamespace(analyze=_leaky_analyze)
    await b_sock.push(
        {
            "type": "ai.video_analyze",
            "id": "va-leak",
            "bvid": BV1,
            "material": {"title": "x"},
            "budget_lane": "patrol",
        }
    )
    va_leak = (await b_sock.recv_until(lambda m: m.get("id") == "va-leak"))[-1]
    assert va_leak == {
        "type": "kernel.result",
        "id": "va-leak",
        "ok": False,
        "error": "invalid_output",
    }

    # 5. ai.video_analyze fallback to ai.screen + ai.generate(BILI_LEARNING)
    svc.video_watch = None
    await b_sock.push(
        {
            "type": "ai.video_analyze",
            "id": "va-fb",
            "bvid": BV2,
            "material": {"title": "文字材料", "material_scope": "partial_subtitle"},
            "budget_lane": "patrol",
        }
    )
    va_fb = (await b_sock.recv_until(lambda m: m.get("id") == "va-fb"))[-1]
    assert va_fb["ok"] is True
    assert va_fb["result"]["mode"] == "text"
    assert va_fb["result"]["material_scope"] == "partial_subtitle"
    assert gw.calls[-1][0] == "generate"
    assert gw.calls[-1][3]["scenario"] is Scenario.BILI_LEARNING
    assert gw.calls[-1][3]["budget_lane"] == "patrol"

    # 6. inflight_ai_calls cap (>4 concurrent calls triggers ai_backpressure)
    gate = asyncio.Event()
    entered = 0

    async def _slow_gen(payload: dict[str, Any], subject: str, **kwargs: Any) -> str:
        nonlocal entered
        entered += 1
        await gate.wait()
        return "done"

    gw.generate = _slow_gen
    for idx in range(5):
        await b_sock.push(
            {
                "type": "ai.generate",
                "id": f"bp-{idx}",
                "scenario": "bili_selection",
                "budget_lane": "patrol",
                "payload": {"title": f"cand-{idx}"},
            }
        )
    bp = (await b_sock.recv_until(lambda m: m.get("id") == "bp-4"))[-1]
    assert bp == {"type": "ai.result", "id": "bp-4", "ok": False, "error": "ai_backpressure"}
    gate.set()
    for idx in range(4):
        res = (await b_sock.recv_until(lambda m, i=idx: m.get("id") == f"bp-{i}"))[-1]
        assert res["ok"] is True
    gw.generate = orig_gen

    # Invalid bvid returns invalid_bvid error
    await b_sock.push(
        {"type": "ai.video_analyze", "id": "va-bad", "bvid": "not_bv", "material": {"title": "x"}}
    )
    assert (await b_sock.recv_until(lambda m: m.get("id") == "va-bad"))[-1][
        "error"
    ] == "invalid_bvid"

    await b_sock.close(1000)
    await asyncio.wait_for(b_run, 2.0)


async def test_bili_kv_upgrades_fallbacks_secrets_and_64kb(settings: Any) -> None:
    from hakimi.modules.bili_watcher import SCHEMA as BILI_SCHEMA
    from hakimi.service import Service
    from hakimi.store import Store
    from tests.fakes import FakeBili, FakeSender

    s = _phase3_settings(
        settings,
        bili_autonomous_enabled=True,
        bili_sessdata=SecretStr("env_sess_123456"),
        bili_bili_jct=SecretStr("env_jct_123456"),
        bili_buvid3=SecretStr("env_b3_123456"),
        bili_buvid4=SecretStr("env_b4_123456"),
    )
    store = Store(s)
    await store.open()
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    svc.kernel = PluginKernel()
    svc.modules.unregister("bilibili_surf")
    bus = PluginBus(s, store, svc)
    await bus.initialize()
    await bus.create_plugin("bilibili_surf", "B 站巡逻", ["actions", "sensors"])

    # Seed legacy SQLite tables
    async with store.transaction():
        await store.db.executescript(BILI_SCHEMA)
        await store.db.execute(
            "INSERT OR REPLACE INTO admin_config(key, payload) VALUES ('bili_account', ?)",
            (
                store.vault.seal(
                    "admin:bili_account",
                    {
                        "enabled": True,
                        "allow_like": True,
                        "allow_favorite": False,
                        "favorite_folder_id": "",
                        "daily_action_limit": 2,
                        "times": ["12:00"],
                    },
                ),
            ),
        )
        await store.db.execute(
            "INSERT OR REPLACE INTO bili_credentials(id, payload) VALUES (1, ?)",
            (
                store.vault.seal(
                    "bili-credentials",
                    {
                        "sessdata": "sql_sess_987654",
                        "bili_jct": "sql_jct_987654",
                        "buvid3": "sql_b3_987654",
                        "buvid4": "",
                    },
                ),
            ),
        )
        await store.db.execute(
            "INSERT OR REPLACE INTO bili_account_state"
            "(id, blocked_until, last_error, checked_at, account) "
            "VALUES (1, ?, ?, ?, ?)",
            (
                12345.0,
                "bili_account_risk",
                12000.0,
                store.vault.seal("bili-account", {"mid": "123456"}),
            ),
        )
        await store.db.execute(
            "INSERT OR REPLACE INTO bili_device_state(id, fingerprint, attempted_at, status) "
            "VALUES (1, 'fp-abc', 11000.0, 'ready')"
        )
        await store.db.execute(
            "INSERT INTO bili_patrol_runs(id, day, slot, status, created, reason) "
            "VALUES ('r1', '2026-11-20', 'manual', 'done', 100.0, NULL)"
        )
        await store.db.execute(
            "INSERT INTO bili_watched_history(bvid, run_id, status, created, summary) "
            "VALUES (?, 'r1', 'done', 100.0, ?)",
            (
                BV1,
                store.vault.seal(
                    "bili-learn:" + BV1, "学习心得 https://b23.tv/x @alice 13800138000"
                ),
            ),
        )
        await store.db.execute(
            "INSERT INTO bili_actions(id, day, bvid, kind, status, created) "
            "VALUES ('a1', '2026-11-20', ?, 'like', 'done', 101.0)",
            (BV1,),
        )

    sock = _Phase3Socket()
    runner = asyncio.create_task(
        bus.serve(
            sock.receive,
            sock.send,
            sock.close,
            source_ip="127.0.0.1",
            plugin_id="bilibili_surf",
        )
    )
    await sock.push(_bili_hello(scopes=["actions", "sensors"], thinking_scenes=[], actions=[]))
    await sock.recv_until(lambda m: m["type"] == "plugin.ready")

    # 1. Fallbacks for env_defaults, policy, credentials, account_state, watched_seed
    await sock.push({"type": "state.get", "id": "g-env", "key": "env_defaults"})
    g_env = (await sock.recv_until(lambda m: m.get("id") == "g-env"))[-1]["result"]
    assert g_env["found"] is True
    assert g_env["value"]["enabled"] is True
    assert g_env["value"]["bot_timezone"] == s.bot_timezone
    assert g_env["value"]["bilibili_enabled"] is True

    await sock.push({"type": "state.get", "id": "g-pol", "key": "policy"})
    g_pol = (await sock.recv_until(lambda m: m.get("id") == "g-pol"))[-1]["result"]
    assert g_pol["found"] is True
    assert g_pol["value"]["allow_like"] is True
    assert "times" not in g_pol["value"]

    await sock.push({"type": "state.get", "id": "g-cred", "key": "credentials"})
    g_cred = (await sock.recv_until(lambda m: m.get("id") == "g-cred"))[-1]["result"]
    assert g_cred == {
        "found": True,
        "value": {
            "creds": {
                "sessdata": "sql_sess_987654",
                "bili_jct": "sql_jct_987654",
                "buvid3": "sql_b3_987654",
                "buvid4": "",
            },
            "source": "sqlite",
        },
    }

    # Remove SQLite creds row -> falls back to env cookies
    async with store.transaction():
        await store.db.execute("DELETE FROM bili_credentials")
    await sock.push({"type": "state.get", "id": "g-cred-env", "key": "credentials"})
    g_cred_env = (await sock.recv_until(lambda m: m.get("id") == "g-cred-env"))[-1]["result"]
    assert g_cred_env["value"]["source"] == "env"
    assert g_cred_env["value"]["creds"]["sessdata"] == "env_sess_123456"

    await sock.push({"type": "state.get", "id": "g-acc", "key": "account_state"})
    g_acc = (await sock.recv_until(lambda m: m.get("id") == "g-acc"))[-1]["result"]
    assert g_acc["value"] == {
        "blocked_until": 12345.0,
        "last_error": "bili_account_risk",
        "checked_at": 12000.0,
        "account": {"mid": "123456"},
        "buvid4_device": {"fingerprint": "fp-abc", "attempted_at": 11000.0, "status": "ready"},
    }

    await sock.push({"type": "state.get", "id": "g-seed", "key": "watched_seed"})
    g_seed = (await sock.recv_until(lambda m: m.get("id") == "g-seed"))[-1]["result"]
    assert g_seed["found"] is True
    assert len(g_seed["value"]["watched"]) == 1
    assert g_seed["value"]["watched"][0]["bvid"] == BV1
    assert g_seed["value"]["watched"][0]["actions"] == ["like"]
    assert "［链接省略］" in g_seed["value"]["watched"][0]["summary"]
    assert "［号码已隐去］" in g_seed["value"]["watched"][0]["summary"]
    assert g_seed["value"]["runs"][0]["id"] == "r1"
    assert g_seed["value"]["actions"][0]["kind"] == "like"

    # 2. 65536 KV limit for bilibili_surf
    await sock.push(
        {"type": "state.set", "id": "s-40k", "key": "watched_index", "value": {"d": "x" * 40_000}}
    )
    assert (await sock.recv_until(lambda m: m.get("id") == "s-40k"))[-1]["ok"] is True
    await sock.push(
        {"type": "state.set", "id": "s-70k", "key": "watched_index", "value": {"d": "x" * 70_000}}
    )
    assert (await sock.recv_until(lambda m: m.get("id") == "s-70k"))[-1][
        "error"
    ] == "state_value_too_large"

    # 3. state.set("credentials") registers extra secrets and bumps credentials_epoch
    prev_epoch = svc.bili_watcher.credentials_epoch
    await sock.push(
        {
            "type": "state.set",
            "id": "s-cred",
            "key": "credentials",
            "value": {
                "creds": {
                    "sessdata": "new_runtime_sess_secret",
                    "bili_jct": "new_runtime_jct_secret",
                    "buvid3": "",
                    "buvid4": "new_runtime_b4_secret",
                },
                "source": "sqlite",
            },
        }
    )
    assert (await sock.recv_until(lambda m: m.get("id") == "s-cred"))[-1]["ok"] is True
    assert svc.bili_watcher.credentials_epoch == prev_epoch + 1
    assert "new_runtime_sess_secret" in configured_secrets(svc.s)
    assert "new_runtime_sess_secret" in svc.secrets

    # 4. state.delete("policy") removes both plugin_kv_state and admin_config['bili_account']
    await sock.push({"type": "state.delete", "id": "d-pol", "key": "policy"})
    assert (await sock.recv_until(lambda m: m.get("id") == "d-pol"))[-1]["result"] == {
        "deleted": True
    }
    await sock.push({"type": "state.get", "id": "g-pol-after", "key": "policy"})
    assert (await sock.recv_until(lambda m: m.get("id") == "g-pol-after"))[-1]["result"] == {
        "found": False,
        "value": None,
    }

    await sock.close(1000)
    await asyncio.wait_for(runner, 2.0)
    await svc.stop()


async def test_budget_exhausted_hard_stop_for_ai_rpcs(settings: Any) -> None:
    from hakimi.service import Service
    from hakimi.store import Store
    from tests.fakes import FakeBili, FakeSender

    s = _phase3_settings(settings, daily_usd=Decimal("0"))
    store = Store(s)
    await store.open()
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    svc.kernel = PluginKernel()
    svc.modules.unregister("bilibili_surf")
    bus = PluginBus(s, store, svc)
    await bus.initialize()
    await bus.create_plugin("bilibili_surf", "B 站巡逻", ["actions", "sensors", "thinking_scenes"])

    sock = _Phase3Socket()
    runner = asyncio.create_task(
        bus.serve(
            sock.receive, sock.send, sock.close, source_ip="127.0.0.1", plugin_id="bilibili_surf"
        )
    )
    await sock.push(_bili_hello(scopes=["actions", "sensors", "thinking_scenes"]))
    await sock.recv_until(lambda m: m["type"] == "plugin.ready")

    await sock.push({"type": "ai.budget_check", "id": "bc-zero", "budget_lane": "patrol"})
    bc = (await sock.recv_until(lambda m: m.get("id") == "bc-zero"))[-1]
    assert bc["result"] == {"available": False, "hard_stop": True}

    await sock.push(
        {"type": "ai.screen", "id": "sc-zero", "budget_lane": "patrol", "payload": {"title": "x"}}
    )
    assert (await sock.recv_until(lambda m: m.get("id") == "sc-zero"))[-1][
        "error"
    ] == "budget_exhausted"

    await sock.push(
        {
            "type": "ai.video_analyze",
            "id": "va-zero",
            "bvid": BV1,
            "material": {"title": "x"},
            "budget_lane": "patrol",
        }
    )
    assert (await sock.recv_until(lambda m: m.get("id") == "va-zero"))[-1][
        "error"
    ] == "budget_exhausted"

    await sock.close(1000)
    await asyncio.wait_for(runner, 2.0)
    await svc.stop()
