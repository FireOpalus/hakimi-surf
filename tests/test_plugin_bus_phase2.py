"""Phase 2 tests for Contract G: PluginBus extensions, KV state, node_bridge, and module exclude."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

import hakimi.modules
from hakimi.app import create_app
from hakimi.modules.base import BaseModule, GoalResult
from hakimi.plugin_admin import _VALID_SCOPE_SET as ADMIN_VALID_SCOPES
from hakimi.plugin_bus import (
    RESERVED_PLUGIN_IDS,
    VALID_SCOPES,
    PluginBus,
    RemoteModuleProxy,
)
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected, configured_secrets
from hakimi.world_state import MilestoneEvent
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN


class _Phase2Socket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict] = asyncio.Queue()
        self.closed_codes: list[int] = []

    async def push(self, obj: dict | str) -> None:
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

    async def recv_until(self, predicate, *, max_wait: float = 2.0) -> list[dict]:
        seen: list[dict] = []
        async with asyncio.timeout(max_wait):
            while True:
                msg = await self.outgoing.get()
                seen.append(msg)
                if predicate(msg):
                    return seen


class _DummyNodeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.responses: dict[str, dict | Exception] = {}

    async def call(self, action: str, params: dict) -> dict:
        self.calls.append((action, params))
        item = self.responses.get(action)
        if isinstance(item, Exception):
            raise item
        if isinstance(item, dict):
            return item
        return {"ok": True, "action": action}


def _phase2_settings(settings, **update):
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            **update,
        }
    )


def _get_bus(svc) -> PluginBus:
    bus = getattr(svc, "plugin_bus", None)
    return bus if bus is not None else svc.modules.systems["plugin_bus"].plugin_bus


@pytest.fixture
async def phase2_env(settings):
    s = _phase2_settings(settings)
    gw = FakeGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        if getattr(svc, "kernel", None) is None:
            svc.kernel = PluginKernel()
        bus = _get_bus(svc)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            yield app, svc, bus, client


def test_platform_entries_exclude_and_install_modules_hook():
    all_names = [p.name for p, _ in hakimi.modules.platform_entries()]
    assert {"qq", "bilibili", "weather", "minecraft"} <= set(all_names)

    filtered = [
        p.name for p, _ in hakimi.modules.platform_entries(exclude={"weather", "minecraft"})
    ]
    assert "weather" not in filtered
    assert "minecraft" not in filtered
    assert {"qq", "bilibili"} <= set(filtered)

    registered: list[str] = []

    class _FakeRegistry:
        def register(self, mod: BaseModule) -> None:
            registered.append(mod.module_id)

    fake_svc_empty = SimpleNamespace(
        s=SimpleNamespace(weather_enabled=False, weather_latitude=None, weather_longitude=None),
        http=None,
        world_state=None,
        node_transport=None,
        bili=None,
        background=lambda coro: coro,
        modules=_FakeRegistry(),
    )
    # Without external_plugins, weather & minecraft are installed (same as legacy behavior).
    registered.clear()
    try:
        hakimi.modules.install_modules(fake_svc_empty)
    except Exception:
        pass

    # Verify with monkeypatched platform_entries so we test install_modules pass-through cleanly.
    seen_excludes: list[frozenset[str]] = []

    class _DummyMod(BaseModule):
        module_id = "dummy"

        async def _raw_sensor_summary(self) -> str:
            return ""

    orig = hakimi.modules.platform_entries

    def _spy(exclude=frozenset()):
        seen_excludes.append(frozenset(exclude))
        return iter([])

    hakimi.modules.platform_entries = _spy
    try:
        hakimi.modules.install_modules(
            SimpleNamespace(s=SimpleNamespace(external_plugins=("weather", "minecraft")))
        )
        hakimi.modules.install_modules(SimpleNamespace(s=SimpleNamespace()))
    finally:
        hakimi.modules.platform_entries = orig

    assert seen_excludes == [frozenset({"weather", "minecraft"}), frozenset()]


async def test_valid_scopes_sync_and_dynamic_reservation_guard(phase2_env):
    _, svc, bus, client = phase2_env

    assert VALID_SCOPES == (
        "actions",
        "sensors",
        "thinking_scenes",
        "chat",
        "media",
        "owner_notify",
        "node_bridge",
    )
    assert ADMIN_VALID_SCOPES == frozenset(VALID_SCOPES)
    ov = (await client.get("/admin/api/plugins")).json()
    assert ov["allowed_scopes_catalog"] == list(VALID_SCOPES)

    # Built-in weather & minecraft are currently registered -> dynamically reserved!
    assert "weather" not in RESERVED_PLUGIN_IDS
    assert "minecraft" not in RESERVED_PLUGIN_IDS
    assert "qq_social" in RESERVED_PLUGIN_IDS
    assert svc.modules.get("weather") is not None
    assert svc.modules.get("minecraft") is not None

    with pytest.raises(Rejected, match="reserved_plugin_id"):
        await bus.create_plugin("weather", "Weather", ["actions", "sensors"])
    with pytest.raises(Rejected, match="reserved_plugin_id"):
        await bus.create_plugin("minecraft", "MC", ["actions", "sensors", "node_bridge"])

    resp_conflict = await client.post(
        "/admin/api/plugins",
        json={"plugin_id": "weather", "display_name": "Weather", "scopes": ["actions"]},
    )
    assert resp_conflict.status_code == 400
    assert resp_conflict.json()["detail"] == "reserved_plugin_id"

    # Unregister built-in modules (simulating external_plugins=("weather", "minecraft"))
    assert svc.modules.unregister("weather") is True
    assert svc.modules.unregister("minecraft") is True

    w_res = await client.post(
        "/admin/api/plugins",
        json={
            "plugin_id": "weather",
            "display_name": "天气观测",
            "scopes": ["actions", "sensors"],
            "node_region": "KR",
        },
    )
    assert w_res.status_code == 200
    w_token = w_res.json()["token"]

    m_res = await client.post(
        "/admin/api/plugins",
        json={
            "plugin_id": "minecraft",
            "display_name": "Minecraft",
            "scopes": ["actions", "sensors", "node_bridge"],
            "node_region": "CN",
        },
    )
    assert m_res.status_code == 200
    assert (await bus.authenticate("weather", w_token))["plugin_id"] == "weather"

    # If a built-in module is registered again later, authenticate & serve reject weather!
    class _BuiltinWeather(BaseModule):
        module_id = "weather"

        async def _raw_sensor_summary(self) -> str:
            return ""

    svc.modules.register(_BuiltinWeather(svc.s))
    assert await bus.authenticate("weather", w_token) is None
    sock = _Phase2Socket()
    await bus.serve(sock.receive, sock.send, sock.close, source_ip="127.0.0.1", plugin_id="weather")
    assert sock.closed_codes == [1008]


async def test_prefix_admin_panel_query_aliases_welcome_and_outcome(phase2_env):
    _, svc, bus, _ = phase2_env
    svc.modules.unregister("weather")
    svc.modules.unregister("minecraft")

    await bus.create_plugin("weather", "天气观测", ["actions", "sensors"], node_region="KR")
    await bus.create_plugin(
        "minecraft", "Minecraft", ["actions", "sensors", "node_bridge"], node_region="CN"
    )
    await bus.create_plugin("other_plug", "Other", ["actions", "sensors"])

    # 1. Foreign plugin cannot hijack weather.*/mc.*/minecraft.* prefixes or core admin_panel
    for pid, bad_hello in (
        (
            "other_plug",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Bad",
                "scopes": ["actions"],
                "actions": [{"name": "weather.read", "description": "Hijack"}],
            },
        ),
        (
            "other_plug",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Bad",
                "scopes": ["actions"],
                "actions": [{"name": "mc.mine", "description": "Hijack"}],
            },
        ),
        (
            "other_plug",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Bad",
                "scopes": ["actions"],
                "actions": [{"name": "minecraft.go", "description": "Hijack"}],
            },
        ),
        (
            "other_plug",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Bad",
                "scopes": ["sensors"],
                "admin_panel": "overview",
            },
        ),
        (
            "other_plug",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Bad",
                "scopes": ["sensors"],
                "admin_panel": "weather",
            },
        ),
        (
            "weather",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Weather",
                "scopes": ["actions"],
                "actions": [{"name": "mc.status", "description": "Cross"}],
            },
        ),
        (
            "weather",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "Weather",
                "scopes": ["actions"],
                "actions": [
                    {
                        "name": "weather.bad_query",
                        "description": "Invalid query",
                        "origins": ["query"],
                        "arguments": {"city": {"type": "string"}},
                    }
                ],
            },
        ),
        (
            "minecraft",
            {
                "type": "plugin.hello",
                "protocol_version": 1,
                "display_name": "MC",
                "scopes": ["actions"],
                "actions": [
                    {
                        "name": "mc.bad_alias",
                        "description": "Wrong target_module alias",
                        "aliases": [["weather", "mine", False]],
                    }
                ],
            },
        ),
    ):
        s_bad = _Phase2Socket()
        t = asyncio.create_task(
            bus.serve(s_bad.receive, s_bad.send, s_bad.close, source_ip="127.0.0.1", plugin_id=pid)
        )
        await s_bad.push(bad_hello)
        await asyncio.wait_for(t, 2.0)
        assert s_bad.closed_codes == [1008]

    # 2. Valid weather mount with query origin, admin_panel, configuration_issue, initial_status
    w_sock = _Phase2Socket()
    w_runner = asyncio.create_task(
        bus.serve(
            w_sock.receive,
            w_sock.send,
            w_sock.close,
            source_ip="127.0.0.1",
            plugin_id="weather",
        )
    )
    await w_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "天气观测",
            "scopes": ["actions", "sensors"],
            "admin_panel": "weather",
            "attention_level": "LOW",
            "configuration_issue": "weather_location_missing",
            "initial_status": {"location": None},
            "actions": [
                {
                    "name": "weather.read",
                    "description": "查询当前天气",
                    "origins": ["planner", "query"],
                    "arguments": {},
                }
            ],
        }
    )
    welcome = (await w_sock.recv_until(lambda m: m["type"] == "plugin.welcome"))[-1]
    assert welcome == {
        "type": "plugin.welcome",
        "plugin_id": "weather",
        "scopes": ["actions", "sensors"],
        "enabled": True,
    }
    w_proxy = svc.modules.get("weather")
    assert isinstance(w_proxy, RemoteModuleProxy)
    assert w_proxy.admin_panel == "weather"
    assert w_proxy.attention_level == "LOW"
    assert w_proxy.configuration_issue == "weather_location_missing"
    assert w_proxy.inactive_reason == "weather_location_missing"
    assert w_proxy.status_dict() == {"location": None}
    assert svc.modules.catalog.actions["weather.read"].quota_rules() == ()
    assert "weather.read" not in svc.life_engine.query_options()

    # plugin.status updates _remote_status and clears configuration_issue -> query becomes active!
    await w_sock.push(
        {
            "type": "plugin.status",
            "status": {
                "location": {
                    "latitude": 31.23,
                    "longitude": 121.47,
                    "source": "city",
                    "label": "上海",
                }
            },
            "configuration_issue": None,
        }
    )
    await asyncio.sleep(0.05)
    assert w_proxy.configuration_issue is None
    assert w_proxy.inactive_reason is None
    assert w_proxy.status_dict()["location"]["label"] == "上海"
    adm_status = w_proxy.admin_status(None)
    assert adm_status["online"] is True
    assert adm_status["location"]["latitude"] == 31.23
    assert "weather.read" in svc.life_engine.query_options()

    # query outcome preserves summary, source, observation faithfully
    q_task = asyncio.create_task(svc.life_engine.query("weather.read"))
    q_req = (await w_sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
    secret = configured_secrets(svc.s)[0]
    await w_sock.push(
        {
            "type": "rpc.result",
            "id": q_req["id"],
            "ok": True,
            "result": {
                "status": "done",
                "reason": "observation_recorded",
                "details": {
                    "summary": f"05-19 14:00，小雨，19.8 °C {secret}",
                    "source": "Open-Meteo",
                    "observation": {
                        "time": 1700000000,
                        "interval": 900,
                        "temperature_2m": 19.8,
                        "weather_code": 61,
                    },
                },
            },
        }
    )
    outcome = await q_task
    assert outcome.status == "done"
    assert outcome.reason == "observation_recorded"
    assert outcome.details["source"] == "Open-Meteo"
    assert outcome.details["observation"]["weather_code"] == 61
    assert secret not in outcome.details["summary"]
    assert "[REDACTED]" in outcome.details["summary"]

    # call_custom_rpc works and propagates PluginRpcError -> Rejected
    rpc_task = asyncio.create_task(w_proxy.call_custom_rpc("weather.location.get", {}, timeout=5.0))
    c_req = (await w_sock.recv_until(lambda m: m.get("method") == "weather.location.get"))[-1]
    await w_sock.push(
        {
            "type": "rpc.result",
            "id": c_req["id"],
            "ok": True,
            "result": {"location": {"label": "上海", "latitude": 31.23, "longitude": 121.47}},
        }
    )
    assert (await rpc_task)["location"]["label"] == "上海"

    err_task = asyncio.create_task(w_proxy.call_custom_rpc("weather.current", {}))
    e_req = (await w_sock.recv_until(lambda m: m.get("method") == "weather.current"))[-1]
    await w_sock.push(
        {
            "type": "rpc.result",
            "id": e_req["id"],
            "ok": False,
            "error": "weather_unavailable",
        }
    )
    with pytest.raises(Rejected, match="weather_unavailable"):
        await err_task

    await w_sock.close(1000)
    await asyncio.wait_for(w_runner, 2.0)


async def test_encrypted_kv_state_isolation_and_legacy_weather_fallback(phase2_env):
    _, svc, bus, _ = phase2_env
    svc.modules.unregister("weather")
    svc.modules.unregister("minecraft")

    # Seed legacy encrypted admin_config weather_location
    legacy_place = {"latitude": 35.68, "longitude": 139.69, "source": "city", "label": "东京"}
    await svc.store.runtime.save_config("weather_location", {"place": legacy_place, "revision": 2})

    await bus.create_plugin("weather", "天气观测", ["actions", "sensors"])
    await bus.create_plugin("other_kv", "其他插件", ["sensors"])

    w_sock = _Phase2Socket()
    o_sock = _Phase2Socket()
    w_run = asyncio.create_task(
        bus.serve(
            w_sock.receive, w_sock.send, w_sock.close, source_ip="127.0.0.1", plugin_id="weather"
        )
    )
    o_run = asyncio.create_task(
        bus.serve(
            o_sock.receive, o_sock.send, o_sock.close, source_ip="127.0.0.1", plugin_id="other_kv"
        )
    )

    await w_sock.push(
        {"type": "plugin.hello", "protocol_version": 1, "display_name": "W", "scopes": ["sensors"]}
    )
    await o_sock.push(
        {"type": "plugin.hello", "protocol_version": 1, "display_name": "O", "scopes": ["sensors"]}
    )
    await w_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    await o_sock.recv_until(lambda m: m["type"] == "plugin.welcome")

    # 1. Weather gets legacy fallback; non-weather gets found=False
    await w_sock.push({"type": "state.get", "id": "wg-1", "key": "location"})
    wg1 = (await w_sock.recv_until(lambda m: m.get("id") == "wg-1"))[-1]
    assert wg1 == {
        "type": "kernel.result",
        "id": "wg-1",
        "ok": True,
        "result": {"found": True, "value": legacy_place},
    }

    await o_sock.push({"type": "state.get", "id": "og-1", "key": "location"})
    og1 = (await o_sock.recv_until(lambda m: m.get("id") == "og-1"))[-1]
    assert og1 == {
        "type": "kernel.result",
        "id": "og-1",
        "ok": True,
        "result": {"found": False, "value": None},
    }

    # 2. state.set encrypts in SQLite and isolates per plugin_id
    new_place = {
        "latitude": 31.23,
        "longitude": 121.47,
        "source": "city",
        "label": "ShanghaiSecret",
    }
    await w_sock.push({"type": "state.set", "id": "ws-1", "key": "location", "value": new_place})
    ws1 = (await w_sock.recv_until(lambda m: m.get("id") == "ws-1"))[-1]
    assert ws1["ok"] is True and ws1["result"] == {"saved": True}

    async with svc.store.lock:
        row = await svc.store.one(
            "SELECT payload FROM plugin_kv_state WHERE plugin_id=? AND state_key=?",
            ("weather", "location"),
        )
    assert row is not None
    assert b"ShanghaiSecret" not in bytes(row["payload"])
    assert svc.store.vault.open("plugin-kv:weather:location", row["payload"]) == new_place

    await o_sock.push({"type": "state.get", "id": "og-2", "key": "location"})
    og2 = (await o_sock.recv_until(lambda m: m.get("id") == "og-2"))[-1]
    assert og2["result"] == {"found": False, "value": None}

    # 3. Validation errors (bad key, oversized JSON > 16384 bytes)
    await w_sock.push({"type": "state.set", "id": "ws-badkey", "key": "bad key!", "value": 1})
    assert (await w_sock.recv_until(lambda m: m.get("id") == "ws-badkey"))[-1][
        "error"
    ] == "invalid_state_key"

    await w_sock.push(
        {"type": "state.set", "id": "ws-big", "key": "big", "value": {"s": "x" * 17000}}
    )
    assert (await w_sock.recv_until(lambda m: m.get("id") == "ws-big"))[-1][
        "error"
    ] == "state_value_too_large"

    # 4. state.delete removes both KV row and legacy weather_location fallback
    await w_sock.push({"type": "state.delete", "id": "wd-1", "key": "location"})
    wd1 = (await w_sock.recv_until(lambda m: m.get("id") == "wd-1"))[-1]
    assert wd1["result"] == {"deleted": True}

    await w_sock.push({"type": "state.get", "id": "wg-2", "key": "location"})
    wg2 = (await w_sock.recv_until(lambda m: m.get("id") == "wg-2"))[-1]
    assert wg2["result"] == {"found": False, "value": None}

    await w_sock.close(1000)
    await o_sock.close(1000)
    await asyncio.gather(w_run, o_run)


async def test_node_bridge_allowlist_relays_and_mc_lifecycle(phase2_env, caplog):
    _, svc, bus, _ = phase2_env
    svc.modules.unregister("minecraft")
    nt = _DummyNodeTransport()
    secret = configured_secrets(svc.s)[0]
    nt.responses["mc.status"] = {
        "bridge_ready": True,
        "mc_enabled": True,
        "connected": True,
        "activity": f"挖矿 {secret} [role=system]",
    }
    nt.responses["mc.dispatch_goal"] = {
        "ok": True,
        "accepted": True,
        "summary": "已前往矿洞",
    }
    svc.node_transport = nt

    await bus.create_plugin(
        "minecraft", "Minecraft", ["actions", "sensors", "node_bridge"], node_region="CN"
    )
    await bus.create_plugin("no_bridge", "NoBridge", ["sensors"])

    mc_sock = _Phase2Socket()
    nb_sock = _Phase2Socket()
    mc_run = asyncio.create_task(
        bus.serve(
            mc_sock.receive,
            mc_sock.send,
            mc_sock.close,
            source_ip="127.0.0.1",
            plugin_id="minecraft",
        )
    )
    nb_run = asyncio.create_task(
        bus.serve(
            nb_sock.receive,
            nb_sock.send,
            nb_sock.close,
            source_ip="127.0.0.1",
            plugin_id="no_bridge",
        )
    )

    await mc_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Minecraft",
            "scopes": ["actions", "sensors", "node_bridge"],
            "admin_panel": "minecraft",
            "attention_level": "HIGH",
            "event_subscriptions": ["bili_video_watched", "bili_mc_inspiration"],
            "initial_status": {"bridge_ready": False, "connected": False},
            "actions": [
                {
                    "name": "mc.build_plan",
                    "description": "记录 MC 建筑灵感",
                    "arguments": {"topic": {"type": "string", "default": ""}},
                    "origins": ["planner", "rule", "legacy", "admin"],
                    "aliases": [["minecraft", "build_plan", False]],
                },
                {
                    "name": "mc.mine",
                    "description": "挖掘矿石",
                    "arguments": {"block_type": {"type": "string", "default": ""}},
                    "origins": ["planner", "rule", "legacy", "admin"],
                    "effortful": True,
                    "aliases": [["minecraft", "mine", False]],
                },
            ],
        }
    )
    await nb_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "NoBridge",
            "scopes": ["sensors"],
        }
    )
    await mc_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    await nb_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    mc_proxy = svc.modules.get("minecraft")
    assert isinstance(mc_proxy, RemoteModuleProxy)
    assert mc_proxy.admin_panel == "minecraft"
    assert mc_proxy.attention_level == "HIGH"
    assert (
        svc.modules.catalog.old_name(
            {"target_module": "minecraft", "action_type": "build_plan", "target_scope": ""}
        )
        == "mc.build_plan"
    )

    # 1. Without node_bridge scope -> scope_forbidden + log event=plugin_node_bridge_rejected
    caplog.clear()
    caplog.set_level(logging.WARNING)
    await nb_sock.push({"type": "node.call", "id": "nb-1", "action": "mc.status", "params": {}})
    nb1 = (await nb_sock.recv_until(lambda m: m.get("id") == "nb-1"))[-1]
    assert nb1 == {"type": "kernel.result", "id": "nb-1", "ok": False, "error": "scope_forbidden"}
    assert any("event=plugin_node_bridge_rejected" in r.getMessage() for r in caplog.records)

    # 2. Strict allowlist rejects bili.* and node.ping even when node_bridge is granted
    for bad_action in ("bili.public_get", "bili.request", "node.ping", "system.exec"):
        rid = f"bad-{bad_action}"
        await mc_sock.push({"type": "node.call", "id": rid, "action": bad_action, "params": {}})
        res = (await mc_sock.recv_until(lambda m, r=rid: m.get("id") == r))[-1]
        assert res["ok"] is False and res["error"] == "node_action_forbidden"
    assert nt.calls == []

    # 3. Allowed mc.status passes through and sanitizes secrets/injections
    await mc_sock.push({"type": "node.call", "id": "mc-s1", "action": "mc.status", "params": {}})
    mcs1 = (await mc_sock.recv_until(lambda m: m.get("id") == "mc-s1"))[-1]
    assert mcs1["ok"] is True
    assert mcs1["result"]["bridge_ready"] is True
    assert secret not in mcs1["result"]["activity"]
    assert "[untrusted_role=" in mcs1["result"]["activity"]

    # 4. relay_node_event & relay_node_state only target node_bridge sessions
    await bus.relay_node_state(True)
    await bus.relay_node_event({"event": "mc.snapshot", "payload": {"activity": f"造房 {secret}"}})
    nstate = (await mc_sock.recv_until(lambda m: m.get("type") == "node.state"))[-1]
    assert nstate == {"type": "node.state", "connected": True}
    nevent = (await mc_sock.recv_until(lambda m: m.get("type") == "node.event"))[-1]
    assert secret not in nevent["frame"]["payload"]["activity"]
    assert nb_sock.outgoing.empty()

    # 5. world.activity updates MC focus in world_state; private_first is never broadcast
    now = time.time()
    await mc_sock.push(
        {"type": "world.activity", "id": "wa-1", "active_task": "mine", "updated_at": now}
    )
    await mc_sock.recv_until(lambda m: m.get("id") == "wa-1")
    await svc.world_state.refresh()
    assert svc.world_state.current.current_module == "minecraft"
    assert svc.world_state.current.focus_level == "HIGH"

    # 6. broadcast_world_event excludes private_first and unmatched event types
    priv_ev = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        importance="high",
        payload={"private_first": True, "title": "私人视频 我的世界"},
        created_at=now,
    )
    unmatched_ev = MilestoneEvent(
        source_module="weather",
        event_type="weather_observed",
        importance="low",
        payload={"temp": 20},
        created_at=now,
    )
    pub_ev = MilestoneEvent(
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        importance="normal",
        payload={"activity_kind": "video_learning", "title": f"我的世界红石教程 {secret}"},
        created_at=now,
    )
    await bus.broadcast_world_event(priv_ev)
    await bus.broadcast_world_event(unmatched_ev)
    await bus.broadcast_world_event(pub_ev)
    bcast = (await mc_sock.recv_until(lambda m: m.get("type") == "world.event.broadcast"))[-1]
    assert bcast["event"]["event_type"] == "bili_video_watched"
    assert "私人视频" not in json.dumps(bcast, ensure_ascii=False)
    assert secret not in bcast["event"]["payload"]["title"]

    # 7. world.reaction validates ownership & enqueues intent ticket + sets wakeup
    caplog.clear()
    caplog.set_level(logging.INFO)
    svc.life_engine.wakeup.clear()
    await mc_sock.push(
        {
            "type": "world.reaction",
            "id": "wr-ok",
            "action_name": "mc.build_plan",
            "note": "刚刷到红石教程，记个建筑灵感",
            "ttl": 3600,
            "arguments": {"topic": "红石门"},
            "legacy_module": "minecraft",
            "legacy_verb": "build_plan",
            "source_event_id": pub_ev.id,
        }
    )
    wr_ok = (await mc_sock.recv_until(lambda m: m.get("id") == "wr-ok"))[-1]
    assert wr_ok["ok"] is True and wr_ok["intent_id"]
    assert svc.life_engine.wakeup.is_set()
    assert any("event=plugin_reaction_enqueued" in r.getMessage() for r in caplog.records)

    # Foreign action & prewritten speech rejected
    for bad_wr in (
        {"type": "world.reaction", "id": "wr-bad1", "action_name": "qq.speak", "note": "偷发消息"},
        {
            "type": "world.reaction",
            "id": "wr-bad2",
            "action_name": "mc.build_plan",
            "note": "say: 直接说话",
        },
    ):
        await mc_sock.push(bad_wr)
        ack = (await mc_sock.recv_until(lambda m, rid=bad_wr["id"]: m.get("id") == rid))[-1]
        assert ack["ok"] is False
    assert any("event=plugin_reaction_rejected" in r.getMessage() for r in caplog.records)

    # 8. RemoteModuleProxy.dispatch_goal via action.run RPC
    goal_task = asyncio.create_task(
        mc_proxy.dispatch_goal({"action_type": "mine", "block_type": "diamond_ore"})
    )
    g_req = (await mc_sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
    assert g_req["params"]["action"] == "mc.mine"
    assert g_req["params"]["arguments"] == {"block_type": "diamond_ore"}
    await mc_sock.push(
        {
            "type": "rpc.result",
            "id": g_req["id"],
            "ok": True,
            "result": {
                "status": "done",
                "reason": "mc_acknowledged",
                "details": {
                    "result": {
                        "ok": True,
                        "module_id": "minecraft",
                        "summary": "正在挖钻石",
                        "artifacts": {"connected": True},
                        "reason": None,
                    }
                },
            },
        }
    )
    goal_res = await goal_task
    assert isinstance(goal_res, GoalResult)
    assert goal_res.ok is True and goal_res.summary == "正在挖钻石"

    # 9. RemoteModuleProxy.on_switch(False) pushes module.switch and clears MC HIGH focus
    await svc.life_engine.set_module_enabled(mc_proxy, False)
    sw_frame = (await mc_sock.recv_until(lambda m: m.get("type") == "module.switch"))[-1]
    assert sw_frame == {"type": "module.switch", "enabled": False}
    await svc.world_state.refresh()
    assert svc.world_state.current.current_module == "idle"
    assert svc.world_state.current.focus_level == "LOW"

    await mc_sock.close(1000)
    await nb_sock.close(1000)
    await asyncio.gather(mc_run, nb_run)


async def test_phase2_edge_cases_flat_outcome_scope_guards_and_attention_filter(phase2_env):
    _, svc, bus, _ = phase2_env
    svc.modules.unregister("weather")
    svc.modules.unregister("minecraft")

    # 1. LOW-attention non-minecraft plugin cannot hijack MC focus via world.activity
    await bus.create_plugin("low_plug", "Low", ["actions", "sensors"])
    low_sock = _Phase2Socket()
    low_run = asyncio.create_task(
        bus.serve(
            low_sock.receive,
            low_sock.send,
            low_sock.close,
            source_ip="127.0.0.1",
            plugin_id="low_plug",
        )
    )
    await low_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Low",
            "scopes": ["actions", "sensors"],
            "attention_level": "LOW",
            "initial_status": {
                "connected": True,
                "bridge_ready": True,
                "location": {"label": "Hangzhou"},
            },
            "actions": [{"name": "low_plug.ping", "description": "Ping"}],
        }
    )
    await low_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    low_proxy = svc.modules.get("low_plug")
    assert isinstance(low_proxy, RemoteModuleProxy)

    adm = low_proxy.admin_status(None)
    assert adm["online"] is True
    assert adm["connected"] is True
    assert adm["bridge_ready"] is True
    assert adm["location"] == {"label": "Hangzhou"}

    await low_sock.push(
        {"type": "world.activity", "id": "low-wa", "active_task": "mine", "updated_at": time.time()}
    )
    await low_sock.recv_until(lambda m: m.get("id") == "low-wa")
    await svc.world_state.refresh()
    assert svc.world_state.current.current_module == "idle"
    assert svc.world_state.current.focus_level == "LOW"

    # Flat action.run response {"ok": True, "status": ..., "reason": ..., "details": ...}
    spec = svc.modules.catalog.actions["low_plug.ping"]
    from hakimi.modules.actions import ActionCall

    act_task = asyncio.create_task(
        spec.run(ActionCall({"origin": "planner", "target_scope": ""}, spec.arguments(), 100.0))
    )
    a_req = (await low_sock.recv_until(lambda m: m.get("method") == "action.run"))[-1]
    await low_sock.push(
        {
            "type": "rpc.result",
            "id": a_req["id"],
            "ok": True,
            "status": "gated_skip",
            "reason": "weather_unavailable",
            "details": {
                "summary": "s",
                "source": "Open-Meteo",
                "observation": {"temperature_2m": 20.0},
                "result": {"ok": False},
            },
        }
    )
    out = await act_task
    assert out.status == "gated_skip"
    assert out.reason == "weather_unavailable"
    assert out.details == {
        "summary": "s",
        "source": "Open-Meteo",
        "observation": {"temperature_2m": 20.0},
        "result": {"ok": False},
    }

    # Missing scopes on world.reaction / world.activity close 1008
    await low_sock.close(1000)
    await asyncio.wait_for(low_run, 2.0)

    await bus.create_plugin("sensors_only", "Sensors", ["sensors"])
    so_sock = _Phase2Socket()
    so_run = asyncio.create_task(
        bus.serve(
            so_sock.receive,
            so_sock.send,
            so_sock.close,
            source_ip="127.0.0.1",
            plugin_id="sensors_only",
        )
    )
    await so_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Sensors",
            "scopes": ["sensors"],
        }
    )
    await so_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    await so_sock.push({"type": "world.reaction", "action_name": "sensors_only.x", "note": "n"})
    await asyncio.wait_for(so_run, 2.0)
    assert so_sock.closed_codes == [1008]

    await bus.create_plugin("actions_only", "Actions", ["actions"])
    ao_sock = _Phase2Socket()
    ao_run = asyncio.create_task(
        bus.serve(
            ao_sock.receive,
            ao_sock.send,
            ao_sock.close,
            source_ip="127.0.0.1",
            plugin_id="actions_only",
        )
    )
    await ao_sock.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Actions",
            "scopes": ["actions"],
        }
    )
    await ao_sock.recv_until(lambda m: m["type"] == "plugin.welcome")
    await ao_sock.push({"type": "world.activity", "active_task": "mine"})
    await asyncio.wait_for(ao_run, 2.0)
    assert ao_sock.closed_codes == [1008]
