"""Tests for Wave 3+4 Concurrency Safety, Deadlock Fixes, PluginBus Decoupling,
executescript() Cleanup, and Cross-Platform Telegram Alignment.
"""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import time
from decimal import Decimal
from unittest.mock import patch

import pytest
from pydantic import SecretStr, ValidationError

from hakimi import affection_rules, model_accounting
from hakimi.affection import (
    _MAX_ANNOYANCE_CACHE_ENTRIES,
    _MAX_SCORE_CACHE_ENTRIES,
    AffectionManager,
    _validate_platform_user,
)
from hakimi.affection_rules import ANNOYANCE_COOLDOWN_SECONDS
from hakimi.attention import AttentionRouter
from hakimi.bili_bridge import RemoteBiliWatcher
from hakimi.file_reader import FileReader
from hakimi.gateway import FilterConfig, FilterManager
from hakimi.group_pipeline import GroupPipeline
from hakimi.identity import IdentityService, validate_platform_user
from hakimi.models import Message
from hakimi.plugin_bus import PluginBus
from hakimi.plugin_bus import protocol as bus_protocol
from hakimi.plugin_bus import proxy as bus_proxy
from hakimi.plugin_bus.protocol import _MAX_KV_KEYS_PER_PLUGIN, _bound_ai_payload
from hakimi.plugin_bus.proxy import _PluginSession
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected
from hakimi.social import OwnerBinding
from hakimi.social_chat_config import (
    RemoteChatConfig,
    cache_remote_chat_config,
    save_remote_chat_config,
)
from hakimi.video_watch import VideoWatch
from hakimi.web_search import WebSearch
from hakimi_plugin_sdk.client import (
    HakimiPluginClient,
    PluginRpcError,
    PluginRuntimeConfig,
    _BoundedNotifQueue,
    _InboundBlobTransfer,
    _OutboundBlobEntry,
)
from tests.test_plugin_bus import _PluginSocket


class _MinimalService:
    def __init__(self, settings, store) -> None:
        self.s = settings
        self.store = store
        self.kernel = PluginKernel()
        self.modules = None
        self.world_state = None
        self.event_ledger = None
        self.proactive = None
        self.secrets: list[str] = []


def _tg_message(
    *,
    user_id: str = "555001",
    bot_id: str = "999001",
    kind: str = "group",
    conversation_id: str = "-1001234567890",
    text: str = "hello telegram",
    event_id: str = "tg-ev-1",
) -> Message:
    return Message(
        platform="telegram",
        bot_id=bot_id,
        event_id=event_id,
        user_id=user_id,
        kind=kind,  # type: ignore[arg-type]
        conversation_id=user_id if kind == "private" else conversation_id,
        text=text,
        addressed=True,
        created_at=time.time(),
        sender_nickname="AliceTG",
    )


async def _connect_socket(
    bus: PluginBus,
    plugin_id: str,
    scopes: list[str],
    *,
    chat_platforms: list[str] | None = None,
    capabilities: list[str] | None = None,
    initial_status: dict | None = None,
    event_subscriptions: list[str] | None = None,
) -> tuple[_PluginSocket, asyncio.Task]:
    sock = _PluginSocket()
    task = asyncio.create_task(
        bus.serve(
            sock.receive,
            sock.send,
            sock.close,
            source_ip="127.0.0.1",
            plugin_id=plugin_id,
        )
    )
    hello = {
        "type": "plugin.hello",
        "protocol_version": 1,
        "display_name": plugin_id,
        "scopes": scopes,
        "chat_platforms": chat_platforms or [],
        "capabilities": capabilities or [],
        "initial_status": initial_status or {},
        "event_subscriptions": event_subscriptions or [],
    }
    await sock.push(hello)
    await sock.recv_until(lambda m: m.get("type") == "plugin.ready")
    return sock, task


# ---------------------------------------------------------------------------
# 1. PluginBus._reader() decoupling, reverse RPC without deadlock, bili_bridge
# ---------------------------------------------------------------------------


async def test_plugin_bus_reader_decoupled_allows_reverse_rpc_and_state_during_world_event(
    settings, store
):
    assert not hasattr(RemoteBiliWatcher, "_inside_own_reader")

    svc = _MinimalService(settings, store)
    bus = PluginBus(settings, store, svc, rpc_timeout=3.0)
    await bus.initialize()
    await bus.create_plugin("echo_plug", "Echo", ["sensors", "actions"])

    rpc_roundtrip_result: list[dict] = []

    class _CallbackWorldState:
        async def publish_event(self, plugin_id, event_type, payload, **kwargs):
            sess = bus._sessions[plugin_id]
            assert sess.proxy is not None
            # Reverse RPC back into the SAME plugin while world.event is being handled!
            # Previously deadlocked PluginBus._reader()!
            res = await sess.proxy.call_raw_rpc("custom.echo", {"from": event_type}, timeout=2.0)
            rpc_roundtrip_result.append(res)

    svc.world_state = _CallbackWorldState()
    sock, task = await _connect_socket(bus, "echo_plug", ["sensors", "actions"])
    try:
        await sock.push(
            {
                "type": "world.event",
                "id": "we-1",
                "event_type": "ping_back",
                "payload": {"k": "v"},
            }
        )
        # Wait for the reverse rpc.request triggered from inside world.event handler
        seen = await sock.recv_until(lambda m: m.get("type") == "rpc.request")
        rpc_req = seen[-1]
        assert rpc_req["method"] == "custom.echo"

        # While world.event is STILL waiting for rpc.result, verify state.set / state.get
        # on the dedicated state queue ALSO completes without blocking!
        await sock.push({"type": "state.set", "id": "st-1", "key": "k1", "value": {"n": 42}})
        st_ack = (await sock.recv_until(lambda m: m.get("id") == "st-1"))[-1]
        assert st_ack["ok"] is True

        await sock.push({"type": "state.get", "id": "st-2", "key": "k1"})
        st_get = (await sock.recv_until(lambda m: m.get("id") == "st-2"))[-1]
        assert st_get["ok"] is True and st_get["result"]["value"] == {"n": 42}

        # Now reply to the reverse RPC; world.event completes and sends world.event.ack!
        await sock.push(
            {"type": "rpc.result", "id": rpc_req["id"], "ok": True, "result": {"echoed": True}}
        )
        ack = (await sock.recv_until(lambda m: m.get("type") == "world.event.ack"))[-1]
        assert ack["id"] == "we-1" and ack["accepted"] is True
        assert rpc_roundtrip_result == [{"echoed": True}]
    finally:
        await sock.close()
        await asyncio.wait_for(task, 2.0)


# ---------------------------------------------------------------------------
# 2. Credential TOCTOU fix & delete_plugin cleanup
# ---------------------------------------------------------------------------


async def test_plugin_bus_credential_toctou_rotate_disable_and_delete(settings, store):
    svc = _MinimalService(settings, store)
    bus = PluginBus(settings, store, svc)
    await bus.initialize()
    await bus.create_plugin("race_plug", "Race", ["sensors"])

    # A. Rotate token mid-handshake (after serve() checks initial cred, before hello arrives)
    sock_rot = _PluginSocket()
    serve_task = asyncio.create_task(
        bus.serve(
            sock_rot.receive,
            sock_rot.send,
            sock_rot.close,
            source_ip="127.0.0.1",
            plugin_id="race_plug",
        )
    )
    await asyncio.sleep(0.02)  # let serve() enter Phase 1 waiting for hello
    await bus.rotate_token("race_plug")
    await sock_rot.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Race",
            "scopes": ["sensors"],
        }
    )
    await asyncio.wait_for(serve_task, 2.0)
    assert sock_rot.closed_codes == [1008]
    assert not bus.is_plugin_online("race_plug")
    err_frame = await sock_rot.outgoing.get()
    assert err_frame["type"] == "error" and err_frame["reason"] == "plugin_token_rotated"

    # B. Disable plugin mid-handshake
    sock_dis = _PluginSocket()
    serve_task2 = asyncio.create_task(
        bus.serve(
            sock_dis.receive,
            sock_dis.send,
            sock_dis.close,
            source_ip="127.0.0.1",
            plugin_id="race_plug",
        )
    )
    await asyncio.sleep(0.02)
    await bus.update_plugin("race_plug", enabled=False)
    await sock_dis.push(
        {
            "type": "plugin.hello",
            "protocol_version": 1,
            "display_name": "Race",
            "scopes": ["sensors"],
        }
    )
    await asyncio.wait_for(serve_task2, 2.0)
    assert sock_dis.closed_codes == [1008]
    assert not bus.is_plugin_online("race_plug")

    # C. Re-enable, connect, write KV state + notify history, then delete_plugin ->
    # deletes plugin_kv_state, disposes session, and prunes _connect_locks & _notify_history.
    await bus.update_plugin("race_plug", enabled=True, scopes=["sensors", "owner_notify"])
    sock_del, task_del = await _connect_socket(bus, "race_plug", ["sensors", "owner_notify"])
    await sock_del.push({"type": "state.set", "id": "kv-del", "key": "secret_k", "value": "v"})
    await sock_del.recv_until(lambda m: m.get("id") == "kv-del")
    await sock_del.push({"type": "owner.notify", "id": "n-1", "text": "hello owner"})
    await sock_del.recv_until(lambda m: m.get("type") == "owner.notify.ack")
    assert "race_plug" in bus._notify_history

    await bus.delete_plugin("race_plug")
    await asyncio.wait_for(task_del, 2.0)
    assert not bus.is_plugin_online("race_plug")
    assert "race_plug" not in bus._notify_history
    assert "race_plug" not in bus._connect_locks
    async with store.lock:
        kv_cnt = await store.one(
            "SELECT COUNT(*) FROM plugin_kv_state WHERE plugin_id=?", ("race_plug",)
        )
    assert int(kv_cnt[0]) == 0


# ---------------------------------------------------------------------------
# 3. Per-plugin KV key-count cap (512)
# ---------------------------------------------------------------------------


async def test_plugin_kv_key_count_cap_allows_updates_and_rejects_new_key(settings, store):
    svc = _MinimalService(settings, store)
    bus = PluginBus(settings, store, svc)
    await bus.initialize()
    await bus.create_plugin("kv_plug", "KV", ["sensors"])

    # Seed _MAX_KV_KEYS_PER_PLUGIN - 1 keys directly to keep test sub-millisecond
    now = time.time()
    async with store.transaction():
        for i in range(_MAX_KV_KEYS_PER_PLUGIN - 1):
            key = f"k_{i}"
            blob = store.vault.seal(f"plugin-kv:kv_plug:{key}", i)
            await store.db.execute(
                "INSERT INTO plugin_kv_state(plugin_id, state_key, payload, updated) "
                "VALUES (?,?,?,?)",
                ("kv_plug", key, blob, now),
            )

    sock, task = await _connect_socket(bus, "kv_plug", ["sensors"])
    try:
        # 512th key succeeds
        await sock.push({"type": "state.set", "id": "set-512", "key": "k_last", "value": 512})
        r512 = (await sock.recv_until(lambda m: m.get("id") == "set-512"))[-1]
        assert r512["ok"] is True

        # Updating an existing key still succeeds at the cap
        await sock.push({"type": "state.set", "id": "upd-0", "key": "k_0", "value": "updated"})
        rupd = (await sock.recv_until(lambda m: m.get("id") == "upd-0"))[-1]
        assert rupd["ok"] is True

        # 513th distinct key is rejected
        await sock.push({"type": "state.set", "id": "set-513", "key": "k_overflow", "value": 513})
        r513 = (await sock.recv_until(lambda m: m.get("id") == "set-513"))[-1]
        assert r513["ok"] is False and r513["error"] == "state_key_limit_exceeded"
    finally:
        await sock.close()
        await asyncio.wait_for(task, 2.0)


# ---------------------------------------------------------------------------
# 4. Bounded WS writes & concurrent fan-out
# ---------------------------------------------------------------------------


async def test_send_raw_timeout_and_concurrent_fanout(settings, store):
    svc = _MinimalService(settings, store)
    bus = PluginBus(settings, store, svc)
    await bus.initialize()

    stall = asyncio.Event()

    async def _stalled_send(_wire: str) -> None:
        await stall.wait()

    with pytest.raises(TimeoutError):
        await bus._send_raw(_stalled_send, asyncio.Lock(), {"type": "ping"}, timeout=0.05)

    # Concurrent fan-out: fast session receives broadcast without waiting for slow session
    await bus.create_plugin("slow_p", "Slow", ["sensors", "node_bridge"])
    await bus.create_plugin("fast_p", "Fast", ["sensors", "node_bridge"])
    sock_slow, task_slow = await _connect_socket(
        bus, "slow_p", ["sensors", "node_bridge"], event_subscriptions=["demo_ev"]
    )
    sock_fast, task_fast = await _connect_socket(
        bus, "fast_p", ["sensors", "node_bridge"], event_subscriptions=["demo_ev"]
    )
    try:
        release_slow = asyncio.Event()
        slow_started = asyncio.Event()
        orig_slow_send = bus._sessions["slow_p"].send

        async def _delayed_send(wire: str) -> None:
            slow_started.set()
            await release_slow.wait()
            await orig_slow_send(wire)

        bus._sessions["slow_p"].send = _delayed_send

        bcast_task = asyncio.create_task(
            bus.broadcast_world_event(
                {
                    "id": "ev-1",
                    "event_type": "demo_ev",
                    "source_module": "core",
                    "payload": {"x": 1},
                }
            )
        )
        await asyncio.wait_for(slow_started.wait(), 1.0)
        # fast_p must ALREADY receive the frame even though slow_p is still blocked!
        fast_msgs = await sock_fast.recv_until(
            lambda m: m.get("type") == "world.event.broadcast", max_wait=0.5
        )
        assert fast_msgs[-1]["event"]["event_type"] == "demo_ev"
        release_slow.set()
        await asyncio.wait_for(bcast_task, 1.0)

        # Same check for relay_node_state / relay_node_event
        release_slow.clear()
        slow_started.clear()
        relay_task = asyncio.create_task(bus.relay_node_state(True))
        await asyncio.wait_for(slow_started.wait(), 1.0)
        ns_msgs = await sock_fast.recv_until(lambda m: m.get("type") == "node.state", max_wait=0.5)
        assert ns_msgs[-1]["connected"] is True
        release_slow.set()
        await asyncio.wait_for(relay_task, 1.0)
    finally:
        await sock_slow.close()
        await sock_fast.close()
        await asyncio.wait_for(asyncio.gather(task_slow, task_fast), 2.0)


# ---------------------------------------------------------------------------
# 5. SDK client hardening: _kernel_request timeout, blob eviction, bounded notif_queue
# ---------------------------------------------------------------------------


async def test_sdk_client_kernel_request_timeout_and_bounded_notif_queue_and_blob_eviction():
    cfg = PluginRuntimeConfig(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        plugin_id="sdk_test",
        token="tok-" + "a" * 32,
        display_name="SDK Test",
        scopes=("sensors",),
        heartbeat_seconds=0.05,
    )
    client = HakimiPluginClient(cfg)

    # A. _kernel_request bounds _send_live_frame inside asyncio.timeout
    hang = asyncio.Event()

    class _HangingWS:
        async def send(self, _raw: str) -> None:
            await hang.wait()

    client._ws = _HangingWS()
    client._connected = True
    with pytest.raises(PluginRpcError) as exc_info:
        await client._kernel_request({"type": "state.get", "key": "x"}, timeout=0.05)
    assert exc_info.value.code == "plugin_rpc_timeout"
    assert client._pending_kernel_rpcs == {}

    # B. _BoundedNotifQueue bounds event flood while NEVER dropping control frames
    q = _BoundedNotifQueue(max_size=4, plugin_id="sdk_test")
    q.put_nowait(("on_connect", {"ok": True}))
    for i in range(20):
        q.put_nowait(("world_event", {"seq": i}))
    q.put_nowait(("switch", False))
    q.put_nowait(("node_state", True))
    assert q.qsize() <= 4
    assert q.dropped_events > 0
    drained = [await q.get() for _ in range(q.qsize())]
    kinds = [k for k, _ in drained]
    assert "on_connect" in kinds
    assert ("switch", False) in drained
    assert ("node_state", True) in drained

    # C. _heartbeat calls _evict_expired_blobs periodically
    sent_frames: list[dict] = []
    rx_queue: asyncio.Queue[str] = asyncio.Queue()

    class _FakeWS:
        async def send(self, raw: str) -> None:
            frame = json.loads(raw)
            sent_frames.append(frame)
            if frame.get("type") == "plugin.hello":
                await rx_queue.put(json.dumps({"type": "plugin.welcome", "plugin_id": "sdk_test"}))
            elif frame.get("type") == "ping":
                await rx_queue.put(json.dumps({"type": "pong", "ts": frame.get("ts")}))

        async def recv(self) -> str:
            return await rx_queue.get()

        async def close(self, code: int = 1000) -> None:
            pass

    client2 = HakimiPluginClient(cfg, connect_factory=lambda _url, **_kw: _FakeWS())
    await client2.start()
    try:
        assert await client2.wait_connected(timeout=1.0)
        expired_ts = time.monotonic() - 120.0
        client2._inbound_blobs["ab" * 16] = _InboundBlobTransfer(
            transfer_id="ab" * 16,
            expected_bytes=4,
            total_chunks=1,
            sha256="0" * 64,
            mime_type="application/octet-stream",
            filename="",
            updated_at=expired_ts,
        )
        client2._outbound_blobs["cd" * 16] = _OutboundBlobEntry(
            blob_id="cd" * 16,
            data=b"1234",
            sha256="0" * 64,
            chunk_size=1024,
            total_chunks=1,
            mime_type="application/octet-stream",
            filename="",
            updated_at=expired_ts,
        )
        # Wait for one heartbeat cycle (0.05s) to evict expired blobs
        await asyncio.sleep(0.15)
        assert client2._inbound_blobs == {}
        assert client2._outbound_blobs == {}
    finally:
        await client2.stop()


# ---------------------------------------------------------------------------
# 6. proxy.py pre-check before base64+JSON, no _PluginSession.uncertain_deliveries,
#    connect vs config.update status regression guard, _bound_ai_payload perf
# ---------------------------------------------------------------------------


async def test_proxy_inline_precheck_status_race_guard_and_bound_ai_payload(settings, store):
    assert "uncertain_deliveries" not in _PluginSession.__dataclass_fields__

    svc = _MinimalService(settings, store)
    bus = PluginBus(settings, store, svc)
    await bus.initialize()
    await bus.create_plugin("tg_demo", "Telegram Demo", ["chat", "media"])
    sock, task = await _connect_socket(
        bus,
        "tg_demo",
        ["chat", "media"],
        chat_platforms=["telegram"],
        capabilities=["sticker", "voice"],
        initial_status={"connected": False},
    )
    try:
        proxy = bus._sessions["tg_demo"].proxy
        assert proxy is not None

        # A. Oversized media (> compute_inline_max_bytes) must NOT base64-encode before failing
        big_png = b"\x89PNG\r\n\x1a\n" + (b"\x01" * (200 * 1024))
        with patch.object(bus_proxy.base64, "b64encode", wraps=base64.b64encode) as spy_b64:
            with pytest.raises(Rejected, match="media_payload_too_large"):
                await proxy.send_media(_tg_message(), big_png, kind="sticker")
            assert spy_b64.call_count == 0

        big_ogg = b"OggS" + (b"\x02" * (200 * 1024))
        with patch.object(bus_proxy.base64, "b64encode", wraps=base64.b64encode) as spy_b64:
            with pytest.raises(Rejected, match="voice_payload_too_large"):
                await proxy.send_voice(
                    _tg_message(), big_ogg, fmt="ogg_opus", mime_type="audio/ogg"
                )
            assert spy_b64.call_count == 0

        # B. Connect vs config.update status regression race:
        # push_config starts -> plugin pushes connected=True -> config.update returns False
        cfg = RemoteChatConfig(allowed_groups=["-1001234567890"])
        push_task = asyncio.create_task(proxy.push_config(cfg, platform="telegram"))
        rpc_req = (await sock.recv_until(lambda m: m.get("type") == "rpc.request"))[-1]
        assert rpc_req["method"] == "config.update"

        # Fresher plugin.status arrives while config.update is in flight
        await sock.push(
            {
                "type": "plugin.status",
                "id": "st-conn-1",
                "status": {"connected": True, "bot_id": "999001", "bot_username": "hakimi_bot"},
            }
        )
        await sock.recv_until(lambda m: m.get("type") == "plugin.status.ack")
        assert proxy.chat_connected("telegram") is True

        # Earlier config.update RPC completes with stale connected=False
        await sock.push(
            {
                "type": "rpc.result",
                "id": rpc_req["id"],
                "ok": True,
                "result": {
                    "applied": True,
                    "status": {
                        "connected": False,
                        "allowed_groups": ["-1001234567890"],
                        "config_source": "admin",
                    },
                },
            }
        )
        assert await asyncio.wait_for(push_task, 2.0) is True
        assert proxy.chat_connected("telegram") is True
        assert proxy.status_dict()["connected"] is True
        assert proxy.status_dict()["bot_id"] == "999001"
        assert proxy.status_dict()["allowed_groups"] == ["-1001234567890"]
    finally:
        await sock.close()
        await asyncio.wait_for(task, 2.0)

    # C. _bound_ai_payload avoids repeated full-tree serialization per step
    big_payload = {
        "subtitle_excerpt": "测试字幕内容" * 5000,
        "comments": [f"评论条目 {i} 内容较长用于测试" for i in range(300)],
    }
    orig_untrusted = bus_protocol.untrusted_input
    call_count = 0

    def _counting_untrusted(p):
        nonlocal call_count
        call_count += 1
        return orig_untrusted(p)

    with patch.object(bus_protocol, "untrusted_input", side_effect=_counting_untrusted):
        bounded = _bound_ai_payload(big_payload, [], 2048)

    assert len(orig_untrusted(bounded).encode("utf-8")) <= 2048
    assert bounded["subtitle_complete"] is False
    assert bounded["subtitle_status"] == "truncated"
    assert call_count <= 8


# ---------------------------------------------------------------------------
# 7. IdentityService <-> AffectionManager deadlock freedom & atomic merge
# ---------------------------------------------------------------------------


async def test_identity_affection_no_deadlock_and_atomic_rollback(settings, store):
    assert validate_platform_user is affection_rules.validate_platform_user
    assert _validate_platform_user is affection_rules.validate_platform_user

    aff = AffectionManager(store)
    ident = IdentityService(settings, store)
    store.runtime.affection = aff
    store.runtime.identity = ident

    # Seed legacy affection record BEFORE identity initializes, then trigger concurrent
    # initialize / migrate / link_account / merge_persons to verify zero AB-BA deadlock.
    await aff.initialize()
    await aff.set_user_score("30001", 65, platform="onebot")
    await aff.set_user_score("30002", 45, platform="onebot")

    # Concurrent init + resolves + merges must complete swiftly with zero deadlock
    async with asyncio.timeout(5.0):
        await asyncio.gather(
            ident.initialize(),
            aff.initialize(),
            aff.person_scores_snapshot(),
            ident.resolve_person_id("telegram", "777001", "TG1", create=True),
            ident.resolve_person_id("telegram", "777002", "TG2", create=True),
        )

    assert ident.initialized is True
    assert aff._migrated_to_person is True

    # Test atomic rollback on merge_persons: failure inside merge_person_scores_in_tx rolls back
    # BOTH identity tables AND affection tables AND leaves caches untouched!
    p1 = await ident.resolve_person_id("onebot", "30001", create=False)
    p2 = await ident.resolve_person_id("onebot", "30002", create=False)
    assert p1 is not None and p2 is not None and p1 != p2

    with patch.object(aff, "merge_person_scores_in_tx", side_effect=RuntimeError("boom_in_tx")):
        with pytest.raises(RuntimeError, match="boom_in_tx"):
            await ident.merge_persons(p2, p1, affection=aff)

    # Verify BOTH persons and links still exist intact in DB and caches
    assert ident.cached_person_id("onebot", "30001") == p1
    assert ident.cached_person_id("onebot", "30002") == p2
    async with store.lock:
        prows = await store.all(
            "SELECT person_id FROM identity_persons WHERE scope=? AND person_id IN (?,?)",
            (ident.scope, p1, p2),
        )
    assert {r["person_id"] for r in prows} == {p1, p2}
    assert (await aff.profile("onebot", "30001"))["score"] == 65
    assert (await aff.profile("onebot", "30002"))["score"] == 45

    # Test atomic rollback on link_account too
    with patch.object(aff, "merge_person_scores_in_tx", side_effect=RuntimeError("boom_link_tx")):
        with pytest.raises(RuntimeError, match="boom_link_tx"):
            await ident.link_account(p1, "onebot", "30002", affection=aff)
    assert ident.cached_person_id("onebot", "30002") == p2

    # Now perform real atomic merge_persons: min(65, 45) == 45, caches refreshed immediately
    merged = await ident.merge_persons(p2, p1, affection=aff)
    assert merged["person_id"] == p1
    assert merged["score"] == 45
    assert ident.cached_person_id("onebot", "30002") == p1
    assert aff._person_scores.get(p1) == 45
    assert p2 not in aff._person_scores

    # Test deduplicated on_unmute_person / on_unmute recovery
    await aff.set_user_score("30001", 0, platform="onebot")
    assert (await aff.profile("onebot", "30001"))["score"] == 0
    await aff.on_unmute("30001", platform="onebot")
    assert (await aff.profile("onebot", "30001"))["score"] == aff.config.unmute_recovery_score


# ---------------------------------------------------------------------------
# 8. executescript() cleanup verification across all 9 modules
# ---------------------------------------------------------------------------


async def test_no_executescript_called_during_initializers(settings, store):
    executescript_calls: list[str] = []
    orig_executescript = store.db.executescript

    async def _forbid_executescript(sql: str):
        executescript_calls.append(sql)
        return await orig_executescript(sql)

    svc = _MinimalService(settings, store)
    with patch.object(store.db, "executescript", side_effect=_forbid_executescript):
        await AffectionManager(store).initialize()
        await FilterManager(store).initialize()
        await FileReader(settings, store).initialize()
        await VideoWatch(svc, settings=settings, store=store).initialize()
        await PluginBus(settings, store, svc).initialize()
        await AttentionRouter(store).initialize()
        await GroupPipeline(store).initialize()
        await WebSearch(store).initialize()
        await model_accounting.initialize(store)
        await model_accounting.initialize_thinking(store)

    assert executescript_calls == []


# ---------------------------------------------------------------------------
# 9. Wave 4 Cross-platform Telegram alignment (gateway, affection, identity)
# ---------------------------------------------------------------------------


async def test_wave4_telegram_gateway_affection_and_identity_alignment(settings, store):
    # A. FilterConfig accepts Telegram group keys & rejects bad Telegram keys
    cfg = FilterConfig(
        group_activity={"telegram:-1001234567890": "active", "onebot:40000": "quiet"}
    )
    assert cfg.group_activity["telegram:-1001234567890"] == "active"
    with pytest.raises(ValidationError):
        FilterConfig(group_activity={"telegram:not_numeric": "active"})

    # B. FilterManager.set_group_activity supports Telegram allowlisted via RemoteChatConfig
    tg_cfg = RemoteChatConfig(allowed_groups=["-1001234567890"])
    await save_remote_chat_config(store, "telegram", tg_cfg, settings=settings)
    filters = FilterManager(store)
    await filters.initialize()
    await filters.set_group_activity("telegram", "-1001234567890", "active")
    assert filters.thresholds(_tg_message(conversation_id="-1001234567890"))["reply"] == 0.28
    with pytest.raises(Rejected, match="group_not_allowlisted"):
        await filters.set_group_activity("telegram", "-1009999999999", "active")

    # C. AffectionManager Telegram _bot_id, _is_known_bot, budget multiplier cache hit,
    #    bounded _annoyance_at eviction, and LRU cap on score caches
    s_tg = settings.model_copy(
        update={
            "admin_token": SecretStr("test-token"),
            "affection_enabled": True,
            "social_owner_bindings": {
                "telegram": OwnerBinding(bot_id="999001", user_id="888001", notebook=True)
            },
        }
    )
    store.s = s_tg
    clock_now = [1_000_000.0]
    aff = AffectionManager(store, clock=lambda: clock_now[0])
    ident = IdentityService(s_tg, store, clock=lambda: clock_now[0])
    store.runtime.affection = aff
    store.runtime.identity = ident
    await ident.initialize()
    await aff.initialize()

    assert aff._bot_id("telegram") == "999001"
    assert aff._is_known_bot("telegram", "999001") is True
    assert aff._is_known_bot("telegram", "555001") is False

    # Fallback to "telegram" when no owner binding & no live chat module exist
    s_no_bind = settings.model_copy(update={"social_owner_bindings": {}})
    store.s = s_no_bind
    aff_no_bind = AffectionManager(store)
    assert aff_no_bind._bot_id("telegram") == "telegram"
    store.s = s_tg

    # Budget multiplier cache hit for Telegram user
    await aff.set_user_score("555001", 85, platform="telegram")
    tg_subj = store.subject(_tg_message(user_id="555001", bot_id="999001"))
    assert aff.budget_multiplier_for_subject(tg_subj) == Decimal("1.35")

    # _annoyance_at evicts expired entries and respects bound
    for i in range(5):
        aff._record_annoyance_ts(f"old_{i}", clock_now[0])
    clock_now[0] += ANNOYANCE_COOLDOWN_SECONDS + 5.0
    aff._record_annoyance_ts("fresh_1", clock_now[0])
    assert list(aff._annoyance_at.keys()) == ["fresh_1"]
    for i in range(_MAX_ANNOYANCE_CACHE_ENTRIES + 20):
        aff._record_annoyance_ts(f"burst_{i}", clock_now[0])
    assert len(aff._annoyance_at) <= _MAX_ANNOYANCE_CACHE_ENTRIES

    # Score cache LRU bound keeps owner pinned
    aff._put_person_score("owner", 120)
    for i in range(_MAX_SCORE_CACHE_ENTRIES + 50):
        aff._put_person_score(f"p_{i:016x}", 50)
    assert len(aff._person_scores) <= _MAX_SCORE_CACHE_ENTRIES
    assert "owner" in aff._person_scores

    # D. IdentityService._sanitize_l2_tags strips Telegram allowed groups & telegram:{gid}
    cache_remote_chat_config(s_tg, "telegram", tg_cfg)
    sanitized = ident._sanitize_l2_tags(
        [
            "摄影爱好者",
            "-1001234567890",
            "telegram:-1001234567890",
            "来自telegram:-1001234567890的猫",
            "群-1001234567890",
            "喜欢猫猫",
        ]
    )
    assert sanitized == ["摄影爱好者", "喜欢猫猫"]

    # E. IdentityService indexes exist and list_persons bounds identity_links to fetched person_ids
    async with store.lock:
        idx_rows = await store.all(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name IN ('identity_persons_scope_updated', 'identity_links_scope_updated')"
        )
    assert {r["name"] for r in idx_rows} == {
        "identity_persons_scope_updated",
        "identity_links_scope_updated",
    }
    listed = await ident.list_persons(affection=aff)
    assert any(p["person_id"] == "owner" for p in listed["persons"])
    # Verify list_persons SQL batches identity_links by person_id IN (...) instead of full scan
    src = inspect.getsource(ident.list_persons)
    assert "person_id IN (" in src
