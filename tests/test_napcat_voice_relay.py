"""Tests for the transparent Shanghai OneBot v11 voice-to-text edge relay."""

from __future__ import annotations

import ast
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import serve as ws_serve

from hakimi.config import Settings
from hakimi.modules.adapters import onebot_message

ROOT = Path(__file__).resolve().parents[1]
RELAY_DIR = ROOT / "deploy" / "napcat_voice_relay"
if str(RELAY_DIR) not in sys.path:
    sys.path.insert(0, str(RELAY_DIR))

from relay import (  # noqa: E402
    INTERNAL_ECHO_PREFIX,
    RelayConfig,
    VoiceRelayServer,
    clean_transcript,
    message_has_record,
    rewrite_message_records,
)


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "gemini_api_key": "test-gemini-key",
        "encryption_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "onebot_enabled": True,
        "onebot_mode": "ws_reverse",
        "onebot_ws_token": "x" * 32,
        "onebot_self_id": "3554766829",
        "owner_qq_id": "10001",
        "onebot_owner_ids": {"10001"},
        "onebot_allowed_users": {"10001", "20002"},
        "onebot_allowed_groups": {"879101013"},
        "group_policies": {"onebot:879101013": "at_only"},
        "signature_max_age": 300,
    }
    base.update(overrides)
    return Settings(**base)


def test_relay_has_no_hakimi_imports() -> None:
    for path in RELAY_DIR.glob("*.py"):
        tree = ast.parse(path.read_text("utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("hakimi")
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("hakimi")


def test_helpers_sanitize_and_rewrite() -> None:
    raw = "你好\x00\x07世界\n\t [CQ:at,qq=all]   哈基米"
    cleaned = clean_transcript(raw, max_chars=40)
    assert "\x00" not in cleaned
    assert "[CQ:" not in cleaned
    assert "［CQ:at,qq=all]" in cleaned

    segs = [
        {"type": "reply", "data": {"id": "77"}},
        {"type": "record", "data": {"file": "abc.amr"}},
    ]
    assert message_has_record(segs)
    rewritten = rewrite_message_records(segs, "[语音转文字] 测试")
    assert rewritten == [
        {"type": "reply", "data": {"id": "77"}},
        {"type": "text", "data": {"text": "[语音转文字] 测试"}},
    ]
    assert message_has_record("[CQ:record,file=x.amr]")
    assert rewrite_message_records("[CQ:record,file=x.amr]", "[语音转文字] 喂") == "[语音转文字] 喂"


async def test_relay_passthrough_headers_retry_and_nonblocking_concurrent() -> None:
    received_by_upstream: list[dict[str, Any]] = []
    upstream_headers: dict[str, str] = {}
    upstream_ws_holder: list[Any] = []
    ready_ev = asyncio.Event()

    async def upstream_handler(ws: Any) -> None:
        upstream_ws_holder.append(ws)
        for key in ("authorization", "x-self-id", "x-client-role"):
            val = ws.request.headers.get(key)
            if val is not None:
                upstream_headers[key] = val
        ready_ev.set()
        async for raw in ws:
            received_by_upstream.append(json.loads(raw))

    async with ws_serve(upstream_handler, "127.0.0.1", 0) as upstream_srv:
        up_port = upstream_srv.sockets[0].getsockname()[1]
        cfg = RelayConfig(
            upstream_url=f"ws://127.0.0.1:{up_port}/ws/onebot/secretpath",
            listen_host="127.0.0.1",
            listen_port=0,
            retry_delays=(0.05, 0.05),
            attempt_timeout=2.0,
            total_timeout=4.0,
        )
        relay = VoiceRelayServer(cfg)
        await relay.start()
        try:
            async with ws_connect(
                f"ws://127.0.0.1:{relay.bound_port}/",
                additional_headers={
                    "Authorization": "Bearer supersecret-token",
                    "X-Self-ID": "3554766829",
                    "X-Client-Role": "Universal",
                },
                proxy=None,
            ) as napcat_ws:
                await asyncio.wait_for(ready_ev.wait(), timeout=3.0)
                assert upstream_headers == {
                    "authorization": "Bearer supersecret-token",
                    "x-self-id": "3554766829",
                    "x-client-role": "Universal",
                }

                # 1. Upstream RPC reaches NapCat verbatim, and NapCat response returns verbatim.
                await upstream_ws_holder[0].send(
                    json.dumps(
                        {
                            "action": "send_private_msg",
                            "params": {"user_id": 10001, "message": "hi"},
                            "echo": "rpc-1",
                        }
                    )
                )
                rpc_req = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert rpc_req["action"] == "send_private_msg"
                await napcat_ws.send(
                    json.dumps(
                        {"status": "ok", "retcode": 0, "data": {"message_id": 55}, "echo": "rpc-1"}
                    )
                )

                # 2. Send a voice event, then immediately send a normal text message while STT
                # is still retrying — normal message must arrive BEFORE slow STT completes.
                now = int(time.time())
                voice_ev = {
                    "post_type": "message",
                    "message_type": "group",
                    "group_id": 879101013,
                    "user_id": 20002,
                    "self_id": 3554766829,
                    "message_id": 9001,
                    "time": now,
                    "sender": {"nickname": "群友"},
                    "message": [{"type": "record", "data": {"file": "v1.amr"}}],
                }
                text_ev = {
                    "post_type": "message",
                    "message_type": "group",
                    "group_id": 879101013,
                    "user_id": 20002,
                    "self_id": 3554766829,
                    "message_id": 9002,
                    "time": now,
                    "sender": {"nickname": "群友"},
                    "message": [{"type": "text", "data": {"text": "先发的文字"}}],
                }
                await napcat_ws.send(json.dumps(voice_ev))
                await napcat_ws.send(json.dumps(text_ev))

                # First frame received by NapCat from relay is internal fetch_ptt_text attempt #1.
                stt_req1 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert stt_req1["action"] == "fetch_ptt_text"
                assert stt_req1["params"] == {"message_id": 9001}
                assert stt_req1["echo"].startswith(INTERNAL_ECHO_PREFIX)

                # Wait briefly until text_ev is already delivered upstream BEFORE STT attempt #2!
                for _ in range(40):
                    if any(item.get("message_id") == 9002 for item in received_by_upstream):
                        break
                    await asyncio.sleep(0.02)
                assert any(item.get("message_id") == 9002 for item in received_by_upstream)
                assert not any(item.get("message_id") == 9001 for item in received_by_upstream)

                # Attempt 1 fails -> relay retries automatically.
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "failed",
                            "retcode": 1200,
                            "data": None,
                            "message": "获取语音转文字结果失败",
                            "echo": stt_req1["echo"],
                        }
                    )
                )
                stt_req2 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert stt_req2["action"] == "fetch_ptt_text"
                assert stt_req2["params"] == {"message_id": 9001}
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"text": "哈基米今天上海冷不冷"},
                            "echo": stt_req2["echo"],
                        }
                    )
                )

                for _ in range(40):
                    if any(item.get("message_id") == 9001 for item in received_by_upstream):
                        break
                    await asyncio.sleep(0.02)

                # Internal STT responses were swallowed; rewritten text parses cleanly in Hakimi.
                assert not any(
                    str(item.get("echo", "")).startswith(INTERNAL_ECHO_PREFIX)
                    for item in received_by_upstream
                )
                rewritten_ev = next(
                    item for item in received_by_upstream if item.get("message_id") == 9001
                )
                msg = onebot_message(rewritten_ev, _settings())
                assert msg is not None
                assert msg.text == "[语音转文字] 哈基米今天上海冷不冷"
                assert msg.addressed is True
                assert msg.attention_reason == "alias"
                assert msg.command_text == "[语音转文字] 哈基米今天上海冷不冷"
        finally:
            await relay.close()


async def test_relay_slash_command_blocked_and_short_voice_fallback() -> None:
    received: list[dict[str, Any]] = []

    async def upstream_handler(ws: Any) -> None:
        async for raw in ws:
            received.append(json.loads(raw))

    async with ws_serve(upstream_handler, "127.0.0.1", 0) as upstream_srv:
        up_port = upstream_srv.sockets[0].getsockname()[1]
        relay = VoiceRelayServer(
            RelayConfig(
                upstream_url=f"ws://127.0.0.1:{up_port}/ws/onebot",
                listen_host="127.0.0.1",
                listen_port=0,
                retry_delays=(0.03,),
                attempt_timeout=1.0,
                total_timeout=2.0,
            )
        )
        await relay.start()
        try:
            async with ws_connect(f"ws://127.0.0.1:{relay.bound_port}/", proxy=None) as napcat_ws:
                now = int(time.time())
                # Case A: Voice says "/mute 10001" — prefix prevents slash command execution!
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9101,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "cmd.amr"}}],
                        }
                    )
                )
                req = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"text": "/mute 10001"},
                            "echo": req["echo"],
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("message_id") == 9101 for x in received):
                        break
                    await asyncio.sleep(0.02)
                parsed = onebot_message(received[-1], _settings())
                assert parsed is not None
                assert parsed.text == "[语音转文字] /mute 10001"
                assert not (parsed.command_text or "").startswith("/")

                # Case B: Very short voice permanently/repeatedly fails -> graceful fallback text.
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9102,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "short.amr"}}],
                        }
                    )
                )
                for _ in range(2):
                    r = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                    await napcat_ws.send(
                        json.dumps(
                            {
                                "status": "failed",
                                "retcode": 1200,
                                "data": None,
                                "message": "获取语音转文字结果失败",
                                "echo": r["echo"],
                            }
                        )
                    )
                for _ in range(40):
                    if any(x.get("message_id") == 9102 for x in received):
                        break
                    await asyncio.sleep(0.02)
                fallback = onebot_message(received[-1], _settings())
                assert fallback is not None
                assert fallback.text == "[语音消息:未能识别文字内容]"
        finally:
            await relay.close()


async def test_relay_get_msg_cache_hit_and_miss() -> None:
    received: list[dict[str, Any]] = []
    upstream_ws_holder: list[Any] = []
    connected = asyncio.Event()
    stt_calls: list[int | str] = []

    async def upstream_handler(ws: Any) -> None:
        upstream_ws_holder.append(ws)
        connected.set()
        async for raw in ws:
            received.append(json.loads(raw))

    async with ws_serve(upstream_handler, "127.0.0.1", 0) as upstream_srv:
        up_port = upstream_srv.sockets[0].getsockname()[1]
        relay = VoiceRelayServer(
            RelayConfig(
                upstream_url=f"ws://127.0.0.1:{up_port}/ws/onebot",
                listen_host="127.0.0.1",
                listen_port=0,
                retry_delays=(0.03,),
                attempt_timeout=1.5,
                get_msg_timeout=2.5,
            )
        )
        # Seed cache for 9201 directly to verify 0-RPC cache hit on get_msg.
        relay.cache.set("9201", "缓存命中的语音")
        await relay.start()
        try:
            async with ws_connect(f"ws://127.0.0.1:{relay.bound_port}/", proxy=None) as napcat_ws:
                await asyncio.wait_for(connected.wait(), timeout=2.0)
                up_ws = upstream_ws_holder[0]

                # 1. get_msg for cached 9201: should rewrite without calling fetch_ptt_text.
                await up_ws.send(
                    json.dumps(
                        {"action": "get_msg", "params": {"message_id": 9201}, "echo": "gm-1"}
                    )
                )
                req1 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert req1["action"] == "get_msg"
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {
                                "message_id": 9201,
                                "message_type": "group",
                                "group_id": 879101013,
                                "sender": {"user_id": 20002, "nickname": "群友"},
                                "message": [{"type": "record", "data": {"file": "old.amr"}}],
                            },
                            "echo": "gm-1",
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("echo") == "gm-1" for x in received):
                        break
                    await asyncio.sleep(0.02)
                gm1 = next(x for x in received if x.get("echo") == "gm-1")
                assert gm1["data"]["message"] == [
                    {"type": "text", "data": {"text": "[语音转文字] 缓存命中的语音"}}
                ]
                assert stt_calls == []

                # 2. get_msg for uncached 9202: should call fetch_ptt_text and rewrite response.
                await up_ws.send(
                    json.dumps(
                        {"action": "get_msg", "params": {"message_id": 9202}, "echo": "gm-2"}
                    )
                )
                req2 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert req2["action"] == "get_msg"
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {
                                "message_id": 9202,
                                "message_type": "group",
                                "group_id": 879101013,
                                "sender": {"user_id": 20002, "nickname": "群友"},
                                "message": [{"type": "record", "data": {"file": "uncached.amr"}}],
                            },
                            "echo": "gm-2",
                        }
                    )
                )
                stt_req = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                assert stt_req["action"] == "fetch_ptt_text"
                assert stt_req["params"] == {"message_id": 9202}
                stt_calls.append(stt_req["params"]["message_id"])
                await napcat_ws.send(
                    json.dumps(
                        {
                            "status": "ok",
                            "retcode": 0,
                            "data": {"text": "补查转写成功"},
                            "echo": stt_req["echo"],
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("echo") == "gm-2" for x in received):
                        break
                    await asyncio.sleep(0.02)
                gm2 = next(x for x in received if x.get("echo") == "gm-2")
                assert gm2["data"]["message"] == [
                    {"type": "text", "data": {"text": "[语音转文字] 补查转写成功"}}
                ]
        finally:
            await relay.close()


async def test_relay_scope_rate_limit_circuit_breaker_and_cascade_close() -> None:
    received: list[dict[str, Any]] = []
    upstream_closed = asyncio.Event()

    async def upstream_handler(ws: Any) -> None:
        try:
            async for raw in ws:
                received.append(json.loads(raw))
        finally:
            upstream_closed.set()

    async with ws_serve(upstream_handler, "127.0.0.1", 0) as upstream_srv:
        up_port = upstream_srv.sockets[0].getsockname()[1]
        now_t = [1000.0]
        relay = VoiceRelayServer(
            RelayConfig(
                upstream_url=f"ws://127.0.0.1:{up_port}/ws/onebot",
                listen_host="127.0.0.1",
                listen_port=0,
                scope="allowlist",
                allowed_groups=frozenset({"111"}),
                user_rpm=1,
                global_rpm=10,
                circuit_fail_threshold=2,
                circuit_cooldown=60.0,
                retry_delays=(),
                attempt_timeout=1.0,
                total_timeout=1.0,
            ),
            clock=lambda: now_t[0],
        )
        await relay.start()
        try:
            async with ws_connect(f"ws://127.0.0.1:{relay.bound_port}/", proxy=None) as napcat_ws:
                now = int(time.time())
                # Disallowed group 222 passes untouched without fetch_ptt_text.
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "group",
                            "group_id": 222,
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9301,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "skip.amr"}}],
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("message_id") == 9301 for x in received):
                        break
                    await asyncio.sleep(0.02)
                ev_skip = next(x for x in received if x.get("message_id") == 9301)
                assert ev_skip["message"][0]["type"] == "record"

                # Allowed group 111 attempt #1 fails (failure 1), triggers rate limit for same user.
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "group",
                            "group_id": 111,
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9302,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "a.amr"}}],
                        }
                    )
                )
                stt1 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                await napcat_ws.send(
                    json.dumps(
                        {"status": "failed", "retcode": 1200, "data": None, "echo": stt1["echo"]}
                    )
                )

                # Same user second message within 60s -> rate-limited without RPC!
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "group",
                            "group_id": 111,
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9303,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "b.amr"}}],
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("message_id") == 9303 for x in received):
                        break
                    await asyncio.sleep(0.02)
                assert next(x for x in received if x.get("message_id") == 9303)["message"] == [
                    {"type": "text", "data": {"text": relay.config.rate_limited_text}}
                ]

                # Second failure opens circuit breaker.
                now_t[0] += 61.0
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "group",
                            "group_id": 111,
                            "user_id": 20002,
                            "self_id": 3554766829,
                            "message_id": 9304,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "c.amr"}}],
                        }
                    )
                )
                stt2 = json.loads(await asyncio.wait_for(napcat_ws.recv(), timeout=2.0))
                await napcat_ws.send(
                    json.dumps(
                        {"status": "failed", "retcode": 1200, "data": None, "echo": stt2["echo"]}
                    )
                )
                for _ in range(40):
                    if any(x.get("message_id") == 9304 for x in received):
                        break
                    await asyncio.sleep(0.02)

                # Different user while circuit open -> immediately skipped without RPC.
                await napcat_ws.send(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": 30003,
                            "self_id": 3554766829,
                            "message_id": 9305,
                            "time": now,
                            "message": [{"type": "record", "data": {"file": "d.amr"}}],
                        }
                    )
                )
                for _ in range(40):
                    if any(x.get("message_id") == 9305 for x in received):
                        break
                    await asyncio.sleep(0.02)
                assert next(x for x in received if x.get("message_id") == 9305)["message"] == [
                    {"type": "text", "data": {"text": relay.config.rate_limited_text}}
                ]

            # Closing NapCat side cascades and closes upstream session cleanly.
            await asyncio.wait_for(upstream_closed.wait(), timeout=2.0)
        finally:
            await relay.close()
