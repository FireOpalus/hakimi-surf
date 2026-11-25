"""Tests for standalone screen_watch SystemModule, RAM-only ring buffer, routing, and admin API."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect

from hakimi.app import create_app
from hakimi.gemini import GeminiGateway
from hakimi.models import Message
from hakimi.security import Rejected, configured_secrets
from tests.test_admin import TOKEN
from tests.test_delivery_resilience import TINY_PNG_B64
from tests.test_voice_companion import (
    COMPANION_PATH,
    COMPANION_TOKEN,
    _companion_settings,
    _SocketDouble,
    _StreamMockGateway,
)

SCREEN_TOKEN = "screen-watch-secret-token-32-chars-minimum"
SCREEN_PATH = "/ws/screen/0123456789abcdef0123456789abcdef"


def _screen_settings(settings, **update):
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "screen_ws_enabled": True,
            "screen_ws_token": SecretStr(SCREEN_TOKEN),
            "screen_ws_path": SCREEN_PATH,
            **update,
        }
    )


def _screen_headers(**extra):
    return {
        "Authorization": "Bearer " + SCREEN_TOKEN,
        "X-Screen-Adapter": "windows_capture",
        "X-Screen-Client-Id": "desktop-screen-1",
        **extra,
    }


def test_screen_ws_startup_validation_and_endpoint_auth(settings):
    bad_s = _screen_settings(settings, screen_ws_token=SecretStr("short"))
    app = create_app(bad_s, start_workers=False)
    with pytest.raises(RuntimeError, match="32-character"):
        with TestClient(app):
            pass

    s = _screen_settings(settings)
    assert SCREEN_TOKEN in configured_secrets(s)
    app = create_app(s, start_workers=False)
    with TestClient(app) as client:
        for path, hdr in (
            ("/ws/screen", _screen_headers()),
            (SCREEN_PATH + "?tok=x", _screen_headers()),
            (SCREEN_PATH, _screen_headers(Authorization="Bearer wrong")),
            (SCREEN_PATH, _screen_headers(**{"X-Screen-Adapter": "BAD ADAPTER!"})),
            (SCREEN_PATH, _screen_headers(**{"X-Screen-Client-Id": "bad/client"})),
        ):
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(path, headers=hdr):
                    pytest.fail("unauthenticated screen_watch socket accepted")

        with client.websocket_connect(SCREEN_PATH, headers=_screen_headers()) as ws:
            ws.send_json({"type": "ping"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"


async def test_screen_watch_ram_ring_ttl_pause_privacy_and_zero_disk_persistence(settings):
    now = [1_795_200_000.0]
    s = _screen_settings(settings)
    gw = _StreamMockGateway()
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        sw = svc.screen_watch
        sw.clock = lambda: now[0]

        sock = _SocketDouble()
        runner = asyncio.create_task(
            sw.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                adapter_id="windows_capture",
                client_id="win-1",
            )
        )
        await sock.push(
            {"type": "watch.start", "game_title": "终末地", "window_title": "Endfield.exe"}
        )
        ready = (await sock.recv_until(lambda m: m["type"] == "watch.ready"))[-1]
        assert ready["game_title"] == "终末地" and ready["paused"] is False

        await sock.push({"type": "frame.push", "frames": [TINY_PNG_B64], "change_score": 0.42})
        ack1 = (await sock.recv_until(lambda m: m["type"] == "frame.ack"))[-1]
        assert ack1["accepted"] == 1 and ack1["buffered"] == 1

        # Strictly Owner-private: non-owner lookups always get empty frames
        assert sw.latest_frames(owner_only=False, now=now[0] + 2.0) == ([], "")
        imgs, game = sw.latest_frames(owner_only=True, now=now[0] + 5.0)
        assert len(imgs) == 1 and game == "终末地"
        img_b64_sample = imgs[0].data[:24].encode("ascii")

        # Auto-expire after default TTL (15s)
        now[0] += 16.0
        assert sw.latest_frames(owner_only=True, now=now[0]) == ([], "终末地")

        # Pause wipes existing frames and rejects new pushes until resumed
        await sock.push({"type": "frame.push", "frames": [TINY_PNG_B64, TINY_PNG_B64]})
        ack2 = (await sock.recv_until(lambda m: m["type"] == "frame.ack"))[-1]
        assert ack2["accepted"] == 2 and ack2["buffered"] == 2

        await sock.push({"type": "watch.pause"})
        paused = (await sock.recv_until(lambda m: m["type"] == "watch.paused"))[-1]
        assert paused["cleared"] == 2
        assert sw.latest_frames(owner_only=True, now=now[0]) == ([], "")

        await sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "p1",
            }
        )
        ev_paused = await sock.recv_until(lambda m: m.get("type") == "commentary.result")
        assert ev_paused[0]["reason"] == "screen_watch_paused" and ev_paused[0]["accepted"] == 0
        assert (
            ev_paused[-1]["status"] == "skipped"
            and ev_paused[-1]["reason"] == "screen_watch_paused"
        )

        await sock.push({"type": "watch.resume"})
        await sock.recv_until(lambda m: m["type"] == "watch.resumed")
        await sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
        ack3 = (await sock.recv_until(lambda m: m["type"] == "frame.ack"))[-1]
        assert ack3["accepted"] == 1 and ack3["buffered"] == 1

        # Verify zero screen-frame tables and zero image bytes written to SQLite
        tables = await svc.store.all("SELECT name FROM sqlite_master WHERE type='table'")
        assert not any(
            "screen" in r["name"].lower() or "frame" in r["name"].lower() for r in tables
        )
        assert img_b64_sample not in (s.data_dir / "hakimi.sqlite3").read_bytes()

        # Closing / disconnecting wipes memory buffer immediately
        await sock.push({"type": "watch.stop"})
        stopped = (await sock.recv_until(lambda m: m["type"] == "watch.stopped"))[-1]
        assert stopped["cleared"] == 1
        assert sw.latest_frames(owner_only=True, now=now[0]) == ([], "")

        await sock.close()
        await asyncio.wait_for(runner, 2.0)


async def test_screen_watch_voice_companion_coordination_and_room_public_isolation(settings):
    now = [1_795_300_000.0]
    s = _companion_settings(
        _screen_settings(settings),
        companion_ws_enabled=True,
        companion_ws_token=SecretStr(COMPANION_TOKEN),
        companion_ws_path=COMPANION_PATH,
    )
    gw = _StreamMockGateway(deltas=["看我看到了，", "左边有怪！"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.screen_watch.clock = lambda: now[0]
        svc.voice_companion.clock = lambda: now[0]

        sw_sock = _SocketDouble()
        sw_task = asyncio.create_task(
            svc.screen_watch.serve(
                sw_sock.receive,
                sw_sock.send,
                sw_sock.close,
                source_ip="127.0.0.1",
                adapter_id="win_cap",
                client_id="screen-1",
            )
        )
        await sw_sock.push({"type": "watch.start", "game_title": "艾尔登法环"})
        await sw_sock.recv_until(lambda m: m["type"] == "watch.ready")
        await sw_sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
        await sw_sock.recv_until(lambda m: m["type"] == "frame.ack")

        vc_sock = _SocketDouble()
        vc_task = asyncio.create_task(
            svc.voice_companion.serve(
                vc_sock.receive,
                vc_sock.send,
                vc_sock.close,
                source_ip="127.0.0.1",
                adapter_id="discord_voice",
                client_id="vc-1",
            )
        )
        # 1. room_public MUST NEVER access screen_watch frames or game_title
        await vc_sock.push({"type": "session.start", "mode": "room_public"})
        await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
        await vc_sock.push({"type": "speech.turn", "turn_id": "pub-1", "text": "帮我看下屏幕"})
        await vc_sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "pub-1")
        pub_payload, _, pub_kw = gw.calls[-1]
        assert pub_kw["images"] is None
        assert pub_payload["game_title"] == ""

        # Proactive commentary must NOT route into room_public voice
        now[0] += 30.0
        await sw_sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "sw-no-pub",
            }
        )
        ev_overlay = await sw_sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "sw-no-pub"
        )
        assert ev_overlay[-1]["status"] == "overlay"
        assert vc_sock.outgoing.empty()

        # 2. Switch voice_companion to owner_private -> pulls screen_watch frames automatically
        now[0] += 30.0
        await vc_sock.push({"type": "session.start", "mode": "owner_private"})
        await vc_sock.recv_until(lambda m: m["type"] == "session.ready")
        await sw_sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
        await sw_sock.recv_until(lambda m: m["type"] == "frame.ack")

        await vc_sock.push({"type": "speech.turn", "turn_id": "priv-1", "text": "这boss怎么打"})
        await vc_sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "priv-1")
        priv_payload, _, priv_kw = gw.calls[-1]
        assert len(priv_kw["images"]) == 1
        assert priv_payload["game_title"] == "艾尔登法环"

        # Immediately after speech turn, screen commentary hits cooldown via note_spoken()
        await sw_sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "sw-cool",
            }
        )
        ev_cool = await sw_sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "sw-cool"
        )
        assert ev_cool[-1] == {
            "type": "commentary.result",
            "turn_id": "sw-cool",
            "status": "skipped",
            "reason": "cooldown",
        }

        # 3. After cooldown, frame.push(request_commentary=True) routes to owner_private voice!
        now[0] += 30.0
        gw.deltas = ["小心背后偷袭！"]
        await sw_sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "sw-voice-1",
            }
        )
        ev_routed = await sw_sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "sw-voice-1"
        )
        assert ev_routed[-1]["status"] == "routed_voice"

        vc_ev = await vc_sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "sw-voice-1"
        )
        assert vc_ev[0]["type"] == "turn.start" and vc_ev[0]["kind"] == "screen_pulse"
        assert [m["text"] for m in vc_ev if m.get("type") == "speak.chunk"] == ["小心背后偷袭!"]
        assert vc_ev[-1]["status"] == "spoken"

        # 4. When voice_companion is busy speaking, screen_watch reports voice_busy
        now[0] += 30.0
        gw.delay = 0.25
        await vc_sock.push({"type": "speech.turn", "turn_id": "slow-1", "text": "慢慢分析下"})
        await vc_sock.recv_until(
            lambda m: m.get("type") == "turn.start" and m["turn_id"] == "slow-1"
        )
        await sw_sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "sw-busy",
            }
        )
        ev_busy = await sw_sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "sw-busy"
        )
        assert ev_busy[-1]["status"] == "skipped" and ev_busy[-1]["reason"] == "voice_busy"
        gw.delay = 0.0
        await vc_sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "slow-1")

        await vc_sock.push({"type": "session.stop"})
        await vc_sock.recv_until(lambda m: m["type"] == "session.ended")
        await vc_sock.close()
        await sw_sock.close()
        await asyncio.gather(vc_task, sw_task)


async def test_screen_watch_standalone_overlay_qq_fallback_silent_and_privacy_gate(settings):
    now = [1_795_400_000.0]
    s = _screen_settings(settings)
    gw = _StreamMockGateway(deltas=["[SILENT]"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        sw = svc.screen_watch
        sw.clock = lambda: now[0]
        svc.proactive.clock = lambda: now[0]

        # Real GeminiGateway guard blocks scenario="screen.watch" outside Owner private scope
        real_gw = GeminiGateway(s, svc.store)
        try:
            non_owner = Message(
                platform="onebot",
                bot_id="10000",
                event_id="g1",
                user_id="30000",
                kind="group",
                conversation_id="40000",
                text="peek",
                addressed=True,
                created_at=now[0],
            )
            with pytest.raises(Rejected, match="owner_private_only"):
                await real_gw.generate(
                    {"event_kind": "screen_watch"},
                    "screen:non-owner",
                    scenario="screen.watch",
                    budget_lane="proactive",
                    message=non_owner,
                )
        finally:
            await real_gw.close()

        sock = _SocketDouble()
        runner = asyncio.create_task(
            sw.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                adapter_id="win_cap",
                client_id="screen-2",
            )
        )
        await sock.push({"type": "watch.start", "game_title": "鸣潮"})
        await sock.recv_until(lambda m: m["type"] == "watch.ready")

        # 1. [SILENT] -> silent result, no commentary.ready, no QQ DM
        now[0] += 30.0
        await sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "st-silent",
            }
        )
        res_sil = await sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "st-silent"
        )
        assert not any(m.get("type") == "commentary.ready" for m in res_sil)
        assert res_sil[-1]["status"] == "silent"

        # 2. Normal commentary with deliver_to_qq_when_no_voice=False -> overlay only
        now[0] += 30.0
        gw.deltas = ["这发弹刀好极限！"]
        await sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "st-overlay",
            }
        )
        res_ov = await sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "st-overlay"
        )
        ready_ov = next(m for m in res_ov if m.get("type") == "commentary.ready")
        assert ready_ov["text"] == "这发弹刀好极限!" and ready_ov["delivered_qq"] is False
        assert res_ov[-1]["status"] == "overlay"
        assert not await svc.store.one("SELECT 1 FROM pending_owner_dms WHERE kind='screen_watch'")

        # 3. Enable deliver_to_qq_when_no_voice -> enqueues kind="screen_watch" in pending_owner_dms
        await sw.set_config(sw.config.model_copy(update={"deliver_to_qq_when_no_voice": True}))
        await sock.recv_until(lambda m: m.get("type") == "config.sync")
        now[0] += 30.0
        gw.deltas = ["注意体力快空啦！"]
        await sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "st-qq",
            }
        )
        res_qq = await sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "st-qq"
        )
        ready_qq = next(m for m in res_qq if m.get("type") == "commentary.ready")
        assert ready_qq["delivered_qq"] is True and res_qq[-1]["status"] == "sent_qq"
        dm_row = await svc.store.one(
            "SELECT kind, status FROM pending_owner_dms WHERE kind='screen_watch'"
        )
        assert dm_row and dm_row[0] == "screen_watch" and dm_row[1] == "pending"

        # 4. Leaked secret blocked by safe_output
        now[0] += 30.0
        gw.deltas = [f"口令是{SCREEN_TOKEN}！"]
        await sock.push(
            {
                "type": "frame.push",
                "frames": [TINY_PNG_B64],
                "request_commentary": True,
                "turn_id": "st-leak",
            }
        )
        res_leak = await sock.recv_until(
            lambda m: m.get("type") == "commentary.result" and m["turn_id"] == "st-leak"
        )
        assert not any(m.get("type") == "commentary.ready" for m in res_leak)
        assert res_leak[-1]["status"] == "error" and res_leak[-1]["reason"] == "invalid_output"
        assert SCREEN_TOKEN not in json.dumps(res_leak)

        await sock.close()
        await asyncio.wait_for(runner, 2.0)


async def test_screen_watch_admin_api_models_clear_and_disconnect(settings):
    s = _screen_settings(settings)
    app = create_app(s, gateway_factory=lambda *_: _StreamMockGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert "screen_watch" in sys_mods
            assert sys_mods["screen_watch"]["panel"] == "screen-watch"
            assert sys_mods["screen_watch"]["status"]["state"] == "ready"

            models = (await client.get("/admin/api/models")).json()
            sw_scenes = [
                scene["key"]
                for g in models["groups"]
                if g["id"] == "screen_watch"
                for scene in g["scenes"]
            ]
            assert sw_scenes == ["screen.watch"]

            bad = await client.put(
                "/admin/api/life/screen-watch",
                json={
                    "enabled": True,
                    "proactive_commentary_enabled": True,
                    "min_pulse_interval_seconds": 5,
                    "max_frames_per_query": 2,
                    "frame_ttl_seconds": 15,
                    "deliver_to_qq_when_no_voice": False,
                },
            )
            assert bad.status_code == 400

            sock = _SocketDouble()
            runner = asyncio.create_task(
                svc.screen_watch.serve(
                    sock.receive,
                    sock.send,
                    sock.close,
                    source_ip="127.0.0.1",
                    adapter_id="win_cap",
                    client_id="screen-admin",
                )
            )
            await sock.push({"type": "watch.start", "game_title": "测试游戏"})
            await sock.recv_until(lambda m: m["type"] == "watch.ready")
            await sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
            await sock.recv_until(lambda m: m["type"] == "frame.ack")

            updated = await client.put(
                "/admin/api/life/screen-watch",
                json={
                    "enabled": True,
                    "proactive_commentary_enabled": True,
                    "min_pulse_interval_seconds": 40,
                    "max_frames_per_query": 1,
                    "frame_ttl_seconds": 20,
                    "deliver_to_qq_when_no_voice": True,
                },
            )
            assert updated.status_code == 200
            up_data = updated.json()
            assert up_data["online"] is True and up_data["buffered_frames"] == 1
            assert up_data["source"] == "sqlite"
            assert up_data["config"]["frame_ttl_seconds"] == 20
            assert SCREEN_TOKEN not in json.dumps(up_data)

            synced = (await sock.recv_until(lambda m: m.get("type") == "config.sync"))[-1]
            assert synced["config"]["deliver_to_qq_when_no_voice"] is True

            cleared = await client.post("/admin/api/life/screen-watch/clear")
            assert cleared.status_code == 200 and cleared.json()["buffered_frames"] == 0

            disabled = await client.put(
                "/admin/api/life/screen-watch",
                json={
                    "enabled": False,
                    "proactive_commentary_enabled": True,
                    "min_pulse_interval_seconds": 40,
                    "max_frames_per_query": 1,
                    "frame_ttl_seconds": 20,
                    "deliver_to_qq_when_no_voice": False,
                },
            )
            assert disabled.status_code == 200 and disabled.json()["config"]["enabled"] is False
            await sock.push({"type": "frame.push", "frames": [TINY_PNG_B64]})
            ack_drop = (await sock.recv_until(lambda m: m["type"] == "frame.ack"))[-1]
            assert ack_drop["accepted"] == 0 and ack_drop["reason"] == "screen_watch_disabled"

            disc = await client.post("/admin/api/life/screen-watch/disconnect")
            assert disc.status_code == 200 and disc.json()["online"] is False
            await asyncio.wait_for(asyncio.gather(runner, return_exceptions=True), 2.0)

            reset = await client.delete("/admin/api/life/screen-watch")
            assert reset.status_code == 200
            assert reset.json()["source"] == "default"
            assert reset.json()["config"]["frame_ttl_seconds"] == 15
