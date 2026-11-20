"""Tests for built-in voice_companion SystemModule, WS protocol, privacy, and admin API."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.websockets import WebSocketDisconnect

from hakimi.app import create_app
from hakimi.security import Rejected, configured_secrets
from hakimi.voice_companion import SentenceStreamBuffer
from tests.test_admin import TOKEN
from tests.test_delivery_resilience import TINY_PNG_B64

COMPANION_TOKEN = "companion-test-secret-token-32-chars-min"
COMPANION_PATH = "/ws/companion/abcdef0123456789abcdef0123456789"


def _companion_settings(settings, **update):
    return settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "companion_ws_enabled": True,
            "companion_ws_token": SecretStr(COMPANION_TOKEN),
            "companion_ws_path": COMPANION_PATH,
            **update,
        }
    )


def _headers(**extra):
    return {
        "Authorization": "Bearer " + COMPANION_TOKEN,
        "X-Companion-Adapter": "windows_desktop",
        "X-Companion-Client-Id": "boiser-pc-1",
        **extra,
    }


def test_sentence_stream_buffer_splitting_silent_and_scrubbing():
    buf = SentenceStreamBuffer(["secret-tok-999"])
    chunks = []
    chunks.extend(buf.feed("**第一句**先测试"))
    chunks.extend(buf.feed("标点！再看看这一整段超过十四个字的、不带敏感词"))
    chunks.extend(buf.feed("的话能不能稳稳拆分。尾句"))
    chunks.extend(buf.flush())
    assert chunks == [
        "第一句先测试标点!",
        "再看看这一整段超过十四个字的、",
        "不带敏感词的话能不能稳稳拆分。",
        "尾句",
    ]
    assert all("*" not in c for c in chunks)

    leaked = SentenceStreamBuffer(["secret-tok-999"])
    with pytest.raises(Rejected, match="invalid_output"):
        leaked.feed("口令是secret-tok-999。")

    silent_buf = SentenceStreamBuffer([], allow_silent=True)
    assert silent_buf.feed("[SIL") == []
    assert silent_buf.feed("ENT]") == []
    assert silent_buf.flush() == []
    assert silent_buf.silent is True

    chat_buf = SentenceStreamBuffer([], allow_silent=True)
    assert chat_buf.feed("[SIL") == []
    assert chat_buf.feed("错啦这波好帅！") == ["[SIL错啦这波好帅!"]
    assert chat_buf.flush() == []
    assert chat_buf.silent is False


def test_companion_ws_startup_validation_and_endpoint_auth(settings):
    bad_s = _companion_settings(settings, companion_ws_token=SecretStr("too-short"))
    app = create_app(bad_s, start_workers=False)
    with pytest.raises(RuntimeError, match="32-character"):
        with TestClient(app):
            pass

    s = _companion_settings(settings)
    assert COMPANION_TOKEN in configured_secrets(s)
    app = create_app(s, start_workers=False)
    with TestClient(app) as client:
        for path, hdr in (
            ("/ws/companion", _headers()),
            (COMPANION_PATH + "?tok=x", _headers()),
            (COMPANION_PATH, _headers(Authorization="Bearer wrong")),
            (COMPANION_PATH, _headers(**{"X-Companion-Adapter": "BAD ADAPTER!"})),
            (COMPANION_PATH, _headers(**{"X-Companion-Client-Id": "bad/client"})),
        ):
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect(path, headers=hdr):
                    pytest.fail("unauthenticated companion socket accepted")

        with client.websocket_connect(COMPANION_PATH, headers=_headers()) as ws:
            ws.send_json({"type": "ping"})
            pong = ws.receive_json()
            assert pong["type"] == "pong"


class _StreamMockGateway:
    def __init__(self, *, deltas=None, delay=0.0, block_output=False):
        self.deltas = deltas or ["第一句。", "第二句！"]
        self.delay = delay
        self.block_output = block_output
        self.calls = []
        self.screen_calls = []

    async def screen(self, payload, subject, **kwargs):
        self.screen_calls.append((payload, subject, kwargs.get("purpose")))
        if kwargs.get("purpose") == "output" and self.block_output:
            raise Rejected("content_blocked")
        return "chat"

    async def generate(self, payload, subject, **kwargs):
        self.calls.append((payload, subject, kwargs))
        on_text = kwargs.get("on_text")
        cancel_event = kwargs.get("cancel_event")
        out = []
        for piece in self.deltas:
            if self.delay:
                await asyncio.sleep(self.delay)
            if cancel_event is not None and cancel_event.is_set():
                raise asyncio.CancelledError
            out.append(piece)
            if on_text is not None:
                res = on_text(piece)
                if hasattr(res, "__await__"):
                    await res
        full = "".join(out)
        if kwargs.get("message") and kwargs["message"].kind != "private":
            await self.screen({"generated_text": full}, subject, purpose="output")
        return full

    async def close(self):
        pass


class _SocketDouble:
    def __init__(self):
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict] = asyncio.Queue()
        self.closed_codes: list[int] = []

    async def push(self, obj: dict) -> None:
        await self.incoming.put(json.dumps(obj, ensure_ascii=False))

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

    async def recv_until(self, predicate, *, max_wait=2.0) -> list[dict]:
        seen = []
        async with asyncio.timeout(max_wait):
            while True:
                msg = await self.outgoing.get()
                seen.append(msg)
                if predicate(msg):
                    return seen


async def test_owner_private_lifecycle_memory_barge_in_and_recap(settings, tmp_path):
    now = [1_795_000_000.0]
    s = _companion_settings(
        settings,
        enable_notebook_snapshot=True,
        notebook_snapshot_path=tmp_path / "owner-nb.md",
    )
    gw = _StreamMockGateway(deltas=["诶！", "这波操作太帅了！", "左边好像有宝箱。"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.voice_companion.clock = lambda: now[0]
        svc.notebook.text = "主人喜欢高机动近战配装"
        await svc.event_ledger.post(
            "farm",
            "crop_ready",
            "西边地里草莓明天熟了",
            dedupe_key="farm:strawberry",
            visibility="owner_private",
            now=now[0],
        )

        sock = _SocketDouble()
        runner = asyncio.create_task(
            svc.voice_companion.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="pc-main",
            )
        )
        await sock.push({"type": "session.start", "mode": "owner_private", "game_title": "终末地"})
        ready = (await sock.recv_until(lambda m: m["type"] == "session.ready"))[-1]
        sid = ready["session_id"]
        assert ready["mode"] == "owner_private"
        assert svc.world_state.current.current_module == "voice_companion"
        assert svc.world_state.current.focus_level == "HIGH"

        # 1. Speech turn with tiny PNG screenshot
        await sock.push(
            {
                "type": "speech.turn",
                "turn_id": "t-1",
                "text": "快看我这波闪避反击",
                "frames": [TINY_PNG_B64],
            }
        )
        ev1 = await sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-1")
        chunks1 = [m["text"] for m in ev1 if m.get("type") == "speak.chunk"]
        assert ev1[-1]["status"] == "spoken"
        assert chunks1 == ["诶!", "这波操作太帅了!", "左边好像有宝箱。"]

        gen_payload, _, gen_kw = gw.calls[0]
        assert gen_kw["scenario"] == "voice.companion"
        assert gen_kw["budget_lane"] == "chat"
        assert len(gen_kw["images"]) == 1
        assert "主人喜欢高机动近战配装" in gen_payload["owner_notebook_snapshot"]
        assert any(
            it["summary"] == "西边地里草莓明天熟了" for it in gen_payload["pending_ledger_events"]
        )

        # 2. Screen pulse hits cooldown immediately (0 LLM calls)
        await sock.push({"type": "screen.pulse", "turn_id": "p-cool", "frames": [TINY_PNG_B64]})
        ev_cool = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "p-cool"
        )
        assert ev_cool[-1] == {
            "type": "turn.end",
            "turn_id": "p-cool",
            "status": "skipped",
            "reason": "cooldown",
        }
        assert len(gw.calls) == 1

        # 3. Screen pulse after cooldown with [SILENT] produces zero speak.chunks
        now[0] += 30.0
        gw.deltas = ["[SIL", "ENT]"]
        await sock.push({"type": "screen.pulse", "turn_id": "p-silent", "frames": [TINY_PNG_B64]})
        ev_sil = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "p-silent"
        )
        assert not any(m.get("type") == "speak.chunk" for m in ev_sil)
        assert ev_sil[-1]["status"] == "silent" and ev_sil[-1]["reason"] == "no_comment"
        assert gw.calls[1][2]["budget_lane"] == "proactive"

        # 3b. Leaked secret chunk is blocked by safe_output with Rejected("invalid_output")
        gw.deltas = [f"口令是{COMPANION_TOKEN}！"]
        await sock.push({"type": "speech.turn", "turn_id": "t-leak", "text": "念一遍口令"})
        ev_leak = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-leak"
        )
        assert not any(m.get("type") == "speak.chunk" for m in ev_leak)
        assert ev_leak[-1]["status"] == "error" and ev_leak[-1]["reason"] == "invalid_output"
        assert COMPANION_TOKEN not in json.dumps(ev_leak)

        # 4. Barge-in turn.cancel aborts slow speech turn cleanly
        gw.deltas = ["慢慢说", "第一句。"]
        gw.delay = 0.2
        await sock.push({"type": "speech.turn", "turn_id": "t-slow", "text": "帮我分析下这图"})
        await sock.recv_until(lambda m: m.get("type") == "turn.start" and m["turn_id"] == "t-slow")
        await sock.push({"type": "turn.cancel", "turn_id": "t-slow"})
        ev_can = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-slow"
        )
        assert ev_can[-1]["status"] == "cancelled" and ev_can[-1]["reason"] == "barge_in"

        # 5. session.stop restores world_state, seals SQLite, and records owner_private recap
        await sock.push({"type": "session.stop"})
        ended = await sock.recv_until(lambda m: m.get("type") == "session.ended")
        assert ended[-1]["session_id"] == sid
        assert svc.world_state.current.current_module == "idle"
        assert svc.world_state.current.focus_level == "LOW"

        raw_row = await svc.store.one(
            "SELECT status, spoken_turns, frames_analyzed, body_blob "
            "FROM voice_companion_sessions WHERE id=?",
            (sid,),
        )
        assert raw_row and raw_row[0] == "ended" and raw_row[1] == 1 and raw_row[2] >= 2
        assert "终末地" not in str(raw_row[3]) and "闪避反击" not in str(raw_row[3])

        ledger_items = await svc.event_ledger.read(include_owner_private=True, now=now[0])
        recaps = [it for it in ledger_items if it.source_module == "voice_companion"]
        assert len(recaps) == 1
        assert recaps[0].visibility == "owner_private"
        assert "终末地" in recaps[0].summary
        assert COMPANION_TOKEN not in recaps[0].summary

        await sock.close()
        await asyncio.wait_for(runner, 2.0)


async def test_room_public_privacy_isolation_and_output_screening(settings, tmp_path):
    now = [1_795_100_000.0]
    s = _companion_settings(
        settings,
        enable_notebook_snapshot=True,
        notebook_snapshot_path=tmp_path / "owner-nb.md",
    )
    gw = _StreamMockGateway(deltas=["公开房一句！"], block_output=False)
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.voice_companion.clock = lambda: now[0]
        svc.notebook.text = "OWNER-SECRET-NOTEBOOK-SENTINEL"
        await svc.event_ledger.post(
            "qq_owner",
            "owner_commitment",
            "OWNER-PRIVATE-LEDGER-SENTINEL",
            dedupe_key="chronicle:secret1",
            visibility="owner_private",
            now=now[0],
        )

        sock = _SocketDouble()
        runner = asyncio.create_task(
            svc.voice_companion.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                adapter_id="discord_voice",
                client_id="guild-1",
            )
        )
        await sock.push(
            {
                "type": "session.start",
                "mode": "room_public",
                "game_title": "Apex",
                "is_owner": True,
            }
        )
        ready = (await sock.recv_until(lambda m: m["type"] == "session.ready"))[-1]
        assert ready["mode"] == "room_public"

        await sock.push(
            {
                "type": "speech.turn",
                "turn_id": "t-pub-1",
                "speaker_id": "999888",
                "speaker_name": "路人甲",
                "text": "这把跳哪",
                "is_owner": True,
            }
        )
        ev1 = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-pub-1"
        )
        assert ev1[-1]["status"] == "spoken"
        assert [m["text"] for m in ev1 if m.get("type") == "speak.chunk"] == ["公开房一句!"]

        payload, _, kw = gw.calls[0]
        dumped = json.dumps(payload, ensure_ascii=False)
        assert "OWNER-SECRET-NOTEBOOK-SENTINEL" not in dumped
        assert "OWNER-PRIVATE-LEDGER-SENTINEL" not in dumped
        assert "owner_notebook_snapshot" not in payload
        assert "pending_ledger_events" not in payload
        assert kw["on_text"] is None
        assert len(gw.screen_calls) == 1 and gw.screen_calls[0][2] == "output"

        # Output safety block prevents any speak.chunk from leaving
        gw.block_output = True
        await sock.push({"type": "speech.turn", "turn_id": "t-pub-2", "text": "再说一句"})
        ev2 = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-pub-2"
        )
        assert not any(m.get("type") == "speak.chunk" for m in ev2)
        assert ev2[-1]["status"] == "error" and ev2[-1]["reason"] == "content_blocked"

        await sock.push({"type": "session.stop"})
        await sock.recv_until(lambda m: m.get("type") == "session.ended")
        await sock.close()
        await asyncio.wait_for(runner, 2.0)

        # No owner_private recap posted for room_public; sanitized milestone published instead
        ledger_items = await svc.event_ledger.read(include_owner_private=True, now=now[0])
        assert not any(it.source_module == "voice_companion" for it in ledger_items)
        milestone = await svc.store.one(
            "SELECT event_type, payload_json FROM global_events "
            "WHERE source_module='voice_companion' ORDER BY created_at DESC LIMIT 1"
        )
        assert milestone and milestone[0] == "session_ended"
        assert "这把跳哪" not in milestone[1]


async def test_voice_companion_admin_api_models_and_live_disconnect(settings):
    s = _companion_settings(settings)
    app = create_app(s, gateway_factory=lambda *_: _StreamMockGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert "voice_companion" in sys_mods
            assert sys_mods["voice_companion"]["panel"] == "voice-companion"
            assert sys_mods["voice_companion"]["status"]["state"] == "ready"

            models = (await client.get("/admin/api/models")).json()
            vc_scenes = [
                scene["key"]
                for g in models["groups"]
                if g["id"] == "voice_companion"
                for scene in g["scenes"]
            ]
            assert vc_scenes == ["voice.companion"]

            bad = await client.put(
                "/admin/api/life/voice-companion",
                json={
                    "enabled": True,
                    "proactive_watch_enabled": True,
                    "min_pulse_interval_seconds": 5,
                    "max_frames_per_turn": 1,
                    "save_recap_to_ledger": True,
                    "tts_style_hint": "fast",
                },
            )
            assert bad.status_code == 400

            sock = _SocketDouble()
            runner = asyncio.create_task(
                svc.voice_companion.serve(
                    sock.receive,
                    sock.send,
                    sock.close,
                    source_ip="127.0.0.1",
                    adapter_id="windows_desktop",
                    client_id="pc-admin",
                )
            )
            await asyncio.sleep(0.02)

            updated = await client.put(
                "/admin/api/life/voice-companion",
                json={
                    "enabled": True,
                    "proactive_watch_enabled": False,
                    "min_pulse_interval_seconds": 45,
                    "max_frames_per_turn": 1,
                    "save_recap_to_ledger": True,
                    "tts_style_hint": "轻快简洁",
                },
            )
            assert updated.status_code == 200
            up_data = updated.json()
            assert up_data["online"] is True
            assert up_data["source"] == "sqlite"
            assert up_data["config"]["min_pulse_interval_seconds"] == 45
            assert COMPANION_TOKEN not in json.dumps(up_data)

            synced = (await sock.recv_until(lambda m: m.get("type") == "config.sync"))[-1]
            assert synced["config"]["proactive_watch_enabled"] is False

            disc = await client.post("/admin/api/life/voice-companion/disconnect")
            assert disc.status_code == 200
            assert disc.json()["online"] is False
            await asyncio.wait_for(asyncio.gather(runner, return_exceptions=True), 2.0)

            reset = await client.delete("/admin/api/life/voice-companion")
            assert reset.status_code == 200
            assert reset.json()["source"] == "default"
            assert reset.json()["config"]["min_pulse_interval_seconds"] == 25
