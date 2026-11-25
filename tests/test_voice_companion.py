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
from hakimi.media import ModelImage
from hakimi.security import Rejected, configured_secrets
from hakimi.social import owner_target
from hakimi.voice_companion import SentenceStreamBuffer, VoiceCompanionConfig
from tests.test_admin import TOKEN
from tests.test_delivery_resilience import TINY_PNG_B64
from tests.test_owner_cloud_memory import _populate_memory

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

        # 2. Legacy screen.pulse on /ws/companion is rejected cleanly (moved to screen_watch)
        await sock.push({"type": "screen.pulse", "turn_id": "legacy", "frames": [TINY_PNG_B64]})
        ev_unk = await sock.recv_until(lambda m: m.get("type") == "error")
        assert ev_unk[-1]["reason"] == "companion_unknown_frame"
        assert len(gw.calls) == 1

        # 3. Screen commentary dispatched from screen_watch with [SILENT] emits zero speak.chunks
        now[0] += 30.0
        gw.deltas = ["[SIL", "ENT]"]
        outcome = await svc.voice_companion.trigger_screen_commentary(
            [ModelImage(data=TINY_PNG_B64, mime_type="image/png")],
            game_title="终末地",
            turn_id="p-silent",
        )
        assert outcome == "dispatched"
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
                    "use_screen_watch": "yes",
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
                    "use_screen_watch": False,
                    "save_recap_to_ledger": True,
                    "tts_style_hint": "轻快简洁",
                },
            )
            assert updated.status_code == 200
            up_data = updated.json()
            assert up_data["online"] is True
            assert up_data["source"] == "sqlite"
            assert up_data["config"]["use_screen_watch"] is False
            assert COMPANION_TOKEN not in json.dumps(up_data)

            synced = (await sock.recv_until(lambda m: m.get("type") == "config.sync"))[-1]
            assert synced["config"]["use_screen_watch"] is False

            disc = await client.post("/admin/api/life/voice-companion/disconnect")
            assert disc.status_code == 200
            assert disc.json()["online"] is False
            await asyncio.wait_for(asyncio.gather(runner, return_exceptions=True), 2.0)

            reset = await client.delete("/admin/api/life/voice-companion")
            assert reset.status_code == 200
            assert reset.json()["source"] == "default"
            assert reset.json()["config"]["use_screen_watch"] is True


def _snapshot_dir(root):
    return {
        str(p.relative_to(root)): (p.stat().st_mtime_ns, p.read_bytes())
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


async def test_voice_companion_dual_memory_roundtrip_and_cloud_memory_readonly(settings, tmp_path):
    now = [1_795_200_000.0]
    mem_dir = _populate_memory(tmp_path, settings.gemini_api_key.get_secret_value())
    before_mem = _snapshot_dir(mem_dir)
    s = _companion_settings(
        settings,
        enable_antigravity_memory=True,
        antigravity_memory_dir=mem_dir,
    )
    gw = _StreamMockGateway(deltas=["莱万汀的基建可以先放电力站！"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.voice_companion.clock = lambda: now[0]
        owner_msg = owner_target(svc.s, "onebot", now[0], "vc-dual-check")
        assert owner_msg is not None

        # Session 1: owner_private spoken turn -> session.stop persists DualMemory, ledger, identity
        sock1 = _SocketDouble()
        runner1 = asyncio.create_task(
            svc.voice_companion.serve(
                sock1.receive,
                sock1.send,
                sock1.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="pc-dual-1",
            )
        )
        await sock1.push({"type": "session.start", "mode": "owner_private", "game_title": "终末地"})
        await sock1.recv_until(lambda m: m["type"] == "session.ready")
        await sock1.push(
            {
                "type": "speech.turn",
                "turn_id": "t-dual-1",
                "text": "帮我看下莱万汀的基建排版",
            }
        )
        ev1 = await sock1.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-dual-1"
        )
        assert ev1[-1]["status"] == "spoken"
        await sock1.push({"type": "session.stop"})
        await sock1.recv_until(lambda m: m.get("type") == "session.ended")
        await sock1.close()
        await asyncio.wait_for(runner1, 2.0)

        ledger_items = await svc.event_ledger.read(include_owner_private=True, now=now[0])
        vc_ledger = [it for it in ledger_items if it.source_module == "voice_companion"]
        assert len(vc_ledger) == 1
        assert vc_ledger[0].visibility == "owner_private"

        id_recaps = svc.identity.owner_other_surface_recent("onebot", now=now[0])
        assert len(id_recaps) == 1
        assert id_recaps[0]["platform"] == "voice_companion"
        assert "莱万汀" in id_recaps[0]["summary"]

        view = await svc.sessions.dual.view(owner_msg)
        assert len(view["entries"]) == 1
        entry = view["entries"][0]
        assert entry["kind"] == "chronicle"
        assert entry["topic"].startswith("语音伴随:终末地:")
        assert "莱万汀" in entry["text"]
        assert entry["tags"] == ["语音伴随", "连麦", "终末地"]
        assert "莱万汀".encode() not in svc.sessions.path(owner_msg).read_bytes()
        assert _snapshot_dir(mem_dir) == before_mem

        # Session 2: next owner_private session receives owner_dual_memory in prompt payload
        now[0] += 120.0
        sock2 = _SocketDouble()
        runner2 = asyncio.create_task(
            svc.voice_companion.serve(
                sock2.receive,
                sock2.send,
                sock2.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="pc-dual-2",
            )
        )
        await sock2.push({"type": "session.start", "mode": "owner_private", "game_title": "终末地"})
        await sock2.recv_until(lambda m: m["type"] == "session.ready")
        await sock2.push(
            {
                "type": "speech.turn",
                "turn_id": "t-dual-2",
                "text": "刚才我们聊的莱万汀怎么样",
            }
        )
        ev2 = await sock2.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-dual-2"
        )
        assert ev2[-1]["status"] == "spoken"
        gen_payload = gw.calls[-1][0]
        assert "owner_dual_memory" in gen_payload
        assert any(
            "莱万汀" in item for item in gen_payload["owner_dual_memory"]["chronicle_recall"]
        )

        await sock2.push({"type": "session.stop"})
        await sock2.recv_until(lambda m: m.get("type") == "session.ended")
        await sock2.close()
        await asyncio.wait_for(runner2, 2.0)
        assert _snapshot_dir(mem_dir) == before_mem


async def test_voice_companion_room_public_and_save_recap_to_dual_memory_disabled(settings):
    now = [1_795_300_000.0]
    s = _companion_settings(settings)
    gw = _StreamMockGateway(deltas=["好的呢！"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.voice_companion.clock = lambda: now[0]
        owner_msg = owner_target(svc.s, "onebot", now[0], "vc-iso-check")
        assert owner_msg is not None

        # 1. room_public must NOT write to Owner DualMemory or owner_recaps
        sock_pub = _SocketDouble()
        runner_pub = asyncio.create_task(
            svc.voice_companion.serve(
                sock_pub.receive,
                sock_pub.send,
                sock_pub.close,
                source_ip="127.0.0.1",
                adapter_id="discord_voice",
                client_id="guild-pub",
            )
        )
        await sock_pub.push({"type": "session.start", "mode": "room_public", "game_title": "Apex"})
        await sock_pub.recv_until(lambda m: m["type"] == "session.ready")
        await sock_pub.push(
            {
                "type": "speech.turn",
                "turn_id": "t-pub-iso",
                "speaker_id": "888",
                "speaker_name": "队友",
                "text": "这把跳哪",
            }
        )
        await sock_pub.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-pub-iso"
        )
        await sock_pub.push({"type": "session.stop"})
        await sock_pub.recv_until(lambda m: m.get("type") == "session.ended")
        await sock_pub.close()
        await asyncio.wait_for(runner_pub, 2.0)

        assert not svc.sessions.path(owner_msg).exists()
        assert svc.identity.owner_other_surface_recent("onebot", now=now[0]) == []
        assert not [
            it
            for it in await svc.event_ledger.read(include_owner_private=True, now=now[0])
            if it.source_module == "voice_companion"
        ]

        # 2. save_recap_to_dual_memory=False skips DualMemory write while keeping ledger/recaps
        await svc.voice_companion.set_config(VoiceCompanionConfig(save_recap_to_dual_memory=False))
        assert svc.voice_companion.config.save_recap_to_dual_memory is False

        sock_priv = _SocketDouble()
        runner_priv = asyncio.create_task(
            svc.voice_companion.serve(
                sock_priv.receive,
                sock_priv.send,
                sock_priv.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="pc-no-dual",
            )
        )
        await sock_priv.push(
            {"type": "session.start", "mode": "owner_private", "game_title": "星铁"}
        )
        await sock_priv.recv_until(lambda m: m["type"] == "session.ready")
        await sock_priv.push(
            {
                "type": "speech.turn",
                "turn_id": "t-priv-nodual",
                "text": "帮我看下忘却之庭配队",
            }
        )
        await sock_priv.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-priv-nodual"
        )
        await sock_priv.push({"type": "session.stop"})
        await sock_priv.recv_until(lambda m: m.get("type") == "session.ended")
        await sock_priv.close()
        await asyncio.wait_for(runner_priv, 2.0)

        ledger_after = [
            it
            for it in await svc.event_ledger.read(include_owner_private=True, now=now[0])
            if it.source_module == "voice_companion"
        ]
        assert len(ledger_after) == 1 and "星铁" in ledger_after[0].summary
        assert len(svc.identity.owner_other_surface_recent("onebot", now=now[0])) == 1
        assert not svc.sessions.path(owner_msg).exists()


async def test_barge_in_playout_cursor_clips_recent_turns_accurately(settings):
    now = [1_795_100_000.0]
    s = _companion_settings(settings)
    gw = _StreamMockGateway(deltas=["第一句。", "第二句！", "第三句不会播。"])
    app = create_app(s, gateway_factory=lambda *_: gw, start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        svc.voice_companion.clock = lambda: now[0]

        sock = _SocketDouble()
        runner = asyncio.create_task(
            svc.voice_companion.serve(
                sock.receive,
                sock.send,
                sock.close,
                source_ip="127.0.0.1",
                adapter_id="windows_desktop",
                client_id="pc-cursor",
            )
        )
        await sock.push({"type": "session.start", "mode": "owner_private", "game_title": "MC"})
        await sock.recv_until(lambda m: m["type"] == "session.ready")

        # 1. Late turn.cancel after turn.end clips recent_turns to played_seq=1
        await sock.push({"type": "speech.turn", "turn_id": "t-clip", "text": "说说看"})
        await sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-clip")
        await sock.push({"type": "turn.cancel", "turn_id": "t-clip", "played_seq": 1})
        await asyncio.sleep(0.03)

        # 2. played_seq=0 removes unheard assistant text while keeping user utterance
        await sock.push({"type": "speech.turn", "turn_id": "t-zero", "text": "这句没听到"})
        await sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-zero")
        await sock.push({"type": "turn.cancel", "turn_id": "t-zero", "played_seq": 0})
        await asyncio.sleep(0.03)

        # 3. Streaming cancellation with played_seq=1 preserves the heard chunk
        gw.delay = 0.15
        await sock.push({"type": "speech.turn", "turn_id": "t-stream", "text": "边说边打断"})
        await sock.recv_until(
            lambda m: (
                m.get("type") == "speak.chunk"
                and m.get("turn_id") == "t-stream"
                and m.get("seq") == 1
            )
        )
        await sock.push({"type": "turn.cancel", "turn_id": "t-stream", "played_seq": 1})
        ev_stream = await sock.recv_until(
            lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-stream"
        )
        assert ev_stream[-1]["status"] == "cancelled"

        # 4. Next turn receives accurately clipped recent_voice_turns
        gw.delay = 0.0
        gw.deltas = ["收到。"]
        await sock.push({"type": "speech.turn", "turn_id": "t-next", "text": "继续吧"})
        await sock.recv_until(lambda m: m.get("type") == "turn.end" and m["turn_id"] == "t-next")

        recent = gw.calls[-1][0]["recent_voice_turns"]
        assert recent[0]["user"] == "说说看"
        assert "第一句" in recent[0]["assistant"]
        assert "第二句" not in recent[0]["assistant"]
        assert "被打断" in recent[0]["assistant"]

        assert recent[1] == {"at": round(now[0]), "user": "这句没听到"}

        assert recent[2]["user"] == "边说边打断"
        assert "第一句" in recent[2]["assistant"]
        assert "被打断" in recent[2]["assistant"]

        await sock.push({"type": "session.stop"})
        await sock.recv_until(lambda m: m.get("type") == "session.ended")
        await sock.close()
        await asyncio.wait_for(runner, 2.0)
