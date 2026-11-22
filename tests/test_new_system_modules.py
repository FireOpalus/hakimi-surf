"""Tests for new resident system modules, wiring, audio attachment reading, and /学歌."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import time
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.file_reader import FileReader
from hakimi.models import FileRef, Message
from hakimi.outbound_actions import ChatActionTrigger
from hakimi.plugin_bus import RESERVED_PLUGIN_IDS
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.store import Store
from hakimi.system_modules import SystemResources
from hakimi.system_modules.gcp_billing import Module as GcpBillingSystemModule
from hakimi.system_modules.voice_companion import Module as VoiceCompanionSystemModule
from hakimi.system_modules.voice_gateway import Module as VoiceGatewaySystemModule
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN


def _colab_settings(settings: Settings, **overrides) -> Settings:
    data = {
        "admin_token": SecretStr(TOKEN),
        "colab_worker_url": "http://127.0.0.1:12146",
        "colab_worker_token": SecretStr("colab-worker-secret-token-123456"),
        "colab_drive_songs_folder_id": "folder_hakimi_songs_1",
    }
    data.update(overrides)
    return settings.model_copy(update=data)


def test_settings_colab_and_song_cache_validation(settings, monkeypatch):
    base = settings.model_dump()
    assert settings.colab_worker_url == ""
    assert settings.colab_worker_token.get_secret_value() == ""
    assert settings.colab_drive_songs_folder_id == ""
    assert settings.song_cache_max_bytes == 30 * 1024 * 1024

    for ok_url in (
        "http://127.0.0.1:12146/",
        "http://localhost:12146",
        "http://[::1]:12146",
        "http://hakimi-colab-worker:12146",
        "http://host.docker.internal:12146",
        "http://172.17.0.1:12146",
    ):
        cfg = Settings(**{**base, "colab_worker_url": ok_url})
        assert cfg.colab_worker_url == ok_url.rstrip("/")

    for bad_url in (
        "http://public.example.com:12146",
        "https://public.example.com:12146",
        "http://169.254.169.254:80",
        "http://2130706433:12146",
        "http://0.0.0.0:12146",
        "http://127.0.0.1:12146?token=1",
        "http://user:pass@127.0.0.1:12146",
        "ftp://127.0.0.1:12146",
        "http://127.0.0.1:12146/subpath",
    ):
        with pytest.raises(ValidationError):
            Settings(**{**base, "colab_worker_url": bad_url})

    with pytest.raises(ValidationError):
        Settings(**{**base, "song_cache_max_bytes": 512 * 1024})
    with pytest.raises(ValidationError):
        Settings(**{**base, "song_cache_max_bytes": 600 * 1024 * 1024})
    with pytest.raises(ValidationError):
        Settings(**{**base, "colab_drive_songs_folder_id": "bad/folder?id"})

    monkeypatch.setenv("COLAB_WORKER_URL", "http://127.0.0.1:19999/")
    monkeypatch.setenv("COLAB_WORKER_TOKEN", "env-colab-tok-1234567890")
    monkeypatch.setenv("COLAB_DRIVE_SONGS_FOLDER_ID", "drive_songs_root_99")
    monkeypatch.setenv("SONG_CACHE_MAX_BYTES", str(40 * 1024 * 1024))
    env_cfg = Settings(
        _env_file=None,
        data_dir=settings.data_dir,
        gemini_api_key=settings.gemini_api_key.get_secret_value(),
        encryption_key=settings.encryption_key.get_secret_value(),
    )
    assert env_cfg.colab_worker_url == "http://127.0.0.1:19999"
    assert env_cfg.colab_worker_token.get_secret_value() == "env-colab-tok-1234567890"
    assert env_cfg.colab_drive_songs_folder_id == "drive_songs_root_99"
    assert env_cfg.song_cache_max_bytes == 40 * 1024 * 1024


async def test_new_system_modules_topology_isolation_and_reserved_ids(settings):
    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        names = list(svc.modules.systems.keys())
        for mod_id in ("colab_compute", "song_studio", "voice_gateway", "gcp_billing"):
            assert mod_id in names
            assert mod_id in RESERVED_PLUGIN_IDS
            assert svc.modules.get(mod_id) is None
            assert not hasattr(svc.modules.systems[mod_id], "enabled")
            assert mod_id not in svc.modules.catalog.owners.values()
            with pytest.raises(Rejected, match="reserved_plugin_id"):
                await svc.plugin_bus.create_plugin(mod_id, "Spoof", ["actions"])

        assert "voice_gateway" in VoiceCompanionSystemModule.requires
        assert names.index("voice_gateway") < names.index("voice_companion")
        assert names.index("colab_compute") < names.index("song_studio")
        assert names.index("drive_sync") < names.index("song_studio")

        assert svc.colab_compute is svc.modules.systems["colab_compute"].compute
        assert svc.songs is svc.modules.systems["song_studio"].songs
        assert svc.store.runtime.songs is svc.songs
        assert svc.voice_reply is svc.modules.systems["voice_gateway"].reply
        assert svc.voice_gateway_admin is svc.modules.systems["voice_gateway"].admin
        assert svc.gcp_billing is svc.modules.systems["gcp_billing"].manager
        assert svc.store.runtime.gcp_billing is svc.gcp_billing

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            assert (
                await client.get("/admin/api/system-modules", headers={"Authorization": ""})
            ).status_code == 401
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert sys_mods["colab_compute"]["panel"] == "colab-compute"
            assert sys_mods["song_studio"]["panel"] == "songs"
            assert sys_mods["voice_gateway"]["panel"] == "voice-companion"
            assert sys_mods["gcp_billing"]["panel"] == "ledger"
            assert sys_mods["voice_gateway"]["status"]["state"] == "unconfigured"

            for mod_id in ("colab_compute", "song_studio", "voice_gateway", "gcp_billing"):
                deny = await client.put(
                    f"/admin/api/life/modules/{mod_id}/enabled", json={"enabled": False}
                )
                assert deny.status_code == 404

            assert (
                await client.get("/admin/api/colab", headers={"Authorization": ""})
            ).status_code == 401
            colab_resp = await client.get("/admin/api/colab")
            assert colab_resp.status_code == 200
            colab_ov = colab_resp.json()
            assert "drive_songs_folder_id" not in colab_ov["config"]
            assert "drive_folders" not in colab_ov["worker"]
            assert set(colab_ov["supported_job_types"]) >= {"rvc_cover", "rvc_train"}
            colab_desc = sys_mods["colab_compute"]["description"]
            assert "GPU" in colab_desc
            assert "翻唱" not in colab_desc
            assert "RVC" not in colab_desc
            assert "Hakimi_Songs" not in colab_desc


async def test_voice_gateway_module_initialize_and_nonblocking_status(settings, store):
    http_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        http_calls.append(f"{request.method} {request.url.path}")
        return httpx.Response(503, text="offline")

    s = settings.model_copy(
        update={
            "voice_reply_enabled": True,
            "voice_gateway_url": "http://hakimi-voice-gateway:12145",
            "voice_gateway_token": SecretStr("vg-module-test-token-1234567890"),
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        res = SystemResources(s, store, FakeGateway(), http, SimpleNamespace())
        mod = VoiceGatewaySystemModule(res, SimpleNamespace(systems={}))
        await mod.initialize()
        assert http_calls == []
        st = await mod.status()
        assert st["state"] == "ready"
        assert "语音回复已开启" in st["detail"]
        assert http_calls == []

        # Persisted override triggers sync during initialize without failing when offline.
        sealed = store.vault.seal("voice-gateway-override:v1", {"tts_provider": "siliconflow"})
        async with store.transaction():
            await store.db.execute(
                "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
                ("voice_gateway_override", sealed),
            )
        mod2 = VoiceGatewaySystemModule(res, SimpleNamespace(systems={}))
        await mod2.initialize()
        assert http_calls == ["POST /v1/admin/config"]
        st2 = await mod2.status()
        assert st2["state"] == "ready"
        assert "已持久化覆写" in st2["detail"]


async def test_gcp_billing_module_worker_migration_and_compat_wrapper(settings, monkeypatch):
    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=True)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        task_names = [t.get_name() for t in svc.tasks]
        assert "gcp_billing" in task_names
        assert "gcp-billing-sync" not in task_names
        assert "colab_compute" in task_names
        assert "song_studio" in task_names

        gcp_mod = svc.modules.systems["gcp_billing"]
        assert isinstance(gcp_mod, GcpBillingSystemModule)
        assert gcp_mod.workers() == (gcp_mod.run_sync,)
        st = await gcp_mod.status()
        assert st["state"] in {"ready", "unconfigured"}

        tick_calls = 0

        async def fake_tick() -> None:
            nonlocal tick_calls
            tick_calls += 1

        async def fake_sleep(_sec: float) -> None:
            raise asyncio.CancelledError

        monkeypatch.setattr(svc.store.runtime.gcp_billing, "tick", fake_tick)
        monkeypatch.setattr("hakimi.service.asyncio.sleep", fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await svc.gcp_billing_worker()
        assert tick_calls == 1

    # Isolated runtime without pre-attached gcp_billing still mounts it cleanly.
    fake_store = SimpleNamespace(s=settings, runtime=SimpleNamespace())
    mod = GcpBillingSystemModule(
        SystemResources(settings, fake_store, FakeGateway(), None, SimpleNamespace()),
        SimpleNamespace(systems={}),
    )
    assert fake_store.runtime.gcp_billing is mod.manager


async def test_read_audio_attachment_extensions_size_and_ssrf(settings, store, message):
    payload = b"ID3\x03\x00\x00\x00\x00\x00\x00FAKE_MP3"

    class FakeTransport:
        def __init__(self, resp: dict) -> None:
            self.resp = resp

        async def call(self, action: str, params: dict) -> dict:
            return self.resp

    reader_ok = FileReader(
        settings,
        store,
        transport=FakeTransport({"base64": base64.b64encode(payload).decode("ascii")}),
    )
    for ext in ("mp3", "wav", "flac", "m4a", "ogg", "opus"):
        raw, fname = await reader_ok.read_audio_attachment(
            FileRef(name=f"../../dir/demo.{ext}", file_id=f"fid-{ext}", file_size=len(payload)),
            message,
        )
        assert raw == payload
        assert fname == f"demo.{ext}"

    for bad_name in ("notes.txt", "evil.exe", "photo.png", ".env", "noext"):
        with pytest.raises(Rejected, match="invalid_song_audio"):
            await reader_ok.read_audio_attachment(
                FileRef(name=bad_name, file_id="fid-bad"), message
            )

    with pytest.raises(Rejected, match="oversized"):
        await reader_ok.read_audio_attachment(
            FileRef(name="big.mp3", file_id="fid-big", file_size=2048),
            message,
            max_bytes=1024,
        )
    with pytest.raises(Rejected, match="oversized"):
        await reader_ok.read_audio_attachment(
            FileRef(name="small_claim.mp3", file_id="fid-small"),
            message,
            max_bytes=4,
        )

    reader_ssrf = FileReader(
        settings,
        store,
        transport=FakeTransport({"file": "/etc/passwd", "url": "http://127.0.0.1/secret.mp3"}),
    )
    with pytest.raises(Rejected, match="unavailable"):
        await reader_ssrf.read_audio_attachment(
            FileRef(name="ssrf.mp3", file_id="fid-ssrf"), message
        )

    async def fail_download(_url: str, _maximum: int) -> bytes:
        raise aiohttp.ClientError("boom")

    reader_err = FileReader(settings, store, transport=None)
    reader_err._download_http = fail_download  # type: ignore[method-assign]
    with pytest.raises(Rejected, match="unavailable"):
        await reader_err.read_audio_attachment(
            FileRef(name="net.mp3", url="https://example.com/net.mp3"),
            message,
        )


async def test_owner_chat_learn_song_command_and_action_e2e(
    settings, store, message, caplog, monkeypatch
):
    # 1. Default unconfigured colab does NOT pollute owner_chat_actions
    svc_default = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    try:
        assert svc_default._collect_owner_chat_actions() == []
    finally:
        await svc_default.stop()

    # 2. Configured colab exposes song_studio.learn_song & supports /学歌 + audio attachments
    s = _colab_settings(settings)
    submitted_jobs: list[tuple[str, dict, str]] = []
    uploaded_inbox: list[tuple[str, bytes]] = []
    close_calls = 0

    gw = FakeGateway()
    svc = Service(s, store, gw, FakeBili(), FakeSender())
    try:
        await svc.world_state.initialize()
        await svc.life_engine.initialize()
        await svc.songs.initialize()

        orig_close = svc.colab_compute.close

        async def tracked_close() -> None:
            nonlocal close_calls
            close_calls += 1
            await orig_close()

        async def fake_upload_inbox(
            filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
        ) -> dict:
            uploaded_inbox.append((filename, raw))
            return {
                "file_id": f"inbox-id-{len(uploaded_inbox)}",
                "name": filename,
                "size": len(raw),
            }

        async def fake_submit_job(
            job_type: str, payload: dict, *, requester: str = "owner", gpu: str | None = None
        ) -> dict:
            submitted_jobs.append((job_type, dict(payload), requester))
            return {
                "id": f"job{len(submitted_jobs):029d}",
                "job_type": job_type,
                "status": "queued",
                "payload": payload,
                "created_at": time.time(),
            }

        monkeypatch.setattr(svc.colab_compute, "close", tracked_close)
        monkeypatch.setattr(svc.songs.drive, "upload_inbox_bytes", fake_upload_inbox)
        monkeypatch.setattr(svc.colab_compute, "submit_job", fake_submit_job)

        collected = svc._collect_owner_chat_actions()
        assert [a["action_name"] for a in collected] == ["song_studio.learn_song"]

        # Action mode off or engine disabled hides it
        orig_cfg = svc.life_engine.config
        svc.life_engine.config = orig_cfg.model_copy(
            update={"action_modes": {**orig_cfg.action_modes, "song_studio.learn_song": "off"}}
        )
        assert svc._collect_owner_chat_actions() == []
        svc.life_engine.config = orig_cfg.model_copy(update={"enabled": False})
        assert svc._collect_owner_chat_actions() == []
        svc.life_engine.config = orig_cfg

        # Non-owner & group calls to /学歌 / /learn-song must be rejected with zero AI/Colab calls
        stranger = message.model_copy(
            update={"user_id": "30000", "conversation_id": "30000", "text": "/学歌 晴天"}
        )
        owner_group = message.model_copy(
            update={"kind": "group", "conversation_id": "40000", "text": "/learn-song 晴天"}
        )
        for denied_msg in (stranger, owner_group):
            with pytest.raises(Rejected, match="owner_private_only"):
                await svc.command(denied_msg)
        assert submitted_jobs == []
        assert gw.calls == []

        # Owner DM /学歌 (auto_search)
        reply = await svc.command(
            message.model_copy(update={"text": "/学歌 《晴天》 pitch=-2 artist=周杰伦 tags=抒情"})
        )
        assert reply is not None and "晴天" in reply and "变调 -2" in reply
        assert len(submitted_jobs) == 1
        assert submitted_jobs[0][0] == "rvc_cover"
        assert submitted_jobs[0][1]["title"] == "晴天"
        assert submitted_jobs[0][1]["pitch"] == -2
        assert submitted_jobs[0][1]["artist"] == "周杰伦"
        assert submitted_jobs[0][1]["tags"] == "抒情"
        assert submitted_jobs[0][1]["auto_search"] is True
        assert gw.calls == []

        # Owner DM /learn-song with audio FileRef attachment
        audio_bytes = b"ID3\x03\x00\x00\x00\x00\x00\x00ATTACHMENT_MP3"

        async def fake_read_audio(ref: FileRef, ctx: Message, *, max_bytes: int = 25 * 1024 * 1024):
            assert ref.name == "demo.mp3"
            return audio_bytes, "demo.mp3"

        monkeypatch.setattr(svc.files, "read_audio_attachment", fake_read_audio)
        reply2 = await svc.command(
            message.model_copy(
                update={
                    "text": "/learn-song 小幸运 pitch=+3",
                    "files": [FileRef(name="demo.mp3", file_id="fid-demo")],
                }
            )
        )
        assert reply2 is not None and "小幸运" in reply2 and "附带音频" in reply2
        assert len(uploaded_inbox) == 1
        assert uploaded_inbox[0] == ("demo.mp3", audio_bytes)
        assert submitted_jobs[-1][1]["title"] == "小幸运"
        assert submitted_jobs[-1][1]["pitch"] == 3
        assert submitted_jobs[-1][1]["inbox_file_id"] == "inbox-id-1"

        # Owner chat action (song_studio.learn_song) with audio attachment & replan
        replan_delays: list[float] = []

        async def fake_replan(*, delay_seconds: float = 35.0) -> bool:
            replan_delays.append(delay_seconds)
            return True

        svc.life_engine.planner.request_replan = fake_replan  # type: ignore[method-assign]
        with caplog.at_level(logging.INFO, logger="hakimi"):
            await svc._run_owner_chat_action(
                ChatActionTrigger(
                    action_name="song_studio.learn_song",
                    arguments_json=json.dumps(
                        {"title": "稻香", "artist": "周杰伦", "pitch": 1, "auto_search": False}
                    ),
                ),
                context_message=message.model_copy(
                    update={"files": [FileRef(name="demo.mp3", file_id="fid-daoxiang")]}
                ),
            )
        assert len(uploaded_inbox) == 2
        assert submitted_jobs[-1][1]["title"] == "稻香"
        assert submitted_jobs[-1][1]["pitch"] == 1
        assert submitted_jobs[-1][2] == "owner_chat"
        assert replan_delays == [35.0]
        assert any(
            "event=owner_chat_action_done action=song_studio.learn_song status=done reason=queued"
            in r.getMessage()
            for r in caplog.records
        )

        # Non-owner context_message passed to _run_owner_chat_action is gated
        before_count = len(submitted_jobs)
        await svc._run_owner_chat_action(
            ChatActionTrigger(
                action_name="song_studio.learn_song",
                arguments_json='{"title":"偷跑"}',
            ),
            context_message=stranger,
        )
        assert len(submitted_jobs) == before_count

        # Help & invalid /学歌 args return polite deterministic errors without hitting Colab
        help_reply = await svc.command(message.model_copy(update={"text": "/学歌"}))
        assert help_reply is not None and "用法" in help_reply
        bad_pitch = await svc.command(message.model_copy(update={"text": "/学歌 晴天 pitch=99"}))
        assert bad_pitch is not None and "-24" in bad_pitch
        assert len(submitted_jobs) == before_count
    finally:
        await svc.stop()

    assert close_calls >= 1


async def test_owner_chat_learn_song_handle_message_passes_context_message_e2e(
    settings, store, monkeypatch
):
    s = _colab_settings(settings)
    seen_payloads: list[dict] = []
    submitted: list[tuple[str, dict, str]] = []
    uploaded: list[tuple[str, bytes]] = []
    audio_payload = b"ID3\x03\x00\x00\x00\x00\x00\x00E2E_MP3"

    class _LearnActionGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            seen_payloads.append(dict(payload))
            await super().generate(payload, subject, **kwargs)
            return json.dumps(
                {
                    "abstain": False,
                    "bubbles": ["收到，这就开始练这首歌喵~"],
                    "anchor": {"reply_id": None, "mentions": []},
                    "sticker": {"label": None, "timing": "after_text"},
                    "image_digest": None,
                    "emoji_like": None,
                    "poke_qq": None,
                    "save_meme_label": None,
                    "set_longnick": None,
                    "affection_delta": 0,
                    "affection_deltas": None,
                    "trigger_action": {
                        "action_name": "song_studio.learn_song",
                        "arguments_json": json.dumps({"title": "七里香", "pitch": -1}),
                    },
                },
                ensure_ascii=False,
            )

    class _FileTransport:
        async def call(self, action: str, params: dict) -> dict:
            if action in {"get_private_file_url", "get_file"}:
                return {"base64": base64.b64encode(audio_payload).decode("ascii")}
            return {"message_id": 888}

    svc = Service(
        s,
        store,
        _LearnActionGw(),
        FakeBili(),
        FakeSender(),
    )
    svc.files = FileReader(s, store, transport=_FileTransport())
    try:
        await svc.world_state.initialize()
        await svc.life_engine.initialize()
        await svc.songs.initialize()

        async def fake_upload(
            filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
        ):
            uploaded.append((filename, raw))
            return {"file_id": "inbox-e2e-1", "name": filename, "size": len(raw)}

        async def fake_submit(
            job_type: str, payload: dict, *, requester: str = "owner", gpu: str | None = None
        ):
            submitted.append((job_type, dict(payload), requester))
            return {
                "id": "e2ejob00000000000000000000000001",
                "job_type": job_type,
                "status": "queued",
                "payload": payload,
                "created_at": time.time(),
            }

        monkeypatch.setattr(svc.songs.drive, "upload_inbox_bytes", fake_upload)
        monkeypatch.setattr(svc.colab_compute, "submit_job", fake_submit)

        now = int(time.time())
        assert (
            await svc.accept_event(
                {
                    "post_type": "message",
                    "message_type": "private",
                    "self_id": 10000,
                    "user_id": 20000,
                    "message_id": 901,
                    "time": now,
                    "sender": {"nickname": "BOISER"},
                    "message": [
                        {
                            "type": "file",
                            "data": {"file_name": "qilixiang.mp3", "id": "fid-e2e", "size": 32},
                        },
                        {"type": "text", "data": {"text": "帮我学唱这首七里香"}},
                    ],
                }
            )
            == "accepted"
        )
        job = await store.claim()
        assert job is not None
        await svc.process(job)
        assert await svc.deliver_once()
        pending = list(svc.background_tasks)
        if pending:
            await asyncio.gather(*pending)

        assert any(
            a["action_name"] == "song_studio.learn_song"
            for a in seen_payloads[-1].get("owner_chat_actions", [])
        )
        assert uploaded == [("qilixiang.mp3", audio_payload)]
        assert len(submitted) == 1
        assert submitted[0][1]["title"] == "七里香"
        assert submitted[0][1]["pitch"] == -1
        assert submitted[0][1]["inbox_file_id"] == "inbox-e2e-1"

        # Non-owner DM must NEVER receive song_studio.learn_song
        seen_payloads.clear()
        uploaded.clear()
        submitted.clear()
        assert (
            await svc.accept_event(
                {
                    "post_type": "message",
                    "message_type": "private",
                    "self_id": 10000,
                    "user_id": 30000,
                    "message_id": 902,
                    "time": now + 1,
                    "sender": {"nickname": "BOISER"},
                    "message": [{"type": "text", "data": {"text": "快帮我学首歌"}}],
                }
            )
            == "accepted"
        )
        job2 = await store.claim()
        assert job2 is not None
        await svc.process(job2)
        assert await svc.deliver_once()
        await asyncio.sleep(0)
        assert "owner_chat_actions" not in seen_payloads[-1]
        assert uploaded == []
        assert submitted == []
    finally:
        await svc.stop()


def _valid_admin_tabs() -> set[str]:
    html = (Path(__file__).resolve().parents[1] / "admin" / "admin_page.html").read_text(
        encoding="utf-8"
    )
    return set(re.findall(r'data-tab="([^"]+)"', html))


async def test_wave2_system_modules_wiring_reserved_ids_and_admin_panels(settings):
    s = settings.model_copy(update={"admin_token": SecretStr(TOKEN)})
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    expected_all = {
        "affection",
        "colab_compute",
        "database_backup",
        "drive_sync",
        "event_ledger",
        "gcp_billing",
        "group_memory_mirror",
        "identity",
        "meme_library",
        "memory_compressor",
        "memory_hub",
        "owner_notebook",
        "patrol_diary",
        "plugin_bus",
        "proactive_delivery",
        "screen_watch",
        "song_studio",
        "surf_mirror",
        "video_watch",
        "voice_companion",
        "voice_gateway",
    }
    wave2_panels = {
        "proactive_delivery": "proactive",
        "video_watch": "bili-account",
        "affection": "community",
        "memory_compressor": "memory",
        "meme_library": "memes",
    }

    async with app.router.lifespan_context(app):
        svc = app.state.service
        names = list(svc.modules.systems.keys())
        assert set(names) == expected_all
        assert len(names) == 21

        for mod_id in wave2_panels:
            assert mod_id in RESERVED_PLUGIN_IDS
            assert svc.modules.get(mod_id) is None
            assert not hasattr(svc.modules.systems[mod_id], "enabled")
            assert mod_id not in svc.modules.catalog.owners.values()
            with pytest.raises(Rejected, match="reserved_plugin_id"):
                await svc.plugin_bus.create_plugin(mod_id, "Spoof", ["actions"])

        assert names.index("event_ledger") < names.index("proactive_delivery")
        assert names.index("identity") < names.index("affection")
        assert names.index("memory_hub") < names.index("memory_compressor")

        assert svc.proactive is svc.modules.systems["proactive_delivery"].proactive
        assert svc.modules.owner_delivery is svc.proactive
        assert svc.video_watch is svc.modules.systems["video_watch"].watch
        assert svc.affection is svc.modules.systems["affection"].manager
        assert svc.store.runtime.affection is svc.affection
        assert svc.affection._identity is svc.identity
        assert svc.compressor is svc.modules.systems["memory_compressor"].compressor
        assert svc.memes is svc.modules.systems["meme_library"].library
        assert svc.store.runtime.memes is svc.memes

        valid_tabs = _valid_admin_tabs()

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as client:
            sys_mods = {m["id"]: m for m in (await client.get("/admin/api/system-modules")).json()}
            assert set(sys_mods) == expected_all
            for mod_id, expected_panel in wave2_panels.items():
                assert sys_mods[mod_id]["panel"] == expected_panel
                assert expected_panel in valid_tabs
                assert "enabled" not in sys_mods[mod_id]
                deny = await client.put(
                    f"/admin/api/life/modules/{mod_id}/enabled", json={"enabled": False}
                )
                assert deny.status_code == 404
            for mod_id, entry in sys_mods.items():
                if entry["panel"] is not None:
                    assert entry["panel"] in valid_tabs, (mod_id, entry["panel"])


async def test_wave2_workers_migration_and_proactive_deferred_init_order(settings):
    s = settings.model_copy(
        update={
            "admin_token": SecretStr(TOKEN),
            "owner_qq_id": "20000",
            "onebot_enabled": True,
            "proactive_dm_enabled": True,
        }
    )

    # Pre-seed SQLite with owner_delivery_enabled=False and a legacy pending proactive outbox row.
    # Deferred proactive_delivery.initialize() runs AFTER life_engine.initialize(), so the persisted
    # disable switch cancels the legacy row instead of migrating it under stale .env defaults.
    pre_store = Store(s)
    await pre_store.open()
    try:
        now = time.time()
        msg = Message(
            platform="onebot",
            bot_id=s.onebot_self_id,
            kind="private",
            conversation_id="20000",
            user_id="20000",
            event_id="legacy-owner-dm",
            text="",
            addressed=True,
            created_at=now,
        )
        await pre_store.runtime.save_config(
            "life_engine",
            {"module_settings": {"qq_social": {"owner_delivery_enabled": False}}},
        )
        async with pre_store.transaction():
            await pre_store.db.execute(
                "INSERT INTO outbox(id,payload,status,created,expires) VALUES (?,?,?,?,?)",
                (
                    "legacy-proactive-1",
                    pre_store.vault.seal(
                        "legacy-proactive-1",
                        {"proactive": True, "message": msg.model_dump(), "text": "旧主动消息"},
                    ),
                    "pending",
                    now,
                    now + 3600,
                ),
            )
    finally:
        await pre_store.close()

    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=True)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert svc.proactive.delivery_enabled is False
        async with svc.store.lock:
            outbox_row = await svc.store.one(
                "SELECT status FROM outbox WHERE id=?", ("legacy-proactive-1",)
            )
            dm_row = await svc.store.one(
                "SELECT status FROM pending_owner_dms WHERE id=?", ("legacy-proactive-1",)
            )
        assert outbox_row is not None and outbox_row[0] == "cancelled"
        assert dm_row is None

        task_names = [t.get_name() for t in svc.tasks]
        assert task_names.count("proactive_delivery") == 2
        assert task_names.count("memory_compressor") == 1
        assert "owner-operational-alerts" not in task_names
        assert "owner-dm-delivery" not in task_names
        assert "memory-archive" not in task_names
        assert svc.recovery_task is not None
        assert svc.recovery_task.get_name() == "compression-recovery"
        assert not any(t.done() for t in svc.tasks)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
        ) as client:
            assert (await client.get("/healthz")).status_code == 200
