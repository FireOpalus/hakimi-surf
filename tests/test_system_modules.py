import asyncio
import json
from datetime import datetime
from zoneinfo import ZoneInfo

from hakimi.diary_sync import DiaryDriveSync
from hakimi.roaming_logger import PatrolDiary
from hakimi.security import Rejected
from tests.fakes import FakeGateway
from tests.test_admin import admin_api as admin_api


async def test_resident_services_are_read_only_and_independent_of_platform_switches(
    admin_api, message
):
    app, client, gateway = admin_api
    svc = app.state.service
    url = "/admin/api/system-modules"
    assert (await client.get(url, headers={"Authorization": ""})).status_code == 401
    modules = {item["id"]: item for item in (await client.get(url)).json()}
    assert {"patrol_diary", "drive_sync"} <= modules.keys()
    assert modules["drive_sync"]["status"]["state"] == "unconfigured"
    for name in modules:
        assert "enabled" not in modules[name]
        assert svc.modules.get(name) is None
        deny = await client.put(f"/admin/api/life/modules/{name}/enabled", json={"enabled": False})
        assert deny.status_code == 404
        assert name not in svc.modules.catalog.owners.values()
    models = (await client.get("/admin/api/models")).json()
    owners = [
        g["id"] for g in models["groups"] for scene in g["scenes"] if scene["key"] == "daily_diary"
    ]
    assert owners == ["patrol_diary"]
    assert (
        await client.put("/admin/api/life/modules/bilibili_surf/enabled", json={"enabled": False})
    ).is_success
    assert not gateway.calls
    day = svc.diary.local().date().isoformat()
    await svc.diary.patrol("local-only", "公开巡逻见闻")
    await svc.diary.group("private", message.model_copy(update={"text": "仅本人私聊的原文"}))
    group = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "text": "群聊原文"}
    )
    await svc.diary.group("group", group)
    result = await client.post("/admin/api/actions/diary")
    assert result.is_success and result.json()["status"] == "unconfigured"
    text = (await client.get("/admin/api/diaries/" + day)).json()["markdown"]
    assert "每日总结" in text and "公开巡逻见闻" in text and "日常话题：1 次" in text
    assert "群聊原文" not in text and "仅本人私聊的原文" not in text
    assert [c[0] for c in gateway.calls] == ["screen", "generate"]
    assert gateway.calls[0][1]["chat_excerpts"] == []
    assert (await client.get("/admin/api/diaries")).json()[0]["day"] == day


async def test_local_finalization_independent_of_drive_and_persistent_after_restart(
    settings, store
):
    now = datetime(2026, 11, 16, 23, 55, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    day = "2026-11-16"

    class Drive:
        def __init__(self):
            self.entries = []
            self.denied = True

        async def sync_day(self, day, text):
            self.entries.append((day, text))
            if self.denied:
                raise Rejected("drive_oauth_reauthorization_required")
            return "folder-scoped-file"

    ai, drive = FakeGateway(), Drive()
    diary = PatrolDiary(settings, store, gateway=ai)
    uploader = DiaryDriveSync(diary, drive, store)
    await diary.patrol("p", "公开内容", now=now)
    # The consumer can wake first. It never generates or seals a premature final copy.
    assert await uploader.sync_due(now=now) == "pending"
    assert not ai.calls and not drive.entries
    await asyncio.gather(diary.prepare_due(now=now, force=True), diary.prepare_due(now=now))
    assert await uploader.sync_due(now=now) == "pending"
    assert (await uploader.status())["last_error"] == "drive_oauth_reauthorization_required"
    local = await diary.render(day)
    assert "每日总结" in local and drive.entries == [(day, local)]
    assert len(ai.calls) == 2

    restarted = PatrolDiary(settings, store, gateway=ai)
    await restarted.initialize()
    uploader = DiaryDriveSync(restarted, drive, store)
    drive.denied = False
    await restarted.prepare_due(now=now + 1)
    await uploader.sync_due(now=now + 1)
    assert len(drive.entries) == 1
    assert await uploader.sync_due(now=now + 61) == "uploaded"
    assert await uploader.sync_due(now=now + 62, force=True) == "unchanged"
    assert len(ai.calls) == 2 and drive.entries == [(day, local), (day, local)]


async def test_old_chat_summaries_remain_private_without_archive_authorization(
    settings, store, message
):
    settings.roaming_enabled = True
    diary = PatrolDiary(settings, store, gateway=FakeGateway())
    day = diary.local().date().isoformat()
    await diary.group("dm", message.model_copy(update={"text": "历史私聊材料"}))
    await diary.prepare_due(force=True)
    assert "历史私聊材料" in await diary.render(day)
    assert not await store.one("SELECT day FROM roaming_public_summaries WHERE day=?", (day,))
    settings.roaming_enabled = False
    limited = await diary.render(day)
    assert "历史私聊材料" not in limited and "每日总结" not in limited
    assert "历史私聊材料" not in json.dumps(await diary.public_digest(day), ensure_ascii=False)


async def test_builtin_discovery_collects_only_declared_capabilities(
    tmp_path, monkeypatch, settings
):
    import hakimi.system_modules as systems
    from hakimi.app import create_app

    extra = tmp_path / "installed"
    extra.mkdir()
    (extra / "rain_archive.py").write_text(
        "from hakimi.system_modules import SystemModule\n"
        "from hakimi.thinking_policy import ThinkingScene\n"
        "class Module(SystemModule):\n"
        "    module_id = 'rain_archive'\n"
        "    display_name = '降雨档案'\n"
        "    description = '本地测试服务'\n"
        "    requires = ('patrol_diary',)\n"
        "    thinking_scenes = (ThinkingScene('rain.summary', '观测小结'),)\n"
    )
    monkeypatch.setattr(systems, "__path__", [*systems.__path__, str(extra)])
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert svc.modules.systems["rain_archive"].module_id == "rain_archive"
        assert "rain_archive" not in svc.modules.catalog.owners.values()
        assert svc.modules.thinking_scenes["rain.summary"].label == "观测小结"
        assert not svc.ai.calls


async def test_unusable_drive_configuration_does_not_stop_the_engine(settings, tmp_path):
    from hakimi.app import create_app

    settings.gdrive_auth_mode = "oauth"
    settings.gdrive_oauth_credentials_file = tmp_path / "credential.json"
    settings.gdrive_roaming_folder_id = "root"
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        svc = app.state.service
        assert (await svc.modules.systems["drive_sync"].status())["state"] == "unconfigured"
        assert svc.drive is None and svc.modules.get("qq_social") is not None
        await svc.diary.patrol("offline", "公开观测")
        assert "公开观测" in await svc.diary.render(svc.diary.local().date().isoformat())
        assert not svc.ai.calls


async def test_existing_combined_index_migrates_without_replaying_a_completed_upload(
    settings, store
):
    import hashlib

    day = "2026-11-18"
    now = datetime(2026, 11, 18, 22, 0, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
    await store.db.execute(
        "CREATE TABLE roaming_sync(day TEXT PRIMARY KEY, digest TEXT, file_id TEXT, slot TEXT,"
        "next_attempt REAL DEFAULT 0, failures INTEGER DEFAULT 0)"
    )
    await store.db.execute("INSERT INTO roaming_sync(day,file_id) VALUES (?,?)", (day, "bound-id"))
    diary = PatrolDiary(settings, store)
    await diary.initialize()
    assert (await store.one("SELECT day FROM roaming_days"))[0] == day
    digest = hashlib.sha256((await diary.render(day)).encode()).hexdigest()
    await store.db.execute("UPDATE roaming_sync SET digest=? WHERE day=?", (digest, day))
    uploader = DiaryDriveSync(diary, object(), store)
    assert await uploader.sync_due(now=now, force=True) == "unchanged"
    assert (await store.one("SELECT file_id FROM roaming_sync"))[0] == "bound-id"


async def test_world_pause_keeps_public_diary_observer_and_rejects_private_events(admin_api):
    import time

    app, _, ai = admin_api
    svc = app.state.service
    await svc.life_engine.set_config({"enabled": False})
    now = time.time()
    for kind, payload in [
        ("public_discovery", {"note": "公开里程碑"}),
        ("private_discovery", {"private_first": True, "note": "私人里程碑"}),
    ]:
        await svc.world_state.publish_event("minecraft", kind, payload, importance="high", now=now)
    await svc.life_engine.rules.catch_up(now + 1)
    text = await svc.diary.render(svc.diary.local(now).date().isoformat())
    assert "公开里程碑" in text and "私人里程碑" not in text
    assert not await svc.store.one(
        "SELECT 1 FROM scheduled_intents WHERE source_event_id IS NOT NULL"
    )
    assert not ai.calls
