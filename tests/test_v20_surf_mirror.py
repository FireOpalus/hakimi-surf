"""Tests for Phase 0 SurfMirror and agy-free Owner Memory Bridge."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import stat
import subprocess
import tarfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest

from hakimi.drive_client import TOKEN_URI, UPLOAD, FolderDrive
from hakimi.drive_oauth import FOLDER_MARKER
from hakimi.dual_memory import Extraction
from hakimi.roaming_logger import RoamingLogger
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.surf_mirror import MAX_MIRROR_BYTES, SurfMirror
from scripts.owner_memory_bridge import (
    BRIDGE_APP_KEY,
    BRIDGE_APP_VALUE,
    BRIDGE_FILENAME,
    extract_safe_tar_stream,
    is_valid_md_filename,
    resolve_config,
    run_init,
    run_pull,
    validate_bridge_payload,
)
from tests.fakes import FakeBili, FakeGateway, FakeSender

FOLDER = "1Jng1a_iB4i6hU6FRnNhVyKlPboe0hhco"
MARKER = "b" * 32


def _read_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def _read_bytes(path: str | Path) -> bytes:
    return Path(path).read_bytes()


def _write_oauth_creds(path: Path, folder: str = FOLDER) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "type": "authorized_user",
        "client_id": "123-test.apps.googleusercontent.com",
        "client_secret": "test-oauth-client-secret",
        "token_uri": TOKEN_URI,
        "scopes": ["https://www.googleapis.com/auth/drive.file"],
        "refresh_token": "test-refresh-token",
        "folder_id": folder,
        "folder_marker": MARKER,
    }
    path.write_text(json.dumps(data), encoding="utf-8")
    os.chmod(path, 0o400)
    return path


def _oauth_settings(settings, tmp_path: Path, *, roaming_enabled: bool = True):
    creds = _write_oauth_creds(tmp_path / "roaming-oauth.json")
    return settings.model_copy(
        update={
            "roaming_enabled": roaming_enabled,
            "gdrive_auth_mode": "oauth",
            "gdrive_oauth_credentials_file": creds,
            "gdrive_roaming_folder_id": FOLDER,
        }
    )


def _folder_meta() -> dict:
    return {
        "id": FOLDER,
        "mimeType": "application/vnd.google-apps.folder",
        "appProperties": {FOLDER_MARKER: MARKER},
        "capabilities": {"canAddChildren": True},
        "trashed": False,
    }


def _make_tar_bytes(entries: list[tuple[str, bytes | None, str]]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data, kind in entries:
            info = tarfile.TarInfo(name=name)
            if kind == "dir":
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                tf.addfile(info)
            elif kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = "/etc/passwd"
                tf.addfile(info)
            else:
                assert data is not None
                info.type = tarfile.REGTYPE
                info.size = len(data)
                info.mode = 0o644
                tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _seed_bridge_repo(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    repo = tmp_path / "repo"
    mem_dir = repo / "secrets" / "antigravity-memory"
    (mem_dir / "long_term").mkdir(parents=True)
    (mem_dir / "notes").mkdir(parents=True)
    (mem_dir / "notebook.md").write_text("# 初始小本本\n", encoding="utf-8")
    (mem_dir / "short_term_memory.json").write_text('[{"id": "m1"}]\n', encoding="utf-8")
    (mem_dir / "long_term" / "2026-11.md").write_text("# 十一月\n", encoding="utf-8")
    (mem_dir / "notes" / "卫戍协议_notes.md").write_text("# 笔记\n", encoding="utf-8")
    creds = _write_oauth_creds(repo / "secrets" / "roaming-oauth.json")
    env_file = repo / ".env"
    env_file.write_text(f"GDRIVE_ROAMING_FOLDER_ID={FOLDER}\n", encoding="utf-8")
    return repo, mem_dir, creds, env_file


def _verify_pulled_permissions(mem_dir: Path) -> None:
    assert _read_text(mem_dir / "notebook.md") == "# 更新后的小本本\n"
    assert stat.S_IMODE((mem_dir / "notes").stat().st_mode) == 0o750
    assert stat.S_IMODE((mem_dir / "notebook.md").stat().st_mode) == 0o640
    if os.geteuid() == 0:
        assert (mem_dir / "notebook.md").stat().st_gid == 10001
        assert (mem_dir / "notes").stat().st_gid == 10001


async def test_surf_mirror_empty_and_degrades_gracefully(settings, store, tmp_path):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    mirror = SurfMirror(svc)
    now = datetime(2026, 11, 17, 14, 0, tzinfo=ZoneInfo(settings.bot_timezone)).timestamp()

    res = await mirror.export_once(now=now, upload_drive=False)

    latest = Path(res["latest_path"])
    daily = Path(res["daily_path"])
    assert latest.parent == Path(settings.data_dir) / "surf_mirror"
    assert daily.parent == Path(settings.data_dir) / "surf_mirror" / "daily"
    assert daily.name == "2026-11-17.md"
    text = await asyncio.to_thread(_read_text, latest)
    assert "# 🐾 哈基米分身实时镜像 (hakimi-surf Mirror)" in text
    assert "## 世界状态看板" in text
    assert "## 近期与 BOISER 的私聊记忆与对话摘要" in text
    assert "## 今日冲浪与生活轨迹" in text
    # Must remain read-only: never create an empty owner session SQLite file.
    created_dbs = await asyncio.to_thread(
        lambda: list((Path(settings.data_dir) / "sessions").glob("*/*.sqlite3"))
    )
    assert not created_dbs


async def test_surf_mirror_decrypts_owner_session_world_roaming_and_isolates_groups(
    settings, store, message, tmp_path
):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    now = datetime(2026, 11, 17, 18, 30, tzinfo=ZoneInfo(settings.bot_timezone)).timestamp()
    day = "2026-11-17"

    # 1. Populate 14 turns in Owner private session + rolling summary + dual_memory
    for idx in range(14):
        await svc.sessions.remember(message, f"owner-q-{idx:02d}", f"owner-a-{idx:02d}")

    key = svc.sessions.key(message)
    async with svc.sessions.database(message, preserve_history=True) as (db, _):
        await db.execute(
            "UPDATE rolling_state SET summary=?, expires=?",
            (svc.sessions.vault.seal(key, "BOISER 正在推进 v2.0 记忆镜像开发。"), now + 86400),
        )
        await db.execute(
            "INSERT INTO cold_history_slices VALUES (?,?,?,?,?,?,?)",
            ("slice-1", svc.sessions.vault.seal(key + ":cold:slice-1", []), 0, 1, 2, "fold", now),
        )
        await db.execute(
            "INSERT INTO memory_slice_jobs(id,status) VALUES ('slice-1','attempted')"
        )
        await db.execute(
            "UPDATE recent_messages SET expires=? WHERE id=1",
            (now + 86400,),
        )
        await db.execute(
            "INSERT INTO member_profiles VALUES (?,?,?,?,?,?,?,?)",
            (
                settings.owner_qq_id,
                "preferences",
                svc.sessions.vault.seal(
                    key + f":p:{settings.owner_qq_id}:preferences",
                    {"text": "喜欢乌龙茶与简洁结论", "source_seq": 1},
                ),
                0,
                0,
                0,
                now,
                "slice-1",
            ),
        )
        await db.execute(
            "INSERT INTO member_profiles VALUES (?,?,?,?,?,?,?,?)",
            (
                settings.owner_qq_id,
                "state",
                svc.sessions.vault.seal(
                    key + f":p:{settings.owner_qq_id}:state",
                    {"text": "EXPIRED_STATE_HIDDEN", "source_seq": 1},
                ),
                now - 10,
                0,
                0,
                now,
                "slice-1",
            ),
        )
        await db.execute(
            "INSERT INTO chronicle_events VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "chr-1",
                svc.sessions.vault.seal(
                    key + ":e:chr-1",
                    {"topic": "v2.0 架构", "category": "consensus", "text": "采用无LLM镜像"},
                ),
                svc.sessions.vault.seal(key + ":v:chr-1", "AAAA"),
                3,
                now,
                now,
                0,
                0,
                "slice-1",
            ),
        )
        await db.execute(
            "INSERT INTO chronicle_events VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "chr-del",
                svc.sessions.vault.seal(
                    key + ":e:chr-del",
                    {"topic": "deleted", "category": "event", "text": "DELETED_CHRONICLE_HIDDEN"},
                ),
                svc.sessions.vault.seal(key + ":v:chr-del", "AAAA"),
                1,
                now,
                now,
                0,
                1,
                "slice-1",
            ),
        )

    Extraction.model_validate({"profiles": [], "chronicles": []})

    # 2. Seed group & other private session (must NEVER leak into mirror)
    group_msg = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "event_id": "grp-1"}
    )
    other_private = message.model_copy(
        update={"user_id": "30000", "conversation_id": "30000", "event_id": "oth-1"}
    )
    await svc.sessions.remember(group_msg, "GROUP_SECRET_LEAK", "GROUP_REPLY_LEAK")
    await svc.sessions.remember(other_private, "OTHER_PRIVATE_LEAK", "OTHER_REPLY_LEAK")

    # 3. Seed world_state & global_events plaintext JSON tables
    async with store.transaction():
        await store.db.execute(
            """
            CREATE TABLE IF NOT EXISTS world_state (
              singleton INTEGER PRIMARY KEY CHECK(singleton=1),
              current_module TEXT NOT NULL DEFAULT 'idle',
              focus_level TEXT NOT NULL DEFAULT 'LOW',
              mood TEXT NOT NULL DEFAULT 'calm',
              energy INTEGER NOT NULL DEFAULT 80,
              boredom INTEGER NOT NULL DEFAULT 20,
              last_proactive_at REAL NOT NULL DEFAULT 0,
              updated_at REAL NOT NULL DEFAULT 0,
              snapshot_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        await store.db.execute(
            """
            CREATE TABLE IF NOT EXISTS global_events (
              id TEXT PRIMARY KEY,
              created_at REAL NOT NULL,
              source_module TEXT NOT NULL,
              event_type TEXT NOT NULL,
              importance TEXT NOT NULL DEFAULT 'normal',
              consumed INTEGER NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        await store.db.execute(
            "INSERT INTO world_state VALUES (1,'bilibili_surf','MEDIUM','curious',88,15,?,?,'{}')",
            (now, now),
        )
        await store.db.execute(
            "UPDATE world_state SET snapshot_json=? WHERE singleton=1",
            (json.dumps({"current_topic": "观测科技动态"}, ensure_ascii=False),),
        )
        await store.db.execute(
            "INSERT INTO global_events VALUES (?,?,?,?,?,?,?)",
            (
                "ev-1",
                now,
                "bilibili_surf",
                "patrol_finished",
                "normal",
                0,
                json.dumps({"watched": 2, "highlight": "AI工程实践"}, ensure_ascii=False),
            ),
        )

    # 4. Seed today's roaming_events / roaming_counts and recent_surfed
    settings_roam = settings.model_copy(update={"roaming_enabled": True})
    svc.s = settings_roam
    diary = RoamingLogger(settings_roam, store)
    await diary.initialize()
    await diary.patrol("patrol-1", "看了科技区新视频《异步调度深入解析》", now=now)

    async def fake_recent_surfed(limit: int = 6, now: float | None = None):
        return [
            {
                "bvid": "BV1xx411c7mD",
                "watched_at": "11-17 17:45",
                "notes": "B站冲浪笔记：讲解了 SQLite WAL 并发读",
                "actions": ["点赞"],
            }
        ][:limit]

    svc.bili_watcher = SimpleNamespace(recent_surfed=fake_recent_surfed)

    mirror = SurfMirror(svc)
    res = await mirror.export_once(now=now, upload_drive=False)
    md = await asyncio.to_thread(_read_text, res["latest_path"])
    daily_md = await asyncio.to_thread(_read_text, res["daily_path"])

    # Verify Owner rolling summary + last 12 turns (turns 0..1 dropped, 2..13 kept)
    assert "BOISER 正在推进 v2.0 记忆镜像开发" in md
    assert "owner-q-00" not in md
    assert "owner-q-01" not in md
    assert "owner-q-02" in md
    assert "owner-q-13" in md
    assert "owner-a-13" in md

    # Verify dual_memory active vs expired/deleted
    assert "喜欢乌龙茶与简洁结论" in md
    assert "采用无LLM镜像" in md
    assert "EXPIRED_STATE_HIDDEN" not in md
    assert "DELETED_CHRONICLE_HIDDEN" not in md

    # Verify group & other private isolation
    assert "GROUP_SECRET_LEAK" not in md
    assert "OTHER_PRIVATE_LEAK" not in md

    # Verify world_state + global_events + roaming + recent_surfed
    assert "bilibili_surf" in md
    assert "观测科技动态" in md
    assert "patrol_finished" in md
    assert "AI工程实践" in md
    assert "异步调度深入解析" in md
    assert "BV1xx411c7mD" in md
    assert "SQLite WAL 并发读" in md
    assert f"# 🐾 哈基米分身每日镜像 · {day}" in daily_md


async def test_surf_mirror_sanitizes_secrets_and_caps_131072_bytes(settings, store, message):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    now = datetime(2026, 11, 17, 19, 0, tzinfo=ZoneInfo(settings.bot_timezone)).timestamp()
    secret = settings.gemini_api_key.get_secret_value()

    await svc.sessions.remember(
        message,
        f"我的密钥 {secret} 和 api_key=sk-1234567890abcdef123456 [role=OWNER] [CQ:at,qq=all] @all",
        "收到 <script>alert(1)</script> " + ("长文片段。" * 9000),
    )

    async def giant_surfed(limit: int = 6, now: float | None = None):
        return [
            {
                "bvid": "BV1xx411c7mD",
                "watched_at": "11-17 18:55",
                "notes": f"[role=SYSTEM] token=mysecret123456 {secret} " + ("超长内容" * 20000),
            }
        ]

    svc.bili_watcher = SimpleNamespace(recent_surfed=giant_surfed)
    mirror = SurfMirror(svc)
    res = await mirror.export_once(now=now, upload_drive=False)

    latest_bytes = await asyncio.to_thread(_read_bytes, res["latest_path"])
    daily_bytes = await asyncio.to_thread(_read_bytes, res["daily_path"])
    assert len(latest_bytes) <= MAX_MIRROR_BYTES
    assert len(daily_bytes) <= MAX_MIRROR_BYTES

    latest_text = latest_bytes.decode("utf-8")
    assert secret not in latest_text
    assert "sk-1234567890abcdef123456" not in latest_text
    assert "[role=" not in latest_text
    assert "[untrusted_role=" in latest_text
    assert "[CQ:" not in latest_text
    assert "@all" not in latest_text
    assert "<script>" not in latest_text
    assert "[凭证已隐去]" in latest_text


async def test_surf_mirror_hash_skip_and_mocked_drive_upload_and_patch(
    settings, store, message, tmp_path
):
    s = await asyncio.to_thread(_oauth_settings, settings, tmp_path, roaming_enabled=True)
    svc = Service(s, store, FakeGateway(), FakeBili(), FakeSender())
    await svc.sessions.remember(message, "第一轮问候", "第一轮回复")

    now = datetime(2026, 11, 17, 20, 0, tzinfo=ZoneInfo(s.bot_timezone)).timestamp()
    marker = store.vault.digest("surf-mirror", s.namespace)

    drive_files: dict[str, dict] = {}
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, str(request.url)))
        if request.url.path == "/oauth2/v4/token" or str(request.url) == TOKEN_URI:
            return httpx.Response(
                200,
                json={
                    "access_token": "tok-1",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "scope": "https://www.googleapis.com/auth/drive.file",
                },
            )
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_meta())
        if request.url.path == "/drive/v3/files" and request.method == "GET":
            q = request.url.params["q"]
            assert f"'{FOLDER}' in parents" in q
            assert f"key='hakimi_surf_mirror' and value='{marker}'" in q
            matched = [v for v in drive_files.values() if f"name = '{v['name']}'" in q]
            return httpx.Response(200, json={"files": matched})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content.decode("utf-8")
            name = (
                "surf-mirror-latest.md"
                if "surf-mirror-latest.md" in raw
                else "surf-mirror-2026-11-17.md"
            )
            fid = f"id-{len(drive_files) + 1}"
            item = {
                "id": fid,
                "name": name,
                "parents": [FOLDER],
                "mimeType": "text/markdown",
                "trashed": False,
                "appProperties": {"hakimi_surf_mirror": marker},
            }
            drive_files[fid] = item
            return httpx.Response(200, json=item)
        if request.method == "PATCH" and str(request.url).startswith(UPLOAD + "/"):
            fid = request.url.path.rsplit("/", 1)[-1]
            assert fid in drive_files
            assert request.headers["Content-Type"] == "text/markdown; charset=UTF-8"
            return httpx.Response(200, json=drive_files[fid])
        raise AssertionError(f"Unexpected Drive request: {request.method} {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        drive = FolderDrive(s, http, store.vault.digest("diary", s.namespace))
        mirror = SurfMirror(svc, drive=drive)

        # 1. First export uploads both latest and daily
        res1 = await mirror.export_once(now=now, upload_drive=True)
        assert sorted(res1["uploaded"]) == ["surf-mirror-2026-11-17.md", "surf-mirror-latest.md"]
        assert res1["skipped_drive"] is False
        assert len(drive_files) == 2

        # 2. Unchanged export skips Drive completely
        count_before = len(requests)
        res2 = await mirror.export_once(now=now, upload_drive=True)
        assert res2["uploaded"] == []
        assert res2["skipped_drive"] is True
        assert len(requests) == count_before

        # 3. New turn triggers PATCH on both existing Drive file IDs
        await svc.sessions.remember(message, "第二轮问候", "第二轮回复")
        res3 = await mirror.export_once(now=now, upload_drive=True)
        assert sorted(res3["uploaded"]) == ["surf-mirror-2026-11-17.md", "surf-mirror-latest.md"]
        assert any(m == "PATCH" for m, _ in requests[count_before:])


async def test_surf_mirror_run_survives_exceptions_and_reraises_cancel(
    settings, store, monkeypatch
):
    svc = Service(settings, store, FakeGateway(), FakeBili(), FakeSender())
    mirror = SurfMirror(svc, interval_seconds=0.01)

    calls = 0

    async def fake_export_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient_error")
        if calls == 2:
            return {"ok": True}
        raise asyncio.CancelledError

    monkeypatch.setattr(mirror, "export_once", fake_export_once)
    with pytest.raises(asyncio.CancelledError):
        await mirror.run()
    assert calls == 3


def test_bridge_schema_validation_and_filename_traversal():
    valid = {
        "version": 1,
        "notebook_md": "# 笔记本",
        "short_term_json": json.dumps([{"id": "1"}]),
        "long_term_md": {"2026-11.md": "内容"},
        "notes_md": {"卫戍协议_notes.md": "笔记", "README.md": "说明"},
    }
    parsed = validate_bridge_payload(valid)
    assert parsed["notebook_md"] == "# 笔记本"
    assert "卫戍协议_notes.md" in parsed["notes_md"]

    assert is_valid_md_filename("2026-11.md") is True
    assert is_valid_md_filename("卫戍协议_notes.md") is True
    for bad in ("../escape.md", "sub/a.md", ".hidden.md", "..md", "a\\b.md", "file.txt", ""):
        assert is_valid_md_filename(bad) is False

    for bad_payload in (
        [],
        {"notebook_md": "#"},
        {**valid, "notebook_md": 123},
        {**valid, "short_term_json": "{not-json"},
        {**valid, "short_term_json": '{"not": "a list"}'},
        {**valid, "long_term_md": ["bad"]},
        {**valid, "notes_md": {"../evil.md": "x"}},
        {**valid, "notes_md": {"ok.md": 999}},
    ):
        with pytest.raises(Rejected):
            validate_bridge_payload(bad_payload)


async def test_owner_memory_bridge_init_and_pull_atomic(tmp_path: Path):
    repo, mem_dir, creds, env_file = await asyncio.to_thread(_seed_bridge_repo, tmp_path)

    drive_state: dict = {"file": None, "media": b"", "downloads": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(
                200,
                json={
                    "access_token": "bridge-tok",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "scope": "https://www.googleapis.com/auth/drive.file",
                },
            )
        if request.url.path == f"/drive/v3/files/{FOLDER}":
            return httpx.Response(200, json=_folder_meta())
        if request.url.path == "/drive/v3/files" and request.method == "GET":
            q = request.url.params["q"]
            assert BRIDGE_FILENAME in q
            assert f"key='{BRIDGE_APP_KEY}' and value='{BRIDGE_APP_VALUE}'" in q
            files = [drive_state["file"]] if drive_state["file"] else []
            return httpx.Response(200, json={"files": files})
        if request.method == "POST" and str(request.url).startswith(UPLOAD):
            raw = request.content
            marker_bytes = b"Content-Type: application/json; charset=UTF-8\r\n\r\n"
            json_part = raw.split(marker_bytes)[2].rsplit(b"\r\n--", 1)[0]
            drive_state["media"] = json_part
            md5 = hashlib.md5(json_part, usedforsecurity=False).hexdigest()
            drive_state["file"] = {
                "id": "bridge-file-1",
                "name": BRIDGE_FILENAME,
                "parents": [FOLDER],
                "mimeType": "application/json",
                "trashed": False,
                "appProperties": {BRIDGE_APP_KEY: BRIDGE_APP_VALUE},
                "md5Checksum": md5,
                "modifiedTime": "2026-11-17T12:00:00Z",
                "size": str(len(json_part)),
            }
            return httpx.Response(200, json=drive_state["file"])
        if request.method == "GET" and request.url.path == "/drive/v3/files/bridge-file-1":
            assert request.url.params.get("alt") == "media"
            drive_state["downloads"] += 1
            return httpx.Response(200, content=drive_state["media"])
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    cfg = await asyncio.to_thread(
        resolve_config, repo_root=repo, credentials=creds, env_file=env_file
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(Rejected, match="bridge_not_initialized"):
            await run_pull(cfg, client=http)

        res_init = await run_init(cfg, client=http)
        assert res_init["file_id"] == "bridge-file-1"
        assert res_init["created"] is True

        # Second init reuses existing file_id
        res_init2 = await run_init(cfg, client=http)
        assert res_init2["file_id"] == "bridge-file-1"
        assert res_init2["created"] is False

        # Update remote payload and pull to clean target
        updated_payload = json.dumps(
            {
                "version": 1,
                "updated_at": "2026-11-17T12:05:00Z",
                "notebook_md": "# 更新后的小本本\n",
                "short_term_json": json.dumps([{"id": "m2", "tag": "new"}]),
                "long_term_md": {"2026-11.md": "# 新十一月\n"},
                "notes_md": {"卫戍协议_notes.md": "# 新笔记\n"},
            },
            ensure_ascii=False,
        ).encode("utf-8")
        drive_state["media"] = updated_payload
        drive_state["file"]["md5Checksum"] = hashlib.md5(
            updated_payload, usedforsecurity=False
        ).hexdigest()
        drive_state["file"]["modifiedTime"] = "2026-11-17T12:05:00Z"
        drive_state["file"]["size"] = str(len(updated_payload))

        pull1 = await run_pull(cfg, client=http)
        assert pull1["status"] == "pulled"
        assert drive_state["downloads"] == 1
        await asyncio.to_thread(_verify_pulled_permissions, mem_dir)

        # Second pull with unchanged metadata skips download
        pull2 = await run_pull(cfg, client=http)
        assert pull2["status"] == "unchanged"
        assert drive_state["downloads"] == 1


def test_extract_safe_tar_stream_and_sync_owner_memory_sh(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    script_sh = repo_root / "scripts" / "sync_owner_memory.sh"
    dest_dir = tmp_path / "dest_mem"

    good_tar = _make_tar_bytes(
        [
            ("notebook.md", b"# Tar Notebook\n", "file"),
            ("short_term_memory.json", b'[{"id": "t1"}]\n', "file"),
            ("long_term", None, "dir"),
            ("long_term/2026-11.md", b"# Month\n", "file"),
            ("notes", None, "dir"),
            ("notes/卫戍协议_notes.md", b"# Note\n", "file"),
        ]
    )
    proc = subprocess.run(
        ["bash", str(script_sh), "--stdin-tar"],
        input=good_tar,
        env={**os.environ, "DEST_DIR": str(dest_dir)},
        capture_output=True,
        check=True,
    )
    assert "synced read-only owner memory" in proc.stdout.decode("utf-8")
    assert (dest_dir / "notebook.md").read_text(encoding="utf-8") == "# Tar Notebook\n"
    assert (dest_dir / "notes" / "卫戍协议_notes.md").read_text(encoding="utf-8") == "# Note\n"
    assert stat.S_IMODE((dest_dir / "notes").stat().st_mode) == 0o750
    assert stat.S_IMODE((dest_dir / "notebook.md").stat().st_mode) == 0o640

    # Unsafe tar streams must fail
    bad_sym = _make_tar_bytes([("notes/link.md", None, "symlink")])
    with pytest.raises(Rejected, match="symlink"):
        extract_safe_tar_stream(bad_sym, tmp_path / "s1")

    bad_trav = _make_tar_bytes([("../escape.md", b"x", "file")])
    with pytest.raises(Rejected, match="traversal"):
        extract_safe_tar_stream(bad_trav, tmp_path / "s2")

    bad_file = _make_tar_bytes([("secrets.env", b"KEY=1", "file")])
    with pytest.raises(Rejected, match="unexpected_file"):
        extract_safe_tar_stream(bad_file, tmp_path / "s3")

    # Legacy SRC_DIR mode still works when explicitly passed
    legacy_src = tmp_path / "legacy_src"
    (legacy_src / "long_term").mkdir(parents=True)
    (legacy_src / "notes").mkdir(parents=True)
    (legacy_src / "notebook.md").write_text("# Legacy\n", encoding="utf-8")
    (legacy_src / "short_term_memory.json").write_text("[]\n", encoding="utf-8")
    legacy_dest = tmp_path / "legacy_dest"
    subprocess.run(
        ["bash", str(script_sh)],
        env={**os.environ, "SRC_DIR": str(legacy_src), "DEST_DIR": str(legacy_dest)},
        capture_output=True,
        check=True,
    )
    assert (legacy_dest / "notebook.md").read_text(encoding="utf-8") == "# Legacy\n"

    # Unknown CLI argument exits cleanly with usage error
    bad_cli = subprocess.run(
        ["bash", str(script_sh), "--unknown-flag"],
        capture_output=True,
    )
    assert bad_cli.returncode == 2
