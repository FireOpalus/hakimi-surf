"""Tests for colab_drive, colab_worker, DSP chorus detection, ColabComputeHub, and colab_admin."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import struct
import sys
import wave
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.colab_compute import ColabComputeHub, ColabConfig
from hakimi.security import Rejected
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN as ADMIN_TOKEN

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, rel_path: str):
    if name in sys.modules:
        return sys.modules[name]
    path = REPO_ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


colab_drive = _load_script("colab_drive", "scripts/colab_drive.py")
colab_remote = _load_script("colab_remote_rvc", "scripts/colab_remote_rvc.py")
colab_rvc = _load_script("colab_rvc", "scripts/colab_rvc.py")
colab_worker = _load_script("colab_worker", "scripts/colab_worker.py")


class FakeDriveBackend:
    """In-memory Google Drive v3 API simulator for scoped Hakimi_Songs/ tests."""

    def __init__(self, *, token_scope: str = colab_drive.DRIVE_FILE_SCOPE) -> None:
        self.token_scope = token_scope
        self.counter = 0
        self.folders: dict[str, dict[str, Any]] = {}
        self.files: dict[str, dict[str, Any]] = {
            "outside_secret_file": {
                "id": "outside_secret_file",
                "name": "private_diary.md",
                "parents": ["unrelated_folder_id"],
                "mimeType": "text/markdown",
                "size": "12",
                "modifiedTime": "2026-11-21T00:00:00Z",
                "trashed": False,
                "data": b"top-secret",
            }
        }
        self.resumable_sessions: dict[str, dict[str, Any]] = {}

    def _next_id(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}_{self.counter:010d}"

    def transport(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        del timeout
        parsed = urlsplit(url)
        if url == colab_drive.TOKEN_URI:
            if "invalid" in (body or b"").decode("utf-8", errors="ignore"):
                return 400, {}, json.dumps({"error": "invalid_scope"}).encode()
            return (
                200,
                {},
                json.dumps(
                    {
                        "access_token": "ya29.fake-access-token",
                        "expires_in": 3600,
                        "token_type": "Bearer",
                        "scope": self.token_scope,
                    }
                ).encode(),
            )

        auth = headers.get("Authorization", "")
        if auth != "Bearer ya29.fake-access-token" and method != "PUT":
            return 401, {}, b'{"error": {"message": "unauthorized"}}'

        qs = parse_qs(parsed.query)
        if parsed.path == "/drive/v3/files" and method == "GET":
            q = (qs.get("q") or [""])[0]
            matched: list[dict[str, Any]] = []
            if "mimeType = 'application/vnd.google-apps.folder'" in q:
                for folder in self.folders.values():
                    if folder["trashed"]:
                        continue
                    name_match = f"name = '{folder['name']}'" in q
                    parents = folder.get("parents", [])
                    in_root = "'root' in parents" in q and not parents
                    parent_ok = in_root or any(f"'{p}' in parents" in q for p in parents)
                    if name_match and parent_ok:
                        matched.append(folder)
            else:
                for item in self.files.values():
                    if item["trashed"]:
                        continue
                    parent_ok = any(f"'{p}' in parents" in q for p in item.get("parents", []))
                    name_ok = ("name =" not in q) or (f"name = '{item['name']}'" in q)
                    if parent_ok and name_ok:
                        matched.append({k: v for k, v in item.items() if k != "data"})
            return 200, {}, json.dumps({"files": matched}).encode()

        if parsed.path == "/drive/v3/files" and method == "POST":
            meta = json.loads((body or b"{}").decode("utf-8"))
            fid = self._next_id("drv_folder")
            rec = {
                "id": fid,
                "name": meta["name"],
                "mimeType": meta["mimeType"],
                "parents": meta.get("parents", []),
                "trashed": False,
                "appProperties": meta.get("appProperties", {}),
                "capabilities": {"canAddChildren": True},
            }
            self.folders[fid] = rec
            return 200, {}, json.dumps(rec).encode()

        if parsed.path.startswith("/drive/v3/files/") and method == "GET":
            fid = parsed.path.rsplit("/", 1)[-1]
            obj = self.folders.get(fid) or self.files.get(fid)
            if obj is None:
                return 404, {}, b'{"error": {"message": "not found"}}'
            if (qs.get("alt") or [""])[0] == "media":
                return 200, {}, obj.get("data", b"")
            return 200, {}, json.dumps({k: v for k, v in obj.items() if k != "data"}).encode()

        if parsed.path == "/upload/drive/v3/files" and method == "POST":
            up_type = (qs.get("uploadType") or [""])[0]
            if up_type == "resumable":
                meta = json.loads((body or b"{}").decode("utf-8"))
                sess_id = self._next_id("upload_sess")
                self.resumable_sessions[sess_id] = {
                    "meta": meta,
                    "buf": bytearray(),
                    "total": int(headers.get("X-Upload-Content-Length", "0")),
                    "mime": headers.get("X-Upload-Content-Type", "application/octet-stream"),
                }
                loc = f"https://www.googleapis.com/upload/resumable/{sess_id}"
                return 200, {"location": loc}, b""
            raw = body or b""
            ct = headers.get("Content-Type", "")
            boundary = ct.split("boundary=")[-1].encode()
            parts = raw.split(b"--" + boundary)
            meta = json.loads(parts[1].split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n").decode("utf-8"))
            payload = parts[2].split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n")
            fid = self._next_id("drv_file")
            rec = {
                "id": fid,
                "name": meta["name"],
                "parents": meta.get("parents", []),
                "mimeType": "application/octet-stream",
                "size": str(len(payload)),
                "modifiedTime": "2026-11-21T12:00:00Z",
                "trashed": False,
                "data": payload,
            }
            self.files[fid] = rec
            return 200, {}, json.dumps({k: v for k, v in rec.items() if k != "data"}).encode()

        if parsed.path.startswith("/upload/drive/v3/files/") and method == "PATCH":
            fid = parsed.path.rsplit("/", 1)[-1]
            rec = self.files[fid]
            payload = body or b""
            rec["data"] = payload
            rec["size"] = str(len(payload))
            return 200, {}, json.dumps({k: v for k, v in rec.items() if k != "data"}).encode()

        if parsed.path.startswith("/upload/resumable/") and method == "PUT":
            sess_id = parsed.path.rsplit("/", 1)[-1]
            sess = self.resumable_sessions[sess_id]
            sess["buf"].extend(body or b"")
            if len(sess["buf"]) < sess["total"]:
                return 308, {}, b""
            fid = self._next_id("drv_big")
            meta = sess["meta"]
            data = bytes(sess["buf"])
            rec = {
                "id": fid,
                "name": meta["name"],
                "parents": meta.get("parents", []),
                "mimeType": sess["mime"],
                "size": str(len(data)),
                "modifiedTime": "2026-11-21T12:00:00Z",
                "trashed": False,
                "data": data,
            }
            self.files[fid] = rec
            return 200, {}, json.dumps({k: v for k, v in rec.items() if k != "data"}).encode()

        return 404, {}, b"{}"


def _write_fake_oauth(path: Path, *, refresh_token: str = "1//fake-refresh") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "123456.apps.googleusercontent.com",
                "client_secret": "fake-client-secret",
                "token_uri": colab_drive.TOKEN_URI,
                "scopes": [colab_drive.DRIVE_FILE_SCOPE],
                "refresh_token": refresh_token,
            }
        ),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def _make_modulated_wav(path: Path, *, duration_sec: int = 140, sample_rate: int = 8000) -> Path:
    """Create a synthetic WAV with quiet verse (0..85s) and loud chorus (85..135s)."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        quiet_sec = struct.pack("<h", 8) * sample_rate
        loud_sec = struct.pack("<h", 14000) * sample_rate
        for sec in range(duration_sec):
            wf.writeframes(loud_sec if 85 <= sec < 135 else quiet_sec)
    path.write_bytes(buf.getvalue())
    return path


def _make_silent_wav(path: Path, *, duration_sec: int = 140, sample_rate: int = 8000) -> Path:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(b"\x00\x00" * (duration_sec * sample_rate))
    path.write_bytes(buf.getvalue())
    return path


def _fake_mp3_bytes(tag: bytes = b"test") -> bytes:
    return b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x00" + (tag * 32)


def _worker_settings(settings, url: str, token: str, **extra):
    updates = {
        "colab_worker_url": url,
        "colab_worker_token": SecretStr(token),
        "colab_drive_songs_folder_id": "drv_folder_0000000001",
    }
    updates.update(extra)
    return settings.model_copy(update=updates)


# ---------------------------------------------------------------------------
# 1. colab_drive tests
# ---------------------------------------------------------------------------


def test_colab_drive_folder_tree_resumable_and_scoping(tmp_path: Path) -> None:
    cred_path = tmp_path / "secrets" / "colab-drive-oauth.json"
    _write_fake_oauth(cred_path)
    backend = FakeDriveBackend()
    client = colab_drive.ColabDriveClient(cred_path, transport=backend.transport)

    tree1 = client.ensure_folder_tree()
    tree2 = client.ensure_folder_tree(force_refresh=True)
    assert tree1 == tree2
    assert set(tree1.as_dict()) == {"root", "models", "songs", "inbox"}
    assert len(backend.folders) == 4

    # Multipart upload to inbox + listing
    in_res = client.upload_bytes("inbox", "yu_ai.m4a", b"M4A_BYTES", mime_type="audio/mp4")
    assert in_res["name"] == "yu_ai.m4a"
    inbox_items = client.list_inbox()
    assert len(inbox_items) == 1 and inbox_items[0]["id"] == in_res["file_id"]
    resolved = client.resolve_inbox_file(filename="yu_ai.m4a")
    assert resolved["id"] == in_res["file_id"]

    # Resumable upload (> 4 MiB) to songs
    big_mp3 = b"ID3" + (b"\xff" * (5 * 1024 * 1024))
    song_res = client.upload_bytes("songs", "big_cover.mp3", big_mp3, mime_type="audio/mpeg")
    assert client.download_bytes(song_res["file_id"], allowed_roles=("songs",)) == big_mp3

    # Out-of-scope Drive file ID MUST be rejected
    with pytest.raises(colab_drive.ColabDriveError, match="outside"):
        client.download_bytes("outside_secret_file", allowed_roles=("songs",))

    # Insufficient scope credentials raise clean error without leaking refresh_token
    bad_cred = tmp_path / "secrets" / "bad.json"
    _write_fake_oauth(bad_cred, refresh_token="1//invalid-secret-never-leak")
    bad_client = colab_drive.ColabDriveClient(bad_cred, transport=backend.transport)
    with pytest.raises(colab_drive.ColabDriveError) as exc_info:
        bad_client.ensure_folder_tree()
    assert exc_info.value.code == "drive_insufficient_scope"
    assert "invalid-secret-never-leak" not in str(exc_info.value)


def test_resolve_drive_credentials_decoupled_and_symlink_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("COLAB_DRIVE_OAUTH_FILE", raising=False)
    monkeypatch.delenv("HAKIMI_COLAB_DRIVE_OAUTH_FILE", raising=False)
    roaming = tmp_path / "secrets" / "roaming-oauth.json"
    _write_fake_oauth(roaming)
    before_bytes = roaming.read_bytes()

    resolved = colab_drive.resolve_drive_credentials_path(repo_root=tmp_path)
    assert resolved == roaming

    dedicated = tmp_path / "secrets" / "colab-drive-oauth.json"
    _write_fake_oauth(dedicated)
    assert colab_drive.resolve_drive_credentials_path(repo_root=tmp_path) == dedicated
    assert roaming.read_bytes() == before_bytes

    link = tmp_path / "secrets" / "symlink.json"
    link.symlink_to(dedicated)
    with pytest.raises(colab_drive.ColabDriveError) as sym_exc:
        colab_drive.ColabDriveClient(link).ensure_folder_tree()
    assert sym_exc.value.code == "drive_credentials_invalid"


# ---------------------------------------------------------------------------
# 2. colab_remote_rvc & colab_rvc tests
# ---------------------------------------------------------------------------


def test_transcribe_for_chorus_logs_cuda_fallback_and_stderr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logs: list[str] = []
    monkeypatch.setattr(colab_remote, "log", logs.append)
    captured_scripts: list[str] = []

    def fake_run(cmd, **kwargs):
        del kwargs
        assert "--" not in cmd[:1]
        captured_scripts.append(cmd[2])
        assert 'language="zh"' not in cmd[2]
        assert "language=" not in cmd[2]
        assert "lang={detected_lang}" in cmd[2]
        assert "CUDA transcribe failed" in cmd[2]
        assert "cpu" in cmd[2]
        return colab_remote.subprocess.CompletedProcess(
            cmd,
            0,
            stdout='__WHISPER_SEGS__=[{"start": 62.0, "end": 66.0, "text": "高潮歌词"}]',
            stderr="[whisper] CUDA transcribe failed; falling back to CPU int8.\n"
            "[whisper] transcribed 1 segment(s) on cpu (lang=ja).\n",
        )

    monkeypatch.setattr(colab_remote, "_run", fake_run)
    segs = colab_remote._transcribe_for_chorus("python", tmp_path / "vocals.wav")
    assert len(segs) == 1 and segs[0]["text"] == "高潮歌词"
    assert any("falling back to CPU" in msg for msg in logs)
    assert any("lang=ja" in msg for msg in logs)

    # Verify the generated inline script auto-detects language (no language kwarg) & logs lang
    import contextlib
    import types

    transcribe_calls: list[tuple[str, dict[str, Any]]] = []

    class _FakeWhisperModel:
        def __init__(self, size: str, *, device: str, compute_type: str) -> None:
            self.device = device

        def transcribe(self, path: str, **kwargs: Any):
            transcribe_calls.append((self.device, dict(kwargs)))
            if self.device == "cuda":
                raise RuntimeError("cuda unavailable")
            seg = types.SimpleNamespace(start=10.0, end=14.0, text="夜に駆ける")
            info = types.SimpleNamespace(language="ja")
            return iter([seg]), info

    fake_fw = types.ModuleType("faster_whisper")
    fake_fw.WhisperModel = _FakeWhisperModel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "faster_whisper", fake_fw)
    monkeypatch.setattr(sys, "argv", ["whisper.py", str(tmp_path / "vocals.wav")])
    stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
        exec(compile(captured_scripts[0], "<whisper_inline>", "exec"), {})
    assert transcribe_calls == [
        ("cuda", {"vad_filter": True}),
        ("cuda", {"vad_filter": False}),
        ("cpu", {"vad_filter": True}),
    ]
    assert "(lang=ja)" in stderr_buf.getvalue()
    assert "__WHISPER_SEGS__=" in stdout_buf.getvalue()

    def fake_fail(cmd, **kwargs):
        del kwargs
        return colab_remote.subprocess.CompletedProcess(
            cmd, 1, stdout="", stderr="libcudnn missing"
        )

    monkeypatch.setattr(colab_remote, "_run", fake_fail)
    logs.clear()
    assert colab_remote._transcribe_for_chorus("python", tmp_path / "vocals.wav") == []
    assert any("libcudnn missing" in msg for msg in logs)


def test_dsp_vocal_rms_chorus_detection_before_60s_fallback(tmp_path: Path) -> None:
    loud_wav = _make_modulated_wav(tmp_path / "vocals_modulated.wav", duration_sec=140)
    start, reason = colab_remote.find_chorus_start_sec(
        segments=[],
        anchor="",
        fallback_start_sec=60.0,
        total_duration_sec=140.0,
        clip_duration_sec=50.0,
        vocal_wav=loud_wav,
    )
    assert reason == "dsp_vocal_rms"
    assert 83.0 <= start <= 87.0

    silent_wav = _make_silent_wav(tmp_path / "vocals_silent.wav", duration_sec=140)
    fb_start, fb_reason = colab_remote.find_chorus_start_sec(
        segments=[],
        anchor="",
        fallback_start_sec=60.0,
        total_duration_sec=140.0,
        clip_duration_sec=50.0,
        vocal_wav=silent_wav,
    )
    assert fb_reason == "fallback"
    assert fb_start == 60.0


def test_clean_source_filter_and_migrate_drive_idempotent(tmp_path: Path) -> None:
    assert colab_rvc.is_clean_source_candidate(
        {"title": "杨丞琳 Rainie Yang - 雨爱 (Official Audio)", "duration": 258}
    )
    assert not colab_rvc.is_clean_source_candidate(
        {"title": "雨爱 (素人翻唱 Cover)", "duration": 240}
    )
    assert not colab_rvc.is_clean_source_candidate(
        {"title": "雨爱 演唱会现场 Live", "duration": 260}
    )
    assert not colab_rvc.is_clean_source_candidate(
        {"title": "雨爱", "duration": 30, "webpage_url": "https://youtube.com/shorts/abc"}
    )

    fake_yt = tmp_path / "yt-dlp"
    fake_yt.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\n"
        "print(json.dumps({'id': 'v1', 'title': '雨爱 Cover 翻唱', 'duration': 200}))\n"
        "print(json.dumps({'id': 'v2', 'title': '雨爱 Live 现场', 'duration': 220}))\n",
        encoding="utf-8",
    )
    os.chmod(fake_yt, 0o755)
    with pytest.raises(colab_rvc.SourceNotFoundError) as exc_info:
        colab_rvc.search_and_download_clean_source("雨爱", tmp_path / "dl", yt_dlp_bin=str(fake_yt))
    assert exc_info.value.code == "source_not_found"

    # migrate-drive idempotency
    cred = tmp_path / "secrets" / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()
    drive_client = colab_drive.ColabDriveClient(cred, transport=backend.transport)

    model_dir = tmp_path / "models" / "hakimi"
    model_dir.mkdir(parents=True)
    (model_dir / "hakimi.pth").write_bytes(b"pth-bytes")
    (model_dir / "hakimi.index").write_bytes(b"idx-bytes")
    cover_dir = tmp_path / "covers" / "雨爱"
    cover_dir.mkdir(parents=True)
    (cover_dir / "cover_chorus.mp3").write_bytes(_fake_mp3_bytes(b"chorus"))
    (cover_dir / "cover_full.mp3").write_bytes(_fake_mp3_bytes(b"full"))

    res1 = colab_rvc.migrate_drive_assets(
        model_dir=tmp_path / "models",
        covers_dir=tmp_path / "covers",
        drive_client=drive_client,
    )
    assert len(res1["uploaded"]) == 4
    res2 = colab_rvc.migrate_drive_assets(
        model_dir=tmp_path / "models",
        covers_dir=tmp_path / "covers",
        drive_client=drive_client,
    )
    assert len(res2["uploaded"]) == 0
    assert len(res2["skipped"]) == 4


# ---------------------------------------------------------------------------
# 3. colab_worker tests
# ---------------------------------------------------------------------------


def _build_test_worker(
    tmp_path: Path,
    *,
    clean_search_fn=None,
    cover_runner=None,
    stop_all_fn=None,
):
    cred = tmp_path / "secrets" / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()
    drive_client = colab_drive.ColabDriveClient(cred, transport=backend.transport)
    drive_client.ensure_folder_tree()

    model_dir = tmp_path / "rvc_models"
    (model_dir / "hakimi").mkdir(parents=True, exist_ok=True)
    (model_dir / "hakimi" / "hakimi.pth").write_bytes(b"pth-data")
    (model_dir / "hakimi" / "hakimi.index").write_bytes(b"idx-data")

    cfg = colab_worker.WorkerConfig(
        token="worker-secret-token-123456",
        model_dir=model_dir,
        dataset_dir=tmp_path / "dataset",
        work_root=tmp_path / "work",
    )

    def default_cover(**kwargs):
        c_dir = Path(kwargs["cover_dir"])
        c_dir.mkdir(parents=True, exist_ok=True)
        chorus = c_dir / "cover_chorus.mp3"
        full = c_dir / "cover_full.mp3"
        chorus.write_bytes(_fake_mp3_bytes(b"chorus-audio"))
        full.write_bytes(_fake_mp3_bytes(b"full-audio"))
        return {
            "assigned_gpu": "L4",
            "chorus_mp3": chorus,
            "full_mp3": full,
            "meta": {
                "chorus_start_sec": 85.0,
                "chorus_duration_sec": 50.0,
                "chorus_start_reason": "dsp_vocal_rms",
            },
        }

    usage_state = {"cu": 100.0, "active": 0}

    def usage_fn():
        return {
            "reachable": True,
            "compute_units": usage_state["cu"],
            "usage_rate": 0.0,
            "active_sessions": usage_state["active"],
            "sessions": ["hakimi-w-1"] if usage_state["active"] else [],
            "error": None,
        }

    def default_stop_all(extra=None):
        del extra
        prev = usage_state["active"]
        usage_state["active"] = 0
        return {"terminated_sessions": prev, "sessions": []}

    app = colab_worker.create_worker_app(
        cfg,
        drive_factory=lambda _root: drive_client,
        usage_fn=usage_fn,
        stop_all_fn=stop_all_fn or default_stop_all,
        cover_runner=cover_runner or default_cover,
        clean_search_fn=clean_search_fn,
    )
    return app, cfg, drive_client, usage_state


async def test_worker_auth_whitelist_cover_and_source_not_found(tmp_path: Path) -> None:
    def failing_search(*args, **kwargs):
        del args, kwargs
        raise colab_rvc.SourceNotFoundError("source_not_found", "no clean original")

    app, cfg, drive_client, usage_state = _build_test_worker(
        tmp_path, clean_search_fn=failing_search
    )
    headers = {"Authorization": f"Bearer {cfg.token}"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://127.0.0.1:12146",
    ) as client:
        assert (await client.get("/v1/status")).status_code == 401
        assert (await client.get("/v1/status?q=1", headers=headers)).status_code == 400
        assert (
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={"job_id": "a" * 32, "job_type": "rm_rf", "payload": {}},
            )
        ).status_code == 400
        assert (
            await client.get("/v1/drive/songs/outside_secret_file", headers=headers)
        ).status_code == 400

        # Upload to inbox and run rvc_cover successfully
        up = await client.post(
            "/v1/drive/inbox",
            headers={**headers, "X-Drive-Filename": "yu_ai.m4a", "Content-Type": "audio/mp4"},
            content=b"RAW_SOURCE_AUDIO",
        )
        assert up.status_code == 200
        inbox_id = up.json()["file_id"]

        job_id = "b" * 32
        sub = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": job_id,
                "job_type": "rvc_cover",
                "gpu_candidates": ["L4", "T4"],
                "payload": {"title": "雨爱", "artist": "杨丞琳", "inbox_file_id": inbox_id},
            },
        )
        assert sub.status_code == 202
        if (task1 := app.state.worker_state.tasks.get(job_id)) is not None:
            await task1
        st = (await client.get(f"/v1/jobs/{job_id}", headers=headers)).json()
        assert st["status"] == "succeeded"
        assert st["assigned_gpu"] == "L4"
        assert st["result"]["title"] == "雨爱"
        assert st["result"]["drive_chorus_id"]
        assert st["result"]["drive_full_id"]
        assert cfg.token not in json.dumps(st)

        # Download generated chorus via Drive proxy
        dl = await client.get(f"/v1/drive/songs/{st['result']['drive_chorus_id']}", headers=headers)
        assert dl.status_code == 200
        assert dl.content == _fake_mp3_bytes(b"chorus-audio")

        # source_not_found error propagation
        nf_id = "c" * 32
        sub2 = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": nf_id,
                "job_type": "rvc_cover",
                "payload": {"title": "找不到原曲", "auto_search": True},
            },
        )
        assert sub2.status_code == 202
        if (task2 := app.state.worker_state.tasks.get(nf_id)) is not None:
            await task2
        st2 = (await client.get(f"/v1/jobs/{nf_id}", headers=headers)).json()
        assert st2["status"] == "failed"
        assert st2["error_code"] == "source_not_found"
        assert st2["result"]["error_code"] == "source_not_found"

        # Emergency session termination
        usage_state["active"] = 2
        term = await client.post("/v1/sessions/terminate", headers=headers)
        assert term.status_code == 200 and term.json()["terminated_sessions"] == 2
    assert drive_client.folder_tree is not None


# ---------------------------------------------------------------------------
# 4. ColabComputeHub, SystemModule, and colab_admin tests
# ---------------------------------------------------------------------------


def test_colab_config_validation() -> None:
    cfg = ColabConfig(preferred_gpu="H100", fallback_gpus=["H100", "L4", "L4", "T4"])
    assert cfg.fallback_gpus == ["H100", "L4", "T4"]
    with pytest.raises(ValidationError):
        ColabConfig(unknown_field="x")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ColabConfig(daily_job_limit=True)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        ColabConfig(drive_songs_folder_id="root")


async def test_system_module_status_is_non_blocking_and_hub_scheduler_lifecycle(
    settings, store, tmp_path: Path
) -> None:
    app_w, cfg_w, _, usage_state = _build_test_worker(tmp_path)
    clock_now = [1_700_000_000.0]

    def fake_clock() -> float:
        return clock_now[0]

    s = _worker_settings(settings, "http://127.0.0.1:12146", cfg_w.token)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_w),
        base_url="http://127.0.0.1:12146",
    ) as worker_http:
        hub = ColabComputeHub(s, store, None, http=worker_http, clock=fake_clock)
        await hub.initialize()

        # status_summary must be non-blocking without hitting worker HTTP
        summary = await hub.status_summary()
        assert summary["configured"] is True
        assert summary["state"] == "ready"

        hook_events: list[dict[str, Any]] = []

        async def on_cover(job: dict[str, Any]) -> None:
            hook_events.append(job)

        hub.register_completion_hook("rvc_cover", on_cover)

        # Submit job -> scheduler_step dispatches -> worker finishes -> next step invokes hook
        inbox_up = await hub.upload_drive_inbox("demo.m4a", b"FAKE_M4A", mime_type="audio/mp4")
        job = await hub.submit_job(
            "rvc_cover",
            {"title": "小幸运", "artist": "田馥甄", "inbox_file_id": inbox_up["file_id"]},
        )
        assert job["status"] == "queued"

        await hub.scheduler_step()
        assert (await hub.list_jobs())[0]["status"] == "running"
        if (w_task := app_w.state.worker_state.tasks.get(job["id"])) is not None:
            await w_task
        usage_state["cu"] = 99.5
        await hub.scheduler_step()

        assert len(hook_events) == 1
        assert hook_events[0]["status"] == "succeeded"
        assert hook_events[0]["result"]["title"] == "小幸运"
        downloaded = await hub.download_drive_song(hook_events[0]["result"]["drive_chorus_id"])
        assert downloaded == _fake_mp3_bytes(b"chorus-audio")

        # Orphan session auto-termination after auto_terminate_orphan_minutes
        usage_state["active"] = 1
        clock_now[0] += 120.0
        await hub.scheduler_step()
        assert usage_state["active"] == 1
        clock_now[0] += 25 * 60.0
        await hub.scheduler_step()
        assert usage_state["active"] == 0

        # Daily limit & budget enforcement
        await hub.set_config(ColabConfig(daily_job_limit=1, daily_cu_budget=10.0))
        with pytest.raises(Rejected, match="colab_daily_job_limit"):
            await hub.submit_job("rvc_cover", {"title": "超限歌曲", "auto_search": True})
        await hub.set_config(ColabConfig(daily_job_limit=5, daily_cu_budget=0.0))
        with pytest.raises(Rejected, match="colab_daily_cu_budget"):
            await hub.submit_job("rvc_cover", {"title": "预算为零", "auto_search": True})

        # Job timeout handling
        await hub.set_config(
            ColabConfig(daily_job_limit=5, daily_cu_budget=10.0, job_timeout_seconds=120)
        )
        slow_job = await hub.submit_job("rvc_cover", {"title": "超时测试", "auto_search": True})
        async with store.transaction():
            await store.db.execute(
                "UPDATE colab_jobs SET status='running', started_at=? WHERE id=?",
                (clock_now[0] - 300.0, slow_job["id"]),
            )
        await hub.scheduler_step()
        timed_out = next(j for j in await hub.list_jobs() if j["id"] == slow_job["id"])
        assert timed_out["status"] == "failed" and timed_out["error_code"] == "job_timeout"

        await hub.close()


async def test_colab_admin_routes_and_non_blocking_module_status(settings, tmp_path: Path) -> None:
    app_w, cfg_w, _, usage_state = _build_test_worker(tmp_path)
    # Point URL to closed loopback port for app startup to verify Module.status() never blocks.
    s = _worker_settings(
        settings,
        "http://127.0.0.1:19999",
        cfg_w.token,
        admin_token=SecretStr(ADMIN_TOKEN),
    )
    app = create_app(s, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        mod = app.state.service.modules.systems["colab_compute"]
        assert mod.admin_panel == "colab-compute"
        st = await mod.status()
        assert st["state"] == "ready" and st["configured"] is True

        # Swap hub's http client to in-process ASGI worker app for full admin API integration
        worker_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_w),
            base_url="http://127.0.0.1:19999",
        )
        mod.compute._injected_http = worker_client
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            ) as client:
                unauth = await client.get("/admin/api/colab", headers={"Authorization": ""})
                assert unauth.status_code == 401
                assert (await client.get("/admin/api/colab?foo=1")).status_code == 400

                ov = await client.get("/admin/api/colab")
                assert ov.status_code == 200
                ov_json = ov.json()
                assert ov_json["configured"] is True
                assert ov_json["worker"]["reachable"] is True
                assert ov_json["worker"]["compute_units"] == 100.0
                expected_keys = {"root", "models", "songs", "inbox"}
                assert set(ov_json["worker"]["drive_folders"]) == expected_keys
                assert cfg_w.token not in ov.text

                # Update & reset config
                put = await client.put(
                    "/admin/api/colab",
                    json={"preferred_gpu": "A100", "daily_job_limit": 8},
                )
                assert put.status_code == 200
                assert put.json()["config"]["preferred_gpu"] == "A100"
                assert put.json()["source"] == "sqlite"

                delete = await client.delete("/admin/api/colab")
                assert delete.status_code == 200
                assert delete.json()["config"]["preferred_gpu"] == "L4"
                assert delete.json()["source"] == "default"

                # Submit & cancel job
                sub = await client.post(
                    "/admin/api/colab/jobs",
                    json={
                        "job_type": "rvc_cover",
                        "payload": {"title": "晴天", "auto_search": True},
                    },
                )
                assert sub.status_code == 200
                jid = sub.json()["id"]
                can = await client.post(f"/admin/api/colab/jobs/{jid}/cancel")
                assert can.status_code == 200 and can.json()["status"] == "cancelled"

                # Drive inbox listing
                await mod.compute.upload_drive_inbox("qingtian.wav", b"RIFFDATA")
                inbox = await client.get("/admin/api/colab/drive/inbox")
                assert inbox.status_code == 200
                assert inbox.json()["items"][0]["name"] == "qingtian.wav"

                # Emergency terminate + costly() rate limit
                usage_state["active"] = 1
                term = await client.post("/admin/api/colab/terminate-sessions")
                assert term.status_code == 200 and term.json()["terminated_sessions"] == 1
                for _ in range(4):
                    resp = await client.post("/admin/api/colab/terminate-sessions")
                    assert resp.status_code == 200
                assert (await client.post("/admin/api/colab/terminate-sessions")).status_code == 429
        finally:
            await worker_client.aclose()


async def test_colab_admin_drive_inbox_upload(settings, tmp_path: Path) -> None:
    app_w, cfg_w, drive_client, _ = _build_test_worker(tmp_path)
    s = _worker_settings(
        settings,
        "http://127.0.0.1:19999",
        cfg_w.token,
        admin_token=SecretStr(ADMIN_TOKEN),
    )
    app = create_app(s, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        mod = app.state.service.modules.systems["colab_compute"]
        worker_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_w),
            base_url="http://127.0.0.1:19999",
        )
        mod.compute._injected_http = worker_client
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:12141",
                headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            ) as client:
                mp3_bytes = _fake_mp3_bytes(b"inbox-source")

                # Unauthenticated request rejected
                unauth = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Authorization": "",
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=mp3_bytes,
                )
                assert unauth.status_code == 401

                # Missing / blank / oversized header / traversal / bad extension / empty / too large
                missing = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={"Content-Type": "audio/mpeg"},
                    content=mp3_bytes,
                )
                assert missing.status_code == 400
                assert missing.json()["detail"] == "colab_drive_invalid_file"

                too_long_hdr = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": ("a" * 510) + ".mp3",
                    },
                    content=mp3_bytes,
                )
                assert too_long_hdr.status_code == 400
                assert too_long_hdr.json()["detail"] == "colab_drive_invalid_file"

                for bad_name in ("../a.mp3", "..%2Fa.mp3", "sub\\a.mp3", "a%00.mp3"):
                    trav = await client.post(
                        "/admin/api/colab/drive/inbox",
                        headers={
                            "Content-Type": "audio/mpeg",
                            "X-Drive-Filename": bad_name,
                        },
                        content=mp3_bytes,
                    )
                    assert trav.status_code == 400
                    assert trav.json()["detail"] == "colab_drive_invalid_file"

                for bad_ext in ("evil.txt", "noext", "bad."):
                    resp = await client.post(
                        "/admin/api/colab/drive/inbox",
                        headers={
                            "Content-Type": "audio/mpeg",
                            "X-Drive-Filename": bad_ext,
                        },
                        content=mp3_bytes,
                    )
                    assert resp.status_code == 400
                    assert resp.json()["detail"] == "invalid_song_audio"

                empty_body = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=b"",
                )
                assert empty_body.status_code == 400
                assert empty_body.json()["detail"] == "invalid_song_audio"

                bad_mime = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "text/plain",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=mp3_bytes,
                )
                assert bad_mime.status_code == 415
                assert bad_mime.json()["detail"] == "invalid_song_audio"

                compressed = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "Content-Encoding": "gzip",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=mp3_bytes,
                )
                assert compressed.status_code == 415
                assert compressed.json()["detail"] == "compressed_song_upload_forbidden"

                oversized = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=b"0" * (18 * 1024 * 1024 + 1),
                )
                assert oversized.status_code == 413
                assert oversized.json()["detail"] == "colab_inbox_upload_too_large"
                assert drive_client.list_inbox() == []

                # Valid Unicode MP3 upload proxies to worker /v1/drive/inbox
                ok = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "audio/mpeg",
                        "X-Drive-Filename": quote("晴天.mp3"),
                    },
                    content=mp3_bytes,
                )
                assert ok.status_code == 200
                body = ok.json()
                assert body["name"] == "晴天.mp3"
                assert body["size"] == len(mp3_bytes)
                assert body["file_id"]

                # Verify filename/extension normalization (uppercase ext & spaces)
                ok_norm = await client.post(
                    "/admin/api/colab/drive/inbox",
                    headers={
                        "Content-Type": "application/octet-stream",
                        "X-Drive-Filename": quote("周杰伦 晴天 (Live).MP3"),
                    },
                    content=mp3_bytes,
                )
                assert ok_norm.status_code == 200
                assert ok_norm.json()["name"] == "周杰伦_晴天_Live.mp3"

                inbox = await client.get("/admin/api/colab/drive/inbox")
                assert inbox.status_code == 200
                names = {item["name"]: item for item in inbox.json()["items"]}
                assert "晴天.mp3" in names
                assert names["晴天.mp3"]["id"] == body["file_id"]
                assert names["晴天.mp3"]["size"] == len(mp3_bytes)
                assert "周杰伦_晴天_Live.mp3" in names
        finally:
            await worker_client.aclose()
