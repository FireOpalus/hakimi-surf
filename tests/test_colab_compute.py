"""Tests for colab_drive, colab_worker, DSP chorus detection, ColabComputeHub, and colab_admin."""

from __future__ import annotations

import ast
import importlib.util
import io
import json
import os
import struct
import subprocess
import sys
import wave
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from hakimi.app import create_app
from hakimi.colab_compute import ColabComputeHub, ColabConfig
from hakimi.security import Rejected
from hakimi.songs import (
    RvcCoverJobPayload,
    RvcTrainJobPayload,
    validate_rvc_cover_payload,
    validate_rvc_train_payload,
)
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
colab_jobs = colab_worker.colab_jobs_mod
colab_jobs_rvc = colab_jobs.rvc


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
                    if item.get("trashed"):
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
            if (qs.get("uploadType") or [""])[0] == "resumable":
                meta = json.loads((body or b"{}").decode("utf-8"))
                sess_id = self._next_id("upload_sess")
                self.resumable_sessions[sess_id] = {
                    "meta": meta,
                    "existing_id": fid,
                    "buf": bytearray(),
                    "total": int(headers.get("X-Upload-Content-Length", "0")),
                    "mime": headers.get("X-Upload-Content-Type", "application/octet-stream"),
                }
                loc = f"https://www.googleapis.com/upload/resumable/{sess_id}"
                return 200, {"location": loc}, b""
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
            fid = sess.get("existing_id") or self._next_id("drv_big")
            meta = sess["meta"]
            data = bytes(sess["buf"])
            rec = {
                "id": fid,
                "name": meta["name"],
                "parents": meta.get("parents") or self.files.get(fid, {}).get("parents", []),
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


def test_colab_drive_upload_file_disk_chunked_resumable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cred_path = tmp_path / "secrets" / "colab-drive-oauth.json"
    _write_fake_oauth(cred_path)
    backend = FakeDriveBackend()
    put_sizes: list[int] = []

    def tracking_transport(
        method: str,
        url: str,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, dict[str, str], bytes]:
        if method == "PUT" and "/upload/resumable/" in url:
            put_sizes.append(len(body or b""))
        return backend.transport(method, url, headers, body, timeout)

    client = colab_drive.ColabDriveClient(cred_path, transport=tracking_transport)
    tree = client.ensure_folder_tree()

    # 5 MiB temp file must stream from disk in 2 resumable chunks (4 MiB + 1 MiB).
    big_path = tmp_path / "big_inbox.flac"
    big_bytes = b"fLaC" + (b"\x5a" * (5 * 1024 * 1024 - 4))
    big_path.write_bytes(big_bytes)

    res = client.upload_file("inbox", big_path, mime_type="audio/flac")
    assert res["name"] == "big_inbox.flac"
    assert res["size"] == len(big_bytes)
    assert put_sizes == [4 * 1024 * 1024, 1 * 1024 * 1024]
    assert client.download_bytes(res["file_id"], allowed_roles=("inbox",)) == big_bytes

    # Direct _upload_resumable_file with existing_id updates in place in 2 chunks as well.
    put_sizes.clear()
    updated_bytes = b"fLaC" + (b"\x3c" * (5 * 1024 * 1024 - 4))
    big_path.write_bytes(updated_bytes)
    resp2 = client._upload_resumable_file(
        tree.inbox,
        "big_inbox.flac",
        big_path,
        len(updated_bytes),
        "audio/flac",
        existing_id=res["file_id"],
        app_properties={"hakimi_songs_role": "inbox"},
    )
    assert resp2["id"] == res["file_id"]
    assert put_sizes == [4 * 1024 * 1024, 1 * 1024 * 1024]
    assert client.download_bytes(res["file_id"], allowed_roles=("inbox",)) == updated_bytes

    # Small file (<= 4 MiB) uses multipart/bytes without resumable chunks.
    put_sizes.clear()
    small_path = tmp_path / "small.mp3"
    small_bytes = _fake_mp3_bytes(b"small-file")
    small_path.write_bytes(small_bytes)
    small_res = client.upload_file("inbox", small_path, mime_type="audio/mpeg")
    assert small_res["size"] == len(small_bytes)
    assert put_sizes == []

    # Oversized file beyond MAX_INBOX_UPLOAD_BYTES fails before reading.
    monkeypatch.setattr(colab_drive, "MAX_INBOX_UPLOAD_BYTES", 64)
    with pytest.raises(colab_drive.ColabDriveError) as size_exc:
        client.upload_file("inbox", small_path, mime_type="audio/mpeg")
    assert size_exc.value.code == "drive_file_too_large"


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


_MINIMAL_NUMPY_STUB = """import array

float32 = "float32"


class ndarray:
    def __init__(self, data, dtype=float32):
        self._data = list(data)
        self.dtype = dtype
        self.ndim = 1
        self.size = len(self._data)

    def copy(self):
        return ndarray(self._data, dtype=self.dtype)


def frombuffer(buf, dtype=float32):
    if dtype != float32:
        raise ValueError(f"unsupported dtype: {dtype}")
    arr = array.array("f")
    arr.frombytes(buf)
    return ndarray(list(arr), dtype=dtype)
"""


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
        assert "_load_audio_16k" in cmd[2]
        assert "model.transcribe(audio," in cmd[2]
        assert "model.transcribe(sys.argv[1]" not in cmd[2]
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

        def transcribe(self, audio: Any, **kwargs: Any):
            assert not isinstance(audio, (str, Path))
            assert audio.ndim == 1 and audio.size == 8
            transcribe_calls.append((self.device, dict(kwargs)))
            if self.device == "cuda":
                raise RuntimeError("cuda unavailable")
            seg = types.SimpleNamespace(start=10.0, end=14.0, text="夜に駆ける")
            info = types.SimpleNamespace(language="ja")
            return iter([seg]), info

    fake_fw = types.ModuleType("faster_whisper")
    fake_fw.WhisperModel = _FakeWhisperModel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "faster_whisper", fake_fw)
    if importlib.util.find_spec("numpy") is None:
        fake_np = types.ModuleType("numpy")
        exec(_MINIMAL_NUMPY_STUB, fake_np.__dict__)
        monkeypatch.setitem(sys.modules, "numpy", fake_np)
    monkeypatch.setattr(sys, "argv", ["whisper.py", str(tmp_path / "vocals.wav")])

    def fake_ffmpeg_run(cmd, **kwargs):
        del kwargs
        assert cmd[0] == "ffmpeg"
        assert "-ar" in cmd and cmd[cmd.index("-ar") + 1] == "16000"
        assert "-f" in cmd and cmd[cmd.index("-f") + 1] == "f32le"
        return subprocess.CompletedProcess(
            cmd, 0, stdout=struct.pack("<8f", *([0.25, -0.25] * 4)), stderr=b""
        )

    stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
    with monkeypatch.context() as mp:
        mp.setattr(subprocess, "run", fake_ffmpeg_run)
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


def test_transcribe_for_chorus_subprocess_decodes_ndarray_and_bypasses_pyav(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_dir = tmp_path / "bin"
    py_dir = tmp_path / "pydeps"
    bin_dir.mkdir()
    py_dir.mkdir()

    fake_ffmpeg = bin_dir / "ffmpeg"
    fake_ffmpeg.write_text(
        f"#!{sys.executable}\n"
        "import os, struct, sys\n"
        "argv = sys.argv[1:]\n"
        "assert '-ar' in argv and argv[argv.index('-ar') + 1] == '16000'\n"
        "assert '-ac' in argv and argv[argv.index('-ac') + 1] == '1'\n"
        "assert '-f' in argv and argv[argv.index('-f') + 1] == 'f32le'\n"
        "assert argv[-1] == '-'\n"
        "if os.environ.get('FAKE_FFMPEG_FAIL') == '1':\n"
        "    sys.stderr.write('simulated ffmpeg decode error\\n')\n"
        "    sys.exit(1)\n"
        "sys.stdout.buffer.write(struct.pack('<16f', *([0.1, -0.1] * 8)))\n",
        encoding="utf-8",
    )
    os.chmod(fake_ffmpeg, 0o755)

    if importlib.util.find_spec("numpy") is None:
        (py_dir / "numpy.py").write_text(_MINIMAL_NUMPY_STUB, encoding="utf-8")

    trace_file = tmp_path / "whisper_calls.jsonl"
    (py_dir / "faster_whisper.py").write_text(
        "import json, os, types\n"
        "import numpy as np\n"
        "\n"
        "class WhisperModel:\n"
        "    def __init__(self, size, *, device, compute_type):\n"
        "        self.size = size\n"
        "        self.device = device\n"
        "        self.compute_type = compute_type\n"
        "\n"
        "    def transcribe(self, audio, **kwargs):\n"
        "        trace_path = os.environ.get('FAKE_WHISPER_TRACE')\n"
        "        if trace_path:\n"
        "            rec = {\n"
        "                'device': self.device,\n"
        "                'compute_type': self.compute_type,\n"
        "                'kwargs': kwargs,\n"
        "            }\n"
        "            with open(trace_path, 'a', encoding='utf-8') as fh:\n"
        "                fh.write(json.dumps(rec) + '\\n')\n"
        "        if isinstance(audio, str):\n"
        "            raise TypeError(\n"
        "                \"av.open() got an unexpected keyword argument 'metadata_errors'\"\n"
        "            )\n"
        "        assert isinstance(audio, np.ndarray), f'expected ndarray, got {type(audio)}'\n"
        "        assert audio.ndim == 1, f'expected 1D ndarray, got ndim={audio.ndim}'\n"
        "        assert audio.dtype == np.float32, f'expected float32, got {audio.dtype}'\n"
        "        assert audio.size > 0, f'expected non-empty ndarray, got size={audio.size}'\n"
        "        if self.device == 'cuda':\n"
        "            raise RuntimeError('CUDA driver unavailable')\n"
        "        if kwargs.get('vad_filter'):\n"
        "            raise RuntimeError('silero VAD onnx unavailable')\n"
        "        segs = [\n"
        "            types.SimpleNamespace(start=58.5, end=63.25, text='桜流し'),\n"
        "            types.SimpleNamespace(start=63.25, end=69.0, text='君のそばで'),\n"
        "        ]\n"
        "        info = types.SimpleNamespace(language='ja')\n"
        "        return iter(segs), info\n",
        encoding="utf-8",
    )

    existing_path = os.environ.get("PATH", "")
    existing_pp = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv(
        "PATH", f"{bin_dir}{os.pathsep}{existing_path}" if existing_path else str(bin_dir)
    )
    monkeypatch.setenv(
        "PYTHONPATH", f"{py_dir}{os.pathsep}{existing_pp}" if existing_pp else str(py_dir)
    )
    monkeypatch.setenv("FAKE_WHISPER_TRACE", str(trace_file))

    logs: list[str] = []
    monkeypatch.setattr(colab_remote, "log", logs.append)
    dummy_wav = tmp_path / "vocals.wav"
    dummy_wav.write_bytes(b"RIFF")

    segs = colab_remote._transcribe_for_chorus(sys.executable, dummy_wav)
    assert segs == [
        {"start": 58.5, "end": 63.25, "text": "桜流し"},
        {"start": 63.25, "end": 69.0, "text": "君のそばで"},
    ]

    calls = [json.loads(line) for line in trace_file.read_text(encoding="utf-8").splitlines()]
    assert calls == [
        {"device": "cuda", "compute_type": "float16", "kwargs": {"vad_filter": True}},
        {"device": "cuda", "compute_type": "float16", "kwargs": {"vad_filter": False}},
        {"device": "cpu", "compute_type": "int8", "kwargs": {"vad_filter": True}},
        {"device": "cpu", "compute_type": "int8", "kwargs": {"vad_filter": False}},
    ]
    assert any(
        "CUDA transcribe failed" in msg and "falling back to CPU int8." in msg for msg in logs
    )
    assert any(
        "vad_filter=True failed on cpu" in msg and "retrying without VAD." in msg for msg in logs
    )
    assert any("transcribed 2 segment(s) on cpu (lang=ja)." in msg for msg in logs)
    assert not any("metadata_errors" in msg for msg in logs)

    logs.clear()
    monkeypatch.setenv("FAKE_FFMPEG_FAIL", "1")
    assert colab_remote._transcribe_for_chorus(sys.executable, dummy_wav) == []
    assert any(
        "ffmpeg 16k decode failed" in msg and "simulated ffmpeg decode error" in msg for msg in logs
    )
    assert any("faster-whisper skipped/failed" in msg for msg in logs)


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
# 3. colab_worker & EphemeralDriveClient tests
# ---------------------------------------------------------------------------


def _make_drive_dispatch_context(
    tree: Any,
    *,
    access_token: str = "ya29.fake-access-token",
    cache_folder_id: str = "",
    inbox_file_id: str = "",
    inbox_name: str = "",
    pth_file_id: str = "",
    index_file_id: str = "",
    cache_weights_file_id: str = "",
    cache_weights_sha256: str = "",
) -> dict[str, Any]:
    return {
        "drive": {
            "access_token": access_token,
            "folders": {
                "root": tree.root,
                "models": tree.models,
                "songs": tree.songs,
                "inbox": tree.inbox,
                "cache": cache_folder_id,
            },
            "files": {
                "inbox_file_id": inbox_file_id,
                "inbox_name": inbox_name,
                "pth_file_id": pth_file_id,
                "index_file_id": index_file_id,
                "cache_weights_file_id": cache_weights_file_id,
                "cache_weights_sha256": cache_weights_sha256,
            },
        }
    }


def _build_test_worker(
    tmp_path: Path,
    *,
    clean_search_fn=None,
    cover_runner=None,
    train_runner=None,
    stop_all_fn=None,
):
    cred = tmp_path / "secrets" / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()
    drive_client = colab_drive.ColabDriveClient(cred, transport=backend.transport)
    tree = drive_client.ensure_folder_tree()

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
        drive_transport=backend.transport,
        usage_fn=usage_fn,
        stop_all_fn=stop_all_fn or default_stop_all,
        cover_runner=cover_runner or default_cover,
        train_runner=train_runner,
        clean_search_fn=clean_search_fn,
    )
    return app, cfg, drive_client, tree, usage_state, backend


def test_ephemeral_drive_client_scoping_resumable_and_token_guards(tmp_path: Path) -> None:
    cred = tmp_path / "secrets" / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()
    seed = colab_drive.ColabDriveClient(cred, transport=backend.transport)
    tree = seed.ensure_folder_tree()
    inbox_up = seed.upload_bytes("inbox", "track.m4a", b"M4A-INBOX", mime_type="audio/mp4")
    pth_up = seed.upload_bytes("models", "hakimi.pth", b"PTH-BIN")

    # Token & folder validation guards
    for bad_tok in ("", "1//refresh-never-allowed", "Bearer ya29.abc", "ya29.bad space"):
        with pytest.raises(colab_worker.EphemeralDriveError) as exc:
            colab_worker.EphemeralDriveClient(bad_tok, tree.as_dict(), transport=backend.transport)
        assert exc.value.code == "drive_token_failed"

    with pytest.raises(colab_worker.EphemeralDriveError) as root_exc:
        colab_worker.EphemeralDriveClient(
            "ya29.fake-access-token",
            {**tree.as_dict(), "songs": "root"},
            transport=backend.transport,
        )
    assert root_exc.value.code == "drive_context_required"

    ephem = colab_worker.EphemeralDriveClient(
        "ya29.fake-access-token",
        tree.as_dict(),
        transport=backend.transport,
    )

    # In-scope downloads succeed for inbox/models
    dl_inbox = ephem.download_file(
        inbox_up["file_id"], tmp_path / "in.m4a", allowed_roles=("inbox",)
    )
    assert dl_inbox.read_bytes() == b"M4A-INBOX"
    assert ephem.download_bytes(pth_up["file_id"], allowed_roles=("models",)) == b"PTH-BIN"

    # Cross-role & out-of-scope downloads are strictly rejected
    with pytest.raises(colab_worker.EphemeralDriveError) as cross_exc:
        ephem.download_bytes(inbox_up["file_id"], allowed_roles=("models",))
    assert cross_exc.value.code == "drive_file_outside_scope"

    with pytest.raises(colab_worker.EphemeralDriveError) as out_exc:
        ephem.download_bytes("outside_secret_file", allowed_roles=("inbox", "models", "songs"))
    assert out_exc.value.code == "drive_file_outside_scope"

    # Strict metadata checks: trashed=True, missing trashed, id mismatch, and folder MIME
    backend.files["trashed_in_inbox"] = {
        "id": "trashed_in_inbox",
        "name": "deleted.m4a",
        "parents": [tree.inbox],
        "mimeType": "audio/mp4",
        "size": "4",
        "modifiedTime": "2026-11-21T00:00:00Z",
        "trashed": True,
        "data": b"DEAD",
    }
    backend.files["missing_trashed_flag"] = {
        "id": "missing_trashed_flag",
        "name": "untrusted.m4a",
        "parents": [tree.inbox],
        "mimeType": "audio/mp4",
        "size": "4",
        "modifiedTime": "2026-11-21T00:00:00Z",
        "data": b"NOFL",
    }
    backend.files["mismatched_id_file"] = {
        "id": "spoofed_other_id",
        "name": "spoof.m4a",
        "parents": [tree.inbox],
        "mimeType": "audio/mp4",
        "size": "4",
        "modifiedTime": "2026-11-21T00:00:00Z",
        "trashed": False,
        "data": b"SPOF",
    }
    backend.files["folder_in_inbox"] = {
        "id": "folder_in_inbox",
        "name": "sub",
        "parents": [tree.inbox],
        "mimeType": colab_worker.FOLDER_MIME,
        "size": "0",
        "modifiedTime": "2026-11-21T00:00:00Z",
        "trashed": False,
        "data": b"",
    }
    for bad_fid in (
        "trashed_in_inbox",
        "missing_trashed_flag",
        "mismatched_id_file",
        "folder_in_inbox",
    ):
        with pytest.raises(colab_worker.EphemeralDriveError) as meta_exc:
            ephem.download_bytes(bad_fid, allowed_roles=("inbox",))
        assert meta_exc.value.code == "drive_file_outside_scope", bad_fid

    # Upload restricted strictly to songs & models (never inbox/root)
    small_mp3 = tmp_path / "cover.mp3"
    small_mp3.write_bytes(_fake_mp3_bytes(b"ephem-small"))
    with pytest.raises(colab_worker.EphemeralDriveError) as role_exc:
        ephem.upload_file("inbox", small_mp3)  # type: ignore[arg-type]
    assert role_exc.value.code == "drive_file_outside_scope"

    up_song = ephem.upload_file("songs", small_mp3, mime_type="audio/mpeg")
    assert seed.download_bytes(up_song["file_id"], allowed_roles=("songs",)) == (
        small_mp3.read_bytes()
    )

    # Resumable upload (> 4 MiB) to models succeeds cleanly
    big_pth = tmp_path / "big.pth"
    big_bytes = b"PTH" + (b"\x7a" * (5 * 1024 * 1024 - 3))
    big_pth.write_bytes(big_bytes)
    up_model = ephem.upload_file("models", big_pth, drive_name="hakimi.pth")
    assert seed.download_bytes(up_model["file_id"], allowed_roles=("models",)) == big_bytes


async def test_worker_auth_whitelist_cover_and_source_not_found(tmp_path: Path) -> None:
    def failing_search(*args, **kwargs):
        del args, kwargs
        raise colab_rvc.SourceNotFoundError("source_not_found", "no clean original")

    app, cfg, drive_client, tree, usage_state, _ = _build_test_worker(
        tmp_path, clean_search_fn=failing_search
    )

    # CLI parser accepts pure GPU worker args only (no --drive-cred-path or --songs-folder-id)
    parsed_args = colab_worker._build_parser().parse_args(
        ["--host", "127.0.0.1", "--port", "12146", "--allow-private"]
    )
    assert (parsed_args.host, parsed_args.port, parsed_args.allow_private) == (
        "127.0.0.1",
        12146,
        True,
    )
    for removed_flag in ("--drive-cred-path", "--songs-folder-id"):
        with pytest.raises(SystemExit):
            colab_worker._build_parser().parse_args([removed_flag, "x"])

    headers = {"Authorization": f"Bearer {cfg.token}"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://127.0.0.1:12146",
    ) as client:
        assert (await client.get("/v1/status")).status_code == 401
        assert (await client.get("/v1/status?q=1", headers=headers)).status_code == 400
        st_resp = await client.get("/v1/status", headers=headers)
        assert st_resp.status_code == 200
        st_json = st_resp.json()
        assert "drive_folders" not in st_json
        assert st_json["supported_job_types"] == ["rvc_cover", "rvc_train"]

        # All removed /v1/drive/* proxy endpoints MUST return 404
        assert (await client.get("/v1/drive/inbox", headers=headers)).status_code == 404
        assert (
            await client.post("/v1/drive/inbox", headers=headers, content=b"x")
        ).status_code == 404
        assert (
            await client.get("/v1/drive/songs/outside_secret_file", headers=headers)
        ).status_code == 404
        assert (
            await client.post("/v1/drive/songs", headers=headers, content=b"x")
        ).status_code == 404

        # Reject unknown job_type, legacy drive_songs_folder_id, refresh_token, and "root" folder
        assert (
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={"job_id": "a" * 32, "job_type": "rm_rf", "payload": {}},
            )
        ).status_code == 400
        assert (
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": "a" * 32,
                    "job_type": "rvc_cover",
                    "drive_songs_folder_id": tree.root,
                    "payload": {"title": "旧字段拒绝", "auto_search": True},
                },
            )
        ).status_code == 400
        assert (
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": "a" * 32,
                    "job_type": "rvc_cover",
                    "payload": {"title": "禁刷新令牌", "auto_search": True},
                    "dispatch_context": _make_drive_dispatch_context(
                        tree, access_token="1//refresh-token-forbidden"
                    ),
                },
            )
        ).status_code == 400
        bad_root_ctx = _make_drive_dispatch_context(tree)
        bad_root_ctx["drive"]["folders"]["songs"] = "root"
        assert (
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": "a" * 32,
                    "job_type": "rvc_cover",
                    "payload": {"title": "禁root目录", "auto_search": True},
                    "dispatch_context": bad_root_ctx,
                },
            )
        ).status_code == 400

        # Missing dispatch_context for rvc_cover returns explicit drive_context_required error
        no_ctx_id = "d" * 32
        sub_no_ctx = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": no_ctx_id,
                "job_type": "rvc_cover",
                "payload": {"title": "缺上下文", "auto_search": True},
            },
        )
        assert sub_no_ctx.status_code == 202
        if (task_no_ctx := app.state.worker_state.tasks.get(no_ctx_id)) is not None:
            await task_no_ctx
        st_no_ctx = (await client.get(f"/v1/jobs/{no_ctx_id}", headers=headers)).json()
        assert st_no_ctx["status"] == "failed"
        assert st_no_ctx["error_code"] == "drive_context_required"

        # Out-of-scope inbox file ID rejected even in Drive direct mode
        out_id = "e" * 32
        sub_out = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": out_id,
                "job_type": "rvc_cover",
                "payload": {"title": "越权文件", "inbox_file_id": "outside_secret_file"},
                "dispatch_context": _make_drive_dispatch_context(
                    tree, inbox_file_id="outside_secret_file", inbox_name="secret.m4a"
                ),
            },
        )
        assert sub_out.status_code == 202
        if (task_out := app.state.worker_state.tasks.get(out_id)) is not None:
            await task_out
        st_out = (await client.get(f"/v1/jobs/{out_id}", headers=headers)).json()
        assert st_out["status"] == "failed"
        assert st_out["error_code"] == "drive_file_outside_scope"
        assert "ya29.fake-access-token" not in json.dumps(st_out)

        # Seed inbox directly in Drive and run rvc_cover with ephemeral dispatch_context
        inbox_up = drive_client.upload_bytes(
            "inbox", "yu_ai.m4a", b"RAW_SOURCE_AUDIO", mime_type="audio/mp4"
        )
        inbox_id = inbox_up["file_id"]

        job_id = "b" * 32
        sub = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": job_id,
                "job_type": "rvc_cover",
                "gpu_candidates": ["L4", "T4"],
                "payload": {"title": "雨爱", "artist": "杨丞琳", "inbox_file_id": inbox_id},
                "dispatch_context": _make_drive_dispatch_context(
                    tree, inbox_file_id=inbox_id, inbox_name="yu_ai.m4a"
                ),
            },
        )
        assert sub.status_code == 202
        if (task1 := app.state.worker_state.tasks.get(job_id)) is not None:
            await task1
        rec1 = app.state.worker_state.jobs[job_id]
        assert not hasattr(rec1, "dispatch_context")
        st = (await client.get(f"/v1/jobs/{job_id}", headers=headers)).json()
        assert st["status"] == "succeeded"
        assert st["assigned_gpu"] == "L4"
        assert st["result"]["title"] == "雨爱"
        assert st["result"]["drive_chorus_id"]
        assert st["result"]["drive_full_id"]
        assert cfg.token not in json.dumps(st)
        assert "ya29.fake-access-token" not in json.dumps(st)
        assert "dispatch_context" not in st

        # Verify EphemeralDriveClient fallback uploaded chorus audio into Hakimi_Songs/songs/
        dl_bytes = drive_client.download_bytes(
            st["result"]["drive_chorus_id"], allowed_roles=("songs",)
        )
        assert dl_bytes == _fake_mp3_bytes(b"chorus-audio")

        # source_not_found error propagation with valid Drive context
        nf_id = "c" * 32
        sub2 = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": nf_id,
                "job_type": "rvc_cover",
                "payload": {"title": "找不到原曲", "auto_search": True},
                "dispatch_context": _make_drive_dispatch_context(tree),
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


# ---------------------------------------------------------------------------
# 4. ColabComputeHub, SystemModule, and colab_admin tests
# ---------------------------------------------------------------------------


def test_colab_config_validation() -> None:
    cfg = ColabConfig(preferred_gpu="H100", fallback_gpus=["H100", "L4", "L4", "T4"])
    assert cfg.fallback_gpus == ["H100", "L4", "T4"]
    assert "drive_songs_folder_id" not in ColabConfig.model_fields
    with pytest.raises(ValidationError):
        ColabConfig(unknown_field="x")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ColabConfig(daily_job_limit=True)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        ColabConfig(drive_songs_folder_id="drv_folder_0000000001")  # type: ignore[call-arg]


async def test_colab_hub_legacy_config_migration_dynamic_registry_and_pre_dispatch(
    settings, store
) -> None:
    # 1) Legacy SQLite admin_config("colab_compute_config") with removed drive_songs_folder_id
    legacy_payload = {
        "preferred_gpu": "A100",
        "daily_job_limit": 9,
        "drive_songs_folder_id": "drv_legacy_songs_99",
    }
    sealed = store.vault.seal("admin:colab_compute_config", legacy_payload)
    async with store.transaction():
        await store.db.execute(
            "CREATE TABLE IF NOT EXISTS admin_config (key TEXT PRIMARY KEY, payload BLOB NOT NULL)"
        )
        await store.db.execute(
            "INSERT OR REPLACE INTO admin_config VALUES (?, ?)",
            ("colab_compute_config", sealed),
        )

    dispatched: list[dict[str, Any]] = []

    def mock_worker(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/status":
            return httpx.Response(
                200,
                json={
                    "reachable": True,
                    "compute_units": 42.0,
                    "active_sessions": 0,
                    "running_job_id": None,
                    "supported_job_types": ["demo_gpu"],
                    "warm_sessions": [],
                    "error": None,
                },
            )
        if request.url.path == "/v1/jobs" and request.method == "POST":
            body = json.loads(request.content.decode("utf-8"))
            dispatched.append(body)
            return httpx.Response(202, json={"job_id": body["job_id"], "status": "running"})
        return httpx.Response(404)

    s = _worker_settings(settings, "http://127.0.0.1:12146", "worker-secret-token-123456")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(mock_worker),
        base_url="http://127.0.0.1:12146",
    ) as mock_http:
        hub = ColabComputeHub(s, store, None, http=mock_http)
        await hub.initialize()

        # Legacy field ignored gracefully, valid overrides preserved, SQLite row kept for migration
        assert hub.config.preferred_gpu == "A100"
        assert hub.config.daily_job_limit == 9
        assert not hasattr(hub.config, "drive_songs_folder_id")
        async with store.lock:
            row = await store.one(
                "SELECT payload FROM admin_config WHERE key=?", ("colab_compute_config",)
            )
        assert row is not None
        assert store.vault.open("admin:colab_compute_config", row[0])["drive_songs_folder_id"] == (
            "drv_legacy_songs_99"
        )

        # All removed Drive proxy methods MUST be absent from ColabComputeHub
        for removed_method in (
            "list_drive_inbox",
            "upload_drive_inbox",
            "upload_drive_inbox_stream",
            "download_drive_song",
            "upload_drive_song",
        ):
            assert not hasattr(hub, removed_method)

        # Unregistered job_type is strictly rejected
        assert hub.registered_job_types() == ()
        assert not hub.is_registered_job_type("rvc_cover")
        with pytest.raises(Rejected, match="colab_invalid_job"):
            await hub.submit_job("rvc_cover", {"title": "未注册"})

        # Register dynamic job type with validator + pre_dispatch + on_complete
        ephemeral_secret = "ya29.ephemeral-dispatch-secret-999"
        pre_dispatch_mode = {"mode": "reject_clean"}
        completed_events: list[dict[str, Any]] = []

        def validate_demo(payload: dict[str, Any]) -> dict[str, Any]:
            name = str(payload.get("name") or "").strip()
            if not name:
                raise ValueError("missing name")
            return {"name": name}

        async def pre_dispatch_demo(job: dict[str, Any]) -> dict[str, Any]:
            if pre_dispatch_mode["mode"] == "reject_clean":
                raise Rejected("song_drive_not_configured")
            if pre_dispatch_mode["mode"] == "reject_with_secrets":
                raise Rejected(
                    "err ya29.leak-pre-123456 1//refresh-pre-654321 Bearer secretprebearer999"
                )
            return {
                "drive": {
                    "access_token": ephemeral_secret,
                    "folders": {"songs": "drv_songs_folder_1"},
                    "files": {},
                }
            }

        async def on_demo_done(job: dict[str, Any]) -> None:
            completed_events.append(job)

        hub.register_job_type(
            "demo_gpu",
            validator=validate_demo,
            pre_dispatch=pre_dispatch_demo,
            on_complete=on_demo_done,
        )
        assert hub.is_registered_job_type("demo_gpu")
        assert hub.registered_job_types() == ("demo_gpu",)

        ov = await hub.admin_overview()
        assert ov["supported_job_types"] == ["demo_gpu"]
        assert ov["worker"]["supported_job_types"] == ["demo_gpu"]
        assert "drive_folders" not in ov["worker"]

        # pre_dispatch Rejected -> job finalized as failed + hook triggered + worker NOT called
        j_fail = await hub.submit_job("demo_gpu", {"name": "fail-case"})
        await hub.scheduler_step()
        assert dispatched == []
        assert len(completed_events) == 1
        assert completed_events[0]["id"] == j_fail["id"]
        assert completed_events[0]["status"] == "failed"
        assert completed_events[0]["error_code"] == "song_drive_not_configured"

        # pre_dispatch Rejected containing tokens -> error_code, result, error & overview scrubbed
        pre_dispatch_mode["mode"] = "reject_with_secrets"
        j_leak = await hub.submit_job("demo_gpu", {"name": "leak-case"})
        await hub.scheduler_step()
        assert dispatched == []
        assert len(completed_events) == 2
        assert completed_events[1]["id"] == j_leak["id"]
        assert completed_events[1]["status"] == "failed"
        leaked_tokens = (
            "ya29.leak-pre-123456",
            "1//refresh-pre-654321",
            "secretprebearer999",
        )
        async with store.lock:
            leak_row = await store.one(
                "SELECT error_code, error, result FROM colab_jobs WHERE id=?", (j_leak["id"],)
            )
        assert leak_row is not None
        opened_leak_res = hub._open_blob(j_leak["id"], "result", leak_row["result"]) or {}
        for blob_text in (
            str(leak_row["error_code"] or ""),
            str(leak_row["error"] or ""),
            json.dumps(opened_leak_res),
            json.dumps(completed_events[1]),
            json.dumps(await hub.admin_overview()),
        ):
            for secret_val in leaked_tokens:
                assert secret_val not in blob_text
        assert "[REDACTED]" in str(leak_row["error_code"] or "")
        assert "[REDACTED]" in str(opened_leak_res.get("error_code") or "")

        # pre_dispatch success -> transient dispatch_context forwarded to worker, NEVER in SQLite
        pre_dispatch_mode["mode"] = "ok"
        j_ok = await hub.submit_job("demo_gpu", {"name": "ok-case"})
        await hub.scheduler_step()
        assert len(dispatched) == 1
        assert dispatched[0]["job_id"] == j_ok["id"]
        assert "drive_songs_folder_id" not in dispatched[0]
        assert dispatched[0]["dispatch_context"]["drive"]["access_token"] == ephemeral_secret

        # Verify zero token/dispatch_context persisted in SQLite or returned by list_jobs/overview
        async with store.lock:
            raw_rows = await store.all("SELECT id, payload, result, error FROM colab_jobs")
        for r in raw_rows:
            for col in (r["payload"], r["result"], r["error"]):
                assert ephemeral_secret not in str(col or "")
            opened_p = hub._open_blob(str(r["id"]), "payload", r["payload"]) or {}
            assert "dispatch_context" not in opened_p
            assert ephemeral_secret not in json.dumps(opened_p)
        assert ephemeral_secret not in json.dumps(await hub.list_jobs())
        assert ephemeral_secret not in json.dumps(await hub.admin_overview())
        await hub.close()


async def test_system_module_status_is_non_blocking_and_hub_scheduler_lifecycle(
    settings, store, tmp_path: Path
) -> None:
    app_w, cfg_w, drive_client, tree, usage_state, _ = _build_test_worker(tmp_path)
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

        async def pre_cover(job: dict[str, Any]) -> dict[str, Any]:
            p = job["payload"]
            return _make_drive_dispatch_context(
                tree,
                inbox_file_id=str(p.get("inbox_file_id") or ""),
                inbox_name=str(p.get("inbox_filename") or "demo.m4a"),
            )

        hub.register_job_type(
            "rvc_cover",
            validator=validate_rvc_cover_payload,
            pre_dispatch=pre_cover,
            on_complete=on_cover,
        )

        # Upload source to Drive inbox directly (decoupled from colab_compute) -> dispatch -> done
        inbox_up = drive_client.upload_bytes(
            "inbox", "demo.m4a", b"FAKE_M4A", mime_type="audio/mp4"
        )
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
        downloaded = drive_client.download_bytes(
            hook_events[0]["result"]["drive_chorus_id"], allowed_roles=("songs",)
        )
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
    app_w, cfg_w, _, _, usage_state, _ = _build_test_worker(tmp_path)
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
        assert "GPU" in mod.description
        assert "翻唱" not in mod.description
        assert "RVC" not in mod.description
        assert "Hakimi_Songs" not in mod.description
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
                assert "drive_songs_folder_id" not in ov_json["config"]
                assert set(ov_json["supported_job_types"]) >= {"rvc_cover", "rvc_train"}
                assert ov_json["worker"]["reachable"] is True
                assert ov_json["worker"]["compute_units"] == 100.0
                assert "drive_folders" not in ov_json["worker"]
                assert ov_json["worker"]["supported_job_types"] == ["rvc_cover", "rvc_train"]
                assert cfg_w.token not in ov.text

                # Update & reset config; legacy drive_songs_folder_id rejected by extra="forbid"
                bad_put = await client.put(
                    "/admin/api/colab",
                    json={"drive_songs_folder_id": "drv_folder_0000000001"},
                )
                assert bad_put.status_code == 400

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

                # Unregistered & invalid job_type rejected with 400
                unreg = await client.post(
                    "/admin/api/colab/jobs",
                    json={"job_type": "unregistered_job", "payload": {}},
                )
                assert unreg.status_code == 400
                assert unreg.json()["detail"] == "colab_invalid_job"

                bad_name = await client.post(
                    "/admin/api/colab/jobs",
                    json={"job_type": "BAD-TYPE", "payload": {}},
                )
                assert bad_name.status_code == 400

                # Submit & cancel registered job (registered by song_studio)
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

                # Removed /admin/api/colab/drive/inbox routes MUST return 404
                assert (await client.get("/admin/api/colab/drive/inbox")).status_code == 404
                assert (
                    await client.post("/admin/api/colab/drive/inbox", content=b"x")
                ).status_code == 404

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


def test_colab_decoupling_static_ast_and_routes() -> None:
    targets = (
        REPO_ROOT / "src" / "hakimi" / "colab_compute.py",
        REPO_ROOT / "src" / "hakimi" / "colab_admin.py",
        REPO_ROOT / "src" / "hakimi" / "system_modules" / "colab_compute.py",
        REPO_ROOT / "scripts" / "colab_worker.py",
        REPO_ROOT / "scripts" / "colab_jobs" / "__init__.py",
        REPO_ROOT / "scripts" / "colab_jobs" / "rvc.py",
    )
    for path in targets:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert "songs" not in alias.name.split("."), path
                    assert "colab_drive" not in alias.name.split("."), path
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert "songs" not in mod.split("."), path
                assert "colab_drive" not in mod.split("."), path
        assert "/v1/drive/" not in src, path
        assert "/drive/inbox" not in src, path
        assert "colab_drive.py" not in src, path
        assert "roaming-oauth.json" not in src, path
        assert "colab-drive-oauth.json" not in src, path
        if path.name != "rvc.py":
            assert "drive_songs_folder_id" not in src, path

    worker_src = (REPO_ROOT / "scripts" / "colab_worker.py").read_text(encoding="utf-8")
    for extracted_symbol in (
        "class RvcCoverPayload",
        "class RvcTrainPayload",
        "def _run_cover_sync",
        "def _run_train_sync",
        "class EphemeralDriveClient",
    ):
        assert extracted_symbol not in worker_src


def test_rvc_cover_and_train_defaults_across_all_modules() -> None:
    import inspect

    # 1. Cover defaults: 0.15 / 0.42 / 0.55 and backing mix -14.0 / skip_backing=True
    w_cover = colab_jobs_rvc.RvcCoverPayload(title="test")
    assert (
        w_cover.index_rate,
        w_cover.protect,
        w_cover.volume_envelope,
        w_cover.backing_gain_db,
        w_cover.skip_backing,
    ) == (0.15, 0.42, 0.55, -14.0, True)

    h_cover = RvcCoverJobPayload(title="test")
    assert (
        h_cover.index_rate,
        h_cover.protect,
        h_cover.volume_envelope,
        h_cover.backing_gain_db,
        h_cover.skip_backing,
    ) == (0.15, 0.42, 0.55, -14.0, True)

    # Strict validation for backing_gain_db (finite float in [-24, -6], no bool) & skip_backing
    for model_cls in (colab_jobs_rvc.RvcCoverPayload, RvcCoverJobPayload):
        for bad_bg in (True, False, float("nan"), float("inf"), -24.5, -5.5):
            with pytest.raises(ValidationError):
                model_cls(title="test", backing_gain_db=bad_bg)  # type: ignore[arg-type]
        for bad_skip in (1, 0, "true"):
            with pytest.raises(ValidationError):
                model_cls(title="test", skip_backing=bad_skip)  # type: ignore[arg-type]

    sig_cov_rvc = inspect.signature(colab_rvc.run_cover_pipeline).parameters
    assert sig_cov_rvc["index_rate"].default == 0.15
    assert sig_cov_rvc["protect"].default == 0.42
    assert sig_cov_rvc["volume_envelope"].default == 0.55
    assert sig_cov_rvc["backing_gain_db"].default == -14.0
    assert sig_cov_rvc["skip_backing"].default is True

    sig_cov_rem = inspect.signature(colab_remote.action_cover).parameters
    assert sig_cov_rem["index_rate"].default == 0.15
    assert sig_cov_rem["protect"].default == 0.42
    assert sig_cov_rem["volume_envelope"].default == 0.55
    assert sig_cov_rem["backing_gain_db"].default == -14.0
    assert sig_cov_rem["skip_backing"].default is True

    rvc_cover_args = colab_rvc._build_parser().parse_args(
        ["cover", "--source", "s.mp3", "--title", "t"]
    )
    assert (
        rvc_cover_args.index_rate,
        rvc_cover_args.protect,
        rvc_cover_args.volume_envelope,
        rvc_cover_args.backing_gain_db,
        rvc_cover_args.skip_backing,
    ) == (0.15, 0.42, 0.55, -14.0, True)
    rem_cover_args = colab_remote._build_parser().parse_args(
        ["cover", "--source", "s.mp3", "--pth", "m.pth", "--index", "m.index"]
    )
    assert (
        rem_cover_args.index_rate,
        rem_cover_args.protect,
        rem_cover_args.volume_envelope,
        rem_cover_args.backing_gain_db,
        rem_cover_args.skip_backing,
    ) == (0.15, 0.42, 0.55, -14.0, True)

    # 2. Train defaults: 150 / 30 / 12 / 48000 & train CLI --gpu default
    w_train = colab_jobs_rvc.RvcTrainPayload()
    assert (w_train.epochs, w_train.save_every, w_train.batch_size, w_train.sample_rate) == (
        150,
        30,
        12,
        48000,
    )

    h_train = RvcTrainJobPayload()
    assert (h_train.epochs, h_train.save_every, h_train.batch_size, h_train.sample_rate) == (
        150,
        30,
        12,
        48000,
    )

    sig_tr_rvc = inspect.signature(colab_rvc.run_train_pipeline).parameters
    assert (
        sig_tr_rvc["epochs"].default,
        sig_tr_rvc["save_every"].default,
        sig_tr_rvc["batch_size"].default,
        sig_tr_rvc["sample_rate"].default,
    ) == (150, 30, 12, 48000)

    sig_tr_rem = inspect.signature(colab_remote.action_train).parameters
    assert (
        sig_tr_rem["epochs"].default,
        sig_tr_rem["save_every"].default,
        sig_tr_rem["batch_size"].default,
        sig_tr_rem["sample_rate"].default,
    ) == (150, 30, 12, 48000)

    rvc_train_args = colab_rvc._build_parser().parse_args(["train"])
    assert (
        rvc_train_args.epochs,
        rvc_train_args.save_every,
        rvc_train_args.batch_size,
        rvc_train_args.sample_rate,
        rvc_train_args.gpu,
    ) == (150, 30, 12, 48000, "H100,A100,L4")

    rem_train_args = colab_remote._build_parser().parse_args(["train"])
    assert (
        rem_train_args.epochs,
        rem_train_args.save_every,
        rem_train_args.batch_size,
        rem_train_args.sample_rate,
    ) == (150, 30, 12, 48000)

    # 3. Separation constants check
    assert colab_remote.PRIMARY_SEP_MODELS == (
        "model_bs_roformer_ep_317_sdr_12.9755.ckpt",
        "UVR-MDX-NET-Inst_HQ_3.onnx",
    )
    assert colab_remote.KARAOKE_SEP_MODELS == (
        "mel_band_roformer_karaoke_aufr33_viperx_sdr_10.1956.ckpt",
    )
    assert colab_remote.DEREVERB_MODELS == (
        "dereverb_mel_band_roformer_less_aggressive_anvuew_sdr_18.8050.ckpt",
        "dereverb_mel_band_roformer_anvuew_sdr_19.1729.ckpt",
        "UVR-DeEcho-DeReverb.pth",
    )
    assert colab_remote.DEREVERB_MODEL == colab_remote.DEREVERB_MODELS[0]


# ---------------------------------------------------------------------------
# 5. P0 Drive direct token, P1 warm session pool, P2 cache, & CU analytics
# ---------------------------------------------------------------------------


def test_colab_drive_short_lived_token_and_cache_folder_isolation(tmp_path: Path) -> None:
    cred_path = tmp_path / "secrets" / "colab-drive-oauth.json"
    _write_fake_oauth(cred_path, refresh_token="1//never-expose-refresh-token")
    token_calls = 0
    backend = FakeDriveBackend()

    def counting_transport(method, url, headers, body, timeout):
        nonlocal token_calls
        if url == colab_drive.TOKEN_URI:
            token_calls += 1
        return backend.transport(method, url, headers, body, timeout)

    client = colab_drive.ColabDriveClient(cred_path, transport=counting_transport)

    tok1 = client.get_short_lived_token()
    assert set(tok1) == {"access_token", "token_type", "expires_in", "expires_at"}
    assert tok1["access_token"] == "ya29.fake-access-token"
    assert tok1["token_type"] == "Bearer"
    assert tok1["expires_in"] > 3000
    assert "refresh_token" not in tok1
    assert "client_secret" not in tok1
    assert "never-expose-refresh-token" not in json.dumps(tok1)
    assert token_calls == 1

    # Cached in RAM while TTL healthy; force_refresh fetches anew
    tok2 = client.get_short_lived_token()
    assert tok2["access_token"] == tok1["access_token"]
    assert token_calls == 1
    client.get_short_lived_token(force_refresh=True)
    assert token_calls == 2

    # ensure_folder_tree keeps 4-key contract; ensure_cache_folder resolves cache/ lazily
    tree = client.ensure_folder_tree()
    assert set(tree.as_dict()) == {"root", "models", "songs", "inbox"}
    cache_fid = client.ensure_cache_folder()
    assert cache_fid and cache_fid not in tree.as_dict().values()
    assert client.ensure_cache_folder() == cache_fid

    # Model file ID resolution
    assert client.resolve_model_files("hakimi") is None
    client.upload_bytes("models", "hakimi.pth", b"PTH-BYTES")
    client.upload_bytes("models", "hakimi.index", b"IDX-BYTES")
    resolved_models = client.resolve_model_files("hakimi")
    assert resolved_models is not None
    assert resolved_models["pth"]["name"] == "hakimi.pth"
    assert resolved_models["index"]["name"] == "hakimi.index"

    # Cache manifest write + validation + out-of-scope rejection
    w_key = colab_remote.compute_weight_cache_key("cover")
    arch_up = client.upload_bytes("cache", f"{w_key}_12345678.tar", b"TAR-BYTES")
    sha_ok = "a" * 64
    client.write_cache_manifest(
        w_key,
        {
            "version_key": w_key,
            "kind": "weights",
            "archive_file_id": arch_up["file_id"],
            "sha256": sha_ok,
        },
    )
    hit = client.resolve_cache_manifest(w_key, expected_kind="weights")
    assert hit is not None
    assert hit["archive_file_id"] == arch_up["file_id"]
    assert hit["sha256"] == sha_ok
    assert client.resolve_cache_manifest(w_key, expected_kind="venv") is None

    # Manifest pointing to outside_secret_file MUST be rejected (returns None)
    client.write_cache_manifest(
        "weights_evil_1234",
        {
            "version_key": "weights_evil_1234",
            "kind": "weights",
            "archive_file_id": "outside_secret_file",
            "sha256": sha_ok,
        },
    )
    assert client.resolve_cache_manifest("weights_evil_1234", expected_kind="weights") is None


async def test_worker_warm_pool_limits_reuse_probe_fallback_expiry_and_shutdown(
    tmp_path: Path,
) -> None:
    # 1) Hard GPU & TTL policy enforcement on WorkerConfig and WarmSessionPool
    cfg_clamped = colab_worker.WorkerConfig(
        token="worker-secret-token-123456",
        warm_allowed_gpus=("H100", "A100", "G4", "L4", "T4"),
        warm_idle_ttl_seconds=86400,  # 24h attempt must be clamped to 900s!
    )
    assert cfg_clamped.warm_allowed_gpus == ("L4", "T4")
    assert cfg_clamped.warm_idle_ttl_seconds == colab_worker.MAX_WARM_IDLE_TTL_SECONDS
    assert cfg_clamped.warm_pool_enabled is True

    cfg_off = colab_worker.WorkerConfig(
        token="worker-secret-token-123456",
        warm_idle_ttl_seconds=0,
    )
    assert cfg_off.warm_pool_enabled is False

    cfg_only_banned = colab_worker.WorkerConfig(
        token="worker-secret-token-123456",
        warm_allowed_gpus=("H100", "A100", "G4"),
    )
    assert cfg_only_banned.warm_allowed_gpus == ()
    assert cfg_only_banned.warm_pool_enabled is False

    pool = colab_worker.WarmSessionPool(max_sessions=1)
    for banned in ("H100", "A100", "G4"):
        refused = pool.put(
            colab_worker.WarmSessionEntry(
                session_name=f"bad-{banned}",
                gpu=banned,
                idle_ttl_seconds=360,
                created_at=1000.0,
                last_used_at=1000.0,
                expires_at=1360.0,
            )
        )
        assert len(refused) == 1 and refused[0].gpu == banned
    assert pool.snapshot(now=1000.0) == []

    # 2) End-to-end Worker warm reuse + Drive direct + cache weights + probe fallback + shutdown
    cred = tmp_path / "secrets" / "drive.json"
    _write_fake_oauth(cred)
    backend = FakeDriveBackend()
    drive_client = colab_drive.ColabDriveClient(cred, transport=backend.transport)
    tree = drive_client.ensure_folder_tree()
    cache_fid = drive_client.ensure_cache_folder()
    pth_up = drive_client.upload_bytes("models", "hakimi.pth", b"DRV-PTH")
    idx_up = drive_client.upload_bytes("models", "hakimi.index", b"DRV-IDX")
    inbox_up = drive_client.upload_bytes("inbox", "song.m4a", b"RAW-M4A", mime_type="audio/mp4")
    w_key = colab_remote.compute_weight_cache_key("cover")
    cache_arch = drive_client.upload_bytes("cache", f"{w_key}_1234.tar", b"WEIGHT-TAR")
    cache_sha = "a" * 64

    cover_ctx = _make_drive_dispatch_context(
        tree,
        cache_folder_id=cache_fid,
        inbox_file_id=inbox_up["file_id"],
        inbox_name="song.m4a",
        pth_file_id=pth_up["file_id"],
        index_file_id=idx_up["file_id"],
        cache_weights_file_id=cache_arch["file_id"],
        cache_weights_sha256=cache_sha,
    )
    train_ctx = _make_drive_dispatch_context(
        tree,
        cache_folder_id=cache_fid,
        cache_weights_file_id=cache_arch["file_id"],
        cache_weights_sha256=cache_sha,
    )

    clock_now = [1_700_000_000.0]
    stopped_sessions: list[str] = []
    probe_state = {"healthy": True, "setup_ready": True}
    usage_state = {"cu": 50.0, "active": 0}
    runner_calls: list[dict[str, Any]] = []
    train_calls: list[dict[str, Any]] = []

    def fake_cover_runner(**kwargs):
        runner_calls.append(dict(kwargs))
        # Verify short-lived access token + 3-tier cache metadata passed in RAM & never leaked
        assert kwargs["drive_access_token"] == "ya29.fake-access-token"
        assert kwargs["drive_source_file_id"] == inbox_up["file_id"]
        assert kwargs["drive_pth_file_id"] == pth_up["file_id"]
        assert kwargs["drive_index_file_id"] == idx_up["file_id"]
        assert kwargs["drive_songs_folder_id"] == tree.songs
        assert kwargs["cache_folder_id"] == cache_fid
        assert kwargs["cache_weights_file_id"] == cache_arch["file_id"]
        assert kwargs["cache_weights_sha256"] == cache_sha
        assigned = kwargs.get("reused_gpu") or kwargs["gpu_spec"].split(",")[0]
        return {
            "assigned_gpu": assigned,
            "chorus_mp3": None,
            "full_mp3": None,
            "meta": {"chorus_start_sec": 42.0, "chorus_start_reason": "dsp_vocal_rms"},
            "drive_outputs": {
                "chorus": {
                    "file_id": "drv_direct_chorus_1",
                    "name": "c.mp3",
                    "sha256": "d" * 64,
                    "size": 98765,
                    "duration_ms": 50000,
                },
                "full": {"file_id": "drv_direct_full_1", "name": "f.mp3"},
            },
            "session_kept": True,
            "acceleration": {
                "drive_direct": True,
                "drive_direct_fallback": False,
                "warm_reused": bool(kwargs.get("reuse_session")),
                "weight_cache_hit": True,
                "weight_cache_written": False,
                "venv_cache_hit": False,
                "venv_cache_written": False,
            },
        }

    def fake_train_runner(**kwargs):
        train_calls.append(dict(kwargs))
        assert kwargs["drive_access_token"] == "ya29.fake-access-token"
        assert kwargs["drive_models_folder_id"] == tree.models
        assert kwargs["cache_folder_id"] == cache_fid
        assert kwargs["cache_weights_file_id"] == cache_arch["file_id"]
        return {
            "assigned_gpu": kwargs["gpu_spec"].split(",")[0],
            "pth_path": None,
            "index_path": None,
            "drive_outputs": {
                "pth": {"file_id": "drv_tr_pth", "sha256": "e" * 64},
                "index": {"file_id": "drv_tr_idx", "sha256": "f" * 64},
            },
        }

    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "dataset.tar.gz").write_bytes(b"tar")

    cfg = colab_worker.WorkerConfig(
        token="worker-secret-token-123456",
        model_dir=tmp_path / "models",
        dataset_dir=dataset_dir,
        work_root=tmp_path / "work",
        warm_pool_enabled=True,
        warm_idle_ttl_seconds=360,
        warm_min_cu_balance=2.0,
    )
    app = colab_worker.create_worker_app(
        cfg,
        drive_transport=backend.transport,
        usage_fn=lambda: {
            "reachable": True,
            "compute_units": usage_state["cu"],
            "usage_rate": 0.0,
            "active_sessions": usage_state["active"],
            "sessions": [],
            "error": None,
        },
        stop_all_fn=lambda extra=None: {
            "terminated_sessions": len(extra or []),
            "sessions": list(extra or []),
        },
        stop_session_fn=stopped_sessions.append,
        probe_session_fn=lambda _s: dict(probe_state),
        cover_runner=fake_cover_runner,
        train_runner=fake_train_runner,
        clock=lambda: clock_now[0],
    )
    headers = {"Authorization": f"Bearer {cfg.token}"}

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12146",
        ) as client:
            # Cover 1 (L4): cold boot -> parked in warm pool
            j1 = "1" * 32
            r1 = await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j1,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4", "T4"],
                    "payload": {"title": "暖机测试1", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            assert r1.status_code == 202
            if (t1 := app.state.worker_state.tasks.get(j1)) is not None:
                await t1
            st1 = (await client.get(f"/v1/jobs/{j1}", headers=headers)).json()
            assert st1["status"] == "succeeded"
            assert st1["result"]["drive_chorus_id"] == "drv_direct_chorus_1"
            assert st1["result"]["acceleration"]["drive_direct"] is True
            assert st1["result"]["acceleration"]["warm_reused"] is False
            assert "ya29.fake-access-token" not in json.dumps(st1)

            # Warm session exposed on /health, /healthz, and /v1/status (without tokens)
            h_resp = await client.get("/health")
            assert h_resp.status_code == 200
            h_json = h_resp.json()
            assert h_json["ok"] is True
            assert h_json["warm_active_count"] == 1
            assert h_json["warm_sessions"][0]["gpu"] == "L4"
            assert h_json["warm_sessions"][0]["reuse_count"] == 0
            assert "ya29.fake-access-token" not in h_resp.text
            assert (await client.get("/healthz")).json()["warm_active_count"] == 1

            # Cover 2 (L4): probes healthy -> reuses exact warm session!
            clock_now[0] += 60.0
            j2 = "2" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j2,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4", "T4"],
                    "payload": {"title": "暖机测试2", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t2 := app.state.worker_state.tasks.get(j2)) is not None:
                await t2
            st2 = (await client.get(f"/v1/jobs/{j2}", headers=headers)).json()
            assert st2["status"] == "succeeded"
            assert st2["result"]["acceleration"]["warm_reused"] is True
            assert runner_calls[1]["reuse_session"] is True
            assert runner_calls[1]["session"] == runner_calls[0]["session"]
            h2 = (await client.get("/health")).json()
            assert h2["warm_sessions"][0]["reuse_count"] == 1
            assert stopped_sessions == []

            # Cover 3 (L4) when probe FAILS -> stops broken warm session & cold-boots cleanly!
            probe_state["healthy"] = False
            j3 = "3" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j3,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {"title": "探活失败回退", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t3 := app.state.worker_state.tasks.get(j3)) is not None:
                await t3
            assert runner_calls[0]["session"] in stopped_sessions
            assert runner_calls[2]["reuse_session"] is False

            # High-GPU (H100) cover -> Worker refuses to park & stops session immediately!
            probe_state["healthy"] = True
            j4 = "4" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j4,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["H100"],
                    "payload": {"title": "高价卡禁保活", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t4 := app.state.worker_state.tasks.get(j4)) is not None:
                await t4
            assert runner_calls[3]["session"] in stopped_sessions
            assert (await client.get("/health")).json()["warm_active_count"] == 0

            # Park L4 again, then advance clock past 360s TTL -> reaper automatically stops it!
            j5 = "5" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j5,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {"title": "到期回收", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t5 := app.state.worker_state.tasks.get(j5)) is not None:
                await t5
            assert (await client.get("/health")).json()["warm_active_count"] == 1
            clock_now[0] += 400.0
            reaped = await app.state.worker_state.reap_expired_warm_sessions()
            assert reaped == 1
            assert runner_calls[4]["session"] in stopped_sessions
            assert (await client.get("/health")).json()["warm_active_count"] == 0

            # Park L4 again, then rvc_train drains & stops it immediately (never keeps train warm)
            j6 = "6" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j6,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {"title": "训练前排空", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t6 := app.state.worker_state.tasks.get(j6)) is not None:
                await t6
            j_tr = "7" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j_tr,
                    "job_type": "rvc_train",
                    "gpu_candidates": ["H100"],
                    "payload": {"model_name": "hakimi"},
                    "dispatch_context": train_ctx,
                },
            )
            if (t_tr := app.state.worker_state.tasks.get(j_tr)) is not None:
                await t_tr
            assert len(train_calls) == 1
            assert runner_calls[5]["session"] in stopped_sessions
            assert (await client.get("/health")).json()["warm_active_count"] == 0

            # Budget guard 1: allow_warm_keepalive=False refuses to park & stops session
            j_nobudget = "9" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j_nobudget,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "allow_warm_keepalive": False,
                    "payload": {"title": "预算不足禁保活", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t_nb := app.state.worker_state.tasks.get(j_nobudget)) is not None:
                await t_nb
            assert runner_calls[6]["session"] in stopped_sessions
            assert (await client.get("/health")).json()["warm_active_count"] == 0

            # Budget guard 2: CU balance below warm_min_cu_balance refuses to park & stops
            usage_state["cu"] = 0.5
            j_lowcu = "a" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j_lowcu,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {"title": "余额偏低禁保活", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t_lc := app.state.worker_state.tasks.get(j_lowcu)) is not None:
                await t_lc
            assert runner_calls[7]["session"] in stopped_sessions
            assert (await client.get("/health")).json()["warm_active_count"] == 0
            usage_state["cu"] = 50.0

            # Park one final L4 session and verify lifespan exit cleans it up!
            j7 = "8" * 32
            await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": j7,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {"title": "退出清理", "inbox_file_id": inbox_up["file_id"]},
                    "dispatch_context": cover_ctx,
                },
            )
            if (t7 := app.state.worker_state.tasks.get(j7)) is not None:
                await t7
            final_warm_sess = runner_calls[8]["session"]
            assert (await client.get("/health")).json()["warm_active_count"] == 1

    # Lifespan exited -> graceful shutdown MUST have stopped final_warm_sess
    assert final_warm_sess in stopped_sessions
    assert app.state.worker_state.warm_pool.snapshot(now=clock_now[0]) == []


async def test_colab_compute_cu_trend_gpu_breakdown_warm_status_and_token_scrub(
    settings, store, tmp_path: Path
) -> None:
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    clock_now = [1_700_000_000.0]
    tz = ZoneInfo(settings.bot_timezone)
    today_date = datetime.fromtimestamp(clock_now[0], tz).date()

    warm_status_mode = {"legacy": False}

    def fake_worker_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/status":
            if warm_status_mode["legacy"]:
                return httpx.Response(
                    200,
                    json={
                        "reachable": True,
                        "compute_units": 88.5,
                        "active_sessions": 0,
                        "running_job_id": None,
                        "error": "token leak ya29.secret-abc123456 Bearer supersecret999",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "reachable": True,
                    "compute_units": 88.5,
                    "active_sessions": 1,
                    "running_job_id": None,
                    "supported_job_types": ["rvc_cover", "rvc_train"],
                    "warm_sessions": [
                        {
                            "session_name": "hakimi-warm-l4",
                            "gpu": "L4",
                            "idle_ttl_seconds": 360,
                            "remaining_ttl_seconds": 240,
                            "created_at": clock_now[0] - 120.0,
                            "last_used_at": clock_now[0] - 120.0,
                            "expires_at": clock_now[0] + 240.0,
                            "reuse_count": 2,
                            "setup_ready": True,
                        }
                    ],
                    "warm_pool": {
                        "enabled": True,
                        "allowed_gpus": ["L4", "T4"],
                        "idle_ttl_seconds": 360,
                        "max_ttl_seconds": 900,
                        "active_count": 1,
                    },
                    "error": None,
                },
            )
        if request.url.path == "/v1/jobs" and request.method == "POST":
            body = json.loads(request.content.decode("utf-8"))
            dispatched_bodies.append(body)
            return httpx.Response(202, json={"job_id": body["job_id"], "status": "running"})
        return httpx.Response(404)

    dispatched_bodies: list[dict[str, Any]] = []
    s = _worker_settings(settings, "http://127.0.0.1:12146", "worker-secret-token-123456")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(fake_worker_handler),
        base_url="http://127.0.0.1:12146",
    ) as mock_http:
        hub = ColabComputeHub(s, store, None, http=mock_http, clock=lambda: clock_now[0])
        await hub.initialize()
        hub.register_job_type("rvc_cover", validator=validate_rvc_cover_payload)
        hub.register_job_type("rvc_train", validator=validate_rvc_train_payload)

        # 1) Empty DB overview
        empty_ov = await hub.admin_overview()
        assert len(empty_ov["cu_trend"]) == 30
        assert empty_ov["cu_trend"][-1]["date"] == today_date.isoformat()
        assert empty_ov["cu_trend"][0]["date"] == (today_date - timedelta(days=29)).isoformat()
        assert all(d["cu_used"] == 0.0 and d["jobs"] == 0 for d in empty_ov["cu_trend"])
        assert empty_ov["summary"]["success_rate"] is None
        assert empty_ov["gpu_breakdown"] == []
        assert empty_ov["acceleration_stats"]["measured_jobs"] == 0
        assert empty_ov["acceleration_stats"]["warm_pool_enabled"] is True
        assert len(empty_ov["warm_sessions"]) == 1
        assert empty_ov["worker"]["warm_sessions"] == empty_ov["warm_sessions"]

        # Non-blocking status_summary reflects live warm sessions and drops them after expiry
        st_warm = await hub.status_summary()
        assert st_warm["warm_active_count"] == 1
        assert "L4" in st_warm["detail"]
        clock_now[0] += 300.0  # past expires_at
        st_expired = await hub.status_summary()
        assert st_expired["warm_active_count"] == 0
        clock_now[0] -= 300.0

        # 2) Seed multi-day persisted jobs (including legacy job without acceleration & -45d job)
        async def _seed_job(
            jid: str,
            day_offset: int,
            status: str,
            pref_gpu: str,
            assigned_gpu: str | None,
            cu: float,
            *,
            started: bool = True,
            result: dict[str, Any] | None = None,
        ) -> None:
            ts = clock_now[0] - day_offset * 86400.0
            day_s = (today_date - timedelta(days=day_offset)).isoformat()
            sealed_p = hub._seal_blob(jid, "payload", {"title": jid})
            sealed_r = hub._seal_blob(jid, "result", result) if result is not None else None
            async with store.transaction():
                await store.db.execute(
                    "INSERT INTO colab_jobs ("
                    "id, job_type, status, requester, preferred_gpu, assigned_gpu, "
                    "payload, result, error_code, error, cu_used, created_at, "
                    "started_at, finished_at, updated_at, day"
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        jid,
                        "rvc_cover",
                        status,
                        "owner",
                        pref_gpu,
                        assigned_gpu,
                        sealed_p,
                        sealed_r,
                        None,
                        None,
                        cu,
                        ts,
                        ts if started else None,
                        ts + 60.0 if status in {"succeeded", "failed", "cancelled"} else None,
                        ts + 60.0,
                        day_s,
                    ),
                )

        # Today: L4 succeeded (direct + warm + weight hit)
        await _seed_job(
            "a" * 32,
            0,
            "succeeded",
            "L4",
            "L4",
            1.25,
            result={
                "title": "today_ok",
                "acceleration": {
                    "drive_direct": True,
                    "drive_direct_fallback": False,
                    "warm_reused": True,
                    "weight_cache_hit": True,
                    "venv_cache_hit": True,
                },
            },
        )
        # -5d: T4 failed (direct fallback + cold + weight miss + venv miss)
        await _seed_job(
            "b" * 32,
            5,
            "failed",
            "L4",
            "T4",
            0.5,
            result={
                "error_code": "colab_worker_error",
                "acceleration": {
                    "drive_direct": False,
                    "drive_direct_fallback": True,
                    "warm_reused": False,
                    "weight_cache_hit": False,
                    "venv_cache_hit": False,
                },
            },
        )
        # -20d: H100 succeeded (legacy job without acceleration -> skipped in measured_jobs)
        await _seed_job("c" * 32, 20, "succeeded", "H100", "H100", 4.0, result={"title": "legacy"})
        # -2d: Unstarted cancelled job -> excluded from gpu_breakdown
        await _seed_job("d" * 32, 2, "cancelled", "A100", None, 0.0, started=False)
        # -45d: Outside 30-day trend window, included in lifetime totals
        await _seed_job("e" * 32, 45, "succeeded", "L4", "L4", 2.0, result={"title": "old"})

        ov = await hub.admin_overview()
        assert ov["summary"]["total_jobs"] == 5
        assert ov["summary"]["succeeded_jobs"] == 3
        assert ov["summary"]["failed_jobs"] == 1
        assert ov["summary"]["cancelled_jobs"] == 1
        assert ov["summary"]["success_rate"] == 0.75
        assert ov["summary"]["total_cu_used"] == 7.75
        assert ov["summary"]["last_14d_cu_used"] == 1.75
        assert ov["summary"]["last_30d_cu_used"] == 5.75
        assert ov["summary"]["last_14d_jobs"] == 3  # today + -5d + -2d cancelled
        assert ov["summary"]["last_30d_jobs"] == 4

        by_date = {d["date"]: d for d in ov["cu_trend"]}
        assert by_date[today_date.isoformat()]["cu_used"] == 1.25
        assert by_date[(today_date - timedelta(days=5)).isoformat()]["failed"] == 1
        assert by_date[(today_date - timedelta(days=20)).isoformat()]["cu_used"] == 4.0
        assert (today_date - timedelta(days=45)).isoformat() not in by_date

        # GPU breakdown sorted by cu_used DESC: H100 (4.0) > L4 (3.25) > T4 (0.5); A100 excluded!
        assert [g["gpu"] for g in ov["gpu_breakdown"]] == ["H100", "L4", "T4"]
        assert ov["gpu_breakdown"][0]["cu_share"] == round(4.0 / 7.75, 4)
        assert ov["gpu_breakdown"][1]["jobs"] == 2
        assert ov["gpu_breakdown"][1]["cu_used"] == 3.25

        acc = ov["acceleration_stats"]
        assert acc["measured_jobs"] == 2
        assert acc["drive_direct_jobs"] == 1
        assert acc["drive_direct_fallbacks"] == 1
        assert acc["warm_reuse_hits"] == 1
        assert acc["warm_cold_starts"] == 1
        assert acc["cache_weight_hits"] == 1
        assert acc["cache_weight_misses"] == 1
        assert acc["cache_venv_hits"] == 1
        assert acc["cache_venv_misses"] == 1

        # 3) Legacy worker response without warm_sessions/warm_pool + token scrubbing
        warm_status_mode["legacy"] = True
        legacy_ov = await hub.admin_overview()
        assert legacy_ov["warm_sessions"] == []
        assert legacy_ov["worker"]["warm_sessions"] == []
        assert "ya29.secret-abc123456" not in str(legacy_ov["worker"]["error"])
        assert "supersecret999" not in str(legacy_ov["worker"]["error"])
        scrubbed = hub._scrub("err ya29.abc1234567 1//refresh99999 Bearer tok12345678")
        assert "ya29.abc1234567" not in scrubbed
        assert "1//refresh99999" not in scrubbed
        assert "tok12345678" not in scrubbed

        # 4) Low remaining daily CU budget (<0.5) disables allow_warm_keepalive on dispatch
        await hub.set_config(ColabConfig(daily_job_limit=10, daily_cu_budget=1.5))
        await hub.submit_job("rvc_cover", {"title": "低预算单", "auto_search": True})
        await hub.scheduler_step()
        assert len(dispatched_bodies) == 1
        assert dispatched_bodies[0]["allow_warm_keepalive"] is False
        assert "drive_songs_folder_id" not in dispatched_bodies[0]
        await hub.close()


async def test_worker_status_error_scrubs_secrets_and_cancel_removes_warm_entry(
    tmp_path: Path,
) -> None:
    stopped: list[str] = []
    secret_worker_tok = "worker-secret-token-654321"
    ephem_tok = "custom_ephemeral_token_987654"

    def crashing_cover(**kwargs):
        del kwargs
        raise RuntimeError(
            f"boom tok={secret_worker_tok} ephem={ephem_tok} "
            "ya29.crash-token-999 1//refresh-leak-888 Bearer crashbearer777"
        )

    model_dir = tmp_path / "models"
    (model_dir / "hakimi").mkdir(parents=True, exist_ok=True)
    (model_dir / "hakimi" / "hakimi.pth").write_bytes(b"p")
    (model_dir / "hakimi" / "hakimi.index").write_bytes(b"i")

    cfg = colab_worker.WorkerConfig(
        token=secret_worker_tok,
        model_dir=model_dir,
        dataset_dir=tmp_path / "dataset",
        work_root=tmp_path / "work",
    )
    app = colab_worker.create_worker_app(
        cfg,
        usage_fn=lambda: {
            "reachable": False,
            "compute_units": None,
            "usage_rate": None,
            "active_sessions": 0,
            "sessions": [],
            "error": (
                f"auth failed tok={secret_worker_tok} ya29.leak-oauth-123456 "
                "1//refresh-leak-123 Bearer secretbearer999"
            ),
        },
        stop_session_fn=stopped.append,
        cover_runner=crashing_cover,
        clean_search_fn=lambda _q, out_dir, artist="": (
            (out_dir / "src.m4a").write_bytes(b"m4a") or (out_dir / "src.m4a")
        ),
    )
    headers = {"Authorization": f"Bearer {secret_worker_tok}"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://127.0.0.1:12146",
    ) as client:
        st = await client.get("/v1/status", headers=headers)
        assert st.status_code == 200
        err_msg = str(st.json()["error"])
        assert secret_worker_tok not in err_msg
        assert "ya29.leak-oauth-123456" not in err_msg
        assert "1//refresh-leak-123" not in err_msg
        assert "secretbearer999" not in err_msg
        assert "[REDACTED]" in err_msg

        # Failed job error MUST scrub ephemeral token, OAuth/refresh, Bearer & worker token
        err_jid = "e" * 32
        sub_err = await client.post(
            "/v1/jobs",
            headers=headers,
            json={
                "job_id": err_jid,
                "job_type": "rvc_cover",
                "payload": {"title": "脱敏测试", "auto_search": True},
                "dispatch_context": {
                    "drive": {
                        "access_token": ephem_tok,
                        "folders": {"songs": "drv_songs_0001"},
                        "files": {},
                    }
                },
            },
        )
        assert sub_err.status_code == 202
        if (err_task := app.state.worker_state.tasks.get(err_jid)) is not None:
            await err_task
        job_err_resp = (await client.get(f"/v1/jobs/{err_jid}", headers=headers)).json()
        assert job_err_resp["status"] == "failed"
        dumped = json.dumps(job_err_resp)
        for leaked in (
            secret_worker_tok,
            ephem_tok,
            "ya29.crash-token-999",
            "1//refresh-leak-888",
            "crashbearer777",
        ):
            assert leaked not in dumped

        # Cancelling a job whose session_name is parked in warm_pool must evict the handle
        state = app.state.worker_state
        sess_name = "hakimi-warm-cancel-1"
        state.warm_pool.put(
            colab_worker.WarmSessionEntry(
                session_name=sess_name,
                gpu="L4",
                idle_ttl_seconds=360,
                created_at=1000.0,
                last_used_at=1000.0,
                expires_at=9_999_999_999.0,
            )
        )
        jid = "f" * 32
        state.jobs[jid] = colab_worker.WorkerJobRecord(
            job_id=jid,
            job_type="rvc_cover",
            status="running",
            gpu_candidates=["L4"],
            session_name=sess_name,
        )
        state.running_job_id = jid
        res = await client.post(f"/v1/jobs/{jid}/cancel", headers=headers)
        assert res.status_code == 200
        assert sess_name in stopped
        assert state.warm_pool.snapshot() == []


async def test_colab_jobs_dynamic_handler_registry_rvc_mixing_and_cli_help(
    tmp_path: Path,
) -> None:
    from pydantic import BaseModel, ConfigDict, Field

    class _DemoWhisperPayload(BaseModel):
        model_config = ConfigDict(extra="forbid")

        clip_id: str = Field(min_length=1, max_length=64)
        language: str = Field(default="auto", max_length=16)
        fail_with_secret: bool = False

    class _DemoCustomError(RuntimeError):
        def __init__(self, code: str, msg: str) -> None:
            self.code = code
            super().__init__(msg)

    seen_contexts: list[dict[str, Any]] = []

    class _DemoWhisperHandler(colab_jobs.JobHandler):
        job_type = "demo_whisper"
        payload_model = _DemoWhisperPayload
        allow_warm_keepalive = True
        allowed_error_codes = frozenset({"custom_audio_error"})

        def run_sync(
            self,
            ctx: colab_jobs.WorkerJobContext,
            parsed_payload: _DemoWhisperPayload,
        ) -> tuple[dict[str, Any], str]:
            has_drive = ctx.dispatch_context is not None and ctx.dispatch_context.drive is not None
            seen_contexts.append(
                {
                    "job_id": ctx.job_id,
                    "clip_id": parsed_payload.clip_id,
                    "has_drive": has_drive,
                    "can_keep_l4": ctx.can_keep_warm("L4"),
                    "can_keep_h100": ctx.can_keep_warm("H100"),
                }
            )
            if parsed_payload.fail_with_secret:
                tok = (
                    ctx.dispatch_context.drive.access_token
                    if (ctx.dispatch_context and ctx.dispatch_context.drive)
                    else ""
                )
                raise _DemoCustomError(
                    "custom_audio_error",
                    f"decode failed tok={tok} ya29.demo-secret-123 "
                    "1//demo-refresh-456 Bearer demo999999",
                )
            return {
                "clip_id": parsed_payload.clip_id,
                "transcript": f"ok:{parsed_payload.language}",
            }, ctx.gpu_candidates[0]

    captured_cover_kwargs: list[dict[str, Any]] = []

    def tracking_cover_runner(**kwargs):
        captured_cover_kwargs.append(dict(kwargs))
        c_dir = Path(kwargs["cover_dir"])
        c_dir.mkdir(parents=True, exist_ok=True)
        chorus = c_dir / "cover_chorus.mp3"
        full = c_dir / "cover_full.mp3"
        chorus.write_bytes(_fake_mp3_bytes(b"mix-chorus"))
        full.write_bytes(_fake_mp3_bytes(b"mix-full"))
        return {
            "assigned_gpu": "L4",
            "chorus_mp3": chorus,
            "full_mp3": full,
            "meta": {"chorus_start_sec": 45.0, "chorus_duration_sec": 50.0},
        }

    # 1) Dynamic plugin handler registration + duplicate rejection + end-to-end dispatch
    handler = _DemoWhisperHandler()
    colab_jobs.register_job_handler(handler)
    try:
        with pytest.raises(ValueError, match="already registered"):
            colab_jobs.register_job_handler(handler)
        assert colab_jobs.get_job_handler("demo_whisper") is handler
        assert "demo_whisper" in colab_jobs.registered_job_types()

        app, cfg, drive_client, tree, _, _ = _build_test_worker(
            tmp_path, cover_runner=tracking_cover_runner
        )
        headers = {"Authorization": f"Bearer {cfg.token}"}
        ephem_tok = "opaque_custom_handler_tok_777888"
        inbox_up = drive_client.upload_bytes(
            "inbox", "shidarezakura.m4a", b"RAW_M4A", mime_type="audio/mp4"
        )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12146",
        ) as client:
            st = (await client.get("/v1/status", headers=headers)).json()
            assert st["supported_job_types"] == ["demo_whisper", "rvc_cover", "rvc_train"]

            # Invalid payload for custom handler rejected with 400
            bad = await client.post(
                "/v1/jobs",
                headers=headers,
                json={"job_id": "1" * 32, "job_type": "demo_whisper", "payload": {"bad": 1}},
            )
            assert bad.status_code == 400
            assert bad.json()["detail"] == "colab_invalid_job"

            # Valid custom handler job succeeds without leaking dispatch_context or tokens
            ok_jid = "2" * 32
            sub_ok = await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": ok_jid,
                    "job_type": "demo_whisper",
                    "gpu_candidates": ["L4"],
                    "payload": {"clip_id": "clip-01", "language": "ja"},
                    "dispatch_context": _make_drive_dispatch_context(tree, access_token=ephem_tok),
                },
            )
            assert sub_ok.status_code == 202
            if (t_ok := app.state.worker_state.tasks.get(ok_jid)) is not None:
                await t_ok
            rec_ok = app.state.worker_state.jobs[ok_jid]
            assert not hasattr(rec_ok, "dispatch_context")
            job_ok = (await client.get(f"/v1/jobs/{ok_jid}", headers=headers)).json()
            assert job_ok["status"] == "succeeded"
            assert job_ok["assigned_gpu"] == "L4"
            assert job_ok["result"] == {"clip_id": "clip-01", "transcript": "ok:ja"}
            assert "dispatch_context" not in job_ok
            assert ephem_tok not in json.dumps(job_ok)
            assert seen_contexts[-1] == {
                "job_id": ok_jid,
                "clip_id": "clip-01",
                "has_drive": True,
                "can_keep_l4": True,
                "can_keep_h100": False,
            }

            # Custom handler allowed error_code + full token scrubbing on failure
            fail_jid = "3" * 32
            sub_fail = await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": fail_jid,
                    "job_type": "demo_whisper",
                    "gpu_candidates": ["L4"],
                    "payload": {"clip_id": "clip-02", "fail_with_secret": True},
                    "dispatch_context": _make_drive_dispatch_context(tree, access_token=ephem_tok),
                },
            )
            assert sub_fail.status_code == 202
            if (t_fail := app.state.worker_state.tasks.get(fail_jid)) is not None:
                await t_fail
            job_fail = (await client.get(f"/v1/jobs/{fail_jid}", headers=headers)).json()
            assert job_fail["status"] == "failed"
            assert job_fail["error_code"] == "custom_audio_error"
            dumped_fail = json.dumps(job_fail)
            for secret_val in (
                ephem_tok,
                "ya29.demo-secret-123",
                "1//demo-refresh-456",
                "demo999999",
                cfg.token,
            ):
                assert secret_val not in dumped_fail

            # 3) RVC cover mixing params pass through worker -> RvcCoverHandler -> cover_runner
            mix_jid = "4" * 32
            sub_mix = await client.post(
                "/v1/jobs",
                headers=headers,
                json={
                    "job_id": mix_jid,
                    "job_type": "rvc_cover",
                    "gpu_candidates": ["L4"],
                    "payload": {
                        "title": "枝垂桜",
                        "inbox_file_id": inbox_up["file_id"],
                        "backing_gain_db": -16.5,
                        "skip_backing": True,
                    },
                    "dispatch_context": _make_drive_dispatch_context(
                        tree,
                        inbox_file_id=inbox_up["file_id"],
                        inbox_name="shidarezakura.m4a",
                    ),
                },
            )
            assert sub_mix.status_code == 202
            if (t_mix := app.state.worker_state.tasks.get(mix_jid)) is not None:
                await t_mix
            job_mix = (await client.get(f"/v1/jobs/{mix_jid}", headers=headers)).json()
            assert job_mix["status"] == "succeeded"
            assert captured_cover_kwargs[-1]["backing_gain_db"] == -16.5
            assert captured_cover_kwargs[-1]["skip_backing"] is True
    finally:
        colab_jobs.unregister_job_handler("demo_whisper")
        assert colab_jobs.get_job_handler("demo_whisper") is None


def test_colab_worker_cli_help_standalone() -> None:
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "colab_worker.py"), "--help"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15.0,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--host" in proc.stdout and "--port" in proc.stdout


async def test_song_library_request_learn_song_mixing_params(store) -> None:
    from hakimi.songs import SongLibrary

    submitted: list[tuple[str, dict[str, Any]]] = []

    class _StubCompute:
        def configured(self) -> bool:
            return True

        async def submit_job(
            self, job_type: str, payload: dict[str, Any], *, requester: str = "owner"
        ) -> dict[str, Any]:
            del requester
            validated = validate_rvc_cover_payload(payload)
            submitted.append((job_type, validated))
            return {"id": "a" * 32, "job_type": job_type, "payload": validated}

    lib = SongLibrary(store, compute=_StubCompute())
    await lib.initialize()

    # Default call remains backward-compatible and normalizes defaults via RvcCoverJobPayload
    await lib.request_learn_song("枝垂桜", auto_search=True)
    assert submitted[-1][1]["backing_gain_db"] == -14.0
    assert submitted[-1][1]["skip_backing"] is True

    # Explicit mixing overrides pass through cleanly
    await lib.request_learn_song(
        "枝垂桜",
        auto_search=True,
        backing_gain_db=-16.0,
        skip_backing=False,
    )
    assert submitted[-1][1]["backing_gain_db"] == -16.0
    assert submitted[-1][1]["skip_backing"] is False

    # Invalid mixing parameters are strictly rejected
    for bad_bg in (True, float("nan"), float("inf"), -25.0, -5.0):
        with pytest.raises(Rejected, match="colab_invalid_job"):
            await lib.request_learn_song(
                "枝垂桜",
                auto_search=True,
                backing_gain_db=bad_bg,  # type: ignore[arg-type]
            )
    with pytest.raises(Rejected, match="colab_invalid_job"):
        await lib.request_learn_song(
            "枝垂桜",
            auto_search=True,
            skip_backing="1",  # type: ignore[arg-type]
        )
