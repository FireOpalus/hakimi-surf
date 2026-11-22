"""Tests for the isolated `song_finder` remote plugin and 3-stage `song_studio` wiring."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import hashlib
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.colab_compute import _RvcCoverJobPayload
from hakimi.modules import ModuleRegistry
from hakimi.security import Rejected
from hakimi.songs import SongLibrary
from hakimi.system_modules import SystemResources
from hakimi.system_modules.song_studio import Module as SongStudioModule
from plugins.song_finder.plugin_main import (
    SONG_FINDER_MAX_BLOB_BYTES,
    SongFinderPlugin,
    build_song_finder_plugin_from_env,
)
from plugins.song_finder.providers import (
    BilibiliPublicProvider,
    ProviderCircuitBreaker,
    ProviderRiskControlError,
    SongFinderEngine,
    SongFinderError,
    YouTubeProvider,
    _clean_subprocess_env,
    is_clean_source_candidate,
    parse_duration_seconds,
    sanitize_search_query,
    validate_bili_cdn_url,
    validate_proxy_url,
    validate_youtube_url,
)
from tests.fakes import FakeGateway

REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_DECOUPLING_TOKENS = (
    "bilibili_surf",
    "plugins.bilibili",
    "bili_watcher",
    "bili_bridge",
    "bili_client",
    "BILI_COOKIE",
    "SESSDATA",
)


class _DuplexWsEndpoint:
    """In-memory WebSocket side enforcing strict frame-size tracking."""

    def __init__(
        self,
        rx: asyncio.Queue[str | None],
        tx: asyncio.Queue[str | None],
        frame_sizes: list[int],
    ) -> None:
        self._rx = rx
        self._tx = tx
        self._frame_sizes = frame_sizes
        self.closed = False
        self.close_code: int | None = None

    async def send(self, raw: str) -> None:
        if self.closed:
            raise RuntimeError("ws_closed")
        self._frame_sizes.append(len(raw.encode("utf-8")))
        await self._tx.put(raw)

    async def recv(self) -> str:
        item = await self._rx.get()
        if item is None:
            raise RuntimeError("ws_eof")
        return item

    receive = recv
    send_text = send
    receive_text = recv

    async def close(self, code: int = 1000) -> None:
        if not self.closed:
            self.closed = True
            self.close_code = code
            await self._tx.put(None)
            await self._rx.put(None)


def _make_duplex_ws() -> tuple[_DuplexWsEndpoint, _DuplexWsEndpoint, list[int]]:
    q_a_to_b: asyncio.Queue[str | None] = asyncio.Queue()
    q_b_to_a: asyncio.Queue[str | None] = asyncio.Queue()
    frame_sizes: list[int] = []
    server_ws = _DuplexWsEndpoint(q_a_to_b, q_b_to_a, frame_sizes)
    client_ws = _DuplexWsEndpoint(q_b_to_a, q_a_to_b, frame_sizes)
    return server_ws, client_ws, frame_sizes


class _FakeColabCompute:
    def __init__(self, *, is_configured: bool = True) -> None:
        self.is_configured = is_configured
        self.inbox_uploads: list[tuple[str, bytes, str]] = []
        self.submitted_jobs: list[tuple[str, dict[str, Any], str]] = []
        self.hooks: dict[str, Any] = {}

    def configured(self) -> bool:
        return self.is_configured

    async def upload_drive_inbox(
        self, filename: str, raw: bytes, *, mime_type: str = "application/octet-stream"
    ) -> dict[str, Any]:
        self.inbox_uploads.append((filename, raw, mime_type))
        return {
            "file_id": f"inbox-id-{len(self.inbox_uploads)}",
            "name": filename,
            "size": len(raw),
        }

    async def submit_job(
        self,
        job_type: str,
        payload: dict[str, Any],
        *,
        requester: str = "owner",
        gpu: str | None = None,
    ) -> dict[str, Any]:
        # Enforce the real strict Pydantic model (extra="forbid") on every submission!
        if job_type == "rvc_cover":
            _RvcCoverJobPayload.model_validate(payload)
        self.submitted_jobs.append((job_type, dict(payload), requester))
        return {
            "id": uuid.uuid4().hex,
            "job_type": job_type,
            "status": "queued",
            "requester": requester,
            "payload": dict(payload),
            "created_at": time.time(),
        }

    def register_completion_hook(self, job_type: str, callback: Any) -> None:
        self.hooks[job_type] = callback

    async def close(self) -> None:
        return None


@pytest.fixture
def fake_media_bins(tmp_path: Path) -> dict[str, Any]:
    """Create executable shims for yt-dlp, ffmpeg, and ffprobe that record argv and env."""
    bin_dir = tmp_path / "sf_shims"
    bin_dir.mkdir(exist_ok=True)
    log_file = tmp_path / "shim_calls.jsonl"
    state_file = tmp_path / "shim_state.json"
    state_file.write_text(
        json.dumps(
            {
                "yt_search_mode": "ok",
                "yt_download_mode": "ok",
                "yt_download_bytes": 24_000,
                "ffprobe_duration": "215.5",
                "ffmpeg_output_bytes": 32_000,
                "ffmpeg_fail": False,
            }
        ),
        encoding="utf-8",
    )

    yt_dlp_bin = bin_dir / "yt-dlp"
    yt_dlp_bin.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import json, os, sys",
                "from pathlib import Path",
                f"log_path = Path({str(log_file)!r})",
                f"state = json.loads(Path({str(state_file)!r}).read_text(encoding='utf-8'))",
                "argv = sys.argv[1:]",
                "rec = {'tool': 'yt-dlp', 'argv': argv, 'env_keys': sorted(os.environ.keys())}",
                "with log_path.open('a', encoding='utf-8') as fh:",
                "    fh.write(json.dumps(rec) + '\\n')",
                "if '--dump-json' in argv:",
                "    mode = state.get('yt_search_mode', 'ok')",
                "    if mode == 'risk_429':",
                "        sys.stderr.write('ERROR: HTTP Error 429: Too Many Requests\\n')",
                "        sys.exit(1)",
                "    if mode == 'bot_check':",
                '        sys.stderr.write("ERROR: Sign in to confirm you\'re not a bot\\n")',
                "        sys.exit(1)",
                "    if mode == 'ssrf_bad_url':",
                "        bad_row = {",
                "            'id': 'bad1',",
                "            'title': '晴天 官方原版',",
                "            'uploader': '周杰伦',",
                "            'duration': 269,",
                "            'webpage_url': 'https://evil.example.com/track',",
                "        }",
                "        print(json.dumps(bad_row))",
                "        sys.exit(0)",
                "    if mode == 'empty':",
                "        sys.exit(0)",
                "    rows = [",
                "        {",
                "            'id': 'cov1',",
                "            'title': '晴天 (翻唱 Cover Live)',",
                "            'uploader': 'Someone',",
                "            'duration': 260,",
                "            'webpage_url': 'https://www.youtube.com/watch?v=cov11111111',",
                "        },",
                "        {",
                "            'id': 'sht1',",
                "            'title': '晴天',",
                "            'uploader': 'Shorts',",
                "            'duration': 200,",
                "            'webpage_url': 'https://www.youtube.com/shorts/sht11111111',",
                "        },",
                "        {",
                "            'id': 'ok1',",
                "            'title': '晴天 Official Audio',",
                "            'uploader': 'Jay Chou - Topic',",
                "            'duration': 269,",
                "            'webpage_url': 'https://www.youtube.com/watch?v=ok111111111',",
                "        },",
                "    ]",
                "    for row in rows:",
                "        print(json.dumps(row, ensure_ascii=False))",
                "    sys.exit(0)",
                "dl_mode = state.get('yt_download_mode', 'ok')",
                "if dl_mode == 'risk_403':",
                "    sys.stderr.write('ERROR: HTTP Error 403: Forbidden\\n')",
                "    sys.exit(1)",
                "if dl_mode == 'too_large':",
                "    sys.stderr.write('File is larger than max-filesize\\n')",
                "    sys.exit(1)",
                "out_tpl = ''",
                "for idx, a in enumerate(argv):",
                "    if a == '-o' and idx + 1 < len(argv):",
                "        out_tpl = argv[idx + 1]",
                "out_path = Path(out_tpl.replace('%(ext)s', 'm4a'))",
                "size = int(state.get('yt_download_bytes', 24000))",
                "out_path.write_bytes(b'M4A_RAW_' + (b'A' * max(0, size - 8)))",
            ]
        ),
        encoding="utf-8",
    )

    ffmpeg_bin = bin_dir / "ffmpeg"
    ffmpeg_bin.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import json, os, sys",
                "from pathlib import Path",
                f"log_path = Path({str(log_file)!r})",
                f"state = json.loads(Path({str(state_file)!r}).read_text(encoding='utf-8'))",
                "argv = sys.argv[1:]",
                "rec = {'tool': 'ffmpeg', 'argv': argv, 'env_keys': sorted(os.environ.keys())}",
                "with log_path.open('a', encoding='utf-8') as fh:",
                "    fh.write(json.dumps(rec) + '\\n')",
                "if state.get('ffmpeg_fail'):",
                "    sys.exit(1)",
                "out_path = Path(argv[-1])",
                "out_size = int(state.get('ffmpeg_output_bytes', 32000))",
                "m4a_hdr = b'\\x00\\x00\\x00\\x18ftypM4A '",
                "mp3_hdr = b'ID3\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\xff\\xfb\\x90\\x00'",
                "header = m4a_hdr if out_path.suffix == '.m4a' else mp3_hdr",
                "payload = header + (b'Z' * max(0, out_size - len(header)))",
                "out_path.write_bytes(payload)",
            ]
        ),
        encoding="utf-8",
    )

    ffprobe_bin = bin_dir / "ffprobe"
    ffprobe_bin.write_text(
        "\n".join(
            [
                "#!" + sys.executable,
                "import json, os, sys",
                "from pathlib import Path",
                f"log_path = Path({str(log_file)!r})",
                f"state = json.loads(Path({str(state_file)!r}).read_text(encoding='utf-8'))",
                "argv = sys.argv[1:]",
                "rec = {'tool': 'ffprobe', 'argv': argv, 'env_keys': sorted(os.environ.keys())}",
                "with log_path.open('a', encoding='utf-8') as fh:",
                "    fh.write(json.dumps(rec) + '\\n')",
                "print(state.get('ffprobe_duration', '215.5'))",
            ]
        ),
        encoding="utf-8",
    )

    for p in (yt_dlp_bin, ffmpeg_bin, ffprobe_bin):
        os.chmod(p, 0o755)

    def _set_state(**updates: Any) -> None:
        cur = json.loads(state_file.read_text(encoding="utf-8"))
        cur.update(updates)
        state_file.write_text(json.dumps(cur), encoding="utf-8")

    def _read_calls() -> list[dict[str, Any]]:
        if not log_file.is_file():
            return []
        return [
            json.loads(line)
            for line in log_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    return {
        "yt_dlp": str(yt_dlp_bin),
        "ffmpeg": str(ffmpeg_bin),
        "ffprobe": str(ffprobe_bin),
        "set_state": _set_state,
        "read_calls": _read_calls,
    }


def _make_bili_mock_transport(
    *,
    search_status: int = 200,
    search_code: int = 0,
    playurl_urls: list[str] | None = None,
    cdn_statuses: dict[str, int] | None = None,
    cdn_payload: bytes = b"BILI_DASH_AUDIO_" + b"B" * 20_000,
    set_cookie_header: bool = True,
    view_duration: int = 240,
    captured_requests: list[httpx.Request] | None = None,
) -> httpx.MockTransport:
    default_urls = (
        playurl_urls
        if playurl_urls is not None
        else [
            "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/audio_main.m4s",
            "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/audio_backup.m4s",
        ]
    )
    statuses = cdn_statuses or {}

    def _handler(request: httpx.Request) -> httpx.Response:
        if captured_requests is not None:
            captured_requests.append(request)
        headers: dict[str, str] = {}
        if set_cookie_header:
            headers["Set-Cookie"] = "SESSDATA=should_never_be_persisted; Path=/"

        url_str = str(request.url)
        if "/x/web-interface/nav" in url_str:
            return httpx.Response(
                200,
                headers=headers,
                json={
                    "code": 0,
                    "data": {
                        "isLogin": False,
                        "wbi_img": {
                            "img_url": "https://i0.hdslb.com/bfs/wbi/7cd084941338484aae1ad9425b84077c.png",
                            "sub_url": "https://i0.hdslb.com/bfs/wbi/4932caff0ff746eab6f01bf08b70ac45.png",
                        },
                    },
                },
            )
        if "/x/web-interface/wbi/search/type" in url_str:
            if search_status != 200:
                return httpx.Response(search_status, headers=headers)
            if search_code != 0:
                return httpx.Response(200, headers=headers, json={"code": search_code, "data": {}})
            return httpx.Response(
                200,
                headers=headers,
                json={
                    "code": 0,
                    "data": {
                        "result": [
                            {
                                "bvid": "BV1xx411c7mD",
                                "title": '<em class="keyword">晴天</em> 翻唱 吉他弹唱',
                                "author": "CoverUser",
                                "duration": "4:10",
                            },
                            {
                                "bvid": "BV1Q541167Qg",
                                "title": '<em class="keyword">晴天</em> - 周杰伦 无损原版',
                                "author": "周杰伦音乐",
                                "duration": "4:29",
                            },
                        ]
                    },
                },
            )
        if "/x/web-interface/view" in url_str:
            return httpx.Response(
                200,
                headers=headers,
                json={
                    "code": 0,
                    "data": {"bvid": "BV1Q541167Qg", "cid": 998877, "duration": view_duration},
                },
            )
        if "/x/player/wbi/playurl" in url_str:
            return httpx.Response(
                200,
                headers=headers,
                json={
                    "code": 0,
                    "data": {
                        "dash": {
                            "audio": [
                                {
                                    "id": 30280,
                                    "bandwidth": 192000,
                                    "baseUrl": default_urls[0] if default_urls else "",
                                    "backupUrl": default_urls[1:],
                                }
                            ]
                        }
                    },
                },
            )

        status = statuses.get(url_str, 206)
        if status in (301, 302, 307):
            return httpx.Response(status, headers={"Location": "http://127.0.0.1:8080/internal"})
        if status >= 400:
            return httpx.Response(status, headers=headers)
        return httpx.Response(
            status,
            headers={**headers, "Content-Length": str(len(cdn_payload))},
            content=cdn_payload,
        )

    return httpx.MockTransport(_handler)


# ---------------------------------------------------------------------------
# 1. Static AST / import / env-var decoupling & Dockerfile security audit
# ---------------------------------------------------------------------------


def test_strict_static_ast_and_text_decoupling_from_bilibili_patrol():
    target_files = [
        *sorted((REPO_ROOT / "plugins" / "song_finder").rglob("*.py")),
        REPO_ROOT / "src" / "hakimi" / "songs.py",
        REPO_ROOT / "src" / "hakimi" / "system_modules" / "song_studio.py",
    ]
    assert len(target_files) >= 4

    for path in target_files:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        is_plugin_file = (REPO_ROOT / "plugins" / "song_finder") in path.parents

        # Zero raw text references to forbidden patrol modules or account cookie env vars
        for forbidden in FORBIDDEN_DECOUPLING_TOKENS:
            assert forbidden not in source, f"Forbidden reference {forbidden!r} in {path}"

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    for forbidden in FORBIDDEN_DECOUPLING_TOKENS:
                        assert forbidden not in alias.name, (
                            f"Forbidden import {alias.name} in {path}"
                        )
                    if is_plugin_file:
                        assert not alias.name.startswith("hakimi."), (
                            f"Plugin must not import engine internals: {alias.name} in {path}"
                        )
                        assert not alias.name.startswith("scripts."), (
                            f"Plugin must not import host scripts: {alias.name} in {path}"
                        )
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                for forbidden in FORBIDDEN_DECOUPLING_TOKENS:
                    assert forbidden not in mod, f"Forbidden from-import {mod} in {path}"
                if is_plugin_file:
                    assert not mod.startswith("hakimi."), (
                        f"Plugin must not import engine internals: {mod} in {path}"
                    )
                    assert not mod.startswith("scripts."), (
                        f"Plugin must not import host scripts: {mod} in {path}"
                    )
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                for forbidden in FORBIDDEN_DECOUPLING_TOKENS:
                    assert forbidden not in node.value, (
                        f"Forbidden constant {forbidden!r} in {path}"
                    )
            elif is_plugin_file and isinstance(node, ast.Call):
                # Ensure plugin never invokes shell subprocesses
                if isinstance(node.func, ast.Attribute):
                    assert node.func.attr != "create_subprocess_shell", f"shell exec in {path}"
                for kw in node.keywords:
                    if kw.arg == "shell" and isinstance(kw.value, ast.Constant):
                        assert kw.value.value is not True, f"shell=True forbidden in {path}"


def test_subprocess_env_allowlist_strips_sensitive_vars(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ADMIN_TOKEN", "top-secret-admin")
    monkeypatch.setenv("HAKIMI_PLUGIN_SONG_FINDER_TOKEN", "top-secret-plugin")
    monkeypatch.setenv("UNRELATED_SECRET", "top-secret-other")
    # Build forbidden env names dynamically so test source itself never embeds them in plugin files
    k1 = "BILI_" + "COOKIE"
    k2 = "SESS" + "DATA"
    monkeypatch.setenv(k1, "fake-cookie")
    monkeypatch.setenv(k2, "fake-sess")

    env = _clean_subprocess_env()
    assert env["HOME"] == "/tmp"
    assert env["TMPDIR"] == (os.environ.get("TMPDIR") or "/tmp")
    for secret_key in (
        "ADMIN_TOKEN",
        "HAKIMI_PLUGIN_SONG_FINDER_TOKEN",
        "UNRELATED_SECRET",
        k1,
        k2,
    ):
        assert secret_key not in env


def test_song_finder_dockerfile_hardening_and_physical_isolation():
    dockerfile = (REPO_ROOT / "plugins" / "song_finder" / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM python:3.12-slim@sha256:" in dockerfile
    assert "ffmpeg" in dockerfile
    assert "yt-dlp" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "HOME=/tmp" in dockerfile
    assert "TMPDIR=/tmp" in dockerfile
    assert "COPY plugins/song_finder ./plugins/song_finder" in dockerfile
    assert "COPY plugins ./plugins" not in dockerfile
    assert "secrets" not in dockerfile.lower()


# ---------------------------------------------------------------------------
# 2. Validation, clean-source filtering, injection & SSRF defense
# ---------------------------------------------------------------------------


def test_query_sanitization_rejects_injection_paths_and_urls():
    assert sanitize_search_query("晴天", artist="周杰伦") == "周杰伦 晴天"
    assert sanitize_search_query("夜に駆ける") == "夜に駆ける"
    assert sanitize_search_query("Don't Stop Me Now!") == "Don't Stop Me Now!"
    assert sanitize_search_query("Re:Zero") == "Re:Zero"
    assert sanitize_search_query("AC/DC Back in Black") == "AC/DC Back in Black"

    bad_queries = [
        "",
        "   ",
        "a" * 161,
        "-f bestaudio",
        "--exec id",
        "file:///etc/passwd",
        "file:/etc/passwd",
        "http://127.0.0.1/secret.mp3",
        "https://www.youtube.com/watch?v=abc",
        "ftp://example.com/a.mp3",
        "data:audio/mp3,abc",
        "/etc/passwd",
        "~/secret.mp3",
        "../secret.mp3",
        "./local.mp3",
        "C:\\Windows\\win.ini",
        "\\\\server\\share\\a.mp3",
        "song /etc/passwd",
        "song\n--exec id",
        "song\x00bad",
    ]
    for bad in bad_queries:
        with pytest.raises(SongFinderError) as exc_info:
            sanitize_search_query(bad)
        assert exc_info.value.code == "invalid_query", f"Expected invalid_query for {bad!r}"

    # Malicious artist or title fields are also rejected
    with pytest.raises(SongFinderError) as exc_artist:
        sanitize_search_query("晴天", artist="--proxy http://evil")
    assert exc_artist.value.code == "invalid_query"
    with pytest.raises(SongFinderError) as exc_title:
        sanitize_search_query("晴天", title="file:///etc/shadow")
    assert exc_title.value.code == "invalid_query"


def test_url_allowlists_and_clean_candidate_filters():
    assert validate_youtube_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
    assert validate_youtube_url("https://youtu.be/dQw4w9WgXcQ")
    for bad_yt in (
        "http://www.youtube.com/watch?v=abc",
        "https://evil.com/watch?v=abc",
        "https://127.0.0.1/watch?v=abc",
        "https://www.youtube.com:8443/watch?v=abc",
        "https://user@www.youtube.com/watch?v=abc",
        "https://www.youtube.com/shorts/abc123",
        "-https://www.youtube.com/watch?v=abc",
    ):
        with pytest.raises(SongFinderError) as exc_info:
            validate_youtube_url(bad_yt)
        assert exc_info.value.code == "ssrf_blocked"

    assert validate_bili_cdn_url("https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/a.m4s")
    assert validate_bili_cdn_url("https://cn-gd-cm.bilivideo.cn/upgcxcode/a.m4s")
    assert validate_bili_cdn_url("https://upos-hz-mirrorakam.akamaized.net/upgcxcode/a.m4s")
    for bad_cdn in (
        "http://upos-sz-mirrorcos.bilivideo.com/a.m4s",
        "https://127.0.0.1/a.m4s",
        "https://169.254.169.254/latest/meta-data",
        "https://evil.com/upos.bilivideo.com/a.m4s",
        "https://evil.akamaized.net/a.m4s",
        "https://upos-sz-mirrorcos.bilivideo.com:8443/a.m4s",
        "https://user:pass@upos-sz-mirrorcos.bilivideo.com/a.m4s",
    ):
        with pytest.raises(SongFinderError) as exc_info:
            validate_bili_cdn_url(bad_cdn)
        assert exc_info.value.code == "ssrf_blocked"

    assert validate_proxy_url("socks5h://127.0.0.1:1080") == "socks5h://127.0.0.1:1080"
    with pytest.raises(SongFinderError):
        validate_proxy_url("--exec whoami")
    with pytest.raises(SongFinderError):
        validate_proxy_url("file:///tmp/x")

    assert parse_duration_seconds("4:29") == 269.0
    assert parse_duration_seconds("1:02:03") == 3723.0

    # Clean studio titles pass (including "Alive" which must NOT trigger \blive\b)
    assert is_clean_source_candidate(
        {
            "title": "Stay Alive - Official Audio",
            "duration": 210,
            "webpage_url": "https://youtu.be/1",
        }
    )
    assert is_clean_source_candidate(
        {"title": "晴天 - 周杰伦 原版", "duration": 269, "webpage_url": "https://youtu.be/2"}
    )
    # Dirty / out-of-range candidates rejected
    for dirty in (
        {"title": "晴天 翻唱", "duration": 240},
        {"title": "晴天 (DJ版 串烧)", "duration": 240},
        {"title": "Song Name Live Concert", "duration": 240},
        {"title": "Song Name Karaoke 伴奏", "duration": 240},
        {"title": "Song Name Nightcore Remix", "duration": 240},
        {"title": "晴天", "duration": 45},
        {"title": "晴天", "duration": 650},
        {"title": "晴天", "duration": 240, "is_live": True},
        {"title": "晴天", "duration": 240, "age_limit": 18},
        {"title": "晴天", "duration": 240, "webpage_url": "https://www.youtube.com/shorts/xyz"},
    ):
        assert not is_clean_source_candidate(dirty), f"Should reject dirty candidate: {dirty}"


# ---------------------------------------------------------------------------
# 3. Provider execution, isolated circuit breaker, failover & /tmp cleanup
# ---------------------------------------------------------------------------


async def test_youtube_provider_argv_isolation_proxy_and_limits(
    fake_media_bins, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("ADMIN_TOKEN", "should-not-leak-to-yt-dlp")
    yt = YouTubeProvider(
        yt_dlp_bin=fake_media_bins["yt_dlp"],
        proxy_url="http://127.0.0.1:7890",
        max_download_bytes=512_000,
    )
    cands = await yt.search("周杰伦 晴天", artist="周杰伦", limit=5)
    assert len(cands) == 1
    assert cands[0].source_id == "ok1"
    assert cands[0].candidate_id == "youtube:ok1"

    calls = fake_media_bins["read_calls"]()
    search_argv = calls[0]["argv"]
    assert "ADMIN_TOKEN" not in calls[0]["env_keys"]
    assert "--no-playlist" in search_argv
    assert "--no-config" in search_argv
    assert "--proxy" in search_argv and "http://127.0.0.1:7890" in search_argv
    dash_idx = search_argv.index("--")
    assert search_argv[dash_idx + 1] == "ytsearch8:周杰伦 晴天"

    # Risk control stderr trips ProviderRiskControlError
    fake_media_bins["set_state"](yt_search_mode="risk_429")
    with pytest.raises(ProviderRiskControlError) as exc_risk:
        await yt.search("晴天")
    assert exc_risk.value.provider == "youtube"

    # Disallowed candidate URL from yt-dlp raises ssrf_blocked
    fake_media_bins["set_state"](yt_search_mode="ssrf_bad_url")
    with pytest.raises(SongFinderError) as exc_ssrf:
        await yt.search("晴天")
    assert exc_ssrf.value.code == "ssrf_blocked"

    # Oversized download rejected with download_too_large
    fake_media_bins["set_state"](yt_search_mode="ok", yt_download_mode="too_large")
    with tempfile.TemporaryDirectory(prefix="hakimi-sf-test-") as td:
        with pytest.raises(SongFinderError) as exc_big:
            await yt.download(cands[0], Path(td))
        assert exc_big.value.code == "download_too_large"


async def test_bilibili_public_provider_zero_cookie_cdn_fallback_and_ssrf():
    captured: list[httpx.Request] = []
    primary = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/audio_main.m4s"
    backup = "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/audio_backup.m4s"
    transport = _make_bili_mock_transport(
        playurl_urls=[primary, backup],
        cdn_statuses={primary: 403, backup: 206},
        set_cookie_header=True,
        captured_requests=captured,
    )
    async with httpx.AsyncClient(transport=transport, follow_redirects=False) as http:
        prov = BilibiliPublicProvider(http=http, max_download_bytes=1024 * 1024)
        cands = await prov.search("晴天", artist="周杰伦", limit=5)
        assert len(cands) == 1
        assert cands[0].source_id == "BV1Q541167Qg"
        assert "翻唱" not in cands[0].title

        with tempfile.TemporaryDirectory(prefix="hakimi-sf-bili-") as td:
            out_path = await prov.download(cands[0], Path(td))
            assert out_path.is_file()
            assert out_path.stat().st_size > 8192

    # Strictly zero Cookie header on EVERY request even when server returned Set-Cookie!
    assert len(captured) >= 5
    for req in captured:
        assert "cookie" not in {k.lower() for k in req.headers.keys()}, f"Cookie sent on {req.url}"
    cdn_reqs = [r for r in captured if "upgcxcode" in str(r.url)]
    assert len(cdn_reqs) == 2
    assert all(r.headers.get("Range") == "bytes=0-" for r in cdn_reqs)

    # Disallowed CDN URL blocked with ssrf_blocked
    bad_transport = _make_bili_mock_transport(
        playurl_urls=["https://169.254.169.254/latest/meta-data"],
    )
    async with httpx.AsyncClient(transport=bad_transport, follow_redirects=False) as http:
        prov_ssrf = BilibiliPublicProvider(http=http)
        with tempfile.TemporaryDirectory(prefix="hakimi-sf-bili-") as td:
            with pytest.raises(SongFinderError) as exc_ssrf:
                await prov_ssrf.download(cands[0], Path(td))
            assert exc_ssrf.value.code == "ssrf_blocked"

    # Redirect on CDN blocked with ssrf_blocked
    redir_transport = _make_bili_mock_transport(
        playurl_urls=[primary],
        cdn_statuses={primary: 302},
    )
    async with httpx.AsyncClient(transport=redir_transport, follow_redirects=False) as http:
        prov_redir = BilibiliPublicProvider(http=http)
        with tempfile.TemporaryDirectory(prefix="hakimi-sf-bili-") as td:
            with pytest.raises(SongFinderError) as exc_redir:
                await prov_redir.download(cands[0], Path(td))
            assert exc_redir.value.code == "ssrf_blocked"

    # Streaming download exceeding max_download_bytes aborted immediately
    big_transport = _make_bili_mock_transport(
        playurl_urls=[primary],
        cdn_statuses={primary: 206},
        cdn_payload=b"X" * 120_000,
    )
    async with httpx.AsyncClient(transport=big_transport, follow_redirects=False) as http:
        prov_big = BilibiliPublicProvider(http=http, max_download_bytes=80_000)
        with tempfile.TemporaryDirectory(prefix="hakimi-sf-bili-") as td:
            with pytest.raises(SongFinderError) as exc_big:
                await prov_big.download(cands[0], Path(td))
            assert exc_big.value.code == "download_too_large"

    # API risk code -412 triggers ProviderRiskControlError
    risk_transport = _make_bili_mock_transport(search_code=-412)
    async with httpx.AsyncClient(transport=risk_transport, follow_redirects=False) as http:
        prov_risk = BilibiliPublicProvider(http=http)
        with pytest.raises(ProviderRiskControlError) as exc_risk:
            await prov_risk.search("晴天")
        assert exc_risk.value.provider == "bilibili_public"


async def test_multi_provider_failover_isolated_cooldown_and_tmp_cleanup(fake_media_bins):
    now = [1000.0]
    breaker = ProviderCircuitBreaker(
        base_cooldown_seconds=300.0,
        min_interval_seconds=0.0,
        clock=lambda: now[0],
    )
    # YouTube hits HTTP 429; bilibili_public succeeds!
    fake_media_bins["set_state"](yt_search_mode="risk_429", ffprobe_duration="269.0")
    yt = YouTubeProvider(yt_dlp_bin=fake_media_bins["yt_dlp"])
    bili_http = httpx.AsyncClient(transport=_make_bili_mock_transport(), follow_redirects=False)
    bili = BilibiliPublicProvider(http=bili_http, clock=lambda: now[0])

    engine = SongFinderEngine(
        youtube=yt,
        bilibili_public=bili,
        provider_order="youtube,bilibili_public",
        circuit_breaker=breaker,
        ffmpeg_bin=fake_media_bins["ffmpeg"],
        ffprobe_bin=fake_media_bins["ffprobe"],
    )

    def _sf_tmp_dirs() -> set[Path]:
        return set(Path(tempfile.gettempdir()).glob("hakimi-sf-*"))

    try:
        before_tmp = await asyncio.to_thread(_sf_tmp_dirs)
        fetched = await engine.fetch_source("晴天", artist="周杰伦", strategy="auto")
        after_tmp = await asyncio.to_thread(_sf_tmp_dirs)

        assert fetched.provider == "bilibili_public"
        assert fetched.audio_format == "m4a"
        assert fetched.mime_type == "audio/mp4"
        assert fetched.byte_size == len(fetched.data)
        assert hashlib.sha256(fetched.data).hexdigest() == fetched.sha256
        assert "youtube" in fetched.skipped_providers
        assert fetched.skipped_providers["youtube"].startswith("risk_control:")
        # Only youtube is cooling down; bilibili_public stays completely healthy!
        snap = breaker.snapshot()
        assert snap["youtube"]["cooling_down"] is True
        assert snap["bilibili_public"]["cooling_down"] is False
        # Temporary directory cleaned immediately!
        assert after_tmp == before_tmp

        # Both ffprobe and ffmpeg enforce `-protocol_whitelist file` strictly before `-i`
        all_calls = fake_media_bins["read_calls"]()
        for tool_name in ("ffprobe", "ffmpeg"):
            tool_calls = [c["argv"] for c in all_calls if c["tool"] == tool_name]
            assert tool_calls, f"Expected {tool_name} invocation"
            for argv in tool_calls:
                pw_idx = argv.index("-protocol_whitelist")
                assert argv[pw_idx + 1] == "file"
                assert pw_idx < argv.index("-i")

        # Subsequent call skips youtube immediately without spawning yt-dlp again
        yt_calls_before = sum(1 for c in fake_media_bins["read_calls"]() if c["tool"] == "yt-dlp")
        fetched2 = await engine.fetch_source("晴天", strategy="auto")
        yt_calls_after = sum(1 for c in fake_media_bins["read_calls"]() if c["tool"] == "yt-dlp")
        assert yt_calls_after == yt_calls_before
        assert fetched2.provider == "bilibili_public"
        assert fetched2.skipped_providers["youtube"].startswith("cooldown:")

        # When BOTH providers are cooling down -> all_providers_in_cooldown
        breaker.trip("bilibili_public", "http_412")
        with pytest.raises(SongFinderError) as exc_cd:
            await engine.fetch_source("晴天", strategy="auto")
        assert exc_cd.value.code == "all_providers_in_cooldown"

        # Duration out-of-range check after cooldown expires
        now[0] += 1000.0
        fake_media_bins["set_state"](yt_search_mode="ok", ffprobe_duration="25.0")
        with pytest.raises(SongFinderError) as exc_dur:
            await engine.fetch_source("晴天", strategy="youtube")
        assert exc_dur.value.code == "duration_out_of_range"
        assert await asyncio.to_thread(_sf_tmp_dirs) == before_tmp

        # Output exceeding max_bytes raises download_too_large
        fake_media_bins["set_state"](
            yt_search_mode="ok", ffprobe_duration="210.0", ffmpeg_output_bytes=120_000
        )
        with pytest.raises(SongFinderError) as exc_sz:
            await engine.fetch_source("晴天", strategy="youtube", max_bytes=80_000)
        assert exc_sz.value.code == "download_too_large"

        # Transcode failure propagates transcode_failed
        fake_media_bins["set_state"](
            yt_search_mode="ok", ffprobe_duration="210.0", ffmpeg_fail=True
        )
        with pytest.raises(SongFinderError) as exc_tc:
            await engine.fetch_source("晴天", strategy="youtube")
        assert exc_tc.value.code == "transcode_failed"
    finally:
        await engine.close()
        await bili_http.aclose()


async def test_reverse_provider_order_fails_over_from_bilibili_to_youtube(fake_media_bins):
    now = [2000.0]
    breaker = ProviderCircuitBreaker(
        base_cooldown_seconds=300.0,
        min_interval_seconds=0.0,
        clock=lambda: now[0],
    )
    fake_media_bins["set_state"](
        yt_search_mode="ok",
        yt_download_mode="ok",
        ffprobe_duration="240.0",
        ffmpeg_output_bytes=40_000,
        ffmpeg_fail=False,
    )
    bili_http = httpx.AsyncClient(
        transport=_make_bili_mock_transport(search_status=412),
        follow_redirects=False,
    )
    engine = SongFinderEngine(
        youtube=YouTubeProvider(yt_dlp_bin=fake_media_bins["yt_dlp"]),
        bilibili_public=BilibiliPublicProvider(http=bili_http, clock=lambda: now[0]),
        provider_order="bilibili_public,youtube",
        circuit_breaker=breaker,
        ffmpeg_bin=fake_media_bins["ffmpeg"],
        ffprobe_bin=fake_media_bins["ffprobe"],
    )
    try:
        fetched = await engine.fetch_source("晴天", artist="周杰伦", strategy="auto")
        assert fetched.provider == "youtube"
        assert fetched.skipped_providers["bilibili_public"].startswith("risk_control:")
        snap = breaker.snapshot()
        assert snap["bilibili_public"]["cooling_down"] is True
        assert snap["youtube"]["cooling_down"] is False
    finally:
        await engine.close()
        await bili_http.aclose()


# ---------------------------------------------------------------------------
# 4. End-to-end PluginBus + SongFinderPlugin + SongLibrary 3-stage boundary
# ---------------------------------------------------------------------------


async def test_e2e_song_finder_plugin_chunked_blob_to_inbox_and_fallbacks(
    settings, fake_media_bins, monkeypatch: pytest.MonkeyPatch
):
    # Produce a > 256 KiB transcoded audio payload (~350 KiB) to exercise chunked blob transfer!
    fake_media_bins["set_state"](
        yt_search_mode="ok",
        yt_download_mode="ok",
        ffprobe_duration="240.0",
        ffmpeg_output_bytes=350_000,
    )

    s = settings.model_copy(
        update={
            "admin_token": SecretStr("song-finder-e2e-admin-token"),
            "plugin_bus_ws_enabled": True,
        }
    )
    app = create_app(s, gateway_factory=lambda *_: FakeGateway(), start_workers=False)

    async with app.router.lifespan_context(app):
        svc = app.state.service
        bus = svc.plugin_bus
        songs: SongLibrary = svc.songs
        compute = _FakeColabCompute(is_configured=True)
        songs.compute = compute
        songs.plugin_bus = bus

        token, _ = await bus.create_plugin(
            "song_finder",
            "音源检索器",
            scopes=["actions"],
            node_region="KR",
        )

        server_ws, client_ws, frame_sizes = _make_duplex_ws()
        yt = YouTubeProvider(yt_dlp_bin=fake_media_bins["yt_dlp"])
        bili_http = httpx.AsyncClient(transport=_make_bili_mock_transport(), follow_redirects=False)
        bili = BilibiliPublicProvider(http=bili_http)
        engine = SongFinderEngine(
            youtube=yt,
            bilibili_public=bili,
            provider_order="youtube,bilibili_public",
            circuit_breaker=ProviderCircuitBreaker(min_interval_seconds=0.0),
            ffmpeg_bin=fake_media_bins["ffmpeg"],
            ffprobe_bin=fake_media_bins["ffprobe"],
        )
        plugin = SongFinderPlugin(
            ws_url="ws://127.0.0.1:12141/ws/plugin",
            token=token,
            engine=engine,
            connect_factory=lambda _url, **_kw: client_ws,
            reconnect_min=0.05,
            reconnect_max=0.1,
        )

        serve_task = asyncio.create_task(
            bus.serve(
                server_ws.receive,
                server_ws.send,
                server_ws.close,
                source_ip="127.0.0.1",
                plugin_id="song_finder",
            )
        )
        await plugin.client.start()
        try:
            assert await plugin.client.wait_connected(timeout=3.0)
            for _ in range(40):
                if bus.has_tool("song_finder", "song_finder.fetch_source"):
                    break
                await asyncio.sleep(0.02)
            assert bus.has_tool("song_finder", "song_finder.search")
            assert bus.has_tool("song_finder", "song_finder.fetch_source")
            assert songs.cache_stats()["song_finder_online"] is True

            # 1) Direct song_finder.search RPC via PluginBus + invalid_query guard
            search_res = await bus.invoke_tool(
                "song_finder",
                "song_finder.search",
                {"query": "晴天", "artist": "周杰伦", "strategy": "auto", "limit": 5},
                max_response_blob_bytes=0,
            )
            assert search_res.blob is None
            assert search_res.data["provider_used"] == "youtube"
            assert len(search_res.data["candidates"]) == 1
            assert search_res.data["candidates"][0]["candidate_id"] == "youtube:ok1"

            with pytest.raises(Rejected, match="invalid_query"):
                await bus.invoke_tool(
                    "song_finder",
                    "song_finder.fetch_source",
                    {"query": "file:///etc/passwd"},
                    max_response_blob_bytes=SONG_FINDER_MAX_BLOB_BYTES,
                )

            # Explicit audio_bytes or inbox_filename bypasses remote song_finder
            await songs.request_learn_song(
                "已有本地附件",
                audio_bytes=b"LOCAL_RAW_AUDIO",
                audio_filename="local.mp3",
                auto_search=True,
            )
            assert len(compute.inbox_uploads) == 1
            assert compute.inbox_uploads[0][0] == "local.mp3"
            compute.inbox_uploads.clear()

            # 2) Three-stage learn flow: song_finder -> Hakimi_Songs/inbox/ -> colab_compute
            job = await songs.request_learn_song(
                "晴天",
                artist="周杰伦",
                tags="经典",
                pitch=1,
                auto_search=True,
                requester="owner",
            )
            assert job["job_type"] == "rvc_cover"
            assert len(compute.inbox_uploads) == 1
            up_name, up_bytes, up_mime = compute.inbox_uploads[0]
            assert up_name.endswith(".m4a")
            assert up_mime == "audio/mp4"
            assert len(up_bytes) == 350_000

            _, submitted_payload, _ = compute.submitted_jobs[-1]
            assert submitted_payload["inbox_file_id"] == "inbox-id-1"
            assert submitted_payload["inbox_filename"] == up_name
            assert submitted_payload["auto_search"] is False
            assert "query" not in submitted_payload
            # Every single WS frame stayed strictly <= 256 KiB!
            assert frame_sizes and max(frame_sizes) <= 262_144

            # 3) Graceful fallback when song_finder finds no clean track
            fake_media_bins["set_state"](yt_search_mode="empty")
            bili.http = httpx.AsyncClient(
                transport=_make_bili_mock_transport(search_code=-412),
                follow_redirects=False,
            )
            try:
                await songs.request_learn_song("非常冷门的歌", auto_search=True)
                _, fallback_payload, _ = compute.submitted_jobs[-1]
                assert fallback_payload["auto_search"] is True
                assert fallback_payload["query"] == "非常冷门的歌"
                assert "inbox_file_id" not in fallback_payload

                # 4) Strict SONG_FINDER_REMOTE_ONLY=1 prevents local fallback on miss
                monkeypatch.setenv("SONG_FINDER_REMOTE_ONLY", "1")
                with pytest.raises(Rejected, match="source_not_found"):
                    await songs.request_learn_song("非常冷门的歌", auto_search=True)
            finally:
                await bili.http.aclose()
        finally:
            await plugin.close()
            await server_ws.close()
            with contextlib.suppress(Exception):
                await serve_task
            await bili_http.aclose()

        # 5) Offline behavior: graceful fallback by default, Rejected when SONG_FINDER_REMOTE_ONLY=1
        assert bus.has_tool("song_finder", "song_finder.fetch_source") is False
        monkeypatch.delenv("SONG_FINDER_REMOTE_ONLY", raising=False)
        await songs.request_learn_song("离线回退曲", auto_search=True)
        _, offline_payload, _ = compute.submitted_jobs[-1]
        assert offline_payload["auto_search"] is True
        assert offline_payload["query"] == "离线回退曲"

        monkeypatch.setenv("SONG_FINDER_REMOTE_ONLY", "true")
        with pytest.raises(Rejected, match="song_finder_remote_unavailable"):
            await songs.request_learn_song("严格远程曲", auto_search=True)


async def test_song_library_rejects_corrupt_or_disallowed_remote_blob(
    store, monkeypatch: pytest.MonkeyPatch
):
    from hakimi_plugin_sdk import ToolCallResult

    compute = _FakeColabCompute(is_configured=True)

    class _TamperedBus:
        def __init__(self, result: ToolCallResult) -> None:
            self._result = result

        def has_tool(
            self, plugin_id: str, action_name: str, *, require_enabled: bool = True
        ) -> bool:
            return plugin_id == "song_finder" and action_name == "song_finder.fetch_source"

        async def invoke_tool(self, *_args: Any, **_kwargs: Any) -> ToolCallResult:
            return self._result

    # 1) SHA-256 mismatch -> falls back safely in default mode, rejects in remote_only mode
    bad_sha_bus = _TamperedBus(
        ToolCallResult(
            data={"provider": "youtube", "sha256": "0" * 64, "filename": "track.m4a"},
            blob=b"TAMPERED_BYTES",
            blob_mime_type="audio/mp4",
            blob_filename="track.m4a",
        )
    )
    lib = SongLibrary(store, compute=compute, plugin_bus=bad_sha_bus)
    await lib.initialize()
    await lib.request_learn_song("校验失败曲", auto_search=True)
    assert len(compute.inbox_uploads) == 0
    assert compute.submitted_jobs[-1][1]["auto_search"] is True

    monkeypatch.setenv("SONG_FINDER_REMOTE_ONLY", "1")
    with pytest.raises(Rejected, match="song_finder_remote_unavailable"):
        await lib.request_learn_song("校验失败曲", auto_search=True)

    # 2) Disallowed file extension (.exe) blocked before upload_drive_inbox
    monkeypatch.delenv("SONG_FINDER_REMOTE_ONLY", raising=False)
    bad_ext_bytes = b"NOT_AUDIO"
    bad_ext_bus = _TamperedBus(
        ToolCallResult(
            data={
                "provider": "youtube",
                "sha256": hashlib.sha256(bad_ext_bytes).hexdigest(),
                "filename": "payload.exe",
            },
            blob=bad_ext_bytes,
            blob_mime_type="application/octet-stream",
            blob_filename="payload.exe",
        )
    )
    lib.plugin_bus = bad_ext_bus
    await lib.request_learn_song("坏后缀曲", auto_search=True)
    assert len(compute.inbox_uploads) == 0
    assert compute.submitted_jobs[-1][1]["auto_search"] is True


async def test_song_studio_module_wires_plugin_bus_without_changing_requires(settings, store):
    compute = _FakeColabCompute(is_configured=True)

    class _FakeBus:
        def has_tool(
            self, plugin_id: str, action_name: str, *, require_enabled: bool = True
        ) -> bool:
            return plugin_id == "song_finder" and action_name == "song_finder.fetch_source"

    class _FakeColabMod:
        module_id = "colab_compute"

        def __init__(self) -> None:
            self.compute = compute

    class _FakeDriveMod:
        module_id = "drive_sync"

    fake_bus = _FakeBus()
    registry = ModuleRegistry()
    registry.systems["colab_compute"] = _FakeColabMod()  # type: ignore[assignment]
    registry.systems["drive_sync"] = _FakeDriveMod()  # type: ignore[assignment]

    resources = SystemResources(
        settings=settings,
        store=store,
        gateway=FakeGateway(),  # type: ignore[arg-type]
        http=None,
        service=SimpleNamespace(plugin_bus=fake_bus),  # type: ignore[arg-type]
    )
    mod = SongStudioModule(resources, registry)
    assert mod.requires == ("colab_compute", "drive_sync")
    await mod.initialize()
    assert mod.songs.plugin_bus is fake_bus
    st = await mod.status()
    assert st["song_finder_online"] is True
    assert "独立找歌在线" in st["detail"]


def test_build_song_finder_plugin_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("HAKIMI_PLUGIN_SONG_FINDER_TOKEN", raising=False)
    monkeypatch.delenv("HAKIMI_PLUGIN_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="HAKIMI_PLUGIN_SONG_FINDER_TOKEN"):
        build_song_finder_plugin_from_env()

    monkeypatch.setenv("HAKIMI_PLUGIN_SONG_FINDER_TOKEN", "sf-test-token")
    monkeypatch.setenv("SONG_FINDER_PROVIDER_ORDER", "bilibili_public,youtube")
    monkeypatch.setenv("SONG_FINDER_PROXY_URL", "http://127.0.0.1:7890")
    monkeypatch.setenv("SONG_FINDER_COOLDOWN_SECONDS", "900")
    plugin = build_song_finder_plugin_from_env()
    assert plugin.client.config.plugin_id == "song_finder"
    assert plugin.client.config.scopes == ("actions",)
    assert plugin.client.config.admin_panel is None
    assert plugin.client.max_blob_bytes == SONG_FINDER_MAX_BLOB_BYTES
    assert plugin.engine.provider_order == ("bilibili_public", "youtube")
    assert plugin.engine.providers["youtube"].proxy_url == "http://127.0.0.1:7890"
    assert plugin.engine.circuit_breaker.base_cooldown_seconds == 900.0
