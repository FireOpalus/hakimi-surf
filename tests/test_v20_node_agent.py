"""Unit tests for the Phase 3 standalone domestic node_agent."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import random
import stat
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image
from pydantic import ValidationError
from websockets.asyncio.server import serve

from node_agent.bili_collector import (
    COVER_MAX_JPEG_BYTES,
    STORYBOARD_MAX_JPEG_BYTES,
    STORYBOARD_YTDLP_FORMAT,
    BiliCollector,
    CollectorError,
    _classify_ytdlp_error,
    _validate_videoshot_url,
)
from node_agent.config import NodeAgentSettings
from node_agent.daemon import NodeDaemon, compute_backoff_step, decode_node_frame

STRONG_TOKEN = "cn-node-test-secret-token-0123456789abcdef"
TEST_BVID = "BV1xx411c7mD"


def _make_settings(tmp_path: Path, **overrides: Any) -> NodeAgentSettings:
    defaults: dict[str, Any] = {
        "node_ws_url": "ws://127.0.0.1:18443/ws/node/0123456789abcdef01234567",
        "node_ws_token": STRONG_TOKEN,
        "node_id": "shanghai-cn-1",
        "node_region": "CN",
        "ws_ping_interval": 25.0,
        "ws_ping_timeout": 30.0,
        "reconnect_max": 0.25,
        "max_frame_bytes": 1_048_576,
        "bili_min_interval": 0.0,
        "temp_dir": tmp_path / "node_bili_jobs",
    }
    defaults.update(overrides)
    return NodeAgentSettings(**defaults)


def _png_bytes(width: int, height: int, *, noisy: bool = False) -> bytes:
    if noisy:
        rng = random.Random(42)
        raw = bytes(rng.getrandbits(8) for _ in range(width * height * 3))
        img = Image.frombytes("RGB", (width, height), raw)
    else:
        img = Image.new("RGB", (width, height), (40, 120, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def test_node_agent_settings_validation_and_env_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_settings(
        tmp_path,
        node_ws_url="wss://qq.example.com/ws/node/0123456789abcdef0123456789abcdef",
    )
    assert cfg.node_region == "CN"
    assert cfg.max_frame_bytes == 1_048_576

    for bad_url in (
        "ws://qq.example.com/ws/node",
        "https://qq.example.com/ws/node",
        "wss://qq.example.com/ws/node?token=abc",
        "wss://user:pass@qq.example.com/ws/node",
        "wss://qq.example.com/ws/node#frag",
    ):
        with pytest.raises(ValidationError):
            _make_settings(tmp_path, node_ws_url=bad_url)

    for bad_token in (
        "short",
        "GENERATE_32+_CHAR_RANDOM_SECRET_TOKEN_BEFORE_ENABLE",
        "REPLACE_WITH_LONG_TOKEN_OVER_32_CHARACTERS_LONG",
        "a" * 31 + "\n",
    ):
        with pytest.raises(ValidationError):
            _make_settings(tmp_path, node_ws_token=bad_token)

    with pytest.raises(ValidationError):
        _make_settings(tmp_path, node_region="KR")

    with pytest.raises(ValidationError):
        _make_settings(tmp_path, max_frame_bytes=1_048_577)

    monkeypatch.setenv("NODE_WS_URL", "ws://localhost:12141/ws/node/0123456789abcdef01234567")
    monkeypatch.setenv("NODE_WS_TOKEN", STRONG_TOKEN)
    monkeypatch.setenv("NODE_ID", "shanghai-cn-2")
    monkeypatch.setenv("NODE_REGION", "CN")
    monkeypatch.setenv("BILI_MIN_INTERVAL", "1.5")
    monkeypatch.setenv("NODE_TEMP_DIR", str(tmp_path / "env_jobs"))
    env_cfg = NodeAgentSettings(_env_file=None)
    assert env_cfg.node_id == "shanghai-cn-2"
    assert env_cfg.bili_min_interval == 1.5
    assert env_cfg.temp_dir == tmp_path / "env_jobs"


async def test_ssrf_allowlists_and_http_status_propagation(tmp_path: Path) -> None:
    seen_requests: list[tuple[str, str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append((request.method, str(request.url), dict(request.headers)))
        if request.url.host == "b23.tv":
            return httpx.Response(
                302, headers={"Location": f"https://www.bilibili.com/video/{TEST_BVID}"}
            )
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {"bvid": TEST_BVID}})
        if request.url.path == "/room/v1/Room/get_info":
            return httpx.Response(412, json={"code": -412})
        if request.url.host == "aisubtitle.hdslb.com":
            return httpx.Response(200, json={"body": [{"content": "hello"}]})
        return httpx.Response(200, json={"code": 0, "data": {}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "jobs", http_client=client)
    try:
        pub = await collector.public_get(
            {"live": False, "path": "/x/web-interface/view", "params": {"bvid": TEST_BVID}}
        )
        assert pub == {"code": 0, "data": {"bvid": TEST_BVID}}

        with pytest.raises(CollectorError) as exc_info:
            await collector.public_get(
                {"live": True, "path": "/room/v1/Room/get_info", "params": {"room_id": "123"}}
            )
        assert exc_info.value.code == "bili_http_status"
        assert exc_info.value.upstream_status == 412

        for bad_public in (
            {"live": False, "path": "/x/web-interface/coin/add", "params": {}},
            {"live": False, "path": "/room/v1/Room/get_info", "params": {}},
            {"live": True, "path": "/x/web-interface/view", "params": {}},
            {"live": False, "path": "https://evil.example/x/web-interface/view", "params": {}},
        ):
            with pytest.raises(CollectorError, match="bili_endpoint_forbidden"):
                await collector.public_get(bad_public)

        short = await collector.resolve_short({"url": "https://b23.tv/AbC123"})
        assert short == {"location": f"https://www.bilibili.com/video/{TEST_BVID}"}
        for bad_short in (
            "http://b23.tv/AbC123",
            "https://evil.example/AbC123",
            "https://b23.tv/AbC123?x=1",
            "https://127.0.0.1/AbC123",
        ):
            with pytest.raises(CollectorError, match="invalid_bilibili_short_link"):
                await collector.resolve_short({"url": bad_short})

        for bad_req in (
            {"method": "GET", "path": "/x/web-interface/archive/like"},
            {"method": "POST", "path": "/x/web-interface/nav"},
            {"method": "GET", "path": "/x/internal/secret"},
            {"method": "GET", "path": "/x/web-interface/nav", "post": {"a": "1"}},
        ):
            with pytest.raises(CollectorError, match="bili_endpoint_forbidden"):
                await collector.request(bad_req)

        for bad_sub in (
            {
                "method": "GET",
                "path": "http://aisubtitle.hdslb.com/bfs/sub.json",
                "subtitle": True,
            },
            {"method": "GET", "path": "https://evil.example/bfs/sub.json", "subtitle": True},
            {
                "method": "GET",
                "path": "https://aisubtitle.hdslb.com/other/sub.json",
                "subtitle": True,
            },
            {
                "method": "GET",
                "path": "https://aisubtitle.hdslb.com/bfs/../secret.json",
                "subtitle": True,
            },
            {
                "method": "POST",
                "path": "https://aisubtitle.hdslb.com/bfs/sub.json",
                "subtitle": True,
            },
        ):
            with pytest.raises(CollectorError, match="bili_subtitle_host_forbidden"):
                await collector.request(bad_sub)

        # Subtitle CDN must NEVER receive cookies even if cookies are passed in params.
        sub_res = await collector.request(
            {
                "method": "GET",
                "path": "https://aisubtitle.hdslb.com/bfs/subtitle/demo.json?auth_key=abc",
                "subtitle": True,
                "cookies": {"sessdata": "secret-session-val"},
            }
        )
        assert sub_res == {"envelope": {"body": [{"content": "hello"}]}}
        _, sub_url, sub_headers = seen_requests[-1]
        assert "aisubtitle.hdslb.com" in sub_url
        assert "cookie" not in {k.lower() for k in sub_headers}
    finally:
        await collector.close()
        await client.aclose()


async def test_cookie_in_memory_only_and_never_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    captured_headers: list[dict[str, str]] = []
    secret_sess = "super-secret-sessdata-value-998877"
    secret_jct = "secret-bili-jct-token-112233"

    def handler(request: httpx.Request) -> httpx.Response:
        captured_headers.append(dict(request.headers))
        return httpx.Response(200, json={"code": 0, "data": {"isLogin": True}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    jobs_dir = tmp_path / "jobs"
    collector = BiliCollector(min_interval=0.0, temp_dir=jobs_dir, http_client=client)
    caplog.set_level(logging.DEBUG)
    try:
        res = await collector.request(
            {
                "method": "GET",
                "path": "/x/web-interface/nav",
                "cookies": {"sessdata": secret_sess, "bili_jct": secret_jct},
            }
        )
        assert res == {"envelope": {"code": 0, "data": {"isLogin": True}}}
        cookie_header = captured_headers[-1].get("cookie", "")
        assert f"SESSDATA={secret_sess}" in cookie_header
        assert f"bili_jct={secret_jct}" in cookie_header
        assert not jobs_dir.exists()
        assert secret_sess not in caplog.text
        assert secret_jct not in caplog.text

        for bad_cookie in (
            {"sessdata": "abc;evil=1"},
            {"sessdata": "abc\r\nX-Injected: 1"},
            {"unknown": "abc"},
        ):
            with pytest.raises(CollectorError, match="bili_credentials_invalid"):
                await collector.request(
                    {"method": "GET", "path": "/x/web-interface/nav", "cookies": bad_cookie}
                )
    finally:
        await collector.close()
        await client.aclose()


async def test_cover_thumb_pillow_cap_exif_bomb_and_ssrf(tmp_path: Path) -> None:
    noisy_png = _png_bytes(1600, 1200, noisy=True)
    huge_payload = b"x" * (8_388_608 + 10)

    # Create JPEG with EXIF orientation=6 (90 deg CW -> 400x200 becomes 200x400).
    exif_img = Image.new("RGB", (400, 200), (10, 200, 30))
    exif = exif_img.getexif()
    exif[274] = 6
    exif_buf = io.BytesIO()
    exif_img.save(exif_buf, format="JPEG", exif=exif.tobytes())
    exif_bytes = exif_buf.getvalue()

    # Large-pixel PNG (6000x5000 = 30_000_000 > 25_000_000 limit) compresses tiny as solid PNG.
    bomb_img = Image.new("RGB", (6000, 5000), (0, 0, 0))
    bomb_buf = io.BytesIO()
    bomb_img.save(bomb_buf, format="PNG")
    bomb_bytes = bomb_buf.getvalue()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/bfs/archive/huge.jpg":
            return httpx.Response(200, content=huge_payload)
        if request.url.path == "/bfs/archive/exif.jpg":
            return httpx.Response(200, content=exif_bytes)
        if request.url.path == "/bfs/archive/bomb.png":
            return httpx.Response(200, content=bomb_bytes)
        return httpx.Response(200, content=noisy_png)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "jobs", http_client=client)
    try:
        res = await collector.cover_thumb(
            {"url": "https://i0.hdslb.com/bfs/archive/test_cover.png"}
        )
        raw_jpeg = base64.b64decode(res["data_b64"])
        assert res["mime_type"] == "image/jpeg"
        assert res["bytes"] == len(raw_jpeg)
        assert 1 <= res["bytes"] <= COVER_MAX_JPEG_BYTES
        assert res["width"] <= 640 and res["height"] <= 640
        with Image.open(io.BytesIO(raw_jpeg)) as decoded:
            assert decoded.format == "JPEG"
            assert decoded.size == (res["width"], res["height"])

        exif_res = await collector.cover_thumb(
            {"url": "https://archive.biliimg.com/bfs/archive/exif.jpg"}
        )
        assert (exif_res["width"], exif_res["height"]) == (200, 400)

        with pytest.raises(CollectorError, match="image_format_or_size"):
            await collector.cover_thumb({"url": "https://i2.hdslb.com/bfs/archive/bomb.png"})

        for bad_cover in (
            "http://i0.hdslb.com/bfs/archive/a.jpg",
            "https://evil.example/bfs/archive/a.jpg",
            "https://i0.hdslb.com/other/a.jpg",
            "https://i0.hdslb.com/bfs/archive/../secret.jpg",
        ):
            with pytest.raises(CollectorError, match="bili_cover_host_forbidden"):
                await collector.cover_thumb({"url": bad_cover})

        with pytest.raises(CollectorError, match="upstream_body_too_large"):
            await collector.cover_thumb({"url": "https://i1.hdslb.com/bfs/archive/huge.jpg"})
    finally:
        await collector.close()
        await client.aclose()


async def test_video_storyboard_cookie_cleanup_cancellation_and_3x3_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    temp_root = tmp_path / "node_bili_jobs"
    stale_dir = temp_root / ("job-" + "a" * 32)
    stale_dir.mkdir(parents=True)
    (stale_dir / "leftover.txt").write_text("stale", encoding="utf-8")

    def videoshot_empty_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.bilibili.com"
        assert request.url.path == "/x/player/videoshot"
        return httpx.Response(200, json={"code": 0, "data": {"image": []}})

    client = httpx.AsyncClient(transport=httpx.MockTransport(videoshot_empty_handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=temp_root, http_client=client)
    observed: dict[str, Any] = {"cookie_mode": None, "cookie_wiped_before_ffprobe": False}
    seen_ytdlp_argv: list[str] = []
    block_ytdlp = asyncio.Event()
    ytdlp_started = asyncio.Event()
    timeout_ytdlp = False

    async def fake_exec(
        argv: list[str], *, cwd: Path, env: dict[str, str], wait_limit: float
    ) -> tuple[int, bytes, bytes]:
        _ = wait_limit
        assert "NODE_WS_TOKEN" not in env
        tool = Path(argv[0]).name
        if "yt-dlp" in tool or "yt_dlp" in " ".join(argv):
            seen_ytdlp_argv.clear()
            seen_ytdlp_argv.extend(argv)
            cookie_file = cwd / "cookies.txt"
            assert cookie_file.exists()
            observed["cookie_mode"] = stat.S_IMODE(cookie_file.lstat().st_mode)
            assert "SESSDATA\tcn-temp-cookie-val" in cookie_file.read_text(encoding="utf-8")
            if timeout_ytdlp:
                raise CollectorError("video_download_timeout")
            if block_ytdlp.is_set():
                ytdlp_started.set()
                await asyncio.sleep(10)
            (cwd / "video.mp4").write_bytes(b"\x00\x00\x00\x18ftypmp42")
            return 0, b"", b""
        if tool == "ffprobe":
            observed["cookie_wiped_before_ffprobe"] = not (cwd / "cookies.txt").exists()
            payload = {"format": {"duration": "900.0", "format_name": "mp4"}}
            return 0, json.dumps(payload).encode(), b""
        if tool == "ffmpeg":
            out_path = Path(argv[-1])
            rng = random.Random(hash(out_path.name) & 0xFFFF)
            tile = Image.frombytes(
                "RGB", (320, 180), bytes(rng.getrandbits(8) for _ in range(320 * 180 * 3))
            )
            tile.save(out_path, format="JPEG", quality=85)
            return 0, b"", b""
        raise AssertionError(f"Unexpected argv: {argv}")

    monkeypatch.setattr(collector, "_exec_bounded", fake_exec)
    caplog.set_level(logging.INFO, logger="node_agent.bili")
    try:
        sb = await collector.video_storyboard(
            {
                "bvid": TEST_BVID,
                "max_seconds": 900,
                "cookies": {"sessdata": "cn-temp-cookie-val"},
            }
        )
        assert not stale_dir.exists()
        assert "--no-playlist" in seen_ytdlp_argv
        assert "--max-filesize" in seen_ytdlp_argv
        assert "40M" in seen_ytdlp_argv
        assert STORYBOARD_YTDLP_FORMAT in seen_ytdlp_argv
        assert "worst[ext=mp4]/mp4" not in seen_ytdlp_argv
        assert observed["cookie_mode"] == 0o600
        assert observed["cookie_wiped_before_ffprobe"] is True
        assert f"event=bili_videoshot_fallback bvid={TEST_BVID}" in caplog.text
        assert "cn-temp-cookie-val" not in caplog.text

        raw_jpeg = base64.b64decode(sb["data_b64"])
        assert sb["bvid"] == TEST_BVID
        assert sb["duration"] == 900.0
        assert sb["mime_type"] == "image/jpeg"
        assert sb["grid"] == "3x3"
        assert sb["source"] == "ytdlp_fallback"
        assert sb["bytes"] == len(raw_jpeg)
        assert 1 <= sb["bytes"] <= STORYBOARD_MAX_JPEG_BYTES
        with Image.open(io.BytesIO(raw_jpeg)) as sheet:
            assert sheet.format == "JPEG"
            assert sheet.width == 960 and sheet.height == 540

        # Job directory must be wiped completely after completion.
        assert list(temp_root.iterdir()) == []

        # Failure in ffprobe (duration > max_seconds) must ALSO wipe job directory and cookies.
        with pytest.raises(CollectorError, match="video_download_duration"):
            await collector.video_storyboard(
                {
                    "bvid": TEST_BVID,
                    "max_seconds": 60,
                    "cookies": {"sessdata": "cn-temp-cookie-val"},
                }
            )
        assert list(temp_root.iterdir()) == []

        # Timeout in yt-dlp must ALSO wipe job directory and cookies and log desensitized reason.
        timeout_ytdlp = True
        with pytest.raises(CollectorError, match="video_download_timeout"):
            await collector.video_storyboard(
                {
                    "bvid": TEST_BVID,
                    "max_seconds": 120,
                    "cookies": {"sessdata": "cn-temp-cookie-val"},
                }
            )
        timeout_ytdlp = False
        assert list(temp_root.iterdir()) == []
        assert "fallback_reason=video_download_timeout" in caplog.text
        assert "cn-temp-cookie-val" not in caplog.text

        # Mid-flight cancellation must STILL wipe temporary cookies and job directory.
        block_ytdlp.set()
        task = asyncio.create_task(
            collector.video_storyboard(
                {
                    "bvid": TEST_BVID,
                    "max_seconds": 120,
                    "cookies": {"sessdata": "cn-temp-cookie-val"},
                }
            )
        )
        await asyncio.wait_for(ytdlp_started.wait(), timeout=2.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert list(temp_root.iterdir()) == []
    finally:
        await collector.close()
        await client.aclose()


def _make_sprite_sheet_jpeg(
    cols: int, rows: int, cell_w: int, cell_h: int, colors: list[tuple[int, int, int]]
) -> bytes:
    sheet = Image.new("RGB", (cols * cell_w, rows * cell_h), (0, 0, 0))
    for idx, color in enumerate(colors[: cols * rows]):
        r, c = divmod(idx, cols)
        tile = Image.new("RGB", (cell_w, cell_h), color)
        sheet.paste(tile, (c * cell_w, r * cell_h))
    buf = io.BytesIO()
    sheet.save(buf, format="JPEG", quality=92)
    return buf.getvalue()


async def test_video_storyboard_videoshot_primary_long_video_and_multipart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    cell_colors = [
        (220, 30, 30),
        (30, 220, 30),
        (30, 30, 220),
        (220, 220, 30),
        (220, 30, 220),
        (30, 220, 220),
        (200, 120, 40),
        (120, 40, 200),
        (40, 200, 120),
    ]
    hi_res_sheet = _make_sprite_sheet_jpeg(3, 3, 480, 270, cell_colors)
    compact_sheet = _make_sprite_sheet_jpeg(10, 10, 160, 90, cell_colors * 12)
    bomb_buf = io.BytesIO()
    Image.new("RGB", (6000, 5000), (0, 0, 0)).save(bomb_buf, format="PNG")
    bomb_sheet = bomb_buf.getvalue()

    seen_requests: list[tuple[str, str, dict[str, str]]] = []
    mode: dict[str, str] = {"kind": "basic"}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append((request.method, str(request.url), dict(request.headers)))
        if request.url.host == "api.bilibili.com" and request.url.path == "/x/web-interface/view":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "bvid": TEST_BVID,
                        "pages": [
                            {"page": 1, "cid": 10001, "duration": 120},
                            {"page": 2, "cid": 20002, "duration": 245},
                        ],
                    },
                },
            )
        if request.url.host == "api.bilibili.com" and request.url.path == "/x/player/videoshot":
            if mode["kind"] == "basic":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "data": {
                            "img_x_len": 3,
                            "img_y_len": 3,
                            "img_x_size": 480,
                            "img_y_size": 270,
                            "image": ["//i0.hdslb.com/bfs/videoshot/basic_0.jpg"],
                            "index": [0, 5, 10, 15, 20, 25, 30, 35, 45],
                        },
                    },
                )
            if mode["kind"] == "long":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "data": {
                            "img_x_len": 10,
                            "img_y_len": 10,
                            "img_x_size": 160,
                            "img_y_size": 90,
                            "image": [
                                f"//i1.hdslb.com/bfs/videoshot/long_{i}.jpg" for i in range(20)
                            ],
                            "index": list(range(0, 2000)),
                        },
                    },
                )
            if mode["kind"] == "empty":
                return httpx.Response(200, json={"code": 0, "data": {"image": []}})
            if mode["kind"] == "bomb":
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "data": {
                            "img_x_len": 10,
                            "img_y_len": 10,
                            "img_x_size": 160,
                            "img_y_size": 90,
                            "image": ["https://i0.hdslb.com/bfs/videoshot/bomb.png"],
                            "index": [0, 5, 10, 15, 20, 25, 30, 35, 40],
                        },
                    },
                )
        if request.url.host in {"i0.hdslb.com", "i1.hdslb.com"}:
            if request.url.path.endswith("basic_0.jpg"):
                return httpx.Response(200, content=hi_res_sheet)
            if request.url.path.endswith("bomb.png"):
                return httpx.Response(200, content=bomb_sheet)
            return httpx.Response(200, content=compact_sheet)
        raise AssertionError(f"Unexpected request: {request.url}")

    temp_root = tmp_path / "videoshot_jobs"
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=temp_root, http_client=client)

    async def forbid_subprocess(
        argv: list[str], *, cwd: Path, env: dict[str, str], wait_limit: float
    ) -> tuple[int, bytes, bytes]:
        raise AssertionError(f"Subprocess must not run on videoshot path: {argv}")

    monkeypatch.setattr(collector, "_exec_bounded", forbid_subprocess)
    caplog.set_level(logging.INFO, logger="node_agent.bili")
    try:
        # 1. Primary 3x3 videoshot path: zero subprocesses, capped to 960x540, exact cell layout.
        res = await collector.video_storyboard(
            {"bvid": TEST_BVID, "cookies": {"sessdata": "api-only-secret-val"}}
        )
        assert res["bvid"] == TEST_BVID
        assert res["source"] == "videoshot"
        assert res["grid"] == "3x3"
        assert res["mime_type"] == "image/jpeg"
        assert res["duration"] == 45.0
        raw_jpeg = base64.b64decode(res["data_b64"])
        assert res["bytes"] == len(raw_jpeg)
        assert 1 <= res["bytes"] <= STORYBOARD_MAX_JPEG_BYTES
        assert not temp_root.exists()
        with Image.open(io.BytesIO(raw_jpeg)) as img:
            assert img.format == "JPEG"
            assert img.size == (960, 540)
            tl_r, tl_g, tl_b = img.getpixel((160, 90))
            br_r, br_g, br_b = img.getpixel((800, 450))
            assert tl_r > 180 and tl_g < 80 and tl_b < 80
            assert br_g > 160 and br_b > 80 and br_r < 90
        api_reqs = [r for r in seen_requests if "api.bilibili.com" in r[1]]
        cdn_reqs = [r for r in seen_requests if "hdslb.com" in r[1]]
        assert len(api_reqs) == 1 and "SESSDATA=api-only-secret-val" in api_reqs[0][2].get(
            "cookie", ""
        )
        assert len(cdn_reqs) == 1 and "cookie" not in {k.lower() for k in cdn_reqs[0][2]}

        # 2. Long >15min video with videoshot_only=True downloads at most 9 of 20 sprite sheets.
        seen_requests.clear()
        mode["kind"] = "long"
        long_res = await collector.video_storyboard(
            {
                "bvid": TEST_BVID,
                "duration": 1800,
                "videoshot_only": True,
            }
        )
        assert long_res["source"] == "videoshot"
        assert long_res["duration"] == 1800.0
        long_cdn_reqs = [r for r in seen_requests if "hdslb.com" in r[1]]
        assert 1 <= len(long_cdn_reqs) <= 9

        # 3. Multi-part with explicit cid skips /x/web-interface/view and passes cid to videoshot.
        seen_requests.clear()
        mode["kind"] = "basic"
        cid_res = await collector.video_storyboard({"bvid": TEST_BVID, "page": 2, "cid": 887766})
        assert cid_res["source"] == "videoshot"
        assert not any("/x/web-interface/view" in r[1] for r in seen_requests)
        vs_urls = [r[1] for r in seen_requests if "/x/player/videoshot" in r[1]]
        assert len(vs_urls) == 1 and "cid=887766" in vs_urls[0] and "index=1" in vs_urls[0]

        # 4. Multi-part without cid queries /x/web-interface/view for pages[page-1].cid & duration.
        seen_requests.clear()
        page_res = await collector.video_storyboard({"bvid": TEST_BVID, "page": 2})
        assert page_res["source"] == "videoshot"
        assert page_res["duration"] == 245.0
        assert any("/x/web-interface/view" in r[1] for r in seen_requests)
        vs_urls = [r[1] for r in seen_requests if "/x/player/videoshot" in r[1]]
        assert len(vs_urls) == 1 and "cid=20002" in vs_urls[0]

        # 5. videoshot_only=True fails cleanly without falling back to yt-dlp when empty.
        mode["kind"] = "empty"
        with pytest.raises(CollectorError, match="videoshot_unavailable"):
            await collector.video_storyboard({"bvid": TEST_BVID, "videoshot_only": True})
        assert "fallback_reason=videoshot_only" in caplog.text

        # 6. Decompression bomb sprite sheet is rejected safely.
        mode["kind"] = "bomb"
        with pytest.raises(CollectorError, match="videoshot_invalid_media"):
            await collector.video_storyboard({"bvid": TEST_BVID, "videoshot_only": True})
        assert "api-only-secret-val" not in caplog.text

        # 7. >15min fallback rejects fast before yt-dlp; invalid params raise invalid_params.
        mode["kind"] = "empty"
        with pytest.raises(CollectorError, match="video_download_duration"):
            await collector.video_storyboard(
                {"bvid": TEST_BVID, "duration": 1800, "max_seconds": 1800}
            )
        for bad_params in (
            {"bvid": TEST_BVID, "page": 0},
            {"bvid": TEST_BVID, "page": 1000},
            {"bvid": TEST_BVID, "cid": -1},
            {"bvid": TEST_BVID, "duration": float("nan")},
            {"bvid": TEST_BVID, "videoshot_only": "yes"},
        ):
            with pytest.raises(CollectorError, match="invalid_params"):
                await collector.video_storyboard(bad_params)
    finally:
        await collector.close()
        await client.aclose()


async def test_videoshot_url_ssrf_allowlist_and_cookie_isolation(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    assert (
        _validate_videoshot_url("//i0.hdslb.com/bfs/videoshot/demo.jpg")
        == "https://i0.hdslb.com/bfs/videoshot/demo.jpg"
    )
    assert (
        _validate_videoshot_url("https://i1.hdslb.com/bfs/videoshot/demo.jpg?auth_key=abc")
        == "https://i1.hdslb.com/bfs/videoshot/demo.jpg?auth_key=abc"
    )
    assert (
        _validate_videoshot_url("https://sub.i2.hdslb.com:443/bfs/videoshot/demo.jpg")
        == "https://sub.i2.hdslb.com:443/bfs/videoshot/demo.jpg"
    )
    assert (
        _validate_videoshot_url(
            "//bimp.hdslb.com/videoshotpvhdboss/1234567890_abcdef012345-0001.jpg"
        )
        == "https://bimp.hdslb.com/videoshotpvhdboss/1234567890_abcdef012345-0001.jpg"
    )
    assert (
        _validate_videoshot_url("//bimp.hdslb.com/videoshotpv/12345_hash-0002.webp")
        == "https://bimp.hdslb.com/videoshotpv/12345_hash-0002.webp"
    )
    assert (
        _validate_videoshot_url("https://bimp.hdslb.com/videoshot/sub/123_abc.png?auth_key=k1")
        == "https://bimp.hdslb.com/videoshot/sub/123_abc.png?auth_key=k1"
    )

    for bad_url in (
        "https://evil.com/bfs/videoshot/a.jpg",
        "//evil.com/bfs/videoshot/a.jpg",
        "https://evil.com/videoshotpvhdboss/123_abc-0001.jpg",
        "https://hdslb.com/bfs/videoshot/a.jpg",
        "https://evil-hdslb.com/bfs/videoshot/a.jpg",
        "https://i0.hdslb.com.evil.com/bfs/videoshot/a.jpg",
        "https://127.0.0.1/bfs/videoshot/a.jpg",
        "http://i0.hdslb.com/bfs/videoshot/a.jpg",
        "http://bimp.hdslb.com/videoshotpvhdboss/123_abc-0001.jpg",
        "https://i0.hdslb.com:8443/bfs/videoshot/a.jpg",
        "//bimp.hdslb.com:99999/videoshotpvhdboss/123_abc-0001.jpg",
        "https://user:pass@i0.hdslb.com/bfs/videoshot/a.jpg",
        "https://i0.hdslb.com/bfs/videoshot/a.jpg#frag",
        "https://i0.hdslb.com/other/videoshot/a.jpg",
        "https://bimp.hdslb.com/other/123_abc-0001.jpg",
        "https://i0.hdslb.com/bfs/",
        "https://bimp.hdslb.com/videoshotpvhdboss/",
        "https://bimp.hdslb.com/videoshotpvhdboss/secret.json",
        "https://bimp.hdslb.com/videoshotpvhdboss/../secret.jpg",
        "https://bimp.hdslb.com/videoshotpvhdboss/%2e%2e/secret.jpg",
        "https://bimp.hdslb.com/videoshotpvhdboss//123_abc-0001.jpg",
        "https://bimp.hdslb.com/videoshotpvhdboss/./123_abc-0001.jpg",
        "https://bimp.hdslb.com/videoshotpvhdboss/.jpg",
        "https://i0.hdslb.com/bfs/videoshot/../secret.jpg",
        "https://i0.hdslb.com/bfs/%2e%2e/secret.jpg",
    ):
        with pytest.raises(CollectorError, match="videoshot_host_forbidden"):
            _validate_videoshot_url(bad_url)

    seen_hosts: list[tuple[str, dict[str, str]]] = []
    secret_sess = "never-send-to-hdslb-cdn-991122"

    def evil_handler(request: httpx.Request) -> httpx.Response:
        seen_hosts.append((request.url.host, dict(request.headers)))
        if request.url.host == "api.bilibili.com":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "img_x_len": 3,
                        "img_y_len": 3,
                        "img_x_size": 160,
                        "img_y_size": 90,
                        "image": ["https://evil-hdslb.com/bfs/videoshot/pwn.jpg"],
                        "index": [0, 5, 10],
                    },
                },
            )
        raise AssertionError(f"SSRF target must never be requested: {request.url}")

    client = httpx.AsyncClient(transport=httpx.MockTransport(evil_handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "ssrf_jobs", http_client=client)
    caplog.set_level(logging.DEBUG, logger="node_agent.bili")
    try:
        with pytest.raises(CollectorError, match="videoshot_host_forbidden"):
            await collector.video_storyboard(
                {
                    "bvid": TEST_BVID,
                    "videoshot_only": True,
                    "cookies": {"sessdata": secret_sess},
                }
            )
        assert [host for host, _ in seen_hosts] == ["api.bilibili.com"]
        assert secret_sess not in caplog.text
    finally:
        await collector.close()
        await client.aclose()


async def test_classify_ytdlp_error_and_desensitized_fallback_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    assert (
        _classify_ytdlp_error("ERROR: unable to download video data: HTTP Error 567: EdgeOne")
        == "video_download_cdn_http_567"
    )
    assert _classify_ytdlp_error("HTTP 567 blocked") == "video_download_cdn_http_567"
    assert (
        _classify_ytdlp_error("ERROR: unable to download video data: HTTP Error 403: Forbidden")
        == "video_download_cdn_http_403"
    )
    assert _classify_ytdlp_error("HTTP 403 Forbidden") == "video_download_cdn_http_403"

    for risk_msg in (
        "API error -352",
        "API error -412",
        "v_voucher check failed",
        "HTTP Error 412: Precondition Failed",
        "HTTP Error 429: Too Many Requests",
        "captcha verification required",
        "rate limit exceeded",
    ):
        assert _classify_ytdlp_error(risk_msg) == "video_download_risk"

    assert _classify_ytdlp_error("API error -101") == "video_download_cookie_expired"
    assert _classify_ytdlp_error("Login required") == "video_download_cookie_expired"
    assert _classify_ytdlp_error("Read timed out") == "video_download_timeout"
    assert _classify_ytdlp_error("Connection timeout") == "video_download_timeout"
    assert (
        _classify_ytdlp_error("ERROR: Requested format is not available") == "video_download_format"
    )
    assert _classify_ytdlp_error("premium-only stream") == "video_download_restricted"
    assert _classify_ytdlp_error("File is larger than max-filesize") == "video_download_size"
    assert _classify_ytdlp_error("unexpected failure") == "video_download_unavailable"

    secret_sess = "super-secret-sessdata-must-not-log-7788"
    captured_argv: list[str] = []

    def fallback_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(
                200,
                json={"code": 0, "data": {"pages": [{"cid": 1}, {"cid": 2}, {"cid": 3}]}},
            )
        return httpx.Response(200, json={"code": -404, "data": None})

    client = httpx.AsyncClient(transport=httpx.MockTransport(fallback_handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "err_jobs", http_client=client)

    async def fake_fail_exec(
        argv: list[str], *, cwd: Path, env: dict[str, str], wait_limit: float
    ) -> tuple[int, bytes, bytes]:
        _ = cwd, env, wait_limit
        captured_argv.extend(argv)
        return (
            1,
            b"",
            f"ERROR: HTTP Error 567 while using SESSDATA={secret_sess}".encode(),
        )

    monkeypatch.setattr(collector, "_exec_bounded", fake_fail_exec)
    caplog.set_level(logging.INFO, logger="node_agent.bili")
    try:
        with pytest.raises(CollectorError, match="video_download_cdn_http_567"):
            await collector.video_storyboard(
                {
                    "bvid": TEST_BVID,
                    "page": 3,
                    "cookies": {"sessdata": secret_sess},
                }
            )
        assert f"https://www.bilibili.com/video/{TEST_BVID}?p=3" in captured_argv
        assert STORYBOARD_YTDLP_FORMAT in captured_argv
        assert "event=bili_videoshot_fallback" in caplog.text
        assert "event=bili_storyboard_failed" in caplog.text
        assert "fallback_reason=video_download_cdn_http_567" in caplog.text
        assert secret_sess not in caplog.text
    finally:
        await collector.close()
        await client.aclose()


async def test_daemon_hello_concurrent_nonblocking_ping_and_reconnect_backoff(
    tmp_path: Path,
) -> None:
    cfg = _make_settings(tmp_path)
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "jobs")
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    slow_count = 0

    async def slow_storyboard(params: dict[str, Any]) -> dict[str, Any]:
        nonlocal slow_count
        slow_count += 1
        if slow_count >= 4:
            slow_started.set()
        await release_slow.wait()
        return {
            "bvid": params.get("bvid", TEST_BVID),
            "duration": 10.0,
            "mime_type": "image/jpeg",
            "data_b64": base64.b64encode(b"jpeg").decode("ascii"),
            "bytes": 4,
            "grid": "3x3",
        }

    collector.video_storyboard = slow_storyboard  # type: ignore[method-assign]
    daemon = NodeDaemon(cfg, collector=collector, reconnect_base=0.05)

    connection_count = 0
    observed_headers: dict[str, str] = {}
    hello_msg: dict[str, Any] = {}
    ping_latency_s: float | None = None
    mc_results: dict[str, Any] = {}
    forbidden_res: dict[str, Any] = {}
    reconnected = asyncio.Event()

    async def ws_handler(ws: Any) -> None:
        nonlocal connection_count, observed_headers, hello_msg, ping_latency_s
        connection_count += 1
        observed_headers = {k.lower(): v for k, v in ws.request.headers.items()}
        hello_msg = json.loads(await ws.recv())

        if connection_count == 1:
            await ws.close()
            return

        # Saturate Semaphore(4) with 4 concurrent slow RPCs, then issue node.ping.
        for idx in range(4):
            await ws.send(
                json.dumps(
                    {
                        "type": "rpc_request",
                        "id": f"slow-{idx}",
                        "action": "bili.video_storyboard",
                        "params": {"bvid": TEST_BVID, "max_seconds": 60},
                    }
                )
            )
        await asyncio.wait_for(slow_started.wait(), timeout=2.0)

        t0 = time.monotonic()
        await ws.send(
            json.dumps({"type": "rpc_request", "id": "ping-1", "action": "node.ping", "params": {}})
        )
        ping_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
        ping_latency_s = time.monotonic() - t0
        assert ping_resp["id"] == "ping-1"
        assert ping_resp["ok"] is True
        assert ping_resp["data"]["pong"] is True

        release_slow.set()
        slow_resps = [json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0)) for _ in range(4)]
        assert {r["id"] for r in slow_resps} == {f"slow-{i}" for i in range(4)}
        assert all(r["ok"] is True for r in slow_resps)

        await ws.send(
            json.dumps({"type": "rpc_request", "id": "mc-1", "action": "mc.pause", "params": {}})
        )
        mc_results["pause"] = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
        await ws.send(
            json.dumps(
                {"type": "rpc_request", "id": "mc-2", "action": "mc.dispatch_goal", "params": {}}
            )
        )
        mc_results["goal"] = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0))
        await ws.send(
            json.dumps({"type": "rpc_request", "id": "bad-1", "action": "shell.exec", "params": {}})
        )
        forbidden_res.update(json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0)))
        reconnected.set()
        await ws.wait_closed()

    async with serve(ws_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        daemon.settings = _make_settings(
            tmp_path,
            node_ws_url=f"ws://127.0.0.1:{port}/ws/node/0123456789abcdef01234567",
        )
        run_task = asyncio.create_task(daemon.run())
        try:
            await asyncio.wait_for(reconnected.wait(), timeout=5.0)
            assert connection_count >= 2
            assert observed_headers["authorization"] == f"Bearer {STRONG_TOKEN}"
            assert observed_headers["x-node-id"] == "shanghai-cn-1"
            assert observed_headers["x-node-region"] == "CN"
            assert hello_msg["type"] == "event"
            assert hello_msg["event"] == "node.hello"
            assert hello_msg["node_id"] == "shanghai-cn-1"
            assert hello_msg["region"] == "CN"
            assert hello_msg["payload"]["version"] == "2.1.0"
            assert hello_msg["payload"]["capabilities"] == [
                "bili.public_get",
                "bili.resolve_short",
                "bili.request",
                "bili.cover_thumb",
                "bili.video_storyboard",
                "mc.slot",
            ]
            assert ping_latency_s is not None and ping_latency_s < 0.05
            assert mc_results["pause"]["data"]["paused"] is True
            assert mc_results["goal"]["data"]["reason"] == "mc_paused"
            assert forbidden_res["ok"] is False
            assert forbidden_res["error"] == "node_action_forbidden"
            assert len(daemon.backoff_history) >= 1
        finally:
            await daemon.stop()
            await asyncio.wait_for(run_task, timeout=3.0)


def test_compute_backoff_progression_and_frame_decoder() -> None:
    rng = random.Random(123)
    delay = 1.0
    next_delays: list[float] = []
    for _ in range(7):
        sleep_for, delay = compute_backoff_step(delay, max_delay=30.0, rng=rng)
        assert sleep_for <= 30.0
        next_delays.append(delay)
    assert next_delays == [2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]

    for bad_frame in ('{"a":1,"a":2}', '{"x":NaN}', "[1,2]", "x" * 101):
        with pytest.raises(CollectorError):
            decode_node_frame(bad_frame, 100)


def test_packaging_hardening_invariants() -> None:
    repo = Path(__file__).resolve().parents[1]
    root_df = (repo / "Dockerfile").read_text(encoding="utf-8").splitlines()
    node_df = (repo / "node_agent/Dockerfile").read_text(encoding="utf-8").splitlines()
    assert root_df[0] == node_df[0]
    assert root_df[1] == node_df[1]
    assert "USER 10001:10001" in node_df
    assert 'CMD ["python", "-m", "node_agent.daemon"]' in node_df
    assert "COPY node_agent ./node_agent" in node_df

    compose_text = (repo / "node_agent/compose.cn.yaml").read_text(encoding="utf-8")
    for expected in (
        "image: hakimi-node-agent:v2.1.0",
        "env_file: .env.node",
        "read_only: true",
        "/tmp:size=64m,uid=10001,gid=10001,mode=700,noexec,nosuid,nodev",
        "- ALL",
        "no-new-privileges:true",
        "init: true",
        "mem_limit: 256m",
        "cpus: 0.5",
        "pids_limit: 64",
        "restart: unless-stopped",
    ):
        assert expected in compose_text

    env_example = (repo / "node_agent/.env.node.example").read_text(encoding="utf-8")
    assert "REPLACE_WITH_48_RANDOM_HEX_CHARS" in env_example
    assert "GENERATE_32+_CHAR_RANDOM_SECRET_TOKEN_BEFORE_ENABLE" in env_example
    if live_tok := os.environ.get("NODE_WS_TOKEN"):
        assert live_tok not in env_example
    with pytest.raises(ValidationError):
        NodeAgentSettings(_env_file=repo / "node_agent/.env.node.example")


async def test_bili_collector_subtitle_invalid_port_rejected_cleanly(tmp_path: Path) -> None:
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _req: httpx.Response(200, json={"body": []}))
    )
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "jobs", http_client=http)
    try:
        with pytest.raises(CollectorError) as exc_info:
            await collector.request(
                {
                    "method": "GET",
                    "path": "https://aisubtitle.hdslb.com:99999/bfs/ai_subtitle/demo.json",
                    "subtitle": True,
                }
            )
        assert exc_info.value.code == "bili_subtitle_host_forbidden"
    finally:
        await collector.close()
        await http.aclose()


async def test_videoshot_bimp_videoshotpvhdboss_end_to_end(tmp_path: Path) -> None:
    """Real Bilibili videoshot returns //bimp.hdslb.com/videoshotpvhdboss/<cid>_<hash>-0001.jpg."""
    colors = [(220, 40, 40), (40, 220, 40), (40, 40, 220)] * 34
    hd_sprite = _make_sprite_sheet_jpeg(10, 10, 480, 270, colors)
    seen: list[tuple[str, str, dict[str, str]]] = []
    secret_sess = "live-bimp-secret-sessdata"

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.url.path, dict(request.headers)))
        if request.url.host == "api.bilibili.com" and request.url.path == "/x/player/videoshot":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "img_x_len": 10,
                        "img_y_len": 10,
                        "img_x_size": 480,
                        "img_y_size": 270,
                        "image": [
                            "//bimp.hdslb.com/videoshotpvhdboss/29564123456_a1b2c3d4e5-0001.jpg"
                        ],
                        "index": list(range(0, 400, 5)),
                    },
                },
            )
        if request.url.host == "bimp.hdslb.com":
            return httpx.Response(200, content=hd_sprite, headers={"content-type": "image/jpeg"})
        raise AssertionError(f"Unexpected request: {request.url}")

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = BiliCollector(min_interval=0.0, temp_dir=tmp_path / "bimp_jobs", http_client=http)
    try:
        res = await collector.video_storyboard(
            {
                "bvid": TEST_BVID,
                "videoshot_only": True,
                "cookies": {"sessdata": secret_sess},
            }
        )
        assert res["bvid"] == TEST_BVID
        assert res["source"] == "videoshot"
        assert res["grid"] == "3x3"
        assert res["mime_type"] == "image/jpeg"
        img_bytes = base64.b64decode(res["data_b64"])
        assert res["bytes"] == len(img_bytes)
        assert 1 <= len(img_bytes) <= STORYBOARD_MAX_JPEG_BYTES
        with Image.open(io.BytesIO(img_bytes)) as img:
            assert img.format == "JPEG"
            assert img.size == (960, 540)
        cdn_reqs = [item for item in seen if item[0] == "bimp.hdslb.com"]
        assert len(cdn_reqs) == 1
        assert cdn_reqs[0][1] == "/videoshotpvhdboss/29564123456_a1b2c3d4e5-0001.jpg"
        assert "cookie" not in {k.lower() for k in cdn_reqs[0][2]}
    finally:
        await collector.close()
        await http.aclose()
