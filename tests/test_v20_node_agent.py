"""Unit tests for the Phase 3 standalone Shanghai CN node_agent."""

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
    BiliCollector,
    CollectorError,
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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temp_root = tmp_path / "node_bili_jobs"
    stale_dir = temp_root / ("job-" + "a" * 32)
    stale_dir.mkdir(parents=True)
    (stale_dir / "leftover.txt").write_text("stale", encoding="utf-8")

    collector = BiliCollector(min_interval=0.0, temp_dir=temp_root)
    observed: dict[str, Any] = {"cookie_mode": None, "cookie_wiped_before_ffprobe": False}
    seen_ytdlp_argv: list[str] = []
    block_ytdlp = asyncio.Event()
    ytdlp_started = asyncio.Event()

    async def fake_exec(
        argv: list[str], *, cwd: Path, env: dict[str, str], wait_limit: float
    ) -> tuple[int, bytes, bytes]:
        _ = wait_limit
        assert "NODE_WS_TOKEN" not in env
        tool = Path(argv[0]).name
        if "yt-dlp" in tool or "yt_dlp" in " ".join(argv):
            seen_ytdlp_argv.extend(argv)
            cookie_file = cwd / "cookies.txt"
            assert cookie_file.exists()
            observed["cookie_mode"] = stat.S_IMODE(cookie_file.lstat().st_mode)
            assert "SESSDATA\tcn-temp-cookie-val" in cookie_file.read_text(encoding="utf-8")
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
        assert "worst[ext=mp4]/mp4" in seen_ytdlp_argv
        assert observed["cookie_mode"] == 0o600
        assert observed["cookie_wiped_before_ffprobe"] is True

        raw_jpeg = base64.b64decode(sb["data_b64"])
        assert sb["bvid"] == TEST_BVID
        assert sb["duration"] == 900.0
        assert sb["mime_type"] == "image/jpeg"
        assert sb["grid"] == "3x3"
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
