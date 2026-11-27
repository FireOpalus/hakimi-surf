"""Regression tests for Wave 3+4 remote tool plugins and voice gateway/client hardening."""

from __future__ import annotations

import asyncio
import gc
import json
import os
import sys
import tempfile
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import ImageFont

from hakimi.text_card import _mixed_width as tc_mixed_width
from hakimi.text_card import _tokenize_inline as tc_tokenize_inline
from hakimi.text_card import _wrap_line as tc_wrap_line
from hakimi.text_card import _wrap_rich_lines as tc_wrap_rich_lines
from hakimi_plugin_sdk import PluginRpcError, PluginRuntimeConfig
from plugins.bilibili.patrol import BiliPatrolEngine
from plugins.bilibili.plugin_main import BilibiliPlugin
from plugins.doc_render.plugin_main import _RENDER_CARD_ACTION_SCHEMA, DocRenderPlugin
from plugins.doc_render.renderer import (
    MAX_INPUT_CHARS,
    _mixed_width,
    _tokenize_inline,
    _wrap_line,
    _wrap_rich_lines,
    render_card_bytes,
)
from plugins.minecraft.play_brain import McPlayBrain
from plugins.song_finder.plugin_main import SongFinderPlugin
from plugins.song_finder.providers import (
    BilibiliPublicProvider,
    ProviderRiskControlError,
    SongCandidate,
    SongFinderError,
    _run_exec,
    transcode_clean_audio,
)
from plugins.web_tools.plugin_main import WebToolsPlugin
from plugins.web_tools.preview import preview_urls
from plugins.web_tools.search import (
    SearchProviderConfig,
    clear_deepseek_tools_search_cache,
    execute_search,
)
from plugins.web_tools.ssrf import (
    PinnedPublicTransport,
    build_pinned_client,
    get_default_ssl_context,
)
from voice_client.audio import (
    AfplayAudioPlayer,
    BaseQueuedAudioPlayer,
    FfmpegMicCapture,
    RawStreamAudioPlayer,
)
from voice_client.providers import (
    BreezeBlueTTS,
    EdgeFallbackTTS,
    FishAudioTTS,
    ProviderError,
    SiliconFlowTTS,
    _make_default_http_client,
)
from voice_gateway.app import create_gateway_app
from voice_gateway.audio_codec import probe_audio_duration_ms, transcode_audio_stream
from voice_gateway.config import VoiceGatewayConfig
from voice_gateway.session import VoiceGatewaySession

GATEWAY_TOKEN = "wave3-voice-gateway-token-32chars!!"
GATEWAY_PATH = "/ws/voice/wave30123456789abcdef012345678"


def _gw_config(**overrides: Any) -> VoiceGatewayConfig:
    defaults: dict[str, Any] = {
        "ws_path": GATEWAY_PATH,
        "gateway_token": GATEWAY_TOKEN,
        "default_output_format": "mp3",
        "ffmpeg_bin": "/nonexistent-ffmpeg",
        "stt_provider": "siliconflow",
        "tts_provider": "siliconflow",
        "siliconflow_api_key": "sk-wave3-test-secret-123456",
        "tts_streaming": True,
        "ping_interval_seconds": 0.0,
    }
    defaults.update(overrides)
    return VoiceGatewayConfig(**defaults)


# ---------------------------------------------------------------------------
# 1. song_finder Wave 3 fixes
# ---------------------------------------------------------------------------


def test_song_finder_fetch_slots_is_single_concurrency() -> None:
    plugin = SongFinderPlugin(ws_url="ws://127.0.0.1:12141/ws/plugin", token="tok-sf-1")
    assert plugin._fetch_slots._value == 1  # noqa: SLF001


async def test_song_finder_unlinks_input_before_reading_transcoded_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import plugins.song_finder.providers as sf_prov

    ffprobe_bin = tmp_path / "ffprobe"
    ffprobe_bin.write_text(f"#!{sys.executable}\nprint('180.0')\n", encoding="utf-8")
    os.chmod(ffprobe_bin, 0o755)

    ffmpeg_bin = tmp_path / "ffmpeg"
    ffmpeg_bin.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import pathlib, sys",
                "out = pathlib.Path(sys.argv[-1])",
                "out.write_bytes(b'M4A_CLEAN_' + b'A' * 16000)",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(ffmpeg_bin, 0o755)

    raw_input = tmp_path / "bili_BV1test.m4s"
    raw_input.write_bytes(b"RAW_INPUT" * 2000)

    input_existed_at_read: list[bool] = []
    real_read = sf_prov._read_file_if_bounded

    def _spy_read(path: Path, *, min_bytes: int, max_bytes: int) -> bytes | None:
        input_existed_at_read.append(raw_input.exists())
        return real_read(path, min_bytes=min_bytes, max_bytes=max_bytes)

    monkeypatch.setattr(sf_prov, "_read_file_if_bounded", _spy_read)

    data, dur, _digest = await transcode_clean_audio(
        raw_input,
        tmp_path,
        output_format="m4a",
        ffmpeg_bin=str(ffmpeg_bin),
        ffprobe_bin=str(ffprobe_bin),
    )
    assert len(data) > 8192
    assert dur == 180.0
    assert input_existed_at_read == [False]
    assert not raw_input.exists()


async def test_song_finder_run_exec_suppresses_process_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _GhostProc:
        returncode = -9

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.sleep(5.0)
            return b"", b""

        def kill(self) -> None:
            raise ProcessLookupError("already exited")

        async def wait(self) -> int:
            return -9

    async def _fake_exec(*_args: Any, **_kwargs: Any) -> _GhostProc:
        return _GhostProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    with pytest.raises(SongFinderError, match="subprocess timed out"):
        await _run_exec(["fake-bin"], timeout_sec=0.05, error_code="transcode_failed")

    task = asyncio.create_task(_run_exec(["fake-bin"], timeout_sec=5.0, error_code="x"))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_song_finder_bili_412_clears_poisoned_buvid_cookie() -> None:
    spi_ok = [True]
    seen_cookies: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        cookie = request.headers.get("cookie", "")
        seen_cookies.append((path, cookie))
        if path == "/x/frontend/finger/spi":
            if spi_ok[0]:
                return httpx.Response(
                    200,
                    json={
                        "code": 0,
                        "data": {"b_3": "POISON_B3_123456", "b_4": "POISON_B4_7890"},
                    },
                )
            return httpx.Response(503)
        if path == "/x/web-interface/nav":
            return httpx.Response(412)
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as http:
        prov = BilibiliPublicProvider(http=http)
        cookie = await prov._ensure_anon_buvid()
        assert "POISON_B3_123456" in cookie

        with pytest.raises(ProviderRiskControlError):
            await prov._get_json(prov._NAV_URL)
        assert prov._buvid_cookie == ""
        assert prov._buvid_expires_at == 0.0

        # Even if SPI refresh fails, the poisoned cookie must never be re-sent!
        spi_ok[0] = False
        refreshed = await prov._ensure_anon_buvid()
        assert refreshed == ""
        with pytest.raises(ProviderRiskControlError):
            await prov._get_json(prov._NAV_URL)
        assert seen_cookies[-1] == ("/x/web-interface/nav", "")


async def test_song_finder_bili_download_timeout_and_offloaded_chunk_writes(
    tmp_path: Path,
) -> None:
    cand = SongCandidate(
        provider="bilibili_public",
        source_id="BV1Q541167Qg",
        title="晴天",
        uploader="周杰伦",
        duration_sec=240.0,
        url="https://www.bilibili.com/video/BV1Q541167Qg",
    )

    class _SlowBodyStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"A" * 4096
            await asyncio.sleep(5.0)
            yield b"B" * 4096

        async def aclose(self) -> None:
            return None

    class _FastChunkStream(httpx.AsyncByteStream):
        def __init__(self, chunks: list[bytes]) -> None:
            self._chunks = chunks

        async def __aiter__(self) -> AsyncIterator[bytes]:
            for c in self._chunks:
                yield c

        async def aclose(self) -> None:
            return None

    def _meta_response(request: httpx.Request) -> httpx.Response | None:
        if request.url.path == "/x/frontend/finger/spi":
            return httpx.Response(200, json={"code": 0, "data": {}})
        if request.url.path == "/x/web-interface/nav":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "wbi_img": {
                            "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
                            "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
                        }
                    },
                },
            )
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {"cid": 12345, "duration": 240}})
        if request.url.path == "/x/player/wbi/playurl":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "dash": {
                            "audio": [
                                {
                                    "bandwidth": 128000,
                                    "baseUrl": "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/a.m4s",
                                }
                            ]
                        }
                    },
                },
            )
        return None

    # 1. Wall-clock timeout aborts stalled CDN stream and cleans up partial file
    def slow_handler(request: httpx.Request) -> httpx.Response:
        meta = _meta_response(request)
        if meta is not None:
            return meta
        return httpx.Response(206, stream=_SlowBodyStream())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(slow_handler), follow_redirects=False
    ) as http:
        prov = BilibiliPublicProvider(http=http, download_timeout_sec=0.12)
        t0 = time.monotonic()
        with pytest.raises(SongFinderError, match="download timed out"):
            await prov.download(cand, tmp_path)
        assert time.monotonic() - t0 < 1.0
        assert not (tmp_path / "bili_BV1Q541167Qg.m4s").exists()

    # 2. Normal multi-chunk download writes off the event loop with exact byte integrity
    # 320 KiB -> triggers buffered flush + final finish
    expected_payload = (b"CHUNK0123456789!" * 4096) * 5
    chunks = [expected_payload[i : i + 65536] for i in range(0, len(expected_payload), 65536)]

    def fast_handler(request: httpx.Request) -> httpx.Response:
        meta = _meta_response(request)
        if meta is not None:
            return meta
        return httpx.Response(206, stream=_FastChunkStream(chunks))

    main_tid = threading.get_ident()
    writer_tids: list[int] = []
    import plugins.song_finder.providers as sf_prov

    orig_flush = sf_prov._AsyncBufferedFileWriter._flush_locked

    def _spy_flush(self_w: Any, data: bytes) -> None:
        writer_tids.append(threading.get_ident())
        orig_flush(self_w, data)

    sf_prov._AsyncBufferedFileWriter._flush_locked = _spy_flush  # type: ignore[method-assign]
    try:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(fast_handler), follow_redirects=False
        ) as http:
            prov = BilibiliPublicProvider(http=http, download_timeout_sec=5.0)
            out_path = await prov.download(cand, tmp_path)
            assert out_path.read_bytes() == expected_payload
            assert writer_tids and all(tid != main_tid for tid in writer_tids)
    finally:
        sf_prov._AsyncBufferedFileWriter._flush_locked = orig_flush  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# 2. web_tools Wave 3 fixes
# ---------------------------------------------------------------------------


async def test_web_tools_tls_reuse_nonclosing_transport_and_dns_connect_timeout() -> None:
    ctx1 = get_default_ssl_context()
    ctx2 = get_default_ssl_context()
    assert ctx1 is ctx2

    plugin = WebToolsPlugin(ws_url="ws://127.0.0.1:12141/ws/plugin", token="tok-wt")
    try:
        assert isinstance(plugin._search_transport, PinnedPublicTransport)
        assert isinstance(plugin._preview_transport, PinnedPublicTransport)
        assert plugin._search_transport._ssl_context is ctx1
        assert plugin._preview_transport._ssl_context is ctx1
        # Verify keepalive remains disabled (0) for anti-rebinding safety.
        assert plugin._search_transport._inner._pool._max_keepalive_connections == 0
        assert plugin._preview_transport._inner._pool._max_keepalive_connections == 0
    finally:
        await plugin.close()

    # Shared PinnedPublicTransport must NOT be closed when build_pinned_client exits.
    closed_calls = 0

    class _TrackedInner(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            assert request.headers.get("connection") == "close"
            return httpx.Response(200, text="ok")

        async def aclose(self) -> None:
            nonlocal closed_calls
            closed_calls += 1

    async def _fast_dns(_host: str, _port: int) -> list[str]:
        return ["93.184.216.34"]

    shared = PinnedPublicTransport(inner=_TrackedInner(), dns_resolver=_fast_dns)
    for _ in range(2):
        async with build_pinned_client(timeout=2.0, transport=shared) as client:
            resp = await client.get("https://example.org/hello")
            assert resp.status_code == 200
    assert closed_calls == 0

    # DNS resolution respects request connect_timeout and raises httpx.ConnectTimeout.
    async def _hanging_dns(_host: str, _port: int) -> list[str]:
        await asyncio.sleep(5.0)
        return ["93.184.216.34"]

    slow_transport = PinnedPublicTransport(inner=_TrackedInner(), dns_resolver=_hanging_dns)
    t0 = time.monotonic()
    async with httpx.AsyncClient(
        transport=slow_transport,
        timeout=httpx.Timeout(2.0, connect=0.1),
        trust_env=False,
    ) as client:
        with pytest.raises(httpx.ConnectTimeout):
            await client.get("https://slow-dns.example.org/test")
    assert time.monotonic() - t0 < 0.8


async def test_web_tools_search_failover_caps_primary_timeout() -> None:
    calls: list[str] = []

    class _PrimaryHangsFallbackOk(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            host = request.url.host
            calls.append(host)
            if host == "api.deepseek.com":
                await asyncio.sleep(5.0)
                return httpx.Response(200, json={"results": []})
            if host == "api.tavily.com":
                return httpx.Response(
                    200,
                    json={
                        "answer": "Fallback Tavily succeeded promptly.",
                        "results": [
                            {
                                "title": "Fallback Source",
                                "url": "https://fallback.example.org/doc",
                                "content": "verified fact",
                            }
                        ],
                    },
                )
            return httpx.Response(404)

    cfg = SearchProviderConfig(
        provider_order=("deepseek", "tavily"),
        deepseek_api_key="sk-ds-hang-123456",
        tavily_api_key="tvly-fast-123456",
    )
    t0 = time.monotonic()
    res = await execute_search(
        "failover timeout cap test",
        cfg,
        provider="auto",
        timeout_seconds=1.5,
        transport=_PrimaryHangsFallbackOk(),
    )
    elapsed = time.monotonic() - t0
    assert res["provider"] == "tavily"
    assert "Fallback Tavily" in res["text"]
    assert calls == ["api.deepseek.com", "api.tavily.com"]
    # Primary got bounded slice (~0.9s), leaving budget for Tavily within 1.5s.
    assert elapsed < 1.4


async def test_web_tools_deepseek_404_cached_with_ttl() -> None:
    seen_paths: list[str] = []
    base = "https://ds-cache-test.example.org"

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path == "/v1/tools/search":
            return httpx.Response(404)
        if request.url.path == "/chat/completions":
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "content": "DeepSeek chat fallback answer.",
                                "annotations": [],
                            }
                        }
                    ]
                },
            )
        return httpx.Response(404)

    cfg = SearchProviderConfig(
        deepseek_api_key="sk-ds-cache-123456",
        deepseek_base_url=base,
    )
    clear_deepseek_tools_search_cache()
    try:
        transport = httpx.MockTransport(handler)
        r1 = await execute_search("query one", cfg, provider="deepseek", transport=transport)
        r2 = await execute_search("query two", cfg, provider="deepseek", transport=transport)
        assert "DeepSeek chat fallback" in r1["text"]
        assert "DeepSeek chat fallback" in r2["text"]
        # First call probes /v1/tools/search -> 404 -> /chat/completions;
        # second call skips /v1/tools/search straight to /chat/completions!
        assert seen_paths == ["/v1/tools/search", "/chat/completions", "/chat/completions"]
    finally:
        clear_deepseek_tools_search_cache()


async def test_web_tools_preview_keeps_fast_sibling_when_other_times_out() -> None:
    class _MixedSpeedTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.url.host == "fast.example.org":
                return httpx.Response(
                    200,
                    headers={"Content-Type": "text/html; charset=utf-8"},
                    content=b"<html><head><title>Fast Page</title></head></html>",
                )
            await asyncio.sleep(5.0)
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html; charset=utf-8"},
                content=b"<html><head><title>Too Late</title></head></html>",
            )

    t0 = time.monotonic()
    outcome = await preview_urls(
        ["https://fast.example.org/ok", "https://slow.example.org/hang"],
        budget_seconds=0.25,
        transport=_MixedSpeedTransport(),
    )
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0
    assert outcome.total_urls == 2
    assert len(outcome.previews) == 1
    assert outcome.previews[0]["title"] == "Fast Page"


# ---------------------------------------------------------------------------
# 3. doc_render & text_card Wave 3 fixes
# ---------------------------------------------------------------------------


def _ref_wrap_line(text: str, max_width: float, font_ascii: Any, font_cjk: Any) -> list[str]:
    if not text:
        return [""]
    lines: list[str] = []
    cur = ""
    for ch in text:
        if cur and _mixed_width(cur + ch, font_ascii, font_cjk) > max_width:
            lines.append(cur)
            cur = ch
        else:
            cur += ch
    lines.append(cur)
    return lines


def _ref_wrap_rich_lines(
    text: str,
    max_width: float,
    font_text: Any,
    font_bold: Any,
    font_code_ascii: Any,
    font_code_cjk: Any,
) -> list[list[tuple[str, str, float]]]:
    lines: list[list[tuple[str, str, float]]] = [[]]
    cur_width = 0.0

    def seg_width(kind: str, chunk: str) -> float:
        if kind == "code":
            return _mixed_width(chunk, font_code_ascii, font_code_cjk) + 12
        font = font_bold if kind == "bold" else font_text
        return font.getlength(chunk)

    for para_idx, para in enumerate(text.splitlines() or [""]):
        if para_idx > 0:
            lines.append([])
            cur_width = 0.0
        for kind, content in _tokenize_inline(para):
            if kind == "code":
                total_w = seg_width("code", content)
                if cur_width and cur_width + total_w > max_width:
                    lines.append([])
                    cur_width = 0.0
                if total_w <= max_width:
                    lines[-1].append(("code", content, total_w))
                    cur_width += total_w
                    continue
                for wrapped in _ref_wrap_line(
                    content, max(80.0, max_width - 14), font_code_ascii, font_code_cjk
                ):
                    w = seg_width("code", wrapped)
                    if lines[-1]:
                        lines.append([])
                    lines[-1].append(("code", wrapped, w))
                    cur_width = w
                continue
            buf = ""
            font = font_bold if kind == "bold" else font_text
            for ch in content:
                cw = font.getlength(ch)
                if cur_width + font.getlength(buf) + cw > max_width and (buf or lines[-1]):
                    if buf:
                        lines[-1].append((kind, buf, font.getlength(buf)))
                    lines.append([])
                    cur_width = 0.0
                    buf = ch
                else:
                    buf += ch
            if buf:
                bw = font.getlength(buf)
                lines[-1].append((kind, buf, bw))
                cur_width += bw
    return lines


def test_doc_render_and_text_card_wrapping_parity_and_linear_speed() -> None:
    font_a = ImageFont.load_default(size=18)
    font_b = ImageFont.load_default(size=20)
    assert _RENDER_CARD_ACTION_SCHEMA["properties"]["text"]["max_length"] == MAX_INPUT_CHARS == 8000

    samples = [
        "",
        "hello",
        "中文混排 English 1234567890 测试换行边界！",
        "WWWWWWWWWWWWWWWWWWWW",
        "Prefix `inline_code_segment` **bold text here** and trailing plain text across lines.",
        "First line\nSecond line with **bold** and `very_long_inline_code_token_0123456789abcdef`!",
    ]
    for sample in samples:
        for max_w in (10.0, 60.0, 180.0, 500.0):
            expected_plain = _ref_wrap_line(sample, max_w, font_a, font_b)
            assert _wrap_line(sample, max_w, font_a, font_b) == expected_plain
            assert tc_wrap_line(sample, max_w, font_a, font_b) == expected_plain

            expected_rich = _ref_wrap_rich_lines(sample, max_w, font_b, font_b, font_a, font_b)
            assert _wrap_rich_lines(sample, max_w, font_b, font_b, font_a, font_b) == expected_rich
            assert (
                tc_wrap_rich_lines(sample, max_w, font_b, font_b, font_a, font_b) == expected_rich
            )
            # Touch imported helpers to confirm consistent symbols across both modules
            assert tc_tokenize_inline(sample) == _tokenize_inline(sample)
            assert tc_mixed_width(sample, font_a, font_b) == _mixed_width(sample, font_a, font_b)

    # 8000-char worst-case single-line input completes far below O(L^2) time.
    long_text = "A中文12345" * 1000
    assert len(long_text) == 8000
    t0 = time.monotonic()
    wrapped_wide = _wrap_line(long_text, 1_000_000.0, font_a, font_b)
    wrapped_normal = _wrap_line(long_text, 240.0, font_a, font_b)
    rich_wide = _wrap_rich_lines(long_text, 1_000_000.0, font_b, font_b, font_a, font_b)
    elapsed = time.monotonic() - t0
    assert wrapped_wide == [long_text]
    assert "".join(wrapped_normal) == long_text
    assert len(rich_wide) == 1
    assert elapsed < 0.5


async def test_doc_render_deadline_and_semaphore_tracks_running_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import plugins.doc_render.plugin_main as pm

    # Direct expired deadline aborts promptly with TimeoutError.
    with pytest.raises(TimeoutError):
        render_card_bytes("# Expired", deadline=time.monotonic() - 1.0)

    started = threading.Event()
    release = threading.Event()
    seen_deadlines: list[float | None] = []

    def _blocked_render(*_args: Any, **kwargs: Any) -> Any:
        seen_deadlines.append(kwargs.get("deadline"))
        started.set()
        release.wait(timeout=3.0)
        raise TimeoutError("worker aborted after deadline")

    monkeypatch.setattr(pm, "render_card_bytes", _blocked_render)
    plugin = DocRenderPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token="tok-doc-sem",
        render_timeout_seconds=0.06,
        max_workers=1,
    )
    loop = asyncio.get_running_loop()
    loop_exceptions: list[dict[str, Any]] = []
    prev_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: loop_exceptions.append(ctx))
    try:
        with pytest.raises(PluginRpcError) as exc_info:
            await plugin._handle_render_card({"arguments": {"text": "hello"}})
        assert exc_info.value.code == "render_failed"
        assert started.is_set()
        assert seen_deadlines and seen_deadlines[0] is not None
        # Semaphore MUST remain locked while the worker thread is still running!
        assert plugin._sem.locked() is True

        release.set()
        for _ in range(100):
            if not plugin._sem.locked():
                break
            await asyncio.sleep(0.01)
        assert plugin._sem.locked() is False
        # Let detached asyncio.Future settle and collect; must NOT log unretrieved exception.
        await asyncio.sleep(0.02)
        gc.collect()
        await asyncio.sleep(0)
        assert loop_exceptions == []
    finally:
        loop.set_exception_handler(prev_handler)
        release.set()
        await plugin.close()


# ---------------------------------------------------------------------------
# 4. bilibili & minecraft/play_brain Wave 3 fixes
# ---------------------------------------------------------------------------


async def test_bilibili_plugin_pool_limit_and_patrol_401_vs_403() -> None:
    cfg = PluginRuntimeConfig(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        plugin_id="bilibili_surf",
        token="tok-bili",
        display_name="B 站巡逻",
    )
    plugin = BilibiliPlugin(cfg)
    try:
        assert plugin.http._transport._pool._max_connections == 6
    finally:
        await plugin.close()

    now_box = [1_700_000_000.0]
    status_box = [401]

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.host == "aisubtitle.hdslb.com":
            return httpx.Response(status_box[0], text="sub err")
        return httpx.Response(status_box[0], text="api err")

    class _MiniFakeClient:
        def __init__(self) -> None:
            self.kv: dict[str, Any] = {
                "policy": {"enabled": True},
                "credentials": {
                    "creds": {
                        "sessdata": "s-cookie",
                        "bili_jct": "j" * 32,
                        "buvid3": "b3",
                        "buvid4": "b4",
                    },
                    "source": "sqlite",
                },
            }
            self.notifications: list[tuple[str, str]] = []
            self.connected = True

        async def state_get(self, key: str) -> Any:
            return self.kv.get(key)

        async def state_set(self, key: str, value: Any) -> None:
            self.kv[key] = value

        async def push_status(self, _status: dict[str, Any], **_kw: Any) -> None:
            return None

        async def notify_owner(self, key: str, text: str) -> None:
            self.notifications.append((key, text))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        engine = BiliPatrolEngine(
            _MiniFakeClient(),  # type: ignore[arg-type]
            http,
            clock=lambda: now_box[0],
            sleep=lambda _: asyncio.sleep(0),
        )
        await engine.initialize()

        # Authenticated API HTTP 401 -> bili_cookie_expired + 12h cooldown
        status_box[0] = 401
        with pytest.raises(Exception, match="bili_cookie_expired"):
            await engine._request("/x/web-interface/view", {"bvid": "BV1Q541167Qg"})
        assert engine.blocked_until == now_box[0] + 43200

        # HTTP 403 remains decoupled from 12h cooldown (preserving CDN/edge decoupling)
        engine.blocked_until = 0.0
        status_box[0] = 403
        with pytest.raises(Exception, match="bili_account_unavailable"):
            await engine._request("/x/web-interface/view", {"bvid": "BV1Q541167Qg"})
        assert engine.blocked_until == 0.0

        # Subtitle HTTP 401 never triggers 12h cooldown
        status_box[0] = 401
        with pytest.raises(Exception, match="bili_account_unavailable"):
            await engine._request(
                "https://aisubtitle.hdslb.com/bfs/subtitle/test.json", subtitle=True
            )
        assert engine.blocked_until == 0.0


async def test_mc_play_brain_tracks_and_flushes_waypoint_save_tasks_on_stop() -> None:
    saved_calls: list[tuple[str, Any]] = []
    save_started = asyncio.Event()
    allow_finish = asyncio.Event()

    class _FakeClient:
        connected = True

        async def state_set(self, key: str, value: Any) -> None:
            save_started.set()
            await allow_finish.wait()
            saved_calls.append((key, value))

    class _FakeSnap:
        connected = False
        dimension = "overworld"
        pos = (10, 64, -5)

    class _FakePlugin:
        enable_brain = True
        enabled = True
        paused = False
        bridge_ready = False
        session_intent = ""
        snapshot = _FakeSnap()
        client = _FakeClient()

        def clock(self) -> float:
            return 1000.0

    brain = McPlayBrain(_FakePlugin())  # type: ignore[arg-type]
    brain.on_milestone("mc_death", {"pos": [12, 64, -8], "dimension": "overworld"})
    assert len(brain._waypoint_save_tasks) == 1
    await asyncio.wait_for(save_started.wait(), timeout=1.0)

    async def _release_soon() -> None:
        await asyncio.sleep(0.05)
        allow_finish.set()

    releaser = asyncio.create_task(_release_soon())
    await brain.stop()
    await releaser

    assert len(saved_calls) == 1
    assert saved_calls[0][0] == "mc_waypoints"
    assert brain._waypoint_save_tasks == set()


# ---------------------------------------------------------------------------
# 5. voice_gateway & voice_client Wave 3+4 fixes
# ---------------------------------------------------------------------------


async def test_voice_gateway_transcode_timeout_and_probe_unlink_on_cancel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1. Wedged ffmpeg hits read_timeout and releases ffmpeg_semaphore cleanly.
    hang_ffmpeg = tmp_path / "ffmpeg_hang.sh"
    hang_ffmpeg.write_text(
        "#!/bin/sh\nprintf 'FIRST_OUT'\nexec sleep 10\n",
        encoding="utf-8",
    )
    os.chmod(hang_ffmpeg, 0o755)

    sem = asyncio.Semaphore(1)
    gen = transcode_audio_stream(
        [b"MP3DATA"],
        src_fmt="mp3",
        dst_fmt="pcm_16k",
        ffmpeg_bin=str(hang_ffmpeg),
        ffmpeg_semaphore=sem,
        read_timeout=1.5,
        overall_timeout=6.0,
    )
    first = await anext(gen)
    assert first == b"FIRST_OUT"
    assert sem.locked() is True
    with pytest.raises(TimeoutError):
        await anext(gen)
    assert sem.locked() is False

    # 1b. Slow trickle stream hits overall_timeout even when each read finishes under read_timeout.
    trickle_ffmpeg = tmp_path / "ffmpeg_trickle.sh"
    trickle_ffmpeg.write_text(
        "#!/bin/sh\n"
        "i=0\n"
        "while [ $i -lt 50 ]; do\n"
        "  printf 'TICK'\n"
        "  sleep 0.05\n"
        "  i=$((i+1))\n"
        "done\n",
        encoding="utf-8",
    )
    os.chmod(trickle_ffmpeg, 0o755)
    with pytest.raises(TimeoutError):
        async for _chunk in transcode_audio_stream(
            [b"MP3DATA"],
            src_fmt="mp3",
            dst_fmt="pcm_16k",
            ffmpeg_bin=str(trickle_ffmpeg),
            ffmpeg_semaphore=sem,
            read_timeout=3.0,
            overall_timeout=0.6,
        ):
            pass
    assert sem.locked() is False

    # 2. Cancelling probe_audio_duration_ms still unlinks its temp file synchronously.
    ffprobe_hang = tmp_path / "ffprobe_hang.sh"
    ffprobe_hang.write_text("#!/bin/sh\nexec sleep 10\n", encoding="utf-8")
    os.chmod(ffprobe_hang, 0o755)

    probe_tmp = tmp_path / "probe_tmp"
    probe_tmp.mkdir()
    monkeypatch.setenv("TMPDIR", str(probe_tmp))
    monkeypatch.setattr(tempfile, "tempdir", str(probe_tmp))

    task = asyncio.create_task(
        probe_audio_duration_ms(
            b"ID3-DATA-1234",
            "mp3",
            ffmpeg_bin=str(hang_ffmpeg),
            timeout=5.0,
            ffmpeg_semaphore=sem,
        )
    )
    for _ in range(200):
        if list(probe_tmp.glob("vg-dur-*")):
            break
        await asyncio.sleep(0.01)
    assert list(probe_tmp.glob("vg-dur-*"))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert list(probe_tmp.glob("vg-dur-*")) == []
    assert sem.locked() is False


async def test_voice_gateway_ws_audio_cap_queue_bound_turns_cap_and_barge_in_order() -> None:
    events: list[tuple[str, Any]] = []
    updated = asyncio.Event()

    async def send_text(raw: str) -> None:
        events.append(("json", json.loads(raw)))
        updated.set()

    async def send_bytes(data: bytes) -> None:
        events.append(("bytes", bytes(data)))
        updated.set()

    async def wait_for_event(pred: Any, max_wait: float = 2.0) -> None:
        async with asyncio.timeout(max_wait):
            while not pred(events):
                updated.clear()
                if pred(events):
                    return
                await updated.wait()

    class _DummySTT:
        async def transcribe(self, wav_bytes: bytes, *, filename: str = "u.wav") -> str:
            _ = (wav_bytes, filename)
            return "ok"

        async def aclose(self) -> None:
            return None

    # A. WS TTS max_tts_audio_bytes cap (multi-chunk & first-chunk)
    class _OverCapTTS:
        response_format = "mp3"

        async def synthesize(self, text: str) -> tuple[bytes, str]:
            _ = text
            return b"X" * 50, "mp3"

        async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
            if text == "first-over":
                yield b"F" * 30
                return
            yield b"A" * 10
            yield b"B" * 10
            yield b"C" * 10  # pushes total 30 > 25

        async def aclose(self) -> None:
            return None

    session = VoiceGatewaySession(
        _gw_config(max_tts_audio_bytes=25),
        stt_provider=_DummySTT(),
        tts_provider=_OverCapTTS(),
        send_text=send_text,
        send_bytes=send_bytes,
        max_tts_queue_size=2,
    )
    await session.start()
    try:
        # First chunk alone > 25 -> error without tts.start
        await session.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "r1", "text": "first-over"})
        )
        await wait_for_event(
            lambda evs: any(
                k == "json" and v.get("type") == "error" and v.get("req_id") == "r1" for k, v in evs
            )
        )
        assert not any(
            k == "json" and v.get("type") == "tts.start" and v.get("req_id") == "r1"
            for k, v in events
        )
        assert any(
            k == "json" and v.get("reason") == "tts_audio_too_large" and v.get("req_id") == "r1"
            for k, v in events
        )

        # Multi-chunk overflow -> stops before 3rd chunk (20 bytes sent <= 25), emits tts.end(error)
        events.clear()
        await session.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "r2", "text": "multi-over"})
        )
        await wait_for_event(
            lambda evs: any(
                k == "json" and v.get("type") == "error" and v.get("req_id") == "r2" for k, v in evs
            )
        )
        sent_bin = b"".join(v for k, v in events if k == "bytes")
        assert sent_bin == b"A" * 10 + b"B" * 10
        assert any(
            k == "json" and v.get("type") == "tts.end" and v.get("reason") == "tts_audio_too_large"
            for k, v in events
        )

        # B. _turns bounded regardless of ended=False + age eviction
        for idx in range(100):
            st = session._register_turn(f"unended-{idx}")
            assert st.ended is False
        assert len(session._turns) <= 64
        stale_key = next(iter(session._turns))
        session._turns[stale_key].sent_at -= 3600.0
        session._register_turn("fresh-turn")
        assert stale_key not in session._turns
    finally:
        await session.close()

    # C. Queue bound + barge-in awaits cancelled _active_tts_task (no interleaved chunks)
    gate = asyncio.Event()
    turn1_started = asyncio.Event()

    class _GatedTTS:
        response_format = "mp3"

        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return text.encode(), "mp3"

        async def synthesize_stream(self, text: str) -> AsyncIterator[bytes]:
            if text == "turn1":
                yield b"T1-CHUNK1"
                turn1_started.set()
                try:
                    await gate.wait()
                    yield b"T1-CHUNK2"
                finally:
                    # Simulate async cleanup work on cancellation.
                    await asyncio.sleep(0.03)
            else:
                yield b"T2-CHUNK1"

        async def aclose(self) -> None:
            return None

    events.clear()
    session2 = VoiceGatewaySession(
        _gw_config(),
        stt_provider=_DummySTT(),
        tts_provider=_GatedTTS(),
        send_text=send_text,
        send_bytes=send_bytes,
        max_tts_queue_size=2,
    )
    await session2.start()
    try:
        # Start active job "turn1"
        await session2.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "q0", "text": "turn1"})
        )
        await asyncio.wait_for(turn1_started.wait(), timeout=1.0)

        # Fill queue to max_tts_queue_size=2, 3rd queued job must be rejected with tts_queue_full!
        await session2.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "q1", "text": "queued1"})
        )
        await session2.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "q2", "text": "queued2"})
        )
        await session2.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "q3", "text": "queued3"})
        )
        assert len(session2._tts_queue) == 2
        assert any(
            k == "json" and v.get("reason") == "tts_queue_full" and v.get("req_id") == "q3"
            for k, v in events
        )

        # Cancel active turn1 via _cancel_local_tts and immediately enqueue turn2:
        # turn1's tts.end(cancelled) MUST already be emitted before turn2 starts!
        await session2._cancel_local_tts()
        assert any(
            k == "json"
            and v.get("type") == "tts.end"
            and v.get("req_id") == "q0"
            and v.get("status") == "cancelled"
            for k, v in events
        )
        await session2.handle_text_frame(
            json.dumps({"type": "tts.synthesize", "req_id": "q4", "text": "turn2"})
        )
        await wait_for_event(
            lambda evs: any(
                k == "json" and v.get("type") == "tts.end" and v.get("req_id") == "q4"
                for k, v in evs
            )
        )
        q0_end_idx = next(
            i
            for i, (k, v) in enumerate(events)
            if k == "json" and v.get("type") == "tts.end" and v.get("req_id") == "q0"
        )
        q4_start_idx = next(
            i
            for i, (k, v) in enumerate(events)
            if k == "json" and v.get("type") == "tts.start" and v.get("req_id") == "q4"
        )
        assert q0_end_idx < q4_start_idx
        assert b"T1-CHUNK2" not in [v for k, v in events if k == "bytes"]
    finally:
        gate.set()
        await session2.close()


async def test_voice_gateway_stt_multipart_offloads_to_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import voice_gateway.app as vg_app

    main_tid = threading.get_ident()
    parse_tids: list[int] = []
    real_parse = vg_app._parse_stt_multipart_sync

    def _spy_parse(mime_buf: bytearray, *, max_audio_bytes: int) -> tuple[bytes, str, int, int]:
        parse_tids.append(threading.get_ident())
        assert isinstance(mime_buf, bytearray)
        return real_parse(mime_buf, max_audio_bytes=max_audio_bytes)

    monkeypatch.setattr(vg_app, "_parse_stt_multipart_sync", _spy_parse)

    class _CapSTT:
        async def transcribe(self, wav_bytes: bytes, *, filename: str = "u.wav") -> str:
            _ = filename
            assert wav_bytes.startswith(b"RIFF")
            return "threaded-multipart"

        async def aclose(self) -> None:
            return None

    class _NoopTTS:
        async def synthesize(self, text: str) -> tuple[bytes, str]:
            return text.encode(), "mp3"

        async def aclose(self) -> None:
            return None

    app = create_gateway_app(_gw_config(), stt_provider=_CapSTT(), tts_provider=_NoopTTS())
    auth = {"Authorization": f"Bearer {GATEWAY_TOKEN}"}
    wav_payload = b"RIFF\x24\x00\x00\x00WAVEfmt " + b"\x00" * 32
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://voice-gateway.local",
            headers=auth,
        ) as client:
            resp = await client.post(
                "/v1/stt",
                files={"file": ("clip.wav", wav_payload, "audio/wav")},
                data={"format": "wav", "sample_rate": "16000", "channels": "1"},
            )
            assert resp.status_code == 200
            assert resp.json()["text"] == "threaded-multipart"
            assert parse_tids and all(tid != main_tid for tid in parse_tids)


async def test_voice_client_stderr_drain_avoids_64k_deadlock_and_shared_base(
    tmp_path: Path,
) -> None:
    assert issubclass(AfplayAudioPlayer, BaseQueuedAudioPlayer)
    assert issubclass(RawStreamAudioPlayer, BaseQueuedAudioPlayer)

    # 1. FfmpegMicCapture writing >128 KiB to stderr BEFORE stdout does NOT deadlock!
    noisy_mic = tmp_path / "noisy_mic.py"
    noisy_mic.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import sys",
                "for _ in range(160):",
                "    sys.stderr.buffer.write(b'E' * 1024)",
                "    sys.stderr.buffer.flush()",
                "sys.stdout.buffer.write(b'\\x01\\x00' * 16)",
                "sys.stdout.buffer.flush()",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(noisy_mic, 0o755)
    mic = FfmpegMicCapture(frame_bytes=32, cmd_factory=lambda: [sys.executable, str(noisy_mic)])
    await mic.start()
    try:
        frame = await asyncio.wait_for(mic.read_frame(), timeout=5.0)
        assert len(frame) == 32
    finally:
        await mic.stop()

    # 2. Streaming player writing >128 KiB to stderr while reading stdin does NOT deadlock!
    noisy_play = tmp_path / "noisy_play.py"
    sink = tmp_path / "played.bin"
    noisy_play.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import pathlib, sys",
                "for _ in range(160):",
                "    sys.stderr.buffer.write(b'W' * 1024)",
                "    sys.stderr.buffer.flush()",
                "data = sys.stdin.buffer.read()",
                "pathlib.Path(sys.argv[1]).write_bytes(data)",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(noisy_play, 0o755)

    raw_player = RawStreamAudioPlayer(
        stream_cmd_factory=lambda _fmt: [sys.executable, str(noisy_play), str(sink)],
        enable_streaming=True,
    )
    await raw_player.start()
    try:
        raw_player.begin_stream("t-noisy", 1, "mp3")
        raw_player.push_chunk("t-noisy", b"AUDIO-OK")
        raw_player.end_stream("t-noisy", 1)
        await asyncio.wait_for(raw_player.wait_idle(), timeout=5.0)
        assert sink.read_bytes() == b"AUDIO-OK"
    finally:
        await raw_player.stop()


async def test_voice_client_providers_trust_env_false_byte_cap_and_edge_tts_hardening(
    tmp_path: Path,
) -> None:
    # 1. Default HTTP client sets trust_env=False
    client = _make_default_http_client()
    try:
        assert client._trust_env is False  # noqa: SLF001
    finally:
        await client.aclose()

    # 2. Non-streaming TTS enforces byte cap across SiliconFlow / Fish / BreezeBlue
    oversized = b"A" * 128

    def big_handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=oversized)

    async with httpx.AsyncClient(transport=httpx.MockTransport(big_handler)) as http:
        sf = SiliconFlowTTS(api_key="sk-test", max_audio_bytes=64, http=http)
        fish = FishAudioTTS(api_key="fk-test", max_audio_bytes=64, http=http)
        breeze = BreezeBlueTTS(api_key="brz-test", voice_id="v1", max_audio_bytes=64, http=http)
        for prov in (sf, fish, breeze):
            with pytest.raises(ProviderError, match="超过大小上限"):
                await prov.synthesize("测试字节上限")

    # 3. EdgeFallbackTTS uses --text=... (no flag injection) + kills hung/cancelled process
    argv_log = tmp_path / "edge_argv.json"
    ok_edge = tmp_path / "fake_edge_ok.py"
    ok_edge.write_text(
        "\n".join(
            [
                f"#!{sys.executable}",
                "import json, pathlib, sys",
                (
                    f"pathlib.Path({str(argv_log)!r}).write_text("
                    "json.dumps(sys.argv[1:]), encoding='utf-8')"
                ),
                "out = ''",
                "for idx, arg in enumerate(sys.argv[1:]):",
                "    if arg == '--write-media':",
                "        out = sys.argv[idx + 2]",
                "pathlib.Path(out).write_bytes(b'ID3-EDGE-OK')",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(ok_edge, 0o755)

    edge_ok = EdgeFallbackTTS(edge_tts_bin=str(ok_edge), timeout_seconds=5.0)
    audio, ext = await edge_ok.synthesize("--write-subtitles /tmp/pwned")
    assert audio == b"ID3-EDGE-OK" and ext == "mp3"
    recorded_argv = json.loads(argv_log.read_text(encoding="utf-8"))
    assert "--text=--write-subtitles /tmp/pwned" in recorded_argv

    # Hanging edge-tts times out promptly and cleans up subprocess
    pid_file = tmp_path / "hang_edge.pid"
    hang_edge = tmp_path / "fake_edge_hang.sh"
    hang_edge.write_text(
        f"#!/bin/sh\necho $$ > '{pid_file}'\nexec sleep 30\n",
        encoding="utf-8",
    )
    os.chmod(hang_edge, 0o755)

    edge_slow = EdgeFallbackTTS(edge_tts_bin=str(hang_edge), timeout_seconds=1.0)
    t0 = time.monotonic()
    with pytest.raises(ProviderError, match="超时"):
        await edge_slow.synthesize("挂起测试")
    assert time.monotonic() - t0 < 4.0
    assert pid_file.is_file()
    pid = int(pid_file.read_text(encoding="utf-8").strip())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)

    # CancelledError also kills child process cleanly
    pid_file.unlink(missing_ok=True)
    edge_cancel = EdgeFallbackTTS(edge_tts_bin=str(hang_edge), timeout_seconds=5.0)
    cancel_task = asyncio.create_task(edge_cancel.synthesize("取消测试"))
    for _ in range(200):
        if pid_file.is_file():
            break
        await asyncio.sleep(0.01)
    assert pid_file.is_file()
    cancel_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancel_task
    pid = int(pid_file.read_text(encoding="utf-8").strip())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
