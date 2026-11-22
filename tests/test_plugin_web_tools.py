"""Tests for the out-of-process `web_tools` plugin and kernel bridge."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import gzip
import ipaddress
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from hakimi.link_preview import preview_links
from hakimi.plugin_bus import PluginBus
from hakimi.plugin_kernel import PluginKernel
from hakimi.security import Rejected
from hakimi.web_search import (
    WebSearchConfig,
    execute_deepseek_search,
    execute_remote_web_search,
)
from plugins.web_tools.plugin_main import WebToolsPlugin, build_web_tools_plugin_from_env
from plugins.web_tools.preview import preview_urls
from plugins.web_tools.search import SearchError, SearchProviderConfig, execute_search
from plugins.web_tools.ssrf import (
    PinnedPublicTransport,
    SsrfBlockedError,
    is_public_ip,
    resolve_public_ips,
    validate_public_url,
)


class _PipeClientSide:
    def __init__(self, pipe: _BiPipe) -> None:
        self._pipe = pipe
        self.close_code: int | None = None

    async def send(self, raw: str) -> None:
        await self._pipe.c2s.put(raw)

    async def recv(self) -> str:
        item = await self._pipe.s2c.get()
        if item.startswith("__EOF__:"):
            code = int(item.split(":", 1)[1])
            self.close_code = code
            raise ConnectionError(f"closed:{code}")
        return item

    async def close(self, code: int = 1000) -> None:
        self.close_code = code
        await self._pipe.close_from_client(code)


class _BiPipe:
    def __init__(self, bus: PluginBus, plugin_id: str) -> None:
        self.bus = bus
        self.plugin_id = plugin_id
        self.c2s: asyncio.Queue[str] = asyncio.Queue()
        self.s2c: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.server_task: asyncio.Task[None] | None = None

    async def server_recv(self) -> str:
        item = await self.c2s.get()
        if item.startswith("__EOF__:"):
            raise ConnectionError(item)
        return item

    async def server_send(self, raw: str) -> None:
        if not self.closed:
            await self.s2c.put(raw)

    async def server_close(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.s2c.put(f"__EOF__:{code}")
        await self.c2s.put(f"__EOF__:{code}")

    async def close_from_client(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        await self.c2s.put(f"__EOF__:{code}")
        await self.s2c.put(f"__EOF__:{code}")

    def connect_factory(self, _url: str, _headers: dict[str, str]) -> _PipeClientSide:
        self.server_task = asyncio.create_task(
            self.bus.serve(
                self.server_recv,
                self.server_send,
                self.server_close,
                source_ip="127.0.0.1",
                plugin_id=self.plugin_id,
            )
        )
        return _PipeClientSide(self)

    async def shutdown(self) -> None:
        await self.close_from_client(1000)
        if self.server_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(self.server_task, timeout=2.0)


async def _wait_until(predicate: Any, *, max_wait: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + max_wait
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition not met within max_wait")
        await asyncio.sleep(0.01)  # noqa: ASYNC110


class _ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        pass


class _StubModules:
    def __init__(self) -> None:
        self.systems: dict[str, Any] = {}
        self._items: dict[str, Any] = {}
        self.thinking_scenes: tuple[Any, ...] = ()
        self.chat_platforms: dict[str, Any] = {}
        self.catalog = SimpleNamespace(actions={}, aliases={})

    def get(self, name: str) -> Any:
        return self._items.get(name)

    def register(self, mod: Any) -> None:
        self._items[mod.module_id] = mod

    def unregister(self, name: str) -> None:
        self._items.pop(name, None)

    def refresh_catalog(self) -> None:
        pass


@contextlib.asynccontextmanager
async def _connected_web_tools(
    settings,
    store,
    *,
    search_config: SearchProviderConfig | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    dns_resolver: Any = None,
):
    bus_settings = settings.model_copy(update={"plugin_bus_ws_enabled": True})
    svc = SimpleNamespace(
        s=bus_settings,
        store=store,
        modules=_StubModules(),
        kernel=PluginKernel(),
        secrets=(),
    )
    bus = PluginBus(bus_settings, store, svc)
    await bus.initialize()
    token, _ = await bus.create_plugin("web_tools", "网页工具", ["actions"], node_region="KR")
    pipe = _BiPipe(bus, "web_tools")
    plugin = WebToolsPlugin(
        ws_url="ws://127.0.0.1:12141/ws/plugin",
        token=token,
        search_config=search_config or SearchProviderConfig(),
        transport=transport,
        dns_resolver=dns_resolver,
        connect_factory=pipe.connect_factory,
        reconnect_min=0.05,
        reconnect_max=0.2,
    )
    runner = asyncio.create_task(plugin.client.run_forever())
    try:
        await _wait_until(lambda: bus.has_tool("web_tools", "web_tools.search"))
        await _wait_until(lambda: bus.has_tool("web_tools", "web_tools.preview"))
        yield bus, plugin
    finally:
        await plugin.close()
        await pipe.shutdown()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(runner, timeout=2.0)


# ---------------------------------------------------------------------------
# 1. SSRF, DNS Rebinding, Redirects, and Oversized Body Defenses
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_url",
    [
        "file:///etc/passwd",
        "ftp://example.com/file.txt",
        "gopher://127.0.0.1:6379/_INFO",
        "javascript:alert(1)",
        "data:text/html,<title>x</title>",
        "http://127.0.0.1/admin",
        "http://127.0.0.1:80/admin",
        "http://0.0.0.0/",
        "http://10.0.0.5/secret",
        "http://172.16.0.1/internal",
        "http://192.168.1.1/router",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.64.0.1/cgnat",
        "http://100.100.100.200/latest/meta-data/",
        "http://198.18.0.1/bench",
        "http://192.88.99.1/6to4-relay",
        "https://[::1]/",
        "https://[fe80::1]/",
        "https://[fec0::1]/",
        "https://[fc00::1]/",
        "https://[::ffff:127.0.0.1]/",
        "https://[2002:7f00:0001::1]/",
        "http://localhost/admin",
        "http://metadata.google.internal/computeMetadata/v1/",
        "https://service.local/api",
        "https://service.internal/api",
        "http://0177.0.0.1/",
        "http://2130706433/",
        "http://0x7f000001/",
        "https://example.com:6099/",
        "http://example.com:8080/",
        "https://example.com:80/",
        "http://example.com:443/",
        "https://example.com:99999/",
        "https://user:secret@example.com/",
        "http://admin@example.com/",
        "https://example.com/a\r\nX-Injected: 1",
    ],
)
def test_ssrf_validate_public_url_blocks_attacks(bad_url: str):
    with pytest.raises(SsrfBlockedError):
        validate_public_url(bad_url)


async def test_ssrf_dns_resolution_blocks_private_and_mixed_ips():
    async def resolver_private(_host: str, _port: int):
        return ["127.0.0.1"]

    async def resolver_mixed(_host: str, _port: int):
        return ["93.184.216.34", "10.0.0.8"]

    async def resolver_metadata(_host: str, _port: int):
        return ["169.254.169.254"]

    async def resolver_site_local(_host: str, _port: int):
        return ["fec0::1"]

    async def resolver_6to4_relay(_host: str, _port: int):
        return ["192.88.99.1"]

    async def resolver_public(_host: str, _port: int):
        return ["93.184.216.34"]

    assert is_public_ip(ipaddress.ip_address("fec0::1")) is False
    assert is_public_ip(ipaddress.ip_address("192.88.99.1")) is False
    assert is_public_ip(ipaddress.ip_address("93.184.216.34")) is True

    with pytest.raises(SsrfBlockedError):
        await resolve_public_ips("rebind.example", 443, resolver=resolver_private)
    with pytest.raises(SsrfBlockedError):
        await resolve_public_ips("mixed.example", 443, resolver=resolver_mixed)
    with pytest.raises(SsrfBlockedError):
        await resolve_public_ips("meta.example", 443, resolver=resolver_metadata)
    with pytest.raises(SsrfBlockedError):
        await resolve_public_ips("sitelocal.example", 443, resolver=resolver_site_local)
    with pytest.raises(SsrfBlockedError):
        await resolve_public_ips("relay6to4.example", 443, resolver=resolver_6to4_relay)

    ips = await resolve_public_ips("public.example", 443, resolver=resolver_public)
    assert [str(ip) for ip in ips] == ["93.184.216.34"]


async def test_pinned_public_transport_pins_ip_sni_and_blocks_rebinding():
    seen_requests: list[httpx.Request] = []
    dns_calls = 0

    def fake_inner(request: httpx.Request) -> httpx.Response:
        seen_requests.append(request)
        return httpx.Response(200, text="ok")

    async def rebinding_resolver(_host: str, _port: int):
        nonlocal dns_calls
        dns_calls += 1
        return ["93.184.216.34"] if dns_calls == 1 else ["127.0.0.1"]

    transport = PinnedPublicTransport(
        inner=httpx.MockTransport(fake_inner),
        dns_resolver=rebinding_resolver,
    )
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        # 1st request pins the resolved public IP literal + Host header + TLS SNI.
        resp = await client.get("https://rebind.example/page")
        assert resp.status_code == 200
        assert len(seen_requests) == 1
        assert seen_requests[0].url.host == "93.184.216.34"
        assert seen_requests[0].headers["host"] == "rebind.example"
        assert seen_requests[0].extensions["sni_hostname"] == "rebind.example"

        # 2nd request sees DNS rebind to 127.0.0.1 and blocks BEFORE hitting inner transport.
        with pytest.raises(SsrfBlockedError):
            await client.get("https://rebind.example/page")
        assert len(seen_requests) == 1

        # Non-standard port is rejected before DNS or socket connect.
        with pytest.raises(SsrfBlockedError):
            await client.get("https://public.example:6099/page")
        assert len(seen_requests) == 1

    # Public IPv6 resolution is pinned with bracketed URL + SNI intact.
    v6_seen: list[httpx.Request] = []

    async def v6_resolver(_host: str, _port: int):
        return ["2606:2800:220:1:248:1893:25c8:1946"]

    v6_transport = PinnedPublicTransport(
        inner=httpx.MockTransport(lambda req: (v6_seen.append(req), httpx.Response(200))[1]),
        dns_resolver=v6_resolver,
    )
    async with httpx.AsyncClient(transport=v6_transport, trust_env=False) as client:
        resp_v6 = await client.get("https://ipv6.example/path")
        assert resp_v6.status_code == 200
        assert v6_seen[0].url.host == "2606:2800:220:1:248:1893:25c8:1946"
        assert v6_seen[0].extensions["sni_hostname"] == "ipv6.example"


async def test_preview_blocks_redirect_ssrf_and_dns_rebind_and_oversized():
    seen: list[str] = []
    bomb_gz = gzip.compress(b"A" * (512 * 1024))

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        seen.append(url)
        if url == "https://bounce.example/loopback":
            return httpx.Response(302, headers={"Location": "http://127.0.0.1/admin"})
        if url == "https://bounce.example/metadata":
            return httpx.Response(301, headers={"Location": "http://169.254.169.254/latest/"})
        if url == "https://bounce.example/scheme-rel":
            return httpx.Response(302, headers={"Location": "//127.0.0.1/secret"})
        if url == "https://bounce.example/to-private-dns":
            return httpx.Response(302, headers={"Location": "https://privdns.example/doc"})
        if url == "https://huge.example/cl":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html", "Content-Length": str(20 * 1024 * 1024)},
                stream=_ChunkStream([b"<html><title>Huge</title></html>"]),
            )
        if url == "https://bomb.example/gz":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html; charset=utf-8", "Content-Encoding": "gzip"},
                stream=_ChunkStream([bomb_gz]),
            )
        return httpx.Response(404)

    async def dns_resolver(host: str, _port: int):
        if host == "privdns.example":
            return ["10.1.2.3"]
        return ["93.184.216.34"]

    transport = httpx.MockTransport(handler)
    for bad in (
        "https://bounce.example/loopback",
        "https://bounce.example/metadata",
        "https://bounce.example/scheme-rel",
        "https://bounce.example/to-private-dns",
    ):
        outcome = await preview_urls([bad], transport=transport, dns_resolver=dns_resolver)
        assert outcome.previews == []
        assert outcome.ssrf_blocked_count == 1

    assert not any(
        "127.0.0.1" in u or "169.254.169.254" in u or "privdns.example" in u for u in seen
    )

    huge_out = await preview_urls(
        ["https://huge.example/cl"], transport=transport, dns_resolver=dns_resolver
    )
    assert huge_out.previews == []
    assert huge_out.oversized_count == 1

    # Gzip bomb is bounded cleanly at 64 KiB without crashing or exhausting memory.
    bomb_out = await preview_urls(
        ["https://bomb.example/gz"], transport=transport, dns_resolver=dns_resolver
    )
    assert bomb_out.previews == []


async def test_search_rejects_oversized_response_gzip_bomb_and_redirect_ssrf():
    big_chunk = b"x" * 70_000
    gz_bomb = gzip.compress(b'{"results":[{"title":"t","summary":"' + b"A" * 400_000 + b'"}]}')

    def big_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json"},
            stream=_ChunkStream([big_chunk, big_chunk, big_chunk]),
        )

    def bomb_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
            stream=_ChunkStream([gz_bomb]),
        )

    def redirect_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/steal-key"})

    cfg = SearchProviderConfig(deepseek_api_key="sk-secret-deepseek-99999")
    assert "sk-secret-deepseek-99999" not in repr(cfg)

    with pytest.raises(SearchError) as exc_big:
        await execute_search(
            "oversized test", cfg, provider="deepseek", transport=httpx.MockTransport(big_handler)
        )
    assert exc_big.value.code == "web_search_failed"

    with pytest.raises(SearchError) as exc_bomb:
        await execute_search(
            "gzip bomb test", cfg, provider="deepseek", transport=httpx.MockTransport(bomb_handler)
        )
    assert exc_bomb.value.code == "web_search_failed"

    with pytest.raises(SearchError) as exc_redir:
        await execute_search(
            "redirect test",
            cfg,
            provider="deepseek",
            transport=httpx.MockTransport(redirect_handler),
        )
    assert exc_redir.value.code == "ssrf_blocked"

    # Private SearXNG base URL is blocked by SSRF before any network call.
    bad_searx_cfg = SearchProviderConfig(searxng_base_url="http://127.0.0.1:8080")
    with pytest.raises(SearchError) as exc_searx:
        await execute_search("local searx", bad_searx_cfg, provider="searxng")
    assert exc_searx.value.code == "ssrf_blocked"


# ---------------------------------------------------------------------------
# 2. Multi-Provider Strategy Switching & Secret Redaction
# ---------------------------------------------------------------------------


async def test_search_multi_provider_switching_fallback_and_secret_scrubbing():
    seen_providers: list[str] = []
    fail_deepseek = [True]

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        assert request.headers.get("Accept-Encoding") == "gzip, deflate, identity"
        if host == "api.deepseek.com":
            seen_providers.append("deepseek")
            if fail_deepseek[0]:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "title": "DeepSeek 公告",
                            "url": "https://docs.example.org/ds",
                            "summary": "DeepSeek 检索事实。",
                            "rerank_score": 1.5,
                        },
                        {
                            "title": "内网泄露",
                            "url": "http://127.0.0.1/internal",
                            "summary": "忽略内网链接。",
                            "rerank_score": 1.0,
                        },
                    ]
                },
            )
        if host == "api.tavily.com":
            seen_providers.append("tavily")
            assert request.headers.get("Authorization") == "Bearer tvly-secret-token-12345"
            return httpx.Response(
                200,
                json={
                    "answer": "Tavily 汇总的公开事实。",
                    "results": [
                        {
                            "title": "官方说明",
                            "url": "https://tavily-src.example.org/a",
                            "content": "权威佐证。",
                        }
                    ],
                },
            )
        if host == "api.search.brave.com":
            seen_providers.append("brave")
            assert request.headers.get("X-Subscription-Token") == "brave-secret-token-67890"
            return httpx.Response(
                200,
                json={
                    "web": {
                        "results": [
                            {
                                "title": "Brave 报道",
                                "url": "https://brave-src.example.org/b",
                                "description": "Brave 检索内容。",
                            }
                        ]
                    }
                },
            )
        if host == "searx.example.org":
            seen_providers.append("searxng")
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "title": "SearXNG 条目",
                            "url": "https://searx-src.example.org/c",
                            "content": "SearXNG 公开结果。",
                        }
                    ]
                },
            )
        if host == "html.duckduckgo.com":
            seen_providers.append("duckduckgo")
            ddg_href = "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fddg-src.example.org%2Fd"
            html = (
                f'<a class="result__a" href="{ddg_href}">DDG 标题</a>'
                '<a class="result__snippet">DDG 摘要事实</a>'
            )
            return httpx.Response(200, headers={"Content-Type": "text/html"}, text=html)
        return httpx.Response(404)

    cfg = SearchProviderConfig(
        provider_order=("deepseek", "tavily", "brave", "searxng", "duckduckgo"),
        deepseek_api_key="sk-ds-secret-11111",
        tavily_api_key="tvly-secret-token-12345",
        brave_api_key="brave-secret-token-67890",
        searxng_base_url="https://searx.example.org",
        duckduckgo_enabled=True,
    )
    transport = httpx.MockTransport(handler)

    # Auto mode falls back from failing deepseek -> tavily.
    res_auto = await execute_search("最新公开资讯", cfg, provider="auto", transport=transport)
    assert seen_providers == ["deepseek", "tavily"]
    assert res_auto["provider"] == "tavily"
    assert res_auto["query_count"] == 1
    assert "Tavily" in res_auto["text"]
    assert res_auto["sources"] == [{"title": "官方说明", "url": "https://tavily-src.example.org/a"}]

    # Explicit provider switches directly to brave, searxng, duckduckgo, and deepseek.
    res_brave = await execute_search("Brave 查询", cfg, provider="brave", transport=transport)
    assert res_brave["provider"] == "brave" and "Brave" in res_brave["text"]

    res_searx = await execute_search("SearXNG 查询", cfg, provider="searxng", transport=transport)
    assert res_searx["provider"] == "searxng" and "SearXNG" in res_searx["text"]

    res_ddg = await execute_search("DDG 查询", cfg, provider="duckduckgo", transport=transport)
    assert res_ddg["provider"] == "duckduckgo"
    assert res_ddg["sources"] == [{"title": "DDG 标题", "url": "https://ddg-src.example.org/d"}]

    fail_deepseek[0] = False
    res_ds = await execute_search("DeepSeek 查询", cfg, provider="deepseek", transport=transport)
    assert res_ds["provider"] == "deepseek"
    # Private URL citation is stripped from sources.
    assert res_ds["sources"] == [{"title": "DeepSeek 公告", "url": "https://docs.example.org/ds"}]

    # If an upstream response echoes a secret API key, output is rejected.
    def leak_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"answer": "leaked tvly-secret-token-12345 here", "results": []},
        )

    with pytest.raises(SearchError) as exc_leak:
        await execute_search(
            "leak check", cfg, provider="tavily", transport=httpx.MockTransport(leak_handler)
        )
    assert exc_leak.value.code == "search_invalid_output"
    assert "tvly-secret-token-12345" not in str(exc_leak.value)


# ---------------------------------------------------------------------------
# 3. End-to-End PluginBus <-> WebToolsPlugin + Kernel Bridge & Budget Gates
# ---------------------------------------------------------------------------


async def test_remote_web_tools_e2e_search_preview_and_kernel_budget_gate(
    settings, store, monkeypatch
):
    gz_html = gzip.compress(
        (
            '<html><head><meta property="og:title" content="远程预览标题">'
            '<meta name="description" content="远程抓取摘要"></head></html>'
        ).encode()
    )
    remote_search_hits = 0

    def mock_net(request: httpx.Request) -> httpx.Response:
        nonlocal remote_search_hits
        url = str(request.url)
        if request.url.host == "api.tavily.com":
            remote_search_hits += 1
            return httpx.Response(
                200,
                json={
                    "answer": "远程 Tavily 核实的公开事件。",
                    "results": [
                        {
                            "title": "权威信源",
                            "url": "https://news.example.org/item",
                            "content": "详细事实内容。",
                        }
                    ],
                },
            )
        if url == "https://short.example/go":
            return httpx.Response(301, headers={"Location": "https://dest.example/post"})
        if url == "https://dest.example/post":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html; charset=utf-8", "Content-Encoding": "gzip"},
                stream=_ChunkStream([gz_html]),
            )
        if url == "https://evil.example/bounce":
            return httpx.Response(302, headers={"Location": "http://127.0.0.1/admin"})
        return httpx.Response(404)

    search_cfg = SearchProviderConfig(tavily_api_key="tvly-e2e-test-key-99999")
    await store.runtime.web_search.save(WebSearchConfig(enabled=True, daily_limit=1))

    screened_calls: list[dict[str, Any]] = []
    local_ai_search_calls = 0

    class _FakeAI:
        async def screen(self, payload, subject, *, purpose, budget_lane, before_call=None):
            screened_calls.append(
                {
                    "payload": payload,
                    "subject": subject,
                    "purpose": purpose,
                    "budget_lane": budget_lane,
                }
            )
            if before_call is not None:
                await before_call()

        async def search(self, query, subject, *, before_call=None, cancel_event=None):
            nonlocal local_ai_search_calls
            local_ai_search_calls += 1
            return f"LOCAL:{query}"

    # Ensure local aiohttp is NEVER called when remote web_tools is online.
    def _forbid_local_aiohttp(*_a, **_kw):
        raise AssertionError("local aiohttp should not be used when web_tools is online")

    monkeypatch.setattr("hakimi.link_preview.aiohttp.ClientSession", _forbid_local_aiohttp)

    async with _connected_web_tools(
        settings,
        store,
        search_config=search_cfg,
        transport=httpx.MockTransport(mock_net),
    ) as (bus, _plugin):
        controller = store.runtime.web_search
        controller.plugin_bus = bus
        ai = _FakeAI()

        # 1) Remote-first search goes through plugin_bus + kernel budget + kernel screen.
        rendered = await controller.execute_search("今天发生了什么大事", "subj:1", ai=ai)
        assert "远程 Tavily 核实的公开事件" in rendered
        assert "权威信源" in rendered
        assert remote_search_hits == 1
        assert local_ai_search_calls == 0
        assert len(screened_calls) == 1
        assert screened_calls[0]["budget_lane"] == "search"
        assert screened_calls[0]["purpose"] == "source"

        ws_status = await controller.status()
        assert ws_status["today"]["attempts"] == 1
        assert ws_status["today"]["queries"] == 1
        assert ws_status["remote_tools"]["online"] is True

        # 2) Kernel daily_limit=1 blocks a 2nd search BEFORE calling remote plugin.
        with pytest.raises(Rejected, match="search_daily_limit"):
            await controller.execute_search("第二次检索被预算拦截", "subj:1", ai=ai)
        assert remote_search_hits == 1

        # 2b) Kernel disabled gate blocks search BEFORE calling remote plugin.
        await controller.save(WebSearchConfig(enabled=False, daily_limit=5))
        with pytest.raises(Rejected, match="web_search_disabled"):
            await controller.execute_search("已关闭检索", "subj:1", ai=ai)
        assert remote_search_hits == 1
        await controller.save(WebSearchConfig(enabled=True, daily_limit=1))

        # 3) Remote-first preview follows safe redirects, blocks SSRF hop, skips local aiohttp.
        previews = await preview_links(
            "看看 https://short.example/go 和 https://evil.example/bounce",
            plugin_bus=bus,
            budget_seconds=1.5,
        )
        assert previews == [{"title": "远程预览标题", "description": "远程抓取摘要"}]

        # 4) All-SSRF preview raises ssrf_blocked at RPC layer and returns [] locally.
        with pytest.raises(Rejected, match="ssrf_blocked"):
            await bus.invoke_tool(
                "web_tools",
                "web_tools.preview",
                {"urls": ["http://127.0.0.1/admin"], "budget_seconds": 1.0},
            )
        assert await preview_links("http://127.0.0.1/admin", plugin_bus=bus) == []


async def test_offline_graceful_fallback_and_remote_only_strict_mode(settings, store, monkeypatch):
    bus_settings = settings.model_copy(update={"plugin_bus_ws_enabled": True})
    svc = SimpleNamespace(
        s=bus_settings,
        store=store,
        modules=_StubModules(),
        kernel=PluginKernel(),
        secrets=(),
    )
    offline_bus = PluginBus(bus_settings, store, svc)
    await offline_bus.initialize()

    controller = store.runtime.web_search
    controller.plugin_bus = offline_bus
    await controller.save(WebSearchConfig(enabled=True, daily_limit=5))

    local_search_calls: list[str] = []

    class _FakeAI:
        async def screen(self, *_a, **_kw):
            pass

        async def search(self, query, subject, *, before_call=None, cancel_event=None):
            local_search_calls.append(query)
            return f"LOCAL_FALLBACK:{query}"

    # --- 1) Offline graceful fallback (default remote_only=False) ---
    monkeypatch.delenv("WEB_TOOLS_REMOTE_ONLY", raising=False)
    out = await controller.execute_search("离线平滑回退测试", "subj:2", ai=_FakeAI())
    assert out == "LOCAL_FALLBACK:离线平滑回退测试"
    assert local_search_calls == ["离线平滑回退测试"]

    # Local preview fallback works cleanly when web_tools is offline.
    class _FakeContent:
        async def read(self, _limit: int) -> bytes:
            return b"<html><head><title>Local Fallback Title</title></head></html>"

    class _FakeResp:
        status = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}
        content = _FakeContent()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    local_aiohttp_hits = 0

    class _FakeClient:
        def __init__(self, **kwargs):
            self.connector = kwargs["connector"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.connector.close()

        def get(self, *_args, **_kwargs):
            nonlocal local_aiohttp_hits
            local_aiohttp_hits += 1
            return _FakeResp()

    monkeypatch.setattr("hakimi.link_preview.aiohttp.ClientSession", _FakeClient)
    local_prev = await preview_links("https://example.com/fallback", plugin_bus=offline_bus)
    assert local_prev == [{"title": "Local Fallback Title", "description": ""}]
    assert local_aiohttp_hits == 1

    # --- 2) Strict remote_only mode (both explicit kwarg and WEB_TOOLS_REMOTE_ONLY env) ---
    local_search_calls.clear()
    local_aiohttp_hits = 0

    with pytest.raises(Rejected, match="web_tools_remote_unavailable"):
        await controller.execute_search("严格远程拦截", "subj:2", ai=_FakeAI(), remote_only=True)
    assert local_search_calls == []

    with pytest.raises(Rejected, match="web_tools_remote_unavailable"):
        await execute_deepseek_search(
            "严格远程拦截",
            settings,
            api_key="sk-local-key-12345678",
            base_url="https://api.deepseek.com",
            model="deepseek-flash",
            plugin_bus=offline_bus,
            remote_only=True,
        )

    with pytest.raises(Rejected, match="web_tools_remote_unavailable"):
        await preview_links("https://example.com/blocked", plugin_bus=offline_bus, remote_only=True)
    assert local_aiohttp_hits == 0

    # Messages without URLs still return [] cleanly even in remote_only mode.
    assert await preview_links("普通聊天没有链接", plugin_bus=offline_bus, remote_only=True) == []

    # Env var WEB_TOOLS_REMOTE_ONLY=1 also blocks local claim() and preview_links().
    monkeypatch.setenv("WEB_TOOLS_REMOTE_ONLY", "1")
    with pytest.raises(Rejected, match="web_tools_remote_unavailable"):
        await controller.claim()
    with pytest.raises(Rejected, match="web_tools_remote_unavailable"):
        await preview_links("https://example.com/env-blocked", plugin_bus=offline_bus)
    assert local_aiohttp_hits == 0


async def test_online_unconfigured_provider_falls_back_without_double_charge(settings, store):
    await store.runtime.web_search.save(WebSearchConfig(enabled=True, daily_limit=2))
    local_calls = 0

    class _FakeAI:
        async def screen(self, *_a, **_kw):
            pass

        async def search(self, query, _subject, *, before_call=None, cancel_event=None):
            nonlocal local_calls
            local_calls += 1
            return f"LOCAL_OK:{query}"

    # Remote web_tools connected with NO search provider keys configured.
    async with _connected_web_tools(settings, store, search_config=SearchProviderConfig()) as (
        bus,
        _plugin,
    ):
        controller = store.runtime.web_search
        controller.plugin_bus = bus

        # Default mode: gracefully releases reserved remote attempt and delegates to ai.search.
        out = await controller.execute_search("未配置远端搜索源", "subj:3", ai=_FakeAI())
        assert out == "LOCAL_OK:未配置远端搜索源"
        assert local_calls == 1
        assert (await controller.status())["today"]["attempts"] == 0

        # Strict remote_only mode: refuses local fallback and surfaces search_provider_unconfigured.
        with pytest.raises(Rejected, match="search_provider_unconfigured"):
            await controller.execute_search(
                "严格远端未配置", "subj:3", ai=_FakeAI(), remote_only=True
            )
        assert local_calls == 1


async def test_execute_remote_web_search_direct_and_kernel_secret_guard(settings, store):
    secret_settings = settings.model_copy(
        update={"admin_token": SecretStr("adm-super-secret-kernel-tok")}
    )
    leak_kernel_secret = [False]

    def ok_handler(_req: httpx.Request) -> httpx.Response:
        summary = (
            "leaked adm-super-secret-kernel-tok"
            if leak_kernel_secret[0]
            else "Python 3.12 文档说明。"
        )
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "title": "Python 官网",
                        "url": "https://docs.python.org/3/",
                        "summary": summary,
                        "rerank_score": 2.0,
                    }
                ]
            },
        )

    cfg = SearchProviderConfig(deepseek_api_key="sk-ds-direct-99999")
    async with _connected_web_tools(
        secret_settings, store, search_config=cfg, transport=httpx.MockTransport(ok_handler)
    ) as (bus, _plugin):
        result = await execute_remote_web_search(
            "Python 3.12 新特性", secret_settings, plugin_bus=bus, provider="deepseek"
        )
        assert "Python 3.12 文档说明" in result.text
        assert result.sources == (("Python 官网", "https://docs.python.org/3/"),)

        # execute_deepseek_search routes remotely when plugin_bus is online & transport is None.
        via_ds = await execute_deepseek_search(
            "Python 3.12",
            secret_settings,
            api_key="",  # Kernel does not even need DeepSeek key when remote plugin handles it!
            base_url="https://api.deepseek.com",
            model="deepseek-flash",
            plugin_bus=bus,
        )
        assert "Python 3.12 文档说明" in via_ds.text

        # PluginBus scrubs kernel secrets in transit; secret never reaches caller.
        leak_kernel_secret[0] = True
        scrubbed = await execute_remote_web_search(
            "Python secret check", secret_settings, plugin_bus=bus, provider="deepseek"
        )
        assert "adm-super-secret-kernel-tok" not in scrubbed.text
        assert "[REDACTED]" in scrubbed.text

    # Defense-in-depth: even if a bus did not scrub, execute_remote_web_search rejects secret leak.
    class _UnscrubbedBus:
        def has_tool(self, pid: str, action: str) -> bool:
            return pid == "web_tools" and action == "web_tools.search"

        async def invoke_tool(self, *_args, **_kwargs):
            return SimpleNamespace(
                data={
                    "text": "leak adm-super-secret-kernel-tok",
                    "sources": [],
                    "provider": "deepseek",
                    "query_count": 1,
                }
            )

    with pytest.raises(Rejected, match="search_invalid_output"):
        await execute_remote_web_search(
            "secret leak fallback check",
            secret_settings,
            plugin_bus=_UnscrubbedBus(),
            provider="deepseek",
        )


async def test_web_tools_timeout_enforcement():
    class _SlowTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(3.0)
            return httpx.Response(200, text="too late")

    outcome = await preview_urls(
        ["https://slow.example/article"],
        budget_seconds=0.2,
        transport=_SlowTransport(),
    )
    assert outcome.previews == []

    cfg = SearchProviderConfig(tavily_api_key="tvly-timeout-key-9999")
    with pytest.raises(SearchError) as exc:
        await execute_search(
            "timeout query",
            cfg,
            provider="tavily",
            timeout_seconds=1.0,
            transport=_SlowTransport(),
        )
    assert exc.value.code == "web_search_failed"


def test_build_web_tools_plugin_from_env_and_lightweight_dependencies(monkeypatch):
    monkeypatch.delenv("HAKIMI_PLUGIN_WEB_TOOLS_TOKEN", raising=False)
    monkeypatch.delenv("HAKIMI_PLUGIN_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="HAKIMI_PLUGIN_WEB_TOOLS_TOKEN"):
        build_web_tools_plugin_from_env()

    monkeypatch.setenv("HAKIMI_PLUGIN_WEB_TOOLS_TOKEN", "tok-web-tools-123")
    monkeypatch.setenv("WEB_TOOLS_TAVILY_API_KEY", "tvly-env-key-99999")
    monkeypatch.setenv("WEB_TOOLS_DEFAULT_PROVIDER", "tavily")
    plugin = build_web_tools_plugin_from_env()
    assert plugin.client.config.plugin_id == "web_tools"
    assert plugin.client.config.scopes == ("actions",)
    assert plugin.client.max_blob_bytes == 0
    assert plugin.search_config.configured_providers() == ["tavily"]
    # Status payload never exposes configured secrets.
    assert "tvly-env-key-99999" not in json.dumps(plugin.status_payload())

    # Static check: plugins/web_tools/ only uses stdlib + lightweight plugins/Dockerfile deps.
    allowed_third_party = {"hakimi_plugin_sdk", "httpx", "pydantic"}
    stdlib_names = getattr(sys, "stdlib_module_names", frozenset())
    for py_file in Path("plugins/web_tools").glob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert top in stdlib_names or top in allowed_third_party, (
                        f"{py_file}: {alias.name}"
                    )
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                top = node.module.split(".")[0]
                assert top in stdlib_names or top in allowed_third_party, (
                    f"{py_file}: {node.module}"
                )
