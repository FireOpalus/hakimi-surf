"""Tests for web search account-ID redaction and DeepSeek connect-time DNS pinning."""

from __future__ import annotations

import asyncio
import json
import socket
from types import SimpleNamespace
from typing import Any

import httpcore
import httpx
import pytest

from hakimi.security import Rejected
from hakimi.social import OwnerBinding
from hakimi.web_search import (
    _configured_account_ids,
    _PinnedPublicTransport,
    _scrub_account_ids,
    execute_deepseek_search,
    sanitize_model_search_query,
    search_query,
)


def _addrinfo_entries(ips: list[str], port: int = 443) -> list[tuple[Any, ...]]:
    entries: list[tuple[Any, ...]] = []
    for ip in ips:
        family = socket.AF_INET6 if ":" in ip else socket.AF_INET
        sockaddr = (ip, port, 0, 0) if family == socket.AF_INET6 else (ip, port)
        entries.append((family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", sockaddr))
    return entries


class _RecordingStream(httpcore.AsyncNetworkStream):
    def __init__(self, events: list[tuple[Any, ...]], raw_response: bytes) -> None:
        self._events = events
        self._response = raw_response
        self._sent = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:  # noqa: ASYNC109
        if self._sent:
            return b""
        self._sent = True
        return self._response

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:  # noqa: ASYNC109
        self._events.append(("write", buffer))

    async def aclose(self) -> None:
        return None

    async def start_tls(
        self,
        ssl_context: Any,
        server_hostname: str | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> httpcore.AsyncNetworkStream:
        self._events.append(("start_tls", server_hostname, bool(ssl_context.check_hostname)))
        return self

    def get_extra_info(self, info: str) -> Any:
        return None


class _RecordingNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, events: list[tuple[Any, ...]], responses: list[bytes]) -> None:
        self._events = events
        self._responses = list(responses)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        self._events.append(("connect_tcp", host, port))
        payload = self._responses.pop(0)
        return _RecordingStream(self._events, payload)


def _http_json_response(status_line: str, body: dict[str, Any]) -> bytes:
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    return (
        f"HTTP/1.1 {status_line}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(raw)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii") + raw


# ---------------------------------------------------------------------------
# 1. Account ID Scrubbing Completeness & Parity
# ---------------------------------------------------------------------------


def test_configured_account_ids_and_scrubbing_parity_across_all_fields(settings, message):
    rich_settings = settings.model_copy(
        update={
            "owner_qq_id": "20000",
            "onebot_self_id": "10000",
            "onebot_owner_ids": {"20000", "20001"},
            "onebot_allowed_users": {"30000", "30001"},
            "onebot_allowed_groups": {"40000", "50000"},
            "bot_qq_ids": {"60000"},
            "muted_user_ids": {"70000"},
            "qq_app_id": "99999",
            "qq_owner_ids": {"qq-owner-open.id+1"},
            "qq_allowed_users": {"qq-friend-openid"},
            "qq_allowed_groups": {"qq-group-openid"},
            "social_owner_bindings": {
                "discord": OwnerBinding(bot_id="disc-bot", user_id="discord-owner-888"),
            },
            "voice_reply_allowed_groups": {"onebot:811111", "822222"},
            "roaming_group_ids": {"onebot:833333"},
            "bot_reply_thresholds": {"844444": 0.9},
        }
    )
    msg = message.model_copy(
        update={
            "user_id": "21111",
            "conversation_id": "41111",
        }
    )
    batch = [
        msg,
        message.model_copy(update={"user_id": "31111", "conversation_id": "41111"}),
    ]

    ids = _configured_account_ids(msg, rich_settings, batch_messages=batch)
    expected_ids = {
        "21111",
        "41111",
        "31111",
        "20000",
        "10000",
        "20001",
        "30000",
        "30001",
        "40000",
        "50000",
        "60000",
        "70000",
        "99999",
        "qq-owner-open.id+1",
        "qq-friend-openid",
        "qq-group-openid",
        "discord-owner-888",
        "onebot:811111",
        "811111",
        "822222",
        "onebot:833333",
        "833333",
        "844444",
    }
    assert expected_ids <= ids

    # Every single sensitive ID is scrubbed identically by BOTH entrypoints.
    for sensitive in sorted(expected_ids):
        raw_text = f"英伟达财报 {sensitive} 最新进展"
        model_clean = sanitize_model_search_query(
            raw_text, msg, rich_settings, batch_messages=batch
        )
        user_msg = msg.model_copy(update={"text": raw_text, "command_text": None})
        user_clean = search_query(user_msg, rich_settings, batch_messages=batch)
        assert model_clean == "英伟达财报 最新进展", f"model query leaked {sensitive}"
        assert user_clean == "英伟达财报 最新进展", f"search_query leaked {sensitive}"
        assert "onebot:" not in model_clean
        assert "onebot:" not in user_clean


def test_account_id_scrubbing_handles_dict_bindings_numeric_boundaries_and_minimal_settings(
    message, monkeypatch
):
    minimal_settings = SimpleNamespace(
        onebot_owner_ids={"12345", ""},
        social_owner_bindings={
            "telegram": {"bot_id": "tg-bot", "user_id": "tg_owner+99"},
            "slack": OwnerBinding(bot_id="sl-bot", user_id="slack-owner-77"),
        },
        voice_reply_allowed_groups={"onebot:556677", "onebot:"},
        roaming_group_ids={"discord:998877"},
        bot_reply_thresholds={"776655": 0.8},
    )
    msg = message.model_copy(update={"user_id": "", "conversation_id": ""})

    # Helper works cleanly on minimal mock settings and None without AttributeError.
    assert _configured_account_ids(msg, None) == set()
    assert _configured_account_ids(msg, SimpleNamespace()) == set()
    extracted = _configured_account_ids(msg, minimal_settings)
    assert extracted == {
        "12345",
        "tg_owner+99",
        "slack-owner-77",
        "onebot:556677",
        "556677",
        "onebot:",
        "discord:998877",
        "998877",
        "776655",
    }

    # Numeric boundary: standalone 12345 is scrubbed, while 9123456 stays intact.
    text = (
        "预算 9123456 元 12345 tg_owner+99 slack-owner-77 "
        "onebot:556677 556677 discord:998877 998877 776655 公开新闻"
    )
    assert " ".join(_scrub_account_ids(text, msg, minimal_settings).split()) == (
        "预算 9123456 元 公开新闻"
    )

    monkeypatch.setattr("hakimi.web_search.configured_secrets", lambda _s: [])
    monkeypatch.setattr("hakimi.memory_compressor.configured_secrets", lambda _s: [])
    cleaned_model = sanitize_model_search_query(text, msg, minimal_settings)
    cleaned_user = search_query(
        msg.model_copy(update={"text": text, "command_text": None}),
        minimal_settings,
    )
    assert cleaned_model == "预算 9123456 元 公开新闻"
    assert cleaned_user == "预算 9123456 元 公开新闻"

    assert sanitize_model_search_query("Python 3.14 发布日期", msg, None) == "Python 3.14 发布日期"
    assert (
        search_query(
            msg.model_copy(update={"text": "/search Python 3.14 发布日期", "command_text": None}),
            SimpleNamespace(),
        )
        == "Python 3.14 发布日期"
    )


# ---------------------------------------------------------------------------
# 2. DeepSeek Connect-Time DNS Pinning & Anti-DNS-Rebinding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "resolved_ips",
    [
        ["127.0.0.1"],
        ["::1"],
        ["10.0.0.5"],
        ["172.16.0.1"],
        ["192.168.1.10"],
        ["169.254.169.254"],
        ["100.64.0.1"],
        ["198.18.0.1"],
        ["192.88.99.1"],
        ["0.0.0.0"],
        ["::"],
        ["224.0.0.1"],
        ["ff02::1"],
        ["240.0.0.1"],
        ["fe80::1"],
        ["fe80::1%en0"],
        ["fc00::1"],
        ["fec0::1"],
        ["::ffff:127.0.0.1"],
        ["::ffff:93.184.216.34"],
        ["2002:7f00:1::1"],
        ["2001::1"],
        ["93.184.216.34", "10.0.0.8"],
        ["93.184.216.34", "::ffff:127.0.0.1"],
    ],
)
async def test_execute_deepseek_search_blocks_non_public_dns_and_leaks_nothing(
    settings, monkeypatch, resolved_ips
):
    loop = asyncio.get_running_loop()
    dns_queries: list[tuple[str, int]] = []
    connect_attempts: list[Any] = []

    async def fake_getaddrinfo(host, port, *args, **kwargs):
        dns_queries.append((str(host), int(port)))
        return _addrinfo_entries(resolved_ips, int(port))

    orig_transport = httpx.AsyncHTTPTransport

    def spy_transport(*args, **kwargs):
        t = orig_transport(*args, **kwargs)
        t._pool._network_backend = _RecordingNetworkBackend(connect_attempts, [])
        return t

    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", spy_transport)

    secret_key = "sk-deepseek-super-secret-token-999"
    with pytest.raises(Rejected) as exc_info:
        await execute_deepseek_search(
            "英伟达最新新闻",
            settings,
            api_key=secret_key,
            base_url="https://custom-gw.deepseek.example",
            model="deepseek-flash",
            transport=None,
        )

    assert str(exc_info.value) == "web_search_failed"
    assert exc_info.value.__cause__ is None
    assert secret_key not in repr(exc_info.value)
    for bad_ip in resolved_ips:
        assert bad_ip not in repr(exc_info.value)
    assert dns_queries == [("custom-gw.deepseek.example", 443)]
    assert connect_attempts == []


@pytest.mark.parametrize("failure_mode", ["gaierror", "empty"])
async def test_execute_deepseek_search_rejects_dns_errors_cleanly(
    settings, monkeypatch, failure_mode
):
    loop = asyncio.get_running_loop()

    async def fake_getaddrinfo(host, port, *args, **kwargs):
        if failure_mode == "gaierror":
            raise socket.gaierror(8, "nodename nor servname provided")
        return []

    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(Rejected) as exc_info:
        await execute_deepseek_search(
            "公开科技新闻",
            settings,
            api_key="sk-deepseek-secret-key-1234",
            base_url="https://missing.deepseek.example",
            model="deepseek-flash",
            transport=None,
        )
    assert str(exc_info.value) == "web_search_failed"
    assert exc_info.value.__cause__ is None


async def test_execute_deepseek_search_blocks_metadata_host_before_dns(settings, monkeypatch):
    loop = asyncio.get_running_loop()
    called = False

    async def fail_getaddrinfo(*args, **kwargs):
        nonlocal called
        called = True
        return _addrinfo_entries(["93.184.216.34"])

    monkeypatch.setattr(loop, "getaddrinfo", fail_getaddrinfo)

    with pytest.raises(Rejected, match="^web_search_failed$"):
        await execute_deepseek_search(
            "公开新闻",
            settings,
            api_key="sk-deepseek-secret-key-1234",
            base_url="https://metadata.goog",
            model="deepseek-flash",
            transport=None,
        )
    assert called is False


@pytest.mark.parametrize(
    "public_ip",
    ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"],
)
async def test_execute_deepseek_search_pins_public_ip_and_preserves_tls_sni_and_host(
    settings, monkeypatch, public_ip
):
    loop = asyncio.get_running_loop()
    events: list[tuple[Any, ...]] = []
    wire_resp = _http_json_response(
        "200 OK",
        {
            "results": [
                {
                    "title": "权威快讯",
                    "url": "https://example.org/news",
                    "summary": "已核实的公开新闻摘要。",
                    "rerank_score": 2.5,
                }
            ]
        },
    )

    async def fake_getaddrinfo(host, port, *args, **kwargs):
        assert host == "custom-gw.deepseek.example"
        assert int(port) == 443
        return _addrinfo_entries([public_ip], 443)

    orig_transport = httpx.AsyncHTTPTransport

    def recording_transport(*args, **kwargs):
        t = orig_transport(*args, **kwargs)
        t._pool._network_backend = _RecordingNetworkBackend(events, [wire_resp])
        return t

    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", recording_transport)

    result = await execute_deepseek_search(
        "英伟达最新新闻",
        settings,
        api_key="sk-deepseek-secret-key-1234",
        base_url="https://custom-gw.deepseek.example/proxy",
        model="deepseek-flash",
        transport=None,
    )

    assert "已核实的公开新闻摘要" in result.text
    assert result.sources == (("权威快讯", "https://example.org/news"),)

    connect_events = [ev for ev in events if ev[0] == "connect_tcp"]
    tls_events = [ev for ev in events if ev[0] == "start_tls"]
    write_payload = b"".join(ev[1] for ev in events if ev[0] == "write")

    assert connect_events == [("connect_tcp", public_ip, 443)]
    assert tls_events == [("start_tls", "custom-gw.deepseek.example", True)]
    assert b"POST /proxy/v1/tools/search HTTP/1.1\r\n" in write_payload
    assert b"Host: custom-gw.deepseek.example\r\n" in write_payload
    assert b"Connection: close\r\n" in write_payload


async def test_execute_deepseek_search_blocks_dns_rebinding_during_chat_fallback(
    settings, monkeypatch
):
    loop = asyncio.get_running_loop()
    events: list[tuple[Any, ...]] = []
    dns_calls = 0

    async def rebinding_getaddrinfo(host, port, *args, **kwargs):
        nonlocal dns_calls
        dns_calls += 1
        # 1st request (/v1/tools/search) resolves publicly and gets 404;
        # 2nd request (/chat/completions) rebinds to loopback and MUST be blocked.
        return _addrinfo_entries(["93.184.216.34"] if dns_calls == 1 else ["127.0.0.1"], 443)

    orig_transport = httpx.AsyncHTTPTransport
    not_found_resp = _http_json_response("404 Not Found", {"error": "not_found"})

    def recording_transport(*args, **kwargs):
        t = orig_transport(*args, **kwargs)
        t._pool._network_backend = _RecordingNetworkBackend(events, [not_found_resp])
        return t

    monkeypatch.setattr(loop, "getaddrinfo", rebinding_getaddrinfo)
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", recording_transport)

    with pytest.raises(Rejected) as exc_info:
        await execute_deepseek_search(
            "公开科技新闻",
            settings,
            api_key="sk-deepseek-secret-key-1234",
            base_url="https://rebind.deepseek.example",
            model="deepseek-flash",
            transport=None,
        )

    assert str(exc_info.value) == "web_search_failed"
    assert dns_calls == 2
    connect_events = [ev for ev in events if ev[0] == "connect_tcp"]
    assert connect_events == [("connect_tcp", "93.184.216.34", 443)]


async def test_pinned_public_transport_failover_and_protocol_guards():
    seen: list[httpx.Request] = []

    def inner_handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "93.184.216.34":
            raise httpx.ConnectError("first ip unreachable")
        body = json.loads(request.content)
        return httpx.Response(200, json={"echo": body})

    async def multi_ip_resolver(_host: str, _port: int):
        return ["93.184.216.34", "93.184.216.35"]

    transport = _PinnedPublicTransport(
        inner=httpx.MockTransport(inner_handler),
        dns_resolver=multi_ip_resolver,
    )
    async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
        resp = await client.post(
            "https://multi.deepseek.example/v1/tools/search",
            json={"query": "test"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"echo": {"query": "test"}}
        assert [req.url.host for req in seen] == ["93.184.216.34", "93.184.216.35"]
        assert all(req.headers["host"] == "multi.deepseek.example" for req in seen)
        assert all(req.headers["connection"] == "close" for req in seen)
        assert all(req.extensions["sni_hostname"] == "multi.deepseek.example" for req in seen)

        with pytest.raises(Rejected, match="^web_search_failed$"):
            await client.get("http://multi.deepseek.example/plain")
        with pytest.raises(Rejected, match="^web_search_failed$"):
            await client.get("https://multi.deepseek.example:8443/custom-port")


async def test_execute_deepseek_search_preserves_injected_mock_transport_without_dns(
    settings, monkeypatch
):
    loop = asyncio.get_running_loop()

    async def forbid_dns(*args, **kwargs):
        raise AssertionError("Injected mock transport must not invoke system DNS")

    monkeypatch.setattr(loop, "getaddrinfo", forbid_dns)

    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "title": "测试信源",
                        "url": "https://example.org/mock",
                        "summary": "MockTransport 兼容测试结果。",
                        "rerank_score": 1.0,
                    }
                ]
            },
        )

    result = await execute_deepseek_search(
        "测试问题",
        settings,
        api_key="sk-deepseek-mock-key-1234",
        base_url="https://api.deepseek.com",
        model="deepseek-flash",
        transport=httpx.MockTransport(handler),
    )
    assert seen_paths == ["/v1/tools/search"]
    assert "MockTransport 兼容测试结果" in result.text
