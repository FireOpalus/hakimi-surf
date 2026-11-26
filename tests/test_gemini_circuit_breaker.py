"""Tests for granular `gemini_paid` circuit breaker and delayed retired client reclamation."""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.provider_routing import ProviderCredentialsUpdate, ProviderRoutingConfig
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from hakimi.web_search import WebSearchConfig
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_video_gateway import Files, local_video, manager

_ = local_video


def _chat_response(text: str = "你好喵。") -> dict:
    resp = interaction_response()
    resp["steps"][0]["content"][0]["text"] = json.dumps(
        {
            "abstain": False,
            "bubbles": [text],
            "anchor": {},
            "sticker": {},
            "image_digest": None,
        },
        ensure_ascii=False,
    )
    return resp


@pytest.mark.parametrize("status", [400, 404, 409, 413, 422])
async def test_client_4xx_never_trips_global_or_channel_breaker(settings, store, sdk_pair, status):
    calls: list[str] = []
    failing = True

    def handler(request):
        calls.append(request.url.path)
        if failing:
            return httpx.Response(status, json={"error": {"code": status, "message": "bad req"}})
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=_chat_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        for _ in range(5):
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        for _ in range(5):
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.generate({"current_message": "hi"}, "sub")

        assert len(calls) == 10
        assert gateway.blocked_until == 0.0
        assert gateway._channel_blocked_until == {}
        assert gateway._channel_failures == {}

        failing = False
        assert await gateway.screen({"current_message": "hi"}, "sub", purpose="message") == "chat"
        out = await gateway.generate({"current_message": "hi"}, "sub")
        assert json.loads(out)["bubbles"] == ["你好喵。"]
    finally:
        await gateway.close()


@pytest.mark.parametrize("status", [401, 403])
async def test_auth_401_403_fast_trips_global_circuit_across_all_lanes(
    settings, store, sdk_pair, status
):
    await store.runtime.web_search.save(WebSearchConfig(enabled=True))
    calls: list[str] = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(status, json={"error": {"code": status, "message": "denied"}})

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert len(calls) == 1
        assert gateway.blocked_until > time.monotonic()

        with pytest.raises(Rejected, match="gemini_circuit_open") as dec_exc:
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert dec_exc.value.provider_diagnostics[-1]["cause"] == "circuit_open"
        assert 0 < dec_exc.value.provider_diagnostics[-1]["cooldown_ms"] <= 60000

        with pytest.raises(Rejected, match="gemini_circuit_open") as chat_exc:
            await gateway.generate({"current_message": "hi"}, "sub")
        assert chat_exc.value.provider_diagnostics[-1]["cause"] == "circuit_open"

        settings.primary_model = "neon"
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.generate({"current_message": "hi"}, "sub")

        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.generate(
                {},
                "sub",
                scenario=Scenario.MEMORY_COMPRESSION,
                budget_lane="memory",
                memory_archive=True,
            )

        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.search("公开问题", "sub")

        assert len(calls) == 1
    finally:
        await gateway.close()


async def test_429_isolates_per_channel_without_blocking_other_lanes(settings, store, sdk_pair):
    calls: list[tuple[str, str]] = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            calls.append(("decision", request.url.path))
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        model = body.get("model", "")
        calls.append((model, request.url.path))
        if model == "gemini-4-hydrogen":
            return httpx.Response(429, json={"error": {"code": 429, "message": "rate limit"}})
        return httpx.Response(200, json=_chat_response(f"ok-{model}"))

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # 429 on hydrogen opens ONLY the hydrogen channel, never global blocked_until.
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.generate({"current_message": "hi"}, "sub")
        assert gateway.blocked_until == 0.0
        assert gateway._paid_circuit_remaining("hydrogen") > 0
        assert gateway._paid_circuit_remaining("decision") == 0.0
        assert gateway._paid_circuit_remaining("neon") == 0.0
        assert gateway._paid_circuit_remaining("carbon") == 0.0

        before = len(calls)
        with pytest.raises(Rejected, match="gemini_circuit_open") as exc_info:
            await gateway.generate({"current_message": "hi"}, "sub")
        assert len(calls) == before
        [diag] = exc_info.value.provider_diagnostics
        assert diag["cause"] == "circuit_open"
        assert diag["model_lane"] == "hydrogen"
        assert 0 < diag["cooldown_ms"] <= 60000

        # Decision, neon, and carbon remain completely unaffected.
        assert await gateway.screen({"current_message": "hi"}, "sub", purpose="message") == "chat"

        settings.primary_model = "neon"
        out_neon = await gateway.generate({"current_message": "hi"}, "sub")
        assert json.loads(out_neon)["bubbles"] == ["ok-gemini-4-neon"]

        settings.primary_model = "carbon"
        out_carbon = await gateway.generate({"current_message": "hi"}, "sub")
        assert json.loads(out_carbon)["bubbles"] == ["ok-gemini-4-carbon"]
    finally:
        await gateway.close()


async def test_5xx_timeout_and_network_require_consecutive_failures_and_reset_on_success(
    settings, store, sdk_pair, monkeypatch
):
    now = [1000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])

    mode = ["503"]
    calls: list[str] = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("decisions"):
            if mode[0] == "503":
                return httpx.Response(503, json={"error": {"code": 503, "message": "down"}})
            if mode[0] == "timeout":
                raise httpx.ReadTimeout("slow", request=request)
            if mode[0] == "network":
                raise httpx.ConnectError("reset", request=request)
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=_chat_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # 2 consecutive 503s do NOT open the breaker (threshold is 3).
        for _ in range(2):
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert gateway._paid_circuit_remaining("decision") == 0.0
        assert gateway.blocked_until == 0.0

        # A single success resets the failure counter.
        mode[0] = "ok"
        assert await gateway.screen({"current_message": "hi"}, "sub", purpose="message") == "chat"
        assert "decision" not in gateway._channel_failures

        # 2 failures (timeout + network), then sliding window expires -> does NOT open on 3rd.
        mode[0] = "timeout"
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        mode[0] = "network"
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        now[0] += gateway._circuit_failure_window_seconds + 1.0
        mode[0] = "503"
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert gateway._paid_circuit_remaining("decision") == 0.0

        # 2 more 503s within the fresh window reach threshold (3 total) -> opens ONLY decision.
        for _ in range(2):
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert gateway._paid_circuit_remaining("decision") > 0
        assert gateway._paid_circuit_remaining("hydrogen") == 0.0
        assert gateway.blocked_until == 0.0

        before = len(calls)
        with pytest.raises(Rejected, match="gemini_circuit_open") as exc_info:
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        assert len(calls) == before
        assert exc_info.value.provider_diagnostics[-1]["cause"] == "circuit_open"

        # Unrelated channel (hydrogen) can still run while decision is open.
        direct_result = await gateway._call(
            "sub",
            "chat",
            "hydrogen",
            {"input": "hi"},
            64,
            10.0,
            lambda: gateway.chat_client.aio.interactions.create(
                model="gemini-4-hydrogen", input="hi"
            ),
        )
        assert direct_result is not None
    finally:
        await gateway.close()


async def test_flex_models_and_search_exempt_from_tripping_chat_or_decision_breakers(
    settings, store, sdk_pair
):
    await store.runtime.web_search.save(WebSearchConfig(enabled=True))
    mode = ["503"]

    def handler(request):
        if mode[0] == "ok":
            if request.url.path.endswith("decisions"):
                return httpx.Response(200, json=decision_response())
            return httpx.Response(200, json=_chat_response())
        if mode[0] == "429":
            return httpx.Response(429, json={"error": {"code": 429, "message": "busy"}})
        if mode[0] == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        return httpx.Response(503, json={"error": {"code": 503, "message": "down"}})

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    gateway._search_retry_backoff = 0.0
    try:
        for kind in ("503", "503", "503", "429", "timeout"):
            mode[0] = kind
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.generate(
                    {},
                    "sub",
                    scenario=Scenario.MEMORY_COMPRESSION,
                    budget_lane="memory",
                    memory_archive=True,
                )
            with pytest.raises(Rejected, match="gemini_unavailable"):
                await gateway.search("公开问题", "sub")

        assert gateway.blocked_until == 0.0
        assert gateway._channel_blocked_until == {}
        assert gateway._channel_failures == {}

        mode[0] = "ok"
        assert await gateway.screen({"current_message": "hi"}, "sub", purpose="message") == "chat"
        out = await gateway.generate({"current_message": "hi"}, "sub")
        assert json.loads(out)["bubbles"] == ["你好喵。"]
    finally:
        await gateway.close()


async def test_generate_retry_timeout_uses_channel_breaker_not_global(settings, store, sdk_pair):
    calls: list[str] = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            calls.append("decision")
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        model = body.get("model", "")
        calls.append(model)
        if model == "gemini-4-hydrogen":
            raise httpx.ReadTimeout("slow-hydrogen", request=request)
        return httpx.Response(200, json=_chat_response("neon-ok"))

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # Turn 1: medium -> low retry both time out (2 timeouts < threshold 3).
        with pytest.raises(Rejected, match="generation_incomplete"):
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=2))
        assert gateway.blocked_until == 0.0
        assert gateway._paid_circuit_remaining("hydrogen") == 0.0

        # Turn 2: next retry timeout pushes hydrogen past threshold -> opens ONLY hydrogen.
        with pytest.raises(Rejected, match="generation_incomplete"):
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=2))
        assert gateway.blocked_until == 0.0
        assert gateway._paid_circuit_remaining("hydrogen") > 0
        assert gateway._paid_circuit_remaining("decision") == 0.0
        assert gateway._paid_circuit_remaining("neon") == 0.0

        # Turn 3 on hydrogen fails fast with gemini_circuit_open (no new network call).
        before = len(calls)
        with pytest.raises(Rejected, match="gemini_circuit_open") as exc_info:
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=2))
        assert len(calls) == before
        assert exc_info.value.provider_diagnostics[-1]["cause"] == "circuit_open"

        # Decision and neon continue working cleanly.
        assert await gateway.screen({"current_message": "hi"}, "sub", purpose="message") == "chat"
        settings.primary_model = "neon"
        out = await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=2))
        assert json.loads(out)["bubbles"] == ["neon-ok"]
    finally:
        await gateway.close()


async def test_watch_video_channel_aware_circuit_breaker(
    settings, store, sdk_pair, local_video, message
):
    files = Files()

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=interaction_response())

    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(handler),
        files_client=SimpleNamespace(aio=SimpleNamespace(files=files)),
    )
    gateway.video_files = manager(store, files)
    try:
        # 1. Unrelated channel (carbon) open while primary_model is hydrogen -> watch_video runs.
        gateway._record_paid_failure("carbon", "chat", status=429)
        assert gateway._paid_circuit_remaining("carbon") > 0
        text = await gateway.watch_video(
            local_video, {"title": "test"}, "sub", duration=20, message=message
        )
        assert text == "你好喵。"
        assert any(op == "upload" for op, _ in files.calls)

        # 2. Primary generation channel (hydrogen) open -> fails fast BEFORE upload.
        files.calls.clear()
        gateway._record_paid_failure("hydrogen", "chat", status=429)
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.watch_video(
                local_video, {"title": "test"}, "sub", duration=20, message=message
            )
        assert files.calls == []

        # 3. Decision channel open -> fails fast BEFORE upload.
        gateway.blocked_until = 0
        gateway._record_paid_failure("decision", "chat", status=429)
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.watch_video(
                local_video, {"title": "test"}, "sub", duration=20, message=message
            )
        assert files.calls == []

        # 4. Global blocked_until override -> fails fast BEFORE upload.
        gateway.blocked_until = time.monotonic() + 30.0
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.watch_video(
                local_video, {"title": "test"}, "sub", duration=20, message=message
            )
        assert files.calls == []
    finally:
        await gateway.close()


async def test_blocked_until_backward_compat_override_and_clear(settings, store, sdk_pair):
    calls: list[str] = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=_chat_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # Manual global override blocks all lanes.
        gateway.blocked_until = time.monotonic() + 30.0
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.screen({"current_message": "hi"}, "sub", purpose="message")
        with pytest.raises(Rejected, match="gemini_circuit_open"):
            await gateway.generate({"current_message": "hi"}, "sub")
        assert calls == []

        # Setting blocked_until = 0 clears both global and per-channel cooldowns.
        gateway._record_paid_failure("hydrogen", "chat", status=429)
        gateway.blocked_until = 0.0
        assert gateway._paid_circuit_remaining("hydrogen") == 0.0
        assert gateway._paid_circuit_remaining("decision") == 0.0
        out = await gateway.generate({"current_message": "hi"}, "sub")
        assert json.loads(out)["bubbles"] == ["你好喵。"]
    finally:
        await gateway.close()


async def test_reload_provider_clients_delayed_reclamation_and_separate_transport_tracking(
    settings, store, monkeypatch
):
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "false")
    gateway = GeminiGateway(settings, store)
    gateway._retire_delay_seconds = 0.05
    try:
        startup_transports = list(gateway._transports)
        assert len(startup_transports) == 2

        # Incomplete Vertex SA config (no project) must not create/leak any transport.
        await gateway.routing.set_config(
            ProviderRoutingConfig(vertex_enabled=True, vertex_auth_mode="service_account")
        )
        gateway.reload_provider_clients()
        assert gateway.vertex_chat_client is None
        assert gateway._owned_vertex_chat_transport is None
        assert gateway._transports == startup_transports

        # Configure runtime-owned free_decision and vertex_chat clients.
        await gateway.routing.set_config(
            ProviderRoutingConfig(vertex_enabled=True, vertex_auth_mode="api_key")
        )
        await gateway.routing.update_credentials(
            ProviderCredentialsUpdate(
                gemini_free_api_key="AIzaSyFreeKeyOne1234567890",
                vertex_api_key="AIzaSyVertexKeyOne1234567890",
            )
        )
        gateway.reload_provider_clients()

        assert gateway._transports == startup_transports
        old_free_client = gateway._owned_free_decision_client
        old_free_transport = gateway._owned_free_decision_transport
        old_vertex_client = gateway._owned_vertex_chat_client
        old_vertex_transport = gateway._owned_vertex_chat_transport
        assert old_free_client is not None and old_free_transport is not None
        assert old_vertex_client is not None and old_vertex_transport is not None

        close_events: list[str] = []

        def _spy(client, label: str):
            orig_aclose = client.aio.aclose
            orig_close = client.close

            async def wrapped_aclose():
                close_events.append(f"{label}:aclose")
                await orig_aclose()

            def wrapped_close():
                close_events.append(f"{label}:close")
                orig_close()

            client.aio.aclose = wrapped_aclose
            client.close = wrapped_close

        _spy(old_free_client, "free")
        _spy(old_vertex_client, "vertex")

        # Rotate credentials -> old clients/transports are retired asynchronously after grace delay.
        await gateway.routing.update_credentials(
            ProviderCredentialsUpdate(
                gemini_free_api_key="AIzaSyFreeKeyTwo1234567890",
                vertex_api_key="AIzaSyVertexKeyTwo1234567890",
            )
        )
        gateway.reload_provider_clients()

        assert gateway._transports == startup_transports
        new_free_transport = gateway._owned_free_decision_transport
        new_vertex_transport = gateway._owned_vertex_chat_transport
        assert new_free_transport is not None and new_free_transport is not old_free_transport
        assert new_vertex_transport is not None and new_vertex_transport is not old_vertex_transport

        # Immediately after reload (within grace delay): still open so in-flight calls drain.
        assert not old_free_transport.is_closed
        assert not old_vertex_transport.is_closed
        assert len(gateway._retired_provider_clients) == 2
        assert len(gateway._retire_tasks) == 1

        # Wait past grace period -> retired clients & transports are closed and untracked.
        await asyncio.sleep(0.15)
        assert old_free_transport.is_closed
        assert old_vertex_transport.is_closed
        assert "free:aclose" in close_events and "free:close" in close_events
        assert "vertex:aclose" in close_events and "vertex:close" in close_events
        assert gateway._retired_provider_clients == []
        assert not gateway._retire_tasks

        # Newly active runtime transports remain open.
        assert not new_free_transport.is_closed
        assert not new_vertex_transport.is_closed
    finally:
        await gateway.close()

    assert new_free_transport.is_closed
    assert new_vertex_transport.is_closed
    assert all(t.is_closed for t in startup_transports)


async def test_close_cancels_pending_retire_tasks_and_is_idempotent(settings, store, monkeypatch):
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "false")
    gateway = GeminiGateway(settings, store)
    gateway._retire_delay_seconds = 60.0

    await gateway.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_auth_mode="api_key")
    )
    await gateway.routing.update_credentials(
        ProviderCredentialsUpdate(
            gemini_free_api_key="AIzaSyFreeCloseOne1234567890",
            vertex_api_key="AIzaSyVertexCloseOne1234567890",
        )
    )
    gateway.reload_provider_clients()
    retired_free_transport = gateway._owned_free_decision_transport
    retired_vertex_transport = gateway._owned_vertex_chat_transport
    assert retired_free_transport is not None and retired_vertex_transport is not None

    await gateway.routing.update_credentials(
        ProviderCredentialsUpdate(
            gemini_free_api_key="AIzaSyFreeCloseTwo1234567890",
            vertex_api_key="AIzaSyVertexCloseTwo1234567890",
        )
    )
    gateway.reload_provider_clients()
    active_free_transport = gateway._owned_free_decision_transport
    active_vertex_transport = gateway._owned_vertex_chat_transport
    assert active_free_transport is not None and active_vertex_transport is not None
    assert len(gateway._retire_tasks) == 1
    assert len(gateway._retired_provider_clients) == 2

    # close() cancels the 60s sleep task immediately and closes all active + retired resources.
    t0 = time.monotonic()
    await gateway.close()
    assert time.monotonic() - t0 < 5.0
    assert not gateway._retire_tasks
    assert gateway._retired_provider_clients == []
    assert retired_free_transport.is_closed
    assert retired_vertex_transport.is_closed
    assert active_free_transport.is_closed
    assert active_vertex_transport.is_closed
    assert all(t.is_closed for t in gateway._transports)

    # Calling close() a second time is completely idempotent without errors.
    await gateway.close()


async def test_low_thinking_timeout_and_stream_provider_error_circuit_behavior(
    settings, store, sdk_pair
):
    mode = ["timeout"]

    def sse_error(code: str) -> httpx.Response:
        event = {"event_type": "error", "error": {"code": code, "message": "err"}}
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=("data: " + json.dumps(event) + "\n\n").encode(),
        )

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        if mode[0] == "timeout":
            raise httpx.ReadTimeout("slow-low", request=request)
        if mode[0] == "sse_400":
            return sse_error("400")
        if mode[0] == "sse_429":
            return sse_error("RESOURCE_EXHAUSTED")
        return httpx.Response(200, json=_chat_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # Low-thinking chat timeout (no downgrade retry) requires 3 consecutive timeouts.
        for _ in range(2):
            with pytest.raises(Rejected, match="generation_incomplete"):
                await gateway.generate(
                    {"current_message": "hi"}, "sub", route=RouteDecision(tier=1)
                )
        assert gateway._paid_circuit_remaining("hydrogen") == 0.0
        with pytest.raises(Rejected, match="generation_incomplete"):
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=1))
        assert gateway._paid_circuit_remaining("hydrogen") > 0
        assert gateway.blocked_until == 0.0

        # SSE stream 400 never trips breaker; SSE stream 429 trips ONLY the active channel.
        gateway.blocked_until = 0
        mode[0] = "sse_400"
        with pytest.raises(Rejected, match="stream_provider_error"):
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=1))
        assert gateway._paid_circuit_remaining("hydrogen") == 0.0
        assert gateway.blocked_until == 0.0

        mode[0] = "sse_429"
        with pytest.raises(Rejected, match="stream_provider_error"):
            await gateway.generate({"current_message": "hi"}, "sub", route=RouteDecision(tier=1))
        assert gateway._paid_circuit_remaining("hydrogen") > 0
        assert gateway._paid_circuit_remaining("decision") == 0.0
        assert gateway.blocked_until == 0.0
    finally:
        await gateway.close()


async def test_default_retire_delay_and_sync_reload_without_loop_cleanup(
    settings, store, monkeypatch
):
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "false")
    gateway = GeminiGateway(settings, store)
    assert gateway._retire_delay_seconds == (
        max(float(settings.decision_timeout), float(settings.generation_timeout)) + 5.0
    )

    await gateway.routing.update_credentials(
        ProviderCredentialsUpdate(gemini_free_api_key="AIzaSyFreeSyncOne1234567890")
    )
    gateway.reload_provider_clients()
    first_transport = gateway._owned_free_decision_transport
    assert first_transport is not None

    # Simulate reload_provider_clients() invoked when no event loop is running.
    await gateway.routing.update_credentials(
        ProviderCredentialsUpdate(gemini_free_api_key="AIzaSyFreeSyncTwo1234567890")
    )
    monkeypatch.setattr(
        "hakimi.gemini.asyncio.get_running_loop",
        lambda: (_ for _ in ()).throw(RuntimeError("no running event loop")),
    )
    gateway.reload_provider_clients()
    assert len(gateway._retired_provider_clients) == 1
    assert not gateway._retired_provider_clients[0].scheduled
    assert not first_transport.is_closed

    # close() still closes the unscheduled retired client & transport cleanly.
    await gateway.close()
    assert first_transport.is_closed
    assert gateway._retired_provider_clients == []
