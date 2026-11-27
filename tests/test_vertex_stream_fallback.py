import asyncio
import json
import time
from decimal import Decimal

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.provider_routing import ProviderRoutingConfig
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeSender
from tests.js_runtime import run_js_harness
from tests.test_gateway import group
from tests.test_provider_routing_and_vertex import _client_pair, _close_clients
from tests.test_sdk_contract import decision_response, interaction_response, wire_input_text


def sse(*events):
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content="".join("data: " + json.dumps(event) + "\n\n" for event in events),
    )


def stream_error(code):
    # The human-readable message is deliberately misleading and must never enter the audit.
    return {"event_type": "error", "error": {"code": code, "message": "private text about 429"}}


def _paygo_headers(headers: dict[str, str]) -> dict[str, str]:
    return {
        k.lower(): v
        for k, v in headers.items()
        if k.lower() in {"x-vertex-ai-llm-request-type", "x-vertex-ai-llm-shared-request-type"}
    }


@pytest.fixture
async def routes(settings, store):
    requests = {"vertex": [], "developer": [], "vertex_headers": []}
    responses = {"vertex": [], "developer": []}

    def handler(provider):
        def respond(request):
            if request.url.path.endswith("decisions"):
                return httpx.Response(200, json=decision_response())
            requests[provider].append(json.loads(request.content))
            if provider == "vertex":
                requests["vertex_headers"].append(_paygo_headers(dict(request.headers)))
            response = responses[provider].pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        return respond

    clients, transports = _client_pair(handler("developer"))
    vertex, v_clients, v_transports = _client_pair(handler("vertex"), vertex=True)
    gateway = GeminiGateway(settings, store, *clients, vertex_chat_client=vertex)
    await gateway.routing.set_config(ProviderRoutingConfig(vertex_enabled=True))
    try:
        yield gateway, requests, responses
    finally:
        await gateway.close()
        await _close_clients([*clients, *v_clients], transports + v_transports)


async def test_thought_only_429_retries_priority_paygo_before_developer(
    settings, store, message, routes
):
    gateway, calls, replies = routes
    replies["vertex"].extend(
        [
            sse(
                {"event_type": "step.start", "index": 0, "step": {"type": "thought"}},
                {"event_type": "step.stop", "index": 0},
                {"event_type": "step.start", "index": 1, "step": {"type": "model_output"}},
                {"event_type": "step.delta", "index": 1, "delta": {"type": "text", "text": ""}},
                stream_error("rate_limit_exceeded"),
            ),
            httpx.Response(200, json=interaction_response()),
        ]
    )
    sender = FakeSender()
    svc = Service(settings, store, gateway, FakeBili(), sender)
    msg = group(message, addressed=True, mentioned_ids=["10000"], attention_reason="mention")
    try:
        assert await svc.accept(msg) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert not await svc.deliver_once()
        assert len(sender.sent) == 1
        assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 0
        assert calls["vertex_headers"] == [
            {"x-vertex-ai-llm-request-type": "shared"},
            {
                "x-vertex-ai-llm-request-type": "shared",
                "x-vertex-ai-llm-shared-request-type": "priority",
            },
        ]
        assert gateway.vertex_blocked_until == 0.0
        assert (await store.one("SELECT COUNT(*) FROM outbox"))[0] == 1
        [audit] = await gateway.filters.recent_audit()
        assert audit["execution_status"] == "sent"
        assert audit["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "rate_limit_exceeded",
            "http_status": 429,
            "fallback": "succeeded",
            "fallback_target": "vertex_priority",
        }
        [failure] = audit["provider_diagnostics"]
        assert (
            failure["stage"],
            failure["cause"],
            failure["service_tier"],
            failure["fallback"],
        ) == (
            "generation",
            "stream_error",
            "standard",
            "succeeded",
        )
        assert failure["stream_phase"] == "started" and "http_status" not in failure
        assert "private text" not in json.dumps(audit)
        rows = await store.all(
            "SELECT p.provider,l.status FROM ledger l JOIN ledger_usage u ON u.id=l.id "
            "JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [tuple(row) for row in rows] == [
            ("vertex_paid", "unknown"),
            ("vertex_paid", "settled"),
        ]
    finally:
        await svc.stop()


async def test_thought_only_429_falls_back_to_developer_after_priority_429(
    settings, store, message, routes
):
    gateway, calls, replies = routes
    thought_429 = sse(
        {"event_type": "step.start", "index": 0, "step": {"type": "thought"}},
        {"event_type": "step.stop", "index": 0},
        {"event_type": "step.start", "index": 1, "step": {"type": "model_output"}},
        {"event_type": "step.delta", "index": 1, "delta": {"type": "text", "text": ""}},
        stream_error("rate_limit_exceeded"),
    )
    replies["vertex"].extend([thought_429, thought_429])
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    sender = FakeSender()
    svc = Service(settings, store, gateway, FakeBili(), sender)
    msg = group(message, addressed=True, mentioned_ids=["10000"], attention_reason="mention")
    try:
        assert await svc.accept(msg) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert not await svc.deliver_once()
        assert len(sender.sent) == 1
        assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 1
        assert calls["vertex_headers"] == [
            {"x-vertex-ai-llm-request-type": "shared"},
            {
                "x-vertex-ai-llm-request-type": "shared",
                "x-vertex-ai-llm-shared-request-type": "priority",
            },
        ]
        assert gateway.vertex_blocked_until > time.monotonic()
        [audit] = await gateway.filters.recent_audit()
        assert audit["execution_status"] == "sent"
        assert audit["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "rate_limit_exceeded",
            "http_status": 429,
            "fallback": "succeeded",
            "fallback_target": "gemini_paid",
        }
        assert len(audit["provider_diagnostics"]) == 2
        std_fail, prio_fail = audit["provider_diagnostics"]
        assert (std_fail["service_tier"], std_fail["fallback"]) == ("standard", "failed")
        assert (prio_fail["service_tier"], prio_fail["fallback"]) == ("priority", "succeeded")
        rows = await store.all(
            "SELECT p.provider,l.status FROM ledger l JOIN ledger_usage u ON u.id=l.id "
            "JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [tuple(row) for row in rows] == [
            ("vertex_paid", "unknown"),
            ("vertex_paid", "unknown"),
            ("gemini_paid", "settled"),
        ]
    finally:
        await svc.stop()


@pytest.mark.parametrize(
    "stage,code,enabled,reason,expected_vertex_calls",
    [
        ("start", "429", True, "partial_output", 1),
        ("delta", "RESOURCE_EXHAUSTED", True, "partial_output", 1),
        ("empty", "quota_exceeded", False, "disabled", 2),
        ("empty", "api_error", True, "ineligible", 1),
        ("empty", "bad code with private text", True, "ineligible", 1),
    ],
)
async def test_stream_fallback_boundaries(
    routes, message, stage, code, enabled, reason, expected_vertex_calls
):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=enabled)
    )
    step = {"type": "model_output", "content": []}
    events = [{"event_type": "step.start", "index": 0, "step": step}]
    if stage == "start":
        step["content"].append({"type": "text", "text": "{"})
    if stage == "delta":
        events.append(
            {"event_type": "step.delta", "index": 0, "delta": {"type": "text", "text": "{"}}
        )
    for _ in range(expected_vertex_calls):
        replies["vertex"].append(sse(*events, stream_error(code)))
    trace = {}
    with pytest.raises(Rejected, match="stream_provider_error"):
        await gate.generate(
            {"current_message": "hi"}, "subject", message=message, generation_trace=trace
        )
    assert len(calls["vertex"]) == expected_vertex_calls and not calls["developer"]
    assert trace["provider_diagnostics"][-1]["fallback"] == reason
    if expected_vertex_calls == 2:
        assert [d["service_tier"] for d in trace["provider_diagnostics"]] == [
            "standard",
            "priority",
        ]
        assert gate.vertex_blocked_until == 0.0
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "unknown" if " " in code else code,
        "fallback": reason,
    }


async def test_priority_paygo_succeeds_even_when_gemini_fallback_disabled(routes, message):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=False)
    )
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "standard 429"}}),
            httpx.Response(200, json=interaction_response()),
        ]
    )
    trace = {}
    out = await gate.generate(
        {"current_message": "hi"}, "subject", message=message, generation_trace=trace
    )
    assert json.loads(out)["bubbles"]
    assert len(calls["vertex"]) == 2 and not calls["developer"]
    assert calls["vertex_headers"] == [
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
    ]
    assert trace["provider"] == "vertex_paid"
    assert trace["vertex_service_tier"] == "priority"
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "succeeded",
        "fallback_target": "vertex_priority",
    }


@pytest.mark.parametrize(
    "failure,detail,code",
    [
        ("sse", "stream_provider_error", "too_many_requests"),
        ("http", "gemini_unavailable", "503"),
        ("timeout", "timeout", None),
        ("incomplete", "generation_incomplete", None),
    ],
)
async def test_developer_failure_cannot_restart_vertex_or_downgrade(
    routes, message, failure, detail, code
):
    gate, calls, replies = routes
    replies["vertex"].extend(
        [sse(stream_error("RESOURCE_EXHAUSTED")), sse(stream_error("RESOURCE_EXHAUSTED"))]
    )
    incomplete = {**interaction_response(), "status": "incomplete"}
    replies["developer"].append(
        {
            "sse": sse(stream_error("too_many_requests")),
            "http": httpx.Response(503, json={"error": {"code": 503, "message": "private error"}}),
            "timeout": httpx.ReadTimeout("test"),
            "incomplete": httpx.Response(200, json=incomplete),
        }[failure]
    )
    trace = {}
    with pytest.raises(Rejected):
        await gate.generate(
            {"current_message": "hi"}, "subject", message=message, generation_trace=trace
        )
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 1
    assert calls["vertex_headers"] == [
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
    ]
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "RESOURCE_EXHAUSTED",
        "fallback": "failed",
        "fallback_target": "gemini_paid",
        "fallback_detail": detail,
        **({"fallback_code": code} if code else {}),
    }


async def test_fallback_rebuilds_local_history_without_vertex_chain(routes, message):
    gate, calls, replies = routes
    replies["vertex"].extend(
        [
            httpx.Response(200, json=interaction_response()),
            sse(stream_error("quota_exceeded")),
            sse(stream_error("quota_exceeded")),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    history = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "一起去看星星"}]
    payload = {"current_message": "你好", "recent_messages": history}
    trace = {}
    await gate.generate(payload, "subject", message=message, generation_trace=trace)
    chain = {**trace["chain_commit"], "expires": time.time() + 600}
    await gate.generate(
        {**payload, "interaction_chains": {"hydrogen": chain}}, "subject", message=message
    )
    # Standard PayGo attempted with chain; after stream-started 429, Priority & Developer
    # both rebuild full local history without previous_interaction_id.
    assert calls["vertex"][1]["previous_interaction_id"] == chain["interaction_id"]
    assert "previous_interaction_id" not in calls["vertex"][2]
    assert "一起去看星星" in wire_input_text(calls["vertex"][2])
    [backup] = calls["developer"]
    assert "previous_interaction_id" not in backup
    assert "一起去看星星" in wire_input_text(backup)
    assert backup["generation_config"] == calls["vertex"][1]["generation_config"]


async def test_pre_stream_429_priority_keeps_vertex_chain_then_developer_drops_it(routes, message):
    gate, calls, replies = routes
    replies["vertex"].extend(
        [
            httpx.Response(200, json=interaction_response()),
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    history = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "一起去看星星"}]
    payload = {"current_message": "你好", "recent_messages": history}
    trace = {}
    await gate.generate(payload, "subject", message=message, generation_trace=trace)
    chain = {**trace["chain_commit"], "expires": time.time() + 600}
    await gate.generate(
        {**payload, "interaction_chains": {"hydrogen": chain}}, "subject", message=message
    )
    # Pre-stream HTTP 429 preserves valid Vertex chain on Priority retry, but Developer drops it.
    assert calls["vertex"][1]["previous_interaction_id"] == chain["interaction_id"]
    assert calls["vertex"][2]["previous_interaction_id"] == chain["interaction_id"]
    [backup] = calls["developer"]
    assert "previous_interaction_id" not in backup
    assert "一起去看星星" in wire_input_text(backup)


async def test_cancel_after_429_skips_fallback_reservation(routes, message, store, monkeypatch):
    gate, calls, replies = routes
    replies["vertex"].append(sse(stream_error("429")))
    cancel = asyncio.Event()
    settle = store.settle

    async def cancel_after_settlement(*args, **kwargs):
        await settle(*args, **kwargs)
        cancel.set()

    monkeypatch.setattr(store, "settle", cancel_after_settlement)
    trace = {}
    with pytest.raises(asyncio.CancelledError):
        await gate.generate(
            {"current_message": "hi"},
            "subject",
            message=message,
            generation_trace=trace,
            cancel_event=cancel,
        )
    assert len(calls["vertex"]) == 1 and not calls["developer"]
    assert (await store.one("SELECT COUNT(*) FROM ledger"))[0] == 1
    assert trace["provider_incident"]["fallback"] == "cancelled"
    assert trace["provider_incident"]["fallback_target"] == "vertex_priority"


async def test_fallback_rechecks_budget_before_dispatch(routes, message, store, settings):
    gate, calls, replies = routes
    replies["vertex"].append(sse(stream_error(429)))

    async def guard():
        if calls["vertex"]:
            settings.daily_usd = Decimal(0)

    trace = {}
    with pytest.raises(Rejected, match="budget_exhausted"):
        await gate.generate(
            {"current_message": "hi"},
            "subject",
            message=message,
            generation_trace=trace,
            before_call=guard,
        )
    assert len(calls["vertex"]) == 1 and not calls["developer"]
    assert (await store.one("SELECT COUNT(*) FROM ledger"))[0] == 1
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "not_dispatched",
        "fallback_target": "vertex_priority",
        "fallback_detail": "budget_exhausted_daily",
    }


@pytest.mark.parametrize("thought_only", [True, False])
async def test_vertex_generate_content_stream_429_respects_output_boundary(
    routes, message, thought_only
):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_protocol="generate_content")
    )
    part = {"text": "thinking" if thought_only else "{", "thought": thought_only}
    err_sse = sse(
        {"candidates": [{"content": {"role": "model", "parts": [part]}}]},
        {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "private text"}},
    )
    replies["vertex"].append(err_sse)
    if thought_only:
        replies["vertex"].append(err_sse)
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    trace = {}
    call = gate.generate(
        {"current_message": "hi"}, "subject", message=message, generation_trace=trace
    )
    if thought_only:
        assert json.loads(await call)["bubbles"]
    else:
        with pytest.raises(Rejected):
            await call
    assert len(calls["vertex"]) == (2 if thought_only else 1)
    assert len(calls["developer"]) == int(thought_only)
    if thought_only:
        assert calls["vertex_headers"] == [
            {"x-vertex-ai-llm-request-type": "shared"},
            {
                "x-vertex-ai-llm-request-type": "shared",
                "x-vertex-ai-llm-shared-request-type": "priority",
            },
        ]
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "succeeded",
            "fallback_target": "gemini_paid",
        }
    else:
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "partial_output",
        }


async def test_vertex_429_sets_cooldown_skipping_doomed_vertex_calls_when_fallback_enabled(
    routes, message
):
    gate, calls, replies = routes
    # Call 1: Standard 429 -> Priority 200 OK -> does NOT activate Vertex cooldown!
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(200, json=interaction_response()),
            # Call 2: Standard 429 -> Priority 429 -> falls back to Developer API & sets cooldown!
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    replies["developer"].extend(
        [
            httpx.Response(200, json=interaction_response()),
            httpx.Response(200, json=interaction_response()),
        ]
    )

    out1 = await gate.generate({"current_message": "你好"}, "subject", message=message)
    assert json.loads(out1)["bubbles"]
    assert len(calls["vertex"]) == 2
    assert len(calls["developer"]) == 0
    assert gate.vertex_blocked_until == 0.0

    # Call 2 tries Standard again, hits 429 on Standard & Priority -> Developer + 60s cooldown.
    out2 = await gate.generate({"current_message": "再来一句"}, "subject", message=message)
    assert json.loads(out2)["bubbles"]
    assert len(calls["vertex"]) == 4
    assert len(calls["developer"]) == 1
    assert gate.vertex_blocked_until > time.monotonic()
    assert calls["vertex_headers"] == [
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
    ]

    # Call 3 during cooldown skips Vertex completely and goes straight to Developer API.
    out3 = await gate.generate({"current_message": "冷却中"}, "subject", message=message)
    assert json.loads(out3)["bubbles"]
    assert len(calls["vertex"]) == 4
    assert len(calls["developer"]) == 2

    # Reloading provider clients clears the cooldown.
    gate.reload_provider_clients()
    assert gate.vertex_blocked_until == 0.0


async def test_non_streaming_vertex_429_standard_to_priority_to_gemini(routes):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_protocol="generate_content")
    )
    gc_ok = {
        "candidates": [
            {
                "content": {"role": "model", "parts": [{"text": "今日手账观察"}]},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 100,
            "candidatesTokenCount": 20,
            "thoughtsTokenCount": 30,
            "cachedContentTokenCount": 80,
            "totalTokenCount": 150,
            "trafficType": "ON_DEMAND_PRIORITY",
        },
    }
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(200, json=gc_ok),
            # Second non-streaming call: both Standard & Priority 429 -> Gemini Developer API
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    dev_ok = interaction_response()
    dev_ok["steps"][0]["content"][0]["text"] = "Gemini手账兜底"
    replies["developer"].append(httpx.Response(200, json=dev_ok))

    # 1. Non-streaming Standard 429 -> Priority 200 OK
    trace1 = {}
    out1 = await gate.generate(
        {"public_data": "今日总结"},
        "diary-sub",
        scenario=Scenario.DAILY_DIARY,
        generation_trace=trace1,
    )
    assert out1 == "今日手账观察"
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 0
    assert calls["vertex_headers"][:2] == [
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
    ]
    assert trace1["provider"] == "vertex_paid"
    assert trace1["vertex_service_tier"] == "priority"
    assert trace1["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "succeeded",
        "fallback_target": "vertex_priority",
    }
    assert gate.vertex_blocked_until == 0.0

    # Verify 1.8x Priority pricing with scaled cached discount:
    # Base (1.0x): (20*1.50 + 80*0.075 + 50*7.50)/1e6 = $0.000411 (411 microdollars)
    # Priority (1.8x): 411 * 1.8 = 739.8 -> ceil = 740 microdollars
    row1 = await gate.store.one(
        "SELECT estimated_amount FROM ledger_usage WHERE cached_tokens=80 "
        "ORDER BY rowid DESC LIMIT 1"
    )
    assert row1["estimated_amount"] == 740

    # 2. Non-streaming Standard 429 -> Priority 429 -> Gemini API
    trace2 = {}
    out2 = await gate.generate(
        {"public_data": "今日总结2"},
        "diary-sub",
        scenario=Scenario.DAILY_DIARY,
        generation_trace=trace2,
    )
    assert out2 == "Gemini手账兜底"
    assert len(calls["vertex"]) == 4 and len(calls["developer"]) == 1
    assert trace2["provider"] == "gemini_paid"
    assert "vertex_service_tier" not in trace2
    assert trace2["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "succeeded",
        "fallback_target": "gemini_paid",
    }
    assert gate.vertex_blocked_until > time.monotonic()


async def test_priority_paygo_server_downgrade_to_on_demand_settles_at_standard_rate(routes):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_protocol="generate_content")
    )
    gc_downgraded = {
        "candidates": [
            {
                "content": {"role": "model", "parts": [{"text": "降级标准计费"}]},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 100,
            "candidatesTokenCount": 20,
            "thoughtsTokenCount": 30,
            "cachedContentTokenCount": 80,
            "totalTokenCount": 150,
            "trafficType": "ON_DEMAND",
        },
    }
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(200, json=gc_downgraded),
        ]
    )
    out = await gate.generate(
        {"public_data": "今日总结"},
        "diary-sub",
        scenario=Scenario.DAILY_DIARY,
    )
    assert out == "降级标准计费"
    assert len(calls["vertex"]) == 2
    row = await gate.store.one(
        "SELECT estimated_amount FROM ledger_usage "
        "WHERE cached_tokens=80 ORDER BY rowid DESC LIMIT 1"
    )
    # Settled at 1.0x standard rate (411 microdollars), not 1.8x (740 microdollars).
    assert row["estimated_amount"] == 411


async def test_vertex_auto_404_not_repeated_on_priority_retry(routes):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_protocol="auto")
    )
    gc_ok = {
        "candidates": [
            {
                "content": {"role": "model", "parts": [{"text": "Priority直走GC"}]},
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 100,
            "candidatesTokenCount": 20,
            "thoughtsTokenCount": 0,
            "totalTokenCount": 120,
        },
    }
    replies["vertex"].extend(
        [
            # Stage 1 Standard: interactions 404 -> generateContent 429
            httpx.Response(404, json={"error": {"code": 404, "message": "not found"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            # Stage 2 Priority: skips interactions 404, hits generateContent directly -> 200 OK
            httpx.Response(200, json=gc_ok),
        ]
    )
    out = await gate.generate(
        {"public_data": "今日总结"}, "diary-sub", scenario=Scenario.DAILY_DIARY
    )
    assert out == "Priority直走GC"
    assert len(calls["vertex"]) == 3
    assert calls["vertex_headers"] == [
        {"x-vertex-ai-llm-request-type": "shared"},
        {"x-vertex-ai-llm-request-type": "shared"},
        {
            "x-vertex-ai-llm-request-type": "shared",
            "x-vertex-ai-llm-shared-request-type": "priority",
        },
    ]


def test_admin_js_renders_vertex_paygo_service_tier_and_fallback_targets():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js_src = (root / "admin" / "src" / "js" / "03-routing-reply.js").read_text(encoding="utf-8")
    start = js_src.index("const rejectDetails=")
    end = js_src.index("async function decisionAudit()")
    snippet = js_src[start:end]
    prio_row = json.dumps(
        {
            "execution_status": "sent",
            "provider_incident": {
                "provider": "vertex_paid",
                "code": "429",
                "http_status": 429,
                "fallback": "succeeded",
                "fallback_target": "vertex_priority",
            },
        }
    )
    dev_row = json.dumps(
        {
            "execution_status": "sent",
            "provider_incident": {
                "provider": "vertex_paid",
                "code": "429",
                "http_status": 429,
                "fallback": "succeeded",
                "fallback_target": "gemini_paid",
            },
        }
    )
    diag_row = json.dumps(
        {
            "provider_diagnostics": [
                {
                    "stage": "generation",
                    "provider": "vertex_paid",
                    "service_tier": "standard",
                    "model_lane": "hydrogen",
                    "cause": "http",
                    "http_status": 429,
                    "elapsed_ms": 120,
                    "timeout_ms": 45000,
                    "fallback": "failed",
                },
                {
                    "stage": "generation",
                    "provider": "vertex_paid",
                    "service_tier": "priority",
                    "model_lane": "hydrogen",
                    "cause": "http",
                    "http_status": 429,
                    "elapsed_ms": 180,
                    "timeout_ms": 45000,
                    "fallback": "succeeded",
                },
            ]
        }
    )
    harness = f"""
function el(tag, text, cls) {{
  return {{
    tag, text: text == null ? '' : String(text), cls: cls || '', children: [],
    append(...items) {{ this.children.push(...items); }},
    toText() {{
      const kids = this.children.map(c => typeof c === 'string' ? c : c.toText());
      return [this.text, ...kids].filter(Boolean).join(' | ');
    }}
  }};
}}
function dateTime(v) {{ return String(v); }}
{snippet}
const prioOutcome = auditOutcome({prio_row});
const devOutcome = auditOutcome({dev_row});
const diag = diagnosticCell({diag_row}).toText();
const out = JSON.stringify({{ prioOutcome, devOutcome, diag }});
if (typeof print === 'function') print(out); else console.log(out);
"""
    proc = run_js_harness(harness, cwd=root)
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip())
    assert "Vertex Priority PayGo 回退成功" in result["prioOutcome"]
    assert "Gemini Developer API 回退成功" in result["devOutcome"]
    assert "Standard PayGo" in result["diag"]
    assert "Priority PayGo" in result["diag"]
