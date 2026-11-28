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
            "SELECT p.provider,l.status,l.amount,u.estimated_amount "
            "FROM ledger l JOIN ledger_usage u ON u.id=l.id "
            "JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [(r[0], r[1]) for r in rows] == [
            ("vertex_paid", "settled"),
            ("vertex_paid", "settled"),
        ]
        assert rows[0]["amount"] == 0 and rows[0]["estimated_amount"] == 0
        assert rows[1]["amount"] > 0 and rows[1]["estimated_amount"] == rows[1]["amount"]
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
            "SELECT p.provider,l.status,l.amount,u.estimated_amount "
            "FROM ledger l JOIN ledger_usage u ON u.id=l.id "
            "JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [(r[0], r[1]) for r in rows] == [
            ("vertex_paid", "settled"),
            ("vertex_paid", "settled"),
            ("gemini_paid", "settled"),
        ]
        assert rows[0]["amount"] == 0 and rows[0]["estimated_amount"] == 0
        assert rows[1]["amount"] == 0 and rows[1]["estimated_amount"] == 0
        assert rows[2]["amount"] > 0 and rows[2]["estimated_amount"] == rows[2]["amount"]
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
    row = await store.one("SELECT status,amount FROM ledger")
    assert tuple(row) == ("settled", 0)
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "not_dispatched",
        "fallback_target": "gemini_paid",
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
        rows = await gate.store.all(
            "SELECT p.provider,l.status,l.amount FROM ledger l "
            "JOIN ledger_usage u ON u.id=l.id JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [(r[0], r[1]) for r in rows] == [
            ("vertex_paid", "settled"),
            ("vertex_paid", "settled"),
            ("gemini_paid", "settled"),
        ]
        assert rows[0]["amount"] == 0 and rows[1]["amount"] == 0 and rows[2]["amount"] > 0
    else:
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "partial_output",
        }
        row = await gate.store.one(
            "SELECT l.status,l.amount FROM ledger l "
            "JOIN ledger_usage u ON u.id=l.id WHERE u.model_lane='hydrogen'"
        )
        assert row["status"] == "unknown" and row["amount"] > 0


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


@pytest.mark.parametrize("reported_tier,expected_micro", [("standard", 411), ("priority", 740)])
async def test_streaming_interactions_priority_service_tier_billing(
    routes, message, reported_tier, expected_micro
):
    gate, calls, replies = routes
    resp = interaction_response()
    resp["service_tier"] = reported_tier
    resp["usage"] = {
        "total_input_tokens": 100,
        "total_output_tokens": 20,
        "total_thought_tokens": 30,
        "total_cached_tokens": 80,
        "total_tokens": 150,
    }
    replies["vertex"].extend(
        [
            sse(stream_error("rate_limit_exceeded")),
            httpx.Response(200, json=resp),
        ]
    )
    out = await gate.generate({"current_message": "你好"}, "subject", message=message)
    assert json.loads(out)["bubbles"]
    assert len(calls["vertex"]) == 2 and not calls["developer"]
    rows = await gate.store.all(
        "SELECT l.status,l.amount,u.estimated_amount "
        "FROM ledger l JOIN ledger_usage u ON u.id=l.id "
        "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
    )
    assert [tuple(r) for r in rows] == [
        ("settled", 0, 0),
        ("settled", expected_micro, expected_micro),
    ]


@pytest.mark.parametrize("fallback_enabled", [True, False])
async def test_stage2_priority_budget_exhausted_falls_back_to_developer_when_enabled(
    routes, message, store, settings, fallback_enabled
):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=fallback_enabled)
    )
    # Measure exact 1.0x reserve on Stage 1, then tighten daily_usd to sit strictly between
    # 1.0x (Standard / Gemini Developer API) and 1.8x (Vertex Priority).
    orig_reserve = store.reserve
    seen_reserves: list[Decimal] = []

    async def spy_reserve(*args, **kwargs):
        seen_reserves.append(args[2])
        if len(seen_reserves) == 1:
            settings.daily_usd = args[2] * Decimal("1.4")
        return await orig_reserve(*args, **kwargs)

    store.reserve = spy_reserve
    replies["vertex"].append(sse(stream_error("429")))
    replies["developer"].append(httpx.Response(200, json=interaction_response()))

    trace = {}
    if fallback_enabled:
        out = await gate.generate(
            {"current_message": "预算夹心测试"}, "subject", message=message, generation_trace=trace
        )
        assert json.loads(out)["bubbles"]
        assert len(calls["vertex"]) == 1 and len(calls["developer"]) == 1
        assert trace["provider"] == "gemini_paid"
        assert "vertex_service_tier" not in trace
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "succeeded",
            "fallback_target": "gemini_paid",
        }
        assert trace["provider_diagnostics"][0]["fallback"] == "succeeded"
        rows = await store.all(
            "SELECT p.provider,l.status,l.amount FROM ledger l "
            "JOIN ledger_usage u ON u.id=l.id JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [(r[0], r[1]) for r in rows] == [
            ("vertex_paid", "settled"),
            ("gemini_paid", "settled"),
        ]
        assert rows[0]["amount"] == 0 and rows[1]["amount"] > 0
    else:
        with pytest.raises(Rejected, match="budget_exhausted"):
            await gate.generate(
                {"current_message": "预算夹心测试"},
                "subject",
                message=message,
                generation_trace=trace,
            )
        assert len(calls["vertex"]) == 1 and not calls["developer"]
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "not_dispatched",
            "fallback_target": "vertex_priority",
            "fallback_detail": "budget_exhausted_daily",
        }


@pytest.mark.parametrize("fallback_enabled", [True, False])
async def test_stage2_priority_http_400_falls_back_to_developer_when_enabled(
    routes, message, store, fallback_enabled
):
    gate, calls, replies = routes
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=fallback_enabled)
    )
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(400, json={"error": {"code": 400, "message": "priority unsupported"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    trace = {}
    if fallback_enabled:
        out = await gate.generate(
            {"current_message": "Priority 400 回退测试"},
            "subject",
            message=message,
            generation_trace=trace,
        )
        assert json.loads(out)["bubbles"]
        assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 1
        assert gate.vertex_blocked_until == 0.0
        assert trace["provider"] == "gemini_paid"
        assert "vertex_service_tier" not in trace
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "succeeded",
            "fallback_target": "gemini_paid",
        }
        assert len(trace["provider_diagnostics"]) == 2
        std_diag, prio_diag = trace["provider_diagnostics"]
        assert (std_diag["service_tier"], std_diag["http_status"], std_diag["fallback"]) == (
            "standard",
            429,
            "failed",
        )
        assert (prio_diag["service_tier"], prio_diag["http_status"], prio_diag["fallback"]) == (
            "priority",
            400,
            "succeeded",
        )
        rows = await store.all(
            "SELECT p.provider,l.status,l.amount FROM ledger l "
            "JOIN ledger_usage u ON u.id=l.id JOIN ledger_provider p ON p.id=l.id "
            "WHERE u.model_lane='hydrogen' ORDER BY l.rowid"
        )
        assert [(r[0], r[1]) for r in rows] == [
            ("vertex_paid", "settled"),
            ("vertex_paid", "settled"),
            ("gemini_paid", "settled"),
        ]
        assert rows[0]["amount"] == 0 and rows[1]["amount"] == 0 and rows[2]["amount"] > 0
    else:
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gate.generate(
                {"current_message": "Priority 400 禁用回退"},
                "subject",
                message=message,
                generation_trace=trace,
            )
        assert len(calls["vertex"]) == 2 and not calls["developer"]
        assert gate.vertex_blocked_until == 0.0
        assert trace["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "429",
            "fallback": "disabled",
        }
        assert trace["provider_diagnostics"][-1]["fallback"] == "disabled"


@pytest.mark.parametrize("vertex_enabled", [False, True])
async def test_memory_archive_allow_flex_false_uses_standard_routing_and_extraction_contract(
    routes, store, vertex_enabled
):
    from hakimi.dual_memory import EXTRACTION_INSTRUCTION, EXTRACTION_RESPONSE_FORMAT

    gate, calls, replies = routes
    await gate.routing.set_config(ProviderRoutingConfig(vertex_enabled=vertex_enabled))
    archive_json = '{"profiles":[],"chronicles":[]}'

    def _archive_ok():
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = archive_json
        return httpx.Response(200, json=resp)

    # 1. Default allow_flex=True: always uses Developer API with service_tier="flex" & 0.5x rate.
    replies["developer"].append(_archive_ok())
    trace_flex = {}
    out_flex = await gate.generate(
        {"task": "dual_memory", "recent_messages": []},
        "mem-sub",
        scenario=Scenario.MEMORY_COMPRESSION,
        budget_lane="memory",
        audit_source="memory",
        memory_archive=True,
        generation_trace=trace_flex,
    )
    assert out_flex == archive_json
    assert len(calls["developer"]) == 1 and len(calls["vertex"]) == 0
    flex_body = calls["developer"][0]
    assert flex_body["service_tier"] == "flex"
    assert flex_body["store"] is False
    assert EXTRACTION_INSTRUCTION in flex_body["system_instruction"]
    assert flex_body["response_format"]["schema"] == EXTRACTION_RESPONSE_FORMAT
    assert trace_flex["model_lane"] == "memory_flex"
    assert trace_flex["provider"] == "gemini_paid"

    # 2. allow_flex=False: uses 1.0x selected_model, normal Vertex/Gemini routing, no flex tier.
    if vertex_enabled:
        replies["vertex"].append(_archive_ok())
    else:
        replies["developer"].append(_archive_ok())
    trace_std = {}
    out_std = await gate.generate(
        {"task": "dual_memory", "recent_messages": []},
        "mem-sub",
        scenario=Scenario.MEMORY_COMPRESSION,
        budget_lane="memory",
        audit_source="memory",
        memory_archive=True,
        allow_flex=False,
        generation_trace=trace_std,
    )
    assert out_std == archive_json
    if vertex_enabled:
        assert len(calls["vertex"]) == 1 and len(calls["developer"]) == 1
        std_body = calls["vertex"][0]
        assert trace_std["provider"] == "vertex_paid"
    else:
        assert len(calls["developer"]) == 2 and len(calls["vertex"]) == 0
        std_body = calls["developer"][1]
        assert trace_std["provider"] == "gemini_paid"
    assert "service_tier" not in std_body
    assert std_body["store"] is False
    assert std_body["model"] == "gemini-4-hydrogen"
    assert EXTRACTION_INSTRUCTION in std_body["system_instruction"]
    assert std_body["response_format"]["schema"] == EXTRACTION_RESPONSE_FORMAT
    assert trace_std["model_lane"] == "hydrogen"

    # Verify memory budget lane + 0.5x vs 1.0x billing:
    # 100*1.50 + 50*7.50 = 525 microdollars (1.0x); 0.5x = 262.5 -> ceil 263 microdollars.
    rows = await store.all(
        "SELECT l.lane,u.model_lane,l.amount,p.provider FROM ledger l "
        "JOIN ledger_usage u ON u.id=l.id JOIN ledger_provider p ON p.id=l.id "
        "WHERE u.model_lane IN ('memory_flex','hydrogen') ORDER BY l.rowid"
    )
    assert [tuple(r) for r in rows] == [
        ("memory", "memory_flex", 263, "gemini_paid"),
        ("memory", "hydrogen", 525, "vertex_paid" if vertex_enabled else "gemini_paid"),
    ]


async def test_vertex_breaker_exponential_backoff_half_open_probe_and_recovery(
    routes, message, settings, monkeypatch
):
    now = [1000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])
    gate, calls, replies = routes
    assert settings.vertex_circuit_base_cooldown_seconds == 120.0
    assert settings.vertex_circuit_max_cooldown_seconds == 300.0

    # Trip 1 (closed -> open): Standard 429 + Priority 429 -> 120s cooldown.
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "status": "RESOURCE_EXHAUSTED"}}),
            httpx.Response(429, json={"error": {"code": 429, "status": "TOO_MANY_REQUESTS"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    out1 = await gate.generate({"current_message": "trip1"}, "subject", message=message)
    assert json.loads(out1)["bubbles"]
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 1
    assert gate.vertex_blocked_until == pytest.approx(1120.0)

    # During cooldown: skips Vertex completely, records circuit_open diagnostic.
    now[0] = 1050.0
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    trace_skip = {}
    out_skip = await gate.generate(
        {"current_message": "skip"}, "subject", message=message, generation_trace=trace_skip
    )
    assert json.loads(out_skip)["bubbles"]
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 2
    assert trace_skip["provider"] == "gemini_paid"
    assert "vertex_service_tier" not in trace_skip
    assert "provider_incident" not in trace_skip
    [skip_diag] = trace_skip["provider_diagnostics"]
    assert skip_diag["stage"] == "generation"
    assert skip_diag["provider"] == "vertex_paid"
    assert skip_diag["model_lane"] == "hydrogen"
    assert skip_diag["cause"] == "circuit_open"
    assert skip_diag["cooldown_ms"] == 70_000
    assert skip_diag["elapsed_ms"] == 0
    assert skip_diag["fallback"] == "succeeded"

    # Trip 2 (half-open probe fails with 429): probes Standard ONLY (no Priority retry!),
    # falls back straight to Developer API, escalates cooldown to 240s.
    now[0] = 1120.0
    replies["vertex"].append(sse(stream_error("RESOURCE_EXHAUSTED")))
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    trace_p1 = {}
    out_p1 = await gate.generate(
        {"current_message": "probe1"}, "subject", message=message, generation_trace=trace_p1
    )
    assert json.loads(out_p1)["bubbles"]
    assert len(calls["vertex"]) == 3 and len(calls["developer"]) == 3
    assert calls["vertex_headers"][-1] == {"x-vertex-ai-llm-request-type": "shared"}
    assert gate.vertex_blocked_until == pytest.approx(1360.0)
    assert trace_p1["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "RESOURCE_EXHAUSTED",
        "fallback": "succeeded",
        "fallback_target": "gemini_paid",
    }

    # Trip 3 (half-open probe fails again): capped at max_cooldown = 300s (not 480s).
    now[0] = 1360.0
    replies["vertex"].append(
        httpx.Response(429, json={"error": {"code": 429, "status": "TOO_MANY_REQUESTS"}})
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    out_p2 = await gate.generate({"current_message": "probe2"}, "subject", message=message)
    assert json.loads(out_p2)["bubbles"]
    assert len(calls["vertex"]) == 4 and len(calls["developer"]) == 4
    assert gate.vertex_blocked_until == pytest.approx(1660.0)

    # Half-open probe SUCCEEDS -> closes breaker and resets backoff to 0!
    now[0] = 1660.0
    replies["vertex"].append(httpx.Response(200, json=interaction_response()))
    trace_rec = {}
    out_rec = await gate.generate(
        {"current_message": "recovered"}, "subject", message=message, generation_trace=trace_rec
    )
    assert json.loads(out_rec)["bubbles"]
    assert len(calls["vertex"]) == 5 and len(calls["developer"]) == 4
    assert trace_rec["provider"] == "vertex_paid"
    assert gate.vertex_blocked_until == 0.0

    # Verify backoff was reset: next 429 pair opens at base 120s (not 300s).
    now[0] = 1700.0
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    await gate.generate({"current_message": "fresh-trip"}, "subject", message=message)
    assert gate.vertex_blocked_until == pytest.approx(1820.0)


async def test_vertex_breaker_keeps_gemini_paid_chain_continuity_during_cooldown(
    routes, message, monkeypatch
):
    now = [2000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])
    gate, calls, replies = routes

    # Turn 1: Vertex Standard & Priority 429 -> falls back to gemini_paid, opens cooldown.
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    dev1 = interaction_response()
    dev1["id"] = "gem-chain-1"
    dev2 = interaction_response()
    dev2["id"] = "gem-chain-2"
    dev3 = interaction_response()
    dev3["id"] = "gem-chain-3"
    replies["developer"].extend(
        [
            httpx.Response(200, json=dev1),
            httpx.Response(200, json=dev2),
            httpx.Response(200, json=dev3),
        ]
    )

    history1 = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "旧消息一"}]
    trace1 = {}
    await gate.generate(
        {"current_message": "第一轮", "recent_messages": history1},
        "subject",
        message=message,
        generation_trace=trace1,
    )
    assert trace1["provider"] == "gemini_paid"
    chain1 = {**trace1["chain_commit"], "expires": time.time() + 600}
    assert chain1["interaction_id"] == "gem-chain-1"

    # Turn 2 during cooldown: skips Vertex and reuses gemini_paid's previous_interaction_id!
    now[0] = 2030.0
    history2 = [
        *history1,
        {"seq": 2, "role": "user", "user_id": message.user_id, "text": "新增第二句"},
    ]
    trace2 = {}
    await gate.generate(
        {
            "current_message": "第二轮",
            "recent_messages": history2,
            "interaction_chains": {"hydrogen": chain1},
        },
        "subject",
        message=message,
        generation_trace=trace2,
    )
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 2
    assert calls["developer"][1]["previous_interaction_id"] == "gem-chain-1"
    assert "旧消息一" not in wire_input_text(calls["developer"][1])
    assert "新增第二句" in wire_input_text(calls["developer"][1])
    assert trace2["provider_diagnostics"][0]["cause"] == "circuit_open"
    assert trace2["provider_diagnostics"][0]["provider"] == "vertex_paid"
    chain2 = {**trace2["chain_commit"], "expires": time.time() + 600}
    assert chain2["interaction_id"] == "gem-chain-2"
    assert chain2["system_digest"] == chain1["system_digest"]

    # Turn 3 still during cooldown: chains cleanly off gem-chain-2.
    now[0] = 2060.0
    trace3 = {}
    await gate.generate(
        {
            "current_message": "第三轮",
            "recent_messages": history2,
            "interaction_chains": {"hydrogen": chain2},
        },
        "subject",
        message=message,
        generation_trace=trace3,
    )
    assert len(calls["vertex"]) == 2 and len(calls["developer"]) == 3
    assert calls["developer"][2]["previous_interaction_id"] == "gem-chain-2"


async def test_vertex_half_open_single_probe_concurrency_and_lock_release_on_failure(
    settings, store, message, monkeypatch
):
    now = [3000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])
    probe_started = asyncio.Event()
    release_probe = asyncio.Event()
    vertex_calls = []
    developer_calls = []
    vertex_mode = ["429_burst"]

    async def vertex_handler(request):
        vertex_calls.append(json.loads(request.content))
        mode = vertex_mode[0]
        if mode == "429_burst":
            return httpx.Response(429, json={"error": {"code": 429, "message": "busy"}})
        if mode == "slow_ok":
            probe_started.set()
            await release_probe.wait()
            return httpx.Response(200, json=interaction_response())
        if mode == "slow_cancel":
            probe_started.set()
            await release_probe.wait()
            return httpx.Response(200, json=interaction_response())
        if mode == "503":
            return httpx.Response(503, json={"error": {"code": 503, "message": "err"}})
        return httpx.Response(200, json=interaction_response())

    def dev_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        developer_calls.append(json.loads(request.content))
        return httpx.Response(200, json=interaction_response())

    clients, transports = _client_pair(dev_handler)
    vertex, v_clients, v_transports = _client_pair(vertex_handler, vertex=True)
    gate = GeminiGateway(settings, store, *clients, vertex_chat_client=vertex)
    await gate.routing.set_config(ProviderRoutingConfig(vertex_enabled=True))
    try:
        # 1. Open the breaker (Standard 429 + Priority 429).
        await gate.generate({"current_message": "trip"}, "subject", message=message)
        assert len(vertex_calls) == 2 and len(developer_calls) == 1
        assert gate.vertex_blocked_until == pytest.approx(3120.0)

        # 2. Cooldown expires -> Task A acquires single half-open probe; concurrent Task B
        #    must NOT hit Vertex (zero thundering herd) and goes straight to gemini_paid.
        now[0] = 3120.0
        vertex_mode[0] = "slow_ok"
        trace_a, trace_b = {}, {}
        task_a = asyncio.create_task(
            gate.generate(
                {"current_message": "probe-a"}, "subject", message=message, generation_trace=trace_a
            )
        )
        await asyncio.wait_for(probe_started.wait(), timeout=2.0)
        out_b = await gate.generate(
            {"current_message": "peer-b"}, "subject", message=message, generation_trace=trace_b
        )
        assert json.loads(out_b)["bubbles"]
        assert len(vertex_calls) == 3  # Only Task A hit Vertex!
        assert len(developer_calls) == 2  # Task B skipped straight to Developer API!
        assert trace_b["provider_diagnostics"][0]["cause"] == "circuit_open"
        assert trace_b["provider_diagnostics"][0]["cooldown_ms"] == 0
        assert trace_b["provider_diagnostics"][0]["fallback"] == "succeeded"

        release_probe.set()
        out_a = await task_a
        assert json.loads(out_a)["bubbles"]
        assert trace_a["provider"] == "vertex_paid"
        assert gate.vertex_blocked_until == 0.0

        # 3. Re-trip breaker, then verify cancelled probe & 503 probe NEVER leave probing stuck!
        now[0] = 3200.0
        vertex_mode[0] = "429_burst"
        await gate.generate({"current_message": "trip2"}, "subject", message=message)
        assert gate.vertex_blocked_until == pytest.approx(3320.0)

        now[0] = 3320.0
        probe_started.clear()
        release_probe.clear()
        vertex_mode[0] = "slow_cancel"
        cancel_ev = asyncio.Event()
        cancel_task = asyncio.create_task(
            gate.generate(
                {"current_message": "cancel-probe"},
                "subject",
                message=message,
                cancel_event=cancel_ev,
            )
        )
        await asyncio.wait_for(probe_started.wait(), timeout=2.0)
        cancel_ev.set()
        with pytest.raises(asyncio.CancelledError):
            await cancel_task
        assert not gate._vertex_circuits["hydrogen"].probing
        assert gate._vertex_circuits["hydrogen"].consecutive_trips == 1

        # 503 during half-open probe falls back to Developer API and releases probing lock.
        vertex_mode[0] = "503"
        out_503 = await gate.generate({"current_message": "probe-503"}, "subject", message=message)
        assert json.loads(out_503)["bubbles"]
        assert not gate._vertex_circuits["hydrogen"].probing
        assert gate._vertex_circuits["hydrogen"].consecutive_trips == 1

        # Next call can immediately probe again and recover Vertex!
        vertex_mode[0] = "ok"
        trace_ok = {}
        await gate.generate(
            {"current_message": "probe-ok"}, "subject", message=message, generation_trace=trace_ok
        )
        assert trace_ok["provider"] == "vertex_paid"
        assert gate.vertex_blocked_until == 0.0
    finally:
        release_probe.set()
        await gate.close()
        await _close_clients([*clients, *v_clients], transports + v_transports)


async def test_vertex_breaker_no_block_without_fallback_and_isolates_embeddings(
    routes, message, settings, store, monkeypatch
):
    from hakimi.embeddings import EmbeddingClient

    now = [4000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])
    gate, calls, replies = routes

    # 1. With vertex_fallback_to_gemini=False, even after repeated 429s Vertex is never skipped.
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=False)
    )
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
            httpx.Response(200, json=interaction_response()),
        ]
    )
    with pytest.raises(Rejected, match="gemini_unavailable"):
        await gate.generate({"current_message": "no-fb-1"}, "subject", message=message)
    assert gate.vertex_blocked_until == 0.0
    out_sole = await gate.generate({"current_message": "no-fb-2"}, "subject", message=message)
    assert json.loads(out_sole)["bubbles"]
    assert len(calls["vertex"]) == 3 and len(calls["developer"]) == 0

    # 2. Re-enable fallback, trip hydrogen breaker, verify:
    #    (a) chat_client=None disables skipping even while cooldown timestamp is active
    #    (b) neon route is NOT blocked by hydrogen's breaker
    #    (c) EmbeddingClient on Vertex is completely unaffected!
    await gate.routing.set_config(
        ProviderRoutingConfig(vertex_enabled=True, vertex_fallback_to_gemini=True)
    )
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    await gate.generate({"current_message": "trip-h"}, "subject", message=message)
    assert gate.vertex_blocked_until == pytest.approx(4120.0)

    # (a) If fallback client disappears, Vertex is NOT blocked.
    orig_chat_client = gate.chat_client
    gate.chat_client = None
    replies["vertex"].append(httpx.Response(200, json=interaction_response()))
    try:
        out_no_client = await gate.generate(
            {"current_message": "sole-client"}, "subject", message=message
        )
        assert json.loads(out_no_client)["bubbles"]
        assert len(calls["vertex"]) == 6
    finally:
        gate.chat_client = orig_chat_client

    # Re-trip hydrogen for isolation checks.
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
        ]
    )
    replies["developer"].append(httpx.Response(200, json=interaction_response()))
    await gate.generate({"current_message": "trip-h2"}, "subject", message=message)
    assert gate.vertex_blocked_until == pytest.approx(4120.0)

    # (b) Neon generation route stays closed while hydrogen is open.
    settings.primary_model = "neon"
    replies["vertex"].append(httpx.Response(200, json=interaction_response()))
    trace_neon = {}
    await gate.generate(
        {"current_message": "neon-ok"}, "subject", message=message, generation_trace=trace_neon
    )
    assert trace_neon["provider"] == "vertex_paid"
    assert trace_neon["model_lane"] == "neon"
    settings.primary_model = "hydrogen"

    # (c) EmbeddingClient is completely unaffected by Vertex generation breaker.
    embed_calls = []

    async def fake_embed_content(**kwargs):
        embed_calls.append(kwargs)
        from types import SimpleNamespace

        return SimpleNamespace(
            embeddings=[SimpleNamespace(values=[0.5, 0.5, 0.5, 0.5])],
            usage_metadata=SimpleNamespace(prompt_token_count=4),
        )

    from types import SimpleNamespace

    fake_vertex_embed = SimpleNamespace(
        aio=SimpleNamespace(models=SimpleNamespace(embed_content=fake_embed_content))
    )
    embed_client = EmbeddingClient(
        settings, store, routing=gate.routing, vertex_client=fake_vertex_embed
    )
    try:
        vecs = await embed_client.embed_texts(["测试向量"])
        assert len(vecs) == 1 and vecs[0] is not None
        assert len(embed_calls) == 1
        assert embed_client._vertex_blocked_until == 0.0
    finally:
        await embed_client.close()


async def test_vertex_breaker_reload_resets_backoff_and_ignores_stale_epoch_429(
    routes, message, monkeypatch
):
    now = [5000.0]
    monkeypatch.setattr("hakimi.gemini.time.monotonic", lambda: now[0])
    gate, calls, replies = routes

    # Trip twice so consecutive_trips == 2 (240s backoff).
    replies["vertex"].extend(
        [
            httpx.Response(429, json={"error": {"code": 429, "message": "std 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "prio 429"}}),
            httpx.Response(429, json={"error": {"code": 429, "message": "probe 429"}}),
        ]
    )
    replies["developer"].extend(
        [
            httpx.Response(200, json=interaction_response()),
            httpx.Response(200, json=interaction_response()),
        ]
    )
    await gate.generate({"current_message": "t1"}, "subject", message=message)
    now[0] = 5120.0
    await gate.generate({"current_message": "t2"}, "subject", message=message)
    assert gate.vertex_blocked_until == pytest.approx(5360.0)
    stale_epoch = gate._vertex_circuit_epoch

    # reload_provider_clients resets cooldown, consecutive_trips, and increments epoch.
    gate.reload_provider_clients()
    assert gate.vertex_blocked_until == 0.0
    assert gate._vertex_circuits == {}
    assert gate._vertex_circuit_epoch > stale_epoch

    # A stale pre-reload 429 cannot trip the freshly reloaded breaker!
    assert gate._trip_vertex_circuit("hydrogen", expected_epoch=stale_epoch) == 0.0
    assert gate.vertex_blocked_until == 0.0

    # Immediate Vertex call succeeds right after reload.
    replies["vertex"].append(httpx.Response(200, json=interaction_response()))
    trace = {}
    await gate.generate(
        {"current_message": "after-reload"}, "subject", message=message, generation_trace=trace
    )
    assert trace["provider"] == "vertex_paid"
    assert len(calls["vertex"]) == 4


def test_vertex_circuit_settings_aliases_and_validation(tmp_path, monkeypatch):
    from hakimi.config import Settings

    monkeypatch.setenv("VERTEX_CIRCUIT_BASE_COOLDOWN_SECONDS", "90")
    monkeypatch.setenv("VERTEX_CIRCUIT_MAX_COOLDOWN_SECONDS", "240")
    s = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        gemini_api_key="paid-env-secret-abcdef",
        encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    )
    assert s.vertex_circuit_base_cooldown_seconds == 90.0
    assert s.vertex_circuit_max_cooldown_seconds == 240.0

    monkeypatch.setenv("VERTEX_CIRCUIT_BASE_COOLDOWN_SECONDS", "300")
    monkeypatch.setenv("VERTEX_CIRCUIT_MAX_COOLDOWN_SECONDS", "120")
    with pytest.raises(ValueError, match="vertex_circuit_max_cooldown_seconds"):
        Settings(
            _env_file=None,
            data_dir=tmp_path / "data",
            gemini_api_key="paid-env-secret-abcdef",
            encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        )


def test_admin_js_renders_vertex_circuit_open_diagnostic_row():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js_src = (root / "admin" / "src" / "js" / "03-routing-reply.js").read_text(encoding="utf-8")
    start = js_src.index("const rejectDetails=")
    end = js_src.index("async function decisionAudit()")
    snippet = js_src[start:end]
    skip_row = json.dumps(
        {
            "provider_diagnostics": [
                {
                    "stage": "generation",
                    "provider": "vertex_paid",
                    "model_lane": "hydrogen",
                    "cause": "circuit_open",
                    "cooldown_ms": 95200,
                    "timeout_ms": 45000,
                    "elapsed_ms": 0,
                    "fallback": "succeeded",
                }
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
const diag = diagnosticCell({skip_row}).toText();
const out = JSON.stringify({{ diag }});
if (typeof print === 'function') print(out); else console.log(out);
"""
    proc = run_js_harness(harness, cwd=root)
    assert proc.returncode == 0, proc.stderr
    rendered = json.loads(proc.stdout.strip())["diag"]
    assert "Vertex AI" in rendered
    assert "本地熔断冷却" in rendered
    assert "本次未发出请求" in rendered
    assert "95.2 秒" in rendered
    assert "回退成功" in rendered
