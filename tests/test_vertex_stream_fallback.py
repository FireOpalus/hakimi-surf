import asyncio
import json
import time
from decimal import Decimal

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.provider_routing import ProviderRoutingConfig
from hakimi.security import Rejected
from hakimi.service import Service
from tests.fakes import FakeBili, FakeSender
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


@pytest.fixture
async def routes(settings, store):
    requests = {"vertex": [], "developer": []}
    responses = {"vertex": [], "developer": []}

    def handler(provider):
        def respond(request):
            if request.url.path.endswith("decisions"):
                return httpx.Response(200, json=decision_response())
            requests[provider].append(json.loads(request.content))
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


async def test_thought_only_429_delivers_once_and_retains_audit(settings, store, message, routes):
    gateway, calls, replies = routes
    replies["vertex"].append(
        sse(
            {"event_type": "step.start", "index": 0, "step": {"type": "thought"}},
            {"event_type": "step.stop", "index": 0},
            {"event_type": "step.start", "index": 1, "step": {"type": "model_output"}},
            {"event_type": "step.delta", "index": 1, "delta": {"type": "text", "text": ""}},
            stream_error("rate_limit_exceeded"),
        )
    )
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
        assert len(calls["vertex"]) == len(calls["developer"]) == 1
        assert (await store.one("SELECT COUNT(*) FROM outbox"))[0] == 1
        [audit] = await gateway.filters.recent_audit()
        assert audit["execution_status"] == "sent"
        assert audit["provider_incident"] == {
            "provider": "vertex_paid",
            "code": "rate_limit_exceeded",
            "http_status": 429,
            "fallback": "succeeded",
        }
        [failure] = audit["provider_diagnostics"]
        assert (failure["stage"], failure["cause"], failure["fallback"]) == (
            "generation",
            "stream_error",
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
            ("gemini_paid", "settled"),
        ]
    finally:
        await svc.stop()


@pytest.mark.parametrize(
    "stage,code,enabled,reason",
    [
        ("start", "429", True, "partial_output"),
        ("delta", "RESOURCE_EXHAUSTED", True, "partial_output"),
        ("empty", "quota_exceeded", False, "disabled"),
        ("empty", "api_error", True, "ineligible"),
        ("empty", "bad code with private text", True, "ineligible"),
    ],
)
async def test_stream_fallback_boundaries(routes, message, stage, code, enabled, reason):
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
    replies["vertex"].append(sse(*events, stream_error(code)))
    trace = {}
    with pytest.raises(Rejected, match="stream_provider_error"):
        await gate.generate(
            {"current_message": "hi"}, "subject", message=message, generation_trace=trace
        )
    assert len(calls["vertex"]) == 1 and not calls["developer"]
    assert trace["provider_diagnostics"][0]["fallback"] == reason
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "unknown" if " " in code else code,
        "fallback": reason,
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
    replies["vertex"].append(sse(stream_error("RESOURCE_EXHAUSTED")))
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
    assert len(calls["vertex"]) == len(calls["developer"]) == 1
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "RESOURCE_EXHAUSTED",
        "fallback": "failed",
        "fallback_detail": detail,
        **({"fallback_code": code} if code else {}),
    }


async def test_fallback_rebuilds_local_history_without_vertex_chain(routes, message):
    gate, calls, replies = routes
    replies["vertex"].extend(
        [httpx.Response(200, json=interaction_response()), sse(stream_error("quota_exceeded"))]
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
    assert calls["vertex"][-1]["previous_interaction_id"] == chain["interaction_id"]
    [backup] = calls["developer"]
    assert "previous_interaction_id" not in backup
    assert "一起去看星星" in wire_input_text(backup)
    assert backup["generation_config"] == calls["vertex"][-1]["generation_config"]


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
    replies["vertex"].append(
        sse(
            {"candidates": [{"content": {"role": "model", "parts": [part]}}]},
            {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "private text"}},
        )
    )
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
    assert len(calls["vertex"]) == 1
    assert len(calls["developer"]) == int(thought_only)
    assert trace["provider_incident"] == {
        "provider": "vertex_paid",
        "code": "429",
        "fallback": "succeeded" if thought_only else "partial_output",
    }
