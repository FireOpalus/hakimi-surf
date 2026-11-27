import asyncio
import json

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.routing import RouteDecision
from hakimi.security import Rejected
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


def line(data):
    return ("data: " + json.dumps(data) + "\n\n").encode()


async def test_real_sdk_stream_cancel_closes_http_and_keeps_full_reserve(settings, store, sdk_pair):
    arrived, closed = asyncio.Event(), asyncio.Event()
    cancel = asyncio.Event()

    class Partial(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield line({"event_type": "step.start", "index": 0, "step": {"type": "model_output"}})
            yield line(
                {
                    "event_type": "step.delta",
                    "index": 0,
                    "delta": {"type": "text", "text": '{"abstain":'},
                }
            )
            arrived.set()
            await asyncio.Event().wait()

        async def aclose(self):
            closed.set()

    def handler(request):
        assert json.loads(request.content)["stream"] is True
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Partial())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    task = asyncio.create_task(
        gateway.generate({"current_message": "分析"}, "s", cancel_event=cancel)
    )
    await asyncio.wait_for(arrived.wait(), 3)
    cancel.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    row = await store.one("SELECT reserved,amount,status FROM ledger")
    assert 0 < row[1] < row[0] and row[2] == "unknown"
    assert len(await store.all("SELECT * FROM ledger")) == 1


async def test_real_sdk_timeout_downgrades_once_new_reservation_and_fixed_schema(
    settings, store, sdk_pair
):
    requests = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            raise httpx.ReadTimeout("mock-only", request=request)
        return httpx.Response(200, json=interaction_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    value = await gateway.generate(
        {"current_message": "分析代码"}, "s", route=RouteDecision(tier=3)
    )
    assert json.loads(value)["bubbles"] == ["你好喵。"]
    assert [r["generation_config"]["thinking_level"] for r in requests] == ["high", "medium"]
    assert requests[0]["response_format"] == requests[1]["response_format"]
    assert requests[0]["system_instruction"] == requests[1]["system_instruction"]
    assert all(r["stream"] and not r["store"] for r in requests)
    rows = await store.all(
        "SELECT amount,reserved,status FROM ledger WHERE lane='chat' ORDER BY rowid"
    )
    assert rows[0]["status"] == "unknown" and 0 < rows[0]["amount"] < rows[0]["reserved"]
    assert rows[1]["status"] == "settled"


async def test_sdk_arbitrary_error_is_never_retried(settings, store, sdk_pair):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503, json={"error": {"code": 503, "message": "mock"}})

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    with pytest.raises(Rejected):
        await gateway.generate({"current_message": "你好"}, "s")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "status,refunded", [(400, True), (429, True), (500, False), (502, False), (503, False)]
)
async def test_http_error_billing_policy_distinguishes_unbilled_vs_ambiguous(
    settings, store, sdk_pair, status, refunded
):
    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(status, json={"error": {"code": status, "message": "mock"}})

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.generate({"current_message": "测试长一点的中文提示词用于对比预算"}, "s")
        row = await store.one(
            "SELECT l.status,l.amount,l.reserved,u.estimated_amount "
            "FROM ledger l JOIN ledger_usage u ON u.id=l.id WHERE u.model_lane='hydrogen'"
        )
        if refunded:
            assert row["status"] == "settled"
            assert row["amount"] == 0 and row["estimated_amount"] == 0
        else:
            assert row["status"] == "unknown"
            assert 0 < row["amount"] < row["reserved"]
            assert row["estimated_amount"] is None
    finally:
        await gateway.close()


async def test_pre_dispatch_cancel_refunds_zero_vs_mid_stream_fallback(settings, store, sdk_pair):
    cancel = asyncio.Event()
    cancel.set()
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(lambda r: httpx.Response(200, json=decision_response())),
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await gateway.generate(
                {"current_message": "不会被发出去的一段中文消息"},
                "s",
                cancel_event=cancel,
            )
        row = await store.one("SELECT reserved,amount,status FROM ledger")
        assert row["status"] == "settled" and row["amount"] == 0 and row["reserved"] > 0
    finally:
        await gateway.close()


@pytest.mark.parametrize(
    "code,emit_partial,expect_refund",
    [
        ("429", False, True),
        ("rate_limit_exceeded", False, True),
        ("RESOURCE_EXHAUSTED", False, True),
        ("400", False, True),
        ("401", False, True),
        ("403", False, True),
        ("404", False, True),
        ("409", False, True),
        ("413", False, True),
        ("422", False, True),
        ("INVALID_ARGUMENT", False, True),
        ("PERMISSION_DENIED", False, True),
        ("429", True, False),
        ("400", True, False),
        ("500", False, False),
        ("502", False, False),
        ("503", False, False),
        ("api_error", False, False),
        ("unknown", False, False),
    ],
)
async def test_sse_stream_provider_error_billing_zero_output_vs_partial_and_5xx(
    settings, store, sdk_pair, code, emit_partial, expect_refund
):
    events = [
        {"event_type": "step.start", "index": 0, "step": {"type": "thought"}},
        {"event_type": "step.stop", "index": 0},
        {"event_type": "step.start", "index": 1, "step": {"type": "model_output"}},
    ]
    if emit_partial:
        events.append(
            {"event_type": "step.delta", "index": 1, "delta": {"type": "text", "text": "{"}}
        )
    else:
        events.append(
            {"event_type": "step.delta", "index": 1, "delta": {"type": "text", "text": ""}}
        )
    events.append({"event_type": "error", "error": {"code": code, "message": "provider detail"}})
    payload = b"".join(line(ev) for ev in events)

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=payload)

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        with pytest.raises(Rejected, match="stream_provider_error"):
            await gateway.generate({"current_message": "测试流式计费边界的中文提示词"}, "s")
        row = await store.one(
            "SELECT l.status,l.amount,l.reserved,u.estimated_amount "
            "FROM ledger l JOIN ledger_usage u ON u.id=l.id WHERE u.model_lane='hydrogen'"
        )
        if expect_refund:
            assert row["status"] == "settled"
            assert row["amount"] == 0 and row["estimated_amount"] == 0
        else:
            assert row["status"] == "unknown"
            assert 0 < row["amount"] < row["reserved"]
            assert row["estimated_amount"] is None
    finally:
        await gateway.close()


async def test_sse_stream_timeout_error_keeps_conservative_fallback_billing(
    settings, store, sdk_pair
):
    payload = b"".join(
        [
            line({"event_type": "step.start", "index": 0, "step": {"type": "thought"}}),
            line({"event_type": "error", "error": {"code": "deadline_exceeded", "message": "t"}}),
        ]
    )

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=payload)

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    try:
        # Medium thinking downgrades once to low on GenerationTimeout;
        # both attempts hit SSE deadline_exceeded.
        with pytest.raises(Rejected, match="generation_incomplete"):
            await gateway.generate({"current_message": "测试流式超时保守计费"}, "s")
        rows = await store.all(
            "SELECT l.status,l.amount,l.reserved,u.estimated_amount "
            "FROM ledger l JOIN ledger_usage u ON u.id=l.id WHERE u.model_lane='hydrogen'"
        )
        assert len(rows) == 2
        for row in rows:
            assert row["status"] == "unknown"
            assert 0 < row["amount"] < row["reserved"]
            assert row["estimated_amount"] is None
    finally:
        await gateway.close()
