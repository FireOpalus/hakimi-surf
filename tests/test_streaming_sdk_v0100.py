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
    assert row[0] == row[1] > 0 and row[2] == "unknown"
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
    assert rows[0]["status"] == "unknown" and rows[0]["amount"] == rows[0]["reserved"]
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
