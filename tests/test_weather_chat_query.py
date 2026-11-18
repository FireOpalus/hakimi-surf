"""Drive the production Decisions → module query → chat path over fake HTTP."""

import json
import time

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.service import Service
from tests.fakes import FakeBili, FakeSender
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


@pytest.mark.parametrize("choice", ["weather.read", "none", "mc.mine"])
async def test_model_can_select_only_offered_read_only_query(
    settings, store, message, sdk_pair, choice
):
    settings.weather_enabled = True
    settings.weather_latitude, settings.weather_longitude = 31.23, 121.47
    decisions, generations, weather_requests = [], [], []

    def gemini(req):
        body = json.loads(req.content)
        if req.url.path.endswith("decisions"):
            result = decision_response()
            if "module_query" in body["questions"]:
                decisions.append(body["questions"]["module_query"])
                result["answers"]["module_query"] = {
                    "type": "choice",
                    "choice": choice,
                    "confidence": 0.99,
                    "probabilities": {choice: 0.99},
                }
                # When this query can answer, the separate paid web search is unnecessary.
                result["answers"]["need_external_lookup"]["noul"] = 0.95
            return httpx.Response(200, json=result)
        generations.append(body)
        return httpx.Response(200, json=interaction_response())

    def meteo(req):
        weather_requests.append(req)
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": int(time.time()),
                    "interval": 900,
                    "weather_code": 61,
                    "temperature_2m": 19.75,
                }
            },
        )

    ai = GeminiGateway(settings, store, *sdk_pair(gemini))
    sender = FakeSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    async with httpx.AsyncClient(transport=httpx.MockTransport(meteo)) as weather:
        svc.weather.http = weather
        try:
            await svc.world_state.initialize()
            await svc.life_engine.initialize()
            msg = message.model_copy(update={"text": "你那边现在要带伞吗？"})
            assert await svc.accept(msg) == "accepted"
            await svc.process(await store.claim())
            assert await svc.deliver_once() and len(sender.sent) == 1, svc.counters
        finally:
            await svc.stop()
    assert len(decisions) == 1 and set(decisions[0]["criteria"]) == {"none", "weather.read"}
    assert len(generations) == 1
    model_input = json.dumps(generations[0]["input"], ensure_ascii=False)
    assert "121.47" not in model_input
    assert "module_query_result" not in generations[0].get("system_instruction", "")
    if choice == "weather.read":
        assert len(weather_requests) == 1 and "module_query_result" in model_input
        assert (
            "19.75" in model_input
            and "小雨" in model_input
            and "已设地点" in model_input
            and "web_search_status" not in model_input
        )
    else:
        assert not weather_requests and "module_query_result" not in model_input
