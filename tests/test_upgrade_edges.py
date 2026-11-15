import asyncio
import json

import httpx
import pytest

from hakimi.adapters import Sender
from hakimi.bilibili import Bilibili
from hakimi.config import Settings
from hakimi.rich_message_parser import parse_message
from hakimi.security import Rejected, safe_output
from hakimi.store import Store


def test_ws_configuration_rejects_example_placeholder_token(settings):
    from pydantic import ValidationError

    values = settings.model_dump()
    values.update(onebot_mode="ws_reverse", onebot_ws_token="GENERATE_WITH_INIT_ENV")
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    "overrides",
    [
        {"summary_model": "unapproved-model"},
        {"owner_thinking_level": "minimal"},
        {"summary_thinking_level": "automatic"},
        {"diary_thinking_level": "ultra"},
    ],
)
def test_model_policy_rejects_unsupported_models_and_levels(settings, overrides):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None, **(settings.model_dump() | overrides))


@pytest.mark.parametrize(
    "target",
    [
        "http://169.254.169.254/latest/meta-data/",
        "https://www.bilibili.com.evil.test/video/BV1xx411c7mD",
        "https://127.0.0.1/video/BV1xx411c7mD",
    ],
)
async def test_b23_never_follows_redirect_or_fetches_forbidden_target(settings, target):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": target})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(Rejected):
            await Bilibili(settings, client).resolve_short("https://b23.tv/abc123")
    assert len(requests) == 1 and requests[0].url.host == "b23.tv"


async def test_b23_only_extracts_canonical_bvid(settings):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                302, headers={"Location": "https://www.bilibili.com/video/BV1xx411c7mD?share=123"}
            )
        )
    ) as client:
        assert (
            await Bilibili(settings, client).resolve_short("https://b23.tv/abc123")
            == "BV1xx411c7mD"
        )


def test_foreign_card_url_is_not_misclassified_as_bilibili():
    rich = parse_message(
        [{"type": "json", "data": {"data": {"jumpUrl": "https://evil.test/video/BV1xx411c7mD"}}}],
        "10000",
    )
    assert rich.video_ids == []


async def test_outgoing_random_delay_is_serial_and_truncation_is_visible(
    settings, message, monkeypatch
):
    events = []

    async def sleep(delay):
        assert 0.8 <= delay <= 2.0
        events.append("delay")

    monkeypatch.setattr("hakimi.adapters.asyncio.sleep", sleep)
    settings = settings.model_copy(update={"send_delay_min": 0.8, "send_delay_max": 2})

    def handler(request):
        events.append("send")
        text = json.loads(request.content)["message"][0]["data"]["text"]
        assert len(text) <= 1800 and text.endswith("…")
        return httpx.Response(200, json={"status": "ok", "retcode": 0, "data": {"message_id": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = Sender(settings, client)
        await asyncio.gather(
            *(sender.send(message, safe_output("长" * 4000, [])) for _ in range(2))
        )
    assert events == ["delay", "send", "delay", "send"]


async def test_group_policy_survives_restart_without_env_override(settings):
    configured = settings.model_copy(update={"group_policies": {"onebot:40000": "smart_reply"}})
    first = Store(configured)
    await first.open()
    await first.close()
    recovered = Store(settings)
    await recovered.open()
    try:
        assert recovered.s.group_policies["onebot:40000"] == "smart_reply"
    finally:
        await recovered.close()
