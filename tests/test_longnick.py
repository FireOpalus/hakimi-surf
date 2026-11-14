import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from hakimi.longnick import Longnick, LongnickConfig
from hakimi.security import Rejected


@pytest.fixture
async def longnick(settings, store):
    settings.owner_qq_id = "20000"
    wire = SimpleNamespace(call=AsyncMock(return_value={}), ready=asyncio.Event())
    wire.ready.set()
    svc = SimpleNamespace(s=settings, store=store, sender=SimpleNamespace(onebot=wire))
    module = Longnick(svc, clock=lambda: 1_000_000)
    await module.initialize()
    return module


async def test_daily_longnick_disabled_by_default_no_rpc(longnick):
    assert (await longnick.apply_digest("你好"))["status"] == "disabled_or_absent"
    assert not longnick.service.sender.onebot.call.called


async def test_owner_manual_signature_sanitized_and_zero_model_calls(longnick, message):
    result = await longnick.command(
        message.model_copy(update={"text": "/set-sign **今天**\n看看星星 " + ("喵" * 60)})
    )
    assert "success" in result
    action, params = longnick.service.sender.onebot.call.call_args.args
    assert action == "set_self_longnick" and set(params) == {"longNick"}
    assert "\n" not in params["longNick"] and "*" not in params["longNick"]
    assert len(params["longNick"]) <= 50
    assert not await longnick.store.all("SELECT * FROM ledger")


@pytest.mark.parametrize(
    "changes",
    [
        {"user_id": "30000", "conversation_id": "30000"},
        {"kind": "group", "conversation_id": "40000"},
        {
            "platform": "qq",
            "bot_id": "99999",
            "user_id": "user-openid",
            "conversation_id": "user-openid",
        },
    ],
)
async def test_signature_cannot_be_changed_by_member_or_group(longnick, message, changes):
    with pytest.raises(Rejected, match="owner_private_only"):
        await longnick.command(message.model_copy(update=changes | {"text": "/set-sign 不能改"}))
    assert not longnick.service.sender.onebot.call.called


async def test_quoted_and_credential_signature_rejected(longnick, message):
    assert (
        await longnick.command(
            message.model_copy(update={"text": "/set-sign test", "command_text": ""})
        )
        is None
    )
    with pytest.raises(Rejected, match="credential_in_longnick"):
        await longnick.update("x" * 100 + longnick.s.gemini_api_key.get_secret_value())
    assert not longnick.service.sender.onebot.call.called


async def test_daily_cooldown_persistent_and_manual_explicit_override(longnick):
    clock = [1_000_000]
    longnick.clock = lambda: clock[0]
    await longnick.set_config(LongnickConfig(enabled=True, cooldown_hours=20))
    assert (await longnick.apply_digest("今天的签名"))["status"] == "success"
    assert (await longnick.apply_digest("重复自动签名"))["status"] == "cooldown"
    restored = Longnick(longnick.service, clock=longnick.clock)
    await restored.initialize()
    assert restored.config.enabled
    assert (await restored.apply_digest("重启以后"))["status"] == "cooldown"
    clock[0] += 61
    assert (await restored.update("Owner 明确修改"))["status"] == "success"
    clock[0] += 20 * 3600
    assert (await restored.apply_digest("第二天"))["status"] == "success"


@pytest.mark.parametrize(
    "response", [{"result": 1, "errMsg": "risk control secret"}, {"errCode": 403}, False]
)
async def test_napcat_business_failure_does_not_raise_or_claim_success(longnick, response):
    longnick.service.sender.onebot.call.return_value = response
    result = await longnick.update("不能成功")
    assert result["status"] in {"failed", "uncertain"}
    state = await longnick.status()
    assert state["last_success"] == 0 and "secret" not in str(state)


async def test_interrupted_signature_not_automatically_retried(longnick):
    await longnick.set_config(LongnickConfig(enabled=True))
    longnick.service.sender.onebot.call.side_effect = TimeoutError("secret")
    assert (await longnick.apply_digest("同一句签名"))["status"] == "uncertain"
    restored = Longnick(longnick.service, clock=lambda: 1_000_000 + 25 * 3600)
    await restored.initialize()
    assert (await restored.apply_digest("同一句签名"))["status"] == "uncertain"
    assert longnick.service.sender.onebot.call.await_count == 1


async def test_invalid_daily_payload_safe_and_settings_reset(longnick):
    await longnick.set_config(LongnickConfig(enabled=True))
    assert (await longnick.apply_digest({"not": "text"}))["status"] == "failed"
    assert not longnick.service.sender.onebot.call.called
    await longnick.set_config(None)
    assert not longnick.config.enabled and not longnick.overridden
    with pytest.raises(ValidationError):
        LongnickConfig(enabled=True, unexpected="override")
