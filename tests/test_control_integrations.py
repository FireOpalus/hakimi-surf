import json

import pytest

from hakimi.longnick import LongnickConfig
from hakimi.onebot_transport import response_data, validate_action
from hakimi.security import Rejected
from tests.test_proactive_dm import dm as dm


@pytest.mark.parametrize("disable_during_generation", [False, True])
async def test_signature_reuses_single_digest_call_without_blocking_dm(
    dm, disable_during_generation
):
    await dm.service.longnick.initialize()
    await dm.service.longnick.set_config(LongnickConfig(enabled=True))
    original = dm.service.ai.generate

    async def generate(*args, **kwargs):
        await original(*args, **kwargs)
        assert kwargs["with_longnick"] is True
        if disable_during_generation:
            await dm.service.longnick.set_config(LongnickConfig(enabled=False))
        return json.dumps(
            {"digest": "今天学到一点公开科学知识喵。", "daily_longnick": "星光与猫猫"},
            ensure_ascii=False,
        )

    dm.service.ai.generate = generate
    original_call = dm.service.sender.onebot.call

    async def call(action, params):
        result = await original_call(action, params)
        return {"result": 1, "errMsg": "not supported"} if action == "set_self_longnick" else result

    dm.service.sender.onebot.call = call
    result = await dm.manual_digest()
    assert result["delivery"] == "sent"
    calls = dm.service.ai.calls
    assert [x[0] for x in calls] == ["screen", "generate"]
    sent = [
        params for action, params in dm.service.sender.onebot.calls if action == "send_private_msg"
    ]
    assert len(sent) == 1
    assert "daily_longnick" not in sent[0]["message"][0]["data"]["text"]
    assert (await dm.service.longnick.status())["status"] == (
        "never" if disable_during_generation else "failed"
    )


def test_transport_side_effects_are_narrow_and_protected(settings):
    assert response_data({"status": "ok", "retcode": 0, "data": None}, "set_self_longnick") == {}
    assert response_data({"status": "ok", "retcode": 0, "data": []}, "get_friend_list") == []
    with pytest.raises(Rejected):
        response_data({"status": "ok", "retcode": 0, "data": []}, "send_private_msg")
    for action in ["set_qq_avatar", "set_qq_profile", "set_friend_add_request", "publish_qzone"]:
        with pytest.raises(Rejected):
            validate_action(settings, action, {})
    for uid in ["20000", "10000", True, "-1", "20000.0"]:
        with pytest.raises(Rejected):
            validate_action(settings, "delete_friend", {"user_id": uid})
