import hashlib
import hmac
import time

import httpx
import pytest

from hakimi.adapters import Sender, onebot_message
from hakimi.bilibili import Bilibili, bounded_json, extract_bvid
from hakimi.security import (
    Rejected,
    authorize,
    qq_signing_key,
    safe_output,
    verify_onebot,
    verify_qq,
)

BV = "BV1xx411c7mD"


def test_onebot_raw_body_signature():
    body, secret = b'{"text":"hello"}', "a-secret"
    signature = "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()
    verify_onebot(body, signature, secret)
    with pytest.raises(Rejected):
        verify_onebot(body + b" ", signature, secret)
    with pytest.raises(Rejected):
        verify_onebot(body, "", secret)


def test_qq_signing_matches_official_document_vector():
    key = qq_signing_key("DG5g3B4j9X2KOErG")
    result = key.sign(b"1725442341Arq0D5A61EgUu4OxUvOp").hex()
    assert result == (
        "87befc99c42c651b3aac0278e71ada338433ae26fcb24307bdc5ad38c1adc2d01bcf"
        "cadc0842edac85e85205028a1132afe09280305f13aa6909ffc2d652c706"
    )


def test_qq_rejects_tampering_and_stale_signature():
    secret, body = "test-qq-secret", b'{"op":0}'
    key, timestamp = qq_signing_key(secret), str(int(time.time()))
    sig = key.sign(timestamp.encode() + body).hex()
    verify_qq(body, timestamp, sig, secret, 300)
    with pytest.raises(Rejected):
        verify_qq(body + b" ", timestamp, sig, secret, 300)
    old = str(int(time.time()) - 301)
    with pytest.raises(Rejected):
        verify_qq(body, old, key.sign(old.encode() + body).hex(), secret, 300)


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/",
        "http://169.254.169.254/latest/meta-data/",
        f"https://www.bilibili.com.evil.test/video/{BV}",
        f"https://www.bilibili.com@evil.test/video/{BV}",
        f"https://evil.test/?id={BV}",
        f"https://www.bilibili.com:8443/video/{BV}",
        f"http://www.bilibili.com/video/{BV}",
        "https://b23.tv/test",
        "file:///etc/passwd",
        f"https://www.bilibili.com/video/{BV}/../../admin",
    ],
)
def test_no_untrusted_url_is_fetchable(url):
    assert extract_bvid(url) is None


def test_video_reference_canonicalization():
    assert extract_bvid(BV) == BV
    assert extract_bvid(f"帮我看看 https://www.bilibili.com/video/{BV}?share=123") == BV
    assert extract_bvid(BV + " BV1Q541167Qg") == BV
    assert (
        extract_bvid(
            f"https://www.bilibili.com/video/{BV} https://www.bilibili.com/video/BV1Q541167Qg"
        )
        == BV
    )


def test_owner_and_group_policy_do_not_depend_on_display_names(settings, message):
    assert authorize(message, settings)
    assert not authorize(message.model_copy(update={"user_id": "999"}), settings)
    assert not authorize(message.model_copy(update={"bot_id": "other"}), settings)
    group = message.model_copy(update={"kind": "group", "conversation_id": "40000"})
    assert authorize(group, settings)
    assert not authorize(group.model_copy(update={"addressed": False}), settings)
    assert not authorize(group.model_copy(update={"conversation_id": "unapproved"}), settings)
    assert not authorize(group.model_copy(update={"user_id": "unknown"}), settings)


def test_onebot_literal_cq_tag_cannot_forge_mention(settings):
    base = {
        "post_type": "message",
        "self_id": 10000,
        "user_id": 20000,
        "group_id": 40000,
        "message_type": "group",
        "message_id": -12,
        "time": time.time(),
        "message": "&#91;CQ:at,qq=10000&#93; hi",
    }
    parsed = onebot_message(base, settings)
    assert not parsed.addressed
    base["message"] = "[CQ:at,qq=10000] hi"
    assert onebot_message(base, settings).addressed
    base["message"] = [{"type": "text", "data": {"text": "[CQ:at,qq=10000] hi"}}]
    assert not onebot_message(base, settings).addressed


async def test_http_reader_refuses_redirects_and_oversized_json():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(302, headers={"Location": "http://169.254.169.254"})
        ),
        follow_redirects=False,
    ) as client:
        with pytest.raises((Rejected, httpx.HTTPStatusError)):
            await bounded_json(client, "GET", "https://api.bilibili.com/test")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"text": "x" * 1000}))
    ) as client:
        with pytest.raises(Rejected, match="upstream_body_too_large"):
            await bounded_json(client, "GET", "https://api.bilibili.com/test", max_bytes=50)


async def test_bilibili_risk_control_opens_circuit_without_retry(settings):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"code": -352, "message": "risk control"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bili = Bilibili(settings, client)
        with pytest.raises(Rejected, match="bilibili_unavailable"):
            await bili.video(BV)
        with pytest.raises(Rejected, match="bilibili_circuit_open"):
            await bili.video(BV)
    assert len(calls) == 1
    assert calls[0].url.host == "api.bilibili.com"
    assert calls[0].method == "GET"
    assert "cookie" not in calls[0].headers


def test_output_control_codes_and_secret_detection():
    assert "[CQ:" not in safe_output("[CQ:at,qq=all] <@all>", [])
    with pytest.raises(Rejected):
        safe_output("secret-long-token", ["secret-long-token"])


async def test_onebot_delivery_uses_text_segments(settings, message):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "ok", "retcode": 0, "data": {"message_id": 42}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = Sender(settings, client)
        await sender.send(message, "[CQ:at,qq=all]")
    import json

    body = json.loads(requests[0].content)
    assert body["message"] == [{"type": "text", "data": {"text": "[CQ:at,qq=all]"}}]
    assert body["auto_escape"] is True
    assert body["user_id"] == 20000


async def test_official_qq_token_and_delivery_fixed_origin_and_reply_id(settings, message):
    import json

    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(
                200, json={"access_token": "local-fake-token", "expires_in": 7200}
            )
        return httpx.Response(200, json={"id": "sent-id"})

    target = message.model_copy(
        update={
            "platform": "qq",
            "bot_id": settings.qq_app_id,
            "user_id": "user-openid",
            "conversation_id": "user-openid",
        }
    )
    # An official QQ OpenID is an ordinary friend, not proof of OWNER_QQ_ID.
    settings.qq_allowed_users.add("user-openid")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = Sender(settings, client)
        await sender.send(target, "hello")
        await sender.send(target.model_copy(update={"event_id": "another-event"}), "hi")
        with pytest.raises(Rejected, match="proactive_not_authorized"):
            await sender.send(target, "patrol", proactive=True)
    assert len(requests) == 3
    assert all(r.url.host == "api.bot.qq.com" for r in requests)
    body = json.loads(requests[1].content)
    assert body["msg_seq"] == 1 and body["msg_id"] == target.event_id
    assert body["msg_type"] == 0
