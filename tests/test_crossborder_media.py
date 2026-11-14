"""No Tencent, NapCat, filesystem image or paid-model traffic in these tests."""

import asyncio
import base64
import io
import logging
import ssl

import pytest
from PIL import Image

from hakimi.media import MAX_IMAGE_BYTES, ImageLoader, validate_image_url
from hakimi.message_context import MessageContextResolver
from hakimi.models import ImageRef
from hakimi.rich_message_parser import parse_forward, parse_message
from hakimi.security import Rejected
from tests.test_media import Client, Response


def png():
    f = io.BytesIO()
    Image.new("RGB", (900, 450), "purple").save(f, "PNG")
    return f.getvalue()


class Wire:
    def __init__(self, values):
        self.values, self.calls = values, []
        self.active = self.maximum = 0

    async def call(self, action, params):
        self.calls.append((action, params))
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            await asyncio.sleep(0)
            value = self.values[action]
            if isinstance(value, Exception):
                raise value
            return value
        finally:
            self.active -= 1


def test_file_size_and_file_only_image_are_preserved():
    rich = parse_message(
        [
            {"type": "image", "data": {"file": "abc.image", "file_size": "3200000"}},
            {"type": "image", "data": {"file": "def.image", "file_size": 1200}},
        ],
        "10000",
    )
    assert [ref.file for ref in rich.images] == ["abc.image", "def.image"]
    assert rich.images[0].file_size == 3_200_000
    assert rich.images[0].identity != rich.images[1].identity


@pytest.mark.parametrize(
    "file", ["file:///etc/passwd", "/root/secret", "../token", "base64://aA==", "https://evil/a"]
)
def test_image_file_cannot_be_a_path_or_url(file):
    rich = parse_message([{"type": "image", "data": {"file": file}}], "10000")
    assert not rich.images


async def test_napcat_image_base64_avoids_http_and_normalizes(settings, monkeypatch):
    wire = Wire({"get_image": {"base64": base64.b64encode(png()).decode()}})
    loader = ImageLoader(settings, wire)
    client = Client([])
    monkeypatch.setattr(loader, "_client", lambda: client)
    results = await loader.load_results([ImageRef(file="native-image-id")])
    assert [action for action, _ in wire.calls] == ["get_image"]
    assert not client.requests
    with Image.open(io.BytesIO(base64.b64decode(results[0].data))) as im:
        assert im.format == "JPEG" and im.size == (768, 384)


async def test_get_file_fallback_uses_same_file_identifier(settings, monkeypatch):
    wire = Wire(
        {
            "get_image": {"file": "/app/qq/cache/file"},
            "get_file": {"base64": base64.b64encode(png()).decode()},
        }
    )
    loader = ImageLoader(settings, wire)
    monkeypatch.setattr(loader, "_client", lambda: Client([]))
    assert (await loader.load_results([ImageRef(file="abc")]))[0] is not None
    assert wire.calls == [("get_image", {"file": "abc"}), ("get_file", {"file": "abc"})]


async def test_ws_image_transfers_are_serial_across_conversations(settings, monkeypatch):
    wire = Wire({"get_image": {"base64": base64.b64encode(png()).decode()}})
    loader = ImageLoader(settings, wire)
    monkeypatch.setattr(loader, "_client", lambda: Client([]))
    await asyncio.gather(*(loader.load_results([ImageRef(file=f"id{i}")]) for i in range(4)))
    assert len(wire.calls) == 4 and wire.maximum == 1


async def test_known_large_image_skips_before_ws_or_http(settings, monkeypatch):
    wire = Wire({})
    loader = ImageLoader(settings, wire)
    client = Client([])
    monkeypatch.setattr(loader, "_client", lambda: client)
    ref = ImageRef(file="big", url="https://gchat.qpic.cn/big", file_size=MAX_IMAGE_BYTES + 1)
    assert await loader.load_results([ref]) == [None]
    assert loader.failure_placeholder(ref) == "[大图略过]"
    assert not wire.calls and not client.requests


@pytest.mark.parametrize(
    "value",
    [{"base64": "!!!"}, {"base64": "YQ==", "file_size": "99999999"}, {"base64": "A" * (5_000_000)}],
)
async def test_invalid_or_oversized_rpc_payload_never_loads_remote_path(
    settings, monkeypatch, value
):
    wire = Wire({"get_image": value, "get_file": value})
    loader = ImageLoader(settings, wire)
    monkeypatch.setattr(loader, "_client", lambda: Client([]))
    assert await loader.load_results([ImageRef(file="native-id")]) == [None]


async def test_403_retries_http_once_with_browser_user_agent(settings, monkeypatch, caplog):
    loader = ImageLoader(settings)
    client = Client([Response(b"", status=403), Response(png())])
    monkeypatch.setattr(loader, "_client", lambda: client)
    url = "https://multimedia.nt.qq.com.cn/download?rkey=private-signed-token"
    with caplog.at_level(logging.WARNING, logger="hakimi"):
        result = await loader.load_results([ImageRef(url=url)])
    assert result[0] is not None
    assert [u for u, _ in client.requests] == [url, url.replace("https://", "http://", 1)]
    assert all("Mozilla/5.0" in kw["headers"]["User-Agent"] for _, kw in client.requests)
    assert "status=403" in caplog.text and "private-signed-token" not in caplog.text


async def test_refreshed_rpc_url_is_preferred_to_expired_event_url(settings, monkeypatch):
    wire = Wire({"get_image": {"url": "https://gchat.qpic.cn/fresh"}, "get_file": {}})
    loader = ImageLoader(settings, wire)
    client = Client([Response(png())])
    monkeypatch.setattr(loader, "_client", lambda: client)
    assert (await loader.load_results([ImageRef(file="abc", url="https://gchat.qpic.cn/old")]))[0]
    assert client.requests[0][0] == "https://gchat.qpic.cn/fresh"


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/a",
        "http://evil.qpic.cn/a",
        "http://gchat.qpic.cn:8080/a",
        "http://gchat.qpic.cn@evil.test/a",
    ],
)
def test_http_compatibility_never_widens_ssrf_allowlist(url):
    with pytest.raises(Rejected):
        validate_image_url(url, allow_http=True)


async def test_compatibility_tls_is_scoped_to_cdn_client(settings):
    loader = ImageLoader(settings)
    async with loader._client() as client:
        assert client.connector._ssl.verify_mode == ssl.CERT_NONE
        assert client.trust_env is False


async def test_reply_uses_same_session_local_message_and_preserves_command(settings, message):
    original = message.model_copy(
        update={
            "event_id": "12",
            "text": "这是之前的截图",
            "sender_nickname": "截图者",
            "images": [ImageRef(file="pic")],
        }
    )

    async def lookup(current, ref):
        assert ref == "12"
        return original

    resolver = MessageContextResolver(settings, Wire({}), lookup)
    incoming = message.model_copy(update={"reply_ids": ["12"], "command_text": "这是谁"})
    expanded = await resolver.resolve(incoming)
    assert "[引用 截图者(QQ:20000) 的消息:" in expanded.text
    assert expanded.images[0].file == "pic" and expanded.images[0].origin == "引用消息"
    assert expanded.user_id == incoming.user_id and expanded.command_text == "这是谁"
    assert expanded.context_expanded


async def test_group_reply_cannot_read_private_or_another_group(settings, message):
    incoming = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "reply_ids": ["12"]}
    )
    for original in (
        message.model_copy(update={"event_id": "12", "text": "PRIVATESECRET"}),
        incoming.model_copy(
            update={"conversation_id": "50000", "event_id": "12", "text": "OTHERGROUP"}
        ),
    ):

        async def lookup(*_, original=original):
            return original

        expanded = await MessageContextResolver(settings, Wire({}), lookup).resolve(incoming)
        assert original.text not in expanded.text
        assert "引用消息暂不可读取" in expanded.text


async def test_remote_get_msg_requires_matching_message_and_group(settings, message):
    incoming = message.model_copy(
        update={"kind": "group", "conversation_id": "40000", "reply_ids": ["12"]}
    )
    value = {
        "message_id": 12,
        "message_type": "group",
        "group_id": 40000,
        "sender": {"user_id": 31000, "nickname": "原作者"},
        "message": [
            {"type": "image", "data": {"file": "pic"}},
            {"type": "text", "data": {"text": "是这张吗"}},
        ],
    }
    wire = Wire({"get_msg": value})
    expanded = await MessageContextResolver(settings, wire).resolve(incoming)
    assert wire.calls == [("get_msg", {"message_id": 12})]
    assert "原作者(QQ:31000)" in expanded.text and expanded.images[0].sender_id == "31000"
    value["group_id"] = 50000
    rejected = await MessageContextResolver(settings, wire).resolve(incoming)
    assert "是这张吗" not in rejected.text and not rejected.images


def test_forward_first_twenty_senders_self_anchor_images_and_no_command():
    nodes = [
        {
            "sender": {"user_id": 10000 if i == 0 else 31000 + i, "nickname": f"作者{i}"},
            "content": [
                {"type": "text", "data": {"text": f"第{i}条 /new-session"}},
                {"type": "image", "data": {"file": f"img{i}"}},
                {"type": "at", "data": {"qq": "10000"}},
            ],
        }
        for i in range(22)
    ]
    rich = parse_forward({"messages": nodes}, "10000", include_authors=True)
    assert "[转发记录 - 我自己(吉米奈喵)]" in rich.text
    assert "作者1(QQ:31001)" in rich.text
    assert "第19条" in rich.text and "第20条" not in rich.text
    assert len(rich.images) == 2 and rich.images[0].sender_id == "10000"
    assert not rich.addressed and not rich.mentioned_ids and not rich.command_text


async def test_forward_failures_are_placeholder_and_never_escape_scope(settings, message):
    wire = Wire({"get_forward_msg": Rejected("onebot_rpc_timeout")})
    incoming = message.model_copy(
        update={"forward_ids": ["forward-ref"], "command_text": "解释一下"}
    )
    result = await MessageContextResolver(settings, wire).resolve(incoming)
    assert result.forward_ids == [] and "合并转发暂不可读取" in result.text
    assert result.command_text == "解释一下"


async def test_rpc_cancellation_propagates(settings, message):
    class Cancel:
        async def call(self, *_):
            raise asyncio.CancelledError

    resolver = MessageContextResolver(settings, Cancel())
    with pytest.raises(asyncio.CancelledError):
        await resolver.resolve(message.model_copy(update={"reply_ids": ["12"]}))


async def test_expanded_context_never_exceeds_message_limit(settings, message):
    wire = Wire({"get_msg": Rejected("onebot_rpc_timeout")})
    incoming = message.model_copy(update={"text": "字" * 7998, "reply_ids": ["12"]})
    result = await MessageContextResolver(settings, wire).resolve(incoming)
    assert len(result.text) <= 8000
