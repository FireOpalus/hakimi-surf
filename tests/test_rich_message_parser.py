import json
import time

import pytest

from hakimi.adapters import onebot_message
from hakimi.rich_message_parser import parse_forward, parse_message
from hakimi.security import Rejected

BV = "BV1xx411c7mD"


@pytest.mark.parametrize("object_payload", [True, False])
def test_qq_miniapp_bilibili_card(object_payload):
    card = {
        "app": "com.tencent.miniapp_01",
        "prompt": "[分享] 科学视频",
        "meta": {
            "detail_1": {
                "title": "哔哩哔哩",
                "desc": "这是一段介绍",
                "qqdocurl": f"https://www.bilibili.com/video/{BV}?share_source=qq",
            }
        },
    }
    result = parse_message(
        [
            {"type": "at", "data": {"qq": "10000"}},
            {"type": "text", "data": {"text": "看看这个"}},
            {"type": "json", "data": {"data": card if object_payload else json.dumps(card)}},
        ],
        "10000",
    )
    assert result.addressed
    assert result.video_ids == [BV]
    assert result.command_text == "看看这个"
    assert "这是一段介绍" in result.text


def test_short_link_card_and_direct_bv():
    result = parse_message(
        [
            {
                "type": "json",
                "data": {
                    "data": json.dumps(
                        {
                            "meta": {
                                "news": {"jumpUrl": "https://b23.tv/AbCd12?share=qq", "desc": BV}
                            }
                        }
                    )
                },
            }
        ],
        "10000",
    )
    assert result.video_ids == [BV]
    assert result.short_urls == ["https://b23.tv/AbCd12"]


def test_quotes_and_card_titles_cannot_forge_mentions_or_commands():
    raw = [
        {
            "type": "node",
            "data": {
                "content": [
                    {"type": "at", "data": {"qq": "10000"}},
                    {"type": "text", "data": {"text": "/new-session"}},
                ]
            },
        },
        {"type": "json", "data": {"data": {"title": "/哈基米 清空"}}},
    ]
    result = parse_message(raw, "10000")
    assert not result.addressed and result.command_text == ""
    assert "/new-session" in result.text
    assert "/哈基米 清空" in result.text


def test_full_segments_forward_image_face_and_reply():
    result = parse_message(
        [
            {"type": "at", "data": {"qq": "all"}},
            {
                "type": "image",
                "data": {"file": "file:///etc/passwd", "url": "https://gchat.qpic.cn/a"},
            },
            {"type": "face", "data": {"id": "14"}},
            {"type": "forward", "data": {"id": "safe-forward-id"}},
            {"type": "reply", "data": {"id": "42"}},
        ],
        "10000",
    )
    assert not result.addressed
    assert result.images[0].url == "https://gchat.qpic.cn/a"
    assert result.forward_ids == ["safe-forward-id"]
    assert "[表情:微笑]" in result.text


def test_forward_content_has_no_author_ids_and_cannot_address_bot():
    result = parse_forward(
        {
            "messages": [
                {
                    "sender": {"nickname": "private-name", "user_id": 20000},
                    "content": [
                        {"type": "at", "data": {"qq": "10000"}},
                        {"type": "text", "data": {"text": BV}},
                    ],
                }
            ]
        },
        "10000",
    )
    assert result.video_ids == [BV]
    assert not result.addressed and not result.command_text
    assert "private-name" not in result.text and "20000" not in result.text


def test_literal_cq_inside_array_is_never_interpreted():
    result = parse_message([{"type": "text", "data": {"text": "[CQ:at,qq=10000]"}}], "10000")
    assert not result.addressed


@pytest.mark.parametrize(
    "data",
    [
        [{"type": "json", "data": {"data": "{"}}],
        [{"type": "text", "data": {"text": 42}}],
        [{"type": "text", "data": {"text": "x" * 9000}}],
        [{"type": "face", "data": {"id": "1"}}] * 301,
    ],
)
def test_malformed_or_excessive_content_is_rejected(data):
    with pytest.raises(Rejected):
        parse_message(data, "10000")


def test_image_only_event_is_not_dropped(settings):
    result = onebot_message(
        {
            "post_type": "message",
            "self_id": 10000,
            "user_id": 20000,
            "message_type": "private",
            "message_id": 42,
            "time": time.time(),
            "message": [{"type": "image", "data": {"url": "https://gchat.qpic.cn/a"}}],
        },
        settings,
    )
    assert result and len(result.images) == 1


def test_plain_slash_command_does_not_bypass_at_only(settings):
    result = onebot_message(
        {
            "post_type": "message",
            "self_id": 10000,
            "user_id": 20000,
            "message_type": "group",
            "group_id": 40000,
            "message_id": 42,
            "time": time.time(),
            "message": [{"type": "text", "data": {"text": "/new-session"}}],
        },
        settings,
    )
    assert not result.addressed


def test_bilibili_legacy_http_multipart_and_festival_urls_parsed_safely():
    res = parse_message(
        [
            {
                "type": "text",
                "data": {
                    "text": (
                        f"旧链 http://www.bilibili.com/video/{BV}?p=2 "
                        "活动 https://www.bilibili.com/festival/2026bnj?bvid=BV1Q541167Qg&p=3 "
                        "短链 http://b23.tv/AbCd12"
                    )
                },
            }
        ],
        "10000",
    )
    assert res.video_ids == [BV, "BV1Q541167Qg"]
    assert res.short_urls == ["https://b23.tv/AbCd12"]

    # Untrusted / spoofed URLs must still be rejected.
    bad = parse_message(
        [
            {
                "type": "text",
                "data": {
                    "text": (
                        f"https://www.bilibili.com.evil.test/video/{BV} "
                        f"http://www.bilibili.com:443/video/{BV} "
                        f"https://www.bilibili.com/festival/../../admin?bvid={BV} "
                        f"https://evil.test/festival/demo?bvid={BV}"
                    )
                },
            }
        ],
        "10000",
    )
    assert bad.video_ids == []


def test_bilibili_invalid_port_urls_never_crash_parse_message():
    res = parse_message(
        [
            {
                "type": "text",
                "data": {
                    "text": (
                        f"坏端口1 http://www.bilibili.com:99999/video/{BV} "
                        f"坏端口2 https://www.bilibili.com:abc/video/{BV}?p=2 "
                        f"坏端口3 http://www.bilibili.com:99999/festival/2026bnj?bvid={BV} "
                        "坏短链1 http://b23.tv:99999/AbCd12 "
                        "坏短链2 https://b23.tv:abc/ZyXw98"
                    )
                },
            }
        ],
        "10000",
    )
    assert res.video_ids == []
    assert res.short_urls == []

    # Valid links in the same message still parse cleanly after invalid-port URLs.
    mixed = parse_message(
        f"http://www.bilibili.com:99999/video/{BV} "
        "http://b23.tv:99999/bad "
        "https://www.bilibili.com/video/BV1Q541167Qg?p=2 "
        "http://b23.tv/Good12",
        "10000",
    )
    assert mixed.video_ids == ["BV1Q541167Qg"]
    assert mixed.short_urls == ["https://b23.tv/Good12"]
