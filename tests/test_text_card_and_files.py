"""Tests for rich Pillow Markdown/code/table cards & safe text/log file reading."""

import base64
import io
import json
import time

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.adapters import Sender, notice_message, onebot_message
from hakimi.app import create_app
from hakimi.file_reader import FileReader, decode_text_bytes
from hakimi.models import FileRef
from hakimi.outgoing import deliver_actions
from hakimi.rich_message_parser import parse_message, sanitize_filename, valid_file_id
from hakimi.security import Rejected
from hakimi.service import Service
from hakimi.text_card import render_card, should_render_rich_card
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import TOKEN


def test_should_render_rich_card_rules():
    plain = "这是一段普通聊天回复，没有长代码也没有表格。"
    assert not should_render_rich_card(plain, "rich_auto")
    short_code = "试试 `print(1)`\n```python\nprint(1)\n```"
    assert not should_render_rich_card(short_code, "rich_auto")
    long_code = "如下代码：\n```python\na = 1\nb = 2\nc = 3\nprint(a + b + c)\n```"
    assert should_render_rich_card(long_code, "rich_auto")
    assert not should_render_rich_card(long_code, "fallback_only")
    table = "| 项 | 说明 |\n| :--- | :--- |\n| A | 第一项 |\n| B | 第二项 |"
    assert should_render_rich_card(table, "rich_auto")
    assert not should_render_rich_card(table, "fallback_only")


def test_render_card_supports_markdown_code_cjk_and_table_within_bounds():
    markdown = (
        "# 排查结论\n\n"
        "请核对 `config.yaml` 中的**关键参数**，参考对照表：\n\n"
        "| 参数 | 默认值 | 说明 |\n"
        "| --- | --- | --- |\n"
        "| host | 127.0.0.1 | 本地监听 |\n"
        "| port | 12141 | Web 管理端口 |\n\n"
        "```python\n"
        "def greet(name: str) -> str:\n"
        "    # 中文注释与 ASCII 等宽混排检查\n"
        "    msg = f'你好, {name}'\n"
        "    return msg\n"
        "```\n\n"
        "> 注意不要把密钥提交进仓库\n"
        "- 先检查端口\n"
        "1. 查看容器日志\n"
    )
    encoded = render_card(markdown)
    assert encoded.startswith("base64://")
    raw = base64.b64decode(encoded[9:])
    assert raw.startswith(b"\xff\xd8\xff") and len(raw) < 4_500_000
    with Image.open(io.BytesIO(raw)) as img:
        assert img.width == 960
        assert 200 <= img.height <= 4400


async def test_group_deliver_actions_uses_send_card_only_for_rich_content(settings, store, message):
    calls = []

    class CardSender(FakeSender):
        async def send_card(self, msg, text, *, before_send=None):
            calls.append(("card", msg.conversation_id, text))
            return {"message_id": 9001}

        async def send_folded(self, msg, text, *, before_send=None):
            calls.append(("folded", msg.conversation_id, text))
            return {"message_id": 9002}

        async def send_action_bubble(self, msg, segments, *, targets=None, before_send=None):
            calls.append(("bubble", msg.conversation_id, segments[-1]["data"]["text"]))
            return {"message_id": 9003}

    svc = Service(settings, store, FakeGateway(), FakeBili(), CardSender())
    try:
        grp = message.model_copy(
            update={"kind": "group", "conversation_id": "40000", "event_id": "m-card"}
        )
        rich_text = "| 列1 | 列2 |\n| --- | --- |\n| 值1 | 值2 |"
        await store.enqueue_reply(
            "job-card",
            grp,
            rich_text,
            session_epoch=0,
            actions={
                "abstain": False,
                "bubbles": [rich_text],
                "anchor": {"reply_id": None, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "folded": None,
                "watermark": 0,
                "public": True,
            },
        )
        assert await deliver_actions(svc, await store.claim_reply())
        assert calls[0][0] == "card" and "| 列1 | 列2 |" in calls[0][2]

        calls.clear()
        settings.text_card_mode = "fallback_only"
        await store.enqueue_reply(
            "job-plain",
            grp,
            rich_text,
            session_epoch=0,
            actions={
                "abstain": False,
                "bubbles": [rich_text],
                "anchor": {"reply_id": None, "mentions": []},
                "sticker": {"label": None, "timing": "after_text"},
                "image_digest": None,
                "folded": None,
                "watermark": 0,
                "public": True,
            },
        )
        assert await deliver_actions(svc, await store.claim_reply())
        assert calls[0][0] == "bubble"
    finally:
        await svc.stop()


def test_sender_send_card_sanitizes_and_checks_group_boundary(settings, message):
    class Wire:
        def __init__(self):
            self.sent = []

        async def call(self, action, params):
            self.sent.append((action, params))
            return {"message_id": 101}

    wire = Wire()
    sender = Sender(settings, httpx.AsyncClient(), wire)
    priv_err = None
    try:
        import asyncio

        asyncio.run(sender.send_card(message, "secret"))
    except Rejected as exc:
        priv_err = str(exc)
    assert priv_err == "card_group_only"


def test_parse_file_segments_and_notices_enforce_safety(settings):
    assert sanitize_filename("../../etc/passwd") == "passwd"
    assert sanitize_filename("foo\\bar\\crash.log") == "crash.log"
    assert valid_file_id("/etc/passwd") == ""
    assert valid_file_id("../secret") == ""
    assert valid_file_id("abc-123/file_id") == "abc-123/file_id"

    parsed = parse_message(
        [
            {"type": "file", "data": {"file_name": "app.log", "id": "fid-1", "size": 512}},
            {"type": "text", "data": {"text": "帮我看看报错"}},
        ],
        "10000",
    )
    assert len(parsed.files) == 1
    assert parsed.files[0].name == "app.log" and parsed.files[0].file_id == "fid-1"
    assert "[文件:app.log]" in parsed.text

    upload = notice_message(
        {
            "post_type": "notice",
            "notice_type": "group_upload",
            "self_id": settings.onebot_self_id,
            "group_id": 40000,
            "user_id": 30000,
            "time": time.time(),
            "file": {"id": "/fid-group", "name": "error.log", "size": 1024, "busid": 102},
        },
        settings,
    )
    assert upload is not None and not upload.addressed
    assert upload.files[0].name == "error.log" and upload.files[0].busid == 102


@pytest.mark.parametrize(
    "raw,expected_code",
    [
        (b"\x00\x01\x02ELF", "binary_unsupported"),
        (b"\xff\xfe\xfd", "encoding_unsupported"),
    ],
)
def test_decode_text_bytes_rejects_binary_and_garbage(settings, raw, expected_code):
    with pytest.raises(Rejected, match=expected_code):
        decode_text_bytes(raw, secrets=(), settings=settings)


async def test_file_reader_allowlist_quota_and_head_tail_truncation(settings, store, message):
    class Transport:
        def __init__(self, payload_b64):
            self.payload_b64 = payload_b64
            self.calls = []

        async def call(self, action, params):
            self.calls.append((action, params))
            return {"base64": self.payload_b64}

    long_log = ("HEAD-" + "A" * 1500) + "\n" + ("M" * 2000) + "\nTraceback: TAIL-ERR-42"
    b64 = base64.b64encode(long_log.encode("utf-8")).decode("ascii")
    reader = FileReader(settings, store, transport=Transport(b64))

    for bad_name in ("app.exe", "archive.zip", ".env", ".env.prod", "dump.sqlite"):
        with pytest.raises(Rejected, match="unsupported_extension"):
            await reader.read_one(
                FileRef(name=bad_name, file_id="id-1", file_size=100), message, "subj"
            )

    loaded = await reader.read_one(
        FileRef(name="crash.log", file_id="id-2", file_size=len(long_log)), message, "subj"
    )
    assert (
        loaded.truncated
        and "HEAD-" in loaded.content
        and "中间省略" in loaded.content
        and "TAIL-ERR-42" in loaded.content
    )

    settings.file_daily_limit_per_user = 1
    with pytest.raises(Rejected, match="daily_limit"):
        await reader.read_one(
            FileRef(name="next.txt", file_id="id-3", file_size=10), message, "subj"
        )


async def test_file_reader_ignores_local_path_and_blocks_ssrf_url(settings, store, message):
    class LocalPathTransport:
        async def call(self, action, params):
            return {"file": "/etc/passwd", "url": "http://127.0.0.1/secret.txt"}

    reader = FileReader(settings, store, transport=LocalPathTransport())
    with pytest.raises(Rejected, match="unavailable"):
        await reader.read_one(FileRef(name="notes.txt", file_id="fid-ssrf"), message, "subj")


async def test_service_reads_recent_group_upload_runs_source_gate_and_consumes(
    settings, store, message
):
    settings.onebot_allowed_groups = {"40000"}
    settings.group_debounce_seconds = 0
    settings.mention_debounce_seconds = 0
    settings.send_delay_min = 0
    settings.send_delay_max = 0

    class FileTransport:
        async def call(self, action, params):
            if action == "get_group_file_url":
                return {"base64": base64.b64encode(b"ValueError: bad config line 42").decode()}
            return {"message_id": 777}

    ai = FakeGateway()
    svc = Service(
        settings, store, ai, FakeBili(), Sender(settings, httpx.AsyncClient(), FileTransport())
    )
    try:
        now = time.time()
        upload = onebot_message(
            {
                "post_type": "message",
                "message_type": "group",
                "self_id": settings.onebot_self_id,
                "user_id": 30000,
                "group_id": 40000,
                "message_id": 8100,
                "time": now - 2,
                "message": [
                    {"type": "file", "data": {"file_name": "err.log", "id": "f-1", "size": 28}}
                ],
            },
            settings,
        )
        assert await svc.accept(upload) == "ignored"
        followup = onebot_message(
            {
                "post_type": "message",
                "message_type": "group",
                "self_id": settings.onebot_self_id,
                "user_id": 30000,
                "group_id": 40000,
                "message_id": 8101,
                "time": now,
                "message": [
                    {"type": "at", "data": {"qq": settings.onebot_self_id}},
                    {"type": "text", "data": {"text": "帮我看下刚才的文件"}},
                ],
            },
            settings,
        )
        assert await svc.accept(followup) == "accepted"
        job = await store.claim()
        await svc.process(job)
        source_screens = [
            c for c in ai.calls if c[0] == "screen" and c[3].get("purpose") == "source"
        ]
        assert source_screens and "err.log" in json.dumps(source_screens[0][1])
        gen_call = next(c for c in ai.calls if c[0] == "generate")
        assert gen_call[1]["file_attachments"][0]["filename"] == "err.log"
        assert "ValueError: bad config line 42" in gen_call[1]["file_attachments"][0]["content"]
        assert await svc.deliver_once()
        consumed = await svc.store.groups.consumed_file_events(followup)
        assert "8100" in consumed

        ai.calls.clear()
        ai.block_purpose = "source"
        evil_msg = onebot_message(
            {
                "post_type": "message",
                "message_type": "group",
                "self_id": settings.onebot_self_id,
                "user_id": 30000,
                "group_id": 40000,
                "message_id": 8102,
                "time": now + 1,
                "message": [
                    {"type": "at", "data": {"qq": settings.onebot_self_id}},
                    {"type": "file", "data": {"file_name": "evil.txt", "id": "f-2", "size": 28}},
                    {"type": "text", "data": {"text": "分析这个文件"}},
                ],
            },
            settings,
        )
        assert await svc.accept(evil_msg) == "accepted"
        await svc.process(await store.claim())
        assert not any(c[0] == "generate" for c in ai.calls)
    finally:
        await svc.stop()


async def test_admin_content_tools_persistence_and_reset(settings):
    settings.admin_token = SecretStr(TOKEN)
    headers = {"Authorization": "Bearer " + TOKEN}
    app = create_app(settings.model_copy(), gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers=headers,
        ) as client:
            initial = (await client.get("/admin/api/content-tools")).json()
            assert initial["source"] == "env"
            assert initial["config"]["text_card_mode"] == "rich_auto"
            updated = await client.put(
                "/admin/api/content-tools",
                json={
                    "file_read_enabled": False,
                    "file_max_bytes": 65536,
                    "file_daily_limit_per_user": 5,
                    "text_card_mode": "fallback_only",
                },
            )
            assert updated.status_code == 200 and updated.json()["source"] == "admin"

    restarted = create_app(settings.model_copy(), gateway_factory=FakeGateway, start_workers=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted),
            base_url="http://127.0.0.1:12141",
            headers=headers,
        ) as client:
            res = (await client.get("/admin/api/content-tools")).json()
            assert res["source"] == "admin"
            assert res["config"]["file_max_bytes"] == 65536
            assert res["config"]["text_card_mode"] == "fallback_only"
            reset = (await client.delete("/admin/api/content-tools")).json()
            assert reset["source"] == "env" and reset["config"]["file_read_enabled"] is True
