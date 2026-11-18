import json
import time
from decimal import Decimal

import httpx
from pydantic import SecretStr

from hakimi.adapters import Sender
from hakimi.bili_watcher import BiliPolicy, BiliWatcher, CredentialUpdate
from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.media import ModelImage
from hakimi.memory_compressor import MemoryCompressor
from hakimi.models import ImageRef
from hakimi.roaming_logger import RoamingLogger
from hakimi.security import Rejected, configured_secrets
from hakimi.service import Service
from hakimi.web_search import WebSearchConfig
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import admin_api as admin_api
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_social_integration import Wire, group, process
from tests.test_streaming_v0100 import action


class RichWire(Wire):
    async def call(self, action_name, params):
        if action_name == "set_msg_emoji_like":
            self.calls.append((action_name, params))
            return {}
        if action_name in {"group_poke", "friend_poke", "send_poke"}:
            self.calls.append((action_name, params))
            return {}
        if action_name == "set_self_longnick":
            self.calls.append((action_name, params))
            return {"result": 0}
        return await super().call(action_name, params)


class ActionFakeGateway(FakeGateway):
    def __init__(self, reply_json: str):
        super().__init__()
        self.reply_json = reply_json

    async def generate(self, payload, subject, **kwargs):
        await super().generate(payload, subject, **kwargs)
        return self.reply_json


async def test_roaming_diary_logs_allowlisted_chats_without_roaming_group_ids(
    settings, store, message
):
    s = settings.model_copy(
        update={
            "roaming_enabled": True,
            "roaming_group_ids": set(),
            "deepseek_api_key": SecretStr("sk-deepseek-secret-token-1234"),
        }
    )
    diary = RoamingLogger(s, store, None)
    await diary.initialize()
    now = time.time()
    owner_dm = message.model_copy(update={"text": "今天调试代码"})
    friend_dm = message.model_copy(
        update={
            "user_id": "30000",
            "conversation_id": "30000",
            "sender_nickname": "阿花",
            "text": "周末去喝奶茶吗",
        }
    )
    group_msg = group(
        message, "g-1", text="大家来看看新番 " + s.deepseek_api_key.get_secret_value()
    )
    unlisted_group = message.model_copy(
        update={"kind": "group", "conversation_id": "777777", "text": "未授权群不要进"}
    )
    await diary.group("ev-owner", owner_dm, now=now)
    await diary.group("ev-friend", friend_dm, now=now)
    await diary.group("ev-group", group_msg, now=now)
    await diary.bot_reply("rep-1", friend_dm, "走啊走啊~", now=now)
    await diary.group("ev-unlisted", unlisted_group, now=now)

    md = await diary.render(diary.local(now).date().isoformat())
    assert "私聊 · BOISER" in md and "今天调试代码" in md
    assert "私聊 · 阿花" in md and "周末去喝奶茶吗" in md
    assert "群聊 40000" in md and "大家来看看新番" in md
    assert "哈基米:走啊走啊~" in md
    assert "未授权群不要进" not in md
    assert s.deepseek_api_key.get_secret_value() not in md


async def test_bili_patrol_memory_preferences_and_error_classification(settings, store, message):
    from hakimi.session_manager import SessionManager
    from tests.fakes import FakeSender

    settings.owner_qq_id = "20000"
    sessions = SessionManager(settings, store)
    await sessions.remember(message, "我最近超想了解芯片架构和 Rust 异步原理", "好呀~")
    snap = await sessions.snapshot(message)
    assert snap["recent_messages"]

    captured = []

    class BiliGw(FakeGateway):
        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            scenario = kwargs.get("scenario")
            captured.append((scenario, payload))
            if scenario == Scenario.BILI_SELECTION:
                return json.dumps(
                    {"selected": [{"bvid": "BV1xx411c7mD", "like": False, "favorite": False}]}
                )
            return "芯片科普视频简述。"

    svc = Service(settings, store, BiliGw(), FakeBili(), FakeSender())
    svc.sessions = sessions

    async def transport(request):
        path = request.url.path
        if path == "/x/web-interface/nav":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "isLogin": True,
                        "mid": 123456,
                        "wbi_img": {
                            "img_url": "https://i0.hdslb.com/bfs/wbi/" + "a" * 32 + ".png",
                            "sub_url": "https://i0.hdslb.com/bfs/wbi/" + "b" * 32 + ".png",
                        },
                    },
                },
            )
        if path == "/x/web-interface/wbi/index/top/feed/rcmd":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "item": [
                            {
                                "goto": "av",
                                "bvid": "BV1xx411c7mD",
                                "title": "Rust教程",
                                "owner": {"name": "UP"},
                            },
                            {
                                "goto": "av",
                                "bvid": "BV1Q541167Qg",
                                "title": "芯片架构科普",
                                "owner": {"name": "UP2"},
                            },
                        ]
                    },
                },
            )
        if path == "/x/polymer/web-dynamic/v1/feed/all":
            return httpx.Response(200, json={"code": 0, "data": {"items": []}})
        if path == "/x/web-interface/view":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "bvid": "BV1xx411c7mD",
                        "aid": 1,
                        "cid": 2,
                        "title": "Rust",
                        "desc": "desc",
                    },
                },
            )
        if path in {"/x/player/wbi/v2", "/x/v2/reply"}:
            return httpx.Response(200, json={"code": 0, "data": {}})
        return httpx.Response(404)

    async def no_sleep(_n):
        return None

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
            watcher = BiliWatcher(
                svc,
                client=http,
                clock=lambda: 1700000000.0,
                sleep=no_sleep,
            )
            await watcher.initialize()
            await watcher.set_policy(BiliPolicy(enabled=True))
            await watcher.update_credentials(CredentialUpdate(sessdata="s", buvid3="b"))
            res = await watcher.run_once(slot="13:00")
            assert res["status"] == "done"
            sel_payload = next(p for s, p in captured if s == Scenario.BILI_SELECTION)
            assert "memory_preferences" in sel_payload
            assert any(
                "Rust" in item
                for item in sel_payload["memory_preferences"].get("recent_chat_topics", [])
            )

            # Gemini unavailability during patrol must NOT record bili_account_failed.
            class FailGeminiGw(FakeGateway):
                async def screen(self, payload, subject, **kwargs):
                    raise Rejected("gemini_unavailable")

            svc.ai = FailGeminiGw()
            store.runtime.sink.pending.clear()
            res2 = await watcher.run_once(slot="20:30")
            assert res2 == {"status": "failed", "learned": 0, "reason": "gemini_unavailable"}
            assert "bili_account_failed" not in list(store.runtime.sink.pending)
    finally:
        await svc.stop()


async def test_memory_flex_503_requeues_zero_cost(settings, store, message, sdk_pair):
    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(503, json={"error": {"message": "capacity"}})

    gw = GeminiGateway(settings, store, *sdk_pair(handler))
    from hakimi.session_manager import SessionManager

    sessions = SessionManager(settings, store)
    for n in range(2):
        await sessions.admit_history(
            message,
            [
                message.model_copy(
                    update={
                        "event_id": f"m-{n}",
                        "text": f"我喜欢编程记录第 {n} 条",
                        "created_at": 100.0 + n,
                    }
                )
            ],
            expected_epoch=0,
        )
    compressor = MemoryCompressor(settings, sessions, gw, store)
    result = await compressor.extract(message, force=True, background=True)
    assert result["status"] == "provider_backoff"
    row = await store.one(
        "SELECT amount,status FROM ledger WHERE lane='memory' ORDER BY rowid DESC LIMIT 1"
    )
    assert row["amount"] == 0 and row["status"] == "settled"
    view = await sessions.dual.view(message)
    assert view["jobs"].get("pending", 0) >= 1


async def test_qq_private_memes_reactions_poke_auto_save_and_owner_longnick(
    settings, store, message
):
    from io import BytesIO

    from PIL import Image

    Image.new("RGB", (64, 64), (180, 120, 220)).save(buf := BytesIO(), format="JPEG")
    jpg_bytes = buf.getvalue()
    Image.new("RGB", (64, 64), (50, 200, 100)).save(buf2 := BytesIO(), format="JPEG")
    new_jpg = buf2.getvalue()

    class MockImages:
        def __init__(self, raw_bytes: bytes):
            self.raw_bytes = raw_bytes

        async def load_results(self, refs):
            return [ModelImage("/9j/2Q==") for _ in refs]

        async def fetch_raw(self, ref, *, max_bytes=5 * 1024 * 1024):
            return self.raw_bytes

    gw = ActionFakeGateway("{}")
    wire = RichWire()
    async with httpx.AsyncClient() as http:
        sender = Sender(settings, http, wire)
        svc = Service(
            settings,
            store,
            gw,
            FakeBili(),
            sender,
            image_loader=MockImages(new_jpg),
        )
        try:
            await svc.memes.initialize()
            await svc.moderation.initialize()
            await svc.longnick.initialize()
            await svc.memes.upload_bytes("开心喵", jpg_bytes)

            # 1) Owner private: sticker + emoji_like + poke_qq + set_longnick + auto-save meme
            gw.reply_json = action(
                bubbles=["主人你好喵"],
                anchor={"reply_id": "101", "mentions": []},
                sticker={"label": "开心喵", "timing": "after_text"},
                emoji_like="爱心",
                poke_qq="20000",
                save_meme_label="绿色猫猫",
                set_longnick="今日与 BOISER 同步在线",
            )
            owner_msg = message.model_copy(
                update={
                    "event_id": "101",
                    "text": "帮我收下这张图并换个签名 [图片]",
                    "images": [ImageRef(file="newmeme.jpg")],
                }
            )
            await process(svc, owner_msg)
            assert await svc.deliver_once()
            assert "绿色猫猫" in svc.memes.labels
            actions = [name for name, _ in wire.calls]
            assert "set_self_longnick" in actions
            assert "send_private_msg" in actions
            assert "set_msg_emoji_like" in actions
            assert any(name in {"friend_poke", "send_poke"} for name in actions)

            # 2) Group ordinary member cannot trigger set_longnick; abstain still reacts & pokes
            wire.calls.clear()
            gw.reply_json = action(
                abstain=True,
                bubbles=[""],
                anchor={"reply_id": "202", "mentions": []},
                emoji_like="赞",
                poke_qq="30000",
                set_longnick="越权签名尝试",
            )
            grp_msg = group(
                message,
                "202",
                text="@吉米奈喵 拍拍你",
                addressed=True,
                attention_reason="mention",
                mentioned_ids=["10000"],
            )
            await process(svc, grp_msg)
            assert await svc.deliver_once()
            action_names = [name for name, _ in wire.calls]
            assert "set_self_longnick" not in action_names
            assert "send_group_msg" not in action_names
            assert "set_msg_emoji_like" in action_names
            assert any(name in {"group_poke", "send_poke"} for name in action_names)
        finally:
            await svc.stop()


async def test_manual_balance_calibration(admin_api):
    app, client, _ = admin_api
    admin_store = app.state.service.store

    # Put a balance, run paid reserve/settle and unbilled search reserve/settle
    r1 = await client.put("/admin/api/account-balance", json={"balance_usd": "50.00"})
    assert r1.status_code == 200 and r1.json()["remaining_usd"] == "50.0000"

    res_chat = await admin_store.reserve("sub", "chat", Decimal("1.00"), model_lane="hydrogen")
    await admin_store.settle(res_chat, Decimal("0.25"), model_lane="hydrogen")
    res_search = await admin_store.reserve("sub", "search", Decimal("0.50"), model_lane="hydrogen")
    await admin_store.settle(res_search, Decimal("0.20"), model_lane="hydrogen")

    bal = (await client.get("/admin/api/account-balance")).json()
    assert bal["remaining_usd"] == "49.7500"
    dash = (await client.get("/admin/api/dashboard")).json()
    assert dash["budget_status"]["account_balance"]["remaining_usd"] == "49.7500"
    assert all(row["lane"] != "search" for row in dash["usage"]["lanes"])

    cleared = await client.delete("/admin/api/account-balance")
    assert cleared.status_code == 200 and cleared.json()["configured"] is False


async def test_legacy_search_ledger_reconciled_on_store_open(store):
    # Legacy search ledger row reconciliation on fresh store open
    await store.db.execute(
        "INSERT INTO ledger VALUES ('old-s','2026-11-16','sub','search',12345,12345,'settled')"
    )
    await store.db.execute("UPDATE budget_totals SET amount=12345")
    await store.db.execute("DELETE FROM meta WHERE key='search_unbilled_v1'")
    await store.close()
    await store.open()
    assert (await store.one("SELECT amount,reserved FROM ledger WHERE id='old-s'"))[:] == (0, 0)
    assert (await store.one("SELECT amount FROM budget_totals"))[0] == 0


async def test_monthly_google_5000_auto_switch_to_deepseek_and_admin_key(admin_api, sdk_pair):
    app, client, _ = admin_api
    admin_store = app.state.service.store
    admin_settings = app.state.settings
    admin_ws = admin_store.runtime.web_search
    await admin_ws.save(WebSearchConfig(enabled=True, daily_limit=20, monthly_google_limit=2))

    # Save DeepSeek key via admin API; verify encrypted, secret-registered, and redacted
    ds_save = await client.post(
        "/admin/api/web-search/deepseek",
        json={
            "api_key": "sk-deepseek-test-key-xyz99",
            "base_url": "https://api.deepseek.com/",
            "model": "deepseek-flash",
        },
    )
    assert ds_save.status_code == 200 and ds_save.json()["api_key_configured"] is True
    assert "sk-deepseek-test-key-xyz99" not in ds_save.text
    ws_status = (await client.get("/admin/api/web-search")).json()
    assert "sk-deepseek-test-key-xyz99" not in json.dumps(ws_status)
    assert any(sec == "sk-deepseek-test-key-xyz99" for sec in configured_secrets(admin_settings))

    # Simulate 2 Google queries already consumed this month, then auto-switch to DeepSeek
    ds_calls = []
    use_chat_fallback = [False]

    def ds_handler(request):
        ds_calls.append((request.url.path, json.loads(request.content)))
        if request.url.path == "/v1/tools/search" and not use_chat_fallback[0]:
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "title": "DeepSeek 官方新闻",
                            "url": "https://example.org/ds",
                            "summary": "DeepSeek 搜索返回的最新核实结果。",
                            "rerank_score": 2.4,
                        }
                    ]
                },
            )
        if request.url.path == "/chat/completions" and use_chat_fallback[0]:
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {
                                "role": "assistant",
                                "content": "DeepSeek Chat 联网搜索返回的备选结果。",
                            }
                        }
                    ]
                },
            )
        return httpx.Response(404)

    def gemini_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(
            200,
            json={
                **interaction_response(),
                "steps": [
                    {"type": "google_search_call", "id": "s1", "arguments": {"queries": ["q"]}},
                    {
                        "type": "model_output",
                        "content": [{"type": "text", "text": "Google结果。"}],
                    },
                ],
                "usage": {
                    "total_input_tokens": 50,
                    "total_output_tokens": 20,
                    "grounding_tool_count": [{"type": "google_search", "count": 2}],
                },
            },
        )

    gw = GeminiGateway(admin_settings, admin_store, *sdk_pair(gemini_handler))
    gw._deepseek_transport = httpx.MockTransport(ds_handler)

    # 1st search consumes 2 google_search queries -> reaches monthly_google_limit=2
    out1 = await gw.search("测试 Google", "s")
    assert "Google结果" in out1 and not ds_calls

    # 2nd search automatically switches to DeepSeek /v1/tools/search
    out2 = await gw.search("测试 DeepSeek", "s")
    assert "DeepSeek" in out2 and ds_calls[0][0] == "/v1/tools/search"

    # 3rd search tests /chat/completions fallback when /v1/tools/search returns 404
    use_chat_fallback[0] = True
    out3 = await gw.search("测试 Chat Fallback", "s")
    assert "备选结果" in out3
    assert ds_calls[-1][0] == "/chat/completions"
    st = await admin_ws.status()
    assert st["monthly"]["google_queries"] == 2
    assert st["monthly"]["deepseek_attempts"] == 2
    assert st["monthly"]["active_provider"] == "deepseek"
