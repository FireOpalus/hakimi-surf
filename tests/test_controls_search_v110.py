import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from hakimi.debounce_policy import DebounceConfig
from hakimi.gemini import GeminiGateway
from hakimi.security import Rejected
from hakimi.web_search import (
    WebSearchConfig,
    has_concrete_search_subject,
    natural_search_cue,
    parse_result,
    public_url,
    query_count,
    sanitize_model_search_query,
    search_query,
)
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_admin import admin_api as admin_api
from tests.test_group_pipeline import clock as clock
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_streaming_v0100 import ActionSender, action


async def enable_search(store, **updates):
    await store.runtime.web_search.save(WebSearchConfig(enabled=True, **updates))


def grounded_response():
    data = interaction_response()
    data["steps"] = [
        {"type": "google_search_call", "id": "s1", "arguments": {"queries": ["公开资料"]}},
        {
            "type": "google_search_result",
            "call_id": "s1",
            "result": [
                {
                    "search_suggestions": '<style>.x{}</style><a href="https://www.google.com/search?q=public">公开资料</a>'
                }
            ],
        },
        {
            "type": "model_output",
            "content": [
                {
                    "type": "text",
                    "text": "已核实的公开事实。",
                    "annotations": [
                        {
                            "type": "url_citation",
                            "url": "https://example.org/source",
                            "title": "原始来源",
                            "start_index": 0,
                            "end_index": 9,
                        }
                    ],
                }
            ],
        },
    ]
    data["usage"].update(
        total_cached_tokens=40, grounding_tool_count=[{"type": "google_search", "count": 2}]
    )
    return data


@pytest.mark.parametrize("path", ["debounce", "web-search"])
@pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
async def test_controls_authentication_precedes_changes(admin_api, path, method):
    app, client, gateway = admin_api
    response = await client.request(
        method, "/admin/api/" + path, headers={"Authorization": "Bearer bad"}, json={}
    )
    assert response.status_code == 401
    assert not await app.state.service.store.all("SELECT * FROM admin_config")
    assert not gateway.calls


async def test_controls_save_restart_reset_and_validate(admin_api):
    app, client, gateway = admin_api
    store = app.state.service.store
    original = (await client.get("/admin/api/debounce")).json()
    debounce = DebounceConfig(**dict.fromkeys(DebounceConfig.model_fields, 30.0)).model_dump()
    response = await client.put("/admin/api/debounce", json=debounce)
    assert response.status_code == 200 and response.json()["config"] == debounce
    response = await client.put(
        "/admin/api/web-search", json={"enabled": True, "daily_limit": 3, "daily_usd": "0.75"}
    )
    assert response.status_code == 200 and response.json()["source"] == "sqlite"
    await store.runtime.load_config()
    assert store.groups.config.mention_debounce_seconds == 30.0
    assert store.runtime.web_search.config.enabled
    for path, invalid in [
        ("debounce", {"group_debounce_seconds": 30.1}),
        ("debounce", {"group_debounce_max_seconds": -1}),
        ("web-search", {"enabled": True, "daily_limit": -1}),
        ("web-search", {"enabled": True, "daily_usd": "NaN"}),
    ]:
        assert (await client.put("/admin/api/" + path, json=invalid)).status_code == 400
    assert store.groups.config.mention_debounce_seconds == 30.0
    assert (await client.delete("/admin/api/debounce")).json()["config"] == original["environment"]
    assert not (await client.delete("/admin/api/web-search")).json()["config"]["enabled"]
    assert not gateway.calls


async def test_debounce_retimes_pending_not_working(store, message, clock):
    state = store.runtime
    await state.set_debounce(DebounceConfig())
    assert await store.admit(message) == "accepted"
    assert await store.claim() is None
    start = clock[0]
    clock[0] += 0.3
    await state.set_debounce(DebounceConfig(mention_debounce_seconds=1.2))
    row = await store.one("SELECT ready_at FROM group_debounce_batches")
    assert row[0] == pytest.approx(start + 1.2)
    clock[0] += 0.4
    assert await store.claim() is None
    await state.set_debounce(DebounceConfig(mention_debounce_seconds=0.1))
    job = await store.claim()
    assert job is not None
    before = dict(await store.one("SELECT * FROM group_debounce_batches"))
    await state.set_debounce(DebounceConfig(mention_debounce_seconds=4.5))
    assert dict(await store.one("SELECT * FROM group_debounce_batches")) == before
    assert (await store.one("SELECT status FROM jobs"))[0] == "working"
    assert not await store.all("SELECT * FROM ledger")


async def test_debounce_pending_capped_and_zero_release(store, message, clock):
    await store.runtime.set_debounce(
        DebounceConfig(mention_debounce_seconds=30, group_debounce_max_seconds=20)
    )
    start = clock[0]
    await store.admit(message)
    clock[0] += 1
    await store.admit(message.model_copy(update={"event_id": "follow", "created_at": clock[0]}))
    assert (await store.one("SELECT ready_at FROM group_debounce_batches"))[0] == start + 20
    await store.runtime.set_debounce(DebounceConfig(group_debounce_max_seconds=0))
    job = await store.claim()
    assert len(job["batch_messages"]) == 2


async def test_attribution_survives_unknown_usage_and_crash(settings, store):
    pending = await store.reserve("subject", "chat", Decimal("0.01"), model_lane="hydrogen")
    known_model = await store.reserve("subject", "chat", Decimal("0.02"), model_lane="decision")
    await store.settle(known_model, None)
    assert (await store.one("SELECT model_lane FROM ledger_usage WHERE id=?", (known_model,)))[
        0
    ] == "decision"
    before = (await store.one("SELECT amount FROM budget_totals"))[0]
    await store.close()
    await store.open()
    after = await store.one("SELECT amount,status FROM ledger WHERE id=?", (pending,))
    assert after[0] == 10000 and after[1] == "unknown"
    assert (await store.one("SELECT model_lane FROM ledger_usage WHERE id=?", (pending,)))[
        0
    ] == "hydrogen"
    assert (await store.one("SELECT amount FROM budget_totals"))[0] == before
    models = (await store.usage())["token_usage"]["today"]["by_model"]
    assert {m["model_lane"] for m in models} == {"hydrogen", "decision"}
    assert sum(m["charged_amount"] for m in models) == before
    await store.close()
    await store.open()
    assert (await store.one("SELECT amount FROM budget_totals"))[0] == before


async def test_legacy_unknown_not_relabelled_as_drive(store):
    key = await store.reserve("legacy", "admin", Decimal("0.01"))
    await store.settle(key, Decimal("0.01"))
    before = dict(await store.one("SELECT * FROM ledger"))
    await store.close()
    await store.open()
    assert dict(await store.one("SELECT * FROM ledger")) == before
    models = (await store.usage())["token_usage"]["today"]["by_model"]
    assert models[0]["model_lane"] == "unknown"
    assert models[0]["charged_amount"] == 10000


async def test_sdk_search_v1_isolated_tool_usage_and_source_gate(settings, store, sdk_pair):
    await enable_search(store)
    requests = []

    def handler(request):
        data = json.loads(request.content)
        requests.append((request.url.path, data))
        return httpx.Response(
            200,
            json=decision_response()
            if request.url.path.endswith("decisions")
            else grounded_response(),
        )

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    output = await gateway.search("请查一下公开事实", "subject")
    assert output == "已核实的公开事实。\n参考来源：原始来源"
    assert "<style>" not in output and "<a" not in output and "http" not in output
    path, data = requests[0]
    assert path == "/v1/interactions"
    assert data["store"] is False and data["stream"] is False
    assert data["model"] == "gemini-4-hydrogen"
    assert data["tools"] == [{"type": "google_search", "search_types": ["web_search"]}]
    assert data["generation_config"]["thinking_level"] == "medium"
    assert "previous_interaction_id" not in data and "history" not in data["input"]
    assert requests[1][0] == "/v1beta/decisions"
    row = await store.one("SELECT * FROM ledger WHERE lane='search' ORDER BY rowid LIMIT 1")
    assert row["amount"] == 0 and row["reserved"] == 0
    usage = await store.one("SELECT * FROM ledger_usage WHERE id=?", (row["id"],))
    assert usage["estimated_amount"] == 0
    assert usage["model_lane"] == "hydrogen" and usage["cached_tokens"] == 40
    status = await store.runtime.web_search.status()
    assert status["today"]["queries"] == 2 and status["today"]["tool_amount"] == 0
    assert (await store.one("SELECT amount FROM budget_totals"))[0] == 0


async def test_lookup_is_not_bili_without_bvid(settings, store, message, sdk_pair):
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(lambda r: httpx.Response(200, json=decision_response("bilibili_watch"))),
    )
    route = await gateway.screen(
        {"current_message": "请查今天的科技新闻"}, "s", purpose="message", message=message
    )
    assert route == "chat" and route.need_external_lookup


@pytest.mark.parametrize("reason", ["disabled", "limit", "monthly_unconfigured"])
async def test_search_blocked_locally_has_zero_provider_calls(settings, store, sdk_pair, reason):
    if reason == "limit":
        await enable_search(store, daily_limit=0)
    elif reason == "monthly_unconfigured":
        await enable_search(store, monthly_google_limit=0)
    calls = []
    gateway = GeminiGateway(settings, store, *sdk_pair(lambda r: calls.append(r)))
    with pytest.raises(Rejected):
        await gateway.search("测试", "s")
    assert not calls and not await store.all("SELECT * FROM ledger")


async def test_search_unknown_fee_keeps_unbilled_and_attribution(settings, store, sdk_pair):
    await enable_search(store)
    response = grounded_response()
    del response["usage"]["grounding_tool_count"]
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(
            lambda r: httpx.Response(
                200, json=decision_response() if r.url.path.endswith("decisions") else response
            )
        ),
    )
    await gateway.search("测试公开信息", "s")
    row = await store.one("SELECT * FROM ledger WHERE lane='search' ORDER BY rowid LIMIT 1")
    assert row["amount"] == 0 and row["reserved"] == 0 and row["status"] == "settled"
    assert (await store.one("SELECT model_lane FROM ledger_usage WHERE id=?", (row["id"],)))[
        0
    ] == "hydrogen"
    assert (await store.runtime.web_search.status())["today"]["known_calls"] == 0


async def test_search_is_excluded_from_billing_and_never_opens_circuit(settings, store, sdk_pair):
    await enable_search(store)
    response = grounded_response()
    response["usage"]["grounding_tool_count"][0]["count"] = 100
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(
            lambda r: httpx.Response(
                200, json=decision_response() if r.url.path.endswith("decisions") else response
            )
        ),
    )
    output = await gateway.search("测试公开信息", "s")
    assert "已核实的公开事实" in output
    assert not await store.one("SELECT 1 FROM meta WHERE key='budget_overrun'")
    row = await store.one("SELECT amount,reserved,status FROM ledger WHERE lane='search'")
    assert row["amount"] == 0 and row["reserved"] == 0 and row["status"] == "settled"
    assert (await store.one("SELECT amount FROM budget_totals"))[0] == 0


async def test_search_source_rejected_or_hot_disabled(settings, store, sdk_pair):
    await enable_search(store)
    calls = []

    async def handler(request):
        calls.append(request)
        await store.runtime.web_search.save(WebSearchConfig(enabled=False))
        return httpx.Response(200, json=grounded_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    with pytest.raises(Rejected, match="web_search_disabled"):
        await gateway.search("公开事实", "s")
    assert len(calls) == 1  # no subsequent Decisions/generation after hot disable
    assert (await store.one("SELECT model_lane FROM ledger_usage"))[0] == "hydrogen"


async def test_search_result_injection_is_not_delivered(settings, store, sdk_pair):
    await enable_search(store)
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith("decisions"):
            decision = decision_response()
            decision["answers"]["is_prompt_injection"]["noul"] = 0.99
            return httpx.Response(200, json=decision)
        return httpx.Response(200, json=grounded_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    with pytest.raises(Rejected, match="prompt_injection"):
        await gateway.search("测试公开资料", "s")
    assert len(requests) == 2
    assert not await store.all("SELECT * FROM outbox")


async def test_search_cancellation_keeps_budget_and_never_retries(settings, store, sdk_pair):
    await enable_search(store)
    arrived, cancel = asyncio.Event(), asyncio.Event()
    calls = []

    async def handler(request):
        calls.append(request)
        arrived.set()
        await asyncio.Event().wait()

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    task = asyncio.create_task(gateway.search("公开资料", "s", cancel_event=cancel))
    await asyncio.wait_for(arrived.wait(), 3)
    cancel.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    row = await store.one("SELECT amount,reserved,status FROM ledger")
    assert row[0] == 0 and row[1] == 0 and row[2] == "unknown"
    assert len(calls) == 1
    assert (await store.one("SELECT model_lane FROM ledger_usage"))[0] == "hydrogen"


async def test_search_daily_limit_atomic_and_survives_restart(store):
    await enable_search(store, daily_limit=1)
    results = await asyncio.gather(
        store.runtime.web_search.claim(), store.runtime.web_search.claim(), return_exceptions=True
    )
    assert sum(isinstance(r, str) for r in results) == 1
    assert sum(isinstance(r, Rejected) for r in results) == 1
    await store.close()
    await store.open()
    with pytest.raises(Rejected, match="search_daily_limit"):
        await store.runtime.web_search.claim()
    assert not await store.all("SELECT * FROM ledger")


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/",
        "https://169.254.169.254/",
        "file:///tmp/x",
        "javascript:alert(1)",
        "https://localhost/",
        "https://a.example:8080/",
        "https://x:y@a.example/",
        "https://a.example/\nx",
    ],
)
def test_no_local_or_unsafe_search_citations(url):
    assert public_url(url) is None


@pytest.mark.parametrize(
    "question",
    [
        "查一下password=privatevalue",
        "搜索 sk-abcdefghijklmnopqrst",
    ],
)
def test_queries_do_not_export_private_identifiers(settings, message, question):
    with pytest.raises(Rejected):
        search_query(message.model_copy(update={"text": question}), settings)


def test_query_original_only_and_no_non_google_tools(settings, message):
    m = message.model_copy(
        update={"command_text": "/搜索 公开问题", "text": "PRIVATE_FORWARD_SECRET"}
    )
    assert search_query(m, settings) == "公开问题"
    response = grounded_response()
    response["steps"].insert(0, {"type": "function_call", "name": "delete_file"})
    with pytest.raises(Rejected, match="search_unexpected_tool"):
        parse_result(response)
    assert query_count(SimpleNamespace(usage=None)) is None


async def test_service_search_never_exports_or_learns_private_memory(settings, store, message):
    from hakimi.service import Service

    class SearchGateway(FakeGateway):
        async def search(self, query, subject, **kwargs):
            self.calls.append(("search", query, subject))
            return "今天的公开新闻事实"

        async def generate(self, payload, subject, **kwargs):
            await super().generate(payload, subject, **kwargs)
            assert payload["web_search_status"] == "grounded"
            assert payload["web_search_findings"] == "今天的公开新闻事实"
            return action(bubbles=["今天的公开新闻是这个，你之前提的问题也能接上啦~"])

    ai, sender = SearchGateway(), ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    try:
        await enable_search(store)
        await svc.sessions.reply(
            message,
            "OWNER_PRIVATE_MEMORY",
            "seed",
            expected_epoch=(await svc.sessions.snapshot(message))["epoch"],
        )
        m = message.model_copy(
            update={"text": "/搜索 今天的科技新闻", "command_text": "/搜索 今天的科技新闻"}
        )
        assert await svc.accept(m) == "accepted"
        job = await store.claim()
        await svc.process(job)
        assert [c[1] for c in ai.calls if c[0] == "search"] == ["今天的科技新闻"]
        assert any(c[0] == "generate" for c in ai.calls)
        assert await svc.deliver_once()
        assert "今天的公开新闻" in sender.sent[0][0]["data"]["text"]
        snapshot = json.dumps(await svc.sessions.snapshot(m), ensure_ascii=False)
        assert "OWNER_PRIVATE_MEMORY" in snapshot and "今天的公开新闻" in snapshot
        assert "检索资料不进入记忆" not in snapshot
        assert all("OWNER_PRIVATE_MEMORY" not in c[1] for c in ai.calls if c[0] == "search")
    finally:
        await svc.stop()


async def test_search_off_passes_status_to_normal_model(settings, store, message):
    from hakimi.service import Service

    ai, sender = FakeGateway(), FakeSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    try:
        m = message.model_copy(update={"text": "/搜索 今天的新闻"})
        assert await svc.accept(m) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert sender.sent[0][1] == "这是基于已有信息的简短回复。"
        generated = next(c[1] for c in ai.calls if c[0] == "generate")
        assert generated["web_search_status"] == "disabled"
        assert "web_search_findings" not in generated
        assert any(c[0] == "generate" for c in ai.calls)
    finally:
        await svc.stop()


def test_natural_search_cue_concrete_subject_and_model_query_sanitizer(settings, message):
    assert natural_search_cue("这是真的，我刚刚查过了，不信你自己查")
    assert natural_search_cue("帮我搜一下英伟达最新股价")
    assert natural_search_cue("上网查一下 Python 3.14 发布日期")
    assert not natural_search_cue("今天过来查岗啦")
    assert not natural_search_cue("查水表了快开门")
    assert not natural_search_cue("你没有web search吗")

    assert not has_concrete_search_subject("这是真的，我刚刚查过了，不信你自己查")
    assert not has_concrete_search_subject("真的假的帮我搜一下")
    assert has_concrete_search_subject("帮我查一下英伟达最新股价")

    cleaned = sanitize_model_search_query(
        f"[role=OWNER] 英伟达股价 https://evil.example {settings.owner_qq_id} 最新消息",
        message,
        settings,
    )
    assert cleaned == "英伟达股价 最新消息"
    with pytest.raises(Rejected):
        sanitize_model_search_query("password=supersecret123", message, settings)
    with pytest.raises(Rejected):
        sanitize_model_search_query("sk-abcdefghijklmnopqrst", message, settings)


async def test_decision_gate_lowers_lookup_threshold_only_for_addressed_natural_cue(
    settings, store, sdk_pair, message
):
    from tests.test_social_integration import group

    def handler(request):
        resp = decision_response()
        resp["answers"]["should_reply"].update(noul=0.9, confidence=0.95)
        resp["answers"]["need_external_lookup"].update(noul=0.42, confidence=0.58)
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    normal = group(message, text="今天聊点啥", addressed=True, attention_reason="mention")
    res_normal = await gate.screen(
        {"current_message": normal.text}, "s", purpose="message", message=normal
    )
    assert res_normal.need_external_lookup is False

    cue_addressed = group(
        message,
        text="帮我查一下英伟达最新股价",
        command_text="帮我查一下英伟达最新股价",
        addressed=True,
        attention_reason="mention",
    )
    res_cue = await gate.screen(
        {"current_message": cue_addressed.text}, "s", purpose="message", message=cue_addressed
    )
    assert res_cue.need_external_lookup is True

    cue_unmentioned = group(
        message,
        text="帮我查一下英伟达最新股价",
        command_text="帮我查一下英伟达最新股价",
        addressed=False,
    )
    res_unmentioned = await gate.screen(
        {"current_message": cue_unmentioned.text}, "s", purpose="message", message=cue_unmentioned
    )
    assert res_unmentioned.need_external_lookup is False


async def test_model_initiated_search_for_context_backref_is_bounded_and_scrubbed(
    settings, store, message
):
    from hakimi.bot_server import assemble_chat_input
    from hakimi.outbound_actions import ACTION_INSTRUCTION, CHAT_RESPONSE_FORMAT, ChatActions
    from hakimi.routing import RouteDecision
    from hakimi.service import Service
    from tests.test_social_integration import group

    assert "search_query" in CHAT_RESPONSE_FORMAT["required"]
    assert set(CHAT_RESPONSE_FORMAT["required"]) == set(ChatActions.model_fields)
    assert "search_query" not in ACTION_INSTRUCTION

    class TwoPassSearchGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.gen_payloads = []
            self.search_queries = []

        async def screen(self, payload, subject, **kwargs):
            await super().screen(payload, subject, **kwargs)
            if kwargs.get("purpose") == "message":
                # Even if decision gate fires need_external_lookup=True on a bare back-reference,
                # pre-turn blind search must defer to the model so it crafts a real topic query.
                return RouteDecision("chat", tier=1, need_external_lookup=True)
            return "chat"

        async def search(self, query, subject, **kwargs):
            self.search_queries.append(query)
            return "核实结果：英伟达当日股价确有剧烈波动。"

        async def generate(self, payload, subject, **kwargs):
            snap = dict(payload)
            self.gen_payloads.append((snap, kwargs.get("route")))
            await super().generate(snap, subject, **kwargs)
            if len(self.gen_payloads) == 1:
                assert snap.get("web_search_available") is True
                assert snap.get("web_search_requested") is True
                wire = assemble_chat_input(snap, kwargs["message"], settings)
                assert "web_search_hint" in wire
                return action(bubbles=[""], search_query="英伟达 今日股价 最新消息")
            assert "web_search_available" not in snap
            assert snap.get("web_search_status") == "grounded"
            assert "英伟达当日股价" in snap.get("web_search_findings", "")
            # Attempting a second search_query on pass #2 MUST be ignored (max 1 search/turn).
            return action(
                bubbles=["刚查了下还真是，今天股价确实上蹿下跳了"],
                search_query="不要死循环再搜一次",
            )

    ai, sender = TwoPassSearchGateway(), ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    try:
        await enable_search(store)
        grp_msg = group(
            message,
            event_id="backref-1",
            text="这是真的，我刚刚查过了，不信你自己查",
            command_text="这是真的，我刚刚查过了，不信你自己查",
            addressed=True,
            attention_reason="mention",
            mentioned_ids=[settings.onebot_self_id],
        )
        assert await svc.accept(grp_msg) == "accepted"
        await svc.process(await store.claim())
        assert ai.search_queries == ["英伟达 今日股价 最新消息"]
        assert len(ai.gen_payloads) == 2
        assert ai.gen_payloads[1][1].tier == 2
        assert await svc.deliver_once()
        assert len(sender.sent) == 1
        assert "上蹿下跳" in sender.sent[0][0]["data"]["text"]
    finally:
        await svc.stop()


async def test_model_initiated_search_blocks_secret_query_and_unmentioned_group(
    settings, store, message
):
    from hakimi.routing import RouteDecision
    from hakimi.service import Service
    from tests.test_social_integration import group

    class SecretAttemptGateway(FakeGateway):
        def __init__(self):
            super().__init__()
            self.gen_payloads = []
            self.search_queries = []

        async def screen(self, payload, subject, **kwargs):
            await super().screen(payload, subject, **kwargs)
            return RouteDecision("chat", tier=1, need_external_lookup=False)

        async def search(self, query, subject, **kwargs):
            self.search_queries.append(query)
            return "should_not_run"

        async def generate(self, payload, subject, **kwargs):
            snap = dict(payload)
            self.gen_payloads.append(snap)
            await super().generate(snap, subject, **kwargs)
            if len(self.gen_payloads) == 1:
                return action(bubbles=[""], search_query="password=supersecretvalue")
            assert snap.get("web_search_status") == "skipped_query"
            return action(bubbles=["这个搜不了呀，直接按我知道的说吧"])

    ai, sender = SecretAttemptGateway(), ActionSender()
    svc = Service(settings, store, ai, FakeBili(), sender)
    try:
        await enable_search(store)
        # 1. Secret query proposed by model is rejected before hitting search provider
        m = message.model_copy(
            update={"event_id": "sec-1", "text": "帮我查查", "command_text": "帮我查查"}
        )
        assert await svc.accept(m) == "accepted"
        await svc.process(await store.claim())
        assert ai.search_queries == []
        assert len(ai.gen_payloads) == 2
        assert await svc.deliver_once()
        assert "搜不了" in sender.sent[-1][0]["data"]["text"]

        # 2. Unmentioned group turn never exposes web_search_available or runs model search
        settings.group_policies["onebot:40000"] = "smart_reply"
        ai.gen_payloads.clear()
        ai.search_queries.clear()
        unmentioned = group(
            message,
            event_id="unm-1",
            text="随便聊聊",
            command_text="随便聊聊",
            addressed=False,
        )
        assert await svc.accept(unmentioned) == "accepted"
        await svc.process(await store.claim())
        assert ai.search_queries == []
        assert len(ai.gen_payloads) == 1
        assert "web_search_available" not in ai.gen_payloads[0]
    finally:
        await svc.stop()


async def test_google_search_503_retries_once_and_records_trace(settings, store, sdk_pair):
    await enable_search(store)
    interaction_calls = 0

    def handler(request):
        nonlocal interaction_calls
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        interaction_calls += 1
        if interaction_calls == 1:
            return httpx.Response(503, json={"error": {"code": 503, "status": "UNAVAILABLE"}})
        return httpx.Response(200, json=grounded_response())

    gateway = GeminiGateway(settings, store, *sdk_pair(handler))
    gateway._search_retry_backoff = 0
    trace = {}
    output = await gateway.search("英伟达最新新闻", "subject", search_trace=trace)
    assert "已核实的公开事实" in output
    assert interaction_calls == 2
    assert trace["provider"] == "google"
    assert trace["retried"] is True
    assert len(trace["provider_diagnostics"]) == 1
    assert trace["provider_diagnostics"][0]["stage"] == "search"
    assert trace["provider_diagnostics"][0]["http_status"] == 503
    rows = await store.all("SELECT status, provider, query_count FROM web_search_attempts")
    assert [tuple(r) for r in rows] == [("completed", "google", 2)]


async def test_google_search_persistent_503_falls_back_to_deepseek_and_screens(
    settings, store, sdk_pair
):
    await enable_search(store)
    await store.runtime.web_search.save_deepseek(
        api_key="sk-deepseek-fallback-test-key",
        base_url="https://api.deepseek.com",
        model="deepseek-flash",
    )
    google_calls = 0
    decision_calls = 0
    ds_calls = []

    def gemini_handler(request):
        nonlocal google_calls, decision_calls
        if request.url.path.endswith("decisions"):
            decision_calls += 1
            return httpx.Response(200, json=decision_response())
        google_calls += 1
        return httpx.Response(503, json={"error": {"code": 503, "status": "UNAVAILABLE"}})

    def ds_handler(request):
        ds_calls.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "title": "权威科技媒体",
                        "url": "https://example.org/nvda",
                        "summary": "DeepSeek 兜底查到的英伟达最新公开动态。",
                        "rerank_score": 2.0,
                    }
                ]
            },
        )

    gateway = GeminiGateway(settings, store, *sdk_pair(gemini_handler))
    gateway._search_retry_backoff = 0
    gateway._deepseek_transport = httpx.MockTransport(ds_handler)
    trace = {}
    output = await gateway.search("英伟达最新新闻", "subject", search_trace=trace)
    assert "DeepSeek 兜底查到的英伟达最新公开动态" in output
    assert google_calls == 2
    assert ds_calls == ["/v1/tools/search"]
    assert decision_calls == 1
    assert trace["provider"] == "deepseek"
    assert trace["fallback"] == "deepseek"
    assert trace["retried"] is True
    assert len(trace["provider_diagnostics"]) == 2
    assert trace["provider_diagnostics"][-1]["fallback"] == "succeeded"
    rows = await store.all("SELECT status, provider, query_count FROM web_search_attempts")
    assert [tuple(r) for r in rows] == [("completed", "deepseek", 1)]
    st = await store.runtime.web_search.status()
    assert st["monthly"]["google_queries"] == 0
    assert st["monthly"]["deepseek_attempts"] == 1


async def test_google_search_503_deepseek_network_error_preserves_diagnostics(
    settings, store, sdk_pair
):
    await enable_search(store)
    await store.runtime.web_search.save_deepseek(
        api_key="sk-deepseek-fallback-test-key",
        base_url="https://api.deepseek.com",
        model="deepseek-flash",
    )

    def gemini_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(503, json={"error": {"code": 503, "status": "UNAVAILABLE"}})

    def ds_handler(request):
        raise httpx.ConnectTimeout("simulated timeout")

    gateway = GeminiGateway(settings, store, *sdk_pair(gemini_handler))
    gateway._search_retry_backoff = 0
    gateway._deepseek_transport = httpx.MockTransport(ds_handler)
    trace = {}
    with pytest.raises(Rejected, match="gemini_unavailable") as exc_info:
        await gateway.search("英伟达最新新闻", "subject", search_trace=trace)
    diags = exc_info.value.provider_diagnostics
    assert len(diags) == 2
    assert all(d["stage"] == "search" and d["http_status"] == 503 for d in diags)
    assert diags[-1]["fallback"] == "failed"
    assert trace["provider"] == "google" and trace["retried"] is True
    rows = await store.all("SELECT status, provider FROM web_search_attempts")
    assert [tuple(r) for r in rows] == [("unknown", "google")]


async def test_decision_audit_preserves_search_status_and_merges_diagnostics(
    settings, store, sdk_pair, message
):
    from hakimi.service import Service
    from tests.test_social_integration import group

    await enable_search(store)

    # Case 1: Google search 503 x2 (no DeepSeek key) + main generation succeeds on fallback/normal
    def handler_fail_search(request):
        if request.url.path.endswith("decisions"):
            resp = decision_response()
            resp["answers"]["need_external_lookup"].update(noul=0.72, confidence=0.85)
            return httpx.Response(200, json=resp)
        body = json.loads(request.content)
        if body.get("tools"):
            return httpx.Response(503, json={"error": {"code": 503, "status": "UNAVAILABLE"}})
        return httpx.Response(200, json=interaction_response())

    ai = GeminiGateway(settings, store, *sdk_pair(handler_fail_search))
    ai._search_retry_backoff = 0
    svc = Service(settings, store, ai, FakeBili(), ActionSender())
    try:
        m1 = group(
            message,
            event_id="audit-ws-1",
            text="帮我查一下英伟达最新财报",
            command_text="帮我查一下英伟达最新财报",
            addressed=True,
            attention_reason="mention",
            mentioned_ids=[settings.onebot_self_id],
        )
        assert await svc.accept(m1) == "accepted"
        await svc.process(await store.claim())
        audit = (await store.runtime.filters.recent_audit(1))[0]
        assert audit["web_search"] == {
            "status": "unavailable",
            "trigger": "gate",
            "provider": "google",
            "retried": True,
            "reason": "gemini_unavailable",
        }
        # Search 503 diagnostics survive even after main generation records thinking_level
        assert audit.get("thinking_level")
        search_diags = [d for d in audit["provider_diagnostics"] if d["stage"] == "search"]
        assert len(search_diags) == 2
        assert search_diags[-1]["fallback"] == "disabled"
        # Appending generation diagnostics later must preserve earlier search diagnostics
        await store.runtime.filters.record_generation(
            m1.event_id,  # non-matching id is a no-op
            web_search={"status": "grounded", "query": "MUST_NOT_LEAK"},
        )
        # Direct unit check that untrusted fields in web_search audit are stripped
        last_row = await store.one("SELECT id FROM decision_audit ORDER BY at DESC LIMIT 1")
        await store.runtime.filters.record_generation(
            last_row["id"],
            diagnostics=[
                {
                    "stage": "generation",
                    "provider": "vertex_paid",
                    "model_lane": "hydrogen",
                    "cause": "http",
                    "http_status": 429,
                }
            ],
            web_search={"status": "grounded", "trigger": "explicit", "secret": "LEAK_TEXT"},
        )
        merged = (await store.runtime.filters.recent_audit(1))[0]
        assert "LEAK_TEXT" not in json.dumps(merged, ensure_ascii=False)
        assert [d["stage"] for d in merged["provider_diagnostics"]] == [
            "search",
            "search",
            "generation",
        ]
        assert merged["web_search"] == {"status": "grounded", "trigger": "explicit"}
    finally:
        await svc.stop()

    # Case 2: Google search 503 x2 rescued by DeepSeek fallback -> decision_audit shows both
    await store.runtime.web_search.save_deepseek(
        api_key="sk-deepseek-audit-test-key",
        base_url="https://api.deepseek.com",
        model="deepseek-flash",
    )

    def ds_ok_handler(request):
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "title": "公开快讯",
                        "url": "https://example.org/news",
                        "summary": "英伟达发布最新季报亮点。",
                        "rerank_score": 1.5,
                    }
                ]
            },
        )

    ai2 = GeminiGateway(settings, store, *sdk_pair(handler_fail_search))
    ai2._search_retry_backoff = 0
    ai2._deepseek_transport = httpx.MockTransport(ds_ok_handler)
    svc2 = Service(settings, store, ai2, FakeBili(), ActionSender())
    try:
        m2 = message.model_copy(
            update={
                "event_id": "audit-ws-2",
                "text": "/search 英伟达最新季报",
                "command_text": "/search 英伟达最新季报",
            }
        )
        assert await svc2.accept(m2) == "accepted"
        await svc2.process(await store.claim())
        audit2 = (await store.runtime.filters.recent_audit(1))[0]
        assert audit2["web_search"] == {
            "status": "grounded",
            "trigger": "explicit",
            "provider": "deepseek",
            "fallback": "deepseek",
            "retried": True,
        }
        assert len(audit2["provider_diagnostics"]) == 2
        assert audit2["provider_diagnostics"][-1]["fallback"] == "succeeded"
    finally:
        await svc2.stop()
