"""Natural responses keep the authentication, source gates and ledger boundaries intact."""

import hashlib

import httpx
import pytest

from hakimi.bot_server import assemble_chat_input, prompt_context
from hakimi.context_budget import estimate_tokens
from hakimi.gemini import GeminiGateway
from hakimi.message_context import MessageContextResolver
from hakimi.outbound_actions import parse_actions
from hakimi.output_links import filter_reply_links
from hakimi.persona import OWNER_PERSONA_PROMPT, PUBLIC_PERSONA_PROMPT, SYSTEM_INSTRUCTION
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.reply_style import public_reply
from hakimi.routing import RouteDecision
from hakimi.security import Rejected, safe_output
from hakimi.service import Service
from hakimi.session_manager import SessionManager
from hakimi.web_search import WebSearchConfig, explicit_search, parse_result, search_query
from tests.fakes import FakeBili, FakeGateway, FakeSender
from tests.test_controls_search_v110 import grounded_response
from tests.test_dual_memory_v100 import facts, group, seed
from tests.test_sdk_contract import decision_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_streaming_v0100 import action


@pytest.mark.parametrize(
    "value,digest",
    [
        (SYSTEM_INSTRUCTION, "6f1ef928e5fac56fb64c4dcb191a1b4fb2d2820933f3f15e9954e21053b352db"),
        (PUBLIC_PERSONA_PROMPT, "bdd7e30a9429a31012105dc31991cba98ec20a2d3ee813b3f83d9f5e94ae0230"),
        (OWNER_PERSONA_PROMPT, "6bd4015e6a5914be4c4ab421b390ff924a1bd16a177a11270f2c2ef9f1ded77d"),
    ],
)
def test_frozen_personas_include_exact_boundary_newlines(value, digest):
    assert hashlib.sha256(value.encode()).hexdigest() == digest


@pytest.mark.parametrize("private", [True, False])
def test_search_rules_are_static_but_findings_only_enter_c(settings, message, private):
    msg = message if private else group(message)
    prefix = StaticPrefixes().get(prompt_context(msg, settings), settings.onebot_self_id)
    assert 4200 <= estimate_tokens(prefix) <= 5200
    assert "web_search_findings" in prefix
    payload = {
        "current_message": "接着聊",
        "recent_messages": [{"seq": 1, "role": "user", "text": "原历史"}],
        "web_search_findings": "UNIQUE_PUBLIC_FINDING",
        "context_status": [{"event_id": "1", "states": ["reply_unavailable"]}],
    }
    content = assemble_chat_input(payload, msg, settings)
    assert content.index("UNIQUE_PUBLIC_FINDING") > content.index("<transient_turn_context>")
    assert "UNIQUE_PUBLIC_FINDING" not in prefix
    assert "reply_unavailable" in content
    assert "UNIQUE_PUBLIC_FINDING" not in assemble_chat_input(
        payload, msg, settings, sandbox_mode=True
    )


@pytest.mark.parametrize(
    "text,expected",
    [
        ("/search Python 3.14", True),
        ("/搜索 最新发布", True),
        ("帮我查下这个报错", False),
        ("搜索这两个字", False),
        ("联网聊聊", False),
    ],
)
def test_only_slash_search_is_explicit(text, expected):
    assert explicit_search(text) is expected


def test_query_strips_accounts_but_preserves_public_versions_and_links(settings, message):
    text = "/搜索 20000 10000 30000 40000 Python 3.14 https://docs.python.org/3/"
    query = search_query(message.model_copy(update={"text": text}), settings)
    assert "Python 3.14 https://docs.python.org/3/" in query
    assert not any(account in query for account in ("20000", "10000", "30000", "40000"))


def test_search_facts_need_no_citation_and_never_render_redirects(settings):
    response = grounded_response()
    part = response["steps"][-1]["content"][0]
    part.pop("annotations")
    part["text"] = (
        "官方已发布新版本。[发布公告](https://vertexaisearch.cloud.google.com/grounding-api-redirect/long)"
    )
    rendered = parse_result(response).render(settings)
    assert rendered == "官方已发布新版本。发布公告"
    assert "http" not in rendered and "Google 搜索建议" not in rendered


@pytest.mark.parametrize(
    "score,addressed,expected", [(0.64, True, False), (0.65, True, True), (0.99, False, False)]
)
async def test_lookup_threshold_and_group_addressing(
    settings, store, message, sdk_pair, score, addressed, expected
):
    response = decision_response()
    response["answers"]["need_external_lookup"]["noul"] = score
    gateway = GeminiGateway(
        settings, store, *sdk_pair(lambda r: httpx.Response(200, json=response))
    )
    msg = group(message).model_copy(update={"addressed": addressed})
    try:
        result = await gateway.screen(
            {"current_message": "今天有什么发布"}, "s", purpose="message", group=True, message=msg
        )
        assert result.need_external_lookup is expected
    finally:
        await gateway.close()


@pytest.mark.parametrize(
    "reason",
    [
        "search_daily_limit",
        "search_no_grounding",
        "prompt_injection",
        "gemini_unavailable",
        "empty",
    ],
)
async def test_unavailable_search_continues_main_chat(settings, store, message, reason):
    class AI(FakeGateway):
        async def search(self, *args, **kwargs):
            self.calls.append(("search", args[0]))
            if reason == "empty":
                return ""
            raise Rejected(reason)

    ai = AI()
    svc = Service(settings, store, ai, FakeBili(), FakeSender())
    await store.runtime.web_search.save(WebSearchConfig(enabled=True))
    try:
        msg = message.model_copy(update={"text": "/搜索 今日新闻"})
        await svc.accept(msg)
        await svc.process(await store.claim())
        payload = next(c[1] for c in ai.calls if c[0] == "generate")
        assert payload["web_search_status"] == "unavailable"
        assert "web_search_findings" not in payload
        assert await svc.deliver_once()
        assert svc.sender.sent[0][1] == "这是基于已有信息的简短回复。"
    finally:
        await svc.stop()


async def test_search_provider_failure_does_not_open_chat_circuit(settings, store, sdk_pair):
    await store.runtime.web_search.save(WebSearchConfig(enabled=True))
    gateway = GeminiGateway(
        settings,
        store,
        *sdk_pair(
            lambda r: httpx.Response(503, json={"error": {"message": "offline", "code": 503}})
        ),
    )
    try:
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gateway.search("公开问题", "s")
        assert gateway.blocked_until == 0
        row = await store.one("SELECT status,amount,reserved FROM ledger WHERE lane='search'")
        assert row["status"] == "unknown" and row["amount"] == row["reserved"] > 0
    finally:
        await gateway.close()


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("prompt_injection", None),
        ("content_blocked", None),
        ("owner_private_only", None),
        ("gemini_unavailable", "卡了一下"),
        ("gemini_circuit_open", "卡了一下"),
        ("generation_incomplete", "卡了一下"),
        ("budget_exhausted", "额度用完"),
        ("budget_circuit_open", "额度用完"),
    ],
)
async def test_private_rejections_are_specific_or_silent(
    settings, store, message, reason, expected
):
    class AI(FakeGateway):
        async def screen(self, *args, **kwargs):
            raise Rejected(reason)

    svc = Service(settings, store, AI(), FakeBili(), FakeSender())
    try:
        await svc.accept(message)
        await svc.process(await store.claim())
        sent = await svc.deliver_once()
        assert sent is bool(expected)
        if expected:
            assert expected in svc.sender.sent[0][1]
    finally:
        await svc.stop()


async def test_bili_intent_without_id_is_normal_chat(settings, store, message):
    ai = FakeGateway()
    ai.intent = RouteDecision("bilibili_watch")
    svc = Service(settings, store, ai, FakeBili(), FakeSender())
    try:
        await svc.accept(message)
        await svc.process(await store.claim())
        assert any(c[0] == "generate" for c in ai.calls)
    finally:
        await svc.stop()


@pytest.mark.parametrize("tier", [1, 2, 3])
def test_full_public_answer_folds_at_every_tier(tier):
    text = "先把变量的类型理清楚，再看函数返回值。" * 12
    parsed, folded = parse_actions(action(bubbles=[text]), public=True, tier=tier)
    assert folded == text == parsed.bubbles[0]
    shorter = "这个比较符保留着用：`a < b && c > d`。然后检查返回值。最后再试一次~"
    parsed, folded = parse_actions(action(bubbles=[shorter]), public=True, tier=tier)
    assert folded is None and parsed.bubbles == [shorter]


def test_safe_output_keeps_code_and_natural_truncation():
    code = "vector<int> x; if (a < b) return a -> b;"
    assert safe_output(code, []) == code
    text = "第一句完整。\n下一句也完整。" + "这句还有很多细节" * 10
    assert safe_output(text, [], limit=24) == "第一句完整。\n下一句也完整。…"
    assert (
        public_reply("好~~~ 下次~~ `~~~` https://github.com/user/~~~")
        == "好~ 下次~~ `~~~` https://github.com/user/~~~"
    )
    assert "@everyone" not in safe_output("@everyone [CQ:at,qq=all]", [])


@pytest.mark.parametrize("public", [False, True])
def test_links_are_exact_domain_scoped_without_placeholders(public):
    good = "https://docs.python.org/3/ https://github.com/team/project https://www.bilibili.com/video/BV1xx411c7mD"
    unknown = "https://example.org/notes"
    text = good + " " + unknown + " https://github.com.evil.example/phish"
    result = filter_reply_links(text, public=public)
    assert good in result
    assert (unknown in result) is (not public)
    assert ("evil.example" in result) is (not public)
    assert "[外部链接" not in result
    assert "vertexaisearch" not in filter_reply_links(
        "https://vertexaisearch.cloud.google.com/grounding-api-redirect/x", public=public
    )
    parsed, _ = parse_actions(action(bubbles=[text]), public=public)
    assert unknown in parsed.bubbles[0] if not public else unknown not in parsed.bubbles[0]


async def test_reference_failures_and_truncation_only_in_metadata(settings, message):
    resolver = MessageContextResolver(settings, None)
    msg = message.model_copy(update={"reply_ids": ["99"], "forward_ids": ["fwd"]})
    resolved = await resolver.resolve(msg)
    assert resolved.text == message.text
    assert set(resolved.context_status) == {"reply_unavailable", "forward_unavailable"}

    async def lookup(current, ref):
        return current.model_copy(update={"event_id": ref, "text": "原文" * 3000, "reply_ids": []})

    resolved = await MessageContextResolver(settings, None, lookup).resolve(
        message.model_copy(update={"text": "本轮" * 2000, "reply_ids": ["99"]})
    )
    assert len(resolved.text) == 8000 and resolved.context_status == ["truncated"]
    assert "[上下文截断]" not in resolved.text


@pytest.mark.parametrize(
    "quote,expected",
    [
        ("哈哈，我喜欢天文", True),
        ("开玩笑，我喜欢天文", False),
        ("假的，我喜欢天文", False),
        ("据说我喜欢天文", False),
    ],
)
async def test_profile_laughter_is_not_a_joke_marker(settings, store, message, quote, expected):
    sessions, msg = SessionManager(settings, store), group(message)
    await seed(sessions, msg, text=quote)
    await sessions.dual.snapshot_cold(msg, force=True)
    job = await sessions.dual.claim(msg)
    assert await sessions.dual.commit(msg, job, facts(quote=quote))
    view = await sessions.dual.view(msg)
    assert any(e["kind"] == "profile" for e in view["entries"]) is expected
