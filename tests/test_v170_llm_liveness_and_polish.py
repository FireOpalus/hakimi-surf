import hashlib
import json
import re
from pathlib import Path

import httpx
import pytest

import hakimi
from hakimi.adapters import Sender
from hakimi.bot_server import prompt_context
from hakimi.context_budget import estimate_tokens
from hakimi.gemini import GeminiGateway, ModelImage
from hakimi.outbound_actions import ACTION_INSTRUCTION
from hakimi.persona import OWNER_PERSONA_PROMPT, PUBLIC_PERSONA_PROMPT, SYSTEM_INSTRUCTION
from hakimi.prefix_runtime import StaticPrefixes
from hakimi.reply_style import trim_complete_text
from hakimi.service import Service
from hakimi.web_search import WebSearchConfig
from tests.fakes import FakeBili, FakeGateway
from tests.test_controls_search_v110 import grounded_response
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_social_integration import Wire, group, process


async def test_help_command_documents_all_new_commands(settings, store, message):
    ai = FakeGateway()
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, ai, FakeBili(), Sender(settings, http, Wire()))
        try:
            for cmd in ("/哈基米", "/哈基米 帮助"):
                reply = await svc.command(message.model_copy(update={"text": cmd}))
                assert reply is not None
                for expected in (
                    "/new-session",
                    "/search",
                    "/搜索",
                    "/watch-video",
                    "/哈基米 记忆 开",
                    "/存表情",
                    "/bot-interact",
                    "/deep",
                    "/set-sign",
                    "/mute",
                    "/unmute",
                    "/mute-approve",
                    "/mute-reject",
                    "/哈基米 订阅",
                    "/哈基米 报告",
                    "/哈基米 状态",
                ):
                    assert expected in reply
        finally:
            await svc.stop()


def test_trim_complete_text_respects_sentence_and_clause_boundaries():
    short = "正好一句话。"
    assert trim_complete_text(short, 20) == short

    text = "第一句很完整，也很自然。第二句开始展开讲了一大堆关于战术配合的细节内容。"
    trimmed = trim_complete_text(text, 25)
    assert trimmed == "第一句很完整，也很自然。"

    clause_text = "前戏铺垫先讲到这里，后面还有很长很长的复盘展开"
    clause_trimmed = trim_complete_text(clause_text, 20)
    assert clause_trimmed == "前戏铺垫先讲到这里…"


async def test_patrol_share_is_owned_by_world_rule_and_never_defaults_to_group(settings, store):
    from hakimi.life_rules import milestone_sharing
    from hakimi.world_state import MilestoneEvent

    event = MilestoneEvent(
        id="watch",
        source_module="bilibili_surf",
        event_type="bili_video_watched",
        created_at=1,
        importance="high",
        payload={"title": "一次精彩复盘"},
    )
    settings.owner_qq_id = ""
    assert milestone_sharing(settings).match(event) is None
    settings.owner_qq_id = "20000"
    reaction = milestone_sharing(settings).match(event)
    assert reaction.action == "qq.speak" and reaction.recipient == "owner:20000"
    assert (
        milestone_sharing(settings).match(event.model_copy(update={"importance": "normal"})) is None
    )


@pytest.mark.parametrize(
    "value,digest",
    [
        (SYSTEM_INSTRUCTION, "6f1ef928e5fac56fb64c4dcb191a1b4fb2d2820933f3f15e9954e21053b352db"),
        (
            PUBLIC_PERSONA_PROMPT,
            "bd0516cf2b0b9d20f35b6e411ee0be528be5df6549bfb808f8fbe3af6b16e3b5",
        ),
        (
            OWNER_PERSONA_PROMPT,
            "6bd4015e6a5914be4c4ab421b390ff924a1bd16a177a11270f2c2ef9f1ded77d",
        ),
    ],
)
def test_llm_liveness_preserves_frozen_personas_bounds_and_distillation(
    settings, message, value, digest
):
    assert hashlib.sha256(value.encode()).hexdigest() == digest

    assert "？" in ACTION_INSTRUCTION and "bubbles" in ACTION_INSTRUCTION
    pool = StaticPrefixes()
    for msg in (message, group(message)):
        ctx = prompt_context(msg, settings)
        assert f"[role={ctx.role}]" in ctx.instruction
        assert "客服腔" in ctx.instruction and "松弛感" in ctx.compact_instruction
        prefix = pool.get(ctx, settings.onebot_self_id)
        tokens = estimate_tokens(prefix)
        assert 4500 <= tokens <= 5200
        assert "反 AI 腔" in prefix
        assert "真实语料蒸馏" in prefix
        assert "web_search_findings" in prefix
        assert "你想吐槽还是先聊点别的？" not in prefix
        assert "今天想聊游戏还是随便胡扯两句？" not in prefix


async def test_casual_image_and_recall_stay_low_while_search_and_long_text_upgrade(
    settings, store, message, sdk_pair
):
    settings.owner_qq_id = "20000"
    settings.onebot_mode = "ws_reverse"
    gen_requests = []

    def handler(request):
        body = json.loads(request.content)
        if request.url.path.endswith("interactions"):
            gen_requests.append(body)
            resp = grounded_response() if body.get("tools") else interaction_response()
            return httpx.Response(200, json=resp)
        resp = decision_response()
        resp["answers"]["need_memory_recall"] = {"type": "noul", "noul": 0.95, "confidence": 0.99}
        resp["answers"]["reasoning_tier"] = {
            "type": "choice",
            "choice": "tier_1_low",
            "confidence": 0.95,
            "probabilities": {
                "tier_1_low": 0.9,
                "tier_2_medium": 0.1,
                "tier_3_high": 0.0,
                "tier_4_carbon": 0.0,
            },
        }
        return httpx.Response(200, json=resp)

    await store.runtime.web_search.save(WebSearchConfig(enabled=True))
    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    async with httpx.AsyncClient() as http:
        svc = Service(settings, store, gate, FakeBili(), Sender(settings, http, Wire()))
        try:

            async def fake_message_images(*args, **kwargs):
                return [ModelImage(data="aW1n", mime_type="image/jpeg")], [{"image_number": 1}]

            svc.message_images = fake_message_images

            # 1) Casual DM with an image + need_memory_recall=0.95 stays at thinking_level="low"!
            await process(svc, message.model_copy(update={"event_id": "301", "text": "看这猫笑死"}))
            assert gen_requests[-1]["generation_config"]["thinking_level"] == "low"
            assert await svc.deliver_once()

            # 2) Long text (> 280 chars) upgrades to medium.
            await process(svc, message.model_copy(update={"event_id": "302", "text": "长" * 285}))
            assert gen_requests[-1]["generation_config"]["thinking_level"] == "medium"
            assert await svc.deliver_once()

            # 3) Explicit /search with grounded findings upgrades to medium.
            await process(
                svc, message.model_copy(update={"event_id": "303", "text": "/搜索 Python"})
            )
            assert gen_requests[-1]["generation_config"]["thinking_level"] == "medium"
        finally:
            await svc.stop()


def test_version_170_consistent_across_repo():
    root = Path(__file__).resolve().parent.parent
    assert hakimi.__version__ == "2.1.0"
    assert 'version = "2.1.0"' in (root / "pyproject.toml").read_text()
    lock_text = (root / "uv.lock").read_text()
    assert re.search(r'name = "hakimi-surf"\nversion = "2\.1\.0"', lock_text)
    admin_html = (root / "src/hakimi/admin_page.html").read_text()
    assert admin_html.count("v2.1.0") >= 2
    assert "**当前版本：v2.1.0**" in (root / "README.md").read_text()
