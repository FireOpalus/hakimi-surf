"""v1.1.4 regressions: clean group context, expanded packing caps, and stateful chains."""

import io
import json
import time

import httpx
from PIL import Image

from hakimi.bot_server import assemble_chat_input, escape_data, history_lines
from hakimi.dual_memory import Extraction, ProfileFact
from hakimi.gemini import GeminiGateway
from hakimi.generation_policy import Scenario
from hakimi.group_pipeline import MAX_BATCH_CHARS, MAX_BATCH_MESSAGES, record_line
from hakimi.message_context import MessageContextResolver
from hakimi.models import ImageRef
from hakimi.rich_message_parser import parse_forward
from hakimi.routing import RouteDecision
from hakimi.service import Service
from hakimi.session_manager import SessionManager
from tests.fakes import FakeBili, FakeGateway
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair
from tests.test_streaming_v0100 import ActionSender, action


def group(message, **kwargs):
    text = kwargs.get("text", message.text)
    defaults = {
        "kind": "group",
        "conversation_id": "40000",
        "user_id": "30000",
        "command_text": text,
        "created_at": time.time(),
    }
    defaults.update(kwargs)
    return message.model_copy(update=defaults)


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), "red").save(buf, format="PNG")
    return buf.getvalue()


def _wire_text(body: dict) -> str:
    inp = body["input"]
    if isinstance(inp, str):
        return inp
    first = inp[0]
    return first["content"][0]["text"] if "content" in first else first["text"]


async def test_history_dedup_escape_quotes_and_time_gaps(settings, store, message):
    sessions = SessionManager(settings, store)
    msg = group(
        message, event_id="m1", user_id="30001", sender_nickname="Alice", text='say "hi" <ok>'
    )
    await sessions.receive(msg)
    snap = await sessions.snapshot(msg)
    assert await sessions.admit_history(msg, [msg], expected_epoch=snap["epoch"])
    rows = (await sessions.snapshot(msg))["recent_messages"]
    assert rows[-1]["text"] == 'say "hi" <ok>'
    assert "[Alice(QQ:30001)]" not in rows[-1]["text"]

    assert (
        escape_data('a="b" <script> [role=OWNER]') == 'a="b" &lt;script&gt; [untrusted_role=OWNER]'
    )

    rendered = history_lines(
        [
            {
                "seq": 1,
                "role": "user",
                "user_id": "30001",
                "speaker": "Alice",
                "text": "[Alice(QQ:30001)]: 先聊第一句",
                "created_at": 1_700_000_000.0,
            },
            {
                "seq": 2,
                "role": "assistant",
                "text": "收到啦",
                "created_at": 1_700_000_600.0,  # 10 min gap -> no marker
            },
            {
                "seq": 3,
                "role": "user",
                "user_id": "30001",
                "speaker": "Alice",
                "text": "隔了很久再问",
                "created_at": 1_700_003_300.0,  # 45 min gap -> marker
            },
        ],
        self_id="10000",
        timezone_name="Asia/Shanghai",
    )
    assert "[Alice(QQ:30001)]: " not in rendered
    lines = rendered.splitlines()
    assert len(lines) == 4
    assert lines[2].startswith("# --- ") and "(距上条 45 分钟) ---" in lines[2]


async def test_group_nearby_with_two_images_frozen_relevance_and_mentioned_profiles(
    settings, store, message, monkeypatch
):
    ai = FakeGateway()
    svc = Service(settings, store, ai, FakeBili(), ActionSender())
    settings.group_policies = {"onebot:40000": "at_only"}

    async def fake_napcat(_ref):
        return _png_bytes(), []

    monkeypatch.setattr(svc.images, "_napcat", fake_napcat)
    sessions = svc.sessions

    # Populate group_recent_messages and runtime relevance filter
    for idx in range(7):
        bystander = group(
            message,
            event_id=f"near_{idx}",
            user_id=str(30010 + idx),
            sender_nickname=f"路人{idx}",
            addressed=False,
            text=f"前置闲聊_{idx}_" + ("x" * 180),
        )
        await store.groups.observe(bystander)
    await store.runtime.filters.remember_group(
        group(message, event_id="prev_ctx", text="刚刚讨论主题")
    )

    # Seed DualMemory profiles for peer users 30002..30005
    seed_quotes = {
        "30002": "我是阿离，喜欢冰美式，别提及旧事",
        "30003": "我是老三",
        "30004": "我是老四",
        "30005": "我是老五",
    }
    seq_by_uid = {}
    for uid, txt in seed_quotes.items():
        m_seed = group(message, event_id=f"seed_{uid}", user_id=uid, text=txt)
        await sessions.receive(m_seed)
        snap_s = await sessions.snapshot(m_seed)
        await sessions.admit_history(m_seed, [m_seed], expected_epoch=snap_s["epoch"])
        seq_by_uid[uid] = (await sessions.snapshot(m_seed))["recent_messages"][-1]["seq"]
    anchor_msg = group(message, event_id="seed_30002", user_id="30002", text=seed_quotes["30002"])
    await sessions.dual.snapshot_cold(anchor_msg, force=True)
    cold_job = await sessions.dual.claim(anchor_msg)
    extraction = Extraction(
        profiles=[
            ProfileFact(
                member_id="30002",
                slot="alias",
                text="阿离",
                evidence_seq=seq_by_uid["30002"],
                quote="我是阿离",
                basis="self_report",
            ),
            ProfileFact(
                member_id="30002",
                slot="preferences",
                text="喜欢冰美式",
                evidence_seq=seq_by_uid["30002"],
                quote="喜欢冰美式",
                basis="self_report",
            ),
            ProfileFact(
                member_id="30002",
                slot="boundaries",
                text="别提及旧事",
                evidence_seq=seq_by_uid["30002"],
                quote="别提及旧事",
                basis="self_report",
            ),
            ProfileFact(
                member_id="30003",
                slot="alias",
                text="老三",
                evidence_seq=seq_by_uid["30003"],
                quote="我是老三",
                basis="self_report",
            ),
            ProfileFact(
                member_id="30004",
                slot="alias",
                text="老四",
                evidence_seq=seq_by_uid["30004"],
                quote="我是老四",
                basis="self_report",
            ),
            ProfileFact(
                member_id="30005",
                slot="alias",
                text="老五",
                evidence_seq=seq_by_uid["30005"],
                quote="我是老五",
                basis="self_report",
            ),
        ],
        chronicles=[],
    )
    assert await sessions.dual.commit(anchor_msg, cold_job, extraction)

    incoming = group(
        message,
        event_id="9001",
        user_id="30001",
        sender_nickname="主问者",
        addressed=True,
        text="帮我看看这两张图和群里刚聊的",
        mentioned_ids=["10000", "30002", "30003", "30004", "30005"],
        images=[
            ImageRef(file="p1"),
            ImageRef(file="p2"),
        ],
    )
    assert await svc.accept(incoming) == "accepted"
    await svc.process(await store.claim())

    screen_calls = [c for c in ai.calls if c[0] == "screen" and c[3].get("purpose") == "message"]
    assert screen_calls and screen_calls[0][3].get("relevance_context")

    gen_calls = [c for c in ai.calls if c[0] == "generate"]
    assert gen_calls
    gen_payload, _, gen_kwargs = gen_calls[0][1], gen_calls[0][2], gen_calls[0][3]
    assert len(gen_kwargs["images"]) == 2
    assert 1 <= len(gen_payload["nearby_group_context"]) <= 6
    assert all(len(item["text"]) <= 160 for item in gen_payload["nearby_group_context"])
    assert len(gen_payload["dual_memory_data"]["mentioned_profiles"]) == 3
    assert set(gen_payload["dual_memory_data"]["mentioned_profiles"]["30002"]) == {
        "alias",
        "preferences",
    }

    assembled = assemble_chat_input(gen_payload, incoming, settings)
    ctx_json = json.loads(
        assembled.split("<untrusted_user_input>")[1].split("</untrusted_user_input>")[0]
    )
    assert isinstance(ctx_json["current_batch"], list)
    assert ctx_json["current_batch"][0]["event_id"] == "9001"
    await svc.stop()


async def test_expanded_packing_and_forwarding_limits(settings, store, message):
    assert MAX_BATCH_MESSAGES == 16 and MAX_BATCH_CHARS == 24000
    line = json.loads(record_line(message))
    assert line["event_id"] == message.event_id

    settings.group_debounce_seconds = 10.0
    settings.group_debounce_max_seconds = 20.0
    for i in range(16):
        msg = group(message, event_id=f"b_{i}", addressed=True, text=f"连发{i}")
        await store.admit(msg)
    assert len(await store.all("SELECT * FROM group_debounce_items")) == 16
    batches = await store.all("SELECT * FROM group_debounce_batches")
    assert len(batches) == 1 and batches[0]["count"] == 16

    nodes_40 = [
        {"sender": {"user_id": str(31000 + i), "nickname": f"人{i}"}, "content": f"消息{i}"}
        for i in range(40)
    ]
    fwd_40 = parse_forward({"messages": nodes_40}, "10000", include_authors=True)
    assert not fwd_40.truncated and "消息39" in fwd_40.text
    fwd_42 = parse_forward(
        {
            "messages": nodes_40
            + [
                {"sender": {"user_id": "31040", "nickname": "人40"}, "content": "消息40"},
                {"sender": {"user_id": "31041", "nickname": "人41"}, "content": "消息41"},
            ]
        },
        "10000",
        include_authors=True,
    )
    assert fwd_42.truncated and "消息39" in fwd_42.text and "消息40" not in fwd_42.text

    replies = {
        str(hop): group(
            message,
            event_id=str(hop),
            text=f"回复层{hop}:" + ("文" * 3500),
            reply_ids=[str(hop - 1)] if hop > 1 else [],
        )
        for hop in range(1, 7)
    }

    async def lookup(_, ref):
        return replies.get(ref)

    resolved = await MessageContextResolver(settings, None, lookup).resolve(
        group(message, event_id="head", text="顶层问题", reply_ids=["6"])
    )
    assert "回复层1:" in resolved.text and len(resolved.text) <= 24000


async def test_end_to_end_stateful_chat_chain_and_budget_reservation(
    settings, store, message, sdk_pair
):
    turn_counter = {"n": 0}
    recorded_interactions = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        recorded_interactions.append(body)
        turn_counter["n"] += 1
        resp = interaction_response()
        resp["id"] = f"inter_{turn_counter['n']}"
        resp["steps"][0]["content"][0]["text"] = action(bubbles=[f"第{turn_counter['n']}轮收到"])
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    sender = ActionSender()
    svc = Service(settings, store, gate, FakeBili(), sender)
    settings.group_policies = {"onebot:40000": "at_only"}

    m1 = group(message, event_id="1001", addressed=True, text="第一轮你好" + ("背" * 800))
    assert await svc.accept(m1) == "accepted"
    await svc.process(await store.claim())

    # Before outbox ACK delivery completes, chain must not be committed yet!
    assert (await svc.sessions.snapshot(m1))["interaction_chains"] == {}
    assert recorded_interactions[0]["store"] is True
    assert isinstance(recorded_interactions[0]["input"], list)
    assert recorded_interactions[0]["input"][0]["type"] == "user_input"
    assert "previous_interaction_id" not in recorded_interactions[0]

    assert await svc.deliver_once()
    snap_after_1 = await svc.sessions.snapshot(m1)
    chain_1 = snap_after_1["interaction_chains"]["hydrogen"]
    assert chain_1["interaction_id"] == "inter_1"
    assert chain_1["covered_seq"] == snap_after_1["recent_messages"][-1]["seq"]

    m2 = group(message, event_id="1002", addressed=True, text="第二轮继续")
    assert await svc.accept(m2) == "accepted"
    await svc.process(await store.claim())

    assert recorded_interactions[1]["store"] is True
    assert recorded_interactions[1]["previous_interaction_id"] == "inter_1"
    wire2 = _wire_text(recorded_interactions[1])
    log_block = wire2.split("<conversation_history_log>\n")[1].split("</conversation_history_log>")[
        0
    ]
    assert "第一轮你好" not in log_block
    assert "第1轮收到" not in log_block
    assert await svc.deliver_once()
    assert (await svc.sessions.snapshot(m2))["interaction_chains"]["hydrogen"][
        "interaction_id"
    ] == "inter_2"

    reservations = await store.all(
        "SELECT l.reserved FROM ledger l JOIN ledger_usage u ON u.id=l.id "
        "WHERE l.lane='chat' AND u.model_lane='hydrogen' ORDER BY l.rowid"
    )
    # Turn 2 has full-history conservative reservation > Turn 1 despite delta wire input
    assert len(reservations) == 2
    assert reservations[1][0] > reservations[0][0]
    await svc.stop()


async def test_chain_safety_and_invalidation_paths(settings, store, message, sdk_pair):
    requests = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        requests.append(body)
        resp = interaction_response()
        resp["id"] = f"inter_{len(requests)}"
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    sender = ActionSender()
    svc = Service(settings, store, gate, FakeBili(), sender)
    settings.group_policies = {"onebot:40000": "at_only"}

    # 1. Stateless/unsafe cases always stay store=False without previous_interaction_id
    m_priv = message.model_copy(update={"event_id": "2001", "text": "测试"})
    await gate.generate(
        {
            "current_message": "沙盒",
            "interaction_chains": {
                "hydrogen": {"interaction_id": "inter_stale", "expires": time.time() + 600}
            },
        },
        "s",
        message=m_priv,
        route=RouteDecision(tier=1, sandbox_mode=True),
    )
    await gate.generate(
        {"current_message": "禁用记忆", "session_enabled": False},
        "s",
        message=m_priv,
    )
    await gate.generate({"current_message": "无消息上下文"}, "s", message=None)
    await gate.generate({"current_message": "摘要任务"}, "s", scenario=Scenario.DAILY_DIARY)
    assert all(r["store"] is False and "previous_interaction_id" not in r for r in requests)

    # 2. Failed delivery invalidates existing committed chain
    m1 = group(message, event_id="2002", addressed=True, text="建链消息")
    await svc.accept(m1)
    await svc.process(await store.claim())
    await svc.deliver_once()
    assert "hydrogen" in (await svc.sessions.snapshot(m1))["interaction_chains"]

    sender.fail = True
    m2 = group(message, event_id="2003", addressed=True, text="这次发送超时")
    await svc.accept(m2)
    await svc.process(await store.claim())
    await svc.deliver_once()
    assert (await svc.sessions.snapshot(m2))["interaction_chains"] == {}
    sender.fail = False

    # 3. Group recall notice removes recalled event_id from history and clears chain
    m3 = group(message, event_id="2004", addressed=True, text="会撤回的话")
    await svc.accept(m3)
    await svc.process(await store.claim())
    await svc.deliver_once()
    assert "hydrogen" in (await svc.sessions.snapshot(m3))["interaction_chains"]

    recall_payload = {
        "post_type": "notice",
        "notice_type": "group_recall",
        "self_id": int(settings.onebot_self_id),
        "group_id": 40000,
        "user_id": 30000,
        "message_id": 2004,
    }
    assert await svc.accept_event(recall_payload) == "recalled"
    snap_recalled = await svc.sessions.snapshot(m3)
    assert snap_recalled["interaction_chains"] == {}
    assert all(item.get("event_id") != "2004" for item in snap_recalled["recent_messages"])

    # 4. /new-session (clear) invalidates chain
    m4 = group(message, event_id="2005", addressed=True, text="重建")
    await svc.accept(m4)
    await svc.process(await store.claim())
    await svc.deliver_once()
    assert "hydrogen" in (await svc.sessions.snapshot(m4))["interaction_chains"]
    await svc.sessions.clear(m4)
    assert (await svc.sessions.snapshot(m4))["interaction_chains"] == {}
    await svc.stop()


async def test_continuation_turn_deduplicates_unchanged_transient_context_and_caps_turns(
    settings, store, message, sdk_pair, monkeypatch
):
    from hakimi.prefix_runtime import MAX_CHAIN_TURNS

    assert MAX_CHAIN_TURNS == 12 and settings.interaction_chain_ttl_seconds == 1800
    monkeypatch.setattr("hakimi.gemini.MAX_CHAIN_TURNS", 2)
    recorded = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            dec = decision_response()
            dec["answers"]["need_memory_recall"]["noul"] = 0.9
            return httpx.Response(200, json=dec)
        body = json.loads(request.content)
        recorded.append(body)
        resp = interaction_response()
        resp["id"] = f"inter_slim_{len(recorded)}"
        resp["steps"][0]["content"][0]["text"] = action(bubbles=[f"第{len(recorded)}轮"])
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    sender = ActionSender()
    svc = Service(settings, store, gate, FakeBili(), sender)
    settings.group_policies = {"onebot:40000": "at_only"}
    await svc.memes.upload("测试表情.png", _png_bytes())

    now = time.time()
    m1 = group(message, event_id="3001", addressed=True, text="第一轮", created_at=now + 1)
    async with svc.sessions.database(m1) as (db, key):
        await db.execute(
            "UPDATE rolling_state SET summary=?,expires=?",
            (svc.sessions.vault.seal(key, "固定群聊摘要"), now + 3600),
        )
    bystander = group(
        message,
        event_id="3000",
        uid="30001",
        addressed=False,
        text="旁观背景一句",
        created_at=now,
    )
    assert await svc.accept(bystander) == "ignored"
    assert await svc.accept(m1) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()

    def wire(idx):
        item = recorded[idx]["input"][0]
        return item["content"][0]["text"] if "content" in item else item["text"]

    w1 = wire(0)
    assert "固定群聊摘要" in w1
    assert '"meme_labels":["测试表情"]' in w1
    assert "旁观背景一句" in w1
    assert "公开回复不得称对方为" in w1

    # Turn 2 continues inter_slim_1 and drops unchanged summary, memes, nearby, static text.
    m2 = group(message, event_id="3002", addressed=True, text="第二轮", created_at=now + 2)
    assert await svc.accept(m2) == "accepted"
    await svc.process(await store.claim())
    assert await svc.deliver_once()
    assert recorded[1]["previous_interaction_id"] == "inter_slim_1"
    w2 = wire(1)
    assert "固定群聊摘要" not in w2
    assert "meme_labels" not in w2
    assert "旁观背景一句" not in w2
    assert "公开回复不得称对方为" not in w2
    assert "<memory_summary_data>" not in w2
    assert "<untrusted_memory_data>" not in w2

    # Turn 3 hits MAX_CHAIN_TURNS=2 and starts a fresh chain with full context again.
    m3 = group(message, event_id="3003", addressed=True, text="第三轮", created_at=now + 3)
    assert await svc.accept(m3) == "accepted"
    await svc.process(await store.claim())
    assert "previous_interaction_id" not in recorded[2]
    w3 = wire(2)
    assert "固定群聊摘要" in w3 and '"meme_labels":["测试表情"]' in w3
    await svc.stop()


async def test_interaction_chain_ttl_default_override_and_memory_clamp(
    tmp_path, settings, store, message, monkeypatch
):
    from hakimi.config import Settings

    assert settings.interaction_chain_ttl_seconds == 1800

    monkeypatch.setenv("HAKIMI_INTERACTION_CHAIN_TTL_SECONDS", "900")
    s_override = Settings(
        _env_file=None,
        data_dir=tmp_path / "override",
        gemini_api_key="paid-env-secret-abcdef",
        encryption_key="MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    )
    assert s_override.interaction_chain_ttl_seconds == 900

    sessions = SessionManager(settings, store)
    msg = group(message, event_id="ttl_1", text="你好")
    await sessions.receive(msg)
    snap = await sessions.snapshot(msg)
    now = 1_750_000_000.0
    monkeypatch.setattr("hakimi.session_manager.time.time", lambda: now)

    # Normal default TTL = 1800s (memory_ttl_seconds is 86400s).
    assert await sessions.commit_chain(
        msg,
        lane="hydrogen",
        interaction_id="inter_ttl_1800",
        expected_epoch=snap["epoch"],
        trim_version=snap["history_revision"],
        system_digest="dig-1",
        covered_seq=0,
    )
    chain_1800 = (await sessions.snapshot(msg))["interaction_chains"]["hydrogen"]
    assert chain_1800["expires"] == now + 1800

    # Clamped by memory_ttl_seconds when memory_ttl_seconds < interaction_chain_ttl_seconds.
    settings.memory_ttl_seconds = 600
    assert await sessions.commit_chain(
        msg,
        lane="hydrogen",
        interaction_id="inter_ttl_clamped",
        expected_epoch=snap["epoch"],
        trim_version=snap["history_revision"],
        system_digest="dig-1",
        covered_seq=0,
    )
    chain_clamped = (await sessions.snapshot(msg))["interaction_chains"]["hydrogen"]
    assert chain_clamped["expires"] == now + 600


async def test_intervening_proactive_and_social_speak_rows_appear_in_next_continuation(
    settings, store, message, sdk_pair
):
    recorded = []

    def handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        body = json.loads(request.content)
        recorded.append(body)
        resp = interaction_response()
        resp["id"] = f"inter_gap_{len(recorded)}"
        resp["steps"][0]["content"][0]["text"] = action(bubbles=[f"回复{len(recorded)}"])
        return httpx.Response(200, json=resp)

    gate = GeminiGateway(settings, store, *sdk_pair(handler))
    sender = ActionSender()
    svc = Service(settings, store, gate, FakeBili(), sender)
    try:
        # Turn 1 on owner private session establishes chain.
        m1 = message.model_copy(update={"event_id": "priv_1", "text": "第一句私聊"})
        assert await svc.accept(m1) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        snap1 = await svc.sessions.snapshot(m1)
        epoch = snap1["epoch"]
        assert snap1["interaction_chains"]["hydrogen"]["interaction_id"] == "inter_gap_1"

        # Intervening proactive DM (remember_assistant) and JIT social_speak (reply_batch).
        assert await svc.sessions.remember_assistant(
            m1, "主动发来的早安提醒", "proactive-rcpt-1", expected_epoch=epoch
        )
        assert await svc.sessions.reply_batch(
            m1,
            "social-speak-1",
            [{"message_id": "m-social-1", "text": "主动搭话冒泡一句"}],
            expected_epoch=epoch,
        )

        # Turn 2 continues the chain and MUST include both intervening rows in delta history!
        m2 = message.model_copy(update={"event_id": "priv_2", "text": "刚看到你的消息"})
        assert await svc.accept(m2) == "accepted"
        await svc.process(await store.claim())
        assert recorded[1]["previous_interaction_id"] == "inter_gap_1"
        wire2 = _wire_text(recorded[1])
        log_block = wire2.split("<conversation_history_log>\n")[1].split(
            "</conversation_history_log>"
        )[0]
        assert "第一句私聊" not in log_block
        assert "回复1" not in log_block
        assert "主动发来的早安提醒" in log_block
        assert "主动搭话冒泡一句" in log_block
    finally:
        await svc.stop()


async def test_vertex_location_change_isolates_vertex_chain_system_digest(settings, store, message):
    from hakimi.provider_routing import ProviderRoutingConfig
    from tests.test_provider_routing_and_vertex import _client_pair, _close_clients

    vertex_calls = []

    def dev_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=interaction_response())

    def vertex_handler(request):
        body = json.loads(request.content)
        vertex_calls.append(body)
        resp = interaction_response()
        resp["id"] = f"vtx_loc_{len(vertex_calls)}"
        return httpx.Response(200, json=resp)

    clients, transports = _client_pair(dev_handler)
    v_client, v_clients, v_transports = _client_pair(vertex_handler, vertex=True)
    gate = GeminiGateway(settings, store, *clients, vertex_chat_client=v_client)
    try:
        # Turn 1 on vertex_location="us"
        await gate.routing.set_config(
            ProviderRoutingConfig(vertex_enabled=True, vertex_location="us")
        )
        history = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "前文内容"}]
        trace1 = {}
        await gate.generate(
            {"current_message": "hi", "recent_messages": history},
            "subject",
            message=message,
            generation_trace=trace1,
        )
        us_chain = {**trace1["chain_commit"], "expires": time.time() + 1800}
        assert us_chain["interaction_id"] == "vtx_loc_1"

        # Turn 2 on same location ("us") reuses chain
        trace2 = {}
        await gate.generate(
            {
                "current_message": "same-region",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": us_chain},
            },
            "subject",
            message=message,
            generation_trace=trace2,
        )
        assert vertex_calls[1]["previous_interaction_id"] == "vtx_loc_1"

        # Switch vertex_location to "global" -> system_digest differs -> drops old "us" chain!
        await gate.routing.set_config(
            ProviderRoutingConfig(vertex_enabled=True, vertex_location="global")
        )
        trace3 = {}
        await gate.generate(
            {
                "current_message": "global-region",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": us_chain},
            },
            "subject",
            message=message,
            generation_trace=trace3,
        )
        assert "previous_interaction_id" not in vertex_calls[2]
        assert "前文内容" in _wire_text(vertex_calls[2])
        assert trace3["chain_commit"]["system_digest"] != us_chain["system_digest"]
    finally:
        await gate.close()
        await _close_clients([*clients, *v_clients], transports + v_transports)


async def test_stale_previous_interaction_id_400_404_retries_once_stateless(
    settings, store, message, sdk_pair
):
    import pytest

    from hakimi.provider_routing import ProviderRoutingConfig
    from hakimi.security import Rejected
    from tests.test_provider_routing_and_vertex import _client_pair, _close_clients

    # 1. gemini_paid: 404 and 400 INVALID_ARGUMENT retry stateless with full history;
    #    400 SAFETY_BLOCKED does NOT waste a retry.
    dev_calls = []
    dev_responses = []

    def dev_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        dev_calls.append(json.loads(request.content))
        return dev_responses.pop(0)

    gate = GeminiGateway(settings, store, *sdk_pair(dev_handler))
    try:
        history = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "必须保留的历史"}]
        first_ok = interaction_response()
        first_ok["id"] = "gem_orig_1"
        dev_responses.append(httpx.Response(200, json=first_ok))
        t0 = {}
        await gate.generate(
            {"current_message": "首轮", "recent_messages": history},
            "subject",
            message=message,
            generation_trace=t0,
        )
        stale_chain = {**t0["chain_commit"], "expires": time.time() + 1800}

        # Pre-stream 404 on prev_id -> $0 settled, 1 stateless retry with full history -> 200 OK
        fresh_ok = interaction_response()
        fresh_ok["id"] = "gem_fresh_2"
        dev_responses.extend(
            [
                httpx.Response(404, json={"error": {"code": 404, "status": "NOT_FOUND"}}),
                httpx.Response(200, json=fresh_ok),
            ]
        )
        t404 = {}
        out404 = await gate.generate(
            {
                "current_message": "续链404",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": stale_chain},
            },
            "subject",
            message=message,
            generation_trace=t404,
        )
        assert json.loads(out404)["bubbles"]
        assert dev_calls[1]["previous_interaction_id"] == "gem_orig_1"
        assert "previous_interaction_id" not in dev_calls[2]
        assert "必须保留的历史" in _wire_text(dev_calls[2])
        assert t404["chain_commit"]["interaction_id"] == "gem_fresh_2"
        assert t404["chain_commit"]["turns"] == 1
        assert t404["provider_diagnostics"][0]["http_status"] == 404
        assert t404["provider_diagnostics"][0]["fallback"] == "succeeded"

        # Pre-stream 400 INVALID_ARGUMENT on prev_id -> stateless retry -> 200 OK
        fresh_ok_3 = interaction_response()
        fresh_ok_3["id"] = "gem_fresh_3"
        dev_responses.extend(
            [
                httpx.Response(400, json={"error": {"code": 400, "status": "INVALID_ARGUMENT"}}),
                httpx.Response(200, json=fresh_ok_3),
            ]
        )
        out400 = await gate.generate(
            {
                "current_message": "续链400",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": stale_chain},
            },
            "subject",
            message=message,
        )
        assert json.loads(out400)["bubbles"]
        assert dev_calls[3]["previous_interaction_id"] == "gem_orig_1"
        assert "previous_interaction_id" not in dev_calls[4]
        assert "必须保留的历史" in _wire_text(dev_calls[4])

        # Pre-stream 400 SAFETY_BLOCKED does NOT retry stateless.
        before_safety = len(dev_calls)
        dev_responses.append(
            httpx.Response(400, json={"error": {"code": 400, "status": "SAFETY_BLOCKED"}})
        )
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gate.generate(
                {
                    "current_message": "安全400",
                    "recent_messages": history,
                    "interaction_chains": {"hydrogen": stale_chain},
                },
                "subject",
                message=message,
            )
        assert len(dev_calls) == before_safety + 1
    finally:
        await gate.close()

    # 2. Vertex proto="auto": 404 on stale prev_id retries stateless interactions.create FIRST;
    #    if stateless interactions.create ALSO 404s, falls back cleanly to streamGenerateContent!
    v_paths = []
    v_bodies = []
    v_resps = []

    def v_handler(request):
        v_paths.append(request.url.path)
        v_bodies.append(json.loads(request.content))
        return v_resps.pop(0)

    def d_handler(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(200, json=interaction_response())

    clients, transports = _client_pair(d_handler)
    v_client, v_clients, v_transports = _client_pair(v_handler, vertex=True)
    v_gate = GeminiGateway(settings, store, *clients, vertex_chat_client=v_client)
    try:
        await v_gate.routing.set_config(
            ProviderRoutingConfig(vertex_enabled=True, vertex_protocol="auto")
        )
        history = [{"seq": 1, "role": "user", "user_id": message.user_id, "text": "Vertex旧记忆"}]
        v1 = interaction_response()
        v1["id"] = "vtx_stale_1"
        v_resps.append(httpx.Response(200, json=v1))
        vt1 = {}
        await v_gate.generate(
            {"current_message": "v1", "recent_messages": history},
            "subject",
            message=message,
            generation_trace=vt1,
        )
        v_chain = {**vt1["chain_commit"], "expires": time.time() + 1800}

        # (a) Stale prev_id 404 -> retries stateless interactions.create -> 200 OK on interactions!
        v2 = interaction_response()
        v2["id"] = "vtx_fresh_2"
        v_resps.extend(
            [
                httpx.Response(404, json={"error": {"code": 404, "message": "stale id"}}),
                httpx.Response(200, json=v2),
            ]
        )
        vt2 = {}
        await v_gate.generate(
            {
                "current_message": "v2",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": v_chain},
            },
            "subject",
            message=message,
            generation_trace=vt2,
        )
        assert v_paths[1].endswith("/interactions")
        assert v_bodies[1]["previous_interaction_id"] == "vtx_stale_1"
        assert v_paths[2].endswith("/interactions")
        assert "previous_interaction_id" not in v_bodies[2]
        assert "Vertex旧记忆" in _wire_text(v_bodies[2])
        assert vt2["chain_commit"]["interaction_id"] == "vtx_fresh_2"

        # (b) Both prev_id AND stateless interactions.create return 404 -> falls back to GC stream!
        gc_chunk = {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [{"text": action(bubbles=["GC回退成功"])}],
                    },
                    "finishReason": "STOP",
                }
            ]
        }
        v_resps.extend(
            [
                httpx.Response(404, json={"error": {"code": 404, "message": "no endpoint 1"}}),
                httpx.Response(404, json={"error": {"code": 404, "message": "no endpoint 2"}}),
                httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    content=f"data: {json.dumps(gc_chunk)}\n\n",
                ),
            ]
        )
        out_gc = await v_gate.generate(
            {
                "current_message": "v3",
                "recent_messages": history,
                "interaction_chains": {"hydrogen": v_chain},
            },
            "subject",
            message=message,
        )
        assert json.loads(out_gc)["bubbles"] == ["GC回退成功"]
        assert v_paths[3].endswith("/interactions") and v_paths[4].endswith("/interactions")
        assert "streamGenerateContent" in v_paths[5]
        assert "Vertex旧记忆" in json.dumps(v_bodies[5], ensure_ascii=False)
    finally:
        await v_gate.close()
        await _close_clients([*clients, *v_clients], transports + v_transports)


def test_continuation_resets_world_state_summary_on_return_to_calm_idle(settings, message):
    base_payload = {
        "current_message": "现在在干嘛",
        "recent_messages": [
            {"seq": 1, "role": "user", "user_id": "20000", "speaker": "BOISER", "text": "嗨"}
        ],
    }
    active_world = {
        "current_module": "minecraft",
        "focus_level": "HIGH",
        "mood": "focused",
    }

    def _meta(raw: str) -> dict:
        return json.loads(
            raw.split("<untrusted_user_input>")[1].split("</untrusted_user_input>")[0]
        )

    # Turn 1: active world state emitted & hashed
    st1: dict = {}
    out1 = assemble_chat_input(
        {**base_payload, "world_state_summary": active_world},
        message,
        settings,
        continuation=False,
        out_state=st1,
    )
    assert _meta(out1)["world_state_summary"] == active_world
    assert st1["context_digests"].get("world")

    # Turn 2: calm idle (world_state_summary=None) on continuation -> emits explicit idle reset
    # and clears world digest so stale activity/mood does not linger over a 30m chain.
    st2: dict = {}
    out2 = assemble_chat_input(
        dict(base_payload),
        message,
        settings,
        continuation=True,
        prev_digests=st1["context_digests"],
        out_state=st2,
    )
    assert _meta(out2)["world_state_summary"] == {
        "current_module": "idle",
        "focus_level": "LOW",
        "mood": "calm",
    }
    assert "world" not in st2["context_digests"]

    # Turn 3: still calm idle -> does not repeatedly emit the idle reset.
    st3: dict = {}
    out3 = assemble_chat_input(
        dict(base_payload),
        message,
        settings,
        continuation=True,
        prev_digests=st2["context_digests"],
        out_state=st3,
    )
    assert "world_state_summary" not in _meta(out3)
    assert "world" not in st3["context_digests"]
