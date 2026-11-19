"""Tests for privacy-tiered decision routing, 2500-token context, Vertex AI, and provider admin."""

import datetime
import inspect
import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google import genai
from google.genai import types
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.config import Settings
from hakimi.context_budget import estimate_tokens
from hakimi.gateway import redact_excerpt
from hakimi.gemini import GeminiGateway, questions
from hakimi.generation_policy import Scenario
from hakimi.media import ModelImage
from hakimi.provider_admin import router as provider_router
from hakimi.provider_routing import (
    ProviderCredentialsUpdate,
    ProviderRoutingConfig,
    ProviderRoutingManager,
    SessionPrivacyUpdate,
    validate_service_account_json,
)
from hakimi.security import Rejected, configured_secrets
from hakimi.store import Store
from tests.test_admin import TOKEN
from tests.test_admin import admin_api as admin_api
from tests.test_gateway import group
from tests.test_sdk_contract import decision_response, interaction_response, stream_response


def _make_sa_json(
    *,
    project_id: str = "hakimi-prod",
    client_email: str = "hakimi-bot@hakimi-prod.iam.gserviceaccount.com",
    token_uri: str = "https://oauth2.googleapis.com/token",
    sa_type: str = "service_account",
    private_key: str | None = None,
) -> str:
    if private_key is None:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_key = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode("ascii")
    return json.dumps(
        {
            "type": sa_type,
            "project_id": project_id,
            "private_key_id": "key-id-1",
            "private_key": private_key,
            "client_email": client_email,
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": token_uri,
        }
    )


def _client_pair(handler, *, vertex: bool = False):
    transports: list[httpx.AsyncClient] = []
    clients: list[genai.Client] = []

    async def adapted(request):
        resp = handler(request)
        if inspect.isawaitable(resp):
            resp = await resp
        return stream_response(request, resp)

    if vertex:
        transport = httpx.AsyncClient(transport=httpx.MockTransport(adapted), trust_env=False)
        transports.append(transport)
        client = genai.Client(
            enterprise=True,
            vertexai=True,
            api_key="vertex-test-key-never-live",
            project=None,
            location=None,
            http_options={
                "httpx_async_client": transport,
                "retry_options": {"attempts": 1},
                "client_args": {"trust_env": False},
                "async_client_args": {"trust_env": False},
            },
        )
        clients.append(client)
        return client, clients, transports

    for version in ("v1beta", "v1"):
        transport = httpx.AsyncClient(transport=httpx.MockTransport(adapted), trust_env=False)
        transports.append(transport)
        client = genai.Client(
            enterprise=False,
            vertexai=False,
            api_key="test-only",
            http_options={
                "api_version": version,
                "httpx_async_client": transport,
                "retry_options": {"attempts": 1},
                "client_args": {"trust_env": False},
                "async_client_args": {"trust_env": False},
            },
        )
        clients.append(client)
    return clients, transports


async def _close_clients(clients, transports):
    for client in clients:
        await client.aio.aclose()
        client.close()
    for transport in transports:
        await transport.aclose()


def _chat_json(text: str = "收到喵") -> str:
    return json.dumps(
        {
            "abstain": False,
            "bubbles": [text],
            "anchor": {"reply_id": None, "mentions": []},
            "sticker": {"label": None, "timing": "after_text"},
            "image_digest": None,
        },
        ensure_ascii=False,
    )


def test_settings_aliases_and_configured_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("GEMINI_FREE_API_KEY", "free-env-secret-12345")
    monkeypatch.setenv("VERTEX_API_KEY", "vertex-env-secret-67890")
    monkeypatch.setenv("DECISION_CONTEXT_TOKENS", "1800")
    monkeypatch.setenv("VERTEX_ENABLED", "true")
    monkeypatch.setenv("VERTEX_PROJECT", "proj-demo")
    monkeypatch.setenv("VERTEX_LOCATION", "us-central1")
    monkeypatch.setenv("VERTEX_HYDROGEN_MODEL", "gemini-2.5-pro")
    monkeypatch.setenv("GCP_BILLING_PROJECT", "bill-proj")
    monkeypatch.setenv("GCP_BILLING_TABLE", "bill-proj.ds.export")

    s = Settings(
        _env_file=None,
        data_dir=tmp_path / "data",
        gemini_api_key="paid-env-secret-abcdef",
        encryption_key=Settings.model_fields["encryption_key"].default
        if False
        else "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    )
    assert s.gemini_free_api_key.get_secret_value() == "free-env-secret-12345"
    assert s.vertex_api_key.get_secret_value() == "vertex-env-secret-67890"
    assert s.decision_context_tokens == 1800
    assert s.vertex_enabled is True
    assert s.vertex_project == "proj-demo"
    assert s.vertex_location == "us-central1"
    assert s.vertex_hydrogen_model == "gemini-2.5-pro"
    assert s.gcp_billing_project == "bill-proj"
    assert s.gcp_billing_table == "bill-proj.ds.export"

    s._runtime_extra_secrets = ("sqlite-free-secret-999",)
    secrets = configured_secrets(s)
    assert "free-env-secret-12345" in secrets
    assert "vertex-env-secret-67890" in secrets
    assert "sqlite-free-secret-999" in secrets
    assert "sqlite-free-secret-999" not in redact_excerpt("leak sqlite-free-secret-999 here", s)


def test_service_account_json_strict_validation():
    valid = _make_sa_json()
    info = validate_service_account_json(valid)
    assert info["client_email"].endswith(".iam.gserviceaccount.com")

    with pytest.raises(ValueError):
        validate_service_account_json("not-json")
    with pytest.raises(ValueError):
        validate_service_account_json("[]")
    with pytest.raises(ValueError):
        validate_service_account_json(_make_sa_json(sa_type="authorized_user"))
    with pytest.raises(ValueError):
        validate_service_account_json(_make_sa_json(token_uri="https://evil.example/token"))
    with pytest.raises(ValueError):
        validate_service_account_json(_make_sa_json(client_email="user@gmail.com"))
    with pytest.raises(ValueError):
        validate_service_account_json(_make_sa_json(private_key="not-a-valid-pem"))
    with pytest.raises(ValueError):
        validate_service_account_json("x" * 65537)
    with pytest.raises(ValueError):
        ProviderCredentialsUpdate(gcp_service_account_json='{"type":"authorized_user"}')
    with pytest.raises(ValueError):
        ProviderRoutingConfig(group_privacy={"onebot:bad": "high"})
    with pytest.raises(ValueError):
        SessionPrivacyUpdate(kind="group", platform="onebot", target_id="40000")


async def test_privacy_and_hydrogen_routing_rules_and_persistence(settings, store, message):
    routing = ProviderRoutingManager(store)
    await routing.initialize()

    grp = group(message)
    owner_dm = message  # user_id=20000 == owner_qq_id
    other_dm = message.model_copy(update={"user_id": "30000", "conversation_id": "30000"})

    assert routing.privacy_for(None, settings) == "high"
    assert routing.privacy_for(grp, settings) == "low"
    assert routing.privacy_for(owner_dm, settings) == "high"
    assert routing.privacy_for(other_dm, settings) == "high"

    # Even if default_private_privacy is set to "low", Owner DM stays "high" unless overridden.
    # Upgrade previously encrypted privacy levels and opposing per-session model settings.
    await store.runtime.save_config(
        "provider_routing",
        {
            "default_group_privacy": "low",
            "default_private_privacy": "low",
            "group_privacy": {"onebot:40000": "high"},
            "vertex_enabled": True,
            "group_hydrogen_provider": {"onebot:40000": "gemini"},
            "private_hydrogen_provider": {"onebot:20000": "vertex"},
        },
    )
    routing = ProviderRoutingManager(store)
    await routing.initialize()
    row = await store.one("SELECT payload FROM admin_config WHERE key='provider_routing'")
    saved = store.vault.open("admin:provider_routing", row[0])
    assert "private_hydrogen_provider" not in saved and "group_hydrogen_provider" not in saved
    assert routing.privacy_for(grp, settings) == "high"
    assert routing.privacy_for(other_dm, settings) == "low"
    assert routing.privacy_for(owner_dm, settings) == "high"

    # Explicit override on owner DM can flip it if admin deliberately chooses.
    await routing.set_session_policy(
        SessionPrivacyUpdate(kind="private", platform="onebot", target_id="20000", privacy="low")
    )
    assert routing.privacy_for(owner_dm, settings) == "low"

    # Every conversation uses the global channel before and after credential changes.
    assert routing.hydrogen_provider_for(other_dm) == "gemini"
    assert routing.hydrogen_provider_for(grp) == "gemini"
    assert routing.hydrogen_provider_for(owner_dm) == "gemini"

    await routing.update_credentials(
        ProviderCredentialsUpdate(
            gemini_free_api_key="free-key-sqlite-1234",
            vertex_api_key="vertex-key-sqlite-5678",
            gcp_service_account_json=_make_sa_json(),
        )
    )
    assert routing.hydrogen_provider_for(other_dm) == "vertex"
    assert routing.hydrogen_provider_for(grp) == routing.hydrogen_provider_for(owner_dm) == "vertex"
    sa_info, sa_src = routing.service_account_info()
    assert sa_info is not None and sa_src == "sqlite"
    assert "free-key-sqlite-1234" in configured_secrets(settings)
    assert "vertex-key-sqlite-5678" in configured_secrets(settings)

    # Survives Store restart
    await store.close()
    reopened = Store(settings)
    await reopened.open()
    try:
        r2 = ProviderRoutingManager(reopened)
        await r2.initialize()
        assert r2.config.group_privacy == {"onebot:40000": "high"}
        assert r2.gemini_free_api_key() == "free-key-sqlite-1234"
        assert r2.vertex_api_key() == "vertex-key-sqlite-5678"
        assert r2.service_account_info()[0]["project_id"] == "hakimi-prod"
    finally:
        await reopened.close()
    await store.open()


async def test_decision_privacy_routing_free_tier_and_429_fallback(settings, store, message):
    paid_calls = []
    free_calls = []
    free_status = [200]

    def paid_handler(request):
        paid_calls.append(request.url.path)
        return httpx.Response(200, json=decision_response())

    def free_handler(request):
        free_calls.append(request.url.path)
        if free_status[0] != 200:
            return httpx.Response(free_status[0], json={"error": {"message": "rate"}})
        return httpx.Response(200, json=decision_response())

    (dec_client, chat_client), transports = _client_pair(paid_handler)
    (free_client, free_unused), free_transports = _client_pair(free_handler)
    try:
        gate = GeminiGateway(
            settings,
            store,
            dec_client,
            chat_client,
            free_decision_client=free_client,
        )
        grp = group(message, addressed=True)

        # 1. Group message defaults to low privacy -> Free Tier ($0)
        trace_low = {}
        assert (
            await gate.screen(
                {"current_message": "你好"},
                "grp-sub",
                purpose="message",
                message=grp,
                trace=trace_low,
            )
            == "chat"
        )
        assert len(free_calls) == 1 and len(paid_calls) == 0
        assert trace_low["privacy_level"] == "low"
        assert trace_low["decision_tier"] == "free"
        audit_low = (await gate.filters.recent_audit(limit=1))[0]
        assert audit_low["privacy_level"] == "low" and audit_low["decision_tier"] == "free"
        free_row = await store.one("SELECT amount,status FROM ledger ORDER BY rowid DESC LIMIT 1")
        assert tuple(free_row) == (0, "settled")

        # 2. Owner private defaults to high privacy -> Paid Tier
        trace_owner = {}
        assert (
            await gate.screen(
                {"current_message": "私聊"},
                "own-sub",
                purpose="message",
                message=message,
                trace=trace_owner,
            )
            == "chat"
        )
        assert len(paid_calls) == 1
        assert trace_owner["privacy_level"] == "high"
        assert trace_owner["decision_tier"] == "paid"

        # 3. Free Tier 429 -> falls back to Paid Tier and NEVER trips blocked_until
        free_status[0] = 429
        trace_fb = {}
        assert (
            await gate.screen(
                {"current_message": "再来"},
                "grp-sub",
                purpose="message",
                message=grp,
                trace=trace_fb,
            )
            == "chat"
        )
        assert len(free_calls) == 2 and len(paid_calls) == 2
        assert trace_fb["decision_tier"] == "paid_fallback"
        [failed_attempt] = trace_fb["provider_diagnostics"]
        assert failed_attempt["provider"] == "gemini_free"
        assert failed_attempt["http_status"] == 429 and failed_attempt["fallback"] == "succeeded"
        assert gate.blocked_until == 0.0

        # 4. With fallback disabled, Free Tier 503 fails without Paid call or circuit trip
        await gate.routing.set_config(ProviderRoutingConfig(free_tier_fallback_to_paid=False))
        free_status[0] = 503
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gate.screen(
                {"current_message": "失败"}, "grp-sub", purpose="message", message=grp
            )
        assert len(paid_calls) == 2
        assert gate.blocked_until == 0.0
    finally:
        await _close_clients(
            [dec_client, chat_client, free_client, free_unused], transports + free_transports
        )


async def test_output_screen_in_generate_uses_session_privacy(settings, store, message):
    paid_decision_calls = []
    free_decision_calls = []

    def paid_handler(request):
        if request.url.path.endswith("decisions"):
            paid_decision_calls.append(request)
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _chat_json("群聊回复")
        return httpx.Response(200, json=resp)

    def free_handler(request):
        free_decision_calls.append(request)
        return httpx.Response(200, json=decision_response())

    (dec_client, chat_client), transports = _client_pair(paid_handler)
    (free_client, free_unused), free_transports = _client_pair(free_handler)
    try:
        gate = GeminiGateway(
            settings, store, dec_client, chat_client, free_decision_client=free_client
        )
        grp = group(message, addressed=True)
        await gate.generate({"current_message": "你好"}, "grp-sub", message=grp)
        assert len(free_decision_calls) == 1 and len(paid_decision_calls) == 0

        await gate.generate({"current_message": "主人好"}, "own-sub", message=message)
        assert len(paid_decision_calls) == 1
    finally:
        await _close_clients(
            [dec_client, chat_client, free_client, free_unused], transports + free_transports
        )


async def test_decision_context_2500_tokens_and_gateway_24_rows(
    settings, store, message, monkeypatch
):
    import time as time_mod

    now = time_mod.time()
    monkeypatch.setattr("hakimi.gateway.time.time", lambda: now)
    filters = store.runtime.filters
    for i in range(28):
        await filters.remember_group(
            group(
                message,
                event_id=f"ev-{i}",
                text=f"第{i}条 " + ("话" * 300),
                created_at=now - 1,
            )
        )
    current = group(message, event_id="cur", text="当前", created_at=now)
    # Default limit=10 preserved for compatibility; explicit limit=24 returns up to 24 rows.
    assert len(await filters.group_context(current)) == 10
    ctx24 = await filters.group_context(current, limit=24)
    assert len(ctx24) == 24
    assert len(ctx24[-1]) <= 320
    assert len(await filters.group_context(current, limit=99)) == 24

    captured = []

    def handler(request):
        captured.append(json.loads(request.content))
        return httpx.Response(200, json=decision_response())

    (dec_client, chat_client), transports = _client_pair(handler)
    try:
        gate = GeminiGateway(settings, store, dec_client, chat_client)
        assert gate.routing.config.decision_context_tokens == 2500

        # 24 context items trimmed cleanly to <= 2500 total tokens (including query)
        rich_ctx = [f"条目{i}-" + ("聊" * 180) for i in range(24)]
        await gate.screen(
            {"current_message": "哈基米在吗"},
            "s",
            purpose="message",
            message=group(message),
            relevance_context=rich_ctx,
        )
        body = captured[0]
        total_tokens = estimate_tokens(body["input"]) + estimate_tokens(
            json.dumps(questions("message"), ensure_ascii=False)
        )
        assert total_tokens <= 2500
        assert "条目23-" in body["input"]
        assert "条目0-" not in body["input"]

        # Setting decision_context_tokens=600 trims all recent context, but still allows
        # a 700-token turn since ceiling is max(budget_tokens, decision_input_tokens).
        await gate.routing.set_config(ProviderRoutingConfig(decision_context_tokens=600))
        await gate.screen(
            {"current_message": "测" * 400},
            "s",
            purpose="message",
            message=group(message),
            relevance_context=["背景旧消息"] * 5,
        )
        assert "背景旧消息" not in captured[1]["input"]
    finally:
        await _close_clients([dec_client, chat_client], transports)


async def test_vertex_production_client_init_and_sa_build(settings, store, monkeypatch):
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "false")
    gate = GeminiGateway(settings, store)
    try:
        # Default paid Gemini clients stay non-Vertex.
        assert not gate.chat_client._api_client.vertexai

        # Vertex API key without project -> project=None, location=None, vertexai=True.
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="api_key",
                vertex_project="",
                vertex_location="global",
            )
        )
        await gate.routing.update_credentials(
            ProviderCredentialsUpdate(vertex_api_key="AIzaSyFakeVertexApiKey12345")
        )
        gate.reload_provider_clients()
        vc = gate.vertex_chat_client
        assert vc is not None
        assert vc._api_client.vertexai is True
        assert vc._api_client.project is None
        assert vc._api_client.location is None
        assert vc._api_client._http_options.base_url == "https://aiplatform.googleapis.com/"

        # Service Account mode -> uses credentials + project + location.
        await gate.routing.update_credentials(
            ProviderCredentialsUpdate(
                gcp_service_account_json=_make_sa_json(project_id="sa-proj-1")
            )
        )
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_auth_mode="service_account",
                vertex_project="sa-proj-1",
                vertex_location="us-central1",
            )
        )
        gate.reload_provider_clients()
        vc_sa = gate.vertex_chat_client
        assert vc_sa is not None
        assert vc_sa._api_client.vertexai is True
        assert vc_sa._api_client.project == "sa-proj-1"
        assert vc_sa._api_client.location == "us-central1"
        assert (
            vc_sa._api_client._http_options.base_url
            == "https://us-central1-aiplatform.googleapis.com/"
        )
    finally:
        await gate.close()


async def test_vertex_interactions_auto_404_generate_content_and_usage_metadata(
    settings, store, message
):
    gemini_calls = []
    vertex_calls = []
    vertex_404_interactions = [False]

    def gemini_handler(request):
        gemini_calls.append((request.url.path, json.loads(request.content)))
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["id"] = "gem-int-1"
        resp["steps"][0]["content"][0]["text"] = _chat_json("Gemini回答")
        return httpx.Response(200, json=resp)

    def vertex_handler(request):
        body = json.loads(request.content)
        vertex_calls.append((request.url.path, body))
        if request.url.path.endswith("interactions"):
            if vertex_404_interactions[0]:
                return httpx.Response(404, json={"error": {"code": 404, "message": "not found"}})
            resp = interaction_response()
            resp["id"] = "vtx-int-1"
            resp["steps"][0]["content"][0]["text"] = _chat_json("Vertex交互回答")
            return httpx.Response(200, json=resp)
        # generateContent / streamGenerateContent
        chunk1 = {
            "candidates": [
                {"content": {"role": "model", "parts": [{"text": "内心思考", "thought": True}]}}
            ]
        }
        chunk2 = {
            "candidates": [
                {
                    "content": {"role": "model", "parts": [{"text": _chat_json("Vertex生成回答")}]},
                    "finishReason": "STOP",
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 100,
                "candidatesTokenCount": 20,
                "thoughtsTokenCount": 30,
                "cachedContentTokenCount": 80,
                "totalTokenCount": 150,
            },
        }
        if "streamGenerateContent" in request.url.path:
            sse = f"data: {json.dumps(chunk1)}\n\ndata: {json.dumps(chunk2)}\n\n"
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=sse)
        return httpx.Response(200, json=chunk2)

    (dec_client, chat_client), g_transports = _client_pair(gemini_handler)
    v_client, v_clients, v_transports = _client_pair(vertex_handler, vertex=True)
    try:
        gate = GeminiGateway(settings, store, dec_client, chat_client, vertex_chat_client=v_client)
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="auto",
                vertex_hydrogen_model="gemini-4-hydrogen",
                vertex_fallback_to_gemini=True,
            )
        )
        await gate.routing.update_credentials(
            ProviderCredentialsUpdate(vertex_api_key="vertex-key-live-mock-123")
        )

        # 1. gemini-4-hydrogen with auto tries interactions.create on Vertex;
        #    also verify chain isolation if an old Gemini interaction_chain is passed!
        trace = {}
        out1 = await gate.generate(
            {
                "current_message": "你好",
                "interaction_chains": {
                    "hydrogen": {
                        "interaction_id": "gem-stale",
                        "expires": datetime.datetime.now(datetime.UTC).timestamp() + 300,
                        "epoch": 0,
                        "trim_version": 0,
                        "system_digest": store.vault.digest("static-prefix", "anything"),
                        "turns": 1,
                    }
                },
            },
            "v-sub",
            message=message,
            generation_trace=trace,
        )
        assert json.loads(out1)["bubbles"] == ["Vertex交互回答"]
        assert vertex_calls[0][0].endswith("/interactions")
        assert "previous_interaction_id" not in vertex_calls[0][1]
        assert trace["provider"] == "vertex_paid"
        assert trace["chain_commit"]["interaction_id"] == "vtx-int-1"

        # 2. HTTP 404 on Vertex interactions under "auto" falls back to streamGenerateContent.
        vertex_404_interactions[0] = True
        out2 = await gate.generate(
            {
                "current_message": "继续",
                "recent_messages": [
                    {"seq": 1, "role": "user", "user_id": "20000", "text": "前情提要"}
                ],
                "interaction_chains": {"hydrogen": trace["chain_commit"]},
            },
            "v-sub",
            message=message,
        )
        assert json.loads(out2)["bubbles"] == ["Vertex生成回答"]
        assert "streamGenerateContent" in vertex_calls[-1][0]
        assert "前情提要" in json.dumps(vertex_calls[-1][1], ensure_ascii=False)
        # Verify usage_metadata accounting (100 in, 20 out, 30 thoughts, 80 cached -> $0.000411)
        row = await store.one(
            "SELECT * FROM ledger_usage WHERE cached_tokens=80 ORDER BY rowid DESC LIMIT 1"
        )
        assert row["input_tokens"] == 100 and row["output_tokens"] == 20
        assert row["thought_tokens"] == 30 and row["estimated_amount"] == 411

        # An explicit protocol can use GenerateContent with the selected global model.
        before_count = len(vertex_calls)
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="generate_content",
                vertex_hydrogen_model="gemini-2.5-pro",
                vertex_fallback_to_gemini=False,
            )
        )
        diary_out = await gate.generate(
            {"public_data": "今日总结"},
            "diary-sub",
            scenario=Scenario.DAILY_DIARY,
            images=[],
        )
        assert json.loads(diary_out)["bubbles"] == ["Vertex生成回答"]
        assert len(vertex_calls) == before_count + 1
        assert vertex_calls[-1][0].endswith("gemini-4-hydrogen:generateContent")
    finally:
        await _close_clients([dec_client, chat_client, *v_clients], g_transports + v_transports)


@pytest.mark.parametrize("model", ["hydrogen", "neon", "carbon"])
async def test_vertex_fallback_to_gemini_and_usage_metadata_helpers(
    settings, store, message, model
):
    settings.primary_model = model
    gemini_calls = []

    def gemini_handler(request):
        gemini_calls.append(request.url.path)
        if request.url.path.endswith("interactions"):
            assert json.loads(request.content)["model"] == "gemini-4-" + model
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _chat_json("Gemini兜底")
        return httpx.Response(200, json=resp)

    def failing_vertex_handler(request):
        assert "gemini-4-" + model in request.url.path
        return httpx.Response(503, json={"error": {"code": 503, "message": "vertex down"}})

    (dec_client, chat_client), g_transports = _client_pair(gemini_handler)
    v_client, v_clients, v_transports = _client_pair(failing_vertex_handler, vertex=True)
    try:
        gate = GeminiGateway(settings, store, dec_client, chat_client, vertex_chat_client=v_client)
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="generate_content",
                vertex_hydrogen_model="gemini-2.5-flash",
                vertex_fallback_to_gemini=True,
            )
        )
        out = await gate.generate({"current_message": "hi"}, "fb-sub", message=message)
        assert json.loads(out)["bubbles"] == ["Gemini兜底"]
        assert "/v1/interactions" in gemini_calls
        assert gate.blocked_until == 0.0

        # Disable fallback -> raises gemini_unavailable without calling Gemini chat_client
        before = len(gemini_calls)
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="generate_content",
                vertex_hydrogen_model="gemini-2.5-flash",
                vertex_fallback_to_gemini=False,
            )
        )
        with pytest.raises(Rejected, match="gemini_unavailable"):
            await gate.generate({"current_message": "hi"}, "fb-sub", message=message)
        assert len(gemini_calls) == before
    finally:
        await _close_clients([dec_client, chat_client, *v_clients], g_transports + v_transports)

    # Direct unit test on _usage and _actual with GenerateContentResponse.usage_metadata
    gc_resp = types.GenerateContentResponse(
        usage_metadata=types.GenerateContentResponseUsageMetadata(
            prompt_token_count=200,
            candidates_token_count=40,
            thoughts_token_count=10,
            cached_content_token_count=50,
            total_token_count=250,
        )
    )
    usage = GeminiGateway._usage(gc_resp, "hydrogen", thinking_level="medium")
    assert usage is not None
    assert (usage.input_tokens, usage.output_tokens, usage.thought_tokens, usage.cached_tokens) == (
        200,
        40,
        10,
        50,
    )
    assert GeminiGateway._actual(None, gc_resp, (Decimal("1.50"), Decimal("7.50"))) == Decimal(
        "0.000675"
    )
    # Missing thoughts and total -> None (never assume thinking is free when price[1] > 0)
    partial = SimpleNamespace(
        usage_metadata=SimpleNamespace(prompt_token_count=10, candidates_token_count=5)
    )
    assert GeminiGateway._actual(None, partial, (Decimal("1.50"), Decimal("7.50"))) is None
    assert GeminiGateway._actual(None, partial, (Decimal(0), Decimal(0))) == Decimal(0)


async def test_provider_admin_endpoints_crud_session_policy_and_secret_masking(settings):
    settings.admin_token = SecretStr(TOKEN)
    settings.owner_qq_id = "20000"
    app = create_app(settings, start_workers=False)
    if not any(getattr(r, "path", "").startswith("/admin/api/providers") for r in app.routes):
        app.include_router(provider_router)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            # 1. GET initial state
            res = await client.get("/admin/api/providers")
            assert res.status_code == 200
            data = res.json()
            assert data["source"] == "env"
            assert data["credentials"]["gemini_paid_configured"] is True
            assert data["credentials"]["gemini_free_configured"] is False
            owner_row = next(r for r in data["sessions_matrix"] if r["target_id"] == "20000")
            assert owner_row["is_owner"] is True and owner_row["effective_privacy"] == "high"
            grp_row = next(r for r in data["sessions_matrix"] if r["target_id"] == "40000")
            assert grp_row["effective_privacy"] == "low"

            # 2. Save credentials (free key + vertex key + SA JSON); never echoed in response
            sa_json = _make_sa_json(project_id="my-gcp-proj")
            cred_res = await client.post(
                "/admin/api/providers/credentials",
                json={
                    "gemini_free_api_key": "free-admin-secret-xyz987",
                    "vertex_api_key": "vertex-admin-secret-abc123",
                    "gcp_service_account_json": sa_json,
                },
            )
            assert cred_res.status_code == 200
            for secret in (
                "free-admin-secret-xyz987",
                "vertex-admin-secret-abc123",
                "BEGIN PRIVATE KEY",
            ):
                assert secret not in cred_res.text
            c_info = cred_res.json()["credentials"]
            assert c_info["gemini_free_configured"] is True
            assert c_info["gemini_free_source"] == "sqlite"
            assert c_info["vertex_api_key_configured"] is True
            assert c_info["gcp_service_account_configured"] is True
            assert c_info["gcp_service_account_project"] == "my-gcp-proj"
            assert "free-admin-secret-xyz987" in app.state.service.secrets
            assert app.state.service.ai.free_decision_client is not None

            # 3. Session policy override -> reflected in matrix
            sp_res = await client.post(
                "/admin/api/qq/privacy",
                json={
                    "kind": "group",
                    "platform": "onebot",
                    "target_id": "40000",
                    "privacy": "high",
                },
            )
            assert sp_res.status_code == 200
            grp_row = next(r for r in sp_res.json()["sessions"] if r["target_id"] == "40000")
            assert grp_row["effective_privacy"] == "high"
            assert grp_row["effective_decision_tier"] == "paid"
            assert (
                "credentials" not in sp_res.json() and "override_hydrogen_provider" not in grp_row
            )

            # 4. PUT /admin/api/providers without group_privacy keeps per-session override
            put_res = await client.put(
                "/admin/api/providers",
                json={
                    "default_group_privacy": "low",
                    "default_private_privacy": "high",
                    "decision_context_tokens": 2200,
                    "free_tier_fallback_to_paid": True,
                    "vertex_enabled": True,
                    "vertex_auth_mode": "service_account",
                    "vertex_project": "my-gcp-proj",
                    "vertex_location": "global",
                    "vertex_hydrogen_model": "gemini-4-hydrogen",
                    "vertex_protocol": "auto",
                    "vertex_fallback_to_gemini": True,
                },
            )
            assert put_res.status_code == 200
            assert put_res.json()["config"]["decision_context_tokens"] == 2200
            assert put_res.json()["config"]["group_privacy"] == {"onebot:40000": "high"}
            assert app.state.service.ai.vertex_chat_client is not None

            # 5. Clear individual credential & DELETE resets config
            clr = await client.post(
                "/admin/api/providers/credentials",
                json={"clear_gemini_free_api_key": True},
            )
            assert clr.status_code == 200
            assert clr.json()["credentials"]["gemini_free_configured"] is False
            assert clr.json()["credentials"]["vertex_api_key_configured"] is True

            del_res = await client.delete("/admin/api/providers")
            assert del_res.status_code == 200
            assert del_res.json()["config"]["group_privacy"] == {"onebot:40000": "high"}
            assert del_res.json()["config"]["decision_context_tokens"] == 2500

            # 6. Validation errors (bad SA JSON, unknown key) and missing auth rejected cleanly
            bad_cred = await client.post(
                "/admin/api/providers/credentials",
                json={"gcp_service_account_json": '{"type":"authorized_user"}'},
            )
            assert bad_cred.status_code == 400
            bad_cfg = await client.put("/admin/api/providers", json={"unknown_key": 1})
            assert bad_cfg.status_code == 400
            unauth = await client.get("/admin/api/providers", headers={"Authorization": ""})
            assert unauth.status_code == 401


async def test_vertex_multimodal_max_tokens_downgrade_and_by_provider_ledger(
    settings, store, message
):
    vertex_bodies = []
    attempt = [0]

    def gemini_handler(request):
        return httpx.Response(200, json=decision_response())

    def vertex_handler(request):
        body = json.loads(request.content)
        vertex_bodies.append(body)
        attempt[0] += 1
        finish = "MAX_TOKENS" if attempt[0] == 1 else "STOP"
        chunk = {
            "candidates": [
                {
                    "content": {"role": "model", "parts": [{"text": _chat_json("看清图片啦")}]},
                    "finishReason": finish,
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 300,
                "candidatesTokenCount": 30,
                "thoughtsTokenCount": 10,
                "cachedContentTokenCount": 0,
                "totalTokenCount": 340,
            },
        }
        sse = f"data: {json.dumps(chunk)}\n\n"
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=sse)

    (dec_client, chat_client), g_transports = _client_pair(gemini_handler)
    (free_client, free_unused), f_transports = _client_pair(gemini_handler)
    v_client, v_clients, v_transports = _client_pair(vertex_handler, vertex=True)
    try:
        gate = GeminiGateway(
            settings,
            store,
            dec_client,
            chat_client,
            free_decision_client=free_client,
            vertex_chat_client=v_client,
        )
        await gate.routing.set_config(
            ProviderRoutingConfig(
                vertex_enabled=True,
                vertex_protocol="generate_content",
                vertex_hydrogen_model="gemini-2.5-pro",
                vertex_fallback_to_gemini=False,
            )
        )
        # Group turn (low privacy -> gemini_free for output screen) + Vertex multimodal generation
        grp = group(message, addressed=True)
        out = await gate.generate(
            {"current_message": "看看图片"},
            "mm-sub",
            message=grp,
            images=[ModelImage("AQID")],
        )
        assert json.loads(out)["bubbles"] == ["看清图片啦"]
        # First attempt MAX_TOKENS downgraded once to lower thinking level ("medium" -> "low")
        assert len(vertex_bodies) == 2
        assert vertex_bodies[0]["generationConfig"]["thinkingConfig"]["thinking_level"] == "MEDIUM"
        assert vertex_bodies[1]["generationConfig"]["thinkingConfig"]["thinking_level"] == "LOW"
        parts = vertex_bodies[0]["contents"][0]["parts"]
        assert parts[1]["inlineData"] == {"data": "AQID", "mime_type": "image/jpeg"}

        # Owner private screen -> gemini_paid
        await gate.screen(
            {"current_message": "主人"}, "own-sub", purpose="message", message=message
        )

        by_provider = {
            r["provider"]: r for r in (await store.usage())["token_usage"]["today"]["by_provider"]
        }
        assert by_provider["gemini_free"]["calls"] == 1
        assert by_provider["gemini_free"]["charged_amount"] == 0
        assert by_provider["gemini_paid"]["calls"] == 1
        assert by_provider["vertex_paid"]["calls"] == 2
        assert by_provider["vertex_paid"]["input_tokens"] == 600
    finally:
        await _close_clients(
            [dec_client, chat_client, free_client, free_unused, *v_clients],
            g_transports + f_transports + v_transports,
        )


async def test_qq_privacy_controls_are_independent_of_provider_controls(admin_api):
    app, client, _ = admin_api
    routing = app.state.service.store.runtime.provider_routing
    path = "/admin/api/qq/privacy"
    original = (await client.get(path)).json()
    assert set(original) == {"defaults", "sessions"}
    high = {"default_group_privacy": "high", "default_private_privacy": "low"}
    assert (await client.put(path, json=high)).json()["defaults"] == high
    one = {"kind": "group", "platform": "onebot", "target_id": "40000", "privacy": "low"}
    assert (await client.post(path, json=one)).is_success
    assert (await client.post(path, json={**one, "hydrogen_provider": "vertex"})).status_code == 400
    # Existing consoles may have cached the retired selector; it never changes the global route.
    old = await client.post(
        "/admin/api/providers/session-policy", json={**one, "hydrogen_provider": "vertex"}
    )
    assert old.is_success
    group_row = next(r for r in old.json()["sessions_matrix"] if r["key"] == "onebot:40000")
    assert group_row["effective_hydrogen_provider"] == routing.hydrogen_provider_for()
    await client.put("/admin/api/providers", json={"vertex_enabled": True})
    await client.delete("/admin/api/providers")
    saved = (await client.get(path)).json()
    assert saved["defaults"] == high
    assert next(r for r in saved["sessions"] if r["is_owner"])["effective_privacy"] == "high"
    assert (
        next(r for r in saved["sessions"] if r["key"] == "onebot:40000")["effective_privacy"]
        == "low"
    )
    cleared = (await client.post(path, json={**one, "privacy": "default"})).json()
    assert (
        next(r for r in cleared["sessions"] if r["key"] == "onebot:40000")["effective_privacy"]
        == "high"
    )


async def test_global_admin_works_without_loading_qq_sessions(admin_api, monkeypatch):
    app, client, _ = admin_api
    routing = app.state.service.store.runtime.provider_routing
    root = "/admin/api/global/providers"
    privacy_path = "/admin/api/qq/privacy"
    policy = {"kind": "group", "platform": "onebot", "target_id": "40000", "privacy": "high"}
    saved = (await client.post(privacy_path, json=policy)).json()
    privacy_fields = {
        "default_group_privacy",
        "default_private_privacy",
        "group_privacy",
        "private_privacy",
    }

    async def unavailable():
        pytest.fail("Global admin accessed the QQ conversation list")

    with monkeypatch.context() as patch:
        patch.setattr(routing, "_sessions_matrix", unavailable)
        dashboard = await client.get("/admin/api/global/dashboard")
        assert dashboard.is_success
        assert {"onebot", "onebot_detail"}.isdisjoint(dashboard.json())
        assert privacy_fields.isdisjoint(dashboard.json()["provider_routing"])
        for response in (
            await client.get(root),
            await client.put(root, json={"vertex_enabled": True}),
            await client.post(root + "/credentials", json={"clear": True}),
            await client.delete(root),
        ):
            assert response.is_success
            data = response.json()
            assert "sessions_matrix" not in data
            assert "decision_context_tokens" in data["config"]
            assert privacy_fields.isdisjoint(data["config"])
            assert privacy_fields.isdisjoint(data["environment"])
        assert (await client.put(root, json={"group_privacy": {}})).status_code == 400
        assert (await client.get(root, headers={"Authorization": ""})).status_code == 401
    assert (await client.get(privacy_path)).json() == saved
