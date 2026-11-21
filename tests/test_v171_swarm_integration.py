"""v1.7.1 end-to-end swarm integration tests across routing, billing, admin UI, and Service."""

import base64
import hashlib
import io
import json
import re
import time
from decimal import Decimal
from urllib.parse import parse_qs

import httpx
import pytest
from PIL import Image
from pydantic import SecretStr

from hakimi.app import create_app
from hakimi.context_budget import estimate_tokens
from hakimi.gcp_billing import BIGQUERY_SCOPE, MONITORING_SCOPE, TOKEN_URI, GcpBillingConfig
from hakimi.gemini import GeminiGateway, questions
from hakimi.models import ImageRef
from hakimi.provider_routing import ProviderCredentialsUpdate, ProviderRoutingConfig
from hakimi.service import Service
from tests.fakes import FakeBili, FakeGateway
from tests.test_admin import TOKEN
from tests.test_gateway import group
from tests.test_gcp_billing_and_monitoring import make_sa_info
from tests.test_provider_routing_and_vertex import _chat_json, _client_pair, _close_clients
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_streaming_v0100 import ActionSender


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (64, 64), "teal").save(buf, format="PNG")
    return buf.getvalue()


async def test_admin_page_csp_hashes_and_security_invariants(admin_frontend, settings):
    settings.admin_token = SecretStr(TOKEN)
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            res = await client.get("/admin")
            assert res.status_code == 200
            html = res.text
            assert html.splitlines()[0].strip() == "<!doctype html>"
            styles = re.findall(r"<style>(.*?)</style>", html, re.S)
            scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
            assert len(styles) == 1 and len(scripts) == 1
            style_hash = base64.b64encode(hashlib.sha256(styles[0].encode()).digest()).decode()
            script_hash = base64.b64encode(hashlib.sha256(scripts[0].encode()).digest()).decode()
            csp = res.headers["content-security-policy"]
            assert f"'sha256-{style_hash}'" in csp
            assert f"'sha256-{script_hash}'" in csp
            assert not re.search(r"\sstyle\s*=", html)
            assert not re.search(r"\son[a-z]+\s*=", html, re.I)
            assert "innerHTML" not in html
            assert "unpkg.com" not in html
            assert html.count("v2.1.0") >= 2
            for marker in (
                'id="nav-api-routing"',
                'id="api-routing"',
                'id="provider-ledger"',
                'id="overview-routing-gcp"',
                "memory-mirror-save",
                "双轨记忆治理",
                "cache-alert",
            ):
                assert marker in html


async def test_mounted_routes_dashboard_and_worker_liveness(settings):
    settings.admin_token = SecretStr(TOKEN)
    sa = make_sa_info(project_id="int-proj-1")
    watermark_ts = 1_760_000_000.0

    def gcp_handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "tok-int", "expires_in": 3600})
        if request.url.path == "/bigquery/v2/projects/int-proj-1/queries":
            body = json.loads(request.content.decode())
            if "UNNEST(credits) AS c" in body["query"] and "GROUP BY" in body["query"]:
                return httpx.Response(
                    200,
                    json={
                        "jobComplete": True,
                        "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                        "rows": [{"f": [{"v": "Free Trial"}, {"v": "25.0000"}]}],
                    },
                )
            return httpx.Response(
                200,
                json={
                    "jobComplete": True,
                    "schema": {
                        "fields": [
                            {"name": "gross_cost_usd"},
                            {"name": "net_cost_usd"},
                            {"name": "latest_usage_end_ts"},
                            {"name": "currency"},
                        ]
                    },
                    "rows": [
                        {
                            "f": [
                                {"v": "25.0000"},
                                {"v": "0.0000"},
                                {"v": str(int(watermark_ts))},
                                {"v": "USD"},
                            ]
                        }
                    ],
                },
            )
        assert request.url.path == "/v3/projects/int-proj-1/timeSeries"
        return httpx.Response(200, json={"timeSeries": []})

    app = create_app(settings, gateway_factory=FakeGateway, start_workers=True)
    async with app.router.lifespan_context(app):
        app.state.gcp_transport = httpx.MockTransport(gcp_handler)
        svc = app.state.service
        assert any(t.get_name() == "gcp_billing" and not t.done() for t in svc.tasks)

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers={"Authorization": "Bearer " + TOKEN},
        ) as client:
            health = await client.get("/healthz")
            assert health.status_code == 200 and health.json() == {"status": "ok"}

            # Routes are mounted by create_app without manual include_router calls.
            prov = await client.get("/admin/api/providers")
            assert prov.status_code == 200
            gcp = await client.get("/admin/api/gcp-billing")
            assert gcp.status_code == 200

            # Upload shared SA JSON + keys via /admin/api/providers/credentials
            cred = await client.post(
                "/admin/api/providers/credentials",
                json={
                    "gemini_free_api_key": "free-int-secret-111",
                    "vertex_api_key": "vertex-int-secret-222",
                    "gcp_service_account_json": json.dumps(sa),
                },
            )
            assert cred.status_code == 200
            assert "free-int-secret-111" in svc.secrets
            svc.refresh_secrets()
            assert "vertex-int-secret-222" in svc.secrets

            # Shared SA automatically resolves in /admin/api/gcp-billing
            gcp_after = (await client.get("/admin/api/gcp-billing")).json()
            assert gcp_after["credentials"]["service_account_configured"] is True
            assert gcp_after["credentials"]["service_account_source"] == "provider_credentials"

            await client.put(
                "/admin/api/providers",
                json={
                    "default_group_privacy": "low",
                    "default_private_privacy": "high",
                    "decision_context_tokens": 2300,
                    "free_tier_fallback_to_paid": True,
                    "vertex_enabled": True,
                    "vertex_auth_mode": "service_account",
                    "vertex_project": "int-proj-1",
                    "vertex_location": "global",
                    "vertex_hydrogen_model": "gemini-4-hydrogen",
                    "vertex_protocol": "auto",
                    "vertex_fallback_to_gemini": True,
                },
            )
            await client.put(
                "/admin/api/gcp-billing",
                json={
                    "bigquery_enabled": True,
                    "billing_project": "int-proj-1",
                    "billing_table": "billing_ds.export_tbl",
                    "initial_credits_usd": "300",
                    "credit_grants": [{"name_match": "Free Trial", "initial_usd": "300"}],
                    "monitoring_enabled": True,
                    "monitoring_project": "int-proj-1",
                },
            )
            assert (await client.post("/admin/api/gcp-billing/sync-bigquery")).status_code == 200
            assert (await client.post("/admin/api/gcp-billing/sync-monitoring")).status_code == 200

            await svc.store.runtime.event("gcp_bigquery_sync_failed", "system")
            await svc.store.runtime.event("gcp_monitoring_sync_failed", "system")

            dash_res = await client.get("/admin/api/dashboard")
            assert dash_res.status_code == 200
            dash = dash_res.json()
            assert dash["provider_routing"]["decision_context_tokens"] == 2300
            assert dash["provider_routing"]["vertex_enabled"] is True
            assert dash["budget_status"]["gcp_billing"]["synced"] is True
            assert dash["budget_status"]["gcp_billing"]["settled_remaining_usd"] == pytest.approx(
                275.0
            )
            assert "by_provider" in dash["usage"]["token_usage"]["today"]
            metric_codes = {m["code"] for m in dash["metrics"]}
            assert {"gcp_bigquery_sync_failed", "gcp_monitoring_sync_failed"} <= metric_codes


async def test_e2e_group_free_vs_owner_private_vertex_and_billing_reconciliation(
    settings, store, message, monkeypatch
):
    now = time.time()
    watermark_ts = now - 600
    sa = make_sa_info(project_id="e2e-proj-1")

    paid_decision_calls = []
    free_decision_calls = []
    gemini_chat_calls = []
    vertex_calls = []

    def paid_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.url.path.endswith("decisions"):
            paid_decision_calls.append(body)
            return httpx.Response(200, json=decision_response())
        gemini_chat_calls.append(body)
        resp = interaction_response()
        resp["steps"][0]["content"][0]["text"] = _chat_json("Gemini备用")
        return httpx.Response(200, json=resp)

    def free_handler(request: httpx.Request) -> httpx.Response:
        free_decision_calls.append(json.loads(request.content))
        return httpx.Response(200, json=decision_response())

    def vertex_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        vertex_calls.append((request.url.path, body))
        resp = interaction_response()
        resp["id"] = f"vtx-{len(vertex_calls)}"
        resp["steps"][0]["content"][0]["text"] = _chat_json(f"Vertex回复{len(vertex_calls)}")
        resp["usage"] = {
            "total_input_tokens": 100,
            "total_output_tokens": 20,
            "total_thought_tokens": 30,
            "total_cached_tokens": 40,
            "total_tokens": 150,
        }
        return httpx.Response(200, json=resp)

    def gcp_handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            assertion = parse_qs(request.content.decode())["assertion"][0]
            from google.auth import jwt

            claims = jwt.decode(assertion, verify=False)
            assert claims["scope"] in {BIGQUERY_SCOPE, MONITORING_SCOPE}
            tok = "bq-tok" if claims["scope"] == BIGQUERY_SCOPE else "mon-tok"
            return httpx.Response(200, json={"access_token": tok, "expires_in": 3600})
        if request.url.path == "/bigquery/v2/projects/e2e-proj-1/queries":
            body = json.loads(request.content.decode())
            if "UNNEST(credits) AS c" in body["query"] and "GROUP BY" in body["query"]:
                return httpx.Response(
                    200,
                    json={
                        "jobComplete": True,
                        "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                        "rows": [{"f": [{"v": "Free Trial"}, {"v": "50.0000"}]}],
                    },
                )
            return httpx.Response(
                200,
                json={
                    "jobComplete": True,
                    "schema": {
                        "fields": [
                            {"name": "gross_cost_usd"},
                            {"name": "net_cost_usd"},
                            {"name": "latest_usage_end_ts"},
                            {"name": "currency"},
                        ]
                    },
                    "rows": [
                        {
                            "f": [
                                {"v": "50.0000"},
                                {"v": "0.0000"},
                                {"v": str(int(watermark_ts))},
                                {"v": "USD"},
                            ]
                        }
                    ],
                },
            )
        assert request.url.path == "/v3/projects/e2e-proj-1/timeSeries"
        assert request.headers["authorization"] == "Bearer mon-tok"
        # Baseline Monitoring payload (200 in, 40 out, 60 thought, 80 cached)
        return httpx.Response(
            200,
            json={
                "timeSeries": [
                    {
                        "metric": {"labels": {"type": "input"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "200"}}],
                    },
                    {
                        "metric": {"labels": {"type": "output"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "40"}}],
                    },
                    {
                        "metric": {"labels": {"type": "thought"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "60"}}],
                    },
                    {
                        "metric": {"labels": {"type": "cached"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "80"}}],
                    },
                ]
            },
        )

    (dec_client, chat_client), g_transports = _client_pair(paid_handler)
    (free_client, free_unused), f_transports = _client_pair(free_handler)
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
        sender = ActionSender()
        svc = Service(settings, store, gate, FakeBili(), sender)

        await store.runtime.provider_routing.update_credentials(
            ProviderCredentialsUpdate(
                gemini_free_api_key="free-e2e-key-1234",
                vertex_api_key="vertex-e2e-key-5678",
                gcp_service_account_json=json.dumps(sa),
            )
        )
        await store.runtime.provider_routing.set_config(
            ProviderRoutingConfig(
                default_group_privacy="low",
                default_private_privacy="high",
                decision_context_tokens=2500,
                vertex_enabled=True,
                vertex_hydrogen_model="gemini-4-hydrogen",
                vertex_protocol="auto",
            )
        )
        svc.refresh_secrets()
        assert "free-e2e-key-1234" in svc.secrets

        store.runtime.gcp_billing.set_transport(httpx.MockTransport(gcp_handler))
        await store.runtime.gcp_billing.set_config(
            GcpBillingConfig(
                bigquery_enabled=True,
                billing_project="e2e-proj-1",
                billing_table="billing_ds.export_v1",
                initial_credits_usd=Decimal("300"),
                monitoring_enabled=True,
                monitoring_project="e2e-proj-1",
            )
        )

        # Verify 30 short relevance items cap cleanly at 24 and <= 2500 tokens.
        probe_msg = group(message, event_id="cap-probe", addressed=True)
        await gate.screen(
            {"current_message": "测试上限"},
            "cap-sub",
            purpose="message",
            message=probe_msg,
            relevance_context=[f"短条{i}" for i in range(30)],
        )
        probe_input = free_decision_calls[-1]["input"]
        probe_ctx = json.loads(
            probe_input.split("<readonly_recent_group_context>")[1].split(
                "</readonly_recent_group_context>"
            )[0]
        )
        assert len(probe_ctx) == 24
        assert probe_ctx[0] == "短条6" and probe_ctx[-1] == "短条29"
        assert (
            estimate_tokens(probe_input)
            + estimate_tokens(json.dumps(questions("message"), ensure_ascii=False))
            <= 2500
        )
        free_decision_calls.clear()

        # Verify private turn with disabled/empty session does NOT inject relevance_context.
        dm_disabled = message.model_copy(
            update={"event_id": "dm-disabled", "text": "在吗", "created_at": now}
        )
        assert await svc.accept(dm_disabled) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()
        assert len(paid_decision_calls) == 2  # inbound + output
        assert (
            "<readonly_recent_group_context>[]</readonly_recent_group_context>"
            in paid_decision_calls[0]["input"]
        )
        paid_decision_calls.clear()
        vertex_calls.clear()

        # 1. Group message (`low` privacy) with neighbour image -> all Decisions Free Tier
        monkeypatch.setattr("hakimi.gateway.time.time", lambda: now)
        for idx in range(26):
            await store.runtime.filters.remember_group(
                group(
                    message,
                    event_id=f"hist-{idx}",
                    text=f"群历史{idx}",
                    created_at=now - 100 + idx,
                )
            )

        async def fake_napcat(_ref):
            return _png_bytes(), []

        monkeypatch.setattr(svc.images, "_napcat", fake_napcat)
        bystander_img = group(
            message,
            event_id="near-img-1",
            user_id="30001",
            addressed=False,
            text="",
            command_text="",
            images=[ImageRef(file="near-img-file")],
            created_at=now - 2,
        )
        await store.groups.observe(bystander_img)

        grp_msg = group(
            message,
            event_id="grp-turn-1",
            user_id="30000",
            addressed=True,
            text="帮我看看前面这张图",
            created_at=now,
        )
        assert await svc.accept(grp_msg) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()

        # Inbound message + neighbour_images source + output screen -> 3 Free Tier calls, 0 Paid!
        assert len(free_decision_calls) == 3
        assert len(paid_decision_calls) == 0
        assert len(vertex_calls) == 1
        grp_ctx = json.loads(
            free_decision_calls[0]["input"]
            .split("<readonly_recent_group_context>")[1]
            .split("</readonly_recent_group_context>")[0]
        )
        # remember_group keeps 24 rows (23 historical + grp_msg, excluded from group_context)
        assert len(grp_ctx) == 23
        assert grp_ctx[-1] == "群历史25"
        assert grp_ctx[0] == "群历史3"
        assert "群历史2" not in grp_ctx

        # 2. Owner private (`high` privacy) with session history (~2500 tokens + redaction)
        owner_base = message.model_copy(
            update={"event_id": "owner-base", "text": "开启记忆", "created_at": now + 10}
        )
        await svc.sessions.enable(owner_base)
        for idx in range(18):
            user_txt = (
                f"私聊历史{idx}-api_key=sk-supersecret1234567890-" + ("记" * 160)
                if idx == 10
                else f"私聊历史{idx}-" + ("聊" * 160)
            )
            await svc.sessions.remember(owner_base, user_txt, f"喵答复{idx}-" + ("嗯" * 40))

        owner_msg = message.model_copy(
            update={"event_id": "owner-turn-1", "text": "继续刚才的话题", "created_at": now + 20}
        )
        assert await svc.accept(owner_msg) == "accepted"
        await svc.process(await store.claim())
        assert await svc.deliver_once()

        # Owner private inbound + output screen -> Paid Tier; Hydrogen -> Vertex AI!
        assert len(paid_decision_calls) == 2
        assert len(free_decision_calls) == 3  # unchanged
        assert len(vertex_calls) == 2
        assert len(gemini_chat_calls) == 0
        inbound_owner_input = paid_decision_calls[0]["input"]
        assert "sk-supersecret1234567890" not in inbound_owner_input
        assert "私聊历史" in inbound_owner_input
        assert (
            estimate_tokens(inbound_owner_input)
            + estimate_tokens(json.dumps(questions("message"), ensure_ascii=False))
            <= 2500
        )

        # Verify audit records capture privacy_level and decision_tier
        audits = await store.runtime.filters.recent_audit(limit=5)
        owner_audit = audits[0]
        grp_audit = audits[1]
        assert owner_audit["privacy_level"] == "high" and owner_audit["decision_tier"] == "paid"
        assert grp_audit["privacy_level"] == "low" and grp_audit["decision_tier"] == "free"

        # 3. Verify provider_usage_aggregates + BigQuery + Cloud Monitoring reconciliation
        usage = await store.usage()
        by_prov = {r["provider"]: r for r in usage["token_usage"]["today"]["by_provider"]}
        assert by_prov["gemini_free"]["calls"] == 4  # 1 cap probe + 3 group turn screens
        assert by_prov["gemini_free"]["charged_amount"] == 0
        assert by_prov["gemini_paid"]["calls"] >= 4
        assert by_prov["gemini_paid"]["charged_amount"] > 0
        # 3 total vertex_paid turns: dm_disabled + grp_msg + owner_msg
        assert by_prov["vertex_paid"]["calls"] == 3
        assert by_prov["vertex_paid"]["input_tokens"] == 300

        bq_status = await store.runtime.gcp_billing.sync_bigquery()
        bq = bq_status["bigquery"]
        assert bq["credits_used_usd"] == pytest.approx(50.0)
        assert bq["settled_remaining_usd"] == pytest.approx(250.0)
        expected_unsettled = (await store.paid_spend_since(watermark_ts)) / 1_000_000
        assert bq["unsettled_local_usd"] == pytest.approx(expected_unsettled)
        assert bq["realtime_remaining_usd"] == pytest.approx(250.0 - expected_unsettled)

        # First Monitoring sync returns 300 tokens vs local 450 tokens -> delta 150
        mon_lag = await store.runtime.gcp_billing.sync_monitoring()
        assert mon_lag["monitoring"]["delta_tokens"] == 150
        assert mon_lag["monitoring"]["local_vertex_paid"]["total_tokens"] == 450

        # Now update Monitoring response to exact 450 tokens -> in_sync!
        def synced_gcp_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v3/projects/e2e-proj-1/timeSeries":
                return httpx.Response(
                    200,
                    json={
                        "timeSeries": [
                            {
                                "metric": {"labels": {"type": "input"}},
                                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                                "points": [{"value": {"int64Value": "300"}}],
                            },
                            {
                                "metric": {"labels": {"type": "output"}},
                                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                                "points": [{"value": {"int64Value": "60"}}],
                            },
                            {
                                "metric": {"labels": {"type": "thought"}},
                                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                                "points": [{"value": {"int64Value": "90"}}],
                            },
                            {
                                "metric": {"labels": {"type": "cached"}},
                                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                                "points": [{"value": {"int64Value": "120"}}],
                            },
                        ]
                    },
                )
            return gcp_handler(request)

        store.runtime.gcp_billing.set_transport(httpx.MockTransport(synced_gcp_handler))
        mon_synced = (await store.runtime.gcp_billing.sync_monitoring())["monitoring"]
        assert mon_synced["status"] == "in_sync"
        assert mon_synced["delta_tokens"] == 0
        assert mon_synced["official"]["total_tokens"] == 450
        assert mon_synced["local_all_providers"]["total_tokens"] > 450
        await svc.stop()
    finally:
        await _close_clients(
            [dec_client, chat_client, free_client, free_unused, *v_clients],
            g_transports + f_transports + v_transports,
        )
