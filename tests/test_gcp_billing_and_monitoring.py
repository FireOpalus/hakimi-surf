"""Tests for GCP BigQuery billing sync, Cloud Monitoring, provider ledger, and admin API."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from google.auth import jwt
from pydantic import SecretStr

import hakimi.store as store_module
from hakimi.app import create_app
from hakimi.gcp_admin import router as gcp_admin_router
from hakimi.gcp_billing import (
    BIGQUERY_SCOPE,
    MONITORING_SCOPE,
    TOKEN_URI,
    CreditGrantRule,
    GcpBillingConfig,
    GcpBillingCredentialsUpdate,
    GcpBillingManager,
    validate_bigquery_table,
    validate_service_account_json,
)
from hakimi.security import Rejected
from hakimi.store import Store
from hakimi.usage_accounting import TokenUsage
from tests.conftest import cached_rsa_private_key_pem
from tests.fakes import FakeGateway
from tests.test_admin import TOKEN


def make_sa_info(
    *,
    project_id: str = "my-gcp-project",
    client_email: str = "billing@my-gcp-project.iam.gserviceaccount.com",
    token_uri: str = TOKEN_URI,
    account_type: str = "service_account",
    private_key: str | None = None,
) -> dict:
    if private_key is None:
        private_key = cached_rsa_private_key_pem()
    return {
        "type": account_type,
        "project_id": project_id,
        "private_key_id": "test-key-id-1",
        "private_key": private_key,
        "client_email": client_email,
        "client_id": "123456789012345678901",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": token_uri,
    }


@pytest.mark.parametrize(
    "raw,default_proj,expected",
    [
        (
            "my-gcp-project.billing_ds.gcp_billing_export_v1_0123AB",
            "",
            "my-gcp-project.billing_ds.gcp_billing_export_v1_0123AB",
        ),
        (
            "billing_ds.gcp_billing_export_v1",
            "my-gcp-project",
            "my-gcp-project.billing_ds.gcp_billing_export_v1",
        ),
        (
            "legacy-proj:sub.ds_1.tbl_2",
            "",
            "legacy-proj:sub.ds_1.tbl_2",
        ),
    ],
)
def test_validate_bigquery_table_accepts_valid_identifiers(raw, default_proj, expected):
    assert validate_bigquery_table(raw, default_project=default_proj) == expected


@pytest.mark.parametrize(
    "raw,default_proj",
    [
        ("", "my-gcp-project"),
        ("ds.tbl", ""),
        ("`my-gcp-project.ds.tbl`", ""),
        ("my-gcp-project.ds.tbl ", ""),
        (" my-gcp-project.ds.tbl", ""),
        ("my-gcp-project.ds.tbl; DROP TABLE x--", ""),
        ("my-gcp-project.ds.tbl` UNION SELECT 1", ""),
        ("my-gcp-project.ds.tbl WHERE 1=1", ""),
        ("a.ds.tbl", ""),
        ("Bad_Project.ds.tbl", ""),
        ("my-gcp-project.ds-bad.tbl", ""),
        ("my-gcp-project.ds.tbl-bad", ""),
        ("my-gcp-project.ds.tbl.extra", ""),
        ("ds.tbl", "bad_proj"),
    ],
)
def test_validate_bigquery_table_rejects_injection_and_invalid_names(raw, default_proj):
    with pytest.raises(Rejected, match="invalid_bigquery_table"):
        validate_bigquery_table(raw, default_project=default_proj)


def test_validate_service_account_json_strict_checks():
    valid = make_sa_info()
    parsed = validate_service_account_json(json.dumps(valid))
    assert parsed["client_email"] == valid["client_email"]

    for bad in (
        "not json",
        json.dumps([1, 2, 3]),
        json.dumps(make_sa_info(account_type="authorized_user")),
        json.dumps(make_sa_info(token_uri="https://attacker.invalid/token")),
        json.dumps(make_sa_info(client_email="user@gmail.com")),
        json.dumps(make_sa_info(private_key="not-a-pem-key")),
        json.dumps({**valid, "subject": "admin@example.com"}),
    ):
        with pytest.raises(Rejected, match="gcp_credentials_invalid"):
            validate_service_account_json(bad)

    ed_key = (
        ed25519.Ed25519PrivateKey.generate()
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode("utf-8")
    )
    with pytest.raises(Rejected, match="gcp_credentials_invalid"):
        validate_service_account_json(make_sa_info(private_key=ed_key))

    oversized = json.dumps({**valid, "padding": "x" * 70000})
    with pytest.raises(Rejected, match="gcp_credentials_invalid"):
        validate_service_account_json(oversized)


async def test_provider_usage_aggregates_recovery_and_prune(settings, monkeypatch):
    class FixedDate(datetime):
        current = datetime(2000, 1, 1, 10, 0, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.current if tz is None else cls.current.astimezone(tz)

    monkeypatch.setattr(store_module, "datetime", FixedDate)
    store = Store(settings)
    await store.open()
    try:
        r_paid = await store.reserve("u1", "chat", Decimal("0.05"))
        await store.settle(
            r_paid,
            Decimal("0.02"),
            usage=TokenUsage(
                1000, 100, cached_tokens=200, thought_tokens=50, model_lane="hydrogen"
            ),
        )
        r_free = await store.reserve("u1", "decision", Decimal("0"), provider="gemini_free")
        await store.settle(
            r_free,
            Decimal("0"),
            usage=TokenUsage(500, 20, cached_tokens=0, thought_tokens=0, model_lane="decision"),
            provider="gemini_free",
        )
        r_vertex = await store.reserve(
            "u1", "chat", Decimal("0.10"), model_lane="hydrogen", provider="vertex_paid"
        )
        await store.settle(
            r_vertex,
            Decimal("0.03"),
            usage=TokenUsage(
                2000, 300, cached_tokens=400, thought_tokens=80, model_lane="hydrogen"
            ),
        )
        with pytest.raises(Rejected, match="invalid_provider"):
            await store.reserve("u1", "chat", Decimal("0.01"), provider="invalid_prov")

        # Open vertex reservation before simulated crash.
        crashed = await store.reserve(
            "u1", "chat", Decimal("0.04"), model_lane="hydrogen", provider="vertex_paid"
        )
        assert crashed
    finally:
        await store.close()

    # Reopen and verify crash recovery attributes crashed row to vertex_paid.
    recovered = Store(settings)
    await recovered.open()
    try:
        usage = await recovered.usage()
        for label in ("today", "month", "lifetime"):
            by_prov = {r["provider"]: r for r in usage["token_usage"][label]["by_provider"]}
            assert set(by_prov) == {"gemini_free", "gemini_paid", "vertex_paid"}
            assert by_prov["gemini_paid"]["calls"] == 1
            assert by_prov["gemini_paid"]["charged_amount"] == 20_000
            assert by_prov["gemini_paid"]["input_tokens"] == 1000
            assert by_prov["gemini_free"]["calls"] == 1
            assert by_prov["gemini_free"]["charged_amount"] == 0
            assert by_prov["gemini_free"]["input_tokens"] == 500
            assert by_prov["vertex_paid"]["calls"] == 2  # settled + crash-recovered
            assert by_prov["vertex_paid"]["input_tokens"] == 2000
            assert by_prov["vertex_paid"]["output_tokens"] == 300
            assert by_prov["vertex_paid"]["cached_tokens"] == 400
            assert by_prov["vertex_paid"]["thought_tokens"] == 80
            assert by_prov["vertex_paid"]["charged_amount"] > 30_000

        # Prune rows >32d old; ledger_provider orphaned rows delete while aggregates survive.
        await recovered.prune()
        assert (await recovered.one("SELECT COUNT(*) FROM ledger"))[0] == 0
        assert (await recovered.one("SELECT COUNT(*) FROM ledger_provider"))[0] == 0
        life_usage = await recovered.usage()
        life_prov = {r["provider"]: r for r in life_usage["token_usage"]["lifetime"]["by_provider"]}
        assert life_prov["vertex_paid"]["calls"] == 2
    finally:
        await recovered.close()


async def test_credential_resolution_fallback_and_override(store):
    sa_shared = make_sa_info(
        project_id="shared-project",
        client_email="shared@shared-project.iam.gserviceaccount.com",
    )
    sa_dedicated = make_sa_info(
        project_id="dedicated-project",
        client_email="dedicated@dedicated-project.iam.gserviceaccount.com",
    )
    async with store.transaction():
        await store.db.execute(
            "INSERT INTO admin_config VALUES ('provider_credentials',?)",
            (
                store.vault.seal(
                    "admin:provider_credentials",
                    {"gcp_service_account_json": json.dumps(sa_shared)},
                ),
            ),
        )
    mgr = GcpBillingManager(store)
    await mgr.initialize()
    st = await mgr.status()
    assert st["credentials"] == {
        "service_account_configured": True,
        "service_account_source": "provider_credentials",
        "service_account_email": "shared@shared-project.iam.gserviceaccount.com",
        "service_account_project": "shared-project",
    }
    assert "private_key" not in json.dumps(st)

    st2 = await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa_dedicated))
    )
    assert st2["credentials"]["service_account_source"] == "gcp_billing"
    assert (
        st2["credentials"]["service_account_email"]
        == "dedicated@dedicated-project.iam.gserviceaccount.com"
    )
    assert "private_key" not in json.dumps(st2)

    st3 = await mgr.update_credentials(GcpBillingCredentialsUpdate(clear=True))
    assert st3["credentials"]["service_account_source"] == "provider_credentials"
    assert (
        st3["credentials"]["service_account_email"]
        == "shared@shared-project.iam.gserviceaccount.com"
    )


async def test_bigquery_sync_polling_grants_unsettled_delta_and_hard_stop(store):
    sa = make_sa_info(project_id="billing-proj-1")
    table = "billing-proj-1.billing_export.gcp_billing_export_v1_ABC"
    watermark_ts = 1_760_000_000.0
    now_ts = watermark_ts + 120.0

    # Raise daily/user caps so local test spend isn't blocked before GCP credit exhaustion.
    store.s.daily_usd = Decimal("1000")
    store.s.monthly_usd = Decimal("1000")
    store.s.total_usd = Decimal("1000")
    store.s.user_daily_usd = Decimal("1000")

    # Seed pre-watermark settled call (must NOT count in unsettled), in-flight reserved call
    # post-watermark (must NOT count), search call post-watermark (must NOT count),
    # and post-watermark settled call of $5.25 (MUST count).
    rid_old = await store.reserve("u1", "chat", Decimal("1.00"))
    await store.settle(rid_old, Decimal("0.80"))
    async with store.transaction():
        await store.db.execute(
            "UPDATE ledger_times SET created=? WHERE id=?", (watermark_ts - 60, rid_old)
        )
    rid_reserved = await store.reserve("u1", "chat", Decimal("2.00"))
    async with store.transaction():
        await store.db.execute(
            "UPDATE ledger_times SET created=? WHERE id=?", (now_ts, rid_reserved)
        )
    rid_search = await store.reserve("u1", "search", Decimal("0"))
    await store.settle(rid_search, Decimal("0"))
    async with store.transaction():
        await store.db.execute("UPDATE ledger_times SET created=? WHERE id=?", (now_ts, rid_search))
    rid_new = await store.reserve("u1", "chat", Decimal("10.00"), provider="vertex_paid")
    await store.settle(rid_new, Decimal("5.25"))
    async with store.transaction():
        await store.db.execute("UPDATE ledger_times SET created=? WHERE id=?", (now_ts, rid_new))

    assert await store.paid_spend_since(watermark_ts) == 5_250_000

    seen_requests: list[httpx.Request] = []
    poll_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal poll_count
        seen_requests.append(request)
        if str(request.url) == TOKEN_URI:
            assertion = parse_qs(request.content.decode())["assertion"][0]
            claims = jwt.decode(assertion, verify=False)
            assert claims["scope"] == BIGQUERY_SCOPE
            assert claims["aud"] == TOKEN_URI
            assert "sub" not in claims
            return httpx.Response(200, json={"access_token": "bq-token-1", "expires_in": 3600})
        assert request.headers["authorization"] == "Bearer bq-token-1"
        if request.method == "POST":
            assert request.url.path == "/bigquery/v2/projects/billing-proj-1/queries"
            body = json.loads(request.content.decode())
            assert body["useLegacySql"] is False
            sql = body["query"]
            assert f"`{table}`" in sql
            if "UNNEST(credits) AS c" in sql and "GROUP BY" in sql:
                # Return incomplete job first to exercise polling + pagination.
                return httpx.Response(
                    200,
                    json={
                        "jobComplete": False,
                        "jobReference": {
                            "projectId": "billing-proj-1",
                            "jobId": "job-credits-1",
                            "location": "US",
                        },
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
                                {"v": "110.5000"},
                                {"v": "2.0000"},
                                {"v": str(int(watermark_ts))},
                                {"v": "USD"},
                            ]
                        }
                    ],
                },
            )
        assert request.method == "GET"
        assert request.url.path == "/bigquery/v2/projects/billing-proj-1/queries/job-credits-1"
        poll_count += 1
        if poll_count == 1:
            return httpx.Response(
                200,
                json={
                    "jobComplete": True,
                    "jobReference": {"projectId": "billing-proj-1", "jobId": "job-credits-1"},
                    "pageToken": "page-2",
                    "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                    "rows": [
                        {"f": [{"v": "Free Trial Credit"}, {"v": "100.0000"}]},
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "jobComplete": True,
                "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                "rows": [
                    {"f": [{"v": "GenAI App Builder"}, {"v": "8.5000"}]},
                    {"f": [{"v": "Sustained Use Discount"}, {"v": "1.5000"}]},
                ],
            },
        )

    mgr = GcpBillingManager(store, transport=httpx.MockTransport(handler))
    await mgr.initialize()
    store.runtime.gcp_billing = mgr
    await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
    )
    await mgr.set_config(
        GcpBillingConfig(
            bigquery_enabled=True,
            billing_project="billing-proj-1",
            billing_table="billing_export.gcp_billing_export_v1_ABC",
            initial_credits_usd=Decimal("0"),
            credit_grants=[
                CreditGrantRule(name_match="Free Trial", initial_usd=Decimal("100")),
                CreditGrantRule(name_match="GenAI App Builder", initial_usd=Decimal("20")),
            ],
            enforce_credit_hard_stop=True,
        )
    )
    st = await mgr.sync_bigquery()
    bq = st["bigquery"]
    assert bq["synced"] is True
    assert bq["full_table"] == table
    assert bq["currency"] == "USD"
    assert bq["gross_cost_usd"] == pytest.approx(110.5)
    assert bq["net_cost_usd"] == pytest.approx(2.0)
    assert bq["initial_credits_usd"] == pytest.approx(120.0)
    assert bq["credits_used_usd"] == pytest.approx(110.0)
    assert bq["settled_remaining_usd"] == pytest.approx(10.0)
    assert bq["unsettled_local_usd"] == pytest.approx(5.25)
    assert bq["realtime_remaining_usd"] == pytest.approx(4.75)
    assert bq["exhausted"] is False

    by_credit = {c["credit_name"]: c for c in bq["credits"]}
    assert by_credit["Free Trial Credit"]["initial_usd"] == pytest.approx(100.0)
    assert by_credit["Free Trial Credit"]["remaining_usd"] == pytest.approx(0.0)
    assert by_credit["GenAI App Builder"]["initial_usd"] == pytest.approx(20.0)
    assert by_credit["GenAI App Builder"]["remaining_usd"] == pytest.approx(11.5)
    assert by_credit["Sustained Use Discount"]["initial_usd"] is None
    assert by_credit["Sustained Use Discount"]["remaining_usd"] is None

    assert (await store.budget_status())["hard_stop"] is False
    assert await store.can_reserve("u1", "chat", Decimal("0.10")) is True

    # Spend another $4.75 locally -> realtime_remaining_usd becomes 0.0 -> hard stop triggers!
    rid_exhaust = await store.reserve("u1", "chat", Decimal("5.00"))
    await store.settle(rid_exhaust, Decimal("4.75"))
    async with store.transaction():
        await store.db.execute(
            "UPDATE ledger_times SET created=? WHERE id=?", (now_ts + 10, rid_exhaust)
        )

    bstatus = await store.budget_status()
    assert bstatus["hard_stop"] is True
    assert bstatus["gcp_billing"]["exhausted"] is True
    assert bstatus["gcp_billing"]["realtime_remaining_usd"] == pytest.approx(0.0)
    assert await store.can_reserve("u1", "chat", Decimal("0.01")) is False
    with pytest.raises(Rejected, match="budget_exhausted") as exc_info:
        await store.reserve("u1", "chat", Decimal("0.01"))
    assert exc_info.value.detail == "gcp_credit_exhausted"

    # Free tier calls (amount == 0) must STILL succeed even when GCP credits are exhausted!
    free_rid = await store.reserve("u1", "decision", Decimal("0"), provider="gemini_free")
    await store.settle(free_rid, Decimal("0"), provider="gemini_free")


async def test_monitoring_sync_parsing_and_drift_detection(store):
    sa = make_sa_info(project_id="mon-proj-1")
    # Local vertex_paid today: 1000 input + 200 output + 50 thought = 1250 total (300 cached)
    v_rid = await store.reserve("u1", "chat", Decimal("0.10"), provider="vertex_paid")
    await store.settle(
        v_rid,
        Decimal("0.01"),
        usage=TokenUsage(1000, 200, cached_tokens=300, thought_tokens=50, model_lane="hydrogen"),
    )
    # Local gemini_paid & gemini_free must NOT pollute vertex_paid monitoring comparison
    g_rid = await store.reserve("u1", "chat", Decimal("0.10"), provider="gemini_paid")
    await store.settle(g_rid, Decimal("0.01"), usage=TokenUsage(5000, 500, model_lane="hydrogen"))

    series_payload = [
        [
            {
                "metric": {"labels": {"type": "input"}},
                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                "points": [{"value": {"int64Value": "600"}}, {"value": {"int64Value": "400"}}],
            },
            {
                "metric": {"labels": {"type": "output"}},
                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                "points": [{"value": {"doubleValue": 200.0}}],
            },
        ],
        [
            {
                "metric": {"labels": {"type": "thought"}},
                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                "points": [{"value": {"int64Value": "50"}}],
            },
            {
                "metric": {"labels": {"type": "cached"}},
                "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                "points": [{"value": {"int64Value": "300"}}],
            },
        ],
    ]

    mode = {"idx": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            assertion = parse_qs(request.content.decode())["assertion"][0]
            claims = jwt.decode(assertion, verify=False)
            assert claims["scope"] == MONITORING_SCOPE
            return httpx.Response(200, json={"access_token": "mon-token-1", "expires_in": 3600})
        assert request.url.path == "/v3/projects/mon-proj-1/timeSeries"
        assert request.headers["authorization"] == "Bearer mon-token-1"
        assert "token_count" in request.url.params["filter"]
        assert request.url.params["aggregation.alignmentPeriod"] == "3600s"
        assert request.url.params["aggregation.perSeriesAligner"] == "ALIGN_SUM"
        assert request.url.params["aggregation.crossSeriesReducer"] == "REDUCE_SUM"
        assert request.url.params.get_list("aggregation.groupByFields") == [
            "metric.labels.type",
            "resource.labels.model_user_id",
        ]
        assert request.url.params["view"] == "FULL"
        if mode["idx"] == 0:
            page = request.url.params.get("pageToken", "")
            if not page:
                return httpx.Response(
                    200, json={"timeSeries": series_payload[0], "nextPageToken": "p2"}
                )
            return httpx.Response(200, json={"timeSeries": series_payload[1]})
        if mode["idx"] == 1:
            # Official is 1100 vs local 1250 -> delta +150 (drift_ratio=0.12) -> normal_lag
            return httpx.Response(
                200,
                json={
                    "timeSeries": [
                        {
                            "metric": {"labels": {"type": "input"}},
                            "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                            "points": [{"value": {"int64Value": "900"}}],
                        },
                        {
                            "metric": {"labels": {"type": "output"}},
                            "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                            "points": [{"value": {"int64Value": "200"}}],
                        },
                    ]
                },
            )
        # Official is 2500 vs local 1250 -> drift_detected
        return httpx.Response(
            200,
            json={
                "timeSeries": [
                    {
                        "metric": {"labels": {"type": "input"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "2500"}}],
                    }
                ]
            },
        )

    mgr = GcpBillingManager(store, transport=httpx.MockTransport(handler))
    await mgr.initialize()
    await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
    )
    await mgr.set_config(GcpBillingConfig(monitoring_enabled=True, monitoring_project="mon-proj-1"))

    st = await mgr.sync_monitoring()
    mon = st["monitoring"]
    assert mon["synced"] is True
    assert mon["status"] == "in_sync"
    assert mon["delta_tokens"] == 0
    assert mon["drift_ratio"] == 0.0
    assert mon["official"]["input_tokens"] == 1000
    assert mon["official"]["output_tokens"] == 200
    assert mon["official"]["thought_tokens"] == 50
    assert mon["official"]["cached_tokens"] == 300
    assert mon["official"]["total_tokens"] == 1250
    assert mon["official"]["by_model"][0]["model"] == "gemini-4-hydrogen"
    assert mon["local_vertex_paid"]["total_tokens"] == 1250
    assert mon["local_all_providers"]["total_tokens"] == 6750

    mode["idx"] = 1
    st_lag = await mgr.sync_monitoring()
    assert st_lag["monitoring"]["status"] == "normal_lag"
    assert st_lag["monitoring"]["delta_tokens"] == 150

    mode["idx"] = 2
    st_drift = await mgr.sync_monitoring()
    assert st_drift["monitoring"]["status"] == "drift_detected"
    assert st_drift["monitoring"]["delta_tokens"] == -1250


async def test_tick_respects_intervals_and_handles_upstream_failures(store):
    now = {"t": 10_000.0}
    calls = {"bq": 0, "mon": 0, "fail": False}
    sa = make_sa_info(project_id="tick-proj-1")

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        if calls["fail"]:
            return httpx.Response(503, json={"error": "unavailable"})
        if "/bigquery/" in str(request.url):
            calls["bq"] += 1
            return httpx.Response(200, json={"jobComplete": True, "rows": []})
        calls["mon"] += 1
        return httpx.Response(200, json={"timeSeries": []})

    mgr = GcpBillingManager(store, transport=httpx.MockTransport(handler), clock=lambda: now["t"])
    await mgr.initialize()
    await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
    )
    await mgr.set_config(
        GcpBillingConfig(
            bigquery_enabled=True,
            billing_project="tick-proj-1",
            billing_table="ds.tbl",
            bigquery_sync_interval_seconds=600,
            monitoring_enabled=True,
            monitoring_project="tick-proj-1",
            monitoring_sync_interval_seconds=120,
        )
    )
    await mgr.tick()
    assert calls["bq"] == 2 and calls["mon"] == 1
    now["t"] += 60
    await mgr.tick()
    assert calls["bq"] == 2 and calls["mon"] == 1
    now["t"] += 70  # 130s elapsed -> monitoring due, bigquery not due
    await mgr.tick()
    assert calls["bq"] == 2 and calls["mon"] == 2
    now["t"] += 600
    calls["fail"] = True
    await mgr.tick()  # Must not raise
    assert mgr.summary_for_budget()["last_error"] == "gcp_billing_request_failed"


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/admin/api/gcp-billing"),
        ("PUT", "/admin/api/gcp-billing"),
        ("DELETE", "/admin/api/gcp-billing"),
        ("POST", "/admin/api/gcp-billing/credentials"),
        ("POST", "/admin/api/gcp-billing/sync-bigquery"),
        ("POST", "/admin/api/gcp-billing/sync-monitoring"),
    ],
)
async def test_gcp_admin_auth_guards(settings, method, path):
    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app.include_router(gcp_admin_router)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:12141"
        ) as client:
            r_unconf = await client.request(method, path, json={})
            assert r_unconf.status_code == 403

    settings.admin_token = SecretStr(TOKEN)
    app2 = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app2.include_router(gcp_admin_router)
    async with app2.router.lifespan_context(app2):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app2), base_url="http://127.0.0.1:12141"
        ) as client:
            r_bad = await client.request(
                method, path, headers={"Authorization": "Bearer wrong"}, json={"unknown": 1}
            )
            assert r_bad.status_code == 401
            assert not await app2.state.service.store.all("SELECT * FROM admin_config")


async def test_gcp_admin_endpoints_crud_sync_and_restart_persistence(settings):
    settings.admin_token = SecretStr(TOKEN)
    sa = make_sa_info(project_id="api-proj-1")
    headers = {"Authorization": "Bearer " + TOKEN}

    def gcp_handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "api-tok", "expires_in": 3600})
        if request.url.path == "/bigquery/v2/projects/api-proj-1/queries":
            body = json.loads(request.content.decode())
            if "UNNEST(credits) AS c" in body["query"] and "GROUP BY" in body["query"]:
                return httpx.Response(
                    200,
                    json={
                        "jobComplete": True,
                        "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                        "rows": [{"f": [{"v": "Free Trial"}, {"v": "12.5000"}]}],
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
                    "rows": [{"f": [{"v": "12.5"}, {"v": "0"}, {"v": "1760000000"}, {"v": "USD"}]}],
                },
            )
        assert request.url.path == "/v3/projects/api-proj-1/timeSeries"
        return httpx.Response(
            200,
            json={
                "timeSeries": [
                    {
                        "metric": {"labels": {"type": "input"}},
                        "resource": {"labels": {"model_user_id": "gemini-4-hydrogen"}},
                        "points": [{"value": {"int64Value": "400"}}],
                    }
                ]
            },
        )

    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app.include_router(gcp_admin_router)
    async with app.router.lifespan_context(app):
        app.state.gcp_transport = httpx.MockTransport(gcp_handler)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers=headers,
        ) as client:
            init = await client.get("/admin/api/gcp-billing")
            assert init.status_code == 200
            data = init.json()
            assert data["source"] == "env"
            assert data["credentials"]["service_account_configured"] is False

            # Reject invalid inputs atomically
            assert (await client.put("/admin/api/gcp-billing", json={})).status_code == 400
            assert (
                await client.put("/admin/api/gcp-billing", json={"unknown": 1})
            ).status_code == 400
            assert (
                await client.put(
                    "/admin/api/gcp-billing", json={"billing_table": "proj.ds.tbl; DROP"}
                )
            ).status_code == 400
            assert (
                await client.post(
                    "/admin/api/gcp-billing/credentials",
                    json={"gcp_service_account_json": "not-json"},
                )
            ).status_code == 409

            # Save credentials & partial config updates
            cred = await client.post(
                "/admin/api/gcp-billing/credentials",
                json={"gcp_service_account_json": json.dumps(sa)},
            )
            assert cred.status_code == 200
            assert cred.json()["credentials"]["service_account_configured"] is True
            assert "private_key" not in cred.text

            saved_bq = await client.put(
                "/admin/api/gcp-billing",
                json={
                    "bigquery_enabled": True,
                    "billing_project": "api-proj-1",
                    "billing_table": "ds.tbl",
                    "initial_credits_usd": "300",
                    "credit_grants": [{"name_match": "Free Trial", "initial_usd": "300"}],
                    "enforce_credit_hard_stop": True,
                },
            )
            assert saved_bq.status_code == 200
            assert saved_bq.json()["source"] == "admin"

            # Partial update for monitoring preserves BigQuery fields
            saved_mon = await client.put(
                "/admin/api/gcp-billing",
                json={"monitoring_enabled": True, "monitoring_project": "api-proj-1"},
            )
            assert saved_mon.status_code == 200
            assert saved_mon.json()["config"]["bigquery_enabled"] is True
            assert saved_mon.json()["config"]["monitoring_enabled"] is True

            bq_resp = await client.post("/admin/api/gcp-billing/sync-bigquery")
            assert bq_resp.status_code == 200
            assert bq_resp.json()["bigquery"]["credits_used_usd"] == pytest.approx(12.5)
            assert bq_resp.json()["bigquery"]["realtime_remaining_usd"] == pytest.approx(287.5)

            mon_resp = await client.post("/admin/api/gcp-billing/sync-monitoring")
            assert mon_resp.status_code == 200
            assert mon_resp.json()["monitoring"]["official"]["input_tokens"] == 400
            assert mon_resp.json()["monitoring"]["status"] == "drift_detected"

    # Restart app and verify config, credentials, and snapshots survive encrypted in SQLite
    app2 = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app2.include_router(gcp_admin_router)
    async with app2.router.lifespan_context(app2):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app2),
            base_url="http://127.0.0.1:12141",
            headers=headers,
        ) as client:
            restored = (await client.get("/admin/api/gcp-billing")).json()
            assert restored["source"] == "admin"
            assert restored["credentials"]["service_account_configured"] is True
            assert restored["bigquery"]["synced"] is True
            assert restored["bigquery"]["credits_used_usd"] == pytest.approx(12.5)
            assert restored["monitoring"]["synced"] is True
            assert restored["monitoring"]["official"]["input_tokens"] == 400

            reset = (await client.delete("/admin/api/gcp-billing")).json()
            assert reset["source"] == "env"
            assert reset["config"]["bigquery_enabled"] is False


async def test_integration_with_provider_routing_and_env_defaults_and_bq_errors(settings):
    from hakimi.provider_routing import ProviderCredentialsUpdate, ProviderRoutingManager

    s = settings.model_copy(
        update={
            "gcp_billing_project": "env-proj-1",
            "gcp_billing_table": "env_ds.env_tbl",
            "vertex_project": "env-vtx-1",
        }
    )
    sa = make_sa_info(project_id="shared-sa-proj")
    store = Store(s)
    await store.open()
    try:
        routing = ProviderRoutingManager(store)
        await routing.initialize()
        store.runtime.provider_routing = routing
        await routing.update_credentials(
            ProviderCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
        )

        mgr = GcpBillingManager(store)
        await mgr.initialize()
        store.runtime.gcp_billing = mgr
        st = await mgr.status()
        assert st["config"]["billing_project"] == "env-proj-1"
        assert st["config"]["billing_table"] == "env_ds.env_tbl"
        assert st["config"]["monitoring_project"] == "env-vtx-1"
        assert st["credentials"]["service_account_source"] == "provider_credentials"
        assert st["credentials"]["service_account_email"] == sa["client_email"]

        # Verify BigQuery 200 response with errors array raises cleanly without losing prior state
        def err_handler(request: httpx.Request) -> httpx.Response:
            if str(request.url) == TOKEN_URI:
                return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
            return httpx.Response(200, json={"jobComplete": True, "errors": [{"reason": "bad"}]})

        with pytest.raises(Rejected, match="gcp_billing_request_failed"):
            await mgr.sync_bigquery(transport=httpx.MockTransport(err_handler))
        assert mgr.summary_for_budget()["last_error"] == "gcp_billing_request_failed"

        # Verify good sync followed by lowering initial_credits_usd triggers hard stop immediately
        def ok_handler(request: httpx.Request) -> httpx.Response:
            if str(request.url) == TOKEN_URI:
                return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
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
                    "rows": [{"f": [{"v": "50.0"}, {"v": "0"}, {"v": "1760000000"}, {"v": "USD"}]}],
                },
            )

        await mgr.set_config(
            GcpBillingConfig(
                bigquery_enabled=True,
                billing_project="env-proj-1",
                billing_table="env_ds.env_tbl",
                initial_credits_usd=Decimal("100"),
                enforce_credit_hard_stop=True,
            )
        )
        ok_st = await mgr.sync_bigquery(transport=httpx.MockTransport(ok_handler))
        assert ok_st["bigquery"]["exhausted"] is False
        assert mgr.hard_stop_exhausted() is False

        lowered = await mgr.set_config(
            GcpBillingConfig(
                bigquery_enabled=True,
                billing_project="env-proj-1",
                billing_table="env_ds.env_tbl",
                initial_credits_usd=Decimal("40"),
                enforce_credit_hard_stop=True,
            )
        )
        assert lowered["bigquery"]["exhausted"] is True
        assert mgr.hard_stop_exhausted() is True
    finally:
        await store.close()


async def test_gcp_admin_reconciliation_states_and_read_only_safety(settings):
    """Verify /admin/api/gcp-billing attaches read-only reconciliation across all statuses."""
    settings.admin_token = SecretStr(TOKEN)
    settings.daily_usd = Decimal("1000")
    settings.monthly_usd = Decimal("1000")
    settings.total_usd = Decimal("1000")
    settings.user_daily_usd = Decimal("1000")
    sa = make_sa_info(project_id="rec-proj-1")
    headers = {"Authorization": "Bearer " + TOKEN}
    watermark_ts = 1_760_000_000.0
    bq_state = {"gross": "10.0000", "used": "10.0000", "net": "0.0000"}

    def gcp_handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "rec-tok", "expires_in": 3600})
        body = json.loads(request.content.decode())
        if "UNNEST(credits) AS c" in body["query"] and "GROUP BY" in body["query"]:
            return httpx.Response(
                200,
                json={
                    "jobComplete": True,
                    "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                    "rows": [{"f": [{"v": "Free Trial"}, {"v": bq_state["used"]}]}],
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
                            {"v": bq_state["gross"]},
                            {"v": bq_state["net"]},
                            {"v": str(int(watermark_ts))},
                            {"v": "USD"},
                        ]
                    }
                ],
            },
        )

    app = create_app(settings, gateway_factory=FakeGateway, start_workers=False)
    app.include_router(gcp_admin_router)
    async with app.router.lifespan_context(app):
        app.state.gcp_transport = httpx.MockTransport(gcp_handler)
        store = app.state.service.store

        # Seed $10.00 settled pre-watermark spend on vertex_paid
        rid1 = await store.reserve("u1", "chat", Decimal("12.00"), provider="vertex_paid")
        await store.settle(rid1, Decimal("10.00"), provider="vertex_paid")
        async with store.transaction():
            await store.db.execute(
                "UPDATE ledger_times SET created=? WHERE id=?", (watermark_ts - 120, rid1)
            )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:12141",
            headers=headers,
        ) as client:
            # 1. Before BigQuery sync -> not_synced, still exposes local reference baseline.
            before = (await client.get("/admin/api/gcp-billing")).json()
            assert "reconciliation" in before
            rec0 = before["reconciliation"]
            assert rec0["status"] == "not_synced"
            assert rec0["billing_actual_usd"] is None
            assert rec0["cost_delta_usd"] is None
            assert rec0["local_charged_usd"] == pytest.approx(10.0)
            assert rec0["local_reference_usd"] == pytest.approx(10.0)
            assert rec0["local_vertex_charged_usd"] == pytest.approx(10.0)
            assert "private_key" not in json.dumps(before)

            # Configure SA + BigQuery
            assert (
                await client.post(
                    "/admin/api/gcp-billing/credentials",
                    json={"gcp_service_account_json": json.dumps(sa)},
                )
            ).status_code == 200
            assert (
                await client.put(
                    "/admin/api/gcp-billing",
                    json={
                        "bigquery_enabled": True,
                        "billing_project": "rec-proj-1",
                        "billing_table": "ds.tbl",
                        "initial_credits_usd": "100",
                    },
                )
            ).status_code == 200

            # 2. BigQuery gross == $10.00 vs local $10.00 -> in_sync
            sync_in = (await client.post("/admin/api/gcp-billing/sync-bigquery")).json()
            rec1 = sync_in["reconciliation"]
            assert rec1["status"] == "in_sync"
            assert rec1["billing_actual_usd"] == pytest.approx(10.0)
            assert rec1["cost_delta_usd"] == pytest.approx(0.0)
            assert rec1["cost_delta_ratio"] == pytest.approx(0.0)
            assert rec1["credit_utilization_ratio"] == pytest.approx(0.1)

            # 3. Add $4.00 post-watermark unsettled local spend -> export_lag
            rid2 = await store.reserve("u1", "chat", Decimal("5.00"), provider="vertex_paid")
            await store.settle(rid2, Decimal("4.00"), provider="vertex_paid")
            async with store.transaction():
                await store.db.execute(
                    "UPDATE ledger_times SET created=? WHERE id=?", (watermark_ts + 300, rid2)
                )
            lag_view = (await client.get("/admin/api/gcp-billing")).json()
            rec2 = lag_view["reconciliation"]
            assert rec2["status"] == "export_lag"
            assert rec2["cost_delta_usd"] == pytest.approx(-4.0)
            assert rec2["billing_plus_unsettled_usd"] == pytest.approx(14.0)
            assert rec2["cost_delta_after_unsettled_usd"] == pytest.approx(0.0)

            # 4. BigQuery jumps to $30.00 (other GCP services / drift) -> drift_detected
            bq_state["gross"] = "30.0000"
            bq_state["used"] = "28.0000"
            bq_state["net"] = "2.0000"
            drift_view = (await client.post("/admin/api/gcp-billing/sync-bigquery")).json()
            rec3 = drift_view["reconciliation"]
            assert rec3["status"] == "drift_detected"
            assert rec3["billing_actual_usd"] == pytest.approx(30.0)
            assert rec3["billing_net_usd"] == pytest.approx(2.0)
            assert rec3["cost_delta_usd"] == pytest.approx(16.0)
            assert rec3["cost_delta_ratio"] > 0.5
            assert rec3["credit_utilization_ratio"] == pytest.approx(0.28)
            assert "private_key" not in json.dumps(drift_view)
