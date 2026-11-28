"""Tests for Drive v3 metadata ETag synthesis, MemoryMirror integration, and unified balance."""

import hashlib
import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from hakimi.drive_client import UPLOAD, FolderDrive
from hakimi.gcp_billing import (
    TOKEN_URI,
    GcpBillingConfig,
    GcpBillingCredentialsUpdate,
    GcpBillingManager,
)
from hakimi.memory_mirror import MemoryMirror, render
from hakimi.security import Rejected
from hakimi.session_manager import SessionManager
from hakimi.store import Store
from tests.fakes import FakeGateway
from tests.test_admin import admin_api as admin_api
from tests.test_drive_client import FILE, FOLDER, drive_for, file_info, folder_info
from tests.test_dual_memory_v100 import populate
from tests.test_gateway import group
from tests.test_gcp_billing_and_monitoring import make_sa_info


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


async def test_drive_v3_metadata_etag_synthesis_read_and_patch_without_http_etag(settings):
    scope = "c" * 64
    filename = "group-memory-" + scope[:24] + ".md"
    content_v1 = "# Memory\n- likes tea\n"
    md5_v1 = _md5(content_v1).upper()
    item = file_info(
        name=filename,
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [item]})
        if request.method == "GET" and request.url.params.get("alt") == "media":
            assert "If-Match" not in request.headers
            return httpx.Response(200, text=content_v1)
        if request.method == "GET":
            fields = request.url.params["fields"]
            assert "md5Checksum" in fields
            assert "version" in fields
            assert "headRevisionId" in fields
            assert request.url.params["supportsAllDrives"] == "true"
            return httpx.Response(
                200,
                json={
                    **item,
                    "size": str(len(content_v1.encode("utf-8"))),
                    "md5Checksum": md5_v1,
                    "version": "7",
                    "headRevisionId": "rev_abc-123",
                },
            )
        assert request.method == "PATCH"
        assert str(request.url).startswith(UPLOAD + "/" + FILE)
        assert "If-Match" not in request.headers
        return httpx.Response(200, json=item)

    drive, client = await drive_for(settings, handler)
    async with client:
        head = await drive.memory_head(scope)
        assert head is not None
        assert head["id"] == FILE
        assert head["etag"] == f"v3:7:rev_abc-123:{md5_v1.lower()}"
        assert await drive.read_memory(scope, head) == content_v1
        assert await drive.write_memory(scope, "# Memory\n- likes coffee\n", head) == FILE
    assert any(r.method == "PATCH" for r in seen)


@pytest.mark.parametrize(
    "extra_meta,expected_etag_prefix",
    [
        ({"version": "12"}, "v3:12::"),
        ({"headRevisionId": "rev-only_9"}, "v3::rev-only_9:"),
    ],
)
async def test_drive_v3_etag_synthesis_accepts_single_version_or_revision(
    settings, extra_meta, expected_etag_prefix
):
    scope = "d" * 64
    filename = "group-memory-" + scope[:24] + ".md"
    body = "hello drive"
    md5 = _md5(body)
    item = file_info(
        name=filename,
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [item]})
        return httpx.Response(
            200,
            json={**item, "size": str(len(body)), "md5Checksum": md5, **extra_meta},
        )

    drive, client = await drive_for(settings, handler)
    async with client:
        head = await drive.memory_head(scope)
        assert head is not None
        assert head["etag"] == expected_etag_prefix + md5


@pytest.mark.parametrize(
    "bad_meta",
    [
        {},
        {"md5Checksum": "0123456789abcdef0123456789abcdef"},
        {"version": "5", "headRevisionId": "rev1"},
        {"md5Checksum": "not-md5", "version": "5", "headRevisionId": "rev1"},
        {
            "md5Checksum": "0123456789abcdef0123456789abcdef",
            "version": "abc",
            "headRevisionId": "rev1",
        },
        {
            "md5Checksum": "0123456789abcdef0123456789abcdef",
            "version": "5",
            "headRevisionId": "bad:rev",
        },
    ],
)
async def test_drive_v3_missing_or_invalid_metadata_still_requires_etag(settings, bad_meta):
    scope = "e" * 64
    item = file_info(
        name="group-memory-" + scope[:24] + ".md",
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [item]})
        return httpx.Response(200, json={**item, "size": "10", **bad_meta})

    drive, client = await drive_for(settings, handler)
    async with client:
        with pytest.raises(Rejected, match="drive_etag_required"):
            await drive.memory_head(scope)


async def test_drive_v3_detects_version_change_and_download_md5_mismatch(settings):
    scope = "f" * 64
    item = file_info(
        name="group-memory-" + scope[:24] + ".md",
        appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
    )
    body = "expected content"
    state = {"ver": "1", "download_text": body, "patch_called": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            return httpx.Response(200, json={"files": [item]})
        if request.method == "GET" and request.url.params.get("alt") == "media":
            return httpx.Response(200, text=state["download_text"])
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    **item,
                    "size": str(len(body)),
                    "md5Checksum": _md5(body),
                    "version": state["ver"],
                    "headRevisionId": "rev_1",
                },
            )
        state["patch_called"] = True
        return httpx.Response(200, json=item)

    drive, client = await drive_for(settings, handler)
    async with client:
        head = await drive.memory_head(scope)
        # Downloaded bytes corrupted / raced after metadata read -> MD5 mismatch
        state["download_text"] = "tampered content"
        with pytest.raises(Rejected, match="drive_etag_conflict"):
            await drive.read_memory(scope, head)

        # Remote version moved between head and write -> conflict, no PATCH sent
        state["download_text"] = body
        state["ver"] = "2"
        with pytest.raises(Rejected, match="drive_etag_conflict"):
            await drive.write_memory(scope, "new markdown", head)
    assert state["patch_called"] is False


async def test_memory_mirror_end_to_end_with_metadata_only_drive(settings, store, message):
    sessions = SessionManager(settings, store)
    msg = group(message)
    await populate(sessions, msg)
    scope = sessions.key(msg)
    filename = "group-memory-" + scope[:24] + ".md"
    drive_state: dict[str, object] = {"content": None, "ver": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/drive/v3/files/" + FOLDER:
            return httpx.Response(200, json=folder_info())
        if request.url.path == "/drive/v3/files":
            if drive_state["content"] is None:
                return httpx.Response(200, json={"files": []})
            item = file_info(
                name=filename,
                appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
            )
            return httpx.Response(200, json={"files": [item]})
        if request.method == "POST":
            raw = request.content.decode("utf-8")
            md = raw.split("Content-Type: text/markdown; charset=UTF-8\r\n\r\n", 1)[1].rsplit(
                "\r\n--", 1
            )[0]
            drive_state["content"] = md
            drive_state["ver"] = int(drive_state["ver"]) + 1
            return httpx.Response(
                200,
                json=file_info(
                    name=filename,
                    appProperties={
                        "hakimi_group_memory": "test-marker",
                        "hakimi_group_scope": scope,
                    },
                ),
            )
        if request.method == "GET" and request.url.params.get("alt") == "media":
            return httpx.Response(200, text=str(drive_state["content"]))
        if request.method == "GET":
            md = str(drive_state["content"])
            return httpx.Response(
                200,
                json=file_info(
                    name=filename,
                    appProperties={
                        "hakimi_group_memory": "test-marker",
                        "hakimi_group_scope": scope,
                    },
                    size=str(len(md.encode("utf-8"))),
                    md5Checksum=_md5(md),
                    version=str(drive_state["ver"]),
                    headRevisionId=f"rev-{drive_state['ver']}",
                ),
            )
        assert request.method == "PATCH"
        assert "If-Match" not in request.headers
        drive_state["content"] = request.content.decode("utf-8")
        drive_state["ver"] = int(drive_state["ver"]) + 1
        return httpx.Response(
            200,
            json=file_info(
                name=filename,
                appProperties={"hakimi_group_memory": "test-marker", "hakimi_group_scope": scope},
            ),
        )

    settings_drive = settings.model_copy(update={"gdrive_roaming_folder_id": FOLDER})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        drive = FolderDrive(settings_drive, client, "test-marker")

        async def token():
            return "test-token"

        drive._access_token = token
        svc = SimpleNamespace(
            s=settings,
            sessions=sessions,
            store=store,
            ai=FakeGateway(),
            drive=drive,
        )
        mirror = MemoryMirror(svc)
        view = await sessions.dual.view(msg)
        await mirror.configure(msg, True, view["version"])

        # Initial push creates the Drive markdown file
        pushed = await mirror.sync(msg, push=True)
        assert pushed == {"status": "pushed"}
        assert drive_state["ver"] == 1

        # Subsequent sync with no HTTP ETag header succeeds via v3 metadata synthesis
        unchanged = await mirror.sync(msg, push=True)
        assert unchanged == {"status": "unchanged"}

        # Human edit on Drive is pulled and screened cleanly
        state = await mirror.state(msg)
        edited = {k: v for k, v in state["entries"].items() if k.startswith("p:")}
        edited["p:30000:preferences"] = "喜欢绿茶与星图"
        drive_state["content"] = render(scope, edited)
        drive_state["ver"] = 2
        pulled = await mirror.sync(msg)
        assert pulled == {"status": "pulled"}
        assert (await mirror.state(msg))["last_error"] is None


async def _settle_paid_at(store: Store, usd: Decimal, ts: float, *, provider: str = "vertex_paid"):
    rid = await store.reserve("u1", "chat", usd + Decimal("1.00"), provider=provider)
    await store.settle(rid, usd)
    async with store.transaction():
        await store.db.execute("UPDATE ledger_times SET created=? WHERE id=?", (ts, rid))


def _make_bq_transport(
    *,
    table: str,
    credits_used: str,
    watermark_ts: float,
    project: str = "unified-proj-1",
):
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == TOKEN_URI:
            return httpx.Response(200, json={"access_token": "bq-tok", "expires_in": 3600})
        assert request.url.path == f"/bigquery/v2/projects/{project}/queries"
        body = json.loads(request.content.decode())
        assert f"`{table}`" in body["query"]
        if "UNNEST(credits) AS c" in body["query"] and "GROUP BY" in body["query"]:
            return httpx.Response(
                200,
                json={
                    "jobComplete": True,
                    "schema": {"fields": [{"name": "credit_name"}, {"name": "used_usd"}]},
                    "rows": [{"f": [{"v": "Free Trial"}, {"v": credits_used}]}],
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
                            {"v": credits_used},
                            {"v": "0.0000"},
                            {"v": str(int(watermark_ts))},
                            {"v": "USD"},
                        ]
                    }
                ],
            },
        )

    return httpx.MockTransport(handler)


async def test_unified_balance_four_modes_no_double_deduction_and_hard_stop_rescue(settings):
    sa = make_sa_info(project_id="unified-proj-1")
    table = "unified-proj-1.billing_ds.export_v1"
    watermark_ts = 1_760_000_000.0

    store = Store(settings)
    await store.open()
    try:
        store.s.daily_usd = Decimal("1000")
        store.s.monthly_usd = Decimal("1000")
        store.s.user_daily_usd = Decimal("1000")

        # 1. Mode "none"
        bal_none = await store.account_balance_status()
        assert bal_none == {
            "configured": False,
            "mode": "none",
            "remaining_microdollars": None,
            "remaining_usd": None,
            "set_microdollars": None,
            "set_usd": None,
            "spent_since_set_microdollars": 0,
            "updated_at": None,
            "manual_configured": False,
            "manual_remaining_usd": None,
            "gcp_synced": False,
            "gcp_settled_remaining_usd": None,
            "gcp_unsettled_local_usd": None,
            "gcp_realtime_remaining_usd": None,
            "exhausted": False,
        }

        # 2. Mode "manual"
        await store.set_account_balance(Decimal("50.00"))
        await _settle_paid_at(store, Decimal("1.25"), watermark_ts + 10)
        rid_search = await store.reserve("u1", "search", Decimal("0.50"))
        await store.settle(rid_search, Decimal("0.20"))
        bal_manual = await store.account_balance_status()
        assert bal_manual["configured"] is True
        assert bal_manual["mode"] == "manual"
        assert bal_manual["manual_configured"] is True
        assert bal_manual["manual_remaining_usd"] == "48.7500"
        assert bal_manual["remaining_microdollars"] == 48_750_000
        assert bal_manual["remaining_usd"] == "48.7500"
        assert bal_manual["spent_since_set_microdollars"] == 1_250_000
        assert bal_manual["gcp_synced"] is False
        assert bal_manual["exhausted"] is False

        # Clear manual and attach GCP -> Mode "gcp"
        await store.clear_account_balance()
        mgr = GcpBillingManager(
            store,
            transport=_make_bq_transport(
                table=table, credits_used="90.0000", watermark_ts=watermark_ts
            ),
        )
        await mgr.initialize()
        store.runtime.gcp_billing = mgr
        await mgr.update_credentials(
            GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
        )
        await mgr.set_config(
            GcpBillingConfig(
                bigquery_enabled=True,
                billing_project="unified-proj-1",
                billing_table="billing_ds.export_v1",
                initial_credits_usd=Decimal("100"),
                enforce_credit_hard_stop=True,
            )
        )
        await mgr.sync_bigquery()

        # Settled remaining = $100 - $90 = $10; unsettled local post-watermark = $1.25 -> $8.75
        bal_gcp = await store.account_balance_status()
        assert bal_gcp["configured"] is True
        assert bal_gcp["mode"] == "gcp"
        assert bal_gcp["manual_configured"] is False
        assert bal_gcp["manual_remaining_usd"] is None
        assert bal_gcp["set_microdollars"] is None
        assert bal_gcp["set_usd"] is None
        assert bal_gcp["gcp_synced"] is True
        assert bal_gcp["gcp_settled_remaining_usd"] == pytest.approx(10.0)
        assert bal_gcp["gcp_unsettled_local_usd"] == pytest.approx(1.25)
        assert bal_gcp["gcp_realtime_remaining_usd"] == pytest.approx(8.75)
        assert bal_gcp["remaining_microdollars"] == 8_750_000
        assert bal_gcp["remaining_usd"] == "8.7500"
        assert bal_gcp["exhausted"] is False

        # Spend $13.75 more locally -> GCP realtime = -$5.00 -> hard stop triggers!
        await _settle_paid_at(store, Decimal("13.75"), watermark_ts + 20)
        bal_gcp_exhausted = await store.account_balance_status()
        assert bal_gcp_exhausted["gcp_realtime_remaining_usd"] == pytest.approx(-5.0)
        assert bal_gcp_exhausted["remaining_microdollars"] == 0
        assert bal_gcp_exhausted["exhausted"] is True
        assert (await store.budget_status())["hard_stop"] is True
        with pytest.raises(Rejected, match="budget_exhausted") as exc_info:
            await store.reserve("u1", "chat", Decimal("0.01"))
        assert exc_info.value.detail == "gcp_credit_exhausted"

        # 4. Mode "merged": Add $25.00 manual cash -> rescues GCP exhaustion (-$5 + $25 = $20)!
        bal_merged = await store.set_account_balance(Decimal("25.00"))
        assert bal_merged["configured"] is True
        assert bal_merged["mode"] == "merged"
        assert bal_merged["manual_configured"] is True
        assert bal_merged["gcp_synced"] is True
        assert bal_merged["gcp_realtime_remaining_usd"] == pytest.approx(-5.0)
        assert bal_merged["remaining_microdollars"] == 20_000_000
        assert bal_merged["remaining_usd"] == "20.0000"
        assert bal_merged["exhausted"] is False
        bstatus = await store.budget_status()
        assert bstatus["hard_stop"] is False
        assert bstatus["gcp_billing"]["exhausted"] is False

        # Spend $3.00 locally -> must deduct $3.00 ONCE ($20 -> $17), NOT twice ($14)!
        await _settle_paid_at(store, Decimal("3.00"), watermark_ts + 30)
        bal_after = await store.account_balance_status()
        assert bal_after["mode"] == "merged"
        assert bal_after["manual_remaining_usd"] == "22.0000"
        assert bal_after["gcp_unsettled_local_usd"] == pytest.approx(18.0)
        assert bal_after["gcp_realtime_remaining_usd"] == pytest.approx(-8.0)
        assert bal_after["remaining_microdollars"] == 17_000_000
        assert bal_after["remaining_usd"] == "17.0000"
        assert bal_after["exhausted"] is False
    finally:
        await store.close()

    # Reopen store and verify merged state + rescue persists across restart
    reopened = Store(settings)
    await reopened.open()
    try:
        mgr2 = GcpBillingManager(reopened)
        await mgr2.initialize()
        reopened.runtime.gcp_billing = mgr2
        assert mgr2.hard_stop_exhausted() is False
        bal_reopened = await reopened.account_balance_status()
        assert bal_reopened["mode"] == "merged"
        assert bal_reopened["remaining_microdollars"] == 17_000_000

        # Spend remaining $17.00 -> unified hits $0 -> hard stop triggers again!
        await _settle_paid_at(reopened, Decimal("17.00"), watermark_ts + 40)
        bal_zero = await reopened.account_balance_status()
        assert bal_zero["remaining_microdollars"] == 0
        assert bal_zero["exhausted"] is True
        assert (await reopened.budget_status())["hard_stop"] is True
        with pytest.raises(Rejected, match="budget_exhausted") as exc_info:
            await reopened.reserve("u1", "chat", Decimal("0.01"))
        assert exc_info.value.detail == "gcp_credit_exhausted"

        # Clearing manual balance reverts cleanly to mode "gcp"
        bal_cleared = await reopened.clear_account_balance()
        assert bal_cleared["mode"] == "gcp"
        assert bal_cleared["manual_configured"] is False
        assert bal_cleared["exhausted"] is True
    finally:
        await reopened.close()


async def test_affection_budget_multiplier_hook_in_store_budget_refusal(store):
    store.s.daily_usd = Decimal("100")
    store.s.monthly_usd = Decimal("100")
    store.s.user_daily_usd = Decimal("1.00")

    multipliers = {
        "low-sub": Decimal("0.20"),
        "high-sub": Decimal("1.50"),
        "zero-sub": Decimal("0"),
    }
    store.runtime.affection = SimpleNamespace(
        budget_multiplier_for_subject=lambda subj: multipliers.get(subj, Decimal("1"))
    )

    assert await store.can_reserve("low-sub", "chat", Decimal("0.20")) is True
    with pytest.raises(Rejected, match="budget_exhausted") as low_exc:
        await store.reserve("low-sub", "chat", Decimal("0.21"))
    assert low_exc.value.detail == "user_daily"

    assert await store.can_reserve("high-sub", "chat", Decimal("1.50")) is True
    with pytest.raises(Rejected, match="budget_exhausted") as high_exc:
        await store.reserve("high-sub", "chat", Decimal("1.51"))
    assert high_exc.value.detail == "user_daily"

    assert await store.can_reserve("zero-sub", "decision", Decimal("0")) is True
    with pytest.raises(Rejected, match="budget_exhausted") as zero_exc:
        await store.reserve("zero-sub", "chat", Decimal("0.01"))
    assert zero_exc.value.detail == "user_daily"


async def test_admin_api_account_balance_unified_endpoint(admin_api):
    app, client, _ = admin_api
    store = app.state.service.store
    mgr = store.runtime.gcp_billing
    sa = make_sa_info(project_id="api-merged-1")
    table = "api-merged-1.ds.tbl"
    watermark_ts = 1_760_000_000.0

    mgr.set_transport(
        _make_bq_transport(
            table=table,
            credits_used="100.0000",
            watermark_ts=watermark_ts,
            project="api-merged-1",
        )
    )
    await mgr.update_credentials(
        GcpBillingCredentialsUpdate(gcp_service_account_json=json.dumps(sa))
    )
    await mgr.set_config(
        GcpBillingConfig(
            bigquery_enabled=True,
            billing_project="api-merged-1",
            billing_table="ds.tbl",
            initial_credits_usd=Decimal("100"),
            enforce_credit_hard_stop=True,
        )
    )
    await mgr.sync_bigquery()

    # Before manual balance: mode="gcp", exhausted=True
    r0 = await client.get("/admin/api/account-balance")
    assert r0.status_code == 200
    assert r0.json()["mode"] == "gcp"
    assert r0.json()["exhausted"] is True

    # Put manual balance $12.50 -> mode="merged", exhausted=False
    r1 = await client.put("/admin/api/account-balance", json={"balance_usd": "12.50"})
    assert r1.status_code == 200
    assert r1.json()["mode"] == "merged"
    assert r1.json()["remaining_usd"] == "12.5000"
    assert r1.json()["exhausted"] is False

    dash = (await client.get("/admin/api/dashboard")).json()
    assert dash["budget_status"]["hard_stop"] is False
    assert dash["budget_status"]["account_balance"]["mode"] == "merged"
    assert dash["budget_status"]["gcp_billing"]["exhausted"] is False
