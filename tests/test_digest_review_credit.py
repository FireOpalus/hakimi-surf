"""Real SDK Interactions and mandatory Decisions output review with an offline peer."""

import json
from decimal import Decimal

import httpx
import pytest

from hakimi.gemini import GeminiGateway
from hakimi.security import Rejected
from hakimi.store import DigestReviewCredit, Store
from tests.test_proactive_dm import dm as dm
from tests.test_sdk_contract import decision_response, interaction_response
from tests.test_sdk_contract import sdk_pair as sdk_pair


@pytest.mark.parametrize("unsafe", [False, True])
async def test_real_digest_settlement_trips_fuse_before_mandatory_output_screen(
    dm, sdk_pair, unsafe
):
    outbound = []
    dm.s.daily_usd = Decimal("0.20")

    async def provider(request):
        data = json.loads(request.content)
        if request.url.path.endswith("interactions"):
            outbound.append("model")
            hold = await dm.store.one("SELECT amount FROM owner_digest_review_holds")
            assert hold and hold[0] > 0
            result = interaction_response()
            result["usage"].update(total_input_tokens=1_000_000, total_tokens=1_000_050)
        else:
            is_output = "generated_text" in data["input"]
            outbound.append("output" if is_output else "source")
            result = decision_response()
            if is_output:
                assert (await dm.store.budget_status())["hard_stop"]
                assert not await dm.store.one("SELECT 1 FROM owner_digest_review_holds")
                if unsafe:
                    result["answers"]["is_unsafe_or_spam"]["noul"] = 0.99
        return httpx.Response(200, json=result)

    dm.service.ai = GeminiGateway(dm.s, dm.store, *sdk_pair(provider))
    dm.service.sender.onebot.ready.clear()
    result = await dm.manual_digest()
    assert outbound == ["source", "model", "output"]
    assert result["receipt"]["stage"] == ("failed" if unsafe else "awaiting_qq")
    assert not await dm.store.one("SELECT 1 FROM owner_digest_review_holds")
    ledger = await dm.store.all(
        "SELECT l.amount,l.status,l.lane,w.kind,w.work_id FROM ledger l "
        "JOIN ledger_work w ON w.id=l.id"
    )
    assert len(ledger) == 3 and all(r["status"] == "settled" for r in ledger)
    assert {(r["lane"], r["kind"], r["work_id"]) for r in ledger} == {
        ("proactive", "qq.owner_digest", result["id"])
    }
    account = await dm.store.budget_status()
    assert account["hard_stop"] and account["daily"] == sum(r["amount"] for r in ledger)
    dm.service.sender.onebot.ready.set()
    result = await dm.manual_digest()
    assert result["receipt"]["stage"] == ("failed" if unsafe else "confirmed")
    assert len(dm.service.sender.onebot.calls) == (0 if unsafe else 1)
    assert len(outbound) == 3


async def test_model_error_releases_unspent_review_and_does_not_count_it_as_a_call(dm, sdk_pair):
    def provider(request):
        if request.url.path.endswith("decisions"):
            return httpx.Response(200, json=decision_response())
        return httpx.Response(400, json={"error": {"code": 400, "message": "invalid"}})

    dm.service.ai = GeminiGateway(dm.s, dm.store, *sdk_pair(provider))
    assert (await dm.manual_digest())["receipt"]["stage"] == "failed"
    assert not await dm.store.one("SELECT 1 FROM owner_digest_review_holds")
    account = await dm.store.budget_status()
    ledger = await dm.store.all("SELECT amount FROM ledger")
    assert len(ledger) == 2 and account["total"] == sum(r[0] for r in ledger)
    assert not dm.service.sender.onebot.calls


async def test_digest_and_review_authorize_together_and_unsubmitted_credit_is_released_on_boot(
    settings,
):
    settings.proactive_daily_usd = Decimal("0.06")
    credit = DigestReviewCredit("test-credit", Decimal("0.04"))
    store = Store(settings)
    await store.open()
    try:
        with pytest.raises(Rejected, match="budget_exhausted"):
            await store.reserve(
                "owner", "proactive", Decimal(".03"), model_lane="hydrogen", digest_review=credit
            )
        assert not await store.one("SELECT 1 FROM ledger")
        assert not await store.one("SELECT 1 FROM owner_digest_review_holds")
        primary = await store.reserve(
            "owner", "proactive", Decimal(".02"), model_lane="hydrogen", digest_review=credit
        )
        assert (await store.budget_status())["total"] == 60_000
        with pytest.raises(Rejected, match="budget_exhausted"):
            await store.reserve("other", "proactive", Decimal(".001"))
        with pytest.raises(Rejected, match="invalid_digest_review"):
            await store.reserve(
                "other", "proactive", Decimal(".01"), model_lane="decision", digest_review=credit
            )
        with pytest.raises(Rejected, match="digest_review_unfunded"):
            await store.reserve(
                "owner", "proactive", Decimal(".05"), model_lane="decision", digest_review=credit
            )
        await store.settle(primary, Decimal(".01"), model_lane="hydrogen")
    finally:
        await store.close()

    restarted = Store(settings)
    await restarted.open()
    try:
        assert not await restarted.one("SELECT 1 FROM owner_digest_review_holds")
        assert (await restarted.budget_status())["total"] == 10_000
        assert (await restarted.one("SELECT COUNT(*) FROM ledger"))[0] == 1
    finally:
        await restarted.close()
