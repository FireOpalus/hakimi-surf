import asyncio
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

import hakimi.store as store_module
from hakimi.budget_policy import BudgetConfig
from hakimi.security import Rejected
from hakimi.store import Store
from hakimi.usage_accounting import TokenUsage


async def observed(store, *, subject="s"):
    reservation = await store.reserve(subject, "chat", Decimal(".01"))
    await store.settle(
        reservation,
        Decimal(".001875"),
        usage=TokenUsage(1000, 20, cached_tokens=600, thought_tokens=30),
        discounted_actual=Decimal(".001020"),
    )
    return reservation


async def test_cache_discount_settles_at_bill_aligned_charge(store):
    reservation = await observed(store)
    data = await store.usage()
    for period in ("today", "month", "lifetime"):
        tokens = data["token_usage"][period]
        costs = data["cost_estimates"][period]
        assert tokens["input_tokens"] == 1000
        assert tokens["output_tokens"] == 20
        assert tokens["thought_tokens"] == 30
        assert tokens["cached_tokens"] == 600
        assert tokens["cache_hit_ratio"] == 0.6
        assert costs["charged_microdollars"] == 1020
        assert costs["estimated_microdollars"] == 1020
        assert costs["complete"] is True
    assert data["lanes"][0]["amount"] == 1020
    row = await store.one("SELECT * FROM ledger_usage WHERE id=?", (reservation,))
    assert row["cached_tokens"] == 600 and row["estimated_amount"] == 1020
    assert (await store.budget_status())["monthly"] == 1020


async def test_cache_ratio_uses_all_reported_interaction_inputs_and_marks_partial_coverage(store):
    await observed(store)
    decision = await store.reserve("s", "chat", Decimal(".01"))
    await store.settle(
        decision,
        Decimal(".0001"),
        usage=TokenUsage(1000, 0, cached_tokens=0, thought_tokens=0, model_lane="decision"),
        discounted_actual=Decimal(".0001"),
    )
    missing = await store.reserve("s", "chat", Decimal(".01"))
    await store.settle(missing, Decimal(".0003"), usage=TokenUsage(100, 20))
    usage = await store.usage()
    tokens = usage["token_usage"]["lifetime"]
    assert tokens["calls"] == tokens["known_usage_calls"] == 3
    assert tokens["input_tokens"] == 2100
    assert tokens["interaction_input_tokens"] == 1100
    assert tokens["cache_known_input_tokens"] == 1000
    assert tokens["cache_known_calls"] == 1
    assert tokens["cache_hit_ratio"] == 600 / 1100
    assert tokens["cache_observed_hit_ratio"] == 0.6
    assert tokens["cache_coverage_complete"] is False
    assert tokens["thought_known_calls"] == 2
    costs = usage["cost_estimates"]["lifetime"]
    assert costs["estimated_calls"] == 3 and costs["unknown_estimate_calls"] == 0
    assert costs["complete"] is True
    assert costs["charged_microdollars"] == costs["estimated_microdollars"] == 1420
    assert (
        await store.one(
            "SELECT cached_tokens,thought_tokens FROM ledger_usage WHERE id=?", (missing,)
        )
    )[:] == (None, None)


async def test_settlement_is_idempotent_under_concurrency(store):
    reservation = await store.reserve("s", "chat", Decimal(".01"))
    await asyncio.gather(
        *(
            store.settle(
                reservation,
                Decimal(".001875"),
                usage=TokenUsage(1000, 20, 600, 30),
                discounted_actual=Decimal(".001020"),
            )
            for _ in range(10)
        )
    )
    data = await store.usage()
    assert data["token_usage"]["lifetime"]["input_tokens"] == 1000
    assert data["cost_estimates"]["lifetime"]["estimated_calls"] == 1
    assert data["cost_estimates"]["lifetime"]["charged_microdollars"] == 1020
    assert (await store.one("SELECT COUNT(*) FROM ledger_usage"))[0] == 1


async def test_unknown_usage_is_not_invented_and_preserves_reservation(store):
    reservation = await store.reserve("s", "chat", Decimal(".01"))
    await store.settle(reservation, None)
    usage = await store.usage()
    tokens = usage["token_usage"]["lifetime"]
    assert tokens["input_tokens"] == 0 and tokens["unknown_usage_calls"] == 1
    assert tokens["cache_hit_ratio"] is None
    costs = usage["cost_estimates"]["lifetime"]
    assert costs["charged_microdollars"] == 10000
    assert costs["estimated_microdollars"] is None and not costs["complete"]


async def test_aggregates_survive_pruning_and_restart(settings, monkeypatch):
    class FixedDate(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2000, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(store_module, "datetime", FixedDate)
    store = Store(settings)
    await store.open()
    try:
        reservation = await observed(store)
        await store.prune()
        assert (await store.one("SELECT COUNT(*) FROM ledger"))[0] == 0
        assert (await store.one("SELECT COUNT(*) FROM ledger_usage"))[0] == 0
        await store.settle(
            reservation, Decimal("99")
        )  # A pruned id cannot re-charge or re-aggregate.
    finally:
        await store.close()
    restored = Store(settings)
    await restored.open()
    try:
        usage = await restored.usage()
        assert usage["token_usage"]["today"]["cached_tokens"] == 600
        assert usage["token_usage"]["lifetime"]["input_tokens"] == 1000
        assert usage["cost_estimates"]["lifetime"]["estimated_microdollars"] == 1020
        assert (await restored.budget_status())["monthly"] == 1020
        assert (await restored.budget_status())["total"] == 1020
    finally:
        await restored.close()


async def test_old_ledger_migration_keeps_unknown_history_explicit(settings):
    store = Store(settings)
    await store.open()
    await store.close()
    day = datetime.now(UTC).date().isoformat()
    with sqlite3.connect(settings.data_dir / "hakimi.sqlite3") as db:
        db.execute("DELETE FROM meta WHERE key='usage_history_v1'")
        db.execute("DROP TABLE usage_aggregates")
        db.execute("DROP TABLE ledger_usage")
        db.execute(
            "INSERT INTO ledger VALUES ('legacy',?,'s','chat',200000,200000,'settled')", (day,)
        )
        db.execute("UPDATE budget_totals SET amount=700000")
    for _ in range(2):
        restored = Store(settings)
        await restored.open()
        try:
            usage = await restored.usage()
            assert usage["history"]["legacy_unattributed_microdollars"] == 500000
            assert usage["history"]["lifetime_call_count_complete"] is False
            assert usage["token_usage"]["lifetime"]["unknown_usage_calls"] == 1
            assert usage["token_usage"]["lifetime"]["cache_hit_ratio"] is None
            assert usage["cost_estimates"]["lifetime"]["charged_microdollars"] == 700000
            assert usage["cost_estimates"]["lifetime"]["estimated_microdollars"] is None
            assert (await restored.budget_status())["monthly"] == 200000
        finally:
            await restored.close()


async def test_monthly_limit_reservations_are_atomic(store):
    store.s.user_daily_usd = Decimal("100")
    store.s.total_usd = Decimal(0)
    await store.set_budget_config(BudgetConfig(daily_usd="100", monthly_usd="1"))
    results = await asyncio.gather(
        *(store.reserve(f"s-{i}", "chat", Decimal(".2")) for i in range(20)), return_exceptions=True
    )
    assert sum(isinstance(result, str) for result in results) == 5
    status = await store.budget_status()
    assert status["monthly"] == status["monthly_limit"] == 1_000_000
    assert status["daily"] < status["daily_limit"]
    assert status["hard_stop"]


async def test_local_month_rollover_and_late_settlement_keep_original_month(store, monkeypatch):
    class FixedDate(datetime):
        current = datetime(2026, 1, 31, 15, 59, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(store_module, "datetime", FixedDate)
    await store.set_budget_config(BudgetConfig(daily_usd="1", monthly_usd=".1"))
    january = await store.reserve("s", "chat", Decimal(".08"))
    assert (await store.budget_status())["month_local"] == "2026-01"
    FixedDate.current = datetime(2026, 1, 31, 16, tzinfo=UTC)
    await store.settle(january, Decimal(".06"))
    status = await store.budget_status()
    assert status["monthly"] == status["daily"] == 0
    assert status["total"] == 60000
    await store.reserve("s", "chat", Decimal(".1"))
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("s", "chat", Decimal(".000001"))
    assert (
        await store.one("SELECT charged_amount FROM usage_aggregates WHERE period='month:2026-01'")
    )[0] == 60000
    assert (await store.budget_status())["monthly"] == 100000
    assert (await store.budget_status())["total"] == 160000


async def test_lowering_budget_below_spent_blocks_immediately_without_refunding(store):
    await observed(store)
    await store.set_budget_config(BudgetConfig(daily_usd="1", monthly_usd=".001"))
    status = await store.budget_status()
    assert status["monthly"] == 1020 and status["monthly_limit"] == 1000
    assert status["hard_stop"]
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("s", "chat", Decimal(".000001"))
    await store.set_budget_config(BudgetConfig(daily_usd="1", monthly_usd="1"))
    assert not (await store.budget_status())["hard_stop"]
    assert (await store.budget_status())["total"] == 1020


@pytest.mark.parametrize("value", [BudgetConfig(daily_usd="0"), BudgetConfig(monthly_usd="0")])
async def test_zero_daily_or_monthly_budget_pauses_paid_requests(store, value):
    await store.set_budget_config(value)
    assert (await store.budget_status())["hard_stop"]
    with pytest.raises(Rejected, match="budget_exhausted"):
        await store.reserve("s", "chat", Decimal("0"))


async def test_hot_budget_override_restart_and_environment_reset(settings):
    store = Store(settings)
    await store.open()
    try:
        await observed(store)
        await store.set_budget_config(BudgetConfig(daily_usd=".005", monthly_usd=".02"))
    finally:
        await store.close()
    settings = settings.model_copy(update={"daily_usd": Decimal("9"), "monthly_usd": Decimal("99")})
    restored = Store(settings)
    await restored.open()
    try:
        config = await restored.budget_config_status()
        assert config["source"] == "admin"
        assert Decimal(config["config"]["daily_usd"]) == Decimal(".005")
        assert Decimal(config["environment"]["monthly_usd"]) == 99
        with pytest.raises(Rejected, match="budget_exhausted"):
            await restored.reserve("s", "chat", Decimal(".004"))
        reset = await restored.set_budget_config(None)
        assert reset["source"] == "env"
        assert Decimal(reset["config"]["daily_usd"]) == 9
        assert (await restored.budget_status())["total"] == 1020
    finally:
        await restored.close()


async def test_changing_daily_monthly_caps_cannot_clear_overrun_or_total_cap(store):
    reservation = await store.reserve("s", "chat", Decimal(".01"))
    with pytest.raises(Rejected, match="budget_reservation_exceeded"):
        await store.settle(reservation, Decimal(".02"))
    await store.set_budget_config(BudgetConfig(daily_usd="100", monthly_usd="1000"))
    assert (await store.budget_status())["circuit"]
    with pytest.raises(Rejected, match="budget_circuit_open"):
        await store.reserve("s", "chat", Decimal(".01"))


@pytest.mark.parametrize("amount", [Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
async def test_invalid_budget_amount_never_modifies_accounting(store, amount):
    with pytest.raises(Rejected, match="invalid_budget_amount"):
        await store.reserve("s", "chat", amount)
    assert (await store.budget_status())["total"] == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cached_tokens": 101},
        {"cached_tokens": True},
        {"thought_tokens": -1},
        {"input_tokens": True},
        {"output_tokens": float("nan")},
        {"model_lane": "unknown"},
    ],
)
def test_observed_usage_rejects_invalid_provider_counts(kwargs):
    with pytest.raises(ValueError):
        TokenUsage(**{"input_tokens": 100, "output_tokens": 10, **kwargs})


@pytest.mark.parametrize("amount", [True, "NaN", "Infinity", "-1", "10001", "0.0000001"])
def test_budget_configuration_validates_amounts(amount):
    with pytest.raises(ValidationError):
        BudgetConfig(monthly_usd=amount)


async def test_bill_aligned_reconciliation_upgrades_retained_rows_idempotently(settings):
    day = datetime.now(UTC).astimezone(ZoneInfo(settings.bot_timezone)).date().isoformat()
    store = Store(settings)
    await store.open()
    await store.close()
    with sqlite3.connect(settings.data_dir / "hakimi.sqlite3") as db:
        db.execute("DELETE FROM meta WHERE key='bill_aligned_v1'")
        rows = [
            ("cached-hit", day, "u1", "chat", 10000, 50000, "settled"),
            ("legacy-settled", day, "u1", "chat", 1500, 5000, "settled"),
            ("chat-unknown", day, "u1", "chat", 80000, 80000, "unknown"),
            ("flex-unknown", day, "u1", "memory", 21000, 21000, "unknown"),
        ]
        db.executemany("INSERT INTO ledger VALUES (?,?,?,?,?,?,?)", rows)
        db.execute("INSERT INTO ledger_usage VALUES ('cached-hit',1000,20,800,30,'hydrogen',3000)")
        db.execute("INSERT INTO ledger_attribution VALUES ('cached-hit','hydrogen')")
        db.execute("INSERT INTO ledger_attribution VALUES ('chat-unknown','hydrogen')")
        db.execute("INSERT INTO ledger_attribution VALUES ('flex-unknown','memory_flex')")
        # Include 50000 pruned unattributed historical microdollars in budget_totals / lifetime.
        charged_total = 10000 + 1500 + 80000 + 21000
        db.execute("UPDATE budget_totals SET amount=?", (charged_total + 50000,))
        for period in ("day:" + day, "month:" + day[:7], "lifetime"):
            total = charged_total + (50000 if period == "lifetime" else 0)
            db.execute(
                "INSERT OR REPLACE INTO usage_aggregates(period,charged_amount,calls,"
                "known_usage_calls,input_tokens,output_tokens,cached_tokens,thought_tokens,"
                "interaction_calls,interaction_input_tokens,cache_known_calls,"
                "cache_known_input_tokens,thought_known_calls,estimated_amount,estimated_calls) "
                "VALUES (?,?,4,1,1000,20,800,30,1,1000,1,1000,1,3000,1)",
                (period, total),
            )
            for lane, n, chg, est, est_n in (
                ("hydrogen", 2, 90000, 3000, 1),
                ("unknown", 1, 1500, 0, 0),
                ("memory_flex", 1, 21000, 0, 0),
            ):
                db.execute(
                    "INSERT INTO model_usage_aggregates VALUES (?,?,?,?,?,?,1000,800,20,30)",
                    (period, lane, n, chg, est, est_n),
                )
    expected_snapshot = None
    for _ in range(2):
        restored = Store(settings)
        await restored.open()
        try:
            chat_u = (await restored.one("SELECT amount FROM ledger WHERE id='chat-unknown'"))[0]
            flex_u = (await restored.one("SELECT amount FROM ledger WHERE id='flex-unknown'"))[0]
            assert 0 < chat_u < 80000 and 0 < flex_u < 21000
            cached_row = await restored.one("SELECT amount FROM ledger WHERE id='cached-hit'")
            assert cached_row[0] == 3000
            legacy_est = await restored.one(
                "SELECT estimated_amount FROM ledger_usage WHERE id='legacy-settled'"
            )
            assert legacy_est[0] == 1500
            expected_charged = 3000 + 1500 + chat_u + flex_u
            usage = await restored.usage()
            costs_today = usage["cost_estimates"]["today"]
            costs_life = usage["cost_estimates"]["lifetime"]
            assert costs_today["charged_microdollars"] == expected_charged
            assert costs_today["estimated_microdollars"] == 4500
            assert costs_today["estimated_calls"] == 2
            assert costs_today["unknown_estimate_calls"] == 2
            assert costs_life["charged_microdollars"] == expected_charged + 50000
            assert costs_life["estimated_microdollars"] == 4500
            if expected_snapshot is None:
                expected_snapshot = usage
            else:
                assert usage["cost_estimates"] == expected_snapshot["cost_estimates"]
        finally:
            await restored.close()
