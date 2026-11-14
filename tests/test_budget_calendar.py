"""Shanghai budget boundaries and lossless/explicit migration from the old UTC buckets."""

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

import hakimi.store as store_module
from hakimi.budget_calendar import METRICS, observations
from hakimi.security import Rejected
from hakimi.store import Store
from hakimi.usage_accounting import TokenUsage


class Clock(datetime):
    current = datetime(2026, 11, 30, 15, 59, tzinfo=UTC)

    @classmethod
    def now(cls, tz=None):
        return cls.current if tz is None else cls.current.astimezone(tz)


def set_clock(monkeypatch, value):
    Clock.current = datetime.fromisoformat(value)
    monkeypatch.setattr(store_module, "datetime", Clock)


async def charge(store, amount="0.02"):
    reservation = await store.reserve("same-user", "chat", Decimal(amount))
    await store.settle(
        reservation,
        Decimal(amount),
        usage=TokenUsage(1000, 25, cached_tokens=500, thought_tokens=12),
        discounted_actual=Decimal(".01"),
    )
    return reservation


async def test_midnight_and_month_rollover_are_shanghai_not_host_utc(store, monkeypatch):
    set_clock(monkeypatch, "2026-11-30T15:59:59+00:00")
    await charge(store)
    before = await store.budget_status()
    assert before["day_local"] == "2026-11-30" and before["monthly"] == 20000
    set_clock(monkeypatch, "2026-11-30T16:00:00+00:00")
    after = await store.budget_status()
    assert after["day_local"] == "2026-12-01" and after["month_local"] == "2026-12"
    assert after["daily"] == after["monthly"] == 0
    assert after["total"] == 20000 and after["timezone"] == "Asia/Shanghai"
    await charge(store)
    usage = await store.usage()
    assert usage["day_local"] == "2026-12-01"
    assert usage["token_usage"]["today"]["total_cached_tokens"] == 500
    assert usage["token_usage"]["lifetime"]["total_cached_tokens"] == 1000


async def test_late_settlement_stays_in_original_shanghai_day(store, monkeypatch):
    set_clock(monkeypatch, "2026-11-30T15:59:59+00:00")
    reservation = await store.reserve("u", "chat", Decimal(".04"))
    set_clock(monkeypatch, "2026-11-30T16:00:01+00:00")
    await store.settle(reservation, Decimal(".02"))
    assert (await store.budget_status())["monthly"] == 0
    old = await store.one(
        "SELECT charged_amount FROM usage_aggregates WHERE period='month:2026-11'"
    )
    assert old[0] == 20000
    assert (await store.budget_status())["total"] == 20000


async def old_utc_database(settings, monkeypatch, *, prune=False, no_time=False):
    set_clock(monkeypatch, "2026-11-30T18:00:00+00:00")
    store = Store(settings)
    await store.open()
    first = await charge(store)
    second = await charge(store)
    lifetime = dict(await store.one("SELECT * FROM usage_aggregates WHERE period='lifetime'"))
    # Turn this into the 0.7.0 on-disk layout: timestamps/usage exist, date buckets were UTC.
    async with store.transaction():
        await store.db.execute("DELETE FROM meta WHERE key='budget_calendar_v2'")
        await store.db.execute("DELETE FROM usage_calendar_legacy")
        await store.db.execute("DELETE FROM budget_calendar_carry")
        await store.db.execute("DELETE FROM usage_aggregates WHERE period<>'lifetime'")
        await store.db.execute("UPDATE ledger SET day='2026-11-30'")
        row = dict(
            await store.one(
                "SELECT l.amount,u.* FROM ledger l JOIN ledger_usage u USING(id) WHERE l.id=?",
                (first,),
            )
        )
        stats = observations(row)
        cols, marks = ",".join(METRICS), ",".join("?" for _ in METRICS)
        for period in ("day:2026-11-30", "month:2026-11"):
            await store.db.execute(
                f"INSERT INTO usage_aggregates(period,{cols}) VALUES (?,{marks})",
                (period, *(stats[k] * 2 for k in METRICS)),
            )
        if prune:
            for table in ("ledger", "ledger_usage", "ledger_times"):
                await store.db.execute(f"DELETE FROM {table} WHERE id=?", (second,))
        if no_time:
            await store.db.execute("DELETE FROM ledger_times")
    await store.close()
    return lifetime


@pytest.mark.parametrize("prune", [False, True])
async def test_utc_migration_rebuckets_retained_rows_preserves_all_lifetime_metrics_and_restart(
    settings, monkeypatch, prune
):
    lifetime = await old_utc_database(settings, monkeypatch, prune=prune)
    first_state = None
    for _ in range(2):
        store = Store(settings)
        await store.open()
        try:
            assert (
                dict(await store.one("SELECT * FROM usage_aggregates WHERE period='lifetime'"))
                == lifetime
            )
            status = await store.budget_status()
            assert status["daily"] == status["monthly"] == status["total"] == 40000
            assert status["monthly_calendar_carry"] == (20000 if prune else 0)
            usage = await store.usage()
            assert usage["token_usage"]["lifetime"]["cached_tokens"] == 1000
            assert usage["token_usage"]["lifetime"]["thought_tokens"] == 24
            assert usage["token_usage"]["today"]["cached_tokens"] == (500 if prune else 1000)
            archived = await store.one(
                "SELECT payload FROM usage_calendar_legacy WHERE period='day:2026-11-30'"
            )
            assert (
                store.vault.open("usage-calendar:day:2026-11-30", archived[0])["cached_tokens"]
                == 1000
            )
            assert usage["history"]["calendar"]["pruned_calls"] == int(prune)
            state = [
                dict(r)
                for r in await store.all("SELECT * FROM budget_calendar_carry ORDER BY period")
            ]
            if first_state is not None:
                assert first_state == state
            first_state = state
        finally:
            await store.close()


async def test_unknown_legacy_timestamps_add_conservative_carry_without_inventing_tokens(
    settings, monkeypatch
):
    await old_utc_database(settings, monkeypatch, no_time=True)
    store = Store(settings)
    await store.open()
    try:
        status = await store.budget_status()
        assert status["monthly"] == status["daily"] == 40000
        assert status["monthly_observed"] == 0
        assert status["monthly_calendar_carry"] == 40000
        assert (await store.usage())["token_usage"]["today"]["cached_tokens"] == 0
        assert (await store.usage())["history"]["calendar"]["missing_timestamp_calls"] == 2
        store.s.daily_usd = Decimal(".04")
        with pytest.raises(Rejected, match="budget_exhausted"):
            await store.reserve("u", "chat", Decimal(".000001"))
        # Day carry expires at the Shanghai boundary, month carry remains until next month.
        set_clock(monkeypatch, "2026-12-01T16:00:00+00:00")
        assert (await store.budget_status())["daily"] == 0
        assert (await store.budget_status())["monthly"] == 40000
    finally:
        await store.close()


async def test_preaggregate_unassigned_lifetime_cost_is_not_lost(settings, monkeypatch):
    await old_utc_database(settings, monkeypatch)
    with sqlite3.connect(settings.data_dir / "hakimi.sqlite3") as db:
        db.execute("UPDATE budget_totals SET amount=amount+10000")
        db.execute(
            "UPDATE usage_aggregates SET charged_amount=charged_amount+10000 "
            "WHERE period='lifetime'"
        )
    store = Store(settings)
    await store.open()
    try:
        status = await store.budget_status()
        assert status["total"] == status["monthly"] == status["daily"] == 50000
        assert (await store.usage())["history"]["calendar"]["unassigned_microdollars"] == 10000
    finally:
        await store.close()


async def test_timezone_change_cannot_silently_reset_existing_budget_calendar(settings):
    store = Store(settings)
    await store.open()
    await store.close()
    changed = Store(settings.model_copy(update={"bot_timezone": "UTC"}))
    try:
        with pytest.raises(RuntimeError, match="persisted budget calendar"):
            await changed.open()
    finally:
        await changed.close()
