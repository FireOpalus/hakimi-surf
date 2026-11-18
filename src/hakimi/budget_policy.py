"""Mutable daily/monthly and lane/user caps; zero pauses paid work cleanly."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

PATROL_LANE = "patrol"
PROACTIVE_LANE = "proactive"


def paid_lane_available(status: dict, lane: str) -> bool:
    """Cheap preflight; the ledger still reserves the actual request atomically."""
    sub = status.get("sub_budgets", {}).get(lane)
    return not status.get("hard_stop") and (sub is None or sub["used"] < sub["limit"])


class BudgetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    daily_usd: Decimal = Field(default=Decimal("2"), ge=0, le=1000, max_digits=12, decimal_places=6)
    monthly_usd: Decimal = Field(
        default=Decimal("40"), ge=0, le=10000, max_digits=12, decimal_places=6
    )
    user_daily_usd: Decimal = Field(
        default=Decimal("1"), ge=0, le=1000, max_digits=12, decimal_places=6
    )
    patrol_daily_usd: Decimal = Field(
        default=Decimal("0.50"), ge=0, le=1000, max_digits=12, decimal_places=6
    )
    memory_daily_usd: Decimal = Field(
        default=Decimal("0.20"), ge=0, le=100, max_digits=12, decimal_places=6
    )
    proactive_daily_usd: Decimal = Field(
        default=Decimal("0.10"), ge=0, le=100, max_digits=12, decimal_places=6
    )
    admin_daily_usd: Decimal = Field(
        default=Decimal("0.20"), ge=0, le=100, max_digits=12, decimal_places=6
    )

    @field_validator(
        "daily_usd",
        "monthly_usd",
        "user_daily_usd",
        "patrol_daily_usd",
        "memory_daily_usd",
        "proactive_daily_usd",
        "admin_daily_usd",
        mode="before",
    )
    @classmethod
    def amount(cls, value):
        if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
            raise ValueError("USD budget must be a finite decimal amount")
        return value

    @classmethod
    def from_settings(cls, settings):
        return cls(
            **{
                name: getattr(settings, name, field.default)
                for name, field in cls.model_fields.items()
            }
        )
