"""Mutable daily/monthly caps; zero pauses paid work rather than removing a limit."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BudgetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    daily_usd: Decimal = Field(default=Decimal("2"), ge=0, le=1000, max_digits=12, decimal_places=6)
    monthly_usd: Decimal = Field(
        default=Decimal("40"), ge=0, le=10000, max_digits=12, decimal_places=6
    )

    @field_validator("daily_usd", "monthly_usd", mode="before")
    @classmethod
    def amount(cls, value):
        if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
            raise ValueError("USD budget must be a finite decimal amount")
        return value

    @classmethod
    def from_settings(cls, settings):
        return cls(daily_usd=settings.daily_usd, monthly_usd=getattr(settings, "monthly_usd", "40"))
