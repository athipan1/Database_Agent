from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class FinanceBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid", str_strip_whitespace=True)


class FinanceEntry(FinanceBaseModel):
    entry_id: str
    account_id: str
    entry_type: Literal["income", "expense"]
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Literal["THB"] = "THB"
    category: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=240)
    occurred_at: dt.datetime
    created_at: dt.datetime | None = None
    updated_at: dt.datetime | None = None


class CreateFinanceEntryBody(FinanceBaseModel):
    entry_id: str = Field(min_length=8, max_length=80)
    account_id: str | int
    entry_type: Literal["income", "expense"]
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Literal["THB"] = "THB"
    category: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=240)
    occurred_at: dt.datetime

    @field_validator("account_id", mode="before")
    @classmethod
    def normalize_account_id(cls, value):
        return str(value)


class FinanceBudgets(FinanceBaseModel):
    account_id: str
    personal_investment_budget_thb: Decimal = Field(default=Decimal("0"), ge=0, max_digits=18, decimal_places=2)
    trade_plan_limit_usd: Decimal = Field(default=Decimal("0"), ge=0, max_digits=18, decimal_places=2)
    updated_at: dt.datetime | None = None


class UpsertFinanceBudgetsBody(FinanceBaseModel):
    personal_investment_budget_thb: Decimal = Field(default=Decimal("0"), ge=0, max_digits=18, decimal_places=2)
    trade_plan_limit_usd: Decimal = Field(default=Decimal("0"), ge=0, max_digits=18, decimal_places=2)


class PersonalFinanceState(FinanceBaseModel):
    account_id: str
    entries: list[FinanceEntry] = Field(default_factory=list)
    budgets: FinanceBudgets
