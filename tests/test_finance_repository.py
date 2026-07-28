import os
from decimal import Decimal

import pytest
from fastapi import HTTPException

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from finance_models import CreateFinanceEntryBody, UpsertFinanceBudgetsBody
from finance_repository import (
    create_finance_entry,
    delete_finance_entry,
    get_personal_finance_state,
    setup_finance_tables,
    upsert_finance_budgets,
)
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_finance_tables(database)
    return database


def test_finance_state_persists_entries_and_separate_currency_budgets(db):
    create_finance_entry(
        db,
        CreateFinanceEntryBody(
            entry_id="entry-income-1",
            account_id="1",
            entry_type="income",
            amount="30000.00",
            currency="THB",
            category="salary",
            description="monthly salary",
            occurred_at="2026-07-01T12:00:00Z",
        ),
    )
    create_finance_entry(
        db,
        CreateFinanceEntryBody(
            entry_id="entry-expense-1",
            account_id="1",
            entry_type="expense",
            amount="3500.50",
            currency="THB",
            category="food",
            occurred_at="2026-07-02T12:00:00Z",
        ),
    )
    upsert_finance_budgets(
        db,
        "1",
        UpsertFinanceBudgetsBody(
            personal_investment_budget_thb="5000.00",
            trade_plan_limit_usd="250.00",
        ),
    )

    state = get_personal_finance_state(db, "1")

    assert len(state.entries) == 2
    assert state.entries[0].entry_id == "entry-expense-1"
    assert state.budgets.personal_investment_budget_thb == Decimal("5000.00")
    assert state.budgets.trade_plan_limit_usd == Decimal("250.00")


def test_delete_finance_entry_requires_matching_account(db):
    create_finance_entry(
        db,
        CreateFinanceEntryBody(
            entry_id="entry-protected-1",
            account_id="1",
            entry_type="expense",
            amount="100.00",
            category="travel",
            occurred_at="2026-07-02T12:00:00Z",
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        delete_finance_entry(db, "entry-protected-1", "2")
    assert exc_info.value.status_code == 404

    delete_finance_entry(db, "entry-protected-1", "1")
    assert get_personal_finance_state(db, "1").entries == []
