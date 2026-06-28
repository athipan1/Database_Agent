import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from models import OrderSide
from plan_record_repository import (
    create_plan_record,
    get_plan_record,
    list_plan_records,
    setup_plan_record_table,
    update_plan_record_status,
)
from trade_plan_models import CreateTradePlanBody, ListTradePlansQuery, TradePlanLifecycleStatus, UpdateTradePlanStatusBody
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_plan_record_table(database)
    return database


def plan_body(trade_plan_id="plan-db-1", *, account_id=1, symbol="aapl", status=TradePlanLifecycleStatus.CREATED, strategy="trend_pullback", strategy_bucket="value_rebound"):
    return CreateTradePlanBody(
        trade_plan_id=trade_plan_id,
        account_id=account_id,
        symbol=symbol,
        side=OrderSide.BUY,
        status=status,
        correlation_id="corr-db-1",
        source="manager-agent",
        strategy=strategy,
        strategy_bucket=strategy_bucket,
        plan={
            "plan_id": trade_plan_id,
            "symbol": symbol.upper(),
            "side": "buy",
            "entry_price": 100,
            "quantity": 5,
        },
        metadata={"source_test": "plan_record_repository"},
    )


def test_create_and_get_plan_record(db):
    created = create_plan_record(db, plan_body())
    fetched = get_plan_record(db, "plan-db-1")

    assert created.trade_plan_id == "plan-db-1"
    assert fetched.trade_plan_id == "plan-db-1"
    assert fetched.account_id == "1"
    assert fetched.symbol == "AAPL"
    assert fetched.side == OrderSide.BUY
    assert fetched.status == TradePlanLifecycleStatus.CREATED
    assert fetched.strategy_bucket == "value_rebound"
    assert fetched.plan["entry_price"] == 100
    assert fetched.metadata["source_test"] == "plan_record_repository"
    assert fetched.lifecycle[0]["status"] == "created"


def test_update_plan_record_status_appends_lifecycle_event(db):
    create_plan_record(db, plan_body())

    updated = update_plan_record_status(
        db,
        "plan-db-1",
        UpdateTradePlanStatusBody(
            status=TradePlanLifecycleStatus.RISK_APPROVED,
            reason="risk agent approved plan",
            risk_approval_id="risk-db-1",
            metadata={"risk_score": 0.25},
        ),
    )

    assert updated.status == TradePlanLifecycleStatus.RISK_APPROVED
    assert updated.risk_approval_id == "risk-db-1"
    assert updated.metadata["source_test"] == "plan_record_repository"
    assert updated.metadata["risk_score"] == 0.25
    assert len(updated.lifecycle) == 2
    assert updated.lifecycle[-1]["status"] == "risk_approved"
    assert updated.lifecycle[-1]["reason"] == "risk agent approved plan"


def test_update_plan_record_status_links_execution_ids(db):
    create_plan_record(db, plan_body())

    updated = update_plan_record_status(
        db,
        "plan-db-1",
        UpdateTradePlanStatusBody(
            status=TradePlanLifecycleStatus.EXECUTION_SUBMITTED,
            order_id=42,
            execution_job_id="job-42",
            broker_order_id="broker-42",
        ),
    )

    assert updated.status == TradePlanLifecycleStatus.EXECUTION_SUBMITTED
    assert updated.order_id == 42
    assert updated.execution_job_id == "job-42"
    assert updated.broker_order_id == "broker-42"


def test_list_plan_records_filters_by_account_symbol_status_and_bucket(db):
    create_plan_record(db, plan_body("plan-db-1", account_id=1, symbol="aapl", status=TradePlanLifecycleStatus.FILLED, strategy_bucket="value_rebound"))
    create_plan_record(db, plan_body("plan-db-2", account_id=1, symbol="msft", status=TradePlanLifecycleStatus.REJECTED, strategy_bucket="news_momentum"))
    create_plan_record(db, plan_body("plan-db-3", account_id=2, symbol="aapl", status=TradePlanLifecycleStatus.FILLED, strategy_bucket="value_rebound"))

    records = list_plan_records(
        db,
        ListTradePlansQuery(
            account_id=1,
            symbol="aapl",
            status=TradePlanLifecycleStatus.FILLED,
            strategy_bucket="value_rebound",
        ),
    )

    assert [record.trade_plan_id for record in records] == ["plan-db-1"]
    assert records[0].symbol == "AAPL"
    assert records[0].status == TradePlanLifecycleStatus.FILLED


def test_list_plan_records_supports_limit_offset_and_sort(db):
    create_plan_record(db, plan_body("plan-db-1"))
    create_plan_record(db, plan_body("plan-db-2"))
    create_plan_record(db, plan_body("plan-db-3"))

    records = list_plan_records(
        db,
        ListTradePlansQuery(limit=2, offset=1, sort="created_at", order="asc"),
    )

    assert len(records) == 2
    assert [record.trade_plan_id for record in records] == ["plan-db-2", "plan-db-3"]


def test_update_missing_plan_record_returns_404(db):
    with pytest.raises(HTTPException) as exc_info:
        update_plan_record_status(
            db,
            "missing-plan",
            UpdateTradePlanStatusBody(status=TradePlanLifecycleStatus.REJECTED),
        )

    assert exc_info.value.status_code == 404
