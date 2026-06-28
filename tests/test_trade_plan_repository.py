import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from models import CreateTradePlanBody, OrderSide, TradePlanLifecycleStatus, UpdateTradePlanStatusBody
from trading_db import TradingDB
from trade_plan_repository import (
    create_trade_plan,
    get_trade_plan,
    setup_trade_plan_table,
    update_trade_plan_status,
)


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_trade_plan_table(database)
    return database


def trade_plan_body(trade_plan_id="plan-db-1"):
    return CreateTradePlanBody(
        trade_plan_id=trade_plan_id,
        account_id=1,
        symbol="aapl",
        side=OrderSide.BUY,
        status=TradePlanLifecycleStatus.CREATED,
        correlation_id="corr-db-1",
        source="manager-agent",
        strategy="trend_pullback",
        strategy_bucket="value_rebound",
        risk_approval_id=None,
        plan={
            "plan_id": trade_plan_id,
            "symbol": "AAPL",
            "side": "buy",
            "entry_price": 100,
            "quantity": 5,
        },
        metadata={"source_test": "trade_plan_repository"},
    )


def test_create_and_get_trade_plan(db):
    created = create_trade_plan(db, trade_plan_body())
    fetched = get_trade_plan(db, "plan-db-1")

    assert created.trade_plan_id == "plan-db-1"
    assert fetched.trade_plan_id == "plan-db-1"
    assert fetched.account_id == "1"
    assert fetched.symbol == "AAPL"
    assert fetched.side == OrderSide.BUY
    assert fetched.status == TradePlanLifecycleStatus.CREATED
    assert fetched.strategy_bucket == "value_rebound"
    assert fetched.plan["entry_price"] == 100
    assert fetched.metadata["source_test"] == "trade_plan_repository"
    assert fetched.lifecycle[0]["status"] == "created"


def test_update_trade_plan_status_appends_lifecycle_event(db):
    create_trade_plan(db, trade_plan_body())

    updated = update_trade_plan_status(
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
    assert updated.metadata["source_test"] == "trade_plan_repository"
    assert updated.metadata["risk_score"] == 0.25
    assert len(updated.lifecycle) == 2
    assert updated.lifecycle[-1]["status"] == "risk_approved"
    assert updated.lifecycle[-1]["reason"] == "risk agent approved plan"


def test_update_trade_plan_status_links_execution_ids(db):
    create_trade_plan(db, trade_plan_body())

    updated = update_trade_plan_status(
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


def test_update_missing_trade_plan_returns_404(db):
    with pytest.raises(HTTPException) as exc_info:
        update_trade_plan_status(
            db,
            "missing-plan",
            UpdateTradePlanStatusBody(status=TradePlanLifecycleStatus.REJECTED),
        )

    assert exc_info.value.status_code == 404
