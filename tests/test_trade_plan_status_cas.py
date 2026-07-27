import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from models import OrderSide
from plan_record_repository import create_plan_record, setup_plan_record_table, update_plan_record_status
from trade_plan_models import CreateTradePlanBody, TradePlanLifecycleStatus, UpdateTradePlanStatusBody
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_plan_record_table(database)
    return database


def create_queued_plan(db, trade_plan_id="plan-cas-1"):
    return create_plan_record(
        db,
        CreateTradePlanBody(
            trade_plan_id=trade_plan_id,
            account_id="1",
            symbol="AAPL",
            side=OrderSide.BUY,
            status=TradePlanLifecycleStatus.QUEUED,
            plan={"plan_id": trade_plan_id, "symbol": "AAPL", "side": "buy", "quantity": 1},
        ),
    )


def test_expected_status_reserves_confirmation_once(db):
    create_queued_plan(db)

    reserved = update_plan_record_status(
        db,
        "plan-cas-1",
        UpdateTradePlanStatusBody(
            status=TradePlanLifecycleStatus.EXECUTION_PENDING,
            expected_status=TradePlanLifecycleStatus.QUEUED,
            reason="operator confirmation reserved",
        ),
    )

    assert reserved.status == TradePlanLifecycleStatus.EXECUTION_PENDING

    with pytest.raises(HTTPException) as exc_info:
        update_plan_record_status(
            db,
            "plan-cas-1",
            UpdateTradePlanStatusBody(
                status=TradePlanLifecycleStatus.EXECUTION_PENDING,
                expected_status=TradePlanLifecycleStatus.QUEUED,
            ),
        )

    assert exc_info.value.status_code == 409
    assert "status conflict" in exc_info.value.detail
