import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from trading_db import TradingDB
from models import CreateRiskApprovalBody, OrderSide, RiskApprovalStatus
from risk_approval_repository import (
    create_risk_approval,
    get_risk_approval,
    mark_risk_approval_used,
    setup_risk_approval_table,
)


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_risk_approval_table(database)
    return database


def approval_body(approval_id="risk-test", expires_delta=timedelta(minutes=5)):
    return CreateRiskApprovalBody(
        approval_id=approval_id,
        account_id=1,
        symbol="AAPL",
        side=OrderSide.BUY,
        approved_quantity=10,
        expires_at=datetime.now(timezone.utc) + expires_delta,
        metadata={"source": "risk-agent-test"},
    )


def test_create_and_get_risk_approval(db):
    created = create_risk_approval(db, approval_body())
    fetched = get_risk_approval(db, "risk-test")

    assert created.approval_id == "risk-test"
    assert fetched.approval_id == "risk-test"
    assert fetched.status == RiskApprovalStatus.APPROVED
    assert fetched.symbol == "AAPL"
    assert fetched.metadata["source"] == "risk-agent-test"


def test_mark_risk_approval_used_prevents_replay(db):
    create_risk_approval(db, approval_body())

    used = mark_risk_approval_used(db, "risk-test", 123)
    assert used.status == RiskApprovalStatus.USED
    assert used.order_id == 123
    assert used.used_at is not None

    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "risk-test", 456)
    assert exc_info.value.status_code == 409


def test_expired_risk_approval_cannot_be_used(db):
    create_risk_approval(db, approval_body(approval_id="risk-expired", expires_delta=timedelta(seconds=-1)))

    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "risk-expired", 123)

    assert exc_info.value.status_code == 409
    expired = get_risk_approval(db, "risk-expired")
    assert expired.status == RiskApprovalStatus.EXPIRED


def test_missing_risk_approval_returns_404(db):
    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "missing", 123)
    assert exc_info.value.status_code == 404
