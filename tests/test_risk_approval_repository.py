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
    revoke_risk_approval,
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


def force_expired(db, approval_id: str) -> None:
    expired_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"UPDATE risk_approvals SET expires_at = {db.param_style} WHERE approval_id = {db.param_style}",
                (expired_at, approval_id),
            )
            conn.commit()
        finally:
            cursor.close()


def lifecycle_events(approval):
    return approval.metadata.get("lifecycle_events") or []


def test_create_and_get_risk_approval(db):
    created = create_risk_approval(db, approval_body())
    fetched = get_risk_approval(db, "risk-test")

    assert created.approval_id == "risk-test"
    assert fetched.approval_id == "risk-test"
    assert fetched.status == RiskApprovalStatus.APPROVED
    assert fetched.symbol == "AAPL"
    assert fetched.metadata["source"] == "risk-agent-test"
    assert fetched.metadata["last_lifecycle_event"] == "created"
    assert lifecycle_events(fetched)[-1]["event"] == "created"


def test_create_rejects_already_expired_risk_approval(db):
    with pytest.raises(HTTPException) as exc_info:
        create_risk_approval(db, approval_body(approval_id="risk-expired-create", expires_delta=timedelta(seconds=-1)))

    assert exc_info.value.status_code == 422
    assert "future" in exc_info.value.detail


def test_mark_risk_approval_used_prevents_replay(db):
    create_risk_approval(db, approval_body())

    used = mark_risk_approval_used(db, "risk-test", 123)
    assert used.status == RiskApprovalStatus.USED
    assert used.order_id == 123
    assert used.used_at is not None
    assert used.metadata["last_lifecycle_event"] == "used"
    assert lifecycle_events(used)[-1]["event"] == "used"
    assert lifecycle_events(used)[-1]["order_id"] == 123

    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "risk-test", 456)
    assert exc_info.value.status_code == 409


def test_expired_risk_approval_cannot_be_used(db):
    create_risk_approval(db, approval_body(approval_id="risk-expired"))
    force_expired(db, "risk-expired")

    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "risk-expired", 123)

    assert exc_info.value.status_code == 409
    expired = get_risk_approval(db, "risk-expired")
    assert expired.status == RiskApprovalStatus.EXPIRED
    assert expired.metadata["last_lifecycle_event"] == "expired"
    assert lifecycle_events(expired)[-1]["event"] == "expired"


def test_get_risk_approval_auto_marks_expired_approval(db):
    create_risk_approval(db, approval_body(approval_id="risk-auto-expired"))
    force_expired(db, "risk-auto-expired")

    fetched = get_risk_approval(db, "risk-auto-expired")

    assert fetched.status == RiskApprovalStatus.EXPIRED
    assert fetched.metadata["last_lifecycle_event"] == "expired"


def test_revoke_risk_approval_blocks_future_use(db):
    create_risk_approval(db, approval_body(approval_id="risk-revoke"))

    revoked = revoke_risk_approval(db, "risk-revoke", reason="manual_test")

    assert revoked.status == RiskApprovalStatus.REVOKED
    assert revoked.metadata["last_lifecycle_event"] == "revoked"
    assert lifecycle_events(revoked)[-1]["reason"] == "manual_test"

    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "risk-revoke", 123)
    assert exc_info.value.status_code == 409


def test_revoke_used_risk_approval_is_rejected(db):
    create_risk_approval(db, approval_body(approval_id="risk-used-revoke"))
    mark_risk_approval_used(db, "risk-used-revoke", 123)

    with pytest.raises(HTTPException) as exc_info:
        revoke_risk_approval(db, "risk-used-revoke", reason="too_late")

    assert exc_info.value.status_code == 409


def test_missing_risk_approval_returns_404(db):
    with pytest.raises(HTTPException) as exc_info:
        mark_risk_approval_used(db, "missing", 123)
    assert exc_info.value.status_code == 404
