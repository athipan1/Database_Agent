import os

import pytest

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from order_review_ticket_models import CreateOrderReviewTicketBody
from order_review_ticket_repository import create_order_review_ticket_audit, setup_order_review_ticket_table
from order_review_ticket_summary import get_latest_order_review_ticket_audit, get_order_review_ticket_summary
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_order_review_ticket_table(database)
    return database


def ticket_payload(ticket_id, *, ready_count=1, blocked_count=0):
    return {
        "status": "success",
        "data": {
            "ticket_id": ticket_id,
            "mode": "manual_approval_ticket",
            "safety": "read_only_no_orders_submitted_no_orders_cancelled",
            "approval_required": True,
            "execution_enabled": False,
            "manual_confirmation_phrase": "APPROVE_ORDER_REVIEW_TICKET",
            "summary": {
                "ready_for_manual_approval_count": ready_count,
                "blocked_count": blocked_count,
                "orders_submitted": False,
                "orders_cancelled": False,
            },
            "approval_items": [
                {"symbol": "ACGL"},
                {"symbol": "ADBE"},
            ],
        },
    }


def create_ticket(db, ticket_id, *, account_id=1, source="manager-agent-hourly-workflow", ready_count=1, blocked_count=0):
    return create_order_review_ticket_audit(
        db,
        CreateOrderReviewTicketBody(
            account_id=account_id,
            source=source,
            ticket_payload=ticket_payload(ticket_id, ready_count=ready_count, blocked_count=blocked_count),
        ),
    )


def test_get_latest_order_review_ticket_audit_returns_newest_by_updated_at(db):
    create_ticket(db, "ticket-1")
    create_ticket(db, "ticket-2")

    latest = get_latest_order_review_ticket_audit(db)

    assert latest.ticket_id == "ticket-2"
    assert latest.status == "ready_for_manual_approval"


def test_get_latest_order_review_ticket_audit_filters_by_account_and_source(db):
    create_ticket(db, "ticket-1", account_id=1, source="manager-agent-hourly-workflow")
    create_ticket(db, "ticket-2", account_id=2, source="manager-agent-hourly-workflow")
    create_ticket(db, "ticket-3", account_id=1, source="other-source")

    latest = get_latest_order_review_ticket_audit(
        db,
        account_id="1",
        source="manager-agent-hourly-workflow",
    )

    assert latest.ticket_id == "ticket-1"
    assert latest.account_id == "1"
    assert latest.source == "manager-agent-hourly-workflow"


def test_get_order_review_ticket_summary_counts_statuses_and_items(db):
    create_ticket(db, "ticket-ready-1", ready_count=4, blocked_count=0)
    create_ticket(db, "ticket-ready-2", ready_count=2, blocked_count=0)
    create_ticket(db, "ticket-blocked", ready_count=0, blocked_count=3)

    summary = get_order_review_ticket_summary(db, account_id="1")

    assert summary["total_count"] == 3
    assert summary["ready_ticket_count"] == 2
    assert summary["blocked_ticket_count"] == 1
    assert summary["approval_required_count"] == 3
    assert summary["execution_enabled_count"] == 0
    assert summary["total_ready_items"] == 6
    assert summary["total_blocked_items"] == 3
    assert summary["latest_ticket"].ticket_id == "ticket-blocked"
    assert {item["status"]: item["count"] for item in summary["status_counts"]} == {
        "blocked": 1,
        "ready_for_manual_approval": 2,
    }


def test_get_order_review_ticket_summary_can_pin_latest_ticket_by_id(db):
    create_ticket(db, "ticket-ready-1", ready_count=4, blocked_count=0)
    create_ticket(db, "ticket-ready-2", ready_count=2, blocked_count=0)

    summary = get_order_review_ticket_summary(db, latest_ticket_id="ticket-ready-1")

    assert summary["latest_ticket"].ticket_id == "ticket-ready-1"
    assert summary["total_count"] == 2
