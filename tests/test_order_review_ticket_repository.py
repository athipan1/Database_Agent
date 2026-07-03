import os

import pytest

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from order_review_ticket_models import CreateOrderReviewTicketBody, ListOrderReviewTicketsQuery
from order_review_ticket_repository import (
    create_order_review_ticket_audit,
    get_order_review_ticket_audit,
    list_order_review_ticket_audits,
    setup_order_review_ticket_table,
)
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_order_review_ticket_table(database)
    return database


def approval_ticket_payload(ticket_id="order-review-test-1", *, blocked_count=0):
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
                "ready_for_manual_approval_count": 4 if blocked_count == 0 else 0,
                "blocked_count": blocked_count,
                "orders_submitted": False,
                "orders_cancelled": False,
            },
            "approval_items": [
                {
                    "symbol": "ACGL",
                    "qty": "82",
                    "current_stop_order_id": "stop-acgl",
                    "stop_price": 92.94,
                    "take_profit_price": 120.72,
                    "approval_status": "manual_approval_required",
                },
                {
                    "symbol": "ADBE",
                    "qty": "52",
                    "current_stop_order_id": "stop-adbe",
                    "stop_price": 190.12,
                    "take_profit_price": 278.92,
                    "approval_status": "manual_approval_required",
                },
            ],
        },
    }


def create_body(ticket_id="order-review-test-1", *, account_id=1, blocked_count=0):
    return CreateOrderReviewTicketBody(
        account_id=account_id,
        source="manager-agent",
        ticket_payload=approval_ticket_payload(ticket_id, blocked_count=blocked_count),
        metadata={"workflow_run_id": "123"},
    )


def test_create_and_get_order_review_ticket_audit_from_execution_payload(db):
    created = create_order_review_ticket_audit(db, create_body())
    fetched = get_order_review_ticket_audit(db, "order-review-test-1")

    assert created.ticket_id == "order-review-test-1"
    assert fetched.ticket_id == "order-review-test-1"
    assert fetched.account_id == "1"
    assert fetched.mode == "manual_approval_ticket"
    assert fetched.safety == "read_only_no_orders_submitted_no_orders_cancelled"
    assert fetched.status == "ready_for_manual_approval"
    assert fetched.approval_required is True
    assert fetched.execution_enabled is False
    assert fetched.manual_confirmation_phrase == "APPROVE_ORDER_REVIEW_TICKET"
    assert fetched.ready_count == 4
    assert fetched.blocked_count == 0
    assert fetched.orders_submitted is False
    assert fetched.orders_cancelled is False
    assert fetched.requested_symbols == ["ACGL", "ADBE"]
    assert fetched.ticket_payload["data"]["ticket_id"] == "order-review-test-1"
    assert fetched.metadata["workflow_run_id"] == "123"


def test_create_order_review_ticket_marks_blocked_when_ticket_has_blocked_items(db):
    created = create_order_review_ticket_audit(db, create_body("order-review-blocked", blocked_count=2))

    assert created.ticket_id == "order-review-blocked"
    assert created.status == "blocked"
    assert created.ready_count == 0
    assert created.blocked_count == 2
    assert created.execution_enabled is False


def test_create_order_review_ticket_accepts_explicit_fields(db):
    created = create_order_review_ticket_audit(
        db,
        CreateOrderReviewTicketBody(
            ticket_id="explicit-ticket",
            account_id="paper-1",
            status="ready_for_manual_approval",
            requested_symbols=["cinf"],
            ready_count=1,
            blocked_count=0,
            approval_required=True,
            execution_enabled=False,
            orders_submitted=False,
            orders_cancelled=False,
            manual_confirmation_phrase="APPROVE_ORDER_REVIEW_TICKET",
            ticket_payload={"data": {"ticket_id": "ignored-by-explicit"}},
        ),
    )

    assert created.ticket_id == "explicit-ticket"
    assert created.account_id == "paper-1"
    assert created.requested_symbols == ["cinf"]
    assert created.ready_count == 1
    assert created.status == "ready_for_manual_approval"


def test_list_order_review_ticket_audits_filters_and_sorts(db):
    create_order_review_ticket_audit(db, create_body("ticket-1", account_id=1))
    create_order_review_ticket_audit(db, create_body("ticket-2", account_id=2))
    create_order_review_ticket_audit(db, create_body("ticket-3", account_id=1, blocked_count=1))

    records = list_order_review_ticket_audits(
        db,
        ListOrderReviewTicketsQuery(
            account_id=1,
            status="ready_for_manual_approval",
            approval_required=True,
            execution_enabled=False,
            sort="created_at",
            order="asc",
        ),
    )

    assert [record.ticket_id for record in records] == ["ticket-1"]
    assert records[0].status == "ready_for_manual_approval"
    assert records[0].execution_enabled is False


def test_list_order_review_ticket_audits_supports_limit_offset(db):
    create_order_review_ticket_audit(db, create_body("ticket-1"))
    create_order_review_ticket_audit(db, create_body("ticket-2"))
    create_order_review_ticket_audit(db, create_body("ticket-3"))

    records = list_order_review_ticket_audits(
        db,
        ListOrderReviewTicketsQuery(limit=2, offset=1, sort="created_at", order="asc"),
    )

    assert len(records) == 2
    assert [record.ticket_id for record in records] == ["ticket-2", "ticket-3"]
