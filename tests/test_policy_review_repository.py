import os

import pytest

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from policy_review_models import CreatePolicyReviewAuditBody, ListPolicyReviewAuditsQuery
from policy_review_repository import (
    create_policy_review_audit,
    get_policy_review_audit,
    list_policy_review_audits,
    setup_policy_review_table,
)
from trading_db import TradingDB


@pytest.fixture()
def db():
    database = TradingDB()
    database.setup_database()
    setup_policy_review_table(database)
    return database


def policy_body(policy_review_id="review-1", *, account_id=1, symbol="aapl", status=None, auto_apply=False):
    return CreatePolicyReviewAuditBody(
        policy_review_id=policy_review_id,
        account_id=account_id,
        symbol=symbol,
        correlation_id="corr-review-1",
        source="manager-agent",
        status=status,
        advisory_only=True,
        auto_apply=auto_apply,
        performance_summary={"closed_plan_count": 10, "net_pnl": 100},
        learning_result={"learning_state": "success", "policy_deltas": {"risk": {}}},
        curated_policy={"curation_state": status or "review_required", "action_count": 2},
        metadata={"test": "policy_review_repository"},
    )


def test_create_and_get_policy_review_audit(db):
    created = create_policy_review_audit(db, policy_body())
    fetched = get_policy_review_audit(db, "review-1")

    assert created.policy_review_id == "review-1"
    assert fetched.policy_review_id == "review-1"
    assert fetched.account_id == "1"
    assert fetched.symbol == "AAPL"
    assert fetched.status == "review_required"
    assert fetched.advisory_only is True
    assert fetched.auto_apply is False
    assert fetched.performance_summary["net_pnl"] == 100
    assert fetched.learning_result["learning_state"] == "success"
    assert fetched.curated_policy["action_count"] == 2
    assert fetched.metadata["test"] == "policy_review_repository"


def test_create_policy_review_defaults_id_and_status_from_curated_policy(db):
    created = create_policy_review_audit(
        db,
        CreatePolicyReviewAuditBody(
            account_id="1",
            symbol="msft",
            curated_policy={"curation_state": "observation_only"},
        ),
    )

    assert created.policy_review_id.startswith("policy-review-")
    assert created.status == "observation_only"
    assert created.symbol == "MSFT"


def test_list_policy_review_audits_filters_and_sorts(db):
    create_policy_review_audit(db, policy_body("review-1", account_id=1, symbol="aapl", status="review_required"))
    create_policy_review_audit(db, policy_body("review-2", account_id=1, symbol="msft", status="observation_only"))
    create_policy_review_audit(db, policy_body("review-3", account_id=2, symbol="aapl", status="review_required"))

    records = list_policy_review_audits(
        db,
        ListPolicyReviewAuditsQuery(
            account_id=1,
            symbol="aapl",
            status="review_required",
            advisory_only=True,
            auto_apply=False,
            sort="created_at",
            order="asc",
        ),
    )

    assert [record.policy_review_id for record in records] == ["review-1"]
    assert records[0].symbol == "AAPL"
    assert records[0].status == "review_required"


def test_list_policy_review_audits_supports_limit_offset(db):
    create_policy_review_audit(db, policy_body("review-1"))
    create_policy_review_audit(db, policy_body("review-2"))
    create_policy_review_audit(db, policy_body("review-3"))

    records = list_policy_review_audits(
        db,
        ListPolicyReviewAuditsQuery(limit=2, offset=1, sort="created_at", order="asc"),
    )

    assert len(records) == 2
    assert [record.policy_review_id for record in records] == ["review-2", "review-3"]
