import os
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from policy_review_models import PolicyReviewAuditRecord
from policy_review_routes import create_policy_review_routes


async def get_correlation_id():
    return "corr-policy-api"


def get_api_key(api_key_header: str):
    if api_key_header == "test-key":
        return api_key_header
    raise AssertionError("unexpected api key")


def client():
    app = FastAPI()
    app.include_router(create_policy_review_routes(db=object(), get_api_key_dependency=get_api_key, get_correlation_id_dependency=get_correlation_id))
    return TestClient(app)


def headers():
    return {"X-API-KEY": "test-key", "X-Correlation-ID": "corr-policy-api"}


def policy_record(policy_review_id="review-api-1", status="review_required"):
    return PolicyReviewAuditRecord(
        policy_review_id=policy_review_id,
        account_id="1",
        symbol="AAPL",
        correlation_id="corr-policy-api",
        source="manager-agent",
        status=status,
        advisory_only=True,
        auto_apply=False,
        performance_summary={"net_pnl": 100},
        learning_result={"learning_state": "success"},
        curated_policy={"curation_state": status, "action_count": 2},
        metadata={"test": "policy_review_routes"},
    )


def test_create_policy_review_endpoint():
    payload = {
        "policy_review_id": "review-api-1",
        "account_id": "1",
        "symbol": "aapl",
        "correlation_id": "corr-policy-api",
        "source": "manager-agent",
        "advisory_only": True,
        "auto_apply": False,
        "performance_summary": {"net_pnl": 100},
        "learning_result": {"learning_state": "success"},
        "curated_policy": {"curation_state": "review_required", "action_count": 2},
        "metadata": {"test": "policy_review_routes"},
    }
    with patch("policy_review_routes.create_policy_review_audit", return_value=policy_record()) as create_record:
        response = client().post("/policy-reviews", json=payload, headers=headers())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["policy_review_id"] == "review-api-1"
    assert body["data"]["symbol"] == "AAPL"
    create_record.assert_called_once()


def test_list_policy_reviews_endpoint():
    records = [policy_record()]
    with patch("policy_review_routes.list_policy_review_audits", return_value=records) as list_records:
        response = client().get(
            "/policy-reviews?account_id=1&symbol=aapl&status=review_required&advisory_only=true&auto_apply=false&limit=50&offset=10&sort=created_at&order=asc",
            headers=headers(),
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"][0]["policy_review_id"] == "review-api-1"
    query = list_records.call_args.args[1]
    assert query.account_id == "1"
    assert query.symbol == "aapl"
    assert query.status == "review_required"
    assert query.advisory_only is True
    assert query.auto_apply is False
    assert query.limit == 50
    assert query.offset == 10
    assert query.sort == "created_at"
    assert query.order == "asc"


def test_get_policy_review_endpoint():
    with patch("policy_review_routes.get_policy_review_audit", return_value=policy_record()) as get_record:
        response = client().get("/policy-reviews/review-api-1", headers=headers())

    assert response.status_code == 200
    assert response.json()["data"]["policy_review_id"] == "review-api-1"
    get_record.assert_called_once()


def test_get_policy_review_endpoint_returns_404_when_missing():
    with patch("policy_review_routes.get_policy_review_audit", return_value=None):
        response = client().get("/policy-reviews/missing-review", headers=headers())

    assert response.status_code == 404
