from app import runtime as base_runtime
from app.application import create_application
from app.route_registry import is_http_signature, route_signature
from fastapi.testclient import TestClient
import position_bucket_repository as bucket_repository


class RuntimeStub:
    def __getattr__(self, name):
        return getattr(base_runtime, name)

    @staticmethod
    def get_api_key():
        return "test-api-key"

    @staticmethod
    def get_correlation_id():
        return "bucket-contract-test"

    async def startup_event(self):
        return None

    async def shutdown_event(self):
        return None


def _client():
    runtime = RuntimeStub()
    app = create_application(runtime)
    return app, TestClient(app)


def test_modular_application_mounts_manager_bucket_contracts_once():
    app, _ = _client()
    signatures = [
        signature
        for route in app.router.routes
        if is_http_signature(signature := route_signature(route))
    ]

    for path, method in (
        ("/accounts/{account_id}/strategy-bucket-assignments", "GET"),
        ("/accounts/{account_id}/position-buckets", "GET"),
        ("/accounts/{account_id}/position-buckets/{symbol}", "PATCH"),
        ("/accounts/{account_id}/position-buckets/bulk", "POST"),
    ):
        assert signatures.count((path, frozenset({method}))) == 1


def test_strategy_bucket_assignment_response_matches_manager_preflight(monkeypatch):
    expected = [
        {
            "account_id": 1,
            "symbol": "AAPL",
            "strategy_bucket": "quality_growth",
            "source": "approved_registry",
            "reason": "contract-test",
        }
    ]
    monkeypatch.setattr(
        bucket_repository,
        "list_strategy_bucket_assignments",
        lambda db, account_id: expected if account_id == 1 else [],
    )
    _, client = _client()

    response = client.get("/accounts/1/strategy-bucket-assignments")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["assignments"] == expected
    assert data["count"] == 1


def test_bulk_position_bucket_response_matches_manager_preflight(monkeypatch):
    captured = {}

    def fake_bulk(db, account_id, assignments, *, default_source):
        captured.update(
            account_id=account_id,
            assignments=assignments,
            default_source=default_source,
        )
        return [
            {
                "account_id": account_id,
                "symbol": item["symbol"],
                "strategy_bucket": item["strategy_bucket"],
            }
            for item in assignments
        ]

    monkeypatch.setattr(bucket_repository, "bulk_upsert_position_buckets", fake_bulk)
    _, client = _client()
    assignments = [
        {
            "account_id": 1,
            "symbol": "AAPL",
            "strategy_bucket": "quality_growth",
            "source": "approved_registry",
            "reason": "contract-test",
        },
        {
            "account_id": 1,
            "symbol": "CINF",
            "strategy_bucket": "value_rebound",
            "source": "approved_registry",
            "reason": "contract-test",
        },
    ]

    response = client.post(
        "/accounts/1/position-buckets/bulk",
        json={"source": "approved_registry", "assignments": assignments},
    )

    assert response.status_code == 200
    assert captured == {
        "account_id": 1,
        "assignments": assignments,
        "default_source": "approved_registry",
    }
    data = response.json()["data"]
    assert data["updated_count"] == 2
    assert data["requested_count"] == 2
    assert [row["symbol"] for row in data["updated"]] == ["AAPL", "CINF"]


def test_bulk_position_bucket_rejects_non_list_assignments():
    _, client = _client()

    response = client.post(
        "/accounts/1/position-buckets/bulk",
        json={"assignments": {"AAPL": "quality_growth"}},
    )

    assert response.status_code == 422
