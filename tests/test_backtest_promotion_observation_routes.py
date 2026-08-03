from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backtest_promotion_observation_routes as routes
from backtest_promotion_observation_models import (
    BacktestPromotionObservationRecord,
)


class StubDB:
    pass


async def _correlation_id() -> str:
    return "corr-route-test"


def _api_key(value: str | None) -> str:
    if value != "service-key":
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail="invalid api key")
    return value


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(
        routes.create_backtest_promotion_observation_routes(
            StubDB(),
            _api_key,
            _correlation_id,
        )
    )
    routes.install_backtest_promotion_observation_openapi(app)
    return TestClient(app)


def _record() -> BacktestPromotionObservationRecord:
    timestamp = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    promotion = {
        "promotion_id": "promotion-1",
        "state": "PAPER_OBSERVING",
        "version": 2,
    }
    return BacktestPromotionObservationRecord(
        observation_id="promotion-observation-route-test",
        promotion_id="promotion-1",
        observation_key="route-test",
        action="START_OBSERVING",
        reason_code="paper_observation_started",
        from_state="APPROVED_FOR_PAPER",
        to_state="PAPER_OBSERVING",
        from_version=1,
        to_version=2,
        observed_at=timestamp,
        created_at=timestamp,
        correlation_id="corr-route-test",
        paper_drawdown_pct=0.0,
        reconciliation_ok=True,
        duplicate_order_count=0,
        broker_order_count=0,
        database_order_count=0,
        filled_order_count=0,
        strategy_drift=False,
        emergency_halt=False,
        metadata={"source": "route-test"},
        promotion=promotion,
        idempotent_replay=False,
    )


def _payload() -> dict:
    return {
        "expected_state": "APPROVED_FOR_PAPER",
        "expected_version": 1,
        "observation_key": "route-test",
        "observed_at": "2026-08-03T12:00:00Z",
    }


def test_openapi_requires_both_security_headers(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "approval-key")
    schema = _client().get("/openapi.json").json()
    operation = schema["paths"][
        "/backtests/promotion-observations/{promotion_id}"
    ]["post"]
    schemes = schema["components"]["securitySchemes"]

    assert schemes["DatabaseAgentAPIKey"]["name"] == "X-API-KEY"
    assert (
        schemes["BacktestPromotionObservationKey"]["name"]
        == "X-PROMOTION-APPROVAL-KEY"
    )
    assert operation["security"] == [
        {
            "DatabaseAgentAPIKey": [],
            "BacktestPromotionObservationKey": [],
        }
    ]


def test_post_requires_service_and_observation_credentials(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "approval-key")
    client = _client()

    missing_service = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=_payload(),
        headers={"X-PROMOTION-APPROVAL-KEY": "approval-key"},
    )
    missing_observation = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=_payload(),
        headers={"X-API-KEY": "service-key"},
    )
    invalid_observation = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=_payload(),
        headers={
            "X-API-KEY": "service-key",
            "X-PROMOTION-APPROVAL-KEY": "wrong",
        },
    )

    assert missing_service.status_code == 403
    assert missing_observation.status_code == 403
    assert invalid_observation.status_code == 403


def test_missing_config_fails_closed(monkeypatch):
    monkeypatch.delenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", raising=False)
    client = _client()
    response = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=_payload(),
        headers={
            "X-API-KEY": "service-key",
            "X-PROMOTION-APPROVAL-KEY": "approval-key",
        },
    )

    assert response.status_code == 403
    assert os.getenv("BACKTEST_PROMOTION_APPROVAL_TOKEN") is None


def test_post_returns_observation_envelope(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "approval-key")
    monkeypatch.setattr(
        routes,
        "observe_backtest_promotion",
        lambda *args, **kwargs: _record(),
    )
    response = _client().post(
        "/backtests/promotion-observations/promotion-1",
        json=_payload(),
        headers={
            "X-API-KEY": "service-key",
            "X-PROMOTION-APPROVAL-KEY": "approval-key",
        },
    )

    assert response.status_code == 200
    document = response.json()
    assert document["data"]["to_state"] == "PAPER_OBSERVING"
    assert document["metadata"]["safe_for_trading"] is True
    assert document["metadata"]["idempotent_replay"] is False


def test_get_lists_observations(monkeypatch):
    monkeypatch.setattr(
        routes,
        "list_backtest_promotion_observations",
        lambda *args, **kwargs: [_record()],
    )
    response = _client().get(
        "/backtests/promotion-observations/promotion-1",
        headers={"X-API-KEY": "service-key"},
    )

    assert response.status_code == 200
    document = response.json()
    assert document["metadata"]["count"] == 1
    assert document["data"][0]["observation_key"] == "route-test"
