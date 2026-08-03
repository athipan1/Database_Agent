from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backtest_promotion_observation_routes import (
    create_backtest_promotion_observation_routes,
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
        create_backtest_promotion_observation_routes(
            StubDB(),
            _api_key,
            _correlation_id,
        )
    )
    return TestClient(app)


def test_openapi_exposes_both_security_headers(monkeypatch):
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
    security_names = {
        name
        for requirement in operation["security"]
        for name in requirement
    }
    assert security_names == {
        "DatabaseAgentAPIKey",
        "BacktestPromotionObservationKey",
    }


def test_post_requires_service_and_observation_credentials(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "approval-key")
    client = _client()
    payload = {
        "expected_state": "APPROVED_FOR_PAPER",
        "expected_version": 1,
        "observation_key": "route-test",
        "observed_at": "2026-08-03T12:00:00Z",
    }

    missing_service = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=payload,
        headers={"X-PROMOTION-APPROVAL-KEY": "approval-key"},
    )
    missing_observation = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=payload,
        headers={"X-API-KEY": "service-key"},
    )
    invalid_observation = client.post(
        "/backtests/promotion-observations/promotion-1",
        json=payload,
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
        json={
            "expected_state": "APPROVED_FOR_PAPER",
            "expected_version": 1,
            "observation_key": "route-test",
            "observed_at": "2026-08-03T12:00:00Z",
        },
        headers={
            "X-API-KEY": "service-key",
            "X-PROMOTION-APPROVAL-KEY": "approval-key",
        },
    )

    assert response.status_code == 403
    assert os.getenv("BACKTEST_PROMOTION_APPROVAL_TOKEN") is None
