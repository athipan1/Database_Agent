from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from openapi_spec_validator import validate

from backtest_promotion_routes import create_backtest_promotion_routes


def _app() -> FastAPI:
    app = FastAPI(title="Promotion Contract Test", version="1.0.0")

    def api_key(value):
        return value

    async def correlation_id():
        return "corr-openapi"

    app.include_router(
        create_backtest_promotion_routes(
            SimpleNamespace(),
            api_key,
            correlation_id,
        )
    )
    return app


def test_promotion_openapi_is_valid_and_complete():
    document = _app().openapi()
    validate(document)

    paths = document["paths"]
    assert "/backtests/promotions" in paths
    assert "/backtests/promotions/latest/exact" in paths
    assert "/backtests/promotions/{promotion_id}" in paths
    assert "/backtests/promotions/{promotion_id}/transition" in paths
    assert "/backtests/promotions/{promotion_id}/history" in paths
    assert "/backtests/promotions/{promotion_id}/revoke" in paths

    assert "post" in paths["/backtests/promotions"]
    assert "get" in paths["/backtests/promotions/latest/exact"]
    assert "post" in paths["/backtests/promotions/{promotion_id}/transition"]
    assert "post" in paths["/backtests/promotions/{promotion_id}/revoke"]

    security_schemes = document["components"]["securitySchemes"]
    header_names = {
        scheme.get("name")
        for scheme in security_schemes.values()
        if scheme.get("type") == "apiKey" and scheme.get("in") == "header"
    }
    assert "X-API-KEY" in header_names
    assert "X-PROMOTION-APPROVAL-KEY" in header_names


def test_transition_contract_references_strict_state_machine_body():
    document = _app().openapi()
    operation = document["paths"][
        "/backtests/promotions/{promotion_id}/transition"
    ]["post"]
    schema = operation["requestBody"]["content"]["application/json"]["schema"]
    assert schema["$ref"].endswith("/TransitionBacktestPromotionBody")

    transition_schema = document["components"]["schemas"][
        "TransitionBacktestPromotionBody"
    ]
    required = set(transition_schema["required"])
    assert {
        "expected_state",
        "expected_version",
        "next_state",
        "reason_code",
        "reason",
        "evidence_run_id",
    }.issubset(required)
    assert transition_schema["additionalProperties"] is False
