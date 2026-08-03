from __future__ import annotations

import hmac
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from backtest_promotion_base import PromotionError
from backtest_promotion_observation_models import ObserveBacktestPromotionBody
from backtest_promotion_observation_service import (
    list_backtest_promotion_observations,
    observe_backtest_promotion,
)
from backtest_promotion_routes import _envelope, _promotion_error_response


def _require_observation_credential(value: Optional[str]) -> None:
    expected = os.getenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "")
    if not expected or not value:
        raise HTTPException(status_code=403, detail="observation credential required")
    if not hmac.compare_digest(value.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=403, detail="observation credential invalid")


def create_backtest_promotion_observation_routes(
    db,
    get_api_key_dependency,
    get_correlation_id_dependency,
) -> APIRouter:
    router = APIRouter(
        prefix="/backtests/promotion-observations",
        tags=["backtest-promotion-observations"],
    )
    service_key = APIKeyHeader(
        name="X-API-KEY",
        scheme_name="DatabaseAgentAPIKey",
        auto_error=False,
    )
    observation_key = APIKeyHeader(
        name="X-PROMOTION-APPROVAL-KEY",
        scheme_name="BacktestPromotionObservationKey",
        auto_error=False,
    )

    async def _api_key(value: str = Security(service_key)):
        return get_api_key_dependency(value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/{promotion_id}", response_model=dict)
    async def observe_endpoint(
        promotion_id: str,
        body: ObserveBacktestPromotionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
        credential: Optional[str] = Security(observation_key),
    ):
        _require_observation_credential(credential)
        try:
            record = observe_backtest_promotion(
                db,
                promotion_id,
                body,
                correlation_id,
            )
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=record.model_dump(mode="json"),
            metadata={
                "idempotent_replay": record.idempotent_replay,
                "authority": "database-agent",
                "safe_for_trading": record.to_state
                in {"APPROVED_FOR_PAPER", "PAPER_OBSERVING"},
            },
        )

    @router.get("/{promotion_id}", response_model=dict)
    async def list_observations_endpoint(
        promotion_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_backtest_promotion_observations(db, promotion_id)
        return _envelope(
            correlation_id=correlation_id,
            data=[record.model_dump(mode="json") for record in records],
            metadata={"count": len(records), "authority": "database-agent"},
        )

    return router
