from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from fastapi.security import APIKeyHeader

from app.core.responses import AGENT_VERSION
from backtest_promotion_metrics import PROMOTION_LOOKUP_FAILURES
from backtest_promotion_models import (
    CreateBacktestPromotionBody,
    PromotionState,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_repository import (
    PromotionError,
    create_backtest_promotion,
    get_backtest_promotion,
    get_latest_exact_backtest_promotion,
    list_backtest_promotion_history,
    revoke_backtest_promotion,
    transition_backtest_promotion,
)


logger = logging.getLogger(__name__)
SCHEMA_VERSION = "backtest-promotion.v1"


def _request_correlation_id(request: Request) -> Optional[str]:
    return getattr(request.state, "correlation_id", None) or request.headers.get(
        "X-Correlation-ID"
    )


class PromotionAPIRoute(APIRoute):
    """Keep dependency and Pydantic errors inside the promotion envelope."""

    def get_route_handler(self):
        original_route_handler = super().get_route_handler()

        async def promotion_route_handler(request: Request):
            correlation_id = _request_correlation_id(request)
            try:
                return await original_route_handler(request)
            except RequestValidationError as exc:
                return JSONResponse(
                    status_code=422,
                    content=jsonable_encoder(
                        _envelope(
                            correlation_id=correlation_id,
                            status="error",
                            metadata={"validation_errors": exc.errors()},
                            error={
                                "code": "validation_failed",
                                "message": "Promotion request validation failed.",
                                "retryable": False,
                            },
                        )
                    ),
                )
            except HTTPException as exc:
                code = (
                    "authentication_failed"
                    if exc.status_code in {401, 403}
                    else "validation_failed"
                )
                message = (
                    "Promotion API authentication failed."
                    if code == "authentication_failed"
                    else "Promotion request could not be accepted."
                )
                return JSONResponse(
                    status_code=exc.status_code,
                    content=jsonable_encoder(
                        _envelope(
                            correlation_id=correlation_id,
                            status="error",
                            error={
                                "code": code,
                                "message": message,
                                "retryable": False,
                            },
                        )
                    ),
                )

        return promotion_route_handler


def _envelope(
    *,
    correlation_id: Optional[str],
    data: Any = None,
    status: str = "success",
    metadata: Optional[dict] = None,
    error: Optional[dict] = None,
) -> dict:
    return {
        "status": status,
        "agent_type": "database-agent",
        "version": AGENT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": metadata or {},
        "error": error,
        "confidence_score": None,
    }


def _promotion_error_response(
    exc: PromotionError,
    *,
    correlation_id: Optional[str],
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.http_status,
        content=jsonable_encoder(
            _envelope(
                correlation_id=correlation_id,
                status="error",
                metadata=exc.metadata,
                error={
                    "code": exc.code,
                    "message": str(exc),
                    "retryable": exc.code
                    in {"stale_version", "database_conflict", "promotion_lookup_failed"},
                },
            )
        ),
    )


def _internal_error_response(*, correlation_id: Optional[str]) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content=jsonable_encoder(
            _envelope(
                correlation_id=correlation_id,
                status="error",
                error={
                    "code": "database_conflict",
                    "message": "The promotion operation could not be completed safely.",
                    "retryable": True,
                },
            )
        ),
    )


def create_backtest_promotion_routes(
    db,
    get_api_key_dependency,
    get_correlation_id_dependency,
) -> APIRouter:
    router = APIRouter(
        prefix="/backtests/promotions",
        tags=["backtest-promotions"],
        route_class=PromotionAPIRoute,
    )
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("", response_model=dict, status_code=201)
    async def create_promotion_endpoint(
        body: CreateBacktestPromotionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            promotion = create_backtest_promotion(db, body, correlation_id)
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            logger.exception(
                "promotion creation failed",
                extra={"event": "promotion_transition_rejected"},
            )
            return _internal_error_response(correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=promotion.model_dump(mode="json"),
            metadata={
                "idempotent_replay": promotion.idempotent_replay,
                "authority": "database-agent",
                "safe_for_trading": False,
            },
        )

    # Static path must be registered before /{promotion_id}.
    @router.get("/latest/exact", response_model=dict)
    async def get_latest_exact_promotion_endpoint(
        account_id: str = Query(..., min_length=1, max_length=128),
        symbol: str = Query(..., min_length=1, max_length=20),
        strategy_id: str = Query(..., min_length=1, max_length=256),
        timeframe: str = Query(..., min_length=1, max_length=32),
        required_state: Optional[PromotionState] = Query(default=None),
        max_age_hours: Optional[int] = Query(default=None, ge=1, le=8760),
        validation_profile: Optional[str] = Query(default=None, max_length=128),
        engine_version: Optional[str] = Query(default=None, max_length=128),
        dataset_fingerprint: Optional[str] = Query(default=None, min_length=32, max_length=128),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            promotion = get_latest_exact_backtest_promotion(
                db,
                account_id=account_id,
                symbol=symbol,
                strategy_id=strategy_id,
                timeframe=timeframe,
                required_state=required_state,
                max_age_hours=max_age_hours,
                validation_profile=validation_profile,
                engine_version=engine_version,
                dataset_fingerprint=dataset_fingerprint,
            )
        except PromotionError as exc:
            PROMOTION_LOOKUP_FAILURES.labels(error_code=exc.code).inc()
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            PROMOTION_LOOKUP_FAILURES.labels(
                error_code="promotion_lookup_failed"
            ).inc()
            logger.exception(
                "promotion exact lookup failed",
                extra={"event": "promotion_lookup_failed"},
            )
            return JSONResponse(
                status_code=503,
                content=jsonable_encoder(
                    _envelope(
                        correlation_id=correlation_id,
                        status="error",
                        error={
                            "code": "promotion_lookup_failed",
                            "message": "Exact promotion lookup failed closed.",
                            "retryable": True,
                        },
                    )
                ),
            )
        return _envelope(
            correlation_id=correlation_id,
            data=promotion.model_dump(mode="json"),
            metadata={
                "lookup": "exact_backtest_promotion_v1",
                "exact_match": True,
                "authority": "database-agent",
                "safe_for_trading": promotion.state
                in {"APPROVED_FOR_PAPER", "PAPER_OBSERVING"},
            },
        )

    @router.get("/{promotion_id}", response_model=dict)
    async def get_promotion_endpoint(
        promotion_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            promotion = get_backtest_promotion(db, promotion_id)
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            logger.exception("promotion lookup failed")
            return _internal_error_response(correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=promotion.model_dump(mode="json"),
            metadata={"authority": "database-agent"},
        )

    @router.post("/{promotion_id}/transition", response_model=dict)
    async def transition_promotion_endpoint(
        promotion_id: str,
        body: TransitionBacktestPromotionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            promotion = transition_backtest_promotion(
                db,
                promotion_id,
                body,
                correlation_id,
            )
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            logger.exception(
                "promotion transition failed",
                extra={"event": "promotion_transition_rejected"},
            )
            return _internal_error_response(correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=promotion.model_dump(mode="json"),
            metadata={
                "idempotent_replay": promotion.idempotent_replay,
                "authority": "database-agent",
                "safe_for_trading": promotion.state
                in {"APPROVED_FOR_PAPER", "PAPER_OBSERVING"},
            },
        )

    @router.get("/{promotion_id}/history", response_model=dict)
    async def get_promotion_history_endpoint(
        promotion_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            history = list_backtest_promotion_history(db, promotion_id)
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            logger.exception("promotion history lookup failed")
            return _internal_error_response(correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=[item.model_dump(mode="json") for item in history],
            metadata={"count": len(history), "authority": "database-agent"},
        )

    @router.post("/{promotion_id}/revoke", response_model=dict)
    async def revoke_promotion_endpoint(
        promotion_id: str,
        body: RevokeBacktestPromotionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            promotion = revoke_backtest_promotion(
                db,
                promotion_id,
                body,
                correlation_id,
            )
        except PromotionError as exc:
            return _promotion_error_response(exc, correlation_id=correlation_id)
        except Exception:
            logger.exception(
                "promotion revocation failed",
                extra={"event": "promotion_transition_rejected"},
            )
            return _internal_error_response(correlation_id=correlation_id)
        return _envelope(
            correlation_id=correlation_id,
            data=promotion.model_dump(mode="json"),
            metadata={
                "idempotent_replay": promotion.idempotent_replay,
                "authority": "database-agent",
                "safe_for_trading": False,
            },
        )

    return router
