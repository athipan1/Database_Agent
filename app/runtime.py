"""Patch-compatible runtime dependencies for the Database Agent API.

The module is deliberately free of FastAPI route declarations.  It exposes the
objects and helper functions consumed by router factories, startup orchestration,
and existing tests that patch attributes through ``main``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from fastapi import HTTPException

from alpaca_client import AlpacaClient
from app.core.config import Settings
from app.core.logging import configure_logging
from app.core.middleware import (
    create_correlation_id_context,
    create_correlation_id_dependency,
)
from app.core.responses import wrap_response
from app.core.security import create_api_key_dependency
from app.services.market_data import (
    build_default_portfolio_metrics,
    build_mock_price_history,
    ingest_data_for_symbol_timeframe as ingest_market_data,
    run_ingestion_job as run_market_data_ingestion,
)
from app.services.scheduler import RuntimeScheduler
from app.startup import (
    log_database_stats as collect_database_stats,
    shutdown_runtime,
    startup_runtime,
)
from broker_sync_repository import sync_broker_state
from execution_job_repository import (
    claim_next_execution_job,
    create_execution_job,
    get_execution_job,
    get_execution_job_by_order_id,
    update_execution_job,
)
from fill_repository import create_fill_record, get_fill_records
from history_repository import (
    create_performance_record,
    create_signal_record,
    get_performance_records,
    get_signal_records,
)
from models import CreateOrderBody, PortfolioMetrics, Price
from protective_order_repository import (
    normalize_order_protective_metadata,
    persist_protective_order_metadata,
    setup_protective_order_columns,
)
from risk_approval_repository import (
    create_risk_approval,
    get_risk_approval,
    mark_risk_approval_used,
)
from session_risk_repository import build_session_risk_snapshot
from trading_db import TradingDB


load_dotenv()

TRADING_MODE = "PAPER"
DATABASE_DEV_MODE = False
DATABASE_EMERGENCY_HALT = False
DATABASE_AGENT_API_KEY: Optional[str] = None
DEFAULT_DEV_ACCOUNT_ID = "1"
DEFAULT_DEV_CASH_BALANCE = Decimal("100000")
ALPACA_API_KEY: Optional[str] = None
ALPACA_SECRET_KEY: Optional[str] = None


def apply_settings(settings: Settings) -> None:
    """Apply a validated settings snapshot to patch-compatible module globals."""

    global TRADING_MODE
    global DATABASE_DEV_MODE
    global DATABASE_EMERGENCY_HALT
    global DATABASE_AGENT_API_KEY
    global DEFAULT_DEV_ACCOUNT_ID
    global DEFAULT_DEV_CASH_BALANCE
    global ALPACA_API_KEY
    global ALPACA_SECRET_KEY

    TRADING_MODE = settings.trading_mode
    DATABASE_DEV_MODE = settings.database_dev_mode
    DATABASE_EMERGENCY_HALT = settings.database_emergency_halt
    DATABASE_AGENT_API_KEY = settings.database_agent_api_key
    DEFAULT_DEV_ACCOUNT_ID = settings.default_dev_account_id
    DEFAULT_DEV_CASH_BALANCE = settings.default_dev_cash_balance
    ALPACA_API_KEY = settings.alpaca_api_key
    ALPACA_SECRET_KEY = settings.alpaca_secret_key


def current_settings() -> Settings:
    """Return settings from current globals so tests can patch them dynamically."""

    return Settings(
        trading_mode=TRADING_MODE,
        database_dev_mode=DATABASE_DEV_MODE,
        database_emergency_halt=DATABASE_EMERGENCY_HALT,
        database_agent_api_key=DATABASE_AGENT_API_KEY,
        default_dev_account_id=DEFAULT_DEV_ACCOUNT_ID,
        default_dev_cash_balance=DEFAULT_DEV_CASH_BALANCE,
        alpaca_api_key=ALPACA_API_KEY,
        alpaca_secret_key=ALPACA_SECRET_KEY,
    )


apply_settings(Settings.from_environ())

correlation_id_var = create_correlation_id_context()
get_correlation_id = create_correlation_id_dependency(correlation_id_var)
get_api_key = create_api_key_dependency(current_settings)
configure_logging(correlation_id_var)

db = TradingDB()
alpaca_client = AlpacaClient(
    api_key=ALPACA_API_KEY,
    secret_key=ALPACA_SECRET_KEY,
)
runtime_scheduler = RuntimeScheduler()
app = None


def _normalize_order_or_404(
    order: Optional[Dict[str, Any]],
    message: str,
) -> Dict[str, Any]:
    if not order:
        raise HTTPException(status_code=404, detail=message)
    return normalize_order_protective_metadata(order)


def _mock_price_history(symbol: str, limit: int = 100) -> List[Price]:
    return build_mock_price_history(symbol, limit=limit)


def _default_portfolio_metrics() -> PortfolioMetrics:
    return build_default_portfolio_metrics()


def _order_body_to_create_args(
    account_id: Union[int, str],
    body: CreateOrderBody,
    correlation_id: str,
) -> dict:
    return {
        "account_id": account_id,
        "trade_id": str(body.trade_id),
        "symbol": body.symbol,
        "side": body.side.value if hasattr(body.side, "value") else str(body.side),
        "order_type": (
            body.order_type.value
            if hasattr(body.order_type, "value")
            else str(body.order_type)
        ),
        "quantity": int(body.quantity),
        "price": body.price,
        "time_in_force": (
            body.time_in_force.value
            if hasattr(body.time_in_force, "value")
            else str(body.time_in_force)
        ),
        "correlation_id": correlation_id,
    }


def ingest_data_for_symbol_timeframe(
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> None:
    ingest_market_data(
        db,
        alpaca_client,
        symbol,
        timeframe,
        start_date,
        end_date,
    )


def run_ingestion_job() -> None:
    run_market_data_ingestion(db, alpaca_client)


def log_database_stats() -> None:
    collect_database_stats(db)


async def startup_event() -> None:
    import sys

    await startup_runtime(sys.modules[__name__])


async def shutdown_event() -> None:
    import sys

    await shutdown_runtime(sys.modules[__name__])
