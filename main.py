import os
import logging
import sys
import uuid
import schedule
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Security, Request, Body
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Optional, Any, List, Union
from decimal import Decimal
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator

from trading_db import TradingDB
from alpaca_client import AlpacaClient
from models import (
    AccountBalance, Position, Order, CreateOrderBody,
    OrderExecutionResponse, ExecutionTrade, Price, StandardAgentResponse,
    PortfolioMetrics, SignalHistory, CreateSignalHistoryBody,
    PerformanceMetric, CreatePerformanceMetricBody,
)

# --- Context setup for Correlation ID ---
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)

# --- Configuration & Setup ---
load_dotenv()

DATABASE_DEV_MODE = os.getenv("DATABASE_DEV_MODE", "false").lower() in ("1", "true", "yes", "y")
DEFAULT_DEV_ACCOUNT_ID = os.getenv("DEFAULT_DEV_ACCOUNT_ID", "1")
DEFAULT_DEV_CASH_BALANCE = Decimal(os.getenv("DEFAULT_DEV_CASH_BALANCE", "100000"))

# In-memory fallback stores for signal/performance history until TradingDB persistence is added.
SIGNAL_HISTORY_STORE: List[SignalHistory] = []
PERFORMANCE_METRICS_STORE: List[PerformanceMetric] = []

# Configure logging
log_handler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s',
    timestamp=True
)
log_handler.setFormatter(formatter)
logging.getLogger().addHandler(log_handler)
logging.getLogger().setLevel(logging.INFO)

class CorrelationIdFilter(logging.Filter):
    """Injects the correlation_id into log records."""
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logging.getLogger().addFilter(CorrelationIdFilter())

app = FastAPI(title="Database Agent - Secure Trading API")
Instrumentator().instrument(app).expose(app)

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
    token = correlation_id_var.set(correlation_id)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    correlation_id_var.reset(token)
    return response

async def get_correlation_id() -> str:
    return correlation_id_var.get() or str(uuid.uuid4())

def wrap_response(data: Any = None, status: str = "success", error: Optional[dict] = None):
    """Wraps the data into the standard agent response format."""
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": error,
        "confidence_score": None,
    }

# API Key Security
DATABASE_AGENT_API_KEY = os.environ.get("DATABASE_AGENT_API_KEY")
if not DATABASE_AGENT_API_KEY and not DATABASE_DEV_MODE:
    logging.critical("CRITICAL: DATABASE_AGENT_API_KEY environment variable not set. Application will terminate.")
    sys.exit(1)
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

def get_api_key(api_key_header: str = Security(api_key_header)):
    """Validate the API key. DATABASE_DEV_MODE allows local integration tests without hard failure."""
    if DATABASE_DEV_MODE and not DATABASE_AGENT_API_KEY:
        return "dev-mode"
    if DATABASE_AGENT_API_KEY and api_key_header == DATABASE_AGENT_API_KEY:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate credentials")

# Database Connection
db = TradingDB()

# Alpaca API Client
alpaca_client = AlpacaClient(
    api_key=os.environ.get("ALPACA_API_KEY"),
    secret_key=os.environ.get("ALPACA_SECRET_KEY")
)

def _mock_price_history(symbol: str, limit: int = 100) -> List[Price]:
    """Generate deterministic price data for dev/integration mode."""
    now = datetime.now(timezone.utc)
    count = max(1, min(int(limit or 100), 500))
    prices: List[Price] = []
    base = Decimal("100")
    for idx in range(count):
        close = base + Decimal(idx) * Decimal("0.10")
        prices.append(Price(
            symbol=symbol.upper(),
            timestamp=now - timedelta(hours=count - idx),
            open=close - Decimal("0.05"),
            high=close + Decimal("0.15"),
            low=close - Decimal("0.15"),
            close=close,
            volume=1000 + idx,
        ))
    return prices

def _default_portfolio_metrics() -> PortfolioMetrics:
    return PortfolioMetrics(
        win_rate=0.0,
        average_return=0.0,
        max_drawdown=0.0,
        sharpe_ratio=0.0,
    )

# --- Scheduled Jobs ---
def ingest_data_for_symbol_timeframe(symbol: str, timeframe: str, start_date: str, end_date: str):
    try:
        price_data = alpaca_client.fetch_historical_prices(symbol, timeframe, start_date, end_date)
        if price_data:
            db.ingest_historical_prices(price_data)
        else:
            logging.warning(f"No price data to ingest for {symbol} ({timeframe}).")
    except Exception as e:
        logging.error(f"Failed to ingest data for {symbol} ({timeframe}): {e}", exc_info=True)

def run_ingestion_job():
    logging.info("Scheduler starting historical data ingestion job...")
    symbols_to_fetch = ["GOOG", "AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META"]
    timeframes_to_fetch = ["1h", "1d"]
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2 * 365)).strftime('%Y-%m-%d')
    tasks = [(symbol, timeframe) for symbol in symbols_to_fetch for timeframe in timeframes_to_fetch]
    max_workers = min(len(tasks), 10)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol, timeframe in tasks:
            executor.submit(ingest_data_for_symbol_timeframe, symbol, timeframe, start_date, end_date)
    logging.info("Scheduler finished historical data ingestion job.")

def log_database_stats():
    logging.info("Collecting database statistics...")
    try:
        stats = db.get_database_stats()
        if stats:
            logging.info("Database Statistics", extra={"db_stats": stats})
    except Exception as e:
        logging.warning(f"Could not collect database stats: {e}")

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
async def startup_event():
    logging.info("Database Agent API starting up.")
    try:
        db.setup_database()
        logging.info("Database tables verification/creation complete.")
        schedule.every().day.at("00:00").do(run_ingestion_job)
        schedule.every().day.at("01:00").do(db.ensure_price_partitions)
        schedule.every(1).hours.do(log_database_stats)
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logging.info("Scheduler started in a background thread.")
    except Exception as e:
        logging.critical(f"FATAL: Application startup failed: {e}", exc_info=True)
        if not DATABASE_DEV_MODE:
            raise
        logging.warning("DATABASE_DEV_MODE is enabled, continuing startup with fallback responses.")

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Database Agent API shutting down.")

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(wrap_response(
            status="error",
            error={
                "code": str(exc.status_code),
                "message": exc.detail,
                "retryable": False,
            }
        ))
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=jsonable_encoder(wrap_response(
            status="error",
            error={
                "code": "INTERNAL_SERVER_ERROR",
                "message": str(exc),
                "retryable": False,
            }
        ))
    )

@app.get("/health", response_model=StandardAgentResponse[dict])
async def health_check():
    logging.info("Health check endpoint was called.")
    try:
        is_connected = db.check_connection()
    except Exception as e:
        logging.warning(f"Health check database connection failed: {e}")
        is_connected = False
    health_data = {
        "status": "healthy" if is_connected or DATABASE_DEV_MODE else "unhealthy",
        "database_connection": "connected" if is_connected else "dev_fallback" if DATABASE_DEV_MODE else "disconnected",
        "dev_mode": DATABASE_DEV_MODE,
    }
    return wrap_response(data=health_data)

@app.get("/accounts/{account_id}/balance", response_model=StandardAgentResponse[AccountBalance])
async def get_balance(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get balance for account {account_id}.")
    try:
        balance = db.get_account_balance(account_id)
    except Exception as e:
        logging.warning(f"Balance lookup failed for account {account_id}: {e}")
        balance = None
    if balance is None:
        if DATABASE_DEV_MODE and str(account_id) == str(DEFAULT_DEV_ACCOUNT_ID):
            return wrap_response(data=AccountBalance(account_id=account_id, cash_balance=DEFAULT_DEV_CASH_BALANCE))
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
    return wrap_response(data=AccountBalance(account_id=account_id, cash_balance=balance))

@app.get("/accounts/{account_id}/positions", response_model=StandardAgentResponse[List[Position]])
async def get_positions_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get positions for account {account_id}.")
    try:
        positions = db.get_positions(account_id)
    except Exception as e:
        logging.warning(f"Positions lookup failed for account {account_id}: {e}")
        positions = []
    return wrap_response(data=positions or [])

@app.get("/accounts/{account_id}/portfolio_metrics", response_model=StandardAgentResponse[PortfolioMetrics])
async def get_portfolio_metrics_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Returns portfolio performance metrics expected by Manager_Agent."""
    logging.info(f"Request to get portfolio metrics for account {account_id}.")
    try:
        if hasattr(db, "get_portfolio_metrics"):
            metrics = db.get_portfolio_metrics(account_id)
            if metrics:
                return wrap_response(data=PortfolioMetrics.model_validate(metrics))
    except Exception as e:
        logging.warning(f"Portfolio metrics lookup failed for account {account_id}: {e}")
    return wrap_response(data=_default_portfolio_metrics())

@app.post("/signals", response_model=StandardAgentResponse[SignalHistory])
async def create_signal(signal_body: CreateSignalHistoryBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    record = SignalHistory(
        signal_id=signal_body.signal_id or str(uuid.uuid4()),
        account_id=signal_body.account_id,
        symbol=signal_body.symbol.upper(),
        timestamp=datetime.now(timezone.utc),
        source_agent=signal_body.source_agent,
        candidate_score=signal_body.candidate_score,
        technical_score=signal_body.technical_score,
        fundamental_score=signal_body.fundamental_score,
        final_verdict=signal_body.final_verdict,
        market_regime=signal_body.market_regime,
        metadata=signal_body.metadata,
    )
    SIGNAL_HISTORY_STORE.append(record)
    return wrap_response(data=record)

@app.get("/signals", response_model=StandardAgentResponse[List[SignalHistory]])
async def get_signals(account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    rows = list(reversed(SIGNAL_HISTORY_STORE))
    if account_id is not None:
        rows = [row for row in rows if str(row.account_id) == str(account_id)]
    if symbol:
        rows = [row for row in rows if row.symbol.upper() == symbol.upper()]
    return wrap_response(data=rows[offset: offset + limit])

@app.post("/performance_metrics", response_model=StandardAgentResponse[PerformanceMetric])
async def create_performance_metric(metric_body: CreatePerformanceMetricBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    record = PerformanceMetric(
        metric_id=metric_body.metric_id or str(uuid.uuid4()),
        account_id=metric_body.account_id,
        symbol=metric_body.symbol.upper(),
        timestamp=datetime.now(timezone.utc),
        source_agent=metric_body.source_agent,
        entry_price=metric_body.entry_price,
        exit_price=metric_body.exit_price,
        current_price=metric_body.current_price,
        return_pct=metric_body.return_pct,
        holding_days=metric_body.holding_days,
        outcome=metric_body.outcome,
        metadata=metric_body.metadata,
    )
    PERFORMANCE_METRICS_STORE.append(record)
    return wrap_response(data=record)

@app.get("/performance_metrics", response_model=StandardAgentResponse[List[PerformanceMetric]])
async def get_performance_metrics(account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    rows = list(reversed(PERFORMANCE_METRICS_STORE))
    if account_id is not None:
        rows = [row for row in rows if str(row.account_id) == str(account_id)]
    if symbol:
        rows = [row for row in rows if row.symbol.upper() == symbol.upper()]
    return wrap_response(data=rows[offset: offset + limit])

@app.get("/accounts/{account_id}/orders", response_model=StandardAgentResponse[List[Order]])
async def get_order_history_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get order history for account {account_id}.")
    try:
        orders_data = db.get_order_history(account_id)
        orders = [Order.model_validate(o) for o in orders_data]
    except Exception as e:
        logging.warning(f"Order history lookup failed for account {account_id}: {e}")
        orders = []
    return wrap_response(data=orders)

@app.post("/accounts/{account_id}/orders", response_model=Order, status_code=201)
async def create_new_order(account_id: Union[int, str], order_body: CreateOrderBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to create new order for account {account_id}.")
    trade_id = order_body.trade_id or order_body.client_order_id or str(uuid.uuid4())
    try:
        order_id = db.create_order(
            account_id=account_id,
            trade_id=str(trade_id),
            symbol=order_body.symbol,
            side=order_body.side,
            order_type=order_body.order_type,
            quantity=order_body.quantity,
            price=order_body.price,
            time_in_force=order_body.time_in_force,
            correlation_id=correlation_id,
        )
        if order_id is None:
            raise RuntimeError("Database returned no order_id")
        order_data = db.get_order_by_id(order_id)
        return Order.model_validate(order_data)
    except Exception as e:
        logging.warning(f"Create order failed: {e}")
        if DATABASE_DEV_MODE:
            return Order(
                order_id=1,
                trade_id=str(trade_id),
                account_id=account_id,
                symbol=order_body.symbol,
                side=order_body.side,
                order_type=order_body.order_type,
                quantity=order_body.quantity,
                price=order_body.price or Decimal("0"),
                time_in_force=order_body.time_in_force,
                status="pending",
                reason="Created by DATABASE_DEV_MODE fallback",
            )
        raise HTTPException(status_code=500, detail="Failed to create order due to a database error.")

@app.get("/orders/{order_id}", response_model=Order)
async def get_order(order_id: int, api_key: str = Depends(get_api_key)):
    try:
        order_data = db.get_order_by_id(order_id)
    except Exception:
        order_data = None
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.get("/orders/trade/{trade_id}", response_model=Order)
async def get_order_by_trade(trade_id: str, api_key: str = Depends(get_api_key)):
    try:
        order_data = db.get_order_by_trade_id(trade_id)
    except Exception:
        order_data = None
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.patch("/orders/{order_id}", response_model=Order)
async def update_order(order_id: int, updates: dict = Body(...), api_key: str = Depends(get_api_key)):
    try:
        order_data = db.update_order(order_id, updates)
    except Exception:
        order_data = None
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.post("/accounts/{account_id}/orders/{order_id}/execute", response_model=StandardAgentResponse[OrderExecutionResponse])
async def execute_existing_order(account_id: str, order_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to execute order {order_id} for account {account_id}.")
    try:
        status, reason, ret_account_id = db.execute_order(order_id)
    except Exception as e:
        logging.warning(f"Execute order failed: {e}")
        if DATABASE_DEV_MODE:
            status, reason, ret_account_id = "executed", "Executed by DATABASE_DEV_MODE fallback", account_id
        else:
            raise HTTPException(status_code=500, detail="An unexpected internal server error occurred.")
    return wrap_response(data=OrderExecutionResponse(
        order_id=int(order_id),
        trade_id=None,
        account_id=ret_account_id,
        status=status,
        reason=reason,
    ))

@app.get("/accounts/{account_id}/trade_history", response_model=StandardAgentResponse[List[ExecutionTrade]])
async def get_trade_history_for_account(account_id: Union[int, str], limit: int = 50, offset: int = 0, start_date: Optional[str] = None, end_date: Optional[str] = None, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get trade history for account {account_id}.")
    try:
        trades = db.get_executions(account_id, limit, offset, start_date, end_date)
    except Exception as e:
        logging.warning(f"Trade history lookup failed for account {account_id}: {e}")
        trades = []
    return wrap_response(data=trades or [])

@app.get("/accounts/{account_id}/prices/{symbol}", response_model=StandardAgentResponse[List[Price]])
async def get_price_history_for_symbol(account_id: str, symbol: str, timeframe: str = '1h', limit: int = 100, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get price history for symbol {symbol} (Context: Account {account_id}).")
    try:
        prices = db.get_price_history(symbol, timeframe, limit)
    except Exception as e:
        logging.warning(f"Price history lookup failed for {symbol}: {e}")
        prices = []
    if not prices:
        if DATABASE_DEV_MODE:
            return wrap_response(data=_mock_price_history(symbol, limit))
        raise HTTPException(status_code=404, detail=f"No price data found for symbol {symbol}")
    return wrap_response(data=prices)
