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
from history_repository import (
    create_performance_record,
    create_signal_record,
    get_performance_records,
    get_signal_records,
    setup_history_tables,
)
from models import (
    AccountBalance, Position, Order, CreateOrderBody,
    OrderExecutionResponse, ExecutionTrade, Price, StandardAgentResponse,
    PortfolioMetrics, SignalHistory, CreateSignalHistoryBody,
    PerformanceMetric, CreatePerformanceMetricBody,
)

correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


TRADING_MODE = os.getenv("TRADING_MODE", "PAPER").strip().upper()
DATABASE_DEV_MODE = _env_bool("DATABASE_DEV_MODE", False)
DEFAULT_DEV_ACCOUNT_ID = os.getenv("DEFAULT_DEV_ACCOUNT_ID", "1")
DEFAULT_DEV_CASH_BALANCE = Decimal(os.getenv("DEFAULT_DEV_CASH_BALANCE", "100000"))

if TRADING_MODE not in {"PAPER", "LIVE"}:
    logging.critical("CRITICAL: TRADING_MODE must be PAPER or LIVE. Application will terminate.")
    sys.exit(1)

if TRADING_MODE == "LIVE" and DATABASE_DEV_MODE:
    logging.critical("CRITICAL: DATABASE_DEV_MODE=true is forbidden when TRADING_MODE=LIVE. Application will terminate.")
    sys.exit(1)

log_handler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s',
    timestamp=True
)
log_handler.setFormatter(formatter)
logging.getLogger().addHandler(log_handler)
logging.getLogger().setLevel(logging.INFO)

class CorrelationIdFilter(logging.Filter):
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
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": error,
        "confidence_score": None,
    }

DATABASE_AGENT_API_KEY = os.environ.get("DATABASE_AGENT_API_KEY")
if not DATABASE_AGENT_API_KEY and not DATABASE_DEV_MODE:
    logging.critical("CRITICAL: DATABASE_AGENT_API_KEY environment variable not set. Application will terminate.")
    sys.exit(1)
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

def get_api_key(api_key_header: str = Security(api_key_header)):
    if DATABASE_DEV_MODE and not DATABASE_AGENT_API_KEY:
        return "dev-mode"
    if DATABASE_AGENT_API_KEY and api_key_header == DATABASE_AGENT_API_KEY:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate credentials")

db = TradingDB()

alpaca_client = AlpacaClient(
    api_key=os.environ.get("ALPACA_API_KEY"),
    secret_key=os.environ.get("ALPACA_SECRET_KEY")
)

def _mock_price_history(symbol: str, limit: int = 100) -> List[Price]:
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
        setup_history_tables(db)
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
        "trading_mode": TRADING_MODE,
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

@app.get("/accounts/{account_id}/orders", response_model=StandardAgentResponse[List[Order]])
async def get_orders_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get orders for account {account_id}.")
    try:
        orders = db.get_orders(account_id)
    except Exception as e:
        logging.warning(f"Orders lookup failed for account {account_id}: {e}")
        orders = []
    return wrap_response(data=orders or [])

@app.post("/accounts/{account_id}/orders", response_model=StandardAgentResponse[Order])
async def create_order_for_account(account_id: Union[int, str], body: CreateOrderBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to create order for account {account_id}, symbol {body.symbol}.")
    try:
        order = db.create_order(account_id, body)
    except Exception as e:
        logging.error(f"Order creation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=order)

@app.post("/accounts/{account_id}/orders/{order_id}/execute", response_model=StandardAgentResponse[OrderExecutionResponse])
async def execute_order_for_account(account_id: Union[int, str], order_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to execute order {order_id} for account {account_id}.")
    try:
        result = db.execute_order(account_id, order_id)
    except Exception as e:
        logging.error(f"Order execution failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=result)

@app.get("/accounts/{account_id}/trades", response_model=StandardAgentResponse[List[ExecutionTrade]])
async def get_trades_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get trades for account {account_id}.")
    try:
        trades = db.get_trade_history(account_id)
    except Exception as e:
        logging.warning(f"Trade history lookup failed for account {account_id}: {e}")
        trades = []
    return wrap_response(data=trades or [])

@app.get("/accounts/{account_id}/portfolio/metrics", response_model=StandardAgentResponse[PortfolioMetrics])
async def get_portfolio_metrics(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get portfolio metrics for account {account_id}.")
    try:
        metrics = db.get_portfolio_metrics(account_id)
    except Exception as e:
        logging.warning(f"Portfolio metrics lookup failed for account {account_id}: {e}")
        metrics = None
    if metrics is None:
        if DATABASE_DEV_MODE:
            metrics = _default_portfolio_metrics()
        else:
            raise HTTPException(status_code=404, detail=f"Portfolio metrics not found for account {account_id}")
    return wrap_response(data=metrics)

@app.get("/prices/{symbol}/history", response_model=StandardAgentResponse[List[Price]])
async def get_price_history(symbol: str, limit: int = 100, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    logging.info(f"Request to get price history for symbol {symbol} with limit {limit}.")
    try:
        prices = db.get_price_history(symbol.upper(), limit)
    except Exception as e:
        logging.warning(f"Price history lookup failed for {symbol}: {e}")
        prices = []
    if not prices and DATABASE_DEV_MODE:
        prices = _mock_price_history(symbol, limit)
    return wrap_response(data=prices or [])

@app.post("/signals", response_model=StandardAgentResponse[SignalHistory])
async def create_signal(body: CreateSignalHistoryBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    try:
        record = create_signal_record(db, body)
    except Exception as e:
        logging.error(f"Signal history creation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=record)

@app.get("/signals", response_model=StandardAgentResponse[List[SignalHistory]])
async def get_signals(account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    try:
        records = get_signal_records(db, account_id=account_id, symbol=symbol, limit=limit, offset=offset)
    except Exception as e:
        logging.error(f"Signal history lookup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=records)

@app.post("/performance-metrics", response_model=StandardAgentResponse[PerformanceMetric])
async def create_performance_metric(body: CreatePerformanceMetricBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    try:
        record = create_performance_record(db, body)
    except Exception as e:
        logging.error(f"Performance metric creation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=record)

@app.get("/performance-metrics", response_model=StandardAgentResponse[List[PerformanceMetric]])
async def get_performance_metrics(account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    try:
        records = get_performance_records(db, account_id=account_id, symbol=symbol, limit=limit, offset=offset)
    except Exception as e:
        logging.error(f"Performance metric lookup failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return wrap_response(data=records)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Database Agent API"}
