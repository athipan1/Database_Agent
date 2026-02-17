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
from starlette.responses import Response
from typing import Optional, Any, TypeVar, Generic, List, Union
from decimal import Decimal
from pythonjsonlogger import jsonlogger
from prometheus_fastapi_instrumentator import Instrumentator

from trading_db import TradingDB
from alpaca_client import AlpacaClient
from models import (
    AccountBalance, Position, Order, CreateOrderBody, CreateOrderResponse,
    OrderExecutionResponse, ExecutionTrade, Price, StandardAgentResponse,
    OrderSide, OrderType, TimeInForce, OrderStatus
)

# --- Context setup for Correlation ID ---
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)

# --- Custom Logging Filter ---
class CorrelationIdFilter(logging.Filter):
    """Injects the correlation_id into log records."""
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

# --- Configuration & Setup ---
# Load environment variables from .env file
load_dotenv()

# Configure logging
log_handler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s',
    timestamp=True
)
log_handler.setFormatter(formatter)
logging.getLogger().addHandler(log_handler)
logging.getLogger().setLevel(logging.INFO)

# Add our custom filter to the root logger
logging.getLogger().addFilter(CorrelationIdFilter())


app = FastAPI(title="Database Agent - Secure Trading API")

# Initialize Prometheus Instrumentator
Instrumentator().instrument(app).expose(app)

# --- Middleware for Correlation ID ---
@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    # Get correlation ID from header or generate a new one
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())

    # Set the correlation ID in the context variable
    token = correlation_id_var.set(correlation_id)

    response = await call_next(request)

    # Also add it to the response header
    response.headers["X-Correlation-ID"] = correlation_id

    # Reset the context variable
    correlation_id_var.reset(token)

    return response

# --- Dependency to get Correlation ID ---
async def get_correlation_id() -> str:
    """Dependency to get the correlation ID from the context variable."""
    return correlation_id_var.get()


def wrap_response(data: Any = None, status: str = "success", error: Optional[dict] = None):
    """Wraps the data into a standard response format."""
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.0",
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": error,
        "confidence_score": None
    }

# API Key Security
DATABASE_AGENT_API_KEY = os.environ.get("DATABASE_AGENT_API_KEY")
if not DATABASE_AGENT_API_KEY:
    logging.critical("CRITICAL: DATABASE_AGENT_API_KEY environment variable not set. Application will terminate.")
    sys.exit(1) # Exit gracefully
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

def get_api_key(api_key_header: str = Security(api_key_header)):
    """Dependency to validate the API key."""
    if DATABASE_AGENT_API_KEY and api_key_header == DATABASE_AGENT_API_KEY:
        return api_key_header
    raise HTTPException(status_code=403, detail="Could not validate credentials")

# Database Connection
# This single instance will be shared across all requests.
db = TradingDB()

# Alpaca API Client
alpaca_client = AlpacaClient(
    api_key=os.environ.get("ALPACA_API_KEY"),
    secret_key=os.environ.get("ALPACA_SECRET_KEY")
)

# --- Scheduled Jobs ---
def ingest_data_for_symbol_timeframe(symbol: str, timeframe: str, start_date: str, end_date: str):
    """Worker function for parallel data ingestion."""
    try:
        price_data = alpaca_client.fetch_historical_prices(
            symbol, timeframe, start_date, end_date
        )
        if price_data:
            db.ingest_historical_prices(price_data)
        else:
            logging.warning(f"No price data to ingest for {symbol} ({timeframe}).")
    except Exception as e:
        logging.error(f"Failed to ingest data for {symbol} ({timeframe}): {e}", exc_info=True)

def run_ingestion_job():
    """
    Defines the scheduled job to fetch and ingest historical data using parallel threads.
    """
    logging.info("Scheduler starting historical data ingestion job...")
    # Expanded symbol list for better coverage
    symbols_to_fetch = ["GOOG", "AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META"]
    timeframes_to_fetch = ["1h", "1d"] # Changed 4h to 1h as Alpaca 4h is tricky

    # Calculate date range for the last 2 years
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=2*365)).strftime('%Y-%m-%d')

    tasks = []
    for symbol in symbols_to_fetch:
        for timeframe in timeframes_to_fetch:
            tasks.append((symbol, timeframe))

    # Run ingestion in parallel using a thread pool
    max_workers = min(len(tasks), 10)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol, timeframe in tasks:
            executor.submit(ingest_data_for_symbol_timeframe, symbol, timeframe, start_date, end_date)

    logging.info("Scheduler finished historical data ingestion job.")

def log_database_stats():
    """Periodically logs database statistics."""
    logging.info("Collecting database statistics...")
    stats = db.get_database_stats()
    if stats:
        logging.info("Database Statistics", extra={"db_stats": stats})

def run_scheduler():
    """
    Continuously runs pending scheduled jobs.
    """
    while True:
        schedule.run_pending()
        time.sleep(1)

# --- Events ---
@app.on_event("startup")
async def startup_event():
    """Ensure the database and tables are created and start the scheduler."""
    logging.info("Database Agent API starting up.")
    try:
        db.setup_database()
        logging.info("Database tables verification/creation complete.")

        # Schedule ingestion job
        schedule.every().day.at("00:00").do(run_ingestion_job)
        logging.info("Scheduled data ingestion job to run daily at 00:00.")

        # Schedule partition maintenance job
        schedule.every().day.at("01:00").do(db.ensure_price_partitions)
        logging.info("Scheduled partition maintenance job to run daily at 01:00.")

        # Schedule stats logging job
        schedule.every(1).hours.do(log_database_stats)
        logging.info("Scheduled database stats logging job to run every hour.")

        # Run the scheduler in a background thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logging.info("Scheduler started in a background thread.")

    except Exception as e:
        logging.critical(f"FATAL: Application startup failed: {e}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Database Agent API shutting down.")

# --- Exception Handlers ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Check if request is from Execution Agent (e.g. starts with /orders or /accounts/.../orders)
    # For now, we'll try to keep error responses wrapped to follow the standard,
    # but Execution Agent's HttpDatabaseClient might fail to parse them if it expects raw error details.
    # However, the test shows it handles 404/422 by status code, so wrapped might be okay if it doesn't try to parse body.
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(wrap_response(
            status="error",
            error={
                "code": str(exc.status_code),
                "message": exc.detail,
                "retryable": False
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
                "retryable": False
            }
        ))
    )

# --- API Endpoints ---

@app.get("/health", response_model=StandardAgentResponse[dict])
async def health_check():
    """Simple health check endpoint."""
    logging.info("Health check endpoint was called.")
    is_connected = db.check_connection()
    health_data = {
        "status": "healthy" if is_connected else "unhealthy",
        "database_connection": "connected" if is_connected else "disconnected"
    }
    return wrap_response(data=health_data)


@app.get("/accounts/{account_id}/balance", response_model=StandardAgentResponse[AccountBalance])
async def get_balance(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retriees the cash balance for a specific account."""
    logging.info(f"Request to get balance for account {account_id}.")
    balance = db.get_account_balance(account_id)
    if balance is None:
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
    return wrap_response(data=AccountBalance(account_id=account_id, cash_balance=balance))

@app.get("/accounts/{account_id}/positions", response_model=StandardAgentResponse[List[Position]])
async def get_positions_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retrieves all positions for a specific account."""
    logging.info(f"Request to get positions for account {account_id}.")
    positions = db.get_positions(account_id)
    return wrap_response(data=positions)

@app.get("/accounts/{account_id}/orders", response_model=StandardAgentResponse[List[Order]])
async def get_order_history_for_account(account_id: Union[int, str], api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retrieves the complete order history for a specific account."""
    logging.info(f"Request to get order history for account {account_id}.")
    orders_data = db.get_order_history(account_id)
    return wrap_response(data=[Order.model_validate(o) for o in orders_data])

@app.post("/accounts/{account_id}/orders", response_model=Order, status_code=201)
async def create_new_order(account_id: Union[int, str], order_body: CreateOrderBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """
    Creates a new trade order.
    Returns raw Order object for Execution Agent compatibility.
    """
    logging.info(f"Request to create new order for account {account_id}.")

    # Resolve trade_id
    trade_id = order_body.trade_id or order_body.client_order_id or str(uuid.uuid4())

    order_id = db.create_order(
        account_id=account_id,
        trade_id=str(trade_id),
        symbol=order_body.symbol,
        side=order_body.side,
        order_type=order_body.order_type,
        quantity=order_body.quantity,
        price=order_body.price,
        time_in_force=order_body.time_in_force,
        correlation_id=correlation_id
    )
    if order_id is None:
        raise HTTPException(status_code=500, detail="Failed to create order due to a database error.")

    order_data = db.get_order_by_id(order_id)
    return Order.model_validate(order_data)

@app.get("/orders/{order_id}", response_model=Order)
async def get_order(order_id: int, api_key: str = Depends(get_api_key)):
    """Retrieves an order by its ID. Raw response for Execution Agent."""
    order_data = db.get_order_by_id(order_id)
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.get("/orders/trade/{trade_id}", response_model=Order)
async def get_order_by_trade(trade_id: str, api_key: str = Depends(get_api_key)):
    """Retrieves an order by its trade ID. Raw response for Execution Agent."""
    order_data = db.get_order_by_trade_id(trade_id)
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.patch("/orders/{order_id}", response_model=Order)
async def update_order(order_id: int, updates: dict = Body(...), api_key: str = Depends(get_api_key)):
    """Updates an order. Raw response for Execution Agent."""
    order_data = db.update_order(order_id, updates)
    if not order_data:
        raise HTTPException(status_code=404, detail="Order not found")
    return Order.model_validate(order_data)

@app.post("/accounts/{account_id}/orders/{order_id}/execute", response_model=StandardAgentResponse[OrderExecutionResponse])
async def execute_existing_order(
    account_id: str,
    order_id: Union[int, str], 
    api_key: str = Depends(get_api_key), 
    correlation_id: str = Depends(get_correlation_id)
):
    """Executes a pending order."""
    logging.info(f"Request to execute order {order_id} for account {account_id}.")
    try:
        status, reason, ret_account_id = db.execute_order(order_id)

        # Map status to OrderStatus enum values if needed
        # In DB it's already 'executed' or 'failed'

        return wrap_response(data=OrderExecutionResponse(
            order_id=int(order_id),
            trade_id=None, # Will be populated if needed
            account_id=ret_account_id,
            status=status,
            reason=reason
        ))

    except Exception as e:
        logging.error(f"An unexpected error occurred while executing order {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected internal server error occurred.")


@app.get("/accounts/{account_id}/trade_history", response_model=StandardAgentResponse[List[ExecutionTrade]])
async def get_trade_history_for_account(
    account_id: Union[int, str],
    limit: int = 50,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    correlation_id: str = Depends(get_correlation_id)
):
    """Retrieves the executed trade history for a specific account."""
    logging.info(f"Request to get trade history for account {account_id}.")
    trades = db.get_executions(account_id, limit, offset, start_date, end_date)
    return wrap_response(data=trades)

@app.get("/accounts/{account_id}/prices/{symbol}", response_model=StandardAgentResponse[List[Price]])
async def get_price_history_for_symbol(
    account_id: str,
    symbol: str,
    timeframe: str = '1h',
    limit: int = 100,
    api_key: str = Depends(get_api_key),
    correlation_id: str = Depends(get_correlation_id)
):
    """Retrieves price history for a specific symbol."""
    logging.info(f"Request to get price history for symbol {symbol} (Context: Account {account_id}).")
    prices = db.get_price_history(symbol, timeframe, limit)
    if not prices:
        raise HTTPException(status_code=404, detail=f"No price data found for symbol {symbol}")
    return wrap_response(data=prices)
