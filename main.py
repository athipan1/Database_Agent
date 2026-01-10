import os
import logging
import uuid
from contextvars import ContextVar
from fastapi import FastAPI, HTTPException, Depends, Security, Request
from fastapi.security import APIKeyHeader
from starlette.responses import Response
from typing import List, Optional
from decimal import Decimal
from typing import List, Optional

from trading_db import TradingDB
from models import (
    AccountBalance, Position, Order, CreateOrderBody, CreateOrderResponse,
    OrderExecutionResponse, Trade, PortfolioMetrics, Price
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
# Configure logging with a placeholder for the correlation ID
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# Add our custom filter to the root logger
logging.getLogger().addFilter(CorrelationIdFilter())


app = FastAPI(title="Database Agent - Secure Trading API")

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


# API Key Security
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    logging.warning("API_KEY environment variable not set. Security is disabled.")
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

def get_api_key(api_key_header: str = Security(api_key_header)):
    """Dependency to validate the API key."""
    if not API_KEY: # Allow access if no API key is configured (for local dev/testing)
        return "development_key"
    if api_key_header == API_KEY:
        return api_key_header
    else:
        raise HTTPException(status_code=403, detail="Could not validate credentials")

# Database Connection
# This single instance will be shared across all requests.
db = TradingDB()

# --- Events ---
@app.on_event("startup")
async def startup_event():
    logging.info("Database Agent API starting up.")
    try:
        db.setup_database()
        logging.info("Database setup verification complete.")
    except Exception as e:
        logging.critical(f"FATAL: Database setup failed on startup: {e}")
        # In a real-world scenario, you might want the app to fail fast
        # if the database is not ready.
        raise

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Database Agent API shutting down.")
    # The db connection is closed automatically by the TradingDB destructor.

# --- API Endpoints ---

@app.get("/health")
async def health_check():
    """Simple health check endpoint."""
    logging.info("Health check endpoint was called.")
    return {"status": "ok"}


@app.get("/accounts/{account_id}/balance", response_model=AccountBalance)
async def get_balance(account_id: int, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retriees the cash balance for a specific account."""
    logging.info(f"Request to get balance for account {account_id}.")
    balance = db.get_account_balance(account_id)
    if balance is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return AccountBalance(cash_balance=balance)

@app.get("/accounts/{account_id}/positions", response_model=List[Position])
async def get_positions_for_account(account_id: int, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retrieves all positions for a specific account."""
    logging.info(f"Request to get positions for account {account_id}.")
    positions = db.get_positions(account_id)
    return positions

@app.get("/accounts/{account_id}/orders", response_model=List[Order])
async def get_order_history_for_account(account_id: int, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retrieves the complete order history for a specific account."""
    logging.info(f"Request to get order history for account {account_id}.")
    orders = db.get_order_history(account_id)
    return orders

@app.post("/accounts/{account_id}/orders", response_model=CreateOrderResponse, status_code=201)
async def create_new_order(account_id: int, order_body: CreateOrderBody, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """
    Creates a new trade order with a 'pending' status.
    This endpoint is idempotent based on the `client_order_id`.
    If an order with the same `client_order_id` already exists,
    the existing order's ID will be returned.
    """
    logging.info(f"Request to create new order for account {account_id}.")

    # If client_order_id is not provided, generate one.
    client_order_id = order_body.client_order_id or uuid.uuid4()

    order_id = db.create_order(
        account_id=account_id,
        client_order_id=str(client_order_id),
        symbol=order_body.symbol,
        order_type=order_body.order_type,
        quantity=order_body.quantity,
        price=order_body.price,
        correlation_id=correlation_id
    )
    if order_id is None:
        raise HTTPException(status_code=500, detail="Failed to create order due to a database error.")

    return CreateOrderResponse(
        order_id=order_id,
        status="pending",
        client_order_id=client_order_id
    )

@app.post("/orders/{order_id}/execute", response_model=OrderExecutionResponse)
async def execute_existing_order(order_id: int, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """
    Executes a pending order. This is the core transactional endpoint.
    It will update balances and positions atomically.
    If the order is already processed or doesn't exist, it will return an error.
    """
    logging.info(f"Request to execute order {order_id}.")
    try:
        # The execute_order method is now fully atomic and returns the outcome.
        status, reason = db.execute_order(order_id)

        return OrderExecutionResponse(
            order_id=order_id,
            status=status,
            reason=reason
        )

    except Exception as e:
        # This catch block is for unexpected system/database errors.
        # Business logic errors are handled by the return value of execute_order.
        logging.error(f"An unexpected error occurred while executing order {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected internal server error occurred.")


@app.get("/accounts/{account_id}/trade_history", response_model=List[Trade])
async def get_trade_history_for_account(
    account_id: int,
    limit: int = 50,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api_key: str = Depends(get_api_key),
    correlation_id: str = Depends(get_correlation_id)
):
    """Retrieves the executed trade history for a specific account with optional filters."""
    logging.info(f"Request to get trade history for account {account_id}.")
    trades = db.get_trade_history(account_id, limit, offset, start_date, end_date)
    return trades

@app.get("/accounts/{account_id}/portfolio_metrics", response_model=PortfolioMetrics)
async def get_portfolio_metrics_for_account(account_id: int, api_key: str = Depends(get_api_key), correlation_id: str = Depends(get_correlation_id)):
    """Retrieves portfolio metrics for a specific account."""
    logging.info(f"Request to get portfolio metrics for account {account_id}.")
    metrics = db.get_portfolio_metrics(account_id)
    if metrics is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return metrics

@app.get("/prices/{symbol}", response_model=List[Price])
async def get_price_history_for_symbol(
    symbol: str,
    timeframe: str = '1h',
    limit: int = 100,
    api_key: str = Depends(get_api_key),
    correlation_id: str = Depends(get_correlation_id)
):
    """Retrieves price history for a specific symbol."""
    logging.info(f"Request to get price history for symbol {symbol}.")
    prices = db.get_price_history(symbol, timeframe, limit)
    if not prices:
        raise HTTPException(status_code=404, detail=f"No price data found for symbol {symbol}")
    return prices
