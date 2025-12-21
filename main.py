import os
import logging
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from typing import List
from decimal import Decimal

from trading_db import TradingDB
from models import AccountBalance, Position, Order, CreateOrderBody, CreateOrderResponse

# --- Configuration & Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(title="Database Agent - Secure Trading API")

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

@app.get("/accounts/{account_id}/balance", response_model=AccountBalance, dependencies=[Depends(get_api_key)])
async def get_balance(account_id: int):
    """Retrieves the cash balance for a specific account."""
    balance = db.get_account_balance(account_id)
    if balance is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return AccountBalance(cash_balance=balance)

@app.get("/accounts/{account_id}/positions", response_model=List[Position], dependencies=[Depends(get_api_key)])
async def get_positions_for_account(account_id: int):
    """Retrieves all positions for a specific account."""
    positions = db.get_positions(account_id)
    return positions

@app.get("/accounts/{account_id}/orders", response_model=List[Order], dependencies=[Depends(get_api_key)])
async def get_order_history_for_account(account_id: int):
    """Retrieves the complete order history for a specific account."""
    orders = db.get_order_history(account_id)
    return orders

@app.post("/accounts/{account_id}/orders", response_model=CreateOrderResponse, status_code=201, dependencies=[Depends(get_api_key)])
async def create_new_order(account_id: int, order_body: CreateOrderBody):
    """
    Creates a new trade order with a 'pending' status.
    This endpoint is idempotent based on the `client_order_id`.
    If an order with the same `client_order_id` already exists,
    the existing order's ID will be returned.
    """
    order_id = db.create_order(
        account_id=account_id,
        client_order_id=str(order_body.client_order_id),
        symbol=order_body.symbol,
        order_type=order_body.order_type,
        quantity=order_body.quantity,
        price=order_body.price
    )
    if order_id is None:
        raise HTTPException(status_code=500, detail="Failed to create order due to a database error.")

    return CreateOrderResponse(
        order_id=order_id,
        status="pending",
        client_order_id=order_body.client_order_id
    )

@app.post("/orders/{order_id}/execute", response_model=Order, dependencies=[Depends(get_api_key)])
async def execute_existing_order(order_id: int):
    """
    Executes a pending order. This is the core transactional endpoint.
    It will update balances and positions atomically.
    If the order is already processed or doesn't exist, it will return an error.
    """
    try:
        # The execute_order method is fully atomic.
        db.execute_order(order_id)

        # After execution, fetch the final state of the order to return it.
        # We need a new cursor as the one in execute_order is closed.
        cursor = db.get_cursor()
        cursor.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
        final_order = cursor.fetchone()
        cursor.close()

        if not final_order:
            raise HTTPException(status_code=404, detail=f"Order with ID {order_id} not found after execution attempt.")

        return final_order

    except Exception as e:
        # This is a generic catch-all. Specific logic inside execute_order handles
        # business logic errors (like insufficient funds) by marking the order as 'failed'.
        # This catch block is for unexpected system/database errors.
        logging.error(f"An unexpected error occurred while executing order {order_id}: {e}")
        raise HTTPException(status_code=500, detail="An unexpected internal error occurred.")
