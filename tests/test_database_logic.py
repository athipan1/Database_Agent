import pytest
import os
from decimal import Decimal
from uuid import uuid4
import psycopg2.errors

# Set environment variables for testing.
# In a real CI/CD pipeline, these would be configured in the environment.
# For local testing, ensure the PostgreSQL container is running.
os.environ.setdefault("POSTGRES_DB", "trading_db")
os.environ.setdefault("POSTGRES_USER", "trading_user")
os.environ.setdefault("POSTGRES_PASSWORD", "your_strong_password_here")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")

# This import must come after setting the environment variables
from trading_db import TradingDB

@pytest.fixture(scope="function")
def db_session():
    """
    Provides a clean database session for each test function.
    It truncates all relevant tables to ensure test isolation.
    """
    try:
        db = TradingDB()
    except Exception as e:
        pytest.fail(f"Failed to connect to the test database: {e}. "
                    "Please ensure the PostgreSQL container is running and accessible.")

    # Ensure the database schema is created before cleaning
    db.setup_database()

    # Clean all tables before each test for a clean slate
    with db.get_cursor() as cursor:
        cursor.execute("TRUNCATE TABLE ledger, orders, positions, accounts RESTART IDENTITY CASCADE;")
        db.conn.commit()

    # Re-initialize the default account since TRUNCATE cleared it
    db.setup_database()

    yield db

    # The connection is closed by the TradingDB destructor.

def test_initial_account_setup(db_session: TradingDB):
    """Tests that the default account and initial ledger entry are created correctly."""
    ACCOUNT_ID = 1
    balance = db_session.get_account_balance(ACCOUNT_ID)
    assert balance == Decimal('1000000.00')

    with db_session.get_cursor() as cursor:
        cursor.execute("SELECT * FROM ledger WHERE account_id = %s", (ACCOUNT_ID,))
        ledger_entry = cursor.fetchone()

    assert ledger_entry is not None
    assert ledger_entry['asset'] == 'CASH'
    assert ledger_entry['change'] == Decimal('1000000.00')
    assert ledger_entry['description'] == 'Initial account funding'

def test_successful_buy_order(db_session: TradingDB):
    """Tests a complete successful buy workflow and verifies data integrity across all tables."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = str(uuid4())
    symbol, quantity, price = "GOOG", 10, Decimal("175.50")
    total_cost = quantity * price

    # 1. Create a pending order
    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price)
    assert order_id is not None

    # 2. Execute the order
    db_session.execute_order(order_id)

    # 3. Verify the final state of the database
    # Account balance check
    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance - total_cost

    # Positions check
    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 1
    goog_position = positions[0]
    assert goog_position['symbol'] == symbol
    assert goog_position['quantity'] == quantity
    assert goog_position['average_cost'] == price

    # Order status check
    order = db_session.get_order_history(ACCOUNT_ID)[0]
    assert order['order_id'] == order_id
    assert str(order['client_order_id']) == client_order_id
    assert order['status'] == 'executed'
    assert order['failure_reason'] is None

    # Ledger entries check for full auditability
    with db_session.get_cursor() as cursor:
        cursor.execute("SELECT * FROM ledger WHERE order_id = %s ORDER BY entry_id", (order_id,))
        entries = cursor.fetchall()

    assert len(entries) == 2
    cash_entry = next(e for e in entries if e['asset'] == 'CASH')
    stock_entry = next(e for e in entries if e['asset'] == symbol)

    assert cash_entry['change'] == -total_cost
    assert cash_entry['new_balance'] == final_balance

    assert stock_entry['change'] == quantity
    assert stock_entry['new_balance'] == quantity

def test_insufficient_funds_buy_order(db_session: TradingDB):
    """Tests that a buy order fails correctly when funds are insufficient."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = str(uuid4())
    symbol, quantity, price = "AMZN", 1, Decimal("2000000.00") # Price exceeds initial balance

    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price)
    db_session.execute_order(order_id)

    # Verify that the state has not changed, and the order is marked as failed
    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance

    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 0

    order = db_session.get_order_history(ACCOUNT_ID)[0]
    assert order['status'] == 'failed'
    assert order['failure_reason'] == 'Insufficient funds'

    # Verify no ledger entries were created for this failed order
    with db_session.get_cursor() as cursor:
        cursor.execute("SELECT * FROM ledger WHERE order_id = %s", (order_id,))
        assert cursor.fetchone() is None

def test_idempotency_of_order_creation(db_session: TradingDB):
    """Ensures that creating an order with the same client_order_id is idempotent."""
    ACCOUNT_ID = 1
    client_order_id = str(uuid4())

    # First creation attempt
    order_id_1 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"))
    assert order_id_1 is not None

    # Second creation attempt with the same client_order_id
    order_id_2 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"))
    assert order_id_2 == order_id_1

    # Verify that only one order was actually created in the database
    order_history = db_session.get_order_history(ACCOUNT_ID)
    assert len(order_history) == 1
    assert order_history[0]['order_id'] == order_id_1
