import pytest
import os
import time
from decimal import Decimal
from uuid import uuid4
import psycopg2.errors

# Set environment variables for testing.
# These will be overridden by the CI environment if set there.
os.environ.setdefault("POSTGRES_DB", "trading_db_test") # Use a separate test DB
os.environ.setdefault("POSTGRES_USER", "trading_user")
os.environ.setdefault("POSTGRES_PASSWORD", "your_strong_password_here")
# When running tests via `docker compose exec`, the API container can resolve the DB container by its service name.
os.environ.setdefault("POSTGRES_HOST", "db")
os.environ.setdefault("POSTGRES_PORT", "5432")

# This import must come after setting the environment variables
from trading_db import TradingDB

@pytest.fixture(scope="function")
def db_session():
    """
    Provides a clean database session for each test function.
    It truncates all relevant tables to ensure test isolation.
    """
    # Allow the DB container time to initialize
    if os.environ.get("CI"): # Running in CI
        time.sleep(5)

    try:
        db = TradingDB()
    except Exception as e:
        pytest.fail(f"Failed to connect to the test database: {e}. "
                    "Please ensure the PostgreSQL container is running and accessible.")

    # Ensure the database schema is created before cleaning
    db.setup_database()

    # Clean all tables before each test for a clean slate
    cursor = db.get_cursor()
    try:
        if db.db_type == 'postgres':
            cursor.execute("TRUNCATE TABLE ledger, orders, positions, accounts RESTART IDENTITY CASCADE;")
        else: # SQLite
            cursor.execute("DELETE FROM ledger;")
            cursor.execute("DELETE FROM orders;")
            cursor.execute("DELETE FROM positions;")
            cursor.execute("DELETE FROM accounts;")
            # Reset autoincrement sequence for accounts in SQLite
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='accounts';")
        db.conn.commit()
    finally:
        cursor.close()

    # Re-initialize the default account since TRUNCATE/DELETE cleared it
    db.setup_database()

    yield db

    # The connection is closed by the TradingDB destructor.

def test_initial_account_setup(db_session: TradingDB):
    """Tests that the default account and initial ledger entry are created correctly."""
    ACCOUNT_ID = 1
    balance = db_session.get_account_balance(ACCOUNT_ID)
    assert balance == Decimal('1000000.00')

    cursor = db_session.get_cursor()
    try:
        cursor.execute(f"SELECT * FROM ledger WHERE account_id = {db_session.param_style}", (ACCOUNT_ID,))
        ledger_entry = cursor.fetchone()
    finally:
        cursor.close()

    assert ledger_entry is not None
    assert ledger_entry['asset'] == 'CASH'
    assert db_session._to_decimal(ledger_entry['change']) == Decimal('1000000.00')
    assert ledger_entry['description'] == 'Initial account funding'

def test_successful_buy_order(db_session: TradingDB):
    """Tests a complete successful buy workflow and verifies data integrity across all tables."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = str(uuid4())
    symbol, quantity, price = "GOOG", 10, Decimal("175.50")
    total_cost = quantity * price

    # 1. Create a pending order
    order_id = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=client_order_id,
        symbol=symbol,
        side="BUY",
        order_type="market",
        quantity=quantity,
        price=price,
        correlation_id="test-correlation-id"
    )
    assert order_id is not None

    # 2. Execute the order
    status, reason, aid = db_session.execute_order(order_id)
    assert status == 'executed'
    assert aid == ACCOUNT_ID
    assert reason is None

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
    assert str(order['trade_id']) == client_order_id
    assert order['status'] == 'executed'
    assert order['reason'] is None

    # Ledger entries check for full auditability
    cursor = db_session.get_cursor()
    try:
        cursor.execute(f"SELECT * FROM ledger WHERE order_id = {db_session.param_style} ORDER BY entry_id", (order_id,))
        entries = cursor.fetchall()
    finally:
        cursor.close()

    assert len(entries) == 2
    cash_entry = next(e for e in entries if e['asset'] == 'CASH')
    stock_entry = next(e for e in entries if e['asset'] == symbol)

    assert db_session._to_decimal(cash_entry['change']) == -total_cost
    assert db_session._to_decimal(cash_entry['new_balance']) == final_balance

    assert int(db_session._to_decimal(stock_entry['change'])) == quantity
    assert int(db_session._to_decimal(stock_entry['new_balance'])) == quantity

def test_insufficient_funds_buy_order(db_session: TradingDB):
    """Tests that a buy order fails correctly when funds are insufficient."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = str(uuid4())
    symbol, quantity, price = "AMZN", 1, Decimal("2000000.00") # Price exceeds initial balance

    order_id = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=client_order_id,
        symbol=symbol,
        side="BUY",
        order_type="market",
        quantity=quantity,
        price=price,
        correlation_id="test-correlation-id"
    )
    status, reason, aid = db_session.execute_order(order_id)
    assert status == 'failed'
    assert reason == 'insufficient_funds'

    # Verify that the state has not changed, and the order is marked as failed
    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance

    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 0

    order = db_session.get_order_history(ACCOUNT_ID)[0]
    assert order['status'] == 'failed'
    assert order['reason'] == 'insufficient_funds'

    # Verify no ledger entries were created for this failed order
    cursor = db_session.get_cursor()
    try:
        cursor.execute(f"SELECT * FROM ledger WHERE order_id = {db_session.param_style}", (order_id,))
        assert cursor.fetchone() is None
    finally:
        cursor.close()

def test_idempotency_of_order_creation(db_session: TradingDB):
    """Ensures that creating an order with the same client_order_id is idempotent."""
    ACCOUNT_ID = 1
    client_order_id = str(uuid4())

    # First creation attempt
    order_id_1 = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=client_order_id,
        symbol="TSLA",
        side="BUY",
        order_type="market",
        quantity=5,
        price=Decimal("250.00"),
        correlation_id="test-correlation-id-1"
    )
    assert order_id_1 is not None

    # Second creation attempt with the same client_order_id
    order_id_2 = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=client_order_id,
        symbol="TSLA",
        side="BUY",
        order_type="market",
        quantity=5,
        price=Decimal("250.00"),
        correlation_id="test-correlation-id-2"
    )
    assert order_id_2 == order_id_1

    # Verify that only one order was actually created in the database
    order_history = db_session.get_order_history(ACCOUNT_ID)
    assert len(order_history) == 1
    assert order_history[0]['order_id'] == order_id_1

def test_get_executions(db_session: TradingDB):
    """Tests the retrieval of executed trades, ignoring non-executed ones."""
    ACCOUNT_ID = 1

    # Create and execute a buy order
    buy_order_id = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=str(uuid4()),
        symbol="AAPL",
        side="BUY",
        order_type="market",
        quantity=10,
        price=Decimal("150.00"),
        correlation_id="corr-1"
    )
    db_session.execute_order(buy_order_id)

    # Create a pending order (should not appear in trade history)
    db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=str(uuid4()),
        symbol="GOOG",
        side="BUY",
        order_type="market",
        quantity=5,
        price=Decimal("2800.00"),
        correlation_id="corr-2"
    )

    # Create and execute a sell order
    sell_order_id = db_session.create_order(
        account_id=ACCOUNT_ID,
        trade_id=str(uuid4()),
        symbol="TSLA",
        side="SELL",
        order_type="market",
        quantity=2,
        price=Decimal("700.00"),
        correlation_id="corr-3"
    )
    # To execute a sell, we first need a position. Let's create one directly for simplicity.
    cursor = db_session.get_cursor()
    try:
        cursor.execute(
            f"INSERT INTO positions (account_id, symbol, quantity, average_cost) VALUES ({db_session.param_style}, {db_session.param_style}, {db_session.param_style}, {db_session.param_style})",
            (ACCOUNT_ID, "TSLA", 10, "650.00")
        )
        db_session.conn.commit()
    finally:
        cursor.close()
    db_session.execute_order(sell_order_id)

    # Test fetching the execution history
    execution_history = db_session.get_executions(ACCOUNT_ID)
    assert len(execution_history) == 2

    # Verify the contents of the executions (most recent first)
    assert execution_history[0]['side'] == 'sell'
    assert execution_history[0]['symbol'] == 'TSLA'
    assert execution_history[1]['side'] == 'buy'
    assert execution_history[1]['symbol'] == 'AAPL'
    assert execution_history[1]['notional'] == Decimal("1500.00")

def test_get_price_history(db_session: TradingDB):
    """Tests retrieval of price history for a given symbol."""
    # The setup_database in the fixture already adds sample prices for AAPL and GOOG.

    # Test for a symbol that exists
    aapl_prices = db_session.get_price_history("AAPL")
    assert len(aapl_prices) >= 2
    assert aapl_prices[0]['symbol'] == 'AAPL'
    assert aapl_prices[0]['close'] == Decimal('152.50')

    # Test for a symbol that does not exist
    non_existent_prices = db_session.get_price_history("XXXX")
    assert len(non_existent_prices) == 0
