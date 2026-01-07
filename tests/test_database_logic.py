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
os.environ.setdefault("POSTGRES_HOST", "localhost") # Default for local docker-compose
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
    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price, "test-correlation-id")
    assert order_id is not None

    # 2. Execute the order
    status, reason = db_session.execute_order(order_id)
    assert status == 'executed'
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
    assert str(order['client_order_id']) == client_order_id
    assert order['status'] == 'executed'
    assert order['failure_reason'] is None

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

    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price, "test-correlation-id")
    status, reason = db_session.execute_order(order_id)
    assert status == 'failed'
    assert reason == 'insufficient_funds'

    # Verify that the state has not changed, and the order is marked as failed
    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance

    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 0

    order = db_session.get_order_history(ACCOUNT_ID)[0]
    assert order['status'] == 'failed'
    assert order['failure_reason'] == 'insufficient_funds'

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
    order_id_1 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"), "test-correlation-id-1")
    assert order_id_1 is not None

    # Second creation attempt with the same client_order_id
    order_id_2 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"), "test-correlation-id-2")
    assert order_id_2 == order_id_1

    # Verify that only one order was actually created in the database
    order_history = db_session.get_order_history(ACCOUNT_ID)
    assert len(order_history) == 1
    assert order_history[0]['order_id'] == order_id_1

def test_get_trade_history(db_session: TradingDB):
    """Tests the retrieval of executed trades, ignoring non-executed ones."""
    ACCOUNT_ID = 1

    # Create and execute a buy order
    buy_order_id = db_session.create_order(ACCOUNT_ID, str(uuid4()), "AAPL", "BUY", 10, Decimal("150.00"), "corr-1")
    db_session.execute_order(buy_order_id)

    # Create a pending order (should not appear in trade history)
    db_session.create_order(ACCOUNT_ID, str(uuid4()), "GOOG", "BUY", 5, Decimal("2800.00"), "corr-2")

    # Create and execute a sell order
    sell_order_id = db_session.create_order(ACCOUNT_ID, str(uuid4()), "TSLA", "SELL", 2, Decimal("700.00"), "corr-3")
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

    # Test fetching the trade history
    trade_history = db_session.get_trade_history(ACCOUNT_ID)
    assert len(trade_history) == 2

    # Verify the contents of the trades (most recent first)
    assert trade_history[0]['side'] == 'sell'
    assert trade_history[0]['symbol'] == 'TSLA'
    assert trade_history[1]['side'] == 'buy'
    assert trade_history[1]['symbol'] == 'AAPL'
    assert trade_history[1]['notional'] == Decimal("1500.00")

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

def test_get_portfolio_metrics(db_session: TradingDB):
    """Tests the calculation of portfolio metrics."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    # 1. Buy 10 shares of AAPL at $150
    aapl_buy_price = Decimal("150.00")
    aapl_quantity = 10
    buy_order_id = db_session.create_order(ACCOUNT_ID, str(uuid4()), "AAPL", "BUY", aapl_quantity, aapl_buy_price, "corr-aapl")
    db_session.execute_order(buy_order_id)

    # The latest price of AAPL in sample data is 152.50
    latest_aapl_price = Decimal("152.50")

    # 2. Get metrics
    metrics = db_session.get_portfolio_metrics(ACCOUNT_ID)

    # 3. Assertions
    assert metrics is not None
    assert metrics['account_id'] == ACCOUNT_ID

    # Cash balance check
    expected_cash = initial_balance - (aapl_quantity * aapl_buy_price)
    assert metrics['cash_balance'] == expected_cash

    # Positions check
    assert len(metrics['positions']) == 1
    aapl_pos = metrics['positions'][0]
    assert aapl_pos['symbol'] == "AAPL"
    assert aapl_pos['quantity'] == aapl_quantity
    assert aapl_pos['avg_cost'] == aapl_buy_price
    assert aapl_pos['market_price'] == latest_aapl_price

    # P&L and Value checks
    expected_market_value = aapl_quantity * latest_aapl_price
    expected_unrealized_pnl = (latest_aapl_price - aapl_buy_price) * aapl_quantity
    assert aapl_pos['market_value'] == expected_market_value
    assert aapl_pos['unrealized_pnl'] == expected_unrealized_pnl

    assert metrics['unrealized_pnl'] == expected_unrealized_pnl
    assert metrics['total_portfolio_value'] == expected_cash + expected_market_value

    # Realized PnL is 0 for now as per implementation
    assert metrics['realized_pnl'] == Decimal("0.00")
