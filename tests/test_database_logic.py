import pytest
import os
import time
from decimal import Decimal
from uuid import uuid4
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from alembic.config import Config
from alembic import command

# Set environment variables for testing.
os.environ.setdefault("POSTGRES_DB", "trading_db_test")
os.environ.setdefault("POSTGRES_USER", "trading_user")
os.environ.setdefault("POSTGRES_PASSWORD", "your_strong_password_here")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")

# This import must come after setting the environment variables
from trading_db import TradingDB
from database_models import Base, Account, Position, Order, Ledger, Price


@pytest.fixture(scope="function")
def db_session():
    """
    Provides a clean database session for each test function.
    It runs migrations, truncates all tables, and creates initial data.
    """
    if os.environ.get("CI"):
        time.sleep(5)
    else:
        # Give DB time to start locally
        time.sleep(20)

    # Ensure a clean slate by deleting the test DB file if it exists
    if os.environ.get('USE_SQLITE') and os.path.exists('trading.db'):
        os.remove('trading.db')

    db = TradingDB()

    # Apply migrations
    alembic_cfg = Config("alembic.ini")
    db_url = str(db.engine.url)
    if "sqlite" in db_url:
        db_url = "sqlite:///trading.db" # Use the file db for tests too

    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")

    # Clean all tables before each test
    with db.get_session() as session:
        session.execute(text("DELETE FROM ledger;"))
        session.execute(text("DELETE FROM orders;"))
        session.execute(text("DELETE FROM positions;"))
        session.execute(text("DELETE FROM prices;"))
        session.execute(text("DELETE FROM accounts;"))
        session.commit()

        # Re-initialize default data
        # Insert sample data for prices
        sample_prices = [
                Price(symbol='AAPL', timestamp=datetime.fromisoformat('2025-01-01T10:00:00Z'), open='150.00', high='152.00', low='149.50', close='151.50', volume=1000000),
                Price(symbol='AAPL', timestamp=datetime.fromisoformat('2025-01-01T11:00:00Z'), open='151.50', high='153.00', low='151.00', close='152.50', volume=1200000),
                Price(symbol='GOOG', timestamp=datetime.fromisoformat('2025-01-01T10:00:00Z'), open='2800.00', high='2810.00', low='2795.00', close='2805.00', volume=500000)
        ]
        session.add_all(sample_prices)

        # Insert main account
        initial_balance = Decimal('1000000.00')
        main_account = Account(account_name='main_account', cash_balance=initial_balance)
        session.add(main_account)
        session.flush()

        # Initial ledger entry
        session.add(Ledger(
            account_id=main_account.account_id,
            asset='CASH',
            change=initial_balance,
            new_balance=initial_balance,
            description='Initial account funding'
        ))
        session.commit()

    yield db

def test_initial_account_setup(db_session: TradingDB):
    """Tests that the default account and initial ledger entry are created correctly."""
    ACCOUNT_ID = 1
    balance = db_session.get_account_balance(ACCOUNT_ID)
    assert balance == Decimal('1000000.00')

    with db_session.get_session() as session:
        ledger_entry = session.query(Ledger).filter(Ledger.account_id == ACCOUNT_ID).first()
        assert ledger_entry is not None
        assert ledger_entry.asset == 'CASH'
        assert ledger_entry.change == Decimal('1000000.00')
        assert ledger_entry.description == 'Initial account funding'

def test_successful_buy_order(db_session: TradingDB):
    """Tests a complete successful buy workflow and verifies data integrity across all tables."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = uuid4()
    symbol, quantity, price = "GOOG", 10, Decimal("175.50")
    total_cost = quantity * price

    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price, "test-correlation-id")
    assert order_id is not None

    status, reason = db_session.execute_order(order_id)
    assert status == 'executed'
    assert reason is None

    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance - total_cost

    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 1
    goog_position = positions[0]
    assert goog_position['symbol'] == symbol
    assert goog_position['quantity'] == quantity
    assert goog_position['average_cost'] == price

    order_history = db_session.get_order_history(ACCOUNT_ID)
    order = order_history[0]
    assert order.order_id == order_id
    assert order.client_order_id == client_order_id
    assert order.status == 'executed'
    assert order.failure_reason is None

    with db_session.get_session() as session:
        entries = session.query(Ledger).filter(Ledger.order_id == order_id).order_by(Ledger.entry_id).all()
        assert len(entries) == 2
        cash_entry = next(e for e in entries if e.asset == 'CASH')
        stock_entry = next(e for e in entries if e.asset == symbol)

        assert cash_entry.change == -total_cost
        assert cash_entry.new_balance == final_balance
        assert stock_entry.change == quantity
        assert stock_entry.new_balance == quantity

def test_insufficient_funds_buy_order(db_session: TradingDB):
    """Tests that a buy order fails correctly when funds are insufficient."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    client_order_id = uuid4()
    symbol, quantity, price = "AMZN", 1, Decimal("2000000.00")

    order_id = db_session.create_order(ACCOUNT_ID, client_order_id, symbol, "BUY", quantity, price, "test-correlation-id")
    status, reason = db_session.execute_order(order_id)
    assert status == 'failed'
    assert reason == 'insufficient_funds'

    final_balance = db_session.get_account_balance(ACCOUNT_ID)
    assert final_balance == initial_balance

    positions = db_session.get_positions(ACCOUNT_ID)
    assert len(positions) == 0

    order = db_session.get_order_history(ACCOUNT_ID)[0]
    assert order.status == 'failed'
    assert order.failure_reason == 'insufficient_funds'

    with db_session.get_session() as session:
        ledger_entries = session.query(Ledger).filter(Ledger.order_id == order_id).all()
        assert len(ledger_entries) == 0

def test_idempotency_of_order_creation(db_session: TradingDB):
    """Ensures that creating an order with the same client_order_id is idempotent."""
    ACCOUNT_ID = 1
    client_order_id = uuid4()

    order_id_1 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"), "test-corr-1")
    assert order_id_1 is not None

    order_id_2 = db_session.create_order(ACCOUNT_ID, client_order_id, "TSLA", "BUY", 5, Decimal("250.00"), "test-corr-2")
    assert order_id_2 == order_id_1

    order_history = db_session.get_order_history(ACCOUNT_ID)
    assert len(order_history) == 1
    assert order_history[0].order_id == order_id_1

def test_get_trade_history(db_session: TradingDB):
    """Tests the retrieval of executed trades, ignoring non-executed ones."""
    ACCOUNT_ID = 1

    buy_order_id = db_session.create_order(ACCOUNT_ID, uuid4(), "AAPL", "BUY", 10, Decimal("150.00"), "corr-1")
    db_session.execute_order(buy_order_id)

    db_session.create_order(ACCOUNT_ID, uuid4(), "GOOG", "BUY", 5, Decimal("2800.00"), "corr-2")

    with db_session.get_session() as session:
        session.add(Position(account_id=ACCOUNT_ID, symbol="TSLA", quantity=10, average_cost=Decimal("650.00")))
        session.commit()

    sell_order_id = db_session.create_order(ACCOUNT_ID, uuid4(), "TSLA", "SELL", 2, Decimal("700.00"), "corr-3")
    db_session.execute_order(sell_order_id)

    trade_history = db_session.get_trade_history(ACCOUNT_ID)
    assert len(trade_history) == 2

    assert trade_history[0]['side'] == 'sell'
    assert trade_history[0]['symbol'] == 'TSLA'
    assert trade_history[1]['side'] == 'buy'
    assert trade_history[1]['symbol'] == 'AAPL'
    assert trade_history[1]['notional'] == Decimal("1500.00")

def test_get_price_history(db_session: TradingDB):
    """Tests retrieval of price history for a given symbol."""
    aapl_prices = db_session.get_price_history("AAPL")
    assert len(aapl_prices) >= 2
    assert aapl_prices[0]['symbol'] == 'AAPL'
    assert aapl_prices[0]['close'] == Decimal('152.50')

    non_existent_prices = db_session.get_price_history("XXXX")
    assert len(non_existent_prices) == 0

def test_get_portfolio_metrics(db_session: TradingDB):
    """Tests the calculation of portfolio metrics."""
    ACCOUNT_ID = 1
    initial_balance = db_session.get_account_balance(ACCOUNT_ID)

    aapl_buy_price = Decimal("150.00")
    aapl_quantity = 10
    buy_order_id = db_session.create_order(ACCOUNT_ID, uuid4(), "AAPL", "BUY", aapl_quantity, aapl_buy_price, "corr-aapl")
    db_session.execute_order(buy_order_id)

    latest_aapl_price = Decimal("152.50")
    metrics = db_session.get_portfolio_metrics(ACCOUNT_ID)

    assert metrics is not None
    assert metrics['account_id'] == ACCOUNT_ID

    expected_cash = initial_balance - (aapl_quantity * aapl_buy_price)
    assert metrics['cash_balance'] == expected_cash

    assert len(metrics['positions']) == 1
    aapl_pos = metrics['positions'][0]
    assert aapl_pos['symbol'] == "AAPL"
    assert aapl_pos['quantity'] == aapl_quantity
    assert aapl_pos['avg_cost'] == aapl_buy_price
    assert aapl_pos['market_price'] == latest_aapl_price

    expected_market_value = aapl_quantity * latest_aapl_price
    expected_unrealized_pnl = (latest_aapl_price - aapl_buy_price) * aapl_quantity
    assert aapl_pos['market_value'] == expected_market_value
    assert aapl_pos['unrealized_pnl'] == expected_unrealized_pnl

    assert metrics['unrealized_pnl'] == expected_unrealized_pnl
    assert metrics['total_portfolio_value'] == expected_cash + expected_market_value
    assert metrics['realized_pnl'] == Decimal("0.00")
