import os
import pytest
from trading_db import TradingDB

TEST_DB = "test_trading.db"
ACCOUNT_ID = 1


@pytest.fixture
def db():
    """Create a fresh database for each test."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    db = TradingDB(db_file=TEST_DB)
    db.setup_database()
    yield db

    # cleanup
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_default_account_created(db):
    balance = db.get_account_balance(ACCOUNT_ID)
    assert balance == 1_000_000.0


def test_successful_buy_order(db):
    order_id = db.create_order(ACCOUNT_ID, "AAPL", "BUY", 10, 150.0)
    db.execute_order(order_id)

    balance = db.get_account_balance(ACCOUNT_ID)
    assert balance == 1_000_000.0 - (10 * 150.0)

    positions = db.get_positions(ACCOUNT_ID)
    assert len(positions) == 1
    assert positions[0]["symbol"] == "AAPL"
    assert positions[0]["quantity"] == 10


def test_buy_updates_average_cost(db):
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "BUY", 10, 100.0))
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "BUY", 10, 200.0))

    positions = db.get_positions(ACCOUNT_ID)
    assert positions[0]["quantity"] == 20
    assert positions[0]["average_cost"] == pytest.approx(150.0)


def test_successful_sell_order(db):
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "BUY", 10, 100.0))
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "SELL", 5, 150.0))

    positions = db.get_positions(ACCOUNT_ID)
    assert positions[0]["quantity"] == 5

    balance = db.get_account_balance(ACCOUNT_ID)
    assert balance == 1_000_000.0 - (10 * 100.0) + (5 * 150.0)


def test_sell_all_removes_position(db):
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "BUY", 5, 100.0))
    db.execute_order(db.create_order(ACCOUNT_ID, "AAPL", "SELL", 5, 120.0))

    positions = db.get_positions(ACCOUNT_ID)
    assert positions == []


def test_buy_fails_on_insufficient_funds(db):
    order_id = db.create_order(ACCOUNT_ID, "GOOG", "BUY", 1, 10_000_000.0)
    db.execute_order(order_id)

    orders = db.get_order_history(ACCOUNT_ID)
    assert orders[0]["status"] == "failed"


def test_sell_fails_on_insufficient_shares(db):
    order_id = db.create_order(ACCOUNT_ID, "AAPL", "SELL", 10, 200.0)
    db.execute_order(order_id)

    orders = db.get_order_history(ACCOUNT_ID)
    assert orders[0]["status"] == "failed"