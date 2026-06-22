import sqlite3
from contextlib import contextmanager
from decimal import Decimal

from fill_repository import create_fill_record
from stock_accounting_repository import build_stock_portfolio_summary, create_equity_snapshot, get_open_lots


class SQLiteTestDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()

    def _to_decimal(self, value):
        if value is None:
            return None
        return Decimal(str(value))

    def get_account_balance(self, account_id):
        return Decimal("10000")


def test_buy_fill_opens_position_lot():
    db = SQLiteTestDB()

    fill = create_fill_record(
        db,
        account_id=1,
        order_id=10,
        trade_id="t-buy",
        symbol="AAPL",
        side="buy",
        quantity=10,
        fill_price=100,
        fees=1,
        broker_fill_id="buy-1",
    )

    lots = get_open_lots(db, 1, symbol="AAPL")
    assert fill["stock_accounting"]["accounting_action"] == "opened_lot"
    assert len(lots) == 1
    assert lots[0]["quantity_open"] == 10
    assert lots[0]["cost_basis"] == Decimal("1001")


def test_sell_fill_consumes_fifo_lot_and_reports_realized_pnl():
    db = SQLiteTestDB()
    create_fill_record(db, account_id=1, order_id=10, trade_id="t-buy", symbol="AAPL", side="buy", quantity=10, fill_price=100, fees=1, broker_fill_id="buy-1")

    sell = create_fill_record(
        db,
        account_id=1,
        order_id=11,
        trade_id="t-sell",
        symbol="AAPL",
        side="sell",
        quantity=4,
        fill_price=110,
        fees=Decimal("0.4"),
        broker_fill_id="sell-1",
    )

    lots = get_open_lots(db, 1, symbol="AAPL")
    assert sell["stock_accounting"]["accounting_action"] == "closed_lots_fifo"
    assert sell["stock_accounting"]["closed_quantity"] == 4
    assert sell["stock_accounting"]["remaining_unmatched_quantity"] == 0
    assert sell["stock_accounting"]["realized_pnl"] == Decimal("39.2")
    assert lots[0]["quantity_open"] == 6
    assert lots[0]["cost_basis"] == Decimal("600.6")


def test_portfolio_summary_uses_open_lots_cost_basis():
    db = SQLiteTestDB()
    create_fill_record(db, account_id=1, order_id=10, trade_id="t-buy", symbol="AAPL", side="buy", quantity=10, fill_price=100, fees=1, broker_fill_id="buy-1")

    summary = build_stock_portfolio_summary(db, 1)

    assert summary["cash_balance"] == Decimal("10000")
    assert summary["positions_value_cost_basis"] == Decimal("1001")
    assert summary["equity_cost_basis"] == Decimal("11001")
    assert summary["open_positions"][0]["symbol"] == "AAPL"
    assert summary["open_positions"][0]["average_cost"] == Decimal("100.1")


def test_create_equity_snapshot_from_cost_basis_summary():
    db = SQLiteTestDB()
    create_fill_record(db, account_id=1, order_id=10, trade_id="t-buy", symbol="AAPL", side="buy", quantity=10, fill_price=100, fees=1, broker_fill_id="buy-1")

    snapshot = create_equity_snapshot(db, 1, source="test")

    assert snapshot["cash_balance"] == Decimal("10000")
    assert snapshot["positions_value"] == Decimal("1001")
    assert snapshot["equity"] == Decimal("11001")
    assert snapshot["source"] == "test"
