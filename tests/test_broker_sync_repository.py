import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from datetime import datetime, timezone

from broker_sync_repository import setup_broker_sync_tables, sync_broker_state


class SQLiteBrokerSyncTestDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.setup_database()

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()

    def _add_column_if_not_exists(self, cursor, table, column, definition):
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError:
            pass

    def _to_decimal(self, value):
        if value is None:
            return None
        return Decimal(str(value))

    def setup_database(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_name TEXT NOT NULL UNIQUE,
                cash_balance TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE positions (
                position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity BIGINT NOT NULL,
                average_cost TEXT NOT NULL,
                UNIQUE (account_id, symbol)
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT NOT NULL UNIQUE,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity BIGINT NOT NULL,
                price TEXT,
                time_in_force TEXT DEFAULT 'GTC',
                status TEXT NOT NULL,
                broker_order_id TEXT,
                reason TEXT,
                executed_quantity BIGINT DEFAULT 0,
                avg_execution_price TEXT,
                executed_at TEXT,
                correlation_id TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                client_order_id TEXT UNIQUE,
                failure_reason TEXT
            )
        """)
        cur.execute("INSERT INTO accounts (account_id, account_name, cash_balance) VALUES (1, 'main_account', '1000000.00')")
        self.conn.commit()

    def get_account_balance(self, account_id):
        cur = self.conn.cursor()
        cur.execute("SELECT cash_balance FROM accounts WHERE account_id = ?", (account_id,))
        row = cur.fetchone()
        return Decimal(str(row["cash_balance"])) if row else None

    def get_positions(self, account_id):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM positions WHERE account_id = ? ORDER BY symbol", (account_id,))
        return [dict(row) for row in cur.fetchall()]

    def get_orders(self, account_id):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM orders WHERE account_id = ? ORDER BY symbol", (account_id,))
        return [dict(row) for row in cur.fetchall()]


def broker_state(open_orders=None):
    return {
        "source": "execution_agent",
        "account_id": 1,
        "broker": "ALPACA",
        "paper": True,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "account": {
            "broker": "ALPACA",
            "cash": "-100223.4",
            "buying_power": "0",
            "equity": "99909.6",
            "portfolio_value": "99909.6",
        },
        "positions": [
            {"symbol": "AAPL", "qty": "1", "avg_entry_price": "254.48", "current_price": "295.5", "market_value": "295.5"},
            {"symbol": "ACGL", "qty": "2190", "avg_entry_price": "91.31", "current_price": "91.25", "market_value": "199837.5"},
        ],
        "open_orders": open_orders if open_orders is not None else [
            {"id": "order-aapl", "symbol": "AAPL", "side": "sell", "qty": "1", "filled_qty": "0", "type": "market", "time_in_force": "day", "status": "new", "submitted_at": "2026-06-22T08:01:16Z"},
            {"id": "order-acgl", "symbol": "ACGL", "side": "sell", "qty": "2190", "filled_qty": "0", "type": "market", "time_in_force": "day", "status": "new", "submitted_at": "2026-06-22T08:01:16Z"},
        ],
        "summary": {"position_count": 2, "open_order_count": 2, "buying_power_unavailable": True, "cash_negative": True},
    }


def test_sync_broker_state_updates_cash_positions_and_open_orders():
    db = SQLiteBrokerSyncTestDB()

    result = sync_broker_state(db, broker_state())

    assert result["cash_balance"] == Decimal("-100223.4")
    assert result["positions_synced"] == 2
    assert result["open_orders_synced"] == 2
    assert db.get_account_balance(1) == Decimal("-100223.4")

    positions = db.get_positions(1)
    assert [p["symbol"] for p in positions] == ["AAPL", "ACGL"]
    assert positions[0]["quantity"] == 1
    assert Decimal(str(positions[1]["market_value"])) == Decimal("199837.5")

    orders = db.get_orders(1)
    assert len(orders) == 2
    assert {order["broker_order_id"] for order in orders} == {"order-aapl", "order-acgl"}
    assert {order["status"] for order in orders} == {"placed"}


def test_sync_broker_state_marks_missing_broker_orders_cancelled():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())

    result = sync_broker_state(db, broker_state(open_orders=[]))

    assert result["open_orders_synced"] == 0
    assert result["missing_open_orders_marked_cancelled"] == 2
    orders = db.get_orders(1)
    assert {order["status"] for order in orders} == {"cancelled"}
    assert {order["reason"] for order in orders} == {"missing_from_broker_sync"}


def test_sync_broker_state_records_snapshot():
    db = SQLiteBrokerSyncTestDB()

    sync_broker_state(db, broker_state())

    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM broker_sync_snapshots")
    assert cur.fetchone()["count"] == 1
