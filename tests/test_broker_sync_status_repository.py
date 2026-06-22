import sqlite3
from contextlib import contextmanager

from broker_sync_repository import sync_broker_state
from broker_sync_status_repository import broker_sync_status


class SQLiteStatusTestDB:
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


def broker_state():
    return {
        "source": "execution_agent",
        "account_id": 1,
        "broker": "ALPACA",
        "paper": True,
        "captured_at": "2026-06-22T15:37:04Z",
        "account": {"cash": "-100223.4", "buying_power": "3827.07", "equity": "102016.81"},
        "positions": [
            {"symbol": "AAPL", "qty": "1", "avg_entry_price": "254.48", "current_price": "300.3", "market_value": "300.3"},
            {"symbol": "ACGL", "qty": "2190", "avg_entry_price": "91.31", "current_price": "92.21", "market_value": "201939.9"},
        ],
        "open_orders": [],
        "summary": {"position_count": 2, "open_order_count": 0, "cash_negative": True},
    }


def test_broker_sync_status_reports_synced_after_sync():
    db = SQLiteStatusTestDB()
    sync_broker_state(db, broker_state())

    status = broker_sync_status(db, account_id=1)

    assert status["has_snapshot"] is True
    assert status["database"]["position_count"] == 2
    assert status["database"]["open_order_count"] == 0
    assert status["mismatch"]["is_synced"] is True
    assert status["mismatch"]["mismatch_count"] == 0
    assert status["latest_snapshot"]["summary"]["position_count"] == 2


def test_broker_sync_status_reports_stale_database_without_sync():
    db = SQLiteStatusTestDB()

    status = broker_sync_status(db, account_id=1)

    assert status["has_snapshot"] is False
    assert status["database"]["position_count"] == 0
    assert status["mismatch"]["is_synced"] is False
