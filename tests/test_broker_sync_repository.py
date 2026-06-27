import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from datetime import datetime, timezone

from broker_sync_repository import sync_broker_state
from broker_sync_status_repository import broker_sync_status


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
            "cash": "93276.78",
            "buying_power": "402033.47",
            "equity": "103607.62",
            "portfolio_value": "103607.62",
        },
        "positions": [
            {"symbol": "ADBE", "qty": "52", "avg_entry_price": "198.76", "current_price": "198.67", "market_value": "10330.84"},
        ],
        "open_orders": open_orders if open_orders is not None else [
            {"id": "stop-adbe", "symbol": "ADBE", "side": "sell", "qty": "52", "filled_qty": "0", "type": "stop", "time_in_force": "gtc", "status": "new", "submitted_at": "2026-06-24T16:01:39Z", "stop_price": "190.12"},
        ],
        "summary": {"position_count": 1, "open_order_count": 1, "buying_power_unavailable": False, "cash_negative": False},
    }


def test_sync_broker_state_updates_cash_positions_and_open_orders():
    db = SQLiteBrokerSyncTestDB()

    result = sync_broker_state(db, broker_state())

    assert result["cash_balance"] == Decimal("93276.78")
    assert result["positions_synced"] == 1
    assert result["open_orders_synced"] == 1
    assert db.get_account_balance(1) == Decimal("93276.78")

    positions_by_symbol = {p["symbol"]: p for p in db.get_positions(1)}
    assert set(positions_by_symbol) == {"ADBE"}
    assert positions_by_symbol["ADBE"]["quantity"] == 52
    assert Decimal(str(positions_by_symbol["ADBE"]["market_value"])) == Decimal("10330.84")

    orders = db.get_orders(1)
    assert len(orders) == 1
    assert {order["broker_order_id"] for order in orders} == {"stop-adbe"}
    assert {order["status"] for order in orders} == {"placed"}


def test_sync_broker_state_preserves_existing_position_bucket_when_broker_has_none():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())

    cur = db.conn.cursor()
    cur.execute("UPDATE positions SET strategy_bucket = ? WHERE symbol = ?", ("core_dividend", "ADBE"))
    db.conn.commit()

    sync_broker_state(db, broker_state())

    positions_by_symbol = {p["symbol"]: p for p in db.get_positions(1)}
    assert positions_by_symbol["ADBE"]["strategy_bucket"] == "core_dividend"


def test_sync_broker_state_preserves_existing_order_bucket_when_broker_has_none():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())

    cur = db.conn.cursor()
    cur.execute("UPDATE orders SET strategy_bucket = ? WHERE broker_order_id = ?", ("core_dividend", "stop-adbe"))
    db.conn.commit()

    sync_broker_state(db, broker_state())

    orders_by_broker_id = {order["broker_order_id"]: order for order in db.get_orders(1)}
    assert orders_by_broker_id["stop-adbe"]["strategy_bucket"] == "core_dividend"


def test_sync_broker_state_prefers_explicit_broker_bucket_over_existing_bucket():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())

    cur = db.conn.cursor()
    cur.execute("UPDATE positions SET strategy_bucket = ? WHERE symbol = ?", ("core_dividend", "ADBE"))
    cur.execute("UPDATE orders SET strategy_bucket = ? WHERE broker_order_id = ?", ("core_dividend", "stop-adbe"))
    db.conn.commit()

    updated = broker_state(open_orders=[
        {
            "id": "stop-adbe",
            "symbol": "ADBE",
            "side": "sell",
            "qty": "52",
            "filled_qty": "0",
            "type": "stop",
            "time_in_force": "gtc",
            "status": "new",
            "submitted_at": "2026-06-24T16:01:39Z",
            "stop_price": "190.12",
            "strategy_bucket": "value_rebound",
        },
    ])
    updated["positions"][0]["strategy_bucket"] = "value_rebound"

    sync_broker_state(db, updated)

    assert db.get_positions(1)[0]["strategy_bucket"] == "value_rebound"
    assert db.get_orders(1)[0]["strategy_bucket"] == "value_rebound"


def test_sync_broker_state_marks_missing_broker_orders_cancelled():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())

    result = sync_broker_state(db, broker_state(open_orders=[]))

    assert result["open_orders_synced"] == 0
    assert result["missing_open_orders_marked_cancelled"] == 1
    orders = db.get_orders(1)
    assert {order["status"] for order in orders} == {"cancelled"}
    assert {order["reason"] for order in orders} == {"missing_from_broker_sync"}


def test_sync_broker_state_records_snapshot():
    db = SQLiteBrokerSyncTestDB()

    sync_broker_state(db, broker_state())

    cur = db.conn.cursor()
    cur.execute("SELECT COUNT(*) AS count FROM broker_sync_snapshots")
    assert cur.fetchone()["count"] == 1


def test_broker_sync_status_uses_canonical_cash_qty_and_broker_order_ids():
    db = SQLiteBrokerSyncTestDB()
    sync_broker_state(db, broker_state())
    cur = db.conn.cursor()
    cur.execute("UPDATE accounts SET cash_balance = ? WHERE account_id = ?", ("93276.78000", 1))
    db.conn.commit()

    status = broker_sync_status(db, account_id=1)

    assert status["has_snapshot"] is True
    assert status["mismatch"]["is_synced"] is True
    assert status["mismatch"]["mismatch_count"] == 0
    assert status["database"]["position_count"] == 1
    assert status["database"]["open_order_count"] == 1
