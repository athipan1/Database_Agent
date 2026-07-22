import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

from broker_sync_repository import setup_broker_sync_tables, sync_broker_state
from broker_sync_status_repository import broker_sync_status
from models import Position


class SQLitePositionPeakTestDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._create_legacy_schema()

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

    def _create_legacy_schema(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_name TEXT NOT NULL UNIQUE,
                cash_balance TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE positions (
                position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity BIGINT NOT NULL,
                average_cost TEXT NOT NULL,
                UNIQUE (account_id, symbol)
            )
            """
        )
        cursor.execute(
            """
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
            """
        )
        cursor.execute(
            "INSERT INTO accounts (account_id, account_name, cash_balance) "
            "VALUES (1, 'main_account', '100000.00')"
        )
        self.conn.commit()

    def positions(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM positions ORDER BY symbol")
        return [dict(row) for row in cursor.fetchall()]


def broker_state(*, entry_price="100", current_price="100", positions_open=True):
    positions = []
    if positions_open:
        positions = [
            {
                "symbol": "AAPL",
                "qty": "10",
                "avg_entry_price": entry_price,
                "current_price": current_price,
                "market_value": str(Decimal(current_price) * Decimal("10")),
            }
        ]
    return {
        "source": "execution_agent",
        "account_id": 1,
        "broker": "ALPACA",
        "paper": True,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "account": {"cash": "90000.00", "equity": "100000.00"},
        "positions": positions,
        "open_orders": [],
        "summary": {
            "position_count": len(positions),
            "open_order_count": 0,
        },
    }


def _peak(db):
    positions = db.positions()
    assert len(positions) == 1
    return Decimal(str(positions[0]["highest_price_since_entry"]))


def test_migration_backfills_existing_position_without_breaking_legacy_rows():
    db = SQLitePositionPeakTestDB()
    db.conn.execute(
        "INSERT INTO positions (account_id, symbol, quantity, average_cost) "
        "VALUES (1, 'MSFT', 5, '250.00')"
    )
    db.conn.commit()

    setup_broker_sync_tables(db)

    row = db.positions()[0]
    assert Decimal(str(row["highest_price_since_entry"])) == Decimal("250.00")


def test_broker_sync_keeps_highest_price_when_latest_price_falls():
    db = SQLitePositionPeakTestDB()

    sync_broker_state(db, broker_state(entry_price="100", current_price="100"))
    assert _peak(db) == Decimal("100")

    sync_broker_state(db, broker_state(entry_price="100", current_price="125"))
    assert _peak(db) == Decimal("125")

    sync_broker_state(db, broker_state(entry_price="100", current_price="110"))
    assert _peak(db) == Decimal("125")

    status = broker_sync_status(db, account_id=1)
    database_position = status["database"]["positions"][0]
    snapshot_position = status["latest_snapshot"]["positions"][0]
    assert Decimal(str(database_position["highest_price_since_entry"])) == Decimal("125")
    assert Decimal(str(snapshot_position["highest_price_since_entry"])) == Decimal("125")


def test_broker_sync_preserves_position_identity_and_profit_lifecycle():
    db = SQLitePositionPeakTestDB()

    sync_broker_state(db, broker_state(entry_price="100", current_price="110"))
    first = db.positions()[0]
    db.conn.execute(
        "UPDATE positions SET position_version = 4, "
        "first_target_executed = TRUE, total_exited_quantity = '3' "
        "WHERE position_id = ?",
        (first["position_id"],),
    )
    db.conn.commit()

    sync_broker_state(db, broker_state(entry_price="100", current_price="115"))
    second = db.positions()[0]

    assert second["position_id"] == first["position_id"]
    assert second["position_version"] == 4
    assert bool(second["first_target_executed"]) is True
    assert Decimal(str(second["total_exited_quantity"])) == Decimal("3")


def test_closed_position_reopens_with_a_fresh_entry_peak():
    db = SQLitePositionPeakTestDB()

    sync_broker_state(db, broker_state(entry_price="100", current_price="125"))
    assert _peak(db) == Decimal("125")

    sync_broker_state(db, broker_state(positions_open=False))
    assert db.positions() == []

    sync_broker_state(db, broker_state(entry_price="90", current_price="88"))
    assert _peak(db) == Decimal("90")


def test_database_trigger_applies_max_semantics_to_direct_price_updates():
    db = SQLitePositionPeakTestDB()
    setup_broker_sync_tables(db)

    db.conn.execute(
        "INSERT INTO positions (account_id, symbol, quantity, average_cost, current_market_price) "
        "VALUES (1, 'NVDA', 2, '120', '120')"
    )
    db.conn.commit()
    assert _peak(db) == Decimal("120")

    db.conn.execute(
        "UPDATE positions SET current_market_price = '140' WHERE symbol = 'NVDA'"
    )
    db.conn.commit()
    assert _peak(db) == Decimal("140")

    db.conn.execute(
        "UPDATE positions SET current_market_price = '130' WHERE symbol = 'NVDA'"
    )
    db.conn.commit()
    assert _peak(db) == Decimal("140")


def test_position_api_schema_exposes_highest_price_since_entry():
    schema = Position.model_json_schema()
    assert "highest_price_since_entry" in schema["properties"]
    assert "position_id" in schema["properties"]
    assert "position_version" in schema["properties"]
    assert "first_target_executed" in schema["properties"]

    position = Position(
        account_id=1,
        symbol="AAPL",
        quantity=10,
        average_cost=Decimal("100"),
        current_market_price=Decimal("110"),
        highest_price_since_entry=Decimal("125"),
    )
    assert position.highest_price_since_entry == Decimal("125")
