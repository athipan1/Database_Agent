import sqlite3
from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

import broker_sync_repository


class SQLiteRouteTestDB:
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
        cur.execute("CREATE TABLE accounts (account_id INTEGER PRIMARY KEY, account_name TEXT NOT NULL UNIQUE, cash_balance TEXT NOT NULL)")
        cur.execute("CREATE TABLE positions (position_id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL, symbol TEXT NOT NULL, quantity BIGINT NOT NULL, average_cost TEXT NOT NULL, UNIQUE (account_id, symbol))")
        cur.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY AUTOINCREMENT, trade_id TEXT NOT NULL UNIQUE, account_id INTEGER NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL, quantity BIGINT NOT NULL, price TEXT, time_in_force TEXT DEFAULT 'GTC', status TEXT NOT NULL, broker_order_id TEXT, reason TEXT, executed_quantity BIGINT DEFAULT 0, avg_execution_price TEXT, executed_at TEXT, correlation_id TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, client_order_id TEXT UNIQUE, failure_reason TEXT)")
        cur.execute("INSERT INTO accounts (account_id, account_name, cash_balance) VALUES (1, 'main_account', '1000000.00')")
        self.conn.commit()


def _register_routes(monkeypatch):
    app = FastAPI()
    fake_main = type("FakeMain", (), {"app": app})()
    monkeypatch.setitem(__import__("sys").modules, "main", fake_main)
    db = SQLiteRouteTestDB()
    broker_sync_repository.setup_broker_sync_tables(db)
    return app, db


def test_setup_broker_sync_tables_registers_status_and_snapshot_routes(monkeypatch):
    app, _ = _register_routes(monkeypatch)

    paths = {route.path for route in app.routes}
    assert "/broker-sync/status" in paths
    assert "/broker-sync/snapshot" in paths
    assert app.state.broker_sync_status_route_registered is True
    assert app.state.broker_sync_snapshot_route_registered is True


def test_broker_sync_snapshot_route_captures_snapshot(monkeypatch):
    app, db = _register_routes(monkeypatch)
    client = TestClient(app)

    response = client.post("/broker-sync/snapshot", json={
        "account_id": 1,
        "broker": "ALPACA",
        "paper": True,
        "account": {"cash": "93276.77", "equity": "103313.29"},
        "positions": [{"symbol": "ADBE", "qty": "52", "avg_entry_price": "198.76", "current_price": "193.01", "market_value": "10036.52"}],
        "open_orders": [{"id": "stop-adbe", "symbol": "ADBE", "side": "sell", "qty": "52", "type": "stop", "status": "new", "stop_price": "190.12"}],
        "summary": {"position_count": 1, "open_order_count": 1},
    })

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["positions_synced"] == 1
    assert body["data"]["open_orders_synced"] == 1

    status = client.get("/broker-sync/status?account_id=1").json()["data"]
    assert status["has_snapshot"] is True
    assert status["mismatch"]["is_synced"] is True
    assert status["database"]["position_count"] == 1
    assert status["database"]["open_order_count"] == 1
