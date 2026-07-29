import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from order_creation_persistence import (
    _legacy_client_order_id,
    install_strategy_bucket_order_creation,
)


class SQLiteOrderDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                trade_id TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price TEXT,
                time_in_force TEXT,
                strategy_bucket TEXT DEFAULT 'unassigned',
                status TEXT NOT NULL,
                correlation_id TEXT,
                client_order_id TEXT UNIQUE
            )
            """
        )
        self.conn.commit()

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()

    def create_order(self, **kwargs):  # pragma: no cover - replaced by installer
        raise AssertionError("legacy create_order should have been replaced")


def _legacy_body_mapper(account_id, body, correlation_id):
    return {
        "account_id": account_id,
        "trade_id": body.trade_id,
        "symbol": body.symbol,
        "side": body.side,
        "order_type": body.order_type,
        "quantity": body.quantity,
        "price": body.price,
        "time_in_force": body.time_in_force,
        "correlation_id": correlation_id,
    }


def _runtime():
    runtime = SimpleNamespace(
        db=SQLiteOrderDB(),
        _order_body_to_create_args=_legacy_body_mapper,
    )
    install_strategy_bucket_order_creation(runtime)
    return runtime


def _body(symbol, trade_id, bucket):
    return SimpleNamespace(
        trade_id=trade_id,
        symbol=symbol,
        side="buy",
        order_type="market",
        quantity=3,
        price=None,
        time_in_force="GTC",
        strategy_bucket=bucket,
    )


@pytest.mark.parametrize(
    ("symbol", "bucket"),
    [
        ("NEWCORE", "core_dividend"),
        ("NEWVALUE", "value_rebound"),
        ("NEWMOMO", "news_momentum"),
    ],
)
def test_strategy_bucket_is_written_in_initial_insert_for_any_symbol(symbol, bucket):
    runtime = _runtime()
    body = _body(symbol, f"trade-{symbol.lower()}", bucket)

    args = runtime._order_body_to_create_args(1, body, "corr-direct-insert")
    order_id = runtime.db.create_order(**args)

    row = runtime.db.conn.execute(
        "SELECT * FROM orders WHERE order_id = ?",
        (order_id,),
    ).fetchone()

    assert args["strategy_bucket"] == bucket
    assert row["symbol"] == symbol
    assert row["strategy_bucket"] == bucket
    assert row["status"] == "pending"
    assert row["correlation_id"] == "corr-direct-insert"


def test_duplicate_trade_id_is_idempotent_for_same_bucket():
    runtime = _runtime()
    body = _body("XYZ", "trade-idempotent", "value_rebound")
    args = runtime._order_body_to_create_args(1, body, "corr-1")

    first_order_id = runtime.db.create_order(**args)
    second_order_id = runtime.db.create_order(**args)

    assert second_order_id == first_order_id
    assert runtime.db.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1


def test_duplicate_can_upgrade_unassigned_to_specific_bucket():
    runtime = _runtime()

    unassigned = runtime._order_body_to_create_args(
        1,
        _body("XYZ", "trade-upgrade", "unassigned"),
        "corr-unassigned",
    )
    order_id = runtime.db.create_order(**unassigned)

    specific = runtime._order_body_to_create_args(
        1,
        _body("XYZ", "trade-upgrade", "news_momentum"),
        "corr-specific",
    )
    duplicate_order_id = runtime.db.create_order(**specific)

    row = runtime.db.conn.execute(
        "SELECT strategy_bucket FROM orders WHERE order_id = ?",
        (order_id,),
    ).fetchone()

    assert duplicate_order_id == order_id
    assert row["strategy_bucket"] == "news_momentum"


def test_duplicate_conflicting_specific_bucket_fails_closed():
    runtime = _runtime()

    first = runtime._order_body_to_create_args(
        1,
        _body("XYZ", "trade-conflict", "core_dividend"),
        "corr-core",
    )
    runtime.db.create_order(**first)

    conflicting = runtime._order_body_to_create_args(
        1,
        _body("XYZ", "trade-conflict", "value_rebound"),
        "corr-value",
    )

    with pytest.raises(ValueError, match="strategy_bucket_conflict"):
        runtime.db.create_order(**conflicting)


def test_unknown_bucket_is_rejected_before_database_write():
    runtime = _runtime()
    body = _body("XYZ", "trade-unknown", "mystery_bucket")

    with pytest.raises(ValueError, match="unsupported strategy_bucket"):
        runtime._order_body_to_create_args(1, body, "corr-unknown")

    assert runtime.db.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0


def test_installer_is_idempotent():
    runtime = _runtime()
    first_create_order = runtime.db.create_order
    first_mapper = runtime._order_body_to_create_args

    install_strategy_bucket_order_creation(runtime)

    assert runtime.db.create_order == first_create_order
    assert runtime._order_body_to_create_args == first_mapper


def test_postgres_legacy_client_order_id_ignores_opaque_trade_id():
    trade_id = "profit:account-102:position-2:HARD:v1:hard-stop"

    assert _legacy_client_order_id("postgres", trade_id) is None
    assert _legacy_client_order_id("sqlite", trade_id) == trade_id


def test_postgres_legacy_client_order_id_preserves_uuid():
    trade_id = "9FD5E882-C2F8-41F7-AE7A-337318A50CEB"

    assert (
        _legacy_client_order_id("postgres", trade_id)
        == "9fd5e882-c2f8-41f7-ae7a-337318a50ceb"
    )
