import json
from contextlib import contextmanager

import pytest

from protective_order_repository import (
    normalize_order_protective_metadata,
    persist_protective_order_metadata,
)


class FakeCursor:
    def __init__(self):
        self.query = None
        self.params = None
        self.closed = False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class FakeDB:
    param_style = "?"

    def __init__(self):
        self.conn = FakeConnection()
        self.cursor = FakeCursor()

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        assert conn is self.conn
        return self.cursor


@pytest.mark.parametrize(
    "bucket",
    ["core_dividend", "value_rebound", "news_momentum"],
)
def test_normalize_recovers_specific_bucket_when_db_default_is_unassigned(bucket):
    order = {
        "symbol": "NEW_STOCK",
        "strategy_bucket": "unassigned",
        "guard_plan": json.dumps({"strategy_bucket": bucket}),
        "protective_exit": None,
        "metadata": None,
    }

    normalized = normalize_order_protective_metadata(order)

    assert normalized["strategy_bucket"] == bucket


def test_persist_writes_bucket_from_payload_for_any_symbol():
    db = FakeDB()

    persist_protective_order_metadata(
        db,
        77,
        guard_plan={"symbol": "XYZ", "strategy_bucket": "news_momentum"},
    )

    assert "strategy_bucket = ?" in db.cursor.query
    assert "news_momentum" in db.cursor.params
    assert db.cursor.params[-1] == 77
    assert db.conn.committed is True
    assert db.cursor.closed is True


def test_explicit_bucket_is_supported_without_symbol_hardcoding():
    db = FakeDB()

    persist_protective_order_metadata(
        db,
        88,
        strategy_bucket="value_rebound",
        guard_plan={"symbol": "BRAND_NEW"},
    )

    assert "value_rebound" in db.cursor.params


def test_conflicting_specific_buckets_fail_closed():
    order = {
        "symbol": "XYZ",
        "strategy_bucket": "unassigned",
        "guard_plan": {"strategy_bucket": "core_dividend"},
        "metadata": {"strategy_bucket": "value_rebound"},
    }

    with pytest.raises(ValueError, match="strategy_bucket_conflict"):
        normalize_order_protective_metadata(order)


def test_unknown_bucket_is_rejected():
    order = {
        "symbol": "XYZ",
        "strategy_bucket": "mystery_bucket",
        "guard_plan": None,
        "metadata": {},
    }

    with pytest.raises(ValueError, match="unsupported strategy_bucket"):
        normalize_order_protective_metadata(order)
