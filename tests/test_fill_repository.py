from decimal import Decimal

from fill_repository import calculate_fill_pnl, create_fill_record, get_fill_records, setup_fill_table


class FakeCursor:
    def __init__(self):
        self.rows = []
        self.lastrowid = 1
        self.fetchone_result = None

    def execute(self, query, params=None):
        if query.strip().startswith("SELECT average_cost"):
            self.fetchone_result = {"average_cost": "100"}
        elif query.strip().startswith("SELECT * FROM fills WHERE fill_id"):
            self.fetchone_result = {
                "fill_id": 1,
                "account_id": 1,
                "order_id": 10,
                "trade_id": "t-1",
                "symbol": "AAPL",
                "side": "sell",
                "quantity": 2,
                "fill_price": "110",
                "average_entry_price": "100",
                "gross_pnl": "20",
                "fees": "1",
                "realized_pnl": "19",
                "broker_fill_id": "bf-1",
                "broker_order_id": "bo-1",
                "liquidity": "taker",
                "filled_at": "2026-06-21T15:00:00+00:00",
                "correlation_id": "corr-1",
                "metadata": '{"source":"test"}',
                "created_at": "2026-06-21T15:00:00+00:00",
            }
        elif query.strip().startswith("SELECT * FROM fills WHERE account_id"):
            self.rows = [{"fill_id": 1, "account_id": 1, "order_id": 10, "trade_id": "t-1", "symbol": "AAPL", "side": "sell", "quantity": 2, "fill_price": "110", "average_entry_price": "100", "gross_pnl": "20", "fees": "1", "realized_pnl": "19", "broker_fill_id": "bf-1", "broker_order_id": "bo-1", "liquidity": "taker", "filled_at": "2026-06-21T15:00:00+00:00", "correlation_id": "corr-1", "metadata": '{}", "created_at": "2026-06-21T15:00:00+00:00"}]

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def commit(self):
        pass

    def rollback(self):
        pass


class Scope:
    def __enter__(self):
        return FakeConnection()

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.cursor = FakeCursor()

    def connection_scope(self):
        return Scope()

    def get_cursor(self, conn):
        return self.cursor

    def _to_decimal(self, value):
        if value is None:
            return None
        return Decimal(str(value))


def test_calculate_sell_fill_realized_pnl_after_fees():
    pnl = calculate_fill_pnl(side="sell", quantity=2, fill_price=110, average_entry_price=100, fees=1)
    assert pnl["gross_pnl"] == Decimal("20")
    assert pnl["realized_pnl"] == Decimal("19")


def test_calculate_buy_cover_realized_pnl_after_fees():
    pnl = calculate_fill_pnl(side="buy", quantity=2, fill_price=90, average_entry_price=100, fees=1)
    assert pnl["gross_pnl"] == Decimal("20")
    assert pnl["realized_pnl"] == Decimal("19")


def test_create_fill_uses_position_average_cost_and_parses_metadata():
    db = FakeDB()
    setup_fill_table(db)
    fill = create_fill_record(
        db,
        account_id=1,
        order_id=10,
        trade_id="t-1",
        symbol="AAPL",
        side="sell",
        quantity=2,
        fill_price=110,
        average_entry_price=None,
        fees=1,
        broker_fill_id="bf-1",
        broker_order_id="bo-1",
        liquidity="taker",
        correlation_id="corr-1",
        metadata={"source": "test"},
    )
    assert fill["realized_pnl"] == Decimal("19")
    assert fill["metadata"] == {"source": "test"}


def test_get_fill_records_returns_decimal_fields():
    records = get_fill_records(FakeDB(), 1, symbol="AAPL")
    assert records[0]["realized_pnl"] == Decimal("19")
