import sqlite3
from contextlib import contextmanager

from history_repository import create_signal_record, setup_history_tables
from models import CreateSignalHistoryBody


class SQLiteHistoryTestDB:
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


def test_create_signal_record_uses_two_argument_repository_contract():
    db = SQLiteHistoryTestDB()
    setup_history_tables(db)

    record = create_signal_record(
        db,
        CreateSignalHistoryBody(
            account_id=1,
            symbol="acgl",
            source_agent="manager-agent",
            final_verdict="buy",
        ),
    )

    assert record.symbol == "ACGL"
    with db.connection_scope() as conn:
        row = conn.execute(
            "SELECT symbol, final_verdict FROM signal_history WHERE signal_id = ?",
            (record.signal_id,),
        ).fetchone()
    assert dict(row) == {"symbol": "ACGL", "final_verdict": "buy"}
