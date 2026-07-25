from __future__ import annotations

from types import SimpleNamespace

from app.core.postgres_tls import connection_uses_tls


class RecordingCursor:
    def __init__(self, row):
        self.row = row
        self.executed = False
        self.closed = False

    def execute(self, _query):
        self.executed = True

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, *, ssl_in_use=None, fallback_row=None):
        info_values = {}
        if ssl_in_use is not None:
            info_values["ssl_in_use"] = ssl_in_use
        self.info = SimpleNamespace(**info_values)
        self.cursor_instance = RecordingCursor(fallback_row)

    def cursor(self):
        return self.cursor_instance


def test_connection_uses_tls_prefers_libpq_signal_without_query():
    connection = FakeConnection(ssl_in_use=True, fallback_row={"tls": False})

    assert connection_uses_tls(connection) is True
    assert connection.cursor_instance.executed is False
    assert connection.cursor_instance.closed is False


def test_connection_uses_tls_preserves_false_libpq_signal():
    connection = FakeConnection(ssl_in_use=False, fallback_row={"tls": True})

    assert connection_uses_tls(connection) is False
    assert connection.cursor_instance.executed is False


def test_connection_uses_tls_falls_back_to_pg_stat_ssl_for_older_driver():
    connection = FakeConnection(fallback_row={"tls": True})

    assert connection_uses_tls(connection) is True
    assert connection.cursor_instance.executed is True
    assert connection.cursor_instance.closed is True


def test_connection_uses_tls_uses_supplied_cursor_without_closing_it():
    connection = FakeConnection()
    cursor = RecordingCursor((True,))

    assert connection_uses_tls(connection, cursor) is True
    assert cursor.executed is True
    assert cursor.closed is False
