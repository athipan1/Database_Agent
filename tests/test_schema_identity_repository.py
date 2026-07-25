from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from schema_identity_repository import (
    SCHEMA_NAME,
    SCHEMA_SHA256,
    SCHEMA_VERSION,
    get_schema_identity,
    schema_identity_matches,
    setup_schema_identity_table,
)


class SQLiteDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn=None):
        return (conn or self.conn).cursor()


def test_schema_identity_setup_is_idempotent_and_matches():
    db = SQLiteDB()

    setup_schema_identity_table(db)
    setup_schema_identity_table(db)

    identity = get_schema_identity(db)
    assert identity["schema_name"] == SCHEMA_NAME
    assert identity["schema_version"] == SCHEMA_VERSION
    assert identity["schema_sha256"] == SCHEMA_SHA256
    assert schema_identity_matches(db) is True

    count = db.conn.execute(
        "SELECT count(*) FROM database_agent_schema_metadata"
    ).fetchone()[0]
    assert count == 1


def test_schema_identity_detects_drift():
    db = SQLiteDB()
    setup_schema_identity_table(db)
    db.conn.execute(
        "UPDATE database_agent_schema_metadata SET schema_sha256 = ?",
        ("0" * 64,),
    )
    db.conn.commit()

    assert schema_identity_matches(db) is False
