from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from backtest_promotion_base import PromotionEvidenceMismatch, setup_backtest_promotion_tables
from backtest_promotion_exact_lookup import get_latest_exact_backtest_promotion


class LookupDB:
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


def _insert(
    db,
    *,
    promotion_id: str,
    run_id: str,
    fingerprint: str,
    state: str,
    created_at: datetime,
    updated_at: datetime,
):
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        cursor.execute(
            """
            INSERT INTO backtest_promotions (
                promotion_id, account_id, run_id, skill_id, strategy_id,
                symbol, timeframe, dataset_fingerprint, engine_version,
                validation_profile, state, version, evidence_version,
                created_at, updated_at, expires_at, reason_code, reason,
                correlation_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                promotion_id,
                "account-1",
                run_id,
                "skill-1",
                "strategy-1",
                "AAPL",
                "1d",
                fingerprint,
                "engine-1",
                "nested_walk_forward_v2",
                state,
                1,
                1,
                created_at.isoformat(),
                updated_at.isoformat(),
                (created_at + timedelta(days=7)).isoformat(),
                "test",
                "test evidence",
                "corr-test",
                "{}",
            ),
        )
        conn.commit()
        cursor.close()


def test_newer_failed_evidence_cannot_be_hidden_by_older_late_update():
    db = LookupDB()
    setup_backtest_promotion_tables(db)
    now = datetime.now(timezone.utc)
    _insert(
        db,
        promotion_id="promotion-old-approved",
        run_id="run-old",
        fingerprint="a" * 64,
        state="APPROVED_FOR_PAPER",
        created_at=now - timedelta(hours=4),
        updated_at=now,
    )
    _insert(
        db,
        promotion_id="promotion-new-failed",
        run_id="run-new",
        fingerprint="b" * 64,
        state="FAILED",
        created_at=now - timedelta(hours=1),
        updated_at=now - timedelta(hours=1),
    )

    newest = get_latest_exact_backtest_promotion(
        db,
        account_id="account-1",
        symbol="aapl",
        strategy_id="strategy-1",
        timeframe="1D",
    )
    assert newest.promotion_id == "promotion-new-failed"
    assert newest.state == "FAILED"

    with pytest.raises(PromotionEvidenceMismatch) as exc_info:
        get_latest_exact_backtest_promotion(
            db,
            account_id="account-1",
            symbol="AAPL",
            strategy_id="strategy-1",
            timeframe="1d",
            required_state="APPROVED_FOR_PAPER",
        )
    assert exc_info.value.metadata["promotion_id"] == "promotion-new-failed"
    db.conn.close()
