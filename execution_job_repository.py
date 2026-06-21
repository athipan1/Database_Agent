from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

import psycopg2
import sqlite3


EXECUTION_JOB_STATUSES = {"queued", "running", "succeeded", "failed"}


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def _format_job_row(row) -> Optional[Dict[str, Any]]:
    return dict(row) if row else None


def setup_execution_job_table(db) -> None:
    pk_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if db.db_type == "sqlite" else "SERIAL PRIMARY KEY"
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS execution_jobs (
                    job_id {pk_type},
                    order_id INTEGER NOT NULL UNIQUE REFERENCES orders(order_id),
                    trade_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    attempts BIGINT NOT NULL DEFAULT 0,
                    max_attempts BIGINT NOT NULL DEFAULT 3,
                    last_error TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_execution_job(db, job_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM execution_jobs WHERE job_id = {db.param_style}", (int(job_id),))
            return _format_job_row(cursor.fetchone())
        finally:
            cursor.close()


def get_execution_job_by_order_id(db, order_id: int) -> Optional[Dict[str, Any]]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM execution_jobs WHERE order_id = {db.param_style}", (int(order_id),))
            return _format_job_row(cursor.fetchone())
        finally:
            cursor.close()


def create_execution_job(db, order_id: int, trade_id: Union[int, str], max_attempts: int = 3) -> Dict[str, Any]:
    setup_execution_job_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO execution_jobs (order_id, trade_id, status, attempts, max_attempts)
                VALUES ({db.param_style}, {db.param_style}, 'queued', 0, {db.param_style})
                """,
                (int(order_id), str(trade_id), int(max_attempts or 3)),
            )
            conn.commit()
        except (psycopg2.errors.UniqueViolation, sqlite3.IntegrityError):
            conn.rollback()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    job = get_execution_job_by_order_id(db, int(order_id))
    if not job:
        raise RuntimeError(f"Execution job for order {order_id} was not created")
    return job


def claim_next_execution_job(db) -> Optional[Dict[str, Any]]:
    setup_execution_job_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            if db.db_type == "sqlite":
                cursor.execute("BEGIN IMMEDIATE;")
                cursor.execute(
                    """
                    SELECT * FROM execution_jobs
                    WHERE status = 'queued' AND attempts < max_attempts
                    ORDER BY created_at ASC, job_id ASC
                    LIMIT 1
                    """
                )
            else:
                cursor.execute("BEGIN;")
                cursor.execute(
                    """
                    SELECT * FROM execution_jobs
                    WHERE status = 'queued' AND attempts < max_attempts
                    ORDER BY created_at ASC, job_id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None
            job = dict(row)
            cursor.execute(
                f"""
                UPDATE execution_jobs
                SET status = 'running', attempts = attempts + 1, updated_at = {db.param_style}
                WHERE job_id = {db.param_style}
                """,
                (_now(db), job["job_id"]),
            )
            conn.commit()
            return get_execution_job(db, job["job_id"])
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def update_execution_job(db, job_id: Union[int, str], updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    setup_execution_job_table(db)
    allowed = {"status", "attempts", "max_attempts", "last_error"}
    clean: Dict[str, Any] = {}
    for key, value in (updates or {}).items():
        if key not in allowed:
            continue
        if key == "status":
            status = str(value).lower()
            if status not in EXECUTION_JOB_STATUSES:
                raise ValueError(f"Unsupported execution job status: {value}")
            clean[key] = status
        else:
            clean[key] = value
    clean["updated_at"] = _now(db)

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            set_clause = ", ".join(f"{key} = {db.param_style}" for key in clean)
            params = list(clean.values()) + [int(job_id)]
            cursor.execute(f"UPDATE execution_jobs SET {set_clause} WHERE job_id = {db.param_style}", tuple(params))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_execution_job(db, job_id)
