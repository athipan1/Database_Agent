"""Durable outbox persistence for asynchronous Supabase replication."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List


OUTBOX_TABLE = "supabase_replication_outbox"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize(value: Any) -> str:
    return json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except (TypeError, ValueError):
        return {}


def setup_supabase_replication_outbox(db) -> None:
    """Create the local source-of-truth outbox table and indexes."""

    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE} (
                    event_id TEXT PRIMARY KEY,
                    event_payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at {timestamp_type},
                    last_error TEXT,
                    created_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} NOT NULL,
                    sent_at {timestamp_type}
                )
                """
            )
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{OUTBOX_TABLE}_pending
                ON {OUTBOX_TABLE} (status, next_attempt_at, created_at)
                """
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def reset_stale_supabase_events(db) -> int:
    """Return events left in `sending` state to the retry queue after a restart."""

    now = _utc_now().isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                UPDATE {OUTBOX_TABLE}
                SET status = 'retry', updated_at = {db.param_style}
                WHERE status = 'sending'
                """,
                (now,),
            )
            updated = int(cursor.rowcount or 0)
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def enqueue_supabase_event(db, event: Dict[str, Any]) -> bool:
    """Insert one event idempotently. Returns True only for a new event."""

    event_id = str(event["event_id"])
    payload = _serialize(event)
    now = _utc_now().isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            if db.db_type == "sqlite":
                cursor.execute(
                    f"""
                    INSERT OR IGNORE INTO {OUTBOX_TABLE} (
                        event_id, event_payload, status, attempts,
                        next_attempt_at, created_at, updated_at
                    ) VALUES (?, ?, 'pending', 0, NULL, ?, ?)
                    """,
                    (event_id, payload, now, now),
                )
            else:
                cursor.execute(
                    f"""
                    INSERT INTO {OUTBOX_TABLE} (
                        event_id, event_payload, status, attempts,
                        next_attempt_at, created_at, updated_at
                    ) VALUES (%s, %s, 'pending', 0, NULL, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (event_id, payload, now, now),
                )
            inserted = int(cursor.rowcount or 0) == 1
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def claim_supabase_events(db, *, limit: int = 50) -> List[Dict[str, Any]]:
    """Claim due events for one worker without blocking the trading request path."""

    batch_limit = max(1, min(int(limit), 500))
    now = _utc_now().isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            if db.db_type == "postgres":
                cursor.execute(
                    f"""
                    SELECT event_id, event_payload, attempts
                    FROM {OUTBOX_TABLE}
                    WHERE status IN ('pending', 'retry')
                      AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                    """,
                    (now, batch_limit),
                )
            else:
                cursor.execute(
                    f"""
                    SELECT event_id, event_payload, attempts
                    FROM {OUTBOX_TABLE}
                    WHERE status IN ('pending', 'retry')
                      AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (now, batch_limit),
                )
            rows = [_row_to_dict(row) for row in cursor.fetchall()]
            event_ids = [row["event_id"] for row in rows]
            if event_ids:
                placeholders = ",".join(db.param_style for _ in event_ids)
                cursor.execute(
                    f"""
                    UPDATE {OUTBOX_TABLE}
                    SET status = 'sending', updated_at = {db.param_style}
                    WHERE event_id IN ({placeholders})
                    """,
                    (now, *event_ids),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    claimed: List[Dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["event_payload"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        claimed.append(
            {
                "event_id": row["event_id"],
                "event": payload,
                "attempts": int(row.get("attempts") or 0),
            }
        )
    return claimed


def mark_supabase_event_sent(db, event_id: str) -> None:
    now = _utc_now().isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                UPDATE {OUTBOX_TABLE}
                SET status = 'sent', sent_at = {db.param_style},
                    updated_at = {db.param_style}, last_error = NULL
                WHERE event_id = {db.param_style}
                """,
                (now, now, str(event_id)),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def mark_supabase_event_failed(
    db,
    event_id: str,
    *,
    attempts: int,
    error: str,
    max_attempts: int = 10,
) -> str:
    """Schedule exponential retry or park the event after repeated failures."""

    next_attempt = int(attempts) + 1
    status = "dead" if next_attempt >= max(1, int(max_attempts)) else "retry"
    delay_seconds = min(3600, 2 ** min(next_attempt, 10))
    due_at = (_utc_now() + timedelta(seconds=delay_seconds)).isoformat()
    now = _utc_now().isoformat()
    safe_error = str(error).replace("\n", " ")[:500]
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                UPDATE {OUTBOX_TABLE}
                SET status = {db.param_style}, attempts = {db.param_style},
                    next_attempt_at = {db.param_style}, last_error = {db.param_style},
                    updated_at = {db.param_style}
                WHERE event_id = {db.param_style}
                """,
                (status, next_attempt, due_at, safe_error, now, str(event_id)),
            )
            conn.commit()
            return status
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_supabase_outbox_stats(db) -> Dict[str, int]:
    """Return safe status counts for health reporting."""

    stats = {"pending": 0, "retry": 0, "sending": 0, "sent": 0, "dead": 0}
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT status, COUNT(*) AS total FROM {OUTBOX_TABLE} GROUP BY status"
            )
            for raw_row in cursor.fetchall():
                row = _row_to_dict(raw_row)
                status = str(row.get("status") or "")
                if status in stats:
                    stats[status] = int(row.get("total") or 0)
            return stats
        finally:
            cursor.close()
