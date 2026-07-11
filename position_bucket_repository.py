from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

VALID_STRATEGY_BUCKETS = {
    "core_dividend",
    "quality_growth",
    "value_rebound",
    "news_momentum",
    "unassigned",
}
UNASSIGNED = "unassigned"
ASSIGNMENT_ENV = "STRATEGY_BUCKET_ASSIGNMENTS_JSON"
ASSIGNMENT_ACCOUNT_ENV = "STRATEGY_BUCKET_ASSIGNMENTS_ACCOUNT_ID"


def _param(db) -> str:
    return db.param_style


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def normalize_strategy_bucket(value: Any) -> str:
    bucket = str(value or UNASSIGNED).strip().lower()
    return bucket if bucket in VALID_STRATEGY_BUCKETS else UNASSIGNED


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _default_assignment_account_id() -> int:
    try:
        return int(os.getenv(ASSIGNMENT_ACCOUNT_ENV, "1"))
    except Exception:
        return 1


def _configured_strategy_bucket_assignments() -> List[Dict[str, Any]]:
    """Load durable symbol assignments from environment configuration.

    Supported forms:
      * JSON object: {"CINF": "value_rebound"}
      * JSON object by account: {"1": {"CINF": "value_rebound"}}
      * JSON list of assignment objects
      * Compact fallback: CINF=value_rebound,ADBE=value_rebound

    The environment is intentionally a seed source. It survives ephemeral broker
    snapshot databases because deployment configuration is version-controlled or
    supplied by the operator at runtime.
    """
    raw = os.getenv(ASSIGNMENT_ENV, "").strip()
    if not raw:
        return []

    default_account_id = _default_assignment_account_id()
    parsed: Any
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None

    assignments: List[Dict[str, Any]] = []

    def remember(account_id: Any, symbol: Any, bucket: Any, *, source: str = "deployment_seed", reason: Any = None) -> None:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_bucket = normalize_strategy_bucket(bucket)
        if not normalized_symbol or normalized_bucket == UNASSIGNED:
            return
        try:
            normalized_account_id = int(account_id)
        except Exception:
            normalized_account_id = default_account_id
        assignments.append(
            {
                "account_id": normalized_account_id,
                "symbol": normalized_symbol,
                "strategy_bucket": normalized_bucket,
                "source": str(source or "deployment_seed"),
                "reason": reason or "seeded_from_deployment_configuration",
            }
        )

    if isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            remember(
                item.get("account_id", default_account_id),
                item.get("symbol") or item.get("ticker"),
                item.get("strategy_bucket") or item.get("bucket"),
                source=str(item.get("source") or "deployment_seed"),
                reason=item.get("reason"),
            )
        return assignments

    if isinstance(parsed, dict):
        nested_accounts = any(isinstance(value, dict) for value in parsed.values())
        if nested_accounts:
            for account_id, values in parsed.items():
                if not isinstance(values, dict):
                    continue
                for symbol, bucket in values.items():
                    if isinstance(bucket, dict):
                        remember(
                            account_id,
                            symbol,
                            bucket.get("strategy_bucket") or bucket.get("bucket"),
                            source=str(bucket.get("source") or "deployment_seed"),
                            reason=bucket.get("reason"),
                        )
                    else:
                        remember(account_id, symbol, bucket)
        else:
            for symbol, bucket in parsed.items():
                if isinstance(bucket, dict):
                    remember(
                        bucket.get("account_id", default_account_id),
                        symbol,
                        bucket.get("strategy_bucket") or bucket.get("bucket"),
                        source=str(bucket.get("source") or "deployment_seed"),
                        reason=bucket.get("reason"),
                    )
                else:
                    remember(default_account_id, symbol, bucket)
        return assignments

    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if not token or "=" not in token:
            continue
        symbol, bucket = token.split("=", 1)
        remember(default_account_id, symbol, bucket)
    return assignments


def _create_assignment_table(cursor, db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS strategy_bucket_assignments (
            account_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            strategy_bucket TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'manual',
            reason TEXT,
            updated_at {timestamp_type} NOT NULL,
            PRIMARY KEY (account_id, symbol)
        )
        """
    )


def _upsert_assignment_cursor(
    cursor,
    db,
    account_id: int,
    symbol: str,
    strategy_bucket: str,
    *,
    source: str,
    reason: Optional[str],
    updated_at: Any = None,
) -> Optional[Dict[str, Any]]:
    symbol = _normalize_symbol(symbol)
    bucket = normalize_strategy_bucket(strategy_bucket)
    if not symbol or bucket == UNASSIGNED:
        return None
    p = _param(db)
    timestamp = updated_at or _now(db)
    cursor.execute(
        f"""
        INSERT INTO strategy_bucket_assignments (
            account_id, symbol, strategy_bucket, source, reason, updated_at
        ) VALUES ({p}, {p}, {p}, {p}, {p}, {p})
        ON CONFLICT (account_id, symbol) DO UPDATE SET
            strategy_bucket = excluded.strategy_bucket,
            source = excluded.source,
            reason = excluded.reason,
            updated_at = excluded.updated_at
        """,
        (account_id, symbol, bucket, source, reason, timestamp),
    )
    return {
        "account_id": account_id,
        "symbol": symbol,
        "strategy_bucket": bucket,
        "source": source,
        "reason": reason,
        "updated_at": timestamp,
    }


def _seed_strategy_bucket_assignments(cursor, db) -> None:
    for item in _configured_strategy_bucket_assignments():
        _upsert_assignment_cursor(
            cursor,
            db,
            int(item["account_id"]),
            str(item["symbol"]),
            str(item["strategy_bucket"]),
            source=str(item.get("source") or "deployment_seed"),
            reason=item.get("reason"),
        )


def _install_sqlite_assignment_triggers(cursor) -> None:
    valid = "'core_dividend','quality_growth','value_rebound','news_momentum'"
    cursor.executescript(
        f"""
        CREATE TRIGGER IF NOT EXISTS trg_positions_apply_strategy_assignment_insert
        AFTER INSERT ON positions
        WHEN COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned')
        BEGIN
            UPDATE positions
            SET strategy_bucket = (
                    SELECT strategy_bucket FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                ),
                strategy_bucket_source = 'canonical_assignment',
                strategy_bucket_reason = COALESCE((
                    SELECT reason FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                ), 'restored_from_canonical_assignment'),
                strategy_bucket_updated_at = CURRENT_TIMESTAMP
            WHERE position_id = NEW.position_id
              AND EXISTS (
                    SELECT 1 FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                );
        END;

        CREATE TRIGGER IF NOT EXISTS trg_positions_apply_strategy_assignment_update
        AFTER UPDATE OF strategy_bucket, symbol, account_id ON positions
        WHEN COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned')
        BEGIN
            UPDATE positions
            SET strategy_bucket = (
                    SELECT strategy_bucket FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                ),
                strategy_bucket_source = 'canonical_assignment',
                strategy_bucket_reason = COALESCE((
                    SELECT reason FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                ), 'restored_from_canonical_assignment'),
                strategy_bucket_updated_at = CURRENT_TIMESTAMP
            WHERE position_id = NEW.position_id
              AND EXISTS (
                    SELECT 1 FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                );
        END;

        CREATE TRIGGER IF NOT EXISTS trg_positions_capture_strategy_assignment_insert
        AFTER INSERT ON positions
        WHEN TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN ({valid})
        BEGIN
            INSERT INTO strategy_bucket_assignments (
                account_id, symbol, strategy_bucket, source, reason, updated_at
            ) VALUES (
                NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                COALESCE(NEW.strategy_bucket_source, 'position_row'),
                NEW.strategy_bucket_reason, CURRENT_TIMESTAMP
            )
            ON CONFLICT (account_id, symbol) DO UPDATE SET
                strategy_bucket = excluded.strategy_bucket,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            WHERE strategy_bucket_assignments.strategy_bucket <> excluded.strategy_bucket;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_positions_capture_strategy_assignment_update
        AFTER UPDATE OF strategy_bucket ON positions
        WHEN TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN ({valid})
        BEGIN
            INSERT INTO strategy_bucket_assignments (
                account_id, symbol, strategy_bucket, source, reason, updated_at
            ) VALUES (
                NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                COALESCE(NEW.strategy_bucket_source, 'position_row'),
                NEW.strategy_bucket_reason, CURRENT_TIMESTAMP
            )
            ON CONFLICT (account_id, symbol) DO UPDATE SET
                strategy_bucket = excluded.strategy_bucket,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            WHERE strategy_bucket_assignments.strategy_bucket <> excluded.strategy_bucket;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_orders_apply_strategy_assignment_insert
        AFTER INSERT ON orders
        WHEN COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned')
        BEGIN
            UPDATE orders
            SET strategy_bucket = (
                    SELECT strategy_bucket FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                )
            WHERE order_id = NEW.order_id
              AND EXISTS (
                    SELECT 1 FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                );
        END;

        CREATE TRIGGER IF NOT EXISTS trg_orders_apply_strategy_assignment_update
        AFTER UPDATE OF strategy_bucket, symbol, account_id ON orders
        WHEN COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned')
        BEGIN
            UPDATE orders
            SET strategy_bucket = (
                    SELECT strategy_bucket FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                )
            WHERE order_id = NEW.order_id
              AND EXISTS (
                    SELECT 1 FROM strategy_bucket_assignments
                    WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol)
                );
        END;

        CREATE TRIGGER IF NOT EXISTS trg_orders_capture_strategy_assignment_insert
        AFTER INSERT ON orders
        WHEN TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN ({valid})
        BEGIN
            INSERT INTO strategy_bucket_assignments (
                account_id, symbol, strategy_bucket, source, reason, updated_at
            ) VALUES (
                NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                'order_row', 'captured_from_order', CURRENT_TIMESTAMP
            )
            ON CONFLICT (account_id, symbol) DO UPDATE SET
                strategy_bucket = excluded.strategy_bucket,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            WHERE strategy_bucket_assignments.strategy_bucket <> excluded.strategy_bucket;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_orders_capture_strategy_assignment_update
        AFTER UPDATE OF strategy_bucket ON orders
        WHEN TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN ({valid})
        BEGIN
            INSERT INTO strategy_bucket_assignments (
                account_id, symbol, strategy_bucket, source, reason, updated_at
            ) VALUES (
                NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                'order_row', 'captured_from_order', CURRENT_TIMESTAMP
            )
            ON CONFLICT (account_id, symbol) DO UPDATE SET
                strategy_bucket = excluded.strategy_bucket,
                source = excluded.source,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            WHERE strategy_bucket_assignments.strategy_bucket <> excluded.strategy_bucket;
        END;
        """
    )


def _install_postgres_assignment_triggers(cursor) -> None:
    cursor.execute(
        """
        CREATE OR REPLACE FUNCTION apply_position_strategy_bucket_assignment()
        RETURNS TRIGGER AS $$
        DECLARE
            assignment RECORD;
        BEGIN
            IF COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned') THEN
                SELECT strategy_bucket, source, reason, updated_at
                INTO assignment
                FROM strategy_bucket_assignments
                WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol);
                IF FOUND THEN
                    NEW.strategy_bucket := assignment.strategy_bucket;
                    NEW.strategy_bucket_source := 'canonical_assignment';
                    NEW.strategy_bucket_reason := COALESCE(assignment.reason, 'restored_from_canonical_assignment');
                    NEW.strategy_bucket_updated_at := assignment.updated_at;
                END IF;
            END IF;

            IF TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN (
                'core_dividend', 'quality_growth', 'value_rebound', 'news_momentum'
            ) THEN
                INSERT INTO strategy_bucket_assignments (
                    account_id, symbol, strategy_bucket, source, reason, updated_at
                ) VALUES (
                    NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                    COALESCE(NEW.strategy_bucket_source, 'position_row'),
                    NEW.strategy_bucket_reason, CURRENT_TIMESTAMP
                )
                ON CONFLICT (account_id, symbol) DO UPDATE SET
                    strategy_bucket = EXCLUDED.strategy_bucket,
                    source = EXCLUDED.source,
                    reason = EXCLUDED.reason,
                    updated_at = EXCLUDED.updated_at
                WHERE strategy_bucket_assignments.strategy_bucket IS DISTINCT FROM EXCLUDED.strategy_bucket;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION apply_order_strategy_bucket_assignment()
        RETURNS TRIGGER AS $$
        DECLARE
            assignment RECORD;
        BEGIN
            IF COALESCE(TRIM(LOWER(NEW.strategy_bucket)), 'unassigned') IN ('', 'unassigned') THEN
                SELECT strategy_bucket, source, reason, updated_at
                INTO assignment
                FROM strategy_bucket_assignments
                WHERE account_id = NEW.account_id AND symbol = UPPER(NEW.symbol);
                IF FOUND THEN
                    NEW.strategy_bucket := assignment.strategy_bucket;
                END IF;
            END IF;

            IF TRIM(LOWER(COALESCE(NEW.strategy_bucket, ''))) IN (
                'core_dividend', 'quality_growth', 'value_rebound', 'news_momentum'
            ) THEN
                INSERT INTO strategy_bucket_assignments (
                    account_id, symbol, strategy_bucket, source, reason, updated_at
                ) VALUES (
                    NEW.account_id, UPPER(NEW.symbol), LOWER(NEW.strategy_bucket),
                    'order_row', 'captured_from_order', CURRENT_TIMESTAMP
                )
                ON CONFLICT (account_id, symbol) DO UPDATE SET
                    strategy_bucket = EXCLUDED.strategy_bucket,
                    source = EXCLUDED.source,
                    reason = EXCLUDED.reason,
                    updated_at = EXCLUDED.updated_at
                WHERE strategy_bucket_assignments.strategy_bucket IS DISTINCT FROM EXCLUDED.strategy_bucket;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    cursor.execute("DROP TRIGGER IF EXISTS trg_positions_strategy_bucket_insert ON positions")
    cursor.execute("DROP TRIGGER IF EXISTS trg_positions_strategy_bucket_update ON positions")
    cursor.execute("DROP TRIGGER IF EXISTS trg_orders_strategy_bucket_insert ON orders")
    cursor.execute("DROP TRIGGER IF EXISTS trg_orders_strategy_bucket_update ON orders")
    cursor.execute(
        """
        CREATE TRIGGER trg_positions_strategy_bucket_insert
        BEFORE INSERT ON positions
        FOR EACH ROW EXECUTE FUNCTION apply_position_strategy_bucket_assignment()
        """
    )
    cursor.execute(
        """
        CREATE TRIGGER trg_positions_strategy_bucket_update
        BEFORE UPDATE OF strategy_bucket, symbol, account_id ON positions
        FOR EACH ROW EXECUTE FUNCTION apply_position_strategy_bucket_assignment()
        """
    )
    cursor.execute(
        """
        CREATE TRIGGER trg_orders_strategy_bucket_insert
        BEFORE INSERT ON orders
        FOR EACH ROW EXECUTE FUNCTION apply_order_strategy_bucket_assignment()
        """
    )
    cursor.execute(
        """
        CREATE TRIGGER trg_orders_strategy_bucket_update
        BEFORE UPDATE OF strategy_bucket, symbol, account_id ON orders
        FOR EACH ROW EXECUTE FUNCTION apply_order_strategy_bucket_assignment()
        """
    )


def setup_position_bucket_columns(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket", "TEXT DEFAULT 'unassigned'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_source", "TEXT DEFAULT 'unknown'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_reason", "TEXT")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_updated_at", timestamp_type)
            db._add_column_if_not_exists(cursor, "orders", "strategy_bucket", "TEXT DEFAULT 'unassigned'")
            _create_assignment_table(cursor, db)
            _seed_strategy_bucket_assignments(cursor, db)
            if db.db_type == "sqlite":
                _install_sqlite_assignment_triggers(cursor)
            else:
                _install_postgres_assignment_triggers(cursor)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    register_position_bucket_routes(db)


def _row_to_dict(db, row) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    item = dict(row)
    for key in ("average_cost", "current_market_price", "market_value"):
        if key in item:
            item[key] = db._to_decimal(item[key])
    item["strategy_bucket"] = normalize_strategy_bucket(item.get("strategy_bucket"))
    item["strategy_bucket_source"] = item.get("strategy_bucket_source") or "unknown"
    return item


def list_strategy_bucket_assignments(db, account_id: int) -> List[Dict[str, Any]]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT account_id, symbol, strategy_bucket, source, reason, updated_at
                FROM strategy_bucket_assignments
                WHERE account_id = {p}
                ORDER BY symbol
                """,
                (account_id,),
            )
            return [dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()


def get_strategy_bucket_assignment(db, account_id: int, symbol: str) -> Optional[Dict[str, Any]]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT account_id, symbol, strategy_bucket, source, reason, updated_at
                FROM strategy_bucket_assignments
                WHERE account_id = {p} AND symbol = {p}
                """,
                (account_id, _normalize_symbol(symbol)),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            cursor.close()


def upsert_strategy_bucket_assignment(
    db,
    account_id: int,
    symbol: str,
    strategy_bucket: str,
    *,
    source: str = "manual",
    reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            result = _upsert_assignment_cursor(
                cursor,
                db,
                account_id,
                symbol,
                strategy_bucket,
                source=source,
                reason=reason,
            )
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def enrich_positions_with_bucket_metadata(db, account_id: int, positions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = list(positions or [])
    symbols = [_normalize_symbol(row.get("symbol")) for row in rows if row.get("symbol")]
    if not symbols:
        return rows
    p = _param(db)
    placeholders = ",".join([p] * len(symbols))
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT symbol, strategy_bucket, strategy_bucket_source, strategy_bucket_reason, strategy_bucket_updated_at
                FROM positions
                WHERE account_id = {p} AND symbol IN ({placeholders})
                """,
                tuple([account_id, *symbols]),
            )
            metadata = {_normalize_symbol(row["symbol"]): dict(row) for row in cursor.fetchall()}
            cursor.execute(
                f"""
                SELECT symbol, strategy_bucket, source, reason, updated_at
                FROM strategy_bucket_assignments
                WHERE account_id = {p} AND symbol IN ({placeholders})
                """,
                tuple([account_id, *symbols]),
            )
            assignments = {_normalize_symbol(row["symbol"]): dict(row) for row in cursor.fetchall()}
        finally:
            cursor.close()
    enriched: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        symbol = _normalize_symbol(item.get("symbol"))
        meta = metadata.get(symbol) or {}
        assignment = assignments.get(symbol) or {}
        bucket = normalize_strategy_bucket(item.get("strategy_bucket") or meta.get("strategy_bucket"))
        if bucket == UNASSIGNED:
            bucket = normalize_strategy_bucket(assignment.get("strategy_bucket"))
        item["strategy_bucket"] = bucket
        item["strategy_bucket_source"] = (
            item.get("strategy_bucket_source")
            or meta.get("strategy_bucket_source")
            or ("canonical_assignment" if assignment else "unknown")
        )
        item["strategy_bucket_reason"] = (
            item.get("strategy_bucket_reason")
            or meta.get("strategy_bucket_reason")
            or assignment.get("reason")
        )
        item["strategy_bucket_updated_at"] = (
            item.get("strategy_bucket_updated_at")
            or meta.get("strategy_bucket_updated_at")
            or assignment.get("updated_at")
        )
        enriched.append(item)
    return enriched


def upsert_position_bucket(
    db,
    account_id: int,
    symbol: str,
    strategy_bucket: str,
    *,
    source: str = "manual",
    reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    symbol = _normalize_symbol(symbol)
    if not symbol:
        raise ValueError("symbol is required")
    bucket = normalize_strategy_bucket(strategy_bucket)
    if bucket == UNASSIGNED:
        raise ValueError("strategy_bucket must be an assigned bucket")
    now = _now(db)
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            assignment = _upsert_assignment_cursor(
                cursor,
                db,
                account_id,
                symbol,
                bucket,
                source=source,
                reason=reason,
                updated_at=now,
            )
            cursor.execute(
                f"""
                UPDATE positions
                SET strategy_bucket = {p}, strategy_bucket_source = {p}, strategy_bucket_reason = {p}, strategy_bucket_updated_at = {p}
                WHERE account_id = {p} AND symbol = {p}
                """,
                (bucket, source, reason, now, account_id, symbol),
            )
            position_updated = bool(cursor.rowcount)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    row = get_position_bucket(db, account_id, symbol)
    if row:
        row["position_updated"] = position_updated
        return row
    if assignment:
        assignment["position_updated"] = False
    return assignment


def bulk_upsert_position_buckets(
    db,
    account_id: int,
    assignments: Iterable[Dict[str, Any]],
    *,
    default_source: str = "manual",
) -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []
    for item in assignments or []:
        row = upsert_position_bucket(
            db,
            int(item.get("account_id") or account_id),
            str(item.get("symbol") or ""),
            str(item.get("strategy_bucket") or item.get("bucket") or UNASSIGNED),
            source=str(item.get("source") or default_source),
            reason=item.get("reason"),
        )
        if row:
            updated.append(row)
    return updated


def get_position_bucket(db, account_id: int, symbol: str) -> Optional[Dict[str, Any]]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM positions WHERE account_id = {p} AND symbol = {p}",
                (account_id, _normalize_symbol(symbol)),
            )
            return _row_to_dict(db, cursor.fetchone())
        finally:
            cursor.close()


def list_position_buckets(db, account_id: int) -> List[Dict[str, Any]]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM positions WHERE account_id = {p} ORDER BY symbol",
                (account_id,),
            )
            return [_row_to_dict(db, row) for row in cursor.fetchall()]
        finally:
            cursor.close()


def _main_module():
    import sys

    return sys.modules.get("main") or sys.modules.get("__main__")


def register_position_bucket_routes(db) -> None:
    main_module = _main_module()
    app = getattr(main_module, "app", None)
    if app is None or getattr(app.state, "position_bucket_routes_registered", False):
        return

    wrap_response = getattr(main_module, "wrap_response", None)
    if wrap_response is None:
        def wrap_response(data=None, status="success", error=None):
            return {"status": status, "agent_type": "database", "data": data, "error": error}

    dependencies = []
    get_api_key = getattr(main_module, "get_api_key", None)
    if get_api_key is not None:
        try:
            from fastapi import Depends

            dependencies = [Depends(get_api_key)]
        except Exception:
            dependencies = []

    async def list_position_buckets_endpoint(account_id: int):
        return wrap_response(data=list_position_buckets(db, account_id))

    async def list_strategy_bucket_assignments_endpoint(account_id: int):
        rows = list_strategy_bucket_assignments(db, account_id)
        return wrap_response(data={"assignments": rows, "count": len(rows)})

    async def set_position_bucket_endpoint(account_id: int, symbol: str, payload: Dict[str, Any]):
        row = upsert_position_bucket(
            db,
            account_id,
            symbol,
            payload.get("strategy_bucket") or payload.get("bucket") or UNASSIGNED,
            source=payload.get("source") or "manual",
            reason=payload.get("reason"),
        )
        return wrap_response(data=row)

    async def bulk_set_position_buckets_endpoint(account_id: int, payload: Dict[str, Any]):
        assignments = payload.get("assignments") or []
        updated = bulk_upsert_position_buckets(
            db,
            account_id,
            assignments,
            default_source=payload.get("source") or "manual",
        )
        return wrap_response(
            data={
                "updated": updated,
                "updated_count": len(updated),
                "requested_count": len(assignments),
            }
        )

    app.add_api_route(
        "/accounts/{account_id}/position-buckets",
        list_position_buckets_endpoint,
        methods=["GET"],
        dependencies=dependencies,
        name="list_position_buckets_endpoint",
    )
    app.add_api_route(
        "/accounts/{account_id}/strategy-bucket-assignments",
        list_strategy_bucket_assignments_endpoint,
        methods=["GET"],
        dependencies=dependencies,
        name="list_strategy_bucket_assignments_endpoint",
    )
    app.add_api_route(
        "/accounts/{account_id}/position-buckets/{symbol}",
        set_position_bucket_endpoint,
        methods=["PATCH"],
        dependencies=dependencies,
        name="set_position_bucket_endpoint",
    )
    app.add_api_route(
        "/accounts/{account_id}/position-buckets/bulk",
        bulk_set_position_buckets_endpoint,
        methods=["POST"],
        dependencies=dependencies,
        name="bulk_set_position_buckets_endpoint",
    )
    app.state.position_bucket_routes_registered = True
