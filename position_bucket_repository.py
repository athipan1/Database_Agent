from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

VALID_STRATEGY_BUCKETS = {"core_dividend", "value_rebound", "news_momentum", "unassigned"}
UNASSIGNED = "unassigned"


def _param(db) -> str:
    return db.param_style


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def normalize_strategy_bucket(value: Any) -> str:
    bucket = str(value or UNASSIGNED).strip().lower()
    return bucket if bucket in VALID_STRATEGY_BUCKETS else UNASSIGNED


def setup_position_bucket_columns(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket", "TEXT DEFAULT 'unassigned'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_source", "TEXT DEFAULT 'unknown'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_reason", "TEXT")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_updated_at", timestamp_type)
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


def enrich_positions_with_bucket_metadata(db, account_id: int, positions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    symbols = [str(row.get("symbol") or "").upper() for row in positions or [] if row.get("symbol")]
    if not symbols:
        return list(positions or [])
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
            metadata = {str(row["symbol"]).upper(): dict(row) for row in cursor.fetchall()}
        finally:
            cursor.close()
    enriched: List[Dict[str, Any]] = []
    for row in positions or []:
        item = dict(row)
        symbol = str(item.get("symbol") or "").upper()
        meta = metadata.get(symbol) or {}
        item["strategy_bucket"] = normalize_strategy_bucket(item.get("strategy_bucket") or meta.get("strategy_bucket"))
        item["strategy_bucket_source"] = item.get("strategy_bucket_source") or meta.get("strategy_bucket_source") or "unknown"
        item["strategy_bucket_reason"] = item.get("strategy_bucket_reason") or meta.get("strategy_bucket_reason")
        item["strategy_bucket_updated_at"] = item.get("strategy_bucket_updated_at") or meta.get("strategy_bucket_updated_at")
        enriched.append(item)
    return enriched


def upsert_position_bucket(db, account_id: int, symbol: str, strategy_bucket: str, *, source: str = "manual", reason: Optional[str] = None) -> Optional[Dict[str, Any]]:
    symbol = str(symbol or "").upper().strip()
    if not symbol:
        raise ValueError("symbol is required")
    bucket = normalize_strategy_bucket(strategy_bucket)
    now = _now(db)
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                UPDATE positions
                SET strategy_bucket = {p}, strategy_bucket_source = {p}, strategy_bucket_reason = {p}, strategy_bucket_updated_at = {p}
                WHERE account_id = {p} AND symbol = {p}
                """,
                (bucket, source, reason, now, account_id, symbol),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return None
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_position_bucket(db, account_id, symbol)


def bulk_upsert_position_buckets(db, account_id: int, assignments: Iterable[Dict[str, Any]], *, default_source: str = "manual") -> List[Dict[str, Any]]:
    updated: List[Dict[str, Any]] = []
    for item in assignments or []:
        row = upsert_position_bucket(
            db,
            account_id,
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
            cursor.execute(f"SELECT * FROM positions WHERE account_id = {p} AND symbol = {p}", (account_id, str(symbol).upper()))
            return _row_to_dict(db, cursor.fetchone())
        finally:
            cursor.close()


def list_position_buckets(db, account_id: int) -> List[Dict[str, Any]]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM positions WHERE account_id = {p} ORDER BY symbol", (account_id,))
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

    async def set_position_bucket_endpoint(account_id: int, symbol: str, payload: Dict[str, Any]):
        row = upsert_position_bucket(
            db,
            account_id,
            symbol,
            payload.get("strategy_bucket") or payload.get("bucket") or UNASSIGNED,
            source=payload.get("source") or "manual",
            reason=payload.get("reason"),
        )
        if not row:
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail=f"Position {symbol.upper()} not found for account {account_id}")
        return wrap_response(data=row)

    async def bulk_set_position_buckets_endpoint(account_id: int, payload: Dict[str, Any]):
        assignments = payload.get("assignments") or []
        updated = bulk_upsert_position_buckets(db, account_id, assignments, default_source=payload.get("source") or "manual")
        return wrap_response(data={"updated": updated, "updated_count": len(updated), "requested_count": len(assignments)})

    app.add_api_route(
        "/accounts/{account_id}/position-buckets",
        list_position_buckets_endpoint,
        methods=["GET"],
        dependencies=dependencies,
        name="list_position_buckets_endpoint",
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
