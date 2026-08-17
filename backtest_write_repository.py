from __future__ import annotations

import json
import uuid
from typing import Any, Iterable, Sequence

from psycopg2.extras import Json, execute_values

from backtest_models import BacktestRun, BacktestRunDetail
from backtest_repository import _default_skill_result, _json_dumps, _utc_now


def _json_param(db, value: Any) -> Any:
    normalized = {} if value is None else value
    if db.db_type == "postgres":
        return Json(
            normalized,
            dumps=lambda item: json.dumps(item, default=str, sort_keys=True),
        )
    return _json_dumps(normalized)


def _execute_batch(
    db,
    cursor,
    *,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> None:
    if not rows:
        return

    column_list = ", ".join(columns)
    if db.db_type == "postgres":
        execute_values(
            cursor,
            f"INSERT INTO {table} ({column_list}) VALUES %s",
            rows,
            page_size=max(1, len(rows)),
        )
        return

    placeholders = ", ".join([db.param_style] * len(columns))
    cursor.executemany(
        f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
        rows,
    )


def _bulk_insert_trades(db, cursor, trades: Iterable[Any]) -> None:
    rows = [
        (
            trade.trade_id,
            trade.run_id,
            trade.symbol,
            trade.side,
            trade.quantity,
            trade.entry_time,
            trade.entry_price,
            trade.exit_time,
            trade.exit_price,
            trade.realized_pl,
            trade.realized_pl_pct,
            trade.fees,
            trade.outcome,
            _json_param(db, trade.metadata),
            trade.created_at,
        )
        for trade in trades
    ]
    _execute_batch(
        db,
        cursor,
        table="backtest_trades",
        columns=(
            "trade_id",
            "run_id",
            "symbol",
            "side",
            "quantity",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
            "realized_pl",
            "realized_pl_pct",
            "fees",
            "outcome",
            "metadata",
            "created_at",
        ),
        rows=rows,
    )


def _bulk_insert_equity_curve(db, cursor, equity_curve: Iterable[Any]) -> None:
    rows = [
        (
            point.point_id,
            point.run_id,
            point.timestamp,
            point.equity,
            point.drawdown,
            _json_param(db, point.metadata),
        )
        for point in equity_curve
    ]
    _execute_batch(
        db,
        cursor,
        table="backtest_equity_curve",
        columns=(
            "point_id",
            "run_id",
            "timestamp",
            "equity",
            "drawdown",
            "metadata",
        ),
        rows=rows,
    )


def create_backtest_run_detail(db, body) -> BacktestRunDetail:
    """Persist a complete backtest run without per-row PostgreSQL round trips."""

    now = _utc_now()
    run_id = body.run_id or str(uuid.uuid4())
    run = BacktestRun(
        **body.model_dump(
            exclude={
                "run_id",
                "symbol",
                "trades",
                "equity_curve",
                "skill_result",
                "created_at",
                "updated_at",
            }
        ),
        run_id=run_id,
        symbol=body.symbol.upper() if body.symbol else None,
        created_at=body.created_at or now,
        updated_at=body.updated_at or now,
    )
    trades = [
        trade.model_copy(
            update={
                "trade_id": trade.trade_id or str(uuid.uuid4()),
                "run_id": run_id,
                "symbol": trade.symbol.upper(),
                "created_at": trade.created_at or now,
            }
        )
        for trade in body.trades
    ]
    equity_curve = [
        point.model_copy(
            update={
                "point_id": point.point_id or str(uuid.uuid4()),
                "run_id": run_id,
            }
        )
        for point in body.equity_curve
    ]

    skill_result = None
    if body.skill_result:
        skill_result = body.skill_result.model_copy(
            update={
                "result_id": body.skill_result.result_id or str(uuid.uuid4()),
                "run_id": run_id,
                "created_at": body.skill_result.created_at or now,
            }
        )
    elif body.skill_id:
        skill_result = _default_skill_result(run, trades)

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO backtest_runs
                (run_id, account_id, skill_id, strategy_id, symbol, timeframe, start_time, end_time,
                 status, engine_version, parameters, metrics, source_agent, metadata, created_at, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    run.run_id,
                    run.account_id,
                    run.skill_id,
                    run.strategy_id,
                    run.symbol,
                    run.timeframe,
                    run.start_time,
                    run.end_time,
                    run.status,
                    run.engine_version,
                    _json_param(db, run.parameters),
                    _json_param(db, run.metrics),
                    run.source_agent,
                    _json_param(db, run.metadata),
                    run.created_at,
                    run.updated_at,
                ),
            )

            _bulk_insert_trades(db, cursor, trades)
            _bulk_insert_equity_curve(db, cursor, equity_curve)

            if skill_result:
                cursor.execute(
                    f"""
                    INSERT INTO skill_backtest_results
                    (result_id, skill_id, run_id, passed, status, win_rate, profit_factor, expectancy,
                     max_drawdown, total_trades, score, reasons, metadata, created_at)
                    VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                    """,
                    (
                        skill_result.result_id,
                        skill_result.skill_id,
                        skill_result.run_id,
                        int(skill_result.passed),
                        skill_result.status,
                        skill_result.win_rate,
                        skill_result.profit_factor,
                        skill_result.expectancy,
                        skill_result.max_drawdown,
                        skill_result.total_trades,
                        skill_result.score,
                        _json_param(db, skill_result.reasons),
                        _json_param(db, skill_result.metadata),
                        skill_result.created_at,
                    ),
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    return BacktestRunDetail(
        run=run,
        trades=trades,
        equity_curve=equity_curve,
        skill_result=skill_result,
    )
