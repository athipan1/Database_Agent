from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import backtest_routes
import backtest_write_repository as write_repository
from backtest_models import BacktestEquityPoint
from tests.test_backtest_endpoints import HEADERS, _build_client
from trading_db import TradingDB


class _PostgresLikeDB:
    db_type = "postgres"
    param_style = "%s"


class _Cursor:
    pass


def _equity_points(count: int):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        BacktestEquityPoint(
            point_id=f"point-{index}",
            run_id="run-batch",
            timestamp=start + timedelta(days=index),
            equity=100_000 + index,
            drawdown=0.0,
            metadata={"index": index},
        )
        for index in range(count)
    ]


def test_postgres_equity_curve_uses_one_execute_values_batch(monkeypatch):
    calls = []

    def fake_execute_values(cursor, sql, rows, page_size):
        calls.append(
            {
                "cursor": cursor,
                "sql": sql,
                "rows": list(rows),
                "page_size": page_size,
            }
        )

    monkeypatch.setattr(write_repository, "execute_values", fake_execute_values)

    cursor = _Cursor()
    points = _equity_points(1001)
    write_repository._bulk_insert_equity_curve(_PostgresLikeDB(), cursor, points)

    assert len(calls) == 1
    assert calls[0]["cursor"] is cursor
    assert "INSERT INTO backtest_equity_curve" in calls[0]["sql"]
    assert len(calls[0]["rows"]) == 1001
    assert calls[0]["page_size"] == 1001


def test_backtest_routes_do_not_run_schema_ddl_per_request(monkeypatch):
    original_setup = backtest_routes.setup_backtest_tables
    calls = []

    def counted_setup(db):
        calls.append(db)
        return original_setup(db)

    monkeypatch.setattr(backtest_routes, "setup_backtest_tables", counted_setup)
    client = _build_client()

    assert len(calls) == 1

    response = client.get("/skills/missing-skill/backtest-status", headers=HEADERS)
    assert response.status_code == 200
    assert len(calls) == 1

    response = client.get(
        "/backtests/runs/latest",
        headers=HEADERS,
        params={
            "skill_id": "missing",
            "strategy_id": "missing",
            "symbol": "AAPL",
            "timeframe": "1d",
        },
    )
    assert response.status_code == 404
    assert len(calls) == 1


def test_backtest_database_handlers_are_sync_for_fastapi_threadpool():
    db = TradingDB()

    def get_api_key(api_key):
        return api_key

    async def get_correlation_id():
        return "corr-backtest"

    router = backtest_routes.create_backtest_routes(
        db,
        get_api_key,
        get_correlation_id,
    )
    paths = {
        "/market-data/bars",
        "/backtests/runs",
        "/backtests/runs/latest",
        "/backtests/runs/{run_id}",
        "/skills/{skill_id}/backtests",
        "/skills/{skill_id}/backtest-status",
    }
    matching_routes = [
        route for route in router.routes if getattr(route, "path", None) in paths
    ]

    assert matching_routes
    assert all(
        not inspect.iscoroutinefunction(route.endpoint)
        for route in matching_routes
    )


def test_backtest_round_trip_with_1001_equity_points():
    client = _build_client()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    equity_curve = [
        {
            "timestamp": (start + timedelta(days=index)).isoformat(),
            "equity": 100_000 + index,
            "drawdown": 0.0,
            "metadata": {"index": index},
        }
        for index in range(1001)
    ]

    response = client.post(
        "/backtests/runs",
        headers=HEADERS,
        json={
            "run_id": "run-1001-equity-points",
            "account_id": "1",
            "skill_id": "skill-batch",
            "strategy_id": "strategy-batch",
            "symbol": "meta",
            "timeframe": "1d",
            "status": "completed",
            "metrics": {
                "win_rate": 0.55,
                "profit_factor": 1.45,
                "max_drawdown": 0.12,
                "total_trades": 24,
            },
            "equity_curve": equity_curve,
        },
    )

    assert response.status_code == 200
    assert len(response.json()["data"]["equity_curve"]) == 1001

    response = client.get(
        "/backtests/runs/run-1001-equity-points",
        headers=HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["run"]["symbol"] == "META"
    assert len(payload["equity_curve"]) == 1001
    assert payload["equity_curve"][0]["metadata"] == {"index": 0}
    assert payload["equity_curve"][-1]["metadata"] == {"index": 1000}
