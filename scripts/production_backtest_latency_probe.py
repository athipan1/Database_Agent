"""Temporary production Railway Backtest POST -> exact GET latency probe."""

from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone


def _request_json(method: str, url: str, headers: dict[str, str], payload=None, timeout=45):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        elapsed = time.perf_counter() - started
        return int(response.status), json.loads(raw.decode("utf-8")), elapsed, len(raw)


def main() -> int:
    base_url = os.environ["DATABASE_AGENT_URL"].strip().rstrip("/")
    api_key = os.environ["DATABASE_AGENT_API_KEY"].strip()
    if not api_key:
        raise RuntimeError("DATABASE_AGENT_API_KEY is missing")

    github_run_id = os.environ.get("GITHUB_RUN_ID", "local")
    github_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    run_id = f"prod-backtest-latency-{github_run_id}-{github_attempt}"
    skill_id = f"latency-probe-{github_run_id}"
    strategy_id = "storage-only-latency-v1"
    symbol = "ZZTEST"
    timeframe = "1d"
    correlation_id = f"prod-backtest-latency-{github_run_id}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-API-KEY": api_key,
        "X-Correlation-ID": correlation_id,
    }

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trades = []
    for i in range(43):
        entry = start + timedelta(days=i * 2)
        trades.append(
            {
                "trade_id": f"{run_id}-trade-{i:03d}",
                "run_id": run_id,
                "symbol": symbol,
                "side": "buy",
                "quantity": 1.0,
                "entry_time": entry.isoformat(),
                "entry_price": 100.0 + i,
                "exit_time": (entry + timedelta(days=1)).isoformat(),
                "exit_price": 100.5 + i,
                "realized_pl": 0.5,
                "realized_pl_pct": 0.005,
                "fees": 0.0,
                "outcome": "diagnostic",
                "metadata": {"synthetic": True, "safe_for_trading": False},
            }
        )

    equity_curve = []
    for i in range(1001):
        equity_curve.append(
            {
                "point_id": f"{run_id}-point-{i:04d}",
                "run_id": run_id,
                "timestamp": (start + timedelta(minutes=i)).isoformat(),
                "equity": 100000.0 + i * 0.1,
                "drawdown": 0.0,
                "metadata": {"synthetic": True},
            }
        )

    payload = {
        "run_id": run_id,
        "account_id": "diagnostic",
        "skill_id": skill_id,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "start_time": start.isoformat(),
        "end_time": (start + timedelta(days=90)).isoformat(),
        "status": "completed",
        "engine_version": "production-latency-probe-v1",
        "parameters": {"storage_only": True, "diagnostic": True},
        "metrics": {"synthetic": True},
        "source_agent": "production-backtest-latency-probe",
        "metadata": {
            "synthetic": True,
            "storage_only": True,
            "safe_for_trading": False,
            "promotion_eligible": False,
            "github_run_id": github_run_id,
        },
        "trades": trades,
        "equity_curve": equity_curve,
        "skill_result": {
            "result_id": f"{run_id}-skill-result",
            "skill_id": skill_id,
            "run_id": run_id,
            "passed": False,
            "status": "diagnostic_only",
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "total_trades": 43,
            "score": 0.0,
            "reasons": ["synthetic production latency probe; never eligible for trading"],
            "metadata": {"synthetic": True, "safe_for_trading": False},
        },
    }

    ready_status, ready_body, ready_seconds, _ = _request_json(
        "GET", f"{base_url}/ready", headers
    )
    ready_data = ready_body.get("data") if isinstance(ready_body, dict) else None
    if ready_status != 200 or not isinstance(ready_data, dict) or ready_data.get("ready") is not True:
        raise RuntimeError(f"production /ready failed: HTTP {ready_status}")
    if ready_data.get("provider") != "supabase" or ready_data.get("schema_identity_match") is not True:
        raise RuntimeError(f"unexpected production readiness: {ready_data}")

    post_status, post_body, post_seconds, post_bytes = _request_json(
        "POST", f"{base_url}/backtests/runs", headers, payload
    )
    post_data = post_body.get("data") if isinstance(post_body, dict) else None
    if post_status != 200 or not isinstance(post_data, dict):
        raise RuntimeError(f"POST /backtests/runs failed: HTTP {post_status}")
    if (post_data.get("run") or {}).get("run_id") != run_id:
        raise RuntimeError("POST returned an unexpected run_id")

    query = urllib.parse.urlencode(
        {
            "skill_id": skill_id,
            "strategy_id": strategy_id,
            "symbol": symbol,
            "timeframe": timeframe,
        }
    )
    get_status, get_body, get_seconds, get_bytes = _request_json(
        "GET", f"{base_url}/backtests/runs/latest?{query}", headers
    )
    get_data = get_body.get("data") if isinstance(get_body, dict) else None
    get_meta = get_body.get("metadata") if isinstance(get_body, dict) else None
    if get_status != 200 or not isinstance(get_data, dict):
        raise RuntimeError(f"GET exact failed: HTTP {get_status}")

    fetched_run = get_data.get("run") or {}
    fetched_trades = get_data.get("trades") or []
    fetched_equity = get_data.get("equity_curve") or []
    exact_identity_match = (
        fetched_run.get("run_id") == run_id
        and fetched_run.get("skill_id") == skill_id
        and fetched_run.get("strategy_id") == strategy_id
        and fetched_run.get("symbol") == symbol
        and fetched_run.get("timeframe") == timeframe
        and isinstance(get_meta, dict)
        and get_meta.get("exact_match") is True
    )
    if not exact_identity_match:
        raise RuntimeError("GET exact identity mismatch")
    if len(fetched_trades) != 43 or len(fetched_equity) != 1001:
        raise RuntimeError(
            f"unexpected persisted counts: trades={len(fetched_trades)} equity={len(fetched_equity)}"
        )

    latency_gate_seconds = 20.0
    result = {
        "result": "PASS" if max(post_seconds, get_seconds) < latency_gate_seconds else "LATENCY_REGRESSION",
        "run_id": run_id,
        "skill_id": skill_id,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "provider": ready_data.get("provider"),
        "schema_version": (ready_data.get("schema_identity") or {}).get("schema_version"),
        "schema_identity_match": ready_data.get("schema_identity_match"),
        "ready_seconds": round(ready_seconds, 4),
        "post_seconds": round(post_seconds, 4),
        "get_exact_seconds": round(get_seconds, 4),
        "post_plus_get_seconds": round(post_seconds + get_seconds, 4),
        "post_response_bytes": post_bytes,
        "get_response_bytes": get_bytes,
        "trade_count": len(fetched_trades),
        "equity_point_count": len(fetched_equity),
        "exact_identity_match": exact_identity_match,
        "storage_only": True,
        "safe_for_trading": False,
        "promotion_eligible": False,
        "latency_gate_seconds_each": latency_gate_seconds,
    }
    print(json.dumps(result, sort_keys=True))
    if result["result"] != "PASS":
        raise RuntimeError("production backtest latency regression detected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
