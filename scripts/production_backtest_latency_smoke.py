"""Production-safe Backtest POST -> exact GET latency smoke for Database_Agent."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any


TRADE_COUNT = 43
EQUITY_POINT_COUNT = 1001
SYMBOL = "ZZTEST"
TIMEFRAME = "1d"
STRATEGY_ID = "storage-only-latency-v1"


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    payload: dict[str, Any] | None = None,
    timeout: int = 45,
) -> tuple[int, dict[str, Any], float, int]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            elapsed = time.perf_counter() - started
            parsed = json.loads(raw.decode("utf-8"))
            return int(response.status), parsed, elapsed, len(raw)
    except urllib.error.HTTPError as exc:
        elapsed = time.perf_counter() - started
        raw = exc.read()
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            parsed = {"detail": "non_json_error_response"}
        return int(exc.code), parsed, elapsed, len(raw)


def _require_environment() -> tuple[str, str, float]:
    base_url = os.environ.get("DATABASE_AGENT_URL", "").strip().rstrip("/")
    api_key = os.environ.get("DATABASE_AGENT_API_KEY", "").strip()
    threshold_raw = os.environ.get("LATENCY_GATE_SECONDS", "20").strip()

    missing = []
    if not base_url:
        missing.append("DATABASE_AGENT_URL")
    if not api_key:
        missing.append("DATABASE_AGENT_API_KEY")
    if missing:
        raise RuntimeError("Missing required configuration: " + ", ".join(missing))

    try:
        threshold = float(threshold_raw)
    except ValueError as exc:
        raise RuntimeError("LATENCY_GATE_SECONDS must be numeric") from exc
    if threshold <= 0 or threshold > 120:
        raise RuntimeError("LATENCY_GATE_SECONDS must be > 0 and <= 120")

    return base_url, api_key, threshold


def _build_payload(run_id: str, skill_id: str, github_run_id: str) -> dict[str, Any]:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trades = []
    for i in range(TRADE_COUNT):
        entry = start + timedelta(days=i * 2)
        trades.append(
            {
                "trade_id": f"{run_id}-trade-{i:03d}",
                "run_id": run_id,
                "symbol": SYMBOL,
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
                "metadata": {
                    "synthetic": True,
                    "storage_only": True,
                    "safe_for_trading": False,
                },
            }
        )

    equity_curve = [
        {
            "point_id": f"{run_id}-point-{i:04d}",
            "run_id": run_id,
            "timestamp": (start + timedelta(minutes=i)).isoformat(),
            "equity": 100000.0 + (i * 0.1),
            "drawdown": 0.0,
            "metadata": {"synthetic": True, "storage_only": True},
        }
        for i in range(EQUITY_POINT_COUNT)
    ]

    return {
        "run_id": run_id,
        "account_id": "diagnostic",
        "skill_id": skill_id,
        "strategy_id": STRATEGY_ID,
        "symbol": SYMBOL,
        "timeframe": TIMEFRAME,
        "start_time": start.isoformat(),
        "end_time": (start + timedelta(days=90)).isoformat(),
        "status": "completed",
        "engine_version": "production-latency-smoke-v1",
        "parameters": {"storage_only": True, "diagnostic": True},
        "metrics": {"synthetic": True, "latency_smoke": True},
        "source_agent": "production-backtest-latency-smoke",
        "metadata": {
            "synthetic": True,
            "latency_smoke": True,
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
            "total_trades": TRADE_COUNT,
            "score": 0.0,
            "reasons": [
                "synthetic production latency smoke; never eligible for trading"
            ],
            "metadata": {
                "synthetic": True,
                "storage_only": True,
                "safe_for_trading": False,
                "promotion_eligible": False,
            },
        },
    }


def main() -> int:
    try:
        base_url, api_key, threshold = _require_environment()
        github_run_id = os.environ.get("GITHUB_RUN_ID", "local")
        github_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        run_id = f"prod-backtest-latency-{github_run_id}-{github_attempt}"
        skill_id = f"latency-probe-{github_run_id}-{github_attempt}"
        correlation_id = f"prod-backtest-latency-{github_run_id}-{github_attempt}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-KEY": api_key,
            "X-Correlation-ID": correlation_id,
        }

        ready_status, ready_body, ready_seconds, _ = _request_json(
            "GET", f"{base_url}/ready", headers=headers
        )
        ready_data = ready_body.get("data") if isinstance(ready_body, dict) else None
        if ready_status != 200 or not isinstance(ready_data, dict):
            raise RuntimeError(f"Production readiness returned HTTP {ready_status}")
        if ready_data.get("ready") is not True:
            raise RuntimeError("Production readiness did not report ready=true")
        if ready_data.get("provider") != "supabase":
            raise RuntimeError("Production readiness provider is not supabase")
        if ready_data.get("schema_identity_match") is not True:
            raise RuntimeError("Production schema identity does not match")

        payload = _build_payload(run_id, skill_id, github_run_id)

        post_status, post_body, post_seconds, post_bytes = _request_json(
            "POST",
            f"{base_url}/backtests/runs",
            headers=headers,
            payload=payload,
        )
        post_data = post_body.get("data") if isinstance(post_body, dict) else None
        if post_status != 200 or not isinstance(post_data, dict):
            raise RuntimeError(f"POST /backtests/runs returned HTTP {post_status}")
        posted_run = post_data.get("run") or {}
        if posted_run.get("run_id") != run_id:
            raise RuntimeError("POST returned an unexpected run_id")

        query = urllib.parse.urlencode(
            {
                "skill_id": skill_id,
                "strategy_id": STRATEGY_ID,
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
            }
        )
        get_status, get_body, get_seconds, get_bytes = _request_json(
            "GET",
            f"{base_url}/backtests/runs/latest?{query}",
            headers=headers,
        )
        get_data = get_body.get("data") if isinstance(get_body, dict) else None
        get_meta = get_body.get("metadata") if isinstance(get_body, dict) else None
        if get_status != 200 or not isinstance(get_data, dict):
            raise RuntimeError(f"GET exact returned HTTP {get_status}")

        fetched_run = get_data.get("run") or {}
        fetched_trades = get_data.get("trades") or []
        fetched_equity = get_data.get("equity_curve") or []
        fetched_skill_result = get_data.get("skill_result") or {}
        run_metadata = fetched_run.get("metadata") or {}

        exact_identity_match = (
            fetched_run.get("run_id") == run_id
            and fetched_run.get("skill_id") == skill_id
            and fetched_run.get("strategy_id") == STRATEGY_ID
            and fetched_run.get("symbol") == SYMBOL
            and fetched_run.get("timeframe") == TIMEFRAME
            and isinstance(get_meta, dict)
            and get_meta.get("exact_match") is True
        )
        if not exact_identity_match:
            raise RuntimeError("GET exact identity mismatch")
        if len(fetched_trades) != TRADE_COUNT:
            raise RuntimeError(
                f"Unexpected persisted trade count: {len(fetched_trades)}"
            )
        if len(fetched_equity) != EQUITY_POINT_COUNT:
            raise RuntimeError(
                f"Unexpected persisted equity point count: {len(fetched_equity)}"
            )
        if fetched_skill_result.get("passed") is not False:
            raise RuntimeError("Synthetic skill result unexpectedly passed")
        if run_metadata.get("storage_only") is not True:
            raise RuntimeError("Synthetic run lost storage_only=true")
        if run_metadata.get("safe_for_trading") is not False:
            raise RuntimeError("Synthetic run is not marked safe_for_trading=false")
        if run_metadata.get("promotion_eligible") is not False:
            raise RuntimeError("Synthetic run is not marked promotion_eligible=false")

        combined_seconds = post_seconds + get_seconds
        latency_pass = (
            post_seconds < threshold
            and get_seconds < threshold
            and combined_seconds < threshold
        )
        schema_version = ready_data.get("schema_version")
        if schema_version is None and isinstance(ready_data.get("schema_identity"), dict):
            schema_version = ready_data["schema_identity"].get("schema_version")

        result = {
            "result": "PASS" if latency_pass else "LATENCY_REGRESSION",
            "run_id": run_id,
            "provider": ready_data.get("provider"),
            "schema_version": schema_version,
            "schema_identity_match": ready_data.get("schema_identity_match"),
            "ready_seconds": round(ready_seconds, 4),
            "post_seconds": round(post_seconds, 4),
            "get_exact_seconds": round(get_seconds, 4),
            "post_plus_get_seconds": round(combined_seconds, 4),
            "latency_gate_seconds": threshold,
            "post_response_bytes": post_bytes,
            "get_response_bytes": get_bytes,
            "trade_count": len(fetched_trades),
            "equity_point_count": len(fetched_equity),
            "exact_identity_match": exact_identity_match,
            "storage_only": True,
            "safe_for_trading": False,
            "promotion_eligible": False,
        }
        print(json.dumps(result, sort_keys=True))

        if not latency_pass:
            raise RuntimeError(
                "Latency regression: POST, exact GET, and POST+GET total must each "
                f"be < {threshold:.3f}s"
            )
        return 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "result": "FAIL",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                    "safe_for_trading": False,
                },
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
