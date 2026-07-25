"""Secret-safe deployed Database_Agent readiness and write/read smoke test."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            parsed = json.loads(response.read().decode("utf-8"))
            return int(response.status), parsed
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(response_body)
        except json.JSONDecodeError:
            parsed = {"detail": "non_json_error_response"}
        return int(exc.code), parsed


def _require_environment() -> tuple[str, str, str]:
    base_url = os.environ.get("DATABASE_AGENT_URL", "").strip().rstrip("/")
    api_key = os.environ.get("DATABASE_AGENT_API_KEY", "").strip()
    expected_provider = os.environ.get(
        "DATABASE_EXPECTED_PROVIDER", "supabase"
    ).strip().lower()
    missing = []
    if not base_url:
        missing.append("DATABASE_AGENT_URL")
    if not api_key:
        missing.append("DATABASE_AGENT_API_KEY")
    if missing:
        raise RuntimeError("Missing required configuration: " + ", ".join(missing))
    if expected_provider not in {"postgres", "supabase"}:
        raise RuntimeError("DATABASE_EXPECTED_PROVIDER must be postgres or supabase")
    return base_url, api_key, expected_provider


def _wait_until_ready(
    base_url: str,
    headers: dict[str, str],
    expected_provider: str,
    *,
    attempts: int = 12,
    interval_seconds: int = 10,
) -> dict[str, Any]:
    last_status = None
    for attempt in range(1, attempts + 1):
        status_code, body = _request_json(
            "GET",
            f"{base_url}/ready",
            headers=headers,
        )
        last_status = status_code
        data = body.get("data") if isinstance(body, dict) else None
        provider_ready = (
            isinstance(data, dict)
            and data.get("ready") is True
            and data.get("database_connection") == "connected"
            and data.get("provider") == expected_provider
            and data.get("schema_identity_match") is True
        )
        tls_ready = expected_provider != "supabase" or data.get("tls") is True
        if status_code == 200 and provider_ready and tls_ready:
            return data
        if attempt < attempts:
            time.sleep(interval_seconds)
    raise RuntimeError(
        f"Database_Agent readiness failed after {attempts} attempts "
        f"(last HTTP status: {last_status})"
    )


def main() -> int:
    try:
        base_url, api_key, expected_provider = _require_environment()
        run_id = os.environ.get("GITHUB_RUN_ID") or datetime.now(
            timezone.utc
        ).strftime("%Y%m%d%H%M%S")
        suffix = uuid.uuid4().hex[:8]
        correlation_id = f"database-primary-smoke-{run_id}-{suffix}"
        signal_id = f"synthetic-database-primary-smoke-{run_id}-{suffix}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-KEY": api_key,
            "X-Correlation-ID": correlation_id,
        }

        readiness = _wait_until_ready(
            base_url,
            headers,
            expected_provider,
        )
        payload = {
            "signal_id": signal_id,
            "account_id": "diagnostic",
            "symbol": "ZZTEST",
            "source_agent": "database-primary-api-smoke",
            "candidate_score": 0.0,
            "technical_score": 0.0,
            "fundamental_score": 0.0,
            "final_verdict": "SYNTHETIC_TEST_ONLY",
            "market_regime": "TEST",
            "metadata": {
                "synthetic": True,
                "safe_for_trading": False,
                "expected_provider": expected_provider,
                "github_run_id": run_id,
            },
        }
        create_status, create_body = _request_json(
            "POST",
            f"{base_url}/history/signals",
            headers=headers,
            payload=payload,
        )
        created = create_body.get("data") if isinstance(create_body, dict) else None
        if create_status != 200 or not isinstance(created, dict):
            raise RuntimeError(
                f"Synthetic row write failed with HTTP status {create_status}"
            )
        if created.get("signal_id") != signal_id:
            raise RuntimeError("Write response returned an unexpected signal_id")

        query = urllib.parse.urlencode({"symbol": "ZZTEST", "limit": 100})
        read_status, read_body = _request_json(
            "GET",
            f"{base_url}/history/signals?{query}",
            headers=headers,
        )
        rows = read_body.get("data") if isinstance(read_body, dict) else None
        if read_status != 200 or not isinstance(rows, list):
            raise RuntimeError(
                f"Synthetic row read failed with HTTP status {read_status}"
            )
        stored = next(
            (
                row
                for row in rows
                if isinstance(row, dict) and row.get("signal_id") == signal_id
            ),
            None,
        )
        if stored is None:
            raise RuntimeError("Synthetic row was not found after the write")
        metadata = stored.get("metadata") or {}
        if not isinstance(metadata, dict) or metadata.get("synthetic") is not True:
            raise RuntimeError("Stored metadata does not identify a synthetic row")
        if metadata.get("safe_for_trading") is not False:
            raise RuntimeError("Synthetic row was not marked unsafe for trading")

        print(
            json.dumps(
                {
                    "result": "PASS",
                    "provider": readiness.get("provider"),
                    "tls": readiness.get("tls"),
                    "schema_identity_match": readiness.get(
                        "schema_identity_match"
                    ),
                    "signal_id": signal_id,
                    "symbol": stored.get("symbol"),
                    "synthetic": True,
                    "safe_for_trading": False,
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "result": "FAIL",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
