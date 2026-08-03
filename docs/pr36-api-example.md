# Paper Observation API Example

```http
POST /backtests/promotion-observations/{promotion_id}
X-API-KEY: <database-service-key>
X-PROMOTION-APPROVAL-KEY: <paper-observation-key>
X-Correlation-ID: paper-observation-20260803-001
Content-Type: application/json
```

```json
{
  "expected_state": "PAPER_OBSERVING",
  "expected_version": 6,
  "observation_key": "account-1:AAPL:2026-08-03T12:00:00Z",
  "observed_at": "2026-08-03T12:00:00Z",
  "paper_drawdown_pct": 0.02,
  "reconciliation_ok": true,
  "duplicate_order_count": 0,
  "broker_order_count": 1,
  "database_order_count": 1,
  "filled_order_count": 1,
  "strategy_drift": false,
  "emergency_halt": false,
  "notes": ["broker and database order state reconciled"],
  "correlation_id": "paper-observation-20260803-001",
  "metadata": {
    "source": "manager-hourly-paper-reconciler"
  }
}
```

Retry the same reconciliation cycle with the same `observation_key`. A retry
returns the original observation and does not increment the promotion version.
