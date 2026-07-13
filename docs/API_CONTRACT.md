# Database_Agent API Contract

This document defines the baseline API contract for `Database_Agent` as part of the multi-agent trading system.

`Database_Agent` is the system of record for balances, positions, orders, fills, execution jobs, risk approvals, broker sync state, trade plans, and audit data.

## Standard Headers

Every internal request should include:

```http
Content-Type: application/json
X-Correlation-ID: <uuid>
X-API-KEY: <database-agent-api-key>
```

## Correlation ID

`Database_Agent` accepts `X-Correlation-ID` and echoes it back in the response header.

Rules:

1. `Manager_Agent` creates the correlation ID at workflow start.
2. `Database_Agent` preserves the same correlation ID across persistence and audit operations.
3. If a request arrives without `X-Correlation-ID`, `Database_Agent` may generate one.

## Standard Response Envelope

Every response should use this envelope:

```json
{
  "status": "success",
  "agent_type": "database",
  "version": "1.1.0",
  "schema_version": "1.0",
  "timestamp": "2026-07-04T00:00:00Z",
  "correlation_id": "00000000-0000-0000-0000-000000000000",
  "data": {},
  "metadata": {},
  "error": null,
  "confidence_score": null
}
```

## Error Response

```json
{
  "status": "error",
  "agent_type": "database",
  "version": "1.1.0",
  "schema_version": "1.0",
  "timestamp": "2026-07-04T00:00:00Z",
  "correlation_id": "00000000-0000-0000-0000-000000000000",
  "data": null,
  "metadata": {},
  "error": {
    "code": "404",
    "message": "Record not found",
    "retryable": false
  },
  "confidence_score": null
}
```

## Required Operational Endpoints

The operational endpoint set is:

```http
GET /health
GET /ready
GET /version
```

Current baseline:

- `/health` reports database connectivity, dev mode, trading mode, and emergency halt state.
- `/ready` reports runtime readiness, trading mode, dev mode, emergency halt state, API key configuration, and live/dev-mode violations.
- `/version` reports agent version, schema version, and API contract metadata.

## Exact Backtest Evidence Lookup

`GET /backtests/runs/latest` returns the newest Backtest run matching one exact
execution-evidence identity. All four query parameters are required:

- `skill_id`
- `strategy_id`
- `symbol` (normalized to uppercase)
- `timeframe`

Example:

```http
GET /backtests/runs/latest?skill_id=hourly-sma-crossover&strategy_id=hourly-sma-crossover&symbol=AAPL&timeframe=1d
X-API-KEY: <database-agent-api-key>
```

The endpoint returns `404` when that exact tuple has no run and never falls
back to another symbol, strategy, or timeframe. It returns the newest run even
when that run failed, so callers cannot silently reuse an older passing result.
The response is evidence only: Manager/Risk must still verify pass status and
freshness before authorizing execution.

## Safety Rules

1. `DATABASE_DEV_MODE=true` must not be used with `TRADING_MODE=LIVE`.
2. `DATABASE_AGENT_API_KEY` should be required outside dev mode.
3. Dev fallback behavior should stay out of live mode.
4. Database writes should remain auditable with correlation IDs.
5. `Database_Agent` stores state and audit data; orchestration remains in `Manager_Agent`.

## Rollout Plan

1. Add schema fields to `StandardAgentResponse`.
2. Add runtime `/ready` and `/version` endpoints.
3. Ensure response helpers include `schema_version`, `correlation_id`, and `metadata`.
4. Add API contract tests.
5. Expand contract tests around write endpoints such as risk approvals, execution jobs, fills, and broker sync.
