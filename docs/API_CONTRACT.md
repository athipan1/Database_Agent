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

## Position Price-Watermark Contract

Every canonical position returned by `Database_Agent` includes:

```json
{
  "account_id": 1,
  "symbol": "AAPL",
  "quantity": 10,
  "average_cost": 100.0,
  "current_market_price": 110.0,
  "market_value": 1100.0,
  "highest_price_since_entry": 125.0,
  "strategy_bucket": "unassigned"
}
```

The field is exposed by every endpoint that returns canonical position state:

```http
GET /accounts/{account_id}/positions
GET /broker-sync/status
```

For `/broker-sync/status`, the field is present under both
`data.database.positions[*]` and the enriched
`data.latest_snapshot.positions[*]` payload.

`highest_price_since_entry` has the following lifecycle contract:

1. A newly opened position starts at its entry price (`average_cost`).
2. When a newer market price is received, the stored value changes only when
   that price is greater than the existing value. A lower price never reduces
   the watermark.
3. Broker synchronization updates an existing open-position row in place. It
   preserves `position_id`, the watermark, version, and profit lifecycle state.
4. When a position disappears from the broker snapshot, its row is deleted.
   A later position for the same symbol is a new lifecycle and starts from the
   new entry price.
5. `Database_Agent` is the source of truth. Incoming clients must not overwrite
   this field directly.
6. Existing rows are migrated additively. A missing watermark is backfilled to
   the greater of the stored entry price and the latest stored market price,
   which is the safest known lower bound when historical ticks are unavailable.

`Manager_Agent` may read this value after the Database_Agent migration is
successfully deployed. `Profit_Agent` must not derive or persist a competing
watermark.

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

## Profit lifecycle and decision state

`Database_Agent` owns the open-position lifecycle and exposes authenticated
endpoints using schema `profit-lifecycle.v1`:

```http
GET  /accounts/{account_id}/profit-lifecycles
GET  /accounts/{account_id}/profit-lifecycles/{position_id}
POST /accounts/{account_id}/profit-decisions/reserve
GET  /accounts/{account_id}/profit-decisions/{decision_id}
POST /accounts/{account_id}/profit-decisions/{decision_id}/transition
```

An open position returns a stable external identity such as
`account-1:position-42`, `position_version`, both target flags, total exited
quantity, remaining quantity, peak price, and last decision fields. Broker sync
preserves these values while the position remains open. A position removed from
a broker snapshot is closed by deleting its active row; reopening later creates
a new `position_id`.

Decision reservation locks the position row, verifies the optimistic version,
and inserts `PROPOSED` under the unique key
`(account_id, position_id, decision_id)`. Repeating the same reservation returns
the existing record with `duplicate=true`; it never creates a second execution
identity.

Allowed state transitions are:

```text
PROPOSED -> RISK_APPROVED -> EXECUTION_PENDING -> EXECUTED
PROPOSED -> REJECTED | FAILED | EXPIRED
RISK_APPROVED -> FAILED | EXPIRED
EXECUTION_PENDING -> FAILED | EXPIRED
```

Target flags, `total_exited_quantity`, and `position_version` change only on an
`EXECUTED` transition with a positive broker-confirmed fill quantity. Replaying
the same transition is idempotent. A stale version or invalid transition returns
HTTP `409`.

PostgreSQL deployment migration:

```text
migrations/002_profit_lifecycle.up.sql
migrations/002_profit_lifecycle.down.sql
```

Existing positions are backfilled to version 1, false target flags, and zero
exited quantity. The downgrade removes the decision table and additive columns;
export audit records before rollback if they must be retained.

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
