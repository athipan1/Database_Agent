# Backtest Promotion Paper Observation

Database_Agent is the source of truth for the paper-observation lifecycle.
Only promotions in `APPROVED_FOR_PAPER` or `PAPER_OBSERVING` may be observed.

## State flow

- A healthy first observation transitions `APPROVED_FOR_PAPER` to `PAPER_OBSERVING`.
- A healthy later observation records an atomic heartbeat and increments the promotion version.
- An observation at or after `expires_at` transitions the promotion to `EXPIRED`.
- Emergency halt, duplicate orders, broker/database reconciliation mismatch,
  strategy drift, or excessive paper drawdown transitions the promotion to `REVOKED`.

`REVOKED` and `EXPIRED` are terminal and are never safe for trading.

## Idempotency

Clients must provide a stable `observation_key` for each reconciliation cycle.
The observation ID is a SHA-256 digest of `promotion_id` and `observation_key`.
Retries return the original result and must not create another ledger row or
increment the promotion version again.

State transitions use the existing deterministic transition ledger. If a
process stops after the promotion transition commits but before the observation
ledger is written, retrying the same request replays the transition and repairs
the missing observation record.

## Concurrency

Heartbeat updates use optimistic compare-and-swap on state and version. The
promotion update and observation insert commit in one database transaction.
Concurrent identical requests return the single committed observation.
Conflicting requests with the same expected version allow one winner and reject
the stale request.

## Authentication

- `X-API-KEY` authenticates the Database_Agent service caller.
- `X-PROMOTION-APPROVAL-KEY` authorizes paper observation writes.
- Missing server-side approval-token configuration fails closed.
- Observation reads require `X-API-KEY`.

## Safety

This lifecycle is paper-only. It does not place broker orders. Manager_Agent
must reconcile broker and Database order state, Risk_Agent must approve trading
decisions, and only Execution_Agent may call the broker. Live trading remains
disabled.
