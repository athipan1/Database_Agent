# Backtest Promotion Lifecycle

Database_Agent is the source of truth for promotion state. Backtest_Agent publishes evidence and may advance only through `ROBUSTNESS_PASSED`. Manager_Agent controls paper approval policy. Risk_Agent must approve every candidate before Execution_Agent. Only Execution_Agent may contact the broker.

## State machine

| From | Allowed next states |
|---|---|
| GENERATED | VALIDATED, REJECTED, FAILED |
| VALIDATED | OOS_PASSED, REJECTED, FAILED |
| OOS_PASSED | ROBUSTNESS_PASSED, REJECTED, FAILED |
| ROBUSTNESS_PASSED | APPROVED_FOR_PAPER, REJECTED, FAILED |
| APPROVED_FOR_PAPER | PAPER_OBSERVING, REVOKED, FAILED, EXPIRED |
| PAPER_OBSERVING | REVOKED, FAILED, EXPIRED |

`REJECTED`, `FAILED`, `EXPIRED`, and `REVOKED` are terminal. Skips, reversals, no-op transitions, stale versions, mismatched run IDs, expired evidence, and transitions after terminal states are rejected.

Every transition supplies `expected_state`, `expected_version`, `next_state`, `evidence_run_id`, `reason_code`, `reason`, and `correlation_id`. PostgreSQL uses a row lock plus compare-and-swap. SQLite tests use a process lock plus compare-and-swap. Database constraints enforce one version step per transition.

## Evidence gates

`GENERATED -> VALIDATED` requires a completed exact backtest run, passing skill result, canonical symbol, exact strategy/timeframe/engine/dataset identity, timezone-aware timestamps, supported validation profile, finite JSON, and fresh evidence.

`VALIDATED -> OOS_PASSED` additionally requires completed and passing nested walk-forward validation, enough independent windows, no overlapping test windows, eligible latest selection, exact selected strategy, all promotion gates, enabled statistical validation, passing Bonferroni-adjusted p-value, Probabilistic Sharpe, Deflated Sharpe, bootstrap lower bound, finite metrics, and no kill-switch event.

`OOS_PASSED -> ROBUSTNESS_PASSED` requires explicit parameter perturbation, fee, spread, slippage, liquidity, and drawdown stress evidence, a configured minimum scenario pass rate, finite metrics, and no catastrophic loss.

`ROBUSTNESS_PASSED -> APPROVED_FOR_PAPER` requires current non-expired evidence, the latest exact stored run, no newer failed or revoked promotion, matching policy evidence, and a named approver unless policy explicitly permits automatic approval. Production defaults remain fail closed.

## Idempotency

The transition ID is deterministic from promotion ID, expected version, expected state, next state, evidence run ID, and reason code. A completed transition stores its result snapshot. Replaying the identical request returns the original snapshot with `idempotent_replay=true`, creates no additional history row, and does not increment version, even after later transitions.

## API and authorization

All endpoints require `X-API-KEY` and return schema `backtest-promotion.v1`:

- `POST /backtests/promotions`
- `GET /backtests/promotions/{promotion_id}`
- `POST /backtests/promotions/{promotion_id}/transition`
- `GET /backtests/promotions/latest/exact`
- `GET /backtests/promotions/{promotion_id}/history`
- `POST /backtests/promotions/{promotion_id}/revoke`

Transitions to `APPROVED_FOR_PAPER`, `PAPER_OBSERVING`, `REVOKED`, or `EXPIRED` additionally require `X-PROMOTION-APPROVAL-KEY`. The value is compared in constant time against `BACKTEST_PROMOTION_APPROVAL_TOKEN`. The secret is never accepted in the request body and must never be logged. Missing configuration fails closed. This prevents Backtest_Agent from approving paper trading even when it has the ordinary Database_Agent service key.

Exact lookup first selects the newest promotion for account, symbol, strategy, and timeframe. Optional state, age, profile, engine, and dataset filters are checked afterward. This prevents an older approved row from hiding a newer failed, revoked, expired, or incompatible row.

## Migration and rollback

Migration `003_backtest_promotion_lifecycle.up.sql` is additive. Historical backtest runs are not auto-promoted because that would manufacture approval without an auditable request and correlation ID. The downgrade drops only the two promotion tables and does not alter historical backtests, trades, equity curves, positions, orders, fills, or ledger data.

## Policy defaults

```text
BACKTEST_PROMOTION_AUTO_APPROVE_PAPER=false
BACKTEST_PROMOTION_APPROVAL_REQUIRED=true
BACKTEST_PROMOTION_APPROVAL_TOKEN=<secret managed outside Git>
BACKTEST_PROMOTION_EVIDENCE_MAX_AGE_HOURS=168
BACKTEST_PROMOTION_MAX_AGE_HOURS=168
BACKTEST_PROMOTION_MIN_ROBUSTNESS_PASS_RATE=0.80
```

Paper-observation freshness, broker reconciliation, expiration automation, and fill-confirmed observation are extended in the paper rollout PR. A promotion must never be treated as executed or successfully observed before broker fill and reconciliation.

## Operations and incident recovery

On stale version, re-read the promotion and do not reuse assumed state. On database timeout or exact lookup failure, take no trade action. Revoke on duplicate order, broker reconciliation failure, emergency halt, strategy drift, superseded evidence, drawdown breach, or data-quality failure. Inspect transition history by version and correlation ID before retrying. Identical retries are safe.

Before paper rollout, verify migration upgrade and downgrade, exact lookup, manual approval, Risk approval, simulator fill, duplicate-order prevention, reconciliation, revocation, expiration, and scheduled workflow health. Keep automatic paper approval disabled until the soak test is stable.
