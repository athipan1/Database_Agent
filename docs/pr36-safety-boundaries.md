# PR 36 Safety Boundaries

- `TRADING_MODE` must remain `PAPER` for observation workflows.
- `ALLOW_LIVE_TRADING` remains `false`.
- Database_Agent records promotion authority and observation evidence only.
- Database_Agent never places broker orders.
- Manager_Agent performs orchestration and reconciliation.
- Risk_Agent approval is required before any execution decision.
- Only Execution_Agent may call the broker.
- Cross-repository end-to-end tests use the simulator or Alpaca paper environment.
- Any stale version, missing credential, malformed payload, reconciliation mismatch,
  duplicate order, emergency halt, strategy drift, excessive drawdown, or expired
  evidence fails closed.
