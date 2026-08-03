# PR 36 Rollout

1. Merge Database_Agent observation contract only after all PR checks pass.
2. Verify the post-merge main-branch workflows.
3. Add the Manager_Agent observer and reconciler against the versioned API.
4. Run cross-repository simulator E2E scenarios.
5. Run Alpaca paper observation as a 24 to 72 hour soak test.
6. Keep live trading disabled until observation, reconciliation, halt, and rollback
   drills have all passed.

Rollback is the `004_backtest_promotion_observations.down.sql` migration plus a
Manager_Agent deployment that stops sending observation requests. Promotion
states already moved to terminal states are preserved as audit evidence and are
not automatically reopened.
