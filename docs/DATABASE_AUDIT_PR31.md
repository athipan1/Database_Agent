# Database_Agent promotion audit findings

1. No authoritative promotion schema, state machine, or transition history existed.
2. Existing exact backtest lookup was advisory and explicitly not safe for trading.
3. Exact backtest lookup did not include account identity.
4. Backtest tables and routes were assembled outside the modular application/startup path, creating route and schema ownership drift.
5. Generic exception handling returned raw exception text to clients.
6. Backtest write endpoints returned raw database exception text.
7. Backtest JSON serialization used permissive fallback conversion and could mask unsupported metadata types.
8. No expected-state and expected-version compare-and-swap protected promotion writes.
9. No deterministic transition ID or historical replay snapshot existed.
10. No terminal-state lock, expiration authority, or authenticated revoke flow existed.
11. No newest-record rule prevented an older passing record from hiding a newer failed or revoked record.
12. The canonical schema manifest excluded existing backtest evidence tables.
13. Backtest_Agent metadata does not yet persist the complete numeric statistical and robustness evidence required by the new OOS and robustness gates. Database_Agent therefore fails closed until the Backtest_Agent integration PR supplies those contracts.

This PR addresses findings 1 through 12. Finding 13 remains an explicit blocking cross-repository dependency rather than weakening evidence validation.
