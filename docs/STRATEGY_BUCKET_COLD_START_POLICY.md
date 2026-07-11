# Strategy Bucket Cold-Start Policy

## Canonical precedence

When a broker snapshot does not contain `strategy_bucket`, Database_Agent restores metadata in this order:

1. An explicit valid bucket carried by the incoming position or order.
2. The canonical `(account_id, symbol)` row in `strategy_bucket_assignments`.
3. A valid deployment migration seed supplied through `STRATEGY_BUCKET_ASSIGNMENTS_JSON`.
4. `unassigned`, which must be reported as a semantic sync mismatch.

`unassigned` is never accepted as a canonical assignment.

## Current Alpaca Paper migration

The confirmed pre-existing holdings are migrated as:

- `ACGL=value_rebound`
- `ADBE=value_rebound`
- `BKNG=value_rebound`
- `CINF=value_rebound`

The versioned source of this mapping is maintained by Manager_Agent in
`config/strategy_bucket_assignments.v1.json` and passed to Database_Agent by the
hourly Compose stack.

## Safety requirements

- A held position or active order that remains `unassigned` makes broker sync unsafe.
- A canonical assignment mismatch must be visible in broker-sync diagnostics.
- Unknown bucket names are rejected to `unassigned` and are not seeded.
- Changes to the migration registry require manual review because bucket ownership
  affects portfolio capacity, exposure limits, and risk policy.
- This migration authorizes Alpaca Paper metadata recovery only. It does not authorize
  live trading or broker order mutation.
