# Migrations-first validation matrix

| Check | Expected |
| --- | --- |
| Current schema migration rerun | exits successfully without DDL |
| Missing/stale schema identity | deployment migration runs before API start |
| Migration failure | new deployment does not start |
| Production API startup | read-only schema identity check only |
| Runtime scheduler | ingestion + stats only; no partition/schema mutation |
| `/ready` | schema identity match required for managed Supabase primary |
| Post-migration boot benchmark | under 15 seconds in CI; compare real Supabase boot with previous ~44 second schema setup |
