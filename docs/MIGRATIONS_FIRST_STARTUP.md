# Migrations-first startup

Database_Agent production runtime must not repair or mutate schema during application startup.

## Deployment order

1. Build the release image.
2. Run `python -m scripts.apply_runtime_migrations` as the deployment migration step.
3. Start the API only after the migration command exits successfully.
4. API startup performs a read-only check of `database_agent_schema_metadata` and fails closed when the schema name, version, or hash differs from the release constants.

Railway reads `railway.json` and runs the migration command as `preDeployCommand`, so a failed migration prevents the new release from starting.

## Ownership

Schema-changing operations belong to the deployment migration command. This includes base table setup, compatibility columns, Backtest/Profit/Risk support tables, indexes, and price partitions. Runtime startup and the background scheduler are schema read-only.

The current migration runner wraps the repository's idempotent setup routines behind the release schema identity gate. If the target identity is already applied, it performs no DDL. Future schema changes must bump the canonical schema version/hash and move toward explicit versioned SQL migrations instead of adding new startup-time setup calls.

## Fail-closed rule

Do not make startup create missing tables or columns. A missing or stale schema marker means the deployment migration was not applied successfully, and production startup must fail rather than silently self-heal.
