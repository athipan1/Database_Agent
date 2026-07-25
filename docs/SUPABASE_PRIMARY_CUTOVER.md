# Supabase Primary Cutover Runbook

This runbook moves Database_Agent's PostgreSQL source of truth from Railway
PostgreSQL to the `Database_Agent_Trading` Supabase project.

## Non-negotiable rules

1. Freeze all trading writes before copying data.
2. Never allow Railway PostgreSQL and Supabase PostgreSQL to accept writes at
   the same time.
3. Do not enable automatic database fallback. It can create split-brain orders,
   fills, and lifecycle transitions.
4. Keep all connection URLs in Railway or GitHub environment secrets only.
5. Use Supavisor Session Pooler on port 5432 for the long-running FastAPI pool.
6. Keep Railway PostgreSQL available as a rollback snapshot until the cutover
   has been observed for the agreed rollback window.

## Merge order

1. Merge PR 1, managed Supabase PostgreSQL connection support.
2. Merge PR 2, canonical schema and verification.
3. Merge PR 3, readiness and cutover controls.

Deploy the merged code before changing the primary connection variables.

## Required secrets

For the manual GitHub cutover workflow:

- `RAILWAY_DATABASE_URL`: source Railway PostgreSQL connection string
- `SUPABASE_DATABASE_URL`: target Supabase Session Pooler connection string

For the deployed API smoke workflow:

- `DATABASE_AGENT_URL`: public Database_Agent URL
- `DATABASE_AGENT_API_KEY`: Database_Agent API key

For Railway Database_Agent after cutover:

```ini
DATABASE_PROVIDER=supabase
DATABASE_URL=<Supabase Session Pooler URL on port 5432>
DATABASE_SSL_MODE=require
DATABASE_CREATE_IF_MISSING=false
DATABASE_POOL_MIN=1
DATABASE_POOL_MAX=20
DATABASE_CONNECT_TIMEOUT_SECONDS=10
DATABASE_CUTOVER_GUARD_ENABLED=true
DATABASE_EXPECTED_PROVIDER=supabase
DATABASE_REQUIRE_SCHEMA_IDENTITY=true
DATABASE_DEV_MODE=false
```

Do not use a publishable key, anon key, service role key, or Supabase REST URL as
`DATABASE_URL`. This variable requires a PostgreSQL connection string and the
database password.

## Phase 1: Preflight

1. Confirm PR 1, PR 2, and PR 3 checks are green.
2. Confirm the target schema verifier succeeds.
3. Confirm Supabase has the expected schema version and fingerprint.
4. Confirm the target core tables are empty.
5. Confirm a fresh Railway PostgreSQL backup or snapshot exists.
6. Record the current Railway deployment revision and current variables without
   copying secret values into tickets or logs.

## Phase 2: Freeze writes

1. Enable the system emergency halt.
2. Disable the hourly Manager_Agent workflow.
3. Wait for in-flight execution jobs to finish or move to a known terminal state.
4. Confirm no pending execution job is being claimed.
5. Stop Database_Agent traffic or place the service in maintenance mode.

The data copy must begin only after the write freeze is verified.

## Phase 3: Copy and verify

Run the `Supabase Primary Cutover` workflow with:

- mode: `migrate`
- confirmation: `MIGRATE_TO_SUPABASE`

The workflow performs these guarded operations:

1. Verify target schema, TLS, RLS, privileges, and schema identity.
2. Refuse to continue when target core tables are not empty.
3. Create a data-only custom-format dump from Railway.
4. Restore into the pre-created Supabase schema in one transaction.
5. Synchronize serial sequences.
6. Compare source and target counts for every logical table.
7. Perform a non-trading write that is rolled back and confirm no row persists.

Do not continue when any count differs.

## Phase 4: Switch Railway Database_Agent

1. Replace `DATABASE_URL` with the Supabase Session Pooler URL.
2. Set the cutover variables shown above.
3. Redeploy Database_Agent.
4. Confirm `GET /ready` returns HTTP 200.
5. Confirm `/ready` reports:
   - provider `supabase`
   - TLS enabled
   - schema identity matched
   - no readiness failure reasons
6. Confirm `GET /health` reports a connected database.
7. Run the `Database Primary API Smoke` workflow with expected provider
   `supabase`. It creates and reads one `ZZTEST` signal marked
   `synthetic=true` and `safe_for_trading=false`.
8. Run one read-only Manager_Agent integration check.
9. Re-enable the hourly workflow in simulator or paper mode first.

## Observation window

During the rollback window, monitor:

- Database_Agent readiness and request errors
- connection pool exhaustion
- execution-job idempotency
- duplicate orders or fills
- stale position versions
- Supabase database logs and resource usage

Keep Railway PostgreSQL read-only and do not point any agent at it.

## Rollback

Rollback is safe only while both databases have identical committed data.

1. Freeze all writes again.
2. Disable hourly and manual trading workflows.
3. Determine whether Supabase accepted any writes after cutover.
4. If Supabase accepted writes, copy and verify those changes back to Railway
   before switching. Do not discard them.
5. Set Railway variables back to the Railway PostgreSQL URL.
6. Set:

```ini
DATABASE_PROVIDER=postgres
DATABASE_EXPECTED_PROVIDER=postgres
DATABASE_CUTOVER_GUARD_ENABLED=true
DATABASE_REQUIRE_SCHEMA_IDENTITY=true
DATABASE_CREATE_IF_MISSING=false
```

7. Redeploy and require `/ready` HTTP 200.
8. Compare counts again before restoring trading traffic.

## What this cutover does not solve

Supabase becoming primary removes Railway PostgreSQL as the database dependency.
It does not keep the API online if the Railway application service itself is
down. A secondary Database_Agent deployment and endpoint failover are separate
high-availability work.
