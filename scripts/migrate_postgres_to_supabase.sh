#!/usr/bin/env bash
set -euo pipefail

: "${SOURCE_DATABASE_URL:?SOURCE_DATABASE_URL is required}"
: "${TARGET_DATABASE_URL:?TARGET_DATABASE_URL is required}"
: "${CONFIRM_MIGRATION:?CONFIRM_MIGRATION is required}"

if [[ "${CONFIRM_MIGRATION}" != "MIGRATE_TO_SUPABASE" ]]; then
  echo "Refusing migration: CONFIRM_MIGRATION must equal MIGRATE_TO_SUPABASE" >&2
  exit 2
fi

for command_name in pg_dump pg_restore psql; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${command_name}" >&2
    exit 3
  fi
done

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT
archive="${workdir}/database-agent-data.dump"

# The target must remain empty before the one-way restore unless the operator
# deliberately enables a reviewed exception. This prevents accidental merges.
target_rows="$({
  psql "${TARGET_DATABASE_URL}" \
    --no-psqlrc --tuples-only --no-align --set ON_ERROR_STOP=1 \
    --command "
      select
        (select count(*) from public.accounts) +
        (select count(*) from public.positions) +
        (select count(*) from public.orders) +
        (select count(*) from public.fills) +
        (select count(*) from public.execution_jobs) +
        (select count(*) from public.risk_approvals) +
        (select count(*) from public.profit_decisions) +
        (select count(*) from public.signal_history) +
        (select count(*) from public.performance_metrics)
    "
} | tr -d '[:space:]')"

if [[ "${target_rows}" != "0" && "${ALLOW_NONEMPTY_TARGET:-false}" != "true" ]]; then
  echo "Refusing migration: target core tables are not empty" >&2
  exit 4
fi

echo "Creating source data archive without schema or ownership metadata."
pg_dump \
  --dbname "${SOURCE_DATABASE_URL}" \
  --format custom \
  --data-only \
  --no-owner \
  --no-privileges \
  --file "${archive}"

echo "Restoring data into the pre-created Supabase schema."
pg_restore \
  --dbname "${TARGET_DATABASE_URL}" \
  --data-only \
  --no-owner \
  --no-privileges \
  --single-transaction \
  --exit-on-error \
  "${archive}"

echo "Synchronizing generated sequences."
psql "${TARGET_DATABASE_URL}" \
  --no-psqlrc \
  --set ON_ERROR_STOP=1 \
  --file "$(dirname "$0")/sync_postgres_sequences.sql"

echo "Data restore completed. Run compare_primary_counts.py before changing Railway variables."
