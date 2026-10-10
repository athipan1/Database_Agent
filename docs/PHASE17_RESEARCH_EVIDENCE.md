# Phase 17: Research-Only Evidence Store (Database_Agent)

A new **isolated** authenticated research store in the existing Railway PostgreSQL primary. It never writes `backtest_runs`, `backtest_promotions`, orders, risk approvals, or broker records. No new services, repos, workflows or separate databases are needed.

## Deployment

The existing Railway predeploy command `python -m scripts.apply_runtime_migrations` adds the indexed `research_evidence` table and updates `database_agent_schema_metadata` to release identity `2026-10-10.1`. App startup remains SELECT-only and refuses mismatched schema. Do not deploy without a successful predeploy migration. Rollback requires a reviewed schema-identity-compatible release; the additive table can remain after application rollback. Avoid deleting research evidence.

Existing production `DATABASE_URL`, `DATABASE_AGENT_API_KEY` and Railway domain remain unchanged.

## API and immutable identity

- `POST /research/evidence`: `201` on new insert or exact replay, `metadata.idempotent_replay=true` on replay. Requires `X-API-KEY`. A hash collision or mismatched stored data returns `409`. Invalid source/flags/digest/phase evidence returns `422`.
- `GET /research/evidence/{evidence_id}`: authenticated read of exactly one immutable evidence report; returns `404` when absent.

The document schema `phase17-research-evidence.v1` binds source, symbol, profile, dataset fingerprint, SHA-256 of a complete original per-symbol research artifact and bounded summaries for Phase 13, 14, 15, and 16. Canonical JSON serialization sorts keys with compact separators, UTF-8, `allow_nan=false`; `evidence_id` is SHA-256 of all fields *except* `evidence_id`. Neither an authenticated writer nor an exact checksum proves third-party provenance.

All documents must assert `research_only=true`, `diagnostic_only=true`, and **all** of `used_for_selection`, `promotion_allowed`, `execution_allowed`, `sealed_holdout_opened` false. Each Phase report must likewise deny promotion, execution and opening holdout. These hardcoded API flags never change production trading authorization.

## Acceptance and verification

Check migration + schema identity; PostgreSQL service starts; authenticated research write; identical replay; GET round-trip; conflicting write; unauthenticated POST/GET; actual report SHA-256; no promotion/order side effects. No synthetic fixture constitutes a real profitability proof. Backtest rollout may enable *separate* `BACKTEST_RESEARCH_EVIDENCE_PUBLISH=true` only after Database_Agent is deployed and healthy.
