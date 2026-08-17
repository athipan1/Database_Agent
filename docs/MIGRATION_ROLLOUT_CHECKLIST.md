# Migration rollout checklist

- Merge and deploy the Backtest persistence performance prerequisite before this change.
- Confirm `DATABASE_PROVIDER`, `DATABASE_URL`, TLS, and API settings are present in Railway.
- Railway pre-deploy must run `python -m scripts.apply_runtime_migrations` successfully before the API container starts.
- A migration failure blocks the new release; do not bypass the pre-deploy step.
- After migration, `/ready` must report `schema_identity_match=true`.
- Application startup must not emit table/column creation or ALTER logs.
- Keep live trading disabled independently of database deployment status; database readiness does not prove trading profitability.
