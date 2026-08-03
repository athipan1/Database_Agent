# PR 32 audit: promotion idempotency and CI hardening

PR 31 established the authoritative promotion schema, ordered state machine, authenticated API, deterministic transition IDs, exact lookup, and fail-closed evidence gates. This audit identified the remaining Database_Agent risks before Backtest_Agent and Manager_Agent integration.

## Findings

1. Concurrent transition tests used SQLite and mocked PostgreSQL recovery paths, but did not prove behavior against a real PostgreSQL connection pool and row locks.
2. Identical concurrent retries needed an integration test proving exactly one version increment, one history row, and deterministic replay snapshots for every loser.
3. Conflicting transitions from the same expected state and version needed an integration test proving one winner and one fail-closed loser.
4. Existing pull-request checks did not provide a dedicated promotion branch-coverage threshold.
5. Ruff, MyPy, Bandit, pip-audit, OpenAPI validation, and SBOM generation were not blocking promotion checks.
6. Existing workflows did not provide a promotion-specific `push` gate for `main`, so a squash merge could not be independently verified by a post-merge workflow.
7. Container checks built and scanned the image, but did not prove that the production image runs as non-root and reaches the declared health check against PostgreSQL and Redis.
8. Promotion API routes were not validated as a complete OpenAPI contract, including both service and privileged approval headers.
9. Quality-tool configuration was absent from `pyproject.toml`, leaving static analysis and coverage behavior implicit.
10. Quality evidence was not retained as machine-readable coverage and CycloneDX artifacts.

## Remediation in this PR

- Add real PostgreSQL identical-retry and conflicting-transition concurrency tests.
- Add promotion OpenAPI validation and strict request-contract assertions.
- Add branch coverage of at least 90 percent for promotion identity, concurrency, exact lookup, and strict model modules.
- Add blocking Ruff, MyPy, Bandit, pip-audit, migration, container-health, SBOM, and OpenAPI gates.
- Pin GitHub Actions to immutable commit SHAs.
- Run promotion gates on pull requests, manual dispatch, and pushes to `main`.
- Verify the production image declares user `app`, declares a health check, and becomes healthy against real service containers.
- Retain coverage XML and CycloneDX SBOM as workflow artifacts.

No broker, Risk_Agent, Execution_Agent, or approval policy behavior is changed by this PR.
