# CI entrypoint

Pull-request validation runs the migration command against PostgreSQL, reruns it to verify idempotency, then starts the API and measures readiness latency.
