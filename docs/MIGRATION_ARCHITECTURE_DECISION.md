# Architecture decision: migrations before runtime

Database_Agent uses a migrations-first deployment boundary. Schema mutation is a release operation, not an API startup behavior.

The migration command is version-gated by the canonical schema identity. For an already-current release it exits without DDL. When migration is required, the identity marker is written last so a partial migration cannot advertise itself as current.

The API lifespan performs one read-only identity lookup and fails closed in production when the release identity does not match. Runtime scheduled jobs are operational only and do not create partitions or repair columns.

This isolates deployment-time locks and DDL latency from request serving and makes schema drift visible instead of silently self-healing it.
