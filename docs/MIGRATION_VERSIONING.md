# Migration versioning

Any future schema change must update the canonical schema identity and include the deployment migration needed to reach it. Changing runtime code alone must not silently alter database schema at startup.
