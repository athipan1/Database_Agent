# Schema identity gate

Runtime compares `schema_name`, `schema_version`, and `schema_sha256` in `database_agent_schema_metadata` with the release constants. The check is SELECT-only. A mismatch instructs operators to run deployment migrations and production startup fails closed.
