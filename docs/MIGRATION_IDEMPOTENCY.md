# Migration idempotency

The migration entrypoint checks the canonical schema identity first. When the release schema is already applied, it returns successfully without calling schema setup or partition creation. This keeps repeated deployment attempts safe and cheap.
