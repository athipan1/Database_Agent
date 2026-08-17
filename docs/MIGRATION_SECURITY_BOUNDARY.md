# Migration security boundary

The API process must not require schema-owner behavior during startup. Schema changes are isolated to the pre-deploy migration command. This reduces the window in which application startup can take schema locks or silently repair drift.

Migration credentials remain supplied through the deployment environment; secrets are not written to repository files or logs.
