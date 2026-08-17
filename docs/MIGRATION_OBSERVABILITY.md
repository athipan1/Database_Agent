# Migration observability

Deployment logs should distinguish `schema already current`, `applying migration target`, and `migration target verified`. Runtime logs should contain `Database schema identity verified` and must not contain ensure-column or schema creation messages during normal production boot.
