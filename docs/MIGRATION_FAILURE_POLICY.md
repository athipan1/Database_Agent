# Migration failure policy

A schema migration is part of deployment safety. If the pre-deploy migration command fails, the release must not start. Operators should fix or roll back the migration rather than enabling runtime schema repair.

Production startup treats a missing or mismatched schema identity as fatal. `DATABASE_DEV_MODE` remains the only development fallback and must not be enabled for live trading.
