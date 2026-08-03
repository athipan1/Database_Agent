# PR 36 Verification Matrix

| Requirement | Verification |
| --- | --- |
| First healthy observation enters `PAPER_OBSERVING` | SQLite service test |
| Healthy heartbeat is atomic | SQLite service test and PostgreSQL concurrency test |
| Identical retries do not increment version twice | Recovery and PostgreSQL concurrency tests |
| Crash after transition commit is repairable | Recovery test with one failed ledger write |
| Conflicting observers have one winner | PostgreSQL concurrency test |
| Emergency halt revokes | Parameterized service test |
| Duplicate order revokes | Parameterized service test |
| Broker/Database mismatch revokes | Parameterized service test |
| Strategy drift revokes | Parameterized service test |
| Drawdown limit revokes | Parameterized service test |
| Expired evidence becomes `EXPIRED` | Service test |
| Missing or invalid credentials fail closed | Route tests |
| OpenAPI exposes both authentication headers | Route contract test |
| Migration supports up/down/up | PostgreSQL workflow gate |
| Ruff, MyPy, Bandit, pip-audit, SBOM | Observation quality workflow |

Live trading is outside this PR and remains disabled. Manager_Agent integration and
cross-repository paper reconciliation are completed only after this Database_Agent
contract is merged and green.
