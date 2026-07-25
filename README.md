# Database Agent

The Database Agent is a FastAPI-based service responsible for managing all database interactions for the trading system. It provides a secure and transactional API for logging decisions, tracking outcomes, and serving data to other agents like the `LearningAgent`.

## Core Responsibilities

1. **Decision Logging**: Records every `buy`/`sell`/`hold` decision from all trading agents, linking them with a unique `correlation_id` for end-to-end traceability.
2. **Outcome Tracking**: Stores the actual results of trades, such as profit/loss and drawdown over various time horizons.
3. **Data Source for Learning**: Acts as the single source of truth for the `LearningAgent` to evaluate agent performance and adjust agent weights.
4. **System Auditing**: Provides an audit trail that can be traced by `correlation_id`.

## Profit decision lifecycle

Database Agent is the source of truth for profit-taking idempotency. It keeps a
stable `position_id` during broker sync, an optimistic `position_version`,
executed target flags, total exited quantity, and a uniquely reserved decision
record. Manager must reserve an advisory decision here before Risk approval and
must transition it to `EXECUTED` only after a confirmed fill.

The PostgreSQL upgrade and rollback scripts are:

```text
migrations/002_profit_lifecycle.up.sql
migrations/002_profit_lifecycle.down.sql
```

## Database providers

The repository supports two PostgreSQL deployment modes without changing its
repository or API contracts.

### Local or self-managed PostgreSQL

```ini
DATABASE_PROVIDER=postgres
DATABASE_URL=postgresql://trading_user:password@db:5432/trading_db
DATABASE_SSL_MODE=prefer
DATABASE_CREATE_IF_MISSING=true
```

This preserves the existing behavior that may create the configured database
through the maintenance `postgres` database.

### Supabase PostgreSQL primary

```ini
DATABASE_PROVIDER=supabase
DATABASE_URL=<server-side Supabase Session Pooler connection string>
DATABASE_SSL_MODE=require
DATABASE_CREATE_IF_MISSING=false
DATABASE_POOL_MIN=1
DATABASE_POOL_MAX=20
DATABASE_CONNECT_TIMEOUT_SECONDS=5
```

Use the Session Pooler connection string intended for persistent server
processes. Store the database password only in Railway Variables or GitHub
Secrets. Never put it in the repository or frontend configuration.

In managed mode Database Agent:

- does not connect to a maintenance database
- never attempts `CREATE DATABASE`
- requires TLS for Supabase
- validates the pool with `SELECT 1` before serving requests
- keeps the existing `TradingDB` repository interface

---

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### 1. Set Up Environment Variables

```bash
cp .env.example .env
```

For local Docker, customize:

```ini
POSTGRES_USER=trading_user
POSTGRES_PASSWORD=your_super_secret_password
POSTGRES_DB=trading_db
POSTGRES_HOST=db
POSTGRES_PORT=5432
DATABASE_PROVIDER=postgres
DATABASE_URL=postgresql://trading_user:your_super_secret_password@db:5432/trading_db
DATABASE_AGENT_API_KEY=your_generated_api_key_here
```

Generate the API key with `openssl rand -hex 32` or an equivalent secure random generator.

### 2. Build and Run the Service

```bash
sudo docker compose up --build -d
```

### 3. Verify the Service

```bash
sudo docker compose ps
sudo docker compose logs -f api
curl http://localhost:8000/health
```

The health response uses the standard Database Agent envelope and reports whether the active PostgreSQL connection is connected.
