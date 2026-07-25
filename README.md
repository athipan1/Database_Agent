# Database Agent

The Database Agent is a FastAPI-based service responsible for managing all database interactions for the trading system. It provides a secure and transactional API for logging decisions, tracking outcomes, and serving data to other agents like the `LearningAgent`.

## Core Responsibilities

1. **Decision Logging**: Records every `buy`/`sell`/`hold` decision from all trading agents, linking them with a unique `correlation_id` for end-to-end traceability.
2. **Outcome Tracking**: Stores the actual results of trades, such as profit/loss and drawdown over various time horizons.
3. **Data Source for Learning**: Acts as the single source of truth for the `LearningAgent` to evaluate agent performance, calculate rewards/penalties, and adjust agent weights.
4. **System Auditing**: Provides a complete audit trail. A single `correlation_id` can trace an entire decision and execution chain.

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

## Supabase asynchronous mirror

Railway PostgreSQL remains the only source of truth for trading decisions and
execution state. Supabase is an optional secondary mirror for dashboards,
analytics, and reporting.

The request path never calls Supabase directly. Successful write responses are
stored in the local `supabase_replication_outbox` table. A background worker then
upserts versioned events into Supabase by deterministic `event_id`. Failed
network calls are retried with exponential backoff, and repeated events remain
idempotent.

Required Railway variables:

```ini
SUPABASE_REPLICATION_ENABLED=true
SUPABASE_URL=https://djolfrfrghhvwmpvtkpt.supabase.co
SUPABASE_SECRET_KEY=<server-side secret key>
SUPABASE_TABLE=database_agent_events
SUPABASE_REPLICATION_INTERVAL_SECONDS=10
SUPABASE_REPLICATION_BATCH_SIZE=50
SUPABASE_REPLICATION_MAX_ATTEMPTS=10
```

`SUPABASE_SECRET_KEY` must be a server-only Supabase secret key or legacy
service-role key. Never put it in frontend code, Vite variables, browser storage,
or logs. A publishable or anon key is intentionally insufficient because the
mirror table has RLS enabled and grants no access to `anon` or `authenticated`.

`GET /health` reports safe replication information such as enabled/configured
state, worker status, and outbox counts. It never returns the URL or key.

---

## Getting Started

This guide walks through running the Database Agent using Docker and Docker Compose.

### Prerequisites

* Docker
* Docker Compose

### 1. Set Up Environment Variables

Create a `.env` file by copying the example file:

```bash
cp .env.example .env
```

Customize the variables:

* `POSTGRES_PASSWORD`: Set a strong and unique password for PostgreSQL.
* `DATABASE_URL`: Use the same password and correct PostgreSQL host.
* `DATABASE_AGENT_API_KEY`: Generate a secure random API key for service authentication.

Example core configuration:

```ini
POSTGRES_USER=trading_user
POSTGRES_PASSWORD=your_super_secret_password
POSTGRES_DB=trading_db
POSTGRES_HOST=db
POSTGRES_PORT=5432
DATABASE_URL=postgresql://trading_user:your_super_secret_password@db:5432/trading_db
DATABASE_AGENT_API_KEY=your_generated_api_key_here
```

### 2. Build and Run the Service

```bash
sudo docker compose up --build -d
```

* `--build`: Rebuilds the image with the latest code.
* `-d`: Runs the containers in the background.

### 3. Verify the Service

Check container status:

```bash
sudo docker compose ps
```

View logs:

```bash
sudo docker compose logs -f api
```

Call the health endpoint:

```bash
curl http://localhost:8000/health
```

The response includes primary database connectivity and Supabase replication
status inside the standard Database Agent response envelope.
