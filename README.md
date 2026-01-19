# Database Agent

The Database Agent is a FastAPI-based service responsible for managing all database interactions for the trading system. It provides a secure and transactional API for logging decisions, tracking outcomes, and serving data to other agents like the `LearningAgent`.

## Core Responsibilities

1.  **Decision Logging**: Records every `buy`/`sell`/`hold` decision from all trading agents, linking them with a unique `correlation_id` for end-to-end traceability.
2.  **Outcome Tracking**: Stores the actual results of trades, such as profit/loss and drawdown over various time horizons (e.g., t+1, t+7, t+30).
3.  **Data Source for Learning**: Acts as the single source of truth for the `LearningAgent` to evaluate agent performance, calculate rewards/penalties, and adjust agent weights.
4.  **System Auditing**: Provides a complete audit trail. A single `correlation_id` can be used to trace an entire decision and execution chain, simplifying debugging and enhancing explainability.

---

## Getting Started

This guide will walk you through setting up and running the Database Agent using Docker and Docker Compose.

### Prerequisites

*   Docker
*   Docker Compose

### 1. Set Up Environment Variables

The service is configured using environment variables. First, create a `.env` file by copying the example file:

```bash
cp .env.example .env
```

Next, open the `.env` file and customize the variables:

*   `POSTGRES_PASSWORD`: **(Required)** Set a strong and unique password for the PostgreSQL database. This is used by the database container itself.
*   `DATABASE_URL`: **(Required)** Update the password in this URL to match the `POSTGRES_PASSWORD` you set above. This is the full connection string the application uses.
*   `DATABASE_AGENT_API_KEY`: **(Required)** Generate a secure, random API key that clients will use to authenticate with this service. You can generate one with `openssl rand -hex 32`.

**Example `.env` file:**

```ini
# PostgreSQL Database Configuration
POSTGRES_USER=trading_user
POSTGRES_PASSWORD=your_super_secret_password
POSTGRES_DB=trading_db
POSTGRES_HOST=db
POSTGRES_PORT=5432

# Application Configuration
DATABASE_URL=postgresql://trading_user:your_super_secret_password@db:5432/trading_db

# API Security Configuration
DATABASE_AGENT_API_KEY=your_generated_api_key_here
```

### 2. Build and Run the Service

With the `.env` file configured, you can start the entire stack (the API service and the PostgreSQL database) using Docker Compose:

```bash
sudo docker compose up --build -d
```

*   `--build`: Forces a rebuild of the Docker image to ensure your latest code changes are included.
*   `-d`: Runs the containers in detached mode (in the background).

### 3. Verify the Service

You can check if the service is running correctly in a few ways:

*   **Check container status:**
    ```bash
    sudo docker compose ps
    ```
    You should see both the `trading_db_api` and `trading_db_postgres` containers running with a "healthy" status.

*   **View logs:**
    ```bash
    sudo docker compose logs -f api
    ```
    This will show you the real-time logs for the API service. Look for a message indicating a successful connection to the PostgreSQL database.

*   **Access the health check endpoint:**
    ```bash
    curl http://localhost:8000/health
    ```
    If the service is running correctly, you will receive the following response:
    ```json
    {"status":"ok"}
    ```
