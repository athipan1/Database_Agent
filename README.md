# Database Agent for Financial Trading Systems

This project provides a robust, secure, and reliable Database Agent designed specifically for financial trading applications. Built with Python, FastAPI, and PostgreSQL, it serves as the foundational data layer, ensuring transaction safety, data integrity, and complete auditability for a trading system where real money is involved.

The entire environment is containerized using Docker, allowing for easy setup and consistent deployment.

## Core Principles & Features

This system is architected around critical principles required for financial-grade applications:

-   **Transaction Safety & Atomicity:** Every order execution is fully atomic. All database changes (updating balances, positions, orders, and ledgers) either succeed together or fail together, preventing any partial or inconsistent data states.
-   **Data Integrity:** Utilizes precise `NUMERIC` data types for all financial values to avoid floating-point inaccuracies. Referential integrity is enforced with foreign key constraints.
-   **Concurrency Control:** Employs pessimistic locking (`SELECT ... FOR UPDATE`) during transactions to prevent race conditions when processing multiple orders simultaneously.
-   **Complete Auditability:** Features a double-entry `ledger` table that records every single movement of assets (both cash and stocks). This provides a full, verifiable audit trail for every transaction.
-   **Idempotency:** The order creation endpoint is idempotent. Submitting the same order multiple times (using the same `client_order_id`) will not result in duplicate entries, preventing costly mistakes from network retries.
-   **API Security:** Endpoints are protected via an API Key, ensuring that only authorized clients can interact with the trading database.

## Technology Stack

-   **Backend:** FastAPI (Python)
-   **Database:** PostgreSQL
-   **Containerization:** Docker & Docker Compose

## Getting Started

### Prerequisites

-   Docker and Docker Compose (v2) installed.
-   A `.env` file created in the project root.

### 1. Setup Environment File

First, create a `.env` file by copying the example template:

```bash
cp .env.example .env
```

Review the `.env` file. You can change the default database credentials if you wish. It is highly recommended to replace the placeholder `API_KEY` with a secure, randomly generated key, for example:

```bash
# Generate a new key in your terminal
openssl rand -hex 32

# Copy the output and paste it into your .env file
API_KEY=your_newly_generated_secret_key_here
```

### 2. Build and Run the Services

With Docker running, bring up the entire stack (API and Database) in detached mode:

```bash
sudo docker compose up --build -d
```

-   The `db` service will start a PostgreSQL container.
-   The `api` service will build the FastAPI application image and start the server.
-   The API will be available at `http://localhost:8000`.

To stop the services, run:

```bash
sudo docker compose down
```

## Running the Automated Tests

The project includes a comprehensive test suite using `pytest` to ensure all business logic is functioning correctly.

### 1. Start the Database

The tests require a running database to connect to. Ensure the PostgreSQL container is up:

```bash
sudo docker compose up -d db
```

### 2. Run Pytest

Execute the test suite from the project's root directory:

```bash
pytest
```

The tests will automatically connect to the database, set up the schema, run all test cases, and clean up afterwards.

## API Usage

All endpoints require an `X-API-KEY` header containing your secret API key.

### Example: Creating and Executing a Buy Order

**1. Create a Pending Order**

`POST /accounts/1/orders`

**Headers:**
`X-API-KEY: your_secret_api_key_here`

**Body:**

```json
{
  "client_order_id": "a8c9b1a2-2b8c-4a3e-9b6b-3e5f2a1d7c4e",
  "symbol": "AAPL",
  "order_type": "BUY",
  "quantity": 10,
  "price": "150.75"
}
```

**2. Execute the Order**

`POST /orders/{order_id}/execute`

**Headers:**
`X-API-KEY: your_secret_api_key_here`

This will trigger the atomic transaction to update balances, positions, and create ledger entries.
