import os
import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
import sqlite3
import time
import json
import redis
from decimal import Decimal
from typing import Optional, Dict, Any, List, Union
from urllib.parse import urlparse
from datetime import datetime, timezone
from contextlib import contextmanager

# Configure logging (handlers are added in main.py)
logger = logging.getLogger(__name__)

class TradingDB:
    """
    A class to manage the database for the trading robot.
    It handles database connection, schema creation, and all trading operations
    with a strong focus on transaction safety and data integrity.
    It supports both PostgreSQL and SQLite for flexibility in testing and deployment.
    """
    def __init__(self, max_retries=5, initial_delay=1):
        """
        Initializes the TradingDB and connects to the database with retry logic.
        """
        self.conn = None # Used for SQLite or initial setup
        self.pool = None # Used for PostgreSQL multi-threading
        self.redis_client = None
        self.db_type = 'sqlite' if os.environ.get('USE_SQLITE') else 'postgres'
        self.param_style = '?' if self.db_type == 'sqlite' else '%s'
        self.max_retries = max_retries
        self.initial_delay = initial_delay

        try:
            self._connect_with_retry()
            self._init_redis()
        except Exception as e:
            logging.critical(f"FATAL: Could not connect to the database after {self.max_retries} retries. Application will exit.")
            raise e

    def _init_redis(self):
        """Initializes the Redis client."""
        redis_host = os.environ.get("REDIS_HOST", "redis")
        redis_port = int(os.environ.get("REDIS_PORT", 6379))
        try:
            self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self.redis_client.ping()
            logging.info(f"Successfully connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logging.warning(f"Failed to connect to Redis: {e}. Caching will be disabled.")
            self.redis_client = None

    def _connect_with_retry(self):
        """
        Attempts to connect to the database, retrying with exponential backoff.
        """
        if self.db_type == 'sqlite':
            try:
                self.conn = sqlite3.connect(':memory:', check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
                logging.info("Successfully connected to in-memory SQLite database.")
                return
            except sqlite3.Error as e:
                logging.error(f"Error connecting to SQLite database: {e}")
                raise e

        # PostgreSQL connection logic
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logging.info("DATABASE_URL not set, attempting to construct from individual POSTGRES_ variables.")
            db_user = os.environ.get("POSTGRES_USER")
            db_pass = os.environ.get("POSTGRES_PASSWORD")
            db_host = os.environ.get("POSTGRES_HOST")
            db_port = os.environ.get("POSTGRES_PORT")
            db_name = os.environ.get("POSTGRES_DB")

            if all([db_user, db_pass, db_host, db_port, db_name]):
                database_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            else:
                raise ValueError("Database connection details not found. Set DATABASE_URL or all individual POSTGRES_ variables.")

        result = urlparse(database_url)
        target_db_name = result.path[1:]

        # Connection details for the maintenance 'postgres' database
        maintenance_conn_params = {
            "dbname": "postgres",
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port,
            "connect_timeout": 3
        }

        self._ensure_database_exists(maintenance_conn_params, target_db_name)

        # Connection details for the target application database
        conn_params = {
            "dbname": target_db_name,
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port,
            "connect_timeout": 3
        }

        retries = 0
        delay = self.initial_delay
        while retries < self.max_retries:
            try:
                # Initialize connection pool for PostgreSQL
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    **conn_params
                )
                # Test the pool by getting a connection
                conn = self.pool.getconn()
                self.pool.putconn(conn)
                logging.info(f"Successfully initialized PostgreSQL connection pool.")
                return
            except psycopg2.OperationalError as e:
                logging.warning(f"Database connection attempt {retries + 1}/{self.max_retries} failed: {e}")
                retries += 1
                if retries < self.max_retries:
                    logging.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2 # Exponential backoff
                else:
                    logging.error("Maximum retry attempts reached. Could not connect to the database.")
                    raise e
            except psycopg2.Error as e:
                 # Handle other potential psycopg2 errors (e.g., authentication)
                logging.error(f"A non-retriable PostgreSQL error occurred: {e}")
                raise e

    def _ensure_database_exists(self, conn_params, db_name):
        """Connects to the maintenance DB to create the target DB if it doesn't exist."""
        conn_temp = None
        try:
            conn_temp = psycopg2.connect(**conn_params)
            conn_temp.autocommit = True
            with conn_temp.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                db_exists = cursor.fetchone()
                if not db_exists:
                    logging.info(f"Database '{db_name}' does not exist. Creating it...")
                    # Use psycopg2's sql module for safe quoting of identifiers
                    from psycopg2 import sql
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                    logging.info(f"Database '{db_name}' created successfully.")
        except psycopg2.Error as e:
            logging.error(f"Error while checking/creating database '{db_name}': {e}")
            raise
        finally:
            if conn_temp:
                conn_temp.close()

    def __del__(self):
        if self.conn:
            self.conn.close()
        if self.pool:
            self.pool.closeall()
        logging.info("Database connection/pool closed.")

    def get_connection(self):
        """Returns a connection from the pool (Postgres) or the single connection (SQLite)."""
        if self.db_type == 'postgres':
            return self.pool.getconn()
        else:
            return self.conn

    def release_connection(self, conn):
        """Releases a connection back to the pool (Postgres) or does nothing (SQLite)."""
        if self.db_type == 'postgres':
            self.pool.putconn(conn)

    @contextmanager
    def connection_scope(self):
        """Context manager to handle connection lifecycle."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.release_connection(conn)

    def get_cursor(self, conn=None):
        """Gets a cursor for the given connection. Defaults to self.conn for SQLite compatibility."""
        if conn is None:
            conn = self.conn

        if conn is None:
            raise ValueError("Connection must be provided when using a connection pool (Postgres).")

        if self.db_type == 'postgres':
            return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            return conn.cursor()

    def check_connection(self) -> bool:
        """Checks if the database connection is alive."""
        try:
            with self.connection_scope() as conn:
                cursor = self.get_cursor(conn)
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
        except Exception as e:
            logging.error(f"Database connection check failed: {e}")
            return False

    def get_database_stats(self) -> Dict[str, Any]:
        """Collects database statistics like table sizes and row counts."""
        stats = {"tables": {}}
        try:
            with self.connection_scope() as conn:
                cursor = self.get_cursor(conn)
                tables = ['accounts', 'positions', 'orders', 'ledger', 'prices']
                for table in tables:
                    # Row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row_count = cursor.fetchone()[0]

                    table_stats = {"row_count": row_count}

                    if self.db_type == 'postgres':
                        # Table size in bytes
                        cursor.execute(f"SELECT pg_total_relation_size(%s)", (table,))
                        size = cursor.fetchone()[0]
                        table_stats["size_bytes"] = size

                    stats["tables"][table] = table_stats

                if self.db_type == 'postgres':
                    cursor.execute("SELECT pg_database_size(current_database())")
                    stats["total_db_size_bytes"] = cursor.fetchone()[0]

                cursor.close()
            return stats
        except Exception as e:
            logging.error(f"Error collecting database stats: {e}")
            return {}

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        """Converts a database value (potentially string from SQLite) to Decimal."""
        if value is None:
            return None
        return Decimal(str(value))

    def ensure_price_partitions(self):
        """Public method to ensure price partitions exist."""
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            try:
                self._ensure_price_partitions(cursor)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(f"Failed to ensure price partitions: {e}")
                raise
            finally:
                cursor.close()

    def _ensure_price_partitions(self, cursor):
        if self.db_type != 'postgres':
            return
        from psycopg2 import sql
        today = datetime.now(timezone.utc).date()
        year_start = today.replace(month=1, day=1)
        next_year_start = year_start.replace(year=year_start.year + 1)
        for start, end in [(year_start, next_year_start)]:
            partition_name = f"prices_{start.year}"
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {partition_name}
                PARTITION OF prices
                FOR VALUES FROM ({start_date}) TO ({end_date})
            """).format(
                partition_name=sql.Identifier(partition_name),
                start_date=sql.Literal(start.isoformat()),
                end_date=sql.Literal(end.isoformat())
            )
            cursor.execute(query)

    def _add_column_if_not_exists(self, cursor, table, column, definition):
        """Adds a column to a table if it doesn't exist."""
        if self.db_type == 'sqlite':
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                logging.info(f"Added column {column} to table {table} (SQLite).")
            except sqlite3.OperationalError:
                # Column likely already exists
                pass
        else:
            # PostgreSQL uses a DO block to safely add a column
            query = f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name='{table}' AND column_name='{column}'
                    ) THEN
                        ALTER TABLE {table} ADD COLUMN {column} {definition};
                    END IF;
                END $$;
            """
            cursor.execute(query)
            logging.info(f"Ensured column {column} exists in table {table} (Postgres).")

    def setup_database(self):
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            self._setup_database_internal(cursor, conn)

    def _setup_database_internal(self, cursor, conn):
        # Define types compatible with both DBs
        numeric_type = 'TEXT' if self.db_type == 'sqlite' else 'NUMERIC(18, 5)'
        pk_type = 'INTEGER PRIMARY KEY AUTOINCREMENT' if self.db_type == 'sqlite' else 'SERIAL PRIMARY KEY'
        uuid_type = 'TEXT' if self.db_type == 'sqlite' else 'UUID'
        timestamp_type = 'TEXT' if self.db_type == 'sqlite' else 'TIMESTAMPTZ'

        try:
            if self.db_type == 'postgres':
                cursor.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_type_enum') THEN
                            CREATE TYPE order_type_enum AS ENUM ('BUY', 'SELL');
                        END IF;
                        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status_enum') THEN
                            CREATE TYPE order_status_enum AS ENUM ('pending', 'executed', 'cancelled', 'failed', 'placed', 'partially_filled');
                        END IF;
                    END
                    $$;
                """)

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id {pk_type},
                    account_name TEXT NOT NULL UNIQUE,
                    cash_balance {numeric_type} NOT NULL
                );
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS positions (
                    position_id {pk_type},
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    quantity BIGINT NOT NULL,
                    average_cost {numeric_type} NOT NULL,
                    UNIQUE (account_id, symbol)
                );
            """)

            # Using TEXT for status/type to be more flexible during migration
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id {pk_type},
                    trade_id TEXT NOT NULL UNIQUE,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity BIGINT NOT NULL,
                    price {numeric_type},
                    time_in_force TEXT DEFAULT 'GTC',
                    status TEXT NOT NULL,
                    broker_order_id TEXT,
                    reason TEXT,
                    executed_quantity BIGINT DEFAULT 0,
                    avg_execution_price {numeric_type},
                    executed_at {timestamp_type},
                    correlation_id TEXT,
                    timestamp {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    -- Backward compatibility
                    client_order_id {uuid_type} UNIQUE,
                    failure_reason TEXT
                );
            """)

            # Ensure new columns exist for existing databases
            self._add_column_if_not_exists(cursor, "orders", "trade_id", "TEXT")
            self._add_column_if_not_exists(cursor, "orders", "side", "TEXT")
            self._add_column_if_not_exists(cursor, "orders", "time_in_force", "TEXT DEFAULT 'GTC'")
            self._add_column_if_not_exists(cursor, "orders", "broker_order_id", "TEXT")
            self._add_column_if_not_exists(cursor, "orders", "reason", "TEXT")
            self._add_column_if_not_exists(cursor, "orders", "executed_quantity", "BIGINT DEFAULT 0")
            self._add_column_if_not_exists(cursor, "orders", "avg_execution_price", numeric_type)
            self._add_column_if_not_exists(cursor, "orders", "executed_at", timestamp_type)

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS ledger (
                    entry_id {pk_type},
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    order_id INTEGER REFERENCES orders(order_id),
                    asset TEXT NOT NULL,
                    change {numeric_type} NOT NULL,
                    new_balance {numeric_type} NOT NULL,
                    timestamp {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                );
            """)

            if self.db_type == 'postgres':
                # Check if prices table exists and if it is partitioned
                cursor.execute("""
                    SELECT relkind FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relname = 'prices'
                """)
                res = cursor.fetchone()
                if res and res['relkind'] != 'p':
                    logging.info("Existing 'prices' table is not partitioned. Migrating...")
                    cursor.execute("ALTER TABLE prices RENAME TO prices_old")
                    # We will create the partitioned one below

                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS prices (
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        timestamp {timestamp_type} NOT NULL,
                        open {numeric_type} NOT NULL,
                        high {numeric_type} NOT NULL,
                        low {numeric_type} NOT NULL,
                        close {numeric_type} NOT NULL,
                        volume BIGINT NOT NULL,
                        PRIMARY KEY (symbol, timeframe, timestamp)
                    ) PARTITION BY RANGE (timestamp);
                """)
                self._ensure_price_partitions(cursor)

                # If we renamed old, try to copy data
                if res and res['relkind'] != 'p':
                    try:
                        cursor.execute("INSERT INTO prices SELECT symbol, timeframe, timestamp, open, high, low, close, volume FROM prices_old ON CONFLICT DO NOTHING")
                        cursor.execute("DROP TABLE prices_old")
                        logging.info("Successfully migrated data to partitioned 'prices' table.")
                    except Exception as e:
                        logging.error(f"Failed to migrate data from prices_old: {e}")
            else:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS prices (
                        price_id {pk_type},
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        timestamp {timestamp_type} NOT NULL,
                        open {numeric_type} NOT NULL,
                        high {numeric_type} NOT NULL,
                        low {numeric_type} NOT NULL,
                        close {numeric_type} NOT NULL,
                        volume BIGINT NOT NULL,
                        UNIQUE (symbol, timeframe, timestamp)
                    );
                """)

            # Insert sample data for prices if it doesn't exist
            cursor.execute(f"SELECT * FROM prices WHERE symbol = {self.param_style}", ('AAPL',))
            if cursor.fetchone() is None:
                sample_prices = [
                    ('AAPL', '1h', '2025-01-01T10:00:00Z', '150.00', '152.00', '149.50', '151.50', 1000000),
                    ('AAPL', '1h', '2025-01-01T11:00:00Z', '151.50', '153.00', '151.00', '152.50', 1200000),
                    ('GOOG', '1d', '2025-01-01T10:00:00Z', '2800.00', '2810.00', '2795.00', '2805.00', 500000)
                ]
                for price_data in sample_prices:
                    cursor.execute(f"""
                        INSERT INTO prices (symbol, timeframe, timestamp, open, high, low, close, volume)
                        VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style})
                    """, price_data)

            cursor.execute(f"SELECT * FROM accounts WHERE account_name = {self.param_style}", ('main_account',))
            if cursor.fetchone() is None:
                initial_balance = '1000000.00'
                cursor.execute(
                    f"INSERT INTO accounts (account_name, cash_balance) VALUES ({self.param_style}, {self.param_style})",
                    ('main_account', initial_balance)
                )

                # Fetch the new account_id
                cursor.execute(f"SELECT account_id FROM accounts WHERE account_name = {self.param_style}", ('main_account',))
                account_id = cursor.fetchone()['account_id']

                cursor.execute(f"""
                    INSERT INTO ledger (account_id, asset, change, new_balance, description)
                    VALUES ({self.param_style}, 'CASH', {self.param_style}, {self.param_style}, 'Initial account funding')
                """, (account_id, initial_balance, initial_balance))
            conn.commit()
        except Exception as e:
            logging.error(f"Error setting up database: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()

    def create_order(self, account_id: Union[int, str], trade_id: str, symbol: str, side: str, order_type: str = 'market', quantity: int = 0, price: Optional[Decimal] = None, time_in_force: str = 'GTC', correlation_id: str = '') -> Optional[int]:
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            try:
                account_id = int(account_id)
                query = f"""
                    INSERT INTO orders (account_id, trade_id, symbol, side, order_type, quantity, price, time_in_force, status, correlation_id, client_order_id)
                    VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, 'pending', {self.param_style}, {self.param_style})
                """
                params = (account_id, trade_id, symbol.upper(), side.lower(), order_type.lower(), quantity, str(price) if price is not None else None, time_in_force, correlation_id, trade_id)
                cursor.execute(query, params)

                # Fetch last inserted ID
                cursor.execute(f"SELECT order_id FROM orders WHERE trade_id = {self.param_style}", (trade_id,))
                order_id = cursor.fetchone()['order_id']

                conn.commit()
                return order_id
            except (psycopg2.errors.UniqueViolation, sqlite3.IntegrityError):
                conn.rollback()
                cursor.execute(f"SELECT order_id FROM orders WHERE trade_id = {self.param_style}", (trade_id,))
                existing = cursor.fetchone()
                return existing['order_id'] if existing else None
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()

    def get_order_by_id(self, order_id: int) -> Optional[Dict[str, Any]]:
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            try:
                cursor.execute(f"SELECT * FROM orders WHERE order_id = {self.param_style}", (order_id,))
                row = cursor.fetchone()
                return self._format_order_row(row) if row else None
            finally:
                cursor.close()

    def get_order_by_trade_id(self, trade_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            try:
                cursor.execute(f"SELECT * FROM orders WHERE trade_id = {self.param_style}", (str(trade_id),))
                row = cursor.fetchone()
                return self._format_order_row(row) if row else None
            finally:
                cursor.close()

    def update_order(self, order_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with self.connection_scope() as conn:
            cursor = self.get_cursor(conn)
            try:
                if not updates:
                    return self.get_order_by_id(order_id)

                # Security: Whitelist allowed columns to prevent SQL injection or arbitrary updates
                ALLOWED_COLUMNS = {
                    'status', 'broker_order_id', 'reason', 'executed_quantity',
                    'avg_execution_price', 'executed_at', 'failure_reason', 'correlation_id'
                }

                set_clauses = []
                params = []
                for key, value in updates.items():
                    if key not in ALLOWED_COLUMNS:
                        logging.warning(f"Ignored disallowed update key: {key}")
                        continue

                ...
