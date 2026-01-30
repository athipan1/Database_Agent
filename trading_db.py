import os
import logging
import psycopg2
import psycopg2.extras
import sqlite3
import time
from decimal import Decimal
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        self.conn = None
        self.db_type = 'sqlite' if os.environ.get('USE_SQLITE') else 'postgres'
        self.param_style = '?' if self.db_type == 'sqlite' else '%s'
        self.max_retries = max_retries
        self.initial_delay = initial_delay

        try:
            self._connect_with_retry()
        except Exception as e:
            logging.critical(f"FATAL: Could not connect to the database after {self.max_retries} retries. Application will exit.")
            raise e

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
                # Set a short timeout to allow the retry logic to work quickly
                self.conn = psycopg2.connect(**conn_params)
                logging.info(f"Successfully connected to PostgreSQL database.")
                return # Exit the loop on successful connection
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
            logging.info("Database connection closed.")

    def get_cursor(self):
        if self.db_type == 'postgres':
            # Returns rows that behave like dictionaries
            return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            # sqlite3.Row objects are similar enough to DictCursor for this project
            return self.conn.cursor()

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        """Converts a database value (potentially string from SQLite) to Decimal."""
        if value is None:
            return None
        return Decimal(str(value))

    def setup_database(self):
        cursor = self.get_cursor()
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
                            CREATE TYPE order_status_enum AS ENUM ('pending', 'executed', 'cancelled', 'failed');
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
            order_type_pg = 'order_type_enum' if self.db_type == 'postgres' else 'TEXT'
            order_status_pg = 'order_status_enum' if self.db_type == 'postgres' else 'TEXT'
            order_type_check = "CHECK(order_type IN ('BUY', 'SELL'))" if self.db_type == 'sqlite' else ''
            order_status_check = "CHECK(status IN ('pending', 'executed', 'cancelled', 'failed'))" if self.db_type == 'sqlite' else ''

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id {pk_type},
                    client_order_id {uuid_type} NOT NULL UNIQUE,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    order_type {order_type_pg} NOT NULL {order_type_check},
                    quantity BIGINT NOT NULL,
                    price {numeric_type},
                    status {order_status_pg} NOT NULL {order_status_check},
                    failure_reason TEXT,
                    correlation_id TEXT,
                    timestamp {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
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
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error setting up database: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def create_order(self, account_id: int, client_order_id: str, symbol: str, order_type: str, quantity: int, price: Decimal, correlation_id: str) -> Optional[int]:
        cursor = self.get_cursor()
        try:
            query = f"""
                INSERT INTO orders (account_id, client_order_id, symbol, order_type, quantity, price, status, correlation_id)
                VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, 'pending', {self.param_style})
            """
            params = (account_id, client_order_id, symbol.upper(), order_type.upper(), quantity, str(price), correlation_id)
            cursor.execute(query, params)

            # Fetch last inserted ID
            cursor.execute(f"SELECT order_id FROM orders WHERE client_order_id = {self.param_style}", (client_order_id,))
            order_id = cursor.fetchone()['order_id']

            self.conn.commit()
            return order_id
        except (psycopg2.errors.UniqueViolation, sqlite3.IntegrityError):
            self.conn.rollback()
            cursor.execute(f"SELECT order_id FROM orders WHERE client_order_id = {self.param_style}", (client_order_id,))
            existing = cursor.fetchone()
            return existing['order_id'] if existing else None
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cursor.close()

    def execute_order(self, order_id: int) -> (str, Optional[str]):
        cursor = self.get_cursor()
        try:
            # For SQLite, FOR UPDATE is not supported, so we start a transaction
            # with an immediate lock to prevent concurrent writes.
            # For Postgres, the SELECT...FOR UPDATE will handle locking.
            if self.db_type == 'sqlite':
                cursor.execute("BEGIN IMMEDIATE;")
            else:
                cursor.execute("BEGIN;")


            lock_clause = "FOR UPDATE" if self.db_type == 'postgres' else ""
            cursor.execute(f"SELECT * FROM orders WHERE order_id = {self.param_style} AND status = 'pending' {lock_clause}", (order_id,))
            order = cursor.fetchone()
            if not order:
                self.conn.rollback()
                return 'failed', 'invalid_state'

            account_id, symbol, order_type, quantity = order['account_id'], order['symbol'], order['order_type'], order['quantity']
            price = self._to_decimal(order['price'])
            total_cost = quantity * price

            cursor.execute(f"SELECT * FROM accounts WHERE account_id = {self.param_style} {lock_clause}", (account_id,))
            account = cursor.fetchone()
            if not account:
                raise Exception(f"Account {account_id} not found for order {order_id}")

            cash_balance = self._to_decimal(account['cash_balance'])

            if order_type == 'BUY':
                if cash_balance < total_cost:
                    self._update_order_status_in_txn(cursor, order_id, 'failed', "insufficient_funds")
                    self.conn.commit()
                    return 'failed', 'insufficient_funds'

                new_balance = cash_balance - total_cost
                self._update_balance_in_txn(cursor, account_id, new_balance, order_id, -total_cost, f"BUY {quantity} {symbol}")
                self._update_position_and_ledger_on_buy_in_txn(cursor, account_id, symbol, quantity, price, order_id)
                self._update_order_status_in_txn(cursor, order_id, 'executed')

            elif order_type == 'SELL':
                cursor.execute(f"SELECT * FROM positions WHERE account_id = {self.param_style} AND symbol = {self.param_style} {lock_clause}", (account_id, symbol))
                position = cursor.fetchone()
                if not position or position['quantity'] < quantity:
                    self._update_order_status_in_txn(cursor, order_id, 'failed', "insufficient_shares")
                    self.conn.commit()
                    return 'failed', 'insufficient_shares'

                new_balance = cash_balance + total_cost
                self._update_balance_in_txn(cursor, account_id, new_balance, order_id, total_cost, f"SELL {quantity} {symbol}")
                self._update_position_and_ledger_on_sell_in_txn(cursor, dict(position), quantity, order_id)
                self._update_order_status_in_txn(cursor, order_id, 'executed')

            self.conn.commit()
            return 'executed', None
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Failed to execute order {order_id}: {e}", exc_info=True)
            raise
        finally:
            cursor.close()

    def _update_order_status_in_txn(self, cursor, order_id, status, reason=None):
        cursor.execute(
            f"UPDATE orders SET status = {self.param_style}, failure_reason = {self.param_style} WHERE order_id = {self.param_style}",
            (status, reason, order_id)
        )

    def _update_balance_in_txn(self, cursor, account_id, new_balance, order_id, change, description):
        cursor.execute(f"UPDATE accounts SET cash_balance = {self.param_style} WHERE account_id = {self.param_style}", (str(new_balance), account_id))
        cursor.execute(f"""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES ({self.param_style}, {self.param_style}, 'CASH', {self.param_style}, {self.param_style}, {self.param_style})
        """, (account_id, order_id, str(change), str(new_balance), description))

    def _update_position_and_ledger_on_buy_in_txn(self, cursor, account_id, symbol, quantity, price, order_id):
        lock_clause = "FOR UPDATE" if self.db_type == 'postgres' else ""
        cursor.execute(f"SELECT * FROM positions WHERE account_id = {self.param_style} AND symbol = {self.param_style} {lock_clause}", (account_id, symbol))
        position = cursor.fetchone()

        if position:
            avg_cost = self._to_decimal(position['average_cost'])
            new_quantity = position['quantity'] + quantity
            new_avg_cost = ((avg_cost * position['quantity']) + (price * quantity)) / new_quantity
            cursor.execute(
                f"UPDATE positions SET quantity = {self.param_style}, average_cost = {self.param_style} WHERE position_id = {self.param_style}",
                (new_quantity, str(new_avg_cost), position['position_id'])
            )
        else:
            new_quantity = quantity
            cursor.execute(f"""
                INSERT INTO positions (account_id, symbol, quantity, average_cost)
                VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style})
            """, (account_id, symbol, quantity, str(price)))

        cursor.execute(f"""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style})
        """, (account_id, order_id, symbol, quantity, new_quantity, f"BUY {quantity} {symbol}"))

    def _update_position_and_ledger_on_sell_in_txn(self, cursor, position, sell_quantity, order_id):
        new_quantity = position['quantity'] - sell_quantity
        if new_quantity == 0:
            cursor.execute(f"DELETE FROM positions WHERE position_id = {self.param_style}", (position['position_id'],))
        else:
            cursor.execute(f"UPDATE positions SET quantity = {self.param_style} WHERE position_id = {self.param_style}", (new_quantity, position['position_id']))

        cursor.execute(f"""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style})
        """, (position['account_id'], order_id, position['symbol'], -sell_quantity, new_quantity, f"SELL {sell_quantity} {position['symbol']}"))

    def get_account_balance(self, account_id: int) -> Optional[Decimal]:
        cursor = self.get_cursor()
        try:
            cursor.execute(f"SELECT cash_balance FROM accounts WHERE account_id = {self.param_style}", (account_id,))
            result = cursor.fetchone()
            return self._to_decimal(result['cash_balance']) if result else None
        finally:
            cursor.close()

    def get_positions(self, account_id: int) -> List[Dict[str, Any]]:
        cursor = self.get_cursor()
        try:
            cursor.execute(f"SELECT * FROM positions WHERE account_id = {self.param_style}", (account_id,))
            return [{k: self._to_decimal(v) if k == 'average_cost' else v for k, v in dict(row).items()} for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_order_history(self, account_id: int) -> List[Dict[str, Any]]:
        cursor = self.get_cursor()
        try:
            cursor.execute(f"SELECT * FROM orders WHERE account_id = {self.param_style} ORDER BY timestamp DESC", (account_id,))
            return [{k: self._to_decimal(v) if k == 'price' else v for k, v in dict(row).items()} for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_executions(self, account_id: int, limit: int = 50, offset: int = 0, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        cursor = self.get_cursor()
        try:
            query = "SELECT order_id, account_id, symbol, order_type, quantity, price, timestamp FROM orders WHERE account_id = ? AND status = 'executed'"
            params = [account_id]

            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date)
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date)

            query += " ORDER BY timestamp DESC, order_id DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            # Adjust param style for PostgreSQL
            if self.db_type == 'postgres':
                query = query.replace('?', '%s')

            cursor.execute(query, tuple(params))
            trades = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                price = self._to_decimal(row_dict.get('price'))
                quantity = row_dict.get('quantity')
                notional = price * quantity if price and quantity else Decimal('0')

                trades.append({
                    "trade_id": row_dict.get('order_id'),
                    "account_id": row_dict.get('account_id'),
                    "symbol": row_dict.get('symbol'),
                    "side": row_dict.get('order_type').lower(),
                    "quantity": quantity,
                    "price": price,
                    "notional": notional,
                    "executed_at": row_dict.get('timestamp'),
                    # NOTE: asset_id and source_agent are not in the current schema
                    "asset_id": None,
                    "source_agent": None
                })
            return trades
        finally:
            cursor.close()

    def get_price_history(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        cursor = self.get_cursor()
        try:
            query = f"SELECT * FROM prices WHERE symbol = {self.param_style} AND timeframe = {self.param_style} ORDER BY timestamp DESC LIMIT {self.param_style}"
            cursor.execute(query, (symbol.upper(), timeframe, limit))

            return [
                {
                    'symbol': row['symbol'],
                    'timeframe': row['timeframe'],
                    'timestamp': row['timestamp'],
                    'open': self._to_decimal(row['open']),
                    'high': self._to_decimal(row['high']),
                    'low': self._to_decimal(row['low']),
                    'close': self._to_decimal(row['close']),
                    'volume': row['volume'],
                }
                for row in cursor.fetchall()
            ]
        finally:
            cursor.close()

    def ingest_historical_prices(self, price_data: List[Dict[str, Any]]):
        """
        Ingests a list of historical price data points into the database.
        It uses an 'ON CONFLICT DO NOTHING' clause to prevent duplicates
        based on the (symbol, timeframe, timestamp) unique constraint.
        This is a highly efficient bulk operation.
        """
        if not price_data:
            logging.info("No price data provided to ingest.")
            return

        if self.db_type == 'sqlite':
            # SQLite uses a different syntax for ON CONFLICT
            query = """
                INSERT INTO prices (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (:symbol, :timeframe, :timestamp, :open, :high, :low, :close, :volume)
                ON CONFLICT(symbol, timeframe, timestamp) DO NOTHING;
            """
        else: # PostgreSQL
            query = """
                INSERT INTO prices (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (%(symbol)s, %(timeframe)s, %(timestamp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
                ON CONFLICT (symbol, timeframe, timestamp) DO NOTHING;
            """

        cursor = self.get_cursor()
        try:
            # For SQLite, execute many with a list of dicts
            if self.db_type == 'sqlite':
                 cursor.executemany(query, price_data)
            else: # For psycopg2, execute_batch is highly efficient
                psycopg2.extras.execute_batch(cursor, query, price_data)

            self.conn.commit()
            logging.info(f"Successfully ingested or skipped {len(price_data)} price records.")
        except Exception as e:
            logging.error(f"Database error during price ingestion: {e}", exc_info=True)
            self.conn.rollback()
            raise
        finally:
            cursor.close()

