import os
import logging
import psycopg2
import psycopg2.extras
import sqlite3
from decimal import Decimal
from typing import Optional, Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradingDB:
    """
    A class to manage the database for the trading robot.
    It handles database connection, schema creation, and all trading operations
    with a strong focus on transaction safety and data integrity.
    It supports both PostgreSQL and SQLite for flexibility in testing and deployment.
    """
    def __init__(self):
        """
        Initializes the TradingDB object and connects to the database.
        If USE_SQLITE is set in the environment, it uses an in-memory SQLite database.
        Otherwise, it connects to a PostgreSQL database using environment variables.
        """
        self.conn = None
        self.db_type = 'sqlite' if os.environ.get('USE_SQLITE') else 'postgres'
        self.param_style = '?' if self.db_type == 'sqlite' else '%s'

        if self.db_type == 'sqlite':
            try:
                self.conn = sqlite3.connect(':memory:', check_same_thread=False)
                self.conn.row_factory = sqlite3.Row
                logging.info("Successfully connected to in-memory SQLite database.")
            except sqlite3.Error as e:
                logging.error(f"Error connecting to SQLite database: {e}")
                raise e
        else:
            try:
                self.conn = psycopg2.connect(
                    dbname=os.environ.get("POSTGRES_DB"),
                    user=os.environ.get("POSTGRES_USER"),
                    password=os.environ.get("POSTGRES_PASSWORD"),
                    host=os.environ.get("POSTGRES_HOST"),
                    port=os.environ.get("POSTGRES_PORT")
                )
                logging.info(f"Successfully connected to PostgreSQL database: {os.environ.get('POSTGRES_DB')}")
            except psycopg2.OperationalError as e:
                logging.error(f"Error connecting to PostgreSQL database: {e}")
                raise e

    def __del__(self):
        """
        Destructor to close the database connection when the object is destroyed.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def get_cursor(self):
        """Returns a cursor object compatible with the connected database."""
        if self.db_type == 'postgres':
            return self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            return self.conn.cursor()

    def setup_database(self):
        """
        Creates the necessary tables if they don't exist and initializes
        the default account. This function is idempotent.
        """
        cursor = self.get_cursor()
        try:
            # Create accounts table with NUMERIC for financial data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id SERIAL PRIMARY KEY,
                    account_name TEXT NOT NULL UNIQUE,
                    cash_balance NUMERIC(18, 5) NOT NULL
                );
            """)
            logging.info("Table 'accounts' created or already exists.")

            # Create positions table with NUMERIC for financial data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    position_id SERIAL PRIMARY KEY,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    quantity BIGINT NOT NULL,
                    average_cost NUMERIC(18, 5) NOT NULL,
                    UNIQUE (account_id, symbol)
                );
            """)
            logging.info("Table 'positions' created or already exists.")

            # Create orders table with NUMERIC, UUID for idempotency, and failure reason
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    client_order_id UUID NOT NULL UNIQUE,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    order_type TEXT NOT NULL CHECK(order_type IN ('BUY', 'SELL')),
                    quantity BIGINT NOT NULL,
                    price NUMERIC(18, 5),
                    status TEXT NOT NULL CHECK(status IN ('pending', 'executed', 'cancelled', 'failed')),
                    failure_reason TEXT,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            logging.info("Table 'orders' created or already exists.")

            # Create ledger table for full auditability (double-entry bookkeeping)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ledger (
                    entry_id SERIAL PRIMARY KEY,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    order_id INTEGER REFERENCES orders(order_id),
                    asset TEXT NOT NULL, -- 'CASH' or a stock symbol like 'AAPL'
                    change NUMERIC(18, 5) NOT NULL, -- Positive for credit, negative for debit
                    new_balance NUMERIC(18, 5) NOT NULL,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                );
            """)
            logging.info("Table 'ledger' created or already exists.")

            # Create a default account if it doesn't exist
            cursor.execute("SELECT * FROM accounts WHERE account_name = %s", ('main_account',))
            if cursor.fetchone() is None:
                initial_balance = Decimal('1000000.00')
                cursor.execute(
                    "INSERT INTO accounts (account_name, cash_balance) VALUES (%s, %s) RETURNING account_id",
                    ('main_account', initial_balance)
                )
                account_id = cursor.fetchone()['account_id']
                # Initial funding ledger entry
                cursor.execute("""
                    INSERT INTO ledger (account_id, asset, change, new_balance, description)
                    VALUES (%s, 'CASH', %s, %s, 'Initial account funding')
                """, (account_id, initial_balance, initial_balance))
                logging.info(f"Created default 'main_account' (ID: {account_id}) with {initial_balance} cash balance.")

            self.conn.commit()
            logging.info("Database setup completed successfully.")
        except Exception as e:
            logging.error(f"Error setting up database: {e}")
            self.conn.rollback()
            raise e
        finally:
            cursor.close()

    def create_order(self, account_id: int, client_order_id: str, symbol: str, order_type: str, quantity: int, price: Decimal) -> Optional[int]:
        """
        Creates a new order with 'pending' status.
        :return: The ID of the newly created order, or None on failure.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("""
                INSERT INTO orders (account_id, client_order_id, symbol, order_type, quantity, price, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                RETURNING order_id
            """, (account_id, client_order_id, symbol.upper(), order_type.upper(), quantity, price))
            order_id = cursor.fetchone()['order_id']
            self.conn.commit()
            logging.info(f"Created pending {order_type} order for {quantity} {symbol} @ {price}. Order ID: {order_id}")
            return order_id
        except psycopg2.errors.UniqueViolation:
            self.conn.rollback()
            logging.warning(f"Attempted to create an order with a duplicate client_order_id: {client_order_id}")
            # Optionally, find and return the existing order_id
            cursor.execute("SELECT order_id FROM orders WHERE client_order_id = %s", (client_order_id,))
            existing = cursor.fetchone()
            return existing['order_id'] if existing else None
        except Exception as e:
            logging.error(f"Failed to create order: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()

    def execute_order(self, order_id: int):
        """
        Executes a pending order within a single atomic transaction.
        Updates account balance, positions, ledger, and the order itself.
        Uses pessimistic locking ('SELECT FOR UPDATE') to prevent race conditions.
        """
        cursor = self.get_cursor()
        try:
            # --- Start Atomic Transaction ---
            cursor.execute("BEGIN;")

            # 1. Lock and fetch the order to ensure it's pending and not being processed elsewhere
            cursor.execute("SELECT * FROM orders WHERE order_id = %s AND status = 'pending' FOR UPDATE", (order_id,))
            order = cursor.fetchone()

            if not order:
                logging.warning(f"Order {order_id} not found or not pending. Cannot execute.")
                self.conn.rollback() # Rollback is safe even if nothing happened
                return

            account_id = order['account_id']
            symbol = order['symbol']
            order_type = order['order_type']
            quantity = order['quantity']
            price = order['price']
            total_cost = quantity * price

            logging.info(f"Executing {order_type} order {order_id} for {quantity} {symbol} @ {price}")

            # 2. Lock and fetch the account to ensure funds/positions are not changed by another transaction
            cursor.execute("SELECT account_id, cash_balance FROM accounts WHERE account_id = %s FOR UPDATE", (account_id,))
            account = cursor.fetchone()

            if not account:
                # This should not happen if foreign keys are set up correctly
                raise Exception(f"Account {account_id} not found for order {order_id}")


            if order_type == 'BUY':
                if account['cash_balance'] < total_cost:
                    self._update_order_status_in_txn(cursor, order_id, 'failed', "Insufficient funds")
                else:
                    new_balance = account['cash_balance'] - total_cost
                    self._update_balance_in_txn(cursor, account_id, new_balance, order_id, -total_cost, f"BUY {quantity} {symbol}")
                    self._update_position_and_ledger_on_buy_in_txn(cursor, account_id, symbol, quantity, price, order_id)
                    self._update_order_status_in_txn(cursor, order_id, 'executed')

            elif order_type == 'SELL':
                cursor.execute("SELECT * FROM positions WHERE account_id = %s AND symbol = %s FOR UPDATE", (account_id, symbol))
                position = cursor.fetchone()

                if not position or position['quantity'] < quantity:
                    self._update_order_status_in_txn(cursor, order_id, 'failed', "Insufficient shares to sell")
                else:
                    new_balance = account['cash_balance'] + total_cost
                    self._update_balance_in_txn(cursor, account_id, new_balance, order_id, total_cost, f"SELL {quantity} {symbol}")
                    self._update_position_and_ledger_on_sell_in_txn(cursor, position, quantity, order_id)
                    self._update_order_status_in_txn(cursor, order_id, 'executed')

            # --- Commit Transaction ---
            self.conn.commit()
            logging.info(f"Transaction for order {order_id} committed successfully.")

        except Exception as e:
            logging.error(f"Failed to execute order {order_id}: {e}. Rolling back transaction.")
            self.conn.rollback()
            # We DO NOT try to update status outside the transaction. The order remains 'pending' for review.
        finally:
            cursor.close()

    def _update_order_status_in_txn(self, cursor, order_id, status, reason=None):
        """Helper to update order status within a transaction."""
        cursor.execute(
            "UPDATE orders SET status = %s, failure_reason = %s WHERE order_id = %s",
            (status, reason, order_id)
        )
        logging.info(f"Order {order_id} status updated to '{status}' in transaction.")

    def _update_balance_in_txn(self, cursor, account_id, new_balance, order_id, change, description):
        """Helper to update account balance and create a ledger entry within a transaction."""
        cursor.execute("UPDATE accounts SET cash_balance = %s WHERE account_id = %s", (new_balance, account_id))
        cursor.execute("""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES (%s, %s, 'CASH', %s, %s, %s)
        """, (account_id, order_id, change, new_balance, description))

    def _update_position_and_ledger_on_buy_in_txn(self, cursor, account_id, symbol, quantity, price, order_id):
        """Helper to update/create a position and create ledger entries after a buy."""
        cursor.execute("SELECT * FROM positions WHERE account_id = %s AND symbol = %s FOR UPDATE", (account_id, symbol))
        position = cursor.fetchone()

        if position:
            new_quantity = position['quantity'] + quantity
            new_avg_cost = ((position['average_cost'] * position['quantity']) + (price * quantity)) / new_quantity
            cursor.execute(
                "UPDATE positions SET quantity = %s, average_cost = %s WHERE position_id = %s",
                (new_quantity, new_avg_cost, position['position_id'])
            )
        else:
            cursor.execute("""
                INSERT INTO positions (account_id, symbol, quantity, average_cost)
                VALUES (%s, %s, %s, %s)
            """, (account_id, symbol, quantity, price))

        # Ledger entry for the stock
        cursor.execute("""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (account_id, order_id, symbol, quantity, new_quantity if position else quantity, f"BUY {quantity} {symbol}"))

    def _update_position_and_ledger_on_sell_in_txn(self, cursor, position, sell_quantity, order_id):
        """Helper to update/delete a position and create ledger entries after a sell."""
        new_quantity = position['quantity'] - sell_quantity
        if new_quantity == 0:
            cursor.execute("DELETE FROM positions WHERE position_id = %s", (position['position_id'],))
        else:
            cursor.execute("UPDATE positions SET quantity = %s WHERE position_id = %s", (new_quantity, position['position_id']))

        # Ledger entry for the stock
        cursor.execute("""
            INSERT INTO ledger (account_id, order_id, asset, change, new_balance, description)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (position['account_id'], order_id, position['symbol'], -sell_quantity, new_quantity, f"SELL {sell_quantity} {position['symbol']}"))


    def get_account_balance(self, account_id: int) -> Optional[Decimal]:
        """
        Retrieves the cash balance for a specific account.
        :param account_id: The ID of the account.
        :return: The cash balance as a Decimal, or None if account not found.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("SELECT cash_balance FROM accounts WHERE account_id = %s", (account_id,))
            result = cursor.fetchone()
            return result['cash_balance'] if result else None
        except Exception as e:
            logging.error(f"Error getting account balance: {e}")
            return None
        finally:
            cursor.close()

    def get_positions(self, account_id: int) -> List[Dict[str, Any]]:
        """
        Retrieves all positions for a specific account.
        :param account_id: The ID of the account.
        :return: A list of dictionaries representing the positions.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("SELECT symbol, quantity, average_cost FROM positions WHERE account_id = %s", (account_id,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error getting positions: {e}")
            return []
        finally:
            cursor.close()

    def get_order_history(self, account_id: int) -> List[Dict[str, Any]]:
        """
        Retrieves the entire order history for a specific account.
        :param account_id: The ID of the account.
        :return: A list of dictionaries representing the orders.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("""
                SELECT order_id, client_order_id, symbol, order_type, quantity, price, status, failure_reason, timestamp
                FROM orders
                WHERE account_id = %s
                ORDER BY timestamp DESC
            """, (account_id,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error getting order history: {e}")
            return []
        finally:
            cursor.close()
