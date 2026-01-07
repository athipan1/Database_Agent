import os
import logging
import psycopg2
import psycopg2.extras
import sqlite3
from decimal import Decimal
from typing import Optional, Dict, Any, List
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
                # Using a file-based DB for tests can simplify debugging, but memory is faster
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
                    host=os.environ.get("POSTGRES_HOST") or "localhost",
                    port=os.environ.get("POSTGRES_PORT") or "5432"
                )
                logging.info(f"Successfully connected to PostgreSQL database.")
            except psycopg2.OperationalError as e:
                logging.error(f"Error connecting to PostgreSQL database: {e}")
                raise e

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
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id {pk_type},
                    client_order_id {uuid_type} NOT NULL UNIQUE,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id),
                    symbol TEXT NOT NULL,
                    order_type TEXT NOT NULL CHECK(order_type IN ('BUY', 'SELL')),
                    quantity BIGINT NOT NULL,
                    price {numeric_type},
                    status TEXT NOT NULL CHECK(status IN ('pending', 'executed', 'cancelled', 'failed')),
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
                    timestamp {timestamp_type} NOT NULL,
                    open {numeric_type} NOT NULL,
                    high {numeric_type} NOT NULL,
                    low {numeric_type} NOT NULL,
                    close {numeric_type} NOT NULL,
                    volume BIGINT NOT NULL,
                    UNIQUE (symbol, timestamp)
                );
            """)

            # Insert sample data for prices if it doesn't exist
            cursor.execute(f"SELECT * FROM prices WHERE symbol = {self.param_style}", ('AAPL',))
            if cursor.fetchone() is None:
                sample_prices = [
                    ('AAPL', '2025-01-01T10:00:00Z', '150.00', '152.00', '149.50', '151.50', 1000000),
                    ('AAPL', '2025-01-01T11:00:00Z', '151.50', '153.00', '151.00', '152.50', 1200000),
                    ('GOOG', '2025-01-01T10:00:00Z', '2800.00', '2810.00', '2795.00', '2805.00', 500000)
                ]
                for price_data in sample_prices:
                    cursor.execute(f"""
                        INSERT INTO prices (symbol, timestamp, open, high, low, close, volume)
                        VALUES ({self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style}, {self.param_style})
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

    def get_trade_history(self, account_id: int, limit: int = 50, offset: int = 0, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
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
        # Note: timeframe is not used in this MVP implementation.
        # A real implementation would require time-series aggregation logic.
        cursor = self.get_cursor()
        try:
            query = f"SELECT * FROM prices WHERE symbol = {self.param_style} ORDER BY timestamp DESC LIMIT {self.param_style}"
            cursor.execute(query, (symbol.upper(), limit))

            return [
                {
                    'symbol': row['symbol'],
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

    def _get_latest_price(self, symbol: str) -> Optional[Decimal]:
        cursor = self.get_cursor()
        try:
            query = f"SELECT close FROM prices WHERE symbol = {self.param_style} ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(query, (symbol.upper(),))
            result = cursor.fetchone()
            return self._to_decimal(result['close']) if result else None
        finally:
            cursor.close()

    def get_portfolio_metrics(self, account_id: int) -> Optional[Dict[str, Any]]:
        cash_balance = self.get_account_balance(account_id)
        if cash_balance is None:
            return None

        positions_list = self.get_positions(account_id)

        total_market_value = Decimal('0')
        total_unrealized_pnl = Decimal('0')
        positions_metrics = []

        for pos in positions_list:
            symbol = pos['symbol']
            quantity = pos['quantity']
            avg_cost = self._to_decimal(pos['average_cost'])

            market_price = self._get_latest_price(symbol)
            if market_price is None:
                # If no price is available, we can't value this position.
                # Skip it or use a default value. Here we skip.
                continue

            market_value = quantity * market_price
            unrealized_pnl = (market_price - avg_cost) * quantity

            total_market_value += market_value
            total_unrealized_pnl += unrealized_pnl

            positions_metrics.append({
                "symbol": symbol,
                "quantity": quantity,
                "avg_cost": avg_cost,
                "market_price": market_price,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                 # NOTE: asset_id not in current schema
                "asset_id": None
            })

        total_portfolio_value = cash_balance + total_market_value

        # NOTE: Realized PnL calculation is complex and requires full transaction history analysis.
        # Returning 0.00 for this MVP.
        realized_pnl = Decimal('0.00')

        return {
            "account_id": account_id,
            "as_of": datetime.now(timezone.utc).isoformat(),
            "total_portfolio_value": total_portfolio_value,
            "cash_balance": cash_balance,
            "unrealized_pnl": total_unrealized_pnl,
            "realized_pnl": realized_pnl,
            "positions": positions_metrics
        }
