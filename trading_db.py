import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradingDB:
    """
    A class to manage the SQLite database for the trading robot.
    It handles database connection, schema creation, and all trading operations.
    """
    def __init__(self, db_file="trading.db"):
        """
        Initializes the TradingDB object and connects to the SQLite database.

        :param db_file: The path to the SQLite database file.
        """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_file, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row # Allows accessing columns by name
            logging.info(f"Successfully connected to database: {db_file}")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database: {e}")
            raise e

    def __del__(self):
        """
        Destructor to close the database connection when the object is destroyed.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def get_cursor(self):
        """Returns a cursor object."""
        return self.conn.cursor()

    def setup_database(self):
        """
        Creates the necessary tables if they don't exist and initializes
        the default account.
        """
        cursor = self.get_cursor()
        try:
            # Create accounts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_name TEXT NOT NULL UNIQUE,
                    cash_balance REAL NOT NULL
                );
            """)
            logging.info("Table 'accounts' created or already exists.")

            # Create positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    average_cost REAL NOT NULL,
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id),
                    UNIQUE (account_id, symbol)
                );
            """)
            logging.info("Table 'positions' created or already exists.")

            # Create orders table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    order_type TEXT NOT NULL CHECK(order_type IN ('BUY', 'SELL')),
                    quantity INTEGER NOT NULL,
                    price REAL,
                    status TEXT NOT NULL CHECK(status IN ('pending', 'executed', 'cancelled', 'failed')),
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES accounts (account_id)
                );
            """)
            logging.info("Table 'orders' created or already exists.")

            # Create a default account if it doesn't exist
            cursor.execute("SELECT * FROM accounts WHERE account_name = ?", ('main_account',))
            if cursor.fetchone() is None:
                cursor.execute("INSERT INTO accounts (account_name, cash_balance) VALUES (?, ?)",
                               ('main_account', 1000000.0))
                logging.info("Created default 'main_account' with 1,000,000 cash balance.")

            self.conn.commit()
            logging.info("Database setup completed successfully.")
        except sqlite3.Error as e:
            logging.error(f"Error setting up database: {e}")
            self.conn.rollback()
            raise e

    def create_order(self, account_id, symbol, order_type, quantity, price):
        """
        Creates a new order with 'pending' status.

        :param account_id: The ID of the account placing the order.
        :param symbol: The stock symbol (e.g., 'AAPL').
        :param order_type: 'BUY' or 'SELL'.
        :param quantity: The number of shares.
        :param price: The price per share.
        :return: The ID of the newly created order, or None on failure.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("""
                INSERT INTO orders (account_id, symbol, order_type, quantity, price, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
            """, (account_id, symbol.upper(), order_type.upper(), quantity, price))
            self.conn.commit()
            order_id = cursor.lastrowid
            logging.info(f"Created pending {order_type} order for {quantity} {symbol} @ {price}. Order ID: {order_id}")
            return order_id
        except sqlite3.Error as e:
            logging.error(f"Failed to create order: {e}")
            self.conn.rollback()
            return None

    def execute_order(self, order_id):
        """
        Executes a pending order, updating account balance and positions.
        This operation is transactional.

        :param order_id: The ID of the order to execute.
        """
        cursor = self.get_cursor()
        try:
            # Fetch the order
            cursor.execute("SELECT * FROM orders WHERE order_id = ? AND status = 'pending'", (order_id,))
            order = cursor.fetchone()

            if not order:
                logging.warning(f"Order {order_id} not found or not pending. Cannot execute.")
                return

            account_id = order['account_id']
            symbol = order['symbol']
            order_type = order['order_type']
            quantity = order['quantity']
            price = order['price']
            total_cost = quantity * price

            logging.info(f"Executing {order_type} order {order_id} for {quantity} {symbol} @ {price}")

            # --- Start Transaction ---
            self.conn.execute('BEGIN')

            # Get account balance
            cursor.execute("SELECT cash_balance FROM accounts WHERE account_id = ?", (account_id,))
            account = cursor.fetchone()
            cash_balance = account['cash_balance']

            if order_type == 'BUY':
                if cash_balance < total_cost:
                    self._update_order_status(cursor, order_id, 'failed', "Insufficient funds")
                else:
                    # Update account balance
                    new_balance = cash_balance - total_cost
                    cursor.execute("UPDATE accounts SET cash_balance = ? WHERE account_id = ?", (new_balance, account_id))

                    # Update position
                    self._update_position_on_buy(cursor, account_id, symbol, quantity, price)

                    # Update order status
                    self._update_order_status(cursor, order_id, 'executed')

            elif order_type == 'SELL':
                # Get current position
                cursor.execute("SELECT * FROM positions WHERE account_id = ? AND symbol = ?", (account_id, symbol))
                position = cursor.fetchone()

                if not position or position['quantity'] < quantity:
                    self._update_order_status(cursor, order_id, 'failed', "Insufficient shares to sell")
                else:
                    # Update account balance
                    new_balance = cash_balance + total_cost
                    cursor.execute("UPDATE accounts SET cash_balance = ? WHERE account_id = ?", (new_balance, account_id))

                    # Update position
                    self._update_position_on_sell(cursor, position, quantity)

                    # Update order status
                    self._update_order_status(cursor, order_id, 'executed')

            self.conn.commit()
            # --- End Transaction ---

        except sqlite3.Error as e:
            logging.error(f"Failed to execute order {order_id}: {e}")
            self.conn.rollback()
            self._update_order_status(self.get_cursor(), order_id, 'failed', str(e)) # Try to mark as failed out of transaction
            self.conn.commit()


    def _update_order_status(self, cursor, order_id, status, reason=""):
        """Helper to update order status and log it."""
        cursor.execute("UPDATE orders SET status = ? WHERE order_id = ?", (status, order_id))
        logging.info(f"Order {order_id} status updated to '{status}'. {reason}".strip())

    def _update_position_on_buy(self, cursor, account_id, symbol, quantity, price):
        """Helper to update or create a position after a buy."""
        cursor.execute("SELECT * FROM positions WHERE account_id = ? AND symbol = ?", (account_id, symbol))
        position = cursor.fetchone()

        if position:
            # Update existing position
            new_quantity = position['quantity'] + quantity
            new_avg_cost = ((position['average_cost'] * position['quantity']) + (price * quantity)) / new_quantity
            cursor.execute("""
                UPDATE positions SET quantity = ?, average_cost = ?
                WHERE position_id = ?
            """, (new_quantity, new_avg_cost, position['position_id']))
        else:
            # Create new position
            cursor.execute("""
                INSERT INTO positions (account_id, symbol, quantity, average_cost)
                VALUES (?, ?, ?, ?)
            """, (account_id, symbol, quantity, price))

    def _update_position_on_sell(self, cursor, position, sell_quantity):
        """Helper to update or delete a position after a sell."""
        if position['quantity'] == sell_quantity:
            # Delete position if all shares are sold
            cursor.execute("DELETE FROM positions WHERE position_id = ?", (position['position_id'],))
        else:
            # Update quantity for a partial sell
            new_quantity = position['quantity'] - sell_quantity
            cursor.execute("UPDATE positions SET quantity = ? WHERE position_id = ?", (new_quantity, position['position_id']))

    def get_account_balance(self, account_id):
        """
        Retrieves the cash balance for a specific account.

        :param account_id: The ID of the account.
        :return: The cash balance as a float, or None if account not found.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("SELECT cash_balance FROM accounts WHERE account_id = ?", (account_id,))
            result = cursor.fetchone()
            return result['cash_balance'] if result else None
        except sqlite3.Error as e:
            logging.error(f"Error getting account balance: {e}")
            return None

    def get_positions(self, account_id):
        """
        Retrieves all positions for a specific account.

        :param account_id: The ID of the account.
        :return: A list of dictionaries representing the positions.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("SELECT symbol, quantity, average_cost FROM positions WHERE account_id = ?", (account_id,))
            positions = [dict(row) for row in cursor.fetchall()]
            return positions
        except sqlite3.Error as e:
            logging.error(f"Error getting positions: {e}")
            return []

    def get_order_history(self, account_id):
        """
        Retrieves the entire order history for a specific account.

        :param account_id: The ID of the account.
        :return: A list of dictionaries representing the orders.
        """
        cursor = self.get_cursor()
        try:
            cursor.execute("""
                SELECT order_id, symbol, order_type, quantity, price, status, timestamp
                FROM orders
                WHERE account_id = ?
                ORDER BY timestamp DESC
            """, (account_id,))
            orders = [dict(row) for row in cursor.fetchall()]
            return orders
        except sqlite3.Error as e:
            logging.error(f"Error getting order history: {e}")
            return []

if __name__ == '__main__':
    """
    An example usage script to demonstrate the functionality of the TradingDB class.
    This will:
    1. Create a fresh database file `example.db`.
    2. Set up the tables and a default account.
    3. Perform a series of valid and invalid transactions.
    4. Print the final state of the database.
    """
    import os
    db_file = "example.db"
    if os.path.exists(db_file):
        os.remove(db_file) # Start with a clean slate
        logging.info(f"Removed old database file '{db_file}'.")

    db = TradingDB(db_file=db_file)
    db.setup_database()

    # The default account_id is 1
    ACCOUNT_ID = 1

    def print_status():
        print("\n" + "="*50)
        balance = db.get_account_balance(ACCOUNT_ID)
        print(f"Account Balance: ${balance:,.2f}")

        positions = db.get_positions(ACCOUNT_ID)
        print("Current Positions:")
        if positions:
            for p in positions:
                print(f"  - {p['symbol']}: {p['quantity']} shares @ avg cost ${p['average_cost']:.2f}")
        else:
            print("  - No positions held.")

        history = db.get_order_history(ACCOUNT_ID)
        print("Order History:")
        if history:
            for o in history:
                print(f"  - ID: {o['order_id']}, {o['order_type']} {o['symbol']} {o['quantity']} @ ${o['price']:.2f}, Status: {o['status']}")
        else:
            print("  - No order history.")
        print("="*50 + "\n")

    # --- Start Simulation ---
    print("--- Initial State ---")
    print_status()

    # 1. Successful Buy
    print("\n--- Step 1: Submitting a valid BUY order for 10 AAPL @ $150.00 ---")
    buy_order_id_1 = db.create_order(ACCOUNT_ID, 'AAPL', 'BUY', 10, 150.00)
    db.execute_order(buy_order_id_1)
    print_status()

    # 2. Another successful Buy (updates existing position)
    print("\n--- Step 2: Submitting another valid BUY order for 5 AAPL @ $160.00 ---")
    buy_order_id_2 = db.create_order(ACCOUNT_ID, 'AAPL', 'BUY', 5, 160.00)
    db.execute_order(buy_order_id_2)
    print("Average cost for AAPL should be updated.")
    print_status()

    # 3. Successful Sell
    print("\n--- Step 3: Submitting a valid SELL order for 8 AAPL @ $170.00 ---")
    sell_order_id_1 = db.create_order(ACCOUNT_ID, 'AAPL', 'SELL', 8, 170.00)
    db.execute_order(sell_order_id_1)
    print_status()

    # 4. Failed Buy (Insufficient Funds)
    print("\n--- Step 4: Submitting a BUY order that should FAIL (insufficient funds) ---")
    buy_order_id_fail = db.create_order(ACCOUNT_ID, 'GOOG', 'BUY', 100, 99999.99)
    db.execute_order(buy_order_id_fail)
    print_status()

    # 5. Failed Sell (Insufficient Shares)
    print("\n--- Step 5: Submitting a SELL order that should FAIL (insufficient shares) ---")
    sell_order_id_fail = db.create_order(ACCOUNT_ID, 'AAPL', 'SELL', 100, 200.00) # We only have 7 shares left
    db.execute_order(sell_order_id_fail)
    print_status()

    # 6. Buy a different stock
    print("\n--- Step 6: Submitting a valid BUY order for a new stock (MSFT) ---")
    buy_msft_id = db.create_order(ACCOUNT_ID, 'MSFT', 'BUY', 20, 300.00)
    db.execute_order(buy_msft_id)
    print_status()
