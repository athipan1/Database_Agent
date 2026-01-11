import os
import logging
from decimal import Decimal
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from sqlalchemy import create_engine, desc, and_, or_
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.exc import IntegrityError, NoResultFound
from contextlib import contextmanager

from database_models import Base, Account, Position, Order, Ledger, Price

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradingDB:
    """
    A class to manage the database for the trading robot.
    It handles database connection, schema creation, and all trading operations
    with a strong focus on transaction safety and data integrity.
    It uses SQLAlchemy ORM to support both PostgreSQL and SQLite.
    """
    def __init__(self):
        """
        Initializes the TradingDB object and connects to the database.
        """
        self.engine = None
        self.db_type = 'sqlite' if os.environ.get('USE_SQLITE') else 'postgres'

        if self.db_type == 'sqlite':
            db_url = 'sqlite:///trading.db'
            self.engine = create_engine(db_url, connect_args={'check_same_thread': False})
            logging.info("Successfully configured for SQLite database.")
        else:
            db_url = (
                f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@"
                f"{os.environ.get('POSTGRES_HOST', 'localhost')}:{os.environ.get('POSTGRES_PORT', '5432')}/"
                f"{os.environ.get('POSTGRES_DB')}"
            )
            self.engine = create_engine(db_url)
            logging.info("Successfully configured for PostgreSQL database.")

        # Session factory
        self.SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    @contextmanager
    def get_session(self) -> Session:
        """Provide a transactional scope around a series of operations."""
        session = self.SessionFactory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_order(self, account_id: int, client_order_id: str, symbol: str, order_type: str, quantity: int, price: Decimal, correlation_id: str) -> Optional[int]:
        with self.get_session() as session:
            try:
                # Check for idempotency
                existing_order = session.query(Order).filter(Order.client_order_id == client_order_id).first()
                if existing_order:
                    return existing_order.order_id

                new_order = Order(
                    account_id=account_id,
                    client_order_id=client_order_id,
                    symbol=symbol.upper(),
                    order_type=order_type.upper(),
                    quantity=quantity,
                    price=price,
                    status='pending',
                    correlation_id=correlation_id
                )
                session.add(new_order)
                session.flush() # Flush to get the new order_id
                return new_order.order_id
            except IntegrityError:
                # This could happen in a race condition, so we double-check.
                session.rollback()
                existing_order = session.query(Order).filter(Order.client_order_id == client_order_id).first()
                return existing_order.order_id if existing_order else None


    def execute_order(self, order_id: int) -> (str, Optional[str]):
        with self.get_session() as session:
            try:
                order = session.query(Order).with_for_update().filter(Order.order_id == order_id).one()

                if order.status != 'pending':
                    return 'failed', 'invalid_state'

                account = session.query(Account).with_for_update().filter(Account.account_id == order.account_id).one()

                total_cost = order.quantity * order.price

                if order.order_type == 'BUY':
                    if account.cash_balance < total_cost:
                        order.status = 'failed'
                        order.failure_reason = 'insufficient_funds'
                        return 'failed', 'insufficient_funds'

                    account.cash_balance -= total_cost
                    self._update_position_and_ledger_on_buy(session, account, order)

                elif order.order_type == 'SELL':
                    position = session.query(Position).with_for_update().filter(
                        Position.account_id == order.account_id,
                        Position.symbol == order.symbol
                    ).first()

                    if not position or position.quantity < order.quantity:
                        order.status = 'failed'
                        order.failure_reason = 'insufficient_shares'
                        return 'failed', 'insufficient_shares'

                    account.cash_balance += total_cost
                    self._update_position_and_ledger_on_sell(session, position, order)

                order.status = 'executed'
                return 'executed', None

            except NoResultFound:
                return 'failed', 'order_not_found'
            except Exception as e:
                logging.error(f"Failed to execute order {order_id}: {e}", exc_info=True)
                raise

    def _update_position_and_ledger_on_buy(self, session: Session, account: Account, order: Order):
        position = session.query(Position).with_for_update().filter(
            Position.account_id == account.account_id,
            Position.symbol == order.symbol
        ).first()

        if position:
            new_avg_cost = ((position.average_cost * position.quantity) + (order.price * order.quantity)) / (position.quantity + order.quantity)
            position.quantity += order.quantity
            position.average_cost = new_avg_cost
        else:
            position = Position(
                account_id=account.account_id,
                symbol=order.symbol,
                quantity=order.quantity,
                average_cost=order.price
            )
            session.add(position)

        # Ledger for cash
        session.add(Ledger(
            account_id=account.account_id,
            order_id=order.order_id,
            asset='CASH',
            change=-(order.quantity * order.price),
            new_balance=account.cash_balance,
            description=f"BUY {order.quantity} {order.symbol}"
        ))
        # Ledger for asset
        session.add(Ledger(
            account_id=account.account_id,
            order_id=order.order_id,
            asset=order.symbol,
            change=order.quantity,
            new_balance=position.quantity,
            description=f"BUY {order.quantity} {order.symbol}"
        ))

    def _update_position_and_ledger_on_sell(self, session: Session, position: Position, order: Order):
        position.quantity -= order.quantity

        # Ledger for cash
        session.add(Ledger(
            account_id=order.account_id,
            order_id=order.order_id,
            asset='CASH',
            change=(order.quantity * order.price),
            new_balance=position.account.cash_balance, # account balance already updated
            description=f"SELL {order.quantity} {order.symbol}"
        ))
        # Ledger for asset
        session.add(Ledger(
            account_id=order.account_id,
            order_id=order.order_id,
            asset=order.symbol,
            change=-order.quantity,
            new_balance=position.quantity,
            description=f"SELL {order.quantity} {order.symbol}"
        ))

        if position.quantity == 0:
            session.delete(position)

    def get_account_balance(self, account_id: int) -> Optional[Decimal]:
        with self.get_session() as session:
            account = session.query(Account).filter(Account.account_id == account_id).first()
            return account.cash_balance if account else None

    def get_positions(self, account_id: int) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            positions = session.query(Position).filter(Position.account_id == account_id).all()
            return [
                {
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "average_cost": p.average_cost
                } for p in positions
            ]

    def get_order_history(self, account_id: int) -> List[Order]:
        with self.get_session() as session:
            orders = session.query(Order).filter(Order.account_id == account_id).order_by(desc(Order.timestamp)).all()
            session.expunge_all()
            return orders

    def get_trade_history(self, account_id: int, limit: int = 50, offset: int = 0, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            query = session.query(Order).filter(
                Order.account_id == account_id,
                Order.status == 'executed'
            )
            if start_date:
                query = query.filter(Order.timestamp >= start_date)
            if end_date:
                query = query.filter(Order.timestamp <= end_date)

            trades = query.order_by(desc(Order.timestamp), desc(Order.order_id)).limit(limit).offset(offset).all()

            return [
                {
                    "trade_id": t.order_id,
                    "account_id": t.account_id,
                    "symbol": t.symbol,
                    "side": t.order_type.lower(),
                    "quantity": t.quantity,
                    "price": t.price,
                    "notional": t.price * t.quantity,
                    "executed_at": t.timestamp.isoformat(),
                    "asset_id": None,
                    "source_agent": None
                } for t in trades
            ]

    def get_price_history(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            prices = session.query(Price).filter(Price.symbol == symbol.upper()).order_by(desc(Price.timestamp)).limit(limit).all()
            return [
                {
                    'symbol': p.symbol,
                    'timestamp': p.timestamp.isoformat(),
                    'open': p.open,
                    'high': p.high,
                    'low': p.low,
                    'close': p.close,
                    'volume': p.volume,
                } for p in prices
            ]

    def add_price(self, price_data: Dict[str, Any]) -> Price:
        with self.get_session() as session:
            # Check for existing price to ensure idempotency
            existing_price = session.query(Price).filter(
                Price.symbol == price_data['symbol'].upper(),
                Price.timestamp == price_data['timestamp']
            ).first()
            if existing_price:
                return existing_price

            new_price = Price(**price_data)
            session.add(new_price)
            session.flush()
            return new_price

    def _get_latest_price(self, session: Session, symbol: str) -> Optional[Decimal]:
        price = session.query(Price.close).filter(Price.symbol == symbol.upper()).order_by(desc(Price.timestamp)).first()
        return price.close if price else None

    def get_portfolio_metrics(self, account_id: int) -> Optional[Dict[str, Any]]:
        with self.get_session() as session:
            account = session.query(Account).filter(Account.account_id == account_id).first()
            if not account:
                return None

            positions = session.query(Position).filter(Position.account_id == account_id).all()

            total_market_value = Decimal('0')
            total_unrealized_pnl = Decimal('0')
            positions_metrics = []

            for pos in positions:
                market_price = self._get_latest_price(session, pos.symbol)
                if market_price is None:
                    continue

                market_value = pos.quantity * market_price
                unrealized_pnl = (market_price - pos.average_cost) * pos.quantity

                total_market_value += market_value
                total_unrealized_pnl += unrealized_pnl

                positions_metrics.append({
                    "symbol": pos.symbol,
                    "quantity": pos.quantity,
                    "avg_cost": pos.average_cost,
                    "market_price": market_price,
                    "market_value": market_value,
                    "unrealized_pnl": unrealized_pnl,
                    "asset_id": None
                })

            total_portfolio_value = account.cash_balance + total_market_value
            realized_pnl = Decimal('0.00') # Placeholder

            return {
                "account_id": account_id,
                "as_of": datetime.now(timezone.utc).isoformat(),
                "total_portfolio_value": total_portfolio_value,
                "cash_balance": account.cash_balance,
                "unrealized_pnl": total_unrealized_pnl,
                "realized_pnl": realized_pnl,
                "positions": positions_metrics
            }
