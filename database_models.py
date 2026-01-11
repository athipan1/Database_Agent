from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Numeric,
    BigInteger,
    ForeignKey,
    DateTime,
    UniqueConstraint,
    CheckConstraint,
    event,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.types import TypeDecorator, CHAR
import uuid
from datetime import datetime
import os

Base = declarative_base()

# Define a custom UUID type that works with both SQLite and PostgreSQL
class UUID(TypeDecorator):
    """Platform-independent UUID type."""
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value

class Account(Base):
    __tablename__ = 'accounts'
    account_id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(String, nullable=False, unique=True)
    cash_balance = Column(Numeric(18, 5), nullable=False)
    positions = relationship("Position", back_populates="account")
    orders = relationship("Order", back_populates="account")
    ledger_entries = relationship("Ledger", back_populates="account")

class Position(Base):
    __tablename__ = 'positions'
    position_id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('accounts.account_id'), nullable=False)
    symbol = Column(String, nullable=False)
    quantity = Column(BigInteger, nullable=False)
    average_cost = Column(Numeric(18, 5), nullable=False)
    account = relationship("Account", back_populates="positions")
    __table_args__ = (UniqueConstraint('account_id', 'symbol', name='_account_symbol_uc'),)

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True, autoincrement=True)
    client_order_id = Column(UUID, nullable=False, unique=True, default=uuid.uuid4)
    account_id = Column(Integer, ForeignKey('accounts.account_id'), nullable=False)
    symbol = Column(String, nullable=False)
    order_type = Column(String, nullable=False)
    quantity = Column(BigInteger, nullable=False)
    price = Column(Numeric(18, 5))
    status = Column(String, nullable=False)
    failure_reason = Column(String)
    correlation_id = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    account = relationship("Account", back_populates="orders")
    ledger_entry = relationship("Ledger", back_populates="order", uselist=False)
    __table_args__ = (
        CheckConstraint(order_type.in_(['BUY', 'SELL']), name='order_type_check'),
        CheckConstraint(status.in_(['pending', 'executed', 'cancelled', 'failed']), name='status_check'),
    )

class Ledger(Base):
    __tablename__ = 'ledger'
    entry_id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('accounts.account_id'), nullable=False)
    order_id = Column(Integer, ForeignKey('orders.order_id'), nullable=True)
    asset = Column(String, nullable=False)
    change = Column(Numeric(18, 5), nullable=False)
    new_balance = Column(Numeric(18, 5), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    description = Column(String)
    account = relationship("Account", back_populates="ledger_entries")
    order = relationship("Order", back_populates="ledger_entry")

class Price(Base):
    __tablename__ = 'prices'
    price_id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Numeric(18, 5), nullable=False)
    high = Column(Numeric(18, 5), nullable=False)
    low = Column(Numeric(18, 5), nullable=False)
    close = Column(Numeric(18, 5), nullable=False)
    volume = Column(BigInteger, nullable=False)
    __table_args__ = (UniqueConstraint('symbol', 'timestamp', name='_symbol_timestamp_uc'),)
