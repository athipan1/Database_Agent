"""Historical market-data ingestion and development fallback helpers."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List

from models import PortfolioMetrics, Price


DEFAULT_INGESTION_SYMBOLS = ("GOOG", "AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META")
DEFAULT_INGESTION_TIMEFRAMES = ("1h", "1d")


def build_mock_price_history(symbol: str, limit: int = 100) -> List[Price]:
    now = datetime.now(timezone.utc)
    count = max(1, min(int(limit or 100), 500))
    base = Decimal("100")
    prices: List[Price] = []
    for idx in range(count):
        close = base + Decimal(idx) * Decimal("0.10")
        prices.append(
            Price(
                symbol=symbol.upper(),
                timestamp=now - timedelta(hours=count - idx),
                open=close - Decimal("0.05"),
                high=close + Decimal("0.15"),
                low=close - Decimal("0.15"),
                close=close,
                volume=1000 + idx,
            )
        )
    return prices


def build_default_portfolio_metrics() -> PortfolioMetrics:
    return PortfolioMetrics(
        win_rate=0.0,
        average_return=0.0,
        max_drawdown=0.0,
        sharpe_ratio=0.0,
    )


def ingest_data_for_symbol_timeframe(
    db,
    alpaca_client,
    symbol: str,
    timeframe: str,
    start_date: str,
    end_date: str,
) -> None:
    try:
        price_data = alpaca_client.fetch_historical_prices(
            symbol,
            timeframe,
            start_date,
            end_date,
        )
        if price_data:
            db.ingest_historical_prices(price_data)
        else:
            logging.warning("No price data to ingest for %s (%s).", symbol, timeframe)
    except Exception as exc:
        logging.error(
            "Failed to ingest data for %s (%s): %s",
            symbol,
            timeframe,
            exc,
            exc_info=True,
        )


def run_ingestion_job(
    db,
    alpaca_client,
    *,
    symbols: Iterable[str] = DEFAULT_INGESTION_SYMBOLS,
    timeframes: Iterable[str] = DEFAULT_INGESTION_TIMEFRAMES,
) -> None:
    logging.info("Scheduler starting historical data ingestion job.")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=2 * 365)).strftime("%Y-%m-%d")
    tasks = [(symbol, timeframe) for symbol in symbols for timeframe in timeframes]
    max_workers = min(len(tasks), 10)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol, timeframe in tasks:
            executor.submit(
                ingest_data_for_symbol_timeframe,
                db,
                alpaca_client,
                symbol,
                timeframe,
                start_date,
                end_date,
            )
    logging.info("Scheduler finished historical data ingestion job.")
