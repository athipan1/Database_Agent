import os
import logging
import sys
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from tenacity import retry, stop_after_attempt, wait_exponential

# --- Project specific imports ---
# Add the project root to the python path to allow imports from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.database import SessionLocal, create_tables
    from src.models import Price
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
except ImportError as e:
    print(f"Error importing modules. Make sure you have installed all dependencies from requirements.txt")
    print(f"Import error: {e}")
    sys.exit(1)

# --- Configuration & Logging ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Environment Variable Loading ---
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Ingestion settings
SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,NVDA").split(',')
TIMEFRAME_STR = os.getenv("TIMEFRAME", "1Day")
DAYS_TO_FETCH = int(os.getenv("DAYS_TO_FETCH", "30"))

# --- Alpaca TimeFrame Mapping ---
# A simple mapper, can be extended
TIMEFRAME_MAP = {
    "1Day": TimeFrame(1, TimeFrameUnit.Day)
}
ALPACA_TIMEFRAME = TIMEFRAME_MAP.get(TIMEFRAME_STR)

# --- Validation ---
if not all([ALPACA_API_KEY, ALPACA_SECRET_KEY, DATABASE_URL, ALPACA_TIMEFRAME]):
    logging.error("Missing critical environment variables. Check your .env file or GitHub Secrets.")
    sys.exit(1)


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(3))
def fetch_market_data(symbols, timeframe, start_date, end_date):
    """
    Fetches historical market data from Alpaca API.
    Retries with exponential backoff on failure.
    """
    logging.info(f"Fetching data for {symbols} from {start_date} to {end_date}")
    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
    request_params = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=timeframe,
        start=start_date,
        end=end_date
    )
    bars = client.get_stock_bars(request_params)
    logging.info(f"Successfully fetched data for {len(bars.data)} bars.")
    return bars.df

@retry(wait=wait_exponential(multiplier=1, min=2, max=5), stop=stop_after_attempt(3))
def upsert_data(df: pd.DataFrame):
    """
    Inserts/updates data into the prices table using ON CONFLICT DO NOTHING.
    Retries with exponential backoff on database connection failure.
    """
    if df.empty:
        logging.info("DataFrame is empty. No data to insert.")
        return 0

    # Prepare data for insertion
    data_to_insert = df.to_dict(orient='records')

    table = Price.__table__
    stmt = insert(table).values(data_to_insert)

    # Use ON CONFLICT DO NOTHING to prevent duplicates
    stmt = stmt.on_conflict_do_nothing(
        index_elements=['symbol', 'timeframe', 'timestamp']
    )

    session = SessionLocal()
    try:
        logging.info(f"Attempting to UPSERT {len(data_to_insert)} rows...")
        result = session.execute(stmt)
        session.commit()
        logging.info(f"Successfully inserted/updated {result.rowcount} rows.")
        return result.rowcount
    except Exception as e:
        logging.error(f"Database error: {e}")
        session.rollback()
        raise  # Re-raise the exception to trigger tenacity retry
    finally:
        session.close()

def transform_data(df: pd.DataFrame, timeframe_str: str) -> pd.DataFrame:
    """
    Transforms the DataFrame from Alpaca to match the 'prices' table schema.
    """
    if df.empty:
        return df

    df = df.reset_index()
    df.rename(columns={'timestamp': 'timestamp', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}, inplace=True)
    df['timeframe'] = timeframe_str

    # Ensure correct column order for the database
    df = df[['symbol', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]
    return df

def main():
    """
    Main function to orchestrate the data ingestion process.
    """
    logging.info("Starting data ingestion process...")

    # 1. Ensure database tables are created
    create_tables()

    # 2. Define date range for data fetching
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_FETCH)

    # 3. Fetch data from Alpaca
    try:
        raw_df = fetch_market_data(SYMBOLS, ALPACA_TIMEFRAME, start_date, end_date)
    except Exception as e:
        logging.error(f"Failed to fetch data from Alpaca after multiple retries: {e}")
        sys.exit(1)

    # 4. Transform data
    transformed_df = transform_data(raw_df, TIMEFRAME_STR)

    # 5. Upsert data into the database
    try:
        rows_affected = upsert_data(transformed_df)
        logging.info(f"Ingestion process finished. Rows affected: {rows_affected}")
    except Exception as e:
        logging.error(f"Failed to save data to the database after multiple retries: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
