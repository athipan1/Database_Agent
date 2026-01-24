import os
import logging
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

class AlpacaClient:
    """
    A client for interacting with the Alpaca API, with built-in retry logic.
    This client uses the modern alpaca-py library.
    """
    def __init__(self, api_key: str, secret_key: str):
        if not api_key or not secret_key:
            raise ValueError("API key and secret key cannot be empty.")
        # sandbox=True is used for paper trading
        self.client = StockHistoricalDataClient(api_key, secret_key, sandbox=True)
        logging.info("Alpaca API client (alpaca-py) initialized for paper trading.")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logging.warning(
            f"Retrying Alpaca API call due to: {retry_state.outcome.exception()}. "
            f"Attempt #{retry_state.attempt_number}..."
        )
    )
    def fetch_historical_prices(self, symbol: str, timeframe_str: str, start_date: str, end_date: str):
        """
        Fetches historical OHLCV data from Alpaca for a given symbol and timeframe.

        Args:
            symbol (str): The stock symbol (e.g., 'GOOG').
            timeframe_str (str): The timeframe for the bars ('4h', '1d').
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.

        Returns:
            list[dict]: A list of dictionaries, where each dictionary represents a price bar.
                        Returns an empty list if there's an error or no data.
        """
        logging.info(f"Fetching historical data for {symbol} with timeframe {timeframe_str} from {start_date} to {end_date}.")
        try:
            # Map our string timeframe to the Alpaca SDK's Enum
            timeframe_map = {
                '4h': TimeFrame.Hour, # Note: Alpaca API might not support 4H directly.
                '1d': TimeFrame.Day,
            }
            if timeframe_str.lower() == '4h':
                # Alpaca's get_bars doesn't directly support '4H'.
                # We fetch '1H' data as a workaround.
                logging.warning("Alpaca API does not directly support '4H' timeframe. Fetching '1H' data instead.")
                alpaca_timeframe = TimeFrame.Hour
            elif timeframe_str.lower() == '1d':
                 alpaca_timeframe = TimeFrame.Day
            else:
                logging.error(f"Unsupported timeframe: {timeframe_str}")
                return []

            request_params = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=alpaca_timeframe,
                start=start_date,
                end=end_date
            )

            bars = self.client.get_stock_bars(request_params).df

            if bars.empty:
                logging.warning(f"No data returned for {symbol} in the given date range.")
                return []

            # Data comes in a multi-index DataFrame, reset index to work with it
            bars.reset_index(inplace=True)

            # Rename columns to match our database schema
            bars.rename(columns={
                'symbol': 'symbol_col', # Avoid clash with our own 'symbol'
                'timestamp': 'timestamp_col',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            }, inplace=True)

            # Format for database insertion
            bars['symbol'] = symbol
            bars['timeframe'] = timeframe_str
            bars['timestamp'] = bars['timestamp_col'].apply(lambda ts: ts.isoformat())

            # Select and reorder columns
            formatted_data = bars[[
                'symbol', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume'
            ]].to_dict('records')

            logging.info(f"Successfully fetched {len(formatted_data)} data points for {symbol}.")
            return formatted_data

        except Exception as e:
            logging.error(f"Failed to fetch historical data for {symbol}: {e}", exc_info=True)
            raise

# Example usage:
if __name__ == '__main__':
    load_dotenv()
    logging.basicConfig(level=logging.INFO)

    API_KEY = os.getenv("ALPACA_API_KEY")
    SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

    if not API_KEY or not SECRET_KEY:
        print("Please set ALPACA_API_KEY and ALPACA_SECRET_KEY environment variables.")
    else:
        client = AlpacaClient(API_KEY, SECRET_KEY)

        # Calculate dates for the last 2 years
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=2*365)

        # Fetch data
        historical_data = client.fetch_historical_prices(
            'GOOG',
            '1d',
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d')
        )

        if historical_data:
            print(f"Fetched {len(historical_data)} records.")
            print("First 5 records:")
            for record in historical_data[:5]:
                print(record)
