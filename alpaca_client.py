import os
import logging
from datetime import datetime, timedelta
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class AlpacaClient:
    """
    A client for interacting with the Alpaca API, with built-in retry logic.
    """
    def __init__(self, api_key: str, secret_key: str):
        if not api_key or not secret_key:
            raise ValueError("API key and secret key cannot be empty.")
        self.api = tradeapi.REST(api_key, secret_key, base_url='https://paper-api.alpaca.markets')
        logging.info("Alpaca API client initialized for paper trading.")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: logging.warning(
            f"Retrying Alpaca API call due to: {retry_state.outcome.exception()}. "
            f"Attempt #{retry_state.attempt_number}..."
        )
    )
    def fetch_historical_prices(self, symbol: str, timeframe: str, start_date: str, end_date: str):
        """
        Fetches historical OHLCV data from Alpaca for a given symbol and timeframe.

        Args:
            symbol (str): The stock symbol (e.g., 'GOOG').
            timeframe (str): The timeframe for the bars ('4h', '1d').
            start_date (str): The start date in 'YYYY-MM-DD' format.
            end_date (str): The end date in 'YYYY-MM-DD' format.

        Returns:
            list[dict]: A list of dictionaries, where each dictionary represents a price bar.
                        Returns an empty list if there's an error or no data.
        """
        logging.info(f"Fetching historical data for {symbol} with timeframe {timeframe} from {start_date} to {end_date}.")
        try:
            # Map our string timeframe to the Alpaca SDK's Enum
            timeframe_map = {
                '4h': TimeFrame.Hour, # Note: Alpaca API might not support 4H directly, will need adjustment if so.
                '1d': TimeFrame.Day,
            }
            if timeframe.lower() == '4h':
                # Alpaca's get_bars doesn't directly support '4H'.
                # A common workaround is to fetch 1H data and resample, but for simplicity,
                # we'll log a warning and fetch '1H' data instead for now.
                logging.warning("Alpaca API does not directly support '4H' timeframe. Fetching '1H' data instead.")
                alpaca_timeframe = TimeFrame.Hour
            elif timeframe.lower() == '1d':
                 alpaca_timeframe = TimeFrame.Day
            else:
                logging.error(f"Unsupported timeframe: {timeframe}")
                return []


            bars = self.api.get_bars(
                symbol,
                alpaca_timeframe,
                start=start_date,
                end=end_date,
                adjustment='raw'
            ).df

            if bars.empty:
                logging.warning(f"No data returned for {symbol} in the given date range.")
                return []

            # Rename columns to match our database schema
            bars.rename(columns={
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume'
            }, inplace=True)

            # Add symbol and timeframe, and format for database insertion
            bars['symbol'] = symbol
            bars['timeframe'] = timeframe
            bars.reset_index(inplace=True) # Convert timestamp index to column
            bars = bars.rename(columns={'timestamp': 'timestamp_col'}) # Avoid name clash
            bars['timestamp'] = bars['timestamp_col'].apply(lambda ts: ts.isoformat())

            # Select and reorder columns
            formatted_data = bars[[
                'symbol', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume'
            ]].to_dict('records')

            logging.info(f"Successfully fetched {len(formatted_data)} data points for {symbol}.")
            return formatted_data

        except Exception as e:
            logging.error(f"Failed to fetch historical data for {symbol}: {e}", exc_info=True)
            # The retry decorator will handle retrying. If all retries fail, it will re-raise.
            raise

from dotenv import load_dotenv

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
