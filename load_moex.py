import os
from datetime import datetime, timedelta
import random

import pandas as pd
from tqdm import tqdm

import asyncio
import aiohttp

global START_DATE

TICKERS = ['GAZP', 'GMKN', 'X5', 'LKOH', 'MOEX', 'NLMK', 'NVTK', 'PLZL', 'ROSN', 'SBER', 'CHMF', 'SNGS', 'T', 'TATN', 'YDEX']

INTERVALS = ['10', '60', '24']

END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

FILE_PATH = 'moex.parquet'


def split_date_ranges(start_date_str, end_date_str, delta_days=3):
    """
    Splits a date range into smaller consecutive sub-ranges each spanning up to delta_days.
    MOEX does not allow data to be obtained for more than 43 days in a single request.
    Example:
        >>> split_date_ranges('2024-01-01', '2024-02-15', delta_days=20)
        [('2024-01-01', '2024-01-20'), ('2024-01-21', '2024-02-09'), ('2024-02-10', '2024-02-15')]
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    ranges = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=delta_days - 1), end_date)
        ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
        current_start = current_end + timedelta(days=1)
    return ranges


async def get_history_df_with_session(ticker, start_date, end_date, interval, session):
    """
    Fetches historical stock candle data asynchronously from the MOEX ISS API for a 
    given ticker and date range using an existing HTTP session.
    
    Date format: YYYY-MM-DD
    intervals: 1 - 1 minute
               10 - 10 minutes
               60 - 1 hour
               24 - 1 day
               7 - 1 week
               31 - 1 month
               4 - 1 quarter
    """
    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json?from={start_date}&till={end_date}&interval={interval}"
    async with session.get(url) as response:
        json_data = await response.json()
        candles_data = json_data.get('candles', {}).get('data', [])
        df = pd.DataFrame(candles_data, columns=["open", "close", "high", "low", "value", "volume", "begin", "end"])
        df['ticker'] = ticker
        df['interval'] = interval
        return df


async def fetch_for_ticker_interval(ticker, interval, start_date, end_date, session):
    """
    Asynchronously fetches historical candle data for a specific ticker and interval over a date range,
    splitting the date range into smaller chunks (default 43 days) to comply with API limits,
    and concatenates the results into a single pandas DataFrame.

    Between each chunk request, waits for a random delay between 0.5 and 1.2 seconds to reduce server load.
    """
    date_ranges = split_date_ranges(start_date, end_date)
    dfs = []
    for dr in date_ranges:
        df = await get_history_df_with_session(ticker, dr[0], dr[1], interval, session)
        if not df.empty:
            dfs.append(df)
        await asyncio.sleep(random.uniform(0.01, 0.05))
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


async def main():
    """
    The core function executing the data retrieval process from the MOEX API.
    It performs the following steps:
    
    1. Checks if the parquet data file exists and loads it if available, updating the start date based on the latest data.
    2. Sets the start date for data download, either from the latest date in the existing dataset or from a default date.
    3. Opens an asynchronous HTTP session to perform multiple requests concurrently.
    4. Iterates over a predefined list of stock tickers and intervals, fetching historical candle data for each combination.
    5. Concatenates newly fetched data with existing data.
    6. Saves the aggregated DataFrame back into a parquet file for persistence.
    
    Execution is handled asynchronously through `asyncio.run()`, allowing multiple network requests to run concurrently for efficiency.
    """
    if os.path.exists(FILE_PATH) and os.path.isfile(FILE_PATH):
        df_moex = pd.read_parquet(FILE_PATH)
        df_moex['end'] = pd.to_datetime(df_moex['end'])
        max_date = df_moex['end'].dt.floor('D').max()
        df_moex = df_moex[df_moex['end'].dt.floor('D') < max_date]
        START_DATE = max_date.strftime('%Y-%m-%d')


    else:
        df_moex = pd.DataFrame(columns=["open", "close", "high", "low", "value", "volume", "begin", "end", "ticker", "interval"])
        START_DATE = '2022-10-01'
    
    print(f"Start date: {START_DATE}")
    async with aiohttp.ClientSession() as session:
        for ticker in tqdm(TICKERS, desc="Tickers"):
            for interval in INTERVALS:
                new_data = await fetch_for_ticker_interval(ticker, interval, START_DATE, END_DATE, session)
                if not new_data.empty:
                    df_moex = pd.concat([df_moex, new_data], ignore_index=True)

    df_moex.to_parquet(FILE_PATH, index=False)
    print(f"Data fetched and saved to {FILE_PATH}")


if __name__ == '__main__':
    asyncio.run(main())