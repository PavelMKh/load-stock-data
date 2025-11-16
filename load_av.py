import os
import logging
import traceback
import sqlite3

from datetime import datetime
from dateutil.relativedelta import relativedelta

import random
import pandas as pd
from tqdm import tqdm

import asyncio
import aiohttp

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

DATE_FILE = 'dates.txt'

TICKERS = ['SPY', 'IBIT', 'ETHA', 'AAPL', 'MSFT', 'NVDA', 'JPM', 'V', 'JNJ', 'PG', 'DIS', 'KO'] # 'PFE', 'CSCO',

INTERVALS = ['15min'] # '60min'

DB_PATH = 'stocks.db'

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS candles (
            begin DATETIME,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            ticker TEXT,
            interval TEXT,
            UNIQUE(begin, ticker, interval)
        )
    ''')
    conn.commit()
    return conn

def insert_df_into_db(conn, df):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO candles (begin, open, high, low, close, volume, ticker, interval)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['begin'].strftime('%Y-%m-%d %H:%M:%S'),
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                int(row['volume']),
                row['ticker'],
                row['interval']
            ))
        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {e}")
    conn.commit()

def load_dates():
    if os.path.exists(DATE_FILE):
        with open(DATE_FILE, 'r') as f:
            lines = f.read().splitlines()
            if len(lines) == 2:
                return lines[0], lines[1]

    return '2022-10-01', '2023-07-01'

def save_dates(start_date, end_date):
    with open(DATE_FILE, 'w') as f:
        f.write(f"{start_date}\n{end_date}")

def split_date_ranges_to_months(start_date_str, end_date_str):
    """
    Converts a date range into a list of months in the YYYY-MM format.
    Example:
        >>> split_date_ranges_to_months('2024-01-15', '2024-03-10')
        ['2024-01', '2024-02', '2024-03']
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(day=1)
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(day=1)
    
    months = []
    current = start_date
    while current <= end_date:
        months.append(current.strftime('%Y-%m'))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months



async def get_history_df_with_session(ticker, month, apikey, interval, session):
    """
    Fetches historical stock candle data asynchronously from the AlphaVantage for a 
    given ticker and date range using an existing HTTP session.
    
    Date format: YYYY-MM-DD
    intervals: 1min, 5min, 15min, 30min, 60min
    """
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&month={month}&outputsize=full&apikey={apikey}"
        async with session.get(url) as response:
            json_data = await response.json()
            time_series_key = f"Time Series ({interval})"
            candles_data = json_data.get(time_series_key, {})
            print(f'{ticker} {str(candles_data)[:10]}')
            df = pd.DataFrame.from_dict(candles_data, orient='index')

            df = df.rename(columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume"
            })

            df = df.reset_index().rename(columns={'index': 'begin'})

            df['begin'] = pd.to_datetime(df['begin'])

            df['ticker'] = ticker
            df['interval'] = interval

            return df
    
    except Exception as e:
        logger.error(f"Error fetching data for {ticker} {month} {interval}: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()


async def fetch_for_ticker_interval(ticker, apikey, interval, start_date, end_date, session):
    """
    Fetches historical data for a given ticker and interval over a date range by aggregating data from relevant months.
    """
    date_ranges = split_date_ranges_to_months(start_date, end_date)
    dfs = []
    for dr in date_ranges:
        df = await get_history_df_with_session(ticker, dr, apikey, interval, session)
        if not df.empty:
            dfs.append(df)
        await asyncio.sleep(random.uniform(0.5, 1))
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()
    

async def main():
    """
    Main asynchronous function that:
    - prompts the user for their API key;
    - loads existing data from a file or initializes a new DataFrame;
    - iterates over tickers and intervals to gather historical data over the specified date range;
    - saves the collected data to a file.    
    """
    conn = init_db()
    try:
        apikey = os.getenv("API_KEY")
        START_DATE, END_DATE = load_dates()
        async with aiohttp.ClientSession() as session:
            for ticker in tqdm(TICKERS, desc="Tickers"):
                for interval in INTERVALS:
                    new_data = await fetch_for_ticker_interval(ticker, apikey, interval, START_DATE, END_DATE, session)
                    if not new_data.empty:
                        insert_df_into_db(conn, new_data)
        start_dt = datetime.strptime(START_DATE, '%Y-%m-%d') + relativedelta(months=2)
        end_dt = datetime.strptime(END_DATE, '%Y-%m-%d') + relativedelta(months=2)

        if start_dt > datetime.now():
            start_dt = datetime.now()

        if end_dt > datetime.now():
            end_dt = datetime.now()

        save_dates(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
        logger.info(f"Data fetched and saved to {DB_PATH}; Updated dates to {start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}")
    finally:
        conn.close()


if __name__ == '__main__':
    asyncio.run(main())

