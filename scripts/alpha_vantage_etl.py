import snowflake.connector

from datetime import timedelta
from datetime import datetime
import requests
import logging
import os

# Initialize the logger
logger = logging.getLogger(__name__)

def return_snowflake_conn():
    conn = snowflake.connector.connect(
        user= os.getenv('SNOWFLAKE_USER'),
        password= os.getenv('SNOWFLAKE_PASSWORD'),
        account= os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='compute_wh',
        database='time_series',
        schema='raw_data'
    )
    cur = conn.cursor()
    return cur

def create_table(table, cur):
    try:
        cur.execute(f"""
        create or replace table {table} (
          id INT AUTOINCREMENT PRIMARY KEY,
          symbol string,
          date date,
          open float,
          high float,
          low float,
          close float,
          volume float
        );
        """)
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise Exception("Error creating table")

def insert_records(table, data, symbol, cur):
    try:
        count = 0
        for d in data:
            if count >= 90:
                break
            open_price = d["1. open"]
            high = d["2. high"]
            low = d["3. low"]
            close = d["4. close"]
            volume = d["5. volume"]
            date = d["date"]
            sql = f"""
                INSERT INTO {table} (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cur.execute(sql, (symbol, date, open_price, high, low, close, volume))
                count += 1
                logger.info(f"Inserted record for {date}")
            except Exception as e:
                logger.error(f"Error inserting record: {e}")
                raise
        logger.info(f"Inserted {count} records successfully")
    except Exception as e:
        logger.error(f"Transaction failed: {e}")
        raise

def extract(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()
    return data

def transform(data):
    results = []   # empty list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        results.append(stock_info)
    return results

def load(data, target_table, symbol):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        create_table(target_table, cur)
        insert_records(target_table, data, symbol, cur)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
    finally:
        cur.close()

def merge(target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
            MERGE INTO {target_table} t
            USING raw_data.temp s
            ON t.symbol = s.symbol AND t.date = s.date
            WHEN NOT MATCHED THEN
            INSERT (symbol, date, open, high, low, close, volume)
            VALUES (s.symbol, s.date, s.open, s.high, s.low, s.close, s.volume);
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
    finally:
        cur.close()

symbol = "AAPL"
stage_table = "raw_data.temp"
target_table = "raw_data.time_series_daily"
api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

data_aapl = extract(symbol, api_key)
transformed_data_aapl = transform(data_aapl)
load_task_aapl = load(transformed_data_aapl, stage_table, symbol)
merge_task_aapl = merge(target_table)

symbol = "NVDA"
data_nvda = extract(symbol, api_key)
transformed_data_nvda = transform(data_nvda)
load_task_nvda = load(transformed_data_nvda, stage_table, symbol)
merge_task_nvda = merge(target_table)

logger.info("ETL process completed for AAPL and NVDA")