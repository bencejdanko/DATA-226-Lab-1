from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


from datetime import timedelta
from datetime import datetime
import requests
import logging

# Initialize the logger
logger = logging.getLogger(__name__)

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

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

@task
def extract(symbol, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()
    return data

@task
def transform(data):
    results = []   # empyt list for now to hold the 90 days of stock info (open, high, low, close, volume)
    for d in data["Time Series (Daily)"]:   # here d is a date: "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d
        results.append(stock_info)
    return results

@task
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

@task
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

with DAG(
    dag_id = 'AlphaVantage_ETL_Lab1',
    start_date = datetime(2024,9,27),
    catchup=False,
    tags=['ETL'],
    schedule_interval='@daily',
) as dag:
    symbol = "AAPL"
    stage_table = "raw_data.temp"
    target_table = "raw_data.time_series_daily"
    api_key = Variable.get("alpha_vantage_api_key")

    data = extract(symbol, api_key)
    data = transform(data)
    load(data, stage_table, symbol)
    merge(target_table)
    logger.info("ETL process completed for AAPL")

    symbol = "NVDA"
    data = extract(symbol, api_key)
    data = transform(data)
    load(data, stage_table, symbol)
    merge(target_table)
    logger.info("ETL process completed for NVDA")
