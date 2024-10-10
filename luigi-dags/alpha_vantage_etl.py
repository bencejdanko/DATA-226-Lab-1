

import luigi
import snowflake.connector
import requests
import os

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
            except Exception as e:
                raise
    except Exception as e:
        raise

# Define function to return a Snowflake connection
def return_snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='compute_wh',
        database='time_series',
        schema='raw_data'
    )
    cur = conn.cursor()
    return cur

# Task to extract data from Alpha Vantage API
class ExtractTask(luigi.Task):
    symbol = luigi.Parameter()
    api_key = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"data_{self.symbol}.json")

    def run(self):
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={self.symbol}&apikey={self.api_key}'
        r = requests.get(url)
        data = r.json()
        with self.output().open('w') as f:
            f.write(str(data))

# Task to transform the extracted data
class TransformTask(luigi.Task):
    symbol = luigi.Parameter()
    api_key = luigi.Parameter()

    def requires(self):
        return ExtractTask(self.symbol, self.api_key)

    def output(self):
        return luigi.LocalTarget(f"transformed_data_{self.symbol}.json")

    def run(self):
        results = []
        with self.input().open() as f:
            data = eval(f.read())  # Note: Ensure proper data handling here for safety
            for d in data["Time Series (Daily)"]:
                stock_info = data["Time Series (Daily)"][d]
                stock_info["date"] = d
                results.append(stock_info)
        with self.output().open('w') as f:
            f.write(str(results))

# Task to load the transformed data into Snowflake
class LoadTask(luigi.Task):
    symbol = luigi.Parameter()
    api_key = luigi.Parameter()
    stage_table = luigi.Parameter()
    target_table = luigi.Parameter()

    def requires(self):
        return TransformTask(self.symbol, self.api_key)

    def run(self):
        with self.input().open() as f:
            data = eval(f.read())

        cur = return_snowflake_conn()
        try:
            cur.execute("BEGIN;")
            create_table(self.stage_table, cur)
            insert_records(self.stage_table, data, self.symbol, cur)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise
        finally:
            cur.close()

# Task to upsert data into the target table
class UpsertTask(luigi.Task):
    symbol = luigi.Parameter()
    api_key = luigi.Parameter()
    target_table = luigi.Parameter()

    def requires(self):
        return LoadTask(self.symbol, self.api_key, "raw_data.temp", self.target_table)

    def run(self):
        cur = return_snowflake_conn()
        try:
            cur.execute("BEGIN;")
            cur.execute(f"""
                MERGE INTO {self.target_table} t
                USING raw_data.temp s
                ON t.symbol = s.symbol AND t.date = s.date
                WHEN NOT MATCHED THEN
                INSERT (symbol, date, open, high, low, close, volume)
                VALUES (s.symbol, s.date, s.open, s.high, s.low, s.close, s.volume);
            """)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise
        finally:
            cur.close()

# Wrapper to execute tasks in sequence
class ETLPipeline(luigi.Task):
    symbol = luigi.Parameter(default="AAPL")
    api_key = luigi.Parameter()
    target_table = luigi.Parameter(default="raw_data.time_series_daily")

    def requires(self):
        return UpsertTask(self.symbol, self.api_key, self.target_table)

if __name__ == '__main__':
    luigi.run()
