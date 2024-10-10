import os
import snowflake.connector


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

def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        CAST(DATE AS TIMESTAMP_NTZ) AS DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model.
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store the results to a table named forecast_table.
     - Union your predictions with your historical data, then create the final table
    """
    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions.
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval.
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table.
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT
            SYMBOL,
            DATE,
            CLOSE AS actual,
            NULL AS forecast,
            NULL AS lower_bound,
            NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT
            replace(series, '"', '') as SYMBOL,
            ts as DATE,
            NULL AS actual,
            forecast,
            lower_bound,
            upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

train_input_table = "raw_data.time_series_daily"
train_view = "adhoc.market_data_view"
forecast_table = "adhoc.market_data_forecast"
forecast_function_name = "analytics.predict_stock_price"
final_table = "analytics.market_data"
cur = return_snowflake_conn()

train(cur, train_input_table, train_view, forecast_function_name)
predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)