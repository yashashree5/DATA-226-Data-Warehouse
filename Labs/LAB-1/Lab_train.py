from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task 
def train(train_input_table: str, train_view: str, forecast_function_name: str):
    cur = None
    try:
        cur = return_snowflake_conn()
         # --- Execute SQL ---
        create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
            TRADE_DATE AS DATE,
            CLOSE AS CLOSE,
            SYMBOL AS SYMBOL
            FROM {train_input_table};"""

        create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );"""

        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        
    except Exception as e:
        print(f"Error in train task: {e}")
        raise
    finally:
        if cur:
            cur.close()
            if hasattr(cur, '_conn') and cur._conn:
                cur._conn.close()


@task
def predict(forecast_function_name: str, train_input_table: str, forecast_table: str, final_table: str):
    cur = None
    try:
        cur = return_snowflake_conn()
        # --- Execute SQL ---
        make_prediction_sql = f"""BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;"""

        create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
            SELECT SYMBOL, TRADE_DATE AS DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
            UNION ALL
            SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table};"""

        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
        
    except Exception as e:
        print(f"Error in predict task: {e}")
        raise
    finally:
        if cur:
            cur.close()
            # Close the connection stored on the cursor object
            if hasattr(cur, '_conn') and cur._conn:
                cur._conn.close()


with DAG(
    dag_id = 'yfinance_train',
    start_date = datetime(2025,9,29),
    catchup=False,
    tags=['ML', 'ELT','Train'],
    schedule = '30 2 * * *'
) as dag:

    # Naming conventions
    train_input_table      = "RAW.STOCK_PRICES_LAB" 
    train_view             = "ADHOC.STOCK_PRICE_VIEW" 
    forecast_table         = "ADHOC.STOCK_PRICES_FORECAST_7D" 
    forecast_function_name = "ANALYTICS.PREDICT_STOCK_PRICES_LAB"
    final_table            = "ANALYTICS.STOCK_PRICES_FINAL"

    # Task calls
    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)
    
    # Set dependency
    train_task >> predict_task