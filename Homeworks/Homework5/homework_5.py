# /opt/airflow/dags/homework_4.py
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import logging

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="hw4_stock_pipeline",
    description="Step 1 of HW4: Use @task and set dependencies",
    schedule_interval='30 2 * * *',          
    start_date=datetime(2025, 1, 1),    
    catchup=False,
    default_args=default_args,
    tags=["hw4", "step1", "modified"],
)
def hw4_step1():

    @task
    def extract(symbol: str = "AMZN") -> list[dict]: 
        """
        Returns a list of dicts like:
        {'1. open': '...', '2. high': '...', '3. low': '...', '4. close': '...', '5. volume': '...', 'date': 'YYYY-MM-DD'}
        """
        api_key = Variable.get("alpha_vantage_api_key")
        api_url = Variable.get("alpha_vantage_url", default_var="https://www.alphavantage.co/query")

        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": api_key,
            "outputsize": "compact",  # ~90 most recent days
            "datatype": "json",
        }
        r = requests.get(api_url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        ts = data.get("Time Series (Daily)")
        if not isinstance(ts, dict):
            msg = data.get("Note") or data.get("Error Message") or str(data)
            raise RuntimeError(f"Alpha Vantage missing/invalid time series: {msg}")

        out = []
        for ds, stock in ts.items():
            row = dict(stock)
            row["date"] = ds
            out.append(row)
        return out

    @task
    def transform(records: list[dict]) -> list[dict]:
       
        if not isinstance(records, list):
            raise ValueError("transform expected list[dict]")
        logging.info("[transform] Data received and passing through.")
        print(f"[transform] Got {len(records)} rows")
        return records

    @task
    def load(records: list[dict]) -> None:
        logging.info("[load] Snowflake stub called. Attempting to insert %d records.", len(records))

    extracted = extract("AMZN")  
    transformed = transform(extracted)
    load(transformed)

# Expose the DAG to Airflow
hw4_step1 = hw4_step1()