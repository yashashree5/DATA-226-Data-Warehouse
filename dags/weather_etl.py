# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from __future__ import annotations

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
import requests
import uuid

# -------------------
# Config
# -------------------
SNOWFLAKE_CONN_ID = "snowflake_conn"
TARGET_TABLE = "RAW.WEATHER_HISTORY"

# San Francisco Coordinates
WEATHER_API_URL = Variable.get("weather_api_url", default_var="https://archive-api.open-meteo.com/v1/archive")
LATITUDE = Variable.get("weather_latitude", default_var="37.7749")
LONGITUDE = Variable.get("weather_longitude", default_var="-122.4194")
START_DATE = Variable.get("weather_start_date", default_var="2024-01-01")
END_DATE = Variable.get("weather_end_date", default_var="2024-12-31")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def return_snowflake_conn_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn.cursor()

with DAG(
    dag_id="weather_ETL_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["ETL", "weather", "snowflake", "daily", "San Francisco"],
    default_args=default_args,
    description="Fetch daily weather for San Francisco and upsert into Snowflake",
) as dag:

    @task
    def extract() -> dict:
        """
        Extract daily weather data for San Francisco for the fixed date range.
        """
        url = (
            f"{WEATHER_API_URL}"
            f"?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&start_date={START_DATE}&end_date={END_DATE}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
            f"&timezone=auto"
        )

        print(f"[extract] San Francisco ({LATITUDE}, {LONGITUDE}) | Range: {START_DATE} → {END_DATE}")
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if not data or "daily" not in data or "time" not in data["daily"]:
                raise ValueError("API returned no daily data")
            print(f"[extract] OK. API Lat/Lon: {data.get('latitude')}, {data.get('longitude')}")
            return data
        except requests.exceptions.Timeout:
            raise Exception("API request timed out after 60 seconds")
        except requests.exceptions.RequestException as e:
            raise Exception(f"API request failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Extraction failed: {str(e)}")

    @task
    def transform(raw_data: dict) -> list[tuple]:
        """
        Transform daily weather data into rows matching the Snowflake schema.
        """
        daily = raw_data.get("daily", {})
        dates = daily.get("time", [])
        if not dates:
            raise ValueError("No dates found in daily data")

        print(f"[transform] Rows: {len(dates)}")

        def safe_num(arr, i, cast):
            if arr is None or i >= len(arr) or arr[i] is None:
                return None
            try:
                return cast(arr[i])
            except Exception:
                return None

        rows: list[tuple] = []
        for i, date in enumerate(dates):
            rows.append(
                (
                    date,  # FORECAST_DATE
                    safe_num(daily.get("temperature_2m_max"), i, float),
                    safe_num(daily.get("temperature_2m_min"), i, float),
                    safe_num(daily.get("precipitation_sum"), i, float),
                    safe_num(daily.get("wind_speed_10m_max"), i, float),
                )
            )

        print(f"[transform] Done")
        return rows

    @task
    def load(daily_rows: list[tuple]):
        """
        Load daily weather data into Snowflake using an idempotent MERGE operation.
        """
        if not daily_rows:
            print("[load] No rows to load; nothing to do.")
            return

        print(f"[load] Upserting {len(daily_rows)} rows...")
        cur = return_snowflake_conn_cursor()
        conn = cur.connection
        stage_table = f"TMP_WEATHER_STAGE_{uuid.uuid4().hex.upper()[:8]}"

        try:
            # 1) Ensure target exists (with PK for additional safety)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    FORECAST_DATE DATE,
                    MAX_TEMP FLOAT,
                    MIN_TEMP FLOAT,
                    PRECIPITATION FLOAT,
                    WIND_SPEED FLOAT,
                    CONSTRAINT PK_WEATHER_HISTORY_DATE PRIMARY KEY (FORECAST_DATE)
                )
            """)
            print("[load] Target table ready")

            # 2) Transaction
            conn.autocommit = False
            cur.execute("BEGIN")
            print("[load] Transaction started")

            # 3) Create TEMP stage
            cur.execute(f"""
                CREATE TEMPORARY TABLE {stage_table} (
                    FORECAST_DATE DATE,
                    MAX_TEMP FLOAT,
                    MIN_TEMP FLOAT,
                    PRECIPITATION FLOAT,
                    WIND_SPEED FLOAT
                )
            """)
            print(f"[load] Stage table {stage_table} created")

            # 4) Stage data
            insert_sql = f"""
                INSERT INTO {stage_table}
                (FORECAST_DATE, MAX_TEMP, MIN_TEMP, PRECIPITATION, WIND_SPEED)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, daily_rows)
            print(f"[load] Staged {len(daily_rows)} rows")

            # 5) MERGE (UPSERT) into target
            merge_sql = f"""
                MERGE INTO {TARGET_TABLE} AS T
                USING {stage_table} AS S
                ON T.FORECAST_DATE = S.FORECAST_DATE
                WHEN MATCHED THEN UPDATE SET
                    T.MAX_TEMP      = S.MAX_TEMP,
                    T.MIN_TEMP      = S.MIN_TEMP,
                    T.PRECIPITATION = S.PRECIPITATION,
                    T.WIND_SPEED    = S.WIND_SPEED
                WHEN NOT MATCHED THEN INSERT (
                    FORECAST_DATE, MAX_TEMP, MIN_TEMP, PRECIPITATION, WIND_SPEED
                ) VALUES (
                    S.FORECAST_DATE, S.MAX_TEMP, S.MIN_TEMP, S.PRECIPITATION, S.WIND_SPEED
                )
            """
            cur.execute(merge_sql)
            print("[load] MERGE completed")

            # 6) Optional sanity check
            cur.execute(f"SELECT COUNT(*) FROM {stage_table}")
            staged = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE FORECAST_DATE IN (SELECT FORECAST_DATE FROM {stage_table})")
            affected = cur.fetchone()[0]
            print(f"[load] Sanity: staged={staged}, target_rows_for_same_date={affected}")

            # 7) Commit & cleanup
            cur.execute("COMMIT")
            print("[load] Transaction committed")
            try:
                cur.execute(f"DROP TABLE IF EXISTS {stage_table}")
            except Exception:
                pass

        except Exception as e:
            print(f"[load] Error: {e}")
            try:
                cur.execute("ROLLBACK")
                print("[load] Rolled back")
            except Exception:
                pass
            raise
        finally:
            cur.close()
            print("[load] Cursor closed")

    # Trigger another DAG after load completes
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="weather_ELT_dag",  
        wait_for_completion=False,  
        reset_dag_run=False,
        execution_date="{{ ds }}",  
        conf={"source_dag": "sf_weather_daily_etl"} 
    )

    # Dependencies
    raw_data = extract()
    transformed_rows = transform(raw_data)
    load_task = load(transformed_rows)

    # Trigger the next DAG after load completes
    load_task >> trigger_dbt