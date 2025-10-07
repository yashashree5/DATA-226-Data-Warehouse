from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, date, timedelta
import requests
import logging
from typing import List, Dict, Tuple

# --- DAG Configuration ---
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="hw4_stock_pipeline_final",
    description="ETL pipeline for HW4: Extracts 90 trading days and loads to Snowflake.",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 30),
    catchup=False,
    default_args=default_args,
    tags=["hw4", "etl", "snowflake"],
)
def hw4_stock_pipeline_final():
    
    # Define constants
    STOCK_SYMBOL = "AMZN"
    SNOWFLAKE_TABLE = "RAW.STOCK_PRICES_DAG" 
    SNOWFLAKE_CONN_ID = "snowflake_conn" 


    @task
    def extract(symbol: str) -> List[Dict]:
        """Fetches daily stock prices by formatting the full URL template from the Variable."""
        
        vantage_api_key = Variable.get("vantage_api_key") 
        api_url_template = Variable.get(
            "alpha_vantage_url",
            default_var="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}"
        )
        
        final_url = api_url_template.format(symbol=symbol, vantage_api_key=vantage_api_key)
        
        r = requests.get(final_url, timeout=60)
        r.raise_for_status()
        data = r.json()
        
        results = []

        ts = data.get("Time Series (Daily)")
        if not isinstance(ts, dict):
            msg = data.get("Note") or data.get("Error Message") or str(data)
            raise RuntimeError(f"Alpha Vantage missing/invalid time series: {msg}")
            
        for d in ts:
            stock_info = ts[d]
            stock_info["date"] = d
            results.append(stock_info)

        logging.info("[extract] Retrieved %d total records for %s.", len(results), symbol)
        return results

    @task
    def transform(records: List[Dict], **context) -> List[Dict]:
        """
        Filters stock data to include only trading days within the last 90 calendar days.
        This results in approximately 64 trading days.
        """
        
        logical_date = context['dag_run'].logical_date.date()
        cutoff = logical_date - timedelta(days=90)
        
        seen = set()
        out = []
        
        for r in sorted(records, key=lambda x: x["date"], reverse=True):
            try:
                d = datetime.fromisoformat(r["date"]).date()
            except ValueError:
                logging.warning("Skipping record with invalid date format: %s", r["date"])
                continue
            
            # 1. ENFORCE 90 CALENDAR DAY CUTOFF
            if d < cutoff:
                continue
                
            # 2. ENFORCE TRADING DAY FILTER (Weekends)
            if d.weekday() >= 5: 
                continue
                
            # 3. Prevent duplicates (if needed)
            if d not in seen:
                out.append(r)
                seen.add(d)

        logging.info("[transform] Cutoff Date: %s. Filtered and kept %d trading days.", cutoff.isoformat(), len(out))
        return out
    
    
    @task
    def load(records_90d: List[Dict], table: str, symbol: str, conn_id: str) -> None:
        """Loads and manages the Snowflake transaction (Create Table, Delete, Insert, Commit/Rollback)."""
        
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        
        conn = None
        cur = None
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
            
            # --- 1. CREATE TABLE LOGIC (Stable DDL) ---
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                  SYMBOL    VARCHAR      NOT NULL,
                  "DATE"    DATE         NOT NULL,
                  OPEN      NUMBER(18,4),
                  CLOSE     NUMBER(18,4),
                  HIGH      NUMBER(18,4),
                  LOW       NUMBER(18,4),
                  VOLUME    NUMBER(38,0),
                  CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (SYMBOL, "DATE")
                );
            """)
            logging.info("[load] Ensured table %s exists in the specified schema.", table)
            
            # --- 2. TRANSACTIONAL INSERT LOGIC ---
            
            delete_sql = f'DELETE FROM {table} WHERE SYMBOL = \'{symbol}\''
            
            insert_sql = f'''
            INSERT INTO {table} (SYMBOL, "DATE", OPEN, CLOSE, HIGH, LOW, VOLUME)
            VALUES (%s, TO_DATE(%s), %s, %s, %s, %s, %s)
            '''.strip()

            rows_to_insert = []
            for r in records_90d:
                rows_to_insert.append((
                    symbol,
                    r["date"],
                    float(r["1. open"]),
                    float(r["4. close"]),
                    float(r["2. high"]),
                    float(r["3. low"]),
                    int(r["5. volume"]),
                ))
            
            # Start transaction: Delete -> Insert -> Commit
            cur.execute(delete_sql)
            logging.info("[load] Deleted existing records for %s from %s.", symbol, table)

            if rows_to_insert:
                cur.executemany(insert_sql, rows_to_insert)
                
            conn.commit()
            logging.info("Committed. Inserted %d rows into %s", len(rows_to_insert), table)
            
        except Exception as e:
            if conn:
                # Attempt to rollback (this is the correct error path)
                try:
                    conn.rollback()
                except Exception as rb_e:
                    logging.error("Error during connection rollback: %s", rb_e)
            logging.error("Transaction failed and rolled back: %s", e)
            raise 
            
        finally:
            # FIX: Stabilize the finally block to prevent failure on close/cleanup
            if cur:
                try:
                    cur.close()
                except Exception as close_e:
                    logging.warning("Warning: Failed to close cursor during cleanup: %s", close_e)
            if conn:
                try:
                    conn.close()
                except Exception as close_e:
                    logging.warning("Warning: Failed to close connection during cleanup: %s", close_e)

  
 
    extracted_records = extract(symbol=STOCK_SYMBOL) 
    filtered_records = transform(extracted_records)
    load(
        records_90d=filtered_records, 
        table=SNOWFLAKE_TABLE, 
        symbol=STOCK_SYMBOL,
        conn_id=SNOWFLAKE_CONN_ID
    )

# Expose the DAG to Airflow
etl_dag = hw4_stock_pipeline_final()