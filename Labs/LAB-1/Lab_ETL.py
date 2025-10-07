from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import snowflake.connector

# Snowflake connection helper
def snowflake_cursor():
    return snowflake.connector.connect(
        user=Variable.get('snowflake_userid'),
        password=Variable.get('snowflake_password'),
        account=Variable.get('snowflake_account'),
        warehouse=Variable.get('snowflake_warehouse'),
        database=Variable.get('snowflake_database'),
        schema=Variable.get('snowflake_schema'),
        role=Variable.get('snowflake_role', default_var='TRAINING_ROLE'),
    ).cursor()


# 1) EXTRACT

@task
def extract(symbols_csv: str, lookback_days: int) -> dict:
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    if not symbols:
        raise ValueError("No symbols provided in Variable 'stock_symbols'.")

    
    end_date = pd.Timestamp.utcnow().normalize()
    start_date = end_date - pd.Timedelta(days=int(lookback_days))

    all_rows = [] 
    for sym in symbols:
        df = yf.download(
            sym,
            start=start_date.date(),
            end=(end_date + pd.Timedelta(days=1)).date(), 
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
        )
        if df.empty:
            continue

        df = df.reset_index()  # Date -> column
        df["symbol"] = sym
        df.rename(columns={
            "Date": "trade_date",
            "Open": "open",
            "Close": "close",
            "Low": "low",
            "High": "high",
            "Volume": "volume",
        }, inplace=True)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df = df[["symbol", "trade_date", "open", "close", "low", "high", "volume"]]

        # append as lists (JSON-serializable)
        all_rows.extend(df.astype(object).values.tolist())

    if not all_rows:

        return {
            "rows": [],
            "symbols": symbols,
            "start": str(start_date.date()),
            "end": str(end_date.date()),
        }

    return {
        "rows": all_rows,
        "symbols": symbols,
        "start": str(start_date.date()),
        "end": str(end_date.date()),
    }


# 2) TRANSFORM

@task
def transform(payload: dict) -> dict:
    rows = payload.get("rows", [])
    cleaned = []
    for r in rows:
        # r order from extract: symbol, trade_date, open, close, low, high, volume
        symbol = str(r[0])
        trade_date = str(r[1])  # 'YYYY-MM-DD'
        open_ = float(r[2]) if r[2] is not None else None
        close_ = float(r[3]) if r[3] is not None else None
        low_ = float(r[4]) if r[4] is not None else None
        high_ = float(r[5]) if r[5] is not None else None
        vol_ = int(r[6]) if r[6] is not None else None

        cleaned.append([symbol, trade_date, open_, close_, low_, high_, vol_])

    return {
        "rows": cleaned,
        "symbols": payload.get("symbols", []),
        "start": payload.get("start"),
        "end": payload.get("end"),
    }


# 3) LOAD

@task
def load(payload: dict, target_table_name: str):
    rows = payload["rows"]
    symbols = payload["symbols"]
    start = payload["start"]
    end = payload["end"]

    db = Variable.get('snowflake_database')
    sc = Variable.get('snowflake_schema')
    table = f"{db}.{sc}.{target_table_name}"

    cur = snowflake_cursor()
    try:
        cur.execute("BEGIN;")

        # Create table if needed
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                symbol STRING,
                trade_date DATE,
                open FLOAT,
                close FLOAT,
                low FLOAT,
                high FLOAT,
                volume NUMBER,
                CONSTRAINT pk_symbol_date PRIMARY KEY (symbol, trade_date)
            );
        """)

        if symbols:
            # Delete existing rows in the window for these symbols to avoid duplicates
            placeholders = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"""
                DELETE FROM {table}
                WHERE trade_date BETWEEN %s AND %s
                  AND symbol IN ({placeholders})
                """,
                (start, end, *symbols)
            )

        # Insert fresh rows (if any)
        if rows:
            cur.executemany(
                f"""
                INSERT INTO {table}
                (symbol, trade_date, open, close, low, high, volume)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                rows
            )

        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        raise
    finally:
        try:
            cur.close()
        except:
            pass

    return f"Loaded {len(rows)} rows into {table} for window {start}..{end}"

# DAG definition

with DAG(
    dag_id='yfinance_ETL',
    description='3-task ETL: yfinance → Snowflake (delete+insert window, Lab1)',
    start_date=datetime(2025, 9, 29),
    schedule='0 2 * * *',  # daily 02:00
    catchup=False,
    tags=['ETL', 'yfinance', 'snowflake', '3-task', 'simple'],
) as dag:
   symbols_csv   = "NVDA,AAPL"     
   lookback_days = 180            
   target_table  = "stock_prices_lab" 

   raw = extract(symbols_csv, lookback_days)
   tidy = transform(raw)
   load(tidy, target_table)
