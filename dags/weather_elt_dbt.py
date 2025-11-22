from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt" 

conn = BaseHook.get_connection('snowflake_conn')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
    "env": {
        "DBT_USER": conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_ACCOUNT": conn.extra_dejson.get("account"),
        "DBT_SCHEMA": "ANALYTICS",
        "DBT_DATABASE": conn.extra_dejson.get("database"),
        "DBT_ROLE": conn.extra_dejson.get("role"),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
        "DBT_TYPE": "snowflake"
    }
}

with DAG(
    dag_id="weather_ELT_dag",
    default_args=default_args,
    schedule=None,
    description="Simple dbt transformation for SF Weather",
    tags=["dbt", "weather"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test >> dbt_snapshot