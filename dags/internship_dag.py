"""
Airflow DAG — AutoTrack pipeline.

Tasks:
    extract   -> run_bronze (writes Parquet handoff)
    transform -> run_silver (pass-through; keeps the 4-stage shape)
    load      -> run_gold  (DuckDB upsert)
    notify    -> run_notify (WhatsApp)

Each task has its own retry policy per PRD §5.2:
3 retries, exponential backoff. notify is the noisiest in practice
(Meta API flakiness) so it gets the same policy but its delays are
slightly more spread out via the underlying notify layer.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
# dags/src/ is on PYTHONPATH (/opt/airflow/src) per docker-compose,
# but we add it defensively for local DAG-parsing scenarios.
SRC = "/opt/airflow/src"
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import pipeline  # noqa: E402

# ─────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────

default_args = {
    "owner": "autotrack",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────
# DAG
# ─────────────────────────────────────────

with DAG(
    dag_id="internship_dag",
    description="AutoTrack: Gmail → Silver → DuckDB → WhatsApp",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["autotrack", "gmail", "internship"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=pipeline.run_bronze,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=pipeline.run_silver,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=pipeline.run_gold,
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=pipeline.run_notify,
    )

    # PRD: Extract >> Transform >> Load >> Notify
    extract_task >> transform_task >> load_task >> notify_task
