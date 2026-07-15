"""
Airflow DAG — AutoTrack pipeline.

Tasks:
    extract   -> run_bronze (writes Parquet handoff)
    transform -> run_silver (pass-through; keeps the 4-stage shape)
    load      -> run_gold  (DuckDB upsert)
    notify    -> run_notify (WhatsApp)

Each task has its own retry policy per PRD §5.2: 3 retries,
exponential backoff. notify is the noisiest in practice (Meta
API flakiness) so it gets the same policy but its delays are
slightly more spread out via the underlying notify layer.

The DAG file lives in ``dags/`` (where Airflow picks it up) and
imports from the installed ``autotrack`` package. The package is
added to PYTHONPATH by the docker-compose volume mount that maps
``./src`` to ``/opt/airflow/src`` and the ``PYTHONPATH: /opt/airflow/src``
environment variable in the airflow-common block.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the package importable when this file is parsed by Airflow
# outside of the container (e.g. local ``airflow dags list``). In
# the container, PYTHONPATH is already set by docker-compose.
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from autotrack import pipeline  # noqa: E402

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
