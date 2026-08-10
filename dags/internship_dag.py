"""
Airflow DAG — AutoTrack pipeline.

Tasks:
    extract   -> run_bronze (writes Parquet handoff)
    transform -> run_silver (pass-through; keeps the 4-stage shape)
    load      -> run_gold  (DuckDB upsert)
    notify    -> run_notify (SMTP email)

Each task has its own retry policy per PRD §5.2: 3 retries,
exponential backoff. notify is the noisiest in practice (Gmail
SMTP flakiness) so it gets the same policy but its delays are
slightly more spread out via the underlying notify layer.

The DAG file lives in ``dags/`` (where Airflow picks it up) and
imports the ``autotrack`` package. In the container, the package
is mounted at ``/opt/airflow/autotrack`` (via
``./dags/src/autotrack:/opt/airflow/autotrack``) and
``PYTHONPATH=/opt/airflow`` makes the bare ``autotrack`` name
importable. When parsing this file outside the container (e.g.
local ``airflow dags list``), the fallback below adds the
package root to ``sys.path`` so the same import works.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Local-dev fallback: when this DAG file is parsed outside the
# container (e.g. ``pytest`` or ``airflow dags list`` on the host),
# the ``autotrack`` package isn't on PYTHONPATH. Add
# ``dags/src`` so the bare ``autotrack`` import below resolves.
# In the container, PYTHONPATH already covers this, so the
# ``if`` guard keeps the change a no-op there.
SRC_PARENT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PARENT) not in sys.path:
    sys.path.insert(0, str(SRC_PARENT))

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
    description="AutoTrack: Gmail → Silver → DuckDB → Email",
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
