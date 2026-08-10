"""
Pipeline orchestrator.

Glues the four layers together. Designed for two consumers:

  * The Airflow DAG (calls run_bronze / run_silver / run_gold /
    run_notify one task at a time, with the DataFrame handed off
    via Parquet on the shared ``data/`` volume).
  * The CLI (``python -m autotrack.pipeline``) for ad-hoc local
    runs and tests.

The DataFrame handoff is a Parquet file at the configured path
(see :mod:`autotrack.config`). The reason: Airflow's XCom
metadata database has a 48KB limit and a real silver DataFrame
will exceed that by orders of magnitude.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from autotrack import silver
from autotrack.config import Settings, load_settings
from autotrack.logging import get_logger
from autotrack import bronze, gold, notify

log = get_logger(__name__)


# ─────────────────────────────────────────
# HANDOFF HELPERS
# ─────────────────────────────────────────

def write_handoff(df: pd.DataFrame, path: Path) -> Path:
    """Persist the silver DataFrame for the next task to pick up."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_handoff(path: Path) -> pd.DataFrame:
    """Read the silver DataFrame the previous task wrote."""
    return pd.read_parquet(path)


# ─────────────────────────────────────────
# DAG-FACING ENTRY POINTS
# ─────────────────────────────────────────

def run_bronze(
    handoff: Optional[Path] = None,
    settings: Optional[Settings] = None,
) -> int:
    """Bronze task: pull from Gmail, transform to silver, persist.

    Even on no records we write an empty handoff so the next task
    has a well-defined input. Returns the number of records pulled.
    """
    s = settings or load_settings()
    handoff_path = handoff or s.handoff_path

    records = bronze.run_bronze(settings=s)
    df = silver.run_silver(records)
    write_handoff(df, handoff_path)
    log.info(f"Bronze task: {len(records)} records.")
    return len(records)


def run_silver(
    handoff: Optional[Path] = None,
    settings: Optional[Settings] = None,
) -> int:
    """Silver task: a separate retriable unit.

    In the current design, the bronze task already produces a
    silver DataFrame (transformed in-process). This task is a
    pass-through that re-reads the handoff so the DAG still
    honors the 4-stage shape and each step is independently
    retriable.

    On an empty handoff we write an empty one so downstream
    tasks don't error on a missing file.
    """
    s = settings or load_settings()
    handoff_path = handoff or s.handoff_path

    if not handoff_path.exists():
        empty = silver.run_silver([])
        write_handoff(empty, handoff_path)
        log.info("Silver task: no handoff to read, wrote empty placeholder.")
        return 0

    df = read_handoff(handoff_path)
    log.info(
        f"Silver task: pass-through, {len(df)} row(s) carried forward."
    )
    return len(df)


def run_gold(
    handoff: Optional[Path] = None,
    settings: Optional[Settings] = None,
) -> dict:
    """Gold task: read silver handoff, upsert into DuckDB."""
    s = settings or load_settings()
    handoff_path = handoff or s.handoff_path

    if not handoff_path.exists():
        return {"inserted": 0, "updated": 0}
    df = read_handoff(handoff_path)
    if df.empty:
        return {"inserted": 0, "updated": 0}
    return gold.run_gold(df, db_path=s.duckdb_path)


def run_notify(
    settings: Optional[Settings] = None,
) -> dict:
    """Notify task: send pending rows via SMTP email (or fallback log)."""
    s = settings or load_settings()
    return notify.run_notify(settings=s)


# ─────────────────────────────────────────
# CLI / FULL-RUN ENTRY POINT
# ─────────────────────────────────────────

def run(settings: Optional[Settings] = None) -> dict:
    """Run the full pipeline in-process. Returns the run summary."""
    s = settings or load_settings()
    bronze_n = run_bronze(settings=s)
    run_silver(settings=s)
    gold_counts = run_gold(settings=s)
    notify_counts = run_notify(settings=s)
    summary = {
        "bronze": bronze_n,
        "gold": gold_counts,
        "notify": notify_counts,
    }
    log.info(f"Pipeline summary: {summary}")
    return summary


if __name__ == "__main__":
    run()
