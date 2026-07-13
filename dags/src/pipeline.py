"""
Pipeline orchestrator.

Glues the four layers together. Designed for two consumers:

  * The Airflow DAG (calls run_bronze / run_silver / run_gold / run_notify
    one task at a time, with the DataFrame handed off via Parquet on
    the shared ``data/`` volume).
  * The CLI (``python -m pipeline``) for ad-hoc local runs and tests.

The DataFrame handoff is a Parquet file at
``/opt/airflow/data/_handoff.parquet`` (overridable via
``AUTOTRACK_HANDOFF_PATH``). The reason: Airflow's XCom metadata
database has a 48KB limit and a real silver DataFrame will exceed
that by orders of magnitude.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

import bronze
import gold
import notify
import silver

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

HANDOFF_PATH = Path(
    os.getenv("AUTOTRACK_HANDOFF_PATH", "/opt/airflow/data/_handoff.parquet")
)

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# HANDOFF HELPERS
# ─────────────────────────────────────────

def write_handoff(df: pd.DataFrame, path: Optional[Path] = None) -> Path:
    """Persist the silver DataFrame for the next task to pick up."""
    p = Path(path) if path is not None else HANDOFF_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False)
    return p


def read_handoff(path: Optional[Path] = None) -> pd.DataFrame:
    """Read the silver DataFrame the previous task wrote."""
    p = Path(path) if path is not None else HANDOFF_PATH
    return pd.read_parquet(p)


# ─────────────────────────────────────────
# DAG-FACING ENTRY POINTS
# ─────────────────────────────────────────

def run_bronze() -> int:
    """Bronze task: pull from Gmail, persist as Parquet handoff."""
    records = bronze.run_bronze()
    # Even on no records, write an empty handoff so the next task
    # has a well-defined input.
    df = silver.run_silver(records)  # empty df if no records
    write_handoff(df)
    log.info(f"Bronze task: {len(records)} registos.")
    return len(records)


def run_silver() -> int:
    """Silver task: read bronze records, transform, re-write handoff."""
    handoff = HANDOFF_PATH
    if not handoff.exists():
        # Nothing to do; previous task produced no records.
        empty = silver.run_silver([])
        write_handoff(empty)
        return 0
    raw_df = pd.read_parquet(handoff)  # not used; placeholder for shape
    # In the current design, bronze already produced a silver DataFrame
    # (via run_silver inside run_bronze), so this task is essentially
    # a no-op pass-through. Kept as a distinct task so the DAG still
    # honors the 4-stage flow and the silver step is a separate
    # retriable unit.
    log.info("Silver task: pass-through (transform already applied).")
    return len(raw_df)


def run_gold() -> dict:
    """Gold task: read silver handoff, upsert into DuckDB."""
    if not HANDOFF_PATH.exists():
        return {"inserted": 0, "updated": 0}
    df = read_handoff()
    if df.empty:
        return {"inserted": 0, "updated": 0}
    return gold.run_gold(df)


def run_notify() -> dict:
    """Notify task: send pending rows via WhatsApp (or fallback)."""
    return notify.run_notify()


# ─────────────────────────────────────────
# CLI / FULL-RUN ENTRY POINT
# ─────────────────────────────────────────

def run() -> dict:
    """Run the full pipeline in-process. Returns the run summary."""
    raw_n = run_bronze()
    run_silver()
    gold_counts = run_gold()
    notify_counts = run_notify()
    summary = {
        "bronze": raw_n,
        "gold": gold_counts,
        "notify": notify_counts,
    }
    log.info(f"Pipeline summary: {summary}")
    return summary


if __name__ == "__main__":
    run()
