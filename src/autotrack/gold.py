"""
Gold layer — idempotent persistence to DuckDB.

Input  : pandas.DataFrame from silver.run_silver()
Output : dict {inserted: int, updated: int}

The primary key is ``message_id`` (RFC 5322 header). DuckDB ≥ 0.9
ships native ``ON CONFLICT`` upsert — we use it directly, which is
faster and safer than the previous SELECT-then-swap pattern.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import duckdb
import pandas as pd

from autotrack.logging import get_logger
from autotrack.silver import SILVER_COLUMNS

log = get_logger(__name__)

# Container-friendly absolute path. Override via env var for tests.
DEFAULT_DB_PATH = "/opt/airflow/data/autotrack.duckdb"

TABLE_NAME = "silver_internships"

# Columns we persist, in DB order. Boolean default is folded into
# the CREATE TABLE so we don't need to send it on every insert.
DB_COLUMNS: list[str] = [
    "message_id", "email_uid", "sender", "sender_domain",
    "company_name", "position", "subject", "date_received",
    "body_clean", "status", "scraped_at",
]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    message_id        TEXT PRIMARY KEY,
    email_uid         TEXT,
    sender            TEXT,
    sender_domain     TEXT,
    company_name      TEXT,
    position          TEXT,
    subject           TEXT,
    date_received     TIMESTAMP,
    body_clean        TEXT,
    status            TEXT,
    alerta_enviado    BOOLEAN DEFAULT FALSE,
    scraped_at        TIMESTAMP
);
"""

# ON CONFLICT upsert (DuckDB ≥ 0.9). EXCLUDED.column refers to the
# value the INSERT would have written. We update every non-PK
# column so re-running the pipeline with a slightly-cleaner
# ``body_clean`` actually reflects that change.
UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} ({", ".join(DB_COLUMNS)})
VALUES ({", ".join("?" for _ in DB_COLUMNS)})
ON CONFLICT (message_id) DO UPDATE SET
    email_uid      = EXCLUDED.email_uid,
    sender         = EXCLUDED.sender,
    sender_domain  = EXCLUDED.sender_domain,
    company_name   = EXCLUDED.company_name,
    position       = EXCLUDED.position,
    subject        = EXCLUDED.subject,
    date_received  = EXCLUDED.date_received,
    body_clean     = EXCLUDED.body_clean,
    status         = EXCLUDED.status,
    scraped_at     = EXCLUDED.scraped_at
"""


class GoldError(RuntimeError):
    """Raised on a fatal gold-layer failure."""


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _ensure_parent(path: Path) -> None:
    """Create the parent directory for the DB file if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def _row_to_tuple(row: pd.Series) -> tuple:
    """Convert a DataFrame row to a tuple in DB_COLUMNS order.

    NaN and NaT are mapped to ``None`` so DuckDB stores NULL rather
    than a stringified "nan".
    """
    return tuple(
        None if pd.isna(row[col]) else row[col] for col in DB_COLUMNS
    )


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run_gold(
    df: pd.DataFrame,
    db_path: Optional[Path] = None,
) -> Dict[str, int]:
    """Upsert the silver DataFrame into DuckDB.

    Returns ``{inserted: int, updated: int}``. The split is exact:
    rows with a message_id already in the table count as updated;
    everything else counts as inserted. Re-running with the same
    input reports ``inserted=0, updated=N``.
    """
    if df is None or df.empty:
        log.info("Gold: empty dataframe, nothing to insert.")
        return {"inserted": 0, "updated": 0}

    path = Path(db_path) if db_path is not None else Path(DEFAULT_DB_PATH)
    _ensure_parent(path)

    # Defensive: only keep the columns the table knows about, in order.
    # An upstream change to the silver schema should fail loudly here
    # rather than silently persisting extra columns to NULL.
    missing = [c for c in DB_COLUMNS if c not in df.columns]
    if missing:
        raise GoldError(f"Silver DataFrame missing columns: {missing}")
    payload = df[DB_COLUMNS].copy()

    # Pre-classify: a row is "updated" iff its message_id already
    # exists in the table. We do this with a single IN query rather
    # than one SELECT per row.
    payload_ids = payload["message_id"].astype(str).tolist()
    inserted = 0
    updated = 0

    try:
        with duckdb.connect(str(path)) as con:
            _ensure_table(con)

            # Fetch existing IDs in one shot, parameterized.
            placeholders = ",".join(["?"] * len(payload_ids))
            existing_rows = con.execute(
                f"SELECT message_id FROM {TABLE_NAME} "
                f"WHERE message_id IN ({placeholders})",
                payload_ids,
            ).fetchall()
            existing_ids = {r[0] for r in existing_rows}

            for _, row in payload.iterrows():
                row_tuple = _row_to_tuple(row)
                con.execute(UPSERT_SQL, row_tuple)
                if row["message_id"] in existing_ids:
                    updated += 1
                else:
                    inserted += 1
    except duckdb.Error as e:
        # DuckDB errors can include path-like info; we wrap rather
        # than re-raise so the caller doesn't need to import duckdb.
        raise GoldError(f"DuckDB upsert failed: {e}") from e

    counts = {"inserted": int(inserted), "updated": int(updated)}
    log.info(f"Gold: {counts}")
    return counts
