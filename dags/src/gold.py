"""
Gold layer — idempotent persistence to DuckDB.

Input  : pandas.DataFrame from silver.run_silver()
Output : dict {inserted: int, updated: int}

The primary key is ``message_id`` (RFC 5322 header). DuckDB has no
native UPSERT, so we implement it with a SELECT-then-swap pattern.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

# Container-friendly absolute path. Override via env var for tests.
DEFAULT_DB_PATH = "/opt/airflow/data/autotrack.duckdb"
DB_PATH = Path(os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH))

# The single table for now. Schema matches the silver DataFrame.
TABLE_NAME = "silver_internships"

# Columns we persist, in DB order. Boolean default is folded into the
# CREATE TABLE so we don't need to send it on every insert.
DB_COLUMNS = [
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
# HELPERS
# ─────────────────────────────────────────

def _ensure_parent(path: Path) -> None:
    """Create the parent directory for the DB file if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def _existing_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the set of message_ids already in the table."""
    rows = con.execute(
        f"SELECT message_id FROM {TABLE_NAME}"
    ).fetchall()
    return {r[0] for r in rows}


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run_gold(
    df: pd.DataFrame,
    db_path: Optional[Path] = None,
) -> dict:
    """
    Upsert the silver DataFrame into DuckDB.

    Returns ``{inserted: int, updated: int}``. The split is exact:
    rows with a message_id already in the table count as updated;
    everything else counts as inserted. Re-running with the same
    input reports ``inserted=0, updated=N``.
    """
    path = Path(db_path) if db_path is not None else DB_PATH
    _ensure_parent(path)

    if df is None or df.empty:
        log.info("Gold: dataframe vazio, nada a inserir.")
        return {"inserted": 0, "updated": 0}

    # Defensive: only keep the columns the table knows about, in order.
    payload = df[DB_COLUMNS].copy()

    with duckdb.connect(str(path)) as con:
        _ensure_table(con)
        existing = _existing_ids(con)

        incoming_ids = set(payload["message_id"].astype(str))
        new_ids = incoming_ids - existing
        update_ids = incoming_ids & existing

        new_rows = payload[payload["message_id"].isin(new_ids)]
        upd_rows = payload[payload["message_id"].isin(update_ids)]

        if not new_rows.empty:
            con.register("new_df", new_rows)
            con.execute(
                f"INSERT INTO {TABLE_NAME} ({', '.join(DB_COLUMNS)}) "
                f"SELECT {', '.join(DB_COLUMNS)} FROM new_df"
            )
            con.unregister("new_df")

        if not upd_rows.empty:
            # Swap pattern: DELETE old, INSERT new.
            ids_tuple = tuple(upd_rows["message_id"].tolist())
            placeholders = ",".join(["?"] * len(ids_tuple))
            con.execute(
                f"DELETE FROM {TABLE_NAME} "
                f"WHERE message_id IN ({placeholders})",
                ids_tuple,
            )
            con.register("upd_df", upd_rows)
            con.execute(
                f"INSERT INTO {TABLE_NAME} ({', '.join(DB_COLUMNS)}) "
                f"SELECT {', '.join(DB_COLUMNS)} FROM upd_df"
            )
            con.unregister("upd_df")

    counts = {"inserted": int(len(new_rows)), "updated": int(len(upd_rows))}
    log.info(f"Gold: {counts}")
    return counts
