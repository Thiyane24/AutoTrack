"""
AutoTrack package.

A four-layer pipeline that watches a Gmail inbox for internship-related
updates, normalizes them, persists them in DuckDB, and notifies via
SMTP email. Designed to run under Apache Airflow 3.x but each layer
is also runnable on its own for tests and ad-hoc CLI work.

Layers:
    bronze  - Gmail IMAP → raw list[dict]
    silver  - normalize, extract, classify
    gold    - DuckDB upsert
    notify  - SMTP email (or local JSONL fallback when no creds)
"""

__version__ = "0.2.0"
