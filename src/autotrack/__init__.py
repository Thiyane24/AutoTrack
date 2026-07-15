"""
AutoTrack package.

A four-layer pipeline that watches a Gmail inbox for internship-related
updates, normalizes them, persists them in DuckDB, and notifies via
WhatsApp. Designed to run under Apache Airflow but each layer is also
runnable on its own for tests and ad-hoc CLI work.

Layers:
    bronze  - Gmail IMAP → raw list[dict]
    silver  - normalize, extract, classify
    gold    - DuckDB upsert
    notify  - WhatsApp (Meta Cloud API) or local fallback
"""

__version__ = "0.2.0"
