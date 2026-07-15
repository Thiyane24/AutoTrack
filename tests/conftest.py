"""
Shared pytest fixtures.

Adds the project's source tree (``src/``) to ``sys.path`` so tests
can import ``autotrack`` without installing the package. ``src/``
is also added by the ``pythonpath`` setting in ``pyproject.toml``;
this conftest makes the same path available to ``pytest`` runs
that don't honor that setting.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make src/ importable as `import autotrack` for test collection
# environments that don't pick up pyproject.toml's pythonpath.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def tmp_duckdb_path(tmp_path) -> Path:
    """A throwaway DuckDB file path under pytest's tmp dir."""
    return tmp_path / "autotrack.duckdb"


@pytest.fixture
def tmp_handoff_path(tmp_path) -> Path:
    """A throwaway Parquet handoff path under pytest's tmp dir."""
    return tmp_path / "_handoff.parquet"


@pytest.fixture
def tmp_fallback_log(tmp_path) -> Path:
    """A throwaway notify fallback log path under pytest's tmp dir."""
    return tmp_path / "notify_log.jsonl"


@pytest.fixture
def sample_raw_records() -> list[dict]:
    """Three bronze records: one rejected, one advanced, one unknown."""
    return [
        {
            "email_id": "101",
            "message_id": "<abc@grab.com>",
            "sender": "Recruiting <noreply@grab.com>",
            "subject": "Application for Software Engineer Intern - Grab",
            "date": "Tue, 14 Jul 2026 09:42:11 +0000",
            "body": "Unfortunately, we will not be moving forward with your application.",
            "scraped_at": "2026-07-14T09:42:30+00:00",
        },
        {
            "email_id": "102",
            "message_id": "<xyz@stripe.com>",
            "sender": "Stripe Careers <no-reply@stripe.com>",
            "subject": "Next steps: Backend Engineer Intern at Stripe",
            "date": "Tue, 14 Jul 2026 10:00:00 +0000",
            "body": "Congratulations! We are pleased to inform you of next steps.",
            "scraped_at": "2026-07-14T10:00:30+00:00",
        },
        {
            "email_id": "103",
            "message_id": "<nope@unknown.io>",
            "sender": "Some Sender <hello@unknown.io>",
            "subject": "Hello there",
            "date": "Tue, 14 Jul 2026 11:00:00 +0000",
            "body": "Just a friendly note with no job-related content.",
            "scraped_at": "2026-07-14T11:00:30+00:00",
        },
    ]


@pytest.fixture
def empty_settings(tmp_duckdb_path, tmp_handoff_path, tmp_fallback_log):
    """A Settings object pointing at temp paths, no real creds."""
    from autotrack.config import Settings

    return Settings(
        gmail_address=None,
        gmail_app_password=None,
        gmail_imap_host="imap.gmail.com",
        gmail_imap_port=993,
        gmail_mailbox="inbox",
        meta_access_token=None,
        meta_phone_number_id=None,
        meta_destination_phone=None,
        meta_api_version="v20.0",
        duckdb_path=tmp_duckdb_path,
        handoff_path=tmp_handoff_path,
        notify_fallback_log=tmp_fallback_log,
        notify_max_per_run=50,
        notify_max_attempts=3,
        notify_backoff_base=0.0,  # speed tests up
        notify_http_timeout=10,
        imap_timeout=30,
    )
