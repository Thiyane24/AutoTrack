"""
Shared pytest fixtures.

Adds the project's source tree to sys.path so tests can import
``bronze`` / ``silver`` / ``gold`` / ``notify`` / ``pipeline``
without installing the package.
"""

import sys
from pathlib import Path

import pytest

# Make dags/src/ importable as `import bronze`, `import silver`, etc.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "dags" / "src"
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
