"""
Centralized configuration.

Every env var in the project is read in exactly one place. This makes
the surface auditable and lets us fail fast if a required credential
is missing rather than discovering it deep inside a network call.

All paths are :class:`pathlib.Path`. All strings are stripped of
surrounding whitespace; empty strings are normalized to ``None`` so
the consumer can do ``if token: ...`` without ``.strip()``-and-check
chatter.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Default IMAP server for Gmail. Gmail is the only supported mailbox
# in v1; if a future version needs to talk to Outlook or generic IMAP,
# turn this into a config var rather than hard-coding per-provider.
GMAIL_IMAP_HOST: str = "imap.gmail.com"
GMAIL_IMAP_PORT: int = 993
GMAIL_DEFAULT_MAILBOX: str = "inbox"

# The placeholder the .env.example ships with. If a real user drops a
# token in but forgets to remove the placeholder string, we treat it
# as missing. Without this check, the placeholder would happily be
# sent to the Meta API and return a 401 every run.
META_TOKEN_PLACEHOLDER: str = "seu_token_aqui"

# Pipeline tunables.
DEFAULT_NOTIFY_MAX_PER_RUN: int = 50
DEFAULT_META_API_VERSION: str = "v20.0"
NOTIFY_BACKOFF_BASE_SECONDS: float = 2.0
NOTIFY_MAX_ATTEMPTS: int = 3
NOTIFY_HTTP_TIMEOUT_SECONDS: int = 10
IMAP_TIMEOUT_SECONDS: int = 30


def _clean(value: Optional[str]) -> Optional[str]:
    """Strip whitespace; return ``None`` for empty strings."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def _clean_path(value: Optional[str], default: Path) -> Path:
    """Return a stripped absolute Path, falling back to ``default``."""
    cleaned = _clean(value)
    return Path(cleaned) if cleaned else default


@dataclass(frozen=True)
class Settings:
    """Runtime configuration, frozen at process start."""

    # Gmail.
    gmail_address: Optional[str]
    gmail_app_password: Optional[str]
    gmail_imap_host: str
    gmail_imap_port: int
    gmail_mailbox: str

    # Meta WhatsApp.
    meta_access_token: Optional[str]
    meta_phone_number_id: Optional[str]
    meta_destination_phone: Optional[str]
    meta_api_version: str

    # Storage paths.
    duckdb_path: Path
    handoff_path: Path
    notify_fallback_log: Path

    # Pipeline.
    notify_max_per_run: int
    notify_max_attempts: int
    notify_backoff_base: float
    notify_http_timeout: int
    imap_timeout: int

    def has_gmail_creds(self) -> bool:
        return bool(self.gmail_address and self.gmail_app_password)

    def has_meta_creds(self) -> bool:
        return bool(
            self.meta_access_token
            and self.meta_access_token != META_TOKEN_PLACEHOLDER
            and self.meta_phone_number_id
            and self.meta_destination_phone
        )


def load_settings() -> Settings:
    """Read all env vars and return a frozen Settings object.

    We do NOT raise on missing Gmail creds at module import time —
    that would break unit tests and CLI tools that only need silver
    or gold. Callers that need Gmail (bronze.run_bronze) check
    :meth:`Settings.has_gmail_creds` and raise a clear error.
    """
    return Settings(
        gmail_address=_clean(os.getenv("GMAIL_ADDRESS")),
        gmail_app_password=_clean(os.getenv("GMAIL_APP_PASSWORD")),
        gmail_imap_host=os.getenv("GMAIL_IMAP_HOST", GMAIL_IMAP_HOST),
        gmail_imap_port=int(os.getenv("GMAIL_IMAP_PORT", str(GMAIL_IMAP_PORT))),
        gmail_mailbox=os.getenv("GMAIL_MAILBOX", GMAIL_DEFAULT_MAILBOX),

        meta_access_token=_clean(os.getenv("META_ACCESS_TOKEN")),
        meta_phone_number_id=_clean(os.getenv("PHONE_NUMBER_ID")),
        meta_destination_phone=_clean(os.getenv("DESTINATION_PHONE")),
        meta_api_version=os.getenv("META_API_VERSION", DEFAULT_META_API_VERSION),

        duckdb_path=_clean_path(
            os.getenv("DUCKDB_PATH"),
            Path("/opt/airflow/data/autotrack.duckdb"),
        ),
        handoff_path=_clean_path(
            os.getenv("AUTOTRACK_HANDOFF_PATH"),
            Path("/opt/airflow/data/_handoff.parquet"),
        ),
        notify_fallback_log=_clean_path(
            os.getenv("AUTOTRACK_NOTIFY_LOG"),
            Path("/opt/airflow/data/notify_log.jsonl"),
        ),

        notify_max_per_run=int(
            os.getenv("AUTOTRACK_NOTIFY_MAX_PER_RUN", str(DEFAULT_NOTIFY_MAX_PER_RUN))
        ),
        notify_max_attempts=int(
            os.getenv("AUTOTRACK_NOTIFY_MAX_ATTEMPTS", str(NOTIFY_MAX_ATTEMPTS))
        ),
        notify_backoff_base=float(
            os.getenv(
                "AUTOTRACK_NOTIFY_BACKOFF_BASE", str(NOTIFY_BACKOFF_BASE_SECONDS)
            )
        ),
        notify_http_timeout=int(
            os.getenv("AUTOTRACK_NOTIFY_TIMEOUT", str(NOTIFY_HTTP_TIMEOUT_SECONDS))
        ),
        imap_timeout=int(
            os.getenv("AUTOTRACK_IMAP_TIMEOUT", str(IMAP_TIMEOUT_SECONDS))
        ),
    )
