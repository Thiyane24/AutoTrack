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

# Gmail SMTP — we use the same App Password used for IMAP. This is
# the only credential surface; no Meta tokens, no phone numbers.
GMAIL_SMTP_HOST: str = "smtp.gmail.com"
GMAIL_SMTP_PORT: int = 587  # 587 = STARTTLS submission, the modern default

# Pipeline tunables.
DEFAULT_NOTIFY_MAX_PER_RUN: int = 50
NOTIFY_BACKOFF_BASE_SECONDS: float = 2.0
NOTIFY_MAX_ATTEMPTS: int = 3
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


def _clean_lower(value: Optional[str], default: Optional[str] = None) -> Optional[str]:
    """Strip + lowercase; fall back to ``default`` if empty."""
    cleaned = _clean(value)
    if cleaned is None:
        return default
    return cleaned.lower()


@dataclass(frozen=True)
class Settings:
    """Runtime configuration, frozen at process start."""

    # Gmail (used for both IMAP fetch and SMTP notify).
    gmail_address: Optional[str]
    gmail_app_password: Optional[str]
    gmail_imap_host: str
    gmail_imap_port: int
    gmail_mailbox: str
    gmail_smtp_host: str
    gmail_smtp_port: int

    # Notification target. Defaults to GMAIL_ADDRESS so a single
    # Gmail account can both watch its own inbox and receive its own
    # notifications — set NOTIFY_RECIPIENT_EMAIL to a different
    # address (e.g. an alias) to split the two.
    notify_recipient_email: Optional[str]

    # Storage paths.
    duckdb_path: Path
    handoff_path: Path
    notify_fallback_log: Path

    # Pipeline.
    notify_max_per_run: int
    notify_max_attempts: int
    notify_backoff_base: float
    imap_timeout: int

    def has_gmail_creds(self) -> bool:
        return bool(self.gmail_address and self.gmail_app_password)

    def has_notify_creds(self) -> bool:
        """True when we have everything needed to send an email.

        Both the sender's Gmail creds AND a recipient address are
        required. If the recipient is unset we fall back to the
        sender's address (common case for single-account setups).
        """
        if not self.has_gmail_creds():
            return False
        # If recipient wasn't explicitly set, we still consider it
        # configured when the sender address is present (we'll use
        # the sender's own address as the recipient at send time).
        return bool(self.gmail_address)

    def resolved_recipient(self) -> Optional[str]:
        """Return the email address we actually send notifications to."""
        return self.notify_recipient_email or self.gmail_address


def load_settings() -> Settings:
    """Read all env vars and return a frozen Settings object.

    We do NOT raise on missing Gmail creds at module import time —
    that would break unit tests and CLI tools that only need silver
    or gold. Callers that need Gmail (bronze.run_bronze, notify.run_notify)
    check :meth:`Settings.has_gmail_creds` / :meth:`has_notify_creds`
    and raise a clear error.
    """
    gmail_addr = _clean_lower(os.getenv("GMAIL_ADDRESS"))
    return Settings(
        gmail_address=gmail_addr,
        gmail_app_password=_clean(os.getenv("GMAIL_APP_PASSWORD")),
        gmail_imap_host=os.getenv("GMAIL_IMAP_HOST", GMAIL_IMAP_HOST),
        gmail_imap_port=int(os.getenv("GMAIL_IMAP_PORT", str(GMAIL_IMAP_PORT))),
        gmail_mailbox=os.getenv("GMAIL_MAILBOX", GMAIL_DEFAULT_MAILBOX),
        gmail_smtp_host=os.getenv("GMAIL_SMTP_HOST", GMAIL_SMTP_HOST),
        gmail_smtp_port=int(os.getenv("GMAIL_SMTP_PORT", str(GMAIL_SMTP_PORT))),

        # Default the recipient to the sender's own Gmail address —
        # this is the most common setup and avoids one extra config
        # var the user has to remember to set.
        notify_recipient_email=_clean_lower(
            os.getenv("NOTIFY_RECIPIENT_EMAIL"), default=gmail_addr
        ),

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
        imap_timeout=int(
            os.getenv("AUTOTRACK_IMAP_TIMEOUT", str(IMAP_TIMEOUT_SECONDS))
        ),
    )
