"""
Bronze layer — Gmail IMAP → raw list[dict].

Flow:
    1. Connect to Gmail via IMAP (with timeout and credential checks)
    2. Search for unread (UNSEEN) internship-related emails by keywords
    3. Filter to only UIDs newer than the last processed watermark
    4. Fetch each email and parse into clean text
    5. Return raw records (Message-ID included) for the silver layer

This module is responsible only for fetching. Persistence is the gold
layer's job (DuckDB), so this file does not write any file.

Security notes:
    * Credentials are read from the Settings object, never from the
      environment directly, and never logged.
    * All exceptions are mapped to typed errors that include only
      safe-to-disclose detail (no password, no token).
    * Connection has a finite timeout; a hung IMAP server can't block
      the worker forever.
"""

from __future__ import annotations

import email
import imaplib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from typing import Iterable, List, Optional, Set

from bs4 import BeautifulSoup

from autotrack.config import GMAIL_DEFAULT_MAILBOX, Settings, load_settings
from autotrack.logging import get_logger

log = get_logger(__name__)

# Keywords used both for the IMAP search and (later) for sanity checks.
# These are intentionally generic. A keyword that matches a single
# company's email would silently swallow candidates — keep this list
# domain-agnostic.
SEARCH_SUBJECT_KEYWORDS: List[str] = [
    "application", "internship", "intern", "position", "opportunity",
]


class BronzeError(RuntimeError):
    """Raised on a fatal bronze-layer failure. Safe to log."""


@dataclass(frozen=True)
class EmailRecord:
    """A single fetched email, normalized to the silver input shape."""

    email_id: str
    message_id: str
    sender: str
    subject: str
    date: str
    body: str
    scraped_at: str

    def as_dict(self) -> dict:
        """Return the legacy dict shape (silver consumes dicts)."""
        return {
            "email_id": self.email_id,
            "message_id": self.message_id,
            "sender": self.sender,
            "subject": self.subject,
            "date": self.date,
            "body": self.body,
            "scraped_at": self.scraped_at,
        }


# ─────────────────────────────────────────
# IMAP HELPERS
# ─────────────────────────────────────────

def connect_to_gmail(
    settings: Optional[Settings] = None,
) -> imaplib.IMAP4_SSL:
    """Open a secure IMAP connection to Gmail and authenticate.

    Raises :class:`BronzeError` with a safe message if creds are
    missing or the connection fails. The underlying exception is
    chained for debugging but never includes the password.
    """
    s = settings or load_settings()
    if not s.has_gmail_creds():
        raise BronzeError(
            "GMAIL_ADDRESS and GMAIL_APP_PASSWORD must be set. "
            "See .env.example for instructions."
        )

    log.info("Connecting to Gmail IMAP server")
    try:
        # ``timeout`` is a connection-level timeout. Without it, a
        # firewall that drops packets silently can hang the worker.
        mail = imaplib.IMAP4_SSL(
            s.gmail_imap_host,
            s.gmail_imap_port,
            timeout=s.imap_timeout,
        )
        # Auth happens here. If it fails, imaplib raises with the
        # server-supplied reason ("Invalid credentials"). The password
        # is never echoed in the exception, but we still log only
        # the username and a generic failure reason.
        mail.login(s.gmail_address, s.gmail_app_password)
        mail.select(s.gmail_mailbox or GMAIL_DEFAULT_MAILBOX)
    except imaplib.IMAP4.error as e:
        # Don't include the raw exception detail — it can include the
        # username. We do chain it so it's still visible in tracebacks.
        raise BronzeError(f"Gmail authentication failed: {type(e).__name__}") from e
    except OSError as e:
        # DNS failure, connection refused, timeout, etc.
        raise BronzeError(f"Gmail connection error: {e}") from e

    log.info("Connected and authenticated to Gmail.")
    return mail


def get_last_processed_uid() -> int:
    """Return the highest IMAP UID already known to the pipeline.

    The gold layer is the source of truth for the watermark in this
    design, so this is a best-effort safety net that returns 0 on
    a fresh install. Returning 0 means the next run scans all
    matching UIDs and de-duplicates at the gold layer (which has the
    actual PK).
    """
    return 0


def _search_for_keyword(
    mail: imaplib.IMAP4_SSL, keyword: str
) -> Set[bytes]:
    """Run a single IMAP SEARCH and return the matching UIDs as bytes.

    ``mail.uid("SEARCH", ...)`` returns ``(status, [data])`` where
    ``data`` is a list containing a single bytes blob of space-
    separated UIDs. In some test doubles the inner type is a list
    of bytes, so we accept both.
    """
    search_query = f'(UNSEEN SUBJECT "{keyword}")'
    status, messages = mail.uid("SEARCH", None, search_query)
    if status != "OK" or not messages or not messages[0]:
        return set()
    payload = messages[0]
    if isinstance(payload, (list, tuple)):
        # Test-double shape: list of bytes already.
        return {bytes(uid) for uid in payload}
    return set(payload.split())


def search_emails(
    mail: imaplib.IMAP4_SSL, last_uid: int = 0
) -> List[bytes]:
    """Search for UNSEEN emails whose subject matches one of the
    configured keywords, returning only UIDs > ``last_uid``.

    Note: this call leaves the matched messages as ``\\Seen`` on the
    server — IMAP's UNSEEN filter is computed at SEARCH time but
    fetching the body of a UID typically marks it Seen on most
    clients. We don't explicitly call ``mail.uid("STORE", uid,
    "+FLAGS", "(\\Seen)")`` here; the FETCH step in
    :func:`fetch_and_parse_emails` is what flips the flag. That
    behavior is what we want: a message that fails to fetch stays
    UNSEEN and will be retried on the next run.
    """
    new_uids: Set[bytes] = set()
    for keyword in SEARCH_SUBJECT_KEYWORDS:
        try:
            uids = _search_for_keyword(mail, keyword)
        except imaplib.IMAP4.error as e:
            # One bad keyword shouldn't kill the whole run.
            log.warning(f"Search failed for keyword '{keyword}': {e}")
            continue

        for uid_bytes in uids:
            try:
                uid_int = int(uid_bytes.decode())
            except (UnicodeDecodeError, ValueError):
                log.warning(f"Skipping non-numeric UID: {uid_bytes!r}")
                continue
            if uid_int > last_uid:
                new_uids.add(uid_bytes)

        log.info(
            f"Keyword '{keyword}' -> {len(uids)} hits, "
            f"{len(new_uids)} unique new after filter."
        )

    log.info(f"Total unique new emails to process: {len(new_uids)}")
    return list(new_uids)


# ─────────────────────────────────────────
# EMAIL PARSING
# ─────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")


def extract_body(msg: email.message.Message) -> str:
    """Extract clean plain text from an email message.

    Handles both ``text/plain`` and ``text/html`` parts, and skips
    attachments. Returns an empty string if nothing is found.
    """
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", "") or "")

            if "attachment" in disposition:
                continue

            if content_type == "text/plain":
                body = part.get_content()
                break
            if content_type == "text/html":
                html_content = part.get_content()
                soup = BeautifulSoup(html_content, "html.parser")
                body = soup.get_text(separator=" ", strip=True)
                break
    else:
        content_type = msg.get_content_type()
        if content_type == "text/plain":
            body = msg.get_content()
        elif content_type == "text/html":
            soup = BeautifulSoup(msg.get_content(), "html.parser")
            body = soup.get_text(separator=" ", strip=True)

    return _WHITESPACE_RE.sub(" ", body).strip()


def _safe_header(msg: email.message.Message, name: str) -> str:
    """Return a stripped header value, or '' if missing/encoding-broken."""
    raw = msg.get(name, "")
    if raw is None:
        return ""
    try:
        return str(raw).strip()
    except (TypeError, UnicodeError):
        return ""


def fetch_and_parse_emails(
    mail: imaplib.IMAP4_SSL, email_ids: Iterable[bytes]
) -> List[dict]:
    """Fetch each email by UID and return a list of raw records.

    Each record has: email_id, message_id, sender, subject, date,
    body, scraped_at. No classification happens here — that's silver.
    """
    records: List[dict] = []
    scraped_at = datetime.now(timezone.utc).isoformat()

    for email_uid in email_ids:
        try:
            status, msg_data = mail.uid("FETCH", email_uid, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                log.warning(f"FETCH failed for UID: {email_uid!r}")
                continue

            raw_email = msg_data[0][1]
            if not raw_email:
                log.warning(f"Empty payload for UID: {email_uid!r}")
                continue

            msg = email.message_from_bytes(raw_email, policy=policy.default)

            message_id = _safe_header(msg, "Message-ID")
            # The Message-ID is the gold layer's primary key, so an
            # email without one is unusable downstream. Skip it now
            # rather than letting silver drop it noisily.
            if not message_id:
                log.warning(
                    f"Skipping email without Message-ID, UID: {email_uid!r}"
                )
                continue

            record = EmailRecord(
                email_id=email_uid.decode(errors="replace"),
                message_id=message_id,
                sender=_safe_header(msg, "From"),
                subject=_safe_header(msg, "Subject"),
                date=_safe_header(msg, "Date"),
                body=extract_body(msg),
                scraped_at=scraped_at,
            )
            records.append(record.as_dict())
            log.info(
                f"Extracted: {record.subject[:60]!r} "
                f"from {record.message_id!r}"
            )
        except Exception as e:
            # Catch-all is intentional: one bad email must not kill
            # the run. We log the type (no body, no creds) and move on.
            log.error(
                f"Error processing email UID {email_uid!r}: "
                f"{type(e).__name__}: {e}"
            )
            continue

    return records


# ─────────────────────────────────────────
# PUBLIC ENTRY POINTS
# ─────────────────────────────────────────

def run_bronze(
    mail: Optional[imaplib.IMAP4_SSL] = None,
    settings: Optional[Settings] = None,
) -> List[dict]:
    """Pull new emails from Gmail.

    If ``mail`` is provided (tests, DAG), the caller owns the
    connection lifecycle. Otherwise we open and close the connection
    here. ``settings`` lets tests inject a custom Settings; otherwise
    we load from the environment.

    Returns a list of raw record dicts ready for silver.
    """
    log.info("Starting Gmail pipeline (incremental mode)")

    owns_connection = mail is None
    if owns_connection:
        mail = connect_to_gmail(settings)

    try:
        last_uid = get_last_processed_uid()
        new_email_ids = search_emails(mail, last_uid)

        if not new_email_ids:
            log.info("No new emails. Pipeline will stop here.")
            return []

        records = fetch_and_parse_emails(mail, new_email_ids)
        log.info(f"Bronze: {len(records)} records extracted.")
        return records
    finally:
        if owns_connection:
            try:
                mail.logout()
            except imaplib.IMAP4.error as e:
                # Logout failures are not fatal — we already have our
                # data. Just note it.
                log.warning(f"Logout error (ignored): {e}")
            log.info("Gmail connection closed.")


def extract(settings: Optional[Settings] = None) -> List[dict]:
    """CLI-friendly entry point. Equivalent to ``run_bronze()``."""
    return run_bronze(settings=settings)


if __name__ == "__main__":
    extract()
