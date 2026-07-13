"""
Bronze layer — Gmail IMAP → raw list[dict].

Flow:
    1. Connect to Gmail via IMAP
    2. Search for unread (UNSEEN) internship-related emails by keywords
    3. Filter to only UIDs newer than the last processed watermark
    4. Fetch each email and parse into clean text
    5. Return raw records (Message-ID included) for the silver layer

This module is responsible only for fetching. Persistence is the gold
layer's job (DuckDB), so this file no longer writes any file.
"""

import imaplib
import email
from email import policy
import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

load_dotenv()

EMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
APP_PASSWORD  = os.getenv("GMAIL_APP_PASSWORD")

IMAP_SERVER = "imap.gmail.com"
MAILBOX     = "inbox"

# Keywords used both for the IMAP search and (later) for sanity checks.
SEARCH_SUBJECT_KEYWORDS = [
    "application", "internship", "intern", "position", "opportunity"
]

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
# IMAP HELPERS
# ─────────────────────────────────────────

def connect_to_gmail() -> imaplib.IMAP4_SSL:
    """Open a secure IMAP connection to Gmail and authenticate."""
    log.info("Connecting to Gmail IMAP server")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ADDRESS, APP_PASSWORD)
    mail.select(MAILBOX)
    log.info("Connected and authenticated.")
    return mail


def get_last_processed_uid() -> int:
    """
    Returns the highest IMAP UID already known to the pipeline.
    Bronze has no on-disk state of its own anymore (gold owns it),
    so this is a best-effort safety net that the caller may ignore.
    """
    # No-op fallback: the gold layer is the source of truth for the
    # watermark. We expose 0 here so the first run is a full sweep.
    return 0


def search_emails(
    mail: imaplib.IMAP4_SSL, last_uid: int
) -> list[bytes]:
    """
    Search for UNSEEN emails whose subject matches one of the
    keywords, returning only UIDs greater than ``last_uid``.

    The UNSEEN filter is applied at the IMAP server via the search
    criterion form ``(UNSEEN SUBJECT "keyword")``.
    """
    new_uids: set[bytes] = set()

    for keyword in SEARCH_SUBJECT_KEYWORDS:
        search_query = f'(UNSEEN SUBJECT "{keyword}")'
        status, messages = mail.uid("SEARCH", None, search_query)

        if status != "OK":
            log.warning(f"Pesquisa falhou para a palavra-chave: {keyword}")
            continue

        uids = messages[0].split()
        for uid_bytes in uids:
            uid_int = int(uid_bytes.decode())
            if uid_int > last_uid:
                new_uids.add(uid_bytes)

        log.info(
            f"Palavra '{keyword}' -> Encontrados {len(new_uids)} "
            "novos e-mails (após filtro)."
        )

    log.info(
        f"Total de e-mails ÚNICOS e NOVOS a processar: {len(new_uids)}"
    )
    return list(new_uids)


# ─────────────────────────────────────────
# EMAIL PARSING
# ─────────────────────────────────────────

def extract_body(msg: email.message.Message) -> str:
    """
    Extract clean plain text from an email message.
    Handles both plain text and HTML emails.
    Returns an empty string if nothing is found.
    """
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition  = str(part.get("Content-Disposition", ""))

            if "attachment" in disposition:
                continue

            if content_type == "text/plain":
                body = part.get_content()
                break
            elif content_type == "text/html":
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

    body = re.sub(r"\s+", " ", body).strip()
    return body


def fetch_and_parse_emails(
    mail: imaplib.IMAP4_SSL, email_ids: list[bytes]
) -> list[dict]:
    """
    Fetch each email by UID and return a list of raw records.
    Each record has: email_id, message_id, sender, subject, date, body,
    scraped_at. No classification happens here — that's silver's job.
    """
    records: list[dict] = []

    for email_uid in email_ids:
        try:
            status, msg_data = mail.uid("FETCH", email_uid, "(RFC822)")

            if status != "OK":
                log.warning(f"Falhou ao extrair o e-mail UID: {email_uid}")
                continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email, policy=policy.default)

            subject    = msg.get("Subject", "").strip()
            sender     = msg.get("From", "").strip()
            date       = msg.get("Date", "").strip()
            body       = extract_body(msg)
            # Message-ID is the RFC 5322 header; globally unique and
            # stable across mailbox re-creations. It's the gold layer's
            # primary key, so we must capture it here.
            message_id = msg.get("Message-ID", "").strip()

            record = {
                "email_id"  : email_uid.decode(),
                "message_id": message_id,
                "sender"    : sender,
                "subject"   : subject,
                "date"      : date,
                "body"      : body,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

            records.append(record)
            log.info(f"Extraído: {subject[:60]}")

        except Exception as e:
            log.error(f"Erro a processar o e-mail UID {email_uid}: {e}")
            continue

    return records


# ─────────────────────────────────────────
# PUBLIC ENTRY POINTS
# ─────────────────────────────────────────

def run_bronze(
    mail: Optional[imaplib.IMAP4_SSL] = None,
) -> list[dict]:
    """
    Pull new emails from Gmail. If ``mail`` is provided (tests, DAG),
    the caller owns the connection lifecycle. Otherwise we open and
    close the connection here.

    Returns a list of raw record dicts ready for silver.
    """
    log.info("A iniciar a Pipeline do Gmail (Modo Incremental)")

    owns_connection = mail is None
    if owns_connection:
        mail = connect_to_gmail()

    try:
        last_uid = get_last_processed_uid()
        new_email_ids = search_emails(mail, last_uid)

        if not new_email_ids:
            log.info("Não há e-mails novos. A pipeline vai terminar.")
            return []

        records = fetch_and_parse_emails(mail, new_email_ids)
        log.info(f"Bronze: {len(records)} registos extraídos.")
        return records
    finally:
        if owns_connection:
            mail.logout()
            log.info("Ligação ao Gmail encerrada.")


def extract() -> list[dict]:
    """CLI-friendly entry point. Equivalent to run_bronze()."""
    return run_bronze()


if __name__ == "__main__":
    extract()
