"""
Notify layer — email notifications via Gmail SMTP, with safe fallback.

Reads from DuckDB the rows that are not yet alerted, sends each one
as an email through Gmail's SMTP submission server, and flips
``alerta_enviado = TRUE`` on success.

If the Gmail sender creds (``GMAIL_ADDRESS`` / ``GMAIL_APP_PASSWORD``)
are missing, we skip the SMTP call entirely and write the payload
to ``data/notify_log.jsonl`` instead. This keeps the pipeline green
in CI and local dev without real credentials.

Why SMTP (and not a chat-API):
    * No third-party account, sandbox opt-in, or phone-number
      verification dance.
    * The Gmail App Password is already required for the bronze
      layer's IMAP fetch — zero new credentials.
    * Each notification is a real, searchable email in the user's
      inbox, which doubles as a permanent audit log.

Security notes:
    * Passwords never appear in logs. The SMTP ``login`` call is
      wrapped so a failed auth raises our typed error with no detail
      beyond the exception class name.
    * STARTTLS is used (port 587 by default) so the App Password
      travels over an encrypted channel. SSL-on-connect (port 465)
      is also supported if ``GMAIL_SMTP_PORT`` is overridden.
    * The fallback log uses atomic append mode and is safe under
      concurrent notify runs.
"""

from __future__ import annotations

import json
import logging
import random
import re
import smtplib
import time
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Optional, Tuple

import duckdb

from autotrack.config import Settings, load_settings
from autotrack.gold import TABLE_NAME
from autotrack.logging import get_logger

log = get_logger(__name__)


class NotifyError(RuntimeError):
    """Raised on a fatal notify-layer failure."""


# Lightweight email syntax check. The local-part allows the common
# subset that Gmail itself accepts; we don't try to be RFC-perfect
# because the SMTP server does the real validation.
_EMAIL_RE = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"
)


# Status → human-readable label map. Falls back to the status itself
# when an unknown status sneaks through (kept conservative).
STATUS_LABEL: Dict[str, str] = {
    "rejected": "Rejected",
    "advanced": "Next step",
    "application_received": "Application Received",
    "offer": "Offer",
    "interview_invite": "Interview Invitation",
    "unknown": "Update",
}


# ─────────────────────────────────────────
# PURE HELPERS (testable, no I/O)
# ─────────────────────────────────────────

def build_payload(company_name: str, position: str, status: str) -> str:
    """Build the plain-text notification body."""
    label = STATUS_LABEL.get(status, status.title() if status else "")
    return (
        f"Update on your internship application\n"
        f"\n"
        f"  Company : {company_name or 'Unknown'}\n"
        f"  Role    : {position or 'Unknown'}\n"
        f"  Status  : {label}\n"
    )


def build_email_message(
    subject: str,
    body: str,
    sender: str,
    recipient: str,
    message_id: str,
) -> EmailMessage:
    """Build an RFC 5322 ``EmailMessage`` with a stable Message-ID.

    Setting a deterministic Message-ID on outbound mail makes it
    easy to correlate an email-in-the-inbox with a DuckDB row when
    debugging. The original Gmail ``Message-ID`` is included as a
    prefix so the relationship is preserved.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg["X-AutoTrack-Message-ID"] = message_id
    msg.set_content(body)
    return msg


def subject_for(company_name: str, status: str) -> str:
    """Build the email subject line."""
    label = STATUS_LABEL.get(status, status.title() if status else "Update")
    company = company_name or "An employer"
    return f"[AutoTrack] {label} — {company}"


def is_valid_email(address: Optional[str]) -> bool:
    """Light local-syntax check. The SMTP server does the real one."""
    if not address:
        return False
    return bool(_EMAIL_RE.match(address.strip()))


# ─────────────────────────────────────────
# RETRY / BACKOFF
# ─────────────────────────────────────────

def _sleep_backoff(attempt: int, base: float) -> None:
    """Sleep with exponential backoff + jitter.

    Jitter prevents thundering-herd retries from multiple workers
    (or multiple Airflow task instances) slamming the SMTP server
    at the same instant after a brief outage.
    """
    if attempt <= 0:
        return
    delay = base * (2 ** (attempt - 1))
    # Full jitter: random value in [0, delay]. This gives better
    # spread at small N than the equal-jitter variant.
    delay = random.uniform(0, delay)
    time.sleep(delay)


def _send_with_retry(
    message: EmailMessage,
    settings: Settings,
) -> Tuple[bool, str]:
    """Open an SMTP connection, send ``message``, close it.

    Retries up to ``settings.notify_max_attempts`` times on network
    errors with exponential backoff. Auth failures and other
    ``SMTPException``-class errors are treated as hard failures —
    retrying won't fix a bad password.
    """
    sender = settings.gmail_address
    recipient = settings.resolved_recipient()

    if not sender or not settings.gmail_app_password:
        return False, "missing_sender_creds"
    if not is_valid_email(recipient):
        return False, "invalid_recipient"

    last_err = ""
    for attempt in range(1, settings.notify_max_attempts + 1):
        try:
            # ``with`` block guarantees the connection is closed
            # even on exception — a leaked SMTP socket would otherwise
            # pile up until the worker restarts.
            with smtplib.SMTP(settings.gmail_smtp_host, settings.gmail_smtp_port) as smtp:
                # STARTTLS upgrades the plain-text channel to TLS.
                # Gmail's submission port (587) requires it.
                smtp.starttls()
                smtp.login(sender, settings.gmail_app_password)
                smtp.send_message(message)
            return True, "sent"

        except smtplib.SMTPAuthenticationError as e:
            # 535: bad password. Won't get better on retry. We log
            # the exception class only — the message can include the
            # username which we don't want in operator-facing logs.
            return False, f"auth: {type(e).__name__}"

        except smtplib.SMTPRecipientsRefused as e:
            # 550-class: bad recipient address. Hard fail.
            return False, f"recipient_refused: {type(e).__name__}"

        except (smtplib.SMTPException, OSError) as e:
            # Covers SMTPException (connect, data, protocol, etc.)
            # and OSError (DNS, refused connection, timeout). Both are
            # transient — worth a retry.
            last_err = f"{type(e).__name__}"
            log.warning(
                f"SMTP attempt {attempt}/{settings.notify_max_attempts} "
                f"transient error: {last_err}"
            )
            _sleep_backoff(attempt, settings.notify_backoff_base)
            continue

    return False, f"exhausted retries: {last_err}"


# ─────────────────────────────────────────
# FALLBACK LOG (when creds missing)
# ─────────────────────────────────────────

def _append_fallback_log(
    message_id: str, payload: str, log_path: Path
) -> None:
    """Append a JSON line to the local fallback log.

    Atomic append with UTF-8 encoding. The file is JSONL so any
    reader can ``json.loads(line)`` per line.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps({
            "message_id": message_id,
            "payload": payload,
        }, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────

def _fetch_pending(
    con: duckdb.DuckDBPyConnection, limit: int
) -> list[tuple]:
    """Return rows that are not yet alerted."""
    return con.execute(
        f"""
        SELECT message_id, company_name, position, status
        FROM {TABLE_NAME}
        WHERE alerta_enviado = FALSE
          AND status IN ('rejected', 'advanced')
        ORDER BY date_received DESC NULLS LAST
        LIMIT ?
        """,
        [limit],
    ).fetchall()


def _mark_alerted(
    con: duckdb.DuckDBPyConnection, message_id: str
) -> None:
    con.execute(
        f"UPDATE {TABLE_NAME} "
        f"SET alerta_enviado = TRUE WHERE message_id = ?",
        [message_id],
    )


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run_notify(
    db_path: Optional[Path] = None,
    fallback_log_path: Optional[Path] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, int]:
    """Read pending rows from DuckDB, send (or log) each one, and
    update the ``alerta_enviado`` flag.

    Returns ``{notified, failed, fallback}``. ``fallback`` counts
    rows that were written to the local log instead of sent
    (because Gmail creds were missing).

    If the target table doesn't exist yet (e.g. ``notify`` is run
    manually before ``gold``), we treat it as a no-op: no rows to
    notify, no error. This makes individual layers safe to run in
    any order, which is the whole point of the decoupled design.
    """
    s = settings or load_settings()
    path = Path(db_path) if db_path is not None else s.duckdb_path
    log_path = (
        Path(fallback_log_path) if fallback_log_path is not None
        else s.notify_fallback_log
    )

    notified = 0
    failed = 0
    fallback = 0
    in_fallback_mode = not s.has_notify_creds()

    if in_fallback_mode:
        log.warning(
            f"Gmail notify creds missing — using local fallback "
            f"at {log_path}."
        )

    try:
        with duckdb.connect(str(path)) as con:
            # Defensive: if the table doesn't exist (e.g. notify was
            # invoked before gold), there's nothing to do and that's
            # not an error.
            table_exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = ?",
                [TABLE_NAME],
            ).fetchone()[0]
            if not table_exists:
                log.info(
                    f"Notify: table {TABLE_NAME} does not exist yet; "
                    f"nothing to do."
                )
                return {"notified": 0, "failed": 0, "fallback": 0}

            rows = _fetch_pending(con, s.notify_max_per_run)
            log.info(f"Notify: {len(rows)} pending row(s).")

            for message_id, company, position, status in rows:
                payload = build_payload(company, position, status)

                if in_fallback_mode:
                    _append_fallback_log(message_id, payload, log_path)
                    _mark_alerted(con, message_id)
                    fallback += 1
                    continue

                email = build_email_message(
                    subject=subject_for(company, status),
                    body=payload,
                    sender=s.gmail_address or "",
                    recipient=s.resolved_recipient() or "",
                    message_id=message_id,
                )

                ok, detail = _send_with_retry(email, s)
                if ok:
                    _mark_alerted(con, message_id)
                    notified += 1
                    log.info(f"Notify ok: {message_id}")
                else:
                    # Do NOT mark alerted: next run will retry. The
                    # cap (notify_max_per_run) prevents a flood of
                    # rejections from accidentally spamming.
                    failed += 1
                    log.error(f"Notify failed: {message_id} ({detail})")
    except duckdb.Error as e:
        raise NotifyError(f"DuckDB notify read failed: {e}") from e

    counts = {
        "notified": notified,
        "failed": failed,
        "fallback": fallback,
    }
    log.info(f"Notify: {counts}")
    return counts


__all__ = [
    "NotifyError",
    "STATUS_LABEL",
    "build_payload",
    "build_email_message",
    "subject_for",
    "is_valid_email",
    "run_notify",
]
