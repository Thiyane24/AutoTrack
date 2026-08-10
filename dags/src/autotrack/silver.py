"""
Silver layer — normalize, extract structured fields, classify.

Input  : list[dict] from bronze.run_bronze()
Output : pandas.DataFrame matching the silver_internships schema

Pure: no I/O, no DB calls, no network. Trivially testable.
"""

from __future__ import annotations

import email as email_lib
import logging
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import pandas as pd

from autotrack.logging import get_logger

log = get_logger(__name__)

# ─────────────────────────────────────────
# CLASSIFICATION KEYWORDS
# ─────────────────────────────────────────

# Order matters: rejection is checked first (it's the highest-value
# signal). Within each list, longer phrases come first so a
# substring match in a longer phrase doesn't fire on a shorter one.
REJECTION_KEYWORDS: List[str] = [
    "not moving forward",
    "not been shortlisted",
    "decided not to proceed",
    "unable to offer",
    "other candidates",
    "not selected",
    "not successful",
    "unfortunately",
    "regret",
]

ACCEPTANCE_KEYWORDS: List[str] = [
    "pleased to inform",
    "happy to inform",
    "next steps",
    "welcome aboard",
    "interview invitation",
    "congratulations",
    "moving forward",
    "selected",
]

# Subject patterns, in priority order. First match wins.
# Each pattern returns 2 capture groups: (position, company).
# The 3-group pattern below is an exception: the 3rd group is the
# company and we explicitly map it. Patterns are kept conservative
# — over-greedy regexes on subject lines produce nonsense company
# names more often than they help.
SUBJECT_PATTERNS: List[Tuple[re.Pattern, int, int]] = [
    # "Application for Software Engineer Intern - Grab"
    (re.compile(
        r"application\s+(?:for|to)\s+(.+?)\s+(?:at|[-–])\s+(\S+)",
        re.IGNORECASE,
    ), 1, 2),
    # "Software Engineer Intern at Grab"
    (re.compile(
        r"(.+?\b(?:intern|internship|position|role))\s+at\s+(\S+)",
        re.IGNORECASE,
    ), 1, 2),
    # "Software Engineer Intern | Grab"
    (re.compile(
        r"(.+?)\s+[-–|]\s+(.+?)\s+at\s+(\S+)",
        re.IGNORECASE,
    ), 1, 3),
    # "Your application to Grab: Software Engineer Intern"
    (re.compile(
        r"application\s+to\s+(\S+):\s*(.+)",
        re.IGNORECASE,
    ), 2, 1),
]

# ─────────────────────────────────────────
# TEXT NORMALIZATION
# ─────────────────────────────────────────

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(s: str) -> str:
    """Lowercase, collapse whitespace, strip."""
    if not s:
        return ""
    return _WHITESPACE_RE.sub(" ", s).strip().lower()


_SENDER_DOMAIN_RE = re.compile(r"@([\w.\-]+)")


def extract_sender_domain(sender: str) -> str:
    """Return the bare domain from a ``From:`` header (lowercased)."""
    if not sender:
        return ""
    m = _SENDER_DOMAIN_RE.search(sender)
    return m.group(1).lower() if m else ""


def domain_to_company(domain: str) -> str:
    """Best-effort human-readable company from a domain.

    ``grab.com`` -> ``Grab``; ``stripe.co.uk`` -> ``Stripe``;
    ``credit-karma.com`` -> ``Credit Karma``.
    """
    if not domain:
        return ""
    root = domain.split(".")[0]
    return root.replace("-", " ").replace("_", " ").title()


def _parse_subject(
    subject: str, fallback_company: str
) -> Tuple[str, str]:
    """Try the SUBJECT_PATTERNS in priority order.

    Returns ``(position, company)``. Falls back to
    ``(subject, fallback_company)`` if nothing matches.
    """
    if not subject:
        return "", fallback_company

    for pattern, pos_group, comp_group in SUBJECT_PATTERNS:
        m = pattern.search(subject)
        if not m:
            continue
        try:
            position = m.group(pos_group).strip()
            company = m.group(comp_group).strip()
        except (IndexError, AttributeError):
            continue
        if position and company:
            return position, company
    return subject.strip(), fallback_company


def extract_company_and_position(
    subject: str, sender: str
) -> Tuple[str, str]:
    """Return ``(position, company_name)`` for an email."""
    domain = extract_sender_domain(sender)
    fallback_company = domain_to_company(domain)
    return _parse_subject(subject, fallback_company)


def parse_date_received(date_str: str) -> Optional[datetime]:
    """Parse an RFC 2822 ``Date`` header; return None on failure."""
    if not date_str:
        return None
    try:
        return email_lib.utils.parsedate_to_datetime(date_str)
    except (TypeError, ValueError):
        return None


def _parse_iso(s: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp; return None on failure."""
    if not s:
        return None
    try:
        # ``fromisoformat`` accepts a 'Z' suffix only on 3.11+; the
        # manual replace keeps this working on 3.9 / 3.10 too.
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────
# CLASSIFICATION
# ─────────────────────────────────────────

def classify_status(subject: str, body: str) -> str:
    """Classify an email as ``rejected`` / ``advanced`` / ``unknown``.

    Rejection is checked first because an email that mentions both
    a rejection and a positive signal (e.g. "we can't move forward,
    but we wish you the best") is unambiguously a rejection.
    """
    text = f"{subject or ''} {body or ''}".lower()
    if not text.strip():
        return "unknown"

    if any(kw in text for kw in REJECTION_KEYWORDS):
        return "rejected"
    if any(kw in text for kw in ACCEPTANCE_KEYWORDS):
        return "advanced"
    return "unknown"


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

# Schema the gold layer expects. Defined once here, exported, and
# imported by gold.py so the two layers can't drift.
SILVER_COLUMNS: List[str] = [
    "message_id", "email_uid", "sender", "sender_domain",
    "company_name", "position", "subject", "date_received",
    "body_clean", "status", "alerta_enviado", "scraped_at",
]


def _empty_dataframe() -> pd.DataFrame:
    """Return an empty DataFrame with the silver schema."""
    return pd.DataFrame(columns=SILVER_COLUMNS)


def run_silver(raw: List[dict]) -> pd.DataFrame:
    """Transform a list of bronze records into the silver DataFrame.

    Drops rows whose ``message_id`` is empty — the gold layer's
    primary key. We log the drop so the operator can investigate
    a misbehaving mailbox.
    """
    if not raw:
        return _empty_dataframe()

    rows = []
    for r in raw:
        message_id = (r.get("message_id") or "").strip()
        if not message_id:
            # No PK → cannot dedupe in gold. Skip.
            log.warning(
                f"Dropped (no Message-ID): {(r.get('subject') or '')[:60]!r}"
            )
            continue

        subject = r.get("subject", "") or ""
        body = r.get("body", "") or ""
        sender = r.get("sender", "") or ""

        position, company = extract_company_and_position(subject, sender)
        body_clean = normalize_text(body)

        # IMPORTANT: the original subject (preserved as-is) is what
        # we store, not the lowercased one. The lowercased version
        # is only used for the keyword classifier (case-insensitive
        # in the classifier anyway). Storing the lowercased subject
        # was a real bug in v1 — it made the "subject" column useless
        # for display in any UI.
        status = classify_status(subject, body)

        date_received = parse_date_received(r.get("date", "") or "")
        scraped_at = _parse_iso(r.get("scraped_at", "") or "")

        rows.append({
            "message_id": message_id,
            "email_uid": r.get("email_id", "") or "",
            "sender": sender,
            "sender_domain": extract_sender_domain(sender),
            "company_name": company,
            "position": position,
            "subject": subject,
            "date_received": date_received,
            "body_clean": body_clean,
            "status": status,
            "alerta_enviado": False,
            "scraped_at": scraped_at,
        })

    df = pd.DataFrame(rows, columns=SILVER_COLUMNS)
    log.info(f"Silver: {len(df)} rows transformed.")
    return df


__all__ = [
    "SILVER_COLUMNS",
    "REJECTION_KEYWORDS",
    "ACCEPTANCE_KEYWORDS",
    "classify_status",
    "normalize_text",
    "extract_sender_domain",
    "domain_to_company",
    "extract_company_and_position",
    "parse_date_received",
    "run_silver",
]
