"""
Silver layer — normalize, extract structured fields, classify.

Input  : list[dict] from bronze.run_bronze()
Output : pandas.DataFrame matching the silver_internships schema

Pure: no I/O, no DB calls, no network. Trivially testable.
"""

import email as email_lib
import logging
import re
from datetime import datetime
from typing import Optional

import pandas as pd

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

# Keywords to classify the email status.
# Order matters: rejection is checked first.
REJECTION_KEYWORDS = [
    "unfortunately", "regret", "not moving forward",
    "not selected", "other candidates", "not successful",
    "not been shortlisted", "unable to offer", "decided not to proceed",
]

ACCEPTANCE_KEYWORDS = [
    "congratulations", "pleased to inform", "happy to inform",
    "next steps", "welcome aboard",
    "selected", "moving forward", "interview invitation",
]

# Subject patterns, in priority order. First match wins.
# Each entry: (compiled regex, capture group index for position,
#               capture group index for company, or None)
SUBJECT_PATTERNS = [
    # "Application for Software Engineer Intern – Grab"
    re.compile(
        r"application\s+(?:for|to)\s+(.+?)\s+(?:at|[-–])\s+(\S+)",
        re.IGNORECASE,
    ),
    # "Software Engineer Intern at Grab"
    re.compile(
        r"(.+?)\s+(?:intern|position|role)\s+at\s+(\S+)",
        re.IGNORECASE,
    ),
    # "Software Engineer Intern | Grab"
    re.compile(
        r"(.+?)\s+[-–|]\s+(.+?)\s+at\s+(\S+)",
        re.IGNORECASE,
    ),
    # "Your application to Grab: Software Engineer Intern"
    re.compile(
        r"application\s+to\s+(\S+):\s*(.+)",
        re.IGNORECASE,
    ),
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
# CLASSIFICATION
# ─────────────────────────────────────────

def classify_status(subject: str, body: str) -> str:
    """
    Classify an email as rejected, advanced, or unknown based on
    keywords found in the subject or body.

    Note: the old 3-class name was 'accepted'; the silver schema uses
    'advanced' (per the PRD) — this is the only rename needed.
    """
    text = f"{subject} {body}".lower()

    if any(kw in text for kw in REJECTION_KEYWORDS):
        return "rejected"
    if any(kw in text for kw in ACCEPTANCE_KEYWORDS):
        return "advanced"
    return "unknown"


# ─────────────────────────────────────────
# TEXT NORMALIZATION
# ─────────────────────────────────────────

def normalize_text(s: str) -> str:
    """Lowercase, collapse whitespace, strip."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip().lower()


# ─────────────────────────────────────────
# STRUCTURED FIELD EXTRACTION
# ─────────────────────────────────────────

def extract_sender_domain(sender: str) -> str:
    """Return the bare domain from a From: header (lowercased)."""
    if not sender:
        return ""
    m = re.search(r"@([\w.\-]+)", sender)
    return m.group(1).lower() if m else ""


def domain_to_company(domain: str) -> str:
    """
    Best-effort human-readable company from a domain.
    'grab.com' -> 'Grab', 'stripe.co.uk' -> 'Stripe'.
    """
    if not domain:
        return ""
    root = domain.split(".")[0]
    return root.replace("-", " ").replace("_", " ").title()


def _parse_subject(
    subject: str, fallback_company: str
) -> tuple[str, str]:
    """
    Try the SUBJECT_PATTERNS in priority order and return
    (position, company). Falls back to (subject, fallback_company).
    """
    for pat in SUBJECT_PATTERNS:
        m = pat.search(subject)
        if not m:
            continue
        groups = m.groups()
        if len(groups) == 2:
            return groups[0].strip(), groups[1].strip()
        if len(groups) == 3:
            # "X | Y at Z" form: position=X, company=Z
            return groups[0].strip(), groups[2].strip()
    return subject.strip(), fallback_company


def extract_company_and_position(
    subject: str, sender: str
) -> tuple[str, str]:
    """Return (position, company_name) for an email."""
    domain = extract_sender_domain(sender)
    fallback_company = domain_to_company(domain)
    return _parse_subject(subject, fallback_company)


def parse_date_received(date_str: str) -> Optional[datetime]:
    """Parse an RFC 2822 Date header; return None on failure."""
    if not date_str:
        return None
    try:
        return email_lib.utils.parsedate_to_datetime(date_str)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run_silver(raw: list[dict]) -> pd.DataFrame:
    """
    Transform a list of bronze records into the silver DataFrame.

    Drops rows with empty Message-ID — they can't be deduped
    downstream in DuckDB.
    """
    if not raw:
        return pd.DataFrame(
            columns=[
                "message_id", "email_uid", "sender", "sender_domain",
                "company_name", "position", "subject", "date_received",
                "body_clean", "status", "alerta_enviado", "scraped_at",
            ]
        )

    rows = []
    for r in raw:
        message_id = (r.get("message_id") or "").strip()
        if not message_id:
            # No PK -> cannot be deduped in gold. Skip.
            log.warning(
                f"Descartado (sem Message-ID): {r.get('subject', '')[:60]}"
            )
            continue

        subject = r.get("subject", "")
        body    = r.get("body", "")
        sender  = r.get("sender", "")

        position, company = extract_company_and_position(subject, sender)
        body_clean = normalize_text(body)
        subject_clean = normalize_text(subject)

        # Note: status is derived from the *original* subject+body
        # (not the lowercased one) — the keyword lists are already
        # lowercased and the check is case-insensitive, so this is
        # equivalent but keeps the function call signature simple.
        status = classify_status(subject, body)

        date_received = parse_date_received(r.get("date", ""))
        scraped_at = _parse_iso(r.get("scraped_at", ""))

        rows.append({
            "message_id"   : message_id,
            "email_uid"    : r.get("email_id", ""),
            "sender"       : sender,
            "sender_domain": extract_sender_domain(sender),
            "company_name" : company,
            "position"     : position,
            "subject"      : subject_clean,
            "date_received": date_received,
            "body_clean"   : body_clean,
            "status"       : status,
            "alerta_enviado": False,
            "scraped_at"   : scraped_at,
        })

    df = pd.DataFrame(rows)
    log.info(f"Silver: {len(df)} linhas transformadas.")
    return df


def _parse_iso(s: str) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp; return None on failure."""
    if not s:
        return None
    try:
        # fromisoformat handles 'Z' suffix only on 3.11+; replace for safety.
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
