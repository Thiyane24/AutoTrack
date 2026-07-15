"""
Notify layer — WhatsApp via Meta Cloud API, with safe fallback.

Reads from DuckDB the rows that are not yet alerted, sends each one
via the Meta Cloud API, and flips ``alerta_enviado = TRUE`` on
success.

If the Meta creds are missing or still the placeholder
``seu_token_aqui``, we skip the HTTP call entirely and write the
payload to ``data/notify_log.jsonl`` instead. This keeps the
pipeline green in CI and local dev without real Meta credentials.

Security notes:
    * Tokens never appear in logs (the headers are constructed
      inside the request and never stringified elsewhere).
    * The 4xx branch does not log the response body in full — it
      could contain a partial token in some Meta error responses.
      We log the status and a short reason, and rely on Meta's
      own dashboard for the full error.
    * The fallback log uses atomic-append with O_CREAT|O_APPEND,
      which is safe under concurrent notify runs (DuckDB-level
      locking is on the DB, not the file).
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import duckdb
import requests

from autotrack.config import META_TOKEN_PLACEHOLDER, Settings, load_settings
from autotrack.gold import TABLE_NAME
from autotrack.logging import get_logger

log = get_logger(__name__)


class NotifyError(RuntimeError):
    """Raised on a fatal notify-layer failure."""


# Phone-number sanity check: must be 8–15 digits with an optional
# leading ``+`` (E.164). We don't enforce the exact country code
# length — Meta accepts the format and validates downstream.
_PHONE_RE = re.compile(r"^\+?[1-9]\d{7,14}$")


# Status label translations.
STATUS_LABEL: Dict[str, str] = {
    "rejected": "Rejeitado",
    "advanced": "Avanço",
}


# ─────────────────────────────────────────
# PURE HELPERS (testable)
# ─────────────────────────────────────────

def build_payload(company_name: str, position: str, status: str) -> str:
    """Build the WhatsApp message body (PRD §6 acceptance #2)."""
    label = STATUS_LABEL.get(status, status.title() if status else "")
    return (
        f"🚨 Atualização | Empresa: {company_name} | "
        f"Vaga: {position} | Status: {label}"
    )


def build_meta_request(
    message: str,
    phone_number_id: str,
    destination_phone: str,
) -> dict:
    """Build the JSON body for the Meta Cloud API text-message call."""
    return {
        "messaging_product": "whatsapp",
        "to": destination_phone,
        "type": "text",
        "text": {"body": message},
    }


def meta_url(api_version: str, phone_number_id: str) -> str:
    """Build the full Meta Graph URL for a given version + phone id."""
    return (
        f"https://graph.facebook.com/{api_version}/"
        f"{phone_number_id}/messages"
    )


def creds_are_placeholder(
    token: Optional[str], placeholder: str = META_TOKEN_PLACEHOLDER
) -> bool:
    """True when the token is empty or still the .env placeholder."""
    return not token or token == placeholder


def is_valid_phone(phone: Optional[str]) -> bool:
    """Light E.164 check. Meta does the real validation."""
    if not phone:
        return False
    return bool(_PHONE_RE.match(phone))


# ─────────────────────────────────────────
# HTTP CALL WITH EXPONENTIAL BACKOFF + JITTER
# ─────────────────────────────────────────

def _sleep_backoff(attempt: int, base: float) -> None:
    """Sleep with exponential backoff + jitter.

    Jitter prevents thundering-herd retries from multiple workers
    (or multiple Airflow task instances) slamming the Meta API at
    the same instant after a brief outage.
    """
    if attempt <= 0:
        return
    delay = base * (2 ** (attempt - 1))
    # Full jitter: random value in [0, delay]. RFC suggests equal
    # jitter too; full jitter gives better spread at small N.
    delay = random.uniform(0, delay)
    time.sleep(delay)


def _send_with_retry(
    message: str,
    settings: Settings,
) -> Tuple[bool, str]:
    """Try to send via Meta. Returns ``(success, detail)``.

    Retries up to ``settings.notify_max_attempts`` times on 5xx /
    network errors with exponential backoff. 4xx is a hard fail
    (bad creds / bad number) and does not retry.
    """
    if creds_are_placeholder(settings.meta_access_token):
        return False, "creds_placeholder"

    if not is_valid_phone(settings.meta_destination_phone):
        return False, "invalid_destination_phone"

    if not settings.meta_phone_number_id:
        return False, "missing_phone_number_id"

    body = build_meta_request(
        message,
        settings.meta_phone_number_id,
        settings.meta_destination_phone,
    )
    headers = {
        "Authorization": f"Bearer {settings.meta_access_token}",
        "Content-Type": "application/json",
    }
    url = meta_url(settings.meta_api_version, settings.meta_phone_number_id)

    last_err = ""
    for attempt in range(1, settings.notify_max_attempts + 1):
        try:
            resp = requests.post(
                url, headers=headers, json=body,
                timeout=settings.notify_http_timeout,
            )
        except requests.RequestException as e:
            last_err = f"network: {type(e).__name__}"
            log.warning(
                f"Meta attempt {attempt}/{settings.notify_max_attempts} "
                f"network error: {type(e).__name__}"
            )
            _sleep_backoff(attempt, settings.notify_backoff_base)
            continue

        if 200 <= resp.status_code < 300:
            return True, f"http {resp.status_code}"

        if 400 <= resp.status_code < 500:
            # 4xx won't get better on retry. We deliberately do NOT
            # log the full response body — it can include a partial
            # access token in some Meta error shapes. Status + a
            # short reason is enough to debug.
            reason = "unauthorized" if resp.status_code == 401 else (
                "forbidden" if resp.status_code == 403 else "client_error"
            )
            return False, f"http {resp.status_code} ({reason})"

        # 5xx: retryable.
        last_err = f"http {resp.status_code}"
        log.warning(
            f"Meta attempt {attempt}/{settings.notify_max_attempts} "
            f"server error: {resp.status_code}"
        )
        _sleep_backoff(attempt, settings.notify_backoff_base)

    return False, f"exhausted retries: {last_err}"


# ─────────────────────────────────────────
# FALLBACK LOG
# ─────────────────────────────────────────

def _append_fallback_log(
    message_id: str, payload: str, log_path: Path
) -> None:
    """Append a JSON line to the local fallback log.

    The mode is ``"a"`` (append) with explicit encoding. We do not
    use the ``newline=""`` argument because we want \n line endings
    and we're not running on Windows for this code path (it is,
    but the JSONL format is line-based, so \r\n is acceptable;
    json.loads handles both transparently).
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
    (because Meta creds were missing/placeholder).
    """
    s = settings or load_settings()
    path = Path(db_path) if db_path is not None else s.duckdb_path
    log_path = Path(fallback_log_path) if fallback_log_path is not None else s.notify_fallback_log

    notified = 0
    failed = 0
    fallback = 0
    in_fallback_mode = not s.has_meta_creds()

    if in_fallback_mode:
        log.warning(
            f"Meta creds missing/placeholder — using local fallback "
            f"at {log_path}."
        )

    with duckdb.connect(str(path)) as con:
        rows = _fetch_pending(con, s.notify_max_per_run)
        log.info(f"Notify: {len(rows)} pending row(s).")

        for message_id, company, position, status in rows:
            payload = build_payload(company, position, status)

            if in_fallback_mode:
                _append_fallback_log(message_id, payload, log_path)
                _mark_alerted(con, message_id)
                fallback += 1
                continue

            ok, detail = _send_with_retry(payload, s)
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

    counts = {
        "notified": notified,
        "failed": failed,
        "fallback": fallback,
    }
    log.info(f"Notify: {counts}")
    return counts
