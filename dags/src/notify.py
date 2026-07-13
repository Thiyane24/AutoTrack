"""
Notify layer — WhatsApp via Meta Cloud API, with safe fallback.

Reads from DuckDB the rows that are not yet alerted, sends each one
via the Meta Cloud API, and flips ``alerta_enviado = TRUE`` on success.

If ``META_ACCESS_TOKEN`` is missing or still the placeholder
``seu_token_aqui``, we skip the HTTP call entirely and write the
payload to ``data/notify_log.jsonl`` instead. This keeps the pipeline
green in CI and local dev without real Meta credentials.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import duckdb
import requests

import gold  # for DB_PATH and TABLE_NAME constants

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

META_ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN", "")
PHONE_NUMBER_ID    = os.getenv("PHONE_NUMBER_ID", "")
DESTINATION_PHONE  = os.getenv("DESTINATION_PHONE", "")
META_API_VERSION   = os.getenv("META_API_VERSION", "v20.0")

# The placeholder users put in .env when they haven't filled in real
# creds yet. Anything matching this is treated as "no creds".
_PLACEHOLDER = "seu_token_aqui"

# Cap per run so a flood of rejections can't accidentally spam.
MAX_PER_RUN = int(os.getenv("AUTOTRACK_NOTIFY_MAX_PER_RUN", "50"))

# Retry config — PRD §5.2: 3 retries with exponential backoff.
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 2.0  # 2s, 4s, 8s

# Local fallback log. Container-friendly absolute path; override via env.
FALLBACK_LOG_PATH = Path(
    os.getenv("AUTOTRACK_NOTIFY_LOG", "/opt/airflow/data/notify_log.jsonl")
)

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
# PURE HELPERS (testable)
# ─────────────────────────────────────────

STATUS_LABEL = {
    "rejected": "Rejeitado",
    "advanced": "Avanço",
}


def build_payload(
    company_name: str, position: str, status: str
) -> str:
    """Build the WhatsApp message body (PRD §6 acceptance #2)."""
    label = STATUS_LABEL.get(status, status.title())
    return f"🚨 Atualização | Empresa: {company_name} | Vaga: {position} | Status: {label}"


def build_meta_request(
    message: str,
    phone_number_id: str = PHONE_NUMBER_ID,
    destination_phone: str = DESTINATION_PHONE,
) -> dict:
    """Build the JSON body for the Meta Cloud API text-message call."""
    return {
        "messaging_product": "whatsapp",
        "to": destination_phone,
        "type": "text",
        "text": {"body": message},
    }


def meta_url(phone_number_id: str = PHONE_NUMBER_ID) -> str:
    return f"https://graph.facebook.com/{META_API_VERSION}/{phone_number_id}/messages"


def creds_are_placeholder(token: str = META_ACCESS_TOKEN) -> bool:
    """True when the token is empty or still the .env placeholder."""
    return not token or token == _PLACEHOLDER


# ─────────────────────────────────────────
# HTTP CALL WITH EXPONENTIAL BACKOFF
# ─────────────────────────────────────────

def _send_with_retry(message: str) -> tuple[bool, str]:
    """
    Try to send via Meta. Returns (success, detail).
    Retries up to MAX_ATTEMPTS times on 5xx / network errors with
    exponential backoff. 4xx is a hard fail (bad creds / bad number).
    """
    if creds_are_placeholder():
        return False, "creds_placeholder"

    body = build_meta_request(message)
    headers = {
        "Authorization": f"Bearer {META_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    url = meta_url()

    last_err = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=10)
        except requests.RequestException as e:
            last_err = f"network: {e}"
            log.warning(f"Meta attempt {attempt}/{MAX_ATTEMPTS} network error: {e}")
            _sleep_backoff(attempt)
            continue

        if 200 <= resp.status_code < 300:
            return True, f"http {resp.status_code}"

        if 400 <= resp.status_code < 500:
            # 4xx won't get better on retry. Surface it.
            return False, f"http {resp.status_code}: {resp.text[:200]}"

        # 5xx: retry.
        last_err = f"http {resp.status_code}: {resp.text[:200]}"
        log.warning(f"Meta attempt {attempt}/{MAX_ATTEMPTS} {last_err}")
        _sleep_backoff(attempt)

    return False, f"exhausted retries: {last_err}"


def _sleep_backoff(attempt: int) -> None:
    """Sleep with exponential backoff. Skipped in tests via monkeypatch."""
    if attempt >= MAX_ATTEMPTS:
        return
    time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))


# ─────────────────────────────────────────
# FALLBACK LOG
# ─────────────────────────────────────────

def _append_fallback_log(message_id: str, payload: str) -> None:
    """Append a JSON line to the local fallback log."""
    FALLBACK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FALLBACK_LOG_PATH.open("a", encoding="utf-8") as f:
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
        FROM {gold.TABLE_NAME}
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
        f"UPDATE {gold.TABLE_NAME} "
        f"SET alerta_enviado = TRUE WHERE message_id = ?",
        [message_id],
    )


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def run_notify(
    db_path: Optional[Path] = None,
    fallback_log_path: Optional[Path] = None,
) -> dict:
    """
    Read pending rows from DuckDB, send (or log) each one, and
    update the alerta_enviado flag. Returns a small stats dict.
    """
    path = Path(db_path) if db_path is not None else gold.DB_PATH

    # If we're in fallback mode, the log path can be overridden.
    global FALLBACK_LOG_PATH
    if fallback_log_path is not None:
        FALLBACK_LOG_PATH = Path(fallback_log_path)

    notified = 0
    failed   = 0
    fallback = 0
    in_fallback_mode = creds_are_placeholder()

    if in_fallback_mode:
        log.warning(
            "Meta creds ausentes/placeholder — a usar fallback local "
            f"em {FALLBACK_LOG_PATH}."
        )

    with duckdb.connect(str(path)) as con:
        rows = _fetch_pending(con, MAX_PER_RUN)
        log.info(f"Notify: {len(rows)} linha(s) pendente(s).")

        for message_id, company, position, status in rows:
            payload = build_payload(company, position, status)

            if in_fallback_mode:
                _append_fallback_log(message_id, payload)
                _mark_alerted(con, message_id)
                fallback += 1
                continue

            ok, detail = _send_with_retry(payload)
            if ok:
                _mark_alerted(con, message_id)
                notified += 1
                log.info(f"Notify ok: {message_id}")
            else:
                # Do NOT mark alerted: next run will retry.
                failed += 1
                log.error(f"Notify falhou: {message_id} ({detail})")

    counts = {
        "notified": notified,
        "failed": failed,
        "fallback": fallback,
    }
    log.info(f"Notify: {counts}")
    return counts
