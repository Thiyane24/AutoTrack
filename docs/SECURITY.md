# AutoTrack — Security Model

This document describes the threats AutoTrack is designed against and the
controls in place. It is intentionally short and operational; threat-model
discussions that have not been implemented do not belong here.

## Threat model (in scope)

1. **Credential leakage via version control.** A developer accidentally
   commits `.env` to GitHub. The token/password is exposed to anyone
   with read access to the repo and to GitHub's secret-scanning bots.
2. **Credential leakage via logs.** A network error message includes the
   password or the bearer token in its text and the error gets logged
   to disk or sent to a log aggregator.
3. **Hanging workers.** A flaky network keeps the IMAP/HTTP client open
   forever, blocking the Airflow worker and starving other DAGs.
4. **Replay / double-send.** A retry of the notify task double-sends a
   WhatsApp message to the user.
5. **Schema confusion.** A change to the silver DataFrame silently
   drops a column the gold layer expects, corrupting DuckDB.
6. **Unauthorized placeholder credentials.** A user copies `.env.example`
   to `.env` but forgets to fill in a real Meta token. The placeholder
   is sent to Meta's API and the pipeline returns 401 every run.
7. **Thundering-herd retries.** A brief Meta outage causes all
   in-flight workers to retry at the same instant, prolonging the
   outage.

## Controls

### 1. No committed secrets

- `.env` is in `.gitignore`.
- `.env.example` ships with **empty** values for every credential.
- `python-dotenv` is added to `requirements.txt` so the local dev
  flow is the same as production (env-vars via `.env`).
- CI tests force `META_ACCESS_TOKEN=""` and friends to empty strings
  so a leaked secret from a developer's local `.env` would not
  reach the test run.

### 2. No credentials in logs or error messages

- `bronze.connect_to_gmail` wraps any `imaplib.IMAP4.error` in a
  `BronzeError` that includes only the exception **type name**, not
  the message (which can include the username).
- `notify._send_with_retry` deliberately logs the HTTP status code
  but **not** the response body — Meta 4xx errors occasionally
  echo a partial token in the body.
- The `Authorization: Bearer …` header is constructed in-place
  and never stringified elsewhere, so it cannot be captured by a
  broad `repr()` of the request object.
- Exceptions are chained (raise … from e) for debugging, but the
  outer message is sanitized.

### 3. Timeouts on all network calls

| Call               | Timeout env var                  | Default |
|--------------------|----------------------------------|---------|
| IMAP connect       | `AUTOTRACK_IMAP_TIMEOUT`         | 30 s    |
| Meta HTTP          | `AUTOTRACK_NOTIFY_TIMEOUT`       | 10 s    |

A hung server can no longer block an Airflow worker indefinitely.

### 4. Bounded notify retries

- Max attempts: `AUTOTRACK_NOTIFY_MAX_ATTEMPTS` (default 3).
- 4xx is a hard fail — no retry, no log spam.
- Per-run cap: `AUTOTRACK_NOTIFY_MAX_PER_RUN` (default 50) so a
  flood of rejections cannot accidentally spam the user.
- Backoff includes full-jitter randomization
  (`time.sleep(random.uniform(0, base * 2**attempt))`) to spread
  retries across workers.

### 5. Idempotent persistence

- DuckDB upsert uses `ON CONFLICT (message_id) DO UPDATE` (native
  to DuckDB ≥ 0.9). Re-running the same bronze records results in
  `inserted=0, updated=N` — no duplicates.
- The notify layer only flips `alerta_enviado = TRUE` after a
  successful send; a 5xx that exhausts retries leaves the flag
  `FALSE`, so the next run re-attempts.

### 6. Schema validation

- `gold.run_gold` raises `GoldError` (a typed, catchable error)
  if the silver DataFrame is missing any of the expected columns,
  rather than silently inserting NULLs.
- The silver column list (`SILVER_COLUMNS`) is exported and
  imported by gold, so the two layers cannot drift.

### 7. Placeholder detection

- `notify.creds_are_placeholder` returns `True` for empty strings
  and the literal `"seu_token_aqui"` shipped in `.env.example`.
- `Settings.has_meta_creds` re-checks this and the pipeline falls
  back to a local JSONL log instead of hitting the API with
  garbage credentials.

## Out of scope (acknowledged but not built)

- **At-rest encryption of DuckDB.** A single DuckDB file contains all
  PII (sender email, email body). For a real deployment, run DuckDB
  on an encrypted volume or use a managed warehouse.
- **PII redaction in logs.** Log lines can include the email subject
  (truncated to 60 chars in bronze). This is fine for development;
  production should pipe through a redaction filter.
- **OAuth for Gmail.** We use an App Password, which is fine for
  personal use. Production deployments with multiple users should
  move to OAuth2.
- **Secrets manager integration.** The pipeline reads from
  environment variables; secrets should be injected by the
  orchestrator (Kubernetes Secret, Airflow Variable, AWS
  Secrets Manager, etc.). The application does not know or
  care which one is used.

## Reporting vulnerabilities

Open a private GitHub Security Advisory or email the maintainer
directly. Do not open a public issue.
