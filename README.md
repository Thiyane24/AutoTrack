# AutoTrack

A four-layer pipeline that watches a Gmail inbox for internship-related
updates, normalizes them, persists them in DuckDB, and notifies via
WhatsApp. Designed to run under Apache Airflow but each layer is also
runnable on its own for tests and ad-hoc CLI work.

```
   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
   │ BRONZE  │ -> │ SILVER  │ -> │  GOLD   │ -> │ NOTIFY  │
   │  Gmail  │    │ normali │    │ DuckDB  │    │ WhatsApp│
   │  IMAP   │    │  ze +   │    │ upsert  │    │  Meta   │
   │         │    │extract  │    │         │    │ Cloud   │
   └─────────┘    └─────────┘    └─────────┘    └─────────┘
```

## Project layout

```
.
├── dags/
│   └── internship_dag.py        # Airflow DAG definition
├── src/
│   └── autotrack/               # The installable package
│       ├── bronze.py            # IMAP fetch
│       ├── silver.py            # normalize + extract + classify
│       ├── gold.py              # DuckDB upsert
│       ├── notify.py            # WhatsApp + fallback
│       ├── pipeline.py          # Orchestrator (DAG-facing)
│       ├── cli.py               # `python -m autotrack.cli`
│       ├── config.py            # Centralized env-var reading
│       └── logging.py           # One logging setup, used by all layers
├── tests/
│   ├── unit/                    # Fast, no-network tests
│   ├── integration/             # End-to-end with a fake IMAP
│   └── conftest.py
├── docs/
│   ├── SECURITY.md              # Threat model & credential handling
│   └── ARCHITECTURE.md          # How the layers fit together
├── data/                        # DuckDB + handoffs (gitignored)
├── logs/                        # Airflow logs (gitignored)
├── .env.example                 # Copy to .env; never commit .env
├── pyproject.toml               # Build metadata + tool config
├── Dockerfile
└── docker-compose.yaml
```

## Quick start (local)

```bash
# 1. Clone and enter
git clone <repo-url> autotrack && cd autotrack

# 2. Create a venv
python -m venv .venv
. .venv/bin/activate          # or .venv\Scripts\activate on Windows

# 3. Install (editable + dev extras)
pip install -e ".[dev]"

# 4. Copy and fill the .env
cp .env.example .env
# then edit .env with your real Gmail app password + Meta creds

# 5. Run unit + integration tests
pytest

# 6. Run the pipeline (full)
python -m autotrack.cli run

# 7. Or run a single layer
python -m autotrack.cli bronze
python -m autotrack.cli silver
python -m autotrack.cli gold
python -m autotrack.cli notify
```

## Quick start (Docker / Airflow)

```bash
cp .env.example .env  # then edit
docker compose up --build
# Web UI: http://localhost:8080 (admin / admin by default)
```

## Credentials — what you need

| Variable              | Where to get it                                      |
|-----------------------|------------------------------------------------------|
| `GMAIL_ADDRESS`       | Your Gmail address                                   |
| `GMAIL_APP_PASSWORD`  | <https://myaccount.google.com/apppasswords>          |
| `META_ACCESS_TOKEN`   | <https://developers.facebook.com/apps/> (WhatsApp)   |
| `PHONE_NUMBER_ID`     | Meta dashboard → WhatsApp → API Setup                |
| `DESTINATION_PHONE`   | E.164 number to receive notifications                |

**Never commit `.env`.** It is in `.gitignore`. See `docs/SECURITY.md` for
the full credential-handling model.

## Configuration knobs (all optional)

| Env var                              | Default                                  |
|--------------------------------------|------------------------------------------|
| `LOG_LEVEL`                          | `INFO`                                   |
| `DUCKDB_PATH`                        | `/opt/airflow/data/autotrack.duckdb`     |
| `AUTOTRACK_HANDOFF_PATH`             | `/opt/airflow/data/_handoff.parquet`     |
| `AUTOTRACK_NOTIFY_LOG`               | `/opt/airflow/data/notify_log.jsonl`     |
| `AUTOTRACK_NOTIFY_MAX_PER_RUN`       | `50`                                     |
| `AUTOTRACK_NOTIFY_BACKOFF_BASE`      | `2.0` (seconds)                          |
| `AUTOTRACK_NOTIFY_TIMEOUT`           | `10` (seconds)                           |
| `AUTOTRACK_IMAP_TIMEOUT`             | `30` (seconds)                           |
| `GMAIL_IMAP_HOST`                    | `imap.gmail.com`                         |
| `GMAIL_IMAP_PORT`                    | `993`                                    |
| `GMAIL_MAILBOX`                      | `inbox`                                  |
| `META_API_VERSION`                   | `v20.0`                                  |

## Development

```bash
# Format
ruff format src tests

# Lint
ruff check src tests

# Type-check
mypy src

# Tests with coverage
pytest --cov=autotrack --cov-report=term-missing
```

## Architecture

See `docs/ARCHITECTURE.md`. Security model: see `docs/SECURITY.md`.
