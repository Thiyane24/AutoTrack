# AutoTrack

A four-layer pipeline that watches a Gmail inbox for internship-related
updates, normalizes them, persists them in DuckDB, and notifies via SMTP
email. Designed to run under Apache Airflow 3.x but each layer is also
runnable on its own for tests and ad-hoc CLI work.

```
   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
   │ BRONZE  │ -> │ SILVER  │ -> │  GOLD   │ -> │ NOTIFY  │
   │  Gmail  │    │ normali │    │ DuckDB  │    │  SMTP   │
   │  IMAP   │    │ ze +    │    │ upsert  │    │  email  │
   │         │    │extract  │    │         │    │         │
   └─────────┘    └─────────┘    └─────────┘    └─────────┘
```

> **Status:** 88 tests passing across 4 layers. Runs locally on this
> machine; auto-deploys to the same machine via a self-hosted GitHub
> Actions runner on every push to `master`.

## Why AutoTrack?

Manually checking a Gmail inbox for "We regret to inform you…" emails
is the worst kind of busywork — emotionally loaded and easy to miss.
AutoTrack does it once an hour, classifies the result (rejected,
advanced, or unknown), and emails a one-liner summary so the inbox
itself doesn't have to be the source of truth.

## Project layout

```
.
├── .github/
│   └── workflows/
│       └── ci_cd_autotrack.yaml   # lint → typecheck → test → build → deploy
├── dags/
│   ├── internship_dag.py          # Airflow DAG definition
│   └── src/
│       └── autotrack/             # The installable package
│           ├── bronze.py          # IMAP fetch
│           ├── silver.py          # normalize + extract + classify
│           ├── gold.py            # DuckDB upsert
│           ├── notify.py          # SMTP email + JSONL fallback
│           ├── pipeline.py        # Orchestrator (DAG-facing)
│           ├── cli.py             # `python -m autotrack.cli <subcmd>`
│           ├── config.py          # Centralized env-var reading
│           └── logging.py         # One logging setup, used by all layers
├── tests/
│   ├── unit/                      # Fast, no-network tests
│   ├── integration/               # End-to-end with a fake IMAP
│   └── conftest.py
├── docs/
│   ├── ARCHITECTURE.md            # How the layers fit together
│   ├── SECURITY.md                # Threat model & credential handling
│   └── CONTRIBUTING.md            # Style guide + layer contract
├── data/                          # DuckDB + handoffs (gitignored)
├── logs/                          # Airflow logs (gitignored)
├── .env.example                   # Copy to .env; never commit .env
├── pyproject.toml                 # Build metadata + tool config
├── Dockerfile
└── docker-compose.yaml            # Airflow 3.x stack (sqlite-metadata DB)
```

## Quick start (local)

```bash
# 1. Clone and enter
git clone https://github.com/Thiyane24/AutoTrack.git autotrack && cd autotrack

# 2. Create a venv
python -m venv .venv
. .venv/Scripts/activate             # Windows
# .venv/bin/activate                 # macOS/Linux

# 3. Install (editable + dev extras)
pip install -e ".[dev]"

# 4. Copy and fill the .env
cp .env.example .env
# edit .env with your Gmail address + App Password

# 5. Run the test suite
pytest

# 6. Run a single layer (e.g. bronze)
python -m autotrack.cli bronze
```

## Quick start (Docker / Airflow)

```bash
cp .env.example .env  # then edit
docker compose up -d --build
# Web UI: http://localhost:8088 (admin / admin by default)
# DAG id: internship_dag (auto-runs @hourly)
```

Five containers come up:

| Service                 | Purpose                                           |
|-------------------------|---------------------------------------------------|
| `airflow-init`          | One-shot: chowns the volume + creates admin user  |
| `airflow-db-migrate`    | One-shot: runs `airflow db migrate`               |
| `airflow-webserver`     | Long-running: Airflow 3.x `api-server` on :8080  |
| `airflow-scheduler`     | Long-running: schedules DAG runs                  |
| `airflow-dag-processor` | Long-running: parses DAG files (Airflow 3 split)  |

`localhost:8088` is the published port — 8080 is occupied by EDB PEM
HTTPD on the host.

## Deployment

The pipeline is meant to run **on a single machine** that already has
Docker. The `deploy` job in CI runs there via a self-hosted runner.

### One-time setup

1. **Register a self-hosted runner on this machine.**
   <https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners>
   Pick any labels; the workflow expects at least `self-hosted` and
   `windows`. The recommended registration command looks like:

   ```powershell
   # Download the runner from the GitHub UI, then in its directory:
   ./config.cmd --url https://github.com/Thiyane24/AutoTrack --token <TOKEN>
   ./run.cmd
   ```

   Run it as a service with `svc install` so it survives reboots.

2. **Add the two required repo secrets** at
   <https://github.com/Thiyane24/AutoTrack/settings/secrets/actions>:

   | Secret name         | Value                            |
   |---------------------|----------------------------------|
   | `GMAIL_ADDRESS`     | your Gmail address               |
   | `GMAIL_APP_PASSWORD`| the App Password, no spaces      |

   The deploy job renders these into `.env` on the runner before
   `docker compose up`. The file is never committed.

### Every-time flow

Push to `master` → CI runs lint → typecheck → test matrix → build → deploy.
The deploy step is the only one that needs the runner; the other jobs
run on GitHub-hosted Linux.

```bash
git push origin master
# → green checkmark on the commit
# → docker compose stack restarted on this machine
# → http://localhost:8088/login shows the Airflow UI within ~30s
```

## CI/CD

`.github/workflows/ci_cd_autotrack.yaml` runs five jobs:

| Job          | Runs on                | Branches       | What it does                                                    |
|--------------|------------------------|----------------|-----------------------------------------------------------------|
| `lint`       | `ubuntu-latest`        | all PRs + push | `ruff check dags/src tests` + `ruff format --check`             |
| `typecheck`  | `ubuntu-latest`        | all PRs + push | `mypy dags/src`                                                 |
| `test`       | `ubuntu-latest`        | all PRs + push | `pytest tests/unit tests/integration` on Python 3.10/3.11/3.12 |
| `build-image`| `ubuntu-latest`        | push to master | `docker build` with GHA cache (no push)                         |
| `deploy`     | `self-hosted, windows` | push to master | Render `.env` from secrets, `docker compose up -d --build`, smoke-test `http://localhost:8088/login` |

A `concurrency` group (`autotrack-deploy`) ensures only one deploy runs
at a time on the self-hosted runner.

## Credentials — what you need

| Variable                | Where to get it                              |
|-------------------------|----------------------------------------------|
| `GMAIL_ADDRESS`         | Your Gmail address                           |
| `GMAIL_APP_PASSWORD`    | <https://myaccount.google.com/apppasswords>  |
| `NOTIFY_RECIPIENT_EMAIL`| Optional. Defaults to `GMAIL_ADDRESS`.       |

**Never commit `.env`.** It is in `.gitignore`. The deploy job writes
it on the runner from repo secrets; the file never reaches GitHub.

When `GMAIL_APP_PASSWORD` is empty (e.g. CI without secrets), the
notify layer falls back to writing events to
`AUTOTRACK_NOTIFY_LOG` (a JSONL file). This is the path the tests
exercise; it doesn't require any real credentials.

## Configuration knobs (all optional)

| Env var                              | Default                                  |
|--------------------------------------|------------------------------------------|
| `LOG_LEVEL`                          | `INFO`                                   |
| `DUCKDB_PATH`                        | `/opt/airflow/data/autotrack.duckdb`     |
| `AUTOTRACK_HANDOFF_PATH`             | `/opt/airflow/data/_handoff.parquet`     |
| `AUTOTRACK_NOTIFY_LOG`               | `/opt/airflow/data/notify_log.jsonl`     |
| `AUTOTRACK_NOTIFY_MAX_PER_RUN`       | `50`                                     |
| `AUTOTRACK_NOTIFY_MAX_ATTEMPTS`      | `3`                                      |
| `AUTOTRACK_NOTIFY_BACKOFF_BASE`      | `2.0` (seconds)                          |
| `AUTOTRACK_IMAP_TIMEOUT`             | `30` (seconds)                           |
| `GMAIL_IMAP_HOST`                    | `imap.gmail.com`                         |
| `GMAIL_IMAP_PORT`                    | `993`                                    |
| `GMAIL_MAILBOX`                      | `inbox`                                  |
| `GMAIL_SMTP_HOST`                    | `smtp.gmail.com`                         |
| `GMAIL_SMTP_PORT`                    | `587`                                    |

## Development

```bash
# Format
ruff format dags/src tests

# Lint
ruff check dags/src tests

# Type-check
mypy dags/src

# Tests with coverage
pytest --cov=autotrack --cov-report=term-missing
```

## Architecture

See `docs/ARCHITECTURE.md`. Security model: `docs/SECURITY.md`.
Contributing guide: `docs/CONTRIBUTING.md`.