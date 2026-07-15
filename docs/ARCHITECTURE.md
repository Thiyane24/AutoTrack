# AutoTrack — Architecture

## Why four layers?

The Medallion pattern (Bronze → Silver → Gold) gives us:

- **Independent retries.** Airflow retries a single PythonOperator
  task; a multi-layer DAG means a partial pipeline failure (e.g.
  Meta API 5xx) does not require re-fetching from Gmail.
- **Independent testability.** Silver is pure (no I/O); unit tests
  run in milliseconds without Docker, IMAP, or DuckDB.
- **Independent observability.** Each layer emits a distinct
  counter (`bronze_n`, `gold.inserted/updated`, `notify.notified/
  failed/fallback`) so a dashboard can show exactly where a
  pipeline is stuck.

## Data flow

```
 Gmail IMAP                                 Meta Cloud API
      │                                              ▲
      ▼                                              │
  ┌───────┐  raw list[dict]   ┌────────┐  DataFrame  ┌────────┐
  │ BRONZE├──────────────────►│ SILVER ├────────────►│  GOLD  │──┐
  └───────┘                   └────────┘             └────────┘  │
      │                                                       │
      │  bronze_n records extracted                            │
      ▼                                                       ▼
   logs                                            DuckDB  (notified=0)
                                                          │
                                                          ▼
                                                    ┌────────┐
                                                    │ NOTIFY ├─► Meta API
                                                    └────────┘
                                                          │
                                                          ▼
                                                DuckDB  (notified=N)
```

The bronze task already produces a silver DataFrame in-process
(transform is cheap). The DAG still has a separate `transform`
task as a pass-through so each step is independently retriable
and visible in the Airflow UI.

## Why DuckDB and not Postgres?

DuckDB is single-file, zero-config, and faster than Postgres for
the analytical queries this pipeline runs (full-table scans of
~thousands of rows). When the data volume outgrows a single
machine, swap the gold layer's `duckdb.connect` for a Postgres
or Snowflake equivalent — the upsert pattern is the same.

## Hand-off format

Between bronze→silver→gold tasks, the DataFrame is serialized to
a Parquet file at `AUTOTRACK_HANDOFF_PATH`. The reason: Airflow's
XCom metadata database has a 48KB row limit, and a real silver
DataFrame (with email bodies) is several orders of magnitude
larger.

The hand-off file is the only piece of mutable state between
tasks. Every other artifact is either an env var (config) or a
file in `data/` (DuckDB, fallback log).

## Settings object

All env-var reading happens in `autotrack.config.load_settings()`,
returning a frozen `Settings` dataclass. Functions accept an
optional `settings` argument so tests can inject a custom
`Settings` without monkey-patching the environment.
