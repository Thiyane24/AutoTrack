# AutoTrack contributor notes

## Coding style

- **Format:** `ruff format` (settings in `pyproject.toml`).
- **Lint:** `ruff check` (settings in `pyproject.toml`).
- **Type hints:** required on all new public functions. The
  project is not 100% strict-mypy yet but new code should
  type-check.
- **Logging:** always `from autotrack.logging import get_logger`.
  Never call `logging.basicConfig` from a module — it's done
  once in `autotrack.logging.configure`.
- **Error types:** each layer defines its own `XxxError`
  (subclass of `RuntimeError`) so callers can catch layer
  failures without `except Exception:`.

## Layer contract

- Bronze returns a `list[dict]` with the keys:
  `email_id, message_id, sender, subject, date, body, scraped_at`.
  It is pure with respect to the rest of the pipeline — no file
  or DB writes.
- Silver accepts a `list[dict]` (the bronze shape) and returns a
  `pandas.DataFrame` with columns listed in `SILVER_COLUMNS`.
  Pure: no I/O.
- Gold accepts the silver DataFrame and upserts to DuckDB.
- Notify reads DuckDB and writes back the `alerta_enviado` flag.

If you change a contract, update the corresponding test in
`tests/unit/test_<layer>.py` and the documentation above.

## Testing

- Unit tests are in `tests/unit/` and must not require a network
  or a real DB.
- Integration tests are in `tests/integration/` and use a fake
  IMAP server.
- Run `pytest` from the repo root. The `conftest.py` adds
  `dags/src/` to `sys.path` so `import autotrack` works without a
  prior `pip install -e`.
