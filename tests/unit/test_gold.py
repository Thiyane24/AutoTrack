"""Unit tests for the gold layer (DuckDB upsert)."""

import pandas as pd
import pytest

import gold
import silver


def _make_df(records: list[dict]) -> pd.DataFrame:
    """Run records through silver so the schema matches exactly."""
    return silver.run_silver(records)


class TestGoldUpsert:
    def test_first_run_inserts_all(self, sample_raw_records, tmp_duckdb_path):
        df = _make_df(sample_raw_records)
        counts = gold.run_gold(df, db_path=tmp_duckdb_path)
        assert counts == {"inserted": 3, "updated": 0}

    def test_second_run_updates_all(self, sample_raw_records, tmp_duckdb_path):
        df = _make_df(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)
        counts = gold.run_gold(df, db_path=tmp_duckdb_path)
        assert counts == {"inserted": 0, "updated": 3}

    def test_no_duplicate_rows_after_repeat(
        self, sample_raw_records, tmp_duckdb_path
    ):
        df = _make_df(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        import duckdb
        with duckdb.connect(str(tmp_duckdb_path)) as con:
            count = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME}"
            ).fetchone()[0]
        assert count == 3

    def test_mixed_new_and_updated(
        self, sample_raw_records, tmp_duckdb_path
    ):
        df = _make_df(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        # Second batch: 1 brand-new record + 1 update + 1 unchanged.
        new_records = sample_raw_records[:1] + [
            {
                "email_id": "999",
                "message_id": "<fresh@newco.com>",
                "sender": "New Co <hr@newco.com>",
                "subject": "Application for SWE Intern - NewCo",
                "date": "Wed, 15 Jul 2026 09:00:00 +0000",
                "body": "We regret to inform you...",
                "scraped_at": "2026-07-15T09:00:30+00:00",
            },
        ]
        df2 = _make_df(new_records)
        counts = gold.run_gold(df2, db_path=tmp_duckdb_path)
        # 1 new (the fresh message_id), 1 updated (the carried-over
        # sample record). The 3rd record is simply not present in the
        # incoming batch — neither new nor updated.
        assert counts["inserted"] == 1
        assert counts["updated"] == 1

    def test_empty_dataframe_is_noop(self, tmp_duckdb_path):
        empty = pd.DataFrame()
        counts = gold.run_gold(empty, db_path=tmp_duckdb_path)
        assert counts == {"inserted": 0, "updated": 0}

    def test_creates_parent_dir(self, tmp_path):
        nested = tmp_path / "deep" / "nested" / "test.duckdb"
        df = _make_df([
            {
                "email_id": "1",
                "message_id": "<a@b.com>",
                "sender": "x@y.com",
                "subject": "Hi",
                "date": "",
                "body": "Body",
                "scraped_at": "2026-01-01T00:00:00+00:00",
            }
        ])
        gold.run_gold(df, db_path=nested)
        assert nested.exists()

    def test_alerta_enviado_defaults_to_false(
        self, sample_raw_records, tmp_duckdb_path
    ):
        df = _make_df(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        import duckdb
        with duckdb.connect(str(tmp_duckdb_path)) as con:
            rows = con.execute(
                f"SELECT message_id, alerta_enviado FROM {gold.TABLE_NAME}"
            ).fetchall()
        # Every row should be present and have alerta_enviado = False.
        assert len(rows) == 3
        assert all(r[1] is False or r[1] == 0 for r in rows)
