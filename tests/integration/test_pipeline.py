"""
Integration test for the full pipeline.

Stubs imaplib.IMAP4_SSL with a fake that returns 3 canned emails
(rejection, acceptance, unknown), then runs the full pipeline and
asserts that:
  * DuckDB ends up with 3 rows,
  * statuses are correct,
  * notify_log.jsonl has 2 entries (rejected + advanced),
  * running the pipeline a second time is a no-op for the DB
    (idempotency guarantee).
"""

from __future__ import annotations

import json
from unittest.mock import patch

import duckdb
import pytest

from autotrack import bronze, gold, notify, pipeline, silver


# ─────────────────────────────────────────
# Fake IMAP server
# ─────────────────────────────────────────

def _build_raw_email(
    message_id: str, sender: str, subject: str, body: str
) -> bytes:
    """Build a minimal RFC 822 message (single-part, text/plain)."""
    raw = (
        f"Message-ID: {message_id}\r\n"
        f"From: {sender}\r\n"
        f"Subject: {subject}\r\n"
        f"Date: Tue, 14 Jul 2026 09:42:11 +0000\r\n"
        f"Content-Type: text/plain; charset=utf-8\r\n"
        f"\r\n"
        f"{body}\r\n"
    )
    return raw.encode("utf-8")


class FakeIMAP:
    """Just enough of imaplib.IMAP4_SSL to make bronze.run_bronze work."""

    EMAILS = [
        (
            b"101",
            _build_raw_email(
                "<rej@grab.com>",
                "Recruiting <noreply@grab.com>",
                "Application for Software Engineer Intern - Grab",
                "Unfortunately, we will not be moving forward with your application.",
            ),
        ),
        (
            b"102",
            _build_raw_email(
                "<adv@stripe.com>",
                "Stripe Careers <no-reply@stripe.com>",
                "Next steps: Backend Engineer Intern at Stripe",
                "Congratulations! We are pleased to inform you of next steps.",
            ),
        ),
        (
            b"103",
            _build_raw_email(
                "<unk@unknown.io>",
                "Some Sender <hello@unknown.io>",
                "Hello there",
                "Just a friendly note with no job-related content.",
            ),
        ),
    ]

    def __init__(self, *args, **kwargs):
        self.selected = False
        self.logged_in = False

    def login(self, user, password):
        self.logged_in = True
        return ("OK", [b""])

    def select(self, mailbox):
        self.selected = True
        return ("OK", [b"1"])

    def uid(self, command, charset, query):
        if command == "SEARCH":
            # Return all UIDs as a single space-separated bytes blob
            # inside a list — this matches real imaplib's response
            # shape: (status, [bytes_blob]).
            uids = b" ".join(uid for uid, _ in self.EMAILS)
            return ("OK", [uids])
        if command == "FETCH":
            # Parse the UID from the query bytes.
            for uid, raw in self.EMAILS:
                if uid in query:
                    return ("OK", [(b"", raw)])
            return ("NO", [b""])
        return ("NO", [b""])

    def logout(self):
        return ("BYE", [b""])


# ─────────────────────────────────────────
# Tests
# ─────────────────────────────────────────

class TestFullPipeline:
    def test_end_to_end(
        self, tmp_path, monkeypatch, empty_settings,
    ):
        # Use a fresh Settings pointing at temp paths and with
        # fake Gmail creds (the IMAP is stubbed below).
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "gmail_address": "test@example.com",
               "gmail_app_password": "fake-app-pw",
               "duckdb_path": tmp_path / "autotrack.duckdb",
               "handoff_path": tmp_path / "_handoff.parquet",
               "notify_fallback_log": tmp_path / "notify_log.jsonl"}
        )

        # Stub IMAP so bronze doesn't hit the network.
        with patch.object(
            bronze.imaplib, "IMAP4_SSL", side_effect=lambda *a, **kw: FakeIMAP()
        ):
            summary = pipeline.run(settings=s)

        # Bronze pulled 3, gold inserted 3, notify fell back on 2.
        assert summary["bronze"] == 3
        assert summary["gold"] == {"inserted": 3, "updated": 0}
        assert summary["notify"]["fallback"] == 2
        assert summary["notify"]["notified"] == 0
        assert summary["notify"]["failed"] == 0

        # DuckDB has the rows with the right statuses.
        with duckdb.connect(str(s.duckdb_path)) as con:
            rows = con.execute(
                f"SELECT message_id, status, alerta_enviado "
                f"FROM {gold.TABLE_NAME} ORDER BY message_id"
            ).fetchall()

        assert len(rows) == 3
        statuses = {r[0]: r[1] for r in rows}
        assert statuses["<rej@grab.com>"] == "rejected"
        assert statuses["<adv@stripe.com>"] == "advanced"
        assert statuses["<unk@unknown.io>"] == "unknown"
        # Two rows marked as alerted (the non-unknown ones).
        assert sum(1 for r in rows if r[2]) == 2

        # Fallback log has 2 entries with the expected shape.
        log_lines = s.notify_fallback_log.read_text(
            encoding="utf-8"
        ).strip().split("\n")
        assert len(log_lines) == 2
        for line in log_lines:
            entry = json.loads(line)
            assert "🚨" in entry["payload"]
            assert "Rejeitado" in entry["payload"] or "Avanço" in entry["payload"]

    def test_idempotency(self, tmp_path, empty_settings):
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "gmail_address": "test@example.com",
               "gmail_app_password": "fake-app-pw",
               "duckdb_path": tmp_path / "autotrack.duckdb",
               "handoff_path": tmp_path / "_handoff.parquet",
               "notify_fallback_log": tmp_path / "notify_log.jsonl"}
        )

        with patch.object(
            bronze.imaplib, "IMAP4_SSL", side_effect=lambda *a, **kw: FakeIMAP()
        ):
            pipeline.run(settings=s)
            # Re-run: bronze returns 0 (fake IMAP returns same UIDs
            # but our search filter requires UNSEEN — since we never
            # actually marked anything seen in the fake, the second
            # run still returns the same set. The point of the test
            # is that gold's idempotency holds regardless of how
            # much silver hands it.)
            summary2 = pipeline.run(settings=s)

        # Second pass: gold sees the same message_ids and reports
        # them as updated. Notify sees alerta_enviado=TRUE and
        # returns 0. Either of the two acceptable shapes:
        #   {"inserted": 0, "updated": 0, ...} if the fake's UNSEEN
        #     filter strips them out, or
        #   {"inserted": 0, "updated": 3, ...} if they came through.
        assert summary2["gold"]["inserted"] == 0

        # The fallback log was not touched on the second run
        # (all the non-unknown rows are already alerta_enviado=TRUE).
        log_lines = s.notify_fallback_log.read_text(
            encoding="utf-8"
        ).strip().split("\n")
        assert len(log_lines) == 2  # still 2 from the first run

        # DuckDB still has exactly 3 rows.
        with duckdb.connect(str(s.duckdb_path)) as con:
            count = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME}"
            ).fetchone()[0]
        assert count == 3
