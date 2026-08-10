"""Unit tests for the notify layer (payload, fallback, SMTP retry)."""

import json
import smtplib
from email.message import EmailMessage
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from autotrack import gold, notify, silver


# ─────────────────────────────────────────
# Payload shape
# ─────────────────────────────────────────

class TestBuildPayload:
    def test_rejected_label(self):
        msg = notify.build_payload("Grab", "SWE Intern", "rejected")
        assert "Grab" in msg
        assert "SWE Intern" in msg
        assert "Rejected" in msg

    def test_advanced_label(self):
        msg = notify.build_payload("Stripe", "Backend Intern", "advanced")
        assert "Stripe" in msg
        assert "Backend Intern" in msg
        assert "Next step" in msg

    def test_offer_label(self):
        msg = notify.build_payload("X", "Y", "offer")
        assert "Offer" in msg

    def test_unknown_status_falls_back_to_title(self):
        msg = notify.build_payload("X", "Y", "mystery")
        assert "Mystery" in msg

    def test_empty_status_is_still_valid(self):
        # Defensive: a NULL/empty status should not raise.
        msg = notify.build_payload("X", "Y", "")
        assert "Update on your internship application" in msg


# ─────────────────────────────────────────
# Email message shape
# ─────────────────────────────────────────

class TestBuildEmailMessage:
    def test_basic_headers(self):
        msg = notify.build_email_message(
            subject="[AutoTrack] Rejected — Grab",
            body="body",
            sender="me@gmail.com",
            recipient="me@gmail.com",
            message_id="<abc@grab.com>",
        )
        assert msg["Subject"] == "[AutoTrack] Rejected — Grab"
        assert msg["From"] == "me@gmail.com"
        assert msg["To"] == "me@gmail.com"
        assert msg["X-AutoTrack-Message-ID"] == "<abc@grab.com>"
        assert "body" in msg.get_content()

    def test_subject_helper(self):
        subj = notify.subject_for("Grab", "rejected")
        assert "Grab" in subj
        assert "Rejected" in subj
        assert subj.startswith("[AutoTrack]")


# ─────────────────────────────────────────
# Email validation
# ─────────────────────────────────────────

class TestIsValidEmail:
    @pytest.mark.parametrize("addr", [
        "user@example.com",
        "first.last+tag@sub.domain.org",
        "x@y.io",
    ])
    def test_valid(self, addr):
        assert notify.is_valid_email(addr) is True

    @pytest.mark.parametrize("addr", [
        "",
        None,
        "not-an-email",
        "@nodomain.com",
        "noat.com",
        "spaces in@addr.com",
    ])
    def test_invalid(self, addr):
        assert notify.is_valid_email(addr) is False


# ─────────────────────────────────────────
# Settings has_notify_creds + recipient
# ─────────────────────────────────────────

class TestSettingsNotifyCreds:
    def test_no_creds(self, empty_settings):
        assert empty_settings.has_notify_creds() is False
        assert empty_settings.resolved_recipient() is None

    def test_with_gmail_creds(self, empty_settings):
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "gmail_address": "me@gmail.com",
               "gmail_app_password": "app-pw"}
        )
        assert s.has_notify_creds() is True
        # Recipient defaults to the sender's own address.
        assert s.resolved_recipient() == "me@gmail.com"

    def test_explicit_recipient_overrides_default(self, empty_settings):
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "gmail_address": "me@gmail.com",
               "gmail_app_password": "app-pw",
               "notify_recipient_email": "alias@gmail.com"}
        )
        assert s.resolved_recipient() == "alias@gmail.com"


# ─────────────────────────────────────────
# run_notify: fallback path (no Gmail creds)
# ─────────────────────────────────────────

class TestNotifyFallback:
    def test_fallback_when_creds_missing(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        counts = notify.run_notify(
            db_path=tmp_duckdb_path,
            fallback_log_path=tmp_fallback_log,
            settings=empty_settings,
        )

        # Two non-unknown rows: rejected + advanced.
        assert counts["notified"] == 0
        assert counts["failed"] == 0
        assert counts["fallback"] == 2

        lines = tmp_fallback_log.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            entry = json.loads(line)
            assert "message_id" in entry
            assert "payload" in entry
            assert "Update on your internship application" in entry["payload"]

        with duckdb.connect(str(tmp_duckdb_path)) as con:
            alerted = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME} "
                f"WHERE alerta_enviado = TRUE"
            ).fetchone()[0]
        assert alerted == 2

    def test_unknown_rows_are_not_notified(
        self, tmp_duckdb_path, tmp_fallback_log, empty_settings,
    ):
        records = [
            {
                "email_id": "1",
                "message_id": "<unknown@x.com>",
                "sender": "x@x.com",
                "subject": "Hi",
                "date": "",
                "body": "Just saying hi.",
                "scraped_at": "2026-01-01T00:00:00+00:00",
            }
        ]
        df = silver.run_silver(records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        counts = notify.run_notify(
            db_path=tmp_duckdb_path,
            fallback_log_path=tmp_fallback_log,
            settings=empty_settings,
        )

        assert counts["fallback"] == 0
        assert counts["notified"] == 0
        assert counts["failed"] == 0


# ─────────────────────────────────────────
# run_notify: real SMTP path
# ─────────────────────────────────────────

def _gmail_settings(empty_settings, address="me@gmail.com"):
    """A Settings with valid Gmail creds for the success-path tests."""
    from autotrack.config import Settings

    return Settings(
        **{**empty_settings.__dict__,
           "gmail_address": address,
           "gmail_app_password": "fake-app-pw"}
    )


class _FakeSMTP:
    """Minimal stand-in for ``smtplib.SMTP``.

    Tracks calls without doing real I/O. The class attribute
    ``raise_on`` controls per-call exception injection.
    """

    instances: list = []

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.started_tls = False
        self.logged_in_as: tuple = ()
        self.sent: list = []
        type(self).instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def starttls(self):
        self.started_tls = True

    def login(self, user, password):
        self.logged_in_as = (user, password)
        # By default, login succeeds. Tests can override via patch.
        if self.__class__.raise_on == "auth":
            raise smtplib.SMTPAuthenticationError(535, b"auth failed")

    def send_message(self, msg):
        if self.__class__.raise_on == "data":
            raise smtplib.SMTPDataError(451, b"try again later")
        self.sent.append(msg)


class TestNotifyRealSMTP:
    def setup_method(self):
        _FakeSMTP.instances = []
        _FakeSMTP.raise_on = None

    def test_success_marks_alerted(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        settings = _gmail_settings(empty_settings)
        with patch.object(notify.smtplib, "SMTP", _FakeSMTP):
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        assert counts["notified"] == 2
        assert counts["failed"] == 0
        assert counts["fallback"] == 0

        # Two non-unknown rows were sent. Each row gets its own
        # SMTP connection (one per message), so we expect 2 instances.
        assert len(_FakeSMTP.instances) == 2
        smtp = _FakeSMTP.instances[0]
        assert smtp.started_tls is True
        assert smtp.logged_in_as == (settings.gmail_address, "fake-app-pw")
        # Each instance sent exactly one message.
        assert sum(len(s.sent) for s in _FakeSMTP.instances) == 2

        with duckdb.connect(str(tmp_duckdb_path)) as con:
            alerted = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME} "
                f"WHERE alerta_enviado = TRUE"
            ).fetchone()[0]
        assert alerted == 2

    def test_auth_failure_is_hard_fail(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        _FakeSMTP.raise_on = "auth"
        settings = _gmail_settings(empty_settings)

        with patch.object(notify.smtplib, "SMTP", _FakeSMTP), \
             patch.object(notify.time, "sleep") as sleep_mock:
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        # Auth errors are hard fails — no retries.
        assert counts["notified"] == 0
        assert counts["failed"] == 2
        sleep_mock.assert_not_called()

    def test_transient_error_retries_then_succeeds(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        _FakeSMTP.raise_on = "data"
        settings = _gmail_settings(empty_settings)

        # First call per row raises, second call (same instance) succeeds.
        # We swap raise_on back to None after the first send_message fails.
        original_send = _FakeSMTP.send_message

        def flaky_send(self, msg):
            if _FakeSMTP.raise_on == "data":
                _FakeSMTP.raise_on = None
                raise smtplib.SMTPDataError(451, b"try again")
            return original_send(self, msg)

        _FakeSMTP.send_message = flaky_send

        try:
            with patch.object(notify.smtplib, "SMTP", _FakeSMTP), \
                 patch.object(notify.time, "sleep"):
                counts = notify.run_notify(
                    db_path=tmp_duckdb_path,
                    fallback_log_path=tmp_fallback_log,
                    settings=settings,
                )

            assert counts["notified"] == 2
            assert counts["failed"] == 0
        finally:
            _FakeSMTP.send_message = original_send

    def test_transient_error_exhausts_retries(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        _FakeSMTP.raise_on = "data"  # always fails
        settings = _gmail_settings(empty_settings)

        with patch.object(notify.smtplib, "SMTP", _FakeSMTP), \
             patch.object(notify.time, "sleep") as sleep_mock:
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        # 2 rows, each retried 3x = 6 sleep calls.
        assert counts["notified"] == 0
        assert counts["failed"] == 2
        assert sleep_mock.call_count == 6

    def test_invalid_recipient_fails_without_retry(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        from autotrack.config import Settings

        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        bad_recipient_settings = Settings(
            **{**_gmail_settings(empty_settings).__dict__,
               "notify_recipient_email": "not-an-email"}
        )
        counts = notify.run_notify(
            db_path=tmp_duckdb_path,
            fallback_log_path=tmp_fallback_log,
            settings=bad_recipient_settings,
        )
        assert counts["notified"] == 0
        assert counts["failed"] == 2
