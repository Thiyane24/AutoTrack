"""Unit tests for the bronze layer (IMAP plumbing)."""

from __future__ import annotations

import imaplib
from unittest.mock import patch

import pytest

from autotrack import bronze
from autotrack.config import Settings


def _gmail_settings(empty_settings) -> Settings:
    return Settings(
        **{**empty_settings.__dict__,
           "gmail_address": "test@example.com",
           "gmail_app_password": "fake-app-password"}
    )


class TestConnectToGmail:
    def test_missing_creds_raises(self, empty_settings):
        with pytest.raises(bronze.BronzeError):
            bronze.connect_to_gmail(settings=empty_settings)

    def test_connect_calls_imap_with_timeout(self, empty_settings):
        settings = _gmail_settings(empty_settings)
        with patch.object(bronze.imaplib, "IMAP4_SSL") as imap_mock:
            bronze.connect_to_gmail(settings=settings)
            imap_mock.assert_called_once_with(
                settings.gmail_imap_host,
                settings.gmail_imap_port,
                timeout=settings.imap_timeout,
            )

    def test_auth_failure_raises_typed_error(self, empty_settings):
        settings = _gmail_settings(empty_settings)
        with patch.object(
            bronze.imaplib, "IMAP4_SSL",
            side_effect=imaplib.IMAP4.error("Invalid credentials"),
        ):
            with pytest.raises(bronze.BronzeError) as excinfo:
                bronze.connect_to_gmail(settings=settings)
            # The error must NOT include the password in its message.
            assert "fake-app-password" not in str(excinfo.value)


class TestSearchEmails:
    def test_returns_unique_uids_above_watermark(self, empty_settings):
        from unittest.mock import MagicMock

        # Real imaplib returns (status, [bytes_blob]); we mock that
        # shape directly. The mock cycles through the responses so
        # every keyword gets the same UID set.
        from itertools import cycle

        mail = MagicMock()
        mail.uid.side_effect = cycle([
            ("OK", [b"10 20 30"]),
            ("OK", [b"20 40"]),
        ])
        result = bronze.search_emails(mail, last_uid=15)
        assert set(int(x) for x in result) == {20, 30, 40}

    def test_search_failure_continues_with_next_keyword(self, empty_settings):
        from unittest.mock import MagicMock

        from itertools import cycle

        mail = MagicMock()
        mail.uid.side_effect = cycle([
            ("NO", [b""]),    # first keyword fails
            ("OK", [b"99"]),  # second works
        ])
        result = bronze.search_emails(mail, last_uid=0)
        assert result == [b"99"]

    def test_skips_non_numeric_uids(self, empty_settings):
        from unittest.mock import MagicMock

        from itertools import cycle

        mail = MagicMock()
        mail.uid.side_effect = cycle([("OK", [b"1 abc 2"])])
        result = bronze.search_emails(mail, last_uid=0)
        # "1" and "2" parse as ints; "abc" is skipped with a warning.
        assert set(int(x) for x in result) == {1, 2}

    def test_imap_error_on_one_keyword_keeps_going(self, empty_settings):
        """A network blip on one keyword should not kill the run."""
        from unittest.mock import MagicMock

        import imaplib

        from itertools import cycle

        mail = MagicMock()
        mail.uid.side_effect = cycle([
            imaplib.IMAP4.error("temporary blip"),
            ("OK", [b"42"]),
        ])
        result = bronze.search_emails(mail, last_uid=0)
        assert result == [b"42"]


class TestRunBronze:
    def test_full_run_returns_empty_list(self, empty_settings):
        # A fake IMAP that returns no UIDs at all.
        class EmptyIMAP:
            def login(self, *a, **kw): return ("OK", [b""])
            def select(self, *a, **kw): return ("OK", [b""])
            def uid(self, *a, **kw): return ("OK", [b""])
            def logout(self): return ("BYE", [b""])

        with patch.object(
            bronze.imaplib, "IMAP4_SSL", side_effect=lambda *a, **kw: EmptyIMAP()
        ):
            records = bronze.run_bronze(settings=_gmail_settings(empty_settings))
        assert records == []
