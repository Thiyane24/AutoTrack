"""Unit tests for the notify layer (payload, fallback, retry)."""

import json
from unittest.mock import patch

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
        assert "Rejeitado" in msg
        assert msg.startswith("🚨")

    def test_advanced_label(self):
        msg = notify.build_payload("Stripe", "Backend Intern", "advanced")
        assert "Stripe" in msg
        assert "Backend Intern" in msg
        assert "Avanço" in msg

    def test_unknown_status_falls_back_to_title(self):
        msg = notify.build_payload("X", "Y", "mystery")
        assert "Mystery" in msg

    def test_empty_status_is_blank_label(self):
        # Defensive: a NULL/empty status should not raise.
        msg = notify.build_payload("X", "Y", "")
        assert "🚨" in msg


# ─────────────────────────────────────────
# Request shape
# ─────────────────────────────────────────

class TestBuildMetaRequest:
    def test_request_body_shape(self):
        body = notify.build_meta_request(
            "hello", phone_number_id="12345", destination_phone="+15555550100"
        )
        assert body["messaging_product"] == "whatsapp"
        assert body["to"] == "+15555550100"
        assert body["type"] == "text"
        assert body["text"]["body"] == "hello"

    def test_request_url_uses_phone_number_id(self):
        url = notify.meta_url(api_version="v20.0", phone_number_id="12345")
        assert "12345" in url
        assert "v20.0" in url
        assert url.startswith("https://graph.facebook.com/")


# ─────────────────────────────────────────
# Placeholder detection
# ─────────────────────────────────────────

class TestCredsArePlaceholder:
    def test_empty_string(self):
        assert notify.creds_are_placeholder("") is True

    def test_default_placeholder(self):
        assert notify.creds_are_placeholder("seu_token_aqui") is True

    def test_real_token(self):
        assert notify.creds_are_placeholder("EAArealMetaToken") is False

    def test_none(self):
        assert notify.creds_are_placeholder(None) is True


# ─────────────────────────────────────────
# Phone validation
# ─────────────────────────────────────────

class TestIsValidPhone:
    @pytest.mark.parametrize("phone", [
        "+15555550100",
        "15555550100",
        "+442071234567",
        "+5511987654321",
    ])
    def test_valid(self, phone):
        assert notify.is_valid_phone(phone) is True

    @pytest.mark.parametrize("phone", [
        "",
        None,
        "not-a-phone",
        "+0123456789",     # leading 0 after +
        "123",             # too short
    ])
    def test_invalid(self, phone):
        assert notify.is_valid_phone(phone) is False


# ─────────────────────────────────────────
# Settings.has_meta_creds
# ─────────────────────────────────────────

class TestSettingsMetaCreds:
    def test_placeholder_is_not_real_creds(self, empty_settings):
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "meta_access_token": "seu_token_aqui",
               "meta_phone_number_id": "12345",
               "meta_destination_phone": "+15555550100"}
        )
        assert s.has_meta_creds() is False

    def test_real_token_with_ids_and_phone(self, empty_settings):
        from autotrack.config import Settings

        s = Settings(
            **{**empty_settings.__dict__,
               "meta_access_token": "EAAreal",
               "meta_phone_number_id": "12345",
               "meta_destination_phone": "+15555550100"}
        )
        assert s.has_meta_creds() is True


# ─────────────────────────────────────────
# run_notify: fallback path
# ─────────────────────────────────────────

class TestNotifyFallback:
    def test_fallback_when_creds_missing(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        # Seed DuckDB via silver+gold.
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

        # Fallback log file should have 2 lines.
        lines = tmp_fallback_log.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            entry = json.loads(line)
            assert "message_id" in entry
            assert "payload" in entry
            assert "🚨" in entry["payload"]

        # Both rows should now be marked as alerted.
        with duckdb.connect(str(tmp_duckdb_path)) as con:
            alerted = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME} "
                f"WHERE alerta_enviado = TRUE"
            ).fetchone()[0]
        assert alerted == 2

    def test_unknown_rows_are_not_notified(
        self, tmp_duckdb_path, tmp_fallback_log, empty_settings,
    ):
        # Only an "unknown" row in the DB.
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
# run_notify: real Meta path
# ─────────────────────────────────────────

def _meta_settings(empty_settings, token="real-token"):
    """A Settings with valid Meta creds for the success-path tests."""
    from autotrack.config import Settings

    return Settings(
        **{**empty_settings.__dict__,
           "meta_access_token": token,
           "meta_phone_number_id": "1234567890",
           "meta_destination_phone": "+15555550100"}
    )


class TestNotifyRealMeta:
    def test_success_marks_alerted(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        fake_response = type("Resp", (), {
            "status_code": 200,
            "text": '{"messages":[{"id":"abc"}]}',
        })()

        settings = _meta_settings(empty_settings)
        with patch.object(notify.requests, "post", return_value=fake_response):
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        # Two non-unknown rows: both sent successfully.
        assert counts["notified"] == 2
        assert counts["failed"] == 0
        assert counts["fallback"] == 0

        with duckdb.connect(str(tmp_duckdb_path)) as con:
            alerted = con.execute(
                f"SELECT COUNT(*) FROM {gold.TABLE_NAME} "
                f"WHERE alerta_enviado = TRUE"
            ).fetchone()[0]
        assert alerted == 2

    def test_4xx_is_not_retried(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        fake_response = type("Resp", (), {
            "status_code": 401,
            "text": "Unauthorized",
        })()

        settings = _meta_settings(empty_settings)
        with patch.object(notify.requests, "post", return_value=fake_response), \
             patch.object(notify.time, "sleep") as sleep_mock:  # no backoff
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        # 4xx: no retry, both rows fail, sleep was never called.
        assert counts["notified"] == 0
        assert counts["failed"] == 2
        sleep_mock.assert_not_called()

    def test_5xx_retries_then_fails(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        fake_response = type("Resp", (), {
            "status_code": 503,
            "text": "Service Unavailable",
        })()

        settings = _meta_settings(empty_settings)
        with patch.object(notify.requests, "post", return_value=fake_response), \
             patch.object(notify.time, "sleep") as sleep_mock:
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        # 2 rows, each retried 3x = 6 total post calls.
        # 2 rows end up in 'failed', 0 'notified'.
        assert counts["notified"] == 0
        assert counts["failed"] == 2
        # Sleep called after every failed attempt (3 attempts, 2 rows).
        # 3 sleeps per row * 2 rows = 6 total.
        assert sleep_mock.call_count == 6

    def test_retry_eventually_succeeds(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        fail_503 = type("Resp", (), {
            "status_code": 503,
            "text": "Service Unavailable",
        })()
        ok_200 = type("Resp", (), {
            "status_code": 200,
            "text": '{"messages":[{"id":"abc"}]}',
        })()

        # First call per row fails, second succeeds.
        responses = [fail_503, ok_200] * 2  # 4 calls total

        settings = _meta_settings(empty_settings)
        with patch.object(notify.requests, "post", side_effect=responses), \
             patch.object(notify.time, "sleep"):
            counts = notify.run_notify(
                db_path=tmp_duckdb_path,
                fallback_log_path=tmp_fallback_log,
                settings=settings,
            )

        assert counts["notified"] == 2
        assert counts["failed"] == 0

    def test_invalid_phone_fails_without_retry(
        self, sample_raw_records, tmp_duckdb_path, tmp_fallback_log,
        empty_settings,
    ):
        from autotrack.config import Settings

        df = silver.run_silver(sample_raw_records)
        gold.run_gold(df, db_path=tmp_duckdb_path)

        bad_phone_settings = Settings(
            **{**_meta_settings(empty_settings).__dict__,
               "meta_destination_phone": "not-a-phone"}
        )
        counts = notify.run_notify(
            db_path=tmp_duckdb_path,
            fallback_log_path=tmp_fallback_log,
            settings=bad_phone_settings,
        )
        assert counts["notified"] == 0
        assert counts["failed"] == 2
