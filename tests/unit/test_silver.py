"""Unit tests for the silver layer."""

from datetime import datetime, timezone

import pandas as pd
import pytest

from autotrack import silver


# ─────────────────────────────────────────
# classify_status
# ─────────────────────────────────────────

class TestClassifyStatus:
    def test_rejection_keyword(self):
        assert silver.classify_status(
            "Update on your application",
            "Unfortunately, we will not be moving forward.",
        ) == "rejected"

    def test_acceptance_keyword(self):
        assert silver.classify_status(
            "Interview invitation",
            "Congratulations! We are pleased to inform you of next steps.",
        ) == "advanced"

    def test_unknown_when_no_keywords(self):
        assert silver.classify_status("Hello", "Just saying hi.") == "unknown"

    def test_rejection_takes_priority_over_acceptance(self):
        # A sentence with both signals must still classify as rejected.
        assert silver.classify_status(
            "Update",
            "We regret to inform you. Please consider us for future opportunities.",
        ) == "rejected"

    def test_is_case_insensitive(self):
        assert silver.classify_status(
            "UPDATE", "UNFORTUNATELY WE CANNOT PROCEED"
        ) == "rejected"

    def test_empty_input_is_unknown(self):
        assert silver.classify_status("", "") == "unknown"

    def test_none_input_is_unknown(self):
        # The function should not crash on None.
        assert silver.classify_status(None, None) == "unknown"  # type: ignore[arg-type]


# ─────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────

class TestNormalizeText:
    def test_lowercases(self):
        assert silver.normalize_text("HELLO") == "hello"

    def test_collapses_whitespace(self):
        assert silver.normalize_text("a   b\n\nc\td") == "a b c d"

    def test_strips(self):
        assert silver.normalize_text("  hi  ") == "hi"

    def test_empty(self):
        assert silver.normalize_text("") == ""


# ─────────────────────────────────────────
# Sender domain
# ─────────────────────────────────────────

class TestExtractSenderDomain:
    def test_plain_email(self):
        assert silver.extract_sender_domain("noreply@grab.com") == "grab.com"

    def test_named_email(self):
        assert (
            silver.extract_sender_domain("Recruiting <noreply@grab.com>")
            == "grab.com"
        )

    def test_subdomain(self):
        assert (
            silver.extract_sender_domain("noreply@careers.stripe.com")
            == "careers.stripe.com"
        )

    def test_empty(self):
        assert silver.extract_sender_domain("") == ""

    def test_no_at_sign(self):
        assert silver.extract_sender_domain("noreply") == ""


# ─────────────────────────────────────────
# Domain → company
# ─────────────────────────────────────────

class TestDomainToCompany:
    def test_simple(self):
        assert silver.domain_to_company("grab.com") == "Grab"

    def test_with_hyphen(self):
        assert silver.domain_to_company("credit-karma.com") == "Credit Karma"

    def test_with_underscore(self):
        assert silver.domain_to_company("rolls_royce.com") == "Rolls Royce"

    def test_multi_part_tld(self):
        assert silver.domain_to_company("stripe.co.uk") == "Stripe"

    def test_empty(self):
        assert silver.domain_to_company("") == ""


# ─────────────────────────────────────────
# Subject regex
# ─────────────────────────────────────────

class TestExtractCompanyAndPosition:
    def test_application_for_at(self):
        pos, comp = silver.extract_company_and_position(
            "Application for Software Engineer Intern at Grab",
            "noreply@grab.com",
        )
        assert pos == "Software Engineer Intern"
        assert comp == "Grab"

    def test_dash_separator(self):
        pos, comp = silver.extract_company_and_position(
            "Application for Backend Intern - Stripe",
            "noreply@stripe.com",
        )
        assert pos == "Backend Intern"
        assert comp == "Stripe"

    def test_at_intern_form(self):
        # "Backend Intern at Microsoft" — bug in v1 returned
        # "Backend" instead of "Backend Intern" because the regex
        # used a non-capturing alternation that ate the word.
        pos, comp = silver.extract_company_and_position(
            "Backend Intern at Microsoft",
            "noreply@microsoft.com",
        )
        assert pos == "Backend Intern"
        assert comp == "Microsoft"

    def test_pipe_with_at(self):
        pos, comp = silver.extract_company_and_position(
            "Data Intern | Robinhood at Robinhood",
            "noreply@robinhood.com",
        )
        assert pos == "Data Intern"
        assert comp == "Robinhood"

    def test_application_to_form(self):
        pos, comp = silver.extract_company_and_position(
            "Your application to Grab: Software Engineer Intern",
            "noreply@grab.com",
        )
        # "Grab" is the company in this regex, not the position.
        assert comp == "Grab"
        assert "Software Engineer Intern" in pos

    def test_fallback_to_sender_domain(self):
        # No subject pattern matches: fall back to (subject, sender domain).
        pos, comp = silver.extract_company_and_position(
            "Random subject line",
            "noreply@grab.com",
        )
        assert pos == "Random subject line"
        assert comp == "Grab"


# ─────────────────────────────────────────
# Date parsing
# ─────────────────────────────────────────

class TestParseDateReceived:
    def test_standard_rfc2822(self):
        d = silver.parse_date_received("Tue, 14 Jul 2026 09:42:11 +0000")
        assert d == datetime(2026, 7, 14, 9, 42, 11, tzinfo=timezone.utc)

    def test_empty(self):
        assert silver.parse_date_received("") is None

    def test_garbage(self):
        assert silver.parse_date_received("not a date") is None


# ─────────────────────────────────────────
# run_silver (end-to-end transform)
# ─────────────────────────────────────────

class TestRunSilver:
    def test_empty_input_returns_empty_df_with_schema(self):
        df = silver.run_silver([])
        assert df.empty
        for col in silver.SILVER_COLUMNS:
            assert col in df.columns

    def test_three_records_produce_three_rows(self, sample_raw_records):
        df = silver.run_silver(sample_raw_records)
        assert len(df) == 3

    def test_drops_rows_without_message_id(self):
        records = [
            {
                "email_id": "1",
                "message_id": "",
                "sender": "x@y.com",
                "subject": "Hi",
                "date": "",
                "body": "Body",
                "scraped_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "email_id": "2",
                "message_id": "<valid@x.com>",
                "sender": "x@y.com",
                "subject": "Hi",
                "date": "",
                "body": "Body",
                "scraped_at": "2026-01-01T00:00:00+00:00",
            },
        ]
        df = silver.run_silver(records)
        assert len(df) == 1
        assert df.iloc[0]["message_id"] == "<valid@x.com>"

    def test_status_classification_per_record(self, sample_raw_records):
        df = silver.run_silver(sample_raw_records)
        statuses = dict(zip(df["email_uid"], df["status"]))
        assert statuses["101"] == "rejected"
        assert statuses["102"] == "advanced"
        assert statuses["103"] == "unknown"

    def test_alerta_enviado_defaults_to_false(self, sample_raw_records):
        df = silver.run_silver(sample_raw_records)
        # `bool(df["alerta_enviado"].all())` because numpy.bool_ is
        # not the same object as Python's False.
        assert not df["alerta_enviado"].any()

    def test_sender_domain_extracted(self, sample_raw_records):
        df = silver.run_silver(sample_raw_records)
        assert "grab.com" in df["sender_domain"].values
        assert "stripe.com" in df["sender_domain"].values

    def test_subject_preserved_not_lowercased(self, sample_raw_records):
        """v1 bug: subject was stored lowercased. v2 keeps the
        original casing for display in any UI."""
        df = silver.run_silver(sample_raw_records)
        grab_row = df[df["email_uid"] == "101"].iloc[0]
        assert grab_row["subject"] == "Application for Software Engineer Intern - Grab"
