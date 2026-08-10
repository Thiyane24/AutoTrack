"""Unit tests for the config and logging modules."""

from __future__ import annotations

import logging
import os

import pytest

from autotrack import config
from autotrack.logging import configure, get_logger, reset


class TestSettings:
    def test_clean_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("GMAIL_ADDRESS", "  user@example.com  ")
        s = config.load_settings()
        assert s.gmail_address == "user@example.com"

    def test_clean_normalizes_empty_to_none(self, monkeypatch):
        monkeypatch.setenv("GMAIL_ADDRESS", "")
        s = config.load_settings()
        assert s.gmail_address is None

    def test_gmail_creds_check(self, monkeypatch):
        monkeypatch.setenv("GMAIL_ADDRESS", "a@b.com")
        monkeypatch.setenv("GMAIL_APP_PASSWORD", "")
        s = config.load_settings()
        assert s.has_gmail_creds() is False

        monkeypatch.setenv("GMAIL_APP_PASSWORD", "pw")
        s = config.load_settings()
        assert s.has_gmail_creds() is True

    def test_meta_creds_check_rejects_placeholder(self, monkeypatch):
        # The Meta placeholder check no longer exists — the new
        # `has_notify_creds` requires only Gmail sender creds.
        monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
        monkeypatch.delenv("GMAIL_APP_PASSWORD", raising=False)
        s = config.load_settings()
        assert s.has_notify_creds() is False

    def test_meta_creds_check_accepts_real_token(self, monkeypatch):
        monkeypatch.setenv("GMAIL_ADDRESS", "user@example.com")
        monkeypatch.setenv("GMAIL_APP_PASSWORD", "app-password")
        s = config.load_settings()
        assert s.has_notify_creds() is True
        # Recipient defaults to the sender's own address.
        assert s.resolved_recipient() == "user@example.com"


class TestLogging:
    def test_get_logger_returns_named_logger(self):
        reset()
        log = get_logger("autotrack.test")
        assert log.name == "autotrack.test"
        assert isinstance(log, logging.Logger)

    def test_configure_is_idempotent(self):
        reset()
        configure()
        first = logging.getLogger().level
        configure(level=99)  # should be ignored
        assert logging.getLogger().level == first
