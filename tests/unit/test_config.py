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
        monkeypatch.setenv("META_ACCESS_TOKEN", "seu_token_aqui")
        monkeypatch.setenv("PHONE_NUMBER_ID", "12345")
        monkeypatch.setenv("DESTINATION_PHONE", "+15555550100")
        s = config.load_settings()
        assert s.has_meta_creds() is False

    def test_meta_creds_check_accepts_real_token(self, monkeypatch):
        monkeypatch.setenv("META_ACCESS_TOKEN", "EAAreal")
        monkeypatch.setenv("PHONE_NUMBER_ID", "12345")
        monkeypatch.setenv("DESTINATION_PHONE", "+15555550100")
        s = config.load_settings()
        assert s.has_meta_creds() is True


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
