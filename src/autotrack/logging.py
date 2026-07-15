"""
Centralized logging configuration.

Each module should do::

    from autotrack.logging import get_logger
    log = get_logger(__name__)

We do not call ``logging.basicConfig`` from individual modules. That
avoids a race when the same module is imported twice (e.g., once by
the Airflow task runner, once by a test) and prevents the formatter
from being clobbered by an import order accident.

The root logger is configured lazily on first call to :func:`configure`
or :func:`get_logger`. Honors ``LOG_LEVEL`` from the environment
(default INFO).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Final

# A safe default format: timestamp, level, logger name, message.
# Logger name is critical for debugging in multi-task Airflow runs.
_DEFAULT_FORMAT: Final[str] = (
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
_DEFAULT_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

_configured: bool = False


def _resolve_level() -> int:
    """Read ``LOG_LEVEL`` env var and clamp to known levels."""
    raw = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    level = logging.getLevelName(raw)
    if not isinstance(level, int):
        return logging.INFO
    return level


def configure(level: int | None = None) -> None:
    """Configure the root logger exactly once.

    Safe to call from multiple entry points; subsequent calls are no-ops
    so we never trample the format set by an earlier caller.
    """
    global _configured
    if _configured:
        return
    logging.basicConfig(
        level=level if level is not None else _resolve_level(),
        format=_DEFAULT_FORMAT,
        datefmt=_DEFAULT_DATEFMT,
        stream=sys.stderr,
    )
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger, initializing the root if needed."""
    configure()
    return logging.getLogger(name)


def reset() -> None:
    """Reset configuration. For tests only."""
    global _configured
    _configured = False
