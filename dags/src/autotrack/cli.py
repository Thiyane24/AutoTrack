"""
Command-line entry points.

Usage::

    python -m autotrack.cli run          # full pipeline, end-to-end
    python -m autotrack.cli bronze       # one layer at a time
    python -m autotrack.cli silver
    python -m autotrack.cli gold
    python -m autotrack.cli notify
    python -m autotrack.cli version
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from autotrack import pipeline
from autotrack.config import load_settings
from autotrack.logging import configure, get_logger
from autotrack import __version__

log = get_logger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="autotrack",
        description="Run the AutoTrack pipeline layers.",
    )
    p.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable DEBUG logging.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="Run the full pipeline end-to-end.")
    sub.add_parser("bronze", help="Run only the bronze layer.")
    sub.add_parser("silver", help="Run only the silver layer.")
    sub.add_parser("gold", help="Run only the gold layer.")
    sub.add_parser("notify", help="Run only the notify layer.")
    sub.add_parser("version", help="Print the version and exit.")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.verbose:
        configure(level=10)  # logging.DEBUG
    else:
        configure()

    settings = load_settings()

    if args.command == "version":
        print(f"autotrack {__version__}")
        return 0
    if args.command == "run":
        summary = pipeline.run(settings=settings)
        print(json.dumps(summary, default=str, indent=2))
        return 0
    if args.command == "bronze":
        n = pipeline.run_bronze(settings=settings)
        return 0 if n >= 0 else 1
    if args.command == "silver":
        pipeline.run_silver(settings=settings)
        return 0
    if args.command == "gold":
        counts = pipeline.run_gold(settings=settings)
        print(json.dumps(counts))
        return 0
    if args.command == "notify":
        counts = pipeline.run_notify(settings=settings)
        print(json.dumps(counts))
        return 0

    return 2  # argparse should prevent this


if __name__ == "__main__":
    sys.exit(main())
