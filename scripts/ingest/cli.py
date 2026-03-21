#!/usr/bin/env python3
"""
17lands ingest CLI.

Usage:
  python -m scripts.ingest.cli --helpers
  python -m scripts.ingest.cli --file <url_or_path>
  python -m scripts.ingest.cli --set MKM
  python -m scripts.ingest.cli --set MKM --format premier_draft
  python -m scripts.ingest.cli --all
"""

import argparse
import logging
from pathlib import Path

from . import helpers, card_lists, datasets
from .config import get_project_root

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    root = get_project_root()
    default_config = root / "config" / "datasets.yaml"

    parser = argparse.ArgumentParser(description="17lands data ingest")
    parser.add_argument(
        "--config",
        type=Path,
        default=default_config,
        help="Path to datasets.yaml",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--helpers", action="store_true", help="Ingest helper files")
    group.add_argument("--file", metavar="URL_OR_PATH", help="Ingest single file")
    group.add_argument("--set", dest="expansion", metavar="CODE", help="Ingest expansion (e.g. MKM)")
    group.add_argument("--all", action="store_true", help="Ingest all expansions")

    parser.add_argument(
        "--format",
        choices=["premier_draft", "quick_draft", "traditional_draft"],
        help="Limit to one format (with --set)",
    )

    args = parser.parse_args()
    config_path = args.config if args.config and args.config.exists() else None

    if args.helpers:
        helpers.ingest_helpers(config_path)
        try:
            card_lists.ingest_card_lists(config_path)
        except Exception as e:
            if "card_lists" not in str(e).lower():
                logger.warning("Card lists ingest skipped or failed: %s", e)
    elif args.file:
        datasets.ingest_single_file(args.file, config_path)
    elif args.expansion:
        datasets.ingest_set(args.expansion, args.format, config_path)
    elif args.all:
        datasets.ingest_all(config_path)


if __name__ == "__main__":
    main()
