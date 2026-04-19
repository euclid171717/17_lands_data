#!/usr/bin/env python3
"""
17lands ingest CLI.

Usage:
  python -m scripts.ingest.cli --helpers
  python -m scripts.ingest.cli --helpers --set MKM
  python -m scripts.ingest.cli --file <url_or_path>
  python -m scripts.ingest.cli --set MKM
  python -m scripts.ingest.cli --set MKM --format premier_draft
  python -m scripts.ingest.cli --all
"""

import argparse
import logging
from pathlib import Path

from . import helpers, card_lists, datasets
from .config import get_paths, get_project_root, load_config

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
    parser.add_argument(
        "--helpers",
        action="store_true",
        help="Download and load helper files + card lists",
    )
    parser.add_argument(
        "--set",
        dest="expansion",
        metavar="CODE",
        default=None,
        help="With --helpers: target DB when use_per_set_database is true. "
        "Otherwise: ingest this expansion's datasets (same as primary)",
    )
    parser.add_argument("--file", metavar="URL_OR_PATH", help="Ingest a single dataset file")
    parser.add_argument("--all", action="store_true", help="Ingest all expansions from config")

    parser.add_argument(
        "--format",
        choices=["premier_draft", "trad_draft", "trad_sealed", "sealed"],
        help="Limit to one format (with --set)",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Re-download datasets even when ETag/Last-Modified unchanged (slower; full network GETs).",
    )

    args = parser.parse_args()
    config_path = args.config if args.config and args.config.exists() else None

    modes = [
        args.helpers,
        args.file is not None,
        args.expansion is not None and not args.helpers,
        args.all,
    ]
    if sum(bool(x) for x in modes) != 1:
        parser.error(
            "Specify exactly one primary action: --helpers, --file PATH, --set CODE, or --all "
            "(use --helpers --set CODE to target a per-set database)"
        )

    if args.helpers:
        hs = helpers.ingest_helpers(
            config_path, expansion_code=args.expansion, force_download=args.fresh
        )
        try:
            cs = card_lists.ingest_card_lists(
                config_path, expansion_code=args.expansion, force_download=args.fresh
            )
        except Exception as e:
            if "card_lists" not in str(e).lower():
                logger.warning("Card lists ingest skipped or failed: %s", e)
        else:
            logger.info(
                "Summary (--helpers): %s | %s",
                hs.summary_line("helpers"),
                cs.summary_line("card_lists"),
            )
        if config_path:
            cfg = load_config(config_path)
            if cfg.get("use_per_set_database"):
                logger.info(
                    "Database: %s (set DUCKDB_PATH for dbt to match)",
                    get_paths(cfg, args.expansion)["db"],
                )
    elif args.file:
        datasets.ingest_single_file(args.file, config_path)
    elif args.all:
        datasets.ingest_all(config_path, force_download=args.fresh)
    elif args.expansion:
        datasets.ingest_set(
            args.expansion, args.format, config_path, force_download=args.fresh
        )
        if config_path:
            cfg = load_config(config_path)
            if cfg.get("use_per_set_database"):
                logger.info(
                    "Database: %s (set DUCKDB_PATH for dbt to match)",
                    get_paths(cfg, args.expansion)["db"],
                )


if __name__ == "__main__":
    main()
