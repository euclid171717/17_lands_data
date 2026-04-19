#!/usr/bin/env python3
"""
Print CSV column headers for each draft/game/replay .csv.gz under data/raw/datasets/.
Useful when adding a new set or updating SCHEMA_EVOLUTION_PLAN.

Usage:
  python -m scripts.ingest.header_inventory
  python -m scripts.ingest.header_inventory --limit 3
"""

from __future__ import annotations

import argparse
import csv
import gzip
from pathlib import Path


def header_row(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def main() -> int:
    root = Path(__file__).resolve().parent.parent.parent
    raw = root / "data" / "raw" / "datasets"
    parser = argparse.ArgumentParser(description="List CSV headers for ingested gz files")
    parser.add_argument("--limit", type=int, default=0, help="Max files to print (0=all)")
    args = parser.parse_args()

    if not raw.exists():
        print(f"No directory: {raw}")
        return 1

    paths = sorted(raw.rglob("*.csv.gz"))
    if args.limit:
        paths = paths[: args.limit]

    if not paths:
        print("No .csv.gz files found under data/raw/datasets/")
        return 0

    for p in paths:
        try:
            h = header_row(p)
            print(f"{p.relative_to(root)}  cols={len(h)}")
            print(f"  first: {h[:12]}")
        except OSError as e:
            print(f"{p}: {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
