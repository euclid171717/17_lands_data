#!/usr/bin/env python3
"""Job: Ingest a single expansion. Usage: ingest_set.py [--set CODE] [--format FMT]."""

import argparse
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root))

from scripts.ingest import datasets


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--set", "--expansion", dest="expansion", required=True)
    parser.add_argument("--format", choices=["premier_draft", "quick_draft", "traditional_draft"])
    args = parser.parse_args()

    config_path = root / "config" / "datasets.yaml"
    if not config_path.exists():
        print("ERROR: config/datasets.yaml not found. Copy from datasets.yaml.example")
        return 1
    try:
        datasets.ingest_set(args.expansion, args.format, config_path)
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
