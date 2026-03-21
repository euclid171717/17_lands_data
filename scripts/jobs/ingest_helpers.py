#!/usr/bin/env python3
"""Job: Ingest helper files and card lists."""

import sys
from pathlib import Path

# Add project root to path
root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root))

from scripts.ingest import helpers, card_lists


def main() -> int:
    config_path = root / "config" / "datasets.yaml"
    if not config_path.exists():
        print("ERROR: config/datasets.yaml not found. Copy from datasets.yaml.example")
        return 1
    try:
        helpers.ingest_helpers(config_path)
        card_lists.ingest_card_lists(config_path)
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
