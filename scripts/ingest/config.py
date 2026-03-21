"""Load and validate ingest configuration."""

from pathlib import Path
from typing import Any

import yaml


def get_project_root() -> Path:
    """Project root (parent of scripts/)."""
    return Path(__file__).resolve().parent.parent.parent


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load datasets.yaml. Uses config/datasets.yaml if path not given."""
    root = get_project_root()
    path = config_path or root / "config" / "datasets.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Config not found: {path}\n"
            "Copy config/datasets.yaml.example to config/datasets.yaml and fill in URLs."
        )
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_paths(config: dict[str, Any] | None = None) -> dict[str, Path]:
    """Return standard paths for raw data, db, etc."""
    root = get_project_root()
    cfg = config or load_config()
    data_dir = root / "data"
    raw = data_dir / "raw"
    return {
        "root": root,
        "data": data_dir,
        "raw": raw,
        "raw_helpers": raw / "helpers",
        "raw_card_lists": raw / "card_lists",
        "raw_datasets": raw / "datasets",
        "db": data_dir / "db" / "17lands.duckdb",
    }


def ensure_paths(paths: dict[str, Path]) -> None:
    """Create directories if they don't exist."""
    for name, p in paths.items():
        if name.endswith("_dir") or "raw" in name or name == "db":
            if p.suffix:
                p.parent.mkdir(parents=True, exist_ok=True)
            else:
                p.mkdir(parents=True, exist_ok=True)
