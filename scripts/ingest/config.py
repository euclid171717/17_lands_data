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


def expansion_code_for_default_path(cfg: dict[str, Any]) -> str | None:
    """Pick an expansion code when a tool needs a DB path but the CLI did not pass ``--set``.

    Order: explicit ``default_expansion`` in config; else the **first** ``expansions[]``
    entry with a ``code``. Convention: **prepend new sets at the top** so they become the
    default; older sets follow (release order is fine); put cubes / special releases at the end.
    """
    if cfg.get("default_expansion"):
        return str(cfg["default_expansion"])
    exps = cfg.get("expansions") or []
    if not exps:
        return None
    first = exps[0]
    if isinstance(first, dict) and first.get("code"):
        return str(first["code"])
    return None


def get_paths(
    config: dict[str, Any] | None = None,
    expansion_code: str | None = None,
) -> dict[str, Path]:
    """Return standard paths for raw data, db, etc.

    When ``use_per_set_database`` is true in config, ``db`` is ``data/db/<CODE>.duckdb``
    (``expansion_code``, else ``default_expansion``, else first ``expansions[]`` code).
    Otherwise ``data/db/17lands.duckdb`` (or ``duckdb_path`` override).
    """
    root = get_project_root()
    cfg = config or load_config()
    data_dir = root / "data"
    raw = data_dir / "raw"
    db_dir = data_dir / "db"

    use_per_set = cfg.get("use_per_set_database", False)
    if use_per_set:
        code = expansion_code or expansion_code_for_default_path(cfg)
        if not code:
            raise ValueError(
                "use_per_set_database is true but no expansion code is known. "
                "Add `expansions` with at least one `{code: ...}`, set `default_expansion`, "
                "or pass e.g. `--set MKM`."
            )
        db_path = db_dir / f"{code}.duckdb"
    else:
        override = cfg.get("duckdb_path")
        if override:
            p = Path(override)
            db_path = p if p.is_absolute() else root / p
        else:
            db_path = db_dir / "17lands.duckdb"

    return {
        "root": root,
        "data": data_dir,
        "raw": raw,
        "raw_helpers": raw / "helpers",
        "raw_card_lists": raw / "card_lists",
        "raw_datasets": raw / "datasets",
        "sample": data_dir / "sample",
        "db": db_path,
        "ingest_remote_state": data_dir / ".ingest_remote_state.json",
    }


def ensure_paths(paths: dict[str, Path]) -> None:
    """Create directories if they don't exist."""
    for name, p in paths.items():
        if name.endswith("_dir") or "raw" in name or name == "db":
            if p.suffix:
                p.parent.mkdir(parents=True, exist_ok=True)
            else:
                p.mkdir(parents=True, exist_ok=True)
