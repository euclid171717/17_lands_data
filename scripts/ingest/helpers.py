"""Ingest helper files (formats, ranks, column docs, etc.) from 17lands."""

import logging
from pathlib import Path

import duckdb
import requests

from .config import get_paths, load_config, ensure_paths
from .datasets import load_csv_into_duckdb

logger = logging.getLogger(__name__)


def download_file(url: str, dest: Path) -> None:
    """Stream download URL to dest. Creates parent dirs."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


def ingest_helpers(config_path: Path | None = None) -> None:
    """Download helper files to data/raw/helpers/ and load CSVs into raw_helpers schema."""
    config = load_config(config_path)
    paths = get_paths(config)
    ensure_paths(paths)

    helpers = config.get("helpers", [])
    if not helpers:
        logger.warning("No helpers defined in config")
        return

    conn = duckdb.connect(str(paths["db"]))
    try:
        for h in helpers:
            url = h.get("url")
            if not url:
                logger.warning("Helper %s has no url, skipping", h.get("name", "?"))
                continue
            filename = Path(url).name
            dest = paths["raw_helpers"] / filename
            try:
                download_file(url, dest)
            except Exception as e:
                logger.error("Failed to download %s: %s", url, e)
                raise

            # Load CSV helpers into raw_helpers; skip non-CSV (e.g. .py)
            if dest.suffix.lower() == ".csv":
                table_cfg = h.get("table", "")
                if table_cfg and "." in table_cfg:
                    schema, table_name = table_cfg.split(".", 1)
                    load_csv_into_duckdb(dest, conn, table_name, schema)
                else:
                    table_name = dest.stem.lower().replace("-", "_")
                    load_csv_into_duckdb(dest, conn, table_name, "raw_helpers")
    finally:
        conn.close()
