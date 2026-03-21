"""Ingest card lists (MTGA ID -> card name mappings) from 17lands."""

import logging
from pathlib import Path

import duckdb
import requests

from .config import get_paths, load_config, ensure_paths
from .datasets import load_csv_into_duckdb

logger = logging.getLogger(__name__)


def download_file(url: str, dest: Path) -> None:
    """Stream download URL to dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


def ingest_card_lists(config_path: Path | None = None) -> None:
    """Download card list files to data/raw/card_lists/ and load into raw_card_data schema."""
    config = load_config(config_path)
    paths = get_paths(config)
    ensure_paths(paths)

    cl = config.get("card_lists", {})
    urls = cl.get("urls", [])
    if not urls:
        logger.warning("No card_lists.urls defined in config")
        return

    conn = duckdb.connect(str(paths["db"]))
    try:
        for url in urls:
            if not url:
                continue
            filename = Path(url).name
            dest = paths["raw_card_lists"] / filename
            try:
                download_file(url, dest)
            except Exception as e:
                logger.error("Failed to download %s: %s", url, e)
                raise

            if dest.suffix.lower() == ".csv":
                table_name = dest.stem.lower().replace("-", "_")
                load_csv_into_duckdb(dest, conn, table_name, "raw_card_data")
    finally:
        conn.close()
