"""Ingest helper files (formats, ranks, column docs, etc.) from 17lands."""

import logging
from pathlib import Path

import duckdb

from .config import get_paths, load_config, ensure_paths
from .datasets import load_csv_into_duckdb
from .ingest_stats import IngestStats
from .remote_freshness import download_if_newer, simple_download

logger = logging.getLogger(__name__)


def ingest_helpers(
    config_path: Path | None = None,
    expansion_code: str | None = None,
    *,
    force_download: bool = False,
) -> IngestStats:
    """Download helper files to data/raw/helpers/ and load CSVs into raw_helpers schema."""
    config = load_config(config_path)
    paths = get_paths(config, expansion_code)
    ensure_paths(paths)

    helpers = config.get("helpers", [])
    stats = IngestStats()
    if not helpers:
        logger.warning("No helpers defined in config")
        return stats

    # When true: always GET helpers (ignore ETag skip). Card lists use same flag via card_lists.
    always_refresh = config.get("always_refresh_helpers", False)
    skip_if_unchanged = (
        config.get("skip_download_if_unchanged", False) and not always_refresh and not force_download
    )
    state_path = paths["ingest_remote_state"]

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
                if skip_if_unchanged:
                    result = download_if_newer(url, dest, state_path, skip_if_unchanged=True)
                    stats.add_download_result(result)
                else:
                    simple_download(url, dest)
                    stats.downloaded += 1
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
    return stats
