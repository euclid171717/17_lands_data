"""Ingest card lists (MTGA ID -> card name mappings) from 17lands."""

import logging
import shutil
from pathlib import Path

import duckdb

from .config import get_paths, load_config, ensure_paths
from .datasets import load_csv_into_duckdb
from .ingest_stats import IngestStats
from .remote_freshness import download_if_newer, simple_download

logger = logging.getLogger(__name__)


def ingest_card_lists(
    config_path: Path | None = None,
    expansion_code: str | None = None,
    *,
    force_download: bool = False,
) -> IngestStats:
    """Download card list files to data/raw/card_lists/ and load into raw_card_data schema."""
    config = load_config(config_path)
    paths = get_paths(config, expansion_code)
    ensure_paths(paths)

    cl = config.get("card_lists", {})
    urls = cl.get("urls", [])
    stats = IngestStats()
    if not urls:
        logger.warning("No card_lists.urls defined in config")
        return stats

    always_refresh = config.get("always_refresh_helpers", False)
    skip_if_unchanged = (
        config.get("skip_download_if_unchanged", False) and not always_refresh and not force_download
    )
    state_path = paths["ingest_remote_state"]

    conn = duckdb.connect(str(paths["db"]))
    try:
        for url in urls:
            if not url:
                continue
            filename = Path(url).name
            dest = paths["raw_card_lists"] / filename
            try:
                if skip_if_unchanged:
                    result = download_if_newer(url, dest, state_path, skip_if_unchanged=True)
                    stats.add_download_result(result)
                else:
                    simple_download(url, dest)
                    stats.downloaded += 1
            except Exception as e:
                sample_file = paths.get("sample") and (paths["sample"] / filename)
                if sample_file and sample_file.exists():
                    logger.warning(
                        "Download failed for %s (%s), using sample file %s",
                        url, e, sample_file,
                    )
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(sample_file, dest)
                else:
                    logger.error("Failed to download %s: %s", url, e)
                    raise

            if dest.suffix.lower() == ".csv":
                table_name = dest.stem.lower().replace("-", "_")
                load_csv_into_duckdb(dest, conn, table_name, "raw_card_data")
    finally:
        conn.close()
    return stats
