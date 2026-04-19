"""Ingest draft, game, and replay datasets from 17lands."""

import logging
from pathlib import Path

import duckdb
import requests

from .config import get_paths, load_config, ensure_paths
from .ingest_stats import IngestStats
from .remote_freshness import download_if_newer, simple_download

logger = logging.getLogger(__name__)

# Some S3 buckets block requests without a browser-like User-Agent
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def build_dataset_url(
    s3_base: str,
    pattern: str,
    expansion_slug: str,
    format_key: str,
    format_map: dict[str, str],
) -> str:
    """Build URL from pattern. expansion_slug is the middle segment in the filename (e.g. TMT or Cube_-_Powered)."""
    fmt = format_map.get(format_key, format_key)
    path = pattern.format(expansion=expansion_slug, format=fmt)
    return f"{s3_base.rstrip('/')}/{path}"


def download_file(url: str, dest: Path) -> None:
    """Stream download URL to dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=120, headers=REQUEST_HEADERS)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)


def load_csv_into_duckdb(
    csv_path: Path,
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    schema: str,
) -> None:
    """Load a CSV (or .csv.gz) into DuckDB. Replaces table if exists."""
    full_table = f"{schema}.{table_name}"
    conn.execute(f"DROP TABLE IF EXISTS {full_table}")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(
        f"""
        CREATE TABLE {full_table} AS
        SELECT * FROM read_csv_auto(?)
        """,
        [str(csv_path)],
    )
    count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
    logger.info("Loaded %s -> %s (%d rows)", csv_path.name, full_table, count)


def append_to_unified_table(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    target_table: str,
    expansion: str,
    event_type: str,
) -> None:
    """
    Append CSV data into unified raw table (raw.draft, raw.game, or raw.replay).
    Deletes existing rows for this expansion/event_type first for idempotency.
    Replay data uses all_varchar to avoid type errors on pipe-separated card IDs.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    full_table = f"raw.{target_table}"

    # Replay has columns with pipe-separated values (e.g. "89178|89044") that
    # break auto type detection; load as VARCHAR to preserve them.
    read_sql = (
        "read_csv_auto(?, all_varchar=true)" if target_table == "replay" else "read_csv_auto(?)"
    )

    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = ?",
        [target_table],
    ).fetchone()[0]

    if exists:
        conn.execute(
            f"DELETE FROM {full_table} WHERE expansion = ? AND event_type = ?",
            [expansion, event_type],
        )
        conn.execute(
            f"INSERT INTO {full_table} SELECT * FROM {read_sql}",
            [str(csv_path)],
        )
        count = conn.execute(
            f"SELECT COUNT(*) FROM {full_table} WHERE expansion = ? AND event_type = ?",
            [expansion, event_type],
        ).fetchone()[0]
        logger.info("Upserted %s/%s -> %s (%d rows)", expansion, event_type, full_table, count)
    else:
        conn.execute(
            f"CREATE TABLE {full_table} AS SELECT * FROM {read_sql}",
            [str(csv_path)],
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        logger.info("Created %s from %s (%d rows)", full_table, csv_path.name, count)


def _slice_has_rows(
    conn: duckdb.DuckDBPyConnection,
    target_table: str,
    expansion: str,
    event_type: str,
) -> bool:
    """True if raw.<target_table> exists and has rows for this expansion + event_type."""
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = ?",
        [target_table],
    ).fetchone()[0]
    if not exists:
        return False
    n = conn.execute(
        f"SELECT COUNT(*) FROM raw.{target_table} WHERE expansion = ? AND event_type = ?",
        [expansion, event_type],
    ).fetchone()[0]
    return n > 0


def _parse_dataset_filename(filename: str) -> tuple[str, str, str] | None:
    """
    Parse 17lands dataset filename into (data_type, expansion, event_type).
    e.g. draft_data_public.MKM.PremierDraft.csv.gz -> (draft, MKM, PremierDraft)
    Returns None if not a recognized dataset file.
    """
    stem = Path(filename).stem
    if stem.endswith(".csv"):
        stem = Path(stem).stem
    # Split on dots only — expansion may contain hyphens/underscores (e.g. Cube_-_Powered)
    parts = stem.split(".")
    # draft_data_public.MKM.PremierDraft -> [draft_data_public, MKM, PremierDraft]
    if len(parts) >= 3 and "public" in parts[0]:
        prefix = parts[0]  # draft_data_public, game_data_public, replay_data_public
        data_type = prefix.split("_")[0]
        expansion = parts[1]
        event_type = parts[2]
        if data_type in ("draft", "game", "replay"):
            return (data_type, expansion, event_type)
    return None


def ingest_single_file(
    url_or_path: str,
    config_path: Path | None = None,
) -> None:
    """
    Ingest a single dataset file. url_or_path can be URL or local path.
    Downloads if URL, then loads into unified raw.draft/game/replay.
    """
    config = load_config(config_path)
    paths = get_paths(config)
    ensure_paths(paths)

    local_path: Path
    if url_or_path.startswith(("http://", "https://")):
        url = url_or_path
        filename = Path(url).name
        local_path = paths["raw_datasets"] / filename
        download_file(url, local_path)
    else:
        local_path = Path(url_or_path)
        if not local_path.exists():
            raise FileNotFoundError(f"File not found: {local_path}")

    parsed = _parse_dataset_filename(local_path.name)
    conn = duckdb.connect(str(paths["db"]))
    try:
        if parsed:
            data_type, expansion, event_type = parsed
            append_to_unified_table(
                conn, local_path, data_type, expansion, event_type
            )
        else:
            # Fallback: load into raw with inferred table name (for non-standard files)
            stem = local_path.stem
            if stem.endswith(".csv"):
                stem = Path(stem).stem
            table_name = stem.replace(".", "_").replace("-", "_").lower()
            load_csv_into_duckdb(local_path, conn, table_name, schema="raw")
    finally:
        conn.close()


def ingest_set(
    expansion: str,
    format_key: str | None = None,
    config_path: Path | None = None,
    *,
    force_download: bool = False,
) -> IngestStats:
    """Ingest all datasets for an expansion (optionally filtered by format)."""
    config = load_config(config_path)
    paths = get_paths(config, expansion)
    ensure_paths(paths)

    stats = IngestStats()
    skip_download_if_unchanged = bool(config.get("skip_download_if_unchanged", False)) and not force_download
    state_path = paths["ingest_remote_state"]

    s3_base = config.get("s3_base", "https://17lands-public.s3.amazonaws.com")
    patterns = config.get("url_patterns", {})
    format_map = config.get("format_map", {})
    expansions_cfg = {e["code"]: e for e in config.get("expansions", []) if "code" in e}

    if expansion not in expansions_cfg:
        raise ValueError(f"Unknown expansion: {expansion}. Add to config expansions.")

    exp_cfg = expansions_cfg[expansion]
    # URL segment may differ from code (e.g. url_expansion: Cube_-_Powered vs code: CUBE_PW)
    expansion_slug = exp_cfg.get("url_expansion", expansion)
    formats = [format_key] if format_key else exp_cfg.get("formats", [])

    if not formats:
        raise ValueError(f"No formats for expansion {expansion}")

    ingest_data_types = config.get("ingest_data_types")
    if not ingest_data_types:
        ingest_data_types = list(patterns.keys())
    else:
        ingest_data_types = list(ingest_data_types)

    conn = duckdb.connect(str(paths["db"]))
    try:
        for fmt in formats:
            event_type = format_map.get(fmt, fmt)
            for data_type, pattern in patterns.items():
                if data_type not in ingest_data_types:
                    continue
                url = build_dataset_url(
                    s3_base, pattern, expansion_slug, fmt, format_map
                )
                filename = Path(url).name
                local_path = paths["raw_datasets"] / expansion / fmt / filename
                dl_result = "downloaded"
                try:
                    if skip_download_if_unchanged:
                        dl_result = download_if_newer(
                            url, local_path, state_path, skip_if_unchanged=True
                        )
                        stats.add_download_result(dl_result)
                    else:
                        download_file(url, local_path)
                        stats.downloaded += 1
                except requests.HTTPError as e:
                    if e.response.status_code in (403, 404):
                        logger.warning("Skipping %s (%s): %s", url, e.response.status_code, e)
                        stats.skipped_http += 1
                        continue
                    raise

                try:
                    if (
                        dl_result == "skipped"
                        and _slice_has_rows(conn, data_type, expansion, event_type)
                    ):
                        logger.info(
                            "Skipping DuckDB reload for %s / %s / %s (file unchanged, slice already loaded)",
                            expansion,
                            event_type,
                            data_type,
                        )
                        continue
                    append_to_unified_table(
                        conn, local_path, data_type, expansion, event_type
                    )
                except Exception as e:
                    logger.error("Failed to load %s: %s", local_path, e)
                    raise
    finally:
        conn.close()

    logger.info("Ingest summary [set %s]: %s", expansion, stats.summary_line(f"{expansion}"))
    return stats


def ingest_all(
    config_path: Path | None = None,
    *,
    force_download: bool = False,
) -> IngestStats:
    """Ingest all expansions and formats from config."""
    config = load_config(config_path)
    total = IngestStats()
    for exp in config.get("expansions", []):
        code = exp.get("code")
        if code:
            total.merge(ingest_set(code, config_path=config_path, force_download=force_download))
    logger.info("Ingest summary [all sets]: %s", total.summary_line("total"))
    return total
