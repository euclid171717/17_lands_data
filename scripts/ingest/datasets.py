"""Ingest draft, game, and replay datasets from 17lands."""

import logging
from pathlib import Path

import duckdb
import requests

from .config import get_paths, load_config, ensure_paths

logger = logging.getLogger(__name__)


def build_dataset_url(
    s3_base: str,
    pattern: str,
    expansion: str,
    format_key: str,
    format_map: dict[str, str],
) -> str:
    """Build URL from pattern. format_key is our key (e.g. premier_draft)."""
    fmt = format_map.get(format_key, format_key)
    path = pattern.format(expansion=expansion, format=fmt)
    return f"{s3_base.rstrip('/')}/{path}"


def download_file(url: str, dest: Path) -> None:
    """Stream download URL to dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, stream=True, timeout=120)
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
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    full_table = f"raw.{target_table}"

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
            f"INSERT INTO {full_table} SELECT * FROM read_csv_auto(?)",
            [str(csv_path)],
        )
        count = conn.execute(
            f"SELECT COUNT(*) FROM {full_table} WHERE expansion = ? AND event_type = ?",
            [expansion, event_type],
        ).fetchone()[0]
        logger.info("Upserted %s/%s -> %s (%d rows)", expansion, event_type, full_table, count)
    else:
        conn.execute(
            f"CREATE TABLE {full_table} AS SELECT * FROM read_csv_auto(?)",
            [str(csv_path)],
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        logger.info("Created %s from %s (%d rows)", full_table, csv_path.name, count)


def _parse_dataset_filename(filename: str) -> tuple[str, str, str] | None:
    """
    Parse 17lands dataset filename into (data_type, expansion, event_type).
    e.g. draft_data_public.MKM.PremierDraft.csv.gz -> (draft, MKM, PremierDraft)
    Returns None if not a recognized dataset file.
    """
    stem = Path(filename).stem
    if stem.endswith(".csv"):
        stem = Path(stem).stem
    parts = stem.replace("-", "_").split(".")
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
) -> None:
    """Ingest all datasets for an expansion (optionally filtered by format)."""
    config = load_config(config_path)
    paths = get_paths(config)
    ensure_paths(paths)

    s3_base = config.get("s3_base", "https://17lands-public.s3.amazonaws.com")
    patterns = config.get("url_patterns", {})
    format_map = config.get("format_map", {})
    expansions_cfg = {e["code"]: e for e in config.get("expansions", []) if "code" in e}

    if expansion not in expansions_cfg:
        raise ValueError(f"Unknown expansion: {expansion}. Add to config expansions.")

    exp_cfg = expansions_cfg[expansion]
    formats = [format_key] if format_key else exp_cfg.get("formats", [])

    if not formats:
        raise ValueError(f"No formats for expansion {expansion}")

    conn = duckdb.connect(str(paths["db"]))
    try:
        for fmt in formats:
            event_type = format_map.get(fmt, fmt)
            for data_type, pattern in patterns.items():
                url = build_dataset_url(
                    s3_base, pattern, expansion, fmt, format_map
                )
                filename = Path(url).name
                local_path = paths["raw_datasets"] / expansion / fmt / filename
                try:
                    download_file(url, local_path)
                except requests.HTTPError as e:
                    if e.response.status_code == 404:
                        logger.warning("Not found (404): %s", url)
                        continue
                    raise

                try:
                    append_to_unified_table(
                        conn, local_path, data_type, expansion, event_type
                    )
                except Exception as e:
                    logger.error("Failed to load %s: %s", local_path, e)
                    raise
    finally:
        conn.close()


def ingest_all(config_path: Path | None = None) -> None:
    """Ingest all expansions and formats from config."""
    config = load_config(config_path)
    for exp in config.get("expansions", []):
        code = exp.get("code")
        if code:
            ingest_set(code, config_path=config_path)
